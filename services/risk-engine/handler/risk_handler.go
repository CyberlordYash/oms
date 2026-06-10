package handler

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	orderv1 "github.com/yourusername/oms/gen/order/v1"
	riskv1 "github.com/yourusername/oms/gen/risk/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxDailyTurnover int64 = 2_500_000_000 // ₹25,00,000 in paise
	maxPosition      int64 = 10_000
	circuitBreakerPct      = 0.20
	dupOrderTTL            = 500 * time.Millisecond
	dailyLimitTTL          = 24 * time.Hour
)

type RiskHandler struct {
	riskv1.UnimplementedRiskServiceServer
	rdb *redis.Client
}

func New(rdb *redis.Client) *RiskHandler {
	return &RiskHandler{rdb: rdb}
}

func (h *RiskHandler) CheckRisk(ctx context.Context, req *riskv1.CheckRiskRequest) (*riskv1.CheckRiskResponse, error) {
	if err := validateRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid request: %v", err)
	}

	dupKey := fmt.Sprintf("dup:%s:%s:%s", req.ClientId, req.Symbol, req.Side.String())
	set, err := h.rdb.SetNX(ctx, dupKey, "1", dupOrderTTL).Result()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "redis: dup check: %v", err)
	}
	if !set {
		return reject(riskv1.ReasonCode_REASON_CODE_DUPLICATE_ORDER,
			"duplicate order detected within 500 ms window"), nil
	}

	dailyKey := fmt.Sprintf("daily_limit:%s", req.ClientId)
	existing, err := h.rdb.Get(ctx, dailyKey).Int64()
	if err != nil && err != redis.Nil {
		return nil, status.Errorf(codes.Internal, "redis: daily limit read: %v", err)
	}
	if existing+req.OrderValue > maxDailyTurnover {
		return reject(riskv1.ReasonCode_REASON_CODE_DAILY_LIMIT_BREACH,
			fmt.Sprintf("daily turnover limit of ₹%.2f exceeded", float64(maxDailyTurnover)/100)), nil
	}

	lastPriceKey := fmt.Sprintf("last_price:%s", req.Symbol)
	lastPriceStr, err := h.rdb.Get(ctx, lastPriceKey).Result()
	if err != nil && err != redis.Nil {
		return nil, status.Errorf(codes.Internal, "redis: last price read: %v", err)
	}
	if err != redis.Nil {
		lastPrice, parseErr := strconv.ParseFloat(lastPriceStr, 64)
		if parseErr == nil && lastPrice > 0 {
			orderPrice := float64(req.Price)
			deviation := math.Abs(orderPrice-lastPrice) / lastPrice
			if deviation > circuitBreakerPct {
				return reject(riskv1.ReasonCode_REASON_CODE_CIRCUIT_BREAKER_HIT,
					fmt.Sprintf("order price deviates %.1f%% from last traded price (limit %.0f%%)",
						deviation*100, circuitBreakerPct*100)), nil
			}
		}
	}

	posKey := fmt.Sprintf("position:%s:%s", req.ClientId, req.Symbol)
	currentPos, err := h.rdb.Get(ctx, posKey).Int64()
	if err != nil && err != redis.Nil {
		return nil, status.Errorf(codes.Internal, "redis: position read: %v", err)
	}
	if currentPos+req.Quantity > maxPosition {
		return reject(riskv1.ReasonCode_REASON_CODE_POSITION_LIMIT_BREACH,
			fmt.Sprintf("position limit of %d exceeded (current: %d, incoming: %d)",
				maxPosition, currentPos, req.Quantity)), nil
	}

	pipe := h.rdb.Pipeline()
	incrCmd := pipe.IncrBy(ctx, dailyKey, req.OrderValue)
	pipe.ExpireNX(ctx, dailyKey, dailyLimitTTL) // only sets TTL on first increment
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "redis: daily limit update: %v", err)
	}
	_ = incrCmd // value recorded; log if needed

	return &riskv1.CheckRiskResponse{
		Approved:   true,
		ReasonCode: riskv1.ReasonCode_REASON_CODE_OK,
		Message:    "all risk checks passed",
	}, nil
}

func validateRequest(req *riskv1.CheckRiskRequest) error {
	if req.ClientId == "" {
		return fmt.Errorf("client_id is required")
	}
	if req.Symbol == "" {
		return fmt.Errorf("symbol is required")
	}
	if req.Quantity <= 0 {
		return fmt.Errorf("quantity must be > 0")
	}
	if req.Exchange == orderv1.Exchange_EXCHANGE_UNSPECIFIED {
		return fmt.Errorf("exchange is required")
	}
	if req.Side == orderv1.Side_SIDE_UNSPECIFIED {
		return fmt.Errorf("side is required")
	}
	return nil
}

func reject(code riskv1.ReasonCode, msg string) *riskv1.CheckRiskResponse {
	return &riskv1.CheckRiskResponse{
		Approved:   false,
		ReasonCode: code,
		Message:    msg,
	}
}
