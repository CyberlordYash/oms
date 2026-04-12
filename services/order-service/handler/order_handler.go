package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	orderv1 "github.com/yourusername/oms/gen/order/v1"
	riskv1 "github.com/yourusername/oms/gen/risk/v1"
	"github.com/yourusername/oms/services/order-service/internal/natswrapper"
	"github.com/yourusername/oms/services/order-service/repository"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	natsSubjectOrderPlaced    = "orders.placed"
	natsSubjectOrderModified  = "orders.modified"
	natsSubjectOrderCancelled = "orders.cancelled"
)

// OrderHandler implements orderv1.OrderServiceServer.
type OrderHandler struct {
	orderv1.UnimplementedOrderServiceServer

	repo     *repository.Repository
	nats     natswrapper.Publisher
	riskAddr string // gRPC address of the Risk Engine, e.g. "localhost:50052"
	logger   *slog.Logger
}

// New returns a configured OrderHandler.
func New(
	repo *repository.Repository,
	nats natswrapper.Publisher,
	riskAddr string,
	logger *slog.Logger,
) *OrderHandler {
	return &OrderHandler{
		repo:     repo,
		nats:     nats,
		riskAddr: riskAddr,
		logger:   logger,
	}
}

// ---------------------------------------------------------------------------
// PlaceOrder
// ---------------------------------------------------------------------------

// PlaceOrder validates the request, performs a risk check, persists the order
// with status PENDING, and publishes an event to NATS.
func (h *OrderHandler) PlaceOrder(ctx context.Context, req *orderv1.PlaceOrderRequest) (*orderv1.OrderResponse, error) {
	// 1. Basic field validation.
	if err := validatePlaceOrder(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "validation: %v", err)
	}

	// 2. Risk Engine check.
	if err := h.checkRisk(ctx, req); err != nil {
		return nil, err // already a gRPC status error
	}

	// 3. Persist with status PENDING.
	order := repository.Order{
		ID:           uuid.New().String(),
		ClientID:     req.ClientId,
		Symbol:       req.Symbol,
		Exchange:     req.Exchange.String(),
		Side:         req.Side.String(),
		OrderType:    req.OrderType.String(),
		Quantity:     req.Quantity,
		Price:        req.Price,
		TriggerPrice: req.TriggerPrice,
		Status:       "PENDING",
	}

	created, err := h.repo.CreateOrder(ctx, order)
	if err != nil {
		h.logger.ErrorContext(ctx, "failed to create order", "error", err)
		return nil, status.Errorf(codes.Internal, "persist order: %v", err)
	}

	// 4. Publish event (non-fatal on failure — order is already persisted).
	if pubErr := h.publishEvent(natsSubjectOrderPlaced, map[string]any{
		"order_id":  created.ID,
		"client_id": created.ClientID,
		"symbol":    created.Symbol,
		"exchange":  created.Exchange,
		"side":      created.Side,
		"quantity":  created.Quantity,
		"price":     created.Price,
		"status":    created.Status,
	}); pubErr != nil {
		h.logger.WarnContext(ctx, "nats publish failed", "subject", natsSubjectOrderPlaced, "error", pubErr)
	}

	h.logger.InfoContext(ctx, "order placed", "order_id", created.ID, "client_id", created.ClientID)

	return &orderv1.OrderResponse{
		OrderId:   created.ID,
		Status:    orderv1.OrderStatus_ORDER_STATUS_PENDING,
		Message:   "Order placed successfully",
		Timestamp: timestamppb.New(created.CreatedAt),
	}, nil
}

// ---------------------------------------------------------------------------
// ModifyOrder
// ---------------------------------------------------------------------------

func (h *OrderHandler) ModifyOrder(ctx context.Context, req *orderv1.ModifyOrderRequest) (*orderv1.OrderResponse, error) {
	if req.OrderId == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id is required")
	}

	existing, err := h.repo.GetOrderByID(ctx, req.OrderId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "order %s not found", req.OrderId)
	}

	if existing.Status != "PENDING" && existing.Status != "OPEN" {
		return nil, status.Errorf(codes.FailedPrecondition,
			"cannot modify order in status %s", existing.Status)
	}

	if pubErr := h.publishEvent(natsSubjectOrderModified, map[string]any{
		"order_id":      req.OrderId,
		"quantity":      req.Quantity,
		"price":         req.Price,
		"trigger_price": req.TriggerPrice,
	}); pubErr != nil {
		h.logger.WarnContext(ctx, "nats publish failed", "subject", natsSubjectOrderModified, "error", pubErr)
	}

	return &orderv1.OrderResponse{
		OrderId: req.OrderId,
		Status:  orderv1.OrderStatus_ORDER_STATUS_OPEN,
		Message: "Modify request submitted",
	}, nil
}

// ---------------------------------------------------------------------------
// CancelOrder
// ---------------------------------------------------------------------------

func (h *OrderHandler) CancelOrder(ctx context.Context, req *orderv1.CancelOrderRequest) (*orderv1.OrderResponse, error) {
	if req.OrderId == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id is required")
	}

	existing, err := h.repo.GetOrderByID(ctx, req.OrderId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "order %s not found", req.OrderId)
	}
	if existing.ClientID != req.ClientId {
		return nil, status.Error(codes.PermissionDenied, "order does not belong to client")
	}
	if existing.Status == "EXECUTED" || existing.Status == "CANCELLED" {
		return nil, status.Errorf(codes.FailedPrecondition,
			"cannot cancel order in status %s", existing.Status)
	}

	if err := h.repo.UpdateOrderStatus(ctx, req.OrderId, "CANCELLED"); err != nil {
		return nil, status.Errorf(codes.Internal, "cancel order: %v", err)
	}

	if pubErr := h.publishEvent(natsSubjectOrderCancelled, map[string]any{
		"order_id":  req.OrderId,
		"client_id": req.ClientId,
	}); pubErr != nil {
		h.logger.WarnContext(ctx, "nats publish failed", "subject", natsSubjectOrderCancelled, "error", pubErr)
	}

	return &orderv1.OrderResponse{
		OrderId: req.OrderId,
		Status:  orderv1.OrderStatus_ORDER_STATUS_CANCELLED,
		Message: "Order cancelled",
	}, nil
}

// ---------------------------------------------------------------------------
// GetOrderStatus
// ---------------------------------------------------------------------------

func (h *OrderHandler) GetOrderStatus(ctx context.Context, req *orderv1.OrderStatusRequest) (*orderv1.OrderStatusResponse, error) {
	if req.OrderId == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id is required")
	}

	o, err := h.repo.GetOrderByID(ctx, req.OrderId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "order %s not found", req.OrderId)
	}

	return &orderv1.OrderStatusResponse{
		OrderId:        o.ID,
		Symbol:         o.Symbol,
		Exchange:       orderv1.Exchange(orderv1.Exchange_value["EXCHANGE_"+o.Exchange]),
		Side:           orderv1.Side(orderv1.Side_value["SIDE_"+o.Side]),
		OrderType:      orderv1.OrderType(orderv1.OrderType_value["ORDER_TYPE_"+o.OrderType]),
		Quantity:       o.Quantity,
		FilledQuantity: o.FilledQuantity,
		Price:          o.Price,
		Status:         orderv1.OrderStatus(orderv1.OrderStatus_value["ORDER_STATUS_"+o.Status]),
		CreatedAt:      timestamppb.New(o.CreatedAt),
		UpdatedAt:      timestamppb.New(o.UpdatedAt),
	}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func validatePlaceOrder(req *orderv1.PlaceOrderRequest) error {
	if req.Symbol == "" {
		return fmt.Errorf("symbol is required")
	}
	if req.ClientId == "" {
		return fmt.Errorf("client_id is required")
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
	if req.OrderType == orderv1.OrderType_ORDER_TYPE_UNSPECIFIED {
		return fmt.Errorf("order_type is required")
	}
	if req.OrderType == orderv1.OrderType_ORDER_TYPE_LIMIT && req.Price <= 0 {
		return fmt.Errorf("price must be > 0 for LIMIT orders")
	}
	if (req.OrderType == orderv1.OrderType_ORDER_TYPE_SL ||
		req.OrderType == orderv1.OrderType_ORDER_TYPE_SL_M) && req.TriggerPrice <= 0 {
		return fmt.Errorf("trigger_price must be > 0 for SL/SL_M orders")
	}
	return nil
}

// checkRisk dials the Risk Engine and returns a gRPC status error on rejection.
func (h *OrderHandler) checkRisk(ctx context.Context, req *orderv1.PlaceOrderRequest) error {
	conn, err := grpc.NewClient(h.riskAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return status.Errorf(codes.Unavailable, "risk engine unavailable: %v", err)
	}
	defer conn.Close()

	resp, err := riskv1.NewRiskServiceClient(conn).CheckRisk(ctx, &riskv1.CheckRiskRequest{
		ClientId:   req.ClientId,
		Symbol:     req.Symbol,
		Exchange:   req.Exchange,
		Side:       req.Side,
		Quantity:   req.Quantity,
		Price:      req.Price,
		OrderValue: req.Quantity * req.Price,
	})
	if err != nil {
		return status.Errorf(codes.Unavailable, "risk check RPC failed: %v", err)
	}
	if !resp.Approved {
		return status.Errorf(codes.PermissionDenied,
			"risk rejected [%s]: %s", resp.ReasonCode.String(), resp.Message)
	}
	return nil
}

func (h *OrderHandler) publishEvent(subject string, payload map[string]any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	return h.nats.Publish(subject, data)
}
