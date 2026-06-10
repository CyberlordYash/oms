package handler

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	orderv1 "github.com/yourusername/oms/gen/order/v1"
	riskv1 "github.com/yourusername/oms/gen/risk/v1"
)

// redisForTest starts a Redis client pointed at a local server.
// Tests are skipped when Redis is unreachable so the suite stays green in CI
// environments without a Redis sidecar.
func redisForTest(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6380"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not reachable at localhost:6380 (%v) — skipping integration tests", err)
	}
	return rdb
}

// isolatedHandler returns a RiskHandler and a unique key prefix so parallel
// test cases don't trample each other's Redis state.
func isolatedHandler(t *testing.T) (*RiskHandler, string, *redis.Client) {
	t.Helper()
	rdb := redisForTest(t)
	prefix := "test:" + t.Name() + ":"
	h := New(rdb)
	return h, prefix, rdb
}

// setKey is a test helper that writes a raw string value to Redis and removes
// it when the test ends.
func setKey(t *testing.T, rdb *redis.Client, key, val string) {
	t.Helper()
	ctx := context.Background()
	if err := rdb.Set(ctx, key, val, 0).Err(); err != nil {
		t.Fatalf("setKey %s: %v", key, err)
	}
	t.Cleanup(func() { rdb.Del(context.Background(), key) })
}

func baseReq(clientID, symbol string) *riskv1.CheckRiskRequest {
	return &riskv1.CheckRiskRequest{
		ClientId:   clientID,
		Symbol:     symbol,
		Exchange:   orderv1.Exchange_EXCHANGE_NSE,
		Side:       orderv1.Side_SIDE_BUY,
		Quantity:   100,
		Price:      280000, // ₹2800 in paise
		OrderValue: 100 * 280000,
	}
}

// ── a. Duplicate order ────────────────────────────────────────────────────────

func TestCheckRisk_DuplicateOrder(t *testing.T) {
	h, _, rdb := isolatedHandler(t)
	ctx := context.Background()
	req := baseReq("client_dup", "RELIANCE_DUP")

	// First call must be approved.
	resp, err := h.CheckRisk(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Approved {
		t.Fatalf("expected first call to be approved, got: %s", resp.Message)
	}

	// Second call within the TTL window must be rejected as duplicate.
	resp2, err := h.CheckRisk(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error on second call: %v", err)
	}
	if resp2.Approved {
		t.Fatal("expected second call to be rejected as duplicate")
	}
	if resp2.ReasonCode != riskv1.ReasonCode_REASON_CODE_DUPLICATE_ORDER {
		t.Fatalf("expected DUPLICATE_ORDER, got %s", resp2.ReasonCode)
	}

	// Cleanup: allow TTL to expire or manually delete.
	dupKey := "dup:client_dup:RELIANCE_DUP:SIDE_BUY"
	t.Cleanup(func() { rdb.Del(context.Background(), dupKey) })
}

// ── b. Daily limit breach ─────────────────────────────────────────────────────

func TestCheckRisk_DailyLimitBreach(t *testing.T) {
	h, _, rdb := isolatedHandler(t)
	ctx := context.Background()

	clientID := "client_limit_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	dailyKey := "daily_limit:" + clientID

	// Pre-fill the counter just below the limit.
	almostFull := maxDailyTurnover - 100 // 100 paise under
	setKey(t, rdb, dailyKey, strconv.FormatInt(almostFull, 10))
	t.Cleanup(func() { rdb.Del(context.Background(), dailyKey) })

	req := baseReq(clientID, "TCS")
	req.OrderValue = 200 // pushes over by 100 paise

	// Duplicate guard: pre-clear it so it doesn't interfere.
	dupKey := "dup:" + clientID + ":TCS:SIDE_BUY"
	rdb.Del(ctx, dupKey)
	t.Cleanup(func() { rdb.Del(context.Background(), dupKey) })

	resp, err := h.CheckRisk(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Approved {
		t.Fatal("expected daily limit breach rejection")
	}
	if resp.ReasonCode != riskv1.ReasonCode_REASON_CODE_DAILY_LIMIT_BREACH {
		t.Fatalf("expected DAILY_LIMIT_BREACH, got %s", resp.ReasonCode)
	}
}

// ── c. Circuit breaker ────────────────────────────────────────────────────────

func TestCheckRisk_CircuitBreakerHit(t *testing.T) {
	h, _, rdb := isolatedHandler(t)
	ctx := context.Background()

	clientID := "client_cb_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	symbol := "INFY_CB"
	lastPriceKey := "last_price:" + symbol

	// Last price: ₹1700 (170000 paise).
	setKey(t, rdb, lastPriceKey, "170000.00")

	dupKey := "dup:" + clientID + ":" + symbol + ":SIDE_BUY"
	rdb.Del(ctx, dupKey)
	t.Cleanup(func() { rdb.Del(context.Background(), dupKey) })

	req := baseReq(clientID, symbol)
	// Price 50% above last_price — well beyond the 20% circuit breaker.
	req.Price = 255000
	req.OrderValue = req.Quantity * req.Price

	resp, err := h.CheckRisk(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Approved {
		t.Fatal("expected circuit breaker rejection")
	}
	if resp.ReasonCode != riskv1.ReasonCode_REASON_CODE_CIRCUIT_BREAKER_HIT {
		t.Fatalf("expected CIRCUIT_BREAKER_HIT, got %s", resp.ReasonCode)
	}
}

func TestCheckRisk_CircuitBreakerSkippedWhenKeyAbsent(t *testing.T) {
	h, _, rdb := isolatedHandler(t)
	ctx := context.Background()

	clientID := "client_cb_no_key_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	symbol := "NOPRICE_SYMBOL"

	// Ensure last_price key does not exist.
	rdb.Del(ctx, "last_price:"+symbol)

	dupKey := "dup:" + clientID + ":" + symbol + ":SIDE_BUY"
	rdb.Del(ctx, dupKey)
	t.Cleanup(func() {
		rdb.Del(context.Background(), dupKey)
		rdb.Del(context.Background(), "daily_limit:"+clientID)
		rdb.Del(context.Background(), "position:"+clientID+":"+symbol)
	})

	req := baseReq(clientID, symbol)
	req.Price = 999999999 // extreme price — should NOT trigger circuit breaker

	resp, err := h.CheckRisk(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Approved {
		t.Fatalf("expected approval when last_price key is absent, got: %s %s", resp.ReasonCode, resp.Message)
	}
}

// ── d. Position limit ─────────────────────────────────────────────────────────

func TestCheckRisk_PositionLimitBreach(t *testing.T) {
	h, _, rdb := isolatedHandler(t)
	ctx := context.Background()

	clientID := "client_pos_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	symbol := "SBIN_POS"
	posKey := "position:" + clientID + ":" + symbol

	// Already holding 9950 shares.
	setKey(t, rdb, posKey, "9950")

	dupKey := "dup:" + clientID + ":" + symbol + ":SIDE_BUY"
	rdb.Del(ctx, dupKey)
	t.Cleanup(func() {
		rdb.Del(context.Background(), dupKey)
		rdb.Del(context.Background(), "daily_limit:"+clientID)
	})

	req := baseReq(clientID, symbol)
	req.Quantity = 100 // 9950 + 100 = 10050 > 10000

	resp, err := h.CheckRisk(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Approved {
		t.Fatal("expected position limit rejection")
	}
	if resp.ReasonCode != riskv1.ReasonCode_REASON_CODE_POSITION_LIMIT_BREACH {
		t.Fatalf("expected POSITION_LIMIT_BREACH, got %s", resp.ReasonCode)
	}
}

// ── e. Approval + daily counter commit ───────────────────────────────────────

func TestCheckRisk_Approved_CommitsDailyCounter(t *testing.T) {
	h, _, rdb := isolatedHandler(t)
	ctx := context.Background()

	clientID := "client_ok_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	symbol := "WIPRO_OK"
	dailyKey := "daily_limit:" + clientID
	dupKey := "dup:" + clientID + ":" + symbol + ":SIDE_BUY"

	rdb.Del(ctx, dailyKey, dupKey,
		"position:"+clientID+":"+symbol,
		"last_price:"+symbol)
	t.Cleanup(func() {
		rdb.Del(context.Background(),
			dailyKey, dupKey,
			"position:"+clientID+":"+symbol)
	})

	req := baseReq(clientID, symbol)

	resp, err := h.CheckRisk(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Approved {
		t.Fatalf("expected approval, got rejection: %s", resp.Message)
	}
	if resp.ReasonCode != riskv1.ReasonCode_REASON_CODE_OK {
		t.Fatalf("expected REASON_CODE_OK, got %s", resp.ReasonCode)
	}

	// Daily counter must have been incremented by order_value.
	stored, err := rdb.Get(ctx, dailyKey).Int64()
	if err != nil {
		t.Fatalf("daily_limit key not found after approval: %v", err)
	}
	if stored != req.OrderValue {
		t.Fatalf("expected daily_limit=%d, got %d", req.OrderValue, stored)
	}
}

// ── validation ────────────────────────────────────────────────────────────────

func TestCheckRisk_InvalidRequest(t *testing.T) {
	rdb := redisForTest(t)
	h := New(rdb)
	ctx := context.Background()

	cases := []struct {
		name   string
		mutate func(*riskv1.CheckRiskRequest)
	}{
		{"empty client_id", func(r *riskv1.CheckRiskRequest) { r.ClientId = "" }},
		{"empty symbol", func(r *riskv1.CheckRiskRequest) { r.Symbol = "" }},
		{"zero quantity", func(r *riskv1.CheckRiskRequest) { r.Quantity = 0 }},
		{"unspecified exchange", func(r *riskv1.CheckRiskRequest) {
			r.Exchange = orderv1.Exchange_EXCHANGE_UNSPECIFIED
		}},
		{"unspecified side", func(r *riskv1.CheckRiskRequest) {
			r.Side = orderv1.Side_SIDE_UNSPECIFIED
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := baseReq("c1", "RELIANCE")
			tc.mutate(req)
			_, err := h.CheckRisk(ctx, req)
			if err == nil {
				t.Fatal("expected error for invalid request, got nil")
			}
		})
	}
}
