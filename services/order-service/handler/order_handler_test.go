package handler

import (
	"testing"

	orderv1 "github.com/yourusername/oms/gen/order/v1"
)

func TestValidatePlaceOrder(t *testing.T) {
	validBase := func() *orderv1.PlaceOrderRequest {
		return &orderv1.PlaceOrderRequest{
			ClientId:  "client1",
			Symbol:    "RELIANCE",
			Exchange:  orderv1.Exchange_EXCHANGE_NSE,
			Side:      orderv1.Side_SIDE_BUY,
			OrderType: orderv1.OrderType_ORDER_TYPE_MARKET,
			Quantity:  10,
			Price:     0,
		}
	}

	cases := []struct {
		name    string
		mutate  func(r *orderv1.PlaceOrderRequest)
		wantErr bool
	}{
		{"valid market order", func(r *orderv1.PlaceOrderRequest) {}, false},
		{"valid limit order", func(r *orderv1.PlaceOrderRequest) {
			r.OrderType = orderv1.OrderType_ORDER_TYPE_LIMIT
			r.Price = 280000
		}, false},
		{"valid SL order", func(r *orderv1.PlaceOrderRequest) {
			r.OrderType = orderv1.OrderType_ORDER_TYPE_SL
			r.Price = 280000
			r.TriggerPrice = 275000
		}, false},
		{"missing symbol", func(r *orderv1.PlaceOrderRequest) { r.Symbol = "" }, true},
		{"missing client_id", func(r *orderv1.PlaceOrderRequest) { r.ClientId = "" }, true},
		{"zero quantity", func(r *orderv1.PlaceOrderRequest) { r.Quantity = 0 }, true},
		{"negative quantity", func(r *orderv1.PlaceOrderRequest) { r.Quantity = -1 }, true},
		{"unspecified exchange", func(r *orderv1.PlaceOrderRequest) {
			r.Exchange = orderv1.Exchange_EXCHANGE_UNSPECIFIED
		}, true},
		{"unspecified side", func(r *orderv1.PlaceOrderRequest) {
			r.Side = orderv1.Side_SIDE_UNSPECIFIED
		}, true},
		{"unspecified order type", func(r *orderv1.PlaceOrderRequest) {
			r.OrderType = orderv1.OrderType_ORDER_TYPE_UNSPECIFIED
		}, true},
		{"limit order with zero price", func(r *orderv1.PlaceOrderRequest) {
			r.OrderType = orderv1.OrderType_ORDER_TYPE_LIMIT
			r.Price = 0
		}, true},
		{"limit order with negative price", func(r *orderv1.PlaceOrderRequest) {
			r.OrderType = orderv1.OrderType_ORDER_TYPE_LIMIT
			r.Price = -1
		}, true},
		{"SL order missing trigger_price", func(r *orderv1.PlaceOrderRequest) {
			r.OrderType = orderv1.OrderType_ORDER_TYPE_SL
			r.Price = 280000
			r.TriggerPrice = 0
		}, true},
		{"SL_M order missing trigger_price", func(r *orderv1.PlaceOrderRequest) {
			r.OrderType = orderv1.OrderType_ORDER_TYPE_SL_M
			r.TriggerPrice = 0
		}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := validBase()
			tc.mutate(req)
			err := validatePlaceOrder(req)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
