// This file adds an HTTP/REST transport (Gin) that sits in front of the very
// same OrderHandler used by gRPC. The HTTP handlers are thin adapters: they bind
// JSON, build the existing proto request, call the handler method, and translate
// the gRPC status error into an HTTP status code. No business logic lives here.
package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	orderv1 "github.com/yourusername/oms/gen/order/v1"
	"github.com/yourusername/oms/services/order-service/handler"
	"github.com/yourusername/oms/services/order-service/internal/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewHTTPServer builds an *http.Server exposing the order API over REST plus a
// /metrics and /healthz endpoint. The caller runs ListenAndServe / Shutdown.
func NewHTTPServer(addr string, h *handler.OrderHandler, m *metrics.Registry, logger *slog.Logger) *http.Server {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), httpLogger(logger))

	api := &httpAPI{h: h, m: m}

	r.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusOK) })
	r.GET("/metrics", api.metrics)

	v1 := r.Group("/v1")
	{
		v1.POST("/orders", api.placeOrder)
		v1.GET("/orders/:id", api.getOrder)
		v1.PUT("/orders/:id", api.modifyOrder)
		v1.DELETE("/orders/:id", api.cancelOrder)
	}

	return &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

type httpAPI struct {
	h *handler.OrderHandler
	m *metrics.Registry
}

// ── Request DTOs ────────────────────────────────────────────────────────────
// Enum fields are accepted as their proto string names (e.g. "EXCHANGE_NSE") to
// keep the JSON self-describing.

type placeOrderBody struct {
	Symbol       string `json:"symbol"`
	Exchange     string `json:"exchange"`
	Side         string `json:"side"`
	OrderType    string `json:"order_type"`
	Quantity     int64  `json:"quantity"`
	Price        int64  `json:"price"`
	ClientID     string `json:"client_id"`
	TriggerPrice int64  `json:"trigger_price"`
}

type modifyOrderBody struct {
	Quantity     int64 `json:"quantity"`
	Price        int64 `json:"price"`
	TriggerPrice int64 `json:"trigger_price"`
}

type cancelOrderBody struct {
	ClientID string `json:"client_id"`
}

// ── Handlers ────────────────────────────────────────────────────────────────

func (a *httpAPI) placeOrder(c *gin.Context) {
	var body placeOrderBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json: " + err.Error()})
		return
	}

	req := &orderv1.PlaceOrderRequest{
		Symbol:       body.Symbol,
		Exchange:     orderv1.Exchange(orderv1.Exchange_value[body.Exchange]),
		Side:         orderv1.Side(orderv1.Side_value[body.Side]),
		OrderType:    orderv1.OrderType(orderv1.OrderType_value[body.OrderType]),
		Quantity:     body.Quantity,
		Price:        body.Price,
		ClientId:     body.ClientID,
		TriggerPrice: body.TriggerPrice,
	}

	resp, err := a.h.PlaceOrder(c.Request.Context(), req)
	if err != nil {
		writeStatusError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{
		"order_id": resp.OrderId,
		"status":   resp.Status.String(),
		"message":  resp.Message,
	})
}

func (a *httpAPI) getOrder(c *gin.Context) {
	resp, err := a.h.GetOrderStatus(c.Request.Context(), &orderv1.OrderStatusRequest{
		OrderId: c.Param("id"),
	})
	if err != nil {
		writeStatusError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"order_id":        resp.OrderId,
		"symbol":          resp.Symbol,
		"exchange":        resp.Exchange.String(),
		"side":            resp.Side.String(),
		"order_type":      resp.OrderType.String(),
		"quantity":        resp.Quantity,
		"filled_quantity": resp.FilledQuantity,
		"price":           resp.Price,
		"status":          resp.Status.String(),
	})
}

func (a *httpAPI) modifyOrder(c *gin.Context) {
	var body modifyOrderBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json: " + err.Error()})
		return
	}
	resp, err := a.h.ModifyOrder(c.Request.Context(), &orderv1.ModifyOrderRequest{
		OrderId:      c.Param("id"),
		Quantity:     body.Quantity,
		Price:        body.Price,
		TriggerPrice: body.TriggerPrice,
	})
	if err != nil {
		writeStatusError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"order_id": resp.OrderId,
		"status":   resp.Status.String(),
		"message":  resp.Message,
	})
}

func (a *httpAPI) cancelOrder(c *gin.Context) {
	var body cancelOrderBody
	// Body is optional for DELETE; ignore bind errors on an empty body.
	_ = c.ShouldBindJSON(&body)
	resp, err := a.h.CancelOrder(c.Request.Context(), &orderv1.CancelOrderRequest{
		OrderId:  c.Param("id"),
		ClientId: body.ClientID,
	})
	if err != nil {
		writeStatusError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"order_id": resp.OrderId,
		"status":   resp.Status.String(),
		"message":  resp.Message,
	})
}

func (a *httpAPI) metrics(c *gin.Context) {
	c.JSON(http.StatusOK, a.m.Snapshot())
}

// ── Helpers ─────────────────────────────────────────────────────────────────

// writeStatusError maps a gRPC status error onto an HTTP status code so the same
// handler errors drive both transports consistently.
func writeStatusError(c *gin.Context, err error) {
	st := status.Convert(err)
	c.JSON(grpcCodeToHTTP(st.Code()), gin.H{
		"error": st.Message(),
		"code":  st.Code().String(),
	})
}

func grpcCodeToHTTP(code codes.Code) int {
	switch code {
	case codes.OK:
		return http.StatusOK
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.NotFound:
		return http.StatusNotFound
	case codes.FailedPrecondition:
		return http.StatusConflict
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// httpLogger is a minimal structured access log, mirroring the gRPC interceptor.
func httpLogger(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		logger.LogAttrs(context.Background(), slog.LevelInfo, "http",
			slog.String("method", c.Request.Method),
			slog.String("path", c.FullPath()),
			slog.Int("status", c.Writer.Status()),
			slog.Int64("duration_ms", time.Since(start).Milliseconds()),
		)
	}
}
