package routes

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yourusername/oms/services/order-service/handler"
	"github.com/yourusername/oms/services/order-service/internal/metrics"
)

type Router struct {
	orders  *handler.OrderHandler
	metrics *metrics.Registry
}

func New(addr string, orders *handler.OrderHandler, m *metrics.Registry, logger *slog.Logger) *http.Server {
	gin.SetMode(gin.ReleaseMode)

	e := gin.New()
	e.Use(gin.Recovery(), accessLog(logger))

	rt := &Router{orders: orders, metrics: m}

	e.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusOK) })
	e.GET("/metrics", rt.metricsSnapshot)

	order := e.Group("/order")
	{
		order.POST("", rt.place)
		order.GET("/:id", rt.status)
		order.PUT("/:id", rt.modify)
		order.DELETE("/:id", rt.cancel)
	}

	return &http.Server{
		Addr:              addr,
		Handler:           e,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func accessLog(logger *slog.Logger) gin.HandlerFunc {
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
