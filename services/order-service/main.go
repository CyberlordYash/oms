package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	pkgdb "github.com/yourusername/oms/pkg/db"
	pkgnats "github.com/yourusername/oms/pkg/nats"
	"github.com/yourusername/oms/services/order-service/internal/consumer"
	"github.com/yourusername/oms/services/order-service/internal/metrics"
	"github.com/yourusername/oms/services/order-service/internal/processor"
	"github.com/yourusername/oms/services/order-service/repository"
	"github.com/yourusername/oms/services/order-service/routes"
	"github.com/yourusername/oms/services/order-service/server"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := run(logger); err != nil {
		logger.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	// ── Config ────────────────────────────────────────────────────────────────
	viper.SetDefault("grpc.port", 50051)
	viper.SetDefault("http.port", 8080)
	viper.SetDefault("db.host", "localhost")
	viper.SetDefault("db.port", 5433)
	viper.SetDefault("db.user", "oms")
	viper.SetDefault("db.password", "oms_secret")
	viper.SetDefault("db.name", "oms")
	viper.SetDefault("nats.url", "nats://localhost:4223")
	viper.SetDefault("risk.addr", "localhost:50052")
	viper.SetDefault("redis.addr", "localhost:6380")
	viper.SetDefault("pool.workers", 8)
	viper.SetDefault("pool.queue", 1024)
	viper.SetDefault("pool.submit_timeout_ms", 50)
	viper.SetDefault("pool.min_latency_ms", 5)
	viper.SetDefault("pool.max_latency_ms", 50)

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
	_ = viper.ReadInConfig()

	grpcPort := viper.GetInt("grpc.port")
	httpPort := viper.GetInt("http.port")

	// ── Postgres ──────────────────────────────────────────────────────────────
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pkgdb.NewPool(ctx, pkgdb.Config{
		Host:     viper.GetString("db.host"),
		Port:     viper.GetInt("db.port"),
		User:     viper.GetString("db.user"),
		Password: viper.GetString("db.password"),
		DBName:   viper.GetString("db.name"),
	})
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()
	logger.Info("postgres connected")

	// ── Redis (position tracking for JetStream consumer) ─────────────────────
	rdb := redis.NewClient(&redis.Options{
		Addr:         viper.GetString("redis.addr"),
		DialTimeout:  5 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		// Redis is used for position tracking only; let the service start without it.
		logger.Warn("redis unavailable — position tracking will be disabled", "error", err)
		rdb = nil
	} else {
		defer rdb.Close()
		logger.Info("redis connected", "addr", viper.GetString("redis.addr"))
	}

	// ── NATS ──────────────────────────────────────────────────────────────────
	natsClient, err := pkgnats.New(pkgnats.Config{
		URL:           viper.GetString("nats.url"),
		MaxReconnects: 10,
	})
	if err != nil {
		logger.Warn("nats unavailable — order events will not be published", "error", err)
		natsClient = nil
	} else {
		defer natsClient.Close()
		logger.Info("nats connected")
	}

	// ── Dependencies ──────────────────────────────────────────────────────────
	repo := repository.New(pool)

	var publisher natswrapper
	if natsClient != nil {
		publisher = natsClient
	} else {
		publisher = noopPublisher{}
	}

	reg := metrics.New()

	workerPool := processor.New(repo, publisher, reg, logger, processor.Config{
		Workers:       viper.GetInt("pool.workers"),
		QueueSize:     viper.GetInt("pool.queue"),
		SubmitTimeout: time.Duration(viper.GetInt("pool.submit_timeout_ms")) * time.Millisecond,
		MinLatency:    time.Duration(viper.GetInt("pool.min_latency_ms")) * time.Millisecond,
		MaxLatency:    time.Duration(viper.GetInt("pool.max_latency_ms")) * time.Millisecond,
	})
	workerPool.Start()

	// ── Servers ───────────────────────────────────────────────────────────────
	grpcServer, orderHandler, err := server.New(repo, publisher, workerPool, reg, viper.GetString("risk.addr"), logger)
	if err != nil {
		return fmt.Errorf("build gRPC server: %w", err)
	}
	defer orderHandler.Close()

	httpServer := routes.New(fmt.Sprintf(":%d", httpPort), orderHandler, reg, logger)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		return fmt.Errorf("listen on :%d: %w", grpcPort, err)
	}

	// ── JetStream consumer ────────────────────────────────────────────────────
	var posTracker *consumer.PositionTracker
	if natsClient != nil && rdb != nil {
		posTracker = consumer.New(natsClient.JS, rdb, logger)
		if err := posTracker.Start(context.Background()); err != nil {
			// Non-fatal: position tracking is a best-effort enhancement.
			logger.Warn("position tracker failed to start", "error", err)
			posTracker = nil
		} else {
			logger.Info("position tracker started")
		}
	}

	// ── Run + graceful shutdown ───────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		logger.Info("gRPC server starting", "port", grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			errCh <- fmt.Errorf("grpc serve: %w", err)
		}
	}()
	go func() {
		logger.Info("HTTP server starting", "port", httpPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("http serve: %w", err)
		}
	}()

	select {
	case sig := <-quit:
		logger.Info("shutting down", "signal", sig)
	case err := <-errCh:
		return err
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if posTracker != nil {
		posTracker.Stop()
	}
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http shutdown", "error", err)
	}
	grpcServer.GracefulStop()
	workerPool.Stop(shutdownCtx)

	return nil
}

type natswrapper interface {
	Publish(subject string, data []byte) error
}

type noopPublisher struct{}

func (noopPublisher) Publish(_ string, _ []byte) error { return nil }
