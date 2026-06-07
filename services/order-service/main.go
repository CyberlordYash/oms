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

	"github.com/spf13/viper"
	pkgdb "github.com/yourusername/oms/pkg/db"
	pkgnats "github.com/yourusername/oms/pkg/nats"
	"github.com/yourusername/oms/services/order-service/internal/metrics"
	"github.com/yourusername/oms/services/order-service/internal/processor"
	"github.com/yourusername/oms/services/order-service/repository"
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
	viper.SetDefault("db.port", 5433)       // docker-compose host port
	viper.SetDefault("db.user", "oms")
	viper.SetDefault("db.password", "oms_secret")
	viper.SetDefault("db.name", "oms")
	viper.SetDefault("nats.url", "nats://localhost:4223") // docker-compose host port
	viper.SetDefault("risk.addr", "localhost:50052")
	viper.SetDefault("pool.workers", 8)
	viper.SetDefault("pool.queue", 1024)
	viper.SetDefault("pool.submit_timeout_ms", 50)
	viper.SetDefault("pool.min_latency_ms", 5)
	viper.SetDefault("pool.max_latency_ms", 50)

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // db.host → DB_HOST
	viper.AutomaticEnv()
	_ = viper.ReadInConfig() // config file is optional; env vars / defaults suffice

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

	// ── NATS ──────────────────────────────────────────────────────────────────
	natsClient, err := pkgnats.New(pkgnats.Config{
		URL:           viper.GetString("nats.url"),
		MaxReconnects: 10,
	})
	if err != nil {
		// NATS is important but we allow the service to start without it so
		// that local development without a NATS server is still possible.
		logger.Warn("nats unavailable — order events will not be published", "error", err)
		natsClient = nil
	} else {
		defer natsClient.Close()
		logger.Info("nats connected")
	}

	// ── Dependencies ──────────────────────────────────────────────────────────
	repo := repository.New(pool)

	// natsClient satisfies natswrapper.Publisher; when nil we use a no-op.
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

	// ── Servers (gRPC + HTTP share the same OrderHandler) ──────────────────────
	grpcServer, orderHandler := server.New(repo, publisher, workerPool, reg, viper.GetString("risk.addr"), logger)
	httpServer := server.NewHTTPServer(fmt.Sprintf(":%d", httpPort), orderHandler, reg, logger)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		return fmt.Errorf("listen on :%d: %w", grpcPort, err)
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

	// 1. Stop accepting new orders on both transports.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http shutdown", "error", err)
	}
	grpcServer.GracefulStop()

	// 2. Drain in-flight orders so no accepted order is lost.
	workerPool.Stop(shutdownCtx)

	// 3. NATS and DB pool are closed by the deferred calls above.
	return nil
}

// natswrapper is the minimal interface used inside main.
type natswrapper interface {
	Publish(subject string, data []byte) error
}

// noopPublisher silently drops events when NATS is unavailable.
type noopPublisher struct{}

func (noopPublisher) Publish(_ string, _ []byte) error { return nil }
