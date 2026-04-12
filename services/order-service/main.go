package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/viper"
	pkgdb "github.com/yourusername/oms/pkg/db"
	pkgnats "github.com/yourusername/oms/pkg/nats"
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
	viper.SetDefault("db.host", "localhost")
	viper.SetDefault("db.port", 5433)       // docker-compose host port
	viper.SetDefault("db.user", "oms")
	viper.SetDefault("db.password", "oms_secret")
	viper.SetDefault("db.name", "oms")
	viper.SetDefault("nats.url", "nats://localhost:4223") // docker-compose host port
	viper.SetDefault("risk.addr", "localhost:50052")

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // db.host → DB_HOST
	viper.AutomaticEnv()
	_ = viper.ReadInConfig() // config file is optional; env vars / defaults suffice

	grpcPort := viper.GetInt("grpc.port")

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

	// ── gRPC server ───────────────────────────────────────────────────────────
	repo := repository.New(pool)

	// natsClient satisfies natswrapper.Publisher; when nil we use a no-op.
	var publisher natswrapper
	if natsClient != nil {
		publisher = natsClient
	} else {
		publisher = noopPublisher{}
	}

	grpcServer := server.New(repo, publisher, viper.GetString("risk.addr"), logger)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		return fmt.Errorf("listen on :%d: %w", grpcPort, err)
	}

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		logger.Info("gRPC server starting", "port", grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			errCh <- fmt.Errorf("grpc serve: %w", err)
		}
	}()

	select {
	case sig := <-quit:
		logger.Info("shutting down", "signal", sig)
		grpcServer.GracefulStop()
	case err := <-errCh:
		return err
	}

	return nil
}

// natswrapper is the minimal interface used inside main.
type natswrapper interface {
	Publish(subject string, data []byte) error
}

// noopPublisher silently drops events when NATS is unavailable.
type noopPublisher struct{}

func (noopPublisher) Publish(_ string, _ []byte) error { return nil }
