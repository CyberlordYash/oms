package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	riskv1 "github.com/yourusername/oms/gen/risk/v1"
	"github.com/yourusername/oms/services/risk-engine/config"
	"github.com/yourusername/oms/services/risk-engine/handler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
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
	cfg := config.Load()

	// ── Redis ─────────────────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: ping %s: %w", cfg.RedisAddr, err)
	}
	logger.Info("redis connected", "addr", cfg.RedisAddr)

	// ── gRPC server ───────────────────────────────────────────────────────────
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(loggingInterceptor(logger)),
	}
	if creds, ok := grpcServerCreds(logger); ok {
		opts = append(opts, grpc.Creds(creds))
		logger.Info("mTLS enabled")
	}

	srv := grpc.NewServer(opts...)
	riskv1.RegisterRiskServiceServer(srv, handler.New(rdb))
	reflection.Register(srv)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		return fmt.Errorf("listen on :%d: %w", cfg.GRPCPort, err)
	}

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		logger.Info("risk engine gRPC server starting", "port", cfg.GRPCPort)
		if err := srv.Serve(lis); err != nil {
			errCh <- fmt.Errorf("grpc serve: %w", err)
		}
	}()

	select {
	case sig := <-quit:
		logger.Info("shutting down", "signal", sig)
		srv.GracefulStop()
		_ = rdb.Close()
	case err := <-errCh:
		return err
	}

	return nil
}

// grpcServerCreds returns mTLS credentials when GRPC_TLS_CERT / GRPC_TLS_KEY
// are set; adds client-cert verification when GRPC_CA_CERT is also set.
func grpcServerCreds(logger *slog.Logger) (credentials.TransportCredentials, bool) {
	certFile := os.Getenv("GRPC_TLS_CERT")
	keyFile := os.Getenv("GRPC_TLS_KEY")
	if certFile == "" || keyFile == "" {
		return nil, false
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		logger.Warn("TLS cert load failed, falling back to insecure", "error", err)
		return nil, false
	}

	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}}

	if caFile := os.Getenv("GRPC_CA_CERT"); caFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err == nil {
			pool := x509.NewCertPool()
			pool.AppendCertsFromPEM(caPEM)
			tlsCfg.ClientCAs = pool
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			logger.Warn("CA cert load failed, skipping client auth", "error", err)
		}
	}

	return credentials.NewTLS(tlsCfg), true
}

// loggingInterceptor logs every unary RPC call with its duration and gRPC status code.
func loggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logger.InfoContext(ctx, "rpc",
			"method", info.FullMethod,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return resp, err
	}
}
