// Package server wires together all gRPC service handlers and returns a
// ready-to-serve *grpc.Server.
package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"

	orderv1 "github.com/yourusername/oms/gen/order/v1"
	"github.com/yourusername/oms/services/order-service/handler"
	"github.com/yourusername/oms/services/order-service/internal/metrics"
	"github.com/yourusername/oms/services/order-service/internal/natswrapper"
	"github.com/yourusername/oms/services/order-service/internal/processor"
	"github.com/yourusername/oms/services/order-service/repository"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
)

// New creates a gRPC server with all OMS services registered and returns it
// together with the shared OrderHandler (so the same handler also backs HTTP).
// The caller must call orderHandler.Close() on shutdown.
func New(
	repo *repository.Repository,
	nats natswrapper.Publisher,
	pool *processor.Pool,
	m *metrics.Registry,
	riskAddr string,
	logger *slog.Logger,
) (*grpc.Server, *handler.OrderHandler, error) {
	orderHandler, err := handler.New(repo, nats, pool, m, riskAddr, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("create order handler: %w", err)
	}

	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(loggingInterceptor(logger)),
	}
	if creds, ok := grpcServerCreds(); ok {
		opts = append(opts, grpc.Creds(creds))
	}

	s := grpc.NewServer(opts...)
	orderv1.RegisterOrderServiceServer(s, orderHandler)
	reflection.Register(s)

	return s, orderHandler, nil
}

// grpcServerCreds returns mTLS credentials when GRPC_TLS_CERT / GRPC_TLS_KEY
// (and optionally GRPC_CA_CERT for client verification) are set.
func grpcServerCreds() (credentials.TransportCredentials, bool) {
	certFile := os.Getenv("GRPC_TLS_CERT")
	keyFile := os.Getenv("GRPC_TLS_KEY")
	if certFile == "" || keyFile == "" {
		return nil, false
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
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
		}
	}

	return credentials.NewTLS(tlsCfg), true
}
