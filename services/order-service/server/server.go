// Package server wires together all gRPC service handlers and returns a
// ready-to-serve *grpc.Server.
package server

import (
	"log/slog"

	orderv1 "github.com/yourusername/oms/gen/order/v1"
	"github.com/yourusername/oms/services/order-service/handler"
	"github.com/yourusername/oms/services/order-service/internal/natswrapper"
	"github.com/yourusername/oms/services/order-service/repository"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// New creates a gRPC server with all OMS services registered.
// The caller is responsible for listening on a net.Listener and calling Serve.
func New(
	repo *repository.Repository,
	nats natswrapper.Publisher,
	riskAddr string,
	logger *slog.Logger,
) *grpc.Server {
	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			loggingInterceptor(logger),
		),
	)

	orderHandler := handler.New(repo, nats, riskAddr, logger)
	orderv1.RegisterOrderServiceServer(s, orderHandler)

	// Enable server reflection so tools like grpcurl work out of the box.
	reflection.Register(s)

	return s
}
