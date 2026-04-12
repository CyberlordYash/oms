package server

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// loggingInterceptor logs every unary RPC with its duration and status code.
func loggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		code := status.Code(err)
		logger.InfoContext(ctx, "rpc",
			"method", info.FullMethod,
			"code", code.String(),
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return resp, err
	}
}
