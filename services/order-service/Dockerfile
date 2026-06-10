# ── Stage 1: build ───────────────────────────────────────────────────────────
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /workspace

# Copy go.work so the builder knows the module layout
COPY go.work go.work.sum ./

# Copy each module's dependency manifests first (better layer caching)
COPY gen/go.mod      gen/go.sum      ./gen/
COPY pkg/go.mod      pkg/go.sum      ./pkg/
COPY services/order-service/go.mod \
     services/order-service/go.sum      ./services/order-service/
COPY services/risk-engine/go.mod \
     services/risk-engine/go.sum        ./services/risk-engine/
COPY services/market-data-sim/go.mod \
     services/market-data-sim/go.sum    ./services/market-data-sim/

# Download all dependencies (cached unless go.mod/go.sum change)
RUN go work sync

# Copy source
COPY gen/       ./gen/
COPY pkg/       ./pkg/
COPY services/order-service/ ./services/order-service/

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build \
      -ldflags="-s -w" \
      -o /order-service \
      ./services/order-service

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM alpine:3.21

# ca-certificates for TLS; nc (netcat) for the healthcheck in docker-compose
RUN apk add --no-cache ca-certificates netcat-openbsd

COPY --from=builder /order-service /order-service

EXPOSE 50051 8080

ENTRYPOINT ["/order-service"]
