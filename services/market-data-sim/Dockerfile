# ── Stage 1: build ───────────────────────────────────────────────────────────
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /workspace

COPY go.work go.work.sum ./

COPY gen/go.mod             gen/go.sum             ./gen/
COPY pkg/go.mod             pkg/go.sum             ./pkg/
COPY services/order-service/go.mod \
     services/order-service/go.sum                 ./services/order-service/
COPY services/risk-engine/go.mod \
     services/risk-engine/go.sum                   ./services/risk-engine/
COPY services/market-data-sim/go.mod \
     services/market-data-sim/go.sum               ./services/market-data-sim/

RUN go work sync

COPY services/market-data-sim/ ./services/market-data-sim/

RUN CGO_ENABLED=0 GOOS=linux go build \
      -ldflags="-s -w" \
      -o /market-data-sim \
      ./services/market-data-sim

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /market-data-sim /market-data-sim

ENTRYPOINT ["/market-data-sim"]
