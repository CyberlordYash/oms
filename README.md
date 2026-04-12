# OMS — Order Management System

A production-grade, microservices Order Management System targeting **Indian equity markets (NSE/BSE)**, built in Go. Designed for low-latency order routing, pre-trade risk validation, and event-driven downstream processing.

---

## Architecture

```
                         ┌─────────────────────────────────────────────────┐
                         │                  Client (gRPC)                  │
                         └──────────────────────┬──────────────────────────┘
                                                │  PlaceOrder / ModifyOrder
                                                │  CancelOrder / GetOrderStatus
                                                ▼
                    ┌───────────────────────────────────────────────┐
                    │              order-service  :50051             │
                    │                                               │
                    │  1. Field validation                          │
                    │  2. ──────────────────────────────────────►  │
                    │     Risk Engine (sync gRPC call)             │
                    │  ◄──────────────────────────────────────────  │
                    │     approved / rejected                       │
                    │  3. INSERT orders (status=PENDING)            │
                    │  4. Publish "orders.placed"                   │
                    └──────┬──────────────┬────────────────────────┘
                           │              │
               ┌───────────▼──┐     ┌────▼──────────────────────┐
               │   Postgres   │     │        NATS JetStream       │
               │  (orders)    │     │  orders.placed             │
               └──────────────┘     │  orders.modified           │
                                    │  orders.cancelled          │
                                    └────┬──────────────────────-┘
                                         │  (consumers — next phase)
                              ┌──────────┼──────────┐
                              ▼          ▼          ▼
                        exchange-   position-   notification-
                         gateway    service      service
                        (FIX/NEAT)


                    ┌───────────────────────────────────────────────┐
                    │              risk-engine  :50052               │
                    │                                               │
                    │  a. Duplicate order check   (Redis SET NX)   │
                    │  b. Daily turnover limit    (₹25L / day)     │
                    │  c. Circuit breaker         (±20% LTP)       │
                    │  d. Position limit          (10,000 qty)     │
                    │  e. Commit daily turnover   (Redis INCRBY)   │
                    └───────────────────┬───────────────────────────┘
                                        │
                                   ┌────▼────┐
                                   │  Redis  │
                                   └─────────┘
```

---

## Services

### order-service
Exposes the `OrderService` gRPC interface. Every `PlaceOrder` call synchronously clears risk before touching the database, ensuring no non-compliant order is ever persisted.

| RPC | Description |
|-----|-------------|
| `PlaceOrder` | Validates → risk check → persist → publish |
| `ModifyOrder` | Guards against terminal-state modification |
| `CancelOrder` | Ownership-verified cancel with NATS event |
| `GetOrderStatus` | Single-order fetch from Postgres |

### risk-engine
A stateless gRPC service backed entirely by Redis. Each `CheckRisk` call runs five ordered checks and short-circuits on the first rejection. All state is in Redis so the service is horizontally scalable with zero coordination.

| Check | Mechanism | Limit |
|-------|-----------|-------|
| Duplicate order | `SET NX PX 500` | 500 ms dedup window |
| Daily turnover | `GET` + threshold | ₹25,00,000 / client / day |
| Circuit breaker | LTP deviation | ±20% from last traded price |
| Position limit | Cumulative qty | 10,000 units / symbol / client |
| Commit | `INCRBY` + `EXPIRENV` pipeline | — |

---

## Tech Stack

| Concern | Choice | Rationale |
|---------|--------|-----------|
| Language | Go 1.24 | Performance, strong concurrency primitives, single binary deploys |
| Service transport | gRPC + protobuf | Strongly typed contracts, efficient binary encoding, built-in reflection |
| Primary store | PostgreSQL 16 (pgx v5) | ACID guarantees for order state; pgx chosen over database/sql for performance |
| Risk state | Redis 7 | Sub-millisecond reads; atomic `SET NX` / `INCRBY` ops map directly to risk check semantics |
| Messaging | NATS 2.10 + JetStream | At-least-once delivery for order events; much lighter than Kafka for this workload |
| Config | Viper (order-service) / stdlib env (risk-engine) | Viper for service with many knobs; stdlib is sufficient for the leaner risk-engine |
| Logging | stdlib `log/slog` (JSON) | Structured, zero-dependency, ships with Go 1.21+ |
| Module system | Go workspaces (`go.work`) | Monorepo with independent modules and no private registry required |
| Containerisation | Docker + Compose | One-command local stack; multi-stage builds for minimal production images (~10 MB) |
| Proto toolchain | buf | Lint, breaking-change detection, remote plugin execution |

---

## Project Structure

```
oms/
├── proto/                    # Protobuf source — single source of truth
│   ├── order/v1/order.proto
│   ├── risk/v1/risk.proto
│   └── common/v1/common.proto
│
├── gen/                      # Generated Go types + gRPC stubs (gitignored, rebuilt via `make proto`)
│
├── pkg/                      # Shared internal libraries
│   ├── db/postgres.go        # pgxpool factory
│   └── nats/client.go        # NATS + JetStream wrapper
│
├── services/
│   ├── order-service/
│   │   ├── handler/          # gRPC method implementations
│   │   ├── repository/       # Postgres data access (pgx)
│   │   ├── server/           # gRPC server wiring + logging interceptor
│   │   └── internal/         # natswrapper interface (enables test fakes)
│   │
│   └── risk-engine/
│       ├── handler/          # CheckRisk — all 5 pre-trade checks
│       └── config/           # Env-based config loader
│
├── buf.yaml / buf.gen.yaml   # Buf configuration
├── docker-compose.yml
├── Makefile
└── .gitignore
```

---

## Design Decisions

**Synchronous risk gate.** The risk engine is called in-line during `PlaceOrder` rather than as an async pre-filter. This guarantees that a rejected order never reaches the database and eliminates an entire class of eventual-consistency bugs around order state.

**Redis for risk state, Postgres for order state.** Risk checks are read-heavy, time-sensitive, and tolerate eventual persistence — Redis is the right fit. Orders require durable ACID writes — Postgres is the right fit. Using the same store for both would be the wrong tradeoff.

**Separate gRPC service for risk.** The risk engine is deployed and scaled independently from order processing. Risk policy changes (new checks, updated limits) can be deployed without touching order-service. The gRPC interface provides a hard contract between the two.

**`natswrapper.Publisher` interface.** The order handler depends on a one-method interface rather than the concrete NATS client. This makes the handler trivially unit-testable with a fake publisher — no embedded broker needed in tests.

**Multi-stage Docker builds.** The builder stage uses `golang:1.25-alpine`; the runtime stage uses `alpine:3.21`. Final image is ~15 MB. The Dockerfile copies `go.work` and all module manifests before source so the dependency download layer is cached independently of code changes.

**Go workspaces over a single module.** `gen`, `pkg`, `order-service`, and `risk-engine` are separate modules linked via `go.work`. This mirrors how the services would be structured if they were ever split into separate repositories, and keeps dependency graphs clean and independently auditable.

---

## Getting Started

### Prerequisites
- Docker Desktop
- Go 1.24+
- `buf` CLI (`brew install bufbuild/buf/buf`) — only needed to regenerate protos

### Run the full stack

```bash
# Start all infrastructure + both services
docker compose up -d --build

# Verify everything is healthy
docker compose ps
```

### Run services locally (for development)

```bash
# Start infra only
docker compose up -d postgres redis nats

# Terminal 1 — risk engine (must start first, order-service dials it)
make run-risk-engine

# Terminal 2 — order service
make run-order-service
```

### Regenerate protobuf code

```bash
make proto        # runs buf generate → writes to gen/
```

### Other Makefile targets

```bash
make test         # go test -race ./...
make tidy         # go work sync + go mod tidy for all modules
make docker-down  # stop containers
make docker-clean # stop containers + delete volumes
```

---

## API

The gRPC server runs on `:50051` with server reflection enabled. You can explore it with [`grpcurl`](https://github.com/fullstorydev/grpcurl):

```bash
# List services
grpcurl -plaintext localhost:50051 list

# Place an order
grpcurl -plaintext -d '{
  "symbol":     "RELIANCE",
  "exchange":   "EXCHANGE_NSE",
  "side":       "SIDE_BUY",
  "order_type": "ORDER_TYPE_LIMIT",
  "quantity":   10,
  "price":      250000,
  "client_id":  "client-001"
}' localhost:50051 order.v1.OrderService/PlaceOrder
```

> Prices are in **paise** (₹1 = 100 paise). ₹2500.00 → `250000`.

---

## NATS Events

| Subject | Published when |
|---------|----------------|
| `orders.placed` | Order persisted with status `PENDING` |
| `orders.modified` | Modify request submitted to exchange |
| `orders.cancelled` | Order cancelled and status updated |

JetStream is enabled. Durable consumers (exchange gateway, position service, notifications) are the next phase of development.

---

## Environment Variables

### order-service

| Variable | Default | Docker override |
|----------|---------|-----------------|
| `GRPC_PORT` | `50051` | `50051` |
| `DB_HOST` | `localhost` | `postgres` |
| `DB_PORT` | `5433` | `5432` |
| `DB_USER` | `oms` | `oms` |
| `DB_PASSWORD` | `oms_secret` | `oms_secret` |
| `DB_NAME` | `oms` | `oms` |
| `NATS_URL` | `nats://localhost:4223` | `nats://nats:4222` |
| `RISK_ADDR` | `localhost:50052` | `risk-engine:50052` |

### risk-engine

| Variable | Default | Docker override |
|----------|---------|-----------------|
| `GRPC_PORT` | `50052` | `50052` |
| `REDIS_ADDR` | `localhost:6379` | `redis:6379` |

---

## Roadmap

- [ ] Database migrations (golang-migrate or goose)
- [ ] Exchange gateway service — consumes `orders.placed`, routes to NSE/BSE
- [ ] Position service — maintains real-time position state in Redis
- [ ] Persistent gRPC connection from order-service to risk-engine
- [ ] Market data feed — populates `last_price:{symbol}` for circuit breaker
- [ ] mTLS between services
- [ ] Integration test suite with testcontainers
- [ ] Prometheus metrics + Grafana dashboard
- [ ] risk-engine added to docker-compose
