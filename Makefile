.PHONY: proto run-order-service test docker-up docker-down lint tidy

# ── Protobuf ─────────────────────────────────────────────────────────────────
proto:
	buf generate

proto-lint:
	buf lint

# ── Services ─────────────────────────────────────────────────────────────────
run-order-service:
	go run ./services/order-service/...

# ── Tests ────────────────────────────────────────────────────────────────────
test:
	go test -race -count=1 ./...

# ── Docker ───────────────────────────────────────────────────────────────────
docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-clean:
	docker compose down -v --remove-orphans

# ── Go workspace ─────────────────────────────────────────────────────────────
tidy:
	go work sync
	go mod tidy -C services/order-service

lint:
	golangci-lint run ./...
