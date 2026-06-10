.PHONY: proto run-order-service run-risk-engine test docker-up docker-down lint tidy gen-certs db-connect db-migrate

# ── Protobuf ─────────────────────────────────────────────────────────────────
proto:
	buf generate

proto-lint:
	buf lint

# ── Services ─────────────────────────────────────────────────────────────────
run-order-service:
	go run ./services/order-service/...

run-risk-engine:
	go run ./services/risk-engine/...

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

# ── Database ──────────────────────────────────────────────────────────────────
db-connect:
	docker exec -it oms-postgres psql -U oms -d oms

db-migrate:
	docker exec -i oms-postgres psql -U oms -d oms < db/init/01_schema.sql

# ── Go workspace ─────────────────────────────────────────────────────────────
tidy:
	go work sync
	go mod tidy -C services/order-service
	go mod tidy -C services/risk-engine
	go mod tidy -C services/market-data-sim

lint:
	golangci-lint run ./...

gen-certs:
	go run ./certs/gen.go
