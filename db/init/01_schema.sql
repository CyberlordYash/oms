-- Auto-applied by the postgres image on first boot (mounted into
-- /docker-entrypoint-initdb.d). Keeps `docker compose up` turnkey until a real
-- migration tool (golang-migrate / goose) is wired in.

CREATE TABLE IF NOT EXISTS orders (
    id              VARCHAR(36)  PRIMARY KEY,
    client_id       VARCHAR(255) NOT NULL,
    symbol          VARCHAR(32)  NOT NULL,
    exchange        VARCHAR(16)  NOT NULL,           -- NSE | BSE
    side            VARCHAR(8)   NOT NULL,           -- BUY | SELL
    order_type      VARCHAR(16)  NOT NULL,           -- MARKET | LIMIT | SL | SL_M | AMO
    quantity        BIGINT       NOT NULL,
    filled_quantity BIGINT       NOT NULL DEFAULT 0,
    price           BIGINT       NOT NULL DEFAULT 0, -- in paise
    trigger_price   BIGINT       NOT NULL DEFAULT 0, -- in paise
    status          VARCHAR(16)  NOT NULL,           -- PENDING | OPEN | EXECUTED | CANCELLED | REJECTED
    created_at      TIMESTAMPTZ  NOT NULL,
    updated_at      TIMESTAMPTZ  NOT NULL
);

-- Common lookup paths.
CREATE INDEX IF NOT EXISTS idx_orders_client_id ON orders (client_id);
CREATE INDEX IF NOT EXISTS idx_orders_status    ON orders (status);
