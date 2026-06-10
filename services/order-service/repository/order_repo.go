package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Order struct {
	ID             string
	ClientID       string
	Symbol         string
	Exchange       string
	Side           string
	OrderType      string
	Quantity       int64
	FilledQuantity int64
	Price          int64  // in paise
	TriggerPrice   int64  // in paise; 0 if not applicable
	Status         string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type Repository struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) CreateOrder(ctx context.Context, o Order) (Order, error) {
	const q = `
		INSERT INTO orders (
			id, client_id, symbol, exchange, side, order_type,
			quantity, filled_quantity, price, trigger_price, status,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11,
			$12, $13
		)
		RETURNING id, client_id, symbol, exchange, side, order_type,
		          quantity, filled_quantity, price, trigger_price, status,
		          created_at, updated_at`

	now := time.Now().UTC()
	o.CreatedAt = now
	o.UpdatedAt = now

	row := r.pool.QueryRow(ctx, q,
		o.ID, o.ClientID, o.Symbol, o.Exchange, o.Side, o.OrderType,
		o.Quantity, o.FilledQuantity, o.Price, o.TriggerPrice, o.Status,
		o.CreatedAt, o.UpdatedAt,
	)

	var out Order
	if err := row.Scan(
		&out.ID, &out.ClientID, &out.Symbol, &out.Exchange, &out.Side, &out.OrderType,
		&out.Quantity, &out.FilledQuantity, &out.Price, &out.TriggerPrice, &out.Status,
		&out.CreatedAt, &out.UpdatedAt,
	); err != nil {
		return Order{}, fmt.Errorf("repo: create order: %w", err)
	}

	return out, nil
}

func (r *Repository) UpdateOrderStatus(ctx context.Context, orderID, status string) error {
	const q = `
		UPDATE orders
		SET    status = $1, updated_at = $2
		WHERE  id = $3`

	tag, err := r.pool.Exec(ctx, q, status, time.Now().UTC(), orderID)
	if err != nil {
		return fmt.Errorf("repo: update order status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("repo: update order status: order %s not found", orderID)
	}
	return nil
}

func (r *Repository) UpdateOrderFill(ctx context.Context, orderID, status string, filledQty int64) error {
	const q = `
		UPDATE orders
		SET    status = $1, filled_quantity = $2, updated_at = $3
		WHERE  id = $4`

	tag, err := r.pool.Exec(ctx, q, status, filledQty, time.Now().UTC(), orderID)
	if err != nil {
		return fmt.Errorf("repo: update order fill: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("repo: update order fill: order %s not found", orderID)
	}
	return nil
}

func (r *Repository) GetOrderByID(ctx context.Context, orderID string) (Order, error) {
	const q = `
		SELECT id, client_id, symbol, exchange, side, order_type,
		       quantity, filled_quantity, price, trigger_price, status,
		       created_at, updated_at
		FROM   orders
		WHERE  id = $1`

	row := r.pool.QueryRow(ctx, q, orderID)

	var o Order
	if err := row.Scan(
		&o.ID, &o.ClientID, &o.Symbol, &o.Exchange, &o.Side, &o.OrderType,
		&o.Quantity, &o.FilledQuantity, &o.Price, &o.TriggerPrice, &o.Status,
		&o.CreatedAt, &o.UpdatedAt,
	); err != nil {
		return Order{}, fmt.Errorf("repo: get order by id %s: %w", orderID, err)
	}

	return o, nil
}
