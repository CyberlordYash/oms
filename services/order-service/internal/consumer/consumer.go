package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"
)

const (
	streamName    = "ORDERS"
	consumerName  = "position-tracker"
	filterSubject = "orders.executed"
	streamMaxAge  = 24 * time.Hour
)

type executedEvent struct {
	OrderID       string `json:"order_id"`
	ClientID      string `json:"client_id"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	FilledQuantity int64 `json:"filled_quantity"`
	Status        string `json:"status"`
}

type PositionTracker struct {
	js     jetstream.JetStream
	rdb    *redis.Client
	logger *slog.Logger
	cc     jetstream.ConsumeContext
	mu     sync.Mutex
}

func New(js jetstream.JetStream, rdb *redis.Client, logger *slog.Logger) *PositionTracker {
	return &PositionTracker{js: js, rdb: rdb, logger: logger}
}

func (p *PositionTracker) Start(ctx context.Context) error {
	_, err := p.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{"orders.*"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    streamMaxAge,
	})
	if err != nil {
		return fmt.Errorf("create/update ORDERS stream: %w", err)
	}

	stream, err := p.js.Stream(ctx, streamName)
	if err != nil {
		return fmt.Errorf("get ORDERS stream: %w", err)
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        consumerName,
		FilterSubject:  filterSubject,
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  jetstream.DeliverNewPolicy,
		MaxDeliver:     5,
		AckWait:        30 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("create/update position-tracker consumer: %w", err)
	}

	cc, err := consumer.Consume(p.handle)
	if err != nil {
		return fmt.Errorf("start consume: %w", err)
	}

	p.mu.Lock()
	p.cc = cc
	p.mu.Unlock()

	return nil
}

func (p *PositionTracker) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cc != nil {
		p.cc.Stop()
	}
}

func (p *PositionTracker) handle(msg jetstream.Msg) {
	var ev executedEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil {
		p.logger.Warn("position-tracker: unmarshal failed", "error", err)
		_ = msg.Nak()
		return
	}

	if ev.ClientID == "" || ev.Symbol == "" || ev.FilledQuantity <= 0 {
		_ = msg.Ack() // malformed but not retryable
		return
	}

	posKey := fmt.Sprintf("position:%s:%s", ev.ClientID, ev.Symbol)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// BUY increases the held position; SELL decreases it (floored at zero).
	if strings.HasSuffix(ev.Side, "BUY") {
		if err := p.rdb.IncrBy(ctx, posKey, ev.FilledQuantity).Err(); err != nil {
			p.logger.Error("position-tracker: incrby failed", "key", posKey, "error", err)
			_ = msg.Nak()
			return
		}
	} else {
		cur, err := p.rdb.DecrBy(ctx, posKey, ev.FilledQuantity).Result()
		if err != nil {
			p.logger.Error("position-tracker: decrby failed", "key", posKey, "error", err)
			_ = msg.Nak()
			return
		}
		if cur < 0 {
			p.rdb.Set(ctx, posKey, 0, 0)
		}
	}

	p.logger.Info("position updated",
		"client_id", ev.ClientID,
		"symbol", ev.Symbol,
		"side", ev.Side,
		"filled", ev.FilledQuantity,
	)
	_ = msg.Ack()
}
