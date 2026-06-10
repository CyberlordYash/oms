// Package processor is the async worker pool that processes accepted orders.
// Orders are pushed onto a bounded channel and drained by a fixed set of
// workers; a full queue is rejected rather than allowed to grow without limit.
package processor

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/yourusername/oms/services/order-service/internal/metrics"
	"github.com/yourusername/oms/services/order-service/internal/natswrapper"
	"github.com/yourusername/oms/services/order-service/repository"
)

const (
	subjectOrderExecuted = "orders.executed"
	subjectOrderRejected = "orders.rejected"
)

var ErrQueueFull = errors.New("processor: order queue full")

type Job struct {
	OrderID  string
	ClientID string
	Symbol   string
	Side     string // "SIDE_BUY" | "SIDE_SELL"
	Quantity int64
}

type Config struct {
	Workers       int
	QueueSize     int
	SubmitTimeout time.Duration
	MinLatency    time.Duration
	MaxLatency    time.Duration
}

type Pool struct {
	cfg     Config
	jobs    chan Job
	repo    *repository.Repository
	pub     natswrapper.Publisher
	metrics *metrics.Registry
	logger  *slog.Logger
	wg      sync.WaitGroup
}

func New(repo *repository.Repository, pub natswrapper.Publisher, m *metrics.Registry, logger *slog.Logger, cfg Config) *Pool {
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 1024
	}
	if cfg.SubmitTimeout <= 0 {
		cfg.SubmitTimeout = 50 * time.Millisecond
	}
	if cfg.MinLatency <= 0 {
		cfg.MinLatency = 5 * time.Millisecond
	}
	if cfg.MaxLatency <= cfg.MinLatency {
		cfg.MaxLatency = cfg.MinLatency + 45*time.Millisecond
	}

	return &Pool{
		cfg:     cfg,
		jobs:    make(chan Job, cfg.QueueSize),
		repo:    repo,
		pub:     pub,
		metrics: m,
		logger:  logger,
	}
}

func (p *Pool) Start() {
	p.wg.Add(p.cfg.Workers)
	for i := 0; i < p.cfg.Workers; i++ {
		go p.worker()
	}
	p.logger.Info("processor pool started", "workers", p.cfg.Workers, "queue", p.cfg.QueueSize)
}

// Submit enqueues a job. If the queue is full for longer than SubmitTimeout it
// gives up with ErrQueueFull instead of blocking the caller indefinitely.
func (p *Pool) Submit(ctx context.Context, j Job) error {
	timer := time.NewTimer(p.cfg.SubmitTimeout)
	defer timer.Stop()

	select {
	case p.jobs <- j:
		p.metrics.IncAccepted()
		return nil
	case <-timer.C:
		p.metrics.IncQueueFull()
		return ErrQueueFull
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop closes the queue and waits for in-flight orders to finish, bounded by ctx.
func (p *Pool) Stop(ctx context.Context) {
	close(p.jobs)

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info("processor pool drained")
	case <-ctx.Done():
		p.logger.Warn("processor pool drain timed out", "error", ctx.Err())
	}
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for j := range p.jobs {
		p.process(j)
	}
}

func (p *Pool) process(j Job) {
	p.metrics.WorkerStarted()
	defer p.metrics.WorkerFinished()

	res := p.fakeColo(j)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.repo.UpdateOrderFill(ctx, j.OrderID, res.status, res.filledQty); err != nil {
		p.logger.ErrorContext(ctx, "failed to persist order outcome",
			"order_id", j.OrderID, "status", res.status, "error", err)
		return
	}

	switch res.status {
	case "EXECUTED":
		p.metrics.RecordExecuted(j.Symbol)
		p.publish(subjectOrderExecuted, map[string]any{
			"order_id":        j.OrderID,
			"client_id":       j.ClientID,
			"symbol":          j.Symbol,
			"side":            j.Side,
			"filled_quantity": res.filledQty,
			"status":          res.status,
		})
	case "REJECTED":
		p.metrics.IncRejected()
		p.publish(subjectOrderRejected, map[string]any{
			"order_id":  j.OrderID,
			"client_id": j.ClientID,
			"symbol":    j.Symbol,
			"reason":    res.reason,
			"status":    res.status,
		})
	}

	p.logger.Info("order processed", "order_id", j.OrderID, "status", res.status, "filled_quantity", res.filledQty)
}

type coloResult struct {
	status    string
	filledQty int64
	reason    string
}

// fakeColo stands in for a real exchange: random latency, then a random outcome
// (~85% filled, ~15% rejected). Replace with an exchange gateway later.
func (p *Pool) fakeColo(j Job) coloResult {
	span := int64(p.cfg.MaxLatency - p.cfg.MinLatency)
	time.Sleep(p.cfg.MinLatency + time.Duration(rand.Int63n(span)))

	if rand.Intn(100) < 15 {
		return coloResult{status: "REJECTED", reason: "colo_random_reject"}
	}

	filled := j.Quantity
	if j.Quantity > 1 && rand.Intn(100) < 20 {
		filled = 1 + rand.Int63n(j.Quantity-1)
	}
	return coloResult{status: "EXECUTED", filledQty: filled}
}

func (p *Pool) publish(subject string, payload map[string]any) {
	data, err := json.Marshal(payload)
	if err != nil {
		p.logger.Error("marshal event", "subject", subject, "error", err)
		return
	}
	if err := p.pub.Publish(subject, data); err != nil {
		p.logger.Warn("nats publish failed", "subject", subject, "error", err)
	}
}
