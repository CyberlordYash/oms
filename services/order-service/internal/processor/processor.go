// Package processor is the asynchronous order-processing worker pool. Accepted
// orders are submitted to a bounded buffered channel and drained by a fixed set
// of worker goroutines, which gives us back-pressure (a full queue is rejected
// rather than growing unbounded) and a clean graceful-drain on shutdown.
//
// Each worker hands the order to a faked "colo" (exchange colocation) that
// returns a random outcome — there is no real exchange integration yet.
package processor

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand"
	"time"

	"github.com/yourusername/oms/services/order-service/internal/metrics"
	"github.com/yourusername/oms/services/order-service/internal/natswrapper"
	"github.com/yourusername/oms/services/order-service/repository"
)

// NATS subjects published by the pool for terminal order outcomes.
const (
	subjectOrderExecuted = "orders.executed"
	subjectOrderRejected = "orders.rejected"
)

// ErrQueueFull is returned by Submit when the queue is saturated and the submit
// timeout elapses. Callers should surface this as a "busy, retry" error.
var ErrQueueFull = errors.New("processor: order queue full")

// Job is a unit of work: an already-persisted (PENDING) order to process.
type Job struct {
	OrderID  string
	Symbol   string
	Quantity int64
}

// Config tunes the pool size, queue depth, submit back-pressure timeout, and the
// simulated colo latency window.
type Config struct {
	Workers       int
	QueueSize     int
	SubmitTimeout time.Duration
	MinLatency    time.Duration
	MaxLatency    time.Duration
}

// Pool is a bounded worker pool that processes orders concurrently.
type Pool struct {
	cfg     Config
	jobs    chan Job
	repo    *repository.Repository
	pub     natswrapper.Publisher
	metrics *metrics.Registry
	logger  *slog.Logger

	// done is closed once all workers have returned, so Stop can wait for a
	// full drain without an explicit WaitGroup leaking outside the pool.
	workerDone chan struct{}
	rng        *rand.Rand
	rngMu      chan struct{} // 1-slot channel used as a mutex for rng access
}

// New constructs a Pool. Call Start to launch the workers.
func New(
	repo *repository.Repository,
	pub natswrapper.Publisher,
	m *metrics.Registry,
	logger *slog.Logger,
	cfg Config,
) *Pool {
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

	rngMu := make(chan struct{}, 1)
	rngMu <- struct{}{} // seed the lock token

	return &Pool{
		cfg:        cfg,
		jobs:       make(chan Job, cfg.QueueSize),
		repo:       repo,
		pub:        pub,
		metrics:    m,
		logger:     logger,
		workerDone: make(chan struct{}),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
		rngMu:      rngMu,
	}
}

// Start launches the worker goroutines. It returns immediately.
func (p *Pool) Start() {
	left := make(chan struct{}, p.cfg.Workers)
	for i := 0; i < p.cfg.Workers; i++ {
		go func(id int) {
			p.worker(id)
			left <- struct{}{}
		}(i)
	}
	// Closer goroutine: once every worker has signalled, close workerDone.
	go func() {
		for i := 0; i < p.cfg.Workers; i++ {
			<-left
		}
		close(p.workerDone)
	}()
	p.logger.Info("processor pool started", "workers", p.cfg.Workers, "queue", p.cfg.QueueSize)
}

// Submit enqueues a job, applying back-pressure: if the queue stays full for the
// configured timeout, it returns ErrQueueFull instead of blocking forever.
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

// Stop closes the job channel and waits for in-flight work to drain, bounded by
// ctx. After Stop returns no further Submit should be called.
func (p *Pool) Stop(ctx context.Context) {
	close(p.jobs)
	select {
	case <-p.workerDone:
		p.logger.Info("processor pool drained")
	case <-ctx.Done():
		p.logger.Warn("processor pool drain timed out", "error", ctx.Err())
	}
}

// worker pulls jobs until the channel is closed and drained.
func (p *Pool) worker(id int) {
	for j := range p.jobs {
		p.process(j)
	}
}

// process runs one order through the fake colo and persists/publishes the result.
func (p *Pool) process(j Job) {
	p.metrics.WorkerStarted()
	defer p.metrics.WorkerFinished()

	res := p.fakeColo(j)

	// Use a short per-job context so a slow DB never wedges a worker forever.
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
			"symbol":          j.Symbol,
			"filled_quantity": res.filledQty,
			"status":          res.status,
		})
	case "REJECTED":
		p.metrics.IncRejected()
		p.publish(subjectOrderRejected, map[string]any{
			"order_id": j.OrderID,
			"symbol":   j.Symbol,
			"reason":   res.reason,
			"status":   res.status,
		})
	}

	p.logger.Info("order processed", "order_id", j.OrderID, "status", res.status,
		"filled_quantity", res.filledQty)
}

// coloResult is the outcome returned by the faked exchange colocation.
type coloResult struct {
	status    string // "EXECUTED" or "REJECTED"
	filledQty int64
	reason    string
}

// fakeColo simulates an exchange round-trip with random latency and a random
// outcome. ~85% of orders fill (fully or partially), ~15% are rejected. There is
// no real exchange — this is a placeholder until an exchange gateway exists.
func (p *Pool) fakeColo(j Job) coloResult {
	// Random latency in [MinLatency, MaxLatency).
	span := p.cfg.MaxLatency - p.cfg.MinLatency
	latency := p.cfg.MinLatency + time.Duration(p.randInt63n(int64(span)))
	time.Sleep(latency)

	if p.randInt63n(100) < 15 {
		return coloResult{status: "REJECTED", reason: "colo_random_reject"}
	}

	// Filled fully 80% of the time, otherwise a random partial fill (>=1).
	filled := j.Quantity
	if j.Quantity > 1 && p.randInt63n(100) < 20 {
		filled = 1 + p.randInt63n(j.Quantity-1)
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

// randInt63n serialises access to the shared *rand.Rand (which is not safe for
// concurrent use) via a 1-slot channel used as a mutex.
func (p *Pool) randInt63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	<-p.rngMu
	v := p.rng.Int63n(n)
	p.rngMu <- struct{}{}
	return v
}
