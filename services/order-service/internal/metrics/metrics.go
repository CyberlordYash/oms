// Package metrics holds lightweight, concurrency-safe counters for the order
// pipeline. It deliberately mixes two synchronization styles to use each where
// it fits best:
//
//   - atomic.Int64 for single scalar counters touched on every order by many
//     worker goroutines (lock-free, no contention).
//   - an RWMutex-guarded map for the per-symbol breakdown, because a Go map
//     cannot be mutated concurrently without a lock.
package metrics

import (
	"sync"
	"sync/atomic"
)

// Registry aggregates runtime counters for the order pipeline.
type Registry struct {
	// Hot-path scalar counters — accessed by every worker, so they are atomic.
	accepted  atomic.Int64 // orders admitted to the worker pool
	executed  atomic.Int64 // orders the (fake) colo filled
	rejected  atomic.Int64 // orders rejected (risk or colo)
	queueFull atomic.Int64 // submits dropped because the queue was saturated
	inFlight  atomic.Int64 // orders currently being processed by a worker

	// Composite breakdown — a map needs a lock; reads (snapshots) are rarer
	// than writes, so an RWMutex lets concurrent workers read cheaply when
	// they don't write.
	mu       sync.RWMutex
	bySymbol map[string]int64 // executed count per symbol
}

// New returns an initialised Registry.
func New() *Registry {
	return &Registry{bySymbol: make(map[string]int64)}
}

func (r *Registry) IncAccepted()  { r.accepted.Add(1) }
func (r *Registry) IncRejected()  { r.rejected.Add(1) }
func (r *Registry) IncQueueFull() { r.queueFull.Add(1) }

// WorkerStarted / WorkerFinished bracket a unit of work so in-flight reflects
// the number of orders currently held by workers.
func (r *Registry) WorkerStarted()  { r.inFlight.Add(1) }
func (r *Registry) WorkerFinished() { r.inFlight.Add(-1) }

// RecordExecuted increments the executed total and the per-symbol breakdown.
func (r *Registry) RecordExecuted(symbol string) {
	r.executed.Add(1)
	r.mu.Lock()
	r.bySymbol[symbol]++
	r.mu.Unlock()
}

// Snapshot is a point-in-time, JSON-serialisable view of the counters.
type Snapshot struct {
	Accepted  int64            `json:"accepted"`
	Executed  int64            `json:"executed"`
	Rejected  int64            `json:"rejected"`
	QueueFull int64            `json:"queue_full"`
	InFlight  int64            `json:"in_flight"`
	BySymbol  map[string]int64 `json:"by_symbol"`
}

// Snapshot returns a consistent copy of all counters.
func (r *Registry) Snapshot() Snapshot {
	r.mu.RLock()
	bySymbol := make(map[string]int64, len(r.bySymbol))
	for k, v := range r.bySymbol {
		bySymbol[k] = v
	}
	r.mu.RUnlock()

	return Snapshot{
		Accepted:  r.accepted.Load(),
		Executed:  r.executed.Load(),
		Rejected:  r.rejected.Load(),
		QueueFull: r.queueFull.Load(),
		InFlight:  r.inFlight.Load(),
		BySymbol:  bySymbol,
	}
}
