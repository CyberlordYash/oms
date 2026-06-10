package metrics

import (
	"sync"
	"sync/atomic"
)

type Registry struct {
	accepted  atomic.Int64
	executed  atomic.Int64
	rejected  atomic.Int64
	queueFull atomic.Int64
	inFlight  atomic.Int64

	mu       sync.RWMutex
	bySymbol map[string]int64
}

func New() *Registry {
	return &Registry{bySymbol: make(map[string]int64)}
}

func (r *Registry) IncAccepted()  { r.accepted.Add(1) }
func (r *Registry) IncRejected()  { r.rejected.Add(1) }
func (r *Registry) IncQueueFull() { r.queueFull.Add(1) }

func (r *Registry) WorkerStarted()  { r.inFlight.Add(1) }
func (r *Registry) WorkerFinished() { r.inFlight.Add(-1) }

func (r *Registry) RecordExecuted(symbol string) {
	r.executed.Add(1)
	r.mu.Lock()
	r.bySymbol[symbol]++
	r.mu.Unlock()
}

type Snapshot struct {
	Accepted  int64            `json:"accepted"`
	Executed  int64            `json:"executed"`
	Rejected  int64            `json:"rejected"`
	QueueFull int64            `json:"queue_full"`
	InFlight  int64            `json:"in_flight"`
	BySymbol  map[string]int64 `json:"by_symbol"`
}

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
