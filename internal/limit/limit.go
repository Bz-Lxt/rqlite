// Package limit provides node-local admission control for database
// requests. Requests are split into two classes, Write and Read, and
// each class is gated independently by a bounded number of concurrent
// execution slots and a bounded FIFO wait queue. When both the slots
// and the queue are full, Acquire returns ErrOverloaded immediately
// instead of blocking, so callers can shed load without delay.
//
// A waiter whose context is canceled while still queued is removed at
// once and never executes: Acquire never reports success after the
// context has been canceled, and any slot handed over in the
// cancel/promote race is returned immediately, so capacity is never
// lost.
//
// The zero-value/nil Limiter performs no limiting: all of its methods
// are no-ops, leaving the system behavior unchanged when admission
// control is not configured.
package limit

import (
	"context"
	"errors"
	"expvar"
	"sync"

	"github.com/rqlite/rqlite/v10/command/proto"
)

// Classifier reports the admission class of a set of SQL statements.
// Implementations return Read only if every non-empty statement can be
// proven read-only; any write, unparseable statement, or unavailable
// backing database yields Write.
type Classifier interface {
	Class(stmts []*proto.Statement) Class
}

// ErrOverloaded is returned by Acquire when every concurrency slot is
// occupied and the wait queue has reached its capacity.
var ErrOverloaded = errors.New("admission control: node overloaded")

// Class identifies the admission class of a request.
type Class int

const (
	// Write is the class for requests that modify the database. A
	// mixed request containing any write is classified as Write.
	Write Class = iota
	// Read is the class for read-only requests.
	Read
)

// Config configures a Limiter. A concurrency value of 0 leaves the
// corresponding class unlimited, regardless of the queue value.
// Negative values are treated as 0.
type Config struct {
	WriteConcurrency int
	WriteQueue       int
	ReadConcurrency  int
	ReadQueue        int
}

// stats captures stats for admission control.
var stats *expvar.Map

const (
	writeInFlight    = "write_in_flight"
	writeQueued      = "write_queued"
	readInFlight     = "read_in_flight"
	readQueued       = "read_queued"
	numRejected      = "num_rejected"
	numQueueCanceled = "num_queue_canceled"
)

func init() {
	stats = expvar.NewMap("limit")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test
// purposes.
func ResetStats() {
	stats.Init()
	stats.Add(writeInFlight, 0)
	stats.Add(writeQueued, 0)
	stats.Add(readInFlight, 0)
	stats.Add(readQueued, 0)
	stats.Add(numRejected, 0)
	stats.Add(numQueueCanceled, 0)
}

// waiter is a request parked in the FIFO queue.
type waiter struct {
	ctx context.Context
	ch  chan struct{}

	// promoted is set under the limiter lock when a slot is handed
	// over to this waiter; ch is closed at the same time.
	promoted bool
}

// classState holds the slots and queue for one admission class.
type classState struct {
	tokens     chan struct{} // Buffered channel acting as the slot semaphore.
	queueDepth int           // Maximum number of parked waiters.

	queue []*waiter // FIFO list of parked waiters.
}

// Limiter limits concurrency per read/write class. A nil *Limiter is
// valid and imposes no limits.
type Limiter struct {
	mu     sync.Mutex
	states [2]*classState
}

// New returns a Limiter built from the given configuration. Classes
// with a zero concurrency value are left unlimited.
func New(cfg Config) *Limiter {
	l := &Limiter{}
	l.states[Write] = newClassState(cfg.WriteConcurrency, cfg.WriteQueue)
	l.states[Read] = newClassState(cfg.ReadConcurrency, cfg.ReadQueue)
	return l
}

func newClassState(concurrency, queueDepth int) *classState {
	if concurrency < 0 {
		concurrency = 0
	}
	if queueDepth < 0 {
		queueDepth = 0
	}
	if concurrency == 0 {
		return nil // Unlimited class.
	}
	return &classState{
		tokens:     make(chan struct{}, concurrency),
		queueDepth: queueDepth,
	}
}

// Acquire acquires an execution slot in the given class. If a slot is
// available it returns immediately; otherwise it parks in the FIFO
// queue until promoted or until ctx is canceled. If the queue is also
// full it returns ErrOverloaded without blocking.
//
// On a nil Limiter or an unlimited class it is a no-op.
func (l *Limiter) Acquire(ctx context.Context, c Class) error {
	if l == nil {
		return nil
	}
	cs := l.state(c)
	if cs == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	l.mu.Lock()
	select {
	case cs.tokens <- struct{}{}:
		l.refreshGauges(c)
		l.mu.Unlock()
		return nil
	default:
	}
	if len(cs.queue) >= cs.queueDepth {
		l.mu.Unlock()
		stats.Add(numRejected, 1)
		return ErrOverloaded
	}
	w := &waiter{
		ctx: ctx,
		ch:  make(chan struct{}),
	}
	cs.queue = append(cs.queue, w)
	l.refreshGauges(c)
	l.mu.Unlock()

	select {
	case <-w.ch:
		if ctx.Err() != nil {
			// Promotion and cancel raced. Never report success for a
			// canceled request; hand the slot straight back.
			l.Release(c)
			stats.Add(numQueueCanceled, 1)
			return ctx.Err()
		}
		return nil
	case <-ctx.Done():
		l.mu.Lock()
		if w.promoted {
			// The slot was already handed over; return it.
			l.mu.Unlock()
			l.Release(c)
			stats.Add(numQueueCanceled, 1)
			return ctx.Err()
		}
		l.removeWaiter(cs, w)
		l.refreshGauges(c)
		l.mu.Unlock()
		stats.Add(numQueueCanceled, 1)
		return ctx.Err()
	}
}

// TryAcquire attempts to claim an execution slot without blocking and
// without entering the wait queue. It returns true if a slot was
// claimed, in which case the caller must later call Release; it
// returns false if every slot is currently occupied.
//
// A nil Limiter or an unlimited class always returns true.
func (l *Limiter) TryAcquire(c Class) bool {
	if l == nil {
		return true
	}
	cs := l.state(c)
	if cs == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case cs.tokens <- struct{}{}:
		l.refreshGauges(c)
		return true
	default:
		return false
	}
}

// Release releases an execution slot in the given class. If waiters
// are parked, the slot is handed to the first waiter whose context is
// still alive; canceled waiters at the head are discarded. Must be
// called exactly once per successful Acquire.
//
// On a nil Limiter or an unlimited class it is a no-op.
func (l *Limiter) Release(c Class) {
	if l == nil {
		return
	}
	cs := l.state(c)
	if cs == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	select {
	case <-cs.tokens:
	default:
		// No slot held; avoid blocking the lock. A well-behaved caller
		// never triggers this.
		return
	}

	for len(cs.queue) > 0 {
		w := cs.queue[0]
		cs.queue = cs.queue[1:]
		if w.ctx.Err() != nil {
			// Stale waiter; it will observe its own cancellation.
			continue
		}
		// Hand the freed slot over directly. We just drained a token
		// and the fast path is locked out, so this send cannot block.
		cs.tokens <- struct{}{}
		w.promoted = true
		close(w.ch)
		break
	}
	l.refreshGauges(c)
}

// Stats returns a snapshot of the limiter configuration and current
// occupancy. A nil Limiter reports itself as disabled.
func (l *Limiter) Stats() map[string]any {
	if l == nil {
		return map[string]any{"enabled": false}
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	enabled := false
	classStats := func(c Class) map[string]any {
		cs := l.state(c)
		if cs == nil {
			return map[string]any{"enabled": false}
		}
		enabled = true
		return map[string]any{
			"enabled":     true,
			"concurrency": cap(cs.tokens),
			"queue_depth": cs.queueDepth,
			"in_flight":   len(cs.tokens),
			"queued":      len(cs.queue),
		}
	}
	writeStats := classStats(Write)
	readStats := classStats(Read)
	return map[string]any{
		"enabled": enabled,
		"write":   writeStats,
		"read":    readStats,
	}
}

func (l *Limiter) state(c Class) *classState {
	if c < 0 || int(c) >= len(l.states) {
		return nil
	}
	return l.states[c]
}

// removeWaiter removes w from the queue if still present. Caller must
// hold l.mu.
func (l *Limiter) removeWaiter(cs *classState, w *waiter) {
	for i, x := range cs.queue {
		if x == w {
			cs.queue = append(cs.queue[:i], cs.queue[i+1:]...)
			return
		}
	}
}

// refreshGauges updates the in-flight and queued gauges. Caller must
// hold l.mu.
func (l *Limiter) refreshGauges(c Class) {
	cs := l.state(c)
	if cs == nil {
		return
	}
	var inFlightKey, queuedKey string
	switch c {
	case Write:
		inFlightKey, queuedKey = writeInFlight, writeQueued
	case Read:
		inFlightKey, queuedKey = readInFlight, readQueued
	default:
		return
	}
	gauge := func(key string, v int64) {
		if iv := stats.Get(key); iv != nil {
			iv.(*expvar.Int).Set(v)
		}
	}
	gauge(inFlightKey, int64(len(cs.tokens)))
	gauge(queuedKey, int64(len(cs.queue)))
}
