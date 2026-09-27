package limit

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// parkedAcquire starts an Acquire in the background. The returned
// cancel aborts the waiter's context.
func parkedAcquire(l *Limiter, c Class) (<-chan error, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() {
		ch <- l.Acquire(ctx, c)
	}()
	return ch, cancel
}

func assertNotReturned(t *testing.T, ch <-chan error, msg string) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("%s: acquire returned unexpectedly with %v", msg, err)
	case <-time.After(50 * time.Millisecond):
	}
}

func assertReturned(t *testing.T, ch <-chan error, want error, msg string) error {
	t.Helper()
	select {
	case err := <-ch:
		if want != nil && !errors.Is(err, want) {
			t.Fatalf("%s: got %v, want %v", msg, err, want)
		}
		return err
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: acquire did not return", msg)
		return nil
	}
}

func gaugeValue(key string) int64 {
	return stats.Get(key).(*expvar.Int).Value()
}

func TestNilLimiter(t *testing.T) {
	var l *Limiter
	if err := l.Acquire(context.Background(), Write); err != nil {
		t.Fatalf("nil limiter Acquire: %v", err)
	}
	l.Release(Read) // must not panic
}

func TestUnlimitedClass(t *testing.T) {
	// Only writes limited; reads must be unlimited.
	l := New(Config{WriteConcurrency: 1})
	for i := 0; i < 16; i++ {
		if err := l.Acquire(context.Background(), Read); err != nil {
			t.Fatalf("read acquire %d: %v", i, err)
		}
	}
}

func TestNegativeConfigNormalized(t *testing.T) {
	l := New(Config{WriteConcurrency: -1, ReadConcurrency: 2, ReadQueue: -3})
	if l.states[Write] != nil {
		t.Fatalf("negative write concurrency should be unlimited")
	}
	if err := l.Acquire(context.Background(), Read); err != nil {
		t.Fatalf("read acquire: %v", err)
	}
}

func TestConcurrencySlotsAndQueue(t *testing.T) {
	ResetStats()
	l := New(Config{WriteConcurrency: 3, WriteQueue: 2})

	for i := 0; i < 3; i++ {
		if err := l.Acquire(context.Background(), Write); err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
	}
	if got, want := gaugeValue(writeInFlight), int64(3); got != want {
		t.Fatalf("in-flight gauge: got %d want %d", got, want)
	}

	// Two waiters park in the queue.
	w1, cancel1 := parkedAcquire(l, Write)
	defer cancel1()
	assertNotReturned(t, w1, "waiter 1")
	if got, want := gaugeValue(writeQueued), int64(1); got != want {
		t.Fatalf("queued gauge: got %d want %d", got, want)
	}

	w2, cancel2 := parkedAcquire(l, Write)
	defer cancel2()
	assertNotReturned(t, w2, "waiter 2")
	if got, want := gaugeValue(writeQueued), int64(2); got != want {
		t.Fatalf("queued gauge: got %d want %d", got, want)
	}

	// Queue and slots full: immediate rejection.
	start := time.Now()
	w3, cancel3 := parkedAcquire(l, Write)
	defer cancel3()
	assertReturned(t, w3, ErrOverloaded, "waiter 3 overloaded")
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("overload rejection took %s, should be immediate", elapsed)
	}
	if got, want := gaugeValue(numRejected), int64(1); got != want {
		t.Fatalf("rejected counter: got %d want %d", got, want)
	}

	// Release one slot; waiter 1 is promoted.
	l.Release(Write)
	assertReturned(t, w1, nil, "waiter 1 promoted")
	assertNotReturned(t, w2, "waiter 2 still queued")
	if got, want := gaugeValue(writeQueued), int64(1); got != want {
		t.Fatalf("queued gauge after promote: got %d want %d", got, want)
	}

	// Release again; waiter 2 is promoted.
	l.Release(Write)
	assertReturned(t, w2, nil, "waiter 2 promoted")
	if got, want := gaugeValue(writeInFlight), int64(3); got != want {
		t.Fatalf("in-flight after promotions: got %d want %d", got, want)
	}

	// Drain everything; gauges must return to zero.
	for i := 0; i < 3; i++ {
		l.Release(Write)
	}
	if got := gaugeValue(writeInFlight); got != 0 {
		t.Fatalf("in-flight after drain: %d", got)
	}
	if got := gaugeValue(writeQueued); got != 0 {
		t.Fatalf("queued after drain: %d", got)
	}
}

func TestTryAcquire(t *testing.T) {
	ResetStats()

	var nilL *Limiter
	if !nilL.TryAcquire(Write) {
		t.Fatalf("nil limiter TryAcquire should report true")
	}

	// An unlimited class is always acquirable.
	unl := New(Config{ReadConcurrency: 1})
	if !unl.TryAcquire(Write) {
		t.Fatalf("unlimited class TryAcquire should report true")
	}

	l := New(Config{WriteConcurrency: 2})
	if !l.TryAcquire(Write) {
		t.Fatalf("first TryAcquire should succeed")
	}
	if !l.TryAcquire(Write) {
		t.Fatalf("second TryAcquire should succeed")
	}
	if l.TryAcquire(Write) {
		t.Fatalf("third TryAcquire should fail with slots full")
	}
	if got, want := gaugeValue(writeInFlight), int64(2); got != want {
		t.Fatalf("in-flight gauge: got %d want %d", got, want)
	}
	// TryAcquire never enters the queue.
	if got := gaugeValue(writeQueued); got != 0 {
		t.Fatalf("queued should be zero, got %d", got)
	}

	// The other class is unaffected.
	if !l.TryAcquire(Read) {
		t.Fatalf("read TryAcquire should remain available")
	}

	// After a release the slot is claimable again.
	l.Release(Read)
	l.Release(Write)
	if !l.TryAcquire(Write) {
		t.Fatalf("TryAcquire after Release should succeed")
	}
	l.Release(Write)
}

func TestQueueCancel(t *testing.T) {
	ResetStats()
	l := New(Config{WriteConcurrency: 1, WriteQueue: 1})

	if err := l.Acquire(context.Background(), Write); err != nil {
		t.Fatalf("slot acquire: %v", err)
	}

	w, cancel := parkedAcquire(l, Write)
	assertNotReturned(t, w, "parked waiter")

	// Cancel while queued.
	cancel()
	assertReturned(t, w, context.Canceled, "canceled waiter")
	if got := gaugeValue(writeQueued); got != 0 {
		t.Fatalf("queued after cancel: %d", got)
	}
	if got, want := gaugeValue(numQueueCanceled), int64(1); got != want {
		t.Fatalf("canceled counter: got %d want %d", got, want)
	}

	// A fresh waiter must be able to take the queue slot and be
	// promoted, proving no capacity was lost.
	w2, cancel2 := parkedAcquire(l, Write)
	defer cancel2()
	assertNotReturned(t, w2, "fresh waiter parked")
	l.Release(Write)
	assertReturned(t, w2, nil, "fresh waiter promoted")
	l.Release(Write)

	// Repeated cancellation cannot shrink the concurrency capacity.
	for i := 0; i < 20; i++ {
		ctx, cancel3 := context.WithCancel(context.Background())
		cancel3() // Cancel before Acquire starts.
		ch := make(chan error, 1)
		go func() { ch <- l.Acquire(ctx, Write) }()
		assertReturned(t, ch, context.Canceled, fmt.Sprintf("iter %d canceled", i))
	}
	if err := l.Acquire(context.Background(), Write); err != nil {
		t.Fatalf("slot not usable after repeated cancels: %v", err)
	}
	l.Release(Write)
}

func TestCanceledWaiterSkippedAtHead(t *testing.T) {
	ResetStats()
	l := New(Config{WriteConcurrency: 1, WriteQueue: 2})
	if err := l.Acquire(context.Background(), Write); err != nil {
		t.Fatalf("slot acquire: %v", err)
	}

	w1, cancel1 := parkedAcquire(l, Write)
	w2, cancel2 := parkedAcquire(l, Write)
	defer cancel2()
	assertNotReturned(t, w1, "waiter 1 parked")
	assertNotReturned(t, w2, "waiter 2 parked")

	// Cancel the head waiter; releasing the slot must skip it and
	// promote waiter 2 directly.
	cancel1()
	assertReturned(t, w1, context.Canceled, "waiter 1 canceled")

	l.Release(Write)
	assertReturned(t, w2, nil, "waiter 2 promoted despite canceled head")
	l.Release(Write)
}

func TestFIFOOrder(t *testing.T) {
	l := New(Config{WriteConcurrency: 1, WriteQueue: 2})
	if err := l.Acquire(context.Background(), Write); err != nil {
		t.Fatalf("slot acquire: %v", err)
	}

	order := make(chan int, 2)
	mk := func(i int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		order <- i + 1
		if err := l.Acquire(ctx, Write); err != nil {
			t.Errorf("waiter %d: %v", i, err)
		}
		order <- i
	}
	go mk(0)
	<-order // waiter 0 parked
	go mk(1)
	<-order // waiter 1 parked

	l.Release(Write)
	if got := <-order; got != 0 {
		t.Fatalf("first promoted: got %d want 0", got)
	}
	l.Release(Write)
	if got := <-order; got != 1 {
		t.Fatalf("second promoted: got %d want 1", got)
	}
	l.Release(Write)
}

func TestReadWriteIndependence(t *testing.T) {
	ResetStats()
	l := New(Config{WriteConcurrency: 1, WriteQueue: 0, ReadConcurrency: 1, ReadQueue: 0})

	if err := l.Acquire(context.Background(), Write); err != nil {
		t.Fatalf("write slot: %v", err)
	}
	// Writes saturated: a read still gets its own slot.
	if err := l.Acquire(context.Background(), Read); err != nil {
		t.Fatalf("read slot despite saturated writes: %v", err)
	}
	// Read saturated too: second read rejected, but classes independent.
	r, cancel := parkedAcquire(l, Read)
	defer cancel()
	assertReturned(t, r, ErrOverloaded, "second read rejected")

	l.Release(Write)
	l.Release(Read)
}

func TestPromoteCancelRaceStress(t *testing.T) {
	ResetStats()
	l := New(Config{WriteConcurrency: 8, WriteQueue: 16})

	var (
		success  int64
		canceled int64
		rejected int64
		released int64
	)
	const workers = 400
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			res := make(chan error, 1)
			go func() { res <- l.Acquire(ctx, Write) }()

			// Randomly cancel before promotion, or after the call
			// returns.
			if i%3 == 0 {
				time.Sleep(time.Duration(rand.Intn(2)) * time.Millisecond)
				cancel()
			}
			err := <-res
			switch {
			case err == nil:
				atomic.AddInt64(&success, 1)
				time.Sleep(time.Duration(rand.Intn(2)) * time.Millisecond)
				l.Release(Write)
				atomic.AddInt64(&released, 1)
				cancel()
			case errors.Is(err, ErrOverloaded):
				atomic.AddInt64(&rejected, 1)
				cancel()
			default: // context canceled
				atomic.AddInt64(&canceled, 1)
			}
		}(i)
	}
	wg.Wait()

	if success != released {
		t.Fatalf("every successful acquire must be released: success %d released %d", success, released)
	}
	if got := gaugeValue(writeQueued); got != 0 {
		t.Fatalf("queued not zero after stress: %d", got)
	}

	// Full concurrency must still be available: no leaked slots.
	for i := 0; i < 8; i++ {
		if err := l.Acquire(context.Background(), Write); err != nil {
			t.Fatalf("capacity shrunk after stress: %v", err)
		}
	}
	if got := gaugeValue(writeInFlight); got != 8 {
		t.Fatalf("in-flight: got %d want 8", got)
	}
	for i := 0; i < 8; i++ {
		l.Release(Write)
	}
}

func TestStats(t *testing.T) {
	if got := (*Limiter)(nil).Stats()["enabled"]; got != false {
		t.Fatalf("nil stats enabled: %v", got)
	}
	l := New(Config{WriteConcurrency: 2, WriteQueue: 1, ReadConcurrency: 3})
	if err := l.Acquire(context.Background(), Write); err != nil {
		t.Fatalf("write acquire: %v", err)
	}
	st := l.Stats()
	if st["enabled"] != true {
		t.Fatalf("enabled: %v", st["enabled"])
	}
	ws := st["write"].(map[string]any)
	if ws["in_flight"] != 1 || ws["concurrency"] != 2 || ws["queue_depth"] != 1 {
		t.Fatalf("write stats: %v", ws)
	}
	rs := st["read"].(map[string]any)
	if rs["enabled"] != true || rs["concurrency"] != 3 {
		t.Fatalf("read stats: %v", rs)
	}
	l.Release(Write)
}
