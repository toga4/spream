package spream

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

// inflightTracker tracks in-flight records within a partition.
type inflightTracker struct {
	mu sync.Mutex

	// Semaphore for backpressure control.
	sem *semaphore.Weighted

	// Sequence management.
	nextSeq int64

	// State tracking (acked flag integrated into pending).
	pending map[int64]*pendingRecord

	// Continuous ack tracking.
	lastContinuousAcked int64
	safeWatermark       time.Time

	// Event notification channels (separated for normal and error cases).
	watermarks chan time.Time
	errors     chan error

	// Stop accepting new records (blocks acquire).
	closed atomic.Bool

	// Graceful shutdown.
	// When shutdown=true and pending=0, done is closed.
	shutdown atomic.Bool

	// For waitAllCompleted.
	done      chan struct{}
	closeDone func()
}

type pendingRecord struct {
	seq       int64
	timestamp time.Time
	acked     bool
}

// newInflightTracker creates a new inflightTracker.
func newInflightTracker(maxInflight int) *inflightTracker {
	t := &inflightTracker{
		sem:                 semaphore.NewWeighted(int64(maxInflight)),
		pending:             make(map[int64]*pendingRecord),
		lastContinuousAcked: -1, // Sequence numbers start from 0.
		watermarks:          make(chan time.Time, maxInflight),
		errors:              make(chan error, maxInflight),
		done:                make(chan struct{}),
	}
	// sync.OnceFunc creates a function that executes close(t.done) only once.
	t.closeDone = sync.OnceFunc(func() { close(t.done) })
	return t
}

// acquire acquires the semaphore (MaxInflight control).
// Blocks if in-flight count has reached maxInflight.
// semaphore.Weighted handles context cancellation automatically.
func (t *inflightTracker) acquire(ctx context.Context) error {
	if t.closed.Load() {
		return errTrackerClosed
	}
	return t.sem.Acquire(ctx, 1)
}

// add adds a new record to in-flight and returns the sequence number.
func (t *inflightTracker) add(timestamp time.Time) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	seq := t.nextSeq
	t.nextSeq++

	t.pending[seq] = &pendingRecord{
		seq:       seq,
		timestamp: timestamp,
	}

	return seq
}

// complete notifies the completion of record processing and emits events as needed.
// This function is called from goroutines (for DataChangeRecord).
func (t *inflightTracker) complete(seq int64, err error) {
	t.mu.Lock()

	var newWatermark time.Time

	if err != nil {
		// On error: don't mark as acked (continuous ack won't advance).
		// Only release semaphore.
	} else {
		// On success: ack processing and continuous ack calculation.
		if rec, ok := t.pending[seq]; ok {
			rec.acked = true
		}
		newWatermark = t.advanceWatermark()
	}

	// Release semaphore.
	t.sem.Release(1)

	t.mu.Unlock()

	// Send to channel after releasing mutex (prevents deadlock).
	if err != nil {
		t.errors <- err
	} else if !newWatermark.IsZero() {
		t.watermarks <- newWatermark
	}
}

// ackImmediate is for records that ack immediately (HeartbeatRecord, ChildPartitionsRecord).
// Does not use semaphore (no goroutine is spawned).
func (t *inflightTracker) ackImmediate(timestamp time.Time) {
	t.mu.Lock()

	seq := t.nextSeq
	t.nextSeq++

	t.pending[seq] = &pendingRecord{
		seq:       seq,
		timestamp: timestamp,
		acked:     true, // Immediately acked.
	}

	newWatermark := t.advanceWatermark()

	t.mu.Unlock()

	if !newWatermark.IsZero() {
		t.watermarks <- newWatermark
	}
}

// advanceWatermark calculates continuous ack and advances the watermark.
// Must be called while holding mu.Lock().
// Returns: updated watermark (zero value if no update).
func (t *inflightTracker) advanceWatermark() time.Time {
	var watermark time.Time

	for {
		nextExpected := t.lastContinuousAcked + 1
		rec, ok := t.pending[nextExpected]
		if !ok || !rec.acked {
			break
		}

		if rec.timestamp.After(t.safeWatermark) {
			t.safeWatermark = rec.timestamp
			watermark = t.safeWatermark
		}

		t.lastContinuousAcked = nextExpected
		delete(t.pending, nextExpected)
	}

	// Close done if pending becomes 0 during shutdown phase.
	if t.shutdown.Load() && len(t.pending) == 0 {
		t.closeDone()
	}

	return watermark
}

// initiateShutdown starts graceful shutdown.
// Call after readStream() has finished.
// After this, done is closed when pending becomes 0.
func (t *inflightTracker) initiateShutdown() {
	t.shutdown.Store(true)

	// Close done immediately if pending is already 0.
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.pending) == 0 {
		t.closeDone()
	}
}

// getSafeWatermark returns the continuously acked watermark.
func (t *inflightTracker) getSafeWatermark() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.safeWatermark
}

// waitAllCompleted waits until all in-flight records are completed.
func (t *inflightTracker) waitAllCompleted(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.done:
		return nil
	}
}

// close closes the tracker.
func (t *inflightTracker) close() {
	t.closed.Store(true)
	close(t.watermarks)
	close(t.errors)

	// Close done if not already closed.
	t.closeDone()
}
