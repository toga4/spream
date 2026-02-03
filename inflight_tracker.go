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

	// sendMu synchronizes channel sends and close operations.
	// Senders use RLock (concurrent), close uses Lock (exclusive).
	sendMu sync.RWMutex

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

// sendWatermark はウォーターマークをチャネルに送信する。
// watermark がゼロ値の場合は何もしない。
func (t *inflightTracker) sendWatermark(watermark time.Time) {
	if watermark.IsZero() {
		return
	}
	t.sendMu.RLock()
	defer t.sendMu.RUnlock()
	if !t.closed.Load() {
		t.watermarks <- watermark
	}
}

// sendError はエラーをチャネルに送信する。
func (t *inflightTracker) sendError(err error) {
	t.sendMu.RLock()
	defer t.sendMu.RUnlock()
	if !t.closed.Load() {
		t.errors <- err
	}
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
	if err != nil {
		t.sem.Release(1)
		t.sendError(err)
		return
	}

	watermark := t.completeRecord(seq)
	t.sendWatermark(watermark)
}

// completeRecord はレコードを acked にし、更新されたウォーターマークを返す。
func (t *inflightTracker) completeRecord(seq int64) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	defer t.sem.Release(1)

	if rec, ok := t.pending[seq]; ok {
		rec.acked = true
	}
	return t.advanceWatermark()
}

// ackImmediate is for records that ack immediately (HeartbeatRecord, ChildPartitionsRecord).
// Does not use semaphore (no goroutine is spawned).
func (t *inflightTracker) ackImmediate(timestamp time.Time) {
	watermark := t.ackImmediateRecord(timestamp)
	t.sendWatermark(watermark)
}

// ackImmediateRecord はレコードを即座に acked として追加し、更新されたウォーターマークを返す。
func (t *inflightTracker) ackImmediateRecord(timestamp time.Time) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()

	seq := t.nextSeq
	t.nextSeq++

	t.pending[seq] = &pendingRecord{
		seq:       seq,
		timestamp: timestamp,
		acked:     true, // Immediately acked.
	}

	return t.advanceWatermark()
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

// drain initiates graceful shutdown and waits until all in-flight records are completed.
// Call after readStream() has finished.
func (t *inflightTracker) drain() {
	t.shutdown.Store(true)

	// Close done immediately if pending is already 0.
	t.mu.Lock()
	if len(t.pending) == 0 {
		t.closeDone()
	}
	t.mu.Unlock()

	<-t.done
}

// getSafeWatermark returns the continuously acked watermark.
func (t *inflightTracker) getSafeWatermark() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.safeWatermark
}

// close closes the tracker.
// This method is idempotent; calling it multiple times is safe.
func (t *inflightTracker) close() {
	// Use sendMu to ensure no goroutine is sending to channels.
	t.sendMu.Lock()
	defer t.sendMu.Unlock()

	if t.closed.Swap(true) {
		return // Already closed.
	}
	close(t.watermarks)
	close(t.errors)
	t.closeDone()
}
