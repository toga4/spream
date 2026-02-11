package partitionstorage

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// constSource is a rand.Source that always returns the same value.
type constSource uint64

func (s constSource) Uint64() uint64 { return uint64(s) }

// newConstRand returns a *rand.Rand whose Float64 always returns f.
// Float64 is computed as (Uint64()<<11>>11) / (1<<53),
// so we reverse that to find the Uint64 value that produces f.
func newConstRand(f float64) *rand.Rand {
	bits := uint64(f * (1 << 53))
	return rand.New(constSource(bits))
}

func TestRetryPolicy_backoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		policy  RetryPolicy
		attempt int
		want    time.Duration
	}{
		{
			name: "first retry",
			policy: RetryPolicy{
				MinBackoff:   100 * time.Millisecond,
				MaxBackoff:   5 * time.Second,
				Multiplier:   2.0,
				JitterFactor: 0,
			},
			attempt: 1,
			want:    100 * time.Millisecond,
		},
		{
			name: "exponential growth",
			policy: RetryPolicy{
				MinBackoff:   100 * time.Millisecond,
				MaxBackoff:   5 * time.Second,
				Multiplier:   2.0,
				JitterFactor: 0,
			},
			attempt: 3,
			want:    400 * time.Millisecond,
		},
		{
			name: "capped at MaxBackoff",
			policy: RetryPolicy{
				MinBackoff:   100 * time.Millisecond,
				MaxBackoff:   300 * time.Millisecond,
				Multiplier:   2.0,
				JitterFactor: 0,
			},
			attempt: 4,
			want:    300 * time.Millisecond,
		},
		{
			name: "Multiplier < 1.0 treated as 1.0",
			policy: RetryPolicy{
				MinBackoff:   100 * time.Millisecond,
				MaxBackoff:   5 * time.Second,
				Multiplier:   0.5,
				JitterFactor: 0,
			},
			attempt: 3,
			want:    100 * time.Millisecond,
		},
		{
			name: "MinBackoff > MaxBackoff uses MinBackoff as cap",
			policy: RetryPolicy{
				MinBackoff:   200 * time.Millisecond,
				MaxBackoff:   50 * time.Millisecond,
				Multiplier:   2.0,
				JitterFactor: 0,
			},
			attempt: 2,
			want:    200 * time.Millisecond,
		},
		{
			name: "jitter positive direction",
			policy: RetryPolicy{
				MinBackoff:   100 * time.Millisecond,
				MaxBackoff:   5 * time.Second,
				Multiplier:   2.0,
				JitterFactor: 0.2,
				rng:          newConstRand(0.75),
			},
			attempt: 1,
			// 100ms + 100ms * 0.2 * (2*0.75 - 1) = 100ms + 10ms = 110ms
			want: 110 * time.Millisecond,
		},
		{
			name: "jitter negative direction",
			policy: RetryPolicy{
				MinBackoff:   100 * time.Millisecond,
				MaxBackoff:   5 * time.Second,
				Multiplier:   2.0,
				JitterFactor: 0.2,
				rng:          newConstRand(0.25),
			},
			attempt: 1,
			// 100ms + 100ms * 0.2 * (2*0.25 - 1) = 100ms - 10ms = 90ms
			want: 90 * time.Millisecond,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.policy.backoff(tt.attempt)
			if got != tt.want {
				t.Errorf("backoff(%d) = %v, want %v", tt.attempt, got, tt.want)
			}
		})
	}
}

func TestRetryPolicy_attempts(t *testing.T) {
	t.Parallel()

	t.Run("MaxAttempts=0 yields once", func(t *testing.T) {
		t.Parallel()
		p := RetryPolicy{MaxAttempts: 0}
		count := 0
		for range p.attempts(context.Background()) {
			count++
		}
		if count != 1 {
			t.Errorf("got %d yields, want 1", count)
		}
	})

	t.Run("MaxAttempts=1 yields once", func(t *testing.T) {
		t.Parallel()
		p := RetryPolicy{MaxAttempts: 1}
		count := 0
		for range p.attempts(context.Background()) {
			count++
		}
		if count != 1 {
			t.Errorf("got %d yields, want 1", count)
		}
	})

	t.Run("MaxAttempts=3 yields three times", func(t *testing.T) {
		t.Parallel()
		p := RetryPolicy{MaxAttempts: 3, MinBackoff: 0}
		count := 0
		for range p.attempts(context.Background()) {
			count++
		}
		if count != 3 {
			t.Errorf("got %d yields, want 3", count)
		}
	})

	t.Run("early break stops iteration", func(t *testing.T) {
		t.Parallel()
		p := RetryPolicy{MaxAttempts: 5, MinBackoff: 0}
		count := 0
		for range p.attempts(context.Background()) {
			count++
			if count == 2 {
				break
			}
		}
		if count != 2 {
			t.Errorf("got %d yields, want 2", count)
		}
	})

	t.Run("ctx cancel stops during sleep", func(t *testing.T) {
		t.Parallel()
		p := RetryPolicy{MaxAttempts: 5, MinBackoff: 10 * time.Second}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		count := 0
		for range p.attempts(ctx) {
			count++
			// Cancel after first yield so the next sleep is interrupted.
			cancel()
		}
		if count != 1 {
			t.Errorf("got %d yields, want 1", count)
		}
	})
}

func TestIsRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		code codes.Code
		want bool
	}{
		{codes.OK, false},
		{codes.Canceled, false},
		{codes.InvalidArgument, false},
		{codes.NotFound, false},
		{codes.AlreadyExists, false},
		{codes.PermissionDenied, false},
		{codes.Aborted, false},
		{codes.Unavailable, true},
		{codes.DeadlineExceeded, true},
		{codes.ResourceExhausted, true},
		{codes.Internal, true},
	}
	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			t.Parallel()
			err := status.Error(tt.code, "test error")
			if got := isRetryable(err); got != tt.want {
				t.Errorf("isRetryable(%v) = %v, want %v", tt.code, got, tt.want)
			}
		})
	}

	t.Run("nil error", func(t *testing.T) {
		t.Parallel()
		if got := isRetryable(nil); got != false {
			t.Errorf("isRetryable(nil) = %v, want false", got)
		}
	})
}
