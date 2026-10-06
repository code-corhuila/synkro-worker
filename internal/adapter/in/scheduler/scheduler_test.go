package scheduler

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// runInBackground starts Run in a goroutine and returns a channel closed when
// it returns, so tests can assert on shutdown and never leak the loop.
func runInBackground(ctx context.Context, every, runTimeout time.Duration, job Job) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		Run(ctx, discardLogger(), every, runTimeout, job)
		close(done)
	}()
	return done
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not stop within 1s of context cancellation")
	}
}

func TestRun_CallsJobOnEveryTick(t *testing.T) {
	// Event-driven rather than "count calls inside a fixed window": a fixed
	// 35ms window flaked on Windows when the OS stalled the process, and would
	// be worse under -race on a busy CI runner.
	const wantCalls = 3
	calls := make(chan struct{}, wantCalls)
	job := func(ctx context.Context) error {
		select {
		case calls <- struct{}{}:
		default:
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runInBackground(ctx, 10*time.Millisecond, 5*time.Second, job)

	deadline := time.After(2 * time.Second)
	for i := 0; i < wantCalls; i++ {
		select {
		case <-calls:
		case <-deadline:
			t.Fatalf("expected %d job calls with a 10ms interval within 2s, got %d", wantCalls, i)
		}
	}

	cancel()
	waitDone(t, done)
}

func TestRun_StopsWhenContextIsCancelled(t *testing.T) {
	job := func(ctx context.Context) error { return nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(ctx, 5*time.Millisecond, 5*time.Second, job)

	time.Sleep(20 * time.Millisecond)
	cancel()

	waitDone(t, done)
}

func TestRun_BoundsEachJobRunWithRunTimeout(t *testing.T) {
	jobStarted := make(chan struct{})
	jobSawCancel := make(chan bool, 1)

	// Only the first call is observed; later ticks fire while the test is
	// still running and must be no-ops (closing jobStarted twice panics).
	var first sync.Once
	job := func(ctx context.Context) error {
		first.Do(func() {
			close(jobStarted)
			select {
			case <-ctx.Done():
				jobSawCancel <- true
			case <-time.After(1 * time.Second):
				jobSawCancel <- false
			}
		})
		return nil
	}

	// The parent context outlives runTimeout by far, so a cancellation seen
	// by the job can only come from runTimeout.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runInBackground(ctx, 10*time.Millisecond, 20*time.Millisecond, job)

	<-jobStarted
	select {
	case sawCancel := <-jobSawCancel:
		if !sawCancel {
			t.Fatal("expected the job's context to be cancelled by RunTimeout, but it ran to completion")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("job never observed its context being cancelled")
	}

	cancel()
	waitDone(t, done)
}
