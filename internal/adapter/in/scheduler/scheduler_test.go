package scheduler

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestRun_CallsJobOnEveryTick(t *testing.T) {
	var calls int32
	job := func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	Run(ctx, logger, 10*time.Millisecond, 5*time.Second, job)

	got := atomic.LoadInt32(&calls)
	if got < 2 {
		t.Fatalf("expected at least 2 job calls in 35ms with a 10ms interval, got %d", got)
	}
}

func TestRun_StopsWhenContextIsCancelled(t *testing.T) {
	job := func(ctx context.Context) error { return nil }

	ctx, cancel := context.WithCancel(context.Background())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	done := make(chan struct{})
	go func() {
		Run(ctx, logger, 5*time.Millisecond, 5*time.Second, job)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Run returned promptly after cancellation, as expected
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not stop within 1s of context cancellation")
	}
}

func TestRun_BoundsEachJobRunWithRunTimeout(t *testing.T) {
	jobStarted := make(chan struct{})
	jobSawCancel := make(chan bool, 1)

	job := func(ctx context.Context) error {
		close(jobStarted)
		select {
		case <-ctx.Done():
			jobSawCancel <- true
		case <-time.After(1 * time.Second):
			jobSawCancel <- false
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go Run(ctx, logger, 10*time.Millisecond, 20*time.Millisecond, job)

	<-jobStarted
	select {
	case sawCancel := <-jobSawCancel:
		if !sawCancel {
			t.Fatal("expected the job's context to be cancelled by RunTimeout, but it ran to completion")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("job never observed its context being cancelled")
	}
}
