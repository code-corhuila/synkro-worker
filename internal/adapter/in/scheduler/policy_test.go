package scheduler

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/code-corhuila/synkro-worker/internal/application/port"
)

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func bufferLogger() (*slog.Logger, *syncBuffer) {
	buf := &syncBuffer{}
	return slog.New(slog.NewJSONHandler(buf, nil)), buf
}

func TestRun_GivesEachRunItsOwnCorrelationIDInTheContextAndTheLogs(t *testing.T) {
	logger, logs := bufferLogger()
	ids := make(chan string, 8)
	job := func(ctx context.Context) error {
		select {
		case ids <- port.CorrelationIDFrom(ctx):
		default:
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { Run(ctx, logger, 5*time.Millisecond, 5*time.Second, job); close(done) }()

	first, second := <-ids, <-ids
	cancel()
	<-done

	if first == "" || second == "" || first == second {
		t.Fatalf("each run needs its own non-empty correlation id, got %q and %q", first, second)
	}
	if !strings.Contains(logs.String(), `"correlationId":"`+first+`"`) {
		t.Errorf("the scheduler's own log lines must carry the run's correlation id: %s", logs.String())
	}
}

func TestRun_RecoversFromAPanicInTheJobAndKeepsRunning(t *testing.T) {
	logger, logs := bufferLogger()
	var calls atomic.Int32
	survived := make(chan struct{})
	job := func(ctx context.Context) error {
		if calls.Add(1) == 1 {
			panic("boom: nil pointer in a job")
		}
		select {
		case <-survived:
		default:
			close(survived)
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { Run(ctx, logger, 5*time.Millisecond, 5*time.Second, job); close(done) }()

	select {
	case <-survived:
	case <-time.After(2 * time.Second):
		t.Fatal("the scheduler did not run again after a panic")
	}
	cancel()
	<-done

	out := logs.String()
	if !strings.Contains(out, "panicked") || !strings.Contains(out, "boom: nil pointer in a job") {
		t.Errorf("the panic must be logged as a failed run: %s", out)
	}
}

func TestRun_NeverRunsTwoJobsAtTheSameTime(t *testing.T) {
	var running, maxSeen, calls atomic.Int32
	job := func(ctx context.Context) error {
		n := running.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(25 * time.Millisecond) // several ticks fire meanwhile
		running.Add(-1)
		calls.Add(1)
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(ctx, 2*time.Millisecond, 5*time.Second, job)

	deadline := time.After(2 * time.Second)
	for calls.Load() < 3 {
		select {
		case <-deadline:
			t.Fatal("job did not run three times")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	waitDone(t, done)

	if maxSeen.Load() != 1 {
		t.Fatalf("runs overlapped: %d jobs at once", maxSeen.Load())
	}
}

func TestRun_ShutdownCancelsTheRunInFlightAndWaitsForItToReturn(t *testing.T) {
	started := make(chan struct{})
	var finished atomic.Bool
	job := func(ctx context.Context) error {
		close(started)
		<-ctx.Done() // a cooperative job stops when shutdown cancels its context
		time.Sleep(20 * time.Millisecond)
		finished.Store(true)
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(ctx, 2*time.Millisecond, 5*time.Second, job)
	<-started

	cancel()
	waitDone(t, done)

	if !finished.Load() {
		t.Fatal("Run returned before the job in flight did")
	}
}

type fakeSync struct {
	rep port.SyncReport
	err error
	ctx context.Context
}

func (f *fakeSync) Run(ctx context.Context) (port.SyncReport, error) {
	f.ctx = ctx
	return f.rep, f.err
}

func TestLowStockJob_ReportsTheCountsAndSucceedsWhenNothingFailed(t *testing.T) {
	logger, logs := bufferLogger()
	uc := &fakeSync{rep: port.SyncReport{Opened: 2, AlreadyOpen: 1, Resolved: 3, StillLow: 4}}
	ctx := port.WithCorrelationID(context.Background(), "corr-9")

	err := LowStockJob(logger, uc)(ctx)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uc.ctx != ctx {
		t.Error("the run context (deadline, correlation id) must reach the use case unchanged")
	}
	out := logs.String()
	for _, want := range []string{`"correlationId":"corr-9"`, `"opened":2`, `"resolved":3`, `"processed":10`, `"failed":0`} {
		if !strings.Contains(out, want) {
			t.Errorf("log should contain %s: %s", want, out)
		}
	}
}

func TestLowStockJob_AFailedItemMakesTheRunFailButKeepsTheCounts(t *testing.T) {
	logger, logs := bufferLogger()
	uc := &fakeSync{rep: port.SyncReport{Opened: 1, Failed: 2}}

	err := LowStockJob(logger, uc)(port.WithCorrelationID(context.Background(), "corr-1"))

	if err == nil || !strings.Contains(err.Error(), "2") {
		t.Fatalf("want an error naming the 2 failed items, got %v", err)
	}
	if !strings.Contains(logs.String(), `"failed":2`) || !strings.Contains(logs.String(), `"opened":1`) {
		t.Errorf("counts must be logged even when the run fails: %s", logs.String())
	}
}

func TestLowStockJob_ARunLevelErrorIsReturnedAndLogged(t *testing.T) {
	logger, logs := bufferLogger()
	cause := errors.New("run aborted: products rejected the service credential")
	uc := &fakeSync{rep: port.SyncReport{Failed: 1}, err: cause}

	err := LowStockJob(logger, uc)(context.Background())

	if !errors.Is(err, cause) {
		t.Fatalf("want the cause returned, got %v", err)
	}
	if !strings.Contains(logs.String(), "rejected the service credential") {
		t.Errorf("the abort reason must be logged: %s", logs.String())
	}
}
