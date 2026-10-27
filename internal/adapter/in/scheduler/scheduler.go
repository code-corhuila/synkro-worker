package scheduler

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"time"

	"github.com/code-corhuila/synkro-worker/internal/application/port"
)

// Job is a unit of scheduled work. It must observe ctx: the scheduler cannot
// interrupt a job that ignores it.
type Job func(ctx context.Context) error

// newCorrelationID is a variable so tests can pin it; production uses a
// random UUIDv4.
var newCorrelationID = randomUUID

// Run ticks every `every` and calls job, until ctx is cancelled. Runs are
// sequential: ticks that arrive while a job is running never start a second
// run, and the ticker keeps at most one of them pending.
//
// Each call to job receives a context with a runTimeout deadline, cancelled
// when ctx is, and carrying the run's own correlation id (port.WithCorrelationID).
// The deadline is cooperative: a job that ignores its context keeps running
// past runTimeout and delays both the next run and the return of Run.
//
// Failure policy: an error returned by a job and a panic raised by it both
// end only that run. Both are logged with the run's correlation id and the
// scheduler waits for the next tick. A panic is recovered per run, so one bad
// run cannot stop the Worker; the stack is logged to find the cause.
//
// Shutdown: when ctx is cancelled, the run in flight sees its context
// cancelled and Run returns once the job does. No new run starts afterwards.
func Run(ctx context.Context, logger *slog.Logger, every, runTimeout time.Duration, job Job) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("scheduler stopped")
			return
		case <-ticker.C:
			runOnce(ctx, logger, runTimeout, job)
		}
	}
}

func runOnce(ctx context.Context, logger *slog.Logger, runTimeout time.Duration, job Job) {
	correlationID := newCorrelationID()
	logger = logger.With("correlationId", correlationID)

	runCtx, cancel := context.WithTimeout(port.WithCorrelationID(ctx, correlationID), runTimeout)
	defer cancel()

	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			logger.Error("job run panicked", "panic", fmt.Sprint(r), "stack", stack(), "duration", time.Since(start))
		}
	}()

	if err := job(runCtx); err != nil {
		logger.Error("job run failed", "error", err, "duration", time.Since(start))
		return
	}
	logger.Info("job run completed", "duration", time.Since(start))
}

func randomUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail on supported platforms; a time-based id is
		// still unique enough to correlate one run's lines.
		return fmt.Sprintf("run-%d", time.Now().UnixNano())
	}
	b[6] = b[6]&0x0f | 0x40 // version 4
	b[8] = b[8]&0x3f | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
