package scheduler

import (
	"context"
	"log/slog"
	"time"
)

// Job is a unit of scheduled work. It must observe ctx: the scheduler cannot
// interrupt a job that ignores it.
type Job func(ctx context.Context) error

// Run ticks every `every` and calls job, until ctx is cancelled. Runs are
// sequential: ticks that arrive while a job is running never start a second
// run, and the ticker keeps at most one of them pending.
// Each call to job receives a context with a runTimeout deadline, cancelled
// when ctx is. The deadline is cooperative: a job that ignores its context
// keeps running past runTimeout and delays both the next run and the return
// of Run.
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
	runCtx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()

	start := time.Now()
	if err := job(runCtx); err != nil {
		logger.Error("job run failed", "error", err, "duration", time.Since(start))
		return
	}
	logger.Info("job run completed", "duration", time.Since(start))
}
