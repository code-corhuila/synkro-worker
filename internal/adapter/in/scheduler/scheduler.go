package scheduler

import (
	"context"
	"log/slog"
	"time"
)

// Job is a unit of scheduled work. It should respect ctx's deadline.
type Job func(ctx context.Context) error

// Run ticks every `every` and calls job, until ctx is cancelled. Each call
// to job is bounded by runTimeout, enforced here — job does not need to
// implement its own timeout.
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
