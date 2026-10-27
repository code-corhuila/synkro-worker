package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/code-corhuila/synkro-worker/internal/application/port"
)

// stack is the stack of the goroutine recovering a panic, capped so a log
// line stays bounded.
func stack() string {
	s := string(debug.Stack())
	if len(s) > 4096 {
		s = s[:4096] + "...(truncated)"
	}
	return s
}

// LowStockJob adapts the low-stock use case to a scheduler Job. It reports
// the counts of the run, and turns both a run-level error and any failed item
// into a failed run, so the scheduler logs it as such. It logs counts only:
// no tokens, no response bodies.
func LowStockJob(logger *slog.Logger, uc port.LowStockAlertSync) Job {
	return func(ctx context.Context) error {
		log := logger.With("correlationId", port.CorrelationIDFrom(ctx))

		rep, err := uc.Run(ctx)
		counts := []any{
			"processed", rep.Processed(),
			"failed", rep.Failed,
			"productsExamined", rep.ProductsExamined,
			"opened", rep.Opened,
			"alreadyOpen", rep.AlreadyOpen,
			"alertsExamined", rep.AlertsExamined,
			"resolved", rep.Resolved,
			"stillLow", rep.StillLow,
		}
		switch {
		case err != nil:
			log.Error("low-stock run aborted", append(counts, "error", err)...)
			return err
		case rep.Failed > 0:
			log.Warn("low-stock run finished with failed items", counts...)
			return fmt.Errorf("low-stock run finished with %d failed item(s)", rep.Failed)
		}
		log.Info("low-stock run completed", counts...)
		return nil
	}
}
