package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/code-corhuila/synkro-worker/internal/adapter/in/scheduler"
	"github.com/code-corhuila/synkro-worker/internal/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("startup failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	logger.Info("starting synkro-worker", "every", cfg.LowStockEvery.String())

	// Placeholder job — the real low-stock check (ADR-007 Decision 6)
	// arrives in a future story, once synkro-products-api's stock-alerts
	// endpoints exist to call.
	job := func(ctx context.Context) error {
		logger.Info("job run (placeholder, no real work yet)")
		return nil
	}

	scheduler.Run(ctx, logger, cfg.LowStockEvery, cfg.RunTimeout, job)
	logger.Info("synkro-worker stopped")
}
