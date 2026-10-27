package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/code-corhuila/synkro-worker/internal/adapter/in/scheduler"
	"github.com/code-corhuila/synkro-worker/internal/adapter/out/products"
	"github.com/code-corhuila/synkro-worker/internal/application/usecase"
	"github.com/code-corhuila/synkro-worker/internal/config"
)

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		// The error names the invalid settings, never their values.
		logger.Error("startup failed", "error", err)
		os.Exit(1)
	}

	gateway, err := products.New(products.Options{
		BaseURL:  cfg.ProductsAPIURL,
		Token:    cfg.ServiceToken,
		Timeout:  cfg.HTTPTimeout,
		Attempts: cfg.HTTPAttempts,
	})
	if err != nil {
		logger.Error("startup failed", "error", err)
		os.Exit(1)
	}

	lowStock, err := usecase.NewLowStockAlerts(gateway, systemClock{}, usecase.Config{
		Threshold: cfg.LowStockThreshold,
		PageSize:  cfg.BatchSize,
		MaxPages:  cfg.MaxPages,
	}, logger)
	if err != nil {
		logger.Error("startup failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	logger.Info("starting synkro-worker",
		"every", cfg.LowStockEvery.String(),
		"threshold", cfg.LowStockThreshold,
		"batchSize", cfg.BatchSize,
		"maxPages", cfg.MaxPages,
		"runTimeout", cfg.RunTimeout.String(),
		"httpTimeout", cfg.HTTPTimeout.String(),
		"httpAttempts", cfg.HTTPAttempts,
	)

	scheduler.Run(ctx, logger, cfg.LowStockEvery, cfg.RunTimeout, scheduler.LowStockJob(logger, lowStock))
	logger.Info("synkro-worker stopped")
}
