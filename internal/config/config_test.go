package config

import (
	"testing"
	"time"
)

func TestLoad_FailsFastWithoutServiceToken(t *testing.T) {
	t.Setenv("SERVICE_TOKEN", "")
	_, err := Load()
	if err == nil {
		t.Fatal("expected an error when SERVICE_TOKEN is not set")
	}
}

func TestLoad_AppliesDefaultsWhenTokenIsPresent(t *testing.T) {
	t.Setenv("SERVICE_TOKEN", "test-token")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ServiceToken != "test-token" {
		t.Errorf("expected ServiceToken to be set from the environment")
	}
	if cfg.LowStockEvery != 15*time.Minute {
		t.Errorf("expected default LowStockEvery of 15m, got %v", cfg.LowStockEvery)
	}
	if cfg.RunTimeout != 2*time.Minute {
		t.Errorf("expected default RunTimeout of 2m, got %v", cfg.RunTimeout)
	}
	if cfg.LowStockThreshold != 5 {
		t.Errorf("expected default LowStockThreshold of 5, got %d", cfg.LowStockThreshold)
	}
}

func TestLoad_ReadsOverridesFromEnvironment(t *testing.T) {
	t.Setenv("SERVICE_TOKEN", "test-token")
	t.Setenv("LOW_STOCK_EVERY", "1m")
	t.Setenv("LOW_STOCK_THRESHOLD", "10")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.LowStockEvery != time.Minute {
		t.Errorf("expected overridden LowStockEvery of 1m, got %v", cfg.LowStockEvery)
	}
	if cfg.LowStockThreshold != 10 {
		t.Errorf("expected overridden LowStockThreshold of 10, got %d", cfg.LowStockThreshold)
	}
}
