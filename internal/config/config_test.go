package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

const secretToken = "tok-SECRET-value-9f3a"

// clearEnv blanks every variable Load reads, so a developer's shell cannot
// leak into the tests.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"SERVICE_TOKEN", "PRODUCTS_API_URL", "LOW_STOCK_EVERY", "LOW_STOCK_THRESHOLD",
		"BATCH_SIZE", "LOW_STOCK_MAX_PAGES", "RUN_TIMEOUT", "HTTP_TIMEOUT", "HTTP_ATTEMPTS",
	} {
		t.Setenv(k, "")
	}
}

func TestLoad_FailsFastWithoutServiceToken(t *testing.T) {
	clearEnv(t)
	_, err := Load()
	if !errors.Is(err, ErrMissingServiceToken) {
		t.Fatalf("expected ErrMissingServiceToken, got %v", err)
	}
}

func TestLoad_AppliesDefaultsWhenTokenIsPresent(t *testing.T) {
	clearEnv(t)
	t.Setenv("SERVICE_TOKEN", "test-token")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ServiceToken != "test-token" {
		t.Errorf("expected ServiceToken to be set from the environment")
	}
	want := Config{
		ServiceToken:      "test-token",
		ProductsAPIURL:    "http://synkro-products-api:8080",
		LowStockEvery:     15 * time.Minute,
		LowStockThreshold: 5,
		BatchSize:         100,
		MaxPages:          5,
		RunTimeout:        2 * time.Minute,
		HTTPTimeout:       5 * time.Second,
		HTTPAttempts:      3,
	}
	if cfg != want {
		t.Errorf("defaults:\nwant %+v\n got %+v", want, cfg)
	}
}

func TestLoad_ReadsOverridesFromEnvironment(t *testing.T) {
	clearEnv(t)
	t.Setenv("SERVICE_TOKEN", "test-token")
	t.Setenv("PRODUCTS_API_URL", "https://products.internal:9443/")
	t.Setenv("LOW_STOCK_EVERY", "1m")
	t.Setenv("LOW_STOCK_THRESHOLD", "10")
	t.Setenv("BATCH_SIZE", "25")
	t.Setenv("LOW_STOCK_MAX_PAGES", "2")
	t.Setenv("RUN_TIMEOUT", "30s")
	t.Setenv("HTTP_TIMEOUT", "3s")
	t.Setenv("HTTP_ATTEMPTS", "4")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.LowStockEvery != time.Minute || cfg.LowStockThreshold != 10 || cfg.BatchSize != 25 ||
		cfg.MaxPages != 2 || cfg.RunTimeout != 30*time.Second || cfg.HTTPTimeout != 3*time.Second || cfg.HTTPAttempts != 4 {
		t.Errorf("overrides not applied: %+v", cfg)
	}
	if cfg.ProductsAPIURL != "https://products.internal:9443" {
		t.Errorf("a trailing slash should be trimmed, got %q", cfg.ProductsAPIURL)
	}
}

func TestLoad_AZeroThresholdIsValid(t *testing.T) {
	clearEnv(t)
	t.Setenv("SERVICE_TOKEN", "test-token")
	t.Setenv("LOW_STOCK_THRESHOLD", "0")

	cfg, err := Load()
	if err != nil || cfg.LowStockThreshold != 0 {
		t.Fatalf("threshold 0 means \"only out of stock\" and must be accepted: %+v / %v", cfg, err)
	}
}

func TestLoad_InvalidSettingsAreErrorsNotSilentDefaults(t *testing.T) {
	tests := []struct {
		name, key, value string
	}{
		{"malformed duration", "LOW_STOCK_EVERY", "soon"},
		{"zero duration", "LOW_STOCK_EVERY", "0s"},
		{"negative duration", "LOW_STOCK_EVERY", "-5m"},
		{"interval under one second", "LOW_STOCK_EVERY", "500ms"},
		{"malformed run timeout", "RUN_TIMEOUT", "two minutes"},
		{"zero run timeout", "RUN_TIMEOUT", "0"},
		{"malformed http timeout", "HTTP_TIMEOUT", "fast"},
		{"negative http timeout", "HTTP_TIMEOUT", "-1s"},
		{"http timeout above the run timeout", "HTTP_TIMEOUT", "3m"},
		{"malformed threshold", "LOW_STOCK_THRESHOLD", "five"},
		{"negative threshold", "LOW_STOCK_THRESHOLD", "-1"},
		{"fractional threshold", "LOW_STOCK_THRESHOLD", "2.5"},
		{"zero batch size", "BATCH_SIZE", "0"},
		{"negative batch size", "BATCH_SIZE", "-10"},
		{"batch size above the API limit", "BATCH_SIZE", "101"},
		{"malformed batch size", "BATCH_SIZE", "many"},
		{"zero max pages", "LOW_STOCK_MAX_PAGES", "0"},
		{"too many pages", "LOW_STOCK_MAX_PAGES", "101"},
		{"zero attempts", "HTTP_ATTEMPTS", "0"},
		{"negative attempts", "HTTP_ATTEMPTS", "-3"},
		{"too many attempts", "HTTP_ATTEMPTS", "11"},
		{"malformed attempts", "HTTP_ATTEMPTS", "x"},
		{"url without scheme", "PRODUCTS_API_URL", "synkro-products-api:8080"},
		{"url with another scheme", "PRODUCTS_API_URL", "ftp://products"},
		{"url without host", "PRODUCTS_API_URL", "http://"},
		{"url with credentials", "PRODUCTS_API_URL", "http://user:hunter2@products:8080"},
		{"url with a query", "PRODUCTS_API_URL", "http://products:8080/?x=1"},
		{"token with whitespace", "SERVICE_TOKEN", "abc def"},
		{"token with a newline", "SERVICE_TOKEN", "abc\ndef"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("SERVICE_TOKEN", secretToken)
			t.Setenv(tc.key, tc.value)

			_, err := Load()

			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.key) {
				t.Errorf("the error should name %s: %v", tc.key, err)
			}
			for _, secret := range []string{secretToken, "hunter2", "abc def"} {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("the error leaks %q: %v", secret, err)
				}
			}
		})
	}
}

func TestLoad_ReportsEverySettingThatIsInvalidAtOnce(t *testing.T) {
	clearEnv(t)
	t.Setenv("SERVICE_TOKEN", "test-token")
	t.Setenv("BATCH_SIZE", "0")
	t.Setenv("HTTP_ATTEMPTS", "0")

	_, err := Load()

	if err == nil || !strings.Contains(err.Error(), "BATCH_SIZE") || !strings.Contains(err.Error(), "HTTP_ATTEMPTS") {
		t.Fatalf("both settings should be reported, got %v", err)
	}
}
