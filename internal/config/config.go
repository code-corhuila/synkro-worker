package config

import (
	"errors"
	"os"
	"strconv"
	"time"
)

var ErrMissingServiceToken = errors.New("SERVICE_TOKEN is required")

type Config struct {
	ServiceToken      string
	ProductsAPIURL    string
	LowStockEvery     time.Duration
	LowStockThreshold int
	BatchSize         int
	RunTimeout        time.Duration
	HTTPTimeout       time.Duration
	HTTPAttempts      int
}

func Load() (Config, error) {
	token := os.Getenv("SERVICE_TOKEN")
	if token == "" {
		return Config{}, ErrMissingServiceToken
	}
	return Config{
		ServiceToken:      token,
		ProductsAPIURL:    getEnv("PRODUCTS_API_URL", "http://synkro-products-api:8080"),
		LowStockEvery:     getDuration("LOW_STOCK_EVERY", 15*time.Minute),
		LowStockThreshold: getInt("LOW_STOCK_THRESHOLD", 5),
		BatchSize:         getInt("BATCH_SIZE", 100),
		RunTimeout:        getDuration("RUN_TIMEOUT", 2*time.Minute),
		HTTPTimeout:       getDuration("HTTP_TIMEOUT", 5*time.Second),
		HTTPAttempts:      getInt("HTTP_ATTEMPTS", 3),
	}, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		// Non-positive durations are rejected: time.NewTicker panics on them.
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return def
}

func getInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}
