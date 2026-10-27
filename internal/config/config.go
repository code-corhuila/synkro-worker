package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var ErrMissingServiceToken = errors.New("SERVICE_TOKEN is required")

const (
	// maxPageSize is the largest page Products serves (limit is 1..100).
	maxPageSize = 100

	minLowStockEvery = time.Second
	maxMaxPages      = 100
	maxHTTPAttempts  = 10
)

type Config struct {
	ServiceToken      string
	ProductsAPIURL    string
	LowStockEvery     time.Duration
	LowStockThreshold int
	BatchSize         int
	MaxPages          int
	RunTimeout        time.Duration
	HTTPTimeout       time.Duration
	HTTPAttempts      int
}

// Load reads the configuration from the environment. A variable that is unset
// or empty takes its default; one that is set to something invalid is an
// error, never a silent default. Errors name the variable and the rule but
// never echo values, since SERVICE_TOKEN and PRODUCTS_API_URL can carry
// credentials. All problems are reported together.
func Load() (Config, error) {
	token := os.Getenv("SERVICE_TOKEN")
	if token == "" {
		return Config{}, ErrMissingServiceToken
	}

	var errs []error
	bad := func(key, rule string) { errs = append(errs, fmt.Errorf("%s %s", key, rule)) }

	if strings.IndexFunc(token, func(r rune) bool { return r <= ' ' || r == 0x7f }) >= 0 {
		bad("SERVICE_TOKEN", "must not contain whitespace or control characters")
	}

	cfg := Config{ServiceToken: token}
	cfg.ProductsAPIURL = getEnv("PRODUCTS_API_URL", "http://synkro-products-api:8080")
	if msg := checkURL(cfg.ProductsAPIURL); msg != "" {
		bad("PRODUCTS_API_URL", msg)
	}
	cfg.ProductsAPIURL = strings.TrimRight(cfg.ProductsAPIURL, "/")

	cfg.LowStockEvery = duration("LOW_STOCK_EVERY", 15*time.Minute, minLowStockEvery, bad)
	cfg.RunTimeout = duration("RUN_TIMEOUT", 2*time.Minute, time.Millisecond, bad)
	cfg.HTTPTimeout = duration("HTTP_TIMEOUT", 5*time.Second, time.Millisecond, bad)
	cfg.LowStockThreshold = integer("LOW_STOCK_THRESHOLD", 5, 0, 1<<31-1, bad)
	cfg.BatchSize = integer("BATCH_SIZE", 100, 1, maxPageSize, bad)
	cfg.MaxPages = integer("LOW_STOCK_MAX_PAGES", 5, 1, maxMaxPages, bad)
	cfg.HTTPAttempts = integer("HTTP_ATTEMPTS", 3, 1, maxHTTPAttempts, bad)

	if cfg.HTTPTimeout > cfg.RunTimeout {
		bad("HTTP_TIMEOUT", "must not exceed RUN_TIMEOUT")
	}

	if len(errs) > 0 {
		return Config{}, fmt.Errorf("invalid configuration: %w", errors.Join(errs...))
	}
	return cfg, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// checkURL returns the rule a PRODUCTS_API_URL breaks, or "" if it is valid.
func checkURL(raw string) string {
	u, err := url.Parse(raw)
	switch {
	case err != nil:
		return "is not a valid URL"
	case u.Scheme != "http" && u.Scheme != "https":
		return "must start with http:// or https://"
	case u.Hostname() == "":
		return "must include a host"
	case u.User != nil:
		return "must not contain credentials"
	case u.RawQuery != "" || u.Fragment != "":
		return "must not contain a query or fragment"
	}
	return ""
}

func duration(key string, def, min time.Duration, bad func(key, rule string)) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	switch {
	case err != nil:
		bad(key, "must be a duration such as 30s or 2m")
		return def
	case d < min:
		bad(key, "must be at least "+min.String())
		return def
	}
	return d
}

func integer(key string, def, min, max int, bad func(key, rule string)) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	switch {
	case err != nil:
		bad(key, "must be an integer")
		return def
	case i < min || i > max:
		bad(key, fmt.Sprintf("must be between %d and %d", min, max))
		return def
	}
	return i
}
