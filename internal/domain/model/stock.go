package model

import (
	"errors"
	"strings"
	"time"
)

// AlertStatus is the lifecycle state of a stock alert. Products owns it; the
// Worker only ever reads it back.
type AlertStatus string

const (
	AlertOpen     AlertStatus = "OPEN"
	AlertResolved AlertStatus = "RESOLVED"
)

// Product is the part of a Products catalog entry the low-stock job needs.
type Product struct {
	ID    string
	Stock int
}

// StockAlert is an alert as Products reports it. It is never stored by the
// Worker: Products is the only owner of alert state (ADR-007 Decision 6).
type StockAlert struct {
	ID             string
	ProductID      string
	Status         AlertStatus
	StockAtOpening int
}

// IsLowStock is the single global rule: stock at or below the threshold.
func IsLowStock(stock, threshold int) bool { return stock <= threshold }

var (
	ErrProductIDRequired = errors.New("product id is required")
	ErrNegativeStock     = errors.New("stock must not be negative")
)

// OpeningIntent is one intention to open an alert for one product. Its Key is
// the Idempotency-Key and belongs to the intention, not to a single request:
// every retry of the same intention sends the same key, even if the calendar
// day changes while the Worker waits to retry.
type OpeningIntent struct {
	ProductID      string
	StockAtOpening int
	Key            string
}

// NewOpeningIntent builds the intent whose key is low-stock:<productId>:<date>.
// The date is the UTC calendar day of `at`, formatted YYYY-MM-DD; the Products
// contract does not name a time zone, so UTC is the Worker's convention.
func NewOpeningIntent(productID string, stock int, at time.Time) (OpeningIntent, error) {
	if strings.TrimSpace(productID) == "" {
		return OpeningIntent{}, ErrProductIDRequired
	}
	if stock < 0 {
		return OpeningIntent{}, ErrNegativeStock
	}
	return OpeningIntent{
		ProductID:      productID,
		StockAtOpening: stock,
		Key:            "low-stock:" + productID + ":" + at.UTC().Format("2006-01-02"),
	}, nil
}
