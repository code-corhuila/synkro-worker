package model

import (
	"errors"
	"strings"
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
	// ErrInvalidRunID: the run identity is missing or is not a non-nil UUID in
	// canonical form. The offending value is never echoed.
	ErrInvalidRunID = errors.New("run id must be a non-nil canonical UUID")
)

// ValidateRunID accepts only a canonical 8-4-4-4-12 hexadecimal UUID that is
// not the nil UUID. Anything else (empty, a timestamp, surrounding spaces,
// braces, a "urn:uuid:" prefix) is rejected so that an Idempotency-Key can
// never be built from a fallback identity.
func ValidateRunID(id string) error {
	if len(id) != 36 || id == "00000000-0000-0000-0000-000000000000" {
		return ErrInvalidRunID
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case i == 8 || i == 13 || i == 18 || i == 23:
			if c != '-' {
				return ErrInvalidRunID
			}
		case !isHex(c):
			return ErrInvalidRunID
		}
	}
	return nil
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

// OpeningIntent is one intention to open an alert for one product within one
// run (ADR-018). Its Key is the Idempotency-Key and belongs to the intention,
// not to a single request: it is built once, before the first POST, and every
// retry sends the same key, productId and stockAtOpening, whatever stock or
// calendar day the Worker observes in the meantime.
type OpeningIntent struct {
	ProductID      string
	StockAtOpening int
	Key            string
}

// NewOpeningIntent builds the intent whose key is low-stock:<productId>:<runId>.
// runId is the UUID of the execution that observed the product. A different
// run builds a different key; the same product and run produce the same key.
// The caller constructs the intention only once and retains its original body.
func NewOpeningIntent(productID string, stock int, runID string) (OpeningIntent, error) {
	if strings.TrimSpace(productID) == "" {
		return OpeningIntent{}, ErrProductIDRequired
	}
	if stock < 0 {
		return OpeningIntent{}, ErrNegativeStock
	}
	if err := ValidateRunID(runID); err != nil {
		return OpeningIntent{}, err
	}
	return OpeningIntent{
		ProductID:      productID,
		StockAtOpening: stock,
		Key:            "low-stock:" + productID + ":" + runID,
	}, nil
}
