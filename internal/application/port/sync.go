package port

import (
	"context"
	"time"
)

// Clock lets tests fix "now"; the use case reads it only to date idempotency
// keys.
type Clock interface{ Now() time.Time }

type correlationKey struct{}

// WithCorrelationID returns a context that carries the correlation id of one
// scheduled run. The scheduler sets it; the use case and the Products adapter
// read it, so every log line and outbound request of a run shares one id.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

// CorrelationIDFrom returns the id set by WithCorrelationID, or "".
func CorrelationIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)
	return id
}

// SyncReport counts what one run did. Failed counts items (and page reads)
// whose outcome is not confirmed, so Failed == 0 means every item examined
// ended in a state Products confirmed.
type SyncReport struct {
	ProductsExamined int // low-stock products read from Products
	Opened           int // alerts Products created (201)
	AlreadyOpen      int // Products answered with an existing OPEN alert (200)
	AlertsExamined   int // OPEN alerts read from Products
	Resolved         int // alerts Products confirmed as RESOLVED
	StillLow         int // OPEN alerts left alone: their product is still low
	Failed           int // items or page reads with an unconfirmed outcome
}

// Processed is the number of items that ended in a confirmed state.
func (r SyncReport) Processed() int { return r.Opened + r.AlreadyOpen + r.Resolved + r.StillLow }

// LowStockAlertSync is the input port: one run opens the alerts that are
// missing and resolves the ones whose product recovered.
type LowStockAlertSync interface {
	Run(ctx context.Context) (SyncReport, error)
}
