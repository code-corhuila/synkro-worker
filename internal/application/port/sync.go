package port

import "context"

type correlationKey struct{}

// WithCorrelationID returns a context that carries the identity of one
// scheduled run. The scheduler sets it; the use case and the Products adapter
// read it, so every log line and outbound request of a run shares one id.
//
// The same value is the run identity of ADR-018: the use case builds every
// Idempotency-Key from it (low-stock:<productId>:<runId>) and refuses to
// start a run, before any request, when it is not a valid UUID.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

// CorrelationIDFrom returns the id set by WithCorrelationID, or "".
func CorrelationIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)
	return id
}

// SyncReport separates failed requests/items from valid but unsatisfied opening
// reconciliations. A clean run requires both Failed and ReconciliationPending
// to be zero; Processed counts only confirmed reconciliation outcomes.
type SyncReport struct {
	ProductsExamined int // low-stock products read from Products and selected for an opening
	Opened           int // alerts Products created (201) and returned OPEN
	AlreadyOpen      int // Products answered with an existing OPEN alert (200)
	AlertsExamined   int // OPEN alerts read from Products and selected for evaluation
	Resolved         int // alerts Products confirmed as RESOLVED
	StillLow         int // OPEN alerts left alone: their product is still low
	Failed           int // items or page reads with an unconfirmed outcome

	// ReconciliationPending counts openings whose valid answer (200 or 201)
	// carried a RESOLVED alert (ADR-018): a recorded key replayed after
	// resolution, or a resolution that committed before the representation was
	// read. The product is low and no OPEN alert was confirmed, so the opening
	// is unsatisfied. It is neither Opened nor AlreadyOpen, and it is not part
	// of Failed (the answer was valid and transport succeeded): a later run
	// re-evaluates the current stock under a new run identity.
	ReconciliationPending int
}

// Processed is the number of items that ended in a confirmed state. Pending
// reconciliations are not confirmed states and are excluded.
func (r SyncReport) Processed() int { return r.Opened + r.AlreadyOpen + r.Resolved + r.StillLow }

// LowStockAlertSync is the input port: one run opens the alerts that are
// missing and resolves the ones whose product recovered. The run identity is
// taken from ctx (see WithCorrelationID).
type LowStockAlertSync interface {
	Run(ctx context.Context) (SyncReport, error)
}
