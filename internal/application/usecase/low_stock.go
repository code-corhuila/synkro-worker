package usecase

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/code-corhuila/synkro-worker/internal/application/port"
	"github.com/code-corhuila/synkro-worker/internal/domain/model"
)

// MaxPageSize is the largest page Products serves (limit is 1..100).
const MaxPageSize = 100

// maxConsecutiveUnavailable is how many items in a row may end Unavailable
// (every HTTP attempt spent) before the run gives up: past that, Products is
// down and each further item would only burn its attempts.
const maxConsecutiveUnavailable = 3

var ErrRunInProgress = errors.New("a low-stock run is already in progress")

// Config bounds the work of one run.
//
// BatchSize is the TOTAL number of reconciliation elements of a run, shared by
// both phases: the elements Products returns in the product and alert lists
// and that the run selects to evaluate, duplicates included. It is split into
// two fixed quotas (see phaseQuotas): with BatchSize >= 2 the open phase gets
// ceil(BatchSize/2) and the resolve phase floor(BatchSize/2) on every run; with
// BatchSize 1 the single element alternates between the phases from run to run.
// A quota is never lent to the other phase, even when its list is empty: that
// wastes a share now and then in exchange for page sizes that never change.
//
// Each phase keeps a fixed page limit ceil(quota/MaxPages), reserving that
// many admission slots BEFORE each request, even when the answer is short.
// The phase stops when another full page would exceed its remaining slots.
// This bounds both requested and contract-compliant returned elements without
// changing page widths or slicing away an unprocessed tail. Unused slots are
// intentionally not reclaimed. MaxPages caps reads, not the total batch.
//
// The budget counts elements, not HTTP requests. Evaluating one element can
// add requests: an open element is one POST; a resolve element is at most one
// product point-read plus one resolve POST. Each request makes at most the
// gateway's attempts and the whole run is bounded by its context deadline.
// Page cursors persist between runs (in memory only), so a catalog larger than
// one run's budget is covered over successive runs.
type Config struct {
	Threshold int
	BatchSize int
	MaxPages  int
}

// phaseQuotas splits the total batch between the two phases for run number
// run (0-based, counting only runs that started). open+resolve never exceeds
// batch. A quota of zero means the phase makes no request in that run.
func phaseQuotas(batch, run int) (open, resolve int) {
	if batch == 1 {
		if run%2 == 0 {
			return 1, 0
		}
		return 0, 1
	}
	return (batch + 1) / 2, batch / 2
}

// phasePageLimit stays constant for a phase's configuration and across runs.
// The caller supplies a positive quota and MaxPages is validated at startup.
func phasePageLimit(quota, maxPages int) int { return 1 + (quota-1)/maxPages }

// LowStockAlerts opens an alert for every product at or below the threshold
// and resolves the OPEN alerts whose product recovered (ADR-007 Decision 6,
// ADR-018). It keeps no business state: Products owns alerts and the rule that
// a product has one OPEN alert at most. The only memory is the page cursors
// and the count of runs, which say where the next run resumes reading.
type LowStockAlerts struct {
	gw  port.ProductsGateway
	cfg Config
	log *slog.Logger

	running       sync.Mutex
	runs          int // runs that started; selects the phase of a batch of 1
	productCursor int // next page of the low-stock product list, from 1
	alertCursor   int // next page of the OPEN alert list, from 1
}

var _ port.LowStockAlertSync = (*LowStockAlerts)(nil)

func NewLowStockAlerts(gw port.ProductsGateway, cfg Config, logger *slog.Logger) (*LowStockAlerts, error) {
	switch {
	case gw == nil:
		return nil, errors.New("products gateway is required")
	case cfg.Threshold < 0:
		return nil, errors.New("threshold must not be negative")
	case cfg.BatchSize < 1 || cfg.BatchSize > MaxPageSize:
		return nil, fmt.Errorf("batch size must be between 1 and %d", MaxPageSize)
	case cfg.MaxPages < 1:
		return nil, errors.New("max pages must be at least 1")
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &LowStockAlerts{gw: gw, cfg: cfg, log: logger, productCursor: 1, alertCursor: 1}, nil
}

// Run performs one open phase and one resolve phase, within the run budget.
//
// The run identity is the correlation id carried by ctx. It must be a valid
// UUID: otherwise Run fails before any request, so nothing can be mutated and
// no key is built from a substitute identity. The same identity forms every
// Idempotency-Key of the run; no other identity is generated here.
//
// It returns a non-nil error only when the whole run must stop: the identity
// is invalid, the SERVICE credential was rejected, Products stayed
// unavailable, or ctx ended. Failures of single items and page reads are
// counted in the report's Failed field and the run goes on with independent
// work.
//
// When both phases have a quota, the open phase gets half of the time left
// before ctx's deadline, so a slow Products read cannot leave the resolve
// phase without budget run after run.
func (u *LowStockAlerts) Run(ctx context.Context) (port.SyncReport, error) {
	if !u.running.TryLock() {
		return port.SyncReport{}, ErrRunInProgress
	}
	defer u.running.Unlock()

	runID := port.CorrelationIDFrom(ctx)
	if err := model.ValidateRunID(runID); err != nil {
		return port.SyncReport{}, fmt.Errorf("run not started: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return port.SyncReport{}, fmt.Errorf("run not started: %w", err)
	}

	openQuota, resolveQuota := phaseQuotas(u.cfg.BatchSize, u.runs)
	u.runs++
	r := &run{
		u:    u,
		id:   runID,
		log:  u.log.With("correlationId", runID, "runId", runID),
		seen: map[string]int{},
	}

	if openQuota > 0 {
		openCtx, cancel := ctx, context.CancelFunc(func() {})
		if resolveQuota > 0 {
			openCtx, cancel = halfOfRemainingTime(ctx)
		}
		err := r.openPhase(openCtx, openQuota)
		cancel()
		if err != nil {
			return r.rep, err
		}
		if err := ctx.Err(); err != nil {
			return r.rep, fmt.Errorf("run cancelled after the open phase: %w", err)
		}
	}

	if resolveQuota > 0 {
		if err := r.resolvePhase(ctx, resolveQuota); err != nil {
			return r.rep, err
		}
	}
	return r.rep, nil
}

// halfOfRemainingTime bounds a phase to half of what is left before ctx's
// deadline. Without a deadline the phase is only bounded by cancellation.
func halfOfRemainingTime(ctx context.Context) (context.Context, context.CancelFunc) {
	if dl, ok := ctx.Deadline(); ok {
		if remaining := time.Until(dl); remaining > 0 {
			return context.WithTimeout(ctx, remaining/2)
		}
	}
	return context.WithCancel(ctx)
}

// run is the state of one Run call.
type run struct {
	u   *LowStockAlerts
	id  string // the run identity: also the runId of every key
	log *slog.Logger
	rep port.SyncReport

	// seen holds the stock of every low product read in this run, so the
	// resolve phase does not fetch again what it already knows is low, and a
	// product returned twice (shifted pages) gets one opening intention only.
	seen map[string]int

	consecutiveUnavailable int
}

// itemFailed counts a failed item or read and decides whether the phase must
// stop. A non-nil result aborts the whole run.
//
// Interrupted work is counted too: a mutation that was in flight when the
// context ended has an unconfirmed outcome.
func (r *run) itemFailed(ctx context.Context, what string, err error, attrs ...any) error {
	r.rep.Failed++
	r.log.Warn(what+" failed", append(attrs, "error", err)...)

	switch {
	case errors.Is(err, port.ErrCredentialRejected):
		return fmt.Errorf("run aborted: %w", err)
	case ctx.Err() != nil:
		return fmt.Errorf("run interrupted: %w", ctx.Err())
	case errors.Is(err, port.ErrUnavailable):
		r.consecutiveUnavailable++
		if r.consecutiveUnavailable >= maxConsecutiveUnavailable {
			return fmt.Errorf("run aborted after %d consecutive items with products unavailable: %w",
				r.consecutiveUnavailable, port.ErrUnavailable)
		}
	default:
		r.consecutiveUnavailable = 0
	}
	return nil
}

func (r *run) itemSucceeded() { r.consecutiveUnavailable = 0 }

// oversized reports a page with more items than the limit asked for: Products
// ignored the limit, so the page cannot be trusted to respect the budget.
func (r *run) oversized(ctx context.Context, what string, got, limit, page int) error {
	return r.itemFailed(ctx, what, fmt.Errorf("%w: %d items in a page of at most %d", port.ErrMalformedResponse, got, limit), "page", page)
}

// ─── open phase ──────────────────────────────────────────────────────────

// openPhase runs under a context derived from the run's. When only that
// context ended (the phase spent its share of the time) the run goes on with
// the resolve phase; Run itself checks the run's own context right after.
func (r *run) openPhase(phase context.Context, quota int) error {
	err := r.openLowStock(phase, quota)
	if err == nil || errors.Is(err, port.ErrCredentialRejected) || errors.Is(err, port.ErrUnavailable) {
		return err
	}
	if phase.Err() != nil {
		return nil
	}
	return err
}

// openLowStock reserves a fixed page of admission slots before every read.
// Duplicate rows consume slots too. The cursor advances only after the entire
// returned page is evaluated, so cancellation cannot discard its tail.
func (r *run) openLowStock(ctx context.Context, quota int) error {
	u := r.u
	page := u.productCursor
	remaining := quota
	limit := phasePageLimit(quota, u.cfg.MaxPages)
	for read := 0; read < u.cfg.MaxPages && remaining >= limit; read++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		remaining -= limit // reserve the whole possible response before requesting it
		res, err := u.gw.ListProductsAtMost(ctx, u.cfg.Threshold, page, limit)
		if err != nil {
			// The cursor stays: the next run retries this page.
			u.productCursor = page
			return r.itemFailed(ctx, "reading low-stock products", err, "page", page)
		}
		r.consecutiveUnavailable = 0
		if len(res.Items) > limit {
			u.productCursor = page
			return r.oversized(ctx, "reading low-stock products", len(res.Items), limit, page)
		}

		items := res.Items

		for _, p := range items {
			if err := ctx.Err(); err != nil {
				u.productCursor = page
				return err
			}
			if _, dup := r.seen[p.ID]; dup {
				continue // the catalog shifted between page reads
			}
			if err := r.openOne(ctx, p); err != nil {
				u.productCursor = page
				return err
			}
		}

		switch {
		case page >= res.Info.TotalPages || len(res.Items) == 0:
			u.productCursor = 1 // end of the list: the next run starts a new pass
			return nil
		}
		page++
		u.productCursor = page
	}
	return nil
}

func (r *run) openOne(ctx context.Context, p model.Product) error {
	if !model.IsLowStock(p.Stock, r.u.cfg.Threshold) {
		// The stockAtMost filter was not honoured; opening an alert for this
		// product would be wrong, and the answer cannot be trusted as a whole.
		return r.itemFailed(ctx, "opening alert", fmt.Errorf("%w: product above the threshold in a stockAtMost list", port.ErrMalformedResponse),
			"productId", p.ID, "stock", p.Stock)
	}
	r.seen[p.ID] = p.Stock
	r.rep.ProductsExamined++

	// The intention is built once, here, from this observation. The gateway
	// re-sends exactly this key and body on every retry; nothing below builds
	// another one for this product in this run.
	intent, err := model.NewOpeningIntent(p.ID, p.Stock, r.id)
	if err != nil {
		return r.itemFailed(ctx, "opening alert", err, "productId", p.ID)
	}
	res, err := r.u.gw.OpenAlert(ctx, intent)
	if err != nil {
		return r.itemFailed(ctx, "opening alert", err, "productId", p.ID, "stock", p.Stock)
	}
	if res.Alert.ProductID != p.ID {
		return r.itemFailed(ctx, "opening alert",
			fmt.Errorf("%w: answer names another product", port.ErrMalformedResponse), "productId", p.ID)
	}
	switch res.Alert.Status {
	case model.AlertOpen:
		r.itemSucceeded()
		if res.Created {
			r.rep.Opened++
		} else {
			r.rep.AlreadyOpen++
		}
		return nil
	case model.AlertResolved:
		// A valid answer (ADR-018): a recorded key replayed after resolution,
		// or a resolution that committed before the representation was read,
		// with 200 or 201. The HTTP code does not say which, so no replay is
		// inferred. The product is low and no OPEN alert is confirmed: the
		// opening is unsatisfied, not failed, and no other key is tried in
		// this run. A later run reads the stock again under a new identity.
		r.itemSucceeded()
		r.rep.ReconciliationPending++
		r.log.Warn("opening reconciliation pending: Products answered a RESOLVED alert",
			"productId", p.ID, "alertId", res.Alert.ID, "status", res.Alert.Status,
			"created", res.Created, "idempotencyKey", intent.Key)
		return nil
	default:
		return r.itemFailed(ctx, "opening alert",
			fmt.Errorf("%w: answer is a %s alert", port.ErrMalformedResponse, res.Alert.Status),
			"productId", p.ID, "alertId", res.Alert.ID)
	}
}

// ─── resolve phase ───────────────────────────────────────────────────────

// resolvePhase selects at most `quota` OPEN alerts from the persisted cursor,
// evaluates them, and only then decides where the next run resumes.
//
// The portion is collected before any alert is mutated: resolving removes
// alerts from the OPEN list and would shift the pages still to be read. For
// the same reason the cursor stays at the first page of the portion when the
// run resolved something (the list shrank, so those positions now hold the
// elements that follow), moves past it when nothing was resolved, and does not
// move if the portion was interrupted before it was fully evaluated.
func (r *run) resolvePhase(ctx context.Context, quota int) error {
	u := r.u
	start := u.alertCursor
	alerts, next, err := r.collectOpenAlerts(ctx, quota)
	// Alerts read before an error are still worth processing, unless the
	// whole run stops.
	if err != nil && (errors.Is(err, port.ErrCredentialRejected) || errors.Is(err, port.ErrUnavailable) || ctx.Err() != nil) {
		return err
	}

	resolvedBefore := r.rep.Resolved
	handled := map[string]bool{}
	for _, a := range alerts {
		if err := ctx.Err(); err != nil {
			u.alertCursor = start
			return fmt.Errorf("run interrupted: %w", err)
		}
		if handled[a.ID] {
			continue
		}
		handled[a.ID] = true
		if err := r.resolveOne(ctx, a); err != nil {
			u.alertCursor = start
			return err
		}
	}

	if r.rep.Resolved > resolvedBefore {
		u.alertCursor = start
	} else {
		u.alertCursor = next
	}
	return nil
}

// collectOpenAlerts reserves fixed-width pages within the phase quota before
// any mutations and returns where an unresolved pass would continue. Duplicate
// and unused response slots are not reclaimed within the run.
func (r *run) collectOpenAlerts(ctx context.Context, quota int) (alerts []model.StockAlert, next int, err error) {
	u := r.u
	page := u.alertCursor
	next = page
	remaining := quota
	limit := phasePageLimit(quota, u.cfg.MaxPages)
	for read := 0; read < u.cfg.MaxPages && remaining >= limit; read++ {
		if err := ctx.Err(); err != nil {
			return alerts, next, fmt.Errorf("run interrupted: %w", err)
		}
		remaining -= limit // short responses never authorize an oversized next request
		res, err := u.gw.ListAlerts(ctx, model.AlertOpen, page, limit)
		if err != nil {
			return alerts, next, r.itemFailed(ctx, "reading open alerts", err, "page", page)
		}
		r.consecutiveUnavailable = 0
		if len(res.Items) > limit {
			return alerts, next, r.oversized(ctx, "reading open alerts", len(res.Items), limit, page)
		}

		items := res.Items
		alerts = append(alerts, items...)

		switch {
		case page >= res.Info.TotalPages || len(res.Items) == 0:
			return alerts, 1, nil // end of the list: the next pass starts again
		}
		page++
		next = page
	}
	return alerts, next, nil
}

func (r *run) resolveOne(ctx context.Context, a model.StockAlert) error {
	r.rep.AlertsExamined++
	if a.Status != model.AlertOpen {
		return r.itemFailed(ctx, "resolving alert",
			fmt.Errorf("%w: listed alert is %s, not OPEN", port.ErrMalformedResponse, a.Status), "alertId", a.ID)
	}

	stock, known := r.seen[a.ProductID]
	if !known {
		p, err := r.u.gw.GetProduct(ctx, a.ProductID)
		if err != nil {
			// Not found, timeout or garbage: none of them says the product
			// recovered, so the alert is left as it is.
			return r.itemFailed(ctx, "reading product of alert", err, "alertId", a.ID, "productId", a.ProductID)
		}
		if p.ID != a.ProductID {
			return r.itemFailed(ctx, "reading product of alert",
				fmt.Errorf("%w: answer names another product", port.ErrMalformedResponse), "alertId", a.ID, "productId", a.ProductID)
		}
		stock = p.Stock
	}

	if model.IsLowStock(stock, r.u.cfg.Threshold) {
		r.itemSucceeded()
		r.rep.StillLow++
		return nil
	}

	resolved, err := r.u.gw.ResolveAlert(ctx, a.ID)
	if err != nil {
		return r.itemFailed(ctx, "resolving alert", err, "alertId", a.ID, "productId", a.ProductID)
	}
	if resolved.ID != a.ID || resolved.Status != model.AlertResolved {
		return r.itemFailed(ctx, "resolving alert",
			fmt.Errorf("%w: answer is not the RESOLVED alert requested", port.ErrMalformedResponse),
			"alertId", a.ID, "productId", a.ProductID)
	}
	r.itemSucceeded()
	r.rep.Resolved++
	return nil
}
