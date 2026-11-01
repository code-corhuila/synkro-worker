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
