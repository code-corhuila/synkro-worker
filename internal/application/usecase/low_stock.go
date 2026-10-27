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
// Each of the two phases (open, resolve) reads at most MaxPages pages of
// PageSize items, so a run touches at most 2*MaxPages*PageSize list items plus
// one product read per examined alert that was not already read as low in the
// same run. Page cursors persist between runs (in memory only), so a catalog
// larger than one run's budget is covered over successive runs.
type Config struct {
	Threshold int
	PageSize  int
	MaxPages  int
}

// LowStockAlerts opens an alert for every product at or below the threshold
// and resolves the OPEN alerts whose product recovered (ADR-007 Decision 6).
// It keeps no business state: Products owns alerts and the rule that a product
// has one OPEN alert at most. The only memory is the page cursors, which say
// where the next run resumes reading.
type LowStockAlerts struct {
	gw    port.ProductsGateway
	clock port.Clock
	cfg   Config
	log   *slog.Logger

	running       sync.Mutex
	productCursor int // next page of the low-stock product list, from 1
	alertCursor   int // next page of the OPEN alert list, from 1
}

var _ port.LowStockAlertSync = (*LowStockAlerts)(nil)

func NewLowStockAlerts(gw port.ProductsGateway, clock port.Clock, cfg Config, logger *slog.Logger) (*LowStockAlerts, error) {
	switch {
	case gw == nil:
		return nil, errors.New("products gateway is required")
	case clock == nil:
		return nil, errors.New("clock is required")
	case cfg.Threshold < 0:
		return nil, errors.New("threshold must not be negative")
	case cfg.PageSize < 1 || cfg.PageSize > MaxPageSize:
		return nil, fmt.Errorf("page size must be between 1 and %d", MaxPageSize)
	case cfg.MaxPages < 1:
		return nil, errors.New("max pages must be at least 1")
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &LowStockAlerts{gw: gw, clock: clock, cfg: cfg, log: logger, productCursor: 1, alertCursor: 1}, nil
}

// Run performs one open phase and one resolve phase.
//
// It returns a non-nil error only when the whole run must stop: the SERVICE
// credential was rejected, Products stayed unavailable, or ctx ended. Failures
// of single items and page reads are counted in the report's Failed field and
// the run goes on with independent work.
//
// The open phase gets half of the time left before ctx's deadline, so a slow
// Products read cannot leave the resolve phase without budget run after run.
func (u *LowStockAlerts) Run(ctx context.Context) (port.SyncReport, error) {
	if !u.running.TryLock() {
		return port.SyncReport{}, ErrRunInProgress
	}
	defer u.running.Unlock()

	if err := ctx.Err(); err != nil {
		return port.SyncReport{}, fmt.Errorf("run not started: %w", err)
	}
	r := &run{
		u:    u,
		log:  u.log.With("correlationId", port.CorrelationIDFrom(ctx)),
		seen: map[string]int{},
	}

	openCtx, cancel := halfOfRemainingTime(ctx)
	err := r.openPhase(openCtx)
	cancel()
	if err != nil {
		return r.rep, err
	}
	if err := ctx.Err(); err != nil {
		return r.rep, fmt.Errorf("run cancelled after the open phase: %w", err)
	}

	if err := r.resolvePhase(ctx); err != nil {
		return r.rep, err
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
	log *slog.Logger
	rep port.SyncReport

	// seen holds the stock of every low product read in this run, so the
	// resolve phase does not fetch again what it already knows is low.
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

// ─── open phase ──────────────────────────────────────────────────────────

// openPhase runs under a context derived from the run's. When only that
// context ended (the phase spent its share of the time) the run goes on with
// the resolve phase; Run itself checks the run's own context right after.
func (r *run) openPhase(phase context.Context) error {
	err := r.openLowStock(phase)
	if err == nil || errors.Is(err, port.ErrCredentialRejected) || errors.Is(err, port.ErrUnavailable) {
		return err
	}
	if phase.Err() != nil {
		return nil
	}
	return err
}

func (r *run) openLowStock(ctx context.Context) error {
	u := r.u
	page := u.productCursor
	for read := 0; read < u.cfg.MaxPages; read++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		res, err := u.gw.ListProductsAtMost(ctx, u.cfg.Threshold, page, u.cfg.PageSize)
		if err != nil {
			// The cursor stays: the next run retries this page.
			u.productCursor = page
			return r.itemFailed(ctx, "reading low-stock products", err, "page", page)
		}
		r.consecutiveUnavailable = 0

		for _, p := range res.Items {
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

		if page >= res.Info.TotalPages || len(res.Items) == 0 {
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

	intent, err := model.NewOpeningIntent(p.ID, p.Stock, r.u.clock.Now())
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
	if res.Alert.Status != model.AlertOpen {
		// Products replayed the idempotency key with the alert it first
		// produced, which has been resolved since: the product is low but has
		// no OPEN alert. Counting it as opened would hide that.
		return r.itemFailed(ctx, "opening alert",
			fmt.Errorf("%w: answer is a %s alert, not an OPEN one (idempotency key replayed after resolution?)",
				port.ErrMalformedResponse, res.Alert.Status),
			"productId", p.ID, "alertId", res.Alert.ID, "idempotencyKey", intent.Key)
	}

	r.itemSucceeded()
	if res.Created {
		r.rep.Opened++
	} else {
		r.rep.AlreadyOpen++
	}
	return nil
}

// ─── resolve phase ───────────────────────────────────────────────────────

func (r *run) resolvePhase(ctx context.Context) error {
	alerts, err := r.collectOpenAlerts(ctx)
	// Alerts read before an error are still worth processing, unless the
	// whole run stops.
	if err != nil && (errors.Is(err, port.ErrCredentialRejected) || errors.Is(err, port.ErrUnavailable) || ctx.Err() != nil) {
		return err
	}

	handled := map[string]bool{}
	for _, a := range alerts {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("run interrupted: %w", err)
		}
		if handled[a.ID] {
			continue
		}
		handled[a.ID] = true
		if err := r.resolveOne(ctx, a); err != nil {
			return err
		}
	}
	return nil
}

// collectOpenAlerts reads the OPEN alert pages of this run before any alert
// is resolved: resolving removes alerts from the OPEN list and would shift
// the pages still to be read.
func (r *run) collectOpenAlerts(ctx context.Context) ([]model.StockAlert, error) {
	u := r.u
	var alerts []model.StockAlert
	page := u.alertCursor
	for read := 0; read < u.cfg.MaxPages; read++ {
		if err := ctx.Err(); err != nil {
			return alerts, fmt.Errorf("run interrupted: %w", err)
		}
		res, err := u.gw.ListAlerts(ctx, model.AlertOpen, page, u.cfg.PageSize)
		if err != nil {
			u.alertCursor = page
			return alerts, r.itemFailed(ctx, "reading open alerts", err, "page", page)
		}
		r.consecutiveUnavailable = 0
		alerts = append(alerts, res.Items...)

		if page >= res.Info.TotalPages || len(res.Items) == 0 {
			u.alertCursor = 1
			return alerts, nil
		}
		page++
		u.alertCursor = page
	}
	return alerts, nil
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
