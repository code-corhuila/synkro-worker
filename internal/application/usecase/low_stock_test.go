package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/code-corhuila/synkro-worker/internal/application/port"
	"github.com/code-corhuila/synkro-worker/internal/domain/model"
)

var runCounter atomic.Int64

// runID returns a valid, distinct canonical UUID for run number n.
func runID(n int64) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", n) }

// freshCtx is the context of a new run: the scheduler gives every run its own
// identity, so each call here stands for one more scheduled execution.
func freshCtx() context.Context {
	return port.WithCorrelationID(context.Background(), runID(runCounter.Add(1)))
}

func ctxOf(id string) context.Context { return port.WithCorrelationID(context.Background(), id) }

func newUC(t *testing.T, gw port.ProductsGateway, cfg Config) (*LowStockAlerts, *logBuffer) {
	t.Helper()
	logs := &logBuffer{}
	uc, err := NewLowStockAlerts(gw, cfg, logs.logger())
	if err != nil {
		t.Fatalf("NewLowStockAlerts: %v", err)
	}
	return uc, logs
}

// cfg builds a Config; batch is the TOTAL number of reconciliation items of a
// run (BATCH_SIZE).
func cfg(threshold, batch, maxPages int) Config {
	return Config{Threshold: threshold, BatchSize: batch, MaxPages: maxPages}
}

func mustRun(t *testing.T, uc *LowStockAlerts) port.SyncReport {
	t.Helper()
	rep, err := uc.Run(freshCtx())
	if err != nil {
		t.Fatalf("Run returned an error: %v", err)
	}
	return rep
}

func TestRun_OpensAlertsForZeroAndAtThresholdStock(t *testing.T) {
	gw := newFake(
		model.Product{ID: "zero", Stock: 0},
		model.Product{ID: "edge", Stock: 5},
		model.Product{ID: "fine", Stock: 6},
	)
	uc, _ := newUC(t, gw, cfg(5, 100, 1))

	rep := mustRun(t, uc)

	if rep.Opened != 2 || rep.ProductsExamined != 2 || rep.Failed != 0 {
		t.Fatalf("want 2 opened/2 examined/0 failed, got %+v", rep)
	}
	if gw.openAlertCount("zero") != 1 || gw.openAlertCount("edge") != 1 {
		t.Error("expected one OPEN alert for the zero-stock and the at-threshold product")
	}
	if gw.openAlertCount("fine") != 0 {
		t.Error("a product above the threshold must not get an alert")
	}
}

func TestRun_SendsStockAtOpeningAndTheRunScopedKey(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 3})
	uc, _ := newUC(t, gw, cfg(5, 100, 1))
	id := runID(900)

	if _, err := uc.Run(ctxOf(id)); err != nil {
		t.Fatal(err)
	}

	if len(gw.keys) != 1 || gw.keys[0] != "low-stock:p1:"+id {
		t.Fatalf("want key low-stock:p1:%s, got %v", id, gw.keys)
	}
	if got := gw.alerts[0].StockAtOpening; got != 3 {
		t.Errorf("stockAtOpening: want 3, got %d", got)
	}
}

func TestRun_RepeatingDoesNotDuplicateActiveAlerts(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 1}, model.Product{ID: "p2", Stock: 2})
	uc, _ := newUC(t, gw, cfg(5, 100, 1))

	first := mustRun(t, uc)
	second := mustRun(t, uc)

	if first.Opened != 2 || first.AlreadyOpen != 0 {
		t.Fatalf("first run: want 2 opened, got %+v", first)
	}
	if second.Opened != 0 || second.AlreadyOpen != 2 || second.Failed != 0 {
		t.Fatalf("second run: want 0 opened/2 already open, got %+v", second)
	}
	if len(gw.alerts) != 2 {
		t.Errorf("want exactly 2 alerts after two runs, got %d", len(gw.alerts))
	}
}

func TestRun_ResolvesAnAlertWhenItsProductIsAboveTheThresholdAgain(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 40})
	gw.addOpenAlert("a1", "p1")
	uc, _ := newUC(t, gw, cfg(5, 100, 1))

	rep := mustRun(t, uc)

	if rep.Resolved != 1 || rep.AlertsExamined != 1 || rep.Failed != 0 {
		t.Fatalf("want 1 resolved, got %+v", rep)
	}
	if gw.alerts[0].Status != model.AlertResolved {
		t.Error("alert should be RESOLVED")
	}
	// A second run finds no OPEN alert left; resolution is not attempted again.
	again := mustRun(t, uc)
	if again.Resolved != 0 || gw.callsNamed("resolve") != 1 {
		t.Errorf("second run must not resolve again: %+v, resolve calls=%d", again, gw.callsNamed("resolve"))
	}
}

func TestRun_LeavesAlertOpenWhileTheProductIsStillLow(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 2})
	gw.addOpenAlert("a1", "p1")
	uc, _ := newUC(t, gw, cfg(5, 100, 1))

	rep := mustRun(t, uc)

	if rep.StillLow != 1 || rep.Resolved != 0 {
		t.Fatalf("want 1 still-low and 0 resolved, got %+v", rep)
	}
	if gw.callsNamed("resolve") != 0 {
		t.Error("an alert whose product is still low must not be resolved")
	}
	if gw.callsNamed("get-product") != 0 {
		t.Error("a product already read as low in this run must not be fetched again")
	}
}

func TestRun_EmptyBatchesDoNothingAndSucceed(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 50})
	uc, _ := newUC(t, gw, cfg(5, 100, 2))

	rep := mustRun(t, uc)

	if rep != (port.SyncReport{}) {
		t.Fatalf("want an all-zero report, got %+v", rep)
	}
	if gw.callsNamed("list-products") != 1 || gw.callsNamed("list-alerts") != 1 {
		t.Errorf("an empty first page ends each phase: calls=%v", gw.calls)
	}
}

func TestRun_FollowsShortPagesUntilTheItemBudgetIsSpent(t *testing.T) {
	var ps []model.Product
	for i := 1; i <= 5; i++ {
		ps = append(ps, model.Product{ID: fmt.Sprintf("p%d", i), Stock: 1})
	}
	gw := newFake(ps...)
	gw.pageCap = 1                      // one-item pages, stable width 1 for this configuration
	uc, _ := newUC(t, gw, cfg(5, 6, 5)) // open quota 3, resolve quota 3

	rep := mustRun(t, uc)

	if rep.Opened != 3 {
		t.Fatalf("the open quota is 3 items: want 3 opened, got %+v", rep)
	}
	if got := gw.callsNamed("list-products"); got != 3 {
		t.Errorf("3 one-item pages spend the quota of 3 (MaxPages=5 is only a cap): got %d reads (%v)", got, gw.calls)
	}
}

func TestRun_MaxPagesCapsPagesAndNeverRaisesTheItemBudget(t *testing.T) {
	var ps []model.Product
	for i := 1; i <= 9; i++ {
		ps = append(ps, model.Product{ID: fmt.Sprintf("p%d", i), Stock: 1})
	}
	gw := newFake(ps...)
	gw.pageCap = 1
	uc, _ := newUC(t, gw, cfg(5, 8, 2)) // quota 4 admits at most 2 fixed-width pages of 2

	rep := mustRun(t, uc)

	if rep.Opened != 2 || gw.callsNamed("list-products") != 2 {
		t.Fatalf("MaxPages=2 caps the phase at 2 one-item pages: %+v calls=%v", rep, gw.calls)
	}

	big := newFake(ps...)
	uc, _ = newUC(t, big, cfg(5, 6, 100)) // a huge MaxPages does not enlarge the quota
	if rep := mustRun(t, uc); rep.ProductsExamined != 3 {
		t.Fatalf("a run examines the quota (3), not MaxPages*quota: %+v", rep)
	}
}

func TestRun_StopsAtTheWorkBudgetAndContinuesFromThereNextRun(t *testing.T) {
	var ps []model.Product
	for i := 1; i <= 5; i++ {
		ps = append(ps, model.Product{ID: fmt.Sprintf("p%d", i), Stock: 1})
	}
	gw := newFake(ps...)
	uc, _ := newUC(t, gw, cfg(5, 4, 1)) // batch 4: one page of 2 per phase per run

	var opened []int
	for i := 0; i < 4; i++ {
		opened = append(opened, mustRun(t, uc).Opened)
	}

	// pages: [p1 p2] [p3 p4] [p5] then wraps to [p1 p2] (already open).
	if fmt.Sprint(opened) != "[2 2 1 0]" {
		t.Fatalf("opened per run: want [2 2 1 0], got %v", opened)
	}
	var pages []string
	for _, c := range gw.calls {
		if strings.HasPrefix(c, "list-products") {
			pages = append(pages, strings.TrimPrefix(c, "list-products "))
		}
	}
	if fmt.Sprint(pages) != "[p=1 p=2 p=3 p=1]" {
		t.Errorf("product pages per run: want [p=1 p=2 p=3 p=1], got %v", pages)
	}
	if len(gw.alerts) != 5 {
		t.Errorf("every low product must end with one alert, got %d alerts", len(gw.alerts))
	}
}

func TestRun_ReadsAllAlertPagesBeforeResolvingSoResolutionsDoNotShiftPages(t *testing.T) {
	var ps []model.Product
	gw := newFake(ps...)
	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("p%d", i)
		gw.products = append(gw.products, model.Product{ID: id, Stock: 50})
		gw.addOpenAlert("a"+id, id)
	}
	gw.pageCap = 1                      // short pages: the resolve quota (2) spans two page reads
	uc, _ := newUC(t, gw, cfg(5, 4, 2)) // open quota 2, resolve quota 2

	rep := mustRun(t, uc)

	if rep.Resolved != 2 {
		t.Fatalf("want the resolve quota (2) resolved in one run, got %+v (calls %v)", rep, gw.calls)
	}
	lastList, firstResolve := -1, -1
	for i, c := range gw.calls {
		if strings.HasPrefix(c, "list-alerts") {
			lastList = i
		}
		if strings.HasPrefix(c, "resolve") && firstResolve < 0 {
			firstResolve = i
		}
	}
	if lastList > firstResolve {
		t.Errorf("alert pages must be read before resolving: %v", gw.calls)
	}
	// The next run starts again at page 1 (resolved alerts left the list), so
	// the two alerts that were not selected are not skipped.
	if rest := mustRun(t, uc); rest.Resolved != 2 {
		t.Errorf("the remaining alerts must be resolved by the next run: %+v", rest)
	}
}

// WRK-KEY-07: a product seen on two pages is one opening intention, and the
// duplicate still spends item budget (the third page is never requested).
func TestRun_AProductSeenOnTwoPagesIsOpenedOnceAndTheDuplicateSpendsBudget(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 3})
	// The catalog shifts between page reads: the same product comes twice.
	dup := &duplicatingGateway{fakeProducts: gw}
	uc, _ := newUC(t, dup, cfg(5, 4, 5)) // open quota 2

	rep := mustRun(t, uc)

	if rep.Opened != 1 || gw.callsNamed("open") != 1 {
		t.Fatalf("want a single opening, got %+v (calls %v)", rep, gw.calls)
	}
	if got := gw.callsNamed("list-products"); got != 2 {
		t.Errorf("two items received spend the quota of 2; got %d page reads (%v)", got, gw.calls)
	}
}

func TestRun_ADuplicateWithAnotherObservationKeepsTheFirstIntention(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 3})
	uc, _ := newUC(t, &changingStockGateway{fakeProducts: gw}, cfg(5, 4, 5))

	rep := mustRun(t, uc)

	if rep.Opened != 1 || len(gw.intents) != 1 || gw.intents[0].StockAtOpening != 3 {
		t.Fatalf("one intention with the first observation (3) expected: %+v intents=%+v", rep, gw.intents)
	}
}

// changingStockGateway answers page 2 with the same product, now at stock 1.
type changingStockGateway struct{ *fakeProducts }

func (c *changingStockGateway) ListProductsAtMost(ctx context.Context, max, page, limit int) (port.ProductPage, error) {
	p, err := c.fakeProducts.ListProductsAtMost(ctx, max, 1, limit)
	p.Info = port.PageInfo{Page: page, Limit: limit, Total: 2, TotalPages: 2}
	if page == 2 {
		p.Items = []model.Product{{ID: p.Items[0].ID, Stock: 1}}
	}
	return p, err
}

// duplicatingGateway answers page 2 with the product of page 1 again.
type duplicatingGateway struct{ *fakeProducts }

func (d *duplicatingGateway) ListProductsAtMost(ctx context.Context, max, page, limit int) (port.ProductPage, error) {
	if page == 2 {
		first, err := d.fakeProducts.ListProductsAtMost(ctx, max, 1, limit)
		first.Info = port.PageInfo{Page: 2, Limit: limit, Total: 3, TotalPages: 3}
		return first, err
	}
	p, err := d.fakeProducts.ListProductsAtMost(ctx, max, page, limit)
	p.Info.TotalPages, p.Info.Total = 3, 3
	return p, err
}

func TestRun_AFailingItemIsCountedAndTheNextOneStillRuns(t *testing.T) {
	gw := newFake(
		model.Product{ID: "p1", Stock: 1},
		model.Product{ID: "p2", Stock: 1},
		model.Product{ID: "p3", Stock: 1},
	)
	gw.openErr["p2"] = fmt.Errorf("open: %w", port.ErrUnavailable)
	uc, _ := newUC(t, gw, cfg(5, 100, 1))

	rep, err := uc.Run(freshCtx())

	if err != nil {
		t.Fatalf("an isolated item failure must not abort the run: %v", err)
	}
	if rep.Opened != 2 || rep.Failed != 1 {
		t.Fatalf("want 2 opened and 1 failed, got %+v", rep)
	}
	if gw.openAlertCount("p2") != 0 {
		t.Error("the failed item must not be reported as opened")
	}
	if gw.callsNamed("open p3") != 1 {
		t.Error("independent work after the failure must continue")
	}
}

func TestRun_APermanentRejectionOfOneItemDoesNotStopTheOthers(t *testing.T) {
	gw := newFake(
		model.Product{ID: "p1", Stock: 1},
		model.Product{ID: "p2", Stock: 1},
		model.Product{ID: "p3", Stock: 1},
		model.Product{ID: "p4", Stock: 1},
		model.Product{ID: "p5", Stock: 1},
	)
	for _, id := range []string{"p1", "p2", "p3", "p4"} {
		gw.openErr[id] = fmt.Errorf("open: %w", port.ErrRejected) // e.g. 422 inactive product
	}
	uc, _ := newUC(t, gw, cfg(5, 100, 1))

	rep, err := uc.Run(freshCtx())

	if err != nil {
		t.Fatalf("permanent per-item rejections are not run-level failures: %v", err)
	}
	if rep.Failed != 4 || rep.Opened != 1 {
		t.Fatalf("want 4 failed and 1 opened, got %+v", rep)
	}
}

func TestRun_AFailureResolvingOneAlertDoesNotStopTheOthers(t *testing.T) {
	gw := newFake(
		model.Product{ID: "p1", Stock: 50},
		model.Product{ID: "p2", Stock: 50},
		model.Product{ID: "p3", Stock: 50},
	)
	gw.addOpenAlert("a1", "p1")
	gw.addOpenAlert("a2", "p2")
	gw.addOpenAlert("a3", "p3")
	gw.resolveErr["a2"] = fmt.Errorf("resolve: %w", port.ErrUnavailable)
	uc, _ := newUC(t, gw, cfg(5, 100, 1))

	rep := mustRun(t, uc)

	if rep.Resolved != 2 || rep.Failed != 1 {
		t.Fatalf("want 2 resolved and 1 failed, got %+v", rep)
	}
	if gw.alerts[1].Status != model.AlertOpen {
		t.Error("the alert whose resolution failed must not be claimed as resolved")
	}
}

func TestRun_ARejectedCredentialAbortsTheRunWithoutFurtherRequests(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 1}, model.Product{ID: "p2", Stock: 1})
	gw.addOpenAlert("a9", "p9")
	gw.openErr["p1"] = fmt.Errorf("open: %w", port.ErrCredentialRejected)
	uc, _ := newUC(t, gw, cfg(5, 100, 1))

	rep, err := uc.Run(freshCtx())

	if !errors.Is(err, port.ErrCredentialRejected) {
		t.Fatalf("want ErrCredentialRejected, got %v", err)
	}
	if gw.callsNamed("open") != 1 || gw.callsNamed("list-alerts") != 0 {
		t.Errorf("no request may follow a rejected credential: %v", gw.calls)
	}
	if rep.Failed != 1 || rep.Opened != 0 {
		t.Errorf("want 1 failed, 0 opened, got %+v", rep)
	}
}

func TestRun_AbortsWhenProductsStaysUnavailableForSeveralItemsInARow(t *testing.T) {
	var ps []model.Product
	for i := 1; i <= 6; i++ {
		id := fmt.Sprintf("p%d", i)
		ps = append(ps, model.Product{ID: id, Stock: 1})
	}
	gw := newFake(ps...)
	for _, p := range ps {
		gw.openErr[p.ID] = fmt.Errorf("open: %w", port.ErrUnavailable)
	}
	uc, _ := newUC(t, gw, cfg(5, 100, 1))

	rep, err := uc.Run(freshCtx())

	if !errors.Is(err, port.ErrUnavailable) {
		t.Fatalf("want ErrUnavailable once Products keeps failing, got %v", err)
	}
	if got := gw.callsNamed("open"); got != maxConsecutiveUnavailable {
		t.Errorf("want %d attempts before giving up, got %d", maxConsecutiveUnavailable, got)
	}
	if rep.Failed != maxConsecutiveUnavailable || gw.callsNamed("list-alerts") != 0 {
		t.Errorf("run must stop before the next phase: %+v calls=%v", rep, gw.calls)
	}
}

func TestRun_ASuccessResetsTheConsecutiveUnavailableCount(t *testing.T) {
	var ps []model.Product
	for i := 1; i <= 6; i++ {
		ps = append(ps, model.Product{ID: fmt.Sprintf("p%d", i), Stock: 1})
	}
	gw := newFake(ps...)
	for _, id := range []string{"p1", "p2", "p4", "p5"} {
		gw.openErr[id] = fmt.Errorf("open: %w", port.ErrUnavailable)
	}
	uc, _ := newUC(t, gw, cfg(5, 100, 1))

	rep, err := uc.Run(freshCtx())

	if err != nil {
		t.Fatalf("failures interleaved with successes must not abort: %v", err)
	}
	if rep.Failed != 4 || rep.Opened != 2 {
		t.Fatalf("want 4 failed and 2 opened, got %+v", rep)
	}
}

func TestRun_ContradictoryOrMalformedAnswersAreNeverCountedAsSuccess(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*fakeProducts)
	}{
		{"alert for another product", func(f *fakeProducts) {
			f.badOpenAlert = func(_ model.OpeningIntent, a model.StockAlert) model.StockAlert {
				a.ProductID = "someone-else"
				return a
			}
		}},
		{"alert with an unexpected status", func(f *fakeProducts) {
			f.badOpenAlert = func(_ model.OpeningIntent, a model.StockAlert) model.StockAlert {
				a.Status = "CLOSED"
				return a
			}
		}},
		{"product above the threshold in a stockAtMost page", func(f *fakeProducts) {
			f.ignoreFilter = true
			f.products = []model.Product{{ID: "p1", Stock: 99}}
		}},
		{"malformed response error from the adapter", func(f *fakeProducts) {
			f.openErr["p1"] = fmt.Errorf("open: %w", port.ErrMalformedResponse)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gw := newFake(model.Product{ID: "p1", Stock: 1})
			tc.setup(gw)
			uc, _ := newUC(t, gw, cfg(5, 100, 1))

			rep := mustRun(t, uc)

			if rep.Opened != 0 || rep.AlreadyOpen != 0 || rep.Failed != 1 {
				t.Fatalf("want 0 opened and 1 failed, got %+v", rep)
			}
		})
	}
}

// â”€â”€â”€ ADR-018: run-scoped identity (WRK-KEY-01..09) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

// WRK-KEY-02: a new run (new key) finds the product's OPEN alert: no second
// alert, and the original stockAtOpening is not overwritten.
func TestRun_ANewRunReusesTheOpenAlertAndKeepsItsOriginalStock(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 3})
	uc, _ := newUC(t, gw, cfg(5, 100, 1))
	first, second := runID(701), runID(702)

	r1, err1 := uc.Run(ctxOf(first))
	gw.products[0].Stock = 1
	r2, err2 := uc.Run(ctxOf(second))

	if err1 != nil || err2 != nil {
		t.Fatalf("runs failed: %v / %v", err1, err2)
	}
	if r1.Opened != 1 || r2.Opened != 0 || r2.AlreadyOpen != 1 || r2.Failed != 0 {
		t.Fatalf("want 1 opened then 1 already open, got %+v / %+v", r1, r2)
	}
	if len(gw.alerts) != 1 || gw.alerts[0].StockAtOpening != 3 {
		t.Errorf("one alert keeping stockAtOpening 3 expected: %+v", gw.alerts)
	}
	if gw.keys[0] != "low-stock:p1:"+first || gw.keys[1] != "low-stock:p1:"+second {
		t.Errorf("each run must use its own key: %v", gw.keys)
	}
}

// WRK-KEY-03: opened, replenished and resolved, then low again the same day:
// the next run's fresh key creates a distinct A2 and A1 stays RESOLVED.
func TestRun_ALowStockAfterAnObservedResolutionOpensADistinctAlert(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 1})
	uc, _ := newUC(t, gw, cfg(5, 100, 1))
	mustRun(t, uc) // opens A1
	gw.products[0].Stock = 30
	if rep := mustRun(t, uc); rep.Resolved != 1 {
		t.Fatalf("setup: A1 should be resolved: %+v", rep)
	}
	gw.products[0].Stock = 2

	rep := mustRun(t, uc)

	if rep.Opened != 1 || rep.Failed != 0 || rep.ReconciliationPending != 0 {
		t.Fatalf("want a new alert opened, got %+v", rep)
	}
	if len(gw.alerts) != 2 || gw.alerts[0].Status != model.AlertResolved || gw.alerts[1].Status != model.AlertOpen {
		t.Fatalf("A1 must stay RESOLVED and A2 be OPEN: %+v %+v", gw.alerts[0], gw.alerts[1])
	}
	if gw.alerts[0].ID == gw.alerts[1].ID {
		t.Error("A2 must be a different resource")
	}
}

// WRK-KEY-04: replaying A1's recorded key returns A1 unchanged even while A2
// exists (Products' rule, mimicked by the fake); the Worker reads the valid
// RESOLVED state without counting it as an opening or as malformed.
func TestRun_AReplayOfAnOldKeyReturnsTheResolvedAlertAndIsNotCountedAsOpen(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 1})
	uc, _ := newUC(t, gw, cfg(5, 100, 1))
	firstRun := runID(801)
	if _, err := uc.Run(ctxOf(firstRun)); err != nil {
		t.Fatal(err)
	}
	a1 := gw.alerts[0].ID
	gw.products[0].Stock = 30
	mustRun(t, uc) // resolves A1
	gw.products[0].Stock = 2
	mustRun(t, uc) // creates A2
	if len(gw.alerts) != 2 {
		t.Fatalf("setup: want A1 and A2, got %d", len(gw.alerts))
	}

	replay, err := gw.OpenAlert(context.Background(), model.OpeningIntent{ProductID: "p1", StockAtOpening: 1, Key: "low-stock:p1:" + firstRun})

	if err != nil || replay.Alert.ID != a1 || replay.Alert.Status != model.AlertResolved || replay.Created {
		t.Fatalf("the replay must return A1 RESOLVED without creating: %+v / %v", replay, err)
	}
	if len(gw.alerts) != 2 || gw.openAlertCount("p1") != 1 {
		t.Errorf("a replay must create or reopen nothing: %+v", gw.alerts)
	}
}

// WRK-KEY-09 (controlled responses): a valid RESOLVED body to a fresh key,
// with 200 or 201, is a pending reconciliation: not Opened, not AlreadyOpen,
// not Failed, not malformed, and no second key is issued in the same run.
func TestRun_AValidResolvedAnswerIsAPendingReconciliationWith200And201(t *testing.T) {
	for _, created := range []bool{false, true} {
		t.Run(fmt.Sprintf("created=%v", created), func(t *testing.T) {
			gw := newFake(model.Product{ID: "p1", Stock: 1})
			gw.openReply = func(_ model.OpeningIntent, r port.OpenAlertResult) port.OpenAlertResult {
				gw.resolveStored(r.Alert.ID) // the resolution committed before the read
				r.Alert.Status = model.AlertResolved
				r.Created = created
				return r
			}
			uc, logs := newUC(t, gw, cfg(5, 100, 1))

			rep, err := uc.Run(freshCtx())

			if err != nil {
				t.Fatalf("a pending reconciliation is not a run-level error: %v", err)
			}
			if rep.ReconciliationPending != 1 || rep.Opened != 0 || rep.AlreadyOpen != 0 || rep.Failed != 0 {
				t.Fatalf("want only ReconciliationPending=1, got %+v", rep)
			}
			if rep.Processed() != 0 {
				t.Errorf("a pending reconciliation is not a processed item: %d", rep.Processed())
			}
			if len(gw.keys) != 1 {
				t.Errorf("no second key may be issued in the same run: %v", gw.keys)
			}
			out := logs.String()
			if strings.Contains(out, "malformed") || !strings.Contains(out, "reconciliation pending") || !strings.Contains(out, "RESOLVED") {
				t.Errorf("the log must name the pending reconciliation, not a schema error: %s", out)
			}
		})
	}
}

func TestRun_APendingReconciliationDoesNotStopIndependentWorkAndAFutureRunReevaluates(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 1}, model.Product{ID: "p2", Stock: 1})
	gw.openReply = func(in model.OpeningIntent, r port.OpenAlertResult) port.OpenAlertResult {
		if in.ProductID == "p1" {
			r.Alert.Status = model.AlertResolved
		}
		return r
	}
	uc, _ := newUC(t, gw, cfg(5, 100, 1))

	rep := mustRun(t, uc)
	if rep.ReconciliationPending != 1 || rep.Opened != 1 {
		t.Fatalf("p1 pending, p2 opened expected: %+v", rep)
	}

	gw.openReply = nil // the concurrent resolution is over
	gw.alerts[0].Status = model.AlertResolved
	next := mustRun(t, uc)
	if next.Opened != 1 || next.ReconciliationPending != 0 {
		t.Errorf("the next run re-evaluates the product with a new key: %+v", next)
	}
}

// WRK-KEY-01 (use-case side): the intention built before the first POST is
// the one every retry sends, even when the observation or the day changes
// between attempts. The wrapper plays the part of the adapter's retry loop.
func TestRun_TheIntentionIsFrozenAcrossRetriesEvenIfStockChanges(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 3})
	retrying := &lostResponseGateway{fakeProducts: gw}
	uc, _ := newUC(t, retrying, cfg(5, 100, 1))

	rep := mustRun(t, uc)

	if rep.Opened+rep.AlreadyOpen != 1 || rep.Failed != 0 {
		t.Fatalf("the retry must succeed: %+v", rep)
	}
	if len(gw.intents) != 2 || gw.intents[0] != gw.intents[1] {
		t.Fatalf("both attempts must carry the identical intention: %+v", gw.intents)
	}
	if gw.intents[1].StockAtOpening != 3 {
		t.Errorf("a later observation (stock 0) must not change the retry body: %+v", gw.intents[1])
	}
	if len(gw.alerts) != 1 {
		t.Errorf("the committed alert must be selected again, not duplicated: %d alerts", len(gw.alerts))
	}
}

// lostResponseGateway commits the first POST, loses its answer, changes the
// product's stock and retries with the very intent it was handed.
type lostResponseGateway struct{ *fakeProducts }

func (l *lostResponseGateway) OpenAlert(ctx context.Context, in model.OpeningIntent) (port.OpenAlertResult, error) {
	if _, err := l.fakeProducts.OpenAlert(ctx, in); err != nil {
		return port.OpenAlertResult{}, err
	}
	l.fakeProducts.mu.Lock()
	l.fakeProducts.products[0].Stock = 0
	l.fakeProducts.mu.Unlock()
	return l.fakeProducts.OpenAlert(ctx, in)
}

// WRK-KEY-05: a new process starts a new identity; nothing of the previous
// run is resumed and the existing OPEN alert stays unique.
func TestRun_ARestartedWorkerUsesANewIdentityAndReusesTheOpenAlert(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 3})
	before, _ := newUC(t, gw, cfg(5, 100, 1))
	if _, err := before.Run(ctxOf(runID(501))); err != nil {
		t.Fatal(err)
	}

	after, _ := newUC(t, gw, cfg(5, 100, 1)) // restart: new instance, no memory
	rep, err := after.Run(ctxOf(runID(502)))

	if err != nil || rep.AlreadyOpen != 1 || rep.Opened != 0 {
		t.Fatalf("want the existing alert reused: %+v / %v", rep, err)
	}
	if gw.keys[1] != "low-stock:p1:"+runID(502) || gw.keys[0] == gw.keys[1] {
		t.Errorf("the restarted worker must use a fresh key: %v", gw.keys)
	}
	if len(gw.alerts) != 1 {
		t.Errorf("one alert only: %d", len(gw.alerts))
	}
}

// WRK-KEY-08 (application boundary, also for direct invocation).
func TestRun_AMissingOrInvalidIdentityAbortsBeforeAnyRequest(t *testing.T) {
	for name, ctx := range map[string]context.Context{
		"no identity":        context.Background(),
		"empty":              ctxOf(""),
		"timestamp fallback": ctxOf("run-1767225600000000000"),
		"nil uuid":           ctxOf("00000000-0000-0000-0000-000000000000"),
		"not a uuid":         ctxOf("corr-9"),
	} {
		t.Run(name, func(t *testing.T) {
			gw := newFake(model.Product{ID: "p1", Stock: 1})
			gw.addOpenAlert("a1", "p9")
			uc, _ := newUC(t, gw, cfg(5, 100, 1))

			rep, err := uc.Run(ctx)

			if !errors.Is(err, model.ErrInvalidRunID) {
				t.Fatalf("want ErrInvalidRunID, got %v", err)
			}
			if len(gw.calls) != 0 || len(gw.keys) != 0 || rep != (port.SyncReport{}) {
				t.Errorf("not a single request may be made: calls=%v report=%+v", gw.calls, rep)
			}

			// The next run with a valid identity recovers.
			if got := mustRun(t, uc); got.Opened != 1 {
				t.Errorf("a later valid run must work: %+v", got)
			}
		})
	}
}

func TestRun_AnInvalidIdentityDoesNotConsumeTheBatch1Alternation(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 1})
	uc, _ := newUC(t, gw, cfg(5, 1, 1))

	if _, err := uc.Run(context.Background()); err == nil {
		t.Fatal("want an error")
	}

	if rep := mustRun(t, uc); rep.Opened != 1 {
		t.Errorf("the first valid run must still be the open turn: %+v", rep)
	}
}

// â”€â”€â”€ total run budget (BATCH_SIZE is the TOTAL, not per phase) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

func TestPhaseQuotas_SplitTheTotalBatchStablyAndNeverExceedIt(t *testing.T) {
	for _, tc := range []struct {
		batch, run            int
		wantOpen, wantResolve int
	}{
		{100, 0, 50, 50}, {100, 1, 50, 50}, {5, 0, 3, 2}, {3, 7, 2, 1}, {2, 0, 1, 1}, {2, 1, 1, 1},
		{1, 0, 1, 0}, {1, 1, 0, 1}, {1, 2, 1, 0}, {1, 3, 0, 1},
	} {
		open, resolve := phaseQuotas(tc.batch, tc.run)
		if open != tc.wantOpen || resolve != tc.wantResolve {
			t.Errorf("phaseQuotas(%d, %d) = (%d, %d), want (%d, %d)", tc.batch, tc.run, open, resolve, tc.wantOpen, tc.wantResolve)
		}
		if open+resolve > tc.batch {
			t.Errorf("phaseQuotas(%d, %d) exceeds the batch", tc.batch, tc.run)
		}
	}
}

func lowProducts(n int) []model.Product {
	var ps []model.Product
	for i := 1; i <= n; i++ {
		ps = append(ps, model.Product{ID: fmt.Sprintf("p%03d", i), Stock: 1})
	}
	return ps
}

// staleAlerts gives n OPEN alerts whose products are above the threshold.
func staleAlerts(gw *fakeProducts, n int) {
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("s%03d", i)
		gw.products = append(gw.products, model.Product{ID: id, Stock: 50})
		gw.addOpenAlert("a"+id, id)
	}
}

func TestRun_BothPhasesFullStillStayWithinOneBatchInTotal(t *testing.T) {
	gw := newFake(lowProducts(300)...)
	staleAlerts(gw, 300)
	uc, _ := newUC(t, gw, cfg(5, 100, 5))

	rep := mustRun(t, uc)

	if rep.ProductsExamined != 50 || rep.AlertsExamined != 50 {
		t.Fatalf("BATCH_SIZE=100 is the total: want 50 + 50, got %+v", rep)
	}
	if rep.ProductsExamined+rep.AlertsExamined > 100 {
		t.Fatalf("a run may not examine more than BATCH_SIZE elements: %+v", rep)
	}
	if rep.Resolved != 50 {
		t.Errorf("resolution must advance even though the product list is full: %+v", rep)
	}
	if fmt.Sprint(gw.productLimits) != "[10 10 10 10 10]" || fmt.Sprint(gw.alertLimits) != "[10 10 10 10 10]" {
		t.Errorf("fixed pages must stay within each phase quota: products=%v alerts=%v", gw.productLimits, gw.alertLimits)
	}
}

func TestRun_AnOddBatchGivesTheExtraItemToTheOpenPhase(t *testing.T) {
	gw := newFake(lowProducts(10)...)
	staleAlerts(gw, 10)
	uc, _ := newUC(t, gw, cfg(5, 5, 5))

	rep := mustRun(t, uc)

	if rep.ProductsExamined != 3 || rep.AlertsExamined != 2 {
		t.Fatalf("batch 5 = 3 open + 2 resolve, got %+v", rep)
	}
}

func TestRun_Batch1AlternatesPhasesAndBothAdvanceAcrossRuns(t *testing.T) {
	gw := newFake(lowProducts(3)...)
	staleAlerts(gw, 3)
	uc, _ := newUC(t, gw, cfg(5, 1, 1))

	var reports []port.SyncReport
	for i := 0; i < 8; i++ {
		reports = append(reports, mustRun(t, uc))
	}

	for i, r := range reports {
		if r.ProductsExamined+r.AlertsExamined > 1 {
			t.Fatalf("run %d examined more than BATCH_SIZE=1: %+v", i, r)
		}
		openTurn := i%2 == 0
		if openTurn && (r.ProductsExamined != 1 || r.AlertsExamined != 0) {
			t.Errorf("run %d should be an open turn: %+v", i, r)
		}
		if !openTurn && (r.AlertsExamined != 1 || r.ProductsExamined != 0) {
			t.Errorf("run %d should be a resolve turn: %+v", i, r)
		}
	}
	resolved := 0
	for _, r := range reports {
		resolved += r.Resolved
	}
	if resolved != 3 {
		t.Errorf("the 3 stale alerts must be resolved within the 4 resolve turns: %d", resolved)
	}
	if gw.callsNamed("open p001") == 0 || gw.callsNamed("open p002") == 0 || gw.callsNamed("open p003") == 0 {
		t.Errorf("the open turns must rotate over all low products: %v", gw.calls)
	}
	// A phase with no quota makes no request at all.
	for i, limits := range [][]int{gw.productLimits, gw.alertLimits} {
		if len(limits) != 4 {
			t.Errorf("list %d: want 4 reads in 8 runs of batch 1, got %v", i, limits)
		}
		for _, l := range limits {
			if l != 1 {
				t.Errorf("list %d: the limit must stay 1: %v", i, limits)
			}
		}
	}
}

func TestRun_Batch2GivesBothPhasesOneItemEveryRun(t *testing.T) {
	gw := newFake(lowProducts(4)...)
	staleAlerts(gw, 4)
	uc, _ := newUC(t, gw, cfg(5, 2, 3))

	for i := 0; i < 3; i++ {
		rep := mustRun(t, uc)
		if rep.ProductsExamined != 1 || rep.AlertsExamined != 1 {
			t.Fatalf("run %d: want 1 + 1, got %+v", i, rep)
		}
	}
}

func TestRun_TheLimitOfAPhaseNeverChangesAcrossPagesRunsOrFullLists(t *testing.T) {
	gw := newFake(lowProducts(40)...)
	staleAlerts(gw, 40)
	uc, _ := newUC(t, gw, cfg(5, 14, 3)) // 7 + 7

	for i := 0; i < 6; i++ {
		mustRun(t, uc)
	}

	for _, l := range append(append([]int{}, gw.productLimits...), gw.alertLimits...) {
		if l != 3 {
			t.Fatalf("quota 7 and max-pages 3 keep a stable page limit of 3: products=%v alerts=%v", gw.productLimits, gw.alertLimits)
		}
	}
}

func TestRun_ProductCursorAdvancesPageByPageWithoutSkippingAndWraps(t *testing.T) {
	gw := newFake(lowProducts(5)...)
	uc, _ := newUC(t, gw, cfg(5, 4, 3)) // open quota 2

	var firstOfRun []string
	for i := 0; i < 4; i++ {
		before := len(gw.intents)
		mustRun(t, uc)
		firstOfRun = append(firstOfRun, gw.intents[before].ProductID)
	}

	if fmt.Sprint(firstOfRun) != "[p001 p003 p005 p001]" {
		t.Errorf("pages [p001 p002] [p003 p004] [p005] then wrap, got first items %v", firstOfRun)
	}
}

// Resolving removes alerts from the OPEN list: the cursor must not move past
// a page from which the run resolved something, or the shifted items skip.
func TestRun_AlertCursorDoesNotSkipItemsShiftedByResolutions(t *testing.T) {
	gw := newFake()
	for i := 1; i <= 6; i++ {
		id := fmt.Sprintf("p%d", i)
		stock := 1
		if i <= 2 {
			stock = 50 // a1, a2 recovered
		}
		gw.products = append(gw.products, model.Product{ID: id, Stock: stock})
		gw.addOpenAlert("a"+id, id)
	}
	uc, _ := newUC(t, gw, cfg(5, 4, 1)) // open quota 2, resolve quota 2

	var rep []port.SyncReport
	for i := 0; i < 3; i++ {
		rep = append(rep, mustRun(t, uc))
	}

	var pages []string
	for _, c := range gw.calls {
		if strings.HasPrefix(c, "list-alerts") {
			pages = append(pages, strings.TrimPrefix(c, "list-alerts "))
		}
	}
	if fmt.Sprint(pages) != "[p=1 p=1 p=2]" {
		t.Errorf("after resolving from page 1 the next run must re-read page 1, then advance: %v", pages)
	}
	if rep[0].Resolved != 2 || rep[1].StillLow != 2 || rep[2].StillLow != 2 {
		t.Errorf("all six alerts must be evaluated over three runs: %+v", rep)
	}
}

func TestRun_AlertCursorWrapsAfterTheLastPage(t *testing.T) {
	gw := newFake()
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("p%d", i)
		gw.products = append(gw.products, model.Product{ID: id, Stock: 1})
		gw.addOpenAlert("a"+id, id)
	}
	uc, _ := newUC(t, gw, cfg(5, 4, 1)) // resolve quota 2: pages [a1 a2] [a3]

	for i := 0; i < 3; i++ {
		mustRun(t, uc)
	}

	var pages []string
	for _, c := range gw.calls {
		if strings.HasPrefix(c, "list-alerts") {
			pages = append(pages, strings.TrimPrefix(c, "list-alerts "))
		}
	}
	if fmt.Sprint(pages) != "[p=1 p=2 p=1]" {
		t.Errorf("want pages 1, 2 then a new pass at 1, got %v", pages)
	}
}

func TestRun_CancellationWhileEvaluatingAPortionDoesNotAdvanceTheAlertCursor(t *testing.T) {
	gw := newFake()
	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("p%d", i)
		gw.products = append(gw.products, model.Product{ID: id, Stock: 50})
		gw.addOpenAlert("a"+id, id)
	}
	uc, _ := newUC(t, gw, cfg(5, 4, 2)) // resolve quota 2: [a1 a2] [a3 a4]
	ctx, cancel := context.WithCancel(freshCtx())
	defer cancel()
	gw.beforeCall = func(_ context.Context, name string) error {
		if name == "get-product p1" {
			cancel()
			return fmt.Errorf("get: %w", context.Canceled)
		}
		return nil
	}

	_, err := uc.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	gw.beforeCall = nil
	gw.calls = nil
	mustRun(t, uc)

	var pages []string
	for _, c := range gw.calls {
		if strings.HasPrefix(c, "list-alerts") {
			pages = append(pages, strings.TrimPrefix(c, "list-alerts "))
		}
	}
	if len(pages) == 0 || pages[0] != "p=1" {
		t.Errorf("the unevaluated page 1 must be read again, got %v", pages)
	}
}

func TestRun_CancellationWhileOpeningDoesNotAdvanceTheProductCursor(t *testing.T) {
	gw := newFake(lowProducts(4)...)
	uc, _ := newUC(t, gw, cfg(5, 4, 1)) // open quota 2: [p001 p002] [p003 p004]
	ctx, cancel := context.WithCancel(freshCtx())
	defer cancel()
	gw.beforeCall = func(_ context.Context, name string) error {
		if name == "open p002" {
			cancel()
			return fmt.Errorf("open: %w", context.Canceled)
		}
		return nil
	}
	if _, err := uc.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	gw.beforeCall = nil
	gw.calls = nil

	mustRun(t, uc)

	if got := gw.callsNamed("open p001"); got != 1 {
		t.Errorf("page 1 was not fully evaluated and must be read again: %v", gw.calls)
	}
}

func TestRun_APageLargerThanItsLimitIsMalformedAndNothingIsProcessed(t *testing.T) {
	gw := newFake(lowProducts(5)...)
	oversized := &oversizedGateway{fakeProducts: gw}
	uc, _ := newUC(t, oversized, cfg(5, 4, 2)) // quota 2, Products answers 5 items

	rep := mustRun(t, uc)

	if rep.Failed != 1 || rep.ProductsExamined != 0 || gw.callsNamed("open") != 0 {
		t.Fatalf("an oversized page must not widen the budget: %+v calls=%v", rep, gw.calls)
	}
}

// oversizedGateway ignores the requested limit.
type oversizedGateway struct{ *fakeProducts }

func (o *oversizedGateway) ListProductsAtMost(ctx context.Context, max, page, limit int) (port.ProductPage, error) {
	return o.fakeProducts.ListProductsAtMost(ctx, max, page, 100)
}

func TestRun_AResolveItemCostsAtMostOnePointReadAndOneMutation(t *testing.T) {
	gw := newFake()
	staleAlerts(gw, 10)
	uc, _ := newUC(t, gw, cfg(5, 6, 5)) // resolve quota 3

	rep := mustRun(t, uc)

	if rep.Resolved != 3 || gw.callsNamed("get-product") != 3 || gw.callsNamed("resolve") != 3 {
		t.Fatalf("3 selected alerts: 3 point reads and 3 mutations, got %+v calls=%v", rep, gw.calls)
	}
}

func TestRun_ADuplicateAlertInTheSelectedPortionIsHandledOnce(t *testing.T) {
	gw := newFake()
	staleAlerts(gw, 1)
	gw.alerts = append(gw.alerts, &model.StockAlert{ID: gw.alerts[0].ID, ProductID: gw.alerts[0].ProductID, Status: model.AlertOpen})
	uc, _ := newUC(t, gw, cfg(5, 6, 5))

	rep := mustRun(t, uc)

	if rep.Resolved != 1 || gw.callsNamed("resolve") != 1 {
		t.Fatalf("one alert id, one resolution: %+v calls=%v", rep, gw.calls)
	}
}

func TestRun_ResolveAnswerThatIsNotResolvedIsFailed(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 50})
	gw.addOpenAlert("a1", "p1")
	gw.badResolveStatus = true
	uc, _ := newUC(t, gw, cfg(5, 100, 1))

	rep := mustRun(t, uc)

	if rep.Resolved != 0 || rep.Failed != 1 {
		t.Fatalf("want 0 resolved and 1 failed, got %+v", rep)
	}
}

func TestRun_AMissingProductIsNotProofOfReplenishment(t *testing.T) {
	gw := newFake() // no products at all
	gw.addOpenAlert("a1", "ghost")
	uc, _ := newUC(t, gw, cfg(5, 100, 1))

	rep := mustRun(t, uc)

	if gw.callsNamed("resolve") != 0 {
		t.Fatal("a 404 on the product must never resolve its alert")
	}
	if rep.Failed != 1 || rep.Resolved != 0 {
		t.Fatalf("want 1 failed, got %+v", rep)
	}
}

func TestRun_AnUnreadableProductNeverResolvesItsAlert(t *testing.T) {
	for name, err := range map[string]error{
		"timeout":   port.ErrUnavailable,
		"malformed": port.ErrMalformedResponse,
	} {
		t.Run(name, func(t *testing.T) {
			gw := newFake(model.Product{ID: "p1", Stock: 50})
			gw.addOpenAlert("a1", "p1")
			gw.getErr["p1"] = fmt.Errorf("get: %w", err)
			uc, _ := newUC(t, gw, cfg(5, 100, 1))

			rep := mustRun(t, uc)

			if gw.callsNamed("resolve") != 0 || rep.Failed != 1 {
				t.Fatalf("no resolution without a valid product read: %+v calls=%v", rep, gw.calls)
			}
		})
	}
}

func TestRun_AFailedPageReadIsCountedAndRetriedOnTheNextRun(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 1})
	gw.listErr["products:1"] = fmt.Errorf("list: %w", port.ErrUnavailable)
	uc, _ := newUC(t, gw, cfg(5, 100, 1))

	first, err := uc.Run(freshCtx())
	if err != nil {
		t.Fatalf("a failed read must not abort the run: %v", err)
	}
	if first.Failed != 1 || first.Opened != 0 {
		t.Fatalf("first run: want 1 failed, got %+v", first)
	}
	if gw.callsNamed("list-alerts") != 1 {
		t.Error("the resolution phase must still run after a failed product read")
	}

	delete(gw.listErr, "products:1")
	second := mustRun(t, uc)
	if second.Opened != 1 {
		t.Fatalf("second run must retry the same page: %+v", second)
	}
}

func TestRun_CancellationDuringAnItemStartsNoFurtherItem(t *testing.T) {
	gw := newFake(
		model.Product{ID: "p1", Stock: 1},
		model.Product{ID: "p2", Stock: 1},
		model.Product{ID: "p3", Stock: 1},
	)
	ctx, cancel := context.WithCancel(freshCtx())
	defer cancel()
	gw.beforeCall = func(_ context.Context, name string) error {
		if name == "open p2" {
			cancel()
			return fmt.Errorf("open: %w", context.Canceled)
		}
		return nil
	}
	uc, _ := newUC(t, gw, cfg(5, 100, 1))

	rep, err := uc.Run(ctx)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if gw.callsNamed("open") != 2 || gw.callsNamed("list-alerts") != 0 {
		t.Errorf("nothing may start after cancellation: %v", gw.calls)
	}
	if rep.Opened != 1 || rep.Failed != 1 {
		t.Errorf("want 1 opened and the interrupted item as failed, got %+v", rep)
	}
}

func TestRun_CancellationObservedBetweenItemsStartsNoFurtherItem(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 1}, model.Product{ID: "p2", Stock: 1})
	ctx, cancel := context.WithCancel(freshCtx())
	defer cancel()
	gw.beforeCall = func(_ context.Context, name string) error {
		if name == "open p1" {
			cancel() // the request itself succeeds; cancellation lands right after
		}
		return nil
	}
	uc, _ := newUC(t, gw, cfg(5, 100, 1))

	rep, err := uc.Run(ctx)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if gw.callsNamed("open") != 1 {
		t.Errorf("p2 must not be started once cancellation is observed: %v", gw.calls)
	}
	if rep.Opened != 1 || rep.Failed != 0 {
		t.Errorf("want 1 opened and no failure, got %+v", rep)
	}
}

func TestRun_AlreadyCancelledContextMakesNoRequest(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 1})
	ctx, cancel := context.WithCancel(freshCtx())
	cancel()
	uc, _ := newUC(t, gw, cfg(5, 100, 1))

	_, err := uc.Run(ctx)

	if !errors.Is(err, context.Canceled) || len(gw.calls) != 0 {
		t.Fatalf("want context.Canceled and no calls, got %v / %v", err, gw.calls)
	}
}

func TestRun_ASlowOpenPhaseCannotStarveTheResolvePhase(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 50})
	gw.addOpenAlert("a1", "p1")
	// Reading products hangs until the phase budget cuts it off.
	gw.beforeCall = func(ctx context.Context, name string) error {
		if strings.HasPrefix(name, "list-products") {
			<-ctx.Done()
			return fmt.Errorf("list: %w", ctx.Err())
		}
		return nil
	}
	uc, _ := newUC(t, gw, cfg(5, 100, 1))
	ctx, cancel := context.WithTimeout(freshCtx(), 2*time.Second)
	defer cancel()

	start := time.Now()
	rep, err := uc.Run(ctx)

	if err != nil {
		t.Fatalf("a spent phase budget is not a run failure: %v", err)
	}
	if rep.Resolved != 1 {
		t.Fatalf("the resolve phase must still run: %+v", rep)
	}
	if rep.Failed != 1 {
		t.Errorf("the interrupted product read must be counted as failed: %+v", rep)
	}
	if time.Since(start) > 1500*time.Millisecond {
		t.Errorf("the open phase must be cut at about half of the run budget, took %v", time.Since(start))
	}
}

func TestRun_RefusesToOverlapWithItself(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 1})
	entered, release := make(chan struct{}), make(chan struct{})
	gw.beforeCall = func(_ context.Context, name string) error {
		if name == "open p1" {
			close(entered)
			<-release
		}
		return nil
	}
	uc, _ := newUC(t, gw, cfg(5, 100, 1))
	done := make(chan error, 1)
	go func() { _, err := uc.Run(freshCtx()); done <- err }()
	<-entered

	_, err := uc.Run(freshCtx())
	close(release)

	if !errors.Is(err, ErrRunInProgress) {
		t.Errorf("want ErrRunInProgress for the overlapping run, got %v", err)
	}
	if err := <-done; err != nil {
		t.Errorf("the first run must finish normally: %v", err)
	}
}

func TestRun_LogsCarryTheCorrelationIDOfTheRun(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 1})
	gw.openErr["p1"] = fmt.Errorf("open: %w", port.ErrRejected)
	uc, logs := newUC(t, gw, cfg(5, 100, 1))
	ctx := ctxOf(runID(42))

	if _, err := uc.Run(ctx); err != nil {
		t.Fatal(err)
	}

	out := logs.String()
	if !strings.Contains(out, `"correlationId":"`+runID(42)+`"`) || !strings.Contains(out, `"runId":"`+runID(42)+`"`) {
		t.Errorf("failure log must carry the run correlation id: %s", out)
	}
	if !strings.Contains(out, "p1") {
		t.Errorf("failure log must name the product: %s", out)
	}
}

func TestNewLowStockAlerts_ValidatesItsConfiguration(t *testing.T) {
	gw := newFake()
	for name, c := range map[string]Config{
		"negative threshold":  cfg(-1, 100, 1),
		"zero batch size":     cfg(5, 0, 1),
		"batch size over 100": cfg(5, 101, 1),
		"zero max pages":      cfg(5, 100, 0),
	} {
		if _, err := NewLowStockAlerts(gw, c, nil); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	if _, err := NewLowStockAlerts(nil, cfg(0, 1, 1), nil); err == nil {
		t.Error("nil gateway: expected an error")
	}
	if _, err := NewLowStockAlerts(gw, cfg(0, 100, 1), nil); err != nil {
		t.Errorf("threshold 0 and a nil logger are valid: %v", err)
	}
}
