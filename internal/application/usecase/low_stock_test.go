package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/code-corhuila/synkro-worker/internal/application/port"
	"github.com/code-corhuila/synkro-worker/internal/domain/model"
)

var day1 = time.Date(2026, 3, 4, 10, 0, 0, 0, time.UTC)

func newUC(t *testing.T, gw port.ProductsGateway, cfg Config) (*LowStockAlerts, *fixedClock, *logBuffer) {
	t.Helper()
	clock := &fixedClock{t: day1}
	logs := &logBuffer{}
	uc, err := NewLowStockAlerts(gw, clock, cfg, logs.logger())
	if err != nil {
		t.Fatalf("NewLowStockAlerts: %v", err)
	}
	return uc, clock, logs
}

func cfg(threshold, pageSize, maxPages int) Config {
	return Config{Threshold: threshold, PageSize: pageSize, MaxPages: maxPages}
}

func mustRun(t *testing.T, uc *LowStockAlerts) port.SyncReport {
	t.Helper()
	rep, err := uc.Run(context.Background())
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
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

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

func TestRun_SendsStockAtOpeningAndTheDailyKeyOfTheUTCDate(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 3})
	uc, clock, _ := newUC(t, gw, cfg(5, 100, 1))
	// 23:30 in UTC-5 is already the next UTC day.
	clock.set(time.Date(2026, 3, 4, 23, 30, 0, 0, time.FixedZone("UTC-5", -5*3600)))

	mustRun(t, uc)

	if len(gw.keys) != 1 || gw.keys[0] != "low-stock:p1:2026-03-05" {
		t.Fatalf("want key low-stock:p1:2026-03-05, got %v", gw.keys)
	}
	if got := gw.alerts[0].StockAtOpening; got != 3 {
		t.Errorf("stockAtOpening: want 3, got %d", got)
	}
}

func TestRun_RepeatingDoesNotDuplicateActiveAlerts(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 1}, model.Product{ID: "p2", Stock: 2})
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

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
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

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
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

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
	uc, _, _ := newUC(t, gw, cfg(5, 100, 2))

	rep := mustRun(t, uc)

	if rep != (port.SyncReport{}) {
		t.Fatalf("want an all-zero report, got %+v", rep)
	}
	if gw.callsNamed("list-products") != 1 || gw.callsNamed("list-alerts") != 1 {
		t.Errorf("an empty first page ends each phase: calls=%v", gw.calls)
	}
}

func TestRun_FollowsSeveralPagesWithinTheRunBudget(t *testing.T) {
	var ps []model.Product
	for i := 1; i <= 5; i++ {
		ps = append(ps, model.Product{ID: fmt.Sprintf("p%d", i), Stock: 1})
	}
	gw := newFake(ps...)
	uc, _, _ := newUC(t, gw, cfg(5, 2, 3))

	rep := mustRun(t, uc)

	if rep.Opened != 5 {
		t.Fatalf("5 products over 3 pages of 2: want 5 opened, got %+v", rep)
	}
	if got := gw.callsNamed("list-products"); got != 3 {
		t.Errorf("want 3 product pages read, got %d (%v)", got, gw.calls)
	}
}

func TestRun_StopsAtTheWorkBudgetAndContinuesFromThereNextRun(t *testing.T) {
	var ps []model.Product
	for i := 1; i <= 5; i++ {
		ps = append(ps, model.Product{ID: fmt.Sprintf("p%d", i), Stock: 1})
	}
	gw := newFake(ps...)
	uc, _, _ := newUC(t, gw, cfg(5, 2, 1)) // one page of 2 per phase per run

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
	uc, _, _ := newUC(t, gw, cfg(5, 2, 2))

	rep := mustRun(t, uc)

	if rep.Resolved != 4 {
		t.Fatalf("want all 4 alerts resolved in one run, got %+v (calls %v)", rep, gw.calls)
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
}

func TestRun_AProductSeenOnTwoPagesIsOpenedOnce(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 1})
	// The catalog shifts between page reads: the same product comes twice.
	gw.beforeCall = nil
	uc, _, _ := newUC(t, &duplicatingGateway{fakeProducts: gw}, cfg(5, 1, 2))

	rep := mustRun(t, uc)

	if rep.Opened != 1 || gw.callsNamed("open") != 1 {
		t.Fatalf("want a single opening, got %+v (calls %v)", rep, gw.calls)
	}
}

// duplicatingGateway answers page 2 with the product of page 1 again.
type duplicatingGateway struct{ *fakeProducts }

func (d *duplicatingGateway) ListProductsAtMost(ctx context.Context, max, page, limit int) (port.ProductPage, error) {
	if page == 2 {
		first, err := d.fakeProducts.ListProductsAtMost(ctx, max, 1, limit)
		first.Info = port.PageInfo{Page: 2, Limit: limit, Total: 2, TotalPages: 2}
		return first, err
	}
	p, err := d.fakeProducts.ListProductsAtMost(ctx, max, page, limit)
	p.Info.TotalPages, p.Info.Total = 2, 2
	return p, err
}

func TestRun_AFailingItemIsCountedAndTheNextOneStillRuns(t *testing.T) {
	gw := newFake(
		model.Product{ID: "p1", Stock: 1},
		model.Product{ID: "p2", Stock: 1},
		model.Product{ID: "p3", Stock: 1},
	)
	gw.openErr["p2"] = fmt.Errorf("open: %w", port.ErrUnavailable)
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

	rep, err := uc.Run(context.Background())

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
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

	rep, err := uc.Run(context.Background())

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
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

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
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

	rep, err := uc.Run(context.Background())

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
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

	rep, err := uc.Run(context.Background())

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
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

	rep, err := uc.Run(context.Background())

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
		{"alert answered as RESOLVED", func(f *fakeProducts) {
			f.badOpenAlert = func(_ model.OpeningIntent, a model.StockAlert) model.StockAlert {
				a.Status = model.AlertResolved
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
			uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

			rep := mustRun(t, uc)

			if rep.Opened != 0 || rep.AlreadyOpen != 0 || rep.Failed != 1 {
				t.Fatalf("want 0 opened and 1 failed, got %+v", rep)
			}
		})
	}
}

func TestRun_AKeyReplayedAsResolvedIsFailedNotOpened(t *testing.T) {
	// Same-day drop -> alert opened, resolved, stock low again. Products
	// replays the daily key with the original, now RESOLVED, alert: the
	// product has no OPEN alert, and the Worker must say so.
	gw := newFake(model.Product{ID: "p1", Stock: 1})
	uc, _, logs := newUC(t, gw, cfg(5, 100, 1))
	mustRun(t, uc) // 10:00 opens a1
	gw.products[0].Stock = 30
	mustRun(t, uc) // resolves a1
	gw.products[0].Stock = 2

	rep := mustRun(t, uc) // same UTC day, same key

	if rep.Opened != 0 || rep.AlreadyOpen != 0 || rep.Failed != 1 {
		t.Fatalf("want the replayed RESOLVED alert counted as failed, got %+v", rep)
	}
	if gw.openAlertCount("p1") != 0 {
		t.Fatal("test setup: the fake must show no OPEN alert")
	}
	if !strings.Contains(logs.String(), "RESOLVED") {
		t.Errorf("the log must say the replayed alert is RESOLVED: %s", logs.String())
	}

	// The next UTC day uses a new key, and the alert opens again.
	gw.mu.Lock()
	gw.keys = nil
	gw.mu.Unlock()
	uc.clock.(*fixedClock).set(day1.Add(24 * time.Hour))
	next := mustRun(t, uc)
	if next.Opened != 1 || len(gw.keys) != 1 || gw.keys[0] != "low-stock:p1:2026-03-05" {
		t.Errorf("next day: want 1 opened with the new key, got %+v keys=%v", next, gw.keys)
	}
}

func TestRun_ResolveAnswerThatIsNotResolvedIsFailed(t *testing.T) {
	gw := newFake(model.Product{ID: "p1", Stock: 50})
	gw.addOpenAlert("a1", "p1")
	gw.badResolveStatus = true
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

	rep := mustRun(t, uc)

	if rep.Resolved != 0 || rep.Failed != 1 {
		t.Fatalf("want 0 resolved and 1 failed, got %+v", rep)
	}
}

func TestRun_AMissingProductIsNotProofOfReplenishment(t *testing.T) {
	gw := newFake() // no products at all
	gw.addOpenAlert("a1", "ghost")
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

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
			uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

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
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

	first, err := uc.Run(context.Background())
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gw.beforeCall = func(_ context.Context, name string) error {
		if name == "open p2" {
			cancel()
			return fmt.Errorf("open: %w", context.Canceled)
		}
		return nil
	}
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gw.beforeCall = func(_ context.Context, name string) error {
		if name == "open p1" {
			cancel() // the request itself succeeds; cancellation lands right after
		}
		return nil
	}
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

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
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))

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
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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
	uc, _, _ := newUC(t, gw, cfg(5, 100, 1))
	done := make(chan error, 1)
	go func() { _, err := uc.Run(context.Background()); done <- err }()
	<-entered

	_, err := uc.Run(context.Background())
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
	uc, _, logs := newUC(t, gw, cfg(5, 100, 1))
	ctx := port.WithCorrelationID(context.Background(), "run-corr-42")

	if _, err := uc.Run(ctx); err != nil {
		t.Fatal(err)
	}

	out := logs.String()
	if !strings.Contains(out, `"correlationId":"run-corr-42"`) {
		t.Errorf("failure log must carry the run correlation id: %s", out)
	}
	if !strings.Contains(out, "p1") {
		t.Errorf("failure log must name the product: %s", out)
	}
}

func TestNewLowStockAlerts_ValidatesItsConfiguration(t *testing.T) {
	gw, clock := newFake(), &fixedClock{t: day1}
	for name, c := range map[string]Config{
		"negative threshold": cfg(-1, 100, 1),
		"zero page size":     cfg(5, 0, 1),
		"page size over 100": cfg(5, 101, 1),
		"zero max pages":     cfg(5, 100, 0),
	} {
		if _, err := NewLowStockAlerts(gw, clock, c, nil); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	if _, err := NewLowStockAlerts(nil, clock, cfg(0, 1, 1), nil); err == nil {
		t.Error("nil gateway: expected an error")
	}
	if _, err := NewLowStockAlerts(gw, nil, cfg(0, 1, 1), nil); err == nil {
		t.Error("nil clock: expected an error")
	}
	if _, err := NewLowStockAlerts(gw, clock, cfg(0, 100, 1), nil); err != nil {
		t.Errorf("threshold 0 and a nil logger are valid: %v", err)
	}
}
