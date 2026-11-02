//go:build acceptance

// Package acceptance holds opt-in tests that drive the REAL use case and the
// REAL Products HTTP client against a running Products API (and, through it,
// its PostgreSQL). They are excluded from `go test ./...` by the build tag and
// each test skips itself unless its fixtures are provided.
//
// Run (the token is read from the environment and is never printed, logged or
// passed as an argument):
//
//	TEST_PRODUCTS_URL=http://localhost:8080 \
//	TEST_SERVICE_TOKEN=<SERVICE token of the Worker> \
//	TEST_LOW_PRODUCT_ID=<uuid> ... \
//	go test -tags acceptance -count=1 -v ./internal/acceptance/
//
// Fixtures are isolated products prepared beforehand THROUGH THE PRODUCTS API
// by someone with inventory authority; the Worker's token cannot change stock.
// The tests never create or delete products, never touch other products and
// never use SQL. See docs/handoff/HU-WRK-02-run-contract.md for the exact
// fixture preparation of each scenario.
package acceptance

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/code-corhuila/synkro-worker/internal/adapter/out/products"
	"github.com/code-corhuila/synkro-worker/internal/application/port"
	"github.com/code-corhuila/synkro-worker/internal/application/usecase"
	"github.com/code-corhuila/synkro-worker/internal/domain/model"
)

const threshold = 5

func env(t *testing.T, names ...string) map[string]string {
	t.Helper()
	out := map[string]string{}
	var missing []string
	for _, n := range append([]string{"TEST_PRODUCTS_URL", "TEST_SERVICE_TOKEN"}, names...) {
		v := os.Getenv(n)
		if v == "" {
			missing = append(missing, n)
		}
		out[n] = v
	}
	if len(missing) > 0 {
		t.Skipf("NOT EXECUTED: set %s", strings.Join(missing, ", "))
	}
	return out
}

func newClient(t *testing.T, e map[string]string) *products.Client {
	t.Helper()
	c, err := products.New(products.Options{
		BaseURL:  e["TEST_PRODUCTS_URL"],
		Token:    e["TEST_SERVICE_TOKEN"],
		Timeout:  10 * time.Second,
		Attempts: 3,
	})
	if err != nil {
		t.Fatalf("client: %v", err) // New never echoes the token
	}
	return c
}

func freshRunID(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func runCtx(t *testing.T) (context.Context, string) {
	t.Helper()
	id := freshRunID(t)
	ctx, cancel := context.WithTimeout(port.WithCorrelationID(context.Background(), id), 60*time.Second)
	t.Cleanup(cancel)
	return ctx, id
}

// scoped limits the REAL use case to the fixture products so that a run cannot
// open or resolve alerts of the shared catalog. Reads and the opening POST go
// to the real client; the product list is built from point reads of the
// fixture ids, and the alert list is empty so that no resolution is attempted.
type scoped struct {
	*products.Client
	ids []string
}

func (s scoped) ListProductsAtMost(ctx context.Context, maxStock, page, limit int) (port.ProductPage, error) {
	var items []model.Product
	if page == 1 {
		for _, id := range s.ids {
			p, err := s.Client.GetProduct(ctx, id)
			if err != nil {
				return port.ProductPage{}, err
			}
			if p.Stock <= maxStock {
				items = append(items, p)
			}
		}
	}
	return port.ProductPage{Items: items, Info: port.PageInfo{Page: page, Limit: limit, Total: len(items), TotalPages: 1}}, nil
}

func (s scoped) ListAlerts(_ context.Context, _ model.AlertStatus, page, limit int) (port.AlertPage, error) {
	return port.AlertPage{Info: port.PageInfo{Page: page, Limit: limit, TotalPages: 1}}, nil
}

func newUseCase(t *testing.T, c *products.Client, productIDs ...string) *usecase.LowStockAlerts {
	t.Helper()
	uc, err := usecase.NewLowStockAlerts(scoped{Client: c, ids: productIDs},
		usecase.Config{Threshold: threshold, BatchSize: 2, MaxPages: 1},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return uc
}

// openAlertsOf lists the OPEN alerts of the product through the public API.
func openAlertsOf(t *testing.T, c *products.Client, productID string) []model.StockAlert {
	t.Helper()
	var out []model.StockAlert
	for page := 1; page <= 50; page++ {
		res, err := c.ListAlerts(context.Background(), model.AlertOpen, page, 100)
		if err != nil {
			t.Fatalf("list alerts: %v", err)
		}
		for _, a := range res.Items {
			if a.ProductID == productID {
				out = append(out, a)
			}
		}
		if page >= res.Info.TotalPages || len(res.Items) == 0 {
			return out
		}
	}
	t.Fatal("more than 50 pages of OPEN alerts: the fixture catalog is not isolated")
	return nil
}

// WRK-KEY-02 and WRK-KEY-05 against real Products and PostgreSQL.
//
// Fixture: TEST_LOW_PRODUCT_ID is an isolated product with stock <= 5 whose
// current OPEN alert, if any, may exist already. Two runs with two fresh
// identities (the second plays the part of a restarted Worker) must leave
// exactly one OPEN alert, with its original stockAtOpening.
func TestReal_NewRunKeepsTheSingleOpenAlertAndItsOriginalStock(t *testing.T) {
	e := env(t, "TEST_LOW_PRODUCT_ID")
	c := newClient(t, e)
	pid := e["TEST_LOW_PRODUCT_ID"]
	uc := newUseCase(t, c, pid)

	ctx1, _ := runCtx(t)
	r1, err := uc.Run(ctx1)
	if err != nil {
		t.Fatalf("run 1: %v", err)
	}
	if r1.Opened+r1.AlreadyOpen != 1 || r1.Failed != 0 || r1.ReconciliationPending != 0 {
		t.Fatalf("run 1: want the product opened or already open, got %+v", r1)
	}
	first := openAlertsOf(t, c, pid)
	if len(first) != 1 {
		t.Fatalf("want exactly one OPEN alert after run 1, got %d", len(first))
	}

	uc2 := newUseCase(t, c, pid) // a restarted Worker: new instance, no memory
	ctx2, _ := runCtx(t)
	r2, err := uc2.Run(ctx2)
	if err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if r2.Opened != 0 || r2.AlreadyOpen != 1 || r2.Failed != 0 {
		t.Fatalf("run 2 must reuse the OPEN alert, got %+v", r2)
	}
	second := openAlertsOf(t, c, pid)
	if len(second) != 1 || second[0].ID != first[0].ID || second[0].StockAtOpening != first[0].StockAtOpening {
		t.Fatalf("the OPEN alert must be the same and keep its stockAtOpening: before=%+v after=%+v", first, second)
	}
}

// WRK-KEY-03 and WRK-KEY-04 against real Products.
//
// Fixture (prepared through the API before the test): product P was low, its
// alert A1 was opened with a run-scoped key, stock was raised and A1 resolved,
// then stock was lowered again, so P is low with no OPEN alert.
//
//	TEST_REOPEN_PRODUCT_ID  P
//	TEST_REOPEN_A1_ID       A1 (RESOLVED)
//	TEST_REOPEN_A1_KEY      the Idempotency-Key that created A1
//
// A fresh run must create a distinct A2; replaying A1's key afterwards must
// return A1 RESOLVED while A2 exists, creating nothing.
func TestReal_ASameDayDropAfterResolutionOpensADistinctAlertAndTheOldKeyStillReturnsTheOldOne(t *testing.T) {
	e := env(t, "TEST_REOPEN_PRODUCT_ID", "TEST_REOPEN_A1_ID", "TEST_REOPEN_A1_KEY")
	c := newClient(t, e)
	pid, a1, a1Key := e["TEST_REOPEN_PRODUCT_ID"], e["TEST_REOPEN_A1_ID"], e["TEST_REOPEN_A1_KEY"]
	if got := openAlertsOf(t, c, pid); len(got) != 0 {
		t.Fatalf("fixture precondition: the product must have no OPEN alert, found %d", len(got))
	}

	ctx, _ := runCtx(t)
	rep, err := newUseCase(t, c, pid).Run(ctx)
	if err != nil || rep.Opened != 1 || rep.Failed != 0 || rep.ReconciliationPending != 0 {
		t.Fatalf("a fresh run must create A2: %+v / %v", rep, err)
	}
	open := openAlertsOf(t, c, pid)
	if len(open) != 1 || open[0].ID == a1 {
		t.Fatalf("A2 must be a different OPEN alert than A1: %+v", open)
	}

	replay, err := c.OpenAlert(ctx, model.OpeningIntent{ProductID: pid, StockAtOpening: 0, Key: a1Key})
	if err != nil {
		t.Fatalf("replay of A1's key: %v", err)
	}
	if replay.Alert.ID != a1 || replay.Alert.Status != model.AlertResolved {
		t.Fatalf("the old key must return A1 RESOLVED, got %+v", replay)
	}
	if after := openAlertsOf(t, c, pid); len(after) != 1 || after[0].ID != open[0].ID {
		t.Fatalf("the replay must neither create nor reopen anything: %+v", after)
	}
}

// WRK-KEY-06 against real Products and PostgreSQL: different run keys for the
// same product submitted concurrently must converge on one OPEN alert (the
// partial unique index of the owner). Only the public API is used.
//
// Fixture: TEST_CONCURRENCY_PRODUCT_ID, an isolated product with stock <= 5
// and NO OPEN alert.
func TestReal_ConcurrentDifferentRunKeysProduceOneOpenAlert(t *testing.T) {
	e := env(t, "TEST_CONCURRENCY_PRODUCT_ID")
	c := newClient(t, e)
	pid := e["TEST_CONCURRENCY_PRODUCT_ID"]
	if got := openAlertsOf(t, c, pid); len(got) != 0 {
		t.Fatalf("fixture precondition: the product must have no OPEN alert, found %d", len(got))
	}
	p, err := c.GetProduct(context.Background(), pid)
	if err != nil || p.Stock > threshold {
		t.Fatalf("fixture precondition: the product must be low (stock %d, err %v)", p.Stock, err)
	}

	const workers = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make([]port.OpenAlertResult, workers)
	errs := make([]error, workers)
	for i := 0; i < workers; i++ {
		intent, err := model.NewOpeningIntent(pid, p.Stock, freshRunID(t))
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(i int, intent model.OpeningIntent) {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			results[i], errs[i] = c.OpenAlert(ctx, intent)
		}(i, intent)
	}
	close(start)
	wg.Wait()

	ids := map[string]bool{}
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("request %d failed: %v", i, errs[i])
		}
		ids[results[i].Alert.ID] = true
	}
	if len(ids) != 1 {
		t.Fatalf("all %d requests must resolve to the same alert, got %d distinct alerts", workers, len(ids))
	}
	if open := openAlertsOf(t, c, pid); len(open) != 1 {
		t.Fatalf("exactly one OPEN alert expected, got %d", len(open))
	}
}
