package usecase

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/code-corhuila/synkro-worker/internal/application/port"
	"github.com/code-corhuila/synkro-worker/internal/domain/model"
)

// fakeProducts is an in-memory stand-in for the Products service. It mimics
// the two behaviours the job depends on: at most one OPEN alert per product,
// and an Idempotency-Key that replays the alert it first produced, as that
// alert is now (even if it has been resolved since).
type fakeProducts struct {
	mu sync.Mutex

	products []model.Product
	alerts   []*model.StockAlert
	byKey    map[string]string // idempotency key -> alert id
	nextID   int

	// Failure injection, keyed by product id or alert id.
	openErr    map[string]error
	getErr     map[string]error
	resolveErr map[string]error
	listErr    map[string]error // "products:<page>" or "alerts:<page>"

	// Behaviour overrides.
	badOpenAlert     func(model.OpeningIntent, model.StockAlert) model.StockAlert
	badResolveStatus bool
	ignoreFilter     bool // return every product, whatever the threshold

	// beforeCall runs at the start of every gateway call, with the call name.
	beforeCall func(ctx context.Context, name string) error

	calls []string
	keys  []string
}

func newFake(products ...model.Product) *fakeProducts {
	return &fakeProducts{
		products:   products,
		byKey:      map[string]string{},
		openErr:    map[string]error{},
		getErr:     map[string]error{},
		resolveErr: map[string]error{},
		listErr:    map[string]error{},
	}
}

func (f *fakeProducts) addOpenAlert(id, productID string) {
	f.alerts = append(f.alerts, &model.StockAlert{ID: id, ProductID: productID, Status: model.AlertOpen})
}

func (f *fakeProducts) record(ctx context.Context, name string) error {
	f.mu.Lock()
	f.calls = append(f.calls, name)
	hook := f.beforeCall
	f.mu.Unlock()
	if hook != nil {
		return hook(ctx, name)
	}
	return nil
}

func (f *fakeProducts) callsNamed(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if len(c) >= len(prefix) && c[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

func pageOf(total, page, limit int) (from, to, totalPages int) {
	totalPages = (total + limit - 1) / limit
	from = (page - 1) * limit
	if from > total {
		from = total
	}
	to = from + limit
	if to > total {
		to = total
	}
	return
}

func (f *fakeProducts) ListProductsAtMost(ctx context.Context, maxStock, page, limit int) (port.ProductPage, error) {
	if err := f.record(ctx, fmt.Sprintf("list-products p=%d", page)); err != nil {
		return port.ProductPage{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.listErr[fmt.Sprintf("products:%d", page)]; ok {
		return port.ProductPage{}, err
	}
	var matching []model.Product
	for _, p := range f.products {
		if f.ignoreFilter || p.Stock <= maxStock {
			matching = append(matching, p)
		}
	}
	from, to, pages := pageOf(len(matching), page, limit)
	return port.ProductPage{
		Items: append([]model.Product(nil), matching[from:to]...),
		Info:  port.PageInfo{Page: page, Limit: limit, Total: len(matching), TotalPages: pages},
	}, nil
}

func (f *fakeProducts) GetProduct(ctx context.Context, id string) (model.Product, error) {
	if err := f.record(ctx, "get-product "+id); err != nil {
		return model.Product{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.getErr[id]; ok {
		return model.Product{}, err
	}
	for _, p := range f.products {
		if p.ID == id {
			return p, nil
		}
	}
	return model.Product{}, fmt.Errorf("get product: %w", port.ErrNotFound)
}

func (f *fakeProducts) ListAlerts(ctx context.Context, status model.AlertStatus, page, limit int) (port.AlertPage, error) {
	if err := f.record(ctx, fmt.Sprintf("list-alerts p=%d", page)); err != nil {
		return port.AlertPage{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.listErr[fmt.Sprintf("alerts:%d", page)]; ok {
		return port.AlertPage{}, err
	}
	var matching []model.StockAlert
	for _, a := range f.alerts {
		if a.Status == status {
			matching = append(matching, *a)
		}
	}
	from, to, pages := pageOf(len(matching), page, limit)
	return port.AlertPage{
		Items: append([]model.StockAlert(nil), matching[from:to]...),
		Info:  port.PageInfo{Page: page, Limit: limit, Total: len(matching), TotalPages: pages},
	}, nil
}

func (f *fakeProducts) OpenAlert(ctx context.Context, in model.OpeningIntent) (port.OpenAlertResult, error) {
	if err := f.record(ctx, "open "+in.ProductID); err != nil {
		return port.OpenAlertResult{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys = append(f.keys, in.Key)
	if err, ok := f.openErr[in.ProductID]; ok {
		return port.OpenAlertResult{}, err
	}
	if id, ok := f.byKey[in.Key]; ok { // replay: the alert as it is now
		for _, a := range f.alerts {
			if a.ID == id {
				return port.OpenAlertResult{Alert: *a}, nil
			}
		}
	}
	for _, a := range f.alerts { // one OPEN alert per product
		if a.ProductID == in.ProductID && a.Status == model.AlertOpen {
			f.byKey[in.Key] = a.ID
			return port.OpenAlertResult{Alert: *a}, nil
		}
	}
	f.nextID++
	a := &model.StockAlert{
		ID: fmt.Sprintf("alert-%d", f.nextID), ProductID: in.ProductID,
		Status: model.AlertOpen, StockAtOpening: in.StockAtOpening,
	}
	f.alerts = append(f.alerts, a)
	f.byKey[in.Key] = a.ID
	res := *a
	if f.badOpenAlert != nil {
		res = f.badOpenAlert(in, res)
	}
	return port.OpenAlertResult{Alert: res, Created: true}, nil
}

func (f *fakeProducts) ResolveAlert(ctx context.Context, id string) (model.StockAlert, error) {
	if err := f.record(ctx, "resolve "+id); err != nil {
		return model.StockAlert{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.resolveErr[id]; ok {
		return model.StockAlert{}, err
	}
	for _, a := range f.alerts {
		if a.ID == id {
			a.Status = model.AlertResolved
			res := *a
			if f.badResolveStatus {
				res.Status = model.AlertOpen
			}
			return res, nil
		}
	}
	return model.StockAlert{}, fmt.Errorf("resolve alert: %w", port.ErrNotFound)
}

func (f *fakeProducts) openAlertCount(productID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, a := range f.alerts {
		if a.ProductID == productID && a.Status == model.AlertOpen {
			n++
		}
	}
	return n
}

type fixedClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fixedClock) set(t time.Time) {
	c.mu.Lock()
	c.t = t
	c.mu.Unlock()
}

// logBuffer captures structured logs so tests can assert on what is (and is
// not) written.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *logBuffer) logger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug}))
}
