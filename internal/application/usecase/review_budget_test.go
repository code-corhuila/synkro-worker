package usecase

import (
	"context"
	"testing"

	"github.com/code-corhuila/synkro-worker/internal/application/port"
	"github.com/code-corhuila/synkro-worker/internal/domain/model"
)

// A short first response cannot justify asking for another full page when
// the remaining admission budget is smaller than that page's stable limit.
type shortThenFullGateway struct {
	*fakeProducts
	productsReceived, alertsReceived int
}

func (g *shortThenFullGateway) ListProductsAtMost(ctx context.Context, stock, page, limit int) (port.ProductPage, error) {
	g.pageCap = 0
	if len(g.productLimits) == 0 {
		g.pageCap = 1
	}
	r, err := g.fakeProducts.ListProductsAtMost(ctx, stock, page, limit)
	g.productsReceived += len(r.Items)
	return r, err
}

func (g *shortThenFullGateway) ListAlerts(ctx context.Context, status model.AlertStatus, page, limit int) (port.AlertPage, error) {
	g.pageCap = 0
	if len(g.alertLimits) == 0 {
		g.pageCap = 1
	}
	r, err := g.fakeProducts.ListAlerts(ctx, status, page, limit)
	g.alertsReceived += len(r.Items)
	return r, err
}

func TestRun_ShortThenFullPagesNeverExceedTheTotalAdmissionBudget(t *testing.T) {
	for _, batch := range []int{6, 13, 14, 100} {
		t.Run(runID(int64(batch)), func(t *testing.T) {
			base := newFake(lowProducts(300)...)
			staleAlerts(base, 300)
			gw := &shortThenFullGateway{fakeProducts: base}
			uc, _ := newUC(t, gw, cfg(5, batch, 5))
			mustRun(t, uc)
			open, resolve := phaseQuotas(batch, 0)
			for name, values := range map[string]struct {
				received, quota int
				limits          []int
			}{
				"products": {gw.productsReceived, open, gw.productLimits},
				"alerts":   {gw.alertsReceived, resolve, gw.alertLimits},
			} {
				requested := 0
				for _, limit := range values.limits {
					requested += limit
				}
				if values.received > values.quota || requested > values.quota {
					t.Errorf("%s: received=%d requested=%d exceeds quota=%d", name, values.received, requested, values.quota)
				}
			}
			if got := gw.productsReceived + gw.alertsReceived; got > batch {
				t.Errorf("received %d elements across both phases, batch=%d", got, batch)
			}
		})
	}
}
