package usecase

import "testing"

// cfg builds a Config; batch is the TOTAL number of reconciliation items of a
// run (BATCH_SIZE).
func cfg(threshold, batch, maxPages int) Config {
	return Config{Threshold: threshold, BatchSize: batch, MaxPages: maxPages}
}

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
