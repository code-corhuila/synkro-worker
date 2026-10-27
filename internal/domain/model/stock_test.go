package model

import (
	"errors"
	"testing"
	"time"
)

func TestIsLowStock(t *testing.T) {
	for _, tc := range []struct {
		stock, threshold int
		want             bool
	}{
		{0, 5, true}, {4, 5, true}, {5, 5, true}, {6, 5, false}, {0, 0, true}, {1, 0, false},
	} {
		if got := IsLowStock(tc.stock, tc.threshold); got != tc.want {
			t.Errorf("IsLowStock(%d, %d) = %v, want %v", tc.stock, tc.threshold, got, tc.want)
		}
	}
}

func TestNewOpeningIntent_BuildsTheDocumentedDailyKeyFromTheUTCDate(t *testing.T) {
	at := time.Date(2026, 3, 4, 23, 30, 0, 0, time.FixedZone("UTC-5", -5*3600)) // 2026-03-05 04:30 UTC

	in, err := NewOpeningIntent("0190a1b2-0000-7000-8000-000000000001", 3, at)

	if err != nil {
		t.Fatal(err)
	}
	if want := "low-stock:0190a1b2-0000-7000-8000-000000000001:2026-03-05"; in.Key != want {
		t.Errorf("key: want %s, got %s", want, in.Key)
	}
	if in.StockAtOpening != 3 || in.ProductID != "0190a1b2-0000-7000-8000-000000000001" {
		t.Errorf("unexpected intent: %+v", in)
	}
}

func TestNewOpeningIntent_RejectsInvalidInput(t *testing.T) {
	if _, err := NewOpeningIntent(" ", 1, time.Now()); !errors.Is(err, ErrProductIDRequired) {
		t.Errorf("blank product id: want ErrProductIDRequired, got %v", err)
	}
	if _, err := NewOpeningIntent("p", -1, time.Now()); !errors.Is(err, ErrNegativeStock) {
		t.Errorf("negative stock: want ErrNegativeStock, got %v", err)
	}
}
