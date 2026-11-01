package model

import (
	"errors"
	"strings"
	"testing"
)

const (
	testProduct = "0190a1b2-0000-7000-8000-000000000001"
	testRunID   = "6f1c2d3e-4a5b-4c6d-8e7f-0123456789ab"
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

func TestNewOpeningIntent_BuildsTheRunScopedKey(t *testing.T) {
	in, err := NewOpeningIntent(testProduct, 3, testRunID)

	if err != nil {
		t.Fatal(err)
	}
	if want := "low-stock:" + testProduct + ":" + testRunID; in.Key != want {
		t.Errorf("key: want %s, got %s", want, in.Key)
	}
	if in.StockAtOpening != 3 || in.ProductID != testProduct {
		t.Errorf("unexpected intent: %+v", in)
	}
}

func TestNewOpeningIntent_CanonicalUUIDsFitTheProductsKeyLimit(t *testing.T) {
	in, err := NewOpeningIntent(testProduct, 0, testRunID)

	if err != nil {
		t.Fatal(err)
	}
	if len(in.Key) != 83 {
		t.Errorf("ADR-018 states 83 characters for canonical UUIDs, got %d", len(in.Key))
	}
}

func TestNewOpeningIntent_TwoRunsGiveTwoKeysAndOneRunIsStable(t *testing.T) {
	other := "11111111-2222-4333-8444-555555555555"

	a, _ := NewOpeningIntent(testProduct, 3, testRunID)
	again, _ := NewOpeningIntent(testProduct, 3, testRunID)
	b, _ := NewOpeningIntent(testProduct, 3, other)

	if a != again {
		t.Errorf("the same product and run must build the same intent: %+v vs %+v", a, again)
	}
	if a.Key == b.Key {
		t.Error("another run must build another key")
	}
}

func TestNewOpeningIntent_RejectsInvalidInput(t *testing.T) {
	if _, err := NewOpeningIntent(" ", 1, testRunID); !errors.Is(err, ErrProductIDRequired) {
		t.Errorf("blank product id: want ErrProductIDRequired, got %v", err)
	}
	if _, err := NewOpeningIntent("p", -1, testRunID); !errors.Is(err, ErrNegativeStock) {
		t.Errorf("negative stock: want ErrNegativeStock, got %v", err)
	}
}

func TestNewOpeningIntent_RejectsAMissingOrInvalidRunIdentity(t *testing.T) {
	for name, id := range map[string]string{
		"empty":                "",
		"blank":                "   ",
		"timestamp fallback":   "run-1767225600000000000",
		"not a uuid":           "not-a-uuid",
		"nil uuid":             "00000000-0000-0000-0000-000000000000",
		"missing dashes":       strings.ReplaceAll(testRunID, "-", ""),
		"surrounding spaces":   " " + testRunID + " ",
		"too long":             testRunID + "0",
		"non hex digit":        "6f1c2d3e-4a5b-4c6d-8e7f-0123456789ag",
		"braces":               "{" + testRunID + "}",
		"urn prefix":           "urn:uuid:" + testRunID,
		"valid with newline":   testRunID + "\n",
		"key separator inside": "6f1c2d3e:4a5b:4c6d:8e7f:0123456789ab",
	} {
		t.Run(name, func(t *testing.T) {
			in, err := NewOpeningIntent(testProduct, 1, id)

			if !errors.Is(err, ErrInvalidRunID) {
				t.Fatalf("want ErrInvalidRunID, got %v", err)
			}
			if in != (OpeningIntent{}) {
				t.Errorf("no intent may be produced: %+v", in)
			}
			if strings.TrimSpace(id) != "" && strings.Contains(err.Error(), strings.TrimSpace(id)) {
				t.Errorf("the error must not echo the offending value: %v", err)
			}
		})
	}
}

func TestValidateRunID(t *testing.T) {
	if err := ValidateRunID(testRunID); err != nil {
		t.Errorf("canonical UUID must be valid: %v", err)
	}
	if err := ValidateRunID(strings.ToUpper(testRunID)); err != nil {
		t.Errorf("uppercase hex is still a UUID: %v", err)
	}
	if err := ValidateRunID(""); !errors.Is(err, ErrInvalidRunID) {
		t.Errorf("empty: want ErrInvalidRunID, got %v", err)
	}
}
