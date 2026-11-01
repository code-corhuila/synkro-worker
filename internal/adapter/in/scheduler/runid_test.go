package scheduler

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/code-corhuila/synkro-worker/internal/application/port"
	"github.com/code-corhuila/synkro-worker/internal/domain/model"
)

// pinGenerator replaces the identity generator for one test.
func pinGenerator(t *testing.T, gen func() (string, error)) {
	t.Helper()
	prev := newCorrelationID
	newCorrelationID = gen
	t.Cleanup(func() { newCorrelationID = prev })
}

// WRK-KEY-08 (scheduler side): a generator failure aborts the run before the
// job can mutate anything, and the next tick recovers.
func TestRunOnce_AGeneratorFailureNeverCallsTheJobAndTheNextTickRecovers(t *testing.T) {
	good := "6f1c2d3e-4a5b-4c6d-8e7f-0123456789ab"
	var attempts, jobCalls atomic.Int32
	pinGenerator(t, func() (string, error) {
		if attempts.Add(1) == 1 {
			return "", errors.New("entropy source unavailable")
		}
		return good, nil
	})
	var seen []string
	job := func(ctx context.Context) error {
		jobCalls.Add(1)
		seen = append(seen, port.CorrelationIDFrom(ctx))
		return nil
	}
	logger, logs := bufferLogger()

	runOnce(context.Background(), logger, time.Second, job)
	if jobCalls.Load() != 0 {
		t.Fatalf("the job must not run without a valid identity (calls=%d)", jobCalls.Load())
	}
	if !strings.Contains(logs.String(), `"level":"ERROR"`) || !strings.Contains(logs.String(), "identity") {
		t.Errorf("the failure must be logged as an error: %s", logs.String())
	}

	runOnce(context.Background(), logger, time.Second, job)
	if jobCalls.Load() != 1 || seen[0] != good {
		t.Errorf("the next tick must run with a fresh valid identity: calls=%d seen=%v", jobCalls.Load(), seen)
	}
}

func TestRunOnce_AnInvalidGeneratedIdentityNeverReachesTheJob(t *testing.T) {
	for name, id := range map[string]string{
		"empty":              "",
		"timestamp fallback": "run-1767225600000000000",
		"nil uuid":           "00000000-0000-0000-0000-000000000000",
	} {
		t.Run(name, func(t *testing.T) {
			pinGenerator(t, func() (string, error) { return id, nil })
			var jobCalls atomic.Int32
			logger, logs := bufferLogger()

			runOnce(context.Background(), logger, time.Second, func(context.Context) error {
				jobCalls.Add(1)
				return nil
			})

			if jobCalls.Load() != 0 {
				t.Fatal("the job must not run with an invalid identity")
			}
			if !strings.Contains(logs.String(), `"level":"ERROR"`) {
				t.Errorf("an invalid identity must be logged as an error: %s", logs.String())
			}
		})
	}
}

func TestRunOnce_GeneratesExactlyOneIdentityPerRunThatStarts(t *testing.T) {
	var generated atomic.Int32
	pinGenerator(t, func() (string, error) {
		generated.Add(1)
		return "6f1c2d3e-4a5b-4c6d-8e7f-0123456789ab", nil
	})
	var ids []string
	job := func(ctx context.Context) error {
		ids = append(ids, port.CorrelationIDFrom(ctx))
		return nil
	}
	logger, _ := bufferLogger()

	runOnce(context.Background(), logger, time.Second, job)

	if generated.Load() != 1 || len(ids) != 1 {
		t.Errorf("want one identity for one run, generated=%d jobs=%d", generated.Load(), len(ids))
	}
}

func TestRandomUUID_ProducesDistinctValidUUIDv4s(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		id, err := randomUUID()
		if err != nil {
			t.Fatal(err)
		}
		if err := model.ValidateRunID(id); err != nil {
			t.Fatalf("generated identity %q is not a valid run id: %v", id, err)
		}
		if id[14] != '4' || !strings.ContainsRune("89ab", rune(id[19])) {
			t.Fatalf("not a version 4 / variant 10 UUID: %s", id)
		}
		if seen[id] {
			t.Fatalf("duplicate identity %s", id)
		}
		seen[id] = true
	}
}

func TestUUIDFrom_ARandomSourceFailureIsAnErrorNotAFallback(t *testing.T) {
	id, err := uuidFrom(func([]byte) (int, error) { return 0, errors.New("no entropy") })

	if err == nil || id != "" {
		t.Fatalf("want an error and no identity, got %q / %v", id, err)
	}
	if _, err := uuidFrom(func(b []byte) (int, error) { return len(b) - 1, nil }); err == nil {
		t.Error("a short read must be an error too")
	}
}

func TestLowStockJob_APendingReconciliationIsNeitherHiddenNorCountedAsProcessed(t *testing.T) {
	logger, logs := bufferLogger()
	uc := &fakeSync{rep: port.SyncReport{Opened: 1, ReconciliationPending: 2}}

	err := LowStockJob(logger, uc)(port.WithCorrelationID(context.Background(), "corr-1"))

	if err == nil || !strings.Contains(err.Error(), "2") || !strings.Contains(err.Error(), "reconciliation") {
		t.Fatalf("the run must not read as a clean success, got %v", err)
	}
	out := logs.String()
	if !strings.Contains(out, `"reconciliationPending":2`) || !strings.Contains(out, `"processed":1`) || !strings.Contains(out, `"failed":0`) {
		t.Errorf("the log must show the pending count apart from processed and failed: %s", out)
	}
	if uc.rep.Processed() != 1 {
		t.Errorf("Processed must not include pending reconciliations: %d", uc.rep.Processed())
	}
}
