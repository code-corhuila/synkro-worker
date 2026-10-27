package products

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/code-corhuila/synkro-worker/internal/application/port"
	"github.com/code-corhuila/synkro-worker/internal/domain/model"
)

const (
	testToken = "svc-token-SECRET-do-not-leak"
	pid       = "0190a1b2-0000-7000-8000-000000000001"
	aid       = "0190a1b2-0000-7000-8000-0000000000a1"
)

// harness wires a Client to an httptest server and records the backoff the
// client asks for, without waiting for it.
type harness struct {
	t      *testing.T
	srv    *httptest.Server
	client *Client

	mu     sync.Mutex
	sleeps []time.Duration
	caps   []time.Duration
}

type tweak func(*Options)

func newHarness(t *testing.T, h http.HandlerFunc, tweaks ...tweak) *harness {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	hn := &harness{t: t, srv: srv}
	o := Options{
		BaseURL:  srv.URL,
		Token:    testToken,
		Timeout:  2 * time.Second,
		Attempts: 3,
		Sleep: func(ctx context.Context, d time.Duration) error {
			hn.mu.Lock()
			hn.sleeps = append(hn.sleeps, d)
			hn.mu.Unlock()
			return ctx.Err()
		},
		Rand: func(n int64) int64 {
			hn.mu.Lock()
			hn.caps = append(hn.caps, time.Duration(n))
			hn.mu.Unlock()
			return n / 2
		},
		BackoffBase: 100 * time.Millisecond,
		BackoffMax:  350 * time.Millisecond,
	}
	for _, tw := range tweaks {
		tw(&o)
	}
	c, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	hn.client = c
	return hn
}

func ctxWithCorr() context.Context {
	return port.WithCorrelationID(context.Background(), "corr-123")
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

func productJSON(id string, stock int) string {
	return fmt.Sprintf(`{"productId":%q,"name":"n","priceCents":100,"stock":%d,"categoryId":"c","active":true}`, id, stock)
}

func metaJSON(page, limit, total, pages int) string {
	return fmt.Sprintf(`{"page":%d,"limit":%d,"total":%d,"totalPages":%d}`, page, limit, total, pages)
}

func alertJSON(id, product, status string) string {
	return fmt.Sprintf(`{"alertId":%q,"productId":%q,"status":%q,"stockAtOpening":2,"openedAt":"2026-03-04T10:00:00Z"}`, id, product, status)
}

// ─── requests ────────────────────────────────────────────────────────────

func TestListProductsAtMost_SendsTheDocumentedRequestAndParsesTheEnvelope(t *testing.T) {
	var got *http.Request
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		got = r.Clone(context.Background())
		writeJSON(w, 200, `{"data":[`+productJSON(pid, 0)+`],"meta":`+metaJSON(2, 50, 51, 2)+`}`)
	})

	page, err := h.client.ListProductsAtMost(ctxWithCorr(), 5, 2, 50)

	if err != nil {
		t.Fatal(err)
	}
	if got.Method != "GET" || got.URL.Path != "/api/v1/products" {
		t.Errorf("want GET /api/v1/products, got %s %s", got.Method, got.URL.Path)
	}
	q := got.URL.Query()
	if q.Get("stockAtMost") != "5" || q.Get("page") != "2" || q.Get("limit") != "50" {
		t.Errorf("unexpected query: %v", q)
	}
	if got.Header.Get("Authorization") != "Bearer "+testToken {
		t.Error("the SERVICE token must be sent as a bearer credential")
	}
	if got.Header.Get("X-Correlation-Id") != "corr-123" {
		t.Errorf("correlation id must be propagated, got %q", got.Header.Get("X-Correlation-Id"))
	}
	if len(page.Items) != 1 || page.Items[0].ID != pid || page.Items[0].Stock != 0 {
		t.Errorf("unexpected items: %+v", page.Items)
	}
	if page.Info != (port.PageInfo{Page: 2, Limit: 50, Total: 51, TotalPages: 2}) {
		t.Errorf("unexpected page info: %+v", page.Info)
	}
}

func TestListProductsAtMost_AnEmptyPageIsValid(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, `{"data":[],"meta":`+metaJSON(1, 100, 0, 0)+`}`)
	})

	page, err := h.client.ListProductsAtMost(context.Background(), 5, 1, 100)

	if err != nil || len(page.Items) != 0 || page.Info.TotalPages != 0 {
		t.Fatalf("want an empty valid page, got %+v / %v", page, err)
	}
}

func TestListProductsAtMost_RejectsMalformedOrContradictoryResponses(t *testing.T) {
	tests := map[string]string{
		"not json":                `<html>oops</html>`,
		"empty body":              ``,
		"trailing data":           `{"data":[],"meta":` + metaJSON(1, 10, 0, 0) + `} {}`,
		"data missing":            `{"meta":` + metaJSON(1, 10, 0, 0) + `}`,
		"meta missing":            `{"data":[]}`,
		"meta field missing":      `{"data":[],"meta":{"page":1,"limit":10,"total":0}}`,
		"stock missing":           `{"data":[{"productId":"` + pid + `"}],"meta":` + metaJSON(1, 10, 1, 1) + `}`,
		"stock null":              `{"data":[{"productId":"` + pid + `","stock":null}],"meta":` + metaJSON(1, 10, 1, 1) + `}`,
		"stock negative":          `{"data":[` + productJSON(pid, -1) + `],"meta":` + metaJSON(1, 10, 1, 1) + `}`,
		"stock not an integer":    `{"data":[{"productId":"` + pid + `","stock":2.5}],"meta":` + metaJSON(1, 10, 1, 1) + `}`,
		"product id missing":      `{"data":[{"stock":1}],"meta":` + metaJSON(1, 10, 1, 1) + `}`,
		"product id not a uuid":   `{"data":[` + productJSON("../etc", 1) + `],"meta":` + metaJSON(1, 10, 1, 1) + `}`,
		"above the stock filter":  `{"data":[` + productJSON(pid, 9) + `],"meta":` + metaJSON(1, 10, 1, 1) + `}`,
		"more items than limit":   `{"data":[` + productJSON(pid, 1) + `,` + productJSON(aid, 1) + `],"meta":` + metaJSON(1, 1, 2, 2) + `}`,
		"other page than asked":   `{"data":[],"meta":` + metaJSON(2, 10, 0, 0) + `}`,
		"other limit than asked":  `{"data":[],"meta":` + metaJSON(1, 20, 0, 0) + `}`,
		"negative total":          `{"data":[],"meta":` + metaJSON(1, 10, -1, 0) + `}`,
		"data is not an array":    `{"data":{},"meta":` + metaJSON(1, 10, 0, 0) + `}`,
		"error envelope as 200":   `{"error":"INTERNAL_ERROR","message":"x","traceId":"t"}`,
		"json null":               `null`,
		"array instead of object": `[]`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, body) })

			_, err := h.client.ListProductsAtMost(context.Background(), 5, 1, 10)

			if !errors.Is(err, port.ErrMalformedResponse) {
				t.Fatalf("want ErrMalformedResponse, got %v", err)
			}
		})
	}
}

func TestGetProduct_ReadsThePublishedResource(t *testing.T) {
	var path string
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		writeJSON(w, 200, productJSON(pid, 12))
	})

	p, err := h.client.GetProduct(context.Background(), pid)

	if err != nil || p.ID != pid || p.Stock != 12 {
		t.Fatalf("unexpected result: %+v / %v", p, err)
	}
	if path != "/api/v1/products/"+pid {
		t.Errorf("unexpected path %s", path)
	}
}

func TestGetProduct_ANotFoundIsNotFoundAndIsNotRetried(t *testing.T) {
	var n atomic.Int32
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		writeJSON(w, 404, `{"error":"NOT_FOUND","message":"Product not found","traceId":"t-9"}`)
	})

	_, err := h.client.GetProduct(context.Background(), pid)

	if !errors.Is(err, port.ErrNotFound) || n.Load() != 1 {
		t.Fatalf("want one request and ErrNotFound, got %d / %v", n.Load(), err)
	}
}

func TestGetProduct_AnAnswerForAnotherProductIsMalformed(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, productJSON(aid, 1)) })

	_, err := h.client.GetProduct(context.Background(), pid)

	if !errors.Is(err, port.ErrMalformedResponse) {
		t.Fatalf("want ErrMalformedResponse, got %v", err)
	}
}

func TestIDsThatAreNotUUIDsNeverReachThePath(t *testing.T) {
	var n atomic.Int32
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) { n.Add(1); writeJSON(w, 200, "{}") })

	_, err1 := h.client.GetProduct(context.Background(), "../stock-alerts")
	_, err2 := h.client.ResolveAlert(context.Background(), "x/../../y")

	if !errors.Is(err1, port.ErrRejected) || !errors.Is(err2, port.ErrRejected) || n.Load() != 0 {
		t.Fatalf("want ErrRejected and no request, got %v / %v / %d requests", err1, err2, n.Load())
	}
}

func TestListAlerts_AsksForTheStatusAndParsesTheAlerts(t *testing.T) {
	var q map[string][]string
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		q = r.URL.Query()
		writeJSON(w, 200, `{"data":[`+alertJSON(aid, pid, "OPEN")+`],"meta":`+metaJSON(1, 100, 1, 1)+`}`)
	})

	page, err := h.client.ListAlerts(context.Background(), model.AlertOpen, 1, 100)

	if err != nil {
		t.Fatal(err)
	}
	if q["status"][0] != "OPEN" || q["page"][0] != "1" || q["limit"][0] != "100" {
		t.Errorf("unexpected query %v", q)
	}
	want := model.StockAlert{ID: aid, ProductID: pid, Status: model.AlertOpen, StockAtOpening: 2}
	if len(page.Items) != 1 || page.Items[0] != want {
		t.Errorf("unexpected alerts %+v", page.Items)
	}
}

func TestListAlerts_RejectsAlertsThatContradictTheRequestOrTheContract(t *testing.T) {
	tests := map[string]string{
		"unknown status":             `{"data":[` + alertJSON(aid, pid, "CLOSED") + `],"meta":` + metaJSON(1, 10, 1, 1) + `}`,
		"resolved in a list of OPEN": `{"data":[` + alertJSON(aid, pid, "RESOLVED") + `],"meta":` + metaJSON(1, 10, 1, 1) + `}`,
		"alert id missing":           `{"data":[{"productId":"` + pid + `","status":"OPEN","stockAtOpening":1}],"meta":` + metaJSON(1, 10, 1, 1) + `}`,
		"product id missing":         `{"data":[{"alertId":"` + aid + `","status":"OPEN","stockAtOpening":1}],"meta":` + metaJSON(1, 10, 1, 1) + `}`,
		"status missing":             `{"data":[{"alertId":"` + aid + `","productId":"` + pid + `","stockAtOpening":1}],"meta":` + metaJSON(1, 10, 1, 1) + `}`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, body) })

			_, err := h.client.ListAlerts(context.Background(), model.AlertOpen, 1, 10)

			if !errors.Is(err, port.ErrMalformedResponse) {
				t.Fatalf("want ErrMalformedResponse, got %v", err)
			}
		})
	}
}

func TestOpenAlert_SendsTheContractPayloadAndTheIdempotencyKey(t *testing.T) {
	var (
		got  *http.Request
		body map[string]any
	)
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		got = r.Clone(context.Background())
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Location", "/api/v1/products/"+pid)
		writeJSON(w, 201, alertJSON(aid, pid, "OPEN"))
	})
	in := model.OpeningIntent{ProductID: pid, StockAtOpening: 2, Key: "low-stock:" + pid + ":2026-03-04"}

	res, err := h.client.OpenAlert(ctxWithCorr(), in)

	if err != nil {
		t.Fatal(err)
	}
	if got.Method != "POST" || got.URL.Path != "/api/v1/stock-alerts" {
		t.Errorf("want POST /api/v1/stock-alerts, got %s %s", got.Method, got.URL.Path)
	}
	if got.Header.Get("Idempotency-Key") != in.Key {
		t.Errorf("Idempotency-Key: want %s, got %s", in.Key, got.Header.Get("Idempotency-Key"))
	}
	if got.Header.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type: %q", got.Header.Get("Content-Type"))
	}
	if got.Header.Get("X-Correlation-Id") != "corr-123" || got.Header.Get("Authorization") != "Bearer "+testToken {
		t.Error("correlation id and SERVICE token must be sent")
	}
	if len(body) != 2 || body["productId"] != pid || body["stockAtOpening"] != float64(2) {
		t.Errorf("payload must be exactly productId and stockAtOpening, got %v", body)
	}
	if !res.Created || res.Alert.ID != aid || res.Alert.Status != model.AlertOpen {
		t.Errorf("201 must read as Created: %+v", res)
	}
}

func TestOpenAlert_A200IsAReplayNotACreation(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, alertJSON(aid, pid, "OPEN")) })

	res, err := h.client.OpenAlert(context.Background(), model.OpeningIntent{ProductID: pid, Key: "low-stock:" + pid + ":2026-03-04"})

	if err != nil || res.Created || res.Alert.ID != aid {
		t.Fatalf("200 must read as not created: %+v / %v", res, err)
	}
}

func TestOpenAlert_ReportsAReplayedResolvedAlertAsItIs(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, alertJSON(aid, pid, "RESOLVED")) })

	res, err := h.client.OpenAlert(context.Background(), model.OpeningIntent{ProductID: pid, Key: "low-stock:" + pid + ":2026-03-04"})

	if err != nil || res.Alert.Status != model.AlertResolved {
		t.Fatalf("the adapter reports what Products said and the use case judges it: %+v / %v", res, err)
	}
}

func TestOpenAlert_RejectsMalformedOrContradictoryAnswers(t *testing.T) {
	tests := map[string]struct {
		status int
		body   string
	}{
		"other product":    {201, alertJSON(aid, aid, "OPEN")},
		"not json":         {201, `created`},
		"unexpected 202":   {202, alertJSON(aid, pid, "OPEN")},
		"unexpected 204":   {204, ``},
		"status unknown":   {201, alertJSON(aid, pid, "WEIRD")},
		"alert id missing": {200, `{"productId":"` + pid + `","status":"OPEN","stockAtOpening":1}`},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(w, tc.status, tc.body) })

			_, err := h.client.OpenAlert(context.Background(), model.OpeningIntent{ProductID: pid, Key: "low-stock:" + pid + ":2026-03-04"})

			if !errors.Is(err, port.ErrMalformedResponse) {
				t.Fatalf("want ErrMalformedResponse, got %v", err)
			}
		})
	}
}

func TestOpenAlert_AnInvalidIdempotencyKeyIsNotSent(t *testing.T) {
	var n atomic.Int32
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) { n.Add(1) })

	for _, key := range []string{"", "short", strings.Repeat("k", 129)} {
		_, err := h.client.OpenAlert(context.Background(), model.OpeningIntent{ProductID: pid, Key: key})
		if !errors.Is(err, port.ErrRejected) {
			t.Errorf("key of length %d: want ErrRejected, got %v", len(key), err)
		}
	}
	if n.Load() != 0 {
		t.Errorf("no request may be sent with an invalid key, got %d", n.Load())
	}
}

func TestResolveAlert_PostsToTheResolveEndpoint(t *testing.T) {
	var got *http.Request
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		got = r.Clone(context.Background())
		writeJSON(w, 200, alertJSON(aid, pid, "RESOLVED"))
	})

	a, err := h.client.ResolveAlert(ctxWithCorr(), aid)

	if err != nil {
		t.Fatal(err)
	}
	if got.Method != "POST" || got.URL.Path != "/api/v1/stock-alerts/"+aid+"/resolve" {
		t.Errorf("unexpected request %s %s", got.Method, got.URL.Path)
	}
	if got.Header.Get("X-Correlation-Id") != "corr-123" || got.Header.Get("Authorization") != "Bearer "+testToken {
		t.Error("correlation id and SERVICE token must be sent")
	}
	if a.ID != aid || a.Status != model.AlertResolved {
		t.Errorf("unexpected alert %+v", a)
	}
}

func TestResolveAlert_AnAnswerForAnotherAlertIsMalformed(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, alertJSON(pid, pid, "RESOLVED")) })

	_, err := h.client.ResolveAlert(context.Background(), aid)

	if !errors.Is(err, port.ErrMalformedResponse) {
		t.Fatalf("want ErrMalformedResponse, got %v", err)
	}
}

// ─── retry policy ────────────────────────────────────────────────────────

func TestOpenAlert_RetriesTransientStatusesWithTheSameKeyPayloadAndCorrelation(t *testing.T) {
	for _, status := range []int{429, 500, 502, 503, 504} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var (
				mu     sync.Mutex
				keys   []string
				bodies []string
				corrs  []string
			)
			var n atomic.Int32
			h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				mu.Lock()
				keys = append(keys, r.Header.Get("Idempotency-Key"))
				bodies = append(bodies, string(b))
				corrs = append(corrs, r.Header.Get("X-Correlation-Id"))
				mu.Unlock()
				if n.Add(1) < 3 {
					writeJSON(w, status, `{"error":"X","message":"m","traceId":"t"}`)
					return
				}
				writeJSON(w, 201, alertJSON(aid, pid, "OPEN"))
			})
			in := model.OpeningIntent{ProductID: pid, StockAtOpening: 1, Key: "low-stock:" + pid + ":2026-03-04"}

			res, err := h.client.OpenAlert(ctxWithCorr(), in)

			if err != nil || !res.Created {
				t.Fatalf("want success on the third attempt, got %+v / %v", res, err)
			}
			if len(keys) != 3 || keys[0] != in.Key || keys[1] != in.Key || keys[2] != in.Key {
				t.Errorf("every attempt must send the same Idempotency-Key: %v", keys)
			}
			if bodies[0] != bodies[1] || bodies[1] != bodies[2] {
				t.Errorf("every attempt must send the same payload: %v", bodies)
			}
			if corrs[0] != "corr-123" || corrs[1] != "corr-123" || corrs[2] != "corr-123" {
				t.Errorf("every attempt must carry the correlation id: %v", corrs)
			}
			if len(h.sleeps) != 2 {
				t.Errorf("want a backoff before each retry (2), got %d", len(h.sleeps))
			}
		})
	}
}

func TestRetries_NetworkErrorsAreTransientToo(t *testing.T) {
	var n atomic.Int32
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 { // the response is lost: connection dropped after the request arrived
			conn, _, _ := w.(http.Hijacker).Hijack()
			_ = conn.Close()
			return
		}
		writeJSON(w, 200, alertJSON(aid, pid, "OPEN"))
	})

	res, err := h.client.OpenAlert(context.Background(), model.OpeningIntent{ProductID: pid, Key: "low-stock:" + pid + ":2026-03-04"})

	if err != nil || res.Alert.ID != aid || n.Load() != 2 {
		t.Fatalf("want success after one retry, got %+v / %v after %d requests", res, err, n.Load())
	}
}

func TestRetries_AreFiniteAndEndInUnavailable(t *testing.T) {
	var n atomic.Int32
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		writeJSON(w, 503, `{"error":"SERVICE_UNAVAILABLE","message":"m","traceId":"t-503"}`)
	}, func(o *Options) { o.Attempts = 4 })

	_, err := h.client.ResolveAlert(context.Background(), aid)

	if !errors.Is(err, port.ErrUnavailable) {
		t.Fatalf("want ErrUnavailable, got %v", err)
	}
	if n.Load() != 4 {
		t.Errorf("want exactly 4 attempts, got %d", n.Load())
	}
	if len(h.sleeps) != 3 {
		t.Errorf("want 3 backoffs between 4 attempts, got %d", len(h.sleeps))
	}
	if !strings.Contains(err.Error(), "503") {
		t.Errorf("the error should name the last status: %v", err)
	}
}

func TestRetries_NetworkFailureThroughAllAttemptsIsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing listens any more
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {}, func(o *Options) { o.BaseURL = url })

	_, err := h.client.GetProduct(context.Background(), pid)

	if !errors.Is(err, port.ErrUnavailable) {
		t.Fatalf("want ErrUnavailable, got %v", err)
	}
	if len(h.sleeps) != 2 {
		t.Errorf("want 2 backoffs between 3 attempts, got %d", len(h.sleeps))
	}
}

func TestRetries_UseExponentialBackoffWithFullJitterAndACap(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 500, `{}`) },
		func(o *Options) { o.Attempts = 5 })

	_, _ = h.client.GetProduct(context.Background(), pid)

	wantCaps := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 350 * time.Millisecond, 350 * time.Millisecond}
	if fmt.Sprint(h.caps) != fmt.Sprint(wantCaps) {
		t.Fatalf("jitter ceilings: want %v, got %v", wantCaps, h.caps)
	}
	for i, s := range h.sleeps { // the fake jitter returns half of the ceiling
		if s != wantCaps[i]/2 {
			t.Errorf("sleep %d: want %v, got %v", i, wantCaps[i]/2, s)
		}
	}
}

func TestRetries_APerAttemptTimeoutCountsAsATransientFailure(t *testing.T) {
	var n atomic.Int32
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		<-r.Context().Done() // never answers; the client gives up
	}, func(o *Options) { o.Timeout = 40 * time.Millisecond; o.Attempts = 2 })

	_, err := h.client.GetProduct(context.Background(), pid)

	if !errors.Is(err, port.ErrUnavailable) || n.Load() != 2 {
		t.Fatalf("want ErrUnavailable after 2 timed-out attempts, got %v after %d", err, n.Load())
	}
}

func TestPermanentRejectionsAreNeverRetried(t *testing.T) {
	tests := []struct {
		status int
		want   error
	}{
		{400, port.ErrRejected},
		{401, port.ErrCredentialRejected},
		{403, port.ErrCredentialRejected},
		{404, port.ErrNotFound},
		{409, port.ErrRejected},
		{422, port.ErrRejected},
		{302, port.ErrRejected},
		{307, port.ErrRejected},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			var n, redirected atomic.Int32
			other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
			defer other.Close()
			h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
				n.Add(1)
				if tc.status/100 == 3 {
					w.Header().Set("Location", other.URL+"/steal")
					w.WriteHeader(tc.status)
					return
				}
				writeJSON(w, tc.status, `{"error":"X","message":"m","traceId":"t"}`)
			})

			_, err := h.client.OpenAlert(context.Background(), model.OpeningIntent{ProductID: pid, Key: "low-stock:" + pid + ":2026-03-04"})

			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
			if n.Load() != 1 || len(h.sleeps) != 0 {
				t.Errorf("a permanent rejection must not be retried: %d requests, %d backoffs", n.Load(), len(h.sleeps))
			}
			if redirected.Load() != 0 {
				t.Error("redirects must not be followed: the credential would travel to another host")
			}
		})
	}
}

func TestErrors_CarryTheProductsCodeAndTraceIDButNeverTheCredential(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 422, `{"error":"BUSINESS_RULE_VIOLATION","message":"An alert cannot be opened for an inactive product","traceId":"trace-77"}`)
	})

	_, err := h.client.OpenAlert(context.Background(), model.OpeningIntent{ProductID: pid, Key: "low-stock:" + pid + ":2026-03-04"})

	msg := err.Error()
	for _, want := range []string{"422", "BUSINESS_RULE_VIOLATION", "trace-77"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error should mention %q: %s", want, msg)
		}
	}
	if strings.Contains(msg, testToken) {
		t.Fatalf("the credential leaked into an error: %s", msg)
	}
}

func TestErrors_AnErrorBodyThatIsNotTheEnvelopeIsNotEchoed(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 400, `<html>token=`+testToken+` stack trace ...</html>`)
	})

	_, err := h.client.GetProduct(context.Background(), pid)

	if !errors.Is(err, port.ErrRejected) || strings.Contains(err.Error(), "stack trace") || strings.Contains(err.Error(), testToken) {
		t.Fatalf("raw bodies must not be echoed: %v", err)
	}
}

func TestClient_NeverPrintsItsCredential(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {})

	for _, s := range []string{fmt.Sprint(h.client), fmt.Sprintf("%v", h.client), fmt.Sprintf("%+v", h.client), fmt.Sprintf("%#v", h.client)} {
		if strings.Contains(s, testToken) {
			t.Fatalf("the credential was printed: %s", s)
		}
	}
}

func TestResponsesAreSizeBounded(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":[],"meta":`+metaJSON(1, 10, 0, 0)+`,"padding":"`)
		_, _ = io.WriteString(w, strings.Repeat("x", 4<<20))
		_, _ = io.WriteString(w, `"}`)
	})

	_, err := h.client.ListProductsAtMost(context.Background(), 5, 1, 10)

	if !errors.Is(err, port.ErrMalformedResponse) {
		t.Fatalf("an oversized body must be refused, got %v", err)
	}
}

// ─── cancellation ────────────────────────────────────────────────────────

func TestCancellationDuringARequestReturnsPromptlyWithoutRetrying(t *testing.T) {
	var n atomic.Int32
	started := make(chan struct{}, 1)
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		started <- struct{}{}
		<-r.Context().Done()
	}, func(o *Options) { o.Timeout = 10 * time.Second })
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { _, err := h.client.GetProduct(ctx, pid); errc <- err }()
	<-started

	cancel()

	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the call did not return after cancellation")
	}
	if n.Load() != 1 || len(h.sleeps) != 0 {
		t.Errorf("a cancelled call must not retry: %d requests, %d backoffs", n.Load(), len(h.sleeps))
	}
}

func TestCancellationDuringBackoffStopsBeforeTheNextAttempt(t *testing.T) {
	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		writeJSON(w, 503, `{}`)
	}, func(o *Options) {
		o.Sleep = func(c context.Context, d time.Duration) error {
			cancel() // shutdown arrives while waiting to retry
			return c.Err()
		}
	})

	_, err := h.client.GetProduct(ctx, pid)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if n.Load() != 1 {
		t.Errorf("no attempt may start after cancellation, got %d requests", n.Load())
	}
}

func TestACancelledContextSendsNothing(t *testing.T) {
	var n atomic.Int32
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) { n.Add(1) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := h.client.GetProduct(ctx, pid)

	if !errors.Is(err, context.Canceled) || n.Load() != 0 {
		t.Fatalf("want context.Canceled and no request, got %v / %d", err, n.Load())
	}
}

// ─── construction ────────────────────────────────────────────────────────

func TestNew_ValidatesItsOptionsWithoutEchoingSecrets(t *testing.T) {
	ok := Options{BaseURL: "http://synkro-products-api:8080", Token: testToken, Timeout: time.Second, Attempts: 1}
	tests := map[string]func(*Options){
		"empty token":        func(o *Options) { o.Token = "" },
		"token with newline": func(o *Options) { o.Token = "abc\r\nX-Evil: 1" },
		"token with space":   func(o *Options) { o.Token = "abc def" },
		"empty url":          func(o *Options) { o.BaseURL = "" },
		"no scheme":          func(o *Options) { o.BaseURL = "synkro-products-api:8080" },
		"ftp scheme":         func(o *Options) { o.BaseURL = "ftp://host" },
		"no host":            func(o *Options) { o.BaseURL = "http://" },
		"userinfo in url":    func(o *Options) { o.BaseURL = "http://user:s3cret@host:8080" },
		"query in url":       func(o *Options) { o.BaseURL = "http://host:8080/?x=1" },
		"zero timeout":       func(o *Options) { o.Timeout = 0 },
		"zero attempts":      func(o *Options) { o.Attempts = 0 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			o := ok
			mutate(&o)

			_, err := New(o)

			if err == nil {
				t.Fatal("expected an error")
			}
			for _, secret := range []string{testToken, "s3cret", "X-Evil"} {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("error leaks %q: %v", secret, err)
				}
			}
		})
	}
	if _, err := New(ok); err != nil {
		t.Errorf("valid options rejected: %v", err)
	}
}

func TestNew_AcceptsABaseURLWithAPathPrefixAndATrailingSlash(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		writeJSON(w, 200, productJSON(pid, 1))
	}))
	defer srv.Close()
	c, err := New(Options{BaseURL: srv.URL + "/internal/", Token: testToken, Timeout: time.Second, Attempts: 1})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := c.GetProduct(context.Background(), pid); err != nil {
		t.Fatal(err)
	}
	if path != "/internal/api/v1/products/"+pid {
		t.Errorf("unexpected path %s", path)
	}
}
