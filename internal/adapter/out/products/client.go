// Package products is the HTTP adapter of the ProductsGateway output port. It
// speaks the public Products contract (07-api/contracts/openapi/
// synkro-products-api.yaml) with the Worker's SERVICE token and nothing else:
// no database, no private endpoint.
package products

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/code-corhuila/synkro-worker/internal/application/port"
	"github.com/code-corhuila/synkro-worker/internal/domain/model"
)

const (
	// maxBody bounds what is read from any response. A page of 100 products
	// is a few tens of KiB, so 1 MiB is generous and still finite.
	maxBody = 1 << 20

	// Products accepts Idempotency-Key of 8 to 128 characters.
	minKeyLen, maxKeyLen = 8, 128

	defaultBackoffBase = 200 * time.Millisecond
	defaultBackoffMax  = 5 * time.Second
	maxAttempts        = 10
)

var uuidRE = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Options configures a Client. Sleep and Rand exist so tests can observe and
// skip the backoff; production leaves them nil.
type Options struct {
	BaseURL string // origin of Products, e.g. http://synkro-products-api:8080
	Token   string // the Worker's SERVICE token; never logged or put in errors

	Timeout  time.Duration // per attempt
	Attempts int           // total attempts per call, including the first

	HTTPClient  *http.Client
	Sleep       func(ctx context.Context, d time.Duration) error
	Rand        func(n int64) int64 // uniform in [0, n)
	BackoffBase time.Duration
	BackoffMax  time.Duration
}

// Client implements port.ProductsGateway over HTTP.
type Client struct {
	base     *url.URL
	token    string
	http     *http.Client
	timeout  time.Duration
	attempts int
	sleep    func(ctx context.Context, d time.Duration) error
	rand     func(n int64) int64
	bkBase   time.Duration
	bkMax    time.Duration
}

var _ port.ProductsGateway = (*Client)(nil)

func New(o Options) (*Client, error) {
	// None of these errors echoes the URL or the token: either may carry
	// credentials.
	if o.Token == "" {
		return nil, errors.New("products client: service token is required")
	}
	if strings.ContainsAny(o.Token, " \t\r\n") || strings.IndexFunc(o.Token, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
		return nil, errors.New("products client: service token must not contain whitespace or control characters")
	}
	base, err := url.Parse(o.BaseURL)
	switch {
	case err != nil || o.BaseURL == "":
		return nil, errors.New("products client: base URL is not a valid URL")
	case base.Scheme != "http" && base.Scheme != "https":
		return nil, errors.New("products client: base URL must start with http:// or https://")
	case base.Hostname() == "":
		return nil, errors.New("products client: base URL has no host")
	case base.User != nil:
		return nil, errors.New("products client: base URL must not contain credentials")
	case base.RawQuery != "" || base.Fragment != "":
		return nil, errors.New("products client: base URL must not contain a query or fragment")
	}
	if o.Timeout <= 0 {
		return nil, errors.New("products client: timeout must be positive")
	}
	if o.Attempts < 1 || o.Attempts > maxAttempts {
		return nil, fmt.Errorf("products client: attempts must be between 1 and %d", maxAttempts)
	}

	c := &Client{
		base:     base,
		token:    o.Token,
		timeout:  o.Timeout,
		attempts: o.Attempts,
		sleep:    o.Sleep,
		rand:     o.Rand,
		bkBase:   o.BackoffBase,
		bkMax:    o.BackoffMax,
		http:     o.HTTPClient,
	}
	if c.http == nil {
		c.http = &http.Client{}
	}
	// Never follow a redirect: it would carry the SERVICE token to wherever
	// the response points. A 3xx is reported as a rejection instead.
	hc := *c.http
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c.http = &hc
	if c.sleep == nil {
		c.sleep = sleepContext
	}
	if c.rand == nil {
		c.rand = rand.Int64N
	}
	if c.bkBase <= 0 {
		c.bkBase = defaultBackoffBase
	}
	if c.bkMax <= 0 {
		c.bkMax = defaultBackoffMax
	}
	return c, nil
}

// String keeps the credential out of any formatted output of the client.
func (c *Client) String() string { return "products.Client{" + c.base.Host + "}" }

// GoString does the same for %#v.
func (c *Client) GoString() string { return c.String() }

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// ─── port methods ────────────────────────────────────────────────────────

func (c *Client) ListProductsAtMost(ctx context.Context, maxStock, page, limit int) (port.ProductPage, error) {
	q := url.Values{}
	q.Set("stockAtMost", strconv.Itoa(maxStock))
	q.Set("page", strconv.Itoa(page))
	q.Set("limit", strconv.Itoa(limit))
	const op = "list products"

	body, err := c.call(ctx, op, request{method: http.MethodGet, path: "/api/v1/products", query: q}, http.StatusOK)
	if err != nil {
		return port.ProductPage{}, err
	}
	var env struct {
		Data *[]productDTO `json:"data"`
		Meta *metaDTO      `json:"meta"`
	}
	if err := decode(op, body, &env); err != nil {
		return port.ProductPage{}, err
	}
	if env.Data == nil {
		return port.ProductPage{}, malformed(op, "data is missing")
	}
	info, err := env.Meta.validate(op, page, limit, len(*env.Data))
	if err != nil {
		return port.ProductPage{}, err
	}
	items := make([]model.Product, 0, len(*env.Data))
	for i, d := range *env.Data {
		p, err := d.toModel(op)
		if err != nil {
			return port.ProductPage{}, err
		}
		if p.Stock > maxStock {
			return port.ProductPage{}, malformed(op, "item %d has stock %d, above stockAtMost=%d", i, p.Stock, maxStock)
		}
		items = append(items, p)
	}
	return port.ProductPage{Items: items, Info: info}, nil
}

func (c *Client) GetProduct(ctx context.Context, productID string) (model.Product, error) {
	const op = "get product"
	if !uuidRE.MatchString(productID) {
		return model.Product{}, fmt.Errorf("%s: %w: product id is not a UUID", op, port.ErrRejected)
	}
	body, err := c.call(ctx, op, request{method: http.MethodGet, path: "/api/v1/products/" + productID}, http.StatusOK)
	if err != nil {
		return model.Product{}, err
	}
	var d productDTO
	if err := decode(op, body, &d); err != nil {
		return model.Product{}, err
	}
	p, err := d.toModel(op)
	if err != nil {
		return model.Product{}, err
	}
	if !strings.EqualFold(p.ID, productID) {
		return model.Product{}, malformed(op, "answer is for another product")
	}
	return p, nil
}

func (c *Client) ListAlerts(ctx context.Context, status model.AlertStatus, page, limit int) (port.AlertPage, error) {
	q := url.Values{}
	q.Set("status", string(status))
	q.Set("page", strconv.Itoa(page))
	q.Set("limit", strconv.Itoa(limit))
	const op = "list stock alerts"

	body, err := c.call(ctx, op, request{method: http.MethodGet, path: "/api/v1/stock-alerts", query: q}, http.StatusOK)
	if err != nil {
		return port.AlertPage{}, err
	}
	var env struct {
		Data *[]alertDTO `json:"data"`
		Meta *metaDTO    `json:"meta"`
	}
	if err := decode(op, body, &env); err != nil {
		return port.AlertPage{}, err
	}
	if env.Data == nil {
		return port.AlertPage{}, malformed(op, "data is missing")
	}
	info, err := env.Meta.validate(op, page, limit, len(*env.Data))
	if err != nil {
		return port.AlertPage{}, err
	}
	items := make([]model.StockAlert, 0, len(*env.Data))
	for i, d := range *env.Data {
		a, err := d.toModel(op)
		if err != nil {
			return port.AlertPage{}, err
		}
		if a.Status != status {
			return port.AlertPage{}, malformed(op, "item %d is %s in a list filtered by %s", i, a.Status, status)
		}
		items = append(items, a)
	}
	return port.AlertPage{Items: items, Info: info}, nil
}

func (c *Client) OpenAlert(ctx context.Context, intent model.OpeningIntent) (port.OpenAlertResult, error) {
	const op = "open stock alert"
	if n := len(intent.Key); n < minKeyLen || n > maxKeyLen {
		return port.OpenAlertResult{}, fmt.Errorf("%s: %w: idempotency key must have %d to %d characters", op, port.ErrRejected, minKeyLen, maxKeyLen)
	}
	if !uuidRE.MatchString(intent.ProductID) {
		return port.OpenAlertResult{}, fmt.Errorf("%s: %w: product id is not a UUID", op, port.ErrRejected)
	}
	payload, err := json.Marshal(struct {
		ProductID      string `json:"productId"`
		StockAtOpening int    `json:"stockAtOpening"`
	}{intent.ProductID, intent.StockAtOpening})
	if err != nil {
		return port.OpenAlertResult{}, fmt.Errorf("%s: %w", op, err)
	}

	// call re-sends this exact request, key included, on every retry.
	status, body, err := c.callStatus(ctx, op, request{
		method: http.MethodPost, path: "/api/v1/stock-alerts", body: payload, idempotencyKey: intent.Key,
	}, http.StatusOK, http.StatusCreated)
	if err != nil {
		return port.OpenAlertResult{}, err
	}
	var d alertDTO
	if err := decode(op, body, &d); err != nil {
		return port.OpenAlertResult{}, err
	}
	a, err := d.toModel(op)
	if err != nil {
		return port.OpenAlertResult{}, err
	}
	if !strings.EqualFold(a.ProductID, intent.ProductID) {
		return port.OpenAlertResult{}, malformed(op, "answer is an alert of another product")
	}
	return port.OpenAlertResult{Alert: a, Created: status == http.StatusCreated}, nil
}

func (c *Client) ResolveAlert(ctx context.Context, alertID string) (model.StockAlert, error) {
	const op = "resolve stock alert"
	if !uuidRE.MatchString(alertID) {
		return model.StockAlert{}, fmt.Errorf("%s: %w: alert id is not a UUID", op, port.ErrRejected)
	}
	body, err := c.call(ctx, op, request{method: http.MethodPost, path: "/api/v1/stock-alerts/" + alertID + "/resolve"}, http.StatusOK)
	if err != nil {
		return model.StockAlert{}, err
	}
	var d alertDTO
	if err := decode(op, body, &d); err != nil {
		return model.StockAlert{}, err
	}
	a, err := d.toModel(op)
	if err != nil {
		return model.StockAlert{}, err
	}
	if !strings.EqualFold(a.ID, alertID) {
		return model.StockAlert{}, malformed(op, "answer is another alert")
	}
	return a, nil
}

// ─── transport with bounded retries ──────────────────────────────────────

type request struct {
	method         string
	path           string
	query          url.Values
	body           []byte
	idempotencyKey string
}

func (c *Client) call(ctx context.Context, op string, r request, ok ...int) ([]byte, error) {
	_, body, err := c.callStatus(ctx, op, r, ok...)
	return body, err
}

// callStatus sends r until it gets a definitive answer or runs out of
// attempts. Only network failures, 429 and 5xx are retried: they say nothing
// about the request being wrong, and a lost response does not prove the
// mutation was rolled back, which is why the request, Idempotency-Key
// included, is sent unchanged every time. Everything else is final.
func (c *Client) callStatus(ctx context.Context, op string, r request, ok ...int) (int, []byte, error) {
	var last string
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, nil, fmt.Errorf("%s: %w", op, err)
		}
		status, body, header, err := c.once(ctx, r)

		switch {
		case err != nil:
			if cerr := ctx.Err(); cerr != nil { // cancelled or past the run's deadline: not a retry case
				return 0, nil, fmt.Errorf("%s: %w", op, cerr)
			}
			var te interface{ Timeout() bool }
			if errors.As(err, &te) && te.Timeout() {
				last = "timeout after " + c.timeout.String()
			} else {
				last = "network error: " + sanitizeNetErr(err)
			}
		case status == http.StatusTooManyRequests || status >= 500:
			last = "HTTP " + strconv.Itoa(status) + describe(body, header)
		default:
			return finish(op, status, body, header, ok)
		}

		if attempt >= c.attempts {
			return 0, nil, fmt.Errorf("%s: %w: %s (after %d attempts)", op, port.ErrUnavailable, last, attempt)
		}
		if err := c.sleep(ctx, c.backoff(attempt)); err != nil {
			return 0, nil, fmt.Errorf("%s: %w", op, err)
		}
	}
}

// backoff is exponential with full jitter: a uniform wait between 0 and
// min(max, base*2^(attempt-1)).
func (c *Client) backoff(attempt int) time.Duration {
	ceiling := c.bkBase << (attempt - 1)
	if ceiling <= 0 || ceiling > c.bkMax {
		ceiling = c.bkMax
	}
	return time.Duration(c.rand(int64(ceiling)))
}

// once performs a single HTTP attempt under its own timeout.
func (c *Client) once(ctx context.Context, r request) (int, []byte, http.Header, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + r.path
	if r.query != nil {
		u.RawQuery = r.query.Encode()
	}
	var rd io.Reader
	if r.body != nil {
		rd = bytes.NewReader(r.body)
	}
	req, err := http.NewRequestWithContext(ctx, r.method, u.String(), rd)
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if id := port.CorrelationIDFrom(ctx); id != "" {
		req.Header.Set("X-Correlation-Id", id)
	}
	if r.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if r.idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", r.idempotencyKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, b, resp.Header, nil
}

// sanitizeNetErr drops the request URL that *url.Error prefixes to the cause.
func sanitizeNetErr(err error) string {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err.Error()
	}
	return err.Error()
}

// finish classifies a definitive (non-retryable) answer.
func finish(op string, status int, body []byte, h http.Header, ok []int) (int, []byte, error) {
	for _, s := range ok {
		if status == s {
			if len(body) > maxBody {
				return 0, nil, malformed(op, "response larger than %d bytes", maxBody)
			}
			return status, body, nil
		}
	}
	detail := describe(body, h)
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return 0, nil, fmt.Errorf("%s: %w: HTTP %d%s", op, port.ErrCredentialRejected, status, detail)
	case status == http.StatusNotFound:
		return 0, nil, fmt.Errorf("%s: %w: HTTP %d%s", op, port.ErrNotFound, status, detail)
	case status >= 200 && status < 300:
		return 0, nil, malformed(op, "unexpected HTTP %d", status)
	default: // other 4xx, and 3xx which are never followed
		return 0, nil, fmt.Errorf("%s: %w: HTTP %d%s", op, port.ErrRejected, status, detail)
	}
}

// describe extracts the error code and traceId of the Products envelope. The
// message and the raw body are deliberately left out: they are not needed to
// diagnose (the traceId finds the server log) and must never reach ours.
func describe(body []byte, _ http.Header) string {
	if len(body) > maxBody {
		return ""
	}
	var env struct {
		Error   string `json:"error"`
		TraceID string `json:"traceId"`
	}
	if json.Unmarshal(body, &env) != nil || (env.Error == "" && env.TraceID == "") {
		return ""
	}
	return fmt.Sprintf(" (%s, traceId %s)", safeToken(env.Error), safeToken(env.TraceID))
}

// safeToken keeps a short, printable-ASCII identifier and drops the rest.
func safeToken(s string) string {
	if len(s) > 64 {
		s = s[:64]
	}
	var b strings.Builder
	for _, r := range s {
		if r > 0x20 && r < 0x7f {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "-"
	}
	return b.String()
}

// ─── response validation ─────────────────────────────────────────────────

func malformed(op, format string, args ...any) error {
	return fmt.Errorf("%s: %w: %s", op, port.ErrMalformedResponse, fmt.Sprintf(format, args...))
}

// decode parses exactly one JSON value into v.
func decode(op string, body []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(v); err != nil {
		return malformed(op, "body is not the expected JSON (%v)", errKind(err))
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return malformed(op, "unexpected data after the JSON value")
	}
	return nil
}

// errKind names a decode error without echoing any of the body.
func errKind(err error) string {
	var ute *json.UnmarshalTypeError
	var se *json.SyntaxError
	switch {
	case errors.As(err, &ute):
		return "wrong type for field " + ute.Field
	case errors.As(err, &se):
		return "syntax error"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "empty or truncated"
	}
	return "invalid"
}

type metaDTO struct {
	Page       *int `json:"page"`
	Limit      *int `json:"limit"`
	Total      *int `json:"total"`
	TotalPages *int `json:"totalPages"`
}

// validate checks the envelope against what was asked for: a server that
// answers another page or limit, or more items than the limit, contradicts
// the request and cannot be trusted to paginate.
func (m *metaDTO) validate(op string, page, limit, items int) (port.PageInfo, error) {
	if m == nil || m.Page == nil || m.Limit == nil || m.Total == nil || m.TotalPages == nil {
		return port.PageInfo{}, malformed(op, "meta is missing or incomplete")
	}
	if *m.Page != page || *m.Limit != limit {
		return port.PageInfo{}, malformed(op, "meta answers page %d limit %d, asked page %d limit %d", *m.Page, *m.Limit, page, limit)
	}
	if *m.Total < 0 || *m.TotalPages < 0 {
		return port.PageInfo{}, malformed(op, "meta has negative counts")
	}
	if items > limit {
		return port.PageInfo{}, malformed(op, "%d items for a limit of %d", items, limit)
	}
	return port.PageInfo{Page: *m.Page, Limit: *m.Limit, Total: *m.Total, TotalPages: *m.TotalPages}, nil
}

type productDTO struct {
	ProductID *string `json:"productId"`
	Stock     *int    `json:"stock"`
}

// toModel never defaults a missing field: a product without a stock is not a
// product with stock 0.
func (d productDTO) toModel(op string) (model.Product, error) {
	if d.ProductID == nil || !uuidRE.MatchString(*d.ProductID) {
		return model.Product{}, malformed(op, "product without a valid productId")
	}
	if d.Stock == nil {
		return model.Product{}, malformed(op, "product without stock")
	}
	if *d.Stock < 0 {
		return model.Product{}, malformed(op, "product with negative stock")
	}
	return model.Product{ID: *d.ProductID, Stock: *d.Stock}, nil
}

type alertDTO struct {
	AlertID        *string `json:"alertId"`
	ProductID      *string `json:"productId"`
	Status         *string `json:"status"`
	StockAtOpening *int    `json:"stockAtOpening"`
}

func (d alertDTO) toModel(op string) (model.StockAlert, error) {
	if d.AlertID == nil || !uuidRE.MatchString(*d.AlertID) {
		return model.StockAlert{}, malformed(op, "alert without a valid alertId")
	}
	if d.ProductID == nil || !uuidRE.MatchString(*d.ProductID) {
		return model.StockAlert{}, malformed(op, "alert without a valid productId")
	}
	if d.Status == nil {
		return model.StockAlert{}, malformed(op, "alert without status")
	}
	st := model.AlertStatus(*d.Status)
	if st != model.AlertOpen && st != model.AlertResolved {
		return model.StockAlert{}, malformed(op, "alert with unknown status")
	}
	a := model.StockAlert{ID: *d.AlertID, ProductID: *d.ProductID, Status: st}
	if d.StockAtOpening != nil {
		a.StockAtOpening = *d.StockAtOpening
	}
	return a, nil
}
