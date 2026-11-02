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
	"github.com/code-corhuila/synkro-worker/internal/application/port"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
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
