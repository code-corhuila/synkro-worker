package port

import (
	"context"
	"errors"

	"github.com/code-corhuila/synkro-worker/internal/domain/model"
)

// Failure classes a ProductsGateway reports. Adapters wrap them with detail
// (operation, HTTP status, Products error code) but never with credentials or
// response bodies, so callers branch with errors.Is.
var (
	// ErrCredentialRejected: Products refused the SERVICE token (401) or its
	// permissions (403). Retrying or continuing cannot help.
	ErrCredentialRejected = errors.New("products rejected the service credential")
	// ErrNotFound: the product or alert does not exist for this caller. It is
	// never evidence that stock was replenished.
	ErrNotFound = errors.New("products resource not found")
	// ErrRejected: a permanent refusal of this request (validation, business
	// rule, unexpected redirect). The same request would be refused again.
	ErrRejected = errors.New("products rejected the request")
	// ErrUnavailable: network failure, 429 or 5xx persisted through every
	// attempt. The outcome of a mutation is unconfirmed.
	ErrUnavailable = errors.New("products unavailable")
	// ErrMalformedResponse: the response was not what the contract promises
	// or contradicts the request. It never counts as success.
	ErrMalformedResponse = errors.New("malformed products response")
)

// PageInfo is the pagination envelope of a list response.
type PageInfo struct {
	Page       int
	Limit      int
	Total      int
	TotalPages int
}

type ProductPage struct {
	Items []model.Product
	Info  PageInfo
}

type AlertPage struct {
	Items []model.StockAlert
	Info  PageInfo
}

// OpenAlertResult says what Products answered to an opening: Created is true
// for a new alert (201) and false when Products replayed an existing one (200).
type OpenAlertResult struct {
	Alert   model.StockAlert
	Created bool
}

// ProductsGateway is the output port to the Products service. Implementations
// must honour ctx for cancellation and deadlines, and carry the correlation id
// found in ctx (see CorrelationIDFrom) on every request.
type ProductsGateway interface {
	// ListProductsAtMost reads products whose stock is <= maxStock.
	ListProductsAtMost(ctx context.Context, maxStock, page, limit int) (ProductPage, error)
	GetProduct(ctx context.Context, productID string) (model.Product, error)
	ListAlerts(ctx context.Context, status model.AlertStatus, page, limit int) (AlertPage, error)
	// OpenAlert re-sends the same intent.Key on every retry it makes.
	OpenAlert(ctx context.Context, intent model.OpeningIntent) (OpenAlertResult, error)
	ResolveAlert(ctx context.Context, alertID string) (model.StockAlert, error)
}
