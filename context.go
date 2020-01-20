package pgtenant

import (
	"context"
	"database/sql/driver"
	"errors"
)

type keyType int

const (
	queryKey keyType = iota
	tenantKey
)

// WithQuery adds a query to the given context,
// "escaping" it to allow its use by a connection
// even if it does not appear in the driver's whitelist.
func WithQuery(ctx context.Context, query string) context.Context {
	return context.WithValue(ctx, queryKey, query)
}

// WithTenantID adds a tenant ID to the given context.
// Any queries issued with the returned context will be scoped to tenantID.
// The dynamic type of tenantID must be one of:
//   - []byte
//   - int64
//   - float64
//   - string
//   - bool
//   - time.Time
// (as described in the documentation for driver.Value).
func WithTenantID(ctx context.Context, tenantID driver.Value) context.Context {
	return context.WithValue(ctx, tenantKey, tenantID)
}

// ErrNoID is the error produced when no tenant ID value has been attached to a context with WithTenantID.
var ErrNoID = errors.New("no ID")

// ID returns the tenant ID carried by ctx.
func ID(ctx context.Context) (driver.Value, error) {
	id := ctx.Value(tenantKey)
	if id == nil {
		return nil, ErrNoID
	}
	return id.(driver.Value), nil
}
