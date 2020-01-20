package pgtenant

import (
	"context"
	"database/sql/driver"
	"strings"

	pg_query "github.com/lfittl/pg_query_go"
	"github.com/pkg/errors"
)

// assert *Conn satisfies the driver.Conn interface
var _ driver.Conn = (*Conn)(nil)

type ctxConn interface {
	driver.Conn
	driver.ExecerContext
	driver.QueryerContext
	driver.ConnBeginTx

	// Deprecated in Go 1.10: golang/go: 5327148
	driver.Execer
	driver.Queryer
}

// Conn implements driver.Conn.
type Conn struct {
	ctxConn
	driver *Driver
}

// ErrUnknownQuery indicates a query that cannot be safely transformed.
var ErrUnknownQuery = errors.New("unknown query")

func (c *Conn) transform(ctx context.Context, query string) (string, int, error) {
	query = normalize(query)
	if found, ok := c.driver.Whitelist[query]; ok {
		return found.Query, found.Num, nil
	}
	if escapedQuery, ok := ctx.Value(queryKey).(string); !ok || normalize(escapedQuery) != query {
		return "", 0, errors.Wrap(ErrUnknownQuery, query)
	}
	if found, ok := c.driver.dynamicCache.lookup(query); ok {
		return found.Query, found.Num, nil
	}

	tree, err := pg_query.Parse(query)
	if err != nil {
		return "", 0, err
	}
	transformedQ, tenantIDNum, err := c.doTransform(tree)
	if err != nil {
		return "", 0, err
	}
	c.driver.dynamicCache.add(query, Transformed{transformedQ, tenantIDNum})
	return transformedQ, tenantIDNum, nil
}

func (c *Conn) doTransform(tree pg_query.ParsetreeList) (string, int, error) {
	tenantIDNum := 1 + findMaxParam(tree)
	t := &transformer{
		Conn:        c,
		tenantIDNum: tenantIDNum,
	}
	res, err := t.transformTree(tree)
	if err != nil {
		return "", 0, err
	}
	if !t.isTransformed {
		tenantIDNum = 0
	}
	return res, tenantIDNum, err
}

func normalize(q string) string {
	lines := strings.Split(q, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
	}
	for len(lines) > 0 && len(lines[0]) == 0 {
		lines = lines[1:]
	}
	for len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	return strings.Join(lines, " ")
}

// Prepare prepares the given query string,
// transforming it on the fly for tenancy isolation.
// Callers using the resulting statement's Exec or Query methods
// must be sure to add an argument containing the tenant ID.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	transformed, _, err := c.transform(context.Background(), query)
	if err != nil {
		return nil, err
	}
	return c.ctxConn.Prepare(transformed)
}

// Close implements driver.Conn.Close.
func (c *Conn) Close() error {
	return c.ctxConn.Close()
}

// Begin implements driver.Conn.Begin.
func (c *Conn) Begin() (driver.Tx, error) {
	return c.ctxConn.Begin()
}

// QueryContext implements driver.QueryerContext.QueryContext.
// The context must have a tenant-ID value attached from WithTenantID.
// The query must be attached to the context from WithQuery,
// or else appear as a key in the Whitelist field of the Driver from which c was obtained.
// The query is transformed to include any necessary tenant-ID clauses,
// and args extended to include the tenant-ID value from ctx.
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	transformedQuery, tenantIDNum, err := c.transform(ctx, query)
	if err != nil {
		return nil, err
	}
	if tenantIDNum > 0 {
		id, err := ID(ctx)
		if err != nil {
			return nil, err
		}
		tenantIDArg := driver.NamedValue{Ordinal: tenantIDNum, Value: id}
		args = append(args, tenantIDArg)
	}
	return c.ctxConn.QueryContext(ctx, transformedQuery, args)
}

// ExecContext implements driver.ExecerContext.ExecContext.
// The context must have a tenant-ID value attached from WithTenantID.
// The query must be attached to the context from WithQuery,
// or else appear as a key in the Whitelist field of the Driver from which c was obtained.
// The query is transformed to include any necessary tenant-ID clauses,
// and args extended to include the tenant-ID value from ctx.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	transformedQuery, tenantIDNum, err := c.transform(ctx, query)
	if err != nil {
		return nil, err
	}
	if tenantIDNum > 0 {
		id, err := ID(ctx)
		if err != nil {
			return nil, err
		}
		tenantIDArg := driver.NamedValue{Ordinal: tenantIDNum, Value: id}
		args = append(args, tenantIDArg)
	}
	return c.ctxConn.ExecContext(ctx, transformedQuery, args)
}
