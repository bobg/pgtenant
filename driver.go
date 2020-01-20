package pgtenant

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// Driver implements database/sql/driver.Driver and driver.DriverContext.
type Driver struct {
	// TenantIDCol is the name of the column in all tables of the db schema
	// whose value is the tenant ID.
	TenantIDCol string

	// Whitelist maps SQL query strings to the output expected when transforming them.
	// It serves double-duty here:
	//
	//   1. It is a whitelist of permitted queries.
	//      Database connections created from this driver will refuse to execute a query
	//      unless it appears in this whitelist or is "escaped"
	//      by attaching it to a context object using WithQuery.
	//
	//   2. It is a cache of precomputed transforms.
	//
	// The whitelist is consulted by exact string matching
	// (modulo some minimal whitespace trimming)
	// using the query string passed to QueryContext or ExecContext.
	//
	// The value used here should also be used in a unit test that calls TransformTester.
	// That will ensure the pre- and post-transform queries are correct.
	Whitelist map[string]Transformed

	dynamicCache queryCache
}

// Transformed is the output of the transformer:
// a transformed query and the number of the positional parameter added for a tenant-ID value.
type Transformed struct {
	Query string
	Num   int
}

// assert *Driver satisfies the driver.Driver and driver.DriverContext interfaces.
var (
	_ driver.Driver        = (*Driver)(nil)
	_ driver.DriverContext = (*Driver)(nil)
)

// Open implements driver.Driver.Open.
func (d *Driver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// OpenConnector implements driver.DriverContext.OpenConnector.
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	c, err := pq.NewConnector(name)
	return &Connector{nested: c, driver: d}, err
}

// Connector implements driver.Connector.
type Connector struct {
	nested *pq.Connector
	driver *Driver
}

// assert *Connector satisfies the driver.Connector interface.
var _ driver.Connector = (*Connector)(nil)

// Connect implements driver.Connector.Connect.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	nestedConn, err := c.nested.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to database")
	}
	nestedCtx, ok := nestedConn.(ctxConn)
	if !ok {
		return nil, errors.New("connection does not support context")
	}
	conn := &Conn{
		ctxConn: nestedCtx,
		driver:  c.driver,
	}
	return conn, nil
}

// Driver implements driver.Connector.Driver.
func (c *Connector) Driver() driver.Driver { return c.driver }

// Open is a convenient shorthand for either of these two sequences:
//
//   driver := &pgtenant.Driver{TenantIDCol: tenantIDCol, Whitelist: whitelist}
//   sql.Register(driverName, driver)
//   db, err := sql.Open(driverName, dsn)
//
// and
//
//   driver := &pgtenant.Driver{TenantIDCol: tenantIDCol, Whitelist: whitelist}
//   connector, err := driver.OpenConnector(dsn)
//   if err != nil { ... }
//   db := sql.OpenDB(connector)
//
// The first sequence creates a reusable driver object that can open multiple different databases.
// The second sequence creates an additional reusable connector object that can open the same database multiple times.
func Open(dsn, tenantIDCol string, whitelist map[string]Transformed) (*sql.DB, error) {
	d := &Driver{
		TenantIDCol: tenantIDCol,
		Whitelist:   whitelist,
	}
	c, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(c), nil
}
