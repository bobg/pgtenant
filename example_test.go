package pgtenant_test

import (
	"context"
	"log"

	"github.com/bobg/pgtenant"
)

func Example() {
	// This is the list of permitted queries,
	// each mapped to a `Transformed` pair:
	// the string it should transform to,
	// and the number of the added positional parameter for the tenant ID.
	// Your package should include a unit test
	// that calls TransformTester with this same map
	// to ensure the pre- and post-transformation strings are correct.
	whitelist := map[string]pgtenant.Transformed{
		"INSERT INTO foo (a, b) VALUES ($1, $2)": {
			Query: "INSERT INTO foo (a, b, tenant_id) VALUES ($1, $2, $3)",
			Num:   3,
		},
		"SELECT a FROM foo WHERE b = $1": {
			Query: "SELECT a FROM foo WHERE b = $1 AND tenant_id = $2",
			Num:   2,
		},
	}

	db, err := pgtenant.Open("postgres:///mydb", "tenant_id", whitelist)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Constrain your SQL queries to a specific tenant ID
	// by placing it in a context object
	// that is then used in calls to ExecContext and QueryContext.
	ctx := context.Background()
	ctx = pgtenant.WithTenantID(ctx, int64(17))

	// This is automatically transformed to
	//   "INSERT INTO foo (a, b, tenant_id) VALUES ($1, $2, $3)"
	// with positional arguments 326, 3827, and 17.
	_, err = db.ExecContext(ctx, "INSERT INTO foo (a, b) VALUES ($1, $2)", 326, 3827)
	if err != nil {
		log.Fatal(err)
	}

	// This is automatically transformed to
	//   "UPDATE foo SET a = $1 WHERE b = $2 AND tenant_id = $3
	// with positional parameters 412, 3827, and 17,
	// even though this query does not appear in driver.Whitelist.
	// (A context produced by WithQuery bypasses that check.)
	const query = "UPDATE foo SET a = $1 WHERE b = $2"
	_, err = db.ExecContext(pgtenant.WithQuery(ctx, query), query, 412, 3827)
	if err != nil {
		log.Fatal(err)
	}
}
