package pgtenant

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	pg_query "github.com/lfittl/pg_query_go"
)

// TransformTester runs the transformer on each query that is a key in m.
// They are sorted first for a predictable test ordering.
// Each query is tested in a separate call to t.Run.
// The output of each transform is compared against the corresponding value in m.
// A mismatch produces a call to t.Error.
// Other errors produce calls to t.Fatal.
//
// Programs using this package should include a unit test
// that calls this function with the same value for m
// that is used in the Driver.Whitelist field.
func TransformTester(t *testing.T, tenantIDCol string, m map[string]Transformed) {
	// Test the items of m in the same order every time.
	var sorted sort.StringSlice
	for q := range m {
		sorted = append(sorted, q)
	}
	sorted.Sort()

	for i, pre := range sorted {
		post := m[pre]
		t.Run(fmt.Sprintf("%03d", i+1), func(t *testing.T) {
			tree, err := pg_query.Parse(pre)
			if err != nil {
				t.Fatal(err)
			}
			if len(tree.Statements) != 1 {
				t.Fatalf("got %d statements from Parse, want 1", len(tree.Statements))
			}
			stmt := tree.Statements[0]
			xformer := &transformer{
				Conn: &Conn{
					driver: &Driver{TenantIDCol: tenantIDCol},
				},
				tenantIDNum: 1 + findMaxParam(stmt),
			}
			got, err := xformer.transformStmt(stmt)
			if err != nil {
				t.Fatalf("transform error: %s\n%s", err, spew.Sdump(tree))
			}
			if !strings.EqualFold(got, post.Query) {
				t.Errorf("mismatch\ngot  %s\nwant %s\n%s", got, post.Query, spew.Sdump(tree))
			}
		})
	}
}
