// Package pgtenant automatically converts Postgresql queries for tenant isolation.
//
// Database connections made with this package will automatically modify SQL queries
// always to include "... AND tenant_id = ..." expressions in WHERE clauses,
// and to append a "tenant_id" column to any INSERT.
//
// This happens intelligently, by parsing the SQL query
// (rather than by dumb textual substitution).
// A large subset of Postgresql's SQL language is supported.
//
// This eliminates data-leak bugs arising from forgetting to scope queries to a specific tenant
// in multitenant services.
//
// The actual name of your tenant_id column is configurable,
// but every table must be defined to include one.
//
// This implementation covers a lot of the Postgresql query syntax, but not all of it.
// If you write a query that cannot be transformed because of unimplemented syntax,
// and if that query is tested with TransformTester,
// then TransformTester will emit an error message and a representation of the query's parse tree
// that together should help you to add that syntax to the transformer.
// (Alternatively, the author will entertain polite and patient requests to add missing syntax.)
package pgtenant
