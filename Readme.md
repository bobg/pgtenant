# Pgtenant

[![GoDoc](https://godoc.org/github.com/bobg/pgtenant?status.svg)](https://godoc.org/github.com/bobg/pgtenant)
[![Go Report Card](https://goreportcard.com/badge/github.com/bobg/pgtenant)](https://goreportcard.com/report/github.com/bobg/pgtenant)

This is pgtenant,
a library for adding automatic multitenant safety to Postgresql database queries.
It works within the standard Go `database/sql` framework.

In a nutshell, you write code like this as usual:

```
rows, err := db.QueryContext(ctx, "SELECT foo FROM bar WHERE baz = $1", val)
```

but it works as if you had written:

```
rows, err := db.QueryContext(ctx, "SELECT foo FROM bar WHERE baz = $1 AND tenant_id = $2", val, tenantID)
```

This happens intelligently, by parsing the SQL query
(rather than by dumb textual substitution).
A large subset of Postgresql’s SQL language is supported.

This eliminates data-leak bugs in multitenant services that arise from forgetting to scope queries to a specific tenant.

The actual name of your `tenant_id` column is configurable,
but every table must be defined to include one.

For documentation, see https://godoc.org/github.com/bobg/pgtenant.

For more about this package and its history, see https://medium.com/@bob.glickstein/tenant-isolation-in-hosted-services-d4eb75f1cb54
