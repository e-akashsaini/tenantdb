# Putting a tenant-aware proxy in front of a real Rails app

*What tenantdb found in DocuSeal, and how I tried to make sure it doesn't lie.*

## The problem

Most SaaS apps keep every customer's rows in the same tables and rely on one habit: every query carries `WHERE account_id = ...`. The habit lives in application code. One forgotten filter returns another customer's data, and nothing warns you. Postgres row-level security fixes this properly, but retrofitting it onto an app that already exists is a big project, so most apps never do it.

tenantdb is the step before that. It is a proxy that speaks the Postgres wire protocol. It reads every statement with Postgres's own parser, works out which tenant the statement is about, and then either reports the ones it cannot pin to a tenant (alert mode) or refuses them (block mode). The application is not changed; only its database port is.

It is an audit and retrofit tool. It does not replace row-level security.

## The hard part is not parsing

`WHERE tenant_id = 'acme'` is easy. The real statements are not:

```sql
SELECT * FROM orders WHERE id = 5                       -- whose order?
SELECT * FROM items WHERE order_id IN (SELECT id FROM orders WHERE tenant_id = 'acme')
SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers c
                                     WHERE c.id = o.customer_id AND c.tenant_id = 'acme')
SELECT * FROM users u JOIN access_tokens t ON t.user_id = u.id WHERE t.token = $1
```

For each one the tool has to answer "which tenants' rows can this touch?" The rule it follows is deliberately conservative: a statement counts as confined to a tenant only if **every** table it reads is confined. `AND` narrows, `OR` must confine every alternative, and the optional side of an outer join confines nothing. When it cannot prove a statement is confined, it says `unknown`. It would rather flag a harmless statement than vouch for a dangerous one.

Row ownership comes from the data: a unique key or a verified foreign key lets it ask the database (or its cache) who owns row 5, then carry that answer through joins, subqueries, views, triggers and `EXISTS`.

## How I checked it isn't wrong

A tool whose job is to claim "this only touches acme" is dangerous if it is sometimes wrong, so most of the effort went into trying to catch it.

- **Postgres as the oracle.** A fuzzer generates random statements over a schema with several tenants, runs each one against a real Postgres, reads which tenants' rows were actually touched, and fails if the tool claimed a smaller set. Sweeps are tens of thousands of statements, repeated over many seeds; the current code produces no unsound claims.
- **Mutation checks.** When I add a rule, I deliberately break it (let `OR` count as a restriction, drop the check that stops `EXISTS (SELECT count(*) ...)` from being trusted, which always returns a row) and confirm the fuzzer notices. A fuzzer that cannot detect a planted bug proves nothing. The first version of the generator for the latest rule (correlated `EXISTS`) missed the planted bug, so I extended the generator until it did not.
- **Go's native fuzzers** on the parser and the COPY stream reader, about twenty million executions without a failure.
- **Real systems, not just tests.** The most valuable bugs came from running against real software: password logins failing when the database used TLS (SCRAM channel binding), a graceful shutdown that cut a running query off, PL/pgSQL functions the tool treated as unreadable because of `RETURN expr`, and nine known vulnerabilities in the Go toolchain and a dependency.

## What it found in DocuSeal

DocuSeal is an open-source e-signature app (Rails 8). I ran its official image with all database traffic through tenantdb and drove it with a script: setup, login, browsing, uploading a template, creating and completing submissions through the API.

On the second pass over the same traffic, 4,923 statements were attributed:

| How the tenant was determined | Share |
|---|---:|
| shared tables, `SELECT 1`, ... | 37.4% |
| a row looked up by key, foreign key, polymorphic parent, credential or parent of a child table | 38.7% |
| the statement names the account | 18.7% |
| tenant data with no account or row filter | 4.3% |
| tenant data, tenant not determined | 0.3% |
| a row that does not exist | 0.5% |

So **4.7% would be flagged** by block mode. What those were is more interesting than the percentage: lookups by the *other* account's id (cross-account by design), and settings read by `key` with no account filter. That second kind is exactly what an audit is for. Whether any of it is a bug depends on the app, and I did not read DocuSeal's source.

Run in block mode, the unmodified app stops at the first page, because its "has anyone registered yet" check reads `users` with no filter. That is the correct behaviour for a tool that refuses what it cannot attribute. It is the reason the advice is alert mode first, then exempt the roles and queries that are cross-tenant on purpose, then block.

Running the real app also made the tool better. Statements like `submitters WHERE submission_id = $1` (the standard Rails child-table lookup) were wrongly labelled as reading every tenant. Polymorphic associations (`record_type`, `record_id`) were opaque. A tenant column hidden behind a view, or a lookup by API token, could not be followed. Each was fixed, and the share flagged on the same traffic fell from 16% to 4.7%.

## Cost

Almost all of the time is spent in the Postgres parser, about 40 to 270 microseconds per statement depending on its size, and under 5 microseconds for the execution of an already-prepared statement. The analysis on top is a small fraction of that.

## Caveats you should know before quoting any of this

- **One app, one account.** DocuSeal's open-source build runs a single account, so cross-tenant behaviour (a real leak between two accounts) is not exercised by this run. It shows how well statements can be *attributed*, not that isolation holds.
- **Synthetic traffic.** A script, not users. It covers the API and part of the UI.
- **"Flagged" means "could not be tied to a tenant", not "the app has a bug".**
- **A different app will give different numbers.**

## Try it

[docs/QUICKSTART.md](QUICKSTART.md) takes about five minutes and needs only Docker. The [README](../README.md) covers the full feature set, and [docs/OPERATIONS.md](OPERATIONS.md) covers running it in production. If a statement is attributed wrongly, that is the most useful bug report there is: an issue with the SQL and the table definitions is all it takes.
