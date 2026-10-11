# Contributing

Thanks for looking. Bug reports, failing statements and small fixes are all welcome.

## Reporting a problem

- **A statement attributed wrongly** is the most useful report. Include the SQL, the schema it ran against (`CREATE TABLE` statements are enough), what tenantdb said, and what you expected. Please remove real data first.
- **A security problem**: do not open an issue. See [SECURITY.md](SECURITY.md).

## Changing code

```bash
cd proxy
go vet ./...
go test -race ./...
```

Tests that need a database read `TENANTDB_TEST_DSN` (for example `postgres://postgres:pw@127.0.0.1:5432/postgres`, a throwaway Postgres 16) and are skipped without it. CI runs them.

The project's one hard rule is **soundness**: tenantdb may say "unknown", but it must never claim a tenant set that is smaller than what a statement can really touch. A change to how statements are analysed should come with:

1. a unit test for the new shape and one for the shapes it must still *not* trust;
2. a run of the differential fuzzer against a real Postgres (`TENANTDB_FUZZ_N=6000 TENANTDB_FUZZ_SEED=1 go test -run TestDifferential ./internal/attrib`), extended with the new shape if the generator does not produce it yet.

Keep changes small and focused. Format with `gofmt`.

## Sign your commits

By contributing you agree to the [Developer Certificate of Origin](https://developercertificate.org): you wrote the change or have the right to submit it, under this project's Apache-2.0 licence. Add the sign-off with `git commit -s`, which appends a `Signed-off-by:` line.
