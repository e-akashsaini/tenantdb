# Trying tenantdb on a real Rails app: DocuSeal

[DocuSeal](https://github.com/docusealco/docuseal) is an open-source e-signature
app (Rails 8, ActiveRecord, Postgres). Its tables carry an `account_id` column,
which makes it a fair stand-in for "a multi-tenant Rails application nobody wrote
with tenantdb in mind". These scripts run it from its official Docker image with
all of its database traffic going through tenantdb.

```bash
bench/docuseal/up.sh                  # Postgres + tenantdb (alert mode) + DocuSeal
ROUNDS=8 bench/docuseal/traffic.sh    # drive it
bench/docuseal/down.sh
```

`traffic.sh` does what the product's users and integrations do: first-run setup
and login, browsing the web UI, uploading a PDF template, creating submissions
through the JSON API, opening the public signing link, completing it, and listing
everything. Every request is real ActiveRecord traffic.

## What it found

Measured pass: 234 HTTP requests, **4,946 statements** attributed. (The first
version of the engine, before this exercise, flagged 16% of the same kind of
traffic; the changes it drove are listed below.)

| How the tenant was determined | statements |
|---|---:|
| shared tables, `SELECT 1`, ... (`untenanted`) | 40.2% |
| a row looked up by a unique column or foreign key (`cache`, `lookup`) | 28.0% |
| the statement names the account (`explicit`) | 21.5% |
| tenant data with no account or row filter (`scan`) | 6.2% |
| tenant data, tenant not determined (`unknown`) | 3.7% |
| a row that doesn't exist (`nomatch`) | 0.5% |

So **10.0% would be flagged** by strict mode (6.2% scan + 3.7% unknown). What they are:

| Flagged statements | Count | Verdict |
|---|---:|---|
| `users JOIN access_tokens WHERE token = $1` | 152 | Authentication: the tenant is what this query *discovers*. Inherently unattributable. Needs an exempt role or a connection tag. |
| `account_linked_accounts WHERE linked_account_id = $1` | 129 | A lookup by the *other* account's id: cross-account by design. |
| `search_entries` / `active_storage_attachments` by `record_type` + `record_id` | 128 | Rails polymorphic associations. A limitation of tenantdb: it could resolve these (the type names the parent table) but doesn't yet. |
| `encrypted_configs` / `account_configs WHERE key = $1` | 65 | Settings read by key with no account filter. These are the kind of finding an audit exists for; whether they are a bug depends on the app (I did not read DocuSeal's source). |
| `template_versions WHERE template_id = $1` | 16 | A foreign key tenantdb couldn't verify because the table was empty. Resolves once there is data. |
| bootstrap checks (`SELECT 1 FROM users LIMIT 1`) | 3 | The app asking whether anyone has registered. |

Offline, `tenantdb analyze` on the Postgres log of the same traffic gives the
same answer: 4,946 statements, the same counts for every source, 493 flagged.

## What it said about running in `block` mode

With `-strict=block` DocuSeal stops working at the first page: the login flow's
"is anyone registered yet" check (`SELECT 1 FROM users LIMIT 1`) is refused, and
then so is the rest. That is the correct behaviour for a tool that refuses what it
cannot attribute, and it is why the rollout advice is **alert first**: an
unmodified app will have such queries (authentication, bootstrap, global config)
that need an exempt role or a connection tag before blocking is usable.

## What this exercise changed in tenantdb

Real traffic exposed blind spots that synthetic tests never would. Each of these
was a defect found by running this app, and fixed:

- **Any unique column is a key, not only `id`.** The public signing link looks a
  submitter up by `slug`. It was `scan`; it is now attributed (cache or a lookup).
- **Foreign keys resolve child lookups through the parent.** `submitters WHERE
  submission_id = $1` is the standard Rails child-table pattern and was labelled
  `scan` (a read of every tenant), which was wrong. Declared foreign keys, and
  `<thing>_id` columns that name a table, are now used, but only after checking a
  sample of real rows to confirm the child always has its parent's tenant; a wrong
  guess would attribute queries to the wrong tenant, which is worse than none.
- **Joins and aliases.** `SELECT ... FROM templates JOIN submissions ... WHERE
  submissions.id = $1` names its table through a qualifier; that is now read.
- **A lookup is not a scan.** Equality on an identifying column that couldn't be
  resolved is `unknown`; `scan` is kept for tenant data read with no identifying
  filter at all.
- **`OR` no longer hides other tenants.** `tenant_id = 'a' OR amount > 5` used to
  be attributed to `a`. A WHERE clause now confines a statement to a tenant only if
  every alternative does. (Found while reading the results, not by the traffic.)
- **`tenantdb analyze` replays against the database.** With a warm cache and row
  lookups it reproduces the live proxy's numbers instead of leaving every
  statement-by-id `unknown`.

## Caveats: read these before quoting the numbers

- **One app, one account.** DocuSeal's open-source build runs a single account, so
  every `account_id` is the same and *cross-tenant* behaviour (a conflict, a leak
  between two accounts) is untested here. The run shows how well statements can be
  *attributed*, not that isolation holds.
- **Synthetic traffic.** It comes from a script, not from users, and covers the API
  and a slice of the UI. A browser with JavaScript, background jobs and webhooks
  would add more shapes.
- **I did not audit DocuSeal's code.** "Flagged" means tenantdb couldn't tie the
  statement to a tenant, not that the application has a bug.
- **Numbers are for this app and this tool version.** A different app will differ.
