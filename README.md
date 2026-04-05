# sqlize

An SQL layer for REST API endpoints.

Query GitHub issues, Stripe customers, GitLab pipelines — any REST API that has an OpenAPI spec — using plain SQL. Pagination, auth, filter pushdown, and cross-API JOINs are handled automatically.

![sqlize demo](vhs-demo/demo.gif)

```sql
SELECT number, title, state
FROM issues
WHERE owner = 'rust-lang' AND repo = 'rust' AND state = 'open'
LIMIT 5;
```

```
[5]{number,title,state}:
  154162,"(EXPERIMENT) Replace zero-deps nodes with a singleton",open
  154161,On E0277 tweak help when single type impls traits,open
  154160,Rollup of 6 pull requests,open
  154158,"Audit `//@ run-pass` directives in UI tests",open
  154157,Enforce deterministic signed zero behavior in float min/max and clamp,open
```

## Why SQL

REST APIs are imperative — you need to know the endpoint, the parameters, the pagination scheme, the response shape. SQL is declarative — you say what you want and the engine figures out how to get it.

The mapping is natural: endpoints become tables, parameters become columns, and the query planner translates SQL into API calls. `WHERE owner = 'rust-lang'` becomes a path parameter in the URL. `WHERE state = 'open'` becomes `?state=open` in the query string. `ORDER BY`, `GROUP BY`, `LIMIT` are applied locally by the engine after fetching.

Powered by [Apache DataFusion](https://datafusion.apache.org/). Supports `SELECT`, `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, `GROUP BY`, `HAVING`, `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `JOIN`, subqueries, CTEs, `UNION`/`INTERSECT`, `CASE`, `CAST`, and more. Read-only.

## Quickstart

```sh
# macOS / Linux
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/Benaiad/sqlize/releases/latest/download/sqlize-installer.sh | sh

# Windows
powershell -ExecutionPolicy Bypass -c "irm https://github.com/Benaiad/sqlize/releases/latest/download/sqlize-installer.ps1 | iex"

# Or with Cargo
cargo install sqlize
```

### Query GitHub

```sh
export GITHUB_TOKEN=ghp_...
export SQLIZE_BEARER_ENV_VAR=GITHUB_TOKEN
sqlize --spec specs/github-minimal.json
```

```sql
sqlize> SHOW TABLES;
sqlize> DESCRIBE issues;
sqlize> SELECT number, title FROM issues WHERE owner = 'rust-lang' AND repo = 'rust' LIMIT 5;
```

### Query Stripe

```sh
export STRIPE_API_KEY=sk_test_...
export SQLIZE_BEARER_ENV_VAR=STRIPE_API_KEY
sqlize --spec specs/stripe-minimal.json
```

```sql
sqlize> SELECT email, name FROM customers;
```

### Query GitLab

```sh
export GITLAB_TOKEN=glpat-...
export SQLIZE_BEARER_ENV_VAR=GITLAB_TOKEN
sqlize --spec specs/gitlab-minimal.json
```

```sql
sqlize> SELECT title, state FROM issues WHERE id = '12345' LIMIT 5;
```

Curated minimal specs ship with the repo:

| Spec | Tables | Notes |
|------|--------|-------|
| `specs/github-minimal.json` | 9 | Issues, PRs, commits, releases, repos |
| `specs/gitlab-minimal.json` | 5 | Projects, issues, MRs, pipelines, members |
| `specs/stripe-minimal.json` | 5 | Customers, charges, subscriptions, invoices, products |

Any REST API with an OpenAPI spec works — these are just the ones we've tested.

## Cross-API JOINs

Query multiple APIs in a single session:

```sh
sqlize \
  --spec github:specs/github-minimal.json \
  --spec stripe:specs/stripe-minimal.json
```

```sql
SELECT c.name, c.email, k.commit_message
FROM stripe.customers c
JOIN github.commits k ON c.name = k.author_login
WHERE k.owner = 'openclaw' AND k.repo = 'openclaw'
LIMIT 5;
```

Each spec registers a named schema. Use qualified names (`github.issues`, `stripe.customers`) to query across APIs.

### Per-spec auth

Each spec resolves its token independently:

```sh
export SQLIZE_BEARER_ENV_VAR_GITHUB=GITHUB_TOKEN
export SQLIZE_BEARER_ENV_VAR_STRIPE=STRIPE_API_KEY
```

Falls back to `SQLIZE_BEARER_TOKEN` / `SQLIZE_BEARER_ENV_VAR` when no per-spec var is set.

## MCP server

sqlize runs as an MCP server, giving AI agents SQL access to REST APIs through three tools:

- **`get_schema`** — `CREATE TABLE` DDL for table discovery
- **`query`** — executes SQL, returns results in TOON
- **`explain`** — shows the execution plan without running it

Three tools. Not 25 per service. Adding more APIs adds tables, not tools — the context window cost stays flat.

```sh
claude mcp add \
  --transport stdio \
  --env SQLIZE_SPEC_PATH=/path/to/specs/github-minimal.json \
  --env SQLIZE_BEARER_ENV_VAR=GITHUB_TOKEN \
  --scope user \
  sqlize-github -- sqlize mcp
```

Results are returned in [TOON](https://github.com/toon-format/toon) format by default — 40-50% smaller than JSON, designed for LLM consumption.

## CLI reference

Single-shot commands for scripts and automation:

```sh
sqlize --spec specs/github-minimal.json query "SELECT number, title FROM issues WHERE owner = 'rust-lang' AND repo = 'rust' LIMIT 5"
sqlize --spec specs/github-minimal.json explain "SELECT ..."
sqlize --spec specs/github-minimal.json schema issues
```

Output is JSON by default, `--format toon` for compact output, `--format table` for human-readable tables.

## How queries map to API calls

```sql
EXPLAIN SELECT number, title FROM issues
WHERE owner = 'openclaw' AND repo = 'openclaw' AND state = 'open'
ORDER BY created_at DESC
LIMIT 10;
```

- `owner`, `repo` — path parameters, substituted into the URL
- `state` — query parameter, pushed to the API as `?state=open`
- `ORDER BY`, `LIMIT` — applied locally by DataFusion after the fetch

Path parameters are required — omitting them fails at query planning, before any HTTP call is made.

## Pagination

sqlize paginates automatically using the standard `Link` header (`rel="next"`) or common response body fields (`next`, `next_url`). Works with GitHub, GitLab, Stripe, and most REST APIs without configuration.

Each table scan fetches pages lazily until one of these limits is reached:

- **SQL `LIMIT`** — only the needed pages are fetched
- **`max_rows`** (default 1000) — caps total rows per table scan when no SQL `LIMIT` applies

Override the default:

```sh
sqlize --spec specs/github.json --max-rows 5000
# or
export SQLIZE_MAX_ROWS=5000
```

## Bring your own API

sqlize works with any REST API that has an OpenAPI 3.x spec. For large specs, use `--tags` to filter endpoints:

```sh
curl -L -o specs/github.json \
  https://raw.githubusercontent.com/github/rest-api-description/main/descriptions/api.github.com/api.github.com.json

sqlize --spec specs/github.json --tags repos,issues
```

## Status

Early development — APIs may change.
