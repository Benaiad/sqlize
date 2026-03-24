# sqlize

SQL interface for REST APIs.

Point sqlize at an OpenAPI spec and query any REST API using SQL. Path parameters become `WHERE` clauses, query parameters become filters, response fields become columns. One tool, any API.

![sqlize demo](vhs-demo/demo.gif)

## How it works

```sh
export SQLIZE_BEARER_ENV_VAR=GITHUB_TOKEN
sqlize --spec specs/github-minimal.json --format toon
```

```sql
sqlize> SELECT number, title, state FROM issues
     >  WHERE owner = 'rust-lang' AND repo = 'rust' AND state = 'open'
     >  LIMIT 5;
[5]{number,title,state}:
  154162,"(EXPERIMENT) Replace zero-deps nodes with a singleton",open
  154161,On E0277 tweak help when single type impls traits,open
  154160,Rollup of 6 pull requests,open
  154158,"Audit `//@ run-pass` directives in UI tests",open
  154157,Enforce deterministic signed zero behavior in float min/max and clamp,open
```

Same tool, different API — Stripe:

```sh
export SQLIZE_BEARER_ENV_VAR=STRIPE_TEST_API_KEY
sqlize --spec specs/stripe-minimal.json
```

```sql
sqlize> SELECT email, name FROM customers;
╭──────────────────────┬────────────────╮
│ email                │ name           │
├──────────────────────┼────────────────┤
│ sp@summerproject.com │ Summer Project │
╰──────────────────────┴────────────────╯
```

Powered by [Apache DataFusion](https://datafusion.apache.org/). Supports `SELECT`, `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, `GROUP BY`, `HAVING`, `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `JOIN`, subqueries, CTEs, `UNION`/`INTERSECT`, `CASE`, `CAST`, and more. Read-only — no INSERT/UPDATE/DELETE.

Results are returned in [TOON](https://github.com/toon-format/toon) (compact, token-oriented encoding, 40-50% smaller than JSON), JSON, or as a table.

## Quickstart

```sh
# macOS / Linux
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/Benaiad/sqlize/releases/latest/download/sqlize-installer.sh | sh

# Windows
powershell -ExecutionPolicy Bypass -c "irm https://github.com/Benaiad/sqlize/releases/latest/download/sqlize-installer.ps1 | iex"

# Or with Cargo
cargo install sqlize
```

Curated minimal specs ship with the repo:

| Spec | Tables | Auth | Notes |
|------|--------|------|-------|
| `specs/github-minimal.json` | 9 | Bearer token | Issues, PRs, commits, releases, repos |
| `specs/gitlab-minimal.json` | 5 | Bearer token | Projects, issues, MRs, pipelines, members |
| `specs/stripe-minimal.json` | 5 | Bearer token | Customers, charges, subscriptions, invoices, products |

Set your API token:

```sh
# Option 1: set the token directly
export SQLIZE_BEARER_TOKEN=ghp_...

# Option 2: point to an existing env var (e.g., GITHUB_TOKEN)
export SQLIZE_BEARER_ENV_VAR=GITHUB_TOKEN
```

### CLI

Single-shot commands for scripts and agents:

```sh
sqlize --spec specs/github-minimal.json query "SELECT number, title FROM issues WHERE owner = 'rust-lang' AND repo = 'rust' LIMIT 5"
sqlize --spec specs/github-minimal.json explain "SELECT ..."
sqlize --spec specs/github-minimal.json schema issues
```

Output is JSON by default, `--format toon` for compact output.

### Interactive REPL

```sh
sqlize --spec specs/github-minimal.json
```

```
sqlize> SHOW TABLES
sqlize> DESCRIBE issues
sqlize> SELECT number, title FROM issues WHERE owner = 'rust-lang' AND repo = 'rust' LIMIT 5;
```

Tab completion, SQL syntax highlighting, multiline input, persistent history.

With full OpenAPI specs, use `--tags` to filter endpoints by their OpenAPI [tag](https://swagger.io/docs/specification/v3_0/grouping-operations-with-tags/):

```sh
curl -L -o specs/github.json \
  https://raw.githubusercontent.com/github/rest-api-description/main/descriptions/api.github.com/api.github.com.json

sqlize --spec specs/github.json --tags repos,issues
```

### MCP server

sqlize also runs as an MCP server, giving AI agents SQL access to APIs through three tools:

- **`get_schema`** — returns `CREATE TABLE` DDL for table discovery
- **`query`** — executes read-only SQL, returns TOON
- **`explain`** — shows the execution plan without running it

```sh
claude mcp add \
  --transport stdio \
  --env SQLIZE_SPEC_PATH=/path/to/specs/github-minimal.json \
  --env SQLIZE_BEARER_ENV_VAR=GITHUB_TOKEN \
  --scope user \
  sqlize-github -- sqlize mcp
```

## Multi-spec federation

Query multiple APIs in a single session. Each `--spec` registers a named schema:

```sh
sqlize \
  --spec github:specs/github-minimal.json \
  --spec stripe:specs/stripe-minimal.json
```

Use qualified table names to query across APIs:

```sql
sqlize> SELECT name, stargazers_count FROM github.orgs_repos WHERE org = 'openclaw' LIMIT 5;
sqlize> SELECT email, name FROM stripe.customers LIMIT 5;
```

JOINs across APIs work too:

```sql
SELECT c.name, c.email, k.commit_message
FROM stripe.customers c
JOIN github.commits k ON c.name = k.author_login
WHERE k.owner = 'openclaw' AND k.repo = 'openclaw'
LIMIT 5;
```

### Per-spec auth

Each spec resolves its token independently. Set per-spec env vars using the spec name (uppercased):

```sh
# Per-spec tokens
export SQLIZE_BEARER_TOKEN_GITHUB=ghp_...
export SQLIZE_BEARER_TOKEN_STRIPE=sk_test_...

# Or per-spec env var indirection
export SQLIZE_BEARER_ENV_VAR_GITHUB=GITHUB_TOKEN
export SQLIZE_BEARER_ENV_VAR_STRIPE=STRIPE_TEST_API_KEY
```

Falls back to `SQLIZE_BEARER_TOKEN` / `SQLIZE_BEARER_ENV_VAR` when no per-spec var is set.

### Schema name resolution

The schema name is derived from the `--spec` flag:

| Flag | Schema name |
|------|-------------|
| `--spec github:specs/github.json` | `github` |
| `--spec specs/github-minimal.json` | `github` (auto-derived, `-minimal` stripped) |
| `--spec specs/stripe-minimal.json` | `stripe` |

With a single `--spec`, bare table names work without a schema prefix.

## Aggregations

DataFusion provides full aggregate support:

```sql
SELECT language, COUNT(*) as count
FROM orgs_repos
WHERE org = 'openclaw'
GROUP BY language
ORDER BY count DESC;
```

## How queries map to API calls

```sql
sqlize> EXPLAIN SELECT number, title FROM issues
     >  WHERE owner = 'openclaw' AND repo = 'openclaw' AND state = 'open'
     >  ORDER BY created_at DESC
     >  LIMIT 10;
```

`WHERE` conditions on path parameters (`owner`, `repo`) are substituted into the URL. Query parameters (`state`) are pushed to the API as `?key=value`. Everything else (`ORDER BY`, `LIMIT`, `GROUP BY`, `JOIN`) is applied locally by DataFusion after the fetch.

Path parameters are required — omitting them fails at query planning, before any HTTP call is made.

## Pagination

Queries without `LIMIT` return a single page of results (whatever the API's default page size is). Add `LIMIT` to fetch across multiple pages automatically:

```sql
SELECT number, title FROM issues
WHERE owner = 'rust-lang' AND repo = 'rust'
LIMIT 250;
```

sqlize follows pagination using the standard `Link` header (`rel="next"`) or common response body fields (`next`, `next_url`). This works with GitHub, GitLab, Stripe, and most REST APIs without configuration.

## Why SQL

REST APIs are imperative — you need to know the endpoint, the parameters, the pagination scheme, the response shape. SQL is declarative — you say what you want and the engine figures out how to get it. The mapping is natural: endpoints become tables, parameters become columns, and the query planner translates SQL into API calls.

Research and competitive analysis in [`research/`](research/).

## Status

Research prototype. The core pipeline works end-to-end against live APIs. Not production-hardened.
