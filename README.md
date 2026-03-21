# sqlize

SQL interface for REST APIs. Built for AI agents.

MCP tool definitions burn tens of thousands of tokens before an agent processes a single message. GitHub's MCP server alone consumes ~55,000 tokens across 93 tools. Tool selection accuracy collapses from >90% to ~14% as tools scale.

sqlize replaces that with a single SQL endpoint. One `CREATE TABLE` DDL is more composable and more token-efficient than dozens of tool definitions. Agents already know SQL — there's nothing to teach.

## How it works

Point sqlize at an OpenAPI spec. It generates virtual SQL tables from the API's endpoints — path parameters become required `WHERE` clauses, query parameters become filterable columns, response fields become the rest. Write SQL, get results.

```sql
SELECT number, title, state
FROM issues
WHERE owner = 'rust-lang' AND repo = 'rust' AND state = 'open'
LIMIT 5
```

```
[5]{number,title,state}:
  154162,"(EXPERIMENT) Replace zero-deps nodes with a singleton",open
  154161,On E0277 tweak help when single type impls traits,open
  154160,Rollup of 6 pull requests,open
  154158,"Audit `//@ run-pass` directives in UI tests",open
  154157,Enforce deterministic signed zero behavior in float min/max and clamp,open
```

Results are returned in [TOON](https://github.com/toon-format/toon) — a compact, token-oriented encoding that's 40–50% smaller than JSON.

## Install

```sh
cargo install --path crates/sqlize
```

## Setup

A curated minimal spec for GitHub ships with the repo:

```sh
specs/github-minimal.json   # 9 tables, ~120 columns — ready to use
```

You can also use the full GitHub OpenAPI spec (900+ endpoints, hundreds of columns per table):

```sh
curl -L -o specs/github.json \
  https://raw.githubusercontent.com/github/rest-api-description/main/descriptions/api.github.com/api.github.com.json
```

Set your API token:

```sh
# Option 1: set the token directly
export SQLIZE_BEARER_TOKEN=ghp_...

# Option 2: point to an existing env var (e.g., GITHUB_TOKEN)
export SQLIZE_BEARER_ENV_VAR=GITHUB_TOKEN
```

## CLI

```sh
sqlize --spec specs/github-minimal.json
```

With the full spec, use `--tags` to filter endpoints by their OpenAPI [tag](https://swagger.io/docs/specification/v3_0/grouping-operations-with-tags/):

```sh
sqlize --spec specs/github.json --tags repos,issues
```

```
sqlize> SHOW TABLES
sqlize> DESCRIBE issues
sqlize> SELECT number, title FROM issues WHERE owner = 'rust-lang' AND repo = 'rust' LIMIT 5
```

Output formats: `--format table` (default), `--format toon`, `--format json`.

## MCP server

Three tools:

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

## How queries map to API calls

```sql
SELECT number, title FROM issues
WHERE owner = 'anthropics' AND repo = 'claude-code' AND state = 'open'
ORDER BY created_at DESC
LIMIT 10
```

The planner classifies each `WHERE` condition:

| Condition                  | Classification  | Effect                                                       |
|----------------------------|-----------------|--------------------------------------------------------------|
| `owner = 'anthropics'`     | Path parameter  | Substituted into URL: `/repos/anthropics/{repo}/issues`      |
| `repo = 'claude-code'`     | Path parameter  | Substituted into URL: `/repos/anthropics/claude-code/issues` |
| `state = 'open'`           | Query parameter | Pushed to API: `?state=open`                                 |
| `ORDER BY created_at DESC` | Post-processing | Applied locally after fetch                                  |
| `LIMIT 10`                 | Post-processing | Applied locally after fetch                                  |

Path parameters are required — omitting them fails at query planning, before any HTTP call is made.

## Why

The thesis: replace wide imperative tool surfaces with a narrow declarative query surface. SQL is the right choice because LLMs achieve 86–91% accuracy on well-defined schemas, and the queries here are structurally simple — `SELECT` with `WHERE`, `ORDER BY`, `LIMIT`.

Research and competitive analysis in [`research/`](research/).

## Status

Research prototype. The core pipeline works end-to-end against live APIs. Not production-hardened.

What's missing: JOINs across tables, pagination beyond the first page, write operations (procedures for mutations), caching, and multi-API federation.
