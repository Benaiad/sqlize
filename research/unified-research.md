# SQLize: Unified Research Report

## The Core Thesis

Replace wide imperative tool surfaces (many MCP tools) with a narrow declarative query surface (SQL). The winning property is not "SQL everywhere" but **a compact, composable, self-describing interface** — and SQL happens to be the best candidate because LLMs are already deeply trained on it.

---

## 1. The Problem: Quantified

### MCP Tool Token Costs

| Component | Tokens |
|-----------|--------|
| Per tool definition | 550–1,400 (avg ~750) |
| GitHub MCP (93 tools) | ~55,000 |
| 3 servers (GitHub + Playwright + IDE) | 143,000 (72% of 200k window) |
| Cloudflare full API via MCP | ~1,170,000 (exceeds all windows) |
| Typical power user (10 servers x 15 tools) | ~75,000 |

### Performance Degradation with Many Tools

| Tools Available | Accuracy |
|----------------|----------|
| 5–10 | >90% |
| 10–20 | Optimal (OpenAI recommends <20) |
| 20–30 | Degraded, hallucinated parameters |
| 50+ | Significant failures |
| 107 | Complete failure (Speakeasy benchmark) |

RAG-MCP paper: accuracy collapsed from **>90% to ~13.6%** with large toolsets.

### Financial Impact

- Enterprise (1,000 devs, 5 sessions/day): **~$4M/year** wasted on tool definitions
- Standard MCP vs optimized: **$2.19M vs $76K/year** (30x difference)

---

## 2. The SQL Advantage

### Why SQL Specifically

1. **LLMs are already fluent.** Spider 1.0 benchmark: 86–91% accuracy on well-defined schemas. Models have seen enormous SQL volumes during training.

2. **Schema IS the documentation.** A `CREATE TABLE` with 5 columns ~50 tokens vs equivalent MCP tool definitions 2,000–5,000+ tokens for CRUD operations.

3. **Composability.** A single SQL query with JOINs replaces 3–5 sequential tool calls, each consuming context and creating decision points.

4. **Self-describing.** `information_schema`, `SHOW TABLES`, `DESCRIBE` let agents discover the contract by querying, not by reading prose docs.

### Token Efficiency: SQL vs MCP Tools

The user's own experience: 7 MCP tools collapsed to 1 SQL endpoint → major token reduction.

Comparable industry results:
- **Vercel**: 16 tools → 2 (bash + SQL) = 37% fewer tokens, 100% success (was 80%)
- **Cloudflare Code Mode**: 2,500+ endpoints → 2 tools = 99.9% token reduction
- **Speakeasy Dynamic Toolsets**: up to 160x token reduction
- **Anthropic Tool Search**: 85% reduction
- **CLI vs MCP**: 32x fewer tokens for equivalent queries (Scalekit benchmark)

### Text-to-SQL Accuracy for This Use Case

General text-to-SQL is hard (Spider 2.0: ~17–24%). But **SQL over virtual API tables** is a much simpler problem:

- Well-defined schemas with clear column names (5–30 columns, not 800+)
- Agent has structured programmatic context, not vague natural language
- Schema fits in context — no retrieval needed
- Queries are structurally simple (SELECT + WHERE + occasional JOIN)
- Self-correction loops add 8–10% recovery

**Expected accuracy: 90–95%+** with complete DDL, column descriptions, few-shot examples, and error-retry.

---

## 3. Existing Landscape

### Direct Predecessors

| Project | What It Does | Status | Gap |
|---------|-------------|--------|-----|
| **Steampipe** | FDW-based SQL over 153 API plugins, 2,000+ tables | Mature (v2.3.6, 7.7K stars). Has MCP server | Read-only. No AI-native design. Plugin dev burden |
| **Supabase Wrappers** | Rust FDWs for Stripe, Firebase, BigQuery etc. | Production. Has write support | Only specific APIs. Generic OpenAPI FDW proposed but not built |
| **Stainless SQL SDK** | OpenAPI → PostgreSQL functions + composite types | Experimental | Not production. Chose functions over FDWs |
| **DuckDB** | In-process OLAP with extensions for external data | Production, growing fast | Not designed as API proxy |
| **Datasette** | SQLite-based data exploration | Mature. AI integration experiments | Database-focused, not API wrapper |

### Adjacent Patterns (Same Thesis, Different Surface)

| Project | Approach | Result |
|---------|----------|--------|
| **GraphQL** | Typed query language with introspection | Good for graph/tree data. Same "compact declarative interface" principle |
| **OData** | Standard queryable REST with $filter, $select, $metadata | Enterprise standard. Same idea for REST |
| **GAQL** (Google Ads) | SQL-like DSL over API with SELECT/FROM/WHERE/LIMIT | Proves constrained SQL-like DSL over APIs works at scale |
| **Cloudflare Code Mode** | 2 tools (search + execute JS) | 99.9% reduction. Uses JS not SQL |
| **Speakeasy Dynamic Toolsets** | 3 meta-tools (search, describe, execute) | 96% reduction. Protocol-level solution |

### Solutions to MCP Bloat (Not SQL-Based)

| Solution | Approach | Reduction |
|----------|----------|-----------|
| Anthropic Tool Search (Feb 2026) | Deferred loading, on-demand tool fetch | 85% |
| Anthropic Code Execution | Write code that orchestrates tools | 98.7% |
| GitHub Copilot | Embedding-based clustering, 40→13 core tools | +2–5% benchmark improvement |
| Hierarchical routing / meta-proxy | 2 meta-tools replace hundreds | 98% |
| MCP Progressive Discovery | Query tool categories, load relevant subset | 98.7% |

---

## 4. The Research Consensus: Recommended Architecture

Both Claude web and ChatGPT converge on the same design. Here's the synthesized recommendation:

### Principle: SQL for Reads, Procedures for Writes

**Reads** (SELECT): Expose API resources as virtual tables/views. This is where SQL shines — composable, compact, well-understood by LLMs.

**Writes** (mutations): Use explicit procedures like `cancel_subscription(id)`, `create_ticket(...)` rather than raw INSERT/UPDATE/DELETE. Reasons:
- PATCH vs PUT doesn't map cleanly to SQL UPDATE
- Intent-based operations ("trigger a build", "approve a PR") need explicit semantics
- Safety: procedure-level auth is easier to enforce than row-level write policies on virtual tables

### Layer Architecture

```
Layer 1: Schema Registry
  - Auto-generate from OpenAPI/Swagger specs
  - API endpoints → virtual tables
  - Parameters/response fields → columns
  - Enums → domains
  - Auth scopes → permissions metadata

Layer 2: Semantic Layer
  - Don't stop at "one table per endpoint"
  - Publish business-facing views: customer_360, open_tickets, recent_code_hotspots
  - Hide complex joins behind curated views
  - This is the most important production step (dbt, Cube, Looker model)

Layer 3: Query Engine
  - Parse SQL, identify virtual tables, plan API calls
  - Push WHERE clauses down to API-native filters
  - Map LIMIT to pagination, ORDER BY to sort params
  - Parallel execution for JOINs across services
  - Bounded local post-processing for what APIs don't support

Layer 4: MCP Surface (2–4 tools)
  1. query(sql, max_rows, readonly=true)
  2. describe(name) or search_schema(term)
  3. call(procedure, args) — for approved mutations
  4. explain(sql) — optional, for debugging
```

### Keep the Dialect Small

Don't promise full ANSI SQL. A constrained subset is better:
- `SELECT`, `FROM`, `WHERE`, `ORDER BY`, `LIMIT`
- `GROUP BY` + aggregations
- Simple JOINs (where semantics are guaranteed)
- Table functions for search/vector lookup
- No subqueries, CTEs, or window functions initially

GAQL (Google Ads Query Language) is a good precedent — SQL-shaped but intentionally constrained.

### Make the Schema Queryable

The whole point is avoiding docs in the prompt:
- `information_schema` style catalog
- `SHOW TABLES`, `DESCRIBE table_name`
- `SEARCH_SCHEMA(term)` for discovery
- Few-shot examples per relation
- Column comments/descriptions in DDL

### API → Table Mapping Rules

| API Pattern | SQL Mapping |
|-------------|-------------|
| Collection/list endpoints | Tables or views |
| Single-resource lookups | Keyed access on tables |
| Nested arrays | Child tables with foreign keys |
| Search/report endpoints | Table functions |
| Actions/mutations | Stored procedures / RPC functions |
| Long-running jobs | Job tables + `CALL start_job(...)` |
| Files/blobs | Metadata rows + separate fetch function |

---

## 5. Security Considerations

### Prompt-to-SQL Injection (P2SQL)

New attack vector: user prompt → LLM → malicious SQL. Traditional WAFs don't catch it because the SQL is generated *after* user input.

Real-world vulnerability found in Anthropic's reference SQLite MCP server (forked 5,000+ times before being archived).

### Mitigations

1. **Read-only by default** — mutations only through explicit procedures
2. **Least-privilege database accounts** — no DDL, no system tables
3. **Parameterized queries** where possible
4. **Query validation** — parse and inspect AST before execution
5. **EXPLAIN before execute** — reject queries with unbounded cost
6. **Row limits** — always enforce max_rows
7. **No raw string interpolation** — the SQL proxy generates the actual API calls

---

## 6. Where NOT to Use This Pattern

- **Imperative workflows**: browser automation, file uploads, payment capture flows
- **Highly stateful operations**: streaming, subscriptions, long-running jobs with strict idempotency
- **Graph-shaped domains**: where GraphQL may be better (though SQL can handle many graph queries)
- **Write-heavy services**: where the majority of operations are mutations, not queries
- **Messy enterprise schemas**: Spider 2.0 shows 17% accuracy on complex real-world schemas — the virtual table approach works because schemas are *designed*, not inherited

---

## 7. The Gap / Opportunity

**No one has built a purpose-designed "SQL virtual database" that:**
1. Auto-generates from OpenAPI specs
2. Supports both reads (SQL) AND writes (procedures)
3. Is optimized for LLM consumption (schema descriptions, few-shot examples, constrained dialect)
4. Includes a semantic layer for business-level views
5. Exposes as a minimal MCP surface (2–4 tools)

The pieces exist (Steampipe proves API→SQL, Supabase proves writable FDWs, Stainless proves OpenAPI→SQL generation, Calcite provides the query engine). Nobody has assembled them into an agent-native product.

**Nobody is marketing "SQL as agent interface" yet.** Steampipe's MCP server is closest but frames it as "infrastructure queries." The framing of "replace your MCP tool catalog with a SQL endpoint" is unclaimed.

---

## 8. Key Sources

### Must-Read

- [Vercel: We Removed 80% of Our Agent's Tools](https://vercel.com/blog/we-removed-80-percent-of-our-agents-tools)
- [Cloudflare: Code Mode — Give Agents an Entire API in 1,000 Tokens](https://blog.cloudflare.com/code-mode-mcp/)
- [Speakeasy: 100x Token Reduction with Dynamic Toolsets](https://www.speakeasy.com/blog/100x-token-reduction-dynamic-toolsets)
- [Anthropic: Advanced Tool Use](https://www.anthropic.com/engineering/advanced-tool-use)
- [Anthropic: Code Execution with MCP](https://www.anthropic.com/engineering/code-execution-with-mcp)
- [Gunnar Morling: This AI Agent Should Have Been a SQL Query](https://www.morling.dev/blog/this-ai-agent-should-have-been-sql-query/)
- [Stainless: SQL SDK Generator from OpenAPI](https://www.stainless.com/blog/introducing-stainless-sql-sdk-generator-from-openapi)

### Benchmarks & Data

- [RAG-MCP Paper (arXiv:2505.03275)](https://arxiv.org/abs/2505.03275) — tool accuracy collapse
- [Spider 2.0](https://spider2-sql.github.io/) — enterprise text-to-SQL benchmark
- [BIRD-bench](https://bird-bench.github.io/) — real-world text-to-SQL
- [The MCP Tax](https://www.mmntm.net/articles/mcp-context-tax) — financial analysis
- [Microsoft Research: Tool-Space Interference](https://www.microsoft.com/en-us/research/blog/tool-space-interference-in-the-mcp-era-designing-for-agent-compatibility-at-scale/)
- [Live API-Bench (arXiv:2506.11266)](https://arxiv.org/html/2506.11266v2) — NL2SQL ↔ tool calling boundary

### Existing Tools to Study

- [Steampipe](https://steampipe.io/) + [steampipe-mcp](https://github.com/turbot/steampipe-mcp)
- [Supabase Wrappers](https://github.com/supabase/wrappers) + [Generic OpenAPI FDW issue #49](https://github.com/supabase/wrappers/issues/49)
- [Apache Calcite](https://calcite.apache.org/)
- [DuckDB Extensions](https://duckdb.org/docs/stable/extensions/overview)
- [Google Ads Query Language (GAQL)](https://developers.google.com/google-ads/api/docs/query/grammar)
- [PostgREST](https://postgrest.org/)
