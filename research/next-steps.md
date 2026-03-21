# SQLize: Next Steps & Open Questions

## Key Design Decisions to Make

### 1. Execution Engine

| Option | Pros | Cons |
|--------|------|------|
| **DuckDB** | Fast, embeddable, great extension system, growing ecosystem | C++, custom extensions needed |
| **SQLite + FDW-like** | Simplest, lightest, universal | No native federation, limited SQL |
| **PostgreSQL FDW** | Proven (Steampipe model), rich SQL | Heavy, requires PG server |
| **Apache Calcite** | Industrial SQL parser/optimizer, adapter framework | Java, complex |
| **Custom parser** | Full control, minimal dialect | Build from scratch, bugs |
| **sqlparser-rs** (Rust) | Good SQL parser, can target custom backends | Rust learning curve |

**Recommendation**: Start with DuckDB or SQLite for the MVP. The queries are simple enough that you don't need Calcite's optimizer. Move to something more sophisticated if federation becomes complex.

### 2. Language / Runtime

| Option | Pros | Cons |
|--------|------|------|
| **Rust** | Performance, Supabase Wrappers precedent, safety | Slower development |
| **Go** | Steampipe precedent, good for CLI tools | Less expressive |
| **TypeScript** | Fast iteration, MCP SDK ecosystem, npm distribution | Performance |
| **Python** | Fastest prototyping, rich SQL libraries | Performance at scale |

**Recommendation**: TypeScript for MVP (MCP ecosystem is TypeScript-native, fastest to ship). Rust for production if it takes off.

### 3. SQL Dialect

Options:
- **Full PostgreSQL** — maximum compatibility but complex
- **DuckDB SQL** — modern, good extensions, JOINs work well
- **SQLite** — simple, universal, but limited
- **Custom DSL** (GAQL-like) — full control, agent-optimized, but non-standard
- **Subset of ANSI SQL** — familiar, constrained

**Recommendation**: Start with a SQLite-compatible subset. SELECT/FROM/WHERE/ORDER BY/LIMIT/GROUP BY + simple JOINs. Expand based on real usage.

### 4. Distribution Model

Options:
- **MCP server** (primary) — direct agent integration
- **HTTP proxy** — language-agnostic, deployable
- **Embeddable library** — for building into other tools
- **CLI tool** — for testing and development

**Recommendation**: MCP server first (that's where the pain is), HTTP proxy second.

## MVP Scope

### Phase 1: Proof of Concept
- Take a single OpenAPI spec (e.g., GitHub API)
- Auto-generate virtual table definitions
- Support SELECT with WHERE clause pushdown
- Expose as MCP server with 3 tools: `get_schema`, `query`, `explain`
- Measure token savings vs GitHub MCP server's 93 tools

### Phase 2: Validation
- Add 2–3 more APIs (Stripe, Slack, Linear)
- Support JOINs across APIs
- Add semantic views (e.g., `active_subscriptions`, `recent_messages`)
- Add procedure support for common mutations
- Benchmark against equivalent MCP tool setups

### Phase 3: Generalization
- Generic OpenAPI → virtual table generator
- Plugin system for custom API adapters
- Schema search and discovery tools
- Production hardening (rate limiting, caching, auth)

## Open Questions

1. **How to handle API authentication?** Each API has different auth (OAuth, API keys, bearer tokens). The SQL layer needs to manage credentials without exposing them in queries.

2. **How to handle pagination transparently?** SQL expects complete result sets. APIs paginate. Auto-pagination can be expensive. Need sensible defaults and limits.

3. **How to handle API rate limits?** A JOIN across two APIs might hit rate limits on one side. Need backpressure and intelligent scheduling.

4. **How to handle schema evolution?** APIs change. Virtual table definitions need to stay in sync. Auto-regeneration from OpenAPI specs helps but breaking changes need handling.

5. **How to handle nested/complex API responses?** REST APIs often return deeply nested JSON. Flattening to relational tables loses information. Need a strategy for nested data (child tables? JSON columns? both?).

6. **What's the right level of SQL restriction?** Too restricted = agents can't express what they need. Too open = security risks and complex query planning.

7. **Should there be a caching layer?** APIs are slow compared to databases. Caching can help but staleness is a concern. Time-based expiry? Explicit refresh?

8. **How to handle APIs that don't have OpenAPI specs?** Many APIs only have docs. Need manual schema definition or spec generation from docs.

## Validation Experiments to Run

1. **Token measurement**: Take GitHub MCP server (93 tools, 55K tokens). Build equivalent SQL schema. Measure schema DDL token cost. Calculate savings.

2. **Accuracy test**: Give Claude/GPT a virtual table schema for GitHub API. Ask it to write 20 queries of increasing complexity. Measure accuracy.

3. **End-to-end comparison**: Same 10 agent tasks, one with MCP tools, one with SQL endpoint. Measure tokens, time, success rate, cost.

4. **Schema generation**: Take 5 OpenAPI specs of varying complexity. Auto-generate virtual table DDL. Assess quality and completeness.
