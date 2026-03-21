# LLM SQL Generation Capabilities: Research Summary

> Research conducted March 2026. Covers benchmarks, real-world quality, production usage,
> and viability of "SQL as an agent interface" pattern.

---

## 1. Text-to-SQL Benchmark Results

### Spider 1.0 (Standard Benchmark — Largely Solved)

| Model / System | Execution Accuracy |
|---|---|
| GPT-4 (specialized, e.g. DIN-SQL) | ~91.2% |
| GPT-4o (baseline) | ~86.6% |
| DAIL-SQL (GPT-4) | ~85-86% |
| GPT-4 + decomposed prompting | ~85.3% |
| Claude 3.5 Sonnet | Competitive with GPT-4o |

**Verdict:** Spider 1.0 is essentially solved. Top systems approach human performance.

### Spider 2.0 (Enterprise-Scale — Released 2024, ICLR 2025 Oral)

Spider 2.0 introduced real-world complexity: enterprise schemas averaging 812 columns
(some >3,000), multiple SQL dialects (BigQuery, Snowflake), multi-step reasoning,
and external documentation requirements.

| Model | Success Rate |
|---|---|
| GPT-4o | ~10.1% |
| o1-preview | ~17.1% |
| o3-mini | ~23.8% (best) |
| Claude 3.5 Sonnet | < o1-preview |

**Verdict:** Enterprise-scale text-to-SQL remains largely unsolved.

### BIRD Benchmark (Real-World Complexity)

BIRD contains 12,751 text-SQL pairs across 95 large databases (~33.4 GB, 37 domains)
with messy, real-world content. Human performance: **92.96%**.

| Model / System | Execution Accuracy |
|---|---|
| Arctic-Text2SQL-R1-32B (Snowflake, 2025) | **71.83%** (SOTA) |
| Arctic-Text2SQL-R1-14B | 70.04% |
| Distyl (first to cross 70%) | ~70% |
| IBM Granite (July 2024 #1) | ~68% |
| Arctic-Text2SQL-R1-7B | 68.47% |
| GPT-4o | ~52.54% |
| ChatGPT (original BIRD paper) | 40.08% |

**Verdict:** Best systems reach ~72% vs human 93%. Specialized fine-tuned models
significantly outperform general-purpose LLMs.

### WikiSQL

WikiSQL is considered largely obsoleted by Spider/BIRD. Top systems achieved >90%
accuracy years ago. Not a meaningful differentiator for current models.

### Key Benchmark Takeaway

| Schema Type | Best Accuracy | Human Accuracy |
|---|---|---|
| Simple (Spider 1.0) | ~91% | ~95% |
| Moderate (BIRD) | ~72% | ~93% |
| Enterprise (Spider 2.0) | ~24% | N/A |
| Real-world BI (internal evals) | ~51% (GPT-4o) | N/A |

---

## 2. Real-World SQL Generation Quality

### How Well Do Models Handle Complex SQL?

**JOINs** — The #1 failure mode. Models often:
- Invent plausible-looking join keys that don't exist in the schema
- Pattern-match "customers" + "products" and assume direct relationships
- Fail catastrophically on non-standard naming conventions and legacy schemas
- Omit necessary JOINs entirely or use subqueries instead

**Subqueries & CTEs** — Models struggle with:
- Nested subqueries (swapping inner/outer correctly)
- Rewriting queries as CTEs
- Window functions
- Complex multi-step reasoning (performance degrades after 3-4 reasoning steps)

**Aggregation & GROUP BY** — Common errors:
- Applying aggregates to wrong columns or grouping levels
- Missing HAVING clauses
- Incorrect COUNT DISTINCT vs COUNT

### Common Failure Modes (from DataBrain's 50,000+ query evaluation)

1. **Schema hallucination** (#1 failure) — LLM invents tables/columns that don't exist
2. **Join path errors** — Incorrect or missing relationships between tables
3. **Missing filters** — WHERE clauses omitted that should constrain results
4. **Hallucinated column names** — Plausible but non-existent columns
5. **Ambiguous business terms** — "churn", "revenue", "active user" interpreted incorrectly

Syntax errors are actually **rare** in production. The real problem is semantically
incorrect but syntactically valid SQL.

### How Does Providing DDL Schema Affect Quality?

Providing schema context is the **single most impactful lever**:

| Context Provided | Impact |
|---|---|
| No schema | Frequent hallucination of tables/columns |
| Raw DDL (CREATE TABLE) | Baseline — significant improvement |
| DDL + foreign key annotations | Correct JOIN generation |
| DDL + column descriptions | +1-5% accuracy improvement |
| DDL + descriptions + sample values | Further disambiguation |
| DDL + few-shot examples (2-5) | +5-15% improvement |
| Schema linking (relevant subset only) | Reduces noise, improves accuracy |
| Well-documented schemas | +15-20% improvement over undocumented |
| Semantic layer / business definitions | Up to +27% (Tiger Data experiment) |

**Key insight:** A well-defined, well-documented schema with explicit relationships
fundamentally changes the quality equation. The gap between "dump raw DDL" and
"provide rich semantic context" is 15-30% accuracy.

### Dialect-Specific Issues

- LLMs trained primarily on standard SQL; dialect-specific features cause errors
- DuckDB: Snowflake created DuckDB-NSQL specifically to handle DuckDB dialect
- BigQuery, Snowflake: Major source of errors in Spider 2.0
- PostgreSQL vs MySQL vs SQLite: Different function names, type systems,
  JSON handling, window function syntax all cause issues
- SQLite: No native date/time, boolean, JSON types — requires function-based workarounds

---

## 3. SQL Generation in Production

### Companies Using LLM-Generated SQL

| Company | Use Case | Results |
|---|---|---|
| **Snowflake** (Cortex Analyst) | BI query generation | >90% accuracy on real-world BI with semantic model |
| **CBRE** | Real estate data search | 67% reduction in query generation time |
| **Riskspan** | Financial analysis | 3-4 weeks → 3-5 days, 90x cost reduction |
| **CloudQuery** | Cloud infrastructure querying | AI Assistant for NL→SQL on cloud resources |
| **Steampipe/Turbot** | Cloud API querying | MCP servers for AI agent integration (2025) |
| **IBM** | Text-to-SQL system | BIRD benchmark leader (mid-2024) |
| **Databricks** | Data analytics | Text2SQL on Databricks platform |

### Techniques for Improving SQL Generation

**Schema Engineering (most impactful):**
- Provide CREATE TABLE statements with column types and constraints
- Explicitly annotate foreign key relationships
- Add column descriptions / comments
- Include sample values for disambiguation
- Use schema linking to show only relevant tables

**Few-Shot Examples:**
- 2-5 example query-SQL pairs improve accuracy 5-15%
- Dynamic few-shot (retrieve similar examples) outperforms static
- Even 1 example demonstration helps
- Diversity in examples enhances candidate generation

**Architectural Patterns:**
- Self-correction loops (+8-10% accuracy when invoked)
- Chain-of-thought query planning before SQL generation (+5% minimum)
- Data domain abstraction (breaking problem into focused sub-problems)
- Semantic layer / business definitions alongside raw schema
- RAG for retrieving relevant schema + example queries

**Fine-Tuning:**
- Model fine-tuning outperforms few-shot for domain-specific SQL
- Snowflake's Arctic-Text2SQL-R1 (fine-tuned) beats GPT-4o by 20+ points on BIRD
- Lamini claims 95% accuracy with fine-tuning for specific domains

### Safety Measures in Production

1. **Schema validation** — Verify all referenced tables/columns actually exist (catches #1 failure mode)
2. **Join path validation** — Define valid join paths, reject SQL using invalid relationships
3. **Read-only access** — Use read-only database connections/roles
4. **EXPLAIN before execute** — Run EXPLAIN to catch expensive queries
5. **Stored procedures / templates** — Limit to predefined safe query patterns
6. **Human-in-the-loop** — Show generated SQL to user before execution
7. **Datamarts** — Use curated views instead of raw warehouse (removes 90% of schema errors)
8. **Verified Query Repository** — Pre-approved question→SQL pairs for common queries
9. **RBAC integration** — Ensure generated SQL respects access controls
10. **Infrastructure-level guardrails** — Move safety logic out of prompts into code

**Production priority order (from DataBrain):**
- Week 1: Schema validation (catches most errors cheaply)
- Week 2: Execution testing
- Month 2: LLM-as-judge semantic validation

---

## 4. Can an LLM Reliably Generate SQL for Virtual Tables Representing API Resources?

### Why This Case Is Fundamentally Easier

The "SQL as agent interface" pattern for querying virtual tables (like Steampipe/Osquery)
is a **significantly easier** problem than general text-to-SQL for several reasons:

**1. Well-Defined, Simple Schemas**
- Virtual table schemas are designed to be queried, with clear column names
- Typically 5-30 columns per table, not 800+
- No legacy naming conventions or abbreviations
- Column names directly correspond to API resource properties
- Foreign key relationships are explicit and documented

**2. The Agent is NOT Converting Natural Language**
- General text-to-SQL: ambiguous human question → SQL (hard)
- Agent pattern: structured task/goal → SQL (much easier)
- The LLM has programmatic context about what it needs, not vague user questions
- No ambiguous business terms to resolve

**3. Schema Fits Entirely in Context**
- Virtual table DDL for a few tables fits in thousands of tokens
- No need for schema linking or retrieval
- Model sees complete, accurate schema with descriptions

**4. Self-Correction Is Built In**
- Execute query → get error → regenerate (the standard agent loop)
- DuckDB and SQLite return helpful error messages with suggested column names
- Multiple attempts dramatically improve success rate

**5. Queries Are Structurally Simple**
- Mostly SELECT with WHERE filters on known columns
- JOINs are rare or follow explicit documented paths
- No complex business logic, window functions, or nested CTEs needed
- Think: `SELECT * FROM github_issues WHERE repo = 'x' AND state = 'open'`

### Real-World Implementations of This Pattern

| System | How It Works |
|---|---|
| **Steampipe** | 2000+ virtual tables mapping cloud APIs to SQL. PostgreSQL FDW or SQLite extensions translate queries into live API calls. |
| **Osquery** | OS resources exposed as SQLite virtual tables. SQL queries over processes, network connections, etc. |
| **CloudQuery AI Assistant** | NL→SQL on top of synced cloud resource tables |
| **Steampipe MCP Servers** | AI agent integration via MCP protocol (shipped 2025) |
| **DuckDB + LangChain agents** | SQL agent with DuckDB backend, self-correcting loop |
| **Flink SQL + ML_PREDICT()** | Streaming SQL with inline LLM calls for agentic workloads |

### Expected Accuracy for This Pattern

Given the research, for well-defined virtual table schemas with:
- Complete DDL provided in prompt
- Column descriptions included
- 2-3 few-shot examples per table
- Self-correction loop (retry on error)

**Expected accuracy: 90-95%+ for the query types needed**

This estimate is based on:
- Spider 1.0 (simple schemas): 86-91% even without domain-specific tuning
- Snowflake Cortex Analyst (with semantic model): >90% on real BI
- The queries needed are structurally simpler than benchmark queries
- Self-correction loop adds another 8-10% recovery
- Providing complete, well-documented schema is the #1 accuracy lever

### Remaining Risks

1. **Dialect-specific syntax** — If using DuckDB/SQLite, some syntax differences from standard SQL
2. **Qualifier pushdown semantics** — Virtual tables may require certain WHERE clauses
   (e.g., Steampipe: "some tables require a WHERE or JOIN clause")
3. **JSON/nested data handling** — API resources often have nested fields requiring
   JSON functions, which LLMs handle less reliably
4. **Pagination/limits** — Need to ensure LLM adds appropriate LIMIT clauses
5. **Type coercion** — API data types may not map cleanly to SQL types

### Mitigation Strategies Specific to This Pattern

1. **Include CREATE TABLE with comments** — Document each column and required qualifiers
2. **Provide 2-3 example queries per table** — Show the expected query patterns
3. **Validate against schema before execution** — Catch hallucinated columns instantly
4. **Use EXPLAIN/dry-run** — Verify query structure before hitting real APIs
5. **Constrain query complexity** — Instruct the LLM to prefer simple queries
6. **Error-driven retry loop** — Let the LLM self-correct from error messages

---

## Summary: Viability Assessment

**For the specific "SQL as agent interface" pattern with well-defined virtual table schemas,
LLM SQL generation is highly viable today.** The key factors that make this work:

1. The accuracy problems in text-to-SQL are overwhelmingly caused by schema complexity,
   ambiguous natural language, and missing context — none of which apply here.

2. Providing complete DDL with descriptions is the #1 accuracy lever, and this pattern
   provides it by definition.

3. The queries needed are structurally simple (SELECT + WHERE + occasional JOIN),
   which is exactly where LLMs excel (86-91% accuracy even on benchmarks).

4. Self-correction loops (standard in agent architectures) recover most remaining errors.

5. Schema validation (checking tables/columns exist) catches the #1 failure mode
   (hallucination) trivially.

The pattern is not hypothetical — Steampipe (2000+ API tables), Osquery, and CloudQuery
have proven that SQL is an effective interface for API resources. The addition of LLM
agents that generate these queries is a natural and well-supported evolution.

---

## Sources

### Benchmarks
- [Spider 2.0 (ICLR 2025)](https://spider2-sql.github.io/)
- [Spider: Yale Semantic Parsing and Text-to-SQL Challenge](https://yale-lily.github.io/spider)
- [BIRD Benchmark](https://bird-bench.github.io/)
- [Pushing Towards Human-Level Text-to-SQL: Analysis of Top Systems on BIRD](https://medium.com/@adnanmasood/pushing-towards-human-level-text-to-sql-an-analysis-of-top-systems-on-bird-benchmark-666efd211a2d)
- [Snowflake Arctic-Text2SQL-R1 Tops BIRD](https://www.snowflake.com/en/engineering-blog/arctic-text2sql-r1-sql-generation-benchmark/)
- [Distyl Takes #1 Spot on BIRD](https://distylai.substack.com/p/distyl-takes-1-spot-on-bird-benchmark)
- [IBM Granite Text-to-SQL](https://research.ibm.com/blog/granite-LLM-text-to-SQL)

### Quality & Failure Modes
- [We Evaluated 50,000+ LLM-Generated SQL Queries. Here's What Actually Breaks (DataBrain)](https://www.usedatabrain.com/blog/llm-sql-evaluation)
- [Text-to-SQL: Comparison of LLM Accuracy in 2026 (AIMultiple)](https://research.aimultiple.com/text-to-sql/)
- [Why 90% Accuracy in Text-to-SQL is 100% Useless (TDS)](https://towardsdatascience.com/why-90-accuracy-in-text-to-sql-is-100-useless/)
- [State of Text2SQL 2024](https://blog.premai.io/state-of-text2sql-2024/)
- [The Death of Schema Linking? Text-to-SQL in the Age of Well-Reasoned Language Models](https://arxiv.org/html/2408.07702v2)

### Production & Techniques
- [Enterprise-grade NL-to-SQL using LLMs (AWS)](https://aws.amazon.com/blogs/machine-learning/enterprise-grade-natural-language-to-sql-generation-using-llms-balancing-accuracy-latency-and-scale/)
- [Snowflake Cortex Analyst: Evaluating Text-to-SQL Accuracy for Real-World BI](https://www.snowflake.com/en/engineering-blog/cortex-analyst-text-to-sql-accuracy-bi/)
- [Agentic Semantic Model Improvement (Snowflake)](https://www.snowflake.com/en/engineering-blog/agentic-semantic-model-text-to-sql/)
- [Techniques for Improving Text-to-SQL (Google Cloud)](https://cloud.google.com/blog/products/databases/techniques-for-improving-text-to-sql)
- [Improving Text2SQL Performance on Databricks](https://www.databricks.com/blog/improving-text2sql-performance-ease-databricks)
- [How to Safely Use LLMs for Text-to-SQL with Stored Procedures](https://erincon01.medium.com/how-to-safely-use-llms-for-text-to-sql-with-stored-procedures-ba7540067f5f)
- [Enhancing Text-to-SQL with Synthetic Summaries (Tiger Data)](https://www.tigerdata.com/blog/enhancing-text-to-sql-with-synthetic-summaries)
- [Text-to-SQL Achieving 95% Accuracy (Lamini)](https://www.lamini.ai/blog/use-case-text-to-sql)

### SQL as Agent Interface
- [LangChain SQL Agent](https://docs.langchain.com/oss/python/langchain/sql-agent)
- [This AI Agent Should Have Been a SQL Query (Gunnar Morling)](https://www.morling.dev/blog/this-ai-agent-should-have-been-sql-query/)
- [DuckDB-NSQL-7B: Text2SQL LLM for DuckDB (MotherDuck)](https://motherduck.com/blog/duckdb-text2sql-llm/)
- [A Lightweight Local SQL Agent Using LLMs and DuckDB](https://www.techrxiv.org/users/930000/articles/1308180-a-lightweight-local-sql-agent-using-llms-and-duckdb-for-business-analytics)
- [LLM SQL Agents: Querying Data in Plain English (K2View)](https://www.k2view.com/blog/sql-agent-llm/)

### Virtual Tables & API Querying
- [Steampipe: select * from cloud](https://steampipe.io/)
- [Steampipe SQLite Extensions](https://steampipe.io/docs/steampipe_sqlite/overview)
- [Steampipe on GitHub](https://github.com/turbot/steampipe)
- [Simplify SQL Queries to AWS API Operations Using Steampipe (AWS Blog)](https://aws.amazon.com/blogs/infrastructure-and-automation/simplify-sql-queries-to-aws-api-operations-using-steampipe-and-aws-plugin/)
- [Steampipe vs CloudQuery](https://www.cloudquery.io/blog/steampipe-vs-cloudquery)
- [Running Steampipe Extensions in sqlite-utils (Simon Willison)](https://til.simonwillison.net/sqlite/steampipe)
