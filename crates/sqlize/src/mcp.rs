use std::sync::Arc;

use rmcp::ServerHandler;
use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{Implementation, ServerCapabilities, ServerInfo};
use rmcp::schemars;
use rmcp::tool;

use sqlize_core::catalog::Catalog;
use sqlize_core::catalog::ddl::single_table_ddl;
use sqlize_core::catalog::types::TableName;
use sqlize_core::exec::{AuthConfig, execute};
use sqlize_core::output::{result_set_to_json, result_set_to_toon};
use sqlize_core::sql::planner::{explain, plan_query};

pub struct SqlizeServer {
    catalog: Arc<Catalog>,
    auth: AuthConfig,
    instructions: String,
    tool_router: ToolRouter<Self>,
}

impl SqlizeServer {
    pub fn new(catalog: Arc<Catalog>, auth: AuthConfig, api_title: &str) -> Self {
        let table_names: Vec<&str> = catalog.tables().map(|t| t.name.as_str()).collect();
        let instructions = format!(
            "SQLize: Query the {api_title} using SQL.\n\
             Use get_schema to discover tables, then query to execute SQL.\n\
             Results are returned in TOON format (compact, token-efficient).\n\n\
             Available tables: {}",
            table_names.join(", "),
        );

        Self {
            catalog,
            auth,
            instructions,
            tool_router: Self::tool_router(),
        }
    }
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct GetSchemaArgs {
    /// Optional table name to get schema for. If omitted, returns all tables.
    table: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct QueryArgs {
    /// The SQL query to execute (read-only SELECT statements only).
    sql: String,
    /// Maximum number of rows to return. Defaults to 100.
    #[allow(dead_code)] // will be wired to LIMIT injection
    max_rows: Option<u64>,
    /// Output format: "toon" (compact, token-efficient) or "json". Defaults to "toon".
    format: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct ExplainArgs {
    /// The SQL query to explain (shows the execution plan without running it).
    sql: String,
}

#[rmcp::tool_router]
impl SqlizeServer {
    /// Get the SQL schema for available tables. Use this to discover what
    /// tables and columns are available before writing queries.
    /// Returns CREATE TABLE DDL with column types and descriptions.
    #[tool(name = "get_schema")]
    async fn get_schema(&self, Parameters(args): Parameters<GetSchemaArgs>) -> String {
        match &args.table {
            Some(name) => {
                let Ok(table_name) = TableName::new(name) else {
                    return format!("Error: invalid table name '{name}'");
                };
                match self.catalog.get(&table_name) {
                    Some(table) => single_table_ddl(table),
                    None => {
                        let available: Vec<&str> = self
                            .catalog
                            .tables()
                            .map(|t| t.name.as_str())
                            .collect();
                        format!(
                            "Table '{name}' not found. Available tables:\n{}",
                            available.join(", ")
                        )
                    }
                }
            }
            None => {
                let mut out = String::from("Available tables (use get_schema with a table name for full DDL):\n\n");
                for table in self.catalog.tables() {
                    let required: Vec<_> = table.required_params().map(|c| c.name.as_str()).collect();
                    let req = if required.is_empty() {
                        String::new()
                    } else {
                        format!("  required: {}", required.join(", "))
                    };
                    out.push_str(&format!(
                        "  {:<30} -- {}{}\n",
                        table.name,
                        truncate_desc(&table.description, 60),
                        req,
                    ));
                }
                out
            }
        }
    }

    /// Execute a read-only SQL query against virtual API tables.
    /// Returns results in TOON format (compact, token-efficient) by default.
    ///
    /// Example: SELECT number, title FROM repos_issues WHERE owner = 'rust-lang' AND repo = 'rust' AND state = 'open' LIMIT 5
    #[tool(name = "query")]
    async fn query(&self, Parameters(args): Parameters<QueryArgs>) -> String {
        let plan = match plan_query(&args.sql, &self.catalog) {
            Ok(p) => p,
            Err(e) => return format!("Planning error: {e}"),
        };

        let result = match execute(&plan, &self.auth).await {
            Ok(r) => r,
            Err(e) => return format!("Execution error: {e}"),
        };

        let row_count = result.rows.len();

        let use_json = args
            .format
            .as_deref()
            .is_some_and(|f| f.eq_ignore_ascii_case("json"));

        let output = if use_json {
            result_set_to_json(&result)
        } else {
            result_set_to_toon(&result).unwrap_or_else(|_| result_set_to_json(&result))
        };

        format!("{output}\n({row_count} rows)")
    }

    /// Show the execution plan for a SQL query without executing it.
    /// Shows which API calls would be made, what parameters would be pushed down,
    /// and what post-processing would be applied locally.
    #[tool(name = "explain")]
    async fn explain_query(&self, Parameters(args): Parameters<ExplainArgs>) -> String {
        match plan_query(&args.sql, &self.catalog) {
            Ok(plan) => explain(&plan),
            Err(e) => format!("Error: {e}"),
        }
    }
}

fn truncate_desc(s: &str, max: usize) -> String {
    let first_line = s.lines().next().unwrap_or(s);
    if first_line.len() <= max {
        first_line.to_owned()
    } else {
        format!("{}...", &first_line[..max - 3])
    }
}

#[rmcp::tool_handler]
impl ServerHandler for SqlizeServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .build(),
        )
        .with_server_info(
            Implementation::new("sqlize", env!("CARGO_PKG_VERSION")),
        )
        .with_instructions(&self.instructions)
    }
}
