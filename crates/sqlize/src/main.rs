mod mcp;
mod repl;

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};

use rmcp::ServiceExt;
use sqlize_core::catalog::Catalog;
use sqlize_core::datafusion::SqlizeContext;
use sqlize_core::exec::AuthConfig;
use sqlize_core::spec::SpecInfo;

#[derive(Parser)]
#[command(name = "sqlize", about = "SQL interface for REST APIs")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// OpenAPI spec: name:path (e.g. github:specs/github.json) or just path for single spec
    #[arg(short, long, global = true)]
    spec: Vec<String>,

    /// Only load endpoints with these tags (comma-separated)
    #[arg(short, long, value_delimiter = ',', global = true)]
    tags: Option<Vec<String>>,

    /// Output format for query results
    #[arg(short, long, default_value = "table", global = true)]
    format: repl::OutputFormat,
}

#[derive(Subcommand)]
enum Command {
    /// Start the MCP server (stdio transport)
    Mcp,
    /// Execute a SQL query and print results
    Query {
        /// The SQL query to execute
        sql: String,
    },
    /// Show the execution plan for a query
    Explain {
        /// The SQL query to explain
        sql: String,
    },
    /// Show table schema (DDL). Omit table name to list all tables.
    Schema {
        /// Table name (optional)
        table: Option<String>,
    },
}

/// A parsed spec reference: name + file path.
struct SpecRef {
    name: String,
    path: PathBuf,
}

/// A loaded spec with its catalog, info, and auth.
struct LoadedSpec {
    name: String,
    catalog: Catalog,
    info: SpecInfo,
}

/// Parse "name:path" or just "path" (name defaults to filename stem).
fn parse_spec_ref(s: &str) -> SpecRef {
    if let Some((name, path)) = s.split_once(':') {
        // Guard against Windows absolute paths like C:\foo
        if name.len() > 1 {
            return SpecRef {
                name: name.to_owned(),
                path: PathBuf::from(path),
            };
        }
    }
    // No name prefix — derive from filename
    let path = PathBuf::from(s);
    let name = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("default")
        .to_owned()
        // Strip common suffixes like "-minimal"
        .replace("-minimal", "")
        .replace("_minimal", "");
    SpecRef { name, path }
}

/// Resolve bearer token for a named spec.
///
/// Lookup order:
/// 1. SQLIZE_BEARER_TOKEN_{NAME}  (e.g. SQLIZE_BEARER_TOKEN_GITHUB)
/// 2. SQLIZE_BEARER_ENV_VAR_{NAME} → dereference
/// 3. SQLIZE_BEARER_TOKEN  (global fallback)
/// 4. SQLIZE_BEARER_ENV_VAR → dereference (global fallback)
fn resolve_bearer_token(spec_name: &str) -> Option<String> {
    let upper = spec_name.to_ascii_uppercase();

    // Per-spec direct token
    if let Ok(token) = std::env::var(format!("SQLIZE_BEARER_TOKEN_{upper}")) {
        return Some(token);
    }

    // Per-spec env var indirection
    if let Ok(var_name) = std::env::var(format!("SQLIZE_BEARER_ENV_VAR_{upper}")) {
        if let Ok(token) = std::env::var(&var_name) {
            return Some(token);
        }
    }

    // Global fallback
    std::env::var("SQLIZE_BEARER_TOKEN").ok().or_else(|| {
        std::env::var("SQLIZE_BEARER_ENV_VAR")
            .ok()
            .and_then(|var_name| std::env::var(&var_name).ok())
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let is_mcp = matches!(cli.command, Some(Command::Mcp));

    if is_mcp {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .init();
    }

    // Collect spec paths from --spec flags and SQLIZE_SPEC_PATH env var
    let mut spec_args = cli.spec;
    if spec_args.is_empty() {
        if let Ok(env_spec) = std::env::var("SQLIZE_SPEC_PATH") {
            spec_args.push(env_spec);
        }
    }
    if spec_args.is_empty() {
        eprintln!("Error: --spec <path> or SQLIZE_SPEC_PATH required");
        std::process::exit(1);
    }

    let tags: Option<Vec<String>> = cli.tags.or_else(|| {
        std::env::var("SQLIZE_TAGS")
            .ok()
            .map(|s| s.split(',').map(|t| t.trim().to_owned()).collect())
    });
    let effective_tags: Option<Vec<&str>> = tags
        .as_ref()
        .map(|v| v.iter().map(|s| s.as_str()).collect());

    // Load all specs
    let mut specs: Vec<LoadedSpec> = Vec::new();
    for arg in &spec_args {
        let spec_ref = parse_spec_ref(arg);
        let (catalog, info) =
            sqlize_core::spec::load_catalog(&spec_ref.path, effective_tags.as_deref())
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        specs.push(LoadedSpec {
            name: spec_ref.name,
            catalog,
            info,
        });
    }

    let client = sqlize_core::exec::Client::new();
    let sqlize_ctx = Arc::new(SqlizeContext::new());
    let is_single_spec = specs.len() == 1;

    // Register each spec as a named schema
    for spec in &specs {
        let auth = AuthConfig {
            bearer_token: resolve_bearer_token(&spec.name),
        };

        let schema_name = if is_single_spec {
            "default"
        } else {
            &spec.name
        };

        sqlize_ctx
            .register_spec(schema_name, &spec.catalog, auth, client.clone())
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    // Build a merged catalog for SHOW TABLES / DESCRIBE
    let all_catalogs: Vec<(&str, &Catalog)> = specs
        .iter()
        .map(|s| {
            let name = if is_single_spec {
                "default"
            } else {
                s.name.as_str()
            };
            (name, &s.catalog)
        })
        .collect();

    match cli.command {
        Some(Command::Mcp) => {
            let spec = &specs[0];
            tracing::info!(
                tables = spec.catalog.table_count(),
                api = %spec.info.title,
                base_url = %spec.info.base_url,
                "sqlize MCP server starting"
            );

            let server = mcp::SqlizeServer::new(
                Arc::new(repl::CatalogSet::new(&all_catalogs)),
                sqlize_ctx,
                &spec.info.title,
            );
            let transport = rmcp::transport::io::stdio();

            let service = server
                .serve(transport)
                .await
                .map_err(|e| anyhow::anyhow!("failed to start MCP server: {e}"))?;

            service.waiting().await?;
        }
        Some(Command::Query { sql }) => {
            let result = sqlize_ctx
                .query(&sql)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            match cli.format {
                repl::OutputFormat::Toon => {
                    println!(
                        "{}",
                        sqlize_core::output::result_set_to_toon(&result)
                            .map_err(|e| anyhow::anyhow!("{e}"))?
                    );
                }
                _ => {
                    println!("{}", sqlize_core::output::result_set_to_json(&result));
                }
            }
        }
        Some(Command::Explain { sql }) => {
            let explain = sqlize_ctx
                .explain(&sql)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            print!("{explain}");
        }
        Some(Command::Schema { table }) => {
            let catalog_set = repl::CatalogSet::new(&all_catalogs);
            match table {
                Some(name) => {
                    if let Some(ddl) = catalog_set.describe(&name) {
                        print!("{ddl}");
                    } else {
                        eprintln!("Table not found: {name}");
                    }
                }
                None => {
                    print!("{}", catalog_set.full_ddl());
                }
            }
        }
        None => {
            // Print banner
            if is_single_spec {
                let spec = &specs[0];
                eprintln!(
                    "{} — {} tables from {}",
                    spec.info.title,
                    spec.catalog.table_count(),
                    spec.info.base_url,
                );
            } else {
                for spec in &specs {
                    eprintln!(
                        "  {} — {} tables from {}",
                        spec.name,
                        spec.catalog.table_count(),
                        spec.info.base_url,
                    );
                }
            }

            let catalog_set = Arc::new(repl::CatalogSet::new(&all_catalogs));
            repl::run(catalog_set, sqlize_ctx, cli.format).await;
        }
    }

    Ok(())
}
