mod mcp;
mod repl;

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};

use rmcp::ServiceExt;
use sqlize_core::exec::AuthConfig;

#[derive(Parser)]
#[command(name = "sqlize", about = "SQL interface for REST APIs")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Path to an OpenAPI spec file
    #[arg(short, long, global = true)]
    spec: Option<PathBuf>,

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
}

fn resolve_bearer_token() -> Option<String> {
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

    // MCP mode: log to stderr, read spec from env if not passed via flag
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

    let spec_path = cli
        .spec
        .or_else(|| std::env::var("SQLIZE_SPEC_PATH").ok().map(PathBuf::from))
        .unwrap_or_else(|| {
            eprintln!("Error: --spec <path> or SQLIZE_SPEC_PATH required");
            std::process::exit(1);
        });

    let tag_strs = cli
        .tags
        .as_ref()
        .or_else(|| None) // tags from env handled below
        .map(|tags| tags.iter().map(|s| s.as_str()).collect::<Vec<_>>());

    // Also check SQLIZE_TAGS env var
    let env_tags: Option<Vec<String>> = std::env::var("SQLIZE_TAGS")
        .ok()
        .map(|s| s.split(',').map(|t| t.trim().to_owned()).collect());

    let effective_tags: Option<Vec<&str>> = tag_strs.or_else(|| {
        env_tags
            .as_ref()
            .map(|tags| tags.iter().map(|s| s.as_str()).collect())
    });

    let (catalog, spec_info) =
        sqlize_core::spec::load_catalog(&spec_path, effective_tags.as_deref())
            .map_err(|e| anyhow::anyhow!("{e}"))?;

    let auth = AuthConfig {
        bearer_token: resolve_bearer_token(),
    };

    match cli.command {
        Some(Command::Mcp) => {
            tracing::info!(
                tables = catalog.table_count(),
                api = %spec_info.title,
                base_url = %spec_info.base_url,
                "sqlize MCP server starting"
            );

            let server =
                mcp::SqlizeServer::new(Arc::new(catalog), auth, &spec_info.title);
            let transport = rmcp::transport::io::stdio();

            let service = server
                .serve(transport)
                .await
                .map_err(|e| anyhow::anyhow!("failed to start MCP server: {e}"))?;

            service.waiting().await?;
        }
        None => {
            eprintln!(
                "{} — {} tables from {}",
                spec_info.title,
                catalog.table_count(),
                spec_info.base_url,
            );

            let client = sqlize_core::exec::Client::new();
            repl::run(Arc::new(catalog), auth, client, cli.format).await;
        }
    }

    Ok(())
}
