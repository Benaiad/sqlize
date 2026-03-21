use std::path::PathBuf;
use std::sync::Arc;

use rmcp::ServiceExt;
use rmcp::transport::io::stdio;

mod tools;

use tools::SqlizeServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Log to stderr so it doesn't interfere with MCP JSON-RPC on stdout
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env(),
        )
        .with_writer(std::io::stderr)
        .init();

    let spec_path = std::env::var("SQLIZE_SPEC_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("specs/github.json"));

    let tags: Option<Vec<String>> = std::env::var("SQLIZE_TAGS")
        .ok()
        .map(|s| s.split(',').map(|t| t.trim().to_owned()).collect());

    let tag_refs: Option<Vec<&str>> = tags
        .as_ref()
        .map(|v| v.iter().map(|s| s.as_str()).collect());

    let catalog = sqlize_core::spec::load_catalog(&spec_path, tag_refs.as_deref())
        .map_err(|e| anyhow::anyhow!("failed to load spec: {e}"))?;

    tracing::info!(
        tables = catalog.table_count(),
        spec = %spec_path.display(),
        "sqlize MCP server starting"
    );

    let auth = sqlize_core::exec::AuthConfig {
        bearer_token: std::env::var("GITHUB_TOKEN").ok(),
    };

    let server = SqlizeServer::new(Arc::new(catalog), auth);
    let transport = stdio();

    let service = server.serve(transport).await
        .map_err(|e| anyhow::anyhow!("failed to start MCP server: {e}"))?;

    service.waiting().await?;

    Ok(())
}
