use std::path::Path;

use sqlize_core::exec::{AuthConfig, execute};
use sqlize_core::output::{result_set_to_json, result_set_to_toon};
use sqlize_core::spec::load_catalog;
use sqlize_core::sql::planner::{explain, plan_query};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("sqlize=debug")
        .init();

    let spec_path = Path::new("specs/github.json");
    let tags = ["repos", "issues", "pulls"];
    let (catalog, _info) = load_catalog(spec_path, Some(&tags)).expect("failed to load catalog");

    let auth = AuthConfig {
        bearer_token: std::env::var("SQLIZE_BEARER_TOKEN").ok().or_else(|| {
            std::env::var("SQLIZE_BEARER_ENV_VAR")
                .ok()
                .and_then(|var_name| std::env::var(&var_name).ok())
        }),
    };

    let queries = [
        // Query 1: List open issues from a public repo
        "SELECT number, title, state FROM repos_issues WHERE owner = 'rust-lang' AND repo = 'rust' AND state = 'open' LIMIT 5",
        // Query 2: List repos for an org
        "SELECT name, full_name, stargazers_count, language FROM repos WHERE org = 'anthropics' LIMIT 5",
    ];

    for sql in &queries {
        println!("\n{}", "=".repeat(60));
        println!("SQL: {sql}\n");

        let plan = match plan_query(sql, &catalog) {
            Ok(p) => p,
            Err(e) => {
                println!("Planning error: {e}");
                continue;
            }
        };

        println!("EXPLAIN:\n{}", explain(&plan));

        match execute(&plan, &auth).await {
            Ok(result) => {
                let json = result_set_to_json(&result);
                let toon = result_set_to_toon(&result).unwrap_or_else(|e| format!("TOON error: {e}"));

                println!("--- JSON ({} bytes) ---", json.len());
                println!("{json}");
                println!("\n--- TOON ({} bytes, {:.0}% smaller) ---", toon.len(), (1.0 - toon.len() as f64 / json.len() as f64) * 100.0);
                println!("{toon}");
            }
            Err(e) => {
                println!("Execution error: {e}");
            }
        }
    }
}
