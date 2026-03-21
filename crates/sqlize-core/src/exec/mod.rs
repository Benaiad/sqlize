mod postprocess;
mod response;

use reqwest::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, USER_AGENT};

use crate::catalog::types::ResultSet;
use crate::error::{Error, Result};
use crate::sql::plan::{PlanSource, QueryPlan};

/// Configuration for API authentication.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Bearer token for API authentication.
    pub bearer_token: Option<String>,
}

/// Execute a query plan against live APIs.
pub async fn execute(plan: &QueryPlan, auth: &AuthConfig) -> Result<ResultSet> {
    let client = Client::new();
    let mut result = execute_source(&plan.source, &client, auth).await?;
    postprocess::apply(&plan.post, &mut result);
    Ok(result)
}

async fn execute_source(
    source: &PlanSource,
    client: &Client,
    auth: &AuthConfig,
) -> Result<ResultSet> {
    match source {
        PlanSource::ApiCall(call) => {
            let url = call
                .endpoint
                .url(&call.path_params)
                .ok_or_else(|| Error::Spec("failed to resolve URL from path params".to_owned()))?;

            let mut request = client
                .get(&url)
                .header(ACCEPT, "application/vnd.github+json")
                .header(USER_AGENT, "sqlize/0.1.0");

            if let Some(token) = &auth.bearer_token {
                request = request.header(AUTHORIZATION, format!("Bearer {token}"));
            }

            // Push query params to the API
            let mut query_params: Vec<(&str, &str)> = call
                .query_params
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();

            // Default to max page size for efficiency
            query_params.push(("per_page", "100"));

            request = request.query(&query_params);

            tracing::debug!(%url, ?query_params, "executing API call");

            let resp = request.send().await?;

            // Check rate limiting
            if let Some(remaining) = resp.headers().get("x-ratelimit-remaining") {
                if let Ok(s) = remaining.to_str() {
                    if let Ok(n) = s.parse::<u32>() {
                        if n < 100 {
                            tracing::warn!(remaining = n, "GitHub API rate limit running low");
                        }
                    }
                }
            }

            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(Error::Spec(format!(
                    "API returned {status}: {body}"
                )));
            }

            let body: serde_json::Value = resp.json().await?;
            response::json_to_result_set(&body, &call.table)
        }
        PlanSource::Join { .. } => {
            Err(Error::UnsupportedSql("JOINs not yet implemented in execution engine"))
        }
    }
}
