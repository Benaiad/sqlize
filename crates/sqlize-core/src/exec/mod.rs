mod postprocess;
mod response;

pub use reqwest::Client;
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
///
/// Accepts a shared `reqwest::Client` for connection reuse across queries.
pub async fn execute(
    plan: &QueryPlan,
    auth: &AuthConfig,
    client: &Client,
) -> Result<ResultSet> {
    let mut result = execute_source(&plan.source, client, auth).await?;
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
            let string_params: std::collections::HashMap<String, String> = call
                .path_params
                .iter()
                .map(|(k, v)| (k.as_str().to_owned(), v.clone()))
                .collect();
            let url = call
                .endpoint
                .url(&string_params)
                .ok_or(Error::UnresolvedUrl)?;

            let mut request = client
                .get(&url)
                .header(ACCEPT, &call.endpoint.accept)
                .header(USER_AGENT, "sqlize/0.1.0");

            if let Some(token) = &auth.bearer_token {
                request = request.header(AUTHORIZATION, format!("Bearer {token}"));
            }

            // Push query params to the API
            let query_params: Vec<(&str, &str)> = call
                .query_params
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();

            request = request.query(&query_params);

            tracing::debug!(%url, ?query_params, "executing API call");

            let resp = request.send().await?;

            // Check rate limiting
            if let Some(remaining) = resp.headers().get("x-ratelimit-remaining") {
                if let Ok(s) = remaining.to_str() {
                    if let Ok(n) = s.parse::<u32>() {
                        if n < 100 {
                            tracing::warn!(remaining = n, "API rate limit running low");
                        }
                    }
                }
            }

            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(Error::ApiError {
                    status: status.as_u16(),
                    url,
                    body,
                });
            }

            let body: serde_json::Value = resp.json().await?;
            response::json_to_result_set(&body, &call.table)
        }
        PlanSource::Join { .. } => {
            Err(Error::UnsupportedSql("JOINs not yet implemented in execution engine".to_owned()))
        }
    }
}
