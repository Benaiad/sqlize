mod pagination;
mod postprocess;
mod response;

use std::collections::HashMap;

pub use reqwest::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, USER_AGENT};

use crate::catalog::types::{ApiParamName, ColumnName, ResultSet, Row, Scalar};
use crate::error::{Error, Result};
use crate::sql::plan::{ApiCall, PlanSource, QueryPlan};

/// Hard cap on total rows fetched across all pages.
const MAX_ROWS: usize = 10_000;

/// Configuration for API authentication.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub bearer_token: Option<String>,
}

/// Execute a query plan against live APIs.
pub async fn execute(plan: &QueryPlan, auth: &AuthConfig, client: &Client) -> Result<ResultSet> {
    let fetch_limit = plan.post.limit.map(|l| {
        let offset = plan.post.offset.unwrap_or(0);
        l + offset
    });

    let mut result = match &plan.source {
        PlanSource::ApiCall(call) => execute_api_call(call, client, auth, fetch_limit).await?,
    };

    postprocess::apply(&plan.post, &mut result);
    Ok(result)
}

// ---------------------------------------------------------------------------
// API execution with pagination
// ---------------------------------------------------------------------------

async fn execute_api_call(
    call: &ApiCall,
    client: &Client,
    auth: &AuthConfig,
    limit: Option<u64>,
) -> Result<ResultSet> {
    let first_url = resolve_url(call)?;
    let param_values = build_param_values(call);

    // Only paginate when the user explicitly asks for rows via LIMIT.
    // Without LIMIT, return only the first page to avoid hammering APIs.
    let (rows_needed, paginate) = match limit {
        Some(l) => ((l as usize).min(MAX_ROWS), true),
        None => (usize::MAX, false),
    };

    let mut all_columns: Vec<ColumnName> = Vec::new();
    let mut all_rows: Vec<Row> = Vec::new();
    let mut next_url: Option<String> = Some(first_url);
    let mut is_first_page = true;

    while let Some(url) = next_url.take() {
        let (body, headers) = fetch_page(client, auth, call, &url, is_first_page).await?;

        let data = unwrap_response(&body, &call.endpoint.data_path);
        let page = response::json_to_result_set(data, &call.columns, &param_values)?;

        if all_columns.is_empty() {
            all_columns = page.columns;
        }
        all_rows.extend(page.rows);

        if !paginate {
            break;
        }

        if all_rows.len() >= rows_needed {
            all_rows.truncate(rows_needed);
            break;
        }

        let ctx = pagination::PageContext {
            headers: &headers,
            body: &body,
            data,
            current_url: &url,
        };
        next_url = pagination::next_page(&ctx);
        is_first_page = false;
    }

    Ok(ResultSet {
        columns: all_columns,
        rows: all_rows,
    })
}

fn resolve_url(call: &ApiCall) -> Result<String> {
    call.endpoint
        .url(|placeholder| {
            // Find the column whose API name matches this URL placeholder
            call.columns
                .iter()
                .find(|c| c.api_param_key() == placeholder)
                .and_then(|c| {
                    let key = ApiParamName::new(c.api_param_key());
                    match call.params.get(&key)? {
                        Scalar::String(s) => Some(s.as_str()),
                        _ => None,
                    }
                })
        })
        .ok_or(Error::UnresolvedUrl)
}

/// Build a map from column name → string value for response row injection.
/// This lets the response builder inject param values (like `owner`) into
/// result rows for columns that don't appear in the API response body.
fn build_param_values(call: &ApiCall) -> HashMap<ColumnName, String> {
    let mut values = HashMap::new();
    for col in &call.columns {
        if col.role.is_pushable() {
            let key = ApiParamName::new(col.api_param_key());
            if let Some(scalar) = call.params.get(&key) {
                values.insert(col.name.clone(), scalar.to_string());
            }
        }
    }
    values
}

/// Fetch a single page. Returns the JSON body and response headers.
async fn fetch_page(
    client: &Client,
    auth: &AuthConfig,
    call: &ApiCall,
    url: &str,
    is_first_page: bool,
) -> Result<(serde_json::Value, reqwest::header::HeaderMap)> {
    let mut request = client
        .get(url)
        .header(ACCEPT, &call.endpoint.accept)
        .header(USER_AGENT, "sqlize/0.1.0");

    if let Some(token) = &auth.bearer_token {
        request = request.header(AUTHORIZATION, format!("Bearer {token}"));
    }

    if is_first_page {
        // Extract query params (non-path pushable params) and stringify for HTTP
        let query_params: Vec<(String, String)> = call
            .columns
            .iter()
            .filter(|c| c.role.is_pushable() && !c.role.is_required())
            .filter_map(|c| {
                let key = ApiParamName::new(c.api_param_key());
                let val = call.params.get(&key)?;
                Some((key.as_str().to_owned(), val.to_string()))
            })
            .collect();
        let query_refs: Vec<(&str, &str)> = query_params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        request = request.query(&query_refs);
    }

    tracing::debug!(%url, page = !is_first_page, "executing API call");

    let resp = request.send().await?;

    check_rate_limit(&resp);

    let status = resp.status();
    let headers = resp.headers().clone();

    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(Error::ApiError {
            status: status.as_u16(),
            url: url.to_owned(),
            body,
        });
    }

    let body = resp.json().await?;
    Ok((body, headers))
}

fn check_rate_limit(resp: &reqwest::Response) {
    if let Some(remaining) = resp.headers().get("x-ratelimit-remaining") {
        if let Ok(s) = remaining.to_str() {
            if let Ok(n) = s.parse::<u32>() {
                if n < 100 {
                    tracing::warn!(remaining = n, "API rate limit running low");
                }
            }
        }
    }
}

fn unwrap_response<'a>(
    body: &'a serde_json::Value,
    data_path: &Option<String>,
) -> &'a serde_json::Value {
    match data_path {
        Some(field) => body.get(field.as_str()).unwrap_or(body),
        None => body,
    }
}
