mod postprocess;
mod response;

use std::collections::HashMap;

pub use reqwest::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, USER_AGENT};

use crate::catalog::types::{ApiParamName, Column, ColumnName, ResultSet, Row};
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
pub async fn execute(
    plan: &QueryPlan,
    auth: &AuthConfig,
    client: &Client,
) -> Result<ResultSet> {
    let fetch_limit = plan.post.limit.map(|l| {
        let offset = plan.post.offset.unwrap_or(0);
        l + offset
    });

    let mut result = match &plan.source {
        PlanSource::ApiCall(call) => {
            execute_api_call(call, client, auth, fetch_limit).await?
        }
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
    let param_values = build_param_values(call, &call.columns);

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
        let (body, link_next) = fetch_page(client, auth, call, &url, is_first_page).await?;

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

        next_url = link_next.or_else(|| extract_next_url_from_body(&body));
        is_first_page = false;
    }

    Ok(ResultSet { columns: all_columns, rows: all_rows })
}

fn resolve_url(call: &ApiCall) -> Result<String> {
    call.endpoint
        .url(|name| {
            call.path_params
                .iter()
                .find(|(k, _)| k.as_str() == name)
                .map(|(_, v)| v.as_str())
        })
        .ok_or(Error::UnresolvedUrl)
}

fn build_param_values(
    call: &ApiCall,
    columns: &[Column],
) -> HashMap<ColumnName, String> {
    let mut values: HashMap<ColumnName, String> = call.path_params.clone();
    for col in columns.iter().filter(|c| c.role.is_pushable() && !c.role.is_required()) {
        let param_key = ApiParamName::new(col.api_param_key());
        if let Some(val) = call.query_params.get(&param_key) {
            values.insert(col.name.clone(), val.clone());
        }
    }
    values
}

/// Fetch a single page. Returns the JSON body and the Link-header next URL.
async fn fetch_page(
    client: &Client,
    auth: &AuthConfig,
    call: &ApiCall,
    url: &str,
    is_first_page: bool,
) -> Result<(serde_json::Value, Option<String>)> {
    let mut request = client
        .get(url)
        .header(ACCEPT, &call.endpoint.accept)
        .header(USER_AGENT, "sqlize/0.1.0");

    if let Some(token) = &auth.bearer_token {
        request = request.header(AUTHORIZATION, format!("Bearer {token}"));
    }

    if is_first_page {
        let query_params: Vec<(&str, &str)> = call
            .query_params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        request = request.query(&query_params);
    }

    tracing::debug!(%url, page = !is_first_page, "executing API call");

    let resp = request.send().await?;

    check_rate_limit(&resp);
    let link_next = parse_link_next(resp.headers());

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(Error::ApiError {
            status: status.as_u16(),
            url: url.to_owned(),
            body,
        });
    }

    let body = resp.json().await?;
    Ok((body, link_next))
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

// ---------------------------------------------------------------------------
// Pagination detection
// ---------------------------------------------------------------------------

/// Parse `Link` header for `rel="next"` URL (RFC 8288).
fn parse_link_next(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let link = headers.get("link")?.to_str().ok()?;
    for part in link.split(',') {
        let part = part.trim();
        if part.contains("rel=\"next\"") {
            let url = part
                .split(';')
                .next()?
                .trim()
                .strip_prefix('<')?
                .strip_suffix('>')?;
            return Some(url.to_owned());
        }
    }
    None
}

/// Check response body for common "next page" URL fields.
fn extract_next_url_from_body(body: &serde_json::Value) -> Option<String> {
    let obj = body.as_object()?;
    for key in ["next", "next_url", "next_page", "next_page_url"] {
        if let Some(serde_json::Value::String(url)) = obj.get(key) {
            if url.starts_with("http") {
                return Some(url.clone());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_github_link_header() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "link",
            r#"<https://api.github.com/repos/rust-lang/rust/issues?page=2>; rel="next", <https://api.github.com/repos/rust-lang/rust/issues?page=34>; rel="last""#
                .parse()
                .unwrap(),
        );
        let next = parse_link_next(&headers);
        assert_eq!(
            next.as_deref(),
            Some("https://api.github.com/repos/rust-lang/rust/issues?page=2")
        );
    }

    #[test]
    fn parse_link_header_no_next() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "link",
            r#"<https://example.com?page=1>; rel="prev""#
                .parse()
                .unwrap(),
        );
        assert!(parse_link_next(&headers).is_none());
    }

    #[test]
    fn extract_next_url_from_json() {
        let body = serde_json::json!({
            "results": [{"id": 1}],
            "next": "https://api.example.com/items?page=2",
            "count": 42
        });
        assert_eq!(
            extract_next_url_from_body(&body).as_deref(),
            Some("https://api.example.com/items?page=2")
        );
    }

    #[test]
    fn no_next_url_when_null() {
        let body = serde_json::json!({
            "results": [{"id": 1}],
            "next": null
        });
        assert!(extract_next_url_from_body(&body).is_none());
    }
}
