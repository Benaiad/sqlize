mod postprocess;
mod response;

pub use reqwest::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, USER_AGENT};

use crate::catalog::Catalog;
use crate::catalog::types::{ColumnName, ResultSet, Row};
use crate::error::{Error, Result};
use crate::sql::plan::{ApiCall, PlanSource, QueryPlan};

/// Max rows when LIMIT is specified — prevents runaway pagination.
const MAX_ROWS: usize = 10_000;

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
    catalog: &Catalog,
) -> Result<ResultSet> {
    // Only paginate when LIMIT is explicit. OFFSET alone uses the first page.
    let fetch_limit = plan.post.limit.map(|l| {
        let offset = plan.post.offset.unwrap_or(0);
        l + offset
    });
    let mut result = execute_source(&plan.source, client, auth, catalog, fetch_limit).await?;
    postprocess::apply(&plan.post, &mut result);
    Ok(result)
}

async fn execute_source(
    source: &PlanSource,
    client: &Client,
    auth: &AuthConfig,
    catalog: &Catalog,
    limit: Option<u64>,
) -> Result<ResultSet> {
    match source {
        PlanSource::ApiCall(call) => execute_paginated(call, client, auth, catalog, limit).await,
        PlanSource::Join { .. } => {
            Err(Error::UnsupportedSql("JOINs not yet implemented in execution engine".to_owned()))
        }
    }
}

/// Execute an API call with automatic pagination.
///
/// Follows pages until one of:
/// - We have enough rows to satisfy LIMIT
/// - The API signals no more pages
/// - We hit the hard cap (MAX_ROWS)
///
/// Pagination is detected from:
/// 1. `Link` header with `rel="next"` (RFC 8288 — GitHub, GitLab, most REST APIs)
/// 2. Response body URL fields: `next`, `next_url`, `next_page` (Django, others)
async fn execute_paginated(
    call: &ApiCall,
    client: &Client,
    auth: &AuthConfig,
    catalog: &Catalog,
    limit: Option<u64>,
) -> Result<ResultSet> {
    let string_params: std::collections::HashMap<String, String> = call
        .path_params
        .iter()
        .map(|(k, v)| (k.as_str().to_owned(), v.clone()))
        .collect();

    let first_url = call
        .endpoint
        .url(&string_params)
        .ok_or(Error::UnresolvedUrl)?;

    let query_params: Vec<(&str, &str)> = call
        .query_params
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    // With explicit LIMIT: paginate up to that many rows (capped at MAX_ROWS).
    // Without LIMIT: fetch only the first page, return all rows from it.
    let rows_needed = limit.map(|l| (l as usize).min(MAX_ROWS));
    let paginate = rows_needed.is_some();

    let mut all_columns: Vec<ColumnName> = Vec::new();
    let mut all_rows: Vec<Row> = Vec::new();
    let mut next_url: Option<String> = Some(first_url.clone());
    let mut is_first_page = true;

    while let Some(url) = next_url.take() {
        let mut request = client
            .get(&url)
            .header(ACCEPT, &call.endpoint.accept)
            .header(USER_AGENT, "sqlize/0.1.0");

        if let Some(token) = &auth.bearer_token {
            request = request.header(AUTHORIZATION, format!("Bearer {token}"));
        }

        // Only add query params on the first request — subsequent pages
        // use the full URL from the Link header or next field.
        if is_first_page {
            request = request.query(&query_params);
        }

        tracing::debug!(%url, page = !is_first_page, "executing API call");

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

        // Extract next page URL from Link header before consuming the response
        let link_next = parse_link_next(resp.headers());

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

        // Extract the data array (handles wrapped responses like Stripe's {"data": [...]})
        let data = match &call.endpoint.data_path {
            Some(field) => body.get(field).unwrap_or(&body),
            None => &body,
        };

        // Look up the table's declared columns from the catalog
        let table = catalog.require(&call.table)?;

        // Build param values map: merge path_params + query_params
        let mut param_values: std::collections::HashMap<ColumnName, String> =
            call.path_params.clone();
        for col in table.pushdown_params() {
            let api_name = match &col.origin {
                crate::catalog::types::ColumnOrigin::QueryParam { api_name } => {
                    api_name.as_deref().unwrap_or(col.name.as_str())
                }
                _ => continue,
            };
            if let Some(val) = call.query_params.get(api_name) {
                param_values.insert(col.name.clone(), val.clone());
            }
        }

        let page = response::json_to_result_set(data, &table.columns, &param_values)?;

        // Merge page into accumulated results
        if all_columns.is_empty() {
            all_columns = page.columns;
        }
        all_rows.extend(page.rows);

        // Without LIMIT: return first page only, no pagination
        if !paginate {
            break;
        }

        // With LIMIT: check if we have enough rows
        if let Some(needed) = rows_needed {
            if all_rows.len() >= needed {
                all_rows.truncate(needed);
                break;
            }
        }

        // Determine next page URL
        // Priority: Link header > response body fields
        next_url = link_next.or_else(|| extract_next_url_from_body(&body));

        is_first_page = false;
    }

    Ok(ResultSet {
        columns: all_columns,
        rows: all_rows,
    })
}

/// Parse `Link` header for `rel="next"` URL (RFC 8288).
///
/// Example: `<https://api.github.com/repos/rust-lang/rust/issues?page=2>; rel="next"`
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
///
/// Covers APIs that put the next page URL in the response body instead of headers
/// (e.g., Django REST Framework uses `"next": "https://..."`).
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
