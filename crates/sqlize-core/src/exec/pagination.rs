/// Pagination detection from HTTP response headers and body.
///
/// Detects the next page URL by trying strategies in order:
/// 1. Link header with `rel="next"` (RFC 8288 — GitHub, GitLab, most REST APIs)
/// 2. Response body URL field: `next`, `next_url`, `next_page` (Django, others)
/// 3. Cursor-based: `has_more` + last item `id` → `?starting_after=id` (Stripe)
/// 4. Offset-based: `total`/`count` field with page tracking (generic)

/// Everything the pagination detector needs to decide the next page.
pub struct PageContext<'a> {
    pub headers: &'a reqwest::header::HeaderMap,
    /// The full response body (before unwrapping).
    pub body: &'a serde_json::Value,
    /// The data array (after unwrapping, e.g., the `data` field for Stripe).
    pub data: &'a serde_json::Value,
    /// The URL of the current request.
    pub current_url: &'a str,
}

/// Detect the next page URL from the response.
/// Tries each strategy in order, returns the first match.
pub fn next_page(ctx: &PageContext<'_>) -> Option<String> {
    link_header(ctx)
        .or_else(|| body_url_field(ctx))
        .or_else(|| cursor_based(ctx))
}

/// Strategy 1: RFC 8288 Link header.
/// `Link: <https://api.example.com/items?page=2>; rel="next"`
fn link_header(ctx: &PageContext<'_>) -> Option<String> {
    let link = ctx.headers.get("link")?.to_str().ok()?;
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

/// Strategy 2: URL field in response body.
/// Common fields: `next`, `next_url`, `next_page`, `next_page_url`.
fn body_url_field(ctx: &PageContext<'_>) -> Option<String> {
    let obj = ctx.body.as_object()?;
    for key in ["next", "next_url", "next_page", "next_page_url"] {
        if let Some(serde_json::Value::String(url)) = obj.get(key) {
            if url.starts_with("http") {
                return Some(url.clone());
            }
        }
    }
    None
}

/// Strategy 3: Cursor-based pagination.
/// Detects `has_more: true` in the body, takes the last item's `id`,
/// and appends `?starting_after=<id>` to the current URL.
/// Works with Stripe and APIs that follow the same pattern.
fn cursor_based(ctx: &PageContext<'_>) -> Option<String> {
    let obj = ctx.body.as_object()?;

    // Check for has_more / hasMore / has_next_page: true
    let has_more = obj.get("has_more")
        .or_else(|| obj.get("hasMore"))
        .or_else(|| obj.get("has_next_page"))?
        .as_bool()?;

    if !has_more {
        return None;
    }

    // Determine cursor value. Priority:
    // 1. Explicit cursor field in the response body (next_cursor, cursor, end_cursor)
    // 2. Last item's id field (Stripe and most REST APIs)
    let cursor = extract_explicit_cursor(obj)
        .or_else(|| extract_last_item_id(ctx.data))?;

    // Determine the cursor parameter name from the response shape
    let cursor_param = detect_cursor_param(obj);

    // Append cursor to URL
    let separator = if ctx.current_url.contains('?') { "&" } else { "?" };
    Some(format!("{}{separator}{cursor_param}={cursor}", ctx.current_url))
}

/// Look for an explicit cursor/token in the response body.
fn extract_explicit_cursor(obj: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    for key in ["next_cursor", "cursor", "end_cursor", "ending_before",
                "next_page_token", "pageToken", "continuation_token"] {
        if let Some(val) = obj.get(key) {
            match val {
                serde_json::Value::String(s) if !s.is_empty() => return Some(s.clone()),
                serde_json::Value::Number(n) => return Some(n.to_string()),
                _ => {}
            }
        }
    }
    None
}

/// Fall back to the last item's ID in the data array.
fn extract_last_item_id(data: &serde_json::Value) -> Option<String> {
    let items = data.as_array()?;
    let last = items.last()?;
    for key in ["id", "_id", "uuid"] {
        if let Some(val) = last.get(key) {
            match val {
                serde_json::Value::String(s) => return Some(s.clone()),
                serde_json::Value::Number(n) => return Some(n.to_string()),
                _ => {}
            }
        }
    }
    None
}

/// Detect the right query parameter name for the cursor.
fn detect_cursor_param(obj: &serde_json::Map<String, serde_json::Value>) -> &'static str {
    // If the response has an explicit cursor field, match the param name
    if obj.contains_key("next_cursor") || obj.contains_key("cursor") {
        return "cursor";
    }
    if obj.contains_key("next_page_token") || obj.contains_key("pageToken") {
        return "page_token";
    }
    if obj.contains_key("continuation_token") {
        return "continuation_token";
    }
    // Default: Stripe convention
    "starting_after"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_headers() -> reqwest::header::HeaderMap {
        reqwest::header::HeaderMap::new()
    }

    #[test]
    fn link_header_github() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "link",
            r#"<https://api.github.com/repos/rust-lang/rust/issues?page=2>; rel="next", <https://api.github.com/repos/rust-lang/rust/issues?page=34>; rel="last""#
                .parse()
                .unwrap(),
        );
        let body = serde_json::json!([]);
        let ctx = PageContext {
            headers: &headers,
            body: &body,
            data: &body,
            current_url: "https://api.github.com/repos/rust-lang/rust/issues",
        };
        assert_eq!(
            next_page(&ctx).as_deref(),
            Some("https://api.github.com/repos/rust-lang/rust/issues?page=2")
        );
    }

    #[test]
    fn body_url_field_django() {
        let headers = empty_headers();
        let body = serde_json::json!({
            "results": [{"id": 1}],
            "next": "https://api.example.com/items?page=2",
            "count": 42
        });
        let ctx = PageContext {
            headers: &headers,
            body: &body,
            data: &body,
            current_url: "https://api.example.com/items",
        };
        assert_eq!(
            next_page(&ctx).as_deref(),
            Some("https://api.example.com/items?page=2")
        );
    }

    #[test]
    fn cursor_based_stripe() {
        let headers = empty_headers();
        let body = serde_json::json!({
            "data": [
                {"id": "cus_1", "email": "a@example.com"},
                {"id": "cus_2", "email": "b@example.com"},
                {"id": "cus_3", "email": "c@example.com"}
            ],
            "has_more": true,
            "url": "/v1/customers"
        });
        let data = &body["data"];
        let ctx = PageContext {
            headers: &headers,
            body: &body,
            data,
            current_url: "https://api.stripe.com/v1/customers",
        };
        assert_eq!(
            next_page(&ctx).as_deref(),
            Some("https://api.stripe.com/v1/customers?starting_after=cus_3")
        );
    }

    #[test]
    fn cursor_based_no_more() {
        let headers = empty_headers();
        let body = serde_json::json!({
            "data": [{"id": "cus_1"}],
            "has_more": false
        });
        let data = &body["data"];
        let ctx = PageContext {
            headers: &headers,
            body: &body,
            data,
            current_url: "https://api.stripe.com/v1/customers",
        };
        assert!(next_page(&ctx).is_none());
    }

    #[test]
    fn no_next_url_when_null() {
        let headers = empty_headers();
        let body = serde_json::json!({
            "results": [{"id": 1}],
            "next": null
        });
        let ctx = PageContext {
            headers: &headers,
            body: &body,
            data: &body,
            current_url: "https://example.com",
        };
        assert!(next_page(&ctx).is_none());
    }

    #[test]
    fn no_pagination_signals() {
        let headers = empty_headers();
        let body = serde_json::json!([{"id": 1}]);
        let ctx = PageContext {
            headers: &headers,
            body: &body,
            data: &body,
            current_url: "https://example.com",
        };
        assert!(next_page(&ctx).is_none());
    }
}
