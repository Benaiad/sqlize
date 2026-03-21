mod column_map;
mod table_gen;

use std::path::Path;

use openapiv3::OpenAPI;

use crate::catalog::Catalog;
use crate::error::{Error, Result};

/// Load an OpenAPI spec from a JSON file and build a `Catalog`.
///
/// If `tag_filter` is provided, only endpoints tagged with one of the
/// given tags will be included. This is important for large specs like
/// GitHub's (900+ endpoints) where you only want a subset.
pub fn load_catalog(path: &Path, tag_filter: Option<&[&str]>) -> Result<Catalog> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| Error::Spec(format!("failed to read {}: {e}", path.display())))?;

    let spec: OpenAPI = serde_json::from_str(&content)
        .map_err(|e| Error::Spec(format!("failed to parse OpenAPI spec: {e}")))?;

    let base_url = extract_base_url(&spec);
    let tables = table_gen::tables_from_spec(&spec, &base_url, tag_filter)?;

    Catalog::from_tables(tables)
}

fn extract_base_url(spec: &OpenAPI) -> String {
    spec.servers
        .first()
        .map(|s| s.url.trim_end_matches('/').to_owned())
        .unwrap_or_else(|| "https://api.github.com".to_owned())
}
