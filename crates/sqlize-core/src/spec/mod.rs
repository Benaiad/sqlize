mod column_map;
mod table_gen;

use std::path::Path;

use openapiv3::OpenAPI;

use crate::catalog::Catalog;
use crate::catalog::types::BaseUrl;
use crate::error::{Error, Result};

/// Metadata extracted from the OpenAPI spec alongside the catalog.
pub struct SpecInfo {
    pub title: String,
    pub base_url: BaseUrl,
}

/// Load an OpenAPI spec from a JSON file and build a `Catalog`.
///
/// If `tag_filter` is provided, only endpoints tagged with one of the
/// given tags will be included. This is important for large specs like
/// GitHub's (900+ endpoints) where you only want a subset.
///
/// Returns a tuple of (Catalog, SpecInfo).
pub fn load_catalog(path: &Path, tag_filter: Option<&[&str]>) -> Result<(Catalog, SpecInfo)> {
    let content = std::fs::read_to_string(path).map_err(|source| Error::SpecRead {
        path: path.to_owned(),
        source,
    })?;

    let spec: OpenAPI = serde_json::from_str(&content).map_err(Error::SpecParse)?;

    let title = spec.info.title.clone();
    let base_url = extract_base_url(&spec)?;
    let tables = table_gen::tables_from_spec(&spec, &base_url, tag_filter)?;

    let info = SpecInfo { title, base_url };
    Ok((Catalog::from_tables(tables)?, info))
}

fn extract_base_url(spec: &OpenAPI) -> Result<BaseUrl> {
    let url_str = spec
        .servers
        .first()
        .map(|s| s.url.as_str())
        .ok_or(Error::NoServers)?;
    BaseUrl::new(url_str)
}
