mod arrow_convert;
mod exec;
mod provider;
mod schema;

use std::sync::Arc;

use datafusion::prelude::*;

use crate::catalog::Catalog;
use crate::catalog::types::ResultSet;
use crate::error::{Error, Result};
use crate::exec::AuthConfig;

use self::arrow_convert::batches_to_result_set;
use self::schema::ApiSchemaProvider;

/// The main entry point for executing SQL against REST APIs via DataFusion.
///
/// Wraps a DataFusion `SessionContext` with registered API table providers.
/// Supports multiple specs (schemas) for federated queries.
pub struct SqlizeContext {
    ctx: SessionContext,
}

impl Default for SqlizeContext {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlizeContext {
    pub fn new() -> Self {
        let config = SessionConfig::new()
            .with_information_schema(false)
            .with_default_catalog_and_schema("sqlize", "default");
        let ctx = SessionContext::new_with_config(config);
        Self { ctx }
    }

    /// Register a spec's tables under the given schema name.
    /// If `is_default` is true, sets this schema as the default so bare table
    /// names resolve to it.
    pub fn register_spec(
        &self,
        schema_name: &str,
        catalog: &Catalog,
        auth: AuthConfig,
        client: reqwest::Client,
    ) -> Result<()> {
        let schema_provider = Arc::new(ApiSchemaProvider::new(catalog, auth, client));

        let df_catalog = self
            .ctx
            .catalog("sqlize")
            .ok_or_else(|| Error::UnsupportedSql("internal: missing default catalog".into()))?;

        df_catalog
            .register_schema(schema_name, schema_provider)
            .map_err(|e| Error::UnsupportedSql(format!("failed to register schema: {e}")))?;

        Ok(())
    }

    /// Execute a SQL query and return a `ResultSet`.
    pub async fn query(&self, sql: &str) -> Result<ResultSet> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| Error::UnsupportedSql(e.to_string()))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| Error::UnsupportedSql(e.to_string()))?;

        Ok(batches_to_result_set(&batches))
    }

    /// Get DataFusion's EXPLAIN output for a query.
    pub async fn explain(&self, sql: &str) -> Result<String> {
        let df = self
            .ctx
            .sql(&format!("EXPLAIN {sql}"))
            .await
            .map_err(|e| Error::UnsupportedSql(e.to_string()))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| Error::UnsupportedSql(e.to_string()))?;

        let result = batches_to_result_set(&batches);
        let mut out = String::new();
        for row in &result.rows {
            for val in row.values() {
                out.push_str(&val.to_string());
                out.push('\n');
            }
        }
        Ok(out)
    }
}
