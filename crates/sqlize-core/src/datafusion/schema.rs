use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;

use crate::catalog::Catalog;
use crate::catalog::types::VirtualTable;
use crate::exec::AuthConfig;

use super::provider::ApiTableProvider;

/// A DataFusion `SchemaProvider` that exposes all tables from a single API spec.
#[derive(Debug)]
pub struct ApiSchemaProvider {
    tables: HashMap<String, VirtualTable>,
    auth: AuthConfig,
    client: reqwest::Client,
}

impl ApiSchemaProvider {
    pub fn new(catalog: &Catalog, auth: AuthConfig, client: reqwest::Client) -> Self {
        let tables: HashMap<String, VirtualTable> = catalog
            .tables()
            .map(|t| (t.name.as_str().to_owned(), t.clone()))
            .collect();
        Self {
            tables,
            auth,
            client,
        }
    }
}

#[async_trait]
impl SchemaProvider for ApiSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.keys().cloned().collect();
        names.sort();
        names
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        match self.tables.get(name) {
            Some(vt) => {
                let provider =
                    ApiTableProvider::new(vt.clone(), self.auth.clone(), self.client.clone());
                Ok(Some(Arc::new(provider)))
            }
            None => Ok(None),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
