use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;

use crate::catalog::types::{ColumnRole, VirtualTable};
use crate::exec::AuthConfig;

use super::arrow_convert::virtual_table_to_schema;
use super::exec::ApiTableExec;

/// A DataFusion `TableProvider` backed by a REST API endpoint.
pub struct ApiTableProvider {
    table: VirtualTable,
    schema: SchemaRef,
    auth: AuthConfig,
    client: reqwest::Client,
    max_rows: usize,
}

impl fmt::Debug for ApiTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiTableProvider")
            .field("table", &self.table.name)
            .finish()
    }
}

impl ApiTableProvider {
    pub fn new(
        table: VirtualTable,
        auth: AuthConfig,
        client: reqwest::Client,
        max_rows: usize,
    ) -> Self {
        let schema = virtual_table_to_schema(&table);
        Self {
            table,
            schema,
            auth,
            client,
            max_rows,
        }
    }
}

#[async_trait]
impl TableProvider for ApiTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| classify_filter(&self.table, expr))
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // Extract pushable = filters
        let mut params = HashMap::new();
        for filter in filters {
            if let Some((col_name, value)) = extract_eq_filter(filter) {
                // Check if this column is pushable
                if let Some(col) = self
                    .table
                    .columns
                    .iter()
                    .find(|c| c.name.as_str() == col_name)
                {
                    if col.role.is_pushable() {
                        let api_key = col.api_param_key().to_owned();
                        params.insert(api_key, value);
                    }
                }
            }
        }

        // Validate required path params
        for col in self.table.required_params() {
            let api_key = col.api_param_key();
            if !params.contains_key(api_key) {
                let required: Vec<&str> = self
                    .table
                    .required_params()
                    .map(|c| c.name.as_str())
                    .collect();
                return Err(DataFusionError::Plan(format!(
                    "Missing required WHERE clause: {} for table {}",
                    required.join(" AND "),
                    self.table.name
                )));
            }
        }

        // Use DataFusion's pushed-down limit if available, otherwise use max_rows.
        let effective_limit = limit.unwrap_or(self.max_rows);

        let exec = ApiTableExec::new(
            self.table.clone(),
            self.schema.clone(),
            params,
            projection.cloned(),
            effective_limit,
            self.auth.clone(),
            self.client.clone(),
        );

        Ok(Arc::new(exec))
    }
}

/// Classify whether a filter can be pushed down to the API.
fn classify_filter(table: &VirtualTable, expr: &Expr) -> TableProviderFilterPushDown {
    if let Some((col_name, _)) = extract_eq_filter(expr) {
        if let Some(col) = table.columns.iter().find(|c| c.name.as_str() == col_name) {
            if col.role.is_pushable() {
                return match col.role {
                    ColumnRole::PathParam | ColumnRole::QueryParam => {
                        TableProviderFilterPushDown::Exact
                    }
                    ColumnRole::QueryParamAndResponse => TableProviderFilterPushDown::Inexact,
                    _ => TableProviderFilterPushDown::Unsupported,
                };
            }
        }
    }
    TableProviderFilterPushDown::Unsupported
}

/// Extract column name and string value from a `col = 'value'` expression.
fn extract_eq_filter(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::Eq => {
            match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(col), Expr::Literal(lit)) => {
                    Some((col.name.clone(), scalar_to_string(lit)?))
                }
                (Expr::Literal(lit), Expr::Column(col)) => {
                    Some((col.name.clone(), scalar_to_string(lit)?))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn scalar_to_string(lit: &datafusion::common::ScalarValue) -> Option<String> {
    match lit {
        datafusion::common::ScalarValue::Utf8(Some(s))
        | datafusion::common::ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        datafusion::common::ScalarValue::Int8(Some(n)) => Some(n.to_string()),
        datafusion::common::ScalarValue::Int16(Some(n)) => Some(n.to_string()),
        datafusion::common::ScalarValue::Int32(Some(n)) => Some(n.to_string()),
        datafusion::common::ScalarValue::Int64(Some(n)) => Some(n.to_string()),
        datafusion::common::ScalarValue::Boolean(Some(b)) => Some(b.to_string()),
        _ => None,
    }
}
