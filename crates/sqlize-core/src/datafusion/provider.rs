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
use crate::http::AuthConfig;

use super::arrow_convert::virtual_table_to_schema;
use super::exec::ApiTableExec;

/// A DataFusion `TableProvider` backed by a REST API endpoint.
pub struct ApiTableProvider {
    table: Arc<VirtualTable>,
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
        table: Arc<VirtualTable>,
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
    use datafusion::common::ScalarValue;
    match lit {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        ScalarValue::Int8(Some(n)) => Some(n.to_string()),
        ScalarValue::Int16(Some(n)) => Some(n.to_string()),
        ScalarValue::Int32(Some(n)) => Some(n.to_string()),
        ScalarValue::Int64(Some(n)) => Some(n.to_string()),
        ScalarValue::UInt8(Some(n)) => Some(n.to_string()),
        ScalarValue::UInt16(Some(n)) => Some(n.to_string()),
        ScalarValue::UInt32(Some(n)) => Some(n.to_string()),
        ScalarValue::UInt64(Some(n)) => Some(n.to_string()),
        ScalarValue::Float32(Some(n)) => Some(n.to_string()),
        ScalarValue::Float64(Some(n)) => Some(n.to_string()),
        ScalarValue::Boolean(Some(b)) => Some(b.to_string()),
        other => {
            tracing::warn!(scalar_type = %other.data_type(), "unsupported filter literal type — not pushed down");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::types::{
        AcceptHeader, ApiEndpoint, ApiParamName, BaseUrl, Column, ColumnName, ColumnType,
        HttpMethod, PathTemplate, TableName,
    };
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::prelude::{col, lit};

    fn test_table(columns: Vec<Column>) -> VirtualTable {
        VirtualTable {
            name: TableName::new("test").unwrap(),
            description: None,
            columns,
            endpoint: ApiEndpoint {
                method: HttpMethod::Get,
                path: PathTemplate::new("/test/{id}").unwrap(),
                base_url: BaseUrl::new("https://example.com").unwrap(),
                accept: AcceptHeader::new("application/json"),
                response_wrapper_key: None,
            },
        }
    }

    fn make_col(name: &str, role: ColumnRole) -> Column {
        Column {
            name: ColumnName::new(name).unwrap(),
            col_type: ColumnType::String,
            nullable: role != ColumnRole::PathParam,
            description: None,
            role,
            api_name: Some(ApiParamName::new(name).unwrap()),
        }
    }

    #[test]
    fn classify_path_param_exact() {
        let table = test_table(vec![make_col("id", ColumnRole::PathParam)]);
        let expr = col("id").eq(lit("123"));
        assert_eq!(
            classify_filter(&table, &expr),
            TableProviderFilterPushDown::Exact
        );
    }

    #[test]
    fn classify_query_param_exact() {
        let table = test_table(vec![make_col("sort", ColumnRole::QueryParam)]);
        let expr = col("sort").eq(lit("name"));
        assert_eq!(
            classify_filter(&table, &expr),
            TableProviderFilterPushDown::Exact
        );
    }

    #[test]
    fn classify_query_param_and_response_inexact() {
        let table = test_table(vec![make_col("state", ColumnRole::QueryParamAndResponse)]);
        let expr = col("state").eq(lit("open"));
        assert_eq!(
            classify_filter(&table, &expr),
            TableProviderFilterPushDown::Inexact
        );
    }

    #[test]
    fn classify_response_field_unsupported() {
        let table = test_table(vec![make_col("title", ColumnRole::ResponseField)]);
        let expr = col("title").eq(lit("hello"));
        assert_eq!(
            classify_filter(&table, &expr),
            TableProviderFilterPushDown::Unsupported
        );
    }

    #[test]
    fn extract_eq_filter_string_literal() {
        let expr = col("name").eq(lit("value"));
        let (c, v) = extract_eq_filter(&expr).unwrap();
        assert_eq!(c, "name");
        assert_eq!(v, "value");
    }

    #[test]
    fn extract_eq_filter_integer_literal() {
        let expr = col("id").eq(lit(ScalarValue::Int64(Some(42))));
        let (c, v) = extract_eq_filter(&expr).unwrap();
        assert_eq!(c, "id");
        assert_eq!(v, "42");
    }

    #[test]
    fn extract_non_eq_filter_returns_none() {
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
            left: Box::new(col("id")),
            op: Operator::Gt,
            right: Box::new(lit("5")),
        });
        assert!(extract_eq_filter(&expr).is_none());
    }
}
