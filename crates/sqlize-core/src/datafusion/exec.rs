use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use reqwest::header::{ACCEPT, AUTHORIZATION, USER_AGENT};

use crate::catalog::types::{ColumnName, VirtualTable};
use crate::exec::AuthConfig;
use crate::exec::pagination;

use super::arrow_convert::json_response_to_batch;

/// Maximum number of HTTP requests (pages) per table scan.
/// At ~30 rows per page (GitHub default), this yields ~1500 rows.
const MAX_PAGES: usize = 50;

/// A custom DataFusion `ExecutionPlan` that fetches data from a REST API.
/// Returns a lazy stream that fetches one page per `poll_next()`.
#[derive(Debug)]
pub struct ApiTableExec {
    table: VirtualTable,
    full_schema: SchemaRef,
    projected_schema: SchemaRef,
    params: HashMap<String, String>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    auth: AuthConfig,
    client: reqwest::Client,
    properties: PlanProperties,
}

impl ApiTableExec {
    pub fn new(
        table: VirtualTable,
        full_schema: SchemaRef,
        params: HashMap<String, String>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        auth: AuthConfig,
        client: reqwest::Client,
    ) -> Self {
        let projected_schema = match &projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| full_schema.field(i).clone())
                    .collect();
                Arc::new(Schema::new(fields))
            }
            None => full_schema.clone(),
        };

        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            table,
            full_schema,
            projected_schema,
            params,
            projection,
            limit,
            auth,
            client,
            properties,
        }
    }
}

impl DisplayAs for ApiTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ApiTableExec: table={}, endpoint={}{}",
            self.table.name, self.table.endpoint.method, self.table.endpoint.path,
        )
    }
}

impl ExecutionPlan for ApiTableExec {
    fn name(&self) -> &str {
        "ApiTableExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let table = self.table.clone();
        let full_schema = self.full_schema.clone();
        let projected_schema = self.projected_schema.clone();
        let params = self.params.clone();
        let projection = self.projection.clone();
        let limit = self.limit;
        let auth = self.auth.clone();
        let client = self.client.clone();

        let first_url = resolve_url(&table, &params)?;

        let param_values: HashMap<ColumnName, String> = table
            .columns
            .iter()
            .filter(|c| c.role.is_pushable())
            .filter_map(|c| {
                let api_key = c.api_param_key();
                params.get(api_key).map(|v| (c.name.clone(), v.clone()))
            })
            .collect();

        // State for the lazy pagination stream
        struct PageState {
            next_url: Option<String>,
            is_first_page: bool,
            total_rows: usize,
            pages_fetched: usize,
            row_limit: Option<usize>,
            table: VirtualTable,
            full_schema: SchemaRef,
            params: HashMap<String, String>,
            param_values: HashMap<ColumnName, String>,
            projection: Option<Vec<usize>>,
            auth: AuthConfig,
            client: reqwest::Client,
        }

        let initial_state = PageState {
            next_url: Some(first_url),
            is_first_page: true,
            total_rows: 0,
            pages_fetched: 0,
            row_limit: limit,
            table,
            full_schema,
            params,
            param_values,
            projection,
            auth,
            client,
        };

        let stream = futures::stream::unfold(initial_state, |mut state| async move {
            let url = state.next_url.take()?;

            if state.pages_fetched >= MAX_PAGES {
                tracing::warn!(
                    table = %state.table.name,
                    pages = state.pages_fetched,
                    "reached MAX_PAGES limit, stopping pagination"
                );
                return None;
            }

            if let Some(limit) = state.row_limit {
                if state.total_rows >= limit {
                    return None;
                }
            }

            let (body, headers) = match fetch_page(
                &state.client,
                &state.auth,
                &state.table,
                &state.params,
                &url,
                state.is_first_page,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => return Some((Err(e), state)),
            };

            let data = unwrap_response(&body, &state.table.endpoint.data_path);

            let batch = match json_response_to_batch(
                data,
                &state.table.columns,
                &state.param_values,
                &state.full_schema,
            ) {
                Ok(b) => b,
                Err(e) => return Some((Err(e), state)),
            };

            // Apply projection
            let batch = match &state.projection {
                Some(indices) => match batch.project(indices) {
                    Ok(b) => b,
                    Err(e) => return Some((Err(DataFusionError::External(Box::new(e))), state)),
                },
                None => batch,
            };

            state.total_rows += batch.num_rows();
            state.pages_fetched += 1;

            // Determine next page
            let ctx = pagination::PageContext {
                headers: &headers,
                body: &body,
                data,
                current_url: &url,
            };
            state.next_url = pagination::next_page(&ctx);
            state.is_first_page = false;

            Some((Ok(batch), state))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            stream,
        )))
    }
}

fn resolve_url(
    table: &VirtualTable,
    params: &HashMap<String, String>,
) -> Result<String, DataFusionError> {
    table
        .endpoint
        .url(|placeholder| {
            table
                .columns
                .iter()
                .find(|c| c.api_param_key() == placeholder)
                .and_then(|c| {
                    let api_key = c.api_param_key();
                    params.get(api_key).map(|s| s.as_str())
                })
        })
        .ok_or_else(|| {
            DataFusionError::Plan("Failed to resolve URL: missing path parameters".into())
        })
}

async fn fetch_page(
    client: &reqwest::Client,
    auth: &AuthConfig,
    table: &VirtualTable,
    params: &HashMap<String, String>,
    url: &str,
    is_first_page: bool,
) -> Result<(serde_json::Value, reqwest::header::HeaderMap), DataFusionError> {
    let mut request = client
        .get(url)
        .header(ACCEPT, &table.endpoint.accept)
        .header(USER_AGENT, "sqlize/0.1.0");

    if let Some(token) = &auth.bearer_token {
        request = request.header(AUTHORIZATION, format!("Bearer {token}"));
    }

    if is_first_page {
        let query_params: Vec<(String, String)> = table
            .columns
            .iter()
            .filter(|c| c.role.is_pushable() && !c.role.is_required())
            .filter_map(|c| {
                let api_key = c.api_param_key().to_owned();
                params.get(&api_key).map(|v| (api_key, v.clone()))
            })
            .collect();
        let query_refs: Vec<(&str, &str)> = query_params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        request = request.query(&query_refs);
    }

    tracing::debug!(%url, page = !is_first_page, "API call");

    let resp = request
        .send()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let status = resp.status();
    let headers = resp.headers().clone();

    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(DataFusionError::External(Box::new(std::io::Error::other(
            format!("API returned {status}: {body}"),
        ))));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok((body, headers))
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
