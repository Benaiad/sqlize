use std::collections::HashMap;

use crate::catalog::types::{ApiEndpoint, ApiParamName, Column, ColumnName, Scalar};

/// The output of the query planner: a recipe for executing a SQL query.
#[derive(Debug)]
pub struct QueryPlan {
    pub(crate) source: PlanSource,
    pub(crate) post: PostProcessing,
}

/// Where rows come from.
#[derive(Debug)]
pub enum PlanSource {
    ApiCall(ApiCall),
}

/// A single API call to execute.
#[derive(Debug)]
pub struct ApiCall {
    pub(crate) endpoint: ApiEndpoint,
    /// Column metadata from the resolved table.
    pub(crate) columns: Vec<Column>,
    /// Parameter values extracted from WHERE clauses. The executor uses each
    /// column's `ColumnRole` to determine whether a param goes into the URL
    /// path or the query string.
    pub(crate) params: HashMap<ApiParamName, Scalar>,
}

/// Operations applied locally after fetching rows from the API.
#[derive(Debug, Default)]
pub struct PostProcessing {
    /// SELECT column list. Empty means SELECT *.
    pub(crate) projections: Vec<Projection>,
    /// WHERE conditions that couldn't be pushed down to the API.
    pub(crate) local_filters: Vec<LocalFilter>,
    /// ORDER BY clauses.
    pub(crate) order_by: Vec<OrderByItem>,
    /// LIMIT value.
    pub(crate) limit: Option<u64>,
    /// OFFSET value.
    pub(crate) offset: Option<u64>,
}

/// A column in the SELECT list.
#[derive(Debug)]
pub enum Projection {
    /// A named column, optionally aliased.
    Column {
        name: ColumnName,
        alias: Option<String>,
    },
    /// SELECT * (all columns).
    Star,
}

/// A filter condition applied locally (not pushed to the API).
#[derive(Debug)]
pub struct LocalFilter {
    pub(crate) column: ColumnName,
    pub(crate) op: FilterOp,
    pub(crate) value: Scalar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Like,
    IsNull,
    IsNotNull,
}

/// An ORDER BY item.
#[derive(Debug)]
pub struct OrderByItem {
    pub(crate) column: ColumnName,
    pub(crate) descending: bool,
}
