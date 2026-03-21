use std::collections::HashMap;

use crate::catalog::types::{ApiEndpoint, ColumnName, TableName};

/// The output of the query planner: a recipe for executing a SQL query.
#[derive(Debug)]
pub struct QueryPlan {
    pub(crate) source: PlanSource,
    pub(crate) post: PostProcessing,
}

/// Where rows come from — a single API call or a join of two sources.
#[derive(Debug)]
pub enum PlanSource {
    ApiCall(ApiCall),
    Join {
        left: Box<PlanSource>,
        right: Box<PlanSource>,
        on: JoinCondition,
    },
}

/// A single API call to execute.
#[derive(Debug)]
pub struct ApiCall {
    pub(crate) table: TableName,
    pub(crate) endpoint: ApiEndpoint,
    /// Path parameter values extracted from WHERE (e.g., owner = 'anthropics').
    pub(crate) path_params: HashMap<ColumnName, String>,
    /// Query parameter values to push down to the API (e.g., state = 'open').
    pub(crate) query_params: HashMap<String, String>,
}

/// A join condition: left_col = right_col.
#[derive(Debug)]
pub struct JoinCondition {
    pub left_col: ColumnName,
    pub right_col: ColumnName,
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
        table: Option<TableName>,
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
    pub(crate) value: FilterValue,
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

#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}

/// An ORDER BY item.
#[derive(Debug)]
pub struct OrderByItem {
    pub(crate) column: ColumnName,
    pub(crate) descending: bool,
}
