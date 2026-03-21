use sqlparser::ast::{
    self, Expr, LimitClause, OrderByKind, Query, Select, SelectItem, SetExpr, Statement,
    TableFactor, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::catalog::Catalog;
use crate::catalog::types::{ColumnName, ColumnOrigin, TableName, VirtualTable};
use crate::error::{Error, Result};

use super::plan::{
    ApiCall, FilterOp, FilterValue, LocalFilter, OrderByItem, PostProcessing, Projection,
    QueryPlan, PlanSource,
};

/// Parse a SQL string and produce a `QueryPlan`.
///
/// Only supports a subset of SQL:
/// - Single-table SELECT (JOINs planned for later)
/// - WHERE with AND-connected conditions
/// - ORDER BY, LIMIT, OFFSET
/// - Column projections and SELECT *
pub fn plan_query(sql: &str, catalog: &Catalog) -> Result<QueryPlan> {
    let statements = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|e| Error::SqlParse(e.to_string()))?;

    let statement = match statements.as_slice() {
        [single] => single,
        [] => return Err(Error::UnsupportedSql("empty query".to_owned().to_owned())),
        _ => return Err(Error::UnsupportedSql("multiple statements not supported".to_owned().to_owned())),
    };

    let query = match statement {
        Statement::Query(q) => q,
        _ => return Err(Error::UnsupportedSql("only SELECT queries are supported".to_owned().to_owned())),
    };

    plan_select(query, catalog)
}

fn plan_select(query: &Query, catalog: &Catalog) -> Result<QueryPlan> {
    // Reject CTEs, set operations, etc.
    if !query.with.is_none() {
        return Err(Error::UnsupportedSql("WITH (CTEs) not supported".to_owned().to_owned()));
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return Err(Error::UnsupportedSql("only simple SELECT supported (no UNION, INTERSECT, etc.)".to_owned().to_owned())),
    };

    // Extract the source table
    let table_ref = extract_single_table(select)?;
    let table_name = resolve_table_name(&table_ref.name)?;
    let table = catalog.require(&table_name)?;

    // Classify WHERE conditions
    let where_conditions = match &select.selection {
        Some(expr) => flatten_and_conditions(expr),
        None => Vec::new(),
    };

    let classified = classify_conditions(&where_conditions, table)?;

    // Validate all required path params are present
    validate_required_params(table, &classified)?;

    // Build the API call
    let api_call = build_api_call(table, &classified);

    // Build projections
    let projections = plan_projections(&select.projection, table)?;

    // Build ORDER BY
    let order_by = plan_order_by(&query.order_by)?;

    // Extract LIMIT / OFFSET
    let (limit, offset) = extract_limit_offset(&query.limit_clause);

    Ok(QueryPlan {
        source: PlanSource::ApiCall(api_call),
        post: PostProcessing {
            projections,
            local_filters: classified.local_filters,
            order_by,
            limit,
            offset,
        },
    })
}

// ---------------------------------------------------------------------------
// Table extraction
// ---------------------------------------------------------------------------

struct TableRef {
    name: String,
    #[allow(dead_code)] // will be used for JOIN alias resolution
    alias: Option<String>,
}

fn extract_single_table(select: &Select) -> Result<TableRef> {
    if select.from.is_empty() {
        return Err(Error::UnsupportedSql(
            "no FROM clause — sqlize queries require a table (e.g., SELECT * FROM issues WHERE ...)".to_owned(),
        ));
    }
    if select.from.len() > 1 {
        return Err(Error::UnsupportedSql(
            "multiple tables in FROM not yet supported — use one table per query".to_owned(),
        ));
    }

    let from = &select.from[0];
    if !from.joins.is_empty() {
        return Err(Error::UnsupportedSql("JOINs not yet supported".to_owned().to_owned()));
    }

    match &from.relation {
        TableFactor::Table { name, alias, .. } => Ok(TableRef {
            name: name.to_string(),
            alias: alias.as_ref().map(|a| a.name.value.clone()),
        }),
        _ => Err(Error::UnsupportedSql(
            "only simple table references supported (no subqueries, table functions, etc.)".to_owned(),
        )),
    }
}

fn resolve_table_name(name: &str) -> Result<TableName> {
    // sqlparser may preserve case or quoting; normalize to lowercase
    let normalized = name.to_ascii_lowercase();
    TableName::new(&normalized)
}

// ---------------------------------------------------------------------------
// WHERE clause classification
// ---------------------------------------------------------------------------

/// The result of classifying WHERE conditions against a table's column origins.
struct ClassifiedConditions {
    /// Path parameter values (must all be present for the API call).
    path_params: std::collections::HashMap<ColumnName, String>,
    /// Query parameters to push down to the API.
    query_params: std::collections::HashMap<String, String>,
    /// Conditions that must be evaluated locally after fetching.
    local_filters: Vec<LocalFilter>,
}

/// Break an AND-connected expression tree into individual conditions.
fn flatten_and_conditions(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            let mut result = flatten_and_conditions(left);
            result.extend(flatten_and_conditions(right));
            result
        }
        Expr::Nested(inner) => flatten_and_conditions(inner),
        other => vec![other],
    }
}

fn classify_conditions(
    conditions: &[&Expr],
    table: &VirtualTable,
) -> Result<ClassifiedConditions> {
    let mut path_params = std::collections::HashMap::new();
    let mut query_params = std::collections::HashMap::new();
    let mut local_filters = Vec::new();

    for expr in conditions {
        match classify_single_condition(expr, table) {
            Ok(ConditionClass::PathParam { name, value }) => {
                path_params.insert(name, value);
            }
            Ok(ConditionClass::QueryParam { api_name, value }) => {
                query_params.insert(api_name, value);
            }
            Ok(ConditionClass::LocalFilter(f)) => {
                local_filters.push(f);
            }
            Err(_) => {
                // If we can't classify a condition, treat the whole expression
                // as something we can't handle rather than silently dropping it.
                return Err(Error::UnsupportedSql(
                    "unsupported WHERE condition (only column = value with AND supported)".to_owned(),
                ));
            }
        }
    }

    Ok(ClassifiedConditions {
        path_params,
        query_params,
        local_filters,
    })
}

enum ConditionClass {
    PathParam { name: ColumnName, value: String },
    QueryParam { api_name: String, value: String },
    LocalFilter(LocalFilter),
}

fn classify_single_condition(
    expr: &Expr,
    table: &VirtualTable,
) -> Result<ConditionClass> {
    // Handle IS NULL / IS NOT NULL
    if let Expr::IsNull(inner) = expr {
        let col_name = extract_column_name(inner)?;
        return Ok(ConditionClass::LocalFilter(LocalFilter {
            column: col_name,
            op: FilterOp::IsNull,
            value: FilterValue::Null,
        }));
    }
    if let Expr::IsNotNull(inner) = expr {
        let col_name = extract_column_name(inner)?;
        return Ok(ConditionClass::LocalFilter(LocalFilter {
            column: col_name,
            op: FilterOp::IsNotNull,
            value: FilterValue::Null,
        }));
    }

    // Handle binary operations: column op value
    let Expr::BinaryOp { left, op, right } = expr else {
        return Err(Error::UnsupportedSql("unsupported WHERE expression".to_owned().to_owned()));
    };

    let col_name = extract_column_name(left)?;
    let filter_op = convert_op(op)?;
    let filter_value = extract_value(right)?;

    // Look up the column in the table to determine its origin
    let column = table
        .columns
        .iter()
        .find(|c| c.name == col_name);

    match column {
        Some(col) => match &col.origin {
            ColumnOrigin::PathParam if filter_op == FilterOp::Eq => {
                let value_str = filter_value_to_string(&filter_value);
                Ok(ConditionClass::PathParam {
                    name: col.name.clone(),
                    value: value_str,
                })
            }
            ColumnOrigin::QueryParam { api_name } if filter_op == FilterOp::Eq => {
                let param_name = api_name
                    .as_deref()
                    .unwrap_or(col.name.as_str())
                    .to_owned();
                let value_str = filter_value_to_string(&filter_value);
                Ok(ConditionClass::QueryParam {
                    api_name: param_name,
                    value: value_str,
                })
            }
            // Path/query params with non-equality ops become local filters
            _ => Ok(ConditionClass::LocalFilter(LocalFilter {
                column: col_name,
                op: filter_op,
                value: filter_value,
            })),
        },
        // Unknown columns become local filters — the execution engine
        // will skip them if the column doesn't exist in the response.
        None => Ok(ConditionClass::LocalFilter(LocalFilter {
            column: col_name,
            op: filter_op,
            value: filter_value,
        })),
    }
}

fn extract_column_name(expr: &Expr) -> Result<ColumnName> {
    match expr {
        Expr::Identifier(ident) => {
            let name = ident.value.to_ascii_lowercase();
            ColumnName::new(&name)
        }
        Expr::CompoundIdentifier(parts) => {
            // table.column — take the last part
            let name = parts
                .last()
                .map(|p| p.value.to_ascii_lowercase())
                .unwrap_or_default();
            ColumnName::new(&name)
        }
        _ => Err(Error::UnsupportedSql(
            "left side of WHERE condition must be a column name".to_owned(),
        )),
    }
}

fn convert_op(op: &ast::BinaryOperator) -> Result<FilterOp> {
    match op {
        ast::BinaryOperator::Eq => Ok(FilterOp::Eq),
        ast::BinaryOperator::NotEq => Ok(FilterOp::NotEq),
        ast::BinaryOperator::Lt => Ok(FilterOp::Lt),
        ast::BinaryOperator::LtEq => Ok(FilterOp::LtEq),
        ast::BinaryOperator::Gt => Ok(FilterOp::Gt),
        ast::BinaryOperator::GtEq => Ok(FilterOp::GtEq),
        _ => Err(Error::UnsupportedSql(
            "unsupported operator (supported: =, !=, <, <=, >, >=)".to_owned(),
        )),
    }
}

fn extract_value(expr: &Expr) -> Result<FilterValue> {
    match expr {
        Expr::Value(v) => match &v.value {
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                Ok(FilterValue::String(s.clone()))
            }
            SqlValue::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(FilterValue::Integer(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(FilterValue::Float(f))
                } else {
                    Err(Error::UnsupportedSql("unparseable number".to_owned().to_owned()))
                }
            }
            SqlValue::Boolean(b) => Ok(FilterValue::Boolean(*b)),
            SqlValue::Null => Ok(FilterValue::Null),
            _ => Err(Error::UnsupportedSql("unsupported value type in WHERE".to_owned().to_owned())),
        },
        _ => Err(Error::UnsupportedSql(
            "right side of WHERE condition must be a literal value".to_owned(),
        )),
    }
}

fn filter_value_to_string(v: &FilterValue) -> String {
    match v {
        FilterValue::String(s) => s.clone(),
        FilterValue::Integer(i) => i.to_string(),
        FilterValue::Float(f) => f.to_string(),
        FilterValue::Boolean(b) => b.to_string(),
        FilterValue::Null => String::new(),
    }
}

// ---------------------------------------------------------------------------
// Required param validation
// ---------------------------------------------------------------------------

fn validate_required_params(
    table: &VirtualTable,
    classified: &ClassifiedConditions,
) -> Result<()> {
    for col in table.required_params() {
        if !classified.path_params.contains_key(&col.name) {
            return Err(Error::MissingRequiredParam {
                table: table.name.clone(),
                column: col.name.clone(),
            });
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// API call construction
// ---------------------------------------------------------------------------

fn build_api_call(table: &VirtualTable, classified: &ClassifiedConditions) -> ApiCall {
    ApiCall {
        table: table.name.clone(),
        endpoint: table.endpoint.clone(),
        path_params: classified.path_params.clone(),
        query_params: classified.query_params.clone(),
    }
}

// ---------------------------------------------------------------------------
// Projections
// ---------------------------------------------------------------------------

fn plan_projections(
    items: &[SelectItem],
    _table: &VirtualTable,
) -> Result<Vec<Projection>> {
    let mut projections = Vec::new();

    for item in items {
        match item {
            SelectItem::Wildcard(_) => {
                projections.push(Projection::Star);
            }
            SelectItem::UnnamedExpr(expr) => {
                let col_name = extract_column_name(expr)?;
                projections.push(Projection::Column {
                    table: None,
                    name: col_name,
                    alias: None,
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let col_name = extract_column_name(expr)?;
                projections.push(Projection::Column {
                    table: None,
                    name: col_name,
                    alias: Some(alias.value.clone()),
                });
            }
            _ => return Err(Error::UnsupportedSql(
                "unsupported SELECT item (only columns, aliases, and * supported)".to_owned(),
            )),
        }
    }

    Ok(projections)
}

// ---------------------------------------------------------------------------
// ORDER BY
// ---------------------------------------------------------------------------

fn plan_order_by(order_by: &Option<ast::OrderBy>) -> Result<Vec<OrderByItem>> {
    let Some(ob) = order_by else {
        return Ok(Vec::new());
    };

    let exprs = match &ob.kind {
        OrderByKind::Expressions(exprs) => exprs,
        OrderByKind::All(_) => {
            return Err(Error::UnsupportedSql("ORDER BY ALL not supported".to_owned().to_owned()))
        }
    };

    let mut items = Vec::new();
    for expr in exprs {
        let col_name = extract_column_name(&expr.expr)?;
        let descending = expr.options.asc.map_or(false, |asc| !asc);
        items.push(OrderByItem {
            column: col_name,
            descending,
        });
    }
    Ok(items)
}

// ---------------------------------------------------------------------------
// LIMIT / OFFSET
// ---------------------------------------------------------------------------

fn extract_limit_offset(clause: &Option<LimitClause>) -> (Option<u64>, Option<u64>) {
    let Some(clause) = clause else {
        return (None, None);
    };

    match clause {
        LimitClause::LimitOffset { limit, offset, .. } => {
            let l = limit.as_ref().and_then(expr_to_u64);
            let o = offset.as_ref().and_then(|off| expr_to_u64(&off.value));
            (l, o)
        }
        LimitClause::OffsetCommaLimit { offset, limit } => {
            let l = expr_to_u64(limit);
            let o = expr_to_u64(offset);
            (l, o)
        }
    }
}

fn expr_to_u64(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Value(v) => match &v.value {
            SqlValue::Number(n, _) => n.parse().ok(),
            _ => None,
        },
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Format a query plan for human-readable EXPLAIN output.
pub fn explain(plan: &QueryPlan) -> String {
    let mut out = String::new();
    explain_source(&plan.source, &mut out, 0);

    if !plan.post.local_filters.is_empty() {
        out.push_str("\nLocal filters:\n");
        for f in &plan.post.local_filters {
            out.push_str(&format!("  {} {:?} {:?}\n", f.column, f.op, f.value));
        }
    }

    if !plan.post.order_by.is_empty() {
        out.push_str("Order by: ");
        let items: Vec<_> = plan.post.order_by.iter().map(|o| {
            let dir = if o.descending { "DESC" } else { "ASC" };
            format!("{} {dir}", o.column)
        }).collect();
        out.push_str(&items.join(", "));
        out.push('\n');
    }

    if let Some(limit) = plan.post.limit {
        out.push_str(&format!("Limit: {limit}\n"));
    }
    if let Some(offset) = plan.post.offset {
        out.push_str(&format!("Offset: {offset}\n"));
    }

    out
}

fn explain_source(source: &PlanSource, out: &mut String, indent: usize) {
    let pad = " ".repeat(indent);
    match source {
        PlanSource::ApiCall(call) => {
            out.push_str(&format!(
                "{pad}ApiCall: {} {}\n",
                call.endpoint.method, call.endpoint.path
            ));
            if !call.path_params.is_empty() {
                out.push_str(&format!("{pad}  path_params: {:?}\n", call.path_params));
            }
            if !call.query_params.is_empty() {
                out.push_str(&format!("{pad}  query_params: {:?}\n", call.query_params));
            }
        }
        PlanSource::Join { left, right, on } => {
            out.push_str(&format!("{pad}Join on {} = {}:\n", on.left_col, on.right_col));
            explain_source(left, out, indent + 2);
            explain_source(right, out, indent + 2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::types::*;

    fn test_catalog() -> Catalog {
        let issues = VirtualTable {
            name: TableName::new("issues").unwrap(),
            description: "Repository issues".to_owned(),
            columns: vec![
                Column {
                    name: ColumnName::new("owner").unwrap(),
                    col_type: ColumnType::String,
                    nullable: false,
                    description: None,
                    origin: ColumnOrigin::PathParam,
                },
                Column {
                    name: ColumnName::new("repo").unwrap(),
                    col_type: ColumnType::String,
                    nullable: false,
                    description: None,
                    origin: ColumnOrigin::PathParam,
                },
                Column {
                    name: ColumnName::new("state").unwrap(),
                    col_type: ColumnType::String,
                    nullable: false,
                    description: None,
                    origin: ColumnOrigin::QueryParam { api_name: None },
                },
                Column {
                    name: ColumnName::new("id").unwrap(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                    description: None,
                    origin: ColumnOrigin::ResponseField,
                },
                Column {
                    name: ColumnName::new("number").unwrap(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                    description: None,
                    origin: ColumnOrigin::ResponseField,
                },
                Column {
                    name: ColumnName::new("title").unwrap(),
                    col_type: ColumnType::String,
                    nullable: false,
                    description: None,
                    origin: ColumnOrigin::ResponseField,
                },
                Column {
                    name: ColumnName::new("created_at").unwrap(),
                    col_type: ColumnType::Timestamp,
                    nullable: false,
                    description: None,
                    origin: ColumnOrigin::ResponseField,
                },
            ],
            endpoint: ApiEndpoint {
                method: HttpMethod::Get,
                path: PathTemplate::new("/repos/{owner}/{repo}/issues").unwrap(),
                base_url: "https://api.github.com".to_owned(),
                accept: "application/json".to_owned(),
                data_path: None,
            },
        };

        Catalog::from_tables(vec![issues]).unwrap()
    }

    #[test]
    fn plan_simple_select() {
        let catalog = test_catalog();
        let plan = plan_query(
            "SELECT number, title FROM issues WHERE owner = 'anthropics' AND repo = 'claude-code' AND state = 'open' LIMIT 10",
            &catalog,
        ).unwrap();

        let PlanSource::ApiCall(call) = &plan.source else {
            panic!("expected ApiCall");
        };

        let owner = ColumnName::new("owner").unwrap();
        let repo = ColumnName::new("repo").unwrap();
        assert_eq!(call.path_params.get(&owner).unwrap(), "anthropics");
        assert_eq!(call.path_params.get(&repo).unwrap(), "claude-code");
        assert_eq!(call.query_params.get("state").unwrap(), "open");
        assert_eq!(plan.post.limit, Some(10));
        assert_eq!(plan.post.projections.len(), 2);
    }

    #[test]
    fn missing_required_param_is_error() {
        let catalog = test_catalog();
        let result = plan_query(
            "SELECT * FROM issues WHERE owner = 'anthropics'",
            &catalog,
        );
        assert!(matches!(result, Err(Error::MissingRequiredParam { .. })));
    }

    #[test]
    fn local_filter_for_response_column() {
        let catalog = test_catalog();
        let plan = plan_query(
            "SELECT * FROM issues WHERE owner = 'a' AND repo = 'b' AND number > 100",
            &catalog,
        ).unwrap();

        assert_eq!(plan.post.local_filters.len(), 1);
        assert_eq!(plan.post.local_filters[0].column.as_str(), "number");
        assert_eq!(plan.post.local_filters[0].op, FilterOp::Gt);
    }

    #[test]
    fn order_by_and_offset() {
        let catalog = test_catalog();
        let plan = plan_query(
            "SELECT * FROM issues WHERE owner = 'a' AND repo = 'b' ORDER BY created_at DESC LIMIT 5 OFFSET 10",
            &catalog,
        ).unwrap();

        assert_eq!(plan.post.order_by.len(), 1);
        assert_eq!(plan.post.order_by[0].column.as_str(), "created_at");
        assert!(plan.post.order_by[0].descending);
        assert_eq!(plan.post.limit, Some(5));
        assert_eq!(plan.post.offset, Some(10));
    }

    #[test]
    fn explain_output() {
        let catalog = test_catalog();
        let plan = plan_query(
            "SELECT number, title FROM issues WHERE owner = 'anthropics' AND repo = 'claude-code' AND state = 'open' LIMIT 5",
            &catalog,
        ).unwrap();

        let output = explain(&plan);
        assert!(output.contains("ApiCall: GET /repos/{owner}/{repo}/issues"));
        assert!(output.contains("anthropics"));
        assert!(output.contains("Limit: 5"));
    }

    #[test]
    fn rejects_non_select() {
        let catalog = test_catalog();
        let result = plan_query("DROP TABLE issues", &catalog);
        assert!(matches!(result, Err(Error::UnsupportedSql(_))));
    }

    #[test]
    fn rejects_unknown_table() {
        let catalog = test_catalog();
        let result = plan_query("SELECT * FROM nonexistent WHERE x = 'y'", &catalog);
        assert!(matches!(result, Err(Error::TableNotFound(_))));
    }
}
