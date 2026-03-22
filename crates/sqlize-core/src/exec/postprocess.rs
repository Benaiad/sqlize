use crate::catalog::types::{ColumnName, ResultSet, Row, Value};
use crate::sql::plan::{FilterOp, LocalFilter, OrderByItem, PostProcessing, Projection};

/// Apply post-processing steps to a result set in place.
pub fn apply(post: &PostProcessing, result: &mut ResultSet) {
    apply_local_filters(&post.local_filters, result);
    apply_order_by(&post.order_by, result);
    apply_offset(post.offset, result);
    apply_limit(post.limit, result);
    apply_projections(&post.projections, result);
}

// ---------------------------------------------------------------------------
// Local filters
// ---------------------------------------------------------------------------

fn apply_local_filters(filters: &[LocalFilter], result: &mut ResultSet) {
    if filters.is_empty() {
        return;
    }

    result.rows.retain(|row| {
        filters.iter().all(|f| row_matches_filter(row, &result.columns, f))
    });
}

fn row_matches_filter(row: &Row, columns: &[ColumnName], filter: &LocalFilter) -> bool {
    let Some(idx) = columns.iter().position(|c| *c == filter.column) else {
        return true; // Column not in result — don't filter
    };

    let value = &row.values()[idx];

    match filter.op {
        FilterOp::IsNull => matches!(value, Value::Null),
        FilterOp::IsNotNull => !matches!(value, Value::Null),
        FilterOp::Eq => value_eq(value, &filter.value),
        FilterOp::NotEq => !value_eq(value, &filter.value),
        FilterOp::Lt => value_cmp(value, &filter.value).is_some_and(|o| o.is_lt()),
        FilterOp::LtEq => value_cmp(value, &filter.value).is_some_and(|o| o.is_le()),
        FilterOp::Gt => value_cmp(value, &filter.value).is_some_and(|o| o.is_gt()),
        FilterOp::GtEq => value_cmp(value, &filter.value).is_some_and(|o| o.is_ge()),
        FilterOp::Like => false, // Not yet implemented
    }
}

fn value_eq(row_val: &Value, filter_val: &Value) -> bool {
    match (row_val, filter_val) {
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => (*a - *b).abs() < f64::EPSILON,
        (Value::Boolean(a), Value::Boolean(b)) => a == b,
        (Value::Null, Value::Null) => true,
        // Cross-type comparisons: integer as string
        (Value::Integer(a), Value::String(b)) => a.to_string() == *b,
        (Value::String(a), Value::Integer(b)) => a.parse::<i64>().ok() == Some(*b),
        _ => false,
    }
}

fn value_cmp(row_val: &Value, filter_val: &Value) -> Option<std::cmp::Ordering> {
    match (row_val, filter_val) {
        (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// ORDER BY
// ---------------------------------------------------------------------------

fn apply_order_by(order_by: &[OrderByItem], result: &mut ResultSet) {
    if order_by.is_empty() {
        return;
    }

    let col_indices: Vec<(usize, bool)> = order_by
        .iter()
        .filter_map(|item| {
            let idx = result.columns.iter().position(|c| *c == item.column)?;
            Some((idx, item.descending))
        })
        .collect();

    if col_indices.is_empty() {
        return;
    }

    result.rows.sort_by(|a, b| {
        for &(idx, descending) in &col_indices {
            let cmp = compare_values(&a.values()[idx], &b.values()[idx]);
            let cmp = if descending { cmp.reverse() } else { cmp };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Greater, // NULLs sort last
        (_, Value::Null) => std::cmp::Ordering::Less,
        (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    }
}

// ---------------------------------------------------------------------------
// LIMIT / OFFSET
// ---------------------------------------------------------------------------

fn apply_limit(limit: Option<u64>, result: &mut ResultSet) {
    if let Some(n) = limit {
        result.rows.truncate(n as usize);
    }
}

fn apply_offset(offset: Option<u64>, result: &mut ResultSet) {
    if let Some(n) = offset {
        let n = n as usize;
        if n >= result.rows.len() {
            result.rows.clear();
        } else {
            result.rows = result.rows.split_off(n);
        }
    }
}

// ---------------------------------------------------------------------------
// Projections
// ---------------------------------------------------------------------------

fn apply_projections(projections: &[Projection], result: &mut ResultSet) {
    if projections.is_empty() || projections.iter().any(|p| matches!(p, Projection::Star)) {
        return; // SELECT * or empty → keep all columns
    }

    let selected: Vec<(usize, Option<String>)> = projections
        .iter()
        .filter_map(|p| match p {
            Projection::Column { name, alias, .. } => {
                let idx = result.columns.iter().position(|c| *c == *name)?;
                Some((idx, alias.clone()))
            }
            Projection::Star => None,
        })
        .collect();

    // Rebuild columns
    let new_columns: Vec<ColumnName> = selected
        .iter()
        .map(|(idx, alias)| {
            alias
                .as_ref()
                .and_then(|a| ColumnName::new(a).ok())
                .unwrap_or_else(|| result.columns[*idx].clone())
        })
        .collect();

    // Rebuild rows
    let new_rows: Vec<Row> = result
        .rows
        .iter()
        .map(|row| {
            let values = selected.iter().map(|(idx, _)| row.values()[*idx].clone()).collect();
            Row::new(values)
        })
        .collect();

    result.columns = new_columns;
    result.rows = new_rows;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_result() -> ResultSet {
        ResultSet {
            columns: vec![
                ColumnName::new("id").unwrap(),
                ColumnName::new("name").unwrap(),
                ColumnName::new("age").unwrap(),
            ],
            rows: vec![
                Row::new(vec![Value::Integer(1), Value::String("alice".into()), Value::Integer(30)]),
                Row::new(vec![Value::Integer(2), Value::String("bob".into()), Value::Integer(25)]),
                Row::new(vec![Value::Integer(3), Value::String("charlie".into()), Value::Integer(35)]),
            ],
        }
    }

    #[test]
    fn filter_eq() {
        let mut result = make_result();
        let filters = vec![LocalFilter {
            column: ColumnName::new("name").unwrap(),
            op: FilterOp::Eq,
            value: Value::String("bob".into()),
        }];
        apply_local_filters(&filters, &mut result);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values()[0], Value::Integer(2));
    }

    #[test]
    fn filter_gt() {
        let mut result = make_result();
        let filters = vec![LocalFilter {
            column: ColumnName::new("age").unwrap(),
            op: FilterOp::Gt,
            value: Value::Integer(28),
        }];
        apply_local_filters(&filters, &mut result);
        assert_eq!(result.rows.len(), 2); // alice (30) and charlie (35)
    }

    #[test]
    fn order_by_descending() {
        let mut result = make_result();
        let order = vec![OrderByItem {
            column: ColumnName::new("age").unwrap(),
            descending: true,
        }];
        apply_order_by(&order, &mut result);
        assert_eq!(result.rows[0].values()[2], Value::Integer(35)); // charlie first
        assert_eq!(result.rows[2].values()[2], Value::Integer(25)); // bob last
    }

    #[test]
    fn limit_and_offset() {
        let mut result = make_result();
        apply_offset(Some(1), &mut result);
        apply_limit(Some(1), &mut result);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values()[0], Value::Integer(2)); // bob
    }

    #[test]
    fn projection_subset() {
        let mut result = make_result();
        let projections = vec![
            Projection::Column {
                name: ColumnName::new("name").unwrap(),
                alias: None,
            },
            Projection::Column {
                name: ColumnName::new("id").unwrap(),
                alias: Some("issue_id".into()),
            },
        ];
        apply_projections(&projections, &mut result);
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].as_str(), "name");
        assert_eq!(result.columns[1].as_str(), "issue_id");
        assert_eq!(result.rows[0].len(), 2);
    }
}
