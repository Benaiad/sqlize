use std::collections::HashMap;

use crate::catalog::types::{
    Column, ColumnName, ColumnOrigin, ResultSet, Row, Value, sanitize_name,
};
use crate::error::Result;

/// Convert a JSON API response into a `ResultSet` using the catalog schema.
///
/// Uses the declared columns from the `VirtualTable` to determine the result
/// structure. Path and query parameter values are injected from the `ApiCall`
/// so that `SELECT owner, repo, title ...` returns the param values alongside
/// response fields.
pub fn json_to_result_set(
    json: &serde_json::Value,
    columns: &[Column],
    param_values: &HashMap<ColumnName, String>,
) -> Result<ResultSet> {
    let items = match json {
        serde_json::Value::Array(arr) => arr.as_slice(),
        serde_json::Value::Object(_) => std::slice::from_ref(json),
        _ => return Ok(ResultSet { columns: Vec::new(), rows: Vec::new() }),
    };

    let col_names: Vec<ColumnName> = columns.iter().map(|c| c.name.clone()).collect();

    let rows = items
        .iter()
        .map(|item| extract_row(item, columns, param_values))
        .collect();

    Ok(ResultSet { columns: col_names, rows })
}

/// Extract a row of values from a JSON object, matching the catalog's column order.
/// Injects path/query param values for non-response columns.
fn extract_row(
    obj: &serde_json::Value,
    columns: &[Column],
    param_values: &HashMap<ColumnName, String>,
) -> Row {
    let Some(map) = obj.as_object() else {
        return Row(vec![Value::Null; columns.len()]);
    };

    // Build a flat lookup: "sanitized_key" → json value (one level of flattening)
    let mut flat: HashMap<String, &serde_json::Value> = HashMap::new();
    for (key, value) in map {
        let col_base = sanitize_name(key);
        if let serde_json::Value::Object(nested) = value {
            for (nested_key, nested_val) in nested {
                let col_name = format!("{}_{}", col_base, sanitize_name(nested_key));
                flat.insert(col_name, nested_val);
            }
        } else {
            flat.insert(col_base, value);
        }
    }

    let values = columns
        .iter()
        .map(|col| {
            match &col.origin {
                // Path/query params: inject from the WHERE clause values
                ColumnOrigin::PathParam | ColumnOrigin::QueryParam { .. } => {
                    param_values
                        .get(&col.name)
                        .map(|v| Value::String(v.clone()))
                        .unwrap_or(Value::Null)
                }
                // Response fields: extract from JSON
                ColumnOrigin::ResponseField => {
                    flat.get(col.name.as_str())
                        .map(|v| json_value_to_value(v))
                        .unwrap_or(Value::Null)
                }
            }
        })
        .collect();

    Row(values)
}

fn json_value_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        other => Value::Json(other.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::types::ColumnType;

    fn test_columns() -> Vec<Column> {
        vec![
            Column {
                name: ColumnName::new("owner").unwrap(),
                col_type: ColumnType::String,
                nullable: false,
                description: None,
                origin: ColumnOrigin::PathParam,
            },
            Column {
                name: ColumnName::new("id").unwrap(),
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
                name: ColumnName::new("user_login").unwrap(),
                col_type: ColumnType::String,
                nullable: true,
                description: None,
                origin: ColumnOrigin::ResponseField,
            },
        ]
    }

    #[test]
    fn schema_driven_extraction() {
        let json = serde_json::json!([
            {"id": 1, "title": "bug", "user": {"login": "alice"}},
            {"id": 2, "title": "feat", "user": {"login": "bob"}}
        ]);
        let cols = test_columns();
        let mut params = HashMap::new();
        params.insert(ColumnName::new("owner").unwrap(), "rust-lang".to_owned());

        let result = json_to_result_set(&json, &cols, &params).unwrap();

        assert_eq!(result.columns.len(), 4);
        assert_eq!(result.columns[0].as_str(), "owner");
        assert_eq!(result.rows.len(), 2);
        // owner injected from params
        assert_eq!(result.rows[0].0[0], Value::String("rust-lang".into()));
        // response fields extracted
        assert_eq!(result.rows[0].0[1], Value::Integer(1));
        assert_eq!(result.rows[0].0[2], Value::String("bug".into()));
        assert_eq!(result.rows[0].0[3], Value::String("alice".into()));
    }

    #[test]
    fn empty_array() {
        let cols = test_columns();
        let result = json_to_result_set(&serde_json::json!([]), &cols, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 0);
        assert_eq!(result.columns.len(), 4);
    }

    #[test]
    fn missing_fields_become_null() {
        let json = serde_json::json!([{"id": 1}]);
        let cols = test_columns();
        let result = json_to_result_set(&json, &cols, &HashMap::new()).unwrap();
        assert_eq!(result.rows[0].0[2], Value::Null); // title missing
        assert_eq!(result.rows[0].0[3], Value::Null); // user_login missing
    }
}
