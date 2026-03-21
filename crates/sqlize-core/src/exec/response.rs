use crate::catalog::types::{ColumnName, ResultSet, Row, TableName, Value, sanitize_name};
use crate::error::Result;

/// Convert a JSON API response into a `ResultSet`.
///
/// Expects either a JSON array (list endpoints) or a single object.
/// Each object's keys become columns; nested objects are flattened one level
/// with `_` separators (matching the catalog's column naming).
pub fn json_to_result_set(json: &serde_json::Value, _table: &TableName) -> Result<ResultSet> {
    let items = match json {
        serde_json::Value::Array(arr) => arr.as_slice(),
        serde_json::Value::Object(_) => std::slice::from_ref(json),
        _ => return Ok(ResultSet { columns: Vec::new(), rows: Vec::new() }),
    };

    if items.is_empty() {
        return Ok(ResultSet { columns: Vec::new(), rows: Vec::new() });
    }

    // Derive column order from the first item, flattening one level of nesting
    let columns = derive_columns(&items[0]);

    let rows = items
        .iter()
        .map(|item| extract_row(item, &columns))
        .collect();

    Ok(ResultSet { columns, rows })
}

/// Derive column names from a JSON object, flattening one level of nesting.
fn derive_columns(obj: &serde_json::Value) -> Vec<ColumnName> {
    let mut columns = Vec::new();

    let Some(map) = obj.as_object() else {
        return columns;
    };

    for (key, value) in map {
        let col_base = sanitize_name(key);

        if let serde_json::Value::Object(nested) = value {
            // Flatten one level: user.login → user_login
            for nested_key in nested.keys() {
                let col_name = format!("{}_{}", col_base, sanitize_name(nested_key));
                if let Ok(cn) = ColumnName::new(&col_name) {
                    columns.push(cn);
                }
            }
        } else if let Ok(cn) = ColumnName::new(&col_base) {
            columns.push(cn);
        }
    }

    columns
}

/// Extract a row of values from a JSON object, matching the given column order.
fn extract_row(obj: &serde_json::Value, columns: &[ColumnName]) -> Row {
    let Some(map) = obj.as_object() else {
        return Row(vec![Value::Null; columns.len()]);
    };

    // Build a flat lookup: "column_name" → json value
    let mut flat: std::collections::HashMap<String, &serde_json::Value> =
        std::collections::HashMap::new();

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
            flat.get(col.as_str())
                .map(|v| json_value_to_value(v))
                .unwrap_or(Value::Null)
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
        // Arrays and nested objects beyond first level become JSON values
        other => Value::Json(other.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flatten_nested_object() {
        let json = serde_json::json!({
            "id": 1,
            "title": "test",
            "user": {
                "login": "alice",
                "id": 42
            }
        });

        let table = TableName::new("test").unwrap();
        let result = json_to_result_set(&serde_json::json!([json]), &table).unwrap();

        let col_names: Vec<&str> = result.columns.iter().map(|c| c.as_str()).collect();
        assert!(col_names.contains(&"id"));
        assert!(col_names.contains(&"title"));
        assert!(col_names.contains(&"user_login"));
        assert!(col_names.contains(&"user_id"));
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn empty_array() {
        let table = TableName::new("test").unwrap();
        let result = json_to_result_set(&serde_json::json!([]), &table).unwrap();
        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn single_object_wraps_to_one_row() {
        let json = serde_json::json!({"id": 1, "name": "foo"});
        let table = TableName::new("test").unwrap();
        let result = json_to_result_set(&json, &table).unwrap();
        assert_eq!(result.rows.len(), 1);
    }
}
