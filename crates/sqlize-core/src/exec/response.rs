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

    // Exclude pure query params (sort, type, direction, etc.) — they're filter
    // controls, not data columns. Keep path params, response fields, and dual-origin.
    let data_columns: Vec<&Column> = columns
        .iter()
        .filter(|c| !matches!(c.origin, ColumnOrigin::QueryParam))
        .collect();

    let col_names: Vec<ColumnName> = data_columns.iter().map(|c| c.name.clone()).collect();

    let rows = items
        .iter()
        .map(|item| extract_row(item, &data_columns, param_values))
        .collect();

    Ok(ResultSet { columns: col_names, rows })
}

/// Extract a row of values from a JSON object, matching the catalog's column order.
/// Injects path/query param values for non-response columns.
fn extract_row(
    obj: &serde_json::Value,
    columns: &[&Column],
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
            // Try JSON response first — handles fields like `state` that are
            // both a query param and a response field.
            if let Some(v) = flat.get(col.name.as_str()) {
                return json_value_to_value(v);
            }
            // Fall back to param values for path/query params not in the response
            // (e.g., `owner` and `repo` which are URL segments, not response fields).
            if let Some(v) = param_values.get(&col.name) {
                return Value::String(v.clone());
            }
            Value::Null
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
    use crate::catalog::types::ColumnOrigin;
    use crate::catalog::types::ColumnType;

    fn test_columns() -> Vec<Column> {
        vec![
            Column {
                name: ColumnName::new("owner").unwrap(),
                col_type: ColumnType::String,
                nullable: false,
                description: None,
                origin: ColumnOrigin::PathParam,
                api_name: None,
            },
            Column {
                name: ColumnName::new("id").unwrap(),
                col_type: ColumnType::Integer,
                nullable: false,
                description: None,
                origin: ColumnOrigin::ResponseField,
                api_name: None,
            },
            Column {
                name: ColumnName::new("title").unwrap(),
                col_type: ColumnType::String,
                nullable: false,
                description: None,
                origin: ColumnOrigin::ResponseField,
                api_name: None,
            },
            Column {
                name: ColumnName::new("user_login").unwrap(),
                col_type: ColumnType::String,
                nullable: true,
                description: None,
                origin: ColumnOrigin::ResponseField,
                api_name: None,
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

    #[test]
    fn extra_api_fields_not_in_spec_are_ignored() {
        // API returns fields the spec doesn't declare — they shouldn't appear
        let json = serde_json::json!([{
            "id": 1,
            "title": "bug",
            "secret_field": "should not appear",
            "internal_score": 99,
            "user": {"login": "alice", "avatar_url": "https://..."}
        }]);
        let cols = test_columns();
        let mut params = HashMap::new();
        params.insert(ColumnName::new("owner").unwrap(), "test".to_owned());

        let result = json_to_result_set(&json, &cols, &params).unwrap();

        // Only the 4 columns from test_columns() (minus QueryParam ones)
        let col_names: Vec<&str> = result.columns.iter().map(|c| c.as_str()).collect();
        assert!(!col_names.contains(&"secret_field"));
        assert!(!col_names.contains(&"internal_score"));
        assert!(!col_names.contains(&"user_avatar_url"));

        // Declared columns are present
        assert!(col_names.contains(&"owner"));
        assert!(col_names.contains(&"id"));
        assert!(col_names.contains(&"title"));
        assert!(col_names.contains(&"user_login"));
    }

    #[test]
    fn shared_param_response_column_prefers_response_value() {
        // `state` is both a query param (for filtering) and a response field.
        // The response value should win over the param value.
        let cols = vec![
            Column {
                name: ColumnName::new("state").unwrap(),
                col_type: ColumnType::String,
                nullable: false,
                description: None,
                origin: ColumnOrigin::QueryParamAndResponseField,
                api_name: None,
            },
            Column {
                name: ColumnName::new("title").unwrap(),
                col_type: ColumnType::String,
                nullable: false,
                description: None,
                origin: ColumnOrigin::ResponseField,
                api_name: None,
            },
        ];

        let json = serde_json::json!([
            {"state": "open", "title": "bug"},
            {"state": "closed", "title": "done"}
        ]);

        let mut params = HashMap::new();
        params.insert(ColumnName::new("state").unwrap(), "open".to_owned());

        let result = json_to_result_set(&json, &cols, &params).unwrap();

        // First row: state from JSON is "open" (matches filter)
        assert_eq!(result.rows[0].0[0], Value::String("open".into()));
        // Second row: state from JSON is "closed" (differs from filter — response wins)
        assert_eq!(result.rows[1].0[0], Value::String("closed".into()));
    }

    #[test]
    fn param_only_column_uses_param_value() {
        // `owner` is a path param not present in the response — should use param value.
        let cols = vec![
            Column {
                name: ColumnName::new("owner").unwrap(),
                col_type: ColumnType::String,
                nullable: false,
                description: None,
                origin: ColumnOrigin::PathParam,
                api_name: None,
            },
        ];

        let json = serde_json::json!([{"id": 1}]);
        let mut params = HashMap::new();
        params.insert(ColumnName::new("owner").unwrap(), "rust-lang".to_owned());

        let result = json_to_result_set(&json, &cols, &params).unwrap();
        assert_eq!(result.rows[0].0[0], Value::String("rust-lang".into()));
    }
}
