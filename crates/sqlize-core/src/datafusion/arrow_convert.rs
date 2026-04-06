use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::{DataFusionError, ScalarValue};

use crate::catalog::types::{
    Column, ColumnName, ColumnType, ResultSet, Row, VirtualTable, sanitize_name,
};

/// Map a sqlize `ColumnType` to an Arrow `DataType`.
pub fn column_type_to_arrow(ct: &ColumnType) -> DataType {
    match ct {
        ColumnType::String => DataType::Utf8,
        ColumnType::Integer => DataType::Int64,
        ColumnType::Float => DataType::Float64,
        ColumnType::Boolean => DataType::Boolean,
        ColumnType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ColumnType::Json => DataType::Utf8,
    }
}

/// Build an Arrow schema from a `VirtualTable`'s result columns.
pub fn virtual_table_to_schema(table: &VirtualTable) -> SchemaRef {
    let fields: Vec<Field> = table
        .result_columns()
        .map(|col| {
            Field::new(
                col.name.as_str(),
                column_type_to_arrow(&col.col_type),
                col.nullable,
            )
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// Pre-computed mapping from sanitized column names to original JSON key paths.
/// Built once per page from a sample JSON object, eliminates per-row `sanitize_name` calls.
struct SanitizedKeyMap {
    /// Direct matches: sanitized_key → original_key
    direct: HashMap<String, String>,
    /// Nested matches (one-level flattening): sanitized "parent_child" → (parent_key, child_key)
    nested: HashMap<String, (String, String)>,
}

impl SanitizedKeyMap {
    fn build(sample: &serde_json::Map<String, serde_json::Value>) -> Self {
        let mut direct = HashMap::new();
        let mut nested = HashMap::new();
        for (key, value) in sample {
            let sanitized = sanitize_name(key);
            direct.insert(sanitized.clone(), key.clone());
            if let serde_json::Value::Object(nested_map) = value {
                for nk in nested_map.keys() {
                    let suffix = sanitize_name(nk);
                    // Explicit `_` separator — makes the separator bug unconstructable
                    let full = format!("{sanitized}_{suffix}");
                    nested.insert(full, (key.clone(), nk.clone()));
                }
            }
        }
        Self { direct, nested }
    }

    fn get<'a>(
        &self,
        col_name: &str,
        item: &'a serde_json::Map<String, serde_json::Value>,
    ) -> Option<&'a serde_json::Value> {
        if let Some(original_key) = self.direct.get(col_name) {
            return item.get(original_key);
        }
        if let Some((parent, child)) = self.nested.get(col_name) {
            if let Some(serde_json::Value::Object(nested)) = item.get(parent) {
                return nested.get(child);
            }
        }
        None
    }
}

/// Convert a JSON API response into an Arrow `RecordBatch`.
pub fn json_response_to_batch(
    json: &serde_json::Value,
    columns: &[Column],
    param_values: &HashMap<ColumnName, ScalarValue>,
    schema: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let items = match json {
        serde_json::Value::Array(arr) => arr.as_slice(),
        serde_json::Value::Object(_) => std::slice::from_ref(json),
        other => {
            return Err(DataFusionError::External(Box::new(std::io::Error::other(
                format!(
                    "expected JSON array or object from API, got {}",
                    match other {
                        serde_json::Value::Null => "null",
                        serde_json::Value::Bool(_) => "boolean",
                        serde_json::Value::Number(_) => "number",
                        serde_json::Value::String(_) => "string",
                        _ => unreachable!(),
                    }
                ),
            ))));
        }
    };

    // Build key map once from the first item (O(keys) instead of O(rows * cols * keys))
    let key_map = items
        .first()
        .and_then(|item| item.as_object())
        .map(SanitizedKeyMap::build);

    let data_columns: Vec<&Column> = columns
        .iter()
        .filter(|c| c.role.appears_in_results())
        .collect();

    let num_rows = items.len();
    let num_cols = data_columns.len();

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_cols);

    for (col_idx, col) in data_columns.iter().enumerate() {
        let field = &schema.fields()[col_idx];

        match field.data_type() {
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for item in items {
                    let val = extract_value(item, col, param_values, key_map.as_ref());
                    match val {
                        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                            builder.append_value(&s);
                        }
                        v if v.is_null() => builder.append_null(),
                        other => builder.append_value(other.to_string()),
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for item in items {
                    let val = extract_value(item, col, param_values, key_map.as_ref());
                    match val {
                        ScalarValue::Int64(Some(n)) => builder.append_value(n),
                        _ => builder.append_null(),
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for item in items {
                    let val = extract_value(item, col, param_values, key_map.as_ref());
                    match val {
                        ScalarValue::Float64(Some(n)) => builder.append_value(n),
                        ScalarValue::Int64(Some(n)) => builder.append_value(n as f64),
                        _ => builder.append_null(),
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(num_rows);
                for item in items {
                    let val = extract_value(item, col, param_values, key_map.as_ref());
                    match val {
                        ScalarValue::Boolean(Some(b)) => builder.append_value(b),
                        _ => builder.append_null(),
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            DataType::Timestamp(_, _) => {
                // Build as strings, then batch-cast to timestamp
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for item in items {
                    let val = extract_value(item, col, param_values, key_map.as_ref());
                    match val {
                        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                            builder.append_value(&s);
                        }
                        v if v.is_null() => builder.append_null(),
                        other => builder.append_value(other.to_string()),
                    }
                }
                let string_array = builder.finish();
                let target_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
                let ts_array = datafusion::arrow::compute::cast(&string_array, &target_type)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                arrays.push(ts_array);
            }
            _ => {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for item in items {
                    let val = extract_value(item, col, param_values, key_map.as_ref());
                    match val {
                        v if v.is_null() => builder.append_null(),
                        other => builder.append_value(other.to_string()),
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
        }
    }

    RecordBatch::try_new(schema.clone(), arrays).map_err(|e| DataFusionError::External(Box::new(e)))
}

/// Extract a scalar value from a JSON item for a given column.
///
/// Path params always use the pushed value (the user's WHERE clause),
/// not the response field — even if the response has a field with the
/// same name (e.g., `owner` is both a path param and a nested user object).
fn extract_value(
    item: &serde_json::Value,
    col: &Column,
    param_values: &HashMap<ColumnName, ScalarValue>,
    key_map: Option<&SanitizedKeyMap>,
) -> ScalarValue {
    // Path params: always use the pushed value
    if col.role.is_required() {
        if let Some(v) = param_values.get(&col.name) {
            return v.clone();
        }
    }

    // Response fields: use pre-computed key map, fall back to param values
    if let Some(map) = item.as_object() {
        let col_name = col.name.as_str();
        let json_val = key_map.and_then(|km| km.get(col_name, map));
        if let Some(v) = json_val {
            return json_to_scalar_value(v);
        }
    }

    if let Some(v) = param_values.get(&col.name) {
        return v.clone();
    }

    ScalarValue::Null
}

fn json_to_scalar_value(v: &serde_json::Value) -> ScalarValue {
    match v {
        serde_json::Value::Null => ScalarValue::Null,
        serde_json::Value::Bool(b) => ScalarValue::Boolean(Some(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                ScalarValue::Int64(Some(i))
            } else if let Some(f) = n.as_f64() {
                ScalarValue::Float64(Some(f))
            } else {
                ScalarValue::Utf8(Some(n.to_string()))
            }
        }
        serde_json::Value::String(s) => ScalarValue::Utf8(Some(s.clone())),
        other => ScalarValue::Utf8(Some(other.to_string())),
    }
}

/// Convert DataFusion `RecordBatch` results back to a `ResultSet`.
pub fn batches_to_result_set(batches: &[RecordBatch]) -> ResultSet {
    if batches.is_empty() {
        return ResultSet {
            columns: Vec::new(),
            rows: Vec::new(),
        };
    }

    let schema = batches[0].schema();
    let columns: Vec<ColumnName> = schema
        .fields()
        .iter()
        .map(|f| {
            ColumnName::new(f.name()).unwrap_or_else(|_| {
                // Safety: "_unknown" is a valid column name by construction
                match ColumnName::new("_unknown") {
                    Ok(name) => name,
                    Err(_) => unreachable!(),
                }
            })
        })
        .collect();

    let mut rows = Vec::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let values: Vec<ScalarValue> = batch
                .columns()
                .iter()
                .map(|col| {
                    ScalarValue::try_from_array(col.as_ref(), row_idx).unwrap_or(ScalarValue::Null)
                })
                .collect();
            rows.push(Row::new(values));
        }
    }

    ResultSet { columns, rows }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::types::{
        AcceptHeader, ApiEndpoint, ApiParamName, BaseUrl, ColumnRole, HttpMethod, PathTemplate,
        TableName,
    };
    use datafusion::arrow::array::{Array, BooleanArray, Int64Array, StringArray};

    fn test_table(columns: Vec<Column>) -> VirtualTable {
        VirtualTable {
            name: TableName::new("test").unwrap(),
            description: None,
            columns,
            endpoint: ApiEndpoint {
                method: HttpMethod::Get,
                path: PathTemplate::new("/test").unwrap(),
                base_url: BaseUrl::new("https://example.com").unwrap(),
                accept: AcceptHeader::new("application/json").unwrap(),
                response_wrapper_key: None,
            },
        }
    }

    fn response_col(name: &str, col_type: ColumnType) -> Column {
        Column {
            name: ColumnName::new(name).unwrap(),
            col_type,
            nullable: true,
            description: None,
            role: ColumnRole::ResponseField,
            api_name: None,
        }
    }

    fn path_param_col(name: &str, col_type: ColumnType) -> Column {
        Column {
            name: ColumnName::new(name).unwrap(),
            col_type,
            nullable: false,
            description: None,
            role: ColumnRole::PathParam,
            api_name: Some(ApiParamName::new(name).unwrap()),
        }
    }

    #[test]
    fn string_column_from_json() {
        let cols = vec![response_col("title", ColumnType::String)];
        let table = test_table(cols.clone());
        let schema = virtual_table_to_schema(&table);
        let json = serde_json::json!([{"title": "hello"}, {"title": "world"}]);
        let params = HashMap::new();
        let batch = json_response_to_batch(&json, &cols, &params, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "hello");
        assert_eq!(arr.value(1), "world");
    }

    #[test]
    fn integer_column_from_json() {
        let cols = vec![response_col("count", ColumnType::Integer)];
        let table = test_table(cols.clone());
        let schema = virtual_table_to_schema(&table);
        let json = serde_json::json!([{"count": 42}, {"count": 0}]);
        let params = HashMap::new();
        let batch = json_response_to_batch(&json, &cols, &params, &schema).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 42);
        assert_eq!(arr.value(1), 0);
    }

    #[test]
    fn integer_path_param_produces_value() {
        // Regression test for Bug 1: integer path params must not be NULL
        let cols = vec![
            path_param_col("number", ColumnType::Integer),
            response_col("title", ColumnType::String),
        ];
        let table = test_table(cols.clone());
        let schema = virtual_table_to_schema(&table);
        let json = serde_json::json!([{"title": "fix bug", "number": 42}]);
        let mut params = HashMap::new();
        params.insert(
            ColumnName::new("number").unwrap(),
            ScalarValue::Int64(Some(42)),
        );
        let batch = json_response_to_batch(&json, &cols, &params, &schema).unwrap();
        let num_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(!num_arr.is_null(0), "integer path param should not be NULL");
        assert_eq!(num_arr.value(0), 42);
    }

    #[test]
    fn boolean_column_from_json() {
        let cols = vec![response_col("active", ColumnType::Boolean)];
        let table = test_table(cols.clone());
        let schema = virtual_table_to_schema(&table);
        let json = serde_json::json!([{"active": true}, {"active": false}]);
        let params = HashMap::new();
        let batch = json_response_to_batch(&json, &cols, &params, &schema).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(arr.value(0));
        assert!(!arr.value(1));
    }

    #[test]
    fn nested_field_flattening() {
        let cols = vec![response_col("user_login", ColumnType::String)];
        let table = test_table(cols.clone());
        let schema = virtual_table_to_schema(&table);
        let json = serde_json::json!([{"user": {"login": "octocat"}}]);
        let params = HashMap::new();
        let batch = json_response_to_batch(&json, &cols, &params, &schema).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "octocat");
    }

    #[test]
    fn key_map_separator_no_false_prefix_match() {
        // Regression test for Bug 3: JSON key "u" with nested "login" should match
        // column "u_login" (explicit _ separator), but NOT column "user_login".
        // The old code used starts_with without verifying the separator character.
        let cols = vec![response_col("user_login", ColumnType::String)];
        let table = test_table(cols.clone());
        let schema = virtual_table_to_schema(&table);
        // JSON has "u" with nested "login" — should NOT match "user_login"
        let json = serde_json::json!([{"u": {"login": "ghost"}}]);
        let params = HashMap::new();
        let batch = json_response_to_batch(&json, &cols, &params, &schema).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(arr.is_null(0), "u.login should not match column user_login");
    }

    #[test]
    fn key_map_separator_correct_match() {
        // "user" with nested "login" SHOULD match column "user_login"
        let cols = vec![response_col("user_login", ColumnType::String)];
        let table = test_table(cols.clone());
        let schema = virtual_table_to_schema(&table);
        let json = serde_json::json!([{"user": {"login": "octocat"}}]);
        let params = HashMap::new();
        let batch = json_response_to_batch(&json, &cols, &params, &schema).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "octocat");
    }

    #[test]
    fn timestamp_parses_to_arrow() {
        let cols = vec![response_col("created_at", ColumnType::Timestamp)];
        let table = test_table(cols.clone());
        let schema = virtual_table_to_schema(&table);
        let json = serde_json::json!([{"created_at": "2024-01-15T10:30:00Z"}]);
        let params = HashMap::new();
        let batch = json_response_to_batch(&json, &cols, &params, &schema).unwrap();
        assert!(!batch.column(0).is_null(0), "timestamp should not be NULL");
        // Verify it's actually a Timestamp type, not Utf8
        assert!(
            matches!(batch.column(0).data_type(), DataType::Timestamp(_, _)),
            "expected Timestamp data type"
        );
    }

    #[test]
    fn missing_json_field_is_null() {
        let cols = vec![response_col("missing", ColumnType::String)];
        let table = test_table(cols.clone());
        let schema = virtual_table_to_schema(&table);
        let json = serde_json::json!([{"other_field": "value"}]);
        let params = HashMap::new();
        let batch = json_response_to_batch(&json, &cols, &params, &schema).unwrap();
        assert!(batch.column(0).is_null(0));
    }

    #[test]
    fn unexpected_json_shape_is_error() {
        let cols = vec![response_col("title", ColumnType::String)];
        let table = test_table(cols.clone());
        let schema = virtual_table_to_schema(&table);
        let params = HashMap::new();

        for (json, expected_label) in [
            (serde_json::Value::Null, "null"),
            (serde_json::Value::Bool(true), "boolean"),
            (serde_json::json!(42), "number"),
            (serde_json::json!("a string"), "string"),
        ] {
            let err = json_response_to_batch(&json, &cols, &params, &schema)
                .expect_err(&format!("expected error for JSON {expected_label}"));
            let msg = err.to_string();
            assert!(
                msg.contains(expected_label),
                "error for {expected_label} should mention the type, got: {msg}"
            );
        }
    }
}
