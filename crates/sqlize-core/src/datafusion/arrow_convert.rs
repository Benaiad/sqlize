use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, BooleanBuilder, Float32Array, Float64Array, Float64Builder,
    Int16Array, Int32Array, Int64Array, Int64Builder, Int8Array, LargeStringArray, RecordBatch,
    StringArray, StringBuilder, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::DataFusionError;

use crate::catalog::types::{
    Column, ColumnName, ColumnType, ResultSet, Row, Scalar, VirtualTable, sanitize_name,
};

/// Map a sqlize `ColumnType` to an Arrow `DataType`.
pub fn column_type_to_arrow(ct: &ColumnType) -> DataType {
    match ct {
        ColumnType::String => DataType::Utf8,
        ColumnType::Integer => DataType::Int64,
        ColumnType::Float => DataType::Float64,
        ColumnType::Boolean => DataType::Boolean,
        ColumnType::Timestamp => DataType::Utf8,
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

/// Convert a JSON API response into an Arrow `RecordBatch`.
pub fn json_response_to_batch(
    json: &serde_json::Value,
    columns: &[Column],
    param_values: &HashMap<ColumnName, String>,
    schema: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let items = match json {
        serde_json::Value::Array(arr) => arr.as_slice(),
        serde_json::Value::Object(_) => std::slice::from_ref(json),
        _ => &[],
    };

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
                    let val = extract_value(item, col, param_values);
                    match val {
                        Scalar::Null => builder.append_null(),
                        Scalar::String(s) => builder.append_value(&s),
                        Scalar::Json(j) => builder.append_value(j.to_string()),
                        other => builder.append_value(other.to_string()),
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for item in items {
                    let val = extract_value(item, col, param_values);
                    match val {
                        Scalar::Integer(n) => builder.append_value(n),
                        Scalar::Null => builder.append_null(),
                        _ => builder.append_null(),
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for item in items {
                    let val = extract_value(item, col, param_values);
                    match val {
                        Scalar::Float(n) => builder.append_value(n),
                        Scalar::Integer(n) => builder.append_value(n as f64),
                        Scalar::Null => builder.append_null(),
                        _ => builder.append_null(),
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(num_rows);
                for item in items {
                    let val = extract_value(item, col, param_values);
                    match val {
                        Scalar::Boolean(b) => builder.append_value(b),
                        Scalar::Null => builder.append_null(),
                        _ => builder.append_null(),
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            _ => {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for item in items {
                    let val = extract_value(item, col, param_values);
                    match val {
                        Scalar::Null => builder.append_null(),
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
fn extract_value(
    item: &serde_json::Value,
    col: &Column,
    param_values: &HashMap<ColumnName, String>,
) -> Scalar {
    if let Some(map) = item.as_object() {
        let col_name = col.name.as_str();
        if let Some(v) = find_in_json(map, col_name) {
            return json_value_to_scalar(v);
        }
    }

    if let Some(v) = param_values.get(&col.name) {
        return Scalar::String(v.clone());
    }

    Scalar::Null
}

/// Find a value in a JSON object, handling one level of flattening.
fn find_in_json<'a>(
    map: &'a serde_json::Map<String, serde_json::Value>,
    col_name: &str,
) -> Option<&'a serde_json::Value> {
    for (key, value) in map {
        let sanitized = sanitize_name(key);
        if sanitized == col_name {
            return Some(value);
        }
    }

    for (key, value) in map {
        if let serde_json::Value::Object(nested) = value {
            let prefix = sanitize_name(key);
            if col_name.starts_with(&prefix) && col_name.len() > prefix.len() {
                let suffix = &col_name[prefix.len() + 1..];
                for (nk, nv) in nested {
                    if sanitize_name(nk) == suffix {
                        return Some(nv);
                    }
                }
            }
        }
    }

    None
}

fn json_value_to_scalar(v: &serde_json::Value) -> Scalar {
    match v {
        serde_json::Value::Null => Scalar::Null,
        serde_json::Value::Bool(b) => Scalar::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Scalar::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Scalar::Float(f)
            } else {
                Scalar::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => Scalar::String(s.clone()),
        other => Scalar::Json(other.clone()),
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
            ColumnName::new(f.name()).unwrap_or_else(|_| ColumnName::new("_unknown").unwrap())
        })
        .collect();

    let mut rows = Vec::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let values: Vec<Scalar> = batch
                .columns()
                .iter()
                .map(|col| arrow_value_to_scalar(col, row_idx))
                .collect();
            rows.push(Row::new(values));
        }
    }

    ResultSet { columns, rows }
}

fn arrow_value_to_scalar(array: &ArrayRef, idx: usize) -> Scalar {
    if array.is_null(idx) {
        return Scalar::Null;
    }

    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Scalar::String(arr.value(idx).to_owned())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Scalar::String(arr.value(idx).to_owned())
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Scalar::Integer(arr.value(idx) as i64)
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Scalar::Integer(arr.value(idx) as i64)
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Scalar::Integer(arr.value(idx) as i64)
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Scalar::Integer(arr.value(idx))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Scalar::Integer(arr.value(idx) as i64)
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Scalar::Integer(arr.value(idx) as i64)
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Scalar::Integer(arr.value(idx) as i64)
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Scalar::Integer(arr.value(idx) as i64)
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Scalar::Float(arr.value(idx) as f64)
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Scalar::Float(arr.value(idx))
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Scalar::Boolean(arr.value(idx))
        }
        _ => {
            // Fallback: render as string via Display
            Scalar::String(format!("{}", datafusion::arrow::util::display::array_value_to_string(array, idx).unwrap_or_default()))
        }
    }
}
