use serde_json::{Map, Value as JsonValue};
use toon_format::{EncodeOptions, encode_array};

use crate::catalog::types::{ResultSet, Value};
use crate::error::{Error, Result};

/// Format a `ResultSet` as TOON — a compact, token-efficient encoding
/// ideal for LLM consumption.
///
/// TOON renders uniform arrays of objects in a CSV-like tabular layout,
/// which is exactly what query results are.
pub fn result_set_to_toon(result: &ResultSet) -> Result<String> {
    let json = result_set_to_json_value(result);
    encode_array(json, &EncodeOptions::new())
        .map_err(|e| Error::Spec(format!("TOON encoding error: {e}")))
}

/// Format a `ResultSet` as JSON (array of objects).
pub fn result_set_to_json(result: &ResultSet) -> String {
    let json = result_set_to_json_value(result);
    // The json value is always an array, safe to serialize
    serde_json::to_string_pretty(&json).unwrap_or_else(|_| "[]".to_owned())
}

fn result_set_to_json_value(result: &ResultSet) -> JsonValue {
    let rows: Vec<JsonValue> = result
        .rows
        .iter()
        .map(|row| {
            let mut obj = Map::with_capacity(result.columns.len());
            for (col, val) in result.columns.iter().zip(row.0.iter()) {
                obj.insert(col.as_str().to_owned(), value_to_json(val));
            }
            JsonValue::Object(obj)
        })
        .collect();

    JsonValue::Array(rows)
}

fn value_to_json(v: &Value) -> JsonValue {
    match v {
        Value::Null => JsonValue::Null,
        Value::String(s) => JsonValue::String(s.clone()),
        Value::Integer(n) => serde_json::json!(n),
        Value::Float(n) => serde_json::json!(n),
        Value::Boolean(b) => JsonValue::Bool(*b),
        Value::Json(j) => j.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::types::{ColumnName, Row};

    fn sample_result() -> ResultSet {
        ResultSet {
            columns: vec![
                ColumnName::new("number").unwrap(),
                ColumnName::new("title").unwrap(),
                ColumnName::new("state").unwrap(),
            ],
            rows: vec![
                Row(vec![
                    Value::Integer(1),
                    Value::String("Fix bug".into()),
                    Value::String("open".into()),
                ]),
                Row(vec![
                    Value::Integer(2),
                    Value::String("Add feature".into()),
                    Value::String("closed".into()),
                ]),
                Row(vec![
                    Value::Integer(3),
                    Value::String("Refactor module".into()),
                    Value::String("open".into()),
                ]),
            ],
        }
    }

    #[test]
    fn toon_output_is_compact() {
        let result = sample_result();
        let toon = result_set_to_toon(&result).unwrap();
        let json = result_set_to_json(&result);

        // TOON should be shorter than JSON
        assert!(
            toon.len() < json.len(),
            "TOON ({} bytes) should be shorter than JSON ({} bytes)\nTOON:\n{toon}\nJSON:\n{json}",
            toon.len(),
            json.len(),
        );
    }

    #[test]
    fn toon_output_is_tabular() {
        let result = sample_result();
        let toon = result_set_to_toon(&result).unwrap();

        // TOON tabular format should contain the header row and pipe-separated values
        assert!(toon.contains("number"), "should contain column name 'number'");
        assert!(toon.contains("Fix bug"), "should contain row value");
    }

    #[test]
    fn json_output_roundtrips() {
        let result = sample_result();
        let json_str = result_set_to_json(&result);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0]["title"], "Fix bug");
    }
}
