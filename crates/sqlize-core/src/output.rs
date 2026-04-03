use serde_json::{Map, Value as JsonValue};
use toon_format::{EncodeOptions, encode_array};

use crate::catalog::types::{ResultSet, ScalarValue, format_scalar};
use crate::error::{Error, Result};

/// Format a `ResultSet` as TOON — a compact, token-efficient encoding
/// ideal for LLM consumption.
///
/// TOON renders uniform arrays of objects in a CSV-like tabular layout,
/// which is exactly what query results are.
pub fn result_set_to_toon(result: &ResultSet) -> Result<String> {
    let json = result_set_to_json_value(result);
    encode_array(json, &EncodeOptions::new()).map_err(|e| Error::ToonEncode(e.to_string()))
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
            for (col, val) in result.columns.iter().zip(row.values().iter()) {
                obj.insert(col.as_str().to_owned(), value_to_json(val));
            }
            JsonValue::Object(obj)
        })
        .collect();

    JsonValue::Array(rows)
}

fn value_to_json(v: &ScalarValue) -> JsonValue {
    if v.is_null() {
        return JsonValue::Null;
    }
    match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            JsonValue::String(s.clone())
        }
        ScalarValue::Int64(Some(n)) => serde_json::json!(n),
        ScalarValue::Float64(Some(n)) => serde_json::json!(n),
        ScalarValue::Boolean(Some(b)) => JsonValue::Bool(*b),
        ScalarValue::TimestampMicrosecond(Some(_), _)
        | ScalarValue::TimestampSecond(Some(_), _)
        | ScalarValue::TimestampMillisecond(Some(_), _)
        | ScalarValue::TimestampNanosecond(Some(_), _) => JsonValue::String(format_scalar(v)),
        other => JsonValue::String(other.to_string()),
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
                Row::new(vec![
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Utf8(Some("Fix bug".into())),
                    ScalarValue::Utf8(Some("open".into())),
                ]),
                Row::new(vec![
                    ScalarValue::Int64(Some(2)),
                    ScalarValue::Utf8(Some("Add feature".into())),
                    ScalarValue::Utf8(Some("closed".into())),
                ]),
                Row::new(vec![
                    ScalarValue::Int64(Some(3)),
                    ScalarValue::Utf8(Some("Refactor module".into())),
                    ScalarValue::Utf8(Some("open".into())),
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
        assert!(
            toon.contains("number"),
            "should contain column name 'number'"
        );
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
