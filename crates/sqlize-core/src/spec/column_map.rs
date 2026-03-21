use openapiv3::{OpenAPI, ReferenceOr, Schema, SchemaKind, Type as OaType};

use crate::catalog::types::{Column, ColumnName, ColumnOrigin, ColumnType, sanitize_name};
use crate::error::Result;

/// Extract columns from a response schema, flattening one level of nesting.
///
/// Given a schema like `{ id: integer, title: string, user: { login: string, id: integer } }`,
/// produces columns: `id`, `title`, `user_login`, `user_id`.
///
/// Deeply nested objects and arrays become `ColumnType::Json`.
pub fn columns_from_schema(
    spec: &OpenAPI,
    schema: &Schema,
    prefix: &str,
) -> Result<Vec<Column>> {
    let mut columns = Vec::new();
    collect_columns(spec, schema, prefix, 0, &mut columns)?;
    Ok(columns)
}

fn collect_columns(
    spec: &OpenAPI,
    schema: &Schema,
    prefix: &str,
    depth: usize,
    out: &mut Vec<Column>,
) -> Result<()> {
    let required_fields = required_set(schema);

    match &schema.schema_kind {
        SchemaKind::Type(OaType::Object(obj)) => {
            for (name, prop_ref) in &obj.properties {
                let col_name_str = if prefix.is_empty() {
                    sanitize_name(name)
                } else {
                    format!("{prefix}_{}", sanitize_name(name))
                };

                let Some(resolved) = resolve_boxed_schema(spec, prop_ref) else {
                    // Unresolvable ref — skip
                    continue;
                };

                let is_required = required_fields.contains(&name.as_str());

                if depth < 1 && is_nested_object(resolved) {
                    // Flatten one level deep
                    collect_columns(spec, resolved, &col_name_str, depth + 1, out)?;
                } else {
                    let col_type = schema_to_column_type(resolved);
                    let description = schema_description(resolved);

                    if let Ok(col_name) = ColumnName::new(&col_name_str) {
                        out.push(Column {
                            name: col_name,
                            col_type,
                            nullable: !is_required,
                            description,
                            origin: ColumnOrigin::ResponseField,
                        });
                    }
                    // Skip columns with names that don't validate (rare edge cases)
                }
            }
        }
        // For non-object schemas at the top level, there's nothing to flatten.
        _ => {}
    }

    Ok(())
}

fn is_nested_object(schema: &Schema) -> bool {
    matches!(&schema.schema_kind, SchemaKind::Type(OaType::Object(_)))
}

fn schema_to_column_type(schema: &Schema) -> ColumnType {
    match &schema.schema_kind {
        SchemaKind::Type(OaType::String(s)) => {
            if s.format == openapiv3::VariantOrUnknownOrEmpty::Item(openapiv3::StringFormat::DateTime) {
                ColumnType::Timestamp
            } else {
                ColumnType::String
            }
        }
        SchemaKind::Type(OaType::Integer(_)) => ColumnType::Integer,
        SchemaKind::Type(OaType::Number(_)) => ColumnType::Float,
        SchemaKind::Type(OaType::Boolean(_)) => ColumnType::Boolean,
        SchemaKind::Type(OaType::Array(_)) => ColumnType::Json,
        SchemaKind::Type(OaType::Object(_)) => ColumnType::Json,
        _ => ColumnType::Json,
    }
}

fn schema_description(schema: &Schema) -> Option<String> {
    schema
        .schema_data
        .description
        .as_ref()
        .map(|d| truncate_description(d, 120))
}

fn truncate_description(s: &str, max: usize) -> String {
    let first_line = s.lines().next().unwrap_or(s);
    if first_line.len() <= max {
        first_line.to_owned()
    } else {
        format!("{}...", &first_line[..max - 3])
    }
}

fn required_set(schema: &Schema) -> std::collections::HashSet<&str> {
    match &schema.schema_kind {
        SchemaKind::Type(OaType::Object(obj)) => {
            obj.required.iter().map(|s| s.as_str()).collect()
        }
        _ => std::collections::HashSet::new(),
    }
}

/// Resolve a `ReferenceOr<Schema>` into a concrete `&Schema`.
/// Follows `$ref` pointers into `components.schemas`.
pub fn resolve_schema<'a>(
    spec: &'a OpenAPI,
    schema_ref: &'a ReferenceOr<Schema>,
) -> Option<&'a Schema> {
    match schema_ref {
        ReferenceOr::Item(schema) => Some(schema),
        ReferenceOr::Reference { reference } => resolve_ref(spec, reference),
    }
}

/// Same as `resolve_schema` but for `ReferenceOr<Box<Schema>>`,
/// which `openapiv3` uses for nested properties and array items.
pub fn resolve_boxed_schema<'a>(
    spec: &'a OpenAPI,
    schema_ref: &'a ReferenceOr<Box<Schema>>,
) -> Option<&'a Schema> {
    match schema_ref {
        ReferenceOr::Item(schema) => Some(schema.as_ref()),
        ReferenceOr::Reference { reference } => resolve_ref(spec, reference),
    }
}

fn resolve_ref<'a>(spec: &'a OpenAPI, reference: &str) -> Option<&'a Schema> {
    let name = reference.strip_prefix("#/components/schemas/")?;
    let components = spec.components.as_ref()?;
    let entry = components.schemas.get(name)?;
    match entry {
        ReferenceOr::Item(schema) => Some(schema),
        // Don't follow chains of refs to keep this simple and avoid cycles.
        ReferenceOr::Reference { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_camel_case() {
        assert_eq!(sanitize_name("createdAt"), "created_at");
        assert_eq!(sanitize_name("userId"), "user_id");
        assert_eq!(sanitize_name("HTMLParser"), "h_t_m_l_parser");
    }

    #[test]
    fn sanitize_hyphens() {
        assert_eq!(sanitize_name("pull-request"), "pull_request");
    }

    #[test]
    fn sanitize_already_snake() {
        assert_eq!(sanitize_name("created_at"), "created_at");
    }
}
