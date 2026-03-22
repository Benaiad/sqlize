use openapiv3::{
    OpenAPI, Operation, Parameter, ParameterSchemaOrContent, PathItem, ReferenceOr, SchemaKind,
    StatusCode, Type as OaType,
};

use crate::catalog::types::{
    ApiEndpoint, ApiParamName, Column, ColumnName, ColumnRole, ColumnType, HttpMethod,
    PathTemplate, VirtualTable,
};
use crate::error::{Error, Result};

use super::column_map::{columns_from_schema, resolve_boxed_schema, resolve_schema};

/// Generate virtual tables from all GET list-endpoints in the spec.
///
/// Only considers GET operations that return an array (list endpoints).
/// Single-resource endpoints (GET /repos/{owner}/{repo}) are skipped
/// because they map to keyed access on the list table.
///
/// When multiple paths produce the same table name, a disambiguated name
/// is constructed from the path context (e.g., `git_branches` vs `branches`).
pub fn tables_from_spec(
    spec: &OpenAPI,
    base_url: &str,
    tag_filter: Option<&[&str]>,
) -> Result<Vec<VirtualTable>> {
    // First pass: collect (path, table) pairs and detect name collisions.
    let mut candidates: Vec<(String, VirtualTable)> = Vec::new();

    for (path_str, path_item_ref) in &spec.paths.paths {
        let path_item = match path_item_ref {
            ReferenceOr::Item(item) => item,
            ReferenceOr::Reference { .. } => continue,
        };

        if let Some(get_op) = &path_item.get {
            if !matches_tag_filter(get_op, tag_filter) {
                continue;
            }

            if let Some(table) = try_build_table(spec, path_str, path_item, get_op, base_url)? {
                candidates.push((path_str.clone(), table));
            }
        }
    }

    // Detect name collisions, then qualify ALL colliding names (not just the second).
    // This makes the result deterministic regardless of spec path ordering.
    let mut name_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    for (_, table) in &candidates {
        *name_counts
            .entry(table.name.as_str().to_owned())
            .or_default() += 1;
    }

    let mut tables = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    for (path_str, mut table) in candidates {
        if name_counts.get(table.name.as_str()).copied().unwrap_or(1) > 1 {
            if let Ok(qualified) = derive_qualified_table_name(&path_str) {
                table.name = qualified;
            }
        }

        // Final dedup: numeric suffix if still colliding
        let mut final_name = table.name.as_str().to_owned();
        let mut counter = 2;
        while seen.contains(&final_name) {
            final_name = format!("{}_{counter}", table.name.as_str());
            counter += 1;
        }
        if final_name != table.name.as_str() {
            if let Ok(suffixed) = crate::catalog::types::TableName::new(&final_name) {
                table.name = suffixed;
            }
        }

        seen.insert(table.name.as_str().to_owned());
        tables.push(table);
    }

    tables.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(tables)
}

fn matches_tag_filter(op: &Operation, filter: Option<&[&str]>) -> bool {
    let Some(allowed) = filter else {
        return true;
    };
    op.tags.iter().any(|tag| allowed.contains(&tag.as_str()))
}

/// Try to build a virtual table from a GET operation.
/// Returns `None` if the endpoint isn't a list endpoint (doesn't return an array).
fn try_build_table(
    spec: &OpenAPI,
    path_str: &str,
    path_item: &PathItem,
    operation: &Operation,
    base_url: &str,
) -> Result<Option<VirtualTable>> {
    // Find the success response schema and its content type
    let Some((item_schema, content_type, data_path)) = extract_list_item_schema(spec, operation)
    else {
        return Ok(None);
    };

    let table_name = derive_table_name(path_str)?;
    let path_template = PathTemplate::new(path_str).map_err(|_| Error::InvalidPath {
        path: path_str.to_owned(),
        reason: "invalid path template",
    })?;

    // Build columns from three sources:
    // 1. Path parameters → ColumnRole::PathParam
    // 2. Query parameters → ColumnRole::QueryParam
    // 3. Response schema → ColumnRole::ResponseField
    let mut columns = Vec::new();

    // Path parameters from both the path item and the operation
    let all_params = merge_parameters(spec, path_item, operation);
    for param in &all_params {
        if let Some(col) = param_to_column(spec, param)? {
            columns.push(col);
        }
    }

    // Response fields — when a response field shares a name with a query param,
    // upgrade the param column to QueryParamAndResponseField and use the response
    // field's metadata (description, nullability). This handles columns like `state`
    // which is both filterable and present in the response.
    let response_columns = columns_from_schema(spec, item_schema, "")?;
    for resp_col in response_columns {
        if let Some(existing) = columns.iter_mut().find(|c| c.name == resp_col.name) {
            if matches!(existing.role, ColumnRole::QueryParam) {
                existing.role = ColumnRole::QueryParamAndResponse;
                // Prefer the response field's metadata — it describes the value,
                // not the filter semantics.
                if resp_col.description.is_some() {
                    existing.description = resp_col.description;
                }
                existing.nullable = resp_col.nullable;
            }
        } else {
            columns.push(resp_col);
        }
    }

    let description = operation
        .summary
        .as_deref()
        .or(operation.description.as_deref())
        .unwrap_or("")
        .lines()
        .next()
        .unwrap_or("")
        .to_owned();

    Ok(Some(VirtualTable {
        name: table_name,
        description,
        columns,
        endpoint: ApiEndpoint {
            method: HttpMethod::Get,
            path: path_template,
            base_url: base_url.to_owned(),
            accept: content_type,
            data_path,
        },
    }))
}

/// Extract the item schema from a list endpoint's response.
///
/// Returns (item_schema, content_type, data_path) where:
/// - `item_schema` is the schema of each array element
/// - `content_type` is the Accept header value
/// - `data_path` is `None` for top-level arrays, or `Some("field")` for
///   wrapped responses like `{"data": [...]}`
///
/// Handles two response shapes:
/// 1. Top-level array: `[{...}, {...}]`
/// 2. Wrapped array: `{"data": [{...}], "has_more": true}` — finds the
///    object property whose type is `array` and extracts its items schema.
fn extract_list_item_schema<'a>(
    spec: &'a OpenAPI,
    operation: &'a Operation,
) -> Option<(&'a openapiv3::Schema, String, Option<String>)> {
    let response_ref = operation.responses.responses.get(&StatusCode::Code(200))?;

    let response = match response_ref {
        ReferenceOr::Item(r) => r,
        ReferenceOr::Reference { reference } => {
            let name = reference.strip_prefix("#/components/responses/")?;
            let components = spec.components.as_ref()?;
            match components.responses.get(name)? {
                ReferenceOr::Item(r) => r,
                ReferenceOr::Reference { .. } => return None,
            }
        }
    };

    let (content_type, media) = response
        .content
        .iter()
        .find(|(ct, _)| is_json_content_type(ct))?;

    let schema_ref = media.schema.as_ref()?;
    let schema = resolve_schema(spec, schema_ref)?;

    // Case 1: top-level array
    if let SchemaKind::Type(OaType::Array(arr)) = &schema.schema_kind {
        let items_ref = arr.items.as_ref()?;
        let item_schema = resolve_boxed_schema(spec, items_ref)?;
        return Some((item_schema, content_type.clone(), None));
    }

    // Case 2: wrapped array — object with a property that's an array of objects
    if let SchemaKind::Type(OaType::Object(obj)) = &schema.schema_kind {
        for (field_name, prop_ref) in &obj.properties {
            let Some(prop_schema) = resolve_boxed_schema(spec, prop_ref) else {
                continue;
            };
            if let SchemaKind::Type(OaType::Array(arr)) = &prop_schema.schema_kind {
                let items_ref = arr.items.as_ref()?;
                let item_schema = resolve_boxed_schema(spec, items_ref)?;
                return Some((item_schema, content_type.clone(), Some(field_name.clone())));
            }
        }
    }

    None
}

/// Check if a content type string represents JSON.
/// Matches `application/json`, `application/vnd.github+json`, etc.
fn is_json_content_type(ct: &str) -> bool {
    ct == "application/json" || ct.ends_with("+json")
}

/// Derive a table name from an API path.
///
/// Strategy: take the last non-parameter segment.
/// `/repos/{owner}/{repo}/issues` → `issues`
/// `/repos/{owner}/{repo}/pulls` → `pulls`
/// `/orgs/{org}/members` → `members`
fn derive_table_name(path: &str) -> Result<crate::catalog::types::TableName> {
    let segments = static_segments(path);

    let raw_name = segments
        .last()
        .ok_or_else(|| Error::TableNameDerivation(path.to_owned()))?;

    let sanitized = raw_name.replace('-', "_").to_ascii_lowercase();

    crate::catalog::types::TableName::new(&sanitized)
        .map_err(|_| Error::TableNameDerivation(path.to_owned()))
}

/// Derive a qualified table name using parent context for disambiguation.
///
/// `/repos/{owner}/{repo}/git/branches` → `git_branches`
/// `/repos/{owner}/{repo}/branches` → `repo_branches`
fn derive_qualified_table_name(path: &str) -> Result<crate::catalog::types::TableName> {
    let segments = static_segments(path);

    if segments.len() >= 2 {
        let parent = segments[segments.len() - 2]
            .replace('-', "_")
            .to_ascii_lowercase();
        let child = segments[segments.len() - 1]
            .replace('-', "_")
            .to_ascii_lowercase();
        let qualified = format!("{parent}_{child}");
        crate::catalog::types::TableName::new(&qualified)
            .map_err(|_| Error::TableNameDerivation(path.to_owned()))
    } else {
        derive_table_name(path)
    }
}

fn static_segments(path: &str) -> Vec<&str> {
    path.split('/')
        .filter(|s| !s.is_empty() && !s.starts_with('{'))
        .collect()
}

/// Merge path-item-level and operation-level parameters.
/// Operation-level parameters override path-item-level ones with the same name.
/// Resolves `$ref` through `components.parameters`.
fn merge_parameters<'a>(
    spec: &'a OpenAPI,
    path_item: &'a PathItem,
    operation: &'a Operation,
) -> Vec<&'a Parameter> {
    let mut by_name: std::collections::HashMap<&str, &Parameter> = std::collections::HashMap::new();

    for param_ref in &path_item.parameters {
        if let Some(param) = resolve_parameter(spec, param_ref) {
            by_name.insert(param_name(param), param);
        }
    }

    for param_ref in &operation.parameters {
        if let Some(param) = resolve_parameter(spec, param_ref) {
            by_name.insert(param_name(param), param);
        }
    }

    // Filter out pagination params — we handle those internally
    by_name
        .into_values()
        .filter(|p| !is_pagination_param(p))
        .collect()
}

fn param_name(param: &Parameter) -> &str {
    match param {
        Parameter::Query { parameter_data, .. } => &parameter_data.name,
        Parameter::Path { parameter_data, .. } => &parameter_data.name,
        Parameter::Header { parameter_data, .. } => &parameter_data.name,
        Parameter::Cookie { parameter_data, .. } => &parameter_data.name,
    }
}

fn is_pagination_param(param: &Parameter) -> bool {
    let name = param_name(param);
    matches!(name, "page" | "per_page" | "limit" | "offset" | "cursor")
}

/// Resolve a parameter `$ref` through `components.parameters`.
fn resolve_parameter<'a>(
    spec: &'a OpenAPI,
    param_ref: &'a ReferenceOr<Parameter>,
) -> Option<&'a Parameter> {
    match param_ref {
        ReferenceOr::Item(param) => Some(param),
        ReferenceOr::Reference { reference } => {
            let name = reference.strip_prefix("#/components/parameters/")?;
            let components = spec.components.as_ref()?;
            match components.parameters.get(name)? {
                ReferenceOr::Item(param) => Some(param),
                ReferenceOr::Reference { .. } => None,
            }
        }
    }
}

/// Convert an OpenAPI parameter to a Column.
fn param_to_column(spec: &OpenAPI, param: &Parameter) -> Result<Option<Column>> {
    let (data, role) = match param {
        Parameter::Path { parameter_data, .. } => (parameter_data, ColumnRole::PathParam),
        Parameter::Query { parameter_data, .. } => (parameter_data, ColumnRole::QueryParam),
        _ => return Ok(None),
    };

    let col_name_str = crate::catalog::types::sanitize_name(&data.name);
    let col_name = match ColumnName::new(&col_name_str) {
        Ok(n) => n,
        Err(_) => return Ok(None),
    };

    let col_type = param_schema_to_type(spec, &data.format);

    let description = data
        .description
        .as_ref()
        .map(|d| crate::catalog::types::truncate_str(d, 120));

    Ok(Some(Column {
        name: col_name,
        col_type,
        nullable: !data.required,
        description,
        role,
        api_name: Some(ApiParamName::new(data.name.clone())),
    }))
}

fn param_schema_to_type(spec: &OpenAPI, format: &ParameterSchemaOrContent) -> ColumnType {
    match format {
        ParameterSchemaOrContent::Schema(schema_ref) => {
            let Some(schema) = resolve_schema(spec, schema_ref) else {
                return ColumnType::String;
            };
            match &schema.schema_kind {
                SchemaKind::Type(OaType::Integer(_)) => ColumnType::Integer,
                SchemaKind::Type(OaType::Number(_)) => ColumnType::Float,
                SchemaKind::Type(OaType::Boolean(_)) => ColumnType::Boolean,
                SchemaKind::Type(OaType::Array(_)) => ColumnType::Json,
                _ => ColumnType::String,
            }
        }
        ParameterSchemaOrContent::Content(_) => ColumnType::Json,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_name_from_path() {
        let cases = [
            ("/repos/{owner}/{repo}/issues", "issues"),
            ("/repos/{owner}/{repo}/pulls", "pulls"),
            ("/orgs/{org}/members", "members"),
            ("/repos/{owner}/{repo}/check-runs", "check_runs"),
            ("/users", "users"),
        ];
        for (path, expected) in cases {
            let name = derive_table_name(path).unwrap();
            assert_eq!(name.as_str(), expected, "path: {path}");
        }
    }

    #[test]
    fn pagination_params_are_filtered() {
        let param = Parameter::Query {
            parameter_data: openapiv3::ParameterData {
                name: "per_page".to_owned(),
                description: None,
                required: false,
                deprecated: None,
                format: ParameterSchemaOrContent::Schema(ReferenceOr::Item(openapiv3::Schema {
                    schema_data: Default::default(),
                    schema_kind: SchemaKind::Type(OaType::Integer(Default::default())),
                })),
                example: None,
                examples: Default::default(),
                explode: None,
                extensions: Default::default(),
            },
            allow_reserved: false,
            style: openapiv3::QueryStyle::Form,
            allow_empty_value: None,
        };
        assert!(is_pagination_param(&param));
    }
}
