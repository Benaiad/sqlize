use std::fmt::Write;

use super::Catalog;
use super::types::{ColumnOrigin, VirtualTable};

/// Generate a `CREATE TABLE` DDL statement for a single virtual table.
/// Includes column descriptions as inline comments for LLM consumption.
pub fn table_ddl(table: &VirtualTable) -> String {
    let mut out = String::with_capacity(512);

    // Table-level comment
    if !table.description.is_empty() {
        writeln!(out, "-- {}", table.description).unwrap();
    }

    // Note required WHERE params
    let required: Vec<_> = table.required_params().collect();
    if !required.is_empty() {
        let names: Vec<_> = required.iter().map(|c| c.name.as_str()).collect();
        writeln!(
            out,
            "-- Required WHERE clause: {}",
            names.join(" AND ")
        )
        .unwrap();
    }

    writeln!(out, "CREATE TABLE {} (", table.name).unwrap();

    for (i, col) in table.columns.iter().enumerate() {
        let trailing_comma = if i + 1 < table.columns.len() {
            ","
        } else {
            ""
        };
        let nullable = if col.nullable { "" } else { " NOT NULL" };

        let origin_tag = match &col.origin {
            ColumnOrigin::PathParam => " [required param]",
            ColumnOrigin::QueryParam { .. } => " [filterable]",
            ColumnOrigin::QueryParamAndResponseField { .. } => " [filterable]",
            ColumnOrigin::ResponseField => "",
        };

        let comment = match &col.description {
            Some(desc) => format!(" -- {desc}{origin_tag}"),
            None if !origin_tag.is_empty() => format!(" --{origin_tag}"),
            None => String::new(),
        };

        writeln!(
            out,
            "    {name} {typ}{nullable}{trailing_comma}{comment}",
            name = col.name,
            typ = col.col_type,
        )
        .unwrap();
    }

    writeln!(out, ");").unwrap();
    out
}

/// Generate DDL for the entire catalog — all tables, separated by blank lines.
pub fn catalog_ddl(catalog: &Catalog) -> String {
    catalog
        .tables()
        .map(table_ddl)
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::types::*;

    fn test_table() -> VirtualTable {
        VirtualTable {
            name: TableName::new("issues").unwrap(),
            description: "Repository issues".to_owned(),
            columns: vec![
                Column {
                    name: ColumnName::new("owner").unwrap(),
                    col_type: ColumnType::String,
                    nullable: false,
                    description: Some("Repository owner".to_owned()),
                    origin: ColumnOrigin::PathParam,
                },
                Column {
                    name: ColumnName::new("repo").unwrap(),
                    col_type: ColumnType::String,
                    nullable: false,
                    description: Some("Repository name".to_owned()),
                    origin: ColumnOrigin::PathParam,
                },
                Column {
                    name: ColumnName::new("id").unwrap(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                    description: Some("Issue ID".to_owned()),
                    origin: ColumnOrigin::ResponseField,
                },
                Column {
                    name: ColumnName::new("title").unwrap(),
                    col_type: ColumnType::String,
                    nullable: false,
                    description: Some("Issue title".to_owned()),
                    origin: ColumnOrigin::ResponseField,
                },
                Column {
                    name: ColumnName::new("state").unwrap(),
                    col_type: ColumnType::String,
                    nullable: false,
                    description: Some("open or closed".to_owned()),
                    origin: ColumnOrigin::QueryParam { api_name: None },
                },
            ],
            endpoint: ApiEndpoint {
                method: HttpMethod::Get,
                path: PathTemplate::new("/repos/{owner}/{repo}/issues").unwrap(),
                base_url: "https://api.github.com".to_owned(),
                accept: "application/json".to_owned(),
                data_path: None,
            },
        }
    }

    #[test]
    fn ddl_contains_table_name_and_columns() {
        let ddl = table_ddl(&test_table());
        assert!(ddl.contains("CREATE TABLE issues ("));
        assert!(ddl.contains("owner TEXT NOT NULL"));
        assert!(ddl.contains("title TEXT NOT NULL"));
        assert!(ddl.contains("[required param]"));
        assert!(ddl.contains("[filterable]"));
    }

    #[test]
    fn ddl_contains_required_where_comment() {
        let ddl = table_ddl(&test_table());
        assert!(ddl.contains("Required WHERE clause: owner AND repo"));
    }
}
