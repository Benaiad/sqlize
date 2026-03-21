use std::sync::Arc;

use rustyline::DefaultEditor;
use tabled::builder::Builder;
use tabled::settings::{Style, Width};

use sqlize_core::catalog::Catalog;
use sqlize_core::catalog::ddl::{catalog_ddl, single_table_ddl};
use sqlize_core::catalog::types::{TableName, Value};
use sqlize_core::exec::{AuthConfig, execute};
use sqlize_core::output::{result_set_to_json, result_set_to_toon};
use sqlize_core::sql::planner::{explain, plan_query};

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum OutputFormat {
    Table,
    Json,
    Toon,
}

pub async fn run(catalog: Arc<Catalog>, auth: AuthConfig, format: OutputFormat) {
    eprintln!("Type SQL to query, or: SHOW TABLES, DESCRIBE <table>, EXPLAIN <sql>");
    eprintln!("Ctrl+D to exit.\n");

    let mut rl = DefaultEditor::new().expect("failed to initialize readline");

    loop {
        let line = match rl.readline("sqlize> ") {
            Ok(line) => line,
            Err(rustyline::error::ReadlineError::Eof) => break,
            Err(rustyline::error::ReadlineError::Interrupted) => continue,
            Err(e) => {
                eprintln!("readline error: {e}");
                break;
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let _ = rl.add_history_entry(trimmed);

        let upper = trimmed.to_ascii_uppercase();
        if upper == "SHOW TABLES" || upper == "\\D" {
            handle_show_tables(&catalog);
        } else if upper.starts_with("DESCRIBE ") || upper.starts_with("\\D ") {
            let table_name = trimmed.split_whitespace().nth(1).unwrap_or("");
            handle_describe(&catalog, table_name);
        } else if upper.starts_with("EXPLAIN ") {
            let sql = &trimmed[8..];
            handle_explain(&catalog, sql);
        } else if upper == "SCHEMA" || upper == "DDL" {
            println!("{}", catalog_ddl(&catalog));
        } else if upper == "QUIT" || upper == "EXIT" || upper == "\\Q" {
            break;
        } else {
            handle_query(&catalog, &auth, trimmed, format).await;
        }
    }
}

fn handle_show_tables(catalog: &Catalog) {
    let mut builder = Builder::default();
    builder.push_record(["table", "columns", "required", "description"]);

    for table in catalog.tables() {
        let required: Vec<_> = table.required_params().map(|c| c.name.as_str()).collect();
        builder.push_record([
            table.name.as_str(),
            &table.columns.len().to_string(),
            &if required.is_empty() {
                "-".to_owned()
            } else {
                required.join(", ")
            },
            &table.description,
        ]);
    }

    let mut tbl = builder.build();
    tbl.with(Style::rounded());
    tbl.with(Width::wrap(term_width()).keep_words(true));
    println!("{tbl}");
}

fn handle_describe(catalog: &Catalog, name: &str) {
    let Ok(table_name) = TableName::new(name) else {
        eprintln!("Invalid table name: {name}");
        return;
    };

    let Some(table) = catalog.get(&table_name) else {
        eprintln!("Table not found: {name}");
        return;
    };

    println!("{}", single_table_ddl(table));
}

fn handle_explain(catalog: &Catalog, sql: &str) {
    match plan_query(sql, catalog) {
        Ok(plan) => println!("{}", explain(&plan)),
        Err(e) => eprintln!("Error: {e}"),
    }
}

async fn handle_query(catalog: &Catalog, auth: &AuthConfig, sql: &str, format: OutputFormat) {
    let plan = match plan_query(sql, catalog) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Error: {e}");
            return;
        }
    };

    let result = match execute(&plan, auth).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: {e}");
            return;
        }
    };

    let row_count = result.rows.len();

    match format {
        OutputFormat::Table => {
            let mut builder = Builder::default();
            let headers: Vec<&str> = result.columns.iter().map(|c| c.as_str()).collect();
            builder.push_record(headers);

            for row in &result.rows {
                let values: Vec<String> = row.0.iter().map(format_value).collect();
                builder.push_record(values);
            }

            let mut tbl = builder.build();
            tbl.with(Style::rounded());
            tbl.with(Width::wrap(term_width()).keep_words(true));
            println!("{tbl}");
        }
        OutputFormat::Json => {
            println!("{}", result_set_to_json(&result));
        }
        OutputFormat::Toon => match result_set_to_toon(&result) {
            Ok(toon) => println!("{toon}"),
            Err(e) => eprintln!("TOON encoding error: {e}"),
        },
    }

    eprintln!("({row_count} rows)");
}

fn format_value(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_owned(),
        Value::String(s) => s.clone(),
        Value::Integer(n) => n.to_string(),
        Value::Float(n) => format!("{n:.2}"),
        Value::Boolean(b) => b.to_string(),
        Value::Json(j) => j.to_string(),
    }
}

fn term_width() -> usize {
    terminal_size::terminal_size()
        .map(|(w, _)| w.0 as usize)
        .unwrap_or(120)
}
