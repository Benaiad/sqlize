use std::borrow::Cow;
use std::sync::Arc;

use crossterm::cursor::SetCursorStyle;
use nu_ansi_term::{Color, Style};
use reedline::{
    ColumnarMenu, Completer, CursorConfig, Emacs, FileBackedHistory, Highlighter, KeyCode,
    KeyModifiers, MenuBuilder, Prompt, PromptEditMode, PromptHistorySearch, Reedline,
    ReedlineEvent, ReedlineMenu, Signal, Span, StyledText, Suggestion, ValidationResult, Validator,
    default_emacs_keybindings,
};
use tabled::builder::Builder;
use tabled::settings::{self, Width};

use sqlize_core::catalog::Catalog;
use sqlize_core::catalog::ddl::{catalog_ddl, table_ddl};
use sqlize_core::catalog::types::{ResultSet, Scalar, TableName, VirtualTable};
use sqlize_core::datafusion::SqlizeContext;
use sqlize_core::output::{result_set_to_json, result_set_to_toon};

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum OutputFormat {
    Table,
    Json,
    Toon,
}

// ---------------------------------------------------------------------------
// CatalogSet — merged view of multiple named catalogs
// ---------------------------------------------------------------------------

/// A collection of named catalogs for multi-spec support.
/// Provides unified SHOW TABLES / DESCRIBE across all specs.
pub struct CatalogSet {
    entries: Vec<(String, Catalog)>,
}

impl CatalogSet {
    pub fn new(catalogs: &[(&str, &Catalog)]) -> Self {
        let entries = catalogs
            .iter()
            .map(|(name, cat)| (name.to_string(), (*cat).clone()))
            .collect();
        Self { entries }
    }

    pub fn is_multi(&self) -> bool {
        self.entries.len() > 1
    }

    /// All tables across all catalogs, with optional schema prefix.
    pub fn all_tables(&self) -> Vec<(&str, &VirtualTable)> {
        let mut tables = Vec::new();
        for (name, catalog) in &self.entries {
            for table in catalog.tables() {
                tables.push((name.as_str(), table));
            }
        }
        tables
    }

    /// Look up a table by name. Supports "schema.table" or bare "table" (searches all).
    pub fn find_table(&self, name: &str) -> Option<(&str, &VirtualTable)> {
        if let Some((schema, table)) = name.split_once('.') {
            // Qualified: schema.table
            for (cat_name, catalog) in &self.entries {
                if cat_name == schema {
                    if let Ok(tn) = TableName::new(table) {
                        if let Some(t) = catalog.get(&tn) {
                            return Some((cat_name, t));
                        }
                    }
                }
            }
            None
        } else {
            // Bare: search all catalogs
            if let Ok(tn) = TableName::new(name) {
                for (cat_name, catalog) in &self.entries {
                    if let Some(t) = catalog.get(&tn) {
                        return Some((cat_name, t));
                    }
                }
            }
            None
        }
    }

    pub fn describe(&self, name: &str) -> Option<String> {
        self.find_table(name).map(|(_, t)| table_ddl(t))
    }

    pub fn full_ddl(&self) -> String {
        let mut out = String::new();
        for (name, catalog) in &self.entries {
            if self.is_multi() {
                out.push_str(&format!("-- Schema: {name}\n\n"));
            }
            out.push_str(&catalog_ddl(catalog));
            out.push('\n');
        }
        out
    }

    /// All table and column names for autocompletion.
    fn completion_words(&self) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut words = Vec::new();

        let mut add = |s: String| {
            if seen.insert(s.to_ascii_uppercase()) {
                words.push(s);
            }
        };

        for kw in SQL_KEYWORDS {
            add(kw.to_string());
            add(kw.to_ascii_lowercase());
        }

        for (schema_name, catalog) in &self.entries {
            for table in catalog.tables() {
                add(table.name.as_str().to_owned());
                // Add qualified name for multi-spec
                if self.is_multi() {
                    add(format!("{}.{}", schema_name, table.name));
                }
                for col in &table.columns {
                    add(col.name.as_str().to_owned());
                }
            }
        }

        for cmd in ["SHOW TABLES", "DESCRIBE", "EXPLAIN", "SCHEMA", "DDL"] {
            add(cmd.to_owned());
        }

        words.sort();
        words
    }
}

impl Clone for CatalogSet {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Prompt
// ---------------------------------------------------------------------------

struct SqlPrompt;

impl Prompt for SqlPrompt {
    fn render_prompt_left(&self) -> Cow<'_, str> {
        Cow::Borrowed("sqlize")
    }

    fn render_prompt_right(&self) -> Cow<'_, str> {
        Cow::Borrowed("")
    }

    fn render_prompt_indicator(&self, _mode: PromptEditMode) -> Cow<'_, str> {
        Cow::Borrowed("> ")
    }

    fn render_prompt_multiline_indicator(&self) -> Cow<'_, str> {
        Cow::Borrowed("     > ")
    }

    fn render_prompt_history_search_indicator(
        &self,
        history_search: PromptHistorySearch,
    ) -> Cow<'_, str> {
        let prefix = match history_search.status {
            reedline::PromptHistorySearchStatus::Passing => "",
            reedline::PromptHistorySearchStatus::Failing => "failing ",
        };
        Cow::Owned(format!("({prefix}search: {})> ", history_search.term))
    }
}

// ---------------------------------------------------------------------------
// Validator — wait for `;` or a complete single-line command
// ---------------------------------------------------------------------------

struct SqlValidator;

impl Validator for SqlValidator {
    fn validate(&self, line: &str) -> ValidationResult {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return ValidationResult::Complete;
        }

        let upper = trimmed.to_ascii_uppercase();

        if upper == "SHOW TABLES"
            || upper.starts_with("DESCRIBE ")
            || upper == "SCHEMA"
            || upper == "DDL"
            || upper == "QUIT"
            || upper == "EXIT"
            || upper.starts_with("\\")
        {
            return ValidationResult::Complete;
        }

        if trimmed.ends_with(';') {
            ValidationResult::Complete
        } else {
            ValidationResult::Incomplete
        }
    }
}

// ---------------------------------------------------------------------------
// Highlighter — color SQL keywords
// ---------------------------------------------------------------------------

const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL", "ORDER", "BY", "ASC",
    "DESC", "LIMIT", "OFFSET", "GROUP", "HAVING", "JOIN", "ON", "AS", "LIKE", "BETWEEN", "EXISTS",
    "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX", "SHOW", "TABLES", "DESCRIBE", "EXPLAIN",
    "TRUE", "FALSE",
];

struct SqlHighlighter;

impl Highlighter for SqlHighlighter {
    fn highlight(&self, line: &str, _cursor: usize) -> StyledText {
        let mut styled = StyledText::new();
        let keyword_style = Style::new().fg(Color::Cyan).bold();
        let string_style = Style::new().fg(Color::Green);
        let number_style = Style::new().fg(Color::Yellow);
        let default_style = Style::new();

        let mut chars = line.chars().peekable();
        let mut token = String::new();

        while let Some(&ch) = chars.peek() {
            if ch == '\'' {
                if !token.is_empty() {
                    push_token(
                        &mut styled,
                        &token,
                        keyword_style,
                        number_style,
                        default_style,
                    );
                    token.clear();
                }
                let mut s = String::new();
                s.push(chars.next().unwrap());
                while let Some(&c) = chars.peek() {
                    s.push(chars.next().unwrap());
                    if c == '\'' {
                        break;
                    }
                }
                styled.push((string_style, s));
            } else if ch.is_ascii_whitespace() || ch == ',' || ch == '(' || ch == ')' || ch == ';' {
                if !token.is_empty() {
                    push_token(
                        &mut styled,
                        &token,
                        keyword_style,
                        number_style,
                        default_style,
                    );
                    token.clear();
                }
                styled.push((default_style, chars.next().unwrap().to_string()));
            } else {
                token.push(chars.next().unwrap());
            }
        }

        if !token.is_empty() {
            push_token(
                &mut styled,
                &token,
                keyword_style,
                number_style,
                default_style,
            );
        }

        fn push_token(
            styled: &mut StyledText,
            token: &str,
            keyword_style: Style,
            number_style: Style,
            default_style: Style,
        ) {
            if SQL_KEYWORDS.contains(&token.to_ascii_uppercase().as_str()) {
                styled.push((keyword_style, token.to_owned()));
            } else if token.chars().all(|c| c.is_ascii_digit() || c == '.') {
                styled.push((number_style, token.to_owned()));
            } else {
                styled.push((default_style, token.to_owned()));
            }
        }

        styled
    }
}

// ---------------------------------------------------------------------------
// Completer
// ---------------------------------------------------------------------------

struct SqlCompleter {
    words: Vec<String>,
}

impl Completer for SqlCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        let before_cursor = &line[..pos];
        let word_start = before_cursor
            .rfind(|c: char| c.is_ascii_whitespace() || c == ',' || c == '(' || c == ')')
            .map(|i| i + 1)
            .unwrap_or(0);

        let partial = &before_cursor[word_start..];
        if partial.is_empty() {
            return Vec::new();
        }

        let partial_upper = partial.to_ascii_uppercase();

        self.words
            .iter()
            .filter(|w| {
                let wu = w.to_ascii_uppercase();
                wu.starts_with(&partial_upper) && wu != partial_upper
            })
            .map(|w| {
                let value = if partial
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_lowercase())
                {
                    w.to_ascii_lowercase()
                } else {
                    w.clone()
                };
                Suggestion {
                    value,
                    description: None,
                    style: None,
                    extra: None,
                    span: Span::new(word_start, pos),
                    append_whitespace: true,
                    match_indices: None,
                }
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// REPL loop
// ---------------------------------------------------------------------------

pub async fn run(catalog_set: Arc<CatalogSet>, ctx: Arc<SqlizeContext>, format: OutputFormat) {
    eprintln!("Commands: SHOW TABLES, DESCRIBE <table>, EXPLAIN <query>");
    eprintln!("SQL ends with ; | Tab to complete | Ctrl+D to exit");

    // Show a sample query — prefer tables named issues, pulls, or orgs_repos
    let all = catalog_set.all_tables();
    let preferred = [
        "issues",
        "pulls",
        "orgs_repos",
        "repos",
        "commits",
        "customers",
    ];
    let sample_table = preferred
        .iter()
        .find_map(|name| all.iter().find(|(_, t)| t.name.as_str() == *name))
        .or_else(|| {
            all.iter().find(|(_, t)| {
                t.required_params().next().is_some() && t.result_columns().nth(1).is_some()
            })
        });
    if let Some((_, table)) = sample_table {
        let params: Vec<String> = table
            .required_params()
            .map(|c| format!("{} = '...'", c.name))
            .collect();
        let cols: Vec<&str> = table
            .columns
            .iter()
            .filter(|c| {
                matches!(
                    c.role,
                    sqlize_core::catalog::types::ColumnRole::ResponseField
                )
            })
            .take(3)
            .map(|c| c.name.as_str())
            .collect();
        if params.is_empty() {
            eprintln!(
                "\nTry:  SELECT {} FROM {} LIMIT 5;",
                cols.join(", "),
                table.name,
            );
        } else {
            eprintln!(
                "\nTry:  SELECT {} FROM {} WHERE {} LIMIT 5;",
                cols.join(", "),
                table.name,
                params.join(" AND "),
            );
        }
    }
    eprintln!();

    let history_file = history_path();
    if let Some(parent) = history_file.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let history =
        FileBackedHistory::with_file(1000, history_file).expect("failed to create history file");

    let completer = Box::new(SqlCompleter {
        words: catalog_set.completion_words(),
    });
    let completion_menu = Box::new(ColumnarMenu::default().with_name("completion_menu"));

    let mut keybindings = default_emacs_keybindings();
    keybindings.add_binding(
        KeyModifiers::NONE,
        KeyCode::Tab,
        ReedlineEvent::UntilFound(vec![
            ReedlineEvent::Menu("completion_menu".to_string()),
            ReedlineEvent::MenuNext,
        ]),
    );

    let cursor_config = CursorConfig {
        vi_insert: None,
        vi_normal: None,
        emacs: Some(SetCursorStyle::SteadyBar),
    };

    let mut editor = Reedline::create()
        .with_validator(Box::new(SqlValidator))
        .with_highlighter(Box::new(SqlHighlighter))
        .with_completer(completer)
        .with_menu(ReedlineMenu::EngineCompleter(completion_menu))
        .with_edit_mode(Box::new(Emacs::new(keybindings)))
        .with_history(Box::new(history))
        .with_cursor_config(cursor_config);

    let prompt = SqlPrompt;

    loop {
        match editor.read_line(&prompt) {
            Ok(Signal::Success(line)) => {
                let trimmed = line.trim().trim_end_matches(';').trim();
                if trimmed.is_empty() {
                    continue;
                }
                dispatch(&catalog_set, &ctx, trimmed, format).await;
            }
            Ok(Signal::CtrlC) => continue,
            Ok(Signal::CtrlD) => break,
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
        }
    }
}

enum ReplCommand<'a> {
    ShowTables,
    Describe(&'a str),
    Explain(&'a str),
    Schema,
    Quit,
    Query(&'a str),
}

fn parse_command(input: &str) -> ReplCommand<'_> {
    let upper = input.to_ascii_uppercase();
    if upper == "SHOW TABLES" || upper == "\\D" {
        ReplCommand::ShowTables
    } else if upper.starts_with("DESCRIBE ") || upper.starts_with("\\D ") {
        ReplCommand::Describe(input.split_whitespace().nth(1).unwrap_or(""))
    } else if upper.starts_with("EXPLAIN ") {
        ReplCommand::Explain(&input[8..])
    } else if upper == "SCHEMA" || upper == "DDL" {
        ReplCommand::Schema
    } else if upper == "QUIT" || upper == "EXIT" || upper == "\\Q" {
        ReplCommand::Quit
    } else {
        ReplCommand::Query(input)
    }
}

async fn dispatch(
    catalog_set: &CatalogSet,
    ctx: &SqlizeContext,
    input: &str,
    format: OutputFormat,
) {
    match parse_command(input) {
        ReplCommand::ShowTables => handle_show_tables(catalog_set),
        ReplCommand::Describe(name) => handle_describe(catalog_set, name),
        ReplCommand::Explain(sql) => handle_explain(ctx, sql).await,
        ReplCommand::Schema => print!("{}", catalog_set.full_ddl()),
        ReplCommand::Quit => std::process::exit(0),
        ReplCommand::Query(sql) => handle_query(ctx, sql, format).await,
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

fn handle_show_tables(catalog_set: &CatalogSet) {
    let mut builder = Builder::default();

    if catalog_set.is_multi() {
        builder.push_record(["schema", "table", "columns", "required", "description"]);
    } else {
        builder.push_record(["table", "columns", "required", "description"]);
    }

    for (schema_name, table) in catalog_set.all_tables() {
        let required: Vec<_> = table.required_params().map(|c| c.name.as_str()).collect();
        let req_str = if required.is_empty() {
            "-".to_owned()
        } else {
            required.join(", ")
        };

        if catalog_set.is_multi() {
            builder.push_record([
                schema_name,
                table.name.as_str(),
                &table.columns.len().to_string(),
                &req_str,
                &table.description,
            ]);
        } else {
            builder.push_record([
                table.name.as_str(),
                &table.columns.len().to_string(),
                &req_str,
                &table.description,
            ]);
        }
    }

    print_table(builder);
}

fn handle_describe(catalog_set: &CatalogSet, name: &str) {
    match catalog_set.describe(name) {
        Some(ddl) => println!("{ddl}"),
        None => eprintln!("Table not found: {name}"),
    }
}

async fn handle_explain(ctx: &SqlizeContext, sql: &str) {
    match ctx.explain(sql).await {
        Ok(plan) => println!("{plan}"),
        Err(e) => eprintln!("Error: {e}"),
    }
}

async fn handle_query(ctx: &SqlizeContext, sql: &str, format: OutputFormat) {
    let result = match ctx.query(sql).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: {e}");
            return;
        }
    };

    let row_count = result.rows.len();

    match format {
        OutputFormat::Table => {
            if result.columns.len() > 6 {
                print_expanded(&result);
            } else {
                let mut builder = Builder::default();
                let headers: Vec<&str> = result.columns.iter().map(|c| c.as_str()).collect();
                builder.push_record(headers);

                for row in &result.rows {
                    let values: Vec<String> = row.values().iter().map(format_value).collect();
                    builder.push_record(values);
                }

                print_table(builder);
            }
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn print_expanded(result: &ResultSet) {
    let max_col_width = result
        .columns
        .iter()
        .map(|c| c.as_str().len())
        .max()
        .unwrap_or(0);

    for (i, row) in result.rows.iter().enumerate() {
        let label = format!("-[ RECORD {} ]", i + 1);
        let separator_len = term_width().saturating_sub(label.len()).saturating_sub(1);
        println!("{}{}", label, "-".repeat(separator_len));

        for (col, val) in result.columns.iter().zip(row.values().iter()) {
            println!(
                "{:>width$} | {}",
                col.as_str(),
                format_value(val),
                width = max_col_width
            );
        }
    }
}

fn print_table(builder: Builder) {
    let mut tbl = builder.build();
    tbl.with(settings::Style::rounded());
    tbl.with(Width::wrap(term_width()).keep_words(true));
    println!("{tbl}");
}

fn format_value(v: &Scalar) -> String {
    match v {
        Scalar::Null => "NULL".to_owned(),
        Scalar::String(s) => s.clone(),
        Scalar::Integer(n) => n.to_string(),
        Scalar::Float(n) => format!("{n:.2}"),
        Scalar::Boolean(b) => b.to_string(),
        Scalar::Json(j) => j.to_string(),
    }
}

fn term_width() -> usize {
    terminal_size::terminal_size()
        .map(|(w, _)| w.0 as usize)
        .unwrap_or(120)
}

fn history_path() -> std::path::PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("."))
        .join("sqlize")
        .join("history.txt")
}
