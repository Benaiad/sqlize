use std::borrow::Cow;
use std::sync::Arc;

use nu_ansi_term::{Color, Style};
use reedline::{
    ColumnarMenu, Completer, CursorConfig, Emacs, FileBackedHistory, Highlighter, KeyCode,
    KeyModifiers, MenuBuilder, Prompt, PromptEditMode, PromptHistorySearch, Reedline,
    ReedlineEvent, ReedlineMenu, Signal, Span, StyledText, Suggestion, ValidationResult,
    Validator, default_emacs_keybindings,
};
use crossterm::cursor::SetCursorStyle;
use tabled::builder::Builder;
use tabled::settings::{self, Width};

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

        // Single-line commands are always complete
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

        // SQL statements: wait for semicolon
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
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL",
    "ORDER", "BY", "ASC", "DESC", "LIMIT", "OFFSET", "GROUP", "HAVING",
    "JOIN", "ON", "AS", "LIKE", "BETWEEN", "EXISTS", "DISTINCT", "COUNT",
    "SUM", "AVG", "MIN", "MAX", "SHOW", "TABLES", "DESCRIBE", "EXPLAIN",
    "TRUE", "FALSE",
];

struct SqlHighlighter;

impl Highlighter for SqlHighlighter {
    fn highlight(&self, line: &str, _cursor: usize) -> StyledText {
        let mut styled = StyledText::new();
        let keyword_style = Style::new().fg(Color::Cyan).bold();
        let string_style = Style::new().fg(Color::Green);
        let _number_style = Style::new().fg(Color::Yellow);
        let default_style = Style::new();

        let mut chars = line.chars().peekable();
        let mut token = String::new();

        while let Some(&ch) = chars.peek() {
            if ch == '\'' {
                // String literal
                if !token.is_empty() {
                    push_token(&mut styled, &token, keyword_style, default_style);
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
                    push_token(&mut styled, &token, keyword_style, default_style);
                    token.clear();
                }
                styled.push((default_style, chars.next().unwrap().to_string()));
            } else {
                token.push(chars.next().unwrap());
            }
        }

        if !token.is_empty() {
            push_token(&mut styled, &token, keyword_style, default_style);
        }

        fn push_token(
            styled: &mut StyledText,
            token: &str,
            keyword_style: Style,
            default_style: Style,
        ) {
            if SQL_KEYWORDS.contains(&token.to_ascii_uppercase().as_str()) {
                styled.push((keyword_style, token.to_owned()));
            } else if token.chars().all(|c| c.is_ascii_digit() || c == '.') {
                styled.push((Style::new().fg(Color::Yellow), token.to_owned()));
            } else {
                styled.push((default_style, token.to_owned()));
            }
        }

        styled
    }
}

// ---------------------------------------------------------------------------
// Completer — SQL keywords + table/column names from catalog
// ---------------------------------------------------------------------------

struct SqlCompleter {
    words: Vec<String>,
}

impl SqlCompleter {
    fn from_catalog(catalog: &Catalog) -> Self {
        let mut words: Vec<String> = SQL_KEYWORDS.iter().map(|k| k.to_string()).collect();

        // Add lowercase versions too — LLMs and users type lowercase SQL
        for kw in SQL_KEYWORDS {
            words.push(kw.to_ascii_lowercase());
        }

        // Table names
        for table in catalog.tables() {
            words.push(table.name.as_str().to_owned());

            // Column names
            for col in &table.columns {
                let name = col.name.as_str().to_owned();
                if !words.contains(&name) {
                    words.push(name);
                }
            }
        }

        // REPL commands
        words.extend(["SHOW TABLES", "DESCRIBE", "EXPLAIN", "SCHEMA", "DDL"].map(String::from));

        words.sort();
        words.dedup();
        Self { words }
    }
}

impl Completer for SqlCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        // Find the word being typed at the cursor position
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
                // Match the case of what the user is typing
                let value = if partial.chars().next().is_some_and(|c| c.is_ascii_lowercase()) {
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

pub async fn run(catalog: Arc<Catalog>, auth: AuthConfig, format: OutputFormat) {
    eprintln!("Type SQL (end with ;), or: SHOW TABLES, DESCRIBE <table>, EXPLAIN <sql>");
    eprintln!("Ctrl+D to exit.\n");

    let history_file = history_path();
    if let Some(parent) = history_file.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let history = FileBackedHistory::with_file(1000, history_file)
        .expect("failed to create history file");

    let completer = Box::new(SqlCompleter::from_catalog(&catalog));
    let completion_menu = Box::new(
        ColumnarMenu::default().with_name("completion_menu"),
    );

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
                dispatch(&catalog, &auth, trimmed, format).await;
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

async fn dispatch(catalog: &Catalog, auth: &AuthConfig, input: &str, format: OutputFormat) {
    let upper = input.to_ascii_uppercase();

    if upper == "SHOW TABLES" || upper == "\\D" {
        handle_show_tables(catalog);
    } else if upper.starts_with("DESCRIBE ") || upper.starts_with("\\D ") {
        let table_name = input.split_whitespace().nth(1).unwrap_or("");
        handle_describe(catalog, table_name);
    } else if upper.starts_with("EXPLAIN ") {
        let sql = input.strip_prefix("EXPLAIN ").or_else(|| input.strip_prefix("explain ")).unwrap_or(input);
        handle_explain(catalog, sql);
    } else if upper == "SCHEMA" || upper == "DDL" {
        println!("{}", catalog_ddl(catalog));
    } else if upper == "QUIT" || upper == "EXIT" || upper == "\\Q" {
        std::process::exit(0);
    } else {
        handle_query(catalog, auth, input, format).await;
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

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

    print_table(builder);
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

            print_table(builder);
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

fn print_table(builder: Builder) {
    let mut tbl = builder.build();
    tbl.with(settings::Style::rounded());
    tbl.with(Width::wrap(term_width()).keep_words(true));
    println!("{tbl}");
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

fn history_path() -> std::path::PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("."))
        .join("sqlize")
        .join("history.txt")
}
