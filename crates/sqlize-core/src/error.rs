use std::path::PathBuf;

use crate::catalog::types::{ColumnName, TableName};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // ---- Newtype validation ----
    #[error("invalid table name {input:?}: {reason}")]
    InvalidTableName { input: String, reason: &'static str },

    #[error("invalid column name {input:?}: {reason}")]
    InvalidColumnName { input: String, reason: &'static str },

    #[error("invalid path template {input:?}: {reason}")]
    InvalidPathTemplate { input: String, reason: &'static str },

    // ---- Catalog lookup ----
    #[error("table {0} not found in catalog")]
    TableNotFound(TableName),

    #[error("column {column} not found in table {table}")]
    ColumnNotFound {
        table: TableName,
        column: ColumnName,
    },

    #[error("missing required parameter {column} for table {table}")]
    MissingRequiredParam {
        table: TableName,
        column: ColumnName,
    },

    #[error("duplicate table name: {0}")]
    DuplicateTable(TableName),

    // ---- SQL planning ----
    #[error("unsupported SQL: {0}")]
    UnsupportedSql(String),

    #[error("SQL parse error: {0}")]
    SqlParse(String),

    // ---- OpenAPI spec loading ----
    #[error("failed to read spec {path}: {message}")]
    SpecRead { path: PathBuf, message: String },

    #[error("failed to parse OpenAPI spec: {0}")]
    SpecParse(String),

    #[error("spec has no servers defined — cannot determine API base URL")]
    NoServers,

    #[error("invalid API path {path}: {reason}")]
    InvalidPath { path: String, reason: &'static str },

    #[error("cannot derive table name from path: {0}")]
    TableNameDerivation(String),

    // ---- Execution ----
    #[error("API returned {status}: {body}")]
    ApiError {
        status: u16,
        url: String,
        body: String,
    },

    #[error("failed to resolve URL: missing path parameters")]
    UnresolvedUrl,

    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    // ---- Output ----
    #[error("TOON encoding error: {0}")]
    ToonEncode(String),
}

pub type Result<T> = std::result::Result<T, Error>;
