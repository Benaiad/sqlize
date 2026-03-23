use std::path::PathBuf;

use crate::catalog::types::TableName;

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

    #[error("duplicate table name: {0}")]
    DuplicateTable(TableName),

    // ---- SQL / DataFusion ----
    #[error("unsupported SQL: {0}")]
    UnsupportedSql(String),

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

    // ---- Output ----
    #[error("TOON encoding error: {0}")]
    ToonEncode(String),
}

pub type Result<T> = std::result::Result<T, Error>;
