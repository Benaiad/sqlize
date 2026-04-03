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

    #[error("invalid API parameter name {input:?}: {reason}")]
    InvalidApiParamName { input: String, reason: &'static str },

    #[error("invalid base URL {input:?}: {reason}")]
    InvalidBaseUrl { input: String, reason: &'static str },

    #[error("invalid accept header {input:?}: {reason}")]
    InvalidAcceptHeader { input: String, reason: &'static str },

    // ---- Catalog lookup ----
    #[error("table {0} not found in catalog")]
    TableNotFound(TableName),

    #[error("duplicate table name: {0}")]
    DuplicateTable(TableName),

    // ---- SQL / DataFusion ----
    #[error("SQL error: {0}")]
    SqlError(#[source] datafusion::error::DataFusionError),

    #[error("query execution failed: {0}")]
    QueryExecutionError(#[source] datafusion::error::DataFusionError),

    #[error("catalog registration failed: {0}")]
    CatalogRegistrationError(#[source] datafusion::error::DataFusionError),

    // ---- OpenAPI spec loading ----
    #[error("failed to read spec {path}")]
    SpecRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse OpenAPI spec: {0}")]
    SpecParse(#[source] serde_json::Error),

    #[error("spec has no servers defined — cannot determine API base URL")]
    NoServers,

    #[error("invalid API path {path}: {reason}")]
    InvalidPath { path: String, reason: &'static str },

    #[error("cannot derive table name from path: {0}")]
    TableNameDerivation(String),

    // ---- Output ----
    #[error("TOON encoding error: {0}")]
    ToonEncode(#[source] toon_format::ToonError),
}

pub type Result<T> = std::result::Result<T, Error>;
