use crate::catalog::types::{ColumnName, TableName};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid table name {input:?}: {reason}")]
    InvalidTableName { input: String, reason: &'static str },

    #[error("invalid column name {input:?}: {reason}")]
    InvalidColumnName { input: String, reason: &'static str },

    #[error("invalid path template {input:?}: {reason}")]
    InvalidPathTemplate { input: String, reason: &'static str },

    #[error("table {0} not found in catalog")]
    TableNotFound(TableName),

    #[error("column {column} not found in table {table}")]
    ColumnNotFound { table: TableName, column: ColumnName },

    #[error("missing required parameter {column} for table {table}")]
    MissingRequiredParam { table: TableName, column: ColumnName },

    #[error("unsupported SQL: {0}")]
    UnsupportedSql(&'static str),

    #[error("spec error: {0}")]
    Spec(String),

    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
