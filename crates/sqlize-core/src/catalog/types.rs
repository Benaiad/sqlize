use std::fmt;

use crate::error::Error;

// ---------------------------------------------------------------------------
// Newtypes
// ---------------------------------------------------------------------------

/// A validated, non-empty table name containing only `[a-z0-9_]`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableName(String);

impl TableName {
    pub fn new(s: &str) -> Result<Self, Error> {
        if s.is_empty() {
            return Err(Error::InvalidTableName {
                input: s.to_owned(),
                reason: "cannot be empty",
            });
        }
        if !s
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            return Err(Error::InvalidTableName {
                input: s.to_owned(),
                reason: "must contain only lowercase ascii, digits, or underscores",
            });
        }
        Ok(Self(s.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A validated, non-empty column name containing only `[a-z0-9_]`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnName(String);

impl ColumnName {
    pub fn new(s: &str) -> Result<Self, Error> {
        if s.is_empty() {
            return Err(Error::InvalidColumnName {
                input: s.to_owned(),
                reason: "cannot be empty",
            });
        }
        if !s
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            return Err(Error::InvalidColumnName {
                input: s.to_owned(),
                reason: "must contain only lowercase ascii, digits, or underscores",
            });
        }
        Ok(Self(s.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ColumnName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A URL path template like `/repos/{owner}/{repo}/issues`.
/// Guaranteed to start with `/` and contain at least one segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathTemplate(String);

impl PathTemplate {
    pub fn new(s: &str) -> Result<Self, Error> {
        if !s.starts_with('/') {
            return Err(Error::InvalidPathTemplate {
                input: s.to_owned(),
                reason: "must start with '/'",
            });
        }
        if s.len() < 2 {
            return Err(Error::InvalidPathTemplate {
                input: s.to_owned(),
                reason: "must have at least one path segment",
            });
        }
        Ok(Self(s.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Extract placeholder names from the template.
    /// `/repos/{owner}/{repo}/issues` → `["owner", "repo"]`
    pub fn placeholders(&self) -> Vec<&str> {
        self.0
            .split('/')
            .filter_map(|segment| {
                segment
                    .strip_prefix('{')
                    .and_then(|s| s.strip_suffix('}'))
            })
            .collect()
    }

    /// Substitute placeholders with concrete values.
    /// Returns `None` if any placeholder is missing from the map.
    pub fn resolve(&self, params: &std::collections::HashMap<String, String>) -> Option<String> {
        let mut result = self.0.clone();
        for name in self.placeholders() {
            let value = params.get(name)?;
            result = result.replace(&format!("{{{name}}}"), value);
        }
        Some(result)
    }
}

impl fmt::Display for PathTemplate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// ---------------------------------------------------------------------------
// Column types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    String,
    Integer,
    Float,
    Boolean,
    Timestamp,
    Json,
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String => write!(f, "TEXT"),
            Self::Integer => write!(f, "INTEGER"),
            Self::Float => write!(f, "REAL"),
            Self::Boolean => write!(f, "BOOLEAN"),
            Self::Timestamp => write!(f, "TIMESTAMP"),
            Self::Json => write!(f, "JSON"),
        }
    }
}

// ---------------------------------------------------------------------------
// Column origin — where this column comes from in the API
// ---------------------------------------------------------------------------

/// Describes how a column maps back to the underlying API request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnOrigin {
    /// From a URL path placeholder (e.g., `{owner}`). Always required in WHERE.
    PathParam,
    /// From an API query parameter (e.g., `?state=open`). Pushed down when filtered.
    QueryParam {
        /// The API parameter name, if different from the column name.
        api_name: Option<String>,
    },
    /// From the response body. Cannot influence the API call.
    ResponseField,
    /// Both a query parameter (for filtering) and a response field (for the value).
    /// Example: `state` can be filtered via `?state=open` and also appears in the response.
    QueryParamAndResponseField {
        /// The API parameter name, if different from the column name.
        api_name: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Column
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Column {
    pub name: ColumnName,
    pub col_type: ColumnType,
    pub nullable: bool,
    pub description: Option<String>,
    pub origin: ColumnOrigin,
}

// ---------------------------------------------------------------------------
// HTTP method
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl fmt::Display for HttpMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Get => write!(f, "GET"),
            Self::Post => write!(f, "POST"),
            Self::Put => write!(f, "PUT"),
            Self::Patch => write!(f, "PATCH"),
            Self::Delete => write!(f, "DELETE"),
        }
    }
}

// ---------------------------------------------------------------------------
// API endpoint
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ApiEndpoint {
    pub method: HttpMethod,
    pub path: PathTemplate,
    pub base_url: String,
    /// The content type to send in the Accept header, derived from the spec's
    /// response media types (e.g., "application/json").
    pub accept: String,
    /// For wrapped responses (e.g., Stripe's `{"data": [...]}`), the JSON field
    /// name that holds the array. `None` for top-level arrays.
    /// Derived from the OpenAPI response schema at spec-loading time.
    pub data_path: Option<String>,
}

impl ApiEndpoint {
    pub fn url(&self, params: &std::collections::HashMap<String, String>) -> Option<String> {
        let path = self.path.resolve(params)?;
        Some(format!("{}{}", self.base_url, path))
    }
}

// ---------------------------------------------------------------------------
// Virtual table
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct VirtualTable {
    pub name: TableName,
    pub description: String,
    pub columns: Vec<Column>,
    pub endpoint: ApiEndpoint,
}

impl VirtualTable {
    /// Columns that are path parameters — must appear in every WHERE clause.
    pub fn required_params(&self) -> impl Iterator<Item = &Column> {
        self.columns
            .iter()
            .filter(|c| matches!(c.origin, ColumnOrigin::PathParam))
    }

    /// Columns that can be pushed down as query parameters.
    pub fn pushdown_params(&self) -> impl Iterator<Item = &Column> {
        self.columns
            .iter()
            .filter(|c| matches!(c.origin, ColumnOrigin::QueryParam { .. } | ColumnOrigin::QueryParamAndResponseField { .. }))
    }

    /// Columns that come from the response body.
    pub fn response_columns(&self) -> impl Iterator<Item = &Column> {
        self.columns
            .iter()
            .filter(|c| matches!(c.origin, ColumnOrigin::ResponseField))
    }
}

// ---------------------------------------------------------------------------
// Name sanitization
// ---------------------------------------------------------------------------

/// Sanitize an API field name to a valid column/table name.
/// `camelCase` → `camel_case`, hyphens/dots → underscores, uppercase → lowercase.
pub fn sanitize_name(name: &str) -> String {
    let mut result = String::with_capacity(name.len() + 4);
    for (i, ch) in name.chars().enumerate() {
        if ch == '-' || ch == '.' {
            result.push('_');
        } else if ch.is_ascii_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_ascii_lowercase());
        } else {
            result.push(ch);
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

/// A single value in a result row.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Json(serde_json::Value),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::String(s) => write!(f, "{s}"),
            Self::Integer(n) => write!(f, "{n}"),
            Self::Float(n) => write!(f, "{n}"),
            Self::Boolean(b) => write!(f, "{b}"),
            Self::Json(v) => write!(f, "{v}"),
        }
    }
}

/// A row of values, ordered to match `ResultSet.columns`.
#[derive(Debug, Clone)]
pub struct Row(pub Vec<Value>);

/// The result of executing a query.
#[derive(Debug, Clone)]
pub struct ResultSet {
    pub columns: Vec<ColumnName>,
    pub rows: Vec<Row>,
}
