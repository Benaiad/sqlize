pub mod pagination;

use std::time::Duration;

/// Default HTTP request timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Build a configured HTTP client for API requests.
pub fn build_client(timeout: Duration) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder().timeout(timeout).build()
}

/// A bearer token for API authentication. Redacted in `Debug` output.
#[derive(Clone)]
pub struct BearerToken(String);

impl BearerToken {
    pub fn new(token: impl Into<String>) -> Self {
        Self(token.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for BearerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BearerToken(REDACTED)")
    }
}

/// Configuration for API authentication.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub bearer_token: Option<BearerToken>,
}
