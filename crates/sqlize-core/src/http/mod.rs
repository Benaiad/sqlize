pub mod pagination;

use std::time::Duration;

/// Default HTTP request timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Build a configured HTTP client for API requests.
pub fn build_client(timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .expect("TLS backend initialization failed")
}

/// Configuration for API authentication.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub bearer_token: Option<String>,
}
