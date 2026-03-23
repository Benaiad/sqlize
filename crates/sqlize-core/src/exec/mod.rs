pub mod pagination;

pub use reqwest::Client;

/// Configuration for API authentication.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub bearer_token: Option<String>,
}
