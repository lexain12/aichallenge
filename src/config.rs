use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;
use url::Url;

const DEFAULT_BASE_URL: &str = "https://api.deepseek.com";
const DEFAULT_MODEL: &str = "deepseek-v4-flash";
const DEFAULT_SYSTEM_PROMPT: &str = "You are a helpful assistant.";
const DEFAULT_TEMPERATURE: f64 = 1.0;
const DEFAULT_MAX_TOKENS: u32 = 4096;
const DEFAULT_TIMEOUT_SECONDS: u64 = 120;

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    api_key: Option<String>,
    base_url: Option<String>,
    model: Option<String>,
    system_prompt: Option<String>,
    temperature: Option<f64>,
    max_tokens: Option<u32>,
    timeout_seconds: Option<u64>,
}

/// Validated settings used by the API client.
pub struct Config {
    api_key: String,
    base_url: Url,
    model: String,
    system_prompt: String,
    temperature: f64,
    max_tokens: u32,
    timeout_seconds: u64,
}

impl Config {
    /// Loads a TOML file and applies the optional environment key override.
    pub fn load(path: &Path, env_api_key: Option<String>) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        let raw: RawConfig =
            toml::from_str(&contents).map_err(|source| ConfigError::Parse { source })?;
        Self::from_raw(raw, env_api_key)
    }

    fn from_raw(raw: RawConfig, env_api_key: Option<String>) -> Result<Self, ConfigError> {
        let api_key = env_api_key.or(raw.api_key).unwrap_or_default();
        let api_key = api_key.trim().to_owned();
        if api_key.is_empty() {
            return Err(ConfigError::InvalidField {
                field: "api_key",
                reason: "must be set in the file or DEEPSEEK_API_KEY",
            });
        }

        let base_url_text = raw.base_url.unwrap_or_else(|| DEFAULT_BASE_URL.to_owned());
        let base_url = Url::parse(&base_url_text).map_err(|_| ConfigError::InvalidField {
            field: "base_url",
            reason: "must be a valid HTTP(S) URL",
        })?;
        if !matches!(base_url.scheme(), "http" | "https") {
            return Err(ConfigError::InvalidField {
                field: "base_url",
                reason: "must use HTTP or HTTPS",
            });
        }

        let model = raw.model.unwrap_or_else(|| DEFAULT_MODEL.to_owned());
        let model = model.trim().to_owned();
        if model.is_empty() {
            return Err(ConfigError::InvalidField {
                field: "model",
                reason: "must not be blank",
            });
        }

        let temperature = raw.temperature.unwrap_or(DEFAULT_TEMPERATURE);
        if !(0.0..=2.0).contains(&temperature) {
            return Err(ConfigError::InvalidField {
                field: "temperature",
                reason: "must be between 0 and 2",
            });
        }

        let max_tokens = raw.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS);
        if max_tokens == 0 {
            return Err(ConfigError::InvalidField {
                field: "max_tokens",
                reason: "must be greater than zero",
            });
        }

        let timeout_seconds = raw.timeout_seconds.unwrap_or(DEFAULT_TIMEOUT_SECONDS);
        if timeout_seconds == 0 {
            return Err(ConfigError::InvalidField {
                field: "timeout_seconds",
                reason: "must be greater than zero",
            });
        }

        Ok(Self {
            api_key,
            base_url,
            model,
            system_prompt: raw
                .system_prompt
                .unwrap_or_else(|| DEFAULT_SYSTEM_PROMPT.to_owned()),
            temperature,
            max_tokens,
            timeout_seconds,
        })
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn system_prompt(&self) -> &str {
        &self.system_prompt
    }

    pub fn temperature(&self) -> f64 {
        self.temperature
    }

    pub fn max_tokens(&self) -> u32 {
        self.max_tokens
    }

    pub fn timeout_seconds(&self) -> u64 {
        self.timeout_seconds
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Config")
            .field("api_key", &"[REDACTED]")
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .field("system_prompt", &self.system_prompt)
            .field("temperature", &self.temperature)
            .field("max_tokens", &self.max_tokens)
            .field("timeout_seconds", &self.timeout_seconds)
            .finish()
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read configuration file {path}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse configuration")]
    Parse {
        #[source]
        source: toml::de::Error,
    },
    #[error("invalid {field}: {reason}")]
    InvalidField {
        field: &'static str,
        reason: &'static str,
    },
}
