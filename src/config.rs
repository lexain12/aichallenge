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
const DEFAULT_COMPACT_AFTER_PROMPT_TOKENS: u64 = 6000;
const DEFAULT_KEEP_LAST_MESSAGES: usize = 10;
const DEFAULT_SUMMARY_MAX_TOKENS: u32 = 1024;

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawContextConfig {
    enabled: Option<bool>,
    compact_after_prompt_tokens: Option<u64>,
    keep_last_messages: Option<usize>,
    summary_max_tokens: Option<u32>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawDebugConfig {
    log_path: Option<PathBuf>,
    log_payloads: Option<bool>,
}

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
    top_p: Option<f64>,
    stop: Option<Vec<String>>,
    thinking: Option<String>,
    include_usage: Option<bool>,
    #[serde(default)]
    context: RawContextConfig,
    #[serde(default)]
    debug: RawDebugConfig,
}

#[derive(Clone, Debug)]
pub struct ContextConfig {
    enabled: bool,
    compact_after_prompt_tokens: u64,
    keep_last_messages: usize,
    summary_max_tokens: u32,
}

impl ContextConfig {
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn compact_after_prompt_tokens(&self) -> u64 {
        self.compact_after_prompt_tokens
    }

    pub fn keep_last_messages(&self) -> usize {
        self.keep_last_messages
    }

    pub fn summary_max_tokens(&self) -> u32 {
        self.summary_max_tokens
    }
}

#[derive(Clone, Debug)]
pub struct DebugConfig {
    log_path: Option<PathBuf>,
    log_payloads: bool,
}

impl DebugConfig {
    pub fn log_path(&self) -> Option<&Path> {
        self.log_path.as_deref()
    }

    pub fn log_payloads(&self) -> bool {
        self.log_payloads
    }
}

/// Validated settings used by the API client.
#[derive(Clone)]
pub struct Config {
    api_key: String,
    base_url: Url,
    model: String,
    system_prompt: String,
    temperature: f64,
    max_tokens: u32,
    timeout_seconds: u64,
    top_p: Option<f64>,
    stop: Vec<String>,
    thinking: Option<String>,
    include_usage: bool,
    context: ContextConfig,
    debug: DebugConfig,
}

impl Config {
    /// Loads a TOML file and applies the optional environment key override.
    pub fn load(path: &Path, env_api_key: Option<String>) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        Self::from_toml(&contents, env_api_key)
    }

    pub fn from_toml(contents: &str, env_api_key: Option<String>) -> Result<Self, ConfigError> {
        let raw: RawConfig =
            toml::from_str(contents).map_err(|source| ConfigError::Parse { source })?;
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

        if raw.top_p.is_some_and(|value| !(0.0..=1.0).contains(&value)) {
            return Err(ConfigError::InvalidField {
                field: "top_p",
                reason: "must be between 0 and 1",
            });
        }
        let stop = raw.stop.unwrap_or_default();
        if stop.len() > 16 || stop.iter().any(|s| s.is_empty()) {
            return Err(ConfigError::InvalidField {
                field: "stop",
                reason: "at most 16 non-empty strings",
            });
        }
        if raw
            .thinking
            .as_deref()
            .is_some_and(|s| !matches!(s, "enabled" | "disabled"))
        {
            return Err(ConfigError::InvalidField {
                field: "thinking",
                reason: "must be enabled or disabled, or omitted",
            });
        }
        let compact_after_prompt_tokens = raw
            .context
            .compact_after_prompt_tokens
            .unwrap_or(DEFAULT_COMPACT_AFTER_PROMPT_TOKENS);
        if compact_after_prompt_tokens == 0 {
            return Err(ConfigError::InvalidField {
                field: "compact_after_prompt_tokens",
                reason: "must be greater than zero",
            });
        }
        let keep_last_messages = raw
            .context
            .keep_last_messages
            .unwrap_or(DEFAULT_KEEP_LAST_MESSAGES);
        if keep_last_messages == 0 {
            return Err(ConfigError::InvalidField {
                field: "keep_last_messages",
                reason: "must be greater than zero",
            });
        }
        let summary_max_tokens = raw
            .context
            .summary_max_tokens
            .unwrap_or(DEFAULT_SUMMARY_MAX_TOKENS);
        if summary_max_tokens == 0 {
            return Err(ConfigError::InvalidField {
                field: "summary_max_tokens",
                reason: "must be greater than zero",
            });
        }
        Ok(Self {
            top_p: raw.top_p,
            stop,
            thinking: raw.thinking,
            include_usage: raw.include_usage.unwrap_or(true),
            api_key,
            base_url,
            model,
            system_prompt: raw
                .system_prompt
                .unwrap_or_else(|| DEFAULT_SYSTEM_PROMPT.to_owned()),
            temperature,
            max_tokens,
            timeout_seconds,
            context: ContextConfig {
                enabled: raw.context.enabled.unwrap_or(true),
                compact_after_prompt_tokens,
                keep_last_messages,
                summary_max_tokens,
            },
            debug: DebugConfig {
                log_path: raw.debug.log_path,
                log_payloads: raw.debug.log_payloads.unwrap_or(false),
            },
        })
    }

    pub fn top_p(&self) -> Option<f64> {
        self.top_p
    }
    pub fn stop(&self) -> &[String] {
        &self.stop
    }
    pub fn thinking(&self) -> Option<&str> {
        self.thinking.as_deref()
    }
    pub fn include_usage(&self) -> bool {
        self.include_usage
    }

    pub fn context(&self) -> &ContextConfig {
        &self.context
    }

    pub fn debug(&self) -> &DebugConfig {
        &self.debug
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

    pub fn with_temperature(mut self, temperature: f64) -> Result<Self, ConfigError> {
        if !(0.0..=2.0).contains(&temperature) {
            return Err(ConfigError::InvalidField {
                field: "temperature",
                reason: "must be between 0 and 2",
            });
        }
        self.temperature = temperature;
        Ok(self)
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
            .field("context", &self.context)
            .field("debug", &self.debug)
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
