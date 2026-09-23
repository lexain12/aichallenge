use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

use crate::invariants::{InvariantRule, invariant_id, invariant_text};

const DEFAULT_BASE_URL: &str = "https://api.deepseek.com";
const DEFAULT_MODEL: &str = "deepseek-v4-flash";
const DEFAULT_SYSTEM_PROMPT: &str = "You are a helpful assistant.";
const DEFAULT_TEMPERATURE: f64 = 1.0;
const DEFAULT_MAX_TOKENS: u32 = 4096;
const DEFAULT_TIMEOUT_SECONDS: u64 = 120;
const DEFAULT_COMPACT_AFTER_PROMPT_TOKENS: u64 = 6000;
const DEFAULT_KEEP_LAST_MESSAGES: usize = 10;
const DEFAULT_SUMMARY_MAX_TOKENS: u32 = 1024;
const DEFAULT_FACTS_MAX_TOKENS: u32 = 512;
const DEFAULT_INTERPRETER_MAX_TOKENS: u32 = 512;
const DEFAULT_CHECKER_MAX_TOKENS: u32 = 1024;
const DEFAULT_HANDOFF_MAX_TOKENS: u32 = 2048;
const DEFAULT_MIN_CONFIDENCE: f32 = 0.80;
const DEFAULT_MAX_AUTONOMOUS_TURNS: u32 = 8;
const DEFAULT_MAX_AUTONOMOUS_TOKENS: u64 = 20_000;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextStrategy {
    #[default]
    Summary,
    SlidingWindow,
    StickyFacts,
    Branching,
}

impl ContextStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Summary => "summary",
            Self::SlidingWindow => "sliding_window",
            Self::StickyFacts => "sticky_facts",
            Self::Branching => "branching",
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawContextConfig {
    strategy: Option<ContextStrategy>,
    compact_after_prompt_tokens: Option<u64>,
    keep_last_messages: Option<usize>,
    summary_max_tokens: Option<u32>,
    facts_max_tokens: Option<u32>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawDebugConfig {
    log_path: Option<PathBuf>,
    log_payloads: Option<bool>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawWorkflowConfig {
    enabled: Option<bool>,
    interpreter_model: Option<String>,
    checker_model: Option<String>,
    handoff_model: Option<String>,
    interpreter_max_tokens: Option<u32>,
    checker_max_tokens: Option<u32>,
    handoff_max_tokens: Option<u32>,
    min_confidence: Option<f32>,
    max_autonomous_turns: Option<u32>,
    max_autonomous_tokens: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawInvariantConfig {
    id: String,
    text: String,
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
    #[serde(default)]
    workflow: RawWorkflowConfig,
    #[serde(default)]
    invariants: Vec<RawInvariantConfig>,
}

#[derive(Clone, Debug)]
pub struct ContextConfig {
    strategy: ContextStrategy,
    compact_after_prompt_tokens: u64,
    keep_last_messages: usize,
    summary_max_tokens: u32,
    facts_max_tokens: u32,
}

impl ContextConfig {
    pub(crate) fn full_history() -> Self {
        Self {
            strategy: ContextStrategy::Branching,
            compact_after_prompt_tokens: DEFAULT_COMPACT_AFTER_PROMPT_TOKENS,
            keep_last_messages: DEFAULT_KEEP_LAST_MESSAGES,
            summary_max_tokens: DEFAULT_SUMMARY_MAX_TOKENS,
            facts_max_tokens: DEFAULT_FACTS_MAX_TOKENS,
        }
    }

    pub fn strategy(&self) -> ContextStrategy {
        self.strategy
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

    pub fn facts_max_tokens(&self) -> u32 {
        self.facts_max_tokens
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

#[derive(Clone, Debug)]
pub struct WorkflowConfig {
    enabled: bool,
    interpreter_model: String,
    checker_model: String,
    handoff_model: String,
    interpreter_max_tokens: u32,
    checker_max_tokens: u32,
    handoff_max_tokens: u32,
    min_confidence: f32,
    max_autonomous_turns: u32,
    max_autonomous_tokens: u64,
}

impl WorkflowConfig {
    fn from_raw(raw: RawWorkflowConfig, fallback_model: &str) -> Result<Self, ConfigError> {
        let interpreter_model = validated_workflow_model(
            raw.interpreter_model,
            fallback_model,
            "workflow.interpreter_model",
        )?;
        let checker_model =
            validated_workflow_model(raw.checker_model, fallback_model, "workflow.checker_model")?;
        let handoff_model =
            validated_workflow_model(raw.handoff_model, fallback_model, "workflow.handoff_model")?;
        let interpreter_max_tokens = raw
            .interpreter_max_tokens
            .unwrap_or(DEFAULT_INTERPRETER_MAX_TOKENS);
        let checker_max_tokens = raw.checker_max_tokens.unwrap_or(DEFAULT_CHECKER_MAX_TOKENS);
        let handoff_max_tokens = raw.handoff_max_tokens.unwrap_or(DEFAULT_HANDOFF_MAX_TOKENS);
        let max_autonomous_turns = raw
            .max_autonomous_turns
            .unwrap_or(DEFAULT_MAX_AUTONOMOUS_TURNS);
        let max_autonomous_tokens = raw
            .max_autonomous_tokens
            .unwrap_or(DEFAULT_MAX_AUTONOMOUS_TOKENS);
        let min_confidence = raw.min_confidence.unwrap_or(DEFAULT_MIN_CONFIDENCE);

        for (field, value) in [
            ("workflow.interpreter_max_tokens", interpreter_max_tokens),
            ("workflow.checker_max_tokens", checker_max_tokens),
            ("workflow.handoff_max_tokens", handoff_max_tokens),
        ] {
            if value == 0 {
                return Err(ConfigError::InvalidField {
                    field,
                    reason: "must be greater than zero",
                });
            }
        }
        if max_autonomous_turns == 0 {
            return Err(ConfigError::InvalidField {
                field: "workflow.max_autonomous_turns",
                reason: "must be greater than zero",
            });
        }
        if max_autonomous_tokens == 0 {
            return Err(ConfigError::InvalidField {
                field: "workflow.max_autonomous_tokens",
                reason: "must be greater than zero",
            });
        }
        if !min_confidence.is_finite() || !(0.0..=1.0).contains(&min_confidence) {
            return Err(ConfigError::InvalidField {
                field: "workflow.min_confidence",
                reason: "must be finite and between 0 and 1",
            });
        }

        Ok(Self {
            enabled: raw.enabled.unwrap_or(true),
            interpreter_model,
            checker_model,
            handoff_model,
            interpreter_max_tokens,
            checker_max_tokens,
            handoff_max_tokens,
            min_confidence,
            max_autonomous_turns,
            max_autonomous_tokens,
        })
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn interpreter_model(&self) -> &str {
        &self.interpreter_model
    }

    pub fn checker_model(&self) -> &str {
        &self.checker_model
    }

    pub fn handoff_model(&self) -> &str {
        &self.handoff_model
    }

    pub fn interpreter_max_tokens(&self) -> u32 {
        self.interpreter_max_tokens
    }

    pub fn checker_max_tokens(&self) -> u32 {
        self.checker_max_tokens
    }

    pub fn handoff_max_tokens(&self) -> u32 {
        self.handoff_max_tokens
    }

    pub fn min_confidence(&self) -> f32 {
        self.min_confidence
    }

    pub fn max_autonomous_turns(&self) -> u32 {
        self.max_autonomous_turns
    }

    pub fn max_autonomous_tokens(&self) -> u64 {
        self.max_autonomous_tokens
    }
}

fn validated_workflow_model(
    value: Option<String>,
    fallback_model: &str,
    field: &'static str,
) -> Result<String, ConfigError> {
    match value {
        Some(value) => {
            let value = value.trim().to_owned();
            if value.is_empty() {
                return Err(ConfigError::InvalidField {
                    field,
                    reason: "must not be blank",
                });
            }
            Ok(value)
        }
        None => Ok(fallback_model.to_owned()),
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
    workflow: WorkflowConfig,
    invariants: Vec<InvariantRule>,
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
        let strategy = raw.context.strategy.ok_or(ConfigError::InvalidField {
            field: "context.strategy",
            reason: "must be set",
        })?;
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
        let facts_max_tokens = raw
            .context
            .facts_max_tokens
            .unwrap_or(DEFAULT_FACTS_MAX_TOKENS);
        if facts_max_tokens == 0 {
            return Err(ConfigError::InvalidField {
                field: "facts_max_tokens",
                reason: "must be greater than zero",
            });
        }
        let workflow = WorkflowConfig::from_raw(raw.workflow, &model)?;
        let mut invariant_ids = HashSet::new();
        let mut invariants = Vec::with_capacity(raw.invariants.len());
        for rule in raw.invariants {
            let id = invariant_id(&rule.id).map_err(|_| ConfigError::InvalidField {
                field: "invariants.id",
                reason: "must contain 1..64 ASCII letters, digits, hyphens or underscores",
            })?;
            let text = invariant_text(&rule.text).map_err(|_| ConfigError::InvalidField {
                field: "invariants.text",
                reason: "must contain 1..4096 characters",
            })?;
            if !invariant_ids.insert(id.to_owned()) {
                return Err(ConfigError::InvalidField {
                    field: "invariants.id",
                    reason: "IDs must be unique",
                });
            }
            invariants.push(InvariantRule {
                id: id.to_owned(),
                text: text.to_owned(),
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
                strategy,
                compact_after_prompt_tokens,
                keep_last_messages,
                summary_max_tokens,
                facts_max_tokens,
            },
            debug: DebugConfig {
                log_path: raw.debug.log_path,
                log_payloads: raw.debug.log_payloads.unwrap_or(false),
            },
            workflow,
            invariants,
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

    pub fn workflow(&self) -> &WorkflowConfig {
        &self.workflow
    }

    pub fn invariants(&self) -> &[InvariantRule] {
        &self.invariants
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
            .field("workflow", &self.workflow)
            .field("invariants", &self.invariants)
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
