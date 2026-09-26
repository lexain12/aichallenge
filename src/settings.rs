//! Strict, separate server and SSH-client configuration.

use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use chrono_tz::Tz;
use serde::Deserialize;
use thiserror::Error;
use url::Url;

pub const REMOTE_COMMAND: &str = "/opt/light-agent/bin/light-agent serve-stdio";
const DEFAULT_PROVIDER_REQUEST_BYTES: usize = 524_288;
const DEFAULT_MESSAGE_BYTES: usize = 262_144;

#[derive(Debug, Error)]
pub enum SettingsError {
    #[error("cannot read settings: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid settings TOML")]
    Toml,
    #[error("invalid {0}")]
    Invalid(&'static str),
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawServerSettings {
    #[serde(default)]
    provider: RawProviderSettings,
    #[serde(default)]
    mcp: RawMcpSettings,
    #[serde(default)]
    scheduler: RawSchedulerSettings,
    #[serde(default)]
    database: RawDatabaseSettings,
    #[serde(default)]
    prompts: RawPromptSettings,
    #[serde(default)]
    limits: RawLimitSettings,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProviderSettings {
    api_key: Option<String>,
    base_url: Option<String>,
    model: Option<String>,
    max_tokens: Option<u32>,
    timeout_seconds: Option<u64>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMcpSettings {
    connect_timeout_seconds: Option<u64>,
    call_timeout_seconds: Option<u64>,
    max_tool_rounds: Option<u32>,
    #[serde(default)]
    servers: Vec<RawMcpServerSettings>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMcpServerSettings {
    name: String,
    url: String,
    bearer_token: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSchedulerSettings {
    timezone: Option<String>,
    confirmation_timeout_minutes: Option<u64>,
    run_timeout_seconds: Option<u64>,
    lock_path: Option<PathBuf>,
    binary_path: Option<PathBuf>,
    crontab_binary: Option<PathBuf>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDatabaseSettings {
    path: Option<PathBuf>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPromptSettings {
    interactive_system: Option<String>,
    cron_system: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLimitSettings {
    provider_request_bytes: Option<usize>,
    message_bytes: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawClientSettings {
    ssh_binary: Option<PathBuf>,
    ssh_host: String,
    remote_command: Option<String>,
}

#[derive(Clone)]
pub struct ProviderSettings {
    api_key: String,
    base_url: Url,
    model: String,
    max_tokens: u32,
    timeout: Duration,
}

impl ProviderSettings {
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn max_tokens(&self) -> u32 {
        self.max_tokens
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

impl fmt::Debug for ProviderSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderSettings")
            .field("api_key", &"[redacted]")
            .field("base_url", &"[redacted]")
            .field("model", &self.model)
            .field("max_tokens", &self.max_tokens)
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[derive(Clone)]
pub struct McpServerSettings {
    name: String,
    url: Url,
    bearer_token: Option<String>,
}

impl McpServerSettings {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn bearer_token(&self) -> Option<&str> {
        self.bearer_token.as_deref()
    }
}

impl fmt::Debug for McpServerSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpServerSettings")
            .field("name", &self.name)
            .field("url", &"[redacted]")
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct McpSettings {
    connect_timeout: Duration,
    call_timeout: Duration,
    max_tool_rounds: u32,
    servers: Vec<McpServerSettings>,
}

impl McpSettings {
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    pub fn call_timeout(&self) -> Duration {
        self.call_timeout
    }

    pub fn max_tool_rounds(&self) -> u32 {
        self.max_tool_rounds
    }

    pub fn servers(&self) -> &[McpServerSettings] {
        &self.servers
    }
}

#[derive(Clone, Debug)]
pub struct SchedulerSettings {
    timezone: Tz,
    confirmation_timeout: Duration,
    run_timeout: Duration,
    lock_path: PathBuf,
    binary_path: PathBuf,
    crontab_binary: PathBuf,
}

impl SchedulerSettings {
    pub fn timezone(&self) -> Tz {
        self.timezone
    }

    pub fn confirmation_timeout_minutes(&self) -> u64 {
        self.confirmation_timeout.as_secs() / 60
    }

    pub fn confirmation_timeout(&self) -> Duration {
        self.confirmation_timeout
    }

    pub fn run_timeout(&self) -> Duration {
        self.run_timeout
    }

    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }

    pub fn binary_path(&self) -> &Path {
        &self.binary_path
    }

    pub fn crontab_binary(&self) -> &Path {
        &self.crontab_binary
    }
}

#[derive(Clone)]
pub struct ServerSettings {
    provider: ProviderSettings,
    mcp: McpSettings,
    scheduler: SchedulerSettings,
    database_path: PathBuf,
    interactive_system_prompt: String,
    cron_system_prompt: String,
    max_provider_request_bytes: usize,
    max_message_bytes: usize,
}

impl fmt::Debug for ServerSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerSettings")
            .field("provider", &self.provider)
            .field("mcp", &self.mcp)
            .field("scheduler", &self.scheduler)
            .field("database_path", &self.database_path)
            .field("interactive_system_prompt", &"[redacted]")
            .field("cron_system_prompt", &"[redacted]")
            .field(
                "max_provider_request_bytes",
                &self.max_provider_request_bytes,
            )
            .field("max_message_bytes", &self.max_message_bytes)
            .finish()
    }
}

impl ServerSettings {
    /// `env_api_key` is the caller-supplied value of `DEEPSEEK_API_KEY`.
    pub fn load(path: &Path, env_api_key: Option<String>) -> Result<Self, SettingsError> {
        let input = fs::read_to_string(path)?;
        let raw: RawServerSettings = toml::from_str(&input).map_err(|_| SettingsError::Toml)?;
        let key = env_api_key
            .or(raw.provider.api_key)
            .filter(|key| !key.trim().is_empty())
            .ok_or(SettingsError::Invalid(
                "provider.api_key / DEEPSEEK_API_KEY",
            ))?;
        let provider = ProviderSettings {
            api_key: key,
            base_url: checked_http_url(
                raw.provider
                    .base_url
                    .as_deref()
                    .unwrap_or("https://api.deepseek.com"),
                "provider.base_url",
            )?,
            model: nonblank(
                raw.provider
                    .model
                    .unwrap_or_else(|| "deepseek-v4-flash".into()),
                "provider.model",
            )?,
            max_tokens: positive(
                raw.provider.max_tokens.unwrap_or(4096),
                "provider.max_tokens",
            )?,
            timeout: Duration::from_secs(positive(
                raw.provider.timeout_seconds.unwrap_or(120),
                "provider.timeout_seconds",
            )?),
        };

        let mut names = HashSet::new();
        let mut servers = Vec::with_capacity(raw.mcp.servers.len());
        for server in raw.mcp.servers {
            if server.name.is_empty()
                || server.name.len() > 64
                || server.name.contains("__")
                || !server
                    .name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                || !names.insert(server.name.clone())
            {
                return Err(SettingsError::Invalid("mcp.servers.name"));
            }
            let token = server
                .bearer_token
                .map(|value| nonblank(value, "mcp.servers.bearer_token"))
                .transpose()?;
            servers.push(McpServerSettings {
                name: server.name,
                url: checked_http_url(&server.url, "mcp.servers.url")?,
                bearer_token: token,
            });
        }
        let mcp = McpSettings {
            connect_timeout: Duration::from_secs(positive(
                raw.mcp.connect_timeout_seconds.unwrap_or(10),
                "mcp.connect_timeout_seconds",
            )?),
            call_timeout: Duration::from_secs(positive(
                raw.mcp.call_timeout_seconds.unwrap_or(30),
                "mcp.call_timeout_seconds",
            )?),
            max_tool_rounds: positive(raw.mcp.max_tool_rounds.unwrap_or(8), "mcp.max_tool_rounds")?,
            servers,
        };

        let timezone = raw
            .scheduler
            .timezone
            .as_deref()
            .unwrap_or("Europe/Moscow")
            .parse::<Tz>()
            .map_err(|_| SettingsError::Invalid("scheduler.timezone"))?;
        let confirmation_minutes = positive(
            raw.scheduler.confirmation_timeout_minutes.unwrap_or(5),
            "scheduler.confirmation_timeout_minutes",
        )?;
        let scheduler = SchedulerSettings {
            timezone,
            confirmation_timeout: Duration::from_secs(confirmation_minutes.checked_mul(60).ok_or(
                SettingsError::Invalid("scheduler.confirmation_timeout_minutes"),
            )?),
            run_timeout: Duration::from_secs(positive(
                raw.scheduler.run_timeout_seconds.unwrap_or(600),
                "scheduler.run_timeout_seconds",
            )?),
            lock_path: raw
                .scheduler
                .lock_path
                .unwrap_or_else(|| PathBuf::from("/run/lock/light-agent-cron.lock")),
            binary_path: raw
                .scheduler
                .binary_path
                .unwrap_or_else(|| PathBuf::from("/opt/light-agent/bin/light-agent")),
            crontab_binary: raw
                .scheduler
                .crontab_binary
                .unwrap_or_else(|| PathBuf::from("/usr/bin/crontab")),
        };
        if !scheduler_path_is_safe(&scheduler.lock_path, false)
            || !scheduler_path_is_safe(&scheduler.binary_path, true)
            || !scheduler_path_is_safe(&scheduler.crontab_binary, true)
        {
            return Err(SettingsError::Invalid("scheduler paths"));
        }

        let max_provider_request_bytes = bounded(
            raw.limits
                .provider_request_bytes
                .unwrap_or(DEFAULT_PROVIDER_REQUEST_BYTES),
            DEFAULT_PROVIDER_REQUEST_BYTES,
            "limits.provider_request_bytes",
        )?;
        let max_message_bytes = bounded(
            raw.limits.message_bytes.unwrap_or(DEFAULT_MESSAGE_BYTES),
            DEFAULT_MESSAGE_BYTES,
            "limits.message_bytes",
        )?;

        Ok(Self {
            provider,
            mcp,
            scheduler,
            database_path: raw
                .database
                .path
                .unwrap_or_else(|| PathBuf::from("/var/lib/light-agent/light-agent.sqlite3")),
            interactive_system_prompt: nonblank(
                raw.prompts
                    .interactive_system
                    .unwrap_or_else(|| "You are a helpful assistant.".into()),
                "prompts.interactive_system",
            )?,
            cron_system_prompt: nonblank(
                raw.prompts.cron_system.unwrap_or_else(|| {
                    "You are a helpful assistant executing a scheduled task.".into()
                }),
                "prompts.cron_system",
            )?,
            max_provider_request_bytes,
            max_message_bytes,
        })
    }

    pub fn provider(&self) -> &ProviderSettings {
        &self.provider
    }
    pub fn mcp(&self) -> &McpSettings {
        &self.mcp
    }
    pub fn scheduler(&self) -> &SchedulerSettings {
        &self.scheduler
    }
    pub fn database_path(&self) -> &Path {
        &self.database_path
    }
    pub fn interactive_system_prompt(&self) -> &str {
        &self.interactive_system_prompt
    }
    pub fn cron_system_prompt(&self) -> &str {
        &self.cron_system_prompt
    }
    pub fn max_provider_request_bytes(&self) -> usize {
        self.max_provider_request_bytes
    }
    pub fn max_message_bytes(&self) -> usize {
        self.max_message_bytes
    }
}

#[derive(Clone, Debug)]
pub struct ClientSettings {
    ssh_binary: PathBuf,
    ssh_host: String,
}

impl ClientSettings {
    pub fn load(path: &Path) -> Result<Self, SettingsError> {
        let input = fs::read_to_string(path)?;
        let raw: RawClientSettings = toml::from_str(&input).map_err(|_| SettingsError::Toml)?;
        if raw.remote_command.as_deref().unwrap_or(REMOTE_COMMAND) != REMOTE_COMMAND {
            return Err(SettingsError::Invalid("remote_command"));
        }
        let ssh_host = nonblank(raw.ssh_host, "ssh_host")?;
        if ssh_host.len() > 255
            || !ssh_host
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
            || !ssh_host
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
        {
            return Err(SettingsError::Invalid("ssh_host"));
        }
        let ssh_binary = raw.ssh_binary.unwrap_or_else(|| PathBuf::from("ssh"));
        if !ssh_binary_is_safe(&ssh_binary) {
            return Err(SettingsError::Invalid("ssh_binary"));
        }
        Ok(Self {
            ssh_binary,
            ssh_host,
        })
    }

    pub fn ssh_binary(&self) -> &Path {
        &self.ssh_binary
    }
    pub fn ssh_host(&self) -> &str {
        &self.ssh_host
    }
    pub fn remote_command(&self) -> &'static str {
        REMOTE_COMMAND
    }
}

fn ssh_binary_is_safe(path: &Path) -> bool {
    let Some(text) = path.to_str() else {
        return false;
    };
    if text.is_empty()
        || !text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'_' | b'-' | b'.'))
    {
        return false;
    }
    if path.is_absolute() {
        path.components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
    } else {
        path.components().count() == 1
    }
}

fn checked_http_url(value: &str, field: &'static str) -> Result<Url, SettingsError> {
    // Url::parse normalizes empty userinfo ("https://@host") away, so inspect
    // the original authority before relying on the parsed username/password.
    let authority = value
        .split_once("://")
        .map(|(_, rest)| rest.split(['/', '?', '#', '\\']).next().unwrap_or(""));
    if authority.is_none_or(|authority| authority.contains('@')) {
        return Err(SettingsError::Invalid(field));
    }
    let url = Url::parse(value).map_err(|_| SettingsError::Invalid(field))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(SettingsError::Invalid(field));
    }
    Ok(url)
}

fn nonblank(value: String, field: &'static str) -> Result<String, SettingsError> {
    if value.trim().is_empty() {
        Err(SettingsError::Invalid(field))
    } else {
        Ok(value)
    }
}

fn positive<T>(value: T, field: &'static str) -> Result<T, SettingsError>
where
    T: PartialEq + From<u8>,
{
    if value == T::from(0) {
        Err(SettingsError::Invalid(field))
    } else {
        Ok(value)
    }
}

fn bounded(value: usize, maximum: usize, field: &'static str) -> Result<usize, SettingsError> {
    if value == 0 || value > maximum {
        Err(SettingsError::Invalid(field))
    } else {
        Ok(value)
    }
}

fn scheduler_path_is_safe(path: &Path, rendered_or_executed: bool) -> bool {
    if !path.is_absolute() || path.as_os_str().is_empty() {
        return false;
    }
    let Some(text) = path.to_str() else {
        return false;
    };
    if text.contains(['\0', '\r', '\n']) {
        return false;
    }
    !rendered_or_executed
        || text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'_' | b'-' | b'.'))
}
