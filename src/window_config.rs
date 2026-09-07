use deepseek_cli::config::Config;
use serde::{Deserialize, Serialize};
use std::{
    fs, io,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WindowSettings {
    pub api_key: String,
    pub base_url: String,
    pub model: String,
    pub system_prompt: String,
    pub temperature: f64,
    pub max_tokens: u32,
    pub timeout_seconds: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(default)]
    pub stop: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<String>,
    #[serde(default = "default_usage")]
    pub include_usage: bool,
}
fn default_usage() -> bool {
    true
}

impl WindowSettings {
    pub fn from_base(base: &Config, temperature: f64) -> Self {
        Self {
            api_key: base.api_key().into(),
            base_url: base.base_url().to_string(),
            model: base.model().into(),
            system_prompt: String::new(),
            temperature,
            max_tokens: base.max_tokens(),
            timeout_seconds: base.timeout_seconds(),
            top_p: base.top_p(),
            stop: base.stop().to_vec(),
            thinking: Some("disabled".into()),
            include_usage: base.include_usage(),
        }
    }
    pub fn config(&self) -> Result<Config, String> {
        let text = toml::to_string(self).map_err(|_| "Не удалось сериализовать настройки")?;
        Config::from_toml(&text, None).map_err(|e| e.to_string())
    }
    pub fn public_summary(&self) -> String {
        format!("model={} · temperature={} · max_tokens={} · timeout={}s · top_p={:?} · stop={:?} · thinking={} · include_usage={}\nAPI: {}\nSystem prompt:\n{}",
            self.model, self.temperature, self.max_tokens, self.timeout_seconds, self.top_p, self.stop,
            self.thinking.as_deref().unwrap_or("не отправлять"), self.include_usage,
            self.base_url, self.system_prompt).replace(&self.api_key, "[REDACTED]")
    }
    pub fn save(&self, path: &Path) -> Result<(), String> {
        self.config()?;
        let text =
            toml::to_string_pretty(self).map_err(|_| "Не удалось сериализовать настройки")?;
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let temporary = path.with_extension(format!("toml.{}.{stamp}.tmp", std::process::id()));
        let result = (|| -> io::Result<()> {
            use std::io::Write;
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temporary)?;
            file.write_all(text.as_bytes())?;
            file.sync_all()?;
            fs::rename(&temporary, path)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result.map_err(|e| format!("Не удалось сохранить {}: {e}", path.display()))
    }
}

#[derive(Clone)]
pub struct WindowConfig {
    pub windows: [WindowSettings; 4],
}
impl WindowConfig {
    pub fn from_base(base: &Config, temperatures: [f64; 4]) -> Self {
        Self {
            windows: temperatures.map(|temperature| WindowSettings::from_base(base, temperature)),
        }
    }
    pub fn path(directory: &Path, index: usize) -> PathBuf {
        directory.join(format!("window-{}.toml", index + 1))
    }
    pub fn load(directory: &Path, defaults: Self) -> Result<Self, String> {
        fs::create_dir_all(directory)
            .map_err(|e| format!("Не удалось создать {}: {e}", directory.display()))?;
        let mut result = defaults;
        // Preserve the previous temperature-only configuration when initializing panels.
        if directory.file_name().is_some_and(|name| name == "panels")
            && (0..4).any(|i| !Self::path(directory, i).exists())
        {
            let legacy_path = directory
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("windows.toml");
            match fs::read_to_string(&legacy_path) {
                Ok(text) => {
                    #[derive(Deserialize)]
                    struct LegacyWindow {
                        temperature: f64,
                    }
                    #[derive(Deserialize)]
                    struct Legacy {
                        windows: [LegacyWindow; 4],
                    }
                    let legacy: Legacy =
                        toml::from_str(&text).map_err(|_| "Некорректный старый windows.toml")?;
                    for (window, legacy) in result.windows.iter_mut().zip(legacy.windows) {
                        window.temperature = legacy.temperature;
                        window.config()?;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(format!("Не удалось прочитать старый windows.toml: {e}")),
            }
        }
        // Parse existing files before creating missing ones.
        let mut missing = Vec::new();
        for index in 0..4 {
            let path = Self::path(directory, index);
            match fs::read_to_string(&path) {
                Ok(text) => {
                    let settings: WindowSettings = toml::from_str(&text)
                        .map_err(|_| format!("Некорректный TOML в {}", path.display()))?;
                    settings
                        .config()
                        .map_err(|e| format!("Панель {}: {e}", index + 1))?;
                    result.windows[index] = settings;
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => missing.push(index),
                Err(e) => return Err(format!("Не удалось прочитать {}: {e}", path.display())),
            }
        }
        for index in missing {
            result.windows[index].save(&Self::path(directory, index))?;
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn base() -> Config {
        Config::from_toml("api_key = \"secret-test-key\"", None).unwrap()
    }

    #[test]
    fn independent_files_migrate_and_do_not_overwrite_neighbors() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("windows.toml"), "windows = [{temperature=0.2}, {temperature=0.5}, {temperature=1.4}, {temperature=1.9}]").unwrap();
        let panels = dir.path().join("panels");
        let defaults = WindowConfig::from_base(&base(), [0.0, 0.7, 1.2, 1.0]);
        let mut loaded = WindowConfig::load(&panels, defaults.clone()).unwrap();
        assert_eq!(loaded.windows[2].temperature, 1.4);
        let neighbor = fs::read(WindowConfig::path(&panels, 1)).unwrap();
        loaded.windows[0].system_prompt = "Первая строка\nВторая строка".into();
        loaded.windows[0].base_url = "http://localhost:8080/v1".into();
        loaded.windows[0].api_key = "other-private-key".into();
        loaded.windows[0].thinking = None;
        loaded.windows[0].include_usage = false;
        loaded.windows[0]
            .save(&WindowConfig::path(&panels, 0))
            .unwrap();
        let reloaded = WindowConfig::load(&panels, defaults).unwrap();
        assert!(reloaded.windows[0] == loaded.windows[0]);
        assert_eq!(fs::read(WindowConfig::path(&panels, 1)).unwrap(), neighbor);
        assert!(
            !reloaded.windows[0]
                .public_summary()
                .contains("other-private-key")
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(WindowConfig::path(&panels, 0))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        let original = fs::read(WindowConfig::path(&panels, 0)).unwrap();
        loaded.windows[0].temperature = f64::NAN;
        assert!(
            loaded.windows[0]
                .save(&WindowConfig::path(&panels, 0))
                .is_err()
        );
        assert_eq!(fs::read(WindowConfig::path(&panels, 0)).unwrap(), original);
    }

    #[test]
    fn parse_errors_do_not_expose_credentials() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            WindowConfig::path(dir.path(), 0),
            "api_key = secret-test-key",
        )
        .unwrap();
        let error = WindowConfig::load(dir.path(), WindowConfig::from_base(&base(), [0.0; 4]))
            .err()
            .unwrap();
        assert!(!error.contains("secret-test-key"));
    }
}
