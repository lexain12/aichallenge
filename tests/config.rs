use std::io::Write;
use std::path::Path;

use deepseek_cli::config::Config;
use tempfile::NamedTempFile;

fn write_config(contents: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("create temporary config");
    file.write_all(contents.as_bytes())
        .expect("write temporary config");
    file
}

#[test]
fn applies_defaults_and_reads_file_key() {
    let file = write_config("api_key = \"file-key\"");

    let config = Config::load(file.path(), None).expect("load valid config");

    assert_eq!(config.api_key(), "file-key");
    assert_eq!(config.base_url().as_str(), "https://api.deepseek.com/");
    assert_eq!(config.model(), "deepseek-v4-flash");
    assert_eq!(config.system_prompt(), "You are a helpful assistant.");
    assert_eq!(config.temperature(), 1.0);
    assert_eq!(config.max_tokens(), 4096);
    assert_eq!(config.timeout_seconds(), 120);
}

#[test]
fn accepts_all_supported_overrides() {
    let file = write_config(
        r#"
api_key = "file-key"
base_url = "http://localhost:8080/api"
model = "custom-model"
system_prompt = "Answer briefly."
temperature = 0.25
max_tokens = 512
timeout_seconds = 15
"#,
    );

    let config = Config::load(file.path(), None).expect("load overridden config");

    assert_eq!(config.base_url().as_str(), "http://localhost:8080/api");
    assert_eq!(config.model(), "custom-model");
    assert_eq!(config.system_prompt(), "Answer briefly.");
    assert_eq!(config.temperature(), 0.25);
    assert_eq!(config.max_tokens(), 512);
    assert_eq!(config.timeout_seconds(), 15);
}

#[test]
fn environment_key_overrides_file_key() {
    let file = write_config("api_key = \"file-key\"");

    let config = Config::load(file.path(), Some("env-key".into())).expect("load config");

    assert_eq!(config.api_key(), "env-key");
}

#[test]
fn environment_key_allows_omitting_file_key() {
    let file = write_config("");

    let config = Config::load(file.path(), Some("env-key".into())).expect("load config");

    assert_eq!(config.api_key(), "env-key");
}

#[test]
fn rejects_missing_or_blank_api_key() {
    for contents in ["", "api_key = \"   \""] {
        let file = write_config(contents);
        let error = Config::load(file.path(), None)
            .expect_err("blank key must fail")
            .to_string();
        assert!(error.contains("api_key"), "unexpected error: {error}");
    }
}

#[test]
fn rejects_invalid_base_urls() {
    for base_url in ["not-a-url", "ftp://api.deepseek.com"] {
        let file = write_config(&format!(
            "api_key = \"file-key\"\nbase_url = \"{base_url}\""
        ));
        let error = Config::load(file.path(), None)
            .expect_err("invalid base URL must fail")
            .to_string();
        assert!(error.contains("base_url"), "unexpected error: {error}");
    }
}

#[test]
fn rejects_blank_model() {
    let file = write_config("api_key = \"file-key\"\nmodel = \"  \"");

    let error = Config::load(file.path(), None)
        .expect_err("blank model must fail")
        .to_string();

    assert!(error.contains("model"));
}

#[test]
fn rejects_out_of_range_temperature_without_exposing_key() {
    for temperature in [-0.1, 2.1] {
        let file = write_config(&format!(
            "api_key = \"secret-key\"\ntemperature = {temperature}"
        ));
        let error = Config::load(file.path(), None)
            .expect_err("invalid temperature must fail")
            .to_string();
        assert!(error.contains("temperature"));
        assert!(!error.contains("secret-key"));
    }
}

#[test]
fn rejects_zero_limits() {
    for field in ["max_tokens", "timeout_seconds"] {
        let file = write_config(&format!("api_key = \"file-key\"\n{field} = 0"));
        let error = Config::load(file.path(), None)
            .expect_err("zero limit must fail")
            .to_string();
        assert!(error.contains(field), "unexpected error: {error}");
    }
}

#[test]
fn reports_invalid_toml_without_exposing_values() {
    let file = write_config("api_key = [\"secret-key\"");

    let error = Config::load(file.path(), None)
        .expect_err("invalid TOML must fail")
        .to_string();

    assert!(error.contains("parse"));
    assert!(!error.contains("secret-key"));
}

#[test]
fn reports_missing_config_path() {
    let path = Path::new("/definitely/missing/deepseek.toml");

    let error = Config::load(path, Some("env-key".into()))
        .expect_err("missing file must fail")
        .to_string();

    assert!(error.contains("read"));
    assert!(error.contains("deepseek.toml"));
}
