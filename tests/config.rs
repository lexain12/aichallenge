use std::io::Write;
use std::path::Path;

use deepseek_cli::config::{Config, ContextStrategy};
use tempfile::NamedTempFile;

fn write_config(contents: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("create temporary config");
    file.write_all(contents.as_bytes())
        .expect("write temporary config");
    file
}

#[test]
fn applies_defaults_and_reads_file_key() {
    let file = write_config("api_key = \"file-key\"\n[context]\nstrategy = \"summary\"");

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
fn accepts_app_wide_invariants_in_toml() {
    let config = Config::from_toml(
        "api_key='key'\n[context]\nstrategy='summary'\n[[invariants]]\nid='STACK'\ntext='Use Rust only'",
        None,
    );
    assert!(
        config.is_ok(),
        "configured invariants should be accepted: {config:?}"
    );
    let rules = config.unwrap().invariants().to_vec();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].id, "STACK");
    assert_eq!(rules[0].text, "Use Rust only");
}

#[test]
fn rejects_invalid_and_duplicate_configured_invariants() {
    for invariants in [
        "[[invariants]]\nid='bad.id'\ntext='Use Rust'",
        "[[invariants]]\nid='STACK'\ntext='   '",
        "[[invariants]]\nid='STACK'\ntext='Use Rust'\n[[invariants]]\nid='STACK'\ntext='Use Go'",
    ] {
        let error = Config::from_toml(
            &format!("api_key='key'\n[context]\nstrategy='summary'\n{invariants}"),
            None,
        )
        .expect_err("invalid invariant must fail config load");
        assert!(error.to_string().contains("invariants"), "{error}");
    }
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

[context]
strategy = "summary"
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
    let file = write_config("api_key = \"file-key\"\n[context]\nstrategy = \"summary\"");

    let config = Config::load(file.path(), Some("env-key".into())).expect("load config");

    assert_eq!(config.api_key(), "env-key");
}

#[test]
fn environment_key_allows_omitting_file_key() {
    let file = write_config("[context]\nstrategy = \"summary\"");

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

#[test]
fn applies_context_and_debug_defaults() {
    let config =
        Config::from_toml("api_key = \"key\"\n[context]\nstrategy = \"summary\"", None).unwrap();

    assert_eq!(config.context().strategy(), ContextStrategy::Summary);
    assert_eq!(config.context().compact_after_prompt_tokens(), 6000);
    assert_eq!(config.context().keep_last_messages(), 10);
    assert_eq!(config.context().summary_max_tokens(), 1024);
    assert_eq!(config.context().facts_max_tokens(), 512);
    assert_eq!(config.debug().log_path(), None);
    assert!(!config.debug().log_payloads());
}

#[test]
fn reads_context_and_debug_overrides() {
    let config = Config::from_toml(
        r#"
api_key = "key"

[context]
strategy = "sticky_facts"
compact_after_prompt_tokens = 321
keep_last_messages = 4
summary_max_tokens = 77
facts_max_tokens = 55

[debug]
log_path = "logs/context.jsonl"
log_payloads = true
"#,
        None,
    )
    .unwrap();

    assert_eq!(config.context().strategy(), ContextStrategy::StickyFacts);
    assert_eq!(config.context().compact_after_prompt_tokens(), 321);
    assert_eq!(config.context().keep_last_messages(), 4);
    assert_eq!(config.context().summary_max_tokens(), 77);
    assert_eq!(config.context().facts_max_tokens(), 55);
    assert_eq!(
        config.debug().log_path(),
        Some(Path::new("logs/context.jsonl"))
    );
    assert!(config.debug().log_payloads());
}

#[test]
fn rejects_zero_context_limits_and_unknown_nested_fields() {
    for field in [
        "compact_after_prompt_tokens",
        "keep_last_messages",
        "summary_max_tokens",
    ] {
        let text = format!("api_key = \"key\"\n[context]\nstrategy = \"summary\"\n{field} = 0");
        let error = Config::from_toml(&text, None).unwrap_err().to_string();
        assert!(error.contains(field), "unexpected error: {error}");
    }

    let error = Config::from_toml(
        "api_key = \"key\"\n[context]\nstrategy = \"summary\"\nunknown = 1",
        None,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("parse"));
}

#[test]
fn requires_context_strategy_and_accepts_all_four_values() {
    let missing = Config::from_toml("api_key = \"key\"", None).unwrap_err();
    assert!(missing.to_string().contains("strategy"));

    for (text, expected) in [
        ("summary", ContextStrategy::Summary),
        ("sliding_window", ContextStrategy::SlidingWindow),
        ("sticky_facts", ContextStrategy::StickyFacts),
        ("branching", ContextStrategy::Branching),
    ] {
        let source = format!("api_key = \"key\"\n[context]\nstrategy = \"{text}\"");
        assert_eq!(
            Config::from_toml(&source, None)
                .unwrap()
                .context()
                .strategy(),
            expected
        );
    }
}

#[test]
fn rejects_removed_enabled_and_zero_facts_limit() {
    assert!(
        Config::from_toml(
            "api_key = \"key\"\n[context]\nstrategy = \"summary\"\nenabled = true",
            None,
        )
        .is_err()
    );
    let error = Config::from_toml(
        "api_key = \"key\"\n[context]\nstrategy = \"sticky_facts\"\nfacts_max_tokens = 0",
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("facts_max_tokens"));
}

#[test]
fn workflow_defaults_are_bounded_and_inherit_the_chat_model() {
    let config = Config::from_toml(
        "api_key = \"key\"\nmodel = \"chat-model\"\n[context]\nstrategy = \"summary\"",
        None,
    )
    .unwrap();
    let workflow = config.workflow();
    assert!(workflow.enabled());
    assert_eq!(workflow.interpreter_model(), "chat-model");
    assert_eq!(workflow.checker_model(), "chat-model");
    assert_eq!(workflow.handoff_model(), "chat-model");
    assert_eq!(workflow.interpreter_max_tokens(), 512);
    assert_eq!(workflow.checker_max_tokens(), 1024);
    assert_eq!(workflow.handoff_max_tokens(), 2048);
    assert_eq!(workflow.min_confidence(), 0.80);
    assert_eq!(workflow.max_autonomous_turns(), 8);
    assert_eq!(workflow.max_autonomous_tokens(), 20_000);
}

#[test]
fn workflow_rejects_zero_limits_and_confidence_outside_zero_to_one() {
    for source in [
        "api_key='key'\n[context]\nstrategy='summary'\n[workflow]\nchecker_max_tokens=0",
        "api_key='key'\n[context]\nstrategy='summary'\n[workflow]\nmax_autonomous_turns=0",
        "api_key='key'\n[context]\nstrategy='summary'\n[workflow]\nmin_confidence=1.1",
    ] {
        assert!(Config::from_toml(source, None).is_err());
    }
}
