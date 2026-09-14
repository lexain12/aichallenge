use deepseek_cli::chat::{Message, Role};
use deepseek_cli::debug_log::DebugLog;

#[test]
fn payload_content_is_opt_in_and_api_key_is_always_redacted() {
    let directory = tempfile::tempdir().unwrap();
    let safe_path = directory.path().join("safe.jsonl");
    let full_path = directory.path().join("full.jsonl");
    let messages = vec![Message::for_request(
        Role::User,
        "private text secret-key",
    )];

    let mut safe = DebugLog::new(Some(safe_path.clone()), false, "secret-key");
    assert_eq!(safe.log_request("chat", &messages, 0), None);
    let safe_text = std::fs::read_to_string(safe_path).unwrap();
    assert!(safe_text.contains("\"content_chars\":23"));
    assert!(!safe_text.contains("private text"));
    assert!(!safe_text.contains("secret-key"));

    let mut full = DebugLog::new(Some(full_path.clone()), true, "secret-key");
    assert_eq!(full.log_request("chat", &messages, 0), None);
    let full_text = std::fs::read_to_string(full_path).unwrap();
    assert!(full_text.contains("private text [REDACTED]"));
    assert!(!full_text.contains("secret-key"));
}

#[test]
fn logging_failure_warns_once_and_disables_future_writes() {
    let directory = tempfile::tempdir().unwrap();
    let mut log = DebugLog::new(Some(directory.path().to_owned()), false, "key");

    let warning = log
        .log_event("first", serde_json::json!({"value": 1}))
        .expect("first failure is reported");
    assert!(warning.contains("debug log disabled"));
    assert_eq!(
        log.log_event("second", serde_json::json!({"value": 2})),
        None
    );
}
