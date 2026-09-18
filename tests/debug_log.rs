use deepseek_cli::chat::{Message, Role};
use deepseek_cli::config::ContextStrategy;
use deepseek_cli::debug_log::{DebugLog, RequestMetadata};
use deepseek_cli::system_context::{CompactionPolicy, ContextScope, SystemBlockMetadata};

#[test]
fn payload_content_is_opt_in_and_api_key_is_always_redacted() {
    let directory = tempfile::tempdir().unwrap();
    let safe_path = directory.path().join("safe.jsonl");
    let full_path = directory.path().join("full.jsonl");
    let messages = vec![Message::for_request(Role::User, "private text secret-key")];
    let metadata = RequestMetadata::new(
        ContextStrategy::StickyFacts,
        vec![
            SystemBlockMetadata {
                name: "base".into(),
                scope: ContextScope::Application,
                compaction: CompactionPolicy::Exclude,
            },
            SystemBlockMetadata {
                name: "facts".into(),
                scope: ContextScope::Conversation,
                compaction: CompactionPolicy::Exclude,
            },
        ],
        1,
        0,
        3,
    );

    let mut safe = DebugLog::new(Some(safe_path.clone()), false, "secret-key");
    assert_eq!(safe.log_request("chat", &messages, &metadata), None);
    let safe_text = std::fs::read_to_string(safe_path).unwrap();
    assert!(safe_text.contains("\"content_chars\":23"));
    assert!(safe_text.contains("\"strategy\":\"sticky_facts\""));
    assert!(safe_text.contains("\"system_block_names\":[\"base\",\"facts\"]"));
    assert!(safe_text.contains(r#""name":"facts","scope":"conversation","compaction":"exclude""#));
    assert!(safe_text.contains("\"facts_boundary\":3"));
    assert!(!safe_text.contains("private text"));
    assert!(!safe_text.contains("secret-key"));

    let mut full = DebugLog::new(Some(full_path.clone()), true, "secret-key");
    assert_eq!(full.log_request("chat", &messages, &metadata), None);
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
