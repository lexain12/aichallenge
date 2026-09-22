use deepseek_cli::chat::{Message, Role};
use deepseek_cli::client::TokenUsage;
use deepseek_cli::config::ContextStrategy;
use deepseek_cli::debug_log::{
    DebugLog, RequestMetadata, WorkflowDebugMetadata, WorkflowDebugPayload,
};
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

// Break caught: workflow payloads and controller/model secrets must never be
// flattened into default diagnostics or escape the opt-in payload envelope.
#[test]
fn workflow_payloads_are_metadata_only_by_default_and_nested_when_enabled() {
    let directory = tempfile::tempdir().unwrap();
    let safe_path = directory.path().join("workflow-safe.jsonl");
    let full_path = directory.path().join("workflow-full.jsonl");
    let marker = "WORKFLOW_MARKER_secret-key";
    let metadata = WorkflowDebugMetadata {
        source: "controller",
        component: "continuation_checker",
        model: "checker-model",
        mode: "advisory",
        input_version: 4,
        output_version: Some(5),
        proposed_event: Some("execution_completed"),
        accepted: false,
        autonomous_turn: 2,
        autonomous_tokens: 144,
        stage_run_id: 9,
        transition_id: Some(11),
        processing_status: "failed",
        usage: Some(TokenUsage {
            prompt_tokens: 10,
            completion_tokens: 4,
            total_tokens: 14,
            completion_tokens_details: None,
        }),
        input_chars: 31,
        output_chars: 47,
        stage_message_count: 3,
        plan_step_count: 2,
        checkpoint_item_count: 1,
    };
    let payload = WorkflowDebugPayload {
        interpreter_output: Some(marker),
        checker_output: Some(marker),
        plan: Some(marker),
        checkpoint: Some(marker),
        controller_instruction: Some(marker),
        handoff: Some(marker),
        model_prompt: Some(marker),
        model_output: Some(marker),
    };

    let mut safe = DebugLog::new(Some(safe_path.clone()), false, "secret-key");
    assert_eq!(safe.log_workflow(&metadata, Some(&payload)), None);
    let safe_value: serde_json::Value =
        serde_json::from_str(std::fs::read_to_string(safe_path).unwrap().trim()).unwrap();
    assert_eq!(safe_value["event"], "workflow");
    assert_eq!(safe_value["details"]["component"], "continuation_checker");
    assert_eq!(safe_value["details"]["autonomous_tokens"], 144);
    assert!(safe_value["details"].get("payload").is_none());
    let safe_text = safe_value.to_string();
    assert!(!safe_text.contains(marker));
    assert!(!safe_text.contains("secret-key"));

    let mut full = DebugLog::new(Some(full_path.clone()), true, "secret-key");
    assert_eq!(full.log_workflow(&metadata, Some(&payload)), None);
    let full_value: serde_json::Value =
        serde_json::from_str(std::fs::read_to_string(full_path).unwrap().trim()).unwrap();
    assert_eq!(
        full_value["details"]["payload"]["controller_instruction"],
        "WORKFLOW_MARKER_[REDACTED]"
    );
    let mut without_payload = full_value.clone();
    without_payload["details"]
        .as_object_mut()
        .unwrap()
        .remove("payload");
    assert!(!without_payload.to_string().contains("WORKFLOW_MARKER"));
    assert!(!full_value.to_string().contains("secret-key"));
}
