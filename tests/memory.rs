use deepseek_cli::memory::{ContextProvider, DurableMemoryScope, MemorySnapshot, RequestScope};
use deepseek_cli::system_context::{CompactionPolicy, ContextScope};

#[test]
fn identifiers_are_trimmed_and_empty_identifiers_are_rejected() {
    let scope = RequestScope::new(" alice ", " bot ").unwrap();
    assert_eq!(scope.user_id(), "alice");
    assert_eq!(scope.task_id(), "bot");
    assert!(RequestScope::new(" ", "bot").is_err());
    assert!(RequestScope::new("alice", " ").is_err());
}

#[test]
fn snapshot_renders_deterministic_non_compactable_blocks() {
    let scope = RequestScope::new("alice", "bot").unwrap();
    let snapshot = MemorySnapshot::new(
        scope.clone(),
        std::collections::BTreeMap::from([("language".into(), "Russian".into())]),
        std::collections::BTreeMap::from([
            ("stack".into(), "Rust".into()),
            ("database".into(), "SQLite".into()),
        ]),
    );

    let blocks = snapshot.blocks(&scope).unwrap();
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0].name(), "user_memory");
    assert_eq!(blocks[0].scope(), ContextScope::User);
    assert_eq!(blocks[0].compaction(), CompactionPolicy::Exclude);
    assert!(blocks[0].content().contains(r#"{"language":"Russian"}"#));
    assert_eq!(blocks[1].name(), "task_memory");
    assert!(
        blocks[1]
            .content()
            .contains(r#"{"database":"SQLite","stack":"Rust"}"#)
    );
}

#[test]
fn empty_layers_do_not_create_empty_system_messages() {
    let scope = RequestScope::new("alice", "bot").unwrap();
    let snapshot = MemorySnapshot::new(scope.clone(), Default::default(), Default::default());
    assert!(snapshot.blocks(&scope).unwrap().is_empty());
    assert_eq!(scope.address(DurableMemoryScope::User).task_id(), None);
    assert_eq!(
        scope.address(DurableMemoryScope::Task).task_id(),
        Some("bot")
    );
}
