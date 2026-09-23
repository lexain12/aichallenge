use deepseek_cli::dialog::DialogStore;
use deepseek_cli::invariants::{
    InvariantRepository, InvariantRule, InvariantSet, InvariantVerdict, parse_invariant_verdict,
};
use deepseek_cli::memory::{ContextProvider, RequestScope};
use deepseek_cli::system_context::{CompactionPolicy, ContextScope};

#[test]
fn project_rules_survive_restart_and_never_cross_scope() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    let alice = RequestScope::new("alice", "parser").unwrap();
    let other_task = RequestScope::new("alice", "bot").unwrap();
    let other_user = RequestScope::new("bob", "parser").unwrap();
    {
        let mut store = DialogStore::open(&path).unwrap();
        store
            .upsert_invariant(&alice, "STACK", "Use Rust only")
            .unwrap();
        store
            .upsert_invariant(&alice, "ARCH", "Keep the monolith")
            .unwrap();
        store
            .upsert_invariant(&alice, "STACK", "Use Rust 2024 only")
            .unwrap();
    }
    let mut store = DialogStore::open(&path).unwrap();
    let rules = store.load_invariants(&alice).unwrap();
    assert_eq!(
        rules.rules(),
        [
            InvariantRule {
                id: "ARCH".into(),
                text: "Keep the monolith".into()
            },
            InvariantRule {
                id: "STACK".into(),
                text: "Use Rust 2024 only".into()
            },
        ]
    );
    assert!(store.load_invariants(&other_task).unwrap().is_empty());
    assert!(store.load_invariants(&other_user).unwrap().is_empty());
    assert!(store.delete_invariant(&alice, "STACK").unwrap());
    assert!(!store.delete_invariant(&alice, "STACK").unwrap());
    assert_eq!(store.load_invariants(&alice).unwrap().rules().len(), 1);
}

#[test]
fn invariant_block_is_separate_from_dialog_and_excluded_from_compaction() {
    let scope = RequestScope::new("alice", "parser").unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut store = DialogStore::open(&directory.path().join("db.sqlite3")).unwrap();
    store
        .upsert_invariant(&scope, "STACK", "Use Rust only")
        .unwrap();
    let snapshot = store.load_invariants(&scope).unwrap();
    let blocks = snapshot.blocks(&scope).unwrap();
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].name(), "invariants");
    assert_eq!(blocks[0].scope(), ContextScope::Task);
    assert_eq!(blocks[0].compaction(), CompactionPolicy::Exclude);
    assert!(blocks[0].content().contains("STACK"));
    assert!(blocks[0].content().contains("Use Rust only"));
    assert!(store.latest_id().unwrap().is_none());
}

#[test]
fn blank_or_oversized_rules_are_rejected_without_mutation() {
    let scope = RequestScope::new("alice", "parser").unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut store = DialogStore::open(&directory.path().join("db.sqlite3")).unwrap();
    for (id, text) in [("", "valid"), ("bad id", "valid"), ("GOOD", "  ")] {
        assert!(store.upsert_invariant(&scope, id, text).is_err());
    }
    assert!(
        store
            .upsert_invariant(&scope, "GOOD", &"x".repeat(4097))
            .is_err()
    );
    assert!(store.load_invariants(&scope).unwrap().is_empty());
}

#[test]
fn checker_verdict_requires_known_rule_ids_and_strict_shape() {
    let scope = RequestScope::new("alice", "parser").unwrap();
    let set = InvariantSet::new(
        scope,
        vec![InvariantRule {
            id: "STACK".into(),
            text: "Use Rust only".into(),
        }],
    );
    assert_eq!(
        parse_invariant_verdict(r#"{"type":"allow"}"#, &set).unwrap(),
        InvariantVerdict::Allow
    );
    assert!(matches!(
        parse_invariant_verdict(
            r#"{"type":"deny","violations":[{"id":"STACK","reason":"Go conflicts with Rust"}]}"#,
            &set
        )
        .unwrap(),
        InvariantVerdict::Deny { .. }
    ));
    for raw in [
        r#"{"type":"allow","extra":1}"#,
        r#"{"type":"deny","violations":[]}"#,
        r#"{"type":"deny","violations":[{"id":"UNKNOWN","reason":"wrong"}]}"#,
        r#"{"type":"deny","violations":[{"id":"STACK","reason":" "}]}"#,
        "```json\n{\"type\":\"allow\"}\n```",
    ] {
        assert!(parse_invariant_verdict(raw, &set).is_err(), "{raw}");
    }
}
