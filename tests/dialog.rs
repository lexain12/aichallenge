use deepseek_cli::chat::Role;
use deepseek_cli::dialog::{DialogStore, StoreError};

#[test]
fn reopens_messages_in_order_and_lists_latest_activity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dialogs.sqlite3");
    let mut store = DialogStore::open(&path).unwrap();
    assert!(store.latest_id().unwrap().is_none());
    let first = store
        .start_dialog("Original system", "First question")
        .unwrap();
    let second = store.start_dialog("Other system", "Second dialog").unwrap();
    assert_eq!(store.latest_id().unwrap(), Some(second));
    store
        .append_message(first, 1, Role::Assistant, "First answer")
        .unwrap();
    drop(store);

    let store = DialogStore::open(&path).unwrap();
    assert_eq!(store.latest_id().unwrap(), Some(first));
    let dialog = store.load(first).unwrap();
    assert_eq!(dialog.system_prompt, "Original system");
    assert_eq!(
        dialog
            .messages
            .iter()
            .map(|m| (m.role(), m.content()))
            .collect::<Vec<_>>(),
        [
            (Role::User, "First question"),
            (Role::Assistant, "First answer")
        ]
    );
    let list = store.list().unwrap();
    assert_eq!(
        list.iter()
            .map(|d| (d.id, d.message_count))
            .collect::<Vec<_>>(),
        [(first, 2), (second, 1)]
    );
    assert_eq!(list[0].title, "First question");
}

#[test]
fn stale_writer_cannot_append_or_change_latest_dialog() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dialogs.sqlite3");
    let mut first = DialogStore::open(&path).unwrap();
    let mut second = DialogStore::open(&path).unwrap();
    let id = first.start_dialog("System", "Question").unwrap();
    first
        .append_message(id, 1, Role::Assistant, "Answer")
        .unwrap();
    let newest = first.start_dialog("System", "New dialog").unwrap();
    assert!(matches!(
        second.append_message(id, 1, Role::Assistant, "Stale answer"),
        Err(StoreError::Conflict(_))
    ));
    assert_eq!(first.load(id).unwrap().messages.len(), 2);
    assert_eq!(first.latest_id().unwrap(), Some(newest));
    assert!(matches!(first.load(9999), Err(StoreError::NotFound(9999))));
}
