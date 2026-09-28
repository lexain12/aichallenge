mod common;

use deepseek_cli::domain::DialogId;
use deepseek_cli::store::{MessageRole, SafeErrorCode, Store, StoreError};
use rusqlite::{Connection, params};
use tempfile::TempDir;

fn setup() -> (TempDir, Store, Connection) {
    let dir = common::private_tempdir();
    let path = dir.path().join("agent.sqlite");
    let store = Store::open(&path).unwrap();
    let db = Connection::open(path).unwrap();
    db.pragma_update(None, "foreign_keys", true).unwrap();
    (dir, store, db)
}

#[test]
fn migration_creates_exact_v6_schema() {
    let (dir, store, db) = setup();
    let tables: Vec<String> = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").unwrap()
        .query_map([], |r| r.get(0)).unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(
        tables,
        [
            "cron_jobs",
            "cron_runs",
            "dialogs",
            "messages",
            "runtime_coordination",
            "runtime_owners",
            "schema_version",
            "tool_runs",
            "turns"
        ]
    );
    assert_eq!(
        db.query_row("SELECT version FROM schema_version", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        6
    );
    assert_eq!(
        db.query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "wal"
    );
    let d = store.create_dialog("persisted").unwrap();
    chrono::DateTime::parse_from_rfc3339(&d.created_at).unwrap();
    let reopened = Store::open(dir.path().join("agent.sqlite")).unwrap();
    assert_eq!(reopened.list_dialogs().unwrap()[0].id, d.id);
}

#[test]
fn cron_confirmation_permission_defaults_on_and_persists_per_dialog() {
    let (dir, store, _) = setup();
    let first = store.create_dialog("first").unwrap();
    let second = store.create_dialog("second").unwrap();
    assert!(first.cron_confirmation_required);
    assert!(second.cron_confirmation_required);

    let updated = store
        .set_cron_confirmation_required(first.id, false)
        .unwrap();
    assert!(!updated.cron_confirmation_required);
    assert!(
        !store
            .get_dialog(first.id)
            .unwrap()
            .cron_confirmation_required
    );
    assert!(
        store
            .get_dialog(second.id)
            .unwrap()
            .cron_confirmation_required
    );

    drop(store);
    let reopened = Store::open(dir.path().join("agent.sqlite")).unwrap();
    assert!(
        !reopened
            .get_dialog(first.id)
            .unwrap()
            .cron_confirmation_required
    );
    assert!(
        reopened
            .get_dialog(second.id)
            .unwrap()
            .cron_confirmation_required
    );
}

#[test]
fn v5_dialogs_migrate_with_cron_confirmation_enabled() {
    let (dir, store, db) = setup();
    let dialog = store.create_dialog("legacy").unwrap();
    db.execute_batch(
        "ALTER TABLE dialogs DROP COLUMN cron_confirmation_required;
         UPDATE schema_version SET version=5;",
    )
    .unwrap();
    drop(db);
    drop(store);

    let reopened = Store::open(dir.path().join("agent.sqlite")).unwrap();
    assert!(
        reopened
            .get_dialog(dialog.id)
            .unwrap()
            .cron_confirmation_required
    );
}

#[cfg(unix)]
#[test]
fn store_rejects_group_or_world_writable_nonsticky_ancestor() {
    use std::os::unix::fs::PermissionsExt;

    let root = common::private_tempdir();
    let unsafe_ancestor = root.path().join("unsafe");
    let database_directory = unsafe_ancestor.join("database");
    std::fs::create_dir_all(&database_directory).unwrap();
    std::fs::set_permissions(&unsafe_ancestor, std::fs::Permissions::from_mode(0o777)).unwrap();
    std::fs::set_permissions(&database_directory, std::fs::Permissions::from_mode(0o700)).unwrap();

    assert!(matches!(
        Store::open(database_directory.join("agent.sqlite")),
        Err(StoreError::InvalidPath)
    ));
}

#[cfg(unix)]
#[test]
fn store_rejects_a_symlink_in_the_configured_ancestor_chain() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let root = common::private_tempdir();
    let real_ancestor = root.path().join("real");
    let database_directory = real_ancestor.join("database");
    std::fs::create_dir_all(&database_directory).unwrap();
    std::fs::set_permissions(&real_ancestor, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::set_permissions(&database_directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let alias = root.path().join("alias");
    symlink(&real_ancestor, &alias).unwrap();

    assert!(matches!(
        Store::open(alias.join("database/agent.sqlite")),
        Err(StoreError::InvalidPath)
    ));
}

#[cfg(unix)]
#[test]
fn store_accepts_a_root_owned_or_current_uid_sticky_writable_ancestor() {
    use std::os::unix::fs::PermissionsExt;

    let root = common::private_tempdir();
    let sticky_ancestor = root.path().join("sticky");
    let database_directory = sticky_ancestor.join("database");
    std::fs::create_dir_all(&database_directory).unwrap();
    std::fs::set_permissions(&sticky_ancestor, std::fs::Permissions::from_mode(0o1777)).unwrap();
    std::fs::set_permissions(&database_directory, std::fs::Permissions::from_mode(0o700)).unwrap();

    Store::open(database_directory.join("agent.sqlite")).unwrap();
}

#[cfg(unix)]
#[test]
fn store_detects_replaced_ancestor_when_parent_and_database_inodes_are_preserved() {
    use std::os::unix::fs::PermissionsExt;

    let root = common::private_tempdir();
    let ancestor = root.path().join("ancestor");
    let database_directory = ancestor.join("database");
    std::fs::create_dir_all(&database_directory).unwrap();
    std::fs::set_permissions(&ancestor, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::set_permissions(&database_directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let store = Store::open(database_directory.join("agent.sqlite")).unwrap();

    let displaced = root.path().join("displaced");
    std::fs::rename(&ancestor, &displaced).unwrap();
    std::fs::create_dir(&ancestor).unwrap();
    std::fs::set_permissions(&ancestor, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::rename(displaced.join("database"), &database_directory).unwrap();

    assert_eq!(
        store.create_dialog("must fail closed"),
        Err(StoreError::InvalidPath)
    );
}

#[test]
fn dialog_pages_are_keyset_bounded_and_cover_each_dialog_once() {
    let (_dir, store, _) = setup();
    let created = (0..7)
        .map(|index| store.create_dialog(&format!("dialog-{index}")).unwrap().id)
        .collect::<Vec<_>>();

    let first = store.list_dialogs_page(None, 3).unwrap();
    let second = store
        .list_dialogs_page(first.last().map(|dialog| dialog.id), 3)
        .unwrap();
    let third = store
        .list_dialogs_page(second.last().map(|dialog| dialog.id), 3)
        .unwrap();
    let done = store
        .list_dialogs_page(third.last().map(|dialog| dialog.id), 3)
        .unwrap();

    assert_eq!(first.len(), 3);
    assert_eq!(second.len(), 3);
    assert_eq!(third.len(), 1);
    assert!(done.is_empty());
    assert_eq!(
        first
            .into_iter()
            .chain(second)
            .chain(third)
            .map(|dialog| dialog.id)
            .collect::<Vec<_>>(),
        created
    );
    assert_eq!(
        store.list_dialogs_page(None, 0),
        Err(StoreError::InvalidMetadata)
    );
}

#[test]
fn completed_history_excludes_failed_interrupted_and_pending_turns() {
    let (_dir, store, _) = setup();
    let d = store.create_dialog("test").unwrap().id;
    let first = store.begin_turn(d, "first").unwrap();
    store.complete_turn(first.turn_id, "answer 1").unwrap();
    let failed = store.begin_turn(d, "failed").unwrap();
    store
        .fail_turn(failed.turn_id, SafeErrorCode::ProviderError)
        .unwrap();
    let interrupted = store.begin_turn(d, "interrupted").unwrap();
    store
        .interrupt_turn(interrupted.turn_id, SafeErrorCode::Interrupted)
        .unwrap();
    let last = store.begin_turn(d, "last").unwrap();
    store.complete_turn(last.turn_id, "answer 2").unwrap();
    store.begin_turn(d, "pending").unwrap();
    let history = store.completed_messages(d).unwrap();
    assert_eq!(
        history
            .iter()
            .map(|m| m.content.as_str())
            .collect::<Vec<_>>(),
        ["first", "answer 1", "last", "answer 2"]
    );
    assert_eq!(history[0].role, MessageRole::User);
    assert_eq!(history[1].role, MessageRole::Assistant);
}

#[test]
fn begin_turn_persists_user_before_provider_work() {
    let (_dir, store, db) = setup();
    let d = store.create_dialog("test").unwrap().id;
    let started = store.begin_turn(d, "durable input").unwrap();
    let row = db.query_row("SELECT t.status,m.role,m.content,m.id FROM turns t JOIN messages m ON m.turn_id=t.id WHERE t.id=?", [started.turn_id.get()], |r| Ok((r.get::<_, String>(0)?,r.get::<_, String>(1)?,r.get::<_, String>(2)?,r.get::<_, i64>(3)?))).unwrap();
    assert_eq!(
        row,
        (
            "pending".into(),
            "user".into(),
            "durable input".into(),
            started.user_message_id
        )
    );
    assert!(store.completed_messages(d).unwrap().is_empty());
}

#[test]
fn begin_turn_rolls_back_if_user_insert_fails() {
    let (_dir, store, db) = setup();
    let d = store.create_dialog("test").unwrap().id;
    db.execute_batch("CREATE TRIGGER reject_user BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT,'secret failure'); END;").unwrap();
    let error = store.begin_turn(d, "secret input").unwrap_err();
    assert!(!format!("{error:?} {error}").contains("secret"));
    assert_eq!(
        db.query_row("SELECT count(*) FROM turns", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn ignored_required_writes_cannot_commit_partial_turns() {
    let (_dir, store, db) = setup();
    let d = store.create_dialog("test").unwrap().id;
    db.execute_batch(
        "CREATE TRIGGER ignore_user BEFORE INSERT ON messages BEGIN SELECT RAISE(IGNORE); END;",
    )
    .unwrap();
    assert!(matches!(
        store.begin_turn(d, "question"),
        Err(StoreError::Conflict)
    ));
    assert_eq!(
        db.query_row("SELECT count(*) FROM turns", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    db.execute_batch("DROP TRIGGER ignore_user;").unwrap();
    let t = store.begin_turn(d, "question").unwrap().turn_id;
    db.execute_batch("CREATE TRIGGER ignore_status BEFORE UPDATE OF status ON turns BEGIN SELECT RAISE(IGNORE); END;").unwrap();
    assert!(matches!(
        store.complete_turn(t, "answer"),
        Err(StoreError::Conflict)
    ));
    assert_eq!(
        db.query_row("SELECT count(*) FROM messages", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert!(matches!(
        store.fail_turn(t, SafeErrorCode::ProviderError),
        Err(StoreError::Conflict)
    ));
}

#[test]
fn complete_turn_commits_assistant_and_status_atomically() {
    let (_dir, store, db) = setup();
    let d = store.create_dialog("test").unwrap().id;
    let t = store.begin_turn(d, "question").unwrap().turn_id;
    db.execute_batch("CREATE TRIGGER reject_completion BEFORE UPDATE OF status ON turns WHEN NEW.status='completed' BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert!(store.complete_turn(t, "answer").is_err());
    assert_eq!(
        db.query_row("SELECT count(*) FROM messages", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        db.query_row("SELECT status FROM turns", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "pending"
    );
    db.execute_batch("DROP TRIGGER reject_completion").unwrap();
    store.complete_turn(t, "answer").unwrap();
    store.complete_turn(t, "answer").unwrap();
    assert!(matches!(
        store.complete_turn(t, "different"),
        Err(StoreError::Conflict)
    ));
    assert_eq!(store.completed_messages(d).unwrap().len(), 2);
}

#[test]
fn second_active_turn_in_same_dialog_is_busy() {
    let (_dir, store, db) = setup();
    let d = store.create_dialog("test").unwrap().id;
    store.begin_turn(d, "first").unwrap();
    assert!(matches!(
        store.clone().begin_turn(d, "second"),
        Err(StoreError::Busy)
    ));
    let pending = db
        .query_row("SELECT id FROM turns WHERE status='pending'", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();
    store
        .interrupt_turn(
            deepseek_cli::domain::TurnId::new(pending).unwrap(),
            SafeErrorCode::Interrupted,
        )
        .unwrap();
    store.begin_turn(d, "after recovery").unwrap();
}

#[test]
fn different_dialogs_can_write_concurrently() {
    let (_dir, store, _) = setup();
    let dialogs: Vec<_> = (0..8)
        .map(|_| store.create_dialog("parallel").unwrap().id)
        .collect();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(dialogs.len()));
    let threads: Vec<_> = dialogs
        .into_iter()
        .map(|id| {
            let store = store.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let t = store.begin_turn(id, "question").unwrap();
                store.complete_turn(t.turn_id, "answer").unwrap();
                assert_eq!(store.completed_messages(id).unwrap().len(), 2);
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn process_writer_helper() {
    use std::io::Write;
    let Some(path) = std::env::var_os("STORE_TEST_WRITER_DB") else {
        return;
    };
    let db = Connection::open(path).unwrap();
    db.execute_batch("BEGIN IMMEDIATE; UPDATE dialogs SET title='uncommitted';")
        .unwrap();
    println!("writer-ready");
    std::io::stdout().flush().unwrap();
    let mut release = String::new();
    std::io::stdin().read_line(&mut release).unwrap();
    db.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn readers_work_while_another_process_writes() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    let (dir, store, _) = setup();
    store.create_dialog("committed").unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "process_writer_helper", "--nocapture"])
        .env("STORE_TEST_WRITER_DB", dir.path().join("agent.sqlite"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    loop {
        let mut line = String::new();
        assert_ne!(
            output.read_line(&mut line).unwrap(),
            0,
            "writer exited before transaction"
        );
        if line.contains("writer-ready") {
            break;
        }
    }
    let result = store.list_dialogs();
    drop(child.stdin.take());
    assert!(child.wait().unwrap().success());
    assert_eq!(result.unwrap()[0].title, "committed");
}

#[test]
fn schema_rejects_cross_dialog_messages_and_duplicate_roles() {
    let (_dir, store, db) = setup();
    let a = store.create_dialog("a").unwrap().id;
    let b = store.create_dialog("b").unwrap().id;
    let t = store.begin_turn(a, "question").unwrap().turn_id;
    assert!(db.execute("INSERT INTO messages(dialog_id,turn_id,role,content) VALUES(?,?,'assistant','wrong owner')", params![b.get(),t.get()]).is_err());
    assert!(
        db.execute(
            "INSERT INTO messages(dialog_id,turn_id,role,content) VALUES(?,?,'user','duplicate')",
            params![a.get(), t.get()]
        )
        .is_err()
    );
    assert!(
        db.execute(
            "INSERT INTO turns(dialog_id,status) VALUES(?,'pending')",
            [a.get()]
        )
        .is_err()
    );
}

#[test]
fn missing_dialogs_and_invalid_turn_transitions_are_rejected() {
    let (_dir, store, _) = setup();
    let absent = DialogId::new(999).unwrap();
    assert!(matches!(
        store.begin_turn(absent, "input"),
        Err(StoreError::NotFound)
    ));
    assert!(matches!(
        store.rename_dialog(absent, "title"),
        Err(StoreError::NotFound)
    ));
    assert!(matches!(
        store.delete_dialog(absent),
        Err(StoreError::NotFound)
    ));
    assert!(matches!(
        store.completed_messages(absent),
        Err(StoreError::NotFound)
    ));
    let d = store.create_dialog("old").unwrap().id;
    store.rename_dialog(d, "new").unwrap();
    assert_eq!(store.list_dialogs().unwrap()[0].title, "new");
    let t = store.begin_turn(d, "input").unwrap().turn_id;
    store.fail_turn(t, SafeErrorCode::ProviderError).unwrap();
    store.fail_turn(t, SafeErrorCode::ProviderError).unwrap();
    assert!(matches!(
        store.fail_turn(t, SafeErrorCode::InternalError),
        Err(StoreError::Conflict)
    ));
    assert!(matches!(
        store.complete_turn(t, "late answer"),
        Err(StoreError::Conflict)
    ));
}

#[test]
fn incompatible_schemas_and_memory_databases_are_rejected() {
    let dir = common::private_tempdir();
    let path = dir.path().join("legacy.sqlite");
    Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE dialogs(id INTEGER PRIMARY KEY, legacy TEXT);")
        .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    assert!(matches!(
        Store::open(&path),
        Err(StoreError::UnsupportedSchema)
    ));
    assert!(matches!(
        Store::open(":memory:"),
        Err(StoreError::InvalidPath)
    ));
    let (_new_dir, _store, db) = setup();
    db.execute("UPDATE schema_version SET version=7", [])
        .unwrap();
    // A future version must not be silently downgraded.
    assert!(matches!(
        Store::open(db.path().unwrap()),
        Err(StoreError::UnsupportedSchema)
    ));
}

#[cfg(unix)]
#[test]
fn store_rejects_a_symlink_database_path_before_runtime_coordination() {
    let dir = common::private_tempdir();
    let database = dir.path().join("agent.sqlite3");
    Store::open(&database).unwrap();
    let alias = dir.path().join("alias.sqlite3");
    std::os::unix::fs::symlink(&database, &alias).unwrap();

    assert!(matches!(Store::open(alias), Err(StoreError::InvalidPath)));
}

#[cfg(unix)]
#[test]
fn store_rejects_a_hardlink_database_alias() {
    let dir = common::private_tempdir();
    let database = dir.path().join("agent.sqlite3");
    Store::open(&database).unwrap();
    let alias = dir.path().join("alias.sqlite3");
    std::fs::hard_link(&database, &alias).unwrap();

    assert!(matches!(Store::open(alias), Err(StoreError::InvalidPath)));
}

#[cfg(unix)]
#[test]
fn opened_store_rejects_database_path_replacement() {
    let dir = common::private_tempdir();
    let database = dir.path().join("agent.sqlite3");
    let displaced = dir.path().join("displaced.sqlite3");
    let store = Store::open(&database).unwrap();
    std::fs::rename(&database, &displaced).unwrap();
    std::fs::File::create(&database).unwrap();

    assert!(matches!(store.list_dialogs(), Err(StoreError::InvalidPath)));
}

#[cfg(unix)]
#[test]
fn store_rejects_a_group_or_world_writable_database() {
    use std::os::unix::fs::PermissionsExt;

    let dir = common::private_tempdir();
    let database = dir.path().join("agent.sqlite3");
    Store::open(&database).unwrap();
    let mut permissions = std::fs::metadata(&database).unwrap().permissions();
    permissions.set_mode(0o666);
    std::fs::set_permissions(&database, permissions).unwrap();

    assert!(matches!(
        Store::open(database),
        Err(StoreError::InvalidPath)
    ));
}

#[cfg(unix)]
#[test]
fn store_rejects_database_without_exact_owner_only_mode() {
    use std::os::unix::fs::PermissionsExt;

    let dir = common::private_tempdir();
    let database = dir.path().join("agent.sqlite3");
    Store::open(&database).unwrap();
    std::fs::set_permissions(&database, std::fs::Permissions::from_mode(0o640)).unwrap();

    assert_eq!(Store::open(database).unwrap_err(), StoreError::InvalidPath);
}

#[cfg(unix)]
#[test]
fn store_rejects_database_in_a_non_private_directory() {
    use std::os::unix::fs::PermissionsExt;

    let root = common::private_tempdir();
    let directory = root.path().join("agent-data");
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o755)).unwrap();

    assert_eq!(
        Store::open(directory.join("agent.sqlite3")).unwrap_err(),
        StoreError::InvalidPath
    );
}

#[cfg(unix)]
#[test]
fn opened_store_rejects_trusted_directory_replacement_even_if_database_inode_returns() {
    use std::os::unix::fs::PermissionsExt;

    let root = common::private_tempdir();
    let directory = root.path().join("agent-data");
    let displaced = root.path().join("agent-data-old");
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let database = directory.join("agent.sqlite3");
    let store = Store::open(&database).unwrap();

    std::fs::rename(&directory, &displaced).unwrap();
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::rename(displaced.join("agent.sqlite3"), &database).unwrap();

    assert_eq!(store.list_dialogs().unwrap_err(), StoreError::InvalidPath);
}
