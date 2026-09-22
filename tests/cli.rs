use std::collections::VecDeque;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Mutex};

use deepseek_cli::dialog::DialogStore;
use deepseek_cli::memory::{MemoryRepository, RequestScope};
use deepseek_cli::profile::ProfileRepository;
use serde_json::{Value, json};
use tempfile::NamedTempFile;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

fn write_config(base_url: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("create temporary config");
    write!(
        file,
        r#"
api_key = "test-key"
base_url = "{base_url}"
model = "test-model"
timeout_seconds = 5

[workflow]
enabled = false

[context]
strategy = "summary"
"#
    )
    .expect("write temporary config");
    file
}

fn write_compression_config(base_url: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("create temporary config");
    write!(
        file,
        r#"
api_key = "test-key"
base_url = "{base_url}"
model = "test-model"
timeout_seconds = 5

[workflow]
enabled = false

[context]
strategy = "summary"
compact_after_prompt_tokens = 3
keep_last_messages = 2
summary_max_tokens = 64
"#
    )
    .unwrap();
    file
}

fn write_branching_config(base_url: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("create temporary config");
    write!(
        file,
        r#"
api_key = "test-key"
base_url = "{base_url}"
model = "test-model"
timeout_seconds = 5

[workflow]
enabled = false

[context]
strategy = "branching"
"#
    )
    .expect("write temporary config");
    file
}

#[derive(Clone)]
struct SequenceResponder {
    responses: Arc<Mutex<VecDeque<ResponseTemplate>>>,
}

impl Respond for SequenceResponder {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra request")
    }
}

fn sse(answer: &str, prompt: u64, completion: u64, total: u64) -> ResponseTemplate {
    let chunk = json!({
        "choices": [{"delta": {"content": answer}, "finish_reason": "stop"}],
        "usage": {
            "prompt_tokens": prompt,
            "completion_tokens": completion,
            "total_tokens": total
        }
    });
    ResponseTemplate::new(200)
        .insert_header("content-type", "text/event-stream")
        .set_body_string(format!("data: {chunk}\n\ndata: [DONE]\n\n"))
}

fn run_cli(config_path: &Path, input: &str) -> Output {
    let dir = tempfile::tempdir().unwrap();
    run_cli_args(config_path, &dir.path().join("dialogs.sqlite3"), &[], input)
}

fn run_cli_args(config_path: &Path, database: &Path, args: &[&str], input: &str) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_deepseek-cli"))
        .arg("--config")
        .arg(config_path)
        .arg("--db")
        .arg(database)
        .args(args)
        .env_remove("DEEPSEEK_API_KEY")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start deepseek-cli");
    child
        .stdin
        .take()
        .expect("open child stdin")
        .write_all(input.as_bytes())
        .expect("write scripted input");
    child.wait_with_output().expect("wait for deepseek-cli")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shows_one_final_usage_line_for_multiple_messages_and_restored_dialog() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).insert_header("content-type", "text/event-stream")
            .set_body_string("data: {\"choices\":[{\"delta\":{\"content\":\"Answer\"}}],\"usage\":{\"prompt_tokens\":2400,\"completion_tokens\":350,\"total_tokens\":2750,\"completion_tokens_details\":{\"reasoning_tokens\":50}}}\n\ndata: [DONE]\n\n"))
        .mount(&server).await;
    let config = write_config(&server.uri());
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("dialogs.sqlite3");
    let output = run_cli_args(
        config.path(),
        &database,
        &[],
        "Question\nFollow up\n/exit\n",
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout.matches("Токены").count(), 1);
    assert!(!stdout.contains("\x1b["));
    let footer = "Токены · Вход: 2400 · Выход: 350 · Всего: 2750 · Рассуждения: 50";
    assert!(stdout.ends_with(&format!("{footer}\n")), "{stdout}");
    let output = run_cli_args(config.path(), &database, &["--resume-last"], "/exit\n");
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout.matches("Токены").count(), 1);
    assert!(stdout.ends_with(&format!("{footer}\n")));
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_report_persisted_compression_without_making_an_api_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(SequenceResponder {
            responses: Arc::new(Mutex::new(
                [
                    sse("a1", 2, 1, 3),
                    sse("a2", 4, 2, 6),
                    sse("summary", 7, 2, 9),
                ]
                .into_iter()
                .collect(),
            )),
        })
        .mount(&server)
        .await;
    let config = write_compression_config(&server.uri());
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialogs.sqlite3");

    let output = run_cli_args(config.path(), &database, &[], "u1\nu2\n/stats\n/exit\n");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Стратегия · summary"));
    assert!(stdout.contains("Контекст · полная история: 4 · покрыто summary: 2 · дословно: 2"));
    assert!(stdout.contains("Ответы · вход: 6 · выход: 3 · всего: 9"));
    assert!(stdout.contains("Summary · вход: 7 · выход: 2 · всего: 9"));
    assert!(stdout.contains("Facts · вход: 0 · выход: 0 · всего: 0"));
    assert!(stdout.contains("API всего · 18"));
    assert_eq!(server.received_requests().await.unwrap().len(), 3);

    let resumed = run_cli_args(
        config.path(),
        &database,
        &["--resume-last"],
        "/stats\n/exit\n",
    );
    assert!(resumed.status.success());
    let resumed_stdout = String::from_utf8(resumed.stdout).unwrap();
    assert!(
        resumed_stdout.contains("Контекст · полная история: 4 · покрыто summary: 2 · дословно: 2")
    );
    assert!(resumed_stdout.contains("API всего · 18"));
    assert_eq!(server.received_requests().await.unwrap().len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resumes_across_processes_and_lists_without_api_config() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).insert_header("content-type", "text/event-stream")
        .set_body_string("data: {\"choices\":[{\"delta\":{\"content\":\"Saved answer\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
        .mount(&server).await;
    let config = write_config(&server.uri());
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("dialogs.sqlite3");
    assert!(
        run_cli_args(config.path(), &database, &[], "Original question\n/exit\n")
            .status
            .success()
    );
    let output = run_cli_args(
        config.path(),
        &database,
        &["--resume-last"],
        "Follow up\n/exit\n",
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("you> Original question\nassistant> Saved answer"));
    let requests = server.received_requests().await.unwrap();
    let body: Value = requests[1].body_json().unwrap();
    assert_eq!(
        body["messages"],
        json!([
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "Original question"},
            {"role": "assistant", "content": "Saved answer"},
            {"role": "user", "content": "Follow up"}
        ])
    );
    let output = run_cli_args(
        &dir.path().join("missing.toml"),
        &database,
        &["--list-dialogs"],
        "",
    );
    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("Original question")
    );
    assert_eq!(
        DialogStore::open(&database).unwrap().list().unwrap()[0].message_count,
        4
    );

    let id = DialogStore::open(&database)
        .unwrap()
        .latest_id()
        .unwrap()
        .unwrap()
        .to_string();
    let output = run_cli_args(
        config.path(),
        &database,
        &["--resume", &id],
        "/clear\nNew question\n/exit\n",
    );
    assert!(output.status.success());
    let dialogs = DialogStore::open(&database).unwrap().list().unwrap();
    assert_eq!(dialogs.len(), 2);
    assert_eq!(dialogs[0].title, "New question");
    assert_eq!(dialogs[1].message_count, 4);
    let requests = server.received_requests().await.unwrap();
    let body: Value = requests[2].body_json().unwrap();
    assert_eq!(body["messages"].as_array().unwrap().len(), 2);
}

#[test]
fn explicit_resume_of_missing_dialog_fails_without_creating_one() {
    let config = write_config("http://127.0.0.1:1");
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("dialogs.sqlite3");
    for args in [&["--resume-last"][..], &["--resume", "9999"][..]] {
        let output = run_cli_args(config.path(), &database, args, "/exit\n");
        assert!(!output.status.success());
        assert!(String::from_utf8(output.stderr).unwrap().contains("dialog"));
    }
    assert!(
        DialogStore::open(&database)
            .unwrap()
            .list()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_process_preserves_input_while_waiting_for_api() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_secs(30)))
        .mount(&server)
        .await;
    let config = write_config(&server.uri());
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("dialogs.sqlite3");
    let mut child = Command::new(env!("CARGO_BIN_EXE_deepseek-cli"))
        .arg("--config")
        .arg(config.path())
        .arg("--db")
        .arg(&database)
        .env_remove("DEEPSEEK_API_KEY")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"Do not lose me\n")
        .unwrap();
    let received = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while server.received_requests().await.unwrap().is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    child.kill().unwrap();
    child.wait().unwrap();
    received.expect("CLI should send the API request");
    let store = DialogStore::open(&database).unwrap();
    let dialog = store.load(store.latest_id().unwrap().unwrap()).unwrap();
    assert_eq!(dialog.messages.len(), 1);
    assert_eq!(dialog.messages[0].content(), "Do not lose me");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clear_and_api_error_leave_cli_ready_for_more_input() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("temporary failure"))
        .expect(1)
        .mount(&server)
        .await;
    let config = write_config(&server.uri());

    let output = run_cli(config.path(), "/clear\nHello\n/exit\n");

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout is UTF-8"),
        "Scope · user: default · task: default\nyou> Conversation cleared.\nyou> assistant> \nyou> Токены · нет данных API\n"
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("HTTP 500 Internal Server Error"));
    assert!(stderr.contains("temporary failure"));
    assert!(!stderr.contains("test-key"));
}

#[test]
fn end_of_file_exits_successfully_after_prompt() {
    let config = write_config("http://127.0.0.1:1");

    let output = run_cli(config.path(), "");

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout is UTF-8"),
        "Scope · user: default · task: default\nyou> \n"
    );
    assert!(output.stderr.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn branches_and_switches_without_extra_api_calls() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(sse("Answer", 2, 1, 3))
        .expect(3)
        .mount(&server)
        .await;
    let config = write_branching_config(&server.uri());
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("dialogs.sqlite3");

    let output = run_cli_args(
        config.path(),
        &database,
        &[],
        "Shared question\n/branch\nLeft continuation\n/switch 2\nRight continuation\n/exit\n",
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Checkpoint 2: dialog #1 remains active; created branch #2."));
    assert!(stdout.contains("Switched to branch #2."));
    assert!(stdout.contains("you> Shared question\nassistant> Answer"));
    assert_eq!(server.received_requests().await.unwrap().len(), 3);
    let store = DialogStore::open(&database).unwrap();
    assert_eq!(store.load(1).unwrap().messages.len(), 4);
    assert_eq!(store.load(2).unwrap().messages.len(), 4);
    assert_eq!(
        store.load(1).unwrap().messages[2].content(),
        "Left continuation"
    );
    assert_eq!(
        store.load(2).unwrap().messages[2].content(),
        "Right continuation"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_or_wrong_strategy_branch_commands_are_local_errors() {
    let server = MockServer::start().await;
    let config = write_config(&server.uri());
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("dialogs.sqlite3");

    let output = run_cli_args(
        config.path(),
        &database,
        &[],
        "/switch nope\n/branch\n/exit\n",
    );

    assert!(output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("usage: /switch <positive-dialog-id>"));
    assert!(stderr.contains("strategy = branching"));
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_commands_are_local_and_persist_across_dialogs() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(sse("Answer", 2, 1, 3))
        .expect(2)
        .mount(&server)
        .await;
    let mut config = write_config(&server.uri());
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialogs.sqlite3");
    let log_path = directory.path().join("debug.jsonl");
    writeln!(
        config,
        "\n[debug]\nlog_path = {:?}\nlog_payloads = false",
        log_path.to_str().unwrap()
    )
    .unwrap();
    let scope_args = ["--user", "alice", "--task", "bot"];
    let first = run_cli_args(
        config.path(),
        &database,
        &scope_args,
        "/remember user language Russian\n/remember task stack Rust\n/remember task database SQLite\n/memory\n/forget task missing\nQuestion\n/memory\n/exit\n",
    );
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let stdout = String::from_utf8(first.stdout).unwrap();
    assert_eq!(stdout.matches("Scope · user: alice · task: bot").count(), 1);
    assert!(stdout.contains("Saved Long-term memory · language = Russian · user: alice"));
    assert!(stdout.contains("Saved Working memory · stack = Rust · user: alice · task: bot"));
    assert!(stdout.contains("Conversation · dialog: new · strategy: summary · messages: 0 · summary boundary: 0 · sticky facts: 0"));
    assert!(stdout.contains("Conversation · dialog: #1 · strategy: summary · messages: 2 · summary boundary: 0 · sticky facts: 0"));
    assert!(stdout.contains(
        "Working · database = SQLite\nWorking · stack = Rust\nLong-term · language = Russian"
    ));
    assert!(stdout.contains("No Working memory entry named missing"));
    let second = run_cli_args(
        config.path(),
        &database,
        &scope_args,
        "/memory\nFollow up\n/exit\n",
    );
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    let body: Value = requests[1].body_json().unwrap();
    let messages = serde_json::to_string(&body["messages"]).unwrap();
    assert!(messages.contains("Russian"));
    assert!(messages.contains("Rust"));
    assert!(!messages.contains("Question"));
    let log = std::fs::read_to_string(log_path).unwrap();
    assert!(log.contains("user_memory"));
    assert!(log.contains("task_memory"));
    for secret in ["Russian", "Rust", "SQLite", "/remember"] {
        assert!(!log.contains(secret), "safe debug log leaked {secret}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_memory_edits_filters_and_invalid_commands_never_call_api() {
    let server = MockServer::start().await;
    let config = write_config(&server.uri());
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialogs.sqlite3");
    let output = run_cli_args(
        config.path(),
        &database,
        &[],
        "/remember user language English\n/remember user language Russian\n/remember task stack Rust\n/forget task stack\n/forget user language extra\n/remember task bad\n/memory conversation\n/memory user\n/memory task\n/exit\n",
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Forgot Working memory · stack"));
    assert!(stdout.contains("Long-term · language = Russian"));
    assert!(stdout.contains("Working · empty"));
    assert!(!stdout.contains("Conversation ·"));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("usage: /forget <user|task> <key>"));
    assert!(stderr.contains("usage: /remember <user|task> <key> <value>"));
    assert!(stderr.contains("usage: /memory [user|task]"));
    let store = DialogStore::open(&database).unwrap();
    assert!(store.list().unwrap().is_empty());
    let snapshot = store.load_memory(&RequestScope::default()).unwrap();
    assert_eq!(snapshot.user_entries()["language"], "Russian");
    assert!(snapshot.task_entries().is_empty());
    let output = run_cli_args(
        config.path(),
        &database,
        &[],
        "/forget user language\n/memory user\n/exit\n",
    );
    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("Long-term · empty")
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn profile_import_show_and_failures_are_local_and_atomic() {
    let server = MockServer::start().await;
    let config = write_config(&server.uri());
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialogs.sqlite3");
    let valid = directory.path().join("Alice Profile.md");
    let blank = directory.path().join("blank.md");
    let binary = directory.path().join("binary.md");
    let missing = directory.path().join("missing.md");
    std::fs::write(
        &valid,
        "# Alice preferences\n\n- Communicate briefly.\n- Prefer Android.\n",
    )
    .unwrap();
    std::fs::write(&blank, " \n ").unwrap();
    std::fs::write(&binary, [0xff, 0xfe]).unwrap();
    let input = format!(
        "/profile set Original preference\n/profile import {}\n/profile\n/profile import {}\n/profile import {}\n/profile import {}\n/profile\n/exit\n",
        valid.display(),
        missing.display(),
        blank.display(),
        binary.display(),
    );

    let output = run_cli_args(
        config.path(),
        &database,
        &["--user", "alice", "--task", "first"],
        &input,
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Saved profile · user: alice"));
    assert!(stdout.contains("Imported profile · user: alice"));
    assert_eq!(stdout.matches("# Alice preferences").count(), 2);
    assert!(stdout.contains("- Communicate briefly.\n- Prefer Android."));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(stderr.matches("failed to import profile").count(), 3);
    let store = DialogStore::open(&database).unwrap();
    assert!(store.list().unwrap().is_empty());
    assert_eq!(
        store
            .load_profile("alice")
            .unwrap()
            .unwrap()
            .content_markdown(),
        "# Alice preferences\n\n- Communicate briefly.\n- Prefer Android.",
    );
    assert!(server.received_requests().await.unwrap().is_empty());

    let cleared = run_cli_args(
        config.path(),
        &database,
        &["--user", "alice", "--task", "other"],
        "/profile clear\n/profile clear\n/profile\n/exit\n",
    );
    assert!(cleared.status.success());
    let stdout = String::from_utf8(cleared.stdout).unwrap();
    assert!(stdout.contains("Cleared profile · user: alice"));
    assert!(stdout.contains("No profile for user: alice"));
    assert!(stdout.contains("Profile · user: alice · empty"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn profiles_persist_across_tasks_and_personalize_the_same_prompt() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(sse("Answer", 2, 1, 3))
        .expect(2)
        .mount(&server)
        .await;
    let mut config = write_config(&server.uri());
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialogs.sqlite3");
    let log_path = directory.path().join("profiles.jsonl");
    writeln!(
        config,
        "\n[debug]\nlog_path = {:?}\nlog_payloads = false",
        log_path.to_str().unwrap()
    )
    .unwrap();

    let alice_setup = run_cli_args(
        config.path(),
        &database,
        &["--user", "alice", "--task", "setup"],
        "/profile set ALICE_ANDROID_PROFILE\n/exit\n",
    );
    assert!(alice_setup.status.success());
    let bob_setup = run_cli_args(
        config.path(),
        &database,
        &["--user", "bob", "--task", "setup"],
        "/profile set BOB_FLUTTER_PROFILE\n/exit\n",
    );
    assert!(bob_setup.status.success());
    let alice = run_cli_args(
        config.path(),
        &database,
        &["--user", "alice", "--task", "new-task"],
        "/profile\nDesign a mobile app\n/exit\n",
    );
    assert!(alice.status.success());
    assert!(
        String::from_utf8(alice.stdout)
            .unwrap()
            .contains("ALICE_ANDROID_PROFILE")
    );
    let bob = run_cli_args(
        config.path(),
        &database,
        &["--user", "bob", "--task", "new-task"],
        "Design a mobile app\n/exit\n",
    );
    assert!(bob.status.success());

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    let alice_request = serde_json::to_string(&requests[0].body_json::<Value>().unwrap()).unwrap();
    let bob_request = serde_json::to_string(&requests[1].body_json::<Value>().unwrap()).unwrap();
    assert!(alice_request.contains("ALICE_ANDROID_PROFILE"));
    assert!(!alice_request.contains("BOB_FLUTTER_PROFILE"));
    assert!(bob_request.contains("BOB_FLUTTER_PROFILE"));
    assert!(!bob_request.contains("ALICE_ANDROID_PROFILE"));
    let log = std::fs::read_to_string(log_path).unwrap();
    assert!(log.contains("user_profile"));
    assert!(!log.contains("ALICE_ANDROID_PROFILE"));
    assert!(!log.contains("BOB_FLUTTER_PROFILE"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resume_uses_persisted_scope_and_rejects_explicit_conflicts_before_api() {
    let server = MockServer::start().await;
    let config = write_config(&server.uri());
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialogs.sqlite3");
    let mut store = DialogStore::open(&database).unwrap();
    let id = store
        .start_dialog_in_scope(
            &RequestScope::new("alice", "bot").unwrap(),
            "System",
            "Question",
        )
        .unwrap()
        .to_string();
    for resume in [vec!["--resume", id.as_str()], vec!["--resume-last"]] {
        for (flag, value, expected) in [
            (
                "--user",
                "bob",
                "dialog belongs to user 'alice', not requested user 'bob'",
            ),
            (
                "--task",
                "other",
                "dialog belongs to task 'bot', not requested task 'other'",
            ),
        ] {
            let mut args = resume.clone();
            args.extend([flag, value]);
            let output = run_cli_args(config.path(), &database, &args, "Must not send\n/exit\n");
            assert!(!output.status.success());
            assert!(String::from_utf8(output.stderr).unwrap().contains(expected));
            assert!(output.stdout.is_empty());
        }
        for explicit in [
            vec![],
            vec!["--user", "alice"],
            vec!["--task", "bot"],
            vec!["--user", "alice", "--task", "bot"],
        ] {
            let mut args = resume.clone();
            args.extend(explicit);
            let output = run_cli_args(config.path(), &database, &args, "/memory\n/exit\n");
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let stdout = String::from_utf8(output.stdout).unwrap();
            assert_eq!(stdout.matches("Scope · user: alice · task: bot").count(), 1);
            assert!(stdout.contains("Conversation · dialog: #1"));
        }
    }
    assert_eq!(store.load(1).unwrap().messages.len(), 1);
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[test]
fn dialog_listing_includes_each_persisted_scope_without_config() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialogs.sqlite3");
    let mut store = DialogStore::open(&database).unwrap();
    store
        .start_dialog_in_scope(
            &RequestScope::new("alice", "bot").unwrap(),
            "",
            "Same title",
        )
        .unwrap();
    store
        .start_dialog_in_scope(&RequestScope::new("bob", "bot").unwrap(), "", "Same title")
        .unwrap();
    let output = run_cli_args(
        &directory.path().join("missing.toml"),
        &database,
        &["--list-dialogs"],
        "",
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.starts_with("ID | User | Task | Updated (UTC) | Messages | First message\n"));
    assert!(stdout.contains("1 | alice | bot |"));
    assert!(stdout.contains("2 | bob | bot |"));
}
