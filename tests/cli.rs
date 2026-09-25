use std::collections::VecDeque;
#[cfg(unix)]
use std::io::Read;
use std::io::Write;
#[cfg(unix)]
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Output, Stdio};
#[cfg(unix)]
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
#[cfg(unix)]
use std::thread;
#[cfg(unix)]
use std::time::Duration;

use deepseek_cli::dialog::DialogStore;
use deepseek_cli::memory::{MemoryRepository, RequestScope};
use deepseek_cli::profile::ProfileRepository;
#[cfg(unix)]
use deepseek_cli::workflow::{TaskPhase, TaskStatus};
#[cfg(unix)]
use deepseek_cli::workflow_store::PauseOutcome;
use deepseek_cli::workflow_store::{AnswerCommit, WorkflowRepository};
#[cfg(unix)]
use rusqlite::Connection;
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

#[cfg(unix)]
fn write_workflow_config(base_url: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("create temporary workflow config");
    write!(
        file,
        r#"
api_key = "test-key"
base_url = "{base_url}"
model = "test-model"
timeout_seconds = 30

[workflow]
enabled = true

[context]
strategy = "summary"
"#
    )
    .unwrap();
    file
}

#[cfg(unix)]
fn mark_approved_planning_fixture(database: &Path) {
    Connection::open(database)
        .unwrap()
        .execute_batch("UPDATE workflow_tasks SET phase='planning', goal_revision=1 WHERE phase='goal_definition'; UPDATE task_stage_runs SET phase='planning' WHERE phase='goal_definition';")
        .unwrap();
}

#[cfg(unix)]
fn write_observed_workflow_config(base_url: &str, log_path: &Path, context: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("create temporary workflow config");
    write!(
        file,
        r#"
api_key = "test-key"
base_url = "{base_url}"
model = "ordinary-model"
timeout_seconds = 30

[workflow]
enabled = true
interpreter_model = "interpreter-model"
checker_model = "checker-model"
handoff_model = "handoff-model"

[context]
{context}

[debug]
log_path = {log_path:?}
log_payloads = false
"#,
    )
    .unwrap();
    file
}

#[cfg(unix)]
fn send_sigint(child: &std::process::Child) {
    let status = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
}

#[cfg(unix)]
fn observe_stdout(
    mut stdout: std::process::ChildStdout,
    needle: &'static str,
) -> (mpsc::Receiver<()>, thread::JoinHandle<Vec<u8>>) {
    let (seen_tx, seen_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let mut output = Vec::new();
        let mut buffer = [0_u8; 256];
        let mut reported = false;
        loop {
            let read = stdout.read(&mut buffer).unwrap();
            if read == 0 {
                break;
            }
            output.extend_from_slice(&buffer[..read]);
            if !reported && String::from_utf8_lossy(&output).contains(needle) {
                let _ = seen_tx.send(());
                reported = true;
            }
        }
        output
    });
    (seen_rx, handle)
}

#[cfg(unix)]
fn wait_for_exit_while_stdin_is_open(
    mut child: std::process::Child,
    _stdin: std::process::ChildStdin,
) -> std::process::ExitStatus {
    let pid = child.id();
    let (status_tx, status_rx) = mpsc::channel();
    let waiter = thread::spawn(move || {
        let _ = status_tx.send(child.wait());
    });
    let status = match status_rx.recv_timeout(Duration::from_secs(5)) {
        Ok(status) => status.expect("wait for deepseek-cli"),
        Err(error) => {
            let killed = Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status()
                .expect("kill stuck deepseek-cli");
            assert!(killed.success());
            let _ = status_rx.recv_timeout(Duration::from_secs(5));
            waiter.join().unwrap();
            panic!("deepseek-cli did not exit while stdin remained open: {error}");
        }
    };
    waiter.join().unwrap();
    status
}

#[cfg(unix)]
fn spawn_partial_sse_server() -> (
    String,
    mpsc::Sender<()>,
    mpsc::Receiver<()>,
    thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (release_tx, release_rx) = mpsc::channel();
    let (sent_tx, sent_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let read = stream.read(&mut buffer).unwrap();
            request.extend_from_slice(&buffer[..read]);
            let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n")
            else {
                continue;
            };
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            if request.len() >= header_end + 4 + content_length {
                break;
            }
        }
        let chunk = json!({
            "choices": [{"delta": {"content": "partial fragment"}, "finish_reason": null}]
        });
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\ndata: {chunk}\n\n"
        )
        .unwrap();
        stream.flush().unwrap();
        let _ = sent_tx.send(());
        let _ = release_rx.recv_timeout(Duration::from_secs(10));
    });
    (format!("http://{address}"), release_tx, sent_rx, handle)
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

#[tokio::test]
async fn mcp_unavailable_server_aborts_startup_with_safe_name() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_string("SECRET_REMOTE_ERROR"))
        .mount(&server)
        .await;
    let mut config = write_config(&server.uri());
    writeln!(
        config,
        "\n[mcp]\nconnect_timeout_seconds=1\n[[mcp.servers]]\nname='telegram'\nurl='{}/SECRET_URL'",
        server.uri()
    )
    .unwrap();
    let output = run_cli(config.path(), "/exit\n");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("MCP server telegram"), "{stderr}");
    assert!(!stderr.contains("SECRET"), "{stderr}");
}

#[tokio::test]
async fn mcp_invalid_remote_tool_name_is_not_printed() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/mcp")).respond_with(|request: &Request| {
        let body: Value = request.body_json().unwrap();
        let result = match body["method"].as_str().unwrap() {
            "initialize" => json!({"protocolVersion":"2025-03-26","capabilities":{"tools":{}},"serverInfo":{"name":"test","version":"1"}}),
            "notifications/initialized" => return ResponseTemplate::new(202),
            "tools/list" => json!({"tools":[{"name":"https://SECRET.example/credential","inputSchema":{"type":"object"}}]}),
            other => panic!("unexpected MCP method: {other}"),
        };
        ResponseTemplate::new(200).set_body_json(json!({"jsonrpc":"2.0","id":body["id"],"result":result}))
    }).mount(&server).await;
    let mut config = write_config(&server.uri());
    writeln!(
        config,
        "\n[mcp]\nconnect_timeout_seconds=2\n[[mcp.servers]]\nname='telegram'\nurl='{}/mcp'",
        server.uri()
    )
    .unwrap();
    let output = run_cli(config.path(), "/exit\n");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("SECRET"), "{stderr}");
    assert!(stderr.contains("MCP"), "{stderr}");
}

#[tokio::test]
async fn mcp_duplicate_remote_tool_name_is_not_printed() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/mcp")).respond_with(|request: &Request| {
        let body: Value = request.body_json().unwrap();
        let result = match body["method"].as_str().unwrap() {
            "initialize" => json!({"protocolVersion":"2025-03-26","capabilities":{"tools":{}},"serverInfo":{"name":"test","version":"1"}}),
            "notifications/initialized" => return ResponseTemplate::new(202),
            "tools/list" => json!({"tools":[
                {"name":"SECRET_REMOTE_TOKEN","inputSchema":{"type":"object"}},
                {"name":"SECRET_REMOTE_TOKEN","inputSchema":{"type":"object"}}
            ]}),
            other => panic!("unexpected MCP method: {other}"),
        };
        ResponseTemplate::new(200).set_body_json(json!({"jsonrpc":"2.0","id":body["id"],"result":result}))
    }).mount(&server).await;
    let mut config = write_config(&server.uri());
    writeln!(
        config,
        "\n[mcp]\nconnect_timeout_seconds=2\n[[mcp.servers]]\nname='telegram'\nurl='{}/mcp'",
        server.uri()
    )
    .unwrap();
    let output = run_cli(config.path(), "/exit\n");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("SECRET"), "{stderr}");
    assert!(!String::from_utf8_lossy(&output.stdout).contains("SECRET"));
    assert!(stderr.contains("MCP"), "{stderr}");
}

#[tokio::test]
async fn mcp_tool_audit_persistence_failure_is_fatal_and_sanitized() {
    for finalization in [true, false] {
        let server = MockServer::start().await;
        Mock::given(method("POST")).and(path("/mcp")).respond_with(|request: &Request| {
            let body: Value = request.body_json().unwrap();
            let result = match body["method"].as_str().unwrap() {
                "initialize" => json!({"protocolVersion":"2025-03-26","capabilities":{"tools":{}},"serverInfo":{"name":"test","version":"1"}}),
                "notifications/initialized" => return ResponseTemplate::new(202),
                "tools/list" => json!({"tools":[{"name":"send","inputSchema":{"type":"object"},"annotations":{"readOnlyHint":false}}]}),
                "tools/call" => json!({"content":[{"type":"text","text":"sent"}]}),
                other => panic!("unexpected MCP method: {other}"),
            };
            ResponseTemplate::new(200).set_body_json(json!({"jsonrpc":"2.0","id":body["id"],"result":result}))
        }).mount(&server).await;
        let tool_turn = json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"write_1","type":"function","function":{"name":"telegram__send","arguments":"{}"}}]},"finish_reason":"tool_calls"}]});
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(format!("data: {tool_turn}\n\ndata: [DONE]\n\n")),
            )
            .mount(&server)
            .await;
        let mut config = write_config(&server.uri());
        writeln!(
            config,
            "\n[mcp]\nconnect_timeout_seconds=2\n[[mcp.servers]]\nname='telegram'\nurl='{}/mcp'",
            server.uri()
        )
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("tools.sqlite3");
        drop(DialogStore::open(&db).unwrap());
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch(if finalization {
            "CREATE TRIGGER fail_audit BEFORE UPDATE ON tool_executions BEGIN SELECT RAISE(ABORT, 'SECRET_AUDIT_FAILURE'); END;"
        } else {
            "CREATE TRIGGER fail_audit BEFORE INSERT ON tool_executions BEGIN SELECT RAISE(ABORT, 'SECRET_AUDIT_FAILURE'); END;"
        }).unwrap();
        let output = run_cli_args(config.path(), &db, &[], "first\nsecond\n/exit\n");
        assert!(
            !output.status.success(),
            "audit failure must terminate the CLI"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("tool audit failed"), "{stderr}");
        assert!(!stderr.contains("SECRET"), "{stderr}");
        let requests = server.received_requests().await.unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.url.path() == "/chat/completions")
                .count(),
            1,
            "must not accept the second prompt"
        );
        let calls = requests
            .iter()
            .filter(|r| r.body_json::<Value>().unwrap()["method"] == "tools/call")
            .count();
        assert_eq!(
            calls,
            usize::from(finalization),
            "finalization fails only after one real dispatch; start failure prevents dispatch"
        );
        let user_inputs: i64 = conn
            .query_row("SELECT count(*) FROM messages WHERE role='user'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(user_inputs, 1);
    }
}

#[tokio::test]
async fn mcp_registry_is_connected_once_and_injected_into_new_and_resumed_agents() {
    for resumed in [false, true] {
        let server = MockServer::start().await;
        Mock::given(method("POST")).and(path("/mcp")).respond_with(|request: &Request| {
            let body: Value = request.body_json().unwrap();
            let result = match body["method"].as_str().unwrap() {
                "initialize" => json!({"protocolVersion":"2025-03-26","capabilities":{"tools":{}},"serverInfo":{"name":"test","version":"1"}}),
                "notifications/initialized" => return ResponseTemplate::new(202),
                "tools/list" => json!({"tools":[{"name":"read_chat","inputSchema":{"type":"object"},"annotations":{"readOnlyHint":true}}]}),
                other => panic!("unexpected MCP method: {other}"),
            };
            ResponseTemplate::new(200).set_body_json(json!({"jsonrpc":"2.0","id":body["id"],"result":result}))
        }).mount(&server).await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(sse("Done", 2, 1, 3))
            .mount(&server)
            .await;
        let mut config = write_config(&server.uri());
        writeln!(
            config,
            "\n[mcp]\nconnect_timeout_seconds=2\n[[mcp.servers]]\nname='telegram'\nurl='{}/mcp'",
            server.uri()
        )
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("tools.sqlite3");
        if resumed {
            DialogStore::open(&db)
                .unwrap()
                .start_dialog("system", "prior")
                .unwrap();
        }
        let output = run_cli_args(
            config.path(),
            &db,
            if resumed { &["--resume-last"] } else { &[] },
            "first\nsecond\n/exit\n",
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let requests = server.received_requests().await.unwrap();
        let bodies: Vec<Value> = requests.iter().map(|r| r.body_json().unwrap()).collect();
        assert_eq!(
            bodies
                .iter()
                .filter(|b| b["method"] == "initialize")
                .count(),
            1
        );
        assert_eq!(
            bodies
                .iter()
                .filter(|b| b["method"] == "tools/list")
                .count(),
            1
        );
        let chats: Vec<_> = bodies
            .iter()
            .filter(|b| b.get("messages").is_some())
            .collect();
        assert_eq!(chats.len(), 2);
        assert!(
            chats
                .iter()
                .all(|b| b["tools"][0]["function"]["name"] == "telegram__read_chat")
        );
    }
}

#[tokio::test]
async fn debug_command_prints_local_snapshot_without_creating_a_task_or_calling_api() {
    let server = MockServer::start().await;
    let mut config = NamedTempFile::new().unwrap();
    write!(
        config,
        "api_key='secret-key'\nbase_url='{}'\n[context]\nstrategy='summary'\n[[invariants]]\nid='STACK'\ntext='Use Rust only'",
        server.uri()
    )
    .unwrap();
    let output = run_cli(config.path(), "/debug\n/exit\n");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"workflow\": null"), "{stdout}");
    assert!(stdout.contains("\"id\": \"STACK\""), "{stdout}");
    assert!(!stdout.contains("secret-key"));
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn configured_invariant_cannot_be_changed_from_cli_and_session_stays_open() {
    let server = MockServer::start().await;
    let mut config = NamedTempFile::new().unwrap();
    write!(
        config,
        "api_key='key'\nbase_url='{}'\n[context]\nstrategy='summary'\n[[invariants]]\nid='STACK'\ntext='Use Rust only'",
        server.uri()
    )
    .unwrap();
    let output = run_cli(
        config.path(),
        "/invariant add STACK Use Go only\n/invariant remove STACK\n/invariant list\n/exit\n",
    );
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.contains("STACK · Use Rust only"), "{stdout}");
    assert!(stderr.contains("defined in config"), "{stderr}");
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[cfg(unix)]
fn workflow_patch(version: u64) -> Value {
    json!({"expected_version":version,"plan_append":{"steps":[],"acceptance_criteria":[]},
        "step_updates":[],"current_step_id":null,"expected_action":null,"checkpoint":null})
}

// Break caught: a nested durable-answer write error must stop the CLI, without
// processing the next command or exposing SQLite diagnostic payloads.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_workflow_answer_storage_failure_is_fatal_and_sanitized() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(sse("unsaved answer", 2, 1, 3))
        .mount(&server)
        .await;
    let config = write_workflow_config(&server.uri());
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("fatal-answer.sqlite3");
    drop(DialogStore::open(&database).unwrap());
    let connection = Connection::open(&database).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_answer BEFORE INSERT ON messages WHEN NEW.role='assistant'
        BEGIN SELECT RAISE(ABORT, 'SECRET_SQLITE_DIAGNOSTIC'); END;",
        )
        .unwrap();
    let output = run_cli_args(config.path(), &database, &[], "goal\n/task\nexit\n");
    assert!(
        !output.status.success(),
        "durable write failure must be fatal"
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stdout.contains("unsaved answer"));
    assert!(
        !stdout.contains("Workflow task"),
        "later /task was processed: {stdout}"
    );
    assert!(!stderr.contains("SECRET_SQLITE_DIAGNOSTIC"), "{stderr}");
    assert!(stderr.contains("persistence"), "{stderr}");
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    assert_eq!(
        DialogStore::open(&database)
            .unwrap()
            .load(1)
            .unwrap()
            .messages
            .len(),
        1
    );
}

// Break caught: disabling workflow on restore must support both legacy turns
// and forks after hidden controller protocol has already been persisted.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_disabled_workflow_cli_continues_and_branches_without_controller_leakage() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/chat/completions"))
        .respond_with(SequenceResponder { responses: Arc::new(Mutex::new([
            workflow_interpret(json!({"type":"continue","instruction":"continue"})),
            sse("first answer", 2, 1, 3),
            workflow_check(workflow_patch(1), json!({"type":"continue","instruction":"HIDDEN_DISABLED_CONTROLLER","confidence":0.95})),
            sse("second answer", 2, 1, 3),
            workflow_check(workflow_patch(2), json!({"type":"await_user"})),
            sse("legacy answer", 2, 1, 3),
        ].into_iter().collect())) }).mount(&server).await;
    let disabled = write_branching_config(&server.uri());
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("disabled.sqlite3");
    let mut seed = DialogStore::open(&database).unwrap();
    seed.start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "approved goal")
        .unwrap();
    drop(seed);
    mark_approved_planning_fixture(&database);
    let config = write_observed_workflow_config(
        &server.uri(),
        &directory.path().join("debug.jsonl"),
        "strategy='branching'",
    );
    assert!(
        run_cli_args(
            config.path(),
            &database,
            &["--resume-last"],
            "goal\n/branch\n/exit\n"
        )
        .status
        .success()
    );
    assert_eq!(
        DialogStore::open(&database).unwrap().list().unwrap().len(),
        2
    );
    let output = run_cli_args(
        disabled.path(),
        &database,
        &["--resume", "2"],
        "/branch\n/switch 3\nlegacy continuation\n/exit\n",
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("legacy answer"), "{stdout}");
    assert!(!stdout.contains("HIDDEN_DISABLED_CONTROLLER"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("HIDDEN_DISABLED_CONTROLLER"));
    let store = DialogStore::open(&database).unwrap();
    assert_eq!(store.list().unwrap().len(), 3);
    assert_eq!(store.raw_message_count(1).unwrap(), 5);
    assert_eq!(store.raw_message_count(2).unwrap(), 5);
    assert_eq!(store.raw_message_count(3).unwrap(), 7);
    assert_eq!(store.load(3).unwrap().messages.len(), 6);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 6);
    assert!(!String::from_utf8_lossy(&requests[5].body).contains("HIDDEN_DISABLED_CONTROLLER"));
}

// Break caught: /stats and conversation-memory output must report managed
// stage facts and costs separately from the visible transcript total.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_managed_stats_and_memory_report_current_stage_costs() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(SequenceResponder {
            responses: Arc::new(Mutex::new(
                [
                    sse(r#"{"language":"Rust"}"#, 2, 1, 3),
                    sse("saved answer", 7, 3, 10),
                ]
                .into_iter()
                .collect(),
            )),
        })
        .mount(&server)
        .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("stats.sqlite3");
    let config = write_observed_workflow_config(
        &server.uri(),
        &directory.path().join("debug.jsonl"),
        "strategy='sticky_facts'\nkeep_last_messages=8",
    );
    let output = run_cli_args(
        config.path(),
        &database,
        &[],
        "goal\n/stats\n/memory\n/exit\n",
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains("транскрипт: 2 · текущий этап: 2 · в запросе: 2 · facts: 1 · facts до: 1"),
        "{stdout}"
    );
    assert!(
        stdout.contains(
            "messages: 2 · current stage messages: 2 · summary boundary: 0 · sticky facts: 1"
        ),
        "{stdout}"
    );
    assert!(stdout.contains("API этапа всего · 13"), "{stdout}");
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

// Break caught: /task after restart must not report Processing forever when
// both durable leases crashed, nor retry a provider or mutate the task.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_exhausted_processing_cli_reports_failed_without_provider_calls() {
    use deepseek_cli::workflow_store::ProcessingLeaseMode;
    let server = MockServer::start().await;
    let config = write_workflow_config(&server.uri());
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("exhausted-cli.sqlite3");
    let mut store = DialogStore::open(&database).unwrap();
    let started = store
        .start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "goal")
        .unwrap();
    let answer = store
        .append_answer_for_processing(AnswerCommit {
            dialog_id: started.dialog_id,
            task_id: started.task.id,
            stage_run_id: started.stage_run_id,
            expected_version: 0,
            content: "saved answer",
            usage: None,
        })
        .unwrap();
    store
        .lease_processing(answer.processing_id, 0, ProcessingLeaseMode::Normal)
        .unwrap();
    store
        .lease_processing(answer.processing_id, 0, ProcessingLeaseMode::Recovery)
        .unwrap();
    for _ in 0..2 {
        let output = run_cli_args(
            config.path(),
            &database,
            &["--resume-last"],
            "/task\n/exit\n",
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("processing: failed"));
        assert_eq!(
            store
                .load_workflow(started.dialog_id)
                .unwrap()
                .current_task
                .unwrap(),
            started.task
        );
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}

// Break caught: startup recovery may warn on advisory model failure, but a
// failed durable completion write must stop before replay and later commands.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_workflow_recovery_storage_failure_is_fatal_and_sanitized() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(workflow_check(
            workflow_patch(0),
            json!({"type":"await_user"}),
        ))
        .mount(&server)
        .await;
    let config = write_workflow_config(&server.uri());
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("fatal-recovery.sqlite3");
    let mut store = DialogStore::open(&database).unwrap();
    let started = store
        .start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "goal")
        .unwrap();
    mark_approved_planning_fixture(&database);
    let expected = store
        .load_workflow(started.dialog_id)
        .unwrap()
        .current_task
        .unwrap();
    store
        .append_answer_for_processing(AnswerCommit {
            dialog_id: started.dialog_id,
            task_id: started.task.id,
            stage_run_id: started.stage_run_id,
            expected_version: 0,
            content: "saved answer",
            usage: None,
        })
        .unwrap();
    let connection = Connection::open(&database).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_completion BEFORE UPDATE ON response_processing WHEN NEW.status='completed'
        BEGIN SELECT RAISE(ABORT, 'SECRET_RECOVERY_DIAGNOSTIC'); END;").unwrap();
    let output = run_cli_args(
        config.path(),
        &database,
        &["--resume-last"],
        "/task\n/exit\n",
    );
    assert!(
        !output.status.success(),
        "durable recovery write failure must be fatal"
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("Scope ·"));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("persistence"), "{stderr}");
    assert!(!stderr.contains("SECRET_RECOVERY_DIAGNOSTIC"), "{stderr}");
    assert_eq!(
        store
            .load_workflow(started.dialog_id)
            .unwrap()
            .current_task
            .unwrap(),
        expected
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[cfg(unix)]
fn workflow_check(patch: Value, decision: Value) -> ResponseTemplate {
    sse(
        &json!({"patch":patch,"decision":decision}).to_string(),
        2,
        1,
        3,
    )
}

#[cfg(unix)]
fn workflow_interpret(intent: Value) -> ResponseTemplate {
    sse(
        &json!({"confidence":0.95,"intent":intent}).to_string(),
        2,
        1,
        3,
    )
}

#[cfg(unix)]
fn workflow_handoff(summary: &str, next: Option<&str>) -> Value {
    json!({"summary":summary,"completed_step_ids":[],"next_step_id":next,
        "expected_action":"Perform the current stage","plan_changes":[],"decisions":[],"open_issues":[]})
}

#[cfg(unix)]
fn workflow_service(value: Value) -> ResponseTemplate {
    sse(&value.to_string(), 2, 1, 3)
}

#[cfg(unix)]
fn transition_decision(event: &str, evidence: &[&str]) -> Value {
    json!({"type":"emit_transition","event":event,"evidence":evidence,"confidence":0.95})
}

#[cfg(unix)]
fn transition_intent(event: &str) -> Value {
    json!({"type":"propose_transition","event":event,"evidence":[]})
}

#[cfg(unix)]
fn completed_step_patch(version: u64, step: &str) -> Value {
    let mut patch = workflow_patch(version);
    patch["step_updates"] = json!([{"step_id":step,"status":"completed","evidence":["Observed passing targeted test"]}]);
    patch
}

// Break caught: skipping a guard, reusing a repaired stage, replacing the ledger,
// leaking prior-stage protocol, or replaying synthetic text corrupts the persisted lifecycle.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_acceptance_complete_lifecycle_is_durable_ordered_and_stage_isolated() {
    let server = MockServer::start().await;
    let mut plan = workflow_patch(1);
    plan["plan_append"] = json!({"steps":[{"id":"build","description":"Implement parser","status":"pending"}],"acceptance_criteria":["parser tests pass"]});
    plan["current_step_id"] = json!("build");
    let mut repair = workflow_handoff("Repair checkpoint", Some("repair"));
    repair["plan_changes"] =
        json!([{"id":"repair","description":"Fix empty input","status":"pending"}]);
    let responses = Arc::new(Mutex::new(VecDeque::from([
        workflow_interpret(json!({"type":"continue","instruction":"design the parser"})),
        sse("PLAN_RAW_12", 2, 1, 3),
        workflow_check(plan, json!({"type":"await_user"})),
        workflow_interpret(transition_intent("planning_completed")),
        workflow_service(workflow_handoff("Planning checkpoint", Some("build"))),
        sse("EXECUTION_ONE_RAW_12", 2, 1, 3),
        workflow_check(
            workflow_patch(3),
            json!({"type":"continue","instruction":"CONTROLLER_ONLY_12","confidence":0.95}),
        ),
        sse("EXECUTION_TWO_RAW_12", 2, 1, 3),
        workflow_check(
            completed_step_patch(4, "build"),
            transition_decision("execution_completed", &[]),
        ),
        workflow_service(workflow_handoff("Implementation checkpoint", None)),
        sse("VALIDATION_FAILED_RAW_12", 2, 1, 3),
        workflow_check(
            workflow_patch(5),
            transition_decision("validation_failed", &["Empty input test fails"]),
        ),
        workflow_service(repair),
        sse("REPAIR_RAW_12", 2, 1, 3),
        workflow_check(
            completed_step_patch(6, "repair"),
            json!({"type":"await_user"}),
        ),
        workflow_interpret(transition_intent("execution_completed")),
        workflow_service(workflow_handoff("Repair completed checkpoint", None)),
        sse("VALIDATION_PASSED_RAW_12", 2, 1, 3),
        workflow_check(
            workflow_patch(8),
            transition_decision(
                "validation_passed",
                &["parser tests pass => all 12 cases passed"],
            ),
        ),
        workflow_service(workflow_handoff("Final checkpoint", None)),
    ])));
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(SequenceResponder {
            responses: responses.clone(),
        })
        .mount(&server)
        .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("lifecycle.sqlite3");
    let mut seed = DialogStore::open(&database).unwrap();
    seed.start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "approved goal")
        .unwrap();
    drop(seed);
    mark_approved_planning_fixture(&database);
    let config = write_observed_workflow_config(
        &server.uri(),
        &directory.path().join("events.jsonl"),
        "strategy = 'summary'",
    );
    let first = run_cli_args(
        config.path(),
        &database,
        &["--resume-last"],
        "design the parser\n/exit\n",
    );
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let store = DialogStore::open(&database).unwrap();
    let id = store.list().unwrap()[0].id;
    let planned = store.load_workflow(id).unwrap().current_task.unwrap();
    assert_eq!(planned.phase, TaskPhase::Planning);
    assert_eq!(planned.version, 2);
    let connection = Connection::open(&database).unwrap();
    // The complete immutable row is retained across the last human transition.
    let ledger = || -> Vec<(i64, i64, i64, String, String)> {
        connection
            .prepare(
                "SELECT id,from_stage_run_id,to_stage_run_id,event,
            json_object('task',workflow_task_id,'input',workflow_input_id,'version',source_version,
                        'handoff',handoff_json,'source',source_fingerprint,'created_at',created_at)
            FROM task_transitions ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    assert!(ledger().is_empty());
    let middle = run_cli_args(
        config.path(),
        &database,
        &["--resume-last"],
        "plan approved; execute\n/exit\n",
    );
    assert!(
        middle.status.success(),
        "{}",
        String::from_utf8_lossy(&middle.stderr)
    );
    let repaired = store.load_workflow(id).unwrap().current_task.unwrap();
    assert_eq!(repaired.phase, TaskPhase::Execution);
    assert_eq!(repaired.current_stage_sequence, 4);
    assert_eq!(repaired.version, 7);
    let prefix = ledger();
    assert_eq!(prefix.len(), 3);
    let finish = run_cli_args(
        config.path(),
        &database,
        &["--resume-last"],
        "repair complete; validate\n/task\n/exit\n",
    );
    assert!(
        finish.status.success(),
        "{}",
        String::from_utf8_lossy(&finish.stderr)
    );
    let done = store.load_workflow(id).unwrap().current_task.unwrap();
    assert_eq!(done.phase, TaskPhase::Done);
    assert_eq!(done.version, 9);
    assert_eq!(done.id, planned.id);
    let final_ledger = ledger();
    assert_eq!(&final_ledger[..3], prefix.as_slice());
    assert_eq!(
        final_ledger
            .iter()
            .map(|row| row.3.as_str())
            .collect::<Vec<_>>(),
        [
            "planning_completed",
            "execution_completed",
            "validation_failed",
            "execution_completed",
            "validation_passed"
        ]
    );
    assert!(
        final_ledger
            .windows(2)
            .all(|rows| rows[0].0 < rows[1].0 && rows[0].2 == rows[1].1)
    );
    let stages: Vec<(i64, String, u32)> = connection
        .prepare("SELECT id,phase,sequence FROM task_stage_runs ORDER BY sequence")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        stages
            .iter()
            .map(|row| (row.1.as_str(), row.2))
            .collect::<Vec<_>>(),
        [
            ("planning", 1),
            ("execution", 2),
            ("validation", 3),
            ("execution", 4),
            ("validation", 5),
            ("done", 6)
        ]
    );
    assert_ne!(stages[1].0, stages[3].0);
    let requests = server.received_requests().await.unwrap();
    let ordinary: Vec<Value> = requests
        .iter()
        .map(|r| r.body_json::<Value>().unwrap())
        .filter(|body| body["model"] == "ordinary-model")
        .collect();
    assert_eq!(ordinary.len(), 6);
    let protocol: Vec<Vec<&str>> = ordinary
        .iter()
        .map(|body| {
            body["messages"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|message| message["role"] != "system")
                .map(|message| message["content"].as_str().unwrap())
                .collect()
        })
        .collect();
    assert_eq!(
        protocol,
        [
            vec!["approved goal", "design the parser"],
            vec!["plan approved; execute"],
            vec![
                "plan approved; execute",
                "EXECUTION_ONE_RAW_12",
                "CONTROLLER_ONLY_12"
            ],
            vec!["Apply the approved workflow stage transition."],
            vec!["Apply the approved workflow stage transition."],
            vec!["repair complete; validate"],
        ]
    );
    let raw_markers = [
        "PLAN_RAW_12",
        "EXECUTION_ONE_RAW_12",
        "EXECUTION_TWO_RAW_12",
        "VALIDATION_FAILED_RAW_12",
        "REPAIR_RAW_12",
    ];
    for (index, body) in ordinary.iter().enumerate() {
        let text = body["messages"].to_string();
        for marker in raw_markers {
            assert_eq!(
                text.contains(marker),
                index == 2 && marker == "EXECUTION_ONE_RAW_12",
                "request {index}: {text}"
            );
        }
    }
    let replay = run_cli_args(config.path(), &database, &["--resume-last"], "/exit\n");
    assert!(replay.status.success());
    for output in [&first, &middle, &finish, &replay] {
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!text.contains("CONTROLLER_ONLY_12"));
        assert!(!text.contains("Apply the approved workflow stage transition."));
    }
    let replay = String::from_utf8(replay.stdout).unwrap();
    for marker in raw_markers {
        assert!(replay.contains(marker), "{replay}");
    }
    assert!(replay.contains("VALIDATION_PASSED_RAW_12"), "{replay}");
    assert!(responses.lock().unwrap().is_empty());
    assert_eq!(server.received_requests().await.unwrap().len(), 20);
}

// Break caught: crossing task/stage boundaries must not reintroduce raw history,
// full audit handoffs, old reductions, or controller text as user-fact evidence.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_acceptance_two_tasks_keep_only_current_stage_and_projected_checkpoint() {
    let server = MockServer::start().await;
    let plan_patch = |step: &str| {
        let mut patch = workflow_patch(1);
        patch["plan_append"] = json!({"steps":[{"id":step,"description":"Build result","status":"pending"}],"acceptance_criteria":["tests pass"]});
        patch["current_step_id"] = json!(step);
        patch
    };
    let mut projected = workflow_handoff("TASK2_ACCEPTED_CHECKPOINT", Some("second"));
    projected["decisions"] = json!(["TASK2_ACCEPTED_DECISION"]);
    projected["open_issues"] = json!(["TASK2_ACCEPTED_ISSUE"]);
    let responses = Arc::new(Mutex::new(VecDeque::from([
        workflow_interpret(json!({"type":"continue","instruction":"design task one"})),
        workflow_service(json!({"old":"TASK1_FACTS_MARKER"})),
        sse("TASK1_PLANNING_RAW", 2, 1, 3),
        workflow_check(
            plan_patch("first"),
            transition_decision("planning_completed", &[]),
        ),
        workflow_service(workflow_handoff("TASK1_FULL_HANDOFF_MARKER", Some("first"))),
        sse("TASK1_EXECUTION_RAW", 2, 1, 3),
        workflow_check(
            completed_step_patch(2, "first"),
            transition_decision("execution_completed", &[]),
        ),
        workflow_service(workflow_handoff("Task one implementation checkpoint", None)),
        sse("TASK1_VALIDATION_RAW", 2, 1, 3),
        workflow_check(
            workflow_patch(3),
            transition_decision(
                "validation_passed",
                &["tests pass => observed all cases passing"],
            ),
        ),
        workflow_service(workflow_handoff("Task one final checkpoint", None)),
        workflow_interpret(json!({"type":"start_new_task","goal":"TASK2_CURRENT_GOAL"})),
        workflow_service(json!({"old":"TASK2_GOAL_FACTS_MARKER"})),
        sse("Предлагаемая цель: TASK2_CURRENT_GOAL", 2, 1, 3),
        workflow_interpret(json!({"type":"approve_goal"})),
        workflow_service(json!({"old":"TASK2_PLANNING_FACTS_MARKER"})),
        sse("TASK2_PLANNING_RAW", 2, 1, 3),
        workflow_check(plan_patch("second"), json!({"type":"await_user"})),
        workflow_interpret(transition_intent("planning_completed")),
        workflow_service(projected.clone()),
        workflow_service(json!({"current":"TASK2_EXECUTION_FACT"})),
        sse("TASK2_EXECUTION_ONE", 2, 1, 3),
        workflow_check(
            workflow_patch(3),
            json!({"type":"continue","instruction":"CONTROLLER_FACTS_EXCLUDED","confidence":0.95}),
        ),
        sse("TASK2_EXECUTION_TWO", 2, 1, 3),
        workflow_check(workflow_patch(4), json!({"type":"await_user"})),
        workflow_interpret(json!({"type":"continue","instruction":"TASK2_CURRENT_HUMAN"})),
        workflow_service(json!({"current":"TASK2_EXECUTION_FACT"})),
        sse("TASK2_EXECUTION_THREE", 2, 1, 3),
        workflow_check(workflow_patch(5), json!({"type":"await_user"})),
    ])));
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(SequenceResponder {
            responses: responses.clone(),
        })
        .mount(&server)
        .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("two-tasks.sqlite3");
    let mut seed = DialogStore::open(&database).unwrap();
    seed.start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "approved task one")
        .unwrap();
    drop(seed);
    mark_approved_planning_fixture(&database);
    let config = write_observed_workflow_config(
        &server.uri(),
        &directory.path().join("events.jsonl"),
        "strategy = 'sticky_facts'\nkeep_last_messages = 20",
    );
    let output = run_cli_args(
        config.path(),
        &database,
        &["--resume-last"],
        "/profile set PROFILE_MARKER\n/remember user preference USER_MEMORY_MARKER\n/remember task design MEMORY_TASK_MARKER\ndesign task one\nstart second task\napprove second goal\nTASK2_EXECUTE_HUMAN\nTASK2_CURRENT_HUMAN\n/exit\n",
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let store = DialogStore::open(&database).unwrap();
    let id = store.list().unwrap()[0].id;
    let task = store.load_workflow(id).unwrap().current_task.unwrap();
    assert_eq!(task.ordinal, 2);
    assert_eq!(task.phase, TaskPhase::Execution);
    assert_eq!(task.current_stage_sequence, 3);
    assert_eq!(task.checkpoint.summary, "TASK2_ACCEPTED_CHECKPOINT");
    let requests = server.received_requests().await.unwrap();
    let bodies: Vec<Value> = requests.iter().map(|r| r.body_json().unwrap()).collect();
    let target = bodies
        .iter()
        .rev()
        .find(|body| {
            body["model"] == "ordinary-model"
                && !body["messages"][0]["content"]
                    .as_str()
                    .unwrap()
                    .contains("Update the key-value memory")
        })
        .unwrap();
    let messages = target["messages"].as_array().unwrap();
    let text = target["messages"].to_string();
    assert_eq!(
        messages[0]["content"],
        store.load(id).unwrap().system_prompt
    );
    for (index, marker) in [
        (1, "PROFILE_MARKER"),
        (2, "USER_MEMORY_MARKER"),
        (3, "MEMORY_TASK_MARKER"),
        (4, "TASK2_CURRENT_GOAL"),
        (5, "TASK2_EXECUTION_FACT"),
    ] {
        assert_eq!(messages[index]["role"], "system");
        assert!(
            messages[index]["content"]
                .as_str()
                .unwrap()
                .contains(marker),
            "{text}"
        );
    }
    for marker in [
        "TASK1_PLANNING_RAW",
        "TASK1_EXECUTION_RAW",
        "TASK1_VALIDATION_RAW",
        "TASK1_FULL_HANDOFF_MARKER",
        "TASK1_FACTS_MARKER",
        "TASK2_PLANNING_RAW",
        "TASK2_PLANNING_FACTS_MARKER",
        "completed_step_ids",
        "plan_changes",
    ] {
        assert!(!text.contains(marker), "leaked {marker}: {text}");
    }
    for marker in [
        "TASK2_ACCEPTED_CHECKPOINT",
        "TASK2_ACCEPTED_DECISION",
        "TASK2_ACCEPTED_ISSUE",
    ] {
        assert!(messages[4]["content"].as_str().unwrap().contains(marker));
    }
    assert_eq!(
        messages[6..]
            .iter()
            .map(|m| (m["role"].as_str().unwrap(), m["content"].as_str().unwrap()))
            .collect::<Vec<_>>(),
        [
            ("user", "TASK2_EXECUTE_HUMAN"),
            ("assistant", "TASK2_EXECUTION_ONE"),
            ("user", "CONTROLLER_FACTS_EXCLUDED"),
            ("assistant", "TASK2_EXECUTION_TWO"),
            ("user", "TASK2_CURRENT_HUMAN"),
        ]
    );
    let facts: Vec<_> = bodies
        .iter()
        .filter(|body| {
            body["messages"][0]["content"]
                .as_str()
                .unwrap()
                .contains("Update the key-value memory")
        })
        .collect();
    assert_eq!(facts.len(), 5);
    for body in &facts {
        let text = body["messages"].to_string();
        for marker in [
            "CONTROLLER_FACTS_EXCLUDED",
            "PROFILE_MARKER",
            "USER_MEMORY_MARKER",
            "MEMORY_TASK_MARKER",
        ] {
            assert!(!text.contains(marker), "facts leaked {marker}: {text}");
        }
    }
    assert!(
        facts.last().unwrap()["messages"]
            .to_string()
            .contains("TASK2_CURRENT_HUMAN")
    );
    assert!(
        !facts.last().unwrap()["messages"]
            .to_string()
            .contains("TASK2_PLANNING")
    );
    let connection = Connection::open(&database).unwrap();
    let audit: Vec<String> = connection
        .prepare("SELECT handoff_json FROM task_transitions ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(
        audit
            .iter()
            .any(|raw| raw.contains("TASK1_FULL_HANDOFF_MARKER"))
    );
    assert!(
        audit
            .iter()
            .any(|raw| serde_json::from_str::<Value>(raw).unwrap() == projected)
    );
    assert!(responses.lock().unwrap().is_empty());
}

// Break caught: workflow status is a read-only local command and must not
// manufacture a dialog or call the provider when no workflow task exists.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_status_without_task_is_local_and_exact() {
    let server = MockServer::start().await;
    let config = write_workflow_config(&server.uri());
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("status-empty.sqlite3");

    let output = run_cli_args(config.path(), &database, &[], "/task extra\n/task\n/exit\n");

    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("No workflow task in this dialog.")
    );
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("usage: /task")
    );
    assert!(server.received_requests().await.unwrap().is_empty());
    assert!(
        DialogStore::open(&database)
            .unwrap()
            .list()
            .unwrap()
            .is_empty()
    );
}

// Break caught: resume and /task render the same committed projection without
// activating the task, running recovery work, or conflating the workflow ID
// with the existing memory-task label.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_status_is_printed_after_resume_and_on_command_without_mutation() {
    let server = MockServer::start().await;
    let config = write_workflow_config(&server.uri());
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("status.sqlite3");
    let mut store = DialogStore::open(&database).unwrap();
    let started = store
        .start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "saved goal")
        .unwrap();
    drop(store);
    let connection = Connection::open(&database).unwrap();
    connection
        .execute(
            "UPDATE workflow_tasks SET phase='planning', goal_revision=1, plan_json=?1, current_step_id='write', expected_action='Run focused tests' WHERE id=?2",
            rusqlite::params![
                json!({
                    "revision": 7,
                    "steps": [{"id":"write","description":"Implement status","status":"in_progress"}],
                    "acceptance_criteria": []
                })
                .to_string(),
                started.task.id.0,
            ],
        )
        .unwrap();
    connection
        .execute_batch("UPDATE task_stage_runs SET phase='planning';")
        .unwrap();
    drop(connection);
    let before = DialogStore::open(&database)
        .unwrap()
        .load_workflow(started.dialog_id)
        .unwrap()
        .current_task
        .unwrap();
    let id = started.dialog_id.to_string();

    let resumed = run_cli_args(config.path(), &database, &["--resume", &id], "/exit\n");
    assert!(resumed.status.success());
    let stdout = String::from_utf8(resumed.stdout).unwrap();
    assert_eq!(
        stdout.matches("Workflow task #1 · ID: ").count(),
        1,
        "{stdout}"
    );
    for expected in [
        "Phase: planning · status: active",
        "Plan revision: 7 · current step: write",
        "Expected action: Run focused tests",
        "Stage sequence: 1 · processing: none",
    ] {
        assert!(
            stdout.contains(expected),
            "missing {expected:?} in {stdout}"
        );
    }
    assert!(stdout.contains(&format!("Workflow task #1 · ID: {}", before.id.0)));

    let commanded = run_cli_args(
        config.path(),
        &database,
        &["--resume", &id],
        "/task\n/exit\n",
    );
    assert!(commanded.status.success());
    let stdout = String::from_utf8(commanded.stdout).unwrap();
    assert_eq!(
        stdout.matches("Workflow task #1 · ID: ").count(),
        2,
        "{stdout}"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
    assert_eq!(
        DialogStore::open(&database)
            .unwrap()
            .load_workflow(started.dialog_id)
            .unwrap()
            .current_task
            .unwrap(),
        before
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn goal_definition_status_and_debug_show_the_active_proposal() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(sse("Предлагаемая цель: Сделать CLI", 2, 1, 3))
        .mount(&server)
        .await;
    let config = write_workflow_config(&server.uri());
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("goal-status.sqlite3");
    let output = run_cli_args(
        config.path(),
        &database,
        &[],
        "Давай обсудим CLI\n/task\n/debug\n/exit\n",
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains("Phase: goal_definition · status: active"),
        "{stdout}"
    );
    assert!(
        stdout.contains("Goal (working, revision 0): Давай обсудим CLI"),
        "{stdout}"
    );
    assert!(stdout.contains("Current proposal message: "), "{stdout}");
    assert!(stdout.contains("\"goal_proposal\""), "{stdout}");
    let store = DialogStore::open(&database).unwrap();
    let task = store
        .load_workflow(store.latest_id().unwrap().unwrap())
        .unwrap()
        .current_task
        .unwrap();
    assert_eq!(
        task.goal_proposal
            .as_ref()
            .map(|proposal| proposal.text.as_str()),
        Some("Сделать CLI")
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_can_approve_reopen_and_reapprove_a_revised_goal() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(SequenceResponder {
            responses: Arc::new(Mutex::new(
                [
                    sse("Предлагаемая цель: Сделать CLI", 2, 1, 3),
                    workflow_interpret(json!({"type":"approve_goal"})),
                    sse("Обсудим план.", 2, 1, 3),
                    workflow_check(workflow_patch(1), json!({"type":"await_user"})),
                    workflow_interpret(
                        json!({"type":"reopen_goal","change_request":"Добавить Windows"}),
                    ),
                    sse("Предлагаемая цель: Сделать CLI для Windows", 2, 1, 3),
                    workflow_interpret(json!({"type":"approve_goal"})),
                    sse("Обсудим новый план.", 2, 1, 3),
                    workflow_check(workflow_patch(3), json!({"type":"await_user"})),
                ]
                .into_iter()
                .collect(),
            )),
        })
        .mount(&server)
        .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("goal-cycle.sqlite3");
    let log_path = directory.path().join("goal-cycle.jsonl");
    let config = write_observed_workflow_config(&server.uri(), &log_path, "strategy='summary'");
    let output = run_cli_args(
        config.path(),
        &database,
        &[],
        "Нужен CLI\n/task\n/debug\nУтверждаю цель\n/task\nВернись к цели: добавь Windows\n/task\nУтверждаю новую цель\n/task\n/exit\n",
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains("Phase: goal_definition · status: active"),
        "{stdout}"
    );
    assert!(
        stdout.contains("Goal (working, revision 0): Нужен CLI"),
        "{stdout}"
    );
    assert!(
        stdout.contains("Goal (approved, revision 2): Сделать CLI для Windows"),
        "{stdout}"
    );
    let store = DialogStore::open(&database).unwrap();
    let state = store
        .load_workflow(store.latest_id().unwrap().unwrap())
        .unwrap()
        .current_task
        .unwrap();
    assert_eq!(state.phase, TaskPhase::Planning);
    assert_eq!(state.goal, "Сделать CLI для Windows");
    assert_eq!(state.goal_revision, 2);
    assert!(state.goal_proposal.is_none());
    assert_eq!(state.current_stage_sequence, 4);
    let log = std::fs::read_to_string(log_path).unwrap();
    assert!(log.contains("\"goal_parse_result\":\"valid\""));
    assert!(!log.contains("Сделать CLI для Windows"), "{log}");
    assert!(!log.contains("Нужен CLI"), "{log}");
    assert_eq!(server.received_requests().await.unwrap().len(), 9);
}

// Break caught: autonomous ordinary turns must render as distinct assistant
// blocks without attributing the hidden controller instruction to the user.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_autonomous_responses_are_separate_and_hide_controller_text() {
    let server = MockServer::start().await;
    let marker = "SYNTHETIC_CONTROLLER_SECRET_11";
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(SequenceResponder {
            responses: Arc::new(Mutex::new(
                [
                    workflow_interpret(json!({"type":"continue","instruction":"continue"})),
                    sse("first visible answer", 2, 1, 3),
                    sse(
                        &json!({
                            "patch": {"expected_version":1,"plan_append":{"steps":[],"acceptance_criteria":[]},"step_updates":[],"current_step_id":null,"expected_action":null,"checkpoint":null},
                            "decision": {"type":"continue","instruction":marker,"confidence":0.95}
                        })
                        .to_string(),
                        2,
                        1,
                        3,
                    ),
                    sse("second visible answer", 2, 1, 3),
                    sse(
                        &json!({
                            "patch": {"expected_version":2,"plan_append":{"steps":[],"acceptance_criteria":[]},"step_updates":[],"current_step_id":null,"expected_action":null,"checkpoint":null},
                            "decision": {"type":"await_user"}
                        })
                        .to_string(),
                        2,
                        1,
                        3,
                    ),
                ]
                .into_iter()
                .collect(),
            )),
        })
        .mount(&server)
        .await;
    let config = write_workflow_config(&server.uri());
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("autonomous-output.sqlite3");
    let mut seed = DialogStore::open(&database).unwrap();
    seed.start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "approved goal")
        .unwrap();
    drop(seed);
    mark_approved_planning_fixture(&database);

    let output = run_cli_args(
        config.path(),
        &database,
        &["--resume-last"],
        "build it\n/exit\n",
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(stdout.matches("assistant> ").count(), 2, "{stdout}");
    assert!(
        stdout.contains("assistant> first visible answer"),
        "{stdout}"
    );
    assert!(
        stdout.contains("assistant> second visible answer"),
        "{stdout}"
    );
    assert!(
        stdout.contains("Controller · autonomous turn 1 · planning"),
        "{stdout}"
    );
    assert!(!stdout.contains(marker), "{stdout}");
    assert!(!stderr.contains(marker), "{stderr}");

    let replay = run_cli_args(config.path(), &database, &["--resume-last"], "/exit\n");
    assert!(replay.status.success());
    let replay = String::from_utf8(replay.stdout).unwrap();
    assert!(replay.contains("first visible answer"), "{replay}");
    assert!(replay.contains("second visible answer"), "{replay}");
    assert!(!replay.contains(marker), "{replay}");
    let listed = DialogStore::open(&database).unwrap().list().unwrap();
    assert_eq!(listed.len(), 1);
    assert!(!listed[0].title.contains(marker));
}

// Break caught: a provider body may echo a hidden controller instruction, but
// ordinary-turn failures must expose only safe operator metadata.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_ordinary_provider_failure_never_prints_hidden_payload() {
    let server = MockServer::start().await;
    let marker = "CTRL_PAYLOAD_ECHO_SECRET";
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(SequenceResponder {
            responses: Arc::new(Mutex::new(
                [
                    workflow_interpret(json!({"type":"continue","instruction":"continue"})),
                    sse("first visible answer", 2, 1, 3),
                    sse(
                        &json!({
                            "patch": {"expected_version":1,"plan_append":{"steps":[],"acceptance_criteria":[]},"step_updates":[],"current_step_id":null,"expected_action":null,"checkpoint":null},
                            "decision": {"type":"continue","instruction":marker,"confidence":0.95}
                        })
                        .to_string(),
                        2,
                        1,
                        3,
                    ),
                    ResponseTemplate::new(400).set_body_string(marker),
                ]
                .into_iter()
                .collect(),
            )),
        })
        .mount(&server)
        .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("ordinary-error.sqlite3");
    let mut seed = DialogStore::open(&database).unwrap();
    seed.start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "approved goal")
        .unwrap();
    drop(seed);
    mark_approved_planning_fixture(&database);
    let log_path = directory.path().join("ordinary-error.jsonl");
    let config = write_observed_workflow_config(&server.uri(), &log_path, "strategy = \"summary\"");

    let output = run_cli_args(
        config.path(),
        &database,
        &["--resume-last"],
        "build it\n/exit\n",
    );

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stdout.contains(marker), "{stdout}");
    assert!(!stderr.contains(marker), "{stderr}");
    assert!(
        stderr.contains("provider failure · component: ordinary · kind: http · status: 400"),
        "{stderr}"
    );
    let log = std::fs::read_to_string(log_path).unwrap();
    assert!(!log.contains(marker), "{log}");
    assert!(log.lines().any(|line| {
        let value: Value = serde_json::from_str(line).unwrap();
        value["event"] == "workflow"
            && value["details"]["component"] == "ordinary"
            && value["details"]["outcome"] == "failed"
            && value["details"]["error_kind"] == "http"
            && value["details"]["http_status"] == 400
    }));
}

// Break caught: advisory compaction warnings use the same safe provider
// classification as terminal errors and record a failure at the call boundary.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_compaction_provider_failure_never_prints_response_body() {
    let server = MockServer::start().await;
    let marker = "COMPACTION_PROVIDER_BODY_SECRET";
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(SequenceResponder {
            responses: Arc::new(Mutex::new(
                [
                    sse("visible answer", 8, 1, 9),
                    ResponseTemplate::new(400).set_body_string(marker),
                    sse(
                        &json!({
                            "patch": {"expected_version":0,"plan_append":{"steps":[],"acceptance_criteria":[]},"step_updates":[],"current_step_id":null,"expected_action":null,"checkpoint":null},
                            "decision": {"type":"await_user"}
                        })
                        .to_string(),
                        2,
                        1,
                        3,
                    ),
                ]
                .into_iter()
                .collect(),
            )),
        })
        .mount(&server)
        .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("compaction-error.sqlite3");
    let log_path = directory.path().join("compaction-error.jsonl");
    let config = write_observed_workflow_config(
        &server.uri(),
        &log_path,
        "strategy = \"summary\"\ncompact_after_prompt_tokens = 1\nkeep_last_messages = 1\nsummary_max_tokens = 64",
    );

    let output = run_cli_args(config.path(), &database, &[], "build it\n/exit\n");

    assert!(output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stderr.contains(marker), "{stderr}");
    assert!(
        stderr.contains("provider failure · component: compaction · kind: http · status: 400"),
        "{stderr}"
    );
    let log = std::fs::read_to_string(log_path).unwrap();
    assert!(!log.contains(marker), "{log}");
    assert!(log.lines().any(|line| {
        let value: Value = serde_json::from_str(line).unwrap();
        value["event"] == "workflow"
            && value["details"]["component"] == "compaction"
            && value["details"]["outcome"] == "failed"
            && value["details"]["error_kind"] == "http"
    }));
}

// Break caught: facts refresh failures may abort the turn, but neither the
// warning nor the ordinary terminal error may reveal the provider body.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_facts_provider_failure_never_prints_response_body() {
    let server = MockServer::start().await;
    let marker = "FACTS_PROVIDER_BODY_SECRET";
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(400).set_body_string(marker))
        .mount(&server)
        .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("facts-error.sqlite3");
    let log_path = directory.path().join("facts-error.jsonl");
    let config = write_observed_workflow_config(
        &server.uri(),
        &log_path,
        "strategy = \"sticky_facts\"\nfacts_max_tokens = 64",
    );

    let output = run_cli_args(config.path(), &database, &[], "build it\n/exit\n");

    assert!(output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stderr.contains(marker), "{stderr}");
    assert!(
        stderr.contains("provider failure · component: facts · kind: http · status: 400"),
        "{stderr}"
    );
    let log = std::fs::read_to_string(log_path).unwrap();
    assert!(!log.contains(marker), "{log}");
    assert!(log.lines().any(|line| {
        let value: Value = serde_json::from_str(line).unwrap();
        value["event"] == "workflow"
            && value["details"]["component"] == "facts"
            && value["details"]["outcome"] == "failed"
            && value["details"]["error_kind"] == "http"
    }));
}

// Break caught: the CLI must recover before reading the first restored prompt, even when that prompt exits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resumed_cli_recovers_pending_work_before_prompt_and_warns_on_failure() {
    for valid in [false, true] {
        let server = MockServer::start().await;
        let response = if valid {
            json!({"patch":{"expected_version":0,"plan_append":{"steps":[],"acceptance_criteria":[]},"step_updates":[],"current_step_id":null,"expected_action":"review recovered result","checkpoint":null},"decision":{"type":"continue","instruction":"HIDDEN MUST NOT RUN","confidence":0.95}}).to_string()
        } else {
            "invalid checker payload".into()
        };
        Mock::given(method("POST"))
            .respond_with(sse(&response, 2, 1, 3))
            .mount(&server)
            .await;
        let mut config = NamedTempFile::new().unwrap();
        write!(config, "api_key='test-key'\nbase_url='{}'\n[workflow]\nchecker_model='checker-only'\n[context]\nstrategy='summary'\n", server.uri()).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("recovery.sqlite3");
        let mut store = DialogStore::open(&database).unwrap();
        let started = store
            .start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "saved goal")
            .unwrap();
        mark_approved_planning_fixture(&database);
        store
            .append_answer_for_processing(AnswerCommit {
                dialog_id: started.dialog_id,
                task_id: started.task.id,
                stage_run_id: started.stage_run_id,
                expected_version: 0,
                content: "saved answer",
                usage: None,
            })
            .unwrap();
        let output = run_cli_args(config.path(), &database, &["--resume-last"], "/exit\n");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].body_json::<Value>().unwrap()["model"],
            "checker-only"
        );
        assert!(
            !String::from_utf8(output.stdout)
                .unwrap()
                .contains("HIDDEN MUST NOT RUN")
        );
        let stderr = String::from_utf8(output.stderr).unwrap();
        if !valid {
            assert!(stderr.contains("workflow recovery"), "{stderr}");
        }
        assert_eq!(store.load(started.dialog_id).unwrap().messages.len(), 2);
        assert_eq!(
            store
                .load_workflow(started.dialog_id)
                .unwrap()
                .current_task
                .unwrap()
                .expected_action
                .as_deref(),
            if valid {
                Some("review recovered result")
            } else {
                None
            }
        );
    }
}

// Break caught: a configured but unavailable debug path still owns one safe
// warning. Workflow execution and recovery must surface it once without making
// logging availability part of workflow correctness.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unavailable_workflow_debug_path_warns_once_on_run_and_recovery() {
    let server = MockServer::start().await;
    let await_user = json!({
        "patch":{"expected_version":0,"plan_append":{"steps":[],"acceptance_criteria":[]},"step_updates":[],"current_step_id":null,"expected_action":null,"checkpoint":null},
        "decision":{"type":"await_user"}
    })
    .to_string();
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(SequenceResponder {
            responses: Arc::new(Mutex::new(
                [
                    sse("completed answer", 2, 1, 3),
                    sse(&await_user, 2, 1, 3),
                    sse(&await_user, 2, 1, 3),
                ]
                .into_iter()
                .collect(),
            )),
        })
        .mount(&server)
        .await;
    let directory = tempfile::tempdir().unwrap();
    let missing_parent = directory.path().join("missing-debug-parent");

    let normal_database = directory.path().join("normal-warning.sqlite3");
    let normal_log = missing_parent.join("normal.jsonl");
    let normal_config =
        write_observed_workflow_config(&server.uri(), &normal_log, "strategy = \"summary\"");
    let normal = run_cli_args(
        normal_config.path(),
        &normal_database,
        &[],
        "start task\n/exit\n",
    );
    assert!(
        normal.status.success(),
        "{}",
        String::from_utf8_lossy(&normal.stderr)
    );
    let normal_stderr = String::from_utf8(normal.stderr).unwrap();
    assert_eq!(
        normal_stderr
            .matches("debug log disabled: failed to open")
            .count(),
        1,
        "{normal_stderr}"
    );

    let recovery_database = directory.path().join("recovery-warning.sqlite3");
    let mut store = DialogStore::open(&recovery_database).unwrap();
    let started = store
        .start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "saved goal")
        .unwrap();
    mark_approved_planning_fixture(&recovery_database);
    store
        .append_answer_for_processing(AnswerCommit {
            dialog_id: started.dialog_id,
            task_id: started.task.id,
            stage_run_id: started.stage_run_id,
            expected_version: 0,
            content: "saved answer",
            usage: None,
        })
        .unwrap();
    drop(store);
    let recovery_log = missing_parent.join("recovery.jsonl");
    let recovery_config =
        write_observed_workflow_config(&server.uri(), &recovery_log, "strategy = \"summary\"");
    let recovery = run_cli_args(
        recovery_config.path(),
        &recovery_database,
        &["--resume-last"],
        "/exit\n",
    );
    assert!(
        recovery.status.success(),
        "{}",
        String::from_utf8_lossy(&recovery.stderr)
    );
    let recovery_stderr = String::from_utf8(recovery.stderr).unwrap();
    assert_eq!(
        recovery_stderr
            .matches("debug log disabled: failed to open")
            .count(),
        1,
        "{recovery_stderr}"
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

// Break caught: restoring a paused dialog is inert until a human continuation is accepted.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resumed_paused_dialog_waits_for_human_and_preserves_task_and_stage() {
    let server = MockServer::start().await;
    let config = write_workflow_config(&server.uri());
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("paused-resume.sqlite3");
    let mut store = DialogStore::open(&database).unwrap();
    let started = store
        .start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "saved goal")
        .unwrap();
    mark_approved_planning_fixture(&database);
    let PauseOutcome::Paused(paused) = store.pause_current_task(started.dialog_id).unwrap() else {
        panic!("active task should pause");
    };
    let id = started.dialog_id.to_string();

    let restored = run_cli_args(config.path(), &database, &["--resume", &id], "/exit\n");
    assert!(
        restored.status.success(),
        "{}",
        String::from_utf8_lossy(&restored.stderr)
    );
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        0,
        "restore must not call any provider before human input"
    );
    assert_eq!(
        store
            .load_workflow(started.dialog_id)
            .unwrap()
            .current_task
            .unwrap(),
        *paused
    );

    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(SequenceResponder {
            responses: Arc::new(Mutex::new(
                [
                    sse(
                        &json!({"confidence":0.95,"intent":{"type":"continue","instruction":"continue"}}).to_string(),
                        2,
                        1,
                        3,
                    ),
                    sse("resumed answer", 2, 1, 3),
                    sse(
                        &json!({"patch":{"expected_version":2,"plan_append":{"steps":[],"acceptance_criteria":[]},"step_updates":[],"current_step_id":null,"expected_action":null,"checkpoint":null},"decision":{"type":"await_user"}}).to_string(),
                        2,
                        1,
                        3,
                    ),
                    sse(
                        &json!({"confidence":0.95,"intent":{"type":"start_new_task","goal":"other goal"}}).to_string(),
                        2,
                        1,
                        3,
                    ),
                ]
                .into_iter()
                .collect(),
            )),
        })
        .mount(&server)
        .await;

    let accepted = run_cli_args(
        config.path(),
        &database,
        &["--resume", &id],
        "continue\n/exit\n",
    );
    assert!(
        accepted.status.success(),
        "{}",
        String::from_utf8_lossy(&accepted.stderr)
    );
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 3, "interpreter, ordinary, then checker");
    let interpreter_body: Value = requests[0].body_json().unwrap();
    let interpreter_context = interpreter_body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|message| message["content"].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        interpreter_context.contains("\"status\":\"paused\""),
        "{interpreter_context}"
    );
    let ordinary_body: Value = requests[1].body_json().unwrap();
    let ordinary_context = ordinary_body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|message| message["content"].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        ordinary_context.contains("\"status\": \"active\""),
        "{ordinary_context}"
    );
    assert!(
        !ordinary_context.contains("\"status\": \"paused\""),
        "{ordinary_context}"
    );
    let resumed = store
        .load_workflow(started.dialog_id)
        .unwrap()
        .current_task
        .unwrap();
    assert_eq!(resumed.id, started.task.id);
    assert_eq!(resumed.current_stage_run_id, started.stage_run_id);
    assert_eq!(resumed.status, TaskStatus::Active);
    assert_eq!(resumed.version, 2);

    let PauseOutcome::Paused(repaused) = store.pause_current_task(started.dialog_id).unwrap()
    else {
        panic!("resumed task should pause again");
    };
    assert_eq!(repaused.version, 3);
    let rejected = run_cli_args(
        config.path(),
        &database,
        &["--resume", &id],
        "start another task\n/exit\n",
    );
    assert!(
        rejected.status.success(),
        "{}",
        String::from_utf8_lossy(&rejected.stderr)
    );
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        4,
        "rejected new-task interpretation must not reach the ordinary model"
    );
    let after_rejection = store
        .load_workflow(started.dialog_id)
        .unwrap()
        .current_task
        .unwrap();
    assert_eq!(after_rejection.id, started.task.id);
    assert_eq!(after_rejection.current_stage_run_id, started.stage_run_id);
    assert_eq!(after_rejection.status, TaskStatus::Paused);
    assert_eq!(after_rejection.version, 3);
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

// Break caught: completed diagnostic boundaries must reach the log before a
// later checker await, so cancelling that await cannot erase the committed
// ordinary-answer audit trail.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_during_checker_keeps_completed_workflow_diagnostics() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(SequenceResponder {
            responses: Arc::new(Mutex::new(
                [
                    workflow_interpret(json!({"type":"continue","instruction":"continue"})),
                    sse("visible committed answer", 2, 1, 3),
                    ResponseTemplate::new(200)
                        .set_delay(Duration::from_secs(30))
                        .insert_header("content-type", "text/event-stream")
                        .set_body_string("data: [DONE]\n\n"),
                ]
                .into_iter()
                .collect(),
            )),
        })
        .mount(&server)
        .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("checker-interrupt.sqlite3");
    let mut seed = DialogStore::open(&database).unwrap();
    seed.start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "approved goal")
        .unwrap();
    drop(seed);
    mark_approved_planning_fixture(&database);
    let log_path = directory.path().join("checker-interrupt.jsonl");
    let config = write_observed_workflow_config(&server.uri(), &log_path, "strategy = \"summary\"");
    let mut child = Command::new(env!("CARGO_BIN_EXE_deepseek-cli"))
        .arg("--config")
        .arg(config.path())
        .arg("--db")
        .arg(&database)
        .arg("--resume-last")
        .env_remove("DEEPSEEK_API_KEY")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (answer_seen, stdout_reader) =
        observe_stdout(child.stdout.take().unwrap(), "visible committed answer");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"start task\n")
        .unwrap();
    answer_seen
        .recv_timeout(Duration::from_secs(5))
        .expect("ordinary answer should render before checker blocks");
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.received_requests().await.unwrap().len() < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("checker request should start");

    send_sigint(&child);
    let status = child.wait().unwrap();
    let stdout = String::from_utf8(stdout_reader.join().unwrap()).unwrap();
    let mut stderr = Vec::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_end(&mut stderr)
        .unwrap();
    assert!(status.success(), "{}", String::from_utf8_lossy(&stderr));
    assert!(stdout.contains("visible committed answer"), "{stdout}");

    let store = DialogStore::open(&database).unwrap();
    let dialog_id = store.latest_id().unwrap().unwrap();
    let dialog = store.load(dialog_id).unwrap();
    assert_eq!(dialog.messages.len(), 3);
    assert_eq!(dialog.messages[2].content(), "visible committed answer");
    assert_eq!(
        store
            .load_workflow(dialog_id)
            .unwrap()
            .current_task
            .unwrap()
            .status,
        TaskStatus::Paused
    );
    let events: Vec<Value> = std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|value: &Value| value["event"] == "workflow")
        .collect();
    let ordinary = events
        .iter()
        .find(|event| event["details"]["component"] == "ordinary")
        .unwrap();
    assert_eq!(ordinary["details"]["processing_status"], "pending");
}

// Break caught: a checker decision completes before the handoff call starts.
// Cancelling the handoff must not erase that checker record or fabricate a
// committed transition.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_during_handoff_keeps_completed_checker_diagnostic() {
    let server = MockServer::start().await;
    let checker = json!({
        "patch": {
            "expected_version":1,
            "plan_append": {
                "steps":[{"id":"s1","description":"implement it","status":"pending"}],
                "acceptance_criteria":["it works"]
            },
            "step_updates":[],
            "current_step_id":"s1",
            "expected_action":"implement s1",
            "checkpoint":null
        },
        "decision":{"type":"emit_transition","event":"planning_completed","evidence":[],"confidence":0.95}
    })
    .to_string();
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(SequenceResponder {
            responses: Arc::new(Mutex::new(
                [
                    workflow_interpret(json!({"type":"continue","instruction":"continue"})),
                    sse("visible planning answer", 2, 1, 3),
                    sse(&checker, 3, 2, 5),
                    ResponseTemplate::new(200)
                        .set_delay(Duration::from_secs(30))
                        .insert_header("content-type", "text/event-stream")
                        .set_body_string("data: [DONE]\n\n"),
                ]
                .into_iter()
                .collect(),
            )),
        })
        .mount(&server)
        .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("handoff-interrupt.sqlite3");
    let mut seed = DialogStore::open(&database).unwrap();
    seed.start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "approved goal")
        .unwrap();
    drop(seed);
    mark_approved_planning_fixture(&database);
    let log_path = directory.path().join("handoff-interrupt.jsonl");
    let config = write_observed_workflow_config(&server.uri(), &log_path, "strategy = \"summary\"");
    let mut child = Command::new(env!("CARGO_BIN_EXE_deepseek-cli"))
        .arg("--config")
        .arg(config.path())
        .arg("--db")
        .arg(&database)
        .arg("--resume-last")
        .env_remove("DEEPSEEK_API_KEY")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (answer_seen, stdout_reader) =
        observe_stdout(child.stdout.take().unwrap(), "visible planning answer");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"start task\n")
        .unwrap();
    answer_seen
        .recv_timeout(Duration::from_secs(5))
        .expect("ordinary answer should render before handoff blocks");
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.received_requests().await.unwrap().len() < 4 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("handoff request should start");

    send_sigint(&child);
    let status = child.wait().unwrap();
    let _ = stdout_reader.join().unwrap();
    let mut stderr = Vec::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_end(&mut stderr)
        .unwrap();
    assert!(status.success(), "{}", String::from_utf8_lossy(&stderr));
    let store = DialogStore::open(&database).unwrap();
    let task = store
        .load_workflow(store.latest_id().unwrap().unwrap())
        .unwrap()
        .current_task
        .unwrap();
    assert_eq!(task.phase, TaskPhase::Planning);
    assert_eq!(task.current_stage_sequence, 1);
    assert_eq!(task.status, TaskStatus::Paused);

    let events: Vec<Value> = std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|value: &Value| value["event"] == "workflow")
        .collect();
    let components: Vec<&str> = events
        .iter()
        .map(|event| event["details"]["component"].as_str().unwrap())
        .collect();
    assert!(components.contains(&"ordinary"));
    assert!(components.contains(&"continuation"));
    let checker_event = &events
        .iter()
        .find(|event| event["details"]["component"] == "continuation")
        .unwrap()["details"];
    assert_eq!(checker_event["accepted"], true);
    assert_eq!(checker_event["processing_status"], "processing");
    assert_eq!(checker_event["transition_id"], Value::Null);
}

// Break caught: SIGINT must drop the stream before pausing, and pre-task SIGINT must stay taskless.
#[cfg(unix)]
#[test]
fn interrupt_discards_partial_work_pauses_exactly_once_and_never_synthesizes_a_task() {
    {
        let (base_url, release, partial_sent, server) = spawn_partial_sse_server();
        let config = write_workflow_config(&base_url);
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("interrupt.sqlite3");
        let mut child = Command::new(env!("CARGO_BIN_EXE_deepseek-cli"))
            .arg("--config")
            .arg(config.path())
            .arg("--db")
            .arg(&database)
            .env_remove("DEEPSEEK_API_KEY")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdout_pipe = child.stdout.take().unwrap();
        let stdout_reader = thread::spawn(move || {
            let mut output = Vec::new();
            stdout_pipe.read_to_end(&mut output).unwrap();
            output
        });
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"Do not commit a partial answer\n")
            .unwrap();
        partial_sent
            .recv_timeout(Duration::from_secs(5))
            .expect("provider should send the first fragment");
        let stdin = child.stdin.take().unwrap();
        let mut stderr_pipe = child.stderr.take().unwrap();
        send_sigint(&child);
        let status = wait_for_exit_while_stdin_is_open(child, stdin);
        let _ = release.send(());
        server.join().unwrap();
        let stdout = String::from_utf8(stdout_reader.join().unwrap()).unwrap();
        let mut stderr = Vec::new();
        stderr_pipe.read_to_end(&mut stderr).unwrap();
        assert!(status.success(), "{}", String::from_utf8_lossy(&stderr));
        assert!(
            !stdout.contains("partial fragment"),
            "goal proposals must be buffered: {stdout}"
        );
        assert!(
            stdout.contains("partial model result was discarded"),
            "{stdout}"
        );

        let store = DialogStore::open(&database).unwrap();
        let id = store.latest_id().unwrap().unwrap();
        let dialog = store.load(id).unwrap();
        assert_eq!(dialog.messages.len(), 1);
        assert_eq!(
            dialog.messages[0].content(),
            "Do not commit a partial answer"
        );
        let task = store.load_workflow(id).unwrap().current_task.unwrap();
        assert_eq!(task.phase, TaskPhase::GoalDefinition);
        assert_eq!(task.status, TaskStatus::Paused);
        assert_eq!(task.version, 1);
        assert_eq!(task.current_stage_sequence, 1);
        assert!(task.checkpoint.summary.is_empty());
        assert!(store.load_pending_processing(id).unwrap().is_empty());
        let connection = Connection::open(&database).unwrap();
        let stages: i64 = connection
            .query_row("SELECT count(*) FROM task_stage_runs", [], |row| row.get(0))
            .unwrap();
        let transitions: i64 = connection
            .query_row("SELECT count(*) FROM task_transitions", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!((stages, transitions), (1, 0));
    }

    {
        let config = write_workflow_config("http://127.0.0.1:1");
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("no-task.sqlite3");
        let mut child = Command::new(env!("CARGO_BIN_EXE_deepseek-cli"))
            .arg("--config")
            .arg(config.path())
            .arg("--db")
            .arg(&database)
            .env_remove("DEEPSEEK_API_KEY")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let (prompt_seen, stdout_reader) = observe_stdout(child.stdout.take().unwrap(), "you> ");
        prompt_seen
            .recv_timeout(Duration::from_secs(5))
            .expect("CLI should reach the first input prompt");
        // Child::wait closes its owned stdin: keep it open so EOF cannot race SIGINT.
        let stdin = child.stdin.take().unwrap();
        let mut stderr_pipe = child.stderr.take().unwrap();
        send_sigint(&child);
        let status = wait_for_exit_while_stdin_is_open(child, stdin);
        let stdout = String::from_utf8(stdout_reader.join().unwrap()).unwrap();
        let mut stderr = Vec::new();
        stderr_pipe.read_to_end(&mut stderr).unwrap();
        assert!(status.success(), "{}", String::from_utf8_lossy(&stderr));
        assert!(stdout.contains("No workflow task was created"), "{stdout}");
        assert!(
            DialogStore::open(&database)
                .unwrap()
                .list()
                .unwrap()
                .is_empty()
        );
    }
}

// Break caught: Tokio's blocking stdin reader must not hold runtime shutdown open after SIGINT.
#[cfg(unix)]
#[test]
fn interrupt_at_idle_prompt_exits_while_stdin_remains_open() {
    let config = write_workflow_config("http://127.0.0.1:1");
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("idle-interrupt.sqlite3");
    let mut child = Command::new(env!("CARGO_BIN_EXE_deepseek-cli"))
        .arg("--config")
        .arg(config.path())
        .arg("--db")
        .arg(&database)
        .env_remove("DEEPSEEK_API_KEY")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdin = child.stdin.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let (prompt_seen, stdout_reader) = observe_stdout(child.stdout.take().unwrap(), "you> ");
    prompt_seen
        .recv_timeout(Duration::from_secs(5))
        .expect("CLI should reach the first input prompt");

    send_sigint(&child);
    let status = wait_for_exit_while_stdin_is_open(child, stdin);
    let stdout = String::from_utf8(stdout_reader.join().unwrap()).unwrap();
    let mut stderr_output = Vec::new();
    stderr.read_to_end(&mut stderr_output).unwrap();
    assert!(
        status.success(),
        "{}",
        String::from_utf8_lossy(&stderr_output)
    );
    assert!(stdout.contains("No workflow task was created"), "{stdout}");
    assert!(
        DialogStore::open(&database)
            .unwrap()
            .list()
            .unwrap()
            .is_empty()
    );
}

// Break caught: terminal cleanup failures must not bypass the durable pause transaction.
#[cfg(unix)]
#[test]
fn interrupt_with_closed_stdout_still_pauses_the_active_task() {
    let (base_url, release, partial_sent, server) = spawn_partial_sse_server();
    let config = write_workflow_config(&base_url);
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("closed-stdout-interrupt.sqlite3");
    let mut child = Command::new(env!("CARGO_BIN_EXE_deepseek-cli"))
        .arg("--config")
        .arg(config.path())
        .arg("--db")
        .arg(&database)
        .env_remove("DEEPSEEK_API_KEY")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let stdout_pipe = child.stdout.take().unwrap();
    stdin
        .write_all(b"Pause even if output is closed\n")
        .unwrap();
    partial_sent
        .recv_timeout(Duration::from_secs(5))
        .expect("provider should send the first fragment");
    drop(stdout_pipe);

    send_sigint(&child);
    let status = wait_for_exit_while_stdin_is_open(child, stdin);
    let _ = release.send(());
    server.join().unwrap();
    let mut stderr_output = Vec::new();
    stderr.read_to_end(&mut stderr_output).unwrap();
    assert!(
        !status.success(),
        "closed stdout should still report the terminal cleanup error"
    );
    assert!(
        String::from_utf8_lossy(&stderr_output).contains("terminal I/O failed"),
        "{}",
        String::from_utf8_lossy(&stderr_output)
    );

    let store = DialogStore::open(&database).unwrap();
    let id = store.latest_id().unwrap().unwrap();
    let dialog = store.load(id).unwrap();
    assert_eq!(dialog.messages.len(), 1);
    assert_eq!(
        dialog.messages[0].content(),
        "Pause even if output is closed"
    );
    let task = store.load_workflow(id).unwrap().current_task.unwrap();
    assert_eq!(task.status, TaskStatus::Paused);
    assert_eq!(task.version, 1);
    assert!(store.load_pending_processing(id).unwrap().is_empty());
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
    assert!(stderr.contains("provider failure · component: chat · kind: http · status: 500"));
    assert!(!stderr.contains("temporary failure"));
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
