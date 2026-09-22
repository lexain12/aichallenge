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
fn observe_and_close_stdout(
    mut stdout: std::process::ChildStdout,
    needle: &'static str,
) -> (mpsc::Receiver<()>, thread::JoinHandle<Vec<u8>>) {
    let (seen_tx, seen_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let mut output = Vec::new();
        let mut buffer = [0_u8; 256];
        loop {
            let read = stdout.read(&mut buffer).unwrap();
            if read == 0 {
                panic!("stdout closed before rendering {needle:?}");
            }
            output.extend_from_slice(&buffer[..read]);
            if String::from_utf8_lossy(&output).contains(needle) {
                seen_tx.send(()).unwrap();
                return output;
            }
        }
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
fn spawn_partial_sse_server() -> (String, mpsc::Sender<()>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (release_tx, release_rx) = mpsc::channel();
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
        let _ = release_rx.recv_timeout(Duration::from_secs(10));
    });
    (format!("http://{address}"), release_tx, handle)
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
        paused
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

// Break caught: SIGINT must drop the stream before pausing, and pre-task SIGINT must stay taskless.
#[cfg(unix)]
#[test]
fn interrupt_discards_partial_work_pauses_exactly_once_and_never_synthesizes_a_task() {
    {
        let (base_url, release, server) = spawn_partial_sse_server();
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
        let (partial_seen, stdout_reader) =
            observe_stdout(child.stdout.take().unwrap(), "partial fragment");
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"Do not commit a partial answer\n")
            .unwrap();
        partial_seen
            .recv_timeout(Duration::from_secs(5))
            .expect("CLI should render the first streamed fragment");
        send_sigint(&child);
        let status = child.wait().unwrap();
        let _ = release.send(());
        server.join().unwrap();
        let stdout = String::from_utf8(stdout_reader.join().unwrap()).unwrap();
        let mut stderr = Vec::new();
        child
            .stderr
            .take()
            .unwrap()
            .read_to_end(&mut stderr)
            .unwrap();
        assert!(status.success(), "{}", String::from_utf8_lossy(&stderr));
        assert!(stdout.contains("partial fragment"));
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
        assert_eq!(task.phase, TaskPhase::Planning);
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
    let (base_url, release, server) = spawn_partial_sse_server();
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
    let (partial_seen, stdout_reader) =
        observe_and_close_stdout(child.stdout.take().unwrap(), "partial fragment");
    stdin
        .write_all(b"Pause even if output is closed\n")
        .unwrap();
    partial_seen
        .recv_timeout(Duration::from_secs(5))
        .expect("CLI should render the first streamed fragment");
    let rendered = String::from_utf8(stdout_reader.join().unwrap()).unwrap();
    assert!(rendered.contains("partial fragment"));

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
