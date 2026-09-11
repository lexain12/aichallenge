use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use deepseek_cli::dialog::DialogStore;
use serde_json::{Value, json};
use tempfile::NamedTempFile;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn write_config(base_url: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("create temporary config");
    write!(
        file,
        r#"
api_key = "test-key"
base_url = "{base_url}"
model = "test-model"
timeout_seconds = 5
"#
    )
    .expect("write temporary config");
    file
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
        "you> Conversation cleared.\nyou> assistant> \nyou> "
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
        "you> \n"
    );
    assert!(output.stderr.is_empty());
}
