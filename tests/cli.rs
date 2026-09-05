use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

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
    run_cli_with_args(config_path, input, &[])
}

fn run_cli_with_args(config_path: &Path, input: &str, args: &[&str]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_deepseek-cli"))
        .arg("--config")
        .arg(config_path)
        .args(args)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn comparison_sends_same_query_with_independent_controls() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200)
            .insert_header("content-type", "text/event-stream")
            .set_body_string("data: {\"choices\":[{\"delta\":{\"content\":\"Рецепт\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
        .expect(2)
        .mount(&server).await;
    let config = write_config(&server.uri());
    let output = Command::new(env!("CARGO_BIN_EXE_deepseek-cli"))
        .arg("--config")
        .arg(config.path())
        .arg("--compare")
        .arg("ПП-рецепт омлета")
        .output()
        .expect("run comparison");
    assert!(output.status.success());
    let requests = server.received_requests().await.unwrap();
    let baseline: serde_json::Value = requests[0].body_json().unwrap();
    let controlled: serde_json::Value = requests[1].body_json().unwrap();
    assert_eq!(baseline["messages"].as_array().unwrap().len(), 1);
    assert_eq!(controlled["messages"].as_array().unwrap().len(), 2);
    assert_eq!(baseline["messages"][0], controlled["messages"][1]);
    assert_eq!(controlled["messages"][0]["role"], "system");
    assert_eq!(baseline["model"], controlled["model"]);
    assert_eq!(baseline["temperature"], controlled["temperature"]);
    assert_eq!(baseline["thinking"], controlled["thinking"]);
    assert!(baseline.get("stop").is_none());
    assert_eq!(controlled["stop"], serde_json::json!(["Готово"]));
    assert_eq!(baseline["max_tokens"], 4096);
    assert_eq!(controlled["max_tokens"], 1200);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Без ограничений формата"));
    assert!(stdout.contains("С ограничениями"));
    assert_eq!(stdout.matches("finish_reason: stop").count(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncated_recipe_is_reported_and_not_added_to_history() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200)
            .insert_header("content-type", "text/event-stream")
            .set_body_string("data: {\"choices\":[{\"delta\":{\"content\":\"Неполный рецепт\"},\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n\n"))
        .expect(2).mount(&server).await;
    let config = write_config(&server.uri());
    let output = run_cli(config.path(), "ПП-рецепт\nЕщё рецепт\n/exit\n");
    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("обрезан лимитом")
    );
    let requests = server.received_requests().await.unwrap();
    let second: serde_json::Value = requests[1].body_json().unwrap();
    assert_eq!(second["messages"].as_array().unwrap().len(), 2);
    assert_eq!(second["messages"][1]["content"], "Ещё рецепт");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interactive_compare_reads_queries_and_shows_two_answers_each() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200)
            .insert_header("content-type", "text/event-stream")
            .set_body_string("data: {\"choices\":[{\"delta\":{\"content\":\"Рецепт\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
        .expect(4).mount(&server).await;
    let config = write_config(&server.uri());
    let output = run_cli_with_args(
        config.path(),
        "\nПП-ужин\nПП-завтрак\n/exit\n",
        &["--compare"],
    );
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout.matches("Без ограничений формата").count(), 2);
    assert_eq!(stdout.matches("С ограничениями").count(), 2);
    let requests = server.received_requests().await.unwrap();
    for (pair, query) in requests.chunks_exact(2).zip(["ПП-ужин", "ПП-завтрак"]) {
        let baseline: serde_json::Value = pair[0].body_json().unwrap();
        let controlled: serde_json::Value = pair[1].body_json().unwrap();
        assert_eq!(baseline["messages"].as_array().unwrap().len(), 1);
        assert_eq!(controlled["messages"].as_array().unwrap().len(), 2);
        assert_eq!(baseline["messages"][0]["content"], query);
        assert_eq!(baseline["messages"][0], controlled["messages"][1]);
    }
}

#[test]
fn interactive_compare_exits_on_eof_without_request() {
    let config = write_config("http://127.0.0.1:1");
    let output = run_cli_with_args(config.path(), "", &["--compare"]);
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "you> \n");
}
