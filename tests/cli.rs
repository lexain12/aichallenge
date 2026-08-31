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
    let mut child = Command::new(env!("CARGO_BIN_EXE_deepseek-cli"))
        .arg("--config")
        .arg(config_path)
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
