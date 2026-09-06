use deepseek_cli::client::{ClientError, DeepSeekClient};
use deepseek_cli::config::Config;
use deepseek_cli::reasoning::{self, Event, Method, Phase};
use serde_json::json;
use std::io::Write;
use tempfile::NamedTempFile;
use wiremock::matchers::{body_partial_json, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(server: &MockServer) -> DeepSeekClient {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        "api_key = \"test-key\"\nbase_url = \"{}\"\nsystem_prompt = \"Must not reach experiment\"",
        server.uri()
    )
    .unwrap();
    DeepSeekClient::new(&Config::load(file.path(), None).unwrap())
        .unwrap()
        .without_thinking()
}

fn response(text: &str, reason: &str) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("content-type", "text/event-stream")
        .set_body_string(format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"choices": [{"delta": {"content": text}, "finish_reason": reason}]})
        ))
}

#[tokio::test]
async fn generated_prompt_is_really_used_in_a_fresh_second_request() {
    let server = MockServer::start().await;
    let task = "Find the optimum";
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"messages": [{"role": "user", "content": Method::GeneratedPrompt.prompt(task)}]})))
        .respond_with(response("Enumerate every feasible subset.", "stop"))
        .expect(1).mount(&server).await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"messages": [{"role": "user", "content": format!("Enumerate every feasible subset.\n\nИсходная задача (используй именно эти условия):\n{task}")}]})))
        .respond_with(response("B + C = 48", "stop"))
        .expect(1).mount(&server).await;
    let mut prompt = String::new();
    let mut answer = String::new();
    let result = reasoning::solve(&client(&server), task, Method::GeneratedPrompt, |event| {
        if let Event::Text(phase, text) = event {
            match phase {
                Phase::Prompt => prompt.push_str(&text),
                Phase::Answer => answer.push_str(&text),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(prompt, "Enumerate every feasible subset.");
    assert_eq!(answer, result);
    for request in server.received_requests().await.unwrap() {
        let body: serde_json::Value = request.body_json().unwrap();
        assert_eq!(body["messages"].as_array().unwrap().len(), 1);
        assert_eq!(body["thinking"]["type"], "disabled");
    }
}

#[tokio::test]
async fn direct_sends_only_the_original_task_and_experts_share_one_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(response("Result", "stop"))
        .expect(3)
        .mount(&server)
        .await;
    let client = client(&server);
    let methods = [Method::Direct, Method::StepByStep, Method::Experts];
    for method in methods {
        reasoning::solve(&client, "Task", method, |_| {})
            .await
            .unwrap();
    }
    let requests = server.received_requests().await.unwrap();
    for (request, method) in requests.iter().zip(methods) {
        let body: serde_json::Value = request.body_json().unwrap();
        assert_eq!(
            body["messages"],
            json!([{"role": "user", "content": method.prompt("Task")} ])
        );
    }
    assert_eq!(Method::Direct.prompt("Task"), "Task");
}

#[tokio::test]
async fn truncated_or_empty_generated_prompt_never_starts_a_second_request() {
    for (text, reason) in [("incomplete prompt", "length"), ("", "stop")] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(response(text, reason))
            .expect(1)
            .mount(&server)
            .await;
        let error = reasoning::solve(&client(&server), "Task", Method::GeneratedPrompt, |_| {})
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ClientError::Truncated | ClientError::EmptyAnswer
        ));
    }
}

#[tokio::test]
async fn sums_usage_of_both_requests_including_prompt_generation() {
    use deepseek_cli::client::TokenUsage;
    use deepseek_cli::reasoning::TokenAccounting;
    let server = MockServer::start().await;
    let task = "Task";
    for (prompt, text, input, output) in [
        (
            Method::GeneratedPrompt.prompt(task),
            "Solve carefully",
            100,
            30,
        ),
        (
            format!("Solve carefully\n\nИсходная задача (используй именно эти условия):\n{task}"),
            "Solution",
            170,
            50,
        ),
    ] {
        let usage = json!({"prompt_tokens": input, "completion_tokens": output, "total_tokens": input + output});
        let body = format!(
            "data: {}\n\ndata: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
            json!({"choices": [{"delta": {"content": text}}], "usage": null}),
            json!({"choices": [{"delta": {}, "finish_reason": "stop"}], "usage": usage}),
            json!({"choices": [], "usage": usage})
        );
        Mock::given(method("POST"))
            .and(body_partial_json(json!({"messages": [{"role": "user", "content": prompt}], "stream_options": {"include_usage": true}})))
            .respond_with(ResponseTemplate::new(200).insert_header("content-type", "text/event-stream").set_body_string(body))
            .expect(1).mount(&server).await;
    }
    let mut tokens = TokenAccounting::default();
    reasoning::solve(&client(&server), task, Method::GeneratedPrompt, |event| {
        if let Event::Usage(phase, usage) = event {
            tokens.record(phase, usage);
        }
    })
    .await
    .unwrap();
    assert_eq!(
        tokens.total(),
        Some(TokenUsage {
            prompt_tokens: 270,
            completion_tokens: 80,
            total_tokens: 350
        })
    );
    assert!(tokens.summary(Method::GeneratedPrompt).contains("2/2"));
}

#[test]
fn missing_usage_is_not_reported_as_zero_or_as_a_complete_total() {
    use deepseek_cli::client::TokenUsage;
    use deepseek_cli::reasoning::TokenAccounting;
    let mut tokens = TokenAccounting::default();
    assert_eq!(tokens.total(), None);
    assert!(
        tokens
            .summary(Method::GeneratedPrompt)
            .contains("нет данных")
    );
    tokens.record(
        Phase::Prompt,
        TokenUsage {
            prompt_tokens: 10,
            completion_tokens: 5,
            total_tokens: 15,
        },
    );
    assert!(tokens.summary(Method::GeneratedPrompt).contains("1/2"));
    assert_eq!(tokens.total().unwrap().total_tokens, 15);
}
