use deepseek_cli::chat::{Message, Role};
use deepseek_cli::client::{DeepSeekClient, TokenUsage};
use deepseek_cli::config::Config;
use deepseek_cli::workflow_model::{
    CompletionModel, DeepSeekCompletionModel, ModelError, ModelFuture, ModelRequest, ModelResponse,
};

struct FakeCompletionModel {
    name: String,
    content: String,
    usage: TokenUsage,
}

impl FakeCompletionModel {
    fn one(name: &str, content: &str, total_tokens: u64) -> Self {
        Self {
            name: name.to_owned(),
            content: content.to_owned(),
            usage: TokenUsage {
                total_tokens,
                ..TokenUsage::default()
            },
        }
    }
}

impl CompletionModel for FakeCompletionModel {
    fn name(&self) -> &str {
        &self.name
    }

    fn complete(&self, _request: ModelRequest) -> ModelFuture<'_> {
        let response = ModelResponse {
            content: self.content.clone(),
            usage: Some(self.usage),
        };
        Box::pin(async move { Ok::<_, ModelError>(response) })
    }
}

#[tokio::test]
async fn completion_model_can_be_replaced_without_http() {
    let model = FakeCompletionModel::one("checker-a", r#"{"decision":"await_user"}"#, 7);
    let response = model
        .complete(ModelRequest {
            messages: vec![Message::for_request(Role::User, "inspect")],
            max_tokens: 32,
        })
        .await
        .unwrap();

    assert_eq!(model.name(), "checker-a");
    assert_eq!(response.content, r#"{"decision":"await_user"}"#);
    assert_eq!(response.usage.unwrap().total_tokens, 7);
}

#[test]
fn deepseek_completion_model_rejects_blank_model_name() {
    let config =
        Config::from_toml("api_key = \"key\"\n[context]\nstrategy = \"summary\"", None).unwrap();
    let client = DeepSeekClient::new(&config).unwrap();

    assert!(DeepSeekCompletionModel::new(client, "   ".to_owned()).is_err());
}
