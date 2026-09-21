use std::future::Future;
use std::pin::Pin;

use thiserror::Error;

use crate::chat::Message;
use crate::client::{ClientError, DeepSeekClient, TokenUsage};

pub struct ModelRequest {
    pub messages: Vec<Message>,
    pub max_tokens: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelResponse {
    pub content: String,
    pub usage: Option<TokenUsage>,
}

#[derive(Debug, Error)]
pub enum ModelError {
    #[error("model name must not be blank")]
    BlankModelName,
    #[error(transparent)]
    Client(#[from] ClientError),
}

pub type ModelFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ModelResponse, ModelError>> + Send + 'a>>;

pub trait CompletionModel: Send + Sync {
    fn name(&self) -> &str;
    fn complete(&self, request: ModelRequest) -> ModelFuture<'_>;
}

#[derive(Clone)]
pub struct DeepSeekCompletionModel {
    client: DeepSeekClient,
    model: String,
}

impl DeepSeekCompletionModel {
    pub fn new(client: DeepSeekClient, model: String) -> Result<Self, ModelError> {
        let model = model.trim().to_owned();
        if model.is_empty() {
            return Err(ModelError::BlankModelName);
        }
        Ok(Self { client, model })
    }
}

impl CompletionModel for DeepSeekCompletionModel {
    fn name(&self) -> &str {
        &self.model
    }

    fn complete(&self, request: ModelRequest) -> ModelFuture<'_> {
        let client = self.client.clone();
        let model = self.model.clone();
        Box::pin(async move {
            let result = client
                .complete(&model, &request.messages, request.max_tokens)
                .await?;
            Ok(ModelResponse {
                content: result.answer().to_owned(),
                usage: result.usage(),
            })
        })
    }
}
