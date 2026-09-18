use crate::client::TokenUsage;
use crate::memory::DurableMemoryScope;
use serde::Serialize;

/// A role accepted by the DeepSeek Chat Completions API.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
}

/// One serializable chat message.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Message {
    role: Role,
    content: String,
    /// Local metadata; never part of a Chat Completions request.
    #[serde(skip)]
    usage: Option<TokenUsage>,
}

impl Message {
    pub fn for_request(role: Role, content: impl Into<String>) -> Self {
        Self::new(role, content.into())
    }

    pub(crate) fn new(role: Role, content: String) -> Self {
        Self {
            role,
            content,
            usage: None,
        }
    }

    pub(crate) fn with_usage(mut self, usage: Option<TokenUsage>) -> Self {
        self.usage = usage;
        self
    }

    pub fn usage(&self) -> Option<TokenUsage> {
        self.usage
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn content(&self) -> &str {
        &self.content
    }
}

/// A local action derived from one terminal input line.
#[derive(Debug, Eq, PartialEq)]
pub enum InputAction {
    Ignore,
    Exit,
    Clear,
    Stats,
    Branch,
    Switch(i64),
    Remember {
        scope: DurableMemoryScope,
        key: String,
        value: String,
    },
    Forget {
        scope: DurableMemoryScope,
        key: String,
    },
    Memory(Option<DurableMemoryScope>),
    InvalidCommand(String),
    Send(String),
}

pub fn parse_input(input: &str) -> InputAction {
    let input = input.trim();
    let mut parts = input.split_whitespace();
    match parts.next() {
        Some("/remember") => {
            let scope = parse_memory_scope(parts.next());
            let key = parts.next();
            let value = parts.collect::<Vec<_>>().join(" ");
            return match (scope, key) {
                (Some(scope), Some(key)) if !value.is_empty() => InputAction::Remember {
                    scope,
                    key: key.to_owned(),
                    value,
                },
                _ => InputAction::InvalidCommand(
                    "usage: /remember <user|task> <key> <value>".to_owned(),
                ),
            };
        }
        Some("/forget") => {
            let scope = parse_memory_scope(parts.next());
            let key = parts.next();
            return match (scope, key, parts.next()) {
                (Some(scope), Some(key), None) => InputAction::Forget {
                    scope,
                    key: key.to_owned(),
                },
                _ => InputAction::InvalidCommand("usage: /forget <user|task> <key>".to_owned()),
            };
        }
        Some("/memory") => {
            let argument = parts.next();
            let scope = parse_memory_scope(argument);
            return if (argument.is_none() || scope.is_some()) && parts.next().is_none() {
                InputAction::Memory(scope)
            } else {
                InputAction::InvalidCommand("usage: /memory [user|task]".to_owned())
            };
        }
        Some("/branch") => {
            return if parts.next().is_none() {
                InputAction::Branch
            } else {
                InputAction::InvalidCommand("usage: /branch".to_owned())
            };
        }
        Some("/switch") => {
            let id = parts.next().and_then(|value| value.parse::<i64>().ok());
            return if id.is_some_and(|value| value > 0) && parts.next().is_none() {
                InputAction::Switch(id.expect("positive ID checked"))
            } else {
                InputAction::InvalidCommand("usage: /switch <positive-dialog-id>".to_owned())
            };
        }
        _ => {}
    }
    match input {
        "" => InputAction::Ignore,
        "/exit" | "/quit" => InputAction::Exit,
        "/clear" => InputAction::Clear,
        "/stats" => InputAction::Stats,
        message => InputAction::Send(message.to_owned()),
    }
}

fn parse_memory_scope(value: Option<&str>) -> Option<DurableMemoryScope> {
    match value {
        Some("user") => Some(DurableMemoryScope::User),
        Some("task") => Some(DurableMemoryScope::Task),
        _ => None,
    }
}

/// Ordered dialog messages, optionally restored from persistent storage.
pub struct ChatHistory {
    system_message: Option<Message>,
    messages: Vec<Message>,
}

impl ChatHistory {
    pub(crate) fn from_messages(system_prompt: String, messages: Vec<Message>) -> Self {
        let mut history = Self::new(system_prompt);
        history.messages = messages;
        history
    }

    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    pub(crate) fn system_prompt(&self) -> &str {
        self.system_message.as_ref().map_or("", Message::content)
    }

    pub(crate) fn push(&mut self, role: Role, content: String) {
        self.messages.push(Message::new(role, content));
    }

    pub(crate) fn push_answer(&mut self, content: String, usage: Option<TokenUsage>) {
        self.messages
            .push(Message::new(Role::Assistant, content).with_usage(usage));
    }

    pub fn new(system_prompt: String) -> Self {
        let system_message = if system_prompt.trim().is_empty() {
            None
        } else {
            Some(Message::new(Role::System, system_prompt))
        };
        Self {
            system_message,
            messages: Vec::new(),
        }
    }

    /// Returns a request snapshot without committing the pending user input.
    pub fn request_messages(&self, user_message: &str) -> Vec<Message> {
        self.system_message
            .iter()
            .cloned()
            .chain(self.messages.iter().cloned())
            .chain(std::iter::once(Message::new(
                Role::User,
                user_message.to_owned(),
            )))
            .collect()
    }

    pub fn commit_turn(&mut self, user_message: String, assistant_message: String) {
        self.messages.push(Message::new(Role::User, user_message));
        self.messages
            .push(Message::new(Role::Assistant, assistant_message));
    }

    pub fn clear(&mut self) {
        self.messages.clear();
    }

    pub fn turn_count(&self) -> usize {
        self.messages
            .iter()
            .filter(|message| message.role == Role::Assistant)
            .count()
    }
}
