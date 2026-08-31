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
}

impl Message {
    fn new(role: Role, content: String) -> Self {
        Self { role, content }
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
    Send(String),
}

pub fn parse_input(input: &str) -> InputAction {
    let input = input.trim();
    match input {
        "" => InputAction::Ignore,
        "/exit" | "/quit" => InputAction::Exit,
        "/clear" => InputAction::Clear,
        message => InputAction::Send(message.to_owned()),
    }
}

/// Committed turns from the current process only.
pub struct ChatHistory {
    system_message: Option<Message>,
    messages: Vec<Message>,
}

impl ChatHistory {
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
        self.messages.len() / 2
    }
}
