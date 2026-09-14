use crate::chat::{Message, Role};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemBlock {
    name: String,
    content: String,
}

impl SystemBlock {
    pub fn new(name: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            content: content.into(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn content(&self) -> &str {
        &self.content
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SystemContext {
    blocks: Vec<SystemBlock>,
}

impl SystemContext {
    pub fn push(&mut self, block: SystemBlock) {
        if !block.content().trim().is_empty() {
            self.blocks.push(block);
        }
    }

    pub fn blocks(&self) -> &[SystemBlock] {
        &self.blocks
    }

    pub fn to_messages(&self) -> Vec<Message> {
        self.blocks
            .iter()
            .map(|block| Message::new(Role::System, block.content().to_owned()))
            .collect()
    }
}
