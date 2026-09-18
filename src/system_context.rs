use crate::chat::{Message, Role};
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextScope {
    Application,
    User,
    Task,
    Conversation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionPolicy {
    Include,
    Exclude,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SystemBlockMetadata {
    pub name: String,
    pub scope: ContextScope,
    pub compaction: CompactionPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemBlock {
    name: String,
    content: String,
    scope: ContextScope,
    compaction: CompactionPolicy,
}

impl SystemBlock {
    pub fn new(
        name: impl Into<String>,
        content: impl Into<String>,
        scope: ContextScope,
        compaction: CompactionPolicy,
    ) -> Self {
        Self {
            name: name.into(),
            content: content.into(),
            scope,
            compaction,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn content(&self) -> &str {
        &self.content
    }

    pub fn scope(&self) -> ContextScope {
        self.scope
    }

    pub fn compaction(&self) -> CompactionPolicy {
        self.compaction
    }

    pub fn metadata(&self) -> SystemBlockMetadata {
        SystemBlockMetadata {
            name: self.name.clone(),
            scope: self.scope,
            compaction: self.compaction,
        }
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

    pub fn prompt_blocks(&self) -> Vec<&SystemBlock> {
        let mut blocks: Vec<_> = self.blocks.iter().collect();
        blocks.sort_by_key(|block| block.scope());
        blocks
    }

    pub fn compaction_blocks(&self) -> Vec<&SystemBlock> {
        self.prompt_blocks()
            .into_iter()
            .filter(|block| block.compaction() == CompactionPolicy::Include)
            .collect()
    }

    pub fn metadata(&self) -> Vec<SystemBlockMetadata> {
        self.prompt_blocks()
            .into_iter()
            .map(SystemBlock::metadata)
            .collect()
    }

    pub fn to_messages(&self) -> Vec<Message> {
        self.prompt_blocks()
            .into_iter()
            .map(|block| Message::new(Role::System, block.content().to_owned()))
            .collect()
    }
}
