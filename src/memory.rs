use std::collections::BTreeMap;

use thiserror::Error;

use crate::system_context::{CompactionPolicy, ContextScope, SystemBlock};

pub const DEFAULT_USER_ID: &str = "default";
pub const DEFAULT_TASK_ID: &str = "default";

const USER_MEMORY_INTRODUCTION: &str = "Long-term user memory. Use it as background context. Active task memory is more specific, and a current explicit user request overrides conflicting memory:";
const TASK_MEMORY_INTRODUCTION: &str = "Working memory for the active task. It is more specific than user memory, and a current explicit user request overrides conflicting memory:";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableMemoryScope {
    User,
    Task,
}

impl DurableMemoryScope {
    pub fn label(self) -> &'static str {
        match self {
            Self::User => "Long-term",
            Self::Task => "Working",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestScope {
    user_id: String,
    task_id: String,
    dialog_id: Option<i64>,
}

impl RequestScope {
    pub fn new(
        user_id: impl Into<String>,
        task_id: impl Into<String>,
    ) -> Result<Self, MemoryError> {
        let user_id = user_id.into().trim().to_owned();
        if user_id.is_empty() {
            return Err(MemoryError::BlankUserId);
        }
        let task_id = task_id.into().trim().to_owned();
        if task_id.is_empty() {
            return Err(MemoryError::BlankTaskId);
        }
        Ok(Self {
            user_id,
            task_id,
            dialog_id: None,
        })
    }

    pub fn user_id(&self) -> &str {
        &self.user_id
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    pub fn dialog_id(&self) -> Option<i64> {
        self.dialog_id
    }

    pub fn with_dialog_id(&self, dialog_id: Option<i64>) -> Self {
        let mut scope = self.clone();
        scope.dialog_id = dialog_id;
        scope
    }

    pub fn address(&self, scope: DurableMemoryScope) -> MemoryAddress {
        match scope {
            DurableMemoryScope::User => MemoryAddress::User {
                user_id: self.user_id.clone(),
            },
            DurableMemoryScope::Task => MemoryAddress::Task {
                user_id: self.user_id.clone(),
                task_id: self.task_id.clone(),
            },
        }
    }
}

impl Default for RequestScope {
    fn default() -> Self {
        RequestScope::new(DEFAULT_USER_ID, DEFAULT_TASK_ID).expect("default memory scope is valid")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MemoryAddress {
    User { user_id: String },
    Task { user_id: String, task_id: String },
}

impl MemoryAddress {
    pub fn user_id(&self) -> &str {
        match self {
            Self::User { user_id } | Self::Task { user_id, .. } => user_id,
        }
    }

    pub fn task_id(&self) -> Option<&str> {
        match self {
            Self::User { .. } => None,
            Self::Task { task_id, .. } => Some(task_id),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryEntry {
    pub key: String,
    pub value: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemorySnapshot {
    scope: RequestScope,
    user: BTreeMap<String, String>,
    task: BTreeMap<String, String>,
}

impl MemorySnapshot {
    pub fn new(
        scope: RequestScope,
        user: BTreeMap<String, String>,
        task: BTreeMap<String, String>,
    ) -> Self {
        Self { scope, user, task }
    }

    pub fn user_entries(&self) -> &BTreeMap<String, String> {
        &self.user
    }

    pub fn task_entries(&self) -> &BTreeMap<String, String> {
        &self.task
    }
}

pub trait ContextProvider {
    fn blocks(&self, scope: &RequestScope) -> Result<Vec<SystemBlock>, ContextError>;
}

impl ContextProvider for MemorySnapshot {
    fn blocks(&self, scope: &RequestScope) -> Result<Vec<SystemBlock>, ContextError> {
        if self.scope != *scope {
            return Err(ContextError::ScopeMismatch);
        }

        let mut blocks = Vec::with_capacity(2);
        if !self.user.is_empty() {
            let entries = serde_json::to_string(&self.user)
                .map_err(|error| ContextError::Serialization(error.to_string()))?;
            blocks.push(SystemBlock::new(
                "user_memory",
                format!("{USER_MEMORY_INTRODUCTION}\n{entries}"),
                ContextScope::User,
                CompactionPolicy::Exclude,
            ));
        }
        if !self.task.is_empty() {
            let entries = serde_json::to_string(&self.task)
                .map_err(|error| ContextError::Serialization(error.to_string()))?;
            blocks.push(SystemBlock::new(
                "task_memory",
                format!("{TASK_MEMORY_INTRODUCTION}\n{entries}"),
                ContextScope::Task,
                CompactionPolicy::Exclude,
            ));
        }
        Ok(blocks)
    }
}

/// Repository mutations trim address identifiers and reject blank identifiers,
/// matching `RequestScope::new` even for directly constructed `MemoryAddress` variants.
pub trait MemoryRepository {
    type Error;

    fn load_memory(&self, scope: &RequestScope) -> Result<MemorySnapshot, Self::Error>;
    fn upsert_memory(
        &mut self,
        address: &MemoryAddress,
        key: &str,
        value: &str,
    ) -> Result<(), Self::Error>;
    fn delete_memory(&mut self, address: &MemoryAddress, key: &str) -> Result<bool, Self::Error>;
}

pub(crate) fn memory_key(key: &str) -> Result<&str, MemoryError> {
    let key = key.trim();
    if key.is_empty() {
        return Err(MemoryError::BlankKey);
    }
    Ok(key)
}

pub(crate) fn memory_value(value: &str) -> Result<&str, MemoryError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(MemoryError::BlankValue);
    }
    Ok(value)
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum MemoryError {
    #[error("user_id must not be blank")]
    BlankUserId,
    #[error("task_id must not be blank")]
    BlankTaskId,
    #[error("memory key must not be blank")]
    BlankKey,
    #[error("memory value must not be blank")]
    BlankValue,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ContextError {
    #[error("memory snapshot does not match the active request scope")]
    ScopeMismatch,
    #[error("user profile does not match the active request user")]
    ProfileScopeMismatch,
    #[error("failed to serialize memory context: {0}")]
    Serialization(String),
}
