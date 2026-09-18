use thiserror::Error;

use crate::memory::{ContextError, ContextProvider, DEFAULT_TASK_ID, RequestScope};
use crate::system_context::{CompactionPolicy, ContextScope, SystemBlock};

pub const PROFILE_INTRODUCTION: &str = "User profile preferences. Apply them when relevant. They are soft defaults, not hard constraints. A current explicit request and task-specific context override conflicting profile preferences.";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserProfile {
    user_id: String,
    content_markdown: String,
    updated_at: String,
}

impl UserProfile {
    pub fn restored(
        user_id: impl Into<String>,
        markdown: impl Into<String>,
        updated_at: impl Into<String>,
    ) -> Result<Self, ProfileError> {
        let scope =
            RequestScope::new(user_id, DEFAULT_TASK_ID).map_err(|_| ProfileError::BlankUserId)?;
        let content_markdown = profile_markdown(&markdown.into())?.to_owned();
        Ok(Self {
            user_id: scope.user_id().to_owned(),
            content_markdown,
            updated_at: updated_at.into(),
        })
    }

    pub fn user_id(&self) -> &str {
        &self.user_id
    }

    pub fn content_markdown(&self) -> &str {
        &self.content_markdown
    }

    pub fn updated_at(&self) -> &str {
        &self.updated_at
    }
}

impl ContextProvider for UserProfile {
    fn blocks(&self, scope: &RequestScope) -> Result<Vec<SystemBlock>, ContextError> {
        if self.user_id != scope.user_id() {
            return Err(ContextError::ProfileScopeMismatch);
        }
        Ok(vec![SystemBlock::new(
            "user_profile",
            format!("{PROFILE_INTRODUCTION}\n\n{}", self.content_markdown),
            ContextScope::User,
            CompactionPolicy::Exclude,
        )])
    }
}

pub trait ProfileRepository {
    type Error;

    fn load_profile(&self, user_id: &str) -> Result<Option<UserProfile>, Self::Error>;
    fn replace_profile(&mut self, user_id: &str, markdown: &str) -> Result<(), Self::Error>;
    fn delete_profile(&mut self, user_id: &str) -> Result<bool, Self::Error>;
}

pub fn profile_markdown(markdown: &str) -> Result<&str, ProfileError> {
    let markdown = markdown.trim();
    if markdown.is_empty() {
        return Err(ProfileError::BlankContent);
    }
    Ok(markdown)
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ProfileError {
    #[error("user_id must not be blank")]
    BlankUserId,
    #[error("profile content must not be blank")]
    BlankContent,
}
