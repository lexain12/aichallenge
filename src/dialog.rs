use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use thiserror::Error;

use crate::chat::{Message, Role};
use crate::client::TokenUsage;
use crate::context::{ContextState, ContextSummary, UsageTotals};
use crate::facts::{Facts, FactsState};
use crate::invariants::{
    InvariantError, InvariantRepository, InvariantRule, InvariantSet, invariant_id, invariant_text,
};
use crate::memory::{
    MemoryAddress, MemoryError, MemoryRepository, MemorySnapshot, RequestScope, memory_key,
    memory_value,
};
use crate::profile::{
    ProfileError, ProfileRepository, UserProfile, profile_markdown, profile_user_id,
};

pub struct DialogStore {
    pub(crate) connection: Connection,
    config_invariants: Vec<InvariantRule>,
}

pub struct StoredDialog {
    pub id: i64,
    pub scope: RequestScope,
    pub system_prompt: String,
    pub messages: Vec<Message>,
    /// CAS checkpoint from the same snapshot, including hidden protocol rows.
    pub raw_message_count: usize,
    pub context: ContextState,
    pub facts: FactsState,
    pub branch: Option<BranchInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchInfo {
    pub dialog_id: i64,
    pub branch_group_id: i64,
    pub parent_dialog_id: Option<i64>,
    pub checkpoint_message_count: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForkResult {
    pub original_dialog_id: i64,
    pub new_dialog_id: i64,
    pub branch_group_id: i64,
    pub checkpoint_message_count: usize,
}

pub struct DialogSummary {
    pub id: i64,
    pub scope: RequestScope,
    pub title: String,
    pub updated_at: String,
    pub message_count: i64,
}

impl DialogStore {
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let mut connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA synchronous = FULL;
             CREATE TABLE IF NOT EXISTS dialogs (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 system_prompt TEXT NOT NULL,
                 title TEXT NOT NULL,
                 updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
                 last_message_id INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE IF NOT EXISTS dialog_scopes (
                 dialog_id INTEGER PRIMARY KEY REFERENCES dialogs(id),
                 user_id TEXT NOT NULL,
                 task_id TEXT NOT NULL
             );
             INSERT OR IGNORE INTO dialog_scopes (dialog_id, user_id, task_id)
             SELECT id, 'default', 'default' FROM dialogs;
             CREATE TABLE IF NOT EXISTS messages (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 dialog_id INTEGER NOT NULL REFERENCES dialogs(id),
                 role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
                 content TEXT NOT NULL,
                 created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
             );
             CREATE INDEX IF NOT EXISTS messages_by_dialog ON messages(dialog_id, id);
             CREATE TABLE IF NOT EXISTS message_usage (
                 message_id INTEGER PRIMARY KEY REFERENCES messages(id),
                 usage_json TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS dialog_context (
                 dialog_id INTEGER PRIMARY KEY REFERENCES dialogs(id),
                 summary TEXT NOT NULL,
                 covered_message_count INTEGER NOT NULL CHECK (covered_message_count > 0),
                 compaction_count INTEGER NOT NULL DEFAULT 0,
                 known_prompt_tokens INTEGER NOT NULL DEFAULT 0,
                 known_completion_tokens INTEGER NOT NULL DEFAULT 0,
                 known_total_tokens INTEGER NOT NULL DEFAULT 0,
                 missing_usage_count INTEGER NOT NULL DEFAULT 0,
                 updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
             );
             CREATE TABLE IF NOT EXISTS dialog_facts (
                 dialog_id INTEGER PRIMARY KEY REFERENCES dialogs(id),
                 facts_json TEXT NOT NULL,
                 covered_message_count INTEGER NOT NULL CHECK (covered_message_count > 0),
                 update_count INTEGER NOT NULL DEFAULT 0,
                 known_prompt_tokens INTEGER NOT NULL DEFAULT 0,
                 known_completion_tokens INTEGER NOT NULL DEFAULT 0,
                 known_total_tokens INTEGER NOT NULL DEFAULT 0,
                 missing_usage_count INTEGER NOT NULL DEFAULT 0,
                 updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
             );
             CREATE TABLE IF NOT EXISTS dialog_branches (
                 dialog_id INTEGER PRIMARY KEY REFERENCES dialogs(id),
                 branch_group_id INTEGER NOT NULL REFERENCES dialogs(id),
                 parent_dialog_id INTEGER REFERENCES dialogs(id),
                 checkpoint_message_count INTEGER NOT NULL CHECK (checkpoint_message_count >= 0),
                 created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
             );
             CREATE TABLE IF NOT EXISTS memory_entries (
                 scope_type TEXT NOT NULL CHECK (scope_type IN ('user', 'task')),
                 user_id TEXT NOT NULL,
                 task_id TEXT NOT NULL,
                 key TEXT NOT NULL,
                 value TEXT NOT NULL,
                 updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
                 CHECK (
                     (scope_type = 'user' AND task_id = '') OR
                     (scope_type = 'task' AND task_id <> '')
                 ),
                 PRIMARY KEY (scope_type, user_id, task_id, key)
             );
             CREATE TABLE IF NOT EXISTS user_profiles (
                 user_id TEXT PRIMARY KEY,
                 content_markdown TEXT NOT NULL,
                 updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
             );
             CREATE TABLE IF NOT EXISTS invariants (
                 user_id TEXT NOT NULL,
                 task_id TEXT NOT NULL,
                 rule_id TEXT NOT NULL,
                 rule_text TEXT NOT NULL,
                 updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
                 PRIMARY KEY (user_id, task_id, rule_id)
             );",
        )?;
        connection.execute(
            "CREATE INDEX IF NOT EXISTS dialog_branches_by_group
             ON dialog_branches(branch_group_id, dialog_id)",
            [],
        )?;
        crate::workflow_store::migrate(&mut connection)?;
        Ok(Self {
            connection,
            config_invariants: Vec::new(),
        })
    }

    pub(crate) fn set_config_invariants(&mut self, rules: Vec<InvariantRule>) {
        self.config_invariants = rules;
    }

    /// Create a dialog and its first user message in one durable transaction.
    pub fn start_dialog_in_scope(
        &mut self,
        scope: &RequestScope,
        system_prompt: &str,
        prompt: &str,
    ) -> Result<i64, StoreError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let title: String = prompt.chars().take(60).collect();
        tx.execute(
            "INSERT INTO dialogs (system_prompt, title) VALUES (?1, ?2)",
            params![system_prompt, title],
        )?;
        let id = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO dialog_scopes (dialog_id, user_id, task_id) VALUES (?1, ?2, ?3)",
            params![id, scope.user_id(), scope.task_id()],
        )?;
        tx.execute(
            "INSERT INTO messages (dialog_id, role, content) VALUES (?1, 'user', ?2)",
            params![id, prompt],
        )?;
        tx.execute(
            "UPDATE dialogs SET last_message_id = ?1 WHERE id = ?2",
            params![tx.last_insert_rowid(), id],
        )?;
        tx.commit()?;
        Ok(id)
    }

    pub fn start_dialog(&mut self, system_prompt: &str, prompt: &str) -> Result<i64, StoreError> {
        self.start_dialog_in_scope(&RequestScope::default(), system_prompt, prompt)
    }

    /// Raw branch checkpoints include hidden controller protocol rows.
    pub fn raw_message_count(&self, dialog_id: i64) -> Result<usize, StoreError> {
        let count: i64 = self
            .connection
            .query_row(
                "SELECT (SELECT count(*) FROM messages WHERE dialog_id = d.id)
             FROM dialogs d WHERE d.id = ?1",
                [dialog_id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or(StoreError::NotFound(dialog_id))?;
        usize::try_from(count).map_err(|_| StoreError::InvalidBranch("invalid protocol count"))
    }

    /// Reject stale sessions instead of silently mixing independently generated replies.
    pub fn append_message(
        &mut self,
        id: i64,
        expected_count: usize,
        role: Role,
        content: &str,
    ) -> Result<(), StoreError> {
        self.append_with_usage(id, expected_count, role, content, None)
    }

    /// The answer and its provider statistics either both commit or neither does.
    pub fn append_answer(
        &mut self,
        id: i64,
        expected_count: usize,
        content: &str,
        usage: Option<TokenUsage>,
    ) -> Result<(), StoreError> {
        self.append_with_usage(id, expected_count, Role::Assistant, content, usage)
    }

    fn append_with_usage(
        &mut self,
        id: i64,
        expected_count: usize,
        role: Role,
        content: &str,
        usage: Option<TokenUsage>,
    ) -> Result<(), StoreError> {
        let usage_json = usage
            .map(|value| serde_json::to_string(&value))
            .transpose()?;
        let role = match role {
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::System => return Err(StoreError::InvalidRole),
        };
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let count: i64 = tx.query_row(
            "SELECT count(*) FROM messages WHERE dialog_id = ?1",
            [id],
            |row| row.get(0),
        )?;
        if usize::try_from(count).ok() != Some(expected_count) {
            return Err(StoreError::Conflict(id));
        }
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM dialogs WHERE id = ?1)",
            [id],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::NotFound(id));
        }
        tx.execute(
            "INSERT INTO messages (dialog_id, role, content) VALUES (?1, ?2, ?3)",
            params![id, role, content],
        )?;
        let message_id = tx.last_insert_rowid();
        if let Some(usage_json) = usage_json {
            tx.execute(
                "INSERT INTO message_usage (message_id, usage_json) VALUES (?1, ?2)",
                params![message_id, usage_json],
            )?;
        }
        tx.execute("UPDATE dialogs SET last_message_id = ?1, updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = ?2", params![message_id, id])?;
        tx.commit()?;
        Ok(())
    }

    pub fn replace_context(
        &mut self,
        id: i64,
        expected_message_count: usize,
        summary: ContextSummary,
        usage: Option<TokenUsage>,
    ) -> Result<ContextState, StoreError> {
        if summary.covered_message_count() == 0
            || summary.covered_message_count() > expected_message_count
        {
            return Err(StoreError::InvalidContext(
                "summary boundary must cover an existing non-empty prefix",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let count: i64 = tx.query_row(
            "SELECT count(*) FROM messages WHERE dialog_id = ?1",
            [id],
            |row| row.get(0),
        )?;
        if usize::try_from(count).ok() != Some(expected_message_count) {
            return Err(StoreError::Conflict(id));
        }
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM dialogs WHERE id = ?1)",
            [id],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::NotFound(id));
        }

        let stored = tx
            .query_row(
                "SELECT summary, covered_message_count, compaction_count,
                        known_prompt_tokens, known_completion_tokens,
                        known_total_tokens, missing_usage_count
                 FROM dialog_context WHERE dialog_id = ?1",
                [id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?;
        let mut state = decode_context(stored)?;
        state.replace_summary(summary, usage);
        let current = state
            .summary()
            .expect("replacement always installs a summary");
        let totals = state.compaction_usage();
        tx.execute(
            "INSERT INTO dialog_context (
                 dialog_id, summary, covered_message_count, compaction_count,
                 known_prompt_tokens, known_completion_tokens,
                 known_total_tokens, missing_usage_count
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(dialog_id) DO UPDATE SET
                 summary = excluded.summary,
                 covered_message_count = excluded.covered_message_count,
                 compaction_count = excluded.compaction_count,
                 known_prompt_tokens = excluded.known_prompt_tokens,
                 known_completion_tokens = excluded.known_completion_tokens,
                 known_total_tokens = excluded.known_total_tokens,
                 missing_usage_count = excluded.missing_usage_count,
                 updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')",
            params![
                id,
                current.content(),
                to_i64(current.covered_message_count())?,
                to_i64(totals.call_count())?,
                to_i64(totals.prompt_tokens())?,
                to_i64(totals.completion_tokens())?,
                to_i64(totals.total_tokens())?,
                to_i64(totals.missing_usage_count())?,
            ],
        )?;
        tx.commit()?;
        Ok(state)
    }

    pub fn replace_facts(
        &mut self,
        id: i64,
        expected_message_count: usize,
        facts: Facts,
        usage: Option<TokenUsage>,
    ) -> Result<FactsState, StoreError> {
        if expected_message_count == 0 {
            return Err(StoreError::InvalidFacts(
                "facts boundary must cover an existing non-empty prefix",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let count: i64 = tx.query_row(
            "SELECT count(*) FROM messages WHERE dialog_id = ?1",
            [id],
            |row| row.get(0),
        )?;
        if usize::try_from(count).ok() != Some(expected_message_count) {
            return Err(StoreError::Conflict(id));
        }
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM dialogs WHERE id = ?1)",
            [id],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::NotFound(id));
        }
        let stored = tx
            .query_row(
                "SELECT facts_json, covered_message_count, update_count,
                        known_prompt_tokens, known_completion_tokens,
                        known_total_tokens, missing_usage_count
                 FROM dialog_facts WHERE dialog_id = ?1",
                [id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?;
        let visible_message_count = visible_message_count(&tx, id)?;
        let state = decode_facts(stored, visible_message_count)?.updated(
            facts,
            visible_message_count,
            usage,
        );
        let facts_json = serde_json::to_string(state.facts())?;
        let totals = state.update_usage();
        tx.execute(
            "INSERT INTO dialog_facts (
                 dialog_id, facts_json, covered_message_count, update_count,
                 known_prompt_tokens, known_completion_tokens,
                 known_total_tokens, missing_usage_count
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(dialog_id) DO UPDATE SET
                 facts_json = excluded.facts_json,
                 covered_message_count = excluded.covered_message_count,
                 update_count = excluded.update_count,
                 known_prompt_tokens = excluded.known_prompt_tokens,
                 known_completion_tokens = excluded.known_completion_tokens,
                 known_total_tokens = excluded.known_total_tokens,
                 missing_usage_count = excluded.missing_usage_count,
                 updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')",
            params![
                id,
                facts_json,
                to_i64(state.covered_message_count())?,
                to_i64(totals.call_count())?,
                to_i64(totals.prompt_tokens())?,
                to_i64(totals.completion_tokens())?,
                to_i64(totals.total_tokens())?,
                to_i64(totals.missing_usage_count())?,
            ],
        )?;
        tx.commit()?;
        Ok(state)
    }

    pub fn fork_dialog(
        &mut self,
        id: i64,
        expected_message_count: usize,
    ) -> Result<ForkResult, StoreError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let source = tx
            .query_row(
                "SELECT system_prompt, title FROM dialogs WHERE id = ?1",
                [id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .ok_or(StoreError::NotFound(id))?;
        let count: i64 = tx.query_row(
            "SELECT count(*) FROM messages WHERE dialog_id = ?1",
            [id],
            |row| row.get(0),
        )?;
        if usize::try_from(count).ok() != Some(expected_message_count) {
            return Err(StoreError::Conflict(id));
        }
        let existing_group = tx
            .query_row(
                "SELECT branch_group_id FROM dialog_branches WHERE dialog_id = ?1",
                [id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        let branch_group_id = existing_group.unwrap_or(id);
        if existing_group.is_none() {
            require_fork_rows(
                tx.execute(
                    "INSERT INTO dialog_branches (
                     dialog_id, branch_group_id, parent_dialog_id, checkpoint_message_count
                 ) VALUES (?1, ?1, NULL, ?2)",
                    params![id, to_i64(expected_message_count)?],
                )?,
                1,
                id,
            )?;
        }

        require_fork_rows(
            tx.execute(
                "INSERT INTO dialogs (system_prompt, title) VALUES (?1, ?2)",
                params![source.0, format!("{} (branch)", source.1)],
            )?,
            1,
            id,
        )?;
        let new_dialog_id = tx.last_insert_rowid();
        let copied_scope = tx.execute(
            "INSERT INTO dialog_scopes (dialog_id, user_id, task_id)
             SELECT ?1, user_id, task_id
             FROM dialog_scopes
             WHERE dialog_id = ?2",
            params![new_dialog_id, id],
        )?;
        if copied_scope != 1 {
            return Err(StoreError::InvalidDialogScope(
                "source dialog scope is missing",
            ));
        }
        let source_messages = {
            let mut statement = tx.prepare(
                "SELECT m.role, m.content, m.created_at, u.usage_json, m.id
                 FROM messages m
                 LEFT JOIN message_usage u ON u.message_id = m.id
                 WHERE m.dialog_id = ?1 ORDER BY m.id",
            )?;
            statement
                .query_map([id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut last_message_id = 0;
        let mut message_id_map = BTreeMap::new();
        for (role, content, created_at, usage_json, old_message_id) in source_messages {
            require_fork_rows(
                tx.execute(
                    "INSERT INTO messages (dialog_id, role, content, created_at)
                 VALUES (?1, ?2, ?3, ?4)",
                    params![new_dialog_id, role, content, created_at],
                )?,
                1,
                id,
            )?;
            last_message_id = tx.last_insert_rowid();
            message_id_map.insert(old_message_id, last_message_id);
            if let Some(usage_json) = usage_json {
                require_fork_rows(
                    tx.execute(
                        "INSERT INTO message_usage (message_id, usage_json) VALUES (?1, ?2)",
                        params![last_message_id, usage_json],
                    )?,
                    1,
                    id,
                )?;
            }
        }
        crate::workflow_store::copy_workflow_branch(&tx, id, new_dialog_id, &message_id_map)?;
        require_fork_rows(
            tx.execute(
                "UPDATE dialogs SET last_message_id = ?1 WHERE id = ?2",
                params![last_message_id, new_dialog_id],
            )?,
            1,
            id,
        )?;
        let expected_context: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM dialog_context WHERE dialog_id=?1)",
            [id],
            |row| row.get(0),
        )?;
        require_fork_rows(
            tx.execute(
                "INSERT INTO dialog_context (
                 dialog_id, summary, covered_message_count, compaction_count,
                 known_prompt_tokens, known_completion_tokens,
                 known_total_tokens, missing_usage_count
             )
             SELECT ?1, summary, covered_message_count, compaction_count,
                    known_prompt_tokens, known_completion_tokens,
                    known_total_tokens, missing_usage_count
             FROM dialog_context WHERE dialog_id = ?2",
                params![new_dialog_id, id],
            )?,
            usize::from(expected_context),
            id,
        )?;
        let expected_facts: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM dialog_facts WHERE dialog_id=?1)",
            [id],
            |row| row.get(0),
        )?;
        require_fork_rows(
            tx.execute(
                "INSERT INTO dialog_facts (
                 dialog_id, facts_json, covered_message_count, update_count,
                 known_prompt_tokens, known_completion_tokens,
                 known_total_tokens, missing_usage_count
             )
             SELECT ?1, facts_json, covered_message_count, update_count,
                    known_prompt_tokens, known_completion_tokens,
                    known_total_tokens, missing_usage_count
             FROM dialog_facts WHERE dialog_id = ?2",
                params![new_dialog_id, id],
            )?,
            usize::from(expected_facts),
            id,
        )?;
        require_fork_rows(
            tx.execute(
                "INSERT INTO dialog_branches (
                 dialog_id, branch_group_id, parent_dialog_id, checkpoint_message_count
             ) VALUES (?1, ?2, ?3, ?4)",
                params![
                    new_dialog_id,
                    branch_group_id,
                    id,
                    to_i64(expected_message_count)?,
                ],
            )?,
            1,
            id,
        )?;
        let copied_count: i64 = tx.query_row(
            "SELECT count(*) FROM messages WHERE dialog_id=?1",
            [new_dialog_id],
            |row| row.get(0),
        )?;
        if copied_count != count {
            return Err(StoreError::Conflict(id));
        }
        for (old, new) in &message_id_map {
            let matches: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM messages old JOIN messages new
                 LEFT JOIN message_usage old_usage ON old_usage.message_id=old.id
                 LEFT JOIN message_usage new_usage ON new_usage.message_id=new.id
                 WHERE old.id=?1 AND new.id=?2 AND old.dialog_id=?3 AND new.dialog_id=?4
                   AND old.role=new.role AND old.content=new.content AND old.created_at=new.created_at
                   AND old_usage.usage_json IS new_usage.usage_json)",
                params![old,new,id,new_dialog_id], |row| row.get(0),
            )?;
            if !matches {
                return Err(StoreError::Conflict(id));
            }
        }
        let invalid_mapping: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM message_task_stages ms
             JOIN messages m ON m.id=ms.message_id
             JOIN workflow_tasks t ON t.id=ms.workflow_task_id
             JOIN task_stage_runs s ON s.id=ms.stage_run_id
             WHERE (m.dialog_id=?1 OR t.dialog_id=?1)
               AND (m.dialog_id<>t.dialog_id OR s.workflow_task_id<>t.id))",
            [new_dialog_id],
            |row| row.get(0),
        )?;
        if invalid_mapping {
            return Err(StoreError::Conflict(id));
        }
        tx.commit()?;
        Ok(ForkResult {
            original_dialog_id: id,
            new_dialog_id,
            branch_group_id,
            checkpoint_message_count: expected_message_count,
        })
    }

    pub fn load_branch_member(
        &self,
        current_id: i64,
        target_id: i64,
    ) -> Result<StoredDialog, StoreError> {
        let current = self.load(current_id)?;
        let current_branch = current
            .branch
            .ok_or(StoreError::NoBranchGroup(current_id))?;
        let target = self.load(target_id)?;
        let same_group = target
            .branch
            .as_ref()
            .is_some_and(|branch| branch.branch_group_id == current_branch.branch_group_id);
        if !same_group {
            return Err(StoreError::UnrelatedBranch {
                current: current_id,
                target: target_id,
            });
        }
        Ok(target)
    }

    pub fn load(&self, id: i64) -> Result<StoredDialog, StoreError> {
        // Read metadata and messages from a single SQLite snapshot.
        let tx = self.connection.unchecked_transaction()?;
        let system_prompt = tx
            .query_row(
                "SELECT system_prompt FROM dialogs WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or(StoreError::NotFound(id))?;
        let stored_scope = tx
            .query_row(
                "SELECT user_id, task_id FROM dialog_scopes WHERE dialog_id = ?1",
                [id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .ok_or(StoreError::InvalidDialogScope("dialog scope is missing"))?;
        let scope = RequestScope::new(stored_scope.0, stored_scope.1)?;
        let messages = {
            let mut statement = tx.prepare(
                "SELECT m.role, m.content, u.usage_json FROM messages m
                 LEFT JOIN message_usage u ON u.message_id = m.id
                 LEFT JOIN workflow_inputs i ON i.message_id = m.id
                 WHERE m.dialog_id = ?1
                   AND (i.source IS NULL OR i.source = 'human' OR m.role = 'assistant')
                 ORDER BY m.id",
            )?;
            statement
                .query_map([id], |row| {
                    let role: String = row.get(0)?;
                    let role = match role.as_str() {
                        "user" => Role::User,
                        "assistant" => Role::Assistant,
                        _ => return Err(rusqlite::Error::InvalidQuery),
                    };
                    let usage_json: Option<String> = row.get(2)?;
                    let usage = usage_json
                        .map(|value| serde_json::from_str(&value))
                        .transpose()
                        .map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Text,
                                Box::new(error),
                            )
                        })?;
                    Ok(Message::new(role, row.get(1)?).with_usage(usage))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let stored_context = tx
            .query_row(
                "SELECT summary, covered_message_count, compaction_count,
                        known_prompt_tokens, known_completion_tokens,
                        known_total_tokens, missing_usage_count
                 FROM dialog_context WHERE dialog_id = ?1",
                [id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?;
        let context = decode_context(stored_context)?;
        let stored_facts = tx
            .query_row(
                "SELECT facts_json, covered_message_count, update_count,
                        known_prompt_tokens, known_completion_tokens,
                        known_total_tokens, missing_usage_count
                 FROM dialog_facts WHERE dialog_id = ?1",
                [id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?;
        let facts = decode_facts(stored_facts, messages.len())?;
        let stored_branch = tx
            .query_row(
                "SELECT branch_group_id, parent_dialog_id, checkpoint_message_count
                 FROM dialog_branches WHERE dialog_id = ?1",
                [id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()?;
        // Fork checkpoints include hidden protocol messages, unlike transcript replay.
        let protocol_count: i64 = tx.query_row(
            "SELECT count(*) FROM messages WHERE dialog_id = ?1",
            [id],
            |row| row.get(0),
        )?;
        let raw_message_count = usize::try_from(protocol_count)
            .map_err(|_| StoreError::InvalidBranch("invalid protocol count"))?;
        let branch = decode_branch(id, stored_branch, raw_message_count)?;
        tx.commit()?;
        Ok(StoredDialog {
            id,
            scope,
            system_prompt,
            messages,
            raw_message_count,
            context,
            facts,
            branch,
        })
    }

    pub fn latest_id(&self) -> Result<Option<i64>, StoreError> {
        Ok(self
            .connection
            .query_row(
                "SELECT id FROM dialogs ORDER BY last_message_id DESC, id DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub fn list(&self) -> Result<Vec<DialogSummary>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT d.id, s.user_id, s.task_id, d.title, d.updated_at,
                    (SELECT count(*) FROM messages m WHERE m.dialog_id = d.id)
             FROM dialogs d
             JOIN dialog_scopes s ON s.dialog_id = d.id
             ORDER BY d.last_message_id DESC, d.id DESC",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })?;
        let mut dialogs = Vec::new();
        for row in rows {
            let (id, user_id, task_id, title, updated_at, message_count) = row?;
            dialogs.push(DialogSummary {
                id,
                scope: RequestScope::new(user_id, task_id)?,
                title,
                updated_at,
                message_count,
            });
        }
        Ok(dialogs)
    }
}

pub(crate) fn visible_message_count(connection: &Connection, id: i64) -> Result<usize, StoreError> {
    let count: i64 = connection.query_row(
        "SELECT count(*) FROM messages m LEFT JOIN workflow_inputs i ON i.message_id=m.id
         WHERE m.dialog_id=?1 AND (i.source IS NULL OR i.source='human' OR m.role='assistant')",
        [id],
        |row| row.get(0),
    )?;
    usize::try_from(count).map_err(|_| StoreError::InvalidContext("invalid visible message count"))
}

impl MemoryRepository for DialogStore {
    type Error = StoreError;

    fn load_memory(&self, scope: &RequestScope) -> Result<MemorySnapshot, Self::Error> {
        let mut statement = self.connection.prepare(
            "SELECT scope_type, key, value
             FROM memory_entries
             WHERE user_id = ?1
               AND (
                   (scope_type = 'user' AND task_id = '') OR
                   (scope_type = 'task' AND task_id = ?2)
               )
             ORDER BY scope_type, key",
        )?;
        let rows = statement.query_map(params![scope.user_id(), scope.task_id()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut user = BTreeMap::new();
        let mut task = BTreeMap::new();
        for row in rows {
            let (scope_type, key, value) = row?;
            match scope_type.as_str() {
                "user" => {
                    user.insert(key, value);
                }
                "task" => {
                    task.insert(key, value);
                }
                _ => return Err(rusqlite::Error::InvalidQuery.into()),
            }
        }
        Ok(MemorySnapshot::new(scope.clone(), user, task))
    }

    fn upsert_memory(
        &mut self,
        address: &MemoryAddress,
        key: &str,
        value: &str,
    ) -> Result<(), Self::Error> {
        let key = memory_key(key)?;
        let value = memory_value(value)?;
        let (scope_type, user_id, task_id) = address_parts(address)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO memory_entries (scope_type, user_id, task_id, key, value)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(scope_type, user_id, task_id, key) DO UPDATE SET
                 value = excluded.value,
                 updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')",
            params![scope_type, user_id, task_id, key, value],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn delete_memory(&mut self, address: &MemoryAddress, key: &str) -> Result<bool, Self::Error> {
        let key = memory_key(key)?;
        let (scope_type, user_id, task_id) = address_parts(address)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let deleted = tx.execute(
            "DELETE FROM memory_entries
             WHERE scope_type = ?1 AND user_id = ?2 AND task_id = ?3 AND key = ?4",
            params![scope_type, user_id, task_id, key],
        )?;
        tx.commit()?;
        Ok(deleted > 0)
    }
}

impl ProfileRepository for DialogStore {
    type Error = StoreError;

    fn load_profile(&self, user_id: &str) -> Result<Option<UserProfile>, Self::Error> {
        let user_id = profile_user_id(user_id)?;
        let row = self
            .connection
            .query_row(
                "SELECT content_markdown, updated_at
                 FROM user_profiles
                 WHERE user_id = ?1",
                params![user_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        row.map(|(markdown, updated_at)| UserProfile::restored(user_id, markdown, updated_at))
            .transpose()
            .map_err(StoreError::from)
    }

    fn replace_profile(&mut self, user_id: &str, markdown: &str) -> Result<(), Self::Error> {
        let user_id = profile_user_id(user_id)?;
        let markdown = profile_markdown(markdown)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO user_profiles (user_id, content_markdown)
             VALUES (?1, ?2)
             ON CONFLICT(user_id) DO UPDATE SET
                 content_markdown = excluded.content_markdown,
                 updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')",
            params![user_id, markdown],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn delete_profile(&mut self, user_id: &str) -> Result<bool, Self::Error> {
        let user_id = profile_user_id(user_id)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let deleted = tx.execute(
            "DELETE FROM user_profiles WHERE user_id = ?1",
            params![user_id],
        )?;
        tx.commit()?;
        Ok(deleted > 0)
    }
}

impl InvariantRepository for DialogStore {
    type Error = StoreError;

    fn load_invariants(&self, scope: &RequestScope) -> Result<InvariantSet, Self::Error> {
        let mut statement = self.connection.prepare(
            "SELECT rule_id, rule_text FROM invariants
             WHERE user_id = ?1 AND task_id = ?2 ORDER BY rule_id",
        )?;
        let rows = statement.query_map(params![scope.user_id(), scope.task_id()], |row| {
            Ok(InvariantRule {
                id: row.get(0)?,
                text: row.get(1)?,
            })
        })?;
        let mut rules = std::collections::BTreeMap::new();
        for rule in &self.config_invariants {
            rules.insert(rule.id.clone(), rule.clone());
        }
        for row in rows {
            let rule = row?;
            invariant_id(&rule.id)?;
            invariant_text(&rule.text)?;
            rules.entry(rule.id.clone()).or_insert(rule);
        }
        Ok(InvariantSet::new(
            scope.clone(),
            rules.into_values().collect(),
        ))
    }

    fn upsert_invariant(
        &mut self,
        scope: &RequestScope,
        id: &str,
        text: &str,
    ) -> Result<(), Self::Error> {
        let id = invariant_id(id)?;
        let text = invariant_text(text)?;
        if self.config_invariants.iter().any(|rule| rule.id == id) {
            return Err(StoreError::ConfiguredInvariant(id.to_owned()));
        }
        self.connection.execute(
            "INSERT INTO invariants (user_id, task_id, rule_id, rule_text)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(user_id, task_id, rule_id) DO UPDATE SET
                rule_text = excluded.rule_text,
                updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')",
            params![scope.user_id(), scope.task_id(), id, text],
        )?;
        Ok(())
    }

    fn delete_invariant(&mut self, scope: &RequestScope, id: &str) -> Result<bool, Self::Error> {
        let id = invariant_id(id)?;
        if self.config_invariants.iter().any(|rule| rule.id == id) {
            return Err(StoreError::ConfiguredInvariant(id.to_owned()));
        }
        Ok(self.connection.execute(
            "DELETE FROM invariants WHERE user_id = ?1 AND task_id = ?2 AND rule_id = ?3",
            params![scope.user_id(), scope.task_id(), id],
        )? > 0)
    }
}

fn address_parts(address: &MemoryAddress) -> Result<(&'static str, &str, &str), MemoryError> {
    let user_id = address.user_id().trim();
    if user_id.is_empty() {
        return Err(MemoryError::BlankUserId);
    }
    match address {
        MemoryAddress::User { .. } => Ok(("user", user_id, "")),
        MemoryAddress::Task { task_id, .. } => {
            let task_id = task_id.trim();
            if task_id.is_empty() {
                return Err(MemoryError::BlankTaskId);
            }
            Ok(("task", user_id, task_id))
        }
    }
}

type StoredContextRow = (String, i64, i64, i64, i64, i64, i64);
type StoredFactsRow = (String, i64, i64, i64, i64, i64, i64);

fn decode_context(row: Option<StoredContextRow>) -> Result<ContextState, StoreError> {
    let Some((summary, covered, calls, prompt, completion, total, missing)) = row else {
        return Ok(ContextState::default());
    };
    let covered = usize::try_from(covered)
        .map_err(|_| StoreError::InvalidContext("negative summary boundary"))?;
    let values = [calls, prompt, completion, total, missing];
    if values.iter().any(|value| *value < 0) {
        return Err(StoreError::InvalidContext("negative context metric"));
    }
    Ok(ContextState::restored(
        Some(ContextSummary::new(summary, covered)),
        UsageTotals::from_parts(
            calls as u64,
            prompt as u64,
            completion as u64,
            total as u64,
            missing as u64,
        ),
    ))
}

fn decode_facts(
    row: Option<StoredFactsRow>,
    message_count: usize,
) -> Result<FactsState, StoreError> {
    let Some((facts_json, covered, calls, prompt, completion, total, missing)) = row else {
        return Ok(FactsState::default());
    };
    let facts: Facts = serde_json::from_str(&facts_json)
        .map_err(|_| StoreError::InvalidFacts("facts_json must be a string-to-string object"))?;
    let covered = usize::try_from(covered)
        .map_err(|_| StoreError::InvalidFacts("negative facts boundary"))?;
    if covered == 0 || covered > message_count {
        return Err(StoreError::InvalidFacts(
            "facts boundary exceeds dialog history",
        ));
    }
    let values = [calls, prompt, completion, total, missing];
    if values.iter().any(|value| *value < 0) {
        return Err(StoreError::InvalidFacts("negative facts metric"));
    }
    Ok(FactsState::restored(
        facts,
        covered,
        UsageTotals::from_parts(
            calls as u64,
            prompt as u64,
            completion as u64,
            total as u64,
            missing as u64,
        ),
    ))
}

fn require_fork_rows(
    actual: usize,
    expected: usize,
    source_dialog_id: i64,
) -> Result<(), StoreError> {
    if actual != expected {
        return Err(StoreError::Conflict(source_dialog_id));
    }
    Ok(())
}

fn decode_branch(
    dialog_id: i64,
    row: Option<(i64, Option<i64>, i64)>,
    message_count: usize,
) -> Result<Option<BranchInfo>, StoreError> {
    let Some((branch_group_id, parent_dialog_id, checkpoint)) = row else {
        return Ok(None);
    };
    if branch_group_id <= 0 || parent_dialog_id.is_some_and(|parent| parent <= 0) {
        return Err(StoreError::InvalidBranch("invalid dialog identifier"));
    }
    let checkpoint_message_count = usize::try_from(checkpoint)
        .map_err(|_| StoreError::InvalidBranch("negative checkpoint boundary"))?;
    if checkpoint_message_count > message_count {
        return Err(StoreError::InvalidBranch(
            "checkpoint boundary exceeds dialog history",
        ));
    }
    Ok(Some(BranchInfo {
        dialog_id,
        branch_group_id,
        parent_dialog_id,
        checkpoint_message_count,
    }))
}

fn to_i64(value: impl TryInto<i64>) -> Result<i64, StoreError> {
    value
        .try_into()
        .map_err(|_| StoreError::InvalidContext("context metric exceeds SQLite INTEGER"))
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("workflow in dialog {0} changed in another session")]
    WorkflowConflict(i64),
    #[error("invalid workflow state: {0}")]
    InvalidWorkflow(String),
    #[error("invalid token statistics: {0}")]
    Usage(#[from] serde_json::Error),
    #[error("dialog database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("invalid durable memory: {0}")]
    InvalidMemory(#[from] MemoryError),
    #[error("invalid user profile: {0}")]
    InvalidProfile(#[from] ProfileError),
    #[error("invalid invariant: {0}")]
    InvalidInvariant(#[from] InvariantError),
    #[error("invariant {0} is defined in config and cannot be changed here")]
    ConfiguredInvariant(String),
    #[error("dialog {0} was not found")]
    NotFound(i64),
    #[error("dialog {0} changed in another session; restart with --resume {0}")]
    Conflict(i64),
    #[error("system instructions belong to the dialog, not its message list")]
    InvalidRole,
    #[error("invalid dialog context: {0}")]
    InvalidContext(&'static str),
    #[error("invalid dialog facts: {0}")]
    InvalidFacts(&'static str),
    #[error("dialog {0} has no branch group; create a branch first")]
    NoBranchGroup(i64),
    #[error("dialog {target} is not in the branch group of dialog {current}")]
    UnrelatedBranch { current: i64, target: i64 },
    #[error("invalid dialog branch metadata: {0}")]
    InvalidBranch(&'static str),
    #[error("invalid dialog scope: {0}")]
    InvalidDialogScope(&'static str),
}
