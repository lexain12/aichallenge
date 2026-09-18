use std::io;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use deepseek_cli::agent::{Agent, AgentError, AgentEvent};
use deepseek_cli::chat::{InputAction, Role, parse_input};
use deepseek_cli::client::ClientError;
use deepseek_cli::config::{Config, ConfigError};
use deepseek_cli::dialog::{DialogStore, StoreError};
use deepseek_cli::memory::{DEFAULT_TASK_ID, DEFAULT_USER_ID, DurableMemoryScope, RequestScope};
use deepseek_cli::terminal::{BlockStyle, TerminalUi};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Debug, Parser)]
#[command(version, about)]
struct Args {
    /// Path to the TOML configuration file.
    #[arg(long, default_value = "deepseek.toml")]
    config: PathBuf,
    /// SQLite database containing saved dialogs.
    #[arg(long, default_value = "dialogs.sqlite3")]
    db: PathBuf,
    /// Continue the dialog with the most recent saved message.
    #[arg(long, conflicts_with_all = ["resume", "list_dialogs"])]
    resume_last: bool,
    /// Continue a saved dialog by ID.
    #[arg(long, value_parser = clap::value_parser!(i64).range(1..), conflicts_with = "list_dialogs")]
    resume: Option<i64>,
    /// List saved dialogs without calling the API or loading its configuration.
    #[arg(long)]
    list_dialogs: bool,
    /// Long-term memory owner for a new dialog; must match a resumed dialog.
    #[arg(long, value_parser = parse_non_blank_id)]
    user: Option<String>,
    /// Working-memory task for a new dialog; must match a resumed dialog.
    #[arg(long, value_parser = parse_non_blank_id)]
    task: Option<String>,
}

fn parse_non_blank_id(value: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        Err("identifier must not be blank".to_owned())
    } else {
        Ok(value.to_owned())
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let ui = TerminalUi::stderr();
            let mut stderr = io::stderr();
            if ui
                .write_block(&mut stderr, BlockStyle::Error, &format!("error: {error}"))
                .is_err()
            {
                eprintln!("error: {error}");
            }
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), AppError> {
    let args = Args::parse();
    let store = DialogStore::open(&args.db)?;
    let mut stdout = io::stdout();
    let stdout_ui = TerminalUi::stdout();
    if args.list_dialogs {
        let dialogs = store.list()?;
        if dialogs.is_empty() {
            stdout_ui.write_block(&mut stdout, BlockStyle::System, "No saved dialogs.")?;
        } else {
            stdout_ui.write_block(
                &mut stdout,
                BlockStyle::System,
                "ID | User | Task | Updated (UTC) | Messages | First message",
            )?;
            for dialog in dialogs {
                let title = dialog
                    .title
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ");
                stdout_ui.write_block(
                    &mut stdout,
                    BlockStyle::System,
                    &format!(
                        "{} | {} | {} | {} | {} | {}",
                        dialog.id,
                        dialog.scope.user_id(),
                        dialog.scope.task_id(),
                        dialog.updated_at,
                        dialog.message_count,
                        title
                    ),
                )?;
            }
        }
        return Ok(());
    }
    let resume = if args.resume_last {
        Some(store.latest_id()?.ok_or(AppError::NoDialogs)?)
    } else {
        args.resume
    };
    let env_api_key = std::env::var("DEEPSEEK_API_KEY").ok();
    let config = Config::load(&args.config, env_api_key)?;
    let mut agent = match resume {
        Some(id) => {
            let agent = Agent::from_dialog(&config, store, id)?;
            if let Some(user) = args.user.as_deref()
                && user != agent.scope().user_id()
            {
                return Err(AppError::UserScopeMismatch {
                    stored: agent.scope().user_id().to_owned(),
                    requested: user.to_owned(),
                });
            }
            if let Some(task) = args.task.as_deref()
                && task != agent.scope().task_id()
            {
                return Err(AppError::TaskScopeMismatch {
                    stored: agent.scope().task_id().to_owned(),
                    requested: task.to_owned(),
                });
            }
            agent
        }
        None => Agent::with_store_for_scope(
            &config,
            store,
            RequestScope::new(
                args.user.as_deref().unwrap_or(DEFAULT_USER_ID),
                args.task.as_deref().unwrap_or(DEFAULT_TASK_ID),
            )
            .expect("CLI identifiers and default scope are nonblank"),
        )?,
    };
    stdout_ui.write_block(
        &mut stdout,
        BlockStyle::System,
        &format!(
            "Scope · user: {} · task: {}",
            agent.scope().user_id(),
            agent.scope().task_id()
        ),
    )?;
    if let Some(id) = resume {
        stdout_ui.write_block(
            &mut stdout,
            BlockStyle::System,
            &format!("Resumed dialog #{id}."),
        )?;
        for message in agent.history().messages() {
            let (style, prefix) = match message.role() {
                Role::User => (BlockStyle::User, "you> "),
                Role::Assistant => (BlockStyle::Assistant, "assistant> "),
                Role::System => (BlockStyle::System, "system> "),
            };
            let mut block = stdout_ui.start_block(&mut stdout, style, prefix)?;
            block.write_text(message.content())?;
            block.finish()?;
        }
    }

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stderr = io::stderr();
    let stderr_ui = TerminalUi::stderr();
    let mut show_usage = resume.is_some();
    let mut footer_visible = show_usage && stdout_ui.is_interactive();
    if footer_visible {
        stdout_ui.write_usage(&mut stdout, agent.last_usage())?;
    }

    loop {
        stdout_ui.write_input_prompt(&mut stdout)?;

        let Some(line) = lines.next_line().await? else {
            stdout_ui.finish_empty_prompt(&mut stdout)?;
            if footer_visible {
                stdout_ui.erase_usage_before_input(&mut stdout, "")?;
            }
            break;
        };
        if footer_visible {
            stdout_ui.erase_usage_before_input(&mut stdout, &line)?;
        }
        stdout_ui.complete_input(&mut stdout, &line)?;

        match parse_input(&line) {
            InputAction::Ignore => {}
            InputAction::Exit => break,
            InputAction::Clear => {
                show_usage = true;
                agent.clear_history();
                stdout_ui.write_block(&mut stdout, BlockStyle::System, "Conversation cleared.")?;
            }
            InputAction::Stats => {
                stdout_ui.write_context_stats(&mut stdout, agent.context_stats())?;
            }
            InputAction::Remember { scope, key, value } => {
                agent.remember(scope, &key, &value)?;
                let address = memory_address_label(agent.scope(), scope);
                stdout_ui.write_block(
                    &mut stdout,
                    BlockStyle::System,
                    &format!(
                        "Saved {} memory · {key} = {value} · {address}",
                        scope.label()
                    ),
                )?;
            }
            InputAction::Forget { scope, key } => {
                let removed = agent.forget(scope, &key)?;
                let address = memory_address_label(agent.scope(), scope);
                let message = if removed {
                    format!("Forgot {} memory · {key} · {address}", scope.label())
                } else {
                    format!("No {} memory entry named {key} · {address}", scope.label())
                };
                stdout_ui.write_block(&mut stdout, BlockStyle::System, &message)?;
            }
            InputAction::Memory(filter) => {
                let snapshot = agent.memory_snapshot()?;
                stdout_ui.write_memory(
                    &mut stdout,
                    agent.scope(),
                    agent.context_stats(),
                    &snapshot,
                    filter,
                )?;
            }
            InputAction::Branch => match agent.branch_dialog() {
                Ok(fork) => stdout_ui.write_block(
                    &mut stdout,
                    BlockStyle::System,
                    &format!(
                        "Checkpoint {}: dialog #{} remains active; created branch #{}.",
                        fork.checkpoint_message_count, fork.original_dialog_id, fork.new_dialog_id
                    ),
                )?,
                Err(error) => stderr_ui.write_block(
                    &mut stderr,
                    BlockStyle::Error,
                    &format!("error: {error}"),
                )?,
            },
            InputAction::Switch(id) => match agent.switch_branch(id) {
                Ok(()) => {
                    stdout_ui.write_block(
                        &mut stdout,
                        BlockStyle::System,
                        &format!("Switched to branch #{id}."),
                    )?;
                    for message in agent.history().messages() {
                        let (style, prefix) = match message.role() {
                            Role::User => (BlockStyle::User, "you> "),
                            Role::Assistant => (BlockStyle::Assistant, "assistant> "),
                            Role::System => (BlockStyle::System, "system> "),
                        };
                        let mut replay = stdout_ui.start_block(&mut stdout, style, prefix)?;
                        replay.write_text(message.content())?;
                        replay.finish()?;
                    }
                }
                Err(error) => stderr_ui.write_block(
                    &mut stderr,
                    BlockStyle::Error,
                    &format!("error: {error}"),
                )?,
            },
            InputAction::InvalidCommand(error) => {
                stderr_ui.write_block(&mut stderr, BlockStyle::Error, &error)?;
            }
            InputAction::Send(user_message) => {
                show_usage = true;
                let mut block = Some(stdout_ui.start_response(&mut stdout)?);
                let mut deferred_warnings = Vec::new();

                let result = agent
                    .run_streaming(&user_message, |event| match event {
                        AgentEvent::Text(fragment) => block
                            .as_mut()
                            .expect("compaction starts after ordinary response text")
                            .write_text(fragment),
                        AgentEvent::Usage(_) => Ok(()),
                        AgentEvent::CompactionStarted {
                            covered_message_count,
                            kept_message_count,
                            ..
                        } => {
                            if let Some(response) = block.take() {
                                response.finish()?;
                            }
                            if stderr_ui.is_interactive() {
                                stderr_ui.write_status(
                                    &mut stderr,
                                    &format!(
                                        "Контекст · сжимаю до {covered_message_count}, оставляю {kept_message_count} сообщений"
                                    ),
                                )?;
                            }
                            Ok(())
                        }
                        AgentEvent::CompactionCompleted {
                            covered_message_count,
                            ..
                        } => {
                            if stderr_ui.is_interactive() {
                                stderr_ui.write_status(
                                    &mut stderr,
                                    &format!(
                                        "Контекст · summary обновлено до сообщения {covered_message_count}"
                                    ),
                                )?;
                            }
                            Ok(())
                        }
                        AgentEvent::CompactionFailed { error } => stderr_ui.write_block(
                            &mut stderr,
                            BlockStyle::Error,
                            &format!("context compaction failed: {error}"),
                        ),
                        AgentEvent::FactsUpdateStarted {
                            previous_boundary,
                            target_boundary,
                        } => {
                            if stderr_ui.is_interactive() {
                                stderr_ui.write_status(
                                    &mut stderr,
                                    &format!(
                                        "Контекст · обновляю facts: {previous_boundary} → {target_boundary}"
                                    ),
                                )?;
                            }
                            Ok(())
                        }
                        AgentEvent::FactsUpdateCompleted {
                            covered_message_count,
                            ..
                        } => {
                            if stderr_ui.is_interactive() {
                                stderr_ui.write_status(
                                    &mut stderr,
                                    &format!(
                                        "Контекст · facts обновлены до сообщения {covered_message_count}"
                                    ),
                                )?;
                            }
                            Ok(())
                        }
                        AgentEvent::FactsUpdateFailed { error } => {
                            deferred_warnings.push(format!("facts update failed: {error}"));
                            Ok(())
                        }
                        AgentEvent::DebugLogFailed { error } => {
                            if block.is_some() {
                                deferred_warnings.push(error);
                                Ok(())
                            } else {
                                stderr_ui.write_block(&mut stderr, BlockStyle::Error, &error)
                            }
                        }
                    })
                    .await;
                if let Some(response) = block.take() {
                    response.finish()?;
                }
                for warning in deferred_warnings {
                    stderr_ui.write_block(&mut stderr, BlockStyle::Error, &warning)?;
                }

                match result {
                    Ok(_) => {}
                    // Stop on persistence errors: never continue an unsaved session silently.
                    Err(error @ AgentError::Store(_)) => return Err(error.into()),
                    Err(error) => stderr_ui.write_block(
                        &mut stderr,
                        BlockStyle::Error,
                        &format!("error: {error}"),
                    )?,
                }
            }
        }
        footer_visible = show_usage && stdout_ui.is_interactive();
        if footer_visible {
            stdout_ui.write_usage(&mut stdout, agent.last_usage())?;
        }
    }

    if show_usage {
        stdout_ui.write_usage(&mut stdout, agent.last_usage())?;
    }

    Ok(())
}

#[derive(Debug, Error)]
enum AppError {
    #[error("dialog belongs to user '{stored}', not requested user '{requested}'")]
    UserScopeMismatch { stored: String, requested: String },
    #[error("dialog belongs to task '{stored}', not requested task '{requested}'")]
    TaskScopeMismatch { stored: String, requested: String },
    #[error("no saved dialogs; start a new chat without --resume-last")]
    NoDialogs,
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Agent(#[from] AgentError),
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("terminal I/O failed: {0}")]
    Io(#[from] io::Error),
}

fn memory_address_label(scope: &RequestScope, layer: DurableMemoryScope) -> String {
    match layer {
        DurableMemoryScope::User => format!("user: {}", scope.user_id()),
        DurableMemoryScope::Task => {
            format!("user: {} · task: {}", scope.user_id(), scope.task_id())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use clap::Parser;

    use super::Args;

    #[test]
    fn accepts_and_trims_memory_scope_identifiers() {
        let args =
            Args::try_parse_from(["deepseek-cli", "--user", " alice ", "--task", " bot "]).unwrap();
        assert_eq!(args.user.as_deref(), Some("alice"));
        assert_eq!(args.task.as_deref(), Some("bot"));
        let defaults = Args::try_parse_from(["deepseek-cli"]).unwrap();
        assert!(defaults.user.is_none());
        assert!(defaults.task.is_none());
    }

    #[test]
    fn rejects_blank_memory_scope_identifiers() {
        for flag in ["--user", "--task"] {
            for value in ["", " \t "] {
                let error = Args::try_parse_from(["deepseek-cli", flag, value]).unwrap_err();
                assert!(error.to_string().contains("identifier must not be blank"));
            }
        }
    }

    #[test]
    fn uses_deepseek_toml_by_default() {
        let args = Args::try_parse_from(["deepseek-cli"]).expect("parse default arguments");

        assert_eq!(args.config, Path::new("deepseek.toml"));
    }

    #[test]
    fn accepts_custom_config_path() {
        let args = Args::try_parse_from(["deepseek-cli", "--config", "custom.toml"])
            .expect("parse custom configuration path");

        assert_eq!(args.config, Path::new("custom.toml"));
    }
}
