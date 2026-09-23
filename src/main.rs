use std::fs;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use deepseek_cli::agent::{Agent, AgentError, AgentEvent};
use deepseek_cli::chat::{InputAction, InvariantAction, ProfileAction, Role, parse_input};
use deepseek_cli::client::ClientError;
use deepseek_cli::config::{Config, ConfigError};
use deepseek_cli::dialog::{DialogStore, StoreError};
use deepseek_cli::memory::{DEFAULT_TASK_ID, DEFAULT_USER_ID, DurableMemoryScope, RequestScope};
use deepseek_cli::terminal::{BlockStyle, TerminalUi};
use deepseek_cli::workflow::TaskPhase;
use deepseek_cli::workflow_engine::{WorkflowEngineError, WorkflowTurnEvent};
use deepseek_cli::workflow_store::PauseOutcome;
use thiserror::Error;
use tokio::sync::mpsc;

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
            let message = error.operator_message();
            let ui = TerminalUi::stderr();
            let mut stderr = io::stderr();
            if ui
                .write_block(&mut stderr, BlockStyle::Error, &format!("error: {message}"))
                .is_err()
            {
                eprintln!("error: {message}");
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
    if resume.is_some() {
        let recovery_ui = TerminalUi::stderr();
        let mut recovery_stderr = io::stderr();
        let recovery = tokio::select! {
            biased;
            signal = tokio::signal::ctrl_c() => {
                signal?;
                None
            }
            result = agent.recover_workflow_processing_streaming(|event| match event {
                AgentEvent::DebugLogFailed { error } => recovery_ui.write_block(
                    &mut recovery_stderr,
                    BlockStyle::Error,
                    &error,
                ),
                _ => Ok(()),
            }) => Some(result),
        };
        let Some(recovery) = recovery else {
            finish_interruption(
                &mut agent,
                &stdout_ui,
                &mut stdout,
                InterruptionCleanup::None,
                true,
            )?;
            return Ok(());
        };
        let failed = match recovery {
            Ok(recovered) => recovered.iter().any(|job| {
                job.stop_reason
                    != deepseek_cli::workflow_engine::AutonomyStopReason::AwaitUserAfterRestart
            }),
            Err(
                error
                @ (AgentError::Store(_) | AgentError::Workflow(WorkflowEngineError::Store(_))),
            ) => return Err(error.into()),
            Err(_) => true,
        };
        if failed {
            TerminalUi::stderr().write_block(
                &mut io::stderr(),
                BlockStyle::Error,
                "workflow recovery could not finish advisory processing; waiting for human input.",
            )?;
        }
    }
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
        let status = agent.workflow_status()?;
        stdout_ui.write_workflow_status(&mut stdout, status.as_ref())?;
    }

    let mut lines = stdin_lines()?;
    let mut stderr = io::stderr();
    let stderr_ui = TerminalUi::stderr();
    let mut show_usage = resume.is_some();
    let mut footer_visible = show_usage && stdout_ui.is_interactive();
    if footer_visible {
        stdout_ui.write_usage(&mut stdout, agent.last_usage())?;
    }

    loop {
        stdout_ui.write_input_prompt(&mut stdout)?;

        let line = tokio::select! {
            biased;
            signal = tokio::signal::ctrl_c() => {
                signal?;
                None
            }
            line = lines.recv() => Some(line.transpose()?),
        };
        let Some(line) = line else {
            finish_interruption(
                &mut agent,
                &stdout_ui,
                &mut stdout,
                InterruptionCleanup::EmptyPrompt,
                false,
            )?;
            return Ok(());
        };
        let Some(line) = line else {
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
                stdout_ui.write_context_stats(&mut stdout, agent.context_stats()?)?;
            }
            InputAction::TaskStatus => {
                let status = agent.workflow_status()?;
                stdout_ui.write_workflow_status(&mut stdout, status.as_ref())?;
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
                    agent.context_stats()?,
                    &snapshot,
                    filter,
                )?;
            }
            InputAction::Profile(action) => match action {
                ProfileAction::Show => {
                    let profile = agent.profile()?;
                    stdout_ui.write_profile(
                        &mut stdout,
                        agent.scope().user_id(),
                        profile.as_ref(),
                    )?;
                }
                ProfileAction::Set(markdown) => {
                    agent.replace_profile(&markdown)?;
                    stdout_ui.write_block(
                        &mut stdout,
                        BlockStyle::System,
                        &format!("Saved profile · user: {}", agent.scope().user_id()),
                    )?;
                }
                ProfileAction::Import(path) => match fs::read_to_string(&path) {
                    Ok(markdown) => match agent.replace_profile(&markdown) {
                        Ok(()) => stdout_ui.write_block(
                            &mut stdout,
                            BlockStyle::System,
                            &format!(
                                "Imported profile · user: {} · path: {path}",
                                agent.scope().user_id()
                            ),
                        )?,
                        Err(error) => stderr_ui.write_block(
                            &mut stderr,
                            BlockStyle::Error,
                            &format!("failed to import profile from '{path}': {error}"),
                        )?,
                    },
                    Err(error) => stderr_ui.write_block(
                        &mut stderr,
                        BlockStyle::Error,
                        &format!("failed to import profile from '{path}': {error}"),
                    )?,
                },
                ProfileAction::Clear => {
                    let user_id = agent.scope().user_id().to_owned();
                    let message = if agent.clear_profile()? {
                        format!("Cleared profile · user: {user_id}")
                    } else {
                        format!("No profile for user: {user_id}")
                    };
                    stdout_ui.write_block(&mut stdout, BlockStyle::System, &message)?;
                }
            },
            InputAction::Invariant(action) => match action {
                InvariantAction::List => {
                    let snapshot = agent.invariants()?;
                    let message = if snapshot.is_empty() {
                        "Invariants · empty".to_owned()
                    } else {
                        snapshot
                            .rules()
                            .iter()
                            .map(|rule| format!("{} · {}", rule.id, rule.text))
                            .collect::<Vec<_>>()
                            .join("\n")
                    };
                    stdout_ui.write_block(&mut stdout, BlockStyle::System, &message)?;
                }
                InvariantAction::Add { id, text } => match agent.upsert_invariant(&id, &text) {
                    Ok(()) => stdout_ui.write_block(
                        &mut stdout,
                        BlockStyle::System,
                        &format!("Saved invariant · {id}"),
                    )?,
                    Err(error @ AgentError::Store(StoreError::ConfiguredInvariant(_))) => {
                        stderr_ui.write_block(&mut stderr, BlockStyle::Error, &error.to_string())?
                    }
                    Err(error) => return Err(error.into()),
                },
                InvariantAction::Remove { id } => match agent.delete_invariant(&id) {
                    Ok(removed) => {
                        let message = if removed {
                            format!("Removed invariant · {id}")
                        } else {
                            format!("No invariant named {id}")
                        };
                        stdout_ui.write_block(&mut stdout, BlockStyle::System, &message)?;
                    }
                    Err(error @ AgentError::Store(StoreError::ConfiguredInvariant(_))) => {
                        stderr_ui.write_block(&mut stderr, BlockStyle::Error, &error.to_string())?
                    }
                    Err(error) => return Err(error.into()),
                },
            },
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
                let interrupted = tokio::select! {
                    biased;
                    signal = tokio::signal::ctrl_c() => {
                        signal?;
                        true
                    }
                    result = run_prompt(
                        &mut agent,
                        &user_message,
                        stdout_ui,
                        &mut stdout,
                        stderr_ui,
                        &mut stderr,
                    ) => {
                        result?;
                        false
                    }
                };
                if interrupted {
                    finish_interruption(
                        &mut agent,
                        &stdout_ui,
                        &mut stdout,
                        InterruptionCleanup::Response,
                        true,
                    )?;
                    return Ok(());
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

fn stdin_lines() -> io::Result<mpsc::Receiver<io::Result<String>>> {
    let (sender, receiver) = mpsc::channel(1);
    std::thread::Builder::new()
        .name("deepseek-cli-stdin".to_owned())
        .spawn(move || {
            let stdin = io::stdin();
            for line in stdin.lock().lines() {
                let is_error = line.is_err();
                if sender.blocking_send(line).is_err() || is_error {
                    break;
                }
            }
        })?;
    Ok(receiver)
}

#[derive(Clone, Copy)]
enum InterruptionCleanup {
    None,
    EmptyPrompt,
    Response,
}

fn finish_interruption<W: io::Write>(
    agent: &mut Agent,
    ui: &TerminalUi,
    writer: &mut W,
    cleanup: InterruptionCleanup,
    discarded_model_result: bool,
) -> Result<(), AppError> {
    let cleanup_result = match cleanup {
        InterruptionCleanup::None => Ok(()),
        InterruptionCleanup::EmptyPrompt => ui.finish_empty_prompt(writer),
        InterruptionCleanup::Response => ui.finish_interrupted_response(writer),
    };
    let mut errors = Vec::new();
    if let Err(error) = cleanup_result {
        errors.push(format!(
            "terminal I/O failed during interrupt cleanup: {error}"
        ));
    }

    match agent.pause_current_workflow() {
        Ok(outcome) => {
            if let Err(error) = write_interruption(ui, writer, outcome, discarded_model_result) {
                errors.push(format!(
                    "terminal I/O failed while reporting interruption: {error}"
                ));
            }
        }
        Err(error) => errors.push(format!("workflow pause failed: {error}")),
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(AppError::Interruption(errors.join("; ")))
    }
}

async fn run_prompt<W: io::Write, E: io::Write>(
    agent: &mut Agent,
    user_message: &str,
    stdout_ui: TerminalUi,
    stdout: &mut W,
    stderr_ui: TerminalUi,
    stderr: &mut E,
) -> Result<(), AppError> {
    let mut block = Some(stdout_ui.start_response(stdout)?);
    let mut deferred_warnings = Vec::new();
    let mut pending_autonomous_status = None;
    let result = agent
        .run_streaming(user_message, |event| match event {
            AgentEvent::Text(fragment) => block
                .as_mut()
                .expect("compaction starts after ordinary response text")
                .write_text(fragment),
            AgentEvent::Usage(_) => Ok(()),
            AgentEvent::Workflow(event) => match event {
                WorkflowTurnEvent::AutonomousTurnStarted { number, phase } => {
                    block
                        .as_mut()
                        .expect("workflow response renderer remains available")
                        .finish_current()?;
                    pending_autonomous_status = Some(format!(
                        "Controller · autonomous turn {number} · {}",
                        task_phase_name(phase)
                    ));
                    Ok(())
                }
                WorkflowTurnEvent::ResponseStarted {
                    autonomous_turn, ..
                } if autonomous_turn > 0 => {
                    let status = pending_autonomous_status
                        .take()
                        .unwrap_or_else(|| format!("Controller · autonomous turn {autonomous_turn}"));
                    block
                        .as_mut()
                        .expect("workflow response renderer remains available")
                        .start_next_response(&status, stdout_ui.is_interactive())
                }
                WorkflowTurnEvent::ResponseStarted { .. } => Ok(()),
                WorkflowTurnEvent::InputRejected { reason } => {
                    if let Some(response) = block.take() {
                        response.finish()?;
                    }
                    stderr_ui.write_block(stderr, BlockStyle::Error, &reason)
                }
                WorkflowTurnEvent::ProcessingFailed { checker, error } => {
                    deferred_warnings.push(format!(
                        "workflow processing failed ({checker}): {error}"
                    ));
                    Ok(())
                }
                WorkflowTurnEvent::Stopped { .. } => Ok(()),
            },
            AgentEvent::CompactionStarted {
                covered_message_count,
                kept_message_count,
                ..
            } => {
                if let Some(response) = block.as_mut() {
                    response.finish_current()?;
                }
                if stderr_ui.is_interactive() {
                    stderr_ui.write_status(
                        stderr,
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
                        stderr,
                        &format!(
                            "Контекст · summary обновлено до сообщения {covered_message_count}"
                        ),
                    )?;
                }
                Ok(())
            }
            AgentEvent::CompactionFailed { error } => stderr_ui.write_block(
                stderr,
                BlockStyle::Error,
                &format!("context compaction failed: {error}"),
            ),
            AgentEvent::FactsUpdateStarted {
                previous_boundary,
                target_boundary,
            } => {
                if stderr_ui.is_interactive() {
                    stderr_ui.write_status(
                        stderr,
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
                        stderr,
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
                    stderr_ui.write_block(stderr, BlockStyle::Error, &error)
                }
            }
        })
        .await;
    if let Some(response) = block.take() {
        response.finish()?;
    }
    for warning in deferred_warnings {
        stderr_ui.write_block(stderr, BlockStyle::Error, &warning)?;
    }
    match result {
        Ok(_) => Ok(()),
        // Stop on persistence errors: never continue an unsaved session silently.
        Err(
            error @ (AgentError::Store(_) | AgentError::Workflow(WorkflowEngineError::Store(_))),
        ) => Err(error.into()),
        Err(error) => {
            stderr_ui.write_block(
                stderr,
                BlockStyle::Error,
                &format!("error: {}", error.operator_message()),
            )?;
            Ok(())
        }
    }
}

fn write_interruption<W: io::Write>(
    ui: &TerminalUi,
    writer: &mut W,
    outcome: PauseOutcome,
    discarded_model_result: bool,
) -> io::Result<()> {
    let message = match (outcome, discarded_model_result) {
        (PauseOutcome::Paused(_), true) => "Task paused. The partial model result was discarded.",
        (PauseOutcome::AlreadyPaused, true) => {
            "Task remains paused. The partial model result was discarded."
        }
        (PauseOutcome::AlreadyDone, true) => {
            "Task is already complete. The in-flight model result was discarded."
        }
        (PauseOutcome::NoTask, true) => {
            "Interrupted. The partial model result was discarded; no workflow task was created."
        }
        (PauseOutcome::Paused(_), false) => "Task paused.",
        (PauseOutcome::AlreadyPaused, false) => "Task remains paused.",
        (PauseOutcome::AlreadyDone, false) => "Task is already complete.",
        (PauseOutcome::NoTask, false) => "Interrupted. No workflow task was created.",
    };
    ui.write_block(writer, BlockStyle::System, message)
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
    #[error("interruption handling failed: {0}")]
    Interruption(String),
}

impl AppError {
    fn operator_message(&self) -> String {
        match self {
            Self::Agent(error) => error.operator_message(),
            Self::Client(error) => error.operator_message("chat"),
            _ => self.to_string(),
        }
    }
}

fn memory_address_label(scope: &RequestScope, layer: DurableMemoryScope) -> String {
    match layer {
        DurableMemoryScope::User => format!("user: {}", scope.user_id()),
        DurableMemoryScope::Task => {
            format!("user: {} · task: {}", scope.user_id(), scope.task_id())
        }
    }
}

fn task_phase_name(phase: TaskPhase) -> &'static str {
    match phase {
        TaskPhase::Planning => "planning",
        TaskPhase::Execution => "execution",
        TaskPhase::Validation => "validation",
        TaskPhase::Done => "done",
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
