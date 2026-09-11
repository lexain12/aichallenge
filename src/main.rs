use std::io;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use deepseek_cli::agent::{Agent, AgentError};
use deepseek_cli::chat::{InputAction, Role, parse_input};
use deepseek_cli::client::{ClientError, StreamEvent};
use deepseek_cli::config::{Config, ConfigError};
use deepseek_cli::dialog::{DialogStore, StoreError};
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
                "ID | Updated (UTC) | Messages | First message",
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
                        "{} | {} | {} | {}",
                        dialog.id, dialog.updated_at, dialog.message_count, title
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
        Some(id) => Agent::from_dialog(&config, store, id)?,
        None => Agent::with_store(&config, store)?,
    };
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

    loop {
        stdout_ui.write_input_prompt(&mut stdout)?;

        let Some(line) = lines.next_line().await? else {
            stdout_ui.finish_empty_prompt(&mut stdout)?;
            break;
        };
        stdout_ui.complete_input(&mut stdout, &line)?;

        match parse_input(&line) {
            InputAction::Ignore => {}
            InputAction::Exit => break,
            InputAction::Clear => {
                agent.clear_history();
                stdout_ui.write_block(&mut stdout, BlockStyle::System, "Conversation cleared.")?;
            }
            InputAction::Send(user_message) => {
                let mut block =
                    stdout_ui.start_block(&mut stdout, BlockStyle::Assistant, "assistant> ")?;

                let result = agent
                    .run_streaming(&user_message, |event| match event {
                        StreamEvent::Text(fragment) => block.write_text(fragment),
                        StreamEvent::Usage(_) => Ok(()),
                    })
                    .await;
                block.finish()?;

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
    }

    Ok(())
}

#[derive(Debug, Error)]
enum AppError {
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use clap::Parser;

    use super::Args;

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
