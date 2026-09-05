use std::io;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use deepseek_cli::chat::{ChatHistory, InputAction, parse_input};
use deepseek_cli::client::{ClientError, DeepSeekClient};
use deepseek_cli::config::{Config, ConfigError};
use deepseek_cli::terminal::{BlockStyle, TerminalUi};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Debug, Parser)]
#[command(version, about)]
struct Args {
    /// Path to the TOML configuration file.
    #[arg(long, default_value = "deepseek.toml")]
    config: PathBuf,
    /// Compare two answers; omit QUERY to enter queries interactively.
    #[arg(long, num_args = 0..=1, value_name = "QUERY", conflicts_with = "unrestricted")]
    compare: Option<Option<String>>,
    /// Omit the system prompt and custom stops; use a 4096-token budget.
    #[arg(long)]
    unrestricted: bool,
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
    let env_api_key = std::env::var("DEEPSEEK_API_KEY").ok();
    let config = Config::load(&args.config, env_api_key)?;
    let interactive_compare = args.compare.is_some();
    if let Some(Some(query)) = args.compare {
        return compare(&config, &query).await;
    }
    let config = if args.unrestricted {
        config.unrestricted()
    } else {
        config
    };
    let client = DeepSeekClient::new(&config)?;
    let mut history = ChatHistory::new(config.system_prompt().to_owned());

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = io::stdout();
    let mut stderr = io::stderr();
    let stdout_ui = TerminalUi::stdout();
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
                history.clear();
                stdout_ui.write_block(&mut stdout, BlockStyle::System, "Conversation cleared.")?;
            }
            InputAction::Send(user_message) => {
                if interactive_compare {
                    if let Err(error) = compare(&config, &user_message).await {
                        stderr_ui.write_block(
                            &mut stderr,
                            BlockStyle::Error,
                            &format!("error: {error}"),
                        )?;
                    }
                    continue;
                }
                let request = history.request_messages(&user_message);
                let mut block =
                    stdout_ui.start_block(&mut stdout, BlockStyle::Assistant, "assistant> ")?;

                let result = client
                    .stream_chat_detailed(&request, |fragment| block.write_text(fragment))
                    .await;
                block.finish()?;

                match result {
                    Ok(assistant_message) => {
                        if assistant_message.finish_reason.as_deref() == Some("length") {
                            stderr_ui.write_block(
                                &mut stderr,
                                BlockStyle::Error,
                                "Ответ обрезан лимитом токенов; рецепт может быть неполным.",
                            )?;
                        } else {
                            history.commit_turn(user_message, assistant_message.text);
                        }
                    }
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

async fn compare(config: &Config, query: &str) -> Result<(), AppError> {
    if query.trim().is_empty() {
        return Err(AppError::EmptyQuery);
    }
    let ui = TerminalUi::stdout();
    let mut stdout = io::stdout();
    ui.write_block(&mut stdout, BlockStyle::System, &format!("Запрос: {query}"))?;
    for (label, settings) in [
        ("Без ограничений формата", config.unrestricted()),
        ("С ограничениями", config.clone()),
    ] {
        ui.write_block(
            &mut stdout,
            BlockStyle::System,
            &format!(
                "{label} (max_tokens={}, stop={:?})",
                settings.max_tokens(),
                settings.stop()
            ),
        )?;
        let history = ChatHistory::new(settings.system_prompt().to_owned());
        let client = DeepSeekClient::new(&settings)?;
        let mut block = ui.start_block(&mut stdout, BlockStyle::Assistant, "assistant> ")?;
        let result = client
            .stream_chat_detailed(&history.request_messages(query), |fragment| {
                block.write_text(fragment)
            })
            .await;
        block.finish()?;
        let answer = result?;
        ui.write_block(
            &mut stdout,
            BlockStyle::System,
            &format!(
                "Слов: {}; символов: {}; finish_reason: {}{}",
                answer.text.split_whitespace().count(),
                answer.text.chars().count(),
                answer.finish_reason.as_deref().unwrap_or("unknown"),
                if answer.finish_reason.as_deref() == Some("length") {
                    "; ответ обрезан лимитом токенов"
                } else {
                    ""
                },
            ),
        )?;
    }
    Ok(())
}

#[derive(Debug, Error)]
enum AppError {
    #[error("comparison query must not be blank")]
    EmptyQuery,
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
