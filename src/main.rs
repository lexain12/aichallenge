mod day3;
mod window_config;

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
    /// Open four terminal panels for the Day 3 reasoning experiment.
    #[arg(long, conflicts_with_all = ["temperatures", "models"])]
    day3: bool,
    /// Compare the same query at temperatures 0, 0.7, 1.2 and 1.0.
    #[arg(long)]
    temperatures: bool,
    /// Compare models using independent panel configurations.
    #[arg(long, conflicts_with = "temperatures")]
    models: bool,
    /// Directory containing window-1.toml through window-4.toml.
    #[arg(long, alias = "panels-dir", default_value = "panels")]
    windows_config: PathBuf,
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
    if args.day3 {
        return day3::run(config, day3::Mode::Reasoning, args.windows_config)
            .map_err(AppError::Tui);
    }
    if args.models {
        return day3::run(config, day3::Mode::Models, args.windows_config).map_err(AppError::Tui);
    }
    if args.temperatures {
        return day3::run(config, day3::Mode::Temperatures, args.windows_config)
            .map_err(AppError::Tui);
    }
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
                let request = history.request_messages(&user_message);
                let mut block =
                    stdout_ui.start_block(&mut stdout, BlockStyle::Assistant, "assistant> ")?;

                let result = client
                    .stream_chat(&request, |fragment| block.write_text(fragment))
                    .await;
                block.finish()?;

                match result {
                    Ok(assistant_message) => {
                        history.commit_turn(user_message, assistant_message);
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

#[derive(Debug, Error)]
enum AppError {
    #[error("terminal interface failed: {0}")]
    Tui(String),
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
