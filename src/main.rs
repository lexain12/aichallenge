use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use deepseek_cli::chat::{ChatHistory, InputAction, parse_input};
use deepseek_cli::client::{ClientError, DeepSeekClient};
use deepseek_cli::config::{Config, ConfigError};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Debug, Parser)]
#[command(version, about)]
struct Args {
    /// Path to the TOML configuration file.
    #[arg(long, default_value = "deepseek.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), AppError> {
    let args = Args::parse();
    let env_api_key = std::env::var("DEEPSEEK_API_KEY").ok();
    let config = Config::load(&args.config, env_api_key)?;
    let client = DeepSeekClient::new(&config)?;
    let mut history = ChatHistory::new(config.system_prompt().to_owned());

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = io::stdout();

    loop {
        write!(stdout, "you> ")?;
        stdout.flush()?;

        let Some(line) = lines.next_line().await? else {
            writeln!(stdout)?;
            break;
        };

        match parse_input(&line) {
            InputAction::Ignore => {}
            InputAction::Exit => break,
            InputAction::Clear => {
                history.clear();
                writeln!(stdout, "Conversation cleared.")?;
            }
            InputAction::Send(user_message) => {
                let request = history.request_messages(&user_message);
                write!(stdout, "assistant> ")?;
                stdout.flush()?;

                let result = client
                    .stream_chat(&request, |fragment| {
                        write!(stdout, "{fragment}")?;
                        stdout.flush()
                    })
                    .await;
                writeln!(stdout)?;

                match result {
                    Ok(assistant_message) => {
                        history.commit_turn(user_message, assistant_message);
                    }
                    Err(error) => eprintln!("error: {error}"),
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug, Error)]
enum AppError {
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
