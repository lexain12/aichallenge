use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use deepseek_cli::remote_client::SshTransport;
use deepseek_cli::settings::ClientSettings;
use deepseek_cli::terminal_client::TerminalClient;

#[derive(Debug, Parser)]
#[command(
    name = "light-agent-client",
    version,
    about = "Terminal client for a remote light agent"
)]
struct Args {
    /// Local SSH transport configuration.
    #[arg(long, default_value = "light-agent-client.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let result = async {
        let settings = ClientSettings::load(&args.config).map_err(|_| "configuration_error")?;
        let session = SshTransport::connect(&settings)
            .await
            .map_err(|_| "ssh_error")?;
        TerminalClient::run(
            session,
            tokio::io::stdin(),
            tokio::io::stdout(),
            tokio::io::stderr(),
        )
        .await
        .map_err(|_| "client_error")
    }
    .await;
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => {
            eprintln!("error: {code}");
            ExitCode::FAILURE
        }
    }
}
