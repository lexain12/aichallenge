use std::io::{self, BufReader};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use deepseek_cli::agent_runner::{CronAgentService, CronRunOutcome};
use deepseek_cli::domain::JobId;
use deepseek_cli::inspection::{InspectionService, ReadonlyDbShell};
use deepseek_cli::provider::{DeepSeekProvider, Provider};
use deepseek_cli::scheduler::{
    CronRunReconciler, CronSynchronizer, SyncReport, SystemCrontabBackend,
};
use deepseek_cli::server::{ServerDependencies, StdioServer};
use deepseek_cli::settings::ServerSettings;
use deepseek_cli::store::Store;
use deepseek_cli::tools::ToolExecutor;
use deepseek_cli::tools::mcp::McpRegistry;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
#[command(
    name = "light-agent",
    version,
    about = "Remote light-agent runtime",
    disable_help_subcommand = true
)]
struct Args {
    /// Server configuration file on the VM.
    #[arg(long, default_value = "light-agent.toml", global = true)]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve one versioned NDJSON session over stdin/stdout.
    ServeStdio,
    /// Claim and execute one scheduled job.
    RunJob {
        #[arg(value_parser = parse_job_id)]
        job_id: JobId,
    },
    /// Reconcile the managed Cronie block from SQLite.
    CronSync,
    /// Run the restricted local SQL shell.
    DbShell {
        #[arg(long, required = true)]
        readonly: bool,
    },
}

fn parse_job_id(value: &str) -> Result<JobId, String> {
    JobId::from_str(value).map_err(|_| "invalid canonical job ID".to_owned())
}

#[derive(Clone, Copy)]
enum AppError {
    Configuration,
    Store,
    Provider,
    Mcp,
    Scheduler,
    Protocol,
    Inspection,
    Run,
}

impl AppError {
    fn code(self) -> &'static str {
        match self {
            Self::Configuration => "configuration_error",
            Self::Store => "store_error",
            Self::Provider => "provider_error",
            Self::Mcp => "mcp_error",
            Self::Scheduler => "scheduler_error",
            Self::Protocol => "protocol_error",
            Self::Inspection => "inspection_error",
            Self::Run => "run_error",
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {}", error.code());
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<(), AppError> {
    let settings = Arc::new(load_settings(&args.config)?);
    match args.command {
        Command::ServeStdio => serve_stdio(settings).await,
        Command::RunJob { job_id } => run_job(settings, job_id).await,
        Command::CronSync => cron_sync(settings).await,
        Command::DbShell { readonly } => {
            debug_assert!(readonly, "clap requires --readonly");
            db_shell(settings.database_path())
        }
    }
}

fn load_settings(path: &Path) -> Result<ServerSettings, AppError> {
    ServerSettings::load(path, std::env::var("DEEPSEEK_API_KEY").ok())
        .map_err(|_| AppError::Configuration)
}

fn open_store(settings: &ServerSettings) -> Result<Store, AppError> {
    Store::open(settings.database_path()).map_err(|_| AppError::Store)
}

fn synchronizer(
    settings: &ServerSettings,
    store: Store,
) -> Result<Arc<CronSynchronizer>, AppError> {
    let backend = Arc::new(
        SystemCrontabBackend::new(settings.scheduler().crontab_binary().to_owned())
            .map_err(|_| AppError::Scheduler)?,
    );
    CronSynchronizer::new(
        store,
        backend,
        settings.scheduler().lock_path().to_owned(),
        settings.scheduler().binary_path().to_owned(),
    )
    .map(Arc::new)
    .map_err(|_| AppError::Scheduler)
}

async fn runtime_catalog(
    settings: &ServerSettings,
) -> Result<(Arc<dyn Provider>, Arc<dyn ToolExecutor>), AppError> {
    let provider: Arc<dyn Provider> =
        Arc::new(DeepSeekProvider::new(settings.provider()).map_err(|_| AppError::Provider)?);
    let mcp: Arc<dyn ToolExecutor> = Arc::new(
        McpRegistry::connect(settings.mcp())
            .await
            .map_err(|_| AppError::Mcp)?,
    );
    Ok((provider, mcp))
}

async fn serve_stdio(settings: Arc<ServerSettings>) -> Result<(), AppError> {
    let store = open_store(&settings)?;
    StdioServer::recover_startup(&store).map_err(|_| AppError::Store)?;
    let (provider, mcp) = runtime_catalog(&settings).await?;
    let synchronizer = synchronizer(&settings, store.clone())?;
    let server = StdioServer::new(ServerDependencies {
        settings,
        store: store.clone(),
        provider,
        mcp,
        synchronizer,
        inspection: InspectionService::new(store),
    });
    server
        .serve(tokio::io::stdin(), tokio::io::stdout())
        .await
        .map_err(|_| AppError::Protocol)
}

async fn run_job(settings: Arc<ServerSettings>, job_id: JobId) -> Result<(), AppError> {
    let store = open_store(&settings)?;
    let (provider, mcp) = runtime_catalog(&settings).await?;
    let synchronizer = synchronizer(&settings, store.clone())?;
    let reconciler: Arc<dyn CronRunReconciler> = synchronizer;
    let service = CronAgentService::new(
        store,
        provider,
        mcp,
        settings.cron_system_prompt(),
        settings.mcp().max_tool_rounds() as usize,
        settings.max_provider_request_bytes(),
        settings.max_message_bytes(),
        settings.scheduler().run_timeout(),
        Some(reconciler),
    );
    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_cancellation.cancel();
        }
    });
    let result = service.run_job(job_id, cancellation).await;
    signal.abort();
    let _ = signal.await;
    match result.map_err(|_| AppError::Run)? {
        CronRunOutcome::Inactive | CronRunOutcome::Skipped(_) | CronRunOutcome::Completed(_) => {
            Ok(())
        }
    }
}

async fn cron_sync(settings: Arc<ServerSettings>) -> Result<(), AppError> {
    let store = open_store(&settings)?;
    let synchronizer = synchronizer(&settings, store)?;
    match synchronizer.sync().await.map_err(|_| AppError::Scheduler)? {
        SyncReport::Installed { jobs } => println!("installed {jobs}"),
        SyncReport::SavedNotInstalled { jobs } => println!("saved_not_installed {jobs}"),
    }
    Ok(())
}

fn db_shell(path: &Path) -> Result<(), AppError> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    ReadonlyDbShell::new(path)
        .run(BufReader::new(stdin.lock()), stdout.lock())
        .map_err(|_| AppError::Inspection)
}
