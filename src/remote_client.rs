//! System-OpenSSH transport for the macOS terminal client.

use std::ffi::{OsStr, OsString};
use std::process::Stdio;
use std::time::Duration;

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use crate::protocol::{NdjsonReader, NdjsonWriter, RequestEnvelope, ServerEnvelope};
use crate::settings::ClientSettings;

const ENVIRONMENT_ALLOWLIST: [&str; 10] = [
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SSH_AUTH_SOCK",
    "TMPDIR",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "LC_MESSAGES",
];
const SSH_EXIT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ClientError {
    #[error("ssh_spawn_error")]
    SshSpawn,
    #[error("ssh_pipe_error")]
    SshPipe,
    #[error("ssh_exit_error")]
    SshExit,
    #[error("transport_closed")]
    TransportClosed,
    #[error("protocol_error")]
    Protocol,
    #[error("input_error")]
    Input,
    #[error("output_error")]
    Output,
    #[error("no_active_dialog")]
    NoActiveDialog,
    #[error("export_error")]
    Export,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SshLaunchSpec {
    program: OsString,
    arguments: [OsString; 3],
    environment: Vec<(OsString, OsString)>,
}

impl SshLaunchSpec {
    pub fn program(&self) -> &OsStr {
        &self.program
    }

    pub fn arguments(&self) -> &[OsString] {
        &self.arguments
    }

    pub fn environment(&self) -> &[(OsString, OsString)] {
        &self.environment
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.program);
        command
            .args(&self.arguments)
            .env_clear()
            .envs(self.environment.iter().cloned())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        command
    }
}

pub struct SshTransport;

impl SshTransport {
    pub fn launch_spec(settings: &ClientSettings) -> SshLaunchSpec {
        let environment = ENVIRONMENT_ALLOWLIST
            .into_iter()
            .filter_map(|name| std::env::var_os(name).map(|value| (OsString::from(name), value)))
            .collect();
        SshLaunchSpec {
            program: settings.ssh_binary().as_os_str().to_owned(),
            arguments: [
                OsString::from("-T"),
                OsString::from(settings.ssh_host()),
                OsString::from(settings.remote_command()),
            ],
            environment,
        }
    }

    pub async fn connect(
        settings: &ClientSettings,
    ) -> Result<RemoteSession<ChildStdout, ChildStdin>, ClientError> {
        let mut child = Self::launch_spec(settings)
            .command()
            .spawn()
            .map_err(|_| ClientError::SshSpawn)?;
        let reader = child.stdout.take().ok_or(ClientError::SshPipe)?;
        let writer = child.stdin.take().ok_or(ClientError::SshPipe)?;
        Ok(RemoteSession {
            reader: NdjsonReader::new(reader),
            writer: NdjsonWriter::new(writer),
            child: Some(child),
        })
    }
}

pub struct RemoteSession<R, W> {
    reader: NdjsonReader<R>,
    writer: NdjsonWriter<W>,
    child: Option<Child>,
}

impl<R, W> RemoteSession<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    pub fn from_streams(reader: R, writer: W) -> Self {
        Self {
            reader: NdjsonReader::new(reader),
            writer: NdjsonWriter::new(writer),
            child: None,
        }
    }

    pub async fn send(&mut self, request: &RequestEnvelope) -> Result<(), ClientError> {
        self.writer
            .write_request(request)
            .await
            .map_err(|_| ClientError::Protocol)
    }

    pub async fn event(&mut self) -> Result<Option<ServerEnvelope>, ClientError> {
        self.reader
            .read_event()
            .await
            .map_err(|_| ClientError::Protocol)
    }

    pub async fn shutdown(&mut self) -> Result<(), ClientError> {
        self.writer
            .shutdown()
            .await
            .map_err(|_| ClientError::TransportClosed)
    }

    pub async fn finish(self) -> Result<(), ClientError> {
        let RemoteSession { writer, child, .. } = self;
        drop(writer);
        let Some(mut child) = child else {
            return Ok(());
        };
        let status = match tokio::time::timeout(SSH_EXIT_TIMEOUT, child.wait()).await {
            Ok(result) => result.map_err(|_| ClientError::SshExit)?,
            Err(_) => {
                let _ = child.kill().await;
                return Err(ClientError::SshExit);
            }
        };
        if status.success() {
            Ok(())
        } else {
            Err(ClientError::SshExit)
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn finish_closes_child_stdin_before_waiting_for_exit() {
        let mut child = Command::new("/bin/sh")
            .args(["-c", "while IFS= read -r line; do :; done"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let reader = child.stdout.take().unwrap();
        let writer = child.stdin.take().unwrap();
        let mut session = RemoteSession {
            reader: NdjsonReader::new(reader),
            writer: NdjsonWriter::new(writer),
            child: Some(child),
        };

        session.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), session.finish())
            .await
            .expect("child should exit promptly after stdin EOF")
            .expect("child should exit successfully");
    }
}
