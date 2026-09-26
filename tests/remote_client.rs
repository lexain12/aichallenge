use std::ffi::{OsStr, OsString};
use std::io::Write;

use deepseek_cli::remote_client::SshTransport;
use deepseek_cli::settings::ClientSettings;
use tempfile::NamedTempFile;

fn settings(contents: &str) -> ClientSettings {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    ClientSettings::load(file.path()).unwrap()
}

#[test]
fn ssh_launch_uses_exact_argv_and_only_the_environment_allowlist() {
    let settings = settings(
        "ssh_binary = '/usr/bin/ssh'\nssh_host = 'light-agent-vm'\nremote_command = '/opt/light-agent/bin/light-agent serve-stdio'\n",
    );
    let spec = SshTransport::launch_spec(&settings);

    assert_eq!(spec.program(), OsStr::new("/usr/bin/ssh"));
    assert_eq!(
        spec.arguments(),
        [
            OsString::from("-T"),
            OsString::from("light-agent-vm"),
            OsString::from("/opt/light-agent/bin/light-agent serve-stdio"),
        ]
    );
    let allowed = [
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
    let expected = allowed
        .into_iter()
        .filter_map(|name| std::env::var_os(name).map(|value| (OsString::from(name), value)))
        .collect::<Vec<_>>();
    assert_eq!(spec.environment(), expected.as_slice());
}

#[test]
fn client_settings_reject_non_alias_hosts_and_unsafe_binary_paths() {
    for host in [
        "user@example.com",
        "example.com:22",
        "bad/alias",
        "--option",
    ] {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "ssh_host = {host:?}").unwrap();
        assert!(
            ClientSettings::load(file.path()).is_err(),
            "accepted {host:?}"
        );
    }
    for binary in ["ssh --option", "relative/ssh", "../ssh"] {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "ssh_binary = {binary:?}\nssh_host = 'vm'").unwrap();
        assert!(
            ClientSettings::load(file.path()).is_err(),
            "accepted {binary:?}"
        );
    }
}
