use std::io::Write;

use deepseek_cli::settings::{ClientSettings, ServerSettings};
use tempfile::NamedTempFile;

fn config(contents: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file
}

#[test]
fn server_settings_reject_legacy_compaction_sections() {
    for section in ["context", "workflow", "memory", "profile"] {
        let file = config(&format!("[provider]\napi_key = 'key'\n[{section}]\n"));
        assert!(
            ServerSettings::load(file.path(), None).is_err(),
            "accepted {section}"
        );
    }
}

#[test]
fn server_settings_default_to_exact_limits_and_moscow() {
    let file = config("[provider]\napi_key = 'key'\n");
    let settings = ServerSettings::load(file.path(), None).unwrap();
    assert_eq!(settings.max_provider_request_bytes(), 524_288);
    assert_eq!(settings.max_message_bytes(), 262_144);
    assert_eq!(settings.mcp().max_tool_rounds(), 8);
    assert_eq!(settings.scheduler().confirmation_timeout_minutes(), 5);
    assert_eq!(settings.scheduler().timezone().to_string(), "Europe/Moscow");
    assert_eq!(
        settings.scheduler().lock_path(),
        std::path::Path::new("/var/lib/light-agent/light-agent.sqlite3.cron.lock")
    );
    assert_eq!(
        settings.scheduler().crontab_binary(),
        std::path::Path::new("/usr/bin/crontab")
    );
}

#[test]
fn server_settings_load_explicit_scheduler_runtime_defaults() {
    let file = config(
        "[provider]\napi_key = 'key'\n[scheduler]\ntimezone = 'UTC'\nconfirmation_timeout_minutes = 7\n",
    );
    let settings = ServerSettings::load(file.path(), None).unwrap();
    assert_eq!(settings.scheduler().timezone().to_string(), "UTC");
    assert_eq!(settings.scheduler().confirmation_timeout_minutes(), 7);
    assert_eq!(
        settings.scheduler().confirmation_timeout(),
        std::time::Duration::from_secs(7 * 60)
    );
}

#[test]
fn omitted_scheduler_lock_is_derived_beside_the_configured_database() {
    let file = config(
        "[provider]\napi_key = 'key'\n[database]\npath = '/srv/light-agent/state.sqlite3'\n",
    );
    let settings = ServerSettings::load(file.path(), None).unwrap();
    assert_eq!(
        settings.scheduler().lock_path(),
        std::path::Path::new("/srv/light-agent/state.sqlite3.cron.lock")
    );
}

#[test]
fn scheduler_paths_reject_crontab_command_injection() {
    for (field, value) in [
        ("binary_path", "/opt/light agent/bin/light-agent"),
        ("binary_path", "/opt/light-agent/bin/light-agent;id"),
        ("crontab_binary", "crontab"),
        ("crontab_binary", "/usr/bin/crontab\n--help"),
    ] {
        let file = config(&format!(
            "[provider]\napi_key = 'key'\n[scheduler]\n{field} = {value:?}\n"
        ));
        assert!(
            ServerSettings::load(file.path(), None).is_err(),
            "accepted {value:?}"
        );
    }
}

#[test]
fn client_settings_contain_only_ssh_transport_fields() {
    let file = config(
        "ssh_binary = 'ssh'\nssh_host = 'my-vm'\nremote_command = '/opt/light-agent/bin/light-agent serve-stdio'\n",
    );
    let settings = ClientSettings::load(file.path()).unwrap();
    assert_eq!(settings.ssh_binary().to_string_lossy(), "ssh");
    assert_eq!(settings.ssh_host(), "my-vm");
    assert_eq!(
        settings.remote_command(),
        "/opt/light-agent/bin/light-agent serve-stdio"
    );

    for extra in [
        "api_key = 'secret'",
        "[provider]\napi_key = 'secret'",
        "[mcp]",
    ] {
        let file = config(&format!("ssh_host = 'my-vm'\n{extra}\n"));
        assert!(
            ClientSettings::load(file.path()).is_err(),
            "accepted {extra}"
        );
    }
    let file = config("ssh_host = 'my-vm'\nremote_command = 'sh -c id'\n");
    assert!(ClientSettings::load(file.path()).is_err());
}

#[test]
fn settings_debug_redacts_provider_and_mcp_secrets() {
    let file = config(
        "[provider]\napi_key = 'provider-secret'\n[[mcp.servers]]\nname = 'telegram'\nurl = 'http://localhost:8000/mcp'\nbearer_token = 'mcp-secret'\n",
    );
    let settings = ServerSettings::load(file.path(), None).unwrap();
    let debug = format!(
        "{settings:?} {:?} {:?}",
        settings.provider(),
        settings.mcp()
    );
    assert!(!debug.contains("provider-secret"));
    assert!(!debug.contains("mcp-secret"));
    assert_eq!(settings.provider().api_key(), "provider-secret");
    assert_eq!(
        settings.mcp().servers()[0].bearer_token(),
        Some("mcp-secret")
    );
}

#[test]
fn settings_debug_does_not_expose_prompt_or_url_query_data() {
    let file = config(
        "[provider]\napi_key = 'key'\nbase_url = 'https://example.com/?token=url-secret'\n[prompts]\ninteractive_system = 'prompt-secret'\n",
    );
    let settings = ServerSettings::load(file.path(), None).unwrap();
    let debug = format!("{settings:?}");
    assert!(!debug.contains("url-secret"));
    assert!(!debug.contains("prompt-secret"));
}

#[test]
fn server_settings_reject_urls_with_userinfo() {
    let provider =
        config("[provider]\napi_key = 'key'\nbase_url = 'https://user:pass@example.com'\n");
    assert!(ServerSettings::load(provider.path(), None).is_err());
    let mcp = config(
        "[provider]\napi_key = 'key'\n[[mcp.servers]]\nname = 'x'\nurl = 'https://user:pass@example.com/mcp'\n",
    );
    assert!(ServerSettings::load(mcp.path(), None).is_err());
}

#[test]
fn server_settings_reject_empty_url_userinfo() {
    for url in [
        "https://@example.com",
        "https://:@example.com",
        "https://:pass@example.com",
        "http://@localhost:8080/api",
    ] {
        let provider = config(&format!(
            "[provider]\napi_key = 'key'\nbase_url = '{url}'\n"
        ));
        assert!(
            ServerSettings::load(provider.path(), None).is_err(),
            "provider accepted {url}"
        );
    }

    for url in [
        "https://@example.com/mcp",
        "https://:@example.com/mcp",
        "https://:pass@example.com/mcp",
        "http://@localhost:8080/mcp",
    ] {
        let mcp = config(&format!(
            "[provider]\napi_key = 'key'\n[[mcp.servers]]\nname = 'x'\nurl = '{url}'\n"
        ));
        assert!(
            ServerSettings::load(mcp.path(), None).is_err(),
            "MCP accepted {url}"
        );
    }

    let path_and_query = config(
        "[provider]\napi_key = 'key'\nbase_url = 'https://example.com/path/@name'\n[[mcp.servers]]\nname = 'x'\nurl = 'https://example.com/mcp?contact=@name'\n",
    );
    assert!(ServerSettings::load(path_and_query.path(), None).is_ok());
}

#[test]
fn uppercase_http_schemes_preserve_userinfo_validation() {
    let safe = config(
        "[provider]\napi_key = 'key'\nbase_url = 'HTTPS://example.com/api'\n[[mcp.servers]]\nname = 'x'\nurl = 'HTTP://localhost:8080/mcp'\n",
    );
    assert!(ServerSettings::load(safe.path(), None).is_ok());

    for url in ["HTTPS://user@example.com/api", "HTTP://@localhost:8080/mcp"] {
        let file = config(&format!(
            "[provider]\napi_key = 'key'\nbase_url = '{url}'\n"
        ));
        assert!(ServerSettings::load(file.path(), None).is_err());
    }
}
