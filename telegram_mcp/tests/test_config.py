import pytest

from telegram_mcp.config import ConfigError, Settings


def test_missing_required_environment_variable_is_reported_safely() -> None:
    with pytest.raises(ConfigError, match="TELEGRAM_API_ID"):
        Settings.from_env({})


def test_non_integer_api_id_is_reported_safely() -> None:
    with pytest.raises(ConfigError, match="TELEGRAM_API_ID must be an integer"):
        Settings.from_env(
            {
                "TELEGRAM_API_ID": "not-an-integer",
                "TELEGRAM_API_HASH": "hash-secret",
                "TELETHON_SESSION_STRING": "session-secret",
            }
        )


def test_settings_repr_never_contains_secrets() -> None:
    settings = Settings.from_env(
        {
            "TELEGRAM_API_ID": "123",
            "TELEGRAM_API_HASH": "hash-secret",
            "TELETHON_SESSION_STRING": "session-secret",
        }
    )
    assert "hash-secret" not in repr(settings)
    assert "session-secret" not in repr(settings)
