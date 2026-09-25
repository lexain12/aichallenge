import traceback

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
    assert "123" not in repr(settings)


def test_invalid_api_id_is_absent_from_formatted_traceback() -> None:
    invalid_api_id = "sensitive-invalid-api-id"
    with pytest.raises(ConfigError) as error:
        Settings.from_env(
            {
                "TELEGRAM_API_ID": invalid_api_id,
                "TELEGRAM_API_HASH": "hash-secret",
                "TELETHON_SESSION_STRING": "session-secret",
            }
        )

    formatted_traceback = "".join(traceback.format_exception(error.value))
    assert invalid_api_id not in formatted_traceback


def test_endpoint_values_cannot_be_overridden_in_constructor() -> None:
    with pytest.raises(TypeError, match="unexpected keyword argument 'host'"):
        Settings(
            123,
            "hash-secret",
            "session-secret",
            host="0.0.0.0",
        )

    settings = Settings.from_env(
        {
            "TELEGRAM_API_ID": "123",
            "TELEGRAM_API_HASH": "hash-secret",
            "TELETHON_SESSION_STRING": "session-secret",
        }
    )
    assert (settings.host, settings.port, settings.path) == ("127.0.0.1", 8000, "/mcp")
