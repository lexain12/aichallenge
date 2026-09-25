"""Environment configuration for the Telegram MCP server."""

import os
from dataclasses import dataclass, field
from typing import Mapping


class ConfigError(ValueError):
    """Raised when required application configuration is invalid."""


@dataclass(frozen=True)
class Settings:
    api_id: int = field(repr=False)
    api_hash: str = field(repr=False)
    session_string: str = field(repr=False)
    host: str = field(default="127.0.0.1", init=False)
    port: int = field(default=8000, init=False)
    path: str = field(default="/mcp", init=False)

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> "Settings":
        required = (
            "TELEGRAM_API_ID",
            "TELEGRAM_API_HASH",
            "TELETHON_SESSION_STRING",
        )
        missing = [name for name in required if not env.get(name)]
        if missing:
            raise ConfigError(
                f"missing required environment variable: {missing[0]}"
            )

        try:
            api_id = int(env["TELEGRAM_API_ID"])
        except ValueError:
            raise ConfigError("TELEGRAM_API_ID must be an integer") from None

        return cls(
            api_id=api_id,
            api_hash=env["TELEGRAM_API_HASH"],
            session_string=env["TELETHON_SESSION_STRING"],
        )
