"""Expose the Telegram gateway through three MCP tools on loopback HTTP."""

import json
import logging
from typing import Annotated, Awaitable, TypeVar

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import AfterValidator, Field

from telegram_mcp.config import Settings
from telegram_mcp.models import ChatListResult, ReadChatResult, SendMessageResult
from telegram_mcp.telegram import TelegramGateway, TelegramToolFailure, TelethonGateway


_T = TypeVar("_T")
_SAFE_ERROR_CODES = {
    "chat_not_found",
    "delivery_unknown",
    "rate_limited",
    "telegram_unauthorized",
}
ListLimit = Annotated[int, Field(ge=1, le=200)]
ReadLimit = Annotated[int, Field(ge=1, le=100)]
ChatId = Annotated[str, Field(pattern=r"^-?[0-9]+$")]


def non_blank(value: str) -> str:
    if not value.strip():
        raise ValueError("message text must not be blank")
    return value


MessageText = Annotated[
    str,
    Field(min_length=1, max_length=4096),
    AfterValidator(non_blank),
]


class _SafeMcpLogHandler(logging.Handler):
    """Replace SDK diagnostics before they can reach ordinary log handlers."""

    def emit(self, record: logging.LogRecord) -> None:
        tool = "unknown"
        if record.name == "mcp.server.mcpserver.server" and isinstance(record.args, tuple):
            name = record.args[0] if record.args else None
            if isinstance(name, str) and name in {"list_chats", "read_chat", "send_message"}:
                tool = name
        code = "unexpected_error" if record.exc_info else "sdk_event"
        # Build a fresh record: never forward message, args, exception chains,
        # cached traceback text, stack info, or arbitrary SDK extra fields.
        safe_record = logging.LogRecord(
            "telegram_mcp.sdk", record.levelno, __file__, 0,
            "MCP event tool=%s code=%s", (tool, code), None,
        )
        # Forward to handlers directly; logging again inside emit() is suppressed
        # by Python's logger reentrancy protection on supported runtimes.
        logging.getLogger("telegram_mcp.sdk").callHandlers(safe_record)


def _configure_safe_mcp_logging() -> None:
    # The pinned SDK logs full ToolError text at INFO and crashes with
    # logger.exception(). A higher root level alone does not protect embedded
    # use with existing logging configuration. Intercept the SDK namespace.
    logger = logging.getLogger("mcp")
    logger.handlers = [_SafeMcpLogHandler()]
    logger.propagate = False


async def _safe_call(operation: Awaitable[_T]) -> _T:
    try:
        return await operation
    except TelegramToolFailure as error:
        code = error.code if error.code in _SAFE_ERROR_CODES else "unexpected_error"
        # Only an allowlisted code crosses the MCP boundary. Telegram details,
        # peer metadata and exception text remain inside the local server.
        raise ToolError(json.dumps({
            "mcp_error": {"version": 1, "code": code},
        })) from None


def build_server(gateway: TelegramGateway) -> MCPServer:
    _configure_safe_mcp_logging()
    server = MCPServer("telegram-mcp")

    @server.tool(annotations=ToolAnnotations(read_only_hint=True))
    async def list_chats(query: str | None = None, limit: ListLimit = 100) -> ChatListResult:
        """Find chats and return the chat_id required by read_chat and send_message."""
        return await _safe_call(gateway.list_chats(query, limit))

    @server.tool(annotations=ToolAnnotations(read_only_hint=True))
    async def read_chat(chat_id: ChatId, limit: ReadLimit = 20) -> ReadChatResult:
        """Read messages using a chat_id returned by list_chats."""
        return await _safe_call(gateway.read_chat(chat_id, limit))

    @server.tool(annotations=ToolAnnotations(read_only_hint=False))
    async def send_message(chat_id: ChatId, text: MessageText) -> SendMessageResult:
        """Send plain text using a chat_id returned by list_chats."""
        return await _safe_call(gateway.send_message(chat_id, text))

    return server


def main() -> None:
    settings = Settings.from_env()
    build_server(TelethonGateway(settings)).run(
        transport="streamable-http",
        host=settings.host,
        port=settings.port,
        streamable_http_path=settings.path,
    )
