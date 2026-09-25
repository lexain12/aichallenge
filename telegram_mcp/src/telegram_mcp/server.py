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
ListLimit = Annotated[int, Field(ge=1, le=200)]
ReadLimit = Annotated[int, Field(ge=1, le=100)]
ChatName = Annotated[str, Field(min_length=1)]


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
        if error.code == "delivery_unknown":
            # Service-neutral uncertainty contract: no remote message or
            # candidates may be included in this strict versioned envelope.
            raise ToolError(json.dumps({
                "mcp_error": {"version": 1, "code": "delivery_unknown"},
            })) from None
        raise ToolError(json.dumps({
            "code": error.code,
            "message": str(error),
            "candidates": [candidate.model_dump(mode="json") for candidate in error.candidates],
        }, ensure_ascii=False)) from None


def build_server(gateway: TelegramGateway) -> MCPServer:
    _configure_safe_mcp_logging()
    server = MCPServer("telegram-mcp")

    @server.tool(annotations=ToolAnnotations(read_only_hint=True))
    async def list_chats(query: str | None = None, limit: ListLimit = 100) -> ChatListResult:
        return await _safe_call(gateway.list_chats(query, limit))

    @server.tool(annotations=ToolAnnotations(read_only_hint=True))
    async def read_chat(chat: ChatName, limit: ReadLimit = 20) -> ReadChatResult:
        return await _safe_call(gateway.read_chat(chat, limit))

    @server.tool(annotations=ToolAnnotations(read_only_hint=False))
    async def send_message(chat: ChatName, text: MessageText) -> SendMessageResult:
        return await _safe_call(gateway.send_message(chat, text))

    return server


def main() -> None:
    settings = Settings.from_env()
    build_server(TelethonGateway(settings)).run(
        transport="streamable-http",
        host=settings.host,
        port=settings.port,
        streamable_http_path=settings.path,
    )
