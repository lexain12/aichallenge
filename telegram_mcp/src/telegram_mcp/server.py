"""Expose the Telegram gateway through three MCP tools on loopback HTTP."""

import json
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


async def _safe_call(operation: Awaitable[_T]) -> _T:
    try:
        return await operation
    except TelegramToolFailure as error:
        raise ToolError(json.dumps({
            "code": error.code,
            "message": str(error),
            "candidates": [candidate.model_dump(mode="json") for candidate in error.candidates],
        }, ensure_ascii=False)) from None


def build_server(gateway: TelegramGateway) -> MCPServer:
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
