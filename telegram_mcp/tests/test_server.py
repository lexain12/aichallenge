"""The MCP boundary uses the SDK's in-memory client, never a socket."""

import json
from datetime import datetime, timezone

import pytest
from mcp.client import Client

from telegram_mcp.config import Settings
from telegram_mcp.models import (
    ChatListResult,
    ChatRef,
    ChatSummary,
    ReadChatResult,
    ReadMessage,
    SendMessageResult,
)
from telegram_mcp.server import build_server, main
from telegram_mcp.telegram import TelegramToolFailure


NOW = datetime(2026, 9, 25, 10, 30, tzinfo=timezone.utc)
CHAT = ChatSummary(
    chat_id="7", title="Saved Messages", username="owner", kind="private", is_self=True
)


class FakeGateway:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.failure: Exception | None = None

    async def list_chats(self, query: str | None, limit: int) -> ChatListResult:
        self.calls.append(("list_chats", query, limit))
        if self.failure:
            raise self.failure
        return ChatListResult(chats=[CHAT])

    async def read_chat(self, chat: str, limit: int) -> ReadChatResult:
        self.calls.append(("read_chat", chat, limit))
        if self.failure:
            raise self.failure
        return ReadChatResult(
            chat=ChatRef(chat_id="7", title="Saved Messages"),
            messages=[ReadMessage(
                message_id=11, sent_at=NOW, sender_id="7", sender_name="Owner",
                text="hello", outgoing=True, reply_to_message_id=None,
            )],
        )

    async def send_message(self, chat: str, text: str) -> SendMessageResult:
        self.calls.append(("send_message", chat, text))
        if self.failure:
            raise self.failure
        return SendMessageResult(sent=True, chat_id="7", message_id=12, sent_at=NOW)


@pytest.fixture
def fake_gateway() -> FakeGateway:
    return FakeGateway()


async def test_server_exposes_exactly_three_tools_with_expected_schemas(fake_gateway) -> None:
    async with Client(build_server(fake_gateway)) as client:
        listed = await client.list_tools()

    tools = {tool.name: tool for tool in listed.tools}
    assert set(tools) == {"list_chats", "read_chat", "send_message"}
    assert tools["list_chats"].annotations.read_only_hint is True
    assert tools["read_chat"].annotations.read_only_hint is True
    assert tools["send_message"].annotations.read_only_hint is False

    assert set(tools["list_chats"].input_schema["properties"]) == {"query", "limit"}
    assert tools["list_chats"].input_schema["properties"]["limit"]["minimum"] == 1
    assert tools["list_chats"].input_schema["properties"]["limit"]["maximum"] == 200
    assert tools["list_chats"].input_schema["properties"]["limit"]["default"] == 100
    assert set(tools["read_chat"].input_schema["properties"]) == {"chat", "limit"}
    assert tools["read_chat"].input_schema["properties"]["limit"]["minimum"] == 1
    assert tools["read_chat"].input_schema["properties"]["limit"]["maximum"] == 100
    assert set(tools["send_message"].input_schema["properties"]) == {"chat", "text"}
    assert tools["send_message"].input_schema["properties"]["text"]["minLength"] == 1
    assert tools["send_message"].input_schema["properties"]["text"]["maxLength"] == 4096
    assert all(tool.output_schema is not None for tool in tools.values())


async def test_successful_tools_return_typed_structured_results(fake_gateway) -> None:
    async with Client(build_server(fake_gateway)) as client:
        listed = await client.call_tool("list_chats", {"query": "saved"})
        read = await client.call_tool("read_chat", {"chat": "me", "limit": 1})
        sent = await client.call_tool("send_message", {"chat": "me", "text": "hello"})

    assert fake_gateway.calls == [
        ("list_chats", "saved", 100),
        ("read_chat", "me", 1),
        ("send_message", "me", "hello"),
    ]
    assert listed.is_error is False
    assert listed.structured_content == {"chats": [CHAT.model_dump(mode="json")]}
    assert read.is_error is False
    assert read.structured_content["chat"] == {"chat_id": "7", "title": "Saved Messages"}
    assert read.structured_content["messages"][0]["sent_at"] == "2026-09-25T10:30:00Z"
    assert sent.is_error is False
    assert sent.structured_content == {
        "sent": True, "chat_id": "7", "message_id": 12,
        "sent_at": "2026-09-25T10:30:00Z",
    }


async def test_list_chats_accepts_150_and_passes_it_to_gateway(fake_gateway) -> None:
    async with Client(build_server(fake_gateway)) as client:
        result = await client.call_tool("list_chats", {"limit": 150})

    assert result.is_error is False
    assert fake_gateway.calls == [("list_chats", None, 150)]


@pytest.mark.parametrize("tool,args", [
    ("list_chats", {"limit": 0}),
    ("list_chats", {"limit": 201}),
    ("read_chat", {"chat": "me", "limit": 0}),
    ("read_chat", {"chat": "me", "limit": 101}),
    ("read_chat", {"chat": ""}),
    ("send_message", {"chat": "", "text": "hello"}),
    ("send_message", {"chat": "me", "text": ""}),
    ("send_message", {"chat": "me", "text": " \t\n "}),
    ("send_message", {"chat": "me", "text": "x" * 4097}),
])
async def test_invalid_arguments_never_reach_gateway(fake_gateway, tool, args) -> None:
    async with Client(build_server(fake_gateway)) as client:
        result = await client.call_tool(tool, args)

    assert result.is_error is True
    assert fake_gateway.calls == []


async def test_known_gateway_failure_is_a_safe_tool_error(fake_gateway) -> None:
    fake_gateway.failure = TelegramToolFailure(
        "ambiguous_chat", "Multiple chats match; choose a chat_id", [CHAT]
    )

    async with Client(build_server(fake_gateway)) as client:
        result = await client.call_tool("send_message", {"chat": "Saved Messages", "text": "hello"})

    assert result.is_error is True
    prefix, separator, payload = result.content[0].text.partition(": ")
    assert (prefix, separator) == ("Error executing tool send_message", ": ")
    error = json.loads(payload)
    assert error == {
        "code": "ambiguous_chat",
        "message": "Multiple chats match; choose a chat_id",
        "candidates": [CHAT.model_dump(mode="json")],
    }
    assert fake_gateway.calls == [("send_message", "Saved Messages", "hello")]


async def test_unexpected_gateway_failure_is_not_presented_as_known_error(fake_gateway) -> None:
    fake_gateway.failure = RuntimeError("private gateway secret")

    async with Client(build_server(fake_gateway)) as client:
        result = await client.call_tool("read_chat", {"chat": "me"})

    assert result.is_error is True
    assert "private gateway secret" not in result.content[0].text
    assert "ambiguous_chat" not in result.content[0].text


def test_main_runs_fixed_loopback_streamable_http(monkeypatch) -> None:
    import telegram_mcp.server as server_module

    settings = Settings(123, "secret-hash", "secret-session")
    observed = {}
    monkeypatch.setattr(server_module.Settings, "from_env", lambda: settings)
    monkeypatch.setattr(server_module, "TelethonGateway", lambda value: observed.setdefault("settings", value))
    monkeypatch.setattr(server_module.MCPServer, "run", lambda self, **kwargs: observed.setdefault("run", kwargs))

    main()

    assert observed["settings"] is settings
    assert observed["run"] == {
        "transport": "streamable-http", "host": "127.0.0.1", "port": 8000,
        "streamable_http_path": "/mcp",
    }
