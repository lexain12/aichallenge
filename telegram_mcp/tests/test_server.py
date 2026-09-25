"""The MCP boundary uses the SDK's in-memory client, never a socket."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

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


async def test_ambiguous_candidates_remain_in_response_but_never_in_logs(fake_gateway, caplog) -> None:
    private_chat = ChatSummary(
        chat_id="-1009876543210123", title="PRIVATE_CANDIDATE_TITLE",
        username="PRIVATE_CANDIDATE_USERNAME", kind="group", is_self=False,
    )
    fake_gateway.failure = TelegramToolFailure(
        "ambiguous_chat", "Multiple chats match; choose a chat_id", [private_chat]
    )
    with caplog.at_level(logging.INFO):
        async with Client(build_server(fake_gateway)) as client:
            result = await client.call_tool("send_message", {
                "chat": "PRIVATE_ARGUMENT_CHAT", "text": "PRIVATE_ARGUMENT_MESSAGE",
            })

    assert result.is_error is True
    payload = json.loads(result.content[0].text.partition(": ")[2])
    assert payload["candidates"] == [private_chat.model_dump(mode="json")]
    assert payload["code"] == "ambiguous_chat"
    assert caplog.records, "the SDK diagnostic should retain safe metadata"
    for sentinel in [
        private_chat.chat_id, private_chat.title, private_chat.username,
        "PRIVATE_ARGUMENT_CHAT", "PRIVATE_ARGUMENT_MESSAGE", "candidates",
    ]:
        assert sentinel not in caplog.text
    assert "send_message" in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


@pytest.mark.parametrize("failure_source", ["gateway", "sdk_result_conversion"])
async def test_unexpected_exceptions_never_log_details_or_tracebacks(
    fake_gateway, caplog, monkeypatch, failure_source
) -> None:
    server = build_server(fake_gateway)
    sentinel = "PRIVATE_UNEXPECTED_EXCEPTION_DETAIL"
    if failure_source == "gateway":
        fake_gateway.failure = RuntimeError(sentinel)
    else:
        tool = server._tool_manager.get_tool("read_chat")

        def broken_conversion(self, result):
            raise RuntimeError(sentinel)

        monkeypatch.setattr(type(tool.fn_metadata), "convert_result", broken_conversion)

    with caplog.at_level(logging.INFO):
        async with Client(server) as client:
            result = await client.call_tool("read_chat", {"chat": "me"})

    assert result.is_error is True
    assert sentinel not in result.content[0].text
    assert caplog.records, "the SDK diagnostic should retain safe metadata"
    assert sentinel not in caplog.text
    assert "Traceback" not in caplog.text
    assert "read_chat" in caplog.text
    assert all(record.exc_info is None and record.exc_text is None for record in caplog.records)


async def test_delivery_unknown_uses_the_shared_safe_mcp_error_envelope(fake_gateway) -> None:
    fake_gateway.failure = TelegramToolFailure(
        "delivery_unknown", "private error details must not cross this boundary", [CHAT]
    )
    async with Client(build_server(fake_gateway)) as client:
        result = await client.call_tool("send_message", {"chat": "me", "text": "synthetic"})

    fixture = Path(__file__).resolve().parents[2] / "tests/fixtures/mcp_delivery_unknown.json"
    assert result.model_dump(mode="json", by_alias=True, exclude_none=True) == json.loads(
        fixture.read_text()
    )
    assert fake_gateway.calls == [("send_message", "me", "synthetic")]


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
