import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import SimpleNamespace
import traceback

import pytest

from telegram_mcp.config import Settings
from telegram_mcp.models import ChatListResult, ReadChatResult, SendMessageResult
from telegram_mcp.telegram import TelegramToolFailure, TelethonGateway
from telethon.crypto.authkey import AuthKey
from telethon.sessions import StringSession


NOW = datetime(2026, 9, 25, 10, 30, tzinfo=timezone.utc)


def settings() -> Settings:
    return Settings(123, "hash-secret", "session-secret")


def user(id: int, name: str, username: str | None = None, bot: bool = False):
    return SimpleNamespace(
        id=id,
        first_name=name,
        last_name=None,
        username=username,
        bot=bot,
    )


def dialog(id: int, name: str, username: str | None = None, *, kind: str = "private"):
    entity = SimpleNamespace(
        id=abs(id),
        username=username,
        bot=kind == "bot",
        broadcast=kind == "channel",
        megagroup=kind == "group",
    )
    return SimpleNamespace(
        id=id,
        name=name,
        entity=entity,
        is_group=kind == "group",
        is_channel=kind == "channel",
        is_user=kind in {"private", "bot"},
    )


@dataclass
class FakeMessage:
    id: int
    text: str | None = "hello"
    date: datetime = NOW
    sender_id: int | None = 20
    sender: object | None = field(default_factory=lambda: user(20, "Alice"))
    out: bool = False
    reply_to_msg_id: int | None = None

    async def get_sender(self):
        return self.sender


class FakeClient:
    def __init__(self):
        self.me = user(7, "Owner", "owner")
        self.dialogs = []
        self.messages = [FakeMessage(3), FakeMessage(2), FakeMessage(1)]
        self.connected = False
        self.authorized = True
        self.connect_calls = 0
        self.get_me_calls = 0
        self.iter_dialogs_calls = 0
        self.iter_dialogs_yields = 0
        self.sent = []
        self.failure = None
        self.dialogs_failure = None

    def is_connected(self):
        return self.connected

    async def connect(self):
        self.connect_calls += 1
        self.connected = True

    async def is_user_authorized(self):
        return self.authorized

    async def get_me(self):
        self.get_me_calls += 1
        return self.me

    async def iter_dialogs(self):
        self.iter_dialogs_calls += 1
        if self.dialogs_failure is not None:
            raise self.dialogs_failure
        for item in self.dialogs:
            self.iter_dialogs_yields += 1
            yield item

    async def iter_messages(self, entity, *, limit):
        for item in self.messages[:limit]:
            yield item

    async def send_message(self, entity, text, *, parse_mode):
        self.sent.append((entity, text, parse_mode))
        if self.failure is not None:
            raise self.failure
        return FakeMessage(42, text=text, date=NOW, sender_id=self.me.id, out=True)


@pytest.fixture
def fake_client():
    return FakeClient()


async def test_saved_messages_use_get_me_even_without_a_dialog(fake_client) -> None:
    gateway = TelethonGateway(settings(), client=fake_client)

    listed = await gateway.list_chats(None, 10)
    sent = await gateway.send_message(listed.chats[0].chat_id, "hello")

    assert isinstance(listed, ChatListResult)
    assert listed.model_dump(mode="json") == {
        "chats": [{
            "chat_id": "7", "title": "Saved Messages", "username": "owner",
            "kind": "private", "is_self": True,
        }]
    }
    assert fake_client.sent == [(fake_client.me, "hello", None)]
    assert sent.chat_id == "7"
    assert fake_client.get_me_calls == 1


async def test_self_chat_id_resolves_without_loading_dialogs(fake_client) -> None:
    fake_client.dialogs_failure = RuntimeError("unrelated dialog failure")
    gateway = TelethonGateway(settings(), client=fake_client)

    result = await gateway.send_message("7", "hello")

    assert result.chat_id == "7"
    assert fake_client.sent == [(fake_client.me, "hello", None)]


async def test_lazy_client_is_constructed_once_and_connected_once(monkeypatch) -> None:
    import telegram_mcp.telegram as telegram

    created = FakeClient()
    calls = []
    monkeypatch.setattr(telegram, "StringSession", lambda value: ("session", value))
    monkeypatch.setattr(
        telegram,
        "TelegramClient",
        lambda session, api_id, api_hash, **kwargs: calls.append((session, api_id, api_hash)) or created,
    )
    gateway = TelethonGateway(settings())
    assert calls == []

    await gateway.list_chats(None, 10)
    await gateway.list_chats(None, 10)

    assert calls == [(("session", "session-secret"), 123, "hash-secret")]
    assert created.connect_calls == 1


async def test_relay_changes_only_session_endpoint_before_client_construction(monkeypatch) -> None:
    import telegram_mcp.telegram as telegram

    original = StringSession()
    original.set_dc(4, "192.0.2.42", 443)
    original.auth_key = AuthKey(bytes([42]) * 256)
    session_string = original.save()
    captured = []
    dc_calls = []
    created = FakeClient()

    class TrackingSession(StringSession):
        def set_dc(self, dc_id, server_address, port):
            dc_calls.append((dc_id, server_address, port))
            super().set_dc(dc_id, server_address, port)

    def client_factory(session, api_id, api_hash, **kwargs):
        captured.append((session.dc_id, session.server_address, session.port, session.auth_key.key))
        return created

    monkeypatch.setattr(telegram, "StringSession", TrackingSession)
    monkeypatch.setattr(telegram, "TelegramClient", client_factory)
    configured = Settings.from_env(
        {
            "TELEGRAM_API_ID": "123",
            "TELEGRAM_API_HASH": "hash-secret",
            "TELETHON_SESSION_STRING": session_string,
            "TELEGRAM_RELAY_PORT": "18082",
        }
    )
    gateway = TelethonGateway(configured)

    await gateway.list_chats(None, 1)

    assert dc_calls == [(4, "127.0.0.1", 18082)]
    assert captured == [(4, "127.0.0.1", 18082, bytes([42]) * 256)]


async def test_without_relay_preserves_original_session_endpoint(monkeypatch) -> None:
    import telegram_mcp.telegram as telegram

    original = StringSession()
    original.set_dc(4, "192.0.2.42", 443)
    original.auth_key = AuthKey(bytes([42]) * 256)
    captured = []

    def client_factory(session, api_id, api_hash, **kwargs):
        captured.append((session.dc_id, session.server_address, session.port, session.auth_key.key))
        return FakeClient()

    monkeypatch.setattr(telegram, "TelegramClient", client_factory)
    gateway = TelethonGateway(Settings(123, "hash-secret", original.save()))

    await gateway.list_chats(None, 1)

    assert captured == [(4, "192.0.2.42", 443, bytes([42]) * 256)]


async def test_real_client_disables_internal_send_retries(monkeypatch) -> None:
    import telegram_mcp.telegram as telegram

    created = FakeClient()
    options = []
    monkeypatch.setattr(telegram, "StringSession", lambda value: ("session", value))

    def client_factory(session, api_id, api_hash, **kwargs):
        options.append(kwargs)
        return created

    monkeypatch.setattr(telegram, "TelegramClient", client_factory)
    gateway = TelethonGateway(settings())

    await gateway.list_chats(None, 1)

    assert options[0]["request_retries"] == 0
    assert options[0]["flood_sleep_threshold"] == 0


@pytest.mark.parametrize("cancel_waiter", [False, True])
async def test_dispatched_write_is_not_requeued_after_connection_loss(monkeypatch, cancel_waiter) -> None:
    """Catch transport replay even after the caller stops waiting for its write."""
    import telegram_mcp.telegram as telegram
    from telethon import TelegramClient
    from telethon.tl.functions.messages import SendMessageRequest
    from telethon.tl.types import User

    class OfflineConnection:
        def __init__(self):
            self.connect_calls = 0
            self.dispatched = []
            self.sent = asyncio.Event()
            self.receive_errors = asyncio.Queue()
            self._connected = False

        async def connect(self, **kwargs):
            self.connect_calls += 1
            self._connected = True

        async def disconnect(self):
            self._connected = False

        async def send(self, data):
            self.dispatched.append(data)
            self.sent.set()

        async def recv(self):
            raise await self.receive_errors.get()

    connection = OfflineConnection()

    class OfflineTelegramClient(TelegramClient):
        async def connect(self):
            # Only authentication/handshake and the socket are replaced. The
            # request, packer, send/receive loops and reconnect scheduler are real.
            self._sender.auth_key.key = bytes([1]) * 256
            await self._sender.connect(connection)

        async def is_user_authorized(self):
            return True

        async def get_me(self, input_peer=False):
            return User(7, is_self=True, first_name="Owner")

    monkeypatch.setattr(telegram, "TelegramClient", OfflineTelegramClient)
    gateway = TelethonGateway(Settings(123, "synthetic-hash", ""))
    task = asyncio.create_task(gateway.send_message("7", "synthetic pending write"))
    try:
        await asyncio.wait_for(connection.sent.wait(), timeout=1)
        sender = gateway._client._sender
        pending = list(sender._pending_state.values())
        assert len(pending) == 1
        assert isinstance(pending[0].request, SendMessageRequest)
        if cancel_waiter:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        receive_loop = sender._recv_loop_handle
        connection.receive_errors.put_nowait(ConnectionError("synthetic lost response"))
        await asyncio.wait_for(receive_loop, timeout=1)
        await asyncio.wait_for(sender._reconnect_task, timeout=1)

        assert connection.connect_calls == 1, "connection loss must not reconnect and replay"
        assert len(connection.dispatched) == 1
        assert not sender._pending_state
        assert not sender._send_queue._deque
        assert not gateway._client.is_connected()
        if not cancel_waiter:
            with pytest.raises(TelegramToolFailure) as caught:
                await asyncio.wait_for(task, timeout=1)
            assert caught.value.code == "delivery_unknown"
            assert "synthetic lost response" not in str(caught.value)
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError, TelegramToolFailure):
            await task
        if gateway._client is not None:
            await gateway._client._sender.disconnect()
            # Consume the sender's terminal error without running client updates.
            if gateway._client._sender._disconnected.done():
                gateway._client._sender._disconnected.exception()


async def test_unauthorized_session_fails_before_reading_dialogs(fake_client) -> None:
    fake_client.authorized = False
    gateway = TelethonGateway(settings(), client=fake_client)

    with pytest.raises(TelegramToolFailure) as caught:
        await gateway.list_chats(None, 10)

    assert caught.value.code == "telegram_unauthorized"
    assert fake_client.get_me_calls == 0


async def test_list_chats_filters_title_and_username_case_insensitively(fake_client) -> None:
    fake_client.dialogs = [
        dialog(-1001, "Tech News", kind="channel"),
        dialog(-10, "Friends", "TECHfriends", kind="group"),
        dialog(20, "Other", kind="bot"),
    ]
    gateway = TelethonGateway(settings(), client=fake_client)

    result = await gateway.list_chats("tech", 2)

    assert [chat.model_dump(mode="json") for chat in result.chats] == [
        {"chat_id": "-1001", "title": "Tech News", "username": None,
         "kind": "channel", "is_self": False},
        {"chat_id": "-10", "title": "Friends", "username": "TECHfriends",
         "kind": "group", "is_self": False},
    ]


async def test_list_chats_stops_before_loading_unneeded_dialogs(fake_client) -> None:
    fake_client.dialogs = [dialog(10, "Team"), dialog(20, "Other")]
    gateway = TelethonGateway(settings(), client=fake_client)

    result = await gateway.list_chats(None, 1)

    assert [chat.chat_id for chat in result.chats] == ["7"]
    assert fake_client.iter_dialogs_yields == 0


async def test_listed_chat_id_reuses_entity_without_reloading_dialogs(fake_client) -> None:
    fake_client.dialogs = [dialog(10, "Team")]
    gateway = TelethonGateway(settings(), client=fake_client)

    listed = await gateway.list_chats("team", 1)
    result = await gateway.read_chat(listed.chats[0].chat_id, 1)

    assert result.chat.chat_id == "10"
    assert fake_client.iter_dialogs_calls == 1


async def test_non_id_selector_is_rejected_without_loading_dialogs(fake_client) -> None:
    fake_client.dialogs = [dialog(10, "Team", "target")]
    gateway = TelethonGateway(settings(), client=fake_client)

    with pytest.raises(TelegramToolFailure) as caught:
        await gateway.send_message("@target", "never")

    assert caught.value.code == "chat_not_found"
    assert fake_client.iter_dialogs_calls == 0
    assert fake_client.sent == []


async def test_uncached_dialog_id_refreshes_dialogs_once(fake_client) -> None:
    fake_client.dialogs = [dialog(10, "Chosen"), dialog(20, "10", "10")]
    gateway = TelethonGateway(settings(), client=fake_client)

    result = await gateway.send_message("10", "hello")

    assert result.chat_id == "10"
    assert fake_client.sent[0][0] is fake_client.dialogs[0].entity
    assert fake_client.iter_dialogs_calls == 1
    assert fake_client.iter_dialogs_yields == 1


async def test_concurrent_uncached_ids_share_one_dialog_scan(fake_client) -> None:
    scan_started = asyncio.Event()
    release_scan = asyncio.Event()

    async def blocked_iter_dialogs():
        fake_client.iter_dialogs_calls += 1
        scan_started.set()
        await release_scan.wait()
        for item in fake_client.dialogs:
            fake_client.iter_dialogs_yields += 1
            yield item

    fake_client.dialogs = [dialog(10, "Team")]
    fake_client.iter_dialogs = blocked_iter_dialogs
    gateway = TelethonGateway(settings(), client=fake_client)

    first = asyncio.create_task(gateway.read_chat("999", 1))
    await scan_started.wait()
    second = asyncio.create_task(gateway.read_chat("999", 1))
    await asyncio.sleep(0)
    release_scan.set()
    results = await asyncio.gather(first, second, return_exceptions=True)

    assert all(
        isinstance(result, TelegramToolFailure) and result.code == "chat_not_found"
        for result in results
    )
    assert fake_client.iter_dialogs_calls == 1


async def test_repeated_missing_chat_id_does_not_rescan_dialogs(fake_client) -> None:
    fake_client.dialogs = [dialog(10, "Team")]
    gateway = TelethonGateway(settings(), client=fake_client)

    for _ in range(2):
        with pytest.raises(TelegramToolFailure) as caught:
            await gateway.read_chat("999", 1)
        assert caught.value.code == "chat_not_found"

    assert fake_client.iter_dialogs_calls == 1


async def test_read_chat_returns_latest_messages_oldest_first(fake_client) -> None:
    fake_client.dialogs = [dialog(10, "Team")]
    fake_client.messages = [
        FakeMessage(3, text=None, sender_id=None, sender=None),
        FakeMessage(2, text="second", reply_to_msg_id=1),
        FakeMessage(1, text="first"),
    ]
    gateway = TelethonGateway(settings(), client=fake_client)

    result = await gateway.read_chat("10", 2)

    assert isinstance(result, ReadChatResult)
    assert result.chat.model_dump() == {"chat_id": "10", "title": "Team"}
    assert [message.message_id for message in result.messages] == [2, 3]
    assert result.messages[0].sender_id == "20"
    assert result.messages[0].sender_name == "Alice"
    assert result.messages[0].reply_to_message_id == 1
    assert result.messages[1].text == ""
    assert result.messages[1].sender_id is None


async def test_send_result_has_json_safe_id_and_plain_text_delivery(fake_client) -> None:
    fake_client.dialogs = [dialog(-1001, "News")]
    gateway = TelethonGateway(settings(), client=fake_client)

    result = await gateway.send_message("-1001", "<b>literal</b>")

    assert isinstance(result, SendMessageResult)
    assert result.model_dump(mode="json") == {
        "sent": True, "chat_id": "-1001", "message_id": 42,
        "sent_at": "2026-09-25T10:30:00Z",
    }
    assert fake_client.sent == [(fake_client.dialogs[0].entity, "<b>literal</b>", None)]


async def test_failed_send_has_unknown_delivery_and_is_never_retried(fake_client) -> None:
    fake_client.dialogs = [dialog(10, "Team")]
    fake_client.failure = TimeoutError("session-secret")
    gateway = TelethonGateway(settings(), client=fake_client)

    with pytest.raises(TelegramToolFailure) as caught:
        await gateway.send_message("10", "hello")

    assert caught.value.code == "delivery_unknown"
    assert "session-secret" not in str(caught.value)
    assert "session-secret" not in "".join(traceback.format_exception(caught.value))
    assert len(fake_client.sent) == 1


@pytest.mark.parametrize(
    ("failure", "expected_code"),
    [
        ("flood", "rate_limited"),
        ("slow", "rate_limited"),
        ("auth", "telegram_unauthorized"),
        ("peer", "chat_not_found"),
    ],
)
async def test_expected_telethon_send_errors_have_safe_codes(fake_client, failure, expected_code) -> None:
    from telethon import errors

    fake_client.dialogs = [dialog(10, "Team")]
    fake_client.failure = {
        "flood": errors.FloodWaitError(request=None, capture=17),
        "slow": errors.SlowModeWaitError(request=None, capture=17),
        "auth": errors.AuthKeyUnregisteredError(request=None),
        "peer": errors.PeerIdInvalidError(request=None),
    }[failure]
    gateway = TelethonGateway(settings(), client=fake_client)

    with pytest.raises(TelegramToolFailure) as caught:
        await gateway.send_message("10", "hello")

    assert caught.value.code == expected_code
    assert "request" not in str(caught.value).lower()
    assert len(fake_client.sent) == 1
