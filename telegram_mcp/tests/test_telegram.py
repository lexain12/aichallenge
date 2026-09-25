from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import SimpleNamespace
import traceback

import pytest

from telegram_mcp.config import Settings
from telegram_mcp.models import ChatListResult, ReadChatResult, SendMessageResult
from telegram_mcp.telegram import TelegramToolFailure, TelethonGateway


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
        if self.dialogs_failure is not None:
            raise self.dialogs_failure
        for item in self.dialogs:
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
    sent = await gateway.send_message("me", "hello")

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


async def test_me_resolves_without_loading_dialogs(fake_client) -> None:
    fake_client.dialogs_failure = RuntimeError("unrelated dialog failure")
    gateway = TelethonGateway(settings(), client=fake_client)

    result = await gateway.send_message("me", "hello")

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

    assert options == [{"request_retries": 0, "flood_sleep_threshold": 0}]


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


async def test_exact_dialog_id_resolves_before_username_or_title(fake_client) -> None:
    fake_client.dialogs = [dialog(10, "Chosen"), dialog(20, "10", "10")]
    gateway = TelethonGateway(settings(), client=fake_client)

    result = await gateway.send_message("10", "hello")

    assert result.chat_id == "10"
    assert fake_client.sent[0][0] is fake_client.dialogs[0].entity


async def test_exact_username_resolves_before_title(fake_client) -> None:
    fake_client.dialogs = [dialog(10, "Alias", "target"), dialog(20, "TARGET")]
    gateway = TelethonGateway(settings(), client=fake_client)

    result = await gateway.send_message("@TaRgEt", "hello")

    assert result.chat_id == "10"
    assert fake_client.sent[0][0] is fake_client.dialogs[0].entity


async def test_exact_title_is_case_insensitive_and_not_fuzzy(fake_client) -> None:
    fake_client.dialogs = [dialog(10, "Team Updates")]
    gateway = TelethonGateway(settings(), client=fake_client)

    result = await gateway.send_message("TEAM UPDATES", "hello")
    assert result.chat_id == "10"

    with pytest.raises(TelegramToolFailure) as caught:
        await gateway.send_message("Team", "never")
    assert caught.value.code == "chat_not_found"
    assert len(fake_client.sent) == 1


async def test_duplicate_title_is_ambiguous_and_never_sends(fake_client) -> None:
    fake_client.dialogs = [dialog(10, "Team"), dialog(20, "team")]
    gateway = TelethonGateway(settings(), client=fake_client)

    with pytest.raises(TelegramToolFailure) as caught:
        await gateway.send_message("TEAM", "do not deliver")

    assert caught.value.code == "ambiguous_chat"
    assert [candidate.chat_id for candidate in caught.value.candidates] == ["10", "20"]
    assert "do not deliver" not in str(caught.value)
    assert fake_client.sent == []


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
