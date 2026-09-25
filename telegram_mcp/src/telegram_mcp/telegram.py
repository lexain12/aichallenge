"""A small, high-level Telegram gateway backed by Telethon."""

import asyncio
from dataclasses import dataclass
from typing import Protocol

from telethon import TelegramClient, errors
from telethon.sessions import StringSession

from telegram_mcp.config import Settings
from telegram_mcp.models import (
    ChatListResult,
    ChatRef,
    ChatSummary,
    ReadChatResult,
    ReadMessage,
    SendMessageResult,
)


class TelegramGateway(Protocol):
    async def list_chats(self, query: str | None, limit: int) -> ChatListResult: ...

    async def read_chat(self, chat: str, limit: int) -> ReadChatResult: ...

    async def send_message(self, chat: str, text: str) -> SendMessageResult: ...


class TelegramToolFailure(Exception):
    """A safe error for the MCP boundary; never holds a Telethon exception."""

    def __init__(
        self, code: str, message: str, candidates: list[ChatSummary] | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.candidates = candidates or []


@dataclass(frozen=True)
class _ResolvedChat:
    summary: ChatSummary
    entity: object


_AUTH_ERRORS = (
    errors.UnauthorizedError,
    errors.AuthKeyUnregisteredError,
    errors.SessionRevokedError,
)
_RATE_ERRORS = (errors.FloodWaitError, errors.SlowModeWaitError, errors.PeerFloodError)
_PEER_ERRORS = (
    errors.PeerIdInvalidError,
    errors.UserIdInvalidError,
    errors.ChatIdInvalidError,
    errors.ChannelPrivateError,
    errors.UsernameInvalidError,
    errors.UsernameNotOccupiedError,
    errors.ChatWriteForbiddenError,
)


def _safe_failure(error: Exception) -> TelegramToolFailure:
    if isinstance(error, TelegramToolFailure):
        return error
    if isinstance(error, _AUTH_ERRORS):
        return TelegramToolFailure("telegram_unauthorized", "Telegram session is not authorized")
    if isinstance(error, _RATE_ERRORS):
        return TelegramToolFailure("rate_limited", "Telegram rate limit reached")
    if isinstance(error, _PEER_ERRORS):
        return TelegramToolFailure("chat_not_found", "Chat is unavailable")
    return TelegramToolFailure("delivery_unknown", "Telegram operation outcome is unknown")


def _display_name(entity: object | None) -> str | None:
    if entity is None:
        return None
    name = " ".join(
        part for part in (getattr(entity, "first_name", None), getattr(entity, "last_name", None))
        if part
    )
    return name or getattr(entity, "title", None) or getattr(entity, "username", None)


class TelethonGateway:
    def __init__(self, settings: Settings, client: object | None = None) -> None:
        self._settings = settings
        self._client = client
        self._connect_lock = asyncio.Lock()
        self._me: object | None = None

    async def _ensure_connected(self) -> None:
        async with self._connect_lock:
            try:
                if self._client is None:
                    self._client = TelegramClient(
                        StringSession(self._settings.session_string),
                        self._settings.api_id,
                        self._settings.api_hash,
                        request_retries=0,
                        flood_sleep_threshold=0,
                        # RPC retries and transport replay are separate in
                        # Telethon: reconnect would requeue pending writes.
                        auto_reconnect=False,
                    )
                if not self._client.is_connected():
                    await self._client.connect()
                if not await self._client.is_user_authorized():
                    raise TelegramToolFailure(
                        "telegram_unauthorized", "Telegram session is not authorized"
                    )
                if self._me is None:
                    self._me = await self._client.get_me()
                    if self._me is None:
                        raise TelegramToolFailure(
                            "telegram_unauthorized", "Telegram session is not authorized"
                        )
            except Exception as error:
                raise _safe_failure(error) from None

    def _saved_chat(self) -> _ResolvedChat:
        self_id = str(self._me.id)
        return _ResolvedChat(
            ChatSummary(
                chat_id=self_id,
                title="Saved Messages",
                username=getattr(self._me, "username", None),
                kind="private",
                is_self=True,
            ),
            self._me,
        )

    async def _all_chats(self) -> list[_ResolvedChat]:
        await self._ensure_connected()
        saved = self._saved_chat()
        self_id = saved.summary.chat_id
        chats = [saved]
        try:
            async for dialog in self._client.iter_dialogs():
                if str(dialog.id) == self_id:
                    continue
                entity = dialog.entity
                if getattr(entity, "bot", False):
                    kind = "bot"
                elif dialog.is_group or getattr(entity, "megagroup", False):
                    kind = "group"
                elif dialog.is_channel:
                    kind = "channel"
                else:
                    kind = "private"
                chats.append(
                    _ResolvedChat(
                        ChatSummary(
                            chat_id=str(dialog.id),
                            title=dialog.name,
                            username=getattr(entity, "username", None),
                            kind=kind,
                            is_self=False,
                        ),
                        entity,
                    )
                )
        except Exception as error:
            raise _safe_failure(error) from None
        return chats

    async def list_chats(self, query: str | None, limit: int) -> ChatListResult:
        needle = query.casefold() if query else None
        matches = []
        for chat in await self._all_chats():
            summary = chat.summary
            if needle and needle not in summary.title.casefold() and needle not in (
                summary.username or ""
            ).casefold():
                continue
            matches.append(summary)
            if len(matches) >= limit:
                break
        return ChatListResult(chats=matches)

    async def _resolve(self, chat: str) -> _ResolvedChat:
        if chat.casefold() == "me":
            await self._ensure_connected()
            return self._saved_chat()

        chats = await self._all_chats()

        if chat.lstrip("-").isdecimal():
            by_id = [item for item in chats if item.summary.chat_id == chat]
            if by_id:
                return by_id[0]

        username = chat.removeprefix("@").casefold()
        by_username = [
            item for item in chats
            if item.summary.username and item.summary.username.casefold() == username
        ]
        if by_username:
            return self._unique(by_username)

        by_title = [
            item for item in chats if item.summary.title.casefold() == chat.casefold()
        ]
        if by_title:
            return self._unique(by_title)
        raise TelegramToolFailure("chat_not_found", "Chat was not found")

    @staticmethod
    def _unique(matches: list[_ResolvedChat]) -> _ResolvedChat:
        if len(matches) > 1:
            raise TelegramToolFailure(
                "ambiguous_chat",
                "Multiple chats match; choose a chat_id",
                [item.summary for item in matches],
            )
        return matches[0]

    async def read_chat(self, chat: str, limit: int) -> ReadChatResult:
        resolved = await self._resolve(chat)
        messages = []
        try:
            async for message in self._client.iter_messages(resolved.entity, limit=limit):
                sender = await message.get_sender()
                messages.append(
                    ReadMessage(
                        message_id=message.id,
                        sent_at=message.date,
                        sender_id=str(message.sender_id) if message.sender_id is not None else None,
                        sender_name=_display_name(sender),
                        text=message.text or "",
                        outgoing=bool(message.out),
                        reply_to_message_id=message.reply_to_msg_id,
                    )
                )
        except Exception as error:
            raise _safe_failure(error) from None
        messages.reverse()
        return ReadChatResult(
            chat=ChatRef(chat_id=resolved.summary.chat_id, title=resolved.summary.title),
            messages=messages,
        )

    async def send_message(self, chat: str, text: str) -> SendMessageResult:
        resolved = await self._resolve(chat)
        try:
            sent = await self._client.send_message(resolved.entity, text, parse_mode=None)
        except Exception as error:
            raise _safe_failure(error) from None
        return SendMessageResult(
            sent=True,
            chat_id=resolved.summary.chat_id,
            message_id=sent.id,
            sent_at=sent.date,
        )
