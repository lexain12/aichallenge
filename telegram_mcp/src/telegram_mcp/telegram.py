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

    async def read_chat(self, chat_id: str, limit: int) -> ReadChatResult: ...

    async def send_message(self, chat_id: str, text: str) -> SendMessageResult: ...


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
        self._dialog_scan_lock = asyncio.Lock()
        self._me: object | None = None
        self._chats_by_id: dict[str, _ResolvedChat] = {}
        self._missing_chat_ids: set[str] = set()

    async def _ensure_connected(self) -> None:
        async with self._connect_lock:
            try:
                if self._client is None:
                    session = StringSession(self._settings.session_string)
                    if self._settings.relay_port is not None:
                        original_dc_id = session.dc_id
                        session.set_dc(original_dc_id, "127.0.0.1", self._settings.relay_port)
                    self._client = TelegramClient(
                        session,
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

    @staticmethod
    def _resolved_dialog(dialog: object) -> _ResolvedChat:
        entity = dialog.entity
        if getattr(entity, "bot", False):
            kind = "bot"
        elif dialog.is_group or getattr(entity, "megagroup", False):
            kind = "group"
        elif dialog.is_channel:
            kind = "channel"
        else:
            kind = "private"
        return _ResolvedChat(
            ChatSummary(
                chat_id=str(dialog.id),
                title=dialog.name,
                username=getattr(entity, "username", None),
                kind=kind,
                is_self=False,
            ),
            entity,
        )

    async def list_chats(self, query: str | None, limit: int) -> ChatListResult:
        await self._ensure_connected()
        needle = query.casefold() if query else None
        matches = []

        def consider(chat: _ResolvedChat) -> bool:
            self._chats_by_id[chat.summary.chat_id] = chat
            self._missing_chat_ids.discard(chat.summary.chat_id)
            summary = chat.summary
            if needle and needle not in summary.title.casefold() and needle not in (
                summary.username or ""
            ).casefold():
                return False
            matches.append(summary)
            return len(matches) >= limit

        saved = self._saved_chat()
        if consider(saved):
            return ChatListResult(chats=matches)

        async with self._dialog_scan_lock:
            # A fresh list operation is also the explicit refresh mechanism for
            # IDs that were absent during an earlier complete scan.
            self._missing_chat_ids.clear()
            try:
                async for dialog in self._client.iter_dialogs():
                    if str(dialog.id) == saved.summary.chat_id:
                        continue
                    if consider(self._resolved_dialog(dialog)):
                        break
            except Exception as error:
                raise _safe_failure(error) from None
        return ChatListResult(chats=matches)

    async def _resolve(self, chat_id: str) -> _ResolvedChat:
        unsigned = chat_id.removeprefix("-")
        if not unsigned or not unsigned.isascii() or not unsigned.isdecimal():
            raise TelegramToolFailure("chat_not_found", "Chat ID was not found")

        await self._ensure_connected()
        saved = self._saved_chat()
        self._chats_by_id.setdefault(saved.summary.chat_id, saved)

        cached = self._chats_by_id.get(chat_id)
        if cached is not None:
            return cached
        if chat_id in self._missing_chat_ids:
            raise TelegramToolFailure("chat_not_found", "Chat ID was not found")

        async with self._dialog_scan_lock:
            cached = self._chats_by_id.get(chat_id)
            if cached is not None:
                return cached
            if chat_id in self._missing_chat_ids:
                raise TelegramToolFailure("chat_not_found", "Chat ID was not found")

            try:
                async for dialog in self._client.iter_dialogs():
                    if str(dialog.id) == saved.summary.chat_id:
                        continue
                    resolved = self._resolved_dialog(dialog)
                    self._chats_by_id[resolved.summary.chat_id] = resolved
                    if resolved.summary.chat_id == chat_id:
                        return resolved
            except Exception as error:
                raise _safe_failure(error) from None
            self._missing_chat_ids.add(chat_id)
        raise TelegramToolFailure("chat_not_found", "Chat ID was not found")

    async def read_chat(self, chat_id: str, limit: int) -> ReadChatResult:
        resolved = await self._resolve(chat_id)
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

    async def send_message(self, chat_id: str, text: str) -> SendMessageResult:
        resolved = await self._resolve(chat_id)
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
