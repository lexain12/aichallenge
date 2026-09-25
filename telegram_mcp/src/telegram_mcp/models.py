"""Typed, JSON-safe results returned by Telegram tools."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class ChatSummary(BaseModel):
    chat_id: str
    title: str
    username: str | None
    kind: Literal["private", "group", "channel", "bot"]
    is_self: bool


class ChatListResult(BaseModel):
    chats: list[ChatSummary]


class ChatRef(BaseModel):
    chat_id: str
    title: str


class ReadMessage(BaseModel):
    message_id: int
    sent_at: datetime
    sender_id: str | None
    sender_name: str | None
    text: str
    outgoing: bool
    reply_to_message_id: int | None


class ReadChatResult(BaseModel):
    chat: ChatRef
    messages: list[ReadMessage]


class SendMessageResult(BaseModel):
    sent: Literal[True]
    chat_id: str
    message_id: int
    sent_at: datetime
