"""
Pydantic schemas — the API contracts.

These define what JSON shapes the API accepts (incoming) and returns
(outgoing). They're the boundary between untyped HTTP and our typed
Python world.

Naming convention:
- *Create — the body of a POST request to create a thing
- *Update — the body of a PATCH/PUT to update a thing
- *Read — the JSON returned to API clients (NEVER includes secrets)
- *InDB — the full internal shape (used between layers, not exposed)
"""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field


# ============================================
# USER
# ============================================

class UserCreate(BaseModel):
    """Payload to register a new user."""
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)


class UserLogin(BaseModel):
    """Payload to log in."""
    email: EmailStr
    password: str


class UserRead(BaseModel):
    """User returned to API clients. NEVER includes password_hash."""
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    email: EmailStr
    is_active: bool
    created_at: datetime


# ============================================
# AUTH
# ============================================

class TokenResponse(BaseModel):
    """JWT issued on successful login or registration."""
    access_token: str
    token_type: str = "bearer"


class TokenPayload(BaseModel):
    """Decoded JWT contents."""
    sub: str  # user_id as string
    exp: int  # expiry timestamp


# ============================================
# TOPIC
# ============================================

class TopicCreate(BaseModel):
    """Body for POST /topics."""
    name: str = Field(min_length=1, max_length=120)
    keywords: list[str] = Field(default_factory=list, max_length=20)


class TopicUpdate(BaseModel):
    """Body for PATCH /topics/{id}."""
    name: str | None = Field(default=None, min_length=1, max_length=120)
    keywords: list[str] | None = Field(default=None, max_length=20)
    is_active: bool | None = None


class TopicRead(BaseModel):
    """Topic returned to API clients."""
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    user_id: UUID
    name: str
    keywords: list[str]
    is_active: bool
    created_at: datetime


# ============================================
# MENTION
# ============================================

class MentionRead(BaseModel):
    """Mention returned to API clients."""
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    topic_id: UUID
    source: str
    external_id: str
    url: str | None
    author: str | None
    title: str | None
    content: str | None
    content_published_at: datetime | None
    sentiment_score: float | None
    sentiment_label: str | None
    entities: list[str] | None
    summary: str | None
    ingested_at: datetime
    analyzed_at: datetime | None


class MentionsPage(BaseModel):
    """Paginated mentions list response."""
    items: list[MentionRead]
    total: int
    limit: int
    offset: int


# ============================================
# EVENTS (Kafka payloads)
# ============================================

class TopicCreatedEvent(BaseModel):
    """Published to Kafka when a user creates a topic."""
    topic_id: UUID
    user_id: UUID
    name: str
    keywords: list[str]


class MentionRawEvent(BaseModel):
    """Published by ingester for the analyzer to pick up."""
    source: str
    external_id: str
    topic_id: UUID
    url: str | None = None
    author: str | None = None
    title: str | None = None
    content: str | None = None
    content_published_at: datetime | None = None
    raw_payload: dict | None = None


class MentionAnalyzedEvent(BaseModel):
    """Published by analyzer for the websocket service to push to clients."""
    mention_id: UUID
    topic_id: UUID
    user_id: UUID
    source: str
    title: str | None
    sentiment_score: float | None
    sentiment_label: str | None
    entities: list[str] | None
    summary: str | None
    
class IngestionJobEvent(BaseModel):
    """
    Published by scheduler to request that an ingester fetch new
    mentions for a topic from a specific source.
    """
    topic_id: UUID
    user_id: UUID
    name: str
    keywords: list[str]
    source: str  # "hackernews", "reddit", etc.