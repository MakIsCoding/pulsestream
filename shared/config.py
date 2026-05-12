"""
Centralized configuration for all PulseStream services.

Reads from environment variables (loaded from .env in dev, from the platform's
env in production). Every service imports `settings` from here instead of
calling os.environ directly.
"""

from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # === Database ===
    database_url: str = Field(..., alias="DATABASE_URL")
    database_ssl: bool = Field(False, alias="DATABASE_SSL")
    postgres_user: str = Field("pulsestream", alias="POSTGRES_USER")
    postgres_password: str = Field("pulsestream_dev_password", alias="POSTGRES_PASSWORD")
    postgres_db: str = Field("pulsestream", alias="POSTGRES_DB")
    postgres_host: str = Field("postgres", alias="POSTGRES_HOST")
    postgres_port: int = Field(5432, alias="POSTGRES_PORT")

    # === Redis ===
    redis_url: str = Field("redis://redis:6379", alias="REDIS_URL")
    redis_host: str = Field("redis", alias="REDIS_HOST")
    redis_port: int = Field(6379, alias="REDIS_PORT")

    # === Kafka ===
    kafka_bootstrap_servers: str = Field("kafka:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_security_protocol: str = Field("PLAINTEXT", alias="KAFKA_SECURITY_PROTOCOL")
    kafka_sasl_mechanism: str = Field("SCRAM-SHA-256", alias="KAFKA_SASL_MECHANISM")
    kafka_sasl_username: str = Field("", alias="KAFKA_SASL_USERNAME")
    kafka_sasl_password: str = Field("", alias="KAFKA_SASL_PASSWORD")
    kafka_ssl_ca_cert: str = Field("", alias="KAFKA_SSL_CA_CERT")
    kafka_topic_mentions_raw: str = Field("mentions.raw", alias="KAFKA_TOPIC_MENTIONS_RAW")
    kafka_topic_mentions_analyzed: str = Field("mentions.analyzed", alias="KAFKA_TOPIC_MENTIONS_ANALYZED")
    kafka_topic_topics_created: str = Field("topics.created", alias="KAFKA_TOPIC_TOPICS_CREATED")
    kafka_topic_ingestion_jobs: str = Field("ingestion.jobs", alias="KAFKA_TOPIC_INGESTION_JOBS")

    # === Auth ===
    jwt_secret: str = Field("change-me", alias="JWT_SECRET")
    jwt_algorithm: str = Field("HS256", alias="JWT_ALGORITHM")
    jwt_expiry_hours: int = Field(24, alias="JWT_EXPIRY_HOURS")

    # === AI (Google Gemini — kept for fallback) ===
    gemini_api_key: str = Field("", alias="GEMINI_API_KEY")
    gemini_model: str = Field("gemini-2.5-flash-lite", alias="GEMINI_MODEL")

    # === AI (Groq — primary) ===
    llm_provider: str = Field("groq", alias="LLM_PROVIDER")
    groq_api_key: str = Field("", alias="GROQ_API_KEY")
    groq_model: str = Field("llama-3.1-8b-instant", alias="GROQ_MODEL")

    # === Reddit (optional) ===
    reddit_client_id: str = Field("", alias="REDDIT_CLIENT_ID")
    reddit_client_secret: str = Field("", alias="REDDIT_CLIENT_SECRET")
    reddit_user_agent: str = Field("pulsestream/0.1", alias="REDDIT_USER_AGENT")

    # === Scheduler ===
    ingestion_interval_seconds: int = Field(60, alias="INGESTION_INTERVAL_SECONDS")
    mention_retention_days: int = Field(30, alias="MENTION_RETENTION_DAYS")

    # === Service ports ===
    web_port: int = Field(8000, alias="WEB_PORT")
    websocket_port: int = Field(8001, alias="WEBSOCKET_PORT")
    websocket_url: str = Field("ws://localhost:8001", alias="WEBSOCKET_URL")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Returns a cached singleton Settings instance.

    Using lru_cache ensures we read the env vars only once per process,
    not on every import. Every service calls `from shared.config import settings`.
    """
    return Settings()


# Module-level singleton — import this directly:
#   from shared.config import settings
#   print(settings.database_url)
settings = get_settings()