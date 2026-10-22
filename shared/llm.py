"""
Groq LLM settings and health, shared by the analyzer (sentiment) and the
scheduler (digests).

Model choice:
- PulseStream ran on `llama-3.1-8b-instant` until Groq shut that model down on
  2026-08-16. From then on every call failed, and because a mention is stored
  even when analysis fails, the feed kept filling with posts that had no
  sentiment and nothing crashed. That's why this module also tracks LLM
  health (see `LLMHealth`), and why the analyzer re-analyses such posts later.
- The default is now `openai/gpt-oss-20b`, Groq's recommended replacement.
  Two guards stop an old env var from silently breaking analysis again:
  - if GROQ_MODEL names a model Groq is known to have shut down,
    `groq_model()` uses the default instead and logs a warning;
  - if Groq answers that the model in use doesn't exist (any more),
    `fall_back_if_model_unavailable()` switches to the default for the rest
    of the process and logs an error.

Reasoning models:
- gpt-oss-20b "thinks" before it answers, and those reasoning tokens count
  toward the output-token limit. So for reasoning models we ask for low
  reasoning effort (plenty for a sentiment label, and it keeps each call fast
  and inside the free-tier token budget), don't ask for the reasoning text
  back, and set the output limit high enough that reasoning can't use it all
  up and leave an empty answer.
- Other models get none of the reasoning parameters, because they don't
  support them.
"""

import logging
from datetime import datetime, timezone
from typing import Any

from shared.config import settings


logger = logging.getLogger("pulsestream.llm")

DEFAULT_GROQ_MODEL = "openai/gpt-oss-20b"

# Models Groq shut down on 2026-08-16 (console.groq.com/docs/deprecations).
# Either name in GROQ_MODEL is replaced by the default.
RETIRED_MODELS = {"llama-3.1-8b-instant", "llama-3.3-70b-versatile"}

REASONING_MODEL_PREFIXES = ("openai/gpt-oss",)

_warned_about: set[str] = set()
# Models Groq said don't exist while this process was running.
_unavailable_models: set[str] = set()


def groq_model() -> str:
    """The model to call: GROQ_MODEL, unless Groq has retired it."""
    configured = (settings.groq_model or "").strip() or DEFAULT_GROQ_MODEL
    if configured in _unavailable_models:
        return DEFAULT_GROQ_MODEL
    if configured not in RETIRED_MODELS:
        return configured
    if configured not in _warned_about:
        _warned_about.add(configured)
        logger.warning(
            "GROQ_MODEL=%s has been shut down by Groq; using %s instead. "
            "Update GROQ_MODEL to remove this warning.",
            configured, DEFAULT_GROQ_MODEL,
        )
    return DEFAULT_GROQ_MODEL


def is_reasoning_model(model: str) -> bool:
    return model.startswith(REASONING_MODEL_PREFIXES)


def completion_options(max_output_tokens: int) -> dict[str, Any]:
    """
    Keyword arguments for `chat.completions.create`: the model, the output
    limit, and the reasoning settings when the model is a reasoning model.
    """
    model = groq_model()
    options: dict[str, Any] = {
        "model": model,
        "max_completion_tokens": max_output_tokens,
    }
    if is_reasoning_model(model):
        options["reasoning_effort"] = "low"
        options["include_reasoning"] = False
    return options


def groq_error_code(exc: BaseException) -> str | None:
    """Groq's machine-readable error code, e.g. "rate_limit_exceeded", if any."""
    body = getattr(exc, "body", None)
    if not isinstance(body, dict):
        return None
    error = body.get("error") if isinstance(body.get("error"), dict) else body
    code = error.get("code")
    return code[:80] if isinstance(code, str) and code else None


# Groq answers 400 for these, but the problem is the model, not the posts.
MODEL_ERROR_CODES = {"model_decommissioned", "model_not_found"}


def is_request_rejected(exc: BaseException) -> bool:
    """
    True when Groq is up but refused this particular request (HTTP 400/422,
    for example a reply that wasn't valid JSON), so the posts in it are the
    likely cause. False for everything that's about Groq itself: rate
    limits, outages, network errors, a bad API key or a retired model.
    """
    # Imported here so this module stays importable without the groq package.
    from groq import BadRequestError, UnprocessableEntityError

    if not isinstance(exc, (BadRequestError, UnprocessableEntityError)):
        return False
    return groq_error_code(exc) not in MODEL_ERROR_CODES


def fall_back_if_model_unavailable(exc: BaseException) -> bool:
    """
    If Groq says the model in use doesn't exist (any more), use the default
    model from now on, so a model retired after this code was written can't
    silently stop analysis. Returns True if it switched, so the caller can
    retry once straight away.
    """
    if groq_error_code(exc) not in MODEL_ERROR_CODES:
        return False
    model = groq_model()
    if model == DEFAULT_GROQ_MODEL:
        return False  # nothing to fall back to
    _unavailable_models.add(model)
    logger.error(
        "Groq says model %s is unavailable (%s); using %s from now on. "
        "Update GROQ_MODEL.",
        model, groq_error_code(exc), DEFAULT_GROQ_MODEL,
    )
    return True


def describe_error(exc: BaseException | str) -> str:
    """
    A short, public-safe description of an LLM failure for /healthz: the
    error type, HTTP status and Groq's error code (for example
    "RateLimitError (HTTP 429, rate_limit_exceeded)"). The full message,
    which can include account details, goes to the server log only.
    """
    if isinstance(exc, str):
        return exc[:200]
    parts: list[str] = []
    status = getattr(exc, "status_code", None)
    if status is not None:
        parts.append(f"HTTP {status}")
    code = groq_error_code(exc)
    if code:
        parts.append(code)
    name = exc.__class__.__name__
    return f"{name} ({', '.join(parts)})" if parts else name


class LLMHealth:
    """
    In-memory record of how LLM calls are going, reported on /healthz, so a
    model that stops answering is visible at a glance instead of only as
    posts quietly missing their sentiment.
    """

    def __init__(self) -> None:
        self.last_success_at: datetime | None = None
        self.last_failure_at: datetime | None = None
        self.last_error: str | None = None
        self.consecutive_failures = 0
        self.reanalyzed_mentions = 0

    def record_success(self) -> None:
        self.last_success_at = datetime.now(timezone.utc)
        self.consecutive_failures = 0

    def record_failure(self, exc: BaseException | str) -> None:
        self.last_failure_at = datetime.now(timezone.utc)
        self.last_error = describe_error(exc)
        self.consecutive_failures += 1

    def snapshot(self) -> dict[str, Any]:
        def iso(value: datetime | None) -> str | None:
            return value.isoformat() if value else None

        snap: dict[str, Any] = {
            "model": groq_model(),
            "last_success_at": iso(self.last_success_at),
            "last_failure_at": iso(self.last_failure_at),
            "last_error": self.last_error,
            "consecutive_failures": self.consecutive_failures,
            "reanalyzed_mentions": self.reanalyzed_mentions,
        }
        configured = (settings.groq_model or "").strip()
        if configured and configured != snap["model"]:
            snap["configured_model"] = configured
        return snap


llm_health = LLMHealth()
