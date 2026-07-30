"""Direct-Anthropic scripting client with legacy class names.

``GrokClient`` remains source-compatible, but xAI and FAL any-LLM text
routing are disabled. FAL is reserved for Studio media, voice, and STT.
"""
from __future__ import annotations

import os
import random
import time
from typing import Iterator

import httpx

from studio_agent import provider_policy

ANTHROPIC_MESSAGES_URL = "https://api.anthropic.com/v1/messages"
DEFAULT_MODEL = "claude-sonnet-5"


class GrokAuthError(RuntimeError):
    """Legacy name for direct-Anthropic authentication failures."""


class GrokRateLimitError(RuntimeError):
    """Raised after all 429 retries have been exhausted."""


def _output_token_budget(requested: int) -> int:
    """Honor caller max_tokens for long-form expansion; keep short scripts fast."""
    req = int(requested or 1500)
    if req <= 2500:
        return max(200, min(req, 2500))
    return max(200, min(req, 32768))


def _timeout_seconds(max_tokens: int) -> float:
    budget = _output_token_budget(max_tokens)
    if budget <= 2500:
        return 45.0
    return min(600.0, max(90.0, budget / 8.0))


class GrokClient:
    """Direct-Anthropic client; legacy class name retained for callers."""

    def __init__(self, api_key: str | None = None, model: str = DEFAULT_MODEL):
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY", "").strip()
        if not self.api_key:
            raise GrokAuthError("ANTHROPIC_API_KEY not set in env")
        selected = str(model or DEFAULT_MODEL).strip()
        try:
            self.model = provider_policy.assert_runner_model_allowed(selected)
        except provider_policy.ProviderPolicyDenied as exc:
            raise GrokAuthError(str(exc)) from exc
        self._headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        }

    def complete(
        self,
        system: str,
        user: str,
        max_tokens: int = 1500,
        temperature: float = 0.8,
    ) -> str:
        """Return a direct Anthropic Messages API completion."""
        payload = {
            "model": self.model,
            "system": str(system or ""),
            "messages": [{"role": "user", "content": str(user or "")}],
            "max_tokens": _output_token_budget(max_tokens),
            "temperature": max(0.0, min(1.0, float(temperature))),
        }
        payload = provider_policy.sanitize_anthropic_payload(self.model, payload)
        max_retries = 3
        backoff_seconds = [2, 5, 10]

        for attempt in range(max_retries + 1):
            with httpx.Client(timeout=_timeout_seconds(max_tokens)) as client:
                response = client.post(
                    ANTHROPIC_MESSAGES_URL,
                    headers=self._headers,
                    json=payload,
                )
            if response.status_code in (401, 403):
                raise GrokAuthError(
                    f"Anthropic rejected key {response.status_code}: {response.text[:200]}"
                )
            if response.status_code == 429 and attempt < max_retries:
                retry_after = response.headers.get("Retry-After", "").strip()
                wait_seconds = backoff_seconds[attempt]
                if retry_after.isdigit():
                    wait_seconds = max(1, int(retry_after))
                wait_seconds = int(wait_seconds * (1.0 + random.random() * 0.1))
                try:
                    import logging

                    logging.getLogger(__name__).warning(
                        "Anthropic 429 - retrying in %ss (attempt %s/%s)",
                        wait_seconds,
                        attempt + 1,
                        max_retries,
                    )
                except Exception:
                    pass
                time.sleep(wait_seconds)
                continue
            if response.status_code == 429:
                raise GrokRateLimitError(
                    f"Anthropic 429 after {max_retries} retries; body={response.text[:200]}"
                )
            if 500 <= response.status_code < 600 and attempt < max_retries:
                wait_seconds = backoff_seconds[min(attempt, len(backoff_seconds) - 1)]
                wait_seconds *= 1.0 + random.random() * 0.1
                try:
                    import logging

                    logging.getLogger(__name__).warning(
                        "Anthropic %s - retrying in %.1fs (attempt %s/%s)",
                        response.status_code,
                        wait_seconds,
                        attempt + 1,
                        max_retries,
                    )
                except Exception:
                    pass
                time.sleep(wait_seconds)
                continue
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise RuntimeError(f"Anthropic returned non-dict: {data!r}")
            if data.get("error"):
                raise RuntimeError(f"Anthropic upstream error: {data['error']}")
            blocks = data.get("content") if isinstance(data.get("content"), list) else []
            content = "".join(
                str(block.get("text") or "")
                for block in blocks
                if isinstance(block, dict) and block.get("type") == "text"
            ).strip()
            if not content:
                raise RuntimeError(
                    f"Anthropic returned empty content; keys={list(data.keys())}"
                )
            return content

        raise GrokRateLimitError("Anthropic 429 - exhausted retries")

    def stream(
        self,
        system: str,
        user: str,
        max_tokens: int = 1500,
        temperature: float = 0.8,
    ) -> Iterator[str]:
        """Compatibility stream that yields the direct completion once."""
        text = self.complete(system, user, max_tokens=max_tokens, temperature=temperature)
        if text:
            yield text


#: Words, not seconds. Asked for "60 seconds" a model returned 203 words, which
#: narrated to 73.8s - 23% long. Script length sets the number of clips and so
#: the price of the short, and a word budget is followed far more reliably than
#: a duration the model cannot hear.
SHORTFORM_TARGET_SECONDS = 50
SHORTFORM_WORDS_PER_SECOND = 2.75


def shortform_word_budget(target_seconds: int = SHORTFORM_TARGET_SECONDS) -> tuple[int, int]:
    centre = int(round(max(10, int(target_seconds)) * SHORTFORM_WORDS_PER_SECOND))
    return max(20, centre - 8), centre + 8


def build_script_prompt(
    category_system: str,
    topic: str | None = None,
    *,
    target_seconds: int = SHORTFORM_TARGET_SECONDS,
) -> str:
    """Build the user prompt for short-form script generation."""
    low, high = shortform_word_budget(target_seconds)
    budget = (
        f"Length is a hard requirement: write between {low} and {high} words "
        f"(about {int(target_seconds)} seconds spoken). Do not exceed it."
    )
    if topic:
        return (
            f"Generate the YouTube Shorts narration script for this topic now: {topic}\n\n"
            f"{budget}\n\n"
            "Output ONLY the narration text - no headings, no JSON, no "
            "scene breakdown, no commentary."
        )
    return (
        "Generate a fresh YouTube Shorts narration script following the system "
        f"prompt's format.\n\n{budget}\n\n"
        "Output ONLY the narration text - no headings, no JSON, no commentary."
    )
