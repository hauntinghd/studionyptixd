"""
xAI Grok 4.1 Fast Reasoning client for Skeleton AI script generation.

Endpoint: https://api.x.ai/v1/chat/completions
Model:    grok-4-fast-reasoning  (per Casey 2026-05-05 directive)

Auth via XAI_API_KEY env var. Server returns 400 with "Incorrect API key"
if key is dead — caller should surface that to the user as a config error,
not retry blindly.

Streaming supported via SSE (Korpi-style real-time script display).
"""
from __future__ import annotations
import os
import json
import time
import random
import httpx
from typing import Iterator

XAI_BASE = "https://api.x.ai/v1"
DEFAULT_MODEL = "grok-4-fast-reasoning"


class GrokAuthError(RuntimeError):
    """Raised when XAI_API_KEY is missing or rejected by server."""


class GrokRateLimitError(RuntimeError):
    """Raised after all 429 retries have been exhausted."""


class GrokClient:
    def __init__(self, api_key: str | None = None, model: str = DEFAULT_MODEL):
        self.api_key = api_key or os.getenv("XAI_API_KEY", "").strip()
        if not self.api_key:
            raise GrokAuthError("XAI_API_KEY not set in env")
        self.model = model
        self._headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def complete(
        self,
        system: str,
        user: str,
        max_tokens: int = 1500,
        temperature: float = 0.8,
    ) -> str:
        """Synchronous completion with 429 retry-with-backoff.

        PR #151 — when x.ai returns 429 Too Many Requests, honor the
        Retry-After header if present, else exponential backoff
        (30s, 60s, 120s) up to 3 retries. Mid-render Grok calls
        across the skeleton-ai and zerotier-private pipelines no
        longer crash on transient rate limits.
        """
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        max_retries = 3
        # Exponential backoff schedule used when Retry-After is absent.
        backoff_seconds = [30, 60, 120]

        for attempt in range(max_retries + 1):
            with httpx.Client(timeout=120) as c:
                r = c.post(
                    f"{XAI_BASE}/chat/completions",
                    headers=self._headers,
                    json=payload,
                )
            if r.status_code == 400 and "Incorrect API key" in r.text:
                raise GrokAuthError(f"xAI rejected key: {r.text[:200]}")
            if r.status_code in (401, 403):
                raise GrokAuthError(f"xAI auth failed {r.status_code}: {r.text[:200]}")
            if r.status_code == 429 and attempt < max_retries:
                # Honor Retry-After if present (can be integer seconds or
                # HTTP-date; x.ai uses integer seconds in practice).
                retry_after_hdr = r.headers.get("Retry-After", "").strip()
                wait_s = backoff_seconds[attempt]
                if retry_after_hdr.isdigit():
                    wait_s = max(1, int(retry_after_hdr))
                # Add 10% jitter so concurrent threads don't all wake up
                # at the same instant and re-trigger the limit.
                wait_s = int(wait_s * (1.0 + random.random() * 0.1))
                try:
                    import logging
                    logging.getLogger(__name__).warning(
                        f"Grok 429 — retrying in {wait_s}s "
                        f"(attempt {attempt+1}/{max_retries})"
                    )
                except Exception:
                    pass
                time.sleep(wait_s)
                continue
            if r.status_code == 429:
                # Out of retries
                raise GrokRateLimitError(
                    f"xAI 429 after {max_retries} retries; "
                    f"body={r.text[:200]}"
                )
            r.raise_for_status()
            data = r.json()
            choices = data.get("choices") or []
            if not choices:
                raise RuntimeError(f"Grok returned no choices: {data}")
            msg = choices[0].get("message", {})
            # Reasoning models put the answer in .content (sometimes also
            # reasoning_content for the chain-of-thought). We want .content.
            return (msg.get("content") or "").strip()

        # Unreachable in practice — every path above either returns or raises.
        raise GrokRateLimitError("xAI 429 — exhausted retries")

    def stream(
        self,
        system: str,
        user: str,
        max_tokens: int = 1500,
        temperature: float = 0.8,
    ) -> Iterator[str]:
        """Stream completion as SSE chunks. Yields incremental content pieces."""
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "max_tokens": max_tokens,
            "temperature": temperature,
            "stream": True,
        }
        with httpx.Client(timeout=120) as c:
            with c.stream("POST", f"{XAI_BASE}/chat/completions",
                          headers=self._headers, json=payload) as r:
                if r.status_code == 400:
                    body = r.read().decode()
                    if "Incorrect API key" in body:
                        raise GrokAuthError(f"xAI rejected key: {body[:200]}")
                r.raise_for_status()
                for line in r.iter_lines():
                    if not line or not line.startswith("data: "):
                        continue
                    chunk = line[6:].strip()
                    if chunk == "[DONE]":
                        return
                    try:
                        d = json.loads(chunk)
                    except json.JSONDecodeError:
                        continue
                    delta = (d.get("choices") or [{}])[0].get("delta", {})
                    piece = delta.get("content")
                    if piece:
                        yield piece


def build_script_prompt(category_system: str, topic: str | None = None) -> str:
    """Build the user-side prompt for the script gen call.

    `category_system` is the system prompt from idea_lists.CATEGORIES[k]["system_prompt"].
    `topic` is the optional user-provided topic; if None, Grok picks one from the seeds.
    """
    if topic:
        return (
            f"Topic: {topic}\n\n"
            f"Write the 60-second narration script. Output ONLY the script text "
            f"(no commentary, no scene labels, no markdown). Each beat is one "
            f"sentence. Total ~12 sentences."
        )
    return (
        "Pick ONE strong topic from the category and write the 60-second "
        "narration script. Output ONLY the script text (no commentary, no "
        "scene labels, no markdown). Each beat is one sentence. Total ~12 sentences."
    )
