"""LLM client for Skeleton AI + ZeroTier + alt-history script generation.

PR #152 (2026-05-12) — REPOINTED to fal.ai's any-llm router (Claude
Sonnet 4.5 by default). Casey's xAI team hit the monthly spending cap
and 429'd every Grok call. Per `project_studio_models.md`: "Fal
any-llm (Sonnet 4.5) for text." This is the canonical text path.

Backward compat:
  - Class name stays `GrokClient` so every caller keeps working.
  - `GrokAuthError` is raised on fal auth failures (401/403).
  - `GrokRateLimitError` is raised after retry-with-backoff exhausts
    on 429 (PR #151's logic preserved).
  - `.complete()` signature unchanged.
  - `.stream()` degrades to non-streaming under fal any-llm (returns
    the full completion as a single chunk). The frontend SSE plumbing
    in zerotier_private_router still works — it just delivers one big
    chunk instead of incremental tokens. Future PR can re-add real
    streaming if fal any-llm exposes it.

Endpoint: https://fal.run/fal-ai/any-llm
Model:    anthropic/claude-sonnet-4.5  (per Casey 2026-05-01 directive)
Auth:     FAL_AI_KEY env var (already configured across the platform)
"""
from __future__ import annotations
import os
import json
import time
import random
import httpx
from typing import Iterator

FAL_ANYLLM_URL = "https://fal.run/fal-ai/any-llm"
DEFAULT_MODEL = "anthropic/claude-sonnet-4.5"
XAI_CHAT_URL = "https://api.x.ai/v1/chat/completions"
DEFAULT_XAI_SCRIPT_MODEL = "grok-4.20-0309-non-reasoning"


class GrokAuthError(RuntimeError):
    """Raised when fal API key is missing or rejected by server.

    Legacy name kept for callsite compat — this is no longer xAI-specific.
    """


class GrokRateLimitError(RuntimeError):
    """Raised after all 429 retries have been exhausted."""


def _xai_output_token_budget(requested: int) -> int:
    """Honor caller max_tokens for long-form chapter expansion; keep short scripts fast."""
    req = int(requested or 1500)
    if req <= 2500:
        return max(200, min(req, 2500))
    return max(200, min(req, 32768))


def _xai_timeout_seconds(max_tokens: int) -> float:
    budget = _xai_output_token_budget(max_tokens)
    if budget <= 2500:
        return 45.0
    # Conservative ~8 tok/s floor; cap at 10 minutes for very long chapters.
    return min(600.0, max(90.0, budget / 8.0))


class GrokClient:
    """LLM client routing through fal.ai any-llm. Class name kept as
    GrokClient for backward compat with every caller in the codebase.
    """

    def __init__(self, api_key: str | None = None, model: str = DEFAULT_MODEL):
        # FAL_AI_KEY is the canonical env var across the platform (used
        # by backend.py and the fal_client lib). Accept FAL_KEY as a
        # fallback so dev environments that only set the older name
        # still work.
        self.api_key = (
            api_key
            or os.getenv("FAL_AI_KEY", "").strip()
            or os.getenv("FAL_KEY", "").strip()
        )
        if not self.api_key:
            raise GrokAuthError("FAL_AI_KEY not set in env")
        self.model = model
        self.xai_api_key = str(os.getenv("XAI_API_KEY") or "").strip()
        self.xai_model = str(os.getenv("STUDIO_FAST_SCRIPT_MODEL") or DEFAULT_XAI_SCRIPT_MODEL).strip()
        self._headers = {
            "Authorization": f"Key {self.api_key}",
            "Content-Type": "application/json",
        }

    def complete(
        self,
        system: str,
        user: str,
        max_tokens: int = 1500,
        temperature: float = 0.8,
    ) -> str:
        """Synchronous completion. Returns the assistant content.

        PR #151 retry-with-backoff preserved. On 429, honor Retry-After
        header if present, else exponential schedule [30s, 60s, 120s]
        for up to 3 attempts.

        fal any-llm response shape (per backend.py:4755):
          - data["output"]  → primary
          - data["result"]  → secondary
          - data["choices"][0]["message"]["content"] → OpenAI-shaped legacy
        We try all three so the client is resilient to fal changing the
        envelope.
        """
        # fal any-llm does NOT accept max_tokens — its router infers
        # from the underlying provider's defaults. The arg is preserved
        # in the signature so existing callers don't break.
        # Script and scene planning are latency-sensitive. Use direct xAI
        # first, then preserve fal any-llm as a transparent fallback.
        if self.xai_api_key:
            try:
                token_budget = _xai_output_token_budget(max_tokens)
                timeout_s = _xai_timeout_seconds(max_tokens)
                with httpx.Client(timeout=timeout_s) as c:
                    direct = c.post(
                        XAI_CHAT_URL,
                        headers={"Authorization": f"Bearer {self.xai_api_key}", "Content-Type": "application/json"},
                        json={
                            "model": self.xai_model,
                            "messages": [
                                {"role": "system", "content": system},
                                {"role": "user", "content": user},
                            ],
                            "temperature": max(0.2, float(temperature)),
                            "max_tokens": token_budget,
                        },
                    )
                direct.raise_for_status()
                direct_payload = direct.json()
                choices = direct_payload.get("choices") if isinstance(direct_payload, dict) else None
                content = ((choices or [{}])[0].get("message") or {}).get("content")
                if content:
                    return str(content).strip()
            except Exception:
                pass

        payload = {
            "model": self.model,
            "system_prompt": system,
            "prompt": user,
            "temperature": max(0.2, float(temperature)),
        }
        max_retries = 3
        backoff_seconds = [2, 5, 10]
        transient_backoff_seconds = [2, 5, 10]

        for attempt in range(max_retries + 1):
            with httpx.Client(timeout=180) as c:
                r = c.post(FAL_ANYLLM_URL, headers=self._headers, json=payload)
            if r.status_code in (401, 403):
                raise GrokAuthError(
                    f"fal rejected key {r.status_code}: {r.text[:200]}"
                )
            if r.status_code == 429 and attempt < max_retries:
                retry_after_hdr = r.headers.get("Retry-After", "").strip()
                wait_s = backoff_seconds[attempt]
                if retry_after_hdr.isdigit():
                    wait_s = max(1, int(retry_after_hdr))
                wait_s = int(wait_s * (1.0 + random.random() * 0.1))
                try:
                    import logging
                    logging.getLogger(__name__).warning(
                        f"fal any-llm 429 — retrying in {wait_s}s "
                        f"(attempt {attempt+1}/{max_retries})"
                    )
                except Exception:
                    pass
                time.sleep(wait_s)
                continue
            if r.status_code == 429:
                raise GrokRateLimitError(
                    f"fal any-llm 429 after {max_retries} retries; "
                    f"body={r.text[:200]}"
                )
            if 500 <= r.status_code < 600 and attempt < max_retries:
                wait_s = transient_backoff_seconds[min(attempt, len(transient_backoff_seconds) - 1)]
                wait_s = wait_s * (1.0 + random.random() * 0.1)
                try:
                    import logging
                    logging.getLogger(__name__).warning(
                        f"fal any-llm {r.status_code} - retrying in {wait_s:.1f}s "
                        f"(attempt {attempt+1}/{max_retries})"
                    )
                except Exception:
                    pass
                time.sleep(wait_s)
                continue
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, dict):
                raise RuntimeError(f"fal any-llm returned non-dict: {data!r}")
            # Surface upstream errors before trying to extract content
            output_err = data.get("error")
            if output_err:
                raise RuntimeError(f"fal any-llm upstream error: {output_err}")
            # Try the three known envelope shapes
            content = (
                data.get("output")
                or data.get("result")
                or ""
            )
            if not content:
                # OpenAI-shaped legacy envelope
                choices = data.get("choices") or []
                if choices:
                    content = (choices[0] or {}).get("message", {}).get("content", "")
            if not content:
                raise RuntimeError(
                    f"fal any-llm returned empty content; keys={list(data.keys())}"
                )
            return str(content).strip()

        raise GrokRateLimitError("fal any-llm 429 — exhausted retries")

    def stream(
        self,
        system: str,
        user: str,
        max_tokens: int = 1500,
        temperature: float = 0.8,
    ) -> Iterator[str]:
        """Stream completion as SSE chunks. PR #152 — fal any-llm doesn't
        expose token-level SSE through the simple POST surface, so this
        degrades to a single-chunk yield of the full completion. The
        zerotier_private SSE wrapper still works — Casey just sees the
        full script land in one piece instead of progressively.

        Future PR can re-add real streaming if fal exposes an SSE
        endpoint for any-llm.
        """
        text = self.complete(system, user, max_tokens=max_tokens, temperature=temperature)
        if text:
            yield text


def build_script_prompt(category_system: str, topic: str | None = None) -> str:
    """Build the user-side prompt for a script generation call. Combines
    the topic (if given) with a generic generate-a-script directive.
    Category-specific system prompt lives in idea_lists.CATEGORIES[k].
    """
    if topic:
        return (
            f"Generate the 60-second YouTube Shorts narration script for "
            f"this topic now: {topic}\n\n"
            f"Output ONLY the narration text — no headings, no JSON, no "
            f"scene breakdown, no commentary."
        )
    return (
        "Generate a fresh 60-second YouTube Shorts narration script "
        "following the system prompt's format. Output ONLY the "
        "narration text — no headings, no JSON, no commentary."
    )
