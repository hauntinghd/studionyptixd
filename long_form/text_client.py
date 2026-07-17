"""Selected-model text client for long-form planning and scripting.

The legacy long-form code expects a synchronous ``complete`` method.  This
adapter keeps that contract while routing through Studio Agent's canonical
model registry, so the creator's Claude/Grok/OpenRouter selection is honored
exactly instead of silently trying xAI first or buying FAL any-LLM text.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Coroutine

from studio_agent import openrouter


def _run(coro: Coroutine[Any, Any, dict[str, Any]]) -> dict[str, Any]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    # Some FastAPI compatibility routes still call the synchronous long-form
    # generator from an async handler.  Run the provider coroutine on its own
    # loop rather than blocking/re-entering the request loop.
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="longform-text") as pool:
        return pool.submit(asyncio.run, coro).result()


class StudioTextClient:
    def __init__(self, model: str | None = None):
        self.model = str(model or openrouter.DEFAULT_MODEL).strip() or openrouter.DEFAULT_MODEL

    def complete(
        self,
        system: str,
        user: str,
        max_tokens: int = 1500,
        temperature: float = 0.8,
    ) -> str:
        response = _run(
            openrouter.chat_completion(
                model=self.model,
                messages=[
                    {"role": "system", "content": str(system or "")},
                    {"role": "user", "content": str(user or "")},
                ],
                temperature=float(temperature),
                reasoning_depth="balanced",
                web_search=False,
                max_tokens=max(512, int(max_tokens or 1500)),
            )
        )
        message = openrouter.message_from_response(response)
        content = message.get("content") if isinstance(message, dict) else ""
        if isinstance(content, list):
            content = "\n".join(
                str(row.get("text") or "") if isinstance(row, dict) else str(row)
                for row in content
            )
        text = str(content or "").strip()
        if not text:
            raise RuntimeError(f"selected Studio model {self.model!r} returned no text")
        return text
