from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from studio_agent import queue as agent_queue


def test_chat_bypasses_queue_accounting(monkeypatch) -> None:
    try_acquire = AsyncMock(return_value=(True, 1))
    release = AsyncMock()
    redis_ping = AsyncMock(return_value=False)

    monkeypatch.setattr(agent_queue, "STUDIO_AGENT_QUEUE_ENABLED", True)
    monkeypatch.setattr(agent_queue, "_local_try_acquire", try_acquire)
    monkeypatch.setattr(agent_queue, "_local_release", release)
    monkeypatch.setattr(agent_queue, "_redis_ping", redis_ping)

    async def exercise() -> None:
        async with agent_queue.studio_agent_slot(operation="chat") as admission:
            assert admission.mode == "bypass"
            assert admission.active_sessions == 0
            assert admission.queue_position == 0

    asyncio.run(exercise())

    try_acquire.assert_not_awaited()
    release.assert_not_awaited()
    redis_ping.assert_not_awaited()


def test_production_operation_still_acquires_and_releases_queue_slot(monkeypatch) -> None:
    try_acquire = AsyncMock(return_value=(True, 1))
    release = AsyncMock()
    redis_ping = AsyncMock(return_value=False)

    monkeypatch.setattr(agent_queue, "STUDIO_AGENT_QUEUE_ENABLED", True)
    monkeypatch.setattr(agent_queue, "_local_try_acquire", try_acquire)
    monkeypatch.setattr(agent_queue, "_local_release", release)
    monkeypatch.setattr(agent_queue, "_redis_ping", redis_ping)

    async def exercise() -> None:
        async with agent_queue.studio_agent_slot(operation="continue_production") as admission:
            assert admission.mode == "local"
            assert admission.active_sessions == 1
            assert admission.queue_position == 0

    asyncio.run(exercise())

    try_acquire.assert_awaited_once_with()
    release.assert_awaited_once_with()

