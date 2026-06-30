import os
import sys
import types
import asyncio

os.environ["REDIS_QUEUE_ENABLED"] = "0"
os.environ["REDIS_URL"] = ""
sys.modules.setdefault("stripe", types.SimpleNamespace())

from studio_agent.runner import _fire_verification_step, _needs_fresh_public_search, _needs_public_search_preflight


def test_updated_data_followup_requires_public_search_preflight():
    text = "same thing again, but lets get more updated data since its been about 3 days now"

    assert _needs_public_search_preflight(text)
    assert _needs_fresh_public_search(text)


def test_plain_followup_without_data_does_not_force_public_search():
    assert not _needs_public_search_preflight("same thing again")


def test_verification_step_event_payload_is_structured():
    events = []

    async def emit(payload):
        events.append(payload)

    asyncio.run(_fire_verification_step(
        emit,
        "tool_evidence",
        "running",
        label="Run required data tools",
        detail="Pulling public search data.",
    ))

    assert events == [
        {
            "event": "verification_step",
            "step": "tool_evidence",
            "status": "running",
            "label": "Run required data tools",
            "detail": "Pulling public search data.",
            "required": True,
        }
    ]


if __name__ == "__main__":
    test_updated_data_followup_requires_public_search_preflight()
    test_plain_followup_without_data_does_not_force_public_search()
    print("studio agent public search routing tests passed")
