"""Tool-calling reliability: message chains, normalization, success detection."""
from __future__ import annotations

import json

from studio_agent import openrouter, runner, store


def test_align_tool_message_boundary_drops_orphan_tool_results() -> None:
    messages = [
        {"role": "system", "content": "sys"},
        {"role": "tool", "tool_call_id": "x", "content": "orphan"},
        {"role": "user", "content": "hi"},
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "c1",
                    "type": "function",
                    "function": {"name": "get_public_search_trends", "arguments": "{}"},
                }
            ],
        },
        {"role": "tool", "tool_call_id": "c1", "content": "{}"},
        {"role": "assistant", "content": "done"},
    ]
    aligned = store.align_tool_message_boundary(messages)
    assert aligned[0]["role"] == "system"
    assert aligned[1]["role"] == "user"
    assert aligned[2]["role"] == "assistant"
    assert aligned[2].get("tool_calls")
    assert aligned[3]["role"] == "tool"
    assert aligned[4]["content"] == "done"


def test_align_tool_message_boundary_drops_incomplete_tool_chain() -> None:
    messages = [
        {"role": "user", "content": "research this"},
        {
            "role": "assistant",
            "content": "I'll search",
            "tool_calls": [
                {
                    "id": "c1",
                    "type": "function",
                    "function": {"name": "get_public_search_trends", "arguments": "{}"},
                },
                {
                    "id": "c2",
                    "type": "function",
                    "function": {"name": "get_channel_analytics", "arguments": "{}"},
                },
            ],
        },
        # Only one tool result â€” incomplete chain must not ship to the model.
        {"role": "tool", "tool_call_id": "c1", "content": "{}"},
        {"role": "user", "content": "continue"},
    ]
    aligned = store.align_tool_message_boundary(messages)
    assert aligned[0]["role"] == "user"
    # Incomplete assistant tool_calls becomes text-only (content preserved).
    assert aligned[1]["role"] == "assistant"
    assert "tool_calls" not in aligned[1]
    assert aligned[1]["content"] == "I'll search"
    assert aligned[2]["role"] == "user"
    assert aligned[2]["content"] == "continue"


def test_trim_messages_for_model_never_starts_on_orphan_tool() -> None:
    messages = [{"role": "system", "content": "sys"}]
    for i in range(store.MAX_MESSAGES_FOR_MODEL + 20):
        messages.append({"role": "user", "content": f"u{i}"})
        messages.append(
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": f"c{i}",
                        "type": "function",
                        "function": {"name": "list_skills", "arguments": "{}"},
                    }
                ],
            }
        )
        messages.append({"role": "tool", "tool_call_id": f"c{i}", "content": "{}"})
        messages.append({"role": "assistant", "content": f"a{i}"})

    # Force a cut that would previously orphan a tool row at the window start.
    trimmed = store.trim_messages_for_model(messages)
    roles = [str(m.get("role")) for m in trimmed]
    # No tool message may appear without a preceding assistant tool_calls in the window.
    for idx, role in enumerate(roles):
        if role != "tool":
            continue
        # Walk back to the nearest assistant with tool_calls
        found = False
        for prev in reversed(trimmed[:idx]):
            if prev.get("role") == "assistant" and prev.get("tool_calls"):
                found = True
                break
            if prev.get("role") in {"user", "assistant"} and not prev.get("tool_calls"):
                break
        assert found, f"orphan tool at index {idx} in trimmed messages"


def test_normalize_assistant_message_accepts_kimi_alternate_shapes() -> None:
    # Top-level name/arguments (some OpenRouter models incl. Kimi)
    msg = openrouter.normalize_assistant_message(
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {
                    "id": "k1",
                    "name": "get_public_search_trends",
                    "arguments": {"query": "dark psychology", "days": 7},
                }
            ],
        }
    )
    assert msg["tool_calls"][0]["function"]["name"] == "get_public_search_trends"
    args = json.loads(msg["tool_calls"][0]["function"]["arguments"])
    assert args["query"] == "dark psychology"

    # Legacy singular function_call
    msg2 = openrouter.normalize_assistant_message(
        {
            "role": "assistant",
            "content": None,
            "function_call": {
                "name": "get_channel_analytics",
                "arguments": '{"registry_key":"mrskelewelly"}',
            },
        }
    )
    assert msg2["tool_calls"][0]["function"]["name"] == "get_channel_analytics"
    assert "function_call" not in msg2


def test_public_demand_success_detection_rejects_hard_errors() -> None:
    failed = [
        runner.ToolFire(
            "get_public_search_trends",
            {"query": "x"},
            json.dumps({"error": "No module named 'studio_analytics_router'"}),
        )
    ]
    assert runner._has_public_demand_tool(failed) is True
    assert runner._has_successful_public_demand_tool(failed) is False
    assert runner._public_demand_needs_retry(failed) is True

    ok = [
        runner.ToolFire(
            "get_public_search_trends",
            {"query": "dark psychology YouTube Shorts"},
            json.dumps(
                {
                    "source": "youtube_data_api_public_search",
                    "queries": ["dark psychology YouTube Shorts"],
                    "videos": [{"title": "t", "video_id": "abc"}],
                    "evidence_summary": {"total_rows": 1, "hydrated_rows": 1},
                }
            ),
        )
    ]
    assert runner._has_successful_public_demand_tool(ok) is True
    assert runner._public_demand_needs_retry(ok) is False


def test_message_from_response_normalizes_nested_tool_use_blocks() -> None:
    resp = {
        "choices": [
            {
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "text", "text": "pulling data"},
                        {
                            "type": "tool_use",
                            "id": "tu_1",
                            "name": "get_public_search_trends",
                            "input": {"query": "niche", "days": 7},
                        },
                    ],
                }
            }
        ]
    }
    msg = openrouter.message_from_response(resp)
    assert msg["tool_calls"][0]["function"]["name"] == "get_public_search_trends"
    assert "niche" in msg["tool_calls"][0]["function"]["arguments"]

