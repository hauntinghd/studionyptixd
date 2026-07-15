from __future__ import annotations

import json

import pytest

from studio_agent import runner


def _tool_schema(name: str) -> dict:
    return {
        "type": "function",
        "function": {"name": name, "description": name, "parameters": {"type": "object"}},
    }


@pytest.mark.parametrize(
    ("request_context", "offered", "hidden_tool"),
    [
        ("plan", ["list_render_styles"], "animate_production_scenes"),
        ("chat", ["list_render_styles", "list_production_scenes"], "finalize_production"),
        ("weak_model", ["search_youtube_public"], "start_shortform_generate"),
    ],
)
def test_hidden_model_tool_never_reaches_executor(
    request_context: str,
    offered: list[str],
    hidden_tool: str,
) -> None:
    executor_calls: list[tuple] = []

    def executor(*args, **kwargs):
        executor_calls.append((args, kwargs))
        return json.dumps({"ok": True})

    offered_names = runner._offered_model_tool_names([_tool_schema(name) for name in offered])
    result = runner._execute_offered_model_tool(
        hidden_tool,
        {"job_id": "job-1", "context": request_context},
        offered_tool_names=offered_names,
        user_id="user-1",
        content_format="short",
        session_id="session-1",
        executor=executor,
    )

    assert executor_calls == []
    assert json.loads(result) == {
        "status": "blocked_unoffered_tool",
        "error": "unoffered_tool",
        "tool": hidden_tool,
        "message": "The model returned a tool that was not offered in this request. No tool was executed.",
    }


def test_exactly_offered_model_tool_executes_once() -> None:
    calls: list[tuple[str, dict, dict]] = []

    def executor(name, arguments, **context):
        calls.append((name, dict(arguments), dict(context)))
        return json.dumps({"ok": True})

    result = runner._execute_offered_model_tool(
        "list_render_styles",
        {},
        offered_tool_names=frozenset({"list_render_styles"}),
        user_id="user-1",
        content_format="short",
        session_id="session-1",
        executor=executor,
    )

    assert json.loads(result) == {"ok": True}
    assert calls == [
        (
            "list_render_styles",
            {},
            {"user_id": "user-1", "content_format": "short", "session_id": "session-1"},
        )
    ]
