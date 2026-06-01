"""Studio Agent conversation loop."""
from __future__ import annotations

import json
import uuid
from typing import Any

from studio_agent import openrouter, skills
from studio_agent import store
from studio_agent.tools import execute_tool, requires_approval, tool_schemas

MAX_TOOL_ROUNDS = 12


def system_prompt(*, content_format: str) -> str:
    fmt_line = {
        "short": "Focus on short-form (60–90s vertical) via skeleton-ai + Rookcast skills.",
        "long": "Focus on long-form (8–60 min) via long_form pipelines + CHANNEL.md/FLOW.md gates.",
        "both": "Support both short-form and long-form; ask which the user wants if unclear.",
    }.get(content_format, "Support both formats.")

    return f"""You are NYPTID Studio Agent — a production orchestrator for YouTube content.

You have access to the full Rookcast skills library (26 playbooks) at studio/skills/.
Always load the relevant skill BEFORE executing a step (title-creation, script-writing, storyboard, thumbnail-design, compliance-preflight, etc.).

{fmt_line}

Production rules (from Rookcast, non-negotiable):
- Sample-then-confirm for host, thumbnail, and expensive renders unless user chose auto-accept.
- YMYL channels (CrypticScience): primary .gov sources only; run compliance-preflight.
- Follow CHANNEL.md + FLOW.md when a channel_key is set.
- Never invent API keys or credentials.

When proposing renders or shell builds, explain cost/risk briefly.
Use tools to read skills, channel docs, and analytics — do not guess playbook content.
For topic selection, call get_public_search_trends and get_channel_analytics before outlining.
After starting a render, poll with poll_render_job until awaiting_approval or complete.

{skills.skills_index_for_prompt()}
"""


async def run_turn(
    session: dict[str, Any],
    user_text: str,
) -> dict[str, Any]:
    """Process one user message; may queue pending_actions in confirm mode."""
    sid = session["session_id"]
    user_id = session["user_id"]
    model = session.get("model") or openrouter.DEFAULT_MODEL
    approval_mode = session.get("approval_mode") or "confirm"
    content_format = session.get("content_format") or "both"

    messages: list[dict[str, Any]] = list(session.get("messages") or [])
    messages.append({"role": "user", "content": user_text})

    if not any(m.get("role") == "system" for m in messages):
        messages.insert(0, {"role": "system", "content": system_prompt(content_format=content_format)})

    tools = tool_schemas()
    pending: list[dict[str, Any]] = []
    assistant_text = ""
    usage_total: dict[str, Any] = {}

    for _ in range(MAX_TOOL_ROUNDS):
        resp = await openrouter.chat_completion(
            messages=messages,
            tools=tools,
            model=model,
        )
        msg = openrouter.message_from_response(resp)
        usage = openrouter.usage_from_response(resp)
        usage_total = usage

        tool_calls = msg.get("tool_calls") or []
        content = msg.get("content") or ""

        if tool_calls:
            messages.append({
                "role": "assistant",
                "content": content or None,
                "tool_calls": tool_calls,
            })
            blocked = False
            for tc in tool_calls:
                fn = tc.get("function") or {}
                name = fn.get("name") or ""
                try:
                    raw_args = fn.get("arguments") or "{}"
                    args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                except json.JSONDecodeError:
                    args = {}

                if approval_mode == "confirm" and requires_approval(name):
                    action_id = f"act_{uuid.uuid4().hex[:12]}"
                    pending.append({
                        "id": action_id,
                        "tool": name,
                        "arguments": args,
                        "summary": f"{name}({json.dumps(args)[:200]})",
                    })
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.get("id"),
                        "content": json.dumps({
                            "status": "awaiting_user_approval",
                            "action_id": action_id,
                            "message": "User must approve this action in Studio Agent UI (confirm mode).",
                        }),
                    })
                    blocked = True
                    continue

                try:
                    result = execute_tool(
                        name,
                        args,
                        user_id=user_id,
                        content_format=content_format,
                    )
                except Exception as exc:
                    result = json.dumps({"error": str(exc)})

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.get("id"),
                    "content": result,
                })

            if blocked and pending:
                assistant_text = (
                    content
                    or "I prepared the next steps but need your approval before running commands that spend credits or write files."
                )
                break
            continue

        assistant_text = content or ""
        messages.append({"role": "assistant", "content": assistant_text})
        break

    store.update_session(sid, messages=messages)
    if pending:
        store.set_pending_actions(sid, pending)

    return {
        "session_id": sid,
        "assistant_message": assistant_text,
        "pending_actions": pending,
        "approval_mode": approval_mode,
        "usage": usage_total,
    }


async def approve_action(session: dict[str, Any], action_id: str) -> dict[str, Any]:
    action = store.pop_pending_action(session["session_id"], action_id)
    if not action:
        raise KeyError(f"pending action not found: {action_id}")

    name = action["tool"]
    args = action.get("arguments") or {}
    result = execute_tool(
        name,
        args,
        user_id=session["user_id"],
        content_format=session.get("content_format") or "both",
    )

    messages: list[dict[str, Any]] = list(session.get("messages") or [])
    messages.append({
        "role": "user",
        "content": (
            f"[User approved {name}]\nTool result:\n{result[:12000]}\n"
            "Summarize what happened and propose the next production step."
        ),
    })
    store.update_session(session["session_id"], messages=messages)

    refreshed = store.get_session(session["session_id"]) or session
    follow_up = await run_turn(refreshed, "Continue production from the approved action result.")
    follow_up["approved_action"] = {"id": action_id, "tool": name, "result_preview": result[:2000]}
    return follow_up
