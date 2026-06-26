"""Studio Agent conversation loop."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

EventEmitter = Callable[[dict[str, Any]], Awaitable[None] | None]

from studio_agent import openrouter, production_budget, skills
from studio_agent.anti_hallucination import ToolFire, audit_turn, guard_text
from studio_agent import memory, store
from studio_agent import telemetry, training_capture
from studio_agent.tone import (
    CONTENT_TYPE_ROUTING_BLOCK,
    PROFESSIONAL_VOICE_BLOCK,
    sanitize_assistant_text,
)
from studio_agent.tools import (
    _normalize_shortform_category_args,
    execute_tool_logged,
    re_edit_production,
    requires_approval,
    tool_schemas,
)
from studio_agent.queue import (
    StudioAgentQueueFullError,
    StudioAgentQueueTimeoutError,
    studio_agent_slot,
)
from studio_agent.jobs import (
    JOB_START_TOOLS,
    extract_jobs_from_tool,
    merge_active_jobs,
)

MAX_TOOL_ROUNDS = 12

BRAND_NEW_PRODUCTION_TOOLS = frozenset({
    "start_longform_render",
    "start_shortform_generate",
    "analyze_reference_video",
})


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _llm_pricing_for_provider(provider: str, model: str, fallback_model: str) -> tuple[float | None, float | None, str, str]:
    """Return prompt/completion dollars per million tokens and debit reason."""
    if provider == "anthropic_fallback":
        prompt_ppm = _env_float("ANTHROPIC_FALLBACK_PROMPT_USD_PER_M", 1.0)
        completion_ppm = _env_float("ANTHROPIC_FALLBACK_COMPLETION_USD_PER_M", 5.0)
        return prompt_ppm, completion_ppm, "studio_agent_anthropic_fallback", fallback_model
    return None, None, "studio_agent_openrouter", model


def _is_model_credit_error(exc: Exception) -> bool:
    msg = str(exc or "").lower()
    return (
        "openrouter 402" in msg
        or "openrouter 429" in msg
        or "anthropic 402" in msg
        or "anthropic 429" in msg
        or "insufficient credits" in msg
        or "requires more credits" in msg
        or "add more credits" in msg
        or "credit balance" in msg
    )


def _model_credit_recovery_message(exc: Exception) -> str:
    msg = str(exc or "")
    lower = msg.lower()
    provider = "OpenRouter" if "openrouter" in lower else "the selected model provider"
    if "anthropic" in lower:
        provider = "Anthropic fallback"
    if "anthropic fallback is not configured" in lower or "anthropic_api_key missing" in lower:
        return (
            "Studio Agent hit primary model credit limits and Anthropic fallback is not configured on the backend. "
            "Your chat, stills, approvals, and any server-side production job are preserved. "
            "Set ANTHROPIC_API_KEY as a backend secret, redeploy, then press Resume and continue from this point. "
            "This is not an internet drop."
        )
    return (
        f"Studio Agent hit {provider} credit limits before it could finish the text response. "
        "Your chat, stills, approvals, and any server-side production job are preserved. "
        "Add model credits or switch to a cheaper/fallback model, then press Resume and continue from this point. "
        "This is not an internet drop."
    )

CHANNEL_ALIASES: dict[str, tuple[str, ...]] = {
    "empire_magnates": (
        "empire magnates",
        "empiremagnates",
        "@empiremagnates",
        "empire mets",
        "entire mangains",
        "entire magnates",
    ),
    "zerotier": (
        "zerotier",
        "zero tier",
        "@zerotier",
    ),
    "cryptic_science": (
        "cryptic science",
        "crypticscience",
        "@crypticscience",
    ),
    "history_rewind": (
        "history rewind",
        "historyrewind",
        "@historyrewindd",
    ),
    "nyptid_clips": (
        "nyptid clips",
        "nyptid recaps",
        "@nyptidrecaps",
    ),
    "mrskelewelly": (
        "mrskelewelly",
        "mr skelewelly",
        "mr. skelewelly",
        "mr skellywelly",
        "mr. skellywelly",
        "skellywelly",
        "skelewelly",
    ),
}


def _mentions_empire_magnates(text: str) -> bool:
    return _registry_from_text(text) == "empire_magnates"


def _registry_from_text(text: str) -> str:
    low = str(text or "").lower()
    for key, aliases in CHANNEL_ALIASES.items():
        if any(alias in low for alias in aliases):
            return key
    return ""


def _active_registry_key(session: dict[str, Any], user_text: str = "") -> str:
    explicit = str(session.get("registry_key") or "").strip()
    if explicit:
        return _registry_from_text(explicit) or explicit
    inferred = _registry_from_text(user_text)
    if inferred:
        return inferred
    return ""


def _needs_channel_data_preflight(user_text: str) -> bool:
    low = str(user_text or "").lower()
    direct_phrases = (
        "channel data",
        "analytics",
        "pull all data",
        "pull all of the data",
        "pull data",
        "pull the data",
        "recommend",
        "why",
        "failed",
        "performance",
        "source of truth",
        "grounded recommendation",
        "selected the channel",
        "properly selected the channel",
        "see the data",
        "see data",
        "data from",
        "able to see",
        "can you see",
        "fetch data",
        "refresh data",
        "current video",
        "current short",
        "latest video",
        "latest short",
        "newest video",
        "newest short",
        "video we posted",
        "video i posted",
        "posted on the channel",
    )
    if any(phrase in low for phrase in direct_phrases):
        return True
    if "channel" in low and any(
        phrase in low
        for phrase in (
            "perform",
            "grow",
            "improve",
            "better",
            "map out",
            "plan",
            "strategy",
            "views",
            "retention",
            "avd",
            "subscribers",
        )
    ):
        return True
    if "video" in low and any(
        phrase in low
        for phrase in (
            "perform",
            "grow",
            "improve",
            "better",
            "retention",
            "avd",
            "watch time",
            "subscribers",
            "views",
        )
    ):
        return True
    retry_phrases = (
        "tool round limit",
        "clean final answer",
        "try again",
        "continue from the saved tool",
        "continue from saved tool",
    )
    return any(phrase in low for phrase in retry_phrases)


def _needs_latest_upload_focus(user_text: str) -> bool:
    low = str(user_text or "").lower()
    return any(
        phrase in low
        for phrase in (
            "current video",
            "current short",
            "latest video",
            "latest short",
            "newest video",
            "newest short",
            "video we posted",
            "video i posted",
            "posted on the channel",
        )
    )


def _is_channel_data_followup(user_text: str) -> bool:
    low = str(user_text or "").strip().lower()
    compact = low.strip(" ?!.")
    if compact in {
        "so",
        "ok",
        "okay",
        "yes",
        "yeah",
        "yep",
        "go",
        "go ahead",
        "do it",
        "continue",
        "try again",
        "now",
    }:
        return True
    return len(compact) <= 24 and any(
        phrase in compact
        for phrase in (
            "what now",
            "how about now",
            "did it work",
            "any update",
            "still waiting",
            "call it",
        )
    )


def _is_job_status_followup(user_text: str) -> bool:
    compact = str(user_text or "").strip().lower().strip(" ?!.")
    if compact in {
        "continue",
        "resume",
        "status",
        "check status",
        "poll",
        "poll it",
        "check it",
        "any update",
        "what happened",
        "is it done",
        "what did you find",
        "so what did you find",
        "what'd you find",
        "what have you found",
        "show me the results",
        "show the results",
        "what are the results",
        "tell me what you found",
        "is the analysis done",
    }:
        return True
    return bool(
        re.search(
            r"\b(?:what|show|tell|give)\b.*\b(?:find|found|findings|results?|analysis|update|status)\b",
            compact,
            re.IGNORECASE,
        )
    )


def _recover_poll_target(session: dict[str, Any]) -> tuple[str, str] | None:
    """Find the most recent durable production job even if active_jobs was cleared."""
    def _infer_kind(job_id: str, candidate: str = "") -> str:
        kind = str(candidate or "").strip().lower()
        if kind in {"shortform", "longform", "competitor"}:
            return kind
        try:
            from studio_agent import competitor

            if (competitor.WORK_ROOT / job_id / "status.json").is_file():
                return "competitor"
        except Exception:
            pass
        if re.fullmatch(r"[0-9a-f]{12}", job_id or "", re.IGNORECASE):
            # Reference-analysis jobs are short UUID hex ids. Shortform jobs in Studio
            # are usually timestamp/underscore ids with a skeleton workspace.
            return "competitor"
        return "shortform"

    fresh = store.get_session(str(session.get("session_id") or "")) or session
    jobs = list(fresh.get("active_jobs") or [])
    for job in reversed(jobs):
        job_id = str(job.get("job_id") or "").strip()
        if job_id:
            return job_id, _infer_kind(job_id, str(job.get("kind") or ""))

    messages = list(fresh.get("messages") or [])
    for msg in reversed(messages):
        text = str(msg.get("content") or "")
        if "job_id" not in text:
            continue
        match = re.search(r'"job_id"\s*:\s*"([^"]+)"', text)
        if not match:
            match = re.search(r"\bjob[_ -]?id\s*[=:]\s*([A-Za-z0-9_-]+)", text, re.IGNORECASE)
        if match:
            kind = ""
            if re.search(r'"kind"\s*:\s*"longform"', text, re.IGNORECASE):
                kind = "longform"
            elif re.search(r'"kind"\s*:\s*"competitor"', text, re.IGNORECASE):
                kind = "competitor"
            job_id = match.group(1)
            return job_id, _infer_kind(job_id, kind)
    return None


def _format_polled_job_status(result: str) -> str:
    try:
        data = json.loads(result or "{}")
    except Exception:
        data = {}
    if not isinstance(data, dict):
        return "I couldn’t read the production status cleanly. Try Resume once, and I’ll reconnect to the render."
        return f"I couldn’t check the production yet: {data['error']}"
    status = str(data.get("status") or data.get("phase") or data.get("stage") or "running")
    kind = str(data.get("kind") or "").strip().lower()
    if not kind and (
        isinstance(data.get("analysis_profile"), dict)
        or isinstance(data.get("pacing"), dict)
        or "style_reference_note" in data
        or "reference" in str(data.get("note") or "").lower()
    ):
        kind = "competitor"
    job_id = str(data.get("job_id") or "").strip()
    if data.get("error") and kind != "competitor":
        return f"I couldnâ€™t check the production yet: {data['error']}"
    if kind == "competitor":
        stage = str(data.get("stage") or status or "running").replace("_", " ")
        percent = data.get("percent", data.get("progress"))
        suffix = f" ({percent}%)" if percent is not None else ""
        if status == "complete":
            pacing = data.get("pacing") if isinstance(data.get("pacing"), dict) else {}
            avg_shot = pacing.get("avg_shot_sec")
            cut_count = pacing.get("cut_count")
            format_label = str(
                data.get("analysis_profile", {}).get("label")
                if isinstance(data.get("analysis_profile"), dict)
                else ""
            ).strip()
            facts = []
            if avg_shot is not None:
                facts.append(f"average shot length {avg_shot}s")
            if cut_count is not None:
                facts.append(f"{cut_count} detected cuts")
            evidence = ", ".join(facts)
            return (
                f"The reference analysis is complete{f' for {format_label}' if format_label else ''}. "
                + (f"Observed pacing: {evidence}. " if evidence else "")
                + "The reference card now contains the grounded pacing and blueprint signals."
            )
        if status == "failed":
            return f"The reference analysis failed during {stage}: {data.get('error') or 'unknown error'}"
        return f"The reference analysis is still running: {stage}{suffix}."
    if status in {"awaiting_scene_review", "awaiting_approval"}:
        approved = int(data.get("approved_scene_count") or 0)
        total = int(data.get("total_scenes") or data.get("scene_count") or 0)
        if total and approved >= total:
            pending = int(data.get("animation_pending_count") or 0)
            return (
                f"All {total} scenes are approved. "
                + (
                    f"{pending} animation clip{'s' if pending != 1 else ''} still need to render."
                    if pending
                    else "The animation is ready to review before final export."
                )
            )
        count = f" {total} scenes" if total else " the scenes"
        return f"I finished generating{count}. Review them below and tell me what to change, approve, or animate."
    percent = data.get("percent", data.get("progress"))
    suffix = f" ({percent}%)" if percent is not None else ""
    friendly = {
        "complete": "The production is complete and ready to download.",
        "failed": "The production stopped with an error.",
        "animate": "I’m animating the approved scenes now.",
        "scenes_approved": "The scenes are approved and ready for animation.",
        "running": "The production is still running.",
    }.get(status, f"The production is {status.replace('_', ' ')}.")
    return f"{friendly}{suffix}"


def _recent_assistant_promised_channel_data(messages: list[dict[str, Any]], lookback: int = 6) -> bool:
    checked = 0
    for msg in reversed(messages or []):
        if checked >= lookback:
            break
        role = str(msg.get("role") or "")
        if role == "user":
            continue
        checked += 1
        if role != "assistant":
            continue
        content = str(msg.get("content") or "").lower()
        if any(
            phrase in content
            for phrase in (
                "pull the live channel data",
                "pull live channel data",
                "pull the channel data",
                "waiting on the analytics pull",
                "call that now directly",
                "call the analytics",
                "use the studio channel analytics",
                "before we plan anything",
                "before making performance claims",
            )
        ):
            return True
    return False


def _assistant_stalled_on_channel_data(assistant_text: str) -> bool:
    low = str(assistant_text or "").lower()
    if "get_channel_analytics" in low and ("tool:" in low or "registry_key" in low):
        return True
    if any(tool_name in low for tool_name in ("get_public_search_trends", "recommend_video_topics")) and (
        "tool:" in low or "niche_query" in low or "running simultaneously" in low
    ):
        return True
    stall_phrases = (
        "let me pull that data right now",
        "let me pull the live channel data",
        "let me pull live channel data",
        "let me pull the channel data",
        "still waiting on the analytics pull",
        "let me call that now directly",
        "let me call it now directly",
        "i will use the studio channel analytics",
        "i'll use the studio channel analytics",
        "i need the connected channel data before",
        "before we plan anything",
        "before making performance claims",
        "four data pulls running",
        "data pulls running simultaneously",
        "live search trends",
        "public demand data",
        "what i am pulling from public demand data",
        "people are actively typing",
        "read the returned data",
        "cross-reference them",
    )
    return any(phrase in low for phrase in stall_phrases)


def _is_manual_visual_edit_request(user_text: str, reply_to: dict | None = None) -> bool:
    if not reply_to:
        return False
    low = str(user_text or "").lower()
    visual_terms = (
        "skeleton",
        "mannequin",
        "character",
        "clothes",
        "clothing",
        "shirt",
        "pants",
        "hoodie",
        "jacket",
        "shoes",
        "wardrobe",
        "outfit",
        "pose",
        "posture",
        "expression",
        "face",
        "hands",
        "background",
        "room",
        "scene",
        "still",
        "frame",
        "image",
        "make him",
        "make her",
        "look like",
    )
    edit_terms = (
        "edit",
        "change",
        "fix",
        "replace",
        "make",
        "turn",
        "put",
        "remove",
        "add",
        "wear",
    )
    return any(term in low for term in visual_terms) and any(term in low for term in edit_terms)


def _channel_guard_tool_args(
    name: str,
    args: dict[str, Any],
    active_registry: str,
    active_channel_id: str = "",
) -> dict[str, Any]:
    """Keep channel-scoped tool calls on the active session channel.

    If Studio Agent is inside a ZeroTier session/request, analytics and topic
    tools must not pull Empire Magnates just because old context or memory
    mentioned it. This is intentionally conservative: only channel-scoped
    tools are rewritten, and only when we have an explicit active registry.
    """
    if not active_registry and not active_channel_id:
        return args
    channel_scoped = {
        "get_channel_analytics",
        "recommend_video_topics",
        "get_public_search_trends",
        "fetch_archival_for_video",
        "build_scene_blueprint_from_reference",
        "remember_channel_preference",
        "get_perpetual_memory",
    }
    if name not in channel_scoped:
        return args
    fixed = dict(args or {})
    requested = str(fixed.get("registry_key") or "").strip()
    if requested and requested != active_registry:
        fixed["_corrected_registry_key"] = {
            "requested": requested,
            "active": active_registry,
            "reason": "Prevented cross-channel analytics/memory contamination.",
        }
    if active_registry:
        fixed["registry_key"] = active_registry
    if active_channel_id and name in {
        "get_channel_analytics",
        "recommend_video_topics",
        "get_public_search_trends",
        "fetch_archival_for_video",
        "remember_channel_preference",
        "get_perpetual_memory",
    }:
        requested_channel = str(fixed.get("channel_id") or "").strip()
        if requested_channel and requested_channel != active_channel_id:
            fixed["_corrected_channel_id"] = {
                "requested": requested_channel,
                "active": active_channel_id,
                "reason": "Prevented cross-channel analytics/memory contamination.",
            }
        fixed["channel_id"] = active_channel_id
    return fixed


def _tool_observation_message(tool_name: str, result: str) -> dict[str, Any]:
    return {
        "role": "system",
        "content": (
            f"[Studio Agent preflight tool result: {tool_name}]\n"
            f"{str(result or '')[:12000]}\n"
            "[Use this as evidence. Do not claim live analytics if youtube_analytics_live.oauth_connected is false; "
            "call out that stored/Catalyst/public data was used instead.]"
        ),
    }


def _missing_channel_tool_blocked(report: Any) -> bool:
    joined = " ".join(str(x) for x in getattr(report, "blocked_claims", ()) or ()).lower()
    return "channel analytics/performance evidence without a channel-data tool" in joined


def _has_channel_analytics_tool(tool_fires: list[ToolFire]) -> bool:
    return any(str(fire.name or "") == "get_channel_analytics" for fire in tool_fires or [])


def _wants_short_plan(user_text: str) -> bool:
    low = str(user_text or "").lower()
    if "short" not in low and "short-form" not in low and "shortform" not in low:
        return False
    plan_terms = (
        "plan",
        "new",
        "next",
        "make",
        "create",
        "doesn't flop",
        "doesnt flop",
        "not flop",
        "flop",
    )
    return any(term in low for term in plan_terms)


def _wants_production_execution(user_text: str) -> bool:
    """Detect an explicit request to begin or resume a production."""
    return bool(
        re.search(
            r"\b("
            r"start(?: it| this| the (?:video|render|production))?|"
            r"get (?:it|this) started|"
            r"let'?s (?:do|start|make|render|generate) (?:it|this|the video)|"
            r"go ahead|do it|"
            r"render (?:it|this|the video)|"
            r"generate (?:it|this|the video)|"
            r"make (?:it|this|the video)|"
            r"begin (?:the )?(?:render|production)"
            r")\b",
            str(user_text or ""),
            re.IGNORECASE,
        )
    )


def _requires_tool_execution(user_text: str) -> bool:
    """Return true when a user is asking Studio to do work, not discuss it."""
    low = str(user_text or "").strip().lower()
    if not low:
        return False
    action = re.search(
        r"\b("
        r"regenerate|edit|change|fix|replace|remove|add|create|make|render|generate|"
        r"animate|approve|finalize|publish|upload|pull|fetch|refresh|check|inspect|"
        r"list|show|analy[sz]e|crawl|resume|continue|do it|go ahead"
        r")\b",
        low,
    )
    target = re.search(
        r"\b("
        r"scene|still|image|video|short|long[- ]?form|render|production|job|"
        r"channel|youtube|data|analytics|retention|trend|website|product|"
        r"reference|thumbnail|script|caption|subtitle|file|project"
        r")s?\b",
        low,
    )
    return bool(action and target)


def _promised_execution_blocked(audit: Any) -> bool:
    return any(
        "promised execution without firing a matching tool" in str(claim).lower()
        for claim in (getattr(audit, "blocked_claims", None) or [])
    )


def _recover_requested_production(
    session: dict[str, Any],
    user_text: str,
) -> tuple[str, dict[str, Any]] | None:
    """Recover the exact last production when the model narrated instead of acting."""
    if not _wants_production_execution(user_text):
        return None
    fresh = store.get_session(str(session.get("session_id") or "")) or session
    production_tools = {"start_shortform_generate", "start_longform_render"}

    def find_production(value: Any) -> tuple[str, dict[str, Any]] | None:
        if isinstance(value, dict):
            direct_name = str(value.get("tool") or "").strip()
            direct_args = value.get("arguments")
            if direct_name in production_tools and isinstance(direct_args, dict) and direct_args:
                return direct_name, dict(direct_args)
            fn = value.get("function")
            fn_name = str(fn.get("name") or "").strip() if isinstance(fn, dict) else ""
            if isinstance(fn, dict) and fn_name in production_tools:
                raw_args = fn.get("arguments")
                if isinstance(raw_args, dict) and raw_args:
                    return fn_name, dict(raw_args)
                if isinstance(raw_args, str):
                    try:
                        parsed = json.loads(raw_args)
                    except json.JSONDecodeError:
                        parsed = {}
                    if isinstance(parsed, dict) and parsed:
                        return fn_name, parsed
            for child in reversed(list(value.values())):
                found = find_production(child)
                if found:
                    return found
        elif isinstance(value, list):
            for child in reversed(value):
                found = find_production(child)
                if found:
                    return found
        return None

    return find_production(fresh)


def _needs_public_search_preflight(user_text: str) -> bool:
    low = str(user_text or "").lower()
    demand_terms = (
        "what people are actually looking for",
        "what people are looking for",
        "search trend",
        "search trends",
        "public demand",
        "search demand",
        "youtube shorts",
        "people are searching",
        "people are actively typing",
        "go on youtube",
        "look up",
        "live search",
        "trend data",
        "topic demand",
    )
    action_terms = (
        "pull",
        "find",
        "recommend",
        "topic",
        "plan",
        "new short",
        "short-form",
        "short form",
        "make",
        "create",
        "figure out",
    )
    return any(term in low for term in demand_terms) and any(term in low for term in action_terms)


def _public_search_query_for_channel(active_label: str, user_text: str) -> str:
    label = str(active_label or "").lower()
    if "skele" in label:
        return "psychology hidden behavior self improvement YouTube Shorts"
    if "empire" in label:
        return "financial crime documentary business scandal YouTube"
    if "zerotier" in label or "zero tier" in label:
        return "comic book mystery shorts YouTube"
    if "cryptic" in label:
        return "science mystery deep science YouTube Shorts"
    cleaned = " ".join(str(user_text or "").split())
    return cleaned[:180] or "YouTube Shorts topic demand"


def _grounded_channel_status_from_tools(
    tool_fires: list[ToolFire],
    *,
    active_label: str,
) -> str:
    """Deterministic fallback when the LLM/guard loops after analytics ran."""
    for fire in reversed(tool_fires or []):
        if str(fire.name or "") != "get_channel_analytics":
            continue
        try:
            data = json.loads(fire.result or "{}")
        except Exception:
            data = {}
        if not isinstance(data, dict):
            continue
        if data.get("error"):
            return (
                f"I tried to pull {active_label} channel analytics, but the tool returned an error: "
                f"{str(data.get('error'))[:300]}"
            )

        quality = data.get("analytics_data_quality") if isinstance(data.get("analytics_data_quality"), dict) else {}
        metrics = data.get("video_metrics") if isinstance(data.get("video_metrics"), dict) else {}
        live = data.get("youtube_analytics_live") if isinstance(data.get("youtube_analytics_live"), dict) else {}
        latest_upload = data.get("latest_upload") if isinstance(data.get("latest_upload"), dict) else {}
        source = str(quality.get("effective_source") or "unknown").strip()
        reported_title = str(data.get("channel_title") or "").strip()
        title = str(active_label or reported_title or "selected channel").strip()
        rows = int(quality.get("video_rows_available") or metrics.get("video_rows_available") or 0)
        retention_rows = int(quality.get("retention_rows_available") or metrics.get("retention_rows_available") or 0)
        video_row_payloads = _channel_video_rows_from_metrics(metrics)
        oauth_connected = bool(quality.get("oauth_connected"))
        limitation = str(quality.get("limitation") or quality.get("oauth_error") or live.get("error") or "").strip()
        matched_by = ""
        resolution = quality.get("channel_resolution") if isinstance(quality.get("channel_resolution"), dict) else {}
        if resolution:
            matched_by = str(resolution.get("matched_by") or "").strip()

        lines = [
            f"I can see data for {title}.",
            "",
            f"- Source: {source}",
            f"- OAuth connected for private analytics: {'yes' if oauth_connected else 'no'}",
            f"- Video rows available: {rows}",
            f"- Retention rows available: {retention_rows}",
        ]
        if matched_by:
            lines.append(f"- Channel match: {matched_by}")
        if reported_title and reported_title.lower() != title.lower():
            lines.append(f"- Tool-reported channel title ignored: {reported_title} (selected chat channel is {title})")
        if latest_upload:
            lines.append(f"- Latest upload: {_metric_row_line(latest_upload).lstrip('- ')}")
        if limitation:
            lines.extend(["", f"Limitation: {limitation}"])
        lines.append("")
        if retention_rows > 0 and video_row_payloads:
            lines.append(
                "Retention rows with actual video titles/metrics are present for this selected channel. "
                "I should use only those selected-channel rows for the next recommendation and avoid cross-channel memory contamination."
            )
        elif retention_rows > 0:
            lines.append(
                "Retention row count was reported, but no actual video-title/metric rows were returned in the payload. "
                "I should not name a winner from counts alone."
            )
        else:
            lines.append(
                "I should use only this selected-channel result for the next recommendation. "
                "If retention rows are missing, I can still use the available channel snapshot/public performance, "
                "but I should not invent a specific high-AVD winner."
            )
        return "\n".join(lines)
    return ""


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _channel_video_rows_from_metrics(metrics: dict[str, Any]) -> list[dict[str, Any]]:
    """Return actual video rows, not just row counts, from the analytics summary."""
    if not isinstance(metrics, dict):
        return []

    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for key in ("top_shorts_by_retention", "top_by_retention", "top_by_views"):
        bucket = metrics.get(key)
        if not isinstance(bucket, list):
            continue
        for row in bucket:
            if not isinstance(row, dict):
                continue
            title = str(row.get("title") or "").strip()
            video_id = str(row.get("video_id") or "").strip()
            dedupe_key = video_id or title.lower()
            if not dedupe_key or dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            rows.append(dict(row))
            if len(rows) >= 8:
                return rows
    return rows


def _metric_row_line(row: dict[str, Any]) -> str:
    title = str(row.get("title") or row.get("video_id") or "Untitled video").strip()
    stats: list[str] = []
    views = _safe_int(row.get("views", row.get("view_count", 0)))
    avp = _safe_float(row.get("average_view_percentage"))
    avd = _safe_int(row.get("average_view_duration_sec"))
    ctr = _safe_float(row.get("impression_click_through_rate"))
    published = str(row.get("published_at") or "").strip()
    if views:
        stats.append(f"{views:,} views")
    if avp:
        stats.append(f"{avp:.2f}% avg view")
    if avd:
        stats.append(f"{avd}s AVD")
    if ctr:
        stats.append(f"{ctr:.2f}% CTR")
    if published:
        stats.append(f"published {published[:10]}")
    return f"- {title}" + (f": {'; '.join(stats)}" if stats else "")


def _best_retention_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    usable = [
        row
        for row in rows
        if _safe_float(row.get("average_view_percentage")) > 0
        or _safe_int(row.get("average_view_duration_sec")) > 0
    ]
    if not usable:
        return None
    return max(
        usable,
        key=lambda row: (
            _safe_float(row.get("average_view_percentage")),
            _safe_int(row.get("views", row.get("view_count", 0))),
            _safe_int(row.get("average_view_duration_sec")),
        ),
    )


def _latest_channel_analytics_evidence(tool_fires: list[ToolFire]) -> dict[str, Any]:
    for fire in reversed(tool_fires or []):
        if str(fire.name or "") != "get_channel_analytics":
            continue
        try:
            data = json.loads(fire.result or "{}")
        except Exception:
            data = {}
        if not isinstance(data, dict) or data.get("error"):
            continue
        quality = data.get("analytics_data_quality") if isinstance(data.get("analytics_data_quality"), dict) else {}
        metrics = data.get("video_metrics") if isinstance(data.get("video_metrics"), dict) else {}
        video_rows = _channel_video_rows_from_metrics(metrics)
        return {
            "video_rows_available": int(quality.get("video_rows_available") or metrics.get("video_rows_available") or 0),
            "retention_rows_available": int(
                quality.get("retention_rows_available") or metrics.get("retention_rows_available") or 0
            ),
            "source": str(quality.get("effective_source") or "unknown").strip(),
            "oauth_connected": bool(quality.get("oauth_connected")),
            "video_rows": video_rows,
        }
    return {
        "video_rows_available": 0,
        "retention_rows_available": 0,
        "source": "unknown",
        "oauth_connected": False,
        "video_rows": [],
    }


def _grounded_channel_plan_from_tools(
    tool_fires: list[ToolFire],
    *,
    active_label: str,
    user_text: str = "",
) -> str:
    """Plan from available channel evidence without inventing missing retention rows."""
    status = _grounded_channel_status_from_tools(tool_fires, active_label=active_label)
    evidence = _latest_channel_analytics_evidence(tool_fires)
    retention_rows = int(evidence.get("retention_rows_available") or 0)
    video_rows = evidence.get("video_rows") if isinstance(evidence.get("video_rows"), list) else []
    best_row = _best_retention_row(video_rows)
    title = str(active_label or "selected channel").strip() or "selected channel"
    low_title = title.lower().replace(" ", "")
    low_user = str(user_text or "").lower()
    is_skeleton = "skele" in low_title or "skeleton" in low_user or "skelly" in low_user

    if is_skeleton:
        reference_title = str((best_row or {}).get("title") or "").strip()
        idea_title = (
            f"Next {title} short: follow the strongest returned pattern"
            if reference_title
            else f"Next {title} short: draft only until actual video rows are returned"
        )
        format_line = (
            "55-60 second psychology skeleton short built from the selected channel's returned video evidence."
            if reference_title
            else "55-60 second psychology skeleton short, but treat the topic as a draft until row-level evidence is returned."
        )
        hook = (
            f"Use the structure that worked in {reference_title}: immediate curiosity, one clear psychology promise, and a visual interrupt before the first drop."
            if reference_title
            else "Open with one clear psychology promise, but do not claim it is evidence-backed until the analytics payload includes actual titles and retention rows."
        )
        beat_lines = [
            "0-3s: Direct hook, MrSkeleWelly centered, one-word captions if captions are enabled.",
            "3-10s: Explain the tension: the brain rehearses danger to feel in control.",
            "10-22s: Visual pattern interrupt: same skeleton identity, new pose/background, no wardrobe drift.",
            "22-38s: Payoff: overthinking feels useful, but it is usually a fake safety loop.",
            "38-50s: Practical release: name the next tiny action instead of solving the whole future.",
            "50-60s: CTA: ask viewers to comment the thing they overthink most.",
        ]
        tags = "#psychology #overthinking #shorts #mrskelewelly #mindset"
    else:
        idea_title = f"Next {title} short: one clear promise, one visual system, one CTA"
        format_line = "45-60 second short using the selected channel's available snapshot/public performance data."
        hook = "Open with the clearest pain point or curiosity gap the channel audience already cares about."
        beat_lines = [
            "0-3s: Hook with one dominant promise.",
            "3-10s: Establish why the viewer should care now.",
            "10-25s: Deliver the first reveal with a visual change.",
            "25-42s: Add a second reveal or contradiction.",
            "42-55s: Resolve the idea and give a clear CTA.",
        ]
        tags = "#shorts #content #storytelling"

    evidence_lines: list[str] = []
    if video_rows:
        evidence_lines.extend(["Actual selected-channel videos returned:"])
        evidence_lines.extend(_metric_row_line(row) for row in video_rows[:5])
        if best_row:
            best_title = str(best_row.get("title") or "selected video").strip()
            evidence_lines.append(
                f"Best returned reference: {best_title} "
                f"({_safe_float(best_row.get('average_view_percentage')):.2f}% avg view, "
                f"{_safe_int(best_row.get('views', best_row.get('view_count', 0))):,} views)."
            )
    elif retention_rows > 0:
        evidence_lines.append(
            "The analytics tool reported retention rows, but the payload did not include actual video titles/metrics. "
            "I cannot rank a winner until the backend returns top_by_retention or top_shorts_by_retention rows."
        )
    else:
        evidence_lines.append(
            "No usable row-level retention videos were returned, so this cannot claim a specific high-AVD winner."
        )

    lines = [
        status or f"I can use the available selected-channel data for {title}.",
        "",
        (
            "Per-video retention rows with actual video titles are available from the selected-channel analytics result."
            if video_rows and best_row
            else "I cannot name an exact high-AVD winner unless the analytics tool returns per-video retention rows. I can still plan the next short from the available channel data without inventing that missing winner."
        ),
        "",
        *evidence_lines,
        "",
        f"Next short plan: {idea_title}",
        f"Format: {format_line}",
        f"Hook: {hook}",
        "",
        "Beat map:",
    ]
    lines.extend(f"- {line}" for line in beat_lines)
    lines.extend(
        [
            "",
            "Production rules:",
            "- Keep the selected channel locked for this chat.",
            "- Keep character identity and channel watermark/package locked to this channel.",
            "- Generate stills first and show them for review before image-to-video.",
            "- Do not animate until approved scenes are selected.",
            "- If captions are enabled, use one synced caption per word. If captions are disabled, do not add captions.",
            "",
            "Packaging:",
            f"- Title: {idea_title}",
            "- Description: A short, direct setup of the promise plus one CTA.",
            f"- Tags: {tags}",
        ]
    )
    return "\n".join(lines)


def _inject_shortform_render_style(args: dict[str, Any], session: dict[str, Any]) -> dict[str, Any]:
    """Ensure shortform jobs inherit the session Art Style when the model omits it."""
    from studio_agent.render_styles import resolve_render_style

    merged = dict(args or {})
    style = resolve_render_style(
        str(merged.get("render_style") or "").strip() or None,
        session_style=str(session.get("render_style") or "").strip() or None,
    )
    merged["render_style"] = style.key
    return merged


def _inject_shortform_caption_options(args: dict[str, Any], session: dict[str, Any]) -> dict[str, Any]:
    """Ensure shortform jobs inherit session caption preferences when omitted."""
    merged = dict(args or {})
    explicit_mode = "caption_mode" in merged
    explicit_enabled = "captions_enabled" in merged
    session_mode = str(session.get("caption_mode") or "").strip().lower()

    if "captions_enabled" not in merged:
        if "captions_enabled" in session:
            merged["captions_enabled"] = bool(session.get("captions_enabled"))
        else:
            merged["captions_enabled"] = session_mode == "word"

    if not explicit_mode:
        if session_mode in {"word", "off"}:
            merged["caption_mode"] = session_mode
        else:
            merged["caption_mode"] = "word" if merged.get("captions_enabled") is True else "off"

    if str(merged.get("caption_mode") or "").strip().lower() == "off" or merged.get("captions_enabled") is False:
        merged["caption_mode"] = "off"
        merged["captions_enabled"] = False
    elif explicit_mode or explicit_enabled or session_mode == "word":
        merged["caption_mode"] = "word"
        merged["captions_enabled"] = True
    else:
        merged["caption_mode"] = "off"
        merged["captions_enabled"] = False
    return merged


def _production_result_complete(parsed: dict[str, Any] | None) -> bool:
    if not isinstance(parsed, dict):
        return False
    status = str(parsed.get("status") or "").lower()
    phase = str(parsed.get("phase") or parsed.get("stage") or "").lower()
    has_deliverable = bool(parsed.get("video_path") or parsed.get("mp4_url") or parsed.get("download_url"))
    return has_deliverable and (
        status in {"complete", "completed", "ready"}
        or phase in {"done", "complete", "completed", "ready"}
    )


def _production_result_failed(parsed: dict[str, Any] | None) -> bool:
    if not isinstance(parsed, dict):
        return False
    status = str(parsed.get("status") or "").lower()
    phase = str(parsed.get("phase") or parsed.get("stage") or "").lower()
    return bool(parsed.get("error")) or status in {"failed", "error", "cancelled"} or phase in {"failed", "error", "cancelled"}


async def _fire_event(emit: EventEmitter | None, event: str, **payload: Any) -> None:
    if not emit:
        return
    out = emit({"event": event, **payload})
    if asyncio.iscoroutine(out):
        await out


def _valid_image_attachments(attachments: list[dict[str, Any]] | None) -> list[dict[str, str]]:
    valid: list[dict[str, str]] = []
    for item in list(attachments or [])[:4]:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "attached image")[:120]
        mime = str(item.get("mime_type") or item.get("type") or "").lower()
        data_url = str(item.get("data_url") or item.get("url") or "")
        size = int(item.get("size") or 0)
        if not mime.startswith("image/"):
            continue
        if not data_url.startswith("data:image/"):
            continue
        if size and size > 8 * 1024 * 1024:
            continue
        valid.append({"name": name, "mime_type": mime, "data_url": data_url})
    return valid


def _persist_image_attachments(
    session_id: str,
    attachments: list[dict[str, Any]] | None,
) -> list[str]:
    images = _valid_image_attachments(attachments)
    if not images:
        return []
    from studio_agent.tools import SKELETON_OUTPUT

    safe_session = re.sub(r"[^a-zA-Z0-9_-]", "", session_id)[:80]
    target = SKELETON_OUTPUT / "_session_inputs" / safe_session
    target.mkdir(parents=True, exist_ok=True)
    paths: list[str] = []
    for image in images:
        _header, encoded = image["data_url"].split(",", 1)
        try:
            payload = base64.b64decode(encoded, validate=True)
        except (ValueError, TypeError):
            continue
        if not payload or len(payload) > 8 * 1024 * 1024:
            continue
        mime = image["mime_type"]
        suffix = ".png" if mime == "image/png" else ".webp" if mime == "image/webp" else ".jpg"
        path = target / f"{hashlib.sha256(payload).hexdigest()[:20]}{suffix}"
        if not path.exists():
            path.write_bytes(payload)
        paths.append(str(path.resolve()))
    return paths


def _build_user_content(user_text: str, attachments: list[dict[str, Any]] | None = None) -> str | list[dict[str, Any]]:
    images = _valid_image_attachments(attachments)
    if not images:
        return user_text
    parts: list[dict[str, Any]] = [{"type": "text", "text": user_text}]
    for image in images:
        parts.append({
            "type": "image_url",
            "image_url": {"url": image["data_url"]},
        })
    return parts


def _append_text_to_message(message: dict[str, Any], extra: str) -> None:
    content = message.get("content")
    if isinstance(content, list):
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                part["text"] = f"{str(part.get('text') or '')}{extra}"
                return
        content.insert(0, {"type": "text", "text": extra})
        return
    message["content"] = f"{str(content or '')}{extra}"


def _billing_hint(billing_profile: dict[str, Any] | None) -> str:
    profile = billing_profile or {}
    if profile.get("unlimited"):
        return (
            "ACCOUNT: Owner (admin) — unmetered. Do not warn about credit balance or upsells; "
            "still quote fal/render costs so they can sanity-check spend."
        )
    plan = str(profile.get("plan_name") or profile.get("plan") or "subscriber").strip()
    bal = int(profile.get("balance") or 0)
    return (
        f"ACCOUNT: Paying subscriber ({plan}) — {bal:,} credits in unified wallet. "
        "Debit applies to OpenRouter and renders; suggest Wallet top-up if balance is low before expensive jobs."
    )


def system_prompt(
    *,
    content_format: str,
    reasoning_depth: str = "balanced",
    billing_profile: dict[str, Any] | None = None,
    render_style: str = "cinematic",
    memory_summary: str = "",
    active_registry: str = "",
) -> str:
    fmt_hint = {
        "short": (
            "User session bias: YouTube Short (9:16, under ~60s). Plan script + packaging first. "
            "Do NOT assume Skeleton AI — use channel-appropriate visuals unless they asked for skeleton."
        ),
        "long": (
            "User session bias: long-form (8–15 min). Use long-form + script-writing skills; "
            "Skeleton AI is not the default path."
        ),
        "both": (
            "Infer short vs long from the conversation — do not ask unless genuinely ambiguous. "
            "Neither choice implies Skeleton AI by default."
        ),
    }.get(content_format, "Infer short vs long from the conversation.")
    depth = str(reasoning_depth or "balanced").strip().lower()
    if depth not in openrouter.REASONING_DEPTHS:
        depth = "balanced"
    thinking_hint = {
        "fast": "User selected Fast thinking — be concise; prioritize the next 1–3 actions.",
        "balanced": "User selected Balanced thinking — normal depth.",
        "deep": "User selected Deep thinking — analyze tradeoffs before recommending spend or uploads.",
    }.get(depth, "User selected Balanced thinking.")

    from studio_agent.render_styles import get_render_style

    try:
        style = get_render_style(render_style)
        style_hint = (
            f"USER RENDER STYLE (session picker): {style.label} (`{style.key}`). "
            f"Pass render_style=\"{style.key}\" on start_shortform_generate unless they change it in chat. "
            + (
                "Skeleton niche art style — canonical bone/glass mascot host."
                if style.pipeline == "skeleton_host"
                else "Styled T2I scenes — characters match the chosen art style, not skeleton unless selected."
            )
        )
    except KeyError:
        style_hint = (
            "USER RENDER STYLE: cinematic (default). Call list_render_styles; pass render_style on "
            "start_shortform_generate. Use skeleton_host only when the Art Style picker is set to Skeleton."
        )

    color_accessibility_hint = (
        "COLOR ACCESSIBILITY DEFAULT (protan-safe): The operator/viewer may have protan colorblindness, "
        "where bright red, red-orange, and some warm hues can read as brown or low-signal. For every "
        "visual output you plan or generate (video scenes, thumbnails, captions, overlays, charts, UI-style "
        "mockups, packaging guidance), do not rely on red/orange/brown differences as the only meaning. "
        "Prefer high-luminance contrast and protan-friendly pairings such as cyan/blue/teal with white, "
        "yellow, violet, or charcoal. If red is narratively required (blood, warning, police lights, loss, "
        "danger), pair it with shape, position, label text, iconography, a bright outline, or a cool-color "
        "counterpart so the meaning stays obvious. Caption and thumbnail text must remain readable in "
        "grayscale and protan simulation; mention color explicitly only when the generated asset makes that "
        "color distinguishable without guessing."
    )
    channel_specific_hint = ""
    if active_registry == "empire_magnates":
        channel_specific_hint = (
            "- ACTIVE CHANNEL: Empire Magnates. Use financial-crime/business documentary analytics only.\n"
            "- Posted financial-crime catalog includes Mango Markets, Bre-X, Olympus, Denmark loophole, "
            "Germany/Wirecard loophole. These are reference signals, not next-video ideas unless the user asks for a remake/re-edit.\n"
            "- Ignore psychology/POV videos when building the financial crime style unless the user explicitly asks to analyze those uploads."
        )
    elif active_registry == "zerotier":
        channel_specific_hint = (
            "- ACTIVE CHANNEL: ZeroTier. ZeroTier is a comic-book / DC-character shorts channel.\n"
            "- Do not use Empire Magnates financial-crime analytics, business documentary conclusions, or long-form documentary packaging for ZeroTier.\n"
            "- Analyze ZeroTier as comic-book shorts unless the user explicitly switches channels."
        )
    elif active_registry:
        channel_specific_hint = (
            f"- ACTIVE CHANNEL REGISTRY: {active_registry}. Keep analytics, memory, recommendations, and packaging scoped to this channel."
        )

    return f"""You are NYPTID Studio Agent — the primary NYPTID Studio product. You help creators who
do NOT know what to film: pick niche + topic, frame the video beat-by-beat, then produce with
format-specific, channel-specific pacing, packaging, and delivery.

{fmt_hint}
{thinking_hint}

{style_hint}

{color_accessibility_hint}

{PROFESSIONAL_VOICE_BLOCK}

{CONTENT_TYPE_ROUTING_BLOCK}

{_billing_hint(billing_profile)}

PERPETUAL MEMORY:
Use this durable user/channel memory as prior context across chats. It is there to tailor
topic strategy, packaging, visual style, pacing rules, feedback lessons, and channel-specific
defaults. If the user contradicts memory, follow the newest user instruction and call a memory
tool to update it.
{memory_summary.strip() or "- No durable memory saved yet. Build it from channel analytics, feedback, and explicit user preferences."}

ANTI-HALLUCINATION / CHANNEL FACT RULES:
- Every answer is audited after generation. Treat this as a five-layer gate: source authority, claim type,
  tool evidence, memory/contradiction check, and final response correction.
- Do not say "based on your channel data" unless you actually called get_channel_analytics, list_youtube_channels,
  refresh_channel_intelligence, or the user gave the exact channel facts in this chat.
- For AVD/retention claims, read get_channel_analytics.analytics_data_quality first. API-key/public YouTube
  data can show public titles/views, not private AVD/retention. Channel aggregate analytics are not enough
  to identify a specific high-retention short. Only name a specific 50-60% AVD winner when
  video_level_retention_available is true and video_metrics.top_by_retention/top_shorts_by_retention contains
  that row. If false, state the missing data and the required next step: reconnect/refresh YouTube Analytics
  OAuth or use a Studio screenshot/export.
- Do not say "complete", "ready", or "fixed" for a render/re-edit until a production tool result proves it is complete.
  If a job is running, say it is running and show/poll progress.
- If the user says a video/topic is already posted, treat it as locked history. Never recommend it as the next new video.
- Never mix analytics/memory between channels. The active channel is the only valid source for channel-performance claims.
{channel_specific_hint}

WORLD-CLASS YOUTUBE PRODUCTION STANDARD:
- Operate like an elite YouTube strategist, writer, editor, retention analyst, and packaging director in one system.
- Goal: make videos people keep watching, liking, commenting on, and subscribing from. Optimize for high AVD/watch time,
  but never promise virality or guaranteed monetization.
- For every serious video plan, cover the full stack: topic angle, title, thumbnail concept, hook, story arc, pacing map,
  pattern interrupts, scene/shot list, caption rhythm, CTA, description, tags, and timestamps when relevant.
- Packaging rule: EVERY completed video needs an upload package with title, tags, description, and timestamps/beat map
  when useful. Short-form gets title/tags/description/hashtags/package only; do NOT generate or request thumbnails for
  shorts unless the user explicitly asks. Long-form gets title/tags/description/timestamps plus thumbnail candidates.
- Packaging must create curiosity without lying. Storytelling must escalate stakes beat by beat. Pacing must remove dead air
  and match the channel/reference grammar.
- If the user provides reference channels/videos, analyze them as evidence first; extract specific choices, not generic advice.
- Treat long-form and short-form differently: shorts need immediate clarity and visual lockstep; long-form needs durable
  narrative tension, chapter pacing, and frame-accurate timestamps.

═══ "I don't know what to make" (topic + niche discovery) ═══
- Start with `recommend_video_topics` (registry_key if connected, else niche_query).
- New/0-sub channel: positioning sprint + reference homework — never shame them for "failed" videos.
- Established: clone winners from growth_playbook + trending topics.
- Hardest steps (say this clearly): (1) script-writing / story beats, (2) packaging (title + thumbnail).
- After topic is chosen, help them down to the **frame**: scene list, hook, pattern interrupts, outro CTA.

═══ Reference video → scene blueprint + editing education (yt-dlp full power) ═══
When the user asks to "go on YouTube", "look up" a public channel/creator, or find examples from Lume/Jake Tran/Magnates/Fern without giving a URL:
1. Call `search_youtube_public` first with a specific query such as "Lume finance documentary" or "Lume documentary YouTube".
2. Use the returned public video URLs as candidate references. If one is clearly relevant, call `analyze_reference_video` on it; otherwise show 3-5 candidates and ask the user to pick.
3. Do NOT say "I can't browse YouTube directly" unless the public search tool returns a concrete quota/auth/network error. Quote that exact blocker if it happens.

When user links a YouTube URL (especially "watch this and improve my video" or "learn editing from this"):
1. Immediately call `analyze_reference_video` (yt-dlp download + scene keyframes + cut pacing + audio analysis + story structure).
   Pass content_format="short" for Shorts and content_format="long" for long-form. Never apply long-form
   documentary benchmark labels or metrics to a Short.
2. Poll `poll_render_job` kind=competitor — report every stage live.
3. Deeply study and extract **exact editing lessons**: hook timing, cut frequency & rhythm, story beat structure, visual grammar, CTA/subscribe placement, pacing patterns that drive retention, packaging (title/thumbnail synergy).
4. `build_scene_blueprint_from_reference` — per-scene rows using the learned patterns.
5. Apply those precise lessons when re-editing current video or planning new ones (e.g. "match the 2.1s avg shot length and mid-beat pattern interrupts from the reference").
6. `fetch_archival_for_video` etc. as before.
This is how the agent (and you) learn what actually makes videos perform — use it heavily for self-improvement and data collection.

═══ YOUTUBE CHANNEL INTELLIGENCE (start here when user mentions their channel) ═══
1. `youtube_oauth_status` — if not connected, send them to Studio → Settings → Channels.
2. `list_youtube_channels` — only shows THIS user's connected channels.
3. `get_channel_analytics` - Catalyst + live YouTube Analytics (90d Reporting API: views, CTR, AVD,
   per-video retention rows when available, top titles, series arcs) when OAuth is connected. First read
   `analytics_data_quality`. Use `growth_playbook` for brand_new / early / growing / established.
   For 0-sub channels: positioning + competitor homework, NOT "why X failed."
4. `get_public_search_trends` — demand when harvest is thin or channel is new.
5. `get_studio_credits` before expensive renders; low balance → Wallet top-up (unlimited purchases).

Always explain: what's working, what's not, recommended next 1–3 actions, then offer to render.

═══ PREMIUM LONG-FORM (documentaries — Jake Tran / Magnates / MrBeast pacing bar) ═══
Quality target: feels like a $5k+ edit — NOT "good enough AI."
- Voice: ElevenLabs on channel config (`voice_provider_default`); never downgrade to cheap TTS unless user insists.
- Script: `load_skill script-writing` + CHANNEL.md; cold open hook in first 8s; pattern interrupts every 45–90s;
  no dead air; escalate stakes; land a crisp outro CTA.
- Visuals: photoreal premium stills per channel FLOW; stat cards / motion graphics where channel allows.
- Deliver: 4K/UHD when pipeline supports it; default to highest tier the channel registry specifies.
- Thumbnails: `thumbnail-design` skill BEFORE proposing upload package.
- Final package must include title, description, tags, timestamps, and selected/approved thumbnail guidance.
- `start_longform_render` after outline approval; poll until complete.

═══ SHORTFORM RENDER (start_shortform_generate + poll_render_job kind=shortform) ═══
REQUIRED on every short render: `render_style` from list_render_styles OR the user's session Art Style picker.
- Shorts quality target: benchmark against the selected channel's own Shorts outliers and niche-specific
  high-retention Shorts. Optimize viewed-vs-swiped, first-1-to-3-second retention, completion/APV, rewatches,
  engaged views, and interactions per view. Do not use long-form CTR/AVD/chapter benchmarks as substitutes.
- Default for most channels: cinematic, ultra_realism, comic_book, historical_18th_century, etc. — real subjects.
- `skeleton_host` is a niche art style like comic or Ghibli — use it only when the user picked Skeleton in Art Style.
- Before approving render, state the render_style label so the user sees what visuals they are buying.
- Final package must include title, tags, description, hashtags, timestamps/beat map, and CTA notes.
- The selected chat channel is the source of truth for watermark, copyright/branding, CTA, and package copy. Never reuse
  ZeroTier branding on skeleton channels, Empire Magnates copy on ZeroTier, or any other cross-channel brand.
- Captions are optional. If the user asks for no captions, pass captions_enabled=false / caption_mode=off.
  If captions are enabled, default to word-level captions: one spoken word per caption, synced to the narration.
- For skeleton_host shorts, selected-channel branding and the short package are default render outputs.
  Captions follow the selected CC mode; use word captions only when captions are enabled or requested.
  Do not ask the user to re-edit just to get those defaults.
- Do NOT generate thumbnail candidates for short-form by default. Use the strongest frame/cover from the finished Short
  unless the user explicitly asks for a custom short thumbnail.
- `start_shortform_generate` is a still-review gate, not a final render. Never claim the video is complete from that tool.
  After it reaches awaiting_scene_review, show/list the stills, edit bad stills, then wait for explicit scene approval before
  calling `animate_production_scenes` or `finalize_production`.

FULL CREATIVE CONTROL (the massive per-scene iteration system):
After start_shortform_generate (non-skeleton styles land at a review gate with stills):
- Global visual edit rule: if the user says "every scene", "all scenes", or gives a global wardrobe/character rule like
  "put the skeleton in a doctor's uniform", call `edit_production_scenes_still(job_id, instruction, scope="character")`
  once and keep the job at still review. Do not animate or finalize until the user approves the updated stills.
- `list_production_scenes(job_id)` → see every scene, its current still, animate flag, duration, last_edit, motion_prompt.
- `edit_production_scene_still(job_id, scene_index, instruction, scope)` → use Seedream V4.5 *edit* (image reference + natural language) to change exactly that scene. Use `scope="character"` for mannequin/skeleton/wardrobe/pose/body changes, `scope="background"` for environment swaps while preserving the subject, `scope="props"` for held items/screens/objects, and `scope="full"` only when the whole frame should change.
- For identity-consistent characters: do the cheap premium two-pass loop. First edit the character with `scope="character"` until the mannequin/skeleton is right. Then edit the world with `scope="background"` while preserving that exact character. Do this instead of regenerating the whole scene unless the user asks for a fresh variation.
- `regenerate_production_scene_still(job_id, scene_index)` → new seed on the base prompt.
- `set_production_scenes_animate(job_id, scene_indices=[3,7,12], animate=true/false)` or for all.
  This is also the explicit still approval step. Use animate=true only for scenes that should spend i2v; use animate=false
  for approved still/Ken Burns scenes.
- `set_production_scene_duration(job_id, scene_index, duration_sec=4.5)` → precise pacing control per beat.
- `animate_production_scenes(job_id, scene_indices=[3,7])` → run i2v *only* on the chosen ones (or all currently flagged animate=true).
- `finalize_production(job_id)` → any missing motion uses Ken Burns, full VO + captions + mux. Produces the final MP4 with exactly the mix you chose.
You (the agent) can stay in a tight loop with the user: "I edited scene 4 with your note, here's the new still. Animate it? Or another change?" until they say the scenes are perfect, then finalize. This is how you achieve pixel-perfect pacing and visuals over many iterations. For long 30-min docs the same philosophy applies via longform + its regenerate tools + chapter approval.

═══ SKELETON NICHE (render_style=skeleton_host) ═══
MANUAL CHARACTER / STILL EDIT RULE:
- If the user replies to an image/still/video card and asks to change the skeleton, mannequin, character, clothing,
  pose, expression, props, or background, do not answer with a text promise.
- If the request applies to every scene/all scenes, use `edit_production_scenes_still` once with scope="character" for
  wardrobe/body/pose changes. Never use re_edit_production or finalize_production for this still-review edit.
- Call `list_production_scenes(job_id)` first, identify the relevant scene(s), then call
  `edit_production_scene_still(job_id, scene_index, instruction, scope)`.
- Use `scope="character"` for skeleton/mannequin/body/wardrobe/pose edits, `scope="background"` for location-only
  edits, and `scope="props"` for held objects. Preserve the selected channel watermark/package rules.
- After the still edit, report the changed scene and ask whether to animate/finalize or make another still edit.

When the user's Art Style is skeleton_host:
Never use load_skill image-generation for scene stills — Seedream edit from canonical master only.

STILLS (locked — not user-selectable):
- One canonical master PNG → every scene is Seedream 4.5 **edit** (`seedream_v45_edit_canonical`).
- SAME skeleton every beat: ivory bones, glass shell, realistic eyes. Identity never changes.
- Per scene, edit ONLY: background/environment, outfit/clothes, props, pose.
- Need muscles? Add muscle definition ON the same skeleton (wardrobe/body overlay) — not a new character.
- Need clothes? Edit wardrobe on the same skeleton. Different location? Edit background only.
- Rinse and repeat: master → edit → master → edit for every beat.

VIDEO (user-selectable — ask if unclear):
- Call `list_skeleton_video_models` and pass `video_model` to start_shortform_generate:
  `seedance` (default, 5 AC), `pixverse` (permissive), `kling_pro` (7 AC, best motion).

SCRIPT CATEGORY:
- Call `list_skeleton_categories` (20 YouTube lanes + user custom). Use `outcast` for edgy/contrarian.
- Missing lane → `create_skeleton_category` then render with returned key.

WARDROBE / STYLING (same skeleton — not a new character):
- When the user specifies clothes, age vibe, muscles, or props, pass it in `visual_brief` on
  `start_shortform_generate` (e.g. "teenager 18+, black hoodie and black pants, urban night").
- This locks outfit/props on the canonical skeleton; Seedream edit changes background + wardrobe only.

Do NOT tell users only classical_clash / wildcard_clash exist. Do NOT pick image models for skeleton shorts.

═══ Long-form (up to ~15 minutes or 30+ min documentaries) + thumbnails ═══
- Long-form: `start_longform_render` + `load_skill script-writing` + channel CHANNEL.md.
  Target ~8–15 chapters for a 15-minute doc; use compliance-preflight on YMYL channels.
- Thumbnails: `load_skill thumbnail-design` before proposing; cite channel grammar.
- Poll `poll_render_job` with kind=longform until complete.
- Granular control: After chapters are ready or in the render phase use `list_longform_scenes(job_id)` + `regenerate_longform_still(job_id, global_scene_idx)` to re-do specific stills with the longform image model. For full per-scene animate selection on a long doc, guide the user through chapter approval then use the still review + selective re-renders before the final compose (the pipeline already supports mixed animation costs and per-scene motion). The same "edit until perfect, animate only the hero moments" philosophy applies.

═══ Short-form & long-form without Skeleton AI (default for most creators) ═══
Rookcast skills at studio/skills/ — load_skill before steps. image-generation skills apply
to photoreal / channel stills, reference blueprints, thumbnails, and b-roll plans.
Before quoting spend, call get_fal_pricing when helpful.
YMYL: compliance-preflight + .gov sources. Follow CHANNEL.md when channel_key is set.

When proposing renders, explain cost/risk and which pipeline (blueprint short, long-form, Skeleton AI, etc.).
For topic research use get_public_search_trends and get_channel_analytics.
After starting a render, poll poll_render_job until complete or failed. The Studio UI also
auto-polls production jobs: live progress lines in chat, a production rail, bottom-right
monitor, stills gallery at awaiting_approval, one-click Finalize, and in-chat MP4 download.
Use finalize_longform_render when long-form hits awaiting_approval (or tell user to click
Finalize in chat). For shortform non-skeleton jobs the powerful scene control tools (list/edit/ selective animate / finalize) give you the ability to iterate individual scenes with V4.5 edit as many times as needed and choose exactly which ones get real motion. refresh_channel_intelligence after uploads; record_production_feedback
when the user reports performance (internal training, never sold).

Progress reporting (important): long-running tools run in the background and return a job_id.
- analyze_reference_video / analyze_competitor_video: poll kind=competitor through pacing + audio.
- Never go silent between start and finish. Summarize pacing (avg shot length) + hook window.

Data: every turn and tool call is logged for product improvement and future custom model training
(previews only; no secrets). Encourage users to connect YouTube and paste reference URLs — richer signal.

REPLY-TO RE-EDIT SUPPORT: Users can click the small "Reply & re-edit" arrow (or "Reply & re-edit" button under the player) on any completed video card in chat.
This sets up a reply context with the exact prior job_id + kind. When the incoming user message has this context (it will be prefixed with "[User is replying to their previous ... RE-EDIT that exact video]"), you **must** treat it as a request to surgically improve *that specific video the user was just shown*, not generate a fresh one.

Mandatory flow for almost all re-edit replies:
1. Call list_production_scenes (or list_longform_scenes) on the job_id from the context so you can see the current stills, durations, animate flags, narrations.
2. If the instruction calls for visual changes to particular scenes, use edit_production_scene_still (the Seedream V4.5 *edit* tool) on only those indices — never full new T2I for the whole thing. Pick `scope="character"`, `scope="background"`, `scope="props"`, or `scope="full"` based on what must stay locked.
3. For pacing, story flow, caption rhythm, or CTA issues, use set_production_scene_duration (selective), set_production_scenes_animate (selective indices), etc.
4. Call re_edit_production(job_id=..., instruction=the user's full request text, kind=...) — this is the dedicated tool for the "re-edit the same video" use case. It records the intent, re-uses the prior assets, and drives a re-finalize that produces a new MP4 + package.txt with proper editing, tighter pacing, instruction-matched captions, visual-VO lockstep, and a subscribe CTA at the end.
Only fall back to a full new start_*_generate if the user explicitly says "make a completely new one" or "change the entire art style and start over".

The user does real video editing and wants the AI to study real references (via analyze_youtube_video + yt-dlp when they paste links) and apply the exact observed decisions (hook length, cut rhythm, caption timing, CTA placement) on these re-edits. Re-use the video they already have; don't waste it by regenerating everything.

{skills.skills_index_for_prompt()}
"""


async def run_turn(
    session: dict[str, Any],
    user_text: str,
    *,
    membership_plan: str = "",
    billing_profile: dict[str, Any] | None = None,
    reply_to: dict | None = None,
    attachments: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Process one user message; may queue pending_actions in confirm mode."""
    user_id = str(session.get("user_id") or "")
    profile = billing_profile or {}
    async with studio_agent_slot(
        user_id=user_id,
        plan=membership_plan,
        operation="chat",
        unlimited=bool(profile.get("unlimited")),
    ) as admission:
        result = await _run_turn_impl(session, user_text, billing_profile=billing_profile, reply_to=reply_to, attachments=attachments)
        if admission.mode != "disabled":
            result["queue"] = admission.as_dict()
        return result


async def stream_turn(
    session: dict[str, Any],
    user_text: str,
    *,
    membership_plan: str = "",
    billing_profile: dict[str, Any] | None = None,
    reply_to: dict | None = None,
    attachments: list[dict[str, Any]] | None = None,
    run_id: str | None = None,
) -> AsyncIterator[str]:
    """SSE stream of tool/status events, ending with event=done."""
    import json as _json

    user_id = str(session.get("user_id") or "")
    profile = billing_profile or {}

    def _sse(event: str, data: dict[str, Any]) -> str:
        return f"event: {event}\ndata: {_json.dumps(data, default=str)}\n\n"

    queue: asyncio.Queue[tuple[str, dict[str, Any] | None]] = asyncio.Queue()

    async def emit(payload: dict[str, Any]) -> None:
        if run_id:
            try:
                store.append_run_event(
                    session["session_id"],
                    run_id,
                    str(payload.get("event") or "status"),
                    payload,
                )
            except Exception:
                pass
        await queue.put(("event", payload))

    async def worker() -> None:
        try:
            async with studio_agent_slot(
                user_id=user_id,
                plan=membership_plan,
                operation="chat",
                unlimited=bool(profile.get("unlimited")),
            ) as admission:
                result = await _run_turn_impl(
                    session,
                    user_text,
                    billing_profile=billing_profile,
                    emit=emit,
                    reply_to=reply_to,
                    attachments=attachments,
                )
                if admission.mode != "disabled":
                    result["queue"] = admission.as_dict()
                if run_id:
                    result["run_id"] = run_id
                    try:
                        store.append_run_event(session["session_id"], run_id, "done", {"event": "done"})
                        store.finish_run(session["session_id"], run_id, status="complete")
                    except Exception:
                        pass
                await queue.put(("done", result))
        except (StudioAgentQueueFullError, StudioAgentQueueTimeoutError) as exc:
            if run_id:
                try:
                    store.append_run_event(session["session_id"], run_id, "error", {"message": str(exc), "queue": True})
                    store.finish_run(session["session_id"], run_id, status="failed", error=str(exc))
                except Exception:
                    pass
            await queue.put(("error", {"message": str(exc), "queue": True}))
        except Exception as exc:
            if _is_model_credit_error(exc):
                assistant_text = _model_credit_recovery_message(exc)
                result = {
                    "assistant_message": assistant_text,
                    "pending_actions": list(session.get("pending_actions") or []),
                    "active_jobs": list(session.get("active_jobs") or []),
                    "error_kind": "model_credit_limit",
                    "recoverable": True,
                    "run_id": run_id,
                }
                try:
                    store.append_messages(session["session_id"], [{"role": "assistant", "content": assistant_text}])
                    if run_id:
                        store.append_run_event(
                            session["session_id"],
                            run_id,
                            "needs_attention",
                            {"message": assistant_text, "error_kind": "model_credit_limit"},
                        )
                        store.finish_run(session["session_id"], run_id, status="complete")
                except Exception:
                    pass
                await queue.put(("done", result))
                return
            if run_id:
                try:
                    store.append_run_event(session["session_id"], run_id, "error", {"message": str(exc)})
                    store.finish_run(session["session_id"], run_id, status="failed", error=str(exc))
                except Exception:
                    pass
            await queue.put(("error", {"message": str(exc)}))

    task = asyncio.create_task(worker())
    last_persisted_heartbeat = 0.0
    try:
        while True:
            try:
                kind, payload = await asyncio.wait_for(queue.get(), timeout=15.0)
            except asyncio.TimeoutError:
                heartbeat = {"event": "status", "message": "Still working..."}
                if run_id:
                    try:
                        now = time.time()
                        if now - last_persisted_heartbeat >= 60:
                            store.append_run_event(session["session_id"], run_id, "status", heartbeat)
                            last_persisted_heartbeat = now
                    except Exception:
                        pass
                yield _sse("status", heartbeat)
                continue
            if kind == "event" and payload:
                ev = str(payload.get("event") or "status")
                yield _sse(ev, payload)
                continue
            if kind == "done" and payload:
                yield _sse("done", payload)
                break
            if kind == "error":
                yield _sse("error", payload or {"message": "Agent turn failed"})
                break
    finally:
        # Do not cancel the agent turn when the browser stream disconnects.
        # Long research/planning turns can outlive a flaky tab/proxy stream; the
        # worker still saves the transcript when it finishes, and the UI can
        # reload the session/history. Cancelling here was making Studio Agent
        # look like it "did nothing" after a frontend timeout.
        if run_id and not task.done():
            try:
                store.append_run_event(
                    session["session_id"],
                    run_id,
                    "stream_disconnected",
                    {"message": "Browser stream detached; backend run continues."},
                )
            except Exception:
                pass


async def _run_turn_impl(
    session: dict[str, Any],
    user_text: str,
    *,
    billing_profile: dict[str, Any] | None = None,
    emit: EventEmitter | None = None,
    reply_to: dict | None = None,
    attachments: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    sid = session["session_id"]
    user_id = session["user_id"]
    turn_id = f"turn_{uuid.uuid4().hex}"
    original_user_text = str(user_text or "")
    model = session.get("model") or openrouter.DEFAULT_MODEL
    approval_mode = session.get("approval_mode") or "confirm"
    content_format = session.get("content_format") or "both"
    reasoning_depth = session.get("reasoning_depth") or "balanced"
    web_search = bool(session.get("web_search", True))
    active_registry = _active_registry_key(session, user_text)
    active_channel_id = str(session.get("channel_id") or "").strip()
    if active_registry and active_registry != str(session.get("registry_key") or "").strip():
        session = store.update_session(sid, registry_key=active_registry) or session
    persisted_attachment_paths = _persist_image_attachments(sid, attachments)
    if persisted_attachment_paths:
        session = store.update_session(
            sid,
            latest_attachment_paths=persisted_attachment_paths,
            latest_attachment_at=time.time(),
        ) or session
        user_text = (
            f"{user_text}\n\n[Studio system: {len(persisted_attachment_paths)} image attachment(s) "
            "were persisted for production. For a product advertisement, call "
            "ingest_product_reference with use_attached_images=true.]"
        )
    training_capture.capture_event(
        str(user_id),
        "user_turn",
        {
            "text": original_user_text,
            "content_format": content_format,
            "reasoning_depth": reasoning_depth,
            "approval_mode": approval_mode,
            "channel_id": active_channel_id,
            "registry_key": active_registry,
            "reply_to": reply_to or {},
        },
        session_id=sid,
        turn_id=turn_id,
    )
    for attachment_path in persisted_attachment_paths:
        training_capture.capture_event(
            str(user_id),
            "attachment",
            {"artifact": training_capture.artifact_manifest(attachment_path, role="user_reference")},
            session_id=sid,
            turn_id=turn_id,
        )

    messages: list[dict[str, Any]] = list(session.get("messages") or [])
    messages.append({"role": "user", "content": _build_user_content(user_text, attachments)})
    store.touch_title_from_user_message(sid, user_text)
    try:
        memory.observe_user_message(str(user_id), user_text, session=session)
    except Exception:
        pass

    if not reply_to and _is_job_status_followup(user_text):
        poll_target = _recover_poll_target(session)
        if poll_target:
            job_id, kind = poll_target
            await _fire_event(emit, "tool_start", tool="poll_render_job", round=0, awaiting_approval=False)
            try:
                result = execute_tool_logged(
                    "poll_render_job",
                    {"job_id": job_id, "kind": kind},
                    user_id=user_id,
                    content_format=content_format,
                    session_id=sid,
                )
            except Exception as exc:
                result = json.dumps({"job_id": job_id, "kind": kind, "error": str(exc)})
            assistant_text = _format_polled_job_status(result)
            messages.append(_tool_observation_message("poll_render_job", result))
            messages.append({"role": "assistant", "content": assistant_text})
            try:
                polled = json.loads(result or "{}")
            except Exception:
                polled = {}
            polled_status = str(polled.get("status") or polled.get("phase") or polled.get("stage") or "running").lower() if isinstance(polled, dict) else "running"
            still_active = polled_status not in {"complete", "failed", "error", "cancelled", "awaiting_scene_review", "awaiting_approval"}
            active_jobs = [{"job_id": job_id, "kind": kind, "started_at": time.time()}] if still_active else [
                j for j in list(session.get("active_jobs") or [])
                if str(j.get("job_id") or "") != job_id
            ]
            store.update_session(
                sid,
                messages=messages,
                active_jobs=active_jobs,
            )
            await _fire_event(emit, "tool_end", tool="poll_render_job", status="ok")
            return {
                "session_id": sid,
                "assistant_message": assistant_text,
                "pending_actions": list(session.get("pending_actions") or []),
                "active_jobs": active_jobs,
                "approval_mode": approval_mode,
                "reasoning_depth": reasoning_depth,
                "usage": {},
                "billing": {
                    "credits_charged": 0,
                    "provider_usd": 0.0,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                },
            }

    # Explicit production starts are deterministic. If the model previously
    # prepared a start action but narrated instead of firing it, recover the
    # exact stored arguments and surface/execute them before another LLM call.
    if not reply_to and _wants_production_execution(user_text):
        existing_pending = list(session.get("pending_actions") or [])
        if existing_pending:
            assistant_text = "The production is already prepared and waiting for your approval."
            messages.append({"role": "assistant", "content": assistant_text})
            store.update_session(sid, messages=messages, pending_actions=existing_pending)
            await _fire_event(emit, "pending_actions", actions=existing_pending)
            return {
                "session_id": sid,
                "assistant_message": assistant_text,
                "pending_actions": existing_pending,
                "active_jobs": list(session.get("active_jobs") or []),
                "approval_mode": approval_mode,
                "reasoning_depth": reasoning_depth,
                "usage": {},
                "billing": {
                    "credits_charged": 0,
                    "provider_usd": 0.0,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                },
            }
        recovered = _recover_requested_production(session, user_text)
        if recovered:
            name, args = recovered
            if name == "start_shortform_generate":
                args = _inject_shortform_render_style(args, session)
                args = _inject_shortform_caption_options(args, session)
                args = _normalize_shortform_category_args(args)
            args = _channel_guard_tool_args(name, args, active_registry, active_channel_id)
            budget_payload = None
            try:
                estimate = production_budget.enforce_budget(name, args)
                if estimate is not None:
                    budget_payload = estimate.as_dict()
            except production_budget.BudgetExceededError as exc:
                assistant_text = f"Studio could not prepare the production: {exc}"
                messages.append({"role": "assistant", "content": assistant_text})
                store.update_session(sid, messages=messages)
                await _fire_event(emit, "tool_end", tool=name, status="error", error="budget_exceeded")
                return {
                    "session_id": sid,
                    "assistant_message": assistant_text,
                    "pending_actions": [],
                    "active_jobs": list(session.get("active_jobs") or []),
                    "approval_mode": approval_mode,
                    "reasoning_depth": reasoning_depth,
                    "usage": {},
                    "billing": {
                        "credits_charged": 0,
                        "provider_usd": 0.0,
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                    },
                }
            await _fire_event(
                emit,
                "tool_start",
                tool=name,
                round=0,
                awaiting_approval=approval_mode == "confirm" and requires_approval(name),
                deterministic_start=True,
            )
            if approval_mode == "confirm" and requires_approval(name):
                action_id = f"act_{uuid.uuid4().hex[:12]}"
                action = {
                    "id": action_id,
                    "tool": name,
                    "arguments": args,
                    "summary": f"{name}({json.dumps(args)[:200]})",
                    "budget": budget_payload,
                }
                assistant_text = "The production is prepared correctly and waiting for your approval."
                messages.append({"role": "assistant", "content": assistant_text})
                store.update_session(
                    sid,
                    messages=messages,
                    pending_actions=[action],
                    last_production={
                        "tool": name,
                        "arguments": args,
                        "updated_at": time.time(),
                    },
                )
                await _fire_event(emit, "pending_actions", actions=[action])
                await _fire_event(emit, "tool_end", tool=name, status="awaiting_approval")
                return {
                    "session_id": sid,
                    "assistant_message": assistant_text,
                    "pending_actions": [action],
                    "active_jobs": list(session.get("active_jobs") or []),
                    "approval_mode": approval_mode,
                    "reasoning_depth": reasoning_depth,
                    "usage": {},
                    "billing": {
                        "credits_charged": 0,
                        "provider_usd": 0.0,
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                    },
                }
            try:
                result = execute_tool_logged(
                    name,
                    args,
                    user_id=user_id,
                    content_format=content_format,
                    session_id=sid,
                )
            except Exception as exc:
                result = json.dumps({"error": str(exc)})
            parsed_result: dict[str, Any] = {}
            try:
                loaded = json.loads(result or "{}")
                if isinstance(loaded, dict):
                    parsed_result = loaded
            except Exception:
                pass
            error = str(parsed_result.get("error") or "").strip()
            if error:
                assistant_text = f"Studio tried to start the production, but the backend returned: {error}"
                active_jobs = list(session.get("active_jobs") or [])
                await _fire_event(emit, "tool_end", tool=name, status="error", error=error[:160])
            else:
                started = extract_jobs_from_tool(name, result)
                active_jobs = merge_active_jobs(list(session.get("active_jobs") or []), started)
                status = str(parsed_result.get("status") or "started").replace("_", " ")
                job_id = str(parsed_result.get("job_id") or "").strip()
                assistant_text = (
                    f"Production started successfully{f' as {job_id}' if job_id else ''}. "
                    f"Current status: {status}."
                )
                await _fire_event(emit, "active_jobs", jobs=active_jobs)
                await _fire_event(emit, "tool_end", tool=name, status="ok")
            messages.append(_tool_observation_message(name, result))
            messages.append({"role": "assistant", "content": assistant_text})
            store.update_session(
                sid,
                messages=messages,
                active_jobs=active_jobs,
                last_production={
                    "tool": name,
                    "arguments": args,
                    "updated_at": time.time(),
                },
            )
            return {
                "session_id": sid,
                "assistant_message": assistant_text,
                "pending_actions": [],
                "active_jobs": active_jobs,
                "approval_mode": approval_mode,
                "reasoning_depth": reasoning_depth,
                "usage": {},
                "billing": {
                    "credits_charged": 0,
                    "provider_usd": 0.0,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                },
            }

    if reply_to and not _is_manual_visual_edit_request(user_text, reply_to):
        job_id = str(reply_to.get("job_id") or "")
        kind = str(reply_to.get("kind") or "shortform")
        scene_index_raw = reply_to.get("scene_index")
        scene_hint = ""
        try:
            if scene_index_raw is not None:
                scene_hint = f" User clicked scene_index={int(scene_index_raw)}; prioritize that scene unless the request clearly names another scene."
        except Exception:
            scene_hint = ""
        is_long = kind.lower().startswith("long")
        scene_tool_hint = "list_longform_scenes + regenerate_longform_still + longform finalize tools" if is_long else "list_production_scenes + edit_production_scene_still (V4.5 edit) + set_production_scenes_animate + set_production_scene_duration + animate_production_scenes + finalize_production"
        context_note = (
            f"[User is replying to their previous {kind} video production (job_id={job_id}). "
            f"{scene_hint} "
            "Treat the following message as instructions to RE-EDIT **that exact video** the user was just shown (do not start a brand new generation or regenerate all stills unless they explicitly say 'start over' or 'new visual style'). "
            "Goal: proper editing, pacing, storytelling, packaging + a clear subscribe CTA at the end. "
            f"First inspect with list_production_scenes (or list_longform_scenes). Use targeted edits only where the instruction requires (edit_production_scene_still for V4.5 edits on specific scenes, set_production_scene_duration, set_production_scenes_animate for selective animation). "
            "Then call the dedicated re_edit_production(job_id, instruction=the user's exact request, kind=...) tool — this is the correct surgical path that re-uses the prior stills/clips/video the user already has and only re-assembles with better timing, lockstep VO, instruction-matched captions (single-word captions when requested), and CTA. NEVER call start_shortform_generate or start_longform_render during a reply-to re-edit — those create brand new jobs and full visual regeneration. "
            "After it finishes, the new improved deliverable (same job) will appear in chat for the user.]"
        )
        messages[-1]["content"] = _build_user_content(context_note + "\n\n" + user_text, attachments)

        # Pre-load the current state of the video being re-edited so the model sees the exact scenes/stills/narrations
        # immediately. Include as text in the note (avoids breaking chat message alternation with bare "tool" role).
        try:
            list_tool = "list_longform_scenes" if is_long else "list_production_scenes"
            pre_list = execute_tool_logged(
                list_tool,
                {"job_id": job_id},
                user_id=user_id,
                content_format=content_format,
                session_id=sid,
            )
            # Append the scene list as plain text observation so the model has the current state without format issues.
            _append_text_to_message(
                messages[-1],
                f"\n\n[Current scenes state for this job (pre-loaded for surgical re-edit):\n{pre_list}\n]",
            )
        except Exception as pre_exc:
            # Non-fatal; the model can still call list itself.
            pass
    telemetry.record_session_turn(
        user_id, sid, role="user", content_preview=user_text,
        model=session.get("model"), content_format=content_format,
    )

    profile = billing_profile or {}
    owner_unmetered = bool(profile.get("unlimited"))
    try:
        memory_summary = memory.summarize_for_prompt(
            str(user_id),
            channel_id=str(session.get("channel_id") or ""),
            registry_key=str(session.get("registry_key") or ""),
        )
        archived_memory = store.channel_archive_context(
            str(user_id),
            channel_id=str(session.get("channel_id") or ""),
            registry_key=str(session.get("registry_key") or ""),
            channel_title=str(session.get("channel_title") or ""),
        )
        if archived_memory:
            memory_summary = f"{memory_summary}\n\n{archived_memory}".strip()
    except Exception:
        memory_summary = ""
    sys_content = system_prompt(
        content_format=content_format,
        reasoning_depth=reasoning_depth,
        billing_profile=profile,
        render_style=str(session.get("render_style") or "cinematic"),
        memory_summary=memory_summary,
        active_registry=active_registry,
    )
    if messages and messages[0].get("role") == "system":
        messages[0] = {"role": "system", "content": sys_content}
    else:
        messages.insert(0, {"role": "system", "content": sys_content})
    store.update_session(sid, messages=messages)
    compacted = store.compact_session_if_needed(sid)
    if compacted:
        session = compacted
        messages = list(session.get("messages") or messages)

    channel_data_preflight_required = _needs_channel_data_preflight(user_text)
    latest_upload_focus_required = _needs_latest_upload_focus(user_text)
    if not channel_data_preflight_required and _is_channel_data_followup(user_text):
        channel_data_preflight_required = _recent_assistant_promised_channel_data(messages)
    public_search_preflight_required = _needs_public_search_preflight(user_text)
    if not public_search_preflight_required and _is_channel_data_followup(user_text):
        public_search_preflight_required = _recent_assistant_promised_channel_data(messages)

    preflight_tool_fires: list[ToolFire] = []
    if (active_registry or active_channel_id) and (channel_data_preflight_required or public_search_preflight_required):
        active_label = (
            str(session.get("channel_title") or "").strip()
            or active_registry.replace("_", " ")
            or "selected"
        )
        preflight_plan: list[tuple[str, dict[str, Any]]] = []
        if channel_data_preflight_required:
            analytics_args = {"registry_key": active_registry, "channel_id": active_channel_id}
            if latest_upload_focus_required:
                analytics_args["focus"] = "latest_upload"
            preflight_plan.extend([
                ("list_youtube_channels", {}),
                (
                    "get_channel_analytics",
                    analytics_args,
                ),
                (
                    "recommend_video_topics",
                    {"registry_key": active_registry, "channel_id": active_channel_id, "days": 30},
                ),
            ])
        if public_search_preflight_required:
            preflight_plan.append((
                "get_public_search_trends",
                {
                    "registry_key": active_registry,
                    "query": _public_search_query_for_channel(active_label, user_text),
                    "days": 30,
                },
            ))
        deduped_plan: list[tuple[str, dict[str, Any]]] = []
        seen_plan: set[tuple[str, str]] = set()
        for pf_name, pf_args in preflight_plan:
            key = (pf_name, json.dumps(pf_args, sort_keys=True))
            if key in seen_plan:
                continue
            seen_plan.add(key)
            deduped_plan.append((pf_name, pf_args))
        preflight_plan = deduped_plan
        if channel_data_preflight_required and public_search_preflight_required:
            scope = "channel/search data"
        elif public_search_preflight_required:
            scope = "public search data"
        else:
            scope = "channel data"
        await _fire_event(emit, "status", message=f"Pulling {active_label} {scope}...")
        for pf_name, pf_args in preflight_plan:
            await _fire_event(emit, "tool_start", tool=pf_name, round=0, awaiting_approval=False)
            try:
                pf_result = execute_tool_logged(
                    pf_name,
                    pf_args,
                    user_id=user_id,
                    content_format=content_format,
                    session_id=sid,
                )
            except Exception as exc:
                pf_result = json.dumps({"error": str(exc)}, indent=2)
            preflight_tool_fires.append(ToolFire(pf_name, dict(pf_args), pf_result))
            try:
                memory.observe_tool_result(str(user_id), pf_name, pf_args, pf_result)
            except Exception:
                pass
            messages.append(_tool_observation_message(pf_name, pf_result))
            store.update_session(sid, messages=messages)
            err_preview = ""
            try:
                parsed_pf = json.loads(pf_result or "{}")
                if isinstance(parsed_pf, dict) and parsed_pf.get("error"):
                    err_preview = str(parsed_pf.get("error"))[:160]
            except Exception:
                pass
            await _fire_event(
                emit,
                "tool_end",
                tool=pf_name,
                status="error" if err_preview else "ok",
                error=err_preview or None,
            )
        if preflight_tool_fires:
            messages.append({
                "role": "system",
                "content": (
                    "[Studio Agent required data preflight is complete for this turn. "
                    "Answer from the tool observations already in this context. Do not say you still need to pull "
                    "channel/search data or that you will use the analytics/search tool next; if a tool returned limited data, "
                    "state the exact limitation from the tool payload and then give the grounded next step.]"
                ),
            })
            store.update_session(sid, messages=messages)

    if reply_to:
        job_id = str(reply_to.get("job_id") or "").strip()
        kind = str(reply_to.get("kind") or "shortform").strip() or "shortform"
        if job_id:
            tool_fires: list[ToolFire] = []
            await _fire_event(emit, "tool_start", tool="re_edit_production", round=1, awaiting_approval=False)
            try:
                reedit_res = re_edit_production(job_id, user_text, kind)
                parsed = json.loads(reedit_res or "{}")
                status = "error" if _production_result_failed(parsed) else "ok"
                tool_fires.append(ToolFire("re_edit_production", {"job_id": job_id, "kind": kind}, reedit_res))
            except Exception as exc:
                parsed = {"error": str(exc)}
                reedit_res = json.dumps(parsed, indent=2)
                status = "error"
            await _fire_event(
                emit,
                "tool_end",
                tool="re_edit_production",
                status=status,
                error=str(parsed.get("error", ""))[:120] if isinstance(parsed, dict) and parsed.get("error") else None,
            )

            active_jobs = []
            if status == "ok":
                active_jobs = [{
                    "job_id": job_id,
                    "kind": "longform" if kind.lower().startswith("long") else "shortform",
                    "title": "Re-edited production",
                    "started_at": time.time(),
                }]
                store.update_session(sid, active_jobs=active_jobs)
                await _fire_event(emit, "active_jobs", jobs=active_jobs)

            if status == "ok" and _production_result_complete(parsed):
                assistant_text = (
                    "Re-edit complete. Studio has a completed production result and the finished MP4 is available "
                    "from the video card/download link."
                )
            elif status == "ok":
                assistant_text = (
                    "Re-edit is running on that exact production instead of starting over. "
                    "Track the render card for the finished MP4; I should not call it complete until the production result is complete."
                )
            else:
                assistant_text = (
                    "Re-edit failed before completion. I tried to recompose the exact production and hit this error: "
                    f"{str(parsed.get('error') if isinstance(parsed, dict) else reedit_res)[:500]}"
                )
            audit = audit_turn(assistant_text=assistant_text, user_text=user_text, tool_fires=tool_fires)
            if audit.has_issues:
                correction = audit.for_history_correction()
                if correction:
                    messages.append({"role": "system", "content": correction})
                assistant_text = sanitize_assistant_text(guard_text(assistant_text, audit))
            messages.append({"role": "system", "content": f"[Studio Agent re-edit tool result]\n{reedit_res[:4000]}"})
            messages.append({"role": "assistant", "content": assistant_text})
            store.update_session(sid, messages=messages)
            if assistant_text:
                telemetry.record_session_turn(
                    user_id, sid, role="assistant", content_preview=assistant_text,
                    model=model, content_format=content_format,
                )
            return {
                "session_id": sid,
                "assistant_message": assistant_text,
                "pending_actions": [],
                "active_jobs": active_jobs,
                "approval_mode": approval_mode,
                "reasoning_depth": reasoning_depth,
                "usage": {},
                "billing": {
                    "credits_charged": 0,
                    "provider_usd": 0.0,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                },
            }

    tools = tool_schemas()
    # During a reply-to re-edit (user clicked the arrow on a specific video card),
    # completely remove ALL high-level "start a whole new production" tools from what the model can call.
    # This forces the surgical re-edit path on the exact prior job_id (list scenes + granular edits + re_edit_production + finalize).
    # The model must reuse the existing stills/clips from the video the user is replying to.
    if reply_to:
        blocked_for_reedit = set(BRAND_NEW_PRODUCTION_TOOLS)
        tools = [t for t in tools if t.get("function", {}).get("name") not in blocked_for_reedit]
    pending: list[dict[str, Any]] = []
    active_jobs: list[dict[str, Any]] = []
    assistant_text = ""
    tool_fires: list[ToolFire] = list(preflight_tool_fires)
    usage_total: dict[str, Any] = {}
    acc_prompt_tokens = 0
    acc_completion_tokens = 0
    model_provider = "openrouter"
    effective_model = model

    await _fire_event(emit, "status", message="Thinking…")

    for round_idx in range(MAX_TOOL_ROUNDS):
        compacted = store.compact_session_if_needed(sid)
        if compacted:
            session = compacted
        model_messages = store.trim_messages_for_model(messages, session=session)
        await _fire_event(emit, "model_round", round=round_idx + 1)
        must_execute_tool = (
            not preflight_tool_fires
            and not tool_fires
            and _requires_tool_execution(user_text)
        )
        training_capture.capture_event(
            str(user_id),
            "model_request",
            {
                "round": round_idx + 1,
                "model": model,
                "messages": model_messages,
                "tools": tools,
                "reasoning_depth": reasoning_depth,
                "web_search": web_search,
                "force_tool_call": must_execute_tool,
            },
            session_id=sid,
            turn_id=turn_id,
        )
        resp = await openrouter.chat_completion(
            messages=model_messages,
            tools=tools,
            model=model,
            reasoning_depth=reasoning_depth,
            web_search=web_search,
            force_tool_call=must_execute_tool,
        )
        msg = openrouter.message_from_response(resp)
        usage = openrouter.usage_from_response(resp)
        training_capture.capture_event(
            str(user_id),
            "model_response",
            {
                "round": round_idx + 1,
                "provider": str(resp.get("provider") or "openrouter"),
                "model": str(resp.get("model") or model),
                "message": msg,
                "usage": usage,
            },
            session_id=sid,
            turn_id=turn_id,
        )
        model_provider = str(resp.get("provider") or "openrouter")
        effective_model = str(resp.get("model") or model)
        usage_total = usage
        acc_prompt_tokens += int(usage.get("prompt_tokens", 0) or 0)
        acc_completion_tokens += int(usage.get("completion_tokens", 0) or 0)

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
                if name == "start_shortform_generate":
                    args = _inject_shortform_render_style(args, session)
                    args = _inject_shortform_caption_options(args, session)
                    args = _normalize_shortform_category_args(args)
                args = _channel_guard_tool_args(name, args, active_registry, active_channel_id)

                # HARD GUARD for reply-to re-edit: never allow a full new production start when the user
                # explicitly clicked "Reply & re-edit" on an existing video card. Force the surgical path
                # on the exact prior job_id so we re-use the already-made stills/clips/video and only re-edit
                # pacing, captions, CTA, timing, story packaging.
                if reply_to and name in BRAND_NEW_PRODUCTION_TOOLS:
                    job_id = str(reply_to.get("job_id") or "")
                    kind = str(reply_to.get("kind") or "shortform")
                    # Use the original user instruction (the part after the injected context note)
                    raw_user = user_text
                    reedit_res = re_edit_production(job_id, raw_user, kind)
                    tool_fires.append(ToolFire("re_edit_production", {"job_id": job_id, "kind": kind}, reedit_res))
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.get("id"),
                        "content": reedit_res,
                    })
                    await _fire_event(emit, "tool_end", tool=name, status="redirected_to_reedit")
                    continue

                await _fire_event(
                    emit,
                    "tool_start",
                    tool=name,
                    round=round_idx + 1,
                    awaiting_approval=approval_mode == "confirm" and requires_approval(name),
                )

                budget_estimate = None
                if production_budget.is_budgeted_tool(name):
                    try:
                        budget_estimate = production_budget.enforce_budget(name, args)
                    except production_budget.BudgetExceededError as exc:
                        result = str(exc)
                        tool_fires.append(ToolFire(str(name), dict(args or {}), result))
                        messages.append({
                            "role": "tool",
                            "tool_call_id": tc.get("id"),
                            "content": result,
                        })
                        await _fire_event(
                            emit,
                            "tool_end",
                            tool=name,
                            status="error",
                            error="budget_exceeded",
                        )
                        continue

                if approval_mode == "confirm" and requires_approval(name):
                    action_id = f"act_{uuid.uuid4().hex[:12]}"
                    budget_summary = ""
                    budget_payload = None
                    if budget_estimate is not None:
                        budget_payload = budget_estimate.as_dict()
                        budget_summary = (
                            f" | est ${budget_payload['estimated_usd']:.2f} "
                            f"<= cap ${budget_payload['max_budget_usd']:.2f}"
                        )
                    pending.append({
                        "id": action_id,
                        "tool": name,
                        "arguments": args,
                        "summary": f"{name}({json.dumps(args)[:200]}){budget_summary}",
                        "budget": budget_payload,
                    })
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.get("id"),
                        "content": json.dumps({
                            "status": "awaiting_user_approval",
                            "action_id": action_id,
                            "message": "User must approve this action in Studio Agent UI (confirm mode).",
                            "budget": budget_payload,
                        }),
                    })
                    blocked = True
                    await _fire_event(emit, "tool_end", tool=name, status="awaiting_approval")
                    continue

                try:
                    result = execute_tool_logged(
                        name,
                        args,
                        user_id=user_id,
                        content_format=content_format,
                        session_id=sid,
                    )
                except Exception as exc:
                    result = json.dumps({"error": str(exc)})
                tool_fires.append(ToolFire(str(name), dict(args or {}), result))
                try:
                    memory.observe_tool_result(str(user_id), name, args, result)
                except Exception:
                    pass

                if name in JOB_START_TOOLS:
                    active_jobs = merge_active_jobs(
                        active_jobs,
                        extract_jobs_from_tool(name, result),
                    )
                    store.update_session(
                        sid,
                        last_production={
                            "tool": name,
                            "arguments": args,
                            "updated_at": time.time(),
                        },
                    )
                    await _fire_event(emit, "active_jobs", jobs=active_jobs)

                err_preview = ""
                try:
                    parsed = json.loads(result or "{}")
                    if isinstance(parsed, dict) and parsed.get("error"):
                        err_preview = str(parsed.get("error"))[:120]
                except json.JSONDecodeError:
                    pass
                await _fire_event(
                    emit,
                    "tool_end",
                    tool=name,
                    status="error" if err_preview else "ok",
                    error=err_preview or None,
                )

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.get("id"),
                    "content": result,
                })
                store.update_session(sid, messages=messages, active_jobs=active_jobs)

            if blocked and pending:
                await _fire_event(emit, "pending_actions", actions=pending)
                assistant_text = sanitize_assistant_text(
                    content
                    or "I prepared the next steps but need your approval before running commands that spend credits or write files."
                )
                store.update_session(sid, messages=messages, pending_actions=pending, active_jobs=active_jobs)
                break
            continue

        if must_execute_tool:
            # A required-action turn is not successful until the backend has
            # received and executed a real tool call.
            messages.append({
                "role": "system",
                "content": (
                    "[Studio execution contract violation: this request requires a real tool call, but the "
                    "model returned only text. Call the matching Studio tool now. Do not narrate, promise, "
                    "or describe the call.]"
                ),
            })
            if round_idx + 1 < MAX_TOOL_ROUNDS:
                await _fire_event(
                    emit,
                    "status",
                    message="Retrying the required Studio tool call...",
                )
                continue
            assistant_text = (
                "Studio could not execute the required tool after repeated forced attempts. "
                "No tool result was created, so I will not claim the work ran. Retry once or choose a stronger runner model."
            )
            break
        assistant_text = sanitize_assistant_text(content or "")
        break

    if not assistant_text and not pending:
        assistant_text = (
            "I hit the tool-round limit before I could produce a clean final answer. "
            "I saved the work completed so far in this chat; press Resume or send a shorter follow-up and I will continue from the saved tool results instead of starting over."
        )

    if assistant_text:
        assistant_text = sanitize_assistant_text(assistant_text)
        if (
            (active_registry or active_channel_id)
            and _has_channel_analytics_tool(tool_fires)
            and _assistant_stalled_on_channel_data(assistant_text)
        ):
            active_label = (
                str(session.get("channel_title") or "").strip()
                or str(active_registry or active_channel_id or "").replace("_", " ")
                or "selected"
            )
            if _wants_short_plan(user_text):
                assistant_text = _grounded_channel_plan_from_tools(
                    tool_fires,
                    active_label=active_label,
                    user_text=user_text,
                )
            else:
                assistant_text = _grounded_channel_status_from_tools(
                    tool_fires,
                    active_label=active_label,
                )
        audit = audit_turn(
            assistant_text=assistant_text,
            user_text=user_text,
            tool_fires=tool_fires,
        )
        if audit.has_issues and _promised_execution_blocked(audit):
            recovered = _recover_requested_production(session, user_text)
            if recovered:
                name, args = recovered
                if name == "start_shortform_generate":
                    args = _inject_shortform_render_style(args, session)
                    args = _inject_shortform_caption_options(args, session)
                    args = _normalize_shortform_category_args(args)
                args = _channel_guard_tool_args(name, args, active_registry, active_channel_id)
                await _fire_event(
                    emit,
                    "tool_start",
                    tool=name,
                    round=0,
                    awaiting_approval=approval_mode == "confirm" and requires_approval(name),
                    recovered_from_audit=True,
                )
                if approval_mode == "confirm" and requires_approval(name):
                    action_id = f"act_{uuid.uuid4().hex[:12]}"
                    budget_payload = None
                    budget_summary = ""
                    try:
                        estimate = production_budget.enforce_budget(name, args)
                        if estimate is not None:
                            budget_payload = estimate.as_dict()
                            budget_summary = (
                                f" | est ${budget_payload['estimated_usd']:.2f} "
                                f"<= cap ${budget_payload['max_budget_usd']:.2f}"
                            )
                    except production_budget.BudgetExceededError as exc:
                        assistant_text = f"Studio could not prepare the production: {exc}"
                        await _fire_event(
                            emit,
                            "tool_end",
                            tool=name,
                            status="error",
                            error="budget_exceeded",
                        )
                    else:
                        pending.append({
                            "id": action_id,
                            "tool": name,
                            "arguments": args,
                            "summary": f"{name}({json.dumps(args)[:200]}){budget_summary}",
                            "budget": budget_payload,
                        })
                        assistant_text = "The production request is prepared correctly and waiting for your approval."
                        store.update_session(
                            sid,
                            pending_actions=pending,
                            last_production={
                                "tool": name,
                                "arguments": args,
                                "updated_at": time.time(),
                            },
                        )
                        await _fire_event(emit, "pending_actions", actions=pending)
                        await _fire_event(emit, "tool_end", tool=name, status="awaiting_approval")
                else:
                    try:
                        result = execute_tool_logged(
                            name,
                            args,
                            user_id=user_id,
                            content_format=content_format,
                            session_id=sid,
                        )
                    except Exception as exc:
                        result = json.dumps({"error": str(exc)})
                    tool_fires.append(ToolFire(name, dict(args), result))
                    messages.append(_tool_observation_message(name, result))
                    try:
                        memory.observe_tool_result(str(user_id), name, args, result)
                    except Exception:
                        pass
                    parsed_result: dict[str, Any] = {}
                    try:
                        loaded = json.loads(result or "{}")
                        if isinstance(loaded, dict):
                            parsed_result = loaded
                    except Exception:
                        pass
                    error = str(parsed_result.get("error") or "").strip()
                    if error:
                        assistant_text = f"Studio tried to start the production, but the backend returned: {error}"
                        await _fire_event(emit, "tool_end", tool=name, status="error", error=error[:160])
                    else:
                        started = extract_jobs_from_tool(name, result)
                        active_jobs = merge_active_jobs(active_jobs, started)
                        status = str(parsed_result.get("status") or "started").replace("_", " ")
                        job_id = str(parsed_result.get("job_id") or "").strip()
                        assistant_text = (
                            f"Production started successfully{f' as {job_id}' if job_id else ''}. "
                            f"Current status: {status}."
                        )
                        store.update_session(
                            sid,
                            active_jobs=active_jobs,
                            last_production={
                                "tool": name,
                                "arguments": args,
                                "updated_at": time.time(),
                            },
                        )
                        await _fire_event(emit, "active_jobs", jobs=active_jobs)
                        await _fire_event(emit, "tool_end", tool=name, status="ok")
                audit = audit_turn(
                    assistant_text=assistant_text,
                    user_text=user_text,
                    tool_fires=tool_fires,
                )
        if (
            audit.has_issues
            and _missing_channel_tool_blocked(audit)
            and (active_registry or active_channel_id)
        ):
            active_label = (
                str(session.get("channel_title") or "").strip()
                or active_registry.replace("_", " ")
                or "selected"
            )
            await _fire_event(emit, "status", message=f"Pulling {active_label} channel data...")
            retry_plan = [
                ("list_youtube_channels", {}),
                (
                    "get_channel_analytics",
                    {
                        "registry_key": active_registry,
                        "channel_id": active_channel_id,
                        **({"focus": "latest_upload"} if _needs_latest_upload_focus(user_text) else {}),
                    },
                ),
                (
                    "recommend_video_topics",
                    {"registry_key": active_registry, "channel_id": active_channel_id, "days": 30},
                ),
            ]
            for pf_name, pf_args in retry_plan:
                await _fire_event(emit, "tool_start", tool=pf_name, round=0, awaiting_approval=False)
                try:
                    pf_result = execute_tool_logged(
                        pf_name,
                        pf_args,
                        user_id=user_id,
                        content_format=content_format,
                        session_id=sid,
                    )
                except Exception as exc:
                    pf_result = json.dumps({"error": str(exc)}, indent=2)
                tool_fires.append(ToolFire(pf_name, dict(pf_args), pf_result))
                try:
                    memory.observe_tool_result(str(user_id), pf_name, pf_args, pf_result)
                except Exception:
                    pass
                messages.append(_tool_observation_message(pf_name, pf_result))
                store.update_session(sid, messages=messages)
                err_preview = ""
                try:
                    parsed_pf = json.loads(pf_result or "{}")
                    if isinstance(parsed_pf, dict) and parsed_pf.get("error"):
                        err_preview = str(parsed_pf.get("error"))[:160]
                except Exception:
                    pass
                await _fire_event(
                    emit,
                    "tool_end",
                    tool=pf_name,
                    status="error" if err_preview else "ok",
                    error=err_preview or None,
                )
            messages.append({
                "role": "system",
                "content": (
                    "[Studio Agent forced channel-data retry completed because the final audit found an ungrounded "
                    "channel-performance answer. Rewrite the final answer from the tool observations above. Do not "
                    "say the analytics tool did not run. If analytics are incomplete, state the exact limitation.]"
                ),
            })
            store.update_session(sid, messages=messages)
            retry_resp = await openrouter.chat_completion(
                messages=store.trim_messages_for_model(messages, session=session),
                tools=[],
                model=model,
                reasoning_depth=reasoning_depth,
                web_search=web_search,
            )
            retry_msg = openrouter.message_from_response(retry_resp)
            retry_usage = openrouter.usage_from_response(retry_resp)
            model_provider = str(retry_resp.get("provider") or model_provider or "openrouter")
            effective_model = str(retry_resp.get("model") or effective_model or model)
            usage_total = retry_usage
            acc_prompt_tokens += int(retry_usage.get("prompt_tokens", 0) or 0)
            acc_completion_tokens += int(retry_usage.get("completion_tokens", 0) or 0)
            assistant_text = sanitize_assistant_text(str(retry_msg.get("content") or ""))
            audit = audit_turn(
                assistant_text=assistant_text,
                user_text=user_text,
                tool_fires=tool_fires,
            )
        if audit.has_issues:
            correction = audit.for_history_correction()
            if correction:
                messages.append({"role": "system", "content": correction})
            active_label = (
                str(session.get("channel_title") or "").strip()
                or active_registry.replace("_", " ")
                or "selected"
            )
            grounded_status = ""
            if (active_registry or active_channel_id) and _has_channel_analytics_tool(tool_fires):
                guarded = guard_text(assistant_text, audit)
                if (
                    "channel analytics tool did not run" in guarded.lower()
                    or "verify the missing evidence" in guarded.lower()
                    or "cannot make a grounded performance claim" in guarded.lower()
                    or "cannot name an exact high-avd winner" in guarded.lower()
                    or "video-level retention" in guarded.lower()
                    or "per-video retention" in guarded.lower()
                ):
                    if _wants_short_plan(user_text):
                        grounded_status = _grounded_channel_plan_from_tools(
                            tool_fires,
                            active_label=active_label,
                            user_text=user_text,
                        )
                    else:
                        grounded_status = _grounded_channel_status_from_tools(
                            tool_fires,
                            active_label=active_label,
                        )
            assistant_text = sanitize_assistant_text(grounded_status or guard_text(assistant_text, audit))
        messages.append({"role": "assistant", "content": assistant_text})

    store.update_session(sid, messages=messages)
    if pending:
        store.set_pending_actions(sid, pending)
    if assistant_text:
        telemetry.record_session_turn(
            user_id, sid, role="assistant", content_preview=assistant_text,
            model=model, content_format=content_format,
        )
        training_capture.capture_event(
            str(user_id),
            "assistant_turn",
            {
                "text": assistant_text,
                "tool_fires": [
                    {"name": fire.name, "args": fire.args, "result": fire.result}
                    for fire in tool_fires
                ],
                "pending_actions": pending,
                "active_jobs": active_jobs,
            },
            session_id=sid,
            turn_id=turn_id,
        )

    if active_jobs:
        store.update_session(sid, active_jobs=active_jobs)

    # Meter text-model token spend against the unified credit wallet.
    credits_charged = 0
    usd_cost = 0.0
    prompt_ppm, completion_ppm, debit_reason, billed_model = _llm_pricing_for_provider(
        model_provider,
        model,
        effective_model,
    )
    try:
        if model_provider != "anthropic_fallback":
            prompt_ppm, completion_ppm = await openrouter.model_pricing(model)
            billed_model = model
    except Exception:
        prompt_ppm = completion_ppm = None
    try:
        import unified_credits as uc

        usage_for_cost = {
            "prompt_tokens": acc_prompt_tokens,
            "completion_tokens": acc_completion_tokens,
        }
        usd_cost = uc.openrouter_usd(usage_for_cost, prompt_ppm, completion_ppm)
        if usd_cost > 0 and user_id and not owner_unmetered:
            credits_charged, _bal = uc.debit_usd(
                user_id,
                usd_cost,
                reason=debit_reason,
                metadata={
                    "provider": model_provider,
                    "model": billed_model,
                    "session_id": sid,
                    "prompt_tokens": acc_prompt_tokens,
                    "completion_tokens": acc_completion_tokens,
                    "prompt_price_per_m": prompt_ppm,
                    "completion_price_per_m": completion_ppm,
                },
                allow_negative=False,
            )
        elif usd_cost > 0 and user_id and owner_unmetered:
            credits_charged = 0
    except Exception:
        pass

    result = {
        "session_id": sid,
        "assistant_message": assistant_text,
        "pending_actions": pending,
        "active_jobs": active_jobs,
        "approval_mode": approval_mode,
        "reasoning_depth": reasoning_depth,
        "usage": usage_total,
        "billing": {
            "credits_charged": credits_charged,
            "provider_usd": round(float(usd_cost or 0.0), 6),
            "provider": model_provider,
            "model": billed_model,
            "prompt_tokens": acc_prompt_tokens,
            "completion_tokens": acc_completion_tokens,
        },
    }
    await _fire_event(emit, "active_jobs", jobs=active_jobs)
    return result


async def approve_action(
    session: dict[str, Any],
    action_id: str,
    *,
    membership_plan: str = "",
    billing_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    user_id = str(session.get("user_id") or "")
    profile = billing_profile or {}
    async with studio_agent_slot(
        user_id=user_id,
        plan=membership_plan,
        operation="approve",
        unlimited=bool(profile.get("unlimited")),
    ) as admission:
        result = await _approve_action_impl(session, action_id, billing_profile=billing_profile)
        if admission.mode != "disabled":
            result["queue"] = admission.as_dict()
        return result


async def _approve_action_impl(
    session: dict[str, Any],
    action_id: str,
    *,
    billing_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sid = session["session_id"]
    fresh = store.get_session(sid) or session
    action = store.pop_pending_action(sid, action_id)
    if not action:
        action = store.recover_pending_action_from_messages(fresh, action_id)
    if not action:
        store.sync_pending_from_messages(sid)
        still = store.get_session(sid) or fresh
        for row in still.get("pending_actions") or []:
            if row.get("id") == action_id:
                action = store.pop_pending_action(sid, action_id)
                break
    if not action:
        action = store.recover_pending_action_from_messages(
            store.get_session(sid) or fresh,
            action_id,
        )
    if not action:
        raise KeyError(f"pending action not found: {action_id}")

    name = action["tool"]
    args = action.get("arguments") or {}
    if name == "start_shortform_generate":
        args = _inject_shortform_render_style(args, session)
        args = _inject_shortform_caption_options(args, session)
        args = _normalize_shortform_category_args(args)

    # Defense for re-edit threads: if this pending action is a start tool but the conversation
    # has an active reply-to re-edit context in recent messages, redirect to surgical re-edit
    # instead of starting a brand new generation (user may have had a pending start from before the reply).
    recent = "\n".join(str(m.get("content", "")) for m in (store.get_session(sid) or session).get("messages", [])[-3:])
    if name in BRAND_NEW_PRODUCTION_TOOLS and "[User is replying to their previous" in recent:
        job_id = ""  # best effort; the re_edit will use what it can or the LLM will have specified
        # Try to extract from the note if present in the action summary or messages
        for m in reversed((store.get_session(sid) or session).get("messages", [])[-5:]):
            if "[User is replying to their previous" in str(m.get("content", "")):
                # crude extract
                import re
                m_job = re.search(r"job_id=([a-z0-9]+)", str(m.get("content", "")))
                if m_job:
                    job_id = m_job.group(1)
                    break
        kind = "shortform"
        reedit_res = re_edit_production(job_id or "unknown", f"[From approved pending during re-edit thread] {args}", kind)
        return {"session_id": sid, "assistant_message": "Redirected pending start to surgical re-edit for the replied-to video.", "result": reedit_res}

    tool_error = ""
    try:
        result = execute_tool_logged(
            name,
            args,
            user_id=session["user_id"],
            content_format=session.get("content_format") or "both",
            session_id=sid,
        )
    except Exception as exc:
        tool_error = str(exc)
        result = json.dumps({"error": tool_error})
    try:
        memory.observe_tool_result(str(session.get("user_id") or ""), name, args, result)
    except Exception:
        pass

    started = extract_jobs_from_tool(name, result)
    fresh = store.get_session(sid) or session
    messages: list[dict[str, Any]] = list(fresh.get("messages") or [])
    messages.append({
        "role": "user",
        "content": (
            f"[User approved {name}]\nTool result:\n{result[:12000]}\n"
            "Summarize what happened and propose the next production step."
        ),
    })
    store.update_session(sid, messages=messages)

    if tool_error:
        hint = f"Approved {name} failed: {tool_error}"
        return {
            "session_id": sid,
            "assistant_message": hint,
            "pending_actions": [],
            "active_jobs": started,
            "approved_action": {
                "id": action_id,
                "tool": name,
                "error": tool_error,
                "result_preview": result[:2000],
            },
        }

    # Job-start tools: return immediately so approve does not block on a second LLM turn
    # (RunPod /runsync budget is ~90s; shortform spawn should surface in the render dock).
    if name in JOB_START_TOOLS:
        try:
            parsed = json.loads(result or "{}")
            preview = parsed.get("error") or parsed.get("note") or parsed.get("status")
        except json.JSONDecodeError:
            parsed = {}
            preview = result[:400]
        is_complete = _production_result_complete(parsed)
        if is_complete:
            assistant_note = "Your video is complete. You can review or download it from the production card."
        elif name == "start_longform_render":
            assistant_note = "I started the long-form production. I’ll show the chapter scenes here as they become ready for review."
        elif name == "start_shortform_generate":
            assistant_note = "I started building the video. I’ll bring the scenes into this chat for review before animation."
        else:
            assistant_note = "The production is running. I’ll keep the progress visible here."
        messages.append({"role": "assistant", "content": assistant_note})
        store.update_session(
            sid,
            messages=messages,
            active_jobs=started,
            last_production={"tool": name, "arguments": args, "updated_at": time.time()},
        )
        return {
            "session_id": sid,
            "assistant_message": assistant_note,
            "pending_actions": [],
            "active_jobs": started,
            "approved_action": {"id": action_id, "tool": name, "result_preview": result[:2000]},
        }

    if started:
        store.update_session(sid, active_jobs=started)

    refreshed = store.get_session(sid) or session
    follow_up = await _run_turn_impl(
        refreshed,
        "Continue production from the approved action result.",
        billing_profile=billing_profile,
    )
    follow_up["active_jobs"] = merge_active_jobs(
        started,
        follow_up.get("active_jobs") or [],
    )
    follow_up["approved_action"] = {"id": action_id, "tool": name, "result_preview": result[:2000]}
    if started and not str(follow_up.get("assistant_message") or "").strip():
        follow_up["assistant_message"] = (
            f"Started {name} — track progress in the render dock and chat."
        )
    return follow_up


async def retry_last_production(
    session: dict[str, Any],
    *,
    membership_plan: str = "",
    billing_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Re-run the last approved/auto production tool (shortform/longform spawn)."""
    sid = session["session_id"]
    fresh = store.get_session(sid) or session
    lp = store.recover_last_production(fresh) or {}
    name = str(lp.get("tool") or "").strip()
    args = lp.get("arguments") or {}
    if not name or name not in JOB_START_TOOLS:
        raise KeyError("no production to retry — approve or run start_shortform_generate first")
    if name == "start_shortform_generate":
        args = _inject_shortform_render_style(args, fresh)
        args = _inject_shortform_caption_options(args, fresh)
        args = _normalize_shortform_category_args(args)
        # Resume the last shortform job's workspace so finished stills/clips/VO
        # are reused instead of re-rendered (and re-billed) from scratch.
        prev = [
            j for j in (fresh.get("active_jobs") or [])
            if j.get("kind") == "shortform" and j.get("job_id")
        ]
        if prev:
            args = {**args, "_resume_job_id": str(prev[-1]["job_id"])}
    if lp.get("recovered"):
        store.update_session(sid, last_production=lp)

    user_id = str(session.get("user_id") or "")
    profile = billing_profile or {}
    async with studio_agent_slot(
        user_id=user_id,
        plan=membership_plan,
        operation="retry_production",
        unlimited=bool(profile.get("unlimited")),
    ):
        try:
            result = execute_tool_logged(
                name,
                args,
                user_id=session["user_id"],
                content_format=session.get("content_format") or "both",
                session_id=sid,
            )
        except Exception as exc:
            raise RuntimeError(str(exc)) from exc

    started = extract_jobs_from_tool(name, result)
    messages = list((store.get_session(sid) or fresh).get("messages") or [])
    messages.append({
        "role": "user",
        "content": f"[User retried {name}]\nTool result:\n{result[:8000]}",
    })
    messages.append({
        "role": "assistant",
        "content": f"Retrying {name} — track the new job in the render dock.",
    })
    store.update_session(
        sid,
        messages=messages,
        active_jobs=merge_active_jobs(list(fresh.get("active_jobs") or []), started),
        last_production={"tool": name, "arguments": args, "updated_at": time.time()},
    )
    return {
        "session_id": sid,
        "assistant_message": f"Retrying {name} — production is running.",
        "active_jobs": started,
        "retried_tool": name,
    }
