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

from studio_agent import openrouter, production_budget, provider_policy, skills
from studio_agent.anti_hallucination import ToolFire, audit_turn, guard_text
from studio_agent import memory, store
from studio_agent import telemetry, training_capture
from studio_agent.execution_context import production_command_scope
from studio_agent.tone import (
    CONTENT_TYPE_ROUTING_BLOCK,
    PRODUCT_AD_ROUTING_BLOCK,
    PROFESSIONAL_VOICE_BLOCK,
    sanitize_assistant_text,
)
from studio_agent.studio_identity import STUDIO_IDENTITY_PROMPT
from studio_agent.tools import (
    _normalize_shortform_category_args,
    execute_tool_logged,
    re_edit_production,
    requires_approval,
    tool_schemas,
    tools_for_user,
)
from studio_agent.queue import (
    StudioAgentQueueFullError,
    StudioAgentQueueTimeoutError,
    studio_agent_slot,
)
from studio_agent.jobs import (
    JOB_START_TOOLS,
    extract_jobs_from_tool,
    get_job_snapshot,
    merge_active_jobs,
)

MAX_TOOL_ROUNDS = 12

BRAND_NEW_PRODUCTION_TOOLS = frozenset({
    "start_longform_render",
    "start_shortform_generate",
    "analyze_reference_video",
})

PLAN_MODE_EXECUTION_ALLOWLIST = frozenset({
    # Packaging proof is deliberately independent of full-video production.
    "generate_longform_thumbnails",
})


def _plan_mode_blocks_tool(tool_name: str) -> bool:
    name = str(tool_name or "").strip()
    if name in PLAN_MODE_EXECUTION_ALLOWLIST:
        return False
    return bool(
        requires_approval(name)
        or name in JOB_START_TOOLS
        or re.search(r"(?:generate|render|animate|finalize|regenerate|edit_production|write_project|expand_.*proof)", name)
    )


def _offered_model_tool_names(tools: list[dict[str, Any]]) -> frozenset[str]:
    """Return the exact function names included in the current model request."""
    names: set[str] = set()
    for tool in tools or []:
        if not isinstance(tool, dict):
            continue
        function = tool.get("function")
        if not isinstance(function, dict):
            continue
        name = str(function.get("name") or "").strip()
        if name:
            names.add(name)
    return frozenset(names)


def _unoffered_model_tool_result(
    tool_name: str,
    offered_tool_names: frozenset[str] | set[str],
) -> str | None:
    """Fail closed when a provider invents or replays a tool it was not offered."""
    name = str(tool_name or "").strip()
    if name and name in offered_tool_names:
        return None
    return json.dumps(
        {
            "status": "blocked_unoffered_tool",
            "error": "unoffered_tool",
            "tool": name,
            "message": (
                "The model returned a tool that was not offered in this request. "
                "No tool was executed."
            ),
        }
    )


def _execute_offered_model_tool(
    tool_name: str,
    arguments: dict[str, Any],
    *,
    offered_tool_names: frozenset[str] | set[str],
    user_id: Any,
    content_format: str,
    session_id: str,
    executor: Callable[..., str] | None = None,
) -> str:
    """Execute only tools present in the exact schema set sent to the model."""
    rejected = _unoffered_model_tool_result(tool_name, offered_tool_names)
    if rejected is not None:
        return rejected
    return (executor or execute_tool_logged)(
        str(tool_name or "").strip(),
        dict(arguments or {}),
        user_id=user_id,
        content_format=content_format,
        session_id=session_id,
    )


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


SONNET_5_INTRO_PRICE_END_UTC = 1_788_220_800.0  # 2026-09-01T00:00:00Z


def _sonnet_5_intro_pricing_active(*, now: float | None = None) -> bool:
    """Use Anthropic's launch price through August 31, 2026 (inclusive)."""

    current = time.time() if now is None else float(now)
    return current < SONNET_5_INTRO_PRICE_END_UTC


def _llm_pricing_for_provider(provider: str, model: str, fallback_model: str) -> tuple[float | None, float | None, str, str]:
    """Return prompt/completion dollars per million tokens and debit reason."""
    if provider_policy.normalize_provider(provider) == "anthropic":
        billed_model = provider_policy.normalize_anthropic_model_id(fallback_model or model)
        metadata = openrouter.CURATED_META.get(billed_model) or {}
        default_prompt = float(metadata.get("prompt_price_per_m") or 3.0)
        default_completion = float(metadata.get("completion_price_per_m") or 15.0)
        if provider_policy.is_sonnet_5(billed_model) and _sonnet_5_intro_pricing_active():
            default_prompt, default_completion = 2.0, 10.0
        prompt_ppm = _env_float(
            "ANTHROPIC_PROMPT_USD_PER_M",
            default_prompt,
        )
        completion_ppm = _env_float(
            "ANTHROPIC_COMPLETION_USD_PER_M",
            default_completion,
        )
        return prompt_ppm, completion_ppm, "studio_agent_anthropic_direct", billed_model
    return None, None, "studio_agent_openrouter", model


def _shortform_workspace_is_finished(ws_path: Any) -> bool:
    """True when a shortform workspace already finished stills/export (do not re-open as new)."""
    try:
        from pathlib import Path

        ws = Path(ws_path)
        result_path = ws / "result.json"
        if result_path.is_file():
            data = json.loads(result_path.read_text(encoding="utf-8"))
            status = str(data.get("status") or "").lower()
            if status in {"complete", "completed", "done", "success", "exported"}:
                return True
        prog_path = ws / "progress.json"
        if prog_path.is_file():
            prog = json.loads(prog_path.read_text(encoding="utf-8"))
            progress = float(prog.get("progress") or 0)
            stage = str(prog.get("stage") or prog.get("stage_label") or "").lower()
            if progress >= 100 and any(
                key in stage for key in ("ready", "complete", "review", "done", "awaiting")
            ):
                return True
            if progress >= 100 and not any(x in stage for x in ("script", "queued", "generat", "start")):
                return True
    except Exception:
        return False
    return False


def _matching_shortform_resume_job_id(
    session: dict[str, Any],
    args: dict[str, Any],
    *,
    allow_finished: bool = False,
) -> str | None:
    """Find the newest durable shortform workspace matching this user/topic.

    The chat's in-memory active_jobs list can disappear after refresh, fail, or
    deploy. The media workspace on the Fly volume is the real source of truth,
    so retry/resume should reattach to it instead of starting a new render.

    Finished (Ready 100%) workspaces are skipped by default so "next short" cannot
    re-attach to the previous completed job.
    """
    try:
        from studio_agent.jobs import ROOT as _JOBS_ROOT, SKELETON_OUTPUT as _SKELETON_OUTPUT
        root = (_JOBS_ROOT / _SKELETON_OUTPUT).resolve()
        if not root.is_dir():
            return None
    except Exception:
        return None

    wanted_user = str(session.get("user_id") or "").strip()
    wanted_topic = str(args.get("topic") or args.get("title") or "").strip().lower()
    wanted_category = str(args.get("category_key") or "").strip().lower()
    # Never resume into a finished job when user locked a different title.
    locked = store.get_locked_working_title(session).lower()
    if locked and wanted_topic and store._title_overlap_score(locked, wanted_topic) < 0.75:
        return None
    if store.is_new_production_request(store._latest_user_text(list(session.get("messages") or []), limit=2), session):
        return None

    candidates: list[tuple[float, int, str]] = []
    for spec_path in root.glob("*/job_spec.json"):
        ws = spec_path.parent
        try:
            spec = json.loads(spec_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if wanted_user and str(spec.get("user_id") or "").strip() != wanted_user:
            continue
        spec_topic = str(spec.get("topic") or spec.get("title") or "").strip().lower()
        spec_category = str(spec.get("category_key") or "").strip().lower()
        if wanted_topic and spec_topic != wanted_topic:
            # Soft match via keywords so minor title edits still resume mid-flight jobs.
            if store._title_overlap_score(wanted_topic, spec_topic) < 0.75:
                continue
        if not wanted_topic and wanted_category and spec_category != wanted_category:
            continue
        if not allow_finished and _shortform_workspace_is_finished(ws):
            continue
        still_count = 0
        scenes_path = ws / "scenes.json"
        if scenes_path.is_file():
            try:
                scenes = json.loads(scenes_path.read_text(encoding="utf-8"))
                if isinstance(scenes, list):
                    still_count = len([s for s in scenes if isinstance(s, dict)])
            except Exception:
                still_count = 0
        if still_count <= 0:
            stills_dir = ws / "stills"
            if stills_dir.is_dir():
                still_count = len([p for p in stills_dir.glob("*.png") if p.is_file()])
        if still_count <= 0:
            continue
        newest = max(
            (p.stat().st_mtime for p in [spec_path, ws / "progress.json", ws / "result.json", scenes_path] if p.is_file()),
            default=ws.stat().st_mtime,
        )
        candidates.append((newest, still_count, ws.name))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][2]


def _force_fresh_shortform_args(args: dict[str, Any]) -> dict[str, Any]:
    """Strip resume hooks so start_shortform always creates a new job_id."""
    out = dict(args or {})
    out.pop("_resume_job_id", None)
    out["_force_fresh"] = True
    return out


def _apply_locked_title_to_production_args(
    args: dict[str, Any],
    session: dict[str, Any],
    *,
    user_text: str = "",
) -> dict[str, Any]:
    """Force production args onto the CURRENT resolved title (message > lock > args)."""
    out = dict(args or {})
    resolved = store.resolve_production_title(
        user_text or store._latest_user_text(list(session.get("messages") or []), limit=2),
        session,
        fallback=str(out.get("title") or out.get("topic") or ""),
    )
    if not resolved:
        return out
    out["title"] = resolved[:120]
    out["topic"] = resolved[:120]
    return out


def _force_production_title_on_args(
    args: dict[str, Any],
    *,
    session: dict[str, Any],
    user_text: str,
) -> dict[str, Any]:
    """Absolute title lock for Approve cards — never leave a prior Ready title in place."""
    out = _apply_locked_title_to_production_args(args, session, user_text=user_text)
    resolved = str(out.get("title") or out.get("topic") or "").strip()
    prior = store.prior_production_title(session)
    if resolved and prior and store._title_overlap_score(resolved, prior) < 0.75:
        out = _force_fresh_shortform_args(out)
    elif store.is_new_production_request(user_text, session):
        out = _force_fresh_shortform_args(out)
    return out


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
    provider = "Anthropic"
    if "anthropic_api_key missing" in lower or "anthropic_api_key is not set" in lower:
        return (
            "Studio Agent cannot reach its direct Anthropic runner because ANTHROPIC_API_KEY is not configured on the backend. "
            "Your chat, stills, approvals, and any server-side production job are preserved. "
            "Set ANTHROPIC_API_KEY as a backend secret, redeploy, then press Resume and continue from this point. "
            "This is not an internet drop."
        )
    return (
        f"Studio Agent hit {provider} credit limits before it could finish the text response. "
        "Your chat, stills, approvals, and any server-side production job are preserved. "
        "Add Anthropic API credits, then press Resume and continue from this point. "
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
    "lexi_manhwa": (
        "lexi_manhwa",
        "lexi_manhua",
        "lexi manhwa",
        "lexi manhua",
        "mlexi manhua",
        "m-lexi manhua",
        "leximanhwa",
        "leximanhua",
        "@leximanhwa",
        "@leximanhua",
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


def _only_connected_channel_for_user(user_id: str) -> dict[str, str] | None:
    """Return a safe automatic channel choice only when the user has one.

    OAuth connection and chat selection are separate state records. That used
    to leave a newly connected single-channel creator with Catalyst memory
    disabled until they manually opened the picker. We never guess when there
    are multiple channels, but one connected channel is unambiguous.
    """
    try:
        from long_form.prompts.channels import CHANNELS
        from youtube_connections_store import hydrate

        owner = str(user_id or "").strip()
        connected = ((hydrate() or {}).get(owner) or {}).get("channels") or {}
        rows = [(str(channel_id).strip(), rec) for channel_id, rec in connected.items()
                if str(channel_id).strip() and isinstance(rec, dict)]
        if len(rows) != 1:
            return None
        channel_id, record = rows[0]
        registry_key = next(
            (str(key) for key, value in CHANNELS.items() if str(value.get("channel_id") or "") == channel_id),
            "",
        )
        return {
            "channel_id": channel_id,
            "registry_key": registry_key,
            "channel_title": str(record.get("title") or record.get("channel_handle") or "").strip(),
        }
    except Exception:
        return None


def _needs_catalyst_preflight_for_production(user_text: str, production_intent: str) -> bool:
    """Require Catalyst/channel analytics before spawning new channel-native productions."""
    if store.is_ideation_request(user_text):
        return False
    # Product ads use Live Demand (public niche) + product reference, not private analytics.
    if str(production_intent or "").strip() == "product_ad":
        return False
    low = str(user_text or "").lower()
    if re.search(r"\b(?:make|create|generate|render|produce|build|plan|write|script)\b.+\b(?:short|video|script)\b", low):
        return True
    if re.search(r"\b(?:short|video)\b.+\b(?:for my channel|for the channel|for this channel)\b", low):
        return True
    return False


def _needs_live_demand_preflight(
    user_text: str,
    session: dict[str, Any] | None = None,
    *,
    production_intent: str = "",
) -> bool:
    """True when production/research must pull niche-agnostic Live Demand first."""
    try:
        from studio_agent.live_demand import build_live_demand_plan

        plan = build_live_demand_plan(user_text, session)
        if plan.required:
            return True
    except Exception:
        pass
    if str(production_intent or "").strip() == "product_ad" and store.should_auto_run_tools(user_text):
        return True
    return False


def _needs_channel_data_preflight(user_text: str) -> bool:
    if not store.should_auto_run_tools(user_text):
        return False
    # "Find an exact topic first" is a Catalyst request, not idle ideation.
    # When a channel is attached, its history is primary evidence for the
    # recommendation.
    if store.is_exact_topic_discovery_request(user_text):
        return True
    if store.is_explicit_reference_analysis_request(user_text) and not store.is_connected_channel_performance_request(user_text):
        return False
    if store.is_ideation_request(user_text) and not store.is_connected_channel_performance_request(user_text):
        return False
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
        "title",
        "titles",
        "seo",
        "package",
        "packaging",
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


def _needs_current_video_audit(user_text: str) -> bool:
    """Detect requests that require the full current-video evidence chain.

    This is not a response guard. It routes a concrete workflow:
    latest upload identity + private channel metrics + exact-video analysis +
    fresh public YouTube demand before final synthesis.
    """
    low = str(user_text or "").lower()
    if not _needs_latest_upload_focus(low):
        return False
    audit_terms = (
        "all its data",
        "get all",
        "pull all",
        "look at",
        "analyze",
        "analyse",
        "understand where",
        "where we are",
        "why",
        "what's wrong",
        "whats wrong",
        "what is wrong",
        "fix",
        "improve",
        "retention",
        "avd",
        "watch time",
    )
    return any(term in low for term in audit_terms)


def _needs_fresh_public_search(user_text: str) -> bool:
    low = str(user_text or "").lower()
    return any(
        phrase in low
        for phrase in (
            "current",
            "latest",
            "most recent",
            "newest",
            "right now",
            "today",
            "live",
            "fresh",
            "real time",
            "real-time",
            "realtime",
            "most updated",
            "updated data",
            "more updated data",
            "up to date",
            "up-to-date",
            "actual youtube",
            "public search",
            "what people are searching",
            "what people are actually looking for",
            "what people are actually wanting",
            "what people are wanting",
            "currently trending",
            "trending now",
            "upload velocity",
            "trending data",
        )
    )


def _public_search_use_fresh(user_text: str, *, public_demand: bool = False) -> bool:
    """Bypass YouTube search cache only when the user asks for live/right-now data.

    Quota survival: public_demand alone must NOT force fresh=true. Each fresh Live
    Demand turn used to cost ~1000+ units (dual tools × multi-seed × dual search.list).
    Cache-first is the default; explicit recency language opts into cache bypass.
    """
    del public_demand  # kept for call-site compatibility; no longer forces fresh
    return _needs_fresh_public_search(user_text)


def _public_search_window_days(user_text: str) -> int:
    """Upload window for recent-momentum public search (1-90 days).

    Live Demand / market niches can request 1–2 day windows; legacy default is 30.
    """
    try:
        from studio_agent.live_demand import demand_window_days, has_demand_signal

        if has_demand_signal(user_text):
            return demand_window_days(user_text, default=2)
    except Exception:
        pass
    low = str(user_text or "").lower()
    range_match = re.search(r"\b(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\s*days?\b", low)
    if range_match:
        upper = max(int(range_match.group(1)), int(range_match.group(2)))
        return max(1, min(upper, 90))
    last_match = re.search(r"\blast\s+(\d{1,2})\s*days?\b", low)
    if last_match:
        return max(1, min(int(last_match.group(1)), 90))
    if any(
        phrase in low
        for phrase in (
            "right now",
            "today",
            "trending now",
            "currently trending",
            "real time",
            "real-time",
            "realtime",
            "upload velocity",
            "last 24 hours",
            "last 12 hours",
        )
    ):
        return 2
    return 30


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
        "stats",
        "stat",
        "statistics",
        "channel stats",
        "my stats",
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
    if re.fullmatch(r"stats?", compact):
        return True
    return bool(
        re.search(
            r"\b(?:what|show|tell|give)\b.*\b(?:find|found|findings|results?|analysis|update|status|stats?)\b",
            compact,
            re.IGNORECASE,
        )
    )


def _is_continue_production_request(user_text: str) -> bool:
    compact = str(user_text or "").strip().lower().strip(" ?!.")
    if compact in {
        "continue",
        "resume",
        "keep going",
        "continue it",
        "continue working",
        "continue the short",
        "finish it",
        "finish the short",
        "make the short",
        "keep working on the short",
    }:
        return True
    return bool(
        re.search(
            r"\b(?:continue|resume|finish|keep going)\b.*\b(?:short|video|production|render|job)\b",
            compact,
            re.IGNORECASE,
        )
    )


def _is_production_diagnostic_turn(user_text: str) -> bool:
    """True when the user is debugging/correcting a production, not approving a new one."""
    compact = str(user_text or "").strip().lower()
    if not compact:
        return False
    if _wants_production_execution(compact):
        return False
    diagnostic_terms = (
        "wrong short",
        "wrong video",
        "wrong one",
        "wrong title",
        "previous short",
        "previous video",
        "old short",
        "old video",
        "same video",
        "same short",
        "already made",
        "already been made",
        "why are you",
        "why is it",
        "why do you keep",
        "keep trying to make",
        "keeps trying to make",
        "what is causing",
        "what's causing",
        "causing it",
        "stuck",
        "do i need to start a new chat",
        "need to start a new chat",
        "trying to build",
        "keeps trying",
        "keep getting stuck",
    )
    return any(term in compact for term in diagnostic_terms)


_TITLE_STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "when", "they", "them", "you", "your",
    "into", "from", "short", "video", "scene", "test", "make", "making", "going", "title",
    "lets", "let", "will", "we", "one", "exactly",
}


def _title_keywords(value: str) -> set[str]:
    words = re.findall(r"[a-z0-9]+", str(value or "").lower())
    return {w for w in words if len(w) > 2 and w not in _TITLE_STOPWORDS}


def _latest_user_text(session: dict[str, Any], *, limit: int = 4) -> str:
    fresh = session if session.get("messages") else (store.get_session(str(session.get("session_id") or "")) or session)
    rows = [
        str(m.get("content") or "")
        for m in list(fresh.get("messages") or [])
        if str(m.get("role") or "") == "user"
    ]
    return "\n".join(rows[-limit:])


def _explicit_title_candidate(text: str) -> str:
    value = str(text or "")
    def _clean_candidate(candidate: str) -> str:
        cleaned = str(candidate or "").strip(" -:,.")
        cleaned = re.sub(r"^(?:let'?s\s+see|let\s+us\s+see|maybe|okay|ok)\s*,?\s*", "", cleaned, flags=re.IGNORECASE)
        return cleaned.strip(" -:,.")

    quoted = [
        _clean_candidate(q)
        for q in re.findall(r'"([^"\n]{8,140})"', value)
        if len(_title_keywords(q)) >= 2
    ]
    if quoted:
        return quoted[-1]
    patterns = [
        r"(?:title\s+(?:we'?re\s+going\s+to\s+go\s+with|is|it)\s*[:,]?\s*)([^.\n]{8,140})",
        r"(?:we\s+will\s+do|we'?ll\s+do|let'?s\s+do|lets\s+do)\s+([^.\n]{8,140})",
    ]
    for pattern in patterns:
        matches = [_clean_candidate(m) for m in re.findall(pattern, value, flags=re.IGNORECASE)]
        matches = [m for m in matches if len(_title_keywords(m)) >= 2]
        if matches:
            return matches[-1]
    return ""


def _shortform_action_conflicts_with_latest_user(action_args: dict[str, Any], session: dict[str, Any]) -> str:
    action_title = store._production_action_title({"arguments": action_args})
    messages = list(session.get("messages") or [])
    requested_title = store._canonical_production_topic(messages, session=session)
    if not action_title or not requested_title:
        return ""
    action_words = _title_keywords(action_title)
    requested_words = _title_keywords(requested_title)
    if not action_words or not requested_words:
        return ""
    overlap = len(action_words & requested_words) / max(1, min(len(action_words), len(requested_words)))
    if overlap < 0.34:
        return (
            f"Blocked stale production approval. Pending title/topic was '{action_title}', "
            f"but the latest user request appears to be '{requested_title}'."
        )
    return ""


def _filter_stale_pending_actions(actions: list[dict[str, Any]], session: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    kept: list[dict[str, Any]] = []
    blocked: list[str] = []
    blocked_jobs = {
        str(value).strip()
        for value in (session.get("blocked_job_ids") or [])
        if str(value).strip()
    }
    for action in actions:
        args = action.get("arguments") if isinstance(action.get("arguments"), dict) else {}
        target_job = str(args.get("job_id") or "").strip()
        if target_job and target_job in blocked_jobs:
            blocked.append(f"Blocked stale approval targeting detached job {target_job}.")
            continue
        if str(action.get("tool") or "") != "start_shortform_generate":
            kept.append(action)
            continue
        conflict = _shortform_action_conflicts_with_latest_user(args, session)
        if conflict:
            blocked.append(conflict)
            continue
        kept.append(action)
    return kept, blocked


def _prepare_pending_actions(
    actions: list[dict[str, Any]],
    session: dict[str, Any],
    *,
    messages: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Filter stale production approvals and collapse to one current production slot."""
    turn_session = dict(session)
    session_messages = list(messages if messages is not None else session.get("messages") or [])
    if messages is not None:
        turn_session["messages"] = messages
    aligned_actions: list[dict[str, Any]] = []
    for action in actions:
        if not isinstance(action, dict):
            continue
        row = dict(action)
        if str(row.get("tool") or "") == "start_shortform_generate":
            args = row.get("arguments") if isinstance(row.get("arguments"), dict) else {}
            # Pending Approve cards must use multi-scene display args — execution
            # prepare (scene_count=1) makes desktop UI hide the card as stale.
            row["arguments"] = store._prepare_shortform_pending_args(
                args, session_messages, session=turn_session,
            )
        aligned_actions.append(row)
    kept, _blocked = _filter_stale_pending_actions(aligned_actions, turn_session)
    return store.normalize_pending_actions(kept, session_messages)


def _upsert_production_pending(
    pending: list[dict[str, Any]],
    action: dict[str, Any],
) -> list[dict[str, Any]]:
    tool = str(action.get("tool") or "")
    if tool not in store.SINGLETON_PRODUCTION_APPROVAL_TOOLS:
        pending.append(action)
        return pending
    filtered = [
        row
        for row in pending
        if str(row.get("tool") or "") not in store.SINGLETON_PRODUCTION_APPROVAL_TOOLS
    ]
    filtered.append(action)
    return filtered


def _production_conflict_with_latest_user(
    recovered: tuple[str, dict[str, Any]] | None,
    session: dict[str, Any],
) -> str:
    if not recovered:
        return ""
    name, args = recovered
    if name != "start_shortform_generate":
        return ""
    return _shortform_action_conflicts_with_latest_user(args, session)


def _recover_poll_target(session: dict[str, Any], *, allow_transcript_fallback: bool = False) -> tuple[str, str] | None:
    """Find the active durable production job.

    Transcript fallback is intentionally opt-in. Old visible tool logs can
    contain completed or failed job IDs, and treating those as active work can
    resurrect an already-made short when the user simply says "continue".
    """
    def _infer_kind(job_id: str, candidate: str = "") -> str:
        try:
            from studio_agent.jobs import ROOT as _JOBS_ROOT, SKELETON_OUTPUT as _SKELETON_OUTPUT

            if (_JOBS_ROOT / _SKELETON_OUTPUT / job_id).resolve().is_dir():
                return "shortform"
        except Exception:
            pass
        kind = str(candidate or "").strip().lower()
        if kind in {"shortform", "longform", "competitor", "cliplab"}:
            return kind
        if job_id.startswith(("clipi_", "clipa_", "clipr_", "remix_")):
            return "cliplab"
        try:
            from studio_agent.jobs import _resolve_poll_kind

            return _resolve_poll_kind(job_id, kind or "shortform")
        except Exception:
            pass
        return "shortform"

    fresh = store.get_session(str(session.get("session_id") or "")) or session
    jobs = list(fresh.get("active_jobs") or [])
    for job in reversed(jobs):
        if str(job.get("kind") or "") == "competitor":
            job_id = str(job.get("job_id") or "").strip()
            if job_id:
                return job_id, "competitor"
    for job in reversed(jobs):
        job_id = str(job.get("job_id") or "").strip()
        if job_id:
            return job_id, _infer_kind(job_id, str(job.get("kind") or ""))

    if bool(fresh.get("skip_job_recovery")):
        return None

    ref_payload = _latest_complete_reference_analysis(messages=list(fresh.get("messages") or []))
    ref_job_id = str((ref_payload or {}).get("job_id") or "").strip()
    if ref_job_id:
        return ref_job_id, "competitor"

    recovered_shortform = _recover_shortform_job_from_session(fresh)
    if recovered_shortform:
        return recovered_shortform, "shortform"

    # Always allow recovering an explicitly-kinded running tool job from the
    # latest transcript (competitor/reference analysis). Full generic transcript
    # job-id fishing stays opt-in via allow_transcript_fallback.
    messages = list(fresh.get("messages") or [])[-12:]
    for msg in reversed(messages):
        text = str(msg.get("content") or "")
        if "job_id" not in text:
            continue
        kind = ""
        if re.search(r'"kind"\s*:\s*"competitor"', text, re.IGNORECASE):
            kind = "competitor"
        elif re.search(r'"kind"\s*:\s*"longform"', text, re.IGNORECASE):
            kind = "longform"
        elif re.search(r'"kind"\s*:\s*"cliplab"', text, re.IGNORECASE):
            kind = "cliplab"
        running = bool(
            re.search(r'"status"\s*:\s*"(?:running|starting|queued|in_progress|pending)"', text, re.I)
        )
        terminal = bool(
            re.search(r'"status"\s*:\s*"(?:complete|completed|failed|error|cancelled)"', text, re.I)
        )
        if terminal and not allow_transcript_fallback:
            # Don't resurrect finished jobs from old tool logs.
            continue
        if not kind and not allow_transcript_fallback and not running:
            continue
        if not kind and running:
            kind = "shortform"
        match = re.search(r'"job_id"\s*:\s*"([^"]+)"', text)
        if not match:
            match = re.search(r"\bjob[_ -]?id\s*[=:]\s*([A-Za-z0-9_-]+)", text, re.IGNORECASE)
        if match:
            job_id = match.group(1)
            return job_id, _infer_kind(job_id, kind)
    return None


def _shortform_continue_plan(snapshot: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    status = str(snapshot.get("status") or snapshot.get("phase") or snapshot.get("stage") or "").strip().lower()
    job_id = str(snapshot.get("job_id") or "").strip()
    if not job_id or status in {"running", "restarting", "starting"}:
        return []
    if status in {"awaiting_scene_review", "awaiting_approval"}:
        total = int(snapshot.get("total_scenes") or snapshot.get("scene_count") or 0)
        approved = int(snapshot.get("approved_scene_count") or 0)
        pending_animation = int(snapshot.get("animation_pending_count") or 0)
        complete_animation = int(snapshot.get("animation_complete_count") or 0)
        scenes = snapshot.get("scenes") if isinstance(snapshot.get("scenes"), list) else []
        if total >= 2 and scenes:
            blocked = [
                int(row.get("index")) for row in scenes
                if isinstance(row, dict)
                and (
                    str(row.get("status") or "").lower() == "qa_blocked"
                    or (isinstance(row.get("still_qa"), dict) and row["still_qa"].get("pass") is False)
                )
                and str(row.get("index") or "").isdigit()
            ]
            if blocked:
                return [
                    ("regenerate_production_scene", {
                        "job_id": job_id,
                        "scene_index": index,
                        "reason": "Finish the existing video: repair this blocked still with its stored narration and visual QA requirements.",
                        "animate": True,
                    })
                    for index in blocked
                ] + [("finalize_production", {"job_id": job_id})]
            unanimated = [
                int(row.get("index")) for row in scenes
                if isinstance(row, dict)
                and str(row.get("status") or "").lower() in {"still_ready", "approved"}
                and not str(row.get("clip_rel") or "").strip()
                and str(row.get("index") or "").isdigit()
            ]
            if unanimated:
                return [
                    ("set_production_scenes_animate", {"job_id": job_id, "animate": True, "scene_indices": unanimated}),
                    ("animate_production_scenes", {"job_id": job_id, "scene_indices": unanimated}),
                    ("finalize_production", {"job_id": job_id}),
                ]
        if total and approved < total:
            return [
                ("set_production_scenes_animate", {"job_id": job_id, "animate": False}),
                ("finalize_production", {"job_id": job_id}),
            ]
        if pending_animation > 0:
            return [
                ("animate_production_scenes", {"job_id": job_id}),
                ("finalize_production", {"job_id": job_id}),
            ]
        if complete_animation > 0 or approved >= max(total, 1):
            return [("finalize_production", {"job_id": job_id})]
    if status in {"scenes_approved"}:
        pending_animation = int(snapshot.get("animation_pending_count") or 0)
        if pending_animation > 0:
            return [
                ("animate_production_scenes", {"job_id": job_id}),
                ("finalize_production", {"job_id": job_id}),
            ]
        return [("finalize_production", {"job_id": job_id})]
    return []


def _is_full_short_ready_to_finalize(session: dict[str, Any]) -> bool:
    """True only for a real completed multi-scene short, never a one-scene proof."""
    target = _recover_poll_target(session)
    if not target or target[1] != "shortform":
        return False
    try:
        snapshot = get_job_snapshot(target[0], "shortform")
    except Exception:
        return False
    total = int(snapshot.get("total_scenes") or snapshot.get("scene_count") or 0)
    approved = int(snapshot.get("approved_scene_count") or 0)
    if total < 2 or approved < total:
        return False
    scenes = snapshot.get("scenes") if isinstance(snapshot.get("scenes"), list) else []
    if scenes:
        ready = sum(
            1 for row in scenes
            if isinstance(row, dict)
            and (str(row.get("status") or "").lower() in {"clip_ready", "complete"} or bool(row.get("clip_rel")))
        )
        return ready >= total
    return int(snapshot.get("animation_complete_count") or 0) >= total


def _is_established_multiscene_short(session: dict[str, Any]) -> bool:
    """A multi-scene job whose remaining work must never enter proof intake."""
    target = _recover_poll_target(session)
    if not target or target[1] != "shortform":
        return False
    try:
        snapshot = get_job_snapshot(target[0], "shortform")
    except Exception:
        return False
    return int(snapshot.get("total_scenes") or snapshot.get("scene_count") or 0) >= 2


async def _continue_active_production(
    *,
    session: dict[str, Any],
    user_id: str,
    content_format: str,
    emit: EventEmitter | None,
    membership_plan: str,
    billing_profile: dict[str, Any] | None,
) -> dict[str, Any] | None:
    target = _recover_poll_target(session)
    if not target:
        return None
    job_id, kind = target
    if kind != "shortform":
        return None

    sid = str(session.get("session_id") or "")
    messages = list((store.get_session(sid) or session).get("messages") or [])
    snapshot = get_job_snapshot(job_id, "shortform")
    status = str(snapshot.get("status") or "").lower()
    tool_fires: list[ToolFire] = [ToolFire("poll_render_job", {"job_id": job_id, "kind": "shortform"}, json.dumps(snapshot))]
    active_jobs = [
        j for j in list(session.get("active_jobs") or [])
        if str(j.get("job_id") or "") != job_id
    ] if status in {"complete", "failed", "error", "cancelled"} else merge_active_jobs(
        list(session.get("active_jobs") or []),
        [{
            "job_id": job_id,
            "kind": "shortform",
            "title": str(snapshot.get("title") or "Short-form video"),
            "started_at": time.time(),
        }],
    )
    await _fire_event(emit, "active_jobs", jobs=active_jobs)

    if status == "complete":
        assistant_text = _format_polled_job_status(json.dumps(snapshot))
        messages.append(_tool_observation_message("poll_render_job", json.dumps(snapshot)))
        messages.append({"role": "assistant", "content": assistant_text})
        _catalyst_capture_turn(
            user_id=str(user_id),
            session=session,
            turn_kind="production_job",
            job_snapshot=snapshot,
        )
        store.update_session(sid, messages=messages, active_jobs=active_jobs)
        await _fire_event(emit, "tool_end", tool="poll_render_job", status="ok")
        return {
            "session_id": sid,
            "assistant_message": assistant_text,
            "pending_actions": [],
            "active_jobs": active_jobs,
            "approval_mode": str(session.get("approval_mode") or "confirm"),
            "reasoning_depth": str(session.get("reasoning_depth") or "balanced"),
            "usage": {},
            "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
        }

    if status in {"failed", "error", "cancelled"}:
        detail = str(snapshot.get("error") or snapshot.get("message") or snapshot.get("note") or "").strip()
        assistant_text = (
            f"That production is {status}. I did not restart it automatically, so no new provider spend was triggered."
        )
        if detail:
            assistant_text += f" Last error: {detail[:500]}"
        assistant_text += " Ask me to retry that exact job if you want me to spend again."
        messages.append(_tool_observation_message("poll_render_job", json.dumps(snapshot)))
        messages.append({"role": "assistant", "content": assistant_text})
        _catalyst_capture_turn(
            user_id=str(user_id),
            session=session,
            turn_kind="production_job",
            job_snapshot=snapshot,
        )
        store.update_session(sid, messages=messages, active_jobs=active_jobs)
        await _fire_event(emit, "active_jobs", jobs=active_jobs)
        await _fire_event(emit, "tool_end", tool="poll_render_job", status=status)
        return {
            "session_id": sid,
            "assistant_message": assistant_text,
            "pending_actions": [],
            "active_jobs": active_jobs,
            "approval_mode": str(session.get("approval_mode") or "confirm"),
            "reasoning_depth": str(session.get("reasoning_depth") or "balanced"),
            "usage": {},
            "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
        }

    plan = _shortform_continue_plan(snapshot)
    if not plan:
        assistant_text = _format_polled_job_status(json.dumps(snapshot))
        messages.append(_tool_observation_message("poll_render_job", json.dumps(snapshot)))
        messages.append({"role": "assistant", "content": assistant_text})
        store.update_session(sid, messages=messages, active_jobs=active_jobs)
        await _fire_event(emit, "tool_end", tool="poll_render_job", status="ok")
        return {
            "session_id": sid,
            "assistant_message": assistant_text,
            "pending_actions": [],
            "active_jobs": active_jobs,
            "approval_mode": str(session.get("approval_mode") or "confirm"),
            "reasoning_depth": str(session.get("reasoning_depth") or "balanced"),
            "usage": {},
            "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
        }

    profile = billing_profile or {}
    last_result: dict[str, Any] = {}
    async with studio_agent_slot(
        user_id=user_id,
        plan=membership_plan,
        operation="continue_production",
        unlimited=bool(profile.get("unlimited")),
    ):
        for tool_name, args in plan:
            await _fire_event(emit, "tool_start", tool=tool_name, round=0, awaiting_approval=False, deterministic_continue=True)
            try:
                result = execute_tool_logged(
                    tool_name,
                    args,
                    user_id=user_id,
                    content_format=content_format,
                    session_id=sid,
                )
                tool_fires.append(ToolFire(tool_name, dict(args), result))
                messages.append(_tool_observation_message(tool_name, result))
                try:
                    parsed = json.loads(result or "{}")
                except Exception:
                    parsed = {}
                last_result = parsed if isinstance(parsed, dict) else {}
                if _production_result_failed(last_result):
                    await _fire_event(emit, "tool_end", tool=tool_name, status="error", error=str(last_result.get("error") or "")[:160])
                    break
                await _fire_event(emit, "tool_end", tool=tool_name, status="ok")
                active_jobs = merge_active_jobs(active_jobs, extract_jobs_from_tool(tool_name, result))
                await _fire_event(emit, "active_jobs", jobs=active_jobs)
            except Exception as exc:
                last_result = {"error": str(exc), "status": "failed"}
                await _fire_event(emit, "tool_end", tool=tool_name, status="error", error=str(exc)[:160])
                break

    final_snapshot = get_job_snapshot(job_id, "shortform")
    if str(final_snapshot.get("status") or "").lower() == "complete":
        active_jobs = [
            j for j in active_jobs
            if str(j.get("job_id") or "") != job_id
        ]
    else:
        active_jobs = merge_active_jobs(active_jobs, [{
            "job_id": job_id,
            "kind": "shortform",
            "title": str(final_snapshot.get("title") or "Short-form video"),
            "started_at": time.time(),
        }])
    if _production_result_failed(last_result):
        assistant_text = f"I continued the short, but the next production step failed: {str(last_result.get('error') or last_result)[:500]}"
    elif _production_result_complete(last_result) or final_snapshot.get("status") == "complete":
        assistant_text = (
            "All set — the short finished and the MP4 is on the production card. "
            "Download it, or tell me what to tweak for a second pass."
        )
    else:
        assistant_text = _format_polled_job_status(json.dumps(final_snapshot))
    messages.append({"role": "assistant", "content": assistant_text})
    store.update_session(sid, messages=messages, active_jobs=active_jobs)
    await _fire_event(emit, "active_jobs", jobs=active_jobs)
    try:
        await _fire_event(emit, "job_snapshot", snapshot=final_snapshot)
    except Exception:
        pass
    return {
        "session_id": sid,
        "assistant_message": assistant_text,
        "pending_actions": [],
        "active_jobs": active_jobs,
        "approval_mode": str(session.get("approval_mode") or "confirm"),
        "reasoning_depth": str(session.get("reasoning_depth") or "balanced"),
        "usage": {},
        "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
    }


async def _continue_active_cliplab(
    *,
    session: dict[str, Any],
    user_id: str,
    user_text: str,
    content_format: str,
    emit: EventEmitter | None,
    approval_mode: str,
    reasoning_depth: str,
) -> dict[str, Any] | None:
    target = _recover_poll_target(session)
    if not target:
        return None
    job_id, kind = target
    if kind != "cliplab":
        return None

    sid = str(session.get("session_id") or "")
    fresh = store.get_session(sid) or session
    messages = list(fresh.get("messages") or [])
    snapshot = get_job_snapshot(job_id, "cliplab")
    status = str(snapshot.get("status") or "").lower()
    job_type = str(snapshot.get("job_type") or snapshot.get("stage") or "").lower()
    video_id = str(snapshot.get("video_id") or "").strip()
    active_jobs = list(fresh.get("active_jobs") or [])

    await _fire_event(emit, "tool_start", tool="poll_cliplab_job", round=0, awaiting_approval=False)
    await _fire_event(emit, "tool_end", tool="poll_cliplab_job", status="ok")

    if status not in {"complete", "failed", "error"}:
        assistant_text = _format_polled_job_status(json.dumps(snapshot))
        messages.append(_tool_observation_message("poll_cliplab_job", json.dumps(snapshot)))
        messages.append({"role": "assistant", "content": assistant_text})
        store.update_session(sid, messages=messages, active_jobs=active_jobs)
        return {
            "session_id": sid,
            "assistant_message": assistant_text,
            "pending_actions": list(fresh.get("pending_actions") or []),
            "active_jobs": active_jobs,
            "approval_mode": approval_mode,
            "reasoning_depth": reasoning_depth,
            "usage": {},
            "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
        }
    if status in {"failed", "error"}:
        assistant_text = _format_polled_job_status(json.dumps(snapshot))
        messages.append(_tool_observation_message("poll_cliplab_job", json.dumps(snapshot)))
        messages.append({"role": "assistant", "content": assistant_text})
        active_jobs = [j for j in active_jobs if str(j.get("job_id") or "") != job_id]
        store.update_session(sid, messages=messages, active_jobs=active_jobs)
        return {
            "session_id": sid,
            "assistant_message": assistant_text,
            "pending_actions": [],
            "active_jobs": active_jobs,
            "approval_mode": approval_mode,
            "reasoning_depth": reasoning_depth,
            "usage": {},
            "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
        }

    channel_id = str(fresh.get("channel_id") or "").strip()
    registry_key = _active_registry_key(fresh, user_text)
    if job_type == "cliplab_ingest" or job_id.startswith("clipi_"):
        prompt = (
            str(user_text or "").strip()
            or "Find the strongest hooks, highest tension moments, character reveals, emotional peaks, pacing breaks, and dialogue hooks for 9:16 YouTube Shorts."
        )
        args = {
            "video_id": video_id,
            "prompt": prompt,
            "max_segments": 12,
            "channel_id": channel_id,
            "registry_key": registry_key,
        }
        await _fire_event(emit, "tool_start", tool="analyze_cliplab_video", round=0, awaiting_approval=False, deterministic_continue=True)
        try:
            result = execute_tool_logged(
                "analyze_cliplab_video",
                args,
                user_id=user_id,
                content_format=content_format,
                session_id=sid,
            )
            await _fire_event(emit, "tool_end", tool="analyze_cliplab_video", status="ok")
        except Exception as exc:
            result = json.dumps({"error": str(exc), "status": "failed"})
            await _fire_event(emit, "tool_end", tool="analyze_cliplab_video", status="error", error=str(exc)[:160])
        messages.append(_tool_observation_message("analyze_cliplab_video", result))
        started = extract_jobs_from_tool("analyze_cliplab_video", result)
        active_jobs = merge_active_jobs([j for j in active_jobs if str(j.get("job_id") or "") != job_id], started)
        assistant_text = "ClipLab ingest is done, so I started analyzing the uploaded video for the strongest 9:16 clip candidates."
        messages.append({"role": "assistant", "content": assistant_text})
        store.update_session(sid, messages=messages, active_jobs=active_jobs)
        await _fire_event(emit, "active_jobs", jobs=active_jobs)
        return {
            "session_id": sid,
            "assistant_message": assistant_text,
            "pending_actions": [],
            "active_jobs": active_jobs,
            "approval_mode": approval_mode,
            "reasoning_depth": reasoning_depth,
            "usage": {},
            "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
        }

    if job_type == "cliplab_analyze" or job_id.startswith("clipa_"):
        segments = list(snapshot.get("segments") or [])
        ranked = sorted(
            enumerate(segments),
            key=lambda row: (
                -float((row[1] or {}).get("virality_score") or 0),
                -float((row[1] or {}).get("confidence") or 0),
                float((row[1] or {}).get("start") or 0),
            ),
        )
        indices = [idx for idx, _seg in ranked[: min(5, len(ranked))]]
        if not indices:
            assistant_text = "ClipLab analysis finished, but it returned no usable segments to render. Re-run analysis with a sharper prompt."
            messages.append(_tool_observation_message("poll_cliplab_job", json.dumps(snapshot)))
            messages.append({"role": "assistant", "content": assistant_text})
            store.update_session(sid, messages=messages, active_jobs=[j for j in active_jobs if str(j.get("job_id") or "") != job_id])
            return {
                "session_id": sid,
                "assistant_message": assistant_text,
                "pending_actions": [],
                "active_jobs": [],
                "approval_mode": approval_mode,
                "reasoning_depth": reasoning_depth,
                "usage": {},
                "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
            }
        action_id = f"act_{uuid.uuid4().hex[:12]}"
        action = {
            "id": action_id,
            "tool": "render_cliplab_segments",
            "arguments": {
                "video_id": video_id,
                "analyze_job_id": job_id,
                "segment_indices": indices,
                "burn_captions": True,
                "channel_id": channel_id,
                "registry_key": registry_key,
            },
            "summary": f"render_cliplab_segments(video_id={video_id}, segment_indices={indices})",
        }
        assistant_text = (
            f"ClipLab analysis found {len(segments)} candidate segments. "
            f"I selected the top {len(indices)} for rendering: {indices}. Approve the render action to cut the 9:16 clips and build upload packages."
        )
        messages.append(_tool_observation_message("poll_cliplab_job", json.dumps(snapshot)))
        messages.append({"role": "assistant", "content": assistant_text})
        active_jobs = [j for j in active_jobs if str(j.get("job_id") or "") != job_id]
        store.update_session(sid, messages=messages, pending_actions=[action], active_jobs=active_jobs)
        await _fire_event(emit, "pending_actions", actions=[action])
        await _fire_event(emit, "active_jobs", jobs=active_jobs)
        return {
            "session_id": sid,
            "assistant_message": assistant_text,
            "pending_actions": [action],
            "active_jobs": active_jobs,
            "approval_mode": approval_mode,
            "reasoning_depth": reasoning_depth,
            "usage": {},
            "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
        }

    assistant_text = _format_polled_job_status(json.dumps(snapshot))
    messages.append(_tool_observation_message("poll_cliplab_job", json.dumps(snapshot)))
    messages.append({"role": "assistant", "content": assistant_text})
    active_jobs = [j for j in active_jobs if str(j.get("job_id") or "") != job_id]
    store.update_session(sid, messages=messages, active_jobs=active_jobs)
    return {
        "session_id": sid,
        "assistant_message": assistant_text,
        "pending_actions": [],
        "active_jobs": active_jobs,
        "approval_mode": approval_mode,
        "reasoning_depth": reasoning_depth,
        "usage": {},
        "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
    }


def _format_polled_job_status(result: str) -> str:
    from studio_agent.conversation import conversational_production_status

    try:
        data = json.loads(result or "{}")
    except Exception:
        data = {}
    if not isinstance(data, dict):
        return (
            "I couldn't read the production status cleanly. "
            "Hit **Resume** once and I'll reconnect to the render."
        )
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
        return conversational_production_status(
            status="failed",
            kind=kind or "shortform",
            error=str(data.get("error") or "unknown error"),
        )
    if kind == "competitor":
        stage = str(data.get("stage") or status or "running").replace("_", " ")
        percent = data.get("percent", data.get("progress"))
        if status == "complete":
            findings = _format_reference_analysis_findings(data)
            return (
                "I finished watching the reference. Here's what matters for your next video:\n\n"
                + findings
            )
        if status == "failed":
            return (
                f"Reference analysis failed during {stage}: "
                f"{data.get('error') or 'unknown error'}. Want me to retry?"
            )
        return conversational_production_status(
            status="running",
            kind="competitor",
            percent=percent,
            job_note=f"Still analyzing the reference ({stage}).",
        )
    if kind == "cliplab":
        job_type = str(data.get("job_type") or data.get("stage") or "").strip().lower()
        percent = data.get("percent", data.get("progress"))
        if status in {"failed", "error"}:
            return (
                f"ClipLab hit an error during {job_type.replace('_', ' ') or 'processing'}: "
                f"{data.get('error') or 'unknown error'}."
            )
        if status != "complete":
            return conversational_production_status(
                status="running",
                kind="cliplab",
                percent=percent,
                job_note=f"ClipLab is still running: {(job_type or status).replace('_', ' ')}.",
            )
        if job_type == "cliplab_ingest" or str(job_id).startswith("clipi_"):
            cues = data.get("cue_count")
            cue_text = f" ({cues} transcript cues)" if cues is not None else ""
            return (
                f"ClipLab ingest is done{cue_text}. "
                "Say the word and I'll analyze for the strongest clip moments."
            )
        if job_type == "cliplab_analyze" or str(job_id).startswith("clipa_"):
            count = int(data.get("segment_count") or len(data.get("segments") or []) or 0)
            return (
                f"I found {count} candidate segment{'s' if count != 1 else ''}. "
                "Approve the strongest indices and I'll render the clips."
            )
        if job_type == "cliplab_render" or str(job_id).startswith("clipr_"):
            clips = int(data.get("clip_count") or len(data.get("clips") or []) or 0)
            return (
                f"ClipLab render is done — {clips} clip{'s' if clips != 1 else ''} ready "
                "with upload packages on the card."
            )
        return "ClipLab step finished. Tell me if you want the next step (analyze or render)."
    if status in {"awaiting_scene_review", "awaiting_approval"}:
        approved = int(data.get("approved_scene_count") or 0)
        total = int(data.get("total_scenes") or data.get("scene_count") or 0)
        return conversational_production_status(
            status=status,
            kind=kind or "shortform",
            scene_count=total,
            approved_count=approved,
            percent=data.get("percent", data.get("progress")),
        )
    return conversational_production_status(
        status=status,
        kind=kind or "shortform",
        percent=data.get("percent", data.get("progress")),
        error=str(data.get("error") or ""),
    )


def _reference_analysis_depth(data: dict[str, Any]) -> str:
    depth = str(data.get("analysis_depth") or "").strip().lower()
    if depth:
        return depth
    gaps = data.get("analysis_gaps") if isinstance(data.get("analysis_gaps"), dict) else {}
    depth = str(gaps.get("depth") or "").strip().lower()
    if depth:
        return depth
    visual_raw = data.get("visual_summary")
    if isinstance(visual_raw, dict):
        visual_text = str(visual_raw.get("summary") or "").strip()
    else:
        visual_text = str(visual_raw or "").strip()
    transcript = data.get("transcript") if isinstance(data.get("transcript"), dict) else {}
    transcript_text = str(transcript.get("text") or data.get("transcript_excerpt") or "").strip()
    storytelling = data.get("storytelling") if isinstance(data.get("storytelling"), dict) else {}
    has_story = bool(
        str(storytelling.get("summary") or data.get("storytelling_summary") or "").strip()
        or str(storytelling.get("hook") or data.get("hook_summary") or "").strip()
    )
    if visual_text and (transcript_text or has_story):
        return "full"
    if visual_text or transcript_text or has_story:
        return "partial"
    return "pacing_only"


def _reference_stage_errors(data: dict[str, Any]) -> dict[str, str]:
    stage_errors = data.get("stage_errors") if isinstance(data.get("stage_errors"), dict) else {}
    if stage_errors:
        return {str(k): str(v) for k, v in stage_errors.items() if str(v).strip()}
    errors: dict[str, str] = {}
    for key, stage in (
        ("visual_error", "vision"),
        ("transcript_error", "transcript"),
        ("storytelling_error", "storytelling"),
    ):
        err = str(data.get(key) or "").strip()
        if err:
            errors[stage] = err
    gaps = data.get("analysis_gaps") if isinstance(data.get("analysis_gaps"), dict) else {}
    nested = gaps.get("stage_errors") if isinstance(gaps.get("stage_errors"), dict) else {}
    for stage, err in nested.items():
        text = str(err or "").strip()
        if text and stage not in errors:
            errors[str(stage)] = text
    visual_raw = data.get("visual_summary")
    if isinstance(visual_raw, dict) and not str(visual_raw.get("summary") or "").strip():
        err = str(visual_raw.get("error") or "").strip()
        if err:
            errors.setdefault("vision", err)
    transcript = data.get("transcript") if isinstance(data.get("transcript"), dict) else {}
    if not str(transcript.get("text") or data.get("transcript_excerpt") or "").strip():
        err = str(transcript.get("error") or "").strip()
        if err:
            errors.setdefault("transcript", err)
    storytelling = data.get("storytelling") if isinstance(data.get("storytelling"), dict) else {}
    if not str(storytelling.get("summary") or data.get("storytelling_summary") or storytelling.get("hook") or "").strip():
        err = str(storytelling.get("error") or "").strip()
        if err:
            errors.setdefault("storytelling", err)
    if not errors and _reference_analysis_depth(data) == "pacing_only":
        return {
            "vision": "no visual summary returned (vision provider may be unavailable)",
            "transcript": "no transcript returned (audio transcription may have failed)",
            "storytelling": "no hook/story readout returned (depends on vision + transcript)",
        }
    return errors


def _format_packaging_readout(packaging: Any) -> str:
    if isinstance(packaging, dict):
        parts: list[str] = []
        title_angle = str(packaging.get("title_angle") or packaging.get("title") or "").strip()
        thumbnail = str(packaging.get("thumbnail_concept") or packaging.get("thumbnail") or "").strip()
        appeal = str(packaging.get("appeal") or "").strip()
        if title_angle:
            parts.append(f"title angle — {title_angle}")
        if thumbnail:
            parts.append(f"thumbnail — {thumbnail}")
        if appeal:
            parts.append(f"appeal — {appeal}")
        return "; ".join(parts)
    return str(packaging or "").strip()


def _format_reference_analysis_findings(data: dict[str, Any]) -> str:
    pacing = data.get("pacing") if isinstance(data.get("pacing"), dict) else {}
    profile = data.get("analysis_profile") if isinstance(data.get("analysis_profile"), dict) else {}
    meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    engagement = data.get("engagement") if isinstance(data.get("engagement"), dict) else {}
    frames = data.get("frames") if isinstance(data.get("frames"), dict) else {}
    analysis_depth = _reference_analysis_depth(data)
    stage_errors = _reference_stage_errors(data)

    title = str(meta.get("title") or data.get("title") or "the reference video").strip()
    format_label = str(profile.get("label") or profile.get("content_format") or "").strip()
    avg_shot = _safe_float(pacing.get("avg_shot_sec"), 0.0)
    cut_count = _safe_int(pacing.get("cut_count"), 0)
    duration = _safe_float(pacing.get("duration_sec") or meta.get("duration"), 0.0)
    hook_window = _safe_float(pacing.get("hook_window_sec"), 0.0)
    frame_count = _safe_int(frames.get("count") or data.get("frames_extracted"), 0)
    like_rate = _safe_float(engagement.get("like_rate_pct"), 0.0)
    comment_rate = _safe_float(engagement.get("comment_rate_pct"), 0.0)

    facts: list[str] = []
    if duration:
        facts.append(f"duration about {duration:.1f}s")
    if avg_shot:
        facts.append(f"average shot length {avg_shot:.2f}s")
    if cut_count:
        facts.append(f"{cut_count} detected cuts")
    if frame_count:
        facts.append(f"{frame_count} keyframes extracted")
    if like_rate:
        facts.append(f"{like_rate:.2f}% like rate")
    if comment_rate:
        facts.append(f"{comment_rate:.2f}% comment rate")

    if avg_shot and avg_shot <= 3.0:
        pacing_conclusion = "fast-cut retention pattern; match the energy with frequent visual interrupts"
    elif avg_shot and avg_shot <= 7.0:
        pacing_conclusion = "balanced short-form pacing; keep the hook tight and escalate every few seconds"
    elif avg_shot:
        pacing_conclusion = "slow-hold pacing; use it only if the visual promise is strong enough to prevent swipes"
    else:
        pacing_conclusion = "pacing extracted, but the cut rhythm was not strong enough to summarize numerically"

    metric_focus = "completion/APV, first-1-to-3-second hold, rewatches, and swipe-away points"
    if str(profile.get("content_format") or "").lower().startswith("long"):
        metric_focus = "first-30-second retention, chapter retention, AVD, APV, and watch-time per chapter"

    if analysis_depth == "pacing_only":
        header = (
            f"Reference analysis only reached pacing metrics{f' for {format_label}' if format_label else ''}."
        )
    elif analysis_depth == "partial":
        header = (
            f"Reference analysis partially complete{f' for {format_label}' if format_label else ''}."
        )
    else:
        header = f"Reference analysis complete{f' for {format_label}' if format_label else ''}."
    lines = [
        header,
        "",
        f"What I found from {title}:",
    ]
    if facts:
        lines.extend(f"- {fact}" for fact in facts)
    else:
        lines.append("- The analysis finished, but the returned payload did not include enough numeric pacing fields.")
    if hook_window:
        lines.append(f"- Hook window target: first {hook_window:.1f}s")

    visual_raw = data.get("visual_summary")
    if isinstance(visual_raw, dict):
        visual_summary = str(visual_raw.get("summary") or "").strip()
    else:
        visual_summary = str(visual_raw or "").strip()
    pacing_warnings = (
        [str(item).strip() for item in data.get("pacing_warnings") or [] if str(item).strip()]
        if isinstance(data.get("pacing_warnings"), list)
        else []
    )

    if visual_summary:
        lines.extend([
            "",
            "Visual look (from keyframe vision, not session Art Style picker):",
            visual_summary,
        ])
    if pacing_warnings:
        lines.extend(["", "Pacing quality notes:"])
        lines.extend(f"- {note}" for note in pacing_warnings[:4])

    storytelling = data.get("storytelling") if isinstance(data.get("storytelling"), dict) else {}
    story_summary = str(storytelling.get("summary") or data.get("storytelling_summary") or "").strip()
    hook = str(storytelling.get("hook") or data.get("hook_summary") or "").strip()
    packaging_raw = storytelling.get("packaging") if storytelling.get("packaging") is not None else data.get("packaging_notes")
    packaging = _format_packaging_readout(packaging_raw)
    beats = storytelling.get("story_beats") if isinstance(storytelling.get("story_beats"), list) else []
    if not beats and isinstance(data.get("story_beats"), list):
        beats = list(data.get("story_beats") or [])
    transcript = data.get("transcript") if isinstance(data.get("transcript"), dict) else {}
    transcript_text = str(transcript.get("text") or data.get("transcript_excerpt") or "").strip()
    if story_summary or hook or packaging or beats:
        lines.extend(["", "Storytelling & packaging readout:"])
        if hook:
            lines.append(f"- Hook: {hook}")
        if story_summary:
            lines.append(f"- Summary: {story_summary}")
        if packaging:
            lines.append(f"- Packaging angle: {packaging}")
        for beat in beats[:6]:
            lines.append(f"- Beat: {str(beat).strip()}")
    if transcript_text:
        lines.extend(["", "Transcript excerpt:", transcript_text[:800]])

    if stage_errors:
        lines.extend(["", "Stages that did not return usable content:"])
        for stage, err in stage_errors.items():
            lines.append(f"- {stage}: {err}")

    if analysis_depth == "pacing_only":
        lines.extend([
            "",
            "Conclusion: ffmpeg pacing alone is not enough to understand this reference's topic, hook, or visual style. "
            "The vision/transcript/story stages above must succeed before planning from this upload.",
            "",
            "Next move: retry with `try again and watch the video` so Studio re-runs deep analysis on the same upload. "
            "If stage errors repeat, escalate with the exact stage error lines above.",
        ])
    else:
        lines.extend([
            "",
            f"Conclusion: this reference is a {pacing_conclusion}. For this format, judge success by {metric_focus}, not generic long-form metrics.",
            "",
            "Next move: combine this reference pacing with fresh channel analytics and fresh public YouTube demand, then build the next script/scene plan from the overlap. If you want to proceed, say: plan the next short from this data.",
        ])
    return "\n".join(lines)


def _reference_analysis_actionable(data: dict[str, Any] | None) -> bool:
    """True when reference analysis has vision/transcript/story signal — not pacing-only."""
    if not isinstance(data, dict):
        return False
    depth = _reference_analysis_depth(data)
    if depth in {"full", "partial"}:
        return True
    from studio_agent.turn_plan import reference_has_topic_signal

    return reference_has_topic_signal(data)


def _parse_reference_analysis_payload(
    result: str,
    *,
    actionable_only: bool = False,
) -> dict[str, Any] | None:
    from studio_agent.stt_utils import tool_result_dict

    data = tool_result_dict(result)
    if not data:
        return None
    if not isinstance(data, dict):
        return None
    status = str(data.get("status") or "").strip().lower()
    kind = str(data.get("kind") or "").strip().lower()
    has_analysis_fields = bool(
        isinstance(data.get("pacing"), dict)
        or isinstance(data.get("analysis_profile"), dict)
        or data.get("visual_summary")
        or data.get("style_reference_note")
    )
    payload: dict[str, Any] | None = None
    if status == "complete" and (kind == "competitor" or has_analysis_fields):
        payload = data
    elif has_analysis_fields and status not in {"failed", "error"}:
        payload = data
    if not payload:
        return None
    if actionable_only and not _reference_analysis_actionable(payload):
        return None
    return payload


def _latest_reference_poll_payload(
    *,
    tool_fires: list[ToolFire] | None = None,
    messages: list[dict[str, Any]] | None = None,
    actionable_only: bool = False,
) -> dict[str, Any] | None:
    for fire in reversed(tool_fires or []):
        if str(fire.name or "") not in {
            "poll_render_job",
            "analyze_reference_video",
            "analyze_competitor_video",
            "retry_reference_analysis",
        }:
            continue
        payload = _parse_reference_analysis_payload(
            str(fire.result or ""),
            actionable_only=actionable_only,
        )
        if payload:
            return payload
    for msg in reversed(messages or []):
        text = str(msg.get("content") or "")
        if "job_id" not in text and "pacing" not in text and "visual_summary" not in text:
            continue
        payload = _parse_reference_analysis_payload(text, actionable_only=actionable_only)
        if payload:
            return payload
    return None


def _latest_complete_reference_analysis(
    *,
    tool_fires: list[ToolFire] | None = None,
    messages: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Latest poll payload including pacing-only (for formatting + retry decisions)."""
    return _latest_reference_poll_payload(
        tool_fires=tool_fires,
        messages=messages,
        actionable_only=False,
    )


def _recover_competitor_poll_target(session: dict[str, Any]) -> tuple[str, str] | None:
    target = _recover_poll_target(session, allow_transcript_fallback=True)
    if not target:
        return None
    job_id, kind = target
    if kind != "competitor":
        return None
    return job_id, kind


def _recent_assistant_promised_public_search(messages: list[dict[str, Any]], lookback: int = 8) -> bool:
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
                "fresh public youtube demand",
                "fresh channel analytics and fresh public youtube demand",
                "combine this reference pacing with fresh",
                "plan the next short from this data",
                "verified public youtube demand",
                "public youtube demand evidence",
                "predicted moves to test next",
            )
        ):
            return True
    return False


def _is_public_search_followup(user_text: str, messages: list[dict[str, Any]] | None = None) -> bool:
    """True when the user approved a fresh public-search pass after reference/research context."""
    if not store.should_auto_run_tools(user_text):
        return False
    low = str(user_text or "").lower()
    wants_search = bool(
        re.search(r"\b(?:fresh|live)\s+search\b", low)
        or (store.is_explicit_tool_go_ahead(user_text) and re.search(r"\bsearch\b", low))
        or _needs_fresh_public_search(user_text)
    )
    if not wants_search:
        return False
    if _recent_assistant_promised_public_search(messages):
        return True
    if _latest_complete_reference_analysis(messages=messages):
        return True
    return False


def _guard_needs_evidence_synthesis(guarded: str) -> bool:
    low = str(guarded or "").lower()
    return any(
        phrase in low
        for phrase in (
            "need a live/search/reference result",
            "verify the missing evidence",
            "channel analytics tool did not run",
            "cannot make a grounded performance claim",
            "i should use the available public youtube search",
            "verify it first, then answer from the evidence",
            "should not narrate work without executing",
            "run the matching studio tool now",
            "name the exact blocker instead of saying i am doing it",
            "run analyze_reference_video",
            "i caught an unsupported claim before sending it",
        )
    )


def _is_meta_guard_reply(text: str) -> bool:
    """True when the assistant text is an internal audit/guard message, not a user answer."""
    low = str(text or "").lower()
    return any(
        phrase in low
        for phrase in (
            "should not narrate work without executing",
            "run the matching studio tool now",
            "i caught an unsupported claim before sending it",
            "i need a live/search/reference result before presenting",
            "verify the missing evidence first",
        )
    )


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
        # False "analytics missing" narrations when tools already returned rows.
        "pulled your memory but not",
        "pulled memory but not",
        "not the actual youtube analytics",
        "not the actual analytics",
        "hitting a blocker",
        "i'm hitting a blocker",
        "analytics comparison is missing",
        "don't have the actual youtube analytics",
        "do not have the actual youtube analytics",
        "only have memory",
        "only pulled memory",
    )
    return any(phrase in low for phrase in stall_phrases)


def _assistant_asks_user_for_known_analytics(assistant_text: str) -> bool:
    """True when the model interviews the user for AVD/views already in tool evidence."""
    low = str(assistant_text or "").lower()
    if not low:
        return False
    asks = any(
        phrase in low
        for phrase in (
            "what i need to know",
            "i need to know",
            "can you tell me",
            "are both of these",
            "does the skeleton",
            "are both hitting",
            "once you tell me",
            "let me ask you",
        )
    )
    about_metrics = any(
        phrase in low
        for phrase in (
            "retention",
            "hook",
            "pacing",
            "script structure",
            "length",
            "avg view",
            "average view",
            "views",
        )
    )
    return asks and about_metrics


def _latest_shortform_comparison(tool_fires: list[ToolFire]) -> dict[str, Any]:
    for fire in reversed(tool_fires or []):
        if str(fire.name or "") != "get_channel_analytics":
            continue
        try:
            data = json.loads(fire.result or "{}")
        except Exception:
            continue
        if not isinstance(data, dict) or data.get("error"):
            continue
        compare = data.get("shortform_performance_comparison")
        if isinstance(compare, dict) and compare:
            return dict(compare)
    return {}


def _grounded_shortform_comparison_from_tools(
    tool_fires: list[ToolFire],
    *,
    active_label: str = "",
    user_text: str = "",
) -> str:
    """Authoritative Shorts comparison from analytics tool rows — never memory."""
    compare = _latest_shortform_comparison(tool_fires)
    brief = str(compare.get("comparison_brief") or "").strip()
    ready = bool(compare.get("comparison_ready"))
    title = str(active_label or "selected channel").strip() or "selected channel"
    if ready and brief:
        lines = [
            f"YouTube Analytics comparison for **{title}** (live tool result, not memory):",
            "",
            brief,
        ]
        low_user = str(user_text or "").lower()
        if any(k in low_user for k in ("female", "women", "woman", "her side", "for her")):
            lines.extend(
                [
                    "",
                    "Female-side packaging: keep the same measured structure that the retention "
                    "winner already proved (direct psychology promise in the title, 30s density, "
                    "same skeleton identity). Flip only the POV / 'you' framing toward her experience — "
                    "do not invent a new format until we have a measured female-side winner row.",
                ]
            )
        return "\n".join(lines)
    # Fall back to channel status if comparison object is empty but analytics ran.
    return _grounded_channel_status_from_tools(tool_fires, active_label=active_label)


def _assistant_claims_tools_unavailable(assistant_text: str) -> bool:
    """True when the model invents a tool-outage instead of calling production tools."""
    low = str(assistant_text or "").lower()
    return any(
        phrase in low
        for phrase in (
            "tool availability",
            "tools are offline",
            "tool is offline",
            "tools offline",
            "tool offline",
            "comes back online",
            "come back online",
            "when the tool comes back",
            "once the tool comes back",
            "until the tool comes back",
            "until tools return",
            "until the tools return",
            "tool comes back online",
            "tools come back",
            "tool is unavailable",
            "tools are unavailable",
            "tool not available",
            "tools not available",
            "can't call the tool",
            "cannot call the tool",
            "can't access the production tool",
            "cannot access the production tool",
            "production tool is down",
            "render tool is down",
            "start_shortform is unavailable",
            "build your scene blueprint manually",
            "building your scene blueprint manually",
            "work around this by building",
        )
    )


def _assistant_stalled_on_reference_analysis(assistant_text: str) -> bool:
    low = str(assistant_text or "").lower()
    stall_phrases = (
        "analysis is still queued",
        "analysis is still running",
        "reference analysis is still",
        "still waiting on the analysis",
        "still waiting for the analysis",
        "let me analyze the video",
        "let me analyze this video",
        "i'll analyze the video",
        "i will analyze the video",
        "i need to analyze the upload",
        "tell me directly",
        "instead of waiting",
        "poll analyze_reference_video",
        "run analyze_reference_video",
    )
    return any(phrase in low for phrase in stall_phrases)


def _assistant_promised_research_narration(assistant_text: str) -> bool:
    """Detect narration about future research the preflight already completed."""
    low = str(assistant_text or "").lower()
    narration_phrases = (
        "i'll analyze the reference",
        "i will analyze the reference",
        "let me analyze the reference",
        "i'll analyze this reference",
        "i will analyze this reference",
        "i'll pull public youtube",
        "i will pull public youtube",
        "let me pull public youtube",
        "pull public youtube data",
        "analyze the reference video and pull",
        "pull the most recent public youtube",
        "get the most recent public youtube",
    )
    return any(phrase in low for phrase in narration_phrases)


def _ideation_active_label(
    session: dict[str, Any],
    user_text: str,
    *,
    active_registry: str,
    active_channel_id: str,
) -> str:
    title = str(session.get("channel_title") or "").strip()
    if title:
        return title
    if active_registry:
        return active_registry.replace("_", " ")
    if active_channel_id:
        return active_channel_id
    query = _public_search_query_for_channel("", user_text).strip()
    return query[:96] or "your target niche"


def _grounded_ideation_research_from_tools(
    tool_fires: list[ToolFire],
    *,
    active_label: str,
    user_text: str = "",
    has_reference_upload: bool = False,
) -> str:
    """Deterministic ideation answer when the model narrates instead of using preflight evidence.

    Returns Grok-class conversation prose — never the research form.
    """
    from studio_agent.conversation import deterministic_conversational_research_reply, strip_robot_research_artifacts

    research = _grounded_research_summary_from_tools(
        tool_fires,
        active_label=active_label,
        user_text=user_text,
        include_channel=False,
    )
    prose = deterministic_conversational_research_reply(
        user_text=user_text,
        evidence=research,
        niche_hint=active_label or "",
    )
    extra: list[str] = []
    # Keep lightweight visual notes for uploads only (still conversational).
    _ = has_reference_upload
    visual_summary = ""
    for fire in tool_fires or []:
        if str(fire.name or "") not in {"analyze_reference_video", "analyze_competitor_video", "poll_render_job"}:
            continue
        try:
            payload = json.loads(fire.result or "{}")
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            continue
        candidate = str(payload.get("visual_summary") or "").strip()
        if not candidate and isinstance(payload.get("visual_summary"), dict):
            candidate = str(payload["visual_summary"].get("summary") or "").strip()
        if candidate:
            visual_summary = candidate[:400]
            break
    if visual_summary:
        extra.append(f"From the uploaded reference look: {visual_summary}")
    if extra:
        prose = prose + "\n\n" + "\n".join(extra)
    return strip_robot_research_artifacts(prose)


def _user_wants_fresh_reference_analysis(user_text: str) -> bool:
    low = store._user_message_before_attachments(user_text).lower()
    if store.is_contextual_reference_video_request(user_text):
        return True
    return any(
        phrase in low
        for phrase in (
            "try again",
            "retry",
            "watch again",
            "analyze again",
            "re-run",
            "rerun",
            "run it again",
            "do it again",
        )
    )


def _user_wants_reference_stage_retry(user_text: str) -> bool:
    low = store._user_message_before_attachments(user_text).lower()
    return any(
        phrase in low
        for phrase in (
            "retry",
            "try again",
            "re-run",
            "rerun",
            "missing research",
            "transcript",
            "retry transcript",
            "extract transcript",
            "re-extract",
            "fix transcript",
            "run it again",
            "do it again",
        )
    )


def _infer_reference_retry_stages(payload: dict[str, Any]) -> list[str]:
    gaps = payload.get("analysis_gaps") if isinstance(payload.get("analysis_gaps"), dict) else {}
    stage_errors = gaps.get("stage_errors") if isinstance(gaps.get("stage_errors"), dict) else {}
    stages = [str(stage or "").strip().lower() for stage in stage_errors.keys() if str(stage or "").strip()]
    if stages:
        return stages
    depth = _reference_analysis_depth(payload)
    if depth == "partial":
        transcript = payload.get("transcript") if isinstance(payload.get("transcript"), dict) else {}
        visual = payload.get("visual_summary") if isinstance(payload.get("visual_summary"), dict) else {}
        storytelling = payload.get("storytelling") if isinstance(payload.get("storytelling"), dict) else {}
        inferred: list[str] = []
        if str(transcript.get("error") or "").strip() or not str(transcript.get("text") or "").strip():
            inferred.append("transcript")
        if str(visual.get("error") or "").strip() or not str(visual.get("summary") or "").strip():
            inferred.append("vision")
        if str(storytelling.get("error") or "").strip() or not str(storytelling.get("summary") or "").strip():
            inferred.append("storytelling")
        return inferred or ["transcript", "storytelling"]
    return ["transcript", "vision", "storytelling"]


def _reference_retry_already_attempted(
    *,
    tool_fires: list[ToolFire] | None = None,
    messages: list[dict[str, Any]] | None = None,
    job_id: str = "",
) -> bool:
    wanted = str(job_id or "").strip()
    for fire in reversed(tool_fires or []):
        if str(fire.name or "") != "retry_reference_analysis":
            continue
        if not wanted:
            return True
        try:
            payload = json.loads(fire.result or "{}")
        except Exception:
            payload = {}
        if str((payload or {}).get("job_id") or "").strip() == wanted:
            return True
    for msg in reversed(messages or []):
        if str(msg.get("role") or "") != "tool":
            continue
        text = str(msg.get("content") or "")
        if "retry_reference_analysis" not in text and "retried_stages" not in text:
            continue
        if wanted and wanted not in text:
            continue
        return True
    return False


# ~4–5 minutes total — yt-dlp download + keyframes + STT often exceeds 60s on Fly.
_COMPETITOR_POLL_DELAYS = (2.0, 2.0, 3.0, 4.0) + (5.0,) * 48


def _reference_poll_terminal(polled: dict[str, Any]) -> bool:
    status = str(polled.get("status") or "").strip().lower()
    if status in {"failed", "error"}:
        return True
    if status in {"complete", "incomplete"}:
        return True
    return False


def _reference_poll_succeeded(polled: dict[str, Any]) -> bool:
    status = str(polled.get("status") or "").strip().lower()
    if status in {"failed", "error", "incomplete"}:
        return False
    if status != "complete":
        return False
    return _reference_analysis_actionable(polled)


def _latest_reference_actionable_from_fires(
    tool_fires: list[ToolFire] | None,
    *,
    messages: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    return _latest_reference_poll_payload(
        tool_fires=tool_fires,
        messages=messages,
        actionable_only=True,
    )


async def _poll_competitor_reference_job(
    *,
    emit: EventEmitter | None,
    user_id: str,
    content_format: str,
    session_id: str,
    messages: list[dict[str, Any]],
    tool_fires: list[ToolFire],
    job_id: str,
    auto_retry_incomplete: bool = True,
) -> str:
    """Poll until deep analysis succeeds, fails, or pacing-only retry is exhausted."""
    final_poll = ""
    retried = False
    await _fire_event(emit, "tool_start", tool="poll_render_job", round=0, awaiting_approval=False)
    poll_args = {"job_id": job_id, "kind": "competitor"}

    for delay in _COMPETITOR_POLL_DELAYS:
        if delay > 0:
            await asyncio.sleep(delay)
        try:
            poll_result = execute_tool_logged(
                "poll_render_job",
                poll_args,
                user_id=user_id,
                content_format=content_format,
                session_id=session_id,
            )
        except Exception as exc:
            poll_result = json.dumps({"job_id": job_id, "kind": "competitor", "error": str(exc)}, indent=2)
        final_poll = poll_result
        tool_fires.append(ToolFire("poll_render_job", dict(poll_args), poll_result))
        messages.append(_tool_observation_message("poll_render_job", poll_result))
        store.update_session(session_id, messages=messages)
        try:
            polled = json.loads(poll_result or "{}")
        except Exception:
            polled = {}
        if not isinstance(polled, dict):
            polled = {}

        stage_label = str(polled.get("stage_label") or polled.get("stage") or "").strip()
        stage_detail = str(polled.get("stage_detail") or "").strip()
        if stage_label:
            progress_note = stage_label
            if stage_detail:
                progress_note = f"{stage_label} — {stage_detail[:120]}"
            await _fire_event(emit, "status", message=f"Reference analysis: {progress_note}")
        await _fire_event(emit, "job_snapshot", snapshot=polled)

        if _reference_poll_terminal(polled):
            if (
                auto_retry_incomplete
                and not retried
                and not _reference_poll_succeeded(polled)
                and str(polled.get("status") or "").lower() != "failed"
                and not _reference_retry_already_attempted(
                    tool_fires=tool_fires,
                    messages=messages,
                    job_id=job_id,
                )
            ):
                retried = True
                retry_stages = _infer_reference_retry_stages(polled)
                await _fire_event(emit, "status", message="Retrying failed vision/transcript/story stages…")
                await _fire_event(emit, "tool_start", tool="retry_reference_analysis", round=0, awaiting_approval=False)
                retry_args = {"job_id": job_id, "stages": retry_stages}
                try:
                    retry_result = execute_tool_logged(
                        "retry_reference_analysis",
                        retry_args,
                        user_id=user_id,
                        content_format=content_format,
                        session_id=session_id,
                    )
                except Exception as exc:
                    retry_result = json.dumps({"job_id": job_id, "status": "failed", "error": str(exc)}, indent=2)
                tool_fires.append(ToolFire("retry_reference_analysis", dict(retry_args), retry_result))
                messages.append(_tool_observation_message("retry_reference_analysis", retry_result))
                store.update_session(session_id, messages=messages)
                await _fire_event(emit, "tool_end", tool="retry_reference_analysis", status="ok")
                try:
                    refreshed = json.loads(retry_result or "{}")
                except Exception:
                    refreshed = {}
                if isinstance(refreshed, dict) and refreshed:
                    final_poll = json.dumps(refreshed, indent=2, ensure_ascii=False)
                    tool_fires.append(ToolFire("poll_render_job", dict(poll_args), final_poll))
                    messages.append(_tool_observation_message("poll_render_job", final_poll))
                    store.update_session(session_id, messages=messages)
                    polled = refreshed
                if _reference_poll_succeeded(polled):
                    break
                continue
            break

    try:
        final_payload = json.loads(final_poll or "{}")
    except Exception:
        final_payload = {}
    terminal_status = "error"
    if isinstance(final_payload, dict):
        if _reference_poll_succeeded(final_payload):
            terminal_status = "ok"
        elif str(final_payload.get("status") or "").lower() in {"failed", "error"}:
            terminal_status = "error"
        elif str(final_payload.get("status") or "").lower() == "incomplete":
            terminal_status = "error"
    await _fire_event(emit, "tool_end", tool="poll_render_job", status=terminal_status)
    return final_poll


async def _run_youtube_url_reference_preflight(
    *,
    emit: EventEmitter | None,
    user_id: str,
    content_format: str,
    session_id: str,
    messages: list[dict[str, Any]],
    tool_fires: list[ToolFire],
    user_text: str,
    reference_request_text: str = "",
) -> None:
    """Download and analyze a pasted YouTube reference URL before niche research."""
    from studio_agent.turn_plan import extract_youtube_urls_from_text, reference_has_topic_signal

    request_text = str(reference_request_text or user_text).strip()
    existing = _latest_complete_reference_analysis(tool_fires=tool_fires, messages=messages)
    if (
        existing
        and reference_has_topic_signal(existing)
        and not _user_wants_fresh_reference_analysis(request_text)
        and not store.is_contextual_reference_video_request(request_text)
    ):
        return

    urls = extract_youtube_urls_from_text(user_text)
    url = str(urls[0] if urls else "").strip()
    if not url:
        return

    await _fire_tool_start(
        emit,
        "analyze_reference_video",
        args={"url": url},
        round=0,
        awaiting_approval=False,
    )
    args = {"url": url, "content_format": "short" if content_format != "long" else "long", "max_frames": 40}
    try:
        start_result = execute_tool_logged(
            "analyze_reference_video",
            args,
            user_id=user_id,
            content_format=content_format,
            session_id=session_id,
        )
    except Exception as exc:
        start_result = json.dumps({"error": str(exc)}, indent=2)
    tool_fires.append(ToolFire("analyze_reference_video", dict(args), start_result))
    messages.append(_tool_observation_message("analyze_reference_video", start_result))
    store.update_session(session_id, messages=messages)

    try:
        started = json.loads(start_result or "{}")
    except Exception:
        started = {}
    job_id = str(started.get("job_id") or "").strip() if isinstance(started, dict) else ""
    if not job_id:
        await _fire_tool_end(
            emit,
            "analyze_reference_video",
            status="error",
            args=args,
            result=start_result,
            error=str((started or {}).get("error") or "no_job_id")[:160],
        )
        return

    final_poll = await _poll_competitor_reference_job(
        emit=emit,
        user_id=user_id,
        content_format=content_format,
        session_id=session_id,
        messages=messages,
        tool_fires=tool_fires,
        job_id=job_id,
    )

    try:
        final_payload = json.loads(final_poll or "{}") if final_poll else {}
    except Exception:
        final_payload = {}
    analysis_ok = bool(isinstance(final_payload, dict) and _reference_poll_succeeded(final_payload))
    await _fire_tool_end(
        emit,
        "analyze_reference_video",
        status="ok" if analysis_ok else "error",
        args=args,
        result=final_poll or start_result,
        error=None if analysis_ok else str(
            (final_payload or {}).get("error")
            or (final_payload or {}).get("stage")
            or "analysis_incomplete"
        )[:160],
    )

    if final_poll and analysis_ok:
        messages.append({
            "role": "system",
            "content": (
                "[YouTube URL reference analysis preflight completed. The linked video above was downloaded "
                "and analyzed before public niche research. Answer from visual_summary, transcript, and pacing fields.]"
            ),
        })
        store.update_session(session_id, messages=messages)
    elif final_poll:
        # Surface partial metadata so the model is honest instead of inventing a glitch.
        messages.append({
            "role": "system",
            "content": (
                f"[YouTube reference analysis incomplete for {url}. "
                f"Last status: {json.dumps(final_payload, ensure_ascii=False)[:1800]}. "
                "Report what is known (title/views/pacing if present). "
                "Do not invent hooks or transcripts. Offer one retry or direct video URLs. "
                "Do NOT start shortform production.]"
            ),
        })
        store.update_session(session_id, messages=messages)


def _watch_url_from_video_row(row: dict[str, Any]) -> str:
    url = str(row.get("watch_url") or row.get("url") or "").strip()
    if url:
        return url
    video_id = str(row.get("video_id") or row.get("id") or "").strip()
    return f"https://www.youtube.com/watch?v={video_id}" if video_id else ""


def _resolve_contextual_reference_video_url(
    user_text: str,
    *,
    messages: list[dict[str, Any]] | None = None,
    tool_fires: list[ToolFire] | None = None,
) -> str:
    """Resolve a YouTube watch URL for follow-ups like 'download that specific video'."""
    from studio_agent.turn_plan import extract_youtube_urls_from_text

    direct_urls = extract_youtube_urls_from_text(user_text)
    if direct_urls:
        return str(direct_urls[0]).strip()

    for fire in reversed(list(tool_fires or [])):
        name = str(fire.name or "").strip()
        if name == "analyze_reference_video":
            args_url = str((fire.args or {}).get("url") or "").strip()
            if args_url:
                return args_url
        if name == "fetch_competitor_channel_videos":
            try:
                data = json.loads(fire.result or "{}")
            except Exception:
                data = {}
            rows = data.get("video_rows") if isinstance(data, dict) else []
            best = _pick_channel_reference_video(rows if isinstance(rows, list) else [])
            url = _channel_video_watch_url(best) or _watch_url_from_video_row(best)
            if url:
                return url
        if name == "search_youtube_public":
            try:
                data = json.loads(fire.result or "{}")
            except Exception:
                data = {}
            videos = data.get("videos") if isinstance(data, dict) else []
            for row in list(videos or []):
                if not isinstance(row, dict):
                    continue
                url = _watch_url_from_video_row(row)
                if url:
                    return url

    for msg in reversed(list(messages or [])):
        role = str(msg.get("role") or "").strip().lower()
        if role not in {"tool", "assistant", "system"}:
            continue
        text = str(msg.get("content") or "")
        urls = extract_youtube_urls_from_text(text)
        if urls:
            return str(urls[0]).strip()
        if "fetch_competitor_channel_videos" in text or '"video_rows"' in text:
            try:
                payload = json.loads(text)
            except Exception:
                payload = {}
            if isinstance(payload, dict):
                rows = payload.get("video_rows") or payload.get("videos") or []
                best = _pick_channel_reference_video(rows if isinstance(rows, list) else [])
                url = _channel_video_watch_url(best) or _watch_url_from_video_row(best)
                if url:
                    return url
        for match in re.finditer(r'"video_id"\s*:\s*"([A-Za-z0-9_-]{11})"', text):
            video_id = str(match.group(1) or "").strip()
            if video_id:
                return f"https://www.youtube.com/watch?v={video_id}"

    return ""


def _pick_channel_reference_video(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Choose the strongest public upload from a competitor channel page."""
    candidates = [dict(row) for row in list(rows or []) if isinstance(row, dict)]
    if not candidates:
        return {}

    max_views = max(int(row.get("views") or row.get("view_count") or 0) for row in candidates)
    if max_views <= 0:
        # Flat channel extracts list newest uploads first but often omit view counts.
        return candidates[0]

    def _score(row: dict[str, Any]) -> tuple[int, int, str]:
        views = int(row.get("views") or row.get("view_count") or 0)
        published = str(row.get("published_at") or row.get("published_label") or "")
        return (views, len(published), published)

    candidates.sort(key=_score, reverse=True)
    return candidates[0]


def _channel_video_watch_url(row: dict[str, Any]) -> str:
    url = str(row.get("watch_url") or "").strip()
    if url:
        return url
    video_id = str(row.get("video_id") or "").strip()
    return f"https://www.youtube.com/watch?v={video_id}" if video_id else ""


def _channel_analysis_video_limit(user_text: str) -> int:
    """How many uploads to download+analyze for a channel-analysis turn."""
    low = str(user_text or "").lower()
    # Explicit counts: "all 3", "top 5", "every video"
    m = re.search(r"\b(?:all|every|each)\s+(\d+)\b", low)
    if m:
        try:
            return max(1, min(5, int(m.group(1))))
        except ValueError:
            pass
    m = re.search(r"\b(\d+)\s+videos?\b", low)
    if m:
        try:
            return max(1, min(5, int(m.group(1))))
        except ValueError:
            pass
    if re.search(r"\b(?:all|every|each)\s+(?:of\s+)?(?:my\s+)?(?:the\s+)?videos?\b", low):
        return 3
    if re.search(r"\bdownload\s+(?:all|them|every)\b", low):
        return 3
    return 1


def _resolve_channel_url_from_context(
    user_text: str,
    messages: list[dict[str, Any]] | None = None,
    session: dict[str, Any] | None = None,
) -> str:
    """Find a channel URL in this turn, recent chat, or session metadata."""
    from studio_agent.turn_plan import competitor_channel_url, extract_youtube_channel_urls_from_text

    direct = competitor_channel_url(user_text)
    if direct:
        return direct
    # Scan recent user/assistant messages for a channel link.
    for msg in reversed(list(messages or [])[-30:]):
        if not isinstance(msg, dict):
            continue
        content = str(msg.get("content") or "")
        urls = extract_youtube_channel_urls_from_text(content)
        if urls:
            return urls[0]
        via = competitor_channel_url(content)
        if via:
            return via
    sess = session or {}
    for key in ("last_channel_url", "channel_url", "competitor_channel_url"):
        val = str(sess.get(key) or "").strip()
        if val and ("youtube.com/" in val.lower() or val.startswith("@")):
            if val.startswith("@"):
                return f"https://www.youtube.com/{val}"
            return val
    handle = str(sess.get("channel_handle") or "").strip().lstrip("@")
    if handle:
        return f"https://www.youtube.com/@{handle}"
    return ""


async def _run_competitor_channel_analysis_preflight(
    *,
    emit: EventEmitter | None,
    user_id: str,
    content_format: str,
    session_id: str,
    messages: list[dict[str, Any]],
    tool_fires: list[ToolFire],
    user_text: str,
    session: dict[str, Any] | None = None,
) -> None:
    """Fetch uploads from a pasted channel URL, then download + analyze 1–N videos."""
    from studio_agent.turn_plan import extract_competitor_channel_label
    from youtube import _youtube_fetch_public_channel_page_videos

    channel_url = _resolve_channel_url_from_context(user_text, messages=messages, session=session)
    if not channel_url:
        messages.append({
            "role": "system",
            "content": (
                "[Channel analysis requested but no YouTube channel URL was found in this turn "
                "or recent chat. Ask the user to paste the channel link again. "
                "Do NOT start shortform production.]"
            ),
        })
        store.update_session(session_id, messages=messages, pending_actions=[])
        return

    channel_label = extract_competitor_channel_label(user_text) or channel_url
    video_limit = _channel_analysis_video_limit(user_text)
    await _fire_tool_start(
        emit,
        "fetch_competitor_channel_videos",
        args={"channel_url": channel_url, "limit": video_limit},
        round=0,
        awaiting_approval=False,
    )
    try:
        rows = await _youtube_fetch_public_channel_page_videos(
            "",
            channel_url=channel_url,
            max_results=max(6, video_limit),
            hydrate_metadata=False,
        )
    except Exception as exc:
        rows = [{"error": str(exc)[:200]}]
    clean_rows = [r for r in (rows if isinstance(rows, list) else []) if isinstance(r, dict) and not r.get("error")]
    channel_payload = {
        "channel_url": channel_url,
        "channel_label": channel_label,
        "video_rows": clean_rows[:8],
        "video_count": len(clean_rows),
        "analyze_limit": video_limit,
    }
    channel_result = json.dumps(channel_payload, indent=2, ensure_ascii=False)
    tool_fires.append(ToolFire("fetch_competitor_channel_videos", {"channel_url": channel_url}, channel_result))
    messages.append(_tool_observation_message("fetch_competitor_channel_videos", channel_result))
    store.update_session(session_id, messages=messages)
    await _fire_tool_end(
        emit,
        "fetch_competitor_channel_videos",
        status="ok" if clean_rows else "error",
        args={"channel_url": channel_url},
        result=channel_result,
        error=None if clean_rows else "no_public_uploads",
    )

    # Prefer newest / highest-signal uploads first, then take up to video_limit.
    # Cap deep jobs per turn so the chat stream does not time out (3× full yt-dlp+STT
    # sequential runs often exceed proxy limits). Parallel start + shared poll budget.
    deep_limit = max(1, min(int(video_limit or 1), 2))
    ranked: list[dict[str, Any]] = []
    if clean_rows:
        remaining = list(clean_rows)
        while remaining and len(ranked) < deep_limit:
            best = _pick_channel_reference_video(remaining)
            if not best:
                break
            ranked.append(best)
            best_url = _channel_video_watch_url(best)
            remaining = [r for r in remaining if _channel_video_watch_url(r) != best_url]
            if not best_url:
                break

    started_jobs: list[tuple[str, str]] = []  # (url, job_id)
    for row in ranked:
        video_url = _channel_video_watch_url(row)
        if not video_url:
            continue
        await _fire_tool_start(
            emit,
            "analyze_reference_video",
            args={"url": video_url},
            round=0,
            awaiting_approval=False,
        )
        args = {
            "url": video_url,
            "content_format": "short" if content_format != "long" else "long",
            "max_frames": 32,
        }
        try:
            start_result = execute_tool_logged(
                "analyze_reference_video",
                args,
                user_id=user_id,
                content_format=content_format,
                session_id=session_id,
            )
        except Exception as exc:
            start_result = json.dumps({"error": str(exc)}, indent=2)
        tool_fires.append(ToolFire("analyze_reference_video", dict(args), start_result))
        messages.append(_tool_observation_message("analyze_reference_video", start_result))
        try:
            started = json.loads(start_result or "{}")
        except Exception:
            started = {}
        job_id = str((started or {}).get("job_id") or "").strip()
        if job_id:
            started_jobs.append((video_url, job_id))
            active_jobs = merge_active_jobs(
                list((store.get_session(session_id) or {}).get("active_jobs") or []),
                extract_jobs_from_tool("analyze_reference_video", start_result),
            )
            store.update_session(session_id, messages=messages, active_jobs=active_jobs)
            await _fire_event(emit, "active_jobs", jobs=active_jobs)
        else:
            await _fire_tool_end(
                emit,
                "analyze_reference_video",
                status="error",
                args=args,
                result=start_result,
                error=str((started or {}).get("error") or "no_job_id")[:160],
            )

    # Shared poll budget for all started jobs (~3–4 minutes total, not per video).
    complete_urls: list[str] = []
    if started_jobs:
        await _fire_event(emit, "status", message=f"Deep-analyzing {len(started_jobs)} channel video(s)…")
        pending_ids = {jid: url for url, jid in started_jobs}
        finals: dict[str, dict[str, Any]] = {}
        await _fire_event(emit, "tool_start", tool="poll_render_job", round=0, awaiting_approval=False)
        for delay in _COMPETITOR_POLL_DELAYS:
            if delay > 0:
                await asyncio.sleep(delay)
            if not pending_ids:
                break
            for jid in list(pending_ids.keys()):
                poll_args = {"job_id": jid, "kind": "competitor"}
                try:
                    poll_result = execute_tool_logged(
                        "poll_render_job",
                        poll_args,
                        user_id=user_id,
                        content_format=content_format,
                        session_id=session_id,
                    )
                except Exception as exc:
                    poll_result = json.dumps({"job_id": jid, "kind": "competitor", "error": str(exc)}, indent=2)
                tool_fires.append(ToolFire("poll_render_job", dict(poll_args), poll_result))
                messages.append(_tool_observation_message("poll_render_job", poll_result))
                try:
                    polled = json.loads(poll_result or "{}")
                except Exception:
                    polled = {}
                if not isinstance(polled, dict):
                    polled = {}
                stage_label = str(polled.get("stage_label") or polled.get("stage") or "").strip()
                if stage_label:
                    await _fire_event(
                        emit,
                        "status",
                        message=f"Reference analysis ({pending_ids[jid][:48]}…): {stage_label}",
                    )
                await _fire_event(emit, "job_snapshot", snapshot=polled)
                if _reference_poll_terminal(polled):
                    finals[jid] = polled
                    pending_ids.pop(jid, None)
            store.update_session(session_id, messages=messages)
        await _fire_event(
            emit,
            "tool_end",
            tool="poll_render_job",
            status="ok" if any(_reference_poll_succeeded(p) for p in finals.values()) else "error",
        )
        for url, jid in started_jobs:
            polled = finals.get(jid) or {}
            ok = _reference_poll_succeeded(polled)
            await _fire_tool_end(
                emit,
                "analyze_reference_video",
                status="ok" if ok else "error",
                args={"url": url},
                result=json.dumps(polled, ensure_ascii=False) if polled else None,
                error=None if ok else str(polled.get("error") or polled.get("stage") or "incomplete")[:160],
            )
            if ok:
                complete_urls.append(url)

    if not ranked:
        messages.append({
            "role": "system",
            "content": (
                f"[Channel analysis for {channel_label} could not find public uploads to download. "
                f"Channel page: {channel_url}. Tell the user honestly and suggest a direct video URL. "
                "Do NOT start shortform production.]"
            ),
        })
    else:
        outcome = _format_channel_analysis_outcome(
            tool_fires,
            channel_url=channel_url,
            channel_label=channel_label,
        )
        messages.append({
            "role": "system",
            "content": (
                f"[Channel analysis outcome for {channel_label} ({channel_url}). "
                f"Deep jobs started={len(started_jobs)}, complete={len(complete_urls)}. "
                f"User-facing summary to ground the reply:\n{outcome}\n"
                "Do NOT start shortform production unless the user explicitly asks to render.]"
            ),
        })
        # Stash for skip_model_loop path
        messages.append({
            "role": "system",
            "content": f"[CHANNEL_ANALYSIS_USER_SUMMARY]\n{outcome}",
        })
    store.update_session(
        session_id,
        messages=messages,
        pending_actions=[],
        last_channel_url=channel_url,
    )


async def _run_public_youtube_research_preflight(
    *,
    emit: EventEmitter | None,
    user_id: str,
    content_format: str,
    session_id: str,
    messages: list[dict[str, Any]],
    user_text: str,
    session: dict[str, Any],
    active_registry: str,
    active_channel_id: str,
) -> list[ToolFire]:
    """Deterministically run public YouTube niche search tools (no model narration)."""
    from studio_agent.live_demand import (
        build_live_demand_plan,
        format_demand_brief_from_tool_fires,
        format_live_demand_system_note,
    )
    from studio_agent.turn_plan import (
        coerce_public_search_query,
        derive_niche_search_query,
        extract_known_niche_phrase,
        is_garbage_public_search_query,
        is_unusable_public_search_query,
        reference_has_topic_signal,
    )

    from studio_agent.live_demand import resolve_demand_search_query

    live_plan = build_live_demand_plan(user_text, session)
    active_label = (
        str(session.get("channel_title") or "").strip()
        or str(active_registry or "").replace("_", " ")
        or live_plan.niche_hint
        or "YouTube"
    )
    tool_fires: list[ToolFire] = []
    if live_plan.required:
        messages.append({"role": "system", "content": format_live_demand_system_note(live_plan)})
    if store.is_youtube_url_reference_request(user_text):
        await _run_youtube_url_reference_preflight(
            emit=emit,
            user_id=user_id,
            content_format=content_format,
            session_id=session_id,
            messages=messages,
            tool_fires=tool_fires,
            user_text=user_text,
        )

    ref_payload = _latest_complete_reference_analysis(tool_fires=tool_fires, messages=messages)
    fallback_query = _public_search_query_for_channel(
        active_label,
        user_text,
        registry_key=active_registry,
    )
    # Prefer Live Demand niche seed (known phrase / session continuity) over chat-token soup.
    if live_plan.niche_hint or live_plan.search_query:
        seed = live_plan.search_query or f"{live_plan.niche_hint} YouTube Shorts"
        if seed and not is_garbage_public_search_query(seed):
            fallback_query = seed
    known = extract_known_niche_phrase(user_text) or extract_known_niche_phrase(
        " ".join(
            str(m.get("content") or "")
            for m in list(messages or [])[-8:]
            if str(m.get("role") or "") == "user"
        )
    )
    if known:
        fallback_query = f"{known} YouTube Shorts"
    if reference_has_topic_signal(ref_payload):
        preferred_query = (
            derive_niche_search_query(
                ref_payload,
                user_text=user_text,
                active_label=active_label,
                fallback_query=fallback_query,
            )
            or fallback_query
        )
    else:
        preferred_query = (
            derive_niche_search_query(
                None,
                user_text=user_text,
                active_label=active_label,
                fallback_query=fallback_query,
            )
            or fallback_query
        )
    # Central resolver: never ship dictation soup or generic unusable "YouTube Shorts trending".
    search_query = resolve_demand_search_query(
        user_text,
        session,
        active_label=active_label,
        registry_key=active_registry,
        fallback_query=preferred_query or fallback_query,
    )
    if (
        not search_query
        or is_garbage_public_search_query(search_query)
        or is_unusable_public_search_query(search_query)
    ):
        from studio_agent.turn_plan import default_discovery_search_query, is_banned_faceless_hooks_query

        candidates = [
            (f"{known} YouTube Shorts" if known else ""),
            (live_plan.search_query if live_plan.niche_hint else ""),
            fallback_query,
            default_discovery_search_query(),
        ]
        search_query = default_discovery_search_query()
        for cand in candidates:
            cand = str(cand or "").strip()
            if not cand or is_banned_faceless_hooks_query(cand):
                continue
            if is_garbage_public_search_query(cand) or is_unusable_public_search_query(cand):
                continue
            search_query = cand
            break
    # Quota survival: only force cache bypass on explicit live/right-now language.
    fresh = bool(live_plan.fresh) or _public_search_use_fresh(user_text, public_demand=True)
    window_days = (
        live_plan.window_days
        if live_plan.required
        else _public_search_window_days(user_text)
    )
    # Discovery / no-niche: use at least 7d so Shorts search returns hydrated rows.
    try:
        from studio_agent.live_demand import discovery_search_queries

        if any(search_query.lower() == d.lower() for d in discovery_search_queries()) or not live_plan.niche_hint:
            if live_plan.reasons and "discovery_fallback_no_niche" in live_plan.reasons:
                window_days = max(window_days, 7)
            if not live_plan.niche_hint and window_days < 7:
                window_days = max(window_days, 7)
    except Exception:
        pass
    search_args: dict[str, Any] = {
        "query": search_query,
        "days": window_days,
        "fresh": fresh,
    }
    if active_registry:
        search_args["registry_key"] = active_registry
    plan: list[tuple[str, dict[str, Any]]] = []
    # Connected-channel analytics when selected, or when Live Demand explicitly asks for it.
    if active_registry or active_channel_id:
        channel_args: dict[str, Any] = {}
        if active_registry:
            channel_args["registry_key"] = active_registry
        if active_channel_id:
            channel_args["channel_id"] = active_channel_id
        plan.append(("get_channel_analytics", channel_args))
    # ONE public search tool per turn (not get_public_search_trends + search_youtube_public).
    # Dual tools doubled search.list cost for the same niche.
    plan.append(("get_public_search_trends", dict(search_args)))
    cache_label = "live/fresh" if fresh else "cache-first"
    await _fire_event(
        emit,
        "status",
        message=(
            f"Live Demand: pulling public YouTube ({window_days}d, {cache_label}) for "
            f"{search_query[:72]}..."
        ),
    )
    for tool_name, args in plan:
        await _fire_tool_start(emit, tool_name, args=args, round=0, awaiting_approval=False)
        try:
            result = execute_tool_logged(
                tool_name,
                args,
                user_id=user_id,
                content_format=content_format,
                session_id=session_id,
            )
        except Exception as exc:
            result = json.dumps({"error": str(exc)}, indent=2)
        tool_fires.append(ToolFire(tool_name, dict(args), result))
        try:
            memory.observe_tool_result(str(user_id), tool_name, args, result)
        except Exception:
            pass
        messages.append(_tool_observation_message(tool_name, result))
        err_preview = ""
        try:
            parsed = json.loads(result or "{}")
            if isinstance(parsed, dict) and parsed.get("error"):
                err_preview = str(parsed.get("error"))[:160]
        except Exception:
            pass
        await _fire_tool_end(
            emit,
            tool_name,
            status="error" if err_preview else "ok",
            args=args,
            result=result,
            error=err_preview or None,
        )
    if live_plan.required or _has_public_demand_tool(tool_fires):
        brief = format_demand_brief_from_tool_fires(tool_fires, plan=live_plan)
        messages.append({
            "role": "system",
            "content": (
                "[Live Demand brief — production must follow this evidence]\n" + brief
            ),
        })
        try:
            store.update_session(
                session_id,
                messages=messages,
                last_live_demand={
                    **live_plan.as_dict(),
                    "brief": brief,
                    "search_query": search_query,
                    "window_days": window_days,
                    "updated_at": time.time(),
                },
            )
        except Exception:
            store.update_session(session_id, messages=messages)
    else:
        store.update_session(session_id, messages=messages)
    return tool_fires


def _assistant_denies_public_trending_capability(assistant_text: str) -> bool:
    low = str(assistant_text or "").lower()
    return any(
        phrase in low
        for phrase in (
            "does not return real-time",
            "doesn't return real-time",
            "do not return real-time",
            "don't return real-time",
            "youtube search does not return",
            "youtube doesn't return real-time",
            "only static video performance snapshots",
            "static video performance snapshots",
            "not return real-time trending",
            "no real-time trending",
        )
    )


def _assistant_denies_public_research_tool(assistant_text: str) -> bool:
    low = str(assistant_text or "").lower()
    if _assistant_denies_public_trending_capability(assistant_text):
        return True
    return any(
        phrase in low
        for phrase in (
            "don't have a direct \"search youtube niche performance\" tool",
            'don\'t have a direct "search youtube niche performance" tool',
            "do not have a direct \"search youtube niche performance\" tool",
            "don't have a direct market data tool",
            "do not have a direct market data tool",
            "no direct market data tool",
            "don't have a tool that filters",
            "do not have a tool that filters",
            "does not expose granular date",
            "doesn't expose granular date",
            "last 14-30 days only",
            "14-30 days only",
            "strict 14-30 day",
            "manual verification",
            "screenshot their view counts",
            "paste them here",
            "two options:",
            "option 1: manual",
            "which would you prefer",
            "unfortunately, i don't have",
            "unfortunately, i do not have",
            "search youtube niche performance",
            "don't have a direct",
            "do not have a direct",
            "not wired in yet",
            "google trends and third-party keyword-volume apis are not wired",
            "what i can do instead",
            "manual search approach",
            "list_studio_channels",
        )
    )


def _assistant_denies_upload_analysis_capability(assistant_text: str) -> bool:
    low = str(assistant_text or "").lower()
    return any(
        phrase in low
        for phrase in (
            "don't have a tool available",
            "do not have a tool available",
            "functional video analysis tool",
            "video analysis tool available",
            "current toolset",
            "video playback or frame extraction",
            "audio transcription from video",
            "automatic topic/content detection",
            "cannot watch the video",
            "can't watch the video",
            "cannot directly analyze",
            "can't directly analyze",
            "cannot directly process",
            "can't directly process",
            "isn't able to directly process",
            "is not able to directly process",
            "technical limitation",
            "hitting a technical limitation",
            "constraint on my end",
            "read the video file directly",
            "attempt to read the video file",
            "please tell me",
            "what i need from you",
            "fastest path forward is for you to tell me",
            "no retry tool",
            "no tool to retry",
            "cannot retry transcript",
            "can't retry transcript",
            "retry transcript extraction",
            "not available in my toolset",
            "not in my toolset",
            "missing from my toolset",
        )
    )


def _reference_preflight_error_summary(tool_fires: list[ToolFire]) -> str:
    """Extract the first concrete backend error from reference-analysis preflight."""
    for fire in reversed(tool_fires or []):
        if str(fire.name or "") not in {"analyze_reference_video", "poll_render_job"}:
            continue
        try:
            payload = json.loads(fire.result or "{}")
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            continue
        err = str(payload.get("error") or payload.get("message") or "").strip()
        status = str(payload.get("status") or "").strip().lower()
        if err:
            return f"{fire.name}: {err[:500]}"
        if status in {"failed", "error"}:
            stage = str(payload.get("stage") or payload.get("stage_label") or "unknown").strip()
            return f"{fire.name}: analysis {status} at stage {stage}"
    return ""


def _format_reference_preflight_failure(
    tool_fires: list[ToolFire],
    *,
    local_path: str = "",
    channel_context: bool = False,
) -> str:
    """Deterministic failure answer when backend reference analysis did not complete."""
    err = _reference_preflight_error_summary(tool_fires)
    # Detect YouTube URL / channel analysis (not an upload attachment).
    url_hits: list[str] = []
    channel_hits: list[str] = []
    for fire in tool_fires or []:
        try:
            payload = json.loads(fire.result or "{}")
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            continue
        if fire.name == "fetch_competitor_channel_videos":
            channel_hits.append(str(payload.get("channel_url") or payload.get("channel_label") or "channel"))
            rows = payload.get("video_rows") if isinstance(payload.get("video_rows"), list) else []
            for row in rows[:5]:
                if isinstance(row, dict):
                    title = str(row.get("title") or row.get("video_id") or "").strip()
                    views = row.get("view_count") or row.get("views")
                    if title:
                        url_hits.append(f"- {title}" + (f" ({views} views)" if views else ""))
        for key in ("url", "webpage_url", "watch_url"):
            val = str(payload.get(key) or "").strip()
            if "youtube.com" in val or "youtu.be" in val:
                url_hits.append(val)
    is_url_or_channel = bool(channel_context or channel_hits or any("youtube" in u for u in url_hits))

    if is_url_or_channel:
        lines = [
            "I started deep analysis on the public channel videos, but the download/transcript/visual pipeline "
            "did not finish a full breakdown before this turn ended.",
        ]
        if channel_hits:
            lines.append(f"Channel: {channel_hits[0]}")
        if url_hits:
            lines.append("What Studio could see from the channel listing:")
            lines.extend(url_hits[:6])
        if err:
            lines.append(f"Backend detail: {err}")
        else:
            lines.append(
                "Backend detail: analysis jobs started, but polling never reached status=complete "
                "(download or STT took longer than the turn budget)."
            )
        lines.extend([
            "",
            "This is **not** an upload problem — you asked about public YouTube videos, not a file attachment.",
            "",
            "Next options (pick one):",
            "1. Say **retry deep analysis on the top video only** (fastest full breakdown).",
            "2. Paste **1–3 direct watch URLs** so we skip channel-page discovery.",
            "3. I can still coach packaging from the channel titles/views above without a full transcript.",
            "",
            "I will **not** start shortform production until you explicitly ask to render.",
        ])
        return "\n".join(lines)

    lines = [
        "Studio attempted `analyze_reference_video` on your upload, but the backend did not return a complete analysis payload.",
    ]
    if local_path:
        lines.append(f"Upload path Studio resolved: `{local_path}`")
    if err:
        lines.append(f"Exact backend error: {err}")
    else:
        lines.append(
            "Exact backend error: analysis started but polling never reached `status=complete` before this turn ended."
        )
    lines.extend([
        "",
        "This is not a missing-tool problem — `analyze_reference_video` exists server-side. "
        "The failure is in file resolution, ffmpeg/keyframe extraction, or the analysis worker on the backend.",
        "",
        "Next step: re-upload the video once (to refresh the attachment path), then send "
        "`yes, analyze the uploaded video` again. If it fails twice with the same error, "
        "escalate with the exact backend error line above.",
    ])
    return "\n".join(lines)


def _format_channel_analysis_outcome(
    tool_fires: list[ToolFire],
    *,
    channel_url: str = "",
    channel_label: str = "",
) -> str:
    """User-facing summary after channel preflight (success, partial, or failure)."""
    complete: list[dict[str, Any]] = []
    incomplete: list[dict[str, Any]] = []
    listing_lines: list[str] = []
    for fire in tool_fires or []:
        try:
            payload = json.loads(fire.result or "{}")
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        if fire.name == "fetch_competitor_channel_videos":
            channel_url = channel_url or str(payload.get("channel_url") or "")
            channel_label = channel_label or str(payload.get("channel_label") or "")
            for row in (payload.get("video_rows") or [])[:6]:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title") or row.get("video_id") or "video").strip()
                views = row.get("view_count") or row.get("views")
                listing_lines.append(f"- {title}" + (f" · {views} views" if views is not None else ""))
        if fire.name in {"poll_render_job", "analyze_reference_video"}:
            if _reference_analysis_actionable(payload):
                complete.append(payload)
            elif str(payload.get("status") or "").lower() in {"failed", "error", "incomplete", "running"}:
                incomplete.append(payload)

    if complete:
        chunks = [_format_reference_analysis_findings(p) for p in complete[:3]]
        header = (
            f"Deep analysis finished for {len(complete)} video(s) from "
            f"{channel_label or channel_url or 'the channel'}."
        )
        body = "\n\n---\n\n".join(chunks)
        tail = (
            "\n\nI can map hooks/pacing from this into a short script next — "
            "only if you want me to **write or render** something (say so explicitly)."
        )
        return f"{header}\n\n{body}{tail}"

    # Partial / timeout path — still useful for the creator
    lines = [
        f"I pulled the public uploads for **{channel_label or channel_url or 'your channel'}**, "
        "but the full download + transcript + visual pass did not finish for every video this turn.",
    ]
    if listing_lines:
        lines.append("")
        lines.append("Channel listing Studio fetched:")
        lines.extend(listing_lines)
    if incomplete:
        stages = []
        for p in incomplete[:3]:
            stages.append(
                f"- {str(p.get('title') or p.get('job_id') or 'job')}: "
                f"{str(p.get('stage') or p.get('status') or 'incomplete')}"
                + (f" — {p.get('error')}" if p.get("error") else "")
            )
        lines.append("")
        lines.append("Deep-analysis status:")
        lines.extend(stages)
    lines.extend([
        "",
        "Next (fastest): say **retry deep analysis on the top video only**, or paste one watch URL. "
        "I will not start production until you ask to render.",
    ])
    return "\n".join(lines)


def _assistant_needs_turn_synthesis(assistant_text: str, *, turn_plan: Any) -> bool:
    if not getattr(turn_plan, "has_execution", False):
        return False
    text = str(assistant_text or "").strip()
    if not text:
        return True
    low = text.lower()
    if _assistant_stalled_on_reference_analysis(text) or _assistant_promised_research_narration(text):
        return True
    if _assistant_stalled_on_channel_data(text):
        return True
    if any(
        phrase in low
        for phrase in (
            "let me poll",
            "still processing keyframes",
            "still extracting keyframes",
            "let me wait for the full analysis",
            "let me continue polling",
            "analysis is running",
        )
    ):
        return True
    if "i don't have a direct market data tool" in low or "no direct market data tool" in low:
        return True
    if _assistant_denies_public_research_tool(assistant_text) or _assistant_denies_public_trending_capability(assistant_text):
        return True
    if _assistant_denies_upload_analysis_capability(assistant_text):
        return True
    if any(
        phrase in low
        for phrase in (
            "flagged this as a conversational planning turn",
            "conversational planning turn",
            "different approach",
            "what's the hook",
            "what is the hook",
            "visual style",
            "paste or type those three things",
        )
    ):
        return True
    return False


def _latest_public_search_query(tool_fires: list[ToolFire]) -> str:
    for fire in reversed(tool_fires or []):
        if str(fire.name or "") not in {"get_public_search_trends", "search_youtube_public"}:
            continue
        query = str((fire.args or {}).get("query") or "").strip()
        if query:
            return query
    return ""


def _public_search_hydrated_count(tool_fires: list[ToolFire]) -> int:
    count = 0
    for fire in tool_fires or []:
        if str(fire.name or "") not in {
            "get_public_search_trends",
            "search_youtube_public",
            "recommend_video_topics",
        }:
            continue
        try:
            payload = json.loads(fire.result or "{}")
        except Exception:
            payload = {}
        if not isinstance(payload, dict) or payload.get("error"):
            continue
        videos = payload.get("videos") or payload.get("trending_sample")
        if isinstance(videos, list):
            hydrated = [
                row for row in videos
                if isinstance(row, dict)
                and str(row.get("evidence_level") or "") == "hydrated_video_stats"
            ]
            count = max(count, len(hydrated))
        summary = payload.get("evidence_summary")
        if isinstance(summary, dict):
            count = max(count, int(summary.get("hydrated_rows") or summary.get("supported_rows") or 0))
    return count


def _synthesize_turn_from_evidence(
    tool_fires: list[ToolFire],
    *,
    user_text: str,
    turn_plan: Any,
    session: dict[str, Any],
    active_registry: str,
    active_channel_id: str,
    has_reference_upload: bool = False,
    messages: list[dict[str, Any]] | None = None,
) -> str:
    """Mandatory combined synthesis when the model stalls or returns empty after planned tool work.

    Prefer Grok-class conversational research reply when public demand tools ran.
    """
    active_label = _ideation_active_label(
        session,
        user_text,
        active_registry=active_registry,
        active_channel_id=active_channel_id,
    )
    ref_payload = _latest_complete_reference_analysis(tool_fires=tool_fires, messages=messages)
    ref_depth = _reference_analysis_depth(ref_payload) if ref_payload else "missing"
    wants_public = bool(getattr(turn_plan, "public_youtube_demand", False) or _has_public_demand_tool(tool_fires))
    wants_channel = bool(getattr(turn_plan, "channel_analytics", False))
    public_rows = _public_search_hydrated_count(tool_fires)
    search_query = _latest_public_search_query(tool_fires)

    # Prefer conversational Live Demand synthesis over "approved research turn" form.
    if wants_public and public_rows > 0 and not (
        getattr(turn_plan, "niche_from_reference", False)
        and ref_depth == "pacing_only"
    ):
        try:
            from studio_agent.conversation import deterministic_conversational_research_reply
            from studio_agent.live_demand import extract_niche_hint

            evidence = _grounded_research_summary_from_tools(
                tool_fires,
                active_label=active_label,
                user_text=user_text,
                include_channel=bool(wants_channel and _has_channel_analytics_tool(tool_fires)),
                search_query=search_query,
                reference_payload=ref_payload,
            )
            ref_findings = (
                _format_reference_analysis_findings(ref_payload) if ref_payload else ""
            )
            from studio_agent.conversation import strip_robot_research_artifacts

            return strip_robot_research_artifacts(
                deterministic_conversational_research_reply(
                    user_text=user_text,
                    evidence=evidence,
                    reference_findings=ref_findings,
                    niche_hint=extract_niche_hint(user_text, session) or search_query or active_label,
                )
            )
        except Exception:
            pass
    reference_blocked = bool(
        getattr(turn_plan, "reference_analysis", False)
        and ref_depth == "pacing_only"
    )
    from studio_agent.turn_plan import is_garbage_public_search_query, is_unusable_public_search_query

    # Prefer resolved query from tools; if tools used soup, re-resolve from session.
    if not search_query or is_unusable_public_search_query(search_query) or is_garbage_public_search_query(search_query):
        try:
            from studio_agent.live_demand import resolve_demand_search_query

            search_query = resolve_demand_search_query(
                user_text,
                session,
                active_label=active_label,
                registry_key=active_registry,
            )
        except Exception:
            pass

    public_blocked = bool(
        wants_public
        and (
            (
                getattr(turn_plan, "niche_from_reference", False)
                and reference_blocked
            )
            # Only block when we truly have no usable query AND no hydrated rows.
            or (
                (is_unusable_public_search_query(search_query) or is_garbage_public_search_query(search_query))
                and public_rows <= 0
            )
        )
    )

    if reference_blocked and public_blocked:
        sections: list[str] = [
            "Reference analysis did not reach a usable topic yet, so public YouTube search was held for this turn.",
        ]
        if ref_payload:
            sections.append(_format_reference_analysis_findings(ref_payload))
        elif has_reference_upload:
            sections.append(
                "Reference analysis was requested, but no completed reference payload was available in this turn. "
                "Retry once or ask me to re-run analyze_reference_video."
            )
        sections.append(
            "Public YouTube demand was not queried against chat text. "
            "After vision/transcript/story stages succeed, Studio will derive the niche from the upload automatically."
        )
        sections.append(
            "Next: send `try again and watch the video` so deep analysis can retry. "
            "If stage errors repeat, escalate with the exact stage error lines above."
        )
        return "\n\n".join(section for section in sections if str(section).strip())

    if reference_blocked and not wants_public:
        sections = ["Reference analysis did not reach a usable topic yet."]
        if ref_payload:
            sections.append(_format_reference_analysis_findings(ref_payload))
        sections.append(
            "Next: send `try again and watch the video` so vision/transcript/story can retry on the same upload."
        )
        return "\n\n".join(section for section in sections if str(section).strip())

    sections: list[str] = []
    if ref_depth in {"full", "partial"} and wants_public and public_rows > 0:
        sections.append("Here's the combined read from your approved research turn.")
    elif wants_public and public_rows > 0:
        sections.append("Here's the public YouTube demand read from your approved research turn.")
    elif ref_depth in {"full", "partial"}:
        sections.append("Here's the reference analysis read from your approved research turn.")
    else:
        sections.append("Here's what Studio could verify from your approved research turn.")

    if ref_payload:
        sections.append(_format_reference_analysis_findings(ref_payload))
    elif has_reference_upload and getattr(turn_plan, "reference_analysis", False):
        sections.append(
            "Reference analysis was requested, but no completed reference payload was available in this turn. "
            "Retry once or ask me to re-run analyze_reference_video."
        )

    if wants_public and not public_blocked:
        if public_rows > 0:
            sections.append(
                "Google Trends and third-party keyword-volume APIs are not wired in yet. "
                "The market read below uses hydrated public YouTube demand from Studio's search tools."
            )
        sections.append(
            _grounded_research_summary_from_tools(
                tool_fires,
                active_label=active_label,
                user_text=user_text,
                include_channel=wants_channel,
                search_query=search_query,
            )
        )
    elif wants_public:
        niche_guess = ""
        try:
            from studio_agent.live_demand import extract_niche_hint, resolve_demand_search_query

            niche_guess = extract_niche_hint(user_text, session) or resolve_demand_search_query(
                user_text, session, active_label=active_label, registry_key=active_registry
            )
        except Exception:
            niche_guess = search_query or "your niche"
        sections.append(
            "I can pull public YouTube demand for this — I just need a clear niche label to search "
            f"(last attempt: `{str(search_query or niche_guess or 'empty')[:120]}`).\n\n"
            "Reply with one line like **day trading**, **fitness**, or **dark psychology**, "
            "and I'll re-run Live Demand immediately and we can pick a 20-second short from the winners.\n\n"
            "If you already had a niche earlier in this chat, say **same niche** or name it again."
        )

    if wants_channel and _has_channel_analytics_tool(tool_fires):
        sections.append(
            _grounded_channel_status_from_tools(tool_fires, active_label=active_label)
        )

    if ref_depth in {"full", "partial"} and public_rows > 0:
        sections.append(
            "Next: tell me which positioning angle or topic you want to develop. "
            "I will stay in planning mode until you explicitly ask to render or start production."
        )
    elif reference_blocked:
        sections.append(
            "Next: retry deep analysis on the upload before planning from this reference."
        )
    else:
        sections.append(
            "Next: tell me which angle to develop, or retry the missing research step above."
        )
    return "\n\n".join(section for section in sections if str(section).strip())


def _recover_shortform_job_from_session(session: dict[str, Any]) -> str | None:
    """Recover a durable shortform job when active_jobs was cleared after scene review."""
    if bool(session.get("skip_job_recovery")):
        return None
    blocked = {
        str(job_id).strip()
        for job_id in (session.get("blocked_job_ids") or [])
        if str(job_id).strip()
    }
    wanted_user = str(session.get("user_id") or "").strip()
    try:
        from studio_agent.jobs import ROOT as _JOBS_ROOT, SKELETON_OUTPUT as _SKELETON_OUTPUT

        root = (_JOBS_ROOT / _SKELETON_OUTPUT).resolve()
        if root.is_dir():
            candidates: list[tuple[float, str]] = []
            for spec_path in root.glob("*/job_spec.json"):
                ws = spec_path.parent
                try:
                    spec = json.loads(spec_path.read_text(encoding="utf-8"))
                except Exception:
                    continue
                if wanted_user and str(spec.get("user_id") or "").strip() != wanted_user:
                    continue
                progress_path = ws / "progress.json"
                progress = {}
                if progress_path.is_file():
                    try:
                        progress = json.loads(progress_path.read_text(encoding="utf-8"))
                    except Exception:
                        progress = {}
                stage = str(progress.get("stage") or "").strip().lower()
                if stage not in {"awaiting_scene_review", "awaiting_approval", "scenes_approved", "animate"}:
                    continue
                newest = max(
                    (p.stat().st_mtime for p in [spec_path, progress_path, ws / "scenes.json"] if p.is_file()),
                    default=ws.stat().st_mtime,
                )
                if ws.name in blocked:
                    continue
                candidates.append((newest, ws.name))
            if candidates:
                candidates.sort(reverse=True)
                return candidates[0][1]
    except Exception:
        pass

    messages = list(session.get("messages") or [])
    for msg in reversed(messages[-40:]):
        text = str(msg.get("content") or "")
        if '"job_id"' not in text and "job_id" not in text.lower():
            continue
        for match in re.finditer(r'"job_id"\s*:\s*"([A-Za-z0-9_-]{6,48})"', text):
            job_id = str(match.group(1) or "").strip()
            if not job_id:
                continue
            try:
                snap = get_job_snapshot(job_id, "shortform")
            except Exception:
                continue
            status = str(snap.get("status") or "").lower()
            if status in {
                "awaiting_scene_review",
                "awaiting_approval",
                "running",
                "scenes_approved",
                "animate",
            }:
                if job_id not in blocked:
                    return job_id
    return None


def _extract_scene_fix_instruction(user_text: str) -> str:
    text = str(user_text or "").strip()
    if not text:
        return text
    stripped = re.sub(
        r"(?is)^please edit scene \d+ in this (?:short|long)[- ]form video\.\s*"
        r"keep the same character identity, then change only what i describe\.\s*",
        "",
        text,
    ).strip()
    stripped = re.sub(
        r"(?is)^regenerate scene \d+ in this (?:short|long)[- ]form video.*?\.\s*",
        "",
        stripped,
    ).strip()
    return stripped or text


def _is_reply_to_scene_still_edit(user_text: str, reply_to: dict | None = None) -> bool:
    if _is_scene_regenerate_request(user_text):
        return True
    if _is_animation_repair_request(user_text):
        return True
    if not reply_to:
        return _is_scene_review_fix_request(user_text)
    if _is_scene_review_fix_request(user_text):
        return True
    return _is_manual_visual_edit_request(user_text, reply_to)


def _normalize_user_intent_text(user_text: str) -> str:
    text = store.strip_agent_mode_prefix(user_text)
    try:
        from studio_agent.conversation import normalize_spoken_request

        return normalize_spoken_request(text)
    except Exception:
        return text


def _wants_expand_visual_proof_short(user_text: str) -> bool:
    if _wants_bulk_scene_ship_request(user_text):
        return False
    if store.is_expand_short_request(user_text):
        return True
    # Extra natural-language coverage when Scene 1 is already the active proof job.
    low = str(user_text or "").strip().lower()
    if not low:
        return False
    if re.search(
        r"\b(?:other|rest|remaining|more)\s+(?:\d+\s+)?scenes?\b|"
        r"\bmake\s+(?:the\s+)?other\b|"
        r"\banimate\s+them\b",
        low,
    ) and re.search(r"\b(?:make|build|generate|finish|continue|go\s+ahead|animate)\b", low):
        return True
    if re.search(r"\b(?:i\s+)?(?:like|love|approve[sd]?)\s+scene\s*(?:1|one|first)\b", low) and re.search(
        r"\b(?:make|build|generate|finish|continue|go\s+ahead|animate|other|rest)\b",
        low,
    ):
        return True
    return False


def _session_has_expandable_proof_job(session: dict[str, Any], *, reply_to: dict | None = None) -> bool:
    job_id = _find_expandable_shortform_job(session, reply_to=reply_to) or ""
    if not job_id:
        return False
    try:
        from studio_agent.tools import _expandable_proof_job, _shortform_workspace

        ws = _shortform_workspace(job_id)
        spec_path = ws / "job_spec.json"
        if not spec_path.is_file():
            return False
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
        return bool(isinstance(spec, dict) and _expandable_proof_job(spec, ws))
    except Exception:
        return True  # job id found on session — treat as expandable if unsure


def _find_expandable_shortform_job(
    session: dict[str, Any],
    *,
    reply_to: dict | None = None,
) -> str | None:
    """Locate the one-scene proof job the user wants to expand, even after a context fork."""
    blocked = {
        str(job_id).strip()
        for job_id in (session.get("blocked_job_ids") or [])
        if str(job_id).strip()
    }
    if reply_to:
        job_id = str(reply_to.get("job_id") or "").strip()
        if job_id:
            return job_id

    for job in reversed(list(session.get("active_jobs") or [])):
        job_id = str(job.get("job_id") or "").strip()
        if job_id and str(job.get("kind") or "shortform") == "shortform":
            return job_id

    for msg in reversed(list(session.get("messages") or [])[-40:]):
        if not isinstance(msg, dict):
            continue
        deliverable = msg.get("jobDeliverable")
        if isinstance(deliverable, dict):
            job_id = str(deliverable.get("job_id") or "").strip()
            if job_id:
                return job_id
        text = str(msg.get("content") or "")
        match = re.search(r'"job_id"\s*:\s*"([A-Za-z0-9_-]{6,48})"', text)
        if match:
            job_id = str(match.group(1) or "").strip()
            if job_id and job_id not in blocked:
                return job_id

    wanted_user = str(session.get("user_id") or "").strip()
    try:
        from studio_agent.jobs import ROOT as _JOBS_ROOT, SKELETON_OUTPUT as _SKELETON_OUTPUT
        from studio_agent.tools import _expandable_proof_job

        root = (_JOBS_ROOT / _SKELETON_OUTPUT).resolve()
        if root.is_dir():
            candidates: list[tuple[float, str]] = []
            for spec_path in root.glob("*/job_spec.json"):
                ws = spec_path.parent
                job_id = ws.name
                if job_id in blocked:
                    continue
                try:
                    spec = json.loads(spec_path.read_text(encoding="utf-8"))
                except Exception:
                    continue
                if wanted_user and str(spec.get("user_id") or "").strip() != wanted_user:
                    continue
                if not _expandable_proof_job(spec, ws):
                    continue
                newest = max(
                    (p.stat().st_mtime for p in [spec_path, ws / "scenes.json", ws / "progress.json"] if p.is_file()),
                    default=ws.stat().st_mtime,
                )
                candidates.append((newest, job_id))
            if candidates:
                candidates.sort(reverse=True)
                return candidates[0][1]
    except Exception:
        pass
    return None


def _parse_target_duration_seconds(user_text: str) -> int | None:
    low = str(user_text or "").lower()
    for pattern in (
        r"\b(?:only\s+)?(\d{1,3})\s*(?:sec(?:ond)?s?)\s*(?:long)?\b",
        r"\b(\d{1,3})\s*-\s*second\b",
    ):
        match = re.search(pattern, low)
        if match:
            return max(10, min(120, int(match.group(1))))
    match = re.search(r"\b(\d{1,2})\s*(?:h|hr|hrs|hour|hours)\b", low)
    if match:
        return max(3600, min(12 * 3600, int(match.group(1)) * 3600))
    return None


def _scene_count_for_duration(duration_seconds: int, *, seconds_per_scene: float = 5.0) -> int:
    per = max(3.0, float(seconds_per_scene or 5.0))
    return max(2, min(60, int(round(float(duration_seconds) / per))))


def _reference_production_grammar(payload: dict[str, Any]) -> str:
    """Compress a watched reference into transferable direction, never its subject/brand."""
    pacing = payload.get("pacing") if isinstance(payload.get("pacing"), dict) else {}
    visual = payload.get("visual_summary") if isinstance(payload.get("visual_summary"), dict) else {}
    story = payload.get("storytelling") if isinstance(payload.get("storytelling"), dict) else {}
    parts: list[str] = [
        "REFERENCE GRAMMAR ONLY: preserve this video's approved script, skeleton identity, psychology niche, and branding; "
        "do not copy the reference's people, subject, claims, names, locations, or branding."
    ]
    duration = pacing.get("duration_sec")
    cuts = pacing.get("cut_count")
    avg = pacing.get("avg_shot_sec")
    if any(value not in (None, "") for value in (duration, cuts, avg)):
        parts.append(f"Observed edit rhythm: duration={duration}s, cuts={cuts}, average shot={avg}s.")
    visual_text = str(visual.get("summary") or "").strip()
    if visual_text:
        parts.append("Transferable visual language: " + visual_text[:700])
    for key, label in (
        ("hook", "Hook construction"),
        ("pacing_notes", "Pacing"),
        ("cta_placement", "CTA timing"),
        ("summary", "Story/emotional progression"),
    ):
        value = story.get(key)
        if isinstance(value, list):
            value = "; ".join(str(item) for item in value[:4])
        text = str(value or "").strip()
        if text:
            parts.append(f"{label}: {text[:500]}")
    return " ".join(parts)[:2200]


def _is_manual_visual_edit_request(user_text: str, reply_to: dict | None = None) -> bool:
    if not reply_to:
        return False
    if _wants_expand_visual_proof_short(user_text):
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


_SCENE_FIX_TERMS = (
    "fix",
    "change",
    "edit",
    "add",
    "remove",
    "replace",
    "correct",
    "repair",
    "update",
    "artifact",
    "artifacts",
)
_SCENE_FIX_VISUAL_TERMS = (
    "skeleton",
    "eye",
    "eyes",
    "eyeball",
    "eyeballs",
    "socket",
    "sockets",
    "hand",
    "hands",
    "finger",
    "fingers",
    "skull",
    "glass",
    "shell",
    "bone",
    "bones",
    "background",
    "room",
    "lighting",
    "light",
    "glow",
    "scene",
    "still",
    "frame",
    "image",
    "character",
    "outfit",
    "clothes",
    "wardrobe",
    "pose",
    "prop",
)
_SCENE_FIX_ARTIFACT_TERMS = (
    "eyeball",
    "eyeballs",
    "empty socket",
    "empty eye",
    "hollow eye",
    "missing eye",
    "no eye",
    "artifact",
    "bell jar",
    "glass dome",
    "warped hand",
    "extra finger",
    "duplicate body",
)


def _wants_bulk_scene_ship_request(user_text: str) -> bool:
    low = str(user_text or "").strip().lower()
    if not low:
        return False
    if any(term in low for term in _SCENE_FIX_TERMS):
        return False
    if any(term in low for term in _SCENE_FIX_ARTIFACT_TERMS):
        return False
    return store.is_bulk_scene_ship_request(low)


def _wants_animate_all_in_ship(user_text: str) -> bool:
    low = str(user_text or "").strip().lower()
    if any(term in low for term in ("no animation", "still only", "ken burns", "without animat", "animate false")):
        return False
    return any(
        term in low
        for term in ("animate", "animation", "i2v", "motion", "bring them to life", "animate them")
    )


def _shortform_bulk_ship_plan(snapshot: dict[str, Any], *, animate_all: bool) -> list[tuple[str, dict[str, Any]]]:
    status = str(snapshot.get("status") or snapshot.get("phase") or snapshot.get("stage") or "").strip().lower()
    job_id = str(snapshot.get("job_id") or "").strip()
    if not job_id or status in {"running", "restarting", "starting", "complete", "failed", "error", "cancelled"}:
        return []
    if status not in {"awaiting_scene_review", "awaiting_approval", "scenes_approved"}:
        return []
    plan: list[tuple[str, dict[str, Any]]] = [
        ("set_production_scenes_animate", {"job_id": job_id, "animate": bool(animate_all)}),
    ]
    if animate_all:
        plan.append(("animate_production_scenes", {"job_id": job_id}))
    plan.append(("finalize_production", {"job_id": job_id}))
    return plan


async def _apply_bulk_scene_ship(
    *,
    session: dict[str, Any],
    user_id: str,
    user_text: str,
    content_format: str,
    emit: EventEmitter | None,
    membership_plan: str,
    billing_profile: dict[str, Any] | None,
) -> dict[str, Any] | None:
    target = _recover_poll_target(session)
    if not target:
        return None
    job_id, kind = target
    if kind != "shortform":
        return None

    snapshot = get_job_snapshot(job_id, "shortform")
    animate_all = _wants_animate_all_in_ship(user_text)
    plan = _shortform_bulk_ship_plan(snapshot, animate_all=animate_all)
    if not plan:
        return None

    sid = str(session.get("session_id") or "")
    messages = list((store.get_session(sid) or session).get("messages") or [])
    active_jobs = merge_active_jobs(
        list(session.get("active_jobs") or []),
        [{
            "job_id": job_id,
            "kind": "shortform",
            "title": str(snapshot.get("title") or "Short-form video"),
            "started_at": time.time(),
        }],
    )
    await _fire_event(emit, "active_jobs", jobs=active_jobs)

    profile = billing_profile or {}
    last_result: dict[str, Any] = {}
    async with studio_agent_slot(
        user_id=user_id,
        plan=membership_plan,
        operation="bulk_scene_ship",
        unlimited=bool(profile.get("unlimited")),
    ):
        for tool_name, args in plan:
            await _fire_event(
                emit,
                "tool_start",
                tool=tool_name,
                round=0,
                awaiting_approval=False,
                deterministic_bulk_ship=True,
            )
            try:
                result = execute_tool_logged(
                    tool_name,
                    args,
                    user_id=user_id,
                    content_format=content_format,
                    session_id=sid,
                )
                messages.append(_tool_observation_message(tool_name, result))
                try:
                    parsed = json.loads(result or "{}")
                except Exception:
                    parsed = {}
                last_result = parsed if isinstance(parsed, dict) else {}
                if _production_result_failed(last_result):
                    await _fire_event(emit, "tool_end", tool=tool_name, status="error", error=str(last_result.get("error") or "")[:160])
                    break
                await _fire_event(emit, "tool_end", tool=tool_name, status="ok")
                active_jobs = merge_active_jobs(active_jobs, extract_jobs_from_tool(tool_name, result))
                await _fire_event(emit, "active_jobs", jobs=active_jobs)
            except Exception as exc:
                last_result = {"error": str(exc), "status": "failed"}
                await _fire_event(emit, "tool_end", tool=tool_name, status="error", error=str(exc)[:160])
                break

    final_snapshot = get_job_snapshot(job_id, "shortform")
    if str(final_snapshot.get("status") or "").lower() == "complete":
        active_jobs = [j for j in active_jobs if str(j.get("job_id") or "") != job_id]
    if _production_result_failed(last_result):
        assistant_text = (
            f"I tried to approve and ship every scene, but the next step failed: "
            f"{str(last_result.get('error') or last_result)[:500]}"
        )
    elif _production_result_complete(last_result) or final_snapshot.get("status") == "complete":
        if animate_all:
            assistant_text = (
                "I approved every scene, animated them all, and finished the MP4. "
                "The production card has the download and upload package."
            )
        else:
            assistant_text = (
                "I approved every scene and finished the MP4. "
                "The production card has the download and upload package."
            )
    else:
        assistant_text = _format_polled_job_status(json.dumps(final_snapshot))
    messages.append({"role": "assistant", "content": assistant_text})
    store.update_session(sid, messages=messages, active_jobs=active_jobs)
    await _fire_event(emit, "active_jobs", jobs=active_jobs)
    try:
        await _fire_event(emit, "job_snapshot", snapshot=final_snapshot)
    except Exception:
        pass
    return {
        "session_id": sid,
        "assistant_message": assistant_text,
        "pending_actions": [],
        "active_jobs": active_jobs,
        "approval_mode": str(session.get("approval_mode") or "confirm"),
        "reasoning_depth": str(session.get("reasoning_depth") or "balanced"),
        "usage": {},
        "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
    }


def _is_scene_approval_only_request(user_text: str) -> bool:
    low = str(user_text or "").strip().lower()
    if not low:
        return False
    if any(term in low for term in _SCENE_FIX_TERMS):
        return False
    if any(
        phrase in low
        for phrase in ("doesn't have", "does not have", "not have", "missing", "without", "no eye")
    ):
        return False
    approval_terms = (
        "approve",
        "looks good",
        "look good",
        "looks great",
        "perfect",
        "ship it",
        "finalize",
        "animate it",
        "animate this",
        "animate the",
        "go ahead and animate",
        "ready to animate",
        "download",
        "export",
    )
    return any(term in low for term in approval_terms)


def _is_scene_review_fix_request(user_text: str) -> bool:
    low = str(user_text or "").strip().lower()
    if not low:
        return False
    if re.fullmatch(r"(?:please\s+)?(?:just\s+)?(?:fix|repair)(?:\s+it|\s+this)?[.!?]*", low):
        return True
    if _wants_expand_visual_proof_short(low):
        return False
    if _wants_production_execution(low):
        return False
    if _is_continue_production_request(low):
        return False
    if _is_production_diagnostic_turn(low):
        return False
    if _is_scene_approval_only_request(low):
        return False
    if any(term in low for term in _SCENE_FIX_ARTIFACT_TERMS):
        return True
    has_fix = any(term in low for term in _SCENE_FIX_TERMS) or any(
        phrase in low
        for phrase in ("doesn't have", "does not have", "not have", "missing", "without")
    )
    has_visual = any(term in low for term in _SCENE_FIX_VISUAL_TERMS)
    if has_fix and has_visual:
        return True
    return bool(has_fix and re.search(r"\bscene\s+\d+\b", low))


def _parse_scene_index_from_text(user_text: str, total_scenes: int) -> int:
    low = str(user_text or "").lower()
    match = re.search(r"\bscene\s+(\d+)\b", low)
    if match:
        return max(0, int(match.group(1)) - 1)
    word_match = re.search(r"\bscene\s+(one|two|three|four|five|six|seven|eight|nine|ten)\b", low)
    if word_match:
        words = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10}
        return max(0, min(max(total_scenes, 1), words[word_match.group(1)]) - 1)
    if re.search(r"\b(?:first|opening|intro)\s+scene\b", low):
        return 0
    if re.search(r"\blast\s+scene\b", low) and total_scenes > 0:
        return max(0, total_scenes - 1)
    return 0


def _parse_scene_indices_from_text(user_text: str, total_scenes: int) -> list[int]:
    """Parse natural selected batches: 'scenes 1, 3 and 5', '2-4', or '2 through 4'."""
    low = str(user_text or "").lower()
    # Normalize spoken dictation range words before limiting the capture to
    # numeric list syntax.  Without this, "Scenes 2 through 6" was truncated
    # at 2 and could silently audit the wrong scope.
    low = re.sub(r"\b(\d+)\s+(?:through|thru|to)\s+(\d+)\b", r"\1-\2", low)
    match = re.search(r"\bscenes?\s+([\d\s,;&andto\-]+)", low)
    if not match:
        return []
    raw = match.group(1).replace("and", ",").replace("to", "-")
    picked: set[int] = set()
    for start, end in re.findall(r"(\d+)\s*-\s*(\d+)", raw):
        lo, hi = sorted((int(start), int(end)))
        picked.update(range(max(1, lo), min(total_scenes, hi) + 1))
    raw = re.sub(r"\d+\s*-\s*\d+", "", raw)
    picked.update(int(value) for value in re.findall(r"\d+", raw) if 1 <= int(value) <= total_scenes)
    return [value - 1 for value in sorted(picked)]


_SCENE_NUMBER_WORDS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
}


def _bulk_artifact_audit_scene_indices(user_text: str, total_scenes: int) -> list[int]:
    """Resolve selected scenes while honoring natural exclusions."""
    low = str(user_text or "").lower()
    selected = _parse_scene_indices_from_text(low, total_scenes)
    if not selected and re.search(r"\b(?:all|every|each|rest|remaining)\b", low):
        selected = list(range(max(0, total_scenes)))
    excluded: set[int] = set()
    for match in re.finditer(
        r"\b(?:excluding|exclude|except|other than|leave out|skip)\s+(?:for\s+)?scene\s+"
        r"(\d+|one|two|three|four|five|six|seven|eight|nine|ten)\b",
        low,
    ):
        raw = match.group(1)
        number = int(raw) if raw.isdigit() else _SCENE_NUMBER_WORDS.get(raw, 0)
        if 1 <= number <= total_scenes:
            excluded.add(number - 1)
    return [index for index in selected if index not in excluded]


def _is_bulk_artifact_audit_request(user_text: str) -> bool:
    low = str(user_text or "").lower()
    has_scope = bool(re.search(r"\b(?:all|every|each|rest|remaining|scenes\s+\d|five scenes|six scenes)\b", low))
    has_audit = any(term in low for term in ("check", "audit", "inspect", "review", "scan"))
    has_defect = any(term in low for term in ("artifact", "morph", "flicker", "warping", "drift"))
    has_story_defect = any(term in low for term in (
        "correspond", "match the prompt", "match their prompt", "match the narration",
        "same still", "same scene", "same setting", "same background", "too similar",
        "repetitive", "generic", "bland", "wrong scene", "wrong setting",
    ))
    has_repair = any(term in low for term in ("fix", "repair", "correct", "redo"))
    return has_scope and has_repair and ((has_audit and (has_defect or has_story_defect)) or has_story_defect)


def _may_be_scene_quality_request(user_text: str) -> bool:
    """Cheap gate before asking the intent model; it is not the classifier."""
    low = str(user_text or "").lower()
    visual_subject = any(term in low for term in (
        "scene", "still", "frame", "background", "setting", "shot", "visual", "prompt", "narration",
    ))
    visual_action = any(term in low for term in (
        "fix", "check", "review", "audit", "repair", "redo", "regenerate", "rebuild", "change",
        "different", "same", "repetitive", "match", "correspond", "wrong", "bland",
    ))
    return visual_subject and visual_action


async def _apply_bulk_artifact_audit(
    *,
    session: dict[str, Any],
    user_id: str,
    user_text: str,
    content_format: str,
    emit: EventEmitter | None,
    approval_mode: str,
    reasoning_depth: str,
) -> dict[str, Any] | None:
    # Compatibility fallback only. Broad natural language is owned by the
    # typed command layer; this legacy path requires an explicit repair ask so
    # a question or failed classifier can never fall through into mutation.
    if not _is_bulk_artifact_audit_request(user_text):
        return None
    from studio_agent.command_contract import (
        scene_repair_authorization_evidence,
        scene_repair_block_reason,
    )

    if scene_repair_block_reason(user_text) or not scene_repair_authorization_evidence(user_text):
        return None
    # Prefer the largest same-title multi-scene job, even if an earlier UI
    # sync detached it. Never fall back to a newer one-scene proof merely
    # because it was mentioned later in the transcript.
    candidate_ids: list[str] = []
    for row in session.get("active_jobs") or []:
        if isinstance(row, dict) and str(row.get("kind") or "shortform") == "shortform":
            candidate_ids.append(str(row.get("job_id") or ""))
    for message in reversed(list(session.get("messages") or [])[-120:]):
        for match in re.finditer(r'"job_id"\s*:\s*"([A-Za-z0-9_-]{6,48})"', str(message.get("content") or "")):
            candidate_ids.append(match.group(1))
    expected_title = str((session.get("pending_concept") or {}).get("title") or "") if isinstance(session.get("pending_concept"), dict) else ""
    ranked: list[tuple[int, int, str, dict[str, Any]]] = []
    for candidate in dict.fromkeys(value for value in candidate_ids if value):
        try:
            snap = get_job_snapshot(candidate, "shortform")
        except Exception:
            continue
        status = str(snap.get("status") or "").lower()
        if status in {"failed", "error", "cancelled", "missing"} or snap.get("error"):
            continue
        snap_title = str(snap.get("title") or snap.get("topic") or "")
        title_match = 1 if not expected_title or not snap_title or store._title_overlap_score(expected_title, snap_title) >= 0.75 else 0
        count = int(snap.get("total_scenes") or snap.get("scene_count") or len(snap.get("scenes") or []) or 0)
        ranked.append((title_match, count, candidate, snap))
    if not ranked:
        return _bulk_artifact_audit_error(session, "I couldn't find a multi-scene short to audit. Sync the finished short, then try again.", approval_mode, reasoning_depth)
    ranked.sort(reverse=True)
    _, _, job_id, snapshot = ranked[0]
    total = int(snapshot.get("total_scenes") or snapshot.get("scene_count") or len(snapshot.get("scenes") or []) or 0)
    indices = _bulk_artifact_audit_scene_indices(user_text, total)
    if not indices:
        return _bulk_artifact_audit_error(
            session,
            f"That scope leaves no scenes to audit on the selected {total}-scene job. I did not run or prepare any fallback action.",
            approval_mode,
            reasoning_depth,
        )
    sid = str(session.get("session_id") or "")
    await _fire_event(emit, "tool_start", tool="audit_and_repair_production_scenes", round=0, awaiting_approval=False)
    try:
        result = execute_tool_logged(
            "audit_and_repair_production_scenes",
            {"job_id": job_id, "scene_indices": indices, "reason": str(user_text or "")[:900]},
            user_id=user_id,
            content_format=content_format,
            session_id=sid,
        )
        parsed = json.loads(result or "{}")
        await _fire_event(emit, "tool_end", tool="audit_and_repair_production_scenes", status="ok")
    except Exception as exc:
        parsed = {"ok": False, "error": str(exc), "audited": indices, "failed": indices}
        result = json.dumps(parsed)
        await _fire_event(emit, "tool_end", tool="audit_and_repair_production_scenes", status="error", error=str(exc)[:160])
    human_audited = [int(value) + 1 for value in parsed.get("audited", indices)]
    human_stills = [int(value) + 1 for value in parsed.get("repaired_stills", [])]
    human_clips = [int(value) + 1 for value in parsed.get("repaired_animations", [])]
    human_failed = [int(value) + 1 for value in parsed.get("failed", [])]
    # The complaint itself is Catalyst training data: a shipped render needed
    # a creator-flagged artifact pass. Best-effort; never blocks the reply.
    try:
        from studio_agent import catalyst_learning
        catalyst_learning.record_artifact_complaint(
            user_id,
            session,
            complaint=user_text,
            job_id=job_id,
            scenes_repaired=sorted({*parsed.get("repaired_stills", []), *parsed.get("repaired_animations", [])}),
            scenes_failed=list(parsed.get("failed", [])),
        )
    except Exception:
        pass
    if parsed.get("ok"):
        repairs: list[str] = []
        if human_stills:
            repairs.append(f"rebuilt stills {human_stills}")
        if human_clips:
            repairs.append(f"re-animated clips {human_clips}")
        detail = "; ".join(repairs) if repairs else "all selected scenes passed without changes"
        notes: list[str] = []
        for row in parsed.get("scenes", []):
            if not isinstance(row, dict):
                continue
            number = int(row.get("scene_index", -1)) + 1
            correspondence = row.get("correspondence_qa") if isinstance(row.get("correspondence_qa"), dict) else {}
            finding = str(correspondence.get("summary") or "").strip()
            status = str(row.get("status") or "")
            if finding:
                verb = "rebuilt" if status.startswith("repaired") else "kept"
                notes.append(f"Scene {number}: {verb} — {finding}")
        report = "\n".join(f"• {note}" for note in notes[:12])
        assistant_text = (
            f"I audited Scenes {human_audited} for artifacts, skeleton continuity, animation stability, and whether each scene tells its own narration; {detail}."
            + (f"\n\nWhat I found:\n{report}" if report else "")
        )
    else:
        notes: list[str] = []
        for row in parsed.get("scenes", []):
            if not isinstance(row, dict):
                continue
            number = int(row.get("scene_index", -1)) + 1
            detail = ""
            for key in ("correspondence_qa", "still_qa", "clip_qa"):
                report = row.get(key)
                if isinstance(report, dict) and str(report.get("summary") or "").strip():
                    detail = str(report.get("summary") or "").strip()
                    break
            if not detail:
                detail = str((row.get("repair") or {}).get("error") or row.get("error") or "repair needs review").strip()
            notes.append(f"Scene {number}: {detail}")
        report = "\n".join(f"• {note}" for note in notes[:12])
        assistant_text = (
            f"I audited every requested scene: {human_audited}. "
            f"Scenes {human_failed} need another repair pass; the remaining selected scenes were still audited and were not skipped."
            + (f"\n\nWhat blocked the remaining scenes:\n{report}" if report else "")
        )
    fresh = store.get_session(sid) or session
    messages = list(fresh.get("messages") or [])
    messages.append(_tool_observation_message("audit_and_repair_production_scenes", result))
    messages.append({"role": "assistant", "content": assistant_text})
    active_jobs = [{
        "job_id": job_id,
        "kind": "shortform",
        "title": str(snapshot.get("title") or "Short-form video"),
        "started_at": time.time(),
    }]
    store.update_session(sid, messages=messages, active_jobs=active_jobs)
    await _fire_event(emit, "active_jobs", jobs=active_jobs)
    try:
        await _fire_event(emit, "job_snapshot", snapshot=get_job_snapshot(job_id, "shortform"))
    except Exception:
        pass
    return {
        "session_id": sid,
        "assistant_message": assistant_text,
        "pending_actions": [],
        "active_jobs": active_jobs,
        "approval_mode": approval_mode,
        "reasoning_depth": reasoning_depth,
        "usage": {},
        "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
    }


def _bulk_artifact_audit_error(
    session: dict[str, Any],
    message: str,
    approval_mode: str,
    reasoning_depth: str,
) -> dict[str, Any]:
    sid = str(session.get("session_id") or "")
    messages = list(session.get("messages") or [])
    messages.append({"role": "assistant", "content": message})
    store.update_session(sid, messages=messages, pending_actions=[])
    return {
        "session_id": sid,
        "assistant_message": message,
        "pending_actions": [],
        "active_jobs": list(session.get("active_jobs") or []),
        "approval_mode": approval_mode,
        "reasoning_depth": reasoning_depth,
        "usage": {},
        "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
    }


def _infer_scene_edit_scope(instruction: str) -> str:
    low = str(instruction or "").lower()
    character_terms = (
        "skeleton",
        "eye",
        "eyes",
        "eyeball",
        "eyeballs",
        "socket",
        "hand",
        "hands",
        "finger",
        "skull",
        "outfit",
        "clothes",
        "wardrobe",
        "pose",
        "character",
        "glass",
        "shell",
        "bone",
        "bones",
    )
    background_terms = ("background", "room", "environment", "setting", "lighting", "light", "glow", "backdrop")
    prop_terms = ("prop", "holding", "object", "screen", "phone", "clipboard", "trophy")
    if any(term in low for term in character_terms):
        return "character"
    if any(term in low for term in prop_terms):
        return "props"
    if any(term in low for term in background_terms):
        return "background"
    return "character"


def _is_scene_regenerate_request(user_text: str) -> bool:
    low = str(user_text or "").strip().lower()
    if not low:
        return False
    if re.search(
        r"\b(?:regenerate|reanimate|re-animate|remake|re-make|rerender|re-render|rebuild)\b.*\bscenes?\b",
        low,
    ):
        return True
    if "rebuild the still" in low:
        return True
    return "from scratch" in low and any(term in low for term in ("artifact", "hand", "split", "diptych"))


def _is_animation_artifact_fix_request(user_text: str) -> bool:
    """Bind a reported defect to the clip rather than the approved still."""
    low = str(user_text or "").strip().lower()
    has_animation = any(term in low for term in ("animated", "animation", "clip", "image to video", "image-to-video", "i2v"))
    has_defect = any(term in low for term in (
        "artifact", "morph", "flicker", "melting", "warping", "drift", "turns human", "human skin", "extra limb", "extra finger",
    ))
    has_repair = any(term in low for term in ("fix", "repair", "redo", "reanimate", "re-animate"))
    return has_animation and has_defect and has_repair


def _is_motion_quality_critique_request(user_text: str) -> bool:
    """Creator says the clip is too static / needs stronger performance — not still artifacts."""
    low = str(user_text or "").strip().lower()
    if not low:
        return False
    if re.search(
        r"\b(?:barely|hardly|didn't|did not|doesn't|does not)\s+(?:really\s+|fucking\s+)?"
        r"(?:move|moved|animate|animated|animation)\b",
        low,
    ):
        return True
    if re.search(r"\b(?:too\s+)?(?:static|frozen|still|idle|near[- ]static)\b", low) and any(
        term in low for term in ("animat", "clip", "motion", "video", "i2v", "skeleton", "pose", "gesture")
    ):
        return True
    if re.search(
        r"\b(?:more|stronger|actual|real|readable)\s+(?:motion|movement|animation|pose|gesture|vfx|parallax)\b",
        low,
    ):
        return True
    if re.search(
        r"\b(?:pose\s+change|change\s+(?:the\s+)?pose|weight\s+shift|"
        r"background\s+(?:should\s+)?(?:move|change|parallax)|"
        r"(?:vfx|effects?)\s+(?:should|need|from\s+the\s+skeleton)|"
        r"camera\s+(?:push|move|parallax)|didn't\s+animate|did\s+not\s+animate|"
        r"not\s+(?:really\s+)?animat)\b",
        low,
    ):
        return True
    return False


def _is_animation_repair_request(user_text: str) -> bool:
    return _is_animation_artifact_fix_request(user_text) or _is_motion_quality_critique_request(user_text)


def _is_all_scenes_regenerate_request(user_text: str) -> bool:
    low = str(user_text or "").strip().lower()
    if not _is_scene_regenerate_request(low):
        return False
    return bool(re.search(
        r"\b(?:all|every|each|the\s+six|six)\s+(?:of\s+the\s+)?(?:six|6\s+)?scenes?\b"
        r"|\bscenes?\s+(?:1\s*(?:,|and|through|to|-)\s*6|one\s*(?:,|and|through|to|-)\s*six)\b",
        low,
    ))


def _format_scene_regenerate_reply(result: str, scene_index: int) -> str:
    try:
        data = json.loads(result or "{}")
    except Exception:
        data = {}
    if not isinstance(data, dict):
        return f"I tried to regenerate scene {scene_index + 1}, but the response was unreadable."
    if data.get("error"):
        return (
            f"I tried to regenerate scene {scene_index + 1}, but it failed: "
            f"{str(data.get('error'))[:400]}"
        )
    from studio_agent.conversation import conversational_scene_fix_reply

    audit = data.get("catalyst_audit") if isinstance(data.get("catalyst_audit"), dict) else {}
    issues = ", ".join(audit.get("issue_labels") or []) or "the issue you called out"
    method = str((data.get("scene") or data.get("still") or {}).get("regenerate_method") or "catalyst")
    return conversational_scene_fix_reply(
        scene_index=scene_index,
        ok=True,
        catalyst_note=(
            f"Catalyst kept the channel style and fixed {issues} "
            f"via {method.replace('_', ' ')}"
        ),
    )


def _format_scene_review_fix_reply(result: str, scene_index: int) -> str:
    try:
        data = json.loads(result or "{}")
    except Exception:
        data = {}
    if not isinstance(data, dict):
        return (
            f"I tried to edit scene {scene_index + 1}, but the response was unreadable. "
            "Try again with the same note."
        )
    if data.get("error"):
        return (
            f"I tried to edit scene {scene_index + 1}, but it failed: "
            f"{str(data.get('error'))[:400]}"
        )
    errors = data.get("errors")
    if isinstance(errors, list) and errors:
        return (
            f"I tried to edit scene {scene_index + 1}, but it failed: "
            f"{str(errors[0])[:400]}"
        )
    from studio_agent.conversation import conversational_scene_fix_reply

    return conversational_scene_fix_reply(
        scene_index=scene_index,
        ok=True,
        catalyst_note="applied your note on the still-edit path",
    )


async def _apply_scene_review_fix(
    *,
    session: dict[str, Any],
    user_id: str,
    user_text: str,
    content_format: str,
    emit: EventEmitter | None,
    membership_plan: str,
    billing_profile: dict[str, Any] | None,
    approval_mode: str,
    reasoning_depth: str,
    reply_to: dict | None = None,
) -> dict[str, Any] | None:
    if not (
        _is_reply_to_scene_still_edit(user_text, reply_to)
        or _is_scene_review_fix_request(user_text)
        or _is_scene_regenerate_request(user_text)
        or _is_animation_repair_request(user_text)
    ):
        return None

    job_id = ""
    kind = "shortform"
    if reply_to:
        job_id = str(reply_to.get("job_id") or "").strip()
        kind = str(reply_to.get("kind") or "shortform").strip() or "shortform"
    if not job_id:
        recovered = _recover_shortform_job_from_session(session)
        if recovered:
            job_id, kind = recovered, "shortform"
        else:
            target = _recover_poll_target(session, allow_transcript_fallback=True)
            if not target:
                return None
            job_id, kind = target
    if kind != "shortform":
        return None

    snapshot = get_job_snapshot(job_id, "shortform")
    status = str(snapshot.get("status") or "").lower()
    stage = str(snapshot.get("stage") or "").lower()
    if status not in {"awaiting_scene_review", "awaiting_approval", "scenes_approved"} and stage not in {
        "awaiting_scene_review",
        "awaiting_approval",
        "scenes_approved",
        "awaiting_animation_review",
        "stills_done",
        "review_scenes",
    }:
        return None

    sid = str(session.get("session_id") or "")
    fresh = store.get_session(sid) or session
    messages = list(fresh.get("messages") or [])
    total = int(snapshot.get("total_scenes") or snapshot.get("scene_count") or 1)
    regenerate_all = _is_all_scenes_regenerate_request(user_text)
    selected_indices = _parse_scene_indices_from_text(user_text, max(total, 1))
    regenerate_batch = bool(selected_indices and len(selected_indices) > 1)
    scene_index = _parse_scene_index_from_text(user_text, max(total, 1))
    if reply_to and reply_to.get("scene_index") is not None:
        try:
            scene_index = max(0, int(reply_to.get("scene_index")))
        except Exception:
            pass
    # Prefer a clip-bearing scene when the critique is about motion quality
    # and the user did not name a specific scene / reply target.
    named_scene = bool(re.search(r"\bscene\s+(?:\d+|one|two|three|four|five|six|seven|eight|nine|ten)\b", str(user_text or "").lower()))
    if (
        _is_motion_quality_critique_request(user_text)
        and not named_scene
        and (reply_to is None or reply_to.get("scene_index") is None)
    ):
        for raw_idx, scene in enumerate(list(snapshot.get("scenes") or [])):
            if isinstance(scene, dict) and scene.get("has_clip"):
                scene_index = int(scene.get("index", raw_idx) or raw_idx)
                break
    concise_fix = bool(re.fullmatch(
        r"(?:please\s+)?(?:just\s+)?(?:fix|repair)(?:\s+it|\s+this)?[.!?]*",
        str(user_text or "").strip().lower(),
    ))
    direct_scene_fix = bool(re.fullmatch(
        r"(?:please\s+)?(?:just\s+)?(?:fix|repair)\s+scene\s+(?:\d+|one|two|three|four|five|six|seven|eight|nine|ten)[.!?]*",
        str(user_text or "").strip().lower(),
    ))
    if (concise_fix or direct_scene_fix) and not _is_motion_quality_critique_request(user_text):
        # Bind "fix it" to the latest scene whose persisted QA failed.  This
        # avoids asking the creator to repeat an explanation Studio already
        # printed directly below the card.
        failed = []
        for raw_idx, scene in enumerate(list(snapshot.get("scenes") or [])):
            if not isinstance(scene, dict):
                continue
            qa = scene.get("still_qa") if isinstance(scene.get("still_qa"), dict) else {}
            if str(scene.get("status") or "").lower() == "qa_blocked" or qa.get("pass") is False or qa.get("status") == "fail":
                failed.append(int(scene.get("index", raw_idx) or raw_idx))
        if failed:
            scene_index = failed[-1]
    animation_repair = _is_animation_repair_request(user_text)
    low_fix = str(user_text or "").strip().lower()
    # Still artifact complaints must master-regenerate with a short prompt — never
    # edit the broken still with a long Catalyst fix (that made frames worse).
    from studio_agent.visual_fix_contract import is_visual_artifact_complaint

    still_artifact_fix = bool(
        (
            _is_scene_review_fix_request(user_text)
            or is_visual_artifact_complaint(user_text)
        )
        and is_visual_artifact_complaint(user_text)
        and not _is_motion_quality_critique_request(user_text)
    )
    if still_artifact_fix:
        animation_repair = False
    use_regenerate = (
        concise_fix
        or direct_scene_fix
        or _is_scene_regenerate_request(user_text)
        or still_artifact_fix
    ) and not animation_repair
    scope = _infer_scene_edit_scope(user_text)
    instruction = _extract_scene_fix_instruction(user_text)
    if concise_fix or direct_scene_fix:
        # Empty reason selects the narration-driven redesign path. The
        # regeneration tool reads persisted QA itself; feeding the QA prose as
        # art direction would accidentally ask the image model to depict the
        # error report.
        instruction = ""
    active_jobs = merge_active_jobs(
        list(fresh.get("active_jobs") or []),
        [{
            "job_id": job_id,
            "kind": "shortform",
            "title": str(snapshot.get("title") or "Short-form video"),
            "started_at": time.time(),
        }],
    )
    await _fire_event(emit, "active_jobs", jobs=active_jobs)

    profile = billing_profile or {}
    edit_result = ""
    async with studio_agent_slot(
        user_id=user_id,
        plan=membership_plan,
        operation="scene_review_fix",
        unlimited=bool(profile.get("unlimited")),
    ):
        tool_name = (
            "repair_production_scene_animation" if animation_repair
            else "regenerate_production_scenes" if use_regenerate and (regenerate_all or regenerate_batch)
            else "regenerate_production_scene" if use_regenerate
            else "edit_production_scene_still"
        )
        await _fire_event(
            emit,
            "tool_start",
            tool=tool_name,
            round=0,
            awaiting_approval=False,
            deterministic_scene_fix=True,
        )
        try:
            if animation_repair:
                edit_result = execute_tool_logged(
                    tool_name,
                    {"job_id": job_id, "scene_index": scene_index, "reason": user_text},
                    user_id=user_id,
                    content_format=content_format,
                    session_id=sid,
                )
            elif use_regenerate and (regenerate_all or regenerate_batch):
                edit_result = execute_tool_logged(
                    tool_name,
                    {
                        "job_id": job_id,
                        "scene_indices": list(range(max(total, 1))) if regenerate_all else selected_indices,
                        "reason": instruction or user_text,
                        "animate": True,
                    },
                    user_id=user_id,
                    content_format=content_format,
                    session_id=sid,
                )
            elif use_regenerate:
                edit_result = execute_tool_logged(
                    tool_name,
                    {
                        "job_id": job_id,
                        "scene_index": scene_index,
                        "reason": instruction or user_text,
                    },
                    user_id=user_id,
                    content_format=content_format,
                    session_id=sid,
                )
            else:
                edit_result = execute_tool_logged(
                    tool_name,
                    {
                        "job_id": job_id,
                        "scene_index": scene_index,
                        "instruction": instruction,
                        "scope": scope,
                    },
                    user_id=user_id,
                    content_format=content_format,
                    session_id=sid,
                )
            await _fire_event(emit, "tool_end", tool=tool_name, status="ok")
        except Exception as exc:
            edit_result = json.dumps({"error": str(exc), "job_id": job_id, "scene_index": scene_index})
            await _fire_event(
                emit,
                "tool_end",
                tool=tool_name,
                status="error",
                error=str(exc)[:160],
            )

    tool_name = (
        "repair_production_scene_animation" if animation_repair
        else "regenerate_production_scenes" if use_regenerate and (regenerate_all or regenerate_batch)
        else "regenerate_production_scene" if use_regenerate
        else "edit_production_scene_still"
    )
    messages.append(_tool_observation_message(tool_name, edit_result))
    try:
        parsed_edit = json.loads(edit_result or "{}")
    except Exception:
        parsed_edit = {}
    if animation_repair:
        assistant_text = (
            (
                f"Got it — Scene {scene_index + 1} still stays locked. I rewrote the motion from your note "
                "(pose / skeleton VFX / background energy), re-animated, and ran identity QA. Review the new clip."
                if _is_motion_quality_critique_request(user_text)
                else (
                    f"I preserved the Scene {scene_index + 1} still, replaced only its rejected animation, "
                    "and re-ran sampled-frame identity QA. Review the new clip."
                )
            )
            if parsed_edit.get("ok")
            else (
                f"Scene {scene_index + 1} animation repair did not pass yet: "
                f"{str(parsed_edit.get('error') or parsed_edit.get('failed') or 'review the visible QA result')[:350]}"
            )
        )
    elif use_regenerate and (regenerate_all or regenerate_batch):
        assistant_text = (
            "All scenes were regenerated and animated."
            if parsed_edit.get("ok")
            else "Some scenes need another pass; Studio kept the failed scene list visible in the grid."
        )
    elif use_regenerate:
        assistant_text = _format_scene_regenerate_reply(edit_result, scene_index)
    else:
        assistant_text = _format_scene_review_fix_reply(edit_result, scene_index)
    messages.append({"role": "assistant", "content": assistant_text})
    store.update_session(sid, messages=messages, active_jobs=active_jobs)
    await _fire_event(emit, "active_jobs", jobs=active_jobs)
    try:
        refreshed = get_job_snapshot(job_id, "shortform")
        await _fire_event(emit, "job_snapshot", snapshot=refreshed)
    except Exception:
        pass
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


async def _apply_expand_visual_proof_short(
    *,
    session: dict[str, Any],
    user_id: str,
    user_text: str,
    content_format: str,
    emit: EventEmitter | None,
    membership_plan: str,
    billing_profile: dict[str, Any] | None,
    approval_mode: str,
    reasoning_depth: str,
    reply_to: dict | None = None,
) -> dict[str, Any] | None:
    explicit_expand = _wants_expand_visual_proof_short(user_text)
    sticky = bool(session.get("short_expansion_intake"))
    if not explicit_expand and not sticky:
        return None

    # Scene review / animation critique must never be swallowed by a sticky
    # "how long should the full short be?" intake.
    if not explicit_expand and (
        _is_animation_repair_request(user_text)
        or _is_scene_review_fix_request(user_text)
        or _is_scene_regenerate_request(user_text)
        or _may_be_scene_quality_request(user_text)
        or _is_targeted_scene_animation_request(user_text)
        or _is_reply_to_scene_still_edit(user_text, reply_to)
    ):
        sid = str(session.get("session_id") or "")
        if sticky and sid:
            store.update_session(sid, short_expansion_intake={})
        return None

    job_id = _find_expandable_shortform_job(session, reply_to=reply_to) or ""
    sid = str(session.get("session_id") or "")

    def _messages_for_turn(fresh_session: dict[str, Any]) -> list[dict[str, Any]]:
        persisted = list(fresh_session.get("messages") or [])
        in_flight = list(session.get("messages") or [])
        return in_flight if len(in_flight) >= len(persisted) else persisted

    if not job_id:
        fresh = store.get_session(sid) or session
        messages = _messages_for_turn(fresh)
        assistant_text = (
            "I understood you want to keep scene 1 and finish the rest of the short, but I could not find "
            "the proof-scene job to expand. Reply directly to the scene 1 card, or say continue on the "
            "chat that generated that still."
        )
        messages.append({"role": "assistant", "content": assistant_text})
        store.update_session(sid, messages=messages)
        return {
            "session_id": sid,
            "assistant_message": assistant_text,
            "pending_actions": [],
            "active_jobs": list(fresh.get("active_jobs") or []),
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

    # Proof scene must be signed off before we expand into a full short.
    # Animation-ready is NOT approval — creator must explicitly ask to expand.
    try:
        snapshot = get_job_snapshot(job_id, "shortform")
    except Exception:
        snapshot = {}
    stage = str(snapshot.get("stage") or "").lower()
    scenes = list(snapshot.get("scenes") or [])
    has_clip = any(isinstance(sc, dict) and sc.get("has_clip") for sc in scenes)
    awaiting_signoff = stage in {
        "awaiting_animation_review",
        "awaiting_scene_review",
        "animate",
        "stills_done",
        "review_scenes",
    } or (has_clip and stage not in {"complete", "failed", "cancelled"})
    duration_seconds = _parse_target_duration_seconds(user_text)
    low = str(user_text or "").strip().lower()
    signed_off = any(
        phrase in low
        for phrase in (
            "approve",
            "approved",
            "looks good",
            "look good",
            "looks great",
            "good to go",
            "ship it",
            "sign off",
            "signed off",
            "finalize scene",
            "scene 1 is good",
            "clip is good",
            "motion looks",
            "i like scene",
            "like scene one",
            "like scene 1",
            "like scene first",
            "love scene one",
            "love scene 1",
            "scene one looks",
            "scene 1 looks",
            "let's go ahead",
            "lets go ahead",
            "go ahead and make",
            "go ahead and finish",
        )
    )
    sticky_intake = dict(session.get("short_expansion_intake") or {})
    continuing_same_job_intake = bool(
        sticky
        and str(sticky_intake.get("job_id") or "") == job_id
        and str(sticky_intake.get("step") or "") in {"duration", "creative", "confirm"}
    )
    if awaiting_signoff and not explicit_expand and not continuing_same_job_intake:
        # Ignore unrelated replies while the creator is still reviewing Scene 1,
        # but preserve a validated expand intake that is already awaiting an answer.
        if sticky:
            store.update_session(sid, short_expansion_intake={})
        return None
    if awaiting_signoff and explicit_expand and not duration_seconds and not signed_off:
        fresh = store.get_session(sid) or session
        messages = _messages_for_turn(fresh)
        assistant_text = (
            "Scene 1 still needs your sign-off before I expand into the full short. "
            "Play the clip in the card — if the silent motion looks right, say "
            "**approve scene 1 and finish the short** (and give a target length like 30 or 45 seconds). "
            "If the motion is wrong, tell me what to change and I’ll re-animate that one scene only."
        )
        messages.append({"role": "assistant", "content": assistant_text})
        store.update_session(sid, messages=messages, short_expansion_intake={})
        return {
            "session_id": sid,
            "assistant_message": assistant_text,
            "pending_actions": [],
            "active_jobs": list(fresh.get("active_jobs") or []),
            "approval_mode": approval_mode,
            "reasoning_depth": reasoning_depth,
            "usage": {},
            "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
        }

    fresh = store.get_session(sid) or session
    messages = _messages_for_turn(fresh)
    intake = dict(fresh.get("short_expansion_intake") or {})
    same_job_intake = str(intake.get("job_id") or "") == job_id
    if not same_job_intake:
        intake = {"job_id": job_id, "step": "duration"}

    def _question(text: str, next_intake: dict[str, Any]) -> dict[str, Any]:
        messages.append({"role": "assistant", "content": text})
        # Expand intake owns this turn — drop any mistaken "start new short" approval card.
        updated = store.update_session(
            sid,
            messages=messages,
            short_expansion_intake=next_intake,
            pending_actions=[],
            last_production=None,
        )
        return {
            "session_id": sid,
            "assistant_message": text,
            "pending_actions": [],
            "active_jobs": list((updated or fresh).get("active_jobs") or []),
            "approval_mode": approval_mode,
            "reasoning_depth": reasoning_depth,
            "usage": {},
            "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
        }

    duration_seconds = duration_seconds or (
        int(intake.get("duration_seconds")) if intake.get("duration_seconds") else None
    )
    if not duration_seconds:
        # Sticky duration answers like "30" / "45s" while intake is waiting.
        if str(intake.get("step") or "") == "duration":
            bare = re.fullmatch(r"\s*(\d{2,3})\s*(?:s|sec|secs|seconds?)?\s*[.!?]?\s*", str(user_text or ""))
            if bare:
                duration_seconds = max(10, min(120, int(bare.group(1))))
    if not duration_seconds:
        # Only ask (or re-ask) when this turn is an explicit expand request.
        if not explicit_expand:
            return None
        return _question(
            "How long do you want the finished short to be? Give me a target such as 20, 30, 45, or 60 seconds.",
            {"job_id": job_id, "step": "duration"},
        )

    if str(intake.get("step") or "duration") == "duration":
        return _question(
            (
                f"Got it — about {duration_seconds} seconds. Before I build the remaining scenes, what do you want "
                "for motion graphics/effects, pacing, overall emotional tone, captions, and sound design? "
                "You can describe them naturally or say ‘choose for me’ and Studio will direct them from the script."
            ),
            {"job_id": job_id, "step": "creative", "duration_seconds": duration_seconds},
        )

    creative_direction = str(user_text or "").strip()
    if str(intake.get("step") or "") == "confirm":
        from studio_agent.visual_fix_contract import parse_animate_policy, expand_confirm_summary

        duration_seconds = int(intake.get("duration_seconds") or duration_seconds or 0)
        creative_direction = str(intake.get("creative_direction") or creative_direction or "").strip()
        animate_policy = str(intake.get("animate_policy") or parse_animate_policy(creative_direction))
        low_confirm = str(user_text or "").strip().lower()
        # Allow last-second policy tweaks before yes.
        tweaked = parse_animate_policy(user_text, default=None)
        if tweaked in {"heroes", "all", "none"} and not re.search(
            r"^\s*(?:yes|y|confirm|go|do\s+it|start|proceed|locked\s+in)\b",
            low_confirm,
        ):
            animate_policy = tweaked
            scene_count = _scene_count_for_duration(duration_seconds)
            return _question(
                expand_confirm_summary(
                    duration_seconds=duration_seconds,
                    scene_count=scene_count,
                    animate_policy=animate_policy,
                    creative_direction=creative_direction,
                ),
                {
                    "job_id": job_id,
                    "step": "confirm",
                    "duration_seconds": duration_seconds,
                    "creative_direction": creative_direction,
                    "animate_policy": animate_policy,
                },
            )
        if not re.search(
            r"^\s*(?:yes|y|confirm|go|do\s+it|start|proceed|locked\s+in)\b",
            low_confirm,
        ):
            scene_count = _scene_count_for_duration(duration_seconds)
            return _question(
                expand_confirm_summary(
                    duration_seconds=duration_seconds,
                    scene_count=scene_count,
                    animate_policy=animate_policy,
                    creative_direction=creative_direction,
                ),
                {
                    "job_id": job_id,
                    "step": "confirm",
                    "duration_seconds": duration_seconds,
                    "creative_direction": creative_direction,
                    "animate_policy": animate_policy,
                },
            )
        # Confirmed — fall through to expand with stored creative + policy.
    elif len(creative_direction) < 2:
        return _question(
            "Tell me the motion-graphics/effects, pacing, tone, captions, and sound direction—or say ‘choose for me’.",
            {"job_id": job_id, "step": "creative", "duration_seconds": duration_seconds},
        )
    elif re.fullmatch(r"(?i)(?:you\s+)?choose(?:\s+for\s+me)?[.!?]*|surprise\s+me[.!?]*", creative_direction):
        creative_direction = (
            "Studio chooses from the script: cinematic emotional pacing, restrained hero-moment motion graphics only, "
            "clean word captions, subtle sound design, no distracting overlays, and a strong visual escalation into the payoff."
        )
    else:
        from studio_agent.turn_plan import extract_youtube_urls_from_text

        reference_urls = extract_youtube_urls_from_text(creative_direction)
        if reference_urls:
            reference_fires: list[ToolFire] = []
            await _run_youtube_url_reference_preflight(
                emit=emit,
                user_id=user_id,
                content_format="short",
                session_id=sid,
                messages=messages,
                tool_fires=reference_fires,
                user_text=creative_direction,
                reference_request_text=creative_direction,
            )
            payload = _latest_reference_actionable_from_fires(reference_fires, messages=messages)
            if not payload:
                return _question(
                    "I downloaded the reference, but its visual/audio analysis did not finish cleanly enough to direct from. "
                    "Send another YouTube link, upload the MP4, or say ‘choose for me’ and I’ll direct it from the script.",
                    {"job_id": job_id, "step": "creative", "duration_seconds": duration_seconds},
                )
            creative_direction = _reference_production_grammar(payload)

    if str(intake.get("step") or "") != "confirm":
        from studio_agent.visual_fix_contract import parse_animate_policy, expand_confirm_summary

        animate_policy = parse_animate_policy(creative_direction)
        scene_count = _scene_count_for_duration(duration_seconds)
        return _question(
            expand_confirm_summary(
                duration_seconds=duration_seconds,
                scene_count=scene_count,
                animate_policy=animate_policy,
                creative_direction=creative_direction,
            ),
            {
                "job_id": job_id,
                "step": "confirm",
                "duration_seconds": duration_seconds,
                "creative_direction": creative_direction,
                "animate_policy": animate_policy,
            },
        )

    from studio_agent.visual_fix_contract import parse_animate_policy

    animate_policy = str(
        intake.get("animate_policy")
        or parse_animate_policy(creative_direction)
        or "heroes"
    )
    scene_count = _scene_count_for_duration(duration_seconds)
    expand_args: dict[str, Any] = {
        "job_id": job_id,
        "scene_count": scene_count,
        "animate_policy": animate_policy,
    }
    if duration_seconds:
        expand_args["duration_seconds"] = duration_seconds
    expand_args["creative_direction"] = creative_direction
    profile = billing_profile or {}
    expand_result = ""
    async with studio_agent_slot(
        user_id=user_id,
        plan=membership_plan,
        operation="expand_visual_proof_short",
        unlimited=bool(profile.get("unlimited")),
    ):
        await _fire_event(emit, "tool_start", tool="expand_visual_proof_shortform", round=0, awaiting_approval=False)
        try:
            expand_result = execute_tool_logged(
                "expand_visual_proof_shortform",
                expand_args,
                user_id=user_id,
                content_format=content_format,
                session_id=sid,
            )
            await _fire_event(emit, "tool_end", tool="expand_visual_proof_shortform", status="ok")
        except Exception as exc:
            expand_result = json.dumps({"error": str(exc), "job_id": job_id})
            await _fire_event(
                emit,
                "tool_end",
                tool="expand_visual_proof_shortform",
                status="error",
                error=str(exc)[:160],
            )

    try:
        parsed = json.loads(expand_result or "{}")
    except Exception:
        parsed = {}
    if isinstance(parsed, dict) and parsed.get("error"):
        assistant_text = (
            f"I tried to expand the approved proof scene into a full short, but it failed: "
            f"{str(parsed.get('error'))[:400]}"
        )
    else:
        duration_note = f" Target length is about {duration_seconds} seconds." if duration_seconds else ""
        policy_note = f" Animate policy: {animate_policy}."
        assistant_text = (
            "I kept your approved first scene and started Fast expand for the remaining short-form scenes "
            f"on the same job.{duration_note}{policy_note} Watch the production card — scene 1 stays locked."
        )
    active_jobs = merge_active_jobs(
        list(fresh.get("active_jobs") or []),
        [{
            "job_id": job_id,
            "kind": "shortform",
            "title": str((parsed or {}).get("topic") or "Short-form video"),
            "started_at": time.time(),
        }],
    )
    blocked = [
        str(item).strip()
        for item in (fresh.get("blocked_job_ids") or [])
        if str(item).strip() and str(item).strip() != job_id
    ]
    messages.append(_tool_observation_message("expand_visual_proof_shortform", expand_result))
    messages.append({"role": "assistant", "content": assistant_text})
    store.update_session(
        sid,
        messages=messages,
        active_jobs=active_jobs,
        blocked_job_ids=blocked,
        skip_job_recovery=False,
        short_expansion_intake={},
    )
    await _fire_event(emit, "active_jobs", jobs=active_jobs)
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


def _verified_expand_command_target(session: dict[str, Any], job_id: str) -> bool:
    """Verify the proof job from disk before exposing it to a model or tool.

    Session cards are useful routing hints, but they are not authorization. The
    job spec is the authority for both ownership and whether this is still a
    one-scene proof that can be expanded in place.
    """

    wanted_job = str(job_id or "").strip()
    wanted_user = str(session.get("user_id") or "").strip()
    if not wanted_job or not wanted_user:
        return False
    try:
        from studio_agent.tools import _expandable_proof_job, _shortform_workspace

        workspace = _shortform_workspace(wanted_job)
        spec_path = workspace / "job_spec.json"
        if not spec_path.is_file():
            return False
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
        return bool(
            isinstance(spec, dict)
            and str(spec.get("user_id") or "").strip() == wanted_user
            and _expandable_proof_job(spec, workspace)
        )
    except Exception:
        return False


def _verified_scene_repair_command_target(session: dict[str, Any], job_id: str) -> bool:
    """Verify ownership and a durable short-form scene set before any repair.

    A failed render is still editable: repairing failed scenes is the purpose of
    this command.  Active-job pruning may hide terminal work from the polling
    ledger, but it must not turn an owned workspace into an unowned target.
    """

    wanted_job = str(job_id or "").strip()
    wanted_user = str(session.get("user_id") or "").strip()
    if not wanted_job or not wanted_user:
        return False
    try:
        from studio_agent.tools import _shortform_workspace

        workspace = _shortform_workspace(wanted_job)
        spec_path = workspace / "job_spec.json"
        scenes_path = workspace / "scenes.json"
        if not spec_path.is_file() or not scenes_path.is_file():
            return False
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
        scenes = json.loads(scenes_path.read_text(encoding="utf-8"))
        if not isinstance(spec, dict) or not isinstance(scenes, list) or not scenes:
            return False
        if str(spec.get("user_id") or "").strip() != wanted_user:
            return False
        snapshot = get_job_snapshot(wanted_job, "shortform")
        status = str(snapshot.get("status") or "").strip().lower()
        stage = str(snapshot.get("stage") or "").strip().lower()
        non_editable = {"cancelled", "canceled", "missing"}
        return status not in non_editable and stage not in non_editable
    except Exception:
        return False


def _find_repairable_shortform_job(
    session: dict[str, Any],
    *,
    reply_to: dict | None = None,
) -> str | None:
    """Recover the creator's owned multi-scene short without choosing a newer proof."""

    pending = session.get("pending_scene_repair")
    pending_job = str(pending.get("job_id") or "").strip() if isinstance(pending, dict) else ""
    reply_job = str((reply_to or {}).get("job_id") or "").strip()
    for preferred in (pending_job, reply_job):
        if preferred and _verified_scene_repair_command_target(session, preferred):
            return preferred

    candidate_ids: list[str] = []
    for row in reversed(list(session.get("active_jobs") or [])):
        if isinstance(row, dict) and str(row.get("kind") or "shortform") == "shortform":
            candidate_ids.append(str(row.get("job_id") or ""))
    recovered = _recover_shortform_job_from_session(session)
    if recovered:
        candidate_ids.append(recovered)
    # A terminal job is intentionally removed from active_jobs, but the typed
    # command ledger remains durable same-session evidence for follow-up repair.
    # Ownership and scene files are still re-verified below before selection.
    last_command = session.get("last_studio_command")
    if isinstance(last_command, dict):
        for validation_key in ("validation", "command_validation"):
            validation = last_command.get(validation_key)
            resolved = validation.get("resolved_action") if isinstance(validation, dict) else None
            arguments = resolved.get("arguments") if isinstance(resolved, dict) else None
            if isinstance(arguments, dict):
                candidate_ids.append(str(arguments.get("job_id") or ""))
        for receipt_key in ("receipt", "execution_receipt"):
            receipt = last_command.get(receipt_key)
            if isinstance(receipt, dict):
                candidate_ids.extend([
                    str(receipt.get("target_job_id") or ""),
                    str((receipt.get("result") or {}).get("job_id") or "")
                    if isinstance(receipt.get("result"), dict)
                    else "",
                ])
    for message in reversed(list(session.get("messages") or [])[-120:]):
        if not isinstance(message, dict):
            continue
        deliverable = message.get("jobDeliverable")
        if isinstance(deliverable, dict):
            candidate_ids.append(str(deliverable.get("job_id") or ""))
        for match in re.finditer(
            r'"job_id"\s*:\s*"([A-Za-z0-9_-]{6,48})"',
            str(message.get("content") or ""),
        ):
            candidate_ids.append(match.group(1))

    expected_title = (
        str((session.get("pending_concept") or {}).get("title") or "")
        if isinstance(session.get("pending_concept"), dict)
        else ""
    )
    ranked: list[tuple[int, int, int, str]] = []
    for order, candidate in enumerate(dict.fromkeys(value for value in candidate_ids if value)):
        if not _verified_scene_repair_command_target(session, candidate):
            continue
        try:
            snapshot = get_job_snapshot(candidate, "shortform")
        except Exception:
            continue
        title = str(snapshot.get("title") or snapshot.get("topic") or "")
        title_match = int(
            not expected_title
            or not title
            or store._title_overlap_score(expected_title, title) >= 0.75
        )
        count = int(
            snapshot.get("total_scenes")
            or snapshot.get("scene_count")
            or len(snapshot.get("scenes") or [])
            or 0
        )
        ranked.append((title_match, count, -order, candidate))
    if not ranked:
        return None
    ranked.sort(reverse=True)
    return ranked[0][3]


async def _bill_studio_command_compiler(
    *,
    response: dict[str, Any],
    requested_model: str,
    user_id: str,
    session_id: str,
    billing_profile: dict[str, Any] | None,
) -> dict[str, Any]:
    """Meter the selected model's semantic-compiler call exactly once."""

    usage = openrouter.usage_from_response(response) if response else {}
    prompt_tokens = int(usage.get("prompt_tokens", 0) or 0)
    completion_tokens = int(usage.get("completion_tokens", 0) or 0)
    provider = str(response.get("provider") or "unknown") if response else "deterministic_fallback"
    effective_model = str(response.get("model") or requested_model) if response else requested_model
    credits_charged = 0
    usd_cost = 0.0
    prompt_ppm, completion_ppm, debit_reason, billed_model = _llm_pricing_for_provider(
        provider,
        requested_model,
        effective_model,
    )
    if response and provider_policy.normalize_provider(provider) != "anthropic":
        try:
            prompt_ppm, completion_ppm = await openrouter.model_pricing(requested_model)
            billed_model = requested_model
        except Exception:
            prompt_ppm = completion_ppm = None
    try:
        import unified_credits as uc

        usd_cost = uc.openrouter_usd(
            {"prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens},
            prompt_ppm,
            completion_ppm,
        )
        if (
            usd_cost > 0
            and str(user_id or "").strip()
            and not bool((billing_profile or {}).get("unlimited"))
        ):
            credits_charged, _balance = uc.debit_usd(
                user_id,
                usd_cost,
                reason=debit_reason,
                metadata={
                    "provider": provider,
                    "model": billed_model,
                    "session_id": session_id,
                    "operation": "studio_command_compiler",
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                },
                allow_negative=False,
            )
    except Exception:
        pass
    return {
        "credits_charged": credits_charged,
        "provider_usd": round(float(usd_cost or 0.0), 6),
        "provider": provider,
        "model": billed_model,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
    }


def _scene_repair_failure_message(
    selected_scene_numbers: list[int],
    result: dict[str, Any],
    error: str = "",
) -> str:
    """Describe partial repair truth without treating attempted work as success."""

    selected_text = ", ".join(str(number) for number in selected_scene_numbers)
    failed_indices = {
        int(value)
        for value in list(result.get("failed") or [])
        if isinstance(value, (int, float)) or str(value).strip().isdigit()
    }
    repaired_stills = sorted({
        int(value) + 1
        for value in list(result.get("repaired_stills") or [])
        if (isinstance(value, (int, float)) or str(value).strip().isdigit())
        and int(value) not in failed_indices
    })
    repaired_clips = sorted({
        int(value) + 1
        for value in list(result.get("repaired_animations") or [])
        if (isinstance(value, (int, float)) or str(value).strip().isdigit())
        and int(value) not in failed_indices
    })
    failed_scenes = sorted(index + 1 for index in failed_indices)
    details: list[str] = []
    if repaired_stills:
        details.append(f"rebuilt stills for Scene(s) {', '.join(map(str, repaired_stills))}")
    if repaired_clips:
        details.append(f"re-animated Scene(s) {', '.join(map(str, repaired_clips))}")
    if details or failed_scenes:
        completed = "; ".join(details) if details else "no selected scene completed cleanly"
        failed = ", ".join(map(str, failed_scenes)) or selected_text
        return (
            f"I audited exactly Scene(s) {selected_text}. The repair only partially completed: {completed}. "
            f"Scene(s) {failed} still failed, so I am not claiming those scenes are fixed."
        )
    return (
        f"I did not claim a repair for Scene(s) {selected_text} because the validated audit failed: "
        f"{str(error or result.get('error') or 'unknown error')[:400]}"
    )


async def _apply_model_agnostic_studio_command(
    *,
    session: dict[str, Any],
    user_id: str,
    user_text: str,
    content_format: str,
    model: str,
    emit: EventEmitter | None,
    membership_plan: str,
    billing_profile: dict[str, Any] | None,
    approval_mode: str,
    reasoning_depth: str,
    reply_to: dict | None = None,
) -> dict[str, Any] | None:
    """Compile, validate, execute, and verify typed production commands.

    Every selectable chat model sees the same single non-mutating compiler tool.
    The selected model never receives the render tools or permission to mutate a
    job. A typed server command, deterministic policy validation, and observed
    postconditions own those responsibilities.
    """

    if str(os.getenv("STUDIO_COMMAND_ROUTING_MODE", "authoritative")).strip().lower() == "off":
        return None
    from studio_agent.command_contract import (
        scene_repair_candidate,
        scene_repair_cancellation_evidence,
    )

    pending_repair = session.get("pending_scene_repair")
    has_pending_repair = bool(isinstance(pending_repair, dict) and pending_repair)
    if has_pending_repair and scene_repair_cancellation_evidence(user_text):
        sid = str(session.get("session_id") or "")
        fresh = store.get_session(sid) or session
        messages = list(fresh.get("messages") or [])
        assistant_text = "Understood — I will not repair or change those scenes."
        messages.append({"role": "assistant", "content": assistant_text})
        updated = store.update_session(
            sid,
            messages=messages,
            pending_scene_repair={},
            agent_mode="plan",
            interaction_state="plan",
            production_gate_open=False,
            active_command_id="",
        ) or fresh
        return _turn_result(updated, {
            "session_id": sid,
            "assistant_message": assistant_text,
            "pending_actions": list(updated.get("pending_actions") or []),
            "active_jobs": list(updated.get("active_jobs") or []),
            "approval_mode": approval_mode,
            "reasoning_depth": reasoning_depth,
            "usage": {},
            "billing": {
                "credits_charged": 0,
                "provider_usd": 0.0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
            },
        })
    if has_pending_repair and re.search(
        r"\b(?:work|focus|talk|move\s+on)\b.{0,80}\b(?:instead|something\s+else)\b",
        str(user_text or ""),
        flags=re.IGNORECASE,
    ):
        # An explicit topic switch is cancellation, not an ambiguous repair
        # answer. Clear it without forcing the creator through repair dialogue.
        try:
            store.update_session(
                str(session.get("session_id") or ""),
                pending_scene_repair={},
                agent_mode="plan",
                interaction_state="plan",
                production_gate_open=False,
                active_command_id="",
            )
        except Exception:
            pass
        return None
    wants_repair = bool(
        scene_repair_candidate(user_text)
        # Once clarification is pending, every non-cancellation answer goes
        # back through the compiler. Natural contextual answers such as "the
        # second one" or "visuals only" must not be discarded by regex.
        or has_pending_repair
    )
    wants_expand = bool(
        (_wants_expand_visual_proof_short(user_text) or session.get("short_expansion_intake"))
        and not wants_repair
    )
    if not wants_expand and not wants_repair:
        return None

    sid = str(session.get("session_id") or "")
    candidate = (
        _find_repairable_shortform_job(session, reply_to=reply_to)
        if wants_repair
        else _find_expandable_shortform_job(session, reply_to=reply_to)
    ) or ""
    target_verified = bool(
        candidate
        and (
            _verified_scene_repair_command_target(session, candidate)
            if wants_repair
            else _verified_expand_command_target(session, candidate)
        )
    )
    if not target_verified:
        fresh = store.get_session(sid) or session
        messages = list(fresh.get("messages") or session.get("messages") or [])
        assistant_text = (
            "I understood the scene-repair request, but I could not verify an owned short-form job to change. "
            "Reply directly to that short's production card and try again."
            if wants_repair
            else
            "I understood that you want to preserve Scene 1 and expand this short, but I could not verify "
            "an expandable proof job owned by this Studio session. Reply directly to the Scene 1 card and try again."
        )
        messages.append({"role": "assistant", "content": assistant_text})
        updated = store.update_session(
            sid,
            messages=messages,
            agent_mode="plan",
            interaction_state="clarification",
            production_gate_open=False,
            active_command_id="",
            pending_actions=[],
            last_production={},
            short_expansion_intake={},
            pending_scene_repair={},
        ) or fresh
        await _fire_event(emit, "pending_actions", actions=[])
        return _turn_result(updated, {
            "session_id": sid,
            "assistant_message": assistant_text,
            "pending_actions": [],
            "active_jobs": list(updated.get("active_jobs") or []),
            "approval_mode": approval_mode,
            "reasoning_depth": reasoning_depth,
            "usage": {},
            "billing": {
                "credits_charged": 0,
                "provider_usd": 0.0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
            },
        })

    from studio_agent.command_execution import execute_validated_command
    from studio_agent.command_planner import plan_studio_command
    from studio_agent.command_postconditions import fingerprint_workspace_scenes, verify_execution
    from studio_agent.command_state import build_studio_state_context
    from studio_agent.command_validation import validate_studio_command
    from studio_agent.model_registry import assert_model_selectable
    from studio_agent.tools import _shortform_workspace

    try:
        assert_model_selectable(model)
    except Exception as exc:
        fresh = store.get_session(sid) or session
        messages = list(fresh.get("messages") or [])
        assistant_text = f"That Studio Agent model cannot be used: {str(exc)[:300]} Select another model and try again."
        messages.append({"role": "assistant", "content": assistant_text})
        updated = store.update_session(
            sid,
            messages=messages,
            short_expansion_intake={},
            agent_mode="plan",
            interaction_state="clarification",
            production_gate_open=False,
            active_command_id="",
        ) or fresh
        return _turn_result(updated, {
            "session_id": sid,
            "assistant_message": assistant_text,
            "pending_actions": list(updated.get("pending_actions") or []),
            "active_jobs": list(updated.get("active_jobs") or []),
            "approval_mode": approval_mode,
            "reasoning_depth": reasoning_depth,
            "usage": {},
            "billing": {
                "credits_charged": 0,
                "provider_usd": 0.0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
            },
        })

    state = build_studio_state_context(
        session,
        reply_to=reply_to,
        expandable_job_id=candidate if wants_expand else None,
        repairable_job_ids=[candidate] if wants_repair else [],
        snapshot_loader=get_job_snapshot,
        ownership_verifier=lambda job_id: (
            str(job_id or "").strip() == candidate
            and (
                _verified_scene_repair_command_target(session, job_id)
                if wants_repair
                else _verified_expand_command_target(session, job_id)
            )
        ),
    )
    compiler_box: dict[str, Any] = {}

    async def _compiler_chat_completion(**kwargs: Any) -> dict[str, Any]:
        training_capture.capture_event(
            str(user_id),
            "studio_command_model_request",
            {
                "model": model,
                "state_revision": state.state_revision,
                "available_actions": state.available_actions,
            },
            session_id=sid,
        )
        response = await openrouter.chat_completion(**kwargs)
        compiler_box["response"] = response
        training_capture.capture_event(
            str(user_id),
            "studio_command_model_response",
            {
                "provider": str(response.get("provider") or "openrouter"),
                "model": str(response.get("model") or model),
                "usage": openrouter.usage_from_response(response),
            },
            session_id=sid,
        )
        return response

    command = await plan_studio_command(
        user_text,
        state,
        model=model,
        chat_completion=_compiler_chat_completion,
    )
    fingerprint_loader = lambda job_id, scene_numbers: fingerprint_workspace_scenes(
        _shortform_workspace(job_id),
        scene_numbers,
    )
    validation = validate_studio_command(
        command,
        state,
        user_text=user_text,
        fingerprint_loader=fingerprint_loader,
    )
    compiler_response = compiler_box.get("response") if isinstance(compiler_box.get("response"), dict) else {}
    compiler_billing = await _bill_studio_command_compiler(
        response=compiler_response,
        requested_model=model,
        user_id=user_id,
        session_id=sid,
        billing_profile=billing_profile,
    )
    await _fire_event(
        emit,
        "studio_command",
        command=command.model_dump(mode="json", exclude_none=True),
        validation=validation.model_dump(mode="json", exclude_none=True),
    )

    if validation.decision == "no_op":
        store.update_session(
            sid,
            agent_mode="plan",
            interaction_state="plan",
            production_gate_open=False,
            active_command_id="",
        )
        return None
    if not validation.can_execute:
        fresh = store.get_session(sid) or session
        messages = list(fresh.get("messages") or [])
        if validation.clarification is not None:
            assistant_text = validation.clarification.question
        else:
            assistant_text = next(
                (issue.message for issue in validation.issues if issue.severity == "error"),
                "I could not safely turn that request into a production command.",
            )
        messages.append({"role": "assistant", "content": assistant_text})
        pending_scene_repair: dict[str, Any] = {}
        if command.action == "audit_and_repair_scenes" and command.repair is not None:
            pending_scene_repair = {
                "job_id": candidate,
                "scene_numbers": list(command.repair.scene_numbers),
                "repair_scope": command.repair.scope,
                "instruction": command.repair.instruction,
                "execution_requested": bool(command.authorization.execution_requested),
                "missing_fields": (
                    list(validation.clarification.missing_fields)
                    if validation.clarification is not None
                    else []
                ),
            }
        updated = store.update_session(
            sid,
            messages=messages,
            agent_mode="plan",
            interaction_state="clarification",
            production_gate_open=False,
            active_command_id="",
            pending_actions=[],
            last_production={},
            short_expansion_intake={},
            pending_scene_repair=pending_scene_repair,
            last_studio_command={
                "command": command.model_dump(mode="json", exclude_none=True),
                "validation": validation.model_dump(mode="json", exclude_none=True),
            },
        ) or fresh
        await _fire_event(emit, "pending_actions", actions=[])
        return _turn_result(updated, {
            "session_id": sid,
            "assistant_message": assistant_text,
            "pending_actions": [],
            "active_jobs": list(updated.get("active_jobs") or []),
            "approval_mode": approval_mode,
            "reasoning_depth": reasoning_depth,
            "studio_command": command.model_dump(mode="json", exclude_none=True),
            "command_validation": validation.model_dump(mode="json", exclude_none=True),
            "usage": openrouter.usage_from_response(compiler_response) if compiler_response else {},
            "billing": compiler_billing,
        })

    action = validation.resolved_action
    assert action is not None
    tool_args = action.arguments.as_legacy_dict()
    # A validated, confirmed command is the only transition that opens the
    # production gate. Clarification turns above close it and stay resource-free.
    claimed_session = store.claim_production_gate(
        sid,
        command_id=command.command_id,
        job_id=candidate,
    )
    if claimed_session is None:
        fresh = store.get_session(sid) or session
        assistant_text = (
            "Another validated production command is already changing this Studio session. "
            "I did not start a second mutation; wait for the active command to finish, then retry."
        )
        messages = list(fresh.get("messages") or [])
        messages.append({"role": "assistant", "content": assistant_text})
        updated = store.update_session(sid, messages=messages) or fresh
        return _turn_result(updated, {
            "session_id": sid,
            "assistant_message": assistant_text,
            "pending_actions": list(updated.get("pending_actions") or []),
            "active_jobs": list(updated.get("active_jobs") or []),
            "approval_mode": approval_mode,
            "reasoning_depth": reasoning_depth,
            "studio_command": command.model_dump(mode="json", exclude_none=True),
            "command_validation": validation.model_dump(mode="json", exclude_none=True),
            "usage": openrouter.usage_from_response(compiler_response) if compiler_response else {},
            "billing": compiler_billing,
        })
    session = claimed_session
    profile = billing_profile or {}

    def _gate_checked_tool_executor(tool_name: str, arguments: dict[str, Any], **kwargs: Any) -> Any:
        """Reject a mutation unless this exact validated command owns the gate."""

        current = store.get_session(
            sid,
            reconcile_jobs=False,
            _prune_active_jobs=False,
        ) or {}
        gate_matches = bool(
            current.get("production_gate_open")
            and str(current.get("interaction_state") or "") == "production"
            and str(current.get("active_command_id") or "") == command.command_id
        )
        if not gate_matches:
            return {
                "ok": False,
                "status": "rejected",
                "error": "Production gate is closed or owned by a different validated command.",
            }
        return execute_tool_logged(tool_name, arguments, **kwargs)

    def _execute_with_job_lock():
        with store.production_job_mutation_lock(candidate):
            return execute_validated_command(
                validation,
                user_id=user_id,
                session_id=sid,
                content_format=content_format,
                tool_executor=_gate_checked_tool_executor,
            )

    try:
        await _fire_tool_start(
            emit,
            action.tool_name,
            args=tool_args,
            round=0,
            awaiting_approval=False,
            semantic_command=True,
            command_id=command.command_id,
        )
        async with studio_agent_slot(
            user_id=user_id,
            plan=membership_plan,
            operation=action.tool_name,
            unlimited=bool(profile.get("unlimited")),
        ):
            # Production tools are synchronous and may spend several minutes in
            # provider I/O. Running them on the event loop prevents tool_start and
            # heartbeat SSE frames from reaching the browser, making a real repair
            # look idle until the UI times out.
            receipt = await asyncio.to_thread(_execute_with_job_lock)
        verdict = verify_execution(
            receipt,
            snapshot_loader=get_job_snapshot,
            fingerprint_loader=fingerprint_loader,
        )
    except BaseException:
        # Cancellation, slot failures, provider exceptions, and verifier errors
        # must never leave a spend-capable gate open for a later turn.
        store.close_production_gate(
            sid,
            command_id=command.command_id,
            interaction_state="verification",
        )
        raise

    # Close before response/UI bookkeeping so a serialization or SSE failure
    # cannot leave production authorized after provider work has ended.
    store.close_production_gate(
        sid,
        command_id=command.command_id,
        interaction_state="verification",
    )
    tool_status = "ok" if receipt.status in {"accepted", "completed", "duplicate"} else "error"
    await _fire_tool_end(
        emit,
        action.tool_name,
        status=tool_status,
        args=tool_args,
        result=receipt.result,
        error=receipt.error or None,
        semantic_command=True,
        command_id=command.command_id,
        postcondition_status=verdict.status,
    )

    expected = action.expected
    if getattr(expected, "kind", "") == "scene_repair":
        selected = list(expected.selected_scene_numbers)
        selected_text = ", ".join(str(number) for number in selected)
        repaired_stills = [int(value) + 1 for value in receipt.result.get("repaired_stills") or []]
        repaired_clips = [int(value) + 1 for value in receipt.result.get("repaired_animations") or []]
        if receipt.status in {"failed", "rejected"}:
            assistant_text = _scene_repair_failure_message(
                selected,
                receipt.result,
                receipt.error,
            )
        elif receipt.status == "duplicate" and receipt.result.get("idempotency_claim_pending"):
            assistant_text = (
                "The same repair command is being handled by another Studio process, but verification has not "
                "confirmed its result yet, so I am not claiming that any scene changed."
            )
        elif verdict.safe_claim == "completed":
            changes: list[str] = []
            if repaired_stills:
                changes.append(f"rebuilt stills {repaired_stills}")
            if repaired_clips:
                changes.append(f"re-animated clips {repaired_clips}")
            detail = "; ".join(changes) if changes else "all selected scenes passed without regeneration"
            assistant_text = (
                f"I audited exactly Scene(s) {selected_text} against their prompt, narration, still quality, "
                f"and animation quality; {detail}. Every unselected scene was verified unchanged."
            )
        elif verdict.safe_claim == "started":
            assistant_text = (
                f"The audit/repair tool ran for Scene(s) {selected_text}, but excluded-scene byte verification "
                "was incomplete, so I am not claiming the repair is fully verified yet."
            )
        else:
            assistant_text = (
                f"The audit/repair result for Scene(s) {selected_text} did not satisfy the typed postconditions, "
                "so I am not claiming that the scenes were fixed."
            )
    else:
        new_scene_range = (
            f"Scenes {expected.expected_existing_scene_count + 1}-{expected.expected_total_scene_count}"
        )
        if expected.expected_animated_scene_numbers:
            animation_started_text = f"{new_scene_range} are queued for animation"
            animation_completed_text = f"{new_scene_range} are animated"
        elif expected.animation_scope == "none":
            animation_started_text = f"{new_scene_range} will remain still as requested"
            animation_completed_text = f"{new_scene_range} remain still as requested"
        else:
            animation_started_text = "only selected hero scenes are queued for animation"
            animation_completed_text = "only selected hero scenes were animated"
        if receipt.status in {"failed", "rejected"}:
            assistant_text = (
                "I did not start the expansion because the validated tool call failed: "
                f"{str(receipt.error or receipt.result.get('error') or 'unknown error')[:400]}"
            )
        elif receipt.status == "duplicate":
            if receipt.result.get("idempotency_claim_pending"):
                assistant_text = (
                    "The same command is being claimed by another Studio process, but immediate job-state "
                    "verification has not confirmed dispatch yet, so I am not claiming that the expansion started."
                )
            else:
                assistant_text = (
                    f"That exact expansion was already accepted on this same job. Scene 1 remains locked; "
                    f"the target is {expected.expected_total_scene_count} scenes total, and {animation_started_text}."
                )
        elif verdict.safe_claim == "completed":
            assistant_text = (
                f"The same short now has exactly {expected.expected_total_scene_count} scenes. Scene 1 was preserved, "
                f"and {animation_completed_text}."
            )
        elif verdict.safe_claim == "started":
            assistant_text = (
                f"I kept Scene 1 locked and started adding exactly {expected.expected_added_scene_count} new scenes "
                f"to the same short ({expected.expected_total_scene_count} total). {animation_started_text.capitalize()}; "
                "Scene 1 will not be regenerated or reanimated."
            )
        else:
            assistant_text = (
                "The tool accepted the command, but the immediate job-state verification did not match the typed "
                "postconditions, so I am not claiming that production started or completed."
            )

    fresh = store.get_session(sid) or session
    messages = list(fresh.get("messages") or [])
    observation = json.dumps(
        {
            "receipt": receipt.model_dump(mode="json", exclude_none=True),
            "postconditions": verdict.model_dump(mode="json", exclude_none=True),
        },
        indent=2,
        ensure_ascii=False,
    )
    messages.append(_tool_observation_message(action.tool_name, observation))
    try:
        command_snapshot = get_job_snapshot(candidate, "shortform")
    except Exception:
        command_snapshot = {}
    assistant_message: dict[str, Any] = {"role": "assistant", "content": assistant_text}
    if command_snapshot:
        # Persist the owned production card with the answer. Terminal jobs are
        # intentionally pruned from active_jobs, so relying on a browser-only
        # snapshot or transcript id inference makes failed scene grids vanish
        # after Sync/reload.
        assistant_message["jobDeliverable"] = command_snapshot
    messages.append(assistant_message)
    active_jobs = list(fresh.get("active_jobs") or [])
    if receipt.status in {"accepted", "completed", "duplicate"}:
        active_jobs = merge_active_jobs(
            active_jobs,
            [{
                "job_id": candidate,
                "kind": "shortform",
                # A repair receipt normally has no topic. Using the generic
                # fallback here made stale-job reconciliation treat the
                # repaired job as unrelated to the current production, add it
                # back to blocked_job_ids, and hide its six-scene deliverable.
                "title": str(
                    command_snapshot.get("title")
                    or receipt.result.get("topic")
                    or "Short-form video"
                ),
                "status": str(command_snapshot.get("status") or "awaiting_approval"),
                "stage": str(command_snapshot.get("stage") or "awaiting_animation_review"),
                "started_at": time.time(),
            }],
        )
    blocked = [
        str(item).strip()
        for item in (fresh.get("blocked_job_ids") or [])
        if str(item).strip() and str(item).strip() != candidate
    ]
    updated = store.update_session(
        sid,
        messages=messages,
        interaction_state="verification",
        active_jobs=active_jobs,
        blocked_job_ids=blocked,
        skip_job_recovery=False,
        pending_actions=[],
        last_production={},
        short_expansion_intake={},
        pending_scene_repair={},
        last_studio_command={
            "command": command.model_dump(mode="json", exclude_none=True),
            "validation": validation.model_dump(mode="json", exclude_none=True),
            "receipt": receipt.model_dump(mode="json", exclude_none=True),
            "postconditions": verdict.model_dump(mode="json", exclude_none=True),
        },
    ) or fresh
    await _fire_event(emit, "pending_actions", actions=[])
    await _fire_event(emit, "active_jobs", jobs=active_jobs)
    if command_snapshot:
        await _fire_event(emit, "job_snapshot", snapshot=command_snapshot)
    return _turn_result(updated, {
        "session_id": sid,
        "assistant_message": assistant_text,
        "pending_actions": [],
        "active_jobs": active_jobs,
        "approval_mode": approval_mode,
        "reasoning_depth": reasoning_depth,
        "studio_command": command.model_dump(mode="json", exclude_none=True),
        "command_validation": validation.model_dump(mode="json", exclude_none=True),
        "execution_receipt": receipt.model_dump(mode="json", exclude_none=True),
        "postcondition_verdict": verdict.model_dump(mode="json", exclude_none=True),
        "usage": openrouter.usage_from_response(compiler_response) if compiler_response else {},
        "billing": compiler_billing,
    })


def _is_targeted_scene_animation_request(user_text: str) -> bool:
    low = str(user_text or "").strip().lower()
    return bool(re.search(r"\b(?:animate|animation for)\s+(?:only\s+)?scene\s+(?:\d+|one|two|three|four|five|six)\b", low))


async def _apply_targeted_scene_animation(
    *,
    session: dict[str, Any],
    user_id: str,
    user_text: str,
    content_format: str,
    emit: EventEmitter | None,
    approval_mode: str,
    reasoning_depth: str,
) -> dict[str, Any] | None:
    if not _is_targeted_scene_animation_request(user_text):
        return None
    job_id = _recover_shortform_job_from_session(session) or ""
    if not job_id:
        return None
    snapshot = get_job_snapshot(job_id, "shortform")
    total = int(snapshot.get("total_scenes") or snapshot.get("scene_count") or 1)
    scene_index = _parse_scene_index_from_text(user_text, max(1, total))
    sid = str(session.get("session_id") or "")
    fresh = store.get_session(sid) or session
    messages = list(fresh.get("messages") or [])
    try:
        await _fire_event(emit, "tool_start", tool="animate_production_scenes", round=0, awaiting_approval=False)
        execute_tool_logged(
            "set_production_scenes_animate",
            {"job_id": job_id, "animate": True, "scene_indices": [scene_index]},
            user_id=user_id, content_format=content_format, session_id=sid,
        )
        raw = execute_tool_logged(
            "animate_production_scenes",
            {"job_id": job_id, "scene_indices": [scene_index]},
            user_id=user_id, content_format=content_format, session_id=sid,
        )
        parsed = json.loads(raw or "{}")
        if isinstance(parsed, dict) and (parsed.get("error") or parsed.get("failed")):
            raise RuntimeError(str(parsed.get("error") or parsed.get("failed")))
        assistant_text = f"Scene {scene_index + 1} is animated. Review that clip; I have not generated the remaining scenes."
        await _fire_event(emit, "tool_end", tool="animate_production_scenes", status="ok")
    except Exception as exc:
        assistant_text = (
            f"Scene {scene_index + 1} could not be animated yet: {str(exc)[:500]} "
            "Say ‘fix it’ and I’ll repair the failed still automatically."
        )
        await _fire_event(emit, "tool_end", tool="animate_production_scenes", status="error", error=str(exc)[:160])
    messages.append({"role": "assistant", "content": assistant_text})
    active_jobs = merge_active_jobs(list(fresh.get("active_jobs") or []), [{"job_id": job_id, "kind": "shortform", "started_at": time.time()}])
    store.update_session(sid, messages=messages, active_jobs=active_jobs)
    await _fire_event(emit, "active_jobs", jobs=active_jobs)
    try:
        await _fire_event(emit, "job_snapshot", snapshot=get_job_snapshot(job_id, "shortform"))
    except Exception:
        pass
    return {
        "session_id": sid,
        "assistant_message": assistant_text,
        "pending_actions": [],
        "active_jobs": active_jobs,
        "approval_mode": approval_mode,
        "reasoning_depth": reasoning_depth,
        "usage": {},
        "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
    }


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
        "analyze_cliplab_video",
        "render_cliplab_segments",
        "remix_cliplab_short",
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
        "analyze_cliplab_video",
        "render_cliplab_segments",
        "remix_cliplab_short",
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


def _has_public_demand_tool(tool_fires: list[ToolFire]) -> bool:
    return any(str(fire.name or "") in {"get_public_search_trends", "recommend_video_topics"} for fire in tool_fires or [])


def _tool_fire_payload(fire: ToolFire) -> dict[str, Any]:
    try:
        payload = json.loads(fire.result or "{}")
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _tool_fire_failed(fire: ToolFire) -> bool:
    """True when a tool fired but returned a hard failure (import/quota/API)."""
    payload = _tool_fire_payload(fire)
    if not payload:
        text = str(fire.result or "").strip().lower()
        return text.startswith("error:") or "no module named" in text
    if payload.get("error"):
        return True
    status = str(payload.get("status") or "").strip().lower()
    if status.startswith("blocked_") or status in {"error", "failed"}:
        return True
    return False


def _has_successful_public_demand_tool(tool_fires: list[ToolFire]) -> bool:
    """True only when a public-demand tool ran and returned usable (non-error) evidence."""
    for fire in tool_fires or []:
        if str(fire.name or "") not in {"get_public_search_trends", "recommend_video_topics", "search_youtube_public"}:
            continue
        if _tool_fire_failed(fire):
            continue
        payload = _tool_fire_payload(fire)
        if payload.get("videos") or payload.get("trending_sample") or payload.get("predicted_topics"):
            return True
        summary = payload.get("evidence_summary")
        if isinstance(summary, dict) and int(summary.get("total_rows") or summary.get("hydrated_rows") or 0) > 0:
            return True
        # Successful empty search (quota ok, just no matches) still counts as executed.
        if payload.get("source") or payload.get("queries") is not None:
            return True
    return False


def _public_demand_needs_retry(tool_fires: list[ToolFire]) -> bool:
    """Retry when research tools never ran, or only returned hard errors."""
    if not _has_public_demand_tool(tool_fires):
        return True
    if _has_successful_public_demand_tool(tool_fires):
        return False
    # Fired but every attempt failed — retry once more with a clean path.
    return any(
        str(fire.name or "") in {"get_public_search_trends", "recommend_video_topics", "search_youtube_public"}
        and _tool_fire_failed(fire)
        for fire in tool_fires or []
    )


def _is_channel_status_only_answer(assistant_text: str) -> bool:
    low = str(assistant_text or "").strip().lower()
    return (
        low.startswith("i can see data for ")
        and "- source:" in low
        and "- oauth connected for private analytics:" in low
        and "recommended topics" not in low
        and "public youtube demand" not in low
    )


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


def _is_one_still_production_request(user_text: str) -> bool:
    low = store._user_message_before_attachments(user_text).lower()
    if store.is_hard_production_commit(user_text):
        return True
    return bool(
        re.search(
            r"\bmake\s+(?:exactly\s+)?(?:one|1|a|single)\s+(?:still|scene|image|frame)\b|"
            r"\b(?:make|render|start|build|produce|generate)\s+(?:the\s+)?(?:very\s+)?(?:first|1st)\s+scene\b|"
            r"\b(?:make|render|start|build|produce|generate)\s+scene\s*(?:#?\s*1|one)\b|"
            r"\bvisual\s+proof\b|\bproof\s+(?:still|image)\b|\btest\s+(?:still|image)\b",
            low,
        )
    )


def _allows_brand_new_production_tool(user_text: str) -> bool:
    """DEFAULT DENY — only hard commits may open start_shortform/longform Approve.

    Root cause of stale research Approves: tools were default-allowed unless a
    research/soft heuristic matched. Bare \"let's make [title]\" fell through and
    the model queued Approve. Invert: allow only hard commit / explicit / one-still.
    """
    text = str(user_text or "").strip()
    if not text:
        return False
    # Expanding an approved Scene 1 proof must never open a brand-new short Approve.
    if _wants_expand_visual_proof_short(text):
        return False
    # Soft / strategy / research always plan first (even if they contain "make").
    if store.is_soft_production_proposal(text) or store.is_production_strategy_question(text):
        return False
    if _is_research_only_turn(text):
        return False
    if store.is_ideation_request(text):
        return False
    if store.is_channel_video_analysis_request(text):
        return False
    if store.is_youtube_channel_url_reference_request(text):
        return False
    if store.is_hard_production_commit(text) or store.is_explicit_production_request(text):
        return True
    if store.is_scene_one_proof_commit(text):
        return True
    if _is_one_still_production_request(text):
        return True
    return False


def _turn_result(session: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    """Attach authoritative production ledger fields to every chat turn result."""
    sid = str(session.get("session_id") or result.get("session_id") or "").strip()
    fresh = store.get_session(sid) if sid else None
    fresh = fresh or session
    merged = dict(result)
    merged.update(store.production_session_fields(fresh))
    if "active_jobs" in result:
        merged["active_jobs"] = store.filter_active_jobs_for_session({
            **fresh,
            "active_jobs": result.get("active_jobs") or [],
        })
    return merged


def _wants_production_resume(user_text: str) -> bool:
    """Soft start language used to resume an already-planned production (last_production)."""
    low = str(user_text or "").strip().lower()
    if not low:
        return False
    if store.is_soft_production_proposal(low) and not re.search(
        r"\b(?:let'?s do it|do it|get it started|start it|go ahead|begin|kick\s*off|ship it)\b",
        low,
    ):
        return False
    return bool(
        re.search(
            r"\b(?:let'?s do it|let'?s get it started|get it started|start it|"
            r"go ahead(?: and (?:start|do|make|render|generate))?|do it|"
            r"begin(?: production| the (?:short|video|render))?|kick\s*off|ship it)\b",
            low,
        )
    )


def _wants_production_execution(user_text: str) -> bool:
    """Hard commit to begin/resume production (not planning proposals)."""
    if _wants_expand_visual_proof_short(user_text):
        return False
    if _allows_brand_new_production_tool(user_text):
        return True
    # Recovery path: user already planned a short; "let's do it" should resume it.
    return _wants_production_resume(user_text)


def _is_research_only_turn(user_text: str) -> bool:
    """Public demand / niche research without a hard production commit."""
    if store.is_hard_production_commit(user_text) or store.is_explicit_production_request(user_text):
        return False
    if store.is_channel_video_analysis_request(user_text):
        return True
    if store.is_youtube_channel_url_reference_request(user_text):
        return True
    if store.is_public_youtube_research_request(user_text):
        return True
    try:
        from studio_agent.live_demand import has_demand_signal, is_research_execution_request

        if is_research_execution_request(user_text):
            return True
        if has_demand_signal(user_text) and not store.is_hard_production_commit(user_text):
            # Demand language without hard commit = research first.
            return True
    except Exception:
        pass
    return False


def _blocks_brand_new_production(user_text: str) -> bool:
    """True when start_shortform/longform must not open Approve this turn."""
    return not _allows_brand_new_production_tool(user_text)


def _strip_production_pending(actions: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Drop brand-new production approvals (keep non-start tools if any)."""
    out: list[dict[str, Any]] = []
    for row in actions or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("tool") or "") in BRAND_NEW_PRODUCTION_TOOLS:
            continue
        out.append(row)
    return out


CONVERSATIONAL_DATA_TOOLS = frozenset({
    "get_channel_analytics",
    "list_youtube_channels",
    "get_public_search_trends",
    "search_youtube_public",
    "recommend_video_topics",
    "analyze_reference_video",
    "analyze_competitor_video",
    "retry_reference_analysis",
    "refresh_channel_intelligence",
})


def _requires_tool_execution(user_text: str) -> bool:
    """Return true when a user is asking Studio to do work, not discuss it."""
    # Explicit reference / uploaded video analysis always needs tools.
    if store.is_uploaded_video_analysis_request(user_text) or store.is_explicit_reference_analysis_request(
        user_text
    ):
        return True
    if store.is_contextual_reference_video_request(user_text):
        return True
    if store.is_conversational_planning_turn(user_text):
        return False
    if store.is_ideation_request(user_text):
        return False
    if store.is_competitor_channel_reference_request(user_text):
        return False
    if store.is_youtube_channel_url_reference_request(user_text):
        return False
    low = str(user_text or "").strip().lower()
    if not low:
        return False
    action = re.search(
        r"\b("
        r"regenerate|edit|change|fix|replace|remove|add|create|make|render|generate|"
        r"animate|approve|finalize|publish|upload|pull|fetch|refresh|check|inspect|"
        r"list|show|analy[sz]e|crawl|resume|continue|download|watch|do it|go ahead"
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
    # Soft resume language ("let's do it") may not open brand-new production,
    # but it can recover an already-planned start_shortform/longform action.
    if _blocks_brand_new_production(user_text) and not _wants_production_resume(user_text):
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

    recovered = find_production(fresh)
    if not recovered:
        return None
    name, args = recovered
    # Never resurrect a prior one-still proof job unless the user asked for one still again.
    if name == "start_shortform_generate" and (
        args.get("visual_proof_only") or int(args.get("scene_count") or 0) == 1
    ):
        if not re.search(
            r"\b(?:one|1|single|first)\s+(?:still|scene|image|frame)\b|"
            r"\b(?:make|render|start|build)\s+(?:the\s+)?(?:very\s+)?(?:first|1st)\s+scene\b|"
            r"\bvisual\s+proof\b|\bproof\s+(?:still|image)\b|\btest\s+(?:still|image)\b",
            str(user_text or ""),
            re.I,
        ):
            return None
    # Title conflict with latest user request → do not recover stale args.
    if name == "start_shortform_generate":
        conflict = _shortform_action_conflicts_with_latest_user(
            args,
            {"session_id": session.get("session_id"), "messages": list(session.get("messages") or [])},
        )
        if conflict:
            return None
    return name, args


def _chosen_topic_from_user_text(user_text: str) -> str:
    text = " ".join(str(user_text or "").strip().split())
    if not text:
        return ""
    quoted = re.findall(r"['\"]([^'\"]{8,160})['\"]", text)
    if quoted:
        return quoted[-1].strip()
    match = re.search(
        r"\b(?:let'?s|lets|we should|go ahead and|please)?\s*"
        r"(?:do|make|produce|create|start|generate|render)\s+(.{8,180})$",
        text,
        re.IGNORECASE,
    )
    if match:
        topic = match.group(1).strip(" .!?")
        topic = re.sub(r"^(?:the\s+)?(?:video|short|long[- ]?form|render|production)\s+(?:for|about|on)\s+", "", topic, flags=re.IGNORECASE).strip()
        if topic.lower() not in {"it", "this", "that", "the first one", "first one", "option one"}:
            if store.is_production_commit_phrase(topic) or store.is_boilerplate_production_topic(topic):
                return ""
            return topic[:180]
    return ""


def _user_picked_recommended_option(user_text: str) -> bool:
    """True when the user explicitly chose a numbered option or asked to render one."""
    if _wants_production_execution(user_text) or store.is_explicit_production_request(user_text):
        return True
    low = store._user_message_before_attachments(user_text).lower()
    if re.search(r"\boption\s*(?:1|2|3|one|two|three)\b", low):
        return True
    if re.search(r"\b(?:first|second|third)\s+(?:one|option|short|video)\b", low):
        return True
    if re.search(r"\b(?:do|make|start|render|generate)\b.+\b(?:option|micro-short|series)\b", low):
        return True
    return False


def _last_recommended_topic(messages: list[dict[str, Any]]) -> str:
    for msg in reversed(messages or []):
        if str(msg.get("role") or "") != "assistant":
            continue
        content = str(msg.get("content") or "")
        matches = re.findall(r"\d+\.\s+['\"]([^'\"]{8,160})['\"]", content)
        if matches:
            return matches[0].strip()
        matches = re.findall(r"[-*]\s+['\"]([^'\"]{8,160})['\"]", content)
        if matches:
            return matches[0].strip()
    return ""


def _build_requested_topic_production(
    session: dict[str, Any],
    user_text: str,
    *,
    content_format: str,
    active_registry: str,
    active_channel_id: str = "",
    messages: list[dict[str, Any]] | None = None,
) -> tuple[str, dict[str, Any]] | None:
    if store.is_ideation_request(user_text):
        return None
    if _is_job_status_followup(user_text):
        return None
    topic = _explicit_title_candidate(user_text) or _chosen_topic_from_user_text(user_text)
    if topic and store.is_hard_production_commit(user_text):
        low_topic = str(topic).lower().strip()
        if (
            re.search(r"\b(?:first|1st)\s+scene\b", low_topic)
            and len(re.findall(r"[a-z0-9']+", low_topic)) <= 4
        ):
            topic = ""
    if not topic and messages and store.is_hard_production_commit(user_text):
        topic = store.resolve_current_production_target(session, messages)
    if not topic and _user_picked_recommended_option(user_text):
        topic = _last_recommended_topic(messages or [])
    if not topic or store.is_boilerplate_production_topic(topic):
        return None
    low = str(user_text or "").lower()
    fmt = str(content_format or "").strip().lower()
    from studio_agent.render_styles import resolve_render_style
    render_style = resolve_render_style(
        None,
        session_style=str(session.get("render_style") or "").strip() or None,
        user_text=user_text,
    ).key
    video_model = store.normalize_video_model(session.get("video_model"))
    is_long = (
        fmt == "long"
        or bool(re.search(r"\blong[- ]?form\b|\b\d+\s*(?:h|hr|hrs|hour|hours)\b|\b8\s*-\s*15\s*min|\bdocumentary\b", low))
        or str((session.get("pending_concept") or {}).get("format") or "").lower() == "longform"
    ) and "short" not in low
    if is_long:
        title = topic
        args = {
            "channel_key": active_registry or str(session.get("registry_key") or "").strip() or "default",
            "title": title[:120],
            "topic": topic,
            "render_style": render_style,
            "motion_policy": "balanced",
            "sfx_enabled": False,
            "background_music": "off",
        }
        return "start_longform_render", args

    # A previously selected Skeleton channel is context, never an invisible
    # instruction to turn an unrelated new request into a skeleton video.
    category_key = "science_technology" if ("day trad" in low or "market" in low or "finance" in low) else (
        "human_limits" if ("skele" in low or "psychology" in low) else "people_blogs"
    )
    visual_brief = (
        "Short-form YouTube psychology video. Keep the active channel identity locked, "
        "use clear visual metaphors for hidden behavior, fast hook pacing, high-contrast captions, "
        "and no unsupported analytics claims in on-screen text."
    )
    # One-scene visual proof only when the user EXPLICITLY asked for scene 1 / one still.
    # Never treat bare hard commits ("yes make it", "render that plan") as visual_proof_only —
    # that set scene_count=1 and the frontend hid the Approve card as "stale".
    one_scene_visual_request = bool(
        re.search(r"\b(?:exactly\s+)?(?:one|1|single)\s+(?:scene|still|image|frame)\b", low)
        or re.search(
            r"\b(?:make|render|start|build|produce|generate)\s+(?:the\s+)?(?:very\s+)?(?:first|1st)\s+scene\b|"
            r"\b(?:make|render|start|build|produce|generate)\s+scene\s*(?:#?\s*1|one)\b",
            low,
        )
    )
    visual_proof_only = (
        one_scene_visual_request
        or "first image" in low
        or "first still" in low
        or "visual proof" in low
        or "proof image" in low
        or "proof still" in low
        or "test still" in low
        or "test image" in low
        or "approve the look" in low
        or "approve of it being able to generate the entire short" in low
        or "animate that one scene" in low
        or "animate exactly one scene" in low
    )
    args = {
        "render_style": render_style,
        "category_key": category_key,
        "topic": topic,
        "title": topic[:120],
        "video_model": video_model,
        "visual_brief": visual_brief,
        "animate": False,
        "sfx_enabled": False,
        "background_music": "off",
        "user_request": user_text,
    }
    if visual_proof_only:
        args["scene_count"] = 1
        args["visual_proof_only"] = True
    else:
        # Full short: use pending concept / duration so Approve is a real multi-scene plan.
        pending_concept = session.get("pending_concept") if isinstance(session.get("pending_concept"), dict) else {}
        try:
            from studio_agent.concept_plan import parse_duration_sec, scene_count_for_duration

            duration_sec = int(
                pending_concept.get("duration_sec")
                or pending_concept.get("target_duration_sec")
                or parse_duration_sec(user_text, default_format="shortform")
                or 30
            )
            scenes = int(
                pending_concept.get("scene_count")
                or scene_count_for_duration(duration_sec, fmt="shortform")
            )
        except Exception:
            duration_sec = 30
            scenes = 6
        args["scene_count"] = max(4, min(12, scenes))
        args["target_duration_sec"] = max(8, min(120, duration_sec))
        args["visual_proof_only"] = False
    if active_channel_id:
        args["_selected_channel_id"] = active_channel_id
    if active_registry:
        args["_selected_registry_key"] = active_registry
    return "start_shortform_generate", args


def _needs_public_search_preflight(user_text: str) -> bool:
    low = str(user_text or "").lower()
    if store.is_exact_topic_discovery_request(user_text):
        return True
    if re.search(r"\b(?:fresh|live)\s+search\b", low):
        return True
    if re.search(r"\b\d{1,2}\s*(?:-|–|to)\s*\d{1,2}\s*days?\b", low):
        return True
    if "current demand" in low or "not stale" in low:
        return True
    if "verified" in low and any(term in low for term in ("demand", "search", "youtube", "views", "niche")):
        return True
    if store.is_explicit_tool_go_ahead(user_text) and re.search(r"\bsearch\b", low):
        return True
    followup_update_terms = (
        "same thing again",
        "do that again",
        "run it again",
        "rerun",
        "re-run",
        "updated data",
        "more updated data",
    )
    if any(term in low for term in followup_update_terms) and any(
        term in low
        for term in (
            "data",
            "youtube",
            "search",
            "trend",
            "topic",
            "fact",
            "updated",
        )
    ):
        return True
    demand_terms = (
        "what people are actually looking for",
        "what people are looking for",
        "search trend",
        "search trends",
        "public demand",
        "search demand",
        "youtube shorts",
        "people are searching",
        "people are currently searching",
        "people are actively typing",
        "currently searching",
        "actual youtube",
        "go on youtube",
        "look up",
        "live search",
        "trend data",
        "topic demand",
        "what people want to watch",
        "people want to watch",
        "verify all of your information",
        "verify your information",
        "fact-check",
        "fact check",
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
        "verify",
        "fact-check",
        "fact check",
    )
    if any(term in low for term in demand_terms) and any(term in low for term in action_terms):
        return True
    from studio_agent.turn_plan import is_public_youtube_demand_request

    if store.is_public_youtube_research_request(user_text):
        return True
    if is_public_youtube_demand_request(user_text):
        return True
    if store.is_ideation_request(user_text) and store.should_auto_run_tools(user_text):
        research_terms = (
            "market",
            "niche",
            "public",
            "trend",
            "research",
            "competitor",
            "demand",
            "youtube data",
            "search",
            "audience",
            "positioning",
        )
        if any(term in low for term in research_terms):
            return True
    return False


def _is_render_cost_question(user_text: str) -> bool:
    low = str(user_text or "").lower()
    # Cost words frequently occur inside documentary/reference titles (for
    # example, "Man's $250,000 Airline Pass Cost the Airline Over $20M").
    # A bare ``cost``/``$`` token must never divert that creative conversation
    # into the deterministic pricing preflight. Require an actual request for
    # pricing instead of keyword overlap with quoted/example material.
    asks_for_price = bool(re.search(
        r"\bhow much\b|"
        r"\bwhat(?:'s| is| will be| would be)? (?:the |this |that |it )?(?:cost|price|budget)\b|"
        r"\b(?:cost|price|pricing|budget) (?:estimate|estimation|breakdown|calculation|projection)\b|"
        r"\b(?:estimate|calculate|recalculate|project) (?:the |this |that |its )?(?:cost|price|budget)\b|"
        r"\b(?:cost|price) (?:for|of|to make|to produce|to render)\b|"
        r"\b(?:what|how) (?:does|would|will) (?:it|this|that|the (?:short|video|render)) cost\b|"
        r"\b(?:can|could|would) (?:you|studio) (?:price|cost|estimate|calculate)\b|"
        r"\bper short\b",
        low,
    ))
    if not asks_for_price:
        return False
    return any(
        term in low
        for term in (
            "short",
            "render",
            "produce",
            "video",
            "pipeline",
            "second",
            "animate",
            "i2v",
            "still",
            "scene",
            "trading",
        )
    )


def _latest_cost_estimate_payload(tool_fires: list[ToolFire]) -> dict[str, Any] | None:
    for fire in reversed(tool_fires or []):
        if str(fire.name or "") != "estimate_shortform_render_cost":
            continue
        try:
            data = json.loads(fire.result or "{}")
        except Exception:
            continue
        if isinstance(data, dict) and str(data.get("formatted_quote") or "").strip():
            return data
    return None


def _assistant_has_stale_cost_models(assistant_text: str, grounded: dict[str, Any]) -> bool:
    text = str(assistant_text or "").lower()
    image_model = str(grounded.get("image_model_id") or "").strip().lower()
    video_model = str(grounded.get("video_model") or "").strip().lower()
    stale_markers: list[str] = []
    if video_model.startswith("grok"):
        stale_markers.extend(("ltx", "seedance", "$0.02/sec", "0.02/sec", "pixverse"))
    if image_model.startswith("grok"):
        stale_markers.extend(("seedream t2i", "seedream45", "seedream_v45", "ernie-image", "ernie_image"))
    if stale_markers and any(marker in text for marker in stale_markers):
        return True
    grounded_labels = {
        production_budget._model_label(image_model),
        production_budget._model_label(video_model),
        image_model,
        video_model,
    }
    if any(label and label.lower() not in text for label in grounded_labels if label):
        costish = any(term in text for term in ("cost", "price", "pricing", "$", "breakdown", "/sec", "/image"))
        legacy = any(term in text for term in ("ltx", "seedream", "seedance", "ernie", "flux", "pixverse"))
        if costish and legacy:
            return True
    return False


def _format_grounded_cost_reply(grounded: dict[str, Any], *, user_text: str = "") -> str:
    """Build the only allowed cost answer from estimate_shortform_render_cost."""
    quote = str(grounded.get("formatted_quote") or "").strip()
    if not quote:
        return ""
    image_model = str(grounded.get("image_model_id") or "").strip()
    video_model = str(grounded.get("video_model") or "").strip()
    duration = grounded.get("duration_seconds")
    scenes = grounded.get("scene_count")
    total = grounded.get("total_estimated_usd")
    header_bits = []
    if duration is not None:
        try:
            header_bits.append(f"~{int(float(duration))}s short")
        except (TypeError, ValueError):
            pass
    if scenes is not None:
        try:
            header_bits.append(f"{int(scenes)} scene(s)")
        except (TypeError, ValueError):
            pass
    if image_model:
        header_bits.append(production_budget._model_label(image_model))
    if video_model:
        header_bits.append(production_budget._model_label(video_model))
    header = "Cost breakdown grounded in your active Studio session models"
    if header_bits:
        header = f"{header} ({', '.join(header_bits)})"
    lines = [f"**{header}**", ""]
    try:
        planned_scenes = int(scenes or 0)
    except (TypeError, ValueError):
        planned_scenes = 0
    if planned_scenes > 1:
        lines.extend([
            f"**Finished-short scope: {planned_scenes} scenes total.**",
            "The one still listed in the start stage is only Scene 1’s proof image; the remaining scenes stay deferred until that proof is approved.",
            "",
        ])
    lines.append(quote)
    full_projection = grounded.get("full_project_projection") if isinstance(grounded.get("full_project_projection"), dict) else {}
    if full_projection:
        lines.extend([
            "",
            f"**Projected finished-short provider cost: ${float(full_projection.get('projected_full_provider_cost_usd') or 0.0):.2f}**",
            f"Minimum customer price for {float(full_projection.get('target_gross_margin_pct') or 0.0):.0f}% gross margin: "
            f"${float(full_projection.get('minimum_customer_price_usd') or 0.0):.2f}",
        ])
    if total is not None:
        try:
            lines.extend(["", f"**Total grounded estimate: ${float(total):.2f}**"])
        except (TypeError, ValueError):
            pass
    lines.extend(
        [
            "",
            "These numbers come from `estimate_shortform_render_cost` using the image + i2v models "
            "currently selected in this chat (not training-memory pipeline defaults).",
        ]
    )
    # Soft creative nudge only; never invent alternate pipeline pricing.
    low = str(user_text or "").lower()
    if any(term in low for term in ("trading", "day trade", "psychology", "topic", "angle", "want")):
        lines.extend(
            [
                "",
                "If you want to produce it next, say the exact title/hook and whether you want a "
                "one-still visual proof first or the full short after approval.",
            ]
        )
    return "\n".join(lines)


def _recover_stale_cost_quote(
    assistant_text: str,
    *,
    tool_fires: list[ToolFire],
    preflight_tool_fires: list[ToolFire] | None = None,
    user_text: str = "",
    force: bool = False,
) -> str:
    """Replace cost breakdowns with grounded estimate tool output.

    When ``force`` is true (render-cost questions), always overwrite with the
    tool quote. Soft mode only rewrites when the assistant invents legacy models.
    Prefer the preflight estimate (session models) over a later LLM tool call that
    may have passed hallucinated model ids.
    """
    grounded = _latest_cost_estimate_payload(list(preflight_tool_fires or []))
    if not grounded:
        grounded = _latest_cost_estimate_payload(list(tool_fires or []))
    if not grounded:
        return assistant_text
    quote = str(grounded.get("formatted_quote") or "").strip()
    if not quote:
        return assistant_text
    if not force and not _assistant_has_stale_cost_models(assistant_text, grounded):
        # Even in soft mode, if there is no assistant text yet, use the grounded quote.
        if str(assistant_text or "").strip():
            return assistant_text
    return _format_grounded_cost_reply(grounded, user_text=user_text)


def _is_cost_only_question(user_text: str) -> bool:
    """True when the user is asking for price/cost, not ordering production to start now."""
    if not _is_render_cost_question(user_text):
        return False
    low = str(user_text or "").lower()
    start_now = (
        "start now",
        "make it now",
        "produce it now",
        "generate it now",
        "go ahead and make",
        "go ahead and produce",
        "approve and run",
        "start production",
        "start the short",
        "make the short now",
    )
    if any(term in low for term in start_now):
        return False
    return True


def _is_plan_readiness_question(user_text: str) -> bool:
    low = str(user_text or "").lower()
    return bool(re.search(
        r"\b(?:are|is) (?:we|it|this|the plan) (?:actually )?ready\b|"
        r"\bdoes this mean (?:we are|we'?re|it is|it'?s) ready\b",
        low,
    ))


def _wants_final_concept_card(user_text: str) -> bool:
    """Only expose the Plan card after an explicit end-of-planning statement."""
    low = str(user_text or "").lower().strip()
    if "?" in low and _is_plan_readiness_question(low):
        return False
    return bool(re.search(
        r"\b(?:i am|i'?m|we are|we'?re) (?:now )?fully ready\b|"
        r"\b(?:show|give) me (?:the )?(?:final|finished) plan\b|"
        r"\b(?:finalize|finish|lock) (?:the |this )?plan\b|"
        r"\bready (?:to review|for) (?:the )?(?:final )?plan\b",
        low,
    ))


def _wants_explicit_plan_artifact(user_text: str) -> bool:
    """Whether a Plan-mode turn explicitly asks Studio to draft or revise a plan.

    Plan & Conversation mode is conversation-first.  A creator saying "I like
    this", asking a question, or talking through an idea must reach the model
    as ordinary dialogue; it must not silently become a deterministic concept
    card.  This deliberately narrow guard only controls whether Studio stores
    a *plan artifact*.  It does not classify the user's broader intent or
    constrain the model's natural-language answer.
    """
    low = str(user_text or "").lower().strip()
    if not low:
        return False
    return bool(re.search(
        r"\b(?:show|give|create|make|draft|write|outline|map|build|update|revise|refine|"
        r"change|edit|lock|finalize)\b.{0,56}\b(?:plan|concept|outline|beat(?:\s*sheet)?|"
        r"chapter(?:\s*plan)?|story\s*arc)\b"
        r"|\b(?:plan|concept|outline|beat(?:\s*sheet)?|chapter(?:\s*plan)?|story\s*arc)\b"
        r".{0,56}\b(?:please|now|for me|again|instead|update|revise|refine|change|edit)\b",
        low,
        re.S,
    ))


def _short_scene_count_correction(user_text: str) -> int | None:
    low = str(user_text or "").lower()
    match = re.search(r"\b(?:need|want|plan(?:ned)?)\s+(\d{1,2})\s+scenes?\s+(?:total|overall)\b", low)
    if not match or not re.search(r"\b(?:why|saying|said|shows?|only)\b", low):
        return None
    return max(1, min(60, int(match.group(1))))


def _latest_explicit_longform_duration(session: dict[str, Any]) -> int | None:
    """Recover scope from user turns when a generic follow-up overwrote pending_concept."""
    messages = list(session.get("messages") or [])
    for row in reversed(messages):
        if not isinstance(row, dict) or str(row.get("role") or "") != "user":
            continue
        text = str(row.get("content") or "")
        for pattern, multiplier in (
            (r"\b(\d+(?:\.\d+)?)\s*(?:hours?|hrs?)\b", 3600),
            (r"\b(\d+(?:\.\d+)?)\s*(?:minutes?|mins?)\b", 60),
        ):
            match = re.search(pattern, text, re.I)
            if match:
                seconds = int(float(match.group(1)) * multiplier)
                if seconds >= 60:
                    return seconds
    return None


def _recover_pending_longform_concept(session: dict[str, Any], pending: dict[str, Any]) -> dict[str, Any]:
    recovered = dict(pending or {})
    explicit_duration = _latest_explicit_longform_duration(session)
    if explicit_duration:
        recovered["duration_sec"] = explicit_duration
    title = str(recovered.get("title") or "").strip()
    if not title or title.lower() in {"long-form concept", "longform concept", "untitled", "untitled long-form video"}:
        intent = session.get("conversation_intent") if isinstance(session.get("conversation_intent"), dict) else {}
        for key in ("locked_title", "working_title", "last_topic"):
            candidate = str(intent.get(key) or "").strip()
            if candidate:
                recovered["title"] = candidate[:140]
                recovered.setdefault("topic", candidate[:140])
                break
    return recovered


def _matching_expanded_shortform_job(
    session: dict[str, Any],
    *,
    title: str,
    target_scene_count: int,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Return an existing same-title short that already covers the plan.

    This prevents clicking/typing "render that plan" after expanding an
    approved one-scene proof from spawning a second proof job and hiding the
    completed six-scene production behind it.
    """
    wanted = str(title or "").strip()
    if not wanted:
        return None
    target_scenes = max(1, int(target_scene_count or 1))
    # Scene-1 visual proof must never resurrect a finished multi-scene short.
    if target_scenes <= 1:
        return None
    for job in reversed(list(session.get("active_jobs") or [])):
        if not isinstance(job, dict) or str(job.get("kind") or "shortform") != "shortform":
            continue
        job_title = str(job.get("title") or "").strip()
        if job_title and store._title_overlap_score(wanted, job_title) < 0.75:
            continue
        job_id = str(job.get("job_id") or "").strip()
        if not job_id:
            continue
        blocked = {
            str(value).strip()
            for value in (session.get("blocked_job_ids") or [])
            if str(value).strip()
        }
        if job_id in blocked:
            continue
        try:
            snapshot = get_job_snapshot(job_id, "shortform")
        except Exception:
            continue
        status = str(snapshot.get("status") or "").lower()
        if status in {"failed", "error", "cancelled", "missing"} or snapshot.get("error"):
            continue
        scenes = list(snapshot.get("scenes") or [])
        planned = int(snapshot.get("scene_count") or len(scenes) or 0)
        if max(planned, len(scenes)) >= max(1, int(target_scene_count or 1)):
            return job, snapshot
    return None


def _format_grounded_longform_cost_reply(
    estimate: production_budget.BudgetEstimate,
    *,
    readiness_requested: bool = False,
    plan_ready: bool = False,
) -> str:
    """Deterministic long-form quote; never let the chat model invent pricing."""
    data = dict(estimate.breakdown or {})
    nested = data.get("breakdown") if isinstance(data.get("breakdown"), dict) else {}
    scenes = int(data.get("n_scenes") or 0)
    chapters = int(data.get("n_chapters") or 0)
    image_model = production_budget._model_label(str(data.get("image_model") or ""))
    per_image = float(data.get("still_usd_per_image") or 0.0)
    stills = sum(float(value or 0.0) for key, value in nested.items() if str(key).startswith("stills_"))
    thumbnails = float(nested.get("thumbnails_seedream") or 0.0)
    narration = sum(float(value or 0.0) for key, value in nested.items() if "narration" in str(key))
    ambient = sum(float(value or 0.0) for key, value in nested.items() if "ambient" in str(key))
    paid_i2v = float(data.get("paid_i2v_usd") or 0.0)
    total = float(data.get("projected_full_project_usd") or data.get("all_in_usd") or data.get("total_usd") or 0.0)
    if scenes <= 0 or chapters <= 0 or not str(data.get("image_model") or "").strip() or total <= 0:
        readiness = (
            "No—the plan is still missing required scope/model details. "
            if readiness_requested else ""
        )
        return readiness + (
            "I can calculate this in Plan mode, but the current concept is missing a usable duration, "
            "channel pipeline, or selected image model. I have not started production. Tell me the "
            "target duration and image model, or select them in the composer, and I’ll calculate it here."
        )
    lines: list[str] = []
    if readiness_requested:
        lines.extend([
            (
                "**Yes—the plan has enough information to move to Production when you explicitly approve it.**"
                if plan_ready
                else "**Not yet—the plan still needs its missing scope/model details before production.**"
            ),
            "This cost check does not count as approval and does not start the video.",
            "",
        ])
    lines.extend([
        f"**Grounded long-form estimate — {scenes} stills across {chapters} chapter(s), using {image_model}**",
        "",
        f"- Still images: ${stills:.2f}" + (f" ({scenes} × ${per_image:.2f})" if scenes and per_image else ""),
        f"- Thumbnail candidates: ${thumbnails:.2f}",
        f"- Voiceover: ${narration:.2f}",
        f"- Ambient audio: ${ambient:.2f}",
        f"- Paid image-to-video: ${paid_i2v:.2f}",
        "",
        f"**Projected full-project provider cost: ${total:.2f}**",
        "",
        "This is a calculation only. I have not started production or changed the approved plan.",
    ])
    return "\n".join(lines)


def _longform_cost_channel_key(session: dict[str, Any]) -> str:
    raw = str(session.get("registry_key") or "").strip().lower()
    channel_label = str(session.get("channel_title") or session.get("channel_name") or "").strip().lower()
    if "history rewind" in channel_label or "history_rewind" in raw:
        return "history_rewind"
    if "empire magnates" in channel_label or "empire_magnates" in raw:
        return "empire_magnates"
    aliases = {
        "history_rewind_private": "history_rewind",
        "history-rewind": "history_rewind",
        "historyrewind": "history_rewind",
        "empire_magnates_private": "empire_magnates",
        "empire-magnates": "empire_magnates",
    }
    return aliases.get(raw, raw or "history_rewind")


def _cost_image_model(user_text: str, session_model: Any) -> str:
    """User's explicit model wording overrides a stale/blank picker for quotes."""
    low = str(user_text or "").lower()
    if re.search(r"\bgrok imagine\b|\bxai\b", low):
        return "seedream_edit"
    normalized = store.normalize_image_model(session_model)
    return normalized or store.DEFAULT_IMAGE_MODEL


def _parse_short_duration_seconds(user_text: str) -> float | None:
    raw = str(user_text or "")
    for pattern in (
        r"(\d{1,2})\s*[- ]?second",
        r"(\d{1,2})\s*sec\b",
        r"(\d{1,2})s\b",
    ):
        match = re.search(pattern, raw, flags=re.I)
        if match:
            try:
                return max(5.0, min(90.0, float(match.group(1))))
            except (TypeError, ValueError):
                continue
    return None


def _public_search_query_for_channel(active_label: str, user_text: str, *, registry_key: str = "") -> str:
    from studio_agent.turn_plan import (
        _compact_search_keywords,
        _niche_query_with_youtube_context,
        channel_fallback_search_query,
        coerce_public_search_query,
        extract_explicit_niche_label_from_user_text,
        extract_niche_terms_from_user_text,
        sanitize_public_search_query,
    )

    explicit_niche = extract_explicit_niche_label_from_user_text(user_text)
    if explicit_niche:
        sanitized = sanitize_public_search_query(
            _niche_query_with_youtube_context(explicit_niche, shorts=True),
            active_label=active_label,
        )
        if sanitized:
            return sanitized

    niche_phrases = extract_niche_terms_from_user_text(user_text)
    if niche_phrases:
        compact = sanitize_public_search_query(
            " ".join(_compact_search_keywords(niche_phrases, max_terms=8)),
            active_label=active_label,
        )
        if compact:
            return coerce_public_search_query(
                compact,
                user_text=user_text,
                active_label=active_label,
                registry_key=registry_key,
                fallback_query=channel_fallback_search_query(active_label, registry_key),
            )
    return channel_fallback_search_query(active_label, registry_key)


def _resolve_public_search_query(
    *,
    user_text: str,
    intent_text: str,
    messages: list[dict[str, Any]] | None,
    tool_fires: list[ToolFire] | None,
    active_label: str,
    active_registry: str,
) -> str:
    from studio_agent.turn_plan import (
        _niche_query_with_youtube_context,
        coerce_public_search_query,
        competitor_channel_search_query,
        derive_niche_search_query,
        extract_explicit_niche_label_from_user_text,
        sanitize_public_search_query,
    )

    fallback_query = _public_search_query_for_channel(
        active_label,
        intent_text or user_text,
        registry_key=active_registry,
    )
    existing = _latest_public_search_query(list(tool_fires or []))
    competitor_query = competitor_channel_search_query(intent_text or user_text)
    explicit_niche = extract_explicit_niche_label_from_user_text(intent_text or user_text)
    explicit_query = ""
    if explicit_niche:
        explicit_query = sanitize_public_search_query(
            _niche_query_with_youtube_context(explicit_niche, shorts=True),
            active_label=active_label,
        )
    ref_payload = _latest_complete_reference_analysis(tool_fires=tool_fires, messages=messages)
    preferred = (
        competitor_query
        or explicit_query
        or derive_niche_search_query(
            ref_payload,
            user_text=intent_text or user_text,
            active_label=active_label,
            fallback_query=fallback_query,
        )
        or fallback_query
        or (active_registry.replace("_", " ") if active_registry else "")
    )
    return coerce_public_search_query(
        existing or preferred,
        user_text=intent_text or user_text,
        active_label=active_label,
        registry_key=active_registry,
        fallback_query=fallback_query,
    )


def _append_late_public_search(
    tool_fires: list[ToolFire],
    *,
    user_text: str,
    intent_text: str,
    messages: list[dict[str, Any]] | None,
    active_label: str,
    active_registry: str,
    active_channel_id: str,
    user_id: str,
    content_format: str,
    session_id: str,
) -> str:
    """Run public search when research is missing or previous fires only returned hard errors."""
    if _has_successful_public_demand_tool(tool_fires):
        return _latest_public_search_query(tool_fires)

    search_query = _resolve_public_search_query(
        user_text=user_text,
        intent_text=intent_text,
        messages=messages,
        tool_fires=tool_fires,
        active_label=active_label,
        active_registry=active_registry,
    )
    if not search_query:
        return ""

    window_days = _public_search_window_days(intent_text or user_text)
    # On retry after a failed fire, force fresh so we don't re-serve a poisoned empty cache.
    force_fresh = _public_demand_needs_retry(tool_fires) and _has_public_demand_tool(tool_fires)
    fresh = force_fresh or _public_search_use_fresh(intent_text or user_text, public_demand=True)
    trend_args: dict[str, Any] = {
        "query": search_query,
        "days": window_days,
        "fresh": fresh,
    }
    if active_registry:
        trend_args["registry_key"] = active_registry
    if active_channel_id:
        trend_args["channel_id"] = active_channel_id

    # Single tool only — dual public search was a major quota burner.
    try:
        result = execute_tool_logged(
            "get_public_search_trends",
            trend_args,
            user_id=user_id,
            content_format=content_format,
            session_id=session_id,
        )
    except Exception as exc:
        result = json.dumps({"error": str(exc)}, indent=2)
    tool_fires.append(ToolFire("get_public_search_trends", dict(trend_args), result))
    if messages is not None:
        messages.append(_tool_observation_message("get_public_search_trends", result))
    return search_query


def _recover_public_search_denial(
    assistant_text: str,
    *,
    user_text: str,
    intent_text: str,
    messages: list[dict[str, Any]] | None,
    tool_fires: list[ToolFire],
    session: dict[str, Any],
    active_label: str,
    active_registry: str,
    active_channel_id: str,
    user_id: str,
    content_format: str,
    session_id: str,
    ideation_turn: bool = False,
    include_channel: bool = False,
) -> str:
    """Replace public-search capability denials with grounded hydrated demand evidence."""
    if not _assistant_denies_public_research_tool(assistant_text):
        return assistant_text

    search_query = _append_late_public_search(
        tool_fires,
        user_text=user_text,
        intent_text=intent_text,
        messages=messages,
        active_label=active_label,
        active_registry=active_registry,
        active_channel_id=active_channel_id,
        user_id=user_id,
        content_format=content_format,
        session_id=session_id,
    )
    if not _has_public_demand_tool(tool_fires):
        return assistant_text

    from studio_agent.conversation import deterministic_conversational_research_reply, strip_robot_research_artifacts

    ref_payload = _latest_complete_reference_analysis(tool_fires=tool_fires, messages=messages)
    if ideation_turn:
        return _grounded_ideation_research_from_tools(
            tool_fires,
            active_label=active_label,
            user_text=intent_text or user_text,
            has_reference_upload=bool(session.get("latest_attachment_paths")),
        )
    evidence = _grounded_research_summary_from_tools(
        tool_fires,
        active_label=active_label,
        user_text=intent_text or user_text,
        include_channel=include_channel,
        search_query=search_query or _latest_public_search_query(tool_fires),
        reference_payload=ref_payload,
    )
    ref_findings = _format_reference_analysis_findings(ref_payload) if ref_payload else ""
    return strip_robot_research_artifacts(
        deterministic_conversational_research_reply(
            user_text=intent_text or user_text,
            evidence=evidence,
            reference_findings=ref_findings,
            niche_hint=active_label or search_query or "",
        )
    )


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
        shortform_compare = data.get("shortform_performance_comparison") if isinstance(data.get("shortform_performance_comparison"), dict) else {}
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
        if shortform_compare:
            best_prior = shortform_compare.get("best_prior_short") if isinstance(shortform_compare.get("best_prior_short"), dict) else {}
            latest_short = shortform_compare.get("latest_short") if isinstance(shortform_compare.get("latest_short"), dict) else {}
            retention_winner = shortform_compare.get("retention_winner") if isinstance(shortform_compare.get("retention_winner"), dict) else {}
            views_winner = shortform_compare.get("views_winner") if isinstance(shortform_compare.get("views_winner"), dict) else {}
            available_short_metrics = [
                str(v).strip()
                for v in list(shortform_compare.get("available_short_metrics") or [])
                if str(v).strip()
            ]
            missing_short_metrics = [
                str(v).strip()
                for v in list(shortform_compare.get("missing_short_metrics") or [])
                if str(v).strip()
            ]
            lines.append(
                f"- Shorts comparison ready: {'yes' if shortform_compare.get('comparison_ready') else 'no'} "
                f"({int(shortform_compare.get('prior_short_count') or 0)} prior Shorts, "
                f"{int(shortform_compare.get('compared_row_count') or 0)} measured rows)"
            )
            if latest_short:
                lines.append(f"- Latest Short baseline: {_metric_row_line(latest_short).lstrip('- ')}")
            if retention_winner:
                lines.append(f"- Retention winner: {_metric_row_line(retention_winner).lstrip('- ')}")
            if views_winner:
                lines.append(f"- Views leader: {_metric_row_line(views_winner).lstrip('- ')}")
            if best_prior:
                lines.append(f"- Best prior Short control: {_metric_row_line(best_prior).lstrip('- ')}")
            if available_short_metrics:
                lines.append(f"- Shorts metrics available: {', '.join(available_short_metrics)}")
            if missing_short_metrics:
                lines.append(f"- Shorts metrics missing: {', '.join(missing_short_metrics)}")
            brief = str(shortform_compare.get("comparison_brief") or "").strip()
            if brief:
                lines.extend(["", "Authoritative Shorts comparison brief:", brief])
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
        shortform_compare = data.get("shortform_performance_comparison") if isinstance(data.get("shortform_performance_comparison"), dict) else {}
        video_rows = _channel_video_rows_from_metrics(metrics)
        insights = data.get("insights") if isinstance(data.get("insights"), dict) else {}
        return {
            "video_rows_available": int(quality.get("video_rows_available") or metrics.get("video_rows_available") or 0),
            "retention_rows_available": int(
                quality.get("retention_rows_available") or metrics.get("retention_rows_available") or 0
            ),
            "source": str(quality.get("effective_source") or "unknown").strip(),
            "oauth_connected": bool(quality.get("oauth_connected")),
            "video_rows": video_rows,
            "insights": insights,
            "shortform_performance_comparison": shortform_compare,
        }
    return {
        "video_rows_available": 0,
        "retention_rows_available": 0,
        "source": "unknown",
        "oauth_connected": False,
        "video_rows": [],
        "insights": {},
        "shortform_performance_comparison": {},
    }


def _latest_upload_from_tool_fires(tool_fires: list[ToolFire]) -> dict[str, Any]:
    for fire in reversed(tool_fires or []):
        if str(fire.name or "") != "get_channel_analytics":
            continue
        try:
            data = json.loads(fire.result or "{}")
        except Exception:
            data = {}
        if not isinstance(data, dict) or data.get("error"):
            continue
        latest = data.get("latest_upload") if isinstance(data.get("latest_upload"), dict) else {}
        if latest:
            return dict(latest)
        metrics = data.get("video_metrics") if isinstance(data.get("video_metrics"), dict) else {}
        latest = metrics.get("latest_upload") if isinstance(metrics.get("latest_upload"), dict) else {}
        if latest:
            return dict(latest)
    return {}


async def _run_current_video_analysis_preflight(
    *,
    emit: EventEmitter | None,
    user_id: str,
    content_format: str,
    session_id: str,
    messages: list[dict[str, Any]],
    tool_fires: list[ToolFire],
) -> None:
    latest = _latest_upload_from_tool_fires(tool_fires)
    url = str(latest.get("watch_url") or "").strip()
    video_id = str(latest.get("video_id") or "").strip()
    if not url and video_id:
        url = f"https://www.youtube.com/watch?v={video_id}"
    if not url:
        result = json.dumps({
            "error": "latest_upload_missing_watch_url",
            "message": "get_channel_analytics did not return a latest upload URL, so exact-video analysis could not start.",
            "latest_upload": latest,
        }, indent=2)
        tool_fires.append(ToolFire("analyze_reference_video", {"url": "", "content_format": content_format}, result))
        messages.append(_tool_observation_message("analyze_reference_video", result))
        store.update_session(session_id, messages=messages)
        return

    await _fire_event(emit, "tool_start", tool="analyze_reference_video", round=0, awaiting_approval=False)
    args = {"url": url, "content_format": "short" if content_format != "long" else "long", "max_frames": 40}
    try:
        start_result = execute_tool_logged(
            "analyze_reference_video",
            args,
            user_id=user_id,
            content_format=content_format,
            session_id=session_id,
        )
    except Exception as exc:
        start_result = json.dumps({"error": str(exc)}, indent=2)
    tool_fires.append(ToolFire("analyze_reference_video", dict(args), start_result))
    messages.append(_tool_observation_message("analyze_reference_video", start_result))
    store.update_session(session_id, messages=messages)
    await _fire_event(emit, "tool_end", tool="analyze_reference_video", status="ok")

    try:
        started = json.loads(start_result or "{}")
    except Exception:
        started = {}
    job_id = str(started.get("job_id") or "").strip() if isinstance(started, dict) else ""
    if not job_id:
        return

    final_poll = await _poll_competitor_reference_job(
        emit=emit,
        user_id=user_id,
        content_format=content_format,
        session_id=session_id,
        messages=messages,
        tool_fires=tool_fires,
        job_id=job_id,
    )

    if final_poll:
        messages.append({
            "role": "system",
            "content": (
                "[Current-video exact analysis preflight completed. The latest upload URL above was analyzed/polled "
                "before final synthesis. If status is not complete, state the exact stage/error instead of inferring missing analysis.]"
            ),
        })
        store.update_session(session_id, messages=messages)


async def _run_reference_stage_retry_preflight(
    *,
    emit: EventEmitter | None,
    user_id: str,
    content_format: str,
    session_id: str,
    messages: list[dict[str, Any]],
    tool_fires: list[ToolFire],
    existing: dict[str, Any],
    stages: list[str] | None = None,
) -> None:
    job_id = str(existing.get("job_id") or "").strip()
    if not job_id:
        return
    retry_stages = list(stages or _infer_reference_retry_stages(existing))
    await _fire_event(emit, "tool_start", tool="retry_reference_analysis", round=0, awaiting_approval=False)
    args = {"job_id": job_id, "stages": retry_stages}
    try:
        retry_result = execute_tool_logged(
            "retry_reference_analysis",
            args,
            user_id=user_id,
            content_format=content_format,
            session_id=session_id,
        )
    except Exception as exc:
        retry_result = json.dumps({"job_id": job_id, "status": "failed", "error": str(exc)}, indent=2)
    tool_fires.append(ToolFire("retry_reference_analysis", dict(args), retry_result))
    messages.append(_tool_observation_message("retry_reference_analysis", retry_result))
    store.update_session(session_id, messages=messages)
    await _fire_event(emit, "tool_end", tool="retry_reference_analysis", status="ok")
    try:
        refreshed = json.loads(retry_result or "{}")
    except Exception:
        refreshed = {}
    if isinstance(refreshed, dict) and refreshed:
        poll_args = {"job_id": job_id, "kind": "competitor"}
        poll_payload = json.dumps(refreshed, indent=2, ensure_ascii=False)
        tool_fires.append(ToolFire("poll_render_job", poll_args, poll_payload))
        messages.append(_tool_observation_message("poll_render_job", poll_payload))
        store.update_session(session_id, messages=messages)
        terminal_status = "ok" if _reference_poll_succeeded(refreshed) else "error"
        await _fire_event(emit, "tool_start", tool="poll_render_job", round=0, awaiting_approval=False)
        await _fire_event(emit, "tool_end", tool="poll_render_job", status=terminal_status)


async def _run_uploaded_reference_analysis_preflight(
    *,
    emit: EventEmitter | None,
    user_id: str,
    content_format: str,
    session_id: str,
    messages: list[dict[str, Any]],
    tool_fires: list[ToolFire],
) -> None:
    from studio_agent.attachments import resolve_video_attachment_path

    existing = _latest_complete_reference_analysis(tool_fires=tool_fires, messages=messages)
    latest_user = store._latest_user_text(messages, limit=3)
    if existing and not _user_wants_fresh_reference_analysis(latest_user):
        job_id = str(existing.get("job_id") or "").strip()
        depth = _reference_analysis_depth(existing)
        gaps = existing.get("analysis_gaps") if isinstance(existing.get("analysis_gaps"), dict) else {}
        stage_errors = gaps.get("stage_errors") if isinstance(gaps.get("stage_errors"), dict) else {}
        wants_retry = _user_wants_reference_stage_retry(latest_user)
        should_retry = bool(
            job_id
            and not _reference_retry_already_attempted(tool_fires=tool_fires, messages=messages, job_id=job_id)
            and (
                wants_retry
                or depth == "pacing_only"
                or (depth == "partial" and stage_errors)
            )
        )
        if should_retry:
            await _run_reference_stage_retry_preflight(
                emit=emit,
                user_id=user_id,
                content_format=content_format,
                session_id=session_id,
                messages=messages,
                tool_fires=tool_fires,
                existing=existing,
            )
            await _poll_competitor_reference_job(
                emit=emit,
                user_id=user_id,
                content_format=content_format,
                session_id=session_id,
                messages=messages,
                tool_fires=tool_fires,
                job_id=job_id,
                auto_retry_incomplete=False,
            )
            return
        poll_args = {"job_id": job_id, "kind": "competitor"} if job_id else {}
        result = json.dumps(existing, indent=2, ensure_ascii=False)
        tool_fires.append(ToolFire("poll_render_job", poll_args, result))
        messages.append(_tool_observation_message("poll_render_job", result))
        store.update_session(session_id, messages=messages)
        await _fire_event(emit, "tool_start", tool="poll_render_job", round=0, awaiting_approval=False)
        poll_status = "ok" if _reference_poll_succeeded(existing) else "error"
        await _fire_event(emit, "tool_end", tool="poll_render_job", status=poll_status)
        return

    local_path = resolve_video_attachment_path(session_id, user_id, messages=messages)
    if not local_path:
        result = json.dumps({
            "error": "uploaded_reference_missing",
            "message": "No uploaded reference video is available for analysis in this session.",
        }, indent=2)
        tool_fires.append(ToolFire("analyze_reference_video", {"local_path": ""}, result))
        messages.append(_tool_observation_message("analyze_reference_video", result))
        store.update_session(session_id, messages=messages)
        return

    await _fire_event(emit, "tool_start", tool="analyze_reference_video", round=0, awaiting_approval=False)
    args = {
        "local_path": local_path,
        "content_format": "short" if content_format != "long" else "long",
        "max_frames": 40,
    }
    try:
        start_result = execute_tool_logged(
            "analyze_reference_video",
            args,
            user_id=user_id,
            content_format=content_format,
            session_id=session_id,
        )
    except Exception as exc:
        start_result = json.dumps({"error": str(exc)}, indent=2)
    tool_fires.append(ToolFire("analyze_reference_video", dict(args), start_result))
    messages.append(_tool_observation_message("analyze_reference_video", start_result))
    store.update_session(session_id, messages=messages)
    await _fire_event(emit, "tool_end", tool="analyze_reference_video", status="ok")

    try:
        started = json.loads(start_result or "{}")
    except Exception:
        started = {}
    job_id = str(started.get("job_id") or "").strip() if isinstance(started, dict) else ""
    if not job_id:
        return

    final_poll = await _poll_competitor_reference_job(
        emit=emit,
        user_id=user_id,
        content_format=content_format,
        session_id=session_id,
        messages=messages,
        tool_fires=tool_fires,
        job_id=job_id,
    )

    if final_poll:
        messages.append({
            "role": "system",
            "content": (
                "[Uploaded-reference analysis preflight completed. The attached video above was analyzed/polled "
                "before final synthesis. Answer from visual_summary and pacing fields; do not claim the analysis "
                "is still queued if status is complete.]"
            ),
        })
        store.update_session(session_id, messages=messages)


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


def _topic_label(row: dict[str, Any]) -> str:
    return str(
        row.get("topic")
        or row.get("title")
        or row.get("prediction")
        or row.get("name")
        or ""
    ).strip()


def _is_meta_research_topic(label: str) -> bool:
    from studio_agent.turn_plan import is_meta_research_query, is_noise_search_token

    clean = re.sub(r"\s+", " ", str(label or "").strip())
    if not clean or is_meta_research_query(clean):
        return True
    tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9'\-]{2,}", clean)
    if tokens and all(is_noise_search_token(token) for token in tokens):
        return True
    if len(tokens) >= 2 and sum(1 for token in tokens if is_noise_search_token(token)) >= max(2, len(tokens) - 1):
        return True
    return False


def _catalyst_capture_turn(
    *,
    user_id: str,
    session: dict[str, Any],
    turn_kind: str,
    reference_payload: dict[str, Any] | None = None,
    tool_fires: list[ToolFire] | None = None,
    search_query: str = "",
    job_snapshot: dict[str, Any] | None = None,
) -> None:
    """Write Studio Agent outcomes into Catalyst learning + warm background caches."""
    try:
        from studio_agent import catalyst_learning, catalyst_runtime
        from studio_agent.catalyst_prediction import rank_next_video_candidates
        from studio_agent.turn_plan import derive_niche_search_query

        predicted: list[dict[str, Any]] = []
        if tool_fires:
            public_rows: list[dict[str, Any]] = []
            topic_rows: list[dict[str, Any]] = []
            for fire in tool_fires or []:
                if str(fire.name or "") not in {
                    "get_public_search_trends",
                    "search_youtube_public",
                    "recommend_video_topics",
                    "get_channel_analytics",
                }:
                    continue
                try:
                    payload = json.loads(fire.result or "{}")
                except Exception:
                    payload = {}
                if not isinstance(payload, dict):
                    continue
                videos = payload.get("videos") or payload.get("trending_sample")
                if isinstance(videos, list):
                    public_rows.extend([dict(row) for row in videos if isinstance(row, dict)])
                for key in ("recommended_topics", "predicted_topics"):
                    recs = payload.get(key)
                    if isinstance(recs, list):
                        topic_rows.extend([dict(row) for row in recs if isinstance(row, dict)])
            predicted = rank_next_video_candidates(
                reference_payload=reference_payload,
                public_rows=public_rows,
                predicted_topics=topic_rows,
                search_query=search_query,
            )

        catalyst_learning.record_turn_outcome(
            str(user_id),
            session,
            turn_kind=turn_kind,
            reference_payload=reference_payload,
            tool_fires=tool_fires,
            search_query=search_query,
            predicted_topics=predicted,
            job_snapshot=job_snapshot,
        )
        # Continuous evaluation is operational evidence, not model training.
        # It turns repeated verified failures into release-blocking regression
        # cases while preserving only compact, non-sensitive context.
        from studio_agent import continuous_evaluation

        snapshot = dict(job_snapshot or {})
        failure = str(
            snapshot.get("error") or snapshot.get("message") or snapshot.get("note") or ""
        ).strip()
        visual_qa = snapshot.get("visual_qa") if isinstance(snapshot.get("visual_qa"), dict) else {}
        if not failure and str(visual_qa.get("status") or "").lower() == "fail":
            failure = str(visual_qa.get("summary") or "visual QA failed")
        kind = str(turn_kind or "")
        continuous_evaluation.record_evidence(
            session=session,
            event_type=kind,
            outcome=("failure" if failure else "success" if kind == "production_job" else "neutral"),
            evidence={
                "job_id": str(snapshot.get("job_id") or "")[:80],
                "status": str(snapshot.get("status") or "")[:60],
                "failure": failure[:280],
            },
        )

        # Persist Catalyst notes into session conversation_intent for Grok-class continuity.
        try:
            from studio_agent.conversation import (
                merge_catalyst_into_intent,
                update_conversation_intent,
            )
            from studio_agent.turn_plan import extract_known_niche_phrase

            niche_label = extract_known_niche_phrase(search_query) or str(search_query or "")[:80]
            intent = update_conversation_intent(
                session,
                niche=niche_label,
                search_query=str(search_query or ""),
                channel_title=str(session.get("channel_title") or ""),
                channel_id=str(session.get("channel_id") or ""),
                registry_key=str(session.get("registry_key") or ""),
                kind=str(turn_kind or ""),
            )
            notes: list[str] = []
            if predicted:
                top = predicted[0] if isinstance(predicted[0], dict) else {}
                topic = str(top.get("topic") or "").strip()
                locked = str(intent.get("locked_title") or intent.get("working_title") or "").strip()
                if topic:
                    if locked and topic.lower() != locked.lower():
                        notes.append(
                            f"Competitor/research angle (NOT our locked title \"{locked}\"): {topic[:100]}"
                        )
                    else:
                        notes.append(f"Strongest next test after this turn: {topic[:120]}")
                        # Only write last_topic from catalyst when user has not locked a title.
                        if not locked:
                            intent["last_topic"] = topic[:160]
            if job_snapshot and str(job_snapshot.get("status") or "") == "complete":
                notes.append("Last production completed successfully — reuse packaging that worked.")
            intent = merge_catalyst_into_intent(intent, predicted_topics=predicted, notes=notes)
            sid = str(session.get("session_id") or "").strip()
            if sid:
                store.update_session(sid, conversation_intent=intent)
            else:
                session["conversation_intent"] = intent
            # Durable user memory so the next chat still knows the niche.
            if niche_label and str(user_id or "").strip():
                try:
                    from studio_agent import memory as _mem

                    _mem.remember(
                        str(user_id),
                        f"Active content niche: {niche_label}",
                        kind="niche",
                        source="live_demand",
                        importance=4,
                        channel_id=str(session.get("channel_id") or ""),
                        registry_key=str(session.get("registry_key") or ""),
                        title=str(session.get("channel_title") or ""),
                        scope="channel" if session.get("channel_id") or session.get("registry_key") else "global",
                    )
                except Exception:
                    pass
        except Exception:
            pass

        warm_query = str(search_query or "").strip()
        if not warm_query and reference_payload:
            active_label = (
                str(session.get("channel_title") or "").strip()
                or str(session.get("registry_key") or "").replace("_", " ")
                or "YouTube"
            )
            warm_query = derive_niche_search_query(reference_payload, active_label=active_label)
        if warm_query:
            catalyst_runtime.schedule_session_catalyst_warm(
                user_id=str(user_id),
                channel_id=str(session.get("channel_id") or ""),
                search_query=warm_query,
            )
            try:
                catalyst_runtime.record_runtime_learning_event(
                    kind=str(turn_kind or "agent_turn"),
                    query=warm_query,
                    detail=f"Studio Agent turn wrote Catalyst learning ({turn_kind})",
                    metadata={"predicted": len(predicted)},
                )
            except Exception:
                pass
    except Exception:
        pass


def _public_video_line(row: dict[str, Any]) -> str:
    title = str(row.get("title") or row.get("video_id") or "Untitled video").strip()
    stats: list[str] = []
    channel = str(row.get("channel_title") or row.get("channel") or "").strip()
    views = _safe_int(row.get("views", row.get("view_count", 0)))
    likes = _safe_int(row.get("likes", row.get("like_count", 0)))
    published = str(row.get("published_at") or "").strip()
    support = str(row.get("support_label") or row.get("cache_status") or "").strip()
    if channel:
        stats.append(channel)
    if views:
        stats.append(f"{views:,} views")
    if likes:
        stats.append(f"{likes:,} likes")
    if published:
        stats.append(f"published {published[:10]}")
    engagement = row.get("engagement_rate")
    if engagement is not None:
        stats.append(f"engagement {float(engagement) * 100:.2f}%")
    views_per_day = row.get("views_per_day")
    if views_per_day is not None:
        stats.append(f"{float(views_per_day):,.0f} views/day")
    profile = str(row.get("search_profile") or "").strip()
    if profile:
        stats.append(profile.replace("_", " "))
    if support:
        stats.append(support)
    return f"- {title}" + (f": {'; '.join(stats)}" if stats else "")


def _grounded_research_summary_from_tools(
    tool_fires: list[ToolFire],
    *,
    active_label: str,
    user_text: str = "",
    include_channel: bool = True,
    search_query: str = "",
    reference_payload: dict[str, Any] | None = None,
) -> str:
    """Deterministic fallback for fact-check / topic-demand turns."""
    status = ""
    evidence: dict[str, Any] = {}
    channel_rows: list[Any] = []
    best_row = None
    if include_channel:
        status = _grounded_channel_status_from_tools(tool_fires, active_label=active_label)
        evidence = _latest_channel_analytics_evidence(tool_fires)
        channel_rows = evidence.get("video_rows") if isinstance(evidence.get("video_rows"), list) else []
        best_row = _best_retention_row(channel_rows)

    recommended: list[dict[str, Any]] = []
    public_rows: list[dict[str, Any]] = []
    saw_fresh = False
    quota_exhausted = False
    quota_note = ""
    for fire in tool_fires or []:
        if str(fire.name or "") not in {
            "recommend_video_topics",
            "get_public_search_trends",
            "search_youtube_public",
        }:
            continue
        try:
            payload = json.loads(fire.result or "{}")
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            continue
        err_text = str(payload.get("error") or "")
        if (
            payload.get("youtube_quota_exhausted")
            or "youtube_quota_exhausted" in err_text.lower()
            or "quota exceeded" in err_text.lower()
            or "past daily cap" in err_text.lower()
        ):
            quota_exhausted = True
            quota_note = err_text[:240] or "YouTube Data API daily search budget is spent."
        if payload.get("error") and not payload.get("videos"):
            continue
        saw_fresh = saw_fresh or bool(payload.get("fresh") or payload.get("fresh_public_search"))
        for key in ("recommended_topics", "predicted_topics"):
            recs = payload.get(key)
            if isinstance(recs, list):
                recommended.extend([dict(row) for row in recs if isinstance(row, dict)])
        videos = payload.get("videos") or payload.get("trending_sample")
        if isinstance(videos, list):
            public_rows.extend([dict(row) for row in videos if isinstance(row, dict)])

    seen_topics: set[str] = set()
    topic_lines: list[str] = []
    for row in recommended:
        label = _topic_label(row)
        key = label.lower()
        if not label or key in seen_topics or _is_meta_research_topic(label):
            continue
        seen_topics.add(key)
        reason = str(row.get("reason") or row.get("why") or row.get("evidence") or row.get("support_label") or "").strip()
        topic_lines.append(f"- {label}" + (f" — {reason}" if reason else ""))
        if len(topic_lines) >= 6:
            break

    deduped_public_rows: list[dict[str, Any]] = []
    seen_videos: set[str] = set()
    for row in public_rows:
        key = str(row.get("video_id") or row.get("url") or row.get("title") or "").strip().lower()
        if not key or key in seen_videos:
            continue
        seen_videos.add(key)
        deduped_public_rows.append(row)

    def _support_sort_rank(label: str) -> int:
        clean = str(label or "").strip().lower()
        if clean.startswith("strong"):
            return 0
        if clean.startswith("supported"):
            return 1
        if clean.startswith("weak"):
            return 2
        return 3

    # Prefer-recent from user language OR from tool payload windows (Live Demand
    # often uses days=1–2 without repeating "24 hours" in the user text).
    tool_window_days = 0
    for fire in tool_fires or []:
        try:
            payload = json.loads(fire.result or "{}")
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            continue
        try:
            tool_window_days = max(tool_window_days, int(payload.get("window_days") or 0))
        except (TypeError, ValueError):
            pass
        for row in list(payload.get("videos") or []):
            if not isinstance(row, dict):
                continue
            try:
                tool_window_days = max(tool_window_days, int(row.get("query_window_days") or 0))
            except (TypeError, ValueError):
                pass
    prefer_recent_sort = bool(
        re.search(r"\b(?:12|24)\s*hours?\b", str(user_text or "").lower())
        or re.search(r"\blast\s+[1-7]\s*days?\b", str(user_text or "").lower())
        or "24/7" in str(user_text or "").lower()
        or "market moves" in str(user_text or "").lower()
        or "right now" in str(user_text or "").lower()
        or "go viral" in str(user_text or "").lower()
        or "live demand" in str(user_text or "").lower()
        or "fresh" in str(user_text or "").lower()
        or (0 < tool_window_days <= 7)
    )

    def _public_row_sort_key(row: dict[str, Any]) -> tuple[int, int, int, float]:
        profile = str(row.get("search_profile") or "").strip().lower()
        # Live Demand / short windows: recent momentum first, not 2022 100M-view toys.
        if prefer_recent_sort:
            if profile == "recent_momentum":
                profile_rank = 0
            elif profile == "recent_momentum_derived":
                profile_rank = 1
            elif profile == "top_performers":
                profile_rank = 2
            else:
                profile_rank = 3
            age = float(row.get("age_days") or 999)
            return (
                profile_rank,
                _support_sort_rank(str(row.get("support_label") or "")),
                int(age * 10),  # newer first
                -float(row.get("views_per_day") or 0),
            )
        if profile == "top_performers":
            profile_rank = 0
        elif profile == "recent_momentum_derived":
            profile_rank = 1
        elif profile == "recent_momentum":
            profile_rank = 2
        else:
            profile_rank = 3
        return (
            _support_sort_rank(str(row.get("support_label") or "")),
            profile_rank,
            -int(row.get("views") or 0),
            -float(row.get("views_per_day") or 0),
        )

    from studio_agent.catalyst_prediction import (
        _channel_niche_keywords,
        _niche_relevance_score,
        filter_public_rows_for_query,
    )
    from studio_agent.turn_plan import (
        coerce_public_search_query,
        is_allowed_discovery_search_query,
        is_banned_faceless_hooks_query,
        is_garbage_public_search_query,
        is_unusable_public_search_query,
        refine_public_search_query,
    )

    raw_search_query = str(search_query or "").strip()
    if raw_search_query and is_allowed_discovery_search_query(raw_search_query):
        coerced_query = raw_search_query
    elif (
        raw_search_query
        and not is_banned_faceless_hooks_query(raw_search_query)
        and not is_garbage_public_search_query(raw_search_query)
        and not is_unusable_public_search_query(raw_search_query)
    ):
        # Keep the tool's actual q for display/filter when it is already a real niche seed.
        coerced_query = refine_public_search_query(raw_search_query) or raw_search_query
    else:
        coerced_query = coerce_public_search_query(
            raw_search_query,
            user_text=user_text,
            active_label=active_label,
            fallback_query=_public_search_query_for_channel(active_label, user_text),
        )
        coerced_query = refine_public_search_query(coerced_query) or coerced_query
    # Prefer original tool query for the label when it was usable (avoid "self improvement"
    # wiping a more specific "psychology self improvement shorts" display).
    if (
        raw_search_query
        and not is_banned_faceless_hooks_query(raw_search_query)
        and not is_garbage_public_search_query(raw_search_query)
        and not is_unusable_public_search_query(raw_search_query)
    ):
        query_label = raw_search_query
    else:
        query_label = coerced_query or active_label
    # Root-level filter: every public row must match the niche query intent
    # (fidget/toy "trading" never reaches predicted moves or evidence bullets).
    deduped_public_rows = filter_public_rows_for_query(
        deduped_public_rows,
        search_query=coerced_query,
        user_text=user_text or "",
    )
    recommended = [
        row
        for row in recommended
        if isinstance(row, dict)
        and filter_public_rows_for_query(
            [{"title": str(row.get("topic") or row.get("title") or "")}],
            search_query=coerced_query,
            user_text=user_text or "",
        )
    ]
    channel_insights_for_filter = evidence.get("insights") if isinstance(evidence.get("insights"), dict) else {}
    channel_titles_for_filter = [
        str(row.get("title") or "")
        for row in list(channel_insights_for_filter.get("top_titles") or [])
        if isinstance(row, dict) and str(row.get("title") or "").strip()
    ]
    for row in channel_rows:
        if not isinstance(row, dict):
            continue
        title = str(row.get("title") or "").strip()
        if title and title not in channel_titles_for_filter:
            channel_titles_for_filter.append(title)
    # Channel titles define the niche filter. The search query itself is NOT a filter —
    # results of a niche query are already for that niche. Filtering query-against-query
    # after stripping domain tokens (psychology/trading/…) falsely emptied every row.
    niche_keywords_for_filter = _channel_niche_keywords(channel_insights_for_filter, "")
    has_channel_niche_signal = bool(channel_titles_for_filter or niche_keywords_for_filter)

    def _row_matches_channel_niche(row: dict[str, Any]) -> bool:
        # No private channel signature → trust public rows for the niche query.
        if not has_channel_niche_signal:
            return True
        title = str(row.get("title") or "").strip()
        if not title:
            return False
        # Prefer channel-title signature; also score against query as soft secondary.
        score = _niche_relevance_score(
            title,
            niche_keywords=niche_keywords_for_filter,
            channel_titles=channel_titles_for_filter,
        )
        if score >= 0.12:
            return True
        # Soft secondary: title aligns with the niche search query (domain words count)
        if coerced_query:
            return _niche_relevance_score(
                title,
                niche_keywords=[coerced_query],
                channel_titles=[],
            ) >= 0.20
        return False

    niche_relevant_public_rows = [
        row for row in deduped_public_rows if isinstance(row, dict) and _row_matches_channel_niche(row)
    ]
    if niche_relevant_public_rows:
        public_rows_for_display = niche_relevant_public_rows
    elif has_channel_niche_signal:
        # Channel connected but public winners disagree — surface nothing as "on-channel",
        # but still keep query-aligned rows for niche read (not blank).
        query_aligned = [
            row
            for row in deduped_public_rows
            if isinstance(row, dict)
            and coerced_query
            and _niche_relevance_score(
                str(row.get("title") or ""),
                niche_keywords=[coerced_query],
                channel_titles=[],
            )
            >= 0.15
        ]
        public_rows_for_display = query_aligned or list(deduped_public_rows)
    else:
        # Pure public niche research (no channel analytics) — show search results.
        public_rows_for_display = list(deduped_public_rows)

    deduped_public_rows.sort(key=_public_row_sort_key)
    public_rows_for_display.sort(key=_public_row_sort_key)
    recent_rows = [
        row for row in public_rows_for_display
        if str(row.get("search_profile") or "") in {"recent_momentum", "recent_momentum_derived"}
    ]
    supported_rows = [
        row for row in public_rows_for_display
        if str(row.get("support_label") or "") in {"strong_public_precedent", "supported_public_precedent"}
    ]
    weak_rows = [
        row for row in public_rows_for_display
        if str(row.get("support_label") or "") == "weak_public_precedent"
    ]
    exploratory_rows = [
        row for row in public_rows_for_display
        if str(row.get("support_label") or "") == "exploratory_public_signal"
        or (
            str(row.get("evidence_level") or "") == "hydrated_video_stats"
            and int(row.get("views") or 0) > 0
            and str(row.get("support_label") or "") not in {
                "strong_public_precedent",
                "supported_public_precedent",
                "weak_public_precedent",
            }
        )
    ]
    # Always surface real hydrated rows so Live Demand never looks "dead" when
    # the API returned data — just label support honestly.
    display_seed = supported_rows[:4] or weak_rows[:4] or exploratory_rows[:4]
    display_rows = (display_seed + [
        row for row in public_rows_for_display
        if row not in display_seed
        and str(row.get("evidence_level") or "") == "hydrated_video_stats"
    ])[:6]
    public_lines = [_public_video_line(row) for row in display_rows[:6]]
    # Only true "off niche" when channel titles exist AND zero rows match either channel OR query.
    public_results_off_niche = bool(
        deduped_public_rows
        and not public_rows_for_display
        and has_channel_niche_signal
        and not niche_relevant_public_rows
    )
    channel_mismatch_but_query_ok = bool(
        has_channel_niche_signal
        and deduped_public_rows
        and public_rows_for_display
        and not niche_relevant_public_rows
    )

    channel_lines: list[str] = []
    if channel_rows:
        channel_lines.extend(_metric_row_line(row) for row in channel_rows[:4])
    elif best_row:
        channel_lines.append(_metric_row_line(best_row))
    if include_channel:
        lines = [
            f"I verified public YouTube demand for `{query_label}` and cross-checked selected-channel data where available.",
            "",
            "Selected-channel evidence:",
            status or "- No selected-channel analytics summary was available (connect YouTube OAuth or pick a channel).",
        ]
        if channel_lines:
            lines.extend(["", "Returned selected-channel video rows:", *channel_lines])
    else:
        lines = [
            f"I verified public YouTube demand for niche query `{query_label}`.",
        ]
    lines.extend([
        "",
        "Public YouTube demand evidence:",
    ])
    if public_lines:
        lines.extend(public_lines)
        if not supported_rows and not weak_rows and exploratory_rows:
            lines.append(
                "- Note: these are exploratory / low-signal rows (short window or sub-threshold views). "
                "Use them as test angles, not as proven viral precedents."
            )
        if channel_mismatch_but_query_ok:
            lines.append(
                "- Note: these public rows match the niche query, but few overlap your connected channel's "
                "title signature. Treat them as niche demand, not as your channel's past winners."
            )
    elif public_results_off_niche:
        lines.append(
            "- Public search returned rows that do not overlap your connected channel's title patterns. "
            "Showing none as channel-matched precedent; re-run with a clearer niche query if needed."
        )
    elif hydrated_public := [
        row for row in deduped_public_rows
        if str(row.get("evidence_level") or "") == "hydrated_video_stats"
    ]:
        # Fallback should almost never hit now that we surface exploratory rows.
        sample = ", ".join(
            f"{str(r.get('title') or '')[:48]} ({int(r.get('views') or 0):,} views)"
            for r in hydrated_public[:3]
        )
        lines.append(
            f"- Public search hydrated {len(hydrated_public)} videos; none met strong/supported bars yet. "
            f"Highest signals: {sample}."
        )
    else:
        if quota_exhausted:
            try:
                import youtube_quota

                snap = youtube_quota.snapshot_sync()
                lines.append(
                    f"- **youtube_quota_exhausted**: YouTube Data API daily search budget is spent "
                    f"({snap.get('total_spent')}/{snap.get('daily_cap')} units). "
                    f"{snap.get('reset_hint') or 'Resets around midnight Pacific Time.'} "
                    "Do not invent view counts or trends."
                )
                if quota_note:
                    lines.append(f"- Tool note: {quota_note}")
            except Exception:
                lines.append(
                    "- **youtube_quota_exhausted**: YouTube Data API daily search budget is spent. "
                    "Quota resets around midnight Pacific Time. Do not invent view counts or trends."
                )
        else:
            lines.append(
                "- Public search completed, but no verified public video stats came back yet. "
                "I won't claim a trend from search snippets alone."
            )

    hydrated_public = [
        row for row in deduped_public_rows
        if str(row.get("evidence_level") or "") == "hydrated_video_stats"
    ]
    supported_public = [
        row for row in hydrated_public
        if str(row.get("support_label") or "") in {"strong_public_precedent", "supported_public_precedent"}
    ]
    if hydrated_public:
        hydrated_count = len(hydrated_public)
        supported_count = len(supported_public)
        momentum_window_days = [
            int(row.get("query_window_days") or 0)
            for row in deduped_public_rows
            if isinstance(row, dict)
            and str(row.get("search_profile") or "") in {"recent_momentum", "recent_momentum_derived"}
            and int(row.get("query_window_days") or 0) > 0
        ]
        performer_window_days = [
            int(row.get("query_window_days") or 0)
            for row in deduped_public_rows
            if isinstance(row, dict)
            and str(row.get("search_profile") or "") == "top_performers"
            and int(row.get("query_window_days") or 0) > 0
        ]
        momentum_window = max(momentum_window_days) if momentum_window_days else 30
        performer_window = max(performer_window_days) if performer_window_days else 0
        # Prefer-recent Live Demand must never claim an all-time historical top-performer pass.
        effective_window = max(momentum_window, tool_window_days or 0, performer_window or 0)
        if prefer_recent_sort or (0 < effective_window <= 7):
            cap = max(effective_window, momentum_window or 1, 1)
            performer_label = f"last {cap} days (order=viewCount, prefer-recent — not all-time)"
            if momentum_window <= 0:
                momentum_window = cap
        elif performer_window <= 0:
            performer_label = "all-time (order=viewCount, no publishedAfter cap)"
        else:
            performer_label = f"last {performer_window} days (order=viewCount)"
        lines.extend([
            "",
            "Public search coverage:",
            (
                f"- {hydrated_count} public videos returned with verified view/engagement stats"
                + (f"; {supported_count} strong enough to cite as precedent" if supported_count else ".")
            ),
            (
                f"- Search method: live YouTube Data API — recent momentum last {momentum_window} days "
                f"(order=date) + top performers {performer_label}, "
                "then hydrated via videos.list."
                if saw_fresh
                else (
                    f"- Search method: cached public search — recent momentum last {momentum_window} days; "
                    f"top performers {performer_label}."
                )
            ),
            (
                f"- Recent-momentum rows available: {len(recent_rows)}."
                if recent_rows
                else "- Recent-momentum rows available: 0 (rerun with fresh=true if the niche window is too thin)."
            ),
        ])

    from studio_agent.catalyst_prediction import format_prediction_lines, rank_next_video_candidates

    channel_insights = evidence.get("insights") if isinstance(evidence.get("insights"), dict) else {}
    channel_video_rows = evidence.get("video_rows") if isinstance(evidence.get("video_rows"), list) else []
    ranked_moves = rank_next_video_candidates(
        reference_payload=reference_payload,
        public_rows=public_rows_for_display or deduped_public_rows,
        predicted_topics=recommended,
        channel_insights=channel_insights,
        channel_video_rows=channel_video_rows,
        search_query=coerced_query,
    )
    prediction_lines = format_prediction_lines(ranked_moves)

    # Internal grounding only — conversation layer must never ship these as score dumps.
    lines.extend(["", "Candidate angles (internal — do not paste scores to the user):"])
    if prediction_lines:
        lines.extend(prediction_lines)
        if any(str(row.get("source") or "") == "channel_winner" for row in ranked_moves):
            lines.append(
                "- Moves above prioritize your connected-channel retention winners first; "
                "public psychology outliers stay in the evidence section unless they match your title patterns."
            )
        elif supported_public and not any(
            str(row.get("source") or "") == "public_demand" for row in ranked_moves
        ):
            lines.append(
                "- Public window was thin on proven precedents; moves above lean on reference storytelling "
                "and niche-fit scoring until stronger hydrated winners appear."
            )
    elif not supported_public:
        if prefer_recent_sort or (0 < tool_window_days <= 7):
            if exploratory_rows or weak_rows:
                lines.append(
                    "- No strong (supported+) public precedents in this short window yet — "
                    "moves above (if any) are exploratory tests from hydrated recent rows, not 100k viral proof. "
                    "Widen to 7d or connect channel analytics for stronger winners."
                )
            else:
                lines.append(
                    "- Short recency window returned hydrated rows but none scored as weak/exploratory test angles. "
                    "Studio stays recent-first (no all-time toy/viral backfill). "
                    "Widen to 7 days, connect channel analytics, or re-run after more niche uploads land."
                )
        else:
            lines.append(
                "- No strong public precedents cleared the support bar for this niche query. "
                "If rows are still sub-threshold after the top-performer pass, tighten the niche query "
                "or use channel analytics."
            )
    elif topic_lines:
        lines.extend(topic_lines)
    elif best_row:
        lines.append(f"- Build from the strongest selected-channel row: {str(best_row.get('title') or 'selected video').strip()}.")
    else:
        lines.append("- Hold topic picks until public search returns enough verified video stats.")

    lines.extend([
        "",
        "What is confirmed vs blocked:",
        f"- Confirmed: public search {'used fresh data' if saw_fresh else 'ran with the available search/cache path'}.",
        "- Blocked: do not reuse old viral view-count claims unless the current public rows cite the exact title, channel, views, date, and support label.",
    ])
    if prefer_recent_sort and not supported_public:
        lines.append(
            "- Note: short recency window (12–24h / 1–2d) often returns thin hydrated stats. "
            "That is honest scarcity, not proof the niche is dead — widen to 7d or re-run after more uploads land."
        )
    if include_channel:
        lines.insert(
            len(lines) - 2,
            f"- Confirmed: private channel analytics are {'connected' if evidence.get('oauth_connected') else 'not connected'} and source is {evidence.get('source') or 'unknown'}.",
        )
    elif not channel_titles_for_filter and not (active_label and str(active_label).lower() not in {"youtube", "selected", ""}):
        lines.append(
            "- Tip: select a YouTube channel (SELECT CHANNEL) so next-video moves can prioritize your retention winners, not only public niche demand."
        )
    return "\n".join(lines)


def _prepare_shortform_production_args(
    args: dict[str, Any],
    session: dict[str, Any],
    *,
    messages: list[dict[str, Any]] | None = None,
    force_fresh: bool = False,
) -> dict[str, Any]:
    session_messages = list(messages if messages is not None else session.get("messages") or [])
    latest = store._latest_user_text(session_messages, limit=2)
    merged = _inject_shortform_render_style(args, session, user_text=latest)
    merged = _inject_shortform_image_model(merged, session)
    merged = _inject_shortform_video_model(merged, session)
    merged = _inject_shortform_caption_options(merged, session)
    merged = _inject_shortform_live_demand(merged, session)
    merged = _normalize_shortform_category_args(merged)
    merged = _apply_locked_title_to_production_args(merged, session, user_text=latest)
    merged = _force_production_title_on_args(merged, session=session, user_text=latest)
    # Honor explicit "30 seconds" / "1 minute" from the latest commit into scene_count.
    try:
        from studio_agent.concept_plan import (
            _DURATION_RE,
            _MINUTE_RE,
            parse_duration_sec,
            scene_count_for_duration,
        )

        if latest and (_DURATION_RE.search(latest) or _MINUTE_RE.search(latest)):
            duration_sec = parse_duration_sec(latest, default_format="short")
            scenes = scene_count_for_duration(duration_sec, fmt="short")
            merged["target_duration_sec"] = duration_sec
            if not bool(merged.get("visual_proof_only")):
                merged["scene_count"] = scenes
    except Exception:
        pass
    if force_fresh or store.is_new_production_request(latest, session):
        merged = _force_fresh_shortform_args(merged)
    # Pending/approval path: keep multi-scene counts so the Approve card is not
    # filtered away by clients that treat scene_count=1 as stale.
    return store._prepare_shortform_pending_args(merged, session_messages, session=session)


def _inject_shortform_live_demand(args: dict[str, Any], session: dict[str, Any]) -> dict[str, Any]:
    """Attach last Live Demand brief so shorts/ads stay demand-grounded."""
    from studio_agent.live_demand import inject_demand_into_production_args

    merged = dict(args or {})
    last = session.get("last_live_demand") if isinstance(session.get("last_live_demand"), dict) else {}
    brief = str(last.get("brief") or "").strip()
    if not brief:
        return merged
    # Ignore very old demand packets (6h)
    try:
        age = time.time() - float(last.get("updated_at") or 0)
        if age > 6 * 3600:
            return merged
    except (TypeError, ValueError):
        pass
    plan_stub = None
    try:
        from studio_agent.live_demand import LiveDemandPlan

        plan_stub = LiveDemandPlan(
            required=True,
            mode=str(last.get("mode") or "content_creation"),
            window_days=int(last.get("window_days") or 2),
            niche_hint=str(last.get("niche_hint") or ""),
            search_query=str(last.get("search_query") or ""),
        )
    except Exception:
        plan_stub = None
    return inject_demand_into_production_args(merged, brief=brief, plan=plan_stub)


def _inject_shortform_render_style(args: dict[str, Any], session: dict[str, Any], *, user_text: str = "") -> dict[str, Any]:
    """Use an explicit spoken style before falling back to the session picker."""
    from studio_agent.render_styles import resolve_render_style

    merged = dict(args or {})
    style = resolve_render_style(
        str(merged.get("render_style") or "").strip() or None,
        session_style=str(session.get("render_style") or "").strip() or None,
        user_text=user_text,
    )
    merged["render_style"] = style.key
    return merged


def _shortform_uses_skeleton_pipeline(args: dict[str, Any], session: dict[str, Any]) -> bool:
    from studio_agent.render_styles import is_skeleton_style, resolve_render_style

    style = resolve_render_style(
        str((args or {}).get("render_style") or "").strip() or None,
        session_style=(session or {}).get("render_style"),
    )
    return is_skeleton_style(style)


def _inject_shortform_video_model(args: dict[str, Any], session: dict[str, Any]) -> dict[str, Any]:
    """Ensure shortform jobs inherit the session I2V model when the model omits it."""
    merged = dict(args or {})
    if not str(merged.get("video_model") or "").strip():
        merged["video_model"] = store.normalize_video_model(session.get("video_model")) or store.DEFAULT_VIDEO_MODEL
    else:
        merged["video_model"] = store.normalize_video_model(merged.get("video_model"))
    return merged


def _inject_shortform_image_model(args: dict[str, Any], session: dict[str, Any]) -> dict[str, Any]:
    """Ensure shortform jobs inherit the session image model when the model omits it."""
    merged = dict(args or {})
    selected = str(merged.get("image_model_id") or merged.get("image_model") or "").strip()
    if not selected:
        merged["image_model_id"] = store.normalize_image_model(session.get("image_model")) or store.DEFAULT_IMAGE_MODEL
    else:
        merged["image_model_id"] = store.normalize_image_model(selected)
    merged.pop("image_model", None)
    return merged


def _inject_shortform_caption_options(args: dict[str, Any], session: dict[str, Any]) -> dict[str, Any]:
    """Ensure shortform jobs inherit session caption preferences when omitted."""
    merged = dict(args or {})
    user_text = str(
        merged.get("user_request")
        or merged.get("topic")
        or merged.get("script")
        or ""
    ).lower()
    if any(
        mark in user_text
        for mark in (
            "no captions",
            "captions off",
            "without captions",
            "remove captions",
            "disable captions",
        )
    ):
        merged["captions_enabled"] = False
        merged["caption_mode"] = "off"
        return merged
    if any(
        mark in user_text
        for mark in (
            "one word",
            "single word",
            "word-by-word",
            "with captions",
            "enable captions",
            "add captions",
        )
    ):
        merged["captions_enabled"] = True
        merged["caption_mode"] = "word"
        return merged

    session_mode = str(session.get("caption_mode") or "").strip().lower()
    session_enabled = session.get("captions_enabled")
    if session_enabled is None:
        session_enabled = session_mode != "off"
    if not session_enabled or session_mode == "off":
        merged["captions_enabled"] = False
        merged["caption_mode"] = "off"
    else:
        merged["captions_enabled"] = True
        merged["caption_mode"] = "word"
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


def _activity_summary_for_tool(
    tool: str,
    args: dict[str, Any] | None = None,
    result: str | dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compact UI payload for professional activity timeline (query + result count)."""
    name = str(tool or "").strip()
    raw_args = args if isinstance(args, dict) else {}
    query = (
        raw_args.get("query")
        or raw_args.get("search_query")
        or raw_args.get("topic")
        or raw_args.get("niche")
        or raw_args.get("channel_title")
        or raw_args.get("q")
        or ""
    )
    summary: dict[str, Any] = {}
    if query:
        summary["query"] = str(query)[:160]

    parsed: dict[str, Any] = {}
    if isinstance(result, dict):
        parsed = result
    elif isinstance(result, str) and result.strip():
        try:
            loaded = json.loads(result)
            if isinstance(loaded, dict):
                parsed = loaded
        except Exception:
            parsed = {}

    for key in (
        "videos",
        "results",
        "items",
        "trends",
        "channels",
        "candidates",
        "segments",
        "clips",
        "rows",
        "hits",
    ):
        val = parsed.get(key)
        if isinstance(val, list):
            summary["result_count"] = len(val)
            break
    if "result_count" not in summary:
        for key in ("count", "total", "result_count", "video_count", "hit_count"):
            if parsed.get(key) is not None:
                try:
                    summary["result_count"] = int(parsed[key])
                    break
                except (TypeError, ValueError):
                    pass

    low = name.lower()
    if "poll_render" in low:
        source = "studio"
        title = "Deep analysis"
    elif "youtube" in low or "public_search" in low or "demand" in low or "trend" in low:
        source = "youtube"
        title = "Searched YouTube"
    elif "web" in low or "search" in low:
        source = "web"
        title = "Searched web"
    elif "fetch_competitor" in low:
        source = "youtube"
        title = "Channel uploads"
    elif "analytics" in low or "channel" in low:
        source = "studio"
        title = "Channel data"
    elif "competitor" in low:
        source = "youtube"
        title = "Competitor scan"
    elif "reference" in low or "analyze" in low:
        source = "studio"
        title = "Media analysis"
    else:
        source = "studio"
        title = "Tool result"
    if parsed.get("source"):
        source = str(parsed.get("source"))[:40]
    summary["source"] = source
    summary["title"] = title

    # Human label for the parent activity step
    if query and source in {"youtube", "web"}:
        summary["label"] = f"Searching for information on {str(query)[:80]}"
    elif "analytics" in low:
        summary["label"] = "Checking channel analytics"
    elif "memory" in low:
        summary["label"] = "Updating session memory"
    elif "render" in low or "shortform" in low or "longform" in low or "generate" in low:
        summary["label"] = "Starting production"
    return summary


async def _fire_tool_start(
    emit: EventEmitter | None,
    tool: str,
    *,
    args: dict[str, Any] | None = None,
    **extra: Any,
) -> None:
    summary = _activity_summary_for_tool(tool, args, None)
    payload = {
        "tool": tool,
        "label": summary.get("label"),
        "query": summary.get("query"),
        **extra,
    }
    await _fire_event(emit, "tool_start", **{k: v for k, v in payload.items() if v is not None})


async def _fire_tool_end(
    emit: EventEmitter | None,
    tool: str,
    *,
    status: str = "ok",
    args: dict[str, Any] | None = None,
    result: str | dict[str, Any] | None = None,
    error: str | None = None,
    **extra: Any,
) -> None:
    summary = _activity_summary_for_tool(tool, args, result)
    payload: dict[str, Any] = {
        "tool": tool,
        "status": status,
        "summary": summary,
        "label": summary.get("label"),
        "query": summary.get("query"),
        **extra,
    }
    if error:
        payload["error"] = error
    await _fire_event(emit, "tool_end", **payload)


async def _fire_verification_step(
    emit: EventEmitter | None,
    step: str,
    status: str,
    *,
    label: str,
    detail: str = "",
    required: bool = True,
) -> None:
    await _fire_event(
        emit,
        "verification_step",
        step=step,
        status=status,
        label=label,
        detail=detail,
        required=required,
    )


def _normalize_agent_mode(value: Any) -> str:
    mode = str(value or "studio").strip().lower()
    return mode if mode in {"plan", "studio", "cliplab"} else "studio"


def _mentions_cliplab(text: str) -> bool:
    return bool(re.search(r"\bcliplab\b", str(text or ""), re.I))


def _explicit_cliplab_request(text: str) -> bool:
    """True only when the user clearly asked for ClipLab — not negated boilerplate."""
    raw = str(text or "")
    if not _mentions_cliplab(raw):
        return False
    if re.search(r"(?:do\s+not|don'?t)\s+use\s+cliplab", raw, re.I):
        return False
    return True


def _effective_agent_mode(agent_mode: str, user_text: str) -> str:
    normalized = _normalize_agent_mode(agent_mode)
    if normalized == "plan":
        # Natural-language production commits ("make the first scene", "make it")
        # must not bounce through Plan-mode narration asking for magic phrases.
        if (
            store.is_hard_production_commit(user_text)
            or store.is_explicit_production_request(user_text)
            # Expanding an approved proof is an existing-production mutation,
            # even though it is deliberately excluded from the brand-new
            # production commit classifier.
            or store.is_expand_short_request(user_text)
        ):
            return "studio"
        return "plan"
    if normalized == "cliplab":
        return "cliplab"
    # Strip stale/auto-injected ClipLab attachment blocks before intent detection.
    intent_text = _sanitize_inbound_user_text(user_text, "studio")
    if _explicit_cliplab_request(intent_text):
        return "cliplab"
    return "studio"


def _sanitize_inbound_user_text(user_text: str, agent_mode: str) -> str:
    text = str(user_text or "").strip()
    if agent_mode == "cliplab":
        return text
    text = re.sub(r"\[Studio Agent mode:\s*ClipLab\][^\n]*\n?", "", text, flags=re.I)
    text = re.sub(r"\[Video attachment ready for ClipLab:[^\]]*\]\s*", "", text, flags=re.I)
    text = re.sub(r"Use ingest_cliplab_attachment[^\n]*", "", text, flags=re.I)
    text = re.sub(r"analyze_cliplab_video[^\n]*", "", text, flags=re.I)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


def _uploaded_reference_context(
    session: dict[str, Any],
    agent_mode: str,
    user_text: str = "",
) -> str:
    if agent_mode == "cliplab":
        return ""
    from studio_agent.attachments import resolve_video_attachment_path

    sid = str(session.get("session_id") or "")
    uid = str(session.get("user_id") or "")
    path = resolve_video_attachment_path(
        sid,
        uid,
        messages=list(session.get("messages") or []),
    )
    if not path:
        return ""
    if store.is_reference_description_correction(user_text):
        return (
            "[Studio Agent uploaded reference video — user corrected a prior description]\n"
            f"local_path: {path}\n"
            "The user is correcting how you described this upload. Accept their correction immediately. "
            "Do NOT re-run analyze_reference_video unless they explicitly ask for pacing/structure analysis. "
            "If you need the actual look, call analyze_reference_video once and use its visual_summary field — "
            "never guess skeleton or art style from the session Art Style picker."
        )
    if store.is_explicit_reference_analysis_request(user_text):
        return (
            "[Studio Agent uploaded reference video]\n"
            f"local_path: {path}\n"
            "The user asked to analyze/watch this upload. Studio HAS analyze_reference_video for this file. "
            "Call analyze_reference_video with local_path set to exactly that path, poll poll_render_job(kind=competitor), "
            "and extract topic, visual_summary, hook, pacing, and editing lessons. "
            "Never claim you cannot watch or analyze the uploaded file. "
            "Do NOT call start_shortform_generate unless the user explicitly asks to render afterward."
        )
    if store.is_ideation_request(user_text):
        return (
            "[Studio Agent uploaded reference video]\n"
            f"local_path: {path}\n"
            "The user attached this as creative context for planning/ideation. Discuss niche, channel "
            "strategy, art style, and market positioning conversationally. Use lightweight research tools "
            "(get_public_search_trends, search_youtube_public, recommend_video_topics) when helpful. "
            "If you need the actual visual look, call analyze_reference_video and read visual_summary — "
            "never infer skeleton/art style from the session Art Style picker. "
            "Do NOT call start_shortform_generate, ingest_cliplab_attachment, "
            "or analyze_cliplab_video unless the user explicitly asks to analyze pacing/structure or start rendering."
        )
    return (
        "[Studio Agent uploaded reference video]\n"
        f"local_path: {path}\n"
        "An uploaded reference is available. Use it for creative context by default. Only call "
        "analyze_reference_video if the user explicitly asks to study pacing/editing/structure or recreate "
        "the video. Do NOT call ingest_cliplab_attachment, analyze_cliplab_video, or start_shortform_generate "
        "unless the user explicitly switched to ClipLab mode, asked for ClipLab clipping, or asked to render."
    )


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
        "Debit applies to direct Anthropic usage and FAL renders; suggest Wallet top-up if balance is low before expensive jobs."
    )


def _longform_thumbnail_context() -> str:
    """Per-channel thumbnail/style grounding for the system prompt.

    Built live from the long-form registry so the agent always knows (a) which
    channels have their visual identity locked to real published covers and
    (b) that no-channel sessions have NO style authority and must ask instead
    of inventing a brand."""
    try:
        from long_form.prompts.channels import list_channels
        rows = []
        for ch in list_channels("long_form"):
            key = str(ch.get("key") or "")
            label = str(ch.get("label") or key)
            has_covers = str(ch.get("channel_id") or "").strip().startswith("UC")
            if has_covers:
                rows.append(
                    f"- {label} (`{key}`): thumbnails are STYLE-LOCKED to the channel's real published "
                    "covers (latest covers are pulled automatically and used as edit references). "
                    "Never describe or promise a different thumbnail style for this channel."
                )
            else:
                rows.append(
                    f"- {label} (`{key}`): no public channel connected yet — covers cannot be pulled. "
                    "Thumbnails fall back to the registry style prompt; tell the user this and offer "
                    "to match real covers once the channel is connected."
                )
        if not rows:
            return ""
        return (
            "CHANNEL THUMBNAIL & VISUAL IDENTITY (long-form):\n"
            "Thumbnail style is per-channel and comes from the channel's own published covers, not from "
            "generic taste. The on-image title is kept as short as the channel's real covers keep theirs "
            "(e.g. History Rewind covers say 'THE MONGOL EMPIRE', never the full SEO title with "
            "'| Full Documentary | 9 Hours' suffixes).\n"
            + "\n".join(rows)
            + "\n- NO CHANNEL SELECTED / unknown channel: there is no style authority. Say so plainly, "
            "then ask for either (a) the creator's channel to pull covers from, (b) 1-3 reference "
            "thumbnails, or (c) an explicit style direction — before generating. Do not invent a brand.\n"
            "- If the user critiques generated thumbnails ('these don't match my channel'), treat it as a "
            "revision request against their real covers, not a conversation."
        )
    except Exception:
        return ""


def system_prompt(
    *,
    content_format: str,
    reasoning_depth: str = "balanced",
    billing_profile: dict[str, Any] | None = None,
    render_style: str = "cinematic",
    image_model: str = "ernie_image",
    video_model: str = "seedance",
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
            + " When describing an UPLOADED reference video's look, use analyze_reference_video visual_summary "
            "or the user's explicit words — never infer skeleton/art style from this picker alone."
        )
    except KeyError:
        style_hint = (
            "USER RENDER STYLE: cinematic (default). Call list_render_styles; pass render_style on "
            "start_shortform_generate. Use skeleton_host only when the Art Style picker is set to Skeleton."
        )
    selected_image_model = store.normalize_image_model(image_model)
    image_model_labels = {
        "seedream_edit": "Seedream 4.5 Edit (canonical skeleton lock)",
        "seedream_v4": "Seedream 4.0",
        "seedream_v5_lite": "Seedream 5.0 Lite",
    }
    image_model_hint = (
        "USER IMAGE MODEL (session picker): "
        f"{image_model_labels.get(selected_image_model, selected_image_model)} (`{selected_image_model}`). "
        "Pass this as image_model_id on start_shortform_generate unless the user changes it in chat. "
        "For skeleton_host, default to seedream_edit and require a user-uploaded skeleton reference for KORPI-level identity lock."
    )

    selected_video_model = store.normalize_video_model(video_model)
    video_model_labels = {
        "ltx_budget": "LTX Budget (cheapest full animation)",
        "seedance": "Seedance 2.0 (default balanced motion)",
        "pixverse": "Pixverse V6 (permissive moderation)",
        "kling_pro": "Kling 2.1 Pro (premium motion, highest cost)",
    }
    video_model_hint = (
        "USER IMAGE-TO-VIDEO MODEL (session picker): "
        f"{video_model_labels.get(selected_video_model, selected_video_model)} (`{selected_video_model}`). "
        "Pass this as video_model on start_shortform_generate unless the user changes it in chat. "
        "Better motion costs more credits."
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

    from studio_agent.conversation import conversation_system_preamble
    from studio_agent.tools_sop import thin_chat_card

    return f"""You are NYPTID Studio Agent — the primary NYPTID Studio product. You help creators who
do NOT know what to film: pick niche + topic, frame the video beat-by-beat, then produce with
format-specific, channel-specific pacing, packaging, and delivery.

{conversation_system_preamble()}

{thin_chat_card()}

{fmt_hint}
{thinking_hint}

{style_hint}

{image_model_hint}

{video_model_hint}

{color_accessibility_hint}

{PROFESSIONAL_VOICE_BLOCK}

{CONTENT_TYPE_ROUTING_BLOCK}

{PRODUCT_AD_ROUTING_BLOCK}

{STUDIO_IDENTITY_PROMPT}

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
- If a latest upload row has view_count_source=public_inventory_fresher_than_analytics or
  view_count_source=existing_public_or_inventory_fresher_than_velocity, use that higher public view count as
  the current view count and say private Analytics/velocity rows may be lagging. Do not call the channel
  brand-new or low-performing from stale lower private counts when public inventory shows higher views.
- Do not say "complete", "ready", or "fixed" for a render/re-edit until a production tool result proves it is complete.
  If a job is running, say it is running and show/poll progress.
- If the user says a video/topic is already posted, treat it as locked history. Never recommend it as the next new video.
- Never mix analytics/memory between channels. The active channel is the only valid source for channel-performance claims.
{channel_specific_hint}

{_longform_thumbnail_context()}

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
- Short-form analytics rule: when the user asks about a Short's performance, next upload, title, SEO, package, or what
  will work for a Shorts channel, use `get_channel_analytics` and read `shortform_performance_comparison`. If
  `comparison_ready` is true, treat `comparison_brief` as ground truth — never say the tool only pulled memory, never
  interview the user for views/retention already listed, and never invent missing engaged-views/swipe metrics. Compare
  retention_winner / views_winner / best_prior_short using measured Shorts metrics. Never substitute long-form
  CTR/chapter logic for Shorts packaging.
- Lexi Manhwa / Lexi Manhua packaging rule: titles should sound like a professional anime/manhwa Shorts SEO strategist:
  character conflict, betrayal, revenge, impossible comeback, secret identity, power reveal, or emotional cliffhanger in
  plain language. Avoid generic labels like "anime edit", "manhwa recap", or broad genre-only titles unless the data
  explicitly proves they work for the selected channel.

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

When user uploads a reference video file in Studio Agent (not ClipLab):
1. The file is persisted on the server at `latest_attachment_paths` for this chat.
2. Default behavior: treat the upload as creative context. Discuss channel strategy, niche, art style, packaging, and story ideas conversationally first — like a normal chat assistant.
3. Do NOT auto-run `analyze_reference_video`, channel analytics, or public search tools until the user gives explicit go-ahead ("yes run that", "go ahead", "do it", "pull the data now").
4. When analysis IS approved and runs, read transcript + storytelling + visual_summary + pacing — not ffmpeg cut counts alone on static explainers.
5. Never auto-call `start_shortform_generate` just because a reference video was attached.
6. Do NOT switch to ClipLab or call ingest_cliplab_attachment unless the user explicitly chose ClipLab mode or asked for ClipLab clipping.

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
   For Shorts, also read `shortform_performance_comparison` before recommending a title/package. The latest Short and
   best prior Short are the control pair; public trend data is supporting context only.
   For 0-sub channels: positioning + competitor homework, NOT "why X failed."
4. `get_public_search_trends` — demand when harvest is thin or channel is new.
   Public trend evidence contract:
   - Public search is not private YouTube Analytics. For every trend/topic claim, cite hydrated
     evidence rows with video_id/title/channel/views/likes/duration/published_at/cache_status.
   - Never say "fresh YouTube search data", "trending", "high search volume", or exact view-count
     precedent unless the tool returned hydrated_video_stats and a support_label that justifies it.
   - Snippet-only, stale-cache, weak, or low-signal rows must be described as experimental or
     unsupported, not proven.
5. `get_studio_credits` before expensive renders; low balance → Wallet top-up (unlimited purchases).

Always explain: what's working, what's not, recommended next 1–3 actions, then offer to render.

═══ PREMIUM LONG-FORM (documentaries — Jake Tran / Magnates / MrBeast pacing bar) ═══
Quality target: feels like a $5k+ edit — NOT "good enough AI."
- Voice: FAL MiniMax on channel config (`voice_provider_default`); never route narration outside the effective FAL TTS policy.
- Script: `load_skill script-writing` + CHANNEL.md; cold open hook in first 8s; pattern interrupts every 45–90s;
  no dead air; escalate stakes; land a crisp outro CTA.
- Visuals: photoreal premium stills per channel FLOW; stat cards / motion graphics where channel allows.
- Deliver: 4K/UHD when pipeline supports it; default to highest tier the channel registry specifies.
- Thumbnails: `thumbnail-design` skill BEFORE proposing upload package.
- Final package must include title, description, tags, timestamps, and selected/approved thumbnail guidance.
- `start_longform_render` after outline approval; poll until complete.

═══ LIVE DEMAND (all niches — organic + product ads) ═══
- Before claiming what people want / what will go viral / what to make for a niche, pull Live Demand via
  ONE tool: `get_public_search_trends` (do NOT also call `search_youtube_public` for the same niche — that
  doubles YouTube search.list quota cost). Default is cache-first; only pass fresh=true when the user
  says right now / live / last 24h / most recent.
- If tools return youtube_quota_exhausted=true or error mentions quota, say so clearly and do not invent stats.
  Mentions reset around midnight Pacific Time.
- Works for every niche (day trading, psychology, fitness, SaaS ads, etc.) — never invent trends from memory.
- Product ads: Live Demand for the niche first, then `ingest_product_reference`, then map demand hooks → CTA.
- Cite hydrated rows only (title, channel, views, published_at, support_label / cache_status).
- If Studio already injected a Live Demand brief this turn, base the script/topic on that brief.

═══ SHORTFORM RENDER (start_shortform_generate + poll_render_job kind=shortform) ═══
REQUIRED on every short render: `render_style` from list_render_styles OR the user's session Art Style picker, `image_model_id` from the user's session Image picker, and `video_model` from the user's session I2V picker unless they override it in chat.
- Shorts quality target: benchmark against the selected channel's own Shorts outliers and niche-specific
  high-retention Shorts. Optimize viewed-vs-swiped, first-1-to-3-second retention, completion/APV, rewatches,
  engaged views, and interactions per view. Do not use long-form CTR/AVD/chapter benchmarks as substitutes.
- Default for most channels: cinematic, ultra_realism, comic_book, historical_18th_century, etc. — real subjects.
- `skeleton_host` is a niche art style like comic or Ghibli — use it only when the user picked Skeleton in Art Style.
- Before approving render, state the render_style label and selected image-to-video model so the user sees what visuals and motion tier they are buying.
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
- `start_shortform_generate` ALWAYS starts the permanent staged workflow with exactly one still
  (`visual_proof_only=true`, `scene_count=1`), regardless of niche, style, prompt detail, or requested final duration.
  Never generate the remaining scenes in the first call and never claim the video is complete from that tool.
- Required order for every Short: generate Scene 1 still -> creator fixes/approves still -> animate only Scene 1 ->
  creator fixes/approves animation -> creator says make the rest -> ask duration -> ask motion graphics/effects,
  pacing, emotional tone, captions, and sound design -> CONFIRM restatement (duration + animate policy) ->
  only then `expand_visual_proof_shortform` (Fast expand: known-good ≤300 prompts; default animate=heroes, not batch-all).
- Production mutation tools (start/expand/edit/regenerate/animate/finalize) are runner-owned. Do not invent long
  STRICT REPAIR prompts; use the visual_fix_contract short master-regenerate path for artifacting.
- If the user says to make one still and then animate that one scene, do not start a full short. First generate exactly one
  still with `visual_proof_only=true` and `scene_count=1`; after the user approves that still, call
  `set_production_scenes_animate(job_id, scene_indices=[0], animate=true)` and then
  `animate_production_scenes(job_id, scene_indices=[0])`.

PLANNING MODE (default until explicit scene/production commit):
- Stay in planning for niche/market research, channel compare/contrast, concept, follow-up questions, and soft "let's make…".
- Do NOT call start_shortform_generate / start_longform_render until the creator explicitly commits, e.g.
  "generate scene 1", "make the first scene", "render scene one", "go ahead and make the short now".
- Ask follow-ups when intent is unclear. Use Live Demand + channel data to compare what the niche needs vs what
  this channel already does, then propose a better angle — still without rendering.

VISUAL FIX CONTRACT (shortform + longform, stills + clips) — exact recovery method:
- Artifact / fused glass / orb / pod / morph complaints: NEVER edit the broken still with a long repair prompt.
- Master-regenerate with a SHORT prompt (≤300 chars) via the visual_fix_contract path.
- Relationship / love-bomb topics default to TWO identical skeleton hosts standing APART with separate thin glass
  skins and a clear air gap (never one shared bubble).
- i2v motion prompts also ≤300 chars and SILENT (FAL VO later). Retry with another short seed — never STRICT REPAIR sludge.

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
- During awaiting_scene_review, if the user describes a still problem in chat (e.g. "fix the missing eyeballs",
  "add realistic eyes", "fix scene 1 hands") — even without clicking Edit on the still card — Studio Agent auto-runs
  `edit_production_scene_still` on the active job. Confirm the updated still and ask whether to approve/animate.
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
- SAME skeleton every beat: ivory bones, glass shell, large realistic human-like eyeballs in both sockets. Identity never changes.
- Per scene, edit ONLY: background/environment, outfit/clothes, props, pose.
- Need muscles? Add muscle definition ON the same skeleton (wardrobe/body overlay) — not a new character.
- Need clothes? Edit wardrobe on the same skeleton. Different location? Edit background only.
- Rinse and repeat: master → edit → master → edit for every beat.

VIDEO (user-selectable — ask if unclear):
- Call `list_skeleton_video_models` and pass `video_model` to start_shortform_generate:
  `ltx_budget` (3 AC, cheapest full animation), `seedance` (default, 5 AC), `pixverse` (permissive), `kling_pro` (7 AC, best motion).

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
- PLAN-MODE THUMBNAIL EXCEPTION: when the user explicitly asks to make, preview, regenerate, or edit the thumbnail
  for the currently planned long-form video, call `generate_longform_thumbnails` immediately even while agent_mode=plan.
  Omit job_id on the first request and pass the current planned title + channel_key; reuse the returned job_id for every
  revision. Use count=1 when the user asks for "a thumbnail"; use multiple candidates only when requested. This spends
  only for the requested thumbnail candidate(s). It must not start the script, scenes, voice,
  animation, or final video, and it must not switch the conversation out of Plan mode.
- Poll `poll_render_job` with kind=longform until complete.
- Granular control: After chapters are ready or in the render phase use `list_longform_scenes(job_id)` + `regenerate_longform_still(job_id, global_scene_idx)` to re-do specific stills with the longform image model. For full per-scene animate selection on a long doc, guide the user through chapter approval then use the still review + selective re-renders before the final compose (the pipeline already supports mixed animation costs and per-scene motion). The same "edit until perfect, animate only the hero moments" philosophy applies.

═══ Short-form & long-form without Skeleton AI (default for most creators) ═══
Rookcast skills at studio/skills/ — load_skill before steps. image-generation skills apply
to photoreal / channel stills, reference blueprints, thumbnails, and b-roll plans.
RENDER COST QUOTES (mandatory):
- NEVER quote per-short render costs from memory, training data, or generic "standard pipeline" assumptions.
- The user's active session models are binding: {selected_image_model} for stills, {selected_video_model} for i2v.
- Before ANY user-facing cost breakdown, call `estimate_shortform_render_cost` and quote ONLY its `formatted_quote` + `total_estimated_usd`.
- For quality-vs-cost, customer pricing, or profit-margin questions, call `optimize_production_margin` in Plan mode. Report immediate proof spend, projected full-production spend, recommended route, and minimum customer price for the requested margin. This tool is read-only and never authorizes production.
- Recompute whenever duration, scene count, image model, video model, animation choice, selling price, or target margin changes. Never reuse a stale quote after any of those inputs changes.
- Do NOT cite LTX, Seedream T2I, or Mmaudio/Minimax unless those models appear in the estimate tool payload for this session.
- `get_fal_pricing` is supplemental reference only — not a substitute for `estimate_shortform_render_cost`.
YMYL: compliance-preflight + .gov sources. Follow CHANNEL.md when channel_key is set.

When proposing renders, explain cost/risk and which pipeline (blueprint short, long-form, Skeleton AI, etc.) using the estimate tool output.
For topic research use get_public_search_trends and get_channel_analytics.
After starting a render, poll poll_render_job until complete or failed. The Studio UI also
auto-polls production jobs: live progress lines in chat, a production rail, bottom-right
monitor, stills gallery at awaiting_approval, one-click Finalize, and in-chat MP4 download.
Use finalize_longform_render when long-form hits awaiting_approval (or tell user to click
Finalize in chat). For shortform non-skeleton jobs the powerful scene control tools (list/edit/ selective animate / finalize) give you the ability to iterate individual scenes with V4.5 edit as many times as needed and choose exactly which ones get real motion. refresh_channel_intelligence after uploads; record_production_feedback
when the user reports performance (internal training, never sold).

Progress reporting (important): long-running tools run in the background and return a job_id.
- analyze_reference_video / analyze_competitor_video: poll kind=competitor through pacing + audio.
- retry_reference_analysis: when transcript/vision/story failed on an existing job_id, retry those stages without re-uploading.
- Never go silent between start and finish. Summarize pacing (avg shot length) + hook window.

Internal ClipLab workflow (owner/admin only):
- If the user uploads a long recording and asks for clips, do not start a normal shortform render.
- ClipLab cuts/reframes existing footage. Ignore the session short-form style, image style, and image-to-video style picker.
- Do not say "comic book style", "cinematic style", or any generated-scene style for ClipLab unless the user explicitly asks for a Remix Lab visual treatment on an already-cut clip.
- For long-form-to-shorts ClipLab, use clip selection, hook/pacing analysis, 9:16 reframe, captions, light edit treatment, and upload packaging only.
- Never call `start_shortform_generate` or `start_longform_render` to satisfy a ClipLab long-video-to-clips request; those create generated videos, not clips from the uploaded source.
- First use `ingest_cliplab_attachment` to create a ClipLab video_id from the latest uploaded video attachment.
- Poll `poll_cliplab_job` until ingest is complete.
- Use `get_channel_analytics` for the selected channel when available and `get_public_search_trends` for public demand before choosing what clips should be cut.
- Then call `analyze_cliplab_video(video_id, prompt, channel_id, registry_key)` with a prompt that names the target channel, niche, desired hooks, pacing, and emotional/tension moments.
- `provider: "auto"` and `provider: "local"` both use Studio's native model-agnostic ClipLab analysis. Do not claim an external OpusClip provider ran; that adapter is not implemented in this release.
- Poll until segments are ready, pick the strongest segment_indices, then call `render_cliplab_segments`.
- When render completes, summarize each clip with its upload package: title, description, tags, hook, and why it fits the selected channel. Do not claim a clip will go viral; explain the evidence behind the selection.

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
    agent_mode: str = "studio",
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
        result = await _run_turn_impl(
            session,
            user_text,
            membership_plan=membership_plan,
            billing_profile=billing_profile,
            reply_to=reply_to,
            attachments=attachments,
            agent_mode=agent_mode,
        )
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
    agent_mode: str = "studio",
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
                with production_command_scope(run_id):
                    result = await _run_turn_impl(
                        session,
                        user_text,
                        membership_plan=membership_plan,
                        billing_profile=billing_profile,
                        emit=emit,
                        reply_to=reply_to,
                        attachments=attachments,
                        agent_mode=agent_mode,
                    )
                if admission.mode != "disabled":
                    result["queue"] = admission.as_dict()
                if run_id:
                    result["run_id"] = run_id
                    try:
                        store.append_run_event(session["session_id"], run_id, "done", {"event": "done"})
                        store.finish_run(session["session_id"], run_id, status="complete", result=result)
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
                        store.finish_run(session["session_id"], run_id, status="complete", result=result)
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


_TURN_SCOPED_SYSTEM_PREFIXES = (
    "[Studio Agent mode:",
    "[Studio Agent approved research execution mode]",
    "[Studio Agent conversational planning mode]",
    "[Studio Agent reference correction]",
    "[Studio Agent public-search DAG gate]",
    "[Studio Agent required data preflight",
    "[Studio Agent approved research preflight",
    "[Studio Agent ideation preflight",
    "[Studio Agent fresh production context",
    "[Studio Agent cleared pending approval",
    "[TITLE CORRECTION",
    "ANTI-HALLUCINATION AUDIT",
)


def _without_stale_turn_scoped_system_messages(
    messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Keep prior conversation while dropping control instructions from older turns.

    Runner-added system directives describe one specific turn. Persisting them into
    later turns gives models contradictory high-priority instructions (for example,
    an old research-only gate beside a new request to animate scenes). Tool evidence
    and the canonical leading system prompt remain available for recovery and
    grounding; only ephemeral control-plane rows are removed.
    """
    cleaned: list[dict[str, Any]] = []
    for message in messages:
        if str(message.get("role") or "") == "system":
            content = str(message.get("content") or "").lstrip()
            if any(content.startswith(prefix) for prefix in _TURN_SCOPED_SYSTEM_PREFIXES):
                continue
        cleaned.append(message)
    return cleaned


async def _run_turn_impl(
    session: dict[str, Any],
    user_text: str,
    *,
    membership_plan: str = "",
    billing_profile: dict[str, Any] | None = None,
    emit: EventEmitter | None = None,
    reply_to: dict | None = None,
    attachments: list[dict[str, Any]] | None = None,
    agent_mode: str = "studio",
) -> dict[str, Any]:
    sid = session["session_id"]
    user_id = session["user_id"]
    turn_id = f"turn_{uuid.uuid4().hex}"
    original_user_text = str(user_text or "")
    effective_agent_mode = _effective_agent_mode(agent_mode, original_user_text)
    if (
        effective_agent_mode == "studio"
        and _normalize_agent_mode(agent_mode) == "plan"
        and str(session.get("agent_mode") or "").strip().lower() != "studio"
    ):
        session = store.update_session(sid, agent_mode="studio") or session
    plan_only = effective_agent_mode == "plan"
    user_text = _sanitize_inbound_user_text(user_text, effective_agent_mode)
    intent_text = _normalize_user_intent_text(user_text)
    from studio_agent.turn_plan import build_turn_plan

    turn_plan = build_turn_plan(intent_text or user_text, session)
    model = session.get("model") or openrouter.DEFAULT_MODEL
    approval_mode = session.get("approval_mode") or "confirm"
    content_format = session.get("content_format") or "both"
    reasoning_depth = session.get("reasoning_depth") or "balanced"
    web_search = bool(session.get("web_search", True))
    active_registry = _active_registry_key(session, user_text)
    active_channel_id = str(session.get("channel_id") or "").strip()
    if not (active_registry or active_channel_id) and store.is_exact_topic_discovery_request(intent_text or user_text):
        implicit_channel = _only_connected_channel_for_user(str(user_id))
        if implicit_channel:
            session = store.update_session(sid, **implicit_channel) or session
            active_channel_id = implicit_channel["channel_id"]
            active_registry = implicit_channel["registry_key"]
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

    messages: list[dict[str, Any]] = _without_stale_turn_scoped_system_messages(
        list(session.get("messages") or [])
    )
    ideation_turn = store.is_ideation_request(intent_text or user_text)
    reference_correction_turn = store.is_reference_description_correction(intent_text or user_text)
    reference_context = _uploaded_reference_context(
        session,
        effective_agent_mode,
        intent_text or user_text,
    )
    if reference_context:
        messages.append({"role": "system", "content": reference_context})
    if reference_correction_turn:
        messages.append({
            "role": "system",
            "content": (
                "[Studio Agent reference correction]\n"
                "The user is fixing a prior misread of their upload. Acknowledge the correction in plain language. "
                "Do NOT narrate a new analysis run, do NOT poll analyze_reference_video again, and do NOT describe "
                "skeleton/art style from the session Art Style picker. If visual evidence is required, cite an "
                "existing analyze_reference_video visual_summary from this chat or ask one precise follow-up."
            ),
        })
    if plan_only:
        messages.append({
            "role": "system",
            "content": (
                "[Studio Agent mode: Plan & Conversation — HARD BOUNDARY]\n"
                "You are a conversation-first creative partner. Answer the creator's actual words naturally before "
                "offering a next step. Reason with them, brainstorm, explain tradeoffs, ask one useful question only "
                "when it genuinely unblocks the discussion, and retain the thread of the conversation.\n"
                "Do NOT turn ordinary discussion, agreement, a question, or an idea into a concept-plan card, a "
                "workflow checklist, or a production proposal. Only draft, revise, or show a structured plan when the "
                "creator explicitly asks for that artifact. A plan is not consent to produce.\n"
                "Do not generate media, start a render, charge credits, create production approvals, or silently move "
                "the creator into Production mode. Read-only research, analysis, and cost estimates are allowed when "
                "requested, but explain their result conversationally and remain in Plan mode. EXCEPT when the user "
                "explicitly asks to make/preview/revise a long-form thumbnail. That narrow packaging request must call "
                "generate_longform_thumbnails and remain in Plan mode; never start any other video stage. If the plan is "
                "ready, tell the creator to click Implement plan or switch the composer to Production."
            ),
        })
    elif effective_agent_mode == "cliplab":
        messages.append({
            "role": "system",
            "content": (
                "[Studio Agent mode: ClipLab]\n"
                "Use ClipLab only for this turn. Uploaded videos should go through "
                "ingest_cliplab_attachment -> analyze_cliplab_video -> render_cliplab_segments."
            ),
        })
    else:
        messages.append({
            "role": "system",
            "content": (
                "[Studio Agent mode: Normal]\n"
                "Use normal Studio Agent behavior. Do not call ClipLab tools unless the user "
                "explicitly switched to ClipLab mode or asked for ClipLab clipping."
            ),
        })
    conversational_turn = store.is_conversational_planning_turn(intent_text or user_text)
    if turn_plan.has_execution:
        messages.append({
            "role": "system",
            "content": (
                "[Studio Agent approved research execution mode]\n"
                "The user explicitly approved running research tools on this turn. Required backend work "
                "may already be complete in tool observations above. Synthesize NOW from that evidence: "
                "reference visual/story readout (if present) + public YouTube demand rows + channel analytics "
                "(if present). Do NOT narrate polling, waiting, or future tool calls. Google Trends/keyword-volume "
                "APIs are not wired yet — use get_public_search_trends evidence. Do NOT start production unless "
                "the user explicitly asks."
            ),
        })
    elif ideation_turn or conversational_turn:
        messages.append({
            "role": "system",
            "content": (
                "[Studio Agent conversational planning mode]\n"
                "Behave like a normal strategist chat (Claude/ChatGPT/Grok): discuss channel positioning, niche, "
                "art style, topic angles, packaging, and story structure in plain language first. "
                "Do NOT call any backend tools on this turn unless the user explicitly gave go-ahead "
                "(e.g. 'yes run that', 'go ahead', 'do it', 'pull the data now'). "
                "You MAY propose specific tools you could run next and ask for permission. "
                "Never narrate that you are pulling data or analyzing video while tools are not running. "
                "Do NOT call analyze_reference_video, get_channel_analytics, get_public_search_trends, "
                "or start_shortform_generate until the user approves."
            ),
        })
    if ideation_turn:
        blocked_ids = [str(jid).strip() for jid in (session.get("blocked_job_ids") or []) if str(jid).strip()]
        for job in list(session.get("active_jobs") or []):
            job_id = str(job.get("job_id") or "").strip()
            if str(job.get("kind") or "") == "shortform" and job_id:
                if not plan_only:
                    cancel_command_id = "ideation-cancel:" + hashlib.sha256(
                        f"{sid}\0{job_id}\0{intent_text or user_text}".encode("utf-8")
                    ).hexdigest()[:24]
                    try:
                        execute_tool_logged(
                            "cancel_production_job",
                            {"job_id": job_id, "command_id": cancel_command_id},
                            user_id=str(user_id or ""),
                            content_format=str(content_format or "short"),
                            session_id=sid,
                        )
                    except Exception:
                        # Keep session cleanup best-effort; the failed durable
                        # receipt remains available for diagnosis and replay.
                        pass
                blocked_ids.append(job_id)
        session = store.update_session(
            sid,
            skip_job_recovery=True,
            pending_actions=[],
            last_production={},
            active_jobs=[],
            blocked_job_ids=list(dict.fromkeys(blocked_ids))[-48:],
        ) or session
        await _fire_event(emit, "pending_actions", actions=[])
        await _fire_event(emit, "active_jobs", jobs=[])
        await _fire_event(emit, "session_state", **store.production_session_fields(session))
    messages.append({"role": "user", "content": _build_user_content(user_text, attachments)})
    # Persist the initiating user turn before any deterministic or paid tool
    # path. Those paths return before the normal end-of-turn save, and a process
    # or provider failure must not leave an assistant result without its prompt.
    session = store.update_session(sid, messages=messages) or {**session, "messages": messages}
    try:
        session = store.reconcile_production_state(
            session,
            messages=messages,
            user_text=intent_text or user_text,
            persist=True,
        ) or session
    except Exception:
        pass
    await _fire_event(
        emit,
        "session_state",
        **store.production_session_fields(session),
    )
    pending_command_answer = bool(
        str(session.get("interaction_state") or "") == "clarification"
        and isinstance(session.get("pending_scene_repair"), dict)
        and session.get("pending_scene_repair")
    )
    # Expand / scene-repair commands must run even in Plan mode — "make the rest
    # of the scenes" is production continuity on an already-started short, not a
    # planning discussion. Thumbnail create remains the other Plan-mode exception.
    from studio_agent.command_contract import scene_repair_candidate as _scene_repair_candidate

    allow_command_layer = bool(
        not plan_only
        or pending_command_answer
        or bool(session.get("short_expansion_intake"))
        or _wants_expand_visual_proof_short(intent_text or user_text)
        or _scene_repair_candidate(intent_text or user_text)
    )
    if allow_command_layer:
        semantic_command = await _apply_model_agnostic_studio_command(
            session=session,
            user_id=user_id,
            user_text=intent_text or user_text,
            content_format=content_format,
            model=model,
            emit=emit,
            membership_plan=membership_plan,
            billing_profile=billing_profile,
            approval_mode=approval_mode,
            reasoning_depth=reasoning_depth,
            reply_to=reply_to,
        )
        if semantic_command is not None:
            return semantic_command
    from studio_agent.turn_router import route_plan_turn
    plan_turn = route_plan_turn(
        intent_text or user_text,
        has_thumbnail_review=bool(session.get("thumbnail_review")),
    ) if plan_only else None
    if plan_turn and plan_turn.action in {"thumbnail_create", "thumbnail_revise"}:
        # Plan-mode thumbnail generation is the one intentional visual action
        # that does not require a production commit. Route it directly instead
        # of relying on the chat model to emit the right tool call; otherwise a
        # perfectly explicit request can fall through to the old long-form
        # render conversation/state path.
        concept = session.get("pending_concept") if isinstance(session.get("pending_concept"), dict) else {}
        thumbnail_args = {
            "count": plan_turn.thumbnail_count or 1,
            "title": str(concept.get("title") or store.get_locked_working_title(session) or "").strip(),
            "channel_key": str(active_registry or session.get("registry_key") or "history_rewind").strip(),
        }
        if plan_turn.action == "thumbnail_revise":
            existing_review = session.get("thumbnail_review") if isinstance(session.get("thumbnail_review"), dict) else {}
            thumbnail_args["job_id"] = str(existing_review.get("job_id") or "")
            thumbnail_args["feedback"] = plan_turn.feedback
            # A critique of the set ("these don't match my channel") applies to
            # every candidate, not just the first one.
            existing_count = len(list(existing_review.get("candidate_urls") or []))
            if not plan_turn.thumbnail_count and existing_count:
                thumbnail_args["count"] = existing_count
        try:
            thumbnail_result = execute_tool_logged(
                "generate_longform_thumbnails",
                thumbnail_args,
                user_id=user_id,
                content_format=content_format,
                session_id=sid,
            )
            thumbnail_payload = json.loads(thumbnail_result or "{}")
            if not isinstance(thumbnail_payload, dict):
                thumbnail_payload = {}
            thumbnail_error = str(thumbnail_payload.get("error") or "").strip()
        except Exception as exc:
            thumbnail_result = json.dumps({"error": str(exc)})
            thumbnail_payload = {"error": str(exc)}
            thumbnail_error = str(exc)

        messages.append(_tool_observation_message("generate_longform_thumbnails", thumbnail_result))
        if thumbnail_error:
            assistant_text = f"I could not generate the thumbnail candidates: {thumbnail_error}"
            await _fire_tool_end(
                emit,
                "generate_longform_thumbnails",
                status="error",
                args=thumbnail_args,
                result=thumbnail_result,
                error=thumbnail_error[:160],
            )
        else:
            thumbnail_count = int(thumbnail_args["count"])
            review = {
                "review_id": str(thumbnail_payload.get("job_id") or ""),
                "job_id": str(thumbnail_payload.get("job_id") or ""),
                "title": str(thumbnail_payload.get("title") or thumbnail_args["title"] or "Thumbnail review"),
                "candidate_urls": list(thumbnail_payload.get("thumbnails") or []),
                "feedback": str(thumbnail_payload.get("feedback_used") or ""),
                "updated_at": time.time(),
            }
            assistant_text = (
                f"I made {thumbnail_count} thumbnail candidate{'s' if thumbnail_count != 1 else ''} for this plan. "
                "They are attached below; tell me which direction to keep or what to change. "
                "This did not start the long-form video."
            )
            await _fire_event(emit, "thumbnail_review", review=review)
            await _fire_tool_end(
                emit,
                "generate_longform_thumbnails",
                status="ok",
                args=thumbnail_args,
                result=thumbnail_result,
            )
            # A revise means the creator critiqued the previous set — that
            # critique is per-channel Catalyst training data. Best-effort.
            if plan_turn.action == "thumbnail_revise" and str(thumbnail_args.get("feedback") or "").strip():
                try:
                    from studio_agent import catalyst_learning
                    catalyst_learning.record_thumbnail_style_feedback(
                        user_id,
                        session,
                        channel_key=str(thumbnail_args.get("channel_key") or ""),
                        feedback=str(thumbnail_args.get("feedback") or ""),
                        job_id=str(thumbnail_payload.get("job_id") or ""),
                    )
                except Exception:
                    pass
        messages.append({"role": "assistant", "content": assistant_text})
        # A thumbnail proof never becomes active production or last_production.
        store.update_session(
            sid,
            messages=messages,
            pending_actions=[],
            last_production=None,
            thumbnail_review=review if not thumbnail_error else session.get("thumbnail_review"),
        )
        return {
            "session_id": sid,
            "assistant_message": assistant_text,
            "pending_actions": [],
            "thumbnail_review": review if not thumbnail_error else session.get("thumbnail_review"),
            "active_jobs": list(session.get("active_jobs") or []),
            "approval_mode": approval_mode,
            "reasoning_depth": reasoning_depth,
            "usage": {},
            "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
        }
    corrected_scene_count = _short_scene_count_correction(intent_text or user_text)
    if corrected_scene_count is not None:
        pending_concept = session.get("pending_concept") if isinstance(session.get("pending_concept"), dict) else {}
        if pending_concept:
            pending_concept = {**pending_concept, "scene_count": corrected_scene_count}
        assistant_text = (
            f"You’re right: the finished short is planned for **{corrected_scene_count} scenes total**. "
            "The **one scene** in the estimate is only Studio’s first proof still—the permanent quality gate before "
            f"it generates the remaining {max(0, corrected_scene_count - 1)} scenes. It does not mean the finished short has one scene. "
            "I corrected the stored plan, and this did not start production."
        )
        messages.append({"role": "assistant", "content": assistant_text})
        store.update_session(
            sid,
            messages=messages,
            pending_actions=[],
            pending_concept=pending_concept or None,
        )
        await _fire_event(emit, "pending_actions", actions=[])
        return {
            "session_id": sid,
            "assistant_message": assistant_text,
            "pending_actions": [],
            "active_jobs": list(session.get("active_jobs") or []),
            "approval_mode": approval_mode,
            "reasoning_depth": reasoning_depth,
            "usage": {},
            "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
        }
    had_pending_actions = bool(session.get("pending_actions"))
    if had_pending_actions:
        messages.append({
            "role": "system",
            "content": (
                "[Studio Agent cleared pending approval because the user sent a new message.]\n"
                "Pending approvals apply only to the assistant turn that created them. Prepare a fresh "
                "action from the latest user message instead of reusing the previous approval."
            ),
        })
        clear_fields: dict[str, Any] = {"pending_actions": []}
        if not _wants_production_execution(user_text) and not store.is_explicit_production_request(user_text):
            clear_fields["last_production"] = {}
        session = store.update_session(
            sid,
            messages=messages,
            **clear_fields,
        ) or session
        await _fire_event(emit, "pending_actions", actions=[])
    production_diagnostic_turn = _is_production_diagnostic_turn(user_text)
    if production_diagnostic_turn:
        locked = ""
        try:
            from studio_agent.conversation import get_conversation_intent

            locked = str(
                (get_conversation_intent(session).get("locked_title")
                 or get_conversation_intent(session).get("working_title")
                 or "")
            ).strip()
        except Exception:
            pass
        messages.append({
            "role": "system",
            "content": (
                "[Studio Agent production diagnostic mode]\n"
                "The latest user message is asking why the wrong/stale production or title is being used. "
                f"{'Their locked working title is: ' + locked + '. ' if locked else ''}"
                "Apologize briefly, acknowledge the correct title, clear any wrong-title production state, "
                "and do NOT call start_shortform_generate unless they hard-commit on the locked title. "
                "Competitor titles from public data are research comps only — never what we are making."
            ),
        })
        # Always wipe production recover state on title/production diagnostics.
        session = store.update_session(
            sid,
            messages=messages,
            pending_actions=[],
            last_production={},
        ) or session
        await _fire_event(emit, "pending_actions", actions=[])
    store.touch_title_from_user_message(sid, user_text)
    try:
        memory.observe_user_message(str(user_id), user_text, session=session)
    except Exception:
        pass

    production_intent = store.detect_production_intent(intent_text or user_text, session)
    if production_intent != str(session.get("production_intent") or "").strip():
        session = store.update_session(sid, production_intent=production_intent) or session

    if _wants_expand_visual_proof_short(intent_text or user_text) and (session.get("pending_actions") or session.get("last_production")):
        messages.append({
            "role": "system",
            "content": (
                "[Studio Agent expand-short mode]\n"
                "The user wants to keep the approved first scene and generate the rest on the same job. "
                "Clear stale pending production approvals instead of starting a brand-new short."
            ),
        })
        session = store.update_session(
            sid,
            messages=messages,
            pending_actions=[],
            last_production={},
        ) or session
        await _fire_event(emit, "pending_actions", actions=[])

    if not reply_to and store.is_context_ingest_request(intent_text or user_text) and not session.get("context_ingested"):
        parent_id = store.resolve_ingest_parent_session(session, user_text, user_id=str(user_id))
        if parent_id:
            session = store.ingest_parent_context_into_session(
                sid,
                user_id=str(user_id),
                parent_session_id=parent_id,
            ) or session
            messages = list(session.get("messages") or messages)
            messages.append({
                "role": "system",
                "content": (
                    "[Studio Agent context ingest complete]\n"
                    f"Loaded compacted context from prior chat {parent_id}. Use it for planning and creative "
                    "direction, but do not resume old production jobs unless the user explicitly replies to that "
                    "deliverable card."
                ),
            })
            session = store.update_session(sid, messages=messages) or session

    # New short / different locked title / move-on from a Ready job → drop prior production completely.
    _wants_next_short = bool(
        not reply_to
        and store.is_new_production_request(intent_text or user_text, session, reply_to=reply_to)
    )
    if not _wants_next_short and not reply_to and not store.is_expand_short_request(intent_text or user_text):
        locked = store.get_locked_working_title(session)
        prior = store.prior_production_title(session)
        # Explicit next-title lock vs prior Ready short.
        if locked and prior and store._title_overlap_score(locked, prior) < 0.75:
            _wants_next_short = True
        target = store.resolve_current_production_target(session, messages)
        if target and prior and store._title_overlap_score(target, prior) < 0.75:
            _wants_next_short = True
        # Scene-1 commit on a new outline must not rubberband to a prior Ready short.
        if (
            not _wants_next_short
            and prior
            and store.is_scene_one_proof_commit(intent_text or user_text)
        ):
            if not target or store._title_overlap_score(target, prior) < 0.75:
                _wants_next_short = True
    if _wants_next_short:
        session = store.clear_stale_production_context(session)
        locked = store.get_locked_working_title(session)
        messages.append({
            "role": "system",
            "content": (
                "[Studio Agent fresh production context — NEXT SHORT]\n"
                "The user is starting a NEW video/production. Do not continue, poll, re-edit, or reuse the prior "
                "Ready/complete job or its title unless they explicitly reply to that deliverable card.\n"
                f"{'LOCKED WORKING TITLE for this new short: ' + locked if locked else 'Use the latest user-chosen title only.'}\n"
                "Clear stale approvals. Never call start_shortform_generate with the previous short's title. "
                "Never re-attach (_resume_job_id) to a finished workspace. "
                "For channel-native content, call get_channel_analytics (Catalyst) before start_shortform_generate."
            ),
        })
        session = store.update_session(
            sid,
            messages=messages,
            active_jobs=[],
            last_production={},
            pending_actions=[],
            blocked_job_ids=list(session.get("blocked_job_ids") or []),
            skip_job_recovery=True,
            production_intent=production_intent,
        ) or session
        await _fire_event(emit, "active_jobs", jobs=[])
        await _fire_event(emit, "pending_actions", actions=[])

    if production_intent == "product_ad":
        website = str(session.get("product_website") or "").strip()
        product_hint = (
            f"Saved product website on profile: {website}"
            if website
            else "No product website saved on profile yet; ask for a URL or product images."
        )
        messages.append({
            "role": "system",
            "content": (
                "[Studio Agent product-ad mode]\n"
                f"{product_hint}\n"
                "This is a conversion ad, not normal channel content.\n"
                "1) Live Demand: pull fresh public YouTube demand for the ad niche first "
                "(what people actually want to watch/buy in that vertical).\n"
                "2) Call ingest_product_reference for the offer/site/images.\n"
                "3) start_shortform_generate with product_reference_id and an ad-native visual_brief "
                "that maps Live Demand hooks → product CTA/signup/sales."
            ),
        })
        session = store.update_session(sid, messages=messages, production_intent=production_intent) or session

    if not plan_only and not reply_to and _wants_bulk_scene_ship_request(user_text):
        shipped = await _apply_bulk_scene_ship(
            session=session,
            user_id=user_id,
            user_text=user_text,
            content_format=content_format,
            emit=emit,
            membership_plan=membership_plan,
            billing_profile=billing_profile,
        )
        if shipped is not None:
            return shipped

    targeted_animation = None if plan_only else await _apply_targeted_scene_animation(
        session=session,
        user_id=user_id,
        user_text=intent_text or user_text,
        content_format=content_format,
        emit=emit,
        approval_mode=approval_mode,
        reasoning_depth=reasoning_depth,
    )
    if targeted_animation is not None:
        return targeted_animation

    # "Finish the video" means finalize the existing full short when every
    # scene is already approved and animated.  Do this before the one-scene
    # proof expansion intake, otherwise Studio asks for a duration that the
    # completed six-scene job already proves (for example 6 × 5 s = 30 s).
    if (
        not plan_only
        and not reply_to
        and _is_continue_production_request(intent_text or user_text)
        and _is_established_multiscene_short(session)
    ):
        if session.get("short_expansion_intake"):
            session = store.update_session(sid, short_expansion_intake={}) or session
        continued = await _continue_active_production(
            session=session,
            user_id=user_id,
            content_format=content_format,
            emit=emit,
            membership_plan=membership_plan,
            billing_profile=billing_profile,
        )
        if continued is not None:
            return continued

    if not plan_only and (
        _is_reply_to_scene_still_edit(user_text, reply_to)
        or _is_scene_review_fix_request(user_text)
        or _is_animation_repair_request(user_text)
    ):
        # Scene / animation review must win over sticky full-short expansion intake.
        if session.get("short_expansion_intake"):
            session = store.update_session(sid, short_expansion_intake={}) or session
        scene_fixed = await _apply_scene_review_fix(
            session=session,
            user_id=user_id,
            user_text=user_text,
            content_format=content_format,
            emit=emit,
            membership_plan=membership_plan,
            billing_profile=billing_profile,
            approval_mode=approval_mode,
            reasoning_depth=reasoning_depth,
            reply_to=reply_to,
        )
        if scene_fixed is not None:
            return scene_fixed

    if not plan_only and not reply_to and _is_continue_production_request(user_text) and not session.get("skip_job_recovery"):
        continued = await _continue_active_production(
            session=session,
            user_id=user_id,
            content_format=content_format,
            emit=emit,
            membership_plan=membership_plan,
            billing_profile=billing_profile,
        )
        if continued is not None:
            return continued
        if effective_agent_mode == "cliplab" or _explicit_cliplab_request(user_text):
            continued_cliplab = await _continue_active_cliplab(
                session=session,
                user_id=user_id,
                user_text=user_text,
                content_format=content_format,
                emit=emit,
                approval_mode=approval_mode,
                reasoning_depth=reasoning_depth,
            )
            if continued_cliplab is not None:
                return continued_cliplab

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
            terminal_poll = polled_status in {
                "complete", "incomplete", "failed", "error", "cancelled", "awaiting_scene_review", "awaiting_approval",
            }
            still_active = not terminal_poll
            active_jobs = [{"job_id": job_id, "kind": kind, "started_at": time.time()}] if still_active else [
                j for j in list(session.get("active_jobs") or [])
                if str(j.get("job_id") or "") != job_id
            ]
            poll_tool_status = "ok"
            if kind == "competitor":
                if polled_status == "incomplete":
                    poll_tool_status = "error"
                elif polled_status == "complete" and isinstance(polled, dict) and not _reference_poll_succeeded(polled):
                    poll_tool_status = "error"
            if kind == "competitor" and polled_status in {"complete", "incomplete"}:
                active_jobs = [
                    j for j in active_jobs
                    if str(j.get("kind") or "") != "shortform"
                ]
                ref_payload = _parse_reference_analysis_payload(result)
                if ref_payload:
                    _catalyst_capture_turn(
                        user_id=str(user_id),
                        session=session,
                        turn_kind="reference_analysis",
                        reference_payload=ref_payload,
                    )
            store.update_session(
                sid,
                messages=messages,
                active_jobs=active_jobs,
                pending_actions=[],
                last_production={},
            )
            await _fire_event(emit, "pending_actions", actions=[])
            await _fire_event(emit, "tool_end", tool="poll_render_job", status=poll_tool_status)
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

    if not reply_to and store.is_public_youtube_research_request(intent_text or user_text):
        # Grok-class: tools first, natural conversation reply — never ship research form.
        from studio_agent.conversation import (
            get_conversation_intent,
            strip_robot_research_artifacts,
            synthesize_conversational_research_reply,
            update_conversation_intent,
            weave_catalyst_into_reply,
        )
        from studio_agent.live_demand import extract_niche_hint, resolve_demand_search_query

        tool_fires = await _run_public_youtube_research_preflight(
            emit=emit,
            user_id=str(user_id),
            content_format=content_format,
            session_id=sid,
            messages=messages,
            user_text=intent_text or user_text,
            session=session,
            active_registry=active_registry,
            active_channel_id=active_channel_id,
        )
        active_label = (
            str(session.get("channel_title") or "").strip()
            or str(active_registry or "").replace("_", " ")
            or "YouTube"
        )
        ref_payload = _latest_complete_reference_analysis(messages=messages)
        search_query = (
            resolve_demand_search_query(
                intent_text or user_text,
                session,
                active_label=active_label,
                registry_key=active_registry,
                fallback_query=_latest_public_search_query(tool_fires),
            )
            or _latest_public_search_query(tool_fires)
        )
        _has_channel_evidence = bool(
            active_channel_id
            and (
                str(session.get("channel_title") or "").strip()
                or _has_channel_analytics_tool(tool_fires)
            )
        )
        evidence = _grounded_research_summary_from_tools(
            tool_fires,
            active_label=active_label,
            user_text=intent_text or user_text,
            include_channel=_has_channel_evidence,
            search_query=search_query,
            reference_payload=ref_payload,
        )
        ref_findings = (
            _format_reference_analysis_findings(ref_payload) if ref_payload else ""
        )
        niche_hint = (
            extract_niche_hint(intent_text or user_text, session)
            or search_query
            or active_label
        )
        messages.append({
            "role": "system",
            "content": (
                "[Internal Live Demand evidence — grounding only; never paste as a form]\n"
                + evidence[:8000]
            ),
        })
        assistant_text = sanitize_assistant_text(
            strip_robot_research_artifacts(
                await synthesize_conversational_research_reply(
                    user_text=intent_text or user_text,
                    evidence=evidence,
                    reference_findings=ref_findings,
                    niche_hint=str(niche_hint or ""),
                    model=str(model or session.get("model") or ""),
                )
            )
        )
        _catalyst_capture_turn(
            user_id=str(user_id),
            session=session,
            turn_kind="public_research",
            reference_payload=ref_payload,
            tool_fires=tool_fires,
            search_query=search_query,
        )
        session = store.get_session(sid) or session
        assistant_text = sanitize_assistant_text(
            weave_catalyst_into_reply(assistant_text, get_conversation_intent(session))
        )
        messages.append({"role": "assistant", "content": assistant_text})
        try:
            intent = update_conversation_intent(
                session,
                niche=str(niche_hint or "")[:120],
                search_query=str(search_query or "")[:220],
                channel_title=str(session.get("channel_title") or active_label or ""),
                channel_id=str(session.get("channel_id") or ""),
                registry_key=str(session.get("registry_key") or active_registry or ""),
                kind="public_research",
            )
            store.update_session(
                sid,
                messages=messages,
                pending_actions=[],
                last_production={},
                conversation_intent=intent,
            )
        except Exception:
            store.update_session(sid, messages=messages, pending_actions=[], last_production={})
        await _fire_event(emit, "pending_actions", actions=[])
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

    # Never re-surface shortform approval while the user is researching/strategizing.
    _channel_research_now = bool(
        store.is_channel_video_analysis_request(intent_text or user_text)
        or store.is_youtube_channel_url_reference_request(intent_text or user_text)
    )
    _strategy_now = bool(store.is_production_strategy_question(intent_text or user_text))
    _soft_proposal_now = bool(store.is_soft_production_proposal(intent_text or user_text))
    _research_only_now = bool(_is_research_only_turn(intent_text or user_text))
    _hard_commit_now = store.is_hard_production_commit(
        intent_text or user_text
    ) or store.is_explicit_production_request(intent_text or user_text)
    _no_production_card = (
        (plan_only and not _hard_commit_now)
        or _blocks_brand_new_production(intent_text or user_text)
    )
    if _no_production_card:
        stale = list(session.get("pending_actions") or [])
        had_last_prod = bool(session.get("last_production"))
        if stale or had_last_prod:
            # Always wipe last_production on research/plan turns so recovery cannot
            # resurrect a one-still / wrong-title Approve card after public demand chat.
            session = store.update_session(
                sid,
                pending_actions=[],
                last_production=None,
            ) or session
            await _fire_event(emit, "pending_actions", actions=[])
            if stale:
                messages.append({
                    "role": "system",
                    "content": (
                        "[Studio Agent cleared stale production approval. "
                        "This turn is research/strategy/planning only — answer the question or propose a concept. "
                        "Do NOT call start_shortform_generate or re-prepare production until the user hard-commits "
                        "(e.g. 'render it now', 'go ahead and make that short', 'yes make it', "
                        "'make the first scene').]"
                    ),
                })
        # Broad strategy/soft-proposal signals are production safety guards,
        # not permission to replace a Plan-mode conversation with a generated
        # concept card. In Plan mode we create or refresh that artifact only
        # when the creator explicitly asks for it.
        should_prepare_concept = (
            _wants_explicit_plan_artifact(intent_text or user_text)
            if plan_only
            else (_soft_proposal_now or _strategy_now)
        )
        if should_prepare_concept:
            from studio_agent import concept_plan as concept_plan_mod

            existing_plan = session.get("pending_concept") if isinstance(session.get("pending_concept"), dict) else None
            # Do not erase an established scope on an explicit Plan-mode
            # request to show/refine it. Model dialogue can refine the draft;
            # a new deterministic draft is only needed when none exists.
            plan = dict(existing_plan) if plan_only and existing_plan else concept_plan_mod.build_concept_plan(
                user_text=intent_text or user_text,
                session=session,
                messages=messages,
                production_intent=str(session.get("production_intent") or ""),
                content_format=str(content_format or session.get("content_format") or "short"),
            )
            # Reused plans can carry stale cross-format artifacts (Shorts title
            # on a 9-hour doc, shortform hook, beats sized for an old duration).
            plan = concept_plan_mod.reconcile_longform_plan(plan, session=session)
            session = store.update_session(
                sid,
                messages=messages,
                pending_actions=[],
                pending_concept=plan,
            ) or session
            await _fire_event(emit, "status", message="Building concept plan…")
            # Soft "let's make a short" → deterministic concept card (no Approve, no LLM spend).
            if _soft_proposal_now and not plan_only:
                assistant_text = concept_plan_mod.concept_to_assistant_markdown(plan)
                messages.append({"role": "assistant", "content": assistant_text})
                store.update_session(
                    sid,
                    messages=messages,
                    pending_actions=[],
                    pending_concept=plan,
                )
                await _fire_event(emit, "pending_actions", actions=[])
                return {
                    "session_id": sid,
                    "assistant_message": assistant_text,
                    "pending_actions": [],
                    "pending_concept": plan,
                    "concept_plan": plan,
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
            # Pure strategy: inject concept so the model discusses it; card still available.
            messages.append({
                "role": "system",
                "content": (
                    "[PLANNING MODE — user has not hard-committed to production]\n"
                    "A draft concept is stored internally; do not describe it as a visible card yet.\n"
                    f"Format: {plan.get('format')} · Title: {plan.get('title')} · "
                    f"~{plan.get('duration_sec')}s · {plan.get('scene_count')} scenes\n"
                    f"Hook: {plan.get('hook')}\n"
                    "Answer the latest question naturally and keep refining the draft. Do not force a checklist "
                    "or ask for production confirmation unless the creator says they are fully ready.\n"
                    "Do NOT call start_shortform_generate or start_longform_render. Do NOT invent an Approve card."
                ),
            })
            store.update_session(sid, messages=messages, pending_concept=plan)

    # Explicit production starts are deterministic. If the model previously
    # prepared a start action but narrated instead of firing it, recover the
    # exact stored arguments and surface/execute them before another LLM call.
    if (
        not reply_to
        and not ideation_turn
        and not _no_production_card
        and _wants_production_execution(intent_text or user_text)
    ):
        # ROOT: resolve title from THIS message + latest assistant outline, then lock on session.
        _commit_title = store.resolve_current_production_target(session, messages)
        _scene_one_commit = store.is_scene_one_proof_commit(intent_text or user_text)
        try:
            session = store.reconcile_production_state(
                session,
                messages=messages,
                user_text=intent_text or user_text,
                persist=True,
            ) or session
            _commit_title = store.resolve_current_production_target(session, messages) or _commit_title
        except Exception:
            pass
        try:
            from studio_agent.conversation import infer_intent_updates_from_user, update_conversation_intent

            _intent_updates = infer_intent_updates_from_user(intent_text or user_text, session)
            if _intent_updates:
                session = store.update_session(
                    sid,
                    conversation_intent=update_conversation_intent(session, **_intent_updates),
                ) or session
                if not _commit_title:
                    _commit_title = store.resolve_current_production_target(session, messages)
        except Exception:
            pass
        if _commit_title:
            try:
                from studio_agent.conversation import update_conversation_intent

                _intent = update_conversation_intent(session, locked_title=_commit_title)
                session = store.update_session(sid, conversation_intent=_intent) or session
            except Exception:
                pass
            # If prior Ready short differs, detach it before preparing Approve.
            _prior = store.prior_production_title(session)
            if _prior and store._title_overlap_score(_commit_title, _prior) < 0.75:
                session = store.clear_stale_production_context(session)
                session = store.update_session(
                    sid,
                    active_jobs=[],
                    last_production={},
                    pending_actions=[],
                    skip_job_recovery=True,
                    blocked_job_ids=list(session.get("blocked_job_ids") or []),
                    conversation_intent=session.get("conversation_intent"),
                ) or session
                await _fire_event(emit, "active_jobs", jobs=[])
                await _fire_event(emit, "pending_actions", actions=[])

        # Prefer converting a confirmed concept plan → production pending.
        pending_concept = session.get("pending_concept")
        if (
            isinstance(pending_concept, dict)
            and str(pending_concept.get("status") or "") in {"awaiting_confirm", "confirmed", ""}
            and (
                store.is_hard_production_commit(intent_text or user_text)
                or store.is_explicit_production_request(intent_text or user_text)
                or store.is_scene_one_proof_commit(intent_text or user_text)
            )
        ):
            from studio_agent import concept_plan as concept_plan_mod

            # Scene-1 visual proof always starts fresh — never resurrect a prior Ready short.
            if _scene_one_commit and isinstance(pending_concept, dict):
                pending_concept = dict(pending_concept)
                pending_concept["scene_count"] = 1
                if _commit_title:
                    pending_concept["title"] = _commit_title
                    pending_concept["user_request"] = str(intent_text or user_text)[:500]

            # If this exact plan was already expanded from its approved proof,
            # surface that production instead of restarting the staged flow at
            # Scene 1. Explicit regenerate/remake requests still take the fresh
            # path below.
            reuse_existing = (
                not _scene_one_commit
                and not re.search(
                    r"\b(?:regenerate|remake|start over|from scratch|new version)\b",
                    intent_text or user_text,
                    re.I,
                )
            )
            _expand_intent = bool(
                re.search(
                    r"\b(?:expand|rest of|remaining|full short|complete the|build the rest|render the rest)\b",
                    intent_text or user_text,
                    re.I,
                )
            )
            if reuse_existing and str(pending_concept.get("format") or "shortform").lower() != "longform":
                _reuse_title = str(_commit_title or "").strip()
                if not _reuse_title:
                    reuse_existing = False
                else:
                    _plan_title = str(pending_concept.get("title") or "")
                    _prior_reuse = store.prior_production_title(session)
                    if _plan_title and _prior_reuse and store._title_overlap_score(_plan_title, _prior_reuse) < 0.75:
                        reuse_existing = False
                    if (
                        reuse_existing
                        and _prior_reuse
                        and store._title_overlap_score(_reuse_title, _prior_reuse) >= 0.75
                        and not _expand_intent
                    ):
                        reuse_existing = False
                    if _plan_title and store._title_overlap_score(_plan_title, _reuse_title) < 0.75:
                        pending_concept = dict(pending_concept)
                        pending_concept["title"] = _reuse_title
                existing = (
                    _matching_expanded_shortform_job(
                        session,
                        title=_reuse_title,
                        target_scene_count=int(pending_concept.get("scene_count") or 1),
                    )
                    if reuse_existing
                    else None
                )
                if existing:
                    existing_job, existing_snapshot = existing
                    snap_status = str(existing_snapshot.get("status") or "").lower()
                    is_complete = (
                        snap_status in {"complete", "ready", "done", "finished"}
                        or bool(existing_snapshot.get("mp4_url") or existing_snapshot.get("download_url"))
                    )
                    blocked = {
                        str(value).strip()
                        for value in (session.get("blocked_job_ids") or [])
                        if str(value).strip()
                    }
                    if str(existing_job.get("job_id") or "").strip() in blocked:
                        existing = None
                    elif is_complete and not _expand_intent:
                        existing = None
                if existing:
                    existing_job, existing_snapshot = existing
                    active_jobs = [existing_job]
                    assistant_text = (
                        "The full short is already built on the approved Scene 1 job. "
                        "I restored its complete scene review below instead of creating another proof."
                    )
                    messages.append({"role": "assistant", "content": assistant_text})
                    store.update_session(
                        sid,
                        messages=messages,
                        active_jobs=active_jobs,
                        pending_actions=[],
                        pending_concept={**pending_concept, "status": "started"},
                    )
                    await _fire_event(emit, "active_jobs", jobs=active_jobs)
                    await _fire_event(emit, "job_snapshot", snapshot=existing_snapshot)
                    return _turn_result(session, {
                        "session_id": sid,
                        "assistant_message": assistant_text,
                        "pending_actions": [],
                        "pending_concept": {**pending_concept, "status": "started"},
                        "concept_plan": {**pending_concept, "status": "started"},
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
                    })

            # Rewrite stale concept title before convert (was the Approve title source).
            if _commit_title:
                plan_title = str(pending_concept.get("title") or "")
                if not plan_title or store._title_overlap_score(plan_title, _commit_title) < 0.75:
                    pending_concept = dict(pending_concept)
                    pending_concept["title"] = _commit_title
                    pending_concept["user_request"] = str(intent_text or user_text)[:500]

            name, args = concept_plan_mod.concept_to_production_args(
                pending_concept,
                session=session,
                user_text=intent_text or user_text,
            )
            if name == "start_shortform_generate":
                force_next = True  # hard-commit path always fresh when title was resolved/locked
                if not _commit_title and not store.is_new_production_request(intent_text or user_text, session):
                    force_next = bool(session.get("skip_job_recovery"))
                args = _prepare_shortform_production_args(
                    args, session, messages=messages, force_fresh=force_next,
                )
                args = _force_production_title_on_args(
                    args, session=session, user_text=intent_text or user_text,
                )
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
            confirmed = dict(pending_concept)
            confirmed["status"] = "confirmed"
            confirmed["confirmed_at"] = time.time()
            await _fire_event(
                emit,
                "tool_start",
                tool=name,
                round=0,
                awaiting_approval=approval_mode == "confirm" and requires_approval(name),
                deterministic_start=True,
                from_concept=True,
            )
            if approval_mode == "confirm" and requires_approval(name):
                action_id = f"act_{uuid.uuid4().hex[:12]}"
                summary_bits = concept_plan_mod.format_concept_for_pending_summary(confirmed)
                # Final title stamp on the Approve card payload.
                if name == "start_shortform_generate":
                    args = _force_production_title_on_args(
                        args, session=session, user_text=intent_text or user_text,
                    )
                    confirmed["title"] = str(args.get("title") or confirmed.get("title") or "")
                    summary_bits = concept_plan_mod.format_concept_for_pending_summary(confirmed)
                action = {
                    "id": action_id,
                    "tool": name,
                    "arguments": args,
                    "summary": f"{name} · {summary_bits}",
                    "budget": budget_payload,
                    "from_concept_id": confirmed.get("id"),
                }
                from studio_agent.conversation import conversational_production_prepared

                locked_title = str(args.get("title") or confirmed.get("title") or "Untitled")
                assistant_text = (
                    f"Concept locked: **{locked_title}** "
                    f"(~{concept_plan_mod._human_duration(int(confirmed.get('duration_sec') or 30))}).\n\n"
                    + conversational_production_prepared(
                        title=(
                            "this long-form video"
                            if str(confirmed.get("format") or "") == concept_plan_mod.FORMAT_LONG
                            else ""
                        ),
                        auto_start=False,
                    )
                )
                messages.append({"role": "assistant", "content": assistant_text})
                store.update_session(
                    sid,
                    messages=messages,
                    pending_actions=[action],
                    pending_concept=confirmed,
                    last_production={
                        "tool": name,
                        "arguments": args,
                        "updated_at": time.time(),
                        "concept_plan_id": confirmed.get("id"),
                    },
                )
                await _fire_event(emit, "pending_actions", actions=[action])
                await _fire_event(emit, "concept_plan", plan=confirmed)
                await _fire_event(emit, "tool_end", tool=name, status="awaiting_approval")
                return {
                    "session_id": sid,
                    "assistant_message": assistant_text,
                    "pending_actions": [action],
                    "pending_concept": confirmed,
                    "concept_plan": confirmed,
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
            # auto mode: execute immediately
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
                    f"Production started from your concept{f' as {job_id}' if job_id else ''}. "
                    f"Current status: {status}."
                )
                await _fire_event(emit, "active_jobs", jobs=active_jobs)
                await _fire_event(emit, "tool_end", tool=name, status="ok")
            messages.append(_tool_observation_message(name, result))
            messages.append({"role": "assistant", "content": assistant_text})
            confirmed = dict(pending_concept)
            confirmed["status"] = "started"
            store.update_session(
                sid,
                messages=messages,
                active_jobs=active_jobs,
                pending_concept=confirmed,
                pending_actions=[],
                last_production={
                    "tool": name,
                    "arguments": args,
                    "updated_at": time.time(),
                    "concept_plan_id": confirmed.get("id"),
                },
            )
            return {
                "session_id": sid,
                "assistant_message": assistant_text,
                "pending_actions": [],
                "pending_concept": confirmed,
                "concept_plan": confirmed,
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

        existing_pending = list(session.get("pending_actions") or [])
        if existing_pending:
            turn_session = dict(session)
            turn_session["messages"] = messages
            existing_pending, blocked_pending = _filter_stale_pending_actions(existing_pending, turn_session)
            existing_pending = _prepare_pending_actions(existing_pending, turn_session, messages=messages)
            if blocked_pending:
                messages.append({
                    "role": "system",
                    "content": (
                        "[Studio Agent cleared stale pending production action before continuing.]\n"
                        + "\n".join(blocked_pending)
                        + "\nPrepare a new action for the user's latest request instead of reusing the stale one."
                    ),
                })
                session = store.update_session(sid, messages=messages, pending_actions=existing_pending)
            if not existing_pending:
                await _fire_event(emit, "pending_actions", actions=[])
            else:
                from studio_agent.conversation import conversational_production_prepared

                assistant_text = conversational_production_prepared(auto_start=False)
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
        recovered = _recover_requested_production(session, intent_text or user_text)
        # Never recover last_production when THIS message names a different title.
        if recovered and _commit_title:
            _rec_name, _rec_args = recovered
            _rec_title = str((_rec_args or {}).get("title") or (_rec_args or {}).get("topic") or "")
            if _rec_title and store._title_overlap_score(_rec_title, _commit_title) < 0.75:
                recovered = None
        conflict = _production_conflict_with_latest_user(recovered, {"session_id": sid, "messages": messages})
        if conflict:
            messages.append({
                "role": "system",
                "content": (
                    "[Studio Agent rejected recovered stale production.]\n"
                    f"{conflict}\n"
                    "Build a fresh production from the latest user request instead."
                ),
            })
            session = store.update_session(
                sid,
                messages=messages,
                pending_actions=[],
                last_production=None,
            )
            recovered = None
        if not recovered:
            recovered = _build_requested_topic_production(
                session,
                intent_text or user_text,
                content_format=content_format,
                active_registry=active_registry,
                active_channel_id=active_channel_id,
                messages=messages,
            )
        if recovered and _wants_expand_visual_proof_short(intent_text or user_text):
            recovered = None
        if recovered:
            name, args = recovered
            if name == "start_shortform_generate":
                args = _prepare_shortform_production_args(
                    args, session, messages=messages, force_fresh=True,
                )
                args = _force_production_title_on_args(
                    args, session=session, user_text=intent_text or user_text,
                )
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
                if name == "start_shortform_generate":
                    args = _force_production_title_on_args(
                        args, session=session, user_text=intent_text or user_text,
                    )
                action_id = f"act_{uuid.uuid4().hex[:12]}"
                action = {
                    "id": action_id,
                    "tool": name,
                    "arguments": args,
                    "summary": f"{name}({json.dumps(args)[:200]})",
                    "budget": budget_payload,
                }
                from studio_agent.conversation import conversational_production_prepared

                _show_title = str(args.get("title") or args.get("topic") or "").strip()
                assistant_text = conversational_production_prepared(auto_start=False)
                if _show_title:
                    assistant_text = (
                        f"Prepared production for **{_show_title}**.\n\n{assistant_text}"
                    )
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
        motion_critique = _is_motion_quality_critique_request(user_text) or _is_animation_artifact_fix_request(user_text)
        if motion_critique and not is_long:
            context_note = (
                f"[User is critiquing animation on {kind} job_id={job_id}.{scene_hint} "
                "Preserve the approved still. Call repair_production_scene_animation("
                "job_id, scene_index, reason=the user's exact critique) so Studio rewrites "
                "pose/VFX/background motion from their words and re-animates. "
                "Do NOT call re_edit_production, start_shortform_generate, or regenerate the still "
                "unless they explicitly ask to change the still.]"
            )
        else:
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
        from studio_agent.conversation import (
            format_catalyst_for_prompt,
            format_intent_for_prompt,
            infer_intent_updates_from_user,
            update_conversation_intent,
        )

        # Refresh session continuity from this user message (niche/product/mode/title).
        # NOTE: infer_intent_updates_from_user must accept session — a prior arity bug
        # TypeError'd here every turn and wiped memory + continuity + catalyst entirely.
        intent_updates = infer_intent_updates_from_user(intent_text or user_text, session)
        if intent_updates or session.get("channel_title") or session.get("channel_id"):
            intent = update_conversation_intent(
                session,
                niche=str(intent_updates.get("niche") or ""),
                product=str(intent_updates.get("product") or ""),
                mode=str(intent_updates.get("mode") or session.get("production_intent") or ""),
                channel_title=str(session.get("channel_title") or ""),
                channel_id=str(session.get("channel_id") or ""),
                registry_key=str(session.get("registry_key") or active_registry or ""),
                render_style=str(intent_updates.get("render_style") or ""),
                clear_locked_title=bool(intent_updates.get("clear_locked_title")),
                locked_title=str(intent_updates.get("locked_title") or ""),
                last_topic=str(intent_updates.get("last_topic") or ""),
            )
            # Store an explicit spoken style in both the intent ledger and the
            # picker/session source used by every later production tool.
            fields: dict[str, Any] = {"conversation_intent": intent}
            if intent_updates.get("render_style"):
                fields["render_style"] = str(intent_updates["render_style"])
            store.update_session(sid, **fields)
            session = store.get_session(sid) or session

        # Title correction diagnostics: clear stale last_production when user rejects a title.
        try:
            from studio_agent.conversation import extract_rejected_title

            rejected = extract_rejected_title(user_text)
            if rejected:
                lp = session.get("last_production") if isinstance(session.get("last_production"), dict) else {}
                lp_title = str(
                    (lp.get("arguments") or {}).get("title")
                    or (lp.get("arguments") or {}).get("topic")
                    or ""
                ).strip()
                if lp_title and store._title_overlap_score(lp_title, rejected) >= 0.34:
                    store.update_session(sid, last_production={}, pending_actions=[])
                    session = store.get_session(sid) or session
                messages.append({
                    "role": "system",
                    "content": (
                        f"[TITLE CORRECTION — user rejected \"{rejected}\"]\n"
                        f"Working title is "
                        f"\"{str((session.get('conversation_intent') or {}).get('locked_title') or (session.get('conversation_intent') or {}).get('working_title') or 'the user-chosen title')}\". "
                        "Do not prepare or refer to the rejected title as what we are making. "
                        "Competitor titles from public data are research comps only."
                    ),
                })
        except Exception:
            pass

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
        continuity = format_intent_for_prompt(session)
        catalyst_block = format_catalyst_for_prompt(session)
        memory_summary = f"{memory_summary}\n\n{continuity}\n\n{catalyst_block}".strip()
    except Exception:
        # Never blank the whole memory stack — fall back to user memory only.
        try:
            memory_summary = memory.summarize_for_prompt(
                str(user_id),
                channel_id=str(session.get("channel_id") or ""),
                registry_key=str(session.get("registry_key") or ""),
            )
        except Exception:
            memory_summary = ""
    sys_content = system_prompt(
        content_format=content_format,
        reasoning_depth=reasoning_depth,
        billing_profile=profile,
        render_style=str(session.get("render_style") or "cinematic"),
        image_model=str(session.get("image_model") or store.DEFAULT_IMAGE_MODEL),
        video_model=str(session.get("video_model") or "seedance"),
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

    production_intent = str(session.get("production_intent") or store.detect_production_intent(user_text, session)).strip()
    from studio_agent.turn_plan import (
        STEP_CHANNEL_ANALYTICS,
        STEP_PUBLIC_YOUTUBE_DEMAND,
        STEP_REFERENCE_ANALYSIS,
        derive_niche_search_query,
        reference_ready_for_public_search,
    )
    public_search_blocked_pending_reference = False
    channel_data_preflight_required = turn_plan.channel_analytics or _needs_channel_data_preflight(user_text)
    if _needs_catalyst_preflight_for_production(user_text, production_intent):
        channel_data_preflight_required = True
    latest_upload_focus_required = _needs_latest_upload_focus(user_text)
    current_video_audit_required = _needs_current_video_audit(user_text)
    if current_video_audit_required:
        channel_data_preflight_required = True
        latest_upload_focus_required = True
    if not channel_data_preflight_required and _is_channel_data_followup(user_text):
        channel_data_preflight_required = _recent_assistant_promised_channel_data(messages)
    competitor_channel_required = store.is_competitor_channel_reference_request(intent_text or user_text)
    channel_url_reference_required = bool(
        store.is_youtube_channel_url_reference_request(intent_text or user_text)
        or store.is_channel_video_analysis_request(intent_text or user_text)
    )
    live_demand_preflight_required = _needs_live_demand_preflight(
        intent_text or user_text,
        session,
        production_intent=production_intent,
    )
    public_search_preflight_required = (
        turn_plan.public_youtube_demand
        or live_demand_preflight_required
        or _needs_public_search_preflight(user_text)
        or store.is_public_youtube_research_request(user_text)
        or (
            competitor_channel_required
            and not channel_url_reference_required
        )
    )
    fresh_public_search_required = _public_search_use_fresh(
        user_text,
        public_demand=bool(
            turn_plan.public_youtube_demand
            or live_demand_preflight_required
            or store.is_public_youtube_research_request(user_text)
            or store.is_competitor_channel_reference_request(intent_text or user_text)
        ),
    )
    if current_video_audit_required:
        public_search_preflight_required = True
        fresh_public_search_required = True
    if not public_search_preflight_required and channel_data_preflight_required and latest_upload_focus_required:
        public_search_preflight_required = True
        fresh_public_search_required = True
    if not public_search_preflight_required and _is_channel_data_followup(user_text):
        public_search_preflight_required = _recent_assistant_promised_channel_data(messages)
    if not public_search_preflight_required and _is_public_search_followup(intent_text or user_text, messages):
        public_search_preflight_required = True
        fresh_public_search_required = True

    await _fire_verification_step(
        emit,
        "request_scope",
        "done",
        label="Understand the request",
        detail="Classified whether this turn needs private channel analytics, public YouTube search data, production tools, or a direct answer.",
    )
    required_sources: list[str] = []
    if channel_data_preflight_required:
        required_sources.append("connected-channel analytics")
    if public_search_preflight_required:
        required_sources.append(
            "Live Demand (public YouTube niche, fresh window)"
            if live_demand_preflight_required
            else "public YouTube search"
        )
    await _fire_verification_step(
        emit,
        "source_plan",
        "done",
        label="Decide required data sources",
        detail=(
            f"Required: {', '.join(required_sources)}."
            if required_sources
            else "No live data source was required for this turn."
        ),
    )

    preflight_tool_fires: list[ToolFire] = []
    if _is_render_cost_question(intent_text or user_text):
        # Re-read session after chat-turn option sync so picker models are current.
        session = store.get_session(sid) or session
        from studio_agent.tools import _session_production_models

        session_models = _session_production_models(sid)
        pending_concept = session.get("pending_concept") if isinstance(session.get("pending_concept"), dict) else {}
        longform_cost = bool(
            "long" in str(content_format or session.get("content_format") or "").lower()
            or str(pending_concept.get("format") or "").lower() == "longform"
        )
        if longform_cost and _is_cost_only_question(intent_text or user_text):
            pending_concept = _recover_pending_longform_concept(session, pending_concept)
            target_duration = int(
                pending_concept.get("target_duration_sec")
                or pending_concept.get("duration_sec")
                or pending_concept.get("duration_seconds")
                or session.get("target_duration_sec")
                or 1200
            )
            no_animation = bool(re.search(r"\b(?:do not|don'?t|no need to)\s+(?:be\s+)?animate", intent_text or user_text, re.I))
            selected_cost_image_model = _cost_image_model(
                intent_text or user_text,
                session_models.get("image_model_id"),
            )
            long_args = {
                "channel_key": _longform_cost_channel_key(session),
                "title": str(pending_concept.get("title") or session.get("locked_title") or "Long-form concept"),
                "topic": str(pending_concept.get("topic") or pending_concept.get("title") or "Long-form concept"),
                "target_duration_sec": target_duration,
                "image_model_id": selected_cost_image_model,
                "motion_policy": "none" if no_animation else "balanced",
                "visual_proof_only": False,
            }
            estimate = production_budget.estimate_tool_cost("start_longform_render", long_args)
            assistant_cost = _format_grounded_longform_cost_reply(
                estimate,
                readiness_requested=_is_plan_readiness_question(intent_text or user_text),
                plan_ready=bool(pending_concept.get("title") and target_duration > 0 and selected_cost_image_model),
            )
            messages.append({"role": "assistant", "content": assistant_cost})
            store.update_session(sid, messages=messages, pending_concept=pending_concept)
            await _fire_verification_step(
                emit, "tool_evidence", "done", label="Grounded long-form cost",
                detail=f"Calculated from {selected_cost_image_model} without starting production or leaving Plan mode.",
            )
            return {
                "session_id": sid,
                "assistant_message": assistant_cost,
                "pending_actions": [],
                "active_jobs": list(session.get("active_jobs") or []),
                "approval_mode": approval_mode,
                "reasoning_depth": reasoning_depth,
                "usage": {},
                "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
            }
        duration = (
            _parse_short_duration_seconds(intent_text or user_text)
            or float(pending_concept.get("duration_sec") or pending_concept.get("duration_seconds") or 20.0)
        )
        cost_args = {
            "duration_seconds": duration,
            "scene_count": int(pending_concept.get("scene_count") or max(1, round(duration / 5.0))),
            "animate": True,
            "include_finalize": True,
            "image_model_id": session_models.get("image_model_id"),
            "video_model": session_models.get("video_model"),
        }
        await _fire_event(
            emit,
            "status",
            message=(
                f"Estimating shortform render cost for ~{int(duration)}s using "
                f"{session_models.get('image_model_id') or 'session image'} + "
                f"{session_models.get('video_model') or 'session i2v'}..."
            ),
        )
        try:
            cost_result = execute_tool_logged(
                "estimate_shortform_render_cost",
                cost_args,
                user_id=user_id,
                content_format=content_format,
                session_id=sid,
            )
        except Exception as exc:
            cost_result = json.dumps({"error": str(exc)}, indent=2)
        preflight_tool_fires.append(ToolFire("estimate_shortform_render_cost", dict(cost_args), cost_result))
        messages.append(_tool_observation_message("estimate_shortform_render_cost", cost_result))
        store.update_session(sid, messages=messages)
        # Cost-only questions must never go through the LLM cost-narration path —
        # models keep inventing LTX/Seedream pipeline pricing from training memory.
        if _is_cost_only_question(intent_text or user_text):
            grounded_cost = _latest_cost_estimate_payload(preflight_tool_fires)
            if grounded_cost and str(grounded_cost.get("formatted_quote") or "").strip():
                assistant_cost = _format_grounded_cost_reply(
                    grounded_cost,
                    user_text=intent_text or user_text,
                )
                if _is_plan_readiness_question(intent_text or user_text):
                    short_plan_ready = bool(
                        session.get("pending_concept")
                        or session.get("last_production")
                        or session.get("active_jobs")
                    )
                    readiness = (
                        "**Yes—the short is planned and ready to move forward when you explicitly approve it.**"
                        if short_plan_ready
                        else "**Not yet—the short still needs a locked concept before it is ready for production.**"
                    )
                    assistant_cost = (
                        f"{readiness}\n"
                        "This cost check is not approval and does not start production.\n\n"
                        f"{assistant_cost}"
                    )
                messages.append({"role": "assistant", "content": assistant_cost})
                store.update_session(sid, messages=messages)
                await _fire_verification_step(
                    emit,
                    "tool_evidence",
                    "done",
                    label="Grounded render cost",
                    detail=(
                        f"Used session models "
                        f"{grounded_cost.get('image_model_id')} + {grounded_cost.get('video_model')}."
                    ),
                )
                await _fire_verification_step(
                    emit,
                    "final_audit",
                    "done",
                    label="Audit final answer before replying",
                    detail="Cost answer forced from estimate_shortform_render_cost (no model freeform pricing).",
                )
                telemetry.record_session_turn(
                    user_id, sid, role="assistant", content_preview=assistant_cost,
                    model=model, content_format=content_format,
                )
                return {
                    "session_id": sid,
                    "assistant_message": assistant_cost,
                    "pending_actions": list(session.get("pending_actions") or []),
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
    reference_analysis_required = (
        turn_plan.reference_analysis
        or (
            store.is_explicit_reference_analysis_request(intent_text or user_text)
            and store.should_auto_run_tools(intent_text or user_text)
        )
    )
    youtube_url_reference_required = bool(
        store.is_youtube_url_reference_request(intent_text or user_text)
        and store.should_auto_run_tools(intent_text or user_text)
    )
    if channel_url_reference_required and not reply_to:
        await _fire_verification_step(
            emit,
            "tool_evidence",
            "running",
            label="Analyze YouTube channel",
            detail="Fetching channel uploads and analyzing the videos you asked for.",
        )
        await _fire_event(emit, "status", message="Downloading and analyzing channel videos…")
        await _run_competitor_channel_analysis_preflight(
            emit=emit,
            user_id=str(user_id),
            content_format=content_format,
            session_id=sid,
            messages=messages,
            tool_fires=preflight_tool_fires,
            user_text=intent_text or user_text,
            session=session,
        )
        ref_after_channel = _latest_reference_actionable_from_fires(preflight_tool_fires, messages=messages)
        await _fire_verification_step(
            emit,
            "tool_evidence",
            "done" if ref_after_channel else "error",
            label="Analyze YouTube channel",
            detail=(
                "Channel uploads were fetched and reference analysis evidence added."
                if ref_after_channel
                else "Channel fetch finished, but vision/transcript/story analysis did not return usable content."
            ),
        )
    contextual_reference_required = bool(
        store.is_contextual_reference_video_request(intent_text or user_text)
        and store.should_auto_run_tools(intent_text or user_text)
    )
    contextual_reference_url = ""
    if contextual_reference_required and not channel_url_reference_required:
        contextual_reference_url = _resolve_contextual_reference_video_url(
            intent_text or user_text,
            messages=messages,
            tool_fires=preflight_tool_fires,
        )

    if (
        (reference_analysis_required or youtube_url_reference_required or contextual_reference_url)
        and not reply_to
        and not channel_url_reference_required
    ):
        await _fire_verification_step(
            emit,
            "tool_evidence",
            "running",
            label="Run reference analysis",
            detail="Analyzing reference video before public market research.",
        )
        if youtube_url_reference_required:
            await _run_youtube_url_reference_preflight(
                emit=emit,
                user_id=str(user_id),
                content_format=content_format,
                session_id=sid,
                messages=messages,
                tool_fires=preflight_tool_fires,
                user_text=intent_text or user_text,
            )
        elif contextual_reference_url:
            await _run_youtube_url_reference_preflight(
                emit=emit,
                user_id=str(user_id),
                content_format=content_format,
                session_id=sid,
                messages=messages,
                tool_fires=preflight_tool_fires,
                user_text=contextual_reference_url,
                reference_request_text=intent_text or user_text,
            )
        elif reference_analysis_required:
            await _run_uploaded_reference_analysis_preflight(
                emit=emit,
                user_id=str(user_id),
                content_format=content_format,
                session_id=sid,
                messages=messages,
                tool_fires=preflight_tool_fires,
            )
        ref_after_upload = _latest_reference_actionable_from_fires(preflight_tool_fires, messages=messages)
        await _fire_verification_step(
            emit,
            "tool_evidence",
            "done" if ref_after_upload else "error",
            label="Run uploaded-reference analysis",
            detail=(
                "Reference analysis evidence added."
                if ref_after_upload
                else "Reference analysis finished pacing only — vision/transcript/story stages must succeed before planning."
            ),
        )

    exact_topic_discovery_required = store.is_exact_topic_discovery_request(intent_text or user_text)
    can_run_channel_preflight = bool(active_registry or active_channel_id)
    can_run_public_preflight = bool(public_search_preflight_required)
    if (can_run_channel_preflight and channel_data_preflight_required) or can_run_public_preflight:
        active_label = (
            str(session.get("channel_title") or "").strip()
            or str(active_registry or "").replace("_", " ")
            or "YouTube"
        )
        preflight_plan: list[tuple[str, dict[str, Any]]] = []
        if channel_data_preflight_required and can_run_channel_preflight:
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
                    {
                        "registry_key": active_registry,
                        "channel_id": active_channel_id,
                        "days": 30,
                        "fresh": fresh_public_search_required,
                    },
                ),
            ])
        if public_search_preflight_required and not exact_topic_discovery_required:
            ref_for_query = _latest_complete_reference_analysis(
                tool_fires=preflight_tool_fires,
                messages=messages,
            )
            niche_from_reference = bool(
                turn_plan.niche_from_reference
                or (turn_plan.reference_analysis and bool(reference_context))
            )
            if niche_from_reference and not reference_ready_for_public_search(
                ref_for_query,
                niche_from_reference=True,
            ):
                public_search_blocked_pending_reference = True
            else:
                from studio_agent.turn_plan import competitor_channel_search_query

                from studio_agent.turn_plan import coerce_public_search_query

                competitor_query = competitor_channel_search_query(intent_text or user_text)
                fallback_query = competitor_query or _public_search_query_for_channel(
                    active_label,
                    user_text,
                    registry_key=active_registry,
                )
                preferred_query = (
                    competitor_query
                    or derive_niche_search_query(
                        ref_for_query,
                        user_text=user_text,
                        active_label=active_label,
                        fallback_query=fallback_query,
                    )
                    if niche_from_reference
                    else fallback_query
                )
                search_query = coerce_public_search_query(
                    preferred_query,
                    user_text=user_text,
                    active_label=active_label,
                    registry_key=active_registry,
                    fallback_query=fallback_query,
                )
                window_days = _public_search_window_days(user_text)
                search_args = {
                    "query": search_query,
                    "days": window_days,
                    "fresh": bool(fresh_public_search_required),
                }
                if active_registry:
                    search_args["registry_key"] = active_registry
                # Single public search tool — dual tools doubled search.list cost.
                preflight_plan.append(("get_public_search_trends", search_args))
        elif exact_topic_discovery_required and not can_run_channel_preflight:
            # This tool performs its own public-search merge. Running another
            # search first wastes time and quota without producing a better
            # concrete topic recommendation.
            preflight_plan.append((
                "recommend_video_topics",
                {
                    "niche_query": _public_search_query_for_channel(active_label, user_text),
                    "days": 30,
                    "fresh": fresh_public_search_required,
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
        await _fire_verification_step(
            emit,
            "tool_evidence",
            "running",
            label="Run required data tools",
            detail=f"Pulling {active_label} {scope}.",
        )
        await _fire_event(emit, "status", message=f"Pulling {active_label} {scope}...")
        any_preflight_error = False
        for pf_name, pf_args in preflight_plan:
            await _fire_tool_start(emit, pf_name, args=pf_args, round=0, awaiting_approval=False)
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
            any_preflight_error = any_preflight_error or bool(err_preview)
            await _fire_tool_end(
                emit,
                pf_name,
                status="error" if err_preview else "ok",
                args=pf_args,
                result=pf_result,
                error=err_preview or None,
            )
        await _fire_verification_step(
            emit,
            "tool_evidence",
            "error" if any_preflight_error else "done",
            label="Run required data tools",
            detail=(
                "At least one required data tool returned an error; the final answer must name that blocker."
                if any_preflight_error
                else f"Completed {len(preflight_tool_fires)} required data tool call(s)."
            ),
        )
        if public_search_blocked_pending_reference:
            messages.append({
                "role": "system",
                "content": (
                    "[Studio Agent public-search DAG gate] Public YouTube search was withheld because "
                    "reference analysis only reached pacing metrics and did not return a topic signal. "
                    "Answer from reference stage errors; do not query or invent market trends from chat text."
                ),
            })
            store.update_session(sid, messages=messages)
        if current_video_audit_required and any(str(f.name or "") == "get_channel_analytics" for f in preflight_tool_fires):
            await _run_current_video_analysis_preflight(
                emit=emit,
                user_id=user_id,
                content_format=content_format,
                session_id=sid,
                messages=messages,
                tool_fires=preflight_tool_fires,
            )
        if preflight_tool_fires:
            preflight_note = (
                "[Studio Agent required data preflight is complete for this turn. "
                "Answer from the tool observations already in this context. Do not say you still need to pull "
                "channel/search data or that you will use the analytics/search tool next; if a tool returned limited data, "
                "state the exact limitation from the tool payload and then give the grounded next step.]"
            )
            if turn_plan.has_execution:
                preflight_note = (
                    "[Studio Agent approved research preflight is complete. Synthesize NOW from tool observations "
                    "already in this context: reference visual/story readout (if present) + public YouTube demand rows. "
                    "Do NOT narrate polling or say tools are unavailable. Google Trends/keyword-volume APIs are not "
                    "wired yet — use get_public_search_trends evidence. Do NOT start production unless explicitly asked.]"
                )
            elif ideation_turn:
                preflight_note = (
                    "[Studio Agent ideation preflight is complete. Public niche research is already in this context. "
                    "Answer NOW with channel positioning, art-style direction, and market readout from those tool results. "
                    "Do NOT say you will analyze the upload or pull YouTube data later. "
                    "Do NOT call analyze_reference_video or start production unless the user explicitly asks.]"
                )
            messages.append({"role": "system", "content": preflight_note})
            store.update_session(sid, messages=messages)
            await _fire_verification_step(
                emit,
                "source_integrity",
                "done",
                label="Verify tool results before answer",
                detail="Tool observations were added to the model context; final answer must use those observations or state exact limitations.",
            )
    else:
        await _fire_verification_step(
            emit,
            "tool_evidence",
            "skipped",
            label="Run required data tools",
            detail=(
                "No live data tool was required."
                if not (channel_data_preflight_required or public_search_preflight_required)
                else "Required channel data was not available; the final answer must say what is missing."
            ),
            required=bool(channel_data_preflight_required or public_search_preflight_required),
        )
        await _fire_verification_step(
            emit,
            "source_integrity",
            "done",
            label="Verify tool results before answer",
            detail="No tool evidence was available for this turn; unsupported live-data claims are blocked by the final audit.",
        )

    if reply_to and not _is_reply_to_scene_still_edit(user_text, reply_to):
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
            audit = audit_turn(
                assistant_text=assistant_text,
                user_text=user_text,
                tool_fires=tool_fires,
                conversational_mode=conversational_turn,
            )
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

    if (
        not reply_to
        and competitor_channel_required
        and preflight_tool_fires
        and (
            _latest_complete_reference_analysis(tool_fires=preflight_tool_fires, messages=messages)
            or _has_public_demand_tool(preflight_tool_fires)
        )
    ):
        from studio_agent.turn_plan import competitor_channel_search_query, extract_competitor_channel_label

        from studio_agent.conversation import (
            strip_robot_research_artifacts,
            synthesize_conversational_research_reply,
        )

        competitor_label = extract_competitor_channel_label(intent_text or user_text) or "reference channel"
        search_query = competitor_channel_search_query(intent_text or user_text) or _latest_public_search_query(
            preflight_tool_fires
        )
        ref_payload = _latest_complete_reference_analysis(tool_fires=preflight_tool_fires, messages=messages)
        evidence = ""
        if _has_public_demand_tool(preflight_tool_fires):
            evidence = _grounded_research_summary_from_tools(
                preflight_tool_fires,
                active_label=competitor_label,
                user_text=intent_text or user_text,
                include_channel=False,
                search_query=search_query,
                reference_payload=ref_payload,
            )
        ref_findings = (
            _format_reference_analysis_findings(ref_payload) if ref_payload else ""
        )
        if not evidence and not ref_findings:
            assistant_text = (
                f"I couldn't finish the analysis for **{competitor_label}** this turn. "
                "Retry once with the same channel URL, or paste one direct video URL."
            )
        else:
            assistant_text = await synthesize_conversational_research_reply(
                user_text=intent_text or user_text,
                evidence=evidence or "No public demand rows this turn.",
                reference_findings=ref_findings,
                niche_hint=competitor_label,
                model=str(model or session.get("model") or ""),
            )
        assistant_text = sanitize_assistant_text(strip_robot_research_artifacts(assistant_text))
        messages.append({"role": "assistant", "content": assistant_text})
        _catalyst_capture_turn(
            user_id=str(user_id),
            session=session,
            turn_kind="reference_analysis" if ref_payload else "public_research",
            reference_payload=ref_payload,
            tool_fires=preflight_tool_fires,
            search_query=search_query,
        )
        store.update_session(sid, messages=messages, pending_actions=[], last_production={})
        await _fire_event(emit, "pending_actions", actions=[])
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

    # Research / Live Demand turns: tools ground truth; conversation synthesizes the answer.
    # Grok-class rule — never ship the raw research form as the whole product reply.
    try:
        from studio_agent.live_demand import has_demand_signal, is_research_execution_request
        # Demand/research language wins over soft "then we can make a short" production hedges.
        _hard_production_now = bool(_wants_production_execution(intent_text or user_text)) and not (
            has_demand_signal(intent_text or user_text) or is_research_execution_request(intent_text or user_text)
        )
        _production_now = bool(
            (
                store.is_explicit_production_request(intent_text or user_text)
                and not (
                    has_demand_signal(intent_text or user_text)
                    or is_research_execution_request(intent_text or user_text)
                )
            )
            or _hard_production_now
        )
        _research_only_finish = bool(
            not _production_now
            and (
                is_research_execution_request(intent_text or user_text)
                or has_demand_signal(intent_text or user_text)
                or _is_public_search_followup(intent_text or user_text, messages)
                or live_demand_preflight_required
                or public_search_preflight_required
                or _has_public_demand_tool(preflight_tool_fires)
            )
        )
    except Exception:
        _research_only_finish = bool(
            _is_public_search_followup(intent_text or user_text, messages)
            or _has_public_demand_tool(preflight_tool_fires)
        )
    # Late Live Demand: demand language but preflight never fired (or returned empty soup).
    if (
        not reply_to
        and not competitor_channel_required
        and _research_only_finish
        and (
            not _has_public_demand_tool(preflight_tool_fires)
            or _public_search_hydrated_count(preflight_tool_fires) <= 0
        )
    ):
        try:
            await _fire_event(emit, "status", message="Live Demand: resolving niche and pulling public YouTube…")
            late = await _run_public_youtube_research_preflight(
                emit=emit,
                user_id=str(user_id),
                content_format=content_format,
                session_id=sid,
                messages=messages,
                user_text=intent_text or user_text,
                session=session,
                active_registry=active_registry,
                active_channel_id=active_channel_id,
            )
            if late:
                preflight_tool_fires = list(preflight_tool_fires or []) + list(late)
                session = store.get_session(sid) or session
        except Exception:
            pass
    if (
        not reply_to
        and not competitor_channel_required
        and _research_only_finish
        and preflight_tool_fires
        and _has_public_demand_tool(preflight_tool_fires)
    ):
        from studio_agent.conversation import synthesize_conversational_research_reply
        from studio_agent.live_demand import extract_niche_hint, resolve_demand_search_query

        active_label = (
            str(session.get("channel_title") or "").strip()
            or str(active_registry or "").replace("_", " ")
            or "YouTube"
        )
        ref_payload = _latest_complete_reference_analysis(
            tool_fires=preflight_tool_fires,
            messages=messages,
        )
        search_query = (
            resolve_demand_search_query(
                intent_text or user_text,
                session,
                active_label=active_label,
                registry_key=active_registry,
                fallback_query=_latest_public_search_query(preflight_tool_fires),
            )
            or _latest_public_search_query(preflight_tool_fires)
        )
        # Channel analytics only when we actually have a connected/selected channel
        # with tool evidence — market research never requires OAuth.
        _has_channel_evidence = bool(
            active_channel_id
            and (
                str(session.get("channel_title") or "").strip()
                or _has_channel_analytics_tool(preflight_tool_fires)
            )
        )
        evidence = _grounded_research_summary_from_tools(
            preflight_tool_fires,
            active_label=active_label,
            user_text=intent_text or user_text,
            include_channel=_has_channel_evidence,
            search_query=search_query,
            reference_payload=ref_payload,
        )
        # Best/worst Shorts on a connected channel → channel plan synthesis, not public-demand template.
        if (
            store.is_best_vs_worst_shorts_request(intent_text or user_text)
            and _has_channel_analytics_tool(preflight_tool_fires)
        ):
            channel_plan = _grounded_channel_plan_from_tools(
                preflight_tool_fires,
                active_label=active_label,
                user_text=intent_text or user_text,
            )
            if channel_plan:
                evidence = f"{channel_plan}\n\n{evidence}"
        ref_findings = (
            _format_reference_analysis_findings(ref_payload)
            if ref_payload
            else ""
        )
        niche_hint = (
            extract_niche_hint(intent_text or user_text, session)
            or search_query
            or active_label
        )
        # Evidence stays in system context for the model if later turns need it —
        # user-facing reply is conversational only (no form dump).
        messages.append({
            "role": "system",
            "content": (
                "[Internal Live Demand evidence — for grounding only; never paste as a form to the user]\n"
                + evidence[:8000]
            ),
        })
        from studio_agent.conversation import (
            get_conversation_intent,
            strip_robot_research_artifacts,
            update_conversation_intent,
            weave_catalyst_into_reply,
        )

        assistant_text = sanitize_assistant_text(
            strip_robot_research_artifacts(
                await synthesize_conversational_research_reply(
                    user_text=intent_text or user_text,
                    evidence=evidence,
                    reference_findings=ref_findings,
                    niche_hint=str(niche_hint or ""),
                    model=str(model or session.get("model") or ""),
                )
            )
        )
        _catalyst_capture_turn(
            user_id=str(user_id),
            session=session,
            turn_kind="public_research",
            reference_payload=ref_payload,
            tool_fires=preflight_tool_fires,
            search_query=search_query,
        )
        # Reload intent after Catalyst write so we can weave notes into the reply.
        session = store.get_session(sid) or session
        assistant_text = sanitize_assistant_text(
            weave_catalyst_into_reply(assistant_text, get_conversation_intent(session))
        )
        messages.append({"role": "assistant", "content": assistant_text})
        try:
            intent = update_conversation_intent(
                session,
                niche=str(niche_hint or "")[:120],
                search_query=str(search_query or "")[:220],
                channel_title=str(session.get("channel_title") or active_label or ""),
                channel_id=str(session.get("channel_id") or ""),
                registry_key=str(session.get("registry_key") or active_registry or ""),
                kind="public_research",
            )
            store.update_session(
                sid,
                messages=messages,
                pending_actions=[],
                last_production={},
                conversation_intent=intent,
            )
        except Exception:
            store.update_session(sid, messages=messages, pending_actions=[], last_production={})
        await _fire_event(emit, "pending_actions", actions=[])
        await _fire_verification_step(
            emit,
            "final_audit",
            "done",
            label="Audit final answer before replying",
            detail="Grok-class reply synthesized from verified Live Demand evidence.",
        )
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

    tool_fires: list[ToolFire] = list(preflight_tool_fires)
    ref_preflight_payload = _latest_complete_reference_analysis(tool_fires=tool_fires, messages=messages)
    ref_only_execution = bool(
        turn_plan.reference_analysis
        and not turn_plan.public_youtube_demand
        and not turn_plan.channel_analytics
    )
    channel_analysis_turn = bool(
        channel_url_reference_required
        or store.is_channel_video_analysis_request(intent_text or user_text)
    )
    skip_model_loop = bool(ref_only_execution and preflight_tool_fires)
    pending: list[dict[str, Any]] = []
    active_jobs: list[dict[str, Any]] = []
    assistant_text = ""
    if skip_model_loop:
        if ref_preflight_payload:
            assistant_text = _format_reference_analysis_findings(ref_preflight_payload)
            _catalyst_capture_turn(
                user_id=str(user_id),
                session=session,
                turn_kind="reference_analysis",
                reference_payload=ref_preflight_payload,
                tool_fires=tool_fires,
            )
        elif channel_analysis_turn:
            # Prefer the pre-built channel summary over the upload-centric error template.
            stashed = ""
            for msg in reversed(messages or []):
                content = str(msg.get("content") or "")
                if content.startswith("[CHANNEL_ANALYSIS_USER_SUMMARY]"):
                    stashed = content.split("\n", 1)[-1].strip()
                    break
            assistant_text = stashed or _format_channel_analysis_outcome(tool_fires)
            if not stashed and not assistant_text.strip():
                assistant_text = _format_reference_preflight_failure(
                    tool_fires,
                    channel_context=True,
                )
        else:
            from studio_agent.attachments import resolve_video_attachment_path

            local_path = resolve_video_attachment_path(sid, str(user_id), messages=messages)
            assistant_text = _format_reference_preflight_failure(
                tool_fires,
                local_path=local_path,
                channel_context=False,
            )

    usage_total: dict[str, Any] = {}
    acc_prompt_tokens = 0
    acc_completion_tokens = 0
    model_provider = "openrouter"
    effective_model = model
    production_tools_allowed = not _blocks_brand_new_production(intent_text or user_text)
    production_tool_offered = False
    offered_tool_names: frozenset[str] = frozenset()

    if not skip_model_loop:
        tools = tools_for_user(str(user_id or session.get("user_id") or ""))
        # During a reply-to re-edit (user clicked the arrow on a specific video card),
        # completely remove ALL high-level "start a whole new production" tools from what the model can call.
        # This forces the surgical re-edit path on the exact prior job_id (list scenes + granular edits + re_edit_production + finalize).
        # The model must reuse the existing stills/clips from the video the user is replying to.
        blocked_production_tools: set[str] = set()
        if reply_to:
            blocked_production_tools |= set(BRAND_NEW_PRODUCTION_TOOLS)
        if ideation_turn:
            blocked_production_tools |= set(BRAND_NEW_PRODUCTION_TOOLS)
        if plan_only:
            # One narrow Plan-mode execution exception: packaging can be
            # validated visually before the user commits to the full video.
            blocked_production_tools |= {
                str(t.get("function", {}).get("name") or "")
                for t in tools
                if _plan_mode_blocks_tool(str(t.get("function", {}).get("name") or ""))
            }
        # DEFAULT DENY: hide start_shortform/longform unless user hard-committed.
        production_tools_allowed = not _blocks_brand_new_production(intent_text or user_text)
        if not production_tools_allowed:
            blocked_production_tools |= set(BRAND_NEW_PRODUCTION_TOOLS)
        # Research preflight already ran BEFORE the model loop. Do NOT hide production
        # tools on hard-commit turns — that made the model invent "tool availability
        # issue" / "once the tool comes back" while start_shortform was simply unoffered.
        # Soft research-only turns stay gated by production_tools_allowed above.
        if conversational_turn and not store.should_auto_run_tools(intent_text or user_text):
            blocked_production_tools |= set(CONVERSATIONAL_DATA_TOOLS)
        preflight_reference_complete = bool(
            ref_preflight_payload and _reference_analysis_actionable(ref_preflight_payload)
        )
        if turn_plan.has_execution and preflight_tool_fires and (
            preflight_reference_complete or not turn_plan.reference_analysis
        ):
            # Approved research DAG already ran in preflight; block re-entry that causes polling narration loops.
            blocked_production_tools |= set(CONVERSATIONAL_DATA_TOOLS)
            blocked_production_tools.add("poll_render_job")
        if blocked_production_tools:
            tools = [
                t for t in tools
                if t.get("function", {}).get("name") not in blocked_production_tools
            ]
        offered_tool_names = _offered_model_tool_names(tools)
        production_tool_offered = bool(
            offered_tool_names & {"start_shortform_generate", "start_longform_render"}
        )
        # Tell the model the truth when production tools are gated so it never invents
        # "tool availability / offline" excuses for intentional Plan/no-commit gates.
        if not production_tool_offered and not plan_only:
            messages.append({
                "role": "system",
                "content": (
                    "[Studio production gate] start_shortform_generate / start_longform_render are "
                    "intentionally not offered this turn because no hard production commit was detected. "
                    "Do NOT claim tools are offline, unavailable, or will 'come back online'. "
                    "Present/refine the concept plan and ask for a hard commit: "
                    "'yes make it', 'render that plan', 'go ahead and render', or 'make the first scene'."
                ),
            })
        elif production_tool_offered:
            messages.append({
                "role": "system",
                "content": (
                    "[Studio production tools ONLINE] start_shortform_generate is offered and callable now. "
                    "Do NOT claim a tool availability issue or build a 'manual blueprint until tools return'. "
                    "If the user committed to render, call start_shortform_generate with the locked title, "
                    "duration, scene_count, render_style, image_model_id, and visual_brief."
                ),
            })

        # Tyler loop: full tools SOP only when this turn plans/calls tools (task-maker).
        # Chat-only turns keep the thin card from system_prompt — no 53-tool dictionary.
        try:
            from studio_agent.tools_sop import load_tools_sop_text, should_inject_tools_sop

            _tools_available = bool(tools)
            _inject_sop = _tools_available and should_inject_tools_sop(
                user_text=user_text,
                intent_text=intent_text or user_text,
                turn_plan=turn_plan,
                conversational_turn=bool(conversational_turn),
                ideation_turn=bool(ideation_turn),
                reply_to=str((reply_to or {}).get("job_id") or reply_to or ""),
                live_demand_preflight=bool(live_demand_preflight_required),
                public_search_preflight=bool(public_search_preflight_required),
                channel_data_preflight=bool(channel_data_preflight_required),
                competitor_or_reference=bool(
                    competitor_channel_required
                    or channel_url_reference_required
                    or turn_plan.reference_analysis
                ),
                production_now=bool(
                    store.is_explicit_production_request(intent_text or user_text)
                    or _wants_production_execution(intent_text or user_text)
                ),
            )
            if _inject_sop:
                _sop_body = load_tools_sop_text()
                # Avoid duplicate inject if a prior round already added it this turn.
                _already = any(
                    str(m.get("role") or "") == "system"
                    and "TASK-MAKER DICTIONARY" in str(m.get("content") or "")
                    for m in messages
                )
                if not _already and _sop_body:
                    messages.append({"role": "system", "content": _sop_body})
                    store.update_session(sid, messages=messages)
                    await _fire_event(
                        emit,
                        "status",
                        message="Task-maker: tools dictionary loaded for this planning turn…",
                    )
        except Exception:
            pass

        await _fire_event(emit, "status", message="Thinking…")

    for round_idx in range(MAX_TOOL_ROUNDS):
        if skip_model_loop:
            break
        compacted = store.compact_session_if_needed(sid)
        if compacted:
            session = compacted
        # Never send orphaned/incomplete tool chains to the provider — that freezes
        # Kimi/OpenRouter/Claude mid-turn and makes tool calling intermittent.
        messages = store.align_tool_message_boundary(messages)
        model_messages = store.trim_messages_for_model(messages, session=session)
        await _fire_event(emit, "model_round", round=round_idx + 1)
        # Preflight research must not suppress force_tool_call for a hard production
        # commit that still needs start_shortform_generate.
        production_force = bool(
            production_tool_offered
            and production_tools_allowed
            and not ideation_turn
            and "start_shortform_generate" not in {str(f.name or "") for f in tool_fires}
            and "start_longform_render" not in {str(f.name or "") for f in tool_fires}
            and (
                _allows_brand_new_production_tool(intent_text or user_text)
                or _wants_production_execution(intent_text or user_text)
            )
        )
        must_execute_tool = production_force or (
            not preflight_tool_fires
            and not tool_fires
            and not ideation_turn
            and _requires_tool_execution(intent_text or user_text)
        )
        round_tools = tools
        if production_force:
            longform_commit = bool(
                str(content_format or "").lower().startswith("long")
                or re.search(r"\blong[ -]?form\b|\bfull(?:-length)?\s+video\b", str(intent_text or user_text), re.I)
            )
            required_name = "start_longform_render" if longform_commit else "start_shortform_generate"
            if required_name not in offered_tool_names:
                alternate = "start_shortform_generate" if required_name == "start_longform_render" else "start_longform_render"
                required_name = alternate if alternate in offered_tool_names else required_name
            # A forced production turn offers exactly the required mutation.
            # The model cannot opportunistically call research or a different
            # production tool merely because tool_choice is required.
            round_tools = [
                tool for tool in tools
                if str(tool.get("function", {}).get("name") or "") == required_name
            ]
        round_offered_tool_names = _offered_model_tool_names(round_tools)
        training_capture.capture_event(
            str(user_id),
            "model_request",
            {
                "round": round_idx + 1,
                "model": model,
                "messages": model_messages,
                "tools": round_tools,
                "reasoning_depth": reasoning_depth,
                "web_search": web_search,
                "force_tool_call": must_execute_tool,
            },
            session_id=sid,
            turn_id=turn_id,
        )
        preserve_tool_names = None
        if turn_plan.has_execution:
            preserve_tool_names = frozenset({
                "analyze_reference_video",
                "poll_render_job",
                "get_public_search_trends",
                "search_youtube_public",
                "recommend_video_topics",
                "get_channel_analytics",
            })
        resp = await openrouter.chat_completion(
            messages=model_messages,
            tools=round_tools,
            model=model,
            reasoning_depth=reasoning_depth,
            web_search=web_search,
            force_tool_call=must_execute_tool,
            preserve_tool_names=preserve_tool_names,
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

        # Normalize again in case a provider path bypassed message_from_response.
        msg = openrouter.normalize_assistant_message(msg if isinstance(msg, dict) else {})
        tool_calls = list(msg.get("tool_calls") or [])
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
                name = str(fn.get("name") or "").strip()
                try:
                    raw_args = fn.get("arguments") or "{}"
                    args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                except json.JSONDecodeError:
                    args = {}
                if not isinstance(args, dict):
                    args = {}
                unoffered_result = _unoffered_model_tool_result(name, round_offered_tool_names)
                if unoffered_result is not None:
                    tool_fires.append(ToolFire(name, dict(args), unoffered_result))
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.get("id"),
                        "content": unoffered_result,
                    })
                    await _fire_event(
                        emit,
                        "tool_end",
                        tool=name,
                        status="error",
                        error="unoffered_tool",
                    )
                    continue
                if name == "start_shortform_generate":
                    args = _prepare_shortform_production_args(args, session, messages=messages)
                    turn_session = dict(session)
                    turn_session["messages"] = messages
                    conflict = _shortform_action_conflicts_with_latest_user(args, turn_session)
                    if conflict:
                        assistant_text = conflict
                        result = json.dumps({
                            "status": "blocked_stale_pending_action",
                            "message": conflict,
                        })
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
                            error="blocked_stale_pending_action",
                        )
                        continue
                args = _channel_guard_tool_args(name, args, active_registry, active_channel_id)

                if production_diagnostic_turn and name in JOB_START_TOOLS:
                    result = json.dumps({
                        "status": "blocked_diagnostic_turn",
                        "message": (
                            "The latest user message is asking about a wrong/stale production. "
                            "No render was prepared or started."
                        ),
                    })
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
                        error="blocked_diagnostic_turn",
                    )
                    continue

                if (
                    reference_correction_turn
                    and name in {"analyze_reference_video", "analyze_competitor_video"}
                    and not store.is_uploaded_video_analysis_request(intent_text or user_text)
                ):
                    result = json.dumps({
                        "status": "blocked_reference_correction_turn",
                        "message": (
                            "The user corrected a prior description of this upload. Accept their correction "
                            "instead of launching another reference analysis job. Re-run analyze_reference_video "
                            "only if they explicitly ask for pacing/structure analysis."
                        ),
                    })
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
                        error="blocked_reference_correction_turn",
                    )
                    continue

                if (
                    (ideation_turn or conversational_turn)
                    and not store.should_auto_run_tools(intent_text or user_text)
                    and name in (BRAND_NEW_PRODUCTION_TOOLS | CONVERSATIONAL_DATA_TOOLS)
                ):
                    result = json.dumps({
                        "status": "blocked_conversational_turn",
                        "message": (
                            "This turn is conversational planning only. Discuss strategy and propose next steps. "
                            "Wait for explicit user go-ahead (e.g. 'yes run that', 'go ahead', 'do it') before "
                            "calling research, analytics, reference analysis, or production tools."
                        ),
                    })
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
                        error="blocked_ideation_turn",
                    )
                    continue

                # DEFAULT DENY: brand-new production tools only after hard commit.
                # Do not rely on research heuristics alone — bare "let's make [title]"
                # must never open Approve.
                if name in BRAND_NEW_PRODUCTION_TOOLS and not _allows_brand_new_production_tool(
                    intent_text or user_text
                ):
                    result = json.dumps({
                        "status": "blocked_no_hard_commit",
                        "message": (
                            "Production tools are blocked until the user hard-commits. "
                            "Respond with strategy and/or a concept plan only. "
                            "Open Approve after a hard commit: 'yes make it', 'render that plan', "
                            "'go ahead and render', 'make the first scene', or 'render it now'."
                        ),
                    })
                    tool_fires.append(ToolFire(str(name), dict(args or {}), result))
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.get("id"),
                        "content": result,
                    })
                    await _fire_tool_end(
                        emit,
                        name,
                        status="skipped",
                        args=args if isinstance(args, dict) else {},
                        result=result,
                        error="blocked_no_hard_commit",
                    )
                    continue

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

                await _fire_tool_start(
                    emit,
                    name,
                    args=args if isinstance(args, dict) else {},
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
                    # Absolute last gate: never create production Approve without hard commit.
                    if name in BRAND_NEW_PRODUCTION_TOOLS and not _allows_brand_new_production_tool(
                        intent_text or user_text
                    ):
                        result = json.dumps({
                            "status": "blocked_no_hard_commit",
                            "message": (
                                "Refusing to open Approve — user has not hard-committed to production. "
                                "Propose a concept plan and wait for 'yes make it' / 'render that plan'."
                            ),
                        })
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
                            error="blocked_no_hard_commit",
                        )
                        continue
                    if name == "start_shortform_generate":
                        turn_session = dict(session)
                        turn_session["messages"] = messages
                        conflict = _shortform_action_conflicts_with_latest_user(args, turn_session)
                        if conflict:
                            result = json.dumps({
                                "status": "blocked_stale_pending_action",
                                "message": conflict,
                            })
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
                                error="blocked_stale_pending_action",
                            )
                            continue
                    action_id = f"act_{uuid.uuid4().hex[:12]}"
                    budget_summary = ""
                    budget_payload = None
                    if budget_estimate is not None:
                        budget_payload = budget_estimate.as_dict()
                        budget_summary = (
                            f" | est ${budget_payload['estimated_usd']:.2f} "
                            f"<= cap ${budget_payload['max_budget_usd']:.2f}"
                        )
                    pending = _upsert_production_pending(pending, {
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
                    result = _execute_offered_model_tool(
                        name,
                        args,
                        offered_tool_names=round_offered_tool_names,
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
                if name == "generate_longform_thumbnails":
                    # A thumbnail request is an isolated packaging proof, not
                    # production.  Do not put it in active_jobs or
                    # last_production: either makes Plan Mode look as if the
                    # user authorized a full long-form render.
                    #
                    # Emit the visual deliverable immediately instead of
                    # waiting for a poll, so the chat shows the candidates in
                    # this turn rather than raw preview links.
                    try:
                        thumbnail_payload = json.loads(result or "{}")
                        thumbnail_job_id = str(thumbnail_payload.get("job_id") or "").strip()
                        if thumbnail_job_id:
                            await _fire_event(
                                emit,
                                "job_snapshot",
                                snapshot=get_job_snapshot(thumbnail_job_id, "longform"),
                            )
                    except (TypeError, ValueError, json.JSONDecodeError):
                        pass

                err_preview = ""
                try:
                    parsed = json.loads(result or "{}")
                    if isinstance(parsed, dict) and parsed.get("error"):
                        err_preview = str(parsed.get("error"))[:120]
                except json.JSONDecodeError:
                    pass
                await _fire_tool_end(
                    emit,
                    name,
                    status="error" if err_preview else "ok",
                    args=args if isinstance(args, dict) else {},
                    result=result,
                    error=err_preview or None,
                )

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.get("id"),
                    "content": result,
                })
                store.update_session(sid, messages=messages, active_jobs=active_jobs)

            if blocked and pending:
                prepared_pending = _prepare_pending_actions(pending, session, messages=messages)
                pending = prepared_pending
                await _fire_event(emit, "pending_actions", actions=prepared_pending)
                assistant_text = sanitize_assistant_text(
                    content
                    or "I prepared the next steps but need your approval before running commands that spend credits or write files."
                )
                store.update_session(sid, messages=messages, pending_actions=prepared_pending, active_jobs=active_jobs)
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
            # Deterministic backend recovery — do not depend on the model emitting tools.
            if (
                public_search_preflight_required
                or live_demand_preflight_required
                or store.is_public_youtube_research_request(intent_text or user_text)
                or store.is_competitor_channel_reference_request(intent_text or user_text)
            ):
                fallback_fires = await _run_public_youtube_research_preflight(
                    emit=emit,
                    user_id=str(user_id),
                    content_format=content_format,
                    session_id=sid,
                    messages=messages,
                    user_text=intent_text or user_text,
                    session=session,
                    active_registry=active_registry,
                    active_channel_id=active_channel_id,
                )
                if fallback_fires:
                    tool_fires.extend(fallback_fires)
                    if _has_successful_public_demand_tool(tool_fires):
                        assistant_text = sanitize_assistant_text(
                            _synthesize_turn_from_evidence(
                                tool_fires,
                                user_text=user_text,
                                turn_plan=turn_plan,
                                session=session,
                                active_registry=active_registry,
                                active_channel_id=active_channel_id,
                                has_reference_upload=bool(reference_context),
                                messages=messages,
                            )
                        )
                        break
            # Hard production commit + tools offered, but model still returned text only.
            if (
                production_force
                and production_tool_offered
                and not any(
                    str(f.name or "") in {"start_shortform_generate", "start_longform_render"}
                    for f in tool_fires
                )
            ):
                recovered = _recover_requested_production(session, intent_text or user_text)
                if not recovered:
                    recovered = _build_requested_topic_production(
                        session,
                        intent_text or user_text,
                        content_format=content_format,
                        active_registry=active_registry,
                        active_channel_id=active_channel_id,
                        messages=messages,
                    )
                if recovered:
                    name, args = recovered
                    if name == "start_shortform_generate":
                        args = _prepare_shortform_production_args(args, session, messages=messages)
                    args = _channel_guard_tool_args(name, args, active_registry, active_channel_id)
                    await _fire_tool_start(
                        emit,
                        name,
                        args=args if isinstance(args, dict) else {},
                        round=round_idx + 1,
                        awaiting_approval=approval_mode == "confirm" and requires_approval(name),
                    )
                    if approval_mode == "confirm" and requires_approval(name):
                        action_id = f"act_{uuid.uuid4().hex[:12]}"
                        pending = _upsert_production_pending(pending, {
                            "id": action_id,
                            "tool": name,
                            "arguments": args,
                            "summary": f"{name}({json.dumps(args)[:200]})",
                        })
                        prepared_pending = _prepare_pending_actions(pending, session, messages=messages)
                        pending = prepared_pending
                        assistant_text = sanitize_assistant_text(
                            content
                            or "I prepared production from your locked concept. Approve to start the render."
                        )
                        messages.append({"role": "assistant", "content": assistant_text})
                        store.update_session(
                            sid,
                            messages=messages,
                            pending_actions=prepared_pending,
                            last_production={
                                "tool": name,
                                "arguments": args,
                                "updated_at": time.time(),
                            },
                        )
                        await _fire_event(emit, "pending_actions", actions=prepared_pending)
                        await _fire_event(emit, "tool_end", tool=name, status="awaiting_approval")
                        break
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
                    if name in JOB_START_TOOLS:
                        active_jobs = merge_active_jobs(active_jobs, extract_jobs_from_tool(name, result))
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
                    await _fire_tool_end(emit, name, status="ok", args=args, result=result)
                    assistant_text = sanitize_assistant_text(
                        "Production tools are online — I started the render from your locked concept."
                    )
                    break
            if not str(assistant_text or "").strip():
                assistant_text = (
                    "Studio could not execute the required tool after repeated forced attempts. "
                    "No tool result was created, so I will not claim the work ran. Retry once or choose a stronger runner model."
                )
            break
        # False "tools offline" narration while production tools were offered this turn.
        if (
            production_tool_offered
            and _assistant_claims_tools_unavailable(content or "")
            and not any(
                str(f.name or "") in {"start_shortform_generate", "start_longform_render"}
                for f in tool_fires
            )
            and round_idx + 1 < MAX_TOOL_ROUNDS
        ):
            messages.append({
                "role": "system",
                "content": (
                    "[Studio execution contract violation: production tools are ONLINE and offered. "
                    "You claimed a tool availability issue without calling start_shortform_generate. "
                    "Call start_shortform_generate now with the locked title/duration/scenes, or if the "
                    "user has not hard-committed, ask for 'yes make it' / 'render that plan' without "
                    "claiming tools are offline.]"
                ),
            })
            await _fire_event(
                emit,
                "status",
                message="Production tools are online — retrying the required tool call...",
            )
            continue
        assistant_text = sanitize_assistant_text(content or "")
        break

    active_label = _ideation_active_label(
        session,
        user_text,
        active_registry=active_registry,
        active_channel_id=active_channel_id,
    )
    has_reference_upload = bool(reference_context)
    if assistant_text and _assistant_denies_public_research_tool(assistant_text):
        assistant_text = _recover_public_search_denial(
            assistant_text,
            user_text=user_text,
            intent_text=intent_text or user_text,
            messages=messages,
            tool_fires=tool_fires,
            session=session,
            active_label=active_label,
            active_registry=active_registry,
            active_channel_id=active_channel_id,
            user_id=str(user_id),
            content_format=content_format,
            session_id=sid,
            ideation_turn=ideation_turn,
            include_channel=bool(turn_plan.channel_analytics),
        )
    elif (
        (
            turn_plan.has_execution
            or public_search_preflight_required
            or store.is_public_youtube_research_request(user_text)
            or ideation_turn
        )
        and (
            not str(assistant_text or "").strip()
            or _assistant_needs_turn_synthesis(assistant_text, turn_plan=turn_plan)
            or (
                getattr(turn_plan, "reference_analysis", False)
                and has_reference_upload
                and _assistant_denies_upload_analysis_capability(assistant_text)
            )
        )
        and (preflight_tool_fires or tool_fires or has_reference_upload)
    ):
        if _public_demand_needs_retry(tool_fires):
            _append_late_public_search(
                tool_fires,
                user_text=user_text,
                intent_text=intent_text or user_text,
                messages=messages,
                active_label=active_label,
                active_registry=active_registry,
                active_channel_id=active_channel_id,
                user_id=str(user_id),
                content_format=content_format,
                session_id=sid,
            )
        assistant_text = _synthesize_turn_from_evidence(
            tool_fires,
            user_text=user_text,
            turn_plan=turn_plan,
            session=session,
            active_registry=active_registry,
            active_channel_id=active_channel_id,
            has_reference_upload=has_reference_upload,
            messages=messages,
        )
    elif not assistant_text and not pending:
        assistant_text = (
            "I hit the tool-round limit before I could produce a clean final answer. "
            "I saved the work completed so far in this chat; press Resume or send a shorter follow-up and I will continue from the saved tool results instead of starting over."
        )

    if assistant_text:
        assistant_text = sanitize_assistant_text(assistant_text)
        if _is_render_cost_question(intent_text or user_text) or _latest_cost_estimate_payload(
            list(preflight_tool_fires) + list(tool_fires)
        ):
            assistant_text = _recover_stale_cost_quote(
                assistant_text,
                tool_fires=tool_fires,
                preflight_tool_fires=preflight_tool_fires,
                user_text=intent_text or user_text,
                force=_is_render_cost_question(intent_text or user_text),
            )
        ref_analysis_payload = _latest_complete_reference_analysis(tool_fires=tool_fires, messages=messages)
        if (
            reference_analysis_required
            and ref_analysis_payload
            and (
                _assistant_stalled_on_reference_analysis(assistant_text)
                or (
                    (active_registry or active_channel_id)
                    and _assistant_stalled_on_channel_data(assistant_text)
                    and not store.is_connected_channel_performance_request(intent_text or user_text)
                )
            )
        ):
            assistant_text = _format_reference_analysis_findings(ref_analysis_payload)
        elif ideation_turn and _has_public_demand_tool(tool_fires) and (
            _assistant_stalled_on_channel_data(assistant_text)
            or _assistant_promised_research_narration(assistant_text)
            or _assistant_denies_public_research_tool(assistant_text)
        ):
            assistant_text = _grounded_ideation_research_from_tools(
                tool_fires,
                active_label=active_label,
                user_text=user_text,
                has_reference_upload=has_reference_upload,
            )
        elif (
            (active_registry or active_channel_id)
            and _has_channel_analytics_tool(tool_fires)
            and (
                _assistant_stalled_on_channel_data(assistant_text)
                or (
                    bool(_latest_shortform_comparison(tool_fires).get("comparison_ready"))
                    and _assistant_asks_user_for_known_analytics(assistant_text)
                )
            )
        ):
            compare = _latest_shortform_comparison(tool_fires)
            if compare.get("comparison_ready"):
                assistant_text = _grounded_shortform_comparison_from_tools(
                    tool_fires,
                    active_label=active_label,
                    user_text=user_text,
                )
            elif public_search_preflight_required and _has_public_demand_tool(tool_fires):
                assistant_text = _grounded_research_summary_from_tools(
                    tool_fires,
                    active_label=active_label,
                    user_text=user_text,
                )
            elif _wants_short_plan(user_text):
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
        await _fire_verification_step(
            emit,
            "final_audit",
            "running",
            label="Audit final answer before replying",
            detail="Checking the draft against executed tool evidence and blocking unsupported claims.",
        )
        audit = audit_turn(
            assistant_text=assistant_text,
            user_text=user_text,
            tool_fires=tool_fires,
            conversational_mode=conversational_turn,
        )
        if audit.has_blockers:
            try:
                from studio_agent import continuous_evaluation

                continuous_evaluation.record_evidence(
                    session=session,
                    event_type="final_answer_audit",
                    outcome="blocked",
                    evidence={"failure": "; ".join(audit.blocked_claims)[:280]},
                )
            except Exception:
                pass
        if (
            audit.has_issues
            and ideation_turn
            and _has_public_demand_tool(tool_fires)
            and (
                _promised_execution_blocked(audit)
                or _assistant_promised_research_narration(assistant_text)
            )
        ):
            assistant_text = sanitize_assistant_text(
                _grounded_ideation_research_from_tools(
                    tool_fires,
                    active_label=active_label,
                    user_text=user_text,
                    has_reference_upload=has_reference_upload,
                )
            )
            audit = audit_turn(
                assistant_text=assistant_text,
                user_text=user_text,
                tool_fires=tool_fires,
                conversational_mode=conversational_turn,
            )
        if (
            audit.has_issues
            and _promised_execution_blocked(audit)
            and not production_diagnostic_turn
            and not ideation_turn
            and not _is_job_status_followup(user_text)
        ):
            recovered = _recover_requested_production(session, user_text)
            conflict = _production_conflict_with_latest_user(recovered, {"session_id": sid, "messages": messages})
            if conflict:
                messages.append({
                    "role": "system",
                    "content": (
                        "[Studio Agent rejected recovered stale production.]\n"
                        f"{conflict}\n"
                        "Build a fresh production from the latest user request instead."
                    ),
                })
                session = store.update_session(
                    sid,
                    messages=messages,
                    pending_actions=[],
                    last_production=None,
                )
                recovered = None
            if not recovered:
                recovered = _build_requested_topic_production(
                    session,
                    user_text,
                    content_format=content_format,
                    active_registry=active_registry,
                    active_channel_id=active_channel_id,
                    messages=messages,
                )
            if recovered:
                name, args = recovered
                if name == "start_shortform_generate":
                    args = _prepare_shortform_production_args(args, session, messages=messages)
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
                        pending = _upsert_production_pending(pending, {
                            "id": action_id,
                            "tool": name,
                            "arguments": args,
                            "summary": f"{name}({json.dumps(args)[:200]}){budget_summary}",
                            "budget": budget_payload,
                        })
                        prepared_pending = _prepare_pending_actions(pending, session, messages=messages)
                        pending = prepared_pending
                        from studio_agent.conversation import conversational_production_prepared

                        assistant_text = conversational_production_prepared(auto_start=False)
                        store.update_session(
                            sid,
                            pending_actions=prepared_pending,
                            last_production={
                                "tool": name,
                                "arguments": args,
                                "updated_at": time.time(),
                            },
                        )
                        await _fire_event(emit, "pending_actions", actions=prepared_pending)
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
                    conversational_mode=conversational_turn,
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
                    {
                        "registry_key": active_registry,
                        "channel_id": active_channel_id,
                        "days": 30,
                        "fresh": _needs_fresh_public_search(user_text),
                    },
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
                conversational_mode=conversational_turn,
            )
        audit_blocked = bool(audit.has_issues)
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
            ref_analysis_payload = _latest_complete_reference_analysis(tool_fires=tool_fires, messages=messages)
            guarded = guard_text(assistant_text, audit)
            needs_synthesis = (
                _guard_needs_evidence_synthesis(guarded)
                or _assistant_stalled_on_reference_analysis(assistant_text)
                or _assistant_promised_research_narration(assistant_text)
                or _assistant_denies_public_research_tool(assistant_text)
                or "should not narrate work" in guarded.lower()
            )
            if needs_synthesis:
                research_needed = bool(
                    turn_plan.public_youtube_demand
                    or public_search_preflight_required
                    or live_demand_preflight_required
                    or _is_public_search_followup(intent_text or user_text, messages)
                    or _needs_live_demand_preflight(
                        intent_text or user_text,
                        session,
                        production_intent=production_intent,
                    )
                )
                # Keep working: if research is missing OR previous public tools only hard-failed, re-run.
                if research_needed and _public_demand_needs_retry(tool_fires):
                    await _fire_event(
                        emit,
                        "status",
                        message="Continuing: running Live Demand research tools until evidence is ready...",
                    )
                    late_fires = await _run_public_youtube_research_preflight(
                        emit=emit,
                        user_id=str(user_id),
                        content_format=content_format,
                        session_id=sid,
                        messages=messages,
                        user_text=intent_text or user_text,
                        session=session,
                        active_registry=active_registry,
                        active_channel_id=active_channel_id,
                    )
                    tool_fires.extend(late_fires)
                    preflight_tool_fires.extend(late_fires)
                    session = store.get_session(sid) or session
                if (
                    research_needed
                    and _has_successful_public_demand_tool(tool_fires)
                ):
                    grounded_status = _synthesize_turn_from_evidence(
                        tool_fires,
                        user_text=user_text,
                        turn_plan=turn_plan,
                        session=session,
                        active_registry=active_registry,
                        active_channel_id=active_channel_id,
                        has_reference_upload=has_reference_upload,
                        messages=messages,
                    )
                elif ideation_turn and _has_successful_public_demand_tool(tool_fires):
                    grounded_status = _grounded_ideation_research_from_tools(
                        tool_fires,
                        active_label=active_label,
                        user_text=user_text,
                        has_reference_upload=has_reference_upload,
                    )
                elif (public_search_preflight_required or research_needed) and _has_successful_public_demand_tool(tool_fires):
                    from studio_agent.conversation import (
                        deterministic_conversational_research_reply,
                        strip_robot_research_artifacts,
                    )

                    _ev = _grounded_research_summary_from_tools(
                        tool_fires,
                        active_label=active_label,
                        user_text=user_text,
                        include_channel=False,
                        search_query=_latest_public_search_query(tool_fires),
                        reference_payload=ref_analysis_payload,
                    )
                    grounded_status = strip_robot_research_artifacts(
                        deterministic_conversational_research_reply(
                            user_text=user_text,
                            evidence=_ev,
                            reference_findings=(
                                _format_reference_analysis_findings(ref_analysis_payload)
                                if ref_analysis_payload
                                else ""
                            ),
                            niche_hint=active_label or _latest_public_search_query(tool_fires) or "",
                        )
                    )
                elif ref_analysis_payload:
                    grounded_status = _format_reference_analysis_findings(ref_analysis_payload)
                elif (active_registry or active_channel_id) and _has_channel_analytics_tool(tool_fires):
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
            # Never ship internal guard/meta text as the final user answer when research was required.
            if _is_meta_guard_reply(grounded_status or guarded) and _has_successful_public_demand_tool(tool_fires):
                grounded_status = _synthesize_turn_from_evidence(
                    tool_fires,
                    user_text=user_text,
                    turn_plan=turn_plan,
                    session=session,
                    active_registry=active_registry,
                    active_channel_id=active_channel_id,
                    has_reference_upload=has_reference_upload,
                    messages=messages,
                )
            # Last-chance recovery: do not leave the creator stuck on a meta guard.
            if _is_meta_guard_reply(grounded_status or guarded) and not grounded_status:
                if research_needed and _public_demand_needs_retry(tool_fires):
                    await _fire_event(
                        emit,
                        "status",
                        message="Recovering: re-running research tools after a guard block...",
                    )
                    recover_fires = await _run_public_youtube_research_preflight(
                        emit=emit,
                        user_id=str(user_id),
                        content_format=content_format,
                        session_id=sid,
                        messages=messages,
                        user_text=intent_text or user_text,
                        session=session,
                        active_registry=active_registry,
                        active_channel_id=active_channel_id,
                    )
                    tool_fires.extend(recover_fires)
                    preflight_tool_fires.extend(recover_fires)
                    session = store.get_session(sid) or session
                    if _has_successful_public_demand_tool(tool_fires):
                        grounded_status = _synthesize_turn_from_evidence(
                            tool_fires,
                            user_text=user_text,
                            turn_plan=turn_plan,
                            session=session,
                            active_registry=active_registry,
                            active_channel_id=active_channel_id,
                            has_reference_upload=has_reference_upload,
                            messages=messages,
                        )
                if not grounded_status and (active_registry or active_channel_id) and _has_channel_analytics_tool(tool_fires):
                    grounded_status = _grounded_channel_status_from_tools(
                        tool_fires,
                        active_label=active_label,
                    )
                if not grounded_status:
                    # Surface the real tool error instead of a dead-end "retry research" loop.
                    failed = [
                        f"{fire.name}: {str(_tool_fire_payload(fire).get('error') or fire.result or 'failed')[:160]}"
                        for fire in tool_fires
                        if _tool_fire_failed(fire)
                    ]
                    if failed:
                        grounded_status = (
                            "Research tools could not complete this turn:\n- "
                            + "\n- ".join(failed[:3])
                            + "\n\nI still have this chat context — send your next instruction and I will run the tools again."
                        )
                    else:
                        grounded_status = (
                            "I could not finish a clean tool-backed research answer in this turn. "
                            "Your chat and any completed tool results are saved — send a short follow-up "
                            "(for example: 'pull live demand for dark psychology shorts') and I will run the tools again."
                        )
            assistant_text = sanitize_assistant_text(grounded_status or guarded)
        # Hard override: when analytics comparison_ready is true, never ship
        # "memory only" / interview-for-metrics answers.
        if (
            _has_channel_analytics_tool(tool_fires)
            and bool(_latest_shortform_comparison(tool_fires).get("comparison_ready"))
            and (
                _assistant_stalled_on_channel_data(assistant_text)
                or _assistant_asks_user_for_known_analytics(assistant_text)
            )
        ):
            assistant_text = sanitize_assistant_text(
                _grounded_shortform_comparison_from_tools(
                    tool_fires,
                    active_label=active_label,
                    user_text=user_text,
                )
            )
        # Never ship "tools offline" when production tools were offered this turn.
        if (
            production_tool_offered
            and _assistant_claims_tools_unavailable(assistant_text)
            and not any(
                str(f.name or "") in {"start_shortform_generate", "start_longform_render"}
                for f in tool_fires
            )
        ):
            if production_tools_allowed:
                recovered = _recover_requested_production(session, intent_text or user_text)
                if not recovered:
                    recovered = _build_requested_topic_production(
                        session,
                        intent_text or user_text,
                        content_format=content_format,
                        active_registry=active_registry,
                        active_channel_id=active_channel_id,
                        messages=messages,
                    )
                if recovered:
                    name, args = recovered
                    if name == "start_shortform_generate":
                        args = _prepare_shortform_production_args(args, session, messages=messages)
                    args = _channel_guard_tool_args(name, args, active_registry, active_channel_id)
                    await _fire_tool_start(
                        emit,
                        name,
                        args=args if isinstance(args, dict) else {},
                        round=0,
                        awaiting_approval=approval_mode == "confirm" and requires_approval(name),
                    )
                    if approval_mode == "confirm" and requires_approval(name):
                        action_id = f"act_{uuid.uuid4().hex[:12]}"
                        pending = _upsert_production_pending(pending, {
                            "id": action_id,
                            "tool": name,
                            "arguments": args,
                            "summary": f"{name}({json.dumps(args)[:200]})",
                        })
                        prepared_pending = _prepare_pending_actions(pending, session, messages=messages)
                        pending = prepared_pending
                        assistant_text = sanitize_assistant_text(
                            "Production tools are online. I prepared the render from your locked concept — "
                            "approve to start stills generation."
                        )
                        store.update_session(
                            sid,
                            pending_actions=prepared_pending,
                            last_production={
                                "tool": name,
                                "arguments": args,
                                "updated_at": time.time(),
                            },
                        )
                        await _fire_event(emit, "pending_actions", actions=prepared_pending)
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
                        if name in JOB_START_TOOLS:
                            active_jobs = merge_active_jobs(
                                active_jobs,
                                extract_jobs_from_tool(name, result),
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
                        await _fire_tool_end(emit, name, status="ok", args=args, result=result)
                        assistant_text = sanitize_assistant_text(
                            "Production tools are online — I started the render from your locked concept."
                        )
                else:
                    assistant_text = sanitize_assistant_text(
                        "Production tools are online. Your concept is ready — say **yes make it** or "
                        "**render that plan** and I will call start_shortform_generate immediately "
                        "(Confirm mode will show Approve before spend)."
                    )
            else:
                assistant_text = sanitize_assistant_text(
                    "Production tools are online, but this turn is still in plan mode until you hard-commit. "
                    "Say **yes make it**, **render that plan**, or **make the first scene** and I will open "
                    "the production tool path — I will not claim tools are offline."
                )
        if assistant_text and _assistant_denies_public_research_tool(assistant_text):
            assistant_text = sanitize_assistant_text(
                _recover_public_search_denial(
                    assistant_text,
                    user_text=user_text,
                    intent_text=intent_text or user_text,
                    messages=messages,
                    tool_fires=tool_fires,
                    session=session,
                    active_label=active_label,
                    active_registry=active_registry,
                    active_channel_id=active_channel_id,
                    user_id=str(user_id),
                    content_format=content_format,
                    session_id=sid,
                    ideation_turn=ideation_turn,
                    include_channel=bool(turn_plan.channel_analytics),
                )
            )
        if (
            public_search_preflight_required
            and _has_public_demand_tool(tool_fires)
            and _is_channel_status_only_answer(assistant_text)
        ):
            active_label = (
                str(session.get("channel_title") or "").strip()
                or str(active_registry or active_channel_id or "").replace("_", " ")
                or "selected"
            )
            assistant_text = sanitize_assistant_text(_grounded_research_summary_from_tools(
                tool_fires,
                active_label=active_label,
                user_text=user_text,
            ))
        # Final Live Demand lock: only override when the model invents or denies capability.
        # Grok-class natural replies that cite verified evidence must ship as-is.
        try:
            from studio_agent.live_demand import has_demand_signal, is_research_execution_request

            _demand_lock = bool(
                _has_public_demand_tool(tool_fires)
                and not store.is_explicit_production_request(intent_text or user_text)
                and not _wants_production_execution(intent_text or user_text)
                and (
                    has_demand_signal(intent_text or user_text)
                    or is_research_execution_request(intent_text or user_text)
                    or public_search_preflight_required
                    or live_demand_preflight_required
                )
            )
        except Exception:
            _demand_lock = bool(
                _has_public_demand_tool(tool_fires)
                and (public_search_preflight_required or live_demand_preflight_required)
            )
        if _demand_lock and assistant_text:
            low_ans = str(assistant_text or "").lower()
            hallucinated_or_denied = any(
                marker in low_ans
                for marker in (
                    "fidget",
                    "popit",
                    "i can't access youtube",
                    "i cannot access youtube",
                    "don't have a direct",
                    "do not have a direct",
                    "as an ai",
                    "i cannot browse",
                    "i can't browse",
                    "no way to get",
                    "million views",  # often invented when not in tool rows
                )
            )
            has_verified_section = (
                "verified evidence" in low_ans
                or "i verified public youtube demand" in low_ans
                or "public youtube demand evidence" in low_ans
            )
            # Only hard-replace toxic/denial answers. Keep conversational synthesis.
            if hallucinated_or_denied and not has_verified_section:
                from studio_agent.conversation import synthesize_conversational_research_reply
                from studio_agent.live_demand import extract_niche_hint

                active_label = (
                    str(session.get("channel_title") or "").strip()
                    or str(active_registry or active_channel_id or "").replace("_", " ")
                    or "YouTube"
                )
                evidence = _grounded_research_summary_from_tools(
                    tool_fires,
                    active_label=active_label,
                    user_text=intent_text or user_text,
                    include_channel=bool(active_registry or active_channel_id),
                    search_query=_latest_public_search_query(tool_fires),
                )
                assistant_text = sanitize_assistant_text(
                    await synthesize_conversational_research_reply(
                        user_text=intent_text or user_text,
                        evidence=evidence,
                        niche_hint=extract_niche_hint(intent_text or user_text, session) or active_label,
                        model=str(model or session.get("model") or ""),
                    )
                )
        # Final cost lock: audit/synthesis can re-introduce freeform pricing after earlier recovery.
        if _is_render_cost_question(intent_text or user_text) or _latest_cost_estimate_payload(
            list(preflight_tool_fires) + list(tool_fires)
        ):
            assistant_text = _recover_stale_cost_quote(
                assistant_text,
                tool_fires=tool_fires,
                preflight_tool_fires=preflight_tool_fires,
                user_text=intent_text or user_text,
                force=_is_render_cost_question(intent_text or user_text),
            )
        await _fire_verification_step(
            emit,
            "final_audit",
            "done",
            label="Audit final answer before replying",
            detail=(
                "Final answer passed evidence audit."
                if not audit_blocked
                else "Final answer had unsupported claims; Studio Agent corrected or replaced it before sending."
            ),
        )
        messages.append({"role": "assistant", "content": assistant_text})

    store.update_session(sid, messages=messages)
    # Absolute gate: research/strategy/soft turns never leave a production Approve card.
    if _blocks_brand_new_production(intent_text or user_text):
        pending = _strip_production_pending(pending)
        store.update_session(sid, pending_actions=pending, last_production=None)
        await _fire_event(emit, "pending_actions", actions=pending)
    elif pending:
        pending = _prepare_pending_actions(pending, session, messages=messages)
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
        if provider_policy.normalize_provider(model_provider) != "anthropic":
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

    final_concept = session.get("pending_concept")
    if not isinstance(final_concept, dict):
        final_concept = None
    expose_concept = bool(
        final_concept
        and (not plan_only or _wants_final_concept_card(intent_text or user_text))
    )
    if expose_concept and plan_only:
        final_concept = {**final_concept, "status": "ready_for_review"}
        store.update_session(sid, pending_concept=final_concept)
    visible_concept = final_concept if expose_concept else None
    cost_projection = None
    try:
        from studio_agent.cost_optimizer import optimize_from_session

        projection_session = {**session, "pending_concept": final_concept}
        cost_projection = optimize_from_session(projection_session)
        if cost_projection:
            store.update_session(sid, cost_projection=cost_projection)
    except Exception:
        cost_projection = session.get("cost_projection") if isinstance(session.get("cost_projection"), dict) else None
    if visible_concept:
        await _fire_event(emit, "concept_plan", plan=visible_concept)
    session = store.get_session(sid) or session
    filtered_jobs = store.filter_active_jobs_for_session({**session, "active_jobs": active_jobs})
    await _fire_event(emit, "active_jobs", jobs=filtered_jobs)
    await _fire_event(emit, "session_state", **store.production_session_fields(session))
    return _turn_result(session, {
        "session_id": sid,
        "assistant_message": assistant_text,
        "pending_actions": pending,
        "pending_concept": visible_concept,
        "concept_plan": visible_concept,
        "cost_projection": cost_projection,
        "active_jobs": filtered_jobs,
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
    })


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
        result = await _approve_action_impl(
            session,
            action_id,
            membership_plan=membership_plan,
            billing_profile=billing_profile,
        )
        if admission.mode != "disabled":
            result["queue"] = admission.as_dict()
        return result


def _public_tool_args(args: dict[str, Any] | None) -> dict[str, Any]:
    """Strip private/runtime keys so last_production is JSON-safe to persist."""
    out: dict[str, Any] = {}
    for key, val in dict(args or {}).items():
        if str(key).startswith("_"):
            continue
        try:
            json.dumps(val)
        except (TypeError, ValueError):
            continue
        out[key] = val
    return out


async def _approve_action_impl(
    session: dict[str, Any],
    action_id: str,
    *,
    membership_plan: str = "",
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

    name = str(action.get("tool") or "").strip()
    if not name:
        raise KeyError(f"pending action missing tool: {action_id}")
    args = store.coerce_tool_arguments(action.get("arguments"))
    # Root-cause fix: never pass the session dict into store._latest_user_text —
    # that iterates string keys and raises 'str' object has no attribute 'get'.
    latest_user = store._latest_user_text(list(fresh.get("messages") or []), limit=4)
    if not latest_user:
        latest_user = _latest_user_text(fresh, limit=4)

    def _restore_pending_on_failure() -> None:
        """Put the action back so a transient Approve crash does not eat the card."""
        try:
            cur = store.get_session(sid) or fresh
            existing = [
                a for a in list(cur.get("pending_actions") or [])
                if isinstance(a, dict) and str(a.get("id") or "") != action_id
            ]
            restored = {
                "id": action_id,
                "tool": name,
                "arguments": args,
                "summary": str(action.get("summary") or f"{name} (restored after approve error)"),
            }
            if action.get("budget") is not None:
                restored["budget"] = action.get("budget")
            store.update_session(sid, pending_actions=[*existing, restored])
        except Exception:
            pass

    try:
        if store.is_ideation_request(latest_user) and name in BRAND_NEW_PRODUCTION_TOOLS:
            messages = list((store.get_session(sid) or fresh).get("messages") or [])
            assistant_text = (
                "Blocked production approval on an ideation/planning turn. "
                "This chat is for niche research and channel strategy — say when you want to analyze the upload or start rendering."
            )
            messages.append({"role": "assistant", "content": assistant_text})
            store.update_session(sid, messages=messages, pending_actions=[])
            return {
                "session_id": sid,
                "assistant_message": assistant_text,
                "pending_actions": [],
                "active_jobs": list((store.get_session(sid) or fresh).get("active_jobs") or []),
                "approval_mode": str((store.get_session(sid) or fresh).get("approval_mode") or "confirm"),
                "reasoning_depth": str((store.get_session(sid) or fresh).get("reasoning_depth") or "balanced"),
                "usage": {},
                "billing": {"credits_charged": 0, "provider_usd": 0.0, "prompt_tokens": 0, "completion_tokens": 0},
            }
        if name == "start_shortform_generate":
            fresh = store.get_session(sid) or fresh
            # Stamp title from the approve payload + session lock, then always spawn fresh
            # when this pending was prepared for a next short (_force_fresh) or title differs.
            force_fresh = bool(args.get("_force_fresh"))
            resolved = store.resolve_production_title(latest_user, fresh, fallback=str(
                (args.get("title") or args.get("topic") or "")
            ))
            if resolved:
                try:
                    from studio_agent.conversation import update_conversation_intent

                    intent = update_conversation_intent(fresh, locked_title=resolved)
                    store.update_session(sid, conversation_intent=intent)
                    fresh = store.get_session(sid) or fresh
                except Exception:
                    pass
                prior = store.prior_production_title(fresh)
                if prior and store._title_overlap_score(resolved, prior) < 0.75:
                    force_fresh = True
            args = _prepare_shortform_production_args(
                args, fresh, force_fresh=force_fresh, messages=list(fresh.get("messages") or []),
            )
            # Convert multi-scene Approve payload → Scene 1 proof for real execution.
            args = store._prepare_shortform_execution_args(
                args, list(fresh.get("messages") or []), session=fresh,
            )
            args = _force_production_title_on_args(args, session=fresh, user_text=latest_user or resolved)
            if force_fresh or args.get("_force_fresh"):
                args = _force_fresh_shortform_args(args)
                # Detach finished short so the new job is the only shortform track.
                store.update_session(
                    sid,
                    active_jobs=[
                        j for j in (fresh.get("active_jobs") or [])
                        if isinstance(j, dict) and str(j.get("kind") or "") not in {"shortform", "longform"}
                    ],
                    last_production={},
                    skip_job_recovery=False,  # allow the new job to be tracked/recovered
                )
                fresh = store.get_session(sid) or fresh
            # Do NOT conflict-block an explicit user Approve of a prepared start card.
            # Title was already stamped; blocking here was aborting the pipeline after Approve.

        # Defense for re-edit threads: if this pending action is a start tool but the conversation
        # has an active reply-to re-edit context in recent messages, redirect to surgical re-edit
        # instead of starting a brand new generation (user may have had a pending start from before the reply).
        msg_tail = [
            m for m in list((store.get_session(sid) or session).get("messages") or [])[-5:]
            if isinstance(m, dict)
        ]
        recent = "\n".join(str(m.get("content", "")) for m in msg_tail[-3:])
        if name in BRAND_NEW_PRODUCTION_TOOLS and "[User is replying to their previous" in recent:
            job_id = ""  # best effort; the re_edit will use what it can or the LLM will have specified
            # Try to extract from the note if present in the action summary or messages
            for m in reversed(msg_tail):
                if "[User is replying to their previous" in str(m.get("content", "")):
                    # crude extract
                    import re
                    m_job = re.search(r"job_id=([a-z0-9]+)", str(m.get("content", "")))
                    if m_job:
                        job_id = m_job.group(1)
                        break
            kind = "shortform"
            reedit_res = execute_tool_logged(
                "re_edit_production",
                {
                    "job_id": job_id or "unknown",
                    "instruction": f"[From approved pending during re-edit thread] {args}",
                    "kind": kind,
                    "command_id": action_id,
                },
                user_id=session["user_id"],
                content_format=session.get("content_format") or "short",
                session_id=sid,
            )
            return {"session_id": sid, "assistant_message": "Redirected pending start to surgical re-edit for the replied-to video.", "result": reedit_res}

        tool_error = ""
        try:
            args = dict(args or {})
            args.setdefault("_runpod_command_id", action_id)
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
        # Surface embedded tool errors (budget/credits/provider) even when no exception was raised.
        try:
            parsed_result = json.loads(result or "{}") if isinstance(result, str) else {}
        except json.JSONDecodeError:
            parsed_result = {}
        if not isinstance(parsed_result, dict):
            parsed_result = {}
        if not tool_error and parsed_result.get("error"):
            tool_error = str(parsed_result.get("error") or "").strip()
        if not tool_error and name in JOB_START_TOOLS and not started:
            tool_error = (
                f"{name} returned no job_id — production did not start. "
                f"Result preview: {str(result or '')[:240]}"
            )

        fresh = store.get_session(sid) or session
        messages: list[dict[str, Any]] = list(fresh.get("messages") or [])
        messages.append(_tool_observation_message(name, result))
        store.update_session(sid, messages=messages)

        if tool_error:
            hint = f"Approved {name} failed: {tool_error}"
            messages.append({"role": "assistant", "content": hint})
            # Restore pending so the user can retry Approve after a tool/budget failure.
            restore_action = {
                "id": action_id,
                "tool": name,
                "arguments": args,
                "summary": str(action.get("summary") or f"{name} (retry after error)"),
            }
            if action.get("budget") is not None:
                restore_action["budget"] = action.get("budget")
            store.update_session(sid, messages=messages, pending_actions=[restore_action])
            return {
                "session_id": sid,
                "assistant_message": hint,
                "pending_actions": [restore_action],
                "active_jobs": list((store.get_session(sid) or fresh).get("active_jobs") or []) or started,
                "approved_action": {
                    "id": action_id,
                    "tool": name,
                    "error": tool_error,
                    "result_preview": str(result or "")[:2000],
                },
            }

        # Job-start tools: return immediately so approve does not block on a second LLM turn
        # (RunPod /runsync budget is ~90s; shortform spawn should surface in the render dock).
        if name in JOB_START_TOOLS:
            try:
                parsed = json.loads(result or "{}") if isinstance(result, str) else {}
                if not isinstance(parsed, dict):
                    parsed = {}
                preview = parsed.get("error") or parsed.get("note") or parsed.get("status")
            except json.JSONDecodeError:
                parsed = {}
                preview = str(result or "")[:400]
            is_complete = _production_result_complete(parsed)
            job_title = str(
                (args.get("title") if isinstance(args, dict) else None)
                or (args.get("topic") if isinstance(args, dict) else None)
                or ((started[0].get("title") if started and isinstance(started[0], dict) else "") or "")
                or ""
            ).strip()
            if is_complete:
                assistant_note = "Your video is complete. You can review or download it from the production card."
            elif name == "start_longform_render":
                assistant_note = "I started the long-form production. I’ll show the chapter scenes here as they become ready for review."
            elif name == "start_shortform_generate":
                title_bit = f" **{job_title}**" if job_title else ""
                first = started[0] if started and isinstance(started[0], dict) else {}
                jid = str((first or {}).get("job_id") or parsed.get("job_id") or "").strip()
                assistant_note = (
                    f"Started building{title_bit}"
                    f"{f' (job `{jid}`)' if jid else ''}. "
                    "Scenes will appear in this chat for review before animation."
                )
            else:
                assistant_note = "The production is running. I’ll keep the progress visible here."
            messages.append({"role": "assistant", "content": assistant_note})
            # Merge new starts with non-production tracks; mark recovery open for the new job.
            existing = [
                j for j in (fresh.get("active_jobs") or [])
                if isinstance(j, dict) and (
                    str(j.get("kind") or "") not in {"shortform", "longform"}
                    or (started and str(j.get("job_id") or "") in {
                        str(s.get("job_id") or "") for s in started if isinstance(s, dict)
                    })
                )
            ]
            merged_jobs = merge_active_jobs(existing, started)
            public_args = _public_tool_args(args if isinstance(args, dict) else {})
            store.update_session(
                sid,
                messages=messages,
                active_jobs=merged_jobs,
                last_production={"tool": name, "arguments": public_args, "updated_at": time.time()},
                skip_job_recovery=False,
            )
            return {
                "session_id": sid,
                "assistant_message": assistant_note,
                "pending_actions": [],
                "active_jobs": merged_jobs,
                "approved_action": {"id": action_id, "tool": name, "result_preview": str(result or "")[:2000]},
            }

        if started:
            store.update_session(sid, active_jobs=started)

        refreshed = store.get_session(sid) or session
        follow_up = await _run_turn_impl(
            refreshed,
            "Continue production from the approved action result.",
            membership_plan=membership_plan or "",
            billing_profile=billing_profile,
        )
        follow_up["active_jobs"] = merge_active_jobs(
            started,
            follow_up.get("active_jobs") or [],
        )
        follow_up["approved_action"] = {
            "id": action_id,
            "tool": name,
            "result_preview": str(result or "")[:2000],
        }
        if started and not str(follow_up.get("assistant_message") or "").strip():
            follow_up["assistant_message"] = (
                f"Started {name} — track progress in the render dock and chat."
            )
        return follow_up
    except Exception:
        _restore_pending_on_failure()
        raise


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
        args = _prepare_shortform_production_args(args, fresh)
        # Resume the last shortform job's workspace so finished stills/clips/VO
        # are reused instead of re-rendered (and re-billed) from scratch.
        resume_job_id = None
        prev = [
            j for j in (fresh.get("active_jobs") or [])
            if j.get("kind") == "shortform" and j.get("job_id")
        ]
        if prev:
            candidate = str(prev[-1]["job_id"])
            try:
                from studio_agent.jobs import ROOT as _JOBS_ROOT, SKELETON_OUTPUT as _SKELETON_OUTPUT

                spec_path = (_JOBS_ROOT / _SKELETON_OUTPUT / candidate / "job_spec.json").resolve()
                spec = json.loads(spec_path.read_text(encoding="utf-8")) if spec_path.is_file() else {}
                wanted_topic = str(args.get("topic") or "").strip().lower()
                candidate_topic = str(spec.get("topic") or "").strip().lower()
                if not wanted_topic or wanted_topic == candidate_topic:
                    resume_job_id = candidate
            except Exception:
                resume_job_id = None
        latest_user = store._latest_user_text(list(fresh.get("messages") or []), limit=1)
        if (
            store.is_new_production_request(latest_user, fresh)
            or args.get("_force_fresh")
            or (
                store.get_locked_working_title(fresh)
                and store.prior_production_title(fresh)
                and store._title_overlap_score(
                    store.get_locked_working_title(fresh),
                    store.prior_production_title(fresh),
                ) < 0.75
            )
        ):
            resume_job_id = None
            args = _force_fresh_shortform_args(args)
        else:
            resume_job_id = resume_job_id or _matching_shortform_resume_job_id(fresh, args)
        if resume_job_id:
            args = {**args, "_resume_job_id": str(resume_job_id)}
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
    messages.append(_tool_observation_message(name, result))
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
