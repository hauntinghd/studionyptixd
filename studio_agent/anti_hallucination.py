"""Studio Agent anti-hallucination guard.

This adapts Casey's Jarvis/CodeBot five-layer system to Studio Agent's
creator workflow. The original CodeBot chain is:

Router -> Engineer -> Auditor -> Corrector -> Verifier

For Studio, those layers mean:

1. Router: route requests to required evidence/tool families before answering.
2. Engineer: enforce tool/job contracts for production and action claims.
3. Auditor: compare claims against actual tool evidence and known memory.
4. Corrector: scrub hedging, fake-search language, placeholders, and loops.
5. Verifier: block final answers that still lack required proof/artifacts.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Sequence


@dataclass(frozen=True)
class ToolFire:
    name: str
    args: dict[str, Any] = field(default_factory=dict)
    result: str = ""


@dataclass
class AuditReport:
    warnings: list[str] = field(default_factory=list)
    blocked_claims: list[str] = field(default_factory=list)

    @property
    def has_issues(self) -> bool:
        return bool(self.warnings or self.blocked_claims)

    @property
    def has_blockers(self) -> bool:
        return bool(self.blocked_claims)

    def for_history_correction(self) -> str:
        if not self.has_issues:
            return ""
        parts = [
            "ANTI-HALLUCINATION AUDIT (silent correction for Studio Agent):",
        ]
        for item in self.blocked_claims:
            parts.append(f"- Blocked claim: {item}")
        for item in self.warnings:
            parts.append(f"- Warning: {item}")
        parts.append(
            "On the next answer, ground claims in user-provided facts, memory, or tool results. "
            "If the required evidence is missing, do not narrate a future tool call as if it ran. "
            "Either the backend must execute the tool first, or answer with the exact missing evidence/next step."
        )
        return "\n".join(parts)


@dataclass(frozen=True)
class LayerCheck:
    name: str
    passed: bool = True
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class FiveLayerAudit:
    layers: tuple[LayerCheck, ...]
    report: AuditReport

    @property
    def passed(self) -> bool:
        return not self.report.has_blockers

    def layer(self, name: str) -> LayerCheck | None:
        aliases = {
            "source_authority": "router",
            "claim_type": "router",
            "tool_evidence": "engineer",
            "memory_contradiction": "auditor",
            "final_correction": "verifier",
        }
        wanted = aliases.get(name, name)
        for item in self.layers:
            if item.name == wanted:
                return item
        return None


_CURRENT_INFO_RE = re.compile(
    r"\b(latest|current(?:ly)?|most recent|recent(?:ly)?|today|right now|live|fresh|up[- ]?to[- ]?date|newest|"
    r"what'?s? (?:new|happening|going on|the latest)|update me|catch me up|news|breaking)\b",
    re.IGNORECASE,
)

_FAKE_LIVE_DATA_RE = re.compile(
    r"\b("
    r"fresh search|live refresh|live search|verified current|current as of|as of today|"
    r"i just looked|i just searched|i just pulled|i just checked|pulled live data|pulling live data|"
    r"according to the latest|the latest data shows|the latest news shows|from a live web search"
    r")\b",
    re.IGNORECASE,
)
_FAKE_TOOL_PROGRESS_RE = re.compile(
    r"\b("
    r"(?:one|two|three|four|five|\d+)\s+data pulls?\s+(?:running|active|started|in progress)|"
    r"data pulls? running simultaneously|"
    r"let me read (?:all )?(?:the )?(?:returned )?(?:data|results)|"
    r"read (?:all )?(?:the )?returned (?:data|results)|"
    r"results? (?:are|is) still processing|"
    r"cross-?reference (?:them|the data|the results)|"
    r"i now have .*?data pulls?"
    r")\b",
    re.IGNORECASE,
)

_CHANNEL_DATA_CLAIM_RE = re.compile(
    r"\b("
    r"based on your channel data|channel data shows|your analytics show|"
    r"your top performers|your .*? performance|your .*? videos are (?:crushing|performing)|"
    r"your .*? baseline|views? .*? better than baseline|ctr|avd|retention|subscribers? gained|watch time"
    r")\b",
    re.IGNORECASE,
)
_TOOL_NARRATION_RE = re.compile(r"(?im)^\s*Tool:\s*([a-zA-Z_][\w.-]*)\b")

_SPECIFIC_RETENTION_CLAIM_RE = re.compile(
    r"\b("
    r"(?:50|51|52|53|54|55|56|57|58|59|60|6[0-9])\s*%?\s*(?:avd|average view|retention)|"
    r"high[- ]retention (?:short|video)|specific video .*?(?:avd|retention)|"
    r"video .*?(?:hit|has|had|got|gets).*?(?:avd|retention)|"
    r"there'?s definitely a video|i should be seeing that data|i should be able to pull"
    r")\b",
    re.IGNORECASE,
)

_PRODUCTION_COMPLETE_RE = re.compile(
    r"\b("
    r"production (?:is )?complete|render (?:is )?complete|video (?:is )?ready|"
    r"re-?edit (?:is )?complete|i (?:fixed|rebuilt|recomposed) (?:that|the) .*?(?:video|production)|"
    r"download (?:the )?(?:mp4|video)|your .*? is ready"
    r")\b",
    re.IGNORECASE,
)

_ACTION_LANGUAGE_RE = re.compile(
    r"\b("
    r"i'?m (?:applying|fixing|building|starting|running|deploying|rendering|editing|re-editing|submitting|resubmitting|checking|pulling)|"
    r"i (?:applied|fixed|built|started|ran|deployed|rendered|edited|re-edited|checked|pulled)|"
    r"starting now|running now|rendering now|submitting now|resubmitting now|checking completion|checking the status"
    r")\b",
    re.IGNORECASE,
)

_COMMITMENT_RE = re.compile(
    r"\b("
    r"i(?:'ll| will)\s+(?:call|fix|rewrite|retry|submit|resubmit|run|launch|start|build|generate|create|pull|fetch|search|look|check|write|edit|update|patch|deploy|install|test|verify)|"
    r"let\s+me\s+(?:call|fix|rewrite|retry|submit|resubmit|run|launch|start|build|generate|create|pull|fetch|search|look|check|write|edit|update|patch|deploy|install|test|verify|try)|"
    r"i'?m\s+going\s+to\s+(?:call|fix|rewrite|retry|submit|resubmit|run|launch|start|build|generate|create|pull|fetch|search|look|check|write|edit|update|patch|deploy|install|test|verify)|"
    r"still\s+working\s+on\s+(?:the|that|this|it|a)\s+(?:fix|rewrite|retry|build|deploy|install|patch|update|search|lookup|test|render|edit)"
    r")\b",
    re.IGNORECASE,
)

_ACTION_COMMITMENT_RE = re.compile(
    r"\b(?:call|fix|rewrite|retry|submit|resubmit|run|launch|start|build|generate|create|write|edit|update|patch|deploy|install|test)\b",
    re.IGNORECASE,
)

_ALLOWED_COMMITMENT_RE = re.compile(
    r"\bi'?ll\s+(?:keep\s+you\s+posted|let\s+you\s+know|brief\s+you|wait|hold|be\s+here|stay\s+here)\b",
    re.IGNORECASE,
)

_RECOMMENDATION_RE = re.compile(
    r"\b(recommend|next video|next topic|start building|produce|make this next|your next)\b",
    re.IGNORECASE,
)

_HALLUCINATION_PATTERNS = [
    re.compile(r"\bI don'?t have access to\b", re.IGNORECASE),
    re.compile(r"\bI cannot (?:read|see|access|check|verify)\b", re.IGNORECASE),
    re.compile(r"\bI can'?t (?:browse|search|look up|access) (?:YouTube|the web|online)\b", re.IGNORECASE),
    re.compile(r"\bAs an AI language model\b", re.IGNORECASE),
    re.compile(r"\bI don'?t actually have\b", re.IGNORECASE),
    re.compile(r"\bI cannot directly\b", re.IGNORECASE),
    re.compile(r"\bI'?m unable to\b", re.IGNORECASE),
    re.compile(r"\bI don'?t see any files\b", re.IGNORECASE),
    re.compile(r"\bI cannot verify\b", re.IGNORECASE),
    re.compile(r"\bI don'?t have the ability to\b", re.IGNORECASE),
    re.compile(r"\bI'?m an AI and can'?t\b", re.IGNORECASE),
    re.compile(r"\brest of the code remains the same\b", re.IGNORECASE),
    re.compile(r"\.\.\.\s*\(implementation\)", re.IGNORECASE),
    re.compile(r"//\s*your code here", re.IGNORECASE),
    re.compile(r"//\s*TODO implement", re.IGNORECASE),
    re.compile(r"\bplaceholder content\b", re.IGNORECASE),
]

_VAGUE_PHRASES = [
    "you might want to",
    "you could try",
    "it depends on",
    "there are many ways",
    "it's possible that",
    "you may need to",
    "consider checking",
    "you should probably",
]

_COUNT_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
}

_COUNT_CLAIM_RULES: tuple[tuple[re.Pattern[str], set[str], str], ...] = (
    (
        re.compile(r"\b(?:generated|created|rendered|made|produced|queued)\s+(\d+|one|two|three|four|five|six|seven|eight|nine|ten)\s+(?:shorts?|videos?|renders?|productions?)\b", re.IGNORECASE),
        {"start_shortform_generate", "start_longform_render", "finalize_production", "finalize_longform_render"},
        "generated/rendered production count",
    ),
    (
        re.compile(r"\b(?:analyzed|pulled|checked)\s+(\d+|one|two|three|four|five|six|seven|eight|nine|ten)\s+(?:channels?|videos?|references?|competitors?)\b", re.IGNORECASE),
        {"get_channel_analytics", "refresh_channel_intelligence", "analyze_reference_video", "analyze_competitor_video"},
        "analysis count",
    ),
)

_SEARCH_TOOLS = {
    "search_youtube_public",
    "get_public_search_trends",
    "recommend_video_topics",
    "analyze_reference_video",
    "analyze_competitor_video",
    "fetch_archival_for_video",
    "web_search",
    "web_fetch",
    "xai_web_search",
    "xai_research",
}

_CHANNEL_DATA_TOOLS = {
    "list_youtube_channels",
    "get_channel_analytics",
    "refresh_channel_intelligence",
    "recommend_video_topics",
}

_ACTION_TOOLS = {
    "ingest_product_reference",
    "start_shortform_generate",
    "start_longform_render",
    "re_edit_production",
    "edit_production_scene_still",
    "regenerate_production_scene_still",
    "set_production_scenes_animate",
    "set_production_scene_duration",
    "animate_production_scenes",
    "finalize_production",
    "finalize_longform_render",
    "record_production_feedback",
    "remember_perpetual_memory",
    "refresh_channel_intelligence",
    "poll_render_job",
}

_PRODUCTION_TOOLS = {
    "start_shortform_generate",
    "start_longform_render",
    "re_edit_production",
    "finalize_production",
    "finalize_longform_render",
    "poll_render_job",
}

_EMPIRE_MAGNATES_POSTED = {
    "mango markets",
    "bre-x",
    "bre x",
    "olympus",
    "denmark loophole",
    "germany",
    "wirecard",
    "1.7 billion from denmark",
    "1.9 billion from germany",
    "trader who legally stole $114",
    "trader who legally stole 114",
}


def _lower(text: str | None) -> str:
    return (text or "").strip().lower()


def _tool_names(tool_fires: Sequence[ToolFire]) -> set[str]:
    return {str(t.name or "") for t in tool_fires if str(t.name or "")}


def _textual_tool_names(text: str | None) -> set[str]:
    return {match.group(1) for match in _TOOL_NARRATION_RE.finditer(text or "")}


def _tool_count(tool_fires: Sequence[ToolFire], names: set[str]) -> int:
    return sum(1 for fire in tool_fires if fire.name in names)


def _has_any_tool(tool_fires: Sequence[ToolFire], names: set[str]) -> bool:
    return _tool_count(tool_fires, names) > 0


def _tool_fire_succeeded(fire: ToolFire) -> bool:
    raw = str(fire.result or "").strip()
    if not raw:
        return False
    try:
        data = json.loads(raw)
    except Exception:
        return not bool(re.match(r"^(?:error|failed|failure)\b", raw, re.IGNORECASE))
    if not isinstance(data, dict):
        return True
    if data.get("error"):
        return False
    status = str(data.get("status") or "").strip().lower()
    return status not in {"error", "failed", "failure", "rejected"}


def _latest_tool_succeeded(tool_fires: Sequence[ToolFire], names: set[str]) -> bool:
    for fire in reversed(tool_fires):
        if fire.name in names:
            return _tool_fire_succeeded(fire)
    return False


def _analytics_retention_unavailable(tool_fires: Sequence[ToolFire]) -> bool:
    saw_analytics = False
    for fire in tool_fires:
        if fire.name != "get_channel_analytics":
            continue
        saw_analytics = True
        try:
            data = json.loads(fire.result) if isinstance(fire.result, str) and fire.result.strip().startswith("{") else {}
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        quality = data.get("analytics_data_quality") if isinstance(data.get("analytics_data_quality"), dict) else {}
        if quality:
            if quality.get("video_level_retention_available") is True:
                return False
            if quality.get("video_level_retention_available") is False:
                return True
        live = data.get("youtube_analytics_live") if isinstance(data.get("youtube_analytics_live"), dict) else {}
        metrics = live.get("video_metrics") if isinstance(live.get("video_metrics"), dict) else {}
        if metrics.get("video_level_retention_available") is True:
            return False
    return saw_analytics


def _parse_result(result: str) -> dict[str, Any]:
    if not result:
        return {}
    try:
        data = json.loads(result)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _production_completed_this_turn(tool_fires: Sequence[ToolFire]) -> bool:
    for fire in tool_fires:
        if fire.name not in _PRODUCTION_TOOLS:
            continue
        data = _parse_result(fire.result)
        status = str(data.get("status") or "").lower()
        phase = str(data.get("phase") or data.get("stage") or "").lower()
        has_deliverable = bool(data.get("video_path") or data.get("mp4_url") or data.get("download_url"))
        if status in {"complete", "completed", "ready"} and has_deliverable:
            return True
        if phase in {"done", "complete", "completed", "ready"} and has_deliverable:
            return True
    return False


def _mentions_posted_empire_topic(text: str) -> str | None:
    low = _lower(text)
    for topic in sorted(_EMPIRE_MAGNATES_POSTED, key=len, reverse=True):
        if topic in low:
            return topic
    return None


def _parse_count(token: str) -> int | None:
    raw = token.strip().lower()
    if raw.isdigit():
        return int(raw)
    return _COUNT_WORDS.get(raw)


def _scrub_phrases(text: str) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    for pattern in _HALLUCINATION_PATTERNS:
        match = pattern.search(text)
        if match:
            blockers.append(f"disallowed hallucination/placeholder phrase: {match.group(0)!r}")
    low = text.lower()
    for phrase in _VAGUE_PHRASES:
        if phrase in low:
            warnings.append(f"vague advisory phrase should be replaced with a concrete next step: {phrase!r}")
    return blockers, warnings


def _commitment_without_execution(text: str, tool_fires: Sequence[ToolFire]) -> str | None:
    if _ALLOWED_COMMITMENT_RE.search(text):
        masked = _ALLOWED_COMMITMENT_RE.sub("", text)
        match = _COMMITMENT_RE.search(masked)
    else:
        match = _COMMITMENT_RE.search(text)
    if not match:
        return None
    commitment = match.group(0).strip()
    if _ACTION_COMMITMENT_RE.search(commitment):
        return None if _latest_tool_succeeded(tool_fires, _ACTION_TOOLS) else commitment
    evidence_tools = _SEARCH_TOOLS | _CHANNEL_DATA_TOOLS
    return None if _latest_tool_succeeded(tool_fires, evidence_tools) else commitment


def _router_layer(
    *,
    assistant_text: str,
    user_text: str,
    tool_fires: Sequence[ToolFire],
) -> LayerCheck:
    blockers: list[str] = []
    textual_tools = _textual_tool_names(assistant_text)
    if textual_tools:
        fired_tools = _tool_names(tool_fires)
        missing = sorted(name for name in textual_tools if name not in fired_tools)
        if missing:
            blockers.append(
                "printed tool-call text without executing matching backend tool(s): "
                + ", ".join(missing)
            )

    if _FAKE_LIVE_DATA_RE.search(assistant_text) and not _has_any_tool(tool_fires, _SEARCH_TOOLS | _CHANNEL_DATA_TOOLS):
        blockers.append("claimed fresh/live/current data without a search, reference, or channel analytics tool")

    if _FAKE_TOOL_PROGRESS_RE.search(assistant_text) and not _has_any_tool(
        tool_fires, _SEARCH_TOOLS | _CHANNEL_DATA_TOOLS | _ACTION_TOOLS
    ):
        blockers.append("claimed backend tool/search work was running without executed tool evidence")

    if _CURRENT_INFO_RE.search(user_text or "") and not _has_any_tool(tool_fires, _SEARCH_TOOLS | _CHANNEL_DATA_TOOLS):
        grounded_refusal = bool(re.search(r"\bneed (?:a |the )?(?:live|search|reference|channel|analytics|data|tool)\b", _lower(assistant_text)))
        if not grounded_refusal and len(assistant_text.strip()) > 40:
            blockers.append("answered a current/latest request without routing to a current data tool first")

    if _CHANNEL_DATA_CLAIM_RE.search(assistant_text) and not _has_any_tool(tool_fires, _CHANNEL_DATA_TOOLS):
        blockers.append("claimed channel analytics/performance evidence without a channel-data tool")

    if _SPECIFIC_RETENTION_CLAIM_RE.search(assistant_text) and _analytics_retention_unavailable(tool_fires):
        blockers.append("claimed or implied specific video-level AVD/retention while analytics tool lacked video-level retention rows")

    return LayerCheck("router", passed=not blockers, blockers=tuple(dict.fromkeys(blockers)))


def _engineer_layer(
    *,
    assistant_text: str,
    tool_fires: Sequence[ToolFire],
) -> LayerCheck:
    blockers: list[str] = []
    warnings: list[str] = []

    if _PRODUCTION_COMPLETE_RE.search(assistant_text) and not _production_completed_this_turn(tool_fires):
        blockers.append("claimed production/re-edit completion without a completed production tool result")

    if _ACTION_LANGUAGE_RE.search(assistant_text) and not _latest_tool_succeeded(tool_fires, _ACTION_TOOLS):
        warnings.append("used action language without a successful action-taking Studio tool in the turn")

    commitment = _commitment_without_execution(assistant_text, tool_fires)
    if commitment:
        blockers.append(f"promised execution without firing a matching tool: {commitment!r}")

    for pattern, expected_tools, label in _COUNT_CLAIM_RULES:
        for match in pattern.finditer(assistant_text):
            claimed = _parse_count(match.group(1))
            if claimed is None:
                continue
            actual = _tool_count(tool_fires, expected_tools)
            if actual < claimed:
                blockers.append(f"claimed {label} of {claimed}, but only {actual} matching tool result(s) fired")

    return LayerCheck(
        "engineer",
        passed=not blockers,
        blockers=tuple(dict.fromkeys(blockers)),
        warnings=tuple(dict.fromkeys(warnings)),
    )


def _auditor_layer(
    *,
    assistant_text: str,
    user_text: str,
) -> LayerCheck:
    blockers: list[str] = []
    posted = _mentions_posted_empire_topic(assistant_text)
    if posted and _RECOMMENDATION_RE.search(assistant_text):
        user_allows_old_topic = bool(re.search(r"\b(remake|re-edit|reference|analyz|compare|use as signal)\b", _lower(user_text)))
        if not user_allows_old_topic:
            blockers.append(f"recommended already-posted Empire Magnates topic as new work: {posted}")

    return LayerCheck("auditor", passed=not blockers, blockers=tuple(blockers))


def _corrector_layer(*, assistant_text: str) -> LayerCheck:
    blockers, warnings = _scrub_phrases(assistant_text)
    return LayerCheck(
        "corrector",
        passed=not blockers,
        blockers=tuple(blockers),
        warnings=tuple(warnings),
    )


def _verifier_layer(report: AuditReport) -> LayerCheck:
    blockers: list[str] = []
    if report.has_blockers:
        blockers.append("unsafe draft must be replaced with guarded correction")
    return LayerCheck("verifier", passed=not blockers, blockers=tuple(blockers))


def five_layer_audit(
    *,
    assistant_text: str,
    user_text: str,
    tool_fires: Sequence[ToolFire],
) -> FiveLayerAudit:
    report = AuditReport()
    text = assistant_text or ""

    if not text.strip():
        return FiveLayerAudit(
            layers=(
                LayerCheck("router"),
                LayerCheck("engineer"),
                LayerCheck("auditor"),
                LayerCheck("corrector"),
                LayerCheck("verifier"),
            ),
            report=report,
        )

    first_four = (
        _router_layer(assistant_text=text, user_text=user_text, tool_fires=tool_fires),
        _engineer_layer(assistant_text=text, tool_fires=tool_fires),
        _auditor_layer(assistant_text=text, user_text=user_text),
        _corrector_layer(assistant_text=text),
    )
    for layer in first_four:
        report.blocked_claims.extend(layer.blockers)
        report.warnings.extend(layer.warnings)

    report.blocked_claims = list(dict.fromkeys(report.blocked_claims))
    report.warnings = list(dict.fromkeys(report.warnings))

    final_layer = _verifier_layer(report)
    layers = (*first_four, final_layer)
    return FiveLayerAudit(layers=layers, report=report)


def audit_turn(
    *,
    assistant_text: str,
    user_text: str,
    tool_fires: Sequence[ToolFire],
) -> AuditReport:
    """Backward-compatible final turn audit used by the Studio Agent runner."""
    return five_layer_audit(
        assistant_text=assistant_text,
        user_text=user_text,
        tool_fires=tool_fires,
    ).report


def guard_text(assistant_text: str, report: AuditReport) -> str:
    """Replace unsafe final text with an honest, actionable correction."""
    if not report.has_blockers:
        return assistant_text

    joined = " ".join(report.blocked_claims).lower()
    if "production" in joined or "re-edit" in joined or "render" in joined:
        return (
            "I should not call that complete yet. The edit/render needs to stay visible as an in-progress job "
            "until Studio has an actual completed production result. I saved the request and will track the render "
            "state instead of pretending it finished."
        )

    if "already-posted" in joined:
        return (
            "You are right to treat that as reference data, not the next upload. I will use the already-posted "
            "Empire Magnates videos as performance/style evidence and avoid recommending them as new work."
        )

    if "channel" in joined or "analytics" in joined:
        if "video-level avd" in joined or "video-level retention" in joined:
            return (
                "I cannot name an exact high-AVD winner from this tool result yet because per-video retention rows "
                "are unavailable. I should continue from the available selected-channel snapshot/public data, make "
                "that limitation explicit, and avoid inventing a specific winner."
            )
        return (
            "The required channel analytics tool did not run in this turn, so I cannot make a grounded performance "
            "claim from it. Retry this request with the selected channel still attached; Studio should route through "
            "the forced channel-data preflight and then answer from that tool result."
        )

    if "browse" in joined or "youtube" in joined:
        return (
            "I should use the available public YouTube search/reference tools instead of saying I cannot browse. "
            "If the search tool fails, I need to report the exact quota, auth, or network limitation."
        )

    if "fresh/live/current" in joined or "current/latest" in joined or "search" in joined:
        return (
            "I need a live/search/reference result before presenting that as current. I will verify it first, "
            "then answer from the evidence."
        )

    if "promised execution" in joined:
        return (
            "I should not narrate work without executing it. I need to run the matching Studio tool now or name "
            "the exact blocker instead of saying I am doing it."
        )

    return "I caught an unsupported claim before sending it. I will verify the missing evidence first."
