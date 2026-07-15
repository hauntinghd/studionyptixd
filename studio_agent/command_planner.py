"""Model-agnostic natural-language planner for typed Studio commands."""
from __future__ import annotations

import inspect
import json
import re
from collections.abc import Awaitable, Callable
from typing import Any

from pydantic import ValidationError

from studio_agent.command_contract import (
    CompilerProvenance,
    ModelCommandProposal,
    StudioCommand,
    approval_authorization_evidence,
    compiler_tool_schema,
    execution_authorization_evidence,
    extract_scene_count_request,
    make_turn_id,
    normalize_proposal,
)
from studio_agent.command_state import StudioStateContext


ChatCompletion = Callable[..., Awaitable[dict[str, Any]] | dict[str, Any]]

_NUMBER_WORDS = {
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
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
    "thirty": 30,
    "forty": 40,
    "fifty": 50,
    "sixty": 60,
}
_NUMBER_TOKEN = (
    r"(?:\d{1,3}|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|"
    r"thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|"
    r"thirty|forty|fifty|sixty)"
)
_APPROVAL_RE = re.compile(
    r"\b(?:i\s+)?(?:like|love|approve|approved)\s+(?:the\s+)?(?:first\s+scene|scene\s+(?:one|1))\b"
    r"|\b(?:first\s+scene|scene\s+(?:one|1))\s+(?:looks?|is)\s+(?:good|great|right|perfect)\b",
    re.IGNORECASE,
)
_EXECUTE_RE = re.compile(
    r"\b(?:let'?s\s+)?(?:go\s+ahead(?:\s+and)?|please)\s+(?:make|create|add|finish|build|animate)\b"
    r"|\b(?:make|create|add|finish|build|animate)\s+(?:the\s+)?(?:other|remaining|rest|next|more)\b",
    re.IGNORECASE,
)
_ANIMATION_NEGATION_RE = re.compile(
    r"\b(?:do\s+not|don'?t|never)\s+animat\w*\b|\bwithout\s+animation\b|"
    r"\b(?:keep|leave)\s+(?:them|those|these|the\s+(?:new|other|remaining)\s+scenes)\s+(?:still|static)\b|"
    r"\b(?:stills?|ken\s*burns)\s+only\b",
    re.IGNORECASE,
)


def _parse_number(token: str) -> int | None:
    raw = str(token or "").strip().lower().replace("-", " ")
    if raw.isdigit():
        return int(raw)
    if raw in _NUMBER_WORDS:
        return _NUMBER_WORDS[raw]
    parts = raw.split()
    if len(parts) == 2 and parts[0] in _NUMBER_WORDS and parts[1] in _NUMBER_WORDS:
        return _NUMBER_WORDS[parts[0]] + _NUMBER_WORDS[parts[1]]
    return None


def _extract_scene_counts(text: str, *, existing_count: int) -> tuple[int | None, int | None]:
    return extract_scene_count_request(text, existing_count=existing_count)


def _animation_scope(text: str, *, has_additional_scenes: bool) -> str | None:
    low = str(text or "").lower()
    if _ANIMATION_NEGATION_RE.search(low):
        return "none"
    if re.search(r"\banimate\s+(?:all|every)\b|\bevery\s+scene\s+(?:animated|moving)\b|\bfull\s+motion\b", low):
        return "all_scenes"
    if has_additional_scenes and re.search(
        r"\banimate\s+(?:them|those|these)\b|\band\s+animate(?:\s+them)?\b|"
        r"\banimate\s+(?:the\s+)?(?:other|remaining|new)\s+scenes?\b",
        low,
    ):
        return "new_scenes"
    if re.search(r"\bheroes?\s+only\b|\bselective\s+animation\b|\bkey\s+beats?\b", low):
        return "heroes"
    return None


def _duration_seconds(text: str) -> float | None:
    match = re.search(
        r"\b(\d{1,4}(?:\.\d+)?)\s*(?:seconds?|secs?|s)\b",
        str(text or ""),
        re.IGNORECASE,
    )
    if not match:
        return None
    try:
        value = float(match.group(1))
    except (TypeError, ValueError):
        return None
    return value if 0 < value <= 43_200 else None


def _literal_user_fragment(user_text: str, candidate: str) -> bool:
    normalized_user = re.sub(r"\s+", " ", str(user_text or "")).strip().casefold()
    normalized_candidate = re.sub(r"\s+", " ", str(candidate or "")).strip().casefold()
    return bool(normalized_candidate) and normalized_candidate in normalized_user


def _grounded_creative_direction(user_text: str, candidate: str) -> str:
    value = str(candidate or "").strip()
    if not _literal_user_fragment(user_text, value):
        return ""
    # Models sometimes copy the whole production command into this field. A
    # creative brief must contain an actual direction signal, not merely the
    # approval, cardinality, and execution language.
    if not re.search(
        r"\b(?:visual|style|look|palette|color|cinematic|pacing|tone|mood|caption|"
        r"sound|music|audio|motion\s+graphic|vfx|effect|lighting|camera|transition)\w*\b",
        value,
        re.IGNORECASE,
    ):
        return ""
    return value


def _matched_text(pattern: re.Pattern[str], text: str) -> str:
    match = pattern.search(str(text or ""))
    return str(match.group(0) if match else "").strip()[:300]


def _high_confidence_expand(text: str, state: StudioStateContext) -> bool:
    low = str(text or "").lower()
    if len(state.expandable_short_jobs()) != 1:
        return False
    continuity = bool(
        re.search(r"\b(?:other|remaining|rest\s+of|more|finish\s+the\s+(?:short|video))\b", low)
    )
    scene_context = bool(re.search(r"\bscenes?\b", low))
    return bool(
        approval_authorization_evidence(
            text,
            contextual_scene_review=len(state.expandable_short_jobs()) == 1,
        )
        and execution_authorization_evidence(text)
        and continuity
        and scene_context
    )


def _expand_candidate(text: str, state: StudioStateContext) -> bool:
    """Catch possible expand turns so unsafe/ambiguous language is consumed safely."""

    low = str(text or "").lower()
    if len(state.expandable_short_jobs()) != 1:
        return False
    continuity = bool(re.search(r"\b(?:other|remaining|rest\s+of|more|finish\s+the\s+(?:short|video))\b", low))
    scene_context = bool(re.search(r"\bscenes?\b", low))
    return bool(continuity and scene_context and (_APPROVAL_RE.search(text) or _EXECUTE_RE.search(text)))


def _default_mapping() -> dict[str, Any]:
    return {
        "action": "conversation",
        "target_source": "none",
        "target_job_id": "",
        "additional_scene_count": 0,
        "target_total_scene_count": 0,
        "preserve_scene_numbers": [],
        "animation_scope": "unspecified",
        "animation_scene_numbers": [],
        "duration_seconds": 0,
        "creative_direction": "",
        "existing_work_approved": False,
        "approval_evidence": "",
        "execution_requested": False,
        "execution_evidence": "",
        "clarification_question": "",
        "confidence": 0.0,
    }


def _proposal_from_mapping(mapping: dict[str, Any]) -> ModelCommandProposal:
    merged = _default_mapping()
    merged.update(mapping)
    return ModelCommandProposal.model_validate(merged)


def _extract_balanced_object(text: str) -> str:
    source = str(text or "").strip()
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", source, re.IGNORECASE | re.DOTALL)
    if fenced:
        return fenced.group(1)
    start = source.find("{")
    if start < 0:
        return ""
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(source)):
        char = source[index]
        if escaped:
            escaped = False
            continue
        if char == "\\" and in_string:
            escaped = True
            continue
        if char == '"':
            in_string = not in_string
        elif not in_string and char == "{":
            depth += 1
        elif not in_string and char == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]
    return source[start:]


def _parse_json_object(raw: Any) -> tuple[dict[str, Any] | None, bool]:
    if isinstance(raw, dict):
        return raw, False
    candidate = _extract_balanced_object(str(raw or ""))
    if not candidate:
        return None, False
    try:
        parsed = json.loads(candidate)
        return (parsed, False) if isinstance(parsed, dict) else (None, False)
    except Exception:
        try:
            from json_repair import repair_json

            parsed = repair_json(candidate, return_objects=True)
            return (parsed, True) if isinstance(parsed, dict) else (None, True)
        except Exception:
            return None, False


def _response_message(response: dict[str, Any]) -> dict[str, Any]:
    choices = response.get("choices") if isinstance(response, dict) else None
    if isinstance(choices, list) and choices and isinstance(choices[0], dict):
        message = choices[0].get("message")
        if isinstance(message, dict):
            return message
    return {}


def _proposal_from_response(response: dict[str, Any]) -> tuple[ModelCommandProposal | None, str]:
    message = _response_message(response)
    for call in list(message.get("tool_calls") or []):
        if not isinstance(call, dict):
            continue
        function = call.get("function") if isinstance(call.get("function"), dict) else {}
        if str(function.get("name") or "") != "emit_studio_command":
            continue
        parsed, repaired = _parse_json_object(function.get("arguments"))
        if parsed is None:
            return None, "tool_call"
        try:
            return _proposal_from_mapping(parsed), "repaired_json" if repaired else "tool_call"
        except ValidationError:
            return None, "tool_call"
    parsed, repaired = _parse_json_object(message.get("content"))
    if parsed is not None:
        try:
            return _proposal_from_mapping(parsed), "repaired_json" if repaired else "structured_json"
        except ValidationError:
            pass
    return None, "structured_json"


def _ground_proposal(
    proposal: ModelCommandProposal | None,
    *,
    user_text: str,
    state: StudioStateContext,
) -> tuple[ModelCommandProposal, bool]:
    """Apply deterministic facts that must not vary by selected model."""

    high_confidence = _high_confidence_expand(user_text, state)
    expand_candidate = _expand_candidate(user_text, state)
    if proposal is None:
        proposal = _proposal_from_mapping(
            {
                "action": "clarify",
                "clarification_question": "What would you like Studio to do next?",
            }
        )
    eligible = state.expandable_short_jobs()
    existing_count = eligible[0].scene_count if len(eligible) == 1 else 1
    existing_count = max(1, int(existing_count or 1))
    additional, total = _extract_scene_counts(user_text, existing_count=existing_count)
    continuity_reference = bool(
        re.search(
            r"\b(?:other|remaining|rest(?:\s+of)?|more|finish\s+the\s+(?:short|video))\b",
            str(user_text or ""),
            re.IGNORECASE,
        )
    )
    if additional is None and total is None and continuity_reference:
        recent_additional = int(state.recent_expansion_additional_scene_count or 0)
        recent_total = int(state.recent_expansion_total_scene_count or 0)
        if recent_additional > 0 and recent_total > existing_count:
            additional, total = recent_additional, recent_total
    scope = _animation_scope(user_text, has_additional_scenes=bool(additional))
    duration = _duration_seconds(user_text)
    approval_quote = approval_authorization_evidence(
        user_text,
        contextual_scene_review=len(eligible) == 1,
    )
    execution_quote = execution_authorization_evidence(user_text)
    updates: dict[str, Any] = {}
    if high_confidence or expand_candidate:
        updates.update(
            action="expand_existing_short",
            target_source="reply_to" if state.reply_target_job_id else "active_job",
            target_job_id="",
            preserve_scene_numbers=list(range(1, existing_count + 1)),
            existing_work_approved=bool(approval_quote),
            approval_evidence=approval_quote,
            execution_requested=bool(execution_quote),
            execution_evidence=execution_quote,
            confidence=max(0.99 if high_confidence else 0.75, proposal.confidence),
            clarification_question="",
            # Cardinality authorizes scene creation, not an invented duration,
            # motion scope, or creative brief. Only facts grounded in the user
            # turn may affect spend or the approved visual direction.
            duration_seconds=duration or 0,
            creative_direction=_grounded_creative_direction(user_text, proposal.creative_direction),
        )
    if proposal.action == "expand_existing_short" or expand_candidate or high_confidence:
        # A model may not invent cardinality. Resolve it from this turn or the
        # latest explicit, non-negated request stored on the same session.
        updates["additional_scene_count"] = additional or 0
        updates["target_total_scene_count"] = total or 0
    if additional is not None:
        updates["additional_scene_count"] = additional
    if total is not None:
        updates["target_total_scene_count"] = total
    if scope is not None or high_confidence:
        # Negation and pronoun resolution are deterministic safety facts.
        updates["animation_scope"] = scope or "none"
        updates["animation_scene_numbers"] = []
    if proposal.action == "expand_existing_short" or expand_candidate:
        # Authorization is a deterministic user-text fact. A model may neither
        # invent it nor preserve a positive substring embedded in negation.
        updates.update(
            existing_work_approved=bool(approval_quote),
            approval_evidence=approval_quote,
            execution_requested=bool(execution_quote),
            execution_evidence=execution_quote,
        )
    if proposal.action == "expand_existing_short" and len(eligible) == 1 and not proposal.target_job_id:
        updates["target_source"] = "reply_to" if state.reply_target_job_id else "active_job"
    return proposal.model_copy(update=updates), high_confidence


async def plan_studio_command(
    user_text: str,
    state: StudioStateContext,
    *,
    model: str | None = None,
    turn_id: str | None = None,
    chat_completion: ChatCompletion | None = None,
) -> StudioCommand:
    """Compile one user turn with the selected model, then deterministically ground it."""

    if chat_completion is None:
        from studio_agent.openrouter import chat_completion as default_chat_completion

        chat_completion = default_chat_completion
    messages = [
        {
            "role": "system",
            "content": (
                "You are Studio's semantic command compiler. Emit exactly one emit_studio_command call. "
                "Do not call production tools. Scene numbers are human-facing and 1-based. Distinguish "
                "additional scenes from total scenes. Resolve 'them' to the closest mentioned scene set. "
                "Negation wins: 'do not animate them' means animation_scope=none. Evidence fields must "
                "be exact substrings of the user message. Use clarify instead of guessing a target."
            ),
        },
        {
            "role": "user",
            "content": json.dumps(
                {
                    "studio_state": state.model_payload(),
                    "user_message": str(user_text or ""),
                },
                ensure_ascii=False,
            ),
        },
    ]
    response: dict[str, Any] = {}
    proposal: ModelCommandProposal | None = None
    transport = "deterministic_fallback"
    try:
        maybe_response = chat_completion(
            messages=messages,
            tools=[compiler_tool_schema()],
            model=model,
            temperature=0.0,
            reasoning_depth="fast",
            web_search=False,
            force_tool_call=True,
            preserve_tool_names=frozenset({"emit_studio_command"}),
        )
        response = await maybe_response if inspect.isawaitable(maybe_response) else maybe_response
        if not isinstance(response, dict):
            response = {}
        proposal, transport = _proposal_from_response(response)
    except Exception:
        response = {}
        proposal = None
        transport = "deterministic_fallback"
    proposal_was_missing = proposal is None
    proposal, high_confidence = _ground_proposal(
        proposal,
        user_text=user_text,
        state=state,
    )
    if proposal_was_missing:
        transport = "deterministic_fallback"
    elif high_confidence and transport not in {"tool_call", "structured_json", "repaired_json"}:
        transport = "deterministic_fallback"
    resolved_model = str(response.get("model") or model or "")
    provenance = CompilerProvenance(
        requested_model=str(model or ""),
        resolved_model=resolved_model,
        provider=str(response.get("provider") or "unknown"),
        response_id=str(response.get("id") or ""),
        transport=transport,  # type: ignore[arg-type]
        attempts=1,
    )
    resolved_turn_id = turn_id or make_turn_id(
        session_id=state.session_id,
        state_revision=state.state_revision,
        user_text=user_text,
    )
    return normalize_proposal(
        proposal,
        session_id=state.session_id,
        turn_id=resolved_turn_id,
        user_text=user_text,
        provenance=provenance,
    )


compile_studio_command = plan_studio_command
