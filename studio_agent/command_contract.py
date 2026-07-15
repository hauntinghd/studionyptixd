"""Typed, model-agnostic command contract for Studio Agent.

Models emit :class:`ModelCommandProposal`, a deliberately flat schema made of
simple JSON primitives.  The server then normalizes that untrusted proposal
into :class:`StudioCommand`; identifiers and source hashes are never supplied
by the model.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


CommandAction = Literal["conversation", "clarify", "expand_existing_short"]
TargetSource = Literal["none", "reply_to", "active_job", "explicit_job_id", "recent_job"]
AnimationScope = Literal[
    "unspecified",
    "none",
    "new_scenes",
    "all_scenes",
    "heroes",
    "explicit",
]


_APPROVAL_POSITIVE_RE = re.compile(
    r"\b(?:i\s+)?(?:like|love|approve|approved)\s+(?:the\s+)?(?:first\s+scene|scene\s+(?:one|1))\b"
    r"|\b(?:first\s+scene|scene\s+(?:one|1))\s+(?:looks?|is)\s+(?:good|great|right|perfect)\b",
    re.IGNORECASE,
)
_APPROVAL_NEGATED_RE = re.compile(
    r"\b(?:do\s+not|don'?t|dont|did\s+not|didn'?t|never)\s+(?:really\s+)?"
    r"(?:like|love|approve)\s+(?:the\s+)?(?:first\s+scene|scene\s+(?:one|1))\b"
    r"|\b(?:reject|dislike|hate)\s+(?:the\s+)?(?:first\s+scene|scene\s+(?:one|1))\b",
    re.IGNORECASE,
)
_APPROVAL_CONDITIONAL_RE = re.compile(
    r"\b(?:if|unless)\s+(?:i|we)\s+(?:would\s+|did\s+)?"
    r"(?:like|liked|love|loved|approve|approved)\s+(?:the\s+)?"
    r"(?:first\s+scene|scene\s+(?:one|1))\b",
    re.IGNORECASE,
)
_CONTEXTUAL_APPROVAL_POSITIVE_RE = re.compile(
    r"^\s*(?:yes|yeah|yep|yup|sure|okay|ok|good|great|perfect|looks?\s+good|"
    r"that(?:'s|\s+is)?\s+(?:good|great|right|perfect)|that\s+works|"
    r"works\s+for\s+me|love\s+it|i\s+like\s+it)\b",
    re.IGNORECASE,
)
_CONTEXTUAL_APPROVAL_BLOCK_RE = re.compile(
    r"\b(?:not\s+(?:good|great|right|ready|approved)|do\s+not\s+approve|don'?t\s+approve|"
    r"(?:needs?|requires?)\s+(?:a\s+)?(?:fix|change|redo|rework)|"
    r"(?:fix|change|redo|rework|regenerate|reanimate)\s+(?:it|that|scene))\b",
    re.IGNORECASE,
)
_DIRECT_EXECUTION_RE = re.compile(
    r"\blet'?s\s+(?:go\s+ahead(?:\s+and)?\s+)?(?:make|create|add|finish|build|generate|start|produce|continue)\b"
    r"|\bgo\s+ahead(?:\s+and)?\s+(?:make|create|add|finish|build|generate|start|produce|continue)\b"
    r"|\bplease\s+(?:make|create|add|finish|build|generate|start|produce|continue)\b"
    r"|\b(?:can|could|would|will)\s+you\s+(?:please\s+)?(?:make|create|add|finish|build|generate|start|produce|continue)\b"
    r"|\bi\s+(?:want|need)\s+(?:you\s+to\s+)?(?:make|create|add|finish|build|generate|start|produce|continue)\b"
    r"|(?:^|[.!?;,]\s*)(?:(?:now|then|so|okay|ok)\s+)?"
    r"(?:go\s+ahead(?:\s+and)?\s+)?(?:make|create|add|finish|build|generate|start|produce|continue)\b",
    re.IGNORECASE | re.MULTILINE,
)
_EXECUTION_NEGATED_RE = re.compile(
    r"\b(?:do\s+not|don'?t|dont|never)\s+"
    r"(?:(?:actually|really|just|please)\s+){0,3}(?:go\s+ahead(?:\s+and)?\s+)?"
    r"(?:make|create|add|finish|build|generate|start|produce|run|continue)\b"
    r"|\blet'?s\s+not\s+(?:go\s+ahead(?:\s+and)?\s+)?"
    r"(?:make|create|add|finish|build|generate|start|produce|run|continue|do)\b",
    re.IGNORECASE,
)
_EXECUTION_DEFERRED_RE = re.compile(
    r"\b(?:hold\s+off|pause|defer|delay)\s+(?:on\s+)?"
    r"(?:making|creating|adding|finishing|building|generating|starting|producing|continuing)\b"
    r"|\b(?:make|create|add|finish|build|generate|start|produce|continue)\b.{0,60}"
    r"\b(?:not\s+yet|maybe\s+later|later|another\s+time)\b"
    r"|\b(?:maybe|perhaps)\s+later\b",
    re.IGNORECASE | re.DOTALL,
)
_EXECUTION_HYPOTHETICAL_RE = re.compile(
    r"\b(?:how|what)\s+(?:would|could|might|should)\s+(?:you|studio)\s+"
    r"(?:make|create|add|finish|build|generate|start|produce|continue)\b"
    r"|\bwhat\s+would\s+happen\s+if\b",
    re.IGNORECASE,
)
_ASSERTION_SCOPE_BLOCK_RE = re.compile(
    r"\b(?:do\s+not|don'?t|dont|did\s+not|didn'?t|never|not|can'?t|cannot|"
    r"couldn'?t|wouldn'?t|shouldn'?t|won'?t|if|unless|whether|maybe|perhaps|"
    r"might|wonder|unsure|uncertain|guess|suppose|think|how|what)\b",
    re.IGNORECASE,
)
_CLAUSE_CONTRAST_RE = re.compile(r"\b(?:but|however|instead|nevertheless)\b", re.IGNORECASE)

_SCENE_COUNT_WORDS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
    "seven": 7, "eight": 8, "nine": 9, "ten": 10, "eleven": 11,
    "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
    "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19,
    "twenty": 20, "thirty": 30, "forty": 40, "fifty": 50, "sixty": 60,
}
_SCENE_COUNT_TOKEN = (
    r"(?:\d{1,3}|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|"
    r"thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|"
    r"thirty|forty|fifty|sixty)"
)


def _parse_scene_count_token(token: str) -> int | None:
    raw = str(token or "").strip().lower().replace("-", " ")
    if raw.isdigit():
        return int(raw)
    if raw in _SCENE_COUNT_WORDS:
        return _SCENE_COUNT_WORDS[raw]
    parts = raw.split()
    if len(parts) == 2 and parts[0] in _SCENE_COUNT_WORDS and parts[1] in _SCENE_COUNT_WORDS:
        return _SCENE_COUNT_WORDS[parts[0]] + _SCENE_COUNT_WORDS[parts[1]]
    return None


def extract_scene_count_request(text: str, *, existing_count: int) -> tuple[int | None, int | None]:
    """Extract literal scene cardinality without trusting a model proposal."""

    low = str(text or "").lower()
    total: int | None = None
    additional: int | None = None
    for pattern in (
        rf"\b({_SCENE_COUNT_TOKEN})\s+scenes?\s+(?:in\s+)?total\b",
        rf"\btotal\s+(?:of\s+)?({_SCENE_COUNT_TOKEN})\s+scenes?\b",
        rf"\b({_SCENE_COUNT_TOKEN})[-\s]scene\s+short\b",
    ):
        match = re.search(pattern, low, re.IGNORECASE)
        if match:
            total = _parse_scene_count_token(match.group(1))
            break
    for pattern in (
        rf"\b(?:the\s+)?(?:other|remaining|next)\s+({_SCENE_COUNT_TOKEN})\s+scenes?\b",
        rf"\b({_SCENE_COUNT_TOKEN})\s+(?:other|more|additional|remaining|new)\s+scenes?\b",
        rf"\b(?:add|make|create|build|finish)\s+({_SCENE_COUNT_TOKEN})\s+more\s+scenes?\b",
    ):
        match = re.search(pattern, low, re.IGNORECASE)
        if match:
            additional = _parse_scene_count_token(match.group(1))
            break
    if total is not None and additional is None and total > existing_count:
        additional = total - existing_count
    if additional is not None and total is None:
        total = existing_count + additional
    return additional, total


def _assertion_scope_blocked(source: str, candidate: re.Match[str]) -> bool:
    """Detect negation/modality earlier in the same asserted clause."""

    boundary = max(
        source.rfind(".", 0, candidate.start()),
        source.rfind("!", 0, candidate.start()),
        source.rfind("?", 0, candidate.start()),
        source.rfind(";", 0, candidate.start()),
    )
    contrast = -1
    for match in _CLAUSE_CONTRAST_RE.finditer(source, 0, candidate.start()):
        contrast = max(contrast, match.end() - 1)
    prefix = source[max(boundary, contrast) + 1 : candidate.start()]
    return bool(_ASSERTION_SCOPE_BLOCK_RE.search(prefix))


def _last_unblocked_match(
    positive: re.Pattern[str],
    blockers: tuple[re.Pattern[str], ...],
    text: str,
    *,
    check_assertion_scope: bool = False,
) -> re.Match[str] | None:
    """Return the last asserted match, excluding negated/conditional spans.

    Later explicit statements win, so "I did not approve it before, but I
    approve Scene 1 now" remains usable while a positive substring embedded in
    "I don't approve Scene 1" never becomes authorization.
    """

    source = str(text or "")
    positives = list(positive.finditer(source))
    blocked = [match for pattern in blockers for match in pattern.finditer(source)]
    for candidate in reversed(positives):
        if check_assertion_scope and _assertion_scope_blocked(source, candidate):
            continue
        if any(match.start() <= candidate.start() < match.end() for match in blocked):
            continue
        if any(match.start() > candidate.start() for match in blocked):
            continue
        return candidate
    return None


def approval_authorization_evidence(
    user_text: str,
    *,
    contextual_scene_review: bool = False,
) -> str:
    """Return literal, positively asserted approval evidence or an empty string."""

    match = _last_unblocked_match(
        _APPROVAL_POSITIVE_RE,
        (_APPROVAL_NEGATED_RE, _APPROVAL_CONDITIONAL_RE),
        user_text,
        check_assertion_scope=True,
    )
    if match:
        return str(match.group(0)).strip()[:300]
    source = str(user_text or "")
    if contextual_scene_review and not _CONTEXTUAL_APPROVAL_BLOCK_RE.search(source):
        contextual = _CONTEXTUAL_APPROVAL_POSITIVE_RE.search(source)
        if contextual:
            return str(contextual.group(0)).strip()[:300]
    return ""


def execution_block_reason(user_text: str) -> str:
    """Classify language that explicitly withholds a production start."""

    source = str(user_text or "")
    if _EXECUTION_NEGATED_RE.search(source):
        return "negated"
    if _EXECUTION_DEFERRED_RE.search(source):
        return "deferred"
    if _EXECUTION_HYPOTHETICAL_RE.search(source) or _APPROVAL_CONDITIONAL_RE.search(source):
        return "hypothetical"
    return ""


def execution_authorization_evidence(user_text: str) -> str:
    """Return a literal direct production request, never a hypothetical one."""

    source = str(user_text or "")
    blockers = (
        _EXECUTION_NEGATED_RE,
        _EXECUTION_DEFERRED_RE,
        _EXECUTION_HYPOTHETICAL_RE,
        _APPROVAL_CONDITIONAL_RE,
    )
    match = _last_unblocked_match(
        _DIRECT_EXECUTION_RE,
        blockers,
        source,
        check_assertion_scope=True,
    )
    return str(match.group(0) if match else "").strip()[:300]


class ContractModel(BaseModel):
    """Base configuration shared by every command-layer value object."""

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        validate_default=True,
    )


class ModelCommandProposal(ContractModel):
    """Provider-facing wire schema.

    Every field is required on purpose.  Empty strings, zeroes, and empty lists
    represent absent values, which avoids provider-specific ``oneOf`` and null
    handling differences.
    """

    action: CommandAction
    target_source: TargetSource
    target_job_id: str = Field(max_length=48)
    additional_scene_count: int = Field(ge=0, le=10_000)
    target_total_scene_count: int = Field(ge=0, le=10_000)
    preserve_scene_numbers: list[int] = Field(max_length=200)
    animation_scope: AnimationScope
    animation_scene_numbers: list[int] = Field(max_length=200)
    duration_seconds: float = Field(ge=0, le=43_200)
    creative_direction: str = Field(max_length=2_000)
    existing_work_approved: bool
    approval_evidence: str = Field(max_length=300)
    execution_requested: bool
    execution_evidence: str = Field(max_length=300)
    clarification_question: str = Field(max_length=500)
    confidence: float = Field(ge=0.0, le=1.0)

    @field_validator("preserve_scene_numbers", "animation_scene_numbers")
    @classmethod
    def _dedupe_scene_numbers(cls, value: list[int]) -> list[int]:
        out: list[int] = []
        for raw in value:
            number = int(raw)
            if number > 0 and number not in out:
                out.append(number)
        return out


class CompilerProvenance(ContractModel):
    requested_model: str
    resolved_model: str
    provider: str
    response_id: str = ""
    transport: Literal[
        "tool_call",
        "structured_json",
        "repaired_json",
        "deterministic_fallback",
    ]
    attempts: int = Field(default=1, ge=1, le=3)


class CommandTarget(ContractModel):
    source: TargetSource
    job_id: str = Field(default="", max_length=48)


class AnimationDirective(ContractModel):
    scope: AnimationScope = "unspecified"
    scene_numbers: list[int] = Field(default_factory=list, max_length=200)

    @field_validator("scene_numbers")
    @classmethod
    def _dedupe_scene_numbers(cls, value: list[int]) -> list[int]:
        return list(dict.fromkeys(int(item) for item in value if int(item) > 0))


class AuthorizationEvidence(ContractModel):
    existing_work_approved: bool = False
    approval_quote: str = Field(default="", max_length=300)
    execution_requested: bool = False
    execution_quote: str = Field(default="", max_length=300)


class ExpandExistingShortRequest(ContractModel):
    additional_scene_count: int | None = Field(default=None, ge=1)
    target_total_scene_count: int | None = Field(default=None, ge=1)
    preserve_scene_numbers: list[int] = Field(default_factory=list, max_length=200)
    duration_seconds: float | None = Field(default=None, gt=0, le=43_200)
    creative_direction: str = Field(default="", max_length=2_000)
    animation: AnimationDirective = Field(default_factory=AnimationDirective)

    @field_validator("preserve_scene_numbers")
    @classmethod
    def _dedupe_preserved(cls, value: list[int]) -> list[int]:
        return list(dict.fromkeys(int(item) for item in value if int(item) > 0))


class StudioCommand(ContractModel):
    schema_version: Literal["studio-command-v1"] = "studio-command-v1"
    command_id: str
    turn_id: str
    action: CommandAction
    target: CommandTarget
    expand: ExpandExistingShortRequest | None = None
    authorization: AuthorizationEvidence = Field(default_factory=AuthorizationEvidence)
    clarification_question: str = Field(default="", max_length=500)
    source_text_sha256: str
    compiler: CompilerProvenance

    @model_validator(mode="after")
    def _require_expand_payload(self) -> "StudioCommand":
        if self.action == "expand_existing_short" and self.expand is None:
            raise ValueError("expand_existing_short requires an expand payload")
        if self.action != "expand_existing_short" and self.expand is not None:
            raise ValueError("expand payload is only valid for expand_existing_short")
        return self


def compiler_tool_schema() -> dict[str, Any]:
    """Return the one non-mutating function schema offered to any chat model."""

    return {
        "type": "function",
        "function": {
            "name": "emit_studio_command",
            "description": (
                "Translate the user's natural-language request and supplied Studio state "
                "into one typed command proposal. This function plans only; it never runs "
                "production or spends credits. Evidence fields must quote the user exactly."
            ),
            "parameters": ModelCommandProposal.model_json_schema(),
        },
    }


def _stable_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def make_turn_id(*, session_id: str, state_revision: str, user_text: str) -> str:
    digest = _stable_hash([session_id, state_revision, user_text])
    return f"turn_{digest[:20]}"


def normalize_proposal(
    proposal: ModelCommandProposal,
    *,
    session_id: str,
    turn_id: str,
    user_text: str,
    provenance: CompilerProvenance,
) -> StudioCommand:
    """Convert a provider proposal into the trusted canonical envelope."""

    expand: ExpandExistingShortRequest | None = None
    if proposal.action == "expand_existing_short":
        expand = ExpandExistingShortRequest(
            additional_scene_count=proposal.additional_scene_count or None,
            target_total_scene_count=proposal.target_total_scene_count or None,
            preserve_scene_numbers=proposal.preserve_scene_numbers,
            duration_seconds=proposal.duration_seconds or None,
            creative_direction=proposal.creative_direction,
            animation=AnimationDirective(
                scope=proposal.animation_scope,
                scene_numbers=proposal.animation_scene_numbers,
            ),
        )
    canonical = {
        "turn_id": turn_id,
        "action": proposal.action,
        "target": {
            "source": proposal.target_source,
            "job_id": proposal.target_job_id,
        },
        "expand": expand.model_dump(mode="json") if expand else None,
        "authorization": {
            "existing_work_approved": proposal.existing_work_approved,
            "approval_quote": proposal.approval_evidence,
            "execution_requested": proposal.execution_requested,
            "execution_quote": proposal.execution_evidence,
        },
    }
    command_id = f"cmd_{_stable_hash([session_id, canonical])[:20]}"
    return StudioCommand(
        command_id=command_id,
        turn_id=turn_id,
        action=proposal.action,
        target=CommandTarget(
            source=proposal.target_source,
            job_id=proposal.target_job_id,
        ),
        expand=expand,
        authorization=AuthorizationEvidence(
            existing_work_approved=proposal.existing_work_approved,
            approval_quote=proposal.approval_evidence,
            execution_requested=proposal.execution_requested,
            execution_quote=proposal.execution_evidence,
        ),
        clarification_question=proposal.clarification_question,
        source_text_sha256=hashlib.sha256(str(user_text or "").encode("utf-8")).hexdigest(),
        compiler=provenance,
    )


def normalized_text_contains(user_text: str, quote: str) -> bool:
    """Case/whitespace-insensitive literal evidence check."""

    def _normalize(value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip().casefold()

    wanted = _normalize(quote)
    return bool(wanted) and wanted in _normalize(user_text)
