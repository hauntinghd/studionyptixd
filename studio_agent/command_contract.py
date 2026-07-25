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
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


CommandAction = Literal[
    "conversation",
    "clarify",
    "expand_existing_short",
    "audit_and_repair_scenes",
]
ProductionCommandAction = Literal[
    "generate_longform_outline",
    "expand_longform_chapter",
    "start_short",
    "start_longform",
    "start_product_ad",
    "expand_existing_short",
    "expand_longform",
    "audit_and_repair_scenes",
    "approve_scenes",
    "animate_scenes",
    "ship_existing_short",
    "finalize",
    "cancel",
    "generate_thumbnail",
    "start_cliplab",
    "analyze_cliplab",
    "render_cliplab",
    "analyze_reference",
    "retry_reference_analysis",
]
ProductionKind = Literal[
    "shortform",
    "longform",
    "product_ad",
    "cliplab",
    "reference_analysis",
]
TargetSource = Literal["none", "reply_to", "active_job", "explicit_job_id", "recent_job"]
AnimationScope = Literal[
    "unspecified",
    "none",
    "new_scenes",
    "all_scenes",
    "heroes",
    "explicit",
]
RepairScope = Literal[
    "general_scene_quality",
    "narrative_alignment",
    "visual_quality",
    "animation_quality",
    "full_quality",
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

_SCENE_REPAIR_SUBJECT_RE = re.compile(
    r"\b(?:scenes?|stills?|frames?|shots?|visuals?|images?|clips?|animations?|"
    r"prompts?|scripts?|narrations?|story\s+beats?|settings?|backgrounds?|"
    r"those|these|they)\b",
    re.IGNORECASE,
)
_SCENE_REPAIR_ACTION_RE = re.compile(
    r"\b(?:(?:can|could|would|will)\s+you\s+|please\s+)?"
    r"(?:fix|repair|correct|redo|rebuild|regenerate|rerender|re-render|reanimate|"
    r"re-animate|revise|restage|re-stage)\b",
    re.IGNORECASE,
)
_SCENE_REPAIR_DEFECT_RE = re.compile(
    r"\b(?:do(?:es)?\s+not|don'?t|isn'?t|aren'?t|not)\s+(?:perfectly\s+|properly\s+|really\s+)?"
    r"(?:adhere|align|match|follow|correspond|represent|depict|reflect|fit)\b"
    r"|\b(?:fails?|failed|failing)\s+to\s+(?:adhere|align|match|follow|correspond|represent|depict|reflect)\b"
    r"|\b(?:do(?:es)?\s+not|don'?t|isn'?t|aren'?t|not)\s+(?:perfectly\s+|properly\s+|really\s+)?"
    r"(?:do|doing|show|showing|tell|telling|portray|portraying|stage|staging|look|looking)\b"
    r"|\b(?:isn'?t|aren'?t|not)\s+(?:quite\s+)?(?:right|correct|accurate|"
    r"what\s+(?:the\s+)?(?:prompt|script|narration)\s+says)\b"
    r"|\b(?:issues?|problems?|something\s+(?:is\s+)?wrong)\b"
    r"|\b(?:wrong|incorrect|inaccurate|unrelated|off[- ]script|mismatched?|misaligned|"
    r"generic|bland|repetitive|duplicated?|artifact(?:ed|ing|s)?|warped?|morph(?:ed|ing)?|"
    r"flicker(?:ed|ing)?|drift(?:ed|ing)?|broken)\b"
    r"|\b(?:should|needs?\s+to)\s+(?:adhere|align|match|follow|correspond|represent|depict|reflect)\b",
    re.IGNORECASE,
)
_SCENE_REPAIR_NEGATED_ACTION_RE = re.compile(
    r"\b(?:do\s+not|don'?t|never|stop)\s+(?:please\s+)?"
    r"(?:fix|repair|correct|redo|rebuild|regenerate|rerender|re-render|reanimate|"
    r"re-animate|change|update|revise|restage|re-stage|touch|modify)\b"
    r"|\b(?:do\s+not|don'?t)\s+(?:do|make)\s+anything\s+(?:to|with)\b"
    r"|\b(?:leave|keep)\s+(?:the\s+)?(?:scenes?|stills?|clips?)\s+(?:alone|unchanged|as\s+is)\b",
    re.IGNORECASE,
)
_SCENE_REPAIR_QUESTION_RE = re.compile(
    r"^\s*(?:what\s+(?:if|would\s+happen)|suppose|hypothetically|"
    r"how\s+(?:would|could|should)|(?:will|would|could|can)\s+(?:it|studio|the\s+agent)|"
    r"why\s+(?:do|does|did|are|is)|are\s+(?:the\s+)?scenes?|"
    r"do\s+(?:the\s+)?scenes?|does\s+(?:scene|the\s+scene)|is\s+(?:scene|the\s+scene))\b",
    re.IGNORECASE,
)
_CONTEXTUAL_CONFIRMATION_RE = re.compile(
    r"^\s*(?:yes|yeah|yep|yup|sure|okay|ok|correct|exactly|please\s+do|do\s+it|"
    r"go\s+ahead|fix\s+them|repair\s+them)\b",
    re.IGNORECASE,
)
_CONTEXTUAL_CANCELLATION_RE = re.compile(
    r"^\s*(?:no|nope|nah|not\s+now|never\s+mind|nevermind|cancel(?:\s+that)?|"
    r"stop|leave\s+them|don'?t\s+(?:do|fix|repair|change)\s+(?:it|them|that))\b",
    re.IGNORECASE,
)
_CONTEXTUAL_SCENE_SCOPE_RE = re.compile(
    r"\b(?:all\s+(?:of\s+)?(?:them|those|these)|(?:scenes?|shots?|clips?)\s+\d|"
    r"\d{1,3}\s*(?:-|through|thru|to|,|&|and)\s*\d{1,3})\b",
    re.IGNORECASE,
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


def _scene_numbers_as_digits(text: str) -> str:
    source = str(text or "").lower().replace("–", "-").replace("—", "-")
    for word, value in sorted(_SCENE_COUNT_WORDS.items(), key=lambda item: -len(item[0])):
        source = re.sub(rf"\b{re.escape(word)}\b", str(value), source)
    return source


def extract_scene_numbers_request(
    text: str,
    *,
    total_scenes: int,
    allow_bare: bool = False,
) -> list[int]:
    """Resolve explicit human-facing scene selectors to a bounded 1-based set."""

    total = max(0, min(60, int(total_scenes or 0)))
    if total <= 0:
        return []
    low = _scene_numbers_as_digits(text)
    selected: set[int] = set()
    all_selected = bool(
        re.search(
            r"\b(?:all|every|each)\s+(?:of\s+the\s+)?scenes?\b|"
            r"\bscenes?\s+(?:all|every|each)\b",
            low,
        )
    )
    if allow_bare and re.search(r"\ball\s+(?:of\s+)?(?:them|those|these)\b", low):
        all_selected = True
    if all_selected:
        selected.update(range(1, total + 1))

    range_prefix = (
        r"(?:(?:scenes?|shots?|clips?|those|these)\s+)?"
        if allow_bare
        else r"(?:scenes?|shots?|clips?|those|these)\s+"
    )
    for match in re.finditer(
        rf"\b{range_prefix}(\d{{1,3}})\s*(?:-|through|thru|to)\s*(\d{{1,3}})\b",
        low,
    ):
        left, right = int(match.group(1)), int(match.group(2))
        start, end = sorted((left, right))
        selected.update(number for number in range(start, end + 1) if 1 <= number <= total)

    list_prefix = (
        r"(?:(?:scenes?|shots?|clips?|those|these)\s+)?"
        if allow_bare
        else r"(?:scenes?|shots?|clips?|those|these)\s+"
    )
    # Accept ordinary lists with or without an Oxford comma.  The old pattern
    # stopped before ``and 6`` in ``2, 3, 4, 5, and 6`` because it consumed the
    # comma as the separator and then expected a digit immediately.
    list_separator = r"(?:,\s*(?:and\s+)?|&\s*|and\s+)"
    for match in re.finditer(
        rf"\b{list_prefix}((?:\d{{1,3}}\s*{list_separator})+\d{{1,3}})\b",
        low,
    ):
        selected.update(
            number
            for number in (int(value) for value in re.findall(r"\d{1,3}", match.group(1)))
            if 1 <= number <= total
        )

    # A single selected scene is commonly dictated as either "Scene 1" or
    # "Scenes 1".  The latter used to fall through to the scope clarification
    # loop even though its target is fully explicit.
    for match in re.finditer(r"\bscenes?\s+(\d{1,3})\b", low):
        number = int(match.group(1))
        if 1 <= number <= total:
            selected.add(number)
    if allow_bare and not selected:
        bare = re.fullmatch(r"\s*(\d{1,3})\s*[.!?]?\s*", low)
        if bare and 1 <= int(bare.group(1)) <= total:
            selected.add(int(bare.group(1)))

    excluded: set[int] = set()
    for match in re.finditer(
        r"\b(?:excluding|exclude|except|other\s+than|leave\s+out|skip)\s+"
        r"(?:for\s+)?(?:scene\s+)?(\d{1,3})\b",
        low,
    ):
        number = int(match.group(1))
        if 1 <= number <= total:
            excluded.add(number)
    return sorted(selected - excluded)


def scene_repair_candidate(text: str) -> bool:
    """Broad semantic gate only; validation still owns permission and scope."""

    source = str(text or "")
    return bool(
        _SCENE_REPAIR_SUBJECT_RE.search(source)
        and (_SCENE_REPAIR_ACTION_RE.search(source) or _SCENE_REPAIR_DEFECT_RE.search(source))
    )


def scene_repair_block_reason(text: str) -> str:
    """Return why a possible repair turn must not mutate production."""

    source = str(text or "")
    if _SCENE_REPAIR_NEGATED_ACTION_RE.search(source):
        return "negated"
    explicit_request = re.search(
        r"^\s*(?:(?:can|could|would|will)\s+you\s+|please\s+)",
        source,
        re.IGNORECASE,
    )
    if _SCENE_REPAIR_QUESTION_RE.search(source) and not explicit_request:
        return "hypothetical"
    return ""


def scene_repair_authorization_evidence(text: str) -> str:
    """Return literal repair authorization, including direct review-stage defect reports."""

    source = str(text or "").strip()
    if not source or scene_repair_block_reason(source):
        return ""
    action = _SCENE_REPAIR_ACTION_RE.search(source)
    if action:
        return str(action.group(0)).strip()[:300]
    if _SCENE_REPAIR_SUBJECT_RE.search(source) and _SCENE_REPAIR_DEFECT_RE.search(source):
        return source[:300]
    return ""


def contextual_confirmation_evidence(text: str) -> str:
    match = _CONTEXTUAL_CONFIRMATION_RE.search(str(text or ""))
    return str(match.group(0) if match else "").strip()[:300]


def scene_repair_cancellation_evidence(text: str) -> str:
    match = _CONTEXTUAL_CANCELLATION_RE.search(str(text or ""))
    return str(match.group(0) if match else "").strip()[:300]


def scene_repair_followup_candidate(text: str) -> bool:
    source = str(text or "")
    return bool(
        scene_repair_candidate(source)
        or contextual_confirmation_evidence(source)
        or scene_repair_cancellation_evidence(source)
        or _CONTEXTUAL_SCENE_SCOPE_RE.search(source)
    )


def infer_scene_repair_scope(text: str) -> RepairScope:
    low = str(text or "").lower()
    narrative = bool(re.search(
        r"\b(?:prompt|script|narration|story|beat|adhere|align|match|follow|correspond|"
        r"represent|depict|reflect|off[- ]script|mismatch)\w*\b",
        low,
    ))
    animation = bool(re.search(
        r"\b(?:animation|animated|clip|motion|movement|i2v|flicker|morph|warping|drift)\w*\b",
        low,
    ))
    visual = bool(re.search(
        r"\b(?:still|frame|image|visual|background|setting|room|character|skeleton|artifact|"
        r"lighting|pose|prop|composition)\w*\b",
        low,
    ))
    if sum((narrative, animation, visual)) > 1:
        return "full_quality"
    if narrative:
        return "narrative_alignment"
    if animation:
        return "animation_quality"
    if visual:
        return "visual_quality"
    return "general_scene_quality"


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
    repair_scene_numbers: list[int] = Field(max_length=200)
    repair_scope: RepairScope
    repair_instruction: str = Field(max_length=2_000)
    duration_seconds: float = Field(ge=0, le=43_200)
    creative_direction: str = Field(max_length=2_000)
    existing_work_approved: bool
    approval_evidence: str = Field(max_length=300)
    execution_requested: bool
    execution_evidence: str = Field(max_length=300)
    clarification_question: str = Field(max_length=500)
    confidence: float = Field(ge=0.0, le=1.0)

    @field_validator(
        "preserve_scene_numbers",
        "animation_scene_numbers",
        "repair_scene_numbers",
    )
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


class SceneRepairRequest(ContractModel):
    scene_numbers: list[int] = Field(default_factory=list, max_length=60)
    scope: RepairScope = "general_scene_quality"
    instruction: str = Field(default="", max_length=2_000)

    @field_validator("scene_numbers")
    @classmethod
    def _dedupe_scene_numbers(cls, value: list[int]) -> list[int]:
        return list(dict.fromkeys(int(item) for item in value if int(item) > 0))


class StudioCommand(ContractModel):
    schema_version: Literal["studio-command-v1"] = "studio-command-v1"
    command_id: str
    turn_id: str
    action: CommandAction
    target: CommandTarget
    expand: ExpandExistingShortRequest | None = None
    repair: SceneRepairRequest | None = None
    authorization: AuthorizationEvidence = Field(default_factory=AuthorizationEvidence)
    clarification_question: str = Field(default="", max_length=500)
    source_text_sha256: str
    compiler: CompilerProvenance

    @model_validator(mode="after")
    def _require_action_payload(self) -> "StudioCommand":
        if self.action == "expand_existing_short" and self.expand is None:
            raise ValueError("expand_existing_short requires an expand payload")
        if self.action != "expand_existing_short" and self.expand is not None:
            raise ValueError("expand payload is only valid for expand_existing_short")
        if self.action == "audit_and_repair_scenes" and self.repair is None:
            raise ValueError("audit_and_repair_scenes requires a repair payload")
        if self.action != "audit_and_repair_scenes" and self.repair is not None:
            raise ValueError("repair payload is only valid for audit_and_repair_scenes")
        return self


class ProductionCommandTargetV2(ContractModel):
    """Backend-resolved target and ownership binding for a production mutation."""

    source: TargetSource
    job_id: str = Field(default="", max_length=48)
    kind: ProductionKind | None = None
    owner_session_id: str = Field(default="", max_length=128)
    owner_user_id: str = Field(default="", max_length=256)
    expected_job_revision: str = Field(default="", max_length=128)


class ProductionMediaRouteV2(ContractModel):
    """Immutable media route selected before a command is made executable."""

    revision: int = Field(ge=0)
    image_model: str = Field(default="", max_length=128)
    video_model: str = Field(default="", max_length=128)
    speech_model: str = Field(default="", max_length=128)
    route_sha256: str = Field(default="", max_length=64)


class ProductionAuthorizationV2(ContractModel):
    """Literal authorization evidence attached by the trusted compiler."""

    execution_requested: bool
    execution_quote: str = Field(default="", max_length=300)
    confirmation_required: bool = False
    confirmed: bool = False
    confirmation_id: str = Field(default="", max_length=128)

    @model_validator(mode="after")
    def _require_evidence_for_execution(self) -> "ProductionAuthorizationV2":
        if self.execution_requested and not self.execution_quote:
            raise ValueError("execution_requested requires an execution_quote")
        if self.confirmed and not self.confirmation_id:
            raise ValueError("confirmed authorization requires a confirmation_id")
        return self


class ProductionPostconditionV2(ContractModel):
    """Observable condition that must pass before completion may be reported."""

    kind: Literal[
        "job_created",
        "job_updated",
        "analysis_ready",
        "reference_analysis_ready",
        "outline_ready",
        "chapter_ready",
        "scene_qa_pass",
        "scenes_approved",
        "clips_ready",
        "artifact_ready",
        "job_cancelled",
        "thumbnail_ready",
    ]
    scene_numbers: list[int] = Field(default_factory=list, max_length=60)
    artifact_type: Literal["", "mp4", "thumbnail"] = ""
    required: bool = True

    @field_validator("scene_numbers")
    @classmethod
    def _dedupe_scene_numbers(cls, value: list[int]) -> list[int]:
        return list(dict.fromkeys(int(item) for item in value if int(item) > 0))


class StartShortOperation(ContractModel):
    action: Literal["start_short"] = "start_short"
    brief: str = Field(default="", max_length=8_000)
    scene_count: int | None = Field(default=None, ge=1, le=60)
    duration_seconds: float | None = Field(default=None, gt=0, le=3_600)


class StartLongformOperation(ContractModel):
    action: Literal["start_longform"] = "start_longform"
    brief: str = Field(default="", max_length=16_000)
    target_duration_seconds: float | None = Field(default=None, gt=0, le=43_200)


class GenerateLongformOutlineOperation(ContractModel):
    action: Literal["generate_longform_outline"] = "generate_longform_outline"
    topic: str = Field(default="", max_length=16_000)
    channel_key: str = Field(default="", max_length=256)
    target_minutes: int | None = Field(default=None, ge=1, le=720)


class ExpandLongformChapterOperation(ContractModel):
    action: Literal["expand_longform_chapter"] = "expand_longform_chapter"
    outline_title: str = Field(default="", max_length=4_000)
    chapter_index: int = Field(default=0, ge=0, le=1_000)


class StartProductAdOperation(ContractModel):
    action: Literal["start_product_ad"] = "start_product_ad"
    brief: str = Field(default="", max_length=8_000)
    product_name: str = Field(default="", max_length=500)
    duration_seconds: float | None = Field(default=None, gt=0, le=3_600)


class ExpandExistingShortOperation(ContractModel):
    action: Literal["expand_existing_short"] = "expand_existing_short"
    request: ExpandExistingShortRequest


class ExpandLongformOperation(ContractModel):
    action: Literal["expand_longform"] = "expand_longform"
    instruction: str = Field(default="", max_length=8_000)


class AuditAndRepairScenesOperation(ContractModel):
    action: Literal["audit_and_repair_scenes"] = "audit_and_repair_scenes"
    request: SceneRepairRequest


class ApproveScenesOperation(ContractModel):
    action: Literal["approve_scenes"] = "approve_scenes"
    scene_numbers: list[int] = Field(min_length=1, max_length=60)

    @field_validator("scene_numbers")
    @classmethod
    def _dedupe_scene_numbers(cls, value: list[int]) -> list[int]:
        return list(dict.fromkeys(int(item) for item in value if int(item) > 0))

    @model_validator(mode="after")
    def _require_scenes(self) -> "ApproveScenesOperation":
        if not self.scene_numbers:
            raise ValueError("approve_scenes requires at least one positive scene number")
        return self


class AnimateScenesOperation(ContractModel):
    action: Literal["animate_scenes"] = "animate_scenes"
    scene_numbers: list[int] = Field(min_length=1, max_length=60)
    only_missing: bool = True

    @field_validator("scene_numbers")
    @classmethod
    def _dedupe_scene_numbers(cls, value: list[int]) -> list[int]:
        return list(dict.fromkeys(int(item) for item in value if int(item) > 0))

    @model_validator(mode="after")
    def _require_scenes(self) -> "AnimateScenesOperation":
        if not self.scene_numbers:
            raise ValueError("animate_scenes requires at least one positive scene number")
        return self


class ShipExistingShortOperation(ContractModel):
    action: Literal["ship_existing_short"] = "ship_existing_short"
    scene_numbers: list[int] = Field(default_factory=list, max_length=60)
    preserve_passing_assets: bool = True
    repair_failed_scenes: bool = True
    animate_only_missing: bool = True
    output_format: Literal["mp4"] = "mp4"

    @field_validator("scene_numbers")
    @classmethod
    def _dedupe_scene_numbers(cls, value: list[int]) -> list[int]:
        return list(dict.fromkeys(int(item) for item in value if int(item) > 0))


class FinalizeOperation(ContractModel):
    action: Literal["finalize"] = "finalize"
    output_format: Literal["mp4"] = "mp4"


class CancelOperation(ContractModel):
    action: Literal["cancel"] = "cancel"
    reason: str = Field(default="", max_length=1_000)


class GenerateThumbnailOperation(ContractModel):
    action: Literal["generate_thumbnail"] = "generate_thumbnail"
    prompt: str = Field(default="", max_length=4_000)
    scene_number: int | None = Field(default=None, ge=1, le=60)


class StartClipLabOperation(ContractModel):
    action: Literal["start_cliplab"] = "start_cliplab"
    brief: str = Field(default="", max_length=8_000)


class AnalyzeClipLabOperation(ContractModel):
    action: Literal["analyze_cliplab"] = "analyze_cliplab"
    prompt: str = Field(default="", max_length=16_000)
    max_segments: int = Field(default=12, ge=1, le=40)


class RenderClipLabOperation(ContractModel):
    action: Literal["render_cliplab"] = "render_cliplab"
    instruction: str = Field(default="", max_length=8_000)


class AnalyzeReferenceOperation(ContractModel):
    action: Literal["analyze_reference"] = "analyze_reference"
    source: Literal["upload", "url"]
    content_format: Literal["short", "long", "both"] = "short"


class RetryReferenceAnalysisOperation(ContractModel):
    action: Literal["retry_reference_analysis"] = "retry_reference_analysis"
    stages: list[str] = Field(default_factory=list, max_length=12)


ProductionOperationV2 = Annotated[
    GenerateLongformOutlineOperation
    | ExpandLongformChapterOperation
    | StartShortOperation
    | StartLongformOperation
    | StartProductAdOperation
    | ExpandExistingShortOperation
    | ExpandLongformOperation
    | AuditAndRepairScenesOperation
    | ApproveScenesOperation
    | AnimateScenesOperation
    | ShipExistingShortOperation
    | FinalizeOperation
    | CancelOperation
    | GenerateThumbnailOperation
    | StartClipLabOperation
    | AnalyzeClipLabOperation
    | RenderClipLabOperation
    | AnalyzeReferenceOperation
    | RetryReferenceAnalysisOperation,
    Field(discriminator="action"),
]


class ProductionCommandEnvelopeV2(ContractModel):
    """Canonical backend-owned envelope for every production mutation.

    The browser and language model may propose intent, but only trusted backend
    code may populate identity, target ownership, revisions, route locks, and
    idempotency fields in this envelope.
    """

    schema_version: Literal["production-command-v2"] = "production-command-v2"
    command_id: str = Field(min_length=1, max_length=128)
    turn_id: str = Field(min_length=1, max_length=128)
    session_id: str = Field(min_length=1, max_length=128)
    user_id: str = Field(min_length=1, max_length=256)
    state_revision: str = Field(min_length=1, max_length=128)
    action: ProductionCommandAction
    target: ProductionCommandTargetV2
    operation: ProductionOperationV2
    authorization: ProductionAuthorizationV2
    media_route: ProductionMediaRouteV2 | None = None
    expected_postconditions: list[ProductionPostconditionV2] = Field(
        default_factory=list,
        max_length=100,
    )
    idempotency_key: str = Field(min_length=1, max_length=128)
    source_text_sha256: str = Field(default="", max_length=64)
    created_at: float = Field(default=0.0, ge=0)

    @model_validator(mode="after")
    def _validate_backend_bindings(self) -> "ProductionCommandEnvelopeV2":
        if self.action != self.operation.action:
            raise ValueError("envelope action must match operation action")
        start_actions = {
            "start_short",
            "start_longform",
            "start_product_ad",
            "start_cliplab",
        }
        targetless_actions = {
            "generate_longform_outline",
            "expand_longform_chapter",
            "analyze_reference",
        }
        creates_new_target = self.action in start_actions | targetless_actions or (
            self.action == "generate_thumbnail" and not self.target.job_id
        )
        if creates_new_target:
            if self.target.job_id:
                raise ValueError("start commands cannot target an existing job")
        else:
            if not self.target.job_id:
                raise ValueError(f"{self.action} requires an exact target job_id")
            if self.target.owner_session_id != self.session_id:
                raise ValueError("target owner_session_id must match the command session_id")
            if self.target.owner_user_id != self.user_id:
                raise ValueError("target owner_user_id must match the command user_id")
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
    repair: SceneRepairRequest | None = None
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
    if proposal.action == "audit_and_repair_scenes":
        repair = SceneRepairRequest(
            scene_numbers=proposal.repair_scene_numbers,
            scope=proposal.repair_scope,
            instruction=proposal.repair_instruction,
        )
    canonical = {
        "turn_id": turn_id,
        "action": proposal.action,
        "target": {
            "source": proposal.target_source,
            "job_id": proposal.target_job_id,
        },
        "expand": expand.model_dump(mode="json") if expand else None,
        "repair": repair.model_dump(mode="json") if repair else None,
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
        repair=repair,
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
