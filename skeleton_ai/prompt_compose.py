"""Scene-first provider prompt composition.

Root failure mode (fixed here):
  Guardrail/Catalyst text was prepended and the combined string was truncated,
  so the provider never saw location/wardrobe/action — black void stills and
  identity drift.

Contract:
  1) Primary creative content (scene action, wardrobe, topic) is never truncated
     until all lower-priority text is gone.
  2) Identity locks are compact and secondary.
  3) Catalyst directives are tertiary and fill remaining budget only.
"""
from __future__ import annotations

import re
from typing import Any, Iterable


def resolve_cast_count(
    *,
    job_cast: Any = None,
    scene_cast: Any = None,
    topic: str = "",
    visual_brief: str = "",
    narration: str = "",
    scene_action: str = "",
    user_feedback: str = "",
) -> int:
    """How many skeleton hosts belong in frame.

    Default is 1. Relationship contrast beats (love-bombing, ghosting, him/her)
    intelligently default to 2 identical hosts so the short can show both parties.
    """
    for raw in (scene_cast, job_cast):
        try:
            value = int(raw)
        except Exception:
            value = 0
        if value in {1, 2}:
            return value
    blob = " ".join(
        str(part or "")
        for part in (user_feedback, topic, visual_brief, narration, scene_action)
    ).lower()
    if re.search(
        r"\b(?:two|2)\s+skeletons?\b|\bdual[- ]hosts?\b|\bcast[_ ]?count\s*[:=]\s*2\b|"
        r"\bsecond\s+skeleton\b|\bboth\s+skeletons?\b|\btwo\s+hosts?\b",
        blob,
    ):
        return 2
    if re.search(r"\b(?:one|single|only\s+one)\s+skeleton\b|\bcast[_ ]?count\s*[:=]\s*1\b", blob):
        return 1
    # Love-bomb / ghost / relationship contrast inherently needs two parties on camera.
    if re.search(
        r"\blove[\s-]*bomb|\bghost(?:ing)?\b|\bbreak[\s-]*up\b|\bdating\b|\brelationship\b",
        blob,
    ):
        return 2
    if re.search(r"\b(?:men|man|he|him|guys?)\b", blob) and re.search(
        r"\b(?:women|woman|she|her|girl)\b", blob
    ):
        return 2
    return 1


def dual_host_scene_prefix(aspect_ratio: str = "9:16") -> str:
    return f"{aspect_ratio}; two identical ivory skeletons standing apart in one room."


def dual_host_staging_brief() -> str:
    """Keep hosts separated — contact/embrace causes fused shared-glass artifacts."""
    return (
        "Left offers open empty hands; right half-step back guarded; "
        "clear air gap between torsos; each host has its own thin glass shell; "
        "never one shared bubble or fused chest glass"
    )


# Provider prompts stay short: long stacked locks/repairs cause worse artifacting.
# The reference image carries identity; the prompt only stages the scene.
MAX_VISUAL_PROMPT_CHARS = 300
DEFAULT_BUDGET = MAX_VISUAL_PROMPT_CHARS
COMPACT_IDENTITY_BUDGET = 120
CATALYST_MAX = 80

_SCENE_GUARD_MARKERS = re.compile(
    r"(?i)\b(?:HAND RULE|COMPOSITION RULE|GLASS-SHELL RULE|PROVIDER IDENTITY LOCK):"
)
_NON_PHYSICAL_VISUAL_RE = re.compile(
    r"(?i)\b(?:floating|hovering|hologram|holographic|dopamine|molecule|neuron|"
    r"neural pathway|brain graphic|brain scan|medical graphic|diagram|infographic|"
    r"callout|label|clip[- ]?art|circuit|cyber|hunter[- ]?gatherer|caveman|primal|"
    r"neon|luminous outline|colored gels?|gradient background|spotlight pool|black floor|"
    r"split[- ]?depth|foreground|middle ground|background displays|savanna|prey animals|"
    r"rocky outcrop|golden hour|herd)\b"
)
_GUARD_CLAUSE_RE = re.compile(
    r"(?i)\b(?:exactly one skeleton|exactly two hands|no third hand|no fourth hand|"
    r"no text|no watermark|single full-frame|one continuous full-frame|"
    r"no split|no diptych|no side-by-side|no comparison)\b"
)
_GENERIC_CAST_CLAUSE_RE = re.compile(
    r"(?i)^(?:"
    r"\d+\s*:\s*\d+|"
    r"two identical ivory skeletons standing apart in one room|"
    r"one (?:canonical )?(?:ivory )?skeleton host|"
    r"left offers open empty hands|"
    r"right (?:takes a )?half[- ]step back(?: guarded)?|"
    r"clear air gap between torsos|"
    r"each host has (?:its|their) own thin glass shell|"
    r"never one shared bubble or fused chest glass|"
    r"lock:|no (?:human skin|text|watermark)"
    r")\s*$"
)


def compact_skeleton_scene_direction(visual_description: str, *, max_chars: int = 180) -> str:
    """Reduce planner prose to one physical, filmable scene."""
    text = re.sub(r"\s+", " ", str(visual_description or "")).strip()
    if not text:
        return "Detailed cinematic psychology studio with visible walls and floor; relaxed presenter pose"

    marker = _SCENE_GUARD_MARKERS.search(text)
    if marker:
        text = text[: marker.start()].strip(" ,.;")
    text = re.sub(r"(?i)\bstrict\s+repair\b[:\s]*", "", text)
    text = re.sub(
        r"(?i)(?:supporting environment detail from the scene planner|scene|visual)\s*:\s*",
        "",
        text,
    )
    # Planner/canonical prefixes are useful guards but not scene direction.
    text = re.sub(
        r"(?i)\bone\s+continuous\s+(?:vertical\s+)?(?:\d+[:x]\d+\s+)?(?:frame|scene)\s+(?:with\s+)?exactly\s+(?:one|two)\s+(?:identical\s+)?(?:ivory\s+)?skeletons?\b[^.;]*[.;]?",
        "",
        text,
    )
    text = re.sub(
        r"(?i)\bexactly\s+two\s+hands?\s+attached\s+to\s+the\s+arms?\b[^.;]*[.;]?",
        "",
        text,
    )
    text = re.sub(r"\s+", " ", text).strip(" ,.;")
    # Prefer semicolon beats so commas inside a beat are not shredded into junk fragments.
    clauses = [
        re.sub(r"\s+", " ", part).strip(" ,.;:-")
        for part in re.split(r"(?<=[.;])\s+", text)
    ]
    selected: list[str] = []
    seen: set[str] = set()
    for clause in clauses:
        if len(clause) < 8 or _NON_PHYSICAL_VISUAL_RE.search(clause):
            continue
        # Cast/identity prefixes are compiled separately. Keeping them in the
        # creative tier used to consume the entire budget before the actual
        # location and story action were reached.
        if _GENERIC_CAST_CLAUSE_RE.search(clause):
            continue
        # Skip orphan fragments left after aggressive splits ("or fused chest glass").
        if re.match(r"(?i)^(?:or|and|never)\b", clause) and len(clause) < 40:
            continue
        key = clause.lower()
        if key in seen:
            continue
        seen.add(key)
        candidate = "; ".join([*selected, clause])
        if len(candidate) > max_chars:
            # A concrete location/action may be one long planner sentence.
            # Preserve its leading physical direction instead of falling back
            # to a generic studio because no complete clause fit.
            if not selected:
                selected.append(_clip(clause, max_chars))
            continue
        selected.append(clause)
        if len(selected) >= 4:
            break

    if not selected:
        selected = ["Detailed cinematic psychology studio with visible walls and floor", "relaxed presenter pose"]
    return _clip("; ".join(selected), max(120, int(max_chars)))


def _clip(text: str, max_chars: int) -> str:
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    if max_chars <= 3:
        return text[:max_chars]
    return text[: max_chars - 1].rstrip() + "…"


def compose_priority_prompt(
    *,
    primary: str | Iterable[str],
    secondary: str | Iterable[str] = (),
    tertiary: str | Iterable[str] = (),
    budget: int = DEFAULT_BUDGET,
) -> str:
    """Join priority tiers; truncate lowest priority first when over budget."""
    def _parts(val: str | Iterable[str]) -> list[str]:
        if isinstance(val, str):
            items = [val]
        else:
            items = list(val)
        out: list[str] = []
        for item in items:
            s = re.sub(r"\s+", " ", str(item or "")).strip()
            if s:
                out.append(s)
        return out

    pri = _parts(primary)
    sec = _parts(secondary)
    ter = _parts(tertiary)
    budget = max(200, int(budget or DEFAULT_BUDGET))

    primary_text = " ".join(pri).strip()
    if not primary_text:
        # Degenerate: fall back to secondary/tertiary only.
        joined = " ".join(sec + ter).strip()
        return _clip(joined, budget)

    # Never truncate primary below a hard floor unless it alone exceeds budget.
    if len(primary_text) >= budget:
        return _clip(primary_text, budget)

    remaining = budget - len(primary_text) - 1  # space
    sec_text = " ".join(sec).strip()
    ter_text = " ".join(ter).strip()

    # Reserve most leftover budget for identity locks; Catalyst is a small filler.
    # Historical bug: tertiary ate the entire remainder and providers saw only
    # guardrails after secondary was short — still true if we clip tertiary to
    # remaining alone. Cap tertiary hard (≤18% budget, ≤CATALYST_MAX).
    tertiary_cap = min(CATALYST_MAX, max(80, int(budget * 0.18)))
    secondary_cap = max(0, remaining - min(tertiary_cap, remaining // 4 if ter_text else 0))

    chunks = [primary_text]
    if sec_text and remaining > 40:
        take = min(len(sec_text), remaining if not ter_text else max(40, secondary_cap))
        piece = _clip(sec_text, take)
        if piece:
            chunks.append(piece)
            remaining = budget - len(" ".join(chunks)) - 1
    if ter_text and remaining > 40:
        take = min(len(ter_text), remaining, tertiary_cap)
        piece = _clip(ter_text, take)
        if piece:
            chunks.append(piece)
    return " ".join(chunks).strip()


def compact_identity_locks(
    *,
    include_eyes: bool = True,
    include_torso: bool = True,
    include_head: bool = True,
    include_host: bool = True,
    include_props: bool = True,
    sports_topic: bool = False,
    cast_count: int = 1,
) -> str:
    """Ultra-short identity contract; the reference image carries the details."""
    hosts = 2 if int(cast_count or 1) >= 2 else 1
    if hosts >= 2:
        return (
            "LOCK: each host thin glass skin on bones only; never curved pods behind backs; "
            "no shared bubble; eyes in skulls; empty hands; no human skin/text."
        )
    bits: list[str] = [
        "LOCK: thin glass skin on bones only (never dome/pod/capsule); no human skin/text."
    ]
    if include_eyes:
        bits.append("Eyes in skull sockets only.")
    if include_torso:
        bits.append("No chest lights/orbs.")
    if include_props and not sports_topic:
        bits.append("Empty attached hands; no sports props.")
    return " ".join(bits)


def compose_skeleton_still_prompt(
    *,
    visual_description: str,
    outfit: str = "",
    topic: str = "",
    catalyst_block: str = "",
    sports_topic: bool = False,
    aspect_ratio: str = "9:16",
    budget: int = DEFAULT_BUDGET,
    cast_count: int = 1,
) -> str:
    """Compile one short physical-scene edit contract (hard cap ~300 chars)."""
    outfit = re.sub(r"\s+", " ", str(outfit or "")).strip()
    budget_val = min(MAX_VISUAL_PROMPT_CHARS, int(budget or DEFAULT_BUDGET))
    hosts = 2 if int(cast_count or 1) >= 2 else 1
    secondary = compact_identity_locks(
        sports_topic=sports_topic,
        include_host=bool(outfit and not re.search(r"\bno clothing\b", outfit, re.I)),
        cast_count=hosts,
    )

    def _primary_bits(scene_chars: int) -> list[str]:
        raw = re.sub(r"\s+", " ", str(visual_description or "")).strip()
        raw = re.sub(r"(?i)\bstrict\s+repair\b[:\s]*", "", raw).strip(" ;.")
        visual = compact_skeleton_scene_direction(raw, max_chars=scene_chars)
        # Prefer concrete location words from the raw brief when compact drops them.
        loc = re.search(
            r"(?i)\b((?:apartment|hallway|corridor|cafe|kitchen|office|library|subway|entryway|"
            r"doorway|platform|lobby|bedroom|living room)[^.;]{0,48})",
            raw,
        )
        if loc:
            loc_bit = re.sub(r"\s+", " ", loc.group(1)).strip(" ,.;")
            if loc_bit and loc_bit.lower() not in visual.lower():
                visual = f"{loc_bit}; {visual}".strip(" ;")
                visual = visual[: max(scene_chars + 40, 120)]
        bits = [f"EDIT ref. SCENE: {visual}." if visual else "EDIT ref. SCENE: physical interior."]
        if outfit and not re.search(r"\bno clothing\b", outfit, re.I):
            bits.append(f"WARDROBE: {outfit}.")
        else:
            # Deliberately "No garments." and not "No clothes."/"nude"/"naked".
            # FAL's content checker rejected the roster prompt outright with
            # content_policy_violation / partner_validation_failed when this said
            # "No clothes." next to a full-body framing instruction - it reads as a
            # nudity request rather than an anatomical one. That killed whole paid
            # productions intermittently. The anatomy LOCK clause carries the
            # actual meaning ("thin glass skin on bones only", "no human skin"),
            # so the wardrobe bit only has to say no garments exist.
            bits.append("No garments.")
        bits.append(f"{aspect_ratio}; {'two hosts' if hosts >= 2 else 'one host'}; no text.")
        return bits

    for scene_chars in (140, 110, 90):
        primary_bits = _primary_bits(scene_chars)
        if len(" ".join(primary_bits)) + 1 + len(secondary) <= budget_val:
            return compose_priority_prompt(
                primary=primary_bits,
                secondary=secondary,
                tertiary="",
                budget=budget_val,
            )
    return compose_priority_prompt(
        primary=_primary_bits(90),
        secondary=secondary,
        tertiary="",
        budget=budget_val,
    )


def compose_skeleton_motion_prompt(
    *,
    motion: str,
    locked_outfit: str = "",
    effect_direction: str = "",
    budget: int = MAX_VISUAL_PROMPT_CHARS,
    cast_count: int = 1,
) -> str:
    """Short silent i2v prompt (hard cap ~300 chars). VO is a later FAL step."""
    hosts = 2 if int(cast_count or 1) >= 2 else 1
    motion_text = re.sub(r"\s+", " ", str(motion or "")).strip()
    motion_text = re.sub(
        r"(?i)\b(?:talk(?:ing|s)?|speak(?:ing|s)?|lip[- ]?sync|jaw|mouth|dialogue|"
        r"voiceover|voice[- ]over|narrat(?:e|es|ing|ion)|whisper(?:ing)?)[^.]*\.?",
        "",
        motion_text,
    )
    motion_text = re.sub(r"\s+", " ", motion_text).strip(" .;")
    if len(motion_text) < 24:
        motion_text = (
            "Left leans slightly then settles; right draws half-step back; keep air gap; "
            "separate glass highlights; slow push-in"
            if hosts >= 2
            else "Weight shift, quarter-turn, open-hand gesture then settle; slow push-in parallax"
        )
    else:
        motion_text = compact_skeleton_scene_direction(motion_text, max_chars=160)
    effect = re.sub(r"\s+", " ", str(effect_direction or "")).strip()
    if effect:
        effect = compact_skeleton_scene_direction(effect, max_chars=60)
    else:
        effect = "glass refraction + rim light; no chest orbs"
    primary = (
        f"SILENT: {motion_text}. VFX: {effect}. "
        f"{'Two hosts only;' if hosts >= 2 else 'One host only;'} "
        "no talking/jaw; no limb merge; keep frame-zero identity."
    )
    return compose_priority_prompt(
        primary=primary,
        secondary="",
        tertiary="",
        budget=min(MAX_VISUAL_PROMPT_CHARS, int(budget or MAX_VISUAL_PROMPT_CHARS)),
    )


def resolve_locked_outfit(
    *,
    job_locked: str = "",
    scene_outfit: str = "",
    topic: str = "",
    force_simple_host: bool = True,
) -> str:
    """Single source of truth for wardrobe across still + motion + regenerate."""
    from skeleton_ai.canonical_edit import sanitize_skeleton_outfit

    # Job-level lock wins once set.
    locked = str(job_locked or "").strip()
    if locked:
        return sanitize_skeleton_outfit(locked, topic=topic)[:240]

    outfit = sanitize_skeleton_outfit(str(scene_outfit or "").strip(), topic=topic)
    olow = outfit.lower()
    if force_simple_host and (
        not outfit
        or any(
            bad in olow
            for bad in (
                "primitive",
                "animal hide",
                "hunter",
                "tunic",
                "fur",
                "sweater",
                "knit",
                "ballet",
                "leather jacket",
                "chain necklace",
                "caveman",
                "college",
            )
        )
    ):
        return "no clothing; full clear glass shell and ivory skeleton visible; empty hands; no jewelry"
    if not outfit:
        return "no clothing; empty hands, no props"
    return outfit[:240]
