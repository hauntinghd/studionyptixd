"""Universal visual fix contract for Studio Agent (shortform + longform).

This is the exact method that recovered the dual-skeleton love-bomb Scene 1
after long Catalyst “repair” edits made the still worse.

═══════════════════════════════════════════════════════════════════════════
WHY THE OLD PATH FAILED
═══════════════════════════════════════════════════════════════════════════
Editing a broken still with a long stacked prompt (STRICT REPAIR + locks +
“exactly one skeleton” + Catalyst watchouts) diluted the scene and often
introduced fused glass / pods / void backdrops. Longer ≠ smarter.

═══════════════════════════════════════════════════════════════════════════
THE FIX METHOD (do this every time — stills AND clips, short AND long)
═══════════════════════════════════════════════════════════════════════════
1. Never edit the broken still for layout/glass/orb/fuse/pod/artifact defects.
2. Master-regenerate from the canonical reference with a SHORT prompt (≤300 chars).
3. Resolve cast intelligently (relationship / love-bomb / him+her → 2 hosts).
4. Dual hosts must stand APART with a clear air gap and SEPARATE thin glass skins
   (never one shared bubble, never curved pods behind backs, never embrace fusion).
5. Motion/i2v prompts use the same ≤300 char budget and stay SILENT (VO later).
6. If QA fails, retry with another short master seed — do not prepend repair sludge.
7. Stay in PLANNING until the creator explicitly asks to generate/make a scene.
   Planning may use market + channel compare/contrast and ask follow-ups.
"""
from __future__ import annotations

import re
from typing import Any

from skeleton_ai.prompt_compose import (
    MAX_VISUAL_PROMPT_CHARS,
    compact_skeleton_scene_direction,
    compose_skeleton_motion_prompt,
    compose_skeleton_still_prompt,
    dual_host_staging_brief,
    resolve_cast_count,
)

# Single source of truth — providers + agent tools must honor this.
PROMPT_CHAR_BUDGET = MAX_VISUAL_PROMPT_CHARS  # 300

_ARTIFACT_TERMS = (
    "artifact", "artifacts", "artifacting", "orb", "bubble", "pod", "dome",
    "capsule", "fused", "morph", "glass shell", "shared glass", "chest glow",
)

# Patterns that historically caused fused glass / pods / void / sludge.
BANNED_PROMPT_SUBSTRINGS = (
    "strict repair",
    "catalyst watchout",
    "exactly one skeleton",
    "shared bubble",
    "shared glass",
    "embrace",
    "hug",
    "kiss",
    "intertwined",
    "fused chest",
    "chest orb",
    "floating orb",
    "glass pod",
    "curved pod",
    "duplicate skeleton",
    "talking",
    "mouth moving",
    "lip sync",
)

_FAIL_VARIANT_BRIEFS = (
    "Apartment hallway at dusk; medium-wide; hosts stand apart; thin glass on bones only; empty hands",
    "City apartment living room night; medium shot; clear air gap; separate glass skins; no pods",
    "Narrow corridor window light; wide stance; left offers open hands; right half-step back; no shared bubble",
    "Doorway threshold dusk; medium-wide; two ivory hosts; empty palms; no chest glow; physical walls visible",
)


def is_visual_artifact_complaint(text: str) -> bool:
    low = str(text or "").strip().lower()
    if not low:
        return False
    return any(term in low for term in _ARTIFACT_TERMS)


def scrub_banned_prompt_text(text: str) -> str:
    """Strip known artifact-causing phrases without lengthening the prompt."""
    out = re.sub(r"\s+", " ", str(text or "")).strip()
    for banned in BANNED_PROMPT_SUBSTRINGS:
        out = re.sub(re.escape(banned), " ", out, flags=re.I)
    return re.sub(r"\s+", " ", out).strip(" ,.;")


def resolve_hosts_for_scene(
    *,
    job_cast: Any = None,
    scene_cast: Any = None,
    topic: str = "",
    visual_brief: str = "",
    narration: str = "",
    scene_action: str = "",
    user_feedback: str = "",
) -> int:
    return resolve_cast_count(
        job_cast=job_cast,
        scene_cast=scene_cast,
        topic=topic,
        visual_brief=visual_brief,
        narration=narration,
        scene_action=scene_action,
        user_feedback=user_feedback,
    )


def short_artifact_restage_brief(
    *,
    cast_count: int = 1,
    location: str = "",
    aspect_hint: str = "",
    attempt: int = 0,
) -> str:
    """Compact physical brief used when creator/QA flags artifacting."""
    loc = re.sub(r"\s+", " ", str(location or "").strip())
    if not loc:
        idx = max(0, int(attempt or 0)) % len(_FAIL_VARIANT_BRIEFS)
        if int(cast_count or 1) >= 2:
            return scrub_banned_prompt_text(_FAIL_VARIANT_BRIEFS[idx])[:280]
        loc = "Physical interior with visible walls and floor"
    aspect = re.sub(r"\s+", " ", str(aspect_hint or "").strip())
    prefix = f"{loc}; medium-wide"
    if aspect:
        prefix = f"{aspect}; {prefix}"
    if int(cast_count or 1) >= 2:
        return f"{prefix}. {dual_host_staging_brief()}"[:280]
    return scrub_banned_prompt_text(
        f"{prefix}; one ivory skeleton; thin glass skin on bones only; "
        "empty hands; no chest orb/pod/dome"
    )[:280]


def fail_variant_brief(*, cast_count: int = 1, attempt: int = 0) -> str:
    """Next short seed after a QA fail — never repair sludge."""
    return short_artifact_restage_brief(cast_count=cast_count, attempt=attempt)


def compose_short_still_prompt(
    *,
    scene_action: str,
    outfit: str = "no clothing",
    topic: str = "",
    cast_count: int = 1,
    aspect_ratio: str = "9:16",
) -> str:
    action = scrub_banned_prompt_text(compact_skeleton_scene_direction(scene_action))
    return compose_skeleton_still_prompt(
        visual_description=action,
        outfit=outfit,
        topic=topic,
        cast_count=cast_count,
        aspect_ratio=aspect_ratio,
        budget=PROMPT_CHAR_BUDGET,
    )


def compose_short_motion_prompt(
    *,
    scene_action: str,
    outfit: str = "no clothing",
    cast_count: int = 1,
    effect_direction: str = "",
) -> str:
    effect = scrub_banned_prompt_text(
        effect_direction or "thin glass skin refraction only; no pods; no chest orbs"
    )
    action = scrub_banned_prompt_text(compact_skeleton_scene_direction(scene_action, max_chars=120))
    return compose_skeleton_motion_prompt(
        motion=action,
        locked_outfit=outfit,
        effect_direction=effect,
        cast_count=cast_count,
        budget=PROMPT_CHAR_BUDGET,
    )


def artifact_fix_plan(
    *,
    topic: str = "",
    visual_brief: str = "",
    narration: str = "",
    scene_action: str = "",
    user_feedback: str = "",
    job_cast: Any = None,
    scene_cast: Any = None,
    outfit: str = "no clothing",
    aspect_ratio: str = "9:16",
    location: str = "",
    attempt: int = 0,
) -> dict[str, Any]:
    """Return the exact regenerate plan Studio Agent must execute."""
    hosts = resolve_hosts_for_scene(
        job_cast=job_cast,
        scene_cast=scene_cast,
        topic=topic,
        visual_brief=visual_brief,
        narration=narration,
        scene_action=scene_action,
        user_feedback=user_feedback,
    )
    action = short_artifact_restage_brief(
        cast_count=hosts,
        location=location,
        aspect_hint=aspect_ratio,
        attempt=attempt,
    )
    still_prompt = compose_short_still_prompt(
        scene_action=action,
        outfit=outfit,
        topic=topic,
        cast_count=hosts,
        aspect_ratio=aspect_ratio,
    )
    motion_prompt = compose_short_motion_prompt(
        scene_action=action,
        outfit=outfit,
        cast_count=hosts,
    )
    return {
        "method": "master_regenerate",  # never edit_broken_still
        "cast_count": hosts,
        "scene_action": action,
        "still_prompt": still_prompt,
        "motion_prompt": motion_prompt,
        "prompt_budget": PROMPT_CHAR_BUDGET,
        "still_prompt_len": len(still_prompt),
        "motion_prompt_len": len(motion_prompt),
        "attempt": int(attempt or 0),
        "rules": [
            "master_regenerate_only",
            "prompt_max_300_chars",
            "no_strict_repair_prepend",
            "dual_hosts_separate_glass_air_gap" if hosts >= 2 else "single_host_thin_glass",
            "silent_i2v_voiceover_later",
            "fail_uses_variant_seed",
        ],
    }


def parse_animate_policy(text: str, *, default: str | None = "heroes") -> str | None:
    """heroes = Fast default (opening/turn/ending + hero beats). all | none also allowed.

    Pass default=None to detect an explicit override only (no implicit fallback).
    """
    low = str(text or "").strip().lower()
    if re.search(r"\banimate\s+all\b|\bevery\s+scene\s+(?:moving|animated|i2v)\b|\bfull\s+motion\b|\banimate\s+them\b|\band\s+animate\b", low):
        return "all"
    if re.search(r"\bno\s+animat|\bken\s*burns\s+only\b|\bstills?\s+only\b|\banimate\s+none\b", low):
        return "none"
    if re.search(r"\bheroes?\s+only\b|\bkey\s+beats?\b|\bselective\s+animat", low):
        return "heroes"
    if default is None:
        return None
    return default if default in {"heroes", "all", "none"} else "heroes"


def _is_hero_beat(scene: dict[str, Any], *, index: int, total: int) -> bool:
    treatment = scene.get("visual_treatment") if isinstance(scene.get("visual_treatment"), dict) else {}
    if str(treatment.get("role") or "").lower() == "hero":
        return True
    if index in {0, max(0, total - 1), max(0, total // 2)}:
        return True
    narration = str(scene.get("narration") or "").lower()
    return any(
        needle in narration
        for needle in ("but then", "the truth", "everything changed", "finally", "revealed", "disappear")
    )


def harden_planned_scenes_for_expand(
    scenes: list[dict[str, Any]],
    *,
    topic: str = "",
    visual_brief: str = "",
    outfit: str = "no clothing",
    job_cast: Any = None,
    animate_policy: str = "heroes",
    aspect_ratio: str = "9:16",
) -> list[dict[str, Any]]:
    """Rewrite expand scenes onto known-good ≤300 prompts; set animate flags by policy.

    Scene 0 (approved proof) is left intact except cast/prompt scrub if missing clip.
    """
    policy = animate_policy if animate_policy in {"heroes", "all", "none"} else "heroes"
    total = max(1, len(scenes))
    hosts_job = resolve_hosts_for_scene(
        job_cast=job_cast,
        topic=topic,
        visual_brief=visual_brief,
    )
    out: list[dict[str, Any]] = []
    for scene in scenes:
        sc = dict(scene) if isinstance(scene, dict) else {}
        idx = int(sc.get("index", 0) or 0)
        # Preserve the approved Scene 1 row exactly. Expansion never reanimates
        # a preserved index, so rewriting even its prompt/cast metadata would
        # violate the creator's lock without adding any safety benefit.
        if idx == 0 and (sc.get("clip_rel") or sc.get("still_rel")):
            out.append(sc)
            continue
        hosts = resolve_hosts_for_scene(
            job_cast=job_cast,
            scene_cast=sc.get("cast_count"),
            topic=topic,
            visual_brief=visual_brief,
            narration=str(sc.get("narration") or ""),
            scene_action=str(sc.get("scene_action") or sc.get("prompt") or ""),
        ) or hosts_job
        sc["cast_count"] = hosts

        action_raw = str(sc.get("scene_action") or sc.get("prompt") or sc.get("narration") or "").strip()
        action = scrub_banned_prompt_text(compact_skeleton_scene_direction(action_raw))
        if hosts >= 2 and "air gap" not in action.lower():
            # Append trusted staging verbatim — do not scrub (bans would strip "shared bubble" from the lock).
            action = f"{action}; {dual_host_staging_brief()}"[:280]
        still_prompt = compose_short_still_prompt(
            scene_action=action,
            outfit=str(sc.get("outfit") or outfit or "no clothing"),
            topic=topic,
            cast_count=hosts,
            aspect_ratio=aspect_ratio,
        )
        effect = ""
        treatment = sc.get("visual_treatment") if isinstance(sc.get("visual_treatment"), dict) else {}
        if treatment.get("motion_effect"):
            effect = str(treatment.get("motion_effect"))
        motion_prompt = compose_short_motion_prompt(
            scene_action=action,
            outfit=str(sc.get("outfit") or outfit or "no clothing"),
            cast_count=hosts,
            effect_direction=effect,
        )
        sc["scene_action"] = action
        sc["prompt"] = still_prompt
        sc["motion_prompt"] = motion_prompt

        qa = sc.get("still_qa") if isinstance(sc.get("still_qa"), dict) else {}
        still_passed = not qa or (qa.get("status") == "pass" and qa.get("pass") is True)
        if not still_passed:
            sc["approved_for_video"] = False
            sc["approved_for_animation"] = False
            sc["animate"] = False
            out.append(sc)
            continue

        want_animate = False
        if policy == "all":
            want_animate = True
        elif policy == "heroes":
            want_animate = _is_hero_beat(sc, index=idx, total=total)

        sc["approved_for_video"] = True
        sc["approved_for_animation"] = bool(want_animate)
        sc["animate"] = bool(want_animate) and not sc.get("clip_rel")
        out.append(sc)
    return out


def expand_confirm_summary(
    *,
    duration_seconds: float | int,
    scene_count: int,
    animate_policy: str,
    creative_direction: str = "",
) -> str:
    policy = animate_policy if animate_policy in {"heroes", "all", "none"} else "heroes"
    policy_label = {
        "heroes": "hero beats only (Ken Burns elsewhere)",
        "all": "all passing scenes (i2v)",
        "none": "Ken Burns only (no i2v)",
    }[policy]
    direction = re.sub(r"\s+", " ", str(creative_direction or "").strip())[:180]
    vfx = f" Direction: {direction}." if direction else ""
    return (
        f"Confirm Fast expand: keep Scene 1, build about {int(scene_count)} scenes "
        f"for ~{int(duration_seconds)}s. Animate: {policy_label}.{vfx} "
        "Reply yes to start, or say animate all / ken burns only to change motion."
    )


AGENT_SYSTEM_CONTRACT = """
═══ VISUAL FIX CONTRACT (shortform + longform, stills + clips) ═══
When the creator reports artifacting / fused glass / orbs / pods / morphing:
1. Do NOT edit the broken still with a long repair prompt.
2. Call regenerate with a SHORT master prompt (≤300 characters) from the visual_fix_contract.
3. Relationship / love-bomb / him+her topics default to TWO skeleton hosts standing APART
   with separate thin glass skins and a clear air gap — never one shared bubble.
4. Motion/i2v stays ≤300 chars and SILENT (FAL voiceover later).
5. Retry with another short seed if QA fails — never stack STRICT REPAIR sludge.

═══ PLANNING vs PRODUCTION ═══
Stay in PLANNING (market + channel compare/contrast, follow-up questions, concept) until the
creator explicitly commits, e.g. "generate scene 1", "make the first scene", "render scene one",
"go ahead and make the short now". Soft "let's make…" / strategy / research stays planning.

═══ FAST EXPAND ═══
After Scene 1 sign-off: ask duration → creative → CONFIRM restatement → then expand.
Default animate policy is heroes (not weak batch-animate-all). Known-good ≤300 prompts only.
Production tools run via the runner — do not improvise long repair prompts.
""".strip()
