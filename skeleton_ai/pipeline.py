"""
Skeleton AI end-to-end orchestrator.

Inputs:
  - category_key (one of: human_limits, marvel_vs_dc, ancient_history, futuristic_socrates)
  - topic_text (optional — direct Anthropic picks if absent)
  - tier ("standard" → Seedance 2.0, "premium" → Kling 2.1 Pro)
  - voice_id (FAL MiniMax voice id; defaults to Trustworthy Man)
  - workspace_dir (where clips/stills/output land)

Pipeline steps:
  1. Direct Anthropic writes the 60s script (~12 beats).
  2. Per beat: derive outfit + scene_action via a bounded Anthropic call.
  3. Seedream v4.5 *edit* per beat from canonical-skeleton-master.png
     (same skeleton every scene; only background/props/wardrobe change).
  4. Seedance 2.0 (or Kling Pro) i2v on each still.
  5. FAL MiniMax TTS narration of the full script.
  6. ffmpeg compose: trim each clip to its beat duration with captions, concat, mux.

Output: a single .mp4 at workspace_dir/skeleton_short.mp4 + metadata files.
"""
from __future__ import annotations
import hashlib
import json
import math
import os
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

from studio_agent import production_costs

from . import captions as cap
from .scripting_grok import GrokClient, build_script_prompt
from .canonical_edit import build_scene_edit_prompt, generate_still_edit, sanitize_skeleton_outfit
from .i2v_engine import (
    ac_cost_for_video_model,
    generate as gen_clip,
    normalize_fal_video_model_id,
    resolve_video_model_chain,
)
from .styled_stills import normalize_fal_image_model_id
from .voice_fal import FalVoiceClient
from .compose import decode_audio_clock, probe_duration, trim_with_captions, concat_demuxer, mux_narration
from .prompts.category_registry import get_category

CANCEL_FLAG = "CANCELLED"


class RenderCancelled(Exception):
    """Raised inside a render loop when the user requests cancellation."""


def check_cancelled(workspace: Path) -> None:
    """Stop the render at the next checkpoint if a CANCELLED flag is present."""
    if (Path(workspace) / CANCEL_FLAG).exists():
        raise RenderCancelled("Render cancelled by user")


@dataclass
class Beat:
    index: int
    narration: str
    outfit: str
    scene_action: str
    motion_prompt: str
    duration_sec: float


def _retime_beats_to_narration(beats: list[Beat], total_duration: float) -> None:
    """Set beat durations from the real voiceover length instead of fixed 5s blocks."""
    if not beats or total_duration <= 0:
        return
    weights = [max(1, len(re.findall(r"\w+", beat.narration or ""))) for beat in beats]
    total_weight = max(1, sum(weights))
    raw = [max(2.4, total_duration * (w / total_weight)) for w in weights]
    scale = total_duration / max(0.001, sum(raw))
    for beat, dur in zip(beats, raw):
        beat.duration_sec = max(2.0, dur * scale)


#: FAL video lanes bill in whole duration tiers, not by the second. A beat of
#: 5.1s buys a 10s clip at double the price and the extra 4.9s is trimmed away
#: and discarded. This is the single largest avoidable cost in a short.
CHEAP_CLIP_SECONDS = 5.0
MIN_BEAT_SECONDS = 2.0


MUSIC_BED_EXTENSIONS = (".mp3", ".m4a", ".wav", ".aac", ".ogg")


def resolve_music_bed(explicit: str | Path | None = None, *, seed: str = "") -> Path | None:
    """Pick the background music bed for a short, or None to stay silent.

    Beds come from a local library (``STUDIO_MUSIC_BED_DIR``) rather than being
    generated per video: a bespoke track per short would add provider cost to
    every render for something the viewer hears at roughly 17dB under the
    voice. An explicit path always wins so a caller can pin one.

    Selection is seeded by topic so re-rendering the same video keeps the same
    bed instead of shuffling the soundtrack between takes.
    """
    if explicit:
        candidate = Path(explicit)
        return candidate if candidate.is_file() and candidate.stat().st_size > 0 else None
    library = str(os.getenv("STUDIO_MUSIC_BED_DIR", "") or "").strip()
    if not library:
        return None
    directory = Path(library)
    if not directory.is_dir():
        return None
    tracks = sorted(
        path
        for path in directory.iterdir()
        if path.is_file()
        and path.suffix.lower() in MUSIC_BED_EXTENSIONS
        and path.stat().st_size > 0
    )
    if not tracks:
        return None
    digest = hashlib.sha256(str(seed or "").encode("utf-8")).digest()
    return tracks[int.from_bytes(digest[:8], "big") % len(tracks)]


def rebalance_durations(
    values: list[float], cap: float = CHEAP_CLIP_SECONDS
) -> list[float] | None:
    """Fit every duration under ``cap`` without changing what they sum to.

    Total duration is load-bearing, not cosmetic: the final mux pads video to
    the narration clock by *freezing the last frame*, so any second the beats
    fail to cover becomes a visible freeze at the end of the video. Capping by
    truncation would buy a cheaper clip and pay for it in artifacting.

    Returns None when the durations genuinely cannot fit - if the average
    already exceeds the cap the longer clips are really needed, and paying for
    them is correct.
    """
    if not values:
        return None
    total = sum(float(value or 0) for value in values)
    if total <= 0 or total > cap * len(values) + 1e-9:
        return None
    durations = [float(value or 0) for value in values]
    if all(value <= cap for value in durations):
        return durations

    for _ in range(len(durations) + 2):
        excess = sum(max(0.0, value - cap) for value in durations)
        if excess <= 1e-9:
            break
        durations = [min(value, cap) for value in durations]
        headroom = [cap - value for value in durations]
        available = sum(headroom)
        if available <= 1e-9:
            return None
        for i, room in enumerate(headroom):
            durations[i] += excess * (room / available)

    if any(value > cap + 1e-6 for value in durations):
        return None
    # Absorb float drift into the beat with the most headroom so the beats still
    # sum to the narration length - a short video would desync from the audio.
    drift = total - sum(durations)
    if abs(drift) > 1e-9:
        target = min(range(len(durations)), key=lambda i: durations[i])
        durations[target] = max(MIN_BEAT_SECONDS, durations[target] + drift)
    return durations


def _cap_beat_durations(beats: list[Beat], cap: float = CHEAP_CLIP_SECONDS) -> bool:
    """Rebalance beat durations so none crosses a provider billing tier."""
    if not beats:
        return False
    rebalanced = rebalance_durations([float(b.duration_sec or 0) for b in beats], cap)
    if rebalanced is None:
        return False
    for beat, value in zip(beats, rebalanced):
        beat.duration_sec = value
    return True


#: Measured from a real narration: 203 words rendered to 73.8s of voiceover.
#: Used only to size the beat plan before the audio exists; the real duration
#: still governs once it has been synthesised.
NARRATION_WORDS_PER_SECOND = 2.75


def _gate_reference_still(
    roster_path: Path,
    *,
    master_ref: str,
    outfit: str,
    cast_count: int,
) -> dict[str, Any]:
    """Audit a reference still and refuse to animate a structurally broken one.

    Raises ``ReferenceStillRejected`` rather than proceeding, so the one-shot
    render path can no longer spend animation budget on a reference nothing
    checked. QA that cannot run is a rejection, not a pass.
    """
    from studio_agent import still_gate, visual_qa

    try:
        report = visual_qa.audit_skeleton_still(
            roster_path,
            reference=master_ref,
            locked_outfit=str(outfit or ""),
            force=True,
            cast_count=int(cast_count or 1),
        )
    except Exception as exc:  # QA failure is not permission to animate.
        report = {
            "status": "fail",
            "pass": False,
            "issues": ["qa_unavailable"],
            "summary": f"Reference QA raised: {exc}",
        }
    return still_gate.require_animatable_reference(report, still_path=roster_path)


def plan_beat_count(script_text: str, requested: int = 12) -> int:
    """Choose enough beats that the average one fits the cheap clip tier.

    A fixed beat count silently sets the price of a short. 203 words became
    73.8s of narration, which over 12 beats is a 6.15s average - above the
    tier - so every clip billed at 10s and the render cost about $12 instead of
    about $8. More, shorter beats are strictly cheaper here, and they match the
    5s average shot length this channel already cuts to.
    """
    words = len(re.findall(r"\w+", str(script_text or "")))
    if not words:
        return max(1, int(requested or 1))
    estimated_seconds = words / NARRATION_WORDS_PER_SECOND
    needed = int(math.ceil(estimated_seconds / CHEAP_CLIP_SECONDS))
    return max(1, int(requested or 0), needed)


def _split_sentence(sentence: str) -> tuple[str, str] | None:
    """Split one sentence at the clause boundary nearest its middle."""
    words = sentence.split()
    if len(words) < 8:
        return None
    best: tuple[int, int] | None = None
    for i, word in enumerate(words[:-3]):
        if i < 3:
            continue
        if re.search(r"[,;:]$", word) or word.lower() in {
            "and", "but", "because", "so", "which", "while", "when", "then",
        }:
            distance = abs(i - len(words) // 2)
            if best is None or distance < best[0]:
                best = (distance, i + 1)
    index = best[1] if best else len(words) // 2
    head = " ".join(words[:index]).strip()
    tail = " ".join(words[index:]).strip()
    if not head or not tail:
        return None
    return head, tail


def split_script_into_beats(script_text: str, target_count: int = 12) -> list[str]:
    """Split the narration into exactly the beats the render will use.

    This used to return ``sentences[:target_count]``, which quietly dropped the
    tail of the script from the visual plan while the voiceover still spoke it -
    a real short shipped with a sentence of narration that had no shot assigned
    to it. Nothing is dropped now: too many sentences are merged, too few are
    split at clause boundaries.
    """
    sentences = re.split(r"(?<=[.!?])\s+", str(script_text or "").strip())
    sentences = [s.strip() for s in sentences if s.strip()]
    if not sentences:
        return []
    target = max(1, int(target_count or 1))

    # Too many beats: merge the cheapest adjacent pair until the count fits, so
    # every word keeps a shot rather than losing the overflow.
    while len(sentences) > target:
        pair = min(
            range(len(sentences) - 1),
            key=lambda i: len(sentences[i].split()) + len(sentences[i + 1].split()),
        )
        sentences[pair : pair + 2] = [f"{sentences[pair]} {sentences[pair + 1]}"]

    # Too few beats: split the longest until the count fits or nothing splits.
    while len(sentences) < target:
        order = sorted(
            range(len(sentences)), key=lambda i: len(sentences[i].split()), reverse=True
        )
        for index in order:
            halves = _split_sentence(sentences[index])
            if halves:
                sentences[index : index + 1] = list(halves)
                break
        else:
            break
    return sentences


_PLAN_SYSTEM_PROMPT = (
    "You are the visual planner for NYPTID Studio Skeleton AI shorts.\n\n"
    "THE HOST IS LOCKED — one canonical 3D skeleton from the master reference: "
    "ivory-white anatomical bones and translucent glass body shell. The eyes already "
    "present in the reference remain unchanged. No skin, flesh, human limbs, or muscle tissue. "
    "Identity never changes. Scene-to-scene you may only "
    "change OUTFIT (worn over the shell), BACKGROUND, optional set dressing, and POSE.\n\n"
    "CRITICAL HOST RULES:\n"
    "- There is exactly ONE on-camera host: the skeleton. Never invent a separate human cast.\n"
    "- Do NOT plan multi-person characters (guy, woman, couple, ancestor as separate people).\n"
    "- Hands stay EMPTY by default (presenter gestures). No basketball, sports balls, dumbbells, "
    "gym gear, weapons, or random handheld props unless the TOPIC is explicitly about sports/fitness.\n"
    "- Psychology / relationship / dark-psychology topics use a modern psychology-studio or cinematic "
    "interior environment — never a gym, court, or sports arena unless the topic names that setting.\n\n"
    "Given a full narration script and optional topic hint, lock ONE continuous skeleton_host "
    "look so every beat stays consistent. Be specific about colors/fabrics only when wardrobe is needed.\n\n"
    "Output strict JSON:\n"
    "  {\n"
    '    "characters": { "skeleton_host": "<outfit OR no clothing note, ~15-30 words>" },\n'
    '    "topic_setting": "<one sentence: environment / location / lighting — not a sports venue>",\n'
    '    "fallback_outfit": "<default look, or no clothing>"\n'
    "  }\n"
    "No markdown fences, no commentary outside the JSON."
)


WARDROBE_MOTION_LOCK = (
    "Wardrobe continuity lock: keep every garment solid and unchanged for the entire clip; "
    "white T-shirt or undershirt stays opaque under any open coat or jacket; "
    "black pants and shoes stay complete; no shirtless frames, bare chest, exposed sternum, "
    "exposed ribcage, transparent fabric, disappearing clothing, wardrobe popping, or outfit morphing."
)


def apply_wardrobe_motion_lock(motion: str, outfit: str | None = None) -> str:
    """Append a hard continuity guard to image-to-video prompts."""
    motion_text = str(motion or "").strip() or "Subtle idle motion, soft ambient movement"
    outfit_text = str(outfit or "").strip()
    if "wardrobe continuity lock" in motion_text.lower():
        return motion_text[:900]
    lock = WARDROBE_MOTION_LOCK
    if outfit_text:
        lock = f"{lock} Locked outfit remains unchanged: {outfit_text[:240]}."
    return f"{motion_text}. {lock}"[:900]


def _merge_visual_brief(outfit: str, visual_brief: str | None) -> str:
    vb = (visual_brief or "").strip()
    if not vb:
        return outfit
    base = (outfit or "").strip()
    if base and vb.lower() not in base.lower():
        return f"{base}. Session wardrobe lock: {vb}"
    return vb or base


def _visual_brief_requests_wardrobe(visual_brief: str | None) -> bool:
    low = str(visual_brief or "").lower()
    wardrobe_terms = (
        "wearing", "wears", "wardrobe", "outfit", "clothing", "clothes",
        "shirt", "hoodie", "jacket", "coat", "pants", "jeans", "shorts",
        "dress", "suit", "uniform", "armor", "cape", "hat", "helmet",
        "shoes", "sneakers", "boots",
    )
    return any(term in low for term in wardrobe_terms)


def _visual_brief_beat_direction(
    visual_brief: str | None,
    beat_index: int | None,
) -> str:
    """Extract a numbered Beat N directive from a user-authored visual brief."""
    if beat_index is None:
        return ""
    text = str(visual_brief or "").strip()
    if not text:
        return ""
    number = int(beat_index) + 1
    match = re.search(
        rf"\bBeat\s*{number}\s*:\s*(.+?)(?=\s+\bBeat\s*\d+\s*:|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    return re.sub(r"\s+", " ", match.group(1)).strip(" .;") if match else ""


def _expand_locked_scene_direction(direction: str) -> str:
    """Keep the user's beat literal while making the environment visible."""
    scene = re.sub(r"\s+", " ", str(direction or "")).strip(" .;")
    if not scene:
        return ""
    low = scene.lower()
    explicit_closeup = any(
        term in low
        for term in (
            "close-up",
            "closeup",
            "extreme close",
            "macro shot",
            "headshot",
        )
    )
    composition = (
        "Use the explicitly requested close framing while retaining recognizable "
        "environment details in the background."
        if explicit_closeup
        else (
            "Use a medium-wide vertical shot showing the skeleton from at least head "
            "to knees plus substantial surrounding environment; no isolated close-up."
        )
    )
    return (
        f"{scene}. Replace the entire reference background with the named location. "
        f"{composition} The location must contain recognizable physical details and "
        "fill the frame; never use a black void, plain studio backdrop, or unrelated prop."
    )


def _merge_locked_scene_with_generated(
    locked_scene: str,
    generated_action: str,
) -> str:
    """Enrich a mandatory beat without allowing the planner to replace it."""
    locked = str(locked_scene or "").strip()
    generated = re.sub(r"\s+", " ", str(generated_action or "")).strip(" .;")
    if not locked:
        return generated
    if not generated:
        return locked
    return (
        f"{locked} Supporting environment detail from the scene planner: {generated}. "
        "Use those details only as set dressing, camera, and lighting; they must not "
        "change the mandatory location, action, subject, or composition above."
    )


def analyze_script(grok: GrokClient, script_text: str, *, category_label: str = "",
                   topic: str | None = None, visual_brief: str | None = None) -> dict:
    """
    Pre-pass: read the full script and lock a canonical character + style sheet.

    The returned plan is then passed to derive_beat_visuals so every beat
    that mentions the same character renders the SAME outfit. Without this
    pre-pass, Grok would re-invent Thor's cape color on every beat.
    """
    user_lines = []
    if category_label:
        user_lines.append(f"Category: {category_label}")
    if topic:
        user_lines.append(f"Topic hint: {topic}")
    if visual_brief:
        user_lines.append(
            f"USER VISUAL DIRECTION (environment, pose, props, overlays, and wardrobe only if explicitly named): {visual_brief}"
        )
    user_lines.append("Script:")
    user_lines.append(script_text.strip())
    user_lines.append("\nReturn the JSON plan now.")
    user = "\n".join(user_lines)

    raw = grok.complete(_PLAN_SYSTEM_PROMPT, user, max_tokens=900, temperature=0.5)
    raw = raw.strip().strip("`").strip()
    if raw.lower().startswith("json"):
        raw = raw[4:].strip()
    try:
        plan = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        plan = {}
    # The JSON literal `null` is valid but is not a scene plan.  Never let an
    # upstream provider response turn into a `.get` crash in production.
    if not isinstance(plan, dict):
        plan = {}
    if not isinstance(plan.get("characters"), dict):
        plan["characters"] = {}
    plan.setdefault(
        "fallback_outfit",
        "neutral charcoal turtleneck and dark jeans, simple leather sneakers",
    )
    plan.setdefault("topic_setting", "")
    if visual_brief:
        plan["visual_brief_lock"] = visual_brief.strip()
    if not _visual_brief_requests_wardrobe(visual_brief):
        plan["fallback_outfit"] = (
            "no clothing; preserve the complete canonical transparent glass shell "
            "and ivory bone anatomy from the master reference; empty hands, no props"
        )
        # Drop multi-human cast sheets — only the skeleton host is on camera.
        plan["characters"] = {"skeleton_host": plan["fallback_outfit"]}
    else:
        # Collapse invented multi-person cast into a single host look.
        host = (
            plan["characters"].get("skeleton_host")
            or plan.get("fallback_outfit")
            or "no clothing; empty hands"
        )
        plan["characters"] = {"skeleton_host": str(host)[:200]}
    # Strip sports venues from topic_setting when topic is not sports.
    from .canonical_edit import sanitize_skeleton_prop_language

    plan["topic_setting"] = sanitize_skeleton_prop_language(
        str(plan.get("topic_setting") or ""),
        topic=str(topic or ""),
        visual_brief=str(visual_brief or ""),
    )
    plan["fallback_outfit"] = sanitize_skeleton_outfit(
        str(plan.get("fallback_outfit") or ""),
        topic=str(topic or ""),
        visual_brief=str(visual_brief or ""),
    )
    plan["characters"]["skeleton_host"] = sanitize_skeleton_outfit(
        str(plan["characters"].get("skeleton_host") or plan["fallback_outfit"]),
        topic=str(topic or ""),
        visual_brief=str(visual_brief or ""),
    )
    return plan


def derive_beat_visuals(
    grok: GrokClient,
    narration: str,
    category_label: str,
    *,
    plan: dict | None = None,
    visual_brief: str | None = None,
    beat_index: int | None = None,
    cast_count: Any = None,
) -> tuple[str, str, str]:
    """Per-beat visuals for canonical skeleton Seedream edit (background/outfit/props only)."""
    plan = plan or {"characters": {}, "fallback_outfit": "charcoal hoodie and dark joggers"}
    chars_json = json.dumps(plan.get("characters", {}), ensure_ascii=False)
    setting = plan.get("topic_setting", "")
    fallback = plan.get("fallback_outfit", "")
    vbl = (visual_brief or plan.get("visual_brief_lock") or "").strip()
    from .prompt_compose import dual_host_staging_brief, resolve_cast_count

    hosts = resolve_cast_count(
        job_cast=plan.get("cast_count"),
        scene_cast=cast_count,
        topic=str(setting or category_label or ""),
        visual_brief=vbl,
        narration=narration,
    )
    host_identity = (
        "exactly TWO identical canonical ivory skeleton hosts, each with its own thin glass shell"
        if hosts >= 2
        else "exactly ONE canonical ivory skeleton host with its thin glass shell"
    )
    extra_subject_rule = (
        "Do NOT describe humans, a third person, or a third skeleton. "
        if hosts >= 2
        else "Do NOT describe a couple, second person, or second skeleton. "
    )
    hand_rule = (
        "Exactly four hands total, two correctly attached to each host; no extra or floating limbs."
        if hosts >= 2
        else "Exactly two hands total, no third hand, no floating limbs."
    )
    eye_rule = (
        "Exactly four realistic eyes total, two inside each skull's eye sockets."
        if hosts >= 2
        else "Exactly two realistic eyes, ONLY inside the skull eye sockets."
    )
    locked_scene = _expand_locked_scene_direction(
        _visual_brief_beat_direction(vbl, beat_index)
    )

    sys = (
        "You compose ONE per-scene visual prompt for a NYPTID Skeleton AI short.\n\n"
        f"THE CAST IS LOCKED — use {host_identity}, with the unchanged eyes from the master reference. "
        f"{extra_subject_rule}Do NOT describe a different character, porcelain mannequin, or human actor. "
        "Only wardrobe, environment, optional set dressing, and pose may change. "
        "Every exposed body part must remain ivory bone inside clear glass. "
        "Never output skin, flesh, muscles, human hands, human feet, or the phrase 'bare feet'. "
        "The glass shell is body-shaped and hugs the skeleton silhouette like clear skin; "
        "never describe a bell jar, capsule, dome, specimen tube, cylinder, display case, "
        "helmet bubble, glass container, circular base, labels, callouts, diagrams, or readable text. "
        "Never use split screen, diptych, side-by-side panels, before/after layouts, or comparison collages — "
        "those duplicate the skeleton and create extra hands. Use one continuous full-frame scene only. "
        f"{hand_rule}\n\n"
        "HANDS / PROPS (CRITICAL):\n"
        "- Default: EMPTY hands in a clear presenter / talking-head gesture.\n"
        "- FORBIDDEN unless the topic is sports/fitness: basketball, any sports ball, dumbbells, "
        "barbells, gym racks, courts, jerseys, primitive tools, weapons, random handheld objects.\n"
        "- Never invent sports gear.\n"
        "- Psychology / relationship topics: choose a DISTINCT real cinematic location and composition driven by THIS narration beat "
        "(apartment doorway, quiet cafe window, office corridor, library aisle, train platform, parking garage, or a specific studio setup). "
        "Do not repeat a generic studio presenter shot for consecutive beats.\n"
        "EYES (CRITICAL):\n"
        f"- {eye_rule}\n"
        "- FORBIDDEN: eyeballs in the ribcage, sternum, chest cavity, abdomen, or as floating orbs.\n"
        "- Soft amber light along the spine is light only — never literal eyes in the torso.\n\n"
        "Output strict JSON:\n"
        "  outfit — the shared clothing lock worn ON each canonical skeleton OR 'no clothing'. "
        "Never describe human wardrobe as a separate person.\n"
        f"  scene_action — photoreal 9:16 environment + pose for {host_identity} "
        "(psychology studio, moody apartment, cinematic interior — NOT gym/court unless topic is sports). "
        "Empty hands. No text overlays.\n"
        "  motion_prompt — one SILENT 5-second i2v PERFORMANCE (pose change + gesture + camera/background "
        "energy + glass-shell light motion). Not near-static. No talking, no jaw/mouth/lip-sync "
        "(voiceover is added later). No prop moves, no text overlays.\n"
        "  bare_torso — always false\n"
        "No markdown."
    )
    user_lines = [
                  f"Mandatory visual direction: {vbl}" if vbl else "",
                  f"MANDATORY EXACT SCENE FOR THIS BEAT: {locked_scene}" if locked_scene else "",
                  f"Topic setting: {setting}" if setting else "",
                  f"Character sheet (JSON): {chars_json}",
                  f"Fallback outfit: {fallback}",
                  f"Category label: {category_label}",
                  f"Narration beat: {narration}",
                  "Return JSON now."]
    user = "\n".join(line for line in user_lines if line)

    raw = grok.complete(sys, user, max_tokens=500, temperature=0.6)
    raw = raw.strip().strip("`").strip()
    if raw.lower().startswith("json"):
        raw = raw[4:].strip()
    try:
        data = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        data = None
    if not isinstance(data, dict):
        fallback_action = (
            "Two identical canonical skeleton hosts in a modern relationship-psychology interior; "
            f"{dual_host_staging_brief()}; sharp commercial lighting, vertical 9:16, no sports props"
            if hosts >= 2
            else "Exactly one canonical skeleton host in a modern psychology-studio environment matching the narration, "
            "empty hands in a presenter gesture, sharp commercial lighting, vertical 9:16, no sports props"
        )
        fallback_motion = (
            "Left host opens both hands while the right host draws half-step back; slow push-in"
            if hosts >= 2
            else "Subtle weight shift and ambient light flicker over five seconds"
        )
        return (
            fallback or "no clothing; empty hands, no props",
            fallback_action,
            fallback_motion,
        )
    topic_hint = str((plan or {}).get("topic_setting") or category_label or "")
    outfit = sanitize_skeleton_outfit(
        data.get("outfit", fallback) or fallback or "no clothing; empty hands, no props",
        topic=topic_hint,
        visual_brief=vbl,
    )
    if bool(data.get("bare_torso", False)):
        outfit = f"[BARE_TORSO] {outfit}"
    generated_action = data.get(
        "scene_action",
        (
            "Two identical canonical skeleton hosts in a relationship-psychology interior; "
            f"{dual_host_staging_brief()}; premium 9:16 framing, no sports props"
            if hosts >= 2
            else "Exactly one canonical skeleton host in a psychology-studio environment matching the narration, "
            "empty hands, premium 9:16 framing, no sports props"
        ),
    )
    from .canonical_edit import sanitize_skeleton_scene_action

    generated_action, _ = sanitize_skeleton_scene_action(
        generated_action,
        topic=topic_hint,
        visual_brief=vbl,
        narration=narration,
        cast_count=hosts,
    )
    locked_scene, _ = sanitize_skeleton_scene_action(
        locked_scene,
        topic=topic_hint,
        visual_brief=vbl,
        narration=narration,
        cast_count=hosts,
    )
    action = _merge_locked_scene_with_generated(locked_scene, generated_action)
    action, _ = sanitize_skeleton_scene_action(
        action,
        topic=topic_hint,
        visual_brief=vbl,
        narration=narration,
        cast_count=hosts,
    )
    # Providers often return "psychology studio, presenter pose" for every
    # beat. Preserve an explicit user lock, but replace that lazy fallback
    # with a distinct filmable location so a six-scene short is actually six
    # directed scenes rather than six camera angles in the same room.
    if not vbl and re.fullmatch(r"(?is).*\b(?:psychology|modern)\s+studio\b.*", action.strip()):
        if hosts >= 2:
            variations = tuple(
                f"{location}; {dual_host_staging_brief()}"
                for location in (
                    "Quiet apartment doorway at blue hour, medium-wide frame",
                    "Rainy cafe window booth, medium-wide three-quarter frame",
                    "Long empty office corridor at night, medium-wide tracking composition",
                    "Library aisle with warm practical lamps, medium-wide frame",
                    "Cinema lobby after closing, wide frame with reflected floor lights",
                    "Train platform at dawn, medium-wide frame",
                )
            )
        else:
            variations = (
                "Quiet apartment doorway at blue hour, medium side profile; the host pauses before leaving, one hand resting on the doorframe",
                "Rainy cafe window booth, close three-quarter portrait; the host studies a phone left face-down on the table",
                "Long empty office corridor at night, medium-wide tracking composition; the host stops beneath practical ceiling lights",
                "Library aisle with warm practical lamps, profile medium shot; the host reaches toward a book then pulls the hand back",
                "Cinema lobby after closing, wide frame with reflected floor lights; the host stands alone facing the exit signs",
                "Train platform at dawn, medium-wide shot; the host watches a departing train through glass without touching any prop",
            )
        action = variations[int(beat_index or 0) % len(variations)]
        action, _ = sanitize_skeleton_scene_action(
            action,
            topic=topic_hint,
            visual_brief=vbl,
            narration=narration,
            cast_count=hosts,
        )
    from .prompt_compose import compose_skeleton_motion_prompt

    motion = compose_skeleton_motion_prompt(
        motion=(
            f"{_skeleton_performance_motion(narration, beat_index)}; "
            f"{data.get('motion_prompt', 'subtle controlled movement')}"
        ),
        locked_outfit=outfit,
        cast_count=hosts,
    )
    return (
        outfit,
        f"PERFORMANCE: {_skeleton_performance_direction(narration, beat_index)}; {action}",
        apply_wardrobe_motion_lock(motion, outfit),
    )


def _skeleton_performance_direction(narration: str, beat_index: int | None) -> str:
    """Filmable emotion without changing the canonical skull, eyes, or anatomy."""
    text = str(narration or "").lower()
    if any(word in text for word in ("ghost", "pull away", "distance", "avoid", "wall", "withdraw", "silence")):
        return "withdrawn body language, chin lowered, gaze averted, shoulders drawn in, one hand half-raised then held still"
    if any(word in text for word in ("uncertain", "fear", "anxious", "doubt", "why", "confused", "risk")):
        return "uneasy posture, head tilted, focused sideward gaze, weight shifted back, hands held close to the torso"
    if any(word in text for word in ("truth", "real reason", "realize", "insight", "understand", "reveal")):
        return "moment of realization, head raised, direct focused gaze, torso leaning forward, one open explanatory hand"
    if any(word in text for word in ("care", "love", "connection", "warmth", "trust", "secure")):
        return "guard softening, gentle head tilt, calm eye focus, relaxed shoulders, one open welcoming hand"
    if any(word in text for word in ("chase", "hunt", "pressure", "mission", "compete", "win")):
        return "alert purposeful stance, forward lean, intent eye focus, squared shoulders, restrained decisive gesture"
    variants = (
        "quietly reflective pose, chin lowered, eyes focused away from camera, hands loosely open",
        "tense contained pose, head angled to one side, eyes fixed on a distant point, arms close to the body",
        "engaged explanatory pose, direct eye focus, slight forward lean, one hand open in emphasis",
    )
    return variants[int(beat_index or 0) % len(variants)]


def _skeleton_performance_motion(narration: str, beat_index: int | None) -> str:
    direction = _skeleton_performance_direction(narration, beat_index)
    return f"Slow natural performance: {direction}; subtle head turn and controlled hand movement; stable anatomy"


def _write_progress(workspace: Path, *, stage: str, progress: int, detail: str = "") -> None:
    try:
        payload = {
            "stage": stage,
            "progress": max(0, min(100, int(progress))),
            "detail": str(detail or "")[:240],
        }
        try:
            production_costs.attach_to_progress(workspace, payload)
        except Exception:
            pass
        (workspace / "progress.json").write_text(
            json.dumps(payload, indent=2),
            encoding="utf-8",
        )
        # Also bump heartbeat so status poller (and stale detector) sees liveness even between coarse stages.
        try:
            (workspace / "heartbeat.txt").touch(exist_ok=True)
        except Exception:
            pass
    except Exception:
        pass


def run(
    category_key: str,
    topic: str | None,
    workspace: Path,
    *,
    tier: str = "standard",
    video_model: str | None = None,
    visual_brief: str | None = None,
    beats_target: int = 12,
    grok: GrokClient | None = None,
    el: Any = None,
    voice_id: str | None = None,
    script_override: str | None = None,
    user_id: str | None = None,
    watermark_text: str = "Studio",
    captions_enabled: bool = True,
    caption_mode: str = "word",
    master_reference_url: str = "",
    image_model_id: str = "seedream_edit",
    cast_count: Any = None,
    music_track: str | Path | None = None,
) -> dict:
    """Run the full Skeleton AI pipeline. Returns a result dict."""
    workspace = Path(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    requested_image_model = str(image_model_id or "").strip().lower()
    effective_image_model = normalize_fal_image_model_id(image_model_id)
    requested_video_model = str(video_model or "").strip().lower()
    effective_video_model = normalize_fal_video_model_id(video_model, tier=tier)
    master_ref = str(master_reference_url or "").strip()
    if not master_ref:
        try:
            from skeleton_ai.styled_pipeline import _resolve_skeleton_master_reference

            master_ref = _resolve_skeleton_master_reference(workspace, None)
        except Exception:
            master_ref = ""
    stills_dir = workspace / "stills"
    clips_dir = workspace / "clips"
    trimmed_dir = workspace / "trimmed"
    work_dir = workspace / "work"
    for d in (stills_dir, clips_dir, trimmed_dir, work_dir):
        d.mkdir(exist_ok=True)

    grok = grok or GrokClient()
    el = el or FalVoiceClient()
    _write_progress(workspace, stage="script", progress=8, detail="Writing script")

    # 1. Generate the script (or use user-edited override from Create panel).
    cat = get_category(category_key, user_id=user_id)
    if script_override and script_override.strip():
        script_text = script_override.strip()
    else:
        user_prompt = build_script_prompt(cat["system_prompt"], topic)
        script_text = grok.complete(cat["system_prompt"], user_prompt, max_tokens=1500)
    (workspace / "script.txt").write_text(script_text, encoding="utf-8")

    # 2. Pre-pass: analyze the full script to lock a canonical character +
    # style sheet. Every beat that mentions the same subject will reuse the
    # identical outfit description, guaranteeing visual continuity across
    # all 12 beats. Works for the 4 idea-list categories AND custom topics.
    _write_progress(workspace, stage="scene_plan", progress=18, detail="Locking character sheet")
    plan = analyze_script(
        grok,
        script_text,
        category_label=cat["label"],
        topic=topic,
        visual_brief=visual_brief,
    )
    (workspace / "scene_plan.json").write_text(
        json.dumps(plan, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # 3. Split into beats. The requested count is a floor, not a ceiling: a
    # longer script needs more beats to keep each clip inside the cheap tier.
    sentences = split_script_into_beats(
        script_text, target_count=plan_beat_count(script_text, beats_target)
    )
    if not sentences:
        raise RuntimeError("Anthropic returned empty script")

    # 4. For each beat, derive outfit/action/motion using the locked plan.
    requested_cast = cast_count
    if requested_cast is None:
        try:
            job_spec = json.loads((workspace / "job_spec.json").read_text(encoding="utf-8"))
            if isinstance(job_spec, dict):
                requested_cast = job_spec.get("cast_count")
        except Exception:
            requested_cast = None
    from .prompt_compose import resolve_cast_count

    production_cast = resolve_cast_count(
        job_cast=requested_cast,
        topic=str(topic or cat["label"] or ""),
        visual_brief=str(visual_brief or ""),
        narration=script_text,
    )
    beats: list[Beat] = []
    for i, narration in enumerate(sentences):
        outfit, action, motion = derive_beat_visuals(
            grok,
            narration,
            cat["label"],
            plan=plan,
            visual_brief=visual_brief,
            beat_index=i,
            cast_count=production_cast,
        )
        beats.append(Beat(
            index=i,
            narration=narration,
            outfit=outfit,
            scene_action=action,
            motion_prompt=motion,
            duration_sec=5.0,
        ))

    _write_progress(workspace, stage="narration", progress=23, detail="Voiceover timing")
    narration_audio = el.synthesize(
        text=script_text,
        out_path=workspace / "narration.mp3",
        voice_id=voice_id,
    )
    amount, note, key, qty = production_costs.price_fal_tts(script_text)
    production_costs.record_event(
        workspace,
        stage="narration",
        provider="fal",
        operation=key,
        usd=amount,
        quantity=qty,
        unit="1k_chars",
        metadata={"pricing_note": note, "chars": len(script_text)},
    )
    narration_clock = decode_audio_clock(narration_audio, work_dir / "narration_clock.wav")
    narration_duration = probe_duration(narration_clock)
    normalized_caption_mode = (
        "word"
        if str(caption_mode or "").strip().lower() in {"word", "single_word", "one_word"}
        else "phrase"
    )
    global_word_phrases: list[cap.CaptionPhrase] = []
    if captions_enabled and normalized_caption_mode == "word":
        from studio_agent.caption_alignment import align_audio_words

        verified_words = align_audio_words(
            narration_clock,
            cache_path=work_dir / "caption_alignment.json",
        )
        global_word_phrases, timing_source = cap.build_timed_captions(
            script_text,
            narration_duration,
            caption_mode="word",
            verified_word_timings=verified_words,
        )
        if timing_source != "verified_word":
            raise RuntimeError("word caption alignment lost verified timing provenance")
    _retime_beats_to_narration(beats, narration_duration)
    # Keep every beat inside the cheap clip tier where the narration allows it.
    # Without this, a beat that lands at 5.1s silently doubles that clip's price.
    _cap_beat_durations(beats)

    # 4. Render stills + clips per beat (canonical master edit — identity locked).
    trimmed_paths: list[Path] = []
    caption_scenes: list[dict[str, Any]] = []
    caption_offset = 0.0
    roster_cache: dict[str, Path] = {}
    total_beats = max(len(beats), 1)
    for beat in beats:
        check_cancelled(workspace)
        pct = 25 + int((beat.index / total_beats) * 55)
        _write_progress(
            workspace,
            stage="render",
            progress=pct,
            detail=f"Beat {beat.index + 1}/{total_beats} — stills + motion",
        )
        sid = f"b{beat.index:02d}"
        outfit_key = (beat.outfit or "default").strip()[:120]
        extra_refs: list[str] = []
        if outfit_key and outfit_key != "default":
            roster_path = roster_cache.get(outfit_key)
            if not roster_path or not roster_path.exists():
                roster_path = stills_dir / f"roster_{beat.index:02d}_{outfit_key[:32].replace(' ', '_')}.png"
                roster_prompt = build_scene_edit_prompt(
                    topic=topic or cat["label"],
                    # "full skeleton ... front-facing", never "full body front
                    # view": paired with the no-garments wardrobe bit, the latter
                    # tripped FAL's content checker and failed the roster edit
                    # with content_policy_violation.
                    # Anatomy detail is stated positively here because the roster
                    # is the reference every scene inherits. Frame inspection
                    # found a featureless skull and thumbless hands baked into
                    # the reference *before* animation ran, so every clip
                    # inherited them and no downstream QA could recover the
                    # video. Fixing it here fixes every future render at zero
                    # marginal cost.
                    visual_description=(
                        "Plain neutral studio backdrop, full skeleton visible head to feet, "
                        "front-facing. Cranium shows real bone structure with visible sutures "
                        "and temporal ridges, not a smooth dome. Both eyes equal size, seated "
                        "level in the sockets. Each hand has five digits including an opposed "
                        "thumb, correctly jointed."
                    ),
                    outfit=beat.outfit,
                    cast_count=production_cast,
                )
                generate_still_edit(
                    roster_prompt,
                    roster_path,
                    master_url=master_ref,
                    seed=420100 + beat.index,
                    cast_count=production_cast,
                    image_model_id=effective_image_model,
                )
                amount, note, key = production_costs.price_fal_image(
                    edit=True,
                    model_id=effective_image_model,
                )
                production_costs.record_event(
                    workspace,
                    stage="stills",
                    provider="fal",
                    operation=key,
                    usd=amount,
                    quantity=1,
                    unit="image",
                    scene_index=beat.index,
                    metadata={"pricing_note": note, "role": "roster_reference"},
                )
                # This path used to have no visual QA at all - zero call sites -
                # while remaining a public endpoint the Create panel calls. Every
                # short made through it rendered ungated. The roster is the
                # reference every beat inherits, so auditing it here is both the
                # cheapest check available ($0.04 still vs $5.88 of animation)
                # and the only one that can stop a doomed render before it pays.
                _gate_reference_still(
                    roster_path,
                    master_ref=master_ref,
                    outfit=beat.outfit,
                    cast_count=production_cast,
                )
                roster_cache[outfit_key] = roster_path
            extra_refs = [str(roster_path)]

        edit_prompt = build_scene_edit_prompt(
            topic=topic or cat["label"],
            visual_description=beat.scene_action,
            outfit=beat.outfit,
            cast_count=production_cast,
        )
        still_result = generate_still_edit(
            edit_prompt,
            stills_dir / f"{sid}.png",
            master_url=master_ref,
            extra_refs=extra_refs,
            seed=420042 + beat.index,
            cast_count=production_cast,
            image_model_id=effective_image_model,
        )
        still_path = Path(
            still_result["local_path"]
            if isinstance(still_result, dict)
            else still_result
        )
        amount, note, key = production_costs.price_fal_image(
            edit=True,
            model_id=effective_image_model,
        )
        production_costs.record_event(
            workspace,
            stage="stills",
            provider="fal",
            operation=key,
            usd=amount,
            quantity=1,
            unit="image",
            scene_index=beat.index,
            metadata={"pricing_note": note, "role": "scene_still"},
        )
        clip_path = gen_clip(
            still_path,
            beat.motion_prompt,
            clips_dir / f"{sid}.mp4",
            tier=tier,
            video_model=effective_video_model,
            duration_sec=10 if float(beat.duration_sec or 0) > 5 else 5,
            budget_workspace=workspace,
        )
        try:
            sidecar = clip_path.with_suffix(clip_path.suffix + ".fal.json")
            clip_meta = json.loads(sidecar.read_text(encoding="utf-8")) if sidecar.is_file() else {}
        except Exception:
            clip_meta = {}
        endpoint = str(clip_meta.get("endpoint") or "")
        duration = float(clip_meta.get("duration_sec") or (10 if float(beat.duration_sec or 0) > 5 else 5))
        amount, note, key = production_costs.price_fal_video(endpoint, seconds=duration)
        production_costs.record_event(
            workspace,
            stage="animation",
            provider="fal",
            operation=key,
            usd=amount,
            quantity=duration,
            unit="second",
            endpoint=endpoint,
            request_id=str(clip_meta.get("request_id") or ""),
            scene_index=beat.index,
            metadata={"pricing_note": note, "video_model": clip_meta.get("video_model") or effective_video_model},
        )
        render_scene_captions = captions_enabled and normalized_caption_mode != "word"
        trimmed = trim_with_captions(
            clip_path,
            trimmed_dir / f"{sid}.mp4",
            duration_sec=beat.duration_sec,
            narration_text=beat.narration,
            watermark_text=watermark_text,
            caption_mode="phrase",
            captions_enabled=render_scene_captions,
            force=True,
        )
        trimmed_paths.append(trimmed)
        manifest_path = trimmed.with_suffix(trimmed.suffix + ".captions.json")
        try:
            scene_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            if render_scene_captions:
                raise RuntimeError(f"caption manifest missing for beat {beat.index + 1}")
            scene_manifest = {"timing_source": "disabled", "cues": []}
        scene_cues = [
            {
                "text": str(cue.get("text") or ""),
                "start": round(caption_offset + float(cue.get("start") or 0.0), 4),
                "end": round(caption_offset + float(cue.get("end") or 0.0), 4),
                "scene_index": beat.index,
            }
            for cue in list(scene_manifest.get("cues") or [])
            if isinstance(cue, dict)
        ]
        caption_scenes.append({
            "scene_index": beat.index,
            "offset_sec": round(caption_offset, 4),
            "duration_sec": round(float(beat.duration_sec), 4),
            "timing_source": str(scene_manifest.get("timing_source") or "disabled"),
            "cues": scene_cues,
        })
        caption_offset += float(beat.duration_sec)

    _write_progress(workspace, stage="compose", progress=94, detail="Muxing final MP4")
    # 6. Concat + mux.
    silent = concat_demuxer(trimmed_paths, workspace / "silent.mp4", work_dir)
    music_bed = resolve_music_bed(music_track, seed=str(topic or category_key or ""))
    final = mux_narration(
        silent,
        narration_clock,
        workspace / "skeleton_short.mp4",
        caption_phrases=global_word_phrases,
        music_track=music_bed,
    )
    timing_source = (
        "verified_word"
        if captions_enabled and normalized_caption_mode == "word"
        else "script_weighted_estimate"
        if captions_enabled
        else "disabled"
    )
    if captions_enabled and normalized_caption_mode == "word":
        final_caption_cues = [
            {
                "text": phrase.text,
                "start": round(phrase.start_sec, 4),
                "end": round(phrase.start_sec + phrase.duration_sec, 4),
            }
            for phrase in global_word_phrases
        ]
        final_caption_scenes: list[dict[str, Any]] = []
    else:
        final_caption_cues = [cue for scene in caption_scenes for cue in list(scene.get("cues") or [])]
        final_caption_scenes = caption_scenes
    (workspace / "captions.json").write_text(
        json.dumps({
            "version": 2,
            "enabled": bool(captions_enabled),
            "mode": normalized_caption_mode if captions_enabled else "off",
            "timing_source": timing_source,
            "duration_sec": round(narration_duration, 4),
            "cues": final_caption_cues,
            "scenes": final_caption_scenes,
        }, indent=2),
        encoding="utf-8",
    )

    # 7. Cost / AC tracking.
    _, resolved_vm = resolve_video_model_chain(video_model=effective_video_model, tier=tier)
    ac_cost = ac_cost_for_video_model(video_model=resolved_vm, tier=tier)
    result = {
        "video_path": str(final),
        "script_path": str(workspace / "script.txt"),
        "narration_path": str(narration_audio),
        "final_audio_path": str(narration_clock),
        "captions_enabled": bool(captions_enabled),
        "caption_mode": normalized_caption_mode if captions_enabled else "off",
        "caption_timing_source": timing_source,
        "beats": [asdict(b) for b in beats],
        "tier": tier,
        "video_model": resolved_vm,
        "requested_video_model": requested_video_model or None,
        "video_model_migrated_from": (
            requested_video_model
            if requested_video_model and requested_video_model != resolved_vm
            else None
        ),
        "image_model": effective_image_model,
        "requested_image_model": requested_image_model or None,
        "image_model_migrated_from": (
            requested_image_model
            if requested_image_model and requested_image_model != effective_image_model
            else None
        ),
        "stills_model": "seedream_v45_edit_canonical",
        "cast_count": production_cast,
        "ac_charged": ac_cost,
        "category": category_key,
        "topic": topic,
        "cost": production_costs.load_summary(workspace),
    }
    (workspace / "result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    safe_topic = str(topic or cat["label"] or "Skeleton Short").strip()
    safe_topic_tag = re.sub(r"[^a-z0-9]+", "", safe_topic.lower())[:32] or "shorts"
    brand_tag = re.sub(r"[^a-z0-9]+", "", str(watermark_text or "").lower())[:32]
    timestamps: list[str] = []
    cursor = 0.0
    for beat in beats:
        mm = int(cursor // 60)
        ss = int(cursor % 60)
        label = re.sub(r"\s+", " ", beat.narration).strip()[:70].rstrip(" .,")
        timestamps.append(f"{mm:02d}:{ss:02d} - {label or f'Beat {beat.index + 1}'}")
        cursor += float(beat.duration_sec or 0)
    tags = list(dict.fromkeys([
        safe_topic_tag,
        category_key,
        "shorts",
        "youtube shorts",
        "ai video",
        "nyptid studio",
        brand_tag,
    ]))
    brand_hashtag = f" #{brand_tag}" if brand_tag else ""
    pkg = f"""Title:
{safe_topic}

Alternate Titles:
1. {safe_topic}
2. {safe_topic} | Full Story in 60 Seconds
3. The Part Everyone Missed About {safe_topic[:70]}

Description:
{safe_topic}

Watch the full story unfold in a fast, tightly edited short. Subscribe to {watermark_text} for more.

Timestamps:
{chr(10).join(timestamps) if timestamps else "00:00 - Full short"}

Tags:
{", ".join(t for t in tags if t)}

Hashtags:
#shorts #{safe_topic_tag} #nyptidstudio{brand_hashtag}

Thumbnail:
Not generated for short-form by default. Use the strongest frame/cover from the finished Short unless the user explicitly asks for a custom thumbnail.

CTA:
Subscribe for more.

"""
    (workspace / "package.txt").write_text(pkg, encoding="utf-8")
    return result
