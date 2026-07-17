"""
Styled shortform orchestrator — Seedream T2I per beat (no skeleton master).

Used when Studio Agent render_style is anything except skeleton_host.
"""
from __future__ import annotations

import json
import os
import re
import secrets
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

from studio_agent import production_costs
from studio_agent.image_model_catalog import (
    MODAL_SEEDREAM_MODEL_ID,
    is_seedream_model,
    normalize_seedream_model_id,
    seedream_provider,
)
from studio_agent.production_slots import production_slot
from studio_agent.render_styles import RenderStyle, get_render_style

from . import render_simulation
from .compose import concat_demuxer, mux_narration, probe_duration, trim_with_captions
from .i2v_engine import (
    I2VRouteChanged,
    ac_cost_for_video_model,
    generate as gen_clip,
    resolve_video_model_chain,
)
from .pipeline import Beat, _write_progress, apply_wardrobe_motion_lock, check_cancelled, split_script_into_beats
from .prompts.category_registry import get_category
from .scripting_grok import GrokClient, build_script_prompt
from .styled_stills import StyledStillError, build_styled_scene_prompt, generate_still_t2i, generate_still_xai_edit
from .voice_auto import AutoVoiceClient


def _slot_wait_progress(workspace: Path, stage: str, detail: str):
    def _on_wait(admission: Any) -> None:
        try:
            check_cancelled(workspace)
            _write_progress(
                workspace,
                stage=stage,
                progress=5,
                detail=(
                    f"{detail}: waiting for {admission.lane} slot "
                    f"#{admission.queue_position} ({admission.active}/{admission.limit} active)"
                ),
            )
        except Exception:
            pass

    return _on_wait


def _local_topic_label(topic: str | None, category_label: str = "") -> str:
    text = re.sub(r"\s+", " ", str(topic or category_label or "the topic")).strip()
    return text.strip(" .") or "the topic"


_FAL_SKELETON_IMAGE_MODELS = frozenset({
    "seedream45",
    "seedream_edit",
    "seedream_v4",
    "seedream_v5_lite",
    MODAL_SEEDREAM_MODEL_ID,
    "flux_lora_skeleton",
})


def _read_job_spec(workspace: Path) -> dict[str, Any]:
    try:
        loaded = json.loads((Path(workspace) / "job_spec.json").read_text(encoding="utf-8"))
        return dict(loaded) if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def _skeleton_image_model(sc: dict[str, Any] | None, workspace: Path) -> str:
    model = str((sc or {}).get("image_model_id") or "").strip().lower()
    if model:
        return model
    model = str(_read_job_spec(workspace).get("image_model_id") or "").strip().lower()
    if model:
        return model
    return "seedream_edit"


def _resolve_skeleton_master_reference(
    workspace: Path,
    reference_images: list[str] | None = None,
) -> str:
    """User-uploaded skeleton reference wins over the global canonical master."""
    workspace = Path(workspace)
    # RunPod stages the uploaded master into the current workspace. Prefer that
    # portable copy over any absolute API-host path persisted by an older job.
    ref_file = workspace / "reference.png"
    if ref_file.is_file() and ref_file.stat().st_size > 1024:
        return str(ref_file.resolve())

    def _usable(candidate: Any) -> str:
        value = str(candidate or "").strip()
        if not value:
            return ""
        if value.startswith("data:image/") or re.match(r"^[a-z][a-z0-9+.-]*://", value, re.I):
            return value
        try:
            path = Path(value)
            if not path.is_absolute():
                path = workspace / path
            if path.is_file():
                return str(path.resolve())
        except (OSError, ValueError):
            pass
        return ""

    for ref in list(reference_images or []):
        url = _usable(ref)
        if url:
            return url
    spec = _read_job_spec(workspace)
    for key in ("skeleton_reference_image", "reference_image"):
        url = _usable(spec.get(key))
        if url:
            return url
    meta_path = workspace / "skeleton_reference.json"
    if meta_path.is_file():
        try:
            data = json.loads(meta_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                url = _usable(data.get("reference_image_url") or data.get("public_url"))
                if url:
                    return url
        except Exception:
            pass
    return ""


def _persist_skeleton_reference(workspace: Path, reference_image: str) -> str:
    """Persist an uploaded skeleton reference into the job workspace."""
    source = str(reference_image or "").strip()
    if not source:
        return ""
    workspace = Path(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    ref_path = workspace / "reference.png"
    persisted_locally = False
    if source.startswith("data:image/"):
        import base64

        _, encoded = source.split(",", 1)
        ref_path.write_bytes(base64.b64decode(encoded))
        persisted_locally = True
    else:
        try:
            source_path = Path(source)
            source_is_file = source_path.is_file()
        except (OSError, ValueError):
            source_path = Path()
            source_is_file = False
        if source_is_file:
            if source_path.resolve() != ref_path.resolve():
                ref_path.write_bytes(source_path.read_bytes())
            persisted_locally = True
        elif (
            ref_path.is_file()
            and ref_path.stat().st_size > 1024
            and not re.match(r"^[a-z][a-z0-9+.-]*://", source, re.I)
        ):
            # A RunPod workspace can contain the staged upload while the
            # original API-host absolute path is intentionally inaccessible.
            persisted_locally = True

    portable_reference = "reference.png" if persisted_locally else source
    meta = {
        "reference_image_url": portable_reference,
        "public_url": portable_reference,
        "source_kind": "user_upload" if persisted_locally else "url",
    }
    (workspace / "skeleton_reference.json").write_text(
        json.dumps(meta, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    if persisted_locally:
        spec_path = workspace / "job_spec.json"
        spec = _read_job_spec(workspace)
        spec["reference_images"] = [portable_reference]
        spec["skeleton_reference_image"] = portable_reference
        spec_path.write_text(json.dumps(spec, indent=2, ensure_ascii=False), encoding="utf-8")
        return str(ref_path.resolve())
    return source


def _skeleton_prefers_xai(image_model_id: str) -> bool:
    model = str(image_model_id or "grok_imagine").strip().lower()
    return not (is_seedream_model(model) or model == "flux_lora_skeleton")


def _is_xai_availability_or_credit_error(exc: Exception) -> bool:
    """Allow paid-lane failover without hiding invalid credentials or bad prompts."""

    text = str(exc or "").strip().lower()
    provider = str(getattr(exc, "provider", "") or "").strip().lower()
    if provider != "xai" and not any(token in text for token in ("xai", "grok-imagine", "grok imagine")):
        return False
    credit_signal = any(
        phrase in text
        for phrase in (
            "used all available credits",
            "reached its monthly spending limit",
            "monthly spending limit",
            "credit balance",
            "insufficient credits",
            "insufficient balance",
            "billing limit",
            "spending limit",
        )
    )
    availability_signal = any(
        phrase in text
        for phrase in (
            " 429",
            " 500",
            " 502",
            " 503",
            " 504",
            "temporarily unavailable",
            "service unavailable",
            "provider unavailable",
            "upstream timeout",
            "timed out",
            "connection reset",
        )
    )
    return bool(
        (credit_signal and any(token in text for token in ("403", "permission-denied", "credit", "billing", "spending")))
        or availability_signal
    )


def _fal_image_fallback_model(value: str | None) -> str:
    """Return a concrete funded image lane; xAI aliases can never recurse."""

    model = normalize_seedream_model_id(str(value or "").strip().lower())
    return model if model and is_seedream_model(model) else "seedream_edit"


class MediaRouteChangedError(RuntimeError):
    """The creator changed the selected media route before fallback dispatch."""


def _require_current_image_fallback_route(
    fallback_guard: Callable[[], bool] | None,
) -> None:
    """Fail closed immediately before a secondary image-provider request."""

    if fallback_guard is None:
        return
    try:
        route_is_current = fallback_guard()
    except MediaRouteChangedError:
        raise
    except Exception as exc:
        raise MediaRouteChangedError(
            "Could not verify the current media route before image fallback"
        ) from exc
    if route_is_current is not True:
        raise MediaRouteChangedError(
            "Media route changed before image fallback dispatch"
        )


def _skeleton_video_model(sc: dict[str, Any] | None, workspace: Path) -> str:
    model = str((sc or {}).get("video_model") or "").strip().lower()
    if model:
        return model
    model = str(_read_job_spec(workspace).get("video_model") or "").strip().lower()
    if model:
        return model
    return "grok_imagine_video"


def _generate_skeleton_still_from_master(
    prompt: str,
    out_path: Path,
    *,
    image_model_id: str,
    seed: int | None = None,
    master_url: str = "",
    cast_count: int = 1,
) -> tuple[str, float, str, str, str]:
    """Render one skeleton still. Defaults to Seedream v4.5 edit from a locked reference."""
    from .canonical_edit import generate_still_edit, resolve_master_reference_local

    guarded_prompt = _xai_skeleton_artifact_guard(prompt)
    model = str(image_model_id or "seedream_edit").strip().lower()
    if is_seedream_model(model):
        model = normalize_seedream_model_id(model)
    resolved_master = str(master_url or "").strip()
    if not resolved_master:
        local_master = resolve_master_reference_local()
        if not local_master:
            raise RuntimeError(
                "skeleton reference image is missing; upload a skeleton reference or configure "
                "SKELETON_GLOBAL_REFERENCE_IMAGE_URL before rendering"
            )
        resolved_master = str(local_master)
    if _skeleton_prefers_xai(model):
        from .canonical_edit import _reference_url_to_local

        ref = _reference_url_to_local(resolved_master) or Path(resolved_master)
        if not (isinstance(ref, Path) and ref.is_file()):
            ref = resolve_master_reference_local(resolved_master)
        if not ref:
            raise RuntimeError("skeleton reference image is missing; cannot render safely")
        amount, note, key = production_costs.price_xai_image(model, edit=True)
        result = generate_still_xai_edit(
            guarded_prompt,
            out_path,
            reference_path=Path(ref),
            image_model_id=model,
        )
        if result.get("cost_usd") is not None:
            amount = production_costs._usd(result.get("cost_usd"))
        return guarded_prompt, amount, note, key, "xai"
    generate_still_edit(
        guarded_prompt,
        out_path,
        master_url=resolved_master,
        seed=int(seed if seed is not None else 420042),
        cast_count=int(cast_count or 1),
        image_model_id=model,
    )
    amount, note, key = production_costs.price_fal_image(edit=True, model_id=model)
    return guarded_prompt, amount, note, key, seedream_provider(model) or "fal"


def _audit_skeleton_still_for_generation(
    workspace: Path,
    still_path: Path,
    *,
    outfit: str,
    force: bool = False,
    cast_count: int = 1,
) -> dict[str, Any]:
    """Semantic frame-zero gate shared by initial generation and retries."""
    from studio_agent import visual_qa

    return visual_qa.audit_skeleton_still(
        still_path,
        reference=visual_qa._workspace_skeleton_reference(workspace),
        locked_outfit=outfit,
        force=force,
        cast_count=int(cast_count or 1),
    )


def _generate_skeleton_still_edit(
    prompt: str,
    out_path: Path,
    *,
    reference_path: Path,
    image_model_id: str,
) -> tuple[float, str, str, str]:
    """Edit an existing skeleton still. Defaults to xAI Grok Imagine image edit."""
    from .canonical_edit import generate_still_edit

    model = str(image_model_id or "grok_imagine").strip().lower()
    if _skeleton_prefers_xai(model):
        amount, note, key = production_costs.price_xai_image(model, edit=True)
        result = generate_still_xai_edit(
            prompt,
            out_path,
            reference_path=reference_path,
            image_model_id=model,
        )
        if result.get("cost_usd") is not None:
            amount = production_costs._usd(result.get("cost_usd"))
        return amount, note, key, "xai"
    if is_seedream_model(model):
        model = normalize_seedream_model_id(model)
    amount, note, key = production_costs.price_fal_image(edit=True, model_id=model)
    generate_still_edit(
        prompt,
        out_path,
        master_url=str(reference_path),
        image_model_id=model,
    )
    return amount, note, key, seedream_provider(model) or "fal"


def _local_script_fallback(topic: str | None, category_label: str = "") -> str:
    subject = _local_topic_label(topic, category_label)
    return (
        f"The real reason {subject} matters is not the obvious one. "
        f"Most people notice the behavior, but they miss the pattern underneath it. "
        f"At first, it looks random. Then it repeats in the same moments, around the same pressure, with the same emotional signal. "
        f"That is the part worth studying. The silence, the distance, or the sudden shift is usually not the whole story. "
        f"It is a protection strategy, a test, or a reaction to something the person does not know how to explain directly. "
        f"Once you see the pattern, you stop chasing the surface and start reading the cause. "
        f"And that is where the lesson is: behavior becomes predictable when you understand what it is trying to protect."
    )


def _local_plan_fallback(
    *,
    style: RenderStyle,
    category_label: str = "",
    topic: str | None = None,
    visual_brief: str | None = None,
) -> dict[str, Any]:
    subject = _local_topic_label(topic, category_label)
    plan = {
        "characters": {
            "main subject": (
                "photoreal adult subject in neutral modern clothing, expressive but restrained, "
                "consistent face and wardrobe across every scene"
            )
        },
        "topic_setting": (
            f"{style.label} vertical short about {subject}, cinematic interiors, moody contrast, "
            "symbolic relationship psychology visuals, no text in image"
        ),
        "fallback_outfit": "neutral dark jacket, simple shirt, clean modern styling",
        "local_fallback": True,
    }
    if visual_brief:
        plan["visual_brief_lock"] = visual_brief.strip()
    return plan


def _local_beat_visuals_styled(
    narration: str,
    category_label: str,
    *,
    style: RenderStyle,
    plan: dict | None = None,
    visual_brief: str | None = None,
) -> tuple[str, str, str]:
    plan = plan or {}
    fallback = str(plan.get("fallback_outfit") or "neutral dark jacket, simple shirt").strip()
    setting = str(plan.get("topic_setting") or f"{style.label} cinematic vertical short").strip()
    brief = str(visual_brief or plan.get("visual_brief_lock") or "").strip()
    beat = re.sub(r"\s+", " ", str(narration or category_label or "the idea")).strip()
    action_parts = [
        setting,
        f"Visualize this narration beat: {beat}",
        brief,
        "single clear subject, expressive body language, cinematic 9:16 framing, no text, no logos",
    ]
    action = ". ".join(part.strip(" .") for part in action_parts if part).strip() + "."
    motion = "slow controlled camera push, subtle subject movement, restrained emotional tension"
    return fallback, action, motion


def _derive_short_sfx_prompt(scene: dict[str, Any], *, sound_design_brief: str = "") -> str:
    explicit = str(scene.get("sfx_direction") or "").strip()
    action = str(scene.get("scene_action") or scene.get("prompt") or "").strip()
    narration = str(scene.get("narration") or "").strip()
    brief = str(sound_design_brief or "").strip()
    parts = [
        explicit,
        brief,
        f"Visual beat: {action}" if action else "",
        f"Narration beat: {narration}" if narration else "",
        "Create subtle short-form sound design only: ambience, soft risers, whooshes, reveal hits, and clean transition accents. No vocals, no distracting melody, do not overpower narration.",
    ]
    return " ".join(part for part in parts if part).strip()[:900]


def _generate_short_audio_bed(prompt: str, duration_sec: float, out_path: Path) -> Path | None:
    out_path = Path(out_path)
    if out_path.exists() and out_path.stat().st_size > 1024:
        return out_path
    if render_simulation.enabled():
        return render_simulation.write_audio(out_path, duration_sec=duration_sec)
    try:
        from long_form.pipeline import MMAUDIO_URL, _download, _fal_post

        data = _fal_post(
            MMAUDIO_URL,
            {"prompt": prompt, "duration": int(max(2, min(30, float(duration_sec or 0))))},
            timeout_s=180,
        )
        url = (data.get("audio") or {}).get("url") or data.get("audio_url")
        if not url:
            return None
        _download(url, out_path, timeout_s=120)
        return out_path if out_path.exists() and out_path.stat().st_size > 1024 else None
    except Exception:
        return None


def _metered_provider_values(provider: str, amount: float, note: str) -> tuple[str, float, str]:
    if render_simulation.enabled():
        return "simulation", 0.0, f"simulation: {note}"
    return provider, float(amount or 0.0), note


def _xai_moderation_retry_prompt(prompt: str) -> str:
    text = str(prompt or "")
    softeners = (
        ("self-sabotage", "emotional conflict"),
        ("Self-Sabotage", "Emotional Conflict"),
        ("sabotage", "avoidance pattern"),
        ("fall in love", "develop attachment"),
        ("Fall in Love", "Develop Attachment"),
        ("hidden threat", "hidden fear"),
        ("pain", "inner tension"),
        ("shame", "quiet regret"),
        ("broken", "overwhelmed"),
        ("wound", "emotional pressure"),
    )
    for src, dst in softeners:
        text = text.replace(src, dst)
    guard = (
        " Safe PG-13 metaphorical psychology scene. No gore, no injury, no self-harm, "
        "no violence, no blood, no explicit distress, no medical procedure, no readable text."
    )
    return (text + guard)[:759]


def _xai_skeleton_artifact_guard(prompt: str) -> str:
    """Keep the scene-first prompt intact at the final provider boundary.

    The old implementation prepended ~2k characters and then truncated the
    combined payload. That could delete the location and wardrobe immediately
    before xAI/FAL received it, even when the upstream composer was correct.
    """
    from skeleton_ai.prompt_compose import compact_identity_locks, compose_priority_prompt

    provider_lock = compact_identity_locks()
    primary = re.sub(r"\s+", " ", str(prompt or "")).strip()
    # Canonical composer already includes the compact reference contract. Do
    # not append another provider lock and bury the actual scene direction.
    if "EDIT THE CANONICAL REFERENCE" in primary.upper() or "PRIMARY EDIT" in primary.upper():
        return compose_priority_prompt(primary=primary, budget=300)
    if len(primary) > 300:
        primary = primary[:299].rsplit(" ", 1)[0] + "…"
    return compose_priority_prompt(
        primary=primary,
        secondary=provider_lock,
        tertiary="",
        budget=300,
    )


def _hand_guard_retry_prompt(prompt: str) -> str:
    from skeleton_ai.prompt_compose import compose_priority_prompt

    hosts = 2 if re.search(r"(?i)\b(?:exactly\s+)?two\s+(?:identical\s+)?skeleton", str(prompt or "")) else 1
    secondary = (
        "REPAIR LOCK: exactly TWO identical full-frame skeleton hosts in one continuous scene, "
        "four attached arms, four attached hands, five fingers per hand, four attached legs, "
        "and four attached feet. No third body, comparison layout, floating hand, merged limb, "
        "or extra anatomy."
        if hosts >= 2
        else (
            "REPAIR LOCK: exactly one full-frame skeleton host, two attached arms, two attached hands, "
            "five fingers per hand, two attached legs, and two attached feet. No duplicate body, comparison "
            "layout, floating hand, merged limb, or extra anatomy."
        )
    )
    return compose_priority_prompt(
        primary=str(prompt or "").strip(),
        secondary=secondary,
        tertiary="",
        budget=300,
    )


def _concat_audio_tracks(paths: list[Path], out_path: Path) -> Path | None:
    valid = [Path(p) for p in paths if Path(p).exists() and Path(p).stat().st_size > 1024]
    if not valid:
        return None
    out_path = Path(out_path)
    cmd = ["ffmpeg", "-y", "-loglevel", "error"]
    for path in valid:
        cmd.extend(["-i", str(path)])
    concat_inputs = "".join(f"[{i}:a:0]" for i in range(len(valid)))
    cmd.extend([
        "-filter_complex",
        f"{concat_inputs}concat=n={len(valid)}:v=0:a=1[aout]",
        "-map", "[aout]",
        "-c:a", "libmp3lame",
        "-q:a", "2",
        str(out_path),
    ])
    try:
        subprocess.run(cmd, check=True, capture_output=True)
    except Exception:
        return None
    return out_path if out_path.exists() and out_path.stat().st_size > 1024 else None


def _mix_short_sound_design(
    narration_audio: Path,
    *,
    sfx_track: Path | None = None,
    bgm_track: Path | None = None,
    out_path: Path,
    sfx_gain: float = 0.16,
    bgm_gain: float = 0.08,
) -> Path:
    inputs = [Path(narration_audio)]
    labels = ["[0:a]volume=1.0[voice]"]
    mix_parts = ["[voice]"]
    if sfx_track and Path(sfx_track).exists() and Path(sfx_track).stat().st_size > 1024:
        inputs.append(Path(sfx_track))
        labels.append(f"[{len(inputs) - 1}:a]volume={max(0.0, float(sfx_gain or 0.16)):.3f}[sfx]")
        mix_parts.append("[sfx]")
    if bgm_track and Path(bgm_track).exists() and Path(bgm_track).stat().st_size > 1024:
        inputs.append(Path(bgm_track))
        labels.append(f"[{len(inputs) - 1}:a]volume={max(0.0, float(bgm_gain or 0.08)):.3f}[bgm]")
        mix_parts.append("[bgm]")
    if len(inputs) == 1:
        return Path(narration_audio)
    out_path = Path(out_path)
    cmd = ["ffmpeg", "-y", "-loglevel", "error"]
    for path in inputs:
        cmd.extend(["-i", str(path)])
    cmd.extend([
        "-filter_complex",
        ";".join(labels) + ";" + "".join(mix_parts) + f"amix=inputs={len(mix_parts)}:duration=first:dropout_transition=1,apad=pad_dur=0.4[aout]",
        "-map", "[aout]",
        "-c:a", "libmp3lame",
        "-q:a", "2",
        str(out_path),
    ])
    subprocess.run(cmd, check=True, capture_output=True)
    return out_path if out_path.exists() and out_path.stat().st_size > 1024 else Path(narration_audio)


def _default_scene_sfx_direction(narration: str, scene_action: str, *, index: int, total: int) -> str:
    low = f"{narration} {scene_action}".lower()
    if index == 0:
        return "tight hook accent, subtle riser, quick attention-grab hit under the first phrase"
    if index >= max(0, total - 2):
        return "clean payoff resolve, soft impact hit, subtle tail-out for the final idea"
    if any(word in low for word in ("reveal", "truth", "secret", "real reason", "hidden")):
        return "controlled reveal riser and low cinematic hit timed to the insight"
    if any(word in low for word in ("fear", "danger", "wall", "sabotage", "ignore", "dark")):
        return "dark ambient pulse, faint tension texture, restrained whoosh transition"
    return "subtle ambient texture with a clean transition whoosh, kept below narration"


def _plan_system(style: RenderStyle) -> str:
    return (
        f"You are the visual planner for a short-form YouTube video rendered in "
        f"{style.label} style.\n\n"
        "Characters are REAL subjects for the scene (humans, historical figures, "
        "fictional characters, objects) — NOT a recurring skeleton mascot unless "
        "the script explicitly calls for one.\n\n"
        "Given a narration script, list recurring subjects or roles and lock ONE "
        "outfit/appearance description per named subject for continuity across beats.\n\n"
        "Output strict JSON:\n"
        "  {\n"
        '    "characters": { "<subject>": "<appearance + outfit, ~20-35 words>" },\n'
        '    "topic_setting": "<environment / era / lighting in one sentence>",\n'
        '    "fallback_outfit": "<default look when no named subject>"\n'
        "  }\n"
        "No markdown fences."
    )


def analyze_script_styled(
    grok: GrokClient,
    script_text: str,
    *,
    style: RenderStyle,
    category_label: str = "",
    topic: str | None = None,
    visual_brief: str | None = None,
) -> dict:
    user_lines = [
        f"Render style: {style.label}",
        f"Style notes: {style.prompt_prefix[:400]}",
    ]
    if category_label:
        user_lines.append(f"Category: {category_label}")
    if topic:
        user_lines.append(f"Topic: {topic}")
    if visual_brief:
        user_lines.append(f"USER VISUAL LOCK: {visual_brief}")
    user_lines.append("Script:")
    user_lines.append(script_text.strip())
    user_lines.append("\nReturn JSON now.")
    try:
        raw = grok.complete(_plan_system(style), "\n".join(user_lines), max_tokens=900, temperature=0.5)
    except Exception:
        return _local_plan_fallback(
            style=style,
            category_label=category_label,
            topic=topic,
            visual_brief=visual_brief,
        )
    raw = raw.strip().strip("`").strip()
    if raw.lower().startswith("json"):
        raw = raw[4:].strip()
    try:
        plan = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        plan = {}
    # Providers occasionally return the valid JSON literal `null`.  It is not
    # a usable scene plan; normalize it before any .get() calls below.
    if not isinstance(plan, dict):
        plan = {}
    if not isinstance(plan.get("characters"), dict):
        plan["characters"] = {}
    plan.setdefault("fallback_outfit", "period-appropriate clothing matching the topic")
    plan.setdefault("topic_setting", "")
    if visual_brief:
        plan["visual_brief_lock"] = visual_brief.strip()
    return plan


def derive_beat_visuals_styled(
    grok: GrokClient,
    narration: str,
    category_label: str,
    *,
    style: RenderStyle,
    plan: dict | None = None,
    visual_brief: str | None = None,
) -> tuple[str, str, str]:
    plan = plan or {"characters": {}, "fallback_outfit": "topic-appropriate wardrobe"}
    chars_json = json.dumps(plan.get("characters", {}), ensure_ascii=False)
    setting = plan.get("topic_setting", "")
    fallback = plan.get("fallback_outfit", "")
    vbl = (visual_brief or plan.get("visual_brief_lock") or "").strip()

    sys = (
        f"You compose ONE scene still prompt for a {style.label} short.\n"
        "Describe WHO is in frame, WHAT they wear, WHERE they are, and the POSE/action.\n"
        "Use real characters appropriate to the narration — historical figures as humans, "
        "comic characters in full costume, etc.\n"
        "Never describe an anatomical skeleton host unless narration demands it.\n"
        "Output JSON: {\"outfit\": \"...\", \"scene_action\": \"...\", \"motion_prompt\": \"...\"}"
    )
    user = (
        f"Style: {style.label}\n"
        f"Category: {category_label}\n"
        f"Narration beat: {narration}\n"
        f"Locked characters: {chars_json}\n"
        f"Setting: {setting}\n"
        f"Fallback look: {fallback}\n"
    )
    if vbl:
        user += f"Visual lock: {vbl}\n"

    try:
        raw = grok.complete(sys, user, max_tokens=500, temperature=0.55)
    except Exception:
        return _local_beat_visuals_styled(
            narration,
            category_label,
            style=style,
            plan=plan,
            visual_brief=visual_brief,
        )
    raw = raw.strip().strip("`").strip()
    if raw.lower().startswith("json"):
        raw = raw[4:].strip()
    try:
        data = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return _local_beat_visuals_styled(
            narration,
            category_label,
            style=style,
            plan=plan,
            visual_brief=visual_brief,
        )
    # A provider may return syntactically valid JSON such as `null` instead of
    # the requested object. Treat that as a normal provider miss, not a fatal
    # production error.
    if not isinstance(data, dict):
        return _local_beat_visuals_styled(
            narration,
            category_label,
            style=style,
            plan=plan,
            visual_brief=visual_brief,
        )
    outfit = str(data.get("outfit") or fallback or "").strip()
    action = str(data.get("scene_action") or narration).strip()
    motion = str(data.get("motion_prompt") or action).strip()
    return outfit, action, motion


def _still_to_clip(
    still_path: Path,
    out_path: Path,
    *,
    duration_sec: float,
    fps: int = 30,
) -> Path:
    """Non-animated path: turn a still into a clip with a gentle Ken Burns push.

    Used when the user turns OFF motion (animate=False) — no i2v cost, but still
    a premium documentary feel instead of a dead static frame.
    """
    dur = max(1.0, float(duration_sec))
    frames = int(dur * fps)
    vf = (
        "scale=1080:1920:force_original_aspect_ratio=increase,"
        "crop=1080:1920,"
        f"zoompan=z='min(zoom+0.0009,1.12)':d={frames}:s=1080x1920:fps={fps}"
    )
    subprocess.run(
        [
            "ffmpeg", "-y", "-loop", "1", "-i", str(still_path),
            "-t", f"{dur:.3f}", "-vf", vf,
            "-c:v", "libx264", "-pix_fmt", "yuv420p", "-r", str(fps),
            str(out_path),
        ],
        check=True,
        capture_output=True,
    )
    return out_path


SCENES_FILE = "scenes.json"


def _scenes_path(workspace: Path) -> Path:
    return Path(workspace) / SCENES_FILE


def load_scenes(workspace: Path) -> list[dict[str, Any]]:
    p = _scenes_path(workspace)
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    return data if isinstance(data, list) else []


def save_scenes(workspace: Path, scenes: list[dict[str, Any]]) -> None:
    _scenes_path(workspace).write_text(
        json.dumps(scenes, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def _write_result(workspace: Path, payload: dict[str, Any]) -> None:
    (Path(workspace) / "result.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def _read_result(workspace: Path) -> dict[str, Any]:
    p = Path(workspace) / "result.json"
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _setup_dirs(workspace: Path) -> tuple[Path, Path, Path, Path]:
    workspace = Path(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    stills_dir = workspace / "stills"
    clips_dir = workspace / "clips"
    trimmed_dir = workspace / "trimmed"
    work_dir = workspace / "work"
    for d in (stills_dir, clips_dir, trimmed_dir, work_dir):
        d.mkdir(parents=True, exist_ok=True)
    return stills_dir, clips_dir, trimmed_dir, work_dir


def _style_for(workspace: Path, fallback: str = "cinematic") -> RenderStyle:
    # result.json is a mutable progress/status record and completed jobs often
    # omit render_style.  job_spec.json is the durable production contract.
    # Falling straight to "cinematic" here sent a finished skeleton job into
    # the generic Grok lane, where an intentionally blank stale prompt could
    # be submitted to the provider.
    result = _read_result(workspace)
    spec = _read_job_spec(workspace)
    key = str(result.get("render_style") or spec.get("render_style") or fallback)
    return get_render_style(key)


# ─── Stage 1: plan + per-scene stills → scene-review gate ──────────────────────

def _preserved_scene_for_replan(
    index: int,
    previous: dict[str, Any] | None,
    preserve_indices: set[int],
) -> dict[str, Any] | None:
    """Return an untouched approved asset row when a replan must lock it.

    Expansion replans beats for the larger short, but the approved proof's
    still, clip, QA, approvals, duration, and prompts are creator-owned state.
    Reconstructing that row used to clear ``clip_rel`` and reanimate Scene 1.
    """
    if index not in preserve_indices or not isinstance(previous, dict) or not previous:
        return None
    if not (previous.get("still_rel") or previous.get("clip_rel")):
        return None
    preserved = dict(previous)
    preserved["index"] = int(index)
    preserved.setdefault("sid", f"b{int(index):02d}")
    return preserved


def plan_scenes(
    category_key: str,
    topic: str | None,
    workspace: Path,
    *,
    render_style: str,
    tier: str = "standard",
    image_model_id: str | None = None,
    video_model: str | None = None,
    visual_brief: str | None = None,
    beats_target: int = 12,
    grok: GrokClient | None = None,
    script_override: str | None = None,
    user_id: str | None = None,
    default_animate: bool = True,
    reference_images: list[str] | None = None,
    sound_design_brief: str | None = None,
) -> dict[str, Any]:
    """Write the script, plan beats, render one Seedream still per scene, then
    stop at the awaiting_scene_review gate so the user can edit/animate per scene."""
    style = get_render_style(render_style)
    is_skeleton = style.pipeline == "skeleton_host"
    workspace = Path(workspace)
    stills_dir, _clips, _trim, _work = _setup_dirs(workspace)
    grok = grok or GrokClient()
    cat = get_category(category_key, user_id=user_id)

    _write_progress(workspace, stage="script", progress=8, detail="Writing script")
    existing_script = workspace / "script.txt"
    if script_override and script_override.strip():
        script_text = script_override.strip()
    elif existing_script.exists() and existing_script.read_text(encoding="utf-8").strip():
        script_text = existing_script.read_text(encoding="utf-8").strip()
    else:
        user_prompt = build_script_prompt(cat["system_prompt"], topic)
        try:
            script_text = grok.complete(cat["system_prompt"], user_prompt, max_tokens=1500)
        except Exception:
            script_text = _local_script_fallback(topic, cat["label"])
    (workspace / "script.txt").write_text(script_text, encoding="utf-8")
    _write_progress(workspace, stage="script_ready", progress=12, detail="Script ready")

    if is_skeleton:
        from studio_agent.catalyst_health import ensure_catalyst_skeleton_learning_ready

        _write_progress(workspace, stage="character_lock", progress=14, detail="Loading skeleton identity and Catalyst memory")
        ensure_catalyst_skeleton_learning_ready()
    _write_progress(workspace, stage="scene_plan", progress=16, detail=f"Planning {style.label} scenes")
    if is_skeleton:
        from .pipeline import analyze_script

        plan = analyze_script(
            grok,
            script_text,
            category_label=cat["label"],
            topic=topic,
            visual_brief=visual_brief,
        )
    else:
        plan = analyze_script_styled(
            grok, script_text, style=style, category_label=cat["label"],
            topic=topic, visual_brief=visual_brief,
        )
    # Provider/model malformed output must never crash the production worker.
    # The deterministic visual derivation below has safe defaults when the
    # planner is unavailable.
    if not isinstance(plan, dict):
        plan = {}
    (workspace / "scene_plan.json").write_text(
        json.dumps({**plan, "render_style": style.key}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    sentences = split_script_into_beats(script_text, target_count=beats_target)
    if not sentences:
        raise RuntimeError("Grok returned empty script")

    _, resolved_default_vm = resolve_video_model_chain(video_model=video_model, tier=tier)
    selected_image_model = str(image_model_id or "").strip().lower()
    if is_skeleton and not selected_image_model:
        selected_image_model = "seedream_edit"
    skeleton_master_ref = ""
    if is_skeleton:
        if reference_images:
            skeleton_master_ref = _persist_skeleton_reference(workspace, str(reference_images[0] or ""))
        else:
            skeleton_master_ref = _resolve_skeleton_master_reference(workspace, reference_images)
    resolved_video_model = str(video_model or "").strip().lower()
    if is_skeleton and not resolved_video_model:
        resolved_video_model = "grok_imagine_video"
    existing = {s.get("index"): s for s in load_scenes(workspace)}
    replan_spec = _read_job_spec(workspace)
    raw_preserve_indices = replan_spec.get("preserve_scene_indices")
    preserve_indices = {
        int(value)
        for value in raw_preserve_indices
        if str(value).strip().lstrip("-").isdigit()
    } if isinstance(raw_preserve_indices, list) else set()
    scenes: list[dict[str, Any]] = []
    total = max(len(sentences), 1)
    for i, narration in enumerate(sentences):
        check_cancelled(workspace)
        _write_progress(workspace, stage="stills", progress=20 + int(i / total * 60),
                        detail=f"Scene {i + 1}/{total} — {style.label} still")
        prev = existing.get(i, {})
        preserved = _preserved_scene_for_replan(i, prev, preserve_indices)
        if preserved is not None:
            scenes.append(preserved)
            save_scenes(workspace, scenes)
            continue
        prompt = prev.get("prompt")
        outfit = prev.get("outfit")
        action = prev.get("scene_action")
        hosts = 1
        if is_skeleton:
            from skeleton_ai.prompt_compose import resolve_cast_count

            hosts = resolve_cast_count(
                job_cast=_read_job_spec(workspace).get("cast_count"),
                scene_cast=prev.get("cast_count"),
                topic=str(topic or cat["label"] or ""),
                visual_brief=str(visual_brief or ""),
                narration=str(narration or ""),
                scene_action=str(action or ""),
            )
        motion = prev.get("motion_prompt")
        risky_visual = False
        if not prompt:
            if is_skeleton:
                from .canonical_edit import build_scene_edit_prompt, sanitize_skeleton_scene_action
                from .pipeline import derive_beat_visuals
                from skeleton_ai.prompt_compose import (
                    compose_skeleton_motion_prompt,
                    resolve_locked_outfit,
                )

                outfit, action, motion = derive_beat_visuals(
                    grok,
                    narration,
                    cat["label"],
                    plan=plan,
                    visual_brief=visual_brief,
                    beat_index=i,
                    cast_count=hosts,
                )
                action, risky_visual = sanitize_skeleton_scene_action(
                    str(action or ""),
                    topic=str(topic or cat["label"] or ""),
                    visual_brief=str(visual_brief or ""),
                    narration=str(narration or ""),
                    cast_count=hosts,
                )
                outfit = resolve_locked_outfit(
                    job_locked=_job_locked_outfit(workspace),
                    scene_outfit=str(outfit or ""),
                    topic=str(topic or cat["label"] or ""),
                    force_simple_host=True,
                )
                if i == 0:
                    outfit = _persist_job_locked_outfit(workspace, outfit)
                else:
                    outfit = _job_locked_outfit(workspace) or outfit
                motion = compose_skeleton_motion_prompt(
                    motion=str(motion or narration or "subtle idle motion"),
                    locked_outfit=outfit,
                    cast_count=hosts,
                )
                catalyst = ""
                try:
                    from studio_agent.catalyst_skeleton_reference import (
                        catalyst_block_for_compose,
                        resolve_channel_key,
                    )

                    catalyst = catalyst_block_for_compose(resolve_channel_key(workspace))
                except Exception:
                    catalyst = ""
                prompt = build_scene_edit_prompt(
                    topic=topic or cat["label"],
                    visual_description=action,
                    outfit=outfit,
                    visual_brief=str(visual_brief or ""),
                    narration=str(narration or ""),
                    catalyst_block=catalyst,
                    cast_count=hosts,
                )
            else:
                outfit, action, motion = derive_beat_visuals_styled(
                    grok, narration, cat["label"], style=style, plan=plan, visual_brief=visual_brief,
                )
                prompt = build_styled_scene_prompt(
                    style_prefix=style.prompt_prefix, scene_action=action, outfit=outfit,
                    topic=topic or cat["label"], visual_brief=visual_brief or "",
                )
        elif is_skeleton:
            # Existing planner prompts often carry stale multi-cast wardrobe.
            # Always re-lock outfit + motion to the job wardrobe source of truth.
            from skeleton_ai.prompt_compose import (
                compose_skeleton_motion_prompt,
                resolve_locked_outfit,
            )

            outfit = resolve_locked_outfit(
                job_locked=_job_locked_outfit(workspace),
                scene_outfit=str(outfit or ""),
                topic=str(topic or cat["label"] or ""),
                force_simple_host=True,
            )
            if i == 0 or not _job_locked_outfit(workspace):
                outfit = _persist_job_locked_outfit(workspace, outfit)
            else:
                outfit = _job_locked_outfit(workspace) or outfit
            motion = compose_skeleton_motion_prompt(
                motion=str(motion or narration or "subtle idle motion"),
                locked_outfit=outfit,
            )
            # Rebuild prompt if it is guardrail-first or missing wardrobe lock.
            pl = str(prompt or "")
            if (
                re.match(r"^\s*(CATALYST|CHANNEL NOTES|MR SKELEWELLY CANONICAL)", pl, re.I)
                or "WARDROBE" not in pl.upper()
                or "PRIMARY EDIT" not in pl.upper()
                or "black void" in pl.lower()
            ):
                prompt = _rebuild_skeleton_scene_prompt(
                    workspace,
                    {
                        "index": i,
                        "scene_action": action or prev.get("scene_action"),
                        "outfit": outfit,
                        "motion_prompt": motion,
                        "narration": narration,
                        "topic": topic or cat["label"],
                        "visual_brief": visual_brief,
                    },
                )

        sid = f"b{i:02d}"
        sfx_direction = str(prev.get("sfx_direction") or "").strip()
        if not sfx_direction:
            sfx_direction = _default_scene_sfx_direction(
                narration,
                str(action or prompt or ""),
                index=i,
                total=total,
            )
        if sound_design_brief:
            sfx_direction = f"{str(sound_design_brief).strip()} {sfx_direction}".strip()
        from studio_agent.visual_treatment import choose_visual_treatment
        _job_spec_vt = _read_job_spec(workspace)
        visual_treatment = choose_visual_treatment(
            str(narration or ""), index=i, total=total,
            channel_key=str(_job_spec_vt.get("channel_key") or ""),
            skeleton_host=is_skeleton,
            motion_graphics_requested=bool(_job_spec_vt.get("motion_graphics_requested")),
        )
        if is_skeleton:
            from skeleton_ai.prompt_compose import compose_skeleton_motion_prompt
            motion = compose_skeleton_motion_prompt(
                motion=str(motion or narration or "subtle idle motion"),
                locked_outfit=str(outfit or ""),
                effect_direction=str(visual_treatment.get("motion_effect") or ""),
                cast_count=hosts,
            )
        still_target = stills_dir / f"{sid}.png"
        if str(visual_treatment.get("kind") or "") == "motion_graphic" and not still_target.exists():
            from studio_agent.visual_treatment import render_motion_graphic_clip
            preview = workspace / "motion_graphics" / f"preview_{sid}.mp4"
            render_motion_graphic_clip(
                visual_treatment, preview, duration_sec=4.0, fps=24,
                width=1080, height=1920,
            )
            extracted = subprocess.run(
                ["ffmpeg", "-hide_banner", "-loglevel", "warning", "-y", "-ss", "2.0", "-i", str(preview), "-frames:v", "1", str(still_target)],
                capture_output=True, text=True,
            )
            if extracted.returncode != 0:
                raise RuntimeError(f"motion-graphic preview extraction failed: {extracted.stderr[-300:]}")
        if not (still_target.exists() and still_target.stat().st_size > 0):
            provider_name = "fal"
            with production_slot(
                "stills",
                on_wait=_slot_wait_progress(workspace, "stills_queue", f"Scene {i + 1} still"),
            ):
                if is_skeleton:
                    provider_name = (
                        seedream_provider(selected_image_model) or "fal"
                        if not _skeleton_prefers_xai(selected_image_model)
                        else "xai"
                    )
                    try:
                        prompt, amount, note, key, provider_name = _generate_skeleton_still_from_master(
                            prompt,
                            still_target,
                            image_model_id=selected_image_model,
                            seed=420042 + i,
                            master_url=skeleton_master_ref,
                            cast_count=hosts,
                        )
                    except StyledStillError as exc:
                        charged = exc.cost_usd if exc.cost_usd is not None else 0.0
                        if charged:
                            err_provider, err_amount, err_note = _metered_provider_values(
                                "xai",
                                charged,
                                "xai skeleton still: failed request",
                            )
                            production_costs.record_event(
                                workspace,
                                stage="stills",
                                provider=err_provider,
                                operation="grok_imagine_edit",
                                usd=err_amount,
                                quantity=1,
                                unit="image",
                                scene_index=i,
                                metadata={"pricing_note": err_note, "cached": False, "failed": True},
                            )
                        if "content moderation" not in str(exc).lower():
                            raise
                        retry_prompt = _xai_moderation_retry_prompt(_xai_skeleton_artifact_guard(prompt))
                        still_result = generate_still_t2i(
                            retry_prompt,
                            still_target,
                            negative_prompt=(
                                "human skin, muscle tissue, nudity, gore, blood, injury, self-harm, "
                                "violence, deformed anatomy, extra limbs, text, watermark"
                            ),
                            seed=520042 + i,
                            image_model_id=selected_image_model,
                        )
                        prompt = retry_prompt
                        amount, note, key = production_costs.price_xai_image(selected_image_model)
                        if still_result.get("cost_usd") is not None:
                            amount = production_costs._usd(still_result.get("cost_usd"))
                else:
                    if reference_images:
                        from .styled_stills import generate_still_reference_edit

                        generate_still_reference_edit(
                            prompt,
                            still_target,
                            reference_paths=reference_images,
                            negative_prompt=style.negative_prompt,
                            seed=420042 + i,
                        )
                        amount, note, key = production_costs.price_fal_image(
                            edit=True,
                            model_id=selected_image_model,
                        )
                    else:
                        if selected_image_model in {"grok_imagine", "grok_imagine_standard"}:
                            provider_name = "xai"
                            amount, note, key = production_costs.price_xai_image(selected_image_model)
                        else:
                            amount, note, key = production_costs.price_fal_image(
                                edit=False,
                                model_id=selected_image_model,
                            )
                        try:
                            still_result = generate_still_t2i(
                                prompt,
                                still_target,
                                negative_prompt=style.negative_prompt,
                                seed=420042 + i,
                                image_model_id=selected_image_model,
                            )
                            if provider_name == "xai" and still_result.get("cost_usd") is not None:
                                amount = production_costs._usd(still_result.get("cost_usd"))
                        except StyledStillError as exc:
                            charged = exc.cost_usd if exc.cost_usd is not None else 0.0
                            if provider_name == "xai" and charged:
                                err_provider, err_amount, err_note = _metered_provider_values(
                                    "xai",
                                    charged,
                                    f"{note}: failed request",
                                )
                                production_costs.record_event(
                                    workspace,
                                    stage="stills",
                                    provider=err_provider,
                                    operation=key,
                                    usd=err_amount,
                                    quantity=1,
                                    unit="image",
                                    scene_index=i,
                                    metadata={"pricing_note": err_note, "cached": False, "failed": True},
                                )
                            if provider_name != "xai" or "content moderation" not in str(exc).lower():
                                raise
                            retry_prompt = _xai_moderation_retry_prompt(prompt)
                            still_result = generate_still_t2i(
                                retry_prompt,
                                still_target,
                                negative_prompt=style.negative_prompt,
                                seed=520042 + i,
                                image_model_id=selected_image_model,
                            )
                            prompt = retry_prompt
                            if still_result.get("cost_usd") is not None:
                                amount = production_costs._usd(still_result.get("cost_usd"))
            provider, amount, note = _metered_provider_values(provider_name, amount, note)
            production_costs.record_event(
                workspace,
                stage="stills",
                provider=provider,
                operation=key,
                usd=amount,
                quantity=1,
                unit="image",
                scene_index=i,
                metadata={"pricing_note": note, "cached": False},
            )
        still_qa: dict[str, Any] | None = None
        if is_skeleton and still_target.is_file() and str(visual_treatment.get("kind") or "") != "motion_graphic":
            still_qa = _audit_skeleton_still_for_generation(
                workspace,
                still_target,
                outfit=str(outfit or ""),
                cast_count=hosts,
            ) or {"status": "fail", "pass": False, "issues": ["qa_unavailable"], "summary": "Still QA returned no report"}
            if still_qa.get("status") != "pass" and "qa_unavailable" not in list(still_qa.get("issues") or []):
                # One bounded recovery: quarantine the rejected frame, then retry
                # through the creator-selected identity-preserving model.
                rejected_dir = workspace / "rejected_stills"
                rejected_dir.mkdir(parents=True, exist_ok=True)
                rejected = rejected_dir / f"{sid}_attempt1.png"
                rejected.unlink(missing_ok=True)
                still_target.replace(rejected)
                retry_prompt, retry_amount, retry_note, retry_key, retry_provider = (
                    _generate_skeleton_still_from_master(
                        prompt,
                        still_target,
                        image_model_id=selected_image_model or "seedream_edit",
                        seed=720042 + i,
                        master_url=skeleton_master_ref,
                        cast_count=hosts,
                    )
                )
                prompt = retry_prompt
                retry_provider, retry_amount, retry_note = _metered_provider_values(
                    retry_provider, retry_amount, retry_note
                )
                production_costs.record_event(
                    workspace,
                    stage="stills_qa_retry",
                    provider=retry_provider,
                    operation=retry_key,
                    usd=retry_amount,
                    quantity=1,
                    unit="image",
                    scene_index=i,
                    metadata={
                        "pricing_note": retry_note,
                        "cached": False,
                        "semantic_qa_retry": True,
                        "rejected_still": str(rejected.relative_to(workspace)),
                    },
                )
                still_qa = _audit_skeleton_still_for_generation(
                    workspace,
                    still_target,
                    outfit=str(outfit or ""),
                    force=True,
                    cast_count=hosts,
                )
        elif still_target.is_file() and str(visual_treatment.get("kind") or "") != "motion_graphic":
            from studio_agent.visual_qa import audit_generic_still
            still_qa = audit_generic_still(
                still_target,
                scene_contract=" ".join(str(value or "") for value in (prompt, action, narration)),
            )
        still_qa_passed = (
            str(visual_treatment.get("kind") or "") == "motion_graphic"
            or bool(still_qa and still_qa.get("status") == "pass" and still_qa.get("pass") is True)
        )
        scenes.append({
            "index": i, "sid": sid, "narration": narration, "prompt": prompt,
            "outfit": outfit, "scene_action": action,
            "cast_count": hosts if is_skeleton else 1,
            "motion_prompt": apply_wardrobe_motion_lock(motion, outfit) if is_skeleton else motion,
            "sfx_direction": sfx_direction,
            "still_rel": f"stills/{sid}.png", "clip_rel": None,
            "animate": bool(prev.get("animate", default_animate)) if still_qa_passed else False,
            "approved_for_video": bool(prev.get("approved_for_video", False)) if still_qa_passed else False,
            "approved_for_animation": bool(prev.get("approved_for_animation", False)) if still_qa_passed else False,
            "video_model": prev.get("video_model") or resolved_video_model or resolved_default_vm,
            "image_model_id": selected_image_model or prev.get("image_model_id") or "seedream_edit",
            "still_qa": still_qa,
            "visual_treatment": visual_treatment,
            "status": "still_ready" if still_qa_passed else "qa_blocked",
            "duration_sec": float(prev.get("duration_sec", 5.0)),
        })
        save_scenes(workspace, scenes)
    save_scenes(workspace, scenes)

    _write_progress(workspace, stage="awaiting_scene_review", progress=80, detail="Review scenes")
    _write_result(workspace, {
        "status": "awaiting_scene_review", "job_id": workspace.name,
        "render_style": style.key, "render_style_label": style.label,
        "stills_model": selected_image_model or ("seedream_v45_edit_canonical" if is_skeleton else f"seedream_v45_t2i_{style.key}"),
        "image_model_id": selected_image_model or "",
        "category": category_key, "topic": topic, "tier": tier,
        "scene_count": len(scenes),
        "visual_proof_only": len(scenes) == 1,
        "product_reference_count": len(reference_images or []),
        "skeleton_reference_image": (
            "reference.png"
            if is_skeleton and (workspace / "reference.png").is_file()
            else (skeleton_master_ref if is_skeleton else "")
        ),
        "sound_design_brief": sound_design_brief or "",
    })
    return {
        "status": "awaiting_scene_review",
        "scene_count": len(scenes),
        "visual_proof_only": len(scenes) == 1,
        "job_id": workspace.name,
    }


# ─── Per-scene edit / regenerate (Seedream v4.5 + v4.5 edit) ───────────────────

def _job_locked_outfit(workspace: Path) -> str:
    spec = _read_job_spec(workspace)
    return str(spec.get("locked_outfit") or "").strip()


def _persist_job_locked_outfit(workspace: Path, outfit: str) -> str:
    """Stamp job-level wardrobe once so still + motion + regenerate stay synchronized."""
    from skeleton_ai.prompt_compose import resolve_locked_outfit

    workspace = Path(workspace)
    outfit = resolve_locked_outfit(
        job_locked=_job_locked_outfit(workspace),
        scene_outfit=outfit,
        topic=str(_read_result(workspace).get("topic") or _read_job_spec(workspace).get("topic") or ""),
        force_simple_host=True,
    )
    spec_path = workspace / "job_spec.json"
    try:
        spec = _read_job_spec(workspace)
        if str(spec.get("locked_outfit") or "").strip() != outfit:
            spec["locked_outfit"] = outfit
            spec_path.write_text(json.dumps(spec, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass
    return outfit


def _rebuild_skeleton_scene_prompt(workspace: Path, sc: dict[str, Any]) -> str:
    """Rebuild scene-first prompt; never let Catalyst/guards delete location/wardrobe."""
    from .canonical_edit import build_scene_edit_prompt, sanitize_skeleton_scene_action
    from skeleton_ai.prompt_compose import dual_host_staging_brief, resolve_cast_count, resolve_locked_outfit

    meta = _read_result(workspace)
    spec = _read_job_spec(workspace)
    topic = str(meta.get("topic") or sc.get("topic") or spec.get("topic") or "").strip()
    brief = str(meta.get("visual_brief") or sc.get("visual_brief") or spec.get("visual_brief") or "").strip()
    narration = str(sc.get("narration") or "").strip()
    raw_action = str(sc.get("scene_action") or "").strip()
    hosts = resolve_cast_count(
        job_cast=spec.get("cast_count"),
        scene_cast=sc.get("cast_count"),
        topic=topic,
        visual_brief=brief,
        narration=narration,
        scene_action=raw_action,
    )
    sc["cast_count"] = hosts
    # Prefer stored scene_action over full prompt (prompt may be guardrail sludge).
    default_action = (
        f"relationship psychology interior; {dual_host_staging_brief()}"
        if hosts >= 2
        else "psychology studio, medium-wide presenter pose, empty hands"
    )
    action, _ = sanitize_skeleton_scene_action(
        raw_action or default_action,
        topic=topic,
        visual_brief=brief,
        narration=narration,
        cast_count=hosts,
    )
    alow = action.lower()
    if (
        len(action) < 40
        or "basketball" in alow
        or "dumbbell" in alow
        or "eyes in" in alow
        or "internal chest" in alow
        or "black void" in alow
    ):
        action = (
            (
                "Moody apartment hallway at dusk with visible walls and floor. "
                f"{dual_host_staging_brief()} Clean ivory ribcages (no eyes in chest), detailed environment — never a black void."
            )
            if hosts >= 2
            else (
                "Modern dark psychology studio with soft spotlight and visible room detail "
                "(walls, floor, practical light). Exactly one skeleton host, medium-wide 9:16, "
                "empty hands in a clear presenter gesture, clean ivory ribcage (no eyes in chest), "
                "detailed environment — never a black void"
            )
        )
        action, _ = sanitize_skeleton_scene_action(
            action, topic=topic, visual_brief=brief, narration=narration, cast_count=hosts,
        )
    outfit = resolve_locked_outfit(
        job_locked=_job_locked_outfit(workspace),
        scene_outfit=str(sc.get("outfit") or ""),
        topic=topic,
        force_simple_host=True,
    )
    outfit = _persist_job_locked_outfit(workspace, outfit)
    sc["scene_action"] = action
    sc["outfit"] = outfit
    # Motion must match the same lock.
    try:
        from skeleton_ai.prompt_compose import compose_skeleton_motion_prompt

        sc["motion_prompt"] = compose_skeleton_motion_prompt(
            motion=str(
                sc.get("scene_action")
                or sc.get("motion_prompt")
                or narration
                or "subtle idle motion"
            ),
            locked_outfit=outfit,
            effect_direction=str((sc.get("visual_treatment") or {}).get("motion_effect") or ""),
            cast_count=hosts,
        )
    except Exception:
        pass

    catalyst = ""
    try:
        from studio_agent.catalyst_skeleton_reference import (
            catalyst_block_for_compose,
            resolve_channel_key,
        )

        catalyst = catalyst_block_for_compose(resolve_channel_key(workspace))
    except Exception:
        catalyst = ""

    return build_scene_edit_prompt(
        topic=topic or "skeleton short",
        visual_description=action,
        outfit=outfit,
        visual_brief=brief,
        narration=narration,
        catalyst_block=catalyst,
        cast_count=hosts,
    )


def regenerate_scene(
    workspace: Path,
    index: int,
    *,
    seed: int | None = None,
    image_model_id: str | None = None,
    fallback_image_model_id: str | None = None,
    candidate_path: Path | None = None,
    defer_commit: bool = False,
    fallback_guard: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Re-render one scene into an isolated candidate before replacing its still."""
    workspace = Path(workspace)
    stills_dir, _c, _t, _w = _setup_dirs(workspace)
    scenes = load_scenes(workspace)
    sc = next((s for s in scenes if s.get("index") == index), None)
    if not sc:
        raise RuntimeError(f"scene {index} not found")
    style = _style_for(workspace)
    # A regeneration must explore a fresh candidate. The previous fixed
    # 990000+index seed made every click reproduce the same frame, which felt
    # like Studio ignored the user's request.
    fresh_seed = int(seed) if seed is not None else secrets.randbelow(2_000_000_000 - 1) + 1
    still_target = stills_dir / f"{sc['sid']}.png"
    output_target = Path(candidate_path) if candidate_path is not None else (
        stills_dir / f".{sc['sid']}.{secrets.token_hex(8)}.candidate.png"
    )
    if output_target.resolve() == still_target.resolve():
        raise ValueError("regeneration candidate must not be the canonical still")
    output_target.parent.mkdir(parents=True, exist_ok=True)
    output_target.unlink(missing_ok=True)
    image_model_id = str(
        image_model_id
        or (
            _skeleton_image_model(sc, workspace)
            if style.pipeline == "skeleton_host"
            else sc.get("image_model_id")
        )
        or "seedream_edit"
    ).strip().lower()
    provider_name = ""
    fallback_from = ""
    fallback_reason = ""
    if style.pipeline == "skeleton_host":
        # Always rebuild — old scenes.json prompts still describe basketball/gym/glow-eye props.
        rebuilt = str(sc.get("prompt") or "").strip() if sc.get("prompt_user_override") else _rebuild_skeleton_scene_prompt(workspace, sc)
        sc["prompt"] = rebuilt
        master_ref = _resolve_skeleton_master_reference(workspace, None)
        # Prefer Seedream edit from clean empty-hands master for identity lock.
        # Grok T2I/edit free-form invents glowing eyes, circuits, and multi-cast costumes.
        render_model = image_model_id or "seedream_edit"
        hosts = int(sc.get("cast_count") or _read_job_spec(workspace).get("cast_count") or 1)
        try:
            guarded_prompt, amount, note, key, provider_name = _generate_skeleton_still_from_master(
                rebuilt,
                output_target,
                image_model_id=render_model,
                seed=fresh_seed,
                master_url=master_ref,
                cast_count=hosts,
            )
        except StyledStillError as exc:
            if not _is_xai_availability_or_credit_error(exc):
                output_target.unlink(missing_ok=True)
                raise
            fallback_from = render_model
            fallback_reason = str(exc)[:300]
            render_model = _fal_image_fallback_model(fallback_image_model_id)
            output_target.unlink(missing_ok=True)
            _require_current_image_fallback_route(fallback_guard)
            guarded_prompt, amount, note, key, provider_name = _generate_skeleton_still_from_master(
                rebuilt,
                output_target,
                image_model_id=render_model,
                seed=fresh_seed,
                master_url=master_ref,
                cast_count=hosts,
            )
        image_model_id = render_model
        sc["prompt"] = guarded_prompt
        sc["image_model_id"] = render_model
        provider, amount, note = _metered_provider_values(provider_name, amount, note)
        production_costs.record_event(
            workspace,
            stage="regenerate",
            provider=provider,
            operation=key,
            usd=amount,
            quantity=1,
            unit="image",
            scene_index=index,
            metadata={
                "pricing_note": note,
                "regenerate": True,
                "prompt_rebuilt": True,
                "fallback_from": fallback_from or None,
            },
        )
    elif image_model_id in {"grok_imagine", "grok_imagine_standard"}:
        amount, note, key = production_costs.price_xai_image(image_model_id, edit=False)
        try:
            guarded_prompt = sc["prompt"]
            result = generate_still_t2i(
                guarded_prompt,
                output_target,
                negative_prompt=style.negative_prompt,
                seed=fresh_seed,
                image_model_id=image_model_id,
            )
            sc["prompt"] = guarded_prompt
            if result.get("cost_usd") is not None:
                amount = production_costs._usd(result.get("cost_usd"))
        except StyledStillError as exc:
            charged = exc.cost_usd if exc.cost_usd is not None else 0.0
            if charged:
                provider, failed_amount, failed_note = _metered_provider_values("xai", charged, f"{note}: failed regenerate")
                production_costs.record_event(
                    workspace,
                    stage="regenerate",
                    provider=provider,
                    operation=key,
                    usd=failed_amount,
                    quantity=1,
                    unit="image",
                    scene_index=index,
                    metadata={"pricing_note": failed_note, "failed": True},
                )
            if not _is_xai_availability_or_credit_error(exc):
                output_target.unlink(missing_ok=True)
                raise
            fallback_from = image_model_id
            fallback_reason = str(exc)[:300]
            image_model_id = _fal_image_fallback_model(fallback_image_model_id)
            output_target.unlink(missing_ok=True)
            _require_current_image_fallback_route(fallback_guard)
            generate_still_t2i(
                sc["prompt"],
                output_target,
                negative_prompt=style.negative_prompt,
                seed=fresh_seed,
                image_model_id=image_model_id,
            )
            amount, note, key = production_costs.price_fal_image(
                edit=False,
                model_id=image_model_id,
            )
            provider_name = seedream_provider(image_model_id) or "fal"
        else:
            provider_name = "xai"
        provider, amount, note = _metered_provider_values(provider_name, amount, note)
        production_costs.record_event(
            workspace,
            stage="regenerate",
            provider=provider,
            operation=key,
            usd=amount,
            quantity=1,
            unit="image",
            scene_index=index,
            metadata={"pricing_note": note, "fallback_from": fallback_from or None},
        )
    else:
        image_model_id = _fal_image_fallback_model(image_model_id)
        generate_still_t2i(
            sc["prompt"], output_target, negative_prompt=style.negative_prompt,
            seed=fresh_seed,
            image_model_id=image_model_id,
        )
        amount, note, key = production_costs.price_fal_image(
            edit=False,
            model_id=image_model_id,
        )
        provider, amount, note = _metered_provider_values(
            seedream_provider(image_model_id) or "fal",
            amount,
            note,
        )
        production_costs.record_event(
            workspace,
            stage="regenerate",
            provider=provider,
            operation=key,
            usd=amount,
            quantity=1,
            unit="image",
            scene_index=index,
            metadata={"pricing_note": note},
        )
        provider_name = seedream_provider(image_model_id) or "fal"
    if not output_target.is_file() or output_target.stat().st_size <= 0:
        output_target.unlink(missing_ok=True)
        raise StyledStillError("image provider returned no usable regeneration candidate")
    result = {
        "index": index,
        "still_rel": sc["still_rel"],
        "status": "candidate_ready" if defer_commit else "still_ready",
        "seed": fresh_seed,
        "candidate_path": str(output_target),
        "image_model_id": image_model_id,
        "image_provider": provider_name,
        "fallback_from": fallback_from or None,
        "fallback_reason": fallback_reason or None,
    }
    if defer_commit:
        return result
    # Atomic replacement: the canonical asset is untouched until the provider
    # has produced a complete local candidate.
    output_target.replace(still_target)
    # New still invalidates any existing animation for this scene.
    (workspace / "clips" / f"{sc['sid']}.mp4").unlink(missing_ok=True)
    sc["clip_rel"] = None
    sc["status"] = "still_ready"
    sc["approved_for_video"] = False
    sc["approved_for_animation"] = False
    sc["regenerate_seed"] = fresh_seed
    sc["image_model_id"] = image_model_id
    save_scenes(workspace, scenes)
    result["candidate_path"] = None
    return result


def set_scene_prompt_override(workspace: Path, index: int, prompt: str) -> dict[str, Any]:
    """Persist the creator's exact provider prompt for the next regeneration."""
    from skeleton_ai.prompt_compose import MAX_VISUAL_PROMPT_CHARS

    clean = re.sub(r"\s+", " ", str(prompt or "")).strip()
    if len(clean) < 12:
        raise ValueError("prompt is too short")
    if len(clean) > MAX_VISUAL_PROMPT_CHARS:
        raise ValueError(f"prompt exceeds the {MAX_VISUAL_PROMPT_CHARS}-character provider limit")
    scenes = load_scenes(Path(workspace))
    scene = next((item for item in scenes if int(item.get("index", -1)) == int(index)), None)
    if not scene:
        raise RuntimeError(f"scene {index} not found")
    scene["prompt"] = clean
    scene["prompt_user_override"] = True
    scene["last_edit"] = {"scope": "prompt", "instruction": "Exact provider prompt edited by creator"}
    scene["approved_for_video"] = False
    scene["approved_for_animation"] = False
    scene["animate"] = False
    save_scenes(Path(workspace), scenes)
    return {"index": int(index), "prompt": clean, "prompt_user_override": True}


def regenerate_scene_with_catalyst(
    workspace: Path,
    index: int,
    *,
    audit: dict[str, Any] | None = None,
    seed: int | None = None,
    image_model_id: str | None = None,
    fallback_image_model_id: str | None = None,
    candidate_path: Path | None = None,
    defer_commit: bool = False,
    fallback_guard: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Catalyst-guided regenerate: audit artifacts, preserve style, fix limbs/layout."""
    workspace = Path(workspace)
    from studio_agent.catalyst_health import ensure_catalyst_skeleton_learning_ready

    ensure_catalyst_skeleton_learning_ready()
    if audit is None:
        from studio_agent.catalyst_still_audit import audit_scene_still

        audit = audit_scene_still(workspace, index)

    scenes = load_scenes(workspace)
    sc = next((s for s in scenes if s.get("index") == index), None)
    if not sc:
        raise RuntimeError(f"scene {index} not found")

    style = _style_for(workspace)
    if style.pipeline == "skeleton_host":
        # A semantic failure must change the next candidate, not merely roll a
        # new seed with the identical weak instruction.  Keep this compact so
        # the provider prompt remains below its hard limit.
        repair_note = str((audit or {}).get("fix_instruction") or "").lower()
        if repair_note and any(
            term in repair_note
            for term in ("orb", "chest", "sphere", "symbolic_clutter", "bubble", "fused", "merge", "capsule")
        ):
            from skeleton_ai.prompt_compose import dual_host_staging_brief, resolve_cast_count

            hosts = resolve_cast_count(
                job_cast=_read_job_spec(workspace).get("cast_count"),
                scene_cast=sc.get("cast_count"),
                topic=str(_read_result(workspace).get("topic") or ""),
                scene_action=str(sc.get("scene_action") or ""),
                narration=str(sc.get("narration") or ""),
            )
            # Never prepend "STRICT REPAIR" sludge — it bloats the prompt and worsens stills.
            if hosts >= 2:
                sc["scene_action"] = (
                    "Apartment hallway at dusk, medium-wide. "
                    f"{dual_host_staging_brief()}"
                )[:280]
            else:
                sc["scene_action"] = (
                    "Physical interior, medium-wide; one ivory skeleton, open ribcage, "
                    "thin body-hugging glass shell only; empty hands; no chest orb/glow."
                )[:280]
            sc["cast_count"] = hosts
        sc["prompt"] = _rebuild_skeleton_scene_prompt(workspace, sc)
    else:
        from .canonical_edit import sanitize_skeleton_scene_action

        action, _ = sanitize_skeleton_scene_action(str(sc.get("scene_action") or ""))
        sc["scene_action"] = action
    save_scenes(workspace, scenes)

    still_rel = str(sc.get("still_rel") or f"stills/{sc['sid']}.png")
    still_path = workspace / still_rel
    method = str(audit.get("method") or "edit").lower()
    fix_instruction = str(audit.get("fix_instruction") or "").strip()
    # Long Catalyst edit instructions made artifacted stills worse. Artifact
    # cleanup always master-regenerates with the short cast-aware scene brief.
    if (
        method == "edit"
        and (
            len(fix_instruction) > 300
            or "master regenerate" in fix_instruction.lower()
            or any(
                term in fix_instruction.lower()
                for term in ("orb", "bubble", "pod", "fused", "artifact", "shared")
            )
        )
    ):
        method = "regenerate"
        fix_instruction = ""

    if method == "edit" and still_path.is_file() and fix_instruction:
        result = edit_scene(
            workspace,
            index,
            fix_instruction,
            scope="character",
            image_model_id=image_model_id,
            fallback_image_model_id=fallback_image_model_id,
            candidate_path=candidate_path,
            defer_commit=defer_commit,
            fallback_guard=fallback_guard,
        )
        if isinstance(result, dict):
            result["catalyst_audit"] = audit
            result["regenerate_method"] = "catalyst_style_preserving_edit"
        return result

    # Layout artifacts (diptych/split) cannot be fixed by editing the broken still.
    result = regenerate_scene(
        workspace,
        index,
        seed=seed,
        image_model_id=image_model_id,
        fallback_image_model_id=fallback_image_model_id,
        candidate_path=candidate_path,
        defer_commit=defer_commit,
        fallback_guard=fallback_guard,
    )
    if isinstance(result, dict):
        result["catalyst_audit"] = audit
        result["regenerate_method"] = "catalyst_master_regenerate"
    return result


def apply_scene_direction(workspace: Path, index: int, direction: str) -> dict[str, Any]:
    """Replace one scene's physical brief before its next regeneration.

    A user saying "regenerate scene 5 in a courthouse corridor" must alter the
    actual provider scene, not merely become a Catalyst feedback annotation.
    The compiler keeps the supplied location/action first and removes only
    non-physical artifact bait before the existing reference-edit path runs.
    """
    from skeleton_ai.prompt_compose import (
        compact_skeleton_scene_direction,
        dual_host_staging_brief,
        resolve_cast_count,
    )
    from .canonical_edit import sanitize_skeleton_scene_action

    clean = compact_skeleton_scene_direction(str(direction or ""), max_chars=520)
    if len(clean) < 24:
        return {"changed": False, "scene_action": ""}
    scenes = load_scenes(workspace)
    scene = next((item for item in scenes if int(item.get("index", -1)) == int(index)), None)
    if not scene:
        raise RuntimeError(f"scene {index} not found")
    try:
        spec = json.loads((workspace / "job_spec.json").read_text(encoding="utf-8"))
    except Exception:
        spec = {}
    meta = _read_result(workspace)
    hosts = resolve_cast_count(
        job_cast=spec.get("cast_count"),
        scene_cast=scene.get("cast_count"),
        topic=str(spec.get("topic") or meta.get("topic") or ""),
        visual_brief=str(spec.get("visual_brief") or meta.get("visual_brief") or ""),
        narration=str(scene.get("narration") or ""),
        scene_action=clean,
        user_feedback=str(direction or ""),
    )
    if hosts >= 2 and "two" not in clean.lower():
        clean = f"{clean} {dual_host_staging_brief()}".strip()
    action, _ = sanitize_skeleton_scene_action(
        clean,
        topic=str(spec.get("topic") or meta.get("topic") or ""),
        visual_brief=str(spec.get("visual_brief") or meta.get("visual_brief") or ""),
        narration=str(scene.get("narration") or ""),
        cast_count=hosts,
    )
    scene["cast_count"] = hosts
    scene["scene_action"] = action
    scene["prompt"] = ""  # force scene-first recompilation; never reuse a stale long prompt
    scene["last_direction"] = str(direction or "").strip()[:1200]
    scene["last_edit"] = {"scope": "full", "instruction": str(direction or "").strip()[:1200]}
    if hosts >= 2:
        try:
            spec["cast_count"] = 2
            (workspace / "job_spec.json").write_text(json.dumps(spec, indent=2), encoding="utf-8")
        except Exception:
            pass
    save_scenes(workspace, scenes)
    return {"changed": True, "scene_action": action, "cast_count": hosts}


def apply_motion_direction(workspace: Path, index: int, feedback: str) -> dict[str, Any]:
    """Rewrite i2v performance from creator natural-language critique.

    Preserves the approved still. Interprets free-form notes like "barely moved,
    stronger pose + glass VFX + background parallax" into scene_action,
    effect_direction, and a composed motion_prompt strong enough for short-form.
    """
    from skeleton_ai.prompt_compose import (
        compose_skeleton_motion_prompt,
        resolve_locked_outfit,
    )
    from .scripting_grok import GrokClient

    workspace = Path(workspace)
    note = re.sub(r"\s+", " ", str(feedback or "")).strip()
    if len(note) < 8:
        note = (
            "Animation is too static. Need a clear pose change, skeleton-sourced VFX "
            "on the glass shell, and background/camera motion."
        )
    scenes = load_scenes(workspace)
    scene = next((item for item in scenes if int(item.get("index", -1)) == int(index)), None)
    if not scene:
        raise RuntimeError(f"scene {index} not found")

    previous_action = str(scene.get("scene_action") or "").strip()
    previous_motion = str(scene.get("motion_prompt") or "").strip()
    narration = str(scene.get("narration") or "").strip()
    try:
        spec = json.loads((workspace / "job_spec.json").read_text(encoding="utf-8"))
    except Exception:
        spec = {}
    topic = str(spec.get("topic") or _read_result(workspace).get("topic") or "")
    locked = resolve_locked_outfit(
        job_locked=_job_locked_outfit(workspace),
        scene_outfit=str(scene.get("outfit") or ""),
        topic=topic,
        force_simple_host=True,
    )

    system = (
        "You rewrite ONE skeleton short-form image-to-video performance from creator feedback. "
        "Keep the SAME approved still identity (one ivory skeleton in a thin glass shell). "
        "Do NOT change wardrobe into clothing if locked as no clothing. "
        "Translate the creator's complaint into a READABLE SILENT 5-second body performance: "
        "pose/weight change, one clear hand gesture, skeleton-sourced VFX (glass refraction, rim light, dust), "
        "and background or camera energy (push-in / parallax / lighting shift). "
        "MUTE ONLY — no talking, no jaw/mouth motion, no lip-sync, no dialogue, no baked-in voice or music. "
        "Narration is added later as a separate FAL voiceover. Near-static idle is a failure. "
        "No text, graphics, chest orbs, brain/circuits, human skin, or new people. "
        "Return strict JSON only: "
        "{\"scene_action\":\"physical silent performance beat 40-90 words\","
        "\"effect_direction\":\"skeleton-sourced VFX only\","
        "\"motion_prompt\":\"compact silent i2v motion sentence\"}."
    )
    user = (
        f"Topic: {topic}\nNarration beat: {narration}\n"
        f"Previous scene_action: {previous_action[:500]}\n"
        f"Previous motion_prompt: {previous_motion[:400]}\n"
        f"Creator feedback: {note[:900]}"
    )
    data: dict[str, Any] = {}
    try:
        raw = GrokClient().complete(system, user, max_tokens=420, temperature=0.55)
        cleaned = str(raw or "").strip().strip("`").strip()
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:].strip()
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            data = parsed
    except Exception:
        data = {}

    scene_action = re.sub(r"\s+", " ", str(data.get("scene_action") or "")).strip()
    effect = re.sub(r"\s+", " ", str(data.get("effect_direction") or "")).strip()
    motion_seed = re.sub(r"\s+", " ", str(data.get("motion_prompt") or "")).strip()
    if len(scene_action) < 32:
        scene_action = (
            f"{previous_action[:280] + '; ' if previous_action else ''}"
            "PERFORMANCE: weight shifts to one hip, torso turns a quarter toward camera, "
            "one open presenter-hand gesture then settles; glass-shell refraction highlights "
            "travel across the ribs; slow camera push-in with background parallax and a soft "
            "lighting change in the physical interior"
        )
    if len(effect) < 16:
        effect = (
            "glass-shell refraction shimmer and soft rim light along ivory bones; "
            "faint dust motes in gallery light; no chest orbs, no brain/circuits, no text"
        )
    motion = compose_skeleton_motion_prompt(
        motion=motion_seed or scene_action,
        locked_outfit=locked,
        effect_direction=effect,
        budget=300,
        cast_count=int(scene.get("cast_count") or _read_job_spec(workspace).get("cast_count") or 1),
    )

    scene["outfit"] = locked
    scene["scene_action"] = scene_action[:280]
    scene["effect_direction"] = effect[:120]
    scene["sfx_direction"] = effect[:120]
    scene["motion_prompt"] = motion
    scene["last_motion_feedback"] = note[:1200]
    scene["motion_revision"] = {
        "reason": note[:500],
        "previous_action": previous_action[:500],
        "previous_motion": previous_motion[:500],
        "replacement_action": scene_action[:700],
        "replacement_motion": motion[:700],
    }
    save_scenes(workspace, scenes)
    return {
        "changed": True,
        "scene_action": scene["scene_action"],
        "motion_prompt": motion,
        "effect_direction": effect,
        "reason": "creator motion critique rewritten into i2v performance",
    }


def improve_generic_scene_direction(workspace: Path, index: int) -> dict[str, Any]:
    """Turn a lazy stored scene brief into a concrete, filmable beat.

    This is deliberately deterministic rather than a second free-form model
    prompt: a generic regeneration must become more directed without adding
    new anatomy, props, or artifact-prone visual metaphors. Explicit art
    direction is never overwritten.
    """
    from .canonical_edit import sanitize_skeleton_scene_action

    scenes = load_scenes(workspace)
    scene = next((item for item in scenes if int(item.get("index", -1)) == int(index)), None)
    if not scene:
        raise RuntimeError(f"scene {index} not found")
    existing = str(scene.get("scene_action") or "").strip()
    # A previous router version could persist the user's command itself as a
    # scene direction (for example: "Regenerate Scene 1 with its prompt").
    # Treat command-shaped text and bare presenter staging as generic state,
    # never as intentional art direction that must be preserved.
    generic = not existing or bool(re.fullmatch(
        r"(?is).*\b(?:psychology|modern|minimalist|generic)\s+(?:studio|office)\b.*",
        existing,
    )) or bool(re.search(
        r"\b(?:re-?generate|re-?animate)\s+(?:scene\s*\d*|with\s+(?:its|the)\s+prompt)\b"
        r"|\bscene\s*\d+\s+(?:with|based\s+on)\s+(?:its|the|current|actual)\s+prompt\b"
        r"|\bempty\s+hands\s+in\s+(?:a\s+)?presenter\s+gesture\b",
        existing,
        re.I,
    ))
    if not generic:
        return {"changed": False, "scene_action": existing, "reason": "stored direction is already specific"}

    narration = str(scene.get("narration") or "").strip()
    variations = (
        "Quiet apartment doorway at blue hour, medium side profile; the skeleton host pauses before leaving, one empty hand resting on the doorframe",
        "Rainy cafe window booth, close three-quarter portrait; the skeleton host studies a phone left face-down on the table, hands otherwise empty",
        "Long empty office corridor at night, medium-wide composition; the skeleton host stops beneath practical ceiling lights with both hands empty",
        "Library aisle with warm practical lamps, profile medium shot; the skeleton host reaches toward a book then pulls the empty hand back",
        "Cinema lobby after closing, wide frame with reflected floor lights; the skeleton host stands alone facing the exit signs, empty hands",
        "Train platform at dawn, medium-wide shot; the skeleton host watches a departing train through glass, both hands empty",
    )
    from skeleton_ai.prompt_compose import dual_host_staging_brief, resolve_cast_count

    topic = str(_read_result(workspace).get("topic") or "")
    brief = str(_read_result(workspace).get("visual_brief") or "")
    hosts = resolve_cast_count(
        job_cast=_read_job_spec(workspace).get("cast_count"),
        scene_cast=scene.get("cast_count"),
        topic=topic,
        visual_brief=brief,
        narration=narration,
        scene_action=existing,
    )
    if hosts >= 2:
        dual_variations = (
            f"Quiet apartment doorway at blue hour, medium-wide; {dual_host_staging_brief()}",
            f"Rainy cafe window booth, medium-wide; {dual_host_staging_brief()}",
            f"Long empty office corridor at night, medium-wide; {dual_host_staging_brief()}",
            f"Library aisle with warm practical lamps; {dual_host_staging_brief()}",
            f"Cinema lobby after closing, wide frame; {dual_host_staging_brief()}",
            f"Train platform at dawn, medium-wide; {dual_host_staging_brief()}",
        )
        replacement = dual_variations[int(index) % len(dual_variations)]
    else:
        replacement = variations[int(index) % len(variations)]
    clean, _ = sanitize_skeleton_scene_action(
        replacement,
        topic=topic,
        visual_brief=brief,
        narration=narration,
        cast_count=hosts,
    )
    scene["cast_count"] = hosts
    scene["scene_action"] = clean
    scene["prompt"] = ""  # rebuild the compact provider prompt from this direction
    scene["catalyst_direction_repair"] = {
        "reason": "replaced generic stored direction before regeneration",
        "previous": existing[:500],
        "replacement": clean,
        "cast_count": hosts,
    }
    save_scenes(workspace, scenes)
    return {"changed": True, "scene_action": clean, "reason": "replaced generic stored direction", "cast_count": hosts}


def redesign_scene_from_narration(workspace: Path, index: int) -> dict[str, Any]:
    """Give an ordinary Regenerate click a fresh, story-aware direction.

    The creator should not need to prompt-engineer a weak scene.  This pass
    treats narration as intent, asks the fast planning model for one filmable
    emotional moment, then stores only compact physical direction.  Exact
    creator prompt overrides remain untouched.
    """
    from .canonical_edit import sanitize_skeleton_scene_action
    from .scripting_grok import GrokClient

    workspace = Path(workspace)
    scenes = load_scenes(workspace)
    scene = next((item for item in scenes if int(item.get("index", -1)) == int(index)), None)
    if not scene:
        raise RuntimeError(f"scene {index} not found")
    if bool(scene.get("prompt_user_override")):
        return {
            "changed": False,
            "scene_action": str(scene.get("scene_action") or ""),
            "reason": "creator exact-prompt override preserved",
        }

    narration = str(scene.get("narration") or "").strip()
    try:
        spec = json.loads((workspace / "job_spec.json").read_text(encoding="utf-8"))
    except Exception:
        spec = {}
    topic = str(spec.get("topic") or _read_result(workspace).get("topic") or "relationship psychology")
    previous = str(scene.get("scene_action") or "").strip()
    beat = int(index) + 1
    from skeleton_ai.prompt_compose import dual_host_staging_brief, resolve_cast_count

    hosts = resolve_cast_count(
        job_cast=spec.get("cast_count"),
        scene_cast=scene.get("cast_count"),
        topic=topic,
        visual_brief=str(spec.get("visual_brief") or ""),
        narration=narration,
        scene_action=previous,
    )
    scene["cast_count"] = hosts
    if hosts >= 2:
        try:
            spec["cast_count"] = 2
            (workspace / "job_spec.json").write_text(json.dumps(spec, indent=2), encoding="utf-8")
        except Exception:
            pass

    if hosts >= 2:
        system = (
            "You are the visual director for one premium 9:16 skeleton short scene. "
            "Translate narration into ONE physical cinematic story moment with exactly TWO identical "
            "ivory skeleton hosts in thin glass shells sharing one continuous frame (not split-screen). "
            "Left host = love-bomber / pursuer; right host = recipient. Make the contrast unmistakable "
            "through posture and empty-hand gestures. Choose a meaningful real location, camera shot and "
            "lighting. Do not use a generic presenter pose, empty stage, theater, plain studio, text, "
            "diagrams, glowing anatomy, literal brain graphics, humans, or a third skeleton. Return strict "
            "JSON only: {\"scene_action\":\"...\",\"motion_prompt\":\"...\"}. "
            "scene_action must be 45-90 words and physical/filmable. motion_prompt must describe one "
            "silent 5-second dual-host emotional performance with no anatomy morphing or talking."
        )
    else:
        system = (
            "You are the visual director for one premium 9:16 skeleton short scene. "
            "Translate narration into ONE physical cinematic story moment. Keep the same canonical "
            "single skeleton. Make its emotion unmistakable through head angle, eye focus, shoulders, "
            "posture and skeletal hand gesture. Choose a meaningful real location, camera shot and "
            "lighting. Do not use a generic presenter pose, empty stage, theater, plain studio, text, "
            "diagrams, glowing anatomy, literal brain graphics, extra people, or props that are not "
            "essential. Return strict JSON only: {\"scene_action\":\"...\",\"motion_prompt\":\"...\"}. "
            "scene_action must be 45-90 words and physical/filmable. motion_prompt must describe one "
            "subtle stable 5-second emotional performance with no anatomy morphing."
        )
    user = (
        f"Topic: {topic}\nScene {beat} narration: {narration}\n"
        f"Reject and improve this previous direction: {previous[:500]}"
    )
    data: dict[str, Any] = {}
    try:
        raw = GrokClient().complete(system, user, max_tokens=360, temperature=0.72)
        cleaned = str(raw or "").strip().strip("`").strip()
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:].strip()
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            data = parsed
    except Exception:
        data = {}

    # A useful deterministic fallback is preferable to returning the same
    # bland frame when the planning model is temporarily unavailable.
    if hosts >= 2:
        fallbacks = (
            f"Dim apartment entryway at blue hour, medium-wide frame; {dual_host_staging_brief()}",
            f"Rain-streaked cafe booth at dusk; {dual_host_staging_brief()}",
            f"Long office corridor after hours under practical lights; {dual_host_staging_brief()}",
            f"Quiet library aisle under warm lamps; {dual_host_staging_brief()}",
            f"Nearly empty train platform before dawn; {dual_host_staging_brief()}",
            f"Apartment kitchen after midnight; {dual_host_staging_brief()}",
        )
    else:
        fallbacks = (
            "Dim apartment entryway at blue hour, medium side-profile frame; the skeleton pauses halfway through the open doorway, chin lowered and gaze turned back toward the warm room, shoulders drawn inward, one hand gripping the doorframe while the other hangs uncertainly",
            "Rain-streaked cafe window at dusk, close three-quarter frame; the skeleton sits alone beside an untouched second chair, gaze fixed outside, shoulders guarded, both hands slowly folding together on the table edge",
            "Long office corridor after hours, medium-wide frame under receding practical lights; the skeleton has stopped mid-step, torso angled away while its head looks back, one hand half-raised as if wanting to explain before withdrawing",
            "Quiet library aisle under warm lamps, profile medium shot; the skeleton reaches toward a book then hesitates, chin tucked, eyes averted, shoulders tense and the free hand held close to its ribcage",
            "Nearly empty train platform before dawn, medium-wide frame; the skeleton watches the departing train through misted glass, posture rigid then softening, one open hand dropping slowly to its side",
            "Apartment kitchen after midnight, intimate medium shot; the skeleton leans against the counter with lowered gaze and closed posture, then cautiously opens one hand toward the unseen doorway",
        )
    proposed = str(data.get("scene_action") or fallbacks[int(index) % len(fallbacks)]).strip()
    action, _ = sanitize_skeleton_scene_action(
        proposed,
        topic=topic,
        visual_brief=str(spec.get("visual_brief") or ""),
        narration=narration,
        cast_count=hosts,
    )
    motion = re.sub(r"\s+", " ", str(data.get("motion_prompt") or "")).strip()
    if len(motion) < 24:
        motion = (
            "Both skeletons perform a silent contrast beat: left leans in with open hands, "
            "right draws back; glass-shell highlights travel; slow camera push with parallax"
            if hosts >= 2
            else "Slow controlled head turn away from camera, shoulders tighten then soften, one restrained hand gesture; stable skull, eyes, ribs, limbs and glass shell"
        )

    scene["scene_action"] = action
    scene["motion_prompt"] = motion[:420]
    scene["prompt"] = ""
    scene["prompt_user_override"] = False
    scene["creative_redesign"] = {
        "reason": "ordinary regenerate rebuilt direction from narration",
        "previous": previous[:500],
        "replacement": action[:700],
        "cast_count": hosts,
    }
    scene["approved_for_video"] = False
    scene["approved_for_animation"] = False
    scene["animate"] = False
    save_scenes(workspace, scenes)
    return {"changed": True, "scene_action": action, "motion_prompt": motion[:420], "reason": "narration-driven creative redesign", "cast_count": hosts}


def _scoped_edit_prompt(scene_prompt: str, instruction: str, scope: str) -> tuple[str, str]:
    raw_scope = (scope or "full").strip().lower().replace("-", "_")
    aliases = {
        "subject": "character",
        "person": "character",
        "wardrobe": "character",
        "environment": "background",
        "setting": "background",
        "prop": "props",
    }
    normalized = aliases.get(raw_scope, raw_scope)
    if normalized not in {"character", "background", "props", "full"}:
        normalized = "full"

    guardrails = {
        "character": (
            "Edit only the foreground character or subject. Preserve the current background, "
            "camera angle, lens, lighting direction, composition, and scene continuity unless "
            "the instruction explicitly says otherwise. Keep identity consistent while changing "
            "wardrobe, pose, expression, body color, or held items requested by the instruction."
        ),
        "background": (
            "Edit only the background/environment. Preserve the foreground character identity, "
            "face/head shape, body shape, clothing, pose, props, camera framing, and lighting on "
            "the subject. Do not redesign the character."
        ),
        "props": (
            "Edit only the requested object, prop, screen content, or held item. Preserve the "
            "character identity, clothing, pose, background, camera angle, and lighting."
        ),
        "full": (
            "Apply the requested change while preserving as much scene continuity, character "
            "identity, camera framing, and visual style as possible."
        ),
    }[normalized]
    # An edit must carry a specific change plus only the smallest useful source
    # context.  Passing the old 1.6k scene prompt was the 3.3k regression.
    compact_scene = " ".join(str(scene_prompt or "").split())[:330]
    prompt = (
        f"REQUESTED CHANGE — EXECUTE THIS FIRST:\n{instruction.strip()}\n\n"
        f"Edit scope: {normalized}.\n"
        f"Continuity rules: {guardrails}\n\n"
        f"Original scene intent for context only:\n{compact_scene}"
    ).strip()[:759]
    return prompt, normalized


def edit_scene(
    workspace: Path,
    index: int,
    instruction: str,
    scope: str = "full",
    *,
    image_model_id: str | None = None,
    fallback_image_model_id: str | None = None,
    candidate_path: Path | None = None,
    defer_commit: bool = False,
    fallback_guard: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Natural-language edit of one scene's still via Seedream v4.5 edit.

    Uploads the current still as the reference image and applies the user's
    change ("make the lighting darker", "put him in ancient Rome", ...).
    The scope controls preservation for premium character/background passes.
    """
    from .canonical_edit import _ensure_fal, generate_still_edit

    workspace = Path(workspace)
    stills_dir, _c, _t, _w = _setup_dirs(workspace)
    scenes = load_scenes(workspace)
    sc = next((s for s in scenes if s.get("index") == index), None)
    if not sc:
        raise RuntimeError(f"scene {index} not found")
    still_target = stills_dir / f"{sc['sid']}.png"
    if not still_target.exists():
        raise RuntimeError(f"scene {index} has no still to edit")

    from .canonical_edit import (
        sanitize_skeleton_prop_language,
        strengthen_skeleton_edit_instruction,
        topic_allows_sports_props,
    )

    if _style_for(workspace).pipeline == "skeleton_host":
        from studio_agent.catalyst_skeleton_reference import (
            append_catalyst_directives_to_prompt,
            resolve_channel_key,
        )

        meta = _read_result(workspace)
        topic = str(meta.get("topic") or "")
        raw_instruction = sanitize_skeleton_prop_language(
            str(instruction or ""),
            topic=topic,
            narration=str(sc.get("narration") or ""),
        )
        if not topic_allows_sports_props(topic, narration=str(sc.get("narration") or "")):
            raw_instruction = (
                f"{raw_instruction} Empty hands only — remove basketball, sports balls, "
                "dumbbells, and gym equipment completely."
            ).strip()
        edit_instruction = append_catalyst_directives_to_prompt(
            strengthen_skeleton_edit_instruction(raw_instruction),
            resolve_channel_key(workspace),
        )
    else:
        edit_instruction = str(instruction or "")
    edit_prompt, normalized_scope = _scoped_edit_prompt(
        str(sc.get("prompt") or ""),
        edit_instruction,
        scope,
    )
    out_tmp = Path(candidate_path) if candidate_path is not None else stills_dir / f"{sc['sid']}_edit.png"
    if out_tmp.resolve() == still_target.resolve():
        raise ValueError("edit candidate must not be the canonical still")
    out_tmp.parent.mkdir(parents=True, exist_ok=True)
    out_tmp.unlink(missing_ok=True)
    style = _style_for(workspace)
    image_model_id = str(
        image_model_id
        or (
            _skeleton_image_model(sc, workspace)
            if style.pipeline == "skeleton_host"
            else sc.get("image_model_id")
        )
        or "seedream_edit"
    ).strip().lower()
    fallback_from = ""
    fallback_reason = ""
    if style.pipeline == "skeleton_host" and image_model_id in {"grok_imagine", "grok_imagine_standard"}:
        xai_edit_prompt = (
            _xai_skeleton_artifact_guard(edit_prompt)
            if style.pipeline == "skeleton_host"
            else edit_prompt
        )
        try:
            amount, note, key, provider_name = _generate_skeleton_still_edit(
                xai_edit_prompt,
                out_tmp,
                reference_path=still_target,
                image_model_id=image_model_id or "grok_imagine",
            )
        except StyledStillError as exc:
            charged = exc.cost_usd if exc.cost_usd is not None else 0.0
            if charged:
                provider, failed_amount, failed_note = _metered_provider_values("xai", charged, "xai edit: failed")
                production_costs.record_event(
                    workspace,
                    stage="edit",
                    provider=provider,
                    operation="grok_imagine_edit",
                    usd=failed_amount,
                    quantity=1,
                    unit="image",
                    scene_index=index,
                    metadata={"pricing_note": failed_note, "failed": True, "edit_scope": normalized_scope},
                )
            if not _is_xai_availability_or_credit_error(exc):
                out_tmp.unlink(missing_ok=True)
                raise
            fallback_from = image_model_id
            fallback_reason = str(exc)[:300]
            image_model_id = _fal_image_fallback_model(fallback_image_model_id)
            out_tmp.unlink(missing_ok=True)
            _require_current_image_fallback_route(fallback_guard)
            amount, note, key, provider_name = _generate_skeleton_still_edit(
                edit_prompt,
                out_tmp,
                reference_path=still_target,
                image_model_id=image_model_id,
            )
        provider, amount, note = _metered_provider_values(provider_name, amount, note)
        production_costs.record_event(
            workspace,
            stage="edit",
            provider=provider,
            operation=f"{key}_edit" if provider_name == "xai" else key,
            usd=amount,
            quantity=1,
            unit="image",
            scene_index=index,
            metadata={
                "pricing_note": note,
                "edit_scope": normalized_scope,
                "fallback_from": fallback_from or None,
            },
        )
        sc["image_model_id"] = image_model_id or "seedream_edit"
    elif style.pipeline == "skeleton_host":
        master_ref = _resolve_skeleton_master_reference(workspace, None)
        from .canonical_edit import generate_still_edit

        generate_still_edit(
            edit_prompt,
            out_tmp,
            master_url=master_ref or str(still_target),
            image_model_id=image_model_id or "seedream_edit",
        )
        amount, note, key = production_costs.price_fal_image(
            edit=True,
            model_id=image_model_id or "seedream_edit",
        )
        provider, amount, note = _metered_provider_values(
            seedream_provider(image_model_id or "seedream_edit") or "fal",
            amount,
            note,
        )
        production_costs.record_event(
            workspace,
            stage="edit",
            provider=provider,
            operation=key,
            usd=amount,
            quantity=1,
            unit="image",
            scene_index=index,
            metadata={"pricing_note": note, "edit_scope": normalized_scope},
        )
        sc["image_model_id"] = image_model_id or "seedream_edit"
    else:
        current_url = str(still_target)
        if not render_simulation.enabled():
            import fal_client

            _ensure_fal()
            current_url = fal_client.upload_file(str(still_target))
        from .canonical_edit import generate_still_edit

        generate_still_edit(
            edit_prompt,
            out_tmp,
            master_url=current_url,
            image_model_id=image_model_id or "seedream_edit",
        )
        amount, note, key = production_costs.price_fal_image(
            edit=True,
            model_id=image_model_id or "seedream_edit",
        )
        provider, amount, note = _metered_provider_values(
            seedream_provider(image_model_id or "seedream_edit") or "fal",
            amount,
            note,
        )
        production_costs.record_event(
            workspace,
            stage="edit",
            provider=provider,
            operation=key,
            usd=amount,
            quantity=1,
            unit="image",
            scene_index=index,
            metadata={"pricing_note": note, "edit_scope": normalized_scope},
        )
    if not out_tmp.is_file() or out_tmp.stat().st_size <= 0:
        out_tmp.unlink(missing_ok=True)
        raise StyledStillError("image provider returned no usable edit candidate")
    result = {
        "index": index,
        "still_rel": sc["still_rel"],
        "status": "candidate_ready" if defer_commit else "still_ready",
        "last_edit_scope": normalized_scope,
        "candidate_path": str(out_tmp),
        "image_model_id": image_model_id,
        "image_provider": seedream_provider(image_model_id) or ("xai" if image_model_id.startswith("grok") else "fal"),
        "fallback_from": fallback_from or None,
        "fallback_reason": fallback_reason or None,
    }
    if defer_commit:
        return result
    # Promote atomically only after a complete candidate exists.
    out_tmp.replace(still_target)
    (workspace / "clips" / f"{sc['sid']}.mp4").unlink(missing_ok=True)
    sc["clip_rel"] = None
    sc["status"] = "still_ready"
    sc["approved_for_video"] = False
    sc["approved_for_animation"] = False
    sc["last_edit"] = instruction.strip()[:300]
    sc["last_edit_scope"] = normalized_scope
    save_scenes(workspace, scenes)
    result["candidate_path"] = None
    return result


def set_scene_settings(
    workspace: Path, index: int, *, animate: bool | None = None, video_model: str | None = None,
) -> dict[str, Any]:
    scenes = load_scenes(workspace)
    sc = next((s for s in scenes if s.get("index") == index), None)
    if not sc:
        raise RuntimeError(f"scene {index} not found")
    if animate is not None:
        sc["animate"] = bool(animate)
    if video_model:
        sc["video_model"] = video_model
    save_scenes(Path(workspace), scenes)
    return sc


# ─── Stage 2: animate selected scenes (i2v) ───────────────────────────────────

def _load_clip_meta(clip: Path) -> dict[str, Any]:
    try:
        sidecar = clip.with_suffix(clip.suffix + ".fal.json")
        data = json.loads(sidecar.read_text(encoding="utf-8")) if sidecar.is_file() else {}
        return dict(data) if isinstance(data, dict) else {}
    except Exception:
        return {}


def _record_animation_attempt_cost(
    workspace: Path,
    sc: dict[str, Any],
    clip_meta: dict[str, Any],
    *,
    attempt: int,
    rejected: bool,
) -> None:
    endpoint = str(clip_meta.get("endpoint") or "")
    duration = float(clip_meta.get("duration_sec") or sc.get("duration_sec") or 5.0)
    if endpoint.startswith("xai:"):
        amount, note, key = production_costs.price_xai_video(
            str(clip_meta.get("video_model") or endpoint),
            seconds=duration,
            resolution=str(clip_meta.get("xai_resolution") or ""),
        )
        if clip_meta.get("xai_cost_usd") is not None:
            amount = production_costs._usd(clip_meta.get("xai_cost_usd"))
        provider, amount, note = _metered_provider_values("xai", amount, note)
    else:
        amount, note, key = production_costs.price_fal_video(endpoint, seconds=duration)
        provider, amount, note = _metered_provider_values("fal", amount, note)
    production_costs.record_event(
        workspace,
        stage="animation",
        provider=provider,
        operation=key,
        usd=amount,
        quantity=duration,
        unit="second",
        endpoint=endpoint,
        request_id=str(clip_meta.get("request_id") or ""),
        scene_index=int(sc["index"]),
        metadata={
            "pricing_note": note,
            "video_model": sc.get("video_model"),
            "qa_attempt": int(attempt),
            "qa_rejected": bool(rejected),
        },
    )


def _quarantine_i2v_attempt(workspace: Path, clip: Path, *, sid: str, attempt: int) -> str:
    rejected_dir = workspace / "rejected_clips"
    rejected_dir.mkdir(parents=True, exist_ok=True)
    target = rejected_dir / f"{sid}_attempt{attempt}.mp4"
    target.unlink(missing_ok=True)
    if clip.is_file():
        clip.replace(target)
    for suffix in (".fal.json", ".visualqa.json"):
        source = clip.with_suffix(clip.suffix + suffix)
        dest = target.with_suffix(target.suffix + suffix)
        dest.unlink(missing_ok=True)
        if source.is_file():
            source.replace(dest)
    try:
        return str(target.relative_to(workspace)).replace("\\", "/")
    except Exception:
        return str(target)


def _identity_safe_motion_fallback(
    still: Path,
    clip: Path,
    *,
    duration_sec: float,
    rejected_reports: list[dict[str, Any]],
) -> None:
    """Create deterministic motion from the approved still; identity cannot morph."""
    clip.unlink(missing_ok=True)
    _still_to_clip(still, clip, duration_sec=max(1.0, float(duration_sec or 5.0)))
    clip.with_suffix(clip.suffix + ".fal.json").write_text(
        json.dumps(
            {
                "endpoint": "local:identity-safe-motion",
                "request_id": "",
                "duration_sec": float(duration_sec or 5.0),
                "video_model": "identity_safe_motion",
                "simulated": False,
                "fallback_reason": "semantic_i2v_identity_not_proven",
                "rejected_attempts": rejected_reports,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def _clip_replacement_artifacts(clip: Path) -> tuple[Path, ...]:
    return (
        clip,
        clip.with_suffix(clip.suffix + ".fal.json"),
        clip.with_suffix(clip.suffix + ".visualqa.json"),
    )


def _stage_clip_replacement(clip: Path) -> list[tuple[Path, Path]]:
    """Move an existing playable clip aside until its replacement succeeds."""

    token = secrets.token_hex(8)
    staged: list[tuple[Path, Path]] = []
    try:
        for original in _clip_replacement_artifacts(clip):
            if not original.is_file():
                continue
            backup = original.with_name(f".{original.name}.{token}.replace")
            backup.unlink(missing_ok=True)
            original.replace(backup)
            staged.append((original, backup))
    except Exception:
        for original, backup in reversed(staged):
            if backup.exists():
                backup.replace(original)
        raise
    return staged


def _restore_clip_replacement(clip: Path, staged: list[tuple[Path, Path]]) -> None:
    for artifact in _clip_replacement_artifacts(clip):
        artifact.unlink(missing_ok=True)
    for original, backup in staged:
        if backup.exists():
            backup.replace(original)


def _commit_clip_replacement(staged: list[tuple[Path, Path]]) -> None:
    for _original, backup in staged:
        backup.unlink(missing_ok=True)


def animate_scenes_stage(
    workspace: Path,
    *,
    indices: list[int] | None = None,
    tier: str = "standard",
    route_resolver: Callable[[], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Animate the selected scenes (or all with animate=True) via i2v."""
    workspace = Path(workspace)
    _stills, clips_dir, _t, _w = _setup_dirs(workspace)
    scenes = load_scenes(workspace)
    if not scenes:
        raise RuntimeError("no scenes planned")
    if indices is None:
        targets = [s for s in scenes if s.get("animate") and s.get("approved_for_video")]
    else:
        idx_set = set(indices)
        targets = [
            s for s in scenes
            if s.get("index") in idx_set and s.get("approved_for_video")
        ]
    if not targets:
        raise RuntimeError(
            "no approved scenes to animate. Review the stills first, then approve scenes with set_production_scenes_animate before running i2v."
        )
    job_spec = _read_job_spec(workspace)
    product_manifest = job_spec.get("product_reference") if isinstance(job_spec.get("product_reference"), dict) else {}
    product_references = [
        Path(str(image.get("path") or ""))
        for image in list(product_manifest.get("images") or [])
        if isinstance(image, dict) and str(image.get("path") or "").strip()
    ]
    product_name = str(product_manifest.get("product_name") or "")
    total = max(len(targets), 1)
    animated: list[int] = []
    failed: list[int] = []
    fallback_scenes: list[int] = []
    route_switches: list[dict[str, Any]] = []
    final_route: dict[str, Any] = {}
    for n, sc in enumerate(targets):
        check_cancelled(workspace)
        _write_progress(workspace, stage="animate", progress=10 + int(n / total * 80),
                        detail=f"Animating scene {sc['index'] + 1}/{len(scenes)}")
        sc["animate"] = True
        still = workspace / sc["still_rel"]
        clip = clips_dir / f"{sc['sid']}.mp4"
        previous_clip_rel = sc.get("clip_rel")
        previous_status = str(sc.get("status") or "")
        staged_clip: list[tuple[Path, Path]] = []
        try:
            staged_clip = _stage_clip_replacement(clip)
            motion_prompt = sc.get("motion_prompt") or sc["narration"]
            skeleton_scene_text = " ".join(
                str(sc.get(key) or "") for key in ("render_style", "outfit", "prompt", "scene_action")
            ).lower()
            is_skeleton = (
                "skeleton_host" in skeleton_scene_text
                or "skeleton" in skeleton_scene_text
                or "skeleton" in str(job_spec.get("render_style") or "").lower()
            )
            is_product = bool(product_references) and not is_skeleton
            if is_skeleton:
                from skeleton_ai.prompt_compose import (
                    compose_skeleton_motion_prompt,
                    resolve_locked_outfit,
                )

                locked = resolve_locked_outfit(
                    job_locked=_job_locked_outfit(workspace),
                    scene_outfit=str(sc.get("outfit") or ""),
                    topic=str(_read_result(workspace).get("topic") or ""),
                    force_simple_host=True,
                )
                sc["outfit"] = locked
                # Prefer scene_action (performance brief) over a stale/weak planner motion_prompt.
                motion_prompt = compose_skeleton_motion_prompt(
                    motion=str(
                        sc.get("scene_action")
                        or motion_prompt
                        or sc.get("narration")
                        or ""
                    ),
                    locked_outfit=locked,
                    effect_direction=str(sc.get("sfx_direction") or sc.get("effect_direction") or ""),
                    budget=300,
                    cast_count=int(sc.get("cast_count") or _read_job_spec(workspace).get("cast_count") or 1),
                )
                sc["motion_prompt"] = motion_prompt
            video_model = (
                _skeleton_video_model(sc, workspace)
                if is_skeleton
                else str(sc.get("video_model") or "")
            )
            if route_resolver is not None:
                resolved_route = dict(route_resolver() or {})
                if str(resolved_route.get("video_model") or "").strip():
                    video_model = str(resolved_route["video_model"]).strip()
                final_route = resolved_route
            sc["video_model"] = video_model or sc.get("video_model")
            lane = "i2v_premium" if "kling" in str(video_model).lower() or "premium" in str(video_model).lower() else "i2v"
            duration = float(sc.get("duration_sec") or 5.0)
            treatment = dict(sc.get("visual_treatment") or {})
            if str(treatment.get("kind") or "") == "motion_graphic":
                # Graphics are deliberate, local editorial beats.  Do not send
                # labels/data through I2V where they would artifact or become
                # unreadable; render the approved script fact directly.
                from studio_agent.visual_treatment import render_motion_graphic_clip
                render_motion_graphic_clip(
                    treatment, clip, duration_sec=duration, fps=24,
                    width=1080, height=1920,
                )
                sc["clip_rel"] = f"clips/{sc['sid']}.mp4"
                sc["status"] = "clip_ready"
                sc["i2v_qa"] = {"status": "pass", "pass": True, "kind": "deterministic_motion_graphic"}
                animated.append(int(sc["index"]))
                _commit_clip_replacement(staged_clip)
                save_scenes(workspace, scenes)
                continue
            max_attempts = max(1, min(2, int(os.getenv("STUDIO_I2V_QA_MAX_ATTEMPTS", "2") or "2")))
            rejected_reports: list[dict[str, Any]] = []
            semantic_passed = False
            current_motion = str(motion_prompt or "")
            qa_attempt = 0
            final_route_restarts = 0
            while qa_attempt < max_attempts:
                attempt = qa_attempt + 1
                clip.unlink(missing_ok=True)
                clip.with_suffix(clip.suffix + ".fal.json").unlink(missing_ok=True)
                clip.with_suffix(clip.suffix + ".visualqa.json").unlink(missing_ok=True)
                route_restart = 0
                while True:
                    dispatch_route = dict(route_resolver() or {}) if route_resolver is not None else {}
                    dispatch_model = str(dispatch_route.get("video_model") or sc.get("video_model") or "").strip()
                    if dispatch_model:
                        sc["video_model"] = dispatch_model
                    lane = (
                        "i2v_premium"
                        if "kling" in dispatch_model.lower() or "premium" in dispatch_model.lower()
                        else "i2v"
                    )
                    dispatch_token = (
                        int(dispatch_route.get("revision") or 1),
                        str(dispatch_route.get("video_model") or ""),
                    )

                    def _fallback_route_is_current() -> bool:
                        if route_resolver is None:
                            return True
                        current_route = dict(route_resolver() or {})
                        current_token = (
                            int(current_route.get("revision") or 1),
                            str(current_route.get("video_model") or ""),
                        )
                        return dispatch_token == current_token

                    try:
                        with production_slot(
                            lane,
                            on_wait=_slot_wait_progress(
                                workspace,
                                "animation_queue",
                                f"Scene {int(sc['index']) + 1} animation attempt {attempt}",
                            ),
                        ):
                            gen_clip(
                                still,
                                current_motion,
                                clip,
                                tier=tier,
                                video_model=sc.get("video_model"),
                                duration_sec=int(duration),
                                fallback_guard=(
                                    _fallback_route_is_current
                                    if route_resolver is not None
                                    else None
                                ),
                            )
                    except I2VRouteChanged:
                        after_provider = dict(route_resolver() or {}) if route_resolver is not None else dispatch_route
                        for artifact in _clip_replacement_artifacts(clip):
                            artifact.unlink(missing_ok=True)
                        route_switches.append({
                            "scene_index": int(sc["index"]),
                            "stage": "video_fallback",
                            "from": dispatch_route,
                            "to": after_provider,
                            "quarantined": "",
                        })
                        final_route = after_provider
                        route_restart += 1
                        if route_restart >= 4:
                            raise RuntimeError(
                                "Media route changed repeatedly before video fallback dispatch"
                            )
                        continue
                    clip_meta = _load_clip_meta(clip)
                    after_provider = dict(route_resolver() or {}) if route_resolver is not None else dispatch_route
                    after_token = (
                        int(after_provider.get("revision") or 1),
                        str(after_provider.get("video_model") or ""),
                    )
                    if route_resolver is None or dispatch_token == after_token:
                        final_route = after_provider or dispatch_route
                        if after_provider:
                            sc["media_route_revision"] = int(after_provider.get("revision") or 1)
                        break
                    _record_animation_attempt_cost(
                        workspace, sc, clip_meta, attempt=attempt, rejected=True,
                    )
                    stale_path = _quarantine_i2v_attempt(
                        workspace,
                        clip,
                        sid=str(sc["sid"]),
                        attempt=attempt * 100 + route_restart + 1,
                    )
                    route_switches.append({
                        "scene_index": int(sc["index"]),
                        "stage": "video",
                        "from": dispatch_route,
                        "to": after_provider,
                        "quarantined": stale_path,
                    })
                    route_restart += 1
                    if route_restart >= 4:
                        raise RuntimeError("Media route changed repeatedly before a safe animation commit")
                try:
                    if is_skeleton:
                        from studio_agent.visual_qa import audit_skeleton_clip
                        semantic = audit_skeleton_clip(
                            clip, still=still,
                            locked_outfit=str(sc.get("outfit") or ""), force=True,
                            cast_count=int(sc.get("cast_count") or _read_job_spec(workspace).get("cast_count") or 1),
                        )
                    else:
                        if is_product:
                            from studio_agent.visual_qa import audit_product_clip
                            semantic = audit_product_clip(clip, still=still, references=product_references, product_name=product_name, force=True)
                        else:
                            from studio_agent.visual_qa import audit_generic_clip
                            semantic = audit_generic_clip(clip, scene_contract=" ".join(str(value or "") for value in (sc.get("prompt"), sc.get("scene_action"), sc.get("narration"))))
                except Exception as qa_exc:
                    semantic = {
                        "status": "fail",
                        "pass": False,
                        "confidence": 0.0,
                        "summary": f"Semantic QA unavailable: {qa_exc}",
                    }
                semantic_passed = semantic.get("status") == "pass" and semantic.get("pass") is True
                _record_animation_attempt_cost(
                    workspace, sc, clip_meta, attempt=attempt, rejected=not semantic_passed
                )
                before_clip_commit = (
                    dict(route_resolver() or {})
                    if route_resolver is not None
                    else dispatch_route
                )
                dispatch_token = (
                    int(dispatch_route.get("revision") or 1),
                    str(dispatch_route.get("video_model") or ""),
                )
                commit_token = (
                    int(before_clip_commit.get("revision") or 1),
                    str(before_clip_commit.get("video_model") or ""),
                )
                if route_resolver is not None and dispatch_token != commit_token:
                    stale_path = _quarantine_i2v_attempt(
                        workspace,
                        clip,
                        sid=str(sc["sid"]),
                        attempt=attempt * 100 + 50 + final_route_restarts,
                    )
                    route_switches.append({
                        "scene_index": int(sc["index"]),
                        "stage": "video_commit",
                        "from": dispatch_route,
                        "to": before_clip_commit,
                        "quarantined": stale_path,
                    })
                    final_route = before_clip_commit
                    final_route_restarts += 1
                    if final_route_restarts >= 4:
                        raise RuntimeError("Media route changed repeatedly before a safe animation commit")
                    # Route churn does not consume a semantic-QA attempt. Retry
                    # immediately with the creator's newest video picker.
                    continue
                qa_attempt += 1
                sc["i2v_qa"] = semantic
                if semantic_passed:
                    break

                rejected_path = _quarantine_i2v_attempt(
                    workspace, clip, sid=str(sc["sid"]), attempt=attempt
                )
                rejected_reports.append({
                    "attempt": attempt,
                    "path": rejected_path,
                    "summary": str(semantic.get("summary") or "Identity QA failed")[:300],
                    "confidence": semantic.get("confidence"),
                    "violations": semantic.get("violations") or {},
                })
                # A semantic-provider outage cannot be repaired by buying the
                # same I2V generation again. Fall back without a blind retry.
                if not str(semantic.get("provider") or "").strip() and float(
                    semantic.get("confidence") or 0.0
                ) <= 0.0:
                    break
                current_motion = (
                    "Keep the SAME ivory skeleton and thin glass shell identity exactly. "
                    "SILENT visual-only: no talking, no jaw/mouth motion, no lip-sync, no dialogue. "
                    "Increase readable body performance (not freeze-frame): clear weight shift, "
                    "torso quarter-turn, one open-hand gesture, glass-shell "
                    "refraction highlights moving, soft rim light along bones, slow camera "
                    "push with background parallax and a lighting change in the physical "
                    "interior. No human skin/flesh/hair, no morph, no text/graphics, no "
                    "chest orbs/brain/circuits. "
                    + str(motion_prompt or "")
                )[:1800]

            if not semantic_passed:
                _identity_safe_motion_fallback(
                    still,
                    clip,
                    duration_sec=duration,
                    rejected_reports=rejected_reports,
                )
                sc["i2v_qa"] = {
                    "status": "pass",
                    "pass": True,
                    "confidence": 1.0,
                    "provider": "deterministic_local",
                    "summary": "Provider I2V was rejected; used identity-safe motion from the approved still",
                    "rejected_attempts": rejected_reports,
                }
                sc["motion_fallback"] = "identity_safe_motion"
                fallback_scenes.append(int(sc["index"]))
            sc["clip_rel"] = f"clips/{sc['sid']}.mp4"
            sc["status"] = "clip_ready"
            sc.pop("error", None)
            sc.pop("last_repair_error", None)
            if route_resolver is not None:
                commit_route = dict(route_resolver() or {})
                final_token = (
                    int(final_route.get("revision") or 1),
                    str(final_route.get("video_model") or ""),
                )
                commit_token = (
                    int(commit_route.get("revision") or 1),
                    str(commit_route.get("video_model") or ""),
                )
                if final_token != commit_token:
                    stale_path = _quarantine_i2v_attempt(
                        workspace,
                        clip,
                        sid=str(sc["sid"]),
                        attempt=999,
                    )
                    route_switches.append({
                        "scene_index": int(sc["index"]),
                        "stage": "video_final_commit",
                        "from": final_route,
                        "to": commit_route,
                        "quarantined": stale_path,
                    })
                    raise RuntimeError("Media route changed at animation commit; stale clip quarantined")
            animated.append(int(sc["index"]))
            _commit_clip_replacement(staged_clip)
        except Exception as exc:  # noqa: BLE001 — surface per-scene, keep others
            repair_error = str(exc)[:300]
            had_previous_clip = any(original == clip for original, _backup in staged_clip)
            _restore_clip_replacement(clip, staged_clip)
            if had_previous_clip and clip.is_file():
                sc["clip_rel"] = previous_clip_rel or f"clips/{sc['sid']}.mp4"
                sc["status"] = (
                    previous_status
                    if previous_status not in {"", "error", "failed"}
                    else "clip_ready"
                )
                sc["last_repair_error"] = repair_error
                sc.pop("error", None)
            else:
                sc["status"] = "error"
                sc["error"] = repair_error
            failed.append(int(sc["index"]))
        save_scenes(workspace, scenes)
    save_scenes(workspace, scenes)
    if failed:
        _write_progress(
            workspace,
            stage="awaiting_scene_review",
            progress=80,
            detail=f"Animation needs review: {len(failed)} scene(s) failed",
        )
        return {
            "status": "partial",
            "animated": animated,
            "failed": failed,
            "identity_safe_fallbacks": fallback_scenes,
            "route": final_route,
            "route_switches": route_switches,
        }
    _write_progress(workspace, stage="awaiting_scene_review", progress=80, detail="Review animation")
    return {
        "status": "animated",
        "animated": animated,
        "failed": [],
        "identity_safe_fallbacks": fallback_scenes,
        "route": final_route,
        "route_switches": route_switches,
    }


# ─── Stage 3: finalize → compose final MP4 ────────────────────────────────────

def _apply_shortform_story_pacing(scenes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Tighten hook + CTA pacing for retention without changing scene order."""
    if len(scenes) < 2:
        return scenes
    ordered = sorted(scenes, key=lambda s: int(s.get("index", 0)))
    first = ordered[0]
    first_dur = float(first.get("duration_sec") or 5.0)
    if first_dur > 4.5:
        first["duration_sec"] = 4.5
    if len(ordered) >= 3:
        for sc in ordered[1:-1]:
            dur = float(sc.get("duration_sec") or 5.0)
            if dur > 5.5:
                sc["duration_sec"] = 5.5
    last = ordered[-1]
    last_dur = float(last.get("duration_sec") or 5.0)
    if last_dur < 4.0:
        last["duration_sec"] = 4.0
    elif last_dur > 6.5:
        last["duration_sec"] = 6.0
    return ordered


def _build_shortform_upload_package(
    *,
    scenes: list[dict[str, Any]],
    topic: str,
    category_key: str,
    render_style: str,
    watermark_text: str,
    captions_enabled: bool,
    script_text: str = "",
) -> str:
    title = str(topic or "Untitled Short").strip() or "Untitled Short"
    safe_topic_tag = re.sub(r"[^a-z0-9]+", "", title.lower())[:32] or "shorts"
    brand_tag = re.sub(r"[^a-z0-9]+", "", str(watermark_text or "").lower())[:32]
    hook = ""
    for sc in sorted(scenes, key=lambda s: int(s.get("index", 0))):
        hook = str(sc.get("narration") or "").strip()
        if hook:
            break
    hook = re.sub(r"\s+", " ", hook)[:140].rstrip(" .")
    topic_words = [w for w in re.findall(r"[A-Za-z0-9']+", title) if len(w) > 2][:6]
    tags = list(dict.fromkeys([
        safe_topic_tag,
        *(w.lower() for w in topic_words[:4]),
        category_key,
        render_style,
        "shorts",
        "youtube shorts",
        "relationship psychology" if re.search(r"\b(men|women|love|relationship|psychology)\b", title, re.I) else "storytelling",
        "nyptid studio",
        brand_tag,
    ]))
    tags = [t for t in tags if t]

    timestamps: list[str] = []
    cursor = 0.0
    for sc in sorted(scenes, key=lambda s: int(s.get("index", 0))):
        mm = int(cursor // 60)
        ss = int(cursor % 60)
        scene_num = int(sc.get("index") or 0) + 1
        label = str(sc.get("narration") or sc.get("prompt") or f"Scene {scene_num}").strip()
        label = re.sub(r"\s+", " ", label)[:70].rstrip(" .,")
        timestamps.append(f"{mm:02d}:{ss:02d} - {label or f'Scene {scene_num}'}")
        cursor += float(sc.get("duration_sec") or 0) or 0

    brand_hashtag = f" #{brand_tag}" if brand_tag else ""
    script_hint = re.sub(r"\s+", " ", str(script_text or "")).strip()[:220]
    if script_hint and script_hint.lower() not in title.lower():
        description = (
            f"{hook or title}\n\n"
            f"{script_hint}\n\n"
            f"Follow {watermark_text} for sharp short-form psychology and storytelling."
        )
    else:
        description = (
            f"{hook or title}\n\n"
            f"This short breaks down {title.lower()} in a fast, visual way — hook, pattern, and takeaway.\n\n"
            f"Follow {watermark_text} for more shorts engineered for retention and clarity."
        )
    if not captions_enabled:
        description += "\n\nCaptions: Off (no burned captions on export)."

    alt_2 = f"{title} — explained in under 60 seconds"
    alt_3 = f"The real reason behind {title[:72]}"
    hashtag_line = f"#shorts #{safe_topic_tag} #storytelling #psychology{brand_hashtag}"

    return f"""Title:
{title}

Alternate Titles:
1. {title}
2. {alt_2}
3. {alt_3}

Hook:
{hook or title}

Description:
{description}

Timestamps:
{chr(10).join(timestamps) if timestamps else "00:00 - Full short"}

Tags:
{", ".join(tags)}

Hashtags:
{hashtag_line}

Thumbnail:
Use the strongest hook frame from scene 1 unless the user explicitly requests a custom thumbnail.

CTA:
Subscribe for more from {watermark_text}.

"""


def finalize_stage(
    workspace: Path, *, tier: str = "standard", voice_id: str | None = None, el: Any = None,
    reedit_instruction: str | None = None,
    watermark_text: str = "Studio",
    captions_enabled: bool = True,
    caption_mode: str = "word",
    sfx_enabled: bool = False,
    sound_design_brief: str = "",
    background_music: str = "off",
    sfx_gain: float = 0.16,
    bgm_gain: float = 0.08,
) -> dict[str, Any]:
    """Ensure every scene has a clip (Ken Burns for non-animated), then VO + compose.
    If reedit_instruction is provided (from Reply & re-edit or re_edit_production tool), we apply
    the intent: force subscribe CTA on the final beat, keep strict 3-word caption rhythm, etc.
    The visuals (stills/clips) are re-used from the existing job — this is the surgical re-edit path.
    """
    workspace = Path(workspace)
    _stills, clips_dir, trimmed_dir, work_dir = _setup_dirs(workspace)
    job_spec = _read_job_spec(workspace)
    voice_pref = str(
        job_spec.get("voice_provider")
        or os.getenv("STUDIO_TTS_PROVIDER")
        or "fal"
    ).strip().lower()
    # Voiceover is FAL MiniMax only — BGM stays whatever the clip/SFX path already uses.
    if voice_pref in {"", "fal", "minimax", "auto"}:
        voice_pref = "fal_only"
    el = el or AutoVoiceClient(provider=voice_pref)
    scenes = sorted(load_scenes(workspace), key=lambda s: s.get("index", 0))
    if not scenes:
        raise RuntimeError("no scenes to finalize")
    scenes = _apply_shortform_story_pacing(scenes)
    save_scenes(workspace, scenes)
    unapproved = [int(s.get("index", -1)) for s in scenes if not s.get("approved_for_video")]
    if unapproved:
        raise RuntimeError(
            "cannot finalize before still approval. Unapproved scenes: "
            + ", ".join(str(i + 1) for i in unapproved if i >= 0)
        )
    script_text = (workspace / "script.txt").read_text(encoding="utf-8").strip()

    def _normalize_tts_text(text: str) -> str:
        clean = str(text or "")
        # Common LLM/script artifacts that make TTS pronounce numbers as two
        # separate ages ("two eight-year-old") instead of a single compound age.
        clean = re.sub(r"\b2\s+8\s*-\s*year\s*-\s*old\b", "28-year-old", clean, flags=re.I)
        clean = re.sub(r"\b2\s+8\s+year\s+old\b", "28-year-old", clean, flags=re.I)
        clean = re.sub(r"\b(\d{2})\s+year\s+old\b", r"\1-year-old", clean, flags=re.I)
        clean = re.sub(r"\bSarah\s+Chene\b", "Sarah Chen", clean, flags=re.I)
        return clean

    # Apply re-edit instruction effects (surgical, no new generation of stills)
    reedit = (reedit_instruction or "").lower()
    wants_cta = "cta" in reedit or "subscribe" in reedit or "subscribers" in reedit or "packag" in reedit
    captions_enabled = bool(captions_enabled) and str(caption_mode or "").strip().lower() != "off"
    wants_word_captions = captions_enabled and any(
        marker in reedit
        for marker in (
            "one word",
            "single word",
            "word-by-word",
            "word by word",
            "each word",
            "every single word",
            "single caption for every",
            "separate caption",
        )
    ) or (captions_enabled and str(caption_mode or "").lower() in {"word", "single_word", "one_word"})
    if wants_cta and scenes:
        last = scenes[-1]
        narr = last.get("narration", "") or ""
        if "subscribe" not in narr.lower() and "hit the sub" not in narr.lower():
            # Append a clean subscribe CTA to the final narration for packaging
            last["narration"] = (narr.rstrip() + " If you enjoyed this, subscribe for more.").strip()
            save_scenes(workspace, scenes)

    trimmed_paths: list[Path] = []
    narration_audios: list[Path] = []
    total = max(len(scenes), 1)
    for n, sc in enumerate(scenes):
        check_cancelled(workspace)
        _write_progress(workspace, stage="compose", progress=10 + int(n / total * 60),
                        detail=f"Composing scene {sc['index'] + 1}/{total}")
        still = workspace / sc["still_rel"]
        clip = clips_dir / f"{sc['sid']}.mp4"
        if not (clip.exists() and clip.stat().st_size > 0):
            _still_to_clip(still, clip, duration_sec=float(sc.get("duration_sec", 5.0)))
            sc["clip_rel"] = f"clips/{sc['sid']}.mp4"
        # Per-scene narration audio for PERFECT sync (visual + 3-word captions timed to voice chunk)
        sc["narration"] = _normalize_tts_text(sc["narration"])
        na_path = work_dir / f"nar_{sc['sid']}.mp3"
        with production_slot(
            "audio",
            on_wait=_slot_wait_progress(workspace, "audio_queue", f"Scene {n + 1} narration"),
        ):
            na = el.synthesize(text=sc["narration"], out_path=na_path, voice_id=voice_id)
        voice_provider = str(getattr(el, "last_provider", "") or "fal")
        amount, note, key, qty, provider_name, unit = production_costs.price_tts(voice_provider, sc["narration"])
        provider, amount, note = _metered_provider_values(provider_name, amount, note)
        production_costs.record_event(
            workspace,
            stage="narration",
            provider=provider,
            operation=key,
            usd=amount,
            quantity=qty,
            unit=unit,
            scene_index=int(sc.get("index", n)),
            metadata={"pricing_note": note, "chars": len(str(sc.get("narration") or "")), "voice_provider": voice_provider},
        )
        na_dur = probe_duration(na) or float(sc.get("duration_sec", 5.0))
        trimmed = trim_with_captions(
            clip, trimmed_dir / f"{sc['sid']}.mp4",
            duration_sec=na_dur, narration_text=sc["narration"],
            watermark_text=watermark_text,
            caption_mode="word" if wants_word_captions else "phrase",
            captions_enabled=captions_enabled,
            preserve_source_audio=True,
            force=bool(reedit_instruction) or not captions_enabled,
        )
        trimmed_paths.append(trimmed)
        narration_audios.append(na)
    save_scenes(workspace, scenes)

    check_cancelled(workspace)
    _write_progress(workspace, stage="narration", progress=78, detail="Voiceover")
    # Concat per-scene narration audios for the final VO track (ensures exact sync with visuals)
    narration_target = workspace / "narration.mp3"
    if narration_audios:
        cmd = ["ffmpeg", "-y", "-loglevel", "error"]
        for na in narration_audios:
            cmd.extend(["-i", str(na)])
        concat_inputs = "".join(f"[{i}:a:0]" for i in range(len(narration_audios)))
        cmd.extend([
            "-filter_complex",
            f"{concat_inputs}concat=n={len(narration_audios)}:v=0:a=1[aout]",
            "-map", "[aout]",
            "-c:a", "libmp3lame",
            "-q:a", "2",
            str(narration_target),
        ])
        subprocess.run(cmd, check=True, capture_output=True)
        narration_audio = narration_target
    else:
        with production_slot(
            "audio",
            on_wait=_slot_wait_progress(workspace, "audio_queue", "Narration"),
        ):
            narration_audio = el.synthesize(text=script_text, out_path=narration_target, voice_id=voice_id)
        voice_provider = str(getattr(el, "last_provider", "") or "fal")
        amount, note, key, qty, provider_name, unit = production_costs.price_tts(voice_provider, script_text)
        provider, amount, note = _metered_provider_values(provider_name, amount, note)
        production_costs.record_event(
            workspace,
            stage="narration",
            provider=provider,
            operation=key,
            usd=amount,
            quantity=qty,
            unit=unit,
            metadata={"pricing_note": note, "chars": len(script_text), "voice_provider": voice_provider},
        )

    mixed_audio = narration_audio
    sound_result: dict[str, Any] = {
        "sfx_enabled": bool(sfx_enabled),
        "sfx_generated": 0,
        "sfx_track": "",
        "background_music": background_music,
        "bgm_generated": False,
        "bgm_track": "",
        "mixed_audio_path": "",
    }
    if sfx_enabled:
        try:
            _write_progress(workspace, stage="sound_design", progress=84, detail="Generating sound design")
            sfx_paths: list[Path] = []
            for idx, sc in enumerate(scenes):
                prompt = _derive_short_sfx_prompt(sc, sound_design_brief=sound_design_brief)
                if not prompt:
                    continue
                duration = (
                    probe_duration(narration_audios[idx])
                    if idx < len(narration_audios)
                    else float(sc.get("duration_sec") or 5.0)
                )
                with production_slot(
                    "audio",
                    on_wait=_slot_wait_progress(workspace, "audio_queue", f"Scene {idx + 1} SFX"),
                ):
                    generated = _generate_short_audio_bed(prompt, duration, work_dir / f"sfx_{sc['sid']}.mp3")
                if generated:
                    amount, note = production_costs.fal_unit_cost(
                        "mmaudio_v2",
                        fallback_key="mmaudio_v2_per_second",
                        quantity=float(duration or 0.0),
                    )
                    provider, amount, note = _metered_provider_values("fal", amount, note)
                    production_costs.record_event(
                        workspace,
                        stage="sound_design",
                        provider=provider,
                        operation="mmaudio_v2",
                        usd=amount,
                        quantity=float(duration or 0.0),
                        unit="second",
                        scene_index=int(sc.get("index", idx)),
                        metadata={"pricing_note": note},
                    )
                    sfx_paths.append(generated)
            sfx_track = _concat_audio_tracks(sfx_paths, work_dir / "sfx_full.mp3")
            if sfx_track:
                sound_result["sfx_generated"] = len(sfx_paths)
                sound_result["sfx_track"] = str(sfx_track)

            bgm_track: Path | None = None
            bgm_choice = str(background_music or "off").strip()
            if bgm_choice.lower() not in {"", "off", "none", "no", "no background music"}:
                bgm_prompt = (
                    f"{sound_design_brief}. "
                    if sound_design_brief else ""
                ) + (
                    f"Short-form background music bed: {bgm_choice}. "
                    "Instrumental only, low-volume, subtle, loopable, no vocals, no lyrics, no lead melody that fights narration."
                )
                with production_slot(
                    "audio",
                    on_wait=_slot_wait_progress(workspace, "audio_queue", "Background music"),
                ):
                    bgm_track = _generate_short_audio_bed(
                        bgm_prompt,
                        float(probe_duration(narration_audio) or 30.0),
                        work_dir / "bgm.mp3",
                    )
                if bgm_track:
                    bgm_duration = float(probe_duration(narration_audio) or 30.0)
                    amount, note = production_costs.fal_unit_cost(
                        "mmaudio_v2",
                        fallback_key="mmaudio_v2_per_second",
                        quantity=bgm_duration,
                    )
                    provider, amount, note = _metered_provider_values("fal", amount, note)
                    production_costs.record_event(
                        workspace,
                        stage="sound_design",
                        provider=provider,
                        operation="mmaudio_v2_background_music",
                        usd=amount,
                        quantity=bgm_duration,
                        unit="second",
                        metadata={"pricing_note": note},
                    )
                    sound_result["bgm_generated"] = True
                    sound_result["bgm_track"] = str(bgm_track)

            if sfx_track or bgm_track:
                mixed = _mix_short_sound_design(
                    narration_audio,
                    sfx_track=sfx_track,
                    bgm_track=bgm_track,
                    out_path=work_dir / "narration_sound_mix.mp3",
                    sfx_gain=sfx_gain,
                    bgm_gain=bgm_gain,
                )
                mixed_audio = mixed
                sound_result["mixed_audio_path"] = str(mixed)
        except Exception as exc:  # noqa: BLE001 - sound design should not destroy a finished render
            sound_result["warning"] = str(exc)[:300]
            mixed_audio = narration_audio

    _write_progress(workspace, stage="compose", progress=92, detail="Muxing final MP4")
    with production_slot(
        "compose",
        on_wait=_slot_wait_progress(workspace, "compose_queue", "Final MP4"),
    ):
        silent = concat_demuxer(trimmed_paths, workspace / "silent.mp4", work_dir)
        final = mux_narration(silent, mixed_audio, workspace / "styled_short.mp4")

    animated = [s["index"] for s in scenes if s.get("animate")]
    meta = _read_result(workspace)
    ac_cost = sum(
        ac_cost_for_video_model(video_model=s.get("video_model"), tier=tier)
        for s in scenes if s.get("animate")
    )
    result = {
        **{k: meta.get(k) for k in ("render_style", "render_style_label", "stills_model", "category", "topic")},
        "status": "complete", "job_id": workspace.name,
        "video_path": str(final), "script_path": str(workspace / "script.txt"),
        "narration_path": str(narration_audio),
        "final_audio_path": str(mixed_audio),
        "sound_design": sound_result,
        "captions_enabled": captions_enabled,
        "caption_mode": "word" if captions_enabled else "off",
        "scene_count": len(scenes), "animated_scenes": animated,
        "tier": tier, "ac_charged": ac_cost,
        "cost": production_costs.load_summary(workspace),
    }
    _write_result(workspace, result)

    topic = str(meta.get("topic") or "Untitled Short").strip()
    category_key = str(meta.get("category") or "short").strip()
    render_style = str(meta.get("render_style") or "cinematic").strip()
    script_for_pkg = (workspace / "script.txt").read_text(encoding="utf-8").strip() if (workspace / "script.txt").is_file() else ""
    pkg = _build_shortform_upload_package(
        scenes=scenes,
        topic=topic,
        category_key=category_key,
        render_style=render_style,
        watermark_text=watermark_text,
        captions_enabled=bool(captions_enabled),
        script_text=script_for_pkg,
    )
    (workspace / "package.txt").write_text(pkg, encoding="utf-8")
    return result


def run_styled(
    category_key: str,
    topic: str | None,
    workspace: Path,
    *,
    render_style: str,
    tier: str = "standard",
    video_model: str | None = None,
    visual_brief: str | None = None,
    beats_target: int = 12,
    grok: GrokClient | None = None,
    el: Any = None,
    voice_id: str | None = None,
    script_override: str | None = None,
    user_id: str | None = None,
    animate: bool = True,
    watermark_text: str = "Studio",
    captions_enabled: bool = True,
    caption_mode: str = "word",
) -> dict:
    """Straight-through render (no review gate) — fallback / auto path."""
    workspace = Path(workspace)
    plan_scenes(
        category_key, topic, workspace, render_style=render_style, tier=tier,
        video_model=video_model, visual_brief=visual_brief, beats_target=beats_target,
        grok=grok, script_override=script_override, user_id=user_id, default_animate=animate,
    )
    if animate:
        animate_scenes_stage(workspace, indices=None, tier=tier)
    return finalize_stage(
        workspace,
        tier=tier,
        voice_id=voice_id,
        el=el,
        watermark_text=watermark_text,
        captions_enabled=captions_enabled,
        caption_mode=caption_mode,
    )
