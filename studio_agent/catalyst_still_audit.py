"""Catalyst still audit — deterministic artifact diagnosis for scene regeneration.

Runs on CPU + existing scene metadata (no RunPod GPU). Optionally uses PIL for
light diptych/seam heuristics. Records visual watchouts into Catalyst channel memory
so Studio learns from every artifact-driven regenerate.
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any

from skeleton_ai.canonical_edit import sanitize_skeleton_scene_action
from skeleton_ai.prompt_compose import compact_identity_locks, compact_skeleton_scene_direction

_ISSUE_LABELS: dict[str, str] = {
    "split_screen_diptych": "split-screen / diptych layout (duplicates the skeleton)",
    "extra_hand": "extra or duplicate hands",
    "torso_eyes": "eyeballs outside skull sockets (ribcage/chest eyes)",
    "prompt_split_language": "scene prompt requested a comparison/split layout",
    "center_seam_heuristic": "vertical seam suggests a diptych composition",
    "catalyst_prior_watchout": "matched a prior Catalyst visual watchout",
    "identity_drift": "canonical skeleton identity drift",
    "human_or_skin": "human skin, flesh, hair, or human substitution",
    "anatomy_artifact": "incorrect bones, eyes, hands, or limbs",
    "wardrobe_drift": "wardrobe changed from the locked outfit",
    "layout_artifact": "duplicate host or collage layout",
    "symbolic_clutter": "floating brain, molecule, diagram, or literalized metaphor",
    "text_artifact": "readable text or watermark",
    "background_artifact": "black void or incoherent environment",
}

_DEFAULT_SKELETON_WATCHOUTS: tuple[str, ...] = (
    "Never use split-screen or diptych layouts for MrSkeleWelly — they duplicate hands.",
    "Skeleton shorts need exactly two hands in one continuous full-frame shot.",
    "Ban side-by-side comparison panels; use one scene with sequential metaphor instead.",
    "Eyes exist only in the skull sockets — never eyeballs in the ribcage, sternum, or chest cavity.",
    "Soft amber light may illuminate spine bones only — never render chest glow as literal eyes.",
)


def _clip(text: str, max_chars: int = 220) -> str:
    value = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(value) <= max_chars:
        return value
    return value[: max(0, max_chars - 3)].rstrip() + "..."


def _catalyst_memory_path() -> Path:
    from studio_agent.catalyst_skeleton_reference import catalyst_channel_memory_path

    return catalyst_channel_memory_path()


def _load_channel_memory(channel_key: str) -> dict[str, Any]:
    path = _catalyst_memory_path()
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    bucket = data.get(channel_key)
    return dict(bucket) if isinstance(bucket, dict) else {}


def _save_channel_memory(channel_key: str, bucket: dict[str, Any]) -> None:
    if not channel_key:
        return
    path = _catalyst_memory_path()
    try:
        data: dict[str, Any] = {}
        if path.is_file():
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                data = loaded
        data[channel_key] = bucket
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception:
        pass


def _update_weighted_signals(bucket: dict[str, Any], key: str, signals: list[str], weight: float = 0.35) -> None:
    raw_map = bucket.get(key)
    signal_map: dict[str, dict[str, Any]] = dict(raw_map) if isinstance(raw_map, dict) else {}
    for signal in signals:
        text = _clip(str(signal or "").strip(), 180)
        if not text:
            continue
        entry = dict(signal_map.get(text) or {})
        entry["n"] = int(entry.get("n", 0) or 0) + 1
        entry["weight"] = float(entry.get("weight", 0.0) or 0.0) + float(weight or 0.35)
        entry["last_seen"] = time.time()
        signal_map[text] = entry
    bucket[key] = signal_map


def _dedupe(values: list[str], *, max_items: int = 10) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = _clip(str(raw or "").strip(), 180)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= max_items:
            break
    return out


def _detect_center_seam(still_path: Path) -> bool:
    """Light CPU heuristic: strong vertical seam near image center suggests diptych."""
    try:
        from PIL import Image
    except Exception:
        return False
    try:
        img = Image.open(still_path).convert("RGB")
        w, h = img.size
        if w < 200 or h < 200:
            return False
        mid = w // 2
        seam_w = max(2, int(w * 0.02))
        left_x = max(0, mid - seam_w)
        right_x = min(w - 1, mid + seam_w)

        def _edge_energy(x: int) -> float:
            total = 0.0
            count = 0
            step = max(1, h // 64)
            for y in range(0, h - 1, step):
                if x <= 0 or x >= w - 1:
                    continue
                pl = img.getpixel((x - 1, y))
                pr = img.getpixel((x + 1, y))
                total += sum(abs(int(pl[i]) - int(pr[i])) for i in range(3))
                count += 1
            return total / max(count, 1)

        center_energy = _edge_energy(mid)
        side_energy = (_edge_energy(w // 4) + _edge_energy((3 * w) // 4)) / 2.0
        return center_energy > side_energy * 1.55 and center_energy > 18.0
    except Exception:
        return False


def _channel_key_from_workspace(workspace: Path) -> str:
    for name in ("job_spec.json", "result.json"):
        path = workspace / name
        if not path.is_file():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        for key in ("channel_key", "registry_key", "category", "render_style"):
            value = str(data.get(key) or "").strip()
            if value:
                return value
    return "mrskelewelly"


def audit_scene_still(workspace: Path, scene_index: int, *, still_path: Path | None = None) -> dict[str, Any]:
    """Diagnose likely artifact causes for a scene still before regeneration."""
    workspace = Path(workspace)
    from skeleton_ai.styled_pipeline import load_scenes
    from skeleton_ai.prompt_compose import dual_host_staging_brief, resolve_cast_count

    scenes = load_scenes(workspace)
    scene = next((s for s in scenes if int(s.get("index", -1)) == int(scene_index)), None)
    if not scene:
        raise ValueError(f"scene {scene_index} not found")

    try:
        job_spec = json.loads((workspace / "job_spec.json").read_text(encoding="utf-8"))
        if not isinstance(job_spec, dict):
            job_spec = {}
    except Exception:
        job_spec = {}
    hosts = resolve_cast_count(
        job_cast=job_spec.get("cast_count"),
        scene_cast=scene.get("cast_count"),
        topic=str(scene.get("topic") or job_spec.get("topic") or ""),
        visual_brief=str(scene.get("visual_brief") or job_spec.get("visual_brief") or ""),
        narration=str(scene.get("narration") or ""),
        scene_action=str(scene.get("scene_action") or ""),
    )

    if still_path is None:
        rel = str(scene.get("still_rel") or "").strip()
        still_path = workspace / rel if rel else workspace / "stills" / f"{scene.get('sid', f'b{scene_index:02d}')}.png"

    prompt_blob = " ".join(
        str(scene.get(key) or "")
        for key in ("prompt", "scene_action", "outfit", "narration")
    )
    low = prompt_blob.lower()
    issues: list[str] = []
    details: list[str] = []

    _, prompt_risky = sanitize_skeleton_scene_action(
        str(scene.get("scene_action") or ""),
        topic=str(scene.get("topic") or job_spec.get("topic") or ""),
        visual_brief=str(scene.get("visual_brief") or job_spec.get("visual_brief") or ""),
        narration=str(scene.get("narration") or ""),
        cast_count=hosts,
    )
    if prompt_risky:
        issues.append("prompt_split_language")
        details.append("Scene action contained split-screen / comparison language.")

    split_terms = ("split screen", "diptych", "side by side", "left side", "right side", "comparison")
    if any(term in low for term in split_terms):
        if "prompt_split_language" not in issues:
            issues.append("prompt_split_language")
        details.append("Stored prompt still references a split or comparison layout.")

    hand_terms = ("extra hand", "third hand", "duplicate hand", "floating hand", "warped hand")
    if any(term in low for term in hand_terms):
        issues.append("extra_hand")
        details.append("Prompt or prior notes mention hand/limb artifacting.")

    # Eyes-on-torso: "realistic eyeballs" + "chest glow" / feminine wardrobe often causes ribcage eyes.
    torso_eye_terms = (
        "eyes in chest", "eyes in rib", "eyeballs in chest", "chest cavity",
        "eyes on sternum", "breast", "internal chest",
    )
    if any(term in low for term in torso_eye_terms):
        issues.append("torso_eyes")
        details.append("Prompt language risks placing eyeballs outside the skull sockets.")
    # Amber chest/spine glow without "bone light only" is a known trigger.
    if ("chest" in low and "glow" in low) or ("spine" in low and "glow" in low and "eye" in low):
        if "torso_eyes" not in issues:
            issues.append("torso_eyes")
            details.append("Chest/spine glow language may be misread as literal eye orbs in the torso.")

    if still_path.is_file() and _detect_center_seam(still_path):
        issues.append("center_seam_heuristic")
        issues.append("split_screen_diptych")
        details.append("Image shows a strong vertical center seam consistent with a diptych.")

    semantic_failed = False
    if still_path.is_file():
        try:
            from studio_agent.visual_qa import audit_skeleton_still, _workspace_skeleton_reference

            semantic = audit_skeleton_still(
                still_path,
                reference=_workspace_skeleton_reference(workspace),
                locked_outfit=str(scene.get("outfit") or ""),
                cast_count=hosts,
            )
            if semantic.get("status") != "pass":
                semantic_failed = True
                for issue in list(semantic.get("issues") or []):
                    issue = str(issue)
                    if issue and issue not in issues:
                        issues.append(issue)
                summary = str(semantic.get("summary") or semantic.get("error") or "").strip()
                if summary:
                    details.append(f"Semantic still QA: {summary}")
        except Exception as exc:
            semantic_failed = True
            issues.append("identity_drift")
            details.append(f"Semantic still QA could not verify the canonical host: {exc}")

    channel_key = _channel_key_from_workspace(workspace)
    memory = _load_channel_memory(channel_key)
    watchouts = [
        str(v).strip()
        for v in list(memory.get("visual_watchouts") or [])
        if str(v).strip()
    ]
    for watchout in watchouts + list(_DEFAULT_SKELETON_WATCHOUTS):
        wlow = watchout.lower()
        if any(term in wlow for term in ("split", "diptych", "extra hand", "third hand", "duplicate")):
            if "catalyst_prior_watchout" not in issues:
                issues.append("catalyst_prior_watchout")
            break

    if not issues:
        issues.extend(["layout_artifact", "anatomy_artifact"])
        details.append(
            "User requested artifact cleanup; master-regenerate with a short cast-aware prompt."
        )

    issues = list(dict.fromkeys(issues))
    setting = compact_skeleton_scene_direction(
        str(scene.get("scene_action") or scene.get("prompt") or ""), max_chars=120
    )
    outfit = _clip(str(scene.get("outfit") or "no clothing"), 40)

    if hosts >= 2:
        fix_instruction = (
            f"Master regenerate. {setting or 'Physical interior'}. "
            f"{dual_host_staging_brief()} Outfit: {outfit}."
        )[:280]
    else:
        fix_instruction = (
            f"Master regenerate one host. {setting or 'Physical interior'}. "
            f"Thin glass skin on bones only; empty hands; no chest orb/pod. Outfit: {outfit}."
        )[:280]

    from studio_agent.catalyst_skeleton_reference import (
        catalyst_regenerate_method_for_issues,
    )

    # Glass/orb/layout defects cannot be patched by editing the broken still.
    method = "regenerate"
    if not (
        {"symbolic_clutter", "layout_artifact", "background_artifact", "fused_glass",
         "shared_bubble", "glass_pod", "split_screen_diptych", "center_seam_heuristic",
         "prompt_split_language", "identity_drift"} & set(issues)
    ):
        method = (
            "regenerate"
            if semantic_failed
            else catalyst_regenerate_method_for_issues(issues, still_exists=still_path.is_file())
        )
    # Artifact cleanup always prefers a fresh master render over editing sludge.
    if any(term in " ".join(details).lower() for term in ("artifact", "orb", "bubble", "pod", "fuse")):
        method = "regenerate"

    return {
        "scene_index": int(scene_index),
        "channel_key": channel_key,
        "issues": issues,
        "issue_labels": [_ISSUE_LABELS.get(i, i) for i in issues],
        "details": details,
        "method": method,
        "cast_count": hosts,
        "fix_instruction": fix_instruction,
        "style_lock": "Short cast-aware master regenerate; never bloat the prompt.",
        "catalyst_engine": "cpu_metadata_audit",
        "gpu_required": False,
    }


def record_catalyst_still_artifact_learning(
    *,
    channel_key: str,
    audit: dict[str, Any],
    job_id: str = "",
    scene_index: int = 0,
) -> dict[str, Any]:
    """Append artifact watchouts to Catalyst channel memory (self-learning loop)."""
    channel_key = str(channel_key or "").strip() or "mrskelewelly"
    bucket = _load_channel_memory(channel_key)
    bucket.setdefault("key", channel_key)
    bucket["updated_at"] = time.time()
    bucket["last_still_artifact_job_id"] = str(job_id or "")
    bucket["last_still_artifact_scene"] = int(scene_index)

    watchouts: list[str] = []
    for issue in list(audit.get("issues") or []):
        label = _ISSUE_LABELS.get(str(issue), str(issue))
        watchouts.append(f"On regenerate, avoid {label} in skeleton shorts.")
    watchouts.extend(_DEFAULT_SKELETON_WATCHOUTS)

    wins = [
        "Single full-frame skeleton shots with exactly two hands pass review.",
        "Preserve room/lighting/wardrobe exactly; only fix limb and layout artifacts.",
    ]

    bucket["visual_watchouts"] = _dedupe([*watchouts, *list(bucket.get("visual_watchouts") or [])])
    bucket["visual_learnings"] = _dedupe([*wins, *list(bucket.get("visual_learnings") or [])])
    _update_weighted_signals(bucket, "visual_watchouts_map", watchouts, weight=0.42)
    _update_weighted_signals(bucket, "visual_wins_map", wins, weight=0.22)
    _save_channel_memory(channel_key, bucket)
    return {"channel_key": channel_key, "watchouts_recorded": len(watchouts)}
