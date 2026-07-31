"""Visual correctness QA for Studio short-form (skeleton identity + temporal drift).

Complements render_qa (ffprobe/file checks). This module catches creative failures:
skeleton→human, black-void backgrounds, extra-limb risk signals, clothing contradiction
metadata, and optional frame-to-frame identity drift on clips.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

VISUAL_QA_VERSION = 5
SEMANTIC_QA_VERSION = 6
# Bump whenever the acceptance contract changes so old cached decisions
# cannot keep a scene blocked after the rules have been corrected.
STILL_SEMANTIC_QA_VERSION = 8
PRODUCT_SEMANTIC_QA_VERSION = 2
SCENE_VISUAL_QA_VERSION = 2


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _asset_version_token(path: Path | None) -> str:
    if path is None:
        return "none"
    asset = Path(path)
    try:
        digest = hashlib.sha256()
        with asset.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return f"{asset.name}:sha256:{digest.hexdigest()}"
    except OSError:
        return f"{asset.name}:missing"


def _scene_contract_text(scene: dict[str, Any]) -> str:
    action = str(scene.get("scene_action") or scene.get("action") or "").strip()
    action = re.sub(
        r"(?is)\b(?:PERFORMANCE|MOTION|VFX|SILENT|i2v)\b.*$",
        "",
        action,
    ).strip(" .;")
    return " ".join(
        part
        for part in (str(scene.get("narration") or "").strip(), action)
        if part
    )[:900]


def scene_visual_qa_fingerprint(
    workspace: Path,
    scene: dict[str, Any],
    *,
    previous_scene: dict[str, Any] | None = None,
    next_scene: dict[str, Any] | None = None,
) -> str:
    """Bind one aggregate verdict to exact assets, contract, cast, and neighbors."""

    workspace = Path(workspace)

    def _still(row: dict[str, Any] | None) -> Path | None:
        if not row:
            return None
        sid = str(row.get("sid") or f"b{int(row.get('index') or 0):02d}")
        return workspace / str(row.get("still_rel") or f"stills/{sid}.png")

    sid = str(scene.get("sid") or f"b{int(scene.get('index') or 0):02d}")
    still = _still(scene)
    clip_rel = str(scene.get("clip_rel") or "").strip()
    clip = workspace / clip_rel if clip_rel else workspace / "clips" / f"{sid}.mp4"
    parts = [
        str(SCENE_VISUAL_QA_VERSION),
        _asset_version_token(still),
        _asset_version_token(clip if clip_rel or clip.is_file() else None),
        _asset_version_token(_still(previous_scene)),
        _asset_version_token(_still(next_scene)),
        _scene_contract_text(scene),
        str(scene.get("cast_count") or 1),
        str(scene.get("outfit") or ""),
        str(bool(scene.get("clip_rel"))),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def build_scene_visual_qa(
    workspace: Path,
    scene: dict[str, Any],
    *,
    previous_scene: dict[str, Any] | None = None,
    next_scene: dict[str, Any] | None = None,
    still_qa: dict[str, Any] | None = None,
    correspondence_qa: dict[str, Any] | None = None,
    clip_qa: dict[str, Any] | None = None,
    require_clip: bool | None = None,
) -> dict[str, Any]:
    """Create the authoritative identity + story + clip acceptance state."""

    still_report = dict(still_qa or scene.get("still_qa") or {})
    correspondence_report = dict(
        correspondence_qa or scene.get("scene_correspondence_qa") or {}
    )
    clip_report = dict(clip_qa or scene.get("i2v_qa") or {})
    needs_clip = (
        bool(scene.get("clip_rel"))
        if require_clip is None
        else bool(require_clip)
    )
    still_passed = (
        still_report.get("status") == "pass"
        and still_report.get("pass") is True
    )
    correspondence_passed = (
        correspondence_report.get("status") == "pass"
        and correspondence_report.get("pass") is True
    )
    clip_passed = (
        not needs_clip
        or (
            clip_report.get("status") == "pass"
            and clip_report.get("pass") is True
        )
    )
    passed = bool(still_passed and correspondence_passed and clip_passed)
    failed_components = [
        name
        for name, ok in (
            ("identity", still_passed),
            ("correspondence", correspondence_passed),
            ("clip", clip_passed),
        )
        if not ok
    ]
    summaries = [
        str(report.get("summary") or "").strip()
        for report in (still_report, correspondence_report, clip_report if needs_clip else {})
        if isinstance(report, dict) and str(report.get("summary") or "").strip()
    ]
    return {
        "version": SCENE_VISUAL_QA_VERSION,
        "fingerprint": scene_visual_qa_fingerprint(
            Path(workspace),
            scene,
            previous_scene=previous_scene,
            next_scene=next_scene,
        ),
        "status": "pass" if passed else "fail",
        "pass": passed,
        "require_clip": needs_clip,
        "failed_components": failed_components,
        "summary": (
            "Identity, scene correspondence, and required clip QA passed"
            if passed
            else "; ".join(summaries)[:700]
            or f"QA not proven: {', '.join(failed_components)}"
        ),
        "still": still_report,
        "correspondence": correspondence_report,
        "clip": clip_report if needs_clip else {},
        "created_at": time.time(),
    }


def scene_visual_qa_is_fresh(
    workspace: Path,
    scene: dict[str, Any],
    *,
    previous_scene: dict[str, Any] | None = None,
    next_scene: dict[str, Any] | None = None,
) -> bool:
    aggregate = scene.get("visual_qa") if isinstance(scene.get("visual_qa"), dict) else {}
    expected_clip = bool(scene.get("clip_rel"))
    return bool(
        aggregate
        and int(aggregate.get("version") or 0) == SCENE_VISUAL_QA_VERSION
        and bool(aggregate.get("require_clip")) == expected_clip
        and str(aggregate.get("fingerprint") or "")
        == scene_visual_qa_fingerprint(
            Path(workspace),
            scene,
            previous_scene=previous_scene,
            next_scene=next_scene,
        )
    )


def invalidate_scene_visual_qa(
    scene: dict[str, Any],
    *,
    still_changed: bool = False,
    clip_changed: bool = False,
    contract_changed: bool = False,
) -> None:
    scene.pop("visual_qa", None)
    scene["qa_stale"] = True
    if still_changed or contract_changed:
        scene.pop("still_qa", None)
        scene.pop("scene_correspondence_qa", None)
        scene.pop("i2v_qa", None)
    elif clip_changed:
        scene.pop("i2v_qa", None)


def _asset_cast_count(asset_path: Path, requested: Any = None) -> int:
    """Resolve an omitted cast from the durable job/scene contract near an asset."""
    try:
        explicit = int(requested)
    except Exception:
        explicit = 0
    if explicit in {1, 2}:
        return explicit

    asset_path = Path(asset_path)
    workspace: Path | None = None
    for depth, parent in enumerate(asset_path.parents):
        if (parent / "job_spec.json").is_file() or (parent / "scenes.json").is_file():
            workspace = parent
            break
        if depth >= 5:
            break
    if workspace is None:
        return 1

    try:
        spec = json.loads((workspace / "job_spec.json").read_text(encoding="utf-8"))
        if not isinstance(spec, dict):
            spec = {}
    except Exception:
        spec = {}
    try:
        raw_scenes = json.loads((workspace / "scenes.json").read_text(encoding="utf-8"))
        scenes = [item for item in raw_scenes if isinstance(item, dict)] if isinstance(raw_scenes, list) else []
    except Exception:
        scenes = []

    resolved_asset = asset_path.resolve()
    scene: dict[str, Any] = {}
    for item in scenes:
        candidates = [item.get("still_rel"), item.get("clip_rel")]
        matches_path = False
        for rel in candidates:
            if not str(rel or "").strip():
                continue
            try:
                if (workspace / str(rel)).resolve() == resolved_asset:
                    matches_path = True
                    break
            except Exception:
                continue
        if matches_path or str(item.get("sid") or "").strip() == asset_path.stem:
            scene = item
            break

    from skeleton_ai.prompt_compose import resolve_cast_count

    return resolve_cast_count(
        job_cast=spec.get("cast_count"),
        scene_cast=scene.get("cast_count"),
        topic=str(scene.get("topic") or spec.get("topic") or ""),
        visual_brief=str(scene.get("visual_brief") or spec.get("visual_brief") or ""),
        narration=str(scene.get("narration") or ""),
        scene_action=str(scene.get("scene_action") or ""),
    )


def analyze_shortform_workspace(workspace: Path) -> dict[str, Any]:
    """Run visual QA across scenes.json + stills (+ clips when present)."""
    workspace = Path(workspace)
    scenes_path = workspace / "scenes.json"
    scenes: list[dict[str, Any]] = []
    if scenes_path.is_file():
        try:
            raw = json.loads(scenes_path.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                scenes = [s for s in raw if isinstance(s, dict)]
        except Exception:
            scenes = []

    checks: list[dict[str, Any]] = []
    scene_reports: list[dict[str, Any]] = []
    locked_outfit = ""
    render_style = ""
    product_identity_required = False
    job_cast_count: Any = None
    try:
        spec = json.loads((workspace / "job_spec.json").read_text(encoding="utf-8"))
        locked_outfit = str((spec or {}).get("locked_outfit") or "").strip()
        render_style = str((spec or {}).get("render_style") or "").strip().lower()
        job_cast_count = (spec or {}).get("cast_count")
        product = (spec or {}).get("product_reference")
        product_identity_required = isinstance(product, dict) and bool(product.get("images"))
    except Exception:
        pass
    strict_skeleton_identity = "skeleton" in render_style

    for position, sc in enumerate(scenes):
        report = _scene_visual_report(
            workspace,
            sc,
            locked_outfit=locked_outfit,
            strict_skeleton_identity=strict_skeleton_identity,
            product_identity_required=product_identity_required,
            job_cast_count=job_cast_count,
        )
        scene_reports.append(report)
        for c in report.get("checks") or []:
            checks.append(c)
        aggregate = sc.get("visual_qa") if isinstance(sc.get("visual_qa"), dict) else {}
        aggregate_fresh = scene_visual_qa_is_fresh(
            workspace,
            sc,
            previous_scene=scenes[position - 1] if position > 0 else None,
            next_scene=scenes[position + 1] if position + 1 < len(scenes) else None,
        )
        checks.append({
            "id": f"scene_{int(sc.get('index', position))}_aggregate_qa",
            "label": f"Scene {int(sc.get('index', position)) + 1} aggregate QA is current",
            "status": (
                "pass"
                if aggregate_fresh and aggregate.get("pass") is True
                else "fail"
            ),
            "detail": (
                str(aggregate.get("summary") or "Identity, correspondence, and clip QA passed")
                if aggregate_fresh
                else "Aggregate QA is missing or stale for the current asset"
            ),
        })

    # Job-level: all approved animations should exist
    anim_targets = [
        sc for sc in scenes
        if sc.get("approved_for_animation") or sc.get("animate")
    ]
    if anim_targets:
        missing = []
        for sc in anim_targets:
            sid = str(sc.get("sid") or f"b{int(sc.get('index', 0)):02d}")
            rel = str(sc.get("clip_rel") or f"clips/{sid}.mp4")
            path = workspace / rel
            if not path.is_file() or path.stat().st_size < 50_000:
                missing.append(int(sc.get("index", -1)))
        checks.append({
            "id": "animation_complete",
            "label": "Approved scenes have clips",
            "status": "fail" if missing else "pass",
            "detail": f"Missing clips for scenes {missing}" if missing else f"{len(anim_targets)} clips present",
        })

    status = "fail" if any(c.get("status") == "fail" for c in checks) else (
        "warn" if any(c.get("status") == "warn" for c in checks) else "pass"
    )
    score = _score(checks)
    out = {
        "version": VISUAL_QA_VERSION,
        "kind": "shortform_visual",
        "status": status,
        "score": score,
        "summary": _summary(status, score, checks),
        "checks": checks,
        "scenes": scene_reports,
        "created_at": time.time(),
        "ready_to_publish": status == "pass" and score >= 75,
    }
    try:
        (workspace / "visual_qa.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    except Exception:
        pass
    return out


def _scene_visual_report(
    workspace: Path,
    sc: dict[str, Any],
    *,
    locked_outfit: str = "",
    strict_skeleton_identity: bool = False,
    product_identity_required: bool = False,
    job_cast_count: Any = None,
) -> dict[str, Any]:
    idx = int(sc.get("index", -1))
    checks: list[dict[str, Any]] = []
    prompt = str(sc.get("prompt") or "")
    action = str(sc.get("scene_action") or "")
    outfit = str(sc.get("outfit") or "")
    motion = str(sc.get("motion_prompt") or "")
    scene_cast_count = 1
    if strict_skeleton_identity:
        from skeleton_ai.prompt_compose import resolve_cast_count

        scene_cast_count = resolve_cast_count(
            job_cast=job_cast_count,
            scene_cast=sc.get("cast_count"),
            topic=str(sc.get("topic") or ""),
            narration=str(sc.get("narration") or ""),
            scene_action=action,
        )
    still_rel = str(sc.get("still_rel") or f"stills/{sc.get('sid') or f'b{idx:02d}'}.png")
    still = workspace / still_rel

    # Prompt composition integrity: primary content must dominate.
    primary_ok = bool(action.strip()) and len(action.strip()) >= 24
    guard_ratio = 0.0
    if prompt:
        # Heuristic: if "CHANNEL NOTES" or "CATALYST" starts the prompt, composition is wrong.
        starts_with_guard = bool(re.match(r"^\s*(CATALYST|CHANNEL NOTES|MR SKELEWELLY CANONICAL)", prompt, re.I))
        primary_markers = ("PRIMARY EDIT", "PRIMARY SCENE", "WARDROBE", action[:40] if action else "___")
        has_primary = any(m and m in prompt for m in primary_markers)
        checks.append({
            "id": f"scene_{idx}_prompt_composition",
            "label": f"Scene {idx} prompt composition",
            "status": "fail" if starts_with_guard or not has_primary else "pass",
            "detail": (
                "Guardrail/Catalyst prepended over scene content"
                if starts_with_guard
                else ("Missing PRIMARY/WARDROBE markers" if not has_primary else "Scene-first composition OK")
            ),
        })

    checks.append({
        "id": f"scene_{idx}_action_present",
        "label": f"Scene {idx} has location/action",
        "status": "pass" if primary_ok else "fail",
        "detail": action[:160] if primary_ok else "scene_action empty or too short",
    })

    # Wardrobe lock consistency
    if locked_outfit:
        same = _outfit_compatible(locked_outfit, outfit)
        checks.append({
            "id": f"scene_{idx}_wardrobe_lock",
            "label": f"Scene {idx} wardrobe matches job lock",
            "status": "pass" if same else "fail",
            "detail": f"locked={locked_outfit[:80]} | scene={outfit[:80]}",
        })
        # Motion must not contradict locked wardrobe
        if motion and _motion_contradicts_outfit(motion, locked_outfit):
            checks.append({
                "id": f"scene_{idx}_motion_wardrobe",
                "label": f"Scene {idx} motion agrees with wardrobe",
                "status": "fail",
                "detail": "Motion prompt reintroduces conflicting clothing/persona language",
            })

    # Still file
    if still.is_file() and still.stat().st_size > 20_000:
        checks.append({
            "id": f"scene_{idx}_still_present",
            "label": f"Scene {idx} still present",
            "status": "pass",
            "detail": f"{still.stat().st_size} bytes",
        })
        # Black/void heuristic via average luminance
        void = _mostly_black_still(still)
        checks.append({
            "id": f"scene_{idx}_background_detail",
            "label": f"Scene {idx} not black-void",
            "status": "fail" if void else "pass",
            "detail": "Still is mostly black/empty" if void else "Still has luminance variation",
        })
        if strict_skeleton_identity:
            semantic_still = audit_skeleton_still(
                still,
                reference=_workspace_skeleton_reference(workspace),
                locked_outfit=locked_outfit or outfit,
                cast_count=scene_cast_count,
            )
            checks.append({
                "id": f"scene_{idx}_still_semantic_identity",
                "label": f"Scene {idx} still is the canonical artifact-free skeleton",
                "status": str(semantic_still.get("status") or "fail"),
                "detail": str(
                    semantic_still.get("summary")
                    or semantic_still.get("error")
                    or "Still identity audit failed"
                )[:500],
                "semantic": semantic_still,
            })
        elif product_identity_required:
            product_qa = sc.get("still_qa") if isinstance(sc.get("still_qa"), dict) else {}
            checks.append({
                "id": f"scene_{idx}_product_still_identity",
                "label": f"Scene {idx} still preserves approved product identity",
                "status": "pass" if product_qa.get("status") == "pass" and product_qa.get("pass") is True else "fail",
                "detail": str(product_qa.get("summary") or "Product still was not semantically approved")[:500],
                "semantic": product_qa,
            })
    else:
        checks.append({
            "id": f"scene_{idx}_still_present",
            "label": f"Scene {idx} still present",
            "status": "fail",
            "detail": "Missing still",
        })

    # Prompt toxic artifacts
    blob = f"{prompt} {action} {motion}".lower()
    toxic = []
    for term, label in (
        ("holding a basketball", "sports prop"),
        ("glowing eyes", "emissive eyes"),
        ("circuit board", "cyber skull"),
        ("exposed brain", "open cranium"),
        ("eyes in chest", "torso eyes"),
        ("human skin", "human tissue risk"),
    ):
        # Identity locks intentionally say "no human skin" and "never glowing
        # eyes". Those are protections, not requests to draw the artifact.
        positive = re.search(
            rf"(?<!no )(?<!zero )(?<!never )(?<!without )\b{re.escape(term)}\b",
            blob,
        )
        if positive:
            toxic.append(label)
    checks.append({
        "id": f"scene_{idx}_prompt_toxicity",
        "label": f"Scene {idx} prompt clean of known artifacts",
        "status": "fail" if toxic else "pass",
        "detail": ", ".join(toxic) if toxic else "OK",
    })

    # Temporal visual QA on i2v clips (identity drift / black-out frames).
    clip_rel = str(sc.get("clip_rel") or f"clips/{sc.get('sid') or f'b{idx:02d}'}.mp4")
    clip_path = workspace / clip_rel
    if clip_path.is_file() and clip_path.stat().st_size > 50_000:
        temporal = _temporal_clip_report(clip_path, still=still if still.is_file() else None)
        for c in temporal:
            c["id"] = f"scene_{idx}_{c['id']}"
            c["label"] = f"Scene {idx} {c['label']}"
            checks.append(c)
        if strict_skeleton_identity:
            semantic = audit_skeleton_clip(
                clip_path,
                still=still if still.is_file() else None,
                locked_outfit=locked_outfit or outfit,
                cast_count=scene_cast_count,
            )
            checks.append({
                "id": f"scene_{idx}_semantic_identity",
                "label": f"Scene {idx} remains the canonical skeleton in every sampled frame",
                "status": str(semantic.get("status") or "fail"),
                "detail": str(semantic.get("summary") or semantic.get("error") or "Semantic identity audit failed")[:500],
                "semantic": semantic,
            })
        elif product_identity_required:
            product_i2v = sc.get("i2v_qa") if isinstance(sc.get("i2v_qa"), dict) else {}
            checks.append({
                "id": f"scene_{idx}_product_clip_identity",
                "label": f"Scene {idx} preserves approved product identity across sampled frames",
                "status": "pass" if product_i2v.get("status") == "pass" and product_i2v.get("pass") is True else "fail",
                "detail": str(product_i2v.get("summary") or "Product clip was not semantically approved")[:500],
                "semantic": product_i2v,
            })

    status = "fail" if any(c.get("status") != "pass" for c in checks) else "pass"
    return {"index": idx, "status": status, "checks": checks}


def should_block_publish(visual_report: dict[str, Any] | None) -> tuple[bool, str]:
    if not isinstance(visual_report, dict):
        return True, "Visual QA did not return a report"
    if visual_report.get("status") != "pass" or visual_report.get("ready_to_publish") is False:
        return True, str(visual_report.get("summary") or "Visual QA failed")
    return False, ""


def _outfit_compatible(locked: str, scene: str) -> bool:
    a = re.findall(r"[a-z0-9]+", locked.lower())
    b = re.findall(r"[a-z0-9]+", scene.lower())
    if not a or not b:
        return True
    stop = {"the", "and", "over", "with", "glass", "shell", "empty", "hands", "no", "or"}
    sa = {w for w in a if len(w) > 3 and w not in stop}
    sb = {w for w in b if len(w) > 3 and w not in stop}
    if not sa:
        return True
    return len(sa & sb) / max(1, min(len(sa), len(sb))) >= 0.34


def _motion_contradicts_outfit(motion: str, outfit: str) -> bool:
    m = motion.lower()
    # Persona/clothing keywords not in locked outfit
    suspects = (
        "sweater", "tunic", "caveman", "animal hide", "leather jacket",
        "ballet", "college kid", "doctor coat", "fur vest",
    )
    olow = outfit.lower()
    for s in suspects:
        if s in m and s not in olow:
            return True
    return False


def _mostly_black_still(path: Path) -> bool:
    """Cheap luminance check — black void backgrounds from truncated prompts."""
    stats = _luma_stats(path)
    if not stats:
        return False
    return stats["mean"] < 18 and stats["var"] < 120


def _luma_stats(path: Path) -> dict[str, float] | None:
    try:
        from PIL import Image
        import statistics

        im = Image.open(path).convert("L")
        im = im.resize((64, 64))
        pixels = list(im.getdata())
        if not pixels:
            return None
        return {
            "mean": float(statistics.mean(pixels)),
            "var": float(statistics.pvariance(pixels) if len(pixels) > 1 else 0),
        }
    except Exception:
        return None


def _probe_clip_duration(clip_path: Path) -> float:
    ffprobe = shutil.which("ffprobe")
    if not ffprobe:
        return 0.0
    try:
        proc = subprocess.run(
            [
                ffprobe,
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(clip_path),
            ],
            check=False,
            timeout=15,
            capture_output=True,
            text=True,
        )
        return max(0.0, float((proc.stdout or "0").strip() or 0.0))
    except Exception:
        return 0.0


def _audio_silence_report(clip_path: Path) -> dict[str, Any]:
    """Require an i2v clip with no audio stream; unavailable probing blocks."""

    ffprobe = shutil.which("ffprobe")
    if not ffprobe:
        return {
            "status": "fail",
            "pass": False,
            "summary": "Audio-silence QA unavailable because ffprobe is missing",
            "qa_unavailable": True,
        }
    try:
        process = subprocess.run(
            [
                ffprobe,
                "-v", "error",
                "-select_streams", "a",
                "-show_entries", "stream=index,codec_name",
                "-of", "json",
                str(clip_path),
            ],
            check=False,
            timeout=15,
            capture_output=True,
            text=True,
        )
        if process.returncode != 0:
            return {
                "status": "fail",
                "pass": False,
                "summary": f"Audio-silence QA probe failed: {(process.stderr or '').strip()[:240]}",
                "qa_unavailable": True,
            }
        payload = json.loads(process.stdout or "{}")
        streams = payload.get("streams") if isinstance(payload, dict) else None
        if not isinstance(streams, list):
            return {
                "status": "fail",
                "pass": False,
                "summary": "Audio-silence QA returned an invalid stream report",
                "qa_unavailable": True,
            }
        if streams:
            return {
                "status": "fail",
                "pass": False,
                "summary": "Generated i2v clip contains an audio stream; Studio clips must remain silent",
                "audio_streams": streams,
            }
        return {
            "status": "pass",
            "pass": True,
            "summary": "No audio stream is present",
            "audio_streams": [],
        }
    except Exception as exc:
        return {
            "status": "fail",
            "pass": False,
            "summary": f"Audio-silence QA unavailable: {str(exc)[:240]}",
            "qa_unavailable": True,
        }


def _frame_timestamps(duration: float, count: int = 5) -> list[float]:
    """Return timestamps spanning the complete timeline, including the last beat."""
    duration = max(0.1, float(duration or 0.0))
    count = max(3, int(count or 5))
    edge = min(0.15, duration * 0.04)
    start = edge
    end = max(start, duration - edge)
    step = (end - start) / float(count - 1)
    return [round(start + (step * idx), 3) for idx in range(count)]


def _extract_clip_frames(clip_path: Path, count: int = 5) -> list[Path]:
    """Sample real positions across a short clip via ffmpeg.

    The previous fps=count/100 expression sampled one frame every ~25 seconds,
    so a five-second clip often yielded only its first frame.
    """
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return []
    out_dir = clip_path.parent / f".vq_frames_{clip_path.stem}"
    try:
        if out_dir.exists():
            for old in out_dir.glob("*.jpg"):
                try:
                    old.unlink()
                except Exception:
                    pass
        else:
            out_dir.mkdir(parents=True, exist_ok=True)
        duration = _probe_clip_duration(clip_path)
        if duration <= 0:
            return []
        for idx, timestamp in enumerate(_frame_timestamps(duration, count=count)):
            target = out_dir / f"f{idx:02d}.jpg"
            subprocess.run(
                [
                    ffmpeg,
                    "-y", "-hide_banner", "-loglevel", "error",
                    "-ss", f"{timestamp:.3f}",
                    "-i", str(clip_path),
                    "-frames:v", "1",
                    # Hands, fingers, ribs, and eye sockets are exactly where
                    # image-to-video artifacts hide.  512px samples made a
                    # brief small deformation too easy for semantic QA to
                    # overlook.
                    "-vf", "scale=768:-2",
                    "-q:v", "3",
                    str(target),
                ],
                check=False,
                timeout=20,
                capture_output=True,
            )
        return sorted(out_dir.glob("f*.jpg"))[:count]
    except Exception:
        return []


def _semantic_cache_path(clip_path: Path) -> Path:
    return clip_path.with_suffix(clip_path.suffix + ".visualqa.json")


def _semantic_fingerprint(
    clip_path: Path,
    still: Path | None,
    locked_outfit: str,
    cast_count: int = 1,
) -> str:
    parts = [str(SEMANTIC_QA_VERSION), _asset_version_token(clip_path)]
    if still and still.is_file():
        parts.append(_asset_version_token(still))
    parts.append(str(locked_outfit or ""))
    parts.append(f"cast_count={2 if int(cast_count or 1) >= 2 else 1}")
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _parse_json_object(text: str) -> dict[str, Any]:
    raw = str(text or "").strip().strip("`").strip()
    if raw.lower().startswith("json"):
        raw = raw[4:].strip()
    candidates = [raw]
    start, end = raw.find("{"), raw.rfind("}")
    if start >= 0 and end > start:
        candidates.append(raw[start:end + 1])
    for candidate in candidates:
        try:
            data = json.loads(candidate)
            if isinstance(data, dict):
                return dict(data)
        except Exception:
            try:
                from json_repair import repair_json

                data = json.loads(repair_json(candidate))
                if isinstance(data, dict):
                    return dict(data)
            except Exception:
                pass
    return {}


def _semantic_prompt(*, frame_count: int, locked_outfit: str, cast_count: int = 1) -> str:
    hosts = 2 if int(cast_count or 1) >= 2 else 1
    subject = (
        "exactly TWO identical ivory anatomical skeletons inside the same thin clear glass body shell"
        if hosts >= 2
        else "the exact same canonical subject as the reference: an ivory anatomical skeleton inside the same thin clear glass body shell"
    )
    duplicate_rule = (
        "if a third skeleton appears or either host becomes human"
        if hosts >= 2
        else "if the subject duplicates"
    )
    return (
        "You are a strict visual quality-control classifier. The FIRST image is the approved reference still. "
        f"The next {frame_count} images are chronological frames sampled across one image-to-video clip. "
        f"Pass ONLY if every video frame visibly contains {subject}. Fail if any frame becomes a bald human, human "
        "face, skin, flesh, muscle, hair, mannequin, cyborg, or different character; if the skull, torso, ribs, or bones "
        "melt or change; if eyes leave the skull sockets; if any limb, hand, finger, leg, or foot is added, missing, "
        f"merged, detached, or malformed; {duplicate_rule}; if wardrobe visibly changes; if any frame adds "
        "clothing when the reference is unclothed; or if any new brain, molecule, neuron, hologram, diagram, text, "
        "overlay, glowing symbol, prop, or background element appears. Natural pose and small camera motion are the "
        "ONLY allowed changes. Treat even one small malformed finger, extra joint, fused hand, broken rib, drifting "
        "eye, warped skull edge, or temporary anatomy glitch as a failure—there is no acceptable artifact threshold. "
        "Every garment, prop, wall, fixture, and background object must remain exactly as in "
        f"frame zero. Expected wardrobe: {locked_outfit or 'match the reference still exactly'}. "
        "Return JSON only with this exact shape: "
        '{"pass":false,"confidence":0.0,"summary":"short factual reason",'
        '"human_or_skin_frames":[],"identity_drift_frames":[],"anatomy_artifact_frames":[],'
        '"wardrobe_drift_frames":[],"symbolic_clutter_frames":[],"prop_or_background_drift_frames":[],'
        '"frame_subjects":["reference","frame 1 subject"]}. '
        "Frame numbers refer to video frames 1 through the final sampled frame. Uncertainty is a fail."
    )


def _run_semantic_vision(image_paths: list[str], *, prompt: str) -> dict[str, Any]:
    """Run semantic QA through direct Anthropic only."""
    from studio_agent import competitor
    from studio_agent import provider_policy

    provider_policy.assert_provider_allowed(
        "anthropic", provider_policy.SEMANTIC_QA_CAPABILITY
    )
    result = competitor._summarize_keyframe_visuals_anthropic(
        image_paths,
        prompt_text=prompt,
    )
    if not isinstance(result, dict):
        result = {"error": "anthropic_invalid_result", "summary": ""}
    summary = str(result.get("summary") or "").strip()
    return {**result, "parsed": _parse_json_object(summary), "raw_summary": summary[:4000]}


def _workspace_skeleton_reference(workspace: Path) -> Path | None:
    try:
        from skeleton_ai.styled_pipeline import _resolve_skeleton_master_reference
        from skeleton_ai.canonical_edit import _reference_url_to_local, resolve_master_reference_local

        source = _resolve_skeleton_master_reference(Path(workspace), None)
        local = _reference_url_to_local(source) if source else None
        return local or resolve_master_reference_local(source)
    except Exception:
        return None


def _still_semantic_cache_path(still: Path) -> Path:
    return still.with_suffix(still.suffix + ".stillqa.json")


def _still_semantic_fingerprint(
    still: Path,
    reference: Path | None,
    locked_outfit: str,
    cast_count: int = 1,
) -> str:
    parts = [str(STILL_SEMANTIC_QA_VERSION), _asset_version_token(still)]
    if reference and reference.is_file():
        parts.append(_asset_version_token(reference))
    parts.append(str(locked_outfit or ""))
    parts.append(f"cast_count={2 if int(cast_count or 1) >= 2 else 1}")
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _qa_jpeg(source: Path, target: Path) -> Path | None:
    try:
        from PIL import Image

        target.parent.mkdir(parents=True, exist_ok=True)
        with Image.open(source) as opened:
            image = opened.convert("RGB")
            image.thumbnail((1024, 1024))
            image.save(target, format="JPEG", quality=91)
        return target
    except Exception:
        return None


def _still_semantic_prompt(*, locked_outfit: str, cast_count: int = 1) -> str:
    wardrobe_detail = str(locked_outfit or "match the reference or remain unclothed")
    if "turtleneck" in wardrobe_detail.lower():
        wardrobe_detail += (
            ". A turtleneck passes only when an opaque continuous top covers the entire torso from neck to "
            "waistband and both arms to the wrists; a collar alone or exposed ribcage is wardrobe drift"
        )
    hosts = 2 if int(cast_count or 1) >= 2 else 1
    subject_rule = (
        "exactly TWO visibly identical ivory anatomical skeletons standing apart with a clear air gap, "
        "each covered only by a thin body-hugging clear glass skin that follows bone contours "
        "(never a curved pod/dome/capsule behind the back), in one continuous physical interior with "
        "visible walls/floor (not a black void; not a split-screen; not embracing; not sharing one glass bubble)"
        if hosts >= 2
        else "exactly one visibly identical ivory anatomical skeleton inside the same close-fitting clear glass body"
    )
    layout_fail = (
        "a third skeleton, split/multi-panel composition, a shared glass bubble/orb/dome fusing both torsos, "
        "curved glass pods/capsules behind the backs, merged limbs between hosts, a black/empty void backdrop, "
        "or nonphysical empty stage"
        if hosts >= 2
        else "duplicated subjects, collages, or nonphysical empty stages"
    )
    return (
        "You are a strict production visual-QC classifier. The FIRST image is an identity-only canonical skeleton "
        "reference; its neutral backdrop, camera angle, pose, lighting, and framing are NOT scene requirements. "
        "The SECOND image is a generated scene still. Pass only if the second image shows "
        f"{subject_rule}. "
        "Fail for any enclosing glass dome/capsule/container, or a self-emitting light/orb visibly embedded inside the chest/ribcage; sports balls or gym props; any human skin, flesh, hair, bald human head, muscle, mannequin, or alternate character; changed "
        "skull/eye proportions; missing, added, fused, or malformed bones, hands, fingers, legs, or feet; "
        "a black/empty candidate backdrop; baked text; or floating brains, molecules, neurons, diagrams, "
        "holograms, medical overlays, or unrelated props. The candidate may use any coherent physical location and "
        "natural camera/pose. Standing, seated, crouched, profile, walking, or gesturing poses are all valid and MUST "
        "NOT be reported as identity drift or layout artifacts. Ordinary "
        "physical props and fixtures such as a chair, rock, table, lamp, window, or wall are also valid. Reflections, practical lamps, and external light falling across the chest are valid; only an internal/embedded chest light is a failure. A skeleton standing in front of, beside, or lightly leaning on a physical wall is valid; fail only when its anatomy is visibly fused into the wall. Only "
        f"{layout_fail} are layout failures. Expected "
        f"wardrobe: {wardrobe_detail}. Return JSON only with this exact "
        "shape: "
        '{"pass":false,"confidence":0.0,"summary":"short factual reason",'
        '"identity_drift":false,"human_or_skin":false,"anatomy_artifact":false,'
        '"wardrobe_drift":false,"layout_artifact":false,"symbolic_clutter":false,'
        '"text_artifact":false,"background_artifact":false,'
        '"hand_topology_failure":false,"glass_shell_failure":false}. '
        # These four are reported separately from anatomy_artifact because the
        # model reproduces them on essentially every draw. Naming them lets the
        # repair path stop instead of paying for a retry that returns the same
        # defect. anatomy_artifact stays reserved for a one-off limb glitch.
        "The canonical skeleton has a smooth polished cranium and large round eyes by "
        "design - never report those as defects. Report hand_topology_failure when fingers "
        "are missing, added, fused, thumbless, or wrongly jointed; and glass_shell_failure "
        "when the shell shows stray scratch lines, broken refraction, or bones detached "
        "from the body inside it. "
        "Uncertainty is a fail."
    )


def audit_skeleton_still(
    still: Path,
    *,
    reference: Path | None,
    locked_outfit: str = "",
    force: bool = False,
    cast_count: Any = None,
) -> dict[str, Any]:
    """Compare frame zero with the canonical master before approval or animation."""
    still = Path(still)
    reference = Path(reference) if reference else None
    cast_count = _asset_cast_count(still, cast_count)
    required = _env_bool("STUDIO_STILL_SEMANTIC_QA_REQUIRED", True)
    if not still.is_file():
        return {"status": "fail", "pass": False, "summary": "Still is missing"}
    if not reference or not reference.is_file():
        return {
            "status": "fail" if required else "warn",
            "pass": False,
            "confidence": 0.0,
            "summary": "Canonical skeleton reference is unavailable",
        }

    fingerprint = _still_semantic_fingerprint(
        still,
        reference,
        locked_outfit,
        cast_count=cast_count,
    )
    cache_path = _still_semantic_cache_path(still)
    if not force and cache_path.is_file():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if isinstance(cached, dict) and cached.get("fingerprint") == fingerprint:
                return cached
        except Exception:
            pass

    frame_dir = still.parent / f".vq_still_{still.stem}"
    ref_jpg = _qa_jpeg(reference, frame_dir / "reference.jpg")
    candidate_jpg = _qa_jpeg(still, frame_dir / "candidate.jpg")
    if not ref_jpg or not candidate_jpg:
        return {
            "status": "fail" if required else "warn", "pass": False,
            "confidence": 0.0, "summary": "Could not prepare still images for semantic QA",
            "fingerprint": fingerprint,
        }
    try:
        vision = _run_semantic_vision(
            [str(ref_jpg), str(candidate_jpg)],
            prompt=_still_semantic_prompt(locked_outfit=locked_outfit, cast_count=cast_count),
        ) or {}
        parsed = vision.get("parsed") if isinstance(vision.get("parsed"), dict) else {}
        confidence = float(parsed.get("confidence") or 0.0)
        issue_fields = (
            "identity_drift", "human_or_skin", "anatomy_artifact", "wardrobe_drift",
            "layout_artifact", "symbolic_clutter", "text_artifact", "background_artifact",
            # Structural classes: reported separately so the repair path can
            # refuse to spend on a defect a fresh seed will reproduce.
            "hand_topology_failure", "glass_shell_failure",
        )
        issues = [field for field in issue_fields if parsed.get(field) is True]
        if not parsed and vision.get("error"):
            issues.append("qa_unavailable")
        summary = str(parsed.get("summary") or vision.get("error") or (
            "Still preserves the canonical skeleton" if parsed.get("pass") is True else "Still identity was not proven"
        ))[:500]
        # Vision models often fail pass with a clear summary but forget to set
        # boolean flags — backfill issues so regenerate applies the right fix.
        if parsed.get("pass") is not True or issues:
            slow = summary.lower()
            if any(
                term in slow
                for term in (
                    "chest", "ribcage", "rib cage", "sternum", "orb", "embedded light",
                    "self-emitting", "internal light", "glowing light", "chest glow",
                    "chest cavity", "light source",
                )
            ) and "symbolic_clutter" not in issues:
                issues.append("symbolic_clutter")
            if any(term in slow for term in ("human skin", "flesh", "hair", "mannequin")) and "human_or_skin" not in issues:
                issues.append("human_or_skin")
            if any(term in slow for term in ("split", "diptych", "collage", "multi-panel")) and "layout_artifact" not in issues:
                issues.append("layout_artifact")
            if any(term in slow for term in ("text", "hashtag", "watermark", "caption overlay")) and "text_artifact" not in issues:
                issues.append("text_artifact")
            # Structural backfill. Without this a summary that plainly describes
            # a featureless skull or thumbless hand lands as a generic anatomy
            # artifact, and the repair path pays for a retry that cannot fix it.
            for terms, field in (
                (("thumbless", "missing thumb", "missing finger", "extra finger", "fused finger", "six finger", "malformed hand"), "hand_topology_failure"),
                (("scratch line", "stray line", "detached bone", "floating bone", "broken refraction"), "glass_shell_failure"),
            ):
                if any(term in slow for term in terms) and field not in issues:
                    issues.append(field)
        passed = parsed.get("pass") is True and confidence >= 0.80 and not issues
        report = {
            "version": STILL_SEMANTIC_QA_VERSION,
            "status": "pass" if passed else ("fail" if required else "warn"),
            "pass": bool(passed), "confidence": confidence,
            "provider": str(vision.get("provider") or ""), "model": str(vision.get("model") or ""),
            "summary": summary,
            "issues": issues, "fingerprint": fingerprint, "created_at": time.time(),
            "cast_count": 2 if int(cast_count or 1) >= 2 else 1,
        }
        try:
            cache_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        except Exception:
            pass
        return report
    finally:
        for path in (ref_jpg, candidate_jpg):
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass
        try:
            frame_dir.rmdir()
        except Exception:
            pass


def audit_scene_correspondence(
    still: Path,
    *,
    scene_contract: str,
    previous_still: Path | None = None,
    previous_contract: str = "",
    next_still: Path | None = None,
    next_contract: str = "",
) -> dict[str, Any]:
    """Judge whether a still tells *its* beat, not merely whether it is clean.

    Skeleton identity QA intentionally accepts different locations and poses.  That
    is right for continuity, but it cannot catch a sequence of technically clean
    frames that all tell the same moment.  This is the separate story-direction
    gate used by an explicit production audit.
    """
    still = Path(still)
    if not still.is_file():
        return {"status": "fail", "pass": False, "summary": "Still is missing"}

    frame_dir = still.parent / f".vq_story_{still.stem}"
    candidates: list[str] = []
    labels: list[str] = []
    current = _qa_jpeg(still, frame_dir / "current.jpg")
    if not current:
        return {"status": "fail", "pass": False, "summary": "Could not prepare scene for story QA"}
    candidates.append(str(current))
    labels.append("CURRENT")

    for label, neighbor, contract in (
        ("PREVIOUS", previous_still, previous_contract),
        ("NEXT", next_still, next_contract),
    ):
        if neighbor and Path(neighbor).is_file():
            rendered = _qa_jpeg(Path(neighbor), frame_dir / f"{label.lower()}.jpg")
            if rendered:
                candidates.append(str(rendered))
                labels.append(label)

    prompt = (
        "You are QA for AI-generated video stills. Your ONE hard job is to reject GARBAGE — stills that would make "
        "the finished video look broken. The first image is CURRENT (the approved STILL / frame-zero opening pose). "
        f"Any following images are labeled in this order: {', '.join(labels[1:]) or 'none'}. The recurring skeleton "
        "identity is intentionally shared across scenes and MUST NOT be treated as a defect or duplication. "
        "CURRENT is a single still, not the finished animation; do NOT judge motion (weight shifts, shrugs, camera "
        "pushes, VFX travel) — that belongs to image-to-video. "
        "REJECT (pass=false, wrong_or_artifact=true) ONLY for real defects that read as garbage: visible AI artifacts "
        "(warping, melting, extra or missing limbs, fused or garbled anatomy, distorted hands, garbled text or "
        "watermarks), the WRONG subject or content (not the intended skeleton scene — e.g. a landscape, or a truly "
        "empty presenter void), or an otherwise unusable image. "
        "Do NOT reject for subjective staging: a scene that looks similar to an adjacent one, a clean but 'generic' "
        "pose, or a pose that doesn't dramatically express the narration are all ACCEPTABLE — record them as advisory "
        "notes only, never as a failure. "
        f"CURRENT contract: {str(scene_contract or '')[:900]}. "
        f"PREVIOUS contract: {str(previous_contract or '')[:450]}. "
        f"NEXT contract: {str(next_contract or '')[:450]}. "
        "Return JSON only: {\"pass\":true,\"confidence\":0.0,\"summary\":\"short factual finding\","
        "\"wrong_or_artifact\":false,\"duplicate_adjacent\":false,\"narrative_mismatch\":false,"
        "\"generic_staging\":false,\"recommended_restage\":\"brief direction (advisory)\"}. "
        "When unsure whether something is a real artifact, PASS it — only clear garbage fails."
    )
    try:
        vision = _run_semantic_vision(candidates, prompt=prompt) or {}
        parsed = vision.get("parsed") if isinstance(vision.get("parsed"), dict) else {}
        confidence = float(parsed.get("confidence") or 0.0)
        # HARD gate: only real garbage (visible artifacts / wrong content / empty
        # void) blocks a scene. Require reasonable confidence so an "unsure"
        # judgment never blocks — the bar is "no artifacting", not "flawless".
        hard_fail = parsed.get("wrong_or_artifact") is True and confidence >= 0.55
        # Subjective staging is ADVISORY only: recorded for the creator/Catalyst,
        # never a production failure. (Adjacent similarity and narrative-beat
        # expression are not artifacts.)
        advisory = [
            field for field in ("duplicate_adjacent", "narrative_mismatch", "generic_staging")
            if parsed.get(field) is True
        ]
        if not parsed and vision.get("error"):
            # The judge itself is unavailable. A transient QA outage must not fail
            # the whole production — the per-still identity/artifact gate already
            # guarded generation. Soft-pass and record the gap.
            advisory.append("qa_unavailable")
            passed = True
        else:
            passed = not hard_fail
        issues = (["wrong_or_artifact"] if hard_fail else []) + advisory
        return {
            "status": "pass" if passed else "fail",
            "pass": bool(passed),
            "confidence": confidence,
            "summary": str(parsed.get("summary") or vision.get("error") or (
                "Artifact-free" if passed else "Rejected: visible artifact or wrong content"
            ))[:500],
            "issues": issues,
            "advisory": advisory,
            "recommended_restage": str(parsed.get("recommended_restage") or "")[:700],
            "provider": str(vision.get("provider") or ""),
            "model": str(vision.get("model") or ""),
        }
    finally:
        for path_text in candidates:
            try:
                Path(path_text).unlink(missing_ok=True)
            except Exception:
                pass
        try:
            frame_dir.rmdir()
        except Exception:
            pass


_LAUNCH_STYLE_RUBRICS = {
    "skeleton_host": (
        "SKELETON_HOST RUBRIC: require the intended cast count of ivory anatomical skeleton hosts, "
        "complete plausible bones/hands/feet, no human skin or flesh, no duplicated or merged host, and "
        "a separate close-fitting clear glass skin for each host with a visible air gap between dual hosts."
    ),
    "cinematic": (
        "CINEMATIC RUBRIC: require deliberate foreground/midground/background composition, a readable focal "
        "subject, motivated lighting, coherent perspective and depth, intentional crop, and scene-specific staging; "
        "fail generic centered presenter staging or accidental edge cuts."
    ),
    "ultra_realism": (
        "ULTRA_REALISM RUBRIC: require anatomically plausible faces, eyes, hands, fingers, limbs and joints; "
        "physically credible skin, hair, fabric, glass, metal and other materials; consistent contact shadows, "
        "reflections, lighting and perspective; fail waxy, melted, fused or synthetic-looking anatomy/materials."
    ),
    "historical_18th_century": (
        "HISTORICAL_18TH_CENTURY RUBRIC: require period-correct 18th-century clothing construction, hair, "
        "architecture, furniture, tools, transport and lighting; fail electricity, LEDs, plastic, zippers, modern "
        "vehicles, contemporary signage, devices, materials or any other anachronism."
    ),
}


def _normalized_launch_style(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "skeleton": "skeleton_host",
        "skeleton_hosts": "skeleton_host",
        "ultra_realistic": "ultra_realism",
        "photoreal": "ultra_realism",
        "historical": "historical_18th_century",
        "18th_century": "historical_18th_century",
    }
    return aliases.get(raw, raw)


def _asset_render_style(asset_path: Path, explicit: str = "") -> str:
    normalized = _normalized_launch_style(explicit)
    if normalized:
        return normalized
    asset = Path(asset_path)
    workspace = asset.parent.parent if asset.parent.name in {"stills", "clips"} else asset.parent
    try:
        spec = json.loads((workspace / "job_spec.json").read_text(encoding="utf-8"))
        return _normalized_launch_style((spec or {}).get("render_style"))
    except Exception:
        return ""


def audit_generic_still(
    still: Path,
    *,
    scene_contract: str,
    force: bool = False,
    render_style: str = "",
) -> dict[str, Any]:
    """Strict scene-contract QA for every non-reference-led generated still."""
    still = Path(still)
    if not still.is_file():
        return {"status": "fail", "pass": False, "summary": "Still is missing"}
    style = _asset_render_style(still, render_style)
    rubric = _LAUNCH_STYLE_RUBRICS.get(
        style,
        "GENERIC RUBRIC: require coherent anatomy, geometry, materials, composition, perspective and scene-specific staging.",
    )
    fingerprint = hashlib.sha256("|".join((
        str(STILL_SEMANTIC_QA_VERSION),
        _asset_version_token(still),
        style,
        str(scene_contract or ""),
    )).encode("utf-8")).hexdigest()
    frame_dir = still.parent / f".vq_generic_{still.stem}"
    candidate = _qa_jpeg(still, frame_dir / "candidate.jpg")
    if not candidate:
        return {"status": "fail", "pass": False, "summary": "Could not prepare still for QA"}
    try:
        prompt = (
            "You are a strict generated-image QC classifier. Judge the candidate against this scene contract: "
            f"{str(scene_contract or '')[:700]}. Pass only if it is one coherent scene matching the requested subject, "
            "setting, action, and style. Fail for malformed anatomy or geometry, duplicate/merged subjects, unintended "
            "text/watermarks, split/collage layout, black/empty scene, unrelated objects, cropped-off required anatomy "
            "or products, visible generation seams, or obvious visual artifacts. "
            f"{rubric} "
            'Return JSON only: {"pass":false,"confidence":0.0,"summary":"reason","style_match":false,'
            '"artifact":false,"identity_drift":false,"layout_artifact":false,"text_artifact":false,'
            '"crop_artifact":false,"seam_artifact":false,"style_violation":false,"composition_failure":false,'
            '"anatomy_artifact":false,"material_artifact":false,"cast_integrity_failure":false,'
            '"glass_shell_failure":false,"anachronism":false}. Uncertainty or unavailable QA is a fail.'
        )
        vision = _run_semantic_vision([str(candidate)], prompt=prompt) or {}
        parsed = vision.get("parsed") if isinstance(vision.get("parsed"), dict) else {}
        issue_fields = (
            "artifact", "identity_drift", "layout_artifact", "text_artifact",
            "crop_artifact", "seam_artifact", "style_violation",
            "composition_failure", "anatomy_artifact", "material_artifact",
            "cast_integrity_failure", "glass_shell_failure", "anachronism",
        )
        issues = [key for key in issue_fields if parsed.get(key) is True]
        if not parsed or vision.get("error"):
            issues.append("qa_unavailable")
        confidence = float(parsed.get("confidence") or 0.0)
        passed = (
            parsed.get("pass") is True
            and parsed.get("style_match") is True
            and confidence >= 0.80
            and not issues
        )
        return {"status": "pass" if passed else "fail", "pass": bool(passed), "confidence": confidence,
                "provider": str(vision.get("provider") or ""), "model": str(vision.get("model") or ""),
                "summary": str(parsed.get("summary") or vision.get("error") or "Scene quality was not proven")[:500],
                "issues": issues, "render_style": style, "rubric": rubric, "fingerprint": fingerprint}
    finally:
        try:
            candidate.unlink(missing_ok=True)
            frame_dir.rmdir()
        except Exception:
            pass


def _motion_brief_for_clip(clip_path: Path, explicit: str, fallback: str) -> str:
    if str(explicit or "").strip():
        return str(explicit).strip()[:300]
    clip = Path(clip_path)
    workspace = clip.parent.parent if clip.parent.name == "clips" else clip.parent
    try:
        raw = json.loads((workspace / "scenes.json").read_text(encoding="utf-8"))
        for scene in raw if isinstance(raw, list) else []:
            if not isinstance(scene, dict):
                continue
            rel = str(scene.get("clip_rel") or "").replace("\\", "/").strip()
            expected = (workspace / rel).resolve() if rel else None
            if expected == clip.resolve():
                brief = str(scene.get("motion_prompt") or scene.get("scene_action") or "").strip()
                if brief:
                    return brief[:300]
    except Exception:
        pass
    return str(fallback or "").strip()[:300]


def audit_generic_clip(
    clip_path: Path,
    *,
    scene_contract: str,
    motion_brief: str = "",
    render_style: str = "",
    force: bool = False,
) -> dict[str, Any]:
    """Block until a silent clip proves motion and full-timeline visual quality."""

    clip_path = Path(clip_path)
    if not clip_path.is_file():
        return {"status": "fail", "pass": False, "summary": "Clip is missing"}
    style = _asset_render_style(clip_path, render_style)
    rubric = _LAUNCH_STYLE_RUBRICS.get(style, "Preserve the requested style in every sampled frame.")
    motion = _motion_brief_for_clip(clip_path, motion_brief, scene_contract)
    fingerprint = hashlib.sha256("|".join((
        str(SEMANTIC_QA_VERSION),
        _asset_version_token(clip_path),
        style,
        str(scene_contract or ""),
        motion,
    )).encode("utf-8")).hexdigest()
    audio = _audio_silence_report(clip_path)
    if audio.get("status") != "pass" or audio.get("pass") is not True:
        return {
            "status": "fail",
            "pass": False,
            "summary": str(audio.get("summary") or "Clip silence was not proven")[:500],
            "violations": ["audio_not_silent"],
            "audio_silence": audio,
            "motion_brief": motion,
            "render_style": style,
            "fingerprint": fingerprint,
        }

    # Nine evenly spaced samples cover the complete five-second timeline at
    # roughly half-second intervals. Seven samples left a wide enough gap for
    # brief morphs and anatomy failures to evade the public-release gate.
    frames = _extract_clip_frames(clip_path, count=9)
    if len(frames) < 8:
        return {
            "status": "fail",
            "pass": False,
            "summary": "Could not sample full clip timeline",
            "violations": ["qa_unavailable"],
            "audio_silence": audio,
            "motion_brief": motion,
            "render_style": style,
            "fingerprint": fingerprint,
        }
    try:
        vision = _run_semantic_vision([str(frame) for frame in frames], prompt=(
            "You are a strict generated-video QC classifier. These are chronological frames from one clip. "
            f"Scene contract: {str(scene_contract or '')[:700]}. Pass only if all frames preserve one coherent subject, "
            "setting, action, and style. Fail for morphing, anatomy/geometry artifacts, identity/background drift, "
            "duplicate subjects, text/watermarks, collage layouts, black frames, temporal seams, frozen/dead frames, "
            "or motion that contradicts or fails to visibly realize the brief across time. "
            f"Motion brief (must be visibly satisfied): {motion}. {rubric} "
            'Return JSON only: {"pass":false,"confidence":0.0,"summary":"reason",'
            '"motion_brief_satisfied":false,"violations":[]}. Uncertainty or unavailable QA is a fail.'
        )) or {}
        parsed = vision.get("parsed") if isinstance(vision.get("parsed"), dict) else {}
        violations = list(parsed.get("violations") or [])
        if not parsed or vision.get("error"):
            violations.append("qa_unavailable")
        if parsed.get("motion_brief_satisfied") is not True:
            violations.append("motion_brief_not_proven")
        confidence = float(parsed.get("confidence") or 0.0)
        passed = parsed.get("pass") is True and confidence >= 0.75 and not violations
        return {"status": "pass" if passed else "fail", "pass": bool(passed), "confidence": confidence,
                "provider": str(vision.get("provider") or ""), "model": str(vision.get("model") or ""),
                "summary": str(parsed.get("summary") or vision.get("error") or "Clip quality was not proven")[:500],
                "violations": violations, "audio_silence": audio, "motion_brief": motion,
                "render_style": style, "rubric": rubric, "fingerprint": fingerprint,
                "frames_reviewed": len(frames)}
    finally:
        try:
            for frame in frames: frame.unlink(missing_ok=True)
            frames[0].parent.rmdir()
        except Exception:
            pass


def audit_product_still(
    still: Path,
    *,
    references: list[Path],
    product_name: str = "",
    force: bool = False,
) -> dict[str, Any]:
    """Verify that a product-ad still preserves the supplied product identity."""
    still = Path(still)
    refs = [Path(path) for path in references if Path(path).is_file()][:3]
    required = _env_bool("STUDIO_PRODUCT_SEMANTIC_QA_REQUIRED", True)
    if not still.is_file():
        return {"status": "fail", "pass": False, "summary": "Product still is missing"}
    if not refs:
        return {
            "status": "fail" if required else "warn",
            "pass": False,
            "summary": "Product reference image is unavailable",
        }

    fingerprint_parts = [str(PRODUCT_SEMANTIC_QA_VERSION), _asset_version_token(still)]
    fingerprint_parts.extend(_asset_version_token(path) for path in refs)
    fingerprint_parts.append(str(product_name or ""))
    fingerprint = hashlib.sha256("|".join(fingerprint_parts).encode("utf-8")).hexdigest()
    cache_path = still.with_suffix(still.suffix + ".productqa.json")
    if not force and cache_path.is_file():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if isinstance(cached, dict) and cached.get("fingerprint") == fingerprint:
                return cached
        except Exception:
            pass

    frame_dir = still.parent / f".vq_product_{still.stem}"
    prepared_refs = [_qa_jpeg(path, frame_dir / f"reference_{idx}.jpg") for idx, path in enumerate(refs)]
    prepared_refs = [path for path in prepared_refs if path]
    candidate = _qa_jpeg(still, frame_dir / "candidate.jpg")
    if not prepared_refs or not candidate:
        return {
            "status": "fail" if required else "warn",
            "pass": False,
            "summary": "Could not prepare product images for semantic QA",
            "fingerprint": fingerprint,
        }
    try:
        prompt = (
            "You are a strict product-ad visual QC classifier. The first image or images are the approved product "
            "reference. The final image is a generated ad still. Pass only if the final image preserves the exact same "
            f"product identity ({product_name or 'supplied product'}): same shape, materials, colors, logo/label treatment, "
            "and distinctive features. A different product, altered logo/label, warped product geometry, unreadable or invented "
            "claim text, duplicate product collage, or an obstructed/unrecognizable product is a failure. Return JSON only: "
            '{"pass":false,"confidence":0.0,"summary":"short factual reason",'
            '"product_identity_drift":false,"product_artifact":false,"text_artifact":false,'
            '"layout_artifact":false}. Uncertainty is a fail.'
        )
        vision = _run_semantic_vision([*(str(path) for path in prepared_refs), str(candidate)], prompt=prompt) or {}
        parsed = vision.get("parsed") if isinstance(vision.get("parsed"), dict) else {}
        confidence = float(parsed.get("confidence") or 0.0)
        issue_fields = ("product_identity_drift", "product_artifact", "text_artifact", "layout_artifact")
        issues = [field for field in issue_fields if parsed.get(field) is True]
        if not parsed and vision.get("error"):
            issues.append("qa_unavailable")
        passed = parsed.get("pass") is True and confidence >= 0.80 and not issues
        report = {
            "version": PRODUCT_SEMANTIC_QA_VERSION,
            "status": "pass" if passed else ("fail" if required else "warn"),
            "pass": bool(passed),
            "confidence": confidence,
            "provider": str(vision.get("provider") or ""),
            "model": str(vision.get("model") or ""),
            "summary": str(parsed.get("summary") or vision.get("error") or "Product identity was not proven")[:500],
            "issues": issues,
            "fingerprint": fingerprint,
            "created_at": time.time(),
            "kind": "product_reference",
        }
        try:
            cache_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        except Exception:
            pass
        return report
    finally:
        for path in [*prepared_refs, candidate]:
            try:
                Path(path).unlink(missing_ok=True)
            except Exception:
                pass
        try:
            frame_dir.rmdir()
        except Exception:
            pass


def audit_product_clip(
    clip_path: Path,
    *,
    still: Path | None,
    references: list[Path],
    product_name: str = "",
    force: bool = False,
) -> dict[str, Any]:
    """Verify a product remains the approved product throughout sampled I2V frames."""
    clip_path = Path(clip_path)
    refs = [Path(path) for path in references if Path(path).is_file()][:2]
    required = _env_bool("STUDIO_PRODUCT_SEMANTIC_QA_REQUIRED", True)
    if not clip_path.is_file():
        return {"status": "fail", "pass": False, "summary": "Product clip is missing"}
    if not refs:
        return {"status": "fail" if required else "warn", "pass": False, "summary": "Product reference image is unavailable"}
    # Nine samples across a five-second clip catch transient I2V hand/limb
    # failures that a five-frame inspection can miss.  The cache version above
    # invalidates earlier, less strict passes.
    frames = _extract_clip_frames(clip_path, count=9)
    if len(frames) < 8:
        return {"status": "fail" if required else "warn", "pass": False, "summary": f"Could not sample the full product clip timeline ({len(frames)}/5 frames)"}
    frame_dir = frames[0].parent
    prepared_refs = [_qa_jpeg(path, frame_dir / f"product_reference_{idx}.jpg") for idx, path in enumerate(refs)]
    prepared_refs = [path for path in prepared_refs if path]
    image_paths = [*(str(path) for path in prepared_refs), *(str(path) for path in frames)]
    try:
        prompt = (
            "You are a strict product-ad video QC classifier. The first image or images are approved product references; "
            "the remaining images are chronological frames from one generated video. Pass only if every sampled frame that "
            f"shows {product_name or 'the product'} preserves its exact shape, materials, colors, logo/label treatment and "
            "distinctive features. Fail for substitution, identity morphing, warped geometry, invented or unreadable claim text, "
            "duplicate-product collage, obstructed/unrecognizable product, or a frame that no longer proves the same product. "
            "Return JSON only: {\"pass\":false,\"confidence\":0.0,\"summary\":\"short factual reason\","
            "\"product_identity_drift_frames\":[],\"product_artifact_frames\":[],\"text_artifact_frames\":[],"
            "\"layout_artifact_frames\":[]}. Uncertainty is a fail. Frame indexes start at 0 for the video frames."
        )
        vision = _run_semantic_vision(image_paths, prompt=prompt) or {}
        parsed = vision.get("parsed") if isinstance(vision.get("parsed"), dict) else {}
        confidence = float(parsed.get("confidence") or 0.0)
        fields = ("product_identity_drift_frames", "product_artifact_frames", "text_artifact_frames", "layout_artifact_frames")
        violations = {field: list(parsed.get(field) or []) for field in fields if list(parsed.get(field) or [])}
        if not parsed and vision.get("error"):
            violations["qa_unavailable"] = []
        passed = parsed.get("pass") is True and confidence >= 0.80 and not violations
        return {
            "version": PRODUCT_SEMANTIC_QA_VERSION,
            "kind": "product_reference_clip",
            "status": "pass" if passed else ("fail" if required else "warn"),
            "pass": bool(passed), "confidence": confidence,
            "provider": str(vision.get("provider") or ""), "model": str(vision.get("model") or ""),
            "summary": str(parsed.get("summary") or vision.get("error") or "Product identity was not proven across video frames")[:500],
            "violations": violations, "frames_reviewed": len(frames), "created_at": time.time(),
        }
    finally:
        try:
            for path in [*prepared_refs, *frames]:
                Path(path).unlink(missing_ok=True)
            if frame_dir.name.startswith(".vq_frames_"):
                frame_dir.rmdir()
        except Exception:
            pass


def _reference_jpeg(still: Path, frame_dir: Path) -> Path | None:
    try:
        from PIL import Image

        target = frame_dir / "reference.jpg"
        with Image.open(still) as source:
            image = source.convert("RGB")
            image.thumbnail((768, 768))
            image.save(target, format="JPEG", quality=90)
        return target
    except Exception:
        return None


def audit_skeleton_clip(
    clip_path: Path,
    *,
    still: Path | None,
    locked_outfit: str = "",
    force: bool = False,
    cast_count: Any = None,
) -> dict[str, Any]:
    """Return a cached semantic multi-frame skeleton identity verdict."""
    clip_path = Path(clip_path)
    still = Path(still) if still else None
    cast_count = _asset_cast_count(clip_path, cast_count)
    required = _env_bool("STUDIO_I2V_SEMANTIC_QA_REQUIRED", True)
    if not clip_path.is_file():
        return {"status": "fail", "pass": False, "summary": "Clip is missing"}

    # This local fallback is rendered only from the approved still and cannot
    # synthesize a human or mutate anatomy.
    try:
        sidecar = clip_path.with_suffix(clip_path.suffix + ".fal.json")
        meta = json.loads(sidecar.read_text(encoding="utf-8")) if sidecar.is_file() else {}
        if str(meta.get("endpoint") or "").startswith("local:identity-safe-motion"):
            return {
                "status": "pass",
                "pass": True,
                "confidence": 1.0,
                "provider": "deterministic_local",
                "summary": "Identity-safe motion generated directly from the approved still",
            }
    except Exception:
        pass

    fingerprint = _semantic_fingerprint(
        clip_path,
        still,
        locked_outfit,
        cast_count=cast_count,
    )
    cache_path = _semantic_cache_path(clip_path)
    if not force and cache_path.is_file():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if isinstance(cached, dict) and cached.get("fingerprint") == fingerprint:
                return cached
        except Exception:
            pass

    frames = _extract_clip_frames(clip_path, count=9)
    if len(frames) < 8:
        return {
            "status": "fail" if required else "warn",
            "pass": False,
            "confidence": 0.0,
            "summary": f"Could not sample the full clip timeline ({len(frames)}/9 frames)",
            "fingerprint": fingerprint,
        }

    reference = _reference_jpeg(still, frames[0].parent) if still and still.is_file() else None
    image_paths = ([str(reference)] if reference else []) + [str(frame) for frame in frames]
    try:
        vision = _run_semantic_vision(
            image_paths,
            prompt=_semantic_prompt(frame_count=len(frames), locked_outfit=locked_outfit, cast_count=cast_count),
        ) or {}
        parsed = vision.get("parsed") if isinstance(vision.get("parsed"), dict) else {}
        confidence = float(parsed.get("confidence") or 0.0)
        violation_fields = (
            "human_or_skin_frames",
            "identity_drift_frames",
            "anatomy_artifact_frames",
            "wardrobe_drift_frames",
            "symbolic_clutter_frames",
            "prop_or_background_drift_frames",
        )
        violations = {
            field: list(parsed.get(field) or [])
            for field in violation_fields
            if list(parsed.get(field) or [])
        }
        passed = parsed.get("pass") is True and confidence >= 0.75 and not violations
        report = {
            "version": SEMANTIC_QA_VERSION,
            "status": "pass" if passed else ("fail" if required else "warn"),
            "pass": bool(passed),
            "confidence": confidence,
            "provider": str(vision.get("provider") or ""),
            "model": str(vision.get("model") or ""),
            "summary": str(
                parsed.get("summary")
                or vision.get("error")
                or ("All sampled frames preserve skeleton identity" if passed else "Semantic identity was not proven")
            )[:500],
            "violations": violations,
            "frame_subjects": list(parsed.get("frame_subjects") or [])[:8],
            "frames_reviewed": len(frames),
            "fingerprint": fingerprint,
            "cast_count": 2 if int(cast_count or 1) >= 2 else 1,
            "created_at": time.time(),
        }
        try:
            cache_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        except Exception:
            pass
        return report
    finally:
        cleanup = [*frames]
        if reference:
            cleanup.append(reference)
        try:
            for path in cleanup:
                Path(path).unlink(missing_ok=True)
            parent = frames[0].parent
            if parent.name.startswith(".vq_frames_"):
                parent.rmdir()
        except Exception:
            pass


def _temporal_clip_report(clip_path: Path, *, still: Path | None = None) -> list[dict[str, Any]]:
    """Detect black-out / severe luminance drift across clip frames vs still."""
    checks: list[dict[str, Any]] = []
    frames = _extract_clip_frames(clip_path, count=4)
    if not frames:
        checks.append({
            "id": "temporal_sample",
            "label": "clip temporal sample",
            "status": "warn",
            "detail": "Could not sample clip frames (ffmpeg unavailable or empty)",
        })
        return checks

    frame_stats = [s for s in (_luma_stats(f) for f in frames) if s]
    if not frame_stats:
        checks.append({
            "id": "temporal_sample",
            "label": "clip temporal sample",
            "status": "warn",
            "detail": "Frame stats unavailable",
        })
        return checks

    black_frames = sum(1 for s in frame_stats if s["mean"] < 18 and s["var"] < 120)
    means = [s["mean"] for s in frame_stats]
    mean_span = max(means) - min(means) if means else 0.0

    if black_frames >= max(2, len(frame_stats) // 2):
        checks.append({
            "id": "temporal_blackout",
            "label": "clip not black-void over time",
            "status": "fail",
            "detail": f"{black_frames}/{len(frame_stats)} sampled frames are near-black",
        })
    else:
        checks.append({
            "id": "temporal_blackout",
            "label": "clip not black-void over time",
            "status": "pass",
            "detail": f"{len(frame_stats)} frames sampled; black={black_frames}",
        })

    # Large mid-clip luminance collapse vs still ≈ identity/background mutation.
    if still and still.is_file():
        still_stats = _luma_stats(still)
        if still_stats and means:
            avg_clip = sum(means) / len(means)
            delta = abs(avg_clip - still_stats["mean"])
            # Fail only on extreme drift (still bright, clip nearly black or vice versa).
            if delta > 55 and (avg_clip < 22 or still_stats["mean"] < 22):
                checks.append({
                    "id": "temporal_still_drift",
                    "label": "clip matches still brightness identity",
                    "status": "fail",
                    "detail": f"still_mean={still_stats['mean']:.1f} clip_mean={avg_clip:.1f}",
                })
            else:
                checks.append({
                    "id": "temporal_still_drift",
                    "label": "clip matches still brightness identity",
                    "status": "pass",
                    "detail": f"delta={delta:.1f}",
                })

    if mean_span > 70:
        checks.append({
            "id": "temporal_flicker",
            "label": "clip luminance stability",
            "status": "warn",
            "detail": f"frame mean span {mean_span:.1f} (possible flash/morph)",
        })

    # Cleanup temp frames (best-effort)
    try:
        for f in frames:
            f.unlink(missing_ok=True)
        parent = frames[0].parent
        if parent.name.startswith(".vq_frames_"):
            parent.rmdir()
    except Exception:
        pass
    return checks


def _score(checks: list[dict[str, Any]]) -> int:
    if not checks:
        return 0
    pts = 0
    for c in checks:
        st = c.get("status")
        if st == "pass":
            pts += 100
        elif st == "warn":
            pts += 50
    return int(round(pts / max(1, len(checks))))


def _summary(status: str, score: int, checks: list[dict[str, Any]]) -> str:
    fails = [c.get("label") for c in checks if c.get("status") == "fail"]
    if status == "pass":
        return f"Visual QA pass (score {score})"
    if fails:
        return f"Visual QA {status} (score {score}): " + "; ".join(str(f) for f in fails[:6])
    return f"Visual QA {status} (score {score})"
