from __future__ import annotations

import asyncio
import io
import json
import re
from pathlib import Path

import httpx
from PIL import Image, ImageDraw

try:
    import numpy as np
except Exception:
    np = None

try:
    import cv2
except Exception:
    cv2 = None


CATALYST_REFERENCE_ANALYSIS_MAX_SECONDS = 20.0 * 60.0
CATALYST_REFERENCE_UPLOAD_PROXY_FPS = 6.0
CATALYST_REFERENCE_UPLOAD_PROXY_MAX_WIDTH = 960
CATALYST_REFERENCE_FRAME_AUDIT_TARGET_FPS = 4.0
CATALYST_REFERENCE_FRAME_AUDIT_MAX_TIMELINE_FRAMES = 4800
CATALYST_REFERENCE_FRAME_AUDIT_MAX_REPORT_ROWS = 1200
CATALYST_REFERENCE_FRAME_AUDIT_WORKING_MAX_WIDTH = 640
CATALYST_REFERENCE_AUDIO_MAX_SECONDS = 360.0

_reference_extract_video_metadata = None
_reference_extract_audio_from_video = None
_reference_transcribe_audio_with_grok = None
_reference_audio_max_seconds = CATALYST_REFERENCE_AUDIO_MAX_SECONDS
_clone_xai_api_key = ""
_clone_analysis_prompt = ""
_clone_template_system_prompts: dict[str, str] = {}
_clone_heuristic_analysis_fn = None
_clone_clip_text = lambda value, max_chars=240: _clip_text(value, max_chars)


def _clip_text(value: str, max_chars: int = 240) -> str:
    compact = re.sub(r"\s+", " ", str(value or "").strip())
    if len(compact) <= max_chars:
        return compact
    return compact[: max(0, max_chars - 3)].rstrip() + "..."


_reference_clip_text = _clip_text


def configure_reference_video_audit_hooks(
    *,
    clip_text=None,
    analysis_max_seconds: float | None = None,
    upload_proxy_fps: float | None = None,
    upload_proxy_max_width: int | None = None,
    frame_audit_target_fps: float | None = None,
    frame_audit_max_timeline_frames: int | None = None,
    frame_audit_max_report_rows: int | None = None,
    frame_audit_working_max_width: int | None = None,
    extract_video_metadata=None,
    extract_audio_from_video=None,
    transcribe_audio_with_grok=None,
    reference_audio_max_seconds: float | None = None,
) -> None:
    global _reference_clip_text
    global CATALYST_REFERENCE_ANALYSIS_MAX_SECONDS
    global CATALYST_REFERENCE_UPLOAD_PROXY_FPS
    global CATALYST_REFERENCE_UPLOAD_PROXY_MAX_WIDTH
    global CATALYST_REFERENCE_FRAME_AUDIT_TARGET_FPS
    global CATALYST_REFERENCE_FRAME_AUDIT_MAX_TIMELINE_FRAMES
    global CATALYST_REFERENCE_FRAME_AUDIT_MAX_REPORT_ROWS
    global CATALYST_REFERENCE_FRAME_AUDIT_WORKING_MAX_WIDTH
    global _reference_extract_video_metadata
    global _reference_extract_audio_from_video
    global _reference_transcribe_audio_with_grok
    global _reference_audio_max_seconds
    if callable(clip_text):
        _reference_clip_text = clip_text
    if analysis_max_seconds is not None:
        CATALYST_REFERENCE_ANALYSIS_MAX_SECONDS = float(analysis_max_seconds or CATALYST_REFERENCE_ANALYSIS_MAX_SECONDS)
    if upload_proxy_fps is not None:
        CATALYST_REFERENCE_UPLOAD_PROXY_FPS = float(upload_proxy_fps or CATALYST_REFERENCE_UPLOAD_PROXY_FPS)
    if upload_proxy_max_width is not None:
        CATALYST_REFERENCE_UPLOAD_PROXY_MAX_WIDTH = int(upload_proxy_max_width or CATALYST_REFERENCE_UPLOAD_PROXY_MAX_WIDTH)
    if frame_audit_target_fps is not None:
        CATALYST_REFERENCE_FRAME_AUDIT_TARGET_FPS = float(frame_audit_target_fps or CATALYST_REFERENCE_FRAME_AUDIT_TARGET_FPS)
    if frame_audit_max_timeline_frames is not None:
        CATALYST_REFERENCE_FRAME_AUDIT_MAX_TIMELINE_FRAMES = int(
            frame_audit_max_timeline_frames or CATALYST_REFERENCE_FRAME_AUDIT_MAX_TIMELINE_FRAMES
        )
    if frame_audit_max_report_rows is not None:
        CATALYST_REFERENCE_FRAME_AUDIT_MAX_REPORT_ROWS = int(
            frame_audit_max_report_rows or CATALYST_REFERENCE_FRAME_AUDIT_MAX_REPORT_ROWS
        )
    if frame_audit_working_max_width is not None:
        CATALYST_REFERENCE_FRAME_AUDIT_WORKING_MAX_WIDTH = int(
            frame_audit_working_max_width or CATALYST_REFERENCE_FRAME_AUDIT_WORKING_MAX_WIDTH
        )
    if callable(extract_video_metadata):
        _reference_extract_video_metadata = extract_video_metadata
    if callable(extract_audio_from_video):
        _reference_extract_audio_from_video = extract_audio_from_video
    if callable(transcribe_audio_with_grok):
        _reference_transcribe_audio_with_grok = transcribe_audio_with_grok
    if reference_audio_max_seconds is not None:
        _reference_audio_max_seconds = float(reference_audio_max_seconds or CATALYST_REFERENCE_AUDIO_MAX_SECONDS)


def configure_clone_analysis_hooks(
    *,
    xai_api_key: str = "",
    clone_analysis_prompt: str = "",
    template_system_prompts: dict | None = None,
    heuristic_clone_analysis_fn=None,
    clip_text=None,
) -> None:
    global _clone_xai_api_key
    global _clone_analysis_prompt
    global _clone_template_system_prompts
    global _clone_heuristic_analysis_fn
    global _clone_clip_text
    _clone_xai_api_key = str(xai_api_key or "").strip()
    _clone_analysis_prompt = str(clone_analysis_prompt or "").strip()
    if isinstance(template_system_prompts, dict):
        _clone_template_system_prompts = {
            str(k): str(v)
            for k, v in template_system_prompts.items()
            if str(k).strip() and str(v or "").strip()
        }
    if callable(heuristic_clone_analysis_fn):
        _clone_heuristic_analysis_fn = heuristic_clone_analysis_fn
    if callable(clip_text):
        _clone_clip_text = clip_text


def _strip_model_reasoning_artifacts(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lines = []
    for raw in text.splitlines():
        line = str(raw or "").strip()
        if not line:
            continue
        lowered = line.lower()
        if lowered.startswith(("reasoning:", "thinking:", "analysis:", "internal chain")):
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def _contains_analytics_signal(text: str) -> bool:
    compact = str(text or "").strip()
    if not compact:
        return False
    return bool(re.search(
        r"\b(views?|impressions|likes?|comments?|subscribers?|ctr|click[- ]through|average view duration|avd|"
        r"average percentage viewed|retention|watch time|browse features|traffic sources?|title:|channel:|duration|"
        r"tags?:|analytics|audience|reach|engagement)\b",
        compact,
        flags=re.IGNORECASE,
    ))


def _dedupe_clip_list(values: list[str], max_items: int = 8, max_chars: int = 180) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in list(values or []):
        value = _clip_text(str(raw or "").strip(), max_chars)
        key = value.lower()
        if not value or key in seen:
            continue
        seen.add(key)
        out.append(value)
        if len(out) >= max_items:
            break
    return out


def _duration_text_to_seconds(value: str) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    parts = [p for p in text.split(":") if p.strip()]
    if not parts:
        return 0
    try:
        nums = [int(float(part)) for part in parts]
    except Exception:
        return 0
    if len(nums) == 3:
        return (nums[0] * 3600) + (nums[1] * 60) + nums[2]
    if len(nums) == 2:
        return (nums[0] * 60) + nums[1]
    return nums[0]


def _parse_compact_metric_number(raw: str) -> float | None:
    text = str(raw or "").strip().lower().replace(",", "")
    if not text:
        return None
    match = re.match(r"^(-?\d+(?:\.\d+)?)([kmb])?$", text)
    if not match:
        try:
            return float(text)
        except Exception:
            return None
    value = float(match.group(1))
    suffix = str(match.group(2) or "").strip().lower()
    multiplier = {
        "k": 1_000.0,
        "m": 1_000_000.0,
        "b": 1_000_000_000.0,
    }.get(suffix, 1.0)
    return value * multiplier


def _extract_numeric_metric(source: str, patterns: tuple[str, ...]) -> float | None:
    text = str(source or "")
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        raw = str(match.group(1) or "").replace(",", "").strip()
        parsed = _parse_compact_metric_number(raw)
        if parsed is not None:
            return parsed
    return None


def _extract_duration_metric(source: str, patterns: tuple[str, ...]) -> int:
    text = str(source or "")
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        seconds = _duration_text_to_seconds(str(match.group(1) or ""))
        if seconds > 0:
            return seconds
    return 0


def _extract_analytics_text_metrics(extracted_text: str, source_bundle: dict | None = None) -> dict:
    compact = re.sub(r"\s+", " ", _strip_model_reasoning_artifacts(extracted_text)).strip()
    source_bundle = dict(source_bundle or {})
    metrics: dict[str, float | int] = {}
    if not compact:
        return metrics

    def _set_float(key: str, value: float | None, digits: int = 2) -> None:
        if value is None:
            return
        metrics[key] = round(float(value), digits)

    def _set_int(key: str, value: float | int | None) -> None:
        if value is None:
            return
        metrics[key] = int(round(float(value)))

    _set_float("ctr", _extract_numeric_metric(compact, (
        r"(\d+(?:\.\d+)?)\s*%\s*(?:impressions\s+)?click[- ]through rate",
        r"(?:impressions\s+)?click[- ]through rate[^0-9]*(\d+(?:\.\d+)?)\s*%",
        r"\bctr[^0-9]*(\d+(?:\.\d+)?)\s*%",
    )), digits=2)
    _set_float("average_viewed_pct", _extract_numeric_metric(compact, (
        r"(\d+(?:\.\d+)?)\s*%\s*viewed",
        r"average percentage viewed[^0-9]*(\d+(?:\.\d+)?)\s*%",
        r"average viewed[^0-9]*(\d+(?:\.\d+)?)\s*%",
    )), digits=2)
    _set_int("impressions", _extract_numeric_metric(compact, (
        r"impressions[^0-9]*(\d+(?:\.\d+)?\s*[kmb]?)\b",
        r"(?<!:)(\d+(?:\.\d+)?\s*[kmb]?)\s+impressions\b",
    )))
    _set_int("views", _extract_numeric_metric(compact, (
        r"(?<!:)(\d+(?:\.\d+)?\s*[kmb]?)\s+views\b",
        r"views[^0-9]*(\d+(?:\.\d+)?\s*[kmb]?)\b",
    )))
    _set_float("watch_time_hours", _extract_numeric_metric(compact, (
        r"watch time\s*\(hours\)[^0-9]*(\d+(?:\.\d+)?)",
        r"watch time[^0-9]*(\d+(?:\.\d+)?)\s*hours?\b",
    )), digits=2)

    avg_view_duration_sec = _extract_duration_metric(compact, (
        r"average view duration[^0-9]*([0-9:]{3,8})",
        r"\bavd[^0-9]*([0-9:]{3,8})",
    ))
    if avg_view_duration_sec > 0:
        metrics["average_view_duration_sec"] = int(avg_view_duration_sec)
    elif metrics.get("watch_time_hours") and metrics.get("views"):
        hours = float(metrics.get("watch_time_hours", 0.0) or 0.0)
        views = int(metrics.get("views", 0) or 0)
        if hours > 0 and views > 0:
            metrics["average_view_duration_sec"] = int(round((hours * 3600.0) / views))

    duration_sec = int(float(source_bundle.get("duration_sec", 0) or 0) or 0)
    if not metrics.get("average_viewed_pct") and metrics.get("average_view_duration_sec") and duration_sec > 0:
        metrics["average_viewed_pct"] = round((float(metrics["average_view_duration_sec"]) / max(duration_sec, 1)) * 100.0, 2)

    traffic_patterns = {
        "suggested_videos_pct": (r"suggested videos[^0-9]*(\d+(?:\.\d+)?)\s*%",),
        "browse_features_pct": (r"browse features[^0-9]*(\d+(?:\.\d+)?)\s*%",),
        "notifications_pct": (r"notifications[^0-9]*(\d+(?:\.\d+)?)\s*%",),
        "search_pct": (r"youtube search[^0-9]*(\d+(?:\.\d+)?)\s*%",),
        "other_youtube_features_pct": (r"other youtube features[^0-9]*(\d+(?:\.\d+)?)\s*%",),
        "mobile_phone_pct": (r"mobile phone[^0-9]*(\d+(?:\.\d+)?)\s*%",),
        "tablet_pct": (r"tablet[^0-9]*(\d+(?:\.\d+)?)\s*%",),
        "computer_pct": (r"computer[^0-9]*(\d+(?:\.\d+)?)\s*%",),
        "tv_pct": (r"\btv[^0-9]*(\d+(?:\.\d+)?)\s*%",),
    }
    for key, patterns in traffic_patterns.items():
        _set_float(key, _extract_numeric_metric(compact, patterns), digits=1)

    rank_match = re.search(r"\b(\d+)\s+of\s+(\d+)\b", compact, flags=re.IGNORECASE)
    if rank_match:
        try:
            rank = int(rank_match.group(1))
            cohort = int(rank_match.group(2))
            if rank > 0 and cohort > 0:
                metrics["performance_rank"] = rank
                metrics["performance_cohort"] = cohort
        except Exception:
            pass

    return metrics


def _summarize_longform_analytics_text(extracted_text: str, source_bundle: dict | None = None) -> dict:
    text = _strip_model_reasoning_artifacts(extracted_text)
    compact = re.sub(r"\s+", " ", text).strip()
    if not compact:
        return {
            "analytics_summary": "",
            "strongest_signals": [],
            "weak_points": [],
            "retention_findings": [],
            "packaging_findings": [],
            "improvement_moves": [],
            "metrics": {},
        }

    strongest_signals: list[str] = []
    weak_points: list[str] = []
    retention_findings: list[str] = []
    packaging_findings: list[str] = []
    improvement_moves: list[str] = []
    summary_parts: list[str] = []
    metrics = _extract_analytics_text_metrics(compact, source_bundle=source_bundle)
    ctr = metrics.get("ctr")
    avg_viewed_pct = metrics.get("average_viewed_pct")
    impressions = metrics.get("impressions")
    views = metrics.get("views")
    avg_view_duration_sec = int(metrics.get("average_view_duration_sec", 0) or 0)
    watch_time_hours = float(metrics.get("watch_time_hours", 0.0) or 0.0)
    suggested_pct = metrics.get("suggested_videos_pct")
    browse_pct = metrics.get("browse_features_pct")
    mobile_phone_pct = metrics.get("mobile_phone_pct")
    performance_rank = int(metrics.get("performance_rank", 0) or 0)
    performance_cohort = int(metrics.get("performance_cohort", 0) or 0)
    visible_early_drop = re.search(r"\b(first\s+\d{1,3}\s+seconds?|drop(?:s|off)?\s+off\s+in\s+the\s+first\s+\d{1,3}\s+seconds?|dropped\s+off\s+in\s+the\s+first\s+\d{1,3}\s+seconds?)\b", compact, flags=re.IGNORECASE)
    browse_features = (
        float(browse_pct or 0.0) > 0.0
        if browse_pct is not None
        else bool(re.search(r"\bbrowse features\b", compact, flags=re.IGNORECASE))
    )

    if views:
        summary_parts.append(f"Visible analytics mention roughly {int(views):,} views.")
    if impressions:
        summary_parts.append(f"Visible analytics mention roughly {int(impressions):,} impressions.")
    if watch_time_hours > 0:
        summary_parts.append(f"Visible watch time is about {watch_time_hours:.1f} hours.")
    if performance_rank > 0 and performance_cohort > 0:
        summary_parts.append(f"The upload appears to be ranking about {performance_rank} of {performance_cohort} by early views.")
        if performance_rank <= max(2, int(round(performance_cohort * 0.25))):
            strongest_signals.append(f"Early rank around {performance_rank} of {performance_cohort} suggests this upload is outperforming most recent uploads in its first YouTube test window.")
        elif performance_rank >= max(4, int(round(performance_cohort * 0.75))):
            weak_points.append(f"Early rank around {performance_rank} of {performance_cohort} suggests the package is lagging behind recent channel baselines.")
    if ctr is not None:
        packaging_findings.append(f"Visible CTR is about {ctr:.1f}%.")
        if ctr >= 4.0:
            strongest_signals.append(f"CTR around {ctr:.1f}% suggests the topic/title/thumbnail package is at least getting initial clicks.")
        elif ctr < 3.0:
            weak_points.append(f"CTR around {ctr:.1f}% is soft, so the thumbnail/title package needs a stronger curiosity gap.")
            improvement_moves.append("Rebuild the title/thumbnail around one dominant promise, one focal visual, and clearer stakes.")
    if avg_viewed_pct is not None:
        retention_findings.append(f"Average percentage viewed is around {avg_viewed_pct:.1f}%.")
        if avg_viewed_pct < 35.0:
            weak_points.append(f"Average percentage viewed near {avg_viewed_pct:.1f}% points to retention as the main bottleneck.")
            improvement_moves.append("Compress the opening and front-load the payoff so the viewer gets the core promise within the first 20 to 30 seconds.")
    if avg_view_duration_sec > 0:
        retention_findings.append(f"Average view duration is about {avg_view_duration_sec // 60}:{avg_view_duration_sec % 60:02d}.")
        video_duration_sec = int(float((source_bundle or {}).get("duration_sec", 0) or 0))
        if video_duration_sec > 0 and avg_view_duration_sec < max(45, int(video_duration_sec * 0.35)):
            weak_points.append("View duration is noticeably below the video length, so the structure needs faster progression and more payoff density.")
    if suggested_pct is not None and float(suggested_pct) > 0.0:
        packaging_findings.append(f"Suggested Videos are driving about {float(suggested_pct):.1f}% of visible traffic.")
        if float(suggested_pct) >= 60.0:
            strongest_signals.append("Most visible traffic is coming from Suggested Videos, so the topic is already matching adjacent viewer demand and YouTube is testing it in recommendation surfaces.")
            improvement_moves.append("Double down on adjacent-video packaging: title, thumbnail, and opening should feel native to the videos YouTube is placing this beside.")
    if browse_features:
        strongest_signals.append("Browse Features appears in the analytics, which means the package earned home-surface distribution.")
        packaging_findings.append("The source reached Browse Features, so topic selection and packaging were strong enough to win distribution.")
    if browse_pct is not None and float(browse_pct) <= 0.0:
        weak_points.append("Browse Features traffic is effectively zero in the visible sample, so the package is not yet winning home-surface distribution.")
    if mobile_phone_pct is not None and float(mobile_phone_pct) >= 70.0:
        packaging_findings.append(f"Mobile phone watch share is about {float(mobile_phone_pct):.1f}%, so packaging and on-screen readability need to stay phone-first.")
        improvement_moves.append("Keep thumbnails, hooks, subtitles, and key visuals optimized for a phone screen because most of the visible audience is mobile.")
    if visible_early_drop:
        retention_findings.append("The screenshot explicitly mentions an early drop-off in the opening section.")
        improvement_moves.append("Rewrite the intro so the first line names the payoff immediately instead of warming up too slowly.")

    if not summary_parts and _contains_analytics_signal(compact):
        summary_parts.append(_clip_text(compact, 220))
    elif not summary_parts:
        weak_points.append(
            "The uploaded analytics screenshots did not yield clean machine-readable metrics, so Catalyst should rely on channel context and manual notes instead of guessing."
        )

    return {
        "analytics_summary": " ".join(_dedupe_clip_list(summary_parts, max_items=3, max_chars=220)),
        "strongest_signals": _dedupe_clip_list(strongest_signals, max_items=6),
        "weak_points": _dedupe_clip_list(weak_points, max_items=6),
        "retention_findings": _dedupe_clip_list(retention_findings, max_items=6),
        "packaging_findings": _dedupe_clip_list(packaging_findings, max_items=6),
        "improvement_moves": _dedupe_clip_list(improvement_moves, max_items=8),
        "metrics": metrics,
    }


def _build_analytics_contact_sheet(
    image_paths: list[str],
    output_path: str,
    cols: int = 2,
    cell_size: tuple[int, int] = (1280, 720),
) -> str:
    valid_paths = [Path(p) for p in list(image_paths or []) if Path(str(p)).exists()]
    if not valid_paths:
        return ""
    cols = max(1, min(2, int(cols or 2)))
    rows = max(1, int((len(valid_paths) + cols - 1) / cols))
    margin = 24
    label_h = 54
    cell_w, cell_h = cell_size
    sheet = Image.new("RGB", ((cell_w * cols) + (margin * (cols + 1)), (cell_h * rows) + (margin * (rows + 1)) + (label_h * rows)), color=(12, 14, 18))
    draw = ImageDraw.Draw(sheet)
    for idx, path in enumerate(valid_paths):
        row = idx // cols
        col = idx % cols
        x = margin + col * (cell_w + margin)
        y = margin + row * (cell_h + label_h + margin)
        with Image.open(path) as img:
            frame = img.convert("RGB")
            frame.thumbnail((cell_w, cell_h))
            offset_x = x + max(0, int((cell_w - frame.width) / 2))
            offset_y = y + max(0, int((cell_h - frame.height) / 2))
            sheet.paste(frame, (offset_x, offset_y))
        draw.rectangle((x, y + cell_h + 6, x + cell_w, y + cell_h + label_h - 6), outline=(52, 61, 77), width=1)
        draw.text((x + 18, y + cell_h + 16), f"Analytics screenshot {idx + 1}", fill=(230, 234, 240))
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    sheet.save(output_path, format="PNG", optimize=True)
    return output_path


async def _build_catalyst_analysis_proxy_video(
    video_path: str,
    output_dir: Path,
    *,
    max_seconds: float = 1200.0,
) -> dict:
    source_path = str(video_path or "").strip()
    if not source_path or not Path(source_path).exists():
        return {"video_path": "", "proxy_used": False, "error": "missing_video"}

    output_dir.mkdir(parents=True, exist_ok=True)
    source_file = Path(source_path)
    proxy_path = output_dir / f"{source_file.stem}_analysis_proxy.mp4"
    clip_seconds = max(60.0, min(float(max_seconds or 1200.0), CATALYST_REFERENCE_ANALYSIS_MAX_SECONDS))
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        source_path,
        "-t",
        f"{clip_seconds:.2f}",
        "-vf",
        f"fps={CATALYST_REFERENCE_UPLOAD_PROXY_FPS},scale='min({CATALYST_REFERENCE_UPLOAD_PROXY_MAX_WIDTH},iw)':-2:flags=lanczos",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "32",
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
        "-c:a",
        "aac",
        "-ac",
        "1",
        "-ar",
        "16000",
        "-b:a",
        "48k",
        str(proxy_path),
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _stdout, stderr = await proc.communicate()
    except Exception as e:
        return {
            "video_path": source_path,
            "proxy_used": False,
            "error": _reference_clip_text(f"ffmpeg_proxy_failed: {e}", 220),
        }

    if proc.returncode != 0 or not proxy_path.exists() or proxy_path.stat().st_size <= 0:
        try:
            proxy_path.unlink(missing_ok=True)
        except Exception:
            pass
        return {
            "video_path": source_path,
            "proxy_used": False,
            "error": _reference_clip_text(stderr.decode(errors="ignore"), 220) or "ffmpeg_proxy_failed",
        }

    source_bytes = int(source_file.stat().st_size or 0)
    proxy_bytes = int(proxy_path.stat().st_size or 0)
    return {
        "video_path": str(proxy_path),
        "proxy_used": True,
        "source_size_bytes": source_bytes,
        "proxy_size_bytes": proxy_bytes,
        "proxy_fps": CATALYST_REFERENCE_UPLOAD_PROXY_FPS,
        "proxy_max_width": CATALYST_REFERENCE_UPLOAD_PROXY_MAX_WIDTH,
        "proxy_duration_sec": clip_seconds,
        "error": "",
    }


async def _extract_reference_video_full_audit(
    video_path: str,
    output_dir: Path,
    *,
    max_keyframes: int = 14,
    max_seconds: float = 1200.0,
) -> dict:
    if cv2 is None or np is None:
        return await _extract_reference_video_sample_frames(
            video_path,
            output_dir,
            max_frames=max_keyframes,
            max_seconds=max_seconds,
        )
    source_path = str(video_path or "").strip()
    if not source_path or not Path(source_path).exists():
        return {"frame_paths": [], "metrics": {"error": "missing_video"}}

    output_dir.mkdir(parents=True, exist_ok=True)

    def _run() -> dict:
        cap = cv2.VideoCapture(source_path)
        if not cap.isOpened():
            return {"frame_paths": [], "metrics": {"error": "cannot_open_video"}}

        fps = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        duration_sec = (total_frames / fps) if fps > 0 and total_frames > 0 else 0.0
        analysis_seconds = min(float(max_seconds or 1200.0), duration_sec or float(max_seconds or 1200.0))
        if fps <= 0:
            fps = 24.0
        max_frame_index = min(total_frames - 1, max(0, int(round(analysis_seconds * fps)) - 1)) if total_frames > 0 else 0
        analysis_frame_span = max_frame_index + 1 if max_frame_index >= 0 else 0
        target_timeline_frames = max(
            900,
            min(
                CATALYST_REFERENCE_FRAME_AUDIT_MAX_TIMELINE_FRAMES,
                int(round(max(analysis_seconds, 1.0) * CATALYST_REFERENCE_FRAME_AUDIT_TARGET_FPS)),
            ),
        )
        analysis_stride = max(
            1,
            int((analysis_frame_span + max(target_timeline_frames, 1) - 1) / max(target_timeline_frames, 1)),
        )

        prev_gray = None
        frame_paths: list[str] = []
        motion_rows: list[dict] = []
        motion_values: list[float] = []
        sharpness_values: list[float] = []
        contrast_values: list[float] = []
        saturation_values: list[float] = []
        edge_density_values: list[float] = []
        low_motion_streak_frames = 0
        longest_low_motion_streak_frames = 0
        idx = 0

        while True:
            ok, frame = cap.read()
            if not ok:
                break
            if idx > max_frame_index:
                break
            if analysis_stride > 1 and idx % analysis_stride != 0:
                idx += 1
                continue

            if frame is None:
                idx += 1
                continue
            try:
                frame_h, frame_w = frame.shape[:2]
            except Exception:
                frame_h, frame_w = (0, 0)
            if frame_w > CATALYST_REFERENCE_FRAME_AUDIT_WORKING_MAX_WIDTH and frame_w > 0 and frame_h > 0:
                scaled_h = max(2, int(round(frame_h * (CATALYST_REFERENCE_FRAME_AUDIT_WORKING_MAX_WIDTH / float(frame_w)))))
                if scaled_h % 2 != 0:
                    scaled_h += 1
                frame = cv2.resize(
                    frame,
                    (CATALYST_REFERENCE_FRAME_AUDIT_WORKING_MAX_WIDTH, scaled_h),
                    interpolation=cv2.INTER_AREA,
                )

            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)
            edges = cv2.Canny(gray, 80, 180)
            motion = 0.0 if prev_gray is None else float(np.mean(cv2.absdiff(gray, prev_gray)))
            sharpness = float(cv2.Laplacian(gray, cv2.CV_64F).var())
            contrast = float(np.std(gray))
            saturation = float(np.mean(hsv[:, :, 1]))
            edge_density = float(np.mean(edges > 0))
            timestamp_sec = (idx / fps) if fps > 0 else 0.0
            motion_rows.append(
                {
                    "frame_index": idx,
                    "timestamp_sec": round(timestamp_sec, 3),
                    "motion": round(motion, 3),
                    "sharpness": round(sharpness, 3),
                    "contrast": round(contrast, 3),
                    "saturation": round(saturation, 3),
                    "edge_density": round(edge_density, 5),
                }
            )
            motion_values.append(motion)
            sharpness_values.append(sharpness)
            contrast_values.append(contrast)
            saturation_values.append(saturation)
            edge_density_values.append(edge_density)
            if motion < 2.0:
                low_motion_streak_frames += 1
                if low_motion_streak_frames > longest_low_motion_streak_frames:
                    longest_low_motion_streak_frames = low_motion_streak_frames
            else:
                low_motion_streak_frames = 0
            prev_gray = gray
            idx += 1

        cap.release()

        if not motion_rows:
            return {
                "frame_paths": [],
                "metrics": {
                    "duration_sec": round(duration_sec, 2),
                    "analysis_seconds": round(analysis_seconds, 2),
                    "timeline_frames_analyzed": 0,
                    "error": "no_metrics",
                },
            }

        motion_arr = np.array(motion_values, dtype=np.float32)
        dynamic_cut_threshold = float(max(18.0, np.percentile(motion_arr, 97)))
        cut_events = [row for row in motion_rows if float(row.get("motion", 0.0) or 0.0) >= dynamic_cut_threshold]
        hook_rows = [row for row in motion_rows if float(row.get("timestamp_sec", 0.0) or 0.0) <= 30.0]
        top_motion = sorted(
            motion_rows,
            key=lambda row: (-float(row.get("motion", 0.0) or 0.0), float(row.get("timestamp_sec", 0.0) or 0.0)),
        )[:10]
        low_motion = sorted(
            motion_rows,
            key=lambda row: (float(row.get("motion", 0.0) or 0.0), float(row.get("timestamp_sec", 0.0) or 0.0)),
        )[:10]

        keyframe_candidates: list[int] = []
        even_count = max(4, min(int(max_keyframes or 14) // 2, 8))
        if motion_rows:
            for idx_even in range(even_count):
                target_pos = int(round(((len(motion_rows) - 1) * idx_even) / max(even_count - 1, 1)))
                keyframe_candidates.append(int(motion_rows[target_pos].get("frame_index", 0) or 0))
        keyframe_candidates.extend(int(row.get("frame_index", 0) or 0) for row in top_motion[: max(3, int(max_keyframes or 14) // 2)])
        keyframe_candidates.extend(int(row.get("frame_index", 0) or 0) for row in cut_events[:4])

        selected_indices = sorted({idx_val for idx_val in keyframe_candidates if idx_val >= 0})[: max(6, min(int(max_keyframes or 14), 18))]
        if selected_indices:
            cap = cv2.VideoCapture(source_path)
            for selected_index in selected_indices:
                if selected_index > max_frame_index:
                    continue
                try:
                    cap.set(cv2.CAP_PROP_POS_FRAMES, float(selected_index))
                except Exception:
                    pass
                ok, frame = cap.read()
                if not ok or frame is None:
                    continue
                try:
                    frame_h, frame_w = frame.shape[:2]
                except Exception:
                    frame_h, frame_w = (0, 0)
                if frame_w > CATALYST_REFERENCE_UPLOAD_PROXY_MAX_WIDTH and frame_w > 0 and frame_h > 0:
                    scaled_h = max(2, int(round(frame_h * (CATALYST_REFERENCE_UPLOAD_PROXY_MAX_WIDTH / float(frame_w)))))
                    if scaled_h % 2 != 0:
                        scaled_h += 1
                    frame = cv2.resize(
                        frame,
                        (CATALYST_REFERENCE_UPLOAD_PROXY_MAX_WIDTH, scaled_h),
                        interpolation=cv2.INTER_AREA,
                    )
                frame_path = output_dir / f"frame_{selected_index:06d}.jpg"
                cv2.imwrite(str(frame_path), frame)
                frame_paths.append(str(frame_path))
            cap.release()

        report_path = output_dir / "full_timeline_report.json"
        try:
            report_rows = list(motion_rows)
            if len(report_rows) > CATALYST_REFERENCE_FRAME_AUDIT_MAX_REPORT_ROWS:
                report_stride = max(
                    1,
                    int((len(report_rows) + CATALYST_REFERENCE_FRAME_AUDIT_MAX_REPORT_ROWS - 1) / CATALYST_REFERENCE_FRAME_AUDIT_MAX_REPORT_ROWS),
                )
                report_rows = report_rows[::report_stride][:CATALYST_REFERENCE_FRAME_AUDIT_MAX_REPORT_ROWS]
            else:
                report_stride = 1
            report_path.write_text(
                json.dumps(
                    {
                        "video_path": source_path,
                        "duration_sec": round(duration_sec, 3),
                        "analysis_seconds": round(analysis_seconds, 3),
                        "fps": round(fps, 3),
                        "total_frames": total_frames,
                        "analysis_stride_frames": analysis_stride,
                        "timeline_frames_analyzed": len(motion_rows),
                        "timeline_report_rows": len(report_rows),
                        "timeline_report_stride": report_stride,
                        "cut_threshold": round(dynamic_cut_threshold, 3),
                        "top_motion_moments": top_motion,
                        "lowest_motion_moments": low_motion,
                        "timeline": report_rows,
                    },
                    ensure_ascii=True,
                ),
                encoding="utf-8",
            )
        except Exception:
            report_path = Path("")

        metrics = {
            "duration_sec": round(duration_sec, 2),
            "analysis_seconds": round(analysis_seconds, 2),
            "fps": round(fps, 2),
            "sampled_frames": len(frame_paths),
            "timeline_frames_analyzed": len(motion_rows),
            "analysis_stride_frames": analysis_stride,
            "full_runtime_covered": bool(duration_sec <= 0 or analysis_seconds + 1.0 >= duration_sec),
            "avg_motion": round(float(np.mean(motion_arr)), 3),
            "avg_sharpness": round(float(np.mean(np.array(sharpness_values, dtype=np.float32))), 3),
            "avg_contrast": round(float(np.mean(np.array(contrast_values, dtype=np.float32))), 3),
            "avg_saturation": round(float(np.mean(np.array(saturation_values, dtype=np.float32))), 3),
            "avg_edge_density": round(float(np.mean(np.array(edge_density_values, dtype=np.float32))), 5),
            "cut_events": len(cut_events),
            "cut_threshold": round(dynamic_cut_threshold, 3),
            "cuts_per_minute": round((len(cut_events) / max(analysis_seconds, 1.0)) * 60.0, 2),
            "hook_motion_avg_first_30_sec": round(
                sum(float(row.get("motion", 0.0) or 0.0) for row in hook_rows) / max(len(hook_rows), 1),
                3,
            ) if hook_rows else 0.0,
            "hook_sharpness_avg_first_30_sec": round(
                sum(float(row.get("sharpness", 0.0) or 0.0) for row in hook_rows) / max(len(hook_rows), 1),
                3,
            ) if hook_rows else 0.0,
            "longest_low_motion_gap_sec": round(longest_low_motion_streak_frames / max(fps, 1.0), 3),
            "top_motion_moments": top_motion,
            "lowest_motion_moments": low_motion,
            "timeline_report_path": str(report_path) if report_path else "",
        }
        return {"frame_paths": frame_paths[:18], "metrics": metrics}

    return await asyncio.to_thread(_run)


async def analyze_viral_video(topic: str, video_description: str, transcript_hint: str = "", source_notes: str = "") -> dict:
    heuristic_fn = _clone_heuristic_analysis_fn
    heuristic = (
        heuristic_fn(topic, video_description, transcript_hint, source_notes)
        if callable(heuristic_fn)
        else {
            "detected_template": "story",
            "viral_analysis": {
                "hook_type": "documentary promise",
                "pacing": "medium",
                "avg_scene_duration": 4.0,
                "scene_count": 10,
                "tone": "cinematic documentary explainer",
                "retention_tricks": [],
                "what_made_it_viral": "",
                "follow_up_topic": str(topic or "").strip(),
            },
            "optimized_prompt": _clone_clip_text(str(topic or "").strip(), 280),
        }
    )
    user_parts = []
    user_parts.append("Source viral video context: " + str(video_description or ""))
    if transcript_hint:
        user_parts.append("Audio/timing info from source: " + str(transcript_hint or ""))
    if source_notes:
        user_parts.append("Operator notes / analytics hints: " + str(source_notes or ""))
    user_parts.append(
        "New topic to apply the viral formula to: "
        + (
            str(topic or "").strip()
            or str((dict(heuristic.get("viral_analysis") or {})).get("follow_up_topic", "") or "")
        )
    )
    user_msg = "\n\n".join(user_parts)
    if not _clone_xai_api_key or not _clone_analysis_prompt:
        return heuristic
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                "https://api.x.ai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {_clone_xai_api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "grok-3-mini-fast",
                    "messages": [
                        {"role": "system", "content": _clone_analysis_prompt},
                        {"role": "user", "content": user_msg},
                    ],
                    "temperature": 0.6,
                },
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            start = content.find("{")
            end = content.rfind("}") + 1
            if start == -1 or end == 0:
                raise ValueError("No JSON in clone analysis response")
            raw = json.loads(content[start:end])
    except Exception:
        return heuristic
    merged = dict(heuristic)
    merged["detected_template"] = str(raw.get("detected_template", "") or merged.get("detected_template", "story"))
    merged_viral = dict(merged.get("viral_analysis") or {})
    raw_viral = dict(raw.get("viral_analysis") or {})
    for key in ("hook_type", "pacing", "tone", "what_made_it_viral", "follow_up_topic"):
        if str(raw_viral.get(key, "") or "").strip():
            merged_viral[key] = str(raw_viral.get(key) or "").strip()
    for key in ("avg_scene_duration", "scene_count"):
        if raw_viral.get(key) is not None:
            merged_viral[key] = raw_viral.get(key)
    merged_viral["retention_tricks"] = _dedupe_clip_list(
        [str(v).strip() for v in list(raw_viral.get("retention_tricks") or []) if str(v).strip()]
        + [str(v).strip() for v in list(merged_viral.get("retention_tricks") or []) if str(v).strip()],
        max_items=6,
        max_chars=120,
    )
    merged["viral_analysis"] = merged_viral
    optimized_prompt = _clone_clip_text(str(raw.get("optimized_prompt", "") or "").strip(), 280)
    if optimized_prompt:
        merged["optimized_prompt"] = optimized_prompt
    return merged


async def generate_clone_script(template: str, topic: str, viral_analysis: dict) -> dict:
    prompts = dict(_clone_template_system_prompts or {})
    base_prompt = str(prompts.get(template, prompts.get("random", "")) or "")
    hook_type = viral_analysis.get("hook_type", "claim")
    pacing = viral_analysis.get("pacing", "fast")
    avg_dur = viral_analysis.get("avg_scene_duration", 3.5)
    scene_count = viral_analysis.get("scene_count", 10)
    tone = viral_analysis.get("tone", "energetic and punchy")
    tricks = viral_analysis.get("retention_tricks", [])
    what_viral = viral_analysis.get("what_made_it_viral", "")
    optimized_prompt = _clone_clip_text(str(viral_analysis.get("optimized_prompt", "") or "").strip(), 280)
    follow_up_topic = _clone_clip_text(str(viral_analysis.get("follow_up_topic", "") or "").strip(), 180)
    effective_topic = _clone_clip_text(str(topic or "").strip(), 180) or follow_up_topic or optimized_prompt or "A sharper follow-up on the source angle"

    clone_override = (
        "\n\nCRITICAL CLONE INSTRUCTIONS -- you MUST follow these EXACTLY:\n"
        "You are cloning a proven viral video. Replicate its formula precisely.\n"
        "- Hook type: " + str(hook_type) + " -- your opening MUST use this exact hook style\n"
        "- Pacing: " + str(pacing) + " -- match this exact energy level\n"
        "- Target scene count: " + str(scene_count) + " scenes\n"
        "- Average scene duration: " + str(avg_dur) + " seconds\n"
        "- Narration tone: " + str(tone) + "\n"
        "- Retention tricks to replicate: " + ", ".join(list(tricks or [])) + "\n"
        "- Why the original went viral: " + str(what_viral) + "\n"
        "\nDo NOT write generic content. Do NOT say things like 'dive into the world of' or "
        "'buckle up'. Write EXACTLY like the source video's style -- punchy, direct, zero filler. "
        "Every single word must earn its place. If the source was a skeleton comparing things, "
        "YOU compare things the same way. Match the structure beat-for-beat.\n"
        "\nKeep the new script in the same topic arena as the source. Do not wander into an adjacent niche."
        "\nNarration must be SHORT and PUNCHY -- 1-2 sentences max per scene. "
        "No yapping. No fluff. Every sentence is a hook or a fact bomb."
    )
    full_prompt = base_prompt + clone_override
    if not _clone_xai_api_key:
        raise RuntimeError("XAI clone script generation is not configured")

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {_clone_xai_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "grok-3-mini-fast",
                "messages": [
                    {"role": "system", "content": full_prompt},
                    {"role": "user", "content": "Clone this viral formula onto new topic: " + effective_topic},
                ],
                "temperature": 0.7,
            },
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        start = content.find("{")
        end = content.rfind("}") + 1
        if start == -1 or end == 0:
            raise ValueError("No JSON in clone script response")
        return json.loads(content[start:end])


async def _extract_reference_preview_frames_from_urls(
    frame_urls: list[str] | None,
    output_dir: Path,
    *,
    duration_sec: float = 0.0,
    max_seconds: float = 180.0,
) -> dict:
    urls = [str(v).strip() for v in list(frame_urls or []) if str(v).strip()]
    if not urls:
        return {"frame_paths": [], "metrics": {"error": "missing_preview_frames", "analysis_mode": "preview_frames"}}
    output_dir.mkdir(parents=True, exist_ok=True)

    frame_paths: list[str] = []
    sharpness_values: list[float] = []
    contrast_values: list[float] = []
    saturation_values: list[float] = []
    variance_values: list[float] = []
    prev_small = None

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for idx, url in enumerate(urls):
            try:
                resp = await client.get(url)
            except Exception:
                continue
            if resp.status_code != 200 or not resp.content:
                continue
            try:
                if Image is None:
                    continue
                img = Image.open(io.BytesIO(resp.content)).convert("RGB")
            except Exception:
                continue
            out_path = output_dir / f"preview_{idx:02d}.png"
            try:
                img.save(out_path, format="PNG")
            except Exception:
                continue
            frame_paths.append(str(out_path))

            if np is None:
                continue
            try:
                rgb = np.array(img)
                if rgb.size == 0:
                    continue
                if cv2 is not None:
                    gray = cv2.cvtColor(rgb, cv2.COLOR_RGB2GRAY)
                    hsv = cv2.cvtColor(rgb, cv2.COLOR_RGB2HSV)
                    sharpness_values.append(float(cv2.Laplacian(gray, cv2.CV_64F).var()))
                    saturation_values.append(float(np.mean(hsv[:, :, 1])))
                    small = cv2.resize(gray, (160, 90), interpolation=cv2.INTER_AREA)
                else:
                    gray = np.dot(rgb[..., :3], [0.299, 0.587, 0.114]).astype("float32")
                    sharpness_values.append(float(np.var(gray)))
                    saturation_values.append(0.0)
                    small = gray[:: max(1, gray.shape[0] // 90 or 1), :: max(1, gray.shape[1] // 160 or 1)]
                contrast_values.append(float(np.std(gray)))
                if prev_small is not None:
                    variance_values.append(float(np.mean(np.abs(small.astype("float32") - prev_small.astype("float32")))))
                prev_small = small
            except Exception:
                continue

    def _avg(values: list[float]) -> float:
        return round(float(sum(values) / max(len(values), 1)), 3) if values else 0.0

    analyzed_seconds = min(float(max_seconds or 180.0), float(duration_sec or 0.0) or float(max_seconds or 180.0))
    metrics = {
        "analysis_mode": "preview_frames",
        "preview_only": True,
        "duration_sec": round(float(duration_sec or 0.0), 3),
        "analysis_seconds": round(float(analyzed_seconds or 0.0), 3),
        "sampled_frames": len(frame_paths),
        "preview_frame_count": len(frame_paths),
        "avg_motion": 0.0,
        "avg_sharpness": _avg(sharpness_values),
        "avg_contrast": _avg(contrast_values),
        "avg_saturation": _avg(saturation_values),
        "avg_frame_variance": _avg(variance_values),
        "cut_events": 0,
        "cuts_per_minute": 0.0,
        "hook_motion_avg_first_30_sec": 0.0,
        "hook_sharpness_avg_first_30_sec": _avg(sharpness_values[:3]),
    }
    return {"frame_paths": frame_paths, "metrics": metrics}


async def _audit_manual_comparison_video(
    video_path: str,
    *,
    filename: str = "",
    output_dir: Path,
    max_seconds: float = 1200.0,
) -> dict:
    source_path = str(video_path or "").strip()
    if not source_path or not Path(source_path).exists():
        return {}
    output_dir.mkdir(parents=True, exist_ok=True)
    proxy_info = await _build_catalyst_analysis_proxy_video(
        source_path,
        output_dir / "proxy",
        max_seconds=max_seconds,
    )
    analysis_video_path = str(proxy_info.get("video_path", "") or "").strip() or source_path
    try:
        if callable(_reference_extract_video_metadata):
            metadata = await _reference_extract_video_metadata(source_path)
        else:
            metadata = {}
    except Exception:
        metadata = {}
    frame_pack = await _extract_reference_video_full_audit(
        analysis_video_path,
        output_dir / "frames",
        max_keyframes=10,
        max_seconds=max_seconds,
    )
    frame_metrics = dict(frame_pack.get("metrics") or {})
    original_duration_sec = float(metadata.get("duration_sec", 0) or 0.0)
    if original_duration_sec > 0:
        frame_metrics["original_duration_sec"] = round(original_duration_sec, 2)
        if float(max_seconds or 0.0) > 0 and original_duration_sec > float(max_seconds) + 1.0:
            frame_metrics["full_runtime_covered"] = False
    if proxy_info:
        frame_metrics["proxy_used"] = bool(proxy_info.get("proxy_used"))
        if int(float(proxy_info.get("source_size_bytes", 0) or 0) or 0) > 0:
            frame_metrics["source_size_bytes"] = int(float(proxy_info.get("source_size_bytes", 0) or 0) or 0)
        if int(float(proxy_info.get("proxy_size_bytes", 0) or 0) or 0) > 0:
            frame_metrics["proxy_size_bytes"] = int(float(proxy_info.get("proxy_size_bytes", 0) or 0) or 0)
        proxy_error = str(proxy_info.get("error", "") or "").strip()
        if proxy_error:
            frame_metrics["proxy_error"] = _clip_text(proxy_error, 220)
    transcript_excerpt = ""
    audio_path = ""
    try:
        if callable(_reference_extract_audio_from_video):
            audio_path = await _reference_extract_audio_from_video(
                analysis_video_path,
                max_seconds=min(float(max_seconds or _reference_audio_max_seconds), _reference_audio_max_seconds),
            ) or ""
        if audio_path and callable(_reference_transcribe_audio_with_grok):
            transcript_excerpt = await _reference_transcribe_audio_with_grok(audio_path)
    except Exception:
        transcript_excerpt = ""
    finally:
        if audio_path:
            try:
                Path(audio_path).unlink(missing_ok=True)
            except Exception:
                pass
    title = Path(filename or source_path).stem.replace("_", " ").strip() or "Uploaded comparison video"
    duration_sec = int(float(metadata.get("duration_sec", 0) or frame_metrics.get("duration_sec", 0) or 0) or 0)
    summary_bits = [
        f"Uploaded comparison video: {_clip_text(title, 180)}.",
        f"Duration: {duration_sec}s." if duration_sec > 0 else "",
        (
            f"Moving-video audit covered about {int(float(frame_metrics.get('timeline_frames_analyzed', 0) or 0) or 0)} frames and exported {int(float(frame_metrics.get('sampled_frames', 0) or 0) or 0)} keyframes."
            if frame_metrics
            else ""
        ),
        (
            f"Estimated cut density: {float(frame_metrics.get('cuts_per_minute', 0.0) or 0.0):.1f} cuts per minute."
            if float(frame_metrics.get("cuts_per_minute", 0.0) or 0.0) > 0
            else ""
        ),
        (
            "Catalyst downsampled the uploaded comparison video into a lighter analysis proxy before auditing it."
            if bool(frame_metrics.get("proxy_used"))
            else ""
        ),
        ("Transcript excerpt: " + _clip_text(transcript_excerpt, 260)) if transcript_excerpt else "",
    ]
    return {
        "title": _clip_text(title, 180),
        "duration_sec": duration_sec,
        "frame_metrics": frame_metrics,
        "transcript_excerpt": _clip_text(transcript_excerpt, 2800),
        "summary": " ".join(part for part in summary_bits if part),
    }


async def _extract_reference_video_sample_frames(
    video_path: str,
    output_dir: Path,
    *,
    max_frames: int = 12,
    max_seconds: float = 180.0,
) -> dict:
    if cv2 is None or np is None:
        return {"frame_paths": [], "metrics": {"error": "opencv_unavailable"}}
    source_path = str(video_path or "").strip()
    if not source_path or not Path(source_path).exists():
        return {"frame_paths": [], "metrics": {"error": "missing_video"}}

    output_dir.mkdir(parents=True, exist_ok=True)

    def _run() -> dict:
        cap = cv2.VideoCapture(source_path)
        if not cap.isOpened():
            return {"frame_paths": [], "metrics": {"error": "cannot_open_video"}}

        fps = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        duration_sec = (total_frames / fps) if fps > 0 and total_frames > 0 else 0.0
        analysis_seconds = min(float(max_seconds or 180.0), duration_sec or float(max_seconds or 180.0))
        if fps <= 0:
            fps = 24.0
        max_frame_index = min(total_frames - 1, max(0, int(round(analysis_seconds * fps)) - 1)) if total_frames > 0 else 0
        sample_count = max(6, min(int(max_frames or 12), 18))
        target_indices = sorted({
            int(round((max_frame_index * idx) / max(sample_count - 1, 1)))
            for idx in range(sample_count)
        })
        stride = max(1, int(round(fps / 2.0)))
        prev_gray = None
        idx = 0
        motion_rows: list[dict] = []
        frame_paths: list[str] = []
        saved_indices: set[int] = set()
        target_set = set(target_indices)
        while True:
            ok, frame = cap.read()
            if not ok:
                break
            if idx > max_frame_index:
                break
            should_analyze = (idx % stride == 0) or (idx in target_set)
            gray = None
            if should_analyze:
                gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)
                motion = 0.0 if prev_gray is None else float(np.mean(cv2.absdiff(gray, prev_gray)))
                sharpness = float(cv2.Laplacian(gray, cv2.CV_64F).var())
                contrast = float(np.std(gray))
                saturation = float(np.mean(hsv[:, :, 1]))
                motion_rows.append(
                    {
                        "frame_index": idx,
                        "timestamp_sec": round(idx / fps, 3),
                        "motion": round(motion, 3),
                        "sharpness": round(sharpness, 3),
                        "contrast": round(contrast, 3),
                        "saturation": round(saturation, 3),
                    }
                )
                prev_gray = gray
            if idx in target_set and idx not in saved_indices:
                frame_path = output_dir / f"frame_{idx:06d}.jpg"
                cv2.imwrite(str(frame_path), frame)
                frame_paths.append(str(frame_path))
                saved_indices.add(idx)
            idx += 1
        cap.release()

        if not motion_rows:
            return {
                "frame_paths": frame_paths,
                "metrics": {
                    "duration_sec": round(duration_sec, 2),
                    "analysis_seconds": round(analysis_seconds, 2),
                    "sampled_frames": len(frame_paths),
                    "error": "no_metrics",
                },
            }

        avg_motion = sum(float(row.get("motion", 0.0) or 0.0) for row in motion_rows) / max(len(motion_rows), 1)
        avg_sharpness = sum(float(row.get("sharpness", 0.0) or 0.0) for row in motion_rows) / max(len(motion_rows), 1)
        avg_contrast = sum(float(row.get("contrast", 0.0) or 0.0) for row in motion_rows) / max(len(motion_rows), 1)
        avg_saturation = sum(float(row.get("saturation", 0.0) or 0.0) for row in motion_rows) / max(len(motion_rows), 1)
        cut_events = [row for row in motion_rows if float(row.get("motion", 0.0) or 0.0) >= 18.0]
        hook_rows = [row for row in motion_rows if float(row.get("timestamp_sec", 0.0) or 0.0) <= 30.0]
        top_motion = sorted(
            motion_rows,
            key=lambda row: (-float(row.get("motion", 0.0) or 0.0), float(row.get("timestamp_sec", 0.0) or 0.0)),
        )[:6]
        low_motion = sorted(
            motion_rows,
            key=lambda row: (float(row.get("motion", 0.0) or 0.0), float(row.get("timestamp_sec", 0.0) or 0.0)),
        )[:6]
        return {
            "frame_paths": frame_paths[:18],
            "metrics": {
                "duration_sec": round(duration_sec, 2),
                "analysis_seconds": round(analysis_seconds, 2),
                "fps": round(fps, 2),
                "sampled_frames": len(frame_paths),
                "analyzed_steps": len(motion_rows),
                "avg_motion": round(avg_motion, 3),
                "avg_sharpness": round(avg_sharpness, 3),
                "avg_contrast": round(avg_contrast, 3),
                "avg_saturation": round(avg_saturation, 3),
                "cut_events": len(cut_events),
                "cuts_per_minute": round((len(cut_events) / max(analysis_seconds, 1.0)) * 60.0, 2),
                "hook_motion_avg_first_30_sec": round(
                    sum(float(row.get("motion", 0.0) or 0.0) for row in hook_rows) / max(len(hook_rows), 1),
                    3,
                ) if hook_rows else 0.0,
                "hook_sharpness_avg_first_30_sec": round(
                    sum(float(row.get("sharpness", 0.0) or 0.0) for row in hook_rows) / max(len(hook_rows), 1),
                    3,
                ) if hook_rows else 0.0,
                "top_motion_moments": top_motion,
                "lowest_motion_moments": low_motion,
            },
        }

    return await asyncio.to_thread(_run)
