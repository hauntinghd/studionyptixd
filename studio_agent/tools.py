"""Studio Agent tool registry + execution."""
from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import hmac
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from studio_agent import production_budget, production_costs, store
from studio_agent.fs_paths import skeleton_output_root
from studio_agent.production_slots import production_slot
from studio_agent import skills as skill_loader
from studio_agent import telemetry
from studio_agent.execution_context import current_production_command_id
from studio_agent.runpod_contract import (
    RUNPOD_PRODUCTION_TOOL_ALLOWLIST,
    runpod_longform_enabled,
    runpod_production_enabled,
    semantic_dispatch_id,
)
from backend_settings import FAL_PUBLIC_RENDERS_ENABLED, XAI_PUBLIC_RENDERS_ENABLED

ROOT = Path(__file__).resolve().parents[1]
SKELETON_OUTPUT = skeleton_output_root()

LONGFORM_TEXT_METERED_TOOLS = frozenset({
    "generate_longform_outline",
    "expand_longform_chapter",
})

# Tools that mutate state or spend money â€” require confirm mode approval.
APPROVAL_REQUIRED = frozenset({
    "start_longform_render",
    "start_shortform_generate",
    "expand_visual_proof_shortform",
    "ingest_cliplab_attachment",
    "render_cliplab_segments",
    "remix_cliplab_short",
    "set_production_scenes_animate",
    "animate_production_scenes",
    "repair_production_scene_animation",
    "finalize_production",
    "finalize_longform_render",
    "expand_longform_visual_proof",
    "regenerate_longform_thumbnail",
    "run_build_script",
    "write_project_file",
})

CLIPLAB_AGENT_ADMIN_USER_IDS = {
    uid.strip()
    for uid in (
        os.getenv("CLIPLAB_ADMIN_USER_IDS", "")
        + ","
        + os.getenv("STUDIO_ADMIN_USER_IDS", "")
        + ","
        + os.getenv("STUDIO_OWNER_USER_ID", "")
        + ",c16550b3-caf0-4aa4-bdcf-2f3fe53b2837"
    ).split(",")
    if uid.strip()
}

OWNER_ONLY_AGENT_TOOLS = frozenset({
    "ingest_cliplab_attachment",
    "analyze_cliplab_video",
    "render_cliplab_segments",
    "remix_cliplab_short",
    "poll_cliplab_job",
})

# Mutation / spend tools: runner paths call execute_tool_logged directly.
# The LLM must not invent these — hide from tool schemas offered to the model.
RUNNER_ONLY_AGENT_TOOLS = frozenset({
    "generate_longform_outline",
    "expand_longform_chapter",
    "start_shortform_generate",
    "start_longform_render",
    "expand_visual_proof_shortform",
    "expand_longform_visual_proof",
    "edit_production_scene_still",
    "edit_production_scenes_still",
    "regenerate_production_scene_still",
    "regenerate_production_scene",
    "set_production_scenes_animate",
    "set_production_scene_duration",
    "animate_production_scenes",
    "repair_production_scene_animation",
    "audit_and_repair_production_scenes",
    "finalize_production",
    "finalize_longform_render",
    "re_edit_production",
    "generate_longform_thumbnails",
    "regenerate_longform_thumbnail",
})

_async_pool = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="studio-agent-async")
_expand_command_lock = threading.Lock()


def _runpod_production_enabled() -> bool:
    """Feature flag for the strict production-only RunPod execution lane."""

    return runpod_production_enabled()


def _runpod_command_id(arguments: dict[str, Any] | None) -> str:
    """Resolve command identity in priority order without inventing one."""

    args = dict(arguments or {})
    return str(
        args.get("command_id")
        or args.get("_runpod_command_id")
        or current_production_command_id()
        or ""
    ).strip()


def _runpod_studio_job_id(
    name: str,
    *,
    command_id: str,
    user_id: str,
    session_id: str | None,
) -> str:
    """Create the stable Studio job id needed before async RunPod starts.

    RunPod's returned id identifies transport.  Studio still needs its own job
    id immediately so the UI can render one card and poll it while the worker
    is cold-starting.  The id is deterministic for an idempotent command and
    contains no user text or secrets.
    """

    prefix = "lf" if "longform" in str(name or "") else "sf"
    identity = "\0".join(
        (str(user_id or ""), str(session_id or ""), str(name or ""), str(command_id or ""))
    )
    return f"{prefix}_{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:20]}"


def _runpod_workspace_kind(name: str) -> str:
    """Map an allowlisted production tool to its durable workspace family."""

    return "longform" if "longform" in str(name or "").strip().lower() else "shortform"


def _runpod_failure_definitely_not_submitted(
    name: str,
    arguments: dict[str, Any] | None,
    *,
    command_id: str,
    user_id: str,
) -> bool:
    """True when a failed dispatch has no durable ambiguous-submit receipt."""

    try:
        from studio_agent import runpod_bridge

        dispatch_id = semantic_dispatch_id(
            name,
            dict(arguments or {}),
            command_id=command_id,
            user_id=user_id,
        )
        receipt = runpod_bridge.get_dispatch_receipt(dispatch_id)
    except Exception:
        # If the ledger itself cannot be read, retain the hold.  This matches
        # the dispatch ledger's fail-closed duplicate-spend policy.
        return False
    return not (
        isinstance(receipt, dict)
        and str(receipt.get("status") or "").strip().lower() == "dispatch_unknown"
        and bool(receipt.get("fail_closed"))
    )


@contextmanager
def _expand_job_file_lock(workspace: Path):
    """Serialize an expansion claim across API worker processes."""

    lock_path = workspace / ".expand_command.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+b")
    locked = False
    try:
        if os.name == "nt":
            import msvcrt

            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        locked = True
        yield
    finally:
        if locked:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        handle.close()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    temporary.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(temporary, path)

_SHORTFORM_CHANNEL_CATEGORY_ALIASES = {
    # Channel/registry keys are not Skeleton content lanes. MrSkeleWelly is the
    # psychology/skeleton channel, so route accidental channel-key usage to the
    # human behavior lane instead of failing the production after approval.
    "mrskelewelly": "human_limits",
    "mrskellywelly": "human_limits",
    "mrskelewellyai": "human_limits",
    "skeletonai": "human_limits",
}

_CREDIT_RESERVATION_FILE = "credit_reservation.json"


def _public_provider_block_message(name: str, arguments: dict[str, Any], budget: Any, user_id: str) -> str:
    # These are metered text/planning calls, not public media renders.  Their
    # selected Claude/Grok/OpenRouter route is independently priced and held
    # before inference, so the media-provider kill switches do not apply.
    if str(name or "") in LONGFORM_TEXT_METERED_TOOLS:
        return ""
    try:
        import unified_credits as uc
        if uc.is_unlimited(user_id):
            return ""
    except Exception:
        pass
    payload = json.dumps({"tool": name, "arguments": arguments or {}, "budget": getattr(budget, "breakdown", {}) or {}}, default=str).lower()
    uses_xai = any(marker in payload for marker in ("grok", "xai", "grok_imagine"))
    uses_fal = any(marker in payload for marker in ("fal", "seedream", "seedance", "pixverse", "kling", "minimax", "mmaudio"))
    if uses_xai and not XAI_PUBLIC_RENDERS_ENABLED:
        return "xAI/Grok public rendering is temporarily disabled. Owner can test it, but public users cannot spend against the xAI key."
    if uses_fal and not FAL_PUBLIC_RENDERS_ENABLED:
        return "FAL public rendering is temporarily disabled. Owner can test it, but public users cannot spend against the FAL key."
    return ""


def _run_async(coro):
    """Run async coroutine from sync execute_tool (may be called inside FastAPI loop)."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    fut = _async_pool.submit(asyncio.run, coro)
    return fut.result(timeout=120)


def _compact_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _normalize_shortform_category_args(args: dict[str, Any]) -> dict[str, Any]:
    """Keep selected channel keys from being used as Skeleton category lanes."""
    normalized = dict(args or {})
    raw_category = str(normalized.get("category_key") or normalized.get("category") or "").strip()
    mapped = _SHORTFORM_CHANNEL_CATEGORY_ALIASES.get(_compact_key(raw_category))
    if mapped:
        normalized.setdefault("_selected_channel_key", raw_category)
        normalized["category_key"] = mapped
    return normalized


def _compact_video_metric_rows(rows: list[dict[str, Any]], *, limit: int = 12) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in list(rows or []):
        if not isinstance(row, dict):
            continue
        title = str(row.get("title") or "").strip()
        video_id = str(row.get("video_id") or "").strip()
        if not title and not video_id:
            continue
        item = {
            "video_id": video_id,
            "title": title,
            "watch_url": str(row.get("watch_url") or (f"https://www.youtube.com/watch?v={video_id}" if video_id else "")).strip(),
            "published_at": str(row.get("published_at") or "").strip(),
            "views": int(float(row.get("views", row.get("view_count", 0)) or 0)),
            "average_view_duration_sec": int(float(row.get("average_view_duration_sec", 0) or 0)),
            "average_view_percentage": round(float(row.get("average_view_percentage", 0.0) or 0.0), 2),
            "impressions": int(float(row.get("impressions", 0) or 0)),
            "impression_click_through_rate": round(float(row.get("impression_click_through_rate", 0.0) or 0.0), 2),
            "is_short": bool(row.get("is_short") or row.get("shorts") or row.get("is_youtube_short")),
        }
        if row.get("view_count_source"):
            item["view_count_source"] = str(row.get("view_count_source") or "")
        if row.get("analytics_views_lagged") is not None:
            item["analytics_views_lagged"] = int(float(row.get("analytics_views_lagged") or 0))
        if row.get("velocity_views_lagged") is not None:
            item["velocity_views_lagged"] = int(float(row.get("velocity_views_lagged") or 0))
        if row.get("duration_sec") is not None:
            item["duration_sec"] = int(float(row.get("duration_sec") or 0))
            if item["duration_sec"] <= 180:
                item["is_short"] = True
        for source_key, out_key in (
            ("engaged_views", "engaged_views"),
            ("engagedViews", "engaged_views"),
            ("shorts_engaged_views", "engaged_views"),
            ("viewed_vs_swiped_away", "viewed_vs_swiped_away"),
            ("viewedVsSwipedAway", "viewed_vs_swiped_away"),
            ("swipe_away_rate", "swipe_away_rate"),
            ("swipeAwayRate", "swipe_away_rate"),
            ("stayed_to_watch_rate", "stayed_to_watch_rate"),
            ("stayedToWatchRate", "stayed_to_watch_rate"),
        ):
            if row.get(source_key) is not None:
                try:
                    value = float(row.get(source_key) or 0)
                    item[out_key] = int(value) if out_key == "engaged_views" else round(value, 2)
                except Exception:
                    item[out_key] = row.get(source_key)
        out.append(item)
        if len(out) >= limit:
            break
    return out


def _video_metric_summary(snapshot: dict[str, Any]) -> dict[str, Any]:
    uploaded = [dict(v or {}) for v in list((snapshot or {}).get("uploaded_videos") or []) if isinstance(v, dict)]
    top_videos = [dict(v or {}) for v in list((snapshot or {}).get("top_videos") or []) if isinstance(v, dict)]
    retention_videos = [dict(v or {}) for v in list((snapshot or {}).get("retention_videos") or []) if isinstance(v, dict)]
    rows_by_id: dict[str, dict[str, Any]] = {}
    for row in [*uploaded, *top_videos, *retention_videos]:
        video_id = str(row.get("video_id") or "").strip()
        key = video_id or str(row.get("title") or "").strip().lower()
        if not key:
            continue
        merged = dict(rows_by_id.get(key) or {})
        merged.update({k: v for k, v in row.items() if v not in (None, "", [], {})})
        rows_by_id[key] = merged
    rows = list(rows_by_id.values())
    rows_with_retention = [
        row for row in rows
        if float(row.get("average_view_percentage", 0.0) or 0.0) > 0
        or float(row.get("average_view_duration_sec", 0.0) or 0.0) > 0
    ]
    by_retention = sorted(
        rows_with_retention,
        key=lambda row: (
            -float(row.get("average_view_percentage", 0.0) or 0.0),
            -int(float(row.get("views", row.get("view_count", 0)) or 0)),
        ),
    )
    short_candidates = [
        row for row in rows_with_retention
        if bool(row.get("is_short") or row.get("shorts") or row.get("is_youtube_short"))
        or (row.get("duration_sec") is not None and int(float(row.get("duration_sec") or 0)) <= 180)
    ]
    by_views = sorted(
        rows,
        key=lambda row: -int(float(row.get("views", row.get("view_count", 0)) or 0)),
    )
    latest_upload = {}
    dated_rows = [
        row for row in rows
        if str(row.get("published_at") or "").strip()
    ]
    if dated_rows:
        latest_upload = dict(max(
            dated_rows,
            key=lambda row: (
                str(row.get("published_at") or ""),
                str(row.get("video_id") or ""),
            ),
        ))
    elif uploaded:
        latest_upload = dict(uploaded[0] or {})
    return {
        "video_rows_available": len(rows),
        "video_level_retention_available": bool(rows_with_retention),
        "retention_rows_available": len(rows_with_retention),
        "top_by_retention": _compact_video_metric_rows(by_retention, limit=12),
        "top_shorts_by_retention": _compact_video_metric_rows(short_candidates, limit=12),
        "top_by_views": _compact_video_metric_rows(by_views, limit=12),
        "latest_upload": (_compact_video_metric_rows([latest_upload], limit=1) or [{}])[0] if latest_upload else {},
    }


def _shortform_metric_value(row: dict[str, Any], *keys: str) -> float:
    for key in keys:
        value = (row or {}).get(key)
        if value in (None, ""):
            continue
        try:
            return float(value or 0)
        except Exception:
            continue
    return 0.0


def _is_shortform_metric_row(row: dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    if bool(row.get("is_short") or row.get("shorts") or row.get("is_youtube_short")):
        return True
    try:
        duration = int(float(row.get("duration_sec") or 0))
        return 0 < duration <= 180
    except Exception:
        return False


def _shortform_performance_score(row: dict[str, Any], latest_views_baseline: float = 0.0) -> float:
    views = _shortform_metric_value(row, "views", "view_count")
    engaged = _shortform_metric_value(row, "engaged_views", "engagedViews", "shorts_engaged_views")
    apv = _shortform_metric_value(row, "average_view_percentage")
    stayed = _shortform_metric_value(row, "stayed_to_watch_rate", "stayedToWatchRate", "viewed_vs_swiped_away", "viewedVsSwipedAway")
    swipe_away = _shortform_metric_value(row, "swipe_away_rate", "swipeAwayRate")
    likes = _shortform_metric_value(row, "likes", "like_count")
    comments = _shortform_metric_value(row, "comments", "comment_count")
    interactions_per_view = ((likes + comments) / views * 100.0) if views > 0 else 0.0
    normalized_views = min((views / max(latest_views_baseline, 1.0)) * 20.0, 45.0) if latest_views_baseline else min(views / 25.0, 45.0)
    engaged_score = min(engaged / 25.0, 35.0) if engaged else 0.0
    swipe_score = stayed if stayed else max(0.0, 100.0 - swipe_away) if swipe_away else 0.0
    return round(
        normalized_views
        + min(apv, 160.0) * 0.35
        + min(swipe_score, 100.0) * 0.25
        + engaged_score
        + min(interactions_per_view, 15.0),
        2,
    )


def _compare_shortform_video_metrics(video_metrics: dict[str, Any]) -> dict[str, Any]:
    rows_by_key: dict[str, dict[str, Any]] = {}

    def _add_rows(raw_rows: Any) -> None:
        candidates = raw_rows if isinstance(raw_rows, list) else [raw_rows]
        for raw in list(candidates or []):
            if not isinstance(raw, dict):
                continue
            key = str(raw.get("video_id") or "").strip() or str(raw.get("title") or "").strip().lower()
            if not key:
                continue
            merged = dict(rows_by_key.get(key) or {})
            merged.update({k: v for k, v in raw.items() if v not in (None, "", [], {})})
            rows_by_key[key] = merged

    for bucket_name in ("latest_upload", "top_shorts_by_retention", "top_by_retention", "top_by_views"):
        _add_rows((video_metrics or {}).get(bucket_name))

    rows = [dict(row or {}) for row in rows_by_key.values() if _is_shortform_metric_row(row)]
    latest = dict((video_metrics or {}).get("latest_upload") or {})
    if latest and _is_shortform_metric_row(latest):
        latest_key = str(latest.get("video_id") or "").strip() or str(latest.get("title") or "").strip().lower()
        if latest_key and latest_key in rows_by_key:
            latest.update(rows_by_key[latest_key])
        elif latest_key:
            rows.append(latest)
    latest_id = str(latest.get("video_id") or "").strip()
    latest_views = _shortform_metric_value(latest, "views", "view_count")

    prior_rows = [
        dict(row or {})
        for row in rows
        if not latest_id or str(row.get("video_id") or "").strip() != latest_id
    ]
    if not prior_rows and rows:
        prior_rows = [dict(row or {}) for row in rows if str(row.get("title") or "") != str(latest.get("title") or "")]
    scored_prior: list[dict[str, Any]] = []
    for row in prior_rows:
        scored = dict(row)
        scored["shortform_score"] = _shortform_performance_score(scored, latest_views_baseline=max(latest_views, 1.0))
        scored_prior.append(scored)
    best = dict(max(scored_prior, key=lambda row: float(row.get("shortform_score") or 0), default={}))
    latest_scored = dict(latest)
    if latest_scored:
        latest_scored["shortform_score"] = _shortform_performance_score(latest_scored, latest_views_baseline=max(latest_views, 1.0))

    available_metrics: list[str] = []
    missing_metrics: list[str] = []
    sample_rows = [latest_scored, best, *scored_prior[:3]]
    metric_checks = {
        "views": ("views", "view_count"),
        "average_percentage_viewed": ("average_view_percentage",),
        "average_view_duration": ("average_view_duration_sec",),
        "engaged_views": ("engaged_views", "engagedViews", "shorts_engaged_views"),
        "stayed_to_watch_or_swipe_rate": ("stayed_to_watch_rate", "viewed_vs_swiped_away", "swipe_away_rate"),
        "likes_comments": ("likes", "comments", "like_count", "comment_count"),
    }
    for label, keys in metric_checks.items():
        if any(_shortform_metric_value(row, *keys) > 0 for row in sample_rows):
            available_metrics.append(label)
        else:
            missing_metrics.append(label)

    recommendations: list[str] = []
    best_title = str(best.get("title") or "").strip()
    latest_title = str(latest_scored.get("title") or "").strip()
    if best_title:
        recommendations.append(f"Use the prior winner's promise shape as the control: {best_title}")
    if latest_title:
        recommendations.append(f"Do not judge the latest Short from stale low private rows if public views are fresher: {latest_title}")
    if _shortform_metric_value(latest_scored, "average_view_percentage") and _shortform_metric_value(best, "average_view_percentage"):
        if _shortform_metric_value(latest_scored, "average_view_percentage") < _shortform_metric_value(best, "average_view_percentage"):
            recommendations.append("Tighten the first 1-2 seconds: the latest Short is behind the prior winner on average percentage viewed.")
        else:
            recommendations.append("The latest Short is competitive on average percentage viewed; package the next upload around the same immediate-stakes hook.")
    recommendations.append("For Lexi Manhua Shorts, title around the character conflict, betrayal, revenge, secret identity, or impossible comeback instead of generic anime/manhwa labels.")

    return {
        "content_type": "shorts",
        "latest_short": latest_scored,
        "best_prior_short": best,
        "prior_short_count": len(prior_rows),
        "available_short_metrics": available_metrics,
        "missing_short_metrics": missing_metrics,
        "metric_policy": (
            "Shorts are compared on views, engaged views when available, stayed-to-watch/swipe signals when available, "
            "average percentage viewed, AVD, and interactions per view. Long-form CTR/chapter logic is not used as a substitute."
        ),
        "recommendations": recommendations,
    }


def _promote_latest_upload_from_velocity(
    video_metrics: dict[str, Any],
    velocity: dict[str, Any],
) -> dict[str, Any]:
    """Use YouTube's date-ordered latest video as the authoritative current upload.

    The analytics snapshot can be stale or retention/top-video ordered. For requests
    about "the current/latest video", the latest identity must come from the
    date-ordered YouTube Data API result, then matching analytics metrics can be
    layered onto that same video ID.
    """
    metrics = dict(video_metrics or {})
    latest_video_id = str((velocity or {}).get("video_id") or "").strip()
    if not latest_video_id:
        return metrics

    matching_metric_row: dict[str, Any] = {}
    for bucket_name in ("latest_upload", "top_by_retention", "top_shorts_by_retention", "top_by_views"):
        bucket = metrics.get(bucket_name)
        candidates = bucket if isinstance(bucket, list) else [bucket]
        for row in list(candidates or []):
            if not isinstance(row, dict):
                continue
            if str(row.get("video_id") or "").strip() == latest_video_id:
                matching_metric_row = dict(row)
                break
        if matching_metric_row:
            break

    latest = dict(matching_metric_row)
    title = str((velocity or {}).get("title") or "").strip()
    published_at = str((velocity or {}).get("published_at") or "").strip()
    existing_views = int(float(latest.get("views", latest.get("view_count", 0)) or 0) or 0)
    velocity_views = int(float((velocity or {}).get("views", existing_views) or 0) or 0)
    best_views = max(existing_views, velocity_views)
    latest.update({
        "video_id": latest_video_id,
        "title": title or str(latest.get("title") or "").strip(),
        "watch_url": str((velocity or {}).get("watch_url") or f"https://www.youtube.com/watch?v={latest_video_id}").strip(),
        "published_at": published_at or str(latest.get("published_at") or "").strip(),
        "views": best_views,
        "view_count": best_views,
        "latest_upload_source": "youtube_latest_video_velocity",
    })
    if existing_views > velocity_views and existing_views > 0:
        latest["view_count_source"] = "existing_public_or_inventory_fresher_than_velocity"
        latest["velocity_views_lagged"] = velocity_views
    if (velocity or {}).get("hours_since_upload") is not None:
        latest["hours_since_upload"] = float((velocity or {}).get("hours_since_upload") or 0)
    if (velocity or {}).get("velocity_vph") is not None:
        latest["velocity_vph"] = float((velocity or {}).get("velocity_vph") or 0)
    if (velocity or {}).get("is_decaying") is not None:
        latest["is_decaying"] = bool((velocity or {}).get("is_decaying"))

    metrics["latest_upload"] = latest
    for bucket_name in ("top_by_retention", "top_shorts_by_retention", "top_by_views"):
        bucket = metrics.get(bucket_name)
        if not isinstance(bucket, list):
            continue
        updated_bucket: list[dict[str, Any]] = []
        for row in bucket:
            if not isinstance(row, dict):
                continue
            if str(row.get("video_id") or "").strip() == latest_video_id:
                patched = dict(row)
                patched.update({
                    "title": latest["title"],
                    "watch_url": latest["watch_url"],
                    "published_at": latest["published_at"],
                    "views": latest["views"],
                    "view_count": latest["views"],
                    "latest_upload_source": "youtube_latest_video_velocity",
                })
                if latest.get("view_count_source"):
                    patched["view_count_source"] = latest.get("view_count_source")
                    patched["velocity_views_lagged"] = latest.get("velocity_views_lagged")
                updated_bucket.append(patched)
            else:
                updated_bucket.append(row)
        metrics[bucket_name] = updated_bucket
    return metrics


def _live_channel_record_from_snapshot(snapshot: dict[str, Any], fallback_record: dict[str, Any] | None = None) -> dict[str, Any]:
    fallback_record = fallback_record or {}
    return {
        "title": snapshot.get("channel_title") or fallback_record.get("title") or fallback_record.get("channel_handle") or "",
        "channel_handle": snapshot.get("channel_handle") or fallback_record.get("channel_handle") or "",
        "subscriber_count": int(float(snapshot.get("subscriber_count", 0) or 0)),
        "video_count": int(float(snapshot.get("video_count", snapshot.get("channel_video_count", 0)) or 0)),
        "view_count": int(float(snapshot.get("view_count", 0) or 0)),
        "analytics_snapshot": snapshot,
    }


def _channel_match_token(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _channel_registry_aliases(registry_key: str) -> set[str]:
    if str(registry_key or "").strip() == "lexi_manhua":
        registry_key = "lexi_manhwa"
    raw: set[Any] = {registry_key}
    try:
        from long_form.prompts.channels import CHANNELS

        cfg = dict(CHANNELS.get(registry_key) or {})
        raw.update({
            cfg.get("key", ""),
            cfg.get("label", ""),
            cfg.get("channel_id", ""),
            cfg.get("tagline", ""),
        })
    except Exception:
        pass
    hard_aliases = {
        "zerotier": ["ZeroTier", "zero tier", "@zerotierr", "zerotierr"],
        "empire_magnates": ["Empire Magnates", "@empiremagnates", "empiremagnates"],
        "cryptic_science": ["CrypticScience", "Cryptic Science", "@crypticscience"],
        "history_rewind": ["History Rewind", "@historyyyrewinddd"],
        "pb_live": ["PB Lies", "PB Live", "@pblies"],
        "lexi_manhwa": ["Lexi Manhwa", "Lexi Manhua", "MLEXI MANHUA", "@leximanhwa", "@leximanhua"],
        "lexi_manhua": ["Lexi Manhwa", "Lexi Manhua", "MLEXI MANHUA", "@leximanhwa", "@leximanhua"],
    }
    raw.update(hard_aliases.get(registry_key, []))
    return {_channel_match_token(v) for v in raw if _channel_match_token(v)}


def _connected_channel_tokens(channel_key: str, record: dict[str, Any]) -> set[str]:
    rec = dict(record or {})
    values: list[Any] = [
        channel_key,
        rec.get("channel_id"),
        rec.get("title"),
        rec.get("channel_title"),
        rec.get("custom_url"),
        rec.get("channel_handle"),
        rec.get("handle"),
        rec.get("channel_url"),
    ]
    return {_channel_match_token(v) for v in values if _channel_match_token(v)}


def _resolve_user_channel_connection(
    user_id: str,
    requested_channel_id: str,
    registry_key: str,
) -> dict[str, Any]:
    """Resolve Studio's selected channel to the actual connected OAuth row."""
    requested = str(requested_channel_id or "").strip()
    reg_key = str(registry_key or "").strip()
    fallback_id = requested
    registry_channel_id = ""
    try:
        from long_form.catalyst_bridge import CHANNEL_KEY_TO_ID

        if reg_key:
            registry_channel_id = str(CHANNEL_KEY_TO_ID.get(reg_key, "") or "").strip()
        if registry_channel_id:
            fallback_id = registry_channel_id
    except Exception:
        pass

    out = {
        "requested_channel_id": requested,
        "requested_registry_key": reg_key,
        "registry_channel_id": registry_channel_id,
        "lookup_channel_id": fallback_id,
        "analytics_channel_id": fallback_id,
        "snapshot_channel_id": fallback_id,
        "matched": False,
        "matched_by": "none",
        "corrected": False,
        "record": {},
    }
    uid = str(user_id or "").strip()
    if not uid:
        return out

    channels: dict[str, Any] = {}
    try:
        from youtube_connections_store import hydrate

        bucket = dict((hydrate() or {}).get(uid) or {})
        channels = dict(bucket.get("channels") or {})
    except Exception:
        channels = {}
    if not channels:
        return out

    def _match(channel_key: str, record: dict[str, Any], matched_by: str) -> dict[str, Any]:
        rec = dict(record or {})
        lookup_id = str(channel_key or fallback_id).strip()
        canonical_id = str(rec.get("channel_id") or lookup_id or fallback_id).strip()
        return {
            **out,
            "lookup_channel_id": lookup_id,
            "analytics_channel_id": canonical_id or lookup_id,
            "snapshot_channel_id": lookup_id,
            "matched": True,
            "matched_by": matched_by,
            "corrected": bool(
                (requested and fallback_id and requested != fallback_id)
                or (fallback_id and lookup_id and lookup_id != fallback_id)
            ),
            "record": rec,
        }

    if fallback_id and fallback_id in channels and isinstance(channels.get(fallback_id), dict):
        return _match(fallback_id, channels[fallback_id], "exact_channel_id")

    requested_token = _channel_match_token(fallback_id)
    for channel_key, record in channels.items():
        if not isinstance(record, dict):
            continue
        if requested_token and requested_token in _connected_channel_tokens(str(channel_key), record):
            return _match(str(channel_key), record, "normalized_channel_id")

    aliases = _channel_registry_aliases(reg_key)
    if aliases:
        for channel_key, record in channels.items():
            if not isinstance(record, dict):
                continue
            if aliases.intersection(_connected_channel_tokens(str(channel_key), record)):
                return _match(str(channel_key), record, "registry_alias")

    return out


def tool_schemas() -> list[dict[str, Any]]:
    return [
        {
            "type": "function",
            "function": {
                "name": "list_skills",
                "description": "List all Rookcast skill slugs imported into studio/skills/.",
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "load_skill",
                "description": "Load a Rookcast SKILL.md playbook by slug (e.g. script-writing, thumbnail-design).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "slug": {"type": "string", "description": "Skill folder name"},
                        "companion": {
                            "type": "string",
                            "description": "Optional companion file e.g. beat-anatomy.md",
                        },
                    },
                    "required": ["slug"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "load_channel_docs",
                "description": "Load CHANNEL.md and/or FLOW.md for a Studio channel key.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "channel_key": {"type": "string"},
                        "doc": {
                            "type": "string",
                            "enum": ["CHANNEL", "FLOW", "both"],
                            "default": "both",
                        },
                    },
                    "required": ["channel_key"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_studio_channels",
                "description": "List long-form channel keys from long_form/prompts/channels.py registry.",
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "read_project_file",
                "description": "Read a text file under the repo root (paths must stay inside workspace).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "relative_path": {"type": "string"},
                        "max_chars": {"type": "integer", "default": 12000},
                    },
                    "required": ["relative_path"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "write_project_file",
                "description": "Write or overwrite a text file under studio/ or long_form/ (approval in confirm mode).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "relative_path": {"type": "string"},
                        "content": {"type": "string"},
                    },
                    "required": ["relative_path", "content"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "start_longform_render",
                "description": (
                    "Queue a long-form render via the Studio pipeline. "
                    "Requires channel_key + outline JSON. Spends fal credits."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "channel_key": {"type": "string"},
                        "title": {"type": "string"},
                        "topic": {"type": "string"},
                        "chapters_json": {
                            "type": "string",
                            "description": "JSON string: {title, chapters:[{title, beats}]}",
                        },
                        "motion_policy": {
                            "type": "string",
                            "enum": ["full", "balanced", "economy", "stills"],
                            "description": (
                                "Long-form motion budget. balanced animates about 35% hero scenes; economy about 15%; "
                                "stills uses local cinematic motion; full runs i2v on every scene."
                            ),
                        },
                        "hero_motion_ratio": {
                            "type": "number",
                            "description": "Optional exact 0-1 fraction of scenes that receive paid i2v.",
                        },
                        "render_style": {
                            "type": "string",
                            "description": (
                                "Studio art-style key. This becomes a strict production-wide lock across every chapter, "
                                "scene, still, animation prompt, and thumbnail."
                            ),
                        },
                        "max_budget_usd": {
                            "type": "number",
                            "description": "Hard preflight budget cap. Studio refuses to start if estimated provider spend is higher.",
                        },
                        "sfx_enabled": {
                            "type": "boolean",
                            "description": "Whether to include sound design / ambient SFX in the long-form render. Default false to avoid hidden paid audio spend.",
                        },
                        "sound_design_brief": {
                            "type": "string",
                            "description": "Long-form sound direction: ambience, SFX motifs, soundscape, tension beds, product sounds, etc.",
                        },
                        "background_music": {
                            "type": "string",
                            "description": "Music bed direction, or off/no background music.",
                        },
                        "studio_promotion_mode": {
                            "type": "string",
                            "enum": ["off", "subtle", "direct"],
                            "description": (
                                "How the upload package may promote NYPTID Studio. Use subtle by default, "
                                "direct for Studio demonstrations or explicit promotion, and off when declined."
                            ),
                        },
                        "visual_proof_only": {
                            "type": "boolean",
                            "description": "Generate exactly one proof still first. Defaults true for long-form; expand only after user approval.",
                            "default": True,
                        },
                        "ken_burns_enabled": {"type": "boolean", "description": "Apply local cinematic zoom/pan to still-only scenes at compose time."},
                        "light_shake_enabled": {"type": "boolean", "description": "Add rare, subtle local camera emphasis without paid image-to-video."},
                        "image_model_id": {"type": "string", "description": "Session image model used for proof and scene stills."},
                        "captions_enabled": {"type": "boolean", "default": True, "description": "Burn synchronized captions into the final long-form video."},
                        "caption_mode": {"type": "string", "enum": ["word", "phrase", "off"], "default": "word"},
                    },
                    "required": ["channel_key", "title", "topic"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "expand_longform_visual_proof",
                "description": "After the user approves the one-scene long-form proof, generate the remaining still gallery from that approved foundation. Requires approval because it spends image credits.",
                "parameters": {"type": "object", "properties": {"job_id": {"type": "string"}}, "required": ["job_id"]},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_longform_scenes",
                "description": "List every planned long-form scene, prompt, narration, still URL, and completeness status before editing or finalizing.",
                "parameters": {
                    "type": "object",
                    "properties": {"job_id": {"type": "string"}},
                    "required": ["job_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "regenerate_longform_still",
                "description": "Regenerate one selected long-form still with the current session image route and invalidate dependent animation/composition output.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "scene_idx": {"type": "integer", "minimum": 0},
                        "reason": {"type": "string"},
                    },
                    "required": ["job_id", "scene_idx"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_skeleton_video_models",
                "description": (
                    "List selectable i2v models for Skeleton AI shorts. Image stills are "
                    "ALWAYS canonical Seedream 4.5 edit (not selectable). User picks video only."
                ),
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_skeleton_categories",
                "description": (
                    "List Skeleton AI script categories: 20 YouTube-aligned built-ins "
                    "(outcast, people_blogs, gaming, â€¦) plus this user's custom categories. "
                    "Call before start_shortform_generate when category is non-obvious."
                ),
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "create_skeleton_category",
                "description": (
                    "Create a custom Skeleton AI category for this user (e.g. outcast, "
                    "true crime lane, channel-specific tone). Returns the new category_key."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "label": {"type": "string", "description": "Display name e.g. Outcast"},
                        "key": {"type": "string", "description": "Optional slug; auto-generated if omitted"},
                        "tagline": {"type": "string"},
                        "system_prompt": {
                            "type": "string",
                            "description": "Optional Grok system tone; auto-generated from label if omitted",
                        },
                        "seed_ideas": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                    },
                    "required": ["label"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_render_styles",
                "description": (
                    "List Studio shortform render styles (cinematic, comic book, Ghibli, skeleton host, etc.). "
                    "ALWAYS pass render_style to start_shortform_generate â€” default to the user's session "
                    "picker unless they explicitly choose another. skeleton_host = Skeleton niche art style. "
                    "Returns visual preview URLs for a gallery grid (like the reference style cards)."
                ),
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "generate_longform_thumbnails",
                "description": (
                    "Generate or reprompt 1-3 thumbnails for a planned or active longform video (user chooses for A/B test). "
                    "This is explicitly allowed in Plan mode and does not start the script, scenes, voice, or video render. "
                    "feedback for reprompt (e.g. 'more dramatic lighting, teal/orange grade, teaser not spoiler, match the video tone exactly'). "
                    "If there is no longform job yet, omit job_id and provide title/channel_key; Studio creates a thumbnail-only review job. "
                    "STYLE SOURCE: when the channel_key has a connected public channel, candidates are automatically "
                    "style-locked to that channel's real published covers (pulled as edit references) with a short "
                    "on-image title matching the covers' text treatment — tell the user this. When no channel is "
                    "connected, there is no style authority: ask for reference covers or explicit style direction first. "
                    "Keeps iterations attached to the current plan. "
                    "After user approves, download the package.txt (title/tags/desc + exact timestamps)."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "count": {"type": "integer", "description": "1-3 thumbnails"},
                        "feedback": {"type": "string", "description": "Reprompt instruction for edit"},
                        "title": {"type": "string", "description": "Current planned long-form title when no job exists yet."},
                        "channel_key": {"type": "string", "description": "Long-form channel/style key, e.g. history_rewind or empire_magnates."},
                        "prompt": {"type": "string", "description": "Optional exact thumbnail direction. Preserve the planned title and channel grammar."},
                        "max_budget_usd": {
                            "type": "number",
                            "description": "Hard preflight budget cap for thumbnail generation.",
                        },
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "ingest_product_reference",
                "description": (
                    "Create a durable product-reference manifest for a software or physical-product advertisement. "
                    "Uses images attached in the current chat and/or safely crawls a public product website for "
                    "dedicated product images. If website_url is omitted, use the product website saved on the user's "
                    "Studio profile. Call before start_shortform_generate for product ads."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "website_url": {"type": "string", "description": "Official product or landing-page URL."},
                        "use_attached_images": {"type": "boolean", "description": "Use product images attached in this chat."},
                        "product_name": {"type": "string"},
                        "product_description": {"type": "string"},
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "start_shortform_generate",
                "description": (
                    "Queue a styled shortform render (9:16, ~12 beats). "
                    "REQUIRED: render_style from list_render_styles or the user's session Art Style picker. "
                    "category_key is a Skeleton content lane, not the selected YouTube channel key. "
                    "For MrSkeleWelly psychology shorts, use human_limits. "
                    "Default cinematic/photoreal for documentaries and real people â€” NOT skeleton unless "
                    "render_style=skeleton_host. Comic/history/anime/etc. each have their own T2I look. "
                    "Call list_skeleton_video_models for video_model; list_skeleton_categories for script tone. "
                    "If the user asks for one still, one image, one scene, first still/image, visual proof, or to "
                    "approve the look before a full short, pass visual_proof_only=true and scene_count=1. "
                    "After starting (for non-skeleton styles), the job goes to a review gate where you can use "
                    "the scene control tools (list_production_scenes, edit_production_scene_still with V4.5 edit, "
                    "set_production_scenes_animate, animate_production_scenes, etc.) for full creative control."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "render_style": {
                            "type": "string",
                            "description": (
                                "e.g. cinematic, comic_book, studio_ghibli, skeleton_host. "
                                "Must match user session unless they override in chat."
                            ),
                        },
                        "category_key": {
                            "type": "string",
                            "description": (
                                "Skeleton script/content lane e.g. human_limits, outcast, people_blogs, custom_my_lane. "
                                "Do not pass a YouTube channel/registry key here."
                            ),
                        },
                        "topic": {"type": "string"},
                        "script": {"type": "string", "description": "Optional pre-written script"},
                        "scene_count": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": 60,
                            "description": "Planned number of scenes/beats. Used for hard cost preflight before any provider spend.",
                        },
                        "duration_seconds": {
                            "type": "number",
                            "minimum": 1,
                            "description": "Planned total runtime in seconds. Used for full-auto animation cost preflight.",
                        },
                        "seconds_per_scene": {
                            "type": "number",
                            "minimum": 1,
                            "description": "Fallback per-scene animation duration when total duration is not known.",
                        },
                        "video_model": {
                            "type": "string",
                            "enum": [
                                "ltx_budget", "seedance", "pixverse", "kling_pro",
                                "kling21_standard", "pixverse_v6", "pixverse_c1", "kling21_pro",
                                "veo3_fast", "kling21_master", "grok_imagine_video",
                                "grok_imagine_video_15", "grok_imagine_video_15_1080p",
                            ],
                            "description": "i2v model for motion clips. Use ltx_budget when full animation must be cheaper.",
                        },
                        "selling_price_usd": {"type": "number", "description": "Optional customer price for gross-margin calculation."},
                        "target_margin": {"type": "number", "description": "Target gross margin as decimal; default 0.70."},
                        "max_provider_cost_usd": {"type": "number", "description": "Optional hard provider-cost target for quality routing."},
                        "image_model_id": {
                            "type": "string",
                            "enum": [
                                "grok_imagine", "grok_imagine_standard", "imagen4_fast", "imagen4_preview",
                                "imagen4_ultra", "recraft_v4", "seedream45", "ernie_image", "flux_2_pro",
                                "nano_banana_pro", "recraft_v4_pro", "flux_lora_skeleton",
                            ],
                            "description": "Image model for still generation. Inherit the user's session picker unless they override it in chat.",
                        },
                        "visual_brief": {
                            "type": "string",
                            "description": (
                                "Scene-level creative lock: characters, era, wardrobe, palette, "
                                "composition notes â€” applied every beat."
                            ),
                        },
                        "visual_proof_only": {
                            "type": "boolean",
                            "description": (
                                "Hard safety gate for model/prompt tests. If true, Studio generates exactly one still, "
                                "does not animate, and stops for user approval before any remaining scenes are generated."
                            ),
                        },
                        "product_reference_id": {
                            "type": "string",
                            "description": (
                                "Reference id returned by ingest_product_reference. Studio then uses reference editing "
                                "so the real product remains visually locked in every advertisement scene."
                            ),
                        },
                        "animate": {
                            "type": "boolean",
                            "description": (
                                "Default animate flag for the initial plan. Individual scenes can be toggled later "
                                "with set_production_scenes_animate for precise control (recommended for docs and custom pacing)."
                            ),
                        },
                        "captions_enabled": {
                            "type": "boolean",
                            "description": (
                                "Whether to burn captions into the Short. Default true. If true, captions must be "
                                "word-level unless the user explicitly asks for another mode."
                            ),
                        },
                        "caption_mode": {
                            "type": "string",
                            "enum": ["word", "off"],
                            "description": (
                                "Caption mode for burned captions. Use word for one caption per spoken word in sync. "
                                "Use off only when the user explicitly says no captions."
                            ),
                        },
                        "sfx_enabled": {
                            "type": "boolean",
                            "description": "Generate and mix per-scene sound effects/ambience during finalization. Default false to avoid hidden paid audio spend; enable only when the user explicitly asks for sound design.",
                        },
                        "sound_design_brief": {
                            "type": "string",
                            "description": "Global sound design direction: ambience, hits, risers, whooshes, product sounds, or emotional tone.",
                        },
                        "background_music": {
                            "type": "string",
                            "description": "Background music direction. Use off by default to avoid hidden paid audio spend; set a music direction only when the user explicitly asks.",
                        },
                        "_full_auto": {
                            "type": "boolean",
                            "description": "If true, bypass review gate and auto-finalize (faster but less control). Default false for creative work."
                        },
                        "max_budget_usd": {
                            "type": "number",
                            "description": (
                                "Hard preflight budget cap for this render. Studio refuses to start if the estimated "
                                "provider spend is above this amount."
                            ),
                        },
                    },
                    "required": ["category_key", "topic", "video_model", "render_style"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_production_scenes",
                "description": (
                    "For a shortform production job (after start_shortform_generate), list all scenes with their current still, "
                    "animate flag, duration, status, and preview info. Use this to inspect before editing or selectively animating. "
                    "Essential for giving users full creative control over exactly which scenes get motion and iterating with V4.5 edits."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string", "description": "The shortform job_id returned by start_shortform_generate"},
                    },
                    "required": ["job_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "edit_production_scene_still",
                "description": (
                    "Use Seedream V4.5 *edit* (image-to-image edit) to modify ONE specific scene's still with natural language. "
                    "Example: 'make the background a rainy cyberpunk alley at night, add neon reflections on the wet ground'. "
                    "This is the primary way to get pixel-perfect creative control and iterate a scene until it is exactly right before deciding to animate it. "
                    "Use scope='character' to change only the subject/mannequin/skeleton, scope='background' to preserve the subject and change only the world, "
                    "or scope='props' for held items/screens/objects. The previous clip (if any) is invalidated so you can re-animate after the edit."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "scene_index": {"type": "integer", "description": "0-based index of the scene/beat to edit"},
                        "instruction": {
                            "type": "string",
                            "description": "Natural language description of the desired change. Will be applied via V4.5 edit on the current still."
                        },
                        "scope": {
                            "type": "string",
                            "enum": ["character", "background", "props", "full"],
                            "description": "What to edit while preserving everything else. Use character first, then background for identity-consistent multi-pass scenes.",
                            "default": "full",
                        },
                        "max_budget_usd": {
                            "type": "number",
                            "description": "Hard preflight budget cap for this scene edit.",
                        },
                    },
                    "required": ["job_id", "scene_index", "instruction"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "edit_production_scenes_still",
                "description": (
                    "Use Seedream V4.5 edit to apply the same visual change to MULTIPLE shortform scene stills. "
                    "Use this when the user says every scene/all scenes or gives a global wardrobe/character rule. "
                    "Example: 'put the skeleton in a proper doctor's uniform, black pants, white T-shirt, white tux coat'. "
                    "This edits stills only and does not animate/finalize; the user must review the updated stills first."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "instruction": {
                            "type": "string",
                            "description": "Natural language change to apply to each target scene."
                        },
                        "scene_indices": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "description": "0-based scene indices to edit. Omit or pass empty to edit every scene."
                        },
                        "scope": {
                            "type": "string",
                            "enum": ["character", "background", "props", "full"],
                            "description": "Use character for wardrobe/body/pose edits, background for location-only edits.",
                            "default": "character",
                        },
                        "max_budget_usd": {
                            "type": "number",
                            "description": "Hard preflight budget cap for the full batch.",
                        },
                    },
                    "required": ["job_id", "instruction"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "regenerate_production_scene_still",
                "description": (
                    "Catalyst-audited scene regenerate. Preserves exact channel style while fixing "
                    "artifacting (extra hands, split-screen diptychs). Prefer this when the user clicks "
                    "Regenerate or reports limb/layout artifacts."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "scene_index": {"type": "integer"},
                        "reason": {
                            "type": "string",
                            "description": "Optional user note about why the still is being regenerated (artifact details).",
                        },
                        "max_budget_usd": {
                            "type": "number",
                            "description": "Hard preflight budget cap for this still regeneration.",
                        },
                    },
                    "required": ["job_id", "scene_index"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "set_production_scenes_animate",
                "description": (
                    "Precisely control animation per scene for a shortform job. "
                    "Set animate=true/false on specific scene indices (or all). "
                    "This is how you achieve 'animate exactly 20 minutes out of a 30-minute piece' or 'only animate these three hero scenes'. "
                    "Non-animated scenes will use a tasteful Ken Burns push in the final compose."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "scene_indices": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "description": "List of 0-based scene indices to affect. Omit or pass empty to affect all scenes."
                        },
                        "animate": {"type": "boolean"},
                    },
                    "required": ["job_id", "animate"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "set_production_scene_duration",
                "description": "Override the duration (in seconds) for one or more specific scenes. Useful for pacing control â€” shorter for punchy beats, longer for emotional moments.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "scene_index": {"type": "integer"},
                        "duration_sec": {"type": "number", "description": "Target duration for this scene's clip/hold (e.g. 3.5, 7.0)"},
                    },
                    "required": ["job_id", "scene_index", "duration_sec"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "animate_production_scenes",
                "description": (
                    "Run i2v animation (using the job's video_model) on specific scenes only, or all scenes currently marked animate=true. "
                    "Call this after editing stills with edit_production_scene_still until they are perfect. "
                    "You can iterate: edit still -> animate only that scene -> review -> edit again -> re-animate only that one. "
                    "For visual_proof_only jobs, pass scene_indices=[0] and animate exactly that approved proof scene."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "scene_indices": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "description": "Specific scenes to animate right now. If omitted, animates every scene that has animate=true."
                        },
                        "max_budget_usd": {
                            "type": "number",
                            "description": "Hard preflight budget cap for this animation batch.",
                        },
                    },
                    "required": ["job_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "repair_production_scene_animation",
                "description": (
                    "Re-animate one scene while preserving its approved still. "
                    "Pass the creator's exact natural-language critique in `reason`. "
                    "Use for animation quality notes ('barely moved', 'too static', "
                    "'stronger pose/VFX/background motion') AND for clip artifacts "
                    "(morph/flicker/identity drift). Studio rewrites the i2v performance "
                    "from that feedback, then re-runs animation + identity QA."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "scene_index": {"type": "integer"},
                        "reason": {"type": "string"},
                    },
                    "required": ["job_id", "scene_index"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "audit_and_repair_production_scenes",
                "description": (
                    "Force fresh still, narrative-correspondence, duplicate-adjacent, and sampled-frame animation QA "
                    "on selected short-form scenes, then repair only the scenes that fail. Passing scenes remain "
                    "byte-for-byte untouched. Returns a scene-by-scene account of what passed or was fixed. Use for "
                    "natural requests such as 'check scenes 2-6 for artifacting and fix any you find' or 'make these "
                    "scenes match their narration instead of repeating the same still'."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "scene_indices": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "description": "Zero-based scenes to audit. Required so excluded scenes remain untouched.",
                        },
                        "reason": {"type": "string"},
                        "image_model_id": {
                            "type": "string",
                            "description": "Current Studio image picker override for this repair run.",
                        },
                        "video_model": {
                            "type": "string",
                            "description": "Current Studio i2v picker override for this repair run.",
                        },
                    },
                    "required": ["job_id", "scene_indices"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "optimize_production_margin",
                "description": (
                    "Read-only quality/cost/margin optimizer for short-form or long-form. "
                    "Use in Plan mode; it never starts production or reserves credits."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "format": {"type": "string", "enum": ["shortform", "longform"]},
                        "duration_seconds": {"type": "number"},
                        "scene_count": {"type": "integer"},
                        "image_model_id": {"type": "string"},
                        "video_model": {"type": "string"},
                        "animate": {"type": "boolean"},
                        "selling_price_usd": {"type": "number"},
                        "target_margin": {"type": "number", "default": 0.70},
                        "max_provider_cost_usd": {"type": "number"},
                    },
                    "required": ["format", "duration_seconds"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "finalize_production",
                "description": (
                    "After the stills are perfect and you have set exactly which scenes should be animated (and their durations), "
                    "call this to generate any missing motion, do the final VO, captions, mixing, and produce the deliverable MP4. "
                    "Supports mixed animated + Ken-Burns scenes in one video for perfect pacing control."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "max_budget_usd": {
                            "type": "number",
                            "description": "Hard preflight budget cap for final compose/missing motion.",
                        },
                    },
                    "required": ["job_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "re_edit_production",
                "description": (
                    "THE PREFERRED TOOL for reply-to re-edit requests ('re-edit this video', 'fix the pacing/story/CTA/packaging on the one you just made', "
                    "'make the editing proper on the short you showed me', etc.). "
                    "Takes the *exact same prior production* (job_id + its existing stills/clips/scenes.json/video the user already saw), "
                    "records the re-edit instruction, and re-finalizes a new version with improved editing, pacing, storytelling, instruction-matched captions "
                    "(captions off when requested; otherwise word-level captions by default), "
                    "visual-narration lockstep, and a clear subscribe CTA at the end â€” *without* throwing away the video and regenerating everything from scratch. "
                    "The LLM should usually call list_production_scenes (or list_longform_scenes) + any needed targeted edit_production_scene_still / set_*_duration first, "
                    "then call this. Only creates a full new generation if the user explicitly asks to 'start over' or 'change the entire visual style'."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string", "description": "The exact job_id from the reply_to context or the video card the user is replying to."},
                        "instruction": {"type": "string", "description": "The user's full re-edit request (e.g. 'make the pacing tighter, one word per caption on every scene, strong subscribe CTA at the very end, better story flow on the police station beat')."},
                        "kind": {"type": "string", "description": "shortform or longform (defaults to shortform)."},
                        "max_budget_usd": {
                            "type": "number",
                            "description": "Hard preflight budget cap for the re-edit pass.",
                        },
                    },
                    "required": ["job_id", "instruction"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "run_build_script",
                "description": (
                    "Run an allowlisted long_form build script (approval in confirm mode). "
                    "Example: long_form/build_cryptic_ctr_ss_rook.py --preview"
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "script": {
                            "type": "string",
                            "description": "Path under long_form/ e.g. build_cryptic_ctr_ss_rook.py",
                        },
                        "args": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "CLI args e.g. ['--preview']",
                        },
                    },
                    "required": ["script"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "youtube_oauth_status",
                "description": "Explain Studio YouTube OAuth scopes and how to connect channels in Settings.",
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_youtube_channels",
                "description": "List OAuth-connected YouTube channels with harvest/analytics status.",
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_perpetual_memory",
                "description": (
                    "Read durable Studio Agent memory for this user and optionally a specific YouTube channel. "
                    "Use before channel strategy, packaging, visual defaults, and production planning."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "channel_id": {"type": "string"},
                        "registry_key": {"type": "string"},
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "remember_channel_preference",
                "description": (
                    "Persist a durable user/channel preference, rule, lesson, or strategy note. "
                    "Use when the user says remember/always/never, when analytics reveals a lesson, "
                    "or after production feedback changes the channel playbook."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "note": {"type": "string"},
                        "scope": {"type": "string", "enum": ["global", "channel"], "default": "channel"},
                        "channel_id": {"type": "string"},
                        "registry_key": {"type": "string"},
                        "title": {"type": "string"},
                        "kind": {
                            "type": "string",
                            "description": "preference, rule, visual_style, packaging, pacing, lesson, strategy",
                        },
                        "importance": {"type": "integer", "default": 4},
                    },
                    "required": ["note"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_studio_credits",
                "description": (
                    "Unified credit wallet balance, plan, recent ledger. "
                    "Use before expensive renders; tell user to top up in Studio Wallet when low."
                ),
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_channel_analytics",
                "description": (
                    "Channel intelligence: Catalyst harvest + live YouTube Analytics (90d Reporting API: "
                    "views, CTR, AVD, per-video retention rows when available, Shorts-specific latest-vs-winner comparison, top titles, series arcs) "
                    "and latest upload velocity when OAuth is connected. If video_level_retention_available "
                    "is false, do not infer which specific video had high AVD."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "channel_id": {"type": "string"},
                        "registry_key": {
                            "type": "string",
                            "description": "long_form channel key e.g. cryptic_science",
                        },
                        "focus": {
                            "type": "string",
                            "enum": ["general", "latest_upload"],
                            "description": "Use latest_upload when the user asks about the current/latest posted video or short.",
                        },
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "search_youtube_public",
                "description": (
                    "Search public YouTube for niche demand and reference candidates. Always call this for "
                    "14-30 day / fresh / current demand requests. Uses YouTube Data API publishedAfter via the "
                    "days parameter (7-90) for recent-momentum uploads, plus a separate 365-day order=viewCount "
                    "top-performer pass. Returns hydrated title/channel/views/likes/published_at/support_label. "
                    "Does not return private analytics like AVD or retention."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search phrase, e.g. 'government fugitives documentary YouTube'",
                        },
                        "max_results": {"type": "integer", "default": 8},
                        "days": {
                            "type": "integer",
                            "description": "Recent-momentum publishedAfter window in days (7-90). Use 14 for tight 14-30 day reads, 30 default. Top performers still use a 365-day viewCount pass.",
                            "default": 30,
                        },
                        "order": {
                            "type": "string",
                            "enum": ["relevance", "date", "viewCount"],
                            "description": "Legacy hint; demand research always merges recent + top performers inside the days window.",
                            "default": "date",
                        },
                        "fresh": {
                            "type": "boolean",
                            "description": "Bypass cache and run live date+viewCount search for current/trending demand.",
                            "default": True,
                        },
                    },
                    "required": ["query"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_public_search_trends",
                "description": (
                    "Public YouTube search demand (last 30 days) + predicted topic scores. "
                    "Use registry_key to bias queries to a channel niche. Returned videos include "
                    "hydrated public stats when available; do not call something trending/high-volume "
                    "unless support_label and hydrated stats justify it."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "registry_key": {"type": "string"},
                        "days": {"type": "integer", "default": 30},
                        "fresh": {
                            "type": "boolean",
                            "description": "Bypass public-search cache for current/latest/live trend requests; costs fresh YouTube quota.",
                            "default": False,
                        },
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_fal_pricing",
                "description": (
                    "Fetch live fal.ai Platform API pricing for image/i2v/TTS endpoints. "
                    "Supplemental only — prefer estimate_shortform_render_cost for user-facing short quotes."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "endpoints": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Optional fal endpoint ids e.g. fal-ai/flux-pro/v1.1",
                        },
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "estimate_shortform_render_cost",
                "description": (
                    "Grounded USD estimate for a shortform render using the user's active session "
                    "image_model_id and video_model. REQUIRED before quoting per-short production cost — "
                    "never invent LTX/Seedream pricing from memory."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "duration_seconds": {
                            "type": "number",
                            "description": "Target finished short length in seconds (e.g. 20).",
                        },
                        "scene_count": {
                            "type": "integer",
                            "description": "Number of scene stills to generate. Defaults from duration (~5s/scene).",
                        },
                        "animate": {
                            "type": "boolean",
                            "description": "Include i2v animation cost. Default true for full short estimates.",
                        },
                        "include_finalize": {
                            "type": "boolean",
                            "description": "Include finalize_production narration/SFX allowance. Default true.",
                        },
                        "visual_proof_only": {
                            "type": "boolean",
                            "description": "One-scene proof mode (1 still, no multi-scene spread).",
                        },
                        "image_model_id": {
                            "type": "string",
                            "description": "Optional override. Defaults to session image model picker.",
                        },
                        "video_model": {
                            "type": "string",
                            "description": "Optional override. Defaults to session i2v model picker.",
                        },
                        "selling_price_usd": {"type": "number", "description": "Optional customer price for gross-margin calculation."},
                        "target_margin": {"type": "number", "description": "Target gross margin as decimal; default 0.70."},
                        "max_provider_cost_usd": {"type": "number", "description": "Optional hard provider-cost target for quality routing."},
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "ingest_cliplab_attachment",
                "description": (
                    "Internal/admin ClipLab: ingest the latest uploaded video attachment from this Studio Agent chat "
                    "as a long-form source, transcribe it, and create a ClipLab video_id. Use this first when the "
                    "user uploads a long recording and asks Studio Agent to find/produce clips. This does not use "
                    "Studio short-form render styles because it cuts existing footage."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "attachment_path": {
                            "type": "string",
                            "description": "Optional server-persisted attachment path. Usually omit and use the latest video attachment.",
                        },
                        "channel_id": {"type": "string", "description": "Selected YouTube channel id for Catalyst learning context."},
                        "registry_key": {"type": "string", "description": "Studio channel registry key for Catalyst learning context."},
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "analyze_cliplab_video",
                "description": (
                    "ClipLab: analyze an already-ingested long video/short by ClipLab video_id and return ranked 9:16 clip candidates. "
                    "Use after the user uploads/pulls a source in ClipLab or gives a ClipLab video_id. Logs candidates into Catalyst training data. "
                    "Do not apply generated short-form style presets; this is clip selection from existing footage."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "video_id": {"type": "string", "description": "ClipLab video_id, e.g. vid_... or yt_..."},
                        "prompt": {"type": "string", "description": "What to find: hooks, tension, controversy, emotional peaks, stream highlights, etc."},
                        "max_segments": {"type": "integer", "default": 12},
                        "channel_id": {"type": "string", "description": "Selected YouTube channel id for Catalyst learning context."},
                        "registry_key": {"type": "string", "description": "Studio channel registry key for Catalyst learning context."},
                        "provider": {
                            "type": "string",
                            "enum": ["auto", "local"],
                            "description": "ClipLab analysis provider. Auto currently selects Studio's native, model-agnostic ClipLab pipeline.",
                        },
                    },
                    "required": ["video_id", "prompt"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "render_cliplab_segments",
                "description": (
                    "ClipLab: render selected analyzed segments into 9:16 clips with face-track reframe and captions. "
                    "Requires an analyze_cliplab_video job_id and selected segment indices. Does not use generated-scene "
                    "short-form styles or image-to-video styles."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "video_id": {"type": "string"},
                        "analyze_job_id": {"type": "string"},
                        "segment_indices": {"type": "array", "items": {"type": "integer"}},
                        "burn_captions": {"type": "boolean", "default": True},
                        "channel_id": {"type": "string"},
                        "registry_key": {"type": "string"},
                    },
                    "required": ["video_id", "analyze_job_id", "segment_indices"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "remix_cliplab_short",
                "description": (
                    "ClipLab Remix Lab: polish an already-cut 9:16 short with blurred background, captions, color, and pacing treatment. "
                    "Use when the user uploads an Opus-style clip and wants Studio to make it feel native/viral."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "video_id": {"type": "string"},
                        "style_preset": {"type": "string", "enum": ["clean_viral", "empire", "empire_magnates", "documentary", "streamer", "high_energy"], "default": "clean_viral"},
                        "caption_style": {"type": "string", "enum": ["bold", "minimal", "empire"], "default": "bold"},
                        "edit_intensity": {"type": "string", "enum": ["low", "medium", "high"], "default": "medium"},
                        "background_mode": {"type": "string", "enum": ["blur", "solid"], "default": "blur"},
                        "burn_captions": {"type": "boolean", "default": True},
                        "channel_id": {"type": "string"},
                        "notes": {"type": "string"},
                    },
                    "required": ["video_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "poll_cliplab_job",
                "description": "Poll a ClipLab ingest/analyze/render/remix job and return persisted segments, clips, errors, or remix output.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                    },
                    "required": ["job_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "fetch_archival_for_video",
                "description": (
                    "Get archival B-roll matched to THIS exact video: per-scene queries from "
                    "topic + scene blueprint, fan-out Internet Archive (Prelinger/stock), LOC film, "
                    "NASA video, Wikimedia, NPS, FBI. Resolves direct MP4/download URLs. "
                    "Call after build_scene_blueprint_from_reference or with topic + registry_key. "
                    "Use BEFORE fal generation â€” Lume/Magnates docs are ~90% archival stills+B-roll."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "topic": {"type": "string", "description": "Exact video topic"},
                        "title": {"type": "string"},
                        "registry_key": {"type": "string", "description": "long_form channel e.g. cryptic_science"},
                        "preset": {
                            "type": "string",
                            "enum": ["history", "documentary", "science", "criminal", "nature", "all"],
                        },
                        "blueprint_job_id": {
                            "type": "string",
                            "description": "scene blueprint job_id from analyze_reference_video flow",
                        },
                        "production_job_id": {
                            "type": "string",
                            "description": "Stable id for manifest path (defaults to blueprint_job_id)",
                        },
                        "limit_per_scene": {"type": "integer", "default": 5},
                        "resolve_downloads": {
                            "type": "boolean",
                            "default": True,
                            "description": "Resolve direct file URLs (IA mp4, NASA assets, etc.)",
                        },
                    },
                    "required": ["topic"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "resolve_archival_asset",
                "description": (
                    "Resolve direct download URLs for one archival search hit "
                    "(pass the asset object from fetch_archival_for_video or search_archival_media)."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "source": {"type": "string"},
                        "id": {"type": "string"},
                        "title": {"type": "string"},
                        "page_url": {"type": "string"},
                        "download_url": {"type": "string"},
                        "media_type": {"type": "string"},
                    },
                    "required": ["source"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "search_archival_media",
                "description": (
                    "Quick single-query archival search. For a full video shot list use "
                    "fetch_archival_for_video instead (per-scene, direct downloads)."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "preset": {
                            "type": "string",
                            "enum": ["history", "documentary", "science", "criminal", "nature", "all"],
                            "description": "Curated source set. Omit to use 'documentary'.",
                        },
                        "sources": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "enum": ["internet_archive", "nasa", "loc", "wikimedia", "nps", "fbi"],
                            },
                            "description": "Explicit sources (overrides preset).",
                        },
                        "limit_per_source": {"type": "integer", "default": 8},
                    },
                    "required": ["query"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "analyze_reference_video",
                "description": (
                    "Analyze a reference video from YouTube (yt-dlp) or from an uploaded Studio Agent attachment "
                    "(local_path). Extracts metadata, scene keyframes, cut timeline pacing, and audio for transcription. "
                    "Poll poll_render_job(kind=competitor), then build_scene_blueprint_from_reference."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "url": {"type": "string", "description": "YouTube video URL"},
                        "local_path": {
                            "type": "string",
                            "description": "Server path to an uploaded reference video from this chat attachment.",
                        },
                        "source_name": {"type": "string", "description": "Display name for uploaded reference files."},
                        "scene_threshold": {"type": "number", "default": 0.3},
                        "max_frames": {"type": "integer", "default": 40},
                        "content_format": {
                            "type": "string",
                            "enum": ["short", "long"],
                            "description": "Analyze with Shorts metrics or long-form documentary metrics.",
                        },
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "analyze_competitor_video",
                "description": "Alias of analyze_reference_video (competitor/outlier study).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "url": {"type": "string", "description": "YouTube video URL"},
                        "scene_threshold": {
                            "type": "number",
                            "default": 0.3,
                            "description": "Scene-cut sensitivity (0.2 = more frames, 0.4 = fewer).",
                        },
                        "max_frames": {"type": "integer", "default": 32},
                        "content_format": {
                            "type": "string",
                            "enum": ["short", "long"],
                            "description": "Analyze with Shorts metrics or long-form documentary metrics.",
                        },
                    },
                    "required": ["url"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "retry_reference_analysis",
                "description": (
                    "Re-run failed reference-analysis stages (transcript, vision, storytelling) on an existing "
                    "competitor job_id without re-uploading. Use when transcript/vision/story failed but keyframes "
                    "or pacing already exist. Poll poll_render_job(kind=competitor) is not required — this returns "
                    "the refreshed analysis payload immediately."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {
                            "type": "string",
                            "description": "competitor analysis job_id from analyze_reference_video",
                        },
                        "stages": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "enum": ["transcript", "vision", "storytelling", "audio"],
                            },
                            "description": "Stages to retry; defaults to all failed/missing stages.",
                        },
                    },
                    "required": ["job_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "build_scene_blueprint_from_reference",
                "description": (
                    "After analyze_reference_video completes: map keyframes + pacing into per-scene "
                    "rows (1â€“5 characters), Seedream v4.5 edit fields, i2v duration, BGM cues, audio mix."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "topic": {"type": "string"},
                        "channel_style": {
                            "type": "string",
                            "enum": ["premium_doc", "viral_short", "story_manhwa"],
                            "default": "premium_doc",
                        },
                        "characters_per_scene": {
                            "type": "integer",
                            "default": 1,
                            "description": "1 for skeleton host; up to 5 for ensemble/cast channels.",
                        },
                        "visual_brief": {"type": "string"},
                        "target_scene_count": {"type": "integer"},
                    },
                    "required": ["job_id", "topic"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "recommend_video_topics",
                "description": (
                    "For creators who don't know what to film: merge channel analytics (if connected), "
                    "growth playbook, and public search trends into ranked topic + niche recommendations. "
                    "Does not imply Skeleton AI â€” recommend format-appropriate pipelines (short script, long-form, "
                    "reference blueprint, or skeleton only if user wants that visual)."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "registry_key": {"type": "string"},
                        "channel_id": {"type": "string"},
                        "niche_query": {"type": "string"},
                        "days": {"type": "integer", "default": 30},
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "search_music",
                "description": "Search free Creative Commons music (Jamendo) for background tracks. Returns direct audio download URLs.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "instrumental": {"type": "boolean", "default": False},
                        "limit": {"type": "integer", "default": 12},
                    },
                    "required": ["query"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "search_sfx",
                "description": "Search free sound effects (Freesound, CC0 by default for attribution-free commercial use).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "cc0_only": {"type": "boolean", "default": True},
                        "limit": {"type": "integer", "default": 12},
                    },
                    "required": ["query"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "finalize_longform_render",
                "description": (
                    "After stills gate (phase awaiting_approval): run voice, SFX, thumbnails, "
                    "and MP4 composite. Returns job_id to poll via Studio production monitor."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "max_budget_usd": {
                            "type": "number",
                            "description": "Hard preflight budget cap for final long-form render.",
                        },
                    },
                    "required": ["job_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "refresh_channel_intelligence",
                "description": (
                    "Re-sync a connected YouTube channel into Catalyst harvest (analytics, "
                    "packaging/retention learnings). Run after new uploads or when recommendations feel stale."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "channel_id": {"type": "string", "description": "YouTube channel ID"},
                    },
                    "required": ["channel_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "record_production_feedback",
                "description": (
                    "Log what worked or failed on a published video for NYPTID model improvement. "
                    "Internal training signal only â€” never sold to advertisers."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "channel_id": {"type": "string"},
                        "video_id": {"type": "string"},
                        "outcome": {
                            "type": "string",
                            "description": "e.g. breakout, underperformed, strong_retention, weak_packaging",
                        },
                        "notes": {"type": "string"},
                        "views": {"type": "integer"},
                        "ctr_percent": {"type": "number"},
                    },
                    "required": ["channel_id", "outcome"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "poll_render_job",
                "description": (
                    "Poll job status by job_id and kind. Use kind='competitor' for "
                    "analyze_competitor_video to surface live progress stages."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "kind": {"type": "string", "enum": ["longform", "shortform", "competitor", "cliplab"]},
                    },
                    "required": ["job_id", "kind"],
                },
            },
        },
    ]


def _safe_path(relative: str) -> Path:
    rel = relative.replace("\\", "/").lstrip("/")
    if ".." in rel.split("/"):
        raise ValueError("path traversal not allowed")
    full = (ROOT / rel).resolve()
    if not str(full).startswith(str(ROOT.resolve())):
        raise ValueError("path outside workspace")
    return full


def _allow_write(path: Path) -> None:
    rel = path.relative_to(ROOT.resolve())
    parts = rel.parts
    allowed_roots = ("studio", "long_form", "recaps")
    if not parts or parts[0] not in allowed_roots:
        raise ValueError(f"writes only allowed under studio/, long_form/, recaps/ â€” got {rel}")


ALLOWED_BUILD_SCRIPTS = frozenset({
    "build_cryptic_ctr_ss_rook.py",
    "build_cryptic_google_ai_mode_rook.py",
    "build_cryptic_google_ai_mode.py",
})

_PLACEHOLDER_LONGFORM_TOPICS = frozenset({
    "untitled",
    "long-form concept",
    "longform concept",
    "history rewind long-form concept",
})


def _is_placeholder_longform_topic(value: str) -> bool:
    low = str(value or "").strip().lower()
    if not low:
        return True
    if low in _PLACEHOLDER_LONGFORM_TOPICS:
        return True
    return "long-form concept" in low or "longform concept" in low


def _resolve_longform_title_topic(args: dict[str, Any], *, session_id: str | None = None) -> tuple[str, str]:
    title = str(args.get("title") or "Untitled").strip()
    topic = str(args.get("topic") or title).strip()
    if session_id:
        try:
            sess = store.get_session(session_id) or {}
            locked = store.get_locked_working_title(sess)
            if locked and not _is_placeholder_longform_topic(locked):
                return locked[:120], locked[:120]
            review = sess.get("thumbnail_review") if isinstance(sess.get("thumbnail_review"), dict) else {}
            review_title = str(review.get("title") or "").strip()
            if review_title and not _is_placeholder_longform_topic(review_title):
                return review_title[:120], review_title[:120]
            concept = sess.get("pending_concept") if isinstance(sess.get("pending_concept"), dict) else {}
            concept_title = str(concept.get("title") or "").strip()
            if concept_title and not _is_placeholder_longform_topic(concept_title):
                return concept_title[:120], concept_title[:120]
        except Exception:
            pass
    if _is_placeholder_longform_topic(topic) and title and not _is_placeholder_longform_topic(title):
        topic = title
    elif _is_placeholder_longform_topic(title) and topic and not _is_placeholder_longform_topic(topic):
        title = topic
    return title[:120], topic[:120]


def _build_outline_from_args(args: dict[str, Any]) -> dict[str, Any]:
    raw = str(args.get("chapters_json") or "").strip()
    if raw:
        outline = json.loads(raw)
        if isinstance(outline, dict) and outline.get("chapters"):
            return outline
    title, topic = _resolve_longform_title_topic(args)
    target_duration_sec = max(60, int(args.get("target_duration_sec") or 1200))
    chapter_span = 1800 if target_duration_sec >= 3600 else 600
    chapter_count = max(1, min(36, round(target_duration_sec / chapter_span)))
    chapters = []
    for index in range(chapter_count):
        part = index + 1
        chapters.append({
            "title": topic if chapter_count == 1 else f"{topic} — Part {part}",
            "minutes": max(1, round(target_duration_sec / chapter_count / 60)),
            "synopsis": (
                f"Chronological part {part} of {chapter_count}. Follow the conflict arc: unresolved promise, "
                "rising action, decisive conflict, comeback or reversal, then a final payoff that explains "
                f"the lasting consequence of {topic}."
            ),
            "beats": [
                {"text": f"Hook: the unresolved question in this stage of {topic}", "visual": f"Period-accurate cinematic opening for {topic}, chronological part {part}"},
                {"text": f"Rising action: actors, setting, pressure, and stakes in part {part}", "visual": f"Period-accurate historical tableau showing rising stakes in {topic}"},
                {"text": f"Conflict: the decisive collision or constraint in part {part}", "visual": f"Historically grounded conflict moment in {topic}, no modern elements"},
                {"text": f"Comeback or reversal in part {part}", "visual": f"Historically grounded reversal or consequence in {topic}"},
                {"text": f"Final rising action and payoff for part {part}", "visual": f"Quiet cinematic resolution and lasting consequence of {topic}"},
            ],
        })
    return {
        "title": title,
        "topic": topic,
        "target_duration_sec": target_duration_sec,
        "conflict_arc_required": True,
        "hook": f"What central conflict changed {topic}, and why did its consequences outlive the people involved?",
        "description": f"A calm, chronological documentary exploring {topic}, its central conflicts, reversals, and lasting consequences.",
        "tags": [topic, "history", "history documentary", "history for sleep", "sleep documentary"],
        "chapters": chapters,
    }
    return {
        "title": title,
        "chapters": [
            {
                "title": topic,
                "beats": [
                    {"text": f"Intro: {topic}", "visual": f"Cinematic opening â€” {topic}"},
                    {"text": f"Core story: {topic}", "visual": f"Documentary still â€” {topic}"},
                    {"text": f"Conclusion: {topic}", "visual": f"Closing frame â€” {topic}"},
                ],
            }
        ],
    }


def _debit_fal_for_outline(user_id: str, outline: dict[str, Any], *, job_id: str, kind: str) -> dict[str, Any]:
    """Charge the unified wallet for fal spend using the outline's scene plan.

    Estimate-at-start using live fal pricing (reconciled by the pipeline later).
    One image + ~5s i2v clip per beat, plus narration TTS chars.
    """
    try:
        import unified_credits as uc

        beats = 0
        tts_chars = 0
        for ch in outline.get("chapters") or []:
            for beat in ch.get("beats") or []:
                beats += 1
                tts_chars += len(str(beat.get("text") or ""))
        if beats <= 0:
            return {"credits_charged": 0, "note": "no beats to price"}
        credits, balance = uc.debit_fal_render(
            user_id,
            images=beats,
            video_seconds=beats * 5.0,
            tts_chars=tts_chars,
            reason=f"studio_agent_{kind}_estimate",
            metadata={"job_id": job_id, "beats": beats},
        )
        return {"credits_charged": credits, "balance_after": balance, "estimate": True, "beats": beats}
    except Exception as exc:
        return {"credits_charged": 0, "error": str(exc)[:200]}


def _session_render_style(session_id: str | None) -> str | None:
    if not session_id:
        return None
    from studio_agent import store

    session = store.get_session(session_id)
    if not session:
        return None
    style = str(session.get("render_style") or "").strip()
    return style or None


def _session_channel_brand(session_id: str | None) -> str:
    fallback_by_registry = {
        "zerotier": "ZeroTier",
        "empire_magnates": "Empire Magnates",
        "cryptic_science": "CrypticScience",
        "history_rewind": "History Rewind",
        "nyptid_clips": "NYPTID Clips",
        "mrskelewelly": "MrSkelewelly",
        "mr_skelewelly": "MrSkelewelly",
        "skeleton_ai": "Skeleton AI",
    }
    if not session_id:
        return "Studio"
    from studio_agent import store

    session = store.get_session(session_id) or {}
    for key in ("channel_title", "channel_name", "brand_name"):
        value = str(session.get(key) or "").strip()
        if value:
            return value[:48]
    registry = str(session.get("registry_key") or "").strip()
    if registry:
        return fallback_by_registry.get(registry, registry.replace("_", " ").title())[:48]
    handle = str(session.get("channel_handle") or "").strip().lstrip("@")
    if handle:
        return handle[:48]
    return "Studio"


def _session_channel_context(session_id: str | None) -> dict[str, str]:
    if not session_id:
        return {"channel_id": "", "registry_key": "", "channel_title": ""}
    from studio_agent import store

    session = store.get_session(session_id) or {}
    return {
        "channel_id": str(session.get("channel_id") or "").strip(),
        "registry_key": str(session.get("registry_key") or "").strip(),
        "channel_title": str(session.get("channel_title") or session.get("channel_name") or "").strip(),
    }


def _session_production_models(session_id: str | None) -> dict[str, Any]:
    """Resolve image/i2v models from session picker with skeleton pipeline fallbacks."""
    from studio_agent.render_styles import is_skeleton_style, resolve_render_style

    session = store.get_session(session_id) or {} if session_id else {}
    style = resolve_render_style(
        str(session.get("render_style") or "").strip() or None,
        session_style=session.get("render_style"),
    )
    skeleton = is_skeleton_style(style)
    image_model = store.normalize_image_model(session.get("image_model"))
    video_model = store.normalize_video_model(session.get("video_model"))
    return {
        "image_model_id": image_model,
        "video_model": video_model,
        "render_style": str(style.key if hasattr(style, "key") else session.get("render_style") or ""),
        "skeleton_pipeline": skeleton,
    }


def _repair_route_snapshot(
    session_id: str | None,
    *,
    image_model_id: str | None = None,
    video_model: str | None = None,
    route_revision: int | None = None,
) -> dict[str, Any]:
    """Read the binding media route immediately before a provider dispatch."""

    session: dict[str, Any] = {}
    if session_id:
        session = store.get_session(
            session_id,
            reconcile_jobs=False,
            _prune_active_jobs=False,
        ) or {}
    selected_image = (
        store.normalize_image_model(session.get("image_model"))
        if session
        else (store.normalize_image_model(image_model_id) if image_model_id else "")
    )
    selected_video = (
        store.normalize_video_model(session.get("video_model"))
        if session
        else (store.normalize_video_model(video_model) if video_model else "")
    )
    try:
        revision = int(
            session.get("media_route_revision")
            if session
            else route_revision or 1
        )
    except (TypeError, ValueError):
        revision = 1
    return {
        "session_id": str(session_id or ""),
        "revision": max(1, revision),
        "image_model_id": selected_image,
        "video_model": selected_video,
    }


def _same_media_route(left: dict[str, Any], right: dict[str, Any], *, stage: str) -> bool:
    key = "image_model_id" if stage == "image" else "video_model"
    return (
        int(left.get("revision") or 1) == int(right.get("revision") or 1)
        and str(left.get(key) or "") == str(right.get(key) or "")
    )


def _is_studio_admin_user(user_id: str) -> bool:
    uid = str(user_id or "").strip()
    if uid and uid in CLIPLAB_AGENT_ADMIN_USER_IDS:
        return True
    if uid:
        try:
            import unified_credits as uc

            state = uc.get_state(uid) or {}
            plan = str(state.get("plan") or "").strip().lower()
            if bool(state.get("unlimited")) or plan in {"owner", "admin"}:
                return True
        except Exception:
            pass
    return False


def _require_cliplab_admin(user_id: str) -> None:
    if not _is_studio_admin_user(user_id):
        raise PermissionError("ClipLab Agent tools are internal/admin-only right now.")


def _require_longform_entitlement(user_id: str) -> None:
    """Allow owners and active Studio subscribers into the long-form lane.

    Long-form is still credit-gated by the normal production reservation path;
    this check only replaces the obsolete owner-only beta switch.
    """
    uid = str(user_id or "").strip()
    if _is_studio_admin_user(uid):
        return
    if not uid:
        raise PermissionError("Sign in to use long-form production.")
    from studio_agent.access import STUDIO_AGENT_PLANS, unified_plan

    if unified_plan(uid) not in STUDIO_AGENT_PLANS:
        raise PermissionError("An active Studio plan is required for long-form production.")


def tools_for_user(user_id: str | None) -> list[dict[str, Any]]:
    """Hide owner-only + runner-only production tools from the LLM tool list.

    Runner-only tools remain executable via execute_tool_logged from runner paths.
    """
    schemas = tool_schemas()
    blocked = set(RUNNER_ONLY_AGENT_TOOLS)
    if not _is_studio_admin_user(str(user_id or "")):
        blocked |= set(OWNER_ONLY_AGENT_TOOLS)
    return [
        row for row in schemas
        if str(row.get("function", {}).get("name") or "") not in blocked
    ]


def _latest_video_attachment_path(session_id: str | None, user_id: str, *, hint: str = "") -> str:
    from studio_agent.attachments import resolve_video_attachment_path

    return resolve_video_attachment_path(session_id, user_id, hint=hint)


def _shortform_credit_reservation_path(workspace: Path) -> Path:
    return Path(workspace) / _CREDIT_RESERVATION_FILE


def _write_shortform_credit_reservation(
    workspace: Path,
    *,
    reservation: dict[str, Any] | None,
    user_id: str | None,
    tool: str,
    session_id: str | None,
    budget: dict[str, Any] | None = None,
) -> None:
    if not reservation:
        return
    payload = {
        "reservation": reservation,
        "user_id": str(user_id or ""),
        "tool": str(tool or ""),
        "session_id": str(session_id or ""),
        "budget": budget or {},
        "created_at": time.time(),
    }
    try:
        Path(workspace).mkdir(parents=True, exist_ok=True)
        _shortform_credit_reservation_path(workspace).write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass


def _load_shortform_credit_reservation(workspace: Path) -> dict[str, Any] | None:
    path = _shortform_credit_reservation_path(workspace)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _clear_shortform_credit_reservation(workspace: Path) -> None:
    try:
        _shortform_credit_reservation_path(workspace).unlink(missing_ok=True)
    except Exception:
        pass


def _credits_for_pending_usd(pending_usd: Any) -> int:
    import unified_credits as uc

    return max(0, int(uc.usd_to_credits(pending_usd)))


def _reconcile_shortform_costs(
    user_id: str | None,
    job_id: str,
    *,
    reservation_payload: dict[str, Any] | None = None,
    reason: str,
    tool: str = "",
    session_id: str | None = None,
) -> dict[str, Any]:
    """Bill only newly observed provider spend for a shortform workspace."""
    jid = str(job_id or "").strip()
    workspace = (ROOT / SKELETON_OUTPUT / jid).resolve()
    loaded_payload = reservation_payload or _load_shortform_credit_reservation(workspace)
    reservation = (
        loaded_payload.get("reservation")
        if isinstance(loaded_payload, dict) and isinstance(loaded_payload.get("reservation"), dict)
        else None
    )
    uid = str(user_id or "").strip() or str((loaded_payload or {}).get("user_id") or "").strip()
    pending = production_costs.pending_billable_usd(workspace)
    summary = production_costs.load_summary(workspace)
    if pending <= 0:
        if reservation:
            _release_shortform_reservation(
                uid,
                {"reservation": reservation, "user_id": uid},
                reason=f"{reason}:no_new_spend",
            )
            _clear_shortform_credit_reservation(workspace)
        return {
            "charged": 0,
            "charged_usd": 0.0,
            "charged_usd_decimal": "0.000000",
            "actual_usd": summary.get("total_usd", 0.0),
            "actual_usd_decimal": summary.get("total_usd_decimal", "0.000000"),
            "event_count": summary.get("event_count", 0),
        }

    import unified_credits as uc

    actual_credits = _credits_for_pending_usd(pending)
    rid = str((reservation or {}).get("reservation_id") or "").strip()
    held_credits = int((reservation or {}).get("credits", 0) or 0)
    balance_after = uc.get_balance(uid) if uid else 0
    charged_credits = actual_credits
    refunded_credits = 0
    overage_credits = 0
    if uid and rid and not bool((reservation or {}).get("unlimited")):
        state = uc.commit_reservation(
            uid,
            rid,
            actual_credits=actual_credits,
            reason=reason,
            metadata={
                "tool": tool or str((loaded_payload or {}).get("tool") or ""),
                "session_id": session_id or str((loaded_payload or {}).get("session_id") or ""),
                "job_id": jid,
                "provider_usd_decimal": str(pending),
                "actual_credits": actual_credits,
            },
        )
        balance_after = int(state.get("balance", 0) or 0)
        charged_credits = min(actual_credits, held_credits)
        refunded_credits = max(0, held_credits - charged_credits)
        overage_credits = max(0, actual_credits - held_credits)
        if overage_credits:
            ok, balance_after = uc.debit_credits(
                uid,
                overage_credits,
                reason=f"{reason}:overage",
                metadata={
                    "tool": tool,
                    "session_id": session_id,
                    "job_id": jid,
                    "provider_usd_decimal": str(pending),
                    "reservation_id": rid,
                },
                allow_negative=True,
            )
            if ok:
                charged_credits += overage_credits
    elif uid:
        charged_credits, balance_after = uc.debit_usd(
            uid,
            pending,
            reason=reason,
            metadata={"tool": tool, "session_id": session_id, "job_id": jid},
            allow_negative=True,
        )
    production_costs.mark_billed(
        workspace,
        usd=pending,
        credits=charged_credits,
        user_id=uid,
        reservation_id=rid,
        reason=reason,
        metadata={
            "tool": tool,
            "session_id": session_id,
            "job_id": jid,
            "actual_credits": actual_credits,
            "held_credits": held_credits,
            "refunded_credits": refunded_credits,
            "overage_credits": overage_credits,
        },
    )
    _clear_shortform_credit_reservation(workspace)
    return {
        "charged": charged_credits,
        "actual_credits": actual_credits,
        "repair_reserve_refunded": refunded_credits,
        "overage_credits": overage_credits,
        "balance_after": balance_after,
        "charged_usd": float(pending),
        "charged_usd_decimal": str(pending),
        "actual_usd": summary.get("total_usd", 0.0),
        "actual_usd_decimal": summary.get("total_usd_decimal", "0.000000"),
        "event_count": summary.get("event_count", 0),
    }


def _release_shortform_reservation(
    user_id: str | None,
    reservation_payload: dict[str, Any] | None,
    *,
    reason: str,
) -> None:
    reservation = (
        reservation_payload.get("reservation")
        if isinstance(reservation_payload, dict) and isinstance(reservation_payload.get("reservation"), dict)
        else reservation_payload
        if isinstance(reservation_payload, dict) and reservation_payload.get("reservation_id")
        else None
    )
    uid = str(user_id or "").strip() or str((reservation_payload or {}).get("user_id") or "").strip()
    rid = str((reservation or {}).get("reservation_id") or "").strip()
    if not uid or not rid or bool((reservation or {}).get("unlimited")):
        return
    try:
        import unified_credits as uc

        uc.release_reservation(uid, rid, reason=reason)
    except Exception:
        pass


def _resolve_shortform_voice(*, render_style: str, registry_key: str = "") -> tuple[str, str]:
    """Return (voice_id, voice_provider) for shortform narration."""
    from skeleton_ai.voice_fal import resolve_voice_id
    from studio_agent.render_styles import is_skeleton_style, resolve_render_style

    style = resolve_render_style(str(render_style or "").strip() or None)
    reg = str(registry_key or "").strip().lower()
    skeleton = is_skeleton_style(style) or reg in {"mrskelewelly", "mr_skelewelly"}
    return resolve_voice_id(skeleton=skeleton), "fal"


def _migrate_shortform_voice_options(opts: dict[str, Any], *, render_style: str = "cinematic") -> dict[str, Any]:
    """Rewrite legacy ElevenLabs/xAI narration settings to fal MiniMax for active jobs."""
    merged = dict(opts or {})
    provider = str(merged.get("voice_provider") or "").strip().lower()
    if provider in {"", "elevenlabs", "xai", "grok", "fal", "minimax"}:
        voice_id, voice_provider = _resolve_shortform_voice(render_style=render_style)
        merged["voice_id"] = voice_id
        merged["voice_provider"] = voice_provider
    return merged


def _spawn_shortform_job(
    *,
    category_key: str,
    topic: str | None,
    script: str | None,
    scene_count: int | None = None,
    tier: str = "standard",
    image_model_id: str | None = None,
    video_model: str | None = None,
    visual_brief: str | None = None,
    render_style: str,
    user_id: str | None = None,
    animate: bool = True,
    watermark_text: str = "Studio",
    captions_enabled: bool = True,
    caption_mode: str = "word",
    sfx_enabled: bool = False,
    sound_design_brief: str = "",
    background_music: str = "off",
    resume_job_id: str | None = None,
    requested_job_id: str | None = None,
    reference_images: list[str] | None = None,
    product_reference: dict[str, Any] | None = None,
    credit_reservation: dict[str, Any] | None = None,
    credit_session_id: str | None = None,
    credit_budget: dict[str, Any] | None = None,
    visual_proof_only: bool = False,
    studio_promotion_mode: str = "subtle",
) -> str:
    # Resume: reuse the prior job's workspace so finished stills/clips/VO are
    # not re-rendered (and not re-billed). Falls back to a fresh job otherwise.
    resume_id = str(resume_job_id or "").strip()
    requested_id = str(requested_job_id or "").strip()
    requested_id_valid = bool(
        requested_id
        and len(requested_id) <= 48
        and requested_id.replace("_", "").isalnum()
    )
    if resume_id and resume_id.replace("_", "").isalnum() and (ROOT / SKELETON_OUTPUT / resume_id).is_dir():
        job_id = resume_id
    elif requested_id_valid:
        job_id = requested_id
    else:
        job_id = uuid.uuid4().hex[:12]
    workspace = (ROOT / SKELETON_OUTPUT / job_id).resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    # A fresh/resumed run must not be pre-cancelled by stale terminal markers.
    # Re-edit/retry may intentionally reuse the same workspace; if an old
    # result.json says "cancelled" the UI will show a false failure even while
    # the new worker is producing a valid MP4.
    try:
        (workspace / "CANCELLED").unlink(missing_ok=True)
    except OSError:
        pass
    try:
        prior_result = workspace / "result.json"
        if prior_result.exists():
            prior = json.loads(prior_result.read_text(encoding="utf-8"))
            prior_status = str(prior.get("status") or "").lower()
            if prior_status in {"cancelled", "failed"}:
                prior_result.unlink(missing_ok=True)
    except Exception:
        pass
    if visual_proof_only:
        scene_count = 1
        animate = False

    voice_id, voice_provider = _resolve_shortform_voice(render_style=render_style)

    spec = {
        "job_id": job_id,
        "category_key": category_key,
        "topic": topic,
        "script": script,
        "scene_count": scene_count,
        "visual_proof_only": bool(visual_proof_only),
        "staged_shortform_workflow": True,
        "tier": tier,
        "image_model_id": image_model_id,
        "video_model": video_model,
        "visual_brief": visual_brief,
        "render_style": render_style,
        "voice_id": voice_id or None,
        "voice_provider": voice_provider or None,
        "animate": animate,
        "watermark_text": watermark_text,
        "captions_enabled": captions_enabled,
        "caption_mode": caption_mode,
        "sfx_enabled": bool(sfx_enabled),
        "sound_design_brief": sound_design_brief,
        "background_music": background_music,
        "user_id": user_id,
        "reference_images": list(reference_images or []),
        "skeleton_reference_image": str((reference_images or [""])[0] or "").strip(),
        "product_reference": product_reference or None,
        "studio_promotion_mode": studio_promotion_mode,
        "started_at": time.time(),
    }
    (workspace / "job_spec.json").write_text(
        json.dumps(spec, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    _write_shortform_credit_reservation(
        workspace,
        reservation=credit_reservation,
        user_id=user_id,
        tool="start_shortform_generate",
        session_id=credit_session_id,
        budget=credit_budget,
    )

    # Early marker so status can see "we accepted the work" even before first progress write.
    try:
        (workspace / "started.json").write_text(json.dumps({"started_at": time.time(), "render_style": render_style}, indent=2), encoding="utf-8")
    except Exception:
        pass

    # Seed an initial progress.json so the job doesn't look completely dead in the first 1-2 minutes
    # while the thread imports, allocates GrokClient, hits first LLM, etc.
    try:
        init_prog = {"stage": "queued", "progress": 5, "detail": "Job accepted â€” starting script + visuals worker."}
        (workspace / "progress.json").write_text(json.dumps(init_prog, indent=2), encoding="utf-8")
    except Exception:
        pass

    def _work() -> None:
        from studio_agent.render_styles import is_skeleton_style
        import traceback as _tb

        hb_path = workspace / "heartbeat.txt"
        stop_hb = threading.Event()
        hb_thread = threading.Thread(target=_heartbeat_loop, args=(stop_hb, hb_path), daemon=True, name=f"hb-{job_id}")
        hb_thread.start()

        try:
            # Touch heartbeat immediately so even the first long script/plan call is covered.
            hb_path.touch(exist_ok=True)

            # Studio Agent must not burn image-to-video before still approval.
            # First pass always stops at the scene-review gate. The user/agent can
            # edit stills cheaply, then explicitly approve scenes and run
            # animate_production_scenes/finalize_production.
            from skeleton_ai.styled_pipeline import plan_scenes

            def _on_slot_wait(admission: Any) -> None:
                try:
                    (workspace / "progress.json").write_text(
                        json.dumps(
                            {
                                "stage": "render_queue",
                                "progress": 5,
                                "detail": (
                                    f"Waiting for render slot #{admission.queue_position} "
                                    f"({admission.active}/{admission.limit} active)."
                                ),
                            },
                            indent=2,
                        ),
                        encoding="utf-8",
                    )
                except Exception:
                    pass

            with production_slot("render", on_wait=_on_slot_wait):
                plan_scenes(
                    category_key=category_key,
                    topic=topic,
                    workspace=workspace,
                    render_style=render_style,
                    tier=tier,
                    image_model_id=image_model_id,
                    video_model=video_model,
                    visual_brief=visual_brief,
                    beats_target=1 if visual_proof_only else int(scene_count) if scene_count else 12,
                    script_override=script,
                    user_id=user_id,
                    default_animate=False,
                    reference_images=list(reference_images or []),
                    sound_design_brief=sound_design_brief,
                )
            _reconcile_shortform_costs(
                user_id,
                job_id,
                reservation_payload=_load_shortform_credit_reservation(workspace),
                reason="studio_shortform_stills_actual",
                tool="start_shortform_generate",
                session_id=credit_session_id,
            )
        except Exception as exc:
            from skeleton_ai.pipeline import RenderCancelled
            if isinstance(exc, RenderCancelled):
                payload = {"status": "cancelled", "job_id": job_id, "error": "Cancelled by user"}
            else:
                payload = {"status": "failed", "job_id": job_id, "error": str(exc)}
                # Write full traceback for post-mortems and training signal.
                try:
                    (workspace / "job.log").write_text(
                        f"FAILED at {time.time()}\n{str(exc)}\n\n{_tb.format_exc()}",
                        encoding="utf-8",
                    )
                except Exception:
                    pass
            try:
                (workspace / "result.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
            except Exception:
                pass
            try:
                pending = production_costs.pending_billable_usd(workspace)
                reservation_payload = _load_shortform_credit_reservation(workspace)
                if pending > 0:
                    _reconcile_shortform_costs(
                        user_id,
                        job_id,
                        reservation_payload=reservation_payload,
                        reason="studio_shortform_failed_actual",
                        tool="start_shortform_generate",
                        session_id=credit_session_id,
                    )
                else:
                    _release_shortform_reservation(
                        user_id,
                        reservation_payload,
                        reason="studio_shortform_failed_no_spend",
                    )
                    _clear_shortform_credit_reservation(workspace)
            except Exception:
                pass
        finally:
            stop_hb.set()
            # Final heartbeat touch so the "done" state is visible quickly.
            try:
                hb_path.touch(exist_ok=True)
            except Exception:
                pass

    # Non-daemon so the thread has a chance to finish / write result on clean shutdown.
    # Still vulnerable to hard process kills (Fly deploy, OOM, health restart) â€” the heartbeat + resume
    # on retry + on-boot re-claim mitigate.
    t = threading.Thread(target=_work, daemon=False, name=f"sf-{job_id}")
    t.start()
    return job_id


def _heartbeat_loop(stop_event, hb_path: Path, interval: float = 20.0) -> None:
    """Module-level sidecar heartbeat writer. Used by both fresh spawns and the orphan reclaimer."""
    while not stop_event.wait(interval):
        try:
            hb_path.touch(exist_ok=True)
        except Exception:
            pass


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Granular per-scene creative control helpers (full creative control for Agent)
# These give the LLM the power to: list scenes, edit any still with natural-language
# V4.5 edit (exactly as the user described), toggle animate on arbitrary subsets of
# scenes, set per-scene duration for pacing, selectively run i2v only on the chosen
# ones, and finally compose the mixed video. This is the "30-minute documentary,
# animate exactly 20 minutes, re-iterate scene 7 with V4.5 edit until perfect, then
# animate only the three hero scenes" workflow.
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _shortform_workspace(job_id: str) -> Path:
    jid = str(job_id or "").strip()
    if not jid or not jid.replace("_", "").isalnum() or len(jid) > 48:
        raise ValueError("bad job_id")
    return (ROOT / SKELETON_OUTPUT / jid).resolve()


def list_production_scenes(job_id: str) -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import load_scenes
    scenes = load_scenes(ws)
    result_path = ws / "result.json"
    result = {}
    if result_path.exists():
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    out = []
    for sc in scenes:
        out.append({
            "index": sc.get("index"),
            "sid": sc.get("sid"),
            "narration": sc.get("narration"),
            "animate": bool(sc.get("animate", False)),
            "approved_for_video": bool(sc.get("approved_for_video", False)),
            "approved_for_animation": bool(sc.get("approved_for_animation", False)),
            "duration_sec": float(sc.get("duration_sec", 5.0)),
            "video_model": sc.get("video_model"),
            "status": sc.get("status"),
            "still_preview_url": f"/api/studio-agent/jobs/{job_id}/still/{sc.get('index')}",
            "has_clip": bool(sc.get("clip_rel")),
            "last_edit": sc.get("last_edit"),
            "prompt": sc.get("prompt"),
            "motion_prompt": sc.get("motion_prompt"),
        })
    return json.dumps({
        "job_id": job_id,
        "render_style": result.get("render_style"),
        "status": result.get("status"),
        "scene_count": len(out),
        "scenes": out,
    }, indent=2)


def generate_longform_thumbnails(
    job_id: str = "",
    count: int = 3,
    feedback: str = "",
    *,
    title: str = "",
    channel_key: str = "history_rewind",
    prompt: str = "",
    user_id: str = "",
) -> str:
    """Generate/iterate long-form thumbnails without starting video production."""
    from long_form import pipeline as lf
    from long_form.prompts.channels import get_channel

    count = max(1, min(3, int(count or 3)))
    job_id = str(job_id or "").strip()
    state = lf.load_state(job_id) if job_id else None
    # Thumbnail planning must never mutate an existing long-form render.  The
    # model may see an active job id in conversation context and naturally
    # pass it back; that used to overwrite its phase with thumbnail_review and
    # make the UI offer “Finalize” instead of showing the requested candidates.
    # Only an already-isolated thumbnail job is safe to reuse for revisions.
    if state and not bool(state.get("thumbnail_only")):
        job_id = ""
        state = None
    if not state:
        job_id = lf._new_job_id()
        lf._ensure_job_dir(job_id)
        resolved_title = str(title or "Untitled long-form video").strip()[:180]
        resolved_channel = str(channel_key or "history_rewind").strip().lower()
        # Validate/fall back before persisting a capability-token job.
        try:
            get_channel(resolved_channel)
        except Exception:
            resolved_channel = "history_rewind"
        state = {
            "job_id": job_id,
            "user_id": str(user_id or ""),
            "channel_key": resolved_channel,
            "outline": {"title": resolved_title, "topic": resolved_title},
            "phase": "thumbnail_review",
            "percent": 0,
            "thumbnail_only": True,
            "thumbnails_generated": 0,
            "created_at": time.time(),
        }
        lf.save_state(job_id, state)

    resolved_channel = str(state.get("channel_key") or channel_key or "history_rewind")
    resolved_title = str((state.get("outline") or {}).get("title") or title or "Untitled long-form video")
    channel = get_channel(resolved_channel)
    thumbs_dir = lf._job_dir(job_id) / "thumbnails"
    thumbs_dir.mkdir(parents=True, exist_ok=True)
    direction = str(prompt or feedback or "").strip()
    if direction:
        # When the channel's real published covers are reachable they carry the
        # visual identity, so the prompt carries only the title + the creator's
        # requested change. Mixing in the hand-written style paragraph fought
        # the reference images and produced off-brand thumbnails (the yellow
        # caps + badge look the creator explicitly rejected).
        if lf.channel_reference_thumbnails(channel):
            full_prompt = (
                f"Short on-image title (render exactly this text): {lf.thumbnail_display_title(resolved_title)}.\n"
                f"Full video topic for the scene: {resolved_title}.\n"
                f"Creator-requested revision: {direction}. "
                "16:9 YouTube thumbnail, one dominant focal subject, readable at phone size."
            )
        else:
            base = str(channel.get("thumbnail_style_prompt") or channel.get("visual_style") or "").strip()
            full_prompt = (
                f"{base}\n\nDocumentary title context: {resolved_title}.\n\n"
                f"User-directed revision: {direction}. 16:9 YouTube thumbnail, one dominant focal subject, "
                "strong visual hierarchy, readable at phone size."
            )
        for idx in range(1, count + 1):
            lf.regenerate_thumbnail(job_id, idx, custom_prompt=full_prompt)
    else:
        lf._gen_thumbnails(channel, state.get("outline") or {}, thumbs_dir, count=count)

    state = lf.load_state(job_id) or state
    state.update({
        "phase": "thumbnail_review",
        "percent": 100,
        "thumbnail_only": True,
        "thumbnails_generated": count,
        "thumbnail_feedback": direction,
        "updated_at": time.time(),
    })
    lf.save_state(job_id, state)
    # Cache-bust: revision reuses the same URLs, and without a version param the
    # browser can keep showing the pre-revision images — which reads as "my
    # feedback did nothing" even though new files were rendered.
    version = int(time.time())
    thumbs = [
        f"/api/studio-agent/jobs/{job_id}/thumbnail/{i}?v={version}"
        for i in range(1, count + 1)
    ]
    return json.dumps({
        "job_id": job_id,
        "kind": "longform",
        "status": "awaiting_thumbnail_review",
        "stage": "thumbnail_review",
        "title": resolved_title,
        "thumbnail_only": True,
        "thumbnails": thumbs,
        "preview_url": thumbs[0],
        "count": count,
        "feedback_used": direction,
        "production_started": False,
        "note": "Thumbnail-only Plan-mode preview. Inspect it, then give feedback with this job_id to revise without starting the video."
    }, indent=2)


def _expandable_proof_job(spec: dict[str, Any], ws: Path) -> bool:
    if bool(spec.get("visual_proof_only")):
        return True
    if int(spec.get("scene_count") or 0) == 1:
        return True
    scenes_path = ws / "scenes.json"
    if scenes_path.is_file():
        try:
            scenes = json.loads(scenes_path.read_text(encoding="utf-8"))
            if isinstance(scenes, list) and len(scenes) == 1:
                return True
        except Exception:
            pass
    return False


def _capture_shortform_background_state(workspace: Path) -> dict[str, Any]:
    """Capture the small mutable surface needed to roll back a stale route."""

    workspace = Path(workspace)
    json_files: dict[str, bytes | None] = {}
    for name in ("scenes.json", "result.json", "progress.json", "scene_plan.json"):
        path = workspace / name
        json_files[name] = path.read_bytes() if path.is_file() else None
    media_files: set[str] = set()
    for dirname in ("stills", "clips", "trimmed"):
        root = workspace / dirname
        if root.is_dir():
            media_files.update(
                str(path.relative_to(workspace)).replace("\\", "/")
                for path in root.rglob("*")
                if path.is_file()
            )
    return {"json_files": json_files, "media_files": media_files}


def _rollback_stale_shortform_route(
    workspace: Path,
    snapshot: dict[str, Any],
    *,
    command_id: str,
    revision: int,
    stage: str,
) -> list[str]:
    """Quarantine new media and restore job metadata after a picker switch."""

    workspace = Path(workspace)
    before = set(snapshot.get("media_files") or set())
    quarantined: list[str] = []
    quarantine_root = (
        workspace
        / "stale_media_routes"
        / f"{re.sub(r'[^a-zA-Z0-9_-]+', '-', command_id or 'background')[:48]}-r{int(revision)}-{stage}-{uuid.uuid4().hex[:8]}"
    )
    for dirname in ("stills", "clips", "trimmed"):
        root = workspace / dirname
        if not root.is_dir():
            continue
        for path in list(root.rglob("*")):
            if not path.is_file():
                continue
            rel = str(path.relative_to(workspace)).replace("\\", "/")
            if rel in before:
                continue
            target = quarantine_root / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            path.replace(target)
            quarantined.append(str(target.relative_to(workspace)).replace("\\", "/"))
    for name, payload in dict(snapshot.get("json_files") or {}).items():
        path = workspace / str(name)
        if payload is None:
            path.unlink(missing_ok=True)
            continue
        temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
        temporary.write_bytes(payload)
        os.replace(temporary, path)
    return quarantined


def _shortform_background_route_is_current(
    *,
    session_id: str,
    command_id: str,
    job_id: str,
    expected: dict[str, Any],
    stage: str,
) -> bool:
    """Validate both picker revision and any newer production-gate owner."""

    if not session_id:
        return True
    session = store.get_session(
        session_id,
        reconcile_jobs=False,
        _prune_active_jobs=False,
    ) or {}
    if not session:
        return False
    if session.get("production_gate_open"):
        active_command = str(session.get("active_command_id") or "").strip()
        active_job = str(session.get("active_command_job_id") or "").strip()
        if active_command and active_command != str(command_id or "").strip():
            return False
        if active_job and active_job != str(job_id or "").strip():
            return False
    current = _repair_route_snapshot(session_id)
    return _same_media_route(expected, current, stage=stage)


def expand_visual_proof_shortform(
    job_id: str,
    scene_count: int = 12,
    duration_seconds: float | None = None,
    creative_direction: str = "",
    animate_policy: str = "heroes",
    command_id: str = "",
    existing_scene_count: int | None = None,
    preserve_scene_indices: list[int] | None = None,
    animate_scene_indices: list[int] | None = None,
    *,
    image_model_id: str | None = None,
    video_model: str | None = None,
    route_revision: int | None = None,
    credit_reservation: dict[str, Any] | None = None,
    credit_user_id: str = "",
    credit_session_id: str = "",
    credit_budget: dict[str, Any] | None = None,
) -> str:
    """Keep the approved proof scene/script and generate the remaining short scenes.

    Fast expand: known-good ≤300 prompts, default animate=heroes (not weak batch-all).
    """
    import traceback as _tb

    from studio_agent.visual_fix_contract import (
        harden_planned_scenes_for_expand,
        parse_animate_policy,
    )

    ws = _shortform_workspace(job_id)
    spec_path = ws / "job_spec.json"
    normalized_command_id = str(command_id or "").strip()[:160]

    def _contract_indices(raw: Any) -> list[int]:
        if not isinstance(raw, list):
            return []
        normalized: set[int] = set()
        for value in raw[:60]:
            try:
                normalized.add(int(value))
            except (TypeError, ValueError):
                raise ValueError("scene index contracts must contain integers") from None
        return sorted(normalized)

    # Claim a command before mutating the proof. A retry with the same id sees
    # this durable record and returns without starting a second worker.
    with _expand_command_lock, _expand_job_file_lock(ws):
        if not spec_path.is_file():
            raise ValueError(f"job {job_id} has no job_spec.json")
        command_spec = json.loads(spec_path.read_text(encoding="utf-8"))
        if not isinstance(command_spec, dict):
            raise ValueError(f"job {job_id} has invalid job_spec.json")
        if credit_user_id and str(command_spec.get("user_id") or "") != str(credit_user_id):
            raise ValueError("job ownership mismatch")
        existing_rows: list[dict[str, Any]] = []
        scenes_path = ws / "scenes.json"
        if scenes_path.is_file():
            try:
                loaded_scenes = json.loads(scenes_path.read_text(encoding="utf-8"))
                if isinstance(loaded_scenes, list):
                    existing_rows = [row for row in loaded_scenes if isinstance(row, dict)]
            except Exception:
                existing_rows = []
        actual_existing_count = len(existing_rows) or max(1, int(command_spec.get("scene_count") or 1))
        if existing_scene_count is not None and int(existing_scene_count) != actual_existing_count:
            raise ValueError(
                f"existing_scene_count mismatch: command said {int(existing_scene_count)}, "
                f"job has {actual_existing_count}"
            )
        target_scenes = max(2, min(int(scene_count or command_spec.get("scene_count") or 12), 60))
        if target_scenes <= actual_existing_count:
            raise ValueError("scene_count must be greater than the existing proof scene count")
        requested_preserve = _contract_indices(preserve_scene_indices) if preserve_scene_indices is not None else [0]
        preserve_indices = sorted({0, *requested_preserve})
        if any(index < 0 or index >= actual_existing_count for index in preserve_indices):
            raise ValueError("preserve_scene_indices must refer to existing scenes")
        explicit_animation_contract = animate_scene_indices is not None
        selected_animation_indices = _contract_indices(animate_scene_indices)
        invalid_animation_indices = [
            index for index in selected_animation_indices
            if index < actual_existing_count or index >= target_scenes or index in preserve_indices
        ]
        if invalid_animation_indices:
            raise ValueError(
                "animate_scene_indices must refer only to new, non-preserved scenes; "
                f"invalid={invalid_animation_indices}"
            )
        normalized_duration = (
            round(float(duration_seconds), 4)
            if duration_seconds is not None and float(duration_seconds) > 0
            else None
        )
        normalized_direction = re.sub(r"\s+", " ", str(creative_direction or "").strip())[:400]
        policy = parse_animate_policy(
            f"{animate_policy} {normalized_direction}",
            default=(
                str(animate_policy or "heroes")
                if str(animate_policy or "") in {"heroes", "all", "none"}
                else "heroes"
            ),
        )
        semantic_contract = {
            "job_id": str(job_id),
            "scene_count": target_scenes,
            "existing_scene_count": actual_existing_count,
            "preserve_scene_indices": preserve_indices,
            "animate_scene_indices": selected_animation_indices if explicit_animation_contract else None,
            "duration_seconds": normalized_duration,
            "creative_direction": normalized_direction,
            "animate_policy": policy,
        }
        semantic_fingerprint = hashlib.sha256(
            json.dumps(
                semantic_contract,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest()

        prior_command = (
            command_spec.get("last_expand_command")
            if isinstance(command_spec.get("last_expand_command"), dict)
            else {}
        )
        prior_status = str(prior_command.get("status") or "").strip().lower()
        same_command = bool(
            normalized_command_id
            and str(prior_command.get("command_id") or "") == normalized_command_id
        )
        prior_fingerprint = str(prior_command.get("semantic_fingerprint") or "").strip()
        semantic_match = bool(prior_fingerprint and prior_fingerprint == semantic_fingerprint)
        if same_command and not prior_fingerprint:
            # Compatibility for an in-flight claim created before fingerprints
            # existed. Command IDs are generated from the canonical command.
            semantic_match = True
        if prior_command and (same_command or prior_status in {"accepted", "started", "running"}):
            if not semantic_match:
                return json.dumps(
                    {
                        "ok": False,
                        "status": "conflict",
                        "error": (
                            "A different expansion is already in progress on this job. "
                            "Wait for it to finish, then issue a new expansion request."
                        ),
                        "job_id": job_id,
                        "command_id": normalized_command_id or None,
                        "active_command_id": prior_command.get("command_id"),
                        "active_semantic_fingerprint": prior_fingerprint or None,
                        "requested_semantic_fingerprint": semantic_fingerprint,
                        "active_scene_count": prior_command.get("scene_count"),
                        "requested_scene_count": target_scenes,
                        "idempotent_replay": False,
                    },
                    indent=2,
                    ensure_ascii=False,
                )
            replay = dict(prior_command)
            replay["ok"] = prior_status != "failed"
            replay["idempotent_replay"] = True
            if normalized_command_id and not same_command:
                replay["replayed_for_command_id"] = normalized_command_id
            replay["note"] = "This exact expansion contract was already accepted; no second worker was started."
            return json.dumps(replay, indent=2, ensure_ascii=False)
        if not _expandable_proof_job(command_spec, ws):
            raise ValueError("job is not an expandable one-scene proof short")
        if normalized_command_id:
            command_spec["last_expand_command"] = {
                "ok": True,
                "status": "accepted",
                "job_id": job_id,
                "topic": command_spec.get("topic"),
                "command_id": normalized_command_id,
                "scene_count": target_scenes,
                "existing_scene_count": actual_existing_count,
                "additional_scene_count": target_scenes - actual_existing_count,
                "preserve_scene_indices": preserve_indices,
                "animate_scene_indices": selected_animation_indices if explicit_animation_contract else None,
                "duration_seconds": normalized_duration,
                "creative_direction": normalized_direction,
                "animate_policy": policy,
                "semantic_fingerprint": semantic_fingerprint,
                "idempotent_replay": False,
            }
            _atomic_write_json(spec_path, command_spec)
    if not spec_path.is_file():
        raise ValueError(f"job {job_id} has no job_spec.json")
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    if not isinstance(spec, dict):
        raise ValueError(f"job {job_id} has invalid job_spec.json")
    if not _expandable_proof_job(spec, ws):
        raise ValueError("job is not an expandable one-scene proof short")
    target_scenes = max(2, min(int(scene_count or spec.get("scene_count") or 12), 60))
    if duration_seconds is not None and float(duration_seconds) > 0:
        per_scene = max(3.0, float(duration_seconds) / float(target_scenes))
        spec["duration_seconds"] = round(float(duration_seconds), 2)
        spec["seconds_per_scene"] = round(per_scene, 2)
    spec["visual_proof_only"] = False
    spec["scene_count"] = target_scenes
    spec["expand_existing_scene_count"] = actual_existing_count
    spec["preserve_scene_indices"] = preserve_indices
    spec["expand_animate_scene_indices"] = selected_animation_indices if explicit_animation_contract else None
    policy = parse_animate_policy(
        f"{animate_policy} {creative_direction}",
        default=str(animate_policy or "heroes") if str(animate_policy or "") in {"heroes", "all", "none"} else "heroes",
    )
    spec["expand_animate_policy"] = policy
    initial_route = _repair_route_snapshot(
        credit_session_id,
        image_model_id=image_model_id,
        video_model=video_model,
        route_revision=route_revision,
    )
    spec["image_model_id"] = initial_route.get("image_model_id") or spec.get("image_model_id")
    spec["video_model"] = initial_route.get("video_model") or spec.get("video_model")
    spec["media_route_revision"] = int(initial_route.get("revision") or 1)
    spec["media_route_session_id"] = str(credit_session_id or "")
    spec["background_command_id"] = normalized_command_id
    if str(creative_direction or "").strip():
        # Keep expansion direction SHORT — long sludge in visual_brief caused artifacting.
        direction = re.sub(r"\s+", " ", str(creative_direction).strip())[:400]
        spec["expansion_creative_direction"] = direction
        existing_brief = re.sub(r"\s+", " ", str(spec.get("visual_brief") or "").strip())[:800]
        spec["visual_brief"] = f"{existing_brief} Expand: {direction}".strip()[:1200]
        low = direction.lower()
        if any(term in low for term in ("motion graphic", "graphic", "overlay", "effect", "vfx")):
            spec["motion_graphics_requested"] = True
        if any(term in low for term in ("no caption", "captions off", "without caption")):
            spec["captions_enabled"] = False
            spec["caption_mode"] = "off"
    immediate_result: dict[str, Any] = {
        "ok": True,
        "status": "started",
        "job_id": job_id,
        "topic": spec.get("topic"),
        "command_id": normalized_command_id or None,
        "scene_count": target_scenes,
        "existing_scene_count": actual_existing_count,
        "additional_scene_count": target_scenes - actual_existing_count,
        "preserve_scene_indices": preserve_indices,
        "animate_scene_indices": selected_animation_indices if explicit_animation_contract else None,
        "animate_policy": policy,
        "semantic_fingerprint": semantic_fingerprint,
        "visual_proof_only": False,
        "idempotent_replay": False,
        "note": (
            "Fast expand started on the same job. Preserved scenes stay locked; remaining scenes use "
            f"known-good prompts; animate_policy={policy}."
        ),
    }
    if normalized_command_id:
        spec["last_expand_command"] = dict(immediate_result)
    with _expand_command_lock, _expand_job_file_lock(ws):
        _atomic_write_json(spec_path, spec)
    _write_shortform_credit_reservation(
        ws,
        reservation=credit_reservation,
        user_id=credit_user_id or str(spec.get("user_id") or ""),
        tool="expand_visual_proof_shortform",
        session_id=credit_session_id,
        budget=credit_budget,
    )
    try:
        (ws / "result.json").unlink(missing_ok=True)
    except OSError:
        pass
    (ws / "progress.json").write_text(
        json.dumps(
            {
                "stage": "restarting",
                "progress": 18,
                "detail": (
                    f"Fast expand into a {target_scenes}-scene short "
                    f"(animate={policy}; known-good prompts)."
                ),
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    def _work() -> None:
        from skeleton_ai.styled_pipeline import animate_scenes_stage, load_scenes, plan_scenes, save_scenes

        hb_path = ws / "heartbeat.txt"
        stop_hb = threading.Event()
        job_mutation = None
        hb_thread = threading.Thread(
            target=_heartbeat_loop,
            args=(stop_hb, hb_path),
            daemon=True,
            name=f"hb-expand-{job_id}",
        )
        hb_thread.start()
        try:
            # The validated runner's lock ends when this asynchronous tool
            # returns. Re-acquire it inside the worker so no later command can
            # mutate this job while expansion is still writing artifacts.
            job_mutation = store.production_job_mutation_lock(job_id)
            job_mutation.__enter__()
            hb_path.touch(exist_ok=True)
            still_route: dict[str, Any] = {}
            for _route_attempt in range(4):
                still_route = _repair_route_snapshot(
                    credit_session_id,
                    image_model_id=image_model_id or spec.get("image_model_id"),
                    video_model=video_model or spec.get("video_model"),
                    route_revision=route_revision or spec.get("media_route_revision"),
                )
                spec["image_model_id"] = still_route.get("image_model_id") or spec.get("image_model_id")
                spec["video_model"] = still_route.get("video_model") or spec.get("video_model")
                spec["media_route_revision"] = int(still_route.get("revision") or 1)
                with _expand_command_lock, _expand_job_file_lock(ws):
                    latest_spec = json.loads(spec_path.read_text(encoding="utf-8"))
                    latest_spec.update({
                        "image_model_id": spec.get("image_model_id"),
                        "video_model": spec.get("video_model"),
                        "media_route_revision": spec.get("media_route_revision"),
                        "media_route_session_id": str(credit_session_id or ""),
                        "background_command_id": normalized_command_id,
                    })
                    _atomic_write_json(spec_path, latest_spec)
                route_snapshot = _capture_shortform_background_state(ws)
                with production_slot("render"):
                    plan_scenes(
                        category_key=str(spec.get("category_key") or "people_blogs"),
                        topic=spec.get("topic"),
                        workspace=ws,
                        render_style=str(spec.get("render_style") or "cinematic"),
                        tier=str(spec.get("tier") or "standard"),
                        image_model_id=spec.get("image_model_id"),
                        video_model=spec.get("video_model"),
                        visual_brief=spec.get("visual_brief"),
                        beats_target=target_scenes,
                        script_override=spec.get("script"),
                        user_id=spec.get("user_id"),
                        default_animate=False,
                        reference_images=list(spec.get("reference_images") or []),
                        sound_design_brief=str(spec.get("sound_design_brief") or ""),
                    )
                if _shortform_background_route_is_current(
                    session_id=str(credit_session_id or ""),
                    command_id=normalized_command_id,
                    job_id=job_id,
                    expected=still_route,
                    stage="image",
                ):
                    break
                _rollback_stale_shortform_route(
                    ws,
                    route_snapshot,
                    command_id=normalized_command_id,
                    revision=int(still_route.get("revision") or 1),
                    stage="image",
                )
            else:
                raise RuntimeError("media route kept changing during short-form still generation")
            # Harden onto known-good ≤300 prompts; animate only per policy (not weak batch-all).
            planned = harden_planned_scenes_for_expand(
                load_scenes(ws),
                topic=str(spec.get("topic") or ""),
                visual_brief=str(spec.get("visual_brief") or ""),
                outfit=str(spec.get("locked_outfit") or "no clothing"),
                job_cast=spec.get("cast_count"),
                animate_policy=policy,
                aspect_ratio=str(spec.get("aspect_ratio") or "9:16"),
            )
            if explicit_animation_contract:
                selected = set(selected_animation_indices)
                for scene in planned:
                    index = int(scene.get("index", -1))
                    if index in preserve_indices or index < actual_existing_count:
                        continue
                    can_animate = bool(scene.get("approved_for_video")) and str(scene.get("status") or "") not in {
                        "qa_blocked", "error"
                    }
                    requested = index in selected and can_animate
                    scene["approved_for_animation"] = requested
                    scene["animate"] = requested and not bool(scene.get("clip_rel"))
            animate_indices = [
                int(scene.get("index", -1))
                for scene in planned
                if scene.get("animate")
                and not scene.get("clip_rel")
                and int(scene.get("index", -1)) >= actual_existing_count
                and int(scene.get("index", -1)) not in preserve_indices
            ]
            save_scenes(ws, planned)
            (ws / "progress.json").write_text(json.dumps({
                "stage": "expand_animate",
                "progress": 72,
                "detail": (
                    f"Animating {len(animate_indices)} hero/selected scene(s) "
                    f"(policy={policy}); others stay Ken Burns."
                ),
            }, indent=2), encoding="utf-8")
            if animate_indices:
                for _route_attempt in range(4):
                    animation_route = _repair_route_snapshot(
                        credit_session_id,
                        image_model_id=spec.get("image_model_id"),
                        video_model=video_model or spec.get("video_model"),
                        route_revision=route_revision or spec.get("media_route_revision"),
                    )
                    current_scenes = load_scenes(ws)
                    for scene in current_scenes:
                        if int(scene.get("index", -1)) in animate_indices:
                            scene["video_model"] = animation_route.get("video_model")
                            scene["media_route_revision"] = int(animation_route.get("revision") or 1)
                    save_scenes(ws, current_scenes)
                    animation_snapshot = _capture_shortform_background_state(ws)
                    animate_scenes_stage(
                        ws,
                        indices=animate_indices,
                        tier=str(spec.get("tier") or "standard"),
                    )
                    if _shortform_background_route_is_current(
                        session_id=str(credit_session_id or ""),
                        command_id=normalized_command_id,
                        job_id=job_id,
                        expected=animation_route,
                        stage="video",
                    ):
                        break
                    _rollback_stale_shortform_route(
                        ws,
                        animation_snapshot,
                        command_id=normalized_command_id,
                        revision=int(animation_route.get("revision") or 1),
                        stage="video",
                    )
                else:
                    raise RuntimeError("media route kept changing during short-form animation")
            final_scenes = load_scenes(ws)
            qa_blocked = [
                int(scene.get("index", -1))
                for scene in final_scenes
                if str(scene.get("status") or "") in {"qa_blocked", "error"}
            ]
            (ws / "result.json").write_text(json.dumps({
                "status": "awaiting_scene_review",
                "job_id": job_id,
                "topic": spec.get("topic"),
                "scene_count": len(final_scenes),
                "command_id": normalized_command_id or None,
                "existing_scene_count": actual_existing_count,
                "additional_scene_count": max(0, len(final_scenes) - actual_existing_count),
                "preserve_scene_indices": preserve_indices,
                "animate_scene_indices": animate_indices,
                "animate_policy": policy,
                "approved_scene_count": sum(1 for scene in final_scenes if scene.get("approved_for_video")),
                "animation_pending_count": sum(1 for scene in final_scenes if scene.get("approved_for_animation") and not scene.get("clip_rel")),
                "qa_blocked_scenes": qa_blocked,
                "note": "Fast expand complete: known-good prompts; selective animation; ready for review.",
            }, indent=2), encoding="utf-8")
            if normalized_command_id:
                with _expand_command_lock, _expand_job_file_lock(ws):
                    latest_spec = json.loads(spec_path.read_text(encoding="utf-8"))
                    latest_command = (
                        dict(latest_spec.get("last_expand_command"))
                        if isinstance(latest_spec.get("last_expand_command"), dict)
                        else {}
                    )
                    if str(latest_command.get("command_id") or "") == normalized_command_id:
                        latest_command["status"] = "completed"
                        latest_command["finished_at"] = time.time()
                        latest_spec["last_expand_command"] = latest_command
                        _atomic_write_json(spec_path, latest_spec)
            (ws / "progress.json").write_text(json.dumps({
                "stage": "awaiting_scene_review",
                "progress": 80,
                "detail": "Expanded scenes ready for review (Fast expand).",
            }, indent=2), encoding="utf-8")
            _reconcile_shortform_costs(
                credit_user_id or str(spec.get("user_id") or ""),
                job_id,
                reservation_payload=_load_shortform_credit_reservation(ws),
                reason="studio_shortform_expand_actual",
                tool="expand_visual_proof_shortform",
                session_id=credit_session_id,
            )
        except Exception as exc:
            if normalized_command_id:
                try:
                    with _expand_command_lock, _expand_job_file_lock(ws):
                        latest_spec = json.loads(spec_path.read_text(encoding="utf-8"))
                        latest_command = (
                            dict(latest_spec.get("last_expand_command"))
                            if isinstance(latest_spec.get("last_expand_command"), dict)
                            else {}
                        )
                        if str(latest_command.get("command_id") or "") == normalized_command_id:
                            latest_command["status"] = "failed"
                            latest_command["error"] = str(exc)[:500]
                            latest_command["finished_at"] = time.time()
                            latest_spec["last_expand_command"] = latest_command
                            _atomic_write_json(spec_path, latest_spec)
                except Exception:
                    pass
            try:
                (ws / "job.log").write_text(
                    f"EXPAND FAILED {time.time()}\n{exc}\n\n{_tb.format_exc()}",
                    encoding="utf-8",
                )
            except Exception:
                pass
            try:
                (ws / "result.json").write_text(
                    json.dumps({"status": "failed", "job_id": job_id, "error": str(exc)}, indent=2),
                    encoding="utf-8",
                )
            except Exception:
                pass
            try:
                reservation_payload = _load_shortform_credit_reservation(ws)
                if production_costs.pending_billable_usd(ws) > 0:
                    _reconcile_shortform_costs(
                        credit_user_id or str(spec.get("user_id") or ""),
                        job_id,
                        reservation_payload=reservation_payload,
                        reason="studio_shortform_expand_failed_actual",
                        tool="expand_visual_proof_shortform",
                        session_id=credit_session_id,
                    )
                else:
                    _release_shortform_reservation(
                        credit_user_id or str(spec.get("user_id") or ""),
                        reservation_payload,
                        reason="studio_shortform_expand_failed_no_spend",
                    )
                    _clear_shortform_credit_reservation(ws)
            except Exception:
                pass
        finally:
            stop_hb.set()
            if job_mutation is not None:
                job_mutation.__exit__(None, None, None)

    threading.Thread(target=_work, daemon=False, name=f"expand-{job_id}").start()
    return json.dumps(immediate_result, indent=2, ensure_ascii=False)


def edit_production_scene_still(job_id: str, scene_index: int, instruction: str, scope: str = "full") -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import edit_scene
    res = edit_scene(ws, int(scene_index), str(instruction), scope=str(scope or "full"))
    return json.dumps({
        "ok": True,
        "job_id": job_id,
        "scene": res,
        "note": (
            "Still updated via Seedream V4.5 edit. Prior clip invalidated. "
            "For identity-consistent videos, use character edits first, then background edits, then re-animate this scene when ready."
        ),
    }, indent=2)


def edit_production_scenes_still(
    job_id: str,
    instruction: str,
    scene_indices: list[int] | None = None,
    scope: str = "character",
) -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import edit_scene, load_scenes

    scenes = load_scenes(ws)
    requested = {int(x) for x in scene_indices} if scene_indices else None
    targets: list[int] = []
    for sc in scenes:
        idx = int(sc.get("index", -1))
        if idx < 0:
            continue
        if requested is None or idx in requested:
            targets.append(idx)

    if not targets:
        raise ValueError("no scenes matched for bulk still edit")

    changed = []
    errors = []
    edit_scope = str(scope or "character")
    for idx in targets:
        try:
            changed.append(edit_scene(ws, idx, str(instruction), scope=edit_scope))
        except Exception as exc:
            errors.append({"scene_index": idx, "error": str(exc)})

    return json.dumps({
        "ok": not errors,
        "job_id": job_id,
        "affected": targets,
        "changed_count": len(changed),
        "errors": errors,
        "scenes": changed,
        "note": (
            "Bulk still edit complete. Prior clips for changed scenes were invalidated. "
            "Review the updated stills in chat; do not animate or finalize until the user approves them."
        ),
    }, indent=2)


def _still_candidate_sidecars(path: Path) -> tuple[Path, ...]:
    return (
        path.with_suffix(path.suffix + ".stillqa.json"),
        path.with_suffix(path.suffix + ".productqa.json"),
    )


def _discard_still_candidate(
    workspace: Path,
    candidate: Path,
    *,
    scene_index: int,
    label: str,
) -> str:
    """Quarantine a complete but uncommitted candidate for operator forensics."""

    if not candidate.is_file():
        for sidecar in _still_candidate_sidecars(candidate):
            sidecar.unlink(missing_ok=True)
        return ""
    rejected = workspace / "rejected_stills"
    rejected.mkdir(parents=True, exist_ok=True)
    safe_label = re.sub(r"[^a-z0-9_-]+", "-", str(label or "rejected").lower()).strip("-")[:36]
    target = rejected / f"scene-{int(scene_index) + 1}-{safe_label}-{uuid.uuid4().hex[:8]}.png"
    candidate.replace(target)
    for source in _still_candidate_sidecars(candidate):
        if source.is_file():
            source.replace(target.with_suffix(target.suffix + source.name[len(candidate.name):]))
    try:
        return str(target.relative_to(workspace)).replace("\\", "/")
    except Exception:
        return str(target)


def regenerate_production_scene_still(
    job_id: str,
    scene_index: int,
    *,
    reason: str = "",
    force_master_regenerate: bool = False,
    image_model_id: str | None = None,
    fallback_image_model_id: str | None = None,
    session_id: str | None = None,
    route_revision: int | None = None,
) -> str:
    ws = _shortform_workspace(job_id)
    from studio_agent.catalyst_still_audit import audit_scene_still, record_catalyst_still_artifact_learning
    from skeleton_ai.styled_pipeline import (
        MediaRouteChangedError,
        load_scenes,
        regenerate_scene_with_catalyst,
        save_scenes,
    )
    from studio_agent import visual_qa

    idx = int(scene_index)
    route_switches: list[dict[str, Any]] = []
    quarantined: list[str] = []
    last_route = _repair_route_snapshot(
        session_id,
        image_model_id=image_model_id,
        route_revision=route_revision,
    )
    last_error = ""

    # A creator can switch provider/model while an earlier remote request is in
    # flight. Bound restart churn, but never commit an old-revision result.
    for route_attempt in range(1, 5):
        route = _repair_route_snapshot(
            session_id,
            image_model_id=image_model_id,
            route_revision=route_revision,
        )
        last_route = route
        scenes = load_scenes(ws)
        scene = next((item for item in scenes if int(item.get("index", -1)) == idx), None)
        if scene is None:
            raise ValueError(f"scene {idx} not found")
        try:
            spec = json.loads((ws / "job_spec.json").read_text(encoding="utf-8"))
        except Exception:
            spec = {}
        sid = str(scene.get("sid") or f"b{idx:02d}")
        still_rel = str(scene.get("still_rel") or f"stills/{sid}.png")
        still_target = ws / still_rel
        selected_image = str(route.get("image_model_id") or image_model_id or "").strip()
        selected_fallback = str(fallback_image_model_id or "").strip()
        if not selected_fallback and selected_image and not selected_image.startswith("grok"):
            selected_fallback = selected_image
        if not selected_fallback:
            selected_fallback = "seedream_edit"

        repair_audit = audit_scene_still(ws, idx)
        if force_master_regenerate:
            repair_audit["method"] = "regenerate"
            repair_audit["fix_instruction"] = ""
            repair_audit["creative_redesign"] = True
        if str(reason or "").strip():
            repair_audit["user_reason"] = str(reason).strip()[:500]

        candidate = ws / "stills" / (
            f".{sid}.route-{int(route.get('revision') or 1)}-{uuid.uuid4().hex[:10]}.candidate.png"
        )
        current_audit: dict[str, Any] = {}
        res: dict[str, Any] = {}
        retry_count = 0
        route_changed = False
        while True:
            dispatch_route = _repair_route_snapshot(
                session_id,
                image_model_id=selected_image,
                route_revision=int(route.get("revision") or 1),
            )
            if not _same_media_route(route, dispatch_route, stage="image"):
                route_changed = True
                break

            def _fallback_route_is_current() -> bool:
                current_route = _repair_route_snapshot(
                    session_id,
                    image_model_id=selected_image,
                    route_revision=int(route.get("revision") or 1),
                )
                return _same_media_route(route, current_route, stage="image")

            try:
                res = regenerate_scene_with_catalyst(
                    ws,
                    idx,
                    audit=repair_audit if retry_count == 0 else current_audit,
                    image_model_id=selected_image or None,
                    fallback_image_model_id=selected_fallback,
                    candidate_path=candidate,
                    defer_commit=True,
                    fallback_guard=_fallback_route_is_current,
                )
            except MediaRouteChangedError:
                route_changed = True
                break
            current_audit = audit_scene_still(ws, idx, still_path=candidate)
            if str(reason or "").strip():
                current_audit["user_reason"] = str(reason).strip()[:500]
            after_provider = _repair_route_snapshot(
                session_id,
                image_model_id=selected_image,
                route_revision=int(route.get("revision") or 1),
            )
            if not _same_media_route(route, after_provider, stage="image"):
                route_changed = True
                break
            if str(current_audit.get("method") or "").lower() != "regenerate" or retry_count >= 2:
                break
            rejected = _discard_still_candidate(
                ws, candidate, scene_index=idx, label=f"qa-retry-{retry_count + 1}"
            )
            if rejected:
                quarantined.append(rejected)
            candidate = ws / "stills" / (
                f".{sid}.route-{int(route.get('revision') or 1)}-{uuid.uuid4().hex[:10]}.candidate.png"
            )
            retry_count += 1

        if route_changed:
            rejected = _discard_still_candidate(ws, candidate, scene_index=idx, label="stale-route")
            if rejected:
                quarantined.append(rejected)
            latest = _repair_route_snapshot(session_id, image_model_id=image_model_id, route_revision=route_revision)
            route_switches.append({"from": route, "to": latest, "stage": "image"})
            continue

        skeleton_mode = "skeleton" in str((spec or {}).get("render_style") or "").lower()
        if skeleton_mode:
            current_still_qa = visual_qa.audit_skeleton_still(
                candidate,
                reference=visual_qa._workspace_skeleton_reference(ws),
                locked_outfit=str((spec or {}).get("locked_outfit") or scene.get("outfit") or ""),
                cast_count=int(scene.get("cast_count") or (spec or {}).get("cast_count") or 1),
                force=True,
            )
        else:
            current_still_qa = visual_qa.audit_generic_still(
                candidate,
                scene_contract=" ".join(
                    str(scene.get(key) or "") for key in ("prompt", "scene_action", "narration")
                ),
                force=True,
            )
        passed = bool(
            current_still_qa.get("status") == "pass"
            and current_still_qa.get("pass") is True
            and str(current_audit.get("method") or "").lower() != "regenerate"
        )
        learning = record_catalyst_still_artifact_learning(
            channel_key=str(current_audit.get("channel_key") or ""),
            audit=current_audit,
            job_id=job_id,
            scene_index=idx,
        )
        if not passed:
            rejected = _discard_still_candidate(ws, candidate, scene_index=idx, label="qa-failed")
            if rejected:
                quarantined.append(rejected)
            scenes = load_scenes(ws)
            current = next((item for item in scenes if int(item.get("index", -1)) == idx), None)
            if current is not None:
                current["last_repair_error"] = str(
                    current_still_qa.get("summary")
                    or current_still_qa.get("error")
                    or "Replacement candidate did not pass still QA"
                )[:300]
                current["last_repair_route"] = route
                save_scenes(ws, scenes)
            res.update({
                "candidate_path": None,
                "catalyst_retry_count": retry_count,
                "catalyst_repair_audit": repair_audit,
                "catalyst_audit": current_audit,
                "still_qa": current_still_qa,
                "committed": False,
            })
            return json.dumps({
                "ok": False,
                "job_id": job_id,
                "scene_index": idx,
                "scene": res,
                "still_qa": current_still_qa,
                "catalyst_audit": current_audit,
                "catalyst_repair_audit": repair_audit,
                "catalyst_learning": learning,
                "route": route,
                "route_switches": route_switches,
                "quarantined_candidates": quarantined,
                "error": "Replacement still failed visual QA; the previous still and clip were retained.",
            }, indent=2)

        # Re-read immediately before the atomic swap. A stale provider result is
        # forensic evidence only; it can never overwrite the approved asset.
        before_commit = _repair_route_snapshot(
            session_id,
            image_model_id=selected_image,
            route_revision=int(route.get("revision") or 1),
        )
        if not _same_media_route(route, before_commit, stage="image"):
            rejected = _discard_still_candidate(ws, candidate, scene_index=idx, label="stale-before-commit")
            if rejected:
                quarantined.append(rejected)
            route_switches.append({"from": route, "to": before_commit, "stage": "image_commit"})
            continue

        prior = still_target.with_name(f".{still_target.name}.{uuid.uuid4().hex[:8]}.prior")
        if still_target.is_file():
            shutil.copy2(still_target, prior)
        candidate.replace(still_target)
        post_commit = _repair_route_snapshot(
            session_id,
            image_model_id=selected_image,
            route_revision=int(route.get("revision") or 1),
        )
        if not _same_media_route(route, post_commit, stage="image"):
            stale = candidate.with_name(f".{sid}.{uuid.uuid4().hex[:8]}.stale.png")
            still_target.replace(stale)
            if prior.is_file():
                prior.replace(still_target)
            rejected = _discard_still_candidate(ws, stale, scene_index=idx, label="stale-after-commit")
            if rejected:
                quarantined.append(rejected)
            for sidecar in _still_candidate_sidecars(candidate):
                sidecar.unlink(missing_ok=True)
            route_switches.append({"from": route, "to": post_commit, "stage": "image_commit"})
            continue
        prior.unlink(missing_ok=True)
        for source in _still_candidate_sidecars(candidate):
            destination = still_target.with_suffix(still_target.suffix + source.name[len(candidate.name):])
            destination.unlink(missing_ok=True)
            if source.is_file():
                source.replace(destination)

        # Only a committed still invalidates its dependent animation.
        clip = ws / "clips" / f"{sid}.mp4"
        clip.unlink(missing_ok=True)
        clip.with_suffix(clip.suffix + ".fal.json").unlink(missing_ok=True)
        clip.with_suffix(clip.suffix + ".visualqa.json").unlink(missing_ok=True)
        scenes = load_scenes(ws)
        current = next((item for item in scenes if int(item.get("index", -1)) == idx), None)
        if current is not None:
            current["still_qa"] = current_still_qa
            current["status"] = "still_ready"
            current["clip_rel"] = None
            current["approved_for_video"] = False
            current["approved_for_animation"] = False
            current["image_model_id"] = str(res.get("image_model_id") or selected_image or "")
            current["media_route_revision"] = int(route.get("revision") or 1)
            current.pop("last_repair_error", None)
            save_scenes(ws, scenes)
        res.update({
            "candidate_path": None,
            "catalyst_retry_count": retry_count,
            "catalyst_repair_audit": repair_audit,
            "catalyst_audit": current_audit,
            "still_qa": current_still_qa,
            "committed": True,
            "media_route_revision": int(route.get("revision") or 1),
        })
        return json.dumps({
            "ok": True,
            "job_id": job_id,
            "scene_index": idx,
            "scene": res,
            "still_qa": current_still_qa,
            "catalyst_audit": current_audit,
            "catalyst_repair_audit": repair_audit,
            "catalyst_learning": learning,
            "route": route,
            "route_switches": route_switches,
            "quarantined_candidates": quarantined,
            "note": "Replacement still passed QA and was committed atomically; its prior clip was invalidated.",
        }, indent=2)

    last_error = "Media route changed repeatedly before a safe still commit."
    return json.dumps({
        "ok": False,
        "job_id": job_id,
        "scene_index": idx,
        "route": last_route,
        "route_switches": route_switches,
        "quarantined_candidates": quarantined,
        "error": last_error,
    }, indent=2)


def regenerate_production_scene(
    job_id: str,
    scene_index: int,
    *,
    reason: str = "",
    animate: bool = True,
    restage_direction: str = "",
    image_model_id: str | None = None,
    video_model: str | None = None,
    session_id: str | None = None,
    route_revision: int | None = None,
) -> str:
    """Rebuild one complete scene: still QA, then its I2V clip QA.

    The UI's Regenerate action is a scene-level recovery, not a confusing
    still-only replacement. Targeted Edit remains the intentional way to
    change only an image before committing animation spend.
    """
    direction_update: dict[str, Any] | None = None
    force_master_for_artifact = False
    # Short notes such as "fix the hand" remain Catalyst feedback. A real
    # spoken/typed art direction becomes the replacement scene brief.
    reason_text = str(reason or "").strip()
    structured_restage = str(restage_direction or "").strip()
    reuse_existing_direction = bool(re.search(
        r"\b(?:current|actual|existing|stored)\s+(?:scene\s+)?prompt\b|\bbased\s+off\s+(?:the\s+)?(?:current|actual|existing|stored)\s+prompt\b",
        reason_text,
        re.I,
    ))
    try:
        if structured_restage:
            # QA's concrete restage is structured execution input. Do not let
            # the generic "correspondence QA" reason branch discard it.
            from skeleton_ai.styled_pipeline import apply_scene_direction

            direction_update = apply_scene_direction(
                _shortform_workspace(job_id), int(scene_index), structured_restage,
            )
        elif reuse_existing_direction:
            # "Regenerate scene 1 from its current prompt" means preserve a real
            # locked direction, but repair a lazy generic one from narration before
            # it is sent back to the image model.
            from skeleton_ai.styled_pipeline import improve_generic_scene_direction
            direction_update = improve_generic_scene_direction(
                _shortform_workspace(job_id), int(scene_index),
            )
        elif re.search(
            r"\b(?:fresh|semantic)\s+(?:still\s+)?visual\s+qa\b|"
            r"\b(?:still|image)\s+artifact(?:ing)?\b|"
            r"\b(?:artifact(?:ing|s)?|orb|bubble|pod|dome|capsule|fused|morph|glass\s+shell)\b",
            reason_text,
            re.I,
        ):
            # Exact fix method: short cast-aware master regenerate (visual_fix_contract).
            from studio_agent.visual_fix_contract import artifact_fix_plan
            from skeleton_ai.styled_pipeline import apply_scene_direction, load_scenes, save_scenes

            ws = _shortform_workspace(job_id)
            scenes = load_scenes(ws)
            scene = next((row for row in scenes if int(row.get("index", -1)) == int(scene_index)), None) or {}
            try:
                spec = json.loads((ws / "job_spec.json").read_text(encoding="utf-8"))
            except Exception:
                spec = {}
            plan = artifact_fix_plan(
                topic=str(spec.get("topic") or ""),
                visual_brief=str(spec.get("visual_brief") or ""),
                narration=str(scene.get("narration") or ""),
                scene_action=str(scene.get("scene_action") or ""),
                user_feedback=reason_text,
                job_cast=spec.get("cast_count"),
                scene_cast=scene.get("cast_count"),
                outfit=str(scene.get("outfit") or spec.get("locked_outfit") or "no clothing"),
                aspect_ratio="9:16",
            )
            direction_update = apply_scene_direction(ws, int(scene_index), plan["scene_action"])
            scenes = load_scenes(ws)
            scene = next((row for row in scenes if int(row.get("index", -1)) == int(scene_index)), None)
            if scene is not None:
                scene["cast_count"] = plan["cast_count"]
                scene["prompt"] = plan["still_prompt"]
                scene["motion_prompt"] = plan["motion_prompt"]
                scene["prompt_user_override"] = True
                scene["visual_fix_contract"] = {
                    "method": plan["method"],
                    "prompt_budget": plan["prompt_budget"],
                    "rules": plan["rules"],
                }
                save_scenes(ws, scenes)
            try:
                spec["cast_count"] = plan["cast_count"]
                (ws / "job_spec.json").write_text(json.dumps(spec, indent=2), encoding="utf-8")
            except Exception:
                pass
            force_master_for_artifact = True
        elif re.search(r"\b(?:scene\s+)?correspondence\s+qa\b|\bduplicate(?:\s+adjacent)?\b|\bnarrative\s+(?:mismatch|qa)\b", reason_text, re.I):
            # A QA finding is evidence, not a literal art prompt.  Re-direct from
            # the stored narration so we do not render the words "QA failed" into
            # a fresh generic scene.
            from skeleton_ai.styled_pipeline import redesign_scene_from_narration
            direction_update = redesign_scene_from_narration(
                _shortform_workspace(job_id), int(scene_index),
            )
        elif len(reason_text) >= 32:
            from skeleton_ai.styled_pipeline import apply_scene_direction
            direction_update = apply_scene_direction(
                _shortform_workspace(job_id), int(scene_index), reason_text,
            )
        else:
            # A bare Regenerate click is Studio's intelligent creative-director
            # path. Reinterpret the narration instead of reproducing a scene the
            # creator already rejected. An exact Prompt override is preserved by
            # redesign_scene_from_narration itself.
            from skeleton_ai.styled_pipeline import redesign_scene_from_narration
            direction_update = redesign_scene_from_narration(
                _shortform_workspace(job_id), int(scene_index),
            )
    except Exception:
        # Direction redesign is best-effort creative sugar. A missing/mid-write
        # scene manifest must not abort the actual still + clip regeneration.
        direction_update = None
    creative_redesign = bool(direction_update and direction_update.get("changed"))
    still_raw = regenerate_production_scene_still(
        job_id,
        scene_index,
        reason=reason,
        force_master_regenerate=creative_redesign or force_master_for_artifact,
        image_model_id=image_model_id,
        session_id=session_id,
        route_revision=route_revision,
    )
    still_result = json.loads(still_raw)
    if not animate:
        return still_raw
    # A still can be successfully generated yet correctly blocked by semantic
    # QA.  Do not throw from the animation stage in that case: callers need a
    # structured per-scene result so one blocked scene cannot abort a selected
    # five-scene batch and falsely report that later scenes were handled.
    still_qa = still_result.get("still_qa") if isinstance(still_result.get("still_qa"), dict) else {}
    if not bool(still_result.get("ok", False)) or (still_qa and still_qa.get("pass") is not True):
        return json.dumps({
            "ok": False,
            "job_id": job_id,
            "scene_index": int(scene_index),
            "still": still_result.get("scene"),
            "scene_direction": direction_update,
            "catalyst_audit": still_result.get("catalyst_audit"),
            "still_qa": still_qa,
            "route": still_result.get("route"),
            "route_switches": still_result.get("route_switches") or [],
            "animation": None,
            "error": str(still_result.get("error") or "Replacement still is awaiting visual QA; animation was not started."),
            "note": "The previous approved asset was retained; animation was not started.",
        }, indent=2)
    # This performs the semantic still approval gate before any I2V spend.
    set_production_scenes_animate(job_id, True, [int(scene_index)])
    animation_route = _repair_route_snapshot(
        session_id,
        image_model_id=image_model_id,
        video_model=video_model,
        route_revision=route_revision,
    )
    animation_raw = animate_production_scenes(
        job_id,
        [int(scene_index)],
        video_model=str(animation_route.get("video_model") or video_model or "") or None,
        session_id=session_id,
        route_revision=int(animation_route.get("revision") or 1),
    )
    animation = json.loads(animation_raw)
    return json.dumps({
        "ok": bool(animation.get("ok", False)),
        "job_id": job_id,
        "scene_index": int(scene_index),
        "still": still_result.get("scene"),
        "scene_direction": direction_update,
        "catalyst_audit": still_result.get("catalyst_audit"),
        "catalyst_learning": still_result.get("catalyst_learning"),
        "animation": animation,
        "route": {
            "image": still_result.get("route"),
            "video": animation.get("route") or animation_route,
        },
        "route_switches": list(still_result.get("route_switches") or []) + list(animation.get("route_switches") or []),
        "note": "Scene regenerated end-to-end: compact still prompt, still QA, I2V, and sampled-frame identity QA.",
    }, indent=2)


def regenerate_production_scenes(
    job_id: str,
    scene_indices: list[int] | None = None,
    *,
    reason: str = "",
    animate: bool = True,
) -> str:
    """Fully re-render a selected batch and return only after every scene finishes.

    This is the deterministic path behind natural commands such as
    "regenerate all six scenes from their prompts". Each scene receives the
    same state-repair, still QA, animation, and sampled-frame QA as a targeted
    scene regeneration; it is not six blind, concurrent API calls.
    """
    from skeleton_ai.styled_pipeline import load_scenes

    workspace = _shortform_workspace(job_id)
    scenes = load_scenes(workspace)
    available = [int(scene.get("index", idx)) for idx, scene in enumerate(scenes)]
    requested = scene_indices or available
    indices = [idx for idx in dict.fromkeys(int(value) for value in requested) if idx in set(available)]
    if not indices:
        raise ValueError("no valid scenes found for regeneration")

    results: list[dict[str, Any]] = []
    failed: list[int] = []
    for index in indices:
        try:
            result = json.loads(regenerate_production_scene(
                job_id,
                index,
                reason=reason,
                animate=animate,
            ))
            if not bool(result.get("ok", False)):
                failed.append(index)
            results.append(result)
        except Exception as exc:
            failed.append(index)
            results.append({"ok": False, "scene_index": index, "error": str(exc)})
    return json.dumps({
        "ok": not failed,
        "job_id": job_id,
        "regenerated": indices,
        "failed": failed,
        "scenes": results,
        "note": (
            "All requested scenes completed still + I2V regeneration and QA."
            if not failed else
            "Some scenes did not pass the full regeneration path; inspect only the listed failures."
        ),
    }, indent=2)


def set_production_scenes_animate(job_id: str, animate: bool, scene_indices: list[int] | None = None) -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import load_scenes, save_scenes
    scenes = load_scenes(ws)
    changed = []
    if scene_indices is None:
        try:
            spec = json.loads((ws / "job_spec.json").read_text(encoding="utf-8"))
            if bool(spec.get("visual_proof_only")):
                scene_indices = [0]
        except Exception:
            if len(scenes) == 1:
                scene_indices = [int(scenes[0].get("index", 0))]
    idx_set = set(scene_indices) if scene_indices else None
    try:
        spec = json.loads((ws / "job_spec.json").read_text(encoding="utf-8"))
    except Exception:
        spec = {}
    if "skeleton" in str((spec or {}).get("render_style") or "").lower():
        from studio_agent import visual_qa

        reference = visual_qa._workspace_skeleton_reference(ws)
        failed_qa: list[dict[str, Any]] = []
        for sc in scenes:
            if idx_set is not None and sc.get("index") not in idx_set:
                continue
            sid = str(sc.get("sid") or f"b{int(sc.get('index', 0)):02d}")
            still_rel = str(sc.get("still_rel") or f"stills/{sid}.png")
            qa = visual_qa.audit_skeleton_still(
                ws / still_rel,
                reference=reference,
                locked_outfit=str((spec or {}).get("locked_outfit") or sc.get("outfit") or ""),
                cast_count=int(sc.get("cast_count") or (spec or {}).get("cast_count") or 1),
            )
            sc["still_qa"] = qa
            if qa.get("status") != "pass" or qa.get("pass") is not True:
                sc["approved_for_video"] = False
                sc["approved_for_animation"] = False
                sc["animate"] = False
                failed_qa.append({
                    "scene": int(sc.get("index", -1)) + 1,
                    "summary": str(qa.get("summary") or "Canonical skeleton identity was not proven")[:300],
                    "issues": list(qa.get("issues") or []),
                })
        if failed_qa:
            save_scenes(ws, scenes)
            details = "; ".join(
                f"scene {item['scene']}: {item['summary']}" for item in failed_qa
            )
            raise RuntimeError(
                "Still approval blocked by semantic visual QA. " + details[:900]
            )
    elif isinstance((spec or {}).get("product_reference"), dict):
        from studio_agent import visual_qa

        product = dict((spec or {}).get("product_reference") or {})
        references = [
            Path(str(image.get("path") or ""))
            for image in list(product.get("images") or [])
            if isinstance(image, dict) and str(image.get("path") or "").strip()
        ]
        failed_qa: list[dict[str, Any]] = []
        for sc in scenes:
            if idx_set is not None and sc.get("index") not in idx_set:
                continue
            sid = str(sc.get("sid") or f"b{int(sc.get('index', 0)):02d}")
            still_rel = str(sc.get("still_rel") or f"stills/{sid}.png")
            qa = visual_qa.audit_product_still(
                ws / still_rel,
                references=references,
                product_name=str(product.get("product_name") or ""),
            )
            sc["still_qa"] = qa
            if qa.get("status") != "pass" or qa.get("pass") is not True:
                sc["approved_for_video"] = False
                sc["approved_for_animation"] = False
                sc["animate"] = False
                failed_qa.append({
                    "scene": int(sc.get("index", -1)) + 1,
                    "summary": str(qa.get("summary") or "Product identity was not proven")[:300],
                    "issues": list(qa.get("issues") or []),
                })
        if failed_qa:
            save_scenes(ws, scenes)
            details = "; ".join(
                f"scene {item['scene']}: {item['summary']}" for item in failed_qa
            )
            raise RuntimeError(
                "Product still approval blocked by semantic visual QA. " + details[:900]
            )
    for sc in scenes:
        if idx_set is None or sc.get("index") in idx_set:
            sc["animate"] = bool(animate)
            sc["approved_for_video"] = True
            sc["approved_for_animation"] = bool(animate)
            changed.append(sc.get("index"))
    save_scenes(ws, scenes)
    approved_count = sum(1 for sc in scenes if sc.get("approved_for_video"))
    all_approved = bool(scenes) and approved_count == len(scenes)
    if all_approved:
        result_path = ws / "result.json"
        result: dict[str, Any] = {}
        try:
            loaded = json.loads(result_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                result = loaded
        except Exception:
            pass
        final_video = None
        for raw in (
            str(result.get("video_path") or ""),
            "skeleton_short.mp4",
            "styled_short.mp4",
            "final.mp4",
            "short.mp4",
        ):
            if not raw:
                continue
            candidate = Path(raw)
            if not candidate.is_absolute():
                candidate = ws / candidate
            try:
                if candidate.is_file() and candidate.stat().st_size > 1024:
                    final_video = candidate.resolve()
                    break
            except OSError:
                continue
        if final_video and str(result.get("status") or "").lower() in {"complete", "completed", "ready", "scenes_approved"}:
            result["status"] = "complete"
            result["job_id"] = job_id
            result["video_path"] = str(final_video)
            result["scene_count"] = len(scenes)
            result["approved_scene_count"] = approved_count
            result["animation_pending_count"] = 0
            result.pop("error", None)
            result_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
            (ws / "progress.json").write_text(json.dumps({
                "stage": "complete",
                "progress": 100,
                "detail": "Final MP4 ready.",
            }, indent=2), encoding="utf-8")
            return json.dumps({
                "ok": True,
                "job_id": job_id,
                "affected": changed,
                "animate": animate,
                "approved_for_video": changed,
                "approved_count": approved_count,
                "scene_count": len(scenes),
                "all_approved": all_approved,
                "status": "complete",
                "video_path": str(final_video),
                "note": (
                    "The final MP4 already exists, so Studio kept this job complete instead of moving it "
                    "back to scene review. Use a re-edit or regenerate flow for changes after export."
                ),
            }, indent=2)
        result.update({
            "status": "scenes_approved",
            "job_id": job_id,
            "scene_count": len(scenes),
            "approved_scene_count": approved_count,
            "animation_pending_count": sum(
                1 for sc in scenes
                if sc.get("approved_for_animation") and not sc.get("clip_rel")
            ),
        })
        result.pop("error", None)
        result_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        (ws / "progress.json").write_text(json.dumps({
            "stage": "scenes_approved",
            "progress": 85,
            "detail": "All scenes approved; ready to animate",
        }, indent=2), encoding="utf-8")
    return json.dumps({
        "ok": True,
        "job_id": job_id,
        "affected": changed,
        "animate": animate,
        "approved_for_video": changed,
        "approved_count": approved_count,
        "scene_count": len(scenes),
        "all_approved": all_approved,
        "note": (
            "All scenes are approved and ready for animation/final export."
            if all_approved
            else "Selected scenes are approved. animate=true scenes are queued for i2v selection; animate=false scenes use the still/Ken Burns path."
        ),
    }, indent=2)


def set_production_scene_duration(job_id: str, scene_index: int, duration_sec: float) -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import load_scenes, save_scenes
    scenes = load_scenes(ws)
    for sc in scenes:
        if sc.get("index") == int(scene_index):
            sc["duration_sec"] = float(duration_sec)
            save_scenes(ws, scenes)
            return json.dumps({"ok": True, "job_id": job_id, "scene_index": scene_index, "duration_sec": duration_sec}, indent=2)
    raise ValueError(f"scene {scene_index} not found")


def _spawn_shortform_background_stage(
    job_id: str,
    *,
    stage: str,
    detail: str,
    work: Any,
) -> str:
    """Run a long shortform stage off the HTTP thread (animate, finalize, expand)."""
    ws = _shortform_workspace(job_id)
    marker = ws / f".{stage}.running"
    if marker.is_file():
        try:
            marker_age = time.time() - marker.stat().st_mtime
        except OSError:
            marker_age = 0
        if marker_age >= 6 * 60 * 60:
            marker.unlink(missing_ok=True)
    try:
        descriptor = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return json.dumps({
            "status": "running",
            "job_id": job_id,
            "stage": stage,
            "idempotent_replay": True,
            "note": f"{stage.replace('_', ' ').title()} is already running. Poll job status for updates.",
        }, indent=2)
    try:
        os.write(descriptor, str(time.time()).encode("utf-8"))
    finally:
        os.close(descriptor)

    def _run() -> None:
        import traceback as _tb

        stop_hb = threading.Event()
        hb_path = ws / "heartbeat.txt"
        hb_thread = threading.Thread(
            target=_heartbeat_loop,
            args=(stop_hb, hb_path),
            daemon=True,
            name=f"hb-{stage}-{job_id}",
        )
        hb_thread.start()
        try:
            (ws / "progress.json").write_text(
                json.dumps({"stage": stage, "progress": 55, "detail": detail}, indent=2),
                encoding="utf-8",
            )
            hb_path.touch(exist_ok=True)
            work()
        except Exception as exc:
            try:
                (ws / "job.log").write_text(
                    f"{stage.upper()} FAILED {time.time()}\n{exc}\n\n{_tb.format_exc()}",
                    encoding="utf-8",
                )
            except Exception:
                pass
            try:
                (ws / "result.json").write_text(
                    json.dumps({"status": "failed", "job_id": job_id, "error": str(exc)}, indent=2),
                    encoding="utf-8",
                )
            except Exception:
                pass
        finally:
            stop_hb.set()
            try:
                marker.unlink(missing_ok=True)
            except OSError:
                pass

    try:
        threading.Thread(target=_run, daemon=False, name=f"{stage}-{job_id}").start()
    except BaseException:
        marker.unlink(missing_ok=True)
        raise
    return json.dumps({
        "status": "running",
        "job_id": job_id,
        "stage": stage,
        "note": f"{detail} Poll /api/studio-agent/jobs/{job_id}?kind=shortform for progress.",
    }, indent=2)


def spawn_animate_production_scenes(
    job_id: str,
    scene_indices: list[int] | None = None,
    max_budget_usd: float | None = None,
    *,
    user_id: str = "",
    session_id: str | None = None,
    command_id: str = "",
) -> str:
    if str(user_id or "").strip():
        return _spawn_shortform_background_stage(
            job_id,
            stage="animate",
            detail="Animating approved scenes (i2v).",
            work=lambda: execute_tool_logged(
                "animate_production_scenes",
                {
                    "job_id": job_id,
                    "scene_indices": scene_indices,
                    "max_budget_usd": max_budget_usd,
                    "command_id": command_id,
                },
                user_id=str(user_id).strip(),
                content_format="short",
                session_id=session_id,
            ),
        )
    return _spawn_shortform_background_stage(
        job_id,
        stage="animate",
        detail="Animating approved scenes (i2v).",
        work=lambda: animate_production_scenes(job_id, scene_indices=scene_indices, max_budget_usd=max_budget_usd),
    )


def spawn_finalize_production(
    job_id: str,
    *,
    captions_enabled: bool | None = None,
    caption_mode: str | None = None,
    user_id: str = "",
    session_id: str | None = None,
    command_id: str = "",
) -> str:
    preflight = shortform_finalize_preflight(job_id)
    if preflight.get("status") != "ready":
        return json.dumps(preflight, indent=2)
    if str(user_id or "").strip():
        return _spawn_shortform_background_stage(
            job_id,
            stage="compose",
            detail="Composing voice, captions, and final MP4.",
            work=lambda: execute_tool_logged(
                "finalize_production",
                {
                    "job_id": job_id,
                    "captions_enabled": captions_enabled,
                    "caption_mode": caption_mode,
                    "command_id": command_id,
                },
                user_id=str(user_id).strip(),
                content_format="short",
                session_id=session_id,
            ),
        )
    return _spawn_shortform_background_stage(
        job_id,
        stage="compose",
        detail="Composing voice, captions, and final MP4.",
        work=lambda: finalize_production(
            job_id,
            captions_enabled=captions_enabled,
            caption_mode=caption_mode,
        ),
    )


def animate_production_scenes(
    job_id: str,
    scene_indices: list[int] | None = None,
    max_budget_usd: float | None = None,
    *,
    video_model: str | None = None,
    session_id: str | None = None,
    route_revision: int | None = None,
) -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import animate_scenes_stage, load_scenes, save_scenes
    scenes = load_scenes(ws)
    if scene_indices is None:
        try:
            spec = json.loads((ws / "job_spec.json").read_text(encoding="utf-8"))
            if bool(spec.get("visual_proof_only")):
                scene_indices = [0]
        except Exception:
            if len(scenes) == 1:
                scene_indices = [int(scenes[0].get("index", 0))]
    initial_route = _repair_route_snapshot(
        session_id,
        video_model=video_model,
        route_revision=route_revision,
    )
    idx_set = set(scene_indices) if scene_indices else None
    targets: list[int] = []
    for sc in scenes:
        idx = int(sc.get("index", -1))
        if idx_set is not None and idx not in idx_set:
            continue
        if not sc.get("approved_for_video"):
            continue
        sc["animate"] = True
        sc["approved_for_animation"] = True
        if str(initial_route.get("video_model") or "").strip():
            sc["video_model"] = str(initial_route["video_model"])
            sc["media_route_revision"] = int(initial_route.get("revision") or 1)
        targets.append(idx)
    if not targets:
        raise ValueError(
            "no approved scenes to animate. Review the stills first, then approve scenes before running i2v."
        )
    save_scenes(ws, scenes)
    scene_indices = targets
    res = animate_scenes_stage(
        ws,
        indices=scene_indices,
        tier="standard",
        route_resolver=(
            lambda: _repair_route_snapshot(
                session_id,
                video_model=video_model,
                route_revision=route_revision,
            )
        ) if session_id or video_model else None,
    )
    scenes_after = load_scenes(ws)
    reported_failed = {
        int(value)
        for value in (res.get("failed") or [])
        if str(value).lstrip("-").isdigit()
    }
    failed = sorted(reported_failed | {
        int(sc.get("index", -1))
        for sc in scenes_after
        if int(sc.get("index", -1)) in set(targets) and str(sc.get("status") or "") == "error"
    })
    reported_animated = {
        int(value)
        for value in (res.get("animated") or [])
        if str(value).lstrip("-").isdigit()
    }
    animated_ok = [
        int(sc.get("index", -1))
        for sc in scenes_after
        if (
            int(sc.get("index", -1)) in set(targets)
            and int(sc.get("index", -1)) in reported_animated
            and int(sc.get("index", -1)) not in set(failed)
            and sc.get("clip_rel")
        )
    ]
    result_path = ws / "result.json"
    result: dict[str, Any] = {}
    try:
        loaded = json.loads(result_path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            result = loaded
    except Exception:
        pass
    prior_failed = {
        int(value)
        for value in (result.get("animation_failed") or [])
        if str(value).lstrip("-").isdigit()
    }
    # A repair command may invoke this tool once per selected scene.  Each
    # invocation supersedes prior state for its own targets, while failures for
    # the other selected scenes must remain visible in the shared result.
    aggregate_failed = sorted((prior_failed - set(targets)) | set(failed))
    human_failed = [index + 1 for index in aggregate_failed]
    if aggregate_failed:
        result.update({
            "status": "failed" if failed and not animated_ok else "partial",
            "job_id": job_id,
            "scene_count": len(scenes_after),
            "animated_scene_count": sum(bool(sc.get("clip_rel")) for sc in scenes_after),
            "animation_failed": aggregate_failed,
            "animation_failed_scene_numbers": human_failed,
            "error": f"Animation failed for scene(s): {human_failed}",
        })
        result_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        (ws / "progress.json").write_text(json.dumps({
            "stage": "failed" if failed and not animated_ok else "awaiting_animation_review",
            "progress": 0 if failed and not animated_ok else 88,
            "detail": result["error"],
        }, indent=2), encoding="utf-8")
    else:
        result.update({
            "status": "awaiting_animation_review" if animated_ok else str(result.get("status") or "scenes_approved"),
            "job_id": job_id,
            "scene_count": len(scenes_after),
            "animated_scene_count": sum(bool(sc.get("clip_rel")) for sc in scenes_after),
            "animation_failed": [],
            "animation_failed_scene_numbers": [],
        })
        result.pop("error", None)
        result_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        (ws / "progress.json").write_text(json.dumps({
            "stage": "awaiting_animation_review" if animated_ok else "scenes_approved",
            "progress": 88 if animated_ok else 85,
            "detail": (
                "Animation ready — review the clip in chat, then finalize."
                if animated_ok
                else "Animation did not produce clips; review stills and retry."
            ),
        }, indent=2), encoding="utf-8")
    return json.dumps({
        "ok": not failed,
        "job_id": job_id,
        "animated": res.get("animated"),
        "failed": failed,
        "max_budget_usd": max_budget_usd,
        "route": res.get("route") or initial_route,
        "route_switches": res.get("route_switches") or [],
        "note": (
            "Animation completed for approved scenes. Review the clip in the Studio chat card."
            if not failed
            else "Some scenes failed animation. Edit/regenerate those stills or retry animation before finalizing."
        ),
    }, indent=2)


def repair_production_scene_animation(
    job_id: str,
    scene_index: int,
    reason: str = "",
    *,
    video_model: str | None = None,
    session_id: str | None = None,
    route_revision: int | None = None,
) -> str:
    """Re-animate an approved still after the creator rejects the clip.

    This deliberately does not regenerate or edit the still. Natural-language
    `reason` drives the fix: artifact/morph complaints keep identity-safe motion;
    "too static / barely moved / stronger pose+VFX" critiques rewrite the i2v
    performance via apply_motion_direction, then re-animate.
    """
    ws = _shortform_workspace(job_id)
    from skeleton_ai.prompt_compose import compose_skeleton_motion_prompt, resolve_locked_outfit
    from skeleton_ai.styled_pipeline import apply_motion_direction, load_scenes, save_scenes

    idx = int(scene_index)
    scenes = load_scenes(ws)
    scene = next((item for item in scenes if int(item.get("index", -1)) == idx), None)
    if not scene:
        raise ValueError(f"scene {idx} not found")
    still_path = ws / str(scene.get("still_rel") or f"stills/{scene.get('sid', f'b{idx:02d}')}.png")
    if not still_path.is_file():
        raise ValueError(f"scene {idx + 1} still is missing; regenerate the still first")

    note = str(reason or "").strip()
    low = note.lower()
    previous_qa = scene.get("i2v_qa") if isinstance(scene.get("i2v_qa"), dict) else {}
    scene["i2v_user_rejection"] = {
        "reason": (note or "creator rejected animation")[:500],
        "previous_qa": previous_qa,
        "reported_at": time.time(),
    }

    artifact_only = any(
        term in low
        for term in (
            "artifact", "morph", "flicker", "melting", "warping", "drift",
            "turns human", "human skin", "extra limb", "extra finger", "identity",
        )
    )
    wants_stronger_motion = any(
        term in low
        for term in (
            "barely", "hardly", "static", "frozen", "idle", "didn't animate",
            "did not animate", "not animat", "more motion", "more movement",
            "stronger", "pose change", "weight shift", "parallax", "vfx",
            "background", "camera", "gesture", "actually move", "really move",
        )
    ) or not artifact_only

    motion_rewrite: dict[str, Any] = {}
    if wants_stronger_motion:
        # Creator asked for readable performance — rewrite from their words.
        motion_rewrite = apply_motion_direction(ws, idx, note or (
            "Clip is too static. Increase pose change, skeleton-sourced glass VFX, "
            "and background/camera motion while keeping identity."
        ))
        scenes = load_scenes(ws)
        scene = next((item for item in scenes if int(item.get("index", -1)) == idx), None)
        if not scene:
            raise ValueError(f"scene {idx} not found after motion rewrite")
    else:
        # Identity defect without a "more motion" ask: controlled performance, not dead freeze.
        try:
            spec = json.loads((ws / "job_spec.json").read_text(encoding="utf-8"))
        except Exception:
            spec = {}
        locked = resolve_locked_outfit(
            job_locked=str((spec or {}).get("locked_outfit") or ""),
            scene_outfit=str(scene.get("outfit") or ""),
            topic=str((spec or {}).get("topic") or ""),
            force_simple_host=True,
        )
        scene["outfit"] = locked
        scene["motion_prompt"] = compose_skeleton_motion_prompt(
            motion=(
                "Identity-preserving performance from this exact still: small weight shift, "
                "gentle torso settle, one restrained hand gesture; soft glass-shell highlight "
                "travel only; fixed framing with tiny parallax — no morph, no limb drift, "
                "no human skin, no text."
            ),
            locked_outfit=locked,
            effect_direction="soft glass-shell refraction only; no chest orbs or graphics",
            budget=1800,
        )
        motion_rewrite = {"changed": True, "mode": "identity_safe_performance"}

    scene["approved_for_video"] = True
    scene["approved_for_animation"] = True
    scene["animate"] = True
    scene.pop("motion_fallback", None)
    route = _repair_route_snapshot(
        session_id,
        video_model=video_model,
        route_revision=route_revision,
    )
    if str(route.get("video_model") or "").strip():
        scene["video_model"] = str(route["video_model"])
        scene["media_route_revision"] = int(route.get("revision") or 1)
    sid = str(scene.get("sid") or f"b{idx:02d}")
    clip = ws / "clips" / f"{sid}.mp4"
    # Keep the currently playable clip attached until its replacement has
    # completed.  animate_scenes_stage performs a transactional swap and will
    # restore this artifact if the provider or QA path fails.
    if not clip.is_file() or clip.stat().st_size <= 1024:
        scene["status"] = "still_ready"
        scene["clip_rel"] = None
    save_scenes(ws, scenes)

    raw = animate_production_scenes(
        job_id,
        [idx],
        video_model=str(route.get("video_model") or video_model or "") or None,
        session_id=session_id,
        route_revision=int(route.get("revision") or 1),
    )
    result = json.loads(raw or "{}")
    result["repair_kind"] = "animation_only"
    result["still_preserved"] = True
    result["user_reason"] = (note or "animation critique")[:500]
    result["motion_rewrite"] = motion_rewrite
    return json.dumps(result, indent=2)


_SCENE_CORRESPONDENCE_STILL_ISSUES = frozenset({
    "narrative_mismatch",
    "duplicate_adjacent",
    "generic_staging",
    "layout_artifact",
    "identity_drift",
    "text_artifact",
    "artifact",
})


def _scene_correspondence_motion_only(report: dict[str, Any] | None) -> bool:
    """Return True only for an explicit animation-only QA finding.

    Scene-correspondence QA is primarily a *still* gate.  Structured narrative
    or staging issues always require a restage, even when the prose also uses
    words such as "emotional".  The previous substring check for ``"motion"``
    accidentally matched ``"emotional"`` and sent bad stills down the i2v-only
    path.
    """

    payload = report if isinstance(report, dict) else {}
    issues = {
        str(value or "").strip().lower()
        for value in (payload.get("issues") or [])
        if str(value or "").strip()
    }
    if issues & _SCENE_CORRESPONDENCE_STILL_ISSUES:
        return False
    # Unknown structured failures are not safe to downgrade to animation-only.
    if issues:
        return False

    text = " ".join(
        str(payload.get(key) or "")
        for key in ("summary", "recommended_restage")
    ).lower()
    still_signal = re.search(
        r"\b(?:narrative|generic|staging|composition|location|setting|opening\s+pose|"
        r"frame[- ]zero|body\s+language|duplicate|adjacent|wrong\s+room|symmetr(?:y|ical)|"
        r"single\s+skeleton|two\s+skeletons|emotional\s+beat|pose)\b",
        text,
    )
    motion_signal = re.search(
        r"\b(?:animation|animated|multi[- ]second\s+motion|motion\s+over\s+time|"
        r"camera\s+push|background\s+parallax|vfx\s+(?:travel|movement)|"
        r"weight\s+shift\s+over\s+time|head\s+snap\s+sequence|"
        r"lacks?\s+(?:dynamic\s+)?movement)\b",
        text,
    )
    return bool(motion_signal and not still_signal)


def _reconcile_audit_repair_state(
    workspace: Path,
    *,
    job_id: str,
    selected: list[int],
    failed: list[int],
    reports: list[dict[str, Any]],
    repaired_stills: list[int],
    repaired_animations: list[int],
) -> None:
    """Replace stale production errors with the outcome of this repair run."""

    result_path = workspace / "result.json"
    try:
        loaded = json.loads(result_path.read_text(encoding="utf-8"))
        result = dict(loaded) if isinstance(loaded, dict) else {}
    except Exception:
        result = {}
    try:
        from skeleton_ai.styled_pipeline import load_scenes

        scenes = load_scenes(workspace)
    except Exception:
        scenes = []
    selected_set = set(selected)
    failed_set = set(failed)
    current_animation_failures = {
        int(row.get("scene_index"))
        for row in reports
        if row.get("status") == "failed" and row.get("failure_stage") == "animation"
    }
    prior_animation_failures = {
        int(value)
        for value in (result.get("animation_failed") or [])
        if str(value).lstrip("-").isdigit()
    }
    animation_failed = sorted((prior_animation_failures - selected_set) | current_animation_failures)
    still_or_qa_failed = sorted(failed_set - current_animation_failures)
    human_animation = [index + 1 for index in animation_failed]
    human_still = [index + 1 for index in still_or_qa_failed]
    failure_details = [
        {
            "scene_index": int(row.get("scene_index", -1)),
            "scene_number": int(row.get("scene_index", -1)) + 1,
            "stage": str(row.get("failure_stage") or "unknown"),
            "error": str((row.get("repair") or {}).get("error") or row.get("error") or "repair failed")[:500],
        }
        for row in reports
        if row.get("status") == "failed"
    ]
    if human_still and human_animation:
        error = (
            f"Scene repair/QA blocked scene(s): {human_still}; "
            f"animation failed for scene(s): {human_animation}."
        )
    elif human_still:
        error = f"Scene repair/QA blocked scene(s): {human_still}; animation was not started for those scenes."
    elif human_animation:
        error = f"Animation failed for scene(s): {human_animation}."
    else:
        error = ""
    has_clip = any(bool(scene.get("clip_rel")) for scene in scenes)
    status = "failed" if error else ("awaiting_animation_review" if has_clip else "awaiting_scene_review")
    result.update({
        "job_id": job_id,
        "status": status,
        "scene_count": len(scenes),
        "animated_scene_count": sum(bool(scene.get("clip_rel")) for scene in scenes),
        "animation_failed": animation_failed,
        "animation_failed_scene_numbers": human_animation,
        "repair_selected": selected,
        "repair_failed": sorted(failed_set),
        "repair_failed_scene_numbers": [index + 1 for index in sorted(failed_set)],
        "repair_failure_details": failure_details,
        "repair_status": "failed" if failed_set else "complete",
        "repaired_stills": sorted(set(repaired_stills)),
        "repaired_animations": sorted(set(repaired_animations)),
    })
    if error:
        result["error"] = error
    else:
        result.pop("error", None)
    result_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    (workspace / "progress.json").write_text(
        json.dumps({
            "stage": status,
            "progress": 0 if error else (88 if has_clip else 80),
            "detail": error or "Selected scenes passed repair QA and are ready for review.",
        }, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def audit_and_repair_production_scenes(
    job_id: str,
    scene_indices: list[int],
    reason: str = "",
    *,
    image_model_id: str | None = None,
    video_model: str | None = None,
    session_id: str | None = None,
    route_revision: int | None = None,
) -> str:
    """Audit selected stills + clips and repair only failed assets.

    This deliberately has two independent gates: artifact/identity QA and
    narrative correspondence QA.  A clean skeleton in the wrong room or a
    repeated pose is still a failed scene, not a pass.
    """
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import load_scenes, save_scenes
    from studio_agent import visual_qa

    scenes = load_scenes(ws)
    available = {int(scene.get("index", idx)) for idx, scene in enumerate(scenes)}
    selected = [
        idx for idx in dict.fromkeys(int(value) for value in (scene_indices or []))
        if idx in available
    ]
    if not selected:
        raise ValueError("no valid scenes were selected for artifact QA")
    try:
        spec = json.loads((ws / "job_spec.json").read_text(encoding="utf-8"))
    except Exception:
        spec = {}
    locked_outfit = str((spec or {}).get("locked_outfit") or "")
    skeleton_mode = "skeleton" in str((spec or {}).get("render_style") or "").lower()
    reference = visual_qa._workspace_skeleton_reference(ws) if skeleton_mode else None

    def _scene_contract(row: dict[str, Any]) -> str:
        """Still correspondence contract — location + opening beat only.

        Never include motion_prompt: multi-second performance (weight shifts,
        shrugs, camera pushes) belongs to i2v and must not fail/rebuild stills.
        """
        action = str(row.get("scene_action") or row.get("action") or "").strip()
        # Drop explicit performance / i2v instructions from the still contract.
        action = re.sub(
            r"(?is)\b(?:PERFORMANCE|MOTION|VFX|SILENT|i2v)\b.*$",
            "",
            action,
        ).strip(" .;")
        action = re.sub(
            r"(?i)\b(?:weight\s+shift|torso\s+twist|quarter[- ]turn|shrug|parallax|"
            r"camera\s+push|head\s+tilt[- ]?back|snap[- ]forward)[^.|;]*[.|;]?",
            "",
            action,
        )
        return " ".join(
            part
            for part in (
                str(row.get("narration") or "").strip(),
                action,
            )
            if part
        )[:900]

    def _scene_still(row: dict[str, Any]) -> Path:
        scene_id = str(row.get("sid") or f"b{int(row.get('index') or 0):02d}")
        return ws / str(row.get("still_rel") or f"stills/{scene_id}.png")

    reports: list[dict[str, Any]] = []
    repaired_stills: list[int] = []
    repaired_animations: list[int] = []
    attempted_still_repairs: list[int] = []
    attempted_animation_repairs: list[int] = []
    route_history: list[dict[str, Any]] = []
    failed: list[int] = []
    for index in selected:
        scene_route = _repair_route_snapshot(
            session_id,
            image_model_id=image_model_id,
            video_model=video_model,
            route_revision=route_revision,
        )
        route_history.append({"scene_index": index, **scene_route})
        scenes = load_scenes(ws)
        scene = next((row for row in scenes if int(row.get("index", -1)) == index), None)
        if not scene:
            failed.append(index)
            reports.append({
                "scene_index": index,
                "status": "failed",
                "failure_stage": "still",
                "error": "scene missing",
            })
            continue
        sid = str(scene.get("sid") or f"b{index:02d}")
        still = ws / str(scene.get("still_rel") or f"stills/{sid}.png")
        clip = ws / str(scene.get("clip_rel") or f"clips/{sid}.mp4")
        ordered = sorted(scenes, key=lambda row: int(row.get("index", 0)))
        position = next((i for i, row in enumerate(ordered) if int(row.get("index", -1)) == index), -1)
        previous = ordered[position - 1] if position > 0 else None
        following = ordered[position + 1] if 0 <= position < len(ordered) - 1 else None
        if skeleton_mode:
            still_qa = visual_qa.audit_skeleton_still(
                still, reference=reference, locked_outfit=locked_outfit or str(scene.get("outfit") or ""), force=True,
                cast_count=int(scene.get("cast_count") or (spec or {}).get("cast_count") or 1),
            )
        else:
            still_qa = visual_qa.audit_generic_still(
                still,
                scene_contract=" ".join(str(scene.get(key) or "") for key in ("prompt", "scene_action", "narration")),
                force=True,
            )
        scene["still_qa"] = still_qa
        save_scenes(ws, scenes)
        if still_qa.get("pass") is not True:
            attempted_still_repairs.append(index)
            try:
                repair = json.loads(regenerate_production_scene(
                    job_id,
                    index,
                    reason=reason or "Fresh visual QA detected still artifacting",
                    animate=True,
                    image_model_id=str(scene_route.get("image_model_id") or image_model_id or "") or None,
                    video_model=str(scene_route.get("video_model") or video_model or "") or None,
                    session_id=session_id,
                    route_revision=int(scene_route.get("revision") or 1),
                ))
            except Exception as exc:
                repair = {"ok": False, "error": str(exc)}
            if repair.get("ok"):
                repaired_stills.append(index)
            else:
                failed.append(index)
            reports.append({
                "scene_index": index,
                "status": "repaired_still" if repair.get("ok") else "failed",
                "still_qa": still_qa,
                "repair": repair,
                "failure_stage": None if repair.get("ok") else "still",
            })
            continue

        correspondence_qa = visual_qa.audit_scene_correspondence(
            still,
            scene_contract=_scene_contract(scene),
            previous_still=_scene_still(previous) if previous else None,
            previous_contract=_scene_contract(previous) if previous else "",
            next_still=_scene_still(following) if following else None,
            next_contract=_scene_contract(following) if following else "",
        )
        scene["scene_correspondence_qa"] = correspondence_qa
        save_scenes(ws, scenes)
        if correspondence_qa.get("pass") is not True:
            motion_only_fail = _scene_correspondence_motion_only(correspondence_qa)
            # Motion belongs in i2v. Do not rebuild the still for a performance brief.
            if motion_only_fail:
                attempted_animation_repairs.append(index)
                try:
                    repair = json.loads(repair_production_scene_animation(
                        job_id,
                        index,
                        reason or str(correspondence_qa.get("summary") or "still is frame-zero; strengthen silent animation performance"),
                        video_model=str(scene_route.get("video_model") or video_model or "") or None,
                        session_id=session_id,
                        route_revision=int(scene_route.get("revision") or 1),
                    ))
                except Exception as exc:
                    repair = {"ok": False, "error": str(exc)}
                if repair.get("ok"):
                    repaired_animations.append(index)
                else:
                    failed.append(index)
                reports.append({
                    "scene_index": index,
                    "status": "repaired_animation" if repair.get("ok") else "failed",
                    "correspondence_qa": correspondence_qa,
                    "still_qa": still_qa,
                    "repair": repair,
                    "failure_stage": None if repair.get("ok") else "animation",
                    "note": "Correspondence cited motion/performance — preserved still and re-animated",
                })
                continue
            restage = str(correspondence_qa.get("recommended_restage") or "").strip()
            repair_reason = "Scene correspondence QA failed: " + str(correspondence_qa.get("summary") or "scene does not tell its own beat")
            if restage:
                repair_reason += ". Re-stage as: " + restage
            attempted_still_repairs.append(index)
            try:
                repair = json.loads(regenerate_production_scene(
                    job_id,
                    index,
                    reason=repair_reason[:1100],
                    restage_direction=restage,
                    animate=True,
                    image_model_id=str(scene_route.get("image_model_id") or image_model_id or "") or None,
                    video_model=str(scene_route.get("video_model") or video_model or "") or None,
                    session_id=session_id,
                    route_revision=int(scene_route.get("revision") or 1),
                ))
            except Exception as exc:
                repair = {"ok": False, "error": str(exc)}
            if repair.get("ok"):
                repaired_stills.append(index)
            else:
                failed.append(index)
            reports.append({
                "scene_index": index,
                "status": "repaired_correspondence" if repair.get("ok") else "failed",
                "scene_contract": _scene_contract(scene)[:500],
                "correspondence_qa": correspondence_qa,
                "repair": repair,
                "failure_stage": None if repair.get("ok") else "still",
            })
            continue

        if skeleton_mode:
            clip_qa = visual_qa.audit_skeleton_clip(
                clip, still=still, locked_outfit=locked_outfit or str(scene.get("outfit") or ""), force=True,
                cast_count=int(scene.get("cast_count") or (spec or {}).get("cast_count") or 1),
            )
        else:
            clip_qa = visual_qa.audit_generic_clip(
                clip,
                scene_contract=" ".join(str(scene.get(key) or "") for key in ("prompt", "scene_action", "narration")),
                force=True,
            )
        scenes = load_scenes(ws)
        current = next((row for row in scenes if int(row.get("index", -1)) == index), None)
        if current is not None:
            current["i2v_qa"] = clip_qa
            save_scenes(ws, scenes)
        if clip_qa.get("pass") is not True:
            attempted_animation_repairs.append(index)
            try:
                repair = json.loads(repair_production_scene_animation(
                    job_id,
                    index,
                    reason or "Fresh sampled-frame QA detected animation artifacting",
                    video_model=str(scene_route.get("video_model") or video_model or "") or None,
                    session_id=session_id,
                    route_revision=int(scene_route.get("revision") or 1),
                ))
            except Exception as exc:
                repair = {"ok": False, "error": str(exc)}
            if repair.get("ok"):
                repaired_animations.append(index)
            else:
                failed.append(index)
            reports.append({
                "scene_index": index,
                "status": "repaired_animation" if repair.get("ok") else "failed",
                "correspondence_qa": correspondence_qa,
                "still_qa": still_qa,
                "clip_qa": clip_qa,
                "repair": repair,
                "failure_stage": None if repair.get("ok") else "animation",
            })
        else:
            reports.append({
                "scene_index": index,
                "status": "passed",
                "correspondence_qa": correspondence_qa,
                "still_qa": still_qa,
                "clip_qa": clip_qa,
            })
    failed = sorted(set(failed))
    repaired_stills = sorted(set(repaired_stills))
    repaired_animations = sorted(set(repaired_animations))
    _reconcile_audit_repair_state(
        ws,
        job_id=job_id,
        selected=selected,
        failed=failed,
        reports=reports,
        repaired_stills=repaired_stills,
        repaired_animations=repaired_animations,
    )
    return json.dumps({
        "ok": not failed,
        "job_id": job_id,
        "audited": selected,
        "passed_without_changes": [row["scene_index"] for row in reports if row.get("status") == "passed"],
        "repaired_stills": repaired_stills,
        "repaired_animations": repaired_animations,
        "attempted_still_repairs": sorted(set(attempted_still_repairs)),
        "attempted_animation_repairs": sorted(set(attempted_animation_repairs)),
        "failed": failed,
        "routes": route_history,
        "scenes": reports,
        "note": "Fresh artifact, identity, animation, and narrative-correspondence QA completed; only failed scenes were regenerated.",
    }, indent=2)


def _shortform_job_spec_options(ws: Path) -> dict[str, Any]:
    try:
        spec = json.loads((Path(ws) / "job_spec.json").read_text(encoding="utf-8"))
        if isinstance(spec, dict):
            caption_mode = str(spec.get("caption_mode") or "word").strip().lower()
            captions_enabled = bool(spec.get("captions_enabled", True))
            if caption_mode == "off":
                captions_enabled = False
                caption_mode = "off"
            elif caption_mode not in {"word", "single_word", "one_word"}:
                caption_mode = "word"
            migrated = _migrate_shortform_voice_options(
                {
                    "voice_id": str(spec.get("voice_id") or "").strip(),
                    "voice_provider": str(spec.get("voice_provider") or "").strip(),
                },
                render_style=str(spec.get("render_style") or "cinematic"),
            )
            voice_id = str(migrated.get("voice_id") or "").strip()
            voice_provider = str(migrated.get("voice_provider") or "fal").strip()
            if voice_provider == "fal":
                try:
                    spec["voice_id"] = voice_id
                    spec["voice_provider"] = voice_provider
                    (Path(ws) / "job_spec.json").write_text(json.dumps(spec, indent=2), encoding="utf-8")
                except Exception:
                    pass
            return {
                "watermark_text": (str(spec.get("watermark_text") or "Studio").strip() or "Studio")[:48],
                "captions_enabled": captions_enabled,
                "caption_mode": caption_mode,
                "sfx_enabled": bool(spec.get("sfx_enabled", False)),
                "sound_design_brief": str(spec.get("sound_design_brief") or "").strip(),
                "background_music": str(spec.get("background_music") or "off").strip() or "off",
                "voice_id": voice_id or None,
                "voice_provider": voice_provider or "fal",
            }
    except Exception:
        pass
    return {
        "watermark_text": "Studio",
        "captions_enabled": True,
        "caption_mode": "word",
        "sfx_enabled": False,
        "sound_design_brief": "",
        "background_music": "off",
    }


def _sync_job_spec_caption_prefs(
    ws: Path,
    *,
    captions_enabled: bool | None = None,
    caption_mode: str | None = None,
) -> dict[str, Any]:
    opts = _shortform_job_spec_options(ws)
    if captions_enabled is not None:
        opts["captions_enabled"] = bool(captions_enabled)
        opts["caption_mode"] = "off" if not captions_enabled else str(caption_mode or "word")
    elif caption_mode is not None:
        mode = str(caption_mode or "").strip().lower()
        opts["caption_mode"] = "off" if mode == "off" else "word"
        opts["captions_enabled"] = opts["caption_mode"] != "off"
    try:
        spec_path = Path(ws) / "job_spec.json"
        spec = {}
        if spec_path.exists():
            loaded = json.loads(spec_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                spec = loaded
        spec["captions_enabled"] = bool(opts.get("captions_enabled", True))
        spec["caption_mode"] = "off" if not spec["captions_enabled"] else str(opts.get("caption_mode") or "word")
        spec_path.write_text(json.dumps(spec, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass
    return opts


def _apply_caption_instruction_to_options(ws: Path, instruction: str, opts: dict[str, Any]) -> dict[str, Any]:
    text = (instruction or "").lower()
    if any(mark in text for mark in ("no captions", "captions off", "without captions", "remove captions")):
        opts["captions_enabled"] = False
        opts["caption_mode"] = "off"
    elif any(mark in text for mark in ("one word", "single word", "word-by-word", "each word", "every word")):
        opts["captions_enabled"] = True
        opts["caption_mode"] = "word"
    try:
        spec_path = Path(ws) / "job_spec.json"
        spec = {}
        if spec_path.exists():
            loaded = json.loads(spec_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                spec = loaded
        spec["watermark_text"] = opts.get("watermark_text") or "Studio"
        spec["captions_enabled"] = bool(opts.get("captions_enabled", True))
        spec["caption_mode"] = "off" if not spec["captions_enabled"] else str(opts.get("caption_mode") or "word")
        spec["sfx_enabled"] = bool(opts.get("sfx_enabled", False))
        spec["sound_design_brief"] = str(opts.get("sound_design_brief") or "")
        spec["background_music"] = str(opts.get("background_music") or "off")
        spec_path.write_text(json.dumps(spec, indent=2), encoding="utf-8")
    except Exception:
        pass
    return opts


def shortform_finalize_preflight(job_id: str) -> dict[str, Any]:
    """Fail closed when a requested short-form animation is not durable yet."""
    ws = _shortform_workspace(job_id)
    try:
        from skeleton_ai.styled_pipeline import load_scenes

        scenes = load_scenes(ws)
    except Exception as exc:
        return {
            "status": "finalize_preflight_unavailable",
            "job_id": job_id,
            "error": f"Studio could not inspect requested animation clips: {str(exc)[:300]}",
            "note": "Finalize did not start. Retry after the scene workspace is readable.",
        }

    pending_animated: list[int] = []
    workspace_root = ws.resolve()
    for scene in scenes:
        if not scene.get("animate"):
            continue
        try:
            scene_index = int(scene.get("index", -1))
        except (TypeError, ValueError):
            scene_index = -1
        clip_rel = str(scene.get("clip_rel") or f"clips/{scene.get('sid')}.mp4")
        try:
            clip_path = (ws / clip_rel).resolve()
            clip_path.relative_to(workspace_root)
            clip_ready = clip_path.is_file() and clip_path.stat().st_size > 0
        except (OSError, ValueError):
            clip_ready = False
        if not clip_ready and scene_index >= 0:
            pending_animated.append(scene_index)

    if pending_animated:
        return {
            "status": "awaiting_animation",
            "job_id": job_id,
            "pending_animated_scenes": pending_animated,
            "note": (
                "These scenes are marked for animation but do not have i2v clips yet. "
                "Run animate_production_scenes first, then finalize_production. "
                "Studio will not silently downgrade requested animation into still-only video."
            ),
        }
    return {"status": "ready", "job_id": job_id}


def finalize_production(
    job_id: str,
    *,
    captions_enabled: bool | None = None,
    caption_mode: str | None = None,
) -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import finalize_stage
    # Try to pick up a reedit_instruction sidecar if this finalize is part of a re-edit flow
    reedit = None
    try:
        p = ws / "reedit_instruction.txt"
        if p.exists():
            reedit = p.read_text(encoding="utf-8")
    except Exception:
        pass
    opts = _sync_job_spec_caption_prefs(
        ws,
        captions_enabled=captions_enabled,
        caption_mode=caption_mode,
    )
    preflight = shortform_finalize_preflight(job_id)
    if preflight.get("status") != "ready":
        return json.dumps(preflight, indent=2)
    try:
        # Visual QA gate: block only when explicitly required (fail-open for launch stability).
        qa_required = os.getenv("STUDIO_FINALIZE_QA_REQUIRED", "false").strip().lower() in {"1", "true", "yes"}
        try:
            from studio_agent import visual_qa

            vq = visual_qa.analyze_shortform_workspace(ws)
            block, reason = visual_qa.should_block_publish(vq)
            if block and qa_required:
                return json.dumps({
                    "status": "visual_qa_failed",
                    "job_id": job_id,
                    "visual_qa": vq,
                    "error": reason or "Visual QA failed",
                    "note": (
                        "Finalize blocked: visual QA detected identity/prompt/background failures. "
                        "Regenerate failing stills or fix wardrobe locks, re-animate if needed, then finalize again."
                    ),
                }, indent=2)
        except Exception as qa_exc:
            if qa_required:
                return json.dumps({
                    "status": "visual_qa_unavailable",
                    "job_id": job_id,
                    "error": f"Visual QA could not run: {str(qa_exc)[:300]}",
                    "note": (
                        "Finalize blocked because Studio could not prove visual identity safety. "
                        "No artifacted clip will be marked ready while QA is unavailable."
                    ),
                }, indent=2)
    except Exception as inspect_exc:
        if os.getenv("STUDIO_FINALIZE_QA_REQUIRED", "false").strip().lower() in {"1", "true", "yes"}:
            return json.dumps({
                "status": "visual_qa_unavailable",
                "job_id": job_id,
                "error": f"Scene inspection failed before finalize: {str(inspect_exc)[:300]}",
                "note": "Finalize blocked until Studio can inspect every requested animation clip.",
            }, indent=2)
    voice_id = opts.pop("voice_id", None)
    opts.pop("voice_provider", None)
    from skeleton_ai.voice_auto import AutoVoiceClient

    el = AutoVoiceClient(provider="fal_only")
    result = finalize_stage(
        ws,
        tier="standard",
        reedit_instruction=reedit,
        voice_id=voice_id,
        el=el,
        **opts,
    )
    status = str(result.get("status") or "complete").lower()
    return json.dumps({
        "status": status if status in {"complete", "completed", "ready"} else "running",
        "job_id": job_id,
        "video_path": result.get("video_path"),
        "mp4_url": f"/api/studio-agent/jobs/{job_id}/media?kind=shortform" if result.get("video_path") else None,
        "download_url": f"/api/studio-agent/jobs/{job_id}/media?kind=shortform" if result.get("video_path") else None,
        "animated_scenes": result.get("animated_scenes"),
        "sound_design": result.get("sound_design"),
        "final_audio_path": result.get("final_audio_path"),
        "watermark_text": opts.get("watermark_text"),
        "captions_enabled": opts.get("captions_enabled"),
        "caption_mode": "word" if opts.get("captions_enabled") else "off",
        "note": "Finalize complete. The Studio UI can display/download the MP4." if result.get("video_path") else "Finalize running. Poll job status until complete for MP4.",
    }, indent=2)


def re_edit_production(job_id: str, instruction: str, kind: str = "shortform") -> str:
    """Preferred tool for 're-edit this video', 'fix the pacing/CTA/story on the one you just showed me', reply-to re-edit flows, etc.

    Takes the *existing* production (the exact video + stills + clips the user already saw for this job_id),
    inspects its scenes, applies the natural language re-edit instruction surgically (timing, captions, subscribe CTA placement,
    VO lockstep, story beat emphasis, packaging), and re-finalizes a new improved MP4 + package.txt **without** regenerating
    all the underlying visuals from scratch unless the instruction explicitly requires redrawing specific scenes.

    The LLM should have already (or will) used list_production_scenes + optional targeted edit_production_scene_still /
    set_production_scene_duration on the same job_id before or after calling this.

    This is the correct path for the reply-to "re-edit the same video it made for me" use case so the user gets
    a properly re-edited version of *that* video, not a brand new generation.
    """
    instruction = (instruction or "").strip()
    if not instruction:
        return json.dumps({"error": "instruction is required for re_edit_production"}, indent=2)

    is_long = str(kind or "").lower().startswith("long")

    if is_long:
        # Longform path: load state, mark reedit, drive the longform finalize equivalent
        from long_form import pipeline as lf
        st = lf.load_state(job_id) or {}
        st["reedit_instruction"] = instruction
        st["reedit_of"] = st.get("reedit_of") or job_id
        lf.save_state(job_id, st)
        # The longform finalize will pick up the instruction for re-trim / CTA / timestamps etc.
        # For now we surface the instruction and let the caller / pipeline use list_longform_scenes + finalize.
        return json.dumps({
            "status": "reedit_marked",
            "job_id": job_id,
            "kind": "longform",
            "instruction": instruction[:300],
            "note": "Re-edit instruction recorded for this longform job. Use list_longform_scenes then the longform finalize tools to produce the re-edited version while keeping prior chapter stills/clips.",
        }, indent=2)

    # Shortform: write the instruction sidecar so finalize_stage (and future pipeline logic) can see the re-edit intent
    ws = _shortform_workspace(job_id)
    try:
        (ws / "reedit_instruction.txt").write_text(instruction, encoding="utf-8")
    except Exception:
        pass

    # Drive a re-finalize on the *existing* workspace (re-uses stills, existing clips, scenes.json).
    # The per-scene VO + trim_with_captions + CTA logic inside finalize will produce the "properly re-edited" video.
    from skeleton_ai.styled_pipeline import finalize_stage
    opts = _shortform_job_spec_options(ws)
    opts = _apply_caption_instruction_to_options(ws, instruction, opts)
    voice_id = opts.pop("voice_id", None)
    opts.pop("voice_provider", None)
    from skeleton_ai.voice_auto import AutoVoiceClient

    el = AutoVoiceClient(provider="fal_only")
    result = finalize_stage(
        ws,
        tier="standard",
        reedit_instruction=instruction,
        voice_id=voice_id,
        el=el,
        **opts,
    )
    status = str(result.get("status") or "complete").lower()

    return json.dumps({
        "status": status if status in {"complete", "completed", "ready"} else "running",
        "job_id": job_id,
        "kind": "shortform",
        "video_path": result.get("video_path"),
        "mp4_url": f"/api/studio-agent/jobs/{job_id}/media?kind=shortform" if result.get("video_path") else None,
        "download_url": f"/api/studio-agent/jobs/{job_id}/media?kind=shortform" if result.get("video_path") else None,
        "sound_design": result.get("sound_design"),
        "final_audio_path": result.get("final_audio_path"),
        "watermark_text": opts.get("watermark_text"),
        "captions_enabled": opts.get("captions_enabled"),
        "caption_mode": "word" if opts.get("captions_enabled") else "off",
        "note": "Re-edit complete (re-used prior stills/clips from the video the user replied to). The new MP4 is ready in the Studio UI." if result.get("video_path") else "Re-edit is still running. Poll job status until complete for MP4.",
    }, indent=2)


# Lightweight long-form scene helpers (longform has chapter gates + regenerate; we expose
# enough for the agent to list, re-gen specific stills with the existing machinery,
# and advise on finalize after the user has the desired stills).
def list_longform_scenes(job_id: str) -> str:
    from long_form import pipeline as lf
    st = lf.load_state(job_id) or {}
    scenes_out: list[dict[str, Any]] = []
    records = list(st.get("scene_briefs") or [])
    if records:
        for row in records:
            if not isinstance(row, dict):
                continue
            brief = row.get("brief") if isinstance(row.get("brief"), dict) else {}
            g = int(row.get("global_idx") or 0)
            still = lf.job_still_path(job_id, g)
            scenes_out.append({
                "chapter": int(row.get("chapter_index") or 0),
                "local": int(row.get("local_idx") or 0),
                "global": g,
                "narration": str(brief.get("narration") or ""),
                "prompt": str(brief.get("scene_prompt") or ""),
                "duration_sec": float(brief.get("duration_target_sec") or 0.0),
                "still_exists": bool(still and still.is_file()),
                "still_url": f"/api/studio-agent/jobs/{job_id}/still/{g}?kind=longform",
            })
    else:
        chapters_path = lf._chapters_path(job_id)
        try:
            chapters = list((json.loads(chapters_path.read_text(encoding="utf-8")) or {}).get("chapters") or [])
        except Exception:
            chapters = []
        scenes_per_chapter = len(chapters[0].get("scene_prompts") or []) if chapters else 0
        for ch in chapters:
            ch_idx = int(ch.get("chapter_index") or 0)
            prompts = list(ch.get("scene_prompts") or [])
            narration = str(ch.get("narration") or "")
            for local, prompt in enumerate(prompts):
                g = ch_idx * scenes_per_chapter + local
                still = lf.job_still_path(job_id, g)
                scenes_out.append({
                    "chapter": ch_idx,
                    "local": local,
                    "global": g,
                    "narration": narration,
                    "prompt": str(prompt),
                    "still_exists": bool(still and still.is_file()),
                    "still_url": f"/api/studio-agent/jobs/{job_id}/still/{g}?kind=longform",
                })
    manifest = lf.longform_scene_manifest(job_id)
    return json.dumps({
        "job_id": job_id,
        "phase": st.get("phase"),
        "manifest": manifest,
        "scenes": sorted(scenes_out, key=lambda row: int(row.get("global") or 0)),
    }, indent=2)


def _longform_text_billing(client: Any, args: dict[str, Any]) -> dict[str, Any]:
    usage = dict(getattr(client, "last_usage", {}) or {})
    prompt_tokens = max(0, int(usage.get("prompt_tokens") or 0))
    completion_tokens = max(0, int(usage.get("completion_tokens") or 0))
    prompt_ppm = float(args.get("_billing_prompt_price_per_m") or 0.0)
    completion_ppm = float(args.get("_billing_completion_price_per_m") or 0.0)
    usage_reported = bool(prompt_tokens or completion_tokens)
    provider_usd = 0.0
    if usage_reported:
        import unified_credits as uc

        provider_usd = uc.openrouter_usd(
            {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
            },
            prompt_ppm,
            completion_ppm,
        )
    return {
        "provider": str(getattr(client, "last_provider", "") or "unknown"),
        "model": str(getattr(client, "last_effective_model", "") or args.get("model") or ""),
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "provider_usd": round(float(provider_usd or 0.0), 6),
        "usage_reported": usage_reported,
    }


def generate_longform_outline_logged(args: dict[str, Any]) -> str:
    """Run one outline pass after the logged boundary has held credits."""

    from long_form import pipeline as lf_pipeline
    from long_form.prompts.channels import get_channel
    from long_form.scripting import generate_outline
    from long_form.text_client import StudioTextClient

    channel_key = str(args.get("channel_key") or "").strip()
    topic = str(args.get("topic") or "").strip()
    if not topic:
        raise ValueError("topic is required")
    channel = get_channel(channel_key)
    target_minutes = int(args.get("target_minutes") or channel.get("default_minutes") or 15)
    client = StudioTextClient(model=str(args.get("model") or "").strip() or None)
    outline = generate_outline(
        client,
        str(channel.get("system_prompt") or ""),
        topic=topic,
        target_minutes=target_minutes,
        catalyst_context=str(args.get("combined_context") or ""),
        title_template_block=str(args.get("title_template_block") or ""),
    )
    if isinstance(outline, dict):
        outline["chat_model"] = client.model
        desc_tail = str(channel.get("description_tail") or "").strip()
        if desc_tail:
            existing = str(outline.get("description") or "").rstrip()
            if existing and desc_tail not in existing:
                outline["description"] = existing + "\n\n" + desc_tail.lstrip()
            elif not existing:
                outline["description"] = desc_tail.lstrip()
    try:
        cost_estimate = lf_pipeline.compute_render_cost(channel, outline)
    except Exception:
        cost_estimate = None
    return json.dumps(
        {
            "channel_key": channel_key,
            "topic": topic,
            "target_minutes": target_minutes,
            "catalyst_context_used": bool(args.get("catalyst_context_used")),
            "references_used": bool(args.get("references_used")),
            "title_template_enforced": bool(str(args.get("title_template_block") or "").strip()),
            "outline": outline,
            "cost_estimate": cost_estimate,
            "billing": _longform_text_billing(client, args),
        },
        indent=2,
        ensure_ascii=False,
    )


def expand_longform_chapter_logged(args: dict[str, Any]) -> str:
    """Run one paid chapter expansion after pricing and credit preflight."""

    from long_form.prompts.channels import get_channel
    from long_form.scripting import expand_chapter
    from long_form.text_client import StudioTextClient

    channel_key = str(args.get("channel_key") or "").strip()
    channel = get_channel(channel_key)
    chapter = args.get("chapter") if isinstance(args.get("chapter"), dict) else {}
    client = StudioTextClient(model=str(args.get("model") or "").strip() or None)
    beats = expand_chapter(
        client,
        str(channel.get("system_prompt") or ""),
        outline_title=str(args.get("outline_title") or ""),
        chapter=dict(chapter),
        fps=int(channel.get("fps") or 30),
    )
    return json.dumps(
        {
            "channel_key": channel_key,
            "chapter_index": int(chapter.get("index", 0) or 0),
            "beats": beats,
            "billing": _longform_text_billing(client, args),
        },
        indent=2,
        ensure_ascii=False,
    )


def regenerate_longform_still(job_id: str, scene_idx: int, reason: str = "") -> str:
    """Regenerate one long-form still. Artifact complaints use the short-prompt contract."""
    from pathlib import Path

    from long_form import pipeline as lf
    from studio_agent.visual_fix_contract import artifact_fix_plan, is_visual_artifact_complaint

    try:
        if is_visual_artifact_complaint(reason):
            topic = ""
            outfit = "no clothing"
            try:
                d = lf._job_dir(job_id) if hasattr(lf, "_job_dir") else None
                if d is not None and (Path(d) / "state.json").is_file():
                    state = json.loads((Path(d) / "state.json").read_text(encoding="utf-8"))
                    topic = str(state.get("topic") or state.get("title") or "")
                    outfit = str(state.get("locked_outfit") or outfit)
            except Exception:
                pass
            plan = artifact_fix_plan(
                topic=topic,
                user_feedback=reason,
                outfit=outfit,
                aspect_ratio="16:9",
            )
            new_path = lf.regenerate_still(job_id, int(scene_idx), new_prompt=plan["still_prompt"])
            version = int(new_path.stat().st_mtime)
            return json.dumps({
                "ok": True,
                "job_id": job_id,
                "scene_idx": scene_idx,
                "still_url": f"/api/long-form/jobs/{job_id}/still/{scene_idx}?v={version}",
                "new_prompt_used": True,
                "visual_fix_contract": {
                    "method": plan["method"],
                    "prompt_budget": plan["prompt_budget"],
                    "still_prompt_len": plan["still_prompt_len"],
                    "cast_count": plan["cast_count"],
                    "rules": plan["rules"],
                },
            }, indent=2)
        new_path = lf.regenerate_still(job_id, int(scene_idx), new_prompt=str(reason or "").strip() or None)
        version = int(new_path.stat().st_mtime)
        return json.dumps({
            "ok": True,
            "job_id": job_id,
            "scene_idx": scene_idx,
            "still_url": f"/api/long-form/jobs/{job_id}/still/{scene_idx}?v={version}",
            "new_prompt_used": bool(str(reason or "").strip()),
        }, indent=2)
    except Exception as e:
        raise RuntimeError(f"long-form still regeneration failed: {str(e)[:300]}") from e


def regenerate_longform_thumbnail(
    job_id: str,
    idx: int,
    custom_prompt: str = "",
) -> str:
    from long_form import pipeline as lf

    try:
        new_path = lf.regenerate_thumbnail(
            str(job_id or "").strip(),
            int(idx),
            str(custom_prompt or "").strip() or None,
        )
    except Exception as exc:
        raise RuntimeError(f"long-form thumbnail regeneration failed: {str(exc)[:300]}") from exc
    version = int(new_path.stat().st_mtime)
    return json.dumps(
        {
            "ok": True,
            "job_id": str(job_id or "").strip(),
            "idx": int(idx),
            "thumbnail_url": (
                f"/api/long-form/jobs/{str(job_id or '').strip()}/thumbnail/{int(idx)}?v={version}"
            ),
            "custom_prompt_used": bool(str(custom_prompt or "").strip()),
        },
        indent=2,
    )


def _reclaim_orphaned_shortform_jobs() -> int:
    """On process start (or module import), find job_spec.json that have no result.json yet
    and (re)launch the worker thread for them. This gives us a cheap 'resume after restart'
    for specs whose workspaces survived on the Fly volume.

    Returns number of jobs for which we (re)started a worker.
    Safe to call multiple times; we skip dirs that already have a plausible active worker
    (recent heartbeat or progress mtime within last 5min).
    """
    try:
        root = ROOT / SKELETON_OUTPUT
        if not root.is_dir():
            return 0
        reclaimed = 0
        now = time.time()
        for spec_path in root.glob("*/job_spec.json"):
            ws = spec_path.parent
            if (ws / "result.json").exists():
                continue
            # If there's a very recent heartbeat or progress, assume a worker is live in this process or sibling.
            hb = ws / "heartbeat.txt"
            prog = ws / "progress.json"
            recent = False
            for p in (hb, prog):
                if p.is_file() and (now - p.stat().st_mtime) < 300:
                    recent = True
                    break
            if recent:
                continue
            try:
                spec = json.loads(spec_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            # Re-spawn a worker for this spec. The pipelines inside are partially idempotent (skip existing stills/clips).
            # We don't want to block import/startup, so fire in background.
            def _relaunch(s=spec, w=ws):
                # Re-use as much of the spawn logic as possible by calling the inner work shape.
                # For simplicity we just call the same functions the original thread would.
                from studio_agent.render_styles import is_skeleton_style as _is_skel
                import traceback as _tb2
                hb2 = w / "heartbeat.txt"
                stop2 = threading.Event()
                hb_t = threading.Thread(target=_heartbeat_loop, args=(stop2, hb2), daemon=True)
                hb_t.start()
                try:
                    hb2.touch(exist_ok=True)
                    rstyle = str(s.get("render_style") or "cinematic")
                    from skeleton_ai.styled_pipeline import plan_scenes as _plan_scenes

                    _plan_scenes(
                        category_key=str(s.get("category_key")),
                        topic=s.get("topic"),
                        workspace=w,
                        render_style=rstyle,
                        tier=str(s.get("tier") or "standard"),
                        video_model=s.get("video_model"),
                        visual_brief=s.get("visual_brief"),
                        script_override=s.get("script"),
                        user_id=s.get("user_id"),
                        default_animate=False,
                        sound_design_brief=str(s.get("sound_design_brief") or ""),
                    )
                except Exception as e2:
                    try:
                        (w / "job.log").write_text(f"RECLAIM FAILED {time.time()}\n{e2}\n{_tb2.format_exc()}", encoding="utf-8")
                    except Exception:
                        pass
                    try:
                        (w / "result.json").write_text(json.dumps({"status": "failed", "job_id": s.get("job_id"), "error": str(e2)}, indent=2), encoding="utf-8")
                    except Exception:
                        pass
                finally:
                    stop2.set()
            threading.Thread(target=_relaunch, daemon=True, name=f"reclaim-{ws.name}").start()
            reclaimed += 1
        return reclaimed
    except Exception:
        return 0


# Run reclaim on import of this module (i.e. when the backend process that mounts studio-agent starts).
# This is best-effort; it will pick up jobs left behind by a previous worker crash/restart as long as
# the SKELETON_AI_OUTPUT_ROOT volume preserved the workspaces.
try:
    _reclaim_orphaned_shortform_jobs()
except Exception:
    pass


def cancel_shortform_job(job_id: str) -> bool:
    """Signal a running shortform render to stop at its next checkpoint.

    Writes a CANCELLED flag into the job workspace; the render loop checks it
    each beat and exits cleanly. Returns True if the workspace was found.
    """
    jid = str(job_id or "").strip()
    if not jid or not jid.replace("_", "").isalnum() or len(jid) > 48:
        return False
    workspace = (ROOT / SKELETON_OUTPUT / jid).resolve()
    if not workspace.is_dir():
        return False
    from skeleton_ai.pipeline import CANCEL_FLAG, _write_progress
    (workspace / CANCEL_FLAG).write_text("1", encoding="utf-8")
    payload = {"status": "cancelled", "job_id": jid, "error": "Cancelled by user"}
    try:
        (workspace / "result.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        pass
    try:
        _write_progress(workspace, stage="cancelled", progress=100, detail="Cancelled by user")
    except Exception:
        pass
    return True


_SHORTFORM_EXISTING_JOB_TOOLS = frozenset({
    "expand_visual_proof_shortform",
    "list_production_scenes",
    "edit_production_scene_still",
    "edit_production_scenes_still",
    "regenerate_production_scene_still",
    "regenerate_production_scene",
    "set_production_scenes_animate",
    "set_production_scene_duration",
    "animate_production_scenes",
    "repair_production_scene_animation",
    "audit_and_repair_production_scenes",
    "finalize_production",
    "re_edit_production",
})
_LONGFORM_EXISTING_JOB_TOOLS = frozenset({
    "expand_longform_visual_proof",
    "regenerate_longform_still",
    "regenerate_longform_thumbnail",
    "list_longform_scenes",
    "finalize_longform_render",
})
_COMPETITOR_EXISTING_JOB_TOOLS = frozenset({
    "retry_reference_analysis",
    "build_scene_blueprint_from_reference",
})


def _enforce_tool_job_ownership(name: str, args: dict[str, Any], user_id: str) -> None:
    """Prevent model/tool calls from crossing creator workspace boundaries."""
    job_id = ""
    kind = ""
    if name in _SHORTFORM_EXISTING_JOB_TOOLS:
        job_id, kind = str(args.get("job_id") or "").strip(), "shortform"
    elif name in _LONGFORM_EXISTING_JOB_TOOLS:
        job_id, kind = str(args.get("job_id") or "").strip(), "longform"
    elif name in _COMPETITOR_EXISTING_JOB_TOOLS:
        job_id, kind = str(args.get("job_id") or "").strip(), "competitor"
    elif name == "poll_render_job":
        job_id = str(args.get("job_id") or "").strip()
        kind = str(args.get("kind") or "longform").strip().lower()
    elif name == "poll_cliplab_job":
        job_id, kind = str(args.get("job_id") or "").strip(), "cliplab"
    elif name == "render_cliplab_segments":
        job_id, kind = str(args.get("analyze_job_id") or "").strip(), "cliplab"
    elif name == "generate_longform_thumbnails" and str(args.get("job_id") or "").strip():
        job_id, kind = str(args.get("job_id") or "").strip(), "longform"
    if not job_id:
        return

    from studio_agent import jobs as agent_jobs

    access = agent_jobs.job_access_metadata(job_id, kind)
    # Let the underlying tool produce its normal not-found response. Ownership
    # checks apply as soon as a durable workspace/receipt exists.
    if not access.get("exists"):
        return
    uid = str(user_id or "").strip()
    owner_id = str(access.get("owner_id") or "").strip()
    if owner_id and uid and hmac.compare_digest(owner_id, uid):
        return
    if _is_studio_admin_user(uid):
        return
    raise PermissionError("production job not found")


def execute_tool(
    name: str,
    arguments: dict[str, Any],
    *,
    user_id: str,
    content_format: str,
    session_id: str | None = None,
) -> str:
    args = arguments or {}
    if name in OWNER_ONLY_AGENT_TOOLS:
        _require_cliplab_admin(user_id)
    if name in {
        "generate_longform_outline",
        "expand_longform_chapter",
        "start_longform_render",
        "expand_longform_visual_proof",
        "list_longform_scenes",
        "regenerate_longform_still",
        "generate_longform_thumbnails",
        "regenerate_longform_thumbnail",
        "finalize_longform_render",
    }:
        _require_longform_entitlement(user_id)
    if name in ("analyze_reference_video", "analyze_competitor_video"):
        name = "analyze_reference_video"
    _enforce_tool_job_ownership(name, args, user_id)

    if name == "list_skills":
        return json.dumps({"skills": skill_loader.list_skill_slugs()}, indent=2)

    if name == "load_skill":
        slug = str(args.get("slug", "")).strip()
        companion = str(args.get("companion") or "").strip()
        if companion:
            text = skill_loader.read_skill_companion(slug, companion)
        else:
            text = skill_loader.read_skill(slug)
        return text

    if name == "load_channel_docs":
        key = str(args.get("channel_key", "")).strip()
        doc = str(args.get("doc") or "both").strip().lower()
        out: dict[str, str] = {}
        if doc in ("channel", "both"):
            out["CHANNEL"] = skill_loader.read_channel_doc(key, "CHANNEL")
        if doc in ("flow", "both"):
            out["FLOW"] = skill_loader.read_channel_doc(key, "FLOW")
        return json.dumps(out, indent=2)

    if name == "list_studio_channels":
        from long_form.prompts.channels import list_channels
        return json.dumps(list_channels(), indent=2, ensure_ascii=True)

    if name == "read_project_file":
        path = _safe_path(str(args.get("relative_path", "")))
        if not path.is_file():
            raise FileNotFoundError(str(path))
        max_chars = int(args.get("max_chars") or 12000)
        text = path.read_text(encoding="utf-8", errors="replace")
        if len(text) > max_chars:
            text = text[:max_chars] + "\nâ€¦ truncated"
        return text

    if name == "write_project_file":
        path = _safe_path(str(args.get("relative_path", "")))
        _allow_write(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(args.get("content") or ""), encoding="utf-8")
        return json.dumps({"written": str(path.relative_to(ROOT)), "bytes": path.stat().st_size})

    if name == "list_longform_scenes":
        return list_longform_scenes(str(args.get("job_id") or "").strip())

    if name == "generate_longform_outline":
        return generate_longform_outline_logged(dict(args or {}))

    if name == "expand_longform_chapter":
        return expand_longform_chapter_logged(dict(args or {}))

    if name == "regenerate_longform_still":
        return regenerate_longform_still(
            str(args.get("job_id") or "").strip(),
            int(args.get("scene_idx") or 0),
            str(args.get("reason") or ""),
        )

    if name == "regenerate_longform_thumbnail":
        return regenerate_longform_thumbnail(
            str(args.get("job_id") or "").strip(),
            int(args.get("idx") or 0),
            str(args.get("custom_prompt") or ""),
        )

    if name == "start_longform_render":
        _require_longform_entitlement(user_id)
        from long_form.prompts.channels import get_channel
        from long_form import pipeline as lf_pipeline
        from studio_agent.render_styles import resolve_render_style

        channel_key = str(args.get("channel_key") or "").strip()
        # get_channel fuzzy-resolves mangled keys (dictation stretch like
        # "historyyyrewinddd"); keep the canonical key for state + response.
        channel = dict(get_channel(channel_key))
        lf_pipeline.validate_channel_pipeline(channel)
        channel_key = str(channel.get("key") or channel_key)
        render_args = dict(args or {})
        models = _session_production_models(session_id)
        bound_session = store.get_session(session_id, reconcile_jobs=False) or {} if session_id else {}
        picked_image = str(
            render_args.get("image_model_id")
            or render_args.get("image_model")
            or models.get("image_model_id")
            or ""
        ).strip()
        if picked_image:
            render_args["image_model_id"] = store.normalize_image_model(picked_image)
        else:
            render_args["image_model_id"] = models.get("image_model_id")
        channel["image_model_default"] = str(render_args.get("image_model_id") or "").strip()
        resolved_title, resolved_topic = _resolve_longform_title_topic(render_args, session_id=session_id)
        render_args["title"] = resolved_title
        render_args["topic"] = resolved_topic
        outline = _build_outline_from_args(render_args)
        initial_route = _repair_route_snapshot(
            session_id,
            image_model_id=str(render_args.get("image_model_id") or ""),
            video_model=str(models.get("video_model") or ""),
            route_revision=int(args.get("_media_route_revision") or 1),
        )
        # Bind the job to its Studio session, while provider workers continue
        # re-reading this route before every dispatch, fallback, and commit.
        outline["_session_id"] = str(session_id or "")
        outline["image_model_id"] = str(
            initial_route.get("image_model_id") or render_args.get("image_model_id") or ""
        )
        outline["video_model"] = str(
            initial_route.get("video_model") or models.get("video_model") or ""
        )
        outline["media_route_revision"] = int(initial_route.get("revision") or 1)
        outline["chat_model"] = str(
            bound_session.get("model")
            or render_args.get("chat_model")
            or outline.get("chat_model")
            or ""
        ).strip()
        outline["captions_enabled"] = bool(
            render_args.get("captions_enabled", bound_session.get("captions_enabled", True))
        )
        outline["caption_mode"] = str(
            render_args.get("caption_mode") or bound_session.get("caption_mode") or "word"
        ).strip().lower()
        if outline["caption_mode"] == "off":
            outline["captions_enabled"] = False
        selected_image_model = str(args.get("image_model_id") or "").strip()
        if selected_image_model:
            channel["image_model_default"] = selected_image_model
        style = resolve_render_style(
            str(args.get("render_style") or "").strip() or None,
            session_style=_session_render_style(session_id),
        )
        style_lock = (
            f"STRICT PRODUCTION-WIDE ART STYLE LOCK: {style.label}. "
            f"{style.prompt_prefix} Every chapter, scene, character, object, transition frame, "
            "animation prompt, and thumbnail must remain visibly in this exact art style. "
            "Never switch to photorealism, another animation medium, or the channel default."
        )
        channel["visual_style"] = f"{style_lock} {channel.get('visual_style') or ''}".strip()
        outline["render_style"] = style.key
        outline["user_id"] = str(user_id or "").strip()
        outline["render_style_label"] = style.label
        outline["render_style_lock"] = style_lock
        outline["motion_policy"] = str(args.get("motion_policy") or outline.get("motion_policy") or "balanced")
        outline["sfx_enabled"] = bool(args.get("sfx_enabled", False))
        channel_default_bgm = str(channel.get("default_background_music") or "off").strip() or "off"
        outline["sound_design_brief"] = str(
            args.get("sound_design_brief")
            or outline.get("sound_design_brief")
            or channel.get("sound_design")
            or ""
        ).strip()
        outline["background_music"] = str(
            args.get("background_music")
            or outline.get("background_music")
            or channel_default_bgm
        ).strip() or channel_default_bgm
        # Long-form must prove the visual language with scene zero before the
        # gallery consumes a large image budget. Explicit false is reserved
        # for internal recovery of an already-approved proof job.
        outline["visual_proof_only"] = bool(args.get("visual_proof_only", True))
        outline["ken_burns_enabled"] = bool(args.get("ken_burns_enabled", outline.get("motion_policy") == "stills"))
        outline["light_shake_enabled"] = bool(args.get("light_shake_enabled", False))
        outline["image_model_id"] = str(channel.get("image_model_default") or "").strip()
        outline["target_duration_sec"] = max(60, int(args.get("target_duration_sec") or outline.get("target_duration_sec") or 1200))
        if outline["sound_design_brief"]:
            channel["sound_design"] = outline["sound_design_brief"]
        if args.get("hero_motion_ratio") is not None:
            outline["hero_motion_ratio"] = max(0.0, min(1.0, float(args["hero_motion_ratio"])))
        cost_estimate = lf_pipeline.compute_render_cost(channel, outline)
        max_budget = args.get("max_budget_usd")
        if max_budget is not None and float(cost_estimate.get("all_in_usd") or cost_estimate.get("total_usd") or 0.0) > float(max_budget):
            raise ValueError(
                f"Long-form estimate ${float(cost_estimate.get('all_in_usd') or cost_estimate.get('total_usd') or 0.0):.2f} exceeds hard budget ${float(max_budget):.2f}"
            )
        job_id = lf_pipeline.start_render(
            channel,
            outline,
            requested_job_id=str(args.get("_requested_job_id") or "").strip() or None,
        )
        return json.dumps({
            "status": "awaiting_scene_review",
            "job_id": job_id,
            "channel_key": channel_key,
            "pipeline_kind": channel.get("pipeline_kind") or "sleep_doc",
            "render_style": style.key,
            "render_style_label": style.label,
            "poll_url": f"/api/long-form/jobs/{job_id}/status",
            "finalize_url": f"/api/long-form/jobs/{job_id}/finalize",
            "outline_title": outline.get("title"),
            "chapters": len(outline.get("chapters") or []),
            "motion_policy": outline.get("motion_policy"),
            "hero_motion_ratio": lf_pipeline.resolve_motion_ratio(outline)[1],
            "sfx_enabled": outline.get("sfx_enabled"),
            "sound_design_brief": outline.get("sound_design_brief"),
            "background_music": outline.get("background_music"),
            "cost_estimate": cost_estimate,
            "visual_proof_only": outline.get("visual_proof_only"),
            "next_action": "Review exactly one proof scene. After approval, call expand_longform_visual_proof before finalizing.",
        }, indent=2)

    if name == "expand_longform_visual_proof":
        from long_form import pipeline as lf_pipeline
        job_id = str(args.get("job_id") or "").strip()
        lf_pipeline.expand_visual_proof(job_id)
        return json.dumps({
            "status": "expanding_scene_gallery", "job_id": job_id,
            "note": "The approved proof scene is preserved. Studio is generating the remaining review gallery.",
        }, indent=2)

    if name == "list_skeleton_video_models":
        from skeleton_ai.i2v_engine import list_video_models

        return json.dumps(
            {
                "video_models": list_video_models(),
                "stills": {
                    "model": "seedream_v45_edit",
                    "locked": True,
                    "rule": (
                        "Every scene edits the canonical skeleton master â€” same identity; "
                        "change background, clothes, muscles-on-shell, props only."
                    ),
                },
            },
            indent=2,
        )

    if name == "list_skeleton_categories":
        from skeleton_ai.prompts.category_registry import list_categories, list_valid_keys

        uid = str(user_id or "").strip() or None
        cats = list_categories(user_id=uid)
        return json.dumps(
            {
                "categories": cats,
                "valid_keys": list_valid_keys(uid),
                "hint": "Built-in outcast covers edgy/contrarian lanes; use create_skeleton_category for a personalized variant.",
            },
            indent=2,
            ensure_ascii=True,
        )

    if name == "create_skeleton_category":
        from skeleton_ai.prompts.category_registry import create_custom_category

        uid = str(user_id or "").strip()
        if not uid:
            raise ValueError("sign in required to create custom categories")
        entry = create_custom_category(
            uid,
            label=str(args.get("label") or "").strip(),
            key=str(args.get("key") or "").strip() or None,
            tagline=str(args.get("tagline") or "").strip() or None,
            system_prompt=str(args.get("system_prompt") or "").strip() or None,
            seed_ideas=[str(s) for s in (args.get("seed_ideas") or [])],
        )
        return json.dumps({"category": entry, "category_key": entry["key"]}, indent=2)

    if name == "list_render_styles":
        from studio_agent.render_styles import DEFAULT_RENDER_STYLE, list_render_styles

        return json.dumps(
            {
                "default": DEFAULT_RENDER_STYLE,
                "styles": list_render_styles(),
                "rule": (
                    "Pass render_style on every start_shortform_generate. "
                    "skeleton_host is the Skeleton niche â€” same weight as comic_book or cinematic. "
                    "Use the visual gallery with preview_url for style selection (distinct Seedream previews per style)."
                ),
            },
            indent=2,
            ensure_ascii=False,
        )

    if name == "ingest_product_reference":
        from studio_agent import product_reference

        session = store.get_session(str(session_id or ""), user_id=str(user_id or "")) or {}
        attached_paths = (
            list(session.get("latest_attachment_paths") or [])
            if bool(args.get("use_attached_images", True))
            else []
        )
        manifest = product_reference.ingest(
            session_id=str(session_id or ""),
            user_id=str(user_id or ""),
            website_url=(
                str(args.get("website_url") or "").strip()
                or str(session.get("product_website") or "").strip()
            ),
            attached_paths=[str(path) for path in attached_paths],
            product_name=str(args.get("product_name") or ""),
            product_description=str(args.get("product_description") or ""),
        )
        return json.dumps({
            "ok": True,
            "reference_id": manifest["reference_id"],
            "product_name": manifest["product_name"],
            "website": manifest.get("website"),
            "ad_brief": manifest.get("ad_brief"),
            "image_count": len(manifest.get("images") or []),
            "images": [
                {
                    "source": image.get("source"),
                    "source_url": image.get("source_url"),
                    "bytes": image.get("bytes"),
                }
                for image in manifest.get("images") or []
            ],
            "next_action": (
                "Call start_shortform_generate with this product_reference_id and an advertisement visual_brief."
            ),
        }, indent=2)

    if name == "generate_longform_thumbnails":
        session = store.get_session(session_id) if session_id else None
        concept = (session or {}).get("pending_concept") if isinstance((session or {}).get("pending_concept"), dict) else {}
        return generate_longform_thumbnails(
            str(args.get("job_id") or ""),
            int(args.get("count") or 3),
            str(args.get("feedback") or ""),
            title=str(args.get("title") or concept.get("title") or store.get_locked_working_title(session or {}) or ""),
            channel_key=str(args.get("channel_key") or (session or {}).get("registry_key") or "history_rewind"),
            prompt=str(args.get("prompt") or ""),
            user_id=str(user_id or ""),
        )

    if name == "start_shortform_generate":
        from skeleton_ai.prompts.category_registry import get_category
        from skeleton_ai.i2v_engine import resolve_video_model_chain
        from studio_agent.render_styles import is_skeleton_style, resolve_render_style

        args = _normalize_shortform_category_args(args)
        uid = str(user_id or "").strip() or None
        session_row = store.get_session(str(session_id or ""), user_id=uid) or {}
        session_messages = list(session_row.get("messages") or [])
        args = store._prepare_shortform_execution_args(args, session_messages, session=session_row)
        category_key = str(args.get("category_key") or "people_blogs").strip()
        topic = str(args.get("topic") or "").strip() or None
        script = str(args.get("script") or "").strip() or None
        visual_brief = str(args.get("visual_brief") or "").strip() or None
        scene_count_raw = args.get("scene_count")
        try:
            scene_count = int(scene_count_raw) if scene_count_raw is not None else None
        except Exception:
            scene_count = None
        scene_count = max(1, min(scene_count, 60)) if scene_count else None
        # Defense in depth: store normalization already forces this, but the
        # execution boundary also enforces the permanent staged Short workflow.
        visual_proof_only = True
        scene_count = 1
        product_reference_id = str(args.get("product_reference_id") or "").strip()
        production_models = _session_production_models(session_id)
        image_model_id = str(
            args.get("image_model_id") or args.get("image_model") or production_models.get("image_model_id") or ""
        ).strip()
        style = resolve_render_style(
            str(args.get("render_style") or "").strip() or None,
            session_style=_session_render_style(session_id),
        )
        skeleton_style = is_skeleton_style(style)
        video_model = str(args.get("video_model") or production_models.get("video_model") or "").strip()
        tier = str(args.get("tier") or "standard").strip()
        if video_model == "kling_pro":
            tier = "premium"
        get_category(category_key, user_id=uid)
        _, resolved_vm = resolve_video_model_chain(video_model=video_model, tier=tier)
        animate_arg = args.get("animate")
        if animate_arg is None:
            animate = False
        else:
            # First pass is always still-review only. Image-to-video requires
            # explicit scene approval after the user sees the generated stills.
            animate = False
        caption_mode = str(args.get("caption_mode") or "word").strip().lower()
        captions_enabled = bool(args.get("captions_enabled", True))
        if caption_mode == "off":
            captions_enabled = False
            caption_mode = "off"
        elif caption_mode not in {"word", "single_word", "one_word"}:
            caption_mode = "word"
        sfx_enabled = bool(args.get("sfx_enabled", False))
        sound_design_brief = str(args.get("sound_design_brief") or "").strip()
        background_music = str(args.get("background_music") or "off").strip() or "off"
        from studio_agent.studio_identity import normalize_promotion_mode
        studio_promotion_mode = normalize_promotion_mode(args.get("studio_promotion_mode"))
        watermark_text = _session_channel_brand(session_id)
        # Next-short / title-lock paths set _force_fresh so we never reattach to a Ready job.
        force_fresh = bool(args.get("_force_fresh"))
        resume_job_id = None if force_fresh else (str(args.get("_resume_job_id") or "").strip() or None)
        product_manifest: dict[str, Any] | None = None
        reference_images: list[str] = []
        skeleton_reference_image = str(
            args.get("reference_image") or session_row.get("skeleton_reference_image") or ""
        ).strip()
        if skeleton_style and skeleton_reference_image:
            reference_images = [skeleton_reference_image]
        if product_reference_id:
            from studio_agent import product_reference

            product_manifest = product_reference.load(product_reference_id, user_id=str(user_id or ""))
            reference_images = [
                str(image.get("path") or "")
                for image in product_manifest.get("images") or []
                if str(image.get("path") or "").strip()
            ][:3]
            ad_brief = product_manifest.get("ad_brief") if isinstance(product_manifest.get("ad_brief"), dict) else {}
            cta = ", ".join(ad_brief.get("cta_candidates") or [])[:180]
            benefits = "; ".join(ad_brief.get("benefits") or [])[:400]
            prices = ", ".join(ad_brief.get("price_hints") or [])[:120]
            product_lock = (
                f"PRODUCT ADVERTISEMENT FOR {product_manifest.get('product_name')}. "
                "Preserve the exact supplied product identity in every product shot. "
                f"Product facts: {product_manifest.get('product_description') or 'Use only visible or supplied facts.'} "
                f"Headline: {ad_brief.get('headline') or product_manifest.get('product_name') or ''}. "
                f"Benefits: {benefits or 'Use only supplied facts.'} "
                f"Price hints: {prices or 'Do not invent pricing.'} "
                f"CTA candidates: {cta or 'Use one clear signup/purchase CTA.'}"
            )
            visual_brief = f"{product_lock} {visual_brief or ''}".strip()
        job_id = _spawn_shortform_job(
            category_key=category_key,
            topic=topic,
            script=script,
            scene_count=scene_count,
            tier=tier,
            image_model_id=image_model_id or None,
            video_model=resolved_vm,
            visual_brief=visual_brief,
            render_style=style.key,
            user_id=uid,
            animate=animate,
            watermark_text=watermark_text,
            captions_enabled=captions_enabled,
            caption_mode=caption_mode,
            sfx_enabled=sfx_enabled,
            sound_design_brief=sound_design_brief,
            background_music=background_music,
            resume_job_id=resume_job_id,
            requested_job_id=str(args.get("_requested_job_id") or "").strip() or None,
            reference_images=reference_images,
            product_reference=product_manifest,
            credit_reservation=args.get("_credit_reservation") if isinstance(args.get("_credit_reservation"), dict) else None,
            credit_session_id=str(args.get("_credit_session_id") or session_id or ""),
            credit_budget=args.get("_credit_budget") if isinstance(args.get("_credit_budget"), dict) else None,
            visual_proof_only=visual_proof_only,
            studio_promotion_mode=studio_promotion_mode,
        )
        stills_model = image_model_id or (
            store.SKELETON_DEFAULT_IMAGE_MODEL
            if is_skeleton_style(style)
            else "seedream_v45_edit_product_reference"
            if reference_images
            else f"seedream_v45_t2i_{style.key}"
        )
        image_model_low = (image_model_id or "").strip().lower()
        if is_skeleton_style(style):
            ref_note = (
                "user-uploaded skeleton reference"
                if reference_images
                else "canonical master reference (upload a skeleton image for KORPI-level lock)"
            )
            pipeline_note = (
                f"Skeleton host - Seedream v4.5 edit via {image_model_id or store.SKELETON_DEFAULT_IMAGE_MODEL} "
                f"({ref_note}). No image-to-video runs until scenes are approved. "
                f"Later video model: {resolved_vm}."
            )
        elif image_model_low.startswith("grok"):
            pipeline_note = (
                f"Grok/xAI stills selected via {image_model_id}. "
                f"No image-to-video runs until scenes are approved. Later video model: {resolved_vm}."
            )
        else:
            pipeline_note = (
                f"{style.label} - {'product-locked Seedream reference edits' if reference_images else 'Seedream stills'} only. "
                f"No image-to-video runs until scenes are approved. Later video model: {resolved_vm}."
            )
        return json.dumps({
            "status": "awaiting_scene_review",
            "job_id": job_id,
            "category_key": category_key,
            "topic": topic,
            "scene_count": scene_count,
            "visual_proof_only": visual_proof_only,
            "staged_shortform_workflow": True,
            "visual_brief": visual_brief,
            "render_style": style.key,
            "render_style_label": style.label,
            "image_model_id": image_model_id or None,
            "video_model": resolved_vm,
            "stills_model": stills_model,
            "product_reference_id": product_reference_id or None,
            "product_reference_count": len(reference_images),
            "watermark_text": watermark_text,
            "captions_enabled": captions_enabled,
            "caption_mode": "word" if captions_enabled else "off",
            "sfx_enabled": sfx_enabled,
            "sound_design_brief": sound_design_brief,
            "background_music": background_music,
            "poll_url": f"/api/skeleton-ai/jobs/{job_id}",
            "note": pipeline_note + (
                " Visual proof mode is active: exactly one still will be generated before any remaining scenes are allowed."
                if visual_proof_only
                else ""
            ) + f" Channel watermark/package is locked to {watermark_text}. Captions are {'word-level and enabled' if captions_enabled else 'disabled'}. Sound design is {'enabled' if sfx_enabled else 'disabled'} and will be mixed during finalize_production. Poll until result.json reaches awaiting_scene_review. Then list scenes, edit artifacted stills, approve scenes with set_production_scenes_animate, optionally animate approved scenes, and only then finalize_production.",
        }, indent=2)

    # === Granular scene control tools (full creative control) ===
    if name == "list_production_scenes":
        return list_production_scenes(str(args.get("job_id") or ""))

    if name == "expand_visual_proof_shortform":
        raw_duration = args.get("duration_seconds")
        duration_seconds = float(raw_duration) if raw_duration not in (None, "") else None
        raw_existing_count = args.get("existing_scene_count")
        existing_count = int(raw_existing_count) if raw_existing_count not in (None, "") else None
        raw_preserve = args.get("preserve_scene_indices")
        preserve_indices = [int(value) for value in raw_preserve] if isinstance(raw_preserve, list) else None
        raw_animate = args.get("animate_scene_indices")
        animate_indices = [int(value) for value in raw_animate] if isinstance(raw_animate, list) else None
        return expand_visual_proof_shortform(
            str(args.get("job_id") or ""),
            int(args.get("scene_count") or 12),
            duration_seconds=duration_seconds,
            creative_direction=str(args.get("creative_direction") or ""),
            animate_policy=str(args.get("animate_policy") or "heroes"),
            command_id=str(args.get("command_id") or ""),
            existing_scene_count=existing_count,
            preserve_scene_indices=preserve_indices,
            animate_scene_indices=animate_indices,
            image_model_id=str(args.get("image_model_id") or "") or None,
            video_model=str(args.get("video_model") or "") or None,
            route_revision=int(args.get("_media_route_revision") or 1),
            credit_reservation=args.get("_credit_reservation") if isinstance(args.get("_credit_reservation"), dict) else None,
            credit_user_id=str(user_id or ""),
            credit_session_id=str(args.get("_credit_session_id") or session_id or ""),
            credit_budget=args.get("_credit_budget") if isinstance(args.get("_credit_budget"), dict) else None,
        )

    if name == "edit_production_scene_still":
        return edit_production_scene_still(
            str(args.get("job_id") or ""),
            int(args.get("scene_index") or 0),
            str(args.get("instruction") or ""),
            str(args.get("scope") or "full"),
        )

    if name == "edit_production_scenes_still":
        raw_idx = args.get("scene_indices")
        indices = [int(x) for x in raw_idx] if isinstance(raw_idx, list) else None
        return edit_production_scenes_still(
            str(args.get("job_id") or ""),
            str(args.get("instruction") or ""),
            indices,
            str(args.get("scope") or "character"),
        )

    if name == "regenerate_production_scene_still":
        return regenerate_production_scene_still(
            str(args.get("job_id") or ""),
            int(args.get("scene_index") or 0),
            reason=str(args.get("reason") or ""),
            image_model_id=str(args.get("image_model_id") or "") or None,
            session_id=session_id,
            route_revision=int(args.get("_media_route_revision") or 1),
        )

    if name == "regenerate_production_scene":
        return regenerate_production_scene(
            str(args.get("job_id") or ""),
            int(args.get("scene_index") or 0),
            reason=str(args.get("reason") or ""),
            animate=bool(args.get("animate", True)),
            restage_direction=str(args.get("restage_direction") or ""),
            image_model_id=str(args.get("image_model_id") or "") or None,
            video_model=str(args.get("video_model") or "") or None,
            session_id=session_id,
            route_revision=int(args.get("_media_route_revision") or 1),
        )

    if name == "regenerate_production_scenes":
        raw_idx = args.get("scene_indices")
        indices = [int(x) for x in raw_idx] if isinstance(raw_idx, list) else None
        return regenerate_production_scenes(
            str(args.get("job_id") or ""),
            indices,
            reason=str(args.get("reason") or ""),
            animate=bool(args.get("animate", True)),
        )

    if name == "set_production_scenes_animate":
        raw_idx = args.get("scene_indices")
        indices = [int(x) for x in raw_idx] if isinstance(raw_idx, list) else None
        return set_production_scenes_animate(
            str(args.get("job_id") or ""),
            bool(args.get("animate", True)),
            indices,
        )

    if name == "set_production_scene_duration":
        return set_production_scene_duration(
            str(args.get("job_id") or ""),
            int(args.get("scene_index") or 0),
            float(args.get("duration_sec") or 5.0),
        )

    if name == "animate_production_scenes":
        raw_idx = args.get("scene_indices")
        indices = [int(x) for x in raw_idx] if isinstance(raw_idx, list) else None
        raw_budget = args.get("max_budget_usd")
        try:
            max_budget_usd = float(raw_budget) if raw_budget is not None else None
        except (TypeError, ValueError):
            max_budget_usd = None
        return animate_production_scenes(
            str(args.get("job_id") or ""),
            indices,
            max_budget_usd,
            video_model=str(args.get("video_model") or "") or None,
            session_id=session_id,
            route_revision=int(args.get("_media_route_revision") or 1),
        )

    if name == "repair_production_scene_animation":
        return repair_production_scene_animation(
            str(args.get("job_id") or ""),
            int(args.get("scene_index") or 0),
            str(args.get("reason") or ""),
            video_model=str(args.get("video_model") or "") or None,
            session_id=session_id,
            route_revision=int(args.get("_media_route_revision") or 1),
        )

    if name == "audit_and_repair_production_scenes":
        raw_idx = args.get("scene_indices")
        indices = [int(x) for x in raw_idx] if isinstance(raw_idx, list) else []
        return audit_and_repair_production_scenes(
            str(args.get("job_id") or ""),
            indices,
            str(args.get("reason") or ""),
            image_model_id=str(args.get("image_model_id") or "") or None,
            video_model=str(args.get("video_model") or "") or None,
            session_id=session_id,
            route_revision=int(args.get("_media_route_revision") or 1),
        )

    if name == "finalize_production":
        raw_captions_enabled = args.get("captions_enabled")
        captions_enabled = raw_captions_enabled if isinstance(raw_captions_enabled, bool) else None
        caption_mode = str(args.get("caption_mode") or "").strip() or None
        return finalize_production(
            str(args.get("job_id") or ""),
            captions_enabled=captions_enabled,
            caption_mode=caption_mode,
        )

    if name == "re_edit_production":
        return re_edit_production(
            str(args.get("job_id") or ""),
            str(args.get("instruction") or ""),
            str(args.get("kind") or "shortform"),
        )

    if name == "list_longform_scenes":
        return list_longform_scenes(str(args.get("job_id") or ""))

    if name == "regenerate_longform_still":
        return regenerate_longform_still(
            str(args.get("job_id") or ""),
            int(args.get("scene_idx") or args.get("scene_index") or 0),
            reason=str(args.get("reason") or ""),
        )

    if name == "run_build_script":
        script_name = Path(str(args.get("script", ""))).name
        if script_name not in ALLOWED_BUILD_SCRIPTS:
            raise ValueError(f"script not allowlisted: {script_name}")
        script_path = ROOT / "long_form" / script_name
        if not script_path.is_file():
            raise FileNotFoundError(script_name)
        cli_args = [sys.executable, str(script_path)] + [str(a) for a in (args.get("args") or [])]
        proc = subprocess.run(
            cli_args,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=3600,
        )
        return json.dumps({
            "exit_code": proc.returncode,
            "stdout_tail": (proc.stdout or "")[-8000:],
            "stderr_tail": (proc.stderr or "")[-4000:],
        }, indent=2)

    if name == "youtube_oauth_status":
        from backend_settings import YOUTUBE_API_KEYS

        doc = ROOT / "studio" / "docs" / "YOUTUBE_OAUTH_SCOPES.md"
        body = doc.read_text(encoding="utf-8") if doc.exists() else (
            "Connect YouTube in Studio â†’ Settings â†’ Channels (or the banner in Studio Agent). "
            "Scopes: youtube.readonly, yt-analytics.readonly, youtube.force-ssl, youtube.upload. "
            "See OAUTH_PUBLISH_RUNBOOK.md for Google Cloud Console steps."
        )
        key_note = (
            f"\n\nServer YouTube Data API key pool: {len(YOUTUBE_API_KEYS)} key(s) configured "
            "(rotates on quota errors for public search/trends). "
            "Per-user OAuth unlocks YouTube Analytics Reporting API (90d metrics) in get_channel_analytics."
        )
        return body + key_note

    if name == "get_studio_credits":
        import unified_credits as uc

        uid = str(user_id or "").strip()
        if not uid:
            raise ValueError("sign in required")
        try:
            state = uc.get_state(uid)
            state["recent"] = uc.recent_ledger(uid, limit=8)
        except Exception as exc:
            state = {"balance": 0, "error": str(exc)}
        if int(state.get("balance") or 0) < 15:
            state["top_up_hint"] = "Low balance â€” user can add credits anytime in Studio â†’ Wallet (unlimited top-ups)."
        return json.dumps(state, indent=2)

    if name == "list_youtube_channels":
        from youtube_connections_store import hydrate
        from long_form.prompts.channels import CHANNELS

        hyd = hydrate() or {}
        uid = str(user_id or "").strip()
        id_to_key = {v["channel_id"]: k for k, v in CHANNELS.items() if v.get("channel_id")}
        out: list[dict[str, Any]] = []
        for owner_id, u in hyd.items():
            if not isinstance(u, dict):
                continue
            if uid and str(owner_id) != uid:
                continue
            for ch_id, rec in (u.get("channels") or {}).items():
                if not isinstance(rec, dict):
                    continue
                key = id_to_key.get(ch_id, "")
                out.append({
                    "channel_id": ch_id,
                    "title": rec.get("title") or rec.get("channel_handle"),
                    "subscribers": int(rec.get("subscriber_count", 0) or 0),
                    "harvest_present": bool(rec.get("analytics_snapshot")),
                    "registry_key": key,
                })
        return json.dumps({"channels": out, "total": len(out)}, indent=2)

    if name == "get_perpetual_memory":
        from studio_agent import memory

        uid = str(user_id or "").strip()
        if not uid:
            raise ValueError("sign in required")
        summary = memory.summarize_for_prompt(
            uid,
            channel_id=str(args.get("channel_id") or "").strip(),
            registry_key=str(args.get("registry_key") or "").strip(),
        )
        return json.dumps({
            "ok": True,
            "summary": summary,
            "profile": memory.public_profile(uid),
        }, indent=2, ensure_ascii=False)

    if name == "remember_channel_preference":
        from studio_agent import memory

        uid = str(user_id or "").strip()
        note = str(args.get("note") or "").strip()
        if not uid:
            raise ValueError("sign in required")
        if not note:
            raise ValueError("note required")
        item = memory.remember(
            uid,
            note,
            scope=str(args.get("scope") or "channel"),
            channel_id=str(args.get("channel_id") or "").strip(),
            registry_key=str(args.get("registry_key") or "").strip(),
            title=str(args.get("title") or "").strip(),
            kind=str(args.get("kind") or "preference").strip(),
            source="agent_tool",
            importance=int(args.get("importance") or 4),
        )
        return json.dumps({"ok": True, "memory": item}, indent=2, ensure_ascii=False)

    if name == "get_channel_analytics":
        async def _fetch():
            from long_form.catalyst_bridge import (
                CHANNEL_KEY_TO_ID,
                assess_channel_growth,
                fetch_channel_snapshot,
                shape_catalyst_insights,
            )

            ch_id = str(args.get("channel_id") or "").strip()
            reg_key = str(args.get("registry_key") or "").strip()
            if reg_key == "lexi_manhua":
                reg_key = "lexi_manhwa"
            focus = str(args.get("focus") or "general").strip().lower()
            if not ch_id and reg_key:
                ch_id = CHANNEL_KEY_TO_ID.get(reg_key, "")
            if not ch_id:
                raise ValueError("channel_id or registry_key required")
            channel_resolution = _resolve_user_channel_connection(
                str(user_id or "").strip(),
                ch_id,
                reg_key,
            )
            lookup_channel_id = str(channel_resolution.get("lookup_channel_id") or ch_id).strip()
            analytics_channel_id = str(channel_resolution.get("analytics_channel_id") or lookup_channel_id).strip()
            snapshot_channel_id = str(channel_resolution.get("snapshot_channel_id") or lookup_channel_id).strip()
            ch_id = analytics_channel_id or ch_id
            record = (
                dict(channel_resolution.get("record") or {})
                or fetch_channel_snapshot(snapshot_channel_id)
                or fetch_channel_snapshot(ch_id)
                or {}
            )
            insights = shape_catalyst_insights(record)
            snapshot = record.get("analytics_snapshot") or {}
            if not isinstance(snapshot, dict):
                snapshot = {}
            harvest = bool(snapshot)
            growth = assess_channel_growth(insights, harvest_present=harvest)
            video_metrics = _video_metric_summary(snapshot)

            velocity: dict[str, Any] = {}
            live_analytics: dict[str, Any] = {}
            live_insights: dict[str, Any] = {}
            live_growth: dict[str, Any] = {}
            uid = str(user_id or "").strip()
            if uid:
                try:
                    from youtube import (
                        _youtube_connected_channel_access_token,
                        _youtube_fetch_channel_analytics,
                        youtube_get_latest_video_velocity,
                    )

                    access_token, rec = await _youtube_connected_channel_access_token(
                        {"id": uid}, lookup_channel_id or ch_id
                    )
                    if access_token:
                        velocity = await youtube_get_latest_video_velocity(
                            access_token=access_token,
                            channel_id=ch_id,
                        )
                        snap = await _youtube_fetch_channel_analytics(access_token, ch_id)
                        if isinstance(snap, dict) and snap:
                            live_video_metrics = _video_metric_summary(snap)
                            live_record = _live_channel_record_from_snapshot(snap, record)
                            live_insights = shape_catalyst_insights(live_record)
                            live_growth = assess_channel_growth(live_insights, harvest_present=True)
                            live_analytics = {
                                "oauth_connected": True,
                                "period": "90d channel aggregate + lifetime retention-ranked videos",
                                "source": "youtube_data_v3+youtube_analytics_reporting",
                                "channel_title": live_record.get("title") or "",
                                "channel_counts": {
                                    "subscribers": live_insights.get("subscribers", 0),
                                    "videos": live_insights.get("videos", 0),
                                    "channel_views": live_insights.get("channel_views", 0),
                                },
                                "channel_summary": snap.get("channel_summary"),
                                "recent_upload_titles": (snap.get("recent_upload_titles") or [])[:10],
                                "top_video_titles": (snap.get("top_video_titles") or [])[:10],
                                "retention_video_titles": [
                                    str((row or {}).get("title") or "").strip()
                                    for row in list(snap.get("retention_videos") or [])[:10]
                                    if isinstance(row, dict) and str((row or {}).get("title") or "").strip()
                                ],
                                "packaging_learnings": snap.get("packaging_learnings") or [],
                                "retention_learnings": snap.get("retention_learnings") or [],
                                "title_pattern_hints": snap.get("title_pattern_hints") or [],
                                "video_metrics": live_video_metrics,
                                "series_clusters": [
                                    {
                                        "label": c.get("label"),
                                        "video_count": c.get("video_count"),
                                    }
                                    for c in (snap.get("series_clusters") or [])[:5]
                                    if isinstance(c, dict)
                                ],
                            }
                    else:
                        oauth_error = str((rec or {}).get("last_sync_error") or "No usable YouTube OAuth token found for this selected channel.")[:240]
                        oauth_error_lower = oauth_error.lower()
                        live_analytics = {
                            "oauth_connected": False,
                            "error": oauth_error,
                            "record_found": bool(rec),
                            "reconnect_required": any(
                                needle in oauth_error_lower
                                for needle in (
                                    "youtube_reconnect_required",
                                    "invalid_grant",
                                    "authorization expired",
                                    "token refresh failed",
                                    "reconnect this channel",
                                )
                            ),
                            "channel_id": ch_id,
                            "lookup_channel_id": lookup_channel_id,
                        }
                except Exception as exc:
                    oauth_error = str(exc)[:240]
                    oauth_error_lower = oauth_error.lower()
                    live_analytics = {
                        "oauth_connected": False,
                        "error": oauth_error,
                        "reconnect_required": any(
                            needle in oauth_error_lower
                            for needle in (
                                "youtube_reconnect_required",
                                "invalid_grant",
                                "authorization expired",
                                "token refresh failed",
                                "reconnect this channel",
                            )
                        ),
                    }

            live_metrics = (live_analytics.get("video_metrics") or {}) if isinstance(live_analytics, dict) else {}
            live_retention_rows = int((live_metrics or {}).get("retention_rows_available") or 0)
            harvest_retention_rows = int((video_metrics or {}).get("retention_rows_available") or 0)
            live_video_rows = int((live_metrics or {}).get("video_rows_available") or 0)
            latest_upload_focus = focus in {"latest_upload", "current", "current_video", "latest"}
            use_live_metrics = bool(live_analytics.get("oauth_connected")) and (
                (latest_upload_focus and live_video_rows > 0)
                or live_retention_rows >= harvest_retention_rows
            )
            effective_video_metrics = (
                live_metrics
                if use_live_metrics
                else video_metrics
            )
            if isinstance(velocity, dict) and str(velocity.get("video_id") or "").strip():
                effective_video_metrics = _promote_latest_upload_from_velocity(
                    effective_video_metrics,
                    velocity,
                )
            shortform_comparison = _compare_shortform_video_metrics(effective_video_metrics)
            effective_insights = live_insights if bool(live_analytics.get("oauth_connected")) and live_insights else insights
            effective_growth = live_growth if bool(live_analytics.get("oauth_connected")) and live_growth else growth
            effective_harvest = True if bool(live_analytics.get("oauth_connected")) and live_insights else harvest
            oauth_error = str(live_analytics.get("error") or "").strip()
            oauth_error_lower = oauth_error.lower()
            oauth_record_found = bool(live_analytics.get("record_found"))
            oauth_reconnect_required = bool(live_analytics.get("reconnect_required")) or any(
                needle in oauth_error_lower
                for needle in (
                    "youtube_reconnect_required",
                    "invalid_grant",
                    "authorization expired",
                    "token refresh failed",
                    "reconnect this channel",
                )
            )
            if bool(live_analytics.get("oauth_connected")):
                oauth_status = "live_private_analytics_connected"
            elif oauth_reconnect_required:
                oauth_status = "selected_channel_token_reconnect_required"
            elif oauth_record_found:
                oauth_status = "selected_channel_record_found_but_private_analytics_unavailable"
            else:
                oauth_status = "selected_channel_private_analytics_not_connected"

            limitation_parts: list[str] = []
            if oauth_reconnect_required:
                limitation_parts.append(
                    "A saved connection exists for this selected channel, but Google rejected the refresh token. "
                    "Reconnect this exact channel/account in Settings before Studio Agent can use private YouTube Analytics."
                )
            elif not bool(live_analytics.get("oauth_connected")):
                limitation_parts.append(
                    "Studio Agent only has cached/public channel data for this selected channel; private YouTube Analytics did not connect in this tool call."
                )
            if not bool(effective_video_metrics.get("video_level_retention_available")):
                limitation_parts.append(
                    "No per-video retention rows were returned to Studio Agent. To identify a specific 50-60% AVD short, refresh channel intelligence "
                    "after YouTube Analytics reconnects, or upload a YouTube Studio screenshot/export for that video."
                )
            limitation = " ".join(part for part in limitation_parts if part).strip()

            return {
                "channel_id": ch_id,
                "registry_key": reg_key or next(
                    (k for k, v in CHANNEL_KEY_TO_ID.items() if v == ch_id),
                    "",
                ),
                "channel_title": (
                    str((live_analytics.get("channel_title") if isinstance(live_analytics, dict) else "") or "").strip()
                    or str(record.get("title") or record.get("channel_handle") or "").strip()
                ),
                "insights": effective_insights,
                "growth_playbook": effective_growth,
                "analytics_data_quality": {
                    "harvest_present": effective_harvest,
                    "oauth_connected": bool(live_analytics.get("oauth_connected")),
                    "oauth_status": oauth_status,
                    "oauth_record_found": oauth_record_found,
                    "oauth_reconnect_required": oauth_reconnect_required,
                    "video_rows_available": int(effective_video_metrics.get("video_rows_available") or 0),
                    "video_level_retention_available": bool(
                        effective_video_metrics.get("video_level_retention_available")
                    ),
                    "retention_rows_available": int(
                        effective_video_metrics.get("retention_rows_available") or 0
                    ),
                    "effective_source": (
                        "youtube_analytics_live"
                        if use_live_metrics
                        else "catalyst_harvest_snapshot"
                    ),
                    "channel_counts_source": (
                        "youtube_live_oauth"
                        if bool(live_analytics.get("oauth_connected")) and live_insights
                        else "catalyst_harvest_snapshot"
                    ),
                    "requested_channel_id": str(args.get("channel_id") or "").strip(),
                    "resolved_channel_id": ch_id,
                    "lookup_channel_id": lookup_channel_id,
                    "snapshot_channel_id": snapshot_channel_id,
                    "requested_registry_key": reg_key,
                    "focus": "latest_upload" if latest_upload_focus else "general",
                    "latest_upload_available": bool(
                        isinstance(effective_video_metrics.get("latest_upload"), dict)
                        and effective_video_metrics.get("latest_upload")
                    ),
                    "latest_upload_source": (
                        str((effective_video_metrics.get("latest_upload") or {}).get("latest_upload_source") or "").strip()
                        if isinstance(effective_video_metrics.get("latest_upload"), dict)
                        else ""
                    ),
                    "channel_resolution": {
                        "matched": bool(channel_resolution.get("matched")),
                        "matched_by": str(channel_resolution.get("matched_by") or "none"),
                        "corrected": bool(channel_resolution.get("corrected")),
                        "requested_channel_id": str(channel_resolution.get("requested_channel_id") or ""),
                        "lookup_channel_id": lookup_channel_id,
                        "analytics_channel_id": ch_id,
                    },
                    "oauth_error": oauth_error,
                    "limitation": limitation,
                },
                "video_metrics": effective_video_metrics,
                "latest_upload": (
                    effective_video_metrics.get("latest_upload")
                    if isinstance(effective_video_metrics.get("latest_upload"), dict)
                    else {}
                ),
                "shortform_performance_comparison": shortform_comparison,
                "harvest_video_metrics": video_metrics,
                "youtube_analytics_live": live_analytics,
                "latest_video_velocity": velocity,
            }

        return json.dumps(_run_async(_fetch()), indent=2)

    if name == "get_public_search_trends":
        async def _fetch():
            from studio_analytics_router import (
                _default_queries_for_registry,
                _merge_public_search_orders,
                _predict_topics,
                _public_search_evidence_summary,
            )
            from long_form.catalyst_bridge import CHANNEL_KEY_TO_ID, fetch_channel_snapshot, shape_catalyst_insights

            from studio_agent.turn_plan import (
                channel_fallback_search_query,
                coerce_public_search_query,
                refine_public_search_query,
            )

            reg_key = str(args.get("registry_key") or "").strip()
            active_label = ""
            if reg_key:
                try:
                    from long_form.prompts.channels import get_channel

                    active_label = str(get_channel(reg_key).get("label") or reg_key)
                except Exception:
                    active_label = reg_key.replace("_", " ")
            from studio_agent.turn_plan import (
                default_discovery_search_query,
                is_allowed_discovery_search_query,
                is_banned_faceless_hooks_query,
                is_garbage_public_search_query,
                is_unusable_public_search_query,
            )

            fallback_query = channel_fallback_search_query(active_label, reg_key)
            raw_query = str(args.get("query") or "").strip()
            # Discovery seeds must not be coerce-wiped into "YouTube Shorts niche".
            if raw_query and is_allowed_discovery_search_query(raw_query) and not is_banned_faceless_hooks_query(raw_query):
                query = raw_query
            else:
                query = coerce_public_search_query(
                    raw_query,
                    active_label=active_label,
                    registry_key=reg_key,
                    fallback_query=fallback_query,
                )
                query = refine_public_search_query(query) or query
            if (
                not query
                or is_banned_faceless_hooks_query(query)
                or is_garbage_public_search_query(query)
                or is_unusable_public_search_query(query)
            ):
                query = default_discovery_search_query()
            days = max(1, min(int(args.get("days") or 30), 90))
            fresh = bool(args.get("fresh"))
            queries = [query] if query else (_default_queries_for_registry(reg_key) if reg_key else [default_discovery_search_query()])
            channel_titles: list[str] = []
            niche_keywords: list[str] = []
            if reg_key:
                try:
                    from long_form.prompts.channels import get_channel

                    ch = get_channel(reg_key)
                    niche_keywords = [str(ch.get("label") or reg_key)]
                    ch_id = CHANNEL_KEY_TO_ID.get(reg_key, "")
                    if ch_id:
                        ins = shape_catalyst_insights(fetch_channel_snapshot(ch_id))
                        channel_titles = [t.get("title", "") for t in ins.get("top_titles") or []]
                except Exception:
                    pass
            all_titles: list[str] = []
            rows: list[dict[str, Any]] = []
            from studio_agent.catalyst_prediction import filter_public_rows_for_query
            from studio_agent.live_demand import discovery_search_queries
            from studio_analytics_router import _row_usable_for_topic_prediction
            import youtube_quota

            quota_snap = youtube_quota.snapshot_sync()
            # Hard stop before burning more search.list (100 units each).
            if quota_snap.get("youtube_quota_exhausted") or not youtube_quota.can_afford_sync(100):
                return {
                    "source": "youtube_data_api_public_search",
                    "fresh": fresh,
                    "private_analytics": False,
                    "window_days": days,
                    "queries": queries,
                    "videos": [],
                    "evidence_summary": {
                        "total_rows": 0,
                        "hydrated_rows": 0,
                        "supported_rows": 0,
                        "error_rows": 1,
                    },
                    "predicted_topics": [],
                    "youtube_quota_exhausted": True,
                    "quota": quota_snap,
                    "error": (
                        "youtube_quota_exhausted: YouTube Data API daily search budget is spent "
                        f"({quota_snap.get('total_spent')}/{quota_snap.get('daily_cap')} units). "
                        f"{quota_snap.get('reset_hint')}"
                    ),
                    "note": "Do not invent view counts or trends. Tell the user quota is exhausted.",
                }

            # Quota survival: ONE seed first. Expand only if empty AND remaining quota.
            primary = str(queries[0] if queries else query or default_discovery_search_query()).strip()
            if (
                not primary
                or is_banned_faceless_hooks_query(primary)
                or is_garbage_public_search_query(primary)
                or is_unusable_public_search_query(primary)
            ):
                primary = default_discovery_search_query()
            expanded = [primary]
            primary_low = primary.lower()
            run_days = days
            if is_allowed_discovery_search_query(primary_low) or not str(args.get("query") or "").strip():
                run_days = max(days, 7)

            # Cost per merge: ~100 units cache-first, ~200 if fresh (dual search.list).
            unit_cost = 200 if fresh else 100

            async def _run_one(q: str, *, widen: bool = False) -> list[dict[str, Any]]:
                if not youtube_quota.can_afford_sync(unit_cost):
                    return []
                use_days = max(run_days, 30) if widen else run_days
                batch = await _merge_public_search_orders(
                    q,
                    days=use_days,
                    max_results=10,
                    fresh=fresh and not widen,
                    prefer_recent=bool((fresh and not widen) and use_days <= 7),
                )
                return filter_public_rows_for_query(batch, search_query=q, user_text=q)

            batch = await _run_one(primary, widen=False)
            rows.extend(batch)
            # If empty: one widen to 30d (single extra search) only if quota remains.
            if not rows and youtube_quota.can_afford_sync(unit_cost):
                batch = await _run_one(primary, widen=True)
                rows.extend(batch)
            # Multi-seed only when still empty AND we can afford at least one more search.
            if not rows and youtube_quota.can_afford_sync(unit_cost):
                for seed in discovery_search_queries():
                    if seed.lower() == primary_low:
                        continue
                    if not youtube_quota.can_afford_sync(unit_cost):
                        break
                    extra = await _run_one(seed, widen=False)
                    if extra:
                        expanded.append(seed)
                        rows.extend(extra)
                        break  # stop after first seed that returns rows
            all_titles.extend([
                str(r.get("title") or "")
                for r in rows
                if r.get("title") and _row_usable_for_topic_prediction(r, search_query=primary)
            ])
            queries = expanded
            # Defense in depth: never hand the model off-niche false positives.
            primary_q = str(queries[0] if queries else query or "").strip()
            rows = filter_public_rows_for_query(rows, search_query=primary_q, user_text=primary_q)
            predictions = _predict_topics(
                trending_titles=all_titles,
                channel_titles=channel_titles,
                niche_keywords=niche_keywords or queries,
            )
            # Drop predicted topics that are just fidget/homonym echoes of the raw search soup.
            cleaned_predictions: list[dict[str, Any]] = []
            for pred in list(predictions or []):
                if not isinstance(pred, dict):
                    continue
                topic = str(pred.get("topic") or pred.get("title") or "").strip()
                if not topic:
                    continue
                kept = filter_public_rows_for_query(
                    [{"title": topic}],
                    search_query=primary_q,
                    user_text=primary_q,
                )
                if kept:
                    cleaned_predictions.append(pred)
            predictions = cleaned_predictions
            # Register niche for later warm, but do not fire immediate warm when budget is tight.
            try:
                from studio_agent.catalyst_runtime import register_niche_query, schedule_session_catalyst_warm

                register_niche_query(primary_q)
                if not youtube_quota.background_should_pause_sync():
                    schedule_session_catalyst_warm(search_query=primary_q)
            except Exception:
                pass
            quota_after = youtube_quota.snapshot_sync()
            return {
                "source": "youtube_data_api_public_search",
                "fresh": fresh,
                "private_analytics": False,
                "window_days": days,
                "queries": queries,
                "videos": rows[:24],
                "evidence_summary": _public_search_evidence_summary(rows),
                "predicted_topics": predictions,
                "search_profiles": ["recent_momentum", "top_performers"],
                "quota": quota_after,
                "youtube_quota_exhausted": bool(quota_after.get("youtube_quota_exhausted")),
                "evidence_contract": (
                    "Every public trend claim must cite hydrated video_id/title/channel/views/likes/"
                    "engagement_rate/views_per_day/duration/published_at/cache_status/search_profile. "
                    "Snippet-only rows are candidates, not proof. "
                    "Day-trading niches exclude fidget/toy/game homonyms at the tool boundary. "
                    "If youtube_quota_exhausted is true, say so — do not invent stats."
                ),
                "note": (
                    "Fresh=true bypassed the public search cache for this request."
                    if fresh
                    else "Public search used cache-first mode to conserve YouTube quota. "
                    "Use support_label instead of guessing from search order."
                ),
            }

        return json.dumps(_run_async(_fetch()), indent=2)

    if name == "search_youtube_public":
        async def _fetch():
            from studio_analytics_router import _merge_public_search_orders, _public_search_evidence_summary

            from studio_agent.turn_plan import channel_fallback_search_query, coerce_public_search_query

            reg_key = str(args.get("registry_key") or "").strip()
            active_label = ""
            if reg_key:
                try:
                    from long_form.prompts.channels import get_channel

                    active_label = str(get_channel(reg_key).get("label") or reg_key)
                except Exception:
                    active_label = reg_key.replace("_", " ")
            from studio_agent.turn_plan import (
                default_discovery_search_query,
                is_allowed_discovery_search_query,
                is_banned_faceless_hooks_query,
                is_garbage_public_search_query,
                is_unusable_public_search_query,
                refine_public_search_query,
            )

            fallback_query = channel_fallback_search_query(active_label, reg_key)
            raw_query = str(args.get("query") or "").strip()
            if raw_query and is_allowed_discovery_search_query(raw_query) and not is_banned_faceless_hooks_query(raw_query):
                query = raw_query
            else:
                query = coerce_public_search_query(
                    raw_query,
                    active_label=active_label,
                    registry_key=reg_key,
                    fallback_query=fallback_query,
                )
            if not query:
                query = default_discovery_search_query()
            if is_banned_faceless_hooks_query(query) or is_garbage_public_search_query(query) or is_unusable_public_search_query(query):
                query = default_discovery_search_query()
            max_results = max(1, min(int(args.get("max_results") or 8), 15))
            days = max(1, min(int(args.get("days") or 30), 90))
            order = str(args.get("order") or "date").strip() or "date"
            fresh = bool(args.get("fresh")) if "fresh" in args else True
            if order in {"date", "relevance"}:
                fresh = True

            if not is_allowed_discovery_search_query(query):
                query = refine_public_search_query(query) or query
            if is_banned_faceless_hooks_query(query) or is_garbage_public_search_query(query):
                query = default_discovery_search_query()
            import youtube_quota

            if youtube_quota.snapshot_sync().get("youtube_quota_exhausted") or not youtube_quota.can_afford_sync(100):
                snap = youtube_quota.snapshot_sync()
                return {
                    "query": query,
                    "videos": [],
                    "youtube_quota_exhausted": True,
                    "quota": snap,
                    "error": (
                        "youtube_quota_exhausted: YouTube Data API daily search budget is spent "
                        f"({snap.get('total_spent')}/{snap.get('daily_cap')} units). "
                        f"{snap.get('reset_hint')}"
                    ),
                }
            run_days = max(days, 7) if is_allowed_discovery_search_query(query) else days
            rows = await _merge_public_search_orders(
                query,
                days=run_days,
                max_results=max_results,
                fresh=fresh,
                prefer_recent=bool(fresh and run_days <= 7),
            )
            from studio_agent.catalyst_prediction import filter_public_rows_for_query

            rows = filter_public_rows_for_query(rows, search_query=query, user_text=query)
            cache_statuses = {
                str(row.get("cache_status") or "").strip()
                for row in rows
                if isinstance(row, dict) and str(row.get("cache_status") or "").strip()
            }
            if "fresh" in cache_statuses:
                cache_status = "fresh"
            elif "cache" in cache_statuses:
                cache_status = "cache"
            elif "stale-cache" in cache_statuses:
                cache_status = "stale-cache"
            else:
                cache_status = "fresh" if fresh else "cache"
            try:
                from studio_agent.catalyst_runtime import schedule_session_catalyst_warm

                schedule_session_catalyst_warm(search_query=query)
            except Exception:
                pass
            prefer_recent = bool(fresh and days <= 7)
            return {
                "source": "youtube_data_api_public_search",
                "query": query,
                "order": "recent_momentum+top_performers",
                "window_days": days,
                "fresh": fresh,
                "fresh_public_search": fresh,
                "private_analytics": False,
                "cache_status": cache_status,
                "search_profiles": ["recent_momentum", "top_performers"],
                "videos": rows[: max_results * 2],
                "evidence_summary": _public_search_evidence_summary(rows),
                "evidence_contract": (
                    "Use hydrated_video_stats rows for performance claims. Prefer search_profile=top_performers "
                    "for proven winners and recent_momentum for uploads in the short window. "
                    "Ignore unsupported_or_low_signal rows for next-video predictions. "
                    "Snippet-only rows are lookup candidates, not proof of views, momentum, CTR, AVD, or retention. "
                    "Day-trading niches exclude fidget/toy/game homonyms at the tool boundary."
                ),
                "note": (
                    (
                        "Live public demand search uses YouTube search.list: order=date in the short window "
                        "(recent momentum) plus order=viewCount inside a short age cap (not all-time historical), "
                        "then hydrates stats via videos.list."
                        if prefer_recent
                        else "Live public demand search uses YouTube search.list: order=date in the short window "
                        "(recent momentum) plus order=viewCount for top performers, then hydrates via videos.list."
                    )
                    if fresh
                    else "Cached public search may omit the live date-ordered pass; set fresh=true for trending-now reads."
                ),
            }

        return json.dumps(_run_async(_fetch()), indent=2)

    if name == "get_fal_pricing":
        try:
            from long_form.fal_pricing import get_pricing_snapshot, ENDPOINTS

            snap = get_pricing_snapshot()
            endpoints = args.get("endpoints")
            if endpoints:
                filt = {k: v for k, v in ENDPOINTS.items() if v in endpoints or k in endpoints}
                snap = {**snap, "filtered_endpoints": filt}
            return json.dumps(snap, indent=2)
        except Exception as exc:
            return json.dumps({"error": str(exc), "note": "Set FAL_AI_KEY for live fal.ai pricing."})

    if name == "estimate_shortform_render_cost":
        session_models = _session_production_models(session_id)
        duration = max(5.0, min(90.0, float(args.get("duration_seconds") or 20)))
        visual_proof_only = bool(args.get("visual_proof_only"))
        default_scenes = 1 if visual_proof_only else max(1, round(duration / 5.0))
        scene_count = max(1, min(60, int(args.get("scene_count") or default_scenes)))
        animate = bool(args.get("animate", True))
        include_finalize = bool(args.get("include_finalize", True))
        # Prefer session picker. Only honor tool args that resolve to a known catalog id
        # (prevents LLM free-text like "LTX" / "Seedream T2I" from poisoning the quote).
        raw_image = str(args.get("image_model_id") or args.get("image_model") or "").strip().lower()
        raw_video = str(args.get("video_model") or "").strip().lower()
        session_image = str(session_models.get("image_model_id") or store.DEFAULT_IMAGE_MODEL)
        session_video = str(session_models.get("video_model") or store.DEFAULT_VIDEO_MODEL)
        known_image = raw_image in store.IMAGE_MODELS or raw_image in getattr(store, "_IMAGE_MODEL_ALIASES", {})
        known_video = raw_video in store.VIDEO_MODELS
        if known_image:
            image_model_id = store.normalize_image_model(raw_image)
        else:
            image_model_id = store.normalize_image_model(session_image)
        if known_video:
            video_model = store.normalize_video_model(raw_video)
        else:
            video_model = store.normalize_video_model(session_video)
        script_chars = max(400, int(round(duration * 3.0)))
        start_args = {
            "scene_count": scene_count,
            "duration_seconds": duration,
            "video_model": video_model,
            "image_model_id": image_model_id,
            "animate": animate,
            "visual_proof_only": visual_proof_only,
            "_full_auto": animate,
            "script_char_count": script_chars,
            "max_budget_usd": 25.0,
        }
        start_estimate = production_budget.estimate_tool_cost("start_shortform_generate", start_args)
        finalize_estimate = None
        if include_finalize:
            finalize_estimate = production_budget.estimate_tool_cost(
                "finalize_production",
                {
                    "scene_count": scene_count,
                    "duration_seconds": duration,
                    "script_char_count": script_chars,
                    "sfx_enabled": False,
                    "background_music": "off",
                    "max_budget_usd": 5.0,
                },
            )
        quote = production_budget.format_shortform_cost_quote(
            start_estimate,
            finalize_estimate=finalize_estimate,
        )
        total_usd = float(start_estimate.estimated_usd or 0.0) + float(
            (finalize_estimate.estimated_usd if finalize_estimate else 0.0) or 0.0
        )
        from studio_agent.cost_optimizer import optimize_shortform
        full_projection = optimize_shortform(
            scene_count=scene_count,
            duration_seconds=duration,
            image_model_id=image_model_id,
            video_model=video_model,
            animate=animate,
            selling_price_usd=args.get("selling_price_usd"),
            target_margin=float(args.get("target_margin") or 0.70),
            max_provider_cost_usd=args.get("max_provider_cost_usd"),
        )
        return json.dumps(
            {
                "source": "studio_production_budget",
                "session_models": session_models,
                "image_model_id": image_model_id,
                "video_model": video_model,
                "duration_seconds": duration,
                "scene_count": scene_count,
                "animate": animate,
                "start_budget": start_estimate.as_dict(),
                "finalize_budget": finalize_estimate.as_dict() if finalize_estimate else None,
                "total_estimated_usd": round(total_usd, 4),
                "full_project_projection": full_projection,
                "formatted_quote": quote,
                "evidence_contract": (
                    "All user-facing render cost quotes must cite image_model_id and video_model from this payload. "
                    "Never substitute LTX, Seedream T2I, or legacy pipeline defaults unless they appear here."
                ),
            },
            indent=2,
        )

    if name == "optimize_production_margin":
        from studio_agent.cost_optimizer import optimize_longform, optimize_shortform

        fmt = str(args.get("format") or content_format or "shortform").strip().lower()
        session_models = _session_production_models(session_id)
        common = {
            "selling_price_usd": args.get("selling_price_usd"),
            "target_margin": float(args.get("target_margin") or 0.70),
            "max_provider_cost_usd": args.get("max_provider_cost_usd"),
        }
        if "long" in fmt:
            result = optimize_longform(
                target_duration_sec=int(float(args.get("duration_seconds") or 1200)),
                image_model_id=str(args.get("image_model_id") or session_models.get("image_model_id") or "grok_imagine_standard"),
                **common,
            )
        else:
            duration = float(args.get("duration_seconds") or 30)
            result = optimize_shortform(
                scene_count=int(args.get("scene_count") or max(1, round(duration / 5))),
                duration_seconds=duration,
                image_model_id=str(args.get("image_model_id") or session_models.get("image_model_id") or "grok_imagine"),
                video_model=str(args.get("video_model") or session_models.get("video_model") or "grok_imagine_video"),
                animate=bool(args.get("animate", True)),
                **common,
            )
        return json.dumps({"read_only": True, "production_started": False, **result}, indent=2)

    if name == "ingest_cliplab_attachment":
        _require_cliplab_admin(user_id)
        from cliplab.config import CLIPLAB_UPLOAD_DIR
        from cliplab.pipeline import _safe_user_dir, new_job_id, run_ingest_pipeline, save_job_state

        source = str(args.get("attachment_path") or "").strip()
        if not source:
            source = _latest_video_attachment_path(session_id, user_id)
        src = Path(source)
        if not src.is_file():
            raise FileNotFoundError("No uploaded video attachment found. Attach an MP4/MOV/MKV/WEBM/M4V in this Studio Agent chat first.")
        ext = src.suffix.lower() or ".mp4"
        if ext not in {".mp4", ".mov", ".mkv", ".webm", ".m4v"}:
            raise ValueError("ClipLab source must be MP4, MOV, MKV, WEBM, or M4V.")
        uid = str(user_id or "").strip()
        upload_id = new_job_id("clipvid").replace("clipvid_", "vid_")
        dest_dir = CLIPLAB_UPLOAD_DIR / _safe_user_dir({"id": uid})
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / f"{upload_id}{ext}"
        shutil.copy2(src, dest)
        ctx = _session_channel_context(session_id)
        channel_id = str(args.get("channel_id") or ctx.get("channel_id") or "").strip()
        registry_key = str(args.get("registry_key") or ctx.get("registry_key") or "").strip()
        fal_key = str(os.getenv("FAL_AI_KEY") or os.getenv("FAL_KEY") or "").strip()
        job_id = new_job_id("clipi")
        local_jobs = {
            job_id: {
                "status": "queued",
                "progress": 0,
                "type": "cliplab_ingest",
                "lane": "cliplab",
                "video_id": upload_id,
                "user_id": uid,
                "channel_id": channel_id,
                "registry_key": registry_key,
                "created_at": time.time(),
            }
        }
        save_job_state(job_id, {
            **local_jobs[job_id],
            "video_path": str(dest),
            "source_attachment": str(src),
            "cues": [],
        })

        def _worker() -> None:
            _run_async(run_ingest_pipeline(
                job_id,
                local_jobs,
                {"id": uid},
                video_path=str(dest),
                video_id=upload_id,
                fal_key=fal_key,
            ))

        threading.Thread(target=_worker, name=f"cliplab-ingest-{job_id}", daemon=True).start()
        telemetry.record_event(
            user_id,
            "cliplab_agent_ingest_started",
            {"job_id": job_id, "video_id": upload_id, "channel_id": channel_id, "registry_key": registry_key},
            session_id=session_id,
        )
        return json.dumps({
            "status": "running",
            "job_id": job_id,
            "kind": "cliplab",
            "video_id": upload_id,
            "channel_id": channel_id,
            "registry_key": registry_key,
            "poll_tool": "poll_cliplab_job",
            "next_action": (
                "Poll poll_cliplab_job until ingest is complete, then call analyze_cliplab_video "
                "with the selected channel context and clip-finding prompt."
            ),
        }, indent=2, ensure_ascii=True)

    if name == "analyze_cliplab_video":
        _require_cliplab_admin(user_id)
        from cliplab.pipeline import load_job_state, new_job_id, run_analyze_pipeline, save_job_state

        video_id = str(args.get("video_id") or "").strip()
        prompt = str(args.get("prompt") or "").strip()
        if not video_id or not prompt:
            raise ValueError("video_id and prompt required")
        ctx = _session_channel_context(session_id)
        channel_id = str(args.get("channel_id") or ctx.get("channel_id") or "").strip()
        registry_key = str(args.get("registry_key") or ctx.get("registry_key") or "").strip()
        max_segments = max(1, min(int(args.get("max_segments") or 12), 40))
        provider = str(args.get("provider") or os.getenv("CLIPLAB_PROVIDER") or "auto").strip().lower()
        analysis_session = (
            store.get_session(
                str(session_id),
                user_id=str(user_id or ""),
                reconcile_jobs=False,
            )
            if session_id
            else {}
        ) or {}
        analysis_model = str(analysis_session.get("model") or "").strip()
        job_id = new_job_id("clipa")
        local_jobs = {
            job_id: {
                "status": "queued",
                "progress": 0,
                "type": "cliplab_analyze",
                "lane": "cliplab",
                "video_id": video_id,
                "user_id": str(user_id or ""),
                "channel_id": channel_id,
                "registry_key": registry_key,
                "provider": provider,
                "created_at": time.time(),
            }
        }
        save_job_state(job_id, {
            **local_jobs[job_id],
            "prompt": prompt,
            "segments": [],
        })

        def _worker() -> None:
            _run_async(run_analyze_pipeline(
                job_id,
                local_jobs,
                video_id=video_id,
                prompt=prompt,
                max_segments=max_segments,
                json_completion=None,
                user_id=str(user_id or ""),
                channel_id=channel_id,
                registry_key=registry_key,
                source="studio_agent_cliplab",
                provider=provider,
                model=analysis_model,
            ))

        threading.Thread(target=_worker, name=f"cliplab-analyze-{job_id}", daemon=True).start()
        telemetry.record_event(
            user_id,
            "cliplab_agent_analyze_started",
            {"job_id": job_id, "video_id": video_id, "channel_id": channel_id, "registry_key": registry_key},
            session_id=session_id,
        )
        return json.dumps({
            "status": "running",
            "job_id": job_id,
            "kind": "cliplab",
            "video_id": video_id,
            "channel_id": channel_id,
            "registry_key": registry_key,
            "provider": provider,
            "prompt": prompt,
            "poll_tool": "poll_cliplab_job",
            "next_action": "Poll poll_cliplab_job until status is complete, then choose segment_indices for render_cliplab_segments.",
            "current": load_job_state(job_id),
        }, indent=2, ensure_ascii=True)

    if name == "render_cliplab_segments":
        _require_cliplab_admin(user_id)
        from cliplab.pipeline import new_job_id, run_render_pipeline, save_job_state

        video_id = str(args.get("video_id") or "").strip()
        analyze_job_id = str(args.get("analyze_job_id") or args.get("prompt_run_id") or "").strip()
        raw_indices = args.get("segment_indices") or []
        indices = [int(x) for x in raw_indices] if isinstance(raw_indices, list) else []
        if not video_id or not analyze_job_id or not indices:
            raise ValueError("video_id, analyze_job_id, and segment_indices required")
        ctx = _session_channel_context(session_id)
        channel_id = str(args.get("channel_id") or ctx.get("channel_id") or "").strip()
        registry_key = str(args.get("registry_key") or ctx.get("registry_key") or "").strip()
        job_id = new_job_id("clipr")
        local_jobs = {
            job_id: {
                "status": "queued",
                "progress": 0,
                "type": "cliplab_render",
                "lane": "cliplab",
                "video_id": video_id,
                "user_id": str(user_id or ""),
                "channel_id": channel_id,
                "registry_key": registry_key,
                "created_at": time.time(),
            }
        }
        save_job_state(job_id, {**local_jobs[job_id], "analyze_job_id": analyze_job_id, "segment_indices": indices})

        def _worker() -> None:
            _run_async(run_render_pipeline(
                job_id,
                local_jobs,
                video_id=video_id,
                analyze_job_id=analyze_job_id,
                segment_indices=indices,
                burn_captions=bool(args.get("burn_captions", True)),
                user_id=str(user_id or ""),
                channel_id=channel_id,
                registry_key=registry_key,
                source="studio_agent_cliplab",
            ))

        threading.Thread(target=_worker, name=f"cliplab-render-{job_id}", daemon=True).start()
        telemetry.record_event(
            user_id,
            "cliplab_agent_render_started",
            {"job_id": job_id, "video_id": video_id, "analyze_job_id": analyze_job_id, "segment_indices": indices},
            session_id=session_id,
        )
        return json.dumps({
            "status": "running",
            "job_id": job_id,
            "kind": "cliplab",
            "video_id": video_id,
            "analyze_job_id": analyze_job_id,
            "segment_indices": indices,
            "poll_tool": "poll_cliplab_job",
            "next_action": "Poll poll_cliplab_job until clips are ready.",
        }, indent=2, ensure_ascii=True)

    if name == "remix_cliplab_short":
        _require_cliplab_admin(user_id)
        from cliplab.pipeline import new_job_id, run_remix_pipeline, save_job_state

        video_id = str(args.get("video_id") or "").strip()
        if not video_id:
            raise ValueError("video_id required")
        style_preset = str(args.get("style_preset") or "clean_viral").strip().lower()
        caption_style = str(args.get("caption_style") or "bold").strip().lower()
        edit_intensity = str(args.get("edit_intensity") or "medium").strip().lower()
        background_mode = str(args.get("background_mode") or "blur").strip().lower()
        if style_preset not in {"clean_viral", "empire", "empire_magnates", "documentary", "streamer", "high_energy"}:
            style_preset = "clean_viral"
        if caption_style not in {"bold", "minimal", "empire"}:
            caption_style = "bold"
        if edit_intensity not in {"low", "medium", "high"}:
            edit_intensity = "medium"
        if background_mode not in {"blur", "solid"}:
            background_mode = "blur"
        ctx = _session_channel_context(session_id)
        channel_id = str(args.get("channel_id") or ctx.get("channel_id") or "").strip()
        registry_key = str(ctx.get("registry_key") or "").strip()
        job_id = new_job_id("remix")
        local_jobs = {
            job_id: {
                "status": "queued",
                "progress": 0,
                "type": "cliplab_remix",
                "lane": "cliplab",
                "video_id": video_id,
                "user_id": str(user_id or ""),
                "style_preset": style_preset,
                "caption_style": caption_style,
                "edit_intensity": edit_intensity,
                "background_mode": background_mode,
                "created_at": time.time(),
            }
        }
        save_job_state(job_id, {**local_jobs[job_id], "remix": {}})

        def _worker() -> None:
            _run_async(run_remix_pipeline(
                job_id,
                local_jobs,
                video_id=video_id,
                style_preset=style_preset,
                caption_style=caption_style,
                edit_intensity=edit_intensity,
                background_mode=background_mode,
                burn_captions=bool(args.get("burn_captions", True)),
                catalyst_channel_id=channel_id,
                notes=str(args.get("notes") or "")[:500],
                user_id=str(user_id or ""),
                registry_key=registry_key,
                source="studio_agent_cliplab",
            ))

        threading.Thread(target=_worker, name=f"cliplab-remix-{job_id}", daemon=True).start()
        telemetry.record_event(
            user_id,
            "cliplab_agent_remix_started",
            {"job_id": job_id, "video_id": video_id, "style_preset": style_preset, "channel_id": channel_id},
            session_id=session_id,
        )
        return json.dumps({
            "status": "running",
            "job_id": job_id,
            "kind": "cliplab",
            "video_id": video_id,
            "style_preset": style_preset,
            "caption_style": caption_style,
            "edit_intensity": edit_intensity,
            "poll_tool": "poll_cliplab_job",
            "next_action": "Poll poll_cliplab_job until the remixed MP4 URL is ready.",
        }, indent=2, ensure_ascii=True)

    if name == "poll_cliplab_job":
        _require_cliplab_admin(user_id)
        from cliplab.pipeline import load_job_state

        job_id = str(args.get("job_id") or "").strip()
        if not job_id:
            raise ValueError("job_id required")
        state = load_job_state(job_id)
        if not state:
            return json.dumps({
                "job_id": job_id,
                "kind": "cliplab",
                "status": "unknown",
                "running": True,
                "note": "Job has not persisted final state yet. Poll again shortly.",
            }, indent=2)
        status = str(state.get("status") or ("complete" if (state.get("segments") or state.get("clips") or state.get("remix")) else "running"))
        state = dict(state)
        state.update({
            "job_id": job_id,
            "kind": "cliplab",
            "status": status,
            "running": status not in {"complete", "error", "failed"},
        })
        return json.dumps(state, indent=2, ensure_ascii=True)

    if name == "fetch_archival_for_video":
        from media_sources import fetch_archival_for_video

        topic = str(args.get("topic") or "").strip()
        if not topic:
            raise ValueError("topic required")
        manifest = fetch_archival_for_video(
            topic,
            title=str(args.get("title") or "").strip(),
            registry_key=str(args.get("registry_key") or "").strip(),
            preset=str(args.get("preset") or "").strip(),
            blueprint_job_id=str(args.get("blueprint_job_id") or "").strip(),
            limit_per_scene=int(args.get("limit_per_scene") or 5),
            resolve_downloads=bool(args.get("resolve_downloads", True)),
            production_job_id=str(args.get("production_job_id") or "").strip(),
        )
        telemetry.record_event(
            user_id,
            "archival_manifest_built",
            {
                "topic": topic[:200],
                "preset": manifest.get("preset"),
                "scene_count": manifest.get("scene_count"),
                "production_job_id": manifest.get("production_job_id"),
            },
            session_id=session_id,
        )
        return json.dumps(manifest, indent=2, ensure_ascii=False)

    if name == "resolve_archival_asset":
        from media_sources import resolve_archival_asset

        item = {
            "source": str(args.get("source") or ""),
            "id": str(args.get("id") or ""),
            "title": str(args.get("title") or ""),
            "page_url": str(args.get("page_url") or ""),
            "download_url": str(args.get("download_url") or ""),
            "media_type": str(args.get("media_type") or ""),
        }
        if not item["source"]:
            raise ValueError("source required")
        return json.dumps(resolve_archival_asset(item), indent=2, ensure_ascii=True)

    if name == "search_archival_media":
        from media_sources import search_archival

        query = str(args.get("query") or "").strip()
        if not query:
            raise ValueError("query required")
        sources = args.get("sources") or None
        preset = str(args.get("preset") or "").strip()
        limit = int(args.get("limit_per_source") or 8)
        data = search_archival(query, sources=sources, preset=preset, limit_per_source=limit)
        return json.dumps(data, indent=2, ensure_ascii=True)

    if name == "analyze_reference_video":
        from studio_agent import competitor

        url = str(args.get("url") or "").strip()
        local_path = _latest_video_attachment_path(
            session_id,
            user_id,
            hint=str(args.get("local_path") or "").strip(),
        )
        fmt = str(args.get("content_format") or content_format or "short")
        if local_path:
            source_name = str(args.get("source_name") or Path(local_path).name).strip()
            telemetry.record_event(
                user_id,
                "reference_video_started",
                {"local_path": local_path[:500], "source_name": source_name[:200]},
                session_id=session_id,
            )
            job_id = competitor.start_analysis_from_path(
                local_path,
                user_id=str(user_id or ""),
                source_name=source_name,
                scene_threshold=float(args.get("scene_threshold") or 0.3),
                max_frames=int(args.get("max_frames") or 40),
                content_format=fmt,
            )
            note = (
                "Poll poll_render_job(job_id, kind='competitor'): loading upload → keyframes → "
                "pacing → audio → complete. Then build_scene_blueprint_from_reference."
            )
            source = "uploaded_reference"
        elif url:
            telemetry.record_event(
                user_id,
                "reference_video_started",
                {"url": url[:500]},
                session_id=session_id,
            )
            job_id = competitor.start_analysis(
                url,
                user_id=str(user_id or ""),
                scene_threshold=float(args.get("scene_threshold") or 0.3),
                max_frames=int(args.get("max_frames") or 40),
                content_format=fmt,
            )
            note = (
                "Poll poll_render_job(job_id, kind='competitor'): metadata → download → keyframes → "
                "pacing → audio → complete. Then build_scene_blueprint_from_reference."
            )
            source = "youtube_url"
        else:
            raise ValueError("url or local_path required")
        out = {
            "status": "running",
            "job_id": job_id,
            "kind": "competitor",
            "source": source,
            "content_format": competitor.analysis_profile(fmt)["content_format"],
            "stages": [s[0] for s in competitor.STAGES],
            "note": note,
        }
        return json.dumps(out, indent=2)

    if name == "retry_reference_analysis":
        from studio_agent import competitor

        job_id = str(args.get("job_id") or "").strip()
        if not job_id:
            raise ValueError("job_id required")
        stages_raw = args.get("stages") or []
        stages = [str(stage or "").strip().lower() for stage in stages_raw if str(stage or "").strip()]
        result = competitor.retry_reference_stages(job_id, stages=stages or None)
        return json.dumps(result, indent=2, ensure_ascii=False)

    if name == "build_scene_blueprint_from_reference":
        from studio_agent import reference_planner

        job_id = str(args.get("job_id") or "").strip()
        topic = str(args.get("topic") or "").strip()
        if not job_id or not topic:
            raise ValueError("job_id and topic required")
        blueprint = reference_planner.build_scene_blueprint(
            job_id,
            topic=topic,
            channel_style=str(args.get("channel_style") or "premium_doc"),
            characters_per_scene=int(args.get("characters_per_scene") or 1),
            visual_brief=str(args.get("visual_brief") or "").strip(),
            target_scene_count=int(args["target_scene_count"]) if args.get("target_scene_count") else None,
        )
        telemetry.record_event(
            user_id,
            "scene_blueprint_built",
            {"job_id": job_id, "topic": topic[:200], "scene_count": len(blueprint.get("scenes") or [])},
            session_id=session_id,
        )
        return json.dumps(blueprint, indent=2, ensure_ascii=False)

    if name == "recommend_video_topics":
        async def _topics():
            from long_form.catalyst_bridge import (
                CHANNEL_KEY_TO_ID,
                assess_channel_growth,
                fetch_channel_snapshot,
                shape_catalyst_insights,
            )
            from studio_analytics_router import (
                _merge_public_search_orders,
                _predict_topics,
                _public_search_evidence_summary,
            )

            reg_key = str(args.get("registry_key") or "").strip()
            selected_channel_id = str(args.get("channel_id") or "").strip()
            niche = str(args.get("niche_query") or "").strip()
            days = int(args.get("days") or 30)
            fresh = bool(args.get("fresh"))

            channel_block: dict[str, Any] = {}
            ch_id = selected_channel_id or (CHANNEL_KEY_TO_ID.get(reg_key, "") if reg_key else "")
            if ch_id:
                rec = fetch_channel_snapshot(ch_id)
                ins = shape_catalyst_insights(rec)
                harvest = bool((rec or {}).get("analytics_snapshot"))
                channel_block = {
                    "registry_key": reg_key or next((k for k, v in CHANNEL_KEY_TO_ID.items() if v == ch_id), ""),
                    "channel_id": ch_id,
                    "insights": ins,
                    "growth_playbook": assess_channel_growth(ins, harvest_present=harvest),
                }

            queries = [niche] if niche else (
                [reg_key.replace("_", " ")] if reg_key else ["YouTube documentary viral 2026"]
            )
            videos: list[dict[str, Any]] = []
            titles: list[str] = []
            from studio_agent.turn_plan import refine_public_search_query

            for q in queries[:2]:
                q_ref = refine_public_search_query(q) or q
                batch = await _merge_public_search_orders(
                    q_ref,
                    days=days,
                    max_results=12,
                    fresh=fresh,
                    prefer_recent=bool(fresh and int(days or 30) <= 7),
                )
                videos.extend(batch)
                titles.extend([str(r.get("title") or "") for r in batch if r.get("title")])

            top_titles = (channel_block.get("insights") or {}).get("top_titles") or []
            channel_titles = [t.get("title", "") for t in top_titles if isinstance(t, dict)]
            predictions = _predict_topics(
                trending_titles=titles,
                channel_titles=channel_titles,
                niche_keywords=queries,
            )
            playbook = channel_block.get("growth_playbook") or {}
            stage = playbook.get("stage", "unknown")
            framing = (
                "You don't need a topic yet â€” here's your positioning sprint."
                if stage in ("brand_new", "early") and not channel_titles
                else "Double down on what's already working on your channel."
            )
            return {
                "framing_for_creator": framing,
                "fresh_public_search": fresh,
                "channel": channel_block,
                "trending_sample": videos[:15],
                "evidence_summary": _public_search_evidence_summary(videos),
                "recommended_topics": predictions[:12],
                "evidence_contract": (
                    "Recommendations are only as strong as their cited evidence. Quote the matching "
                    "trending_sample rows with hydrated stats and cache_status; if rows are snippet_only "
                    "or low-signal, label the recommendation experimental."
                ),
                "next_actions": (playbook.get("recommended_next_actions") or [])[:5],
                "hardest_steps_reminder": [
                    "Script-writing + story beats (use reference blueprint if you linked a Lume/MrBeast video)",
                    "Packaging: title + thumbnail before you render",
                ],
            }

        result = json.dumps(_run_async(_topics()), indent=2, ensure_ascii=False)
        telemetry.record_event(
            user_id,
            "topic_recommendations",
            {"registry_key": str(args.get("registry_key") or ""), "niche": str(args.get("niche_query") or "")[:120]},
            session_id=session_id,
        )
        return result

    if name == "search_music":
        from media_sources import search_music

        query = str(args.get("query") or "").strip()
        if not query:
            raise ValueError("query required")
        data = search_music(
            query,
            limit=int(args.get("limit") or 12),
            instrumental=bool(args.get("instrumental")),
        )
        return json.dumps(data, indent=2, ensure_ascii=True)

    if name == "search_sfx":
        from media_sources import search_sfx

        query = str(args.get("query") or "").strip()
        if not query:
            raise ValueError("query required")
        data = search_sfx(
            query,
            limit=int(args.get("limit") or 12),
            cc0_only=bool(args.get("cc0_only", True)),
        )
        return json.dumps(data, indent=2, ensure_ascii=True)

    if name == "finalize_longform_render":
        job_id = str(args.get("job_id") or "").strip()
        if not job_id:
            raise ValueError("job_id required")
        from studio_agent.jobs import finalize_longform_job

        out = finalize_longform_job(job_id)
        return json.dumps({
            **out,
            "poll_kind": "longform",
            "note": "Studio UI auto-tracks progress. Poll poll_render_job until status complete.",
        }, indent=2)

    if name == "refresh_channel_intelligence":
        ch_id = str(args.get("channel_id") or "").strip()
        if not ch_id:
            raise ValueError("channel_id required")
        uid = str(user_id or "").strip()
        if not uid:
            raise ValueError("sign in required")

        async def _sync():
            from youtube import _youtube_sync_and_persist_for_user

            return await _youtube_sync_and_persist_for_user(uid, ch_id)

        channel = _run_async(_sync())
        telemetry.record_event(
            user_id,
            "channel_intelligence_refresh",
            {"channel_id": ch_id},
            session_id=session_id,
        )
        snap = channel.get("analytics_snapshot") if isinstance(channel, dict) else {}
        return json.dumps({
            "ok": True,
            "channel_id": ch_id,
            "title": (channel or {}).get("title") if isinstance(channel, dict) else "",
            "packaging_learnings": (snap or {}).get("packaging_learnings") or [],
            "retention_learnings": (snap or {}).get("retention_learnings") or [],
            "note": "Catalyst harvest updated. Use get_channel_analytics for full playbook.",
        }, indent=2, ensure_ascii=True)

    if name == "record_production_feedback":
        ch_id = str(args.get("channel_id") or "").strip()
        outcome = str(args.get("outcome") or "").strip()
        if not ch_id or not outcome:
            raise ValueError("channel_id and outcome required")
        payload = {
            "channel_id": ch_id,
            "video_id": str(args.get("video_id") or "").strip(),
            "outcome": outcome,
            "notes": str(args.get("notes") or "")[:2000],
            "views": int(args.get("views") or 0),
            "ctr_percent": float(args.get("ctr_percent") or 0),
        }
        telemetry.record_event(
            user_id,
            "production_feedback",
            payload,
            session_id=session_id,
        )
        try:
            from studio_agent import memory

            memory.record_feedback_memory(
                str(user_id or ""),
                channel_id=ch_id,
                outcome=outcome,
                video_id=payload["video_id"],
                notes=payload["notes"],
                views=payload["views"],
                ctr_percent=payload["ctr_percent"],
            )
        except Exception:
            pass
        return json.dumps({
            "ok": True,
            "recorded": True,
            "memory_saved": True,
            "message": "Logged for NYPTID training, channel recommendations, and Studio Agent memory.",
        }, indent=2)

    if name == "poll_render_job":
        job_id = str(args.get("job_id") or "").strip()
        kind = str(args.get("kind") or "longform").strip().lower()
        if not job_id:
            raise ValueError("job_id required")
        if kind in {"shortform", "competitor", "cliplab"}:
            from studio_agent.jobs import get_job_snapshot

            return json.dumps(get_job_snapshot(job_id, kind), indent=2, ensure_ascii=True)
        from long_form.pipeline import load_state, get_status

        st = load_state(job_id) or {}
        status = get_status(job_id) or {}
        return json.dumps({
            "job_id": job_id,
            "kind": kind,
            "phase": status.get("phase") or st.get("phase"),
            "percent": status.get("percent", st.get("percent")),
            "error": status.get("error") or st.get("error"),
            "awaiting_approval": (status.get("phase") or st.get("phase")) == "awaiting_approval",
        }, indent=2)

    raise ValueError(f"unknown tool: {name}")


def execute_tool_logged(
    name: str,
    arguments: dict[str, Any],
    *,
    user_id: str,
    content_format: str,
    session_id: str | None = None,
) -> str:
    """Run a tool with telemetry, budget enforcement, and atomic credit hold."""
    if name in {
        "generate_longform_outline",
        "expand_longform_chapter",
        "start_longform_render",
        "expand_longform_visual_proof",
        "list_longform_scenes",
        "regenerate_longform_still",
        "generate_longform_thumbnails",
        "regenerate_longform_thumbnail",
        "finalize_longform_render",
    }:
        # Authorization and static pipeline validation must happen before an
        # idempotency claim, RunPod workspace stage, or credit reservation.
        _require_longform_entitlement(user_id)
        if name == "start_longform_render":
            from long_form.prompts.channels import get_channel
            from long_form import pipeline as lf_pipeline

            channel = dict(get_channel(str((arguments or {}).get("channel_key") or "").strip()))
            lf_pipeline.validate_channel_pipeline(channel)
    if session_id and name in {
        "expand_visual_proof_shortform",
        "regenerate_production_scene_still",
        "regenerate_production_scene",
        "animate_production_scenes",
        "repair_production_scene_animation",
        "audit_and_repair_production_scenes",
    }:
        # Picker state is binding execution input, not an LLM-authored hint.
        # Capture it before idempotency/budget/RunPod routing, while local repair
        # loops continue re-reading the same session before each provider call.
        route = _repair_route_snapshot(session_id)
        arguments = dict(arguments or {})
        arguments["image_model_id"] = route.get("image_model_id")
        arguments["video_model"] = route.get("video_model")
        arguments["_media_route_revision"] = int(route.get("revision") or 1)
    budget_estimate = None
    credit_reservation: dict[str, Any] | None = None
    billed_with_actuals = False
    runpod_route = bool(
        _runpod_production_enabled()
        and str(name or "").strip() in RUNPOD_PRODUCTION_TOOL_ALLOWLIST
    )
    runpod_dispatched = False
    runpod_storage_stage: dict[str, Any] | None = None
    runpod_lease_dispatch_id = ""
    runpod_lease_created = False
    command_id = _runpod_command_id(arguments)
    local_mutation_claim = None
    try:
        if not runpod_route and command_id:
            from studio_agent import idempotent_mutations

            arguments = dict(arguments or {})
            if name == "expand_visual_proof_shortform":
                arguments.setdefault("command_id", command_id)
            local_mutation_claim, replay = idempotent_mutations.begin(
                tool_name=str(name or ""),
                arguments=arguments,
                command_id=command_id,
                user_id=str(user_id or ""),
            )
            if replay is not None:
                return json.dumps(replay, indent=2, ensure_ascii=False)
        if runpod_route and _runpod_workspace_kind(name) == "longform" and not runpod_longform_enabled():
            raise RuntimeError(
                "RunPod long-form production is disabled by STUDIO_RUNPOD_LONGFORM_ENABLED; "
                "local fallback is disabled."
            )
        if runpod_route and not command_id:
            raise RuntimeError(
                "RunPod production dispatch requires a stable command_id; "
                "local fallback is disabled to prevent duplicate billable work."
            )
        if runpod_route and name in {"start_shortform_generate", "start_longform_render"}:
            arguments = dict(arguments or {})
            studio_job_id = str(arguments.get("studio_job_id") or "").strip()
            if not studio_job_id:
                studio_job_id = _runpod_studio_job_id(
                    name,
                    command_id=command_id,
                    user_id=user_id,
                    session_id=session_id,
                )
            arguments["studio_job_id"] = studio_job_id
            arguments["_requested_job_id"] = studio_job_id
        if runpod_route:
            # RunPod workers use the attached network volume, while Studio's
            # control plane keeps its canonical workspaces on Fly/local disk.
            # Validate the return path for every production command before a
            # credit hold is created. Existing-job operations must upload the
            # exact workspace first (for example the approved Scene 1 that an
            # expansion must preserve). A storage failure is therefore always
            # a definite no-submit failure with no local production fallback.
            from studio_agent import runpod_storage

            runpod_storage.assert_configured()
            workspace_kind = _runpod_workspace_kind(name)
            workspace_job_id = str(
                (arguments or {}).get("job_id")
                or (arguments or {}).get("studio_job_id")
                or ""
            ).strip()
            if not workspace_job_id:
                raise RuntimeError("RunPod production dispatch requires a stable Studio job_id")
            from studio_agent import runpod_bridge

            runpod_lease_dispatch_id = semantic_dispatch_id(
                name,
                dict(arguments or {}),
                command_id=command_id,
                user_id=user_id,
            )
            lease = runpod_bridge.acquire_production_lease(
                runpod_lease_dispatch_id,
                studio_job_id=workspace_job_id,
                tool=name,
            )
            runpod_lease_created = bool(lease.get("acquired"))
            if name not in {"start_shortform_generate", "start_longform_render"} and runpod_lease_created:
                runpod_storage_stage = runpod_storage.stage_job_workspace(
                    workspace_job_id,
                    workspace_kind,
                )
            elif name not in {"start_shortform_generate", "start_longform_render"}:
                runpod_storage_stage = {
                    "ok": True,
                    "status": "already_staged_for_active_dispatch",
                    "job_id": workspace_job_id,
                    "kind": workspace_kind,
                    "files_uploaded": 0,
                    "idempotent_replay": True,
                }
            else:
                runpod_storage_stage = {
                    "ok": True,
                    "status": "storage_ready",
                    "job_id": workspace_job_id,
                    "kind": workspace_kind,
                    "files_uploaded": 0,
                }
        budget_estimate = production_budget.enforce_budget(name, arguments)
        provider_block = _public_provider_block_message(name, arguments, budget_estimate, user_id)
        if provider_block:
            raise RuntimeError(provider_block)
        if budget_estimate is not None and str(user_id or "").strip():
            import unified_credits as uc

            credit_reservation = uc.reserve_usd(
                user_id,
                budget_estimate.estimated_usd,
                reason=f"studio_tool:{name}",
                metadata={
                    "tool": name,
                    "session_id": session_id,
                    "budget": budget_estimate.as_dict(),
                },
                # Animation can make one full-cost repair attempt after semantic
                # QA rejection. Actual spend reconciliation refunds unused hold.
                repair_reserve_pct=(
                    1.0
                    if name in {
                        "animate_production_scenes",
                        "repair_production_scene_animation",
                        "audit_and_repair_production_scenes",
                    }
                    else 0.25
                ),
            )
            if name in {
                "start_shortform_generate",
                "expand_visual_proof_shortform",
                "animate_production_scenes",
                "repair_production_scene_animation",
                "audit_and_repair_production_scenes",
                "finalize_production",
                "edit_production_scene_still",
                "edit_production_scenes_still",
                "regenerate_production_scene_still",
                "regenerate_production_scene",
                "re_edit_production",
            } or runpod_route:
                arguments = dict(arguments or {})
                arguments["_credit_reservation"] = credit_reservation
                arguments["_credit_session_id"] = session_id or ""
                arguments["_credit_budget"] = budget_estimate.as_dict()
        if runpod_route:
            from studio_agent import runpod_bridge

            receipt = runpod_bridge.dispatch_production_tool(
                name,
                dict(arguments or {}),
                command_id=command_id,
                user_id=user_id,
                session_id=session_id or "",
                content_format=content_format,
            )
            payload = dict(receipt or {})
            existing_job_id = str(
                (arguments or {}).get("job_id")
                or (arguments or {}).get("studio_job_id")
                or ""
            ).strip()
            if existing_job_id:
                # Expansion/edit/animation continues the exact existing Studio
                # job.  The RunPod id is transport identity, never a replacement.
                payload["job_id"] = existing_job_id
                payload.setdefault("studio_job_id", existing_job_id)
            payload["execution_backend"] = "runpod_serverless"
            if runpod_storage_stage is not None:
                payload["workspace_stage"] = dict(runpod_storage_stage)
            replay = bool(payload.get("idempotent_replay") or payload.get("claim_pending"))
            if replay and runpod_lease_created and not bool(payload.get("claim_pending")):
                runpod_bridge.release_production_lease(runpod_lease_dispatch_id)
                runpod_lease_created = False
            credits: dict[str, Any] = {
                "charged": 0,
                "local_commit": False,
                "status": (
                    "original_dispatch_pending" if replay else "pending_worker_cost_reconciliation"
                ),
                "note": (
                    "Provider cost is reconciled after the RunPod worker reports durable cost facts."
                ),
            }
            if credit_reservation:
                reservation_id = str(credit_reservation.get("reservation_id") or "")
                if reservation_id:
                    credits["reservation_id"] = reservation_id
                if replay and not credit_reservation.get("unlimited"):
                    import unified_credits as uc

                    uc.release_reservation(
                        user_id,
                        reservation_id,
                        reason=f"studio_tool_runpod_replay:{name}",
                    )
                    credits["reservation_released"] = True
            payload["credits"] = credits
            result = json.dumps(payload, indent=2, ensure_ascii=False)
            runpod_dispatched = True
            # An accepted async dispatch owns the hold.  The API must never
            # commit an estimate before worker-reported actuals are available.
            billed_with_actuals = True
        else:
            result = execute_tool(
                name, arguments, user_id=user_id, content_format=content_format, session_id=session_id
            )
        result = production_budget.with_budget_metadata(result, budget_estimate, arguments)
        if credit_reservation and not runpod_dispatched and name in LONGFORM_TEXT_METERED_TOOLS:
            import unified_credits as uc

            payload = json.loads(result or "{}")
            if not isinstance(payload, dict):
                raise RuntimeError("Long-form text execution returned invalid billing data")
            billing = payload.get("billing") if isinstance(payload.get("billing"), dict) else {}
            usage_reported = bool(billing.get("usage_reported"))
            provider_usd = (
                max(0.0, float(billing.get("provider_usd") or 0.0))
                if usage_reported
                else max(0.0, float(budget_estimate.estimated_usd if budget_estimate else 0.0))
            )
            actual_credits = uc.usd_to_credits(provider_usd)
            settlement = uc.settle_reservation(
                user_id,
                str(credit_reservation.get("reservation_id") or ""),
                actual_credits=actual_credits,
                reason=f"studio_tool_actual:{name}",
                metadata={
                    "tool": name,
                    "session_id": session_id,
                    "provider": billing.get("provider"),
                    "model": billing.get("model"),
                    "prompt_tokens": int(billing.get("prompt_tokens") or 0),
                    "completion_tokens": int(billing.get("completion_tokens") or 0),
                    "provider_usd": provider_usd,
                    "metering_mode": "provider_usage" if usage_reported else "reserved_estimate_fallback",
                },
                # The inference has already completed. The preflight hold is a
                # conservative max-token estimate, but verified overage must
                # still be recorded atomically if a provider reports an outlier.
                allow_negative=True,
            )
            billing["provider_usd"] = round(provider_usd, 6)
            billing["metering_mode"] = (
                "provider_usage" if usage_reported else "reserved_estimate_fallback"
            )
            payload["billing"] = billing
            payload["credits"] = {
                "charged": int(settlement.get("credits_charged", 0) or 0),
                "balance_after": int(settlement.get("balance", 0) or 0),
                "refunded_credits": int(settlement.get("refunded_credits", 0) or 0),
                "metering_mode": billing["metering_mode"],
            }
            result = json.dumps(payload, indent=2, ensure_ascii=False)
            billed_with_actuals = True
        if credit_reservation and not runpod_dispatched and name in {
            "animate_production_scenes",
            "repair_production_scene_animation",
            "audit_and_repair_production_scenes",
            "finalize_production",
            "edit_production_scene_still",
            "edit_production_scenes_still",
            "regenerate_production_scene_still",
            "regenerate_production_scene",
            "re_edit_production",
        }:
            try:
                payload = json.loads(result or "{}")
                job_id = str(payload.get("job_id") or (arguments or {}).get("job_id") or "").strip()
                if job_id:
                    credits = _reconcile_shortform_costs(
                        user_id,
                        job_id,
                        reservation_payload={
                            "reservation": credit_reservation,
                            "user_id": user_id,
                            "tool": name,
                            "session_id": session_id or "",
                            "budget": budget_estimate.as_dict() if budget_estimate else {},
                        },
                        reason=f"studio_tool_actual:{name}",
                        tool=name,
                        session_id=session_id,
                    )
                    if isinstance(payload, dict):
                        payload["credits"] = credits
                        result = json.dumps(payload, indent=2, ensure_ascii=False)
                    billed_with_actuals = True
            except Exception:
                billed_with_actuals = False
        if credit_reservation and not billed_with_actuals and name in {
            "start_shortform_generate",
            "expand_visual_proof_shortform",
        }:
            # These tools are asynchronous. Their workers reconcile the hold
            # against real provider spend. Replays and rejected/conflicting
            # dispatches did not start work, so release their hold immediately.
            replay = False
            no_start = False
            no_start_reason = ""
            async_payload: dict[str, Any] = {}
            if name == "expand_visual_proof_shortform":
                try:
                    parsed_payload = json.loads(result or "{}")
                    async_payload = parsed_payload if isinstance(parsed_payload, dict) else {}
                    replay = bool(async_payload.get("idempotent_replay"))
                    async_status = str(async_payload.get("status") or "").strip().lower()
                    no_start = bool(
                        replay
                        or async_payload.get("ok") is False
                        or async_status in {"conflict", "failed", "error", "cancelled", "rejected"}
                    )
                    no_start_reason = "idempotent_replay" if replay else (async_status or "rejected")
                except Exception:
                    replay = False
                    no_start = False
            if no_start and not credit_reservation.get("unlimited"):
                import unified_credits as uc

                uc.release_reservation(
                    user_id,
                    str(credit_reservation.get("reservation_id") or ""),
                    reason=f"studio_tool_not_started:expand_visual_proof_shortform:{no_start_reason}",
                )
                if async_payload:
                    async_payload["credits"] = {
                        "charged": 0,
                        "reservation_released": True,
                        "reason": no_start_reason,
                    }
                    result = json.dumps(async_payload, indent=2, ensure_ascii=False)
            billed_with_actuals = True
        if credit_reservation and not credit_reservation.get("unlimited") and not billed_with_actuals:
            import unified_credits as uc

            base_credits = uc.usd_to_credits(budget_estimate.estimated_usd if budget_estimate else 0.0)
            state = uc.commit_reservation(
                user_id,
                str(credit_reservation.get("reservation_id") or ""),
                actual_credits=base_credits,
                reason=f"studio_tool_started:{name}",
                metadata={"tool": name, "session_id": session_id},
            )
            try:
                payload = json.loads(result or "{}")
                if isinstance(payload, dict):
                    payload["credits"] = {
                        "charged": base_credits,
                        "repair_reserve_refunded": max(
                            0,
                            int(credit_reservation.get("credits", 0) or 0) - base_credits,
                        ),
                        "balance_after": int(state.get("balance", 0) or 0),
                    }
                    result = json.dumps(payload, indent=2, ensure_ascii=False)
            except Exception:
                pass
    except Exception as exc:
        if local_mutation_claim is not None:
            try:
                from studio_agent import idempotent_mutations

                idempotent_mutations.fail(local_mutation_claim, exc)
            except Exception:
                pass
        if credit_reservation and not credit_reservation.get("unlimited"):
            try:
                import unified_credits as uc

                job_id = str((arguments or {}).get("job_id") or "").strip()
                if runpod_route:
                    # Configuration/payment/preflight failures happen before
                    # production starts and release their hold.  If POST /run
                    # may already have been accepted, the bridge persists a
                    # fail-closed receipt and the hold remains for later status
                    # and worker-cost reconciliation.
                    if _runpod_failure_definitely_not_submitted(
                        name,
                        arguments,
                        command_id=command_id,
                        user_id=user_id,
                    ):
                        uc.release_reservation(
                            user_id,
                            str(credit_reservation.get("reservation_id") or ""),
                            reason=f"studio_tool_runpod_failed:{name}",
                        )
                elif job_id and name in {
                    "animate_production_scenes",
                    "repair_production_scene_animation",
                    "audit_and_repair_production_scenes",
                    "finalize_production",
                    "edit_production_scene_still",
                    "edit_production_scenes_still",
                    "regenerate_production_scene_still",
                    "regenerate_production_scene",
                    "re_edit_production",
                }:
                    pending = production_costs.pending_billable_usd(_shortform_workspace(job_id))
                    if pending > 0:
                        _reconcile_shortform_costs(
                            user_id,
                            job_id,
                            reservation_payload={
                                "reservation": credit_reservation,
                                "user_id": user_id,
                                "tool": name,
                                "session_id": session_id or "",
                            },
                            reason=f"studio_tool_failed_actual:{name}",
                            tool=name,
                            session_id=session_id,
                        )
                    else:
                        uc.release_reservation(
                            user_id,
                            str(credit_reservation.get("reservation_id") or ""),
                            reason=f"studio_tool_failed:{name}",
                        )
                else:
                    uc.release_reservation(
                        user_id,
                        str(credit_reservation.get("reservation_id") or ""),
                        reason=f"studio_tool_failed:{name}",
                    )
            except Exception:
                pass
        if runpod_route and runpod_lease_dispatch_id:
            try:
                if _runpod_failure_definitely_not_submitted(
                    name,
                    arguments,
                    command_id=command_id,
                    user_id=user_id,
                ):
                    from studio_agent import runpod_bridge

                    runpod_bridge.release_production_lease(runpod_lease_dispatch_id)
            except Exception:
                # A ledger read/release failure is fail-closed. Never guess
                # that the production lease is safe to remove.
                pass
        telemetry.record_tool_call(
            user_id, name, arguments, session_id=session_id, result_preview=f"error: {exc}"
        )
        try:
            from studio_agent import training_capture

            training_capture.capture_event(
                str(user_id or ""),
                "tool_call",
                {
                    "tool": name,
                    "arguments": arguments,
                    "error": str(exc),
                    "content_format": content_format,
                },
                session_id=str(session_id or ""),
            )
        except Exception:
            pass
        raise
    if local_mutation_claim is not None:
        from studio_agent import idempotent_mutations

        try:
            parsed_result = json.loads(result or "{}")
        except Exception:
            parsed_result = {"raw_result": str(result or "")}
        idempotent_mutations.complete(
            local_mutation_claim,
            parsed_result if isinstance(parsed_result, dict) else {"result": parsed_result},
        )
    telemetry.record_tool_call(user_id, name, arguments, session_id=session_id, result_preview=result[:800])
    try:
        from studio_agent import training_capture

        training_capture.capture_event(
            str(user_id or ""),
            "tool_call",
            {
                "tool": name,
                "arguments": arguments,
                "result": result,
                "content_format": content_format,
            },
            session_id=str(session_id or ""),
        )
    except Exception:
        pass
    return result


def requires_approval(name: str) -> bool:
    return name in APPROVAL_REQUIRED

