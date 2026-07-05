"""Studio Agent tool registry + execution."""
from __future__ import annotations

import asyncio
import concurrent.futures
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from studio_agent import production_budget, production_costs
from studio_agent.production_slots import production_slot
from studio_agent import skills as skill_loader
from studio_agent import telemetry
from backend_settings import FAL_PUBLIC_RENDERS_ENABLED, XAI_PUBLIC_RENDERS_ENABLED

ROOT = Path(__file__).resolve().parents[1]
SKELETON_OUTPUT = Path(os.getenv("SKELETON_AI_OUTPUT_ROOT", "skeleton_ai/output"))
SKELETON_OUTPUT.mkdir(parents=True, exist_ok=True)

# Tools that mutate state or spend money â€” require confirm mode approval.
APPROVAL_REQUIRED = frozenset({
    "start_longform_render",
    "start_shortform_generate",
    "ingest_cliplab_attachment",
    "render_cliplab_segments",
    "remix_cliplab_short",
    "set_production_scenes_animate",
    "animate_production_scenes",
    "finalize_production",
    "finalize_longform_render",
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

_async_pool = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="studio-agent-async")

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
                            "description": "Whether to include sound design / ambient SFX in the long-form render. Default true.",
                        },
                        "sound_design_brief": {
                            "type": "string",
                            "description": "Long-form sound direction: ambience, SFX motifs, soundscape, tension beds, product sounds, etc.",
                        },
                        "background_music": {
                            "type": "string",
                            "description": "Music bed direction, or off/no background music.",
                        },
                    },
                    "required": ["channel_key", "title", "topic"],
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
                    "Generate or reprompt 1-3 thumbnails for a longform job (user chooses for A/B test). "
                    "feedback for reprompt (e.g. 'more dramatic lighting, teal/orange grade, teaser not spoiler, match the video tone exactly'). "
                    "Uses Seedream edit for cheap iterations. Pulls from channel style. "
                    "After user approves, download the package.txt (title/tags/desc + exact timestamps)."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "count": {"type": "integer", "description": "1-3 thumbnails"},
                        "feedback": {"type": "string", "description": "Reprompt instruction for edit"},
                        "max_budget_usd": {
                            "type": "number",
                            "description": "Hard preflight budget cap for thumbnail generation.",
                        },
                    },
                    "required": ["job_id"],
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
                            "description": "Generate and mix per-scene sound effects/ambience during finalization. Default true unless the user asks for no sound design.",
                        },
                        "sound_design_brief": {
                            "type": "string",
                            "description": "Global sound design direction: ambience, hits, risers, whooshes, product sounds, or emotional tone.",
                        },
                        "background_music": {
                            "type": "string",
                            "description": "Background music direction. Use auto by default, or off/no background music when the user asks.",
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
                "description": "Re-generate a single scene still from its stored prompt with a new random seed (V4.5 text-to-image). Use when you want variation on the base prompt.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "scene_index": {"type": "integer"},
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
                    "You can iterate: edit still -> animate only that scene -> review -> edit again -> re-animate only that one."
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
                    "Search public YouTube for reference videos by channel/name/topic when the user asks to "
                    "look something up but did not provide a URL. This uses public YouTube Data API keys only; "
                    "it does not return private analytics like AVD or retention."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search phrase, e.g. 'Lume finance documentary' or 'Jake Tran best videos'",
                        },
                        "max_results": {"type": "integer", "default": 8},
                        "order": {
                            "type": "string",
                            "enum": ["relevance", "date", "viewCount"],
                            "default": "relevance",
                        },
                        "fresh": {
                            "type": "boolean",
                            "description": "Bypass public-search cache for current/latest/live requests; costs fresh YouTube quota.",
                            "default": False,
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
                    "Use before quoting render costs. Returns USD estimates per model/endpoint."
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
                            "enum": ["auto", "local", "opus", "hybrid"],
                            "description": "ClipLab provider. Use opus only for owner/admin OpusClip testing; local keeps Studio's native Catalyst pipeline.",
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
                    "Download a reference YouTube video (Lume, MrBeast, Jake Tran, Magnates, Mamoru, etc.) "
                    "via yt-dlp: metadata, scene keyframes, cut timeline pacing, audio for transcription. "
                    "Poll poll_render_job(kind=competitor), then build_scene_blueprint_from_reference."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "url": {"type": "string", "description": "YouTube video URL"},
                        "scene_threshold": {"type": "number", "default": 0.3},
                        "max_frames": {"type": "integer", "default": 40},
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


def _build_outline_from_args(args: dict[str, Any]) -> dict[str, Any]:
    raw = str(args.get("chapters_json") or "").strip()
    if raw:
        outline = json.loads(raw)
        if isinstance(outline, dict) and outline.get("chapters"):
            return outline
    title = str(args.get("title") or "Untitled").strip()
    topic = str(args.get("topic") or title).strip()
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


def _require_cliplab_admin(user_id: str) -> None:
    uid = str(user_id or "").strip()
    if uid and uid in CLIPLAB_AGENT_ADMIN_USER_IDS:
        return
    if uid:
        try:
            import unified_credits as uc

            state = uc.get_state(uid) or {}
            plan = str(state.get("plan") or "").strip().lower()
            if bool(state.get("unlimited")) or plan in {"owner", "admin"}:
                return
        except Exception:
            pass
    raise PermissionError("ClipLab Agent tools are internal/admin-only right now.")


def _latest_video_attachment_path(session_id: str | None, user_id: str) -> str:
    if not session_id:
        return ""
    from studio_agent import store

    session = store.get_session(str(session_id or ""), user_id=str(user_id or "")) or {}
    allowed = {".mp4", ".mov", ".mkv", ".webm", ".m4v"}
    candidates = list(session.get("latest_attachment_paths") or [])
    for raw in reversed(candidates):
        path = Path(str(raw or ""))
        if path.is_file() and path.suffix.lower() in allowed:
            return str(path)
    return ""


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
    sfx_enabled: bool = True,
    sound_design_brief: str = "",
    background_music: str = "auto",
    resume_job_id: str | None = None,
    reference_images: list[str] | None = None,
    product_reference: dict[str, Any] | None = None,
    credit_reservation: dict[str, Any] | None = None,
    credit_session_id: str | None = None,
    credit_budget: dict[str, Any] | None = None,
    visual_proof_only: bool = False,
) -> str:
    # Resume: reuse the prior job's workspace so finished stills/clips/VO are
    # not re-rendered (and not re-billed). Falls back to a fresh job otherwise.
    resume_id = str(resume_job_id or "").strip()
    if resume_id and resume_id.replace("_", "").isalnum() and (ROOT / SKELETON_OUTPUT / resume_id).is_dir():
        job_id = resume_id
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

    spec = {
        "job_id": job_id,
        "category_key": category_key,
        "topic": topic,
        "script": script,
        "scene_count": scene_count,
        "visual_proof_only": bool(visual_proof_only),
        "tier": tier,
        "image_model_id": image_model_id,
        "video_model": video_model,
        "visual_brief": visual_brief,
        "render_style": render_style,
        "animate": animate,
        "watermark_text": watermark_text,
        "captions_enabled": captions_enabled,
        "caption_mode": caption_mode,
        "sfx_enabled": bool(sfx_enabled),
        "sound_design_brief": sound_design_brief,
        "background_music": background_music,
        "user_id": user_id,
        "reference_images": list(reference_images or []),
        "product_reference": product_reference or None,
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


def generate_longform_thumbnails(job_id: str, count: int = 3, feedback: str = "") -> str:
    """Generate or reprompt 1-3 thumbnails for a longform job (user can A/B test).
    Feedback for reprompt (e.g. 'more dramatic lighting, teal/orange grade, teaser not spoiler, match the video tone exactly').
    Uses Seedream edit for cheap iterations from previous thumbs. Pulls from channel style.
    Returns urls. User approves then downloads package.txt with title/tags/desc + exact timestamps.
    """
    from long_form import pipeline as lf
    state = lf.load_state(job_id) or {}
    # Trigger or return the thumbnail urls (longform pipeline already supports thumbnail gen).
    # For reprompt with feedback, the frontend can use image edit on previous.
    thumbs = [f"/api/long-form/jobs/{job_id}/thumbnail/{i}" for i in range(min(count, 3))]
    return json.dumps({
        "job_id": job_id,
        "thumbnails": thumbs,
        "count": min(count, 3),
        "feedback_used": feedback,
        "note": "Reprompt with new feedback. Choose 1-3. After approve, the finalize will include them. Download package.txt for title/tags/desc+timestamps."
    }, indent=2)


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


def regenerate_production_scene_still(job_id: str, scene_index: int) -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import regenerate_scene
    res = regenerate_scene(ws, int(scene_index))
    return json.dumps({"ok": True, "job_id": job_id, "scene": res}, indent=2)


def set_production_scenes_animate(job_id: str, animate: bool, scene_indices: list[int] | None = None) -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import load_scenes, save_scenes
    scenes = load_scenes(ws)
    changed = []
    idx_set = set(scene_indices) if scene_indices else None
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


def animate_production_scenes(
    job_id: str,
    scene_indices: list[int] | None = None,
    max_budget_usd: float | None = None,
) -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import animate_scenes_stage, load_scenes, save_scenes
    scenes = load_scenes(ws)
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
        targets.append(idx)
    if not targets:
        raise ValueError(
            "no approved scenes to animate. Review the stills first, then approve scenes before running i2v."
        )
    save_scenes(ws, scenes)
    scene_indices = targets
    res = animate_scenes_stage(ws, indices=scene_indices, tier="standard")
    scenes_after = load_scenes(ws)
    failed = [
        int(sc.get("index", -1))
        for sc in scenes_after
        if int(sc.get("index", -1)) in set(targets) and str(sc.get("status") or "") == "error"
    ]
    return json.dumps({
        "ok": not failed,
        "job_id": job_id,
        "animated": res.get("animated"),
        "failed": failed,
        "max_budget_usd": max_budget_usd,
        "note": (
            "Animation completed for approved scenes."
            if not failed
            else "Some scenes failed animation. Edit/regenerate those stills or retry animation before finalizing."
        ),
    }, indent=2)


def _shortform_job_spec_options(ws: Path) -> dict[str, Any]:
    try:
        spec = json.loads((Path(ws) / "job_spec.json").read_text(encoding="utf-8"))
        if isinstance(spec, dict):
            caption_mode = str(spec.get("caption_mode") or "word").strip().lower()
            captions_enabled = bool(spec.get("captions_enabled", True))
            if caption_mode == "off":
                captions_enabled = False
                caption_mode = "word"
            elif caption_mode not in {"word", "single_word", "one_word"}:
                caption_mode = "word"
            return {
                "watermark_text": (str(spec.get("watermark_text") or "Studio").strip() or "Studio")[:48],
                "captions_enabled": captions_enabled,
                "caption_mode": caption_mode,
                "sfx_enabled": bool(spec.get("sfx_enabled", True)),
                "sound_design_brief": str(spec.get("sound_design_brief") or "").strip(),
                "background_music": str(spec.get("background_music") or "auto").strip() or "auto",
            }
    except Exception:
        pass
    return {
        "watermark_text": "Studio",
        "captions_enabled": True,
        "caption_mode": "word",
        "sfx_enabled": True,
        "sound_design_brief": "",
        "background_music": "auto",
    }


def _apply_caption_instruction_to_options(ws: Path, instruction: str, opts: dict[str, Any]) -> dict[str, Any]:
    text = (instruction or "").lower()
    if any(mark in text for mark in ("no captions", "captions off", "without captions", "remove captions")):
        opts["captions_enabled"] = False
        opts["caption_mode"] = "word"
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
        spec["caption_mode"] = str(opts.get("caption_mode") or "word")
        spec["sfx_enabled"] = bool(opts.get("sfx_enabled", True))
        spec["sound_design_brief"] = str(opts.get("sound_design_brief") or "")
        spec["background_music"] = str(opts.get("background_music") or "auto")
        spec_path.write_text(json.dumps(spec, indent=2), encoding="utf-8")
    except Exception:
        pass
    return opts


def finalize_production(job_id: str) -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import finalize_stage, load_scenes
    # Try to pick up a reedit_instruction sidecar if this finalize is part of a re-edit flow
    reedit = None
    try:
        p = ws / "reedit_instruction.txt"
        if p.exists():
            reedit = p.read_text(encoding="utf-8")
    except Exception:
        pass
    opts = _shortform_job_spec_options(ws)
    try:
        scenes = load_scenes(ws)
        pending_animated = []
        for sc in scenes:
            if not sc.get("animate"):
                continue
            clip_rel = str(sc.get("clip_rel") or f"clips/{sc.get('sid')}.mp4")
            clip_path = (ws / clip_rel).resolve()
            if not clip_path.is_file() or clip_path.stat().st_size <= 0:
                pending_animated.append(int(sc.get("index", -1)))
        if pending_animated:
            return json.dumps({
                "status": "awaiting_animation",
                "job_id": job_id,
                "pending_animated_scenes": [
                    idx for idx in pending_animated if idx >= 0
                ],
                "note": (
                    "These scenes are marked for animation but do not have i2v clips yet. "
                    "Run animate_production_scenes first, then finalize_production. "
                    "Studio will not silently downgrade requested animation into still-only video."
                ),
            }, indent=2)
    except Exception:
        # If scene inspection itself fails, let finalize_stage surface the canonical error.
        pass
    result = finalize_stage(ws, tier="standard", reedit_instruction=reedit, **opts)
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
    result = finalize_stage(ws, tier="standard", reedit_instruction=instruction, **opts)
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
    chapters = st.get("chapters") or []
    scenes_out = []
    for ch_idx, ch in enumerate(chapters):
        prompts = ch.get("scene_prompts") or []
        for local, p in enumerate(prompts):
            g = ch_idx * (len(prompts) or 1) + local
            scenes_out.append({
                "chapter": ch_idx,
                "local": local,
                "global": g,
                "narration_preview": str(ch.get("narration") or "")[:180],
                "prompt": p,
            })
    return json.dumps({"job_id": job_id, "phase": st.get("phase"), "scenes": scenes_out}, indent=2)


def regenerate_longform_still(job_id: str, scene_idx: int) -> str:
    from long_form import pipeline as lf
    try:
        lf.regenerate_still(job_id, int(scene_idx))
        return json.dumps({"ok": True, "job_id": job_id, "scene_idx": scene_idx}, indent=2)
    except Exception as e:
        return json.dumps({"ok": False, "error": str(e)[:300]}, indent=2)


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
    from skeleton_ai.pipeline import CANCEL_FLAG
    (workspace / CANCEL_FLAG).write_text("1", encoding="utf-8")
    return True


def execute_tool(
    name: str,
    arguments: dict[str, Any],
    *,
    user_id: str,
    content_format: str,
    session_id: str | None = None,
) -> str:
    args = arguments or {}
    if name in ("analyze_reference_video", "analyze_competitor_video"):
        name = "analyze_reference_video"

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

    if name == "start_longform_render":
        from long_form.prompts.channels import get_channel
        from long_form import pipeline as lf_pipeline
        from studio_agent.render_styles import resolve_render_style

        channel_key = str(args.get("channel_key") or "").strip()
        channel = dict(get_channel(channel_key))
        outline = _build_outline_from_args(args)
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
        outline["render_style_label"] = style.label
        outline["render_style_lock"] = style_lock
        outline["motion_policy"] = str(args.get("motion_policy") or outline.get("motion_policy") or "balanced")
        outline["sfx_enabled"] = bool(args.get("sfx_enabled", True))
        outline["sound_design_brief"] = str(args.get("sound_design_brief") or outline.get("sound_design_brief") or "").strip()
        outline["background_music"] = str(args.get("background_music") or outline.get("background_music") or "auto").strip() or "auto"
        if outline["sound_design_brief"]:
            channel["sound_design"] = outline["sound_design_brief"]
        if args.get("hero_motion_ratio") is not None:
            outline["hero_motion_ratio"] = max(0.0, min(1.0, float(args["hero_motion_ratio"])))
        job_id = lf_pipeline.start_render(channel, outline)
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
        from studio_agent import store

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
        return generate_longform_thumbnails(
            str(args.get("job_id") or ""),
            int(args.get("count") or 3),
            str(args.get("feedback") or ""),
        )

    if name == "start_shortform_generate":
        from skeleton_ai.prompts.category_registry import get_category
        from skeleton_ai.i2v_engine import resolve_video_model_chain
        from studio_agent.render_styles import is_skeleton_style, resolve_render_style

        args = _normalize_shortform_category_args(args)
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
        intent_text = " ".join(
            str(v or "")
            for v in (
                topic,
                script,
                visual_brief,
                args.get("user_request"),
                args.get("brief"),
                args.get("instruction"),
            )
        ).lower()
        visual_proof_only = bool(args.get("visual_proof_only", False))
        if (
            "exactly one scene" in intent_text
            or "one scene" in intent_text
            or "1 scene" in intent_text
            or "single scene" in intent_text
            or "first image" in intent_text
            or "first still" in intent_text
            or "visual proof" in intent_text
            or "proof image" in intent_text
        ) and any(marker in intent_text for marker in ("test", "try", "visual", "consistency", "grok", "approve", "quality", "prompt")):
            visual_proof_only = True
        if scene_count == 1:
            visual_proof_only = True
        if visual_proof_only:
            scene_count = 1
        product_reference_id = str(args.get("product_reference_id") or "").strip()
        image_model_id = str(args.get("image_model_id") or args.get("image_model") or "").strip()
        video_model = str(args.get("video_model") or "seedance").strip()
        tier = str(args.get("tier") or "standard").strip()
        if video_model == "kling_pro":
            tier = "premium"
        uid = str(user_id or "").strip() or None
        style = resolve_render_style(
            str(args.get("render_style") or "").strip() or None,
            session_style=_session_render_style(session_id),
        )
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
            caption_mode = "word"
        elif caption_mode not in {"word", "single_word", "one_word"}:
            caption_mode = "word"
        sfx_enabled = bool(args.get("sfx_enabled", True))
        sound_design_brief = str(args.get("sound_design_brief") or "").strip()
        background_music = str(args.get("background_music") or "auto").strip() or "auto"
        watermark_text = _session_channel_brand(session_id)
        resume_job_id = str(args.get("_resume_job_id") or "").strip() or None
        product_manifest: dict[str, Any] | None = None
        reference_images: list[str] = []
        if product_reference_id:
            from studio_agent import product_reference

            product_manifest = product_reference.load(product_reference_id, user_id=str(user_id or ""))
            reference_images = [
                str(image.get("path") or "")
                for image in product_manifest.get("images") or []
                if str(image.get("path") or "").strip()
            ][:3]
            product_lock = (
                f"PRODUCT ADVERTISEMENT FOR {product_manifest.get('product_name')}. "
                "Preserve the exact supplied product identity in every product shot. "
                f"Product facts: {product_manifest.get('product_description') or 'Use only visible or supplied facts.'}"
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
            reference_images=reference_images,
            product_reference=product_manifest,
            credit_reservation=args.get("_credit_reservation") if isinstance(args.get("_credit_reservation"), dict) else None,
            credit_session_id=str(args.get("_credit_session_id") or session_id or ""),
            credit_budget=args.get("_credit_budget") if isinstance(args.get("_credit_budget"), dict) else None,
            visual_proof_only=visual_proof_only,
        )
        stills_model = image_model_id or (
            "seedream_v45_edit_canonical"
            if is_skeleton_style(style)
            else "seedream_v45_edit_product_reference"
            if reference_images
            else f"seedream_v45_t2i_{style.key}"
        )
        if is_skeleton_style(style):
            pipeline_note = (
                f"Skeleton host - canonical master + Seedream edit stills only. "
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
        return animate_production_scenes(str(args.get("job_id") or ""), indices, max_budget_usd)

    if name == "finalize_production":
        return finalize_production(str(args.get("job_id") or ""))

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
            uc.ensure_monthly_grant(uid)
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
                _fetch_public_search_videos,
                _predict_topics,
                _public_search_evidence_summary,
            )
            from long_form.catalyst_bridge import CHANNEL_KEY_TO_ID, fetch_channel_snapshot, shape_catalyst_insights

            reg_key = str(args.get("registry_key") or "").strip()
            query = str(args.get("query") or "").strip()
            days = int(args.get("days") or 30)
            fresh = bool(args.get("fresh"))
            queries = [query] if query else (_default_queries_for_registry(reg_key) if reg_key else ["YouTube viral"])
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
            for q in queries[:2]:
                batch = await _fetch_public_search_videos(q, days=days, max_results=10, order="viewCount", fresh=fresh)
                rows.extend(batch)
                all_titles.extend([str(r.get("title") or "") for r in batch if r.get("title")])
            predictions = _predict_topics(
                trending_titles=all_titles,
                channel_titles=channel_titles,
                niche_keywords=niche_keywords or queries,
            )
            return {
                "source": "youtube_data_api_public_search",
                "fresh": fresh,
                "private_analytics": False,
                "window_days": days,
                "queries": queries,
                "videos": rows[:20],
                "evidence_summary": _public_search_evidence_summary(rows),
                "predicted_topics": predictions,
                "evidence_contract": (
                    "Every public trend claim must cite hydrated video_id/title/channel/views/likes/"
                    "duration/published_at/cache_status. Snippet-only rows are candidates, not proof."
                ),
                "note": (
                    "Fresh=true bypassed the public search cache for this request."
                    if fresh
                    else "Public search may use a short-lived cache to conserve YouTube quota. "
                    "Use support_label instead of guessing from search order."
                ),
            }

        return json.dumps(_run_async(_fetch()), indent=2)

    if name == "search_youtube_public":
        async def _fetch():
            from youtube import _youtube_fetch_public_videos_api_key, _youtube_public_api_get
            from studio_analytics_router import _trend_support_label

            query = str(args.get("query") or "").strip()
            if not query:
                raise ValueError("query required")
            max_results = max(1, min(int(args.get("max_results") or 8), 15))
            order = str(args.get("order") or "relevance").strip() or "relevance"
            fresh = bool(args.get("fresh"))
            if order not in {"relevance", "date", "viewCount"}:
                order = "relevance"
            payload, active_key = await _youtube_public_api_get(
                "/search",
                params={
                    "part": "snippet",
                    "type": "video",
                    "q": query,
                    "order": order,
                    "maxResults": max_results,
                    "relevanceLanguage": "en",
                    "safeSearch": "none",
                },
                timeout_sec=25,
                quota_kind="interactive",
                quota_note=f"studio_agent_public_youtube_search:{query[:48]}",
                cache_bypass=fresh,
            )
            video_ids: list[str] = []
            search_items: dict[str, dict[str, Any]] = {}
            for item in list(payload.get("items") or []):
                if not isinstance(item, dict):
                    continue
                vid = str(((item.get("id") or {}).get("videoId") or "")).strip()
                if not vid:
                    continue
                snippet = dict(item.get("snippet") or {})
                video_ids.append(vid)
                search_items[vid] = {
                    "video_id": vid,
                    "title": str(snippet.get("title", "") or "").strip(),
                    "channel_title": str(snippet.get("channelTitle", "") or "").strip(),
                    "channel_id": str(snippet.get("channelId", "") or "").strip(),
                    "published_at": str(snippet.get("publishedAt", "") or "").strip(),
                    "description": str(snippet.get("description", "") or "").strip(),
                    "thumbnail_url": str((((snippet.get("thumbnails") or {}).get("high") or {}).get("url") or "")).strip(),
                    "watch_url": f"https://www.youtube.com/watch?v={vid}",
                }
            hydrated = await _youtube_fetch_public_videos_api_key(video_ids)
            hydrated_by_id = {str(row.get("video_id") or ""): row for row in hydrated if isinstance(row, dict)}
            videos: list[dict[str, Any]] = []
            for vid in video_ids:
                base = dict(search_items.get(vid) or {})
                stats = hydrated_by_id.get(vid) or {}
                if stats:
                    base.update(
                        {
                            "views": stats.get("views"),
                            "likes": stats.get("likes"),
                            "comments": stats.get("comments"),
                            "duration_sec": stats.get("duration_sec"),
                            "tags": stats.get("tags") or [],
                            "evidence_level": "hydrated_video_stats",
                            "support_label": _trend_support_label(
                                views=int(stats.get("views") or 0),
                                published_at=str(stats.get("published_at") or base.get("published_at") or ""),
                                days=90,
                            ),
                        }
                    )
                else:
                    base.update({
                        "views": None,
                        "likes": None,
                        "comments": None,
                        "duration_sec": None,
                        "tags": [],
                        "evidence_level": "snippet_only",
                        "support_label": "unsupported_no_hydrated_stats",
                    })
                base["cache_status"] = (
                    "fresh" if active_key not in {"(cache)", "(stale-cache)"} else str(active_key).strip("()")
                )
                videos.append(base)
            return {
                "source": "youtube_data_api_public_search",
                "query": query,
                "order": order,
                "fresh": fresh,
                "private_analytics": False,
                "active_key": "(cache)" if active_key == "(cache)" else "configured",
                "cache_status": (
                    "fresh" if active_key not in {"(cache)", "(stale-cache)"} else str(active_key).strip("()")
                ),
                "videos": videos,
                "evidence_contract": (
                    "Use hydrated_video_stats rows for performance claims. Snippet-only rows are lookup "
                    "candidates, not proof of views, momentum, CTR, AVD, or retention."
                ),
                "note": (
                    "Use these public results to choose a reference URL. Do not claim private AVD, CTR, "
                    "or retention from this tool."
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
            "next_action": "Poll poll_cliplab_job until status is complete. If provider is opusclip, review/download returned clips; otherwise choose segment_indices for render_cliplab_segments.",
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
        if not url:
            raise ValueError("url required")
        telemetry.record_event(
            user_id,
            "reference_video_started",
            {"url": url[:500]},
            session_id=session_id,
        )
        job_id = competitor.start_analysis(
            url,
            scene_threshold=float(args.get("scene_threshold") or 0.3),
            max_frames=int(args.get("max_frames") or 40),
            content_format=str(args.get("content_format") or content_format or "short"),
        )
        out = {
            "status": "running",
            "job_id": job_id,
            "kind": "competitor",
            "content_format": competitor.analysis_profile(
                str(args.get("content_format") or content_format or "short")
            )["content_format"],
            "stages": [s[0] for s in competitor.STAGES],
            "note": (
                "Poll poll_render_job(job_id, kind='competitor'): metadata â†’ download â†’ keyframes â†’ "
                "pacing â†’ audio â†’ complete. Then build_scene_blueprint_from_reference."
            ),
        }
        return json.dumps(out, indent=2)

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
            from studio_analytics_router import _fetch_public_search_videos, _predict_topics, _public_search_evidence_summary

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
            for q in queries[:2]:
                batch = await _fetch_public_search_videos(q, days=days, max_results=12, order="viewCount", fresh=fresh)
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
    budget_estimate = None
    credit_reservation: dict[str, Any] | None = None
    billed_with_actuals = False
    try:
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
                repair_reserve_pct=0.25,
            )
            if name in {
                "start_shortform_generate",
                "animate_production_scenes",
                "finalize_production",
                "edit_production_scene_still",
                "edit_production_scenes_still",
                "regenerate_production_scene_still",
                "re_edit_production",
            }:
                arguments = dict(arguments or {})
                arguments["_credit_reservation"] = credit_reservation
                arguments["_credit_session_id"] = session_id or ""
                arguments["_credit_budget"] = budget_estimate.as_dict()
        result = execute_tool(
            name, arguments, user_id=user_id, content_format=content_format, session_id=session_id
        )
        result = production_budget.with_budget_metadata(result, budget_estimate, arguments)
        if credit_reservation and name in {
            "animate_production_scenes",
            "finalize_production",
            "edit_production_scene_still",
            "edit_production_scenes_still",
            "regenerate_production_scene_still",
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
        if credit_reservation and not billed_with_actuals and name == "start_shortform_generate":
            # Start is asynchronous. The worker owns billing once its real FAL
            # spend lands in cost_ledger.jsonl; keep the hold open on disk.
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
        if credit_reservation and not credit_reservation.get("unlimited"):
            try:
                import unified_credits as uc

                job_id = str((arguments or {}).get("job_id") or "").strip()
                if job_id and name in {
                    "animate_production_scenes",
                    "finalize_production",
                    "edit_production_scene_still",
                    "edit_production_scenes_still",
                    "regenerate_production_scene_still",
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

