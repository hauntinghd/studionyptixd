"""Studio Agent -> Catalyst learning writeback (good and bad outcomes)."""
from __future__ import annotations

import json
import re
import time
import uuid
from pathlib import Path
from typing import Any

from studio_agent.catalyst_skeleton_reference import catalyst_channel_memory_path


def _learning_records_path() -> Path:
    return catalyst_channel_memory_path().parent / "catalyst_learning_records.json"


def _now() -> float:
    return time.time()


def _dedupe_lines(values: list[str], *, max_items: int = 12) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = re.sub(r"\s+", " ", str(raw or "").strip())
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text[:220])
        if len(out) >= max_items:
            break
    return out


def _load_learning_records() -> dict[str, Any]:
    path = _learning_records_path()
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(data) if isinstance(data, dict) else {}


def _save_learning_records(data: dict[str, Any]) -> None:
    path = _learning_records_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def _load_channel_memory() -> dict[str, Any]:
    path = catalyst_channel_memory_path()
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(data) if isinstance(data, dict) else {}


def _save_channel_memory(data: dict[str, Any]) -> None:
    path = catalyst_channel_memory_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def _channel_key(session: dict[str, Any] | None) -> str:
    session = dict(session or {})
    reg = str(session.get("registry_key") or "").strip()
    if reg:
        return reg
    ch = str(session.get("channel_id") or "").strip()
    if ch:
        return ch
    title = str(session.get("channel_title") or "").strip()
    if title:
        return re.sub(r"[^a-z0-9_.-]+", "_", title.lower())[:80]
    return "studio_agent_global"


def _new_learning_entry(
    *,
    user_id: str,
    session: dict[str, Any] | None,
    turn_kind: str,
    outcome: str,
) -> dict[str, Any]:
    session = dict(session or {})
    return {
        "id": f"sa_learn_{uuid.uuid4().hex[:16]}",
        "mode": "studio_agent_turn_learning",
        "source": "studio_agent",
        "turn_kind": str(turn_kind or "").strip(),
        "outcome": str(outcome or "neutral").strip(),
        "user_id": str(user_id or "").strip(),
        "session_id": str(session.get("session_id") or "").strip(),
        "channel_id": str(session.get("channel_id") or "").strip(),
        "registry_key": str(session.get("registry_key") or "").strip(),
        "channel_title": str(session.get("channel_title") or "").strip(),
        "created_at": _now(),
        "wins_to_keep": [],
        "mistakes_to_avoid": [],
        "hook_adjustments": [],
        "packaging_adjustments": [],
        "pacing_adjustments": [],
        "visual_adjustments": [],
        "next_video_moves": [],
        "predicted_topics": [],
        "metadata": {},
    }


def _merge_entry_into_channel_memory(entry: dict[str, Any], channel_key: str) -> None:
    data = _load_channel_memory()
    bucket = dict(data.get(channel_key) or {})
    bucket["key"] = channel_key
    bucket["updated_at"] = _now()
    bucket["studio_agent_learning_at"] = _now()

    def _extend(field: str, values: list[str]) -> None:
        if not values:
            return
        bucket[field] = _dedupe_lines([*list(bucket.get(field) or []), *values])

    _extend("hook_learnings", list(entry.get("hook_adjustments") or []) + list(entry.get("wins_to_keep") or []))
    _extend("packaging_learnings", list(entry.get("packaging_adjustments") or []))
    _extend("pacing_learnings", list(entry.get("pacing_adjustments") or []))
    _extend("visual_learnings", list(entry.get("visual_adjustments") or []))
    _extend("retention_watchouts", list(entry.get("mistakes_to_avoid") or []))
    _extend("next_video_moves", list(entry.get("next_video_moves") or []))

    studio_entries = list(bucket.get("studio_agent_learning") or [])
    studio_entries.append(
        {
            "id": entry.get("id"),
            "turn_kind": entry.get("turn_kind"),
            "outcome": entry.get("outcome"),
            "created_at": entry.get("created_at"),
            "session_id": entry.get("session_id"),
        }
    )
    bucket["studio_agent_learning"] = studio_entries[-40:]
    data[channel_key] = bucket
    _save_channel_memory(data)


def _persist_entry(entry: dict[str, Any]) -> dict[str, Any]:
    records = _load_learning_records()
    key = str(entry.get("id") or "")
    if key:
        records[key] = entry
    _save_learning_records(records)
    channel_key = _channel_key(
        {
            "registry_key": entry.get("registry_key"),
            "channel_id": entry.get("channel_id"),
            "channel_title": entry.get("channel_title"),
        }
    )
    _merge_entry_into_channel_memory(entry, channel_key)
    return {"ok": True, "learning_id": key, "turn_kind": entry.get("turn_kind"), "outcome": entry.get("outcome")}


def record_reference_analysis(
    user_id: str,
    session: dict[str, Any] | None,
    payload: dict[str, Any] | None,
) -> dict[str, Any]:
    """Persist reference-analysis wins/watchouts into Catalyst memory."""
    if not isinstance(payload, dict):
        return {"ok": False, "reason": "missing_payload"}
    depth = str(payload.get("analysis_depth") or "").strip().lower()
    outcome = "win" if depth in {"full", "partial"} else "watchout" if depth == "pacing_only" else "neutral"
    entry = _new_learning_entry(
        user_id=user_id,
        session=session,
        turn_kind="reference_analysis",
        outcome=outcome,
    )
    storytelling = payload.get("storytelling") if isinstance(payload.get("storytelling"), dict) else {}
    hook = str(storytelling.get("hook") or "").strip()
    summary = str(storytelling.get("summary") or "").strip()
    if hook:
        entry["hook_adjustments"].append(f"Reference hook pattern: {hook[:180]}")
    if summary:
        entry["wins_to_keep"].append(f"Reference story read: {summary[:180]}")
    packaging = storytelling.get("packaging")
    if isinstance(packaging, dict):
        title_angle = str(packaging.get("title_angle") or packaging.get("title") or "").strip()
        if title_angle:
            entry["packaging_adjustments"].append(f"Reference packaging angle: {title_angle[:160]}")
    elif isinstance(packaging, str) and packaging.strip():
        entry["packaging_adjustments"].append(f"Reference packaging angle: {packaging[:160]}")
    visual = payload.get("visual_summary")
    visual_text = str((visual or {}).get("summary") if isinstance(visual, dict) else visual or "").strip()
    if visual_text:
        entry["visual_adjustments"].append(f"Reference visual grammar: {visual_text[:180]}")
    pacing = payload.get("pacing") if isinstance(payload.get("pacing"), dict) else {}
    avg_shot = float(pacing.get("avg_shot_sec") or 0)
    duration = float(pacing.get("duration_sec") or 0)
    if avg_shot > 7:
        entry["mistakes_to_avoid"].append(
            f"Slow-hold reference pacing ({avg_shot:.1f}s avg shot) — risky for Shorts unless visual promise is extreme."
        )
        entry["pacing_adjustments"].append("Prefer faster interrupt cadence than this reference for Shorts retention.")
    elif avg_shot > 0:
        entry["pacing_adjustments"].append(f"Reference cut rhythm ~{avg_shot:.1f}s avg shot — match or beat this energy.")
    if duration > 90:
        entry["mistakes_to_avoid"].append(
            f"Reference duration {duration:.0f}s is long-form hold — cut to sub-60s for Shorts unless user wants long-form."
        )
    for warning in list(payload.get("pacing_warnings") or [])[:3]:
        text = str(warning or "").strip()
        if text:
            entry["mistakes_to_avoid"].append(text[:200])
    gaps = payload.get("analysis_gaps") if isinstance(payload.get("analysis_gaps"), dict) else {}
    for stage, err in gaps.items():
        err_text = str(err or "").strip()
        if err_text:
            entry["mistakes_to_avoid"].append(f"Reference stage gap ({stage}): {err_text[:160]}")
    entry["metadata"] = {
        "analysis_depth": depth,
        "duration_sec": duration,
        "avg_shot_sec": avg_shot,
    }
    return _persist_entry(entry)


def record_public_research(
    user_id: str,
    session: dict[str, Any] | None,
    *,
    tool_fires: list[Any] | None,
    search_query: str = "",
    predicted_topics: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Persist public-demand research outcomes (hydrated wins or empty-search watchouts)."""
    import json as _json

    entry = _new_learning_entry(
        user_id=user_id,
        session=session,
        turn_kind="public_research",
        outcome="neutral",
    )
    entry["metadata"]["search_query"] = str(search_query or "").strip()
    hydrated: list[dict[str, Any]] = []
    for fire in tool_fires or []:
        name = str(getattr(fire, "name", "") or "")
        if name not in {"get_public_search_trends", "search_youtube_public", "recommend_video_topics"}:
            continue
        try:
            payload = _json.loads(getattr(fire, "result", "") or "{}")
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            continue
        videos = payload.get("videos") or payload.get("trending_sample") or []
        if isinstance(videos, list):
            hydrated.extend(
                [
                    dict(row)
                    for row in videos
                    if isinstance(row, dict)
                    and str(row.get("evidence_level") or "") == "hydrated_video_stats"
                ]
            )
    if hydrated:
        entry["outcome"] = "win"
        hydrated.sort(key=lambda row: int(row.get("views") or 0), reverse=True)
        for row in hydrated[:4]:
            title = str(row.get("title") or "").strip()
            if not title:
                continue
            views = int(row.get("views") or 0)
            channel = str(row.get("channel_title") or row.get("channel") or "").strip()
            entry["next_video_moves"].append(
                f"Public precedent: '{title[:120]}' ({views:,} views{f' — {channel}' if channel else ''})"
            )
    else:
        entry["outcome"] = "watchout"
        q = str(search_query or "this niche").strip()
        entry["mistakes_to_avoid"].append(
            f"Public search returned no hydrated stats for '{q[:120]}' — do not invent trend/view claims."
        )
    for row in list(predicted_topics or [])[:6]:
        if not isinstance(row, dict):
            continue
        topic = str(row.get("topic") or row.get("title") or "").strip()
        if topic:
            entry["predicted_topics"].append(topic[:140])
    return _persist_entry(entry)


def record_production_job(
    user_id: str,
    session: dict[str, Any] | None,
    snapshot: dict[str, Any] | None,
    *,
    outcome: str = "neutral",
) -> dict[str, Any]:
    """Persist production completion/failure lessons.

    Complete MP4 alone is NOT a win — visual_qa / ready_to_post must also pass.
    """
    if not isinstance(snapshot, dict):
        return {"ok": False, "reason": "missing_snapshot"}
    status = str(snapshot.get("status") or "").lower()
    resolved_outcome = outcome
    vq = snapshot.get("visual_qa") if isinstance(snapshot.get("visual_qa"), dict) else {}
    ready = snapshot.get("ready_to_post")
    visual_failed = (
        vq.get("status") == "fail"
        or vq.get("ready_to_publish") is False
        or ready is False
        or status == "visual_qa_failed"
    )
    if status in {"failed", "error", "cancelled", "visual_qa_failed"} or visual_failed:
        resolved_outcome = "failure"
    elif status in {"complete", "completed", "ready"} and not visual_failed:
        resolved_outcome = "win"
    entry = _new_learning_entry(
        user_id=user_id,
        session=session,
        turn_kind="production_job",
        outcome=resolved_outcome,
    )
    title = str(snapshot.get("title") or snapshot.get("topic") or "").strip()
    job_id = str(snapshot.get("job_id") or "").strip()
    if title and resolved_outcome == "win":
        entry["wins_to_keep"].append(f"Completed production: {title[:140]}")
        outfit = str((snapshot.get("locked_outfit") or vq.get("locked_outfit") or "")).strip()
        if outfit:
            entry["visual_adjustments"].append(f"Keep wardrobe lock: {outfit[:160]}")
    if resolved_outcome == "failure":
        detail = str(snapshot.get("error") or snapshot.get("message") or snapshot.get("note") or "").strip()
        if visual_failed and not detail:
            detail = str(vq.get("summary") or "visual QA failed")
        entry["mistakes_to_avoid"].append(
            f"Production failed ({job_id or 'job'}): {detail[:180] or 'unknown error'}"
        )
        for check in list(vq.get("checks") or [])[:6]:
            if not isinstance(check, dict) or check.get("status") != "fail":
                continue
            label = str(check.get("label") or check.get("id") or "check").strip()
            d = str(check.get("detail") or "").strip()
            entry["visual_adjustments"].append(f"Avoid: {label}" + (f" — {d[:100]}" if d else ""))
    entry["metadata"] = {
        "job_id": job_id,
        "title": title[:160],
        "render_style": str(snapshot.get("render_style") or "").strip(),
        "status": status,
        "kind": str(snapshot.get("kind") or ""),
        "ready_to_post": ready,
        "visual_qa_status": vq.get("status"),
        "visual_qa_score": vq.get("score"),
    }
    return _persist_entry(entry)


def match_published_short_to_studio_job(
    *,
    title: str,
    channel_key: str = "",
) -> dict[str, Any]:
    """Find a completed Studio short that can be safely credited for an upload.

    Channel analytics includes uploads Studio did not make.  Treating those as
    our own outcome is poisoned feedback, so a title match is required before
    Catalyst promotes a hook/style decision as a Studio win or failure.
    """
    target = re.sub(r"[^a-z0-9]+", " ", str(title or "").lower()).strip()
    if not target:
        return {"matched": False, "confidence": 0.0}
    target_tokens = {word for word in target.split() if len(word) > 2}
    best: dict[str, Any] = {"matched": False, "confidence": 0.0}
    for raw in _load_learning_records().values():
        entry = dict(raw or {}) if isinstance(raw, dict) else {}
        if str(entry.get("turn_kind") or "") != "production_job":
            continue
        if channel_key and str(entry.get("registry_key") or "") not in {"", channel_key}:
            continue
        meta = dict(entry.get("metadata") or {})
        candidate = re.sub(r"[^a-z0-9]+", " ", str(meta.get("title") or "").lower()).strip()
        tokens = {word for word in candidate.split() if len(word) > 2}
        if not tokens:
            continue
        confidence = len(target_tokens & tokens) / max(1, len(target_tokens | tokens))
        if target == candidate:
            confidence = 1.0
        if confidence > float(best.get("confidence") or 0.0):
            best = {
                "matched": confidence >= 0.72,
                "confidence": round(confidence, 3),
                "job_id": str(meta.get("job_id") or ""),
                "title": str(meta.get("title") or ""),
            }
    return best


def record_shortform_youtube_performance(
    user_id: str,
    session: dict[str, Any] | None,
    *,
    video_id: str = "",
    title: str = "",
    views: int = 0,
    likes: int = 0,
    comments: int = 0,
    avg_view_duration_sec: float = 0.0,
    duration_sec: float = 0.0,
    impressions: int = 0,
    ctr: float = 0.0,
    channel_key: str = "",
    job_id: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Learn from published short-form YouTube outcomes (not just MP4 completion).

    High retention/CTR → promote hooks/wardrobe/packaging.
    Low retention/CTR → mark watchouts so Catalyst stops treating bad posts as wins.
    """
    views = int(views or 0)
    likes = int(likes or 0)
    comments = int(comments or 0)
    avd = float(avg_view_duration_sec or 0.0)
    dur = float(duration_sec or 0.0)
    impr = int(impressions or 0)
    ctr_f = float(ctr or 0.0)
    if ctr_f <= 0 and impr > 0 and views > 0:
        ctr_f = views / max(1, impr)

    retention = (avd / dur) if dur > 1 else 0.0
    eng_rate = ((likes + comments) / views) if views > 0 else 0.0

    # Thresholds tuned for Shorts: weak if low views after real impressions,
    # or retention collapses under ~25%, or CTR under ~2% with enough impressions.
    weak = False
    strong = False
    if views >= 500 and retention >= 0.35 and (ctr_f >= 0.04 or impr == 0):
        strong = True
    if views >= 1000 and eng_rate >= 0.04:
        strong = True
    if impr >= 1000 and ctr_f > 0 and ctr_f < 0.02:
        weak = True
    if views >= 200 and dur > 0 and retention > 0 and retention < 0.22:
        weak = True
    if views >= 500 and eng_rate < 0.008:
        weak = True
    if not strong and not weak:
        if views >= 100:
            # Mild positive signal only
            outcome = "neutral"
        else:
            outcome = "neutral"
    else:
        outcome = "win" if strong and not weak else "watchout" if weak else "neutral"

    sess = dict(session or {})
    if channel_key:
        sess.setdefault("registry_key", channel_key)
    entry = _new_learning_entry(
        user_id=user_id,
        session=sess,
        turn_kind="shortform_youtube_performance",
        outcome=outcome,
    )
    label = (title or video_id or job_id or "short").strip()[:140]
    metrics_line = (
        f"YT short '{label}': views={views}, likes={likes}, comments={comments}, "
        f"AVD={avd:.1f}s/{dur:.1f}s ret={retention:.0%}, CTR={ctr_f:.1%}, impr={impr}"
    )
    meta = dict(metadata or {})
    attribution = dict(meta.get("studio_job_match") or {})
    attributed_to_studio = bool(job_id) or bool(attribution.get("matched"))
    if attribution.get("job_id") and not job_id:
        job_id = str(attribution["job_id"])
    # External channel videos are excellent reference data, but they are not
    # evidence that Studio's own strategy caused the outcome.
    if not attributed_to_studio:
        entry["outcome"] = "reference"
        entry["next_video_moves"].append(
            f"External channel reference, not credited to Studio: {metrics_line[:170]}"
        )
    elif outcome == "win":
        entry["wins_to_keep"].append(metrics_line[:220])
        entry["hook_adjustments"].append(
            f"Promote packaging/hook patterns from strong short: {label[:100]}"
        )
        if retention >= 0.4:
            entry["pacing_adjustments"].append(
                f"Retention {retention:.0%} on '{label[:80]}' — keep similar pacing/cuts."
            )
        if ctr_f >= 0.05:
            entry["packaging_adjustments"].append(
                f"CTR {ctr_f:.1%} on '{label[:80]}' — reuse thumbnail/title angle family."
            )
    elif attributed_to_studio and outcome == "watchout":
        entry["mistakes_to_avoid"].append(metrics_line[:220])
        if retention and retention < 0.25:
            entry["pacing_adjustments"].append(
                f"Low retention ({retention:.0%}) on '{label[:80]}' — tighten hook in first 2s, cut dead air."
            )
        if ctr_f and ctr_f < 0.025:
            entry["packaging_adjustments"].append(
                f"Weak CTR ({ctr_f:.1%}) on '{label[:80]}' — change title/thumb promise; do not repeat."
            )
        entry["visual_adjustments"].append(
            "Do not treat completed MP4 alone as success; require visual QA + post performance."
        )
    elif attributed_to_studio:
        entry["next_video_moves"].append(metrics_line[:220])

    entry["metadata"] = {
        "video_id": str(video_id or "").strip(),
        "job_id": str(job_id or "").strip(),
        "attributed_to_studio": attributed_to_studio,
        "attribution_confidence": float(attribution.get("confidence") or (1.0 if job_id else 0.0)),
        "views": views,
        "likes": likes,
        "comments": comments,
        "avg_view_duration_sec": avd,
        "duration_sec": dur,
        "retention": round(retention, 4),
        "impressions": impr,
        "ctr": round(ctr_f, 4),
        "engagement_rate": round(eng_rate, 4),
        **meta,
    }
    return _persist_entry(entry)


def record_artifact_complaint(
    user_id: str,
    session: dict[str, Any] | None,
    *,
    complaint: str,
    job_id: str = "",
    scenes_repaired: list[int] | None = None,
    scenes_failed: list[int] | None = None,
) -> dict[str, Any]:
    """Learn from a natural-language artifact complaint ("the video has artifacting").

    Every complaint is a watchout: the render pipeline shipped something the
    creator had to flag. Catalyst accumulates these per channel so recurring
    defect patterns tighten future prompts instead of repeating."""
    entry = _new_learning_entry(
        user_id=user_id,
        session=session,
        turn_kind="artifact_complaint",
        outcome="watchout",
    )
    text = re.sub(r"\s+", " ", str(complaint or "")).strip()[:220]
    label = text or "unspecified artifacting"
    entry["mistakes_to_avoid"].append(
        f"Creator flagged visual artifacting: {label}"
    )
    entry["visual_adjustments"].append(
        "Recent renders shipped with visible artifacts the creator had to flag; "
        "bias toward stronger identity locks and stricter still/clip QA before delivery."
    )
    repaired = [int(v) for v in (scenes_repaired or [])]
    failed = [int(v) for v in (scenes_failed or [])]
    entry["metadata"] = {
        "job_id": str(job_id or "").strip(),
        "complaint": label,
        "scenes_repaired": repaired,
        "scenes_failed": failed,
    }
    return _persist_entry(entry)


def record_thumbnail_style_feedback(
    user_id: str,
    session: dict[str, Any] | None,
    *,
    channel_key: str,
    feedback: str,
    job_id: str = "",
) -> dict[str, Any]:
    """Learn from a thumbnail critique ("these don't match my channel").

    Each critique is channel-scoped training data: the generated covers missed
    the channel's identity in a way the creator had to describe. Catalyst keeps
    the creator's own words so future thumbnail passes for this channel bias
    toward what they actually asked for — and away from what they rejected."""
    entry = _new_learning_entry(
        user_id=user_id,
        session=session,
        turn_kind="thumbnail_style_feedback",
        outcome="watchout",
    )
    text = re.sub(r"\s+", " ", str(feedback or "")).strip()[:260]
    label = text or "unspecified thumbnail style mismatch"
    key = str(channel_key or "").strip() or "unknown_channel"
    entry["mistakes_to_avoid"].append(
        f"Thumbnails for {key} missed the channel identity; creator said: {label}"
    )
    entry["visual_adjustments"].append(
        f"For {key} thumbnails, anchor on the channel's real published covers "
        "(reference-locked style, short on-image title) and apply the creator's "
        "recorded critiques before presenting candidates."
    )
    entry["metadata"] = {
        "channel_key": key,
        "job_id": str(job_id or "").strip(),
        "feedback": label,
    }
    return _persist_entry(entry)


def record_turn_outcome(
    user_id: str,
    session: dict[str, Any] | None,
    *,
    turn_kind: str,
    reference_payload: dict[str, Any] | None = None,
    tool_fires: list[Any] | None = None,
    search_query: str = "",
    predicted_topics: list[dict[str, Any]] | None = None,
    job_snapshot: dict[str, Any] | None = None,
    youtube_performance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Unified Studio Agent turn writeback entry point."""
    kind = str(turn_kind or "").strip().lower()
    if kind == "reference_analysis":
        return record_reference_analysis(user_id, session, reference_payload)
    if kind in {"shortform_youtube_performance", "youtube_short_performance", "yt_short_perf"}:
        perf = dict(youtube_performance or {})
        return record_shortform_youtube_performance(
            user_id,
            session,
            video_id=str(perf.get("video_id") or ""),
            title=str(perf.get("title") or ""),
            views=int(perf.get("views") or 0),
            likes=int(perf.get("likes") or 0),
            comments=int(perf.get("comments") or 0),
            avg_view_duration_sec=float(perf.get("avg_view_duration_sec") or 0),
            duration_sec=float(perf.get("duration_sec") or 0),
            impressions=int(perf.get("impressions") or 0),
            ctr=float(perf.get("ctr") or 0),
            channel_key=str(perf.get("channel_key") or ""),
            job_id=str(perf.get("job_id") or ""),
            metadata=perf.get("metadata") if isinstance(perf.get("metadata"), dict) else None,
        )
    if kind == "public_research":
        return record_public_research(
            user_id,
            session,
            tool_fires=tool_fires,
            search_query=search_query,
            predicted_topics=predicted_topics,
        )
    if kind == "production_job":
        return record_production_job(user_id, session, job_snapshot)
    return {"ok": False, "reason": f"unknown_turn_kind:{kind}"}
