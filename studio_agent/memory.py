"""Durable Studio Agent memory for user and channel personalization."""
from __future__ import annotations

import json
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_MEMORY = ROOT / "data" / "studio_agent_memory"
_APP_DATA_RAW = os.environ.get("APP_DATA_DIR", "").strip()
_APP_DATA = Path(_APP_DATA_RAW).expanduser() if _APP_DATA_RAW else None
if _APP_DATA and _APP_DATA.is_dir():
    _DEFAULT_MEMORY = _APP_DATA / "studio_agent_memory"
MEMORY_DIR = Path(os.environ.get("STUDIO_AGENT_MEMORY_DIR", str(_DEFAULT_MEMORY)))

MAX_GLOBAL_ITEMS = 80
MAX_CHANNEL_ITEMS = 120


def _now() -> float:
    return time.time()


def _safe_key(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return "anonymous"
    return re.sub(r"[^a-z0-9_.-]+", "_", raw)[:120] or "anonymous"


def _profile_path(user_id: str) -> Path:
    return MEMORY_DIR / f"{_safe_key(user_id)}.json"


def _new_profile(user_id: str) -> dict[str, Any]:
    ts = _now()
    return {
        "user_id": str(user_id or ""),
        "created_at": ts,
        "updated_at": ts,
        "global": {"memories": []},
        "channels": {},
    }


def load_profile(user_id: str) -> dict[str, Any]:
    uid = str(user_id or "").strip()
    if not uid:
        return _new_profile("")
    path = _profile_path(uid)
    if not path.exists():
        return _new_profile(uid)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _new_profile(uid)
    if not isinstance(data, dict):
        return _new_profile(uid)
    data.setdefault("user_id", uid)
    data.setdefault("global", {}).setdefault("memories", [])
    data.setdefault("channels", {})
    return data


def save_profile(profile: dict[str, Any]) -> dict[str, Any]:
    uid = str(profile.get("user_id") or "").strip()
    if not uid:
        return profile
    profile["updated_at"] = _now()
    MEMORY_DIR.mkdir(parents=True, exist_ok=True)
    _profile_path(uid).write_text(
        json.dumps(profile, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return profile


def _channel_key(
    channel_id: str = "",
    registry_key: str = "",
    title: str = "",
    handle: str = "",
) -> str:
    return (
        f"channel:{channel_id.strip()}"
        if channel_id.strip()
        else f"registry:{registry_key.strip()}"
        if registry_key.strip()
        else f"handle:{handle.strip()}"
        if handle.strip()
        else f"title:{title.strip()}"
        if title.strip()
        else "channel:unknown"
    )


def _ensure_channel(
    profile: dict[str, Any],
    *,
    channel_id: str = "",
    registry_key: str = "",
    title: str = "",
    handle: str = "",
) -> dict[str, Any]:
    key = _channel_key(channel_id, registry_key, title, handle)
    channels = profile.setdefault("channels", {})
    ch = channels.setdefault(
        key,
        {
            "key": key,
            "channel_id": "",
            "registry_key": "",
            "title": "",
            "handle": "",
            "created_at": _now(),
            "updated_at": _now(),
            "memories": [],
        },
    )
    if channel_id:
        ch["channel_id"] = str(channel_id)
    if registry_key:
        ch["registry_key"] = str(registry_key)
    if title:
        ch["title"] = str(title)
    if handle:
        ch["handle"] = str(handle)
    ch["updated_at"] = _now()
    return ch


def _dedupe_key(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip().lower())[:360]


def _append_memory(
    bucket: dict[str, Any],
    *,
    kind: str,
    note: str,
    source: str,
    importance: int = 3,
    metadata: dict[str, Any] | None = None,
    limit: int,
) -> dict[str, Any] | None:
    clean = re.sub(r"\s+", " ", str(note or "").strip())
    if len(clean) < 8:
        return None
    clean = clean[:900]
    memories = bucket.setdefault("memories", [])
    nk = _dedupe_key(clean)
    for item in memories:
        if _dedupe_key(item.get("note", "")) == nk:
            item["updated_at"] = _now()
            item["importance"] = max(int(item.get("importance") or 1), int(importance or 1))
            if metadata:
                item.setdefault("metadata", {}).update(metadata)
            return item
    item = {
        "id": f"mem_{uuid.uuid4().hex[:12]}",
        "kind": str(kind or "note")[:40],
        "note": clean,
        "source": str(source or "agent")[:80],
        "importance": max(1, min(5, int(importance or 3))),
        "created_at": _now(),
        "updated_at": _now(),
        "metadata": metadata or {},
    }
    memories.append(item)
    memories.sort(
        key=lambda m: (int(m.get("importance") or 1), float(m.get("updated_at") or 0)),
        reverse=True,
    )
    del memories[limit:]
    return item


def remember(
    user_id: str,
    note: str,
    *,
    scope: str = "global",
    channel_id: str = "",
    registry_key: str = "",
    title: str = "",
    handle: str = "",
    kind: str = "preference",
    source: str = "agent",
    importance: int = 3,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    uid = str(user_id or "").strip()
    if not uid:
        return None
    profile = load_profile(uid)
    if scope == "channel":
        bucket = _ensure_channel(
            profile,
            channel_id=channel_id,
            registry_key=registry_key,
            title=title,
            handle=handle,
        )
        item = _append_memory(
            bucket,
            kind=kind,
            note=note,
            source=source,
            importance=importance,
            metadata=metadata,
            limit=MAX_CHANNEL_ITEMS,
        )
    else:
        item = _append_memory(
            profile.setdefault("global", {}),
            kind=kind,
            note=note,
            source=source,
            importance=importance,
            metadata=metadata,
            limit=MAX_GLOBAL_ITEMS,
        )
    save_profile(profile)
    return item


def remember_channel_profile(
    user_id: str,
    *,
    channel_id: str = "",
    registry_key: str = "",
    title: str = "",
    handle: str = "",
    subscribers: int | None = None,
    note: str = "",
) -> dict[str, Any] | None:
    uid = str(user_id or "").strip()
    if not uid:
        return None
    profile = load_profile(uid)
    ch = _ensure_channel(
        profile,
        channel_id=channel_id,
        registry_key=registry_key,
        title=title,
        handle=handle,
    )
    if subscribers is not None:
        ch["subscribers"] = int(subscribers or 0)
    if note:
        _append_memory(
            ch,
            kind="channel_profile",
            note=note,
            source="channel_intelligence",
            importance=3,
            metadata={},
            limit=MAX_CHANNEL_ITEMS,
        )
    save_profile(profile)
    return ch


def record_feedback_memory(
    user_id: str,
    *,
    channel_id: str,
    outcome: str,
    video_id: str = "",
    notes: str = "",
    views: int = 0,
    ctr_percent: float = 0.0,
) -> dict[str, Any] | None:
    pieces = [f"Production outcome: {outcome}."]
    if video_id:
        pieces.append(f"Video {video_id}.")
    if views:
        pieces.append(f"Views: {views:,}.")
    if ctr_percent:
        pieces.append(f"CTR: {ctr_percent:.2f}%.")
    if notes:
        pieces.append(f"Lesson: {notes}")
    return remember(
        user_id,
        " ".join(pieces),
        scope="channel",
        channel_id=channel_id,
        kind="production_feedback",
        source="record_production_feedback",
        importance=5 if outcome in {"breakout", "strong_retention"} else 4,
        metadata={
            "outcome": outcome,
            "video_id": video_id,
            "views": views,
            "ctr_percent": ctr_percent,
        },
    )


def _short_text(value: Any, limit: int = 180) -> str:
    clean = re.sub(r"\s+", " ", str(value or "").strip())
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 1)].rstrip() + "..."


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _format_count(value: Any) -> str:
    count = _as_int(value)
    return f"{count:,}" if count > 0 else "unknown"


def _tool_channel_scope(args: dict[str, Any], data: dict[str, Any]) -> dict[str, str]:
    channel = data.get("channel") if isinstance(data.get("channel"), dict) else {}
    return {
        "channel_id": str(data.get("channel_id") or channel.get("channel_id") or args.get("channel_id") or "").strip(),
        "registry_key": str(data.get("registry_key") or channel.get("registry_key") or args.get("registry_key") or "").strip(),
        "title": str(data.get("channel_title") or channel.get("title") or "").strip(),
    }


def _remember_scoped_tool_lesson(
    user_id: str,
    note: str,
    *,
    args: dict[str, Any],
    data: dict[str, Any],
    kind: str,
    source: str,
    importance: int,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    scope_data = _tool_channel_scope(args, data)
    scope = "channel" if scope_data.get("channel_id") or scope_data.get("registry_key") else "global"
    return remember(
        user_id,
        note,
        scope=scope,
        channel_id=scope_data.get("channel_id", ""),
        registry_key=scope_data.get("registry_key", ""),
        title=scope_data.get("title", ""),
        kind=kind,
        source=source,
        importance=importance,
        metadata=metadata or {},
    )


def _public_evidence_rows(data: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("videos", "trending_sample"):
        for row in list(data.get(key) or []):
            if isinstance(row, dict):
                rows.append(row)
    seen: set[str] = set()
    clean_rows: list[dict[str, Any]] = []
    for row in rows:
        title = str(row.get("title") or "").strip()
        video_id = str(row.get("video_id") or "").strip()
        dedupe = video_id or title.lower()
        if not dedupe or dedupe in seen:
            continue
        seen.add(dedupe)
        clean_rows.append(row)
    clean_rows.sort(
        key=lambda r: (
            str(r.get("evidence_level") or "") != "hydrated_video_stats",
            -_as_int(r.get("views")),
            str(r.get("title") or "").lower(),
        )
    )
    return clean_rows[:5]


def _remember_public_youtube_evidence(
    user_id: str,
    tool_name: str,
    args: dict[str, Any],
    data: dict[str, Any],
) -> None:
    rows = _public_evidence_rows(data)
    query = str(data.get("query") or args.get("query") or args.get("niche_query") or "").strip()
    if not query:
        queries = [str(q).strip() for q in list(data.get("queries") or []) if str(q).strip()]
        query = queries[0] if queries else str(args.get("registry_key") or "public YouTube").strip()
    cache_status = str(data.get("cache_status") or ("fresh" if data.get("fresh") else "cache_allowed")).strip()
    order = str(data.get("order") or args.get("order") or "viewCount").strip()
    saved_any = False
    for row in rows:
        title = _short_text(row.get("title"), 140)
        if not title:
            continue
        evidence_level = str(row.get("evidence_level") or "").strip()
        support_label = str(row.get("support_label") or "unknown_support").strip()
        hydrated = evidence_level == "hydrated_video_stats" and row.get("views") is not None
        verdict = (
            "supported public precedent"
            if hydrated and support_label.startswith(("supported", "strong", "moderate"))
            else "weak public signal"
            if hydrated
            else "candidate only, not performance proof"
        )
        note = (
            f"Public YouTube evidence for '{query}': '{title}'"
            f"{' by ' + _short_text(row.get('channel_title'), 80) if row.get('channel_title') else ''} "
            f"has {_format_count(row.get('views'))} views, {_format_count(row.get('likes'))} likes, "
            f"support={support_label}, evidence={evidence_level or 'unknown'}, cache={cache_status}. "
            f"Verdict: {verdict}. Do not claim CTR, AVD, retention, high search volume, or trending unless a later tool returns that exact data."
        )
        _remember_scoped_tool_lesson(
            user_id,
            note,
            args=args,
            data=data,
            kind="public_youtube_evidence",
            source=tool_name,
            importance=4 if hydrated else 3,
            metadata={
                "query": query,
                "order": order,
                "cache_status": cache_status,
                "video_id": str(row.get("video_id") or ""),
                "views": _as_int(row.get("views")),
                "support_label": support_label,
                "evidence_level": evidence_level,
                "private_analytics": False,
            },
        )
        saved_any = True
    if not saved_any:
        _remember_scoped_tool_lesson(
            user_id,
            (
                f"Public YouTube search for '{query}' returned no hydrated performance proof. "
                "Treat related recommendations as experimental until hydrated views/likes and support labels are available."
            ),
            args=args,
            data=data,
            kind="public_youtube_limitation",
            source=tool_name,
            importance=4,
            metadata={"query": query, "cache_status": cache_status, "private_analytics": False},
        )


def _remember_recommended_topics(
    user_id: str,
    tool_name: str,
    args: dict[str, Any],
    data: dict[str, Any],
) -> None:
    for row in list(data.get("recommended_topics") or data.get("predicted_topics") or [])[:5]:
        if isinstance(row, dict):
            topic = _short_text(row.get("topic") or row.get("title") or row.get("angle") or row.get("query"), 140)
            score = row.get("score")
        else:
            topic = _short_text(row, 140)
            score = None
        if not topic:
            continue
        note = (
            f"Catalyst candidate topic: '{topic}'. Use it only with the cited public evidence rows and selected-channel analytics; "
            "label it experimental if the supporting public rows are weak or cache-only."
        )
        if score is not None:
            note += f" Candidate score: {score}."
        _remember_scoped_tool_lesson(
            user_id,
            note,
            args=args,
            data=data,
            kind="candidate_topic",
            source=tool_name,
            importance=3,
            metadata={"topic": topic, "private_analytics": False},
        )


def _remember_channel_analytics_lessons(
    user_id: str,
    args: dict[str, Any],
    data: dict[str, Any],
) -> None:
    playbook = data.get("growth_playbook") if isinstance(data.get("growth_playbook"), dict) else {}
    quality = data.get("analytics_data_quality") if isinstance(data.get("analytics_data_quality"), dict) else {}
    insights = data.get("insights") if isinstance(data.get("insights"), dict) else {}
    notes: list[str] = []
    source = str(quality.get("effective_source") or "").strip() or "unknown_source"
    stage = str(playbook.get("stage") or "").strip()
    if stage:
        notes.append(f"Growth stage/playbook: {stage}.")
    for key, label in (
        ("hook_patterns", "Hook/package lessons"),
        ("thumbnail_signals", "Retention/thumbnail signals"),
    ):
        vals = [str(v).strip() for v in list(insights.get(key) or []) if str(v).strip()]
        if vals:
            notes.append(f"{label}: " + "; ".join(_short_text(v, 140) for v in vals[:3]) + ".")
    top_titles = [row for row in list(insights.get("top_titles") or []) if isinstance(row, dict)]
    if top_titles:
        top = top_titles[0]
        notes.append(
            f"Top channel performer visible to Catalyst: '{_short_text(top.get('title'), 140)}' "
            f"with {_format_count(top.get('views'))} views."
        )
    breakouts = [row for row in list(insights.get("breakout_titles") or []) if isinstance(row, dict)]
    if breakouts:
        b = breakouts[0]
        notes.append(
            f"Breakout pattern candidate: '{_short_text(b.get('title'), 140)}' "
            f"lift {float(b.get('lift_vs_baseline') or 0.0):.1f}x vs channel baseline."
        )
    limitation = str(quality.get("limitation") or "").strip()
    if limitation:
        notes.append("Analytics limitation: " + _short_text(limitation, 220) + ".")
    if not notes:
        notes.append(
            "Channel analytics tool ran but did not return enough reusable performance lessons. "
            "Do not infer private CTR, AVD, retention, or per-video winners from missing rows."
        )
    remember_channel_profile(
        user_id,
        channel_id=str(data.get("channel_id") or args.get("channel_id") or ""),
        registry_key=str(data.get("registry_key") or args.get("registry_key") or ""),
        title=str(data.get("channel_title") or ""),
        note=" ".join(notes)[:900],
    )
    _remember_scoped_tool_lesson(
        user_id,
        (
            f"Private/channel analytics evidence source: {source}; "
            f"OAuth connected={bool(quality.get('oauth_connected'))}; "
            f"video rows={int(quality.get('video_rows_available') or 0)}; "
            f"retention rows={int(quality.get('retention_rows_available') or 0)}. "
            "Studio Agent may use this for this creator/channel only; do not mix it into other channels or public trend claims."
        ),
        args=args,
        data=data,
        kind="channel_analytics_contract",
        source="get_channel_analytics",
        importance=5,
        metadata={
            "effective_source": source,
            "oauth_connected": bool(quality.get("oauth_connected")),
            "video_rows_available": int(quality.get("video_rows_available") or 0),
            "retention_rows_available": int(quality.get("retention_rows_available") or 0),
            "youtube_authorized_data": bool(quality.get("oauth_connected")),
        },
    )


def _message_should_be_remembered(text: str) -> bool:
    lower = text.lower()
    return any(
        marker in lower
        for marker in (
            "remember ",
            "always ",
            "never ",
            "for this channel",
            "for my channel",
            "this channel should",
            "my channel should",
            "keep in mind",
            "don't forget",
            "already posted",
            "i already posted",
            "we already posted",
            "ignore ",
            "do not recommend",
            "don't recommend",
        )
    )


def observe_user_message(user_id: str, text: str, session: dict[str, Any] | None = None) -> dict[str, Any] | None:
    clean = re.sub(r"\s+", " ", str(text or "").strip())
    if not clean or len(clean) > 4000 or not _message_should_be_remembered(clean):
        return None
    session = session or {}
    scope = "channel" if (
        session.get("channel_id")
        or session.get("registry_key")
        or "channel" in clean.lower()
    ) else "global"
    return remember(
        user_id,
        clean[:900],
        scope=scope,
        channel_id=str(session.get("channel_id") or ""),
        registry_key=str(session.get("registry_key") or ""),
        title=str(session.get("channel_title") or ""),
        kind="user_instruction",
        source="user_message",
        importance=5 if any(w in clean.lower() for w in ("always", "never", "remember")) else 4,
    )


def observe_tool_result(
    user_id: str,
    tool_name: str,
    args: dict[str, Any] | None,
    result: str,
) -> None:
    uid = str(user_id or "").strip()
    if not uid:
        return
    args = args or {}
    try:
        data = json.loads(result) if isinstance(result, str) and result.strip().startswith(("{", "[")) else None
    except json.JSONDecodeError:
        data = None
    if tool_name == "list_youtube_channels" and isinstance(data, dict):
        for row in data.get("channels") or []:
            if not isinstance(row, dict):
                continue
            remember_channel_profile(
                uid,
                channel_id=str(row.get("channel_id") or ""),
                registry_key=str(row.get("registry_key") or ""),
                title=str(row.get("title") or ""),
                subscribers=int(row.get("subscribers") or 0),
                note=(
                    f"Connected YouTube channel {row.get('title') or row.get('channel_id')} "
                    f"has {int(row.get('subscribers') or 0):,} subscribers."
                ),
            )
        return
    if tool_name == "get_channel_analytics" and isinstance(data, dict):
        _remember_channel_analytics_lessons(uid, args, data)
        return
    if tool_name in {"search_youtube_public", "get_public_search_trends", "recommend_video_topics"} and isinstance(data, dict):
        _remember_public_youtube_evidence(uid, tool_name, args, data)
        _remember_recommended_topics(uid, tool_name, args, data)
        return
    if tool_name in {"analyze_reference_video", "analyze_competitor_video", "poll_render_job", "retry_reference_analysis"} and isinstance(data, dict):
        if data.get("error") and not (data.get("pacing") or data.get("storytelling") or data.get("visual_summary")):
            _remember_scoped_tool_lesson(
                uid,
                f"Reference/production job issue ({tool_name}): {str(data.get('error'))[:200]}",
                args=args,
                data=data,
                kind="production_watchout",
                source=tool_name,
                importance=4,
            )
            return
        storytelling = data.get("storytelling") if isinstance(data.get("storytelling"), dict) else {}
        hook = str(storytelling.get("hook") or "").strip()
        if hook:
            _remember_scoped_tool_lesson(
                uid,
                f"Reference hook pattern: {hook[:180]}",
                args=args,
                data=data,
                kind="reference_hook",
                source=tool_name,
                importance=4,
            )
        return
    if tool_name == "refresh_channel_intelligence" and isinstance(data, dict):
        learnings = []
        learnings.extend([str(x) for x in (data.get("packaging_learnings") or [])[:3]])
        learnings.extend([str(x) for x in (data.get("retention_learnings") or [])[:3]])
        remember_channel_profile(
            uid,
            channel_id=str(data.get("channel_id") or args.get("channel_id") or ""),
            title=str(data.get("title") or ""),
            note=("Fresh channel intelligence: " + "; ".join(learnings))[:900] if learnings else "",
        )


def _format_item(item: dict[str, Any]) -> str:
    kind = str(item.get("kind") or "note").replace("_", " ")
    return f"- {kind}: {str(item.get('note') or '').strip()}"


def summarize_for_prompt(
    user_id: str,
    *,
    channel_id: str = "",
    registry_key: str = "",
    max_global: int = 8,
    max_channel: int = 12,
) -> str:
    profile = load_profile(user_id)
    global_items = (profile.get("global") or {}).get("memories") or []
    parts: list[str] = []
    if global_items:
        parts.append("Global user memory:")
        parts.extend(_format_item(i) for i in global_items[:max_global] if isinstance(i, dict))
    channels = profile.get("channels") or {}
    selected: list[dict[str, Any]] = []
    for ch in channels.values():
        if not isinstance(ch, dict):
            continue
        if channel_id and ch.get("channel_id") == channel_id:
            selected.append(ch)
        elif registry_key and ch.get("registry_key") == registry_key:
            selected.append(ch)
    if not selected and channels:
        selected = sorted(
            [c for c in channels.values() if isinstance(c, dict)],
            key=lambda c: float(c.get("updated_at") or 0),
            reverse=True,
        )[:4]
    for ch in selected:
        label = ch.get("title") or ch.get("handle") or ch.get("registry_key") or ch.get("channel_id") or "channel"
        parts.append(f"Channel memory: {label}")
        meta = []
        if ch.get("registry_key"):
            meta.append(f"registry_key={ch.get('registry_key')}")
        if ch.get("channel_id"):
            meta.append(f"channel_id={ch.get('channel_id')}")
        if ch.get("subscribers") is not None:
            meta.append(f"subscribers={int(ch.get('subscribers') or 0):,}")
        if meta:
            parts.append("- " + ", ".join(meta))
        parts.extend(_format_item(i) for i in (ch.get("memories") or [])[:max_channel] if isinstance(i, dict))
    return "\n".join(parts).strip()


def public_profile(user_id: str) -> dict[str, Any]:
    profile = load_profile(user_id)
    return {
        "user_id": profile.get("user_id"),
        "updated_at": profile.get("updated_at"),
        "global": profile.get("global") or {"memories": []},
        "channels": list((profile.get("channels") or {}).values()),
    }
