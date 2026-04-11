from __future__ import annotations

import json
import logging
import math
import random
import re
import time
from pathlib import Path

from fastapi import HTTPException


log = logging.getLogger("nyptid-studio")

CATALYST_HUB_SHORT_WORKSPACES: list[str] = []
CATALYST_HUB_LONGFORM_WORKSPACES: list[str] = []
CATALYST_REFERENCE_ANALYSIS_DEFAULT_MINUTES = 20.0
CATALYST_REFERENCE_ANALYSIS_MAX_SECONDS = 20.0 * 60.0
CATALYST_REFERENCE_AUDIO_MAX_SECONDS = 360.0
CATALYST_MARKETING_DOCTRINE = [
    "Be active in the Daily Marketing Channel.",
    "Analyze and Improve. Evaluate each marketing piece to understand what works and what doesn't. Think about how you could improve it.",
    "Small, daily improvements in your marketing skills can lead to significant progress over time due to compounding.",
    "Just like in boxing or other martial arts, consistent practice and real-world application are crucial for mastering marketing.",
    "Engage with the daily challenges to continuously hone your skills. Missing a day occasionally is okay, but don't make it a habit.",
    "Regardless of your field or business, understanding and practicing marketing is fundamental to success.",
    "Treat the daily marketing challenges seriously and make it a part of your routine to see substantial benefits in your marketing abilities.",
    "Mastering marketing has enabled Arno to start and scale companies and avoid manual labor by understanding how to attract clients and improve businesses.",
    "It is a long-lasting skill. Marketing has been around for millennia and will continue to be valuable in the future.",
    "Anyone can learn it. It doesn't require special skills, abilities, or connections. Pay attention, focus, and you can succeed.",
    "High ROI (Return On Investment). Direct response marketing offers the highest and most reliable return on investment, outperforming traditional investments.",
    "Learning marketing helps you see opportunities and gaps that others miss, making life easier.",
    "You don't need to be the world's best marketer; being better than most is enough to succeed.",
    "It is a fast skill to learn. With ten days of dedicated study, you can acquire valuable marketing skills.",
    "Be ready for a significant change as you learn and apply these marketing skills.",
]
SHORTS_PUBLIC_REFERENCE_QUERY_SEEDS = {
    "skeleton": [
        "viral skeleton shorts",
        "skeleton comparison shorts",
        "dark facts skeleton shorts",
    ],
    "story": [
        "faceless story shorts",
        "cinematic ai story shorts",
        "viral ai story shorts",
    ],
    "motivation": [
        "motivation shorts",
        "self improvement shorts",
        "discipline shorts",
    ],
    "daytrading": [
        "day trading shorts",
        "trading psychology shorts",
        "stock market shorts",
    ],
    "chatstory": [
        "chat story shorts",
        "text story shorts",
        "dramatic chat shorts",
    ],
}

_catalyst_learning_records_getter = lambda: {}
_catalyst_channel_memory_getter = lambda: {}
_catalyst_failure_mode_label = lambda value: str(value or "").strip()
_clip_text = lambda value, max_chars=320: str(value or "").strip()[:max_chars]
_load_youtube_connections = lambda: None
_save_youtube_connections = lambda: None
_youtube_bucket_for_user = lambda user_id: {}
_youtube_sync_and_persist_for_user = None
_youtube_connection_public_view = lambda record: dict(record or {})
_longform_owner_beta_enabled = lambda user: False
_harvest_catalyst_outcomes_for_channel = None
_longform_outcome_sync_semaphore = None
_youtube_ensure_access_token = None
_youtube_fetch_channel_search = None
_longform_sessions_lock = None
_load_longform_sessions = lambda: None
_longform_sessions_getter = lambda: {}
_save_longform_sessions = lambda: None
_match_published_video_to_longform_session = lambda session, candidates: {}
_youtube_fetch_video_analytics = None
_build_auto_outcome_request = None
_persist_catalyst_outcome_for_session = None
_longform_public_session = lambda session: dict(session or {})
_title_is_too_close_to_source = lambda left, right: False
_shortform_priority_snapshot = None
_youtube_selected_channel_context = None
_youtube_connected_channel_access_token = None
_youtube_fetch_public_channel_page_videos = None
_youtube_apply_public_inventory_to_snapshot = lambda snapshot, rows: dict(snapshot or {})
_youtube_historical_compare_measured_public_view = lambda payload: dict(payload or {})
_youtube_channel_audit_measured_public_view = lambda payload: dict(payload or {})
_load_catalyst_memory = lambda: None
_catalyst_channel_memory_key = lambda user_id, channel_id, workspace_id: ""
_catalyst_channel_memory_public_view = lambda payload: dict(payload or {})
_persist_public_shorts_playbook_memory = None
_public_shorts_playbook_from_memory_view = lambda payload: {}
_apply_catalyst_public_shorts_playbook_to_channel_memory = (
    lambda existing=None, **kwargs: dict(existing or {})
)
_reconcile_reference_video_analysis_with_inventory = lambda analysis, channel_context: dict(analysis or {})
_catalyst_reference_video_analysis_public_view = lambda analysis: dict(analysis or {})
_build_catalyst_reference_video_analysis = None
_resolve_catalyst_series_context = lambda *args, **kwargs: {}
_youtube_connections_lock = None
_catalyst_memory_lock = None
_catalyst_hub_workspace_label = lambda workspace_id: str(workspace_id or "").strip()
_xai_json_completion = None
_title_is_too_close_to_any = lambda candidate, existing_titles: False
_catalyst_reference_workspace_profile = lambda workspace_id: {}
_normalize_catalyst_reference_video_analysis = lambda payload: dict(payload or {})
_catalyst_infer_niche = lambda title, description, transcript_excerpt, format_preset="documentary": {}
_catalyst_infer_archetype = lambda title, description, transcript_excerpt, niche_key="", format_preset="documentary": {}
_same_arena_title_variants = lambda seed, topic="", format_preset="documentary", max_items=5: []
_longform_review_state = lambda session_snapshot: {}
_heuristic_catalyst_longform_execution_qa = lambda session_snapshot, edit_blueprint, package: {}
_heuristic_catalyst_short_learning_record = lambda **kwargs: {}
_update_catalyst_channel_memory = lambda existing=None, **kwargs: dict(existing or {})
_dedupe_preserve_order = lambda values, max_items=8, max_chars=180: list(values or [])[:max_items]
_render_catalyst_channel_memory_context = lambda memory_public: ""
_catalyst_rewrite_pressure_profile = lambda memory_public: {}
_build_catalyst_reference_playbook = lambda **kwargs: {}
_catalyst_rank_shorts_angle_candidates = lambda **kwargs: []
_build_shorts_public_reference_playbook = None
_build_shorts_trend_query = lambda template, topic, channel_context, selected_cluster: ""
_youtube_fetch_public_trend_titles = None
_youtube_fetch_public_reference_shorts = None
_youtube_title_keywords = lambda title, max_items=8: []
_catalyst_reference_memory_getter = lambda: {}
_catalyst_reference_analysis_confidence_label = (
    lambda analysis_mode, heuristic_used=False, frame_metrics=None, transcript_excerpt="", audio_summary="": "Low"
)
_dedupe_clip_list = lambda values, max_items=8: list(values or [])[:max_items]
_youtube_watch_url = lambda video_id: ""
_save_catalyst_memory = lambda: None
_normalize_transition_style = lambda value: str(value or "smooth").strip().lower() or "smooth"
_jobs_getter = lambda: {}
_is_admin_user = lambda user: False
_longform_deep_analysis_enabled = lambda user: False
_active_longform_capacity_session_id = None
_create_longform_session_internal = None
_bool_from_any = lambda value, default=False: default if value is None else bool(value)
_normalize_longform_target_minutes = lambda value: float(value or 0.0)
_normalize_longform_language = lambda value: str(value or "en").strip() or "en"
_normalize_external_source_url = lambda value: str(value or "").strip()
_reference_video_analysis_dir = lambda user_id, channel_id, workspace_id, video_id: Path(".")
_manual_catalyst_reference_video_id = lambda source_url="", title="", filename="": ""
_pick_catalyst_reference_video = lambda channel_context, requested_video_id="": {}
_pick_reference_preview_frame_urls = lambda source_bundle, info, max_items=6: []
_summarize_longform_operator_evidence = lambda operator_evidence=None, source_bundle=None: ""
_extract_reference_video_stream_clip = None
_extract_reference_video_full_audit = None
_extract_reference_preview_frames_from_urls = None
_xai_json_completion_multimodal = None
_fetch_source_video_bundle = None
_fetch_algrow_reference_enrichment = None
_youtube_fetch_video_analytics_bulk = None
_download_youtube_video_for_reference_analysis = None
_audit_manual_comparison_video = None
_build_catalyst_analysis_proxy_video = None
_extract_video_metadata = None
_youtube_fetch_owned_video_bundle_oauth = None
_extract_audio_from_video = None
_transcribe_audio_with_grok = None


def _marketing_doctrine_text(extra_notes: str = "") -> str:
    doctrine = list(CATALYST_MARKETING_DOCTRINE)
    if str(extra_notes or "").strip():
        doctrine.append(str(extra_notes).strip())
    return "\n".join(f"- {line}" for line in doctrine if str(line).strip())


def _summarize_public_shorts_reference_playbook(
    template: str,
    reference_rows: list[dict],
    queries: list[str],
    *,
    topic: str = "",
    channel_context: dict | None = None,
    selected_cluster: dict | None = None,
    memory_public: dict | None = None,
    trend_titles: list[str] | None = None,
    trend_hunt_enabled: bool = False,
    youtube_title_keywords=None,
) -> dict:
    rows = [dict(row or {}) for row in list(reference_rows or []) if isinstance(row, dict)]
    if not rows:
        return {}
    channel_context = dict(channel_context or {})
    selected_cluster = dict(selected_cluster or {})
    memory_public = dict(memory_public or {})
    trend_titles = [str(v).strip() for v in list(trend_titles or []) if str(v).strip()]
    keyword_fn = youtube_title_keywords or _youtube_title_keywords
    titles = [str(row.get("title", "") or "").strip() for row in rows if str(row.get("title", "") or "").strip()]
    channels = _dedupe_preserve_order(
        [str(row.get("channel_title", "") or "").strip() for row in rows if str(row.get("channel_title", "") or "").strip()],
        max_items=6,
        max_chars=80,
    )
    avg_title_chars = round(sum(len(title) for title in titles) / max(len(titles), 1), 1)
    avg_duration_sec = round(
        sum(max(0, int(row.get("duration_sec", 0) or 0)) for row in rows) / max(len(rows), 1),
        1,
    )
    strong_keywords: list[str] = []
    keyword_scores: dict[str, int] = {}
    for title in titles:
        for keyword in keyword_fn(title, max_items=8):
            keyword_scores[keyword] = int(keyword_scores.get(keyword, 0)) + 1
    for keyword, _score in sorted(keyword_scores.items(), key=lambda item: (-item[1], item[0]))[:6]:
        strong_keywords.append(keyword)
    number_lead_count = sum(1 for title in titles if re.match(r"^\s*(\d+|top\s+\d+)", title, flags=re.IGNORECASE))
    contrast_count = sum(1 for title in titles if re.search(r"\b(vs\.?|versus|instead of|before|after)\b", title, flags=re.IGNORECASE))
    emotion_count = sum(1 for title in titles if re.search(r"\b(shocking|crazy|insane|worst|best|deadly|dark|secret|hidden|broke|ruined)\b", title, flags=re.IGNORECASE))
    hook_moves: list[str] = []
    packaging_moves: list[str] = []
    visual_moves: list[str] = []
    if number_lead_count >= max(2, len(titles) // 3):
        hook_moves.append("Use a number-led or measurable hook when it sharpens curiosity instantly.")
    if contrast_count >= max(2, len(titles) // 3):
        hook_moves.append("Lead with a clear contrast or before-versus-after premise instead of a vague summary.")
    if emotion_count >= max(2, len(titles) // 4):
        packaging_moves.append("Bias toward stronger emotional words only when the contrast is obvious in the first second.")
    if avg_title_chars > 54:
        packaging_moves.append("Keep titles tighter than the current public benchmark and avoid over-explaining the premise.")
    else:
        packaging_moves.append("Stay concise and immediately readable; winning shorts here are not premise-dense.")
    normalized = str(template or "").strip().lower()
    if normalized == "skeleton":
        visual_moves.extend([
            "Use one dominant skeleton subject with one instantly readable comparison or contradiction.",
            "Avoid cluttered lore, extra characters, or abstract filler props that weaken the first-second read.",
        ])
        hook_moves.append("Push a fresher skeleton angle that feels shareable, not another recycled profession matchup.")
    elif normalized == "daytrading":
        visual_moves.extend([
            "Keep the frame anchored to realistic trading screens, chart hierarchy, and execution stakes.",
            "Avoid generic wealth flexing or abstract sci-fi props that break credibility.",
        ])
        hook_moves.append("Favor consequence-led trading hooks: mistakes, traps, hidden costs, or asymmetric upside.")
    elif normalized == "motivation":
        visual_moves.append("Favor one strong action or obstacle per scene instead of generic cinematic filler.")
    elif normalized == "chatstory":
        visual_moves.append("Make the conflict legible as text-message drama in the first second.")
    elif normalized == "story":
        visual_moves.append("Favor one emotionally charged visual turn per beat with clear subject continuity.")
    benchmark_titles = _dedupe_preserve_order(titles, max_items=6, max_chars=120)
    angle_candidates = _catalyst_rank_shorts_angle_candidates(
        template=template,
        topic=topic,
        channel_context=channel_context,
        selected_cluster=selected_cluster,
        memory_public=memory_public,
        benchmark_titles=benchmark_titles,
        trend_titles=trend_titles,
        hook_moves=hook_moves,
        packaging_moves=packaging_moves,
        visual_moves=visual_moves,
        keyword_moves=strong_keywords[:6],
        trend_hunt_enabled=trend_hunt_enabled,
        max_items=6,
    )
    summary = (
        f"Public shorts benchmark from {len(rows)} fresh references across queries "
        f"{', '.join(_dedupe_preserve_order(list(queries or []), max_items=3, max_chars=60))}: "
        f"avg title length {avg_title_chars} chars, avg duration {avg_duration_sec}s. "
        f"Repeated keywords: {', '.join(strong_keywords[:4]) or 'none'}."
    )
    return {
        "summary": _clip_text(summary, 320),
        "benchmark_titles": benchmark_titles,
        "benchmark_channels": channels,
        "hook_moves": _dedupe_preserve_order(hook_moves, max_items=5, max_chars=180),
        "packaging_moves": _dedupe_preserve_order(packaging_moves, max_items=5, max_chars=180),
        "visual_moves": _dedupe_preserve_order(visual_moves, max_items=5, max_chars=180),
        "keyword_moves": strong_keywords[:6],
        "angle_candidates": angle_candidates,
        "trend_titles": _dedupe_preserve_order(trend_titles, max_items=6, max_chars=120),
    }


def _build_shorts_reference_queries(
    template: str,
    topic: str,
    channel_context: dict,
    selected_cluster: dict,
    trend_hunt_enabled: bool = False,
) -> list[str]:
    normalized = str(template or "").strip().lower()
    queries: list[str] = []
    raw_topic = re.sub(r"\s+", " ", str(topic or "").strip())
    if raw_topic:
        queries.append(_clip_text(raw_topic, 100))
    cluster_keywords = [str(v).strip() for v in list(selected_cluster.get("keywords") or []) if str(v).strip()]
    if cluster_keywords:
        queries.append(_clip_text(" ".join(cluster_keywords[:4]), 100))
    channel_titles = [str(v).strip() for v in list(channel_context.get("recent_upload_titles") or []) if str(v).strip()]
    if channel_titles:
        queries.append(_clip_text(re.sub(r"\s*\|.*$", "", channel_titles[0]).strip(), 100))
    queries.extend(list(SHORTS_PUBLIC_REFERENCE_QUERY_SEEDS.get(normalized, [])))
    if trend_hunt_enabled and normalized == "skeleton":
        queries.extend([
            "viral skeleton trend shorts",
            "skeleton facts shorts trend",
        ])
    return _dedupe_preserve_order([q for q in queries if q], max_items=5, max_chars=100)


async def _build_shorts_public_reference_playbook(
    template: str,
    topic: str,
    channel_context: dict,
    selected_cluster: dict,
    memory_public: dict | None = None,
    trend_hunt_enabled: bool = False,
) -> dict:
    queries = _build_shorts_reference_queries(
        template,
        topic,
        channel_context,
        selected_cluster,
        trend_hunt_enabled=trend_hunt_enabled,
    )
    if not queries:
        return {}
    if _youtube_fetch_public_reference_shorts is None:
        return {}
    rows: list[dict] = []
    seen_ids: set[str] = set()
    for query in queries:
        query_rows = await _youtube_fetch_public_reference_shorts(query, max_results=6)
        for row in query_rows:
            video_id = str(row.get("video_id", "") or "").strip()
            if not video_id or video_id in seen_ids:
                continue
            seen_ids.add(video_id)
            rows.append(dict(row))
        if len(rows) >= 12:
            break
    if not rows:
        return {}
    trend_titles: list[str] = []
    try:
        trend_query = _build_shorts_trend_query(template, topic, channel_context, selected_cluster)
        trend_titles = await _youtube_fetch_public_trend_titles(trend_query, max_results=6)
    except Exception:
        trend_titles = []
    return _summarize_public_shorts_reference_playbook(
        template,
        rows,
        queries,
        topic=topic,
        channel_context=channel_context,
        selected_cluster=selected_cluster,
        memory_public=memory_public,
        trend_titles=trend_titles,
        trend_hunt_enabled=trend_hunt_enabled,
    )


def _public_shorts_playbook_from_memory_view(memory_public: dict | None) -> dict:
    public = dict(memory_public or {})
    angle_candidates = [dict(v or {}) for v in list(public.get("public_shorts_angle_candidates") or []) if isinstance(v, dict)]
    promoted_angles = [str(v).strip() for v in list(public.get("promoted_shorts_angles") or []) if str(v).strip()]
    existing_angle_keys = {
        str((row or {}).get("angle", "") or "").strip().lower()
        for row in angle_candidates
        if isinstance(row, dict) and str((row or {}).get("angle", "") or "").strip()
    }
    for index, angle in enumerate(promoted_angles):
        lowered = angle.lower()
        if lowered in existing_angle_keys:
            continue
        angle_candidates.insert(
            index,
            {
                "angle": _clip_text(angle, 100),
                "source": "catalyst-memory",
                "score": round(999.0 - index, 3),
                "novelty_score": 100,
                "why_now": "Catalyst promoted this short angle from measured channel learning.",
                "hook_move": "",
                "packaging_move": "",
                "visual_move": "",
                "keyword_bias": [],
                "archetype_label": _clip_text(str(public.get("archetype_label", "") or "").strip(), 60),
            },
        )
        existing_angle_keys.add(lowered)
    playbook = {
        "summary": _clip_text(str(public.get("public_shorts_summary", "") or "").strip(), 320),
        "benchmark_titles": [
            str(v).strip() for v in list(public.get("public_shorts_benchmark_titles") or []) if str(v).strip()
        ],
        "benchmark_channels": [
            str(v).strip() for v in list(public.get("public_shorts_benchmark_channels") or []) if str(v).strip()
        ],
        "hook_moves": [str(v).strip() for v in list(public.get("public_shorts_hook_moves") or []) if str(v).strip()],
        "packaging_moves": [
            str(v).strip() for v in list(public.get("public_shorts_packaging_moves") or []) if str(v).strip()
        ],
        "visual_moves": [str(v).strip() for v in list(public.get("public_shorts_visual_moves") or []) if str(v).strip()],
        "keyword_moves": [str(v).strip() for v in list(public.get("public_shorts_keyword_moves") or []) if str(v).strip()],
        "angle_candidates": angle_candidates[:8],
        "trend_titles": [str(v).strip() for v in list(public.get("public_shorts_trend_titles") or []) if str(v).strip()],
    }
    if not any(playbook.values()):
        return {}
    return playbook


async def _persist_public_shorts_playbook_memory(
    *,
    user: dict | None,
    template: str,
    topic: str = "",
    preferred_channel_id: str = "",
    trend_hunt_enabled: bool = False,
    channel_context: dict | None = None,
) -> dict:
    if not isinstance(user, dict):
        return {}
    channel_context = dict(channel_context or {})
    try:
        if not channel_context and (preferred_channel_id or trend_hunt_enabled):
            channel_context = await _youtube_selected_channel_context(user, preferred_channel_id)
    except Exception as e:
        log.warning(f"Persist shorts benchmark channel context failed for template={template}: {e}")
        return {}
    channel_id = str(channel_context.get("channel_id", "") or preferred_channel_id or "").strip()
    if not channel_id:
        return {}
    memory_key = _catalyst_channel_memory_key(
        str(user.get("id", "") or ""),
        channel_id,
        str(template or "story").strip().lower() or "story",
    )
    channel_memory_store = _catalyst_channel_memory_getter()
    if not isinstance(channel_memory_store, dict):
        channel_memory_store = {}
    async with _catalyst_memory_lock:
        _load_catalyst_memory()
        base_memory = dict(channel_memory_store.get(memory_key) or {})
    series_context = _resolve_catalyst_series_context(
        channel_context,
        channel_memory=base_memory,
        topic=topic,
        source_title="",
        input_title="",
        input_description="",
        format_preset="documentary" if str(template or "").strip().lower() == "daytrading" else "",
    )
    selected_cluster = dict(series_context.get("selected_cluster") or {})
    try:
        public_shorts_playbook = await _build_shorts_public_reference_playbook(
            template,
            topic,
            channel_context,
            selected_cluster,
            memory_public=dict(series_context.get("memory_view") or {}),
            trend_hunt_enabled=trend_hunt_enabled,
        )
    except Exception as e:
        log.warning(f"Persist shorts benchmark playbook failed for template={template}: {e}")
        public_shorts_playbook = {}
    if not public_shorts_playbook:
        return {
            "channel_context": channel_context,
            "selected_cluster": selected_cluster,
            "playbook": {},
            "memory_key": memory_key,
            "memory_public": _catalyst_channel_memory_public_view(base_memory),
        }
    async with _catalyst_memory_lock:
        _load_catalyst_memory()
        latest_memory = dict(channel_memory_store.get(memory_key) or {})
        updated_memory = _apply_catalyst_public_shorts_playbook_to_channel_memory(
            existing=latest_memory,
            template=template,
            topic=topic,
            channel_id=channel_id,
            channel_context=channel_context,
            selected_cluster=selected_cluster,
            public_shorts_playbook=public_shorts_playbook,
        )
        updated_memory["key"] = memory_key
        channel_memory_store[memory_key] = updated_memory
        _save_catalyst_memory()
    return {
        "channel_context": channel_context,
        "selected_cluster": selected_cluster,
        "playbook": public_shorts_playbook,
        "memory_key": memory_key,
        "memory_public": _catalyst_channel_memory_public_view(updated_memory),
    }


def _build_longform_operator_notes(
    analytics_notes: str = "",
    transcript_text: str = "",
    operator_evidence: dict | None = None,
) -> str:
    notes: list[str] = []
    analytics_notes = str(analytics_notes or "").strip()
    transcript_text = str(transcript_text or "").strip()
    operator_evidence = dict(operator_evidence or {})
    if analytics_notes:
        notes.append(analytics_notes)
    if transcript_text:
        notes.append("Manual transcript excerpt: " + _clip_text(transcript_text, 2400))
    analytics_summary = str(operator_evidence.get("analytics_summary", "") or "").strip()
    if analytics_summary:
        notes.append("Analytics screenshot summary: " + _clip_text(analytics_summary, 600))
    for label, key in [
        ("Strongest signals", "strongest_signals"),
        ("Weak points", "weak_points"),
        ("Retention findings", "retention_findings"),
        ("Packaging findings", "packaging_findings"),
        ("Improvement moves", "improvement_moves"),
    ]:
        values = [str(v).strip() for v in list(operator_evidence.get(key) or []) if str(v).strip()]
        if values:
            notes.append(f"{label}: " + "; ".join(_clip_text(v, 160) for v in values[:6]))
    return "\n".join(note for note in notes if note)


def _catalyst_short_memory_public_snapshot(
    *,
    user_id: str = "",
    template: str = "",
    youtube_channel_id: str = "",
    seed_memory_public: dict | None = None,
) -> dict:
    seeded = dict(seed_memory_public or {})
    if seeded:
        return seeded
    user_key = str(user_id or "").strip()
    channel_key = str(youtube_channel_id or "").strip()
    template_key = str(template or "").strip().lower() or "shorts"
    if not user_key or not channel_key:
        return {}
    try:
        _load_catalyst_memory()
        memory_key = _catalyst_channel_memory_key(user_key, channel_key, template_key)
        channel_memory_store = _catalyst_channel_memory_getter()
        if not isinstance(channel_memory_store, dict):
            return {}
        return _catalyst_channel_memory_public_view(dict(channel_memory_store.get(memory_key) or {}))
    except Exception:
        return {}


async def _persist_catalyst_short_learning_for_render(
    *,
    user_id: str,
    job_id: str,
    template: str,
    topic: str,
    youtube_channel_id: str = "",
    script_data: dict | None = None,
    scenes: list | None = None,
    word_timings: list | None = None,
    sound_mix_profile: dict | None = None,
    transition_style: str = "smooth",
    pacing_mode: str = "standard",
    voice_speed: float = 1.0,
    sfx_paths: list[str] | None = None,
    bgm_track: str = "",
    subtitle_path: str = "",
    animation_enabled: bool = True,
) -> dict:
    user_id = str(user_id or "").strip()
    if not user_id:
        return {}
    template_key = str(template or "").strip().lower() or "shorts"
    script_data = dict(script_data or {})
    scenes = [dict(scene or {}) for scene in list(scenes or [])]
    memory_channel_id = str(youtube_channel_id or "").strip()
    channel_context = {}
    if memory_channel_id:
        try:
            channel_context = await _youtube_selected_channel_context({"id": user_id}, preferred_channel_id=memory_channel_id)
            memory_channel_id = str((channel_context or {}).get("channel_id", "") or memory_channel_id or "").strip()
        except Exception as e:
            log.warning(f"[{job_id}] Catalyst short learning channel context failed: {e}")
    channel_memory_key = _catalyst_channel_memory_key(user_id, memory_channel_id, template_key)
    channel_memory_store = _catalyst_channel_memory_getter()
    if not isinstance(channel_memory_store, dict):
        return {}
    async with _catalyst_memory_lock:
        _load_catalyst_memory()
        existing_memory = dict(channel_memory_store.get(channel_memory_key) or {})
    selected_series_context = _resolve_catalyst_series_context(
        channel_context,
        channel_memory=existing_memory,
        topic=topic,
        source_title=str(script_data.get("title", "") or ""),
        input_title=str(script_data.get("title", "") or ""),
        input_description=str(script_data.get("description", "") or ""),
        format_preset=template_key,
    )
    memory_view = dict(selected_series_context.get("memory_view") or existing_memory or {})
    selected_cluster = dict(selected_series_context.get("selected_cluster") or {})
    package_payload = {
        "selected_title": str(script_data.get("title", "") or topic or "").strip(),
        "selected_description": str(script_data.get("description", "") or "").strip(),
        "selected_tags": [str(v).strip() for v in list(script_data.get("tags") or []) if str(v).strip()],
    }
    session_snapshot = {
        "session_id": job_id,
        "user_id": user_id,
        "template": template_key,
        "topic": topic,
        "format_preset": template_key,
        "youtube_channel_id": memory_channel_id,
        "channel_memory_key": channel_memory_key,
        "metadata_pack": {
            "youtube_channel": dict(channel_context or {}),
            "selected_series_cluster": dict(selected_cluster or {}),
            "catalyst_channel_memory": dict(memory_view or {}),
            "catalyst_public_shorts_playbook": _public_shorts_playbook_from_memory_view(memory_view),
        },
    }
    learning_record = _heuristic_catalyst_short_learning_record(
        session_snapshot=session_snapshot,
        scenes=scenes,
        word_timings=[dict(item or {}) for item in list(word_timings or []) if isinstance(item, dict)],
        package=package_payload,
        sound_mix_profile=dict(sound_mix_profile or {}),
        transition_style=transition_style,
        pacing_mode=pacing_mode,
        voice_speed=voice_speed,
        sfx_paths=list(sfx_paths or []),
        bgm_track=bgm_track,
        subtitle_path=subtitle_path,
        animation_enabled=animation_enabled,
    )
    timeline_qa = dict(learning_record.get("timeline_qa") or {})
    execution_strategy = dict(learning_record.get("execution_strategy") or {})
    edit_blueprint = {
        "visual_engine": f"{template_key}_shorts",
        "motion_strategy": {
            "transition_style": _normalize_transition_style(transition_style),
            "visual_rules": list(learning_record.get("visual_adjustments") or [])[:4],
        },
        "sound_strategy": {
            "music_profile": str((dict(sound_mix_profile or {})).get("music_profile", "") or ""),
            "mix_notes": list(learning_record.get("sound_adjustments") or [])[:4],
            "voice_direction": [f"Voice pacing bias: {str(execution_strategy.get('voice_pacing_bias', '') or '').strip()}"] if str(execution_strategy.get("voice_pacing_bias", "") or "").strip() else [],
        },
        "execution_strategy": execution_strategy,
    }
    async with _catalyst_memory_lock:
        _load_catalyst_memory()
        latest_memory = dict(channel_memory_store.get(channel_memory_key) or memory_view or {})
        updated_channel_memory = _update_catalyst_channel_memory(
            existing=latest_memory,
            session_snapshot=session_snapshot,
            learning_record=learning_record,
            edit_blueprint=edit_blueprint,
            package=package_payload,
        )
        updated_channel_memory["key"] = channel_memory_key
        channel_memory_store[channel_memory_key] = updated_channel_memory
        learning_records_store = _catalyst_learning_records_getter()
        if isinstance(learning_records_store, dict):
            existing_learning_entry = learning_records_store.get(job_id)
            if isinstance(existing_learning_entry, dict):
                learning_entry = dict(existing_learning_entry)
            else:
                learning_entry = {}
            learning_entry["prepublish"] = dict(learning_record)
            learning_entry["latest_short_timeline_qa"] = timeline_qa
            learning_records_store[job_id] = learning_entry
        _save_catalyst_memory()
    payload = {
        "summary": str(learning_record.get("outcome_summary", "") or "").strip(),
        "timeline_qa": timeline_qa,
        "channel_memory": _catalyst_channel_memory_public_view(updated_channel_memory),
    }
    jobs_store = _jobs_getter()
    if isinstance(jobs_store, dict):
        job_entry = jobs_store.get(job_id)
        if isinstance(job_entry, dict):
            job_entry["catalyst_short_learning"] = payload
    return payload


def configure_catalyst_runtime_hooks(
    *,
    catalyst_hub_short_workspaces: list[str] | None = None,
    catalyst_hub_longform_workspaces: list[str] | None = None,
    catalyst_reference_analysis_default_minutes: float | None = None,
    catalyst_learning_records_getter=None,
    catalyst_channel_memory_getter=None,
    catalyst_failure_mode_label=None,
    clip_text=None,
    load_youtube_connections=None,
    save_youtube_connections_runtime=None,
    youtube_bucket_for_user=None,
    youtube_sync_and_persist_for_user=None,
    youtube_connection_public_view=None,
    longform_owner_beta_enabled=None,
    is_admin_user=None,
    longform_deep_analysis_enabled=None,
    active_longform_capacity_session_id=None,
    create_longform_session_internal=None,
    bool_from_any=None,
    normalize_longform_target_minutes=None,
    normalize_longform_language=None,
    harvest_catalyst_outcomes_for_channel=None,
    longform_outcome_sync_semaphore=None,
    youtube_ensure_access_token=None,
    youtube_fetch_channel_search=None,
    longform_sessions_lock=None,
    load_longform_sessions_runtime=None,
    longform_sessions_getter=None,
    save_longform_sessions_runtime=None,
    match_published_video_to_longform_session=None,
    youtube_fetch_video_analytics=None,
    build_auto_outcome_request=None,
    persist_catalyst_outcome_for_session=None,
    longform_public_session=None,
    title_is_too_close_to_source=None,
    shortform_priority_snapshot=None,
    youtube_selected_channel_context=None,
    youtube_connected_channel_access_token=None,
    youtube_fetch_public_channel_page_videos=None,
    youtube_apply_public_inventory_to_snapshot=None,
    youtube_historical_compare_measured_public_view=None,
    youtube_channel_audit_measured_public_view=None,
    load_catalyst_memory=None,
    catalyst_channel_memory_key=None,
    catalyst_channel_memory_public_view=None,
    apply_catalyst_public_shorts_playbook_to_channel_memory=None,
    persist_public_shorts_playbook_memory=None,
    public_shorts_playbook_from_memory_view=None,
    reconcile_reference_video_analysis_with_inventory=None,
    catalyst_reference_video_analysis_public_view=None,
    build_catalyst_reference_video_analysis=None,
    resolve_catalyst_series_context=None,
    youtube_connections_lock=None,
    catalyst_memory_lock=None,
    catalyst_hub_workspace_label=None,
    xai_json_completion=None,
    title_is_too_close_to_any=None,
    catalyst_reference_workspace_profile=None,
    normalize_catalyst_reference_video_analysis=None,
    catalyst_infer_niche=None,
    catalyst_infer_archetype=None,
    same_arena_title_variants=None,
    longform_review_state=None,
    heuristic_catalyst_longform_execution_qa=None,
    heuristic_catalyst_short_learning_record=None,
    update_catalyst_channel_memory=None,
    normalize_transition_style=None,
    jobs_getter=None,
    dedupe_preserve_order=None,
    render_catalyst_channel_memory_context=None,
    catalyst_rewrite_pressure_profile=None,
    build_catalyst_reference_playbook=None,
    catalyst_rank_shorts_angle_candidates=None,
    build_shorts_public_reference_playbook=None,
    build_shorts_trend_query=None,
    youtube_fetch_public_trend_titles=None,
    youtube_fetch_public_reference_shorts=None,
    youtube_title_keywords=None,
    catalyst_reference_memory_getter=None,
    catalyst_reference_analysis_confidence_label=None,
    dedupe_clip_list=None,
    youtube_watch_url=None,
    save_catalyst_memory=None,
    catalyst_reference_analysis_max_seconds: float | None = None,
    catalyst_reference_audio_max_seconds: float | None = None,
    normalize_external_source_url=None,
    reference_video_analysis_dir=None,
    build_longform_operator_notes=None,
    manual_catalyst_reference_video_id=None,
    pick_catalyst_reference_video=None,
    pick_reference_preview_frame_urls=None,
    summarize_longform_operator_evidence=None,
    extract_reference_video_stream_clip=None,
    extract_reference_video_full_audit=None,
    extract_reference_preview_frames_from_urls=None,
    xai_json_completion_multimodal=None,
    fetch_source_video_bundle=None,
    fetch_algrow_reference_enrichment=None,
    youtube_fetch_video_analytics_bulk=None,
    download_youtube_video_for_reference_analysis=None,
    audit_manual_comparison_video=None,
    build_catalyst_analysis_proxy_video=None,
    extract_video_metadata=None,
    youtube_fetch_owned_video_bundle_oauth=None,
    extract_audio_from_video=None,
    transcribe_audio_with_grok=None,
) -> None:
    global CATALYST_HUB_SHORT_WORKSPACES
    global CATALYST_HUB_LONGFORM_WORKSPACES
    global CATALYST_REFERENCE_ANALYSIS_DEFAULT_MINUTES
    global CATALYST_REFERENCE_ANALYSIS_MAX_SECONDS
    global CATALYST_REFERENCE_AUDIO_MAX_SECONDS
    global _catalyst_learning_records_getter
    global _catalyst_channel_memory_getter
    global _catalyst_failure_mode_label
    global _clip_text
    global _load_youtube_connections
    global _save_youtube_connections
    global _youtube_bucket_for_user
    global _youtube_sync_and_persist_for_user
    global _youtube_connection_public_view
    global _longform_owner_beta_enabled
    global _is_admin_user
    global _longform_deep_analysis_enabled
    global _active_longform_capacity_session_id
    global _create_longform_session_internal
    global _bool_from_any
    global _normalize_longform_target_minutes
    global _normalize_longform_language
    global _harvest_catalyst_outcomes_for_channel
    global _longform_outcome_sync_semaphore
    global _youtube_ensure_access_token
    global _youtube_fetch_channel_search
    global _longform_sessions_lock
    global _load_longform_sessions
    global _longform_sessions_getter
    global _save_longform_sessions
    global _match_published_video_to_longform_session
    global _youtube_fetch_video_analytics
    global _build_auto_outcome_request
    global _persist_catalyst_outcome_for_session
    global _longform_public_session
    global _title_is_too_close_to_source
    global _shortform_priority_snapshot
    global _youtube_selected_channel_context
    global _youtube_connected_channel_access_token
    global _youtube_fetch_public_channel_page_videos
    global _youtube_apply_public_inventory_to_snapshot
    global _youtube_historical_compare_measured_public_view
    global _youtube_channel_audit_measured_public_view
    global _load_catalyst_memory
    global _catalyst_channel_memory_key
    global _catalyst_channel_memory_public_view
    global _apply_catalyst_public_shorts_playbook_to_channel_memory
    global _persist_public_shorts_playbook_memory
    global _public_shorts_playbook_from_memory_view
    global _reconcile_reference_video_analysis_with_inventory
    global _catalyst_reference_video_analysis_public_view
    global _build_catalyst_reference_video_analysis
    global _resolve_catalyst_series_context
    global _youtube_connections_lock
    global _catalyst_memory_lock
    global _catalyst_hub_workspace_label
    global _xai_json_completion
    global _title_is_too_close_to_any
    global _catalyst_reference_workspace_profile
    global _normalize_catalyst_reference_video_analysis
    global _catalyst_infer_niche
    global _catalyst_infer_archetype
    global _same_arena_title_variants
    global _longform_review_state
    global _heuristic_catalyst_longform_execution_qa
    global _heuristic_catalyst_short_learning_record
    global _update_catalyst_channel_memory
    global _normalize_transition_style
    global _jobs_getter
    global _dedupe_preserve_order
    global _render_catalyst_channel_memory_context
    global _catalyst_rewrite_pressure_profile
    global _build_catalyst_reference_playbook
    global _catalyst_rank_shorts_angle_candidates
    global _build_shorts_public_reference_playbook
    global _build_shorts_trend_query
    global _youtube_fetch_public_trend_titles
    global _youtube_fetch_public_reference_shorts
    global _youtube_title_keywords
    global _catalyst_reference_memory_getter
    global _catalyst_reference_analysis_confidence_label
    global _dedupe_clip_list
    global _youtube_watch_url
    global _save_catalyst_memory
    global _normalize_external_source_url
    global _reference_video_analysis_dir
    global _build_longform_operator_notes
    global _manual_catalyst_reference_video_id
    global _pick_catalyst_reference_video
    global _pick_reference_preview_frame_urls
    global _summarize_longform_operator_evidence
    global _extract_reference_video_stream_clip
    global _extract_reference_video_full_audit
    global _extract_reference_preview_frames_from_urls
    global _xai_json_completion_multimodal
    global _fetch_source_video_bundle
    global _fetch_algrow_reference_enrichment
    global _youtube_fetch_video_analytics_bulk
    global _download_youtube_video_for_reference_analysis
    global _audit_manual_comparison_video
    global _build_catalyst_analysis_proxy_video
    global _extract_video_metadata
    global _youtube_fetch_owned_video_bundle_oauth
    global _extract_audio_from_video
    global _transcribe_audio_with_grok
    if catalyst_hub_short_workspaces is not None:
        CATALYST_HUB_SHORT_WORKSPACES = list(catalyst_hub_short_workspaces)
    if catalyst_hub_longform_workspaces is not None:
        CATALYST_HUB_LONGFORM_WORKSPACES = list(catalyst_hub_longform_workspaces)
    if catalyst_reference_analysis_default_minutes is not None:
        CATALYST_REFERENCE_ANALYSIS_DEFAULT_MINUTES = float(
            catalyst_reference_analysis_default_minutes or CATALYST_REFERENCE_ANALYSIS_DEFAULT_MINUTES
        )
    if catalyst_reference_analysis_max_seconds is not None:
        CATALYST_REFERENCE_ANALYSIS_MAX_SECONDS = float(
            catalyst_reference_analysis_max_seconds or CATALYST_REFERENCE_ANALYSIS_MAX_SECONDS
        )
    if catalyst_reference_audio_max_seconds is not None:
        CATALYST_REFERENCE_AUDIO_MAX_SECONDS = float(
            catalyst_reference_audio_max_seconds or CATALYST_REFERENCE_AUDIO_MAX_SECONDS
        )
    if catalyst_learning_records_getter is not None:
        _catalyst_learning_records_getter = catalyst_learning_records_getter
    if catalyst_channel_memory_getter is not None:
        _catalyst_channel_memory_getter = catalyst_channel_memory_getter
    if catalyst_failure_mode_label is not None:
        _catalyst_failure_mode_label = catalyst_failure_mode_label
    if clip_text is not None:
        _clip_text = clip_text
    if load_youtube_connections is not None:
        _load_youtube_connections = load_youtube_connections
    if save_youtube_connections_runtime is not None:
        _save_youtube_connections = save_youtube_connections_runtime
    if youtube_bucket_for_user is not None:
        _youtube_bucket_for_user = youtube_bucket_for_user
    if youtube_sync_and_persist_for_user is not None:
        _youtube_sync_and_persist_for_user = youtube_sync_and_persist_for_user
    if youtube_connection_public_view is not None:
        _youtube_connection_public_view = youtube_connection_public_view
    if longform_owner_beta_enabled is not None:
        _longform_owner_beta_enabled = longform_owner_beta_enabled
    if is_admin_user is not None:
        _is_admin_user = is_admin_user
    if longform_deep_analysis_enabled is not None:
        _longform_deep_analysis_enabled = longform_deep_analysis_enabled
    if active_longform_capacity_session_id is not None:
        _active_longform_capacity_session_id = active_longform_capacity_session_id
    if create_longform_session_internal is not None:
        _create_longform_session_internal = create_longform_session_internal
    if bool_from_any is not None:
        _bool_from_any = bool_from_any
    if normalize_longform_target_minutes is not None:
        _normalize_longform_target_minutes = normalize_longform_target_minutes
    if normalize_longform_language is not None:
        _normalize_longform_language = normalize_longform_language
    if harvest_catalyst_outcomes_for_channel is not None:
        _harvest_catalyst_outcomes_for_channel = harvest_catalyst_outcomes_for_channel
    if longform_outcome_sync_semaphore is not None:
        _longform_outcome_sync_semaphore = longform_outcome_sync_semaphore
    if youtube_ensure_access_token is not None:
        _youtube_ensure_access_token = youtube_ensure_access_token
    if youtube_fetch_channel_search is not None:
        _youtube_fetch_channel_search = youtube_fetch_channel_search
    if longform_sessions_lock is not None:
        _longform_sessions_lock = longform_sessions_lock
    if load_longform_sessions_runtime is not None:
        _load_longform_sessions = load_longform_sessions_runtime
    if longform_sessions_getter is not None:
        _longform_sessions_getter = longform_sessions_getter
    if save_longform_sessions_runtime is not None:
        _save_longform_sessions = save_longform_sessions_runtime
    if match_published_video_to_longform_session is not None:
        _match_published_video_to_longform_session = match_published_video_to_longform_session
    if youtube_fetch_video_analytics is not None:
        _youtube_fetch_video_analytics = youtube_fetch_video_analytics
    if build_auto_outcome_request is not None:
        _build_auto_outcome_request = build_auto_outcome_request
    if persist_catalyst_outcome_for_session is not None:
        _persist_catalyst_outcome_for_session = persist_catalyst_outcome_for_session
    if longform_public_session is not None:
        _longform_public_session = longform_public_session
    if title_is_too_close_to_source is not None:
        _title_is_too_close_to_source = title_is_too_close_to_source
    if shortform_priority_snapshot is not None:
        _shortform_priority_snapshot = shortform_priority_snapshot
    if youtube_selected_channel_context is not None:
        _youtube_selected_channel_context = youtube_selected_channel_context
    if youtube_connected_channel_access_token is not None:
        _youtube_connected_channel_access_token = youtube_connected_channel_access_token
    if youtube_fetch_public_channel_page_videos is not None:
        _youtube_fetch_public_channel_page_videos = youtube_fetch_public_channel_page_videos
    if youtube_apply_public_inventory_to_snapshot is not None:
        _youtube_apply_public_inventory_to_snapshot = youtube_apply_public_inventory_to_snapshot
    if youtube_historical_compare_measured_public_view is not None:
        _youtube_historical_compare_measured_public_view = youtube_historical_compare_measured_public_view
    if youtube_channel_audit_measured_public_view is not None:
        _youtube_channel_audit_measured_public_view = youtube_channel_audit_measured_public_view
    if load_catalyst_memory is not None:
        _load_catalyst_memory = load_catalyst_memory
    if catalyst_channel_memory_key is not None:
        _catalyst_channel_memory_key = catalyst_channel_memory_key
    if catalyst_channel_memory_public_view is not None:
        _catalyst_channel_memory_public_view = catalyst_channel_memory_public_view
    if apply_catalyst_public_shorts_playbook_to_channel_memory is not None:
        _apply_catalyst_public_shorts_playbook_to_channel_memory = (
            apply_catalyst_public_shorts_playbook_to_channel_memory
        )
    if persist_public_shorts_playbook_memory is not None:
        _persist_public_shorts_playbook_memory = persist_public_shorts_playbook_memory
    if public_shorts_playbook_from_memory_view is not None:
        _public_shorts_playbook_from_memory_view = public_shorts_playbook_from_memory_view
    if reconcile_reference_video_analysis_with_inventory is not None:
        _reconcile_reference_video_analysis_with_inventory = reconcile_reference_video_analysis_with_inventory
    if catalyst_reference_video_analysis_public_view is not None:
        _catalyst_reference_video_analysis_public_view = catalyst_reference_video_analysis_public_view
    if build_catalyst_reference_video_analysis is not None:
        _build_catalyst_reference_video_analysis = build_catalyst_reference_video_analysis
    if resolve_catalyst_series_context is not None:
        _resolve_catalyst_series_context = resolve_catalyst_series_context
    if youtube_connections_lock is not None:
        _youtube_connections_lock = youtube_connections_lock
    if catalyst_memory_lock is not None:
        _catalyst_memory_lock = catalyst_memory_lock
    if catalyst_hub_workspace_label is not None:
        _catalyst_hub_workspace_label = catalyst_hub_workspace_label
    if xai_json_completion is not None:
        _xai_json_completion = xai_json_completion
    if title_is_too_close_to_any is not None:
        _title_is_too_close_to_any = title_is_too_close_to_any
    if catalyst_reference_workspace_profile is not None:
        _catalyst_reference_workspace_profile = catalyst_reference_workspace_profile
    if normalize_catalyst_reference_video_analysis is not None:
        _normalize_catalyst_reference_video_analysis = normalize_catalyst_reference_video_analysis
    if catalyst_infer_niche is not None:
        _catalyst_infer_niche = catalyst_infer_niche
    if catalyst_infer_archetype is not None:
        _catalyst_infer_archetype = catalyst_infer_archetype
    if same_arena_title_variants is not None:
        _same_arena_title_variants = same_arena_title_variants
    if longform_review_state is not None:
        _longform_review_state = longform_review_state
    if heuristic_catalyst_longform_execution_qa is not None:
        _heuristic_catalyst_longform_execution_qa = heuristic_catalyst_longform_execution_qa
    if heuristic_catalyst_short_learning_record is not None:
        _heuristic_catalyst_short_learning_record = heuristic_catalyst_short_learning_record
    if update_catalyst_channel_memory is not None:
        _update_catalyst_channel_memory = update_catalyst_channel_memory
    if normalize_transition_style is not None:
        _normalize_transition_style = normalize_transition_style
    if jobs_getter is not None:
        _jobs_getter = jobs_getter
    if dedupe_preserve_order is not None:
        _dedupe_preserve_order = dedupe_preserve_order
    if render_catalyst_channel_memory_context is not None:
        _render_catalyst_channel_memory_context = render_catalyst_channel_memory_context
    if catalyst_rewrite_pressure_profile is not None:
        _catalyst_rewrite_pressure_profile = catalyst_rewrite_pressure_profile
    if build_catalyst_reference_playbook is not None:
        _build_catalyst_reference_playbook = build_catalyst_reference_playbook
    if catalyst_rank_shorts_angle_candidates is not None:
        _catalyst_rank_shorts_angle_candidates = catalyst_rank_shorts_angle_candidates
    if build_shorts_public_reference_playbook is not None:
        _build_shorts_public_reference_playbook = build_shorts_public_reference_playbook
    if build_shorts_trend_query is not None:
        _build_shorts_trend_query = build_shorts_trend_query
    if youtube_fetch_public_trend_titles is not None:
        _youtube_fetch_public_trend_titles = youtube_fetch_public_trend_titles
    if youtube_fetch_public_reference_shorts is not None:
        _youtube_fetch_public_reference_shorts = youtube_fetch_public_reference_shorts
    if youtube_title_keywords is not None:
        _youtube_title_keywords = youtube_title_keywords
    if catalyst_reference_memory_getter is not None:
        _catalyst_reference_memory_getter = catalyst_reference_memory_getter
    if catalyst_reference_analysis_confidence_label is not None:
        _catalyst_reference_analysis_confidence_label = catalyst_reference_analysis_confidence_label
    if dedupe_clip_list is not None:
        _dedupe_clip_list = dedupe_clip_list
    if youtube_watch_url is not None:
        _youtube_watch_url = youtube_watch_url
    if save_catalyst_memory is not None:
        _save_catalyst_memory = save_catalyst_memory
    if normalize_external_source_url is not None:
        _normalize_external_source_url = normalize_external_source_url
    if reference_video_analysis_dir is not None:
        _reference_video_analysis_dir = reference_video_analysis_dir
    if build_longform_operator_notes is not None:
        _build_longform_operator_notes = build_longform_operator_notes
    if manual_catalyst_reference_video_id is not None:
        _manual_catalyst_reference_video_id = manual_catalyst_reference_video_id
    if pick_catalyst_reference_video is not None:
        _pick_catalyst_reference_video = pick_catalyst_reference_video
    if pick_reference_preview_frame_urls is not None:
        _pick_reference_preview_frame_urls = pick_reference_preview_frame_urls
    if summarize_longform_operator_evidence is not None:
        _summarize_longform_operator_evidence = summarize_longform_operator_evidence
    if extract_reference_video_stream_clip is not None:
        _extract_reference_video_stream_clip = extract_reference_video_stream_clip
    if extract_reference_video_full_audit is not None:
        _extract_reference_video_full_audit = extract_reference_video_full_audit
    if extract_reference_preview_frames_from_urls is not None:
        _extract_reference_preview_frames_from_urls = extract_reference_preview_frames_from_urls
    if xai_json_completion_multimodal is not None:
        _xai_json_completion_multimodal = xai_json_completion_multimodal
    if fetch_source_video_bundle is not None:
        _fetch_source_video_bundle = fetch_source_video_bundle
    if fetch_algrow_reference_enrichment is not None:
        _fetch_algrow_reference_enrichment = fetch_algrow_reference_enrichment
    if youtube_fetch_video_analytics_bulk is not None:
        _youtube_fetch_video_analytics_bulk = youtube_fetch_video_analytics_bulk
    if download_youtube_video_for_reference_analysis is not None:
        _download_youtube_video_for_reference_analysis = download_youtube_video_for_reference_analysis
    if audit_manual_comparison_video is not None:
        _audit_manual_comparison_video = audit_manual_comparison_video
    if build_catalyst_analysis_proxy_video is not None:
        _build_catalyst_analysis_proxy_video = build_catalyst_analysis_proxy_video
    if extract_video_metadata is not None:
        _extract_video_metadata = extract_video_metadata
    if youtube_fetch_owned_video_bundle_oauth is not None:
        _youtube_fetch_owned_video_bundle_oauth = youtube_fetch_owned_video_bundle_oauth
    if extract_audio_from_video is not None:
        _extract_audio_from_video = extract_audio_from_video
    if transcribe_audio_with_grok is not None:
        _transcribe_audio_with_grok = transcribe_audio_with_grok


def _catalyst_hub_workspace_ids_for_scope(scope: str) -> list[str]:
    normalized = re.sub(r"[^a-z_]+", "", str(scope or "").strip().lower())
    if not normalized or normalized == "all":
        return [*CATALYST_HUB_SHORT_WORKSPACES, *CATALYST_HUB_LONGFORM_WORKSPACES]
    if normalized == "shorts":
        return list(CATALYST_HUB_SHORT_WORKSPACES)
    if normalized == "longform":
        return list(CATALYST_HUB_LONGFORM_WORKSPACES)
    if normalized in {*(CATALYST_HUB_SHORT_WORKSPACES), *(CATALYST_HUB_LONGFORM_WORKSPACES)}:
        return [normalized]
    return [*CATALYST_HUB_SHORT_WORKSPACES, *CATALYST_HUB_LONGFORM_WORKSPACES]


def _catalyst_hub_workspace_label(workspace_id: str) -> str:
    return {
        "story": "AI Stories",
        "motivation": "Motivation",
        "skeleton": "Skeleton AI",
        "daytrading": "Day Trading",
        "chatstory": "Chat Story",
        "documentary": "Long Form Documentary",
        "recap": "Long Form Recap",
        "explainer": "Long Form Explainer",
        "story_channel": "Long Form Story Channel",
    }.get(str(workspace_id or "").strip().lower(), str(workspace_id or "").strip() or "Catalyst")


def _catalyst_hub_longform_default_minutes(workspace_id: str) -> float:
    normalized = str(workspace_id or "").strip().lower()
    if normalized == "recap":
        return 60.0
    if normalized == "story_channel":
        return 12.0
    if normalized == "documentary":
        return 10.0
    return 8.0


def _apply_catalyst_operator_directives(
    existing: dict | None,
    *,
    channel_id: str = "",
    format_preset: str = "",
    directive: str = "",
    mission: str = "",
    guardrails: list[str] | None = None,
    target_niches: list[str] | None = None,
    apply_scope: str = "all",
) -> dict:
    updated = dict(existing or {})
    directive_text = _clip_text(str(directive or "").strip(), 2400)
    mission_text = _clip_text(str(mission or "").strip(), 320)
    guardrail_list = _dedupe_preserve_order(
        [str(v).strip() for v in list(guardrails or []) if str(v).strip()],
        max_items=10,
        max_chars=180,
    )
    niche_list = _dedupe_preserve_order(
        [str(v).strip() for v in list(target_niches or []) if str(v).strip()],
        max_items=8,
        max_chars=80,
    )
    summary_bits = [
        mission_text,
        directive_text,
        ("Guardrails: " + "; ".join(guardrail_list[:4])) if guardrail_list else "",
        ("Priority niches: " + ", ".join(niche_list[:5])) if niche_list else "",
    ]
    updated["channel_id"] = str(channel_id or updated.get("channel_id", "") or "").strip()
    updated["format_preset"] = str(format_preset or updated.get("format_preset", "") or "").strip()
    updated["operator_directive"] = directive_text
    updated["operator_mission"] = mission_text
    updated["operator_guardrails"] = guardrail_list
    updated["operator_target_niches"] = niche_list
    updated["operator_apply_scope"] = str(apply_scope or updated.get("operator_apply_scope", "") or "").strip().lower() or "all"
    updated["operator_summary"] = _clip_text(" ".join(bit for bit in summary_bits if bit), 320)
    updated["operator_updated_at"] = time.time()
    updated["updated_at"] = time.time()
    return updated


def _catalyst_reference_workspace_profile(workspace_id: str) -> dict:
    normalized = str(workspace_id or "documentary").strip().lower() or "documentary"
    if normalized == "recap":
        return {
            "lane": "recap",
            "reference_label": "recap reference",
            "objective": "break down the reference recap so Catalyst can rebuild it into a sharper original recap without copying story beats literally.",
            "focus": "Focus on hook clarity, chapter escalation, betrayal and payoff timing, character readability, thumbnail-title promise, and how the recap avoids lore dump drag.",
            "translation": "Translate the winner into a cleaner, more emotional, more mobile-readable recap with stronger chapter pivots, character continuity, and consequence-first framing.",
            "system_role": (
                "You are Catalyst's reference-video analyst for NYPTID Studio. "
                "Analyze the provided winning YouTube recap frames plus metadata, transcript excerpt, and frame metrics. "
                "Be specific about hook timing, chapter escalation, visual readability, transition rhythm, sound design, and how to rebuild this into a premium original recap."
            ),
        }
    if normalized == "explainer":
        return {
            "lane": "explainer",
            "reference_label": "explainer reference",
            "objective": "break down the reference explainer so Catalyst can rebuild it into a sharper original explainer without copying it literally.",
            "focus": "Focus on the opening question, concept clarity, proof cadence, visual simplification, packaging, and how the explainer keeps the viewer oriented through every payoff.",
            "translation": "Translate the winner into a cleaner, more visual, more mobile-readable explainer with faster proof beats and tighter concept resets.",
            "system_role": (
                "You are Catalyst's reference-video analyst for NYPTID Studio. "
                "Analyze the provided winning YouTube explainer frames plus metadata, transcript excerpt, and frame metrics. "
                "Be specific about hook timing, proof cadence, visual clarity, transition rhythm, sound design, and how to rebuild this into a premium original explainer."
            ),
        }
    if normalized == "story_channel":
        return {
            "lane": "story channel",
            "reference_label": "story reference",
            "objective": "break down the reference story video so Catalyst can rebuild it into a sharper original story-driven long-form video without copying it literally.",
            "focus": "Focus on the emotional opening, conflict escalation, character readability, payoff timing, sound tension, and the packaging promise that keeps the audience watching.",
            "translation": "Translate the winner into a cleaner, more cinematic, more emotionally readable story video with stronger scene turns and more deliberate payoff control.",
            "system_role": (
                "You are Catalyst's reference-video analyst for NYPTID Studio. "
                "Analyze the provided winning YouTube story-video frames plus metadata, transcript excerpt, and frame metrics. "
                "Be specific about hook timing, emotional escalation, visual framing, transition rhythm, sound design, and how to rebuild this into a premium original story channel video."
            ),
        }
    return {
        "lane": "documentary",
        "reference_label": "documentary reference",
        "objective": "break down the connected channel's strongest current video so Catalyst can rebuild it as a better fully 3D documentary without copying it literally.",
        "focus": "Focus on what makes the best video work, what the weaker upload is missing, and how to translate the winner into a stronger Fern-grade fully 3D documentary.",
        "translation": "Catalyst should translate that into sharper fully 3D pressure scenes, not static dossier tables or empty archive rooms.",
        "system_role": (
            "You are Catalyst's reference-video analyst for NYPTID Studio. "
            "Analyze the provided winning YouTube documentary frames plus metadata, transcript excerpt, and frame metrics. "
            "Be specific about hook timing, narrative escalation, visual framing, transition rhythm, sound design, and how to rebuild this into a premium fully 3D documentary. "
            "Do not describe generic explainer advice. Output strict JSON only."
        ),
    }


def _catalyst_reference_analysis_confidence_label(
    analysis_mode: str,
    *,
    heuristic_used: bool,
    frame_metrics: dict | None,
    transcript_excerpt: str = "",
    audio_summary: str = "",
) -> str:
    mode = str(analysis_mode or "").strip().lower()
    sampled_frames = int(float((frame_metrics or {}).get("sampled_frames", 0) or 0))
    timeline_frames_analyzed = int(float((frame_metrics or {}).get("timeline_frames_analyzed", 0) or 0))
    full_runtime_covered = bool((frame_metrics or {}).get("full_runtime_covered", False))
    has_text = bool(str(transcript_excerpt or "").strip() or str(audio_summary or "").strip())
    if mode == "direct_media" and full_runtime_covered and timeline_frames_analyzed >= 1000 and has_text and not heuristic_used:
        return "High"
    if mode == "stream_clip" and full_runtime_covered and timeline_frames_analyzed >= 1000 and has_text and not heuristic_used:
        return "Medium-High"
    if mode == "direct_media" and sampled_frames >= 8 and has_text and not heuristic_used:
        return "High"
    if mode == "stream_clip" and sampled_frames >= 6 and not heuristic_used:
        return "Medium-High"
    if mode == "preview_frames" and sampled_frames >= 4:
        return "Medium-Low"
    return "Low"


def _catalyst_recent_learning_records_for_channel(channel_id: str, *, limit: int = 12) -> list[dict]:
    channel_key = str(channel_id or "").strip()
    if not channel_key:
        return []
    rows: list[dict] = []
    for raw in list((_catalyst_learning_records_getter() or {}).values()):
        payload = dict(raw or {})
        if str(payload.get("channel_id", "") or "").strip() != channel_key:
            continue
        rows.append(
            {
                "session_id": str(payload.get("session_id", "") or "").strip(),
                "mode": str(payload.get("mode", "") or "").strip(),
                "format_preset": str(payload.get("format_preset", "") or "").strip(),
                "created_at": float(payload.get("created_at", 0.0) or 0.0),
                "outcome_summary": _clip_text(str(payload.get("outcome_summary", "") or "").strip(), 240),
                "selected_title": _clip_text(str(payload.get("selected_title", "") or "").strip(), 180),
                "last_failure_mode_key": str(payload.get("last_failure_mode_key", "") or "").strip(),
                "last_failure_mode_label": _catalyst_failure_mode_label(str(payload.get("last_failure_mode_key", "") or "").strip()),
                "chapter_score_average": float(payload.get("chapter_score_average", 0.0) or 0.0),
                "preview_success_rate": float(payload.get("preview_success_rate", 0.0) or 0.0),
                "wins_to_keep": list(payload.get("wins_to_keep") or [])[:4],
                "mistakes_to_avoid": list(payload.get("mistakes_to_avoid") or [])[:4],
                "next_video_moves": list(payload.get("next_video_moves") or [])[:5],
            }
        )
    rows.sort(key=lambda row: (-float(row.get("created_at", 0.0) or 0.0), str(row.get("session_id", "") or "").lower()))
    return rows[: max(1, min(int(limit or 12), 24))]


async def _build_catalyst_hub_payload(
    *,
    user: dict,
    channel_id: str = "",
    include_public_benchmarks: bool = False,
    refresh_outcomes: bool = False,
) -> dict:
    user_id = str(user.get("id", "") or "").strip()
    selected_channel_id = str(channel_id or "").strip()
    async with _youtube_connections_lock:
        _load_youtube_connections()
        bucket = _youtube_bucket_for_user(user_id)
        default_channel_id = str(bucket.get("default_channel_id", "") or "").strip()
        channels_map = {str(k): dict(v or {}) for k, v in dict(bucket.get("channels") or {}).items()}
    if (not default_channel_id or default_channel_id not in channels_map) and channels_map:
        default_channel_id = str(next(iter(channels_map.keys())) or "").strip()
    if not selected_channel_id:
        selected_channel_id = default_channel_id
    if selected_channel_id and selected_channel_id not in channels_map:
        selected_channel_id = default_channel_id
    if selected_channel_id and selected_channel_id in channels_map:
        try:
            channels_map[selected_channel_id] = await _youtube_sync_and_persist_for_user(user_id, selected_channel_id)
        except Exception as e:
            stale = dict(channels_map.get(selected_channel_id) or {})
            stale["last_sync_error"] = _clip_text(str(e), 220)
            channels_map[selected_channel_id] = stale
    public_channels = [
        _youtube_connection_public_view(record) if ("analytics_snapshot" in record or "access_token" in record) else dict(record)
        for record in channels_map.values()
    ]
    public_channels.sort(key=lambda row: (0 if str(row.get("channel_id", "") or "").strip() == selected_channel_id else 1, str(row.get("title", "") or "").lower()))
    selected_channel = next((dict(row) for row in public_channels if str(row.get("channel_id", "") or "").strip() == selected_channel_id), {})
    if refresh_outcomes and selected_channel_id and _longform_owner_beta_enabled(user):
        try:
            await _harvest_catalyst_outcomes_for_channel(
                user_id=user_id,
                channel_id=selected_channel_id,
                candidate_limit=18,
                refresh_existing=False,
            )
        except Exception as e:
            refreshed = dict(selected_channel or {})
            refreshed["last_outcome_sync_error"] = _clip_text(str(e), 220)
            selected_channel = refreshed
    channel_context = {}
    if selected_channel_id:
        try:
            channel_context = await _youtube_selected_channel_context(user, selected_channel_id)
        except Exception:
            channel_context = {}
    if selected_channel_id and channel_context:
        try:
            access_token, token_record = await _youtube_connected_channel_access_token(user, selected_channel_id)
            public_page_rows = await _youtube_fetch_public_channel_page_videos(
                access_token,
                channel_url=str((token_record or {}).get("channel_url", "") or str(channel_context.get("channel_url", "") or "")).strip(),
                channel_id=selected_channel_id,
                max_results=max(int(channel_context.get("channel_video_count", 0) or 0), 25),
            )
        except Exception:
            public_page_rows = []
        if public_page_rows:
            current_rows = [
                dict(row or {})
                for row in list(channel_context.get("uploaded_videos") or [])
                if isinstance(row, dict) and str((row or {}).get("video_id", "") or "").strip()
            ]
            current_ids = [str((row or {}).get("video_id", "") or "").strip() for row in current_rows]
            public_ids = [str((row or {}).get("video_id", "") or "").strip() for row in public_page_rows]
            if public_ids and public_ids != current_ids:
                raw_snapshot = _youtube_apply_public_inventory_to_snapshot(
                    {
                        "channel_summary": str(channel_context.get("summary", "") or "").strip(),
                        "channel_video_count": int(channel_context.get("channel_video_count", 0) or 0),
                        "recent_upload_titles": list(channel_context.get("recent_upload_titles") or []),
                        "uploaded_videos": list(channel_context.get("uploaded_videos") or []),
                        "top_video_titles": list(channel_context.get("top_video_titles") or []),
                        "top_videos": list(channel_context.get("top_videos") or []),
                        "title_pattern_hints": list(channel_context.get("title_pattern_hints") or []),
                        "packaging_learnings": list(channel_context.get("packaging_learnings") or []),
                        "retention_learnings": list(channel_context.get("retention_learnings") or []),
                        "series_clusters": list(channel_context.get("series_clusters") or []),
                        "series_cluster_playbook": dict(channel_context.get("series_cluster_playbook") or {}),
                        "historical_compare": dict(channel_context.get("historical_compare") or {}),
                    },
                    public_page_rows,
                )
                channel_context = {
                    **dict(channel_context or {}),
                    "channel_video_count": raw_snapshot["channel_video_count"],
                    "recent_upload_titles": list(raw_snapshot.get("recent_upload_titles") or []),
                    "uploaded_videos": list(raw_snapshot.get("uploaded_videos") or []),
                    "top_video_titles": list(raw_snapshot.get("top_video_titles") or []),
                    "top_videos": list(raw_snapshot.get("top_videos") or []),
                    "historical_compare": dict(raw_snapshot.get("historical_compare") or {}),
                    "channel_audit": dict(raw_snapshot.get("channel_audit") or {}),
                }
    if selected_channel and channel_context and str(channel_context.get("channel_id", "") or "").strip() == selected_channel_id:
        refreshed_snapshot = {
            "channel_video_count": int(channel_context.get("channel_video_count", 0) or 0),
            "recent_upload_titles": list(channel_context.get("recent_upload_titles") or []),
            "uploaded_videos": [
                {
                    "video_id": str((row or {}).get("video_id", "") or "").strip(),
                    "title": _clip_text(str((row or {}).get("title", "") or "").strip(), 180),
                    "published_at": str((row or {}).get("published_at", "") or "").strip(),
                    "thumbnail_url": str((row or {}).get("thumbnail_url", "") or "").strip(),
                    "views": int(float((row or {}).get("views", 0) or 0) or 0),
                    "average_view_percentage": round(float((row or {}).get("average_view_percentage", 0.0) or 0.0), 2),
                    "impression_click_through_rate": round(float((row or {}).get("impression_click_through_rate", 0.0) or 0.0), 2),
                    "duration_sec": int(float((row or {}).get("duration_sec", 0) or 0) or 0),
                    "privacy_status": str((row or {}).get("privacy_status", "") or "").strip(),
                }
                for row in list(channel_context.get("uploaded_videos") or [])[:250]
                if isinstance(row, dict) and str((row or {}).get("video_id", "") or "").strip()
            ],
            "top_video_titles": list(channel_context.get("top_video_titles") or []),
            "historical_compare": _youtube_historical_compare_measured_public_view(
                channel_context.get("historical_compare") or {}
            ),
            "channel_audit": _youtube_channel_audit_measured_public_view(
                channel_context.get("channel_audit") or {}
            ),
        }
        selected_channel["analytics_snapshot"] = refreshed_snapshot
        selected_channel["last_sync_error"] = str(channel_context.get("last_sync_error", "") or "").strip()
        for idx, row in enumerate(list(public_channels or [])):
            if str((row or {}).get("channel_id", "") or "").strip() == selected_channel_id:
                refreshed_row = dict(row or {})
                refreshed_row["analytics_snapshot"] = refreshed_snapshot
                refreshed_row["last_sync_error"] = str(channel_context.get("last_sync_error", "") or "").strip()
                public_channels[idx] = refreshed_row
                break
    workspace_snapshots: dict[str, dict] = {}
    async with _catalyst_memory_lock:
        _load_catalyst_memory()
        memory_store = {str(k): dict(v or {}) for k, v in dict(_catalyst_channel_memory_getter() or {}).items()}
        learning_rows = _catalyst_recent_learning_records_for_channel(selected_channel_id, limit=12)
    for workspace_id in CATALYST_HUB_SHORT_WORKSPACES:
        memory_key = _catalyst_channel_memory_key(user_id, selected_channel_id, workspace_id)
        memory_bucket = dict(memory_store.get(memory_key) or {})
        playbook: dict = {}
        selected_cluster: dict = {}
        if selected_channel_id and include_public_benchmarks:
            try:
                persisted = await _persist_public_shorts_playbook_memory(
                    user=user,
                    template=workspace_id,
                    topic="",
                    preferred_channel_id=selected_channel_id,
                    trend_hunt_enabled=True,
                    channel_context=channel_context,
                )
                playbook = dict(persisted.get("playbook") or {})
                selected_cluster = dict(persisted.get("selected_cluster") or {})
                memory_bucket = dict(memory_store.get(memory_key) or {})
                if not memory_bucket:
                    memory_bucket = dict((_catalyst_channel_memory_getter() or {}).get(memory_key) or {})
            except Exception as e:
                playbook = {"summary": _clip_text(f"Catalyst refresh failed for {workspace_id}: {e}", 220)}
        memory_public = _catalyst_channel_memory_public_view(memory_bucket)
        workspace_snapshots[workspace_id] = {
            "workspace_id": workspace_id,
            "kind": "shorts",
            "memory_key": memory_key,
            "memory_public": memory_public,
            "playbook": playbook or _public_shorts_playbook_from_memory_view(memory_public),
            "selected_cluster": selected_cluster,
        }
    for workspace_id in CATALYST_HUB_LONGFORM_WORKSPACES:
        memory_key = _catalyst_channel_memory_key(user_id, selected_channel_id, workspace_id)
        memory_bucket = dict(memory_store.get(memory_key) or {})
        reconciled_reference_video_analysis = _reconcile_reference_video_analysis_with_inventory(
            memory_bucket.get("reference_video_analysis") or {},
            channel_context,
        )
        memory_bucket["reference_video_analysis"] = reconciled_reference_video_analysis
        reference_video_public = _catalyst_reference_video_analysis_public_view(reconciled_reference_video_analysis)
        reference_analysis_mode = str(
            (dict(reference_video_public.get("evidence") or {}).get("analysis_mode", ""))
            or (dict(reconciled_reference_video_analysis.get("evidence") or {}).get("analysis_mode", ""))
            or ""
        ).strip().lower()
        reference_video_id = str(
            (dict(reference_video_public.get("video") or {}).get("video_id", ""))
            or (dict(reconciled_reference_video_analysis.get("video") or {}).get("video_id", ""))
            or ""
        ).strip()
        if (
            workspace_id == "documentary"
            and selected_channel_id
            and reference_video_id
            and reference_analysis_mode == "preview_frames"
        ):
            try:
                rebuilt_reference_video_analysis = await _build_catalyst_reference_video_analysis(
                    user=user,
                    channel_id=selected_channel_id,
                    workspace_id=workspace_id,
                    video_id=reference_video_id,
                    max_analysis_minutes=CATALYST_REFERENCE_ANALYSIS_DEFAULT_MINUTES,
                )
                memory_bucket["reference_video_analysis"] = dict(rebuilt_reference_video_analysis or {})
                reconciled_reference_video_analysis = dict(rebuilt_reference_video_analysis or {})
                reference_video_public = _catalyst_reference_video_analysis_public_view(reconciled_reference_video_analysis)
            except Exception as e:
                log.warning(f"Catalyst documentary reference auto-rebuild skipped: {e}")
        if not reconciled_reference_video_analysis:
            for stale_key in (
                "reference_summary",
                "last_reference_summary",
                "reference_video_title",
                "reference_video_url",
                "reference_video_id",
            ):
                memory_bucket.pop(stale_key, None)
        series_context = _resolve_catalyst_series_context(
            channel_context,
            channel_memory=memory_bucket,
            topic="",
            source_title="",
            input_title="",
            input_description="",
            format_preset=workspace_id,
        ) if channel_context else {}
        memory_public = dict(series_context.get("memory_view") or _catalyst_channel_memory_public_view(memory_bucket))
        workspace_snapshots[workspace_id] = {
            "workspace_id": workspace_id,
            "kind": "longform",
            "memory_key": memory_key,
            "memory_public": memory_public,
            "selected_cluster": dict(series_context.get("selected_cluster") or {}),
            "cluster_context": _clip_text(str(series_context.get("cluster_context", "") or "").strip(), 320),
            "reference_summary": _clip_text(str((memory_public.get("reference_summary") if isinstance(memory_public, dict) else "") or ""), 320),
            "reference_video_analysis": reference_video_public,
        }
    default_workspace_id = next(
        (
            wid
            for wid in [*CATALYST_HUB_SHORT_WORKSPACES, *CATALYST_HUB_LONGFORM_WORKSPACES]
            if dict(workspace_snapshots.get(wid) or {}).get("memory_public")
        ),
        "skeleton",
    )
    return {
        "ok": True,
        "default_channel_id": default_channel_id,
        "selected_channel_id": selected_channel_id,
        "selected_channel": selected_channel,
        "channels": public_channels,
        "workspace_snapshots": workspace_snapshots,
        "recent_learning": learning_rows,
        "default_workspace_id": default_workspace_id,
        "generated_at": time.time(),
    }


async def _catalyst_hub_snapshot_for_user(
    *,
    user: dict,
    channel_id: str = "",
    refresh: bool = False,
) -> dict:
    if not _is_admin_user(user):
        raise HTTPException(403, "Catalyst hub is owner-only")
    try:
        return await _build_catalyst_hub_payload(
            user=user,
            channel_id=str(channel_id or "").strip(),
            include_public_benchmarks=bool(refresh),
            refresh_outcomes=False,
        )
    except Exception as e:
        log.exception("Catalyst hub snapshot failed")
        raise HTTPException(500, _clip_text(f"Catalyst hub failed to load: {e}", 220))


async def _catalyst_hub_refresh_for_user(
    *,
    user: dict,
    channel_id: str = "",
    include_public_benchmarks: bool = False,
    refresh_outcomes: bool = False,
) -> dict:
    if not _is_admin_user(user):
        raise HTTPException(403, "Catalyst hub is owner-only")
    try:
        return await _build_catalyst_hub_payload(
            user=user,
            channel_id=str(channel_id or "").strip(),
            include_public_benchmarks=bool(include_public_benchmarks),
            refresh_outcomes=bool(refresh_outcomes),
        )
    except Exception as e:
        log.exception("Catalyst hub refresh failed")
        raise HTTPException(500, _clip_text(f"Catalyst hub refresh failed: {e}", 220))


async def _catalyst_hub_reference_video_analysis_for_user(
    *,
    user: dict,
    channel_id: str = "",
    workspace_id: str = "documentary",
    video_id: str = "",
    max_analysis_minutes: float = CATALYST_REFERENCE_ANALYSIS_DEFAULT_MINUTES,
) -> dict:
    if not _is_admin_user(user):
        raise HTTPException(403, "Catalyst hub is owner-only")
    channel_id = str(channel_id or "").strip()
    workspace_id = str(workspace_id or "documentary").strip().lower() or "documentary"
    if not channel_id:
        raise HTTPException(400, "channel_id required")
    if workspace_id not in set(CATALYST_HUB_LONGFORM_WORKSPACES):
        raise HTTPException(400, "Reference video analysis currently supports long-form workspaces only")
    try:
        try:
            await _youtube_sync_and_persist_for_user(str(user.get("id", "") or ""), channel_id)
        except Exception:
            pass
        analysis_result = await _build_catalyst_reference_video_analysis(
            user=user,
            channel_id=channel_id,
            workspace_id=workspace_id,
            video_id=str(video_id or "").strip(),
            max_analysis_minutes=max(
                1.0,
                min(
                    float(max_analysis_minutes or CATALYST_REFERENCE_ANALYSIS_DEFAULT_MINUTES),
                    CATALYST_REFERENCE_ANALYSIS_DEFAULT_MINUTES,
                ),
            ),
        )
        payload = await _build_catalyst_hub_payload(
            user=user,
            channel_id=channel_id,
            include_public_benchmarks=False,
            refresh_outcomes=False,
        )
        return {
            "ok": True,
            "analysis": analysis_result,
            "payload": payload,
        }
    except HTTPException:
        raise
    except Exception as e:
        log.exception("Catalyst reference video analysis failed")
        raise HTTPException(500, _clip_text(f"Catalyst reference analysis failed: {e}", 220))


async def _catalyst_hub_reference_video_analysis_manual_for_user(
    *,
    user: dict,
    channel_id: str = "",
    workspace_id: str = "documentary",
    video_id: str = "",
    max_analysis_minutes: float = CATALYST_REFERENCE_ANALYSIS_DEFAULT_MINUTES,
    reference_source_url: str = "",
    reference_title: str = "",
    reference_channel: str = "",
    analytics_notes: str = "",
    transcript_text: str = "",
    reference_video=None,
    comparison_video=None,
    analytics_images: list | None = None,
    upload_dir: Path | None = None,
) -> dict:
    if not _is_admin_user(user):
        raise HTTPException(403, "Catalyst hub is owner-only")
    channel_id = str(channel_id or "").strip()
    workspace_id = str(workspace_id or "documentary").strip().lower() or "documentary"
    if workspace_id not in set(CATALYST_HUB_LONGFORM_WORKSPACES):
        raise HTTPException(400, "Reference video analysis currently supports long-form workspaces only")
    if not channel_id:
        fallback_channel_context = await _youtube_selected_channel_context(user, preferred_channel_id="")
        channel_id = str((fallback_channel_context or {}).get("channel_id", "") or "").strip()
    if not channel_id:
        raise HTTPException(400, "Select or connect a Catalyst channel before analyzing the manual case file")
    target_dir = Path(upload_dir or (Path(".") / "tmp" / "catalyst_reference_evidence"))
    target_dir.mkdir(parents=True, exist_ok=True)
    saved_image_paths: list[str] = []
    saved_video_path = ""
    saved_video_filename = ""
    saved_comparison_video_path = ""
    saved_comparison_video_filename = ""
    try:
        if reference_video and str(getattr(reference_video, "filename", "") or "").strip():
            saved_video_filename = str(getattr(reference_video, "filename", "") or "").strip()
            video_ext = Path(saved_video_filename).suffix.lower()
            if video_ext not in {".mp4", ".mov", ".m4v", ".webm", ".mkv", ".avi"}:
                video_ext = ".mp4"
            video_path = target_dir / f"catalyst_ref_video_{int(time.time())}_{random.randint(1000, 9999)}{video_ext}"
            with open(video_path, "wb") as fh:
                while chunk := await reference_video.read(1024 * 1024):
                    fh.write(chunk)
            if video_path.exists() and video_path.stat().st_size > 0:
                saved_video_path = str(video_path)
        if comparison_video and str(getattr(comparison_video, "filename", "") or "").strip():
            saved_comparison_video_filename = str(getattr(comparison_video, "filename", "") or "").strip()
            comparison_ext = Path(saved_comparison_video_filename).suffix.lower()
            if comparison_ext not in {".mp4", ".mov", ".m4v", ".webm", ".mkv", ".avi"}:
                comparison_ext = ".mp4"
            comparison_path = target_dir / f"catalyst_cmp_video_{int(time.time())}_{random.randint(1000, 9999)}{comparison_ext}"
            with open(comparison_path, "wb") as fh:
                while chunk := await comparison_video.read(1024 * 1024):
                    fh.write(chunk)
            if comparison_path.exists() and comparison_path.stat().st_size > 0:
                saved_comparison_video_path = str(comparison_path)
        for idx, analytics_image in enumerate(list(analytics_images or [])[:24]):
            filename = str(getattr(analytics_image, "filename", "") or "").strip()
            if not filename:
                continue
            ext = Path(filename).suffix.lower()
            if ext not in {".png", ".jpg", ".jpeg", ".webp"}:
                ext = ".png"
            saved_path = target_dir / f"catalyst_ref_{int(time.time())}_{random.randint(1000, 9999)}_{idx}{ext}"
            with open(saved_path, "wb") as fh:
                while chunk := await analytics_image.read(1024 * 1024):
                    fh.write(chunk)
            if saved_path.exists() and saved_path.stat().st_size > 0:
                saved_image_paths.append(str(saved_path))
        try:
            await _youtube_sync_and_persist_for_user(str(user.get("id", "") or ""), channel_id)
        except Exception:
            pass
        analysis_result = await _build_catalyst_reference_video_analysis(
            user=user,
            channel_id=channel_id,
            workspace_id=workspace_id,
            video_id=str(video_id or "").strip(),
            max_analysis_minutes=max(
                1.0,
                min(float(max_analysis_minutes or CATALYST_REFERENCE_ANALYSIS_DEFAULT_MINUTES), CATALYST_REFERENCE_ANALYSIS_DEFAULT_MINUTES),
            ),
            manual_source_url=str(reference_source_url or "").strip(),
            manual_reference_title=str(reference_title or "").strip(),
            manual_reference_channel=str(reference_channel or "").strip(),
            analytics_notes=str(analytics_notes or "").strip(),
            transcript_text=str(transcript_text or "").strip(),
            analytics_image_paths=saved_image_paths,
            manual_video_path=saved_video_path,
            manual_video_filename=saved_video_filename,
            comparison_video_path=saved_comparison_video_path,
            comparison_video_filename=saved_comparison_video_filename,
        )
        payload = await _build_catalyst_hub_payload(
            user=user,
            channel_id=channel_id,
            include_public_benchmarks=False,
            refresh_outcomes=False,
        )
        return {
            "ok": True,
            "analysis": analysis_result,
            "payload": payload,
        }
    except HTTPException:
        raise
    except Exception as e:
        log.exception("Catalyst manual reference video analysis failed")
        raise HTTPException(500, _clip_text(f"Catalyst manual reference analysis failed: {e}", 220))
    finally:
        for path in saved_image_paths:
            try:
                Path(path).unlink(missing_ok=True)
            except Exception:
                pass
        if saved_video_path:
            try:
                Path(saved_video_path).unlink(missing_ok=True)
            except Exception:
                pass
        if saved_comparison_video_path:
            try:
                Path(saved_comparison_video_path).unlink(missing_ok=True)
            except Exception:
                pass


async def _catalyst_hub_clear_reference_video_analysis_for_user(
    *,
    user: dict,
    channel_id: str = "",
    workspace_id: str = "documentary",
) -> dict:
    if not _is_admin_user(user):
        raise HTTPException(403, "Catalyst hub is owner-only")
    channel_id = str(channel_id or "").strip()
    workspace_id = str(workspace_id or "documentary").strip().lower() or "documentary"
    if not channel_id:
        raise HTTPException(400, "channel_id required")
    if workspace_id not in set(CATALYST_HUB_LONGFORM_WORKSPACES):
        raise HTTPException(400, "Reference video clear currently supports long-form workspaces only")
    try:
        await _clear_catalyst_reference_video_analysis(
            user_id=str(user.get("id", "") or "").strip(),
            channel_id=channel_id,
            workspace_id=workspace_id,
        )
        payload = await _build_catalyst_hub_payload(
            user=user,
            channel_id=channel_id,
            include_public_benchmarks=False,
            refresh_outcomes=False,
        )
        return {
            "ok": True,
            "payload": payload,
        }
    except HTTPException:
        raise
    except Exception as e:
        log.exception("Catalyst reference video clear failed")
        raise HTTPException(500, _clip_text(f"Catalyst reference clear failed: {e}", 220))


async def _catalyst_hub_save_instructions_for_user(
    *,
    user: dict,
    channel_id: str = "",
    directive: str = "",
    mission: str = "",
    guardrails: list[str] | None = None,
    target_niches: list[str] | None = None,
    apply_scope: str = "all",
) -> dict:
    if not _is_admin_user(user):
        raise HTTPException(403, "Catalyst hub is owner-only")
    user_id = str(user.get("id", "") or "").strip()
    channel_id = str(channel_id or "").strip()
    if not channel_id:
        raise HTTPException(400, "channel_id required")
    workspace_ids = _catalyst_hub_workspace_ids_for_scope(str(apply_scope or "").strip())
    async with _catalyst_memory_lock:
        _load_catalyst_memory()
        memory_store = _catalyst_channel_memory_getter()
        if not isinstance(memory_store, dict):
            raise HTTPException(500, "Catalyst memory store unavailable")
        for workspace_id in workspace_ids:
            memory_key = _catalyst_channel_memory_key(user_id, channel_id, workspace_id)
            existing = dict(memory_store.get(memory_key) or {})
            updated = _apply_catalyst_operator_directives(
                existing,
                channel_id=channel_id,
                format_preset=workspace_id,
                directive=str(directive or "").strip(),
                mission=str(mission or "").strip(),
                guardrails=list(guardrails or []),
                target_niches=list(target_niches or []),
                apply_scope=str(apply_scope or "all").strip().lower() or "all",
            )
            updated["key"] = memory_key
            memory_store[memory_key] = updated
        _save_catalyst_memory()
    try:
        return await _build_catalyst_hub_payload(
            user=user,
            channel_id=channel_id,
            include_public_benchmarks=False,
            refresh_outcomes=False,
        )
    except Exception as e:
        log.exception("Catalyst hub instruction save failed")
        raise HTTPException(500, _clip_text(f"Catalyst hub save failed: {e}", 220))


async def _catalyst_hub_launch_longform_for_user(
    *,
    user: dict,
    channel_id: str = "",
    workspace_id: str = "",
    mission: str = "",
    directive: str = "",
    guardrails: list[str] | None = None,
    target_niches: list[str] | None = None,
    include_public_benchmarks: bool = True,
    refresh_outcomes: bool = True,
    target_minutes: float = 0.0,
    language: str = "en",
    animation_enabled: bool = True,
    sfx_enabled: bool = True,
    auto_pipeline: bool = True,
) -> dict:
    if not _is_admin_user(user):
        raise HTTPException(403, "Catalyst hub is owner-only")
    if not _longform_owner_beta_enabled(user):
        raise HTTPException(403, "Long-form owner beta is restricted")
    if not _longform_deep_analysis_enabled(user):
        raise HTTPException(403, "Catalyst long-form deep analysis is not enabled for this account")
    channel_id = str(channel_id or "").strip()
    workspace_id = str(workspace_id or "").strip().lower()
    if not channel_id:
        raise HTTPException(400, "channel_id required")
    if workspace_id not in set(CATALYST_HUB_LONGFORM_WORKSPACES):
        raise HTTPException(400, "Catalyst long-form launch only supports long-form workspaces")
    busy_session_id = await _active_longform_capacity_session_id(str(user.get("id", "") or ""))
    if busy_session_id:
        raise HTTPException(
            409,
            f"Long-form isolated capacity is already busy on session {busy_session_id}. Wait for that run to finish before launching another active Long Form generation.",
        )
    try:
        hub_payload = await _build_catalyst_hub_payload(
            user=user,
            channel_id=channel_id,
            include_public_benchmarks=_bool_from_any(include_public_benchmarks, True),
            refresh_outcomes=_bool_from_any(refresh_outcomes, True),
        )
    except Exception as e:
        log.exception("Catalyst hub long-form launch failed to load hub payload")
        raise HTTPException(500, _clip_text(f"Catalyst launch failed to load channel memory: {e}", 220))
    workspace_snapshot = dict((dict(hub_payload.get("workspace_snapshots") or {})).get(workspace_id) or {})
    memory_public = dict(workspace_snapshot.get("memory_public") or {})
    playbook = dict(workspace_snapshot.get("playbook") or {})
    channel_context = await _youtube_selected_channel_context(user, preferred_channel_id=channel_id)
    if not channel_context:
        raise HTTPException(400, "Connect and sync the YouTube channel before launching Catalyst long-form")
    reference_video_analysis = dict(workspace_snapshot.get("reference_video_analysis") or {})
    if workspace_id == "documentary" and not dict(reference_video_analysis.get("analysis") or {}):
        try:
            reference_video_analysis = await _build_catalyst_reference_video_analysis(
                user=user,
                channel_id=channel_id,
                workspace_id=workspace_id,
                video_id="",
                max_analysis_minutes=CATALYST_REFERENCE_ANALYSIS_DEFAULT_MINUTES,
            )
        except Exception as e:
            log.warning(f"Catalyst documentary reference video analysis auto-build failed: {e}")
            reference_video_analysis = {}
    mission_text = _clip_text(str(mission or memory_public.get("operator_mission", "") or "").strip(), 320)
    directive_text = _clip_text(str(directive or memory_public.get("operator_directive", "") or "").strip(), 2400)
    guardrails_list = _dedupe_preserve_order(
        [str(v).strip() for v in list(guardrails or memory_public.get("operator_guardrails") or []) if str(v).strip()],
        max_items=10,
        max_chars=180,
    )
    target_niches_list = _dedupe_preserve_order(
        [str(v).strip() for v in list(target_niches or memory_public.get("operator_target_niches") or []) if str(v).strip()],
        max_items=8,
        max_chars=80,
    )
    seed = await _derive_longform_seed_from_catalyst_hub(
        workspace_id=workspace_id,
        channel_context=channel_context,
        memory_public=memory_public,
        playbook=playbook,
        reference_video_analysis=reference_video_analysis,
        mission=mission_text,
        directive=directive_text,
        guardrails=guardrails_list,
        target_niches=target_niches_list,
    )
    strategy_lines = [
        f"Catalyst hub launch workspace: {_catalyst_hub_workspace_label(workspace_id)}",
        ("Main goal: " + mission_text) if mission_text else "",
        ("Operator directive: " + directive_text) if directive_text else "",
        ("Guardrails: " + "; ".join(guardrails_list[:6])) if guardrails_list else "",
        ("Priority niches: " + ", ".join(target_niches_list[:6])) if target_niches_list else "",
        ("Channel summary: " + _clip_text(str(channel_context.get("summary", "") or "").strip(), 600)) if channel_context.get("summary") else "",
        ("Channel audit: " + _clip_text(str((channel_context.get("channel_audit") or {}).get("summary", "") or "").strip(), 480)) if (channel_context.get("channel_audit") or {}).get("summary") else "",
        ("Channel audit next moves: " + "; ".join(list((channel_context.get("channel_audit") or {}).get("next_moves") or [])[:4])) if list((channel_context.get("channel_audit") or {}).get("next_moves") or []) else "",
        ("Channel audit candidate titles: " + " | ".join(list((channel_context.get("channel_audit") or {}).get("next_video_candidates") or [])[:4])) if list((channel_context.get("channel_audit") or {}).get("next_video_candidates") or []) else "",
        ("Catalyst memory summary: " + _clip_text(str(memory_public.get("summary", "") or "").strip(), 400)) if memory_public.get("summary") else "",
        ("Catalyst playbook summary: " + _clip_text(str(playbook.get("summary", "") or "").strip(), 400)) if playbook.get("summary") else "",
        ("Reference video summary: " + _clip_text(str((dict(reference_video_analysis.get("analysis") or {})).get("summary", "") or "").strip(), 480)) if (dict(reference_video_analysis.get("analysis") or {})).get("summary") else "",
        ("Reference hook system: " + "; ".join(list((dict(reference_video_analysis.get("analysis") or {})).get("hook_system") or [])[:4])) if list((dict(reference_video_analysis.get("analysis") or {})).get("hook_system") or []) else "",
        ("Reference pacing system: " + "; ".join(list((dict(reference_video_analysis.get("analysis") or {})).get("pacing_system") or [])[:4])) if list((dict(reference_video_analysis.get("analysis") or {})).get("pacing_system") or []) else "",
        ("Reference visual system: " + "; ".join(list((dict(reference_video_analysis.get("analysis") or {})).get("visual_system") or [])[:4])) if list((dict(reference_video_analysis.get("analysis") or {})).get("visual_system") or []) else "",
        ("Reference sound system: " + "; ".join(list((dict(reference_video_analysis.get("analysis") or {})).get("sound_system") or [])[:4])) if list((dict(reference_video_analysis.get("analysis") or {})).get("sound_system") or []) else "",
        ("Reference next moves: " + "; ".join(list((dict(reference_video_analysis.get("analysis") or {})).get("next_video_moves") or [])[:5])) if list((dict(reference_video_analysis.get("analysis") or {})).get("next_video_moves") or []) else "",
        "Launch intent: Build the next strongest long-form video for this channel using connected-channel memory, public benchmark mining, and Catalyst operator guidance.",
    ]
    session_public = await _create_longform_session_internal(
        user=user,
        template="story",
        topic=str(seed.get("topic", "") or "").strip(),
        input_title=str(seed.get("title", "") or "").strip(),
        input_description=str(seed.get("description", "") or "").strip(),
        format_preset=workspace_id,
        source_url="",
        youtube_channel_id=channel_id,
        analytics_notes="",
        strategy_notes=_clip_text("\n".join(line for line in strategy_lines if line), 5000),
        transcript_text="",
        target_minutes=_normalize_longform_target_minutes(
            float(target_minutes or 0.0) or _catalyst_hub_longform_default_minutes(workspace_id)
        ),
        language=_normalize_longform_language(str(language or "en")),
        animation_enabled=_bool_from_any(animation_enabled, True),
        sfx_enabled=_bool_from_any(sfx_enabled, True),
        whisper_mode="cinematic",
        auto_pipeline_requested=_bool_from_any(auto_pipeline, True),
    )
    return {
        "ok": True,
        "workspace_id": workspace_id,
        "seed": seed,
        "session": session_public,
    }


def _longform_session_publish_ready_for_outcome(session: dict) -> bool:
    s = dict(session or {})
    if not str(s.get("youtube_channel_id", "") or "").strip():
        return False
    if str(s.get("status", "") or "").strip().lower() in {"bootstrapping", "draft_generating", "rendering"}:
        return False
    package = dict(s.get("package") or {})
    return bool(
        str(package.get("selected_title", "") or "").strip()
        or str(s.get("input_title", "") or "").strip()
    )


def _longform_session_needs_outcome_refresh(session: dict, refresh_existing: bool = False) -> bool:
    if refresh_existing:
        return True
    s = dict(session or {})
    latest = dict(s.get("latest_outcome") or {})
    if not latest:
        return True
    package = dict(s.get("package") or {})
    selected_title = str(package.get("selected_title", "") or s.get("input_title", "") or "").strip()
    latest_title = str(latest.get("title_used", "") or "").strip()
    if selected_title and latest_title and not _title_is_too_close_to_source(selected_title, latest_title):
        return True
    created_at = float(latest.get("created_at", 0.0) or 0.0)
    if created_at <= 0:
        return True
    age_hours = max(0.0, (time.time() - created_at) / 3600.0)
    latest_metrics = dict(latest.get("metrics") or {})
    views = int(latest_metrics.get("views", 0) or 0)
    if age_hours >= 24 and views < 10000:
        return True
    if age_hours >= 6 and views < 1000:
        return True
    return False


async def _harvest_catalyst_outcomes_for_channel_for_user(
    *,
    user_id: str,
    channel_id: str,
    session_id: str = "",
    candidate_limit: int = 18,
    refresh_existing: bool = False,
) -> dict:
    user_key = str(user_id or "").strip()
    channel_key = str(channel_id or "").strip()
    session_filter = str(session_id or "").strip()
    if not user_key or not channel_key:
        raise HTTPException(400, "user_id and channel_id required")
    candidate_limit = max(6, min(25, int(candidate_limit or 18)))
    if _longform_outcome_sync_semaphore is None:
        raise HTTPException(503, "Long-form outcome sync is unavailable")
    if _longform_sessions_lock is None:
        raise HTTPException(503, "Long-form session store is unavailable")
    async with _longform_outcome_sync_semaphore:
        async with _youtube_connections_lock:
            _load_youtube_connections()
            record = dict((_youtube_bucket_for_user(user_key).get("channels") or {}).get(channel_key) or {})
        if not record:
            raise HTTPException(404, "Connected YouTube channel not found")

        access_token, refreshed = await _youtube_ensure_access_token(record)
        analytics_snapshot = dict(refreshed.get("analytics_snapshot") or {})
        selected_channel_context = {
            "channel_id": channel_key,
            "channel_title": str(refreshed.get("title", "") or "").strip(),
            "channel_handle": str(refreshed.get("channel_handle", "") or "").strip(),
            "channel_url": str(refreshed.get("channel_url", "") or "").strip(),
            "summary": str(analytics_snapshot.get("channel_summary", "") or "").strip(),
            "recent_upload_titles": list(analytics_snapshot.get("recent_upload_titles") or []),
            "top_video_titles": list(analytics_snapshot.get("top_video_titles") or []),
            "top_videos": list(analytics_snapshot.get("top_videos") or []),
            "title_pattern_hints": list(analytics_snapshot.get("title_pattern_hints") or []),
            "packaging_learnings": list(analytics_snapshot.get("packaging_learnings") or []),
            "retention_learnings": list(analytics_snapshot.get("retention_learnings") or []),
            "series_clusters": list(analytics_snapshot.get("series_clusters") or []),
            "series_cluster_playbook": dict(analytics_snapshot.get("series_cluster_playbook") or {}),
            "historical_compare": dict(analytics_snapshot.get("historical_compare") or {}),
            "last_sync_error": str(refreshed.get("last_sync_error", "") or "").strip(),
        }
        recent_candidates = await _youtube_fetch_channel_search(access_token, channel_key, order="date", max_results=candidate_limit)
        popular_candidates = await _youtube_fetch_channel_search(access_token, channel_key, order="viewCount", max_results=candidate_limit)
        deduped_candidates: list[dict] = []
        seen_video_ids: set[str] = set()
        for raw in [*list(recent_candidates or []), *list(popular_candidates or [])]:
            candidate = dict(raw or {})
            video_id = str(candidate.get("video_id", "") or "").strip()
            if not video_id or video_id in seen_video_ids:
                continue
            deduped_candidates.append(candidate)
            seen_video_ids.add(video_id)

        async with _longform_sessions_lock:
            _load_longform_sessions()
            sessions_store = _longform_sessions_getter()
            if not isinstance(sessions_store, dict):
                raise HTTPException(500, "Long-form sessions store unavailable")
            if session_filter:
                session_live = dict(sessions_store.get(session_filter) or {})
                if session_live and str(session_live.get("user_id", "") or "").strip() == user_key:
                    session_live["youtube_channel_id"] = channel_key
                    metadata_pack = dict(session_live.get("metadata_pack") or {})
                    metadata_pack["youtube_channel"] = dict(selected_channel_context)
                    session_live["metadata_pack"] = metadata_pack
                    sessions_store[session_filter] = session_live
                    _save_longform_sessions()
                    sessions = [dict(session_live)]
                else:
                    sessions = []
            else:
                sessions = [
                    dict(value or {})
                    for value in sessions_store.values()
                    if str((value or {}).get("user_id", "") or "").strip() == user_key
                    and str((value or {}).get("youtube_channel_id", "") or "").strip() == channel_key
                ]

        sessions.sort(key=lambda row: float(row.get("updated_at", 0.0) or 0.0), reverse=True)
        synced_sessions: list[dict] = []
        matched_video_ids: list[str] = []
        used_video_ids: set[str] = set()
        scanned_sessions = 0
        sync_error = ""

        for session_snapshot in sessions:
            if not _longform_session_publish_ready_for_outcome(session_snapshot):
                continue
            if not _longform_session_needs_outcome_refresh(session_snapshot, refresh_existing=refresh_existing):
                continue
            scanned_sessions += 1
            available_candidates = [
                dict(candidate or {})
                for candidate in deduped_candidates
                if str((candidate or {}).get("video_id", "") or "").strip() not in used_video_ids
            ]
            matched_video = _match_published_video_to_longform_session(session_snapshot, available_candidates)
            if not matched_video:
                continue
            video_id = str(matched_video.get("video_id", "") or "").strip()
            analytics_metrics = {}
            if video_id:
                try:
                    analytics_metrics = await _youtube_fetch_video_analytics(access_token, channel_key, video_id)
                except Exception as exc:
                    sync_error = _clip_text(str(exc), 220)
                    analytics_metrics = {}
            auto_req = _build_auto_outcome_request(
                session_snapshot=session_snapshot,
                video_meta=matched_video,
                analytics_metrics=analytics_metrics,
            )
            outcome_record, session_live = await _persist_catalyst_outcome_for_session(
                session_id=str(session_snapshot.get("session_id", "") or ""),
                user_id=user_key,
                session=session_snapshot,
                req=auto_req,
                video_meta=matched_video,
                analytics_metrics=analytics_metrics,
            )
            if video_id:
                used_video_ids.add(video_id)
                matched_video_ids.append(video_id)
            synced_sessions.append({
                "session_id": str(session_live.get("session_id", "") or ""),
                "title_used": str(outcome_record.get("title_used", "") or ""),
                "video_id": video_id,
                "video_url": str(outcome_record.get("video_url", "") or ""),
                "views": int((dict(outcome_record.get("metrics") or {})).get("views", 0) or 0),
                "reference_overall_score": round(float((dict((dict(outcome_record.get("reference_comparison") or {})).get("scores") or {})).get("overall", 0.0) or 0.0), 2),
            })

        refreshed["last_outcome_sync_at"] = time.time()
        refreshed["last_outcome_sync_count"] = len(synced_sessions)
        refreshed["last_outcome_sync_error"] = sync_error
        async with _youtube_connections_lock:
            _load_youtube_connections()
            bucket = _youtube_bucket_for_user(user_key)
            bucket["channels"][channel_key] = refreshed
            _save_youtube_connections()

    current_session_public = {}
    if session_filter:
        async with _longform_sessions_lock:
            _load_longform_sessions()
            sessions_store = _longform_sessions_getter()
            session_live = sessions_store.get(session_filter) if isinstance(sessions_store, dict) else None
            if isinstance(session_live, dict) and str(session_live.get("user_id", "") or "").strip() == user_key:
                current_session_public = _longform_public_session(session_live)

    return {
        "ok": True,
        "channel": _youtube_connection_public_view(refreshed),
        "synced_sessions": synced_sessions,
        "synced_count": len(synced_sessions),
        "scanned_sessions": scanned_sessions,
        "candidate_videos": len(deduped_candidates),
        "matched_video_ids": matched_video_ids,
        "session": current_session_public,
    }


async def _maybe_refresh_channel_outcomes_before_longform_run_for_user(
    *,
    user_id: str,
    channel_id: str,
    min_interval_sec: int = 1800,
) -> dict:
    user_key = str(user_id or "").strip()
    channel_key = str(channel_id or "").strip()
    if not user_key or not channel_key:
        return {}
    async with _youtube_connections_lock:
        _load_youtube_connections()
        record = dict((_youtube_bucket_for_user(user_key).get("channels") or {}).get(channel_key) or {})
    if not record:
        return {}
    last_sync_at = float(record.get("last_outcome_sync_at", 0.0) or 0.0)
    last_sync_count = int(record.get("last_outcome_sync_count", 0) or 0)
    sync_age = time.time() - last_sync_at if last_sync_at > 0 else float("inf")
    if sync_age < float(min_interval_sec or 1800) and last_sync_count > 0:
        return {"ok": True, "skipped": True, "reason": "fresh_enough"}
    shortform_priority = (
        await _shortform_priority_snapshot()
        if callable(_shortform_priority_snapshot)
        else {}
    )
    if dict(shortform_priority or {}).get("priority_active"):
        return {
            "ok": True,
            "skipped": True,
            "reason": "shortform_priority_window",
            "shortform_priority": shortform_priority,
        }
    try:
        return await _harvest_catalyst_outcomes_for_channel_for_user(
            user_id=user_key,
            channel_id=channel_key,
            candidate_limit=12,
            refresh_existing=False,
        )
    except Exception as e:
        log.warning(f"[catalyst] automatic channel outcome sync failed before longform run: {e}")
        return {"ok": False, "error": _clip_text(str(e), 220)}


async def _derive_longform_seed_from_catalyst_hub(
    *,
    workspace_id: str,
    channel_context: dict | None = None,
    memory_public: dict | None = None,
    playbook: dict | None = None,
    reference_video_analysis: dict | None = None,
    mission: str = "",
    directive: str = "",
    guardrails: list[str] | None = None,
    target_niches: list[str] | None = None,
) -> dict:
    def _looks_like_operator_goal_text(text: str) -> bool:
        sample = str(text or "").strip().lower()
        if not sample:
            return False
        if len(sample.split()) > 20:
            return True
        if re.search(r"\b(stronger hooks?|better retention|without copying|cleaner systems storytelling|priority niches|guardrails?)\b", sample):
            return True
        return bool(re.match(r"^(make|build|create|study|use|avoid|push|keep|launch|optimize)\b", sample))

    def _sanitize_catalyst_topic_text(candidate: str, fallback: str, focus: str) -> str:
        raw = _clip_text(str(candidate or "").strip(), 180)
        if not raw or _looks_like_operator_goal_text(raw):
            return _clip_text(str(fallback or "").strip() or f"The hidden system behind {focus}", 180)
        cleaned = re.sub(r"\s+", " ", raw).strip(" -,:")
        if _looks_like_operator_goal_text(cleaned):
            return _clip_text(str(fallback or "").strip() or f"The hidden system behind {focus}", 180)
        return cleaned

    workspace_key = str(workspace_id or "documentary").strip().lower()
    workspace_label = _catalyst_hub_workspace_label(workspace_key)
    channel_context = dict(channel_context or {})
    memory_public = dict(memory_public or {})
    playbook = dict(playbook or {})
    reference_video_analysis = dict(reference_video_analysis or {})
    reference_video_payload = dict(reference_video_analysis.get("analysis") or {})
    reference_video_meta = dict(reference_video_analysis.get("video") or {})
    mission_text = _clip_text(str(mission or "").strip(), 320)
    directive_text = _clip_text(str(directive or "").strip(), 2400)
    guardrail_list = [str(v).strip() for v in list(guardrails or []) if str(v).strip()]
    niche_list = [str(v).strip() for v in list(target_niches or []) if str(v).strip()]
    selected_cluster = dict(memory_public.get("selected_cluster") or {})
    series_anchor = _clip_text(
        str(
            memory_public.get("series_anchor")
            or memory_public.get("selected_cluster_label")
            or selected_cluster.get("label")
            or ""
        ).strip(),
        140,
    )
    archetype_label = _clip_text(str(memory_public.get("archetype_label", "") or "").strip(), 120)
    channel_summary = _clip_text(str(channel_context.get("summary", "") or "").strip(), 900)
    recent_titles = [str(v).strip() for v in list(channel_context.get("recent_upload_titles") or []) if str(v).strip()]
    top_titles = [str(v).strip() for v in list(channel_context.get("top_video_titles") or []) if str(v).strip()]
    packaging_learnings = [str(v).strip() for v in list(channel_context.get("packaging_learnings") or []) if str(v).strip()]
    retention_learnings = [str(v).strip() for v in list(channel_context.get("retention_learnings") or []) if str(v).strip()]
    channel_audit = dict(channel_context.get("channel_audit") or {})
    reference_summary = _clip_text(str(playbook.get("summary", "") or memory_public.get("reference_playbook_summary", "") or "").strip(), 600)
    best_moves = [str(v).strip() for v in list(memory_public.get("next_video_moves") or []) if str(v).strip()]
    strongest_signals = [str(v).strip() for v in list(memory_public.get("wins_to_keep") or []) if str(v).strip()]
    weak_points = [str(v).strip() for v in list(memory_public.get("mistakes_to_avoid") or []) if str(v).strip()]
    reference_summary = _clip_text(
        str(reference_video_payload.get("summary", "") or "").strip() or reference_summary,
        600,
    )
    reference_hook_rules = [str(v).strip() for v in list(reference_video_payload.get("hook_system") or []) if str(v).strip()]
    reference_pacing_rules = [str(v).strip() for v in list(reference_video_payload.get("pacing_system") or []) if str(v).strip()]
    reference_visual_rules = [str(v).strip() for v in list(reference_video_payload.get("visual_system") or []) if str(v).strip()]
    reference_sound_rules = [str(v).strip() for v in list(reference_video_payload.get("sound_system") or []) if str(v).strip()]
    reference_transition_rules = [str(v).strip() for v in list(reference_video_payload.get("transition_system") or []) if str(v).strip()]
    reference_structure_map = [str(v).strip() for v in list(reference_video_payload.get("structure_map") or []) if str(v).strip()]
    reference_next_video_moves = [str(v).strip() for v in list(reference_video_payload.get("next_video_moves") or []) if str(v).strip()]
    reference_candidate_titles = [str(v).strip() for v in list(reference_video_payload.get("candidate_titles") or []) if str(v).strip()]
    focus_subject = series_anchor or archetype_label or (niche_list[0] if niche_list else "") or "the strongest winning angle on this channel"
    audit_candidate_titles = [str(v).strip() for v in list(channel_audit.get("next_video_candidates") or []) if str(v).strip()]
    fallback_topic = _clip_text(
        reference_candidate_titles[0]
        if reference_candidate_titles
        else audit_candidate_titles[0]
        if audit_candidate_titles
        else (f"The hidden system behind {focus_subject}" if focus_subject else f"A stronger {workspace_label.lower()}"),
        180,
    )
    fallback_title = _clip_text(
        str(
            memory_public.get("operator_summary")
            or memory_public.get("summary")
            or f"The Hidden System Behind {focus_subject}"
        ).strip(),
        140,
    )
    if _title_is_too_close_to_any(fallback_title, [*recent_titles[:8], *top_titles[:8]]):
        fallback_title = f"Why {focus_subject} Quietly Shapes More Than You Think"
    fallback_description = _clip_text(
        " ".join(
            bit
            for bit in [
                mission_text or f"Build a sharper {workspace_label.lower()} in the same winning arena.",
                directive_text[:320] if directive_text else "",
                f"Stay in the channel's strongest lane around {focus_subject}.",
                ("Guardrails: " + "; ".join(guardrail_list[:4])) if guardrail_list else "",
            ]
            if bit
        ),
        420,
    )
    # Fetch trending signals for the channel's niche
    trending_signals: list[dict] = []
    try:
        from youtube import youtube_fetch_niche_trending_signals
        niche_kws = list(set(
            [str(s).strip() for s in (memory_public.get("niche_keywords") or []) if str(s).strip()]
            + [str(s).strip() for s in (niche_list or []) if str(s).strip()]
        ))[:5]
        if niche_kws:
            trending_signals = await youtube_fetch_niche_trending_signals(niche_kws, max_results=8)
    except Exception as trend_exc:
        log.warning("Trending signal fetch failed (non-fatal): %s", str(trend_exc)[:200])

    trending_prompt_section = ""
    if trending_signals:
        trending_text = "\n".join(f"- {t['title']} (by {t['channel']})" for t in trending_signals[:6])
        trending_prompt_section = f"\n\nTRENDING IN YOUR NICHE RIGHT NOW (use for topic inspiration, not duplication):\n{trending_text}\n"

    try:
        seed_user_prompt = "\n".join(
            [
                f"Workspace: {workspace_label}",
                f"Channel summary: {channel_summary}",
                f"Mission: {mission_text}",
                f"Directive: {directive_text}",
                ("Guardrails: " + "; ".join(guardrail_list[:6])) if guardrail_list else "",
                ("Priority niches: " + ", ".join(niche_list[:6])) if niche_list else "",
                f"Series/arc focus: {series_anchor or 'general'}",
                f"Archetype: {archetype_label or 'general'}",
                ("Top titles: " + " | ".join(top_titles[:6])) if top_titles else "",
                ("Recent titles: " + " | ".join(recent_titles[:6])) if recent_titles else "",
                ("Packaging learnings: " + "; ".join(packaging_learnings[:4])) if packaging_learnings else "",
                ("Retention learnings: " + "; ".join(retention_learnings[:4])) if retention_learnings else "",
                ("Channel audit: " + _clip_text(str(channel_audit.get("summary", "") or "").strip(), 380)) if channel_audit.get("summary") else "",
                ("Audit strengths: " + "; ".join(list(channel_audit.get("strengths") or [])[:3])) if channel_audit.get("strengths") else "",
                ("Audit warnings: " + "; ".join(list(channel_audit.get("warnings") or [])[:3])) if channel_audit.get("warnings") else "",
                ("Audit next moves: " + "; ".join(list(channel_audit.get("next_moves") or [])[:4])) if channel_audit.get("next_moves") else "",
                ("Audit candidate titles: " + " | ".join(list(channel_audit.get("next_video_candidates") or [])[:4])) if channel_audit.get("next_video_candidates") else "",
                ("Reference playbook: " + reference_summary) if reference_summary else "",
                ("Reference video title: " + _clip_text(str(reference_video_meta.get("title", "") or "").strip(), 180)) if reference_video_meta.get("title") else "",
                ("Reference video summary: " + reference_summary) if reference_summary else "",
                ("Reference hook system: " + "; ".join(reference_hook_rules[:4])) if reference_hook_rules else "",
                ("Reference pacing system: " + "; ".join(reference_pacing_rules[:4])) if reference_pacing_rules else "",
                ("Reference visual system: " + "; ".join(reference_visual_rules[:4])) if reference_visual_rules else "",
                ("Reference sound system: " + "; ".join(reference_sound_rules[:4])) if reference_sound_rules else "",
                ("Reference transition system: " + "; ".join(reference_transition_rules[:4])) if reference_transition_rules else "",
                ("Reference structure map: " + "; ".join(reference_structure_map[:5])) if reference_structure_map else "",
                ("Reference next moves: " + "; ".join(reference_next_video_moves[:5])) if reference_next_video_moves else "",
                ("Reference candidate titles: " + " | ".join(reference_candidate_titles[:4])) if reference_candidate_titles else "",
                ("Strongest signals: " + "; ".join(strongest_signals[:4])) if strongest_signals else "",
                ("Weak points: " + "; ".join(weak_points[:4])) if weak_points else "",
                ("Next moves: " + "; ".join(best_moves[:5])) if best_moves else "",
                "Return a fresh next video idea, title, and description for this channel.",
            ]
        )
        if trending_prompt_section:
            seed_user_prompt += trending_prompt_section

        payload = await _xai_json_completion(
            system_prompt=(
                "You create fresh next-video briefs for an automated faceless YouTube engine. "
                "Return strict JSON with keys topic, title, description. "
                "Stay inside the channel's proven arena without copying old titles. "
                "Titles must be clear, clickable, under 110 characters, and not generic."
            ),
            user_prompt=seed_user_prompt,
            temperature=0.35,
            timeout_sec=60,
        )
    except Exception:
        payload = {}
    topic = _sanitize_catalyst_topic_text(str(payload.get("topic", "") or ""), fallback_topic, focus_subject)
    title = _clip_text(str(payload.get("title", "") or fallback_title).strip(), 140)
    description = _clip_text(str(payload.get("description", "") or fallback_description).strip(), 420)
    if not topic:
        topic = fallback_topic
    if not title:
        title = fallback_title
    if _title_is_too_close_to_any(title, [*recent_titles[:8], *top_titles[:8]]):
        title = _clip_text(f"How {focus_subject} Quietly Rewrites the Outcome", 140)
    if not description:
        description = fallback_description
    return {
        "topic": topic,
        "title": title,
        "description": description,
    }


def _heuristic_catalyst_reference_video_analysis(
    *,
    source_bundle: dict | None,
    selected_video: dict | None,
    channel_context: dict | None,
    frame_metrics: dict | None,
    workspace_id: str = "documentary",
) -> dict:
    source_bundle = dict(source_bundle or {})
    selected_video = dict(selected_video or {})
    channel_context = dict(channel_context or {})
    frame_metrics = dict(frame_metrics or {})
    workspace_profile = _catalyst_reference_workspace_profile(workspace_id)
    format_preset = "recap" if str(workspace_id or "").strip().lower() == "recap" else "documentary"
    source_kind = str(selected_video.get("source_kind", "") or "connected_channel").strip().lower() or "connected_channel"
    metrics_origin = {
        "manual_upload": "manual reference files plus any screenshot metrics you supplied",
        "external_url": "public/external reference metadata plus any screenshot metrics you supplied",
        "manual_reference": "manual reference evidence you supplied",
    }.get(source_kind, "connected-channel metrics plus sampled video evidence")
    title = _clip_text(str(selected_video.get("title", "") or source_bundle.get("title", "") or "").strip(), 180)
    description = _clip_text(str(source_bundle.get("description", "") or "").strip(), 700)
    transcript_excerpt = _clip_text(str(source_bundle.get("transcript_excerpt", "") or "").strip(), 1800)
    niche = _catalyst_infer_niche(title, description, transcript_excerpt, format_preset=format_preset)
    archetype = _catalyst_infer_archetype(
        title,
        description,
        transcript_excerpt,
        niche_key=str(niche.get("key", "") or "").strip().lower(),
        format_preset=format_preset,
    )
    avg_motion = float(frame_metrics.get("avg_motion", 0.0) or 0.0)
    cuts_per_minute = float(frame_metrics.get("cuts_per_minute", 0.0) or 0.0)
    hook_motion = float(frame_metrics.get("hook_motion_avg_first_30_sec", 0.0) or 0.0)
    title_focus = re.sub(r"\s*\|.*$", "", title).strip()
    candidate_titles = _same_arena_title_variants(
        {"title": title_focus, "description": description},
        topic=title_focus,
        format_preset=format_preset,
        max_items=5,
    )
    summary_bits = [
        f"Catalyst currently estimates that '{title}' is a strong current {str(workspace_profile.get('reference_label', 'reference video') or 'reference video').strip()} in the {str(archetype.get('label', '') or str(workspace_profile.get('lane', '') or 'channel')).strip()} lane.",
        f"This estimate is based on {metrics_origin}, not on a frame-perfect manual editorial review.",
        str(workspace_profile.get("translation", "") or "").strip(),
    ]
    if cuts_per_minute > 0:
        summary_bits.append(f"Current sampled cut density is about {cuts_per_minute:.1f} cuts per minute, so the next run should preserve consequence-first movement instead of drifting into dead visual air.")
    if hook_motion > 0:
        summary_bits.append(f"Hook motion in the first 30 seconds is about {hook_motion:.1f}, which means the opener needs a fast visual payoff instead of a slow warm-up.")
    if str(workspace_id or "").strip().lower() == "recap":
        why_it_worked = [
            "The title promise likely lands one clean betrayal, revenge, or power-turn hook the viewer understands immediately.",
            "The winning recap lane keeps the viewer oriented around the main character and consequence, not around lore bookkeeping.",
            "Strong recap frames usually make the hierarchy, emotion, and threat readable even on a phone screen.",
            "The opening likely reaches conflict or payoff quickly instead of explaining the whole world first.",
        ]
        what_hurt_weaker_upload = [
            "Slow setup, unclear factions, or lore dumping can kill recap momentum before the audience feels the stakes.",
            "If the thumbnail/title promise and first minute focus on different things, viewers bail before the main payoff arrives.",
            "Reusing the same generic confrontation framing lowers urgency and makes the recap feel templated.",
        ]
        if avg_motion < 6.0:
            what_hurt_weaker_upload.append("Low frame motion in sampled scenes suggests the recap can slip into static exposition instead of visible escalation.")
        hook_system = [
            "Open on the betrayal, power gap, or impossible comeback first, then explain how the story got there.",
            "Name the protagonist, antagonist, and consequence fast so the viewer understands the emotional stakes immediately.",
            "Use the first frame to show dominance, panic, revenge, or humiliation clearly on a phone screen.",
        ]
        pacing_system = [
            "Reset the viewer every chapter: problem, reversal, consequence, new pressure.",
            "Keep explanation attached to visible consequence instead of stacking lore without a payoff beat.",
            "Escalate every few minutes with a stronger betrayal, reveal, rank jump, or revenge step.",
        ]
        visual_system = [
            "Keep character identity, hierarchy, and emotional consequence readable in every key frame.",
            "Use chapter images that show action, betrayal, fear, power, or revenge instead of generic standing poses.",
            "Avoid muddy crowd shots, unreadable lore boards, and filler environments that do not change the stakes.",
        ]
        sound_system = [
            "Use controlled tension beds, payoff hits, and short silence pockets around reveals instead of flat constant music.",
            "Accent betrayals, rank jumps, humiliations, and revenge beats so the audio helps the recap feel decisive.",
            "Keep the narration lane clean and let the music underline emotion, not bury it.",
        ]
        transition_system = [
            "Cut on turning points: betrayal to reaction, reaction to consequence, consequence to comeback.",
            "Use transitions to orient the viewer to the next chapter instead of just decorating the edit.",
            "Avoid soft transitions between similar beats when the story needs a harder escalation.",
        ]
        structure_map = [
            "Hook: show the betrayal, humiliation, or impossible problem immediately.",
            "Context: explain only the minimum backstory needed to understand the pain.",
            "Escalation: stack reversals and power shifts clearly.",
            "Payoff: land the revenge, comeback, or reveal with visible consequence.",
            "Reset: point to the next chapter threat so the recap keeps momentum.",
        ]
        threed_translation_moves = [
            "Translate the winner into cleaner cinematic chapter beats with better character continuity and more readable emotional staging.",
            "Use motion, overlays, and framing to clarify who is winning, who is losing, and why the moment matters.",
            "Build recap visuals around consequence and character status, not around generic fantasy filler.",
        ]
        title_thumbnail_rules = [
            "Keep the title promise focused on one brutal reversal, comeback, betrayal, or power reveal.",
            "Thumbnail direction should show the power gap and emotional consequence instantly.",
            "Avoid overloaded lore phrasing or titles that require prior context to feel urgent.",
        ]
        next_video_moves = [
            "Use the strongest recap as the benchmark, but make the next run clearer, faster, and more emotionally readable.",
            "Tighten the opener so the viewer gets conflict and consequence before lore explanation.",
            "Design each chapter around a visible status change instead of summary filler.",
        ]
        avoid_rules = [
            "No lore dump opener before the conflict is clear.",
            "No generic standing-character frames as the main payoff image.",
            "No confusing name/faction overload without a visible consequence.",
            "No filler chapter transitions that do not raise the stakes.",
        ]
    else:
        why_it_worked = [
            "The title promise is immediate, invasive, and easy to understand at a glance.",
            "The winning lane stays inside one clear arena: hidden psychology and personal manipulation.",
            "Frames feel expensive when they center on people under pressure instead of empty objects or generic mechanism props.",
            "The opening should deliver claim, proof, and personal consequence quickly instead of warming up slowly.",
        ]
        what_hurt_weaker_upload = [
            "Static archive rooms, untouched tables, and generic dossier setups dilute the topic instead of explaining it.",
            "Prop-first staging without a visible human consequence makes the image feel off-topic even when the prompt sounds correct.",
            "Overusing the same room grammar lowers visual score and makes the documentary feel templated instead of engineered.",
        ]
        if avg_motion < 6.0:
            what_hurt_weaker_upload.append("Low frame motion in sampled scenes suggests the video can easily slip into dead visual air if Catalyst leans too hard on static proof frames.")
        hook_system = [
            "Open on a person already under pressure, not an empty room explaining the idea.",
            "Land the contradiction immediately: the viewer sees a wrong decision happening before the narration explains why.",
            "Use one obvious human power imbalance in the first frame so the topic reads instantly on a phone screen.",
        ]
        pacing_system = [
            "Deliver proof every 5 to 10 seconds: claim, visible trigger, leverage, consequence, reset.",
            "Escalate from one personal manipulation moment into a broader system, not from one static room into another static room.",
            "Avoid repeating the same boardroom or archive composition back-to-back.",
        ]
        visual_system = [
            "Use Fern-grade human-scale psychological pressure scenes: executive meetings, interviews, mirrored conversations, surveillance reveals, and consequence rooms.",
            "Keep people, posture, gaze, hierarchy, distance, and timing visible in frame; that is the topic.",
            "Avoid empty dossier rooms, untouched desks, centered props, literal anatomy, and generic machine filler.",
        ]
        sound_system = [
            "Use low-end tension pulses, restrained silence pockets, and precise reveal accents instead of horror-camp textures.",
            "Sound should tighten around the moment status or social pressure flips the decision.",
            "Keep the bed expensive and controlled; do not drown narration in constant noise.",
        ]
        transition_system = [
            "Use consequence resets between beats, not soft dissolves between similar rooms.",
            "Cut from pressure moment to proof frame, then from proof frame to consequence frame.",
            "Every transition should clarify leverage, not just vary the angle.",
        ]
        structure_map = [
            "Hook: invasive decision already happening.",
            "Proof: reveal the hidden pressure or trigger.",
            "Mechanism: explain the manipulation cleanly.",
            "Consequence: show what it costs the victim.",
            "Escalation: widen from one scene to the bigger system.",
            "Payoff: return to the viewer with a sharper practical implication.",
        ]
        threed_translation_moves = [
            "Translate the winner into premium CG with designed pressure scenes, reflections, overlays, and human-scale staging.",
            "Use disciplined lighting and composition, but keep consequence-first cuts between proof beats.",
            "Build the visuals around power dynamics and emotional cost, not around props or anatomy substitutes.",
        ]
        title_thumbnail_rules = [
            "Keep the title promise short, invasive, and personal.",
            "Thumbnail direction should show a human under pressure, one dominant power cue, and one obvious consequence.",
            "Avoid lore chains, textbook phrasing, and generic packaging.",
        ]
        next_video_moves = [
            "Use the strongest reference video as the narrative benchmark, but make the next run more human, more expensive, and more deliberate.",
            "Anchor every chapter opener in a visible pressure moment before explanation.",
            "Replace archive filler with pressure scenes, consequence scenes, and controlled symbolic worlds.",
        ]
        avoid_rules = [
            "No empty archive rooms as the main beat.",
            "No repeated still-life proof boards.",
            "No centered prop pedestal shots.",
            "No literal anatomy fallback unless the narration explicitly demands it.",
        ]
    return _normalize_catalyst_reference_video_analysis(
        {
            "summary": " ".join(summary_bits),
            "honesty_note": f"This reference breakdown is Catalyst inference built from {metrics_origin}, not a direct YouTube label set.",
            "why_it_worked": why_it_worked,
            "what_hurt_weaker_upload": what_hurt_weaker_upload,
            "hook_system": hook_system,
            "pacing_system": pacing_system,
            "visual_system": visual_system,
            "sound_system": sound_system,
            "transition_system": transition_system,
            "structure_map": structure_map,
            "threed_translation_moves": threed_translation_moves,
            "title_thumbnail_rules": title_thumbnail_rules,
            "next_video_moves": next_video_moves,
            "avoid_rules": avoid_rules,
            "candidate_titles": candidate_titles,
        }
    )


def _catalyst_longform_preflight(session: dict) -> dict:
    session_snapshot = dict(session or {})
    edit_blueprint = dict(session_snapshot.get("edit_blueprint") or {})
    package = dict(session_snapshot.get("package") or {})
    metadata_pack = dict(session_snapshot.get("metadata_pack") or {})
    review = _longform_review_state(session_snapshot)
    qa = _heuristic_catalyst_longform_execution_qa(
        session_snapshot=session_snapshot,
        edit_blueprint=edit_blueprint,
        package=package,
    )
    timeline_qa = dict(qa.get("timeline_qa") or {})
    execution_scores = dict(qa.get("execution_scores") or {})
    format_preset = str(session_snapshot.get("format_preset", "") or "documentary").strip().lower()
    preview_success = float(timeline_qa.get("preview_success_rate", 0.0) or 0.0)
    preview_failed = int(timeline_qa.get("preview_failed_count", 0) or 0)
    chapter_balance = float(timeline_qa.get("chapter_balance_score", 0.0) or 0.0)
    documentary_visual_lock = float(timeline_qa.get("documentary_visual_lock_score", 0.0) or 0.0)
    duplicate_visual_hits = int(timeline_qa.get("duplicate_visual_hits", 0) or 0)
    repeated_opening_hits = int(timeline_qa.get("repeated_opening_hits", 0) or 0)
    overall_score = float(execution_scores.get("overall", 0.0) or 0.0)
    hook_score = float(execution_scores.get("hook", 0.0) or 0.0)
    pacing_score = float(execution_scores.get("pacing", 0.0) or 0.0)
    visual_score = float(execution_scores.get("visuals", 0.0) or 0.0)
    sound_score = float(execution_scores.get("sound", 0.0) or 0.0)
    packaging_score = float(execution_scores.get("packaging", 0.0) or 0.0)
    chapters = [dict(chapter or {}) for chapter in list(session_snapshot.get("chapters") or []) if isinstance(chapter, dict)]
    character_references = [
        dict(item or {})
        for item in list(session_snapshot.get("character_references") or [])
        if isinstance(item, dict) and str((item or {}).get("character_id", "") or "").strip()
    ]
    total_scene_count = 0
    assigned_scene_count = 0
    missing_character_assignments = 0
    assigned_character_ids: set[str] = set()
    available_character_ids = {
        str((item or {}).get("character_id", "") or "").strip()
        for item in character_references
        if str((item or {}).get("character_id", "") or "").strip()
    }
    for chapter in chapters:
        for raw_scene in list(chapter.get("scenes") or []):
            scene = dict(raw_scene or {})
            total_scene_count += 1
            assigned_id = str(scene.get("assigned_character_id", "") or "").strip()
            if assigned_id:
                if assigned_id in available_character_ids:
                    assigned_scene_count += 1
                    assigned_character_ids.add(assigned_id)
                else:
                    missing_character_assignments += 1

    blockers: list[str] = []
    warnings: list[str] = []
    strengths = _dedupe_preserve_order(
        [str(v).strip() for v in list(qa.get("wins_to_keep") or []) if str(v).strip()],
        max_items=4,
        max_chars=180,
    )
    next_fixes = _dedupe_preserve_order(
        [str(v).strip() for v in list(qa.get("next_run_moves") or []) if str(v).strip()],
        max_items=5,
        max_chars=180,
    )

    if not review.get("all_approved", False):
        blockers.append(
            f"Approve all chapters before finalize ({review.get('approved_chapters', 0)}/{review.get('total_chapters', 0)} approved)."
        )
    if preview_failed > 0:
        blockers.append(
            f"{preview_failed} scene preview{'s' if preview_failed != 1 else ''} failed and must be regenerated before finalize."
        )
    if 0.0 < preview_success < 85.0:
        blockers.append(f"Preview coverage is only {preview_success:.1f}%. Get hook and payoff visuals ready before final render.")
    elif preview_success < 95.0:
        warnings.append(f"Preview coverage is {preview_success:.1f}%. Finalize is safer once more scene previews are ready.")

    if chapter_balance > 0.0 and chapter_balance < 65.0:
        blockers.append(f"Chapter balance is weak at {chapter_balance:.1f}/100. Tighten pacing before final render.")
    elif chapter_balance > 0.0 and chapter_balance < 78.0:
        warnings.append(f"Chapter balance is only {chapter_balance:.1f}/100. The current pacing may sag between reveals.")

    if format_preset == "documentary":
        if documentary_visual_lock > 0.0 and documentary_visual_lock < 60.0:
            blockers.append(
                f"Documentary visual lock is only {documentary_visual_lock:.1f}/100. The run is drifting away from premium 3D documentary proof frames."
            )
        elif documentary_visual_lock > 0.0 and documentary_visual_lock < 72.0:
            warnings.append(
                f"Documentary visual lock is {documentary_visual_lock:.1f}/100. Push more system, dossier, map, and infrastructure frames."
            )

    if duplicate_visual_hits >= 4:
        blockers.append(
            f"{duplicate_visual_hits} repeated visual frame{'s' if duplicate_visual_hits != 1 else ''} were detected. Reset composition before finalize."
        )
    elif duplicate_visual_hits > 0:
        warnings.append(
            f"{duplicate_visual_hits} repeated visual frame{'s' if duplicate_visual_hits != 1 else ''} were detected across adjacent scenes."
        )

    if repeated_opening_hits >= 2:
        blockers.append(
            f"{repeated_opening_hits} chapters reuse the same opening beat. Increase opening variation before finalize."
        )
    elif repeated_opening_hits > 0:
        warnings.append("Some chapters still reuse opening language. A stronger opener rotation would make the run safer.")

    for label, score in (
        ("hook", hook_score),
        ("pacing", pacing_score),
        ("visual execution", visual_score),
        ("sound design", sound_score),
        ("packaging", packaging_score),
    ):
        if score > 0.0 and score < 60.0:
            blockers.append(f"Catalyst scored {label} at {score:.1f}/100. That is too weak for autonomous finalize.")
        elif score > 0.0 and score < 72.0:
            warnings.append(f"Catalyst scored {label} at {score:.1f}/100. This can still improve before final render.")

    if overall_score > 0.0 and overall_score < 64.0:
        blockers.append(f"Overall execution score is only {overall_score:.1f}/100.")
    elif overall_score > 0.0 and overall_score < 76.0:
        warnings.append(f"Overall execution score is {overall_score:.1f}/100. The run is usable but not yet clean.")

    if character_references:
        if missing_character_assignments > 0:
            blockers.append(
                f"{missing_character_assignments} scene assignment{'s' if missing_character_assignments != 1 else ''} reference missing character IDs. Re-save the cast assignments before finalize."
            )
        if assigned_scene_count == 0:
            warnings.append("Character references are uploaded, but no scenes are explicitly assigned to them yet.")
        elif total_scene_count > 0 and assigned_scene_count < max(2, math.ceil(total_scene_count * 0.2)):
            warnings.append(
                f"Only {assigned_scene_count}/{total_scene_count} scenes have explicit character assignments. Identity continuity may still drift."
            )
        if len(assigned_character_ids) == 1 and total_scene_count > 0 and assigned_scene_count >= max(3, math.ceil(total_scene_count * 0.35)):
            strengths.append("A recurring subject is explicitly assigned across multiple scenes, which gives Catalyst a stronger identity continuity anchor.")

    selected_title = str(
        package.get("selected_title", "")
        or session_snapshot.get("input_title", "")
        or ((metadata_pack.get("title_variants") or [""])[0] if isinstance(metadata_pack.get("title_variants"), list) else "")
        or ""
    ).strip()
    title_variants = [
        str(v).strip()
        for v in list(package.get("title_variants") or metadata_pack.get("title_variants") or [])
        if str(v).strip()
    ]
    if not selected_title and not title_variants:
        warnings.append("Publish package does not yet have a selected title.")
    thumbnail_url = str(package.get("thumbnail_url", "") or "").strip()
    thumbnail_prompt = str(
        package.get("thumbnail_prompt", "")
        or ((metadata_pack.get("thumbnail_prompts") or [""])[0] if isinstance(metadata_pack.get("thumbnail_prompts"), list) else "")
        or ""
    ).strip()
    if not thumbnail_url and not thumbnail_prompt:
        warnings.append("Publish package does not yet have a resolved thumbnail angle.")

    blockers = _dedupe_preserve_order(blockers, max_items=6, max_chars=180)
    warnings = _dedupe_preserve_order(warnings, max_items=6, max_chars=180)
    penalty = len(blockers) * 14 + len(warnings) * 4
    readiness_score = round(max(5.0, min(99.0, (overall_score or 72.0) - penalty)), 2)
    if blockers:
        status = "blocked"
        summary = "Catalyst preflight blocked finalize. Fix the blocking execution issues before rendering."
    elif warnings:
        status = "needs_attention"
        summary = "Catalyst preflight found quality risks. The run can improve before final render."
    else:
        status = "ready"
        summary = "Catalyst preflight says this run is ready for finalize."

    return {
        "status": status,
        "ready_for_finalize": status == "ready",
        "readiness_score": readiness_score,
        "summary": summary,
        "blockers": blockers,
        "warnings": warnings,
        "strengths": strengths,
        "next_fixes": next_fixes,
        "timeline_qa": timeline_qa,
        "execution_scores": execution_scores,
    }


def _build_catalyst_reference_analysis_evidence(
    *,
    analysis_mode: str,
    frame_metrics: dict | None,
    selected_video: dict | None,
    transcript_excerpt: str = "",
    audio_summary: str = "",
    heuristic_used: bool = False,
    algrow_summary: str = "",
) -> dict:
    metrics = dict(frame_metrics or {})
    selected = dict(selected_video or {})
    source_kind = str(selected.get("source_kind", "") or "connected_channel").strip().lower() or "connected_channel"
    metrics_label = {
        "manual_upload": "Manual reference metrics",
        "external_url": "Manual/external reference metrics",
        "manual_reference": "Manual reference metrics",
    }.get(source_kind, "Connected-channel metrics")
    honesty_origin = {
        "manual_upload": "Measured facts below come from manual reference files, manual Studio evidence, public metadata when available, plus Catalyst's sampled-frame/audio extraction.",
        "external_url": "Measured facts below come from external/public reference metadata, manual Studio evidence when supplied, plus Catalyst's sampled-frame/audio extraction.",
        "manual_reference": "Measured facts below come from manual reference evidence plus Catalyst's sampled-frame/audio extraction.",
    }.get(source_kind, "Measured facts below come from connected YouTube metrics when available plus Catalyst's sampled-frame/audio extraction.")
    mode = str(analysis_mode or "unknown").strip().lower()
    full_runtime_covered = bool(metrics.get("full_runtime_covered", False))
    timeline_frames_analyzed = int(float(metrics.get("timeline_frames_analyzed", 0) or 0))
    mode_label = {
        "direct_media": "Direct Video Sample",
        "stream_clip": "Stream Clip Sample",
        "preview_frames": "Preview Frames Only",
        "uploaded_file": "Uploaded Reference File",
    }.get(mode, "Unknown")
    if mode == "direct_media" and full_runtime_covered:
        mode_label = "Direct Full Video Analysis"
    elif mode == "stream_clip" and full_runtime_covered:
        mode_label = "Full Stream Clip Analysis"
    elif mode == "uploaded_file" and full_runtime_covered:
        mode_label = "Uploaded Full Video Analysis"
    sampled_frames = int(float(metrics.get("sampled_frames", 0) or 0))
    analysis_seconds = float(metrics.get("analysis_seconds", 0.0) or 0.0)
    video_duration_sec = float(selected.get("duration_sec", 0.0) or 0.0)
    confidence_label = _catalyst_reference_analysis_confidence_label(
        mode,
        heuristic_used=heuristic_used,
        frame_metrics=metrics,
        transcript_excerpt=transcript_excerpt,
        audio_summary=audio_summary,
    )
    measured_facts = _dedupe_clip_list(
        [
            f"Catalyst analyzed this reference in {mode_label.lower()} mode.",
            (
                f"Catalyst inspected the full runtime across about {timeline_frames_analyzed} frames and exported {sampled_frames} keyframes for visual reasoning."
                if full_runtime_covered and timeline_frames_analyzed > 0
                else f"Sampled about {analysis_seconds:.1f} seconds, inspected {timeline_frames_analyzed or sampled_frames} frames, and exported {sampled_frames} keyframes."
                if analysis_seconds > 0 or sampled_frames > 0 or timeline_frames_analyzed > 0
                else ""
            ),
            (
                f"Measured cut density from the moving-video audit is about {float(metrics.get('cuts_per_minute', 0.0) or 0.0):.1f} cuts per minute."
                if float(metrics.get("cuts_per_minute", 0.0) or 0.0) > 0
                else ""
            ),
            (
                f"{metrics_label} show about {int(float(selected.get('impressions', 0) or 0))} impressions and {float(selected.get('watch_time_hours', 0.0) or 0.0):.1f} watch hours."
                if int(float(selected.get("impressions", 0) or 0)) > 0 or float(selected.get("watch_time_hours", 0.0) or 0.0) > 0
                else ""
            ),
            (
                f"{metrics_label} for this video are {int(float(selected.get('views', 0) or 0))} views, {float(selected.get('average_view_percentage', 0.0) or 0.0):.2f}% average viewed, and {float(selected.get('impression_click_through_rate', 0.0) or 0.0):.2f}% CTR."
                if int(float(selected.get("views", 0) or 0)) > 0
                or float(selected.get("average_view_percentage", 0.0) or 0.0) > 0
                or float(selected.get("impression_click_through_rate", 0.0) or 0.0) > 0
                else ""
            ),
            (
                f"Average view duration is about {int(float(selected.get('average_view_duration_sec', 0) or 0) or 0) // 60}:{int(float(selected.get('average_view_duration_sec', 0) or 0) or 0) % 60:02d}."
                if int(float(selected.get("average_view_duration_sec", 0) or 0) or 0) > 0
                else ""
            ),
            ("Catalyst extracted real audio/transcript context from the moving-video audit." if str(audio_summary or "").strip() else ""),
            ("Catalyst recovered transcript context from public or owned captions." if not str(audio_summary or "").strip() and str(transcript_excerpt or "").strip() else ""),
            ("Catalyst built a lighter analysis proxy of the uploaded reference video before auditing it end-to-end." if bool(metrics.get("proxy_used")) else ""),
            ("Algrow enriched the public reference metadata with extra scrape, thumbnail, and viral analog data." if str(algrow_summary or "").strip() else ""),
        ],
        max_items=6,
    )
    inferred_notes = _dedupe_clip_list(
        [
            "The hook, pacing, visual, sound, transition, and 3D translation systems below are Catalyst interpretations of the sampled material.",
            ("Part of this breakdown used heuristic fallback language." if heuristic_used else ""),
        ],
        max_items=4,
    )
    limitations = _dedupe_clip_list(
        [
            ("YouTube blocked full media download, so Catalyst had to analyze a shorter stream clip instead of the entire file." if mode == "stream_clip" and not full_runtime_covered else ""),
            ("YouTube blocked direct media sampling, so Catalyst had to rely on preview frames instead of full moving video." if mode == "preview_frames" else ""),
            ("No transcript or audio summary was available, so spoken pacing and narration conclusions are weaker." if not str(transcript_excerpt or "").strip() and not str(audio_summary or "").strip() else ""),
            ("CTR is missing or zero for this video, so packaging conclusions are less certain." if float(selected.get("impression_click_through_rate", 0.0) or 0.0) <= 0 else ""),
            ("This analysis uses a sampled window, not the full runtime." if not full_runtime_covered and analysis_seconds > 0 and (video_duration_sec <= 0 or analysis_seconds + 5.0 < video_duration_sec) else ""),
            ("Catalyst downsampled the uploaded reference video before auditing it so large manual uploads stay stable on hosted infrastructure." if bool(metrics.get("proxy_used")) else ""),
        ],
        max_items=6,
    )
    honesty_note = (
        f"{honesty_origin} "
        "The playbook and breakdown text are Catalyst inferences, not direct YouTube labels."
    )
    return {
        "analysis_mode": mode,
        "analysis_mode_label": mode_label,
        "confidence_label": confidence_label,
        "heuristic_used": bool(heuristic_used),
        "measured_facts": measured_facts,
        "inferred_notes": inferred_notes,
        "limitations": limitations,
        "honesty_note": honesty_note,
    }


def _catalyst_reference_video_analysis_public_view(raw: dict | None) -> dict:
    payload = dict(raw or {})
    if not payload:
        return {}
    video = dict(payload.get("video") or {})
    if not video:
        video = {
            "video_id": str(payload.get("video_id", "") or "").strip(),
            "title": _clip_text(str(payload.get("video_title", "") or "").strip(), 180),
            "url": str(payload.get("video_url", "") or "").strip(),
            "source_kind": str(payload.get("source_kind", "") or "").strip(),
            "source_channel": _clip_text(str(payload.get("source_channel", "") or "").strip(), 180),
            "views": int(float(payload.get("views", 0) or 0) or 0),
            "impressions": int(float(payload.get("impressions", 0) or 0) or 0),
            "average_view_duration_sec": int(float(payload.get("average_view_duration_sec", 0) or 0) or 0),
            "average_view_percentage": round(float(payload.get("average_view_percentage", 0.0) or 0.0), 2),
            "impression_click_through_rate": round(float(payload.get("impression_click_through_rate", 0.0) or 0.0), 2),
            "watch_time_hours": round(float(payload.get("watch_time_hours", 0.0) or 0.0), 2),
            "duration_sec": int(float(payload.get("duration_sec", 0) or 0) or 0),
        }
    analysis = _normalize_catalyst_reference_video_analysis(payload.get("analysis") or {})
    evidence = dict(payload.get("evidence") or {})
    if not evidence:
        evidence = _build_catalyst_reference_analysis_evidence(
            analysis_mode=str((payload.get("frame_metrics") or {}).get("analysis_mode", "") or ""),
            frame_metrics=dict(payload.get("frame_metrics") or {}),
            selected_video=video,
            transcript_excerpt=str(payload.get("transcript_excerpt", "") or "").strip(),
            heuristic_used=bool(payload.get("heuristic_used", False)),
        )
    return {
        "video": video,
        "frame_metrics": dict(payload.get("frame_metrics") or {}),
        "analysis": {
            "honesty_note": (
                "This hub view only shows measured reference evidence and explicit limitations. "
                "Catalyst playbook interpretation stays internal."
            ),
            "measured_facts": list(analysis.get("measured_facts") or []),
            "limitations": list(analysis.get("limitations") or []),
            "confidence_label": str(analysis.get("confidence_label", "") or "").strip(),
            "analysis_mode_label": str(analysis.get("analysis_mode_label", "") or "").strip(),
            "manual_evidence_summary": _clip_text(str((payload.get("evidence") or {}).get("manual_evidence_summary", "") or "").strip(), 320),
        },
        "evidence": evidence,
        "analyzed_at": float(payload.get("analyzed_at", 0.0) or 0.0),
    }


def _reconcile_reference_video_analysis_with_inventory(raw: dict | None, channel_context: dict | None) -> dict:
    payload = dict(raw or {})
    context = dict(channel_context or {})
    if not payload:
        return {}
    uploaded_rows = [
        dict(v or {})
        for v in list(context.get("uploaded_videos") or [])
        if isinstance(v, dict) and str((v or {}).get("video_id", "") or "").strip()
    ]
    if not uploaded_rows:
        return payload
    inventory_by_id = {
        str(row.get("video_id", "") or "").strip(): dict(row)
        for row in uploaded_rows
        if str(row.get("video_id", "") or "").strip()
    }
    current_video = dict(payload.get("video") or {})
    reference_video_id = str(
        current_video.get("video_id", "")
        or payload.get("video_id", "")
        or ""
    ).strip()
    if not reference_video_id:
        return payload
    inventory_row = dict(inventory_by_id.get(reference_video_id) or {})
    if not inventory_row:
        return payload
    current_video["video_id"] = reference_video_id
    current_video["title"] = _clip_text(str(inventory_row.get("title", "") or current_video.get("title", "") or "").strip(), 180)
    current_video["url"] = _youtube_watch_url(reference_video_id)
    current_video["duration_sec"] = int(float(inventory_row.get("duration_sec", current_video.get("duration_sec", 0)) or 0) or 0)
    current_video["views"] = int(float(inventory_row.get("views", current_video.get("views", 0)) or 0) or 0)
    current_video["average_view_percentage"] = round(float(inventory_row.get("average_view_percentage", current_video.get("average_view_percentage", 0.0)) or 0.0), 2)
    current_video["impression_click_through_rate"] = round(float(inventory_row.get("impression_click_through_rate", current_video.get("impression_click_through_rate", 0.0)) or 0.0), 2)
    payload["video"] = current_video
    payload["video_id"] = reference_video_id
    payload["video_title"] = str(current_video.get("title", "") or "").strip()
    payload["video_url"] = str(current_video.get("url", "") or "").strip()
    payload["duration_sec"] = int(current_video.get("duration_sec", 0) or 0)
    payload["views"] = int(current_video.get("views", 0) or 0)
    payload["impressions"] = int(current_video.get("impressions", payload.get("impressions", 0)) or 0)
    payload["average_view_percentage"] = float(current_video.get("average_view_percentage", 0.0) or 0.0)
    payload["average_view_duration_sec"] = int(current_video.get("average_view_duration_sec", payload.get("average_view_duration_sec", 0)) or 0)
    payload["impression_click_through_rate"] = float(current_video.get("impression_click_through_rate", 0.0) or 0.0)
    payload["watch_time_hours"] = float(current_video.get("watch_time_hours", payload.get("watch_time_hours", 0.0)) or 0.0)
    return payload


def _apply_manual_operator_evidence_to_reference_video(
    selected_video: dict | None,
    source_bundle: dict | None,
    operator_evidence: dict | None = None,
) -> dict:
    updated = dict(selected_video or {})
    metrics = dict((operator_evidence or {}).get("metrics") or {})
    if not metrics:
        return updated
    duration_sec = int(float(updated.get("duration_sec", (source_bundle or {}).get("duration_sec", 0)) or 0) or 0)
    if int(float(metrics.get("views", 0) or 0) or 0) > 0:
        updated["views"] = int(float(metrics.get("views", 0) or 0) or 0)
    if int(float(metrics.get("impressions", 0) or 0) or 0) > 0:
        updated["impressions"] = int(float(metrics.get("impressions", 0) or 0) or 0)
    if float(metrics.get("ctr", 0.0) or 0.0) > 0:
        updated["impression_click_through_rate"] = round(float(metrics.get("ctr", 0.0) or 0.0), 2)
    if int(float(metrics.get("average_view_duration_sec", 0) or 0) or 0) > 0:
        updated["average_view_duration_sec"] = int(float(metrics.get("average_view_duration_sec", 0) or 0) or 0)
    if float(metrics.get("average_viewed_pct", 0.0) or 0.0) > 0:
        updated["average_view_percentage"] = round(float(metrics.get("average_viewed_pct", 0.0) or 0.0), 2)
    elif int(float(updated.get("average_view_duration_sec", 0) or 0) or 0) > 0 and duration_sec > 0:
        updated["average_view_percentage"] = round((float(updated.get("average_view_duration_sec", 0) or 0.0) / max(duration_sec, 1)) * 100.0, 2)
    if float(metrics.get("watch_time_hours", 0.0) or 0.0) > 0:
        updated["watch_time_hours"] = round(float(metrics.get("watch_time_hours", 0.0) or 0.0), 2)
    return updated


def _merge_operator_evidence_into_reference_analysis(
    *,
    analysis: dict | None,
    evidence: dict | None,
    operator_evidence: dict | None = None,
    analytics_notes: str = "",
    transcript_text: str = "",
    analytics_asset_count: int = 0,
) -> tuple[dict, dict]:
    merged_analysis = _normalize_catalyst_reference_video_analysis(analysis or {})
    merged_evidence = dict(evidence or {})
    operator_evidence = dict(operator_evidence or {})
    if not operator_evidence and not analytics_notes and not transcript_text and int(analytics_asset_count or 0) <= 0:
        return merged_analysis, merged_evidence

    analytics_summary = _clip_text(str(operator_evidence.get("analytics_summary", "") or "").strip(), 320)
    strongest_signals = [str(v).strip() for v in list(operator_evidence.get("strongest_signals") or []) if str(v).strip()]
    weak_points = [str(v).strip() for v in list(operator_evidence.get("weak_points") or []) if str(v).strip()]
    retention_findings = [str(v).strip() for v in list(operator_evidence.get("retention_findings") or []) if str(v).strip()]
    packaging_findings = [str(v).strip() for v in list(operator_evidence.get("packaging_findings") or []) if str(v).strip()]
    improvement_moves = [str(v).strip() for v in list(operator_evidence.get("improvement_moves") or []) if str(v).strip()]

    manual_measured = _dedupe_clip_list(
        [
            (
                f"Manual YouTube Studio evidence was analyzed from {int(analytics_asset_count)} screenshot"
                f"{'' if int(analytics_asset_count) == 1 else 's'}."
                if int(analytics_asset_count or 0) > 0
                else ""
            ),
            ("Manual analytics notes were supplied by the operator." if str(analytics_notes or "").strip() else ""),
            ("Manual transcript/context was supplied by the operator." if str(transcript_text or "").strip() else ""),
            (f"Studio analytics summary: {analytics_summary}" if analytics_summary else ""),
            *strongest_signals[:3],
            *retention_findings[:2],
            *packaging_findings[:2],
        ],
        max_items=8,
    )
    manual_limitations = _dedupe_clip_list(
        [
            *[str(v).strip() for v in list(merged_evidence.get("limitations") or []) if str(v).strip()],
            ("Manual screenshot OCR is approximate and should be treated as operator evidence, not a direct YouTube API export." if int(analytics_asset_count or 0) > 0 else ""),
        ],
        max_items=8,
    )
    merged_evidence["measured_facts"] = _dedupe_clip_list(
        [
            *[str(v).strip() for v in list(merged_evidence.get("measured_facts") or []) if str(v).strip()],
            *manual_measured,
        ],
        max_items=8,
    )
    merged_evidence["limitations"] = manual_limitations
    merged_evidence["manual_evidence_summary"] = analytics_summary
    merged_evidence["manual_asset_count"] = int(analytics_asset_count or 0)
    merged_evidence["manual_transcript_supplied"] = bool(str(transcript_text or "").strip())
    merged_evidence["honesty_note"] = _clip_text(
        "Measured facts below can include manual YouTube Studio screenshots and operator-supplied transcript context when direct Google analytics access is unavailable. "
        + str(merged_evidence.get("honesty_note", "") or ""),
        320,
    )

    merged_analysis["measured_facts"] = _dedupe_preserve_order(
        [
            *[str(v).strip() for v in list(merged_analysis.get("measured_facts") or []) if str(v).strip()],
            *manual_measured,
        ],
        max_items=8,
        max_chars=180,
    )
    merged_analysis["limitations"] = _dedupe_preserve_order(
        [
            *[str(v).strip() for v in list(merged_analysis.get("limitations") or []) if str(v).strip()],
            *manual_limitations,
        ],
        max_items=8,
        max_chars=180,
    )
    merged_analysis["inferred_notes"] = _dedupe_preserve_order(
        [
            *[str(v).strip() for v in list(merged_analysis.get("inferred_notes") or []) if str(v).strip()],
            ("Catalyst merged manual Studio screenshot evidence into this reference breakdown." if manual_measured else ""),
        ],
        max_items=8,
        max_chars=180,
    )
    for key, additions, max_items in [
        ("why_it_worked", strongest_signals, 8),
        ("what_hurt_weaker_upload", weak_points, 8),
        ("pacing_system", retention_findings, 8),
        ("title_thumbnail_rules", packaging_findings, 8),
        ("next_video_moves", improvement_moves, 8),
    ]:
        merged_analysis[key] = _dedupe_preserve_order(
            [
                *[str(v).strip() for v in list(merged_analysis.get(key) or []) if str(v).strip()],
                *[str(v).strip() for v in list(additions or []) if str(v).strip()],
            ],
            max_items=max_items,
            max_chars=180,
        )
    merged_analysis["honesty_note"] = _clip_text(
        "This reference breakdown blends connected-channel data, sampled video evidence, and manual Studio screenshot/operator evidence when available. "
        + str(merged_analysis.get("honesty_note", "") or ""),
        320,
    )
    return merged_analysis, merged_evidence


async def _persist_catalyst_reference_video_analysis(
    *,
    user_id: str,
    channel_id: str,
    workspace_id: str,
    source_bundle: dict,
    selected_video: dict,
    frame_metrics: dict,
    analysis: dict,
    evidence: dict | None = None,
    operator_evidence: dict | None = None,
    analytics_notes: str = "",
    transcript_text: str = "",
    analytics_asset_count: int = 0,
) -> dict:
    memory_key = _catalyst_channel_memory_key(user_id, channel_id, workspace_id)
    async with _catalyst_memory_lock:
        _load_catalyst_memory()
        channel_memory = _catalyst_channel_memory_getter() or {}
        existing = dict(channel_memory.get(memory_key) or {})
        updated = dict(existing)
        summary = _clip_text(str((analysis or {}).get("summary", "") or "").strip(), 420)
        reference_video_payload = {
            "video": {
                "video_id": str(selected_video.get("video_id", "") or source_bundle.get("source_url_video_id", "") or "").strip(),
                "title": _clip_text(str(selected_video.get("title", "") or source_bundle.get("title", "") or "").strip(), 180),
                "url": str(source_bundle.get("source_url", "") or _youtube_watch_url(str(selected_video.get("video_id", "") or ""))).strip(),
                "source_kind": str(selected_video.get("source_kind", "") or "connected_channel").strip() or "connected_channel",
                "source_channel": _clip_text(str(selected_video.get("source_channel", "") or source_bundle.get("channel", "") or "").strip(), 180),
                "duration_sec": int(float(source_bundle.get("duration_sec", selected_video.get("duration_sec", 0)) or 0) or 0),
                "views": int(float(selected_video.get("views", source_bundle.get("view_count", 0)) or 0) or 0),
                "impressions": int(float(selected_video.get("impressions", 0) or 0) or 0),
                "average_view_duration_sec": int(float(selected_video.get("average_view_duration_sec", 0) or 0) or 0),
                "average_view_percentage": round(float(selected_video.get("average_view_percentage", 0.0) or 0.0), 2),
                "impression_click_through_rate": round(float(selected_video.get("impression_click_through_rate", 0.0) or 0.0), 2),
                "watch_time_hours": round(float(selected_video.get("watch_time_hours", 0.0) or 0.0), 2),
            },
            "transcript_excerpt": _clip_text(str(source_bundle.get("transcript_excerpt", "") or "").strip(), 2400),
            "public_summary": _clip_text(str(source_bundle.get("public_summary", "") or "").strip(), 480),
            "frame_metrics": dict(frame_metrics or {}),
            "analysis": dict(analysis or {}),
            "evidence": dict(evidence or {}),
            "operator_evidence": dict(operator_evidence or {}),
            "analytics_notes": _clip_text(str(analytics_notes or "").strip(), 2400),
            "manual_transcript_excerpt": _clip_text(str(transcript_text or "").strip(), 2400),
            "analytics_asset_count": int(analytics_asset_count or 0),
            "analyzed_at": time.time(),
        }
        updated["reference_video_analysis"] = {
            **reference_video_payload,
            "video_id": str(reference_video_payload["video"]["video_id"] or "").strip(),
            "video_title": str(reference_video_payload["video"]["title"] or "").strip(),
            "video_url": str(reference_video_payload["video"]["url"] or "").strip(),
            "duration_sec": int(reference_video_payload["video"]["duration_sec"] or 0),
            "views": int(reference_video_payload["video"]["views"] or 0),
            "source_kind": str(reference_video_payload["video"].get("source_kind", "") or "connected_channel").strip() or "connected_channel",
            "source_channel": str(reference_video_payload["video"].get("source_channel", "") or "").strip(),
            "impressions": int(reference_video_payload["video"].get("impressions", 0) or 0),
            "average_view_percentage": float(reference_video_payload["video"]["average_view_percentage"] or 0.0),
            "average_view_duration_sec": int(reference_video_payload["video"].get("average_view_duration_sec", 0) or 0),
            "impression_click_through_rate": float(reference_video_payload["video"]["impression_click_through_rate"] or 0.0),
            "watch_time_hours": float(reference_video_payload["video"].get("watch_time_hours", 0.0) or 0.0),
        }
        updated["reference_summary"] = summary
        updated["last_reference_summary"] = summary
        updated["reference_video_title"] = _clip_text(str(selected_video.get("title", "") or source_bundle.get("title", "") or "").strip(), 180)
        updated["reference_video_url"] = str(source_bundle.get("source_url", "") or _youtube_watch_url(str(selected_video.get("video_id", "") or ""))).strip()
        updated["reference_video_id"] = str(selected_video.get("video_id", "") or "").strip()
        updated["reference_video_source_kind"] = str(reference_video_payload["video"].get("source_kind", "") or "connected_channel").strip() or "connected_channel"
        updated["reference_video_source_channel"] = str(reference_video_payload["video"].get("source_channel", "") or "").strip()
        updated["reference_hook_rules"] = list(analysis.get("hook_system") or [])
        updated["reference_pacing_rules"] = list(analysis.get("pacing_system") or [])
        updated["reference_visual_rules"] = list(analysis.get("visual_system") or [])
        updated["reference_sound_rules"] = list(analysis.get("sound_system") or [])
        updated["reference_transition_rules"] = list(analysis.get("transition_system") or [])
        updated["reference_structure_map"] = list(analysis.get("structure_map") or [])
        updated["reference_title_thumbnail_rules"] = list(analysis.get("title_thumbnail_rules") or [])
        updated["reference_next_video_moves"] = list(analysis.get("next_video_moves") or [])
        updated["wins_to_keep"] = _dedupe_preserve_order(
            [*list(analysis.get("why_it_worked") or []), *list(existing.get("wins_to_keep") or [])],
            max_items=10,
            max_chars=180,
        )
        updated["mistakes_to_avoid"] = _dedupe_preserve_order(
            [*list(analysis.get("what_hurt_weaker_upload") or []), *list(analysis.get("avoid_rules") or []), *list(existing.get("mistakes_to_avoid") or [])],
            max_items=10,
            max_chars=180,
        )
        updated["next_video_moves"] = _dedupe_preserve_order(
            [*list(analysis.get("next_video_moves") or []), *list(existing.get("next_video_moves") or [])],
            max_items=10,
            max_chars=180,
        )
        updated["updated_at"] = time.time()
        channel_memory[memory_key] = updated
        _save_catalyst_memory()
        return dict(updated)


async def _clear_catalyst_reference_video_analysis(
    *,
    user_id: str,
    channel_id: str,
    workspace_id: str,
) -> dict:
    memory_key = _catalyst_channel_memory_key(user_id, channel_id, workspace_id)
    async with _catalyst_memory_lock:
        _load_catalyst_memory()
        channel_memory = _catalyst_channel_memory_getter() or {}
        existing = dict(channel_memory.get(memory_key) or {})
        if not existing:
            return {}
        updated = dict(existing)
        for stale_key in (
            "reference_video_analysis",
            "reference_summary",
            "last_reference_summary",
            "reference_video_title",
            "reference_video_url",
            "reference_video_id",
            "reference_video_source_kind",
            "reference_video_source_channel",
            "reference_hook_rules",
            "reference_pacing_rules",
            "reference_visual_rules",
            "reference_sound_rules",
            "reference_transition_rules",
            "reference_structure_map",
            "reference_title_thumbnail_rules",
            "reference_next_video_moves",
        ):
            updated.pop(stale_key, None)
        updated["updated_at"] = time.time()
        channel_memory[memory_key] = updated
        _save_catalyst_memory()
        return dict(updated)



def _normalize_catalyst_reference_video_analysis(raw: dict | None) -> dict:
    payload = dict(raw or {})
    list_fields = (
        "why_it_worked",
        "what_hurt_weaker_upload",
        "hook_system",
        "pacing_system",
        "visual_system",
        "sound_system",
        "transition_system",
        "structure_map",
        "threed_translation_moves",
        "title_thumbnail_rules",
        "next_video_moves",
        "avoid_rules",
        "candidate_titles",
    )
    normalized = {
        "summary": _clip_text(str(payload.get("summary", "") or "").strip(), 420),
        "honesty_note": _clip_text(str(payload.get("honesty_note", "") or "").strip(), 320),
        "confidence_label": _clip_text(str(payload.get("confidence_label", "") or "").strip(), 40),
        "analysis_mode_label": _clip_text(str(payload.get("analysis_mode_label", "") or "").strip(), 60),
    }
    for field_name in list_fields:
        normalized[field_name] = _dedupe_preserve_order(
            [str(v).strip() for v in list(payload.get(field_name) or []) if str(v).strip()],
            max_items=8 if field_name != "candidate_titles" else 5,
            max_chars=180 if field_name != "candidate_titles" else 140,
        )
    normalized["measured_facts"] = _dedupe_preserve_order(
        [str(v).strip() for v in list(payload.get("measured_facts") or []) if str(v).strip()],
        max_items=8,
        max_chars=180,
    )
    normalized["inferred_notes"] = _dedupe_preserve_order(
        [str(v).strip() for v in list(payload.get("inferred_notes") or []) if str(v).strip()],
        max_items=8,
        max_chars=180,
    )
    normalized["limitations"] = _dedupe_preserve_order(
        [str(v).strip() for v in list(payload.get("limitations") or []) if str(v).strip()],
        max_items=8,
        max_chars=180,
    )
    return normalized



async def _build_catalyst_reference_video_analysis(
    *,
    user: dict,
    channel_id: str,
    workspace_id: str,
    video_id: str = "",
    max_analysis_minutes: float = CATALYST_REFERENCE_ANALYSIS_DEFAULT_MINUTES,
    manual_source_url: str = "",
    manual_reference_title: str = "",
    manual_reference_channel: str = "",
    analytics_notes: str = "",
    transcript_text: str = "",
    analytics_image_paths: list[str] | None = None,
    manual_video_path: str = "",
    manual_video_filename: str = "",
    comparison_video_path: str = "",
    comparison_video_filename: str = "",
) -> dict:
    channel_id = str(channel_id or "").strip()
    workspace_id = str(workspace_id or "documentary").strip().lower() or "documentary"
    channel_context = await _youtube_selected_channel_context(user, preferred_channel_id=channel_id)
    if not channel_context:
        raise HTTPException(400, "Connect and sync the YouTube channel before analyzing a reference video")
    channel_id = str(channel_context.get("channel_id", "") or channel_id).strip()
    await _clear_catalyst_reference_video_analysis(
        user_id=str(user.get("id", "") or "").strip(),
        channel_id=channel_id,
        workspace_id=workspace_id,
    )
    oauth_access_token, _oauth_channel_record = await _youtube_connected_channel_access_token(user, channel_id)
    workspace_profile = _catalyst_reference_workspace_profile(workspace_id)
    manual_source_url = _normalize_external_source_url(manual_source_url)
    manual_reference_title = _clip_text(str(manual_reference_title or "").strip(), 220)
    manual_reference_channel = _clip_text(str(manual_reference_channel or "").strip(), 180)
    manual_video_path = str(manual_video_path or "").strip()
    manual_video_filename = str(manual_video_filename or "").strip()
    comparison_video_path = str(comparison_video_path or "").strip()
    comparison_video_filename = str(comparison_video_filename or "").strip()
    manual_reference_mode = bool(manual_source_url or manual_reference_title or manual_reference_channel or manual_video_path)
    if manual_reference_mode:
        selected_video_id = _manual_catalyst_reference_video_id(
            manual_source_url,
            manual_reference_title,
            manual_video_filename,
        )
        fallback_title = manual_reference_title or Path(manual_video_filename or manual_video_path or "manual-reference").stem.replace("_", " ").strip()
        selected_video = {
            "video_id": selected_video_id,
            "title": _clip_text(fallback_title, 180),
            "url": manual_source_url,
            "source_kind": "manual_upload" if manual_video_path else ("external_url" if manual_source_url else "manual_reference"),
            "source_channel": manual_reference_channel,
        }
        source_url = manual_source_url
    else:
        selected_video = _pick_catalyst_reference_video(channel_context, requested_video_id=video_id)
        if not selected_video:
            raise HTTPException(404, "No connected-channel top video was available to analyze")
        selected_video = dict(selected_video or {})
        selected_video["source_kind"] = "connected_channel"
        selected_video["source_channel"] = _clip_text(str(channel_context.get("title", "") or "").strip(), 180)
        selected_video_id = str(selected_video.get("video_id", "") or "").strip()
        source_url = _youtube_watch_url(selected_video_id)
        if not source_url:
            raise HTTPException(404, "Reference video is missing a usable YouTube video id")

    work_dir = _reference_video_analysis_dir(
        str(user.get("id", "") or "").strip(),
        channel_id,
        workspace_id,
        selected_video_id,
    )
    source_bundle = {}
    if manual_reference_mode:
        if manual_source_url:
            try:
                source_bundle = await _fetch_source_video_bundle(manual_source_url, language="en")
            except Exception:
                source_bundle = {}
        if not source_bundle:
            source_bundle = {"source_url": manual_source_url}
        if manual_reference_title:
            source_bundle["title"] = manual_reference_title
        if manual_reference_channel:
            source_bundle["channel"] = manual_reference_channel
        if manual_video_path and Path(manual_video_path).exists():
            try:
                upload_meta = await extract_video_metadata(manual_video_path)
            except Exception:
                upload_meta = {}
            if upload_meta:
                if int(float(upload_meta.get("duration_sec", 0) or 0) or 0) > 0:
                    source_bundle["duration_sec"] = int(float(upload_meta.get("duration_sec", 0) or 0) or 0)
                source_bundle["uploaded_video_meta"] = dict(upload_meta)
        source_bundle["source_url"] = manual_source_url
        source_bundle["source_url_video_id"] = selected_video_id
    else:
        if oauth_access_token:
            try:
                source_bundle = await _youtube_fetch_owned_video_bundle_oauth(
                    oauth_access_token,
                    selected_video_id,
                    language="en",
                )
            except Exception:
                source_bundle = {}
        fallback_bundle = {}
        if source_url and (not source_bundle or not str(source_bundle.get("transcript_excerpt", "") or "").strip()):
            fallback_bundle = await _fetch_source_video_bundle(source_url, language="en")
        if source_bundle and fallback_bundle:
            merged_source_bundle = dict(fallback_bundle)
            merged_source_bundle.update({k: v for k, v in dict(source_bundle).items() if v not in (None, "", [], {})})
            if str(source_bundle.get("transcript_excerpt", "") or "").strip():
                merged_source_bundle["transcript_excerpt"] = str(source_bundle.get("transcript_excerpt", "") or "").strip()
            source_bundle = merged_source_bundle
        elif fallback_bundle:
            source_bundle = dict(fallback_bundle)
        if not source_bundle:
            source_bundle = {"source_url": source_url}
        source_bundle["source_url"] = source_url
        source_bundle["source_url_video_id"] = selected_video_id
    algrow_enrichment = {}
    if source_url:
        try:
            algrow_enrichment = await _fetch_algrow_reference_enrichment(
                source_url=source_url,
                source_bundle=source_bundle,
                workspace_id=workspace_id,
            )
        except Exception as e:
            algrow_enrichment = {"errors": [_clip_text(str(e), 220)]}
    algrow_source_video = dict((algrow_enrichment or {}).get("source_video") or {})
    if algrow_source_video:
        if not str(source_bundle.get("title", "") or "").strip() and str(algrow_source_video.get("title", "") or "").strip():
            source_bundle["title"] = _clip_text(str(algrow_source_video.get("title", "") or "").strip(), 220)
        if not str(source_bundle.get("channel", "") or "").strip() and str(algrow_source_video.get("channel", "") or "").strip():
            source_bundle["channel"] = _clip_text(str(algrow_source_video.get("channel", "") or "").strip(), 180)
        if not str(source_bundle.get("thumbnail_url", "") or "").strip() and str(algrow_source_video.get("thumbnail_url", "") or "").strip():
            source_bundle["thumbnail_url"] = str(algrow_source_video.get("thumbnail_url", "") or "").strip()
        if int(float(source_bundle.get("duration_sec", 0) or 0) or 0) <= 0 and int(float(algrow_source_video.get("duration_sec", 0) or 0) or 0) > 0:
            source_bundle["duration_sec"] = int(float(algrow_source_video.get("duration_sec", 0) or 0) or 0)
        if not str(source_bundle.get("upload_date", "") or "").strip() and str(algrow_source_video.get("upload_date", "") or "").strip():
            source_bundle["upload_date"] = str(algrow_source_video.get("upload_date", "") or "").strip()
        if not str(source_bundle.get("transcript_excerpt", "") or "").strip() and str(algrow_source_video.get("transcript_excerpt", "") or "").strip():
            source_bundle["transcript_excerpt"] = _clip_text(str(algrow_source_video.get("transcript_excerpt", "") or "").strip(), 4000)
        selected_video["views"] = max(
            int(float(selected_video.get("views", 0) or 0) or 0),
            int(float(algrow_source_video.get("view_count", 0) or 0) or 0),
        )
        selected_video["duration_sec"] = max(
            int(float(selected_video.get("duration_sec", 0) or 0) or 0),
            int(float(algrow_source_video.get("duration_sec", 0) or 0) or 0),
        )
    if algrow_enrichment:
        source_bundle["algrow_enrichment"] = dict(algrow_enrichment)
        algrow_summary = _clip_text(str(algrow_enrichment.get("summary", "") or "").strip(), 700)
        if algrow_summary:
            source_bundle["public_summary"] = " | ".join(
                part
                for part in [
                    str(source_bundle.get("public_summary", "") or "").strip(),
                    algrow_summary,
                ]
                if part
            )
    if transcript_text:
        source_bundle["manual_transcript_excerpt"] = _clip_text(transcript_text, 12000)
        if not str(source_bundle.get("transcript_excerpt", "") or "").strip():
            source_bundle["transcript_excerpt"] = _clip_text(transcript_text, 12000)
    if not str(source_bundle.get("title", "") or "").strip() and str(selected_video.get("title", "") or "").strip():
        source_bundle["title"] = _clip_text(str(selected_video.get("title", "") or "").strip(), 180)
    if not str(source_bundle.get("channel", "") or "").strip() and str(selected_video.get("source_channel", "") or "").strip():
        source_bundle["channel"] = _clip_text(str(selected_video.get("source_channel", "") or "").strip(), 180)
    if oauth_access_token and selected_video_id and not manual_reference_mode:
        selected_video_meta = dict(selected_video or {})
        published_hint = str(
            selected_video_meta.get("published_at", "")
            or source_bundle.get("upload_date", "")
            or ""
        ).strip()
        if published_hint:
            selected_video_meta["published_at"] = published_hint
        try:
            refreshed_video_metrics = await _youtube_fetch_video_analytics_bulk(
                oauth_access_token,
                channel_context.get("channel_id", channel_id),
                [selected_video_id],
                video_meta={selected_video_id: selected_video_meta},
            )
        except Exception:
            refreshed_video_metrics = {}
        metric_row = dict(refreshed_video_metrics.get(selected_video_id) or {})
        if metric_row:
            selected_video.update(
                {
                    "views": int(metric_row.get("views", selected_video.get("views", 0) or 0) or 0),
                    "average_view_duration_sec": int(metric_row.get("averageViewDuration", selected_video.get("average_view_duration_sec", 0) or 0) or 0),
                    "average_view_percentage": round(float(metric_row.get("averageViewPercentage", selected_video.get("average_view_percentage", 0.0) or 0.0) or 0.0), 2),
                    "impressions": int(metric_row.get("impressions", selected_video.get("impressions", 0) or 0) or 0),
                    "impression_click_through_rate": round(float(metric_row.get("impressionClickThroughRate", selected_video.get("impression_click_through_rate", 0.0) or 0.0) or 0.0), 2),
                }
            )

    analytics_image_paths = [str(path).strip() for path in list(analytics_image_paths or []) if str(path).strip()]
    operator_evidence = await _summarize_longform_operator_evidence(
        transcript_text=transcript_text,
        image_paths=analytics_image_paths,
        source_bundle=source_bundle,
    ) if analytics_image_paths or transcript_text else {}
    selected_video = _apply_manual_operator_evidence_to_reference_video(
        selected_video,
        source_bundle,
        operator_evidence,
    )
    operator_notes = _build_longform_operator_notes(
        analytics_notes=analytics_notes,
        transcript_text=transcript_text,
        operator_evidence=operator_evidence,
    )

    download_info = {}
    download_error = ""
    video_path = ""
    if manual_video_path and Path(manual_video_path).exists():
        video_path = manual_video_path
    elif source_url:
        download = await _download_youtube_video_for_reference_analysis(source_url, work_dir / "video")
        download_info = dict(download.get("info") or {})
        download_error = _clip_text(str(download.get("download_error", "") or "").strip(), 420)
        video_path = str(download.get("video_path", "") or "").strip()
    analysis_seconds = max(
        60.0,
        min(float(max_analysis_minutes or CATALYST_REFERENCE_ANALYSIS_DEFAULT_MINUTES) * 60.0, CATALYST_REFERENCE_ANALYSIS_MAX_SECONDS),
    )
    selected_duration_sec = float(
        source_bundle.get("duration_sec", selected_video.get("duration_sec", 0.0)) or 0.0
    )
    if selected_duration_sec > 0:
        analysis_seconds = min(analysis_seconds, selected_duration_sec)
    comparison_video_audit = {}
    if comparison_video_path and Path(comparison_video_path).exists():
        try:
            comparison_video_audit = await _audit_manual_comparison_video(
                comparison_video_path,
                filename=comparison_video_filename,
                output_dir=work_dir / "comparison_video",
                max_seconds=analysis_seconds,
            )
        except Exception as e:
            comparison_video_audit = {
                "summary": _clip_text(f"Uploaded comparison video audit failed: {e}", 220),
            }
    audio_summary = ""
    analysis_mode = "uploaded_file" if video_path and manual_video_path else "direct_media"
    heuristic_used = False
    upload_proxy_info = {}
    stream_info = dict(download_info or source_bundle.get("_yt_dlp_info") or {})
    if not video_path and stream_info:
        stream_clip = await _extract_reference_video_stream_clip(
            stream_info,
            work_dir / "video",
            max_seconds=analysis_seconds,
        )
        stream_error = _clip_text(str(stream_clip.get("error", "") or "").strip(), 420)
        if str(stream_clip.get("video_path", "") or "").strip():
            video_path = str(stream_clip.get("video_path", "") or "").strip()
            analysis_mode = "stream_clip"
            if download_error:
                download_error = " | ".join(
                    part for part in [
                        download_error,
                        "Direct media download was blocked, but Catalyst recovered by clipping the reference video from extracted stream URLs.",
                    ] if part
                )
            elif not download_info and source_bundle.get("_yt_dlp_info"):
                download_error = "Catalyst recovered moving-video analysis from earlier yt-dlp metadata after the direct download step failed."
        elif stream_error:
            download_error = " | ".join(part for part in [download_error, stream_error] if part)
    audio_path = ""
    if video_path:
        analysis_video_path = video_path
        if manual_video_path and Path(manual_video_path).exists():
            upload_proxy_info = await _build_catalyst_analysis_proxy_video(
                video_path,
                work_dir / "video_proxy",
                max_seconds=analysis_seconds,
            )
            analysis_video_path = str(upload_proxy_info.get("video_path", "") or "").strip() or video_path
        try:
            audio_path = await extract_audio_from_video(
                analysis_video_path,
                max_seconds=min(analysis_seconds, CATALYST_REFERENCE_AUDIO_MAX_SECONDS),
            ) or ""
            audio_summary = await transcribe_audio_with_grok(audio_path) if audio_path else ""
        finally:
            if audio_path:
                try:
                    Path(audio_path).unlink(missing_ok=True)
                except Exception:
                    pass
        frame_pack = await _extract_reference_video_full_audit(
            analysis_video_path,
            work_dir / "frames",
            max_keyframes=14,
            max_seconds=analysis_seconds,
        )
    else:
        analysis_mode = "preview_frames"
        preview_urls = _pick_reference_preview_frame_urls(source_bundle, download_info, max_items=6)
        frame_pack = await _extract_reference_preview_frames_from_urls(
            preview_urls,
            work_dir / "frames",
            duration_sec=float(source_bundle.get("duration_sec", selected_video.get("duration_sec", 0)) or 0.0),
            max_seconds=analysis_seconds,
        )
        transcript_excerpt = _clip_text(str(source_bundle.get("transcript_excerpt", "") or "").strip(), 1200)
        if transcript_excerpt:
            audio_summary = f"Reference analysis used public captions excerpt because direct media download was blocked. {transcript_excerpt}"
        if download_error:
            source_bundle["public_summary"] = " | ".join(
                part
                for part in [
                    str(source_bundle.get("public_summary", "") or "").strip(),
                    "Direct media download was blocked by YouTube, so Catalyst switched to preview-frame analysis.",
                ]
                if part
            )
    frame_paths = [str(v).strip() for v in list(frame_pack.get("frame_paths") or []) if str(v).strip()]
    frame_metrics = dict(frame_pack.get("metrics") or {})
    frame_metrics["analysis_mode"] = analysis_mode
    if selected_duration_sec > 0:
        frame_metrics["original_duration_sec"] = round(selected_duration_sec, 2)
        if analysis_seconds + 1.0 < selected_duration_sec:
            frame_metrics["full_runtime_covered"] = False
    if upload_proxy_info:
        frame_metrics["proxy_used"] = bool(upload_proxy_info.get("proxy_used"))
        if int(float(upload_proxy_info.get("source_size_bytes", 0) or 0) or 0) > 0:
            frame_metrics["source_size_bytes"] = int(float(upload_proxy_info.get("source_size_bytes", 0) or 0) or 0)
        if int(float(upload_proxy_info.get("proxy_size_bytes", 0) or 0) or 0) > 0:
            frame_metrics["proxy_size_bytes"] = int(float(upload_proxy_info.get("proxy_size_bytes", 0) or 0) or 0)
        proxy_error = str(upload_proxy_info.get("error", "") or "").strip()
        if proxy_error:
            frame_metrics["proxy_error"] = _clip_text(proxy_error, 220)
    if download_error:
        frame_metrics["download_error"] = download_error
    historical_compare = dict(channel_context.get("historical_compare") or {})
    latest_video = dict(historical_compare.get("latest_video") or {})
    previous_video = dict(historical_compare.get("previous_video") or {})
    worst_recent_video = dict(historical_compare.get("worst_recent_video") or {})
    channel_audit = dict(channel_context.get("channel_audit") or {})
    algrow_thumbnail_matches = [dict(v or {}) for v in list((algrow_enrichment or {}).get("thumbnail_matches") or []) if isinstance(v, dict)]
    algrow_viral_matches = [dict(v or {}) for v in list((algrow_enrichment or {}).get("viral_matches") or []) if isinstance(v, dict)]
    algrow_errors = [str(v).strip() for v in list((algrow_enrichment or {}).get("errors") or []) if str(v).strip()]
    algrow_summary = _clip_text(str((algrow_enrichment or {}).get("summary", "") or "").strip(), 900)

    analysis_prompt = "\n".join(
        part for part in [
            f"Reference video objective: {str(workspace_profile.get('objective', '') or '').strip()}",
            f"Channel summary: {_clip_text(str(channel_context.get('summary', '') or '').strip(), 700)}",
            ("Channel audit: " + _clip_text(str(channel_audit.get("summary", "") or "").strip(), 520)) if channel_audit.get("summary") else "",
            ("Channel audit strengths: " + "; ".join(list(channel_audit.get("strengths") or [])[:4])) if channel_audit.get("strengths") else "",
            ("Channel audit warnings: " + "; ".join(list(channel_audit.get("warnings") or [])[:4])) if channel_audit.get("warnings") else "",
            f"Top reference video title: {_clip_text(str(selected_video.get('title', '') or source_bundle.get('title', '') or '').strip(), 220)}",
            f"Top reference video URL: {source_url}",
            f"Top reference video public summary: {_clip_text(str(source_bundle.get('public_summary', '') or '').strip(), 700)}",
            f"Top reference video transcript excerpt: {_clip_text(str(source_bundle.get('transcript_excerpt', '') or '').strip(), 2800)}",
            (
                "Reference analysis mode: uploaded reference file + full audio extraction."
                if analysis_mode == "uploaded_file"
                else "Reference analysis mode: preview frames + public metadata fallback because direct media download was blocked."
                if analysis_mode == "preview_frames"
                else "Reference analysis mode: stream-clipped moving video + full audio extraction from yt-dlp stream URLs because direct download was blocked."
                if analysis_mode == "stream_clip"
                else "Reference analysis mode: direct full-video audit + full audio extraction."
            ),
            ("Audio summary: " + audio_summary) if audio_summary else "",
            ("Frame metrics: " + json.dumps(frame_metrics, ensure_ascii=True)) if frame_metrics else "",
            ("Manual operator evidence: " + _clip_text(operator_notes, 2200)) if operator_notes else "",
            (f"Reference source channel: {_clip_text(str(source_bundle.get('channel', '') or selected_video.get('source_channel', '') or '').strip(), 220)}" if str(source_bundle.get("channel", "") or selected_video.get("source_channel", "") or "").strip() else ""),
            ("Uploaded comparison/current video: " + _clip_text(str(comparison_video_audit.get("summary", "") or "").strip(), 1200)) if comparison_video_audit else "",
            ("Uploaded comparison transcript excerpt: " + _clip_text(str(comparison_video_audit.get("transcript_excerpt", "") or "").strip(), 2600)) if str(comparison_video_audit.get("transcript_excerpt", "") or "").strip() else "",
            ("Uploaded comparison frame metrics: " + json.dumps(dict(comparison_video_audit.get("frame_metrics") or {}), ensure_ascii=True)) if dict(comparison_video_audit.get("frame_metrics") or {}) else "",
            ("Algrow enrichment summary: " + algrow_summary) if algrow_summary else "",
            (
                "Algrow thumbnail analogs: "
                + "; ".join(
                    _clip_text(
                        f"{str(row.get('title', '') or '').strip()} by {str(row.get('channel_name', '') or '').strip()} ({int(float(row.get('view_count', 0) or 0) or 0)} views, similarity {int(float(row.get('similarity_score', 0) or 0) or 0)})",
                        180,
                    )
                    for row in algrow_thumbnail_matches[:4]
                )
                if algrow_thumbnail_matches
                else ""
            ),
            (
                "Algrow viral analogs: "
                + "; ".join(
                    _clip_text(
                        f"{str(row.get('title', '') or '').strip()} by {str(row.get('channel_name', '') or '').strip()} ({int(float(row.get('view_count', 0) or 0) or 0)} views)",
                        180,
                    )
                    for row in algrow_viral_matches[:4]
                )
                if algrow_viral_matches
                else ""
            ),
            ("Algrow limitations: " + "; ".join(algrow_errors[:4])) if algrow_errors else "",
            ("Latest weak/current upload: " + _clip_text(str(latest_video.get("title", "") or "").strip(), 220)) if latest_video else "",
            ("Previous upload: " + _clip_text(str(previous_video.get("title", "") or "").strip(), 220)) if previous_video else "",
            ("Weakest recent package: " + _clip_text(str(worst_recent_video.get("title", "") or "").strip(), 220)) if worst_recent_video else "",
            "Return strict JSON with keys: summary, why_it_worked, what_hurt_weaker_upload, hook_system, pacing_system, visual_system, sound_system, transition_system, structure_map, threed_translation_moves, title_thumbnail_rules, next_video_moves, avoid_rules, candidate_titles.",
            str(workspace_profile.get("focus", "") or "").strip(),
        ]
        if part
    )
    try:
        raw_analysis = await _xai_json_completion_multimodal(
            system_prompt=str(workspace_profile.get("system_role", "") or "").strip(),
            user_prompt=analysis_prompt,
            image_paths=frame_paths[:14],
            temperature=0.25,
            timeout_sec=180,
            model="grok-4",
        )
        analysis = _normalize_catalyst_reference_video_analysis(raw_analysis)
    except Exception as e:
        log.warning(f"Catalyst reference video multimodal analysis fell back to heuristic mode: {e}")
        heuristic_used = True
        analysis = _heuristic_catalyst_reference_video_analysis(
            source_bundle=source_bundle,
            selected_video=selected_video,
            channel_context=channel_context,
            frame_metrics=frame_metrics,
            workspace_id=workspace_id,
        )
    evidence = _build_catalyst_reference_analysis_evidence(
        analysis_mode=analysis_mode,
        frame_metrics=frame_metrics,
        selected_video=selected_video,
        transcript_excerpt=str(source_bundle.get("transcript_excerpt", "") or "").strip(),
        audio_summary=audio_summary,
        heuristic_used=heuristic_used,
        algrow_summary=algrow_summary,
    )
    analysis, evidence = _merge_operator_evidence_into_reference_analysis(
        analysis=analysis,
        evidence=evidence,
        operator_evidence=operator_evidence,
        analytics_notes=analytics_notes,
        transcript_text=transcript_text,
        analytics_asset_count=len(analytics_image_paths),
    )
    updated_memory = await _persist_catalyst_reference_video_analysis(
        user_id=str(user.get("id", "") or "").strip(),
        channel_id=channel_id,
        workspace_id=workspace_id,
        source_bundle=source_bundle,
        selected_video=selected_video,
        frame_metrics=frame_metrics,
        analysis=analysis,
        evidence=evidence,
        operator_evidence=operator_evidence,
        analytics_notes=analytics_notes,
        transcript_text=transcript_text,
        analytics_asset_count=len(analytics_image_paths),
    )
    return {
        "video": {
            "video_id": selected_video_id,
            "title": _clip_text(str(selected_video.get("title", "") or source_bundle.get("title", "") or "").strip(), 180),
            "url": source_url,
            "source_kind": str(selected_video.get("source_kind", "") or "connected_channel").strip() or "connected_channel",
            "source_channel": _clip_text(str(selected_video.get("source_channel", "") or source_bundle.get("channel", "") or "").strip(), 180),
            "views": int(float(selected_video.get("views", source_bundle.get("view_count", 0)) or 0) or 0),
            "impressions": int(float(selected_video.get("impressions", 0) or 0) or 0),
            "average_view_duration_sec": int(float(selected_video.get("average_view_duration_sec", 0) or 0) or 0),
            "average_view_percentage": round(float(selected_video.get("average_view_percentage", 0.0) or 0.0), 2),
            "impression_click_through_rate": round(float(selected_video.get("impression_click_through_rate", 0.0) or 0.0), 2),
            "watch_time_hours": round(float(selected_video.get("watch_time_hours", 0.0) or 0.0), 2),
            "duration_sec": int(float(source_bundle.get("duration_sec", selected_video.get("duration_sec", 0)) or 0) or 0),
        },
        "frame_metrics": frame_metrics,
        "analysis": analysis,
        "evidence": evidence,
        "frame_paths": frame_paths[:14],
        "memory": _catalyst_channel_memory_public_view(updated_memory),
    }


def _build_shorts_trend_query(
    template: str,
    topic: str,
    channel_context: dict,
    selected_cluster: dict,
) -> str:
    raw_topic = re.sub(r"\s+", " ", str(topic or "").strip())
    if raw_topic:
        return raw_topic
    cluster_keywords = [str(v).strip() for v in list(selected_cluster.get("keywords") or []) if str(v).strip()]
    channel_titles = [str(v).strip() for v in list(channel_context.get("recent_upload_titles") or []) if str(v).strip()]
    if cluster_keywords:
        if str(template or "").strip().lower() == "skeleton":
            return " ".join(cluster_keywords[:3] + ["comparison"])
        return " ".join(cluster_keywords[:3])
    if channel_titles:
        seed = re.sub(r"\s*\|.*$", "", channel_titles[0]).strip()
        return _clip_text(seed, 120)
    if str(template or "").strip().lower() == "skeleton":
        return "viral skeleton comparison shorts"
    if str(template or "").strip().lower() == "daytrading":
        return "day trading mistakes psychology shorts"
    return "viral youtube shorts ideas"


async def _build_shorts_catalyst_extra_instructions(
    user: dict | None,
    template: str,
    preferred_channel_id: str = "",
    topic: str = "",
    trend_hunt_enabled: bool = False,
) -> str:
    if not isinstance(user, dict):
        return ""
    try:
        channel_context = await _youtube_selected_channel_context(user, preferred_channel_id)
    except Exception as e:
        log.warning(f"Catalyst shorts context failed for template={template}: {e}")
        return ""
    channel_id = str(channel_context.get("channel_id", "") or "").strip()
    if not channel_id:
        return ""
    memory_key = _catalyst_channel_memory_key(
        str(user.get("id", "") or ""),
        channel_id,
        str(template or "story").strip().lower() or "story",
    )
    async with _catalyst_memory_lock:
        _load_catalyst_memory()
        memory = dict((_catalyst_channel_memory_getter() or {}).get(memory_key) or {})
    series_context = _resolve_catalyst_series_context(
        channel_context,
        channel_memory=memory,
        topic=topic,
        source_title="",
        input_title="",
        input_description="",
        format_preset="documentary" if str(template or "").strip().lower() == "daytrading" else "",
    )
    memory_public = dict(series_context.get("memory_view") or {})
    memory_context = _render_catalyst_channel_memory_context(memory_public)
    rewrite_pressure = dict(memory_public.get("rewrite_pressure") or _catalyst_rewrite_pressure_profile(memory_public))
    operator_summary = _clip_text(str(memory_public.get("operator_summary", "") or "").strip(), 320)
    operator_directive = _clip_text(str(memory_public.get("operator_directive", "") or "").strip(), 1200)
    operator_mission = _clip_text(str(memory_public.get("operator_mission", "") or "").strip(), 220)
    operator_guardrails = [str(v).strip() for v in list(memory_public.get("operator_guardrails") or []) if str(v).strip()]
    operator_target_niches = [str(v).strip() for v in list(memory_public.get("operator_target_niches") or []) if str(v).strip()]
    selected_cluster = dict(series_context.get("selected_cluster") or {})
    cluster_context = _clip_text(str(series_context.get("cluster_context", "") or "").strip(), 320)
    archetype_label = str(memory_public.get("archetype_label", "") or "").strip()
    promoted_archetypes = [str(v).strip() for v in list(memory_public.get("promoted_archetypes") or []) if str(v).strip()]
    demoted_archetypes = [str(v).strip() for v in list(memory_public.get("demoted_archetypes") or []) if str(v).strip()]
    archetype_memory_summary = _clip_text(str(memory_public.get("archetype_memory_summary", "") or "").strip(), 320)
    best_archetype_memory = dict(memory_public.get("best_archetype_memory") or {})
    weakest_archetype_memory = dict(memory_public.get("weakest_archetype_memory") or {})
    archetype_hook_rule = str(memory_public.get("archetype_hook_rule", "") or "").strip()
    archetype_visual_rule = str(memory_public.get("archetype_visual_rule", "") or "").strip()
    archetype_packaging_rule = str(memory_public.get("archetype_packaging_rule", "") or "").strip()
    best_archetype_hook_wins = [str(v).strip() for v in list(best_archetype_memory.get("hook_wins") or []) if str(v).strip()]
    best_archetype_packaging_wins = [str(v).strip() for v in list(best_archetype_memory.get("packaging_wins") or []) if str(v).strip()]
    best_archetype_moves = [str(v).strip() for v in list(best_archetype_memory.get("next_video_moves") or []) if str(v).strip()]
    best_archetype_keywords = [str(v).strip() for v in list(best_archetype_memory.get("proven_keywords") or []) if str(v).strip()]
    try:
        reference_playbook = _build_catalyst_reference_playbook(
            reference_memory=_catalyst_reference_memory_getter(),
            format_preset="documentary" if str(template or "").strip().lower() == "daytrading" else "",
            topic=topic,
            channel_memory=memory_public,
            selected_cluster=selected_cluster,
        )
    except Exception as e:
        log.warning(f"Catalyst reference playbook failed for template={template}: {e}")
        reference_playbook = {}
    stored_public_shorts_playbook = _public_shorts_playbook_from_memory_view(memory_public)
    try:
        public_shorts_playbook = await _build_shorts_public_reference_playbook(
            template,
            topic,
            channel_context,
            selected_cluster,
            memory_public=memory_public,
            trend_hunt_enabled=trend_hunt_enabled,
        )
    except Exception as e:
        log.warning(f"Catalyst public shorts playbook failed for template={template}: {e}")
        public_shorts_playbook = {}
    if public_shorts_playbook and stored_public_shorts_playbook:
        public_shorts_playbook = {
            "summary": _clip_text(
                str(public_shorts_playbook.get("summary", "") or "")
                or str(stored_public_shorts_playbook.get("summary", "") or ""),
                320,
            ),
            "benchmark_titles": _dedupe_preserve_order(
                [
                    *list(public_shorts_playbook.get("benchmark_titles") or []),
                    *list(stored_public_shorts_playbook.get("benchmark_titles") or []),
                ],
                max_items=6,
                max_chars=120,
            ),
            "benchmark_channels": _dedupe_preserve_order(
                [
                    *list(public_shorts_playbook.get("benchmark_channels") or []),
                    *list(stored_public_shorts_playbook.get("benchmark_channels") or []),
                ],
                max_items=6,
                max_chars=80,
            ),
            "hook_moves": _dedupe_preserve_order(
                [
                    *list(public_shorts_playbook.get("hook_moves") or []),
                    *best_archetype_hook_wins,
                    *list(stored_public_shorts_playbook.get("hook_moves") or []),
                ],
                max_items=8,
                max_chars=180,
            ),
            "packaging_moves": _dedupe_preserve_order(
                [
                    *list(public_shorts_playbook.get("packaging_moves") or []),
                    *best_archetype_packaging_wins,
                    *list(stored_public_shorts_playbook.get("packaging_moves") or []),
                ],
                max_items=8,
                max_chars=180,
            ),
            "visual_moves": _dedupe_preserve_order(
                [
                    *list(public_shorts_playbook.get("visual_moves") or []),
                    *list(stored_public_shorts_playbook.get("visual_moves") or []),
                ],
                max_items=6,
                max_chars=180,
            ),
            "keyword_moves": _dedupe_preserve_order(
                [
                    *list(public_shorts_playbook.get("keyword_moves") or []),
                    *best_archetype_keywords,
                    *list(stored_public_shorts_playbook.get("keyword_moves") or []),
                ],
                max_items=8,
                max_chars=40,
            ),
            "trend_titles": _dedupe_preserve_order(
                [
                    *list(public_shorts_playbook.get("trend_titles") or []),
                    *list(stored_public_shorts_playbook.get("trend_titles") or []),
                ],
                max_items=6,
                max_chars=120,
            ),
            "angle_candidates": sorted(
                [
                    *[dict(v or {}) for v in list(public_shorts_playbook.get("angle_candidates") or []) if isinstance(v, dict)],
                    *[dict(v or {}) for v in list(stored_public_shorts_playbook.get("angle_candidates") or []) if isinstance(v, dict)],
                ],
                key=lambda row: (
                    -float(dict(row or {}).get("score", 0.0) or 0.0),
                    -int(dict(row or {}).get("novelty_score", 0) or 0),
                    str(dict(row or {}).get("angle", "") or "").lower(),
                ),
            )[:8],
        }
    elif not public_shorts_playbook:
        public_shorts_playbook = stored_public_shorts_playbook
    if public_shorts_playbook:
        public_shorts_playbook = {
            "summary": _clip_text(
                str(public_shorts_playbook.get("summary", "") or "") or archetype_memory_summary,
                320,
            ),
            "benchmark_titles": _dedupe_preserve_order(
                list(public_shorts_playbook.get("benchmark_titles") or []),
                max_items=6,
                max_chars=120,
            ),
            "benchmark_channels": _dedupe_preserve_order(
                list(public_shorts_playbook.get("benchmark_channels") or []),
                max_items=6,
                max_chars=80,
            ),
            "hook_moves": _dedupe_preserve_order(
                [*list(public_shorts_playbook.get("hook_moves") or []), *best_archetype_hook_wins],
                max_items=8,
                max_chars=180,
            ),
            "packaging_moves": _dedupe_preserve_order(
                [*list(public_shorts_playbook.get("packaging_moves") or []), *best_archetype_packaging_wins],
                max_items=8,
                max_chars=180,
            ),
            "visual_moves": _dedupe_preserve_order(
                list(public_shorts_playbook.get("visual_moves") or []),
                max_items=8,
                max_chars=180,
            ),
            "keyword_moves": _dedupe_preserve_order(
                [*list(public_shorts_playbook.get("keyword_moves") or []), *best_archetype_keywords],
                max_items=8,
                max_chars=40,
            ),
            "trend_titles": _dedupe_preserve_order(
                list(public_shorts_playbook.get("trend_titles") or []),
                max_items=6,
                max_chars=120,
            ),
            "angle_candidates": [dict(v or {}) for v in list(public_shorts_playbook.get("angle_candidates") or []) if isinstance(v, dict)][:8],
        }
    trend_titles: list[str] = []
    if trend_hunt_enabled:
        trend_query = _build_shorts_trend_query(template, topic, channel_context, selected_cluster)
        trend_titles = await _youtube_fetch_public_trend_titles(trend_query, max_results=6)

    parts: list[str] = [
        "CATALYST SHORTS CHANNEL MODE: Build a NEW short in the same arena as the connected channel. Do not remake or lightly paraphrase an existing upload.",
    ]
    channel_title = str(channel_context.get("channel_title", "") or "").strip()
    if channel_title:
        parts.append(f"Connected YouTube channel: {channel_title}.")
    if channel_context.get("summary"):
        parts.append("Channel performance summary: " + _clip_text(str(channel_context.get("summary", "") or ""), 280))
    recent_titles = [str(v).strip() for v in list(channel_context.get("recent_upload_titles") or []) if str(v).strip()]
    if recent_titles:
        parts.append("Recent upload titles: " + ", ".join(recent_titles[:4]))
    top_titles = [str(v).strip() for v in list(channel_context.get("top_video_titles") or []) if str(v).strip()]
    if top_titles:
        parts.append("Top titles from this channel: " + ", ".join(top_titles[:4]))
    title_hints = [str(v).strip() for v in list(channel_context.get("title_pattern_hints") or []) if str(v).strip()]
    if list(selected_cluster.get("sample_titles") or []):
        title_hints = _dedupe_preserve_order([*list(selected_cluster.get("sample_titles") or []), *title_hints], max_items=6, max_chars=160)
    if title_hints:
        parts.append("Title pattern hints: " + "; ".join(title_hints[:4]))
    packaging = [str(v).strip() for v in list(channel_context.get("packaging_learnings") or []) if str(v).strip()]
    if packaging:
        parts.append("Packaging learnings: " + "; ".join(packaging[:4]))
    retention = [str(v).strip() for v in list(channel_context.get("retention_learnings") or []) if str(v).strip()]
    if retention:
        parts.append("Retention learnings: " + "; ".join(retention[:4]))
    historical_compare = dict(channel_context.get("historical_compare") or {})
    historical_summary = str(historical_compare.get("winner_vs_loser_summary", "") or "").strip()
    if historical_summary:
        parts.append("Historical winner vs loser: " + _clip_text(historical_summary, 280))
    historical_moves = [str(v).strip() for v in list(historical_compare.get("next_moves") or []) if str(v).strip()]
    if historical_moves:
        parts.append("Historical next moves: " + "; ".join(_clip_text(v, 140) for v in historical_moves[:3]))
    if operator_summary:
        parts.append("Operator summary: " + operator_summary)
    if operator_mission:
        parts.append("Operator mission: " + operator_mission)
    if operator_directive:
        parts.append("Operator directive: " + operator_directive)
    if operator_guardrails:
        parts.append("Operator guardrails: " + "; ".join(_clip_text(v, 140) for v in operator_guardrails[:6]))
    if operator_target_niches:
        parts.append("Priority niches from operator: " + ", ".join(_clip_text(v, 60) for v in operator_target_niches[:6]))
    promoted_shorts_angles = [str(v).strip() for v in list(memory_public.get("promoted_shorts_angles") or []) if str(v).strip()]
    demoted_shorts_angles = [str(v).strip() for v in list(memory_public.get("demoted_shorts_angles") or []) if str(v).strip()]
    overused_shorts_angles = [str(v).strip() for v in list(memory_public.get("overused_shorts_angles") or []) if str(v).strip()]
    retest_shorts_angles = [str(v).strip() for v in list(memory_public.get("retest_shorts_angles") or []) if str(v).strip()]
    short_angle_rotation_summary = _clip_text(str(memory_public.get("short_angle_rotation_summary", "") or "").strip(), 260)
    if promoted_shorts_angles:
        parts.append("Catalyst promoted short angles: " + "; ".join(_clip_text(v, 120) for v in promoted_shorts_angles[:3]))
    if demoted_shorts_angles:
        parts.append("Catalyst demoted short angles to avoid repeating: " + "; ".join(_clip_text(v, 120) for v in demoted_shorts_angles[:2]))
    if overused_shorts_angles:
        parts.append("Catalyst overused short angles right now: " + "; ".join(_clip_text(v, 120) for v in overused_shorts_angles[:3]))
    if retest_shorts_angles:
        parts.append("Catalyst retest-worthy short angles: " + "; ".join(_clip_text(v, 120) for v in retest_shorts_angles[:3]))
    if short_angle_rotation_summary:
        parts.append(short_angle_rotation_summary)
    if promoted_archetypes:
        parts.append("Catalyst promoted archetypes: " + "; ".join(_clip_text(v, 120) for v in promoted_archetypes[:3]))
    if demoted_archetypes:
        parts.append("Catalyst demoted archetypes: " + "; ".join(_clip_text(v, 120) for v in demoted_archetypes[:2]))
    if archetype_memory_summary:
        parts.append("Archetype memory summary: " + archetype_memory_summary)
    if best_archetype_hook_wins:
        parts.append("Best archetype hook wins: " + "; ".join(_clip_text(v, 140) for v in best_archetype_hook_wins[:3]))
    if best_archetype_packaging_wins:
        parts.append("Best archetype packaging wins: " + "; ".join(_clip_text(v, 140) for v in best_archetype_packaging_wins[:3]))
    if best_archetype_moves:
        parts.append("Best archetype next-video moves: " + "; ".join(_clip_text(v, 140) for v in best_archetype_moves[:3]))
    if str(weakest_archetype_memory.get("archetype_label", "") or weakest_archetype_memory.get("archetype_key", "") or "").strip():
        parts.append(
            "Weak archetype to avoid drifting into: "
            + _clip_text(
                str(weakest_archetype_memory.get("archetype_label", "") or weakest_archetype_memory.get("archetype_key", "") or "").strip(),
                140,
            )
        )
    if cluster_context:
        parts.append(cluster_context)
    if list(selected_cluster.get("keywords") or []):
        parts.append("Matched arc keywords: " + ", ".join(list(selected_cluster.get("keywords") or [])[:6]))
    if archetype_label:
        parts.append(f"Matched Catalyst archetype: {archetype_label}.")
    if archetype_hook_rule:
        parts.append("Archetype hook rule: " + _clip_text(archetype_hook_rule, 220))
    if archetype_visual_rule:
        parts.append("Archetype visual rule: " + _clip_text(archetype_visual_rule, 220))
    if archetype_packaging_rule:
        parts.append("Archetype packaging rule: " + _clip_text(archetype_packaging_rule, 220))
    if str(reference_playbook.get("summary", "") or "").strip():
        parts.append("Reference playbook match: " + _clip_text(str(reference_playbook.get("summary", "") or ""), 280))
    if list(reference_playbook.get("benchmark_channels") or []):
        parts.append("Reference benchmark channels: " + ", ".join(list(reference_playbook.get("benchmark_channels") or [])[:3]))
    if list(reference_playbook.get("hook_rewrites") or []):
        parts.append("Reference hook moves: " + "; ".join(list(reference_playbook.get("hook_rewrites") or [])[:3]))
    if list(reference_playbook.get("visual_rewrites") or []):
        parts.append("Reference visual moves: " + "; ".join(list(reference_playbook.get("visual_rewrites") or [])[:3]))
    if list(reference_playbook.get("packaging_rewrites") or []):
        parts.append("Reference packaging moves: " + "; ".join(list(reference_playbook.get("packaging_rewrites") or [])[:3]))
    if list(reference_playbook.get("next_video_moves") or []):
        parts.append("Reference next-video moves: " + "; ".join(list(reference_playbook.get("next_video_moves") or [])[:3]))
    if str(public_shorts_playbook.get("summary", "") or "").strip():
        parts.append("Public shorts benchmark: " + _clip_text(str(public_shorts_playbook.get("summary", "") or ""), 280))
    if list(public_shorts_playbook.get("benchmark_channels") or []):
        parts.append("Public shorts channels to beat: " + ", ".join(list(public_shorts_playbook.get("benchmark_channels") or [])[:4]))
    if list(public_shorts_playbook.get("benchmark_titles") or []):
        parts.append("Public shorts reference titles: " + "; ".join(list(public_shorts_playbook.get("benchmark_titles") or [])[:4]))
    if list(public_shorts_playbook.get("hook_moves") or []):
        parts.append("Public shorts hook moves: " + "; ".join(list(public_shorts_playbook.get("hook_moves") or [])[:4]))
    if list(public_shorts_playbook.get("packaging_moves") or []):
        parts.append("Public shorts packaging moves: " + "; ".join(list(public_shorts_playbook.get("packaging_moves") or [])[:4]))
    if list(public_shorts_playbook.get("visual_moves") or []):
        parts.append("Public shorts visual moves: " + "; ".join(list(public_shorts_playbook.get("visual_moves") or [])[:4]))
    if list(public_shorts_playbook.get("keyword_moves") or []):
        parts.append("Public shorts recurring keywords: " + ", ".join(list(public_shorts_playbook.get("keyword_moves") or [])[:6]))
    angle_candidates = [dict(v or {}) for v in list(public_shorts_playbook.get("angle_candidates") or []) if isinstance(v, dict)]
    if angle_candidates:
        parts.append(
            "Ranked angle candidates: "
            + "; ".join(
                _clip_text(
                    f"{str(row.get('angle', '') or '').strip()} (why: {str(row.get('why_now', '') or '').strip()})",
                    180,
                )
                for row in angle_candidates[:4]
                if str(row.get("angle", "") or "").strip()
            )
        )
    if trend_titles:
        parts.append("Fresh public YouTube trend signals: " + "; ".join(trend_titles[:4]))
    if memory_context:
        parts.append(memory_context)
    rewrite_summary = str(rewrite_pressure.get("summary", "") or "").strip()
    if rewrite_summary:
        parts.append("Rewrite pressure: " + rewrite_summary)
    priorities = [str(v).strip() for v in list(rewrite_pressure.get("next_run_priorities") or []) if str(v).strip()]
    if priorities:
        parts.append("Next-run priorities: " + "; ".join(priorities[:5]))
    if str(template or "").strip().lower() == "daytrading":
        parts.append(
            "DAY TRADING TEMPLATE RULES: keep the short anchored to real trading or investing behavior, chart logic, risk management, market psychology, or setup quality. "
            "Use sharper hooks, clearer stakes, and premium trading-desk / chart visuals. Use photoreal market screens, execution dashboards, order-flow or chart realism, and avoid generic wealth-posturing, empty motivational filler, medical objects, or abstract sci-fi props."
        )
    if str(template or "").strip().lower() == "skeleton":
        parts.append(
            "SKELETON TREND RULES: use instantly readable comparison logic, stronger salary/status/effort tradeoffs, and one dominant contrast per short. "
            "Avoid stale repeated profession pairings, dense lore, or confusing premise chains. Prioritize social-shareable hooks that feel new but still obvious in the first second."
        )
    if trend_hunt_enabled:
        parts.append(
            "TREND HUNT MODE: bias toward fresher breakout angles instead of safe repeats. Stay in the same arena, but choose the newer, more surprising, more curiosity-driven framing that could create a new trend for this channel."
        )
    if topic:
        parts.append(
            "Fresh-angle rule: keep the short tightly in the same subject arena as the requested topic, but generate a new hook and framing instead of recycling the source headline."
        )
    return "\n\n".join(part for part in parts if str(part or "").strip())
