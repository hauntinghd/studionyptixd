import time
from datetime import datetime

from backend_catalyst_core import (
    _clip_text,
    _dedupe_preserve_order,
    _catalyst_archetype_memory_key,
    _catalyst_channel_memory_public_view,
    _catalyst_extract_series_anchor,
    _catalyst_infer_archetype,
    _catalyst_infer_niche,
    _catalyst_outcome_weight,
    _catalyst_pick_preferred_choice,
    _catalyst_series_memory_key,
    _catalyst_update_weighted_signals,
    _extract_catalyst_keywords,
)
from backend_models import CatalystOutcomeIngestRequest

def _heuristic_catalyst_learning_record(
    *,
    session_snapshot: dict,
    edit_blueprint: dict | None = None,
    chapter_markers: list[dict] | None = None,
    package: dict | None = None,
) -> dict:
    session_snapshot = dict(session_snapshot or {})
    metadata_pack = dict(session_snapshot.get("metadata_pack") or {})
    source_analysis = dict(metadata_pack.get("source_analysis") or {})
    source_video = dict(metadata_pack.get("source_video") or {})
    channel_context = dict(metadata_pack.get("youtube_channel") or {})
    selected_cluster = dict(metadata_pack.get("selected_series_cluster") or {})
    edit_blueprint = dict(edit_blueprint or session_snapshot.get("edit_blueprint") or {})
    package = dict(package or session_snapshot.get("package") or {})
    chapters = list(session_snapshot.get("chapters") or [])
    preview_total = 0
    preview_ready = 0
    preview_failed = 0
    for chapter in chapters:
        for scene in list((chapter or {}).get("scenes") or []):
            preview_total += 1
            status = str((scene or {}).get("image_status", "") or "").strip().lower()
            if status == "ready":
                preview_ready += 1
            if status == "error":
                preview_failed += 1
    avg_viral = 0.0
    if chapters:
        avg_viral = sum(float((chapter or {}).get("viral_score", 0) or 0) for chapter in chapters) / max(1, len(chapters))
    selected_title = str(package.get("selected_title", "") or session_snapshot.get("input_title", "") or "").strip()
    selected_description = str(package.get("selected_description", "") or "").strip()
    source_title = str(source_video.get("title", "") or "").strip()
    what_hurt = _clip_text(str(source_analysis.get("what_hurt", "") or "").strip(), 220)
    what_worked = _clip_text(str(source_analysis.get("what_worked", "") or "").strip(), 220)
    improvement_moves = [str(v).strip() for v in list(source_analysis.get("improvement_moves") or []) if str(v).strip()]
    retention_findings = [str(v).strip() for v in list(source_analysis.get("retention_findings") or []) if str(v).strip()]
    packaging_findings = [str(v).strip() for v in list(source_analysis.get("packaging_findings") or []) if str(v).strip()]
    hook_strategy = dict(edit_blueprint.get("hook_strategy") or {})
    pacing_strategy = dict(edit_blueprint.get("pacing_strategy") or {})
    motion_strategy = dict(edit_blueprint.get("motion_strategy") or {})
    sound_strategy = dict(edit_blueprint.get("sound_strategy") or {})
    execution_strategy = dict(edit_blueprint.get("execution_strategy") or {})
    chapter_count = len(chapters)
    outcome_summary = (
        f"Catalyst built a {session_snapshot.get('format_preset', 'documentary')} follow-up around "
        f"{_clip_text(session_snapshot.get('topic', '') or selected_title, 80)}. "
        f"Average chapter score: {avg_viral:.1f}. Preview success: {preview_ready}/{max(1, preview_total)} scene frames. "
        f"Packaging now centers on '{selected_title or source_title}'."
    )
    return {
        "session_id": str(session_snapshot.get("session_id", "") or ""),
        "channel_id": str(session_snapshot.get("youtube_channel_id", "") or ""),
        "format_preset": str(session_snapshot.get("format_preset", "") or ""),
        "mode": "prepublish_learning_record",
        "created_at": time.time(),
        "chapter_score_average": round(avg_viral, 2),
        "preview_success_rate": round((preview_ready / max(1, preview_total)) * 100.0, 2),
        "outcome_summary": outcome_summary,
        "wins_to_keep": _dedupe_preserve_order([
            what_worked,
            _clip_text(str(hook_strategy.get("promise", "") or ""), 180),
            _clip_text(str(sound_strategy.get("music_profile", "") or ""), 120),
            _clip_text(selected_description, 180),
        ], max_items=6, max_chars=180),
        "mistakes_to_avoid": _dedupe_preserve_order([
            what_hurt,
            *retention_findings[:3],
            "Do not let packaging or chapter structure drift back toward the source title or source pacing.",
            "Do not accept black or missing scene previews in a publishable run." if preview_failed else "",
        ], max_items=6, max_chars=180),
        "hook_adjustments": _dedupe_preserve_order([
            _clip_text(str(hook_strategy.get("first_30s_mission", "") or ""), 180),
            _clip_text(str(hook_strategy.get("open_loop", "") or ""), 180),
            *retention_findings[:2],
        ], max_items=6, max_chars=180),
        "pacing_adjustments": _dedupe_preserve_order([
            _clip_text(str(pacing_strategy.get("escalation_curve", "") or ""), 160),
            *list(pacing_strategy.get("pacing_rules") or [])[:3],
            _clip_text(f"Cut profile: {str(execution_strategy.get('cut_profile', '') or '').strip()}", 120) if str(execution_strategy.get("cut_profile", "") or "").strip() else "",
            _clip_text(f"Voice pacing bias: {str(execution_strategy.get('voice_pacing_bias', '') or '').strip()}", 120) if str(execution_strategy.get("voice_pacing_bias", "") or "").strip() else "",
            f"Keep each chapter near {chapter_count} high-signal beats and avoid dead air." if chapter_count else "",
        ], max_items=6, max_chars=180),
        "visual_adjustments": _dedupe_preserve_order([
            *list(motion_strategy.get("visual_rules") or [])[:3],
            *list(motion_strategy.get("camera_language") or [])[:2],
            *list(motion_strategy.get("motion_graphics") or [])[:2],
            _clip_text(str(execution_strategy.get("visual_variation_rule", "") or ""), 180),
            _clip_text(f"Caption rhythm: {str(execution_strategy.get('caption_rhythm', '') or '').strip()}", 120) if str(execution_strategy.get("caption_rhythm", "") or "").strip() else "",
        ], max_items=8, max_chars=180),
        "sound_adjustments": _dedupe_preserve_order([
            *list(sound_strategy.get("mix_notes") or [])[:3],
            *list(sound_strategy.get("silence_rules") or [])[:2],
            *list(sound_strategy.get("voice_direction") or [])[:2],
            _clip_text(f"Sound density: {str(execution_strategy.get('sound_density', '') or '').strip()}", 120) if str(execution_strategy.get("sound_density", "") or "").strip() else "",
            _clip_text(f"Opening intensity: {str(execution_strategy.get('opening_intensity', '') or '').strip()}", 120) if str(execution_strategy.get("opening_intensity", "") or "").strip() else "",
        ], max_items=8, max_chars=180),
        "packaging_adjustments": _dedupe_preserve_order([
            *packaging_findings[:3],
            f"Use the selected title as the lead package direction: {selected_title}" if selected_title else "",
            f"Keep tags aligned with the same arena: {', '.join(list(package.get('selected_tags') or [])[:6])}" if list(package.get("selected_tags") or []) else "",
        ], max_items=8, max_chars=180),
        "next_video_moves": _dedupe_preserve_order([
            *improvement_moves[:4],
            "Use channel memory to stay in the same recognizable arena without repeating exact phrasing.",
            "Promote the strongest contrast or hidden mechanism into the next title and thumbnail package.",
        ], max_items=8, max_chars=180),
        "memory_updates": _dedupe_preserve_order([
            _clip_text(str(channel_context.get("summary", "") or ""), 180),
            _clip_text(str(hook_strategy.get("promise", "") or ""), 180),
            _clip_text(str(sound_strategy.get("music_profile", "") or ""), 120),
            _clip_text(str(motion_strategy.get("transition_style", "") or ""), 120),
            _clip_text(str(execution_strategy.get("cut_profile", "") or ""), 120),
            _clip_text(str(execution_strategy.get("caption_rhythm", "") or ""), 120),
            _clip_text(selected_title, 160),
        ], max_items=8, max_chars=180),
        "execution_strategy": {
            "opening_intensity": str(execution_strategy.get("opening_intensity", "") or ""),
            "interrupt_strength": str(execution_strategy.get("interrupt_strength", "") or ""),
            "caption_rhythm": str(execution_strategy.get("caption_rhythm", "") or ""),
            "sound_density": str(execution_strategy.get("sound_density", "") or ""),
            "cut_profile": str(execution_strategy.get("cut_profile", "") or ""),
            "voice_pacing_bias": str(execution_strategy.get("voice_pacing_bias", "") or ""),
            "payoff_hold_sec": round(float(execution_strategy.get("payoff_hold_sec", 0.0) or 0.0), 2),
            "visual_variation_rule": _clip_text(str(execution_strategy.get("visual_variation_rule", "") or ""), 180),
        },
        "selected_title": selected_title,
        "selected_description": selected_description,
        "selected_tags": list(package.get("selected_tags") or []),
        "chapter_markers": list(chapter_markers or []),
    }


def _update_catalyst_channel_memory(
    *,
    existing: dict | None,
    session_snapshot: dict,
    learning_record: dict,
    edit_blueprint: dict | None = None,
    package: dict | None = None,
) -> dict:
    existing = dict(existing or {})
    session_snapshot = dict(session_snapshot or {})
    metadata_pack = dict(session_snapshot.get("metadata_pack") or {})
    source_analysis = dict(metadata_pack.get("source_analysis") or {})
    source_video = dict(metadata_pack.get("source_video") or {})
    channel_context = dict(metadata_pack.get("youtube_channel") or {})
    selected_cluster = dict(metadata_pack.get("selected_series_cluster") or {})
    edit_blueprint = dict(edit_blueprint or session_snapshot.get("edit_blueprint") or {})
    package = dict(package or session_snapshot.get("package") or {})
    sound_strategy = dict(edit_blueprint.get("sound_strategy") or {})
    motion_strategy = dict(edit_blueprint.get("motion_strategy") or {})
    execution_strategy = dict(edit_blueprint.get("execution_strategy") or {})
    selected_title = str(learning_record.get("selected_title", "") or package.get("selected_title", "") or "").strip()
    source_title = str(source_video.get("title", "") or "").strip()
    format_preset = str(session_snapshot.get("format_preset", "") or existing.get("format_preset", "") or "documentary")
    niche = _catalyst_infer_niche(
        selected_title,
        source_title,
        " ".join(str(v).strip() for v in list(package.get("selected_tags") or []) if str(v).strip()),
        " ".join(str(v).strip() for v in list(channel_context.get("recent_upload_titles") or [])[:6] if str(v).strip()),
        format_preset=format_preset,
    )
    archetype = _catalyst_infer_archetype(
        selected_title,
        source_title,
        " ".join(str(v).strip() for v in list(package.get("selected_tags") or []) if str(v).strip()),
        " ".join(str(v).strip() for v in list(channel_context.get("recent_upload_titles") or [])[:6] if str(v).strip()),
        niche_key=str(niche.get("key", "") or ""),
        format_preset=format_preset,
    )
    series_anchor = _catalyst_extract_series_anchor(
        selected_title,
        source_title,
        " ".join(str(v).strip() for v in list(channel_context.get("recent_upload_titles") or [])[:6] if str(v).strip()),
        niche_key=str(niche.get("key", "") or ""),
    ) or str((selected_cluster or {}).get("series_anchor", "") or "").strip() or str(existing.get("series_anchor", "") or "").strip()
    proven_keywords = _extract_catalyst_keywords(
        selected_title,
        source_title,
        *list(package.get("selected_tags") or []),
        *list(channel_context.get("recent_upload_titles") or [])[:4],
    )
    updated = dict(existing)
    run_count = int(updated.get("run_count", 0) or 0) + 1
    updated.update({
        "key": str(updated.get("key", "") or session_snapshot.get("channel_memory_key", "") or ""),
        "channel_id": str(session_snapshot.get("youtube_channel_id", "") or updated.get("channel_id", "") or ""),
        "format_preset": format_preset,
        "niche_key": str(niche.get("key", "") or updated.get("niche_key", "") or ""),
        "niche_label": str(niche.get("label", "") or updated.get("niche_label", "") or ""),
        "niche_confidence": round(float(niche.get("confidence", 0.0) or updated.get("niche_confidence", 0.0) or 0.0), 2),
        "niche_keywords": _dedupe_preserve_order(
            [*list(niche.get("keywords") or []), *list(updated.get("niche_keywords") or [])],
            max_items=8,
            max_chars=40,
        ),
        "niche_follow_up_rule": str(niche.get("follow_up_rule", "") or updated.get("niche_follow_up_rule", "") or ""),
        "archetype_key": str(archetype.get("key", "") or updated.get("archetype_key", "") or ""),
        "archetype_label": str(archetype.get("label", "") or updated.get("archetype_label", "") or ""),
        "archetype_confidence": round(float(archetype.get("confidence", 0.0) or updated.get("archetype_confidence", 0.0) or 0.0), 2),
        "archetype_keywords": _dedupe_preserve_order(
            [*list(archetype.get("keywords") or []), *list(updated.get("archetype_keywords") or [])],
            max_items=8,
            max_chars=40,
        ),
        "archetype_hook_rule": str(archetype.get("hook_rule", "") or updated.get("archetype_hook_rule", "") or ""),
        "archetype_pace_rule": str(archetype.get("pace_rule", "") or updated.get("archetype_pace_rule", "") or ""),
        "archetype_visual_rule": str(archetype.get("visual_rule", "") or updated.get("archetype_visual_rule", "") or ""),
        "archetype_sound_rule": str(archetype.get("sound_rule", "") or updated.get("archetype_sound_rule", "") or ""),
        "archetype_packaging_rule": str(archetype.get("packaging_rule", "") or updated.get("archetype_packaging_rule", "") or ""),
        "selected_cluster_label": str(selected_cluster.get("label", "") or updated.get("selected_cluster_label", "") or ""),
        "selected_cluster_key": str(selected_cluster.get("key", "") or updated.get("selected_cluster_key", "") or ""),
        "series_anchor": series_anchor,
        "run_count": run_count,
        "last_session_id": str(session_snapshot.get("session_id", "") or ""),
        "preferred_transition_style": str(motion_strategy.get("transition_style", "") or updated.get("preferred_transition_style", "") or ""),
        "preferred_music_profile": str(sound_strategy.get("music_profile", "") or updated.get("preferred_music_profile", "") or ""),
        "preferred_visual_engine": str(edit_blueprint.get("visual_engine", "") or updated.get("preferred_visual_engine", "") or ""),
        "preferred_cut_profile": str(execution_strategy.get("cut_profile", "") or updated.get("preferred_cut_profile", "") or ""),
        "preferred_caption_rhythm": str(execution_strategy.get("caption_rhythm", "") or updated.get("preferred_caption_rhythm", "") or ""),
        "preferred_opening_intensity": str(execution_strategy.get("opening_intensity", "") or updated.get("preferred_opening_intensity", "") or ""),
        "preferred_interrupt_strength": str(execution_strategy.get("interrupt_strength", "") or updated.get("preferred_interrupt_strength", "") or ""),
        "preferred_sound_density": str(execution_strategy.get("sound_density", "") or updated.get("preferred_sound_density", "") or ""),
        "preferred_voice_pacing_bias": str(execution_strategy.get("voice_pacing_bias", "") or updated.get("preferred_voice_pacing_bias", "") or ""),
        "preferred_payoff_hold_sec": round(float(execution_strategy.get("payoff_hold_sec", updated.get("preferred_payoff_hold_sec", 0.0)) or 0.0), 2),
        "preferred_visual_variation_rule": _clip_text(str(execution_strategy.get("visual_variation_rule", "") or updated.get("preferred_visual_variation_rule", "") or ""), 180),
        "updated_at": time.time(),
    })
    updated["recent_source_titles"] = _dedupe_preserve_order([source_title, *list(updated.get("recent_source_titles") or [])], max_items=10, max_chars=160)
    updated["recent_selected_titles"] = _dedupe_preserve_order([selected_title, *list(updated.get("recent_selected_titles") or [])], max_items=10, max_chars=160)
    updated["proven_keywords"] = _dedupe_preserve_order([*proven_keywords, *list(updated.get("proven_keywords") or [])], max_items=14, max_chars=80)
    updated["hook_learnings"] = _dedupe_preserve_order([*list(learning_record.get("hook_adjustments") or []), *list(learning_record.get("wins_to_keep") or [])[:2], *list(updated.get("hook_learnings") or [])], max_items=10, max_chars=180)
    updated["pacing_learnings"] = _dedupe_preserve_order([*list(learning_record.get("pacing_adjustments") or []), *list(updated.get("pacing_learnings") or [])], max_items=10, max_chars=180)
    updated["visual_learnings"] = _dedupe_preserve_order([*list(learning_record.get("visual_adjustments") or []), *list(updated.get("visual_learnings") or [])], max_items=10, max_chars=180)
    updated["sound_learnings"] = _dedupe_preserve_order([*list(learning_record.get("sound_adjustments") or []), *list(updated.get("sound_learnings") or [])], max_items=10, max_chars=180)
    updated["packaging_learnings"] = _dedupe_preserve_order([*list(learning_record.get("packaging_adjustments") or []), *list(source_analysis.get("packaging_findings") or [])[:2], *list(updated.get("packaging_learnings") or [])], max_items=10, max_chars=180)
    updated["retention_watchouts"] = _dedupe_preserve_order([*list(learning_record.get("mistakes_to_avoid") or []), *list(source_analysis.get("retention_findings") or [])[:2], *list(updated.get("retention_watchouts") or [])], max_items=10, max_chars=180)
    updated["next_video_moves"] = _dedupe_preserve_order([*list(learning_record.get("next_video_moves") or []), *list(updated.get("next_video_moves") or [])], max_items=10, max_chars=180)
    _catalyst_update_weighted_signals(updated, "hook_wins_map", list(learning_record.get("wins_to_keep") or [])[:2] + list(learning_record.get("hook_adjustments") or []), 0.35)
    _catalyst_update_weighted_signals(updated, "hook_watchouts_map", list(learning_record.get("mistakes_to_avoid") or [])[:2], 0.35)
    _catalyst_update_weighted_signals(updated, "pacing_wins_map", list(learning_record.get("pacing_adjustments") or []), 0.35)
    _catalyst_update_weighted_signals(updated, "visual_wins_map", list(learning_record.get("visual_adjustments") or []), 0.35)
    _catalyst_update_weighted_signals(updated, "sound_wins_map", list(learning_record.get("sound_adjustments") or []), 0.35)
    _catalyst_update_weighted_signals(updated, "packaging_wins_map", list(learning_record.get("packaging_adjustments") or [])[:3], 0.35)
    _catalyst_update_weighted_signals(updated, "retention_watchouts_map", list(learning_record.get("mistakes_to_avoid") or [])[:4], 0.35)
    _catalyst_update_weighted_signals(updated, "next_video_moves_map", list(learning_record.get("next_video_moves") or []), 0.35)
    public = _catalyst_channel_memory_public_view(updated)
    updated["summary"] = _clip_text(
        "Catalyst has "
        + f"{run_count} run{'s' if run_count != 1 else ''} on this channel lane. "
        + (f"Niche: {str(updated.get('niche_label', '') or '').strip()}. " if str(updated.get("niche_label", "") or "").strip() else "")
        + (f"Archetype: {str(updated.get('archetype_label', '') or '').strip()}. " if str(updated.get("archetype_label", "") or "").strip() else "")
        + (f"Series anchor: {str(updated.get('series_anchor', '') or '').strip()}. " if str(updated.get("series_anchor", "") or "").strip() else "")
        + (f"Matched cluster: {str(updated.get('selected_cluster_label', '') or '').strip()}. " if str(updated.get("selected_cluster_label", "") or "").strip() else "")
        + ("Keep " + ", ".join(list(updated.get("proven_keywords") or [])[:6]) + ". " if list(updated.get("proven_keywords") or []) else "")
        + ("Best hook lesson: " + str((list(public.get("hook_wins") or []) or [""])[0]) + ". " if list(public.get("hook_wins") or []) else "")
        + ("Current retention watchout: " + str((list(public.get("retention_watchouts") or []) or [""])[0]) + "." if list(public.get("retention_watchouts") or []) else ""),
        320,
    )
    if series_anchor:
        series_map = dict(updated.get("series_memory_map") or {})
        series_key = _catalyst_series_memory_key(series_anchor)
        series_bucket = dict(series_map.get(series_key) or {})
        series_run_count = int(series_bucket.get("run_count", 0) or 0) + 1
        series_bucket.update({
            "series_anchor": series_anchor,
            "channel_id": str(updated.get("channel_id", "") or ""),
            "format_preset": format_preset,
            "niche_key": str(updated.get("niche_key", "") or ""),
            "niche_label": str(updated.get("niche_label", "") or ""),
            "niche_confidence": round(float(updated.get("niche_confidence", 0.0) or 0.0), 2),
            "niche_keywords": _dedupe_preserve_order([*list(updated.get("niche_keywords") or []), *list(series_bucket.get("niche_keywords") or [])], max_items=8, max_chars=40),
            "niche_follow_up_rule": str(updated.get("niche_follow_up_rule", "") or ""),
            "archetype_key": str(updated.get("archetype_key", "") or ""),
            "archetype_label": str(updated.get("archetype_label", "") or ""),
            "archetype_confidence": round(float(updated.get("archetype_confidence", 0.0) or 0.0), 2),
            "archetype_keywords": _dedupe_preserve_order([*list(updated.get("archetype_keywords") or []), *list(series_bucket.get("archetype_keywords") or [])], max_items=8, max_chars=40),
            "archetype_hook_rule": str(updated.get("archetype_hook_rule", "") or ""),
            "archetype_pace_rule": str(updated.get("archetype_pace_rule", "") or ""),
            "archetype_visual_rule": str(updated.get("archetype_visual_rule", "") or ""),
            "archetype_sound_rule": str(updated.get("archetype_sound_rule", "") or ""),
            "archetype_packaging_rule": str(updated.get("archetype_packaging_rule", "") or ""),
            "run_count": series_run_count,
            "last_session_id": str(session_snapshot.get("session_id", "") or ""),
            "preferred_transition_style": str(motion_strategy.get("transition_style", "") or series_bucket.get("preferred_transition_style", "") or ""),
            "preferred_music_profile": str(sound_strategy.get("music_profile", "") or series_bucket.get("preferred_music_profile", "") or ""),
            "preferred_visual_engine": str(edit_blueprint.get("visual_engine", "") or series_bucket.get("preferred_visual_engine", "") or ""),
            "updated_at": time.time(),
        })
        series_bucket["recent_source_titles"] = _dedupe_preserve_order([source_title, *list(series_bucket.get("recent_source_titles") or [])], max_items=10, max_chars=160)
        series_bucket["recent_selected_titles"] = _dedupe_preserve_order([selected_title, *list(series_bucket.get("recent_selected_titles") or [])], max_items=10, max_chars=160)
        series_bucket["proven_keywords"] = _dedupe_preserve_order([*proven_keywords, *list(series_bucket.get("proven_keywords") or [])], max_items=14, max_chars=80)
        series_bucket["hook_learnings"] = _dedupe_preserve_order([*list(learning_record.get("hook_adjustments") or []), *list(learning_record.get("wins_to_keep") or [])[:2], *list(series_bucket.get("hook_learnings") or [])], max_items=10, max_chars=180)
        series_bucket["pacing_learnings"] = _dedupe_preserve_order([*list(learning_record.get("pacing_adjustments") or []), *list(series_bucket.get("pacing_learnings") or [])], max_items=10, max_chars=180)
        series_bucket["visual_learnings"] = _dedupe_preserve_order([*list(learning_record.get("visual_adjustments") or []), *list(series_bucket.get("visual_learnings") or [])], max_items=10, max_chars=180)
        series_bucket["sound_learnings"] = _dedupe_preserve_order([*list(learning_record.get("sound_adjustments") or []), *list(series_bucket.get("sound_learnings") or [])], max_items=10, max_chars=180)
        series_bucket["packaging_learnings"] = _dedupe_preserve_order([*list(learning_record.get("packaging_adjustments") or []), *list(source_analysis.get("packaging_findings") or [])[:2], *list(series_bucket.get("packaging_learnings") or [])], max_items=10, max_chars=180)
        series_bucket["retention_watchouts"] = _dedupe_preserve_order([*list(learning_record.get("mistakes_to_avoid") or []), *list(source_analysis.get("retention_findings") or [])[:2], *list(series_bucket.get("retention_watchouts") or [])], max_items=10, max_chars=180)
        series_bucket["next_video_moves"] = _dedupe_preserve_order([*list(learning_record.get("next_video_moves") or []), *list(series_bucket.get("next_video_moves") or [])], max_items=10, max_chars=180)
        _catalyst_update_weighted_signals(series_bucket, "hook_wins_map", list(learning_record.get("wins_to_keep") or [])[:2] + list(learning_record.get("hook_adjustments") or []), 0.35)
        _catalyst_update_weighted_signals(series_bucket, "hook_watchouts_map", list(learning_record.get("mistakes_to_avoid") or [])[:2], 0.35)
        _catalyst_update_weighted_signals(series_bucket, "pacing_wins_map", list(learning_record.get("pacing_adjustments") or []), 0.35)
        _catalyst_update_weighted_signals(series_bucket, "visual_wins_map", list(learning_record.get("visual_adjustments") or []), 0.35)
        _catalyst_update_weighted_signals(series_bucket, "sound_wins_map", list(learning_record.get("sound_adjustments") or []), 0.35)
        _catalyst_update_weighted_signals(series_bucket, "packaging_wins_map", list(learning_record.get("packaging_adjustments") or [])[:3], 0.35)
        _catalyst_update_weighted_signals(series_bucket, "retention_watchouts_map", list(learning_record.get("mistakes_to_avoid") or [])[:4], 0.35)
        _catalyst_update_weighted_signals(series_bucket, "next_video_moves_map", list(learning_record.get("next_video_moves") or []), 0.35)
        series_public = _catalyst_channel_memory_public_view(series_bucket, series_anchor_override=series_anchor)
        series_bucket["summary"] = _clip_text(
            f"Catalyst has {series_run_count} run{'s' if series_run_count != 1 else ''} inside {series_anchor}. "
            + (f"Archetype: {str(series_bucket.get('archetype_label', '') or '').strip()}. " if str(series_bucket.get("archetype_label", "") or "").strip() else "")
            + ("Best hook lesson: " + str((list(series_public.get("hook_wins") or []) or [""])[0]) + ". " if list(series_public.get("hook_wins") or []) else "")
            + ("Current watchout: " + str((list(series_public.get('retention_watchouts') or []) or [''])[0]) + ". " if list(series_public.get("retention_watchouts") or []) else "")
            + ("Next move: " + str((list(series_public.get("next_video_moves") or []) or [""])[0]) + "." if list(series_public.get("next_video_moves") or []) else ""),
            320,
        )
        series_map[series_key] = series_bucket
        updated["series_memory_map"] = series_map
    return updated


async def _youtube_fetch_video_analytics(access_token: str, channel_id: str, video_id: str) -> dict:
    vid = str(video_id or "").strip()
    cid = str(channel_id or "").strip()
    if not vid or not cid:
        return {}
    payload = {}
    for metrics in (
        "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,impressions,impressionClickThroughRate",
        "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage",
    ):
        try:
            payload = await _youtube_api_get(
                access_token,
                "/reports",
                analytics=True,
                params={
                    "ids": f"channel=={cid}",
                    "startDate": (datetime.now(timezone.utc).date() - timedelta(days=365)).isoformat(),
                    "endDate": datetime.now(timezone.utc).date().isoformat(),
                    "metrics": metrics,
                    "filters": f"video=={vid}",
                },
            )
            break
        except Exception:
            payload = {}
            continue
    if not isinstance(payload, dict) or not list(payload.get("rows") or []):
        return {}
    row = list((payload.get("rows") or [[[]]])[0] or [])
    headers = [str((col or {}).get("name", "") or "") for col in list(payload.get("columnHeaders") or [])]
    metrics_map: dict[str, float] = {}
    for idx, header in enumerate(headers):
        if idx >= len(row):
            continue
        try:
            metrics_map[header] = float(row[idx] or 0.0)
        except Exception:
            continue
    return {
        "views": int(metrics_map.get("views", 0.0) or 0.0),
        "estimated_minutes_watched": round(float(metrics_map.get("estimatedMinutesWatched", 0.0) or 0.0), 2),
        "average_view_duration_sec": round(float(metrics_map.get("averageViewDuration", 0.0) or 0.0), 2),
        "average_percentage_viewed": round(float(metrics_map.get("averageViewPercentage", 0.0) or 0.0), 2),
        "impressions": int(metrics_map.get("impressions", 0.0) or 0.0),
        "impression_click_through_rate": round(float(metrics_map.get("impressionClickThroughRate", 0.0) or 0.0), 2),
    }


def _parse_utc_datetime(raw_value: str) -> datetime | None:
    value = str(raw_value or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _catalyst_text_overlap_score(primary: str, secondary: str) -> float:
    left = str(primary or "").strip().lower()
    right = str(secondary or "").strip().lower()
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    left_tokens = set(_extract_catalyst_keywords(left, max_items=16))
    right_tokens = set(_extract_catalyst_keywords(right, max_items=16))
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = left_tokens & right_tokens
    union = left_tokens | right_tokens
    return round(len(overlap) / max(1, len(union)), 4)


def _match_published_video_to_longform_session(session_snapshot: dict, candidates: list[dict]) -> dict:
    session_snapshot = dict(session_snapshot or {})
    package = dict(session_snapshot.get("package") or {})
    metadata_pack = dict(session_snapshot.get("metadata_pack") or {})
    source_video = dict(metadata_pack.get("source_video") or {})
    expected_titles = _dedupe_preserve_order(
        [
            str(package.get("selected_title", "") or "").strip(),
            *[str(v).strip() for v in list(package.get("title_variants") or [])[:3] if str(v).strip()],
            str(session_snapshot.get("input_title", "") or "").strip(),
            str(session_snapshot.get("topic", "") or "").strip(),
        ],
        max_items=6,
        max_chars=180,
    )
    expected_tags = [str(v).strip().lower() for v in list(package.get("selected_tags") or package.get("tags") or []) if str(v).strip()]
    source_title = str(source_video.get("title", "") or "").strip()
    created_at = float(session_snapshot.get("updated_at", 0.0) or session_snapshot.get("created_at", 0.0) or 0.0)
    best_score = -1.0
    best_candidate: dict = {}
    for raw in list(candidates or []):
        candidate = dict(raw or {})
        title = str(candidate.get("title", "") or "").strip()
        if not title:
            continue
        title_score = 0.0
        for idx, target in enumerate(expected_titles):
            if not target:
                continue
            similarity = _catalyst_text_overlap_score(title, target)
            title_score = max(title_score, similarity * max(45.0, 100.0 - (idx * 12.0)))
            if title.lower() == target.lower():
                title_score = max(title_score, 140.0)
        tag_overlap = 0.0
        candidate_tags = [str(v).strip().lower() for v in list(candidate.get("tags") or []) if str(v).strip()]
        if expected_tags and candidate_tags:
            overlap = len(set(expected_tags) & set(candidate_tags))
            tag_overlap = overlap * 5.0
        recency_score = 0.0
        published_at = _parse_utc_datetime(str(candidate.get("published_at", "") or ""))
        if created_at > 0 and published_at is not None:
            delta_days = abs((published_at.timestamp() - created_at) / 86400.0)
            if delta_days <= 3:
                recency_score = 24.0
            elif delta_days <= 7:
                recency_score = 18.0
            elif delta_days <= 14:
                recency_score = 10.0
            elif delta_days <= 30:
                recency_score = 4.0
        score = title_score + tag_overlap + recency_score
        if source_title and _title_is_too_close_to_source(title, source_title):
            score -= 16.0
        score += min(12.0, float(candidate.get("views", 0) or 0) / 50000.0)
        if score > best_score:
            best_score = score
            best_candidate = candidate
    return best_candidate if best_score >= 35.0 else {}


def _build_auto_outcome_request(
    *,
    session_snapshot: dict,
    video_meta: dict,
    analytics_metrics: dict | None = None,
) -> CatalystOutcomeIngestRequest:
    session_snapshot = dict(session_snapshot or {})
    video_meta = dict(video_meta or {})
    analytics_metrics = dict(analytics_metrics or {})
    package = dict(session_snapshot.get("package") or {})
    channel_memory = _catalyst_channel_memory_public_view(session_snapshot.get("channel_memory") or {})
    metrics = {
        "views": int(video_meta.get("views", 0) or analytics_metrics.get("views", 0) or 0),
        "impressions": int(analytics_metrics.get("impressions", 0) or 0),
        "likes": int(video_meta.get("likes", 0) or 0),
        "comments": int(video_meta.get("comments", 0) or 0),
        "estimated_minutes_watched": round(float(analytics_metrics.get("estimated_minutes_watched", 0.0) or 0.0), 2),
        "average_view_duration_sec": round(float(analytics_metrics.get("average_view_duration_sec", 0.0) or 0.0), 2),
        "average_percentage_viewed": round(float(analytics_metrics.get("average_percentage_viewed", 0.0) or 0.0), 2),
        "impression_click_through_rate": round(float(analytics_metrics.get("impression_click_through_rate", 0.0) or 0.0), 2),
        "first_30_sec_retention_pct": round(float(analytics_metrics.get("first_30_sec_retention_pct", 0.0) or 0.0), 2),
        "first_60_sec_retention_pct": round(float(analytics_metrics.get("first_60_sec_retention_pct", 0.0) or 0.0), 2),
    }
    title_used = str(video_meta.get("title", "") or package.get("selected_title", "") or session_snapshot.get("input_title", "") or "").strip()
    source_title = str(((dict(session_snapshot.get("metadata_pack") or {})).get("source_video") or {}).get("title", "") or session_snapshot.get("input_title", "") or "").strip()
    recent_titles = [
        *list(channel_memory.get("recent_selected_titles") or []),
        *list(channel_memory.get("recent_source_titles") or []),
    ]
    title_novelty = _catalyst_title_novelty_score(title_used, source_title=source_title, recent_titles=recent_titles)
    avp = float(metrics.get("average_percentage_viewed", 0.0) or 0.0)
    ctr = float(metrics.get("impression_click_through_rate", 0.0) or 0.0)
    avd = float(metrics.get("average_view_duration_sec", 0.0) or 0.0)
    first30 = float(metrics.get("first_30_sec_retention_pct", 0.0) or 0.0)
    first60 = float(metrics.get("first_60_sec_retention_pct", 0.0) or 0.0)
    preview_success = float((dict(session_snapshot.get("learning_record") or {})).get("preview_success_rate", 0.0) or 0.0)
    target_duration_sec = max(float(video_meta.get("duration_sec", 0.0) or 0.0), float(session_snapshot.get("target_minutes", 0.0) or 0.0) * 60.0)
    avd_ratio = round((avd / max(target_duration_sec, 1.0)) * 100.0, 2) if target_duration_sec > 0 else 0.0

    strongest_signals = _dedupe_preserve_order([
        f"CTR around {ctr:.2f}% is showing the package is earning clicks." if ctr >= 4.0 else "",
        f"Average viewed around {avp:.2f}% suggests the pacing is holding attention." if avp >= 42.0 else "",
        f"First 30 second retention around {first30:.2f}% shows the hook is landing." if first30 >= 62.0 else "",
        f"Views have reached roughly {int(metrics['views']):,}, so this run has enough signal to learn from." if int(metrics["views"]) >= 300 else "",
    ], max_items=8, max_chars=180)
    weak_points = _dedupe_preserve_order([
        f"CTR around {ctr:.2f}% is soft, so the title/thumbnail package needs a stronger curiosity gap." if 0 < ctr < 4.0 else "",
        f"Average viewed around {avp:.2f}% suggests the pacing still loses too many viewers." if 0 < avp < 42.0 else "",
        f"First 30 second retention around {first30:.2f}% suggests the hook needs a faster payoff." if 0 < first30 < 62.0 else "",
        f"Title novelty score is only {title_novelty}/100, so the headline is still too close to prior phrasing." if title_novelty < 75 else "",
    ], max_items=8, max_chars=180)
    hook_wins = _dedupe_preserve_order([
        "The opening promise is working; keep starting on the consequence or hidden mechanism." if first30 >= 62.0 else "",
        "The hook is converting because the title promise gets paid off quickly." if first60 >= 52.0 else "",
    ], max_items=6, max_chars=180)
    hook_watchouts = _dedupe_preserve_order([
        "Shorten the setup and show the concrete payoff earlier in the first 15 to 30 seconds." if 0 < first30 < 62.0 else "",
        "The hook still needs a harder first-minute escalation." if 0 < first60 < 52.0 else "",
    ], max_items=6, max_chars=180)
    pacing_wins = _dedupe_preserve_order([
        "The pacing is holding viewers well enough to keep the same escalation pattern." if avp >= 42.0 else "",
        "Average view duration suggests the chapter rhythm is working." if avd_ratio >= 35.0 else "",
    ], max_items=6, max_chars=180)
    pacing_watchouts = _dedupe_preserve_order([
        "Cut dead-air explanation blocks and force a reveal, contrast, or payoff beat every 10 to 15 seconds." if 0 < avp < 42.0 else "",
        "Compress mid-section explanation and escalate consequences sooner." if 0 < avd_ratio < 35.0 else "",
    ], max_items=6, max_chars=180)
    visual_wins = _dedupe_preserve_order([
        "The visual grammar is supporting retention; keep the same premium 3D documentary direction." if avp >= 42.0 and preview_success >= 80.0 else "",
        "Scene coverage is strong enough to keep the same proof-style visual system." if preview_success >= 90.0 else "",
    ], max_items=6, max_chars=180)
    visual_watchouts = _dedupe_preserve_order([
        "Increase visual variety and replace repeated hero-object framing with system cutaways or human-versus-system beats." if 0 < avp < 42.0 else "",
        "Scene-preview reliability needs to stay higher so no proof beat drops out." if 0 < preview_success < 90.0 else "",
    ], max_items=6, max_chars=180)
    sound_wins = _dedupe_preserve_order([
        "Sound design is supporting retention; keep using reveals as the moments for impact punctuation." if first60 >= 52.0 else "",
        "The narration rhythm is holding through the early beats." if avp >= 42.0 else "",
    ], max_items=6, max_chars=180)
    sound_watchouts = _dedupe_preserve_order([
        "Use more deliberate impact punctuation and silence pockets around the main reveals." if 0 < first60 < 52.0 else "",
        "The sound bed is probably too flat or too constant for the reveal cadence." if 0 < avp < 40.0 else "",
    ], max_items=6, max_chars=180)
    packaging_wins = _dedupe_preserve_order([
        "The package is working; keep the same arena while changing the angle." if ctr >= 4.0 else "",
        "The title is distinct enough from prior winners to preserve freshness." if title_novelty >= 80 else "",
    ], max_items=6, max_chars=180)
    packaging_watchouts = _dedupe_preserve_order([
        "Generate a genuinely new headline in the same arena instead of staying too close to prior titles." if title_novelty < 80 else "",
        "Push one stronger hidden mechanism, conflict, or payoff into the title and thumbnail." if 0 < ctr < 4.0 else "",
    ], max_items=6, max_chars=180)
    retention_wins = _dedupe_preserve_order([
        "Viewer retention is strong enough to keep the same chapter escalation grammar." if avp >= 42.0 else "",
        "The first 30 seconds are holding; preserve the fast payoff structure." if first30 >= 62.0 else "",
    ], max_items=6, max_chars=180)
    retention_watchouts = _dedupe_preserve_order([
        "The intro still needs a tighter promise and faster proof." if 0 < first30 < 62.0 else "",
        "Retention is the bottleneck; every next run should remove dead-air explanation and repeat less." if 0 < avp < 42.0 else "",
    ], max_items=6, max_chars=180)
    next_video_moves = _dedupe_preserve_order([
        *list(channel_memory.get("reference_next_video_moves") or [])[:3],
        *packaging_watchouts[:2],
        *hook_watchouts[:1],
        *pacing_watchouts[:1],
        "Stay in the same arena, but shift the angle so the next headline feels adjacent and genuinely new.",
    ], max_items=8, max_chars=180)
    operator_summary = _clip_text(
        f"Auto-harvested outcome for {title_used or 'this video'}. "
        + (f"CTR {ctr:.2f}%. " if ctr > 0 else "")
        + (f"Average viewed {avp:.2f}%. " if avp > 0 else "")
        + (f"First 30 seconds {first30:.2f}%. " if first30 > 0 else "")
        + ("Main issue: the package needs a stronger curiosity gap. " if 0 < ctr < 4.0 else "")
        + ("Main issue: the hook or pacing still loses viewers too early. " if (0 < first30 < 62.0 or 0 < avp < 42.0) else "")
        + ("Next move: " + next_video_moves[0] + ".") if next_video_moves else "",
        320,
    )
    return CatalystOutcomeIngestRequest(
        video_url=str(video_meta.get("url", "") or ""),
        video_id=str(video_meta.get("video_id", "") or ""),
        title_used=title_used,
        description_used=str(video_meta.get("description", "") or package.get("selected_description", "") or ""),
        thumbnail_prompt=str(package.get("thumbnail_prompt", "") or ""),
        thumbnail_url=str(video_meta.get("thumbnail_url", "") or package.get("thumbnail_url", "") or ""),
        tags=[str(v).strip() for v in list(video_meta.get("tags") or package.get("selected_tags") or []) if str(v).strip()],
        views=int(metrics["views"]),
        impressions=int(metrics["impressions"]),
        likes=int(metrics["likes"]),
        comments=int(metrics["comments"]),
        estimated_minutes_watched=float(metrics["estimated_minutes_watched"]),
        average_view_duration_sec=float(metrics["average_view_duration_sec"]),
        average_percentage_viewed=float(metrics["average_percentage_viewed"]),
        impression_click_through_rate=float(metrics["impression_click_through_rate"]),
        first_30_sec_retention_pct=float(metrics["first_30_sec_retention_pct"]),
        first_60_sec_retention_pct=float(metrics["first_60_sec_retention_pct"]),
        operator_summary=operator_summary,
        strongest_signals=strongest_signals,
        weak_points=weak_points,
        hook_wins=hook_wins,
        hook_watchouts=hook_watchouts,
        pacing_wins=pacing_wins,
        pacing_watchouts=pacing_watchouts,
        visual_wins=visual_wins,
        visual_watchouts=visual_watchouts,
        sound_wins=sound_wins,
        sound_watchouts=sound_watchouts,
        packaging_wins=packaging_wins,
        packaging_watchouts=packaging_watchouts,
        retention_wins=retention_wins,
        retention_watchouts=retention_watchouts,
        next_video_moves=next_video_moves,
        auto_fetch_channel_metrics=False,
    )


async def _persist_catalyst_outcome_for_session(
    *,
    session_id: str,
    user_id: str,
    session: dict,
    req: CatalystOutcomeIngestRequest,
    video_meta: dict | None = None,
    analytics_metrics: dict | None = None,
) -> tuple[dict, dict]:
    outcome_record = _build_catalyst_outcome_record(
        session_snapshot=session,
        outcome_req=req,
        video_meta=video_meta,
        analytics_metrics=analytics_metrics,
    )

    channel_id = str(session.get("youtube_channel_id", "") or "").strip()
    async with _catalyst_memory_lock:
        _load_catalyst_memory()
        learning_key = str(session.get("session_id", "") or session_id)
        channel_memory_key = str(
            session.get("channel_memory_key", "")
            or _catalyst_channel_memory_key(
                user_id,
                channel_id,
                str(session.get("format_preset", "") or "documentary"),
            )
        )
        existing_memory = dict(_catalyst_channel_memory.get(channel_memory_key) or session.get("channel_memory") or {})
        updated_channel_memory = _apply_catalyst_outcome_to_channel_memory(
            existing=existing_memory,
            session_snapshot=session,
            outcome_record=outcome_record,
        )
        updated_channel_memory["key"] = channel_memory_key
        _catalyst_channel_memory[channel_memory_key] = updated_channel_memory
        existing_learning_entry = _catalyst_learning_records.get(learning_key)
        if isinstance(existing_learning_entry, dict) and ("prepublish" in existing_learning_entry or "outcome_history" in existing_learning_entry):
            learning_entry = dict(existing_learning_entry)
        else:
            learning_entry = {
                "prepublish": dict(session.get("learning_record") or existing_learning_entry or {}),
                "outcome_history": [],
            }
        history = [dict(item or {}) for item in list(learning_entry.get("outcome_history") or []) if isinstance(item, dict)]
        history.append(outcome_record)
        learning_entry["latest_outcome"] = outcome_record
        learning_entry["outcome_history"] = history[-16:]
        _catalyst_learning_records[learning_key] = learning_entry
        _save_catalyst_memory()

    async with _longform_sessions_lock:
        _load_longform_sessions()
        session_live = _longform_sessions.get(session_id)
        if not isinstance(session_live, dict):
            raise HTTPException(404, "Long-form session not found")
        metadata_pack = dict(session_live.get("metadata_pack") or {})
        metadata_pack["catalyst_channel_memory"] = _catalyst_channel_memory_public_view(updated_channel_memory)
        session_live["metadata_pack"] = metadata_pack
        session_live["latest_outcome"] = outcome_record
        session_live["channel_memory"] = _catalyst_channel_memory_public_view(updated_channel_memory)
        session_live["updated_at"] = time.time()
        _save_longform_sessions()
        session_live = dict(session_live)
    return outcome_record, session_live


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


async def _harvest_catalyst_outcomes_for_channel(
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

    async with _longform_outcome_sync_semaphore:
        async with _youtube_connections_lock:
            _load_youtube_connections()
            record = dict((_youtube_bucket_for_user(user_key).get("channels") or {}).get(channel_key) or {})
        if not record:
            raise HTTPException(404, "Connected YouTube channel not found")

        access_token, refreshed = await _youtube_ensure_access_token(record)
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
            sessions = [
                dict(value or {})
                for value in _longform_sessions.values()
                if str((value or {}).get("user_id", "") or "").strip() == user_key
                and str((value or {}).get("youtube_channel_id", "") or "").strip() == channel_key
                and (not session_filter or str((value or {}).get("session_id", "") or "").strip() == session_filter)
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
            session_live = _longform_sessions.get(session_filter)
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


async def _maybe_refresh_channel_outcomes_before_longform_run(
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
    shortform_priority = await _shortform_priority_snapshot()
    if shortform_priority.get("priority_active"):
        return {
            "ok": True,
            "skipped": True,
            "reason": "shortform_priority_window",
            "shortform_priority": shortform_priority,
        }
    try:
        return await _harvest_catalyst_outcomes_for_channel(
            user_id=user_key,
            channel_id=channel_key,
            candidate_limit=12,
            refresh_existing=False,
        )
    except Exception as e:
        log.warning(f"[catalyst] automatic channel outcome sync failed before longform run: {e}")
        return {"ok": False, "error": _clip_text(str(e), 220)}


async def _shortform_priority_snapshot() -> dict:
    queue_depth = 0
    queue_workers = 1
    queue_max_depth = 1
    try:
        queue_depth = max(0, int(await get_queue_depth()))
    except Exception:
        queue_depth = 0
    try:
        queue_workers = max(1, int(get_queue_workers()))
    except Exception:
        queue_workers = 1
    try:
        queue_max_depth = max(1, int(get_queue_max_depth()))
    except Exception:
        queue_max_depth = max(1, queue_workers)

    active_shortform = 0
    active_longform = 0
    for job in list(jobs.values()):
        if not isinstance(job, dict):
            continue
        status = str(job.get("status", "") or "").strip().lower()
        if status not in _SHORTFORM_PROTECTED_ACTIVE_JOB_STATUSES:
            continue
        lane = str(job.get("lane", "") or "").strip().lower()
        if lane == "longform":
            active_longform += 1
            continue
        if lane in _SHORTFORM_PROTECTED_LANES:
            active_shortform += 1

    queue_utilization_pct = round((queue_depth / max(queue_max_depth, 1)) * 100.0, 1)
    priority_active = bool(active_shortform > 0 or queue_depth > 0)
    reason = ""
    if active_shortform > 0:
        reason = "active_shortform_jobs"
    elif queue_depth > 0:
        reason = "shared_queue_busy"
    return {
        "priority_active": priority_active,
        "reason": reason,
        "active_shortform_jobs": int(active_shortform),
        "active_longform_jobs": int(active_longform),
        "queue_depth": int(queue_depth),
        "queue_workers": int(queue_workers),
        "queue_max_depth": int(queue_max_depth),
        "queue_utilization_pct": float(queue_utilization_pct),
    }


async def _mark_longform_waiting_for_shortform_capacity(
    session_id: str,
    *,
    stage: str,
    snapshot: dict | None = None,
) -> None:
    snapshot = dict(snapshot or {})
    async with _longform_sessions_lock:
        live = _longform_sessions.get(session_id)
        if not isinstance(live, dict):
            return
        progress = dict(live.get("draft_progress") or {})
        progress["stage"] = stage
        progress["shortform_priority"] = {
            "reason": str(snapshot.get("reason", "") or ""),
            "active_shortform_jobs": int(snapshot.get("active_shortform_jobs", 0) or 0),
            "queue_depth": int(snapshot.get("queue_depth", 0) or 0),
            "queue_workers": int(snapshot.get("queue_workers", 1) or 1),
            "queue_utilization_pct": float(snapshot.get("queue_utilization_pct", 0.0) or 0.0),
        }
        live["draft_progress"] = progress
        live["status"] = "draft_review"
        live["updated_at"] = time.time()
        _save_longform_sessions()


def _catalyst_normalize_execution_metric(value: float, *, scale: float = 1.0, maximum: float = 100.0) -> float:
    numeric = float(value or 0.0) * float(scale or 1.0)
    return max(0.0, min(float(maximum or 100.0), numeric))


def _catalyst_execution_scorecard(
    execution_strategy: dict | None,
    metrics: dict | None,
    reference_scores: dict | None,
) -> dict:
    execution_strategy = dict(execution_strategy or {})
    metrics = dict(metrics or {})
    reference_scores = dict(reference_scores or {})

    hook_inputs = [
        _catalyst_normalize_execution_metric(metrics.get("first_30_sec_retention_pct", 0.0)),
        _catalyst_normalize_execution_metric(reference_scores.get("hook", 0.0)),
    ]
    pacing_inputs = [
        _catalyst_normalize_execution_metric(metrics.get("average_percentage_viewed", 0.0)),
        _catalyst_normalize_execution_metric(metrics.get("first_60_sec_retention_pct", 0.0)),
        _catalyst_normalize_execution_metric(reference_scores.get("pacing", 0.0)),
    ]
    visual_inputs = [
        _catalyst_normalize_execution_metric(reference_scores.get("visuals", 0.0)),
    ]
    sound_inputs = [
        _catalyst_normalize_execution_metric(reference_scores.get("sound", 0.0)),
        _catalyst_normalize_execution_metric(metrics.get("first_60_sec_retention_pct", 0.0), scale=0.7),
    ]
    packaging_inputs = [
        _catalyst_normalize_execution_metric(metrics.get("impression_click_through_rate", 0.0), scale=15.0),
        _catalyst_normalize_execution_metric(reference_scores.get("packaging", 0.0)),
        _catalyst_normalize_execution_metric(reference_scores.get("title_novelty", 0.0)),
    ]

    def _avg(values: list[float]) -> float:
        filtered = [float(v) for v in values if float(v) > 0]
        if not filtered:
            return 0.0
        return round(sum(filtered) / len(filtered), 2)

    hook_score = _avg(hook_inputs)
    pacing_score = _avg(pacing_inputs)
    visual_score = _avg(visual_inputs)
    sound_score = _avg(sound_inputs)
    packaging_score = _avg(packaging_inputs)
    overall_inputs = [hook_score, pacing_score, visual_score, sound_score, packaging_score]
    overall_score = _avg(overall_inputs)

    return {
        "overall": overall_score,
        "hook": hook_score,
        "pacing": pacing_score,
        "visuals": visual_score,
        "sound": sound_score,
        "packaging": packaging_score,
        "signature": _clip_text(
            " | ".join(
                part for part in [
                    str(execution_strategy.get("opening_intensity", "") or "").strip(),
                    str(execution_strategy.get("cut_profile", "") or "").strip(),
                    str(execution_strategy.get("caption_rhythm", "") or "").strip(),
                    str(execution_strategy.get("sound_density", "") or "").strip(),
                    str(execution_strategy.get("voice_pacing_bias", "") or "").strip(),
                ] if part
            ),
            180,
        ),
    }


def _catalyst_execution_signal_payloads(execution_strategy: dict | None, execution_scores: dict | None) -> dict:
    execution_strategy = dict(execution_strategy or {})
    execution_scores = dict(execution_scores or {})

    def _bucket(score: float) -> str:
        numeric = float(score or 0.0)
        if numeric >= 68:
            return "win"
        if numeric <= 54:
            return "watchout"
        return "neutral"

    payload = {
        "opening_intensity": {"wins": [], "watchouts": []},
        "interrupt_strength": {"wins": [], "watchouts": []},
        "caption_rhythm": {"wins": [], "watchouts": []},
        "sound_density": {"wins": [], "watchouts": []},
        "cut_profile": {"wins": [], "watchouts": []},
        "voice_pacing_bias": {"wins": [], "watchouts": []},
        "visual_variation_rule": {"wins": [], "watchouts": []},
        "execution_profile": {"wins": [], "watchouts": []},
    }

    mapping = {
        "opening_intensity": ("hook", 40),
        "interrupt_strength": ("pacing", 40),
        "caption_rhythm": ("pacing", 40),
        "sound_density": ("sound", 40),
        "cut_profile": ("pacing", 60),
        "voice_pacing_bias": ("hook", 60),
        "visual_variation_rule": ("visuals", 160),
    }
    for field_name, (score_key, max_chars) in mapping.items():
        value = _clip_text(str(execution_strategy.get(field_name, "") or "").strip(), max_chars)
        if not value:
            continue
        bucket = _bucket(float(execution_scores.get(score_key, 0.0) or 0.0))
        if bucket == "win":
            payload[field_name]["wins"].append(value)
        elif bucket == "watchout":
            payload[field_name]["watchouts"].append(value)

    signature = _clip_text(str(execution_scores.get("signature", "") or "").strip(), 180)
    if signature:
        bucket = _bucket(float(execution_scores.get("overall", 0.0) or 0.0))
        if bucket == "win":
            payload["execution_profile"]["wins"].append(signature)
        elif bucket == "watchout":
            payload["execution_profile"]["watchouts"].append(signature)

    return payload


def _build_catalyst_outcome_record(
    *,
    session_snapshot: dict,
    outcome_req: CatalystOutcomeIngestRequest,
    video_meta: dict | None = None,
    analytics_metrics: dict | None = None,
    source_url_video_id_fn=None,
    score_against_reference_fn=None,
) -> dict:
    session_snapshot = dict(session_snapshot or {})
    outcome_req = outcome_req or CatalystOutcomeIngestRequest()
    package = dict(session_snapshot.get("package") or {})
    metadata_pack = dict(session_snapshot.get("metadata_pack") or {})
    source_analysis = dict(metadata_pack.get("source_analysis") or {})
    channel_context = dict(metadata_pack.get("youtube_channel") or {})
    edit_blueprint = dict(session_snapshot.get("edit_blueprint") or {})
    video_meta = dict(video_meta or {})
    analytics_metrics = dict(analytics_metrics or {})
    selected_title = str(outcome_req.title_used or package.get("selected_title", "") or session_snapshot.get("input_title", "") or "").strip()
    selected_description = str(outcome_req.description_used or package.get("selected_description", "") or "").strip()
    selected_tags = _dedupe_preserve_order([
        *list(outcome_req.tags or []),
        *list(package.get("selected_tags") or []),
        *list(package.get("tags") or []),
        *list(video_meta.get("tags") or []),
    ], max_items=20, max_chars=64)
    source_url_video_id_fn = source_url_video_id_fn or (lambda _source_url: "")
    video_id = str(outcome_req.video_id or video_meta.get("video_id", "") or source_url_video_id_fn(str(outcome_req.video_url or "").strip()) or "").strip()
    video_url = str(outcome_req.video_url or "").strip()
    if not video_url and video_id:
        video_url = f"https://www.youtube.com/watch?v={video_id}"
    metrics = {
        "views": int(outcome_req.views or video_meta.get("views", 0) or analytics_metrics.get("views", 0) or 0),
        "impressions": int(outcome_req.impressions or analytics_metrics.get("impressions", 0) or 0),
        "likes": int(outcome_req.likes or video_meta.get("likes", 0) or 0),
        "comments": int(outcome_req.comments or video_meta.get("comments", 0) or 0),
        "estimated_minutes_watched": round(float(outcome_req.estimated_minutes_watched or analytics_metrics.get("estimated_minutes_watched", 0.0) or 0.0), 2),
        "average_view_duration_sec": round(float(outcome_req.average_view_duration_sec or analytics_metrics.get("average_view_duration_sec", 0.0) or 0.0), 2),
        "average_percentage_viewed": round(float(outcome_req.average_percentage_viewed or analytics_metrics.get("average_percentage_viewed", 0.0) or 0.0), 2),
        "impression_click_through_rate": round(float(outcome_req.impression_click_through_rate or analytics_metrics.get("impression_click_through_rate", 0.0) or 0.0), 2),
        "first_30_sec_retention_pct": round(float(outcome_req.first_30_sec_retention_pct or 0.0), 2),
        "first_60_sec_retention_pct": round(float(outcome_req.first_60_sec_retention_pct or 0.0), 2),
    }
    weight = _catalyst_outcome_weight(metrics)
    execution_strategy = dict(edit_blueprint.get("execution_strategy") or {})
    strongest_signals = _dedupe_preserve_order([
        *list(outcome_req.strongest_signals or []),
        *list(source_analysis.get("strongest_signals") or [])[:2],
    ], max_items=10, max_chars=180)
    weak_points = _dedupe_preserve_order([
        *list(outcome_req.weak_points or []),
        *list(source_analysis.get("weak_points") or [])[:2],
    ], max_items=10, max_chars=180)
    next_video_moves = _dedupe_preserve_order([
        *list(outcome_req.next_video_moves or []),
        *list(source_analysis.get("improvement_moves") or [])[:3],
    ], max_items=10, max_chars=180)
    operator_summary = _clip_text(str(outcome_req.operator_summary or "").strip(), 320)
    if not operator_summary:
        summary_bits = [
            f"Outcome logged for {selected_title}." if selected_title else "Outcome logged for this long-form run.",
            f"CTR {metrics['impression_click_through_rate']:.2f}%." if metrics["impression_click_through_rate"] > 0 else "",
            f"Average viewed {metrics['average_percentage_viewed']:.2f}%." if metrics["average_percentage_viewed"] > 0 else "",
            f"First 30 seconds retention {metrics['first_30_sec_retention_pct']:.2f}%." if metrics["first_30_sec_retention_pct"] > 0 else "",
            ("Next move: " + next_video_moves[0] + ".") if next_video_moves else "",
        ]
        operator_summary = _clip_text(" ".join(bit for bit in summary_bits if bit), 320)
    outcome_record = {
        "session_id": str(session_snapshot.get("session_id", "") or ""),
        "channel_id": str(session_snapshot.get("youtube_channel_id", "") or channel_context.get("channel_id", "") or ""),
        "format_preset": str(session_snapshot.get("format_preset", "") or ""),
        "created_at": time.time(),
        "video_id": video_id,
        "video_url": video_url,
        "video_duration_sec": round(float(video_meta.get("duration_sec", 0.0) or 0.0), 2),
        "title_used": selected_title,
        "description_used": selected_description,
        "thumbnail_prompt": _clip_text(str(outcome_req.thumbnail_prompt or package.get("thumbnail_prompt", "") or ""), 240),
        "thumbnail_url": str(outcome_req.thumbnail_url or package.get("thumbnail_url", "") or "").strip(),
        "tags": selected_tags,
        "weight": weight,
        "metrics": metrics,
        "operator_summary": operator_summary,
        "strongest_signals": strongest_signals,
        "weak_points": weak_points,
        "hook_wins": _dedupe_preserve_order(list(outcome_req.hook_wins or []), max_items=8, max_chars=180),
        "hook_watchouts": _dedupe_preserve_order(list(outcome_req.hook_watchouts or []), max_items=8, max_chars=180),
        "pacing_wins": _dedupe_preserve_order(list(outcome_req.pacing_wins or []), max_items=8, max_chars=180),
        "pacing_watchouts": _dedupe_preserve_order(list(outcome_req.pacing_watchouts or []), max_items=8, max_chars=180),
        "visual_wins": _dedupe_preserve_order(list(outcome_req.visual_wins or []), max_items=8, max_chars=180),
        "visual_watchouts": _dedupe_preserve_order(list(outcome_req.visual_watchouts or []), max_items=8, max_chars=180),
        "sound_wins": _dedupe_preserve_order(list(outcome_req.sound_wins or []), max_items=8, max_chars=180),
        "sound_watchouts": _dedupe_preserve_order(list(outcome_req.sound_watchouts or []), max_items=8, max_chars=180),
        "packaging_wins": _dedupe_preserve_order(list(outcome_req.packaging_wins or []), max_items=8, max_chars=180),
        "packaging_watchouts": _dedupe_preserve_order(list(outcome_req.packaging_watchouts or []), max_items=8, max_chars=180),
        "retention_wins": _dedupe_preserve_order(list(outcome_req.retention_wins or []), max_items=8, max_chars=180),
        "retention_watchouts": _dedupe_preserve_order(list(outcome_req.retention_watchouts or []), max_items=8, max_chars=180),
        "next_video_moves": next_video_moves,
    }
    reference_comparison = score_against_reference_fn(
        session_snapshot=session_snapshot,
        outcome_record=outcome_record,
    ) if score_against_reference_fn else {}
    execution_scores = _catalyst_execution_scorecard(
        execution_strategy=execution_strategy,
        metrics=metrics,
        reference_scores=dict(reference_comparison.get("scores") or {}),
    )
    if execution_strategy:
        outcome_record["execution_strategy"] = {
            "opening_intensity": str(execution_strategy.get("opening_intensity", "") or ""),
            "interrupt_strength": str(execution_strategy.get("interrupt_strength", "") or ""),
            "caption_rhythm": str(execution_strategy.get("caption_rhythm", "") or ""),
            "sound_density": str(execution_strategy.get("sound_density", "") or ""),
            "cut_profile": str(execution_strategy.get("cut_profile", "") or ""),
            "voice_pacing_bias": str(execution_strategy.get("voice_pacing_bias", "") or ""),
            "payoff_hold_sec": round(float(execution_strategy.get("payoff_hold_sec", 0.0) or 0.0), 2),
            "visual_variation_rule": _clip_text(str(execution_strategy.get("visual_variation_rule", "") or ""), 180),
        }
    if execution_scores:
        outcome_record["execution_scores"] = execution_scores
    if reference_comparison:
        outcome_record["reference_comparison"] = reference_comparison
    return outcome_record


def _apply_catalyst_outcome_to_channel_memory(
    *,
    existing: dict | None,
    session_snapshot: dict,
    outcome_record: dict,
) -> dict:
    updated = dict(existing or {})
    session_snapshot = dict(session_snapshot or {})
    outcome_record = dict(outcome_record or {})
    metadata_pack = dict(session_snapshot.get("metadata_pack") or {})
    selected_cluster = dict(metadata_pack.get("selected_series_cluster") or {})
    metrics = dict(outcome_record.get("metrics") or {})
    weight = float(outcome_record.get("weight", 1.0) or 1.0)
    reference_comparison = dict(outcome_record.get("reference_comparison") or {})
    reference_scores = dict(reference_comparison.get("scores") or {})
    execution_strategy = dict(outcome_record.get("execution_strategy") or {})
    execution_scores = dict(outcome_record.get("execution_scores") or {})
    execution_signals = _catalyst_execution_signal_payloads(execution_strategy, execution_scores)
    format_preset = str(session_snapshot.get("format_preset", "") or updated.get("format_preset", "") or "documentary")
    updated["key"] = str(updated.get("key", "") or session_snapshot.get("channel_memory_key", "") or "")
    updated["channel_id"] = str(updated.get("channel_id", "") or session_snapshot.get("youtube_channel_id", "") or outcome_record.get("channel_id", "") or "")
    updated["format_preset"] = format_preset
    updated["selected_cluster_label"] = str(selected_cluster.get("label", "") or updated.get("selected_cluster_label", "") or "")
    updated["selected_cluster_key"] = str(selected_cluster.get("key", "") or updated.get("selected_cluster_key", "") or "")
    updated["run_count"] = max(int(updated.get("run_count", 0) or 0), 1)
    updated["outcome_count"] = int(updated.get("outcome_count", 0) or 0) + 1
    updated["last_session_id"] = str(session_snapshot.get("session_id", "") or updated.get("last_session_id", "") or "")
    updated["updated_at"] = time.time()
    updated["last_outcome_summary"] = _clip_text(str(outcome_record.get("operator_summary", "") or ""), 320)
    updated["recent_selected_titles"] = _dedupe_preserve_order(
        [str(outcome_record.get("title_used", "") or "").strip(), *list(updated.get("recent_selected_titles") or [])],
        max_items=10,
        max_chars=160,
    )
    updated["proven_keywords"] = _dedupe_preserve_order(
        [
            *_extract_catalyst_keywords(
                str(outcome_record.get("title_used", "") or ""),
                *list(outcome_record.get("tags") or []),
            ),
            *list(updated.get("proven_keywords") or []),
        ],
        max_items=14,
        max_chars=80,
    )
    updated["outcome_views_sum"] = float(updated.get("outcome_views_sum", 0.0) or 0.0) + float(metrics.get("views", 0.0) or 0.0)
    updated["outcome_impressions_sum"] = float(updated.get("outcome_impressions_sum", 0.0) or 0.0) + float(metrics.get("impressions", 0.0) or 0.0)
    updated["outcome_ctr_sum"] = float(updated.get("outcome_ctr_sum", 0.0) or 0.0) + float(metrics.get("impression_click_through_rate", 0.0) or 0.0)
    updated["outcome_avp_sum"] = float(updated.get("outcome_avp_sum", 0.0) or 0.0) + float(metrics.get("average_percentage_viewed", 0.0) or 0.0)
    updated["outcome_avd_sum"] = float(updated.get("outcome_avd_sum", 0.0) or 0.0) + float(metrics.get("average_view_duration_sec", 0.0) or 0.0)
    updated["outcome_first30_sum"] = float(updated.get("outcome_first30_sum", 0.0) or 0.0) + float(metrics.get("first_30_sec_retention_pct", 0.0) or 0.0)
    updated["outcome_first60_sum"] = float(updated.get("outcome_first60_sum", 0.0) or 0.0) + float(metrics.get("first_60_sec_retention_pct", 0.0) or 0.0)
    updated["reference_overall_score_sum"] = float(updated.get("reference_overall_score_sum", 0.0) or 0.0) + float(reference_scores.get("overall", 0.0) or 0.0)
    updated["reference_hook_score_sum"] = float(updated.get("reference_hook_score_sum", 0.0) or 0.0) + float(reference_scores.get("hook", 0.0) or 0.0)
    updated["reference_pacing_score_sum"] = float(updated.get("reference_pacing_score_sum", 0.0) or 0.0) + float(reference_scores.get("pacing", 0.0) or 0.0)
    updated["reference_visual_score_sum"] = float(updated.get("reference_visual_score_sum", 0.0) or 0.0) + float(reference_scores.get("visuals", 0.0) or 0.0)
    updated["reference_sound_score_sum"] = float(updated.get("reference_sound_score_sum", 0.0) or 0.0) + float(reference_scores.get("sound", 0.0) or 0.0)
    updated["reference_packaging_score_sum"] = float(updated.get("reference_packaging_score_sum", 0.0) or 0.0) + float(reference_scores.get("packaging", 0.0) or 0.0)
    updated["reference_title_novelty_score_sum"] = float(updated.get("reference_title_novelty_score_sum", 0.0) or 0.0) + float(reference_scores.get("title_novelty", 0.0) or 0.0)
    updated["execution_overall_score_sum"] = float(updated.get("execution_overall_score_sum", 0.0) or 0.0) + float(execution_scores.get("overall", 0.0) or 0.0)
    updated["execution_hook_score_sum"] = float(updated.get("execution_hook_score_sum", 0.0) or 0.0) + float(execution_scores.get("hook", 0.0) or 0.0)
    updated["execution_pacing_score_sum"] = float(updated.get("execution_pacing_score_sum", 0.0) or 0.0) + float(execution_scores.get("pacing", 0.0) or 0.0)
    updated["execution_visual_score_sum"] = float(updated.get("execution_visual_score_sum", 0.0) or 0.0) + float(execution_scores.get("visuals", 0.0) or 0.0)
    updated["execution_sound_score_sum"] = float(updated.get("execution_sound_score_sum", 0.0) or 0.0) + float(execution_scores.get("sound", 0.0) or 0.0)
    updated["execution_packaging_score_sum"] = float(updated.get("execution_packaging_score_sum", 0.0) or 0.0) + float(execution_scores.get("packaging", 0.0) or 0.0)
    updated["hook_learnings"] = _dedupe_preserve_order([*list(outcome_record.get("hook_wins") or []), *list(updated.get("hook_learnings") or [])], max_items=10, max_chars=180)
    updated["pacing_learnings"] = _dedupe_preserve_order([*list(outcome_record.get("pacing_wins") or []), *list(updated.get("pacing_learnings") or [])], max_items=10, max_chars=180)
    updated["visual_learnings"] = _dedupe_preserve_order([*list(outcome_record.get("visual_wins") or []), *list(updated.get("visual_learnings") or [])], max_items=10, max_chars=180)
    updated["sound_learnings"] = _dedupe_preserve_order([*list(outcome_record.get("sound_wins") or []), *list(updated.get("sound_learnings") or [])], max_items=10, max_chars=180)
    updated["packaging_learnings"] = _dedupe_preserve_order([*list(outcome_record.get("packaging_wins") or []), *list(updated.get("packaging_learnings") or [])], max_items=10, max_chars=180)
    updated["retention_watchouts"] = _dedupe_preserve_order([*list(outcome_record.get("retention_watchouts") or []), *list(updated.get("retention_watchouts") or [])], max_items=10, max_chars=180)
    updated["next_video_moves"] = _dedupe_preserve_order([*list(outcome_record.get("next_video_moves") or []), *list(updated.get("next_video_moves") or [])], max_items=10, max_chars=180)
    updated["reference_benchmark_channels"] = _dedupe_preserve_order([
        *list(reference_comparison.get("benchmark_channels") or []),
        *list(updated.get("reference_benchmark_channels") or []),
    ], max_items=8, max_chars=80)
    updated["reference_tier"] = str(reference_comparison.get("tier", "") or updated.get("reference_tier", "") or "")
    updated["last_reference_summary"] = _clip_text(str(reference_comparison.get("reference_summary", "") or updated.get("last_reference_summary", "") or ""), 360)
    _catalyst_update_weighted_signals(updated, "hook_wins_map", list(outcome_record.get("hook_wins") or []), weight)
    _catalyst_update_weighted_signals(updated, "hook_watchouts_map", list(outcome_record.get("hook_watchouts") or []), weight)
    _catalyst_update_weighted_signals(updated, "pacing_wins_map", list(outcome_record.get("pacing_wins") or []), weight)
    _catalyst_update_weighted_signals(updated, "pacing_watchouts_map", list(outcome_record.get("pacing_watchouts") or []), weight)
    _catalyst_update_weighted_signals(updated, "visual_wins_map", list(outcome_record.get("visual_wins") or []), weight)
    _catalyst_update_weighted_signals(updated, "visual_watchouts_map", list(outcome_record.get("visual_watchouts") or []), weight)
    _catalyst_update_weighted_signals(updated, "sound_wins_map", list(outcome_record.get("sound_wins") or []), weight)
    _catalyst_update_weighted_signals(updated, "sound_watchouts_map", list(outcome_record.get("sound_watchouts") or []), weight)
    _catalyst_update_weighted_signals(updated, "packaging_wins_map", list(outcome_record.get("packaging_wins") or []), weight)
    _catalyst_update_weighted_signals(updated, "packaging_watchouts_map", list(outcome_record.get("packaging_watchouts") or []), weight)
    _catalyst_update_weighted_signals(updated, "retention_wins_map", list(outcome_record.get("retention_wins") or []), weight)
    _catalyst_update_weighted_signals(updated, "retention_watchouts_map", list(outcome_record.get("retention_watchouts") or []), weight)
    _catalyst_update_weighted_signals(updated, "next_video_moves_map", list(outcome_record.get("next_video_moves") or []), weight)
    _catalyst_update_weighted_signals(updated, "reference_hook_rewrites_map", list(reference_comparison.get("hook_rewrites") or []), weight)
    _catalyst_update_weighted_signals(updated, "reference_pacing_rewrites_map", list(reference_comparison.get("pacing_rewrites") or []), weight)
    _catalyst_update_weighted_signals(updated, "reference_visual_rewrites_map", list(reference_comparison.get("visual_rewrites") or []), weight)
    _catalyst_update_weighted_signals(updated, "reference_sound_rewrites_map", list(reference_comparison.get("sound_rewrites") or []), weight)
    _catalyst_update_weighted_signals(updated, "reference_packaging_rewrites_map", list(reference_comparison.get("packaging_rewrites") or []), weight)
    _catalyst_update_weighted_signals(updated, "reference_next_video_moves_map", list(reference_comparison.get("next_run_moves") or []), weight)
    _catalyst_update_weighted_signals(updated, "next_video_moves_map", list(reference_comparison.get("next_run_moves") or []), weight * 0.8)
    for field_name, payload in (
        ("opening_intensity", execution_signals.get("opening_intensity") or {}),
        ("interrupt_strength", execution_signals.get("interrupt_strength") or {}),
        ("caption_rhythm", execution_signals.get("caption_rhythm") or {}),
        ("sound_density", execution_signals.get("sound_density") or {}),
        ("cut_profile", execution_signals.get("cut_profile") or {}),
        ("voice_pacing_bias", execution_signals.get("voice_pacing_bias") or {}),
        ("visual_variation_rule", execution_signals.get("visual_variation_rule") or {}),
        ("execution_profile", execution_signals.get("execution_profile") or {}),
    ):
        _catalyst_update_weighted_signals(updated, f"{field_name}_wins_map", list(payload.get("wins") or []), weight)
        _catalyst_update_weighted_signals(updated, f"{field_name}_watchouts_map", list(payload.get("watchouts") or []), weight)
    updated["preferred_cut_profile"] = _catalyst_pick_preferred_choice(
        updated.get("cut_profile_wins_map") or {},
        updated.get("cut_profile_watchouts_map") or {},
        str(updated.get("preferred_cut_profile", "") or ""),
        max_chars=60,
    )
    updated["preferred_caption_rhythm"] = _catalyst_pick_preferred_choice(
        updated.get("caption_rhythm_wins_map") or {},
        updated.get("caption_rhythm_watchouts_map") or {},
        str(updated.get("preferred_caption_rhythm", "") or ""),
        max_chars=40,
    )
    updated["preferred_opening_intensity"] = _catalyst_pick_preferred_choice(
        updated.get("opening_intensity_wins_map") or {},
        updated.get("opening_intensity_watchouts_map") or {},
        str(updated.get("preferred_opening_intensity", "") or ""),
        max_chars=40,
    )
    updated["preferred_interrupt_strength"] = _catalyst_pick_preferred_choice(
        updated.get("interrupt_strength_wins_map") or {},
        updated.get("interrupt_strength_watchouts_map") or {},
        str(updated.get("preferred_interrupt_strength", "") or ""),
        max_chars=40,
    )
    updated["preferred_sound_density"] = _catalyst_pick_preferred_choice(
        updated.get("sound_density_wins_map") or {},
        updated.get("sound_density_watchouts_map") or {},
        str(updated.get("preferred_sound_density", "") or ""),
        max_chars=40,
    )
    updated["preferred_voice_pacing_bias"] = _catalyst_pick_preferred_choice(
        updated.get("voice_pacing_bias_wins_map") or {},
        updated.get("voice_pacing_bias_watchouts_map") or {},
        str(updated.get("preferred_voice_pacing_bias", "") or ""),
        max_chars=60,
    )
    updated["preferred_visual_variation_rule"] = _catalyst_pick_preferred_choice(
        updated.get("visual_variation_rule_wins_map") or {},
        updated.get("visual_variation_rule_watchouts_map") or {},
        str(updated.get("preferred_visual_variation_rule", "") or ""),
        max_chars=180,
    )
    public = _catalyst_channel_memory_public_view(updated)
    updated["summary"] = _clip_text(
        "Catalyst now has "
        + f"{int(public.get('run_count', 0) or 0)} generated run{'s' if int(public.get('run_count', 0) or 0) != 1 else ''} "
        + f"and {int(public.get('outcome_count', 0) or 0)} measured outcome{'s' if int(public.get('outcome_count', 0) or 0) != 1 else ''} on this channel lane. "
        + (f"Avg CTR {float(public.get('average_ctr', 0.0) or 0.0):.2f}%. " if float(public.get("average_ctr", 0.0) or 0.0) > 0 else "")
        + (f"Avg viewed {float(public.get('average_average_percentage_viewed', 0.0) or 0.0):.2f}%. " if float(public.get("average_average_percentage_viewed", 0.0) or 0.0) > 0 else "")
        + (f"Reference playbook average {float(public.get('average_reference_overall_score', 0.0) or 0.0):.1f}/100. " if float(public.get("average_reference_overall_score", 0.0) or 0.0) > 0 else "")
        + ("Best hook win: " + str((list(public.get("hook_wins") or []) or [""])[0]) + ". " if list(public.get("hook_wins") or []) else "")
        + ("Primary watchout: " + str((list(public.get("retention_watchouts") or []) or [""])[0]) + ". " if list(public.get("retention_watchouts") or []) else "")
        + ("Reference rewrite pressure: " + str((list(public.get("reference_packaging_rewrites") or list(public.get("reference_hook_rewrites") or [])) or [""])[0]) + "." if list(public.get("reference_packaging_rewrites") or list(public.get("reference_hook_rewrites") or [])) else ""),
        320,
    )
    outcome_title = str(outcome_record.get("title_used", "") or "").strip()
    outcome_source_title = str(((dict(session_snapshot.get("metadata_pack") or {})).get("source_video") or {}).get("title", "") or "").strip()
    series_anchor = _catalyst_extract_series_anchor(
        outcome_title,
        outcome_source_title,
        str(session_snapshot.get("topic", "") or ""),
        niche_key=str(updated.get("niche_key", "") or ""),
    ) or str(selected_cluster.get("series_anchor", "") or "").strip() or str(updated.get("series_anchor", "") or "").strip()
    if series_anchor:
        series_map = dict(updated.get("series_memory_map") or {})
        series_key = _catalyst_series_memory_key(series_anchor)
        series_bucket = dict(series_map.get(series_key) or {})
        series_bucket["series_anchor"] = series_anchor
        series_bucket["channel_id"] = str(updated.get("channel_id", "") or "")
        series_bucket["format_preset"] = format_preset
        series_bucket["niche_key"] = str(updated.get("niche_key", "") or "")
        series_bucket["niche_label"] = str(updated.get("niche_label", "") or "")
        series_bucket["niche_confidence"] = round(float(updated.get("niche_confidence", 0.0) or 0.0), 2)
        series_bucket["niche_keywords"] = _dedupe_preserve_order([*list(updated.get("niche_keywords") or []), *list(series_bucket.get("niche_keywords") or [])], max_items=8, max_chars=40)
        series_bucket["niche_follow_up_rule"] = str(updated.get("niche_follow_up_rule", "") or "")
        series_bucket["run_count"] = max(int(series_bucket.get("run_count", 0) or 0), 1)
        series_bucket["outcome_count"] = int(series_bucket.get("outcome_count", 0) or 0) + 1
        series_bucket["last_session_id"] = str(session_snapshot.get("session_id", "") or "")
        series_bucket["updated_at"] = time.time()
        series_bucket["last_outcome_summary"] = _clip_text(str(outcome_record.get("operator_summary", "") or ""), 320)
        series_bucket["recent_selected_titles"] = _dedupe_preserve_order([outcome_title, *list(series_bucket.get("recent_selected_titles") or [])], max_items=10, max_chars=160)
        series_bucket["recent_source_titles"] = _dedupe_preserve_order([outcome_source_title, *list(series_bucket.get("recent_source_titles") or [])], max_items=10, max_chars=160)
        series_bucket["proven_keywords"] = _dedupe_preserve_order([
            *_extract_catalyst_keywords(outcome_title, *list(outcome_record.get("tags") or [])),
            *list(series_bucket.get("proven_keywords") or []),
        ], max_items=14, max_chars=80)
        for field, metric_key in (
            ("outcome_views_sum", "views"),
            ("outcome_impressions_sum", "impressions"),
            ("outcome_ctr_sum", "impression_click_through_rate"),
            ("outcome_avp_sum", "average_percentage_viewed"),
            ("outcome_avd_sum", "average_view_duration_sec"),
            ("outcome_first30_sum", "first_30_sec_retention_pct"),
            ("outcome_first60_sum", "first_60_sec_retention_pct"),
        ):
            series_bucket[field] = float(series_bucket.get(field, 0.0) or 0.0) + float(metrics.get(metric_key, 0.0) or 0.0)
        for field, metric_key in (
            ("reference_overall_score_sum", "overall"),
            ("reference_hook_score_sum", "hook"),
            ("reference_pacing_score_sum", "pacing"),
            ("reference_visual_score_sum", "visuals"),
            ("reference_sound_score_sum", "sound"),
            ("reference_packaging_score_sum", "packaging"),
            ("reference_title_novelty_score_sum", "title_novelty"),
        ):
            series_bucket[field] = float(series_bucket.get(field, 0.0) or 0.0) + float(reference_scores.get(metric_key, 0.0) or 0.0)
        for field, metric_key in (
            ("execution_overall_score_sum", "overall"),
            ("execution_hook_score_sum", "hook"),
            ("execution_pacing_score_sum", "pacing"),
            ("execution_visual_score_sum", "visuals"),
            ("execution_sound_score_sum", "sound"),
            ("execution_packaging_score_sum", "packaging"),
        ):
            series_bucket[field] = float(series_bucket.get(field, 0.0) or 0.0) + float(execution_scores.get(metric_key, 0.0) or 0.0)
        series_bucket["hook_learnings"] = _dedupe_preserve_order([*list(outcome_record.get("hook_wins") or []), *list(series_bucket.get("hook_learnings") or [])], max_items=10, max_chars=180)
        series_bucket["pacing_learnings"] = _dedupe_preserve_order([*list(outcome_record.get("pacing_wins") or []), *list(series_bucket.get("pacing_learnings") or [])], max_items=10, max_chars=180)
        series_bucket["visual_learnings"] = _dedupe_preserve_order([*list(outcome_record.get("visual_wins") or []), *list(series_bucket.get("visual_learnings") or [])], max_items=10, max_chars=180)
        series_bucket["sound_learnings"] = _dedupe_preserve_order([*list(outcome_record.get("sound_wins") or []), *list(series_bucket.get("sound_learnings") or [])], max_items=10, max_chars=180)
        series_bucket["packaging_learnings"] = _dedupe_preserve_order([*list(outcome_record.get("packaging_wins") or []), *list(series_bucket.get("packaging_learnings") or [])], max_items=10, max_chars=180)
        series_bucket["retention_watchouts"] = _dedupe_preserve_order([*list(outcome_record.get("retention_watchouts") or []), *list(series_bucket.get("retention_watchouts") or [])], max_items=10, max_chars=180)
        series_bucket["next_video_moves"] = _dedupe_preserve_order([*list(outcome_record.get("next_video_moves") or []), *list(series_bucket.get("next_video_moves") or [])], max_items=10, max_chars=180)
        series_bucket["reference_benchmark_channels"] = _dedupe_preserve_order([
            *list(reference_comparison.get("benchmark_channels") or []),
            *list(series_bucket.get("reference_benchmark_channels") or []),
        ], max_items=8, max_chars=80)
        series_bucket["reference_tier"] = str(reference_comparison.get("tier", "") or series_bucket.get("reference_tier", "") or "")
        series_bucket["last_reference_summary"] = _clip_text(str(reference_comparison.get("reference_summary", "") or series_bucket.get("last_reference_summary", "") or ""), 360)
        for field, items in (
            ("hook_wins_map", list(outcome_record.get("hook_wins") or [])),
            ("hook_watchouts_map", list(outcome_record.get("hook_watchouts") or [])),
            ("pacing_wins_map", list(outcome_record.get("pacing_wins") or [])),
            ("pacing_watchouts_map", list(outcome_record.get("pacing_watchouts") or [])),
            ("visual_wins_map", list(outcome_record.get("visual_wins") or [])),
            ("visual_watchouts_map", list(outcome_record.get("visual_watchouts") or [])),
            ("sound_wins_map", list(outcome_record.get("sound_wins") or [])),
            ("sound_watchouts_map", list(outcome_record.get("sound_watchouts") or [])),
            ("packaging_wins_map", list(outcome_record.get("packaging_wins") or [])),
            ("packaging_watchouts_map", list(outcome_record.get("packaging_watchouts") or [])),
            ("retention_wins_map", list(outcome_record.get("retention_wins") or [])),
            ("retention_watchouts_map", list(outcome_record.get("retention_watchouts") or [])),
            ("next_video_moves_map", list(outcome_record.get("next_video_moves") or [])),
            ("reference_hook_rewrites_map", list(reference_comparison.get("hook_rewrites") or [])),
            ("reference_pacing_rewrites_map", list(reference_comparison.get("pacing_rewrites") or [])),
            ("reference_visual_rewrites_map", list(reference_comparison.get("visual_rewrites") or [])),
            ("reference_sound_rewrites_map", list(reference_comparison.get("sound_rewrites") or [])),
            ("reference_packaging_rewrites_map", list(reference_comparison.get("packaging_rewrites") or [])),
            ("reference_next_video_moves_map", list(reference_comparison.get("next_run_moves") or [])),
        ):
            _catalyst_update_weighted_signals(series_bucket, field, items, weight)
        _catalyst_update_weighted_signals(series_bucket, "next_video_moves_map", list(reference_comparison.get("next_run_moves") or []), weight * 0.8)
        for field_name, payload in (
            ("opening_intensity", execution_signals.get("opening_intensity") or {}),
            ("interrupt_strength", execution_signals.get("interrupt_strength") or {}),
            ("caption_rhythm", execution_signals.get("caption_rhythm") or {}),
            ("sound_density", execution_signals.get("sound_density") or {}),
            ("cut_profile", execution_signals.get("cut_profile") or {}),
            ("voice_pacing_bias", execution_signals.get("voice_pacing_bias") or {}),
            ("visual_variation_rule", execution_signals.get("visual_variation_rule") or {}),
            ("execution_profile", execution_signals.get("execution_profile") or {}),
        ):
            _catalyst_update_weighted_signals(series_bucket, f"{field_name}_wins_map", list(payload.get("wins") or []), weight)
            _catalyst_update_weighted_signals(series_bucket, f"{field_name}_watchouts_map", list(payload.get("watchouts") or []), weight)
        series_public = _catalyst_channel_memory_public_view(series_bucket, series_anchor_override=series_anchor)
        series_bucket["summary"] = _clip_text(
            f"Catalyst now has {int(series_public.get('outcome_count', 0) or 0)} measured outcome{'s' if int(series_public.get('outcome_count', 0) or 0) != 1 else ''} inside {series_anchor}. "
            + (f"Avg CTR {float(series_public.get('average_ctr', 0.0) or 0.0):.2f}%. " if float(series_public.get("average_ctr", 0.0) or 0.0) > 0 else "")
            + ("Best hook win: " + str((list(series_public.get("hook_wins") or []) or [""])[0]) + ". " if list(series_public.get("hook_wins") or []) else "")
            + ("Primary watchout: " + str((list(series_public.get("retention_watchouts") or []) or [""])[0]) + ". " if list(series_public.get("retention_watchouts") or []) else "")
            + ("Reference rewrite pressure: " + str((list(series_public.get("reference_packaging_rewrites") or list(series_public.get("reference_hook_rewrites") or [])) or [""])[0]) + "." if list(series_public.get("reference_packaging_rewrites") or list(series_public.get("reference_hook_rewrites") or [])) else ""),
            320,
        )
        series_map[series_key] = series_bucket
        updated["series_memory_map"] = series_map
    archetype_key = str(updated.get("archetype_key", "") or "").strip()
    archetype_label = str(updated.get("archetype_label", "") or archetype_key or "").strip()
    if archetype_key:
        archetype_map = dict(updated.get("archetype_memory_map") or {})
        archetype_memory_key = _catalyst_archetype_memory_key(archetype_key)
        archetype_bucket = dict(archetype_map.get(archetype_memory_key) or {})
        archetype_bucket["archetype_key"] = archetype_key
        archetype_bucket["archetype_label"] = archetype_label
        archetype_bucket["channel_id"] = str(updated.get("channel_id", "") or "")
        archetype_bucket["format_preset"] = format_preset
        archetype_bucket["run_count"] = max(int(archetype_bucket.get("run_count", 0) or 0), 1)
        archetype_bucket["outcome_count"] = int(archetype_bucket.get("outcome_count", 0) or 0) + 1
        archetype_bucket["updated_at"] = time.time()
        archetype_bucket["last_session_id"] = str(session_snapshot.get("session_id", "") or "")
        archetype_bucket["last_outcome_summary"] = _clip_text(str(outcome_record.get("operator_summary", "") or ""), 320)
        archetype_bucket["recent_selected_titles"] = _dedupe_preserve_order(
            [outcome_title, *list(archetype_bucket.get("recent_selected_titles") or [])],
            max_items=10,
            max_chars=160,
        )
        archetype_bucket["recent_source_titles"] = _dedupe_preserve_order(
            [outcome_source_title, *list(archetype_bucket.get("recent_source_titles") or [])],
            max_items=10,
            max_chars=160,
        )
        archetype_bucket["proven_keywords"] = _dedupe_preserve_order(
            [
                *_extract_catalyst_keywords(outcome_title, *list(outcome_record.get("tags") or [])),
                *list(archetype_bucket.get("proven_keywords") or []),
            ],
            max_items=14,
            max_chars=80,
        )
        archetype_bucket["series_anchors"] = _dedupe_preserve_order(
            [series_anchor, *list(archetype_bucket.get("series_anchors") or [])],
            max_items=8,
            max_chars=80,
        )
        for field, metric_key in (
            ("outcome_views_sum", "views"),
            ("outcome_impressions_sum", "impressions"),
            ("outcome_ctr_sum", "impression_click_through_rate"),
            ("outcome_avp_sum", "average_percentage_viewed"),
            ("outcome_avd_sum", "average_view_duration_sec"),
            ("outcome_first30_sum", "first_30_sec_retention_pct"),
            ("outcome_first60_sum", "first_60_sec_retention_pct"),
        ):
            archetype_bucket[field] = float(archetype_bucket.get(field, 0.0) or 0.0) + float(metrics.get(metric_key, 0.0) or 0.0)
        for field, metric_key in (
            ("reference_overall_score_sum", "overall"),
            ("reference_hook_score_sum", "hook"),
            ("reference_pacing_score_sum", "pacing"),
            ("reference_visual_score_sum", "visuals"),
            ("reference_sound_score_sum", "sound"),
            ("reference_packaging_score_sum", "packaging"),
            ("reference_title_novelty_score_sum", "title_novelty"),
        ):
            archetype_bucket[field] = float(archetype_bucket.get(field, 0.0) or 0.0) + float(reference_scores.get(metric_key, 0.0) or 0.0)
        for field, metric_key in (
            ("execution_overall_score_sum", "overall"),
            ("execution_hook_score_sum", "hook"),
            ("execution_pacing_score_sum", "pacing"),
            ("execution_visual_score_sum", "visuals"),
            ("execution_sound_score_sum", "sound"),
            ("execution_packaging_score_sum", "packaging"),
        ):
            archetype_bucket[field] = float(archetype_bucket.get(field, 0.0) or 0.0) + float(execution_scores.get(metric_key, 0.0) or 0.0)
        archetype_bucket["hook_learnings"] = _dedupe_preserve_order([*list(outcome_record.get("hook_wins") or []), *list(archetype_bucket.get("hook_learnings") or [])], max_items=10, max_chars=180)
        archetype_bucket["pacing_learnings"] = _dedupe_preserve_order([*list(outcome_record.get("pacing_wins") or []), *list(archetype_bucket.get("pacing_learnings") or [])], max_items=10, max_chars=180)
        archetype_bucket["visual_learnings"] = _dedupe_preserve_order([*list(outcome_record.get("visual_wins") or []), *list(archetype_bucket.get("visual_learnings") or [])], max_items=10, max_chars=180)
        archetype_bucket["sound_learnings"] = _dedupe_preserve_order([*list(outcome_record.get("sound_wins") or []), *list(archetype_bucket.get("sound_learnings") or [])], max_items=10, max_chars=180)
        archetype_bucket["packaging_learnings"] = _dedupe_preserve_order([*list(outcome_record.get("packaging_wins") or []), *list(archetype_bucket.get("packaging_learnings") or [])], max_items=10, max_chars=180)
        archetype_bucket["retention_watchouts"] = _dedupe_preserve_order([*list(outcome_record.get("retention_watchouts") or []), *list(archetype_bucket.get("retention_watchouts") or [])], max_items=10, max_chars=180)
        archetype_bucket["next_video_moves"] = _dedupe_preserve_order([*list(outcome_record.get("next_video_moves") or []), *list(archetype_bucket.get("next_video_moves") or [])], max_items=10, max_chars=180)
        archetype_bucket["reference_benchmark_channels"] = _dedupe_preserve_order(
            [*list(reference_comparison.get("benchmark_channels") or []), *list(archetype_bucket.get("reference_benchmark_channels") or [])],
            max_items=8,
            max_chars=80,
        )
        archetype_bucket["reference_tier"] = str(reference_comparison.get("tier", "") or archetype_bucket.get("reference_tier", "") or "")
        archetype_bucket["last_reference_summary"] = _clip_text(
            str(reference_comparison.get("reference_summary", "") or archetype_bucket.get("last_reference_summary", "") or ""),
            360,
        )
        for field, items in (
            ("hook_wins_map", list(outcome_record.get("hook_wins") or [])),
            ("hook_watchouts_map", list(outcome_record.get("hook_watchouts") or [])),
            ("pacing_wins_map", list(outcome_record.get("pacing_wins") or [])),
            ("pacing_watchouts_map", list(outcome_record.get("pacing_watchouts") or [])),
            ("visual_wins_map", list(outcome_record.get("visual_wins") or [])),
            ("visual_watchouts_map", list(outcome_record.get("visual_watchouts") or [])),
            ("sound_wins_map", list(outcome_record.get("sound_wins") or [])),
            ("sound_watchouts_map", list(outcome_record.get("sound_watchouts") or [])),
            ("packaging_wins_map", list(outcome_record.get("packaging_wins") or [])),
            ("packaging_watchouts_map", list(outcome_record.get("packaging_watchouts") or [])),
            ("retention_wins_map", list(outcome_record.get("retention_wins") or [])),
            ("retention_watchouts_map", list(outcome_record.get("retention_watchouts") or [])),
            ("next_video_moves_map", list(outcome_record.get("next_video_moves") or [])),
            ("reference_hook_rewrites_map", list(reference_comparison.get("hook_rewrites") or [])),
            ("reference_pacing_rewrites_map", list(reference_comparison.get("pacing_rewrites") or [])),
            ("reference_visual_rewrites_map", list(reference_comparison.get("visual_rewrites") or [])),
            ("reference_sound_rewrites_map", list(reference_comparison.get("sound_rewrites") or [])),
            ("reference_packaging_rewrites_map", list(reference_comparison.get("packaging_rewrites") or [])),
            ("reference_next_video_moves_map", list(reference_comparison.get("next_run_moves") or [])),
        ):
            _catalyst_update_weighted_signals(archetype_bucket, field, items, weight)
        _catalyst_update_weighted_signals(archetype_bucket, "next_video_moves_map", list(reference_comparison.get("next_run_moves") or []), weight * 0.8)
        for field_name, payload in (
            ("opening_intensity", execution_signals.get("opening_intensity") or {}),
            ("interrupt_strength", execution_signals.get("interrupt_strength") or {}),
            ("caption_rhythm", execution_signals.get("caption_rhythm") or {}),
            ("sound_density", execution_signals.get("sound_density") or {}),
            ("cut_profile", execution_signals.get("cut_profile") or {}),
            ("voice_pacing_bias", execution_signals.get("voice_pacing_bias") or {}),
            ("visual_variation_rule", execution_signals.get("visual_variation_rule") or {}),
            ("execution_profile", execution_signals.get("execution_profile") or {}),
        ):
            _catalyst_update_weighted_signals(archetype_bucket, f"{field_name}_wins_map", list(payload.get("wins") or []), weight)
            _catalyst_update_weighted_signals(archetype_bucket, f"{field_name}_watchouts_map", list(payload.get("watchouts") or []), weight)
        archetype_bucket["summary"] = _clip_text(
            f"Catalyst now has {int(archetype_bucket.get('outcome_count', 0) or 0)} measured outcome{'s' if int(archetype_bucket.get('outcome_count', 0) or 0) != 1 else ''} inside the {archetype_label} archetype. "
            + (f"Avg CTR {(float(archetype_bucket.get('outcome_ctr_sum', 0.0) or 0.0) / max(int(archetype_bucket.get('outcome_count', 0) or 0), 1)):.2f}%. " if int(archetype_bucket.get("outcome_count", 0) or 0) > 0 else "")
            + ("Series anchors: " + ", ".join(list(archetype_bucket.get("series_anchors") or [])[:3]) + ". " if list(archetype_bucket.get("series_anchors") or []) else "")
            + ("Reference rewrite pressure: " + str((list(reference_comparison.get("packaging_rewrites") or list(reference_comparison.get("hook_rewrites") or [])) or [""])[0]) + "." if list(reference_comparison.get("packaging_rewrites") or list(reference_comparison.get("hook_rewrites") or [])) else ""),
            320,
        )
        archetype_map[archetype_memory_key] = archetype_bucket
        updated["archetype_memory_map"] = archetype_map
    return updated
