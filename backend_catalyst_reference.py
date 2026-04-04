from backend_catalyst_core import (
    _clip_text,
    _dedupe_preserve_order,
    _catalyst_channel_memory_public_view,
    _catalyst_infer_archetype,
    _catalyst_metric_score,
    _catalyst_pressure_label,
    _catalyst_reference_score_tier,
    _catalyst_reference_signal_list,
    _catalyst_rewrite_pressure_profile,
    _catalyst_signal_balance_score,
    _catalyst_text_overlap_score,
    _catalyst_title_novelty_score,
    _extract_catalyst_keywords,
)

def _render_catalyst_channel_memory_context(memory: dict | None, series_anchor_override: str = "") -> str:
    public = _catalyst_channel_memory_public_view(memory, series_anchor_override=series_anchor_override)
    if not any(public.values()):
        return ""
    parts: list[str] = []
    if public.get("summary"):
        parts.append("Catalyst channel memory summary: " + _clip_text(str(public.get("summary", "")), 320))
    if public.get("operator_summary"):
        parts.append("Operator directive summary: " + _clip_text(str(public.get("operator_summary", "") or ""), 320))
    if public.get("operator_mission"):
        parts.append("Operator mission: " + _clip_text(str(public.get("operator_mission", "") or ""), 220))
    if public.get("operator_guardrails"):
        parts.append("Operator guardrails: " + "; ".join(list(public.get("operator_guardrails") or [])[:5]))
    if public.get("operator_target_niches"):
        parts.append("Priority niches: " + ", ".join(list(public.get("operator_target_niches") or [])[:6]))
    if public.get("series_anchor"):
        parts.append("Series anchor to preserve: " + _clip_text(str(public.get("series_anchor", "") or ""), 120))
    if public.get("niche_label"):
        niche_line = f"Detected niche: {str(public.get('niche_label', '')).strip()}"
        if public.get("niche_keywords"):
            niche_line += " (" + ", ".join(list(public.get("niche_keywords") or [])[:6]) + ")"
        parts.append(niche_line + ".")
    if public.get("archetype_label"):
        archetype_line = f"Detected Catalyst archetype: {str(public.get('archetype_label', '')).strip()}"
        if public.get("archetype_keywords"):
            archetype_line += " (" + ", ".join(list(public.get("archetype_keywords") or [])[:6]) + ")"
        parts.append(archetype_line + ".")
    if public.get("niche_follow_up_rule"):
        parts.append("Niche follow-up rule: " + _clip_text(str(public.get("niche_follow_up_rule", "") or ""), 220))
    if public.get("archetype_hook_rule"):
        parts.append("Archetype hook rule: " + _clip_text(str(public.get("archetype_hook_rule", "") or ""), 220))
    if public.get("archetype_visual_rule"):
        parts.append("Archetype visual rule: " + _clip_text(str(public.get("archetype_visual_rule", "") or ""), 220))
    if public.get("archetype_packaging_rule"):
        parts.append("Archetype packaging rule: " + _clip_text(str(public.get("archetype_packaging_rule", "") or ""), 220))
    if int(public.get("outcome_count", 0) or 0) > 0:
        parts.append(
            f"Measured outcomes logged: {int(public.get('outcome_count', 0) or 0)}. "
            f"Avg CTR {float(public.get('average_ctr', 0.0) or 0.0):.2f}%. "
            f"Avg viewed {float(public.get('average_average_percentage_viewed', 0.0) or 0.0):.2f}%."
        )
    if public.get("series_memory_summary"):
        parts.append("Series-memory promotion state: " + _clip_text(str(public.get("series_memory_summary", "") or ""), 320))
    if public.get("archetype_memory_summary"):
        parts.append("Archetype promotion state: " + _clip_text(str(public.get("archetype_memory_summary", "") or ""), 320))
    if public.get("promoted_arcs"):
        parts.append("Promoted arcs from measured outcomes: " + ", ".join(list(public.get("promoted_arcs") or [])[:3]))
    if public.get("demoted_arcs"):
        parts.append("Demoted arcs from measured outcomes: " + ", ".join(list(public.get("demoted_arcs") or [])[:3]))
    if public.get("promoted_archetypes"):
        parts.append("Promoted archetypes from measured outcomes: " + ", ".join(list(public.get("promoted_archetypes") or [])[:3]))
    if public.get("demoted_archetypes"):
        parts.append("Demoted archetypes from measured outcomes: " + ", ".join(list(public.get("demoted_archetypes") or [])[:3]))
    if public.get("proven_keywords"):
        parts.append("Proven arena keywords: " + ", ".join(list(public.get("proven_keywords") or [])[:8]))
    if public.get("hook_wins"):
        parts.append("Weighted hook wins: " + "; ".join(list(public.get("hook_wins") or [])[:4]))
    if public.get("hook_watchouts"):
        parts.append("Hook watchouts: " + "; ".join(list(public.get("hook_watchouts") or [])[:4]))
    if public.get("pacing_wins"):
        parts.append("Weighted pacing wins: " + "; ".join(list(public.get("pacing_wins") or [])[:4]))
    if public.get("pacing_watchouts"):
        parts.append("Pacing watchouts: " + "; ".join(list(public.get("pacing_watchouts") or [])[:4]))
    if public.get("visual_wins"):
        parts.append("Visual wins: " + "; ".join(list(public.get("visual_wins") or [])[:4]))
    if public.get("visual_watchouts"):
        parts.append("Visual watchouts: " + "; ".join(list(public.get("visual_watchouts") or [])[:4]))
    if public.get("sound_wins"):
        parts.append("Sound wins: " + "; ".join(list(public.get("sound_wins") or [])[:4]))
    if public.get("sound_watchouts"):
        parts.append("Sound watchouts: " + "; ".join(list(public.get("sound_watchouts") or [])[:4]))
    if public.get("packaging_wins"):
        parts.append("Packaging wins: " + "; ".join(list(public.get("packaging_wins") or [])[:4]))
    if public.get("packaging_watchouts"):
        parts.append("Packaging watchouts: " + "; ".join(list(public.get("packaging_watchouts") or [])[:4]))
    if public.get("retention_wins"):
        parts.append("Retention wins: " + "; ".join(list(public.get("retention_wins") or [])[:4]))
    if public.get("retention_watchouts"):
        parts.append("Retention watchouts: " + "; ".join(list(public.get("retention_watchouts") or [])[:4]))
    if public.get("reference_summary"):
        parts.append("Reference playbook delta: " + _clip_text(str(public.get("reference_summary", "") or ""), 280))
    if public.get("reference_hook_rewrites"):
        parts.append("Reference-backed hook rewrites: " + "; ".join(list(public.get("reference_hook_rewrites") or [])[:3]))
    if public.get("reference_pacing_rewrites"):
        parts.append("Reference-backed pacing rewrites: " + "; ".join(list(public.get("reference_pacing_rewrites") or [])[:3]))
    if public.get("reference_visual_rewrites"):
        parts.append("Reference-backed visual rewrites: " + "; ".join(list(public.get("reference_visual_rewrites") or [])[:3]))
    if public.get("reference_sound_rewrites"):
        parts.append("Reference-backed sound rewrites: " + "; ".join(list(public.get("reference_sound_rewrites") or [])[:3]))
    if public.get("reference_packaging_rewrites"):
        parts.append("Reference-backed packaging rewrites: " + "; ".join(list(public.get("reference_packaging_rewrites") or [])[:3]))
    if public.get("reference_next_video_moves"):
        parts.append("Reference-backed next moves: " + "; ".join(list(public.get("reference_next_video_moves") or [])[:4]))
    rewrite_pressure = dict(public.get("rewrite_pressure") or {})
    if rewrite_pressure.get("summary"):
        parts.append("Rewrite pressure summary: " + _clip_text(str(rewrite_pressure.get("summary", "") or ""), 280))
    if rewrite_pressure.get("next_run_priorities"):
        parts.append("Rewrite pressure priorities: " + "; ".join(list(rewrite_pressure.get("next_run_priorities") or [])[:4]))
    if public.get("next_video_moves"):
        parts.append("Next-video moves: " + "; ".join(list(public.get("next_video_moves") or [])[:4]))
    if public.get("last_outcome_summary"):
        parts.append("Latest measured outcome: " + _clip_text(str(public.get("last_outcome_summary", "")), 220))
    return "\n".join(part for part in parts if part)


def _reference_channel_archetype(entry: dict | None) -> dict:
    payload = dict(entry or {})
    seed = dict(payload.get("seed") or {})
    channel = dict(payload.get("channel") or {})
    memory = dict(payload.get("memory_seed") or {})
    text_parts = [
        str(seed.get("niche", "") or ""),
        str(seed.get("style_notes", "") or ""),
        str(channel.get("title", "") or ""),
        " ".join(str(v).strip() for v in list(payload.get("recent_upload_titles") or [])[:8] if str(v).strip()),
        " ".join(str(v).strip() for v in list(payload.get("top_video_titles") or [])[:8] if str(v).strip()),
        " ".join(str(v).strip() for v in list(memory.get("proven_keywords") or [])[:8] if str(v).strip()),
    ]
    return _catalyst_infer_archetype(" ".join(part for part in text_parts if part), format_preset="documentary")


def _build_catalyst_reference_playbook(
    *,
    reference_memory: dict | None = None,
    format_preset: str = "documentary",
    topic: str = "",
    channel_memory: dict | None = None,
    selected_cluster: dict | None = None,
) -> dict:
    if str(format_preset or "").strip().lower() != "documentary":
        return {}
    payload = dict(reference_memory or {})
    if not payload:
        return {}
    aggregate = dict(payload.get("aggregate_memory_seed") or {})
    channels = [dict(item) for item in list(payload.get("channels") or []) if isinstance(item, dict)]
    cluster = dict(selected_cluster or {})
    memory_public = _catalyst_channel_memory_public_view(channel_memory)
    target_archetype_key = str(
        memory_public.get("archetype_key", "")
        or cluster.get("archetype_key", "")
        or _catalyst_infer_archetype(
            topic,
            " ".join(str(v).strip() for v in list(cluster.get("sample_titles") or []) if str(v).strip()),
            niche_key=str(memory_public.get("niche_key", "") or cluster.get("niche_key", "") or ""),
            format_preset=format_preset,
        ).get("key", "")
        or ""
    ).strip().lower()
    target_archetype_label = str(memory_public.get("archetype_label", "") or cluster.get("archetype_label", "") or "").strip()
    target_keywords = set(
        str(v).strip().lower()
        for v in [
            *_extract_catalyst_keywords(str(topic or ""), max_items=12),
            *list(memory_public.get("proven_keywords") or [])[:8],
            *list(cluster.get("keywords") or [])[:8],
        ]
        if str(v).strip()
    )
    ranked_channels: list[tuple[float, dict, dict]] = []
    for entry in channels:
        seed = dict(entry.get("seed") or {})
        memory = dict(entry.get("memory_seed") or {})
        entry_archetype = _reference_channel_archetype(entry)
        entry_archetype_key = str(entry_archetype.get("key", "") or "").strip().lower()
        searchable = " ".join(
            [
                str(seed.get("niche", "") or ""),
                str(seed.get("style_notes", "") or ""),
                " ".join(list(entry.get("recent_upload_titles") or [])),
                " ".join(list(entry.get("top_video_titles") or [])),
                " ".join(list(memory.get("proven_keywords") or [])),
            ]
        ).lower()
        searchable_tokens = set(_extract_catalyst_keywords(searchable, max_items=24))
        score = 0.0
        if target_keywords and searchable_tokens:
            score += len(target_keywords & searchable_tokens) * 2.0
        if target_archetype_key and entry_archetype_key == target_archetype_key:
            score += 12.0
        elif target_archetype_key and entry_archetype_key:
            score -= 2.0
        score += min(6.0, float(entry.get("average_top_video_views", 0) or 0) / 400000.0)
        ranked_channels.append((score, entry, entry_archetype))
    ranked_channels.sort(
        key=lambda item: (
            -item[0],
            -float((item[1] or {}).get("average_top_video_views", 0) or 0.0),
            str((((item[1] or {}).get("channel") or {}).get("title")) or "").lower(),
        )
    )
    chosen = [entry for _score, entry, _arch in ranked_channels[:4]]
    chosen_arches = [arch for _score, _entry, arch in ranked_channels[:4]]
    chosen_titles = _dedupe_preserve_order(
        [
            str(((entry.get("channel") or {}).get("title")) or "").strip()
            for entry in chosen
            if str(((entry.get("channel") or {}).get("title")) or "").strip()
        ],
        max_items=4,
        max_chars=120,
    )
    matched_archetypes = _dedupe_preserve_order(
        [
            str(arch.get("label", "") or arch.get("key", "") or "").strip()
            for arch in chosen_arches
            if str(arch.get("label", "") or arch.get("key", "") or "").strip()
        ],
        max_items=4,
        max_chars=80,
    )
    hook_rewrites = _dedupe_preserve_order(
        [
            *_catalyst_reference_signal_list(chosen, "hook_learnings", max_items=6),
            *[str(v).strip() for v in list(aggregate.get("hook_learnings") or [])[:3] if str(v).strip()],
        ],
        max_items=8,
        max_chars=180,
    )
    pacing_rewrites = _dedupe_preserve_order(
        [
            *_catalyst_reference_signal_list(chosen, "pacing_learnings", max_items=6),
            *[str(v).strip() for v in list(aggregate.get("pacing_learnings") or [])[:3] if str(v).strip()],
        ],
        max_items=8,
        max_chars=180,
    )
    visual_rewrites = _dedupe_preserve_order(
        [
            *_catalyst_reference_signal_list(chosen, "visual_learnings", max_items=6),
            *[str(v).strip() for v in list(aggregate.get("visual_learnings") or [])[:3] if str(v).strip()],
        ],
        max_items=8,
        max_chars=180,
    )
    sound_rewrites = _dedupe_preserve_order(
        [
            *_catalyst_reference_signal_list(chosen, "sound_learnings", max_items=6),
            *[str(v).strip() for v in list(aggregate.get("sound_learnings") or [])[:3] if str(v).strip()],
        ],
        max_items=8,
        max_chars=180,
    )
    packaging_rewrites = _dedupe_preserve_order(
        [
            *_catalyst_reference_signal_list(chosen, "packaging_learnings", max_items=6),
            *[str(v).strip() for v in list(aggregate.get("packaging_learnings") or [])[:3] if str(v).strip()],
        ],
        max_items=8,
        max_chars=180,
    )
    next_moves = _dedupe_preserve_order(
        [
            *_catalyst_reference_signal_list(chosen, "next_video_moves", max_items=6),
            *[str(v).strip() for v in list(aggregate.get("next_video_moves") or [])[:4] if str(v).strip()],
        ],
        max_items=10,
        max_chars=180,
    )
    proven_keywords = _dedupe_preserve_order(
        [
            *[str(v).strip() for v in list(aggregate.get("proven_keywords") or [])[:10] if str(v).strip()],
            *[str(v).strip() for v in list(memory_public.get("proven_keywords") or [])[:8] if str(v).strip()],
            *[str(v).strip() for v in list(cluster.get("keywords") or [])[:8] if str(v).strip()],
        ],
        max_items=12,
        max_chars=60,
    )
    summary = _clip_text(
        " ".join(
            part
            for part in [
                f"Reference archetype target: {target_archetype_label or target_archetype_key}."
                if (target_archetype_label or target_archetype_key)
                else "",
                f"Matched reference channels: {', '.join(chosen_titles[:3])}."
                if chosen_titles
                else "",
                f"Matched reference archetypes: {', '.join(matched_archetypes[:3])}."
                if matched_archetypes
                else "",
                ("Reference packaging rule: " + packaging_rewrites[0] + ".") if packaging_rewrites else "",
                ("Reference hook rule: " + hook_rewrites[0] + ".") if hook_rewrites else "",
            ]
            if part
        ),
        320,
    )
    return {
        "summary": summary,
        "benchmark_channels": chosen_titles,
        "matched_archetypes": matched_archetypes,
        "target_archetype_key": target_archetype_key,
        "hook_rewrites": hook_rewrites,
        "pacing_rewrites": pacing_rewrites,
        "visual_rewrites": visual_rewrites,
        "sound_rewrites": sound_rewrites,
        "packaging_rewrites": packaging_rewrites,
        "next_video_moves": next_moves,
        "proven_keywords": proven_keywords,
    }


def _select_catalyst_reference_channels(reference_memory: dict | None = None, format_preset: str = "documentary", topic: str = "", channel_memory: dict | None = None, selected_cluster: dict | None = None) -> tuple[dict, list[dict]]:
    if str(format_preset or "").strip().lower() != "documentary":
        return {}, []
    payload = dict(reference_memory or {})
    if not payload:
        return {}, []
    aggregate = dict(payload.get("aggregate_memory_seed") or {})
    playbook = _build_catalyst_reference_playbook(
        reference_memory=reference_memory,
        format_preset=format_preset,
        topic=topic,
        channel_memory=channel_memory,
        selected_cluster=selected_cluster,
    )
    chosen_titles = set(str(v).strip() for v in list(playbook.get("benchmark_channels") or []) if str(v).strip())
    chosen = [
        dict(item)
        for item in list(payload.get("channels") or [])
        if isinstance(item, dict)
        and str(((item.get("channel") or {}).get("title")) or "").strip() in chosen_titles
    ]
    return aggregate, chosen


def _render_catalyst_reference_corpus_context(reference_memory: dict | None = None, format_preset: str = "documentary", topic: str = "", channel_memory: dict | None = None, selected_cluster: dict | None = None) -> str:
    playbook = _build_catalyst_reference_playbook(
        reference_memory=reference_memory,
        format_preset=format_preset,
        topic=topic,
        channel_memory=channel_memory,
        selected_cluster=selected_cluster,
    )
    aggregate, chosen = _select_catalyst_reference_channels(
        reference_memory=reference_memory,
        format_preset=format_preset,
        topic=topic,
        channel_memory=channel_memory,
        selected_cluster=selected_cluster,
    )
    if not aggregate and not chosen:
        return ""
    parts: list[str] = []
    if aggregate:
        parts.append("Catalyst reference documentary corpus:")
        summary = str(aggregate.get("summary", "") or "").strip()
        if summary:
            parts.append(summary)
        keywords = [str(v).strip() for v in list(aggregate.get("proven_keywords") or []) if str(v).strip()]
        if keywords:
            parts.append("Cross-channel public winner keywords: " + ", ".join(keywords[:10]))
        packaging = [str(v).strip() for v in list(aggregate.get("packaging_learnings") or []) if str(v).strip()]
        if packaging:
            parts.append("Cross-channel packaging wins: " + "; ".join(packaging[:4]))
        visuals = [str(v).strip() for v in list(aggregate.get("visual_learnings") or []) if str(v).strip()]
        if visuals:
            parts.append("Cross-channel visual wins: " + "; ".join(visuals[:4]))
        sound = [str(v).strip() for v in list(aggregate.get("sound_learnings") or []) if str(v).strip()]
        if sound:
            parts.append("Cross-channel sound wins: " + "; ".join(sound[:4]))
    if str(playbook.get("summary", "") or "").strip():
        parts.append("Reference playbook match: " + _clip_text(str(playbook.get("summary", "") or ""), 320))
    if list(playbook.get("hook_rewrites") or []):
        parts.append("Reference hook moves: " + "; ".join(list(playbook.get("hook_rewrites") or [])[:3]))
    if list(playbook.get("visual_rewrites") or []):
        parts.append("Reference visual moves: " + "; ".join(list(playbook.get("visual_rewrites") or [])[:3]))
    if list(playbook.get("packaging_rewrites") or []):
        parts.append("Reference packaging moves: " + "; ".join(list(playbook.get("packaging_rewrites") or [])[:3]))
    for entry in chosen:
        channel = dict(entry.get("channel") or {})
        seed = dict(entry.get("seed") or {})
        memory = dict(entry.get("memory_seed") or {})
        channel_title = str(channel.get("title", "") or "").strip()
        style_notes = _clip_text(str(seed.get("style_notes", "") or "").strip(), 180)
        parts.append(
            _clip_text(
                f"Reference channel {channel_title}: "
                + (style_notes + ". " if style_notes else "")
                + ("Public winner titles: " + ", ".join(list(entry.get("top_video_titles") or [])[:3]) + ". " if list(entry.get("top_video_titles") or []) else "")
                + ("Packaging wins: " + "; ".join(list(memory.get("packaging_learnings") or [])[:2]) + ". " if list(memory.get("packaging_learnings") or []) else "")
                + ("Visual wins: " + "; ".join(list(memory.get("visual_learnings") or [])[:2]) + "." if list(memory.get("visual_learnings") or []) else ""),
                420,
            )
        )
    return "\n".join(part for part in parts if str(part or "").strip())


def _score_catalyst_outcome_against_reference(*, session_snapshot: dict, outcome_record: dict, reference_memory: dict | None = None) -> dict:
    session_snapshot = dict(session_snapshot or {})
    outcome_record = dict(outcome_record or {})
    format_preset = str(session_snapshot.get("format_preset", "") or outcome_record.get("format_preset", "") or "documentary").strip().lower()
    if format_preset != "documentary":
        return {}

    metadata_pack = dict(session_snapshot.get("metadata_pack") or {})
    source_video = dict(metadata_pack.get("source_video") or {})
    source_analysis = dict(metadata_pack.get("source_analysis") or {})
    channel_context = dict(metadata_pack.get("youtube_channel") or {})
    channel_memory = _catalyst_channel_memory_public_view(session_snapshot.get("channel_memory") or {})
    metrics = dict(outcome_record.get("metrics") or {})

    topic_anchor = (
        str(session_snapshot.get("topic", "") or "").strip()
        or str(outcome_record.get("title_used", "") or "").strip()
        or str(source_video.get("title", "") or "").strip()
    )
    aggregate, chosen = _select_catalyst_reference_channels(
        reference_memory=reference_memory,
        format_preset=format_preset,
        topic=topic_anchor,
        channel_memory=channel_memory,
        selected_cluster=dict(metadata_pack.get("selected_series_cluster") or {}),
    )
    chosen_titles = [
        str(((entry.get("channel") or {}).get("title")) or "").strip()
        for entry in list(chosen or [])
        if str(((entry.get("channel") or {}).get("title")) or "").strip()
    ]
    chosen_hook = _catalyst_reference_signal_list(chosen, "hook_learnings", max_items=6)
    chosen_pacing = _catalyst_reference_signal_list(chosen, "pacing_learnings", max_items=6)
    chosen_visual = _catalyst_reference_signal_list(chosen, "visual_learnings", max_items=6)
    chosen_sound = _catalyst_reference_signal_list(chosen, "sound_learnings", max_items=6)
    chosen_packaging = _catalyst_reference_signal_list(chosen, "packaging_learnings", max_items=6)
    chosen_next_moves = _catalyst_reference_signal_list(chosen, "next_video_moves", max_items=6)

    ctr = float(metrics.get("impression_click_through_rate", 0.0) or 0.0)
    avp = float(metrics.get("average_percentage_viewed", 0.0) or 0.0)
    avd = float(metrics.get("average_view_duration_sec", 0.0) or 0.0)
    first30 = float(metrics.get("first_30_sec_retention_pct", 0.0) or 0.0)
    first60 = float(metrics.get("first_60_sec_retention_pct", 0.0) or 0.0)
    preview_success_rate = float((dict(session_snapshot.get("learning_record") or {})).get("preview_success_rate", 0.0) or 0.0)
    target_duration_sec = max(
        float(outcome_record.get("video_duration_sec", 0.0) or 0.0),
        float(session_snapshot.get("target_minutes", 0.0) or 0.0) * 60.0,
    )
    avd_ratio_pct = round((avd / max(target_duration_sec, 1.0)) * 100.0, 2) if target_duration_sec > 0 else 0.0

    title_used = str(outcome_record.get("title_used", "") or "").strip()
    source_title = str(source_video.get("title", "") or session_snapshot.get("input_title", "") or "").strip()
    recent_titles = [
        *list(channel_context.get("recent_upload_titles") or []),
        *list(channel_context.get("top_video_titles") or []),
        *list(channel_memory.get("recent_selected_titles") or []),
    ]
    title_novelty_score = _catalyst_title_novelty_score(title_used, source_title=source_title, recent_titles=recent_titles)

    hook_score = int(round(
        (_catalyst_metric_score(first30, 48, 62, 75, neutral=52) * 0.45)
        + (_catalyst_metric_score(first60, 36, 52, 65, neutral=50) * 0.2)
        + (_catalyst_metric_score(avp, 28, 42, 55, neutral=55) * 0.35)
    ))
    pacing_score = int(round(
        (_catalyst_metric_score(avp, 28, 43, 58, neutral=55) * 0.45)
        + (_catalyst_metric_score(avd_ratio_pct, 24, 38, 50, neutral=55) * 0.35)
        + (_catalyst_signal_balance_score(outcome_record.get("pacing_wins"), outcome_record.get("pacing_watchouts"), neutral=60) * 0.2)
    ))
    packaging_score = int(round(
        (_catalyst_metric_score(ctr, 1.8, 3.8, 6.0, neutral=50) * 0.55)
        + (title_novelty_score * 0.3)
        + (_catalyst_signal_balance_score(outcome_record.get("packaging_wins"), outcome_record.get("packaging_watchouts"), neutral=62) * 0.15)
    ))
    visual_score = int(round(
        (_catalyst_signal_balance_score(outcome_record.get("visual_wins"), outcome_record.get("visual_watchouts"), neutral=62) * 0.45)
        + (_catalyst_metric_score(avp, 28, 43, 58, neutral=55) * 0.25)
        + (_catalyst_metric_score(preview_success_rate, 65, 88, 97, neutral=60) * 0.3)
    ))
    sound_score = int(round(
        (_catalyst_signal_balance_score(outcome_record.get("sound_wins"), outcome_record.get("sound_watchouts"), neutral=60) * 0.4)
        + (_catalyst_metric_score(first60, 36, 52, 65, neutral=50) * 0.3)
        + (_catalyst_metric_score(avp, 28, 43, 58, neutral=55) * 0.3)
    ))
    overall_score = int(round(
        (hook_score * 0.24)
        + (pacing_score * 0.22)
        + (visual_score * 0.18)
        + (sound_score * 0.12)
        + (packaging_score * 0.24)
    ))
    tier = _catalyst_reference_score_tier(overall_score)

    aggregate_hook = [str(v).strip() for v in list((dict(aggregate or {})).get("hook_learnings") or []) if str(v).strip()]
    aggregate_pacing = [str(v).strip() for v in list((dict(aggregate or {})).get("pacing_learnings") or []) if str(v).strip()]
    aggregate_visual = [str(v).strip() for v in list((dict(aggregate or {})).get("visual_learnings") or []) if str(v).strip()]
    aggregate_sound = [str(v).strip() for v in list((dict(aggregate or {})).get("sound_learnings") or []) if str(v).strip()]
    aggregate_packaging = [str(v).strip() for v in list((dict(aggregate or {})).get("packaging_learnings") or []) if str(v).strip()]
    aggregate_next = [str(v).strip() for v in list((dict(aggregate or {})).get("next_video_moves") or []) if str(v).strip()]

    hook_rewrites = _dedupe_preserve_order([
        "Compress the opening so the title promise becomes visible before any background context." if hook_score < 70 else "",
        "Open on the hidden mechanism, consequence, or contradiction instead of warming up slowly." if first30 < 60 or first60 < 50 else "",
        *chosen_hook[:2],
        *aggregate_hook[:2],
    ], max_items=6, max_chars=180)
    pacing_rewrites = _dedupe_preserve_order([
        "Force a new reveal, contrast, or escalation beat every 10 to 15 seconds." if pacing_score < 70 else "",
        "Cut dead-air explanation blocks and move the proof or consequence earlier." if avp < 42 or avd_ratio_pct < 38 else "",
        *chosen_pacing[:2],
        *aggregate_pacing[:2],
    ], max_items=6, max_chars=180)
    visual_rewrites = _dedupe_preserve_order([
        "Replace repeated hero-object shots with system cutaways, map logic, or human-versus-system frames." if visual_score < 70 else "",
        "Keep the 3D language intentionally designed and use one dominant subject with one dominant lighting cue per frame.",
        "Treat every scene as a different proof step instead of repeating the same symbol." if preview_success_rate < 90 or visual_score < 72 else "",
        *chosen_visual[:2],
        *aggregate_visual[:2],
    ], max_items=6, max_chars=180)
    sound_rewrites = _dedupe_preserve_order([
        "Use sharper SFX punctuation around reversals and consequences instead of a flat ambient bed." if sound_score < 70 else "",
        "Drop the bed briefly before the biggest reveal so the next hit lands harder." if first60 < 55 or avp < 42 else "",
        *chosen_sound[:2],
        *aggregate_sound[:2],
    ], max_items=6, max_chars=180)
    packaging_rewrites = _dedupe_preserve_order([
        "Generate a genuinely new title in the same arena instead of recycling the source phrasing." if title_novelty_score < 75 else "",
        "Promote one hidden mechanism, conflict, or payoff into the title and thumbnail brief." if packaging_score < 70 or ctr < 4.0 else "",
        "Keep the title compact and curiosity-led; do not over-explain the whole topic." if ctr < 3.5 else "",
        *chosen_packaging[:2],
        *aggregate_packaging[:2],
    ], max_items=6, max_chars=180)
    next_run_moves = _dedupe_preserve_order([
        *packaging_rewrites[:2],
        *hook_rewrites[:1],
        *pacing_rewrites[:1],
        *chosen_next_moves[:2],
        *aggregate_next[:3],
        *list(source_analysis.get("improvement_moves") or [])[:2],
    ], max_items=10, max_chars=180)

    wins_to_double_down = _dedupe_preserve_order([
        "Packaging is landing strongly enough to keep the same arena and keep pushing contrast." if packaging_score >= 75 else "",
        "Hook retention is strong enough to preserve the early reveal structure." if hook_score >= 75 else "",
        "Pacing is holding viewers well enough to keep the escalation pattern." if pacing_score >= 75 else "",
        *list(outcome_record.get("strongest_signals") or [])[:3],
    ], max_items=8, max_chars=180)
    gaps_to_fix = _dedupe_preserve_order([
        hook_rewrites[0] if hook_score < 75 and hook_rewrites else "",
        pacing_rewrites[0] if pacing_score < 75 and pacing_rewrites else "",
        visual_rewrites[0] if visual_score < 75 and visual_rewrites else "",
        sound_rewrites[0] if sound_score < 75 and sound_rewrites else "",
        packaging_rewrites[0] if packaging_score < 75 and packaging_rewrites else "",
        *list(outcome_record.get("weak_points") or [])[:3],
    ], max_items=8, max_chars=180)

    reference_summary = _clip_text(
        f"Reference playbook score {overall_score}/100 ({tier}). "
        + (f"Best matching channels: {', '.join(chosen_titles[:3])}. " if chosen_titles else "")
        + f"Hook {hook_score}, pacing {pacing_score}, visuals {visual_score}, sound {sound_score}, packaging {packaging_score}. "
        + ("Packaging is the lead growth opportunity. " if packaging_score <= min(hook_score, pacing_score, visual_score, sound_score) else "")
        + ("Hook retention is the main bottleneck. " if hook_score <= min(pacing_score, visual_score, sound_score, packaging_score) else "")
        + ("Pacing is the main bottleneck. " if pacing_score <= min(hook_score, visual_score, sound_score, packaging_score) else "")
        + ("Next run should push a genuinely new title angle, stronger payoff in the first 15 seconds, and more varied proof visuals."),
        360,
    )
    return {
        "benchmark_channels": chosen_titles[:4],
        "reference_summary": reference_summary,
        "scores": {
            "overall": overall_score,
            "hook": hook_score,
            "pacing": pacing_score,
            "visuals": visual_score,
            "sound": sound_score,
            "packaging": packaging_score,
            "title_novelty": title_novelty_score,
        },
        "tier": tier,
        "wins_to_double_down": wins_to_double_down,
        "gaps_to_fix": gaps_to_fix,
        "hook_rewrites": hook_rewrites,
        "pacing_rewrites": pacing_rewrites,
        "visual_rewrites": visual_rewrites,
        "sound_rewrites": sound_rewrites,
        "packaging_rewrites": packaging_rewrites,
        "next_run_moves": next_run_moves,
    }


