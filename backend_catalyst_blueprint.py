import json

from backend_catalyst_core import (
    _clip_text,
    _dedupe_preserve_order,
    _render_catalyst_series_cluster_context,
    _resolve_catalyst_series_context,
)
from backend_catalyst_profiles import (
    _catalyst_default_sound_profile,
    _catalyst_default_visual_engine,
    _heuristic_catalyst_chapter_blueprints,
)

def _heuristic_catalyst_edit_blueprint(
    *,
    template: str,
    format_preset: str,
    topic: str,
    input_title: str,
    input_description: str,
    chapter_count: int,
    chapter_target_sec: float,
    source_analysis: dict | None = None,
    channel_context: dict | None = None,
    channel_memory: dict | None = None,
    same_arena_subject_fn=None,
) -> dict:
    source_analysis = dict(source_analysis or {})
    channel_context = dict(channel_context or {})
    channel_memory_raw = dict(channel_memory or {})
    series_context = _resolve_catalyst_series_context(
        channel_context,
        channel_memory=channel_memory_raw,
        topic=topic,
        source_title=input_title,
        input_title=input_title,
        input_description=input_description,
        format_preset=format_preset,
    )
    series_anchor_override = str(series_context.get("series_anchor_override", "") or "").strip()
    selected_cluster = dict(series_context.get("selected_cluster") or {})
    memory_view = dict(series_context.get("memory_view") or {})
    rewrite_pressure = dict(memory_view.get("rewrite_pressure") or {})
    niche_key = str(memory_view.get("niche_key", "") or "").strip().lower()
    series_anchor = _clip_text(str(memory_view.get("series_anchor", "") or ""), 120)
    archetype_key = str(memory_view.get("archetype_key", "") or "").strip().lower()
    archetype_label = _clip_text(str(memory_view.get("archetype_label", "") or ""), 120)
    archetype_keywords = [str(v).strip() for v in list(memory_view.get("archetype_keywords") or []) if str(v).strip()]
    archetype_hook_rule = _clip_text(str(memory_view.get("archetype_hook_rule", "") or ""), 220)
    archetype_pace_rule = _clip_text(str(memory_view.get("archetype_pace_rule", "") or ""), 220)
    archetype_visual_rule = _clip_text(str(memory_view.get("archetype_visual_rule", "") or ""), 220)
    archetype_sound_rule = _clip_text(str(memory_view.get("archetype_sound_rule", "") or ""), 220)
    archetype_packaging_rule = _clip_text(str(memory_view.get("archetype_packaging_rule", "") or ""), 220)
    niche_follow_up_rule = _clip_text(str(memory_view.get("niche_follow_up_rule", "") or selected_cluster.get("follow_up_rule", "") or ""), 220)
    is_recap_lane = bool(format_preset == "recap" or niche_key == "manga_recap")
    cluster_label = _clip_text(str(selected_cluster.get("label", "") or "").strip(), 120)
    same_arena_subject_fn = same_arena_subject_fn or (lambda _source_bundle, topic="": "")
    subject = same_arena_subject_fn({"title": input_title or topic}, topic=topic or input_title) or cluster_label or _clip_text(topic or input_title or "the core subject", 80)
    recap_subject = series_anchor or series_anchor_override or subject
    improvement_moves = [str(v).strip() for v in list(source_analysis.get("improvement_moves") or []) if str(v).strip()]
    retention_findings = [str(v).strip() for v in list(source_analysis.get("retention_findings") or []) if str(v).strip()]
    packaging_findings = [str(v).strip() for v in list(source_analysis.get("packaging_findings") or []) if str(v).strip()]
    title_hints = [str(v).strip() for v in list(channel_context.get("title_pattern_hints") or []) if str(v).strip()]
    title_hints = _dedupe_preserve_order([*title_hints, *list(selected_cluster.get("sample_titles") or [])[:3]], max_items=6, max_chars=160)
    sound_profile, music_profile = _catalyst_default_sound_profile(topic, input_title, format_preset)
    transition_style = "snap" if is_recap_lane else ("cinematic" if format_preset in {"documentary", "explainer", "recap"} else "smooth")
    if archetype_key == "trading_execution":
        transition_style = "crisp"
    elif archetype_key in {"dark_psychology", "power_history"}:
        transition_style = "tension"
    chapter_blueprints = _heuristic_catalyst_chapter_blueprints(
        chapter_count=chapter_count,
        subject=subject,
        improvement_moves=improvement_moves,
        retention_findings=retention_findings,
    )
    primary_move = _clip_text(improvement_moves[0] if improvement_moves else "Tighten the promise and reach the first reveal faster.", 180)
    hook_warning = _clip_text(retention_findings[0] if retention_findings else "The opening needs a stronger promise before any explanation.", 180)
    packaging_warning = _clip_text(packaging_findings[0] if packaging_findings else "Use one dominant promise and one dominant visual symbol.", 180)
    outcome_ctr = float(memory_view.get("average_ctr", 0.0) or 0.0)
    outcome_avp = float(memory_view.get("average_average_percentage_viewed", 0.0) or 0.0)
    hook_wins = list(memory_view.get("hook_wins") or [])
    hook_watchouts = list(memory_view.get("hook_watchouts") or [])
    pacing_wins = list(memory_view.get("pacing_wins") or [])
    pacing_watchouts = list(memory_view.get("pacing_watchouts") or [])
    visual_wins = list(memory_view.get("visual_wins") or [])
    visual_watchouts = list(memory_view.get("visual_watchouts") or [])
    sound_wins = list(memory_view.get("sound_wins") or [])
    sound_watchouts = list(memory_view.get("sound_watchouts") or [])
    packaging_wins = list(memory_view.get("packaging_wins") or [])
    packaging_watchouts = list(memory_view.get("packaging_watchouts") or [])
    retention_wins = list(memory_view.get("retention_wins") or [])
    reference_hook_rewrites = list(memory_view.get("reference_hook_rewrites") or [])
    reference_pacing_rewrites = list(memory_view.get("reference_pacing_rewrites") or [])
    reference_visual_rewrites = list(memory_view.get("reference_visual_rewrites") or [])
    reference_sound_rewrites = list(memory_view.get("reference_sound_rewrites") or [])
    reference_packaging_rewrites = list(memory_view.get("reference_packaging_rewrites") or [])
    pressure_scores = {
        str(row.get("key", "") or ""): int(row.get("score", 0) or 0)
        for row in list(rewrite_pressure.get("categories") or [])
        if str(row.get("key", "") or "").strip()
    }
    rewrite_priorities = [str(v).strip() for v in list(rewrite_pressure.get("next_run_priorities") or []) if str(v).strip()]
    weighted_next_moves = _dedupe_preserve_order([
        *list(memory_view.get("reference_next_video_moves") or []),
        *list(memory_view.get("next_video_moves") or []),
    ], max_items=8, max_chars=180)
    pattern_interrupt_interval = 10 if is_recap_lane else (15 if format_preset == "documentary" else 12)
    if pressure_scores.get("pacing", 0) >= 75:
        pattern_interrupt_interval = 8 if is_recap_lane else 9
    elif pressure_scores.get("pacing", 0) >= 55:
        pattern_interrupt_interval = 9 if is_recap_lane else 11
    hook_promise = _clip_text(reference_hook_rewrites[0] if reference_hook_rewrites else f"Open on the strongest hidden consequence around {subject}, not generic setup.", 220)
    hook_open_loop = _clip_text(weighted_next_moves[0] if weighted_next_moves else primary_move, 180)
    hook_first30 = _clip_text(reference_hook_rewrites[1] if len(reference_hook_rewrites) > 1 else (hook_watchouts[0] if hook_watchouts else hook_warning), 180)
    shock_device = "Use one unsettling or counterintuitive reveal within the first 15 seconds."
    if archetype_hook_rule and not is_recap_lane:
        hook_promise = _clip_text(archetype_hook_rule, 220)
    if archetype_pace_rule and not is_recap_lane and not weighted_next_moves:
        hook_open_loop = _clip_text(archetype_pace_rule, 180)
    if is_recap_lane:
        hook_promise = _clip_text(
            reference_hook_rewrites[0]
            if reference_hook_rewrites
            else f"Open on the sharpest power jump, betrayal, hidden system rule, or chapter-turn inside {recap_subject}.",
            220,
        )
        hook_open_loop = _clip_text(
            weighted_next_moves[0]
            if weighted_next_moves
            else (niche_follow_up_rule or f"Stay inside {recap_subject} and escalate the next chapter-turn instead of retelling setup."),
            180,
        )
        hook_first30 = _clip_text(
            reference_hook_rewrites[1]
            if len(reference_hook_rewrites) > 1
            else "The first 30 seconds must land the strongest chapter-turn, power consequence, or betrayal before any recap exposition.",
            180,
        )
        shock_device = "Use one addiction-grade chapter-turn, betrayal, rank jump, or system reveal in the first 10 to 15 seconds."
    if pressure_scores.get("hook", 0) >= 70:
        hook_promise = _clip_text(f"{hook_promise} The opening must land the claim before any explanation.", 220)
        hook_open_loop = _clip_text(rewrite_priorities[0] if rewrite_priorities else hook_open_loop, 180)
        hook_first30 = _clip_text("Shorten setup brutally. Promise, proof, and reversal must all appear in the first 30 seconds.", 180)
        shock_device = "Use one counterintuitive reveal or disturbing contradiction in the first 10 to 15 seconds."
    elif archetype_key == "dark_psychology":
        shock_device = "Use one intimate contradiction or disturbing subconscious reveal in the first 10 to 15 seconds."
    elif archetype_key == "trading_execution":
        shock_device = "Use one money consequence, execution mistake, or setup edge in the first 10 to 15 seconds."
    elif archetype_key == "power_history":
        shock_device = "Use one power shift, strike, or leadership consequence in the first 10 to 15 seconds."
    if is_recap_lane:
        recap_arc_moves = _dedupe_preserve_order([
            niche_follow_up_rule,
            "Escalate the strongest power turn or betrayal faster than the source video.",
            "Keep the recap locked to the same series arena instead of drifting into generic documentary framing.",
            "Use protagonist, rival, weapon, monster, or system iconography instead of random abstract objects.",
        ], max_items=4, max_chars=180)
        recap_blueprints: list[dict] = []
        for idx, raw_blueprint in enumerate(chapter_blueprints):
            chapter = dict(raw_blueprint or {})
            chapter["focus"] = _clip_text(
                f"{str(chapter.get('focus', '') or '').strip()} Keep the recap anchored to {recap_subject} and escalate the next chapter-turn.",
                220,
            )
            if idx == 0:
                chapter["hook_job"] = _clip_text(
                    f"Start on the most addictive reveal around {recap_subject} before any broad recap setup.",
                    180,
                )
                chapter["shock_device"] = _clip_text("chapter-turn + betrayal or power-system reveal", 160)
            chapter["visual_motif"] = _clip_text(
                f"{str(chapter.get('visual_motif', '') or '').strip()} Use {recap_subject} iconography, power hierarchy, rival tension, or system-energy cues instead of sterile documentary props.",
                220,
            )
            chapter["motion_note"] = _clip_text(
                f"{str(chapter.get('motion_note', '') or '').strip()}; use kinetic panel-smash energy, power-rank contrast, and faster recap resets.",
                180,
            )
            chapter["sound_note"] = _clip_text(
                f"{str(chapter.get('sound_note', '') or '').strip()}; emphasize trailer pulses, reveal hits, and rank-jump impacts.",
                180,
            )
            chapter["improvement_focus"] = _clip_text(
                f"{str(chapter.get('improvement_focus', '') or '').strip()} {recap_arc_moves[idx % len(recap_arc_moves)]}",
                180,
            )
            recap_blueprints.append(chapter)
        chapter_blueprints = recap_blueprints
    camera_language = _dedupe_preserve_order([
        "controlled dolly pushes into hero objects",
        "macro cutaways that reveal the hidden mechanism",
        "miniature-world system sweeps for context",
        "sharp pattern interrupts when the point changes",
        archetype_visual_rule,
        rewrite_priorities[1] if len(rewrite_priorities) > 1 else "",
        *reference_visual_rewrites[:2],
        *visual_wins[:2],
    ], max_items=6, max_chars=160)
    motion_graphics = _dedupe_preserve_order([
        "clean HUD-style overlays only when they clarify the beat",
        "diagram callouts that explain one mechanism at a time",
        "before-versus-after or myth-versus-reality comparisons",
        archetype_packaging_rule,
        rewrite_priorities[2] if len(rewrite_priorities) > 2 else "",
        *reference_visual_rewrites[:2],
        *packaging_wins[:1],
        packaging_warning,
    ], max_items=6, max_chars=180)
    visual_rules = _dedupe_preserve_order([
        "Stay obviously 3D and intentionally designed, not live-action.",
        "Keep one dominant subject per frame and one dominant lighting cue.",
        "Use contrast and scale shifts to reset attention.",
        archetype_visual_rule,
        "Avoid reusing the same isolated hero-object stage in consecutive scenes." if pressure_scores.get("visuals", 0) >= 60 else "",
        *reference_visual_rewrites[:2],
        *visual_watchouts[:2],
        *list(memory_view.get("retention_watchouts") or [])[:1],
    ], max_items=6, max_chars=180)
    mix_notes = _dedupe_preserve_order([
        "Use trailer-grade impacts only on real reveals, not every scene.",
        "Keep the ambience bed present but under narration.",
        "Accent chapter turns with sharp motion-graphic sweeps and low-end hits.",
        archetype_sound_rule,
        "Use more deliberate silence pockets before the strongest reveals." if pressure_scores.get("sound", 0) >= 60 else "",
        *reference_sound_rewrites[:2],
        *sound_wins[:2],
        *sound_watchouts[:1],
    ], max_items=6, max_chars=160)
    voice_direction = _dedupe_preserve_order([
        "Confident, controlled, and slightly ominous.",
        "Short declarative lines in the first 20 to 30 seconds.",
        "Speed up on mechanism beats and slow slightly on payoffs.",
        archetype_hook_rule,
    ], max_items=4, max_chars=160)
    scoring_rubric = _dedupe_preserve_order([
        "First 15 seconds must promise a concrete payoff.",
        "Every scene must visualize the exact narration beat, not a generic metaphor.",
        "Every chapter needs at least one escalation and one pattern interrupt.",
        "Packaging must stay in the same arena while avoiding title repetition.",
        archetype_packaging_rule,
        rewrite_priorities[0] if rewrite_priorities else "",
        *reference_hook_rewrites[:1],
        *reference_packaging_rewrites[:1],
        *hook_wins[:2],
        *packaging_wins[:1],
        *retention_wins[:1],
        *list(memory_view.get("retention_watchouts") or [])[:1],
    ], max_items=8, max_chars=180)
    niche_execution_notes = _dedupe_preserve_order([
        niche_follow_up_rule,
        f"Archetype: {archetype_label}." if archetype_label else "",
        archetype_hook_rule,
        archetype_pace_rule,
        archetype_visual_rule,
        archetype_sound_rule,
        archetype_packaging_rule,
        _render_catalyst_series_cluster_context(selected_cluster),
        f"Preserve the series anchor {recap_subject} across hook, visuals, and packaging." if is_recap_lane and recap_subject else "",
    ], max_items=4, max_chars=180)
    if archetype_key == "trading_execution" and not is_recap_lane:
        camera_language = _dedupe_preserve_order([
            "chart-level push-ins and execution-proof closeups",
            "entry-versus-exit contrasts with immediate stakes",
            *camera_language,
        ], max_items=7, max_chars=160)
        motion_graphics = _dedupe_preserve_order([
            "clean chart overlays and liquidity callouts only when they prove the setup",
            "risk-versus-reward comparisons instead of generic financial luxury visuals",
            *motion_graphics,
        ], max_items=7, max_chars=180)
    elif archetype_key == "dark_psychology" and not is_recap_lane:
        camera_language = _dedupe_preserve_order([
            "intimate macro moves into mental symbols and hidden mechanisms",
            "stark contrast resets that feel invasive and personal",
            *camera_language,
        ], max_items=7, max_chars=160)
        motion_graphics = _dedupe_preserve_order([
            "thought-pattern, subconscious, and contradiction overlays only when they sharpen the emotional point",
            *motion_graphics,
        ], max_items=7, max_chars=180)
    elif archetype_key == "power_history" and not is_recap_lane:
        camera_language = _dedupe_preserve_order([
            "map-room sweeps and consequence-led push-ins on leaders or conflict zones",
            "corridor-of-power staging with clear cause-versus-fallout transitions",
            *camera_language,
        ], max_items=7, max_chars=160)
    if is_recap_lane:
        camera_language = _dedupe_preserve_order([
            "kinetic manga-recap push-ins on power beats",
            "panel-smash snap cuts between reveal states",
            "weapon, aura, or system-energy sweeps that escalate hierarchy",
            *camera_language,
        ], max_items=7, max_chars=160)
        motion_graphics = _dedupe_preserve_order([
            "manga-style energy cues and chapter-impact framing only when they sharpen the beat",
            "power-system or rank overlays only when they clarify escalation",
            "betrayal-versus-trust or before-versus-after clash frames instead of sterile business diagrams",
            *motion_graphics,
        ], max_items=7, max_chars=180)
        visual_rules = _dedupe_preserve_order([
            f"Stay inside the {recap_subject} universe; every scene must look like it belongs to that recap world." if recap_subject else "",
            "Prefer protagonist, rival, weapon, monster, or system iconography over generic abstract objects.",
            "Escalate power, betrayal, rank, survival, or system stakes instead of generic documentary explanation.",
            "Avoid medical, lab, map-room, or business metaphors unless the narration is literally about them.",
            *visual_rules,
        ], max_items=7, max_chars=180)
        mix_notes = _dedupe_preserve_order([
            "Use anime-recap trailer pulses, energy swells, and hard impact accents on power turns.",
            "Hit betrayals, rank jumps, and system reveals with sharper transient design.",
            *mix_notes,
        ], max_items=7, max_chars=160)
        voice_direction = _dedupe_preserve_order([
            "Urgent, high-pressure, and confident.",
            "Punch the turn words harder: reveal, rank, betrayal, system, death, power.",
            *voice_direction,
        ], max_items=5, max_chars=160)
        scoring_rubric = _dedupe_preserve_order([
            f"Every chapter must stay unmistakably inside {recap_subject}, not generic documentary space." if recap_subject else "",
            "Scenes should escalate power, betrayal, rank, survival, or system stakes.",
            "Hook must land the most addictive chapter-turn first.",
            *scoring_rubric,
        ], max_items=8, max_chars=180)
        niche_execution_notes = _dedupe_preserve_order([
            *niche_execution_notes,
            "Use recap energy, chapter-turn escalation, and power hierarchy instead of documentary explanation rhythms.",
            "The next video should feel like a stronger chapter obsession, not a remake of the last upload.",
        ], max_items=5, max_chars=180)
    return {
        "version": "catalyst_edit_v1",
        "visual_engine": _catalyst_default_visual_engine(template, format_preset),
        "format_preset": format_preset,
        "niche_key": niche_key,
        "series_anchor": series_anchor,
        "niche_follow_up_rule": niche_follow_up_rule,
        "niche_execution_notes": niche_execution_notes,
        "analysis_required_before_generation": True,
        "hook_strategy": {
            "promise": hook_promise,
            "open_loop": hook_open_loop,
            "shock_device": shock_device,
            "first_30s_mission": hook_first30,
        },
        "pacing_strategy": {
            "scene_duration_sec": 4.5 if pressure_scores.get("pacing", 0) >= 70 else 5.0,
            "chapter_target_sec": round(float(chapter_target_sec or 0.0), 2),
            "chapter_count": int(chapter_count or 0),
            "escalation_curve": "hook -> mechanism -> consequence -> contrast -> payoff",
            "pattern_interrupt_interval_sec": pattern_interrupt_interval,
            "transition_style": transition_style,
            "micro_escalation_mode": bool(format_preset in {"documentary", "explainer", "recap"}),
            "pacing_rules": _dedupe_preserve_order([
                primary_move,
                rewrite_priorities[0] if rewrite_priorities else "",
                *reference_pacing_rewrites[:2],
                *pacing_wins[:2],
                "Do not spend more than two scenes on the same visual idea.",
                "Every chapter needs at least one contrast or reversal beat.",
                "Escalate the consequences before the viewer settles into the pattern.",
                *pacing_watchouts[:2],
                *retention_findings[:3],
            ], max_items=6, max_chars=180),
        },
        "motion_strategy": {
            "camera_language": camera_language,
            "motion_graphics": motion_graphics,
            "transition_style": transition_style,
            "visual_rules": visual_rules,
        },
        "sound_strategy": {
            "sfx_profile": sound_profile,
            "music_profile": music_profile,
            "mix_notes": mix_notes,
            "silence_rules": _dedupe_preserve_order([
                "Drop the bed briefly before the biggest reveal in each chapter.",
                "Leave short pockets of space after heavy claims so the next hit lands harder.",
            ], max_items=4, max_chars=160),
            "voice_direction": voice_direction,
        },
        "retention_targets": {
            "main_bottleneck": _clip_text(hook_watchouts[0] if hook_watchouts else hook_warning, 220),
            "main_opportunity": _clip_text(weighted_next_moves[0] if weighted_next_moves else primary_move, 220),
            "packaging_opportunity": _clip_text(reference_packaging_rewrites[0] if reference_packaging_rewrites else (packaging_watchouts[0] if packaging_watchouts else packaging_warning), 220),
            "channel_title_hints": title_hints[:4],
            "memory_keywords": list(memory_view.get("proven_keywords") or [])[:8],
            "series_anchor": series_anchor,
            "niche_follow_up_rule": niche_follow_up_rule,
            "measured_ctr_context": f"Measured channel average CTR: {outcome_ctr:.2f}%." if outcome_ctr > 0 else "",
            "measured_retention_context": f"Measured average viewed: {outcome_avp:.2f}%." if outcome_avp > 0 else "",
            "rewrite_pressure_summary": _clip_text(str(rewrite_pressure.get("summary", "") or ""), 240),
            "next_run_priorities": rewrite_priorities[:5],
        },
        "scoring_rubric": scoring_rubric,
        "chapter_blueprints": _heuristic_catalyst_chapter_blueprints(
            chapter_count=chapter_count,
            subject=subject,
            improvement_moves=improvement_moves,
            retention_findings=retention_findings,
            pressure_profile=rewrite_pressure,
        ),
    }


def _normalize_catalyst_edit_blueprint(raw: dict | None, heuristic: dict, chapter_count: int) -> dict:
    raw = dict(raw or {})
    hook_in = dict(raw.get("hook_strategy") or {})
    pacing_in = dict(raw.get("pacing_strategy") or {})
    motion_in = dict(raw.get("motion_strategy") or {})
    sound_in = dict(raw.get("sound_strategy") or {})
    retention_in = dict(raw.get("retention_targets") or {})
    heuristic_chapters = list(heuristic.get("chapter_blueprints") or [])
    raw_chapters = list(raw.get("chapter_blueprints") or [])
    chapter_blueprints: list[dict] = []
    for idx in range(max(1, int(chapter_count or len(heuristic_chapters) or 1))):
        base = dict(heuristic_chapters[idx] if idx < len(heuristic_chapters) else {})
        incoming = dict(raw_chapters[idx] if idx < len(raw_chapters) and isinstance(raw_chapters[idx], dict) else {})
        chapter_blueprints.append({
            "index": idx,
            "focus": _clip_text(str(incoming.get("focus", base.get("focus", "")) or ""), 220),
            "hook_job": _clip_text(str(incoming.get("hook_job", base.get("hook_job", "")) or ""), 180),
            "shock_device": _clip_text(str(incoming.get("shock_device", base.get("shock_device", "")) or ""), 160),
            "visual_motif": _clip_text(str(incoming.get("visual_motif", base.get("visual_motif", "")) or ""), 220),
            "motion_note": _clip_text(str(incoming.get("motion_note", base.get("motion_note", "")) or ""), 180),
            "sound_note": _clip_text(str(incoming.get("sound_note", base.get("sound_note", "")) or ""), 180),
            "retention_goal": _clip_text(str(incoming.get("retention_goal", base.get("retention_goal", "")) or ""), 180),
            "improvement_focus": _clip_text(str(incoming.get("improvement_focus", base.get("improvement_focus", "")) or ""), 180),
        })
    return {
        "version": str(raw.get("version", heuristic.get("version", "catalyst_edit_v1")) or "catalyst_edit_v1"),
        "visual_engine": _clip_text(str(raw.get("visual_engine", heuristic.get("visual_engine", "")) or ""), 120),
        "format_preset": str(raw.get("format_preset", heuristic.get("format_preset", "")) or ""),
        "analysis_required_before_generation": bool(raw.get("analysis_required_before_generation", heuristic.get("analysis_required_before_generation", True))),
        "hook_strategy": {
            "promise": _clip_text(str(hook_in.get("promise", heuristic.get("hook_strategy", {}).get("promise", "")) or ""), 220),
            "open_loop": _clip_text(str(hook_in.get("open_loop", heuristic.get("hook_strategy", {}).get("open_loop", "")) or ""), 220),
            "shock_device": _clip_text(str(hook_in.get("shock_device", heuristic.get("hook_strategy", {}).get("shock_device", "")) or ""), 180),
            "first_30s_mission": _clip_text(str(hook_in.get("first_30s_mission", heuristic.get("hook_strategy", {}).get("first_30s_mission", "")) or ""), 220),
        },
        "pacing_strategy": {
            "scene_duration_sec": float(pacing_in.get("scene_duration_sec", heuristic.get("pacing_strategy", {}).get("scene_duration_sec", 5.0)) or 5.0),
            "chapter_target_sec": float(pacing_in.get("chapter_target_sec", heuristic.get("pacing_strategy", {}).get("chapter_target_sec", 50.0)) or 50.0),
            "chapter_count": int(pacing_in.get("chapter_count", heuristic.get("pacing_strategy", {}).get("chapter_count", chapter_count)) or chapter_count),
            "escalation_curve": _clip_text(str(pacing_in.get("escalation_curve", heuristic.get("pacing_strategy", {}).get("escalation_curve", "")) or ""), 160),
            "pattern_interrupt_interval_sec": int(pacing_in.get("pattern_interrupt_interval_sec", heuristic.get("pacing_strategy", {}).get("pattern_interrupt_interval_sec", 12)) or 12),
            "transition_style": str(pacing_in.get("transition_style", heuristic.get("pacing_strategy", {}).get("transition_style", "smooth")) or "smooth"),
            "micro_escalation_mode": bool(pacing_in.get("micro_escalation_mode", heuristic.get("pacing_strategy", {}).get("micro_escalation_mode", False))),
            "pacing_rules": _dedupe_preserve_order(list(pacing_in.get("pacing_rules") or heuristic.get("pacing_strategy", {}).get("pacing_rules", []) or []), max_items=8, max_chars=180),
        },
        "motion_strategy": {
            "camera_language": _dedupe_preserve_order(list(motion_in.get("camera_language") or heuristic.get("motion_strategy", {}).get("camera_language", []) or []), max_items=8, max_chars=160),
            "motion_graphics": _dedupe_preserve_order(list(motion_in.get("motion_graphics") or heuristic.get("motion_strategy", {}).get("motion_graphics", []) or []), max_items=8, max_chars=180),
            "transition_style": str(motion_in.get("transition_style", heuristic.get("motion_strategy", {}).get("transition_style", "smooth")) or "smooth"),
            "visual_rules": _dedupe_preserve_order(list(motion_in.get("visual_rules") or heuristic.get("motion_strategy", {}).get("visual_rules", []) or []), max_items=8, max_chars=180),
        },
        "sound_strategy": {
            "sfx_profile": _clip_text(str(sound_in.get("sfx_profile", heuristic.get("sound_strategy", {}).get("sfx_profile", "")) or ""), 120),
            "music_profile": _clip_text(str(sound_in.get("music_profile", heuristic.get("sound_strategy", {}).get("music_profile", "")) or ""), 120),
            "mix_notes": _dedupe_preserve_order(list(sound_in.get("mix_notes") or heuristic.get("sound_strategy", {}).get("mix_notes", []) or []), max_items=8, max_chars=180),
            "silence_rules": _dedupe_preserve_order(list(sound_in.get("silence_rules") or heuristic.get("sound_strategy", {}).get("silence_rules", []) or []), max_items=6, max_chars=180),
            "voice_direction": _dedupe_preserve_order(list(sound_in.get("voice_direction") or heuristic.get("sound_strategy", {}).get("voice_direction", []) or []), max_items=6, max_chars=180),
        },
        "retention_targets": {
            "main_bottleneck": _clip_text(str(retention_in.get("main_bottleneck", heuristic.get("retention_targets", {}).get("main_bottleneck", "")) or ""), 220),
            "main_opportunity": _clip_text(str(retention_in.get("main_opportunity", heuristic.get("retention_targets", {}).get("main_opportunity", "")) or ""), 220),
            "packaging_opportunity": _clip_text(str(retention_in.get("packaging_opportunity", heuristic.get("retention_targets", {}).get("packaging_opportunity", "")) or ""), 220),
            "channel_title_hints": _dedupe_preserve_order(list(retention_in.get("channel_title_hints") or heuristic.get("retention_targets", {}).get("channel_title_hints", []) or []), max_items=6, max_chars=160),
            "memory_keywords": _dedupe_preserve_order(list(retention_in.get("memory_keywords") or heuristic.get("retention_targets", {}).get("memory_keywords", []) or []), max_items=10, max_chars=80),
            "rewrite_pressure_summary": _clip_text(str(retention_in.get("rewrite_pressure_summary", heuristic.get("retention_targets", {}).get("rewrite_pressure_summary", "")) or ""), 240),
            "next_run_priorities": _dedupe_preserve_order(list(retention_in.get("next_run_priorities") or heuristic.get("retention_targets", {}).get("next_run_priorities", []) or []), max_items=6, max_chars=180),
        },
        "scoring_rubric": _dedupe_preserve_order(list(raw.get("scoring_rubric") or heuristic.get("scoring_rubric", []) or []), max_items=10, max_chars=180),
        "chapter_blueprints": chapter_blueprints,
    }


async def _build_catalyst_edit_blueprint(
    *,
    template: str,
    format_preset: str,
    topic: str,
    input_title: str,
    input_description: str,
    chapter_count: int,
    chapter_target_sec: float,
    source_bundle: dict | None = None,
    source_analysis: dict | None = None,
    channel_context: dict | None = None,
    channel_memory: dict | None = None,
    strategy_notes: str = "",
    xai_json_completion_fn=None,
    marketing_doctrine_text_fn=None,
    render_reference_corpus_context_fn=None,
    same_arena_subject_fn=None,
) -> dict:
    series_context = _resolve_catalyst_series_context(
        channel_context,
        channel_memory=channel_memory,
        topic=topic,
        source_title=str((source_bundle or {}).get("title", "") or input_title or ""),
        input_title=input_title,
        input_description=input_description,
        format_preset=format_preset,
    )
    heuristic = _heuristic_catalyst_edit_blueprint(
        template=template,
        format_preset=format_preset,
        topic=topic,
        input_title=input_title,
        input_description=input_description,
        chapter_count=chapter_count,
        chapter_target_sec=chapter_target_sec,
        source_analysis=source_analysis,
        channel_context=channel_context,
        channel_memory=channel_memory,
        same_arena_subject_fn=same_arena_subject_fn,
    )
    reference_corpus_context = render_reference_corpus_context_fn(format_preset=format_preset, topic=topic or input_title or input_description) if render_reference_corpus_context_fn else ""
    system_prompt = (
        "You are Catalyst Engine, the edit strategist for NYPTID Studio. "
        "Build a render blueprint for a faceless YouTube long-form video. "
        "This blueprint must tell the renderer how to handle the hook, pacing, motion graphics, sound design, and chapter escalation. "
        "Output strict JSON with keys: version, visual_engine, format_preset, analysis_required_before_generation, "
        "hook_strategy, pacing_strategy, motion_strategy, sound_strategy, retention_targets, scoring_rubric, chapter_blueprints. "
        "chapter_blueprints must be an array of objects with keys: focus, hook_job, shock_device, visual_motif, motion_note, sound_note, retention_goal, improvement_focus."
    )
    user_prompt = (
        f"Template: {template}\n"
        f"Format preset: {format_preset}\n"
        f"Topic: {topic}\n"
        f"Title: {input_title}\n"
        f"Description: {input_description}\n"
        f"Chapter count: {chapter_count}\n"
        f"Target seconds per chapter: {chapter_target_sec}\n"
        f"Source bundle: {json.dumps(source_bundle or {}, ensure_ascii=True)}\n"
        f"Source analysis: {json.dumps(source_analysis or {}, ensure_ascii=True)}\n"
        f"Connected channel context: {json.dumps(channel_context or {}, ensure_ascii=True)}\n"
        f"Matched channel series cluster: {json.dumps(series_context.get('selected_cluster') or {}, ensure_ascii=True)}\n"
        f"Matched channel series cluster context: {_clip_text(str(series_context.get('cluster_context', '') or ''), 1200)}\n"
        f"Catalyst channel memory: {json.dumps(series_context.get('memory_view') or {}, ensure_ascii=True)}\n"
        f"Reference documentary corpus: {_clip_text(reference_corpus_context, 3000)}\n"
        "Use this marketing doctrine as operating context:\n"
        f"{marketing_doctrine_text_fn(strategy_notes) if marketing_doctrine_text_fn else strategy_notes}"
    )
    try:
        raw = await xai_json_completion_fn(system_prompt, user_prompt, temperature=0.35, timeout_sec=60) if xai_json_completion_fn else {}
        return _normalize_catalyst_edit_blueprint(raw, heuristic, chapter_count)
    except Exception as e:
        fallback = _normalize_catalyst_edit_blueprint({}, heuristic, chapter_count)
        fallback["retention_targets"]["main_opportunity"] = _clip_text("Fallback blueprint used after strategy issue. " + str(e), 220)
        return fallback

