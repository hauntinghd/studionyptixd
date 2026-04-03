import re

from backend_catalyst_core import _clip_text, _dedupe_preserve_order


def _catalyst_default_visual_engine(template: str, format_preset: str) -> str:
    fmt = str(format_preset or "").strip().lower()
    if fmt == "documentary":
        return "Catalyst Documentary 3D"
    if fmt == "recap":
        return "Catalyst Recap Cinema"
    if fmt == "story_channel":
        return "Catalyst Storyworld"
    if template == "chatstory":
        return "Catalyst Chat Hybrid"
    return "Catalyst Explainer 3D"


def _catalyst_default_sound_profile(topic: str, input_title: str, format_preset: str) -> tuple[str, str]:
    text = f"{topic} {input_title} {format_preset}".lower()
    if re.search(r"\b(kill|killed|crime|dark|secret|disturb|psychology|war|danger|fear|mind)\b", text):
        return "dark_psychology", "cinematic_dark_tension"
    if format_preset == "documentary":
        return "premium_documentary", "documentary_tension"
    if format_preset == "recap":
        return "high_pressure_recap", "kinetic_recap_bed"
    if format_preset == "story_channel":
        return "story_channel_cinematic", "story_pulse_bed"
    return "clean_explainer", "precision_explainer_bed"


def _heuristic_catalyst_chapter_blueprints(
    *,
    chapter_count: int,
    subject: str,
    improvement_moves: list[str] | None = None,
    retention_findings: list[str] | None = None,
    pressure_profile: dict | None = None,
) -> list[dict]:
    improvement_moves = [str(v).strip() for v in list(improvement_moves or []) if str(v).strip()]
    retention_findings = [str(v).strip() for v in list(retention_findings or []) if str(v).strip()]
    pressure_profile = dict(pressure_profile or {})
    pressure_scores = {
        str(row.get("key", "") or ""): int(row.get("score", 0) or 0)
        for row in list(pressure_profile.get("categories") or [])
        if str(row.get("key", "") or "").strip()
    }
    pressure_priorities = [str(v).strip() for v in list(pressure_profile.get("next_run_priorities") or []) if str(v).strip()]
    base_arc = [
        ("Hook the viewer with the most concrete high-stakes reveal.", "Start on the promise and immediate consequence.", "unexpected reveal", "hero object under aggressive spotlight", "fast push-in then hard contrast cutaway", "opening hit plus sub-bass tension", "No throat-clearing. First 10 seconds must state the payoff."),
        ("Expose the hidden mechanism that makes the topic work.", "Translate abstract theory into visible cause-and-effect.", "mechanism cutaway", "macro cross-section or exploded system view", "precise motion-graphic overlays and clean dolly move", "tight whooshes, ticks, and system sweeps", "Make the viewer feel smarter, not confused."),
        ("Escalate with a consequence the viewer can feel immediately.", "Move from explanation into personal stakes.", "consequence illustration", "stylized human-versus-system composition", "pattern interrupt with scale change", "impact sting then low drone", "Introduce tension before the halfway point."),
        ("Contrast myth versus reality or before versus after.", "Deliver a clean reversal that resets attention.", "contrast frame", "before-versus-after split world or timeline board", "sharp cut, wipe, or x-ray transition", "contrast hit plus quick suction transition", "Break repetition with a visual reversal."),
        ("Prove the idea through a memorable system or case study.", "Ground the theory in something people will remember.", "proof moment", "miniature world, map room, or operating table system view", "orchestrated map or system sweep", "documentary trailer accents", "Raise trust while keeping tension alive."),
        ("Land the payoff and set up the next obsession.", "Close the loop and leave one more open loop.", "payoff + lingering question", "symbolic final hero shot with one unsettling cue", "controlled slow pull-back with clean end-card energy", "resolve hit with trailing ambience", "End with resolution plus curiosity, not a flat summary."),
    ]
    blueprints: list[dict] = []
    for idx in range(max(1, int(chapter_count or 1))):
        base = base_arc[idx % len(base_arc)]
        improvement = improvement_moves[idx % len(improvement_moves)] if improvement_moves else ""
        retention = retention_findings[idx % len(retention_findings)] if retention_findings else ""
        focus = f"{base[0]} Subject focus: {subject}."
        hook_job = base[1]
        shock_device = base[2]
        visual_motif = f"{base[3]} built around {subject}."
        motion_note = base[4]
        sound_note = base[5]
        retention_goal = _clip_text(retention or base[6], 180)
        if idx == 0 and pressure_scores.get("hook", 0) >= 60:
            hook_job = "Start directly on the consequence, contradiction, or hidden control point before any setup."
            shock_device = "counterintuitive reveal + immediate payoff"
            focus = f"Open on the sharpest high-stakes contradiction around {subject}. Subject focus: {subject}."
            retention_goal = _clip_text(pressure_priorities[0] if pressure_priorities else retention_goal, 180)
        if pressure_scores.get("pacing", 0) >= 60 and idx <= 2:
            motion_note = _clip_text(f"{motion_note}; force a pattern interrupt within 10 to 12 seconds", 180)
        if pressure_scores.get("visuals", 0) >= 60:
            visual_motif = _clip_text(f"{visual_motif} Avoid repeating the same hero-object framing back-to-back.", 220)
        if pressure_scores.get("sound", 0) >= 60:
            sound_note = _clip_text(f"{sound_note}; add a silence pocket before the main reveal", 180)
        blueprints.append({
            "index": idx,
            "focus": focus,
            "hook_job": hook_job,
            "shock_device": shock_device,
            "visual_motif": visual_motif,
            "motion_note": motion_note,
            "sound_note": sound_note,
            "retention_goal": retention_goal,
            "improvement_focus": _clip_text(improvement or "Keep the promise clearer and the payoff more immediate.", 180),
        })
    return blueprints


def _catalyst_scene_execution_profile(
    *,
    edit_blueprint: dict | None = None,
    chapter_blueprint: dict | None = None,
    scene_index: int = 0,
    total_scenes: int = 0,
) -> dict:
    edit_blueprint = dict(edit_blueprint or {})
    chapter_blueprint = dict(chapter_blueprint or {})
    hook_strategy = dict(edit_blueprint.get("hook_strategy") or {})
    pacing_strategy = dict(edit_blueprint.get("pacing_strategy") or {})
    motion_strategy = dict(edit_blueprint.get("motion_strategy") or {})
    sound_strategy = dict(edit_blueprint.get("sound_strategy") or {})
    execution_strategy = dict(edit_blueprint.get("execution_strategy") or {})
    format_preset = str(edit_blueprint.get("format_preset", "") or "").strip().lower()
    niche_key = str(edit_blueprint.get("niche_key", "") or "").strip().lower()
    series_anchor = _clip_text(str(edit_blueprint.get("series_anchor", "") or ""), 120)
    niche_execution_notes = [str(v).strip() for v in list(edit_blueprint.get("niche_execution_notes") or []) if str(v).strip()]
    is_recap_lane = bool(format_preset == "recap" or niche_key == "manga_recap")
    total = max(1, int(total_scenes or 1))
    idx = max(0, int(scene_index or 0))
    opening_span = 2 if total >= 6 else 1
    closing_span = 2 if total >= 5 else 1
    interrupt_every = max(2, int(round(float(pacing_strategy.get("pattern_interrupt_interval_sec", 12) or 12) / 5.0)))
    is_opening = idx < opening_span
    is_closer = idx >= max(0, total - closing_span)
    is_interrupt = idx > 0 and (idx + 1) % interrupt_every == 0
    scene_role = "build"
    if is_opening:
        scene_role = "hook"
    elif is_closer:
        scene_role = "payoff"
    elif is_interrupt:
        scene_role = "pattern_interrupt"
    opening_intensity = str(execution_strategy.get("opening_intensity", "measured") or "measured").strip().lower()
    interrupt_strength = str(execution_strategy.get("interrupt_strength", "medium") or "medium").strip().lower()
    caption_rhythm = str(execution_strategy.get("caption_rhythm", "balanced") or "balanced").strip().lower()
    sound_density = str(execution_strategy.get("sound_density", "controlled") or "controlled").strip().lower()
    cut_profile = str(execution_strategy.get("cut_profile", "cinematic") or "cinematic").strip().lower()
    voice_pacing_bias = str(execution_strategy.get("voice_pacing_bias", "steady") or "steady").strip().lower()
    visual_variation_rule = _clip_text(str(execution_strategy.get("visual_variation_rule", "") or ""), 220)
    payoff_hold_sec = max(0.7, min(1.8, float(execution_strategy.get("payoff_hold_sec", 1.1) or 1.1)))
    execution_intensity = "steady"
    if sound_density in {"punchy", "trailer-heavy"} or opening_intensity in {"aggressive", "attack"}:
        execution_intensity = "high"
    if is_opening and opening_intensity == "attack":
        execution_intensity = "attack"
    if is_closer and payoff_hold_sec >= 1.25:
        execution_intensity = "payoff_hold"
    motion_cues = _dedupe_preserve_order([
        str(chapter_blueprint.get("motion_note", "") or ""),
        *list(motion_strategy.get("camera_language") or [])[:3],
        *list(motion_strategy.get("motion_graphics") or [])[:3],
        *list(motion_strategy.get("visual_rules") or [])[:2],
        visual_variation_rule,
        "Open on a hard first-frame image and compress setup instantly." if is_opening and opening_intensity == "attack" else "",
        "Keep the first beats more aggressive and proof-led than explanatory." if is_opening and opening_intensity in {"aggressive", "attack"} else "",
        "Introduce a visible contrast reset in this beat." if is_interrupt else "",
        "Make the interrupt feel abrupt, readable, and impossible to ignore." if is_interrupt and interrupt_strength == "high" else "",
        "Land the visual claim immediately before widening context." if is_opening else "",
        "Use a deliberate payoff linger before the next transition." if is_closer and payoff_hold_sec >= 1.2 else "",
        "Resolve the idea with a clear payoff image and controlled pull-back." if is_closer else "",
    ], max_items=8, max_chars=180)
    sound_cues = _dedupe_preserve_order([
        str(chapter_blueprint.get("sound_note", "") or ""),
        *list(sound_strategy.get("mix_notes") or [])[:3],
        *list(sound_strategy.get("silence_rules") or [])[:2],
        *list(sound_strategy.get("voice_direction") or [])[:2],
        "Use denser trailer punctuation and stronger transient design." if sound_density in {"punchy", "trailer-heavy"} else "",
        "Use recap-style swells and heavier reveal hits." if sound_density == "trailer-heavy" else "",
        "Use the silence pocket right before the reveal lands." if is_interrupt or is_closer else "",
        "Front-load one decisive sting before the explanation starts." if is_opening else "",
    ], max_items=8, max_chars=180)
    retention_cues = _dedupe_preserve_order([
        str(chapter_blueprint.get("retention_goal", "") or ""),
        str(chapter_blueprint.get("improvement_focus", "") or ""),
        str(hook_strategy.get("promise", "") or "") if is_opening else "",
        str(hook_strategy.get("first_30s_mission", "") or "") if is_opening else "",
        str(hook_strategy.get("open_loop", "") or ""),
        "Keep captions staccato and punch the key claim words hard." if caption_rhythm == "staccato" else "",
        "Keep captions measured and clean so the mechanism stays readable." if caption_rhythm == "measured" else "",
        "Reset attention with a new contrast or consequence right now." if is_interrupt else "",
        "Make the payoff concrete enough that the next chapter feels inevitable." if is_closer else "",
    ], max_items=8, max_chars=180)
    if is_recap_lane:
        motion_cues = _dedupe_preserve_order([
            f"Keep the visual language locked to {series_anchor}." if series_anchor else "",
            "Use recap-grade panel energy, power hierarchy, and chapter-turn escalation.",
            "Avoid sterile documentary staging or generic explainer objects.",
            *niche_execution_notes[:2],
            *motion_cues,
        ], max_items=8, max_chars=180)
        sound_cues = _dedupe_preserve_order([
            "Use sharper reveal hits, energy swells, and rank-jump accents.",
            "Push the recap trailer bed harder on betrayal, survival, or power turns.",
            *niche_execution_notes[:1],
            *sound_cues,
        ], max_items=8, max_chars=180)
        retention_cues = _dedupe_preserve_order([
            f"Escalate the next obsession inside {series_anchor}." if series_anchor else "Escalate the next obsession inside the same recap universe.",
            "Make every beat feel like a stronger chapter-turn, not a summary retread.",
            *retention_cues,
        ], max_items=8, max_chars=180)
    return {
        "scene_role": scene_role,
        "is_opening": is_opening,
        "is_closer": is_closer,
        "is_interrupt": is_interrupt,
        "motion_cues": motion_cues,
        "sound_cues": sound_cues,
        "retention_cues": retention_cues,
        "music_profile": str(sound_strategy.get("music_profile", "") or ""),
        "sfx_profile": str(sound_strategy.get("sfx_profile", "") or ""),
        "transition_style": str(motion_strategy.get("transition_style", "") or pacing_strategy.get("transition_style", "") or "smooth"),
        "series_anchor": series_anchor,
        "niche_execution_notes": niche_execution_notes,
        "pattern_interrupt_interval_sec": int(pacing_strategy.get("pattern_interrupt_interval_sec", 12) or 12),
        "voice_direction": _dedupe_preserve_order(list(sound_strategy.get("voice_direction") or []), max_items=6, max_chars=180),
        "execution_intensity": execution_intensity,
        "opening_intensity": opening_intensity,
        "interrupt_strength": interrupt_strength,
        "caption_rhythm": caption_rhythm,
        "sound_density": sound_density,
        "cut_profile": cut_profile,
        "voice_pacing_bias": voice_pacing_bias,
        "visual_variation_rule": visual_variation_rule,
        "payoff_hold_sec": round(float(payoff_hold_sec), 2),
    }


def _catalyst_audio_mix_profile(
    edit_blueprint: dict | None = None,
    *,
    format_preset: str = "",
    render_horror_audio: bool = False,
) -> dict:
    edit_blueprint = dict(edit_blueprint or {})
    sound_strategy = dict(edit_blueprint.get("sound_strategy") or {})
    pacing_strategy = dict(edit_blueprint.get("pacing_strategy") or {})
    execution_strategy = dict(edit_blueprint.get("execution_strategy") or {})
    music_profile = str(sound_strategy.get("music_profile", "") or "").strip().lower()
    mix_notes = " ".join(str(v).strip().lower() for v in list(sound_strategy.get("mix_notes") or []) if str(v).strip())
    pattern_interrupt = int(pacing_strategy.get("pattern_interrupt_interval_sec", 12) or 12)
    voice_pacing_bias = str(execution_strategy.get("voice_pacing_bias", "steady") or "steady").strip().lower()
    sound_density = str(execution_strategy.get("sound_density", "controlled") or "controlled").strip().lower()
    caption_rhythm = str(execution_strategy.get("caption_rhythm", "balanced") or "balanced").strip().lower()
    voice_speed = 1.0
    if pattern_interrupt <= 9:
        voice_speed = 1.08
    elif pattern_interrupt <= 11:
        voice_speed = 1.05
    elif format_preset in {"documentary", "explainer", "recap"}:
        voice_speed = 1.03
    if voice_pacing_bias == "front-loaded":
        voice_speed += 0.03
    elif voice_pacing_bias == "tension-rise":
        voice_speed += 0.015
    ambience_gain = 0.18
    bgm_gain = 0.55
    sfx_gain = 1.0
    if music_profile in {"documentary_tension", "precision_explainer_bed"}:
        ambience_gain = 0.15
        bgm_gain = 0.42
    elif music_profile in {"kinetic_recap_bed"}:
        ambience_gain = 0.22
        bgm_gain = 0.35
    elif music_profile in {"story_pulse_bed"}:
        ambience_gain = 0.17
        bgm_gain = 0.46
    if sound_density == "punchy":
        sfx_gain += 0.08
        bgm_gain = min(0.62, bgm_gain + 0.03)
    elif sound_density == "trailer-heavy":
        sfx_gain += 0.14
        bgm_gain = min(0.64, bgm_gain + 0.04)
        ambience_gain = max(0.14, ambience_gain - 0.02)
    if "silence" in mix_notes:
        ambience_gain = max(0.12, ambience_gain - 0.03)
    if caption_rhythm == "staccato":
        voice_speed += 0.01
    elif caption_rhythm == "measured":
        voice_speed -= 0.01
    return {
        "voice_speed": max(0.92, min(1.16, voice_speed)),
        "voice_gain": 1.0,
        "ambience_gain": ambience_gain,
        "bgm_gain": bgm_gain,
        "sfx_gain": sfx_gain,
        "bgm_required": bool(render_horror_audio or format_preset in {"documentary", "explainer", "recap", "story_channel"}),
        "music_profile": music_profile or ("cinematic_dark_tension" if render_horror_audio else "documentary_tension"),
    }


def _catalyst_chapter_blueprint_for_index(edit_blueprint: dict | None, chapter_index: int) -> dict:
    blueprint = dict(edit_blueprint or {})
    chapter_blueprints = list(blueprint.get("chapter_blueprints") or [])
    if 0 <= int(chapter_index) < len(chapter_blueprints):
        return dict(chapter_blueprints[int(chapter_index)] or {})
    return {}
