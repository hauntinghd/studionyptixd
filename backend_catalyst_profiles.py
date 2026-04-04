import re
import time

from backend_catalyst_core import (
    _catalyst_infer_archetype,
    _catalyst_text_overlap_score,
    _catalyst_title_novelty_score,
    _clip_text,
    _dedupe_preserve_order,
)


_CATALYST_SHORT_ARCHETYPE_EXECUTION_PACKS = {
    "recap_escalation": {
        "opening_intensity": "attack",
        "interrupt_strength": "high",
        "caption_rhythm": "staccato",
        "sound_density": "trailer-heavy",
        "cut_profile": "contrast-cut",
        "voice_pacing_bias": "front-loaded",
        "visual_variation_rule": "Keep every beat tied to a new power turn, betrayal, rank jump, or world-specific escalation cue.",
        "pattern_interrupt_interval_sec": 8,
        "payoff_hold_sec": 1.12,
        "default_execution_intensity": "high",
    },
    "systems_documentary": {
        "opening_intensity": "aggressive",
        "interrupt_strength": "medium",
        "caption_rhythm": "measured",
        "sound_density": "controlled",
        "cut_profile": "dynamic-cut",
        "voice_pacing_bias": "steady",
        "visual_variation_rule": "Alternate hidden-system cutaways, consequence frames, and macro detail instead of repeating the same hero object.",
        "pattern_interrupt_interval_sec": 11,
        "payoff_hold_sec": 1.18,
        "default_execution_intensity": "medium",
    },
    "dark_psychology": {
        "opening_intensity": "attack",
        "interrupt_strength": "high",
        "caption_rhythm": "pulse-synced",
        "sound_density": "punchy",
        "cut_profile": "contrast-cut",
        "voice_pacing_bias": "tension-rise",
        "visual_variation_rule": "Keep contrast high between invasive personal consequence shots and hidden-mechanism explanation shots.",
        "pattern_interrupt_interval_sec": 9,
        "payoff_hold_sec": 1.1,
        "default_execution_intensity": "high",
    },
    "trading_execution": {
        "opening_intensity": "attack",
        "interrupt_strength": "high",
        "caption_rhythm": "staccato",
        "sound_density": "punchy",
        "cut_profile": "punch-cut",
        "voice_pacing_bias": "front-loaded",
        "visual_variation_rule": "Rotate between chart proof, order-flow/execution proof, and loss-versus-edge contrast instead of repeating the same screen angle.",
        "pattern_interrupt_interval_sec": 8,
        "payoff_hold_sec": 1.08,
        "default_execution_intensity": "high",
    },
    "power_history": {
        "opening_intensity": "aggressive",
        "interrupt_strength": "medium",
        "caption_rhythm": "pulse-synced",
        "sound_density": "controlled",
        "cut_profile": "contrast-cut",
        "voice_pacing_bias": "steady",
        "visual_variation_rule": "Alternate map power shifts, leader close-focus, and consequence boards to keep stakes visually climbing.",
        "pattern_interrupt_interval_sec": 10,
        "payoff_hold_sec": 1.2,
        "default_execution_intensity": "medium",
    },
    "science_mechanism": {
        "opening_intensity": "aggressive",
        "interrupt_strength": "medium",
        "caption_rhythm": "measured",
        "sound_density": "controlled",
        "cut_profile": "dynamic-cut",
        "voice_pacing_bias": "steady",
        "visual_variation_rule": "Alternate macro mechanism views, simplified causal diagrams, and human consequence frames to avoid textbook repetition.",
        "pattern_interrupt_interval_sec": 11,
        "payoff_hold_sec": 1.16,
        "default_execution_intensity": "medium",
    },
    "gaming_breakdown": {
        "opening_intensity": "attack",
        "interrupt_strength": "high",
        "caption_rhythm": "staccato",
        "sound_density": "punchy",
        "cut_profile": "punch-cut",
        "voice_pacing_bias": "front-loaded",
        "visual_variation_rule": "Alternate winning state, broken mechanic proof, and decisive mistake frames instead of staying in one camera angle.",
        "pattern_interrupt_interval_sec": 8,
        "payoff_hold_sec": 1.02,
        "default_execution_intensity": "high",
    },
    "viral_explainer": {
        "opening_intensity": "attack",
        "interrupt_strength": "medium",
        "caption_rhythm": "pulse-synced",
        "sound_density": "punchy",
        "cut_profile": "contrast-cut",
        "voice_pacing_bias": "steady",
        "visual_variation_rule": "Use one dominant symbol per beat and force a clear contrast reset before the viewer settles into repetition.",
        "pattern_interrupt_interval_sec": 9,
        "payoff_hold_sec": 1.08,
        "default_execution_intensity": "medium",
    },
}


_CATALYST_SHORT_TEMPLATE_EXECUTION_OVERRIDES = {
    "skeleton": {
        "opening_intensity": "attack",
        "interrupt_strength": "high",
        "caption_rhythm": "staccato",
        "sound_density": "punchy",
        "cut_profile": "contrast-cut",
        "voice_pacing_bias": "front-loaded",
        "visual_variation_rule": "Use one dominant skeleton comparison or contradiction per beat and avoid repeating the same body pose back to back.",
        "pattern_interrupt_interval_sec": 8,
        "default_execution_intensity": "high",
    },
    "daytrading": {
        "opening_intensity": "attack",
        "interrupt_strength": "high",
        "caption_rhythm": "staccato",
        "sound_density": "punchy",
        "cut_profile": "punch-cut",
        "voice_pacing_bias": "front-loaded",
        "visual_variation_rule": "Keep every beat tied to a different proof layer: setup, entry, risk, trap, or payoff.",
        "pattern_interrupt_interval_sec": 8,
        "default_execution_intensity": "high",
    },
    "chatstory": {
        "opening_intensity": "attack",
        "interrupt_strength": "high",
        "caption_rhythm": "staccato",
        "sound_density": "punchy",
        "cut_profile": "punch-cut",
        "voice_pacing_bias": "front-loaded",
        "visual_variation_rule": "Keep the conflict legible as a new message turn, reveal, or betrayal beat every scene.",
        "pattern_interrupt_interval_sec": 7,
        "default_execution_intensity": "high",
    },
    "motivation": {
        "opening_intensity": "aggressive",
        "interrupt_strength": "medium",
        "caption_rhythm": "pulse-synced",
        "sound_density": "controlled",
        "cut_profile": "dynamic-cut",
        "voice_pacing_bias": "steady",
        "visual_variation_rule": "Rotate between obstacle, consequence, and breakthrough imagery so the speech does not sit on one static mood.",
        "pattern_interrupt_interval_sec": 10,
        "default_execution_intensity": "medium",
    },
    "story": {
        "opening_intensity": "aggressive",
        "interrupt_strength": "medium",
        "caption_rhythm": "pulse-synced",
        "sound_density": "controlled",
        "cut_profile": "dynamic-cut",
        "voice_pacing_bias": "steady",
        "visual_variation_rule": "Keep emotional subject continuity, but rotate environment, scale, and consequence framing to avoid visual loopiness.",
        "pattern_interrupt_interval_sec": 10,
        "default_execution_intensity": "medium",
    },
}


def _merge_short_execution_pack(*packs: dict | None) -> dict:
    merged: dict = {}
    for raw in packs:
        pack = dict(raw or {})
        for key, value in pack.items():
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    continue
            elif value is None:
                continue
            merged[key] = value
    return merged


def _normalize_shorts_angle_seed(text: str, max_chars: int = 100) -> str:
    value = re.sub(r"\s+", " ", str(text or "").strip())
    if not value:
        return ""
    value = re.sub(r"\s*\|.*$", "", value).strip()
    value = re.sub(r"#shorts?\b", "", value, flags=re.IGNORECASE).strip()
    value = value.strip(" -|:;,.")
    return _clip_text(value, max_chars)


def _catalyst_short_execution_pack(
    *,
    template: str,
    topic: str = "",
    scene_texts: list[str] | None = None,
    archetype_key: str = "",
    pacing_mode: str = "standard",
) -> dict:
    normalized_template = str(template or "").strip().lower()
    scene_texts = [str(v).strip() for v in list(scene_texts or []) if str(v).strip()]
    inferred = _catalyst_infer_archetype(
        topic,
        " ".join(scene_texts[:6]),
        niche_key="day_trading" if normalized_template == "daytrading" else "",
        format_preset="documentary" if normalized_template == "daytrading" else "",
    )
    resolved_key = str(archetype_key or inferred.get("key", "") or "").strip().lower()
    if not resolved_key:
        resolved_key = "viral_explainer"
    resolved_label = str(inferred.get("label", "") or resolved_key.replace("_", " ").title()).strip()
    pack = _merge_short_execution_pack(
        _CATALYST_SHORT_ARCHETYPE_EXECUTION_PACKS.get("viral_explainer") or {},
        _CATALYST_SHORT_ARCHETYPE_EXECUTION_PACKS.get(resolved_key) or {},
        _CATALYST_SHORT_TEMPLATE_EXECUTION_OVERRIDES.get(normalized_template) or {},
    )
    mode = str(pacing_mode or "standard").strip().lower()
    interrupt_interval = max(7, int(pack.get("pattern_interrupt_interval_sec", 9) or 9))
    if mode == "very_fast":
        interrupt_interval = max(7, interrupt_interval - 2)
        pack["interrupt_strength"] = "high"
        if str(pack.get("opening_intensity", "") or "").strip().lower() == "aggressive":
            pack["opening_intensity"] = "attack"
    elif mode == "fast":
        interrupt_interval = max(7, interrupt_interval - 1)
    elif mode == "slow":
        interrupt_interval = min(13, interrupt_interval + 1)
        if str(pack.get("caption_rhythm", "") or "").strip().lower() == "staccato":
            pack["caption_rhythm"] = "pulse-synced"
    pack["pattern_interrupt_interval_sec"] = interrupt_interval
    pack["archetype_key"] = resolved_key
    pack["archetype_label"] = resolved_label
    return pack


def _catalyst_rank_shorts_angle_candidates(
    *,
    template: str,
    topic: str = "",
    channel_context: dict | None = None,
    selected_cluster: dict | None = None,
    memory_public: dict | None = None,
    benchmark_titles: list[str] | None = None,
    trend_titles: list[str] | None = None,
    hook_moves: list[str] | None = None,
    packaging_moves: list[str] | None = None,
    visual_moves: list[str] | None = None,
    keyword_moves: list[str] | None = None,
    trend_hunt_enabled: bool = False,
    max_items: int = 6,
) -> list[dict]:
    channel_context = dict(channel_context or {})
    selected_cluster = dict(selected_cluster or {})
    memory_public = dict(memory_public or {})
    benchmark_titles = [str(v).strip() for v in list(benchmark_titles or []) if str(v).strip()]
    trend_titles = [str(v).strip() for v in list(trend_titles or []) if str(v).strip()]
    hook_moves = [str(v).strip() for v in list(hook_moves or []) if str(v).strip()]
    packaging_moves = [str(v).strip() for v in list(packaging_moves or []) if str(v).strip()]
    visual_moves = [str(v).strip() for v in list(visual_moves or []) if str(v).strip()]
    keyword_moves = [str(v).strip() for v in list(keyword_moves or []) if str(v).strip()]
    recent_titles = [str(v).strip() for v in list(channel_context.get("recent_upload_titles") or []) if str(v).strip()]
    recent_short_angles = [str(v).strip() for v in list(memory_public.get("recent_short_angles") or []) if str(v).strip()]
    promoted_shorts_angles = [str(v).strip() for v in list(memory_public.get("promoted_shorts_angles") or []) if str(v).strip()]
    demoted_shorts_angles = [str(v).strip() for v in list(memory_public.get("demoted_shorts_angles") or []) if str(v).strip()]
    stored_angle_rows = [dict(v or {}) for v in list(memory_public.get("public_shorts_angle_candidates") or []) if isinstance(v, dict)]
    recent_title_keys = {title.lower() for title in recent_titles}
    recent_short_angle_keys = {title.lower() for title in recent_short_angles}
    promoted_angle_keys = {title.lower() for title in promoted_shorts_angles}
    demoted_angle_keys = {title.lower() for title in demoted_shorts_angles}
    now_ts = time.time()
    cluster_titles = [str(v).strip() for v in list(selected_cluster.get("sample_titles") or []) if str(v).strip()]
    cluster_keywords = [str(v).strip() for v in list(selected_cluster.get("keywords") or []) if str(v).strip()]
    inferred = _catalyst_infer_archetype(
        topic,
        " ".join(benchmark_titles[:4]),
        " ".join(trend_titles[:4]),
        " ".join(cluster_titles[:3]),
        " ".join(keyword_moves[:6]),
        niche_key=str(selected_cluster.get("niche_key", "") or ""),
        format_preset="documentary" if str(template or "").strip().lower() == "daytrading" else "",
    )
    archetype_label = str(inferred.get("label", "") or "").strip()
    archetype_keywords = [str(v).strip() for v in list(inferred.get("keywords") or []) if str(v).strip()]
    shared_keywords = _dedupe_preserve_order([*keyword_moves[:6], *cluster_keywords[:6], *archetype_keywords[:4]], max_items=8, max_chars=40)
    seed_rows: list[tuple[str, str, float]] = []
    raw_topic = _normalize_shorts_angle_seed(topic, max_chars=96)
    if raw_topic:
        seed_rows.append((raw_topic, "topic", 1.02))
    for title in trend_titles[:6]:
        seed_rows.append((_normalize_shorts_angle_seed(title, max_chars=96), "trend", 1.24))
    for title in benchmark_titles[:6]:
        seed_rows.append((_normalize_shorts_angle_seed(title, max_chars=96), "benchmark", 1.08))
    for title in cluster_titles[:4]:
        seed_rows.append((_normalize_shorts_angle_seed(title, max_chars=96), "cluster", 0.96))
    for title in recent_titles[:4]:
        seed_rows.append((_normalize_shorts_angle_seed(title, max_chars=96), "channel", 0.9))
    candidates: list[dict] = []
    seen: set[str] = set()
    for index, (seed, source, base_weight) in enumerate(seed_rows):
        if not seed:
            continue
        normalized_seed = seed.lower()
        if normalized_seed in seen:
            continue
        seen.add(normalized_seed)
        novelty_score = _catalyst_title_novelty_score(seed, source_title=topic, recent_titles=recent_titles)
        overlap_score = max((_catalyst_text_overlap_score(seed, title) for title in recent_titles[:4]), default=0.0)
        promoted_overlap = max((_catalyst_text_overlap_score(seed, title) for title in promoted_shorts_angles[:4]), default=0.0)
        demoted_overlap = max((_catalyst_text_overlap_score(seed, title) for title in demoted_shorts_angles[:4]), default=0.0)
        fatigue_overlap = max((_catalyst_text_overlap_score(seed, title) for title in recent_short_angles[:5]), default=0.0)
        matched_memory_row = {}
        matched_memory_overlap = 0.0
        for raw_row in stored_angle_rows:
            memory_angle = str((raw_row or {}).get("angle", "") or "").strip()
            if not memory_angle:
                continue
            overlap = _catalyst_text_overlap_score(seed, memory_angle)
            if overlap > matched_memory_overlap:
                matched_memory_overlap = overlap
                matched_memory_row = dict(raw_row or {})
        exact_recent_repeat = normalized_seed in recent_title_keys or normalized_seed in recent_short_angle_keys
        exact_promoted_match = normalized_seed in promoted_angle_keys
        exact_demoted_match = normalized_seed in demoted_angle_keys
        memory_times_seen = max(0, int(matched_memory_row.get("times_seen", 0) or 0))
        memory_last_seen_at = float(matched_memory_row.get("last_seen_at", 0.0) or 0.0)
        memory_hours_since_seen = ((now_ts - memory_last_seen_at) / 3600.0) if memory_last_seen_at > 0 else 999.0
        keyword_hits = [kw for kw in shared_keywords if kw and kw.lower() in normalized_seed]
        keyword_bonus = min(0.24, len(keyword_hits) * 0.05)
        length_bonus = 0.08 if 24 <= len(seed) <= 72 else (-0.06 if len(seed) > 86 else 0.0)
        novelty_bonus = ((float(novelty_score) - 50.0) / 100.0) * 0.5
        recency_bonus = 0.12 if trend_hunt_enabled and source == "trend" else 0.0
        promoted_bonus = 0.22 if promoted_overlap >= 0.54 else (0.1 if promoted_overlap >= 0.38 else 0.0)
        if exact_promoted_match:
            promoted_bonus = max(promoted_bonus, 0.18)
        demoted_penalty = 0.45 if demoted_overlap >= 0.5 else (0.18 if demoted_overlap >= 0.34 else 0.0)
        if exact_demoted_match:
            demoted_penalty = max(demoted_penalty, 0.62)
        fatigue_penalty = 0.38 if fatigue_overlap >= 0.76 else (0.16 if fatigue_overlap >= 0.56 else 0.0)
        if exact_recent_repeat:
            fatigue_penalty = max(fatigue_penalty, 0.58)
        saturation_penalty = 0.0
        if matched_memory_overlap >= 0.48:
            saturation_penalty = min(0.26, max(0, memory_times_seen - 1) * 0.045)
            if memory_hours_since_seen <= 24:
                saturation_penalty += 0.12
            elif memory_hours_since_seen <= 72:
                saturation_penalty += 0.06
        stale_reactivation_bonus = 0.0
        if matched_memory_overlap >= 0.48 and 96 <= memory_hours_since_seen <= 336 and memory_times_seen <= 3:
            stale_reactivation_bonus = 0.06
        overlap_penalty = 0.32 if source == "channel" and overlap_score >= 0.78 else (0.12 if overlap_score >= 0.58 else 0.0)
        score = round(
            base_weight
            + keyword_bonus
            + length_bonus
            + novelty_bonus
            + recency_bonus
            + promoted_bonus
            + stale_reactivation_bonus
            - demoted_penalty
            - fatigue_penalty
            - saturation_penalty
            - overlap_penalty
            - (index * 0.015),
            3,
        )
        angle = seed
        template_key = str(template or "").strip().lower()
        if template_key == "skeleton" and not re.search(r"\b(skeleton|bone|bones|skull)\b", angle, flags=re.IGNORECASE):
            if re.search(r"\b(vs\.?|versus|better|worse|stronger|weaker|job|salary|rich|poor)\b", angle, flags=re.IGNORECASE):
                angle = _clip_text(f"{angle} as a skeleton comparison", 96)
        elif template_key == "daytrading" and not re.search(r"\b(trade|trading|chart|setup|risk|market|stock|option|entry|exit)\b", angle, flags=re.IGNORECASE):
            angle = _clip_text(f"{angle} with a real trading consequence", 96)
        why_now = {
            "trend": "Fresh recent public YouTube trend title in this lane.",
            "benchmark": "Recurring public short benchmark pattern that is already getting reach.",
            "cluster": "Matches the connected channel's strongest series cluster.",
            "channel": "Close to the connected channel's current arena, but needs novelty.",
            "topic": "Directly matches the requested topic while keeping room for a fresher hook.",
        }.get(source, "Fresh candidate angle.")
        if trend_hunt_enabled and source == "trend":
            why_now = "Fresh breakout public trend title with strong novelty pressure for this lane."
        if exact_recent_repeat or fatigue_overlap >= 0.76:
            why_now = "Too close to a recently used short angle, so Catalyst should only reuse it if nothing fresher ranks higher."
        elif matched_memory_overlap >= 0.48 and memory_hours_since_seen <= 24:
            why_now = "Too close to a benchmark angle Catalyst refreshed recently, so it should give fresher variants first."
        elif exact_demoted_match or demoted_overlap >= 0.5:
            why_now = "Similar to a demoted short angle, so it needs a much stronger package if used."
        elif matched_memory_overlap >= 0.48 and 96 <= memory_hours_since_seen <= 336 and memory_times_seen <= 3:
            why_now = "This angle family cooled off long enough to retest with a fresher package."
        elif promoted_overlap >= 0.54:
            why_now = "Adjacent to a promoted learned short angle, but fresher than the recent repeats."
        candidates.append(
            {
                "angle": angle,
                "source": source,
                "score": score,
                "novelty_score": novelty_score,
                "why_now": why_now,
                "hook_move": _clip_text(hook_moves[min(index, len(hook_moves) - 1)] if hook_moves else (str(inferred.get("hook_rule", "") or "") or "Lead with the cleanest hidden payoff first."), 180),
                "packaging_move": _clip_text(packaging_moves[min(index, len(packaging_moves) - 1)] if packaging_moves else (str(inferred.get("packaging_rule", "") or "") or "Keep the package cleaner and sharper than the current lane."), 180),
                "visual_move": _clip_text(visual_moves[min(index, len(visual_moves) - 1)] if visual_moves else (str(inferred.get("visual_rule", "") or "") or "Use one dominant visual symbol with cleaner contrast."), 180),
                "keyword_bias": keyword_hits[:4] or shared_keywords[:4],
                "archetype_label": archetype_label,
                "times_seen": memory_times_seen,
                "hours_since_seen": round(memory_hours_since_seen, 2) if memory_hours_since_seen < 999 else None,
            }
        )
    candidates.sort(
        key=lambda row: (
            -float(row.get("score", 0.0) or 0.0),
            -int(row.get("novelty_score", 0) or 0),
            str(row.get("source", "") or ""),
            str(row.get("angle", "") or "").lower(),
        )
    )
    return [dict(row or {}) for row in candidates[: max(3, min(int(max_items or 6), 8))]]


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
