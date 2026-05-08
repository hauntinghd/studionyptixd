"""
Alternate History Battles — visual style locked 2026-05-08.

Replaces the prior Skeleton AI canonical (mint backdrop / anatomical skull /
white-bone character) with the new Alt-History Battles aesthetic: Kings-and-
Generals-meets-Total-War-meets-Ridley-Scott painterly cinematic battle
visuals. The genre rewards stylized AI generation — viewers expect epic
painterly compositions, not photoreal documentary.

Per Casey 2026-05-08: 'AI-generated historical battle recreations or
alternate history scenarios — like What if Napoleon fought Alexander the
Great. History buffs + alternate history fans. AI generation reads as
intentional rather than deceptive.'

Reference grade: Kings and Generals YouTube cinematics + Total War Three
Kingdoms in-engine cinematics + Ridley Scott (Gladiator, Kingdom of Heaven,
Napoleon) atmospheric battle composition.

This module keeps the legacy function names (assemble_scene_prompt, NEG_STILL)
so the rest of skeleton_ai/* and skeleton_ai_router.py keeps working without
churn. The OLD skeleton constants are preserved as no-op aliases.
"""

# THE ALT-HISTORY BATTLES base style — tight enough for cheap models.
ALT_BATTLES_BASE_STYLE = (
    "Cinematic painterly battle illustration in the style of Kings and "
    "Generals + Total War + Ridley Scott historical epic. Wide army-formation "
    "establishing shots, hero close-ups of commanders, mid-action collision "
    "shots, weather/terrain detail. Period-accurate gear shown clearly: "
    "right helmet, right armor, right weapons for the era depicted "
    "(phalanx pikes for ancient Greeks, gladii for Romans, longbows for "
    "English archers, muskets for Napoleonic line infantry, scale-mail "
    "for Mongols, plate-mail for European knights). Real historical flag "
    "colors and unit insignia. Atmospheric haze, dramatic dawn-or-dusk "
    "lighting with one strong directional key. Rich palette of bronze-and-"
    "crimson, charcoal-and-gold, smoke-and-iron, weathered-leather. Shallow "
    "cinematic depth-of-field. Painterly grain — looks like a high-budget "
    "animated battle cinematic, not a photograph. Vertical 9:16 frame for "
    "shorts. NOT photoreal. NOT modern. NOT anime. NOT cartoon. Period-"
    "correct historical painterly cinematic. "
)

# Per-image NEG — kills the regression paths most likely to appear in
# alt-history battle prompts.
NEG_STILL = (
    "modern uniforms, modern weapons, modern firearms (unless gunpowder "
    "era is explicitly the topic), modern tanks, helicopters, jets, "
    "kevlar, MOLLE webbing, "
    "anachronistic gear, wrong-era armor, generic fantasy armor, generic "
    "fantasy swords, plastic-looking armor, costumed actors, cosplay, "
    "anime style, manga style, chibi proportions, cartoon mascot, funko, "
    "photoreal Hollywood-CGI look, photo of real reenactors, photograph, "
    "photoreal portrait, "
    "skeleton character, anatomical skeleton body, hollow eye sockets, "
    "fantasy skeleton soldiers, walking dead, undead army (unless the "
    "topic explicitly calls for them), "
    "text overlap, watermark in body, logo on armor, fake brand names, "
    "fake foreign-language text, captions, in-image text labels, "
    "modern flatscreen monitors, modern signage, "
    "deformed hands, extra fingers, blurry, low quality, low resolution"
)


def assemble_scene_prompt(scene_action: str, outfit: str = "",
                          *, mint_bg: bool = False, bare_torso: bool = False) -> str:
    """
    Compose a per-scene prompt for the Alternate History Battles renderer.

    The scene_action carries the full cinematic battle setup: army positions,
    formations, terrain, weather, who-is-attacking-whom, etc.

    The `outfit` arg is reinterpreted for Alt-Battles: when supplied, it
    describes PERIOD-COSTUME details on the focal commander/unit. Empty
    string is fine — the scene_action usually carries enough.

    `mint_bg` and `bare_torso` are kept for backward-compat with old
    Skeleton AI callers and silently ignored.
    """
    scene_action = (scene_action or "").strip().rstrip(".")
    outfit = (outfit or "").strip()

    parts = [ALT_BATTLES_BASE_STYLE]
    if outfit:
        parts.append(f"Featured commander/unit costume detail: {outfit}. ")
    if scene_action:
        parts.append(f"Scene: {scene_action}. ")
    parts.append(
        "Vertical 9:16 frame. Cinematic dramatic lighting, period-correct "
        "real-world military materials, real depth-of-field, painterly "
        "grain, atmospheric haze. NOT photoreal — looks like a premium "
        "Total War / Kings and Generals battle cinematic."
    )
    return "".join(parts)


# ────────────────────────────────────────────────────────────────────────
# Backward-compat aliases — these names were exported from the old
# Skeleton AI version of this module. Keep them as no-op aliases so any
# stale imports across the worktree don't break the build. New code
# should reference ALT_BATTLES_BASE_STYLE instead.
# ────────────────────────────────────────────────────────────────────────
SKELETON_BASE_STYLE = ALT_BATTLES_BASE_STYLE
SKELETON_BASE_STYLE_BARE_TORSO = ALT_BATTLES_BASE_STYLE
SKELETON_BASE_STYLE_COSTUMED = ALT_BATTLES_BASE_STYLE
SKELETON_BASE_STYLE_NAKED = ALT_BATTLES_BASE_STYLE
MINT_GREEN_BG = ""
