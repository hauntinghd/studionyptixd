"""
Alternate History Battles — visual style locked 2026-05-10 (PR #145).

Casey 2026-05-10: 'Alt-history wants to do skeleton AI which we will
NOT be doing, we will be using manniquins.' This module swaps the
prior painterly Kings-and-Generals cinematic style for a **porcelain
mannequin** grammar — same family as Empire Magnates' red-porcelain
cast lock (see long_form/v5_pipeline.py + project_em_grammar_locked.md),
adapted for alt-history battle scenes.

Why mannequins for alt-history:
  - Stylized AI generation reads as intentional craft rather than
    failed photoreal.
  - Mannequin faces dodge the deepfake-uncanny-valley problem of
    rendering specific real historical figures (Napoleon, Caesar)
    while still giving viewers a clear focal cast.
  - Same cast grammar across Empire Magnates + Alt-History means
    Catalyst learning signal compounds — both channels share the
    "porcelain commander, photoreal world" lane.
  - Casey 2026-05-08: 'Cryptic Science host channel wants real names
    in VO/captions, only-if-needed in visuals.' Porcelain mannequins
    satisfy both: VO + captions name Napoleon, the mannequin doesn't
    need to LOOK like Napoleon.

Reference grade: Empire Magnates v5 episodes (red porcelain on
photoreal world) + Pudding Cups gold standard (PR #128's
em_grammar_locked.md decode). The mannequin shell is what makes the
channel signature unmistakable on the thumbnail rail.

This module keeps the legacy function names (assemble_scene_prompt,
NEG_STILL) so the rest of skeleton_ai/* and skeleton_ai_router.py
keeps working without churn. The OLD skeleton constants are preserved
as no-op aliases.
"""

# ──────────────────────────────────────────────────────────────────────
# THE ALT-HISTORY MANNEQUIN base style — porcelain shell + photoreal
# world + period-correct gear painted on the mannequins.
# ──────────────────────────────────────────────────────────────────────
ALT_BATTLES_BASE_STYLE = (
    "ABSOLUTE CAST RULE: every human figure in this image is a smooth, "
    "stylized PORCELAIN MANNEQUIN — clean glazed ceramic body, no facial "
    "features beyond a subtle suggestion of brow + nose ridge, no eyes, "
    "no mouth. Pure white/off-white porcelain shell with period-correct "
    "armor, helmets, robes, and weapons painted/strapped ON the mannequin "
    "body — never bare skin, never real human faces, never anatomical "
    "skeletons, never modern action figures. The porcelain catches the "
    "light dramatically. Cracked-glaze accents on the chest plate of "
    "commanders for visual hierarchy.\n\n"
    "WORLD: photoreal cinematic battlefield — real-looking terrain "
    "(dirt, grass, stone, water, snow, sand depending on the topic), "
    "real atmospheric haze, real volumetric lighting, real cinematic "
    "depth-of-field. The porcelain cast moves through a photographically "
    "rendered world.\n\n"
    "GEAR: period-accurate armor, weapons, banners, and unit insignia "
    "for the era depicted — phalanx pikes + bronze cuirass for ancient "
    "Greeks, lorica segmentata + scutum + gladius for Romans, scale-mail "
    "+ recurve bow for Mongols, plate-mail + longsword + heraldic "
    "surcoat for European knights, musket + bicorne + line-infantry "
    "coat for Napoleonic, kepi + Springfield rifle for American Civil "
    "War. Real historical flag colors.\n\n"
    "COMPOSITION: cinematic battle framing — wide army-formation "
    "establishing shots, hero close-ups on commander mannequins, "
    "mid-action collision shots. Dramatic dawn-or-dusk lighting with "
    "one strong directional key. Shallow cinematic depth-of-field. "
    "Vertical 9:16 frame for shorts.\n\n"
    "PALETTE: bronze + crimson, charcoal + gold, smoke + iron, "
    "weathered leather. The porcelain cast reads as off-white against "
    "the saturated photoreal world.\n\n"
    "Cinematic battle photography of porcelain-cast historical "
    "combatants in a real-feeling world. NOT a painting. NOT anime. "
    "NOT a real-person photo. NOT a skeleton/anatomical figure. NOT "
    "modern. Porcelain mannequins only, in period gear, photoreal "
    "world."
)


# Per-image NEG — kills regression paths most likely to appear when
# the model is asked to render porcelain-mannequin alt-history battles.
NEG_STILL = (
    # Cast regression — anything that breaks the porcelain mannequin lock
    "real human face, real skin texture, eye sockets, eyeballs, mouth, "
    "lips, teeth, photoreal celebrity face, identifiable celebrity, "
    "anatomical skeleton body, hollow eye sockets, skull head, exposed "
    "ribcage, undead, zombie, action figure, plastic toy, lego figure, "
    "barbie doll, ball-jointed-doll, "
    # Gear regression — wrong era
    "modern uniforms, modern firearms (unless gunpowder era is "
    "explicitly the topic), modern tanks, helicopters, jets, kevlar, "
    "MOLLE webbing, anachronistic gear, wrong-era armor, generic "
    "fantasy armor, generic fantasy swords, plastic-looking armor, "
    "costumed actors, cosplay, reenactor photograph, "
    # Style regression
    "anime style, manga style, chibi proportions, cartoon mascot, "
    "funko, oil painting, watercolor, ink illustration, comic book "
    "panel, low-poly 3D, video-game low-poly render, blurry, low "
    "quality, low resolution, "
    # Text/branding regression
    "text overlap, watermark in body, logo on armor, fake brand names, "
    "fake foreign-language text, captions baked into image, in-image "
    "text labels, modern flatscreen monitors, modern signage, "
    # Hand regression (porcelain mannequin hands are simpler — the
    # model still tries to render real-finger anatomy and produces 6
    # fingers; flag it hard)
    "deformed hands, extra fingers, fused fingers"
)


def assemble_scene_prompt(scene_action: str, outfit: str = "",
                          *, mint_bg: bool = False, bare_torso: bool = False) -> str:
    """
    Compose a per-scene prompt for the Alt-History Mannequin renderer.

    The scene_action carries the full cinematic battle setup: army
    positions, formations, terrain, weather, who-is-attacking-whom.

    The `outfit` arg describes PERIOD-COSTUME details that get painted
    onto the focal commander/unit mannequin. Empty string is fine —
    the scene_action usually carries enough.

    `mint_bg` and `bare_torso` are kept for backward-compat with old
    Skeleton AI callers and silently ignored (the mannequin grammar
    locks neither a mint backdrop nor bare-torso allowance).
    """
    scene_action = (scene_action or "").strip().rstrip(".")
    outfit = (outfit or "").strip()

    parts = [ALT_BATTLES_BASE_STYLE]
    if outfit:
        parts.append(
            f"\n\nFEATURED COMMANDER/UNIT — gear painted onto the porcelain "
            f"mannequin: {outfit}."
        )
    if scene_action:
        parts.append(
            f"\n\nSCENE: {scene_action}. Render the cast as porcelain "
            f"mannequins moving through this photoreal scene."
        )
    parts.append(
        "\n\nVertical 9:16 frame. Cinematic dramatic lighting, period-"
        "correct real-world gear painted onto porcelain bodies, real "
        "depth-of-field, atmospheric haze. PORCELAIN MANNEQUIN CAST "
        "ONLY — no real human faces, no skeletons, no toys."
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
