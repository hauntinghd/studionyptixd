"""
Skeleton AI v6 — LOCKED 2026-05-06 (final).

THE skeleton character. Single fixed look. Identical across every beat,
every topic, every category. Per Casey: 'The skeleton is to be perfectly
consistent regardless of what the topic is. Custom script, remixing,
human limit, [marvel] DC, history, or ancient history mix, futuristic,
or [crypt]ic. Whatever it is, it must be consistent regardless, perfectly.'

Reference frames (all show the same character):
  D:/recaps/do_this_frames_new/v1_05.jpg  — school hallway w/ teen + triangle
  D:/recaps/do_this_frames_new/v1_15.jpg  — concrete bunker w/ teen + suit
  D:/recaps/do_this_frames_new/v1_30.jpg  — apartment doorway w/ guy
  D:/recaps/do_this_frames_new/v1_50.jpg  — brewery w/ bearded man
  Recording studio frame Casey sent      — w/ producer + guitarist

Character is NEVER costumed. The skeleton is the silent observer / viewer
proxy in every scene. Topic-relevant content (Marvel heroes, history
artifacts, futuristic tech, crypto charts) appears as SCENE PROPS — the
skeleton stays the same naked anatomical character.

Earlier rejected approaches retained for reference:
  v1 (mint backdrop + cartoon-mascot + dot pupils + opaque costume) — wrong
  v2 (mint dropped + scene-driven backgrounds) — closer
  v3 (anatomical adult naked, hollow sockets, photoreal world) — RIGHT
  v4/v5 (restored mint + costumes for Marvel/DC) — WRONG, walked back
"""

# THE skeleton — the only character. Fixed across every render.
# ~140 words. Tight enough for cheap models.
SKELETON_BASE_STYLE = (
    "Photoreal cinematic render of THE Cryptic Science / Skeleton AI "
    "character — a tall adult-height (~7.5 head heights, life-size next to "
    "real humans) anatomically accurate full human skeleton. The bones are "
    "off-white slightly luminous with a subtle translucent quality, like "
    "lit glass-bone. Visible full skeletal anatomy: skull, cervical spine, "
    "ribcage, sternum, pelvis, full arm and leg bones, fingers, feet. "
    "Hollow dark eye sockets — no pupils, no cute mascot dots, no glow; "
    "occasional very faint subtle dot of reflected light deep inside the "
    "socket but eyes read as empty. Visible teeth, jaw, nasal cavity. "
    "The skeleton is ALWAYS naked anatomical bone — NEVER wears clothing, "
    "armor, costumes, or accessories. The character stands at adult human "
    "height alongside real props and real human figures in the scene. "
    "It is the silent witness / observer character, not an action figure. "
    "Same exact appearance every render. "
)

# Legacy mint constant kept for backward-compat — unused in v6 default path.
MINT_GREEN_BG = ""

# Hulk-style bare-torso variant — NOT USED in v6. The base spec is already
# bare anatomical. Kept as a no-op alias so older callers don't crash.
SKELETON_BASE_STYLE_BARE_TORSO = SKELETON_BASE_STYLE
SKELETON_BASE_STYLE_COSTUMED = SKELETON_BASE_STYLE
SKELETON_BASE_STYLE_NAKED = SKELETON_BASE_STYLE

# Per-image NEG — kills every regression path we hit today.
NEG_STILL = (
    "text overlap, watermark in body, logo on skull, "
    # Reject cartoon mascot proportions:
    "chibi, funko, big head, oversized head, child proportions, small body, "
    "3-4 head heights, cartoon mascot proportions, cute mascot eyes, "
    "white dot pupils, dot pupils, eye dots, heart-shaped pupils, heart eyes, "
    "star pupils, "
    # Reject eye glow:
    "glowing red eyes, demonic eye glow, supernatural eye glow, "
    "laser beams from eyes, red laser eyes, "
    # Reject costumed-skeleton approach (canonical is NEVER costumed):
    "skeleton wearing costume, skeleton in armor, skeleton in iron man armor, "
    "skeleton in thor armor, skeleton in superman costume, "
    "armored skeleton, costumed skeleton, skeleton dressed as superhero, "
    "skeleton in clothing, skeleton wearing suit, skeleton wearing polo, "
    "skeleton wearing uniform, "
    # Reject non-canonical material:
    "polished plastic toy, porcelain shell, glass body, translucent gel skin, "
    # Reject mint studio backdrop (channel was mint earlier; v6 isn't):
    "mint green studio backdrop, plain green background, solid color backdrop, "
    "studio backdrop, plain background, "
    # Reject skull defects:
    "exposed brain, cracked skull, yellowed skull, weathered skull, "
    "deformed hands, extra fingers, blurry, low quality"
)


def assemble_scene_prompt(scene_action: str, outfit: str = "",
                          *, mint_bg: bool = False, bare_torso: bool = False) -> str:
    """
    Compose a per-scene prompt. v6 ignores `outfit` and `mint_bg` and
    `bare_torso` — the skeleton is always the same naked anatomical
    character. The scene_action carries the full cinematic setting and
    any topic-relevant props (Marvel action figures, real heroes in the
    background, comic books, etc.).

    Older callers passing outfit/mint_bg/bare_torso won't break — those
    arguments are accepted but silently ignored, the character spec
    stays locked.
    """
    scene_action = (scene_action or "").strip().rstrip(".")

    parts = [SKELETON_BASE_STYLE]
    if scene_action:
        parts.append(f"Scene: {scene_action}. ")
    parts.append(
        "Vertical 9:16 frame. Cinematic dramatic lighting, photoreal "
        "real-world materials, real depth of field, real human figures "
        "and real props composited at proper scale alongside the skeleton."
    )
    return "".join(parts)
