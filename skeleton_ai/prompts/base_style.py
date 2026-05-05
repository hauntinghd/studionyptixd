"""
Canonical Skeleton AI character + scene style prompt.

Locked 2026-05-05 from reference videos at D:/recaps/do this/ and
124 reference frames at D:/recaps/do_this_frames_new/.

This is the GROUND TRUTH spec. Do not alter without re-validating against
reference frames. Prior specs (v4 porcelain / v5 rubber gel / v6 clear-glass /
v7 polished plastic) all rejected — see project_skeleton_spec_canonical.md.
"""

# Per-image base style — stuff before the per-scene specifics.
SKELETON_BASE_STYLE = (
    "Cinematic 3D render of the Cryptic Science skeleton character — a "
    "stylized cartoon-mascot anatomical skeleton. PURE WHITE smooth anatomical "
    "bone skull (medical-grade detail, NOT polished plastic toy, NOT porcelain). "
    "HOLLOW DARK eye sockets (deep black) with SMALL WHITE DOT PUPILS centered "
    "inside each socket — eyes look calm and slightly cartoon-mascot, NEVER "
    "glowing, NEVER bright supernatural light. Visible teeth, jaw structure, "
    "nasal cavity. The body wears REAL OPAQUE CLOTHING per the scene's role "
    "(real fabric, real cotton/wool/synthetic, drapes naturally on the skeleton). "
    "Skeletal bones visible ONLY at: hands at wrists below cuffs, neck above "
    "collar (3-5 cervical vertebrae visible), and forearms when sleeves are "
    "short. Body proportions are normal adult (~6-7 head heights). "
)

# The mint green backdrop is the channel signature — every scene unless macro
# prop close-up overrides.
MINT_GREEN_BG = (
    "Solid mint green / cyan studio backdrop, color approximately #5AC8B8 "
    "to #6FD4C0 with a subtle smooth top-to-bottom gradient. Full bleed, "
    "no border. Cinematic studio lighting from upper-left key. "
)

# Per-image NEG list — suppresses the failure modes we identified in v6/v7.
NEG_STILL = (
    "text, watermark, logo, words, lettering, captions, signs, "
    "blur, low quality, deformed hands, extra fingers, multiple heads, child, "
    # Reject prior wrong-spec attempts:
    "polished plastic toy bones, glass shell body, clear acrylic body, "
    "translucent gel skin, rubber silicone wrap, x-ray fade, ghost-like body, "
    # Reject eye glow / supernatural artifacts:
    "glowing eyes, glowing eyeball, white glowing eye, bright eye glow, "
    "supernatural eyes, demonic eyes, possessed eyes, light beams from eyes, "
    "asymmetric eyes, mismatched eye colors, "
    # Reject background drift:
    "photoreal real-world environment, classroom, kitchen, hallway, "
    "office, courtroom — UNLESS scene specifies, "
    # Reject skull defects:
    "cracked skull, weathered skull, yellowed skull, beige skull, decayed skull, "
    # Reject body issues:
    "see-through clothing, x-ray clothing, transparent fabric, ghost in clothing, "
    "stunted body, floating legs, missing legs, deformed proportions"
)


def assemble_scene_prompt(scene_action: str, outfit: str, mint_bg: bool = True) -> str:
    """Compose a full per-scene prompt with canonical character + scene specifics."""
    parts = [SKELETON_BASE_STYLE]
    if mint_bg:
        parts.append(MINT_GREEN_BG)
    parts.append(f"OUTFIT: {outfit.strip()}. ")
    parts.append(f"ACTION/POSE: {scene_action.strip()}. ")
    parts.append("Vertical 9:16 frame. Cinematic studio composition.")
    return "".join(parts)
