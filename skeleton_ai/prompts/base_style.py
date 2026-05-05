"""
Canonical Skeleton AI character + scene style prompt.

Locked 2026-05-05 from reference videos at D:/recaps/do this/ and
124 reference frames at D:/recaps/do_this_frames_new/.

This is the GROUND TRUTH spec. Do not alter without re-validating against
reference frames. Prior specs (v4 porcelain / v5 rubber gel / v6 clear-glass /
v7 polished plastic) all rejected — see project_skeleton_spec_canonical.md.
"""

# Per-image base style — kept SHORT so cheaper models (ernie_image, free
# nano-banana) don't truncate. ~75 words. Full canonical signature lives in
# project_skeleton_spec_canonical.md and is enforced by NEG_STILL below.
SKELETON_BASE_STYLE = (
    "3D cartoon-mascot anatomical skeleton on solid mint-green studio backdrop. "
    "Pure white smooth bone skull with hollow dark eye sockets and small white "
    "dot pupils inside each socket. Body wears real opaque clothing for the "
    "scene's role; bones visible only at hands and neck. Vertical 9:16 frame, "
    "cinematic studio lighting. "
)

# Solid mint backdrop — kept as a separate token so callers can override
# (e.g., specific scene-level macro shots).
MINT_GREEN_BG = "Mint-green seamless backdrop (#5AC8B8). "

# Per-image NEG list — kept tight so cheaper models actually parse it.
# Hits the specific v4-v7 failure modes from project_skeleton_spec_canonical.md.
NEG_STILL = (
    "text, watermark, logo, glowing eyes, supernatural eyes, "
    "polished plastic toy, porcelain shell, glass body, translucent gel skin, "
    "see-through clothing, x-ray clothing, exposed brain, cracked skull, "
    "yellowed skull, deformed hands, extra fingers, blurry, low quality"
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
