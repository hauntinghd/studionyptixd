"""
Skeleton AI v3 character + scene style.

UPDATED 2026-05-06 (afternoon) per Casey's reference image:
  An anatomically-accurate FULL-SIZE adult human skeleton standing in a
  real-world photoreal scene (high-school hallway with students walking
  past), no clothing, hollow empty eye sockets, slight luminous/translucent
  bone material — integrated like a person, not a cartoon.

Rejected from v1/v2:
  - "cartoon-mascot" / "stylized chibi" wording — produced chibi proportions
    and Funko-style heads.
  - Small white dot pupils inside sockets — produced cute mascot eyes.
  - Mint-green studio backdrop default — fought scene_action settings.
  - "6-7 head heights" qualifier — got interpreted as chibi 3-4 heads.

v3 ground truth:
  - Anatomically accurate adult human skeleton, real adult proportions
    (~7.5 head heights, life-size when next to humans).
  - Hollow black empty eye sockets — no pupils, no dots, no glow.
  - Slightly luminous / faintly translucent bone — looks photoreal, not toy.
  - Naked by default; OUTFIT only when narration calls for a costume role.
  - NO mint backdrop — every scene is a real cinematic environment.
  - Captions (rendered post-render): single-word UPPERCASE white bold with
    black 5-6px stroke, centered low.
"""

# Per-image character spec — what stays constant across every scene.
# ~70 words. Cheap-model friendly.
SKELETON_BASE_STYLE = (
    "Photoreal cinematic render of an anatomically accurate adult human "
    "skeleton standing in a real-world environment. Full life-size adult "
    "proportions (~7.5 head heights), realistic bone density, slightly "
    "luminous off-white bone with subtle translucent quality. Hollow empty "
    "black eye sockets — NO pupils, NO dots, NO eye glow. Visible ribcage, "
    "spine, pelvis, full skeletal anatomy. Integrated naturally into the "
    "scene at human scale alongside real props and people. "
)

# Backwards-compat alias — most callers no longer need this.
MINT_GREEN_BG = "Soft mint-green studio backdrop. "

# Per-image NEG — kills the chibi/mascot regression + previous failure modes.
NEG_STILL = (
    "text, watermark, logo, "
    "cartoon mascot, chibi, funko, big head, oversized head, child proportions, "
    "small body, 3-4 head heights, "
    "white dot pupils, dot pupils, eye dots, glowing eyes, glowing eyeballs, "
    "demonic eyes, supernatural eye glow, laser beams from eyes, red laser eyes, "
    "polished plastic toy, porcelain shell, glass body, translucent gel skin, "
    "see-through clothing, x-ray clothing, exposed brain, cracked skull, "
    "yellowed skull, deformed hands, extra fingers, blurry, low quality"
)


def assemble_scene_prompt(scene_action: str, outfit: str, mint_bg: bool = False) -> str:
    """
    Compose a per-scene prompt.

    Default (mint_bg=False) lets `scene_action` describe the full cinematic
    setting — hallway, rooftop, Asgardian throne room, Krypton city, etc.
    The skeleton is a full-size adult, integrated like a real person.

    `outfit` is OPTIONAL now — when narration doesn't call for a costume,
    pass an empty string (or "no clothing" / "naked") and the skeleton
    appears anatomically as bone.
    """
    outfit = (outfit or "").strip().rstrip(".")
    scene_action = (scene_action or "").strip().rstrip(".")

    parts = [SKELETON_BASE_STYLE]
    if outfit and outfit.lower() not in {"none", "naked", "no clothing", "no outfit", "n/a", "-"}:
        parts.append(f"Outfit: {outfit}. ")
    if mint_bg:
        parts.append(MINT_GREEN_BG)
    if scene_action:
        parts.append(f"Scene: {scene_action}. ")
    parts.append("Vertical 9:16 frame. Cinematic dramatic lighting, photoreal materials, real-world depth.")
    return "".join(parts)
