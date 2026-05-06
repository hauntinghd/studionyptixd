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

# The character has TWO modes — only one applies per render. The
# assemble_scene_prompt function picks which mode based on whether
# `outfit` is provided.

# Mode A — wearing a costume.
# Treat the body INSIDE the costume as a normal humanoid (no anatomical
# reveal). Only the SKULL is bone-visible. The costume itself is solid
# opaque metal/fabric. Hands appear bony only where gauntlets/sleeves end.
SKELETON_BASE_STYLE_COSTUMED = (
    "Photoreal cinematic render of a real-world scene. The lead character is "
    "an adult-height humanoid figure (~7.5 head heights) whose head is a "
    "smooth off-white anatomical human skull with hollow empty black eye "
    "sockets (no pupils, no glow). The skull sits naturally on a normal "
    "humanoid neck and body — the BODY underneath the costume is fully "
    "covered, implied normal, NOT skeletal-visible. The character wears the "
    "costume below as solid opaque material — fully painted metal armor / "
    "thick fabric clothing — with absolutely no anatomical reveal through "
    "the chestplate or torso. Bony skeletal hands appear only at the very "
    "edges of gauntlets / sleeve cuffs. The character stands at adult human "
    "height alongside real props and people in the environment. "
)

# Mode B — naked / no-costume / narrator beat.
# Full anatomical adult skeleton, all bones visible.
SKELETON_BASE_STYLE_NAKED = (
    "Photoreal cinematic render of a real-world scene. The lead character is "
    "a full anatomically accurate adult-height human skeleton (~7.5 head "
    "heights), slightly luminous off-white bone with subtle translucent "
    "quality. Hollow empty black eye sockets — NO pupils, NO dots, NO eye "
    "glow. Visible ribcage, spine, pelvis, full skeletal anatomy. The "
    "skeleton stands at adult human height alongside real props and real "
    "people in the environment. "
)

# Backward-compat alias — defaults to costumed mode (most common).
SKELETON_BASE_STYLE = SKELETON_BASE_STYLE_COSTUMED

# Backwards-compat alias — most callers no longer need this.
MINT_GREEN_BG = "Soft mint-green studio backdrop. "

# Per-image NEG — kills the chibi/mascot regression + the X-ray-through-
# costume regression + previous failure modes.
NEG_STILL = (
    "text, watermark, logo, "
    "cartoon mascot, chibi, funko, big head, oversized head, child proportions, "
    "small body, 3-4 head heights, "
    "white dot pupils, dot pupils, eye dots, glowing eyes, glowing eyeballs, "
    "demonic eyes, supernatural eye glow, laser beams from eyes, red laser eyes, "
    "polished plastic toy, porcelain shell, glass body, translucent gel skin, "
    # Reject X-ray-through-costume: when wearing armor/clothing, costume is opaque
    "see-through armor, transparent armor, ribcage visible through chestplate, "
    "spine visible through armor, x-ray costume, see-through costume, "
    "bones glowing through clothing, "
    "exposed brain, cracked skull, yellowed skull, deformed hands, extra fingers, "
    "blurry, low quality"
)


_NAKED_OUTFIT_TOKENS = {"", "none", "naked", "no clothing", "no outfit", "n/a", "-",
                       "no costume", "bare", "unclothed", "nude"}


def assemble_scene_prompt(scene_action: str, outfit: str, mint_bg: bool = False) -> str:
    """
    Compose a per-scene prompt.

    Mode is picked from `outfit`:
      - empty / 'none' / 'naked' / etc → Mode B (full anatomical skeleton)
      - any other value               → Mode A (skull on costumed body)

    Different base styles per mode — solves the regression where one
    universal base prompt told Seedream "skeleton" and "in armor" at
    the same time, producing X-ray-through-chestplate output.
    """
    outfit = (outfit or "").strip().rstrip(".")
    scene_action = (scene_action or "").strip().rstrip(".")

    is_naked = outfit.lower() in _NAKED_OUTFIT_TOKENS

    if is_naked:
        parts = [SKELETON_BASE_STYLE_NAKED]
    else:
        parts = [SKELETON_BASE_STYLE_COSTUMED, f"Costume detail: {outfit}. "]

    if mint_bg:
        parts.append(MINT_GREEN_BG)
    if scene_action:
        parts.append(f"Scene: {scene_action}. ")
    parts.append("Vertical 9:16 frame. Cinematic dramatic lighting, photoreal materials, real-world depth.")
    return "".join(parts)
