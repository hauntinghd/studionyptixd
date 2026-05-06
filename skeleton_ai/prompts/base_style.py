"""
Skeleton AI v4 character + scene style — restored to canonical 2026-05-06.

Verified against actual reference frames at
`D:/recaps/do this/extracted/fbi_mcdonalds/frames/` (267 frames). Casey:
'we need them to look like the marvel/dc heros regardless, and it does
support them, ik cause iv got proof D:\\recaps\\do this\\extracted\\fbi_mcdonalds\\frames'.

What the canonical actually is (per the reference frames):
  - MINT GREEN BACKDROP every scene (channel signature, ~#5AC8B8) with
    real photoreal props (grill, kitchen equipment, restaurant interior,
    desks, real human customers) COMPOSITED ON the mint stage. NOT a
    pure mint void; NOT a pure photoreal world. A hybrid TV-set look.
  - Adult-height stylized cartoon-mascot anatomical skeleton (~6-6.5
    head heights — slightly stylized, not pure realism, NOT chibi).
  - Smooth white anatomical skull with hollow dark eye sockets and
    SMALL CALM DOT PUPILS (not "hollow empty no pupils" — the v3 mistake).
  - Slight glassy/translucent quality on the SKULL only — body inside
    clothes is fully covered (NOT see-through, NOT X-ray).
  - Real opaque clothing for the role: red McDonald's polo + visor +
    trousers; navy FBI suit + tie + name tag; manager dress shirt + tie;
    student grey hoodie + jeans; police blue uniform with chevron;
    NASCAR/F1 racing suit; etc.
  - Skeletal hands visible BELOW cuffs (always); cervical vertebrae
    visible ABOVE collar (always — 3-5 vertebrae); sometimes forearms
    if short-sleeve.
  - Two-tier captions baked in: ORANGE bold for KEY DATA ($ amounts,
    names, ages), WHITE bold for filler narration. Black 5-6px stroke
    on all text. Komika Axis or similar bold sans-serif comic style.
  - "Cryptic Science" retro stencil watermark bottom-center always.

Marvel / DC adaptation (per Casey's directive):
  - Same template — mint backdrop + photoreal scene props/people + dot
    pupils + adult stylized proportions — but the role's outfit is the
    hero's CANONICAL costume: full red-and-gold Iron Man Mark 85 armor,
    full Asgardian breastplate + winged helmet + Mjolnir for Thor, full
    blue/red Superman suit + S-shield + cape, etc. Skull visible above
    collar / through open helmet faceplate.
  - HULK EXCEPTION: torn purple pants + bare anatomical ribcage/torso
    visible (faint green tint as 'rage' aura) since canon Hulk is
    bare-chested. The only character where the spec exposes torso bones
    by design.
"""

# Cryptic Science / Skeleton AI v5 — frame as "humanoid with skull head"
# instead of "skeleton in costume" to dodge the memetic-trained "partial
# armor on skeleton" output Seedream defaults to.
#
# Verified pattern from f_0030.jpg + f_0150.jpg + f_0090.jpg reference frames:
# the body is a NORMAL HUMANOID FIGURE wearing real opaque clothing; only the
# HEAD is a smooth white anatomical skull, the cervical-vertebrae NECK shows
# above the collar, and the HANDS are skeletal at sleeve cuffs. Forearms
# below short-sleeves are bare bone. Everywhere else: implied normal body.
SKELETON_BASE_STYLE = (
    "Cinematic 3D render in the Cryptic Science / Skeleton AI YouTube channel "
    "style. A stylized cartoon-mascot character with the body of a normal "
    "adult humanoid figure (~6-6.5 head heights) and a smooth white "
    "anatomical bone skull as the head — like a person whose head was "
    "replaced with a skull. The skull has hollow dark eye sockets containing "
    "TWO TINY ROUND WHITE DOTS as pupils (calm cute mascot eyes, not hearts, "
    "not stars). Visible teeth and jaw. A short cervical spine (3-5 vertebrae) "
    "shows between the skull and the shirt collar. The character is otherwise "
    "treated like a regular human in clothing: the costume / uniform / armor "
    "is fully opaque, draped or worn normally over a humanoid body. The ONLY "
    "skeletal-bone elements visible are: the SKULL HEAD, the CERVICAL "
    "VERTEBRAE above the collar, the SKELETAL HANDS at the cuff edges, and "
    "if the costume is short-sleeve, the SKELETAL FOREARMS below the cuffs. "
    "Everything else (torso, upper arms under sleeves, legs under trousers) "
    "looks like a normal humanoid body shape inside the clothing — NO ribs, "
    "spine, or pelvis showing through anywhere. "
)

# Mint green channel-signature backdrop — DEFAULT ON.
MINT_GREEN_BG = (
    "Solid mint-green / cyan studio backdrop (#5AC8B8 to #6FD4C0) with a "
    "subtle smooth gradient. Photoreal props and real humans composited on "
    "the mint stage as the scene calls for (kitchen equipment, desks, real "
    "customers, lab gear). Soft cinematic lighting from upper-left key. "
)

# Hulk-exception variant — bare-chested torn pants + naked ribcage.
SKELETON_BASE_STYLE_BARE_TORSO = (
    "Cinematic 3D render of the Cryptic Science skeleton character — a "
    "stylized adult-height anatomical skeleton (~6-6.5 head heights). "
    "Smooth white anatomical bone skull with hollow dark eye sockets and "
    "small calm dot pupils. The character is bare-chested, exposing the "
    "full anatomical ribcage, spine, and visible scapula. Wears only torn "
    "trousers covering the lower body. Skeletal hands always exposed. "
    "Faint colored glow may surround the body if the role calls for it "
    "(green for Hulk-rage, etc). "
)

# Per-image NEG — strongest set yet. Targets the memetic "skeleton + iron man"
# / "skeleton + thor" partial-armor trope that Seedream defaults to.
NEG_STILL = (
    "text overlap, watermark in body, logo on skull, "
    "chibi, funko, big head, oversized head, child proportions, small body, "
    "3-4 head heights, "
    "heart-shaped pupils, heart eyes, star pupils, cartoon heart eyes, "
    "glowing red eyes, demonic eye glow, supernatural eye glow, "
    "laser beams from eyes, red laser eyes, "
    "polished plastic toy, porcelain shell, glass body, translucent gel skin, "
    # Hard ban on the meme-trained "skeleton in armor" partial-coverage trope:
    "skeleton in armor, half-skeleton, skeletal mech, anatomical mech-suit, "
    "robot-skeleton hybrid, mechanical bone arms, segmented bone limbs, "
    "skeleton dressed as iron man, skeleton dressed as thor, "
    "armored skeleton, skeleton inside armor pieces, "
    "bare skeletal arms, bare skeletal legs, bare skeletal torso, "
    "bare ribcage visible, exposed ribcage, exposed spine, exposed pelvis, "
    "ribcage motif on chestplate, ribcage decoration on armor, "
    "skeletal motif on breastplate, anatomical chest engraving, "
    "armor pieces only, partial armor, isolated armor pieces, "
    "skeleton in floating armor pieces, "
    "see-through armor, transparent armor, ribcage visible through chestplate, "
    "spine visible through armor, x-ray costume, see-through costume, "
    "bones glowing through clothing, glowing rib outlines, glowing chest cavity, "
    "exposed brain, cracked skull, yellowed skull, weathered skull, "
    "deformed hands, extra fingers, blurry, low quality"
)


def assemble_scene_prompt(
    scene_action: str,
    outfit: str,
    *,
    mint_bg: bool = True,
    bare_torso: bool | None = None,
) -> str:
    """
    Compose a per-scene prompt.

    mint_bg=True (default) — canonical mint-green backdrop. Real props
    composite onto it per scene_action.

    bare_torso=True triggers the Hulk-exception variant. Can also be
    encoded by Grok via a `[BARE_TORSO] ` prefix on the outfit string —
    assemble_scene_prompt strips the sentinel and applies the variant.
    """
    outfit = (outfit or "").strip()

    # Detect the sentinel from derive_beat_visuals.
    if bare_torso is None:
        bare_torso = False
    if outfit.startswith("[BARE_TORSO]"):
        bare_torso = True
        outfit = outfit[len("[BARE_TORSO]"):].strip()

    outfit = outfit.rstrip(".")
    scene_action = (scene_action or "").strip().rstrip(".")

    base = SKELETON_BASE_STYLE_BARE_TORSO if bare_torso else SKELETON_BASE_STYLE
    parts = [base]
    if mint_bg:
        parts.append(MINT_GREEN_BG)
    if outfit and outfit.lower() not in {"none", "naked", "n/a", "-"}:
        parts.append(f"Outfit: {outfit}. ")
    if scene_action:
        parts.append(f"Scene: {scene_action}. ")
    parts.append("Vertical 9:16 frame. Cinematic studio composition with photoreal props and dramatic lighting.")
    return "".join(parts)
