# Skeleton Reference Video — Frame-by-Frame Analysis

**Date:** 2026-04-21
**Source:** `D:\recaps\do this\` (3 reference videos, 5–67s each, 1fps sampled)
**Purpose:** Extract exact Cryptic Science consistency signals so the LoRA training distribution matches the live channel aesthetic.

---

## The 9 canonical signals

### 1. Skull style = POLISHED PORCELAIN, NOT rough bone

What the reference shows:
- Smooth ivory-cream surface, almost plastic/ceramic sheen
- Subtle warm highlights on forehead + cheekbones
- Looks like a cartoon mask made of polished porcelain or cast rubber
- Clean matte finish on the cranium dome

What my current LoRA produces:
- Rough anatomical bone texture (like a medical-school skeleton)
- No glossy highlights
- Too "textured" — reads as literal bone, not Cryptic Science character

**Fix for v4 training prompt:** `"polished porcelain ivory skull with smooth rubber-like finish, glossy cast-ceramic surface, cartoon-character sheen, not rough anatomical bone texture"`

---

### 2. Eyes = LARGE cartoon black-and-white eyeballs

What the reference shows:
- Prominent, clearly visible in every close-up
- WHITE sclera fills most of the socket
- SMALL BLACK PUPIL with a tiny white reflection highlight
- Eyes track the camera / subject (hero expression)
- This is THE iconic Cryptic Science face signal

What my current LoRA produces:
- Empty dark sockets OR tiny dot "eyes"
- No sclera, no reflection highlights
- Reads as a skull, not a character

**Fix for v4 prompt (keep from v3):** `"large cartoon-style 3D eyeballs with white sclera, small black pupils with catchlight highlight, eyes looking at camera"`

---

### 3. Partial rubber mask, NOT full gel shell

What the reference shows:
- Rubber/silicone skin ONLY around specific zones:
  - Lower jaw + chin (partial mask)
  - Sides of the neck (where cervical vertebrae peek through)
  - Shoulders (where bone meets fabric)
- Rest of the body is CLOTHED like a real person — no see-through effect over torso

What my v3 dataset trained on:
- Prompt said "translucent gel-like body silhouette shell wrapping around the skeleton"
- This overshoots — the reference is 20% shell, not 100%

**Fix for v4 prompt:** Replace the "full gel shell" language with `"partial rubber/silicone mask skin highlights on the lower jaw, neck sides, and shoulders only. Body is clothed like a normal person with a bone-skeleton underneath — no see-through effect over torso or limbs."`

---

### 4. Clothing fits like a real person

What the reference shows:
- FBI navy suit, McDonald's polo, Red Bull racing suit, NASCAR suit, police dress uniform — ALL fit normally
- No exposed ribcage between suit lapels
- No see-through pants
- Only the skeleton's HEAD, HANDS, and sometimes NECK are visible bones
- The rest IS the outfit

What my current LoRA produces (after scale 1.3 anatomy lock):
- Exposed ribcage + pelvis + femurs visible through the suit
- Translucent outfit effect
- Doesn't match the reference

**Fix for v4 prompt:** Drop the anatomy_lock "exposed rib cage visible through outfit" token. Replace with: `"clothing fits the skeleton body like a real person wears it, opaque fabric, skeletal bones visible only at the head, hands, and cervical vertebrae between shirt collar and skull."`

---

### 5. Cervical vertebrae bridge — the signature "skeleton peek"

What the reference shows (in EVERY frame with a visible neck):
- Gap between shirt collar and jaw shows 3–5 exposed cervical vertebrae
- Spine bones clearly visible there, even when the rest of the body is fully clothed
- This is the PRIMARY visual cue that this is a skeleton in the outfit, not just a skull-masked person

**Fix for v4 prompt (add):** `"visible cervical vertebrae spine bones exposed between the shirt collar and the jaw, small gap showing 3-5 neck bones"`

---

### 6. Skeletal hands emerging from sleeves

What the reference shows:
- When hands are visible (holding phone, books, money, folder), the fingers + wrist + part of forearm are clearly skeletal
- Bones emerge naturally from the cuffs
- Occasional rubber/skin highlight on the back of the hand near knuckles

**Fix for v4 prompt (keep/add):** `"skeletal hands visible at wrists emerging from sleeves, bone fingers, partial rubber highlights near knuckles"`

---

### 7. Outfit consistency within a character arc

What the reference shows (FBI video as example):
- **Hook:** FBI agent in navy suit + white shirt + navy tie + FBI badge, close-up skull
- **Career progression:** Trainee → FBI TRAINEE navy t-shirt + training gear (DIFFERENT OUTFIT)
- **Senior:** Back to full navy suit + FBI badge, mature pose
- **VS comparison:** Same navy suit next to McDonald's skeleton

Identity stays CONSTANT (same skull, same body proportions) but the outfit SHIFTS with the story beat. The LoRA has to allow that.

**Implication for inference:** scene prompts should name the outfit per beat. Don't try to lock one outfit across all scenes — the point is the skeleton stays consistent WHILE the outfit varies.

---

### 8. Camera + composition rules

Pattern across all 3 videos:
- **Hook (scene 1):** close-up skull filling top 1/3 of frame, eyes to camera
- **Setup beats:** medium waist-up shot, clear outfit read
- **Environment beats:** full body in a real setting (mud field, destroyed building, office)
- **VS comparison:** two skeletons side-by-side, full body, both fitting frame
- **Data payoff:** skeleton holding a big check / phone screen / taped paper note

All 9:16 portrait, text overlays in Komika Axis-style bold sans, orange + cyan + white accents.

---

### 9. Background policy

- Mint green (#5AC8B8-ish) for hero/intro/outro shots (~60% of frames)
- Real environment for story beats (mud, destroyed building, office)
- Taped paper notes and price-card graphics as foreground layer
- "Cryptic Science" watermark footer, left-aligned bottom

---

## What this changes for the LoRA

**v3 LoRA (currently retraining):** will likely produce too much full-body gel shell because the training prompts said "translucent gel-like body silhouette shell wrapping the skeleton." The reference doesn't do that.

**v4 dataset (if v3 validation fails):** regenerate with updated prompts:

```
BASE_STYLE_v4 = (
    "Photorealistic 3D render, Unreal Engine 5 quality, 8K resolution. "
    "Polished porcelain ivory-cream skull with smooth rubber-like cast-ceramic finish, "
    "glossy cartoon-character sheen on the cranium, subtle warm highlights on forehead and cheekbones. "
    "LARGE cartoon-style 3D eyeballs with white sclera, small black pupils with catchlight highlight, "
    "eyes looking at the camera. "
    "Partial rubber/silicone mask skin highlights only on the lower jaw, neck sides, and shoulders. "
    "Visible cervical vertebrae spine bones exposed between the shirt collar and the jaw "
    "(3-5 neck bones visible in the gap). "
    "Skeletal hands visible at the wrists emerging from sleeves, bone fingers. "
    "Clothing fits the skeleton body like a real person wears it (opaque fabric, no see-through torso). "
    "Signature Cryptic Science YouTube channel character aesthetic."
)
```

v4 cost: ~$5 dataset + $2 retrain = $7 if we need a second iteration. Casey's fal balance after v3: ~$9, so v4 fits.

---

## Frames analyzed (10 representative samples)

- FBI f_001: hero skull close-up, navy suit, large eyes, "FBI" label
- FBI f_015: grey hoodie + backpack + books, student phase
- FBI f_030: FBI TRAINEE shirt, mud training, barbed wire
- FBI f_045: senior FBI agent, pistol + phone with bank balance
- FBI f_060: VS comparison (FBI suit vs McDonald's polo)
- NASCAR f_005: Red Bull racing suit + $70M title, money piles
- NASCAR f_020: racing crouch pose, sponsor patches visible
- NASCAR f_040: F1 vs NASCAR side-by-side VS
- Principal f_010: grey hoodie + books + backpack + ripped jeans
- Principal f_030: police dress uniform + cap + paycheck prop
- Principal f_050: charcoal suit + white shirt + black tie, $120K paycheck

Every single frame shows the same core canonical aesthetic — the 9 signals are universal across all 3 videos.
