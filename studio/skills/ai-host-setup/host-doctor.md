# Tactical Playbook — AI Host: Doctor

> **DEPRECATED (2026-04-29).** This file has been superseded by `T1-ai-host-setup.md` which provides a profession matrix covering 15 professions including doctor. Load T1 instead. This file is preserved for reference only — do NOT use as authoritative guidance.

## The goal

Generate a reference image of a medical professional sitting in their own home or modest home office, looking authentic — like a real doctor turned on their webcam to record a YouTube health video, NOT like a studio shoot. This image becomes the locked character reference for an AI avatar channel in the senior-health / doctor-advice niche (medical-authority channel style).

The output must read as: **"this is a real doctor talking to me from their living room about my health,"** not as **"this is a stock photo of a generic doctor."** That distinction is everything for audience trust, especially with senior demographics who are wary of polish.

## The non-negotiable details

These MUST be present or the image fails:

1. **Visible medical credibility** — at minimum a stethoscope around neck OR white coat hanging visibly nearby OR medical reference book on shelf. NOT all three (over-styled). Pick one or two.
2. **Realistic home setting** — looks like an actual room, not a studio backdrop. Wall has personal items (framed degree, family photo, plant). Slight clutter is good.
3. **Natural-looking light, not cinematic** — soft window light from one side, OR warm lamp light. NEVER three-point studio setup. NEVER backlit or dramatic shadow.
4. **Mid-shot framing** — head + shoulders + chest visible, suggesting a webcam set up at a desk. NOT close-up portrait.
5. **Slightly imperfect composition** — subject not perfectly centered, room not perfectly tidy, expression not posed. Realism = subtle imperfection.
6. **Direct or near-direct eye contact with camera** — they're talking to YOU, not posing for a magazine.
7. **Casual professional dress** — collared shirt, light sweater, or casual button-down. NOT scrubs (too clinical) and NOT a suit (too corporate).
8. **Age 45-65** — younger reads as inexperienced, older reads as past-prime. The sweet spot is "experienced enough to trust, sharp enough to be current."
9. **Photorealistic rendering** — not stylized, not illustrated, not anime, not 3D-render aesthetic.

## The do-not list

These ruin the output:

- ❌ Studio backdrop (gray seamless, ring light, professional setup)
- ❌ Three-point cinematic lighting (key + fill + rim)
- ❌ Wearing scrubs (reads "actively practicing in a hospital, not at home")
- ❌ Stethoscope draped around neck like a fashion accessory (over-styled)
- ❌ Crisp white teeth + perfect skin smoothing (uncanny-valley AI tell)
- ❌ Background blur so deep the room is unreadable (cinematic, not authentic)
- ❌ Centered composition with subject staring directly forward in formal pose
- ❌ Overly clean / staged room (a few personal items help)
- ❌ Suit and tie (too corporate for "doctor at home")
- ❌ Lab coat being worn while sitting at home desk (mismatched signals)
- ❌ "Serious" stock-photo expression — neutral or slight smile is realistic, dramatic furrowed brow is staged

## The locked prompt template

Use this as the base prompt. Variables in `[BRACKETS]` get filled per channel.

```
A [AGE: 50-60] year old [GENDER: male/female] medical doctor sitting at a wooden desk in their home office, photographed as if they had set up a webcam to record a health-advice YouTube video. They are wearing a [WARDROBE: collared button-down / light cardigan / casual professional sweater] in [COLOR: navy / forest green / muted earth tone], with a single stethoscope visible draped over a chair behind them. Behind them is a softly-lit wall with a single framed medical degree, a small bookshelf with medical reference books and a few personal photos, and a small plant. The room is warmly lit by natural daylight from a window to the left of the frame, with a small desk lamp providing fill. The subject is framed mid-shot from chest up, slightly off-center to the left, looking just to the right of the camera with a calm, slightly warm expression — not smiling broadly, not posed, just attentive. The image is photorealistic, captured as if with a basic webcam at slight upward angle (eye-level or just below), with realistic skin texture including subtle imperfections, slight stubble or natural makeup, no skin smoothing. Background is gently out of focus but readable. The aesthetic is "real doctor at home," NOT studio shoot, NOT cinematic, NOT magazine portrait. 16:9, photorealistic, natural color grade, no filter.
```

## Model selection

| Model | Performance | Notes |
|---|---|---|
| **Nano Banana (Gemini 3 Pro Image)** | ⭐⭐⭐⭐⭐ Best | Best at faces, skin texture, photorealism, AND adheres to "no studio backdrop" instructions reliably |
| Flux 1.1 Pro | ⭐⭐⭐⭐ Good | Photoreal but tends toward cinematic lighting unless specifically constrained |
| Midjourney v7 | ⭐⭐⭐ OK | Tends to render too polished/cinematic by default; needs aggressive "amateur webcam" prompting |
| DALL-E 3 | ⭐⭐ Weak | Often produces stock-photo aesthetic that fails the realism test |
| Ideogram | ⭐ Skip | Better for graphic/illustrated content |

**Default: Nano Banana.** Falls back to Flux Pro only if Nano Banana fails or is unavailable.

## Common variations

### Older doctor (65-75, "wise elder physician")
Modify: increase age range. Add: "salt-and-pepper or fully gray hair, faint laugh lines around eyes, gentle confident expression of someone who has seen many patients." Useful for channels positioned as "veteran doctor with decades of experience" — but verify Skill 03 voice DNA doesn't claim specific years (per the avatar-age-consistency rule, avatar age claims must match avatar appearance).

### Female doctor variant
Wardrobe: cardigan or blouse over a simple shell. Hair: shoulder-length or pulled back. Personal item additions: a coffee mug, a small framed family photo. Same realism rules apply.

### Different specialty signaling
- **Family medicine:** stethoscope + general medical books visible. Default config.
- **Geriatric specialist:** add a chair-side mobility aid prop (cane, walker leaning) somewhere visible. Suggests they work with elderly patients.
- **Functional / integrative medicine:** add herbal references, anatomy poster, supplements visible — signals alternative-leaning.
- **Cardiologist:** heart-anatomy poster on wall, otherwise same.

### Cultural / demographic variants
The base prompt assumes Western default. For non-Western contexts, adjust ethnic-presentation, traditional medical credentials, and room aesthetic to match the channel's target audience. Always preserve the "doctor at home, not studio" rule.

## When to use

This playbook is called by:
- **Skill 02 — Thumbnail Design** when generating thumbnails for medical/health channels using a host face
- **Skill 06 — Storyboard / Scene Breakdown** when establishing a host reference for talking-head episodes
- **Skill 09 — Image Generation Prompting** as a sub-routine when the user requests a doctor character
- **Skill 05 — Reference Channel Ingestion** when the user references a doctor-advice channel and the agent needs to generate a parallel host

## Integration with Skill 03 (Voice DNA)

This visual playbook pairs with the V2 Federal Credentialed Expert (medical variant) voice archetype from Skill 03. The face you generate must match the voice you'll synthesize — a young-looking 35-year-old doctor avatar paired with a deep mid-50s authority voice creates dissonance. The agent always picks visual age + voice age in the same step, then renders.

## Sample-then-confirm gate

Before locking this character for the channel, the agent:
1. Generates 4 variants of the same prompt (different seeds, slight wardrobe variation)
2. Surfaces all 4 to the user
3. User picks one OR asks for adjustments
4. Once locked, the character reference image goes into Channel Memory and is reused as the avatar reference for HeyGen / for thumbnail generation / for any character-of-the-host shot

This is the same sample-then-confirm pattern from Skills 06, 07, 08. Never lock a host without explicit user approval.

## Anti-patterns specific to AI doctor hosts

- **The "stethoscope around neck always" tell** — too many AI doctor renders include this. Vary it. Some shots have the stethoscope on the desk; some have it draped over the chair behind; some have it not visible (medical credibility carried by the framed degree on the wall instead).
- **The "perfect smile" failure** — neutral or slightly warm expression. Big perfect smiles read as stock photo.
- **The "white coat at desk" mismatch** — if subject is at home desk, lab coat being worn is illogical. Have it visible (hanging on a chair, on a hook nearby) but not worn.
- **The polished diploma wall** — three perfectly-aligned framed degrees on the wall reads "doctor's office shoot," not "real person's home." One degree, plus a personal photo or plant, is more authentic.
- **The "interview frame" pose** — subject sitting straight at perfect angle, hands folded, looking directly at lens. Too posed. Slight angle, hands relaxed, looking just slightly off-camera works better.

## Why these rules

The senior-health audience is highly skeptical of medical authority for good reason — they've been sold supplements and miracle cures for decades. The single thing that builds trust faster than any credential is the perception that the doctor is a **real person, not a TV doctor**. Studio gloss reads as "trying to sell me something." Authentic at-home presentation reads as "this person is sharing what they know with me directly." The latter wins on retention and conversion every time.

Our medical-authority channel teardown (April 2026) confirmed this — the channels in this niche that perform best (a doctor-personality health channel and similar doctor-personality channels) all use deliberately un-cinematic home setups. Channels with studio polish in this niche underperform.

## Update log

This playbook is current as of April 2026. Update when:
- Image gen models change face rendering quality (every 6 months minimum)
- New tells emerge that AI-generated doctor faces give away
- The senior-health niche aesthetic shifts (currently locked on "authentic at-home")
- Cultural / demographic variants need expansion
