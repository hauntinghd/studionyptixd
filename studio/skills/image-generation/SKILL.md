---
name: image-generation
description: >-
  Generates keyframes for thumbnails + i2v sources via fal.ai (Gemini Nano Banana / Flux / Midjourney). Load when prompting an image model. Companion: bank.md (per-niche prompts × 12 niches + per-model adaptation notes + 8 artifact diagnostics).
---

# Skill 09 — Image Generation Prompting

> **⚠ Model reference (2026-05) — cost/strength notes, NOT a closed list.**
>
> The table below is a starting reference of image models known to work well for these jobs, with their tradeoffs. It is **not the full set of what you can use** — fal hosts many more image models, and the Vercel AI Gateway can route others. There is no single blessed default: pick the model that best fits the task. If the user names a model that isn't in this table (e.g. a newer release), or you think something else suits the job better, **discover what's actually available and use it** — list fal's image models, query the gateway's `/v1/models`, or WebSearch + WebFetch the provider docs. **Never tell the user a model is unavailable just because it's absent from this table — verify first.**
>
> | Strengths | Model id | Cost / use notes |
> |---|---|---|
> | Photoreal humans, cinematic lighting | `fal-ai/flux-pro/v1.1` | Reliable general-purpose workhorse. |
> | Best text rendering + photoreal + multi-subject coherence | `fal-ai/gemini-25-pro-image` | Nano Banana Pro. Strong for hero/photoreal work or scenes with unavoidable in-world text. |
> | Speed / high volume (OS, Apache 2.0) | `fal-ai/flux/schnell` | 1-3 sec/image. Cheapest for >10 generations or budget-critical sessions. |
> | Editing / composition | `fal-ai/flux-pro/kontext` | 1-4 reference images, multi-image composition, prompt-driven edits. |
> | Inpaint | `fal-ai/flux-pro/v1.1/inpainting` (mask) or `fal-ai/bria/eraser` (text-prompt) | Object removal / region-specific edits. |
>
> **How to choose:** match the model to the task — photoreal/cinematic, text-heavy, high-volume/cheap, editing, or inpaint — weighing quality against cost for the specific job. Consider models beyond this table when they fit better.
>
> **Correctness rule (applies to ANY model):** never let the image-gen model render thumbnail text. Text overlays go in code (Pillow/Sharp/Canvas) for pixel-perfect typography control. Even a model with strong text rendering (e.g. Nano Banana Pro) is only for unavoidable in-world text (signs, labels, in-world copy), never for the thumbnail's headline.
>
> Prices and model positioning drift — treat the cost notes as approximate and re-check current rates/availability when it matters. Detailed reasoning in Section 2 below is preserved for context but its specific model lists/prices are stale.

---

This is the operational knowledge an AI YouTube agent needs to generate the perfect still image for any purpose — thumbnails, keyframes for image-to-video, B-roll stills, character portraits, scene establishing shots, motion graphic background plates.

Different from Skill 07 (Image-to-Video Prompting) which animates existing images. This skill is about generating the still image itself.

The single rule: **never let the image gen model render text.** Text overlays go in code (Pillow, Sharp, Canvas) post-generation, not into the image prompt. AI-rendered text in 2026 still has visible failure modes (warped letters, melted serifs, six-stroke E's) that ruin every thumbnail it touches.

---

## 1. The job of image generation

Image generation does six things:

1. **Produces thumbnails** at 1920×1080 that pass mobile readability (320×180 effective render)
2. **Produces keyframes** at storyboard-specified composition for downstream i2v
3. **Produces B-roll stills** for narration-heavy content needing visual variety
4. **Produces character portraits** locked to the channel's host or composite character bible
5. **Produces scene establishing shots** (rooms, environments, settings)
6. **Produces motion graphic backgrounds** (textures, patterns, atmospheric plates)

The agent picks the right model per task, writes a prompt that matches the channel's locked register (per Skill T4), runs the sample-then-confirm gate when stakes warrant, and adds text in code separately if needed.

## 2. Model selection

Different image gen models have radically different strengths. The agent picks per task, not per project.

### Nano Banana (Gemini 3 Pro Image)
**Strengths:** Best at faces, skin texture, photorealism in 2026. Strong at instruction-following ("no studio lighting, no three-point setup"). Reliable on people in environments. Adheres to authenticity prompts better than competitors.
**Weaknesses:** Slower than Flux. Slightly less stylized output range.
**Cost:** ~$0.04 per image
**Best for:** Photorealistic faces (Skills T1, T3), authentic settings (T2), B-roll with people, thumbnails featuring hosts
**Default model when:** the image needs to look real

### Flux 1.1 Pro
**Strengths:** Fast generation, photorealistic output, good at landscapes and stylized aesthetic
**Weaknesses:** Tends slightly cinematic / over-graded by default. Need to constrain explicitly for amateur-authentic register.
**Cost:** ~$0.05 per image
**Best for:** Cinematic stylized content, music video keyframes, atmospheric scenes
**Default model when:** the image needs to look polished but not surreal

### Midjourney v7
**Strengths:** Highest aesthetic quality, strong on dramatic lighting, excellent for cinematic music video content
**Weaknesses:** Tends to over-stylize even when prompted not to. Inconsistent character preservation.
**Cost:** Subscription-based; ~$0.05 effective per image
**Best for:** Music video keyframes (brick-narrative storytelling style), channel trailer imagery, hero shots
**Default model when:** the image needs to be art-direction-quality cinematic

### Ideogram
**Strengths:** Best at clean illustration, excellent at rendering text WITHIN images (rare exception case), good at infographic-style visuals
**Weaknesses:** Not photorealistic. Limited to illustration / graphic style.
**Cost:** ~$0.04 per image
**Best for:** Educational content with clean illustration, science-animation style geometric content, infographic visuals
**Default model when:** the channel uses illustrated aesthetic

### DALL-E 3
**Strengths:** Strong at surreal compositions, good at maintaining specific objects in scenes
**Weaknesses:** Tends toward stock-photo aesthetic. Weaker on faces than Nano Banana.
**Cost:** ~$0.04 per image
**Best for:** Surreal / impossible scenes (some music video content)
**Default model when:** the image is clearly surreal not photoreal

### Selection decision tree
1. Is the channel's register cinematic-stylized (music video, propaganda)? → Midjourney or Flux Pro
2. Is the channel's register illustrated (science-animation / deadpan-history style)? → Ideogram
3. Is the image surreal/impossible? → DALL-E 3
4. Default for everything else (photoreal): → **Nano Banana**

## 3. Prompt structure

Every image gen prompt has the same skeleton, similar to i2v but with composition emphasis:

```
[SUBJECT] + [COMPOSITION] + [LIGHTING] + [GRADE] + [TECHNICAL] + [STYLE/REGISTER]
```

### Subject
What's in the frame. Specific over general.
- ✗ "a person"
- ✓ "a 67-year-old retired schoolteacher in a soft cardigan"

### Composition
How the frame is constructed. Use controlled vocabulary.
- "rule of thirds, subject left third"
- "centered with negative space top-right for text overlay"
- "wide establishing shot, subject in lower third"
- "close-up at eye level, slight angle"

### Lighting
Match register from Skill T4.
- "warm window light from left, soft lamp fill from right"
- "cinematic three-point, key from upper right"
- "golden hour from frame-right, long shadows"
- "clinical bright fluorescent, no shadows"

### Grade
Color treatment.
- "natural color, slight warmth"
- "warm sepia archival grade"
- "cool teal cinematic"
- "saturated music video grade"
- "muted documentary"

### Technical
Resolution, aspect, sharpness.
- "16:9, 1920x1080, sharp focus throughout"
- "9:16 vertical, mobile-optimized framing"
- "1:1 square, social-cropped"

### Style/Register
Final aesthetic direction.
- "photorealistic, no filter, NOT cinematic, NOT studio"
- "cinematic, graded, premium music video aesthetic"
- "illustrated, flat geometric, science-animation style"

### Worked example

For a personal-finance authority channel thumbnail face:
```
A 38-year-old male tax professional with neutral concerned expression, looking just to the right of camera; rule of thirds composition with subject on left third, large negative space top-right reserved for text overlay; warm natural daylight from frame-left, slight warm lamp fill; natural color grade with slight warmth, no filter; 1920x1080 16:9, sharp focus on face with gentle background blur showing wood desk and bookshelf; photorealistic, NOT cinematic, NOT studio, NOT magazine portrait — captured as if a webcam at home office, slight upward angle.
```

This produces a clean thumbnail base. Text gets added in code separately.

## 4. The text-in-code rule (mandatory)

Every successful thumbnail in 2026 follows this pattern:
1. Generate the IMAGE only (no text in the prompt)
2. Use Pillow / Sharp / Canvas / similar code library to add text overlay
3. Render final composite

Why: AI image gen models produce visible text artifacts that ruin thumbnails. Letters get warped, serifs melt, line spacing breaks, character widths shift. Even Nano Banana — the best at text in 2026 — produces unreliable results when the text is critical.

Text-in-code ensures:
- Pixel-perfect rendering at any size
- Mobile readability (proper stroke width, font weight 800+)
- Easy A/B variant generation (swap text without re-rendering image)
- Brand consistency (same font across all thumbs)

The agent's prompt explicitly says "leave space for text overlay top-right" or "negative space bottom 30% for text" — but never asks the model to render the text itself.

### Exceptions where AI-rendered text is acceptable
- Ideogram, when the text is large + simple + central + the image IS text-driven (rare)
- Background texture text that's intentionally illegible (decorative)
- Music video keyframes where stylized text-as-art is the aesthetic

99% of the time, text goes in code.

## 5. Composition rules per surface

### Thumbnail composition
- 1920×1080 16:9
- Subject placed on rule-of-thirds (NOT centered)
- 30-50% of canvas reserved as negative space for text overlay
- Top-left quadrant gets the most viewer attention (eye-tracking data) — place text or face there
- Mobile readability: imagine the thumb at 320×180; the dominant element must resolve

### Keyframe composition (for downstream i2v)
- 16:9 unless vertical content
- Subject framing per storyboard shot type
- Avoid extreme close-ups on faces — i2v models perform worse on extreme face crops
- Background should be readable but not detailed (i2v often distorts complex backgrounds during motion)

### B-roll still composition
- Match the channel's editing rhythm
- Faces work for personal-story B-roll
- Object-only works for technical B-roll
- Wide establishing shots for setting B-roll

### Scene establishing shot composition
- Wide framing
- Show enough environment to establish context
- One dominant element (a building, a person, a key object)

### Motion graphic background composition
- Lower visual priority — the motion graphic foreground will dominate
- Solid colors, gradients, or simple patterns
- Avoid faces, recognizable objects, detailed textures (compete with foreground)

## 6. Aspect ratio per surface

| Surface | Aspect | Notes |
|---|---|---|
| Long-form YouTube thumbnail | 1920×1080 (16:9) | Most common |
| Vertical Shorts thumbnail | 1080×1920 (9:16) | Render at this aspect, don't crop |
| Square (Instagram cross-post) | 1080×1080 | Render at this aspect |
| TV / Connected TV | 1920×1080 (16:9) | Same as standard |
| End screen graphic | 1920×1080 (16:9) | Often custom design over generated bg |

Always render at target aspect from the start. Cropping a 16:9 to 9:16 loses critical composition.

## 7. The reference image input

Nano Banana, Flux, and Midjourney all accept reference images as inputs. Use them strategically.

### When to use reference image input
- **Character consistency** (per Skill T5): always feed the canonical reference for the host/composite
- **Style transfer**: feed a stylistic reference image to match a channel's locked aesthetic
- **Specific composition**: feed a composition reference to anchor the output's framing
- **Color palette match**: feed a reference with the exact colors you want

### Reference image protocols
- **Single reference for character**: one canonical image per character, used at every generation
- **Multiple references for style**: 2-3 reference images for stylistic pattern (e.g., 3 long-form mystery-documentary thumbnails to nail that channel's color register)
- **Reference + prompt alignment**: don't fight the reference. If reference shows daylight, don't prompt for night. The model gets confused.

### Worked example — character consistency

Goal: Generate a new thumbnail face for a personal-finance authority channel's later episode.

Inputs:
- Canonical reference: the channel's locked host face from its first episode (per Skill T5 character bible)
- Prompt: "the same person, slightly concerned expression, looking just to the right of camera, top-left composition for text overlay top-right, warm natural light"
- Model: Nano Banana with reference image input
- Result: face stays consistent with the canonical, expression and composition adjusted for this episode

## 8. Niche-specific image generation patterns

### Senior finance / IRS / retirement (personal-finance authority channel)
- **Model:** Nano Banana
- **Register:** authentic-amateur (T4 Register A)
- **Compositions:** rule-of-thirds with negative space for text
- **Color anchor:** red rage stamp accent over warm-natural base
- **Text in code:** ALL CAPS bold yellow with thick black stroke

### Tech / AI / dev tools
- **Model:** Nano Banana for product shots, Flux for atmospheric
- **Register:** mid-polished (T4 Register B-light)
- **Compositions:** product hero centered or rule-of-thirds with depth
- **Color anchor:** brand color of product reviewed
- **Text in code:** mid-weight, often product name + verdict word

### Gaming / Roblox / Minecraft
- **Model:** Flux or Midjourney for stylized
- **Register:** stylized cinematic (T4 Register B)
- **Compositions:** dramatic, character-led
- **Color anchor:** saturated, vibrant
- **Text in code:** bold ALL CAPS action words

### Music videos / drill / propaganda (brick-narrative storytelling channel)
- **Model:** Midjourney or Flux for cinematic
- **Register:** full cinematic (T4 Register B)
- **Compositions:** rule of thirds, dramatic, atmospheric
- **Color anchor:** genre-locked (drill = dark/red, propaganda = patriotic)
- **Text in code:** stylized, often track name + artist

### News-hijack documentary (investigative-journalism / geopolitics documentary channels)
- **Model:** Nano Banana for hosts, Flux for cinematic recreation
- **Register:** mixed (T4 Register C)
- **Compositions:** intimate for hosts, cinematic for recreation
- **Color anchor:** red/black for thumbnails
- **Text in code:** ALL CAPS rage stamp

### True crime
- **Model:** Nano Banana for portraits, Flux for cinematic recreation
- **Register:** mixed (T4 Register C)
- **Compositions:** atmospheric, mood-led
- **Color anchor:** dark moody
- **Text in code:** name + date + place

### Health / medical / supplements (medical-authority channel)
- **Model:** Nano Banana
- **Register:** authentic-amateur
- **Compositions:** doctor face + body part diagram
- **Color anchor:** clinical bright with green/yellow accent
- **Text in code:** specific condition + supplement name

### History / explainer (long-form mystery-documentary / deadpan-history / side-project history channels)
- **Model:** Flux or Midjourney for cinematic; Ideogram for stylized
- **Register:** cinematic atmospheric (T4 Register B)
- **Compositions:** single mystery object, rule of thirds
- **Color anchor:** warm sepia
- **Text in code:** minimal, sometimes just date

### Science (science-explainer / science-animation channels)
- **Model:** Nano Banana for science-explainer style; Ideogram for science-animation style
- **Register:** clean professional (science-explainer) or geometric illustrated (science-animation)
- **Compositions:** subject-led with clean negative space
- **Color anchor:** muted professional or branded blue
- **Text in code:** concept word

### Beauty / fashion / makeup
- **Model:** Nano Banana
- **Register:** authentic-amateur with personality
- **Compositions:** face + product
- **Color anchor:** brand-relevant
- **Text in code:** brand name + price

### Cooking / food
- **Model:** Nano Banana for food, Flux for atmosphere
- **Register:** food-photography golden warmth
- **Compositions:** food hero center with negative space
- **Color anchor:** food-warm browns/cream/red
- **Text in code:** dish name + technique word

### Real estate / home (host-led real-estate creator)
- **Model:** Nano Banana for hosts, Flux for property hero
- **Register:** mid-polished
- **Compositions:** host pointing at property, OR property hero
- **Color anchor:** bright, dollar-amount-relevant
- **Text in code:** dollar amount + property type

### Vlog / lifestyle
- **Model:** Nano Banana
- **Register:** authentic with personality
- **Compositions:** face + reaction
- **Color anchor:** personality-driven
- **Text in code:** day count or location

### Comedy / shorts (Roblox-scenario)
- **Model:** Flux for stylized characters
- **Register:** stylized vertical
- **Compositions:** character + reaction face
- **Color anchor:** saturated
- **Text in code:** punchline preview

### Ambient / sleep / focus
- **Model:** Flux or Midjourney for atmospheric
- **Register:** cinematic moody
- **Compositions:** atmospheric scene, no subject
- **Color anchor:** cool blues / warm ambers
- **Text in code:** "X hours" + content type

## 9. The sample-then-confirm gate

Same pattern as Skills 07 and 08. For high-stakes generations:

1. Generate 4 variants of the same prompt with different seeds
2. Surface to user
3. User picks one OR asks for adjustments
4. Lock the chosen variant + seed for future generations of the same character/setting

For low-stakes B-roll generations: agent generates, applies sanity check, ships. User approval not required for every B-roll image.

The threshold for "high-stakes":
- Thumbnails: ALWAYS sample-confirm
- Character portraits (host, composite): ALWAYS sample-confirm
- Storyboard keyframes: usually sample-confirm
- Scene establishing shots: usually sample-confirm
- B-roll stills: optional sample-confirm
- Motion graphic backgrounds: rarely sample-confirm

### How to generate N variants

To generate multiple variants, call the fal.ai image generation API multiple times via Bash with different seeds. For example, to generate 3 variants:

```bash
for seed in 1 2 3; do
  curl -s -X POST "https://queue.fal.run/fal-ai/flux/dev" \
    -H "Authorization: Key $FAL_API_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"prompt\": \"...\", \"seed\": $seed, \"image_size\": \"landscape_16_9\"}"
done
```

Seed handling: pass explicit seeds for reproducibility, or omit `seed` for guaranteed-distinct random outputs. For variants of an **existing** image (i2i derivatives of a single source), use the fal.ai image-to-image endpoint with the source image URL instead.

## 10. Anti-patterns

### Visual anti-patterns
- **AI-rendered text artifacts** — letters warped, serifs melted. Always use text-in-code.
- **Six-finger / extra-limb errors** — sample gate catches this.
- **Studio lighting drift on amateur-authentic register** — model defaults to studio; constrain explicitly.
- **Over-graded cinematic on photoreal content** — teal-and-orange ruins authenticity.
- **Unreadable at thumb size** — generate, then preview at 320×180 to validate.
- **Centered composition for thumbnails** — eye-tracking data favors rule-of-thirds.
- **Stock-photo aesthetic on authority content** — instant trust kill.
- **Background detail competing with subject** — too busy = unreadable at small size.

### Prompt anti-patterns
- **Asking for text in the prompt** — text-in-code rule.
- **Vague subject** — "a person" produces unpredictable results.
- **Conflicting register signals** — "amateur authentic, but cinematic and graded" produces nothing.
- **Aspect ratio mismatch** — generate at target aspect, don't crop.
- **No style/register signal** — model defaults to cinematic; specify always.

### Workflow anti-patterns
- **Skipping sample-confirm on thumbnails** — the most A/B-tested surface.
- **Re-prompting endlessly without changing seed** — try seed variation first.
- **Mixing models within a project** — visual consistency drops. Lock model per channel.
- **Generating fresh character every episode** — drift accumulates. Use canonical reference (T5).

## 11. Worked examples

### Example 1 — Personal-finance authority channel thumbnail base

**Goal:** Photorealistic host face base for "MAY 1 — 13 DAYS LATE" thumbnail.

**Setup:**
- Model: Nano Banana
- Reference: canonical locked host face (T5 character bible)
- Aspect: 16:9 1920×1080

**Prompt:**
```
The same 38-year-old male tax professional from the reference image, slightly concerned expression with mouth set, looking just to the right of camera; rule-of-thirds composition with subject on LEFT third, large negative space TOP-RIGHT reserved for text overlay; warm natural daylight from window frame-left, soft lamp fill from right; warm-natural color grade with slight warmth, no filter; 1920x1080 16:9, sharp focus on face, gentle background blur showing wood desk and bookshelf; photorealistic, NOT cinematic, NOT studio, NOT magazine portrait — captured as if a webcam at home office, slight upward angle.
```

**Cost:** $0.04 for 4 variants ($0.01 each).
**Sample gate:** 4 variants generated, user picked variant 2.
**Text overlay added in code:** "MAY 1 — 13 DAYS LATE" in bold yellow with thick black stroke, top-right negative space.

### Example 2 — Brick-narrative storytelling music video keyframe

**Goal:** Cinematic brick-built scene for Verse 1 Shot 5.

**Setup:**
- Model: Midjourney v7
- No character reference (scene-led)
- Aspect: 16:9 1920×1080

**Prompt:**
```
A toy-brick town square crowd of approximately 30 figures in mid-density arrangement, neutral expressions, all turned slightly toward camera as if recognizing an arriving figure; wide establishing shot, rule of thirds with crowd center-frame; cinematic warm-graded golden-hour lighting from frame-right, long shadows visible; warm cinematic grade with slight desaturation, music video aesthetic; 1920x1080 16:9, sharp throughout; brick-stylization preserved (blocky figures, plastic-bright colors), atmospheric.
```

**Cost:** ~$0.05.
**Sample gate:** sample reviewed, approved, fed to LTX-2.3 for i2v.

### Example 3 — Long-form mystery-documentary history channel mystery object

**Goal:** Single-object hero shot for history channel thumbnail.

**Setup:**
- Model: Flux 1.1 Pro
- No character (faceless channel)
- Aspect: 16:9 1920×1080

**Prompt:**
```
A single archival object — a weathered leather-bound 1872 ledger book lying on a dark wooden surface; centered composition with deep negative space surrounding the object; warm sepia archival grade with single warm key light from upper-left, deep shadows; warm sepia color grade, archival film grain texture; 1920x1080 16:9, sharp focus on object, dark moody atmosphere; photorealistic but graded for vintage archival feel, NOT cinematic action, NOT contemporary clean — atmospheric historical document presentation in the aesthetic of a museum archive photo.
```

**Cost:** $0.05.
**Sample gate:** Sample approved, used as thumbnail base. Text added in code: "1872" small subtle in lower corner.

### Example 4 — Medical-authority doctor + supplement diagram

**Goal:** Thumbnail for senior health video about glucosamine.

**Setup:**
- Model: Nano Banana
- Reference: medical-authority doctor canonical face
- Aspect: 16:9 1920×1080

**Prompt:**
```
The same 55-year-old male doctor from the reference image, concerned expression looking at the camera; rule-of-thirds composition with doctor on left third, supplement bottle and joint diagram in right third; warm natural daylight from window frame-left, soft lamp fill; clean warm-natural grade with slight clinical brightness, no filter; 1920x1080 16:9, sharp focus on doctor face and bottle, slight depth on background showing modest home study; photorealistic, NOT cinematic, NOT studio — captured as if a webcam at home, slight upward angle. The supplement bottle (generic white pill bottle) is positioned in the right third, large enough to read; an anatomical joint diagram is faintly overlaid on the background showing where joint pain occurs.
```

**Cost:** $0.04.
**Text overlay added in code:** "WHY GLUCOSAMINE FAILS" in red ALL CAPS over the bottle area.

## 12. Runtime checklist

Before any image generation:
- [ ] Channel register identified (Skill T4)
- [ ] Model selected per task (not per project)
- [ ] Subject + composition + lighting + grade + technical + style all in prompt
- [ ] No text in image prompt (text-in-code rule)
- [ ] Aspect ratio matches output surface
- [ ] Reference image fed if character consistency required (T5)
- [ ] Sample variants generated (4 for high-stakes; 1-2 for low-stakes)
- [ ] User approval for high-stakes surfaces
- [ ] Mobile readability validated for thumbnails (preview at 320×180)
- [ ] Text overlay added in code post-generation if needed

## 13. Why these rules

The image is the most-seen artifact of any video. Thumbnails appear before the user clicks. Keyframes appear in animated B-roll. Character portraits appear across episodes building brand recognition. Getting the image wrong cascades through every other surface.

The text-in-code rule alone saves hours of regeneration time across the channel's life. AI-rendered text in 2026 is still the #1 source of "this thumbnail looks AI-generated" reactions from viewers. Text-in-code eliminates the issue entirely.

The model selection discipline (Nano Banana for photoreal, Flux for cinematic, Midjourney for art-direction-quality, Ideogram for illustration, DALL-E for surreal) is the difference between competent output and channel-defining output. Wrong model = wrong register = wrong audience signal.

Most importantly: **the image gen layer is where most "AI YouTube" channels reveal themselves as AI-generated.** Faces drift, text fails, lighting is wrong, register is wrong. The discipline in this skill is the discipline that makes RookCast channels look professional rather than slop.

## Update log

Current as of April 2026. Update when:
- New image gen models change quality positioning (every 6 months)
- AI-text rendering crosses the threshold to be reliable (eliminates text-in-code rule)
- New niche aesthetic conventions emerge
- Model API changes affect prompt structure (e.g., Nano Banana adds new control parameters)
