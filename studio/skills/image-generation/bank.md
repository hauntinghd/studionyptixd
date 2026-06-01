# Skill 09 Companion — Image Generation Prompt Bank

This is the runtime companion to Skill 09 (Image Generation Prompting). Skill 09 documents the 6-part prompt skeleton (subject + composition + lighting + grade + technical + style/register) and per-model selection. This file provides:

1. **Per-niche prompt reservoir** — 30+ worked image-gen prompts organized by niche × shot purpose
2. **Per-model adaptation notes** — same scene rendered for Nano Banana / Flux / Midjourney / Ideogram / DALL-E
3. **Style-lock prompt patterns** — how to render the same aesthetic across episodes
4. **Common artifact diagnoses** — when image-gen output fails, why, and how to retry

The agent loads this when generating images. Pulling from a per-niche bank cuts iteration cycles and keeps style consistent across episodes (brand-load-bearing).

This complements Skill 07's i2v cookbook: the cookbook gives shot-type templates for video-clip generation; this file gives image prompts for still keyframes (which then become i2v sources or thumbnail bases).

---

## 1. Why per-niche image banks

Skill 09 documents the prompt skeleton. But a generic prompt for "doctor at desk" produces a generic image; the agent needs concrete worked prompts that have been validated to produce on-brand output for specific channels.

A locked per-niche bank:
- Reduces image-gen iteration cycles by 50-70% (one good attempt vs 3-5 trial-and-error)
- Keeps thumbnail aesthetic consistent across episodes (compounding brand)
- Makes channel handoff easier (the prompts are documented)
- Cuts cost: a Nano Banana generation at $0.04 vs the same image with 3 retries at $0.12 — small per-shot but compounds over 50 thumbnails per channel per year

The agent writes the per-channel locked prompt patterns into Channel Profile §3 (Visual Style DNA) once, then cites this companion only when establishing a new channel or when the user explicitly asks to retune the look.

---

## 2. Per-niche prompt reservoir

Each niche gets 3-4 worked prompts spanning the channel's typical shot needs (thumbnail base, hook close-up, B-roll detail, mechanism diagram).

### Senior finance / IRS (personal-finance authority channel)

**Hook close-up (Beat 1):**
> "Photoreal portrait. A 70-year-old American woman in a beige cardigan and reading glasses on a chain, sitting at a small kitchen table in a modest home. She holds an unfolded letter from a federal agency, looking at it with concerned puzzlement, brow slightly furrowed, mouth closed. Warm late-afternoon golden-hour light from a window frame-right; soft fill light from above; slight shadow under her eyes. Background: out-of-focus kitchen interior with mail scattered on the counter. Cinematic, slightly desaturated to ~85% saturation. 1280x720. No text, no graphics — text and graphics will be added in code. Style: documentary thumbnail, personal-finance authority channel aesthetic."

**Composite case (Beat 4):**
> "Photoreal observational shot. A 72-year-old woman in a faded blue cardigan walking from a kitchen counter (mail scattered on it, including one folded SSA letter) toward a front door, picking up car keys from a small table. Mid-day autumn light from kitchen windows, dimmer in the hallway behind her. Subject occupies center-left of frame at medium-wide framing. Cinematic, slightly desaturated. 1280x720. No text, no graphics."

**Mechanism diagram (Beat 5):**
> "Photoreal close-up. An old wooden file cabinet drawer slowly sliding open, revealing rows of beige folders with handwritten dates on tabs. A single hand pulls one folder labeled 'MEDICARE 2026' and lays it flat on a wooden desk. Warm desk-lamp key light from frame-right; deep shadow inside the cabinet drawer. Slight cinematic depth of field. 1280x720. No text, no graphics."

**Authority intro (Beat 3):**
> "Editorial portrait. A 35-year-old male federal tax accountant in a navy quarter-zip and reading glasses, seated at a wooden desk with a small American flag and several books behind him. He glances down at a folder, then looks calmly toward camera. Soft window light from camera-left, warm three-point lighting, slight backlight separating subject from background. Cinematic, slightly desaturated. 1280x720. No text. Style: personal-finance authority channel host shot."

### Senior health / medical (medical-authority channel)

**Hook close-up (Beat 1):**
> "Photoreal portrait. A 65-year-old American woman in a soft cream sweater, sitting at a kitchen table holding a pill bottle, looking at the label with mild concern, lips closed, eyes slightly downcast. Warm window light from frame-left, slight cool fill light. Background: out-of-focus modern kitchen with a glass of water and a fresh apple on the counter. Cinematic, ~95% saturation (clinical clean register). 1280x720. No text, no graphics. Style: medical-authority channel thumbnail."

**Patient composite (Beat 4):**
> "Photoreal observational shot. A 67-year-old man in a worn flannel shirt at a small kitchen table outside Phoenix, evening, single overhead pendant light. Paper utility bills and an orange prescription bottle on the table. He slowly counts pills from the bottle into his palm, frowning. Slight push-in framing. Warm overhead key light, long shadow on far wall. 1280x720. No text, no graphics."

**Food / supplement reveal (Beat 7):**
> "Photoreal top-down shot. An organized 7-day pill organizer with morning/evening compartments, three of them filled with various capsules and tablets in different colors. Bright clean overhead light, slight specular highlight on the plastic surface. White marble countertop background, slightly out of focus. 1280x720. No text. Style: clean clinical product shot."

**Authority intro:**
> "Editorial portrait. A 50-year-old female physician in a white lab coat over a soft-blue blouse, stethoscope around her neck, in a clean modern clinic exam room with diplomas blurred behind her. She sets a clipboard down on a desk, folds hands, looks at camera with calm professional warmth. Bright clean clinic lighting. 1280x720. No text."

### Tech / AI / dev tools

**Tool comparison hero shot:**
> "Editorial product shot. Two laptops side by side on a clean white desk, one open to a Cursor editor interface, one open to a Claude Code terminal. Both screens lit and visible, shot from a low three-quarter angle. Modern minimal desk with a single matte black mechanical keyboard and a coffee cup blurred in the background. Bright daylight from frame-left, slight cool color grade. 1280x720. No text, no graphics — interface text rendered as visible UI in the frame, supplementary text added in code."

**Authority intro / dev portrait:**
> "Editorial portrait. A 30-year-old male software engineer in a navy quarter-zip, in a modern home office with a 32-inch monitor blurred behind him. Casual confident posture, slight smile, looking at camera. Bright soft daylight from a window frame-right, cool color grade. 1280x720. No text. Style: tech YouTuber thumbnail aesthetic."

**Concept diagram:**
> "Photoreal close-up. A clean white desk with a notebook open showing a hand-drawn architecture diagram (boxes with arrows between them, no readable text). A black ballpoint pen rests on the page. Bright overhead daylight, slight cool grade. 1280x720. No text in the diagram — keep boxes and arrows abstract; text added in code."

### History / explainer (long-form mystery-documentary style)

**Mystery object:**
> "Cinematic close-up. A single antique brass key, oxidized with patina, lying on a weathered wooden surface scarred with age. Single warm key light from upper-left creates dramatic chiaroscuro; deep shadow filling the rest of frame. Dark moody charcoal-to-black background gradient. Shallow depth of field, sharp focus on the key. Slight cinematic film grain. 1280x720. No text. Style: long-form mystery-documentary thumbnail."

**Historical reconstruction (1518 dancing plague):**
> "Cinematic wide shot. A medieval European city square at twilight, cobblestone ground, half-timbered Strasbourg-style buildings on either side. A single woman in a plain peasant dress stands in the middle, mid-dance, others gathered watching from doorways and windows. Lit by golden setting sun streaming between buildings; long shadows. Slight film grain, desaturated cinematic grade. 1280x720. No text. Style: HBO historical drama."

**Document close-up:**
> "Cinematic close-up. An aged parchment page from a 16th-century chronicle, hand-written in faded ink, slightly water-stained. A leather-bound book is partially visible in the background, out of focus. Single warm desk-lamp key light from upper-right creates strong shadow on the page. Cinematic, slightly desaturated. 1280x720. No legible text — text appears as period-accurate illegible script."

### News-hijack documentary (investigative-journalism style)

**Investigation hero shot:**
> "Photoreal cinematic. A 32-year-old male investigator in a black hoodie at a cluttered desk with two laptops open, papers and printouts scattered, dim apartment lighting, a single lamp creating warm key light. He leans back from the screen, runs hand through hair, exhales through nose. Static medium close-up, slightly low angle. Cool blue monitor light dominant on his face, warm desk lamp at frame-right. Mood: exhausted realization. 1280x720. No text."

**Document evidence reveal:**
> "Photoreal close-up. A printed document slid across a wooden desk, paragraphs of redacted text (black bars) covering most of the page, a circled paragraph in red ink. Forensic-style overhead camera angle. Cool fluorescent overhead light. 1280x720. No legible text — paragraphs appear as visible-but-unreadable typography; specific text added in code overlay."

**Whistleblower interview:**
> "Photoreal close-up. A 45-year-old woman with grey-streaked hair in a soft cream sweater, sitting in a study with bookshelves blurred behind her, low warm key light from camera-left. She closes a manila folder, pauses, looks just past the camera. Static medium close-up at eye level. Weighted soft warm key, deep shadow on right side of face. 1280x720. No text. Style: 60 Minutes interview lighting."

### Vertical shorts (Roblox-scenario)

**Hook face shot (vertical):**
> "Bright vibrant thumbnail. Centered subject: young woman, age 25, expression of stunned disbelief — closed mouth, raised eyebrows, eyes wide. Front-lit with even soft light. Looking directly at camera. Background: solid saturated yellow #FFD60A. Subject occupies upper 70% of frame. Lower 25% intentionally empty for text overlay. 1080x1920 vertical (9:16). Photoreal, top viral-challenge thumbnail style. No text."

**Roblox-stylized scene (vertical):**
> "Stylized rendering with Roblox-blocky aesthetic but cinematic lighting. A school classroom scene with a single character (Roblox-style avatar wearing a teacher's outfit and tie) standing on the teacher's desk, holding a Roblox Robux icon. Other Roblox-style avatar students seated below looking up in shock. Saturated bright daylight from windows. Vibrant primary colors. 1080x1920 vertical. No text. Style: Roblox in-game render with cinematic upgrade."

### Music video / drill / propaganda (brick-narrative storytelling channel)

**Brick triumph hero:**
> "Photoreal stylized brick-toy scene with cinematic lighting. A brick-toy general figure in green military uniform standing on the back of a brick-toy tank rolling slowly forward through a destroyed brick-toy city. Smoke trailing behind. Sky: dramatic orange-and-purple sunset. Locked low-angle shot looking up at the figure. Cinematic film grain, hyperreal saturation. 1280x720. No text. Style: brick-narrative storytelling channel aesthetic."

**Brick political subject (specific figure):**
> "Photoreal stylized brick-toy scene. A brick-toy figure resembling a US president (use generic brick-toy presidential features — blue suit, red tie, distinct hair) standing at a brick-toy podium with a bank of brick-toy microphones. Background: a brick-toy White House blurred. Dramatic three-point lighting. Slight cinematic film grain. 1280x720. No text. Style: brick-narrative storytelling channel aesthetic, satirical register."

**Brick chaos scene:**
> "Photoreal stylized brick-toy scene. A brick-toy city street with multiple brick-toy vehicles abandoned mid-traffic, smoke rising from a brick-toy building. Dramatic orange sunset lighting from behind, casting long blue shadows. Single brick-toy figure in foreground walking toward camera. Cinematic, hyperreal saturation, slight film grain. 1280x720. No text."

### True crime

**Crime scene investigation:**
> "Cinematic photoreal. An empty residential kitchen, evening, single overhead light. A half-finished cup of coffee on the counter, a chair pulled out from the table at an odd angle, a cardigan draped over the chair back. No people in frame. Cool desaturated grade with slight blue cast. Slight cinematic film grain. 1280x720. No text. Style: true crime documentary reconstruction."

**Reluctant witness portrait:**
> "Cinematic photoreal portrait. A 47-year-old woman with grey-streaked hair in a soft cream sweater, sitting in a study with bookshelves blurred behind her, low warm key light from camera-left. Closes a manila folder, pauses, looks just past the camera. Static medium close-up at eye level. Cool desaturated grade with slight warm key contrast. Heavy shadow on right side of face. 1280x720. No text."

**Cold case file:**
> "Photoreal close-up. An old manila case file folder open on a wooden desk, slightly yellowed papers visible inside, a black-and-white photograph of an unidentified victim partially visible. Detective's hands holding a magnifying glass over the document. Single warm desk-lamp key light, dim ambient. Slight cinematic film grain, desaturated. 1280x720. No legible text on documents — specific text added in code."

### Cooking

**Hero food shot:**
> "Photoreal top-down hero shot. A perfectly cooked sourdough loaf, dark caramelized crust with visible flour dusting, sliced to show open crumb structure with large irregular holes. On a wooden cutting board with a knife resting nearby. Bright natural daylight from frame-left, soft shadow on right. Warm color grade, vibrant saturation. 1280x720. No text. Style: artisan cookbook aesthetic."

**Process / technique close-up:**
> "Photoreal close-up. A pair of hands kneading dough on a floured wooden surface, dough has good structure (windowpane visible at edge), flour dusts the air slightly. Bright natural daylight from above, slight motion in flour. Warm color grade. 1280x720. No text. Style: artisan baking magazine."

**Counter-narrative subject (chef tools):**
> "Photoreal close-up. A bunch of mushrooms on a wooden cutting board, partially wiped with a damp cloth — NOT washed under running water. A kitchen knife and a mushroom brush resting beside them. Soft natural daylight. Warm color grade. 1280x720. No text. Style: cooking technique photography."

### Real estate

**Property hero shot:**
> "Photoreal exterior. A modern Phoenix-area single-story rental property at golden hour, terracotta roof tiles, desert landscaping in front (yucca, prickly pear, gravel). Warm sunset lighting from frame-right casting long shadows. Sky: dramatic orange-purple gradient. Slight cinematic depth of field. 1280x720. No text."

**Investor authority shot:**
> "Editorial portrait. A 40-year-old male real estate investor in a navy half-zip pullover, standing in a modern open-concept living room with floor-to-ceiling windows showing a Phoenix mountain view blurred behind. Confident posture, slight smile, looking at camera. Bright soft daylight. 1280x720. No text. Style: real estate YouTuber thumbnail."

**Market data visualization:**
> "Photoreal close-up. A laptop screen showing a clean line graph (no readable text/numbers — abstract shape only) with a clear downward trend arrow drawn over it in red marker on top of the screen. Wooden desk surface, modern coffee mug at edge of frame. Bright daylight, slight cool color grade. 1280x720. No legible numbers — chart shape only; specific data added in code."

### Faith / Christian

**Reverent contemplative scene:**
> "Cinematic photoreal. The interior of an old stone monastery cell, simple wooden table with a single candle and an aged leather-bound book. Soft warm light from a small high window, dust motes visible in the light beam. Silent solemn atmosphere. Slight cinematic film grain, slightly desaturated warm grade. 1280x720. No text. Style: medieval contemplative."

**Biblical historical reconstruction:**
> "Cinematic photoreal. A young man in 13th-century Italian pilgrim clothing walking away from a wealthy stone-faced merchant building, evening, golden setting sun behind him casting long shadow on cobblestones. Lit warmly. Slight cinematic film grain. 1280x720. No text. Style: religious history documentary reconstruction."

### Fitness / bodybuilding

**Form demonstration:**
> "Photoreal cinematic. A 45-year-old male athlete with visible muscular build, mid-squat at the bottom position, perfect form, in a clean modern home gym with a barbell across his upper back. Side profile angle. Dramatic single key light from frame-right creating sculptural shadow. Slight cool grade. 1280x720. No text. Style: powerlifting magazine."

**Transformation hero shot:**
> "Editorial portrait. A 50-year-old male in a fitted athletic shirt, confident posture, in a clean modern gym with weight rack blurred behind. Slight smile, looking at camera. Bright daylight, slight cool color grade. 1280x720. No text. Style: fitness YouTuber thumbnail."

### Travel

**Destination establishing shot:**
> "Cinematic photoreal. A narrow cobblestone street in Lisbon, Portugal at golden hour, traditional yellow tram visible mid-distance, balconies with hanging laundry above, small café with outdoor seating. Warm sunset light streaming down the street. Vibrant saturated color grade. 1280x720. No text."

**Budget-travel hero:**
> "Photoreal flat-lay top-down. A wooden table with travel essentials laid out: a worn passport, a printed budget spreadsheet (numbers visible but not specifically readable), a Lisbon guidebook, a pair of comfortable walking shoes, a notepad with handwritten notes. Bright natural daylight. Warm color grade. 1280x720. No legible text — specific numbers added in code."

---

## 3. Per-model adaptation notes

The same scene renders differently across models. Adapt the prompt:

### Nano Banana

- Best at photoreal humans, especially faces
- Likes very specific demographic detail (age, attire, expression)
- Slight tendency to over-smooth — add "natural skin texture, slight asymmetry" to counter
- Strong on warm domestic lighting
- Cost: ~$0.04 per image

### Flux

- Best at cinematic moody atmospheres
- Likes painterly lighting language ("rim light, chiaroscuro, single key")
- Slight tendency to over-stylize — add "photoreal, documentary lens" if too painterly
- Strong on cinematic color grades
- Cost: ~$0.10 per image

### Midjourney

- Best at art-directed thumbnails (single dramatic compositions)
- Likes camera-language detail ("85mm portrait lens, shallow depth of field")
- Default aesthetic is cinematic — add "photoreal, journalism photography" for grounded look
- Strong on dramatic lighting and color
- Cost: ~$0.10 per image

### Ideogram

- Best at illustrated content, can render readable text (rare exception to text-in-code rule)
- Use for illustrated channel aesthetics or when text in image is mandatory
- Weaker on photoreal humans
- Cost: ~$0.04 per image

### DALL-E 3

- Best at surreal compositions, impossible scenes
- Strong on metaphorical imagery
- Weaker on consistent character continuity (use Nano Banana for recurring characters)
- Cost: varies by access tier

### Selection by use case

| Use case | Default model |
|---|---|
| Photoreal portrait (host, patient, witness) | Nano Banana |
| Cinematic atmospheric scene | Flux |
| Mystery object hero shot | Flux |
| Brick-toy / stylized scene | Midjourney |
| Illustration with text | Ideogram (rare) |
| Surreal/impossible composite | DALL-E 3 |
| B-roll detail (cheap, photoreal) | Nano Banana |
| Recurring character (continuity) | Nano Banana |

---

## 4. Style-lock prompt patterns

To render the same aesthetic across episodes, lock the visual style segment of the prompt and reuse:

### Personal-finance authority channel locked style segment

```
Cinematic, slightly desaturated to ~85% saturation. Warm domestic interiors (golden hour kitchen) OR cool institutional exteriors (navy blue Treasury). Window-key dominant for domestic; flat overcast for exteriors. Documentary thumbnail aesthetic. Personal-finance authority channel.
```

Append to any prompt for a personal-finance authority channel shot. Locks color grade, lighting register, register modifier.

### Brick-narrative storytelling channel locked style segment

```
Photoreal stylized brick-toy aesthetic with cinematic lighting. Hyperreal saturation, slight cinematic film grain. Dramatic three-point lighting (key + rim + fill). Orange-and-purple sunset gradient OR cool blue-purple night grade. Brick-narrative storytelling channel aesthetic.
```

### Medical-authority channel locked style segment

```
Photoreal, ~95% saturation (clinical clean register). Bright natural daylight or clean clinic overhead. Warm color grade for patient kitchens; cool clean for clinic. Medical-authority channel thumbnail aesthetic.
```

The agent records the locked style segment in Channel Profile §3 (Visual Style DNA). Subsequent prompts append it. Style consistency compounds — the channel develops a recognizable look without manual style-tuning per shot.

---

## 5. Common artifact diagnostic bank

When image-gen output has artifacts, the agent diagnoses and retries.

### Artifact 1 — Warped or extra fingers

**Cause:** model lost track of hand structure mid-generation.
**Fix:** add to prompt: "natural anatomically correct hands, fingers slightly relaxed." Or: crop/composite hands out, regenerate without hands visible.
**Frequency:** common in Nano Banana on close-ups; less common in Flux.

### Artifact 2 — Asymmetric / different colored eyes

**Cause:** subject specs underspecified for face.
**Fix:** add to prompt: "matching eye color, symmetric facial features, consistent gaze direction."

### Artifact 3 — Logo or text artifact (warped letters)

**Cause:** model attempted to render text it shouldn't have.
**Fix:** explicitly add "no text, no logos, no graphics — clear surfaces" to prompt. If the scene needs visible objects with branding, render the brand-free version and add brand in code.

### Artifact 4 — Floating object / impossible physics

**Cause:** model generated subject without consistent ground/support.
**Fix:** specify ground/support: "object resting solidly on wooden surface, with visible shadow."

### Artifact 5 — Aspect ratio drift / framing wrong

**Cause:** model rendered at wrong dimensions.
**Fix:** explicitly set 1280x720 (or other target) at end of prompt; verify model's aspect-ratio parameters.

### Artifact 6 — Style drift across episodes

**Cause:** style segment not consistent between prompts.
**Fix:** use the locked style segment from §4 verbatim every time.

### Artifact 7 — Subject identity drift (recurring character looks different)

**Cause:** insufficient anchoring for recurring composite.
**Fix:** maintain a character reference image; use Nano Banana with consistent demographic details verbatim from the recurring composite registry.

### Artifact 8 — Environment doesn't match niche register

**Cause:** prompt allowed too much freedom on background.
**Fix:** specify the environment register explicitly per Channel Profile §3 visual DNA.

---

## 6. Anti-pattern prompts

Six prompts that look correct but fail.

### Anti-pattern 1 — Generic everything

> "A doctor in a clinic looking at a patient."

Diagnosis: every field is generic. Result is stock-image-feeling. No specific demographic, no specific scene, no specific mood, no style anchor.
Fix: rewrite with specificity. "A 50-year-old female physician in a white lab coat over a soft-blue blouse in a clean modern clinic exam room, looking with calm professional warmth at a 65-year-old male patient seated on the exam table, daylight from frame-right window, medical-authority channel aesthetic."

### Anti-pattern 2 — Asking for text in the image

> "A close-up of an SSA letter with the words 'YOUR BENEFITS HAVE BEEN REDIRECTED' clearly visible."

Diagnosis: AI models render text inconsistently — letters warp, kerning collapses, words distort. Per cross-channel R1 (text-in-code).
Fix: generate the letter as a clean blank document; add the text in code.

### Anti-pattern 3 — Stylized prompt to a photoreal model

> "Veo 3 Fast: A brick-toy figure of a soldier with a vibrant cinematic anime grade, claymation textures, neon outline."

Diagnosis: model has realism gravity. Stylized prompts get pulled toward realism, producing confused half-real, half-stylized output.
Fix: match model to register. For stylized: Midjourney or LTX-2.3 with explicit style locking. For realism: Nano Banana / Flux / Veo with photoreal vocabulary.

### Anti-pattern 4 — Conflicting environmental descriptors

> "An overcast January afternoon with shafts of bright sunlight breaking through onto the cobblestones."

Diagnosis: "overcast" and "shafts of sunlight" are mutually exclusive. Model alternates randomly across the image producing flickering lighting.
Fix: pick one. Either "overcast diffused light" OR "low winter sun breaking through clouds, dappled light." Not both.

### Anti-pattern 5 — Multi-subject without clear hierarchy

> "A doctor, a patient, a nurse, and two family members in an exam room discussing a diagnosis."

Diagnosis: 5 subjects. Model can't track all of them; identity drifts; some get warped. Multi-subject scenes are expensive and risky.
Fix: pick 1 primary subject + 1 supporting (or composite later via separate generations).

### Anti-pattern 6 — No style anchor

> "A 70-year-old grandmother at a kitchen table holding a letter."

Diagnosis: missing style segment. Output could be photoreal, illustrated, painterly — model picks at random. Cross-episode consistency impossible.
Fix: append the channel's locked style segment from §4.

---

## 7. Cross-skill connections

This skill connects to:
- **Skill 02 (Thumbnail Design):** thumbnail keyframes generated here become the base for thumbnail composition (text-in-code added on top per Skill 02 companion).
- **Skill 06 (Storyboard):** the storyboard's per-shot keyframe needs are filled by image-gen per this companion.
- **Skill 07 (i2v):** generated keyframes here become the source images for i2v generation.
- **Tactical T1-T5:** when generating AI host portraits or composite case shots, T1 (host setup), T3 (composite character), and T5 (visual continuity) provide additional discipline.

When image-gen output isn't matching the channel aesthetic, the failure is usually in Style Lock (§4) or in missing tactical-playbook discipline (T3 character drift, T5 continuity).

---

## 8. Runtime workflow

When generating an image for any shot:

1. **Identify niche** from Channel Profile.
2. **Identify shot purpose** (thumbnail base, hook close-up, B-roll, mechanism, authority intro, etc.).
3. **Pull closest-match prompt** from §2 niche reservoir.
4. **Adapt subject specifics** to the current shot's needs (specific patient name attributes from composite registry, specific objects from script).
5. **Append channel's locked style segment** (§4).
6. **Pick model** per §3 selection matrix.
7. **Generate sample** (Sample Gate per cross-channel R8 if first attempt for a new pattern).
8. **Diagnose any artifacts** per §5 if output fails.
9. **Surface to user** for thumbnail / hero shots; auto-proceed for B-roll if pattern is locked.

The agent never invents image-gen prompts from scratch when a niche template exists in §2. The bank cuts iteration cycles 50-70%.

---

## 9. Runtime checklist

Before any image-gen request:

- [ ] Niche identified
- [ ] Shot purpose identified
- [ ] Closest-match prompt pulled from §2
- [ ] Subject specifics adapted to current shot
- [ ] Channel locked style segment appended (§4)
- [ ] "No text, no graphics" instruction included (R1 compliance)
- [ ] Model picked per §3 selection matrix
- [ ] Sample Gate run for new patterns (R8 compliance)
- [ ] Artifact diagnostic bank consulted if first generation fails
- [ ] Anti-pattern audit (§6) — generic? text in image? conflicting descriptors? multi-subject?

If any check fails, fix before regenerating. Never generate at scale without Sample Gate confirmation on first new pattern.
