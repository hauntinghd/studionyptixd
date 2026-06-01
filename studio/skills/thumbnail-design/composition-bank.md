# Skill 02 Companion — Thumbnail Composition Bank

This is the runtime companion to Skill 02 (Thumbnail Design). Skill 02 names the 12 patterns and describes them in prose. This file gives the agent the **pixel-level composition spec** for each pattern — exact subject placement, face position, text placement, color anchors, contrast targets — plus a bank of worked image-gen prompts.

The agent loads this when generating thumbnail keyframes. Instead of inventing composition each time, it cites the pattern's locked spec and adapts the topic into it.

Coordinate convention used throughout: 1280×720 canvas (YouTube standard). Coordinates given as `(x_pct, y_pct)` from top-left, so `(33%, 33%)` is the upper-third intersection (top-left rule-of-thirds anchor).

---

## 1. Why pixel-level specs matter

Most thumbnail advice is verbal: "put the face on the left, add a number in the corner." That's not enough for an AI agent generating images at scale. Two thumbnails that follow the same verbal rule can look completely different — one wins, one fails — because the actual pixel placement, contrast ratio, or text size landed differently.

A locked composition spec collapses that variance. The agent generates within the spec, and the spec is calibrated to the patterns that actually win in production.

The four metrics every spec controls:
1. **Focal hierarchy** — what the eye lands on at 0ms, 100ms, 300ms
2. **Mobile legibility** — readable at 480×270 (small mobile feed thumbnail size)
3. **Contrast budget** — luminance separation between subject and background, target ≥ 4.5:1 for text, ≥ 3:1 for primary subject
4. **Color anchor** — the single saturated color that pulls the eye in feed

---

## 2. Pixel-level composition specs (P1-P12)

### P1 — Face + Arrow + Number

**Canvas zones:**
- **Face:** left third. Eyes positioned at `(28%, 38%)` — slightly above the rule-of-thirds top intersection. Head occupies ~45% of canvas height. Shoulders cut at bottom edge.
- **Arrow:** mid-frame, originating from face's gaze direction, terminating at `(67%, 50%)`. Arrow color = same as the bright accent (red or yellow).
- **Number:** upper-right corner. Anchored at `(85%, 22%)`. Font size = 22-28% of canvas height (180-200px on a 720 canvas). White or yellow with thick black stroke (8-12px).
- **Background:** dark, low-saturation. The bright accent (face skin tone, arrow, number) does the lifting.

**Color anchors:** primary red `#E63946` or yellow `#FFD60A` on a `#1A1A1A` to `#2D2D2D` dark background.
**Contrast target:** number text contrast ≥ 7:1 against background. Face-to-background luminance ratio ≥ 5:1.
**Mobile readability:** number must read at 240×135. Test by squinting at 25% zoom — number is the survivor.

**Worked image-gen prompt:**
> "Photoreal thumbnail composition. Left third of frame: middle-aged white male, age approximately 50, short professional grey hair, navy suit, expression of concerned authority looking slightly off-camera to the right. Eyes at upper-third position. Background: dark gradient navy to black. No text, no graphics — text and graphics will be added in code. Cinematic key light from camera-left. 1280x720. Subject head occupies left 40% of frame, right 60% intentionally empty negative space for graphics overlay."

The arrow and number are added in code (Remotion / Photoshop / SVG overlay), never in the image-gen prompt.

### P2 — Rage Stamp

**Canvas zones:**
- **Stamp text block:** centered or upper-center, anchored at `(50%, 35%)`. Tilted 8-12° counter-clockwise. Text size = 28-35% of canvas height. Three-word maximum (e.g., "MUCH WORSE", "CONFIRMED", "SHOCKING").
- **Background subject:** face-in-shock OR object-of-rage centered behind the stamp at lower opacity (60-70%). Subject occupies bottom 60% of canvas.
- **Stamp color:** saturated red `#D62828` or yellow `#FFB627` text, white inner stroke (4px), thick black outer stroke (10-14px).
- **Distress treatment:** texture overlay on stamp at 15-25% opacity. Edges roughed via clipping mask.

**Color anchors:** primary red `#D62828` on a desaturated background. Background subject treated cool (cyan-shifted) so red pops.
**Contrast target:** stamp text ≥ 9:1 against any underlying area.
**Mobile readability:** stamp must dominate at 240×135. Subject is supportive; stamp is the click.

**Worked image-gen prompt (background subject):**
> "Photoreal background image for thumbnail composition. Centered: middle-aged Caucasian woman, age 65, expression of shocked alarm, mouth slightly open, looking directly at camera with wide eyes. Hair grey, neutral blouse. Background: out-of-focus interior of a bank or government office, warm lighting from windows. Cinematic, slightly desaturated. 1280x720. Note: text and graphic stamp will be added in code, leave upper-third clear for overlay."

The stamp is added in code with the tilt, stroke, and distress treatment.

### P3 — Before/After Split

**Canvas zones:**
- **Vertical split:** divider at exactly `x = 50%`. Sometimes diagonal (8-12° tilt) for energy.
- **Before (left):** desaturated, slightly cool-shifted, often with red X overlay at `(25%, 25%)`. Subject framed centered in left half.
- **After (right):** vibrant, slightly warm-shifted, green check at `(75%, 25%)`. Subject framed centered in right half.
- **Optional center arrow:** anchor at `(50%, 50%)`, arrow body 12-15% of canvas width.

**Color anchors:** red `#D62828` and green `#06A77D` for the X and check. Subjects retain natural color but with saturation/temperature differential.
**Contrast target:** the after-side luminance ≥ 1.4× the before-side luminance. Eye reads the brighter side as "good."
**Mobile readability:** the split must be visible at 240×135. The X and check are the secondary read.

**Worked image-gen prompt (each side generated separately and composited):**
> "Photoreal thumbnail half-frame, vertical orientation 640x720. Subject: middle-aged man, age 45, slumped posture, defeated expression, dim grey lighting, slightly desaturated, cool color grade. Wearing wrinkled white shirt. Background: dim interior, single overhead bulb. Cinematic, intentionally muted."

> "Photoreal thumbnail half-frame, vertical orientation 640x720. Subject: same middle-aged man, age 45, confident upright posture, faint smile, golden-hour warm lighting, vibrant saturation. Wearing crisp white shirt. Background: bright clean office, large windows, golden light. Cinematic, intentionally vibrant."

The two halves are composited in code with the X, check, and arrow overlays.

### P4 — Mystery Object

**Canvas zones:**
- **Object:** centered, anchored at `(50%, 55%)` — slightly below mathematical center. Object occupies 35-50% of canvas height.
- **Background:** dark, atmospheric, with deliberate negative space at top 30% and bottom 15%.
- **Lighting:** single key light from camera-left or above. Rim light optional. The object is the only well-lit thing in frame.
- **Text:** typically NONE on the thumbnail itself. Title carries the mystery.
- **Optional:** small specific anchor (a date, a location text element) bottom-right at `(85%, 88%)`, ~6% of canvas height.

**Color anchors:** monochrome or near-monochrome. Single accent color from the object itself (lit warm against cool dark, or lit cool against warm dark).
**Contrast target:** object-to-background luminance ratio ≥ 6:1. Background should be near-black except for the object's spill.
**Mobile readability:** the object's silhouette must be recognizable at 240×135. Test by converting to silhouette — does it still read?

**Worked image-gen prompt:**
> "Cinematic thumbnail. Single subject centered slightly below midline: an antique brass key, oxidized, lying on weathered wooden surface. Single warm key light from upper-left, deep shadow filling rest of frame. Dark moody atmosphere, charcoal-to-black background gradient. Shallow depth of field, sharp focus on key. No text, no graphics. 1280x720. Style of long-form mystery-documentary thumbnail. Photoreal, slightly cinematic film grain."

### P5 — Reaction Face

**Canvas zones:**
- **Face:** centered at `(50%, 45%)` — slightly above mathematical center. Face occupies 60-75% of canvas height. Eyes at `(50%, 30-35%)` for direct eye contact lock.
- **Optional single word:** lower third, anchored at `(50%, 80%)`. Word size ~18-22% of canvas height. Examples: "WHAT?!", "NO WAY", "$1M".
- **Background:** vibrant solid color OR slight gradient. Often complementary to skin tone (yellow background for reddish skin, blue for warmer skin).

**Color anchors:** vibrant background color (yellow `#FFD60A`, magenta `#E63987`, cyan `#3DA5D9`) — the saturation IS the click trigger.
**Contrast target:** background-to-face contrast ≥ 4:1. Face well-lit from front.
**Mobile readability:** the expression must read at 240×135. Closed-mouth shock reads better than open-mouth at small sizes.

**Worked image-gen prompt:**
> "Bright vibrant thumbnail. Centered subject: young woman, age 25, expression of stunned disbelief — closed mouth, raised eyebrows, eyes wide. Front-lit with even soft light. Looking directly at camera. Background: solid saturated yellow #FFD60A. Subject occupies upper 70% of frame. Lower 25% intentionally empty for text overlay. 1280x720. Photoreal, top viral-challenge thumbnail style."

### P6 — Authority Portrait

**Canvas zones:**
- **Subject:** centered or rule-of-thirds left. Eyes at `(33%, 38%)` if left-anchored, `(50%, 35%)` if centered. Head occupies 40-55% of canvas height. Three-quarter view often, sometimes direct.
- **Background:** office, library, studio. Out-of-focus. Single key light, soft. Often warm-grade.
- **Text:** if any, lower-third small caps title in clean sans-serif. Anchored at `(50%, 88%)`, max 6% of canvas height.
- **Color treatment:** muted, professional, often slightly cool-graded with warm skin tones for separation.

**Color anchors:** restrained — navy `#1A2A4F`, charcoal `#2D2D2D`, cream `#F5F0E1`. The lack of saturation IS the brand signal.
**Contrast target:** subject-to-background contrast ≥ 3:1. Underplays loud thumbnails on purpose.
**Mobile readability:** the face's authority register must be readable. Eye contact + steady expression is the click.

**Worked image-gen prompt:**
> "Editorial portrait thumbnail. Subject: middle-aged Asian man, age 55, distinguished, three-quarter view facing camera, calm authoritative expression, neutral closed mouth, salt-and-pepper hair, navy suit, no tie. Background: out-of-focus library or office interior, warm muted lighting. Single soft key light from camera-left. Photoreal, slightly cinematic. 1280x720. Style of science-explainer or long-form interview thumbnail. No text."

### P7 — Stacked Number Pile

**Canvas zones:**
- **Stack:** dominant lower 65% of canvas. Anchored at `(50%, 70%)`. Stack height = 50-60% of canvas height. Stack should breathe — visible perspective and dimensionality, not flat.
- **Optional host:** rule-of-thirds left, pointing at the stack. If present, host occupies 30-35% of left side.
- **Number callout:** upper-right at `(80%, 22%)`, large. Text size = 25-30% of canvas height.
- **Background:** clean, slightly desaturated, often with a single bright color anchor on the stack itself.

**Color anchors:** stack color is the protagonist. If stack is money/cash → green `#06A77D`. Stack is products → vibrant single product color.
**Contrast target:** stack-to-background contrast ≥ 4:1. Number callout ≥ 7:1.
**Mobile readability:** the stack must read as "lots of [things]" at 240×135. Quantity-shorthand is the read.

**Worked image-gen prompt:**
> "Photoreal thumbnail. Bottom 60% of frame filled with a chaotic but organized pile of one-hundred dollar bills, three-dimensional perspective, slight overhead angle. Stack overflowing forward toward viewer. Bright even lighting. Background: clean white-to-light-grey gradient. Upper 35% of frame intentionally empty for number overlay. 1280x720. Photoreal, top viral-challenge or host-led real-estate thumbnail style."

### P8 — Negative-Space Word

**Canvas zones:**
- **Word:** dominant, filling 55-65% of canvas. Anchored at `(50%, 50%)`. Single word — never two. Examples: "REVEALED", "BANNED", "$0", "DEAD".
- **Background:** solid color or simple gradient. NO subject, NO objects.
- **Optional micro-element:** small icon or tiny secondary detail at `(50%, 80%)` or `(85%, 15%)`. Adds depth without breaking the negative-space rule.

**Color anchors:** two-color maximum. Often white-on-black, black-on-yellow, or red-on-white.
**Contrast target:** word ≥ 12:1 against background. The pattern lives or dies on contrast.
**Mobile readability:** word readable at 120×68 (extreme mobile feed thumbnail size).

**Image-gen prompt:** typically NOT generated by AI — assembled in vector code (SVG / Remotion / Figma export). The font choice, kerning, and sub-pixel rendering matter too much for image generation.

**Spec instead:**
> Background: solid `#FFD60A` yellow. Text: "BANNED" in Bebas Neue Bold or similar industrial display, kerning -2%, vertically and horizontally centered. Color `#1A1A1A`. Optional thin black stroke 2px. 1280x720.

### P9 — Split Frame

**Canvas zones:**
- **Vertical split:** at `x = 50%`. Often a thin border (4-8px) between halves, often white or accent color.
- **Left subject:** anchored at `(25%, 50%)`.
- **Right subject:** anchored at `(75%, 50%)`.
- **Optional vs/and label:** centered at `(50%, 88%)` or floating between subjects.
- **Color treatment:** halves often opposite-graded (left warm + right cool, or left bright + right muted).

**Color anchors:** complementary colors. Background of left and right halves intentionally different.
**Contrast target:** each half ≥ 4:1 internal contrast. Inter-half differential ≥ 1.5× luminance.
**Mobile readability:** the comparison-fact must read at 240×135. Each subject identifiable at small size.

**Worked image-gen prompt (compose two halves separately):**
> Left: "Photoreal thumbnail half-frame 640x720. Apple iPhone Pro on white background, top-down product shot, vibrant saturation, warm lighting."
> Right: "Photoreal thumbnail half-frame 640x720. Samsung Galaxy on dark background, top-down product shot, cool blue lighting."

Composite in code with thin white divider and "VS" label.

### P10 — Color-Pop on Mono Background

**Canvas zones:**
- **Color element:** rule-of-thirds anchored at `(33%, 50%)` or `(67%, 50%)`. Element occupies 25-40% of canvas.
- **Background:** desaturated entirely (≤ 8% saturation). Color element is the only saturated thing.
- **Optional small text:** upper-right or lower-left, sub-anchor, ~8% of canvas height.

**Color anchors:** ONE saturated color, period. Often red `#D62828` or yellow `#FFD60A` on a sepia/grey/charcoal mono background.
**Contrast target:** color element saturation ≥ 80% vs background ≤ 8%. Massive saturation differential.
**Mobile readability:** the colored element must dominate at 240×135 even when desaturated background is muddy.

**Worked image-gen prompt:**
> "Photoreal thumbnail. A single red apple in sharp focus, anchored on left third at `(33%, 50%)`, vibrant saturation. Surrounding scene: photoreal kitchen counter, but rendered entirely in desaturated grey-sepia, near-monochrome. Apple is the only colored object. Cinematic, slightly graded. 1280x720. Documentary thumbnail style. No text."

### P11 — Photoreal Composite

**Canvas zones:**
- **Subject:** rule-of-thirds anchored, often with multiple composited elements.
- **Background:** dramatic environment, often impossible (interior cathedral with explosion, courtroom with stack of money, etc.).
- **Lighting:** cinematic, often three-point (key + fill + rim), graded for emotional register.
- **Text:** if any, integrated into the scene (sign, paper, etc.) rather than overlaid.

**Color anchors:** cinematic color grade. Cool blue `#1B3B6F` for somber, orange `#E76F51` for action, green `#06A77D` for money/legal.
**Contrast target:** ≥ 5:1 between subject and background. Lighting contrast ≥ 3:1 between key and shadow.
**Mobile readability:** the dramatic scene must communicate the topic at 240×135. Test by viewing at 25%.

**Worked image-gen prompt:**
> "Cinematic photoreal composite thumbnail. Foreground left: middle-aged Caucasian woman, age 60, expression of resigned worry, holding a folded letter. Background: dimly lit American kitchen, mail scattered on counter, single overhead bulb. Through window, distant view of a federal building (US Treasury or Capitol). Color grade: cool blue-gray with warm interior accent. Cinematic depth of field. 1280x720. No text. Style of HBO documentary thumbnail."

### P12 — AI-Generated Surreal

**Canvas zones:**
- **Subject:** often rule-of-thirds, sometimes deliberately off-center for dreamlike feel.
- **Background:** impossible perspective, dreamlike rendering, dramatic light angles.
- **Text:** typically NONE — surreal aesthetic is the click. If text, integrated into the surreal scene.

**Color anchors:** highly saturated, often neon or cinematic graded. Specific to the channel's locked aesthetic.
**Contrast target:** ≥ 5:1 between focal subject and background environment.
**Mobile readability:** the surreal vibe must read at 240×135 — abstract enough to look distinctive, concrete enough to be recognizable.

**Caution:** post-Jan 2026 YouTube enforcement — surreal AI thumbnails on authority/finance/health channels trigger inauthentic-content concerns. Use only on channels where surreal IS the genre (music videos, art channels, AI-creative).

**Worked image-gen prompt:**
> "Surreal cinematic thumbnail. Centered: a giant rotary telephone, photoreal but impossibly large, sitting in the middle of an empty desert at sunset. Sky: dramatic orange-to-purple gradient. Long shadow extending toward camera. Slight tilt-shift effect. Hyperreal saturation, slight film grain. 1280x720. Style of brick-narrative storytelling or surreal music video thumbnail. No text."

---

## 3. Cross-pattern composition rules

These apply regardless of which pattern the agent picks.

### The 3-zone hierarchy

Every thumbnail has three readable elements at maximum:
- **Primary:** the eye lands here at 0-100ms. The face, the stamp, the object, the word.
- **Secondary:** the eye lands here at 100-300ms. Number, arrow, supporting subject.
- **Tertiary:** the eye lands here at 300ms+ if curiosity is engaged. Channel branding, small text, micro-detail.

If a thumbnail has a fourth readable element competing for attention, drop it. The primary loses to noise.

### The mobile-first crop test

Every thumbnail must work at:
- **480×270** (mobile feed standard)
- **240×135** (mobile suggested truncation)
- **120×68** (desktop sidebar suggested)

The agent renders the thumbnail and tests readability at each size. If primary readability fails at 240×135, the thumbnail is rejected.

### The face hierarchy rule

When using face-based patterns (P1, P5, P6):
- **Closed-mouth shock outperforms open-mouth shock** at small sizes by ~12% CTR (vidIQ 2024). Open-mouth reads as "yelling" at 240×135 and turns viewers off.
- **Direct eye contact outperforms gaze-off** by ~8% CTR for authority content; **gaze-off outperforms direct** by ~6% for mystery/forensic content.
- **Face-cropped-tight outperforms face-with-shoulders** by ~10% on mobile feed.

### The text-in-code rule (mandatory)

NEVER ask the image-gen model to render text. AI models render text inconsistently — kerning, font, stroke all degrade. Text is added in code (Remotion / SVG / Photoshop) on top of the generated image.

This is the same rule as Skill 09 §2. It's mandatory for all thumbnail generation.

### The "saturated thumb test"

Place the candidate thumbnail next to 11 random YouTube thumbnails (from the actual feed of the target audience). Does the candidate stand out at the macro scale, not just the micro scale? If the candidate blends into the field, the color anchor or composition is wrong.

### The contrast budget

Every thumbnail has a contrast budget. Spend it on the primary read. Spending it on three competing elements means none win.

- **High-contrast accents:** ≤ 1 per thumbnail (the bright stamp, the number, the colored object).
- **Mid-contrast support:** 2-3 elements.
- **Low-contrast atmosphere:** the background.

If the agent finds itself making four high-contrast elements, the composition is failing. Pick ONE primary anchor.

---

## 4. Anti-pattern bank

Six thumbnails that look correct at first glance but fail in production. Diagnoses attached.

### Anti-pattern A — *The Six-Element Cluttered Frame*

Face + arrow + number + stamp + secondary subject + small caption text. Everything competing.
**Why it fails:** No clear primary read. Mobile eye bounces between elements at 240×135 and registers nothing.
**Fix:** Strip to 3 elements. Pick primary (face), secondary (number), tertiary (channel logo). Drop everything else.

### Anti-pattern B — *Open-Mouth Yelling Face Against Loud Background*

Subject with mouth wide open, screaming, against a saturated yellow + red striped background.
**Why it fails:** Reads as 2018 BuzzFeed clickbait; the retain-cohort skips reflexively. Mouth shape pixelates at 240×135 into a black hole.
**Fix:** Closed-mouth shock with slightly raised eyebrows. Calm the background to one anchor color.

### Anti-pattern C — *AI-Generated Text on the Image*

The image-gen model rendered the title onto a sign, a banner, or a chalkboard within the scene. Text shows visible AI artifacts (warped letters, inconsistent kerning).
**Why it fails:** Triggers post-July-2025 inauthentic content signals; viewers reject AI-rendered text on sight.
**Fix:** Generate the image with NO text. Add text in code. Mandatory.

### Anti-pattern D — *Cinematic Mystery Object on a Vibrant Yellow Background*

Pattern-mismatched composition. The mystery object pattern (P4) calls for moody dark backgrounds; pasting a moody object on a saturated yellow is pattern-confused.
**Why it fails:** The viewer sees "loud thumbnail = entertainment register" and clicks for entertainment, then the video is somber documentary. Click-cohort mismatch destroys retention.
**Fix:** Match composition register to content register. Documentary = dark moody. Entertainment = bright vibrant. Don't cross-pollinate.

### Anti-pattern E — *Five Faces in a Row*

Reaction-face panel with 5 different reactions stacked horizontally. Looks like a meme template.
**Why it fails:** Mobile feed renders this as a noisy strip with no readable face. None of the 5 faces is large enough at 240×135.
**Fix:** One face. Make it big. The pattern is P5 Reaction Face, not P5 Reaction-Faces-Plural.

### Anti-pattern F — *Tiny Text on a Busy Photoreal Background*

12-word title rendered in 4% canvas-height text on top of a complex photoreal scene.
**Why it fails:** Text fails the mobile readability test at 240×135. Primary read is unclear because no element dominates.
**Fix:** Either drop the text entirely (let title carry it) or make it dominant (P8 Negative-Space Word) on a clean background.

---

## 5. Per-niche composition recommendations

The patterns work in any niche, but the **dominant pattern** and the **color anchor** vary. Quick reference:

| Niche | Dominant patterns | Color anchor | Notes |
|---|---|---|---|
| Senior finance / IRS | P2, P11, P1 | Red `#D62828` or yellow `#FFD60A` | Rage register — closed-mouth concern, never smiling. |
| Senior health | P11, P6, P2 | Warm `#E76F51` or muted teal | Closed-mouth concern, doctor-credentialed look. |
| Tech / AI | P6, P10, P8 | Charcoal `#2D2D2D` + single accent | Calm authority. NO loud colors, NO ALL CAPS in text. |
| History | P4, P11, P10 | Cinematic warm-cool gradient | Dark moody. Object-focus. Long-form mystery-documentary aesthetic. |
| News-hijack docs | P11, P2, P5 | Cool blue `#1B3B6F` + red accent | Cinematic forensic. Investigative-journalism style. |
| Vertical shorts | P5, P1, P8 | Vibrant background | Big face, big single word. Native Shorts aesthetic. |
| Music video / propaganda | P12, P11 | Saturated cinematic grade | AI-surreal acceptable here; locked channel aesthetic. |
| True crime | P4, P11, P10 | Cool desaturated | Restrained, weighted. Never sensational. |
| Cooking | P3, P5, P11 | Warm food tones | Bright food shots. Closed-mouth host satisfaction. |
| Real estate | P3, P11, P1 | Warm interior + green money | Before/after dominant. House thumbnail = aspirational. |
| Faith | P6, P10, P4 | Cream / muted gold | Reverent. NO ALL CAPS. NO bright stamps. |
| Doc long-form | P10, P6, P4 | Restrained mono + accent | Tech-industrial-history, logistics-explainer style. |

---

## 6. The composition spec at runtime

When the agent generates a thumbnail:

1. **Identify pattern** from Skill 02 §3 + niche match (this companion §5).
2. **Load composition spec** from this file's §2 for that pattern.
3. **Generate image-gen prompt** following the worked example, with explicit "no text, no graphics" instruction.
4. **Render image** at 1280×720.
5. **Add text and graphics in code** using the spec's coordinates and color anchors.
6. **Run the saturated thumb test** — render against 11 niche-typical thumbnails and check standout.
7. **Run the mobile crop test** — verify primary read at 480×270, 240×135, 120×68.
8. **Run the contrast budget audit** — ensure ≤ 1 high-contrast accent.
9. **Run the anti-pattern check** — verify the thumbnail does not match patterns A-F in §4.
10. **Surface to user with composition citation** — show which pattern was used and which spec was applied.

If any check fails, regenerate the failing element. Never surface a thumbnail that fails the contrast or mobile-crop test.

---

## 7. When to consult this file vs Skill 02

**Load Skill 02 when:** picking the pattern, understanding the niche playbook at the strategy level, learning the color theory rules, learning the face-and-emotion theory.

**Load this companion when:** generating the image-gen prompt, calculating where to place text/graphics in code, auditing a candidate thumbnail against composition specs, debugging a thumbnail that "looks fine" but feels wrong.

The two files are complementary: Skill 02 = strategy + theory; this file = pixel-level execution.
