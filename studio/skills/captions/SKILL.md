---
name: captions
description: >-
  Designs and renders captions (S1-S4 systems + mishear dictionary). Load when burning in subtitles or designing a caption style for a channel.
---

# Skill 10 — Captions / Subtitle Design

This is the operational knowledge an AI YouTube agent needs to design and generate captions for any video in any niche. Every paragraph is a concrete rule the agent applies.

Captions are not a post-production afterthought. Captions are the second layer of voice. ~85% of mobile YouTube viewers watch with the sound muted in feed before deciding to click; ~40% of full-watch sessions have captions enabled the entire time. Get captions wrong and the script's voice work doesn't reach those viewers.

This skill is the load-bearing companion to Skills 03 (Script) and 08 (Voice/TTS). Skills 03 and 08 produce the spoken text. This skill produces what's on screen.

---

## 0. Runtime rendering prerequisites — READ FIRST WHEN BURNING IN CAPTIONS

Captions only "exist" once they're rendered as pixels onto video frames. The sandbox runtime has a quiet failure mode where ffmpeg's `subtitles` filter encodes "successfully" but produces zero visible glyphs — see bug `br_dzbtejse5m3y68scexrit6fg`. If the agent doesn't actively check, the user sees the same video back with no captions and you reported done. Avoid this.

### Available fonts

The sandbox installs `fontconfig` + `dejavu-sans-fonts` at boot. The canonical DejaVu paths are:

- `/usr/share/fonts/dejavu-sans-fonts/DejaVuSans.ttf`
- `/usr/share/fonts/dejavu-sans-fonts/DejaVuSans-Bold.ttf`
- `/usr/share/fonts/dejavu-sans-fonts/DejaVuSans-Oblique.ttf`

The fontconfig family names that resolve are `DejaVu Sans` and `DejaVu Sans Bold`. Any other font family (Inter, Anton, Bebas Neue, etc.) will silently fall back to "no font", which renders as nothing. If you want one of the channel's brand fonts, drop the `.ttf` into `/workspace/assets/fonts/` and reference it explicitly via `:fontsdir=` (below).

### Safe ffmpeg `subtitles` invocation

When burning in an SRT/ASS file, always pass `fontsdir=` and a font name that resolves. The minimum-viable safe form:

```bash
ffmpeg -i in.mp4 \
  -vf "subtitles=captions.srt:fontsdir=/usr/share/fonts/dejavu-sans-fonts:force_style='FontName=DejaVu Sans Bold,FontSize=24,PrimaryColour=&HFFFFFF&,OutlineColour=&H000000&,Outline=2,Alignment=2'" \
  -c:a copy out.mp4
```

Pieces that matter:
- `fontsdir=` — explicit so libass doesn't depend on the wider system fontconfig path.
- `FontName=DejaVu Sans Bold` — must match an actual TTF in `fontsdir`. Drop `Bold=1` when you specify a Bold-family font directly; combining `FontName=...Bold` with `Bold=1` requests a synthetic bold-on-bold and looks chunky.

For channel-branded fonts, copy the TTF into `/workspace/assets/fonts/` first and point `fontsdir=` there.

### Transcribe local audio when you don't already have the script

If the spoken text is something you wrote (HeyGen `--script`, ElevenLabs TTS text, Minimax voice prompt), USE THAT TEXT DIRECTLY — distribute it over the audio duration. No transcription needed. The script you wrote is in your conversation context.

Only when you don't have the script (user uploaded a video, you're captioning a reference clip), transcribe the audio. The simplest path:

```bash
# Extract audio from the video into your session dir
ffmpeg -i input.mp4 -vn -acodec copy audio.wav

# Word-timed alignment via fal.ai Whisper
curl -s -X POST "https://queue.fal.run/fal-ai/whisper" \
  -H "Authorization: Key $FAL_API_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"audio_url\": \"data:audio/wav;base64,$(base64 -w0 audio.wav)\", \"task\": \"transcribe\", \"chunk_level\": \"word\"}"
```

The response JSON has per-word `{ word, start, end }` entries in the `chunks` array. Use those to build an SRT or ASS file with accurate timing.

### Verify before declaring done

After burning captions, do NOT just `present` the output — `present` is for handing finished work to the user, not for QA. Extract a frame from inside one of the caption time-ranges and `Read` it; the polymorphic Read tool loads the JPG as visible pixels in your context, so you can confirm the text actually rendered (and that the styling looks right):

```bash
ffmpeg -y -ss 2 -i out.mp4 -frames:v 1 _cap_probe.jpg 2>/dev/null
```

Then `Read _cap_probe.jpg`. If you don't see captions in the frame, libass rendered nothing — re-run the burn-in with an explicit `fontsdir=` (above) before reporting success to the user. If captions are visible but styling is off (wrong size, wrong position, mistimed), fix `force_style` and re-verify. **A frame containing visible text isn't enough — read the text and confirm it matches what you expect to see. Captions that render perfectly but say the wrong thing are the most common silent failure.**

### SRT inline-style gotchas

libass interprets `<b>...</b>` / `<i>...</i>` tags inside SRT lines. Curly-brace ASS overrides (e.g. `{\b1}word{\b0}`) work too. Stick to one form per file — don't mix HTML tags with ASS overrides.

---

## 1. The job of captions

Captions do six things in a long-form video:

1. **Hold sound-off retention** — viewers who scroll into the feed with sound off decide in 2-3 seconds whether to click; readable captions are the only way the script's hook lands.
2. **Anchor attention through the valleys** — the 30%, 60%, 80% retention dips. Captions force the eye to track even when the brain drifts.
3. **Reinforce voice register** — the typography, color, and motion of captions IS part of the brand. A long-form mystery-documentary channel's calm cinematic captions vs a top viral-challenge creator's bold-pop captions are the channel signal.
4. **Pre-screen audience** — captions self-select. Niche vocabulary in the captions tells the right viewer "this is for you."
5. **Maintain accessibility** — 466M people globally have hearing loss; captions are non-negotiable for genuinely accessible content.
6. **Improve SEO** — YouTube indexes captions for search ranking. Auto-generated captions index poorly; human/anchor-aligned captions index well.

The agent never treats captions as filler. Every caption is composed: word selection, line break, timing, position, typography.

---

## 2. The optimization target

The agent optimizes captions against **sound-off-retention-weighted-watch-time**. Specifically:

- **0:00-0:03 sound-off legibility** — can a feed-scrolling muted viewer read the hook?
- **Mid-video readability** — at 30%, 60%, 80% retention valleys, do captions still hold attention?
- **Mobile readability** — captions readable on a 375×667 screen (iPhone SE), the smallest active mobile target.
- **Accessibility compliance** — captions cover 100% of dialogue, named speakers when ambiguous, key sound effects.

A caption system that hits all four metrics is "watch-time positive." A caption system that nails accessibility but fails sound-off retention is incomplete.

---

## 3. The four caption systems

The agent picks ONE system per channel and locks it. Mixing systems within a video reads inconsistent.

### S1 — Word-timed pop captions (default for high-energy)

**Style:** 1-3 words pop on screen as spoken, replaced by next 1-3 words. Each word lands within ±50ms of the spoken phoneme.
**Best for:** vertical shorts, hype showman, top viral-challenge style, gaming, vlogs.
**Pros:** highest sound-off retention; mobile-optimized; reinforces pacing.
**Cons:** distracting in long-form authority; requires precise word-level alignment (Whisper word timestamps + manual cleanup).
**Per-character cost:** highest — every word needs precise timestamp.

### S2 — Sentence-paced captions (default for long-form)

**Style:** 5-12 words at a time, spanning 1-2 lines, replaced when the sentence boundary or breath lands. Each block holds ~2-3 seconds.
**Best for:** long-form authority, documentary, news-hijack, true crime, faith.
**Pros:** readable without distracting; reinforces voice's natural cadence; cheap to produce.
**Cons:** lower sound-off retention than S1; less effective for fast pacing.
**Per-character cost:** moderate.

### S3 — Sentence-paced + emphasis pop (hybrid)

**Style:** Default to S2 sentence-paced display, but specific keywords (numbers, names, dollar amounts) get S1 word-pop treatment with stronger typography.
**Best for:** personal-finance authority IRS, medical-authority health — niches where specific numbers/names carry the load.
**Pros:** combines S2 readability with S1 emphasis on the data viewers came for.
**Cons:** highest production complexity.

### S4 — Block paragraph captions (rare)

**Style:** Full sentence or sub-paragraph displayed as a stationary block, often in a corner or lower third. Stays on screen 4-8 seconds.
**Best for:** ambient/sleep, music videos, art content, rare contemplative moments.
**Pros:** readable without competing with visuals; matches reverent register.
**Cons:** poor for retention-driven content; reads dated for most niches.

### Selection decision tree

1. Is the niche high-energy / vertical short / gaming? → S1 word-pop.
2. Is the niche authority / documentary / true crime? → S2 sentence-paced.
3. Does the niche center specific numbers/names viewers came for? → S3 hybrid.
4. Is the niche ambient or music-led where captions are atmosphere? → S4 block.

The agent records the chosen system in the Channel Profile (Skill 05 §3 Visual Style DNA → Caption System) and never mixes within a video.

---

## 4. The anchor-based alignment system (the hard-won lesson)

This is the load-bearing technical rule for caption accuracy. It came out of production (the v6 anchor-aligned captions beat the previous DP alignment system).

### The problem with sequential 1:1 alignment

Most caption systems take Whisper's word-level transcription and assume the words were spoken in the order Whisper produced them. This fails when:
- Whisper mishears ("Form SSA-634" → "form essay 634")
- The voice generator (ElevenLabs/Minimax) drops or repeats syllables
- Background music shifts Whisper's segmentation
- Long pauses cause Whisper to over-segment

A 1:1 sequential mapping locks the wrong word to the wrong timestamp. Captions land 200-500ms off by the end of a 10-minute segment. The viewer reads "the next" while hearing "Patricia."

### The anchor-based fix

The agent runs alignment in two passes:

**Pass 1 — Anchor extraction.** The agent identifies "anchor" words/phrases in the original script — high-information specific words that Whisper transcribes correctly nearly always: dollar amounts, named entities, specific dates, form numbers. Typically ~1 anchor per 5-10 seconds of script.

**Pass 2 — Inter-anchor interpolation.** Between anchors, the agent stretches/compresses Whisper timestamps to match the script word count. The anchor positions are trusted; the words between are positioned by linear interpolation against the script's word-count-per-anchor-segment.

This is what the production rule "v6 word-timed captions beat DP alignment" refers to.

### Anchor selection heuristics

Strong anchors (Whisper transcribes correctly >95% of the time):
- Dollar amounts with explicit currency words ("twenty thousand dollars")
- Common given names ("Patricia," "Robert," "David")
- Common organization names ("Social Security," "IRS," "Treasury")
- Specific dates fully written ("April twenty third")
- Common nouns of high specificity ("retirement," "supplement," "kidney")

Weak anchors (don't use):
- Acronyms or initialisms ("SSA," "IRS" alone — Whisper sometimes drops these)
- Form numbers ("Form 634" — often rendered as "form 634" or "634")
- Foreign names without context
- Numbers under 10 (Whisper renders inconsistently as digits or words)

The agent selects 1 anchor per 5-10 seconds of audio. Below 1 per 10 seconds and drift compounds; above 1 per 5 seconds and the anchor extraction step takes longer than the alignment.

### Per-pipeline anchor density

| Pipeline | Anchor target | Notes |
|---|---|---|
| Personal-finance authority IRS | 1 per 5-7 sec | Dollar-heavy, specific forms — anchors abundant |
| Medical-authority health | 1 per 6-8 sec | Patient names + dosages |
| Documentary explainer | 1 per 8-10 sec | Slower pace, fewer anchors needed |
| Vertical short | 1 per 3-5 sec | Fast pace requires tighter anchoring |
| Music video | beat-locked | Anchors don't apply; lyrics are aligned to bars |
| News-hijack | 1 per 6-8 sec | Named villains + dollar amounts |

---

## 5. Typography rules

Caption typography IS brand. The agent locks these in the Channel Profile per channel.

### Font selection

**For S1 word-pop (high-energy):**
- Sans-serif bold display: **Bebas Neue Bold**, **Anton**, **League Gothic**, **Bangers**.
- Caps height: 28-36% of canvas height in vertical (9:16); 12-18% in horizontal (16:9).
- Stroke: 4-8px black outer stroke for readability over any background.

**For S2 sentence-paced (authority):**
- Clean sans-serif body: **Inter**, **Open Sans**, **Source Sans Pro**, **Helvetica Neue Bold**.
- Caps height: 4-6% of canvas height in horizontal.
- Stroke: 2-4px black outer stroke OR semi-transparent background panel.

**For S3 hybrid:**
- Body in S2 body font, emphasis words in S1 display font with color shift.

**Forbidden fonts:**
- Comic Sans, Papyrus, Curlz, any font with a meme association
- Monospace fonts (read as code/terminal even when not)
- Fonts with extreme letter-spacing requirements that don't render cross-platform

### Color palette per niche

| Niche | Primary text | Emphasis text | Background panel |
|---|---|---|---|
| Personal-finance authority IRS | White `#FFFFFF` | Yellow `#FFD60A` | Black 60% opacity |
| Medical-authority health | White `#FFFFFF` | Soft red `#E63946` | Black 50% opacity |
| Roblox-scenario | Bright yellow `#FFD60A` | White | None (stroke only) |
| Brick-narrative storytelling | White `#FFFFFF` | Genre-saturated | Black 70% opacity |
| Documentary | White `#F5F0E1` (cream) | None | Subtle black gradient bottom |
| Tech / AI | White `#FFFFFF` | Modern accent (#10A37F or #6366F1) | None (stroke only) |
| News-hijack | White `#FFFFFF` | Red `#D62828` | Black 60% opacity |
| True crime | Cream `#F5F0E1` | None | Black 50% opacity |

### Position discipline

**Default position:** lower-middle third, anchored at `(50%, 80%)` for 16:9; `(50%, 78%)` for 9:16.
**Authority register variant:** lower-left, anchored at `(15%, 85%)` — reads more "broadcast news."
**Music video variant:** centered upper-third for hook; lower-third for verses.

The agent NEVER places captions in the upper third. Upper third is reserved for title cards, channel branding, on-screen text overlays. Cluttering upper third with captions creates visual noise.

### Line break rules

- **Maximum 2 lines per caption display.** Three lines on a mobile screen is unreadable.
- **Maximum ~36 characters per line** in 16:9; ~24 characters per line in 9:16.
- **Break at natural language boundaries:** between clauses, before prepositions, never mid-word.
- **Never split a phrase that's the punchline:** "the form / number" — breaks the read. Move "form number" together to the next display.

### Duration discipline

- **Minimum on-screen time:** 1.0 second (S1 word-pop can go to 0.7s for very short words; below that, eye can't catch).
- **Maximum on-screen time:** 4.0 seconds (after 4s, viewer assumes the caption froze and disengages).
- **Ideal: 1.5-2.5 seconds per S2 sentence-paced display, 0.8-1.5s per S1 word-pop.**

---

## 6. Per-niche caption playbooks

The 12 highest-priority niches with their locked caption recipes.

### Personal-finance authority channel (V2 IRS authority)

```
System: S3 hybrid (sentence-paced + emphasis pop)
Font body: Inter Bold
Font emphasis: Bebas Neue Bold (for dollar amounts, form numbers, dates)
Body color: #FFFFFF
Emphasis color: #FFD60A (yellow)
Background panel: black 60% opacity rounded rectangle
Position: lower-middle third (50%, 80%)
Caps height body: 5% canvas
Caps height emphasis: 8% canvas
Anchor density: 1 per 6 sec
Forbidden: emoji in caption text (breaks authority register)
```

### Medical-authority channel (V2 medical)

```
System: S3 hybrid
Font body: Open Sans Bold
Font emphasis: Inter ExtraBold
Body color: #FFFFFF
Emphasis color: #E63946 (soft red for medical terms)
Background panel: black 50% opacity
Position: lower-middle third
Anchor density: 1 per 7 sec
Special rule: dosages and supplement names always in emphasis style
```

### Roblox-scenario (V6 vertical hype)

```
System: S1 word-pop
Font: Bangers OR Bebas Neue Bold
Color: bright yellow #FFD60A with 6px black stroke
Position: centered, anchored (50%, 50%) for hook; (50%, 75%) for verses
Caps height: 32% canvas (large for vertical mobile)
Anchor density: 1 per 4 sec
Special rule: "BRO" and "WAIT" displayed at +20% size
Animation: subtle scale-in (1.0 → 1.05) over 0.1s on each pop
```

### Brick-narrative storytelling channel (V14 music video)

```
System: S4 block paragraph (lyric blocks aligned to bars)
Font: Anton OR display sans-serif
Color: white with genre-saturated emphasis (channel-specific)
Background: black 70% opacity for readability over saturated visuals
Position: lower-middle third (50%, 78%)
Caps height: 6% canvas
Special rule: hook line repeats with same caption treatment each chorus
Special rule: ad-libs at end of bars in smaller secondary text
```

### Documentary explainer (long-form mystery-documentary style)

```
System: S2 sentence-paced
Font: Inter SemiBold OR Source Sans Pro Bold
Color: cream #F5F0E1 (matches long-form mystery-documentary aesthetic)
Background: subtle black gradient bottom 20% of canvas
Position: lower-middle third (50%, 82%)
Caps height: 4.5% canvas (smaller, reads more cinematic)
No emphasis pop — register is calm and consistent
Forbidden: bright accent colors (breaks moody register)
```

### Tech / AI (developer-channel adjacent)

```
System: S2 sentence-paced with code-aware emphasis
Font body: Inter Bold
Font code: JetBrains Mono Bold (for inline code references)
Body color: #FFFFFF
Code emphasis color: niche accent (#10A37F or #6366F1)
Background: stroke-only (4px black outer stroke)
Position: lower-middle third
Special rule: code references ALWAYS in monospace; product names ALWAYS in body sans
```

### News-hijack documentary (investigative-journalism adjacent)

```
System: S3 hybrid
Font body: Inter Bold
Font emphasis: Bebas Neue Bold
Body color: #FFFFFF
Emphasis color: #D62828 (red for villain names + dollar amounts)
Background panel: black 60% opacity
Position: lower-middle third
Anchor density: 1 per 6 sec
Special rule: named villains always in emphasis red
Special rule: documents shown on screen get callout caption with arrow
```

### True crime

```
System: S2 sentence-paced (slower pace honored)
Font: Inter SemiBold (less aggressive than Bold)
Color: cream #F5F0E1
Background: black 50% opacity
Position: lower-middle third
Caps height: 4.5% canvas (smaller, weighted register)
Duration: 2.5-3.5 sec per display (slower than authority default)
Forbidden: emoji, bright colors, motion animations beyond fade-in
```

### Cooking

```
System: S3 hybrid
Font body: Open Sans Bold
Font emphasis: Bebas Neue Bold (for ingredient amounts, temperatures)
Color: white with warm orange #E76F51 emphasis
Background: stroke-only or warm semi-transparent
Position: lower-middle third
Special rule: temperatures and ingredient measurements always in emphasis
```

### Real estate

```
System: S3 hybrid
Font: Inter Bold
Color: white with green #06A77D emphasis (dollars and percentages)
Background: black 50% opacity
Position: lower-middle third
Anchor density: 1 per 6 sec
Special rule: dollar amounts and percentages always in green emphasis
```

### Faith / Christian

```
System: S2 sentence-paced
Font: Source Sans Pro Bold OR a refined serif (Crimson Pro)
Color: cream #F5F0E1 OR muted gold #C9A227
Background: subtle gradient (no panel)
Position: lower-middle third
Caps height: 4% canvas (smaller, reverent)
Forbidden: bright colors, ALL CAPS, emoji
```

### Vertical short (general)

```
System: S1 word-pop
Font: Bebas Neue Bold OR Anton
Color: locked per channel
Position: centered for hook, lower-third for body
Caps height: 28-36% canvas
Anchor density: 1 per 3-5 sec
```

---

## 7. The mishear dictionary system

Whisper makes the same mistakes consistently across runs. The agent maintains a per-channel mishear dictionary that auto-corrects known errors before captions are rendered.

### How the dictionary builds

After every video, the agent diffs Whisper's output against the original script. Recurring substitutions go in the mishear dictionary:
- "SSA" → Whisper renders as "essay" → dictionary fixes
- "Form 7004" → Whisper renders as "form 70 oh 4" → dictionary fixes
- Host name → Whisper sometimes renders as a phonetically similar spelling → dictionary fixes (channel-locked spelling)

### Per-channel dictionary samples

**Personal-finance authority channel:**
- "essay" / "S-S-A" → "SSA"
- Host first-name variants → channel-locked spelling
- "form seven thousand four" → "Form 7004"
- "form seventy oh four" → "Form 7004"
- "I R S" → "IRS"

**Medical-authority channel:**
- Expert name variants → channel-locked spelling
- specific drug names per video — added per script

**Brick-narrative storytelling channel:**
- character names — added per video
- "Iran" / "Iranian" — capitalization locks

The mishear dictionary is loaded BEFORE caption rendering. Whisper's raw output passes through the dictionary, then the anchor-based alignment runs.

---

## 8. Caps and emphasis discipline

### When ALL CAPS is allowed

ALL CAPS in captions follows different rules than ALL CAPS in titles.

**Allowed:**
- Vertical hype channels (Roblox-scenario) where ALL CAPS is the brand
- Specific 1-2 word emphasis pops (the same way scripts use ALL CAPS in title formula F12)
- Drill music video hook lines

**Forbidden:**
- Long-form authority (reads aggressive)
- Documentary (breaks register)
- True crime (breaks reverence)
- Faith (breaks reverence)

### When italics are allowed

Italics in captions are rare. Use only for:
- Foreign words: "the *amygdala* responds to..."
- Quoted speech inside narration: "she said *I never received the letter*"
- Latin terms in medical/legal: "*per se*"

Italics for emphasis (bolding-via-italics) — never. Use color or weight shift instead.

### When emoji is allowed

Default: never in captions. Emoji in captions reads spammy and breaks immersion.

Narrow exceptions:
- Vlog / lifestyle / cooking — single emoji per display max, only when the speaker actually says the thing
- Vertical hype shorts — single emoji per pop max, sparingly

For personal-finance authority, medical-authority, documentary, true crime, faith, real estate authority, news-hijack: NEVER.

---

## 9. The 1.2 second cap rule (production lesson)

This is a load-bearing production rule from the v6 captions iteration: any single caption display should not exceed 1.2 seconds for word-pop OR exceed the natural breath/sentence boundary for sentence-paced.

The reason: when a caption stays on screen longer than 1.2 seconds for word-pop, viewers think the audio froze and tab away. When a sentence-paced caption stays past the spoken sentence end, viewers feel out of sync.

### Implementation

For S1 word-pop:
- Whisper word timestamps with manual cap at 1.2s — if Whisper claims a word lasts 1.5s, force-replace with next word at 1.2s.
- For very short utterances ("Yes," "OK"), 0.7s minimum.

For S2 sentence-paced:
- Cap at the next breath boundary OR 4.0 seconds, whichever is shorter.
- Use Whisper's segment boundaries (which respect breath) over strict timing.

For S3 hybrid:
- Body caps at sentence-paced rules; emphasis pops cap at 1.2s.

For S4 block:
- Cap at 6 seconds; if the spoken text is longer, split into two blocks.

---

## 10. Mobile readability protocol

Every caption is validated against mobile readability before rendering full video.

### The three test sizes

1. **375×667 (iPhone SE)** — smallest active mobile target.
2. **390×844 (iPhone 14 standard)** — most common mobile target.
3. **414×896 (iPhone 14 Plus)** — large mobile target.

### The legibility test

Render a single caption frame at each size. Squint test:
- Body caption legible at 50% zoom on 375 width? PASS.
- Emphasis caption legible at 25% zoom on 375 width? PASS.
- Background panel readable contrast (text-to-panel ≥ 7:1)? PASS.

If any fails: increase font size, increase stroke width, or increase background panel opacity.

### The motion-tolerance test

Captions over moving backgrounds. Test by rendering the caption against:
- Bright high-frequency motion (water, leaves, traffic)
- Dark low-frequency motion (slow camera moves through interiors)
- Mixed-luminance motion (sunset, neon, explosions)

If readability drops at any of these, the background panel needs to be made more opaque or the stroke widened. Stroke + panel together is acceptable.

---

## 11. Generation workflow

When generating captions for any video:

1. **Identify caption system** from Channel Profile (S1/S2/S3/S4). If new channel, pick from §3 selection tree.
2. **Run Whisper** on the rendered audio to get word-level timestamps.
3. **Apply mishear dictionary** for the channel (§7).
4. **Extract anchors** from script — 1 anchor per 5-10 sec depending on niche (§4).
5. **Anchor-align** Whisper output against script — Pass 1 (anchor extraction), Pass 2 (inter-anchor interpolation).
6. **Apply 1.2s cap rule** (§9).
7. **Generate caption frames** at the channel's locked typography (§5, §6).
8. **Run mobile readability protocol** (§10).
9. **Render captions over video** at the channel's locked position.
10. **Spot-check** at random 5 timestamps for sync, readability, register.

Time budget: 8-15 minutes for a 25-min long-form video. 3-5 minutes for a 90-second vertical short.

---

## 12. Anti-patterns

Eight caption mistakes that fail in production.

### Anti-pattern 1 — Auto-generated YouTube captions left on

YouTube's auto-captions are wrong ~6-12% of the time on specialty content. Leaving them on means a misheard "essay" in place of "SSA" gets indexed and shown to mobile viewers.
**Fix:** disable auto-captions; upload custom captions per this skill.

### Anti-pattern 2 — Captions that just transcribe the audio

Verbatim Whisper output without anchor-alignment, mishear dictionary, or typography care. Reads as "AI captions" and breaks register.
**Fix:** run the full §11 workflow.

### Anti-pattern 3 — Mid-word line breaks

"the next four / months are critical" — eye stutters at the break.
**Fix:** §5 line break rules — break at natural language boundaries.

### Anti-pattern 4 — Caption blocking the visual subject

A caption panel covering the host's face or the document being shown. Caption is supposed to support the visual, not block it.
**Fix:** position discipline (§5). If the lower third is occupied by the visual subject, shift caption to the alternate position (§5 broadcast-news variant) or reduce panel opacity.

### Anti-pattern 5 — Mixed caption systems within one video

Hook is S1 word-pop; main body is S2 sentence-paced; close is S4 block. Reads as inconsistent production.
**Fix:** lock ONE system per channel. Honor across the entire video.

### Anti-pattern 6 — Emoji injected into authority captions

"💰 Patricia lost $1,800 💰" in a personal-finance authority video. Breaks the credentialed expert register and trips the post-July-2025 inauthentic content tone.
**Fix:** zero emoji in authority/medical/documentary/true crime/faith captions.

### Anti-pattern 7 — Fixed-position captions over animated overlays

Caption sits at lower-middle third; channel motion graphic also at lower-middle third (a price tag, a form callout). Caption and graphic collide.
**Fix:** when motion graphics occupy the caption position, shift caption (per §5 alt position) for that segment OR shift the motion graphic.

### Anti-pattern 8 — No mishear correction for known terms

"Form S-S-A six-three-four" rendered as "form essay 634" because Whisper missed it. Caption indexes incorrectly; viewer can't search the term.
**Fix:** maintain channel mishear dictionary (§7); apply before caption render.

---

## 13. Worked examples

### Example 1 — Personal-finance authority IRS caption frame (S3 hybrid)

Script:
> "Patricia is seventy two years old. She receives a Social Security retirement benefit of two thousand four hundred and ninety dollars per month."

Caption frames:
- Frame 1 (1.8 sec): "Patricia is seventy two years old." — Inter Bold white, black panel.
- Frame 2 (2.4 sec): "She receives a Social Security retirement benefit of" — Inter Bold white, black panel.
- Frame 3 (1.5 sec): "**$2,490**" rendered emphasis style: Bebas Neue Bold yellow #FFD60A, black panel, scale-in animation.
- Frame 4 (1.0 sec): "per month." — Inter Bold white, back to body style.

Total time on screen for this passage: 6.7 sec. Matches the spoken length within ±150ms. Anchor word "$2,490" gets emphasis treatment because it's the data viewers came for.

### Example 2 — Roblox-scenario caption frame (S1 word-pop)

Script:
> "BRO! Imagine if a TEACHER actually OWNED Roblox. Like — your fifth grade history teacher just buys the entire game."

Caption frames (each ~0.7-1.0 sec):
- "BRO!" (scale-in + +20% size, bright yellow, centered)
- "Imagine if"
- "a TEACHER" (bright yellow emphasis)
- "actually"
- "OWNED Roblox" (bright yellow emphasis)
- "Like —"
- "your fifth grade"
- "history teacher"
- "just buys"
- "the entire game"

Each pop landing on the spoken word ±50ms. ALL CAPS preserved from original script for emphasis words. Centered position for hook section.

### Example 3 — Brick-narrative storytelling caption frame (S4 block, beat-locked)

Script (4-bar stanza at 136 BPM, ~7 seconds):
> "You're not Jesus, you're not the savior / You ain't ever lived through what we lived through / You came to our country with your fancy talk / But you came to our country and we didn't ask you to..."

Caption frame: full 4-line block displayed across the full ~7 seconds, with subtle bar-line accent (line color brightens slightly on the downbeat of each bar). Anton font, white with channel-saturated emphasis on "Jesus" / "savior." Black 70% opacity panel.

### Example 4 — Documentary explainer caption frame (S2 sentence-paced)

Script:
> "On the morning of October 23rd, 1972, a small experiment in a basement laboratory in Berkeley produced a result the researcher did not expect."

Caption frames (each ~2.5-3.0 sec):
- Frame 1: "On the morning of October 23rd, 1972,"
- Frame 2: "a small experiment in a basement laboratory in Berkeley"
- Frame 3: "produced a result the researcher did not expect."

Inter SemiBold cream #F5F0E1, subtle black gradient bottom of frame. No emphasis pops — register is consistently calm.

---

## 14. Runtime checklist

Before any caption set surfaces on a rendered video:

- [ ] Caption system locked from Channel Profile (S1/S2/S3/S4)
- [ ] Whisper output run through channel mishear dictionary
- [ ] Anchors extracted at niche-appropriate density
- [ ] Anchor-based alignment (Pass 1 + Pass 2) applied
- [ ] 1.2s cap rule applied to word-pop displays
- [ ] Typography matches channel lock (font, color, position, panel)
- [ ] No mid-word line breaks
- [ ] Mobile readability validated at 375×667
- [ ] Captions don't collide with motion graphics
- [ ] Anti-patterns audited (§12)
- [ ] No emoji in authority/medical/documentary/true crime/faith captions
- [ ] Spot-check at 5 random timestamps for sync, readability, register

If any check fails, regenerate the failing segment. Never publish video with failing captions.

---

## 15. Cross-skill connections

This skill connects to:
- **Skill 03 (Script Writing):** the script's voice register dictates caption typography (V1 Documentary Authority → S2 calm cream; V6 Hype Showman → S1 bright yellow word-pop).
- **Skill 05 (Reference Channel Ingestion):** the Channel Profile's Visual Style DNA includes a "Caption System" field that locks the choice.
- **Skill 06 (Storyboard):** caption position must be coordinated with motion graphics and lower-third overlays planned in storyboard.
- **Skill 08 (Voice/TTS):** the same script that drives voice generation drives anchor extraction. Voice settings affect Whisper segmentation, which affects alignment.
- **Skill 09 (Image Generation):** the text-in-code rule applies to captions exactly as it applies to thumbnail text — never let an image-gen model produce caption text.

When caption rendering fails, the agent first checks whether the failure is a caption issue (this skill) or a Voice/TTS issue (Skill 08) — sometimes the audio's rhythm is what's wrong, and adjusting captions can't fix it.
