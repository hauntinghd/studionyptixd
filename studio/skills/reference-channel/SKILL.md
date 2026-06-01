---
name: reference-channel
description: >-
  Builds and locks the Channel Profile (voice DNA, visual style DNA, structural conventions). Load when launching a new channel or refreshing brand DNA. Companion: library.md (8 pre-built profiles for a long-form mystery-documentary channel / an economic-explainer channel / a science-explainer channel / a top viral-challenge creator / an investigative-journalism channel / a deadpan-history channel / a tech-industrial-history channel / a productivity creator).
---

# Skill 05 — Reference Channel Ingestion

This is the meta-skill. Every other skill in the agent (title creation, thumbnail design, script writing, description writing, voice prompting, visual generation) has its own "competitor mining" section. This skill is the overarching protocol that ingests a YouTube channel ONCE and produces a canonical Channel Profile that every downstream skill consumes.

**Get this right and the user typing "make it like the long-form mystery-documentary channel I love" becomes the most powerful single command in the product.** Get it wrong and the agent generates from training-data averages — competent but generic, exactly what TubeGen does.

This skill is what makes RookCast feel niche-portable. The user's reference channel becomes the ground truth for the agent regardless of niche. Tech, history, finance, music videos, ambient, doctor advice — the same ingestion machine works.

---

## 1. The job of reference ingestion

The "make it like X" prompt is the most common, most powerful, most differentiating user request the agent will ever receive. When a user says "I want my channel to feel like that long-form mystery-documentary channel," they're not asking for a costume. They're asking for the cumulative effect of:

- The way the reference channel constructs hooks
- The pacing of its narration
- The visual grade of its thumbnails
- The vocabulary register of its scripts
- The mystery-gap rhythm of its structure
- The chapter timestamp style of its descriptions
- The single-anchor-color discipline of its visual brand

A weak ingestion produces a single style copy ("use sepia tones"). A strong ingestion produces the **Channel Profile** — a 12-section structured artifact that lets every downstream skill (titles, thumbs, scripts, descriptions, voice, visual) generate in that channel's voice without independently re-mining.

The artifact lives in channel memory. It's pulled at every generation step. It compounds over time as the user's own videos add to it. By the time the user has 30 episodes shipped, the Channel Profile is half reference DNA, half the user's own emergent voice — which is exactly the goal.

## 2. The optimization target

The agent optimizes ingestion against **profile fidelity × generation usefulness × differentiation safety**.

- **Fidelity** — does the profile actually capture what makes the reference channel distinct? A profile that says "uses dark thumbnails" failed; a profile that says "warm sepia grade with single golden-amber anchor and 60% face-absent compositions" succeeded.
- **Generation usefulness** — can downstream skills actually use this profile to generate better output? The profile must be structured, machine-readable, and field-complete.
- **Differentiation safety** — does the profile let the agent generate IN the style without producing plagiarism? The profile must distinguish between "anchors" (the channel's brand fingerprint, never copy) and "patterns" (rhythms, structures, conventions — copy and adapt).

Quote framing for the agent's behavior: *"Steal like an artist. Steal the rhythm. Steal the register. Steal the structural patterns. Never steal phrasings. Never steal compositions. Differentiate at the surface; converge at the structure."*

## 3. The 4 ingestion depths

The agent runs ingestion at four possible depths. **Default is medium-deep.**

### Shallow ingestion (~30 seconds)

Pulls only what's available without parsing video content.
- Channel name, subscriber count, total views, total videos
- Last 20 video titles, view counts, upload dates, durations
- Last 20 video thumbnails (URLs only, not analyzed)
- Channel about-page text
- Recent community-tab posts (if visible)

Use case: when the user mentions a channel offhand and the agent wants to confirm it exists and roughly understand the niche before deciding to dig deeper.

Cost: free. Speed: 10-30 sec.

### Medium ingestion (~3 minutes)

Adds transcript and surface-level visual analysis.
- Everything from Shallow
- Transcripts of 5-7 recent videos via youtube-transcript-api
- Vision-model analysis of 8-12 thumbnails (one frame per thumbnail, structural pattern detection)
- Description text for the same 5-7 videos
- Cadence analysis (uploads per week, day-of-week pattern)

Use case: standard fast turnaround when user mentions a single reference and wants to start generating.

Cost: ~$0.05-0.15. Speed: 2-4 min.

### Medium-deep ingestion (~8 minutes) — **DEFAULT**

The agent's default mode. Strong fidelity, reasonable cost, fast enough that the user doesn't lose patience.

- Everything from Medium
- Transcripts of 10-12 recent videos
- Frame sampling: 4-6 frames per video on 5-6 videos (vision-model analyzed)
- Voice/cadence fingerprint: pacing analysis, signature transition extraction, vocabulary level, sentence-rhythm sampling from transcripts
- Thumbnail DNA extraction at full Skill 02 §9 protocol (14 visual dimensions)
- Title DNA extraction at full Skill 01 §7 protocol
- Description DNA extraction at full Skill 04 §9 protocol
- Visual style profile from frame analysis (color anchor, lighting, composition rules)
- Outlier identification: which videos are >3× channel median, what they have in common

Use case: every time the user says "make it like X" and isn't asking for a quick check.

Cost: ~$0.40-1.20 per ingestion. Speed: 6-10 min.

### Deep / forensic ingestion (~30 minutes)

Full multi-modal analysis. Reserved for when the user is paying for it (premium tier or one-time deep-dive request).

- Everything from Medium-Deep
- Full-length frame sampling on 3 outlier videos (15-25 frames per video)
- Audio fingerprint analysis (voice tone, music register, pacing markers, signature audio elements)
- Edit-cut detection: how often does the channel cut, what's the average shot length, what triggers a cut
- Statistical analysis: which formulas/patterns correlate with outlier performance vs median
- Cross-video memory rule extraction: "in 80% of videos, the host says X transition phrase before introducing a new section"

Use case: rare. Triggered when user explicitly asks for deep analysis or when the agent's medium-deep ingestion produces a profile that fails the validation checks.

Cost: $2-5 per ingestion. Speed: 25-40 min.

The agent confirms with the user before running deep ingestion ("This will take ~30 minutes and cost ~$3. Want me to do it, or stick with the standard analysis?").

## 4. The Channel Profile artifact

This is the canonical output of every ingestion. It's a single structured document that every downstream skill consumes.

### The profile structure

```
Channel Profile — [channel name]
Ingested at: [timestamp]
Ingestion depth: [shallow / medium / medium-deep / deep]
Sample size: [N videos analyzed]
Source confidence: [high / medium / low — based on sample size + outlier diversity]

—— SECTION 1: META ——
Channel name, subscriber count, total uploads, niche tags, upload cadence, content type (long-form / shorts / mixed), production sophistication estimate

—— SECTION 2: VOICE DNA ——
(Feeds Skill 03)
- Archetype match: [V1-V15 from Skill 03]
- Pace: [slow / mid / fast]
- Sentence rhythm: [long unfolding / short staccato / mixed varied]
- Vocabulary level: [grade 8 / grade 12 / specialist]
- Emotional register: [list]
- Signature transitions: [3-5 actual phrases extracted]
- Signature openers: [pattern + example]
- Taboo words: [things this channel never says]
- Recurring references: [domains, names, frameworks the voice pulls from]
- Audience relationship: [authority / peer / confidant / preacher]
- Humor register: [none / dry / warm / chaotic / dark]

—— SECTION 3: TITLE DNA ——
(Feeds Skill 01)
- Median character count
- Top 3 formulas (F1-F13 from Skill 01) by frequency
- ALL CAPS frequency
- Em-dash frequency
- Power words (verbs, threat words, named entities) that recur
- Common villains / authority figures named
- Number-formula preference (specific dollar / numbered list / counter-narrative)

—— SECTION 4: THUMBNAIL DNA ——
(Feeds Skill 02)
- Dominant pattern (P1-P12 from Skill 02)
- Anchor color (hex)
- Background tone (dark / light / duotone)
- Face presence (always / sometimes / never)
- Face position + emotional register
- Text rules (avg word count, ALL CAPS frequency, position)
- Recurring props
- Composition rule (rule of thirds / center / split)
- Visual treatment (color grade, filter, signature look)

—— SECTION 5: SCRIPT STRUCTURE DNA ——
(Feeds Skill 03)
- Hook architecture (H1-H4 from Skill 03)
- Average video length
- Beat sequence: do videos have a 12-beat structure, a 5-act, a different shape
- Pattern interrupts cadence (e.g., new beat every 90s)
- CTA placement (cold open, mid, end)

—— SECTION 6: DESCRIPTION DNA ——
(Feeds Skill 04)
- First-125 pattern (specific number / time anchor / named villain)
- Chapter cadence (one every X minutes)
- Lead magnet style (does the channel use one)
- Affiliate density (how many per video)
- Hashtag pattern

—— SECTION 7: VISUAL STYLE DNA ——
(Feeds future visual skills — image-to-video, storyboard)
- Aspect ratio (16:9, 9:16, mixed)
- Color grade (warm sepia / cool cinematic / clinical / saturated)
- Lighting (soft / hard / mixed / available)
- Editing pace (cuts per minute)
- Signature visual treatments (filters, graphics overlays, animation conventions)
- Use of stock footage / b-roll / AI-generated content

—— SECTION 8: PACING DNA ——
(Feeds Skill 03 + future editing skills)
- Cuts per minute (median)
- Music density (continuous BGM / sparse / none)
- Silence permitted (yes / no — some channels never have silence)
- Transition style (hard cut / fade / wipe / signature)

—— SECTION 9: CTA DNA ——
- Lead magnet present (yes / no)
- Affiliate strategy
- Subscribe ask placement (start / end / both)
- Share prompt placement
- End screen style

—— SECTION 10: BRAND IDENTITY ——
- Logo / watermark
- Channel handle conventions
- Color anchor across all surfaces
- Audio signature (intro stinger, outro music)
- Recurring characters / hosts

—— SECTION 11: ANCHOR vs SWAP-ABLE CLASSIFIER ——
For every element in sections 2-10, classify as:
- ANCHOR: appears in 70%+ of analyzed videos. NEVER copy directly. Differentiate.
- TENDENCY: appears in 30-70% of analyzed videos. Use as guide, vary frequently.
- SWAP-ABLE: appears in <30%. Per-video element, freely vary.

—— SECTION 12: OUTLIER NOTES ——
What separates the channel's outlier videos (>3× channel median) from its median videos. Specific observations: which formulas, which thumbnails, which structural beats correlate with outlier performance.
```

### How downstream skills consume the profile

Every generation step pulls the relevant section:
- Title generation → loads Sections 2 (Voice), 3 (Title DNA), 12 (Outlier Notes)
- Thumbnail generation → loads Sections 4 (Thumbnail DNA), 7 (Visual Style), 10 (Brand Identity), 11 (Anchor/Swap)
- Script writing → loads Sections 2 (Voice), 5 (Script Structure), 8 (Pacing), 11 (Anchor/Swap)
- Description writing → loads Sections 3 (Title DNA, for SEO consistency), 6 (Description DNA), 9 (CTA DNA)

The profile is the unified source of truth.

## 5. The pipeline mechanics

The technical execution of ingestion. The agent runs these steps:

### Phase 1 — Channel metadata pull
- Use YouTube Data API v3 `channels.list` endpoint
- Pull: channel ID, title, description, subscriber count, total view count, upload count, country, custom URL
- Pull last 50 video IDs via `playlistItems.list` on the uploads playlist

### Phase 2 — Video metadata pull
- For each of the last ~12-20 video IDs, call `videos.list`
- Pull: title, description, tags, duration, view count, like count, comment count, thumbnail URLs (multiple resolutions), upload date

### Phase 3 — Outlier identification
- Compute the channel's median view count for the last 20 videos
- Flag any video > 3× median as outlier
- Flag any video < 0.3× median as underperformer (useful for negative-space DNA — what doesn't work for this channel)

### Phase 4 — Transcript pull
- For each of the top 5-7 videos (by view count or by being outliers), call youtube-transcript-api
- Fallback: if transcripts are disabled, run Whisper transcription on the audio extracted via yt-dlp (slower, costs compute)
- Concatenate transcripts into a single long-text corpus per video

### Phase 5 — Thumbnail vision analysis
- Pull the maxres thumbnail URL for each of 8-12 recent videos
- Send each to Gemini 3 Pro Vision (or equivalent multimodal model) with a structured analysis prompt:
  ```
  Analyze this YouTube thumbnail. Output structured JSON with:
  - dominant_pattern (face+arrow / rage-stamp / before-after / mystery-object / etc.)
  - anchor_color (hex)
  - background_tone (dark / light / duotone)
  - has_face (boolean)
  - face_emotion (if present)
  - face_position (top-left / top-right / center / etc.)
  - text_word_count
  - text_all_caps_ratio
  - text_position
  - recurring_props (list)
  - composition (rule-of-thirds / centered / split)
  - color_grade_descriptor
  ```
- Aggregate the JSON across all thumbnails to compute frequencies (anchor color frequency, face presence frequency, etc.)

### Phase 6 — Frame sampling for visual style
- For 5-6 selected videos (mix of outliers and median), use yt-dlp to download at low resolution
- Sample 4-6 frames per video at evenly distributed timestamps
- Analyze each frame via vision model for color grade, lighting, composition
- Aggregate to produce Visual Style DNA

### Phase 7 — Voice / pacing fingerprint
- From transcripts, extract:
  - Average sentence length (word count)
  - Average clause count per sentence
  - Question frequency (sentences ending in ?)
  - Fragment frequency (sentences without main verb)
  - Hedge word frequency ("might," "perhaps," "maybe")
  - Specific recurring phrases (n-gram analysis, find phrases appearing 3+ times across transcripts)
- LLM-pass: send transcript samples to Claude/GPT and ask "describe the narrator's voice in terms of pace, register, vocabulary level, emotional register, and audience relationship."
- Match to closest archetype (V1-V15 from Skill 03)

### Phase 8 — Synthesis
- Compile all extracted data into the Channel Profile structure
- Run the Anchor vs Swap-able classifier (Section 11)
- LLM final-pass: feed the structured profile back through Claude/GPT and ask "what's the one-sentence summary of what makes this channel distinct?" Save to top of profile.

### Phase 9 — Validation
- Profile must have minimum field completeness (all 12 sections populated)
- Sample size must be ≥ 8 videos for medium-deep
- If validation fails, escalate to deep ingestion or surface to user with caveat

### Fallbacks when YouTube blocks scraping

- **Transcripts blocked:** fall back to Whisper transcription via yt-dlp audio extraction
- **API quota exhausted:** queue ingestion for the next API window or use scraping libraries (Innertube)
- **Channel has < 10 videos:** mark profile as "low confidence — small sample"
- **Channel is age-gated or members-only:** ingestion fails gracefully, agent tells user

## 6. The synthesis layer — Anchor vs Swap-able classifier

This is the key cognitive task in synthesis. The agent must distinguish what's the channel's brand fingerprint (NEVER copy directly) from what's per-video variation (CAN copy and adapt).

### The frequency rule

For every observed element in the analyzed videos:
- **Appears in 70%+ of videos** → ANCHOR. This is the channel's brand fingerprint. The agent must NOT copy this directly when generating for the user. Instead, the agent introduces a deliberate variation.
- **Appears in 30-70% of videos** → TENDENCY. The agent uses this as a default guide but freely varies. Some user videos follow it, some don't.
- **Appears in <30% of videos** → SWAP-ABLE. Per-video element. Free to use or skip.

### Examples of anchor classification

For a long-form mystery-documentary channel (history/mystery):
- Warm sepia color grade → 95% of videos = ANCHOR (don't copy directly; user gets a related warm tone, e.g., warm amber)
- Single object hero shot in thumbnail → 80% = ANCHOR
- Use of "But here's what's interesting..." transition → 60% = TENDENCY (use sometimes)
- Mention of specific historical date in opening → 90% = ANCHOR (do this in user's videos but with the user's own dates)
- Use of dramatic music swells at reveal moments → 50% = TENDENCY

For an economic-explainer channel (finance critique):
- Yellow + dark navy thumbnail palette → 95% = ANCHOR (user gets a related but distinct palette)
- Erudite Professor voice (V8) → 100% = ANCHOR (architecture stays, specific phrasings vary)
- 8-12 minute video length → 85% = ANCHOR
- Pointing-at-chart thumbnail composition → 70% = TENDENCY (use sometimes)
- Brackets/parentheticals in titles → 40% = TENDENCY

### The differentiation principle

When generating for the user using a reference's profile, the agent applies the **Anchor Differentiation Rule**:

For every ANCHOR element, the agent introduces ONE deliberate variation that signals "we share DNA but I am my own brand." Examples:
- Reference's anchor color is warm sepia → user gets warm amber-orange (10-30 degrees on color wheel)
- Reference's voice is Documentary Authority slow-deliberate → user gets Documentary Authority mid-pace (slightly faster)
- Reference's thumbnail composition is face-on-left → user gets face-on-right (mirror flip)
- Reference's signature transition is "But here's what's interesting" → user gets "And here's the part that..."

The result: the user's channel feels like the reference's neighborhood without being the reference's clone.

For every TENDENCY element, the agent uses freely without modification — these aren't load-bearing for brand recognition.

For every SWAP-ABLE element, the agent generates per-video without reference constraint.

## 7. Multi-reference blending — hierarchy not merge

When the user names two references ("make it like the long-form mystery-documentary channel meets the economic-explainer channel"), naive averaging produces mush. Two strong voices averaged become one weak voice.

The agent uses **hierarchy blending**:

### The primary + accent pattern

- **Primary reference** = the structural and voice foundation. Drives Voice DNA, Script Structure DNA, Pacing DNA. Roughly 70% of the user's profile is inherited from the primary.
- **Accent reference** = the surface differentiator. Drives some Title DNA and Thumbnail DNA elements. Roughly 30% of the profile is inherited from the accent.

When the user names two without specifying, the agent asks: "Which is the foundation and which is the accent?" The primary should match the user's niche more closely; the accent provides the differentiation.

### Worked example

User says: "I want my history channel to feel like the long-form mystery-documentary channel but with the title energy of the economic-explainer channel."

- Primary = the long-form mystery-documentary channel. Inherits Voice DNA (V1 Documentary Authority), Visual Style DNA (warm sepia), Script Structure DNA (mystery-led pacing), Pacing DNA (slow-deliberate cuts), Brand Identity (single-object thumbnails).
- Accent = the economic-explainer channel. Inherits Title DNA (em-dash + numbered list dominance, ALL CAPS for emphasis words), some Thumbnail DNA (yellow accent on otherwise sepia palette).

Result: a history channel with the primary reference's calm authority voice and visual aesthetic, but titles that punch like the accent reference ("The 1346 Map That Hid Something — Here's What Historians Just Found").

### When user names three or more references

Cap at three. Beyond three, the agent says: "I can blend up to three references coherently. More than that produces an unfocused style. Pick your top three."

When three are named, the agent assigns: 60% primary / 30% secondary / 10% tertiary.

## 8. Differentiation rules — applying without plagiarism

Even with the Anchor Differentiation Rule (§6), the agent has additional safeguards.

### The phrase blacklist

When ingesting transcripts, the agent extracts **exact phrases that appear 5+ times across the reference's videos**. These are the channel's signature phrases. The agent adds them to a phrase blacklist for the user's channel. The user's scripts must NEVER contain these exact phrases.

Example for a long-form mystery-documentary channel: phrases like "And so the question becomes..." that appear repeatedly are blacklisted. The user's scripts use the same rhythm with different phrasing.

### The composition mirror rule

For thumbnails: when the reference's anchor composition is face-on-left, the user's anchor composition becomes face-on-right (or vice versa). This single mirroring move differentiates surprisingly well visually while keeping all other anchor elements intact.

### The color shift rule

For visual elements with an anchor color: shift 15-30 degrees on the color wheel. Stays in the same emotional register but reads as a distinct brand. Reference sepia (~30° hue) → user's amber-orange (~50° hue).

### The sample size rule

If the reference has fewer than 10 videos analyzed, the profile's confidence is "low" and the agent warns the user: "Limited sample. The profile is a best estimate. Expect to refine over your first 5-10 videos."

### The pure-copy escape valve

If the user explicitly says "no, I want it EXACTLY like X, copy them directly" — the agent pushes back: "Doing that breaks both YouTube's inauthentic content policy and your long-term channel growth. The audience that retains is the audience that perceives originality. I'll get you 90% of the feel without copying directly. Trust me on this." If the user insists, the agent applies looser differentiation but logs the user's preference.

## 9. Niche-specific ingestion notes

Most niches ingest the same way, but a few have specific considerations.

### Faceless channels (history, ambient, drill, music videos, AI-narration)
- No face position to extract from thumbnails
- Voice fingerprint is the dominant DNA element
- Visual style profile carries more weight than thumbnail face data
- Recurring object/character (a deadpan-history channel's stick figure, a science-animation channel's mascot birds) is a critical anchor

### Vertical / shorts channels
- Aspect ratio is 9:16, not 16:9 — visual style sampling needs to handle this
- Video lengths are 60-90 seconds — fewer beats to extract structurally
- Thumbnail data is less load-bearing (vertical thumbnails get less scrutiny)
- Voice/pacing dominates

### Music video / drill / propaganda channels
- Transcripts are lyrics, not narration — extract bar structure, hook repetition, BPM
- Visual style is the dominant DNA element
- Voice DNA matches V14 Drill Rapper Narrator if applicable

### Long-form interview / podcast channels
- Multiple voices present — agent must identify the host's voice specifically
- Thumbnails are heavily face-driven (host + guest)
- Pacing DNA includes editing decisions (jump cuts vs no cuts)

### News-hijack channels (investigative-journalism, geopolitics documentary)
- Outlier videos correlate strongly with named villain inclusion — note this in Section 12
- Investigation structural beats are highly consistent → strong Anchor
- Visual style includes document/screenshot overlays — extract this

### Health / medical authority channels
- YMYL niche — agent flags compliance considerations in profile
- Disclaimer language is part of the brand DNA
- Citations / source attribution patterns matter

## 10. Anti-patterns

When ingestion goes wrong:

**A1 — Small sample size mismatch.** Channel has 8 videos. Agent ingests anyway and produces a high-confidence profile. Result: profile fits this batch but won't generalize. Fix: agent must mark profile as low-confidence and refine across the user's first 10 videos.

**A2 — Niche mismatch transfer.** User says "make it like an investigative-journalism channel" but the user's niche is yoga/wellness. The investigation register clashes with wellness audience expectations. Fix: agent flags the mismatch and suggests a wellness-niche reference instead, or suggests blending the investigation structure with a wellness-niche voice.

**A3 — Outlier-only sampling.** Agent only ingests outlier videos. Profile reflects what works for THAT outlier topic but not the channel's median DNA. Fix: sample mix of outlier and median videos.

**A4 — Translation loss.** Reference channel is Korean / Spanish / Portuguese. Transcripts come back in source language. Agent translates and loses voice nuance. Fix: agent flags non-English ingestion and tells user "language transfer reduces voice fidelity."

**A5 — Faceless ingestion applied to face channel.** User says "make it like a long-form mystery-documentary channel" but user has on-camera presence. The faceless DNA doesn't transfer. Fix: agent flags the structural mismatch and proposes a face-channel reference with similar voice register (a science-explainer channel, or a tech-industrial-history channel with some narrator presence).

**A6 — Recent-only sampling on a channel that pivoted.** Channel had a different style 6 months ago. Agent samples last 12 videos and gets the new style only. Sometimes that's correct, sometimes the user wanted the old style. Fix: agent confirms with user "this channel pivoted around [date] — should I sample the new era or the old?"

**A7 — Plagiarism temptation.** Profile is so detailed the agent could just copy phrases verbatim. Anchor Differentiation Rule + phrase blacklist must be enforced. Fix: agent runs a final "originality check" — generated text must score < 0.85 cosine similarity against any reference transcript chunk.

**A8 — Profile decay.** Reference channel ingested 6 months ago. Their DNA has shifted since. The agent's user is generating from stale DNA. Fix: profiles older than 90 days are flagged for re-ingestion, and the agent prompts the user to refresh.

## 11. The generation workflow

When the user says "make it like X":

**Step 1 — Channel resolution.** Confirm the channel URL. Pull metadata (Phase 1).

**Step 2 — Sample selection.** Identify last 10-12 videos + 2-3 outliers. Mix.

**Step 3 — Ingestion depth selection.** Default medium-deep. If user is on free tier, propose medium with option to upgrade. If user requests deep, confirm cost.

**Step 4 — Phases 1-7 of pipeline (§5).** Run sequentially or parallel where possible.

**Step 5 — Synthesis (§4).** Compile structured profile. Apply anchor classifier.

**Step 6 — Validation.** Check field completeness, sample size confidence.

**Step 7 — Surface profile to user.** "Here's the profile I extracted from [Reference]. Anything off, or want me to dig deeper?" The user can adjust fields before lock-in.

**Step 8 — Lock to channel memory.** Profile becomes the canonical reference for all downstream skills on this user's channel.

**Step 9 — Generate first content using profile.** Title, thumbnail, script, description all reference the profile.

**Step 10 — Refine over time.** As the user gives feedback ("more deadpan," "less corporate"), the profile updates. After 5-10 user videos, the profile is half-original-reference + half-user-emergent.

## 12. Worked examples

### Example 1 — Ingesting a long-form mystery-documentary channel, applying to a science channel

**User:** "I want a science channel that feels like a long-form mystery-documentary channel."

**Ingestion:**
- 11 videos sampled (median + 3 outliers)
- 7 transcripts pulled
- 9 thumbnails analyzed
- 4 videos frame-sampled

**Resulting Channel Profile (key fields):**

```
VOICE DNA
- Archetype: V1 Documentary Authority (slow-deliberate variant)
- Pace: slow-deliberate, 145 wpm avg
- Sentence rhythm: long unfolding, avg 22 words, multi-clause
- Vocabulary level: educated specialist with translation moments
- Signature transitions: "And so..." (8 instances) / "But what's interesting..." (5) / "Years later..." (4)
- Audience relationship: equal-intelligent peer

VISUAL STYLE DNA
- Color grade: warm sepia (hue ~30°, saturation moderate)
- Lighting: cinematic moody, single key light
- Composition: rule of thirds with single dominant object
- Editing: slow cuts, 18 cuts/min avg

THUMBNAIL DNA
- Pattern P4 Mystery Object dominant (8/9)
- Anchor color: warm amber/sepia
- Face presence: 0% (faceless)
- Recurring props: maps, vintage photos, single-object hero shots
- Text: minimal, when present 2-3 words max

ANCHOR vs SWAP-ABLE
ANCHORS (do NOT copy directly, differentiate):
- Warm sepia anchor color → user gets warm amber-orange variant
- Single object thumbnail → user uses single object but mirrored composition
- "And so..." transition → user uses "And then..." or "What followed was..."
- Long-unfolding sentence rhythm → user uses same rhythm

TENDENCIES (use freely):
- Date-led openers
- Mystery-gap chapter titles
```

**Generation outcome for the user's first science video:**
- Title: "The 1972 Experiment That Predicted Quantum Computing — But Nobody Listened" (mystery gap + date anchor, reference-derived rhythm)
- Thumbnail: warm amber palette (anchor differentiated from sepia), single object (vintage lab equipment) mirrored to right side of frame, faceless, minimal text
- Script: V1 Documentary Authority voice, slow pace, long sentences, opens with date/place/specific detail. Phrase blacklist excludes the reference's specific signature phrases.

User's channel feels like the reference's neighborhood without being a clone.

### Example 2 — Ingesting an economic-explainer channel, applying to a personal-finance authority channel

**User:** "I want title energy and visual punch like the economic-explainer channel."

**Note:** This is an accent reference, not primary. The user's channel already has a locked Voice DNA (V2 Federal Credentialed Expert). So the agent ingests the economic-explainer channel only for Title DNA + Thumbnail DNA contributions.

**Resulting profile delta** (only the relevant sections):

```
TITLE DNA (from accent reference)
- Top formulas: F11 Bracketed Tag (40%), F2 Specific Dollar Number (30%), F12 ALL CAPS Authority (20%)
- Em-dash frequency: 65%
- Power words: "Genius Strategy," "Honest Truth," "Real Reason"
- Bracket/parenthetical pattern: 40% of titles end with a bracketed modifier

THUMBNAIL DNA (from accent reference)
- Anchor color: yellow on dark navy (use 30% as accent, blend with the user's existing red)
- Face position: top-left, accusation register
- Composition: rule of thirds, arrow pointing right at object
```

**Generation outcome:**
- The user's title rhythm picks up the economic-explainer channel's bracketed-tag pattern → "The Account Clause Nobody Reads (And How To Cancel It)"
- Thumbnail introduces yellow as a secondary accent color in some videos, alongside the user's red anchor
- Voice and structure remain locked to the user's original V2 Federal Credentialed Expert

### Example 3 — Ingesting an investigative-journalism channel for a news-hijack channel

**User:** "I want a scam-exposure channel like an investigative-journalism channel."

**Ingestion:**
- 10 videos sampled (3 outliers, 7 median)
- 6 transcripts pulled
- 8 thumbnails analyzed
- Document/screenshot overlay frequency noted

**Resulting Channel Profile (key fields):**

```
VOICE DNA
- Archetype: V4 Skeptical Investigator
- Pace: fast-mid, building momentum
- Signature transitions: "But something didn't add up..." (6) / "So I started digging..." (4)
- Audience relationship: detective + assistant
- Tools: documents, screenshots, named individuals

THUMBNAIL DNA
- Pattern P2 Rage Stamp dominant
- Anchor color: red on dark
- Face position: top-left, accusation/intensity
- Recurring props: scam screenshots, dollar amounts, named villains

SCRIPT STRUCTURE DNA
- Opening: named villain + specific dollar amount + claim
- Investigation arc: setup → evidence → reveal → implication
- Average length: 18-25 minutes
- Document overlays: 60% of videos use screenshot evidence

OUTLIER NOTES
- Outlier videos correlate with named villain in title (95%) vs median 40%
- Outlier videos average 22% longer than median (more evidence depth)
```

**Generation outcome for user's first scam-exposure video:**
- Title: F5 Named Villain + F2 Specific Dollar — "How [Channel] Stole $4 Million From Their Audience"
- Thumbnail: red anchor (differentiated 15° from the reference by hue shift), accusation face position mirrored to right, scam screenshot prop
- Script: V4 Skeptical Investigator voice, opens with the specific anomaly, builds case with document overlays, reveal at 70%
- Channel memory locked: this user's channel will use document-overlay structure every video going forward

## 13. Runtime checklist

Before any Channel Profile lands in channel memory:

- [ ] Channel resolution successful (URL valid, channel public)
- [ ] Sample size ≥ 8 videos for medium-deep, ≥ 12 for deep
- [ ] All 12 profile sections populated
- [ ] Anchor vs Swap-able classifier run with 70/30 thresholds
- [ ] Phrase blacklist generated from 5+ recurring exact phrases
- [ ] Color shift / composition mirror rules locked in
- [ ] Validation passed (no missing critical fields)
- [ ] Confidence rating computed (high / medium / low)
- [ ] User confirmed profile or made edits
- [ ] Profile timestamp logged for future re-ingestion check (90-day window)

If validation fails, escalate to deep ingestion or surface to user with caveats.

---

## Update log

This skill is current as of April 2026. Update when:
- YouTube changes its API quotas or transcript availability
- New vision models offer materially better thumbnail analysis
- Frontier LLMs unlock better voice fingerprint extraction
- New niche emerges with distinct ingestion considerations
- Profile decay heuristics need tuning (currently 90 days)

Profile artifacts older than 90 days should be flagged for re-ingestion. The reference channel may have evolved.

## Connection to other skills

This skill is the meta-skill. Every other skill has a "competitor mining" section that references this one:
- **Skill 01 — Title Creation §7** consumes Section 3 (Title DNA) of the profile
- **Skill 02 — Thumbnail Design §9** consumes Section 4 (Thumbnail DNA)
- **Skill 03 — Script Writing §10** consumes Section 2 (Voice DNA) and Section 5 (Script Structure DNA)
- **Skill 04 — Description Writing §9** consumes Section 6 (Description DNA)

When this skill is updated, downstream skills should be re-validated for consumption pattern compatibility.
