---
name: thumbnail-design
description: >-
  Designs the thumbnail that wins the click. Composition patterns + image-gen prompts + mobile readability. Load when designing or iterating a thumbnail. Companion: composition-bank.md (pixel-level specs for 12 patterns + 6 anti-patterns).
---

# Skill 02 — Thumbnail Design

This is the operational knowledge an AI YouTube agent needs to design a perfect thumbnail for any video in any niche, AND to systematically study a competitor's thumbnail style and modify it for the user's brand. Every paragraph is a concrete rule the agent applies.

---

## 1. The job of a thumbnail

A thumbnail has four jobs in a single 0.6-second decision:

1. **Win the click** against ~12 competing thumbnails on a mobile feed
2. **Communicate the satisfaction promise** the title sets up — coherence with the title is mandatory
3. **Pre-screen the right viewer** — attract people who will retain, not the broadest possible click
4. **Stay on brand** — repeated viewers should recognize the channel's visual fingerprint within 0.3 seconds

The agent never optimizes for click rate alone. A 14% CTR thumbnail with 28% retention loses to an 8% CTR thumbnail with 62% retention. YouTube's 2025-2026 algorithm weights **watch-time-per-impression** over raw CTR by approximately 4:1.

The thumbnail and the title are a single unit. Designing them in isolation produces incoherent pairs that confuse viewers and underperform. The agent always co-designs the pair, even though title creation runs as Skill 01 and thumbnail runs as Skill 02 — they share inputs.

Render reality: thumbnails appear on YouTube at sizes ranging from 168×94 (mobile suggested sidebar) up to 1280×720 (full desktop home page). The most common single render size in 2025-2026 is 320×180 — that's where the click decision usually happens. The agent designs every thumbnail to be readable at 320×180.

## 2. The optimization target

The agent optimizes thumbnails against **predicted watch-time-per-impression**, identical to title strategy. The 2025-2026 algorithm scores both elements together because they are processed together in the viewer's brain.

Predicted watch-time-per-impression for a candidate thumbnail:
- **CTR potential** — does the thumbnail beat the niche median click-rate?
- **Title coherence** — does the thumbnail visually confirm what the title promises?
- **Audience pre-screening** — does the visual register attract retainers, not bouncers?
- **Brand recognition** — does it match the channel's locked visual DNA?

Quote from Todd Beaupre, YouTube's Senior Director of Growth & Discovery: *"The thumbnail is the single biggest predictor of click-through, but click-through alone is not what we optimize for. We weight CTR by the watch-time it produces."*

Quote from a top viral-challenge creator: *"I'll spend $10,000 and a full day on a thumbnail because if the thumbnail is 5% better, the video is 5% better forever. It compounds."* They pay multiple thumbnail artists to compete on every upload, run A/B tests, and revise 8+ times per video.

The agent operates with this discipline as default. Every thumbnail goes through 4-6 variants, mobile readability validation, A/B test setup. No thumbnail ships on the first generation.

## 3. The 12 canonical thumbnail patterns

Every winning thumbnail in 2025-2026 fits one of these 12 patterns, or combines two. The agent generates 4-6 variants per video using deliberate spread across patterns, then ranks by predicted watch-time-per-impression.

### P1 — Face + Arrow + Number

**Structure:** Large face on one side (rule of thirds, eye line at upper-third). Arrow or circle pointing at a key element. Bold number in opposite corner. Bright color anchor.

**Examples:** Top viral-challenge creators, economic-explainer channels, host-led real-estate creators, finance personality channels
**Color palette:** High-contrast — red/yellow on dark background, or white/black with single accent
**When to use:** Finance, lifestyle, vlog, entertainment with personality. Any niche where a host or character is the brand.
**When NOT to use:** Faceless channels, history, ambient, music videos.
**Why it works:** Face provides instant emotional read (~120ms processing time). Arrow forces the eye to the key element. Number adds specificity. Color anchor makes the thumb stand out in feed.
**Expected lift:** Face thumbnails outperform faceless by 25-30% CTR average across most niches (vidIQ, 2024).

### P2 — Rage Stamp

**Structure:** Single dominant text element in red/yellow, often diagonally tilted. Background of either a face in shock OR a relevant object. Stamp visual treatment (texture, distressed edges).

**Examples:** Personal-finance authority IRS, investigative-journalism channels, news-explainer outlets, scam-exposure niche
**Color palette:** Red anchor, white text with thick black stroke, dark background
**When to use:** Senior finance, scam exposure, IRS/government overreach, news-hijack
**When NOT to use:** Tech, science, history, faith, wellness — reads inflammatory
**Why it works:** Rage register pre-screens the audience that engages most in finance/political/news content. The stamp visual shorthand reads "alert" without requiring text reading.
**Per personal-finance authority performance data:** Rage-stamp thumbnails consistently produced 11-14% CTR vs the senior finance niche median of 6-8%.

### P3 — Before/After Split

**Structure:** Vertical or horizontal split. Left side = before/bad/old. Right side = after/good/new. Often with arrow between, or red X over before / green check over after.

**Examples:** Real estate (HGTV-style), fitness transformations, tech upgrades, makeover content
**Color palette:** Desaturated/red for before, vibrant/green for after
**When to use:** Transformation content where the comparison IS the value proposition
**When NOT to use:** Most narrative or educational content
**Why it works:** Visualizes outcome, not effort. Buyer brain pre-evaluates the payoff.

### P4 — Mystery Object

**Structure:** Single object dominantly framed, often with selective lighting. Title carries the mystery; thumbnail carries the visual hook. Minimal or no text on the thumbnail itself.

**Examples:** Long-form mystery-documentary, science-explainer (some), tech-industrial-history, science / history channels
**Color palette:** Cinematic — dark moody background, single object lit with warm or cool key light
**When to use:** Documentary-style, history, science, mystery niches where atmosphere is the brand
**When NOT to use:** Hype/entertainment niches that require louder thumbnails
**Why it works:** Underplays the click. The audience that clicks Mystery Object thumbs is the audience that watches all the way through. CTR is moderate but watch-time-per-impression is high.

### P5 — Reaction Face

**Structure:** Face only (or face dominant), expression at peak emotional moment. Often direct eye contact. Sometimes paired with single bold word.

**Examples:** Reaction channels, vlog channels, gaming, comedy
**Color palette:** Bright, lit-from-front, often vibrant background or single-color backdrop
**When to use:** When the host's reaction is the product (reaction videos, vlog), or when emotional surprise is the hook
**When NOT to use:** Authority content (finance, science) — reads silly
**Why it works:** Mirror neurons. The viewer pre-feels the reaction before clicking.

### P6 — Authority Portrait

**Structure:** Subject portrait in calm, professional pose. Direct eye contact or three-quarter view. Office/library/studio backdrop. Subtle text overlay if any.

**Examples:** Science-explainer, top tech-review channels, long-form interview/podcast hosts, long-form science-podcast hosts
**Color palette:** Muted, professional — navy/charcoal/cream
**When to use:** Tech, science, long-form interview, education, authority-driven niches
**When NOT to use:** Hype/entertainment niches
**Why it works:** Underpromises and overdelivers. The audience that clicks Authority Portrait expects substance and stays through it. Watch-time-per-impression is highest in this pattern when the niche fits.

### P7 — Stacked Number Pile

**Structure:** Multiple objects/units stacked or grouped. The "I bought 10,000 of X" or "These 50 things" visual.

**Examples:** Top viral-challenge creators (the $1 vs $1M videos), product comparison channels, finance "I tested every X"
**Color palette:** Bright, often with the host pointing at the stack
**When to use:** Specific scale claims, product comparisons, abundance/quantity stories
**When NOT to use:** Narrative or educational content
**Why it works:** Quantity is shorthand for value. The brain processes "lots" before reading the count.

### P8 — Negative-Space Word

**Structure:** Single dominant word filling 60%+ of the canvas. Background is solid color or simple gradient. No face, no objects.

**Examples:** Some news-explainer outlet episodes, science-animation rare uses, minimalist tech reviewers
**Color palette:** Bold, often two-color (text + background) maximum
**When to use:** Punchy single-word claims (REVEALED, BANNED, EXPOSED, $0)
**When NOT to use:** Most content — too minimal for crowded feeds
**Why it works:** Stops the scroll because it doesn't look like other thumbnails. High variance — when it works, it dominates; when it fails, it disappears.

### P9 — Split Frame

**Structure:** Two-panel composition. Different content in each. Often a comparison or before/relationship visual. Distinct from P3 Before/After in that it's not a transformation — it's two separate things being contrasted.

**Examples:** "Mac vs PC" style tech, "her vs him" relationship content, comparison reviews
**Color palette:** Often half-warm/half-cool to emphasize the split
**When to use:** Comparison content
**When NOT to use:** Single-narrative content

### P10 — Color-Pop on Mono Background

**Structure:** One dominant color element (object or person) on a monochrome or desaturated background. Forces the eye to the focal point.

**Examples:** Documentary channels (tech-industrial-history channels use this), product reviews, fashion
**Color palette:** Single accent color popping out of grey/black/sepia background
**When to use:** When you have one specific visual claim and want it isolated
**When NOT to use:** When the video is genuinely about multiple things
**Why it works:** Visual hierarchy. The eye lands on the colored element instantly.

### P11 — Photoreal Composite

**Structure:** Multiple photoreal elements composited together — usually impossible scenes, dramatic poses, dramatic environments.

**Examples:** History channels, sci-fi-adjacent content, dramatic news-hijack
**Color palette:** Cinematic, often graded for emotional register (cool blues = somber, warm oranges = action)
**When to use:** When the topic supports a dramatic visual fiction
**When NOT to use:** When the audience expects literal honesty (tutorials, reviews)
**Why it works:** Creates a scene the viewer wants to enter.

### P12 — AI-Generated Surreal

**Structure:** Highly stylized AI image — unusual perspectives, impossible compositions, dreamlike rendering.

**Examples:** AI-narration channels, music video aesthetics, brick-narrative storytelling style
**Color palette:** Saturated, often graded with strong filmic look
**When to use:** AI-narrated channels where the aesthetic is part of the brand. Music videos where surreal IS the genre.
**When NOT to use:** Authority content. AI-generated thumbnails on finance/health channels read as low-effort and trigger July 2025 inauthentic content concerns. **Be careful: post-Jan 2026 YouTube enforcement.**
**Why it works:** Stops the scroll because nothing in the feed looks like it. But high satisfaction risk if the video doesn't deliver the surreal vibe.

## 4. Color theory

Color choices win or lose thumbnails before text is read. The agent applies these rules:

**The contrast rule:** Foreground must have ≥4.5:1 contrast ratio with background. Below that, the thumbnail disappears at mobile size. This is non-negotiable.

**The red+yellow data:** Bright red (RGB 220-240, 0-30, 0-30) outperforms blue by ~23% CTR (TubeBuddy, January 2025). Yellow (255, 200-230, 0) is the second-highest performer. Red+yellow combinations dominate finance, news, scam-exposure niches.

**Brand-color anchoring:** After 5+ videos, every channel should have ONE consistent anchor color. A personal-finance authority channel uses red rage stamps. A long-form mystery-documentary channel uses warm sepia. A science-animation channel uses signature blue. The anchor color is the channel's visual fingerprint and shouldn't change without explicit brand reset.

**Dark vs light backgrounds:** 60-70% of high-CTR thumbnails in 2025-2026 use dark backgrounds. Reasons: the YouTube feed itself is dark mode for ~70% of users now (2024 data), and bright text on dark reads more "designed" than dark text on bright.

**The 7 winning palettes** (validated across 2025-2026 outliers):
1. **Red anchor + black background + white text** — finance authority, scam exposure (personal-finance authority, investigative-journalism)
2. **Yellow anchor + dark navy + red accent** — news hijack, urgency (economic-explainer channels)
3. **Cinematic warm + sepia tones** — history, documentary (long-form mystery-documentary, tech-industrial-history)
4. **Saturated multi-color + bright background** — entertainment, gaming (top viral-challenge creators, gaming)
5. **Muted professional palette** — tech, science (top tech-review, science-explainer)
6. **Mono + single color pop** — minimalist, documentary (tech-industrial-history alternates)
7. **Surreal AI-graded** — music video, AI-narration (brick-narrative storytelling)

**Avoid:**
- Three or more bright colors competing
- Gradient backgrounds (except in narrow stylistic niches)
- Pastel anything (reads weak in feed)
- Pure white backgrounds (disappear in dark-mode feeds)

## 5. Text rules

Text on thumbnails is governed by 6 hard rules. Violation immediately drops mobile readability.

**Rule 1 — 3 to 5 words maximum.** A top viral-challenge creator's stated rule. Confirmed by every top creator. More than 5 words and mobile users skip.

**Rule 2 — Font weight 800 or higher.** Anything less reads thin and disappears. The weight should be Black or ExtraBlack if available. Common winners: Inter Black, Anton, Bebas Neue, Impact (cliché but works).

**Rule 3 — Stroke 4-8px outline.** Around every letter. Black stroke around white/yellow text is the dominant pattern. Stroke is what makes mobile-size text legible at 320×180.

**Rule 4 — ALL CAPS for emphasis words only.** No more than 50% of total thumbnail text in caps. Beyond that, the eye can't pick out the emphasis. Words that benefit from caps: numbers (always), action verbs (BANNED, EXPOSED, REVEALED, GONE), threat words (WARNING, URGENT).

**Rule 5 — One dominant text element.** Even if the design has secondary text, only one should be optically dominant. Multiple equally-weighted text elements compete and lose.

**Rule 6 — Color combinations that pass mobile readability** (high-contrast pairs):
- Yellow text + black stroke + dark red/black background
- White text + black stroke + bright red/yellow background
- Black text + white stroke + bright yellow background
- Cyan text + dark stroke + dark background (rare, niche)

**Avoid:**
- Red text on yellow (vibrating; eye fatigue)
- Light grey on white (invisible at mobile size)
- Drop shadows in lieu of strokes (drop shadows blur at small render sizes; strokes hold edges)

## 6. Face and emotion

Faces in thumbnails are processed by the brain in ~120ms — faster than any other visual element. The agent uses faces deliberately, with a critical 2024-2025 doctrine update.

### The open-mouth → closed-mouth shift

Through 2023, the dominant high-CTR face pattern was extreme open-mouth shock — eyes wide, mouth fully open, exaggerated. A top viral-challenge creator's quote (2022): *"You can't just smile. You have to look like you've seen God or seen a ghost."*

In late 2024 and through 2025, the highest-CTR face thumbnails shifted to **closed-mouth intensity** — narrowed eyes, set jaw, controlled emotion. Same authority, less cartoonish. The top viral-challenge creator's own thumbnails made this shift in mid-2024. Quote (mid-2024 podcast): *"The shock face was getting cliché. The audience reads through it now. Closed-mouth intensity beats open-mouth shock by like 15-20% on watch time."*

The agent's default in 2025-2026: **closed-mouth intensity** for authority and finance niches; open-mouth shock still works for entertainment, gaming, vlog, comedy.

### The 7 winning emotions

| Emotion | Visual cue | Best for |
|---|---|---|
| **Shock** | Open mouth, wide eyes | Entertainment, gaming, comedy, vlog |
| **Accusation** | Pointing finger, narrowed eyes, intense gaze | Finance authority, scam exposure, news-hijack (personal-finance authority rage stamp pairs with this) |
| **Curiosity** | Slight head tilt, raised eyebrow | History, science, documentary |
| **Authority** | Direct eye contact, neutral or slight frown | Finance, tech, science, education |
| **Triumph** | Slight smile, raised arms or victorious pose | Fitness, business success, sports |
| **Despair** | Hand on head, downcast eyes | Wound-first health, finance loss stories |
| **Terror** | Wide eyes, hands raised defensively | True crime, horror, danger-content |

### Face placement

**Rule of thirds applies.** The face should fill approximately 30-40% of the canvas, positioned on the left or right third — not centered. Centered faces feel symmetric and read corporate.

**Eye line at upper third.** The eyes should fall on or near the upper-third horizontal line. Eyes too high: face feels cropped. Eyes too low: viewer reads downcast/sad regardless of expression.

**Direct eye contact wins for authority and accusation niches.** 65% of 18-34 viewers look at eyes before reading any text on the thumbnail (vidIQ eye-tracking, 2024). Three-quarter views work for casual/lifestyle.

**Eye-tracking heat map data:** 40% of attention lands in the top-left quadrant of a thumbnail (where the eye scans first in left-to-right reading order). The agent places either the dominant face or the dominant text element in the top-left for maximum first-glance read.

### Faceless thumbnail substitutes

When the channel is faceless (long-form mystery-documentary, deadpan-history, history docs, AI-narration):
- Use a **single dominant object** (Mystery Object pattern)
- Use **text-as-face** — let the dominant text carry the emotional weight
- Use **scene atmosphere** — cinematic lighting on a relevant object/environment
- Use **animated character** consistently — a deadpan-history channel's stick figure, a science-animation channel's birds, an explainer channel's icons

The faceless channel that wins despite no face does so via consistent visual identity. The trade: lower average CTR, higher brand recognition over time.

## 7. Mobile readability protocol

Every thumbnail passes 5 tests before publishing:

**Test 1 — The Glance Test.** Render the thumbnail at 320×180. Look at it for 0.5 seconds. Can you identify the topic?

**Test 2 — The Phone-Side Test.** Hold a phone arm's-length away. Display the thumbnail in YouTube's actual feed if possible (or in a mock feed). Does the dominant element resolve?

**Test 3 — The Squint Test.** Squint your eyes until the thumbnail is partially blurred. The composition should still read — the face/object/text shape should still be identifiable in silhouette. If the squint test fails, contrast is insufficient.

**Test 4 — The Competitor Sandwich Test.** Place the thumbnail in a row with the channel's top 3 competitors' current thumbnails. Does it stand out, fit in, or get lost? You want it to stand out while feeling native to the niche.

**Test 5 — The 20-Foot / 40% Brightness Test.** Chucky Appleby (thumbnail consultant for top channels) protocol: walk 20 feet away from the screen, dim brightness to 40%. The dominant element must still resolve. If it fails, the design is too detailed or the contrast is too low.

If any test fails, regenerate. Never ship a thumbnail that fails 2+ tests.

## 8. Niche-specific thumbnail playbooks

The agent loads the relevant playbook based on declared niche:

### Senior finance / IRS / retirement (personal-finance authority channel)
- **Top patterns:** P2 Rage Stamp, P1 Face+Arrow+Number, P10 Color-Pop on Mono
- **Palette:** Red anchor, white text, black stroke, dark background. NEVER light backgrounds.
- **Face:** Authority Portrait (closed-mouth intensity) or accusation pose. Host face top-left.
- **Text:** 3-5 words ALL CAPS, dollar amounts highlighted ("$160,000 GONE", "MUCH WORSE")
- **Examples:** "The 10,000 Dollar Bank Rule Just Got MUCH WORSE" → red rage stamp + host pointing
- **Avoid:** Smiles, pastels, complex compositions

### Tech / AI / dev tools (top tech-review and developer channels)
- **Top patterns:** P6 Authority Portrait, P10 Color-Pop on Mono, P4 Mystery Object
- **Palette:** Muted professional — black, charcoal, single accent (often the brand color of the product reviewed)
- **Face:** Calm, three-quarter view. Direct eye contact. No exaggerated emotion.
- **Text:** Often product name + simple modifier ("M5 Pro Tested"). 3-4 words max.
- **Examples:** Top tech reviewer's calm portrait + product hero shot pattern
- **Avoid:** ALL CAPS rage, red anchors, busy compositions

### Gaming / Roblox / Minecraft
- **Top patterns:** P5 Reaction Face, P7 Stacked Number Pile, P12 AI-Generated Surreal
- **Palette:** Saturated, bright. Vibrant backgrounds.
- **Face:** Open-mouth shock (this niche still rewards it). Or animated character.
- **Text:** Bold ALL CAPS, action words ("INSANE", "BROKEN", "LEAKED"). 3-4 words max.
- **Mobile:** Brutal truncation, vertical-feed often. Design for 240×135 thumb.
- **Examples:** "What If A Teacher OWNED Roblox?" → wide-eyed shock face + Roblox character

### Music videos / propaganda / drill (brick-narrative storytelling channel)
- **Top patterns:** P12 AI-Generated Surreal, P11 Photoreal Composite
- **Palette:** Genre-locked — drill goes dark/red, propaganda goes patriotic, sad-boy R&B goes muted
- **Face:** Often artist or character likeness. Stylized.
- **Text:** Track name + artist (sometimes ALL CAPS). Minimal beyond that.
- **Examples:** "You're Not Jesus!" → AI-generated brick-toy scene with text overlay

### News-hijack documentary (investigative-journalism / geopolitics documentary channels)
- **Top patterns:** P2 Rage Stamp, P11 Photoreal Composite, P5 Reaction Face
- **Palette:** Red, white, black. High contrast.
- **Face:** Accusation register. Pointing or intense gaze.
- **Text:** "EXPOSED", "BANNED", "BREAKING" — 2-4 words ALL CAPS
- **Examples:** Investigative-journalism channels' signature red-and-black stamp pattern

### True crime
- **Top patterns:** P4 Mystery Object, P11 Photoreal Composite, P5 Reaction Face (terror variant)
- **Palette:** Dark, moody. Selective lighting.
- **Face:** Often victim or suspect photo. Composite scene.
- **Text:** Names, dates, single-word emphasis ("KILLED", "FOUND")
- **Examples:** Most major true crime channels

### Health / medical / supplements (doctor-personality health / medical-authority)
- **Top patterns:** P1 Face+Arrow+Number, P3 Before/After Split, P10 Color-Pop on Mono
- **Palette:** Often green or yellow accent on white/cream/clinical background
- **Face:** Doctor or expert. Authority Portrait. Concerned but not panicked.
- **Text:** Disease name, supplement name, body part. ALL CAPS for the threat word ("REVERSE", "STOP")
- **Examples:** A doctor-personality channel's signature head-shot + arrow + body diagram

### Real estate / home (host-led real-estate creator)
- **Top patterns:** P1 Face+Arrow+Number, P3 Before/After Split, P7 Stacked Number Pile
- **Palette:** Bright, often with property photo as background
- **Face:** Host pointing at property. Mid-energy expression.
- **Text:** Dollar amounts ALL CAPS, property type, location ("$1M HOUSE")
- **Examples:** A host-led real-estate creator's signature pointing-at-house pattern

### Beauty / fashion / makeup
- **Top patterns:** P5 Reaction Face, P3 Before/After Split, P10 Color-Pop on Mono
- **Palette:** Bright, pastel-friendly here only — pinks, peaches, brights
- **Face:** Always a face. Often direct camera, mid-makeup or product reaction.
- **Text:** Brand names, prices, ratings ("$300 LIPSTICK")
- **Examples:** Beauty creator standard

### Cooking / food
- **Top patterns:** P4 Mystery Object (food hero shot), P5 Reaction Face, P11 Photoreal Composite
- **Palette:** Warm, food-photography golden hour. Brown, cream, red accents.
- **Face:** Optional — many faceless cooking channels work. When face is used, mid-bite or appreciation expression.
- **Text:** Dish name, time, technique ("PERFECT IN 8 MIN")
- **Examples:** Technique-led cooking channel patterns

### History / explainer (long-form mystery-documentary / deadpan-history / side-project history)
- **Top patterns:** P4 Mystery Object, P11 Photoreal Composite, P10 Color-Pop on Mono
- **Palette:** Sepia, warm vintage. Cinematic.
- **Face:** Faceless channel — none. Or stylized character (deadpan-history channel stick figure).
- **Text:** Often minimal. Year-stamps, location, single concept word.
- **Examples:** Long-form mystery-documentary channel signature warm-tone single-object thumbs

### Science (science-explainer / science-animation)
- **Top patterns:** P6 Authority Portrait (science-explainer), P12 illustrated geometric (science-animation), P4 Mystery Object
- **Palette:** Clean, often blue-anchored (science-animation) or muted navy/cream (science-explainer)
- **Face:** Science-explainer channels use the host's face authoritatively. Science-animation channels use no faces, only illustrated mascots and concepts.
- **Text:** Concept words, clean Helvetica. 3-5 words.
- **Examples:** "Why The Universe Is Mostly Empty" — a science-explainer channel's clean text + atmospheric image

### Vlog / lifestyle
- **Top patterns:** P5 Reaction Face, P1 Face+Arrow+Number, P11 Photoreal Composite
- **Palette:** Personality-driven — varies by host
- **Face:** Always face. Mid-expression. Authentic > polished.
- **Text:** Often minimal. Day count, location.
- **Examples:** Cinematic-vlog school of vlog thumbs

### Crypto / finance trading
- **Top patterns:** P2 Rage Stamp, P1 Face+Arrow+Number, P7 Stacked Number Pile
- **Palette:** Red/green money-color anchors, dark backgrounds
- **Face:** Excited or urgent expression. Often pointing at chart.
- **Text:** Coin names, price levels, percentage moves ("+400% IN 24 HOURS")
- **Examples:** Most major crypto YouTubers

### How-to / education
- **Top patterns:** P6 Authority Portrait, P10 Color-Pop on Mono, P4 Mystery Object
- **Palette:** Professional, often single accent color
- **Face:** Optional. Direct or three-quarter.
- **Text:** Tutorial keyword + tool name. 4-6 words allowed for SEO.
- **Examples:** Most tutorial channels

### Fitness / bodybuilding
- **Top patterns:** P3 Before/After Split, P1 Face+Arrow+Number, P5 Reaction Face (triumph)
- **Palette:** Bright, athletic. Often gym backdrop.
- **Face:** Full-body or upper-body shots. Triumph or intensity expression.
- **Text:** ALL CAPS workout names, body part targets ("SHREDDED ABS")
- **Examples:** Form-coaching fitness school of pointing-at-flexed-arm

### Faith / Christian / religion
- **Top patterns:** P6 Authority Portrait, P4 Mystery Object, P10 Color-Pop on Mono
- **Palette:** Warm, golden, reverent. Sometimes celestial blue.
- **Face:** Calm, contemplative. Direct or three-quarter. NEVER shock or rage.
- **Text:** Verse references, concept words. Calm fonts.
- **Examples:** Most major Christian YouTubers

### Politics / commentary
- **Top patterns:** P2 Rage Stamp, P5 Reaction Face (accusation), P11 Photoreal Composite
- **Palette:** Red/blue political anchors, depending on lean
- **Face:** Accusation or shock expressions. Politicians as villains/heroes.
- **Text:** ALL CAPS rage words, names, dollar amounts
- **Examples:** Most political commentary channels

### Travel
- **Top patterns:** P11 Photoreal Composite (location hero shot), P5 Reaction Face (joy/awe), P4 Mystery Object
- **Palette:** Saturated location colors, sunset/golden hour
- **Face:** Optional. Often subject in dramatic location.
- **Text:** Location names, costs ("$50 IN TOKYO")
- **Examples:** Adventure travel and aspirational creator patterns

### Comedy / shorts (Roblox-scenario style)
- **Top patterns:** P5 Reaction Face, P12 AI-Generated Surreal
- **Palette:** Bright, saturated, vertical
- **Face:** Open-mouth shock (still works in comedy/shorts)
- **Text:** 3-5 words bold, often punchline preview
- **Mobile:** Vertical-only. Design for 1080×1920 then crop center.

### Reaction
- **Top patterns:** P5 Reaction Face (almost always), P9 Split Frame
- **Palette:** Often the original content's palette + reactor's signature color
- **Face:** Reactor's exaggerated reaction. Open-mouth still works here.
- **Text:** ALL CAPS feeling words ("INSANE", "UNREAL")
- **Examples:** Major reaction channels

### Documentary long-form (tech-industrial-history / logistics-explainer / engineering-explainer)
- **Top patterns:** P10 Color-Pop on Mono, P4 Mystery Object, P11 Photoreal Composite
- **Palette:** Editorial, often two-color or duotone
- **Face:** Faceless channel — none.
- **Text:** Concept words, location, year. Clean.
- **Examples:** A tech-industrial-history channel's signature single-object editorial style

### Ambient / sleep / focus loop
- **Top patterns:** P11 Photoreal Composite (atmospheric), P4 Mystery Object
- **Palette:** Calm — blues, warm ambers, soft purples. Low contrast intentionally.
- **Face:** Faceless. Atmospheric only.
- **Text:** Minimal. "8 Hours" / "Rain Sounds" / track name.
- **Examples:** Ambient-soundscape and lofi music channels

## 9. The competitor-mining + modify protocol

This is THE meta-skill for thumbnail design. The agent uses this whenever the user mentions a reference channel ("make it look like a long-form mystery-documentary channel"), or whenever the agent enters a new niche where the user has no thumbnail brand yet.

### The 7-step DNA extraction

**Step 1 — Pull the channel's last 15-20 thumbnails.** Via YouTube Data API or scraping. Filter to outliers if possible (videos > 3× channel median).

**Step 2 — Categorize by canonical pattern.** Tag each thumbnail with which of the 12 patterns (P1-P12) it uses. Some thumbnails use combinations.

**Step 3 — Extract the 14 visual dimensions:**

| Dimension | What to extract |
|---|---|
| Pattern frequency | Which P1-P12 dominates? |
| Color anchor | What single color repeats? |
| Background tone | Dark, light, or duotone? |
| Face presence | Faceless, face-dominant, face-incidental |
| Face position | Top-left, top-right, center, bottom-left, etc. |
| Face emotion register | Authority / shock / accusation / curiosity / etc. |
| Text length | Average word count per thumbnail |
| Text placement | Top, bottom, overlaid on face, side panel |
| Text style | Font weight, stroke width, ALL CAPS frequency |
| Object hero shots | Recurring objects/props |
| Composition rule | Rule of thirds, centered, split-frame |
| Lighting | Cinematic, flat, hard, soft |
| Editing tells | Filter, color grade, signature visual treatment |
| Aspect/crop | Always 16:9 vs alternative crops |

**Step 4 — Build the "thumbnail DNA template":**
```
Channel: [name]
Dominant pattern: [P1, P2, etc.]
Anchor color: [hex or descriptive]
Background tone: [dark / light / duotone]
Face: [present / absent / position / emotion]
Text: [avg word count, ALL CAPS frequency, position]
Recurring props: [list]
Composition rule: [rule of thirds / center / split]
Visual treatment: [grade, filter, etc.]
```

**Step 5 — Identify ANCHOR vs SWAP-ABLE elements.**
- **Anchor elements** = what makes the brand recognizable. NEVER change these. Examples: a long-form mystery-documentary channel's sepia grade, a personal-finance authority channel's red anchor, a science-animation channel's blue.
- **Swap-able elements** = the per-video variation surface. CAN and SHOULD change. Examples: the specific object, the specific number, the specific text.

**Step 6 — Apply the 5 modification axes.** When generating new thumbnails for the user's channel that follow the DNA but differentiate enough to be original:

1. **Color shift** — Move the anchor 15-30 degrees on the color wheel. Example: long-form mystery-documentary sepia → user gets warm amber-orange variant. Recognizable, not duplicate.
2. **Prop swap** — Same composition, different hero object. The user's content drives the prop choice.
3. **Face replace** — If the reference uses a face, use the user's face/avatar in the same position with the same emotional register.
4. **Text restructure** — Same word count and placement, different content. Match the rhythm, not the specific words.
5. **Composition mirror** — Flip the composition horizontally. Same DNA, opposite hand. This single move differentiates surprisingly well.

**Step 7 — Validate against the 5-test mobile readability protocol.** No DNA-modified thumbnail ships without passing.

### Worked DNA extraction example

**Reference channel:** an economic-explainer channel
- **Pattern frequency:** P1 Face+Arrow+Number = 60%, P2 Rage Stamp = 25%, P11 Photoreal Composite = 15%
- **Color anchor:** Yellow (255, 215, 0) consistently
- **Background tone:** Dark navy (10, 20, 40)
- **Face:** Host face on left third, accusation register
- **Text:** 3-4 words, ALL CAPS, white on yellow stroke
- **Recurring props:** Stock charts, dollar bills, government building silhouettes
- **Composition:** Rule of thirds, face left, arrow pointing right at object
- **Visual treatment:** Cinematic dark grade, subtle vignette

**Personal-finance authority channel application of the economic-explainer DNA:**
- Anchor: SHIFT yellow → red (matches the personal-finance authority channel's existing brand)
- Prop swap: stock charts → IRS forms / SSA letters
- Face replace: economic-explainer host → personal-finance authority host
- Text restructure: same word count + caps frequency, different content
- Composition mirror: face right, arrow pointing left
- Result: visually distinct from the economic-explainer reference but operating in the same proven design language

The agent runs this DNA extraction when the user says "make it like an economic-explainer channel" OR when the agent identifies that an economic-explainer channel is a top performer in the user's niche.

## 10. The generation workflow

**Step 1 — Competitor research (MANDATORY).** Before generating anything, search YouTube for 5-8 top-performing videos on the same or adjacent topic. Use the Supadata API or youtube_search to find them, then download and visually inspect their thumbnails using the Read tool. Study each one: What pattern (P1-P12) does it use? What's the dominant color? Where is the focal point? What text is on it, how large, what font weight? How does it read at small size? Record your findings as a brief competitive analysis. This is non-negotiable — never generate a thumbnail without first seeing what's already winning in the niche.

**Step 2 — Load the composition bank.** Call `read_skill` to load `composition-bank.md` now. Use its pixel-level specs and worked prompts as templates. Adapt the video's topic into the bank's locked compositions — do NOT invent composition from scratch.

**Step 3 — Read the title + script first 30 seconds + niche DNA.** The thumbnail must visually confirm the title and pre-screen the right viewer for the script's hook.

**Step 4 — Pick the top 2 patterns** from the niche playbook (informed by what you saw winning in Step 1) + 1 wildcard (a pattern not common in the niche but might fit).

**Step 5 — Generate 4-6 candidates across the patterns.** Use the right model for the job:

| Need | Model |
|---|---|
| Photoreal face composite, professional finance/news | Nano Banana (Gemini 3 Pro Image) — best at faces + text |
| Surreal stylized scene, music video aesthetic | Flux 1.1 Pro or Midjourney |
| Clean illustration | Ideogram |
| Photorealistic objects | Nano Banana or DALL-E 3 |
| Reference-image-driven (must match user's face) | Nano Banana with reference image input |

**Step 6 — Visual QA on each generated base image (MANDATORY).** After generating each base image, Read it immediately and inspect it with your own eyes. Check: Is the subject clearly visible? Is contrast sufficient (not a dark blob)? Is the composition matching the intended pattern spec from the bank? Is there clear negative space where text will go? If a base image fails any of these checks, discard it and generate a new one with a corrected prompt. NEVER proceed to text overlay on a bad base. NEVER try to save a bad base with post-processing filters.

**Step 7 — Add text and post-process using `cli-anything-gimp`, not in the image model.** Generate the thumbnail BACKGROUND in the image model. For text overlays, compositing, color grading, and any post-processing, load the `gimp-image-editing` skill and use the `cli-anything-gimp` CLI via Bash. It provides layers, filters, text rendering with font control, and export — all stateful and repeatable. This guarantees pixel-perfect text rendering, mobile readability, and the ability to generate multiple text variants without re-rendering the background. Do NOT write raw Pillow scripts — use the CLI instead.

**Step 8 — Final visual inspection (MANDATORY).** Read each completed thumbnail (base + text overlay) and visually verify: Does the text read clearly against the background? Is the overall composition balanced? Does it match what you saw winning in the competitor research? Would you click this in a feed of 12 thumbnails? If any candidate looks weak on inspection, reject it.

**Step 9 — Run mobile readability tests.** All 5 from §7. Reject any that fail.

**Step 10 — Run simulated YouTube feed preview.** Place the thumbnail beside the channel's top 3 in-niche competitors (the ones you pulled in Step 1). Visual-sandwich check.

**Step 11 — Surface 4 final candidates to the user.** With brief reasoning per variant ("this one leans rage-stamp, this one leans authority portrait, this one leans curiosity gap").

**Step 12 — A/B test setup.** When user picks, immediately set up a YouTube native Test & Compare with 2-3 variants. Same title across all variants. Thumbnail is the only variable.

## 11. A/B testing

YouTube's native Test & Compare for thumbnails has been live since 2023 and is mature. The agent uses it on every upload by default.

**Test setup:**
- 3 variants when possible (YouTube allows up to 3)
- Run 4-14 days (YouTube auto-locks winner)
- Decision metric: watch-time-per-impression (NOT raw CTR)
- Don't touch mid-test

**What to test (high-signal variations):**
- Face vs no face
- Different emotional register (authority vs accusation vs shock)
- Different text claim (which dollar amount or threat word)
- Different color anchor

**What NOT to test (low-signal variations):**
- Minor color tweaks (5-degree shifts on the wheel)
- Minor font weight changes
- Slight repositioning of elements

**Refresh cycle:** After 30-60 days, if a video has plateaued and is still getting impressions, generate a new thumbnail and re-test. Up to 30% of videos can have meaningful CTR improvements from a thumbnail refresh 6+ months post-publish.

## 12. Anti-patterns

Thumbnails that fail across the board. The agent never generates these:

**A1 — Text too small.** Any text below 60px at 1280×720 render becomes unreadable at 320×180.
**A2 — No contrast.** Failing the 4.5:1 contrast ratio test = invisible at mobile size.
**A3 — Too busy.** More than 3 distinct elements competing for attention. Eye doesn't know where to land.
**A4 — Generic stock photo.** "Businessman pointing at chart" generic stock = invisible in feed.
**A5 — AI-slop tells.** Six fingers, melted faces, warped text, unrealistic proportions. Post-Jan 2026 YouTube moderates these aggressively under inauthentic content policy.
**A6 — Pure white background in dark-mode feeds.** White rectangle disappears.
**A7 — Drop shadows replacing strokes.** Drop shadows blur at small render sizes; strokes hold.
**A8 — Three or more bright colors.** Visual noise, no hierarchy.
**A9 — Centered face, centered text, centered object.** Symmetry reads corporate and underperforms.
**A10 — Watermark or channel logo dominant.** Wastes attention. Channel name is shown separately.
**A11 — Year-stamping on evergreen content.** Dies on Jan 1 of next year.
**A12 — Text overlapping the face.** Eye can't process either.
**A13 — Faces with closed eyes.** Reads dead/inactive.
**A14 — Smiling in authority/finance niches.** Reads weak. Even casual smiles underperform vs neutral or intense expressions.
**A15 — Multiple equally-weighted text elements.** No dominant focal point.
**A16 — Iterative post-processing of a bad base.** If the generated base image doesn't work at first glance (too dark, wrong composition, muddy, low contrast), discard it and generate a new one with a corrected prompt. Never try to save a bad base with color grading, brightness adjustments, or filter stacking. A good thumbnail starts from a good base.
**A17 — Generating without competitor research.** Never create a thumbnail in a vacuum. You must see what's currently winning in the niche before generating anything. Thumbnails compete in a feed — you can't design a winner without knowing the competition.
**A18 — Shipping without visual self-inspection.** Never present a thumbnail to the user without first Reading the image yourself and confirming it looks good. Pixel statistics and metadata are not substitutes for looking at the image with your own vision capabilities.

## 13. The expert quote bank

**A top viral-challenge creator (2022):** *"You can't just smile. You have to look like you've seen God or seen a ghost."* (Open-mouth shock era)

**A top viral-challenge creator (mid-2024):** *"The shock face was getting cliché. The audience reads through it now. Closed-mouth intensity beats open-mouth shock by like 15-20% on watch time."* (The doctrine shift)

**A top viral-challenge creator on thumbnail investment:** *"I'll spend $10,000 and a full day on a thumbnail because if the thumbnail is 5% better, the video is 5% better forever. It compounds."*

**A leading thumbnail consultant for top channels:** *"Walk 20 feet from the screen. Set brightness to 40%. If your thumbnail still reads, you're done. If not, redo it."* (The 20-foot / 40% rule)

**A YouTube growth strategist:** *"Generate four thumbnails. Always four. Then test the best two. Don't ship without an A/B."*

**Todd Beaupre (YouTube Senior Director Growth & Discovery):** *"The thumbnail is the single biggest predictor of click-through, but click-through alone is not what we optimize for. We weight CTR by the watch-time it produces."*

**A science-explainer channel host:** *"There's a difference between a thumbnail that promises a real interesting answer and one that promises a fake one. The 2025 algorithm punishes the fake one within 24 hours."*

**A leading tech reviewer:** *"In tech you don't need to scream. The audience is sophisticated. A clean thumbnail with the product hero shot and one calm authority face beats every loud variant."*

**Sean Cannell (Think Media):** *"The thumbnail is the cover of the book. The title is the headline. Together they sell the click. Apart they confuse."*

**vidIQ data (2024):** *"Strong-emotion faces lift CTR by 20-30% across niches. Direct eye contact lifts another 8-12% in 18-34 demos."*

**TubeBuddy data (Jan 2025):** *"Bright red anchors outperform blue anchors by ~23% CTR in matched A/B conditions."*

## 14. Three worked examples

### Example 1 — IRS Senior Finance, an example episode

- **Title:** "The 10,000 Dollar Bank Rule Just Got MUCH WORSE"
- **Niche:** Senior finance / IRS / retirement
- **Niche DNA:** P2 Rage Stamp dominant, red anchor, dark background, host face top-left, accusation register
- **Generated candidates:**
  1. **Rage Stamp + host accusation pose + "MUCH WORSE" stamp** ← winner
  2. P1 Face+Arrow+Number with "$10,000" highlighted
  3. P10 Color-Pop on Mono with single dollar bill object
- **Why winner:** Strongest niche fit (personal-finance authority brand = rage-stamp), maximum mobile readability, accusation register pre-screens the audience that retains
- **A/B variant:** Same composition, different stamp text ("DESTROYED" vs "MUCH WORSE")

### Example 2 — Roblox-scenario short

- **Title:** "What If A Teacher OWNED Roblox?"
- **Niche:** Gaming / Roblox shorts (vertical)
- **Niche DNA:** P5 Reaction Face open-mouth shock, P12 AI-Generated Surreal as wildcard
- **Generated candidates:**
  1. **Open-mouth shock face + Roblox character + "TEACHER OWNS ROBLOX"** ← winner
  2. P12 surreal AI scene of teacher in Roblox
  3. P9 Split frame: teacher classroom vs Roblox HQ
- **Why winner:** Open-mouth shock still wins in gaming/shorts. Vertical-feed-optimized at 1080×1920 cropped to thumb. Mobile readability passes at 240×135.

### Example 3 — Doctor Senior Health (medical-authority style)

- **Title:** "Your Joints Hurt Because Of This One Supplement Mistake"
- **Niche:** Health / medical / supplements
- **Niche DNA:** P1 Face+Arrow+Number dominant, doctor authority face, green/yellow accent, light background or clinical
- **Generated candidates:**
  1. **Doctor face top-left (concerned, closed-mouth) + arrow pointing at supplement bottle + red X over bottle** ← winner
  2. P3 Before/After: painful joints diagram → relief diagram
  3. P10 Color-Pop on Mono: single supplement bottle isolated against grey
- **Why winner:** Authority Portrait pre-screens audience. Direct concern emotion matches the wound-first hook. Arrow + bottle delivers the specific hook visually.

## 15. Runtime checklist

Before any thumbnail surfaces to the user:

- [ ] Competitor thumbnails pulled AND visually inspected (5-8 top performers in niche)
- [ ] Composition bank companion loaded
- [ ] Niche DNA loaded (from competitor research + §8 playbook)
- [ ] Title + script first 30 seconds reviewed (for thumbnail-title coherence)
- [ ] 4-6 candidates generated across patterns (using bank's worked prompts as templates)
- [ ] Each base image visually inspected via Read — contrast, composition, subject clarity confirmed
- [ ] Bad bases discarded and regenerated (not post-processed)
- [ ] Text overlay was rendered via `cli-anything-gimp`, not in image model
- [ ] Each completed thumbnail visually inspected via Read — text legibility, overall quality confirmed
- [ ] Each candidate passes 5-test mobile readability
- [ ] Anchor color matches channel brand (or is being deliberately introduced)
- [ ] Face emotion register matches niche convention
- [ ] No anti-patterns present (check A1-A18)
- [ ] Simulated feed preview run against competitors from Step 1
- [ ] A/B test plan ready (variants, duration, metric)

If any check fails, regenerate. Never surface a failing thumbnail.

---

## Update log

This skill is current as of April 2026. Update when:
- New canonical pattern emerges (last new addition was P12 AI-Generated Surreal in 2024)
- YouTube changes thumbnail rendering rules (resolution shifts, vertical feed evolution)
- Major creator shifts the doctrine (the open-mouth → closed-mouth shift in mid-2024 was a significant pattern update)
- New niche emerges with distinct conventions

The raw research file at `projects/rookcast/knowledge/research/thumbnail-research-raw.md` is the source — quote it for primary sources when explaining decisions to the user.
