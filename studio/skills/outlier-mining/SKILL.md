---
name: outlier-mining
description: >-
  Picks WHAT to make. Outlier classification + topic ideation + ranking. Load when answering "what should I make next?" or evaluating a topic candidate.
---

# Skill 12 — Outlier Mining & Topic Ideation

This is the operational knowledge an AI YouTube agent needs to identify outlier videos, extract their replicable patterns, and ideate topics that have a credible path to outperform a channel's median. Every paragraph is a concrete rule the agent applies.

This is the upstream skill. Skills 01-11 assume a topic is given. This skill answers the question that comes before all of them: **what should we make next?**

A channel that produces 50 videos per year using only Skills 01-11 will produce 50 well-executed but topically random videos. A channel that runs Skill 12 first will produce 50 videos with credible outlier potential — same execution cost, multiplied outcome.

---

## 1. The job of outlier mining

Outlier mining does five things:

1. **Identifies videos that beat their niche median by 5-50×** — the outliers carry signal about what the algorithm and audience actually reward in 2025-2026
2. **Extracts the replicable mechanism** — separating the "why it won" (replicable pattern) from the "what it was about" (specific topic)
3. **Predicts which patterns are still working** — distinguishing live patterns from played-out ones, before investing production cost
4. **Generates topic candidates** — taking proven outlier patterns and applying them to the channel's niche and capabilities
5. **Ranks candidates by predicted watch-time-per-impression** — so the channel commits production budget to the highest-leverage idea

The agent never picks topics from intuition or random brainstorm. Every topic candidate is mined from observable outlier signal in the niche or in adjacent niches. Intuition is for refinement, not selection.

---

## 2. The optimization target

The agent optimizes topic selection against **predicted-outlier-probability × execution-feasibility**.

- **Predicted-outlier-probability:** what's the probability this idea, executed well, hits 3-10× the channel median?
- **Execution-feasibility:** can the existing production stack make this idea well, or does it require new capabilities (new voice, new aesthetic, new locations)?

A topic with 70% outlier probability but 20% feasibility (requires capabilities the channel doesn't have) loses to a topic with 40% outlier probability and 90% feasibility. Production budget is the scarce resource.

The math the agent runs implicitly: **expected_value = outlier_probability × outlier_multiple × feasibility_score × niche_RPM × estimated_views**. The candidate with the highest expected value wins.

---

## 3. The 4 outlier types

Every outlier video falls into one of four categories. The replication strategy differs per type.

### Type 1 — Format outlier

**Definition:** the format itself is the click. Topic could be substituted; the format would still win.
**Examples:**
- A top viral-challenge creator's "$1 vs $1M" — the comparison format wins regardless of what's compared
- "I tried every X" — exhaustive-test format
- "X people pick from Y" — group-decision format
- "I built X in N hours" — speed-build format

**Replication strategy:** identify the format, apply to your channel's topic. The format itself is portable.
**Replication risk:** low. Format outliers transfer well across niches.
**Cite from:** Skill 01 §3 formula library.

### Type 2 — Topic outlier

**Definition:** the topic itself is the click. Format could be substituted; the topic would still win.
**Examples:**
- "What happened to MH370" (true crime / mystery topic that never stops winning)
- "How TSMC manufactures chips" (specific topic that pulls regardless of format)
- "Roman concrete and why it lasted" (topic with built-in curiosity gap)

**Replication strategy:** if the topic is evergreen, target it directly with your channel's format. If the topic is news-pegged, find the next news beat that pulls the same curiosity register.
**Replication risk:** moderate. Topic outliers age — a 2024 outlier on AI tools may not work in 2026 because the topic has saturated.

### Type 3 — Hook outlier

**Definition:** the title-thumbnail combination is the click. Inside the video could be anything.
**Examples:**
- "$32,820 Seized. Zero Charges Filed." — the specificity of the dollar + the legal counterpoint
- "The Map That Shouldn't Exist" — pure paradox premise

**Replication strategy:** identify the hook architecture (Skill 01 §3, Skill 02 §3) and apply to your channel's content. Hook outliers are the cleanest pattern to replicate because the mechanism is at the surface.
**Replication risk:** low. Hook patterns are documented (Skill 01) and re-skinning is cheap.

### Type 4 — Format-x-topic crossover outlier

**Definition:** a combination that ought not work, working extraordinarily well. Two domains collide in a way the audience hasn't seen.
**Examples:**
- LEGO + drill music + political satire — three separate format/topics that collide into a brick-narrative storytelling outlier
- "I spent 30 days using AI to run my entire small business" — AI tool genre + small business genre crossover
- "Senior cybersecurity expert reviews TikTok scam compilations" — expert-review format + TikTok content + senior cohort

**Replication strategy:** identify the crossover. Look for adjacent crossovers your channel can credibly do. Crossovers are high-variance — they win big when they work, fail completely when they don't.
**Replication risk:** highest. The crossover often dies on the second video because the novelty was the entire mechanism.
**Note:** the agent flags Type 4 outliers as "high-variance — propose only when channel is willing to take a single-shot bet." Don't anchor a content calendar on Type 4 alone.

### How to classify an outlier

The agent runs this classification before choosing replication strategy:

1. **Could you swap the topic and the format would still win?** → Type 1 Format outlier.
2. **Could you swap the format and the topic would still win?** → Type 2 Topic outlier.
3. **Is the click entirely in the title/thumbnail combo?** → Type 3 Hook outlier.
4. **Is the win in the unexpected combination of two distinct domains?** → Type 4 Crossover outlier.

If two answers are yes, the outlier is multi-mechanism — replicate the strongest single mechanism first.

---

## 4. The outlier metrics

The agent identifies outliers by computing three ratios:

### Outlier multiple (channel-internal)

```
outlier_multiple = video_views / channel_median_views_per_video_at_same_age
```

- **>3×:** soft outlier
- **>5×:** clear outlier
- **>10×:** strong outlier — pattern is signal-rich
- **>30×:** generational outlier — pattern likely once-in-a-channel

The "at same age" matters. A video posted 6 months ago has had time to accrue views; comparing to the channel's all-time median favors older videos. Compute median for videos posted in the same month or quarter.

### Outlier multiple (niche-relative)

```
outlier_multiple_niche = video_views / niche_median_views_per_video_at_same_age
```

This requires niche-level baseline data (vidIQ, Spotter Studio, viral-database skill output). Niche-relative outliers travel — a video that beats the niche median by 10× usually contains pattern signal that other channels in the niche can replicate.

### Watch-time-per-impression ratio (best signal)

```
wtpi = total_watched_minutes / total_impressions_served
```

This is the algorithm's actual optimization target (per Skill 01 §2). It's harder to access — YouTube Studio Analytics → Reach tab. When available, it dominates the other two metrics. A video can have moderate views but exceptional WTPI because the algorithm is actively pushing it; this is the truest outlier signal.

### Combining the three

The agent ranks outliers by:
1. WTPI ratio (if available) — primary signal
2. Niche outlier multiple — secondary
3. Channel-internal outlier multiple — tertiary

Channel-internal alone is the weakest signal because a channel's median can be artificially low (small channel, niche oversaturation, etc.).

---

## 5. Where outliers come from — the data sources

The agent pulls outlier data from these sources, in priority order:

### Source 1 — Channel's own analytics

YouTube Studio → Analytics → Reach. The channel's own outliers are the strongest signal because the audience and algorithm context are fully matched.
**Use when:** the channel has 30+ videos and ≥1 video at 5× channel median.

### Source 2 — vidIQ outlier scores

vidIQ computes outlier scores per video. It pulls from public data and runs niche normalization. Imperfect but cheap and fast.
**Use when:** scanning a niche for general outlier patterns. Low effort, low precision.

### Source 3 — Spotter Studio competitive analytics

Spotter offers per-niche outlier feeds with format/topic classification. Higher precision than vidIQ but paid.
**Use when:** running a serious niche audit before committing to a channel.

### Source 4 — Internal viral-database skill

Nick's `viral-database` skill maintains a database of every 1M+ view video with 10×+ outlier score across tracked channels.
**Use when:** the agent needs cross-niche outlier signal or wants to find Type 4 crossover patterns.
**Reference:** viral-database skill in `.claude/skills/viral-database/`.

### Source 5 — Manual scraping of outlier tracker accounts

Twitter accounts like @YouTubeOutliers and @ContentOutliers post daily outlier finds. Short-form discovery.
**Use when:** looking for emergent patterns (last 7-30 days) that haven't shown up in tracked databases yet.

### Source 6 — Direct YouTube browsing with niche-specific search

Search a niche keyword → sort by view count → filter to last 30/60/90 days. Find recent outliers manually.
**Use when:** all other sources fail or the niche is too small to be in databases.

### Source 7 — Native YouTube tools (in-agent research)

The agent has built-in YouTube tools that automate Sources 2-6 directly. Faster, cheaper, and reproducible — no external SaaS, no per-seat fees.

Available tools. These return RAW Data API rows — outlier classification (cohort splits, multipliers, Type 1-4 categorization) is YOUR job, applied per the rules in this skill:

- `youtube_search` — keyword search with sort order (`relevance`, `date`, `viewCount`, `rating`). Use `order: "viewCount"` + a recent date filter to find recent outliers. Max 25 results per call.
- `youtube_video_details` — full metadata for specific videos by ID. Returns title, description, tags, view/like/comment counts, duration, and privacy status. Use to enrich search results with full stats.
- `youtube_channel_stats` — channel header (subs, video count, total views). Run this FIRST when sizing an unfamiliar channel — a 5× viewsPerDay multiplier reads completely differently on a 5k-sub vs 5M-sub channel.
- `youtube_list_videos` — recent uploads from the user's own connected channel. Use to pull the channel's own catalog for self-optimization.

For transcripts, use the Supadata API via Bash:
```bash
curl -s "${SUPADATA_API_BASE_URL}/youtube/transcript?videoId=<videoId>&text=true" \
  -H "x-api-key: $SUPADATA_API_KEY"
```

For frame extraction from a video (visual hook analysis), use yt-dlp + ffmpeg via Bash:
```bash
yt-dlp -f 'bestvideo[height<=720]' -o video.mp4 "https://youtube.com/watch?v=<videoId>"
ffmpeg -i video.mp4 -vf "fps=1/15" -q:v 2 frames_%04d.jpg
```
Then `Read` each frame path to load the JPG as visible pixels in your context — that's how you analyze hooks (visual + audio + on-screen text), thumbnail/content alignment, B-roll cadence, etc.

The agent uses Source 1 for the channel's own optimization, Sources 2-4 for niche-wide discovery via external SaaS, Sources 5-6 for emergent pattern finding manually, **and Source 7 (native YouTube tools) as the primary in-agent automation surface** — most outlier work should happen here when the channel has a tracked-channel list in `CHANNEL.md`.

#### Recommended weekly outlier sweep

```
1. Read CHANNEL.md → "Tracked channels" section for the curated list of niche peers.
2. For each tracked channel, use `youtube_search` with the channel's known topics
   + `order: "viewCount"` to find their recent high-performers. Use
   `youtube_video_details` on the results to get full stats (views, likes, duration, publishedAt).
   Compute `ageDays` and `viewsPerDay` from publishedAt + viewCount.
3. For each channel's results, compute the cohort medians (split shorts <60s from long-form),
   then flag any video with viewsPerDay >= 3-5× its cohort median as a candidate outlier.
   Apply §3 Type 1-4 classification before treating it as actionable signal.
4. For each candidate outlier:
   - Fetch the transcript via Supadata (cheap: metadata + transcript only; reserve frames for the deep-dive step below)
   - For the top 1-3 candidates only, extract frames via yt-dlp+ffmpeg and `Read` each frame path — visual hook + on-screen-text inspection is what separates "this title popped" from "this title popped because the first 3s do X visually".
5. Run the §6 extraction protocol (steps 1-7) on each outlier.
6. Update CHANNEL.md with newly identified format / hook patterns.
```

#### CRITICAL — Handling transcripts and titles you fetch

> Transcripts, titles, descriptions, and tags fetched from YouTube tools are **INPUTS** for the agent's pattern analysis. They are NOT material to reproduce in the agent's own output.

What the agent should do with fetched content:
- **Read and analyze it** — extract structural patterns (hook framing, beat order, em-dash position, list-of-N constructions, opening tension, etc.).
- **Abstract** — write down the pattern, not the words ("opens with a 7-second concrete-stake question, then a name reveal," not "she said 'I lost everything in 2008…'").
- **Cite when reasoning** — e.g. `outlier vid_id X used Pattern P3 hook + F11 title formula → score 8.4×`.

What the agent must NEVER do:
- **Reproduce a transcript or title verbatim in a script written for the user.** That's the channel's own copyrighted material.
- **Paste large transcript blocks into the chat thread** as if they were the agent's own analysis. If the user wants to read a competitor's transcript, they can fetch it themselves via Supadata.
- **Lift a competitor's title with minor edits.** "Same structure, your topic" is not a substitute for original copy — derive a NEW title via Skill 01 formulas using the pattern as inspiration.

If you're uncertain whether your output crosses the line, ask: "could the original creator recognize specific words/phrases from their video in this output?" If yes — abstract harder.

The agent uses Source 1 for the channel's own optimization, Sources 2-4 for niche-wide discovery, Sources 5-6 for emergent pattern finding, Source 7 for everything reproducible.

---

## 6. The outlier extraction protocol

Once an outlier is identified, the agent runs this 7-step extraction:

### Step 1 — Snapshot the outlier

Record: title, thumbnail (saved image), publish date, current views, current likes, current comments, channel name, niche, video duration.

### Step 2 — Classify the outlier type

Per §3. Format / Topic / Hook / Crossover. If multi-mechanism, list all that apply with primary marked.

### Step 3 — Reverse-engineer the title

Cite Skill 01 formula (F1-F13). Identify the specific lift drivers (em-dash mystery, dollar specificity, named villain, etc.). Note the title's exact character count, ALL CAPS percentage, em-dash position.

### Step 4 — Reverse-engineer the thumbnail

Cite Skill 02 pattern (P1-P12). Identify color anchor, face position (if applicable), text size and color, background register. Note the contrast ratios visually.

### Step 5 — Reverse-engineer the hook (first 30 seconds)

Pull the transcript of the first 30 seconds (manual or auto-transcribe). Cite Skill 03 hook architecture (H1-H4). Note: pattern interrupt timestamps, promise stack presence, voice archetype.

### Step 6 — Reverse-engineer the structural pattern

Skim the entire video. Note: total beat count, beat lengths, retention valley fixes (the moves at 30%, 60%, 80%), open-loop usage, CTA placement.

### Step 7 — Build the replicable spec

Output a one-page spec that ANY video applying the same mechanism could use. Format:

```
OUTLIER SPEC — [video title]
Type: [1/2/3/4]
Outlier multiple: [niche multiple, channel multiple, WTPI ratio if available]

REPLICABLE PATTERN
Title formula: F[N] with [specific lift drivers]
Thumbnail pattern: P[N] with [color anchor, composition]
Hook architecture: H[N] with [pattern interrupt cadence]
Voice archetype: V[N]
Structural template: [12-beat / vertical short / music video / news-hijack]
Estimated production cost: [hours + dollars]

WHAT TO COPY
- [specific technique 1]
- [specific technique 2]
- [specific technique 3]

WHAT NOT TO COPY
- [topic-specific element that doesn't transfer]
- [presenter-specific element that doesn't transfer]

VARIANTS FOR THE TARGET CHANNEL
- [variant 1 applying pattern to channel's topic]
- [variant 2 applying pattern to channel's topic]
- [variant 3 applying pattern to channel's topic]
```

This spec lives in the `research/` folder of the project and is referenced by every Skill 01-11 generation that uses the pattern.

---

## 7. The 22-niche outlier pattern bank

For the agent's 22 tracked niches, the dominant outlier patterns as of April 2026.

### Senior finance / IRS / retirement (personal-finance authority niche)

**Live patterns:**
- F2 + F11 dollar-counter-narrative ("$246K Lost By Filing Social Security At 70 (Most Retirees Don't Know This)")
- F6 + F12 threat-alert ("CONFIRMED: Social Security Shake-Up Hits May 1")
- F5 + F1 named-villain forensic ("He Lost $160K In One Phone Call — Then Chase Refused")

**Emerging patterns:**
- "Federal credentialed expert reacts to scam" format
- Single-victim-deep-dive with 3 chapters of investigation

**Played out:**
- Generic "5 Social Security Tips" — dies under outlier discrimination

### Senior health / medical / supplements (medical-authority niche)

**Live patterns:**
- F4 + F9 numbered + cited authority ("13 Foods That Reverse Type 2 Diabetes Per A Leading Clinician")
- F10 + F11 wound-first + age-gated ("Your Doctor Won't Tell You About This Test (Catches Kidney Decline 2 Years Early)")

**Emerging patterns:**
- "Geriatric specialist reviews TikTok health claims" reaction-expert hybrid

**Played out:**
- "Top 10 superfoods" — mass-saturated, no longer outliers

### Tech / AI / dev tools

**Live patterns:**
- "I built X in N hours with [tool]" — speed-build with quantified time
- "Tool A vs Tool B after 30 days side-by-side" — duration-tested comparison
- "Why most AI wrappers will be worth zero in 18 months" — doom prediction with time anchor

**Emerging patterns:**
- "I held 5 jobs simultaneously using AI" — nick-style multi-job AI documentation

**Played out:**
- "Top 10 AI tools for 2024" — saturated; recency mismatch

### Vertical shorts (Roblox-scenario channel)

**Live patterns:**
- "POV: [school role] becomes [Roblox element]"
- "What if [game element] was real money / real consequence"

**Emerging patterns:**
- TikTok cross-pollination (Roblox-style POV applied to non-Roblox topics)

### Music video / drill / propaganda

**Live patterns:**
- News-pegged drill response (within 24-48 hours of news beat)
- Brick-narrative storytelling political satire — Type 4 crossover

**Emerging patterns:**
- AI-generated drill responses to specific viral incidents

**Risk:** post-Jan 2026 YouTube enforcement on inauthentic content has started terminating channels in this category. Outlier multiple is high but channel-survival probability is uncertain.

### History / explainer (long-form mystery-documentary style)

**Live patterns:**
- F1 + specific date + impossible event ("In 1518, 400 People Started Dancing")
- F8 paradox premise ("The Map That Shouldn't Exist")
- Single-mystery-deep-dive at 25-45 min length

**Played out:**
- "Top 10 historical mysteries" — never works at the outlier level

### News-hijack documentary (investigative-journalism / geopolitics documentary register)

**Live patterns:**
- Named villain + specific dollar + investigation duration ("I Spent 6 Weeks Investigating This $40M Crypto Scam")
- F1 + suppression frame ("The Document They Don't Want You To See")

### True crime

**Live patterns:**
- Named victim + specific date + place at title top
- Cold-case-with-new-lead format
- "What [investigator] never told the family" framing

**Played out:**
- Generic "5 most disturbing unsolved cases" listicles

### Health / medical / supplements

(Already covered — see senior health.)

### Real estate / home

**Live patterns:**
- First-person dollar-specific reversal ("Why I Just Sold My $1.2M Rental")
- Market-prediction with specific cities ("3 Cities Where Home Prices Will Drop 30% By 2027")

### Beauty / fashion / makeup

**Live patterns:**
- "I bought the most expensive X in [city]" comparison
- "I tested every [brand] product so you don't have to"

### Cooking / food

**Live patterns:**
- Iconic-recipe-but-better ("McDonald's Apple Pie But Better")
- Counter-narrative against chef-class wisdom

### Crypto / finance trading

**Live patterns:**
- Threat alert with specific time anchor
- Named-villain investigation

### How-to / education

**Live patterns:**
- Time-bounded mastery ("Learn X In Y Minutes")
- Single-framework deep-dive

### Fitness / bodybuilding

**Live patterns:**
- Wound-first with age-gating ("Why Most Men Over 40 Are Doing Squats Wrong")

### Faith / Christian / religion

**Live patterns:**
- Counter-narrative against canonical interpretation ("Why The Lord's Prayer Is Mistranslated")
- Personal-testimony with vulnerability

### Politics / commentary

**Live patterns:**
- Named villain + specific evidence pattern (mirror of news-hijack docs)
- Outraged-activist register against institutional frame

### Travel

**Live patterns:**
- "I went to the [extreme] place in [country]"
- Personal-testimonial with vulnerability

### Reaction

**Live patterns:**
- Expert-reaction format (cardiologist reacts, lawyer reacts, etc.)
- "First time watching" reactions to long-popular media

### Documentary long-form (tech-industrial-history / logistics explainer register)

**Live patterns:**
- Industry-doom prediction with specific company
- Single-decision-shaped-the-world historical retrospective

### Ambient / sleep / focus loop

**Live patterns:**
- "X hours of [aesthetic]" with specific time anchor
- Specific-fictional-environment ("rainy library at night with cat")

### Vlog / lifestyle

**Live patterns:**
- Specific challenge with self-imposed constraint
- Personal-vulnerability with reveal

---

## 8. The topic ideation workflow

When the user asks "what should I make next?" the agent runs:

### Step 1 — Pull the channel's own outlier data

What's the channel's median? What's the highest-performing video? What's the third-highest? Identify the channel's internal outliers and their patterns (per §3 classification).

### Step 2 — Pull niche-relative outlier data

What's winning in the niche right now (last 30-60 days)? Pull from vidIQ / Spotter / viral-database (§5). Classify each.

### Step 3 — Find pattern overlaps

Where do channel-internal outliers and niche-relative outliers share patterns? Those are the highest-leverage replicable patterns for THIS channel.

### Step 4 — Generate 12-15 candidate topics

For each replicable pattern identified in Step 3, generate 2-4 topic candidates that:
- Apply the pattern to the channel's topic surface
- Match the channel's existing voice DNA (no new voice required)
- Use the channel's existing visual aesthetic (no new style)
- Can be produced with the channel's locked toolchain

### Step 5 — Validate each candidate

For each candidate, check:
- **Search demand:** does the topic have meaningful search volume? (vidIQ keyword tool, Google Trends)
- **Competition density:** how many videos exist on this topic? (more isn't always worse — saturated topics with fresh angles still win)
- **CPM estimate:** what RPM does this topic command? (finance > tech > health > history > vlog > true crime in raw RPM)
- **Brand fit:** does this topic match the channel's audience trust contract?
- **Outlier potential:** based on the pattern source, what's the realistic outlier multiple if executed well?
- **Faceless feasibility:** can the channel produce this without new on-camera talent?

### Step 6 — Rank candidates

Score each candidate on:
- Predicted outlier probability (0-100%)
- Predicted outlier multiple (e.g., 3×, 5×, 10×)
- Production cost (hours + dollars)
- Brand alignment (1-5)
- Time-sensitivity (evergreen vs news-pegged)

Rank by `outlier_probability × outlier_multiple × niche_RPM / production_cost`. The top 3 candidates are the primary recommendations; candidates 4-6 are alternates.

### Step 7 — Surface to user with reasoning

Present the top 3 candidates with:
- Title candidate (using the pattern's lift drivers)
- One-line topic description
- Source outlier (which video this pattern was extracted from)
- Outlier multiple expectation
- Why this pattern fits this channel
- Estimated production hours / dollars

The user picks (or rejects, or modifies). The agent does NOT pick unilaterally — topic selection is a brand decision that lives with the user.

---

## 9. The replication ethics

YouTube terminates channels for outright copying. The agent replicates patterns, not specifics.

### Replicate

- Title formula and lift drivers (Skill 01 §3)
- Thumbnail pattern and composition rules (Skill 02 §3, §5)
- Hook architecture (Skill 03 §5)
- Voice archetype (Skill 03 §3)
- Structural template (Skill 03 §4)
- Beat shape (Skill 03 companion)
- Music register (Skill 11 §5)

### Don't replicate

- Specific phrasing from the source video (verbatim quotes are stealing)
- Specific named victims or composite characters (re-create your own composites)
- Specific dollar figures from the source (use your own niche data)
- Specific anecdotes (use your own examples)
- Specific images or thumbnails (generate your own)
- Specific channel branding elements (logos, color palettes from the source)

### The differentiation rule

For every outlier replicated, introduce 2-3 deliberate differentiators:
- A different specific anchor (different dollar, different name, different date)
- A different voice cadence variant (within the same archetype)
- A different visual aesthetic detail (within the same pattern)

This is the same rule as Skill 03 §10 (competitor voice mining). Steal the rhythm, register, and structural pattern. Never steal specific phrasings.

### The post-July-2025 enforcement risk

YouTube's inauthentic content policy specifically targets "spam, repetitive, or mass-produced content." Channels that replicate the same outlier pattern with minimal variation across 50 videos are flagged. The agent's defense: 2-3 deliberate differentiators per video + at least 3 different patterns rotated through a content calendar (not 50 videos all on the same one).

---

## 10. The validation gates

Before committing production budget to a topic, the agent runs four validation gates.

### Gate 1 — Title gut-check at 60 chars

Write the candidate title at the channel's typical length. Does it stop the scroll? If not, the topic isn't ready. Refine the angle before committing.

### Gate 2 — Thumbnail concept feasibility

Can the agent produce a thumbnail that fits Skill 02 patterns and visualizes the topic? If the topic resists strong visual treatment, it will underperform regardless of title quality.

### Gate 3 — 5-promise stack feasibility

Can the agent write 5 specific promises (Skill 03 §5) for this topic? Each promise must be concrete enough that the viewer cannot find it in 30 seconds of competitor scrolling. If the topic only supports 2-3 specific promises, the script will lack retention scaffolding.

### Gate 4 — Production cost vs predicted outcome

Estimate production hours + dollars vs predicted views × niche RPM. If the expected revenue minus production cost is negative, the topic is not commercially viable for this channel right now.

If any gate fails, the topic moves to the alternate list (or deprioritizes). Don't push topics through failing gates — production cost is high enough that lower-leverage choices have real opportunity cost.

---

## 11. Anti-patterns

Eight outlier-mining mistakes that waste production budget.

### Anti-pattern 1 — Chasing dead trends

A topic was a 30× outlier in 2023; the channel rushes to make it now. By 2026 the topic is saturated and YouTube's algorithm has moved on. The pattern is dead.
**Fix:** outlier signal is RECENT signal. Filter to last 30-90 days for live patterns, not all-time.

### Anti-pattern 2 — Replicating without classification

The agent finds an outlier and immediately copies the topic without checking whether it's Format / Topic / Hook / Crossover. Wrong replication strategy applied.
**Fix:** classify before replicating. Per §3.

### Anti-pattern 3 — Single-source outlier data

Pulling outlier data only from vidIQ. vidIQ misses ~30% of true outliers because its scoring is imperfect.
**Fix:** triangulate from at least 3 sources. Per §5.

### Anti-pattern 4 — Crossover dependence

Building a content calendar around Type 4 crossover outliers. The novelty is the mechanism; second video dies.
**Fix:** Type 4 outliers are single-shot bets, not calendar foundations. Anchor calendar on Type 1 (Format) and Type 3 (Hook) which transfer reliably.

### Anti-pattern 5 — Outlier-without-feasibility

The agent identifies a great outlier pattern but the channel can't credibly produce it (wrong voice, wrong aesthetic, wrong toolchain). Forces the channel to drift.
**Fix:** feasibility is a hard gate. Skip outliers that require capabilities the channel doesn't have unless the user is explicitly investing in those capabilities.

### Anti-pattern 6 — Skipping validation gates

Topic looks good, agent writes the script directly. Hits Gate 3 mid-script and discovers the topic doesn't support 5 specific promises. Production cost wasted.
**Fix:** all 4 gates pass BEFORE committing to script generation.

### Anti-pattern 7 — Recency-blind median

Computing channel-internal outlier multiple against all-time median. Older videos beat the average; the new video's outlier signal looks weaker than it is.
**Fix:** compare against same-period median (last 90 days posted same age).

### Anti-pattern 8 — Pure-CTR optimization

The agent optimizes for predicted CTR and ignores AVD/satisfaction. A title that wins clicks but the script doesn't deliver burns the channel within 24 hours.
**Fix:** optimize for predicted-watch-time-per-impression (Skill 01 §2), not predicted CTR.

---

## 12. Worked examples

### Example 1 — Outlier extraction (personal-finance authority "$246K" video)

```
OUTLIER SPEC — $246,000 Gone By Filing Social Security At 70

Type: Type 3 Hook outlier (primary) + Type 1 Format outlier (secondary)
Outlier multiple: ~3.2× channel median CTR, ~1.8× AVD

REPLICABLE PATTERN
Title formula: F2 (specific dollar) + F11 (parenthetical tag)
  Lift drivers: specific dollar ($246K not $250K), counter-narrative (filing-at-70 conventional advice subverted), parenthetical inclusion ("most retirees")
Thumbnail pattern: P2 Rage Stamp with $246K as primary read
Hook architecture: H3 Threat Alert with audience-defining condition
Voice archetype: V2 Federal Credentialed Expert
Structural template: 12-beat default

WHAT TO COPY
- Specific large dollar + counter-narrative pattern
- Parenthetical inclusion claim
- Audience-defining first sentence
- Single victim composite in Beat 4

WHAT NOT TO COPY
- "Patricia" specific name
- "Cincinnati" specific location
- Specific dollar $246,000

VARIANTS FOR THE PERSONAL-FINANCE AUTHORITY CHANNEL
- "$192,000 Lost By Taking Medicare Part B At 65 (Most Seniors Don't Know This)"
- "$73,000 Gone By Filing IRA Withdrawals In Order Most CPAs Recommend"
- "$184,000 In Tax Penalties Avoided By Filing One Form (Almost Nobody Uses It)"
```

### Example 2 — Outlier extraction (brick-narrative storytelling channel example episode)

```
OUTLIER SPEC — "You're Not Jesus!" Brick-Narrative Channel Drops ANOTHER LEGO Music Video

Type: Type 4 Crossover outlier (primary)
Outlier multiple: estimated 6× niche median

REPLICABLE PATTERN
Title formula: F9 (quote attribution) + F12 (ALL CAPS) + news-hijack
Thumbnail pattern: P12 AI-Generated Surreal with stylized LEGO
Voice archetype: V14 Drill Rapper Narrator
Music: Suno drill instrumental, 136 BPM
Production: LTX-2.3 self-hosted, ~$2.50/video all-in

WHAT TO COPY
- News-pegged drill format (within 24-48hr of news beat)
- LEGO + drill + political satire crossover
- Quoted lyric in title

WHAT NOT TO COPY
- Specific song lyrics from source
- Specific named characters

VARIANTS — Type 4 outliers don't replicate well; 1-2 follow-ups max before audience saturates novelty.
```

### Example 3 — Topic ideation flow (the personal-finance authority channel needs next 3 videos)

Step 1 — Channel's outliers: $246K (3.2× median), $160K phone call (2.8× median), CONFIRMED May 1 (4.1× median).
Step 2 — Niche outliers in last 30 days: similar dollar-counter-narrative pattern dominates.
Step 3 — Pattern overlap: dollar-specific counter-narrative is the channel's strongest live pattern.
Step 4 — Candidates generated:
  1. "$184,000 In Tax Penalties Avoided By Filing One Form (Almost Nobody Uses It)"
  2. "$73,000 Gone By Filing IRA Withdrawals In Order Most CPAs Recommend"
  3. "$192,000 Lost By Taking Medicare Part B At 65 (Most Seniors Don't Know This)"
  4. "Why The Treasury Just Changed The Rule On 401(k) Withdrawals (Effective May 15)"
Step 5 — Validation:
  - All 4 pass Gate 1 (title gut-check)
  - All 4 pass Gate 2 (thumbnail feasible — P2 rage stamp pattern)
  - Candidates 1, 3, 4 pass Gate 3 (5-promise stack feasible). Candidate 2 fails — only 3 specific promises available.
  - All passing pass Gate 4 (production cost vs predicted outcome — the personal-finance authority channel's all-in production is ~$15-25 per episode).
Step 6 — Ranking: Candidate 4 ranks highest (news-peg + freshness + the channel's strongest format).
Step 7 — Surface to user: present top 3 (4, 1, 3) with reasoning.

---

## 13. Cross-skill connections

This skill is upstream of every Skill 01-11. When a user asks "what should I make next?":

1. This skill (12) generates the topic candidates.
2. **Skill 01** generates titles for the chosen topic using the lift drivers extracted in §6.
3. **Skill 02** generates thumbnail per the pattern extracted in §6.
4. **Skill 03** writes the script using the voice archetype from §6.
5. **Skill 04** writes the description.
6. **Skills 05-11** handle reference channels, storyboarding, i2v, voice TTS, image gen, captions, mix.

When an upstream skill asks "what topic?" the answer always comes from Skill 12. Skill 12 is the first move; Skills 01-11 execute.

When a downstream skill (say Skill 03) struggles to find 5 specific promises for a topic, the failure is upstream: the topic didn't pass Gate 3. The fix is to revisit Skill 12 and pick a different topic, NOT to push the failing topic through Skill 03 anyway.

---

## 14. Runtime checklist

Before any topic recommendation surfaces:

- [ ] Channel's own outlier data pulled and classified
- [ ] Niche-relative outlier data pulled from at least 3 sources
- [ ] Pattern overlap identified between channel and niche outliers
- [ ] 12-15 candidates generated using replicable patterns
- [ ] Each candidate validated against 4 gates (title gut-check, thumbnail feasibility, 5-promise stack, production cost)
- [ ] Top 3 candidates ranked by `outlier_probability × outlier_multiple × niche_RPM / production_cost`
- [ ] Top 3 surfaced to user with source outlier citation, expected multiple, production estimate
- [ ] Differentiation rule applied (2-3 deliberate variations from source pattern)
- [ ] Anti-patterns audited (especially recency-blind median, single-source outlier data, crossover dependence)

If any check fails, the candidate does not surface. Production budget is the scarce resource — better to surface 3 strong candidates than 10 mixed-quality ones.
