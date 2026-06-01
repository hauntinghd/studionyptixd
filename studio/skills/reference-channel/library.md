# Skill 05 — Reference Channel Library

Companion to `05-reference-channel-ingestion.md`. This file is a pre-built library of Channel Profiles for well-known YouTube channel archetypes users reference constantly. The agent loads these instantly instead of running 8-minute ingestion runs.

**When to use this library:**
- User says "make it like [archetype]" and the archetype is in this library → load profile instantly, no API calls
- User mentions a channel as inspiration → pull the matching archetype profile + cite back to user for confirmation
- Agent needs a worked example of a Channel Profile structure → reference these

**When NOT to use:**
- User wants their channel to be like a small/niche channel not represented here → run real ingestion (Skill 05 main protocol)
- User wants the LATEST data — these profiles are archetype snapshots; for recent shifts, run fresh ingestion
- The reference channel has materially evolved since profile date → refresh

Each profile is intentionally structured as the canonical 12-section Channel Profile from Skill 05 §4. Downstream skills (titles, thumbnails, scripts, etc.) consume the relevant sections.

Profile date: April 2026. Refresh quarterly.

---

## Profile 1 — a long-form mystery-documentary channel

History/mystery channel. Single faceless narrator. Long-form (15-60 min) explainers on historical mysteries, conspiracies, and unsolved cases.

```
Channel archetype: a long-form mystery-documentary channel
Niche: History / mystery / explainer
Profile date: April 2026
Sample size: 12 videos analyzed (mix of recent + classics)
Confidence: high

—— SECTION 1: META ——
Sub count: large (multi-million tier)
Upload cadence: extremely slow (1-2 videos per year, 30-60 min each)
Production sophistication: extremely high
Faceless: yes — narration only, host never appears

—— SECTION 2: VOICE DNA ——
Archetype match: V1 Documentary Authority
Pace: slow-deliberate, ~140 wpm
Sentence rhythm: long unfolding, multi-clause, occasional short punctuation
Vocabulary level: educated specialist (translates terms in passing)
Emotional register: calm curiosity, never excited, occasional dry wit
Signature transitions: "And so..." / "But what's interesting..." / "What we know is..." / "Years later..."
Signature openers: opens with a specific date, place, or seemingly mundane detail that becomes the through-line
Taboo words: "guys," "epic," "insane," any hype-stack vocabulary
Recurring references: cites specific historical primary sources, named eyewitnesses, period documents
Audience relationship: equal-intelligent peer, never talks down
Humor register: dry, occasional, never the goal

—— SECTION 3: TITLE DNA ——
Median character count: 50-65
Top 3 formulas: F1 Em-Dash Mystery (50%), F8 Question (25%), F7 Forensic (15%)
ALL CAPS frequency: 0% — never uses caps
Em-dash frequency: 65%
Power words: "vanished," "uncovered," "the truth about," "what happened," "the case of"
Common villains: rarely names individuals — focuses on cases, not perpetrators
Number-formula preference: avoids numbers except specific dates

—— SECTION 4: THUMBNAIL DNA ——
Dominant pattern: P4 Mystery Object (90%) + occasional P11 Photoreal Composite
Anchor color: warm sepia (#8B6F47 or similar warm amber-brown)
Background tone: dark moody atmospheric
Face presence: 0%
Recurring props: archival objects (maps, photographs, documents, vintage equipment)
Composition rule: rule of thirds with single dominant object
Visual treatment: deep cinematic atmosphere, warm sepia grade, slight film grain, hint of vignette
Text on thumbnail: minimal or none — image carries the click

—— SECTION 5: SCRIPT STRUCTURE DNA ——
Hook architecture: H1 Forensic — opens with date + place + specific detail
Average video length: 25-50 minutes
Beat sequence: cold open with specific detail → setup → complication unfolds → multi-section investigation → resolution or unresolved acceptance
Pattern interrupts cadence: every 90-120 seconds (slower than most channels)
Open loops: heavy — the channel plants questions early that resolve much later
CTA placement: extremely minimal, often at very end and brief

—— SECTION 6: DESCRIPTION DNA ——
First-125 pattern: brief context restating premise + tease
Chapter cadence: every 3-5 minutes (longer chapters than most)
Lead magnet: none traditionally
Affiliate density: occasional sponsor (Brilliant, Squarespace) integrated cleanly
Hashtag pattern: #history #mystery + topic-specific

—— SECTION 7: VISUAL STYLE DNA ——
Aspect ratio: 16:9
Color grade: warm sepia archival
Lighting: cinematic moody, single key light, dramatic shadows
Editing pace: slow, ~12-15 cuts/min (way below average)
Signature visual treatments: archival photo restoration animations, slow zooms on still images, period footage mixed with modern shots
Use of stock footage: heavy, mixed with original imagery

—— SECTION 8: PACING DNA ——
Cuts per minute (median): 12-15
Music density: continuous atmospheric BGM, custom-composed
Silence permitted: yes, occasional reverent silence
Transition style: cross-fade dominant, occasional match cut, no hard cuts to surprise

—— SECTION 9: CTA DNA ——
Lead magnet: none
Affiliate strategy: minimal — occasional sponsor read at very end, never interrupting flow
Subscribe ask placement: end only, brief
Share prompt placement: rare to none
End screen style: minimal, related videos only

—— SECTION 10: BRAND IDENTITY ——
Logo / watermark: subtle text mark, lower-left occasionally
Channel handle: standard short handle
Color anchor: warm sepia (#8B6F47)
Audio signature: original orchestral BGM, distinctive
Recurring characters: none (single narrator voice only)

—— SECTION 11: ANCHOR vs SWAP-ABLE ——
ANCHORS (do NOT copy directly, differentiate):
- Warm sepia color anchor → user gets warm amber-orange variant or different earth tone
- Single object thumbnail composition → user uses single object but mirrored composition
- "And so..." transition → user uses "And then..." or "What followed was..."
- Slow pace, long sentences → keep the rhythm but with user's vocabulary
- Faceless narration → if user has on-camera presence, this conflicts; flag mismatch
- Original BGM → user uses different but similarly atmospheric music
- Custom orchestral → user uses Suno-generated or library equivalent

TENDENCIES (use freely):
- Date-led openers
- Mystery-gap chapter titles
- Multi-section investigation arc
- Atmospheric music throughout
- Archival photo animation style

—— SECTION 12: OUTLIER NOTES ——
This archetype's outliers (>3× channel median) correlate with:
- Topics that are unsolved or contested (vs settled)
- Personal stakes attached to a single victim
- Lengths of 30+ minutes (longer outperforms shorter for this audience)
- Cinematic recreation segments mixed with archival
```

**Application notes:** This archetype is the gold standard for cinematic faceless documentary. Users who reference it almost always want the visual aesthetic + the slow narrative pacing + the calm authority voice. The agent should warn users that this archetype's typical upload cadence (1-2 videos/year) is unusual and not replicable — the user's channel needs a faster cadence with retained quality. Common adaptation: same aesthetic, 10-min videos, weekly cadence.

---

## Profile 2 — an economic-explainer channel

Finance authority channel. Single host face. Long-form (8-15 min) finance explainers focused on systemic critique.

```
Channel archetype: an economic-explainer channel
Niche: Finance / business / systemic critique
Profile date: April 2026
Sample size: 15 videos (mix of recent + outliers)
Confidence: high

—— SECTION 1: META ——
Sub count: large (multi-million tier)
Upload cadence: 1-2 per week
Production sophistication: high
Faceless: no — host appears on camera

—— SECTION 2: VOICE DNA ——
Archetype match: V8 Erudite Professor
Pace: slightly fast, ~165 wpm
Sentence rhythm: long clause-heavy, often three-four clauses joined with commas
Vocabulary level: specialist, defines terms but doesn't apologize for them
Emotional register: confident, slightly British in feel, "lecturing"
Signature transitions: "What is interesting is..." / "Curiously..." / "This is to say..." / "It follows that..."
Signature openers: counterintuitive premise that the rest of the script supports
Taboo words: slang, hype, anything that breaks lecture register
Recurring references: studies, papers, named economists, historical context
Audience relationship: senior professor to interested student
Humor register: wry, infrequent, almost always at the expense of an institution

—— SECTION 3: TITLE DNA ——
Median character count: 55-70
Top 3 formulas: F11 Bracketed Tag (40%), F2 Specific Dollar Number (30%), F12 ALL CAPS Authority (20%)
ALL CAPS frequency: ~30%
Em-dash frequency: 65%
Power words: "Genius Strategy," "Honest Truth," "Real Reason," "How [Big Thing] Works"
Common villains: Wall Street, big banks, named CEOs, government agencies
Number-formula preference: specific dollar amounts in bracketed tag

—— SECTION 4: THUMBNAIL DNA ——
Dominant pattern: P1 Face+Arrow+Number (60%) + P2 Rage Stamp (25%)
Anchor color: yellow (#FFD700) on dark navy (#0A1428)
Background tone: dark navy
Face presence: 100% — host on left third
Face emotion: accusation register, slight intensity
Composition rule: rule of thirds, face left, arrow right at object
Visual treatment: cinematic dark grade, subtle vignette, yellow text dominant

—— SECTION 5: SCRIPT STRUCTURE DNA ——
Hook architecture: H4 99% Framing OR H2 Credentialed Subversion
Average video length: 8-12 minutes
Beat sequence: counterintuitive premise → context → mechanism explained → implications → outrage at system → next-video tease
Pattern interrupts cadence: every 60-90 seconds
CTA placement: middle (sponsor read) + end (subscribe)

—— SECTION 6: DESCRIPTION DNA ——
First-125 pattern: provocative premise + specific stake
Chapter cadence: every 1-2 minutes (frequent chapters)
Lead magnet: rarely
Affiliate density: heavy — sponsor reads + multiple affiliate links
Hashtag pattern: channel-specific tag + topic

—— SECTION 7: VISUAL STYLE DNA ——
Aspect ratio: 16:9
Color grade: cinematic dark, navy + yellow accents
Lighting: studio cinematic for host, atmospheric for B-roll
Editing pace: fast, ~25-30 cuts/min
Signature visual treatments: stock chart overlays, dollar bill graphics, government building silhouettes, named-CEO photos
Use of stock footage: heavy

—— SECTION 8: PACING DNA ——
Cuts per minute: 25-30
Music density: continuous urgent/dramatic BGM
Silence permitted: rarely
Transition style: hard cuts dominant, occasional match cut

—— SECTION 9: CTA DNA ——
Lead magnet: occasional
Affiliate strategy: heavy — multiple affiliates per video
Subscribe ask placement: mid + end
End screen style: standard subscribe + next video

—— SECTION 10: BRAND IDENTITY ——
Logo / watermark: channel wordmark
Color anchor: yellow + navy
Audio signature: dramatic stinger intro
Recurring characters: none beyond host

—— SECTION 11: ANCHOR vs SWAP-ABLE ——
ANCHORS:
- Yellow + navy color combo → user shifts to different two-color combo (e.g., red + black, or orange + dark green)
- Erudite Professor voice → keep architecture, vary specific phrasings
- Face-pointing-at-object thumbnail composition → user uses face but mirrored or different prop
- "Genius Strategy" bracketed pattern → user invents own bracketed signature
- Stock-chart overlay aesthetic → user uses different visual evidence style

TENDENCIES:
- Counterintuitive premises
- Sponsor-read placement
- ~10-min length
- Heavy use of named institutions

—— SECTION 12: OUTLIER NOTES ——
Outliers correlate with:
- Specific named villain (CEO, company, agency)
- Bracketed parenthetical title pattern
- Dollar-amount in title
- Topics with personal-finance stakes (vs abstract economics)
```

**Application notes:** This archetype is the canonical reference for "finance YouTube authority with personality." For a personal-finance authority channel, the agent uses this archetype as ACCENT (not primary) since the personal-finance authority channel has different voice DNA (V2 Federal Credentialed Expert vs V8 Erudite Professor). The accent influence: bracketed-tag titles + yellow accent on existing red palette + specific-dollar-amount thumbnails.

---

## Profile 3 — a science-explainer channel

Science channel. Host appears on camera. Long-form (10-25 min) science explainers with experimental/practical demonstrations.

```
Channel archetype: a science-explainer channel
Niche: Science / education / explainer
Profile date: April 2026
Sample size: 12 videos
Confidence: high

—— SECTION 1: META ——
Sub count: very large (10M+ tier)
Upload cadence: weekly to bi-weekly
Production sophistication: very high
Faceless: no — host appears on camera

—— SECTION 2: VOICE DNA ——
Archetype match: V1 Documentary Authority + V12 Curious Kid Adult hybrid
Pace: mid, ~155 wpm
Sentence rhythm: mixed, often question-led
Vocabulary level: educated, occasionally elevated when explaining
Emotional register: wonder + analytical curiosity
Signature transitions: "So I started wondering..." / "Which led me to ask..." / "And the answer turns out to be..." / "But the math says..."
Signature openers: question or "what if" the voice has been mulling
Taboo words: anything cynical, anything dismissive
Recurring references: peer-reviewed studies, named scientists, hands-on experiments, expert interviews
Audience relationship: smart curious peer ready to explore
Humor register: warm, self-deprecating

—— SECTION 3: TITLE DNA ——
Median character count: 45-60
Top 3 formulas: F8 Question (50%), F1 Em-Dash Mystery (25%), F13 How-To (15%)
ALL CAPS frequency: 0%
Em-dash frequency: 35%
Power words: "Why," "How," "What if," "The math," "Why nobody"
Common villains: rarely — focuses on phenomena not actors

—— SECTION 4: THUMBNAIL DNA ——
Dominant pattern: P6 Authority Portrait (50%) + P4 Mystery Object (30%)
Anchor color: muted navy/cream — professional but not corporate
Background tone: clean, occasional atmospheric
Face presence: ~70% — host pointing or with object
Face emotion: curiosity, intellectual surprise
Composition rule: rule of thirds, often face + relevant object
Visual treatment: clean cinematic, sharp, well-lit but not over-graded

—— SECTION 5: SCRIPT STRUCTURE DNA ——
Hook architecture: H4 99% Framing OR H8 Question-led
Average video length: 10-25 minutes
Beat sequence: question + intuition + experiment + counter-intuition + resolution
Pattern interrupts cadence: every 90 seconds
CTA placement: end only, often Brilliant sponsor

—— SECTION 6: DESCRIPTION DNA ——
First-125 pattern: question or counterintuitive premise
Chapter cadence: every 2-4 minutes
Lead magnet: rare
Affiliate density: minimal — usually one sponsor (Brilliant)
Hashtag pattern: #science #physics + topic

—— SECTION 7: VISUAL STYLE DNA ——
Aspect ratio: 16:9
Color grade: clean cinematic, slight warmth
Lighting: cinematic three-point on host, available on experiments
Editing pace: mid, ~18-22 cuts/min
Signature visual treatments: visualization animations (often custom), experiment footage, whiteboard/equation overlays
Use of stock footage: minimal — most footage is original

—— SECTION 8: PACING DNA ——
Cuts per minute: 18-22
Music density: subtle BGM, swells on reveals
Silence permitted: yes, used for emphasis
Transition style: clean cuts, occasional match cut on visual continuity

—— SECTION 9: CTA DNA ——
Lead magnet: rare
Affiliate strategy: typically one sponsor (Brilliant) at end
Subscribe ask: minimal end mention

—— SECTION 10: BRAND IDENTITY ——
Logo / watermark: subtle channel logo
Color anchor: muted navy/cream
Audio signature: subtle BGM, no signature stinger

—— SECTION 11: ANCHOR vs SWAP-ABLE ——
ANCHORS:
- Question-led titles → user uses question form but different question structure
- Host's face pointing at object → user uses face + object but mirrored composition
- Wonder voice → keep register, different specific phrasings
- Custom animations → user uses different animation style or stock equivalent
- Brilliant sponsor → different sponsor

TENDENCIES:
- 10-25 min length
- Hands-on experiments
- Counter-intuitive premises
- Single-sponsor model
```

**Application notes:** This archetype is the reference for "science YouTube done with personality." For science channels, the agent uses this archetype as primary. The wonder voice register is a strong differentiator from the dry documentary authority of the long-form mystery-documentary archetype — users should pick consciously which they want.

---

## Profile 4 — a top viral-challenge creator

Hype/entertainment channel. Highest production value on YouTube. High-energy stunts, challenges, and giveaways.

```
Channel archetype: a top viral-challenge creator
Niche: Entertainment / hype / stunts
Profile date: April 2026
Sample size: 12 videos
Confidence: high

—— SECTION 2: VOICE DNA ——
Archetype match: V6 Hype Showman
Pace: fast-urgent, ~180 wpm
Sentence rhythm: short, punchy, almost no pauses
Vocabulary level: plain conversational, slang accepted
Emotional register: high energy throughout
Signature transitions: "And then..." / "You won't believe..." / "Wait until you see this..." / "But here's the crazy part..."
Signature openers: extreme moment of the video, then rewinds
Taboo words: complex vocabulary, subjunctive mood, anything that slows pace
Recurring references: previous videos, escalating stakes
Audience relationship: friend who's just done something wild
Humor register: chaotic, broad

—— SECTION 3: TITLE DNA ——
Median character count: 35-55
Top 3 formulas: F2 Specific Dollar Number (60%), F11 Bracketed Tag (20%)
ALL CAPS frequency: ~25%
Em-dash frequency: 30%
Power words: "$", specific dollar amounts
Common villains: none — challenges, not villains

—— SECTION 4: THUMBNAIL DNA ——
Dominant pattern: P5 Reaction Face shifted to closed-mouth intensity (post-2024 doctrine)
Anchor color: saturated multi-color, often red+yellow+blue
Face presence: 100% — host
Face emotion: closed-mouth intensity (per the host's stated 2024 shift)
Composition rule: rule of thirds, face dominant
Visual treatment: extremely high contrast, saturated, professional polish

—— SECTION 5: SCRIPT STRUCTURE DNA ——
Hook architecture: H3 Threat Alert + H6 (extreme stakes) hybrid
Average video length: 10-25 minutes
Beat sequence: extreme moment open → setup → escalating challenges → climax → reveal
Pattern interrupts cadence: every 8 seconds (the channel's stated rule)
CTA placement: end + occasionally mid

—— SECTION 11: ANCHOR vs SWAP-ABLE ——
ANCHORS:
- $1M+ stakes → don't replicate; use proportional but different stakes
- The specific host face → never copy
- Extreme production budget → don't replicate; signal energy through pacing not budget
- Saturated multi-color thumbnails → user uses fewer colors but still saturated

TENDENCIES:
- 8-second pacing rule
- Specific dollar amounts
- Closed-mouth intensity faces
```

**Application notes:** This archetype is the wrong reference for almost every channel. The scale, budget, and face are non-replicable. Users who say "make it like this archetype" usually mean they want the ENERGY and PACING. The agent should redirect: "This archetype's specific look and stakes aren't reproducible at typical budgets. What we CAN steal is the 8-second pacing rule, the specific-number titles, and the closed-mouth intensity thumbnail register. Want to match those elements?"

---

## Profile 5 — an investigative-journalism channel

Investigation/scam-exposure channel. Single host. Long-form (15-30 min) investigative deep-dives on crypto scams, influencer fraud, financial deception.

```
Channel archetype: an investigative-journalism channel
Niche: Investigation / scam exposure
Profile date: April 2026
Sample size: 12 videos
Confidence: high

—— SECTION 2: VOICE DNA ——
Archetype match: V4 Skeptical Investigator
Pace: fast-mid, builds momentum
Sentence rhythm: question/answer alternation, building case
Vocabulary level: plain to specialist
Emotional register: pursuit, skepticism, earned outrage
Signature transitions: "But something didn't add up..." / "So I started digging..." / "And then I found this..." / "Here's what nobody is telling you..."
Signature openers: specific anomaly or unanswered question
Taboo words: hype, generic hooks, guru language
Recurring references: documents, screenshots, transcripts, named individuals
Audience relationship: detective with assistant
Humor register: dry, occasional, when absurdity warrants

—— SECTION 3: TITLE DNA ——
Median character count: 50-65
Top 3 formulas: F5 Named Villain (50%), F2 Specific Dollar Number (30%), F1 Em-Dash Mystery (15%)
ALL CAPS frequency: ~20% (single emphasis words)
Em-dash frequency: 50%
Power words: "exposed," "scam," named individuals, dollar amounts
Common villains: named scam artists, crypto influencers, fraudsters
Number-formula preference: dollar amounts of fraud

—— SECTION 4: THUMBNAIL DNA ——
Dominant pattern: P2 Rage Stamp (60%) + P5 Reaction Face accusation variant (30%)
Anchor color: red (#D32F2F) on dark
Background tone: dark
Face presence: ~80% — host in accusation register
Face emotion: accusation, intense focus
Composition rule: face left, named villain or evidence right
Visual treatment: high-contrast, red dominant, slight cinematic dark grade

—— SECTION 5: SCRIPT STRUCTURE DNA ——
Hook architecture: H1 Forensic + H5 Named Villain hybrid
Average video length: 15-30 minutes
Beat sequence: hook with named villain + specific anomaly → investigation setup → evidence chain → reveal → implication → CTA
Pattern interrupts cadence: every 60-90 seconds
CTA placement: end (Patreon, newsletter)

—— SECTION 9: CTA DNA ——
Lead magnet: occasional newsletter
Affiliate strategy: minimal — primarily Patreon supported
Subscribe ask: end

—— SECTION 11: ANCHOR vs SWAP-ABLE ——
ANCHORS:
- The host's face → never copy
- Red rage stamp on dark → user uses different rage color (orange, yellow on dark)
- Skeptical Investigator voice → keep architecture, vary phrasings
- Named-villain titles → user uses named villains in their own niche
- Document overlay aesthetic → keep, vary specific document types

TENDENCIES:
- 15-30 min length
- Patreon-supported model
- Named-villain hooks
- Document evidence presentation
```

**Application notes:** This archetype is the reference for any scam-exposure or investigation channel. The "named villain + specific anomaly" hook is highly portable across niches — works for finance scams, supplement scams, contractor scams, etc.

---

## Profile 6 — a deadpan-history channel

Deadpan history shorts/comedy channel. Faceless with stick-figure animation. Short-form (3-7 min) deadpan history explainers.

```
Channel archetype: a deadpan-history channel
Niche: History / deadpan comedy
Profile date: April 2026
Sample size: 12 videos
Confidence: high

—— SECTION 2: VOICE DNA ——
Archetype match: V7 Deadpan Cynic
Pace: mid, with deliberate pauses on punchlines
Sentence rhythm: mixed — sets up normal sentence, undercuts with one-liner
Vocabulary level: plain to slightly elevated
Emotional register: knowing, dry, never sincere for too long
Signature transitions: "Now obviously..." / "As you'd expect..." / "Predictably..." / "Spoiler alert..."
Signature openers: deceptively simple statement that sets up absurdity
Taboo words: sincerity, earnestness
Recurring references: historical absurdity, internet culture
Audience relationship: equal who finds everything slightly absurd
Humor register: dry, observational, self-deprecating about format

—— SECTION 3: TITLE DNA ——
Median character count: 30-50
Top 3 formulas: F1 Em-Dash Mystery, F8 Question, F11 Bracketed
ALL CAPS frequency: 0%
Em-dash frequency: 40%

—— SECTION 4: THUMBNAIL DNA ——
Dominant pattern: P12 (stylized illustrated) — stick figure on bright background
Anchor color: yellow background dominant
Face presence: stick figure character (not real face)
Visual treatment: deliberately crude stick-figure animation aesthetic

—— SECTION 5: SCRIPT STRUCTURE DNA ——
Hook architecture: H7 deceptively simple opener that escalates to absurdity
Average video length: 3-7 minutes
Beat sequence: cold open with simple statement → setup → absurd complication → resolution that maintains deadpan
CTA placement: end, brief

—— SECTION 11: ANCHOR vs SWAP-ABLE ——
ANCHORS:
- Stick figure character → user uses different but consistent simple character
- Yellow thumbnail background → user uses different bright single-color
- Deadpan voice → keep architecture
- Historical-absurdity content → user adapts to their niche

TENDENCIES:
- 3-7 min length
- Cold open
- Deadpan throughout
```

**Application notes:** This archetype is the reference for any "deadpan history shorts" or "deadpan absurdity" channel. The voice register is highly portable — works for science absurdity, business history absurdity, etc. Users who reference this archetype usually want voice + brevity, not specifically history.

---

## Profile 7 — a tech-industrial-history channel

Long-form documentary channel. Faceless. Long-form (15-40 min) deep-dives on technology, semiconductors, economics, geopolitics.

```
Channel archetype: a tech-industrial-history channel
Niche: Documentary / technology / economics
Profile date: April 2026
Sample size: 10 videos
Confidence: high

—— SECTION 2: VOICE DNA ——
Archetype match: V1 Documentary Authority
Pace: slow-deliberate
Sentence rhythm: long, clause-heavy
Vocabulary level: specialist (semiconductor / economics / geopolitics)
Emotional register: calm analytical
Signature transitions: "What followed was..." / "Curiously..." / "The result was..."
Audience relationship: equal-knowledgeable peer
Humor register: rare, dry

—— SECTION 3: TITLE DNA ——
Median character count: 50-65
Top 3 formulas: F1 Em-Dash Mystery, F8 Question, F7 Forensic
ALL CAPS frequency: 0%
Em-dash frequency: 50%

—— SECTION 4: THUMBNAIL DNA ——
Dominant pattern: P10 Color-Pop on Mono — single object dominant
Anchor color: depends on subject
Face presence: 0%
Visual treatment: editorial, often duotone

—— SECTION 5: SCRIPT STRUCTURE DNA ——
Hook architecture: H1 Forensic
Average video length: 15-40 minutes
Beat sequence: cold open + thesis + multi-part evidence chain + counter-arguments + synthesis
CTA placement: end, sponsor

—— SECTION 11: ANCHOR vs SWAP-ABLE ——
ANCHORS:
- Single-object color-pop thumbnails → user uses similar but different objects
- Slow documentary voice → keep
- Specialist vocabulary in tech/econ/geopolitics → user adapts to their domain

TENDENCIES:
- 15-40 min length
- Faceless narration
- Heavy citation
```

**Application notes:** This archetype is the reference for "long-form faceless documentary in a technical niche." Highly portable for any documentary channel. The faceless aesthetic is a feature — users who insist on having on-camera presence should reference the science-explainer archetype instead.

---

## Profile 8 — a productivity creator

Mentor coach / productivity channel. Single host face. Long-form (10-20 min) productivity, study, and lifestyle content with frameworks.

```
Channel archetype: a productivity creator
Niche: Productivity / mentor / lifestyle authority
Profile date: April 2026
Sample size: 12 videos
Confidence: high

—— SECTION 2: VOICE DNA ——
Archetype match: V5 Mentor Coach
Pace: mid-warm, conversational but structured
Sentence rhythm: often three-part structures (rule of three)
Vocabulary level: educated but plain
Emotional register: warm authority, figured-something-out
Signature transitions: "Here's the framework I use..." / "Three things changed for me..." / "The principle that matters is..."
Signature openers: personal anecdote that surfaces the lesson
Taboo words: hype vocabulary
Audience relationship: older friend who's been where you are
Humor register: warm self-deprecating

—— SECTION 3: TITLE DNA ——
Median character count: 50-65
Top 3 formulas: F4 Numbered List (40%), F13 How-To (30%), F11 Bracketed (20%)
ALL CAPS frequency: 5% (rare)
Em-dash frequency: 30%
Power words: "How I," "The framework," "5 books," "3 habits"

—— SECTION 4: THUMBNAIL DNA ——
Dominant pattern: P1 Face+Arrow+Number (50%) + P6 Authority Portrait (30%)
Anchor color: bright clean — often white or light teal
Face presence: 100%
Face emotion: warm engagement
Composition rule: face + book or framework visualization
Visual treatment: clean bright, high quality but not over-graded

—— SECTION 5: SCRIPT STRUCTURE DNA ——
Hook architecture: H6 personal anecdote that surfaces lesson
Average video length: 10-20 minutes
Beat sequence: anecdote + framework intro + three-part exposition + recap + CTA
CTA placement: mid (course/affiliate) + end (subscribe)

—— SECTION 9: CTA DNA ——
Lead magnet: heavy — newsletter, course, app
Affiliate strategy: heavy — Skillshare, own course, books

—— SECTION 11: ANCHOR vs SWAP-ABLE ——
ANCHORS:
- Bright clean aesthetic → user uses different bright palette
- Three-part frameworks → keep structure, vary specifics
- Mentor voice → keep architecture
- Personal anecdote opens → user uses own anecdotes

TENDENCIES:
- 10-20 min length
- Heavy CTA stack
- Framework-driven content
```

**Application notes:** This archetype is the reference for "productivity / mentor coach with personal brand." The Mentor Coach voice + framework content is highly replicable. Users who reference this archetype usually want the warm-authority register + the personal-anecdote hook + the three-part framework structure.

---

## Quick reference table

When user mentions any of these archetypes, the agent loads the relevant profile instantly:

| Reference archetype | Voice archetype | Best for users in niche... |
|---|---|---|
| a long-form mystery-documentary channel | V1 Documentary Authority | History / mystery / faceless documentary |
| an economic-explainer channel | V8 Erudite Professor | Finance / business / systemic critique |
| a science-explainer channel | V1 + V12 hybrid | Science / education with personality |
| a top viral-challenge creator | V6 Hype Showman | Entertainment (but warn about non-replicable scale) |
| an investigative-journalism channel | V4 Skeptical Investigator | Investigation / scam exposure |
| a deadpan-history channel | V7 Deadpan Cynic | Deadpan history shorts / absurdity |
| a tech-industrial-history channel | V1 Documentary Authority | Long-form faceless documentary |
| a productivity creator | V5 Mentor Coach | Productivity / mentor coach / personal brand |

## Roadmap for additions

This library is incomplete. Add profiles for these archetypes in future sessions when users reference them:

- A geopolitics documentary channel — investigative documentary
- A tech-review personality — tech reviews
- A logistics explainer channel — long-form aerospace/economics
- An engineering explainer channel — engineering documentary
- A science-animation channel — animated science explainers
- A short-form geopolitics/economics channel
- A science-engineering personality — engineering / curious-kid-adult
- A long-form interview show — long-form interview
- A long-form science-podcast host — long-form interview science/tech
- A real-estate / personal-finance channel
- An entrepreneurship / mentor channel (with note: controversial register)
- A productivity / wisdom creator
- A science/tech short-form explainer
- A geography/curiosities channel
- An animated learning channel

## Update log

Profile date: April 2026.

When a user references a channel, the agent first checks this library. If the profile is older than 90 days, the agent flags: "I have a profile for this archetype from April 2026 — channels evolve over time. Should I run a fresh ingestion to capture recent shifts?"

If user wants fresh data, the agent runs Skill 05 main protocol. Otherwise, loads the cached profile.
