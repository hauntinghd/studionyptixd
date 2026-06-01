---
name: broll-selection
description: >-
  B-roll three-jobs test + selection axes + per-niche density + per-beat discipline. Load when picking b-roll for a beat or grading a storyboard for visual rhythm.
---

# Tactical Playbook T6 — B-roll Selection Logic

This is the operational knowledge an AI YouTube agent needs to decide which B-roll shots belong in a video and which don't. Every paragraph is a concrete rule the agent applies.

This is a tactical playbook because the B-roll question cuts across multiple Tier-1 skills:
- **Skill 03 (Script):** the script's beat structure determines where B-roll is needed
- **Skill 06 (Storyboard):** the shot list is built from this logic
- **Skill 07 (i2v):** the per-shot prompt is downstream of "is this shot needed at all?"
- **Skill 11 (Audio Mix):** SFX cues land on B-roll appearances; ducking levels shift around them

A 25-minute long-form authority video typically has 35-45 B-roll inserts. The difference between a video that feels professional and one that feels noisy is not how many B-roll shots — it's whether each one earned its space.

---

## 1. The job of B-roll

B-roll does three things, and only three. If a shot doesn't do at least one, it's clutter.

### Job 1 — Anchor (visualize what the VO is naming)

The VO mentions a specific noun (a form, a person, a building, a number) and the B-roll shows the thing. The viewer's brain pairs the audio + visual into a single memorable representation.

**Examples:**
- VO says "Form SSA-7004"; B-roll shows the form
- VO says "Patricia walked to the mailbox"; B-roll shows Patricia walking to the mailbox
- VO says "$246,000"; B-roll shows a stack of bills

**Job 1 success criterion:** if you removed the B-roll, would the VO feel abstract? If yes, the B-roll is doing Job 1.

### Job 2 — Contrast (pivot the visual register from talking head)

After 60-90 seconds of host-on-camera, the viewer's eye fatigues. B-roll provides a register break — different angle, different subject, different lighting. The eye refreshes.

**Examples:**
- After 90 seconds of host explaining the rule, cut to a kitchen counter exterior
- After Beat 4's 4-minute composite case, cut to a brief B-roll detail before Beat 5's mechanism

**Job 2 success criterion:** does the B-roll provide a visual energy shift? If the cut happens but the energy stays the same, the B-roll didn't do Job 2.

### Job 3 — Breathe (give the script a beat for processing)

Some VO content is information-dense (numbers, mechanisms, multiple variables). Cutting to B-roll for 2-4 seconds without VO change gives the viewer's brain space to process before the next dense statement.

**Examples:**
- VO ends a long mechanism explanation; B-roll holds for 2 seconds without VO; then VO continues with the application
- After a 3-anchor pattern interrupt, B-roll holds while VO transitions to next beat

**Job 3 success criterion:** is the B-roll giving the viewer space to digest? This is the rarest of the three jobs.

### The three-jobs test

The agent runs every candidate B-roll through this:

```
Does this shot do at least one of Job 1 (Anchor), Job 2 (Contrast), Job 3 (Breathe)?
  YES → keep
  NO → cut
```

A shot that does two or three jobs simultaneously is a strong B-roll. A shot doing zero jobs is filler — and filler in a 25-minute video accumulates into "feels long."

---

## 2. The 4 selection axes

Once a B-roll has earned a job, the agent picks the SHAPE of the shot along 4 axes.

### Axis 1 — Literal vs Metaphorical

**Literal:** the shot shows the actual thing the VO names.
- VO says "Patricia at the kitchen table"; literal B-roll shows a 70-year-old woman at a kitchen table.

**Metaphorical:** the shot shows a stand-in that conveys the meaning without showing the literal thing.
- VO says "the rule cascades through every benefit"; metaphorical B-roll shows dominoes falling.

**When to pick literal:** for named subjects (Patricia, Form 7004, the SSA), specific concrete nouns. Authority register favors literal.

**When to pick metaphorical:** for abstract concepts (mechanisms, processes, time passing), when the literal thing is uncinematic (a federal regulation has no visual). Documentary register favors metaphorical.

**Default for authority niches:** 70% literal / 30% metaphorical.
**Default for documentary niches:** 50% literal / 50% metaphorical.
**Default for music video:** mostly metaphorical (the lyrics are abstract).

### Axis 2 — Simple vs Complex

**Simple:** one subject, one action, clean composition. A close-up of a single envelope.
**Complex:** multiple subjects, multiple actions, denser scene. A bank lobby with multiple people transacting.

**When to pick simple:** for brief inserts (2-3 sec), for B-roll that punctuates rather than narrates. Most B-roll should be simple.
**When to pick complex:** for establishing shots, scene-setting, atmospheric inserts. Complex B-roll holds longer (5-7 sec).

**Default ratio:** 80% simple / 20% complex. Complex B-roll is expensive (more i2v failures, more cost) and harder to read at small mobile sizes.

### Axis 3 — Tight vs Wide

**Tight:** close-up. Object fills 50%+ of frame. Letter close-up, hands counting bills, face in soft focus.
**Wide:** establishing or environmental. Subject is small relative to frame. House on the street, kitchen with mail on counter, town square.

**When to pick tight:** for emphasis (the form, the dollar, the patient's hands), for emotional weight (face close-up). Tight B-roll lands harder.
**When to pick wide:** for orientation (where are we?), for scene-setting (what's the world?). Wide B-roll establishes.

**Default rotation:** every 4-5 B-roll shots, vary the tightness. All-tight or all-wide creates monotony.

### Axis 4 — Static vs Motion

**Static:** locked camera. Subject motion only (or no motion).
**Motion:** camera moves (push-in, pull-back, truck, pan). Adds energy.

**When to pick static:** for B-roll over fast-paced VO, for evidence-style inserts (documents, products). Static B-roll lets the VO carry the energy.
**When to pick motion:** for atmospheric beats, for emotional weight, for transitions between sections. Motion B-roll signals "we're moving forward."

**Default ratio:** 70% static / 30% motion. Motion B-roll is expensive (per Skill 07 — motion verb risks) and over-using motion creates a "constant push-in" feel that reads tired.

---

## 3. The B-roll-to-VO timing relationship

The single most-misunderstood B-roll question is when the B-roll cut should happen relative to the VO. The answer:

### The 0.5-second pre-roll rule

When the VO is about to mention a specific noun, the B-roll cuts to the visual ~0.5 seconds BEFORE the VO names it. The viewer sees the visual, then hears the audio confirm what they're seeing. This produces the "of course" reflex that anchors memory.

**Example:**
- VO: "...the form Patricia needed was Form SSA-7004."
- B-roll cut to Form 7004 close-up: 0.5 sec before "Form SSA-7004" is spoken.
- Result: viewer sees the form, brain registers "form," VO confirms "SSA-7004," brain locks the audio + visual.

The opposite (B-roll cuts AFTER the VO names the thing) produces a "wait, what?" reflex — the viewer hears the noun, then has to look for the visual. Memory is weaker.

### The minimum 1-second hold rule

A B-roll cut shorter than 1 second feels like a flash. The eye doesn't have time to register the subject. Minimum 1 second; ideal 2-3 seconds.

The exception: in vertical shorts (Roblox-scenario) where pace is the product, B-roll can flash at 0.7-1 sec. But never below 0.5 sec — at that speed it's subliminal, not communicative.

### The maximum 5-second hold rule

A B-roll cut longer than 5 seconds without VO change feels like the video froze. Either:
- Have the VO continue under the same B-roll
- Cut to a related B-roll
- Cut back to the host

If a B-roll genuinely needs to hold 5+ seconds (rare — usually only for atmospheric establishing shots), add subtle motion to keep the eye engaged.

### The "no two consecutive B-rolls without VO" rule

If B-roll A ends and B-roll B starts with no VO change between them, the cut feels jarring. Either:
- Have VO bridge the cut
- Add an SFX to mark the cut intentionally
- Don't cut — extend B-roll A to where the VO bridges

This rule prevents the "cut for cuts' sake" pattern that some YouTube creators fall into.

---

## 4. Per-niche B-roll density

How much B-roll is right for a niche? Density is measured as **B-roll shots per 1,000 chars of VO script**.

| Niche | B-roll density | Total per 25-min long-form | Notes |
|---|---|---|---|
| Senior finance / IRS | 1.4 / 1,000 chars | ~35 | Authority needs anchor B-roll for forms, dollars, named patients |
| Senior health | 1.2 / 1,000 | ~30 | Patient stories + supplement visuals + clinical B-roll |
| Tech / AI dev | 0.8 / 1,000 | ~20 | Mostly host on camera + screen recordings; less B-roll |
| Documentary explainer | 1.6 / 1,000 | ~40 | Long-form mystery-documentary style heavy on visualization |
| News-hijack docs | 1.8 / 1,000 | ~45 | Document overlays + evidence cycling — densest niche |
| True crime | 1.4 / 1,000 | ~35 | Case file + photos + reconstructions |
| Cooking | 2.0 / 1,000 | ~50 | Process-heavy — every step needs visual |
| Real estate | 1.2 / 1,000 | ~30 | Property tours + market data visuals |
| Faith | 0.6 / 1,000 | ~15 | Mostly host on camera or ambient — sparse B-roll |
| Music video | N/A | 40-70 shots beat-locked | Every shot is B-roll equivalent |
| Vertical short | 1.0 / sec | 8-15 total | Different math — every shot brief |

**Cost implication:** at $0.10-0.50 per i2v generation, a 35-B-roll video is $3.50-17.50 just for B-roll generation. The agent surfaces this cost when proposing the storyboard.

**Density caveat:** density is a target, not a mandate. A particular video might genuinely need fewer B-rolls (a reflective episode that's mostly host monologue) or more (an investigation video full of evidence overlays). The density target is for typical episodes.

---

## 5. Per-beat B-roll discipline

Different beats of a script need different B-roll patterns. The agent loads the per-beat playbook.

### Beat 1 — Hook (30 sec, 4-5 B-roll inserts)

**Pattern:** establishing exterior + hook close-up + 2-3 B-roll detail shots
**Density:** 1 B-roll per 6-7 seconds — high density, fast pacing
**Job mix:** Anchor-heavy (specific items the VO names: form, letter, mailbox)
**Axis defaults:** literal + simple + tight + static
**Why:** the hook needs to land specific anchors fast. Pattern interrupts at 0:03, 0:07, 0:15, 0:30 (per Skill 03 §5) often align with B-roll cuts.

### Beat 2 — 5-Promise Stack (60 sec, 1-2 B-roll inserts)

**Pattern:** mostly host on camera; brief B-roll cycle showing 3-5 promised items
**Density:** very low — host carries this beat
**Job mix:** Anchor (the promises are specific things)
**Axis defaults:** literal + simple + tight + static
**Why:** the promise stack is rapid; B-roll can support but not dominate. Cycling B-roll under host shot works well.

### Beat 3 — Credential Intro (20 sec, 0-1 B-roll inserts)

**Pattern:** typically pure host shot
**Density:** ~zero
**Why:** this beat is identity-establishment. The host's face is the anchor.

### Beat 4 — Core Story (3:30, 8-10 B-roll inserts)

**Pattern:** establishing exterior + 2-3 composite case shots + 3-4 B-roll details + transition
**Density:** ~1 B-roll per 25 sec — moderate
**Job mix:** Anchor (Patricia, mailbox, letter) + Breathe (between dramatic moments)
**Axis defaults:** literal + simple + mixed tight/wide + mixed static/motion
**Why:** Beat 4 is the longest emotional beat. B-roll carries the patient story; viewer needs to SEE Patricia, not just hear about her.

### Beat 5 — Mechanism (3:30, 6-8 B-roll inserts)

**Pattern:** mechanism diagram + host explainer + B-roll evidence + diagram cycling
**Density:** ~1 B-roll per 30 sec
**Job mix:** Anchor (the mechanism's physical metaphor) + Breathe (after dense explanation)
**Axis defaults:** metaphorical + simple + tight + occasional motion
**Why:** mechanism is abstract; B-roll's job is to make it concrete via metaphor.

### Beat 6 — Three Examples (4:30, 9-12 B-roll inserts, 3-4 per example)

**Pattern:** 4 shots per example — establishing + composite case + B-roll detail + number reveal
**Density:** ~1 B-roll per 25 sec
**Job mix:** Anchor-heavy
**Axis defaults:** vary across examples (different demographics → different visual treatments)
**Why:** three examples need three distinct visual flavors so they don't feel repetitive.

### Beat 7 — Numbered List (5:00, 8-10 B-roll inserts)

**Pattern:** 1-2 B-roll per item — item visual + host explainer + (optional) action B-roll
**Density:** ~1 B-roll per 30 sec
**Job mix:** Anchor (each item names a specific thing)
**Axis defaults:** literal + simple + tight + static
**Why:** numbered items need single clear anchors. Don't over-design list B-roll — clarity > cinematic.

### Beat 8 — Bonus (1:30, 2-3 B-roll inserts)

**Pattern:** mechanism + host
**Density:** ~1 B-roll per 30 sec
**Job mix:** Anchor + Contrast (signal "this is the bonus")
**Why:** bonus needs slight visual differentiation from numbered list — different shot type or grade.

### Beats 9-10 — CTAs (90 sec, 2-3 B-roll inserts)

**Pattern:** host + lead magnet visual + affiliate visual
**Density:** moderate
**Job mix:** Anchor (lead magnet visual, product visual)
**Why:** CTAs need clear visualization of what's being offered.

### Beats 11-12 — Close + Share (2:00, 2-3 B-roll inserts)

**Pattern:** host + B-roll callback to Beat 4 + share visual
**Density:** low
**Job mix:** Contrast (callback to opening creates closure)
**Why:** the close benefits from a callback — viewer remembers Beat 4's emotional anchor.

---

## 6. The B-roll fatigue threshold

When does B-roll become noise? Three signals:

### Signal 1 — More than 6 cuts per 30-second window

Even fast-paced channels feel choppy beyond 6 cuts in 30 sec. Most authority channels max out at 4-5 cuts in 30 sec.

### Signal 2 — Three consecutive B-roll inserts of the same axis

Three tight close-ups in a row, or three motion shots in a row, feel monotonous. Vary the axes.

### Signal 3 — B-roll outweighs host time

If a long-form authority video has < 25% host-on-camera time, the channel's expert credibility erodes. The host's face is the credential. Even when B-roll carries scene, return to host for grounding.

The agent runs a fatigue audit during storyboard:
- Count B-roll cuts per 30-sec rolling window
- Verify axis variation across rolling 4-shot windows
- Compute total host-on-camera time vs total B-roll time

If any threshold trips, restructure.

---

## 7. The B-roll source priority

When the agent needs a B-roll shot, it picks from sources in this order:

### Priority 1 — Channel's existing B-roll library

Many channels have shot or generated B-roll over previous episodes that's reusable. The agent indexes the library; if a relevant shot exists, reuse.
**Cost:** $0
**Risk:** over-reuse of identifiable shots can read repetitive (post-July-2025 inauthentic content concern)

### Priority 2 — Stock footage (Pexels, Storyblocks, Artgrid)

Stock has the advantage of guaranteed quality and zero generation cost. Especially good for establishing exteriors, cityscapes, generic environments.
**Cost:** $0 (Pexels) to $10-30/clip (premium stock)
**Risk:** stock footage is recognizable — "I've seen that drone shot of Manhattan before" breaks immersion

**Pexels is wired into the sandbox** — search Pexels Videos directly over the proxy. The API key is injected automatically by the proxy; do NOT pass a key yourself.

```bash
# Search Pexels Videos for a scene matching the beat
curl -s "$PEXELS_API_BASE_URL/videos/search?query=<scene description>&orientation=landscape&min_duration=4&per_page=15" \
  | jq -r '.videos[] | "\(.id)\t\(.duration)s\t\(.url)\t\([.video_files[] | select(.quality=="hd")][0].link)"'

# Download the chosen HD file
curl -sL "<chosen hd video_files link>" -o runs/<runId>/broll-01.mp4
```

Query params: `query` (scene description — keep it concrete, e.g. `elderly woman reading mail kitchen`), `orientation` (`landscape`/`portrait`/`square`), `min_duration` (seconds), `per_page` (≤80). Pick the `hd` entry from `video_files`; fall back to the first file if no HD. Pexels videos are royalty-free for commercial use — attribution recommended but not required. Avoid the obvious stock (generic NYC drone shots, stock handshakes) — prefer lesser-known clips so the video doesn't read as stock.

### Priority 3 — i2v generation (Skill 07)

Generated B-roll is custom-made for the script's specific anchor. Best for named subjects (Patricia walking to mailbox), specific objects (Form 7004 close-up), and any visual the channel hasn't sourced.
**Cost:** $0.05-0.50/clip depending on model and duration
**Risk:** quality variance — some generations fail and need retry

### Priority 4 — Real footage (filmed)

For channels with on-the-ground capability. Highest quality but highest production cost.
**Cost:** time + travel + equipment
**Risk:** scheduling

The agent picks per shot — most B-roll for AI-driven channels lands on Priority 3 (i2v generation). Real-footage hybrid channels (medical-authority) lean Priority 1 + 4.

### Specific source footage from a YouTube video

Some videos need a *specific* real clip you can't substitute with stock or generation — the actual incident/arrest/news footage a story is about. Download it through the managed API, never by scraping YouTube directly:

```bash
# 1. Request — format is a resolution number: 1080 / 720 / 360 for video, or mp3 for audio (NOT "mp4").
RESP=$(curl -s "$YT_DOWNLOAD_API_BASE_URL/ajax/download.php?format=720&add_info=0&url=<youtube_url>")
PROG=$(echo "$RESP" | jq -r .progress_url)
# 2. Poll progress (on p.savenow.to — fetch directly, no proxy/key) until the file is ready
for i in $(seq 1 60); do
  P=$(curl -s "$PROG"); DL=$(echo "$P" | jq -r '.download_url // empty')
  [ -n "$DL" ] && break; sleep 3
done
# 3. Fetch the file directly (a worker*.savenow.to link — not IP-bound, no proxy)
curl -L "$DL" -o runs/<runId>/source-01.mp4
```

The api key is injected by the proxy — do NOT pass one. **Never run `yt-dlp`/`youtube-dl` or curl `youtube.com`/`googlevideo.com`**: the sandbox IP is bot-blocked and it always fails — don't waste a turn improvising Invidious/Cobalt/player-client workarounds. Treat downloaded third-party footage as copyrighted: keep usage transformative/fair-use and run `compliance-preflight` before shipping.

---

## 8. Anti-patterns

Eight B-roll mistakes that cost retention or production budget.

### Anti-pattern 1 — B-roll for B-roll's sake

The shot doesn't pass the three-jobs test (§1) but is included because "B-roll is needed here." The shot doesn't anchor, contrast, or breathe — it just fills time.
**Fix:** if a shot can't pass the three-jobs test, replace with extended host time or remove.

### Anti-pattern 2 — Stock-iest stock footage

Generic drone shot of New York City. Generic person typing on laptop. Generic stock smile-and-handshake. The viewer recognizes the stock and disengages.
**Fix:** if stock is needed, pick lesser-known clips. Or generate via i2v with channel-specific styling.

### Anti-pattern 3 — Text rendered into B-roll image

Form 7004 close-up where the model rendered "FORM SSA-7004" with warped letters on the image. AI-content tell.
**Fix:** generate the form as blank document; add text overlay in code. Same text-in-code rule from Skills 02 and 09.

### Anti-pattern 4 — Wrong axis for beat

Beat 4's emotional core story uses fast motion B-roll with whip pans. Energy mismatch — Beat 4 needs intimate weight, not action.
**Fix:** consult §5 per-beat B-roll discipline for axis defaults.

### Anti-pattern 5 — Holding B-roll past 5 seconds with no VO change

A beautiful establishing shot held for 8 seconds while VO has paused. Viewer assumes the video froze.
**Fix:** maximum 5-second hold rule (§3). Either resume VO under the shot or cut.

### Anti-pattern 6 — B-roll cuts AFTER the VO names the noun

VO says "Form SSA-7004"; B-roll cuts to the form 0.5 seconds later. Audio + visual don't lock; memory is weak.
**Fix:** 0.5-second pre-roll rule (§3). Cut to the visual BEFORE the VO names it.

### Anti-pattern 7 — Same axis 4+ times in a row

Eight consecutive tight close-ups. Or eight consecutive motion shots. Eye fatigues.
**Fix:** vary axes every 3-4 shots. The shot bank patterns in Skill 06 companion §5 already balance this.

### Anti-pattern 8 — More B-roll than host time in authority content

Authority video's host appears on camera < 25% of the time. Credibility erodes; viewer's relationship is with B-roll, not the expert.
**Fix:** authority niches need ≥ 30% host-on-camera time. If storyboard shows less, restructure.

---

## 9. Worked examples

### Example 1 — Personal-finance authority Beat 4 B-roll selection

Beat 4 is Patricia's core story. Per §5 Beat 4 discipline: 8-10 B-roll inserts.

**B-roll candidates and decisions:**

| # | Candidate | Three-jobs test | Axes | Decision |
|---|---|---|---|---|
| 1 | Establishing exterior of Patricia's condo | Anchor + Contrast | literal/simple/wide/motion | KEEP |
| 2 | Patricia walking from porch to mailbox | Anchor + Breathe | literal/simple/wide/motion | KEEP |
| 3 | Mail close-up on counter | Anchor | literal/simple/tight/static | KEEP |
| 4 | Letter unfolding (paper-physics) | Anchor + Breathe | literal/simple/tight/motion | KEEP |
| 5 | Patricia walking out front door | Anchor | literal/simple/wide/motion | KEEP |
| 6 | Calendar with March 11 marked | Anchor | literal/simple/tight/static | KEEP |
| 7 | ATM screen showing low balance | Anchor + Breathe | literal/simple/tight/static | KEEP |
| 8 | Patricia at ATM, expression of shock | Anchor (composite case) | literal/simple/tight/static | KEEP |
| 9 | Generic shot of money flying away | Three-jobs test FAIL — metaphorical fluff that doesn't anchor specifically | KEEP→CUT |
| 10 | Generic stock kitchen footage | Three-jobs test FAIL — already establishing in shot 1 | KEEP→CUT |

Final: 8 B-roll shots survive. Two filler shots cut.

### Example 2 — Beat 7 numbered list B-roll discipline

Beat 7 has 5 numbered items × 5:00 total. Per §5: ~8-10 B-roll inserts (1-2 per item).

**Per-item pattern:**
- Item visual (literal/simple/tight/static, 5-10 sec)
- Host explainer (host on camera, 30-45 sec)
- (Optional) action B-roll (literal/simple/tight/static, 5-10 sec)

For item 2 (Form SSA-7004):
- Item 2 visual: Form 7004 close-up, paper-physics subtle (literal/simple/tight/static, 8 sec)
- Host explainer: host walking through filing
- Action B-roll: hand using pen to fill the form (literal/simple/tight/static, 6 sec)

This pattern repeats per item. Result: 10 B-roll shots for Beat 7.

### Example 3 — Documentary explainer Beat 4 (heavier metaphorical mix)

Documentary niche default: 50% literal / 50% metaphorical.

For long-form mystery-documentary style 1518 Strasbourg dancing plague Beat 4:
- Establishing: cobblestone street in old Strasbourg, motion shot (literal/wide/motion)
- Wide shot: 400 figures dancing in plaza (metaphorical reconstruction)
- Tight shot: bare feet on cobblestones (metaphorical, evokes the dancing)
- Document close-up: 1518 chronicle page (literal/tight/static)
- Time-lapse: shadow moving across square (metaphorical, evokes time passing)
- Reconstruction: a single dancer collapsing (metaphorical, evokes the deaths)
- Document: doctor's notes (literal/tight/static)

Mix: 3 literal, 4 metaphorical. Documentary register honored.

---

## 10. Cross-skill connections

This tactical playbook is loaded by:
- **Skill 03 (Script):** when writing the script, the agent considers where B-roll will land. A script paragraph that doesn't lend itself to B-roll anchor gets revised.
- **Skill 06 (Storyboard):** the storyboard cites this playbook for B-roll selection during shot list construction.
- **Skill 07 (i2v):** the per-shot prompt is downstream of "is this shot needed." This playbook answers the upstream question.
- **Skill 11 (Audio Mix):** SFX cues land on B-roll appearances per the SFX bank — the B-roll selection determines where SFX cues go.

The agent loads T6 alongside Skill 06 when storyboarding any long-form video. For vertical shorts and music videos, the per-niche density tables in §4 apply but the per-beat discipline in §5 may not — those formats use different beat structures.

---

## 11. Runtime checklist

Before locking the storyboard's B-roll list:

- [ ] Every B-roll shot passes the three-jobs test (§1) — Anchor / Contrast / Breathe
- [ ] B-roll axes vary every 3-4 shots (§2)
- [ ] B-roll cuts use the 0.5-second pre-roll rule (§3)
- [ ] No B-roll under 1 second (except vertical shorts at 0.7-1)
- [ ] No B-roll over 5 seconds without VO change
- [ ] No two consecutive B-rolls without VO bridge
- [ ] B-roll density matches niche target (§4)
- [ ] Per-beat B-roll discipline matches §5 patterns
- [ ] Fatigue audit run (§6) — no axis 4+ in a row, host time ≥ 25-30% for authority
- [ ] Source picked per priority (§7) — library > stock > i2v > real footage
- [ ] Anti-patterns audited (§8)
- [ ] No text rendered into B-roll images (text-in-code rule)
- [ ] B-roll cost surfaced if i2v generation is the dominant source

If any check fails, revise the B-roll list before committing to generation.
