---
name: performance-analysis
description: >-
  Reads what worked post-publish (24h / 72h / 7d / 30d analytics + comments). Extracts rules + sequel candidates. Load for a performance review.
---

# Skill 14 — Performance Analysis & Iteration Loop

This is the operational knowledge an AI YouTube agent needs to extract learnings from a published video and feed them back into the system. Every paragraph is a concrete rule the agent applies.

This is the post-publish skill that closes the loop. Skills 01-13 ship the video; Skill 14 reads what happened and updates the agent's understanding so the next video is better. Without it, the system runs production but never learns from outcomes.

This skill is the load-bearing companion to Skill 12 (Outlier Mining) and Tactical T7 (Memory Rule Extraction). Skill 12 picks topics; T7 extracts rules from user feedback; Skill 14 extracts rules from algorithm + audience feedback (the data).

---

## 1. The job of performance analysis

Performance analysis does five things:

1. **Quantifies what worked** — which videos beat the channel median, by how much, and why
2. **Diagnoses what didn't** — where retention dropped, where CTR underperformed, what audience friction surfaced in comments
3. **Extracts replicable lessons** — converts data observations into rules saved per T7 protocol
4. **Updates outlier history** — feeds Skill 12's pattern bank with fresh channel-internal evidence
5. **Decides next moves** — sequel content, format changes, topic adjustments, channel pivots

The agent never treats published videos as "done." Every published video is a learning instrument; every 30-day-old video has data the system can act on.

The compounding consequence: a channel running Skill 14 well makes 50 videos in year 1 and starts year 2 with 50 unique data points. A channel skipping Skill 14 makes 50 videos and starts year 2 with no more knowledge than year 1.

---

## 2. The optimization target

The agent optimizes performance analysis against **decisions-per-data-point**. Specifically:

- **Coverage:** does every published video get analyzed? Lapsed analysis means lost data.
- **Decision velocity:** how quickly do data observations become rule changes? A signal noticed at 24hr that becomes a rule at 30 days has lost most of its value.
- **Precision:** are the rules extracted actually correct? Spurious correlations from small samples produce rule pollution.

A performance analysis that happens 4× per video at the right windows, extracts rules with adequate sample sizes, and feeds them into Skill 12 + T7 within a week — that's the discipline.

---

## 3. The four review windows

Different signals are visible at different post-publish ages. The agent runs analysis at four windows.

### Window 1 — 24-hour review

**What's visible:** initial CTR (first impression batch), early retention curve (first ~30% of viewers), comment volume + early sentiment.
**What's NOT visible:** algorithm settled position, browse vs search distribution, AVD-based outlier ranking (still volatile).
**Decision space:** does this video need an emergency pivot (title swap via YouTube native test, thumbnail swap, description SEO update)?

**Output:** quick verdict — `track / pivot now / kill`
- Track: video is performing within or above niche median, let the algorithm work
- Pivot now: video is dramatically underperforming; consider title/thumbnail change before the algorithm anchors
- Kill: video has a strike risk or content failure that exceeds value of leaving up

### Window 2 — 72-hour review

**What's visible:** CTR stabilizing, retention curve more reliable (50%+ of total viewers), the algorithm's preliminary positioning (browse % vs search %).
**What's NOT visible:** long-tail outlier potential, deep retention valley signals.
**Decision space:** does this video warrant a sequel? Should the next episode adjust any specific element (topic, voice, mix)?

**Output:** initial outlier classification — outlier multiple at 72hr, projected 30-day multiple, sequel-worthiness assessment.

### Window 3 — 7-day review (THE LOAD-BEARING WINDOW)

**What's visible:** CTR landed at its likely 30-day baseline, full retention curve, reliable comment sentiment, top swipe-away timestamps, traffic source distribution (Browse / Search / Suggested / External).
**What's NOT visible:** 30-day decay, second-wave algorithmic surfacing.
**Decision space:** rule extraction. THIS is the window where most learning happens.

**Output:** structured analysis — see §6 for the format.

### Window 4 — 30-day review

**What's visible:** final outlier classification, full traffic source breakdown, long-tail patterns, comment-driven topic candidates for future videos.
**What's NOT visible:** evergreen vs decay split (evergreen patterns visible at 90+ days).
**Decision space:** what does this episode mean for the Channel Profile? Any rules to update?

**Output:** Channel Profile update — Outlier Pattern History (§11) gets the final 30-day numbers; any Hard Rules learned go to §14.

---

## 4. The data sources

The agent pulls from these sources in priority order:

### Source 1 — YouTube Studio Analytics (primary)

**What it provides:** CTR, AVD, AVD%, retention curve (per-second), traffic sources, demographic breakdown, audience retention overlay against the script, top swipe-away timestamps.

**How to access:** YouTube Studio → Content → [video] → Analytics tab → all sub-tabs.

**Limitations:** the data is YouTube's view of YouTube; doesn't capture cross-platform.

### Source 2 — vidIQ + Spotter outlier scoring

**What it provides:** niche-relative outlier multiple, comparison videos in the niche.

**How to access:** vidIQ Pro dashboard, Spotter Studio competitive analytics.

**Limitations:** scoring is imperfect; doesn't account for channel-internal context.

### Source 3 — Comment analysis

**What it provides:** specific viewer questions, common confusion points, signal of audience composition (retain-cohort comments differ from bounce-cohort).

**How to access:** YouTube Studio → Comments tab + sentiment-analysis tooling (Claude can summarize 200-500 comments per video).

**Limitations:** vocal minority skew — commenters are not representative of full audience.

### Source 4 — Channel internal viral-database

**What it provides:** historical context against prior episodes; cross-channel comparison if multiple channels run.

**How to access:** the `viral-database` skill (in `.claude/skills/viral-database/`).

### Source 5 — External signals (optional)

- Reddit / Twitter mentions of the video
- Search volume changes for the topic
- Press / blog citations

Most analysis runs on Sources 1-3. Source 4-5 are for deeper outlier investigation.

---

## 5. The 5 metrics that matter

Beyond raw views, five metrics carry the load.

### Metric 1 — CTR (Click-Through Rate)

**What it is:** impressions that became clicks ÷ total impressions.
**Channel benchmark:** personal-finance authority channel median is 6-8%; outlier episodes hit 11-14%.
**What high CTR signals:** title + thumbnail combo nailed the hook for the impression cohort.
**What low CTR signals:** title or thumbnail mismatch with the cohort being served. Consider native YouTube A/B test (Test & Compare) at 24-72hr.
**What CTR doesn't tell you:** whether the click was a quality click. High CTR + low retention = click-trap.

### Metric 2 — AVD (Average View Duration)

**What it is:** total minutes watched ÷ total views.
**What high AVD signals:** script structure + voice carrying retention through the full video.
**What low AVD signals:** script collapse somewhere — typically Beat 1 hook (under 70% retention at 0:30) or a major valley failure (30%/60%/80%).
**Channel benchmark:** for 25-min videos, target 8-12 min AVD; outliers hit 13-17 min.

### Metric 3 — AVD% (Average % Viewed)

**What it is:** AVD ÷ total video duration.
**What it shows:** retention efficiency normalized against video length.
**Channel benchmark:** 35-50% is good for 25-min long-form; 50-65% is outlier territory.
**Why it matters more than AVD:** comparing AVD across videos of different length is misleading. AVD% normalizes.

### Metric 4 — Retention curve shape

**What it shows:** percentage of viewers still watching at each second.

**Curve shapes by quality:**
- **Clean exponential decay:** healthy retention. Most viewers leave at predictable rates throughout. Win.
- **Cliff at 0:30:** hook failure. The first 30 seconds didn't deliver. Title-thumbnail-script mismatch. Diagnose Beat 1.
- **Mid-video valley deeper than expected:** retention valley failure (30%, 60%, or 80% per Skill 03 §6). Diagnose which beat dropped.
- **Spike:** a chapter-skip or browse-back. Usually positive signal — viewers came back for something specific. Note the timestamp and learn what they came back to.
- **Plateau:** unusually flat after initial drop. Often a sign that the video found its audience and they're committed.

The retention curve is THE most diagnostic signal. AVD is the summary; the curve tells you where to fix.

### Metric 5 — Comments-per-view ratio

**What it is:** comments ÷ views.
**What high ratio signals:** audience engagement. Either positive (this hit nerve) or negative (controversy / correction comments).
**Channel benchmark:** 0.3-0.6% comments-per-view is healthy for authority content; below 0.2% suggests low engagement; above 1% suggests controversy.
**What to look for:** the type of comments matters more than the count. See §7 comment analysis.

---

## 6. The 7-day review format

The structured analysis run at the load-bearing window. Use this format for every video.

```
PERFORMANCE REVIEW — [video title]
Channel: [channel]
Published: [date + time]
Review window: 7-day

—— RAW METRICS ——
Views: [N] (channel-internal multiple: [Nx])
CTR: [%] (channel median: [%], niche median: [%])
AVD: [m:ss] (channel median: [m:ss])
AVD%: [%] (channel median: [%])
Comments: [N] (comments/view: [%])
Likes: [N] (likes/view: [%])

—— OUTLIER CLASSIFICATION ——
Channel-internal outlier multiple: [N×]
Niche outlier multiple: [N×]
Type (per Skill 12 §3): [1 Format / 2 Topic / 3 Hook / 4 Crossover]
Replicable pattern: [the lift driver — formula / hook architecture / topic angle]

—— RETENTION CURVE DIAGNOSIS ——
0:30 retention: [%] (target ≥ 70%)
30% point retention: [%]
60% point retention: [%]
80% point retention: [%]

Curve shape: [exponential decay / cliff / valley failure at X / plateau / other]
Major drop point 1: [timestamp] — [diagnosis]
Major drop point 2: [timestamp] — [diagnosis]
Major drop point 3 (if applicable): [timestamp] — [diagnosis]

—— TRAFFIC SOURCE ——
Browse: [%]
Suggested: [%]
Search: [%]
External: [%]
Channel pages: [%]

—— COMMENT THEMES (top 5) ——
1. [theme + frequency] — [example comment if instructive]
2. [theme + frequency]
3. [theme + frequency]
4. [theme + frequency]
5. [theme + frequency]

—— RULES EXTRACTED ——
[Per T7 §6 format — RULE / WHY / HOW TO APPLY / SCOPE]

—— DECISIONS ——
Sequel-worthy: [yes / no / conditional]
Channel Profile updates: [list any updates to specific sections]
Skill 12 outlier history update: [add to live patterns / played-out / emerging]
Next video implications: [specific guidance for upcoming production]

—— OPEN QUESTIONS ——
[anything ambiguous that needs more time or more data to resolve]
```

The agent runs this format for every video at the 7-day window and saves to `projects/[channel]/reviews/[video_id]_7day.md`.

---

## 7. Comment analysis protocol

Comments are messy data. The agent runs them through a 4-step protocol:

### Step 1 — Pull top 200-500 comments

Use YouTube Studio comments export OR the YouTube API. Sort by top (likes-weighted) by default; also pull "newest" for fresh sentiment.

### Step 2 — Theme extraction

Group comments into themes. Typical themes for personal-finance authority channel:
- "Thank you / this helped me" (positive engagement)
- "Question about my specific situation" (audience-need signal)
- "What about [related topic]" (sequel candidate)
- "Patricia's story sounds like my mother" (resonance signal)
- "Form X you mentioned doesn't work for [edge case]" (correction signal)
- "I tried this and it worked" (proof point)
- "This is wrong because..." (correction or controversy)

### Step 3 — Theme weighting

The themes that DOMINATE the comment section signal what landed. The themes that REPEAT across multiple commenters signal audience need (not just one person's quirk).

Heuristic: if a theme appears in 5%+ of comments AND has multiple distinct commenters, it's signal worth acting on.

### Step 4 — Convert themes to action

Each high-signal theme produces one of:
- **A sequel candidate:** "What about Medicare Advantage" appearing 8% of comments → next-video candidate
- **A rule extraction:** "Form X doesn't work for self-employed" → save to Channel Profile §10 compliance: "for self-employed sub-cohort, recommend Form Y instead"
- **A correction:** if comment correctly identifies a fact-check error, queue a video update or pinned-comment correction
- **An engagement opportunity:** pin a strong testimonial comment; reply to repeated questions with channel-direction info

The agent surfaces theme analysis to the user; user decides which actions to take.

---

## 8. Per-niche performance benchmarks

What "good" looks like per niche. The agent loads the relevant benchmark.

| Niche | CTR median | CTR outlier | AVD% median | AVD% outlier | Comments/view |
|---|---|---|---|---|---|
| Senior finance / IRS | 6-8% | 11-14% | 38-45% | 55-65% | 0.4-0.6% |
| Senior health | 5-7% | 9-12% | 35-42% | 50-60% | 0.4-0.7% |
| Tech / AI | 7-10% | 14-18% | 40-50% | 60-70% | 0.3-0.5% |
| Doc explainer | 6-9% | 12-16% | 45-55% | 65-75% | 0.2-0.4% |
| News-hijack | 8-12% | 16-22% | 35-45% | 55-65% | 0.6-1.0% |
| True crime | 7-10% | 13-18% | 50-60% | 70-80% | 0.5-0.8% |
| Vertical short | 5-8% | 12-20% | 60-75% (completion) | 85-95% | 0.5-1.0% |
| Music video | 4-7% | 10-15% | 45-55% | 65-80% | 0.3-0.6% |
| Cooking | 5-8% | 11-15% | 42-52% | 58-68% | 0.4-0.7% |
| Real estate | 5-8% | 11-15% | 40-50% | 55-65% | 0.4-0.6% |
| Faith | 6-9% | 11-15% | 50-60% | 65-75% | 0.5-0.8% |

These benchmarks are 2026-current. Update annually as YouTube algorithm shifts.

---

## 9. When to double-down on a winner

Some videos signal sequel-worthy strongly. The pattern recognition:

### Signal 1 — Outlier multiple ≥ 3× channel median at 7-day

Strong enough that the algorithm is amplifying. Worth replicating.

### Signal 2 — Comment theme dominance

5%+ of comments asking "what about [related angle]" — audience self-identifies the sequel topic.

### Signal 3 — Search traffic ≥ 30% of total

Search-heavy traffic means the topic has structural demand beyond the algorithmic surface. Sequel content rides the same search wave.

### Signal 4 — Specific timestamp re-watch spikes

Retention curve shows spikes at specific seconds — viewers re-watch a specific moment. That moment is the goldmine; the sequel video unpacks it more.

### Sequel patterns

- **Direct sequel:** "I covered Medicare Part B; now Medicare Part D enrollment." Same niche slot, adjacent topic.
- **Deep-dive sequel:** "an example episode explained the rule; this video walks through Form X step-by-step." Same topic, deeper craft.
- **Reverse-angle sequel:** "an example episode was the threat; this is the recovery." Pair videos.

The agent proposes sequel format to the user with reasoning; user picks.

---

## 10. When to pivot from a loser

Some videos signal misalignment. Recognition patterns:

### Signal 1 — CTR ≤ 50% of channel median at 7-day

The title-thumbnail combo missed. Don't replicate the topic angle.

### Signal 2 — Retention cliff at 0:30 (under 60%)

Hook failed. The script's Beat 1 didn't deliver the title's promise. Don't replicate the hook architecture used.

### Signal 3 — Negative-dominant comments

If 30%+ of comments are corrections, complaints, or "this isn't for me" — the topic-audience match was wrong. The video found a cohort that bounces.

### Signal 4 — AVD% under 25%

Drastic — the video reads as click-trap or content-failure. Consider unlisting after 30 days if AVD% stays below this floor.

### Pivot patterns

- **Topic pivot:** the topic was wrong for this channel. Don't try variants.
- **Format pivot:** the topic was right but the format/angle missed. Try a different approach.
- **Voice pivot:** the script-voice combination missed. Tune voice DNA.
- **Channel direction pivot:** the channel itself may be drifting from the audience. Bigger conversation with the user.

---

## 11. The iteration loop output

Performance analysis feeds back into the system at three places:

### Feedback 1 — Skill 12 outlier history (Channel Profile §11)

After 7-day review:
- Add to "Channel-internal outliers" list if outlier multiple ≥ 3×
- Add to "Live patterns" if pattern repeats across 3+ outlier videos
- Move to "Played-out patterns" if a previously-live pattern fails 2+ consecutive times

### Feedback 2 — Channel Profile rule updates (per T7)

Any rule extracted from data goes to the relevant Channel Profile section per T7 §3 storage map.

### Feedback 3 — Skill 14 internal benchmarks

Update the channel's median/outlier benchmarks every 30 days. Use 90-day rolling window for stable medians.

---

## 12. Anti-patterns

Eight performance-analysis mistakes.

### Anti-pattern 1 — Skipping analysis on "obvious failures"

Video underperforms; agent assumes "hook was bad" and moves on without 7-day review. The actual failure point may be Beat 5 mechanism, but agent never diagnosed it. Same failure pattern repeats next episode.
**Fix:** every video gets the 7-day review, even underperformers. Especially underperformers — that's where rule-extraction value is highest.

### Anti-pattern 2 — Reading 24-hour data as if it's stable

CTR at 24 hours is volatile. A 4% CTR at 24 hours can become 9% at 7 days because the algorithm's serving cohort changed. Acting on 24-hour data as if it's permanent leads to false positives.
**Fix:** the 24-hour window is for emergency pivot decisions only. Real analysis happens at 7 days.

### Anti-pattern 3 — Cherry-picking comments

Pull 10 comments that confirm a hypothesis; ignore the 200 that don't. Confirmation bias.
**Fix:** §7 protocol — pull 200-500, group by theme, weight by frequency.

### Anti-pattern 4 — Outlier mistaken for trend

One video hits 5× channel median; agent declares "we found the pattern." But sample size is one. Replicate-and-fail follows.
**Fix:** 3+ data points required before declaring a pattern. Single outliers go in "candidate" status, not "live pattern."

### Anti-pattern 5 — Ignoring negative signals

A video with average CTR and AVD but a comment section full of "this isn't for me" — the agent reads "metrics OK" and skips the qualitative read. Audience drift goes undetected.
**Fix:** comment analysis is mandatory at 7-day review. Quantitative data without qualitative review is incomplete.

### Anti-pattern 6 — Sequel-spamming a single winner

an example episode hits 4× median; agent makes four follow-up episodes all on adjacent topics with same format. By the fourth follow-up the audience is saturated and the format is played out.
**Fix:** sequels rotate. A winning pattern can produce 1-2 immediate sequels, then the calendar should rotate to other formats. Audience-saturation discipline.

### Anti-pattern 7 — Pivoting too fast

Video underperforms; agent declares the pattern dead and pivots away. But the underperformance was due to a specific topic mis-match, not the pattern. Pattern survives pivots and the agent loses track.
**Fix:** 2 consecutive underperforms before declaring a pattern played-out (per Skill 14 §11 feedback 1).

### Anti-pattern 8 — Not feeding back into the system

7-day review happens; rules get extracted; but never get saved to Channel Profile or cross-channel rules. The compounding loss is total — the analysis was wasted.
**Fix:** every 7-day review ends with explicit Channel Profile / rules updates. Saved or it didn't happen.

---

## 13. Worked example — an example episode hypothetical 7-day review

```
PERFORMANCE REVIEW — an example episode "CONFIRMED: Social Security Shake-Up Hits May 1"
Channel: personal-finance authority channel
Published: 2026-04-23 09:00 ET
Review window: 7-day (review run 2026-04-30)

—— RAW METRICS ——
Views: 142,500 (channel-internal multiple: 4.1×)
CTR: 12.4% (channel median 7%, niche median 6.5%)
AVD: 14:22 (channel median 11:30)
AVD%: 56.3% (channel median 45%)
Comments: 612 (0.43% — within healthy range)
Likes: 8,420 (5.9% — strong)

—— OUTLIER CLASSIFICATION ——
Channel-internal outlier multiple: 4.1× (top outlier of last 90 days)
Niche outlier multiple: 5.2×
Type: Type 3 Hook outlier (primary) + Type 1 Format outlier (secondary)
Replicable pattern: F6 + F12 threat-alert ALL CAPS + specific deadline date

—— RETENTION CURVE DIAGNOSIS ——
0:30 retention: 78% (target ≥ 70% — PASS)
30% point retention: 71%
60% point retention: 62%
80% point retention: 55%

Curve shape: clean exponential decay with one minor valley
Major drop point 1: 0:31 → 0:48 (typical post-promise-stack settle, ~6% drop)
Major drop point 2: 7:42 (~3% drop into Beat 4 transition)
Major drop point 3: 18:30 (~4% drop near Beat 8 bonus)

—— TRAFFIC SOURCE ——
Browse: 64%
Suggested: 19%
Search: 12%
External: 3%
Channel pages: 2%

Browse-heavy traffic = algorithm is amplifying. Strong signal.

—— COMMENT THEMES (top 5) ——
1. "Thank you, this helped my parents" (29% of comments) — strong resonance with adult-children-of-retirees subcohort
2. "What about [Medicare Advantage / Part D / spousal benefits]" (18%) — sequel candidates abundant
3. "I called SSA and the agent confirmed Form 7004 works" (12%) — proof point + evergreen value signal
4. "Can you cover [tax filing under SSA garnishment]" (8%) — sequel candidate
5. "My address on file is wrong, fixing it now" (5%) — direct conversion signal — viewers acted on advice

—— RULES EXTRACTED ——
RULE 1:
  RULE: F6 + F12 threat-alert with specific deadline outperforms by 4× channel median
  WHY: Specific date anchor + ALL CAPS + named institutional change creates urgency the retiree audience self-includes on
  HOW TO APPLY: When a regulatory change with specific effective date occurs, prioritize F6+F12 formula
  SCOPE: personal-finance authority channel channel
  PROMOTED FROM: 7-day review of an example episode (this is 2nd outlier confirming F6+F12 pattern after a prior outlier cluster)

RULE 2:
  RULE: Adult-children-of-retirees is a meaningful subcohort
  WHY: 29% of an example episode comments came from "my parents" framing; previously assumed audience was retirees themselves
  HOW TO APPLY: Consider script Beat 12 share prompt addressing both retirees AND their adult children explicitly
  SCOPE: personal-finance authority channel channel

—— DECISIONS ——
Sequel-worthy: YES — strong outlier + clear sequel candidates from comments
Channel Profile updates:
  §11 Outlier Pattern History: confirm F6+F12 in "live patterns" (now 2 outliers using it)
  §1 Channel Positioning: add adult-children-of-retirees to retain-cohort definition
  §6 Script Structure custom rules: add Beat 12 share prompt addressing both subcohorts

Skill 12 outlier history update: F6+F12 promoted from "candidate" to "live pattern" (3rd confirmation if a prior outlier + an example episode + this analysis count)

Next video implications:
  - Next-episode candidate from comments: Medicare Part D enrollment (similar timing/threat structure)
  - Following-episode candidate: SSA garnishment for tax debt (named in comments 8%)
  - Continue F6+F12 pattern but rotate to F2 dollar-counter-narrative for variety (avoid pattern saturation)

—— OPEN QUESTIONS ——
Q1: Is the adult-children-of-retirees subcohort large enough to warrant a dedicated content slot, or is it serendipitous to the an example episode topic?
   Resolution: track over next 5 episodes; if subcohort signals appear in 3+ comment sections, dedicate a slot.

Q2: Browse 64% / Search 12% — should we lean into Browse optimization (thumbnail + 1st-5-words) or invest in Search (description SEO)?
   Resolution: continue current Browse-optimized approach; Search 12% is healthy for the niche.
```

This review feeds:
- Channel Profile §1, §6, §11 (updates)
- Skill 12 channel-internal outlier list (F6+F12 confirmed)
- T7 saved 2 new rules
- Sequel candidates queued for Skill 12 ideation in next session

---

## 14. Cross-skill connections

This skill connects to:
- **Skill 12 (Outlier Mining):** Skill 14's outlier classifications feed Skill 12's pattern bank. Without Skill 14, Skill 12's "channel-internal outlier" data is stale.
- **T7 (Memory Rule Extraction):** rules extracted from data go through T7's storage map.
- **Skill 13 (Pre-flight):** Skill 14 surfaces fact-check failures from comments that should feed back into Skill 13's check protocol.
- **Skill 03 (Script):** retention valley diagnoses go to Skill 03 — the agent learns which beat structures fail in this channel.
- **Channel Profile:** every analysis updates relevant Profile sections.

When a downstream skill (Skill 12 ideation) feels like it's running on stale data, the upstream cause is often skipped Skill 14 reviews. Run the review.

---

## 15. Runtime checklist

For every published video:

- [ ] 24-hour quick check — track / pivot now / kill verdict
- [ ] 72-hour outlier classification — initial multiple + sequel-worthiness
- [ ] 7-day FULL review per §6 format — saved to `projects/[channel]/reviews/[video_id]_7day.md`
- [ ] Comment analysis per §7 protocol — 200-500 comments grouped by theme
- [ ] Retention curve diagnosed for major drop points
- [ ] Rules extracted per T7 protocol — saved to Channel Profile
- [ ] Outlier history updated in Channel Profile §11
- [ ] Sequel candidates queued for Skill 12 ideation
- [ ] 30-day final review — Channel Profile final updates

For periodic maintenance:

- [ ] Quarterly: review 90-day rolling channel benchmarks; update Channel Profile if shifted
- [ ] Annually: review niche benchmarks (§8); update if YouTube algorithm shifts

The cost of running performance analysis is ~30 min per 7-day review. The compounding benefit is that every channel's outlier mining (Skill 12) and rule extraction (T7) get fed fresh data. Without it, the system runs production but never closes the loop.
