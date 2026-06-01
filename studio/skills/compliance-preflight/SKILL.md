---
name: compliance-preflight
description: >-
  Pre-ship verification: defamation, fact-checking, FTC, copyright, age-rating. Load before shipping a video — every video, no exceptions.
---

# Skill 13 — Compliance Pre-flight & Fact-Checking

This is the operational knowledge an AI YouTube agent needs to verify a video before it ships. Every paragraph is a concrete rule the agent applies.

Compliance is not a creativity tax. Compliance is the discipline that keeps a channel alive long enough for the production work in Skills 01-12 to compound. A single termination-level violation undoes 50 episodes of careful execution. The cost of one careful pre-flight pass is 15-30 minutes per video; the cost of a termination is the entire channel.

This is the last skill in the pipeline. It runs AFTER everything else is rendered, BEFORE the upload button.

---

## 1. The job of compliance pre-flight

Compliance pre-flight does six things:

1. **Verifies platform policy compliance** — YouTube's post-July-2025 inauthentic content policy + the 16 channel terminations of Jan 2026 (4.7B views) define what gets your channel killed
2. **Verifies advertising disclosure compliance** — FTC requires affiliate links to be clearly disclosed in-video and in-description; failing this triggers FTC enforcement, not just YouTube enforcement
3. **Verifies medical/legal/financial disclaimer compliance** — YMYL niches require specific disclaimer language to avoid both platform and regulatory risk
4. **Fact-checks load-bearing claims** — every specific dollar figure, statistic, study citation, named person, and form number must be verified against a primary source
5. **Audits for defamation risk** — named villains, exposed individuals, or institutional accusations need evidence on file before publication
6. **Catches AI-content tells** — phrases, visual patterns, or structural signals that trip post-July-2025 detection algorithms

The agent runs pre-flight even when it feels redundant. The cost of running pre-flight on a video that turns out to be clean is 15 minutes; the cost of skipping pre-flight on a video that wasn't is the channel.

---

## 2. The optimization target

The agent optimizes for **zero termination-level violations × minimal claim risk**. There is no upside to compliance — only downside avoidance. The metric is: did this video create any risk that compounds across the channel?

- **Termination risk:** any violation that could trigger channel-level enforcement.
- **Strike risk:** any violation that could trigger video-level removal or community guidelines strike.
- **Claim risk:** any specific factual claim that, if wrong, would trigger viewer corrections, comment-section drama, or downstream credibility loss.
- **Brand risk:** anything that fits within policy but creates audience trust drift.

Risk is not equal. The agent stratifies: termination > strike > claim > brand, in that order. A video can pass termination + strike + claim checks and still need a brand-risk review.

---

## 3. The 5 compliance domains

Every video is audited across 5 domains. Each domain has its own rules, failure modes, and remediation paths.

### Domain 1 — Platform policy (YouTube)

The domain that can terminate a channel.

**Live policies (April 2026):**
- **Inauthentic / repetitive / mass-produced content** (effective July 2025, enforced harder Jan 2026) — channels producing AI content without "human creative input" risk demonetization or termination
- **Misleading thumbnails / titles** — content not matching the click promise
- **Medical misinformation** — content contradicting health authority consensus on covered topics
- **Election misinformation** — context-specific, varies by jurisdiction and event timing
- **Hate speech / harassment** — broad and aggressive enforcement
- **Copyright** — manual claims + Content ID
- **Spam / deceptive practices** — fake giveaways, account farming, etc.
- **Child safety** — strict Made-for-Kids designations

**Highest-current-risk for this agent's channels:**
- Inauthentic content (brick-narrative storytelling channel, music video genre at risk per Jan 2026 enforcement wave)
- Medical misinformation (medical-authority channel — health niche scrutiny is high)
- Misleading thumbnails (any niche using P2 Rage Stamp pattern needs the video to deliver the threat)

### Domain 2 — Federal Trade Commission (FTC) advertising disclosure

The domain that triggers regulator action, not just platform action.

**Live rules:**
- Affiliate links must be **clearly and conspicuously** disclosed
- Disclosures cannot be buried in description, must be in-video for video-led promotions
- Sponsorship cannot be hidden — paid endorsements must say "paid" or "sponsored" clearly
- Claims about products require substantiation
- Endorsements from "experts" require the expert to actually be qualified

**Live disclosure templates:**
- In-video (spoken): *"This is an affiliate link. Using it supports the channel at no additional cost to you."*
- In-description: separate line, prefixed with `(This is an affiliate link. Using it supports the channel at no additional cost to you.)`
- Sponsor-paid: *"This video is sponsored by [Brand]. They didn't see this script before recording."* (the second sentence isn't required; it builds trust)

### Domain 3 — Medical compliance (YMYL Health)

The domain with the highest claim-risk on health channels.

**Live rules:**
- Don't claim a specific outcome from a specific intervention without cited support
- Don't recommend specific dosages without medical authority
- Don't directly contradict CDC, FDA, or major specialty society consensus without naming the consensus and citing the alternative source
- Always recommend consulting a physician for individual decisions
- "Composite patient" stories must be labeled as composites
- "Reverse" / "cure" / "treat" — load-bearing legal terms; use "support" / "manage" unless citing peer-reviewed evidence of the stronger claim

**Standard disclaimer:**
> "This video is for educational purposes only and does not constitute medical advice. [Patient name] is a composite of clinical patterns. Consult your physician before changing any supplement or dietary protocol."

### Domain 4 — Legal / financial / tax compliance

The domain for a personal-finance authority channel, investigative-journalism-style content, and any finance channel.

**Live rules:**
- Don't give "advice" in the legal sense — give educational explanations
- Don't recommend specific products (other than disclosed affiliates) — recommend categories
- Don't guarantee outcomes ("you WILL recover your money")
- Don't make claims about specific institutions without evidence on file
- Tax advice specifically: don't advise on specific filings without specifying "consult a CPA"

**Standard disclaimer:**
> "This video is for educational purposes only and does not constitute legal, tax, or financial advice. [Subject name] is based on a composite of real cases. Consult a qualified [tax professional / attorney / financial advisor] for your specific situation."

### Domain 5 — Defamation

The domain that gets channels sued.

**Live rules:**
- Naming a real person or institution as a wrongdoer requires evidence on file
- Statements of fact must be true; statements of opinion must be clearly opinion
- "Allegedly" and "according to [source]" are not defamation shields if the underlying claim is reckless
- Composite victims labeled as composites are safe
- Exposing fraud (investigative-journalism style) requires documentary evidence that survives a libel review

**Evidence-on-file standard:**
For every named individual or institution in an accusatory frame, the agent maintains in `research/[video_id]/evidence/`:
- Primary source for each factual claim (article URL, screenshot, court document, public statement)
- Date of source
- Note on whether the source itself is reliable (e.g., reddit post = weak source)
- Counter-source if any (e.g., "the institution responded with X")

If a claim has no primary source, it does not appear in the script.

---

## 4. The 4 claim risk classifications

Every load-bearing claim in a script gets classified before it ships. The agent runs through the script and tags claims:

### Risk class 1 — Low

**Definition:** widely-known facts, public knowledge, no controversial dimension.
**Examples:** "Social Security started in 1935." "The Federal Reserve sets interest rates." "Apple is headquartered in Cupertino."
**Verification:** sanity-check only. Spot-check 1 in 5.
**Disclaimer needed:** none.

### Risk class 2 — Medium

**Definition:** specific numbers, dates, or attributions where a wrong fact would trigger correction comments but not litigation.
**Examples:** "The Treasury announced the rule on March 11, 2026." "A 2025 Stanford meta-analysis found..." "Form SSA-7004 is one page."
**Verification:** must have a primary source on file. Source URL recorded in `research/[video_id]/sources.md`.
**Disclaimer needed:** "[per source]" reference acceptable in the spoken script for stronger trust signal.

### Risk class 3 — High

**Definition:** claims about specific actions by specific named individuals or institutions; medical/legal/financial outcomes; allegations of wrongdoing.
**Examples:** "Coinbase has been quietly locking accounts." "Dr. X's studies do not exist." "This supplement causes kidney damage."
**Verification:** primary source on file + counter-source check + evidence reviewed for adequacy. The agent runs a 3-question audit:
1. Is the source primary (not a summary of a summary)?
2. Is the source recent enough to still be current?
3. If the named subject responded, has the response been considered?
**Disclaimer needed:** disclaimer paragraph in description; in-video acknowledgment if appropriate.

### Risk class 4 — Critical

**Definition:** any claim where being wrong could trigger litigation, regulatory action, or platform termination.
**Examples:** "[Named person] committed [specific crime]." "[Brand] caused [specific harm]." "[Medication] is dangerous."
**Verification:** primary source + counter-source + legal review (or equivalent rigor — second researcher confirms) + evidence package archived.
**Disclaimer needed:** mandatory; specific defamation language reviewed.
**Default action when uncertain:** drop the claim. Channel survival > the strength of the specific accusation.

The agent classifies every claim and runs pre-flight at the highest risk class present in the video. A video with one Critical claim runs Critical-level checks regardless of how many Low claims surround it.

---

## 5. Per-niche compliance matrix

The 12 highest-priority niches with their dominant compliance considerations.

### Senior finance / IRS / retirement (personal-finance authority register)

- **Highest risk:** Domain 4 (financial advice), Domain 5 (institutional naming)
- **Required disclaimer:** Yes, in description after CTA. "Educational purposes only, does not constitute legal/tax/financial advice. Composite cases. Consult qualified professional."
- **Affiliate disclosure:** Yes, FTC standard
- **Common failure:** giving specific tax-filing instructions without "consult a CPA" caveat
- **Mishear-dictionary check:** SSA forms, IRS form numbers, dollar amounts
- **Composite labeling:** every patient story explicitly composite-labeled

### Senior health / medical (medical-authority channel)

- **Highest risk:** Domain 3 (medical), Domain 1 (medical misinformation policy)
- **Required disclaimer:** Yes, mandatory. "Educational purposes only, does not constitute medical advice. Composite patient. Consult physician before changing any supplement or dietary protocol."
- **Affiliate disclosure:** Yes, especially supplement affiliates
- **Common failure:** "reverse" / "cure" / "treat" without peer-reviewed citation
- **Citation rigor:** every medical claim has a primary source on file
- **Authority claim:** if citing a doctor, the doctor's credentials and statements must be verified

### Tech / AI / dev tools

- **Highest risk:** Domain 2 (sponsor disclosure), Domain 5 (vendor naming)
- **Required disclaimer:** None standard
- **Affiliate disclosure:** Yes, especially course/SaaS affiliates
- **Common failure:** un-disclosed sponsorships from tools being reviewed; "this is the best X" claims without controlled testing
- **Comparative claims:** any "Tool A beats Tool B" needs reproducible test

### Roblox-scenario channel (vertical shorts)

- **Highest risk:** Domain 1 (Made-for-Kids designation, child safety)
- **Required disclaimer:** None standard
- **Affiliate disclosure:** None (no affiliates in pipeline)
- **Common failure:** content drift toward content that could be mis-classified as Made-for-Kids
- **Brand risk:** keep voice / aesthetic distinct from kids content even when topic is gaming

### Brick-narrative storytelling channel (drill music videos)

- **Highest risk:** Domain 1 (inauthentic content policy, terminated channel cluster Jan 2026)
- **Required disclaimer:** None standard, but channel-level "satirical" or "AI-generated" tag in About is helpful
- **Affiliate disclosure:** None
- **Common failure:** crossing into political-misinformation territory; AI-content tells in titles/thumbnails
- **Risk mitigation:** label as satire, avoid claims of literal truth, vary visual aesthetic per video to avoid mass-produced flag

### Documentary explainer (long-form mystery-documentary style hypothetical)

- **Highest risk:** Domain 5 (defamation when discussing real cases or institutions)
- **Required disclaimer:** sometimes (for politically sensitive topics)
- **Affiliate disclosure:** Yes for sponsor pitches
- **Common failure:** claims about specific real-world figures without primary sources
- **Citation rigor:** historical claims especially need primary-source citation; secondary sources are weak

### News-hijack documentary (investigative-journalism-adjacent)

- **Highest risk:** Domain 5 (defamation — naming subjects of investigation)
- **Required disclaimer:** "Allegations and the subject's responses" framing
- **Affiliate disclosure:** Yes
- **Common failure:** stating accusations as facts without "alleges" or evidence
- **Evidence rigor:** every accusation has documentary evidence on file BEFORE recording

### True crime

- **Highest risk:** Domain 5 (defamation), brand risk (sensitivity)
- **Required disclaimer:** "Out of respect for the families involved..."
- **Affiliate disclosure:** Patreon CTAs disclosed standard
- **Common failure:** speculating about persons of interest who haven't been charged
- **Channel discipline:** focus on verified public record only

### Cooking

- **Highest risk:** Domain 2 (sponsor disclosure), low overall risk niche
- **Required disclaimer:** None
- **Affiliate disclosure:** Yes for product/equipment links

### Real estate

- **Highest risk:** Domain 4 (financial), Domain 5 (specific market predictions)
- **Required disclaimer:** Yes, "Educational purposes only, does not constitute investment, tax, or financial advice."
- **Affiliate disclosure:** Yes (mortgage calculator, broker referral, etc.)
- **Common failure:** specific "buy this property in X city" recommendations

### Faith / Christian

- **Highest risk:** brand risk (audience trust), low platform risk
- **Required disclaimer:** None standard
- **Affiliate disclosure:** None usually

### Politics / commentary

- **Highest risk:** Domain 1 (election/political misinformation), Domain 5 (defamation), brand risk
- **Required disclaimer:** Sometimes
- **Affiliate disclosure:** Yes
- **Common failure:** specific claims about politicians without citation; election-period content drifts into misinformation territory
- **Risk mitigation:** "according to [source]" framing for every named-politician claim

---

## 6. The pre-flight checklist (mandatory)

The complete pass the agent runs on every video before upload. Time budget: 15-30 minutes per long-form video; 5-10 minutes per short.

### Section A — Platform policy audit (Domain 1)

- [ ] Title accurately previews video content (no clickbait beyond what video delivers)
- [ ] Thumbnail accurately previews video content
- [ ] No prohibited content: hate speech, harassment, dangerous misinformation, election misinformation, dangerous health claims
- [ ] If AI-generated content: clear creative direction documented (not generic mass-produced); aesthetic varies enough from prior videos to avoid "repetitive" flag; channel "About" reflects content type
- [ ] Made-for-Kids designation correctly set (false for adult-targeted authority content even when topic-adjacent)
- [ ] No unattributed copyrighted material — music licensed, footage licensed or fair-use-defensible

### Section B — FTC disclosure audit (Domain 2)

- [ ] Affiliate links disclosed in-description with FTC-compliant language
- [ ] Affiliate disclosure also spoken in-video at the affiliate pitch beat
- [ ] Sponsorships labeled as sponsored
- [ ] No deceptive product claims (specific outcome promises require substantiation)

### Section C — Medical/legal/financial disclaimer audit (Domains 3-4)

- [ ] Required disclaimer present in description for YMYL niches
- [ ] "Composite" labeling on patient/case stories
- [ ] No specific outcome guarantees ("will reverse," "guaranteed to recover")
- [ ] "Consult a [professional]" recommendation present where relevant
- [ ] Cited authorities are real and statements attributed to them are accurate

### Section D — Defamation audit (Domain 5)

- [ ] Every named individual or institution accused of wrongdoing has primary source on file
- [ ] Accusations framed appropriately ("alleges" / "according to [source]" not bare assertions)
- [ ] If the named subject responded, response is acknowledged or addressed
- [ ] Evidence package archived in `research/[video_id]/evidence/`

### Section E — Fact-check audit

- [ ] Every Risk Class 2+ claim has primary source on file
- [ ] All dollar amounts verified against source
- [ ] All dates verified against source
- [ ] All cited studies verified to exist with the cited findings
- [ ] All form numbers / regulation references verified (cite Skill 10's mishear dictionary too)
- [ ] All named people verified to exist and quoted statements verified

### Section F — AI-content tell audit

- [ ] Title doesn't use banned phrases ("you won't believe," "everything you need to know," "ultimate guide" + year stamp)
- [ ] Script doesn't use banned phrases (Skill 03 §9 anti-AI cliché filter)
- [ ] Thumbnail doesn't have AI-rendered text (text-in-code rule)
- [ ] Visual aesthetic differs enough from prior video to avoid "mass-produced" flag (especially for AI-driven channels)
- [ ] Voice consistency vs voice over-modulation (not too perfect, slight variance present)

### Section G — Brand risk audit

- [ ] Tone matches Channel Profile voice DNA
- [ ] No content that would alienate the channel's core retain-cohort
- [ ] No language that would not survive a careful reading by a hostile journalist (the "screenshot test")
- [ ] If video is news-pegged, news beat is still current (not a stale outrage)

If any item fails, fix the failing item. If the failure is structural (e.g., missing primary source for a Critical claim), the video does NOT ship until the fix is in. There is no exception clause.

---

## 7. The fact-checking workflow

Every Risk Class 2+ claim runs through this verification.

### Step 1 — Extract claims

Read the script. Tag every load-bearing claim:
- Specific numbers / dollar amounts / percentages
- Specific dates
- Specific named individuals or institutions
- Specific cited studies / papers / authorities
- Specific form numbers, statutes, regulations
- Specific quoted statements

### Step 2 — Classify risk

Each claim → Risk Class 1-4 per §4.

### Step 3 — Pull primary source

For each Risk Class 2+ claim:
- Identify what the primary source would be (gov.gov for federal, journal for studies, court docs for legal, company filings for corporate)
- Pull the source
- Verify the claim against the source
- Note the source URL, access date, and excerpt that supports the claim

### Step 4 — Counter-source check (Risk Class 3+)

For each Risk Class 3+ claim:
- Search for counter-sources (sources that contradict or qualify the claim)
- If counter-sources exist, decide: (a) drop the claim, (b) frame the claim with the qualification, (c) defend the claim with stronger evidence

### Step 5 — Authority check (cited expert / institution claims)

For each cited authority:
- Verify the person/institution exists and is accurately credentialed
- Verify the statement attributed to them is accurate (look at the actual interview, paper, or quoted passage)
- If paraphrasing, verify the paraphrase doesn't materially distort

### Step 6 — Build the source package

Store all sources in `research/[video_id]/sources.md`:
```
SOURCE PACKAGE — [video title]

CLAIM: [verbatim from script]
RISK CLASS: [1-4]
SOURCE TYPE: [primary federal / peer-reviewed / news / company filing / etc.]
SOURCE URL: [link]
ACCESS DATE: [YYYY-MM-DD]
EXCERPT SUPPORTING CLAIM: "[quoted excerpt from source]"
COUNTER-SOURCE: [if any, link]
DECISION: [verified / qualified / dropped]
```

This package is archived even after upload. If a claim is later challenged, the agent has the verification on file. This is the difference between defending a claim ("I cited a real source verified at the time") and being indefensible ("I don't know where I got that").

### Step 7 — Update the script if needed

Drop any claims that didn't verify. Replace with verified alternatives or qualified versions. Re-run the script audit (Skill 03) to ensure replacement claims fit the beat structure.

---

## 8. Disclaimer template bank

Five locked disclaimer templates the agent uses by niche.

### Template 1 — Senior finance / IRS / personal-finance authority

```
⚠️ DISCLAIMER
This video is for educational purposes only and does not constitute legal, tax, or financial advice. [Subject name] is based on a composite of real cases handled by the channel host's office. Consult a qualified tax professional, attorney, or financial advisor for your specific situation.
```

### Template 2 — Senior health / medical-authority channel

```
⚠️ DISCLAIMER
This video is for educational purposes only and does not constitute medical advice. [Patient name] is a composite of clinical patterns. Consult your physician before changing any supplement or dietary protocol. The author is not your treating physician.
```

### Template 3 — News-hijack documentary / investigation

```
⚠️ DISCLAIMER
This video discusses public allegations and the subject's responses where available. All factual claims are sourced; sources are linked in the description. The subject was contacted for comment [include result of contact attempt or note "did not respond"].
```

### Template 4 — True crime

```
⚠️ DISCLAIMER
Out of respect for the families involved, this video focuses on the verified public record. We do not speculate about persons of interest who have not been formally charged. If you have information relevant to this case, please contact [appropriate authority].
```

### Template 5 — Real estate / financial education

```
⚠️ DISCLAIMER
This video is for educational purposes only and does not constitute investment, tax, or financial advice. Past performance does not predict future results. Consult a licensed financial advisor before making any real estate or investment decision.
```

The agent never modifies disclaimer language without explicit user approval. Disclaimer modifications require legal review.

---

## 9. The post-July-2025 inauthentic content protocol

This is the highest-current-risk policy across our channels. Codified separately because the consequences are channel-level.

### Live signal triggers (April 2026, observed)

- **Mass-produced visuals:** same AI aesthetic across 50+ videos with no variation
- **Title pattern repetition:** identical formula structure across videos, identical formatting
- **Voice over-consistency:** TTS-perfect voice with no breath, no variation, no human cadence
- **AI-rendered text in thumbnails:** warped letters, inconsistent kerning
- **Generic mass-content phrases:** "in this video we'll explore," "let's dive in," "today we're talking about"
- **Channel "About" / metadata mismatch:** AI-generated content channel claiming to be a "creator's personal channel" with no AI disclosure
- **Engagement pattern anomalies:** consistently high view-to-engagement ratios that don't match human watch behavior

### Mitigation protocol

1. **Visual variation:** every video must differ visibly from the prior video. Different establishing shots, different B-roll mix, different host shots. Same Channel Profile but not identical aesthetic.
2. **Title formula rotation:** rotate among 3-5 title formulas across videos rather than always using the same one. The agent's library (Skill 01) supports this.
3. **Voice variation:** keep voice DNA locked but allow micro-variation per script. Don't strip breath, don't auto-correct cadence to perfection.
4. **Caption craft:** anchor-aligned (Skill 10), not auto-captions. Auto-captions are a tell.
5. **Channel "About" honesty:** if the channel is AI-driven, the About should reflect the content type (e.g., "AI-generated educational content reviewed by [credentialed expert]" — personal-finance authority channel model).
6. **Anti-cliché filter:** Skill 03 §9 banned phrase list applied to every script.

If a channel is at high inauthentic-content risk, the agent surfaces the risk explicitly in the pre-flight report. The user makes the ship/don't-ship call.

---

## 10. Anti-patterns

Eight pre-flight failures that have killed channels.

### Anti-pattern 1 — Skipping pre-flight on "obvious" videos

Channel has been running clean for 30 episodes; pre-flight feels redundant. Episode 31 has a Critical claim nobody verified. Channel struck.
**Fix:** pre-flight runs every video. No exceptions for "obvious" content.

### Anti-pattern 2 — Trusting ChatGPT-style citations

Citing a study by name and journal without pulling the actual paper. AI models hallucinate citations frequently. The cited paper either doesn't exist or doesn't say what was claimed.
**Fix:** every citation pulled from primary source, URL recorded. No exceptions.

### Anti-pattern 3 — "Composite" labeling missing

A patient story is presented as if real, no composite labeling. Viewer treats it as factual; if any detail is wrong, viewer credibility shock + potential defamation if "Patricia" matches a real person.
**Fix:** all patient/victim/case stories explicitly labeled as composites in script delivery AND in description disclaimer.

### Anti-pattern 4 — Stale news-pegged content

Video produced over 7 days; news beat that anchored the hook has moved on. Audience reads the video as out-of-touch.
**Fix:** for news-pegged content, ship within 48-72 hours of the news beat. If production exceeds the window, reframe with timeless hook OR shelve.

### Anti-pattern 5 — Affiliate disclosure only in description

In-video affiliate pitch with no in-video disclosure. FTC enforcement action separate from YouTube enforcement.
**Fix:** in-video disclosure spoken at the affiliate pitch beat.

### Anti-pattern 6 — Same thumbnail aesthetic across 50 videos

Mass-produced flag. AI-content algorithm picks up the visual repetition signal.
**Fix:** vary thumbnail elements per video (different face, different object, different color anchor) within the channel's pattern library.

### Anti-pattern 7 — "Reverse" / "cure" / "treat" without peer-reviewed evidence

Health video uses "reverse type 2 diabetes" without citing the specific peer-reviewed evidence supporting reversal claim. Medical misinformation flag risk.
**Fix:** if claim is unsupported, replace verb with "support" / "manage" / "improve markers for." If supported, cite the specific paper.

### Anti-pattern 8 — Naming a real person as wrongdoer without source on file

Investigation video names a person, accuses them of wrongdoing, no primary source archived. Sued for defamation. Channel demonetized during litigation.
**Fix:** evidence-on-file standard from §3 Domain 5. No source = no name in script.

---

## 11. Worked examples

### Example 1 — personal-finance authority channel example episode pre-flight (clean pass)

```
PRE-FLIGHT — example episode Social Security May Shake-Up

Section A — Platform policy: PASS
  Title accurately previews video content
  Thumbnail accurately previews
  No prohibited content
  Channel About correctly reflects "AI-assisted educational content with credentialed expert review"
  Made-for-Kids: false (adult retiree audience)

Section B — FTC disclosure: PASS
  In-description: tax-software affiliate affiliate disclosed
  In-video: affiliate disclosure spoken at Beat 10
  No undisclosed sponsorships

Section C — YMYL disclaimer: PASS
  Standard personal-finance authority channel disclaimer in description
  "Composite" labeling on Patricia's story (in script + in description)
  "Consult a qualified tax professional" recommendation present

Section D — Defamation: PASS
  No real-named individuals accused (composite-only)
  Treasury / SSA institutional references cite official sources

Section E — Fact-check: PASS (12 claims verified)
  - "Treasury rule effective April 2026" — verified, treasury.gov source
  - "37% redirection cap" — verified, SSA published rate
  - "Form SSA-7004 is one page" — verified
  - "Statute of limitations is 10 years" — verified, 31 USC §3716
  [...8 more claims verified...]

Section F — AI-content tell: PASS
  No banned phrases in title or script
  No AI-rendered text in thumbnail (text-in-code)
  Visual aesthetic differs from the prior episode (different establishing shot, different B-roll mix)
  Voice has natural variance

Section G — Brand risk: PASS
  Tone matches personal-finance authority channel DNA
  No content alienating the retain-cohort
  Screenshot test: passes

DECISION: SHIP
```

### Example 2 — Hypothetical health video pre-flight (failing pass)

```
PRE-FLIGHT — Hypothetical: "This 1 Vitamin Reverses Dementia At Any Age"

Section A — Platform policy: FAIL
  Title makes specific medical reversal claim without supporting evidence
  Risk: medical misinformation policy

Section C — YMYL disclaimer: WARN
  Even with disclaimer, "reverses dementia" claim is too strong for unsupported framing

Section E — Fact-check: FAIL
  "Reverses dementia" — no peer-reviewed evidence supports this for any single vitamin
  Cited 2023 study does not actually claim reversal (paraphrase distortion)

Section F — AI-content tell: WARN
  Thumbnail uses generic AI doctor stock-style image
  Title formula matches 50 prior similar videos in the niche

DECISION: DO NOT SHIP

REQUIRED CHANGES:
1. Title: replace "Reverses" with "Supports" or "May Slow"
2. Script: remove paraphrase distortion of 2023 study
3. Citation: pull the actual paper, quote the actual finding
4. Thumbnail: re-shoot with channel-original imagery
5. Re-run pre-flight after changes
```

### Example 3 — News-hijack documentary pre-flight (investigative-journalism style)

```
PRE-FLIGHT — Hypothetical: "I Spent 6 Weeks On This $40M Crypto Scam"

Section A — Platform policy: PASS

Section B — FTC: PASS

Section D — Defamation: REQUIRES EVIDENCE PACKAGE
  Named subject: [Person X]
  Accusation: orchestrating $40M crypto scam
  Evidence package required:
    - Source 1: company filings (SEC) — required
    - Source 2: court documents (if litigation initiated) — required if available
    - Source 3: subject's response (if any) — required to acknowledge
    - Source 4: independent journalism corroborating claim — strengthens
  WITHOUT this package: video does NOT ship

Section E — Fact-check: PASS pending evidence package

DECISION: HOLD until evidence package complete
```

---

## 12. Cross-skill connections

This skill is the FINAL gate before upload. Every prior skill's output is verified here:

- **Skill 01 (Title):** title accurately previews content (Section A) and isn't an AI-content tell (Section F)
- **Skill 02 (Thumbnail):** thumbnail accurately previews content (Section A) and doesn't have AI-rendered text (Section F)
- **Skill 03 (Script):** all claims fact-checked (Section E); banned phrases filtered (Section F)
- **Skill 04 (Description):** disclaimer present (Section C); affiliate disclosure present (Section B)
- **Skill 05 (Reference Channel):** channel positioning audited for inauthentic-content risk (Section A)
- **Skill 06 (Storyboard):** visual aesthetic varies enough to avoid mass-produced flag (Section F)
- **Skill 07 (i2v):** AI-rendered tells absent (Section F)
- **Skill 08 (Voice/TTS):** voice has natural variance (Section F)
- **Skill 09 (Image Generation):** text-in-code rule honored (Section F)
- **Skill 10 (Captions):** anchor-aligned not auto-captions (Section F); mishear dictionary applied for fact-check accuracy
- **Skill 11 (Audio Mixing):** loudness within YouTube spec (not a compliance issue but a delivery spec)
- **Skill 12 (Outlier Mining):** outlier replication applied differentiation rule (Section A — avoiding repetitive content flag)

When pre-flight finds a failure, the fix is in the upstream skill, not in pre-flight. Pre-flight catches the failure; the upstream skill produces the fix.

---

## 13. The user's role

Pre-flight is run by the agent, but ship/don't-ship decisions on items flagged WARN (not FAIL) live with the user. Specifically:

- **FAIL items:** the agent does NOT ship. No exceptions.
- **WARN items:** the agent surfaces the risk and asks the user to decide. User has context the agent doesn't (legal counsel availability, business risk tolerance, brand priorities).
- **PASS items:** the agent ships.

The user owns brand risk and litigation risk. The agent owns mechanical compliance verification.

---

## 14. Runtime checklist

Before any video reaches upload:

- [ ] Pre-flight Section A (platform policy) passed
- [ ] Pre-flight Section B (FTC disclosure) passed
- [ ] Pre-flight Section C (YMYL disclaimer) passed
- [ ] Pre-flight Section D (defamation) passed (or evidence package complete)
- [ ] Pre-flight Section E (fact-check) passed (all Risk 2+ claims verified)
- [ ] Pre-flight Section F (AI-content tells) passed
- [ ] Pre-flight Section G (brand risk) passed
- [ ] Source package archived in `research/[video_id]/sources.md`
- [ ] Evidence package archived for any Risk Class 3+ claims in `research/[video_id]/evidence/`
- [ ] Disclaimers match the niche template from §8
- [ ] User has decided on any WARN items
- [ ] No FAIL items remain unaddressed

If any check fails or any FAIL item remains, the video does not ship. Pre-flight is non-negotiable. The cost of pre-flight is 15-30 minutes; the cost of skipping it is the channel.
