---
name: ai-host-setup
description: >-
  Profession matrix for AI host generation (15 professions: doctor, financial advisor, teacher, war veteran, etc.). Load when setting up an AI host character. Sibling files: host-doctor.md, host-financial-advisor.md, host-teacher.md, host-war-veteran.md (worked profession playbooks).
---

# Tactical Playbook T1 — AI Host Setup (Photoreal Authority)

This is the master playbook for generating any AI avatar host that looks authentic across any profession or niche. The agent uses this for talking-head channels, doctor/health channels, finance/tax channels, education channels, and any channel where the host is the brand.

The single rule that governs every output: **the host must look like a real person who turned on a webcam at home, NOT like an actor in a studio shoot.** The audience for authority content is wary of polish. Studio gloss reads as "ad," authentic at-home reads as "person sharing what they know."

This playbook replaces the per-profession host playbooks. The agent fills in the **Profession Matrix** (§3) with channel-specific variables and runs the universal rules.

---

## 1. The job of an AI host

The host is the channel's face. Every thumbnail, every B-roll cutaway, every avatar lip-sync starts from the host reference. A weak host generation produces a channel that feels generic; a strong host generation produces a channel the audience recognizes after seeing two thumbnails.

The agent's job:
1. **Pick the right register** — authentic-at-home vs slightly polished, based on niche
2. **Fill the profession matrix** — visible credibility, setting, wardrobe, age range, demographic
3. **Match visual age to voice age** — generated visual age within ±10 years of the synthesized voice age
4. **Generate 4 variants and run sample-confirm gate** — never lock a host without explicit user approval
5. **Lock to channel memory** — host reference becomes the canonical face for ALL future visuals

## 2. The universal non-negotiables (every host)

These rules apply across every profession:

1. **Authentic home or near-home setting** — never a studio backdrop. Wood desk, shelves, window, lamp, personal items. ONE framed credential, not three. Slight imperfection is good.
2. **Natural lighting + lamp fill** — never three-point cinematic. Window from one side, warm lamp from another. The eye registers studio lighting in 0.3 seconds and trust collapses.
3. **Mid-shot framing, slightly off-center** — chest-up, slight angle, as if a webcam is propped at a desk. Never centered portrait, never close-up, never wide.
4. **Slight upward camera angle (eye level or just below)** — webcam at desk position, NOT lifted-up dramatic angle.
5. **Near-direct eye contact** — looking just slightly off-camera reads as natural conversation. Locked-on direct eye contact reads as posed. Looking far off-camera reads as dramatic "thousand-yard stare."
6. **Natural expression** — calm, slightly engaged, sometimes warmth. Never broad smiling (uncanny). Never scowling (intimidating). Never staring intensely (uncanny valley).
7. **Realistic skin texture** — visible age detail, slight imperfections, no AI-skin-smoothing. The "perfect skin" tell is the #1 indicator of AI-generated host content.
8. **Wardrobe matches profession** — see profession matrix below. Always one register more casual than the formal version (doctor at home wears a polo, not a lab coat; lawyer at home wears a button-down, not a suit).
9. **Age 35-75, matched to voice DNA** — must align with synthesized voice age within ±10 years.
10. **Photorealistic** — never illustrated, anime, 3D, stylized.

## 3. The profession matrix

The agent fills this matrix per channel. Each row is a profession; each column is a variable that adjusts.

| Profession | Visible credibility | Setting | Wardrobe | Age range | Expression | Voice DNA |
|---|---|---|---|---|---|---|
| **Doctor** | Stethoscope OR framed degree OR medical book | Home study with bookshelf, modest | Polo / collared button-down / casual sweater | 45-65 | Calm concern | V2 Federal Credentialed Expert (medical) |
| **Tax pro / financial advisor** | Single CPA/EA framed cert OR tax code book | Wood home desk, books, plant | Navy/charcoal collared button-down or polo | 38-55 | Calm serious | V2 Federal Credentialed Expert |
| **Retired teacher** | Books on shelf, single small certificate | Cozy home study, kitchen, sunroom | Cardigan over blouse/shirt | 60-75 | Warm soft | V5 Mentor Coach or V15 Wise Elder |
| **War veteran** | Single shadow box (subtle), folded flag, unit photo | Wood-paneled den, leather chair | Flannel / henley / plain button-down | 55-85 (era-dependent) | Weighted calm | V3 War Veteran Storyteller |
| **Pastor / faith leader** | Bible visible, simple cross, religious books | Modest home study, calm tones | Casual button-down or sweater | 45-70 | Reverent warmth | V15 Wise Elder |
| **Mechanic / tradesman** | Tools subtly visible, work hat on desk | Garage, workshop, modest home | Plain t-shirt, work shirt, henley | 35-65 | Direct casual | V5 Mentor Coach (blue-collar variant) |
| **Chef / cook** | Apron hanging, single cookbook visible | Home kitchen with character (not show kitchen) | Apron over plain shirt, OR plain shirt | 30-65 | Warm open | V12 Curious Kid Adult or V9 Best Friend |
| **Real estate agent** | Single license framed, modest home photos | Home desk OR exterior with property visible | Polo / collared shirt | 35-55 | Confident warm | V5 Mentor Coach |
| **Lawyer** | Single bar association cert, law books | Wood home desk, books | Button-down without tie, light sweater | 40-65 | Measured authority | V2 Federal Credentialed Expert (legal) |
| **Nurse / health practitioner** | Stethoscope, scrubs hanging (not worn) | Home study or modest clinical | Casual professional, not scrubs | 30-60 | Warm authority | V2 Federal Credentialed Expert (medical) |
| **Professor / academic** | Wall of books, framed degree (one), reading glasses | Home study with significant books | Cardigan over shirt, casual button-down | 45-75 | Curious calm | V1 Documentary Authority or V8 Erudite Professor |
| **Scientist / researcher** | Lab equipment subtle, science book | Home study, garage workshop | Plain shirt, casual blazer optional | 35-65 | Curious analytical | V12 Curious Kid Adult or V1 Documentary Authority |
| **Fitness coach** | Athletic gear subtle, no full-gym setup | Home gym corner, plain wall | Athletic-casual, fitted but not flashy | 30-55 | Energetic warm | V5 Mentor Coach or V6 Hype Showman |
| **Comedian / personality** | Personality items (records, posters), eclectic | Personal lived-in space | Whatever fits the personality, often quirky | 25-55 | Loose expressive | V7 Deadpan Cynic or V9 Best Friend |
| **News commentator** | Subtle: news magazines, neutral wall | Modest home setup, slightly more polished than other professions | Collared shirt, sometimes light blazer | 35-60 | Neutral focused | V10 News Anchor |

### How the matrix works

User says: "I want a channel about home plumbing tips with a real-feeling host."
Agent recognizes: profession = mechanic/tradesman variant. Loads matrix row.
- Visible credibility: tools subtly visible, work hat
- Setting: garage or modest home workshop
- Wardrobe: plain work shirt, jeans
- Age range: 35-65
- Expression: direct casual
- Voice DNA: V5 Mentor Coach blue-collar variant

Agent fills the master prompt template (§5) with these variables, generates 4 variants, runs sample-confirm gate.

## 4. The universal do-not list

Across every profession:

- ❌ Studio backdrop (gray seamless, ring light, professional setup)
- ❌ Three-point cinematic lighting (key + fill + rim)
- ❌ Multiple framed credentials displayed prominently (over-credentialed reads insecure)
- ❌ Stock-photo "thumbs up" or "pointing at camera" pose
- ❌ Aggressive crossed-arms confrontational pose
- ❌ Centered composition with subject staring directly forward in formal pose
- ❌ AI-smoothed skin, perfect teeth, perfect hair (uncanny-valley tells)
- ❌ Background blur so deep the room is unreadable (cinematic, not authentic)
- ❌ Wearing the formal version of professional attire (lab coat, full suit, etc.) in a home setting
- ❌ Holding a prop "casually" in a forced way (coffee mug raised mid-air, document held up)
- ❌ Period mismatch in props (1990s desk computer in a modern setting, etc.)
- ❌ Demographic mismatch with target audience (host that looks 25 for a content audience that's 65+)

## 5. The master prompt template

```
A [AGE] year old [GENDER] [PROFESSION] sitting at [SETTING], photographed as if they had set up a basic webcam to record a YouTube video about [TOPIC]. They are wearing [WARDROBE] in [COLOR], slightly worn-looking but clean. Visible behind them: [VISIBLE CREDIBILITY ITEMS — one or two, never three], with [PERSONAL ITEMS — books, plant, photo, mug] adding lived-in authenticity. The room is [LIGHTING DESCRIPTION — natural daylight from one side + warm lamp fill from another]. The subject is framed mid-shot from chest up, slightly off-center to the [LEFT/RIGHT], looking just to the [RIGHT/LEFT] of the camera with a [EXPRESSION] expression — [emotional register adjective]. Photorealistic, captured as if with a basic webcam at slight upward angle (eye level or just below), with realistic skin texture including subtle imperfections. Background is gently out of focus but readable. Aesthetic: real [PROFESSION] in their actual [SETTING TYPE], NOT studio shoot, NOT cinematic, NOT magazine portrait. 16:9, photorealistic, natural color grade, no filter.
```

The agent fills variables from the profession matrix + channel-specific user input.

## 6. Model selection

| Model | Performance | Use when |
|---|---|---|
| **Nano Banana (Gemini 3 Pro Image)** | ⭐⭐⭐⭐⭐ Best | Default for all photoreal hosts. Best at faces, skin texture, room authenticity. |
| Flux 1.1 Pro | ⭐⭐⭐⭐ Good | Backup if Nano Banana unavailable. Tends slightly cinematic; constrain explicitly. |
| Midjourney v7 | ⭐⭐⭐ OK | Tends to over-polish; needs aggressive "amateur webcam" prompting. |
| DALL-E 3 | ⭐⭐ Weak | Stock-photo aesthetic by default. Only use if other models fail. |

**Default: Nano Banana for all host generation.**

## 7. Voice DNA pairing rule

The visual age MUST track the voice age within ±10 years. A 30-year-old visual paired with a 60-year-old voice creates instant audience exit. The agent picks visual age and voice age in the SAME step:

1. Determine niche → voice archetype (Skill 03)
2. Determine voice age range from archetype (V2 Federal Credentialed Expert: typically 45-60; V15 Wise Elder: typically 65+)
3. Set host visual age to align within ±10 years
4. Generate host with voice age in mind
5. Sample-confirm voice + visual together — they must read as one person

When the user picks an avatar that doesn't match the voice, the agent flags: "This avatar reads younger than the voice will sound. Want me to age the avatar OR pick a younger voice?"

## 8. Sample-then-confirm gate (mandatory)

Never lock a host without explicit user approval.

1. **Generate 4 variants** — different seeds, slight wardrobe variation, slight age variation within range, slight room-style variation
2. **Surface all 4 to user** with brief reasoning per variant ("this one leans warmer, this one leans more authoritative")
3. **User picks one** OR asks for adjustments
4. **If approved**, lock to channel memory as the canonical host reference
5. **Use locked reference** for HeyGen avatar setup, all thumbnail face generation, all B-roll character shots

The agent never decides on its own which host to use. User picks.

## 9. The "real person at home" register vs polished alternatives

This playbook produces the **authentic-at-home** register by default. Some niches want a slight polish step up. Use Tactical Playbook T4 (Photoreal vs Cinematic Register Decision) to decide.

Quick guide:
- **Senior finance, IRS, retirement, doctor advice, faith content**: pure authentic-at-home (this playbook's default)
- **Tech reviews, science explainer**: slightly polished (cleaner room, slightly better lighting, but still home)
- **News commentary, premium documentary**: more polished (could be a styled home office)
- **Music videos, propaganda, music**: NOT this playbook — use cinematic playbook
- **Vlog / lifestyle**: this playbook with personality leaned in (more personal items, more energy in expression)

## 10. Per-channel host evolution

After 5+ episodes ship, the locked host reference may need refresh:
- Hair grows / styles change (subtle drift acceptable; major change is brand-jarring)
- Wardrobe rotation (1-2 alternate shirts/cardigans; not full reset)
- Seasonal subtle adjustments (lighter shirt in summer videos, slightly warmer in winter)

When the agent regenerates the host, it ALWAYS references the original locked image so face stays consistent (see T5 — Character & Visual Continuity Locking).

## 11. Anti-patterns specific to AI hosts

Beyond the universal do-not list:
- **The "stethoscope around neck always" tell** for doctors — vary it
- **The "diploma wall" failure** — one credential, never three
- **The chin-up "proud" pose** for veterans / hosts in general
- **The salesman handshake or thumbs-up** for finance hosts
- **The chalkboard background** for teachers
- **The Wall Street skyscraper view** for finance authority
- **Wearing the formal version of the uniform** at home

## 12. Why the rules

The audience for authority YouTube content has been burned for decades by polished sales-pitch presenters. The single trust signal that beats every credential is **looking like a real person, not a TV professional**. Studio gloss reads as "trying to sell me something." Authentic at-home presentation reads as "this person is sharing what they know with me directly."

This isn't theoretical — every successful authority channel we've teardown'd (a medical-authority channel, an economic-explainer channel, an investigative-journalism channel, a productivity creator, and similar personal-finance authority channels) follows this register. Every channel that uses studio polish in these niches underperforms. The eye registers the difference within 0.3 seconds.

## 13. Runtime checklist

Before locking any host:
- [ ] Profession matrix row identified
- [ ] Voice DNA pairing established (Skill 03)
- [ ] Visual age within ±10 years of voice age
- [ ] Master prompt template filled
- [ ] 4 variants generated with seed variation
- [ ] All 4 surfaced to user with reasoning
- [ ] User explicitly picks one (no agent-side default)
- [ ] Locked reference saved to Channel Memory
- [ ] Reference will be used for HeyGen + thumbnail face + B-roll character

## Update log

Current as of April 2026. Update when:
- New profession variants needed (add row to matrix)
- Image gen models change face rendering quality
- Authority niche aesthetic shifts (currently locked on authentic-at-home)
- Voice DNA archetypes are updated in Skill 03
