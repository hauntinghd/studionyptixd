# Empire Magnates — Fern-Style Storytelling + 15-Video Packaging Test

**Creator Doctrine:** `long_form/prompts/creator_doctrine.py` — anti-spectacle north star (Caseoh / [Last Honest Streamer](https://www.youtube.com/watch?v=LER7b_09NyM)). $13 lifetime YouTube revenue is a feature, not a bug. Same room, same cast, honest mechanism.

**Source training video:** [Jacksons AI — How I Make VIRAL 3D Documentary Videos 100% With AI](https://www.youtube.com/watch?v=--w3Rumz9sM) (86k views, May 2026)  
**Reference channels cited in video:** [Fern](https://sigmastory.in/fern-proves-high-end-3d-animation-and-documentaries-are-the-future-of-youtube/) (3D mannequin documentary essays), Simplicissimus, Lume-adjacent explainer channels  
**Transcript saved:** `analysis/jacksons_ai_transcript.txt`

---

## What Jackson/Fern actually do (extracted)

### Visual identity
- **3D mannequin cast** in photoreal environments — not face-cam, not stock footage talking heads.
- **Spatial storytelling:** camera moves through locations (boardroom → vault → street) as the narrator explains.
- **Character reference sheets first**, then every scene uses the same cast look (consistency = trust).
- EM already matches this with **red-porcelain mannequin + Coldfusion grade** — you are structurally aligned with Fern; the gap is **story packaging**, not art direction.

### Storytelling structure (the demo cold open)
Jackson's example VO (Dubai underwater city):

> *"It's the 4th of March, 2023. A marble hall in downtown Dubai. A man walks onto the stage. He clicks a remote. The lights dim. Behind him, something appears on the screen… They're under the sea. **So, why is one of the richest cities on Earth building down instead of up?**"*

**Pattern:**
1. **Date anchor** (specific, not "in 2023")
2. **Place anchor** (one room, one texture)
3. **Physical action** (walks, clicks, dims — no abstract intro)
4. **Visual surprise** (under the sea)
5. **Question hook** (why should I care?)

This is **not** your current EM opener (`"In 2019, a trader walked into…"`) — it's **in medias res cinema**, then the question.

### Production workflow (map to your fal stack)

| Jackson step | EM equivalent (fal.ai) |
|---|---|
| Claude master prompt + source PDF | Grok/Claude + `backend_script_prompts.py` + fraud research |
| Title ideas → pick one | **15-video matrix below** |
| Script sized to runtime | Chapter-based EM outline (v5) |
| Batched VO (ElevenLabs) | **fal MiniMax `English_Trustworthy_Man`** (batch by chapter) |
| Character reference images | Seedream cast sheet (red porcelain exec) |
| Chapter scene prompts | Per-scene `visual_description` in v5 |
| Multi-shot i2v per chapter | **LTX 13B distilled** i2v (~$0.04/clip) |
| Subtle music under VO | Optional — duck at 16%, loudnorm -14 LUFS (already in v5) |
| Thumbnail prompts + **reference image lock** | 3 Seedream thumbs + **one master ref PNG** |
| Brand color lock (green/black for Jackson) | **EM lock: red porcelain + deep black + white/red type** |

**Est. cost per 20-min episode:** ~$16 (see `channels.py` empire_magnates block).

---

## Scene consistency + thumbnail style (Fern bar)

**Reference frames:** Jackson demo — white mannequin head, dark suit, photoreal auditorium, teal holographic wireframe screen, warm spotlight, reflective floor, symmetrical composition.

### Why EM looks messy today

| Fern/Jackson | Empire Magnates today |
|---|---|
| **Neutral white/grey mannequin** — one silhouette, zero facial drift | **Red porcelain** — hue/sat shifts every Seedream call |
| **Reference image on every gen** (Jackson attaches ref to Flow) | **Text-to-image only** — no `image_urls` lock |
| **4 repeatable scene templates** (presentation, walk, data-viz, close-up) | **36 unique prompts** — lighting/cast re-invented per scene |
| **Holographic/data layer** behind character (wireframe, charts) | Flat boardroom stills |
| **Fixed lighting bible** (spotlight + cool screen + warm rim) | Coldfusion mentioned in prompt, not enforced shot-by-shot |
| **Thumbnails from same ref as scenes** | Thumbnails generated separately with `$XXXM` text — different aesthetic |

Your pipeline has a `cast_rule` text prefix in `v5_pipeline.py`, but **text alone cannot hold identity** across 36 scenes. That's the gap.

### The fix: EM Cast Kit (do this before any episode render)

**One-time setup (~$0.12–0.16 fal when credits return):**

1. Generate **3-angle cast sheet** (Seedream t2i, plain grey studio background):
   - A: full-body front, neutral pose, dark suit
   - B: chest-up portrait, slight 3/4
   - C: 3/4 back, head turned toward camera

2. Save as `cast/em_protagonist_ref_{a,b,c}.png` — reuse forever.

3. **All scene stills + all thumbnails** switch to [`seedream/v4.5/edit`](https://fal.ai/models/fal-ai/bytedance/seedream/v4.5/edit) with `image_urls: [ref_a, ref_b, ref_c]` — not plain t2i.

4. **Lock one seed** per episode session (same seed + same identity clause in every prompt).

### Cast aesthetic decision (pick one before Bre-X)

**Option A — Fern Neutral (recommended for consistency)**
- Smooth **matte white/grey mannequin head**, no features, dark suit
- Matches Jackson/Fern clean look exactly
- Easier for Seedream edit endpoint to hold identity
- EM brand becomes: *Loophole Files investigations* + voice + hologram UI — not red ceramic

**Option B — Keep red porcelain**
- Harder to keep consistent; only works with edit endpoint + locked seed
- Must drop saturation variance in prompts ("exact same red #C41E3A ceramic head")

### Lighting bible (paste into EVERY scene prompt)

```
Cinematic documentary lighting LOCK:
- Single warm overhead spotlight on mannequin (3200K)
- Cool teal/cyan fill from background screen or data display (6500K)
- Deep black shadows, restrained saturation, reflective floor if interior
- 24fps photoreal, NOT illustration, NOT comic
- Mannequin scale: human-proportioned, medium shot or wide with clear environment
```

### Four scene archetypes (cycle these — don't invent 36 layouts)

| Archetype | Use for | Composition |
|---|---|---|
| **PRESENTATION** | Cold opens, big reveals | Mannequin at podium, massive holographic screen behind (wireframe vault, stock chart, jungle drill map) |
| **WALK** | Transitions, "he walked into…" | Mannequin mid-stride in auditorium/corridor/trading floor, spotlight pool on floor |
| **DATA-VIZ** | Explaining the fraud mechanism | No mannequin OR small silhouette left; dominant teal UI overlay / wireframe / document scan |
| **CLOSE ACTION** | Arrests, signatures, discoveries | Hands on desk, stamp, ledger — mannequin chest-up edge of frame |

Jackson's Dubai demo = **PRESENTATION** archetype. Your Bre-X cold open = helicopter landing (DATA-VIZ + WALK), then PRESENTATION at investor pitch.

### Thumbnail style (replace current `$XXXM boardroom` default)

**Fern/Jackson thumb formula:**
- Same mannequin ref as scenes (edit endpoint)
- **Archetype PRESENTATION or DATA-VIZ** — not generic boardroom portrait
- **Teal holographic element** occupying 40%+ of frame (fraud-specific: wire transfer flow, audit stamp, gold assay chart)
- Mannequin **small-to-medium** — environment tells the story
- Text: **one line max** in YouTube title; on-thumb text optional and small (or none — Jackson refs often have UI chrome not giant $)

**Example Bre-X thumb:** Mannequin at podium + holographic **jungle drill site wireframe** with `$6B` in UI chrome (not floating white text).

**Example Wirecard thumb:** Mannequin silhouette + teal **audit document hologram** with `$1.9B` in corner badge.

### Pipeline code change (when credits return)

In `long_form/v5_pipeline.py`:
- Add `SEEDREAM_EDIT_URL = "https://fal.run/fal-ai/bytedance/seedream/v4.5/edit"`
- `_gen_em_still()` → upload cast refs, call **edit** not t2i
- `_gen_thumbnails()` → attach same cast refs + archetype hint
- `_gen_em_clip()` → only animate stills that passed **visual QA** (mannequin head still white/red-same, no real face)

**Validation cost before full episode:** ~$0.20 (3 cast refs + 2 test scenes + 1 test thumb).

---

## Casey Creator Doctrine (locked 2026-05-27)

**Why this exists:** You didn't keep making videos since 2018 for money ($13 YouTube lifetime, ~$1,700 once from streaming). You kept going because the room is worth building. Caseoh proved the anti-blueprint works at scale: same setup, same schedule, no spectacle chase — [The Last Honest Streamer](https://www.youtube.com/watch?v=LER7b_09NyM).

### Five rules (all channels)

1. **One room** — Same cast, same lighting bible, same visual world. Yellow porcelain auditorium = your vacuum-cleaner monitor stand. Don't "upgrade" into generic Netflix fraud aesthetic.
2. **Mechanism over flex** — Dollar amounts are facts in the story, not thumbnail screams. Hologram + UI badge, not giant floating `$6B`.
3. **Curious recluse voice** — Calm, precise, zero cable-news outrage. Optional contrast open: "Everyone burns money on camera — this is the loophole instead."
4. **No blueprint chasing** — No subathon energy, no manufactured drama, no guilt CTAs, no collab-for-relevance packaging.
5. **A place worth visiting** — End on one moral line. Not "smash subscribe weekly."

### EM-specific packaging (post-doctrine)

| Priority | Title family | Thumb | Notes |
|---|---|---|---|
| **Default** | **T2** date anchor | **Hologram-first** (approved Bre-X ref) | `March 19, 1997: When Bre-X Lost $6 Billion` |
| Alternate | T1 Loophole | Same hologram style | Wirecard repack only — not default |
| Deprioritize | T5 bracket punch | TH-A money hero | Spectacle blueprint — avoid unless A/B testing |

**Act -1 (optional, 15 sec):** Spectacle contrast → pivot to Fern cold open.

Example VO skeleton:
> *"Financial YouTube runs on one question: how much money can we burn on camera? Mansions. Outrage. Giant dollar signs. And then there's this fraud — which started with a helicopter and a lie. It's March 19, 1997…"*

---

## EM storytelling template (Fern-adapted)

Replace generic fraud intros with **scene-first cold opens**. Every episode:

### Act 0 — Cold open (0:00–0:50, ~2 scenes animated)
- `[DATE]. [ROOM]. [ONE ACTION]. [ONE OBJECT THAT SHOULDN'T EXIST]. [QUESTION.]`
- Example (Bre-X): *"It's March 19, 1997. A helicopter drops onto a jungle clearing in Borneo. Geologists jump out with canvas bags. Inside those bags: rocks that are supposed to contain $70 billion in gold. **So why did the biggest mining fraud in history start with a helicopter and a lie?**"*

### Act 1 — The loophole (min 2–5)
- Explain the **system** the fraud exploited (auditors, offshore law, market structure) — Fern's "background so you understand."

### Act 2 — The grift (min 5–12)
- Protagonist exploits it **step by step** (dollar amounts, dates, names).

### Act 3 — The crack (min 12–18)
- Journalist, short seller, or regulator finds the hole.

### Act 4 — Collapse (min 18–22)
- Arrest, bankruptcy, or legal "got away with it" ending.

### Outro (15 sec)
- One moral line + `Loophole Files` sign-off (no long CTA).

**Visual flow rule (from Jackson):** before generating scenes, write the **camera path** — "boardroom → trading floor → jungle → courtroom" — one sentence per chapter.

---

## Title + thumbnail — 15-video test matrix

**Goal:** escape sub-100-view / zero-impression jail (Mango) while keeping Wirecard's **Loophole** DNA.

### Five title families (test 3 videos each)

| ID | Family | Formula | Why test |
|---|---|---|---|
| **T1** | **Loophole** (control — Wirecard winner) | `The {profession} Who Legally {verb} ${amount} From {target}` | Your only proven EM CTR pattern |
| **T2** | **Fern date anchor** | `{Month} {Day}, {Year}: When {Company} Lost ${Amount}M` | Jackson demo + Fern spatial docs |
| **T3** | **Mechanism question** | `How {Name} Got Away With ${Amount}M For {Duration}` | Search intent + curiosity |
| **T4** | **System theft** | `The {System} That Stole ${Amount} From {Victims}` | Fern-style ("The Game That Stole $130M") |
| **T5** | **Bracket punch** | `{6-word shock claim} \| {Company}` | Secondary hook per [title data](https://www.subsub.io/blog/youtube-title-formulas-that-work) |

**Rules for all titles:**
- 41–60 characters, 7–9 words where possible
- **Statement > question** for long-form (data-backed)
- **No** "Rise and Fall", "Full Documentary", "The Story of"
- Include **$ amount** or **specific year** in 14/15 videos

### Three thumbnail families (rotate — hologram-first default)

| ID | Layout | Priority | Seedream prompt skeleton |
|---|---|---|---|
| **TH-H** | **Hologram hero** (default) | **USE FIRST** | Mannequin left-third small. **Teal wireframe** dominates (drill map, vault, wire flow). `$` in **UI badge** only. Matches `thumb_archetype_brex.png`. |
| **TH-B** | **Location hero** | Test | Mannequin small left. Iconic location dominates. `$` corner badge. |
| **TH-C** | **Question stamp** | Test | Mannequin profile right. Prop left (`LEGAL?` / `HOW?` in UI chrome, not rage caps). |
| ~~TH-A~~ | ~~Money hero~~ | **Avoid** | ~~Huge white `$XXXM`~~ — spectacle blueprint, conflicts with doctrine |

**Thumbnail rules (from Jackson + EM spec + doctrine):**
- **One master reference PNG** attached to every thumb gen (lock mannequin + grade)
- Same font hierarchy every time: `$` biggest → entity second → optional red stamp
- **Never** wide establishing shot — subject fills 40–55% height
- Generate **3 variants**, pick best; log which family in tracker

---

## 15 episodes — topic + packaging assignment

| # | Topic | Title family | Thumb | Primary title (draft) |
|---|---|---|---|---|
| 1 | **Bre-X** | T2 | TH-H | `March 19, 1997: When Bre-X Lost $6 Billion` |
| 2 | **Wirecard** (repack) | T1 | TH-B | `The Auditor Who Legally Missed $1.9B From Wirecard` |
| 3 | **Mango Markets** (repack) | T1 | TH-C | `The Trader Who Legally Stole $114M From Mango Markets` |
| 4 | **Parmalat** | T4 | TH-A | `The Dairy Empire That Stole $14B From Italy` |
| 5 | **Theranos** | T5 | TH-B | `She Faked 240 Blood Tests \| Theranos` |
| 6 | **FTX / SBF** | T3 | TH-C | `How SBF Got Away With $8B For 3 Years` |
| 7 | **Luckin Coffee** | T2 | TH-A | `January 2020: When Luckin Coffee Lost $4B` |
| 8 | **Enron** | T4 | TH-B | `The Energy Game That Stole $74B From Shareholders` |
| 9 | **Volkswagen dieselgate** | T5 | TH-C | `They Hacked Emissions Tests \| VW` |
| 10 | **Wirecard mirror (Autonomy)** | T3 | TH-A | `How Autonomy Got Away With $11B For 10 Years` |
| 11 | **Nikola (trevor milton)** | T2 | TH-B | `September 2020: When Nikola Lost $34B` |
| 12 | **1MDB / Jho Low** | T1 | TH-C | `The Financier Who Legally Moved $4.5B From 1MDB` |
| 13 | **Archegos / Bill Hwang** | T4 | TH-A | `The Family Office That Stole $10B From Banks` |
| 14 | **Carlos Ghosn escape** | T5 | TH-B | `He Escaped In A Box \| Nissan` |
| 15 | **WorldCom** | T3 | TH-C | `How WorldCom Got Away With $11B For 5 Years` |

**Tracker columns (Google Sheet or Notion):** video #, title family, thumb family, 48h impressions, CTR, AVD%, 7d views, winner flag.

**Decision rule after 15:**
- Best **title family** = default for next 10 videos
- Best **thumb family** = default layout; keep testing stamp text (`LEGAL?` vs `$` vs date)

---

## fal.ai render checklist (per episode)

1. Research PDF / notes → Fern-style cold open paragraph
2. Chapter outline with **visual flow** sentence
3. Seedream **cast reference sheet** (1 image, reuse)
4. ~36 scene stills + LTX clips + MiniMax VO (batched by chapter)
5. Skip mmaudio on first 3 test episodes if budget tight (~$1.80 saved)
6. 3 thumbnails (Seedream) with master ref attached
7. Export + `.txt` upload pack with A/B alt title

---

## Immediate next action

**Episode 1: Bre-X** — Act -1 contrast (optional) + Fern cold open + **T2 title + TH-H thumb** (approved `thumb_archetype_brex.png`).
This is the first clean test of storytelling upgrade + packaging matrix slot #1.

When ready: `"Build EM Bre-X episode 1 Fern cold open"` and we run v5 with the new script structure.
