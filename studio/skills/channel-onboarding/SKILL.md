---
name: channel-onboarding
description: >-
  Full discovery + lock-in protocol for a fresh channel. Niche-pattern-first architecture — identifies the production archetype from the user's description, loads the matching pipeline template, and asks only the channel-specific questions that template genuinely needs. Covers 12 production archetypes (avatar long-form, stickman explainer, vertical shorts, music video, documentary, investigation, cinematic, picture-education, gaming, ambient, compilation, real-footage) and 22 niche register defaul
---

# Channel Creation — RookCast onboarding protocol

This is the load-bearing skill that takes a brand-new channel from "Untitled" to fully-shaped. The previous version was phase-linear (8 generic phases for every channel). This version is **niche-pattern-first** — the agent identifies the production archetype from the user's description, loads the matching pipeline template from the bank, and asks only the questions that template genuinely needs.

The output is the same: a locked CHANNEL.md + a locked FLOW.md the production agent walks for every video. The path is shorter, the defaults are smarter, and the underlying data understands what AI tools depend on what.

## When to load

- `channelPhase === "untitled"` — fresh channel, name is "Untitled"
- `channelPhase === "discovery"` — partial Channel Profile, user wants to keep shaping
- User explicitly asks to "rebuild" / "redefine" / "redesign" the channel

**Defer to `import-existing-channel`** when the user's first message contains a YouTube channel URL — that skill ingests the source channel directly, no cold-start interviewing needed.

**Don't load on shaped channels.** The production protocol takes over there.

## Companion files (load with this)

- **`pipeline-bank.md`** — 12 production archetype templates with full dependency graphs, AI tool stacks, step sequences, and credit cost estimates per video. The data this protocol routes against.
- **`niche-bank.md`** — 22-niche quick-reference register. Voice archetype + visual register + length target + thumbnail pattern + compliance domain × every supported niche.
- **AI tool tier doc** (RookCast v1 stack) — Nano Banana Pro (premium image), Flux Pro 1.1 (mid image), Flux Schnell (open source), Kling 3.0 / Turbo 2.6 / Wan 2.2 (video), Minimax Speech-02 HD / F5-TTS (voice), Suno v5.5 (music), HeyGen / Hedra Character-3 / Hallo3 (avatars). These are the locked picks at three price tiers — the protocol uses them as the default options unless the user overrides.

---

## Architecture overview — the two-pass model

The previous skill ran 8 sequential phases regardless of channel. This version runs **two passes** with the bank doing the routing in between.

### Pass 1 — Identify the archetype (fast, ~1-3 questions)

Read the channel description first. Extract whatever you can without asking:
- Niche / topic
- Audience hint
- Visual format hint
- Length hint
- Register hint

Match what's left to one of the **12 production archetypes** in the pipeline bank:

| # | Archetype | Trigger words / patterns |
|---|---|---|
| 1 | `avatar_authority_longform` | tax, finance, retirement, medical, legal, IRS, doctor, attorney, "expert explains" |
| 2 | `stickman_explainer` | explainer, education, "how does X work", animated explainer, infographic, science explainer |
| 3 | `vertical_shorts_hype` | shorts, TikTok-style, Roblox, "what if", POV, gaming hype |
| 4 | `music_video_propaganda` | music video, drill, satire, propaganda, news-pegged music, brick-narrative-style |
| 5 | `documentary_voiceover` | documentary, video essay, long-form mystery-documentary style, deep-dive, history |
| 6 | `news_hijack_investigation` | investigation, exposé, investigative-journalism style, scam exposure, named villain |
| 7 | `cinematic_ai_documentary` | cinematic, atmospheric, narrated true crime, premium video essay |
| 8 | `picture_education` | slideshow, archival, wildlife-encounter compilation style, photoreal historical |
| 9 | `gaming_animation` | fandom-lore battle, character vs character, gaming lore, animated gaming |
| 10 | `ambient_loop` | ambient, sleep, focus, study, 1-3 hour, lo-fi |
| 11 | `compilation_supercut` | compilation, supercut, reaction compilation, top 10 |
| 12 | `talking_head_real_footage` | vlog, real footage, lifestyle, real host (no AI avatar) |

Most channel descriptions trigger one archetype with high confidence. If two are plausible, ask one disambiguating `ask_user` question. **Never ask 8 generic questions when 1 specific one will do.**

### Pass 2 — Load the template, ask only what it needs

Once the archetype is locked, load the matching section of `pipeline-bank.md`. That section specifies:
- The dependency graph (what generates first, what depends on what)
- Which AI tool tiers are valid for this archetype (often only 1-2 of the 3 tiers make sense)
- Which questions actually need the user's input (vs. which can be defaulted from the niche)
- The credit cost estimate per video at each available tier

Most archetypes only have 2-4 channel-specific questions worth asking. Run those, hold the answers in memory, then go straight to the proposal phase (Phase H from the previous skill — that part stays).

---

## The decision flow in detail

### Step 1 — Read the description

Before any tool call, parse the channel description. Extract:

```
NICHE: <inferred topic>
AUDIENCE_HINT: <general | informed | expert | unknown>
VISUAL_HINT: <avatar | voiceover | animated | compilation | unknown>
LENGTH_HINT: <shorts | medium | longform | ambient | unknown>
REGISTER_HINT: <authority | hype | satire | documentary | reverent | unknown>
ARCHETYPE_GUESS: <one of 12 from the table above, or "ambiguous">
```

If `ARCHETYPE_GUESS` is unambiguous AND `NICHE` is locked, **skip Pass 1 entirely** and load the matching template from the bank. Go to Step 3.

### Step 2 — Disambiguate (only if needed)

If ambiguous, run ONE batched `ask_user` with the minimum questions needed. Examples:

**Description:** "I want to make explainer videos about money."
- Niche: locked (personal finance)
- Visual: ambiguous (could be avatar authority, stickman explainer, or doc voiceover)
- → Ask ONE question with 3 options matching those three archetypes, with sample channels for each.

**Description:** "Roblox shorts."
- Niche + length + format all locked → no questions, archetype = `vertical_shorts_hype`. Skip to Step 3.

**Description:** "a creepypasta channel."
- Niche locked (true crime / horror narrative) → archetype `cinematic_ai_documentary` is most likely. Ask ONE confirming question only if you genuinely can't tell vs `documentary_voiceover`.

The bias is toward asking fewer questions, not more. Phase A in the old skill asked audience level + niche; both are usually inferrable from the description. Don't pad.

### Step 3 — Load the archetype template

From `pipeline-bank.md`, load the section matching the locked archetype. The section contains:

1. **Dependency graph** — the order of generation (e.g., "Suno track FIRST, then BPM-locked image gen")
2. **Locked AI tools** — which RookCast v1 tiers are valid (most archetypes lock to 1-2 specific tools, not all 3 tiers)
3. **Channel-specific questions** — the 2-4 questions this archetype genuinely needs
4. **Credit estimate per video** — at each available tier
5. **FLOW.md template** — pre-drafted for this archetype, with the dependency order baked in

### Step 4 — Run the channel-specific questions (minimum viable set)

The bank tells you what to ask. For most archetypes it's 2-4 questions. Some examples:

- `avatar_authority_longform` — needs avatar pick (Phase D from old skill, 3-headshot picker), voice pick, host name (optional)
- `stickman_explainer` — needs character base (1 of 3 illustration styles), narrator voice, music register
- `music_video_propaganda` — needs genre (drill / trap / orchestral / EDM), BPM lock, visual aesthetic (brick-narrative / claymation / photoreal stylized)
- `vertical_shorts_hype` — needs voice prompt (Minimax custom voice description), aspect ratio confirmation
- `documentary_voiceover` — needs narrator voice, music palette, length target

Run the questions in **one batched `ask_user` call** when possible. Two batched calls if a downstream question depends on an earlier answer (e.g., voice depends on avatar gender for paired previews).

### Step 5 — Approval cadence

ALWAYS ask one question regardless of archetype:

```javascript
ask_user({
  questions: [{
    id: "approval_cadence",
    prompt: "Should I check in at every step (title, thumbnail, script, etc.) or just generate the full video and show you the result?",
    type: "single_choice",
    options: [
      { id: "high_touch", label: "Check in at each step", description: "You approve title, thumbnail, script, and final. Slowest but you stay in the loop." },
      { id: "low_touch", label: "Just ship it — show me the final", description: "I'll make the calls. Fastest." }
    ]
  }]
})
```

This shapes how many `ask_user` pauses live in FLOW.md.

### Step 6 — Thumbnail DNA (always one batched call)

The thumbnail picker stays from the previous skill — it's universal across archetypes. ONE batched `ask_user` with three questions: style archetype, recurring elements, selection workflow. See "Thumbnail picker" section below for the locked shape.

### Step 7 — Render the proposal via `propose_channel`

The bank gave you a pre-drafted FLOW.md template specialized for the archetype. Combine with the user's answers, draft a CHANNEL.md, and call `propose_channel({ proposalKind: "channel", ... })`.

Same three resolution paths as before:
- **Confirm** → tool writes CHANNEL.md + FLOW.md to disk. Don't re-write via the Write tool.
- **Request Edit** → user typed an edit. Redraft affected sections, call `propose_channel` again.
- **Free chat reply** → next turn, fold feedback into a revised proposal.

---

## Reading the channel description — extraction patterns

Get good at this. Every question you can skip by reading the description carefully is decision-budget the user keeps.

### Strong signals (lock the archetype with high confidence)

| Phrase in description | Lock |
|---|---|
| "tax channel for retirees" / "IRS expert" / "financial advisor" | `avatar_authority_longform` |
| "explainer" + "animated" / "stickman" / "science-animation style" | `stickman_explainer` |
| "Roblox shorts" / "TikTok-style what if" / "POV vertical" | `vertical_shorts_hype` |
| "drill music video" / "AI music videos" / "satirical music" | `music_video_propaganda` |
| "video essay" / "long-form mystery-documentary style" / "deep-dive documentary" | `documentary_voiceover` |
| "investigative-journalism style" / "expose" / "investigative" | `news_hijack_investigation` |
| "creepypasta" / "true crime narrated" / "atmospheric horror" | `cinematic_ai_documentary` |
| "archival photos" / "historical slideshow" / "wildlife-encounter compilation" | `picture_education` |
| "fandom-lore battle" / "character vs character animation" | `gaming_animation` |
| "1-hour ambient" / "lofi study" / "sleep music" | `ambient_loop` |
| "compilation" / "supercut" / "best of" | `compilation_supercut` |
| "vlog" / "real footage" / "I record myself" | `talking_head_real_footage` |

### Medium signals (reduce ambiguity, may still need 1 question)

- "education" → `stickman_explainer` if topic is conceptual, `avatar_authority_longform` if topic is YMYL (medical/legal/financial)
- "money / investing" → defaults to `avatar_authority_longform` for retirees, `stickman_explainer` for general audience
- "history" → defaults to `documentary_voiceover` (long-form mystery-documentary style) unless user mentions "animated" then `stickman_explainer`
- "AI generated" → not a niche; describes production method. Ignore for archetype routing.
- "channel about [topic]" with no format hint → ask the 3-option disambiguation question

### What NOT to ask

- **Audience level** unless the description literally has zero hints AND the archetype requires it. For a personal-finance authority channel, audience is locked to "informed / older retiree" by archetype convention. Don't re-ask.
- **Posting cadence.** Videos are produced on-demand. There is no schedule.
- **Demographic interview for the host** (gender / age / ethnicity / vibe). The user picks from real avatar samples in the avatar picker — that's a Pass-2 question for the avatar archetypes only, and it's a 3-headshot picker, not a checkbox interview.
- **Generic provider preferences** unless the archetype has multiple valid tiers AND the user benefits from choosing. Most archetypes lock to one tier; just default and let the user override later.
- **"What's your niche"** when the description literally says the niche.

---

## The 12 archetypes — when to use each

Brief overview of when each fits. Full pipeline detail in `pipeline-bank.md`.

### 1. `avatar_authority_longform`
Talking-head AI avatar covering YMYL topics (tax, medical, legal, financial) for older / professional audiences. 25-min long-form. The personal-finance authority channel archetype. Voice + avatar tightly coupled.
**Pipeline depth:** **DEEPEST in the bank** — most validated, most production examples.

### 2. `stickman_explainer`
2D illustrated / animated explainer video. Science-animation / TubeGen-killer territory. 5-15 min. Programmatic motion graphics via Remotion + character cutout assets via image gen. Voice-driven.
**Pipeline depth:** **DEEPEST in the bank** — biggest gap in the previous skill, most demand.

### 3. `vertical_shorts_hype`
Vertical 9:16 shorts, 60-90 sec, high-energy / chaotic / hype register. Roblox-scenario channel pattern.

### 4. `music_video_propaganda`
3-3:30 min drill / trap / orchestral music video with AI vocals + AI visuals. News-pegged. Brick-narrative storytelling channel pattern. **Critical: Suno generates the FULL track first; visuals are BPM-locked to it.**

### 5. `documentary_voiceover`
10-25 min long-form mystery-documentary video essay. Voiceover-led, B-roll-heavy, no host on camera. Image gen + i2v for visualizations.

### 6. `news_hijack_investigation`
Investigative-journalism style investigative documentary. 15-25 min. Named villain + documentary evidence + reveal. **Compliance domain: defamation gate critical.**

### 7. `cinematic_ai_documentary`
Premium atmospheric video essay — true crime, mystery, narrative. Cinematic register. Often longer (20+ min). Heavy Veo 3 / Kling 3.0 usage.

### 8. `picture_education`
Slideshow with Ken Burns motion + voiceover. Archival aesthetic (wildlife-encounter compilation pattern) or modern infographic. No i2v needed — image post-processing handles motion. Cheapest archetype to produce.

### 9. `gaming_animation`
Character vs character / gaming lore animation. Fandom-lore battle channel pattern. Image gen for character keyframes + i2v for action shots.

### 10. `ambient_loop`
1-3 hour ambient music + looping visual. Suno tracks + 20s video loop, looped programmatically. Highest watch-time-per-impression in the bank, lowest production cost.

### 11. `compilation_supercut`
Sourced clips + light narration glue. Compliance: copyright gate is dominant.

### 12. `talking_head_real_footage`
Real human host on camera with own footage. Minimal AI involvement (transcription + captions + maybe thumbnails). Vlog / lifestyle / fitness / cooking creators who already record.

---

## Phase D — Avatar + voice picker (avatar archetypes only)

Locked from previous skill, with three updates:

**Update 1 — HeyGen avatar quota.** HeyGen photo-avatar tier is 3 active avatars per account. Generate exactly 3 candidates. Don't try a 4th; it errors mid-flow with 401028.

**Update 2 — Voice and avatar are paired.** Generate the 3 avatars first, train each as a HeyGen photo-avatar, then generate ONE sample performance per avatar with the SAME script line AND SAME voice across all three. The sample shows the voice + face combo for fair A/B. Voice picker (D.2) re-performs the chosen avatar with 3 different voice candidates.

**Update 3 — Always include "Show me different options" with `allowOther: true`.** Don't lock the user into your three picks. If they ask for different aesthetics ("more 60+ professorial" / "less corporate"), regenerate with their feedback baked in.

**Update 4 — Lock HeyGen settings to CHANNEL.md.** After the user picks their avatar and voice, write the concrete IDs to CHANNEL.md `## Locked Provider Assets`:
- Avatar engine: ask the user if they have a preference (III / IV). Default to IV unless they specifically want III.
- character_type + avatar_id / talking_photo_id: from the chosen avatar
- ElevenLabs voice_id + voice_name: from the chosen voice
- talking_photo_style: omit by default (do NOT set "circle")

If the user has a connected HeyGen account (check Integration Status), also query `GET /v1/avatar.list` to discover their existing custom avatars. Surface these alongside the freshly-generated candidates — the user may prefer an avatar they already trained in HeyGen Studio.

The full picker code stays — see previous skill for the exact `ask_user` shape and the headshot generation loop.

---

## Phase G — Visual style + voice samples (non-avatar archetypes)

For non-avatar archetypes, run the visual style picker BEFORE locking the channel. Generate 3-4 sample frames in different stylistic registers based on the niche, surface as `imageUrl` options. Always include "Show me different options" with `allowOther: true`.

For voiceover archetypes (documentary, cinematic, picture-education), ALSO run a separate voice sample picker. 2-3 voice candidates from the ElevenLabs voices API (query via Bash: `curl -s "https://api.elevenlabs.io/v1/voices" -H "xi-api-key: $ELEVENLABS_API_KEY"`) speaking the intro line as `audioUrl` options.

For music-video archetype, voice is generated INSIDE Suno alongside the instrumental — there is no separate voice picker. Instead, the picker chooses from 4-6 Suno generations of the same lyrical sample to pick the best vocal performance + production combo.

---

## Thumbnail picker — locked shape

ONE batched `ask_user` with three questions, regardless of archetype.

```javascript
ask_user({
  questions: [
    {
      id: "thumbnail_style",
      prompt: "What should thumbnails look like for this channel? Pick the closest archetype — we'll refine the specifics per video.",
      type: "single_choice",
      allowOther: true,
      options: [
        { id: "bold_text_face", label: "Bold text + reaction face", description: "Viral-challenge / drama register. Big headline, strong facial expression, saturated colors." },
        { id: "minimal_clean", label: "Minimal / typography-led", description: "Essayist register. Restrained type, negative space, one striking image element.", recommended: true },
        { id: "cinematic_still", label: "Cinematic still + small caption", description: "Documentary register. Movie-poster framing, atmospheric image, light overlay text." },
        { id: "illustration", label: "Custom illustration / graphic", description: "Explainer / animated register. Drawn or motion-graphics style, no photoreal face." }
      ]
    },
    {
      id: "thumbnail_recurring",
      prompt: "Anything that should appear on EVERY thumbnail? Recurring tagline, logo, host face, color palette, frame border. Skip if you want full per-video freedom.",
      type: "text",
      optional: true
    },
    {
      id: "thumbnail_workflow",
      prompt: "How should the agent pick the thumbnail for each video?",
      type: "single_choice",
      allowOther: true,
      options: [
        { id: "user_picks", label: "Generate 3-4 candidates, you pick", description: "Slowest, you stay in the loop on every cover.", recommended: true },
        { id: "agent_picks", label: "Generate candidates, agent picks the best", description: "Agent surfaces the chosen one for confirmation. Fast." },
        { id: "single_shot", label: "Generate one and ship it", description: "Cheapest, no selection step." }
      ]
    }
  ]
})
```

The `thumbnail_workflow` answer goes into FLOW.md verbatim — different value, different production behavior. Don't paraphrase.

For the recommended default style by archetype:
- `avatar_authority_longform` → `bold_text_face` (rage stamp register)
- `stickman_explainer` → `illustration`
- `vertical_shorts_hype` → `bold_text_face`
- `music_video_propaganda` → `cinematic_still` or `illustration`
- `documentary_voiceover` → `minimal_clean` or `cinematic_still`
- `news_hijack_investigation` → `bold_text_face`
- `cinematic_ai_documentary` → `cinematic_still`
- `picture_education` → `cinematic_still`
- `gaming_animation` → `illustration` or `bold_text_face`
- `ambient_loop` → `cinematic_still`
- `compilation_supercut` → `bold_text_face`
- `talking_head_real_footage` → `bold_text_face` or `cinematic_still`

When the user enters discovery and you've already locked the archetype, set `recommended: true` on the matching thumbnail style option. Defaults that match the archetype save user attention.

---

## AI tool tier integration — strong defaults, optional override

RookCast v1 has locked tiers across image / video / voice / music / avatar. Don't ask the user "premium or open source?" upfront unless the archetype legitimately benefits from the choice. Default to the niche's natural tier:

### Default tier by archetype

| Archetype | Image gen | Video gen | Voice | Music | Avatar |
|---|---|---|---|---|---|
| `avatar_authority_longform` | Flux Pro 1.1 (mid) | n/a | Minimax Speech-02 HD (premium) | none | **HeyGen (premium) — locked** |
| `stickman_explainer` | Flux Pro 1.1 (mid) | n/a | Minimax Speech-02 HD (premium) | Suno (single tier) | n/a |
| `vertical_shorts_hype` | Nano Banana Pro (premium) | Kling 3.0 (premium) for hero, Wan 2.2 for B-roll | Minimax Speech-02 HD (premium custom voice) | none | n/a |
| `music_video_propaganda` | Nano Banana Pro (premium) for keyframes | **Wan 2.2 (open source) — volume play locked** | n/a (Suno does vocals) | **Suno v5.5 — locked** | n/a |
| `documentary_voiceover` | Flux Pro 1.1 (mid) | Kling Turbo 2.6 (mid) | Minimax Speech-02 HD (premium) | Suno (single tier) | n/a |
| `news_hijack_investigation` | Flux Pro 1.1 (mid) | Kling Turbo 2.6 (mid) | Minimax Speech-02 HD (premium) | Suno (single tier) | optional Hedra |
| `cinematic_ai_documentary` | Nano Banana Pro (premium) | Kling 3.0 (premium) | Minimax Speech-02 HD (premium) | Suno (single tier) | n/a |
| `picture_education` | Nano Banana Pro (premium) for archival, Flux Pro for modern | n/a (Ken Burns motion in code) | Minimax Speech-02 HD (premium) | Suno (single tier) | n/a |
| `gaming_animation` | Nano Banana Pro (premium) | Kling Turbo 2.6 (mid) | Minimax Speech-02 HD (premium) | Suno (single tier) | n/a |
| `ambient_loop` | Flux Pro 1.1 (mid) | n/a | n/a | **Suno v5.5 — locked** | n/a |
| `compilation_supercut` | Flux Pro 1.1 (mid) | n/a | Minimax Speech-02 HD (premium) | Suno (single tier) | n/a |
| `talking_head_real_footage` | Flux Pro 1.1 (mid, for thumbnails) | n/a | n/a (real audio) | Suno (single tier, optional) | n/a |

When the archetype locks a tool (HeyGen for avatar, Suno for music, Wan 2.2 for music video volume), DON'T ask the user. Just default it and let them override in CHANNEL.md after the fact if they have a reason.

When the archetype has tier flexibility (image gen, video gen, voice), default to the column above and present the options as a single batched `ask_user` IF and ONLY IF the user explicitly asks about provider tiers OR the archetype has demonstrated quality variance at different tiers (e.g., cinematic_ai_documentary genuinely benefits from premium, stickman_explainer is fine with mid).

For most channels, the user shouldn't see tier questions at all. They should see a final proposal that says "this channel will use Flux Pro 1.1 + Minimax Speech-02 HD + HeyGen — change in CHANNEL.md if you want different."

---

## Credit cost estimation — surface in proposal

In the final `propose_channel` payload, include a `creditEstimatePerVideo` field. Pulled from the matching pipeline bank entry. Range covers the typical 25-min authority-channel episode at the locked tiers.

The user sees this in the proposal card. It's the single most-trust-building piece of information in the onboarding flow per RookCast's pricing transparency thesis. Show it before they confirm.

Per-archetype rough credit estimate (full detail in pipeline bank):

| Archetype | Credits / video |
|---|---|
| `avatar_authority_longform` | ~1,800-2,400 |
| `stickman_explainer` | ~1,200-1,800 |
| `vertical_shorts_hype` | ~400-600 |
| `music_video_propaganda` | ~500-800 |
| `documentary_voiceover` | ~2,000-3,000 |
| `news_hijack_investigation` | ~1,800-2,500 |
| `cinematic_ai_documentary` | ~3,000-5,000 |
| `picture_education` | ~600-1,000 |
| `gaming_animation` | ~1,500-2,200 |
| `ambient_loop` | ~150-300 |
| `compilation_supercut` | ~400-700 |
| `talking_head_real_footage` | ~150-300 (transcription + thumbnails only) |

(Credit denomination assumes the RookCast v1 token model: cheap ops at 2-3× markup, video gen at $0 markup.)

---

## CHANNEL.md schema — what to draft

Same shape as the previous skill, with one addition: include the locked archetype as a top-line metadata item so the production agent can route on it.

```
# <Channel Name>

> <one-line tagline>

## Archetype

`<one of 12 archetype ids>` — this drives FLOW.md routing and AI tool defaults.

## Audience

- Level: <general | informed | expert>
- Notes: <any nuance>

## Format

- Visual format: <derived from archetype>
- Length target: <derived from archetype>
- Aspect ratio: <16:9 | 9:16 | varies>

## Host (if archetype is avatar_authority_longform or talking_head_real_footage)

- Avatar asset id: <ch_asset_id>
- Voice asset id: <ch_asset_id>
- Avatar preview: <blob URL>
- Notes: <observed look + register from picked sample>

## Provider preferences

- Image: <locked tool from archetype default>
- Voice: <locked tool>
- Video: <locked tool, or n/a>
- Music: <Suno or none>
- Avatar: <HeyGen | Hedra | none>

## Thumbnails

- Style: <thumbnail_style id or user-typed Other>
- Recurring elements: <thumbnail_recurring text or "none">
- Selection workflow: <thumbnail_workflow id>

## Credit estimate

- Per video: <range from archetype bank>

## Notes

(grows over time as the production agent learns what works on this channel)
```

---

## FLOW.md — pre-drafted templates from the bank

Don't author FLOW.md from scratch. The pipeline bank has a pre-drafted template for each of the 12 archetypes. Pull that template, customize the channel-specific specifics (niche by name, host's voice asset id, locked thumbnail workflow), and propose.

The FLOW.md template includes:
- Step sequence (correct dependency order — voice before HeyGen, Suno before image gen on music video, etc.)
- Skill load callouts per step (`Load skill title-creation + bank.md`, etc.)
- Approval gates wired to user's chosen cadence (high_touch vs low_touch)
- Compliance preflight gate calibrated to niche (full preflight for YMYL, light for entertainment)

Don't add steps the archetype doesn't need. Don't blend archetypes. Pick one, specialize it.

---

## Mid-flow revision handling

Users will backtrack. Common patterns:

| User says | What to do |
|---|---|
| "Actually let's go with a male avatar instead" | Re-run Phase D.1 with constraint; don't reintroduce demographic interview |
| "Hold on, can we use Veo3 for video instead?" | Update in-memory provider preference; no need to re-fire Phase C |
| "I changed my mind, no avatar, just voiceover" | Switch archetype from `avatar_authority_longform` to `documentary_voiceover`; reload bank entry; rerun Phase G; redraft FLOW.md |
| "Make the script longer" | Update length target in CHANNEL.md draft; redraft affected FLOW.md steps; re-propose |
| "Switch from drill to lo-fi" | Music_video_propaganda → still archetype but Suno prompt + visual register shift |

Listen for "actually" / "wait" / "let me change" / "hold on" — re-route gracefully without insisting on linear path.

---

## Anti-patterns

The previous skill's anti-patterns plus three more from the data:

1. **Asking 8 phases of generic questions when the description already locked the archetype.** The user typed "tax channel for retirees" — you don't need to ask audience level, visual format, or niche.
2. **Treating all 12 archetypes as the same template.** They have different dependency graphs. HeyGen needs voice first; Suno music video needs music track first; explainer needs storyboard first. Get the order wrong and the production breaks.
3. **Asking the user to pick AI tool tiers when the archetype locks them.** Don't ask "premium or open source for music?" on a music video channel — Suno is the only valid option. Don't ask "which avatar provider?" on a personal-finance authority channel — HeyGen is the only one that does long-form reliably.
4. **Drafting a generic FLOW.md from a template instead of using the archetype's pre-drafted version from the bank.** The bank versions are already specialized — use them.
5. **Re-introducing demographic questions for avatars.** Pick from samples, never from checkboxes. (Inherited rule, still load-bearing.)
6. **Skipping the credit estimate in the proposal.** Pricing transparency is a brand thing. Show the estimate.
7. **Skipping FLOW.md.** Channel without FLOW.md can't produce videos. Both files always.
8. **Generating from scratch instead of pulling reference channel data when the user names a channel they want to clone.** If the user says "make me a personal-finance authority channel" or "long-form mystery-documentary style," use the reference profiles in `05-reference-channel-library.md` to skip 70% of the questions.

---

## When you're done

CHANNEL.md and FLOW.md are written. The channel auto-derives `channelPhase: "shaped"` on the next session boot. Production protocol takes over. Don't reload this skill on subsequent sessions.

The natural next move from the user is to ask for their first episode. When they do, follow the production protocol from the system prompt: `start_run` → TodoWrite from FLOW.md → walk the steps → present + complete_run.
