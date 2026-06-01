---
name: storyboard
description: >-
  Maps the script to a shot list with model selection per shot. Load when storyboarding before visual generation. Companion: shot-bank.md (beat-to-shot mapping for 5 structures + worked 56-shot storyboard + 8 channel style locks).
---

# Skill 06 — Storyboard / Scene Breakdown

This is the operational knowledge an AI YouTube agent needs to produce a perfect shot-by-shot storyboard for any visual-driven video, AND to know when to skip storyboarding entirely.

The single most important rule of this skill: **storyboard only when called upon.** Talking-head channels don't need storyboards. Documentary explainer channels don't need full storyboards. Music videos, what-if shorts, ambient channels, and visual-narrative pipelines absolutely need them.

The second most important rule: **the agent never starts generating images or video clips without first locking the style with the user.** Generating 20 keyframes that the user hates is a failure mode the agent prevents by sample-then-confirm at every commit gate.

---

## 1. The job of a storyboard

A storyboard does four things:

1. **Translates the script into visual shot intent** so downstream tools (image generators, image-to-video models, editors) can execute
2. **Locks visual continuity** across the video so the channel's visual brand stays coherent
3. **Surfaces decisions early** when they're cheap to change (text and reference images) rather than late when they're expensive (full renders)
4. **Provides the structural map** the editor uses for cut timing, music sync, and beat-locked pacing

A weak storyboard is a one-line "show a guy in a suit." A strong storyboard is a structured shot record: subject, action, camera framing, lighting, color grade, duration, transition, audio note. The agent always produces the strong version when storyboarding is called for.

## 2. When to storyboard, when to skip

The agent's first decision before any storyboard work: does this video actually need a storyboard?

### Skip storyboarding when:
- **Talking-head channel** with locked avatar/host (personal-finance authority, medical-authority style). The "shot" is the host. Cuts are just transitions between motion graphics.
- **Documentary with stock B-roll** that doesn't need shot-level direction (some logistics-explainer, tech-industrial-history).
- **Reaction / commentary / vlog** — content is captured live; no pre-visualization needed.
- **Long-form unscripted** (podcast, interview).
- **Ambient / loop** content with a single sustained scene.

### Storyboard when:
- **Music video / propaganda / drill** — every bar gets a visual decision (brick-narrative storytelling).
- **What-if shorts / Roblox-style** — every scene cascade needs visual planning (Roblox-scenario shorts).
- **News-hijack documentary** with B-roll heavy production (medical-authority, investigative-journalism).
- **History / explainer with visual reenactment** (long-form mystery-documentary, engineering-explainer, tech-industrial-history mid-section visualizations).
- **Ad / sponsor read with visual treatment**.
- **Channel trailer**.
- **Any video where the visuals are the product**, not a wrapper for narration.

### The decision flow

The agent runs this check at the start:
1. Read the script.
2. Determine the dominant visual mode: locked-host, narration + B-roll, scene-cascade, music-video, ambient.
3. If locked-host → no storyboard, skip to motion graphics planning.
4. If narration + B-roll → light storyboard (B-roll list only, no full shot record).
5. If scene-cascade or music-video → full storyboard.
6. If ambient → no storyboard, single scene description.

The agent surfaces this decision to the user before storyboarding: *"This script reads as a music-video format. I'd recommend a full shot-by-shot storyboard before we generate any visuals — that way you can approve the look before we spend on renders. Want me to produce the storyboard?"*

## 3. The 4 storyboard depths

When storyboarding is called for, four depths exist. **Default is medium.**

### Light (~5 minutes)
Just a B-roll list. One line per visual moment. Used for narration-heavy videos that need some visual variety but not scene-by-scene direction.

Format:
```
[00:30] B-roll — historical photograph of pre-1929 stock exchange
[01:15] B-roll — modern bank vault, shallow focus
[02:00] B-roll — close-up newspaper headline from 1933
```

### Medium (~15 minutes) — DEFAULT
A structured shot list with subject, action, framing, duration. No keyframe images yet — just the text plan.

Format per shot:
```
Shot 03 — 0:24 to 0:31 (7 seconds)
Subject: aerial drone shot of empty Manhattan skyline at dawn
Action: camera slowly pushes forward, slight downward tilt
Framing: wide establishing shot
Lighting: golden hour, low sun from frame-right
Mood: melancholy, pre-storm
Transition out: slow cross-fade to next
Audio note: ambient wind, no music yet
```

### Deep (~45 minutes)
Medium + reference image generation per scene. Each shot gets a visual reference (Nano Banana keyframe) the user reviews before any video generation begins.

### Production (~2-4 hours)
Deep + full visual style locking + sample i2v render of one scene + user approval gate before mass production. This is the safety pattern Nick called out — never spend on full production without user approval of the look.

## 4. The sample-then-confirm pattern (load-bearing rule)

This is the most important pattern in this skill. The agent never proceeds from storyboard to mass image/video generation without locking style with the user.

### The 4 commit gates

**Gate 1 — Style lock (BEFORE storyboarding)**
The agent confirms or proposes the visual style profile before writing a single shot. Pulls from the Channel Profile (Skill 05) if reference exists, otherwise asks: *"Before I storyboard — what's the visual feel you want? I can suggest based on your niche, or you can give me a reference channel."*

**Gate 2 — Storyboard text approval (BEFORE keyframe generation)**
Once the storyboard text is written, the agent surfaces it to the user. *"Here are 18 shots planned. Read through, tell me anything that feels off. Once locked, I'll generate keyframes."* No keyframes generated until user approves.

**Gate 3 — Sample keyframe approval (BEFORE all keyframes)**
Even after storyboard approval, before generating ALL keyframes, the agent generates ONE sample keyframe (typically Shot 1 or the most visually-defining shot). *"Here's the look I'd produce. This is the visual style. Approve to generate the remaining 17, or tell me what to change."*

**Gate 4 — Sample i2v approval (BEFORE all i2v renders)**
Once keyframes are approved, before generating all video clips, the agent renders ONE 5-second i2v sample of the most motion-defining shot. *"Here's how the motion will feel. Approve to render the rest."* This is the most expensive gate to skip — i2v at $0.084-0.500/sec adds up fast.

If any gate produces a no, the agent regenerates and re-surfaces. The agent never proceeds without explicit approval at each gate. Cost protection + quality protection.

## 5. The Storyboard Profile artifact

The structured output of a storyboard task. Lives in the project workspace, gets consumed by image generation, i2v generation, editor.

```
Storyboard — [video title]
Generated: [timestamp]
Depth: [light / medium / deep / production]
Style locked: [yes / no — references the Channel Profile]
Total shots: [N]
Total duration: [target seconds]

—— SECTION 1: STYLE LOCK ——
Visual style profile pulled from Channel Profile or established for this video:
- Color grade: [warm sepia / cool cinematic / clinical / saturated]
- Lighting register: [soft / hard / cinematic / available]
- Composition rules: [rule of thirds / center / split / experimental]
- Aspect ratio: [16:9 / 9:16 / 1:1]
- Reference channel match: [if applicable]
- Reference style images: [URLs or generated examples]

—— SECTION 2: SHOT LIST ——
For each shot:
- Shot number
- Timestamp range (start to end)
- Duration (seconds)
- Subject (what's in frame)
- Action (what moves)
- Camera (framing + movement)
- Lighting note
- Mood note
- Color grade override (if different from style lock)
- Transition out
- Audio note (music sync, SFX, ambient)
- Source: keyframe reference image URL once generated

—— SECTION 3: BEAT-LOCK NOTES (music videos only) ——
- BPM
- Beat-aligned cut timestamps
- Hook return points
- Drop / transition moments

—— SECTION 4: B-ROLL LIST (if applicable) ——
Lighter version of shots — just visual concepts paired to script timestamps

—— SECTION 5: TRANSITIONS PLAN ——
Across all shots — what types of transitions, in what frequency. Cuts vs fades vs wipes vs match cuts.

—— SECTION 6: APPROVAL LOG ——
Gate 1 (Style lock): [user approved at timestamp]
Gate 2 (Storyboard text): [user approved at timestamp]
Gate 3 (Sample keyframe): [user approved at timestamp]
Gate 4 (Sample i2v): [pending / approved]
```

## 6. Niche-specific storyboard approaches

### Music videos / drill / propaganda (brick-narrative storytelling style)

- **Beat-locked timing.** Every shot aligned to BPM. For 136 BPM songs, beat lands every 441ms. Shots typically span 4-8 beats (1.7-3.5 seconds).
- **Hook return = visual return.** Every chorus/hook reuses or echoes the same visual treatment — that's how the track builds visual identity.
- **Verse 1 vs Verse 2 differentiation.** Verse 1 introduces visual world. Verse 2 escalates — same world, more action, faster cuts.
- **Bridge = visual reset.** A different look or location for the bridge breaks pattern, then returns to chorus visual.
- **Outro = single sustained image** or slow zoom out.
- **Total shot count** for a 3-minute drill song: typically 40-70 shots.

### What-if shorts / Roblox-style verticals

- **Aspect ratio 9:16** mandatory.
- **Cascading consequence visualization.** Each line of dialogue gets one visual decision. Roblox/game-style stylization expected.
- **Hook visual at 0:00-0:03** (within the first beat — must hold attention before scroll).
- **Total shot count** for 60-90 sec short: typically 8-15 shots.

### News-hijack documentary (investigative-journalism, geopolitics documentary, medical-authority)

- **B-roll heavy, not full storyboard.** The host narration carries the structure; B-roll punctuates.
- **Document overlays** where evidence is presented. The agent records WHERE in the script document/screenshot evidence appears.
- **Cinematic interludes** at major reveals — full storyboard for those moments only.

### History / explainer with visualization (long-form mystery-documentary, tech-industrial-history visualization sections)

- **Mid-tier shot count** — script has narration spine, but key historical moments get full visualization.
- **Map / chart sequences** are scripted shot-by-shot.
- **Photo restoration scenes** get keyframes.

### Ambient / sleep / focus loop

- **Single sustained scene.** No real storyboard. Style lock only.
- **The "shot" is a visual concept** — "rainy library at night, candles, slow-tilt camera over 2 hours."

## 7. Style locking — the prerequisite step

Before any storyboarding work, the agent locks visual style. Three paths:

### Path A — Channel Profile already exists
User has shipped 3+ videos. Channel Profile (Skill 05) has Visual Style DNA. Agent loads it. Done.

### Path B — Reference channel
User says "make it look like a long-form mystery-documentary channel." Agent runs Skill 05 ingestion (medium-deep), pulls Visual Style DNA, applies anchor differentiation. Surfaces the proposed style to user. Locks on approval.

### Path C — Niche-only inference
User has no Channel Profile yet, no reference. Agent infers from niche conventions (e.g., music video → cinematic moody, what-if short → Roblox-stylized, news-hijack → document-overlay heavy). Surfaces 2-3 style proposals to user with sample reference images. User picks.

The agent never storyboards from a vague style. *"Cinematic" is not a style. "Warm sepia, single key light, rule-of-thirds composition, slow cuts" is a style.*

## 8. Beat extraction — script to scene mapping

For scripts with continuous narration (vs music videos), the agent maps script beats to storyboard shots:

**Step 1 — Read the full script.**
**Step 2 — Identify natural beat boundaries:**
- Topic shifts
- Sentence-of-the-paragraph that introduces a new idea
- Rhetorical pauses
- "And then..." / "What happened next..." transitions
**Step 3 — Estimate timestamp for each beat** (using ~150 wpm narration speed).
**Step 4 — Allocate one shot per beat by default.** Exceptions:
- Long beats (>15 sec) get 2-3 shots
- Punchy beats (<3 sec) may share a shot with the surrounding beat
**Step 5 — Surface the beat-shot map to user before generating keyframes.**

For music video scripts (lyrics):
- Each bar = 4 beats
- Default 1 shot per 4-8 bars (4-second to 8-second shot length at 136 BPM)
- Hook lines get reusable shots
- Drop moments get distinct visuals

## 9. Shot type vocabulary

The agent uses a controlled vocabulary so downstream tools (image gen, i2v models) understand intent.

### Camera framing
- Extreme close-up (XCU) — eyes only, single object detail
- Close-up (CU) — face only, hand and small prop
- Medium close-up (MCU) — head and shoulders
- Medium shot (MS) — waist up
- Wide shot (WS) — full body, room
- Extreme wide shot (EWS) — landscape, scale
- Insert — object detail (document, prop)
- Aerial / drone — overhead
- POV — first person
- Over-the-shoulder (OTS) — character interaction

### Camera movement
- Static — no movement
- Pan — horizontal rotation
- Tilt — vertical rotation
- Push in / pull out — toward/away from subject
- Dolly — physical movement
- Tracking — following subject
- Crane — vertical
- Whip pan — fast pan, often as transition
- Arc — circular movement around subject

### Lighting registers
- High-key — bright, even, low contrast
- Low-key — dark, dramatic, high contrast
- Three-point — standard
- Available / natural — no rigging
- Practical — visible light sources in frame
- Silhouette — backlit subject
- Underlit — light from below (creepy register)
- Color-keyed — single dominant color (red room, blue night)

### Color grade descriptors
- Warm sepia / cool cinematic / clinical bright / saturated pop / muted documentary / desaturated grit / film noir / neon

### Transitions
- Hard cut — instant
- Match cut — visual continuity across cut
- Cross-fade — overlap
- Whip pan — fast pan blends two shots
- Wipe — directional reveal
- Smash cut — fast cut to dramatically different scene
- Dissolve — slow blend

The agent uses these terms when describing shots so the i2v model interprets correctly.

## 10. Duration math

For music videos:
- 4 beats = 1 bar
- 1 bar duration (sec) = 60 / BPM × 4
- At 136 BPM: 1 bar = 1.76 sec
- Standard shot length: 4-8 bars (7-14 sec) at 136 BPM
- Very fast cuts: 1-2 bars (1.7-3.5 sec)
- Sustained sections: 8-16 bars (14-28 sec)

For narration:
- Standard B-roll cut: 3-5 seconds (matches average sentence)
- Cinematic establishing shots: 7-10 seconds
- Fast B-roll punctuation: 1-2 seconds
- Avoid shots under 1 second (read as "fast cut" stylistically — only use deliberately)

For i2v constraints:
- Most i2v models cap at 5-10 seconds per generation
- Shots longer than 10 sec require either two consecutive i2v generations stitched, OR a sustained-camera-move single render, OR alternating to a different shot type at the 10-sec mark

The agent factors model constraints into duration decisions.

## 11. Anti-patterns

**A1 — Storyboarding talking-head content.** Wastes time. The "shot" is the host. Skip storyboard.

**A2 — Skipping style lock.** Generating keyframes before user approves the visual style. Result: user hates the look, agent burns budget regenerating.

**A3 — Generating all keyframes before sample approval.** Even with style locked, individual keyframe quality varies. Always sample first.

**A4 — Vague shot descriptions.** "Show a guy in a suit" is not a shot. The agent always specifies framing + lighting + mood + duration.

**A5 — Beat misalignment in music videos.** Shots that don't land on beats produce visual-audio dissonance. The agent always math-checks against BPM.

**A6 — Forgetting the hook return.** Music video chorus/hook should reuse or echo verse 1's visual register so brand identity reinforces. Don't generate fresh visuals every chorus.

**A7 — Over-cutting.** More than ~30 cuts/min in non-music content reads frenetic. Less than 6 cuts/min reads dull. The agent targets niche conventions.

**A8 — Style drift mid-storyboard.** Shot 1 is sepia; Shot 14 is neon-cyberpunk. Style lock applies to ALL shots unless a deliberate visual shift is part of the structure (bridge in music video).

**A9 — Ignoring i2v duration limits.** Storyboarding 20-second sustained shots when the model caps at 10 sec. Plan within constraints.

**A10 — Skipping the approval log.** If gates pass without explicit user approval, the agent has no record and can't roll back cleanly.

## 12. Worked examples

### Example 1 — Brick-narrative storytelling music video, "You're Not Jesus"

- **Style lock (Gate 1):** Reference channel = past brick-narrative storytelling episodes. Visual Style DNA: cinematic moody, saturated graded, brick-character world, mid-pace cuts. User approves.
- **BPM:** 136. Total track length 3:30 = 210 sec. Bar duration 1.76 sec. Total bars: 119.
- **Storyboard depth:** Production
- **Total shots:** 52 (avg shot length ~4 seconds, varies)
- **Beat structure:**
  - Hook (intro, 8 bars, 14 sec): 4 shots
  - Verse 1 (16 bars, 28 sec): 7 shots
  - Chorus (8 bars, 14 sec): 4 shots — REUSES hook visual register
  - Verse 2 (16 bars, 28 sec): 7 shots — escalates from V1
  - Chorus (8 bars, 14 sec): 4 shots — same hook register
  - Bridge (8 bars, 14 sec): 4 shots — visual RESET, distinct register
  - Chorus (8 bars, 14 sec): 4 shots — return to hook register
  - Outro (8 bars, 14 sec): 1 sustained zoom-out shot
- **Shot list snippet (Shot 1):**
  ```
  Shot 01 — 0:00 to 0:03.5 (3.5 sec, 2 bars)
  Subject: brick-toy crowd in town square, mid-density, neutral expressions
  Action: slow camera push-in toward central figure
  Framing: WS establishing
  Lighting: cinematic, golden hour key from frame-right
  Mood: ominous calm before storm
  Color grade: warm saturated cinematic
  Transition out: hard cut on downbeat
  Audio note: hook line drops on cut
  ```
- **Sample keyframe (Gate 3):** Generated for Shot 1 via Nano Banana. User approves the look.
- **Sample i2v (Gate 4):** Generated 5-sec sample of Shot 1 via LTX-2.3. User approves motion. Mass production proceeds.

### Example 2 — Roblox-scenario short, "What If A Teacher Owned Roblox?"

- **Style lock:** Reference = past Roblox-scenario shorts. Visual Style DNA: 9:16 vertical, Roblox-stylized, saturated, fast cuts, mid-pace.
- **Total duration:** 75 seconds
- **Storyboard depth:** Medium
- **Total shots:** 11
- **Shot list snippet:**
  ```
  Shot 01 — 0:00 to 0:05 (5 sec)
  Subject: stern adult teacher in Roblox-stylized classroom
  Action: zoom on teacher's face as scene transitions to Roblox HQ logo
  Framing: MCU
  Mood: deadpan-comedic
  Audio: "BRO! Imagine if a teacher OWNED Roblox..." narration begins
  ```
- **Gates 1-2:** User approves style + shot list.
- **Gate 3:** Sample keyframe Shot 1. User approves.
- **Gate 4:** Sample i2v 5-sec for Shot 1. User approves. Production proceeds.

### Example 3 — News-hijack documentary B-roll list (light depth)

- **Decision:** Light storyboard only — narration drives, B-roll punctuates.
- **No Gate 3 / Gate 4 sample required because keyframes are stock-footage-driven, not generative.**
- **B-roll list:**
  ```
  [00:30] B-roll — Chase Bank exterior, modern day, daylight
  [01:15] B-roll — generic phone showing incoming call screen
  [02:00] B-roll — close-up of bank document being filled out
  [03:45] B-roll — historical footage of 1970 bank security legislation signing
  [05:20] Document overlay — FinCEN Form 112 with key fields highlighted
  ```

## 13. Runtime checklist

- [ ] Decided whether storyboard is needed (decision flow §2)
- [ ] Style locked at Gate 1 with user approval
- [ ] Shot list written at chosen depth
- [ ] Beat math validated (music videos: BPM × bar length × shot count = total duration)
- [ ] Shot list approved at Gate 2 with user explicit OK
- [ ] Sample keyframe generated, approved at Gate 3
- [ ] Sample i2v generated, approved at Gate 4 (production depth only)
- [ ] All shots fit within i2v model duration limits
- [ ] Transitions plan locked
- [ ] Approval log timestamped at every gate

If any gate fails, regenerate. Never bulk-generate without sequential approval.

---

## Update log

This skill is current as of April 2026. Update when:
- New i2v models change duration limits or style capabilities
- Music video / propaganda niche conventions evolve
- New depth tier emerges (e.g., real-time AI live storyboarding)
