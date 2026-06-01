---
name: image-to-video
description: >-
  Generates video clips from keyframes via i2v models (Kling / Hailuo / Veo / Sora / LTX-2.3). Load when prompting an i2v model for a shot. Companion: cookbook.md (10 shot-type templates + per-model fingerprint bank + motion verb bank).
---

# Skill 07 — Image-to-Video Prompting

> **⚠ Model reference (2026-05) — cost/strength notes, NOT a closed list.**
>
> The table below is a starting reference of i2v models known to work well for these jobs, with their per-second costs. It is **not the full set of what you can use** — fal hosts more i2v models, and the Vercel AI Gateway can route others. There is no single blessed default: pick the model that best fits the shot. If the user names a model that isn't in this table (e.g. a newer release), or you think something else suits the shot better, **discover what's actually available and use it** — list fal's video models, query the gateway's `/v1/models`, or WebSearch + WebFetch the provider docs. **Never tell the user a model is unavailable just because it's absent from this table — verify first.**
>
> | Strengths | Model id | Cost / use notes |
> |---|---|---|
> | B-roll, atmospheric, dialogue close-ups | `fal-ai/kling-video/v2.6-turbo/image-to-video` | ~$0.11/sec. Reliable general workhorse. |
> | Dramatic camera moves, action, faces in motion | `fal-ai/kling-video/v3.0/pro/image-to-video` | ~$0.40/sec. 10s max. |
> | High volume, budget (OS, Apache 2.0) | `fal-ai/wan/v2.2-a14b/image-to-video` | ~$0.02/sec. |
> | Cheap fast commercial | `fal-ai/minimax/hailuo-2.3/fast/image-to-video` | Hailuo 2.3 Fast. ~$0.03/sec. |
> | Cheapest | `fal-ai/lightricks/ltx-video-v2.3/image-to-video` | ~$0.01/sec. Already in the brick-narrative storytelling pipeline. |
> | Audio gen / long-form coherence | `fal-ai/veo3/image-to-video` (audio), `fal-ai/sora-2/image-to-video` (coherence) | Premium edge cases. |
>
> **How to choose:** match the model to the shot — general B-roll, hero/dramatic, high-volume/cheap, or premium audio/coherence — weighing fidelity against per-second cost. Pick per shot (not per project), and consider models beyond this table when they fit better. Once chosen, lock to one or two models per project for visual consistency.
>
> Prices and model positioning drift — treat the per-second costs as approximate and re-check current rates/availability when it matters. Detailed reasoning in Section 2 below is preserved for context but its specific model lists/prices are stale.

---

This is the operational knowledge an AI YouTube agent needs to generate the perfect motion clip from a still image and a prompt. Image-to-video (i2v) is the most expensive single operation in the agent's pipeline — typically $0.084 to $0.500 per second of generated video. Getting it right on the first or second try is the difference between profitable production and burning the user's wallet.

The agent's behavior on i2v is governed by one rule above all others: **never run a full render without first running a 5-second sample and getting user approval on the look and motion.** This is the cost-protection gate that makes the platform's $0-markup video pricing economically sustainable for users.

---

## 1. The job of i2v

Image-to-video does three things:

1. **Animates a still image** with motion that fits the script and the storyboard's intent
2. **Bridges keyframes into watchable footage** for music video, what-if shorts, news-hijack B-roll, history-channel reenactments, ambient loops
3. **Stays within the visual style locked at Gate 1** — every clip must feel like the same channel

A weak i2v prompt produces stiff motion, melted faces, six fingers, surreal failures. A strong i2v prompt produces clean, contained motion that reads as cinematography rather than AI artifact.

The agent's job is to write strong prompts on the first attempt by understanding what each model is actually good at and matching the prompt structure to the model.

## 2. Model selection — match the model to the shot

Different i2v models have radically different strengths, costs, and failure modes. The agent picks the model per shot, not per project.

### Kling 2.0 Standard
**Strengths:** Cinematic camera moves, realistic lighting, dependable on faces, strong in 5-10 second range
**Weaknesses:** Can be slow to render, sometimes over-smooths motion
**Cost:** ~$0.084/sec via fal.ai
**Best for:** Cinematic establishing shots, dramatic interludes, news-hijack documentary visuals
**Default model when:** the user hasn't specified and we need balanced quality/cost

### Kling 2.1 Pro
**Strengths:** Higher fidelity faces, better motion coherence, strong on action sequences
**Weaknesses:** ~70% more expensive than Standard
**Cost:** ~$0.140/sec
**Best for:** Hero shots, hook moments, anything with prominent faces

### Kling 2.1 Master
**Strengths:** Highest quality in the Kling family, premium cinematic
**Cost:** ~$0.280/sec
**Best for:** Channel trailer, Year-in-Review, any shot the user is willing to pay for

### Hailuo 02
**Strengths:** Cheapest premium-quality option, surprisingly strong on stylized content
**Weaknesses:** Less consistent on photoreal humans, slightly grainier output
**Cost:** ~$0.020/sec
**Best for:** B-roll, atmospheric shots, music video filler shots, ambient
**Use when:** budget-conscious or stylized aesthetic favors slight grain (e.g., brick-narrative storytelling)

### Veo 3 Fast / Veo 3
**Strengths:** Strong on physical realism, excellent at camera moves, good on text legibility within frame
**Weaknesses:** Tighter aspect ratio constraints, slower generation
**Cost:** ~$0.180/sec (Fast), ~$0.300/sec (full)
**Best for:** When realism is the brand register (documentary, science visualization)

### Sora 2 Pro
**Strengths:** Highest perceived realism, best on complex scenes, longer durations
**Weaknesses:** Most expensive, queue times can be long, content policy more restrictive
**Cost:** ~$0.500/sec
**Best for:** Premium cinematic moments, channel trailer, hero shots where audience expectation is "this looks like a movie"

### LTX-2.3 (self-hosted RunPod)
**Strengths:** Pass-through cost — only pay GPU time, ~$0.004/clip; high volume capable; strong on stylized content
**Weaknesses:** Requires self-hosting; lower fidelity than commercial models on photoreal humans
**Cost:** ~$0.004/sec (compute only)
**Best for:** Music videos at scale (brick-narrative storytelling style), high-volume B-roll, anyone running 50+ clips per video

### Selection decision tree
1. Is the shot a stylized music video / propaganda visual? → LTX-2.3 (cheap, stylized matches)
2. Is it a hero shot or face-centric and quality matters? → Kling 2.1 Pro
3. Is it a budget B-roll filler? → Hailuo 02
4. Is realism the brand and budget allows? → Veo 3 or Sora 2 Pro
5. Default for everything else → Kling 2.0 Standard

The agent surfaces model selection to the user when running the Sample Gate (§5). User can override.

## 3. Prompt structure

Every i2v prompt has the same skeleton, in this order:

```
[SUBJECT] + [ACTION] + [CAMERA] + [LIGHTING] + [MOOD/STYLE] + [TECHNICAL]
```

### Subject (1-2 phrases)
What's in the frame. Specific over general. Use concrete nouns.
- ✗ "a man"
- ✓ "a 70-year-old grandmother in a beige cardigan, reading glasses on a chain"

### Action (1-2 phrases)
What moves. Verbs are the load-bearing words for i2v models.
- ✗ "doing something"
- ✓ "slowly looks up from a letter, eyes widening, mouth tightening"

### Camera (1 phrase, technical)
Use the controlled vocabulary from Skill 06 §9.
- "static medium close-up"
- "slow push-in over 5 seconds"
- "handheld tracking shot following subject from left to right"

### Lighting (1 phrase)
Use the controlled register from Skill 06.
- "warm golden hour from frame-right, soft fill from above"
- "low-key cinematic, single key light, dramatic falloff"

### Mood / style (1 phrase)
The emotional register of the shot.
- "ominous calm before storm"
- "intimate, melancholy, lived-in"
- "frenetic urban energy"

### Technical (1-2 phrases)
Aspect, duration, motion intensity.
- "16:9, 5 seconds, moderate camera motion, no cuts"
- "9:16 vertical, 5 seconds, fast cuts internal to clip"

### Worked example

For a personal-finance authority IRS hook shot:
```
A 70-year-old grandmother in a beige cardigan and reading glasses sits at a kitchen table reading a letter from the Social Security Administration, her face slowly shifting from confusion to alarm; static medium close-up at eye level; warm golden-hour light from frame-right window, soft fill, slight shadow under eyes; mood is intimate concern with hint of dread; 16:9, 5 seconds, minimal camera motion, no cuts.
```

This prompt produces a clip that fits the personal-finance authority brand on the first try ~85% of the time. Vague prompts produce clips that fit ~30% of the time.

## 4. Motion language — the verbs that work

i2v models are extremely sensitive to verb choice. Some verbs produce clean motion. Others produce melted nightmare fuel.

### Verbs that work
- **Slowly + [movement verb]** — "slowly turns," "slowly walks," "slowly leans forward"
- **Subtle + [adjective change]** — "subtle expression shift from neutral to concern"
- **Begins + [movement]** — "begins to raise hand"
- **Looks at / looks toward / looks away** — eye movement is reliable
- **Lifts / lowers / tilts** — small body movements
- **Push in / pull out / pan / tilt** — camera moves, well understood

### Verbs that fail
- **Runs, jumps, throws, fights** — most i2v models struggle with fast multi-limb action
- **Speaks, talks, speaks dialogue** — lip sync is unreliable; avoid generating talking shots; use static lip-closed shots and overlay audio
- **Disappears, vanishes, transforms** — surreal transitions confuse models
- **Multiplies, splits, replicates** — impossible motion produces artifacts
- **Dance** (without specific style) — generic dance fails; "ballet pirouette" works better

### The motion intensity dial
- **Static** — subject barely moves; camera barely moves. Most reliable. Most cinematic for emotional shots.
- **Subtle** — small expression shifts, slow camera. Reliable.
- **Moderate** — full body movement, walking, turning. Mixed reliability.
- **High** — running, fighting, rapid action. Use sparingly. Plan to re-render multiple times.
- **Extreme** — flying, transforming, surreal. Most unpredictable. Often better with stylized models (LTX, Hailuo) than photoreal (Kling, Sora).

The agent always biases toward LOWER motion intensity than the storyboard might initially imply. Static is the friend. Cinema is mostly stillness.

## 5. The Sample Gate (cost protection rule)

This is the load-bearing safety rule. The agent NEVER generates a full-cost render without first running a 5-second sample.

### The sample workflow

1. **User approves storyboard** (Skill 06 Gate 2)
2. **User approves keyframe** (Skill 06 Gate 3)
3. **Agent prepares i2v prompt** for the chosen model
4. **Agent generates 5-second sample** at production resolution but capped at 5 seconds
5. **Agent surfaces sample to user** with the prompt visible: *"Here's the 5-sec sample. Cost so far: $0.42. Want me to render the full 8-second shot at $0.67? Or revise the prompt?"*
6. **User approves OR revises**
7. **Agent renders full-length** ONLY after explicit approval

### What the sample tests
- Does the motion match storyboard intent?
- Does the visual style match Channel Profile?
- Are there obvious AI artifacts (six fingers, melted faces, warped text)?
- Does the camera move feel cinematic or jerky?

### When to skip the sample (rare)
Only when:
- Shot is very short (< 3 seconds)
- Style and motion are identical to a previously-approved shot in the same project
- User has explicitly enabled "no sample" mode for this batch (e.g., trusted bulk B-roll generation)

The agent never decides to skip the sample on its own. User must explicitly authorize.

### Sample cost rolls into full render
For most models, the sample's cost is included in the full render (the sample becomes the first 5 seconds of the longer clip). The agent tells the user this so the cost framing is clear.

## 6. Reference image use

Every i2v generation starts with a reference image (the keyframe). The reference is at least as important as the prompt — often more so.

### Reference image rules
- **Match the aspect ratio** of the target output. Don't feed a 16:9 keyframe and request 9:16 output.
- **Resolve > 1024×576** for cinematic content; 1080×1920 for vertical.
- **Single dominant subject** in the frame — multi-subject reference images confuse motion intent.
- **Avoid text overlays** in the reference. The model will try to animate text and fail. Add text in post.
- **Match the lighting/grade** to your style lock. Don't feed a daylight reference and prompt for "low-key cinematic."

### Reference + prompt alignment
The model treats the prompt as instructions for what to ANIMATE in the reference. If the reference shows a subject already in motion, the model gets confused. Best practice:
- Reference image shows the subject at the START of the motion
- Prompt describes what happens NEXT

### Worked example
- Reference: still image of a man standing still in a doorway, calm expression
- Prompt: "the man slowly takes one step forward, expression shifts from calm to alert"
- Result: clean motion from start to end of the prompt

vs

- Reference: image of a man already running through a doorway
- Prompt: "the man runs through the doorway"
- Result: incoherent motion because the reference is already showing the destination

## 7. Seed locking

Every i2v generation uses a seed (numerical value that controls randomness). Seed locking is the agent's tool for consistency.

### When to lock the seed
- **Same character across multiple shots** — lock seed so the face stays consistent
- **Re-rendering after revision** — lock seed to test only the prompt change, not the random variation
- **Bulk generating same scene with subtle variation** — lock seed, vary only the prompt or motion

### When to vary the seed
- **First sample for a new shot** — let the model surprise you with default behavior
- **When a locked seed is producing artifacts** — try 3-4 seeds and pick the best

### The agent's seed protocol
- First sample: random seed (let the model show its strength)
- If approved: lock that seed for full render
- If revised: try 2-3 different seeds before changing the prompt (random variation can be the cheap fix)

## 8. Aspect ratio rules

| Aspect | Use case | Models that handle natively |
|---|---|---|
| 16:9 (1920×1080) | YouTube long-form | All |
| 9:16 (1080×1920) | Vertical Shorts, Roblox-scenario | All but check Veo (some constraints) |
| 1:1 (1080×1080) | Instagram crossposting | Most via cropping; native in Hailuo |
| 4:3 (1440×1080) | Retro / archival aesthetic | Kling, Sora |
| 21:9 (cinematic ultra-wide) | Premium hero shots | Kling Pro/Master, Sora |

The agent matches output aspect to the channel's primary platform. For YouTube long-form: always 16:9. For Roblox-style shorts: 9:16. For multi-platform creators: generate 16:9 and crop in post.

## 9. Anti-patterns

### Visual anti-patterns
- **Six-finger / extra-limb errors** — pre-Jan 2026 i2v models still occasionally produce. Sample Gate catches.
- **Melted face artifacts** — happens on Kling Standard with fast motion. Solution: lower motion intensity OR upgrade to Kling Pro.
- **Warped text** — text in the reference image animates poorly. Solution: never put text in the reference; overlay in post.
- **Background drift** — backgrounds change unexpectedly between frames. Solution: lock seed, simpler backgrounds, or shorter clips.
- **Limb morphing** — appears in extreme action. Solution: lower motion intensity or accept artifact and recut around it.

### Prompt anti-patterns
- **Generic prompts** — "person walking" produces unpredictable motion. Always specify.
- **Conflicting instructions** — "static shot, fast camera movement" — confusing. Pick one.
- **Too many subjects** — "5 people doing different things in a park" — model can't render all coherently.
- **Pop culture references** — "in the style of Spielberg" — sometimes works, often produces parody.
- **Brand names** — "in a Walmart" — content policy issues. Use generic descriptors.

### Workflow anti-patterns
- **Skipping the Sample Gate** — biggest budget burner.
- **Mass-rendering before testing one** — even with style approved, individual shot quality varies.
- **Mixing models within a project** — visual consistency drops. Lock to one or two models per project.
- **Re-prompting endlessly without changing seed** — if the prompt is right and the model can't deliver, try a different seed before rewriting the prompt.

## 10. Niche-specific i2v approaches

### Music videos / drill / propaganda (brick-narrative storytelling channel)
- **Default model:** LTX-2.3 (cost-effective for 50+ clips per video)
- **Aspect:** 16:9 or 9:16 depending on platform
- **Motion intensity:** Moderate to high — music video pacing rewards visible motion
- **Seed strategy:** Lock seed for chorus return shots, vary for verse shots
- **Sample Gate:** Mandatory on first verse shot, can skip on subsequent if style is consistent

### What-if shorts (Roblox-scenario)
- **Default model:** LTX-2.3 or Hailuo (stylized aesthetic matches)
- **Aspect:** 9:16 vertical
- **Motion intensity:** Mid-high — vertical feed rewards visible motion
- **Duration:** 3-8 seconds per shot

### News-hijack documentary (investigative-journalism / medical-authority)
- **Default model:** Kling 2.0 or Veo 3 Fast (realism matters)
- **Aspect:** 16:9
- **Motion intensity:** Subtle — documentary pacing rewards stillness
- **Use case:** Cinematic interludes between host segments

### History / explainer visualization (long-form mystery-documentary / tech-industrial-history)
- **Default model:** Kling 2.1 Pro (higher fidelity for archival aesthetic)
- **Aspect:** 16:9
- **Motion intensity:** Static to subtle
- **Reference images:** Often archival photo restorations or AI-generated period scenes

### Ambient / sleep / focus
- **Default model:** Hailuo or Kling Standard (sustained slow motion)
- **Duration:** Often single 10-second clip looped, not multi-clip storyboard
- **Motion intensity:** Static to subtle (rain, candles, gentle ambient)

### Talking-head channels (personal-finance authority / medical-authority)
- **Mostly skip i2v entirely.** Avatar carries the visual; motion graphics overlay.
- **Use i2v only for B-roll inserts** (e.g., a kitchen table grandmother shot in an example episode)
- **Default model:** Kling 2.0 Standard for B-roll

## 11. Worked examples

### Example 1 — Personal-finance authority IRS B-roll insert

**Storyboard shot:** "Grandmother reading SSA letter at kitchen table"

**Selected model:** Kling 2.0 Standard ($0.084/sec)

**Reference image:** Generated via Nano Banana — 70-year-old woman in beige cardigan at kitchen table, holding a letter, neutral expression

**Prompt:**
```
A 70-year-old grandmother in a beige cardigan and reading glasses on a chain sits at a wooden kitchen table holding an opened letter from the Social Security Administration; she slowly looks up from the letter, expression shifting from confusion to mild alarm, eyes widening; static medium close-up at eye level, slight push-in over 5 seconds; warm golden-hour light from frame-right window, soft fill from above, slight shadow under eyes; mood is intimate concern with hint of dread; 16:9, 5 seconds, minimal camera motion, no cuts, photorealistic.
```

**Sample render:** $0.42 for 5-sec sample. User approves.
**Full render:** $0.67 for 8-sec total clip.
**Total cost:** $0.67 (sample rolled in).

### Example 2 — Brick-narrative storytelling music video, Verse 1 Shot 5

**Storyboard shot:** "Brick-toy crowd in town square, mid-density, slowly turning toward camera as if recognizing speaker"

**Selected model:** LTX-2.3 self-hosted (~$0.04 for 8-sec clip)

**Reference image:** Generated via Nano Banana — brick-toy town square, ~30 brick-toy figures with neutral expressions, mid-density

**Prompt:**
```
A brick-toy town square crowd of approximately 30 figures slowly turns their heads toward the camera, as if just hearing something significant; static wide shot, frame center; cinematic warm-graded lighting with hint of stylized Saturday-morning-cartoon coloration; mood is communal recognition; 16:9, 8 seconds, only head-turning motion, camera static, brick-blocky stylization preserved.
```

**Sample render:** $0.02 (LTX is cheap). User approves.
**Full render:** $0.04 total. Mass production cleared for remaining 41 shots.

### Example 3 — Documentary explainer, archival visualization

**Storyboard shot:** "Roman engineer adjusting concrete formula in a 79 AD lab"

**Selected model:** Kling 2.1 Pro ($0.140/sec) — fidelity matters for archival aesthetic

**Reference image:** Generated via Nano Banana — historically-accurate Roman engineer in toga, holding clay tablet near a stone basin

**Prompt:**
```
A Roman engineer in toga and short tunic stands beside a stone basin filled with wet concrete, holding a clay tablet; he carefully tilts the tablet to add a small handful of dark volcanic ash to the basin, watching it fall; static medium shot at slight low angle; warm sepia-graded torchlight from frame-left, atmospheric dust particles visible in beam; mood is craftsman concentration; 16:9, 6 seconds, minimal full-body motion, archival cinematic warmth, photorealistic but graded for period.
```

**Sample render:** $0.84 for 6-sec sample. User approves.
**Full render:** $0.84 (sample is full duration).

## 12. Runtime checklist

Before any i2v generation:

- [ ] Storyboard shot exists with subject, action, camera, lighting, mood, duration
- [ ] Style locked at Skill 06 Gate 1 (or earlier)
- [ ] Keyframe approved at Skill 06 Gate 3
- [ ] Model selected per shot (not per project)
- [ ] Reference image meets aspect/resolution/composition requirements
- [ ] Prompt follows the SUBJECT + ACTION + CAMERA + LIGHTING + MOOD + TECHNICAL skeleton
- [ ] Motion intensity biased low when shot allows
- [ ] Aspect ratio matches output platform
- [ ] Sample Gate run with explicit user approval before full render
- [ ] Cost surfaced to user pre-render
- [ ] If repeating shot type from earlier in project, seed locked

If any check fails, regenerate or surface to user. Never bulk-render without sample approval.

---

## Update log

This skill is current as of April 2026. Update when:
- New i2v models change cost-quality positioning (Kling 3, Sora 3, etc.)
- LTX self-hosted improves to match commercial fidelity
- Aspect ratio support changes across models
- Anti-pattern catalog grows as new failure modes emerge
