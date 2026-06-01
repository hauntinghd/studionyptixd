# Channel Creation Pipeline Bank — 12 production archetypes

This is the data file that the channel-creation protocol routes against. Each archetype gets a full specification: dependency graph, AI tool stack, step sequence, channel-specific questions, credit estimate, common failure modes, and a pre-drafted FLOW.md template.

Two archetypes get the deepest treatment — `avatar_authority_longform` (personal-finance authority channel canonical) and `stickman_explainer` (science-animation / TubeGen-killer territory). They are the highest-leverage channels in the bank and deserve full depth. The other 10 get standard treatment — enough to onboard the channel correctly without the user having to invent the pipeline.

How to use this file: the protocol identifies the archetype, loads the matching section, runs the channel-specific questions, drafts CHANNEL.md + FLOW.md from the template, and proposes. Don't author from scratch.

---

# Index

| # | Archetype | Reference channel type | Length | Depth |
|---|---|---|---|---|
| 1 | `avatar_authority_longform` | a personal-finance authority channel (IRS / tax) | 25 min | DEEP |
| 2 | `stickman_explainer` | a science-animation channel / TubeGen-killer | 5-15 min | DEEP |
| 3 | `vertical_shorts_hype` | a Roblox-scenario channel | 60-90 sec | Standard |
| 4 | `music_video_propaganda` | a brick-narrative storytelling channel | 3-3:30 min | Standard |
| 5 | `documentary_voiceover` | a long-form mystery-documentary channel | 10-25 min | Standard |
| 6 | `news_hijack_investigation` | an investigative-journalism channel | 15-25 min | Standard |
| 7 | `cinematic_ai_documentary` | atmospheric true crime | 20-40 min | Standard |
| 8 | `picture_education` | a wildlife-encounter compilation channel | 10-30 min | Standard |
| 9 | `gaming_animation` | a fandom-lore battle channel | 5-15 min | Standard |
| 10 | `ambient_loop` | an ambient-soundscape channel | 1-3 hr | Standard |
| 11 | `compilation_supercut` | reaction / best-of channels | 5-15 min | Standard |
| 12 | `talking_head_real_footage` | vlog / lifestyle / fitness | varies | Standard |

---

# 1. `avatar_authority_longform` — DEEP

The personal-finance authority channel archetype. Talking-head AI avatar covering YMYL topics (Your Money, Your Life — tax / medical / legal / financial / regulatory) for older or professional audiences. 25-min long-form. The most validated archetype in the bank — the lead reference channel alone has 38+ shipped episodes following this pipeline.

## When this archetype fits

**Trigger phrases in description:**
- "tax channel for retirees" / "IRS expert" / "financial advisor"
- "medical / health for seniors" / "doctor explains" / "medical-authority style"
- "legal / attorney / law explainer"
- "personal-finance authority style" / "credentialed expert"

**Niche markers:**
- Audience: older (50+) or professional
- Topic: YMYL (regulatory, financial, medical, legal)
- Authority claim: explicit (federal credentialed expert, registered nurse, attorney)
- Length intent: 20-30 minutes
- Compliance domain: high (defamation + medical misinformation + financial advice gates)

**Reference channel types:**
- A personal-finance authority channel (IRS / tax) — V2 Federal Credentialed Expert voice
- A medical-authority channel (Senior Health) — V2 medical variant voice
- A finance-personality channel (hypothetical)
- A doctor-personality health channel

## Dependency graph

The avatar pipeline has the strictest dependency chain in the bank. Get the order wrong and HeyGen rejects the input.

```
1. Topic + outlier mining
   ↓
2. Title + thumbnail concept (hook lock)
   ↓
3. Script (V2 voice, 12-beat, Minimax-format numbers-as-words, ~25,000 chars)
   ↓
4. Voice generation (ElevenLabs Bill OR Minimax) — produces .wav
   ↓
5. HeyGen avatar performance (consumes .wav + locked avatar asset)
   ↓ produces lip-synced 1080p MP4
6. Preprocess (Whisper transcription + silence detection on rendered audio)
   ↓ produces captions.json + segments.json
7. Find-timings (locate phrase anchors in captions for motion graphics)
   ↓ produces ep-timings.json
8. Motion graphics generation (Remotion TSX with BigStat / EmphasisTextV2 / StaggerList / SplitCompare components)
   ↓ produces ep-motion-graphics.mp4 overlays
9. Final assembly (ffmpeg overlay graphics on avatar video, render captions)
   ↓ produces ep-full.mp4
10. ffprobe verification + Skill 13 pre-flight
    ↓
11. Upload as draft (or via YouTube API if channel OAuth wired)
```

**Critical: voice MUST exist before HeyGen runs.** HeyGen Avatar IV has voice cloning built in but our pipeline uses externally-generated voice for tighter control over pronunciation (especially for form numbers, dollar amounts, and Whisper mishears that need pre-correction). Don't shortcut this.

**Critical: motion graphics depend on Whisper transcription of the RENDERED audio**, not the script. Whisper mishears are real (e.g., the host's name getting transliterated, "SSA" → "essay", "Form 12153" → "Form 1215 3"). Find-timings runs against the actual transcript, not the script. The mishear dictionary corrects systematic errors before render.

## AI tool stack (locked at v1 tiers)

| Component | Tool | Tier | Why locked |
|---|---|---|---|
| Image gen (thumbnails + B-roll keyframes) | Flux Pro 1.1 | Mid | Best photoreal humans for the rage-stamp / patient-composite thumbnails this niche uses |
| Voice generation | Minimax Speech-02 HD | Premium | Best emotion + pronunciation control for long-form narration. ElevenLabs Bill is the long-running fallback if Minimax has issues with specific terms. |
| Avatar | **HeyGen** | **Premium — locked** | Only avatar tool that handles 25-min long-form reliably. Hedra Character-3 quality degrades past 10 min. Hallo3 (open source) is acceptable for B-roll cutaways but not the main host. |
| Video gen (i2v) | n/a | — | Not used. The avatar IS the video; B-roll is photo cutaways. |
| Music | Suno v5.5 (light bed only) | Single tier | Optional — most authority channels skip music or use very light bed. |

**Don't ask the user to pick avatar provider.** HeyGen is locked. They can override in CHANNEL.md after the fact if they want to test Hedra, but the default path uses HeyGen.

## Channel-specific questions (the minimum viable set)

Run these in batched `ask_user` calls. Three batches total — avatar pick, voice pick, channel meta.

### Q1 — Avatar pick (Phase D.1)

Generate 3 candidate headshots based on the channel description + niche. Vary register / age / look across the three. Train each as a HeyGen photo-avatar. Generate 1 sample performance per avatar with the SAME script line and SAME voice. Surface as videoUrl options with `allowOther: true` for "Show me different options."

### Q2 — Voice pick (Phase D.2)

After avatar locked, regenerate the SAME avatar with 3 different voice candidates. From the ElevenLabs voices API (`curl -s "https://api.elevenlabs.io/v1/voices" -H "xi-api-key: $ELEVENLABS_API_KEY"`), pick voices that span the channel's plausible register range (warm + measured, energetic, authoritative). Surface as videoUrl options with the same allowOther escape.

### Q3 — Channel meta (one batched call)

```javascript
ask_user({
  questions: [
    {
      id: "host_name",
      prompt: "What's your host called? This is what they'll be addressed as in the script's credential intro.",
      type: "text",
      optional: false
    },
    {
      id: "credential",
      prompt: "What's their professional credential? This goes in the credential intro line of every script.",
      type: "text",
      optional: false,
      examples: ["federal tax accountant", "registered nurse", "elder-law attorney", "geriatric cardiologist"]
    },
    {
      id: "lead_magnet_url",
      prompt: "Channel's lead magnet URL. Goes in every video's CTA. Skip if you don't have one yet.",
      type: "text",
      optional: true,
      examples: ["yourchannel.kit.com/checklist"]
    },
    {
      id: "affiliate_url",
      prompt: "Primary affiliate (one product, not a stack). Skip if you don't have one yet.",
      type: "text",
      optional: true,
      examples: ["a tax-software affiliate with promo code"]
    }
  ]
})
```

That's it. Don't ask audience level (locked to "informed senior" by archetype). Don't ask visual format (locked to avatar). Don't ask length (locked to 25 min). Don't ask provider tiers (all locked above).

## Credit estimate per video

For a typical 25-min authority-channel episode:

| Stage | Tool | Quantity | Credits |
|---|---|---|---|
| Outlier mining | Claude API | 1 ideation pass | ~50 |
| Title generation | Claude API | 6 candidates | ~30 |
| Thumbnail keyframe | Flux Pro 1.1 | 3-4 candidates | ~150-200 |
| Script writing | Claude API | 25,000 chars | ~200 |
| Voice generation | Minimax Speech-02 HD | 25,000 chars | ~600-800 |
| Avatar performance | HeyGen Avatar IV | 25 minutes @ 6 cr/min | ~150 cr × 6 = **~900** |
| Motion graphics keyframes | Flux Pro 1.1 | 50-60 cutaway B-rolls | ~200-300 |
| Captions + assembly | Compute (no AI cost) | — | ~50 |
| Pre-flight fact-check | Claude API | research-heavy | ~150 |
| **Total** | | | **~1,800-2,400 credits** |

(Credit denomination assumes ~$0.001 per credit at Starter tier, so this video costs the user roughly $1.80-2.40 of their token budget. RookCast absorbs the HeyGen + Minimax costs into the cheap-op markup.)

## Common failure modes (and how the pipeline handles them)

1. **Whisper mishears form numbers** → mishear dictionary in CHANNEL.md auto-corrects before render. Build dictionary over first 3-5 episodes.
2. **HeyGen avatar drift mid-scene** (eye drift, hand stiffness on long renders) → split 25-min script into 3-4 segments, generate avatar performance per segment, concat in ffmpeg. Lower drift risk per segment.
3. **Motion graphics land off the spoken phrase by 200-500ms** → anchor-based caption alignment (find-timings runs phrase searches, not 1:1 sequential alignment).
4. **Pre-render preprocess file lock race** → 45-second buffer between preprocess complete and rename. Bake into FLOW.md.
5. **Composite character drift** (a recurring patient's age varying across episodes) → recurring composite registry in `projects/[channel]/recurring_composites.md`. Reference at every Beat 4.

## FLOW.md template (pre-drafted — channel-customizable)

```markdown
# <Channel Name> — production flow

## Steps

1. **Pick the topic.** Pull outliers from `<niche>` via Skill 12. User provides the topic OR agent surfaces top 3 ranked candidates.
   - High-touch: ask_user for topic confirmation
   - Low-touch: agent picks the highest-ranked candidate

2. **Lock title + working thumbnail concept.** Load Skill 01 + 01-companion (title-creation-bank.md). Generate 6 ranked candidates using the channel's locked formula rotation (typically F2 dollar + F11 parenthetical OR F6 threat alert). Hook-first — title constrains script's promise.
   - High-touch: ask_user with 6 candidates
   - Low-touch: agent picks top-ranked

3. **Write the script.** Load Skill 03 + beat-anatomy.md. V2 voice DNA from CHANNEL.md. 12-beat structure, ~25,000 chars, Minimax format (numbers as words, no symbols). Composite patient from recurring_composites.md for Beat 4. Channel checklist + affiliate pitch in Beats 9-10. No years-of-experience claim.
   - High-touch: ask_user for script approval (often iterative)
   - Low-touch: lock script and continue

4. **Generate voiceover.** Load Skill 08 + voice-config-bank.md. Use channel's locked voice asset (Minimax Speech-02 HD with channel's chosen voice id). Sample-Gate test with first 30 seconds before full generation. Apply mishear dictionary corrections to script before submission.

5. **HeyGen avatar performance.** Use the HeyGen video generation API via Bash (`curl -s -X POST "https://api.heygen.com/v2/video/generate" -H "X-Api-Key: $HEYGEN_API_KEY" -H "Content-Type: application/json" -d '{"video_inputs": [{"character": {"type": "avatar", "avatar_id": "<HEYGEN_AVATAR_ID>"}, "voice": {"type": "audio", "audio_url": "<VO_BLOB_URL>"}}]}'`). For 25-min scripts, split into 3-4 segments, perform each, concat with ffmpeg. Verify lip sync before continuing.

6. **Preprocess.** Run `npm run preprocess <avatar_render.mp4>` — silence detection + Whisper transcription + audio extraction. Wait 45 seconds after detection of preprocess completion before any subsequent file operation (file lock buffer).

7. **Find-timings.** Run channel-specific find-ep{N}-timings.js with phrase anchors. Verify ≥80% hit rate. If lower, inspect captions for actual Whisper output, update phrase patterns, re-run.

8. **Generate motion graphics.** Build Ep{N}Video.tsx using Remotion components (BigStat, EmphasisTextV2, StaggerList, SplitCompare, SectionHeader). Each motion graphic anchored to a timing key. Register in Root.tsx with composition id `Ep{N}-{slug}`.

9. **Final render + verify.** `npx remotion render src/index.ts Ep{N}-{slug} out/ep{N}-full.mp4`. ffprobe to verify (1920×1080, H.264, AAC, no NAL errors).

10. **Pre-flight compliance check.** Load Skill 13. Full preflight (Domain 4 financial / Domain 3 medical / Domain 5 defamation gates as applicable). Sources archived in research/ep{N}/sources.md.

11. **Description + tags.** Load Skill 04 + bank.md. Pull chapters from ep-timings.json key beats. Lead magnet + affiliate pitch + composite labeling disclaimer.

12. **Upload as draft.** Manual via YouTube Studio (or via API if channel OAuth wired). User confirms before public.
   - Always-on: yes (final ship gate, regardless of cadence)
```

For low-touch cadence, collapse steps 1-2-3 (topic → title → script auto-flow without ask_user pauses) and step 11 (description auto-generated). Final video at step 11/12 always surfaces for approval.

---

# 2. `stickman_explainer` — DEEP

The science-animation / TubeGen-killer archetype. 2D illustrated explainer video — animated character cutouts, infographic motion, voice-driven scene transitions. 5-15 min. The biggest pipeline gap in the previous skill — most channels asking for "explainer" content fall here, and the pipeline is fundamentally different from avatar long-form.

## When this archetype fits

**Trigger phrases:**
- "explainer videos about X" (when X is conceptual, not YMYL)
- "science-animation style" / "news-explainer / educational-animation style"
- "stickman animation" / "whiteboard explainer"
- "infographic motion" / "animated education"
- "TubeGen-style" (without the inauthentic-content baggage)

**Niche markers:**
- Audience: general curious public (broad)
- Topic: conceptual, scientific, historical, technological — NOT YMYL
- No on-camera host required
- Length intent: 5-15 minutes (sweet spot for explainer retention)
- Compliance domain: light (educational content, low litigation risk)

**Reference channel types:**
- A science-animation channel (canonical premium quality)
- A news-explainer outlet / educational-animation style — explainer with motion graphics
- Animated education
- A science-explainer channel short-form (some episodes)
- A logistics explainer channel (mixed — sometimes documentary-voiceover, sometimes explainer)

## Dependency graph

The explainer pipeline is fundamentally voice-and-scene-driven, NOT character-driven. The script writes the structure; voice + storyboard run in parallel; programmatic motion ties everything together at assembly time.

```
1. Topic + concept lock
   ↓
2. Script (information-dense, ~5,000-12,000 chars depending on length)
   ↓
3. Storyboard derived from script (one shot per ~20-30 seconds of voice)
   ↓ produces shot list with scene descriptions
4a. Voice generation (parallel with 4b)            4b. Character cutout asset library (parallel with 4a)
   ↓ produces .wav                                       ↓ produces ~10-30 reusable PNG/SVG assets
5. Background scene generation (per shot)
   ↓ produces ~15-30 background images at 1920×1080
6. Programmatic motion graphics (Remotion)
   ↓ React components animate cutouts across backgrounds, time-locked to voice
7. Captions baked into Remotion render
   ↓
8. Music bed + sound effects (Suno + SFX library)
   ↓
9. Final ffmpeg render + verify
   ↓
10. Pre-flight (light — educational content)
    ↓
11. Upload as draft
```

**Critical: motion is programmatic, NOT i2v.** Most channels coming in say "make an animated explainer" thinking we'll generate animated video clips. We don't — that path produces inconsistent results. We use Remotion (React-based programmatic motion graphics) to animate static character cutouts across static backgrounds, time-locked to the voice waveform. This is the same approach top science-animation channels use internally (theirs is After Effects; ours is Remotion), and it's why their videos look consistent across hundreds of episodes.

**Critical: character assets are reusable across episodes.** The first episode of a stickman channel generates ~10-30 character variants (the "narrator stickman," angry boss stickman, scientist stickman, etc.) plus reusable props (lightbulb, brain, money symbol). Subsequent episodes pull from this asset library and only generate NEW assets when the script needs a character or prop the library doesn't have. This drives episode 1 cost up (~3-4× later episodes) and subsequent episodes way down.

**Critical: backgrounds are scene-specific, characters are reusable.** Backgrounds get generated per shot. Characters cut into them via Remotion. Don't generate a single integrated illustration per shot — that breaks reusability and ties you to per-episode generation cost.

## AI tool stack (locked at v1 tiers)

| Component | Tool | Tier | Why locked |
|---|---|---|---|
| Character cutout assets | Flux Pro 1.1 (with stylized prompts + transparent background instruction) | Mid | Best at clean illustration with consistent character identity. Premium tier (Nano Banana Pro) overkill for cutouts. |
| Background scenes | Flux Pro 1.1 (illustration mode) | Mid | Same as character. Illustration-style prompts ("flat 2D, vector illustration, science-animation aesthetic, minimal palette"). |
| Voice generation | Minimax Speech-02 HD | Premium | Voice carries the entire video. Worth premium tier. F5-TTS is the cost-conscious alternative if user's volume is high. |
| Video gen (i2v) | n/a | — | **NOT USED.** Programmatic motion via Remotion replaces i2v entirely. |
| Music + SFX | Suno v5.5 | Single tier | Music bed essential. SFX from library (or Suno SFX mode for custom). |
| Avatar | n/a | — | No host. |

**Don't ask the user about i2v providers.** Stickman explainer doesn't use them. If they ask about Kling / Wan / etc., explain that this archetype uses programmatic motion instead.

## Channel-specific questions (the minimum viable set)

Three batches.

### Q1 — Character base style

Generate 3 candidate "narrator character" assets in different illustration registers. Vary the aesthetic across the three: minimalist stickman vs flat-character science-animation style vs hand-drawn whiteboard register. Surface as imageUrl options with allowOther.

```javascript
// Generate the candidates BEFORE the ask_user
const styles = [
  { id: "minimal_stickman", prompt: "A simple black-and-white stickman character, minimal lines, flat 2D, transparent background, full body" },
  { id: "flat_science_animation", prompt: "A flat 2D vector illustration of a friendly character, science-animation aesthetic, soft palette (teal/orange/cream), transparent background, full body" },
  { id: "whiteboard_sketch", prompt: "A hand-drawn whiteboard-style sketch of a character, marker line aesthetic, transparent background, full body" }
];
// Generate one image per style, surface to user.
ask_user({
  questions: [{
    id: "character_style",
    prompt: "Pick the illustration style for your channel's characters. This locks the visual register for every episode.",
    type: "single_choice",
    options: [
      { id: "minimal_stickman", label: "Minimal stickman", imageUrl: "<v1>", description: "Cleanest, fastest, most explainer-y" },
      { id: "flat_science_animation", label: "Flat-character science-animation style", imageUrl: "<v2>", description: "More premium feel, recommended for science / philosophy", recommended: true },
      { id: "whiteboard_sketch", label: "Whiteboard sketch", imageUrl: "<v3>", description: "Hand-drawn aesthetic, friendly + casual" },
      { id: "regenerate", label: "Show me different options", description: "Tell me what you'd rather see." }
    ],
    allowOther: true
  }]
})
```

### Q2 — Voice pick

Standard voice picker — 3 candidates from the ElevenLabs voices API, speaking the intro line of a sample script. Surface as audioUrl options.

### Q3 — Channel meta + music register

```javascript
ask_user({
  questions: [
    {
      id: "music_register",
      prompt: "Music bed style?",
      type: "single_choice",
      options: [
        { id: "uplifting_synth", label: "Uplifting synth", description: "Modern, optimistic, science-explainer feel", recommended: true },
        { id: "thoughtful_piano", label: "Thoughtful piano", description: "Reflective, philosophical, news-essay feel" },
        { id: "playful_orchestral", label: "Playful orchestral", description: "Whimsical, kid-friendly, educational-animation feel" },
        { id: "minimal_ambient", label: "Minimal ambient", description: "Restrained, lets voice carry, logistics-explainer feel" }
      ]
    },
    {
      id: "length_target",
      prompt: "Typical episode length?",
      type: "single_choice",
      options: [
        { id: "short_5_8", label: "5-8 minutes", description: "Higher CTR, fastest production", recommended: true },
        { id: "medium_8_12", label: "8-12 minutes", description: "Standard explainer length" },
        { id: "long_12_15", label: "12-15 minutes", description: "Deep-dive feel, slower production" }
      ]
    },
    {
      id: "color_palette",
      prompt: "Locked color palette? Skip if you want per-episode freedom.",
      type: "text",
      optional: true,
      examples: ["teal/orange/cream (science-animation classic)", "navy/yellow/red", "pastel pink/blue/yellow"]
    }
  ]
})
```

That's it. Three batched calls. No demographic questions, no audience level (locked to "general curious"), no provider tier questions (all locked above).

## Credit estimate per video

For a typical 8-minute stickman explainer episode (length: medium):

### First episode (asset library bootstrap)

| Stage | Tool | Quantity | Credits |
|---|---|---|---|
| Topic / concept ideation | Claude API | 1 pass | ~50 |
| Script writing | Claude API | ~8,000 chars | ~150 |
| Storyboard generation | Claude API | ~20 shots | ~80 |
| Character asset library (one-time) | Flux Pro 1.1 | 15 character variants + 10 props | **~600-800 (one-time)** |
| Background scenes | Flux Pro 1.1 | ~20 backgrounds | ~250-350 |
| Voice generation | Minimax Speech-02 HD | ~8,000 chars | ~250-300 |
| Music bed | Suno v5.5 | 1 track (4 min, looped) | ~30 |
| SFX library | Suno SFX mode | ~8 SFX | ~50 |
| Programmatic motion (Remotion render) | Compute only | — | ~100 |
| **First episode total** | | | **~1,500-1,900 credits** |

### Subsequent episodes (asset library reused)

| Stage | Tool | Quantity | Credits |
|---|---|---|---|
| Concept + script + storyboard | Claude API | — | ~280 |
| **NEW characters/props only** | Flux Pro 1.1 | 2-5 new assets | ~80-200 |
| Background scenes | Flux Pro 1.1 | ~20 backgrounds | ~250-350 |
| Voice + music + SFX + render | Same as above | — | ~430-450 |
| **Subsequent episode total** | | | **~1,000-1,300 credits** |

The cost cascade is real — episode 1 is ~30% more expensive because of the asset library bootstrap. By episode 5-6, the library is mature and per-episode cost stabilizes at the lower end.

## Common failure modes

1. **User asks for "i2v animation" thinking that's how it works.** Explain the programmatic-motion approach upfront. The proposal card should make this clear: "characters animate via code, not video gen."
2. **Character drift across episodes** (the narrator stickman looks different in episode 5 than episode 1). Lock the asset library after episode 1 — never regenerate from scratch. Add to the library, don't replace.
3. **Background-character style mismatch** (cartoon character on photoreal background). Both must use the same illustration register. Bake into the channel-locked style segment per Skill 09 §4.
4. **Motion timing drifts off voice** at the 60% mark of long episodes. Use Whisper-anchored timestamps to align motion to actual voice waveform, not script estimate.
5. **First episode runs over-budget on the asset library.** Surface this in the proposal: "Episode 1 costs ~30% more than subsequent episodes because you're building the asset library." Manage user expectation.

## FLOW.md template

```markdown
# <Channel Name> — production flow (stickman explainer)

## Steps

1. **Pick the topic / concept.** User provides OR agent suggests from outlier mining.

2. **Write the script.** Load Skill 03 with stickman-explainer voice variant (V12 Curious Kid Adult OR V8 Erudite Professor — depends on channel register). Length target from CHANNEL.md (5-8 / 8-12 / 12-15 min). Write to scenes — one ~20-30 sec block per shot.
   - High-touch: ask_user for script approval
   - Low-touch: continue

3. **Storyboard.** Load Skill 06 + shot-bank.md. Derive shot list from script — one shot per ~20-30 sec block. Each shot: scene description + characters needed + props needed.

4. **Generate voice.** Load Skill 08. Channel's locked voice from CHANNEL.md. Sample Gate before full generation.

5. **Asset library check.** Diff storyboard's required characters/props against `assets/library/` from prior episodes. Generate ONLY missing assets via Flux Pro 1.1 with channel's locked illustration style. Add new assets to library.

6. **Generate background scenes.** Per shot. Flux Pro 1.1 illustration mode + channel-locked color palette + scene description from storyboard.

7. **Build Remotion composition.** Programmatic motion graphics with cutouts on backgrounds, time-locked to voice. Captions baked in. Music bed underneath at -34 LUFS, ducked under voice.

8. **Render + verify.** ffmpeg final render. ffprobe verify.

9. **Pre-flight (light).** Skill 13 §6 — educational content, light preflight. Verify cited claims have source on file.

10. **Title + thumbnail (after visuals locked).** Load Skill 01 + Skill 02. Illustration-style thumbnail using channel's locked color palette + characters from library.

11. **Description + tags.** Load Skill 04. Chapter timestamps from voice waveform.

12. **Upload as draft.**
```

For low-touch cadence: collapse steps 1-2-10-11 (auto-flow without ask_user pauses on topic, script, title, description).

---

# 3. `vertical_shorts_hype` — Standard

The Roblox-scenario channel pattern. Vertical 9:16 shorts, 60-90 sec, high-energy chaotic register.

## When this fits
"Roblox shorts" / "TikTok-style what if" / "POV vertical" / "gaming shorts" / "kid-energy hype shorts"

## Reference channel types
- A Roblox-scenario channel (canonical)
- A Roblox build/play channel (reference for hook + structure)
- Various TikTok-native gaming hype channels

## Dependency graph
```
1. Concept (chaotic premise)
2. Script (40-45 sec, 8-12 sentence-scenes, BRO-style hook)
3. Voice (Minimax custom voice prompt — "chaotic Gen Z shitposter") — manual or API
4. Audio split into per-scene segments (sentence boundaries → 3-5 sec scenes)
5. Image gen per scene (Nano Banana Pro with Roblox-stylized prompts)
6. i2v per scene (Kling Turbo 2.6 for hero, Wan 2.2 for B-roll)
7. ffmpeg vertical assembly (1080×1920, 30fps, captions baked, music)
8. Pre-flight (light)
```

## AI tool stack
- Image: Nano Banana Pro (premium)
- Voice: Minimax Speech-02 HD with custom voice prompt (premium)
- Video: Kling Turbo 2.6 (mid) for hero, Wan 2.2 (open source) for filler
- Music: optional Suno or library SFX

## Channel-specific questions
- Voice prompt confirmation (default: "chaotic Gen Z shitposter, fast pace, uses BRO and fr fr naturally" — let user override)
- Aspect ratio confirmation (default 9:16)
- Music: yes/no/per-episode

## Credit estimate per video
~400-600 credits (8-12 i2v clips at Kling Turbo + voice + image gen).

## Common failure modes
- Voice prompt produces sterile output → iterate the prompt with more chaotic specifics.
- Vertical aspect ratio not honored by some i2v models → Kling Turbo 2.6 supports 9:16 natively, Wan 2.2 needs prompt-level enforcement.
- Captions don't read at 240×135 mobile preview → use S1 word-pop caption system (Skill 10 §3) at 28-32% canvas height.

## FLOW.md template summary
Topic → script → manual voice OR Minimax API → audio split → image gen per scene → i2v per scene → ffmpeg vertical assembly → pre-flight (light) → upload draft.

---

# 4. `music_video_propaganda` — Standard

The brick-narrative storytelling channel pattern. 3-3:30 min news-pegged music video with AI vocals + AI visuals.

## When this fits
"Drill music video" / "AI satirical music videos" / "news-pegged music" / "brick-narrative style"

## Reference channel types
- A brick-narrative storytelling channel (canonical)
- Adjacent military-themed brick-narrative channels (early outliers in the format)
- Various AI music video channels

## Dependency graph (CRITICAL — different from every other archetype)
```
1. News beat detection (24-48hr window from event)
2. Lyrics drafted (Claude — drill register, beat-locked bars)
3. Suno generates 4-6 vocal track variants — pick best (full track: vocals + instrumental locked together)
4. BPM detection + scene breakdown (script → ~50 shots, one per bar at 136 BPM = ~441ms shots)
5. Keyframes per shot (Nano Banana Pro with channel-locked stylized aesthetic)
6. i2v per shot (Wan 2.2 self-hosted on RunPod for volume — 50 clips × 5 sec)
7. ffmpeg beat-locked concat (clips align to downbeats)
8. Captions (S4 block lyric system)
9. Pre-flight (compliance: inauthentic-content discipline + satire framing)
10. Upload draft
```

**Critical: Suno generates the music FIRST, then visuals fit the BPM.** Do not generate visuals before the track exists — the timing won't match. Different from every other archetype where visuals are generated to fit voiceover timestamps.

## AI tool stack
- Image: Nano Banana Pro (premium) for stylized keyframes
- Video: **Wan 2.2 self-hosted on RunPod (open source) — locked** for volume play
- Music: **Suno v5.5 — locked** (vocals + instrumental in one)
- Voice: n/a (Suno does vocals)
- Avatar: n/a

## Channel-specific questions
- Genre (drill / trap / orchestral / EDM / parody)
- BPM lock (default 136 for drill)
- Visual aesthetic (brick-narrative / claymation / photoreal stylized / anime)
- News-peg cadence: opportunistic vs scheduled

## Credit estimate per video
~500-800 credits — Suno track (~30) + 50 Nano Banana keyframes (~600) + Wan 2.2 self-hosted compute (~120) + assembly.

## Common failure modes
- Track generation produces 3:00 actual when 3:30 was targeted → Suno comes 15-20% short. Write lyrics for 4:30 to hit 4:00 actual.
- BPM-locked timing drifts on long videos → re-render specific bars rather than full concat.
- Inauthentic-content policy risk (Jan 2026 enforcement wave) → vary visual aesthetic per video, label as satire on About page, news-peg with verifiable basis.

## FLOW.md template summary
News beat → lyrics → Suno (4-6 variants, pick best) → BPM scene breakdown → keyframes → Wan 2.2 i2v → beat-locked ffmpeg → captions → pre-flight (with satire framing) → upload draft.

---

# 5. `documentary_voiceover` — Standard

A long-form mystery-documentary video essay. 10-25 min. Voiceover-led, B-roll-heavy, no host on camera.

## When this fits
"Video essay" / "long-form mystery-documentary style" / "deep-dive documentary" / "essayist content" / "logistics-explainer style"

## Reference channel types
- A long-form mystery-documentary channel (canonical premium)
- A logistics explainer channel
- A long-form niche-history channel
- A tech-industrial-history channel

## Dependency graph
```
1. Topic + research notes
2. Script (V1 Documentary Authority voice, ~12,000-25,000 chars)
3. Voice (ElevenLabs OR Minimax — long-form narration)
4. Storyboard derived from voice timestamps (B-roll-heavy, ~40-60 shots per 15-min episode)
5. Image gen per shot (Flux Pro 1.1, cinematic register)
6. i2v for shots needing motion (Kling Turbo 2.6, ~30% of shots)
7. Music bed (Suno cinematic ambient)
8. ffmpeg final assembly + captions
9. Pre-flight (defamation gate if real-events)
10. Upload draft
```

## AI tool stack
- Image: Flux Pro 1.1 (mid, cinematic register)
- Video: Kling Turbo 2.6 (mid)
- Voice: Minimax Speech-02 HD OR ElevenLabs Adam/Daniel for V1 register
- Music: Suno v5.5 (cinematic ambient bed)

## Channel-specific questions
- Length target (10 / 15 / 20 / 25 min)
- Music palette (cinematic ambient / orchestral / minimal)
- Visual register (warm cinematic / cool documentary / archival)

## Credit estimate per video
~2,000-3,000 credits — heavy on image gen (40-60 shots) + i2v (15-20 motion shots).

## Common failure modes
- Voice-to-storyboard timing mismatch → derive shot list from actual voice waveform timestamps, not script estimates.
- Image style drift across 40+ shots → lock channel style segment per Skill 09 §4 and append to every prompt.
- Long-form retention dies at 60% mark → Skill 03 retention valley fixes (Beat 5 mechanism reveal, Beat 6 three examples).

## FLOW.md template summary
Topic → research → script → voice → storyboard from VO timestamps → image gen → i2v motion shots → music bed → ffmpeg → captions → pre-flight (defamation gate critical) → upload draft.

---

# 6. `news_hijack_investigation` — Standard

An investigative-journalism channel pattern. 15-25 min. Named villain + documentary evidence + reveal.

## When this fits
"Investigative-journalism style" / "investigative" / "scam exposure" / "named villain" / "exposé"

## Reference channel types
- An investigative-journalism channel (canonical)
- A geopolitics documentary channel (similar register)
- A news-explainer outlet's investigations

## Dependency graph
Same backbone as `documentary_voiceover` BUT with one critical difference: **evidence package archived BEFORE script writing**.

```
1. Topic identified (named villain / specific incident)
2. Evidence package built (primary sources, court documents, named subject's response)
3. Script (V4 Skeptical Investigator voice, evidence-driven, ~15,000-25,000 chars)
4. [Same as documentary_voiceover from here]
```

## AI tool stack
Same as `documentary_voiceover` plus:
- Optional Hedra Character-3 for if user wants on-camera-ish presenter feel without HeyGen long-form

## Channel-specific questions
- Investigation style (forensic / first-person infiltration / pure document analysis)
- Voice register (skeptical investigator / outraged activist / deadpan cynic)
- Hedra Character-3 yes/no for occasional presenter cutaways

## Credit estimate per video
~1,800-2,500 credits — slightly less than pure doc because investigative content uses more documents (cheap to render via Remotion text overlays) and fewer i2v shots.

## Common failure modes
- Defamation risk on named subjects without source on file → mandatory evidence package check at Skill 13 §3 Domain 5.
- "Alleges" / "according to" framing missing on accusations → automatic insertion at script audit phase.
- Subject's response not acknowledged → must be addressed even if "[subject] did not respond to requests for comment."

## FLOW.md template summary
Topic → evidence package → script → voice → storyboard → image gen → i2v → music bed → ffmpeg → captions → **MANDATORY full pre-flight (Domain 5 defamation)** → upload draft.

---

# 7. `cinematic_ai_documentary` — Standard

Premium atmospheric video essay — true crime, mystery, narrative-driven. 20-40 min. Heaviest production cost in the bank.

## When this fits
"Cinematic true crime" / "atmospheric narrative" / "premium video essay" / "creepypasta" / "mystery storytelling"

## Reference channel types
- A true-crime narrative channel
- A long-form mystery-documentary channel (mystery deep-dives)
- A character-driven true-crime channel

## Dependency graph
```
1. Topic / case lock + research
2. Script (V13 Reluctant Witness OR V1 Documentary Authority, atmospheric register, ~20,000-35,000 chars)
3. Voice (premium tier mandatory — narrator carries the entire video)
4. Storyboard derived from voice timestamps + atmospheric beats
5. Image gen — heavy on Nano Banana Pro for cinematic compositions
6. i2v with Kling 3.0 (premium) — atmospheric scenes, slow-motion, dramatic light changes
7. Music bed — sparse minor-key piano + occasional cello (cinematic true crime register)
8. ffmpeg with extra-careful audio mix (voice + music + ambient bed all weighted)
9. Pre-flight (defamation gate critical for true crime)
10. Upload draft
```

## AI tool stack
- Image: Nano Banana Pro (premium) — cinematic register requires the best
- Video: Kling 3.0 (premium) — atmospheric motion needs the best
- Voice: Minimax Speech-02 HD (premium) — voice IS the brand
- Music: Suno v5.5

## Channel-specific questions
- Length target (20 / 30 / 40 min)
- True crime vs mystery vs narrative fiction
- Composite character labeling required? (yes for true crime — Skill 13 §3 Domain 5)

## Credit estimate per video
~3,000-5,000 credits — most expensive in the bank. Premium tier across image + video + voice + 30-min runtime.

## Common failure modes
- Defamation risk highest in this archetype — full evidence package + composite labeling mandatory.
- Cinematic register requires consistent color grade — channel-locked style segment must be religiously appended to every image prompt.
- Long-form (40 min) hits HeyGen drift if archetype mistakenly tries avatar — DON'T offer avatar for this archetype, it's voice-led only.

## FLOW.md template summary
Topic → research → script → voice → storyboard → image gen (cinematic) → i2v (Kling 3.0 atmospheric) → music sparse → mix → captions → pre-flight (full Domain 5) → upload draft.

---

# 8. `picture_education` — Standard

Slideshow + Ken Burns motion + voiceover. Wildlife-encounter compilation pattern. Cheapest archetype to produce.

## When this fits
"Slideshow with narration" / "archival photos" / "historical Ken Burns" / "wildlife-encounter compilation" / "photoreal historical narration"

## Reference channel types
- A wildlife-encounter compilation channel (canonical archival B&W)
- A history-as-narrative channel (similar register, real footage)
- Various history-as-narrative channels

## Dependency graph
```
1. Topic + script
2. Voice (ElevenLabs or Minimax)
3. Image gen per beat (~50-100 images for 30-min episode)
4. Image post-processing (Python: contrast curve + grain + vignette + Ken Burns crop spec)
5. ffmpeg slideshow assembly with programmatic Ken Burns motion (no i2v needed)
6. Captions
7. Pre-flight (light unless real people referenced)
8. Upload draft
```

**Critical: no i2v generation.** Ken Burns motion is programmatic (Python or ffmpeg pan/zoom on stills). Saves ~80% of typical doc-voiceover cost.

## AI tool stack
- Image: Nano Banana Pro (premium) for archival-quality stills, or Flux Pro 1.1 for modern
- Voice: Minimax Speech-02 HD or ElevenLabs
- Video: n/a (programmatic motion)
- Music: Suno v5.5 (period-appropriate bed)

## Channel-specific questions
- Visual register (archival B&W like a wildlife-encounter compilation / modern infographic / period color)
- Image post-processing (which filters baked into the channel: grain intensity, vignette strength, contrast curve)
- Length target (10 / 20 / 30 min)

## Credit estimate per video
~600-1,000 credits — cheapest archetype. Image gen + voice + compute. No i2v.

## Common failure modes
- Image style drift across 100 slides → channel-locked style suffix mandatory.
- Ken Burns motion feels too uniform → vary pan direction + zoom intensity per slide programmatically.
- Voice-to-image sync drift on long episodes → use voice timestamps to position each slide, not naive interval.

## FLOW.md template summary
Topic → script → voice → image gen per beat → image post-processing → ffmpeg slideshow with programmatic Ken Burns → captions → pre-flight → upload draft.

---

# 9. `gaming_animation` — Standard

A fandom-lore battle channel pattern. Character vs character / gaming lore animation.

## When this fits
"Fandom-lore battle" / "gaming character vs character" / "gaming lore animated" / "Smash Bros style commentary"

## Reference channel types
- A fandom-lore battle channel (canonical)
- Various character-battle gaming channels

## Dependency graph
```
1. Matchup / lore lock (e.g., "Character A vs Character B Part 1")
2. Script (gaming hype register or factual lore register)
3. Storyboard (action-heavy, shot-by-shot)
4. Voice (Minimax TTS)
5. Keyframes per shot (Nano Banana Pro, gaming-art style)
6. i2v per shot (Kling Turbo 2.6 for action sequences)
7. ffmpeg with action-heavy cuts + gaming SFX (impact, transition whooshes)
8. Captions
9. Pre-thumbnail wins (gaming thumbnails are critical CTR drivers)
10. Pre-flight (light)
11. Upload draft
```

## AI tool stack
- Image: Nano Banana Pro (premium) for character art consistency
- Video: Kling Turbo 2.6 (mid) — action sequences
- Voice: Minimax Speech-02 HD
- Music: Suno v5.5 (epic battle / gaming hype)

## Channel-specific questions
- Lore consistency reference (which game's canon?)
- Action register (epic battle / commentary / lore deep-dive)
- Length target (5 / 10 / 15 min)

## Credit estimate per video
~1,500-2,200 credits — i2v-heavy for action shots.

## Common failure modes
- Character likeness consistency across shots → reference image library per character (build once, reuse).
- IP / copyright concerns on first-party characters (popular game franchises, etc.) → fan content typically allowed but commercial monetization gets risky. Pre-flight Domain 1 platform compliance check.

## FLOW.md template summary
Matchup → script → storyboard → voice → keyframes (with character library) → i2v → SFX-heavy ffmpeg → captions → pre-flight → upload draft.

---

# 10. `ambient_loop` — Standard

An ambient-soundscape channel pattern. 1-3 hour ambient music + looping visual.

## When this fits
"Ambient" / "1-hour [topic]" / "lofi study" / "sleep music" / "ambient loop"

## Reference channel types
- An ambient-soundscape channel (in-house reference)
- Various ambient YouTube channels (lofi/study, etc.)

## Dependency graph
```
1. Theme lock (e.g., "fantasy ambient")
2. User provides Suno tracks (1-5 MP3s)
3. User provides 20-second video loop (or generates one via i2v)
4. Pipeline assembles: loops video seamlessly + arranges tracks with crossfades + loops to target duration
5. ffmpeg final render at 1080p or 4K
6. Auto-generated YouTube metadata
7. Upload draft
```

**Critical: this is more of a configuration pipeline than a generation pipeline.** Most of the AI work is upstream (Suno tracks, optional i2v loop generation). The "production" step is mostly ffmpeg orchestration.

## AI tool stack
- Music: **Suno v5.5 — locked** (orchestral covers, ambient pads)
- Video: Optional Kling Turbo 2.6 for one-time 20-sec loop generation (then reused indefinitely)
- Voice: n/a
- Image: Optional Flux Pro 1.1 for thumbnail

## Channel-specific questions
- Target duration (1 hr / 2 hr / 3 hr)
- Visual loop style (cinematic landscape / fireplace / rain window / abstract)
- Music register (Suno prompts to default to)

## Credit estimate per video
~150-300 credits — by far the cheapest archetype. The track generation (~30) and optional video loop (~50-100 if generated) plus assembly (~50) is all that's billed.

## Common failure modes
- Visual loop has visible loop seam → use crossfade at loop boundary, or generate a longer one-time loop.
- Music tracks don't crossfade smoothly → align cut points to musical phrases (use Suno's structure markers).
- 4K render hits storage limits → render to 1080p by default, 4K only if user requests.

## FLOW.md template summary
Theme → user provides tracks + loop → ffmpeg orchestration → metadata → upload draft.

---

# 11. `compilation_supercut` — Standard

Reaction compilations / supercuts / best-of channels.

## When this fits
"Compilation" / "supercut" / "best of" / "reaction compilation"

## Reference channel types
- Various reaction/compilation channels

## Dependency graph
```
1. Theme / topic lock
2. Source clip pull (stock library OR research-mining specific clips)
3. Cut order
4. Optional narration glue (if user wants light narration)
5. ffmpeg supercut assembly + captions
6. Pre-flight (COPYRIGHT GATE is dominant)
7. Upload draft
```

## AI tool stack
- Voice: Optional Minimax Speech-02 HD for narration glue
- Image: Flux Pro 1.1 for thumbnail
- Video: n/a (sources are pre-existing clips)

## Channel-specific questions
- Source: stock library (Pexels / Storyblocks) OR research-mined OR user-provided
- Narration glue yes/no
- Compilation length (5 / 10 / 15 min)

## Credit estimate per video
~400-700 credits — lower because no i2v gen.

## Common failure modes
- **Copyright is the dominant risk.** Stock libraries are safest. Research-mined clips need fair-use justification.
- Narration glue feels forced if too heavy → keep it light, transition-only.

## FLOW.md template summary
Theme → source pull → cut order → narration glue (optional) → ffmpeg → captions → pre-flight (Domain 1 copyright critical) → upload draft.

---

# 12. `talking_head_real_footage` — Standard

Real human host with their own footage. Vlog / lifestyle / fitness / cooking creators.

## When this fits
"Vlog" / "real footage" / "I record myself" / "lifestyle channel"

## Reference channel types
- Various real-creator channels in vlog / lifestyle / cooking spaces

## Dependency graph
```
1. User provides raw footage (already shot)
2. Optional script for voiceover overlay
3. Optional voiceover (if user doesn't want to redub)
4. Transcription (Whisper)
5. Caption rendering
6. Editing (cuts + B-roll insertion if AI-supplemented)
7. Optional thumbnail generation
8. Pre-flight (light)
9. Upload draft
```

**Critical: this archetype is mostly NOT AI generation.** It's transcription + captions + thumbnails + editing assistance. Cheapest archetype after ambient_loop.

## AI tool stack
- Voice: n/a (real audio, optional Minimax for redubs)
- Image: Flux Pro 1.1 for thumbnails ONLY
- Video: n/a (real footage)
- Music: Suno v5.5 optional for backing tracks

## Channel-specific questions
- Caption system (S2 sentence-paced default for vlog)
- Thumbnail style
- Music yes/no
- Voiceover overlay yes/no

## Credit estimate per video
~150-300 credits — transcription + thumbnails + optional music. Smallest credit footprint after ambient.

## Common failure modes
- Captions don't track on-screen actions if speaker moves → use anchor-based alignment (Skill 10 §4).
- Multi-camera footage needs cut decisions the AI can't make → pre-flight surfaces "manual edit required" if footage exceeds AI editing capacity.

## FLOW.md template summary
User provides footage → transcription → captions → optional thumbnail + music → light pre-flight → upload draft.

---

# Cross-archetype principles

A few things that cut across all 12 archetypes — don't repeat them in each section.

## Compliance is always-on but calibrated

All 12 archetypes hit Skill 13 pre-flight. The depth of the check varies:
- **Full preflight (Domains 1-5):** YMYL channels (avatar_authority_longform, news_hijack_investigation, cinematic_ai_documentary)
- **Domain 1 + Domain 5 priority:** music_video_propaganda (inauthentic content + satire defamation), gaming_animation (IP)
- **Light preflight:** stickman_explainer, picture_education, vertical_shorts_hype, ambient_loop, compilation_supercut, talking_head_real_footage

## Composite labeling discipline

If the archetype uses named patient/victim/case stories, composite labeling is mandatory in script + description. Cross-channel R7. Applies most strongly to avatar_authority_longform, cinematic_ai_documentary, news_hijack_investigation.

## Channel asset library

For archetypes that benefit from reusable assets (stickman_explainer characters, gaming_animation character art, music_video_propaganda brick figures), the library lives at `projects/[channel]/assets/library/` and gets reused across episodes. First episode bears the bootstrap cost; subsequent episodes pull from library and only generate net-new assets.

## Credit estimates are ranges, not points

The estimates above show typical ranges. Actual cost depends on:
- Channel-locked tier choices (premium image vs mid)
- Episode length (within the archetype's range)
- B-roll density (affects total i2v cost)
- Asset library maturity (low for episode 1, stable from episode 5+)

Surface the range to the user in the proposal. Show actual after the first 3 episodes.

## Production speed varies dramatically

| Archetype | Typical end-to-end time per episode |
|---|---|
| `avatar_authority_longform` | 4-6 hours |
| `stickman_explainer` | 2-4 hours |
| `vertical_shorts_hype` | 30-60 min |
| `music_video_propaganda` | 6-8 hours |
| `documentary_voiceover` | 4-6 hours |
| `news_hijack_investigation` | 4-8 hours (research-heavy) |
| `cinematic_ai_documentary` | 8-12 hours (highest in bank) |
| `picture_education` | 1-2 hours |
| `gaming_animation` | 2-4 hours |
| `ambient_loop` | 30-60 min |
| `compilation_supercut` | 1-3 hours (depends on source pull) |
| `talking_head_real_footage` | 30-90 min (mostly post-production) |

This affects approval cadence default — cinematic_ai_documentary at 12 hours per episode strongly favors low-touch (user can't realistically high-touch every step). Surface the time estimate in the proposal so the user picks cadence with eyes open.
