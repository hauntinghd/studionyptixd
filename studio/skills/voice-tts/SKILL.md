---
name: voice-tts
description: >-
  Generates the voiceover via ElevenLabs / Minimax with per-archetype config. Load when configuring TTS or generating voice. Companion: config-bank.md (per-archetype configs V1-V15 + 8 per-channel locked configs + 12-prompt Minimax bank).
---

# Skill 08 — Voice / TTS Prompting

> **⚠ Model reference (2026-05) — cost/strength notes, NOT a closed list.**
>
> The table below is a starting reference of TTS models known to work well for these jobs, with their tradeoffs. It is **not the full set of what you can use** — ElevenLabs, Minimax, and fal host more TTS models, and the Vercel AI Gateway can route others. There is no single blessed default: pick the model that best fits the job. If the user names a model that isn't in this table, or you think something else suits the voice better, **discover what's actually available and use it** — check the provider's model list or WebSearch + WebFetch the docs. **Never tell the user a model is unavailable just because it's absent from this table — verify first.**
>
> | Strengths | Model id | Cost / use notes |
> |---|---|---|
> | Long-form narration, character voices, 30+ langs | `minimax/speech-02-hd` | Reliable general workhorse. |
> | High volume / cost-critical | `fal-ai/f5-tts` | ~100× cheaper than Minimax HD. Voice clone from 5-15s sample. Weaker on heavy emotion + non-English. |
> | Faster / cheaper variant | `minimax/speech-02-turbo` | Slight fidelity drop vs Minimax HD. |
> | Outage failover / edge cases | `elevenlabs/multilingual-v2`, `elevenlabs/v3`, `elevenlabs/turbo-v2_5` | Strong emotion + English fidelity. |
>
> **How to choose:** match the model to the job — general narration, high-volume/cheap, or emotion/English-fidelity — weighing quality against cost. Consider models beyond this table when they fit better.
>
> **Voice cloning:** ElevenLabs IVC (English, 1-3min sample) works well as `clone_voice`. Use `minimax/voice-clone` for non-English or short samples (8-30s). F5-TTS is zero-shot — no separate clone step, just pass sample URL as `voiceId`. Pick per the sample/language at hand rather than defaulting to one.
>
> Prices and model positioning drift — treat the cost notes as approximate and re-check current rates/availability when it matters. Detailed reasoning in Section 2 below is preserved for context but its specific model lists are stale where they conflict with this block.

---

This is the operational knowledge an AI YouTube agent needs to produce the right voice for any video in any niche, on any voice generator. The agent's job is to match a voice character to the channel's locked Voice DNA (from Skill 03), pick the right TTS engine, write the right voice prompt, and run a sample-then-confirm gate before generating the full episode audio.

The single most important rule: **the agent never produces a full-length episode voiceover without first running a 30-second sample for user approval.** A 25-minute personal-finance authority channel voiceover on the wrong voice character is a wasted hour and several dollars. Sample first, always.

---

## 1. The job of voice prompting

Voice prompting does four things:

1. **Matches voice character to channel's Voice DNA** (Skill 03 archetype → specific voice ID or prompt)
2. **Picks the right engine** based on voice quality requirements + format (long-form narration / shorts / music vocal / avatar lip-sync)
3. **Writes the voice generation prompt** (or selects from library) that produces the intended cadence, tone, and emotional register
4. **Runs the sample-confirm-generate workflow** so the user approves the voice before mass production

A weak voice match produces a script that reads correctly but sounds wrong. The audience can detect "AI narration" within 10 seconds when voice is off-archetype. The agent's job is to never let voice be the failure mode.

## 2. Voice generator selection

Three primary engines, plus the avatar voice case. The agent picks per channel based on Voice DNA + format + budget.

### ElevenLabs
**Strengths:** Highest fidelity in 2025-2026 for English narration. Library of ~50 high-quality stock voices across archetypes. Voice cloning available (v1.1 feature for RookCast). Excellent emotional range.
**Weaknesses:** Cost adds up at scale. Some Library voices have noticeable AI "tells" if not paired with the right prompt.
**Cost:** ~$0.18 per 1000 characters (varies by tier)
**Best for:** Authority channels (personal-finance authority channel, medical-authority channel doctor voice), long-form documentary, science narration, high-quality commercial work
**Default when:** quality matters and the budget supports it

### Minimax
**Strengths:** Custom voice prompting — describe a voice in natural language and Minimax synthesizes it. Excellent for stylized, character-driven channels. Lower cost than ElevenLabs at scale.
**Weaknesses:** English support strong but slightly more "synthetic" feel than ElevenLabs Library at premium settings. Variable consistency across very long generations.
**Cost:** ~$0.10 per 1000 characters
**Best for:** Stylized channels (Roblox-scenario channel "chaotic Gen Z shitposter"), niche character voices, anything where the character voice matters more than commercial smoothness
**Default when:** voice character is unusual or the script benefits from a specific personality the Library doesn't have

### Suno (vocal track)
**Strengths:** Music vocal — generates singing voices over melody. Required for music video / propaganda / drill production.
**Weaknesses:** Not for narration; only musical vocal.
**Cost:** Per-track pricing, varies
**Best for:** brick-narrative storytelling channel music videos, drill content, music video production where vocals are sung not spoken

### HeyGen avatar voice
**Strengths:** Lip-sync to a recorded avatar video. The voice is paired to the avatar's mouth movements.
**Weaknesses:** User must record the avatar in HeyGen — agent cannot generate the avatar's voice independently. The voice is locked when the avatar is created.
**Cost:** HeyGen subscription / per-avatar pricing (user-side cost)
**Best for:** personal-finance authority channel talking-head channels, medical-authority channel doctor avatar
**Default when:** the channel is talking-head with a HeyGen-rendered host

### Selection decision tree
1. Is this a music video / drill / propaganda? → Suno
2. Is the channel a HeyGen-recorded talking head? → HeyGen avatar voice (Nick records, agent doesn't generate)
3. Is the channel stylized/character-driven (Roblox shorts, comedy)? → Minimax custom voice
4. Is the channel authority/documentary/quality narration? → ElevenLabs Library
5. Default for new authority-style channels → ElevenLabs

## 3. Voice character matching to Skill 03 archetypes

The agent maps each of the 15 voice archetypes (from Skill 03) to specific engine + voice configuration.

| Archetype | Best engine | Specific config |
|---|---|---|
| V1 Documentary Authority (science-explainer / long-form mystery-documentary register) | ElevenLabs Library | "Adam" / "Charlie" / "Daniel" — male mid-low, calm pacing. Or "Rachel" / "Charlotte" for female narrator. Settings: Stability 0.55, Similarity 0.85, Style 0.0 |
| V2 Federal Credentialed Expert (personal-finance authority channel, medical-authority channel doctor) | HeyGen avatar voice OR ElevenLabs "Adam" / "Sam" | Male mid-range, slightly slower pace. Avatar lip-sync if HeyGen-based. ElevenLabs settings: Stability 0.60, Similarity 0.85 |
| V3 War Veteran Storyteller | ElevenLabs Library "Antoni" / "Daniel" with low Stability | Male older-sounding with weathered quality. Stability 0.40 (more variation, more lived-in). Style 0.20 |
| V4 Skeptical Investigator (investigative-journalism / geopolitics documentary register) | ElevenLabs "Adam" / "Charlie" with moderate energy | Male mid-energy. Stability 0.50, Style 0.30 (allows more inflection on pursuit moments) |
| V5 Mentor Coach (productivity / personal-finance education register) | ElevenLabs "Adam" / "Sam" / "Bill" | Male warm authority. Stability 0.55, Similarity 0.80, Style 0.10 |
| V6 Hype Showman (top viral-challenge register) | ElevenLabs Library or Minimax custom | High-energy male. Settings: Stability 0.45, Style 0.50. For chaotic-GenZ Roblox style → Minimax custom (see below) |
| V7 Deadpan Cynic (deadpan-history register) | ElevenLabs Library "Antoni" / "Charlie" with very low Style | Male mid-pace, deliberately flat. Stability 0.65 (highly consistent), Style 0.00 (no inflection — that IS the voice) |
| V8 Erudite Professor (economic-explainer / logistics explainer register) | ElevenLabs Library "Adam" / "Daniel" | Male slightly British-leaning if available. Stability 0.55, Similarity 0.85, Style 0.10 |
| V9 Best Friend Confidant (beauty vlogger) | ElevenLabs Library female voices ("Charlotte" / "Rachel" / "Domi") | Female warm, conversational. Stability 0.45 (more variation), Style 0.30 |
| V10 News Anchor (Vox style) | ElevenLabs Library "Adam" / "Rachel" | Mid-pace neutral. Stability 0.60, Style 0.10 |
| V11 Outraged Activist | ElevenLabs Library with elevated Style | Mid-male voice. Stability 0.45, Style 0.40 (allows passion variation) |
| V12 Curious Kid Adult (science-engineering personality register) | ElevenLabs Library "Adam" / "Liam" | Male warm energetic. Stability 0.50, Style 0.25 |
| V13 Reluctant Witness (true crime narrator) | ElevenLabs Library female voices ("Charlotte" / "Rachel") with low Style | Female mid-low, careful pacing. Stability 0.65, Style 0.10 |
| V14 Drill Rapper Narrator (brick-narrative storytelling channel) | Suno (sung) | Generated via Suno song; voice is part of the track, not separate TTS |
| V15 Wise Elder (faith, history) | ElevenLabs Library "Antoni" / "Daniel" / older voices | Slow-pace, reverent. Stability 0.60, Similarity 0.85, Style 0.05 |

These are starting points. The agent runs the Sample Gate (§5) and adjusts per channel.

## 4. ElevenLabs Library navigation

When the agent picks ElevenLabs, the Library voice ID matters. There are ~50 stock voices in 2026; the agent uses ~10 of them regularly.

### The agent's working list (English)

**Male voices**
- **Adam** — versatile mid-range, works for V1, V2, V4, V5, V8 Documentary/Authority/Skeptical/Mentor/Erudite. Most used voice in the agent's default.
- **Sam** — slightly warmer than Adam, works for V2, V5 Mentor/Coach.
- **Charlie** — younger-sounding mid-range, works for V4, V6, V7 Skeptical/Hype/Deadpan.
- **Daniel** — slightly British, works for V1, V8 Documentary/Erudite.
- **Antoni** — older-sounding weathered, works for V3, V7, V15 War Vet/Deadpan/Wise Elder.
- **Bill** — deep authority, works for V2, V5 Authority/Mentor.
- **Liam** — younger energetic, works for V6, V12 Hype/Curious Kid.

**Female voices**
- **Rachel** — versatile warm, works for V1, V9, V13 Documentary/Confidant/Reluctant Witness.
- **Charlotte** — slightly intimate, works for V9, V13 Confidant/Witness.
- **Domi** — energetic warm, works for V9 Confidant.
- **Bella** — younger softer, works for V12 Curious Kid female variant.

### The 4 settings the agent tunes

ElevenLabs voice generation has four settings that materially change output:

- **Stability (0.0-1.0):** lower = more emotional variation, higher = more consistent. Default 0.5. Narration usually 0.55-0.65. Drama 0.40-0.50.
- **Similarity boost (0.0-1.0):** how closely to match the source voice. Higher = tighter match. Default 0.75. Authority/news: 0.85. Stylized: 0.70.
- **Style (0.0-1.0):** how much "expressiveness" to inject. Higher = more dramatic. Default 0.0. Documentary/News/Deadpan: 0.0-0.10. Hype/Outrage: 0.30-0.50.
- **Speaker boost:** boolean. Almost always ON. Helps voice consistency across long generations.

The agent records the chosen settings per channel in the Channel Profile so they're consistent across all uploads.

## 5. Minimax custom voice prompt design

When ElevenLabs Library doesn't fit (stylized characters, niche voices), Minimax custom is the move. The agent writes a natural-language voice prompt that Minimax synthesizes.

### Minimax voice prompt structure

The prompt is a single rich paragraph describing:
1. **Demographic** — age, gender, accent, regional indicators
2. **Energy level** — calm / mid / high-energy / manic
3. **Signature traits** — uptalk, slow drawl, breathy, gravelly, monotone, etc.
4. **Audience relationship** — talking to friends / lecturing / conspiring
5. **Pace** — slow / mid / fast
6. **Vocabulary register** — formal / casual / slang-heavy

### Worked Minimax prompt — Roblox-scenario channel "chaotic Gen Z shitposter"

```
A 19-year-old American male Gen Z voice, slightly nasal, very fast pace with frequent jumps in volume and energy; uses uptalk on emphasis words; has a chaotic shitposter energy — the kind of voice that says "BRO" and "fr fr" naturally; sounds like someone who has been online too long and finds everything simultaneously hilarious and devastating; conversational and intimate, like he's whispering scandal to his best friend; vocabulary is slang-heavy, modern internet vernacular; pacing has occasional sudden pauses for comedic timing; voice carries a hint of fake-academic register when explaining absurd things — like he's narrating a documentary about something stupid.
```

This prompt produced the locked voice for our Roblox-scenario channel channel. ~60-80 word prompt. Specific demographic + energy + signature traits + audience relationship + pace + vocabulary register.

### Other example Minimax prompts

**Wise Elder for faith content:**
```
A 65-year-old male voice, warm and reverent, slow pace with deliberate pauses; sounds like a longtime pastor who speaks softly because he doesn't need to raise his voice to command attention; vocabulary is plain but elevated, occasionally uses biblical-cadence sentences; pacing is patient, with breath pauses between phrases; voice carries slight gravel from age but is fundamentally warm; audience relationship is teacher to seeker, never preachy.
```

**Deadpan Cynic for history shorts:**
```
A 30-year-old male voice with deliberately flat affect; mid-pace, no emotional variation regardless of content; vocabulary is plain but accurate; the voice treats absurd historical facts the same way it treats mundane ones — with the same monotone certainty; occasional very slight pause before a punchline word but never with vocal emphasis; audience relationship is equal-knowing peer who finds everything quietly funny; sounds like someone reading a Wikipedia article about something insane and being unmoved by it.
```

The agent stores these prompts in channel memory once locked.

## 6. The Sample Gate (cost protection rule)

This is the load-bearing safety rule for voice generation, mirrored from Skill 07. The agent NEVER generates a full-length episode voiceover without first running a 30-second sample.

### The sample workflow

1. **Voice character selected** based on Channel Voice DNA
2. **Engine + config locked** (ElevenLabs voice ID + settings, OR Minimax prompt)
3. **30-second sample script extracted** — typically the opening of the script (the hook section). This is where voice character matters most.
4. **Sample generated**
5. **Agent surfaces sample to user** with the full config visible: *"Here's the 30-sec sample at this voice config. Cost: $0.05. Approve to generate the full 25-min episode at $4.50? Or want me to adjust voice character / settings?"*
6. **User approves OR revises**
7. **Full episode generated** ONLY after explicit approval

### What the sample tests
- Does the voice match the Channel Voice DNA?
- Is the pacing right for the content?
- Are there obvious AI tells (over-pronunciation, robotic transitions, mispronounced names)?
- Does the emotional register fit the niche?

### When to skip the sample (rare)
Only when:
- Voice config is identical to a previously-approved voice for this channel
- User has explicitly enabled "no sample" mode for this batch (e.g., bulk re-render of approved voice)

The agent never decides to skip on its own.

### Sample cost
30-sec sample = ~75-100 characters of script = $0.01-0.02 in ElevenLabs cost. Trivial. ALWAYS run it.

## 7. Format-aware script preparation

Before voice generation, the agent applies format rules per engine. These are critical — wrong formatting produces robotic output.

### ElevenLabs format
- Numbers: digit form OK ("$3,400") or word form ("three thousand four hundred dollars") — both work, but consistency matters. Pick one per channel.
- Symbols: % and $ acceptable. & should be "and."
- Pauses: use SSML `<break time="0.5s">` for deliberate pauses.
- Emphasis: capitalization for emphasis works ("This is IMPORTANT") on some voices.
- Pronunciation overrides: SSML `<phoneme>` for tricky names.

### Minimax format
- Numbers: spelled as words ("three thousand four hundred dollars"). Digit form produces robotic delivery.
- Symbols: spell out fully (% → "percent," $ → "dollars").
- No formatting markers at all. Pure prose.
- Pacing: can sometimes be controlled via punctuation (em-dashes for pauses).

### Suno format
- Lyrics formatting with structure tags ([Verse 1], [Chorus], [Bridge])
- Bar-locked syllable counts
- Hook lines repeated verbatim

### HeyGen avatar voice format
- Same as the user's HeyGen recording configuration. Agent doesn't generate audio for HeyGen — user records, agent processes for caption alignment downstream.

The agent applies the right format BEFORE generating, never after. Re-rendering due to format mismatch is wasted budget.

## 8. The voice generation workflow

When the script is final and the user has approved:

**Step 1 — Voice DNA check.** Pull the Channel Voice DNA from memory. Confirm the archetype.

**Step 2 — Engine selection.** Match archetype + niche + budget to engine (ElevenLabs / Minimax / Suno / HeyGen).

**Step 3 — Voice config selection.** ElevenLabs: voice ID + 4 settings. Minimax: load custom prompt. Suno: song parameters. HeyGen: not applicable (user-recorded).

**Step 4 — Format application.** Apply format rules per engine. Convert numbers, symbols, etc.

**Step 5 — Sample generation.** Generate 30 seconds (typically the hook).

**Step 6 — Sample surface.** Show user with full config visible. Cost preview for full render.

**Step 7 — User approves OR revises.** If revise, return to Step 2 or 3 depending on the issue.

**Step 8 — Full generation.** Only after explicit approval. If the script comfortably fits in one TTS call, generate once and move on. Otherwise see Step 8b.

**Step 8b — Chunk + concat for long scripts.** Every TTS provider has a per-call character cap (ElevenLabs ~5K, Minimax ~5K, HeyGen voice ~2.5K — check the live `--help` output for the model you're using, NOT memory). If the script is longer than the cap (or the provider returns a length error), split the script at sentence/paragraph boundaries, generate one audio per chunk, then **immediately concat them via ffmpeg's concat demuxer BEFORE presenting anything:**

```bash
# 1. Write a file list (one line per chunk, in order)
printf "file 'chunk-001.mp3'\nfile 'chunk-002.mp3'\nfile 'chunk-003.mp3'\n" > concat.txt

# 2. Concat with the demuxer — this re-muxes properly (headers, sample rate, channels)
ffmpeg -f concat -safe 0 -i concat.txt -c copy voiceover.mp3

# 3. Verify the output is valid before presenting
ffprobe -v error -show_entries format=duration,size -of csv=p=0 voiceover.mp3
```

**⚠ NEVER use `cat chunk*.mp3 > out.mp3` or `ffmpeg -i "concat:a.mp3|b.mp3"` — both produce corrupt files with broken headers (0 sample rate, 0 channels). Always use `-f concat` with a file list.** Verify with `ffprobe` — if duration or size is 0/missing, the file is corrupt and must be regenerated.

Then upload the merged file via the standard blob workflow. The user sees ONE final audio file — never the raw chunks.

> **Hard rule: never `present` an individual chunk.** Two `present` calls in a row for the "same" voiceover is a bug — concat first, present the merged result once. The chunk rows stay in the DB for lineage but aren't shown to the user.

**Step 9 — Save voice config to Channel Memory.** Future episodes reuse without re-asking.

**Step 10 — Hand off to caption alignment** (downstream skill).

## 9. Anti-patterns

### Voice character anti-patterns
- **Wrong archetype match.** Generic "Adam" voice on what should be V14 Drill Rapper Narrator content — disaster. The agent always confirms archetype before engine selection.
- **Ignoring channel voice DNA.** Each channel has locked DNA after first 2-3 episodes. Don't re-pick voice every episode.
- **Switching engines mid-channel.** ElevenLabs Adam in episode 1, Minimax in episode 2 — audience notices. Lock engine per channel.

### Settings anti-patterns
- **Stability too high (>0.75).** Voice becomes monotone, robotic.
- **Stability too low (<0.30).** Voice becomes wildly inconsistent within a single episode.
- **Style too high (>0.50) on documentary content.** Voice becomes melodramatic.
- **Similarity too low (<0.65) on authority voices.** Voice drifts.

### Format anti-patterns
- **Digit numbers in Minimax.** Robotic. Spell out.
- **Symbols in any TTS.** Read literally as "dollar sign."
- **Missing pronunciation overrides for proper nouns.** "FinCEN" mispronounced as "fin-sen" instead of "fin-cen."
- **All-caps abuse.** ElevenLabs sometimes shouts; varies by voice.

### Workflow anti-patterns
- **Skipping the Sample Gate.** Largest budget burner.
- **Mass-rendering on a "trial" voice.** Even if you've used the voice before, episode-specific content can expose new failures.
- **Not saving config to memory.** Forces user to re-confirm voice every episode.
- **Mixing pre-recorded host voice with TTS for the same channel.** Audience notices the inconsistency.
- **Presenting raw TTS chunks instead of the concat.** If you split a long script across multiple TTS calls, the user must see ONE merged audio file — concat via `ffmpeg -f concat` first (see Step 8b). Two consecutive `present` calls for what should be one voiceover is the smoking-gun bug pattern.
- **Using `cat` or `concat:` protocol to merge MP3 files.** `cat chunk*.mp3 > out.mp3` and `ffmpeg -i "concat:a.mp3|b.mp3" -acodec copy out.mp3` both produce CORRUPT files — the output has broken headers (0 sample rate, 0 channels) and won't play in browsers. Always use `ffmpeg -f concat -safe 0 -i concat.txt -c copy out.mp3` with a file list. Always verify with `ffprobe` after concat.

## 10. Niche-specific voice notes

### Senior finance / IRS / retirement (personal-finance authority channel)
- **Engine:** HeyGen avatar voice (Nick records).
- **Sample Gate:** Not applicable (avatar pre-recorded). But the script format must be Minimax-compatible because audio extraction goes through Minimax-style processing.
- **Format:** Minimax format (numbers as words, no symbols).

### Tech / AI / dev tools
- **Engine:** ElevenLabs (quality matters; tech audience picky).
- **Voice:** "Adam" or "Charlie." Stability 0.55-0.60.
- **Tone:** V1 Documentary Authority or V8 Erudite Professor.

### Roblox-scenario channel shorts
- **Engine:** Minimax custom voice (chaotic Gen Z shitposter — locked).
- **Format:** Minimax. Slang and exclamations preserved.

### News-hijack documentary (investigative-journalism / medical-authority register)
- **Engine:** ElevenLabs.
- **Voice:** V4 Skeptical Investigator — "Adam" or "Charlie" with elevated Style 0.30.

### History / explainer (long-form mystery-documentary style)
- **Engine:** ElevenLabs.
- **Voice:** V1 Documentary Authority — "Daniel" or "Adam." Stability 0.60, Style 0.10.

### True crime
- **Engine:** ElevenLabs.
- **Voice:** V13 Reluctant Witness — female voice ("Rachel" or "Charlotte"). Stability 0.65, Style 0.10.

### Music videos / drill / propaganda (brick-narrative storytelling channel)
- **Engine:** Suno (vocal generated as part of the song).
- **No Sample Gate** in the same sense — the song IS the sample. User approves the Suno track before mass production.

### Health / medical / supplements (medical-authority channel)
- **Engine:** HeyGen avatar voice OR ElevenLabs (depending on channel format).
- **Voice if ElevenLabs:** V2 Federal Credentialed Expert (medical variant) — "Adam" or "Sam." Stability 0.60.

### Ambient / sleep / focus
- **Engine:** Often no narration. If narration: ElevenLabs V15 Wise Elder, very slow pace.

### Vlog / lifestyle / beauty
- **Engine:** Pre-recorded host voice (no TTS). Agent doesn't generate.

## 11. Worked examples

### Example 1 — personal-finance authority channel IRS episode (HeyGen avatar)

- **Voice DNA:** V2 Federal Credentialed Expert
- **Engine:** HeyGen avatar voice (user records in HeyGen, drops MP4 in input folder)
- **Format applied to script before user records:** Minimax format — numbers as words, no symbols
- **Sample Gate:** Not generated by agent (HeyGen recording is the user's responsibility)
- **Agent's role:** prepare the script in correct format, hand off to user. Once HeyGen MP4 is in input folder, agent runs preprocess (Whisper transcription) for caption alignment.

### Example 2 — Documentary explainer for science channel

- **Voice DNA:** V1 Documentary Authority
- **Engine:** ElevenLabs
- **Voice ID:** Daniel (slightly British register fits documentary)
- **Settings:** Stability 0.55, Similarity 0.85, Style 0.10, Speaker Boost ON
- **Format:** Mixed — digit numbers OK, no symbols
- **Sample Gate:** Generate first 30 seconds at full settings. User listens. Approves or adjusts.
- **Cost:** ~$0.02 for sample, ~$2.16 for full 12-minute episode

### Example 3 — Roblox-scenario channel short

- **Voice DNA:** Custom — "chaotic Gen Z shitposter"
- **Engine:** Minimax custom
- **Voice prompt:** [the locked Roblox prompt from §5]
- **Format:** Minimax, with exclamations and slang preserved
- **Sample Gate:** Generate first 15 seconds. User approves.
- **Cost:** ~$0.01 for sample, ~$0.30 for full 90-sec short

### Example 4 — Brick-narrative storytelling channel music video

- **Voice DNA:** V14 Drill Rapper Narrator
- **Engine:** Suno (vocal generated as part of song)
- **Suno parameters:** drill style, 136 BPM, English lyrics, male vocal
- **Sample Gate:** Generate the Suno track. User approves the full song before any visual production.
- **Cost:** Per Suno's pricing model (per-track)

### Example 5 — Doctor advice senior health (medical-authority channel style)

- **Voice DNA:** V2 Federal Credentialed Expert (medical variant)
- **Engine:** ElevenLabs (if AI narration) OR HeyGen (if avatar host)
- **Voice if ElevenLabs:** Adam, Stability 0.60, Style 0.10
- **Format:** Minimax format — numbers as words, no symbols, plus pronunciation overrides for medical terms
- **Sample Gate:** First 30 sec — typically the hook with a medical claim. User listens for: tone match, pronunciation accuracy, pacing.

## 12. Runtime checklist

Before any voice generation:

- [ ] Channel Voice DNA loaded
- [ ] Archetype confirmed (V1-V15)
- [ ] Engine selected per channel (locked, not per-episode)
- [ ] Specific voice ID + settings (ElevenLabs) OR custom voice prompt (Minimax) OR song parameters (Suno) OR avatar workflow (HeyGen)
- [ ] Script format applied per engine (Minimax / ElevenLabs / Suno / HeyGen)
- [ ] Pronunciation overrides added for proper nouns
- [ ] Sample generated (30 sec ElevenLabs/Minimax, full song Suno, N/A for HeyGen)
- [ ] Sample surfaced to user with full config visible
- [ ] Cost preview shown for full render
- [ ] User approval explicit before full generation
- [ ] Voice config saved to Channel Memory after approval

If any check fails, regenerate or surface to user. Never bulk-render without sample approval.

---

## Update log

This skill is current as of April 2026. Update when:
- New TTS engines emerge that materially shift quality/cost positioning
- ElevenLabs adds new Library voices the agent should know
- Suno releases new vocal capabilities
- HeyGen integrates new voice features
- Minimax expands custom voice capabilities

The agent's voice configurations per channel live in Channel Memory and are versioned. When a channel's locked voice is updated, the version is logged so older episodes' voices can be matched if needed.
