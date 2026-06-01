---
name: audio-mix-assembly
description: >-
  Mixes audio (sidechain ducking, music bed, close-lift) and assembles the final video. Load during the final mix + render stage of a production.
---

# Skill 11 — Audio Mixing & Video Assembly

This is the operational knowledge an AI YouTube agent needs to mix audio and assemble final video for any channel in any niche. Every paragraph is a concrete rule the agent applies.

Audio mixing and final assembly are the last 5% of production that determines whether a video reads professional or amateur. A great script with bad audio mix loses; a competent script with broadcast-quality mix wins. The difference is rarely artistic — it's mostly mechanical, and the mechanics are all in this skill.

This skill is the load-bearing companion to Skill 06 (Storyboard), Skill 08 (Voice/TTS), and Skill 10 (Captions). Skill 06 plans the shots; Skill 08 generates the voiceover; Skill 10 generates captions; this skill mixes everything and assembles final delivery.

---

## 1. The job of audio mixing & assembly

Audio mix and final assembly do six things in a delivered video:

1. **Maintain voiceover intelligibility** at -16 LUFS broadcast standard, with peaks under -1.5 dBFS, so the script's voice work actually reaches the viewer's ear.
2. **Build emotional register** through music selection and ducking that matches the script's voice DNA without competing with it.
3. **Anchor specific moments** with sound effects (form-stamp impact, money-counting whir, door-opening creak) that sit in the mix without distracting.
4. **Enforce platform delivery specs** — YouTube, Shorts, TikTok, Instagram all have different loudness, bitrate, and codec requirements.
5. **Coordinate timing** across the audio bed, captions, and motion graphics so everything lands on the right frame.
6. **Reserve a clean, predictable production envelope** so identical channels sound identical episode to episode (brand).

The agent never treats audio mixing as a render afterthought. Every decision (music pick, duck depth, peak target, fade curve) is intentional and documented in the Channel Profile.

---

## 2. The optimization target

The agent optimizes audio against **broadcast-grade intelligibility-weighted retention**:

- **Voiceover loudness:** -16 LUFS (YouTube target), peaks ≤ -1.5 dBFS.
- **Music loudness:** -22 to -28 LUFS depending on niche, ducked further under voiceover.
- **SFX loudness:** -20 to -16 LUFS, sit just at or below voiceover level.
- **Ambient bed loudness:** -32 to -38 LUFS, perceptible but never competing.
- **Stereo image:** voiceover always centered (mono-summed), music wide stereo, SFX positioned per scene.
- **Final mix:** 16-bit 48kHz minimum (YouTube standard); 24-bit 48kHz preferred for archival masters.

A mix that hits all four metrics is "broadcast-grade." A mix that has a hot voiceover but loud music behind it is amateur.

---

## 3. The 4-layer audio model

Every long-form video has four audio layers stacked in this priority order. The agent never collapses or skips a layer; the layers ARE the production.

### Layer 1 — Voiceover (VO)

**Source:** ElevenLabs, Minimax, HeyGen avatar audio, or recorded talent.
**Target loudness:** -16 LUFS integrated; peaks -1.5 dBFS.
**Position:** mono, centered.
**Treatment:** mild compression (3:1, threshold -18 dB), de-esser if sibilance present, high-pass at 80 Hz to remove rumble, gentle EQ presence boost (~+2 dB at 3 kHz).
**Rule:** VO is the foundation. Every other layer is mixed RELATIVE to VO. Set VO first, then duck other layers under it.

### Layer 2 — Music bed

**Source:** Suno, Epidemic Sound, Artlist, royalty-free libraries, or original composition.
**Target loudness:** -24 LUFS unducked (ambient register); -32 LUFS ducked under VO.
**Position:** wide stereo.
**Treatment:** sidechain compression (or static -8 dB ducking) when VO is present. Optional EQ scoop at 1-3 kHz to leave space for VO presence.
**Rule:** music is mood, not melody. Viewers should feel the music, not consciously notice it. If a viewer comments "what's that song?" the music was too prominent.

### Layer 3 — Sound effects (SFX)

**Source:** SFX libraries (Splice, Soundsnap, freesound.org), curated per channel.
**Target loudness:** -18 LUFS for emphasis SFX; -22 LUFS for subtle SFX.
**Position:** stereo placement per scene (left/right pan acceptable for spatial SFX).
**Treatment:** light compression to keep peaks controlled.
**Rule:** SFX punctuate. They land on motion graphic appearances, scene transitions, key reveals. Never use SFX continuously throughout — that's ambient bed work.

### Layer 4 — Ambient bed

**Source:** room tone, environmental ambience (rain, traffic, kitchen ambience), or pure low-shelf noise.
**Target loudness:** -34 LUFS — perceptible but subliminal.
**Position:** wide stereo.
**Treatment:** gentle high-shelf cut to remove brightness; long-tail reverb if room tone.
**Rule:** ambient bed fills silence. When VO pauses for 0.5+ seconds, the ambient bed prevents the mix from feeling "AI" or "robotic." Always present, never noticed.

---

## 4. Ducking discipline

Ducking is the most-misunderstood mixing technique in YouTube production. The agent uses one of two ducking methods, locked per channel.

### Method A — Sidechain compression (preferred)

The VO triggers a compressor on the music/ambient buses. When VO is present, the buses duck dynamically. When VO pauses, the buses ride back up.

**Settings:**
- Threshold: -25 dB (the music compressor opens when it hears VO above this level)
- Ratio: 4:1 to 6:1
- Attack: 5-15 ms (fast but not pumping)
- Release: 200-400 ms (smooth fade back up between sentences)
- Makeup gain: 0 dB

**Best for:** dialogue-heavy, natural-feeling productions (long-form authority, news-hijack, true crime).

### Method B — Static ducking (alternative)

The music is set to two levels — full when VO is absent, ducked by a fixed amount when VO is present. Switched manually or programmatically per VO segment.

**Settings:**
- Unducked level: -22 to -24 LUFS
- Ducked level: -32 LUFS (fixed -8 to -10 dB drop)
- Crossfade: 200-300 ms between states

**Best for:** music-led productions where music CAN'T duck dynamically without sounding broken (music videos use beat-locked timing; static ducking respects the bar structure).

### Per-niche ducking depth

| Niche | Duck depth (VO present) | Method | Music profile when present |
|---|---|---|---|
| personal-finance authority channel (IRS register) | -10 dB | Sidechain | Tense investigative bed, light percussion |
| medical-authority channel | -8 dB | Sidechain | Warm hopeful bed, slight piano motif |
| Roblox-scenario channel | -6 dB | Static | High-energy synth, punchy at all times |
| brick-narrative storytelling channel | 0 dB (no duck) | N/A — VO IS the music | Drill instrumental, full level |
| Documentary explainer | -12 dB | Sidechain | Cinematic ambient pad, no rhythm |
| Tech / AI | -8 dB | Sidechain | Modern electronic, light percussion |
| News-hijack documentary | -10 dB | Sidechain | Tense investigative, occasional drone |
| True crime | -14 dB | Sidechain | Sparse minor-key piano, very subtle |
| Cooking | -6 dB | Static | Upbeat acoustic / lo-fi at all times |
| Real estate | -8 dB | Sidechain | Modern upbeat, midtempo |
| Faith | -12 dB | Sidechain | Slow piano + soft strings |
| Music video | 0 dB | N/A | The VO is in the mix |

### When NOT to duck

- **Music videos:** the VO IS the vocal track, mixed at 0 dB to the instrumental.
- **Pure ambient channels:** there's no VO; the music is the product.
- **Talking-head segments where VO and SFX both fire:** SFX takes priority; VO ducks instead of music.

---

## 5. Per-niche music selection

The agent picks the music register from this table, then sources from an approved library. Music selection is the second-most-important mood lever after voice.

### Personal-finance authority channel (IRS register)

- **Register:** tense investigative / quiet authority
- **Reference style:** investigative-journalism bed, 60 Minutes investigative beds, podcast-investigative
- **Tempo:** 80-100 BPM
- **Instrumentation:** light percussion (kick + soft hat), sustained synth pad, occasional piano
- **Key:** minor (D minor, A minor common)
- **Length needed:** ~26 minutes (loop or compose to length)
- **Source:** Epidemic Sound "Investigative" / "Crime & Mystery" categories; Suno custom for hero episodes

### Medical-authority channel (senior health)

- **Register:** warm authoritative / hopeful
- **Reference style:** PBS health docs, NPR health features
- **Tempo:** 70-90 BPM
- **Instrumentation:** acoustic guitar, soft piano, light strings
- **Key:** major (C major, G major common); occasional relative minor for case stories
- **Length:** ~25 minutes
- **Source:** Epidemic Sound "Wellness" / "Documentary"

### Roblox-scenario channel

- **Register:** chaotic high energy / playful
- **Tempo:** 130-150 BPM
- **Instrumentation:** synth lead, punchy drums, occasional 8-bit chiptune accents
- **Length:** 60-90 seconds
- **Source:** Suno custom or Epidemic "Gaming Energy" categories

### brick-narrative storytelling channel

- **Register:** drill instrumental
- **Tempo:** 130-145 BPM
- **Instrumentation:** drill kit (808s, hi-hats, snare), dark melodic loop, occasional vocal chops
- **Length:** 3:00-3:30
- **Source:** Suno (primary), occasional licensed drill instrumental
- **Locked BPM per channel:** 136 (brick-narrative storytelling canonical)

### Documentary explainer (long-form mystery-documentary style)

- **Register:** cinematic ambient / contemplative
- **Tempo:** 60-80 BPM (or no tempo — pad-based)
- **Instrumentation:** sustained synth pads, occasional piano motif, light orchestral textures
- **Key:** minor or modal
- **Length:** ~12-18 minutes
- **Source:** Epidemic Sound "Cinematic Documentary" / "Ambient"

### Tech / AI

- **Register:** modern electronic / upbeat
- **Tempo:** 100-120 BPM
- **Instrumentation:** synth bass, light electronic percussion, occasional arpeggio
- **Source:** Epidemic "Tech" / "Modern Electronic"

### News-hijack documentary

- **Register:** tense investigative / investigative-journalism style
- **Tempo:** 90-110 BPM
- **Instrumentation:** synth bass, occasional pulsing drone, light percussion
- **Source:** Epidemic "Crime Investigation" / "True Crime"

### True crime

- **Register:** sparse minor-key contemplative
- **Tempo:** 60-80 BPM or pad-based
- **Instrumentation:** solo piano, occasional cello, sparse strings
- **Key:** minor
- **Source:** Epidemic "True Crime" / "Mournful"

### Cooking

- **Register:** upbeat acoustic / lo-fi warm
- **Tempo:** 90-110 BPM
- **Instrumentation:** acoustic guitar, ukulele, light percussion, sometimes lo-fi beats
- **Source:** Epidemic "Cooking" / "Warm Acoustic"

### Real estate

- **Register:** modern upbeat / aspirational
- **Tempo:** 100-115 BPM
- **Instrumentation:** light electronic + acoustic blend
- **Source:** Epidemic "Real Estate" / "Aspirational Modern"

### Faith / Christian

- **Register:** reverent contemplative
- **Tempo:** 60-80 BPM or no tempo
- **Instrumentation:** piano, soft strings, occasional acoustic guitar
- **Key:** major or modal, never aggressive minor
- **Source:** Epidemic "Worship" / "Cinematic Spiritual"

---

## 6. SFX bank per niche

Each channel has a locked SFX bank. The agent inserts SFX at specific cue points planned in the storyboard.

### Personal-finance authority channel (IRS register) — SFX bank

- **Form stamp** — at any "form filed" moment or numbered list reveal
- **Money counting whir** — at any dollar figure reveal
- **Cabinet drawer slide** — at mechanism reveal moments
- **Phone receiver click** — at SSA call mention
- **Page turn** — at reading SSA letter / case file moments
- **Subtle riser (whoosh)** — at major reveals or beat transitions

### Medical-authority channel — SFX bank

- **Pill bottle shake/cap** — at supplement mention
- **Heart monitor beep** — at cardiac/vital mention (sparingly)
- **Stethoscope click** — at clinical examination mention
- **Page turn** — at journal/study citation
- **Soft riser** — at reveal moments

### Roblox-scenario channel — SFX bank

- **8-bit "level up" chime** — at premise reveal
- **Cartoon "boing"** — at absurd consequences
- **Roblox-style impact thud** — at chaotic moments
- **Synth riser** — at hook climbs

### Brick-narrative storytelling channel — SFX bank

- **808 drops** — beat-locked
- **Vocal chops / ad-libs** — at bar transitions
- **Risers** — at chorus entry
- **Drum fills** — at section transitions

### Documentary explainer — SFX bank

- **Subtle whoosh** — at scene transitions
- **Page turn** — at chapter / section break
- **Camera shutter** — at archival photo reveal (rare)
- **Tape reel** — at archival audio reveal (rare)
- **Subtle hum** — under entire mechanism reveal beats

### News-hijack — SFX bank

- **Document slide-on** — at evidence reveal
- **Camera shutter** — at photo evidence
- **Phone notification** — at message screenshot reveal
- **Subtle riser** — at major investigation milestones
- **Vinyl crackle** — at archival framing (sparingly)

### True crime — SFX bank

- **Distant rumble** — at heavy moments
- **Single piano note** — at named victim moment
- **Page turn** — at case file moments
- **Wind / atmosphere** — for time-of-event scenes
- **Soft impact** — never aggressive

### SFX placement discipline

- **Maximum 1 SFX per 2 seconds.** More than that is noise.
- **SFX always at -18 LUFS or below** unless intentional emphasis.
- **SFX never overlap VO syllables.** If VO is mid-word, SFX waits 100 ms.
- **SFX always pre-rendered** to the audio bed, not added at assembly time.

---

## 7. Music length and looping discipline

Most channels use music longer than the source loop. The agent loops or stitches with discipline.

### Stitching method (preferred over looping)

For 25-minute IRS videos with a 3-minute Epidemic source:
1. Cut the source into "intro" / "loop body" / "outro" segments at musically natural points (downbeats).
2. Use the intro at the top of the video.
3. Repeat the loop body N times, with crossfades at 200-400 ms between repetitions.
4. Use the outro at the close.

This is more natural than looping the same 3-minute file 9 times.

### Looping method (fallback)

If only a single track length is available:
- **Loop crossfade:** 1-2 second crossfade between loop end and loop start.
- **Pitch detection:** ensure loop boundaries land on the same chord (no half-step shift between iterations).
- **Volume automation:** very slight (-1 dB) attenuation at loop boundary to mask transition.

### Multi-track method (richest)

For longer videos, prefer 2-3 different tracks that share a register:
- Track A: opening (0:00-8:00)
- Track B: middle (8:00-18:00)
- Track C: close (18:00-end)

Crossfades 4-8 seconds between tracks. The viewer feels variety without conscious shift.

### Music level automation per beat

Beyond ducking, the agent automates music level per script beat:

| Beat | Music level (relative to VO) |
|---|---|
| 1 — Hook | -10 dB (ducked + slight extra duck for VO clarity) |
| 2-3 — Promise + credentials | -10 dB |
| 4 — Core story | -12 dB (heavier duck for emotional weight) |
| 5 — Mechanism | -10 dB |
| 6 — Three examples | -10 dB |
| 7 — Numbered list | -8 dB (lighter duck — list pace allows more music presence) |
| 8 — Bonus | -10 dB |
| 9-10 — CTAs | -8 dB |
| 11-12 — Close + share | -6 dB (music rides up at close for emotional lift) |

The close lift is a small but meaningful retention move — the music gets slightly louder at "I'll see you in the next video," which feels like emotional resolution.

---

## 8. Final mix targets per platform

YouTube and other platforms have different loudness, codec, and bitrate targets. The agent renders to platform spec.

### YouTube (long-form, 16:9)

- **Container:** MP4 (H.264 video, AAC audio)
- **Resolution:** 1920×1080 (or 4K if source supports)
- **Frame rate:** match source (24, 25, 30, 60)
- **Video bitrate:** 8-12 Mbps for 1080p, 35-45 Mbps for 4K
- **Audio loudness:** -16 LUFS integrated
- **Audio peak:** -1.5 dBFS true peak max
- **Audio codec:** AAC LC, 320 kbps stereo
- **Audio sample rate:** 48 kHz

### YouTube Shorts (vertical, 9:16)

- **Container:** MP4
- **Resolution:** 1080×1920
- **Frame rate:** 30 or 60
- **Video bitrate:** 10-15 Mbps
- **Audio loudness:** -14 LUFS integrated (Shorts target slightly hotter than long-form)
- **Audio peak:** -1.0 dBFS true peak max
- **Audio codec:** AAC LC, 256 kbps stereo
- **Audio sample rate:** 48 kHz

### TikTok (vertical, 9:16)

- **Container:** MP4
- **Resolution:** 1080×1920
- **Frame rate:** 30
- **Video bitrate:** 8-12 Mbps
- **Audio loudness:** -14 LUFS
- **Audio peak:** -1.0 dBFS

### Instagram Reels

- Identical to TikTok specs.

### Archival master (always render this)

- **Container:** MOV (ProRes 422 HQ video, PCM 24-bit audio)
- **Resolution:** native source (don't downscale for master)
- **Audio:** 48 kHz 24-bit PCM, no loudness limiting
- **Use case:** repurposing, future re-renders, future platform deltas

The agent renders the platform-specific deliverable AND the archival master. Storage is cheap; re-shooting is expensive.

---

## 9. Assembly pipeline

The agent runs assembly in a fixed sequence. Skipping steps causes file collisions, sync drift, or render failures.

### Step 1 — Asset gathering

- Voiceover audio (all segments)
- Music tracks (with metadata)
- SFX files (named per cue)
- Generated video clips (per shot)
- Captions (rendered per Skill 10)
- Motion graphics (per Channel Profile)
- Title cards / outro cards

### Step 2 — Audio mix in DAW (or scripted ffmpeg)

- VO mastering (compression, EQ, de-ess)
- Music stitching / looping
- SFX placement
- Ambient bed application
- Ducking automation
- Final loudness normalize to platform target

### Step 3 — Video assembly (timeline)

- Each shot placed on the video timeline at the storyboard timestamp
- Transitions per Skill 06 §6 (cuts dominant for authority, etc.)
- Motion graphic overlays at planned timestamps
- Captions overlay at locked position (per Skill 10 §5)
- Title card at 0:00, outro card at close

### Step 4 — Sync check

- VO audio aligned with on-screen mouth movement (HeyGen avatars) or lower-third subtitle pacing
- SFX cues land within ±50 ms of visual triggers
- Music level changes hit beat boundaries

### Step 5 — Render to platform spec

- Run platform render with locked codec/bitrate/loudness settings
- Render archival master in parallel
- Verify file integrity (no half-rendered frames, no audio dropouts)

### Step 6 — Final QC pass

- Scrub at 5 random timestamps and verify audio + video sync
- Listen to the complete first 30 seconds — does the mix sound broadcast-grade?
- Spot-check captions sync at random points
- Verify file size within reasonable bounds (a 25-min 1080p render is ~800-1200 MB)

### Step 7 — Upload

- Upload platform deliverable
- Move archival master to cold storage
- Log render completion in Channel Profile

---

## 10. Per-pipeline assembly toolchains

Different channels use different assembly toolchains. The agent picks per channel.

### Pipeline A — Personal-finance authority channel (Remotion-based motion graphics + ffmpeg final assembly)

- **VO source:** HeyGen avatar render
- **Mix tool:** ffmpeg with sidechain compressor filter
- **Motion graphics:** Remotion (TSX) compiled to MP4 overlays
- **Caption render:** custom Whisper + anchor alignment Python script
- **Final assembly:** ffmpeg concat + overlay
- **Pre-flight check:** preprocess script verifies audio sync before main render

### Pipeline B — Brick-narrative storytelling channel (RunPod LTX-2.3 + Suno + ffmpeg)

- **VO source:** Suno (full vocal track)
- **Mix tool:** Suno mix is pre-mixed; ffmpeg only handles concat
- **Video clips:** LTX-2.3 on RunPod, ~50 clips per video
- **Caption render:** Whisper anchor-aligned for lyric captions
- **Final assembly:** ffmpeg concat with beat-locked timing
- **Pre-flight:** verify all 50 clips rendered before assembly

### Pipeline C — Roblox-scenario channel (Minimax + Kling + ffmpeg)

- **VO source:** Minimax custom voice
- **Mix tool:** ffmpeg simple mix (vertical short doesn't need complex mix)
- **Video clips:** Kling 2.1 Pro for hero shots, Hailuo for B-roll
- **Caption render:** Whisper word-pop for vertical
- **Final assembly:** ffmpeg concat with vertical resolution
- **Pre-flight:** verify caption alignment within 50ms of speech

### Pipeline D — Medical-authority channel (mixed: real footage + AI overlays)

- **VO source:** ElevenLabs (faceless) OR HeyGen avatar
- **Mix tool:** DaVinci Resolve OR ffmpeg
- **Video:** mostly stock real footage (Pexels, Storyblocks) + AI overlays (Nano Banana)
- **Caption render:** anchor-aligned per Skill 10
- **Final assembly:** Resolve timeline OR ffmpeg with overlay filters

The agent records the channel's pipeline in the Channel Profile so subsequent episodes use the same toolchain.

---

## 11. Anti-patterns

Eight audio mix and assembly mistakes that cost re-renders.

### Anti-pattern 1 — Loud music behind voiceover

Music sitting at -18 LUFS while VO is at -16 LUFS. VO is unintelligible at low playback volume; viewer drops.
**Fix:** music ducks to -28 to -32 LUFS under VO. Per §4.

### Anti-pattern 2 — Music with vocals in long-form authority

Music track has its own vocal line; viewer's brain tries to parse two voices simultaneously.
**Fix:** all long-form authority music is instrumental. Vocal music is reserved for music video productions where the vocal IS the VO.

### Anti-pattern 3 — VO peaks clipping at 0 dBFS

Hot VO from ElevenLabs (sometimes peaks at -0.1) goes through compressor with low ceiling and clips on consonants.
**Fix:** limiter at -1.5 dBFS true peak ceiling on the VO chain. Cite §2 spec.

### Anti-pattern 4 — Stale ambient bed

Same room tone loop runs for 25 minutes and develops a perceptible texture pattern. Viewer's ear locks onto it consciously.
**Fix:** ambient bed is 60-second random non-repeating sample, looped with random-offset crossfades, OR pure low-shelf noise (true random).

### Anti-pattern 5 — SFX volume inconsistency

Some SFX at -12 LUFS (loud), others at -22 LUFS (quiet) within the same video.
**Fix:** master all SFX to -18 LUFS before placement. Per §3.

### Anti-pattern 6 — No music level lift at close

Music stays at ducked level through outro; close feels emotionally flat.
**Fix:** music level lift to -6 dB ducked depth in beats 11-12. Per §7.

### Anti-pattern 7 — Sync drift across long video

VO and video drift by 200+ ms by minute 20 because frame rates were inconsistent or the VO pipeline introduced micro-stretches.
**Fix:** anchor sync at 5 timestamps (0:00, 25%, 50%, 75%, 100%) during assembly. Verify ±20 ms at each anchor before final render.

### Anti-pattern 8 — Render fails because preprocess didn't complete

Common on automated pipelines. Render starts before preprocess finishes; file collision corrupts MP4.
**Fix:** add 45-second buffer after preprocess detection (lesson from a prior episode's file-lock race).

### Anti-pattern 9 — Unbounded ffmpeg loop hangs the encode forever

ffmpeg invocations that loop an input (`-loop 1 -i bg.jpg`, `loop=loop=-1` in a filter graph) without a duration cap produce an output stream that never ends. The encoder keeps writing frames indefinitely, the sandbox CPU pegs at 100%, and the Bash tool's 2-minute timeout fires — but the tool card appears stuck in "loading" until the kill propagates. Saw this in bug report `br_ixnfotg37s9pkw7z3hfjmrtd`: the agent tried to composite a still background onto a 14-second avatar video using

```
ffmpeg -i bg.jpg -i avatar.mp4 \
  -filter_complex "[0:v]loop=loop=-1:size=14:start=0[bg]; [bg][1:v]overlay=0:0[out]" \
  -map "[out]" -map 1:a -c:v libx264 -c:a aac out.mp4
```

The looped background never EOFs, and `overlay`'s default `eof_action=repeat` means the filter graph never EOFs either → infinite encode.

**Fix patterns.** Pick the one that fits the input shape:

- **Cap by the finite input (preferred).** Add `-shortest` so the output ends when the shortest input does. Works when one input is naturally bounded (voiceover audio, the avatar video).

  ```
  ffmpeg -loop 1 -i bg.jpg -i avatar.mp4 \
    -filter_complex "[0:v][1:v]overlay=0:0" -map 1:a \
    -c:v libx264 -c:a aac -shortest out.mp4
  ```

- **Cap by absolute duration.** Add `-t <seconds>` (output) or `-t <seconds>` after each `-i` (per-input). Use when none of the inputs are bounded — e.g. compositing two looped sources.

  ```
  ffmpeg -loop 1 -i bg.jpg -i avatar.mp4 \
    -filter_complex "[0:v][1:v]overlay=0:0" -map 1:a \
    -c:v libx264 -c:a aac -t 14 out.mp4
  ```

- **Bound the loop itself.** Use `loop=size=<frames>:start=0` without `-1`, OR pass a finite loop count. The filter then ends naturally.

- **Stop overlay on the bottom input's EOF.** Add `eof_action=endall` (or `:repeatlast=0` plus a duration cap on the underlying source) so `overlay` doesn't keep emitting frames once the looped layer terminates.

**Rule of thumb.** If any input or filter in the graph is "infinite" (`-loop 1`, `loop=-1`, `anullsrc`, `color=...`), the command MUST end with one of `-shortest`, `-t <seconds>`, or a finite `loop=size=...` count. No exceptions. If you can't reason about which input bounds the output, you don't have a runnable command yet.

---

## 12. Worked example — full personal-finance authority channel example episode mix sheet

```
Example episode — Social Security May Shake-Up
Pipeline: A (personal-finance authority channel Remotion + ffmpeg)

VO SETTINGS (HeyGen → ElevenLabs Bill source)
  Loudness: -16 LUFS integrated
  True peak: -1.5 dBFS
  Compression: 3:1 ratio, threshold -18 dB
  EQ: HPF 80 Hz, +2 dB shelf at 3 kHz
  De-esser: 6:1 above 6 kHz when sibilance present

MUSIC (Epidemic "Investigative Tension" — 8-min track stitched 4x)
  Unducked level: -24 LUFS
  Ducked level: -34 LUFS (sidechain compressor, -10 dB duck)
  Stitching: intro 0:00-0:45 / loop body 0:45-7:00 / outro 7:00-8:00
  Per-beat level lift: -6 dB ducked at beat 11 close

SFX BANK
  Form stamp: at all 5 numbered list item reveals (-18 LUFS)
  Money whir: at all dollar reveals in Beat 4 ($246K Patricia loss) (-20 LUFS)
  Cabinet slide: at Beat 5 mechanism reveal (-18 LUFS)
  Page turn: at Beat 4 letter unfold (-22 LUFS)
  Subtle riser: at Beat 1→2 transition, Beat 5→6 transition, Beat 8 bonus (-20 LUFS)

AMBIENT BED
  Source: kitchen ambience (60 sec random sample looped)
  Level: -34 LUFS
  Roll-off: -3 dB shelf at 8 kHz to remove brightness

CAPTION SYSTEM (per Skill 10)
  S3 hybrid (Inter Bold body + Bebas Neue Bold emphasis)
  Mishear dictionary loaded
  Anchor-aligned

FINAL DELIVERY
  YouTube long-form: 1920x1080, 12 Mbps, AAC 320 kbps, -16 LUFS, -1.5 dBFS peak
  Archival master: ProRes 422 HQ, PCM 24-bit, no loudness normalization
```

This worked sheet lives in the Channel Profile. Subsequent IRS episodes copy it and adjust SFX cues per script.

### Second worked example — brick-narrative storytelling channel music video mix sheet

The brick-narrative storytelling channel uses Pipeline B (§10) and has radically different audio mix needs from the personal-finance authority channel. The §12 personal-finance authority channel example demonstrates Pipeline A authority discipline; this brick-narrative storytelling example demonstrates Pipeline B music-video discipline.

```
Brick-narrative storytelling channel example episode — "Never Close It Again"
Pipeline: B (RunPod LTX-2.3 + Suno + ffmpeg)
BPM: 136 (channel canonical)
Track length: 3:12 (Suno actual; targeted 3:30; per memory rule Suno comes 15-20% short)

VOCAL TRACK + INSTRUMENTAL (single Suno output)
  Source: Suno-generated, full track including vocals + drill instrumental + ad-libs
  Target loudness: -14 LUFS integrated (Shorts spec — slightly hotter than long-form)
  Peak ceiling: -1.0 dBFS true peak
  Mastering chain: minimal — Suno output is broadcast-ready
    - Apply: final limiter at -1.0 dBFS ceiling
    - Apply: loudness normalize to -14 LUFS
    - Skip: compression, EQ, de-esser (Suno already handles these)

DUCKING
  Method: N/A
  Duck depth: 0 dB (vocals ARE the music; no ducking)
  Reason: this is a music video, not narration. The vocal track is mixed in by Suno; trying to "duck the music under the vocal" would mean ducking the music under itself.

SFX BANK
  All SFX are baked into the Suno instrumental — no SFX layer added at assembly time
  (808 drops, vocal chops, risers, drum fills are part of the drill production)
  Exception: occasional "explosion" SFX or "siren" SFX overlay if scene calls for it; layered at -16 LUFS

AMBIENT BED
  N/A (music video format; no ambient bed needed)
  The drill instrumental is continuous and fills all silence

PER-BAR LEVEL AUTOMATION
  Music level is constant throughout (not beat-by-beat ducked)
  Optional: -1 dB attenuation at bridge (if the bridge calls for visual reset)
  Optional: +0.5 dB lift on final chorus return (last 8 bars)

CAPTION SYSTEM (per Channel Profile §7)
  S4 block paragraph (lyric blocks aligned to bars)
  Anton font, white #FFFFFF + genre-saturated emphasis
  Black 70% opacity panel
  Lyric blocks displayed for ~7 sec each (4-line stanza per display)
  Mishear dictionary loaded (small — Suno output cleaner than ElevenLabs)

VIDEO CLIPS
  Source: LTX-2.3 generations on RunPod pod (~50 clips per video)
  Each clip: exact bar duration (3.5-4 sec at 136 BPM)
  Concatenation: bar-locked; clip transitions land on downbeats

FINAL ASSEMBLY (ffmpeg)
  Step 1: load Suno full track (vocal + instrumental, single .wav)
  Step 2: concatenate 50 LTX clips at bar boundaries (each clip = 1 bar = ~441ms at 136 BPM × 8 frames)
  Step 3: overlay captions (lyric blocks)
  Step 4: optional explosion / siren SFX overlay if scene calls
  Step 5: render to platform spec

FINAL DELIVERY
  YouTube primary: 1920x1080, 12 Mbps, AAC 256 kbps, -14 LUFS, -1.0 dBFS peak
  Vertical Shorts variant: 1080x1920, 30-sec excerpt of best section, 10 Mbps, AAC 256 kbps
  Archival master: ProRes 422 HQ, PCM 24-bit, no loudness normalization

CRITICAL DIFFERENCES vs personal-finance authority Pipeline A
  - No ducking (vocals in mix vs narration over music)
  - No SFX layer (baked into instrumental)
  - No ambient bed (continuous instrumental)
  - No per-beat level automation (constant music)
  - Caption system S4 vs S3 (block lyrics vs hybrid sentence-paced)
  - Loudness target -14 LUFS vs -16 LUFS (Shorts hotter)
  - Mastering chain minimal vs full personal-finance authority chain
  - Render time: ~10 min total assembly (vs 45+ min for the personal-finance authority pipeline due to motion graphics)
```

This second worked sheet lives in the brick-narrative storytelling channel Channel Profile. Subsequent music video episodes copy it. The two worked examples (personal-finance authority Pipeline A + brick-narrative storytelling Pipeline B) cover the two extremes of Skill 11 application — narration-over-music and full-music-video-with-vocals.

---

## 13. Cross-skill connections

This skill connects to:
- **Skill 03 (Script Writing):** beat structure drives music level automation. Beat 11 close lift is an emotional hand-off the script's "I will see you in the next video" line earns.
- **Skill 06 (Storyboard):** SFX placements are planned in the storyboard, not improvised at assembly time.
- **Skill 08 (Voice/TTS):** VO loudness from voice generators varies; mastering chain (compression + limiter) standardizes.
- **Skill 10 (Captions):** caption rendering happens AFTER audio mix is locked. Whisper alignment uses the final audio, not the unmixed VO.

When a render fails, the agent first checks whether the failure is a mix issue (this skill) or an assembly issue (this skill §9 pipeline) or an upstream skill issue (script timing, voice generation, video generation).

---

## 14. Runtime checklist

Before any final render is shipped:

- [ ] VO mastering chain applied (compression, EQ, de-ess, limit)
- [ ] VO loudness at -16 LUFS (long-form) or -14 LUFS (Shorts)
- [ ] VO peaks under -1.5 dBFS true peak
- [ ] Music ducking method matches Channel Profile
- [ ] Music duck depth matches niche (per §4)
- [ ] Per-beat music level automation applied (per §7)
- [ ] SFX cues per storyboard, all at -18 LUFS or below
- [ ] Ambient bed present at -34 LUFS for full duration
- [ ] No loud music behind VO (anti-pattern 1)
- [ ] No vocal-track music in authority content (anti-pattern 2)
- [ ] Sync verified at 5 anchor timestamps
- [ ] Caption render done AFTER audio mix
- [ ] Platform-specific render to spec
- [ ] Archival master rendered in parallel
- [ ] File integrity verified (no half-rendered frames, no audio dropouts)
- [ ] First 30 seconds listened end-to-end as final QC

If any check fails, fix the failing element. Never publish video with failing audio mix.
