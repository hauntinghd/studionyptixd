# Skill 08 Companion — Voice/TTS Config Bank

This is the runtime companion to Skill 08 (Voice/TTS Prompting). Skill 08 documents generator selection, the Sample Gate rule, and format-aware preparation. This file provides:

1. **Per-archetype config table** — for each V1-V15 voice archetype, the recommended ElevenLabs voice + settings, the Minimax prompt template, the Suno vocal direction
2. **Per-channel locked configs** — 8 worked channel configs the agent can copy-paste
3. **Minimax prompt bank** — 12 full Minimax prompts spanning archetypes
4. **Voice direction notes per archetype** — pacing, breath placement, emphasis patterns
5. **Voice failure diagnostic bank** — 8 common failure modes with fix protocols

The agent loads this when picking voice settings for any channel. Pulling from a bank of locked configs cuts the Sample Gate iteration cycles and keeps voice consistent across uploads.

---

## 1. Why a config bank

Skill 08 names the 15 archetypes and lists the ElevenLabs voices that work for each. But "Adam works for V1, V2, V4, V5, V8" leaves the agent to guess the actual settings (stability, similarity_boost, style) every time. Two videos using the same voice ID with different settings sound different.

A locked config:
- Reduces Sample Gate iterations from 3-5 per channel to 1-2
- Keeps voice consistent across every upload (brand)
- Makes channel handoff easier (config is documented, not implicit)
- Cuts cost: every Sample Gate cycle is ~$0.10-0.30; 3 fewer iterations × 50 videos = $30-50 saved per channel per year

The agent writes the locked config into the Channel Profile (Skill 05) once, then cites this companion only when establishing a new channel or when the user explicitly asks to retune.

---

## 2. Per-archetype voice config table

Format: `Voice ID | Stability | Similarity | Style | Notes`

### V1 — Documentary Authority (long-form mystery-documentary / science-explainer / tech-industrial-history register)

**ElevenLabs:** Adam | 0.60 | 0.85 | 0.05 | Slight British register lift via Daniel as alternative
**Alt male:** Daniel | 0.62 | 0.85 | 0.05 | Slightly British, leans tech-industrial-history register
**Alt female:** Rachel | 0.60 | 0.82 | 0.10 | mystery-documentary-feminine variant
**Minimax prompt:** *"A measured, calm, mid-40s American male voice with slight British vocabulary lift. Slow-deliberate pace, long unfolding sentences, occasional dry register. Calm curiosity is the dominant emotion — never excited, never outraged. Speaks as an equal-intelligent peer; never explains the obvious. Pauses 0.4-0.6 sec at em-dashes. Breath audible but unhurried."*
**Suno vocal:** Not typically used for V1.
**HeyGen avatar:** Not typically — V1 is faceless documentary.

### V2 — Federal Credentialed Expert (personal-finance authority / medical-authority register)

**ElevenLabs:** Bill | 0.62 | 0.88 | 0.08 | Best for "older mentor" weight
**Alt male:** Adam at higher stability (0.65) | similarity 0.85 | style 0.05
**Alt female:** Rachel | 0.62 | 0.85 | 0.10 | medical variant
**Minimax prompt:** *"A 50-year-old American male tax/medical professional voice. Mid-pace with deliberate pauses on critical numbers. Sentence rhythm mixed — long explanatory sentences alternating with short hammer sentences. Authoritative but protectively concerned, like an older mentor speaking to a peer. Pronouncesform numbers, code sections, dollar amounts with crisp clarity. Pauses 0.6-0.8 sec at em-dashes. No hype vocabulary, no slang, no excitement."*
**Suno vocal:** Not used.
**HeyGen avatar:** personal-finance authority channel uses HeyGen with Bill-style ElevenLabs source.

### V3 — War Veteran Storyteller

**ElevenLabs:** Antoni | 0.45 | 0.85 | 0.15 | Lived-in weathered register
**Alt male:** Bill at lower stability (0.50) | similarity 0.88 | style 0.20
**Minimax prompt:** *"A 75-year-old American male voice, weathered, slow, lived-in. Pauses are part of the voice, not bugs. Sentence rhythm short to medium with occasional fragments. Earned gravity, never performed. Plain visceral vocabulary, concrete sensory detail, no abstractions. Breath audible, slow inhalations between sentences. The voice carries the implication that the speaker was there and the listener wasn't."*
**Suno vocal:** Sometimes used for spoken-word intros over orchestral score.

### V4 — Skeptical Investigator (investigative-journalism / geopolitics documentary register)

**ElevenLabs:** Charlie | 0.50 | 0.80 | 0.20 | Younger pursuit register
**Alt male:** Adam at lower stability (0.45) | similarity 0.78 | style 0.25
**Minimax prompt:** *"A 30-something American male investigator voice. Mid-fast pace, building momentum as evidence unfolds. Sentence rhythm mixed — questions alternating with answer sentences. The voice is on the trail of something. Skepticism is the default; outrage when expressed is earned by the evidence. Pace accelerates on revelations, slows on documents. Pauses for emphasis on specific dollar amounts and named villains."*
**Suno vocal:** Not used.

### V5 — Mentor Coach (productivity / personal-finance education register)

**ElevenLabs:** Sam | 0.58 | 0.82 | 0.15 | Warm authority register
**Alt male:** Bill at lower style (0.10) | similarity 0.85 | stability 0.60
**Alt female:** Rachel | 0.58 | 0.82 | 0.15 | warm-mentor female variant
**Minimax prompt:** *"A late-30s educated voice with warm authority. Mid-warm pace, conversational but structured. Often uses three-part rhythms (first... second... third...). Sounds like an older friend who has figured something out. Vocabulary is educated but plain; frameworks are named, concepts labeled. Pauses 0.4 sec between framework parts. Slight self-deprecating warmth in humor moments."*
**HeyGen avatar:** Used for talking-head educational channels.

### V6 — Hype Showman (top viral-challenge / gaming hype register)

**ElevenLabs:** Liam | 0.40 | 0.75 | 0.40 | Fast energetic
**Alt male:** Charlie at higher style (0.45) | similarity 0.75 | stability 0.40
**Minimax prompt:** *"A 25-year-old American male voice, fast-urgent pace, almost no pauses. Energy is the product. Short punchy sentences, exclamations, repetition for emphasis. Sentence length almost never exceeds 15 words. Vocabulary plain and conversational. Sounds like someone who just did something wild. Excited register throughout, never settles. Voice cracks slightly on peak excitement."*
**Suno vocal:** Sometimes for hype intro stings.

### V7 — Deadpan Cynic (deadpan-history register)

**ElevenLabs:** Charlie | 0.65 | 0.82 | 0.0 | Locked deadpan
**Alt male:** Antoni at higher stability (0.70) | similarity 0.85 | style 0.0
**Minimax prompt:** *"A late-20s American male voice, completely deadpan. Mid pace, pauses on punchlines. Sets up sentences in normal register, then undercuts them with one-liners delivered without affect. Knowing, dry, never sincere about anything for too long. Vocabulary plain to slightly elevated — sometimes the vocabulary itself is the joke. The voice has seen everything and finds it slightly absurd."*
**Suno vocal:** Not used.

### V8 — Erudite Professor (economic-explainer / logistics explainer register)

**ElevenLabs:** Daniel | 0.65 | 0.85 | 0.10 | Lecture register
**Alt male:** Adam at higher stability (0.68) | similarity 0.85 | style 0.05
**Minimax prompt:** *"A 40-something educated American or slightly British voice. Slightly fast pace — information density is the product. Sentence rhythm long and clause-heavy, often three or four clauses joined with commas. Confident lecturing register. Defines specialist terms but doesn't apologize for them. Vocabulary specialist; cites studies, named economists, historical context. Slight wry tone at expense of institutions. No hype, no slang."*

### V9 — Best Friend Confidant (lifestyle vlogger / beauty vlogger register)

**ElevenLabs:** Domi | 0.40 | 0.78 | 0.30 | Energetic intimate
**Alt female:** Charlotte at higher style (0.35) | similarity 0.78 | stability 0.42
**Minimax prompt:** *"A 25-year-old American female voice, conversational with frequent tangents, parenthetical asides natural to the cadence. Pace varies, sentences can run on. Casual slang, first-person heavy. Intimate slightly performative — sharing something between friends. Sounds like she's mid-thought when she starts talking. Self-deprecating humor frequent. Voice rises slightly on punchlines."*
**Suno vocal:** Not used.

### V10 — News Anchor (news-explainer outlet / Reuters style)

**ElevenLabs:** Adam | 0.68 | 0.88 | 0.0 | Locked neutral
**Alt male:** Daniel at very high stability (0.72) | similarity 0.88 | style 0.0
**Alt female:** Rachel at higher stability (0.68) | similarity 0.88 | style 0.0
**Minimax prompt:** *"A 40-something professional American newscaster voice. Steady professional pace. Clean sentence structure — subject, verb, object, active voice, inverted pyramid. Neutral emotional register; concern allowed, outrage forbidden. AP-style plain English. No editorializing, no slang, no first-person opinion. Crisp pronunciation of names, places, dates."*

### V11 — Outraged Activist (political YouTubers)

**ElevenLabs:** Bill at lower stability (0.42) | similarity 0.80 | style 0.45 | Building momentum register
**Alt male:** Charlie at higher style (0.50) | similarity 0.78 | stability 0.45
**Minimax prompt:** *"A 35-year-old American voice, building momentum throughout the script. Starts mid-energy, accelerates as outrage builds. Sentence rhythm uses anaphora — repeated openings for emphasis. Plain but impassioned vocabulary, strong action verbs. Righteous outrage register. Co-conspirator audience relationship — 'we' is the dominant pronoun. Voice rises and tightens on revelations of villain action."*

### V12 — Curious Kid Adult (science-engineering personality register)

**ElevenLabs:** Liam at higher stability (0.55) | similarity 0.80 | style 0.20 | Wonder pacing
**Alt male:** Charlie at lower style (0.18) | similarity 0.78 | stability 0.55
**Alt female:** Bella | 0.55 | 0.80 | 0.20 | female variant
**Minimax prompt:** *"A 35-year-old American male/female voice with wonder pacing. Mid-fast pace, often question-led. Sentence rhythm mixed. Plain vocabulary with occasional elevation when explaining specifics. Genuine curiosity that hasn't been worn down. Sounds like a smart curious peer ready to explore. Self-deprecating warmth, frequent. Slight rise in pitch on discoveries."*

### V13 — Reluctant Witness (true crime narrators)

**ElevenLabs:** Charlotte | 0.55 | 0.82 | 0.12 | Weighted careful
**Alt female:** Rachel at lower style (0.10) | similarity 0.82 | stability 0.58
**Minimax prompt:** *"A 45-year-old American female voice, slow, careful, weighted. Pause-heavy delivery — the voice is bearing witness. Sentence rhythm short, fragmentary. Plain specific concrete vocabulary. Heavy mournful when warranted; never sensational, never tabloid. Speaks as if telling someone who needs to know. Specific places, dates, names, weighted with appropriate gravity."*

### V14 — Drill Rapper Narrator (brick-narrative storytelling music videos)

**ElevenLabs:** Not used.
**Suno vocal:** Primary tool. Suno prompt: *"A 22-year-old male voice, slight regional inflection, beat-locked at 130-145 BPM, lyrical bars in four-line stanzas. Aggressive defiant register, performatively threatening. Slang vocabulary, regional indicators, escalating boasts. Hook returns, ad-libs at end of bars."*
**Minimax prompt:** Not used (Suno handles vocal track).

### V15 — Wise Elder (Faith/Christian, meditation)

**ElevenLabs:** Antoni | 0.65 | 0.85 | 0.10 | Reverent register
**Alt male:** Bill at higher style (0.15) | similarity 0.85 | stability 0.62
**Minimax prompt:** *"A 65-year-old American male voice, slow and reverent. Long unfolding sentences with biblical/ancient cadence influence. Elevated but accessible vocabulary, old-soul register. Reverent contemplative emotional tone. Speaks as elder to seeker. Pauses 0.6-0.8 sec at section breaks. Soft breath between phrases. No slang, no hype, no modern cynicism."*

---

## 3. Per-channel locked configs

Eight channel configs the agent can copy directly. Each has been validated by the system.

### Channel 1 — personal-finance authority channel (IRS / senior finance)

```
Generator: HeyGen avatar (visual) + ElevenLabs voice (audio source)
Voice archetype: V2 Federal Credentialed Expert
ElevenLabs source: Bill
Settings: stability=0.62, similarity_boost=0.88, style=0.08, speaker_boost=ON
Format: Minimax-style script (numbers as words, no symbols)
Pacing: ~145 wpm
Duration target: 25-27 minutes for full 25,000-char script
Sample-Gate frequency: per new format only (re-sample if structural variant changes)
```

### Channel 2 — medical-authority channel (senior health)

```
Generator: ElevenLabs (faceless) OR HeyGen avatar (clinic-doctor brand)
Voice archetype: V2 medical variant
ElevenLabs voice: Bill (male doctor) OR Rachel (female practitioner)
Settings: stability=0.62, similarity_boost=0.85, style=0.10, speaker_boost=ON
Format: Minimax-style script
Pacing: ~140 wpm — slightly slower than IRS for medical specificity
Duration target: 22-27 min
```

### Channel 3 — Roblox-scenario channel (vertical shorts)

```
Generator: Minimax custom voice
Voice archetype: V6 vertical variant ("chaotic Gen Z shitposter")
Minimax prompt: see §4 below for full prompt
Format: standard with exclamations, fragments, slang allowed
Pacing: ~190 wpm — fast urgent
Duration target: 60-90 sec
```

### Channel 4 — brick-narrative storytelling channel (drill music videos)

```
Generator: Suno (full vocal + instrumental)
Voice archetype: V14 Drill Rapper Narrator
Suno prompt: see §4 below
BPM: 136 (locked for the channel)
Format: lyrical, bar-locked
Duration target: 3:00-3:30 track
```

### Channel 5 — Documentary explainer (long-form mystery-documentary style hypothetical)

```
Generator: ElevenLabs
Voice archetype: V1 Documentary Authority
ElevenLabs voice: Adam OR Daniel
Settings: stability=0.62, similarity_boost=0.85, style=0.05, speaker_boost=ON
Format: ElevenLabs (digits ok, em-dashes ok)
Pacing: ~135 wpm — slow deliberate
Duration target: 12-25 min
```

### Channel 6 — Tech / AI dev tools (hypothetical fast-cut tech explainer)

```
Generator: ElevenLabs
Voice archetype: V8 Erudite Professor
ElevenLabs voice: Daniel
Settings: stability=0.65, similarity_boost=0.85, style=0.10, speaker_boost=ON
Format: ElevenLabs
Pacing: ~165 wpm — info density
Duration target: 8-15 min
```

### Channel 7 — News-hijack documentary (investigative-journalism-adjacent)

```
Generator: ElevenLabs
Voice archetype: V4 Skeptical Investigator
ElevenLabs voice: Charlie
Settings: stability=0.50, similarity_boost=0.80, style=0.20, speaker_boost=ON
Format: ElevenLabs
Pacing: ~150 wpm — accelerating
Duration target: 15-25 min
```

### Channel 8 — True crime

```
Generator: ElevenLabs
Voice archetype: V13 Reluctant Witness
ElevenLabs voice: Charlotte
Settings: stability=0.55, similarity_boost=0.82, style=0.12, speaker_boost=ON
Format: ElevenLabs
Pacing: ~130 wpm — slow weighted
Duration target: 10-20 min
```

---

## 4. Minimax prompt bank

Twelve full Minimax prompts spanning archetypes. Each is in production use or production-ready.

### Prompt 1 — V2 Federal Credentialed Expert (personal-finance authority channel)

> "A 50-year-old American male tax accountant voice. Mid-pace with deliberate pauses on critical numbers, dollar amounts, and form references. Sentence rhythm mixed — long explanatory sentences alternate with short hammer sentences. Authoritative but protectively concerned, like an older mentor speaking to a peer who needs to know something urgent. Pronounces form numbers, code sections, dollar amounts with crisp clarity. Pauses 0.6-0.8 seconds at em-dashes. Pauses 0.4 seconds between sentences. No hype vocabulary, no slang, no excitement. Slight gravelly weight on numbers. Audible breath between paragraphs."

### Prompt 2 — V2 medical variant (medical-authority channel doctor)

> "A 50-year-old American female physician voice. Mid pace with deliberate pauses on diagnoses, dosages, and patient names. Sentence rhythm mixed — long explanatory sentences alternating with short hammer sentences when delivering critical advice. Warm authority — speaks as someone who has cared for patients for decades. Pronounces medical terms with crisp clarity but defines them in passing. Pauses 0.5-0.7 seconds at section transitions. No alarmism, no fear-mongering, but clear concern when warranted. Audible breath between case discussions."

### Prompt 3 — V6 vertical / Roblox-scenario channel

> "A 19-year-old American male Gen Z voice, slightly nasal, very fast pace with frequent jumps in volume and energy. Uses uptalk on emphasis words. Has chaotic shitposter energy — the kind of voice that says 'BRO' and 'fr fr' naturally. Sounds like someone who has been online too long and finds everything simultaneously hilarious and devastating. Conversational and intimate, like he's whispering scandal to his best friend. Vocabulary is slang-heavy modern internet vernacular. Pacing has occasional sudden pauses for comedic timing. Voice carries hint of fake-academic register when explaining absurd things — like he's narrating a documentary about something stupid."

### Prompt 4 — V1 Documentary Authority

> "A measured calm 40-something American male voice with slight British vocabulary lift. Slow-deliberate pace, long unfolding sentences with embedded clauses. Calm curiosity is the dominant emotion — never excited, never outraged. Wonder is the closest emotion. Speaks as an equal-intelligent peer; never explains the obvious, never talks down. Pauses 0.4-0.6 sec at em-dashes. Breath audible but unhurried. Slight dry humor occasionally surfaces but is never the goal. Citations of studies, named experts, primary sources delivered without emphasis — the citations are the substance."

### Prompt 5 — V3 War Veteran Storyteller

> "A 75-year-old American male voice, weathered, slow, lived-in. Pauses are part of the voice, not flaws — pauses can run 1.0-1.5 seconds and feel right. Sentence rhythm short to medium with deliberate fragments. Earned heaviness, never performed. Plain visceral vocabulary, concrete sensory detail, no abstractions. Breath audible, slow inhalations between sentences. The voice carries the implication that the speaker was there and the listener wasn't. Specific places, dates, names, weapons spoken with the weight of memory."

### Prompt 6 — V4 Skeptical Investigator

> "A 30-something American male investigator voice. Mid-fast pace, building momentum as evidence unfolds. Sentence rhythm mixed — questions alternating with answer sentences. The voice is on the trail of something specific. Skepticism is the default emotional register; outrage when expressed is earned by the evidence. Pace accelerates on revelations, slows when reading from documents. Pauses 0.3-0.5 seconds for emphasis on specific dollar amounts and named villains. Slight dry humor when absurdity warrants. Sounds like a real journalist, not a TV reporter."

### Prompt 7 — V5 Mentor Coach

> "A late-30s educated voice — male or female — with warm authority. Mid-warm pace, conversational but structured. Often uses three-part rhythms (first... second... third...) and the voice naturally accents these. Sounds like an older friend who has figured something out and wants to share. Vocabulary educated but plain; frameworks are named, concepts labeled. Pauses 0.4 sec between framework parts. Slight self-deprecating warmth. Books, named thinkers, personal experiments referenced naturally."

### Prompt 8 — V7 Deadpan Cynic (deadpan-history style)

> "A late-20s American male voice, completely deadpan — almost flat. Mid pace, pauses 0.5-0.8 seconds before punchlines. Sets up sentences in normal register, then undercuts them with one-liners delivered without affect. Knowing, dry, never sincere about anything for too long. Vocabulary plain to slightly elevated — sometimes the vocabulary itself is the joke (using a fancy word for an absurd thing). The voice has seen everything and finds it slightly absurd. No vocal fry, no inflection on emotional moments — the joke is the lack of inflection."

### Prompt 9 — V8 Erudite Professor

> "A 40-something educated American voice with very slight British register. Slightly fast pace — information density is the product. Sentence rhythm long and clause-heavy, often three or four clauses joined with commas. Confident lecturing register. Defines specialist terms but doesn't apologize for them. Vocabulary specialist; cites economists, scientists, engineers, historical context. Slight wry tone at expense of institutions. Pauses 0.3-0.4 sec between clauses. No hype, no slang. The voice sounds like someone who would lecture comfortably for two hours."

### Prompt 10 — V11 Outraged Activist

> "A 35-year-old American male voice, building momentum throughout the script. Starts mid-energy, accelerates as outrage builds, peaks at the third quarter then settles into call-to-action register. Sentence rhythm uses anaphora — repeated openings for emphasis ('They denied her. They cc'd her doctor. They put him on hold for two hours.'). Plain but impassioned vocabulary, strong action verbs. Righteous outrage emotional register. Co-conspirator audience relationship — 'we' is dominant. Voice rises and tightens on villain reveals."

### Prompt 11 — V13 Reluctant Witness

> "A 45-year-old American female voice, slow, careful, weighted. Pause-heavy delivery — pauses can run 0.8-1.2 seconds and feel right. The voice is bearing witness. Sentence rhythm short, sometimes fragmentary. Plain specific concrete vocabulary. Heavy mournful when warranted; never sensational, never tabloid. Speaks as if telling someone who needs to know. Specific places, dates, names delivered with appropriate gravity. Slight tremor possible at peak emotional moments but never full break."

### Prompt 12 — V15 Wise Elder

> "A 65-year-old American male voice, slow and reverent. Long unfolding sentences with biblical/ancient cadence influence — King James-adjacent without being archaic. Elevated but accessible vocabulary, old-soul register. Reverent contemplative emotional tone. Speaks as elder to seeker. Pauses 0.6-0.8 seconds at section breaks. Soft audible breath between phrases. No slang, no hype, no modern cynicism. Slight smile heard occasionally on parables. References to sacred texts, traditional wisdom, named saints/sages delivered with quiet weight."

---

## 5. Voice direction notes per archetype

Beyond settings, each archetype has subtle delivery patterns the generator needs prompted to honor. These are the difference between a "correct" output and a "right" output.

### V1 Documentary Authority — direction notes

- Pause 0.4-0.6 sec at every em-dash
- Soften "the" before specialist terms (signals respect for the term)
- Slight rise on "But here's what's interesting..." transitions
- Breath audible at paragraph breaks
- No vocal smile (V1 is not warm; it's curious)

### V2 Federal Credentialed Expert — direction notes

- Hammer-sentence delivery: short sentences spoken at 80% of paragraph pace, with 0.3 sec gap before
- Dollar amounts: spoken with crisp consonant attack, slight emphasis on the specific digit
- Form numbers: spoken as discrete words with 0.1 sec gap between digits
- Patient names: spoken slightly softer than surrounding text (signals respect)
- No vocal smile

### V3 War Veteran — direction notes

- Pauses up to 1.5 sec between sentences are correct, not bugs
- Specific dates pronounced slowly, fully (not "Sept 23rd '68" but "September 23rd, 1968")
- Names of fallen comrades pronounced with fractional pause before
- Breath audible, not edited out
- Slight gravel on emotional words

### V6 Hype Showman — direction notes

- Voice cracks slightly on peak excitement (don't smooth out)
- "BRO" and "WAIT" spoken at +30% volume of surrounding
- Repeats spoken with escalating energy ("CRAZY. CRAZIER. INSANE.")
- No pauses over 0.3 sec — energy is the product
- Audible breath between sentences (not edited out)

### V7 Deadpan Cynic — direction notes

- Punchlines delivered with NO emotional shift — flat delivery is the joke
- Setup sentences spoken at 100% pace; punchlines at 95% pace (slight slowing)
- Pauses 0.5-0.8 sec before punchlines
- Never use exclamation point delivery; replace with deadpan
- No vocal smile (irony is undermined by smile)

### V13 Reluctant Witness — direction notes

- Pauses 0.8-1.2 sec at heavy moments (specific names, dates, descriptions of harm)
- Names of victims spoken with breath before
- Investigator names spoken matter-of-fact
- Slight tremor on peak emotional moments — but never full break
- Breath fully audible

### V14 Drill Rapper — direction notes

- Beat-locked: every bar must hit the downbeat
- Hook returns delivered with slightly more aggression each repeat
- Ad-libs at end of bars (single word or sound)
- Slight regional inflection consistent throughout track
- No "studio singing" — the voice should sound like a real performance

### V15 Wise Elder — direction notes

- Pauses 0.6-0.8 sec at section breaks
- Sacred references spoken with breath before
- Slight smile heard on parables (warm wisdom, not solemn)
- No hardness, no anger, no excitement
- Slow inhalation audible between long sentences

---

## 6. Voice failure diagnostic bank

Eight common voice failures with diagnostic protocols.

### Failure 1 — Output sounds robotic / TTS-y

**Likely cause:** style=0 with stability too high (>0.70). Voice has no expression.
**Fix:** lower stability to 0.55-0.60, raise style to 0.08-0.15, re-sample.

### Failure 2 — Output sounds dramatic / over-acting

**Likely cause:** stability too low (<0.40) with style too high (>0.40). Every sentence sounds like a movie trailer.
**Fix:** raise stability to 0.55-0.60, lower style to 0.10-0.20, re-sample.

### Failure 3 — Mispronunciation of specific terms (form numbers, drug names)

**Likely cause:** ElevenLabs phoneme inference fails on edge cases.
**Fix:** Spell-out replace in script ("Form S-S-A six-three-four" not "Form SSA634"). For Minimax format, write the spell-out version.

### Failure 4 — Voice drifts mid-script (different in beat 7 than beat 1)

**Likely cause:** Speaker_boost OFF, OR script too long for one generation pass.
**Fix:** Speaker_boost ON. For long scripts (>15 min), split into 5-7 segments and generate each separately, then concat.

### Failure 5 — Pacing too fast (script is right length but feels rushed)

**Likely cause:** Voice's natural pace exceeds the channel target. Settings can't fix pace.
**Fix:** Switch voice. V2 channels run at ~145 wpm; if Adam at 0.62 stability hits 165 wpm, swap to Bill (slower natural pace).

### Failure 6 — Voice inconsistent across uploads (different sound week to week)

**Likely cause:** Settings drifting. The agent is re-generating settings each time instead of citing the locked Channel Profile.
**Fix:** Lock settings in Channel Profile. Cite from profile, never re-derive. This companion's §3 is the authoritative source for the 8 channels.

### Failure 7 — Minimax prompt produces a voice that "sounds correct" but feels wrong

**Likely cause:** Prompt is too vague OR contradicts itself ("calm but urgent").
**Fix:** Specify ONE dominant emotional register. Replace contradictions with sequencing ("calm setup, urgent reveal"). Re-sample.

### Failure 8 — Suno track has wrong vocal energy

**Likely cause:** Suno style tags wrong, OR genre tag wrong.
**Fix:** Verify Suno genre tag matches the track (drill, trap, orchestral, ambient). Verify vocal style tags ("aggressive male", "smooth female", etc.). Re-roll with corrected tags. Suno re-rolls are cheap (~$0.05).

---

## 7. The Sample Gate at runtime — what to test

When the agent runs the Sample Gate (Skill 08 §6), the test should specifically validate:

1. **First 30 seconds of script** — the hook beat. If the hook sounds wrong, every later beat will too.
2. **A short hammer sentence** — confirms the punctuation pacing.
3. **A long unfolding sentence** — confirms the breath and clause delivery.
4. **A specific dollar amount or form number** — confirms numerical pronunciation.
5. **A patient/victim name** — confirms appropriate softening and weight.

If all 5 pass, lock the config and run full generation. If any fail, adjust per §6 and re-sample (cost: $0.10-0.30).

Never generate the full 25-minute script before passing the Sample Gate. A failed full generation costs $5-12; a failed sample costs cents.

---

## 8. Runtime workflow

When voicing any new channel:

1. **Identify voice archetype** from Skill 03 voice DNA.
2. **Pull config from §2** for that archetype.
3. **If channel is one of the 8 in §3**, use that channel's locked config directly.
4. **If new channel**, copy the §2 archetype config as starting point.
5. **Run Sample Gate** on first 30 seconds + hammer + long sentence + dollar + name.
6. **Tune via §6 diagnostic bank** if any sample fails.
7. **Lock config in Channel Profile** (Skill 05) once Sample Gate passes.
8. **Generate full script** using locked config.
9. **Quality check** the output against §5 direction notes for the archetype.
10. **Surface to user** with citation showing which config was used.

The agent never re-derives voice settings. It cites from this companion or the Channel Profile, runs Sample Gate, and locks. Voice consistency across uploads is brand-load-bearing.
