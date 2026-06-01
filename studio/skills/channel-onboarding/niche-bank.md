# Channel Creation Niche Bank — 22-niche register quick-reference

The channel-creation protocol identifies the production archetype (one of 12). Then it identifies the niche (one of 22). The niche register tells the protocol what voice + visual + length + thumbnail + music + compliance settings the niche conventionally uses.

This file is the lookup table. When the agent locks the niche, it pulls the row from this file and pre-fills CHANNEL.md with niche-conventional settings. Most fields are still user-overridable — but the defaults are good.

## How to use

1. Identify niche from channel description (or via Pass-1 disambiguating question if unclear).
2. Look up the row in this file.
3. Pre-fill CHANNEL.md with the row's settings.
4. The user only sees the questions where the niche row says "user picks" — everything else is defaulted from the niche.

This is what makes the new skill smarter than the old one. Old skill asked 8 questions for every channel. New skill asks 2-4 because the niche row pre-filled the rest.

---

## The 22 niches × register dimensions

For each niche: niche id, archetype default, voice archetype, visual register, length default, thumbnail pattern, music density, compliance domain, special notes.

---

### 1. `senior_finance_irs`
- **Archetype default:** `avatar_authority_longform`
- **Voice archetype:** V2 Federal Credentialed Expert
- **Visual register:** warm domestic interiors + cool institutional exteriors, ~85% saturation
- **Length:** 25 min
- **Thumbnail pattern:** P2 Rage Stamp (red anchor, dollar specificity)
- **Title formulas:** F2 (specific dollar) + F11 (parenthetical inclusion); F6 (threat alert) + F12 (ALL CAPS)
- **Music density:** light bed, sidechain ducked
- **Compliance:** Domain 4 (financial advice) + Domain 5 (institutional naming) — full preflight
- **Reference channel type:** a personal-finance authority channel (canonical)
- **Hard rules:** No years-of-experience claim; composite labeling on every patient story; "consult a tax professional" recommendation present
- **User picks:** avatar (3-headshot picker), voice, lead magnet URL, affiliate

---

### 2. `senior_health_medical`
- **Archetype default:** `avatar_authority_longform` OR mixed real-footage (medical-authority style)
- **Voice archetype:** V2 medical variant (warm authority)
- **Visual register:** clean clinical bright + warm patient kitchens
- **Length:** 22-27 min
- **Thumbnail pattern:** P11 Photoreal Composite (closed-mouth concern)
- **Title formulas:** F4 (numbered) + F9 (cited authority); F10 (wound-first)
- **Music density:** light hopeful bed
- **Compliance:** Domain 3 (medical) — full preflight, mandatory disclaimer
- **Reference channel type:** a medical-authority channel
- **Hard rules:** "Reverse / cure / treat" require peer-reviewed citation; consult-physician language mandatory; supplement claims especially scrutinized
- **User picks:** avatar, voice, lead magnet, supplement affiliate (if any)

---

### 3. `tech_ai_dev_tools`
- **Archetype default:** `documentary_voiceover` OR `talking_head_real_footage`
- **Voice archetype:** V1 Documentary Authority OR V8 Erudite Professor
- **Visual register:** modern minimal, high saturation on UI/screen elements
- **Length:** 8-15 min
- **Thumbnail pattern:** P6 Authority Portrait or P10 Color-Pop Mono
- **Title formulas:** F1 (em-dash) + F8 (question); F11 (bracketed REVIEW)
- **Music density:** modern electronic, light percussion
- **Compliance:** Domain 2 (sponsor disclosure) — moderate preflight
- **Reference channel type:** developer-focused real-footage channels; AI-host avatar dev channels
- **Hard rules:** Comparative claims need reproducible test; sponsor disclosure mandatory; product-name accuracy matters
- **User picks:** real footage vs avatar, voice register, primary tool/stack focus

---

### 4. `vertical_shorts_gaming` (Roblox / TikTok-style)
- **Archetype default:** `vertical_shorts_hype`
- **Voice archetype:** V6 Hype Showman vertical variant ("chaotic Gen Z shitposter")
- **Visual register:** vibrant saturated, gaming-stylized
- **Length:** 60-90 sec
- **Thumbnail pattern:** P5 Reaction Face or P1 Face + Number
- **Title formulas:** F8 (POV / what-if) compressed for vertical
- **Music density:** high-energy synth or beat-locked
- **Compliance:** Domain 1 (Made-for-Kids designation risk) — calibrate Made-for-Kids: NO
- **Reference channel type:** a Roblox-scenario channel
- **Hard rules:** 9:16 aspect ratio mandatory; signature voice opener (channel-specific); fast pace (~190 wpm)
- **User picks:** voice prompt, signature opener, music register

---

### 5. `music_video_drill`
- **Archetype default:** `music_video_propaganda`
- **Voice archetype:** V14 Drill Rapper Narrator
- **Visual register:** cinematic warm-cool gradient, hyperreal saturation, slight film grain
- **Length:** 3-3:30 min
- **Thumbnail pattern:** P12 AI-Generated Surreal
- **Title formulas:** F9 (quote attribution) + F12 (ALL CAPS) + news-hijack
- **Music density:** Suno IS the audio (vocals + instrumental together)
- **Compliance:** Domain 1 (post-July-2025 inauthentic content) + Domain 5 (satire defamation) — calibrated preflight
- **Reference channel type:** a brick-narrative storytelling channel (canonical)
- **Hard rules:** News-pegged within 24-48hr; channel locked at one BPM (typically 136); satire framing mandatory in About + title
- **User picks:** genre (drill / trap / orchestral / parody), BPM, visual aesthetic (brick-narrative / claymation / photoreal stylized)

---

### 6. `history_explainer`
- **Archetype default:** `documentary_voiceover` OR `stickman_explainer`
- **Voice archetype:** V1 Documentary Authority (long-form mystery-documentary feel) or V7 Deadpan Cynic (deadpan-history feel)
- **Visual register:** dark moody cinematic OR illustrated whiteboard
- **Length:** 10-25 min
- **Thumbnail pattern:** P4 Mystery Object OR P11 Photoreal Composite
- **Title formulas:** F1 (specific date + impossible event); F8 (paradox premise)
- **Music density:** cinematic ambient pad, sparse
- **Compliance:** Domain 5 (defamation when discussing real figures) — moderate preflight
- **Reference channel types:** a long-form mystery-documentary channel, a deadpan-history channel, a tech-industrial-history channel
- **Hard rules:** Historical claims need primary source on file; secondary sources are weak; specific date + name accuracy matters
- **User picks:** narrator voice, register (long-form mystery-documentary calm / deadpan-history dry), length

---

### 7. `news_hijack_documentary`
- **Archetype default:** `news_hijack_investigation`
- **Voice archetype:** V4 Skeptical Investigator
- **Visual register:** cool blue forensic + warm document accents, slight cinematic grain
- **Length:** 15-25 min
- **Thumbnail pattern:** P11 Photoreal Composite OR P2 Rage Stamp
- **Title formulas:** F5 (named villain) + specific dollar; F1 (suppression frame)
- **Music density:** tense investigative bed
- **Compliance:** Domain 5 (defamation) — full preflight, evidence package mandatory
- **Reference channel types:** an investigative-journalism channel, a geopolitics documentary channel
- **Hard rules:** Every named subject's accusations have primary source on file; "alleges" / "according to" framing mandatory; subject's response acknowledged or noted as sought
- **User picks:** investigation register, voice, presenter cutaway preference

---

### 8. `true_crime`
- **Archetype default:** `cinematic_ai_documentary` OR `documentary_voiceover`
- **Voice archetype:** V13 Reluctant Witness
- **Visual register:** cool desaturated, heavy cinematic, single warm key
- **Length:** 10-30 min
- **Thumbnail pattern:** P4 Mystery Object OR P11 Photoreal Composite
- **Title formulas:** F7 (forensic story-led) + named victim + date
- **Music density:** sparse minor-key piano + occasional cello
- **Compliance:** Domain 5 (defamation) + brand risk (sensitivity to families) — full preflight
- **Reference channel types:** true-crime narrative channels, character-driven true-crime channels
- **Hard rules:** Verified public record only; no speculation about uncharged persons; "Out of respect for families" disclaimer; never sensational
- **User picks:** narrator voice, length target, regional focus (if any)

---

### 9. `cooking_food`
- **Archetype default:** `talking_head_real_footage` OR `stickman_explainer`
- **Voice archetype:** V12 Curious Kid Adult OR V8 Erudite Professor
- **Visual register:** warm food tones, bright clean kitchen
- **Length:** 5-10 min
- **Thumbnail pattern:** P3 Before/After OR P5 Reaction Face
- **Title formulas:** F3 (counter-narrative against chef wisdom); F8 (cooking question)
- **Music density:** upbeat acoustic / lo-fi
- **Compliance:** Domain 2 (sponsor disclosure) — light preflight
- **Reference channel types:** popular technique-focused cooking channels
- **Hard rules:** Specific temperatures + measurements always emphasized; technique accuracy matters
- **User picks:** real footage vs animated, voice, music register

---

### 10. `real_estate_home`
- **Archetype default:** `avatar_authority_longform` OR `documentary_voiceover`
- **Voice archetype:** V5 Mentor Coach OR V8 Erudite Professor
- **Visual register:** warm interior + green money accents
- **Length:** 10-15 min
- **Thumbnail pattern:** P3 Before/After OR P11 Photoreal Composite
- **Title formulas:** F2 (specific dollar) + F11 (parenthetical); F3 (counter-narrative)
- **Music density:** modern upbeat
- **Compliance:** Domain 4 (financial advice) — moderate preflight
- **Reference channel types:** personal-finance / real-estate personality channels
- **Hard rules:** "Educational not financial advice" disclaimer mandatory; specific city + dollar specificity matters
- **User picks:** avatar yes/no, voice, market focus (regional)

---

### 11. `beauty_fashion`
- **Archetype default:** `talking_head_real_footage`
- **Voice archetype:** V9 Best Friend Confidant
- **Visual register:** bright lit-from-front, vibrant background
- **Length:** 5-10 min
- **Thumbnail pattern:** P5 Reaction Face
- **Title formulas:** F1 (compressed branded comparison)
- **Music density:** light upbeat
- **Compliance:** Domain 2 (affiliate disclosure) — light preflight
- **Reference channel type:** various beauty influencer channels
- **Hard rules:** Affiliate disclosure mandatory; product claims need substantiation
- **User picks:** real footage required, music yes/no, music register

---

### 12. `vlog_lifestyle`
- **Archetype default:** `talking_head_real_footage`
- **Voice archetype:** V9 Best Friend Confidant
- **Visual register:** warm casual, intimate lighting
- **Length:** 5-10 min
- **Thumbnail pattern:** P5 Reaction Face OR P1 Face + Arrow
- **Title formulas:** F1 first-person stake; F2 dollar (when applicable)
- **Music density:** light, upbeat or chill
- **Compliance:** Domain 2 (sponsor disclosure if applicable) — light preflight
- **Reference channel type:** various lifestyle vlog channels
- **Hard rules:** First-person voice, casual register; mid-thought openings work
- **User picks:** voice register, music, sponsor framework

---

### 13. `crypto_finance_trading`
- **Archetype default:** `avatar_authority_longform` OR `documentary_voiceover`
- **Voice archetype:** V11 Outraged Activist OR V6 Hype Showman
- **Visual register:** dark technical + bright data callouts
- **Length:** 8-15 min
- **Thumbnail pattern:** P2 Rage Stamp OR P11 Photoreal Composite
- **Title formulas:** F6 (threat alert) + F12 (ALL CAPS)
- **Music density:** tense investigative or hype synth
- **Compliance:** Domain 4 (financial advice) — full preflight, "not investment advice" mandatory
- **Reference channel types:** investigative-journalism channels (skeptic side), various crypto channels
- **Hard rules:** "Not financial advice" disclaimer; specific price predictions framed as opinion; FTC affiliate disclosure
- **User picks:** voice register, market focus (crypto / stocks / macro), avatar yes/no

---

### 14. `how_to_education`
- **Archetype default:** `stickman_explainer` OR `documentary_voiceover`
- **Voice archetype:** V5 Mentor Coach
- **Visual register:** clean modern, warm authoritative
- **Length:** 8-15 min
- **Thumbnail pattern:** P6 Authority Portrait OR P10 Color-Pop Mono
- **Title formulas:** F13 (How To); F1 (em-dash + specific time/effort)
- **Music density:** light, optimistic
- **Compliance:** Domain 1 (light) — calibrated by topic
- **Reference channel type:** a productivity creator
- **Hard rules:** Specific frameworks named; specific actions in numbered lists
- **User picks:** voice, length, primary skill focus

---

### 15. `fitness_bodybuilding`
- **Archetype default:** `talking_head_real_footage` OR `stickman_explainer`
- **Voice archetype:** V5 Mentor Coach OR V6 Hype Showman
- **Visual register:** bright gym + form demonstration
- **Length:** 5-12 min
- **Thumbnail pattern:** P3 Before/After OR P5 Reaction Face
- **Title formulas:** F10 (wound-first) + F4 (numbered)
- **Music density:** energetic synth or lifting-hype
- **Compliance:** Domain 3 (light medical for fitness claims) — moderate preflight
- **Reference channel type:** various fitness YouTube channels
- **Hard rules:** Specific exercise descriptions; "consult doctor before starting" recommended
- **User picks:** real footage required (typically), voice, music

---

### 16. `faith_christian`
- **Archetype default:** `documentary_voiceover` OR `picture_education`
- **Voice archetype:** V15 Wise Elder
- **Visual register:** cream / muted gold, reverent
- **Length:** 8-20 min
- **Thumbnail pattern:** P6 Authority Portrait OR P4 Mystery Object
- **Title formulas:** F1 (parable framing); F8 (subverted scripture)
- **Music density:** slow piano + soft strings
- **Compliance:** Brand risk (audience trust) — light preflight
- **Reference channel type:** various faith-based channels
- **Hard rules:** Reverent register; ancient-modern bridge framing; named saints / scripture cited
- **User picks:** narrator voice, register (more reflective vs more devotional)

---

### 17. `politics_commentary`
- **Archetype default:** `documentary_voiceover` OR `news_hijack_investigation`
- **Voice archetype:** V11 Outraged Activist OR V4 Skeptical Investigator
- **Visual register:** cool forensic + red emphasis
- **Length:** 10-20 min
- **Thumbnail pattern:** P11 Photoreal Composite OR P2 Rage Stamp
- **Title formulas:** F5 (named villain) + F12 (ALL CAPS)
- **Music density:** tense investigative
- **Compliance:** Domain 1 (election misinfo) + Domain 5 (defamation) — full preflight, named-figures with citation
- **Reference channel type:** various political YouTube channels
- **Hard rules:** "According to [source]" framing for every claim about politicians; "alleges" framing on accusations
- **User picks:** voice register, ideological frame (left / right / centrist), length

---

### 18. `travel`
- **Archetype default:** `talking_head_real_footage` OR `documentary_voiceover`
- **Voice archetype:** V12 Curious Kid Adult OR V9 Best Friend Confidant
- **Visual register:** vibrant saturated, location-specific
- **Length:** 5-15 min
- **Thumbnail pattern:** P11 Photoreal Composite (location hero)
- **Title formulas:** F1 first-person + F2 dollar (budget travel) or F8 (extreme location)
- **Music density:** location-appropriate
- **Compliance:** Light preflight
- **Reference channel type:** various travel YouTube channels
- **Hard rules:** Specific dollar amounts for budget claims; location specificity
- **User picks:** real footage vs AI-supplemented, voice, length

---

### 19. `reaction`
- **Archetype default:** `compilation_supercut` OR `talking_head_real_footage`
- **Voice archetype:** V9 Best Friend Confidant OR V6 Hype Showman
- **Visual register:** depends on host setup
- **Length:** 5-15 min
- **Thumbnail pattern:** P5 Reaction Face
- **Title formulas:** F1 expert-reacts + named target
- **Music density:** light or none
- **Compliance:** Domain 1 (copyright on reaction footage) — moderate preflight
- **Reference channel type:** various expert-reaction channels
- **Hard rules:** Fair-use justification for source clips; expert-credentialed when claim is "expert reacts"
- **User picks:** source: stock library / research-mined / user clips, narration glue yes/no

---

### 20. `documentary_longform`
- **Archetype default:** `documentary_voiceover`
- **Voice archetype:** V8 Erudite Professor
- **Visual register:** restrained mono + accent
- **Length:** 15-30 min
- **Thumbnail pattern:** P10 Color-Pop Mono OR P6 Authority Portrait
- **Title formulas:** F1 (industry-history); F8 (counterintuitive economic claim)
- **Music density:** sparse cinematic
- **Compliance:** Domain 5 (defamation when discussing companies / industries) — moderate preflight
- **Reference channel types:** a tech-industrial-history channel, a logistics explainer channel, an engineering explainer channel
- **Hard rules:** Industry claims need cited research; specific company decisions need source
- **User picks:** narrator voice, length target, industry focus

---

### 21. `ambient_sleep_focus`
- **Archetype default:** `ambient_loop`
- **Voice archetype:** None typically (or V15 Wise Elder for narrated openings)
- **Visual register:** calm aesthetic, location-specific
- **Length:** 1-3 hours
- **Thumbnail pattern:** P10 Color-Pop OR scenic still
- **Title formulas:** F1 (X hours of [aesthetic]) + use case
- **Music density:** Suno generated, looped to length
- **Compliance:** Light preflight
- **Reference channel type:** various lofi / ambient channels
- **Hard rules:** Loop seam invisible; consistent ambient register
- **User picks:** music register (orchestral / lofi / piano / nature), visual loop, target duration

---

### 22. `gaming_longform`
- **Archetype default:** `gaming_animation` OR `talking_head_real_footage`
- **Voice archetype:** V6 Hype Showman
- **Visual register:** game-stylized OR real gameplay capture
- **Length:** 8-15 min
- **Thumbnail pattern:** P5 Reaction Face OR P7 Stacked Number Pile
- **Title formulas:** F2 specific dollar / kill count / score; F4 numbered
- **Music density:** epic battle / gaming hype
- **Compliance:** Domain 1 (Made-for-Kids designation, IP concerns) — moderate preflight
- **Reference channel type:** various gaming channels
- **Hard rules:** Made-for-Kids: NO unless explicitly kid-focused; IP fair-use justification
- **User picks:** animated vs real footage, voice register, game focus

---

## Cross-niche routing notes

When the user's description names a niche that doesn't map cleanly to one of the 22 above, route to the closest match and add a CHANNEL.md note that the niche is non-canonical. The default settings still apply but the agent should expect more user customization.

Examples of non-canonical niches and their closest match:
- "Christian finance" → `senior_finance_irs` defaults + `faith_christian` voice register
- "Crypto for retirees" → `senior_finance_irs` archetype + `crypto_finance_trading` topic angle
- "Health for athletes" → `senior_health_medical` archetype + `fitness_bodybuilding` register
- "Tech for seniors" → `tech_ai_dev_tools` topic + `senior_finance_irs` audience defaults

Hybrid niches keep the archetype's pipeline but adopt the voice / visual register from the topic.

---

## When the niche is genuinely unknown

If the description doesn't map to any of the 22 niches AND the description provides no register hint, fall back to the protocol's general path:
1. Ask one Pass-1 disambiguating question with sample channels
2. Default audience to "general curious"
3. Default length based on visual format
4. Default voice to V5 Mentor Coach (most universal)
5. Default thumbnail to `minimal_clean`

But this should be rare. Most user descriptions clearly map to one of the 22 niches.

---

## Niche-mining the channel description

To classify the niche, scan for these keyword clusters:

| Keywords | Niche |
|---|---|
| tax / IRS / retirement / Social Security | `senior_finance_irs` |
| health / medical / supplements / aging body | `senior_health_medical` |
| AI / coding / dev / SaaS / startup | `tech_ai_dev_tools` |
| Roblox / TikTok-style / shorts / what-if | `vertical_shorts_gaming` |
| drill / music video / parody music | `music_video_drill` |
| history / Roman / WWII / 1500s | `history_explainer` |
| investigation / scam / exposé | `news_hijack_documentary` |
| true crime / unsolved / case | `true_crime` |
| recipe / cooking / kitchen | `cooking_food` |
| real estate / property / mortgage | `real_estate_home` |
| beauty / makeup / fashion / skincare | `beauty_fashion` |
| vlog / lifestyle / day-in-life | `vlog_lifestyle` |
| crypto / Bitcoin / Ethereum / trading | `crypto_finance_trading` |
| how-to / tutorial / productivity | `how_to_education` |
| fitness / lifting / training / bodybuilding | `fitness_bodybuilding` |
| faith / Christian / scripture / sermon | `faith_christian` |
| political / election / Congress / policy | `politics_commentary` |
| travel / abroad / country / destination | `travel` |
| reaction / reacts to | `reaction` |
| industrial history / economic / industry | `documentary_longform` |
| ambient / sleep / focus / lofi / 1-hour | `ambient_sleep_focus` |
| gaming / character battle / fandom-lore | `gaming_longform` |

When two clusters appear in one description, the more specific one wins (e.g., "Christian finance for retirees" → `senior_finance_irs` because that's more specific than `faith_christian`).
