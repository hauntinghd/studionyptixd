# Tactical Playbook — AI Host: War Veteran / Military Storyteller

> **DEPRECATED (2026-04-29).** Superseded by `T1-ai-host-setup.md` (profession matrix covers 15 professions). Load T1 instead. Preserved for reference only.

## The goal

Generate a reference image of an older military veteran sitting in their den or home study, photographed as if they had set up a webcam to record a YouTube video sharing a war story or military history. The output must read as: **"this is a real Vietnam / Korea / Gulf War veteran in his actual home talking to me about something he lived through,"** not as **"this is an actor in a costume in front of a flag backdrop."**

This is one of the most demanding playbooks because the audience for war-veteran content (military history enthusiasts, veteran communities, history-channel viewers) is intensely sensitive to authenticity. Wrong rank insignia, wrong uniform era, wrong demographic, wrong body language — any single tell and the audience leaves immediately.

This playbook applies to:
- War history / combat documentary channels (Vietnam, Korea, WWII, Gulf War, Iraq, Afghanistan)
- Veteran storytelling / oral history channels
- Military strategy / tactics analysis channels
- Patriot / faith-and-country-adjacent commentary

## The non-negotiable details

These MUST be present or the image fails:

1. **Visible but understated military identification** — at minimum ONE of: a single framed shadow box with medals (NOT prominently displayed), a folded American flag in a triangular case, a unit photo on the wall, a challenge coin on the desk. NEVER all of these — the room is a HOME, not a museum.
2. **Den / study setting, not a memorial wall** — wood paneling, leather armchair or wooden desk, books, low warm lighting. Could be a cabin, a ranch, a suburban home study.
3. **Warm, dim, lived-in lighting** — single warm lamp, fireplace glow if applicable, OR soft window light from one side. Lighting should feel earned, not staged. NEVER bright clinical.
4. **Mid-shot or wide-mid framing** — head, shoulders, chest, with the room context visible behind. The room is part of the character. Show enough that the audience reads "this is his actual den."
5. **Significant age signaling** — Vietnam vet variant: 75-85 years old. Gulf War vet: 55-65. Iraq/Afghanistan: 40-55. Match the era. Visible age markers: gray or white hair, weathered skin, lines around eyes from squinting in sun decades ago.
6. **Direct or near-direct eye contact** — calm, weighted, present. NOT scowling, NOT smiling broadly, NOT staring intensely. The expression is "I have something to tell you, and I'm not in a hurry."
7. **Casual civilian wardrobe with subtle military signaling** — flannel shirt, henley, plain button-down. Possibly a unit ball cap on the desk (not on the head — wearing the cap reads "trying to convince you"). NEVER full uniform unless the channel is specifically a current-active-duty commentary format.
8. **Slightly weathered hands visible in some shots** — older veterans should have hands that show life. Subtle detail but it lands.
9. **Photorealistic rendering** — never illustrated, anime, 3D-render, or stylized.

## The do-not list

These ruin the output:

- ❌ Subject in full military uniform sitting at home (reads "actor in costume")
- ❌ American flag draped behind subject as backdrop (reads "political ad set")
- ❌ Wall covered in framed medals, certificates, ribbons (reads "memorial wall, not lived-in home")
- ❌ Three-point cinematic studio lighting
- ❌ Salute pose, chin-up "proud" pose, or dramatic eyes-distant "remembering" pose (clichés)
- ❌ Holding a rifle, sword, or any weapon prop in a domestic setting
- ❌ Wearing a unit cap directly facing camera (reads forced, salesman-like)
- ❌ Camo / fatigues at home (off-duty veterans wear civilian clothes)
- ❌ Background blur so deep the den is unreadable (cinematic, not authentic)
- ❌ Wrong-era equipment in shot (Vietnam vet with WWII gear behind him is an instant credibility kill for the audience)
- ❌ Subject too young for the era they're representing
- ❌ Smiling broadly (reads insincere; this audience expects reserved gravitas)

## The locked prompt template

```
A [AGE: 75-85] year old [GENDER: male/female] American military veteran sitting in a wood-paneled home den or study, photographed as if they had set up a webcam at their wooden desk to record a YouTube video about their service. They are wearing a [WARDROBE: faded plaid flannel button-down / dark henley / plain button-down] in [COLOR: muted earth tone, navy, or olive], slightly worn-looking. Visible details: silver-gray or fully white hair, weathered skin with lines around the eyes, slightly weathered hands. Behind them: a single framed shadow box with subtly visible medals (not prominent), a folded American flag in a small triangular case on a shelf, a few personal photographs from younger days, books on military history or general literature, a desk lamp providing warm tungsten light. The room is dimly lit by a single warm lamp from the right and a hint of late-afternoon sunlight from a window to the left. The subject is framed mid-shot from chest up, slightly off-center to the right, looking near-directly at the camera with a calm, present, slightly serious expression — neither smiling nor scowling, simply attentive. Photorealistic, captured as if with a basic webcam at slightly low angle (eye level or just below). Realistic skin texture with significant age detail, NO smoothing, weathered authenticity preserved. Aesthetic: a real older veteran in his actual home den, NOT a memorial set, NOT a studio shoot, NOT a political ad. 16:9, photorealistic, warm natural color grade, no filter.
```

## Model selection

| Model | Performance | Notes |
|---|---|---|
| **Nano Banana (Gemini 3 Pro Image)** | ⭐⭐⭐⭐⭐ Best | Best at age detail (skin texture, weathering, eyes), reliably renders authentic den environments |
| Flux 1.1 Pro | ⭐⭐⭐⭐ Good | Strong on age but tends to over-smooth weathered skin |
| Midjourney v7 | ⭐⭐⭐ Cinematic-leaning | Renders beautifully but defaults to cinematic dramatic register; needs aggressive constraint |
| DALL-E 3 | ⭐⭐ Weak | Often produces "actor playing a veteran" aesthetic |

**Default: Nano Banana.** This playbook particularly benefits from Nano Banana's age-detail strength.

## Common variations

### Vietnam veteran (75-85)
Era markers: photos from late 1960s-early 1970s visible on shelf or wall. Subtle Vietnam-era unit references in shadow box. Wardrobe leans flannel + jeans visible.

### Korean War veteran (90+)
Significantly older. Era markers: black-and-white photos from 1950-1953. Subject may be sitting more carefully, perhaps with a cane visible.

### Gulf War veteran (55-65)
Era markers: photos from 1990-1991. Wardrobe slightly more contemporary. Body language slightly more upright.

### Iraq / Afghanistan veteran (40-55)
Era markers: digital-camo era photos, more recent unit references. Younger appearance. Often more direct emotional register.

### WWII veteran (extremely rare in 2026, 100+)
Use only when the channel specifically has an authentic WWII storyteller. Treat with extreme reverence.

### Female veteran variant
Same authenticity rules apply. Age range matches era. Wardrobe: button-down, henley, or simple sweater. Hair: short, gray or silver. Personal items adjust to match (photos of female service members from the appropriate era).

### Specialty signaling
- **Combat veteran:** subtle inclusion of a Combat Infantryman Badge or similar in the shadow box (researched per era).
- **Pilot:** a small model aircraft on the shelf, framed flight log visible.
- **Special operations:** more reserved expression, less identifying memorabilia (intentional understatement).
- **Medic / corpsman:** medical books in shelf alongside military history books, blends with nurse/medic adjacency.

## When to use

This playbook is called by:
- **Skill 02 — Thumbnail Design** for war-history / veteran-storytelling channel thumbs
- **Skill 06 — Storyboard / Scene Breakdown** when establishing a war-storyteller host reference
- **Skill 09 — Image Generation Prompting** for veteran character generation
- **Skill 05 — Reference Channel Ingestion** when matching channels like Combat Veteran Stories, Mark Felton Productions hosting, or interview-format military history

## Integration with Skill 03 (Voice DNA)

This visual playbook pairs with **V3 War Veteran Storyteller** voice archetype from Skill 03. The match is critical — visual age MUST track voice age, and the voice must carry the lived-in weight the visual establishes. A 25-year-old voice on an 80-year-old face produces immediate audience exit.

## Sample-then-confirm gate

Before locking this character for the channel:
1. Generate 4 variants (different age within era, different ethnic representation, different room style)
2. Surface all 4 with explanation of era / unit / variation
3. User picks one OR asks for adjustments
4. Lock to Channel Memory

For war-veteran content specifically, the agent should ALWAYS surface era options because the audience for Vietnam content is different from the audience for Iraq/Afghanistan content, and the host should match.

## Anti-patterns specific to war-veteran hosts

- **The "salute and flag backdrop" cliché** — never render this combination. Reads as political ad or military recruitment poster.
- **The wall-of-medals memorial** — multiple framed displays of medals reads as "shrine to self," not "real veteran's home." One subtle shadow box is enough.
- **The full-uniform-at-home** — current uniforms aren't worn at home. Reads as actor in costume.
- **The "thousand-yard stare"** — looking distantly at nothing, eyes glazed. Cliché. Real veterans look at you when they're talking to you.
- **The chin-up proud pose** — "I served my country" posture. Real older veterans don't pose like this.
- **Wrong-era contamination** — Vietnam vet with Iraq War-era equipment behind him is an instant credibility kill. Era-research every prop.
- **Using stock photos of "older men in caps"** as reference — most stock photo "veterans" are clearly civilians who put on a hat. Use AI-generated reference, not stock.
- **The "talking with hands" pose** — over-gesturing reads inauthentic. Calm, still, weighted.

## Why these rules

The audience for war-veteran content is unusually authenticity-sensitive because:

1. **Many viewers ARE veterans or veteran-adjacent.** They will spot wrong rank insignia, wrong era equipment, wrong unit references within seconds.
2. **The genre has a long history of being exploited** — "stolen valor" is a real and policed phenomenon. Audiences pre-screen for inauthenticity.
3. **The emotional register is sacred to the audience** — combat experience is lived, not performed. Polish reads as performance.

The visual aesthetic that works: think of how oral history projects (StoryCorps, Library of Congress Veterans History Project) photograph their interview subjects. Soft lighting, lived-in environment, no artificial polish, no flag backdrops, no uniform costuming. The subject IS the visual — the room is just where they live.

The single trust signal: **subtle imperfection in the room.** A slightly askew picture frame, a coffee mug, a stack of mail. These signal "real home, real person." A perfectly arranged shadow-box wall signals "set design, fake person."

## Compliance / sensitivity notes

- Never reference specific named units, awards, or incidents in the visual without explicit user knowledge of military protocol. The audience will research and call out errors.
- Stolen valor laws apply to claims, not visual depictions, but the agent should never imply the avatar earned specific honors without user direction.
- Era research is non-negotiable. Get it wrong and the channel loses credibility on the first video.
- For sensitive content (PTSD discussion, combat trauma), the visual register should be calm and grounded, never sensationalized.

## Update log

This playbook is current as of April 2026. Update when:
- Image gen models improve age-detail rendering
- Era-specific reference databases shift
- The veteran-storytelling niche conventions evolve
- New respect-and-authenticity guidelines emerge from the veteran community
