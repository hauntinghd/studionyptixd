# Skill 07 Companion — i2v Prompt Cookbook

This is the runtime companion to Skill 07 (Image-to-Video Prompting). Skill 07 documents model selection, the SUBJECT+ACTION+CAMERA+LIGHTING+MOOD+TECHNICAL skeleton, and motion-verb principles. This file provides:

1. **Shot-type cookbook** — 10 shot types every long-form video needs, with prompt template + 2-3 worked examples per type
2. **Per-model fingerprint bank** — what each model genuinely excels at, what it breaks on, with worked examples
3. **Motion verb bank** — 30+ verbs sorted into clean / risky / forbidden tiers with diagnostic notes
4. **6 anti-pattern prompts** with diagnoses

The agent loads this when generating i2v prompts. Pulling from a per-shot-type template + per-model-tuned wording is faster, more consistent, and avoids the common AI failure of generic-prompts-yielding-generic-output.

---

## 1. Why a shot-type cookbook

Most i2v prompts collapse for the same reason: the agent treats every shot as a generic "subject doing thing in a place." But real video work has 8-10 recurring shot types (establishing, hook close-up, B-roll, transition, mechanism diagram, etc.), and each one has a different prompt shape that produces clean output.

A locked shot-type template:
- Reduces prompt iteration cycles by 60-70%
- Cuts variance in output style across a single video (consistency = brand)
- Makes the Sample Gate (§5 of Skill 07) cheaper because failures cluster around predictable failure modes

The agent identifies the shot type FIRST (from storyboard), then loads the matching template, then customizes with the specific subject/action.

---

## 2. Shot-type cookbook

### Shot-type 1 — Establishing exterior

**Function:** Opens a scene. Sets location and tone. Often the first 2-3 seconds of a beat.
**Default model:** Veo 3 (realism) or Kling 2.0 Standard (cost).
**Camera:** slow push-in over 4-5 seconds OR slow truck-right.
**Duration:** 4-5 seconds.

**Template:**
> [Specific exterior location, season, time of day]; [environmental detail in motion — flag, leaves, traffic, smoke]; slow [push-in / truck-right] over 5 seconds; [lighting register matched to mood]; [mood/style phrase]; 16:9, 5 seconds, moderate camera motion, no cuts.

**Worked example A — Personal-finance authority IRS:**
> A small one-story brick house on a quiet residential street in suburban Cincinnati, late autumn, fallen leaves drifting across the front lawn in a slow breeze, an American flag hanging on the porch swaying gently; slow push-in over 5 seconds; warm late-afternoon golden hour light from frame-right, long shadows; mood is quiet domestic vulnerability; 16:9, 5 seconds, moderate camera motion, no cuts.

**Worked example B — News-hijack documentary:**
> The Treasury Department building in Washington DC, overcast January afternoon, light rain on the steps, a single black town car pulling away from the entrance; slow truck-right over 5 seconds; flat overcast diffused light, slight cool grade; mood is institutional gravitas; 16:9, 5 seconds, moderate camera motion, no cuts.

**Failure modes:** Models often hallucinate weather changes mid-clip. Lock the weather state in the lighting field. If you say "overcast," do not also say "shafts of sun" — pick one.

---

### Shot-type 2 — Hook close-up (face-centric)

**Function:** Beat 1 hook in long-form authority video. Carries 60% of the click-pay-off load.
**Default model:** Kling 2.1 Pro (face fidelity matters here).
**Camera:** static or near-static. Subject's micro-expressions carry the motion.
**Duration:** 4-6 seconds.

**Template:**
> [Specific subject with age, attire, micro-detail]; [single specific action: looks up / sets down / opens / reads]; [secondary micro-expression: eyes widening, jaw tightening, breath catching]; static medium close-up at eye level; [warm/cool/neutral specific lighting]; [intimate / weighted / urgent mood]; 16:9, 5 seconds, minimal camera motion, no cuts.

**Worked example A — Personal-finance authority IRS:**
> A 70-year-old grandmother in a beige cardigan and reading glasses on a chain, sitting at a kitchen table; slowly looks up from a folded letter, brow furrowing, lips pressing together; static medium close-up at eye level; warm golden-hour light from frame-right window, soft fill from above, slight shadow under eyes; mood is intimate concern with hint of dread; 16:9, 5 seconds, minimal camera motion, no cuts.

**Worked example B — News-hijack documentary:**
> A 38-year-old man in a navy hoodie at a cluttered desk, two laptops open, dim apartment lighting; slowly leans back from the screen, runs hand through hair, exhales through nose; static medium close-up slightly low angle; cool blue monitor light dominant on face, warm desk lamp at frame-right; mood is exhausted realization; 16:9, 5 seconds, minimal camera motion, no cuts.

**Worked example C — True crime narrator:**
> A 47-year-old woman in a soft cream sweater, sitting in a study with bookshelves blurred behind, low warm key light from camera-left; slowly closes a manila folder, pauses, looks just past the camera; static medium close-up at eye level; weighted soft warm key, deep shadow on right side of face; mood is reluctant witness gravity; 16:9, 6 seconds, minimal camera motion, no cuts.

**Failure modes:** Adding a camera move to a hook close-up is the single most common error. Don't. The face is the motion. Camera should be locked.

---

### Shot-type 3 — Mechanism diagram / explainer visual

**Function:** Beat 5 mechanism explanation. The visual that makes the abstract concrete.
**Default model:** Veo 3 (text legibility) or Kling 2.0 (cinematic flow).
**Camera:** slow orbit OR static reveal with subtle motion in graphics.
**Duration:** 5-8 seconds.

**Template:**
> [Specific physical metaphor — book opening, lock turning, file folder filling, water flowing through pipes, dominoes falling]; [camera: orbit, push-in, or top-down]; [lighting: studio clean or single dramatic key]; [mood: methodical, inevitable, mechanical]; 16:9, [duration] seconds, smooth camera motion, no cuts.

**Worked example A — Finance/IRS mechanism:**
> A photoreal close-up of an old wooden file cabinet drawer slowly sliding open, revealing rows of beige folders with dates handwritten on the tabs, a single hand pulling out one folder labeled "1997" and laying it flat on a desk; slow push-in following the folder, then settling on the open file; warm desk-lamp key light, deep shadow in cabinet; mood is methodical, inevitable; 16:9, 6 seconds, smooth camera motion, no cuts.

**Worked example B — Tech/AI mechanism:**
> A photoreal physical chain of dominos in matte black falling in sequence across a clean white surface, slow-motion at the moment of impact, each domino marked with a single icon (lock, key, cog); side-on tracking shot following the cascade left to right; bright studio overhead light, soft shadows; mood is inevitable cause-and-effect; 16:9, 6 seconds, smooth tracking, no cuts.

**Failure modes:** Asking the model to render text on screen often produces warped letters. Use icons, color-coding, or text-in-code overlay instead.

---

### Shot-type 4 — Composite case / patient story shot

**Function:** Beat 4 or 6 — the named patient/victim/case visualized.
**Default model:** Kling 2.1 Pro (faces) or Sora 2 Pro (premium scenes).
**Camera:** slight push-in OR handheld observational.
**Duration:** 5-8 seconds.

**Template:**
> [Specific named composite subject with age, profession-suggesting attire, location-suggesting backdrop]; [domestic action that visualizes their situation — making tea, sorting mail, looking out a window]; [slight camera motion: push-in OR observational handheld]; [warm intimate or cool detached lighting]; [mood matched to where the patient is in the story arc]; 16:9, [duration] seconds.

**Worked example A — Personal-finance authority "Patricia":**
> A 72-year-old woman in a faded blue cardigan standing at a kitchen counter in a small condo, mail scattered on the counter beside her, holding one folded letter from the Social Security Administration; she sets the letter down, walks to the front door, picks up car keys; observational handheld shot following her from kitchen to door; warm late-afternoon light from kitchen window, dimmer hallway behind; mood is mundane vulnerability; 16:9, 7 seconds.

**Worked example B — Medical-authority health "Robert":**
> A 67-year-old man in a worn flannel shirt at a small kitchen table outside Phoenix, evening, single overhead pendant light, paper utility bills and a prescription bottle on the table; he slowly counts pills from the bottle into his palm, frowning; slight push-in over 5 seconds; warm overhead key light, long shadow on far wall; mood is anxious calculation; 16:9, 6 seconds.

**Failure modes:** Don't over-specify physical features (height, weight, exact eye color). The model gets confused. Specify age range, attire, and one defining detail. Let the model fill the rest.

---

### Shot-type 5 — Authority intro / credentialed expert reveal

**Function:** Beat 3 or scripted authority moments. The host or expert on screen.
**Default model:** Kling 2.1 Pro or Veo 3 Fast.
**Camera:** static or very subtle push-in.
**Duration:** 3-5 seconds.

**Template:**
> [Specific authority subject with profession-suggesting attire — lab coat, suit, scrubs]; [neutral confident action: settles into chair, opens a folder, looks at camera]; static or very slow push-in; [neutral professional lighting: three-point or window-key]; [calm authoritative mood]; 16:9, 4 seconds, minimal motion.

**Worked example A — Personal-finance authority host:**
> A 35-year-old male tax accountant in a navy quarter-zip and reading glasses, seated at a wooden desk with a small American flag and books behind him, soft window light from camera-left; he glances down at a folder, then looks calmly at camera; very slow push-in over 4 seconds; warm three-point lighting, slight backlight separating from background; mood is calm credentialed authority; 16:9, 4 seconds.

**Worked example B — Health expert:**
> A 50-year-old female physician in a white lab coat over a soft-blue blouse, stethoscope around neck, in a clean modern clinic exam room with diplomas blurred behind; she sets a clipboard down, folds hands, looks at camera; static medium shot at eye level; bright clean clinic lighting; mood is calm professional warmth; 16:9, 4 seconds.

**Failure modes:** Adding an animated background graphic behind the host often pulls focus and distracts. Keep backgrounds blurred and physical.

---

### Shot-type 6 — Static B-roll cutaway

**Function:** Cuts away from the host to a supporting visual. Used between paragraphs and at retention valleys.
**Default model:** Hailuo 02 (cheap) or LTX-2.3 (cheapest).
**Camera:** very slight motion or static.
**Duration:** 3-4 seconds.

**Template:**
> [Single specific object or scene supporting current narration]; [very subtle motion: leaves rustling, water dripping, paper turning, smoke rising]; [static or imperceptibly slow push-in]; [lighting matched to scene tone]; [mood: atmospheric, supportive]; 16:9, 3 seconds, minimal motion.

**Worked example A — Finance B-roll:**
> A photoreal close-up of a stack of opened envelopes on a wooden kitchen table, one envelope resting on top with the Social Security Administration logo visible, a pen lying across the stack; very subtle paper edge curling from a draft; static; warm afternoon side-light from camera-right; mood is mundane domestic; 16:9, 3 seconds.

**Worked example B — Health B-roll:**
> A photoreal top-down shot of an organized pill organizer with seven daily compartments, three of them filled with various capsules and tablets, the rest empty; static; bright clean overhead light, slight specular highlight on the plastic; mood is methodical clinical; 16:9, 3 seconds.

**Worked example C — News-hijack B-roll:**
> A photoreal close-up of a smartphone screen showing a pause indicator on a video, the YouTube watermark in the corner, the phone on a black surface; static; cool monitor glow on the device; mood is forensic; 16:9, 3 seconds.

**Failure modes:** B-roll prompts that introduce a new subject (a person walking in) become hero shots not B-roll. Keep the subject single-object and motion microscopic.

---

### Shot-type 7 — Transition / passage of time

**Function:** Visual punctuation between beats. Signals a story shift.
**Default model:** Hailuo 02 or Kling 2.0 Standard.
**Camera:** sweep, whip-pan, or static with environmental change.
**Duration:** 2-3 seconds.

**Template:**
> [Specific environmental element representing time passing — sun across a wall, clock hands, calendar pages, sky color shifting]; [obvious motion: sweep, time-lapse, whip pan]; [lighting: dramatic shift across the duration]; [mood: time passing, forward momentum]; 16:9, [duration] seconds, fast camera motion acceptable.

**Worked example A — Time passing:**
> A photoreal time-lapse of a kitchen wall as the sun moves across it, shadows shortening then lengthening, a wall clock visible in the corner with hands sweeping forward; locked camera; lighting shifting from warm morning to bright noon to warm late afternoon over 3 seconds; mood is calendar passage; 16:9, 3 seconds, fast environmental motion.

**Worked example B — Whip-pan transition:**
> A photoreal whip-pan across a Treasury Department interior corridor, motion blur dominant, occasional sharp focal points on doors and signage; rapid horizontal pan over 2 seconds; cool overhead fluorescent lighting; mood is institutional handoff; 16:9, 2 seconds, fast camera motion.

**Failure modes:** Whip-pans across people often produce melted faces. Pan across architecture, environments, or static objects.

---

### Shot-type 8 — Action / dramatic moment

**Function:** Beat 4 or 6 dramatic peak. The "and then it happened" visual.
**Default model:** Kling 2.1 Pro or Veo 3.
**Camera:** dynamic — push, pull, or follow.
**Duration:** 4-6 seconds.

**Template:**
> [Specific subject in motion or about to break stillness]; [single dramatic action verb: collapses / surges / shatters / opens / vanishes]; [matched camera motion: push-in / pull-back]; [dramatic lighting: low-key, single key, hard shadow]; [urgent mood]; 16:9, [duration] seconds.

**Worked example A — Finance dramatic moment:**
> A photoreal hand pulling a single envelope from a mailbox, the envelope unfolds in slow motion to reveal an SSA letterhead, water droplets visible from a light rain; slow push-in following the envelope from mailbox to chest; cool overcast natural light, single warm porch lamp; mood is reluctant inevitability; 16:9, 5 seconds.

**Worked example B — Health dramatic moment:**
> A photoreal cardiologist in scrubs holding up an ECG strip, ink lines visible, the strip slightly trembling in their grip; static medium shot, focus on the strip; warm exam-room overhead, cool monitor glow at frame-edge; mood is moment of recognition; 16:9, 4 seconds.

**Failure modes:** Multiple action verbs in the same prompt confuse the model. Pick one. "Reaches for the phone, picks it up, dials" becomes a melt. "Picks up the phone" works.

---

### Shot-type 9 — Number / dollar / data reveal

**Function:** Beat 7 numbered list visualization. Often paired with on-screen text overlay (text added in code).
**Default model:** Kling 2.0 Standard or Veo 3 Fast.
**Camera:** static with subtle physical-element motion.
**Duration:** 3-4 seconds.

**Template:**
> [Specific physical metaphor for the number — stack of bills, calendar pages, file folders, pills, days marked off]; [subtle motion: bill flipping, page turning, folder closing]; static or imperceptible push-in; [bright clean clinical lighting]; [mood: factual, weighted]; 16:9, 3 seconds.

**Worked example A — Dollar reveal:**
> A photoreal close-up of a stack of one-hundred dollar bills on a wooden desk, a hand placing one more bill on top, the stack slightly fanning at the edges; static; bright warm desk-lamp light; mood is factual specificity; 16:9, 3 seconds.

**Worked example B — Days reveal:**
> A photoreal close-up of a paper desk calendar, today's date marked with a red circle, a hand using a black pen to draw an X across yesterday; static, slight camera shake; warm window side-light; mood is countdown; 16:9, 3 seconds.

**Failure modes:** Asking the model to render the actual number in the frame produces garbled digits. Reveal the metaphor (bills, days, files); add the number in code.

---

### Shot-type 10 — Beat-locked music video shot (brick-narrative storytelling style)

**Function:** Drill / propaganda music video shots, beat-locked.
**Default model:** LTX-2.3 (cost + stylized).
**Camera:** static or fixed gimbal motion locked to BPM.
**Duration:** exactly the bar length (often 3.5-4 seconds at 130-145 BPM).

**Template:**
> [Specific stylized scene with locked aesthetic — brick-toy figures, claymation, gritty filtered photoreal]; [single beat-locked action]; [locked composition with deliberate framing]; [dramatic genre-specific lighting]; [mood: aggressive / defiant / triumphant]; [aspect ratio matched to channel]; [exact bar duration].

**Worked example A — Brick-narrative storytelling:**
> Photoreal stylized brick-toy scene: a brick-toy general figure in green military uniform standing on the back of a brick-toy tank rolling slowly forward, smoke trailing behind, a brick-toy city skyline in the background with a sunset; locked low-angle shot looking up at the figure; orange-and-purple sunset cinematic grade, slight film grain; mood is propaganda triumph; 16:9, 3.5 seconds, slow forward motion.

**Worked example B — Drill music video:**
> Photoreal gritty filtered shot of a young man in a black hoodie and balaclava standing under a single overhead street light in an empty parking lot at night, breath visible, holding a microphone; locked low-angle shot; cold blue moonlight + warm overhead sodium lamp, hard contrast, slight digital grain; mood is defiant; 9:16, 4 seconds, almost no motion.

**Failure modes:** Beat-locking requires precise duration. If a clip is 3.7 seconds and the bar is 3.5 seconds, the cut feels off. Generate at exact bar duration; trust the model to deliver to spec.

---

## 3. Per-model fingerprint bank

Each i2v model has a distinct fingerprint — what it does well, what it fails on, what to leverage. The agent picks the model first (Skill 07 §2), then writes the prompt to the model's strengths.

### Kling 2.0 Standard

**Fingerprint:** Cinematic generalist. Smooth camera motion. Solid faces if not the focal point.
**Excels at:** Establishing shots, dialogue close-ups, slow camera moves.
**Breaks on:** Fast action, complex multi-subject scenes, frame-filling text.
**Tuning notes:** Use SUBJECT + ACTION + CAMERA + LIGHTING fields. Skip MOOD if budget tight; Kling infers it from lighting.
**Worked example to leverage strength:**
> Slow push-in over 5 seconds on a man at a kitchen table; warm late afternoon light; minimal motion. (Kling clean output rate: ~85%.)

**Worked example showing failure:**
> A car chase through a crowded market with three pedestrians and a vendor falling. (Kling collapses; multi-subject + fast motion.)

### Kling 2.1 Pro

**Fingerprint:** Premium face fidelity. Best in class for hero shots.
**Excels at:** Hook close-ups, named-character composite shots, beat 1 face shots.
**Breaks on:** Complex environments behind faces, very long durations (>8 sec).
**Tuning notes:** Spend the budget here ONLY when face is dominant. For B-roll, downgrade to Standard or Hailuo.
**Worked example to leverage strength:**
> Static medium close-up of 70-year-old grandmother reading a letter; subtle expression shift over 5 seconds. (Kling Pro: industry-leading on this.)

### Kling 2.1 Master

**Fingerprint:** Highest quality in Kling family.
**Excels at:** Channel trailers, year-end recaps, hero shots with quality budget.
**Breaks on:** Cost. Use sparingly.
**Tuning notes:** Reserve for the 1-2 hero shots per video where the user is willing to pay 3-4× the standard rate.

### Hailuo 02

**Fingerprint:** Cheap stylized workhorse.
**Excels at:** B-roll, atmospheric shots, slight-grain stylized aesthetics, music videos at scale.
**Breaks on:** Photoreal humans (faces look slightly waxy), tight motion control.
**Tuning notes:** Don't put faces in the center of the frame. Use for B-roll and atmospheric beats. Lean into the stylized texture.
**Worked example to leverage strength:**
> Photoreal slightly stylized close-up of a stack of envelopes with subtle paper-edge motion. (Hailuo: ~3¢ vs Kling's 50¢. Indistinguishable for this shot type.)

### Veo 3 / Veo 3 Fast

**Fingerprint:** Realism specialist with strong physics.
**Excels at:** Object physics (water, smoke, fire), realistic camera moves, science visualization.
**Breaks on:** Highly stylized aesthetics (it auto-pulls back to realistic), short durations under 4 sec.
**Tuning notes:** When realism is the brand register (documentary, science explainer), Veo wins. Don't fight its realism gravity with stylized prompts.
**Worked example to leverage strength:**
> Slow-motion close-up of water dripping from a copper pipe, hitting a polished concrete floor, splash dynamics visible. (Veo: best-in-class on this.)

### Sora 2 Pro

**Fingerprint:** Cinematic premium. Best perceived production value.
**Excels at:** Long durations (up to 20 sec), complex multi-subject scenes, dramatic establishing shots.
**Breaks on:** Cost ($0.50/sec), queue times, restrictive content policy.
**Tuning notes:** Use for the 1 hero shot per video that defines the brand. Don't burn budget on B-roll.
**Worked example to leverage strength:**
> Aerial drone shot pulling back from a single house in a snow-covered suburban street to reveal a neighborhood at dusk, holiday lights twinkling on, smoke from a chimney. (Sora: industry-leading; would cost $10 for 20 seconds.)

### LTX-2.3 (self-hosted RunPod)

**Fingerprint:** Volume workhorse. Pass-through compute cost.
**Excels at:** Music video generation at scale (50+ clips), stylized content, anything where per-clip cost matters.
**Breaks on:** Photoreal humans (lower fidelity than commercial), self-hosting complexity (pod setup required).
**Tuning notes:** Use when generating 50+ clips for a music video or high-volume B-roll. The break-even vs Hailuo is around 30-40 clips per project.
**Worked example to leverage strength:**
> 50 brick-toy-style clips at 3.5 seconds each for a beat-locked drill music video. (LTX: ~$2.50 total; Hailuo equivalent ~$10; Kling ~$25.)

### Per-model selection matrix

| Shot type | Default model | Why |
|---|---|---|
| Establishing exterior | Kling 2.0 Standard | Cinematic, cost-efficient |
| Hook close-up | Kling 2.1 Pro | Face fidelity is load-bearing |
| Mechanism diagram | Veo 3 Fast | Physics realism + text legibility |
| Composite case shot | Kling 2.1 Pro | Named character needs strong face |
| Authority intro | Kling 2.1 Pro or Veo 3 Fast | Subject is host/expert |
| Static B-roll | Hailuo 02 | Cheap, motion is microscopic |
| Transition | Hailuo 02 | Short, low-stakes |
| Action / dramatic | Kling 2.1 Pro or Veo 3 | Motion + face matter |
| Number / data reveal | Kling 2.0 Standard | Static physical metaphor |
| Music video shot | LTX-2.3 | Volume + stylized aesthetic |

---

## 4. Motion verb bank

i2v models are extremely sensitive to verb choice. Some verbs produce clean motion. Others produce melted nightmare fuel. Below: 30+ verbs sorted into clean / risky / forbidden tiers.

### Clean tier — verbs that work reliably across all models

These verbs produce clean motion ~90%+ of the time. Default to these.

- **Looks up / looks down / looks at** — minimal head motion, models handle well
- **Slowly turns** — controlled rotation
- **Sets down / picks up** — single object interaction
- **Reads / writes / signs** — bounded action with object
- **Walks slowly toward / away from** — controlled locomotion
- **Sits / stands** — postural change, single transition
- **Opens / closes** — doors, books, envelopes — clean state change
- **Pours** — liquid physics handled well
- **Nods / shakes head** — micro-motion
- **Furrows brow / raises eyebrow / closes eyes** — micro-expression
- **Drifts / floats / hovers** — slow continuous motion
- **Glances** — micro-attention shift
- **Pauses / hesitates** — non-motion as motion (signals stillness with slight breath/blink)
- **Reaches for** — bounded directional motion (stop here; "reaches for and grabs" doubles the verb)

### Risky tier — verbs that work but require careful prompting

These verbs work but only with a single subject, single direction, and clear duration. Use sparingly.

- **Runs** — works only if direction is locked (left to right, away from camera). "Runs around" produces melt.
- **Throws** — works for single object. "Throws a ball" works; "throws everything off the desk" melts.
- **Drives** — works for static cars in motion-blur background. Foreground driver action collapses.
- **Falls** — works if object is single and trajectory is linear. Person falling tends to ragdoll.
- **Crashes** — works for vehicles into stationary objects. Multi-vehicle crashes melt.
- **Smiles / laughs** — works for slight smiles. Big laughs distort the face.
- **Cries** — works for single tear, slight tremble. Sobbing melts.
- **Argues / yells** — works for one person yelling. Two-person argument melts.
- **Dances** — works for slow dance. Fast dance becomes ragdoll.
- **Flies** — works for objects (paper, leaves). People flying often distort.
- **Cooks** — works for single steady action (stirring). Multi-step cooking melts.

### Forbidden tier — verbs that almost always melt

- **Fights** — combat sequences melt across all models
- **Kisses** — face-to-face contact melts
- **Hugs** — multi-person contact melts
- **Plays sports** — multi-subject coordination melts
- **Performs surgery** — fine motor tool work melts
- **Operates machinery** — fine hand-tool coordination melts
- **Writes paragraphs** — extended hand motion + text melts
- **Cuts hair / shaves** — fine tool work melts
- **Climbs** — multi-limb coordination melts
- **Swims** — water + body motion melts
- **Jumps** — gravity + body coordination melts (contradiction with "falls" — falls works because it's gravity + body without push-off)
- **Performs / dances complex choreography** — multi-step body motion melts

### Verb stacking rules

- **One verb per prompt.** "Walks to the door, opens it, steps outside" is three actions; the model picks one and melts the others.
- **If you must chain, use "then":** "Walks to the door, then opens it" rarely produces clean output. Better: split into two clips.
- **Background verbs are free.** "A man reads a letter while leaves drift past the window" — "reads" is the focal verb; "drift" is environmental motion the model handles separately.

### Camera verb bank

- **Push-in** — clean, default
- **Pull-back** — clean, default
- **Truck-left / truck-right** — clean, lateral motion
- **Pan-left / pan-right** — clean for environments, risky for faces
- **Tilt-up / tilt-down** — clean
- **Orbit / arc** — works for single subjects, melts on complex environments
- **Whip-pan** — works only across architecture/environment, never people
- **Handheld / shaky** — adds realism, slight risk of subject distortion
- **Locked / static** — safest, default for face shots

---

## 5. Anti-pattern prompts

Six prompts that look correct but fail. Diagnoses attached.

### Anti-pattern 1 — *Multi-action prompt*

> "A woman walks to the door, opens it, steps outside, and waves at her neighbor."

Diagnosis: 4 verbs in one prompt. Model picks one and melts the others. The walking + opening + stepping + waving is 4 distinct beats.
Fix: split into 2-3 clips. Clip 1: walks toward door. Clip 2: opens door. Clip 3: waves.

### Anti-pattern 2 — *Conflicting environmental conditions*

> "An overcast January afternoon with shafts of bright sunlight breaking through onto the cobblestones."

Diagnosis: "overcast" and "shafts of sunlight" are mutually exclusive in physical reality. The model alternates randomly across the clip, producing flickering lighting.
Fix: pick one. Either "overcast diffused light" OR "low winter sun breaking through clouds, dappled light." Not both.

### Anti-pattern 3 — *Frame-filling text request*

> "A close-up of a Treasury Department letter, the words 'YOUR BENEFITS HAVE BEEN REDIRECTED' clearly visible in the center."

Diagnosis: AI models render text inconsistently. The letters warp, kerning collapses, words distort.
Fix: generate the letter as a clean blank document; add the text in code (Remotion, SVG, Photoshop overlay). This is the same text-in-code rule as Skill 09 §2 and Skill 02 §3.

### Anti-pattern 4 — *Generic everything*

> "A man at a desk doing finance stuff in an office."

Diagnosis: every field is generic. The model produces a stock-image-feeling output. No identifiable character, no specific environment, no specific action, no mood.
Fix: rewrite with specificity. "A 50-year-old male tax accountant in a navy quarter-zip, sitting at a wooden desk, slowly turning a page of a folder labeled 'AUDIT 2026,' window light from camera-left, mood is methodical concern." — every field has an anchor.

### Anti-pattern 5 — *Camera move on a face shot*

> "Static medium close-up of a 70-year-old grandmother reading a letter. Camera does a slow orbit around her over 5 seconds."

Diagnosis: orbiting around a face produces face distortion and inconsistent angles. Faces should be locked on for hook close-ups.
Fix: either lock the camera (static) OR change to environmental orbit ("camera orbits the kitchen, ending on the grandmother at the table").

### Anti-pattern 6 — *Stylized prompt to a photoreal model*

> "Veo 3: A brick-toy figure of a soldier with a vibrant cinematic anime grade, claymation textures, neon outline."

Diagnosis: Veo 3 has realism gravity. Stylized prompts get pulled toward realism, producing a confused half-real, half-stylized output.
Fix: match model to register. For stylized: LTX-2.3 or Hailuo 02 with explicit style locking. For realism: Veo 3 with photoreal vocabulary.

---

## 6. Runtime cite-from-cookbook workflow

When generating an i2v prompt for any shot:

1. **Identify shot type** from storyboard. Match to one of the 10 shot types in §2.
2. **Pull the template** for that shot type.
3. **Pick the model** from §3 (Per-model fingerprint bank) based on shot type. Use the matrix in §3 as default.
4. **Customize the SUBJECT and ACTION fields** with topic-specific specifics.
5. **Verify motion verb is in the clean tier** (§4). Reject any prompt with risky/forbidden verbs unless the shot demands it.
6. **Verify single-verb rule** — one focal action per prompt. Background motion is allowed.
7. **Verify lighting consistency** — no contradicting environmental conditions.
8. **Verify text-in-code rule** — no on-screen text requested.
9. **Run Sample Gate** (Skill 07 §5). Generate 1 sample. Confirm with user. Cost: ~$0.10-0.30 for sample vs $1-3 for full batch.
10. **Generate full batch** after Sample Gate confirmation.

The agent does not generate i2v prompts in isolation. It identifies shot type → loads template → tunes for model → audits against motion verb bank and anti-patterns → samples → confirms → generates. This collapses prompt iteration cycles by 60-70%.
