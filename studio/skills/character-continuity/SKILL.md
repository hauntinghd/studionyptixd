---
name: character-continuity
description: >-
  Character drift mitigation across i2v generations (locked seed + style ref + reuse rules). Load when generating multiple shots of the same character.
---

# Tactical Playbook T5 — Character & Visual Continuity Locking

This is the playbook for keeping AI-generated faces and characters **visually consistent** across multiple shots, multiple thumbnails, and multiple episodes — without face drift.

The single rule: **drift is the silent killer of AI-driven channels.** A host who looks slightly different in an early episode than in a later one erodes audience trust over time. Viewers don't always articulate it, but the brain registers "is this the same person?" and trust drops. A personal-finance authority channel's drift across its early run is the cautionary tale we built this playbook around.

This playbook covers: why drift happens, how to lock seeds, how to use reference images strategically, the "character bible" concept, when to regenerate vs photoshop, and continuity gates per surface.

---

## 1. The job of visual continuity

A channel running an AI host or recurring composite character needs the character to look the same:
1. **Across all shots in a single episode** (talking-head shot vs B-roll insert vs thumbnail)
2. **Across all episodes the channel ships** (early-episode face = current-episode face, ±natural aging)
3. **Across all surfaces** (avatar render vs thumbnail face vs B-roll face vs social media headshot)

When this fails, the channel loses brand recognition, audience trust, and the cumulative compounding that makes recurring characters work.

The agent's job:
- Lock a canonical reference image at character creation
- Use that reference for ALL future generations
- Detect drift before it ships
- Run continuity gates at every visual surface

## 2. Why drift happens

Five primary drift mechanisms:

**Drift 1 — Random seed variation.** Image gen models use random seeds. Same prompt + different seed = different face. If the agent doesn't lock seeds, every generation produces a slightly different person.

**Drift 2 — Prompt evolution.** Episode 1 prompt was "55-year-old male tax advisor in home office." Episode 25 prompt added "with reading glasses." Suddenly the face has changed because the prompt changed.

**Drift 3 — Model upgrade.** The image gen model gets updated mid-channel-life. Same prompt + same seed but new model version = different output. This happens annually.

**Drift 4 — Reference image not used.** Agent generates a fresh face each time instead of feeding the locked reference image. Each generation is independent and produces variation.

**Drift 5 — Cropping / aspect ratio change.** A face rendered for 16:9 is then cropped for 9:16 thumbnail and the face proportions read differently. Even without regeneration, perceived face changes.

The agent prevents each of these with deliberate protocols.

## 3. The seed-lock strategy

Every image gen model exposes a seed parameter. The agent's discipline:

**Step 1 — At character creation:** Generate 4-6 variants with random seeds. User picks one. Note the seed.

**Step 2 — Lock that seed to channel memory** alongside the reference image and the exact prompt that produced it.

**Step 3 — All subsequent generations use the locked seed** + the SAME prompt + the same model.

**Step 4 — When changes are needed** (e.g., user wants the host wearing a different shirt for a specific episode):
- Modify ONLY the wardrobe variable in the prompt
- Keep the seed locked
- Keep all other variables identical
- Generate sample, verify face stayed consistent
- Approve

**Step 5 — Document any prompt changes** in the character bible so the evolution is tracked.

### When seed lock isn't enough

Some scenarios where seed-lock alone fails:
- Different model (e.g., Nano Banana → Flux). Seeds aren't transferable across models.
- Major prompt change. New action / pose / setting can shift the face even with locked seed.
- Aspect ratio change. Vertical thumb from a 16:9 reference can produce face drift.

In these cases, the agent uses the reference image strategy (§4) as the primary continuity tool.

## 4. The reference image strategy

The most reliable continuity tool. Most modern image gen models accept a reference image as input and try to preserve facial features.

**Step 1 — Lock the canonical reference image at character creation.** Save the high-resolution image (1920×1080 or larger) to channel memory.

**Step 2 — At every subsequent generation,** feed the canonical reference as input alongside the new prompt. The model uses the reference as a face-anchor.

**Step 3 — Verify face consistency in the output** before accepting.

### Reference image protocols per model

- **Nano Banana (Gemini 3 Pro Image):** accepts reference image input, strong face preservation
- **Flux 1.1 Pro:** accepts reference, moderate face preservation
- **Midjourney v7:** uses `--cref` (character reference) flag, varying preservation
- **DALL-E 3:** weaker reference preservation; not recommended for character consistency

Default model for character continuity: **Nano Banana.** The agent locks Nano Banana as the channel's host-rendering model and only switches when the user explicitly opts in.

## 5. The "character bible" concept

For every recurring character (host or composite), the agent maintains a character bible in channel memory:

```
Character Bible — [Character name]
Channel: [channel name]
Created: [first reference]
Updated: [most recent]

—— LOCKED IDENTITY ——
- Name (if composite): [name]
- Age at creation: [age]
- Years aged since creation: [years]
- Gender: [gender]
- Profession / role: [profession]
- Demographic: [region, ethnicity if relevant]

—— LOCKED VISUAL ——
- Canonical reference image URL: [stored in R2]
- Seed used: [number]
- Prompt that produced canonical: [exact prompt]
- Model + version used: [Nano Banana v3.X.X]
- Aspect ratio of canonical: [16:9]

—— APPROVED VARIATIONS ——
For each approved variation (different wardrobe, different angle, different setting):
- Variation name: [description]
- Variation reference image URL: [stored]
- Variation prompt diff from canonical: [what changed]
- Approved at: [episode reference]

—— USAGE LOG ——
Per episode/surface:
- First episode — host shot, canonical reference used
- Second episode — host shot + thumbnail, canonical + thumb-cropped variant
- Early-run episode — B-roll insert, custom action variant approved
- Later-run episode — host shot, canonical reference reused
- ... etc

—— DRIFT WATCH ——
Last face-similarity check: [date]
Drift score from canonical: [0.0-1.0, lower is better]
Action threshold: re-anchor if drift > 0.15
```

This bible lives in channel memory and is loaded whenever the character is referenced.

## 6. Approved variations vs ad-hoc generation

The agent distinguishes between:

**Approved variations** — pre-rendered alternates that are part of the character bible:
- Wardrobe rotation (3-4 different shirts/cardigans)
- Setting variants (home office vs kitchen vs den)
- Expression range (calm / concerned / warm / serious)
- Aspect ratio versions (16:9 host shot, 9:16 vertical, 1:1 square thumbnail crop)

These are generated up front when the character is locked. User approves each. They become the rotation library.

**Ad-hoc generation** — single-use renders for specific episode needs:
- Unique action ("character holding a specific document")
- Unique location for one episode
- Unique pose for a thumbnail moment

These are generated per-episode but ALWAYS use the canonical reference + locked seed + similar prompt.

The agent biases heavily toward approved variations. Ad-hoc generation introduces drift risk; the rotation library minimizes it.

## 7. Cross-thumbnail consistency

Thumbnails are the highest-stakes continuity surface. Audience scrolls past 12 thumbnails on the channel page and the brain instantly checks: "is this the same person?"

The protocol:
1. The host's face on every thumbnail must read as the SAME person from the first episode to current
2. Approved expression range: 3-5 distinct expressions (calm, concerned, accusation, warm, serious)
3. Each expression rendered ONCE during character bible creation, saved as approved variation
4. Thumbnails reuse approved expressions, never generate fresh

When the agent needs a "new" expression (e.g., a thumbnail moment requires something specific):
- Use canonical reference as input
- Use same seed
- Use same lighting/setting/wardrobe as canonical
- Modify ONLY the expression variable in the prompt
- Render
- Sample-confirm with user

If the result drifts, the agent re-anchors (regenerates with stronger reference weight) or photoshops the canonical face onto a different expression body (last resort).

## 8. Cross-episode consistency

For host channels (personal-finance authority), the host should look the SAME across all episodes. Standard rules:

- Same canonical reference for every host shot in every episode
- Same wardrobe variants in rotation (don't introduce new shirts mid-channel-life)
- Same setting variants (home office should look like the same room every time)
- Aging: 1-2 years of natural aging visible across a year of content (subtle)
- Major aging: only when the character is explicitly aged for a specific story arc (rare)

Drift detection per quarter:
- Pull a frame from the most recent episode
- Pull the canonical reference
- Visually compare
- If drift is noticeable, regenerate canonical OR re-anchor the next episode's host shot

## 9. The drift cautionary tale

A personal-finance authority channel was created in mid-2024 with an early HeyGen avatar. Across its first 35 episodes:
- Early run: original avatar, slightly young-looking
- Mid run: HeyGen updated their model, avatar drifted slightly older
- Later run: another model update, slight face proportion shift
- Most recent batch: aware of drift, deliberately re-anchored the visual register

Lesson: even with a locked avatar, model updates can introduce drift over time. The fix:
1. Re-anchor the canonical reference periodically (every 6 months minimum)
2. Document each re-anchor with episode timestamp
3. When drift is detected, make a deliberate "the host evolved" moment in content rather than pretending it didn't happen
4. Use the same HeyGen avatar settings every time (don't experiment with new avatar parameters per episode)

This playbook now exists because of that lesson.

## 10. The regenerate vs photoshop decision

When drift happens or a specific scene requires the canonical face on different body content, two options:

### Regenerate
- Use canonical reference + seed + similar prompt
- Generate new image
- Pros: clean output, fully integrated
- Cons: drift risk if reference doesn't fully constrain
- Use when: the new shot is similar enough that reference will hold

### Photoshop / face-swap
- Take the new body content (different setting, action, etc.)
- Use a face-swap technique (e.g., InsightFace, Roop, or manual Photoshop) to put canonical face on new body
- Pros: face stays exactly canonical
- Cons: lighting / proportion mismatch can look uncanny
- Use when: the new shot is too different for reference-driven generation

### Decision tree
1. Is the new shot a small variation (different expression, slight wardrobe change, similar setting)? → **Regenerate** with canonical reference
2. Is the new shot a major variation (different setting, full body, action)? → **Photoshop** the canonical face onto the new body content
3. If photoshop produces uncanny result → re-render the body content with strong canonical reference, accept some drift

The agent biases toward regenerate. Photoshop is the escape hatch.

## 11. Continuity gates per surface

Before any visual ships:

### Host shot continuity gate
- [ ] Canonical reference used as input
- [ ] Locked seed applied
- [ ] Generated face matches canonical within visual similarity threshold
- [ ] Wardrobe matches approved rotation OR new variation explicitly approved
- [ ] Setting matches approved rotation OR new variation explicitly approved

### Thumbnail continuity gate
- [ ] Face on thumbnail matches canonical within similarity threshold
- [ ] Expression matches one of the approved expression variants
- [ ] Aspect ratio cropping doesn't distort proportions
- [ ] Adjacent thumbnails on channel page read as same person (visual sandwich check)

### B-roll continuity gate
- [ ] If character appears in B-roll: face matches canonical
- [ ] If composite character (Margaret, Linda) appears in B-roll: their bible reference used

### Cross-episode gate (run quarterly)
- [ ] Pull frames from the first episode's host, current episode's host
- [ ] Visual compare
- [ ] Acceptable drift: 1-2 years of natural aging visible
- [ ] Unacceptable drift: face proportions shifted, hair color/style changed, subtle features differ
- [ ] If unacceptable: re-anchor with deliberate re-render, document in bible

## 12. Anti-patterns

**A1 — No locked seed.** Generating with random seed every time. Drift guaranteed.

**A2 — No reference image input.** Generating from prompt alone. Each episode produces a new face.

**A3 — Prompt evolution without tracking.** Adding details to the prompt episode-by-episode. Face changes accumulate.

**A4 — Mid-channel model switch without re-anchoring.** Switching from Nano Banana to Flux mid-life and not regenerating canonical reference.

**A5 — Aspect ratio change without crop validation.** Cropping a 16:9 face for 9:16 thumb without verifying proportions.

**A6 — Forgetting to update character bible.** Approved variations not logged. Future renders re-invent variations.

**A7 — Drift denial.** Audience comments "did the host change?" and the agent / user ignores it. Drift compounds.

**A8 — Over-generating ad-hoc.** Every episode generates fresh because "the locked variations don't fit." This is symptom of insufficient initial variation library.

**A9 — Wardrobe explosion.** New shirt every episode. Audience can't anchor on the host's "look."

**A10 — Photoshop without lighting match.** Face-swap with mismatched lighting reads uncanny.

## 13. Worked examples

### Example 1 — Personal-finance authority channel locked

- **Canonical reference:** HeyGen avatar render from the first episode, locked
- **Seed:** N/A (HeyGen avatar is deterministic from settings)
- **Approved variations:**
  - Calm authority expression (default)
  - Concerned expression (used for victim story moments)
  - Accusation expression (used for system/villain moments)
  - Warm authority expression (used for CTA / educational moments)
- **Wardrobe rotation:** Navy collared shirt (default), charcoal collared shirt, light cardigan over shirt
- **Setting:** Home office (single canonical), variation of book arrangement minor
- **Drift watch:** quarterly review

### Example 2 — Linda (recurring composite, retired teacher)

- **Canonical reference:** Generated at the composite's introduction episode
- **Seed:** locked
- **Approved variations:**
  - At kitchen table (default appearance)
  - At home study with bookshelf (alternate)
  - In armchair with letter (story-specific moment)
- **Wardrobe:** Soft gray cardigan over cream blouse (canonical), navy cardigan variant, sweater variant
- **Drift watch:** every appearance verified against canonical

### Example 3 — Brick-narrative storytelling character

- **Canonical reference:** Brick-toy character render with specific facial detail palette
- **Seed:** locked across all music video appearances
- **Approved variations:**
  - Default standing
  - Reacting / shocked
  - In motion
  - From different angles (front, three-quarter, profile)
- **Wardrobe:** Locked brick-toy outfit, no rotation
- **Drift watch:** track 1 specific facial feature (e.g., specific eyebrow shape) — easy drift indicator

## 14. Runtime checklist

Before any character render:
- [ ] Character bible loaded from channel memory
- [ ] Canonical reference image identified
- [ ] Locked seed identified
- [ ] Approved variation matched (or new variation explicitly approved)
- [ ] Same model + version used as canonical
- [ ] Reference image fed as input
- [ ] Output verified for face consistency
- [ ] If new variation: added to approved variations after user approval
- [ ] Usage logged in bible

After ship:
- [ ] Quarterly drift review scheduled
- [ ] Drift score computed against canonical
- [ ] Re-anchor if needed

## 15. Why these rules

Audience trust on AI-driven channels is asymmetric. **Consistency builds trust slowly. Drift breaks trust quickly.** A viewer who watches 5 episodes of a personal-finance authority channel and notices the host looks "different" each time develops subconscious wariness. They might not articulate why. They just stop coming back as often.

The compounding cost of drift is the largest invisible cost in AI YouTube. Channels that nail consistency over 50+ episodes build cumulative recognition that drives long-term subscriber retention. Channels that drift never reach that compounding.

The discipline is straightforward but the agent must be vigilant: lock the seed, lock the reference, lock the model, log every variation, run quarterly drift checks. None of these are interesting tasks. All of them are load-bearing.

## Update log

Current as of April 2026. Update when:
- Image gen models change face-preservation capabilities
- Face-swap / photoshop tools improve dramatically
- New continuity tools emerge (e.g., character LoRA training, persistent identity systems)
- The personal-finance authority drift mitigation reveals new lessons
