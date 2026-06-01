# CrypticScience — per-video FLOW

Archetype: `avatar_authority_longform` (verified Rook)  
Approval cadence: **high_touch**  
Load skills per step from `studio/skills/`.

---

## Step 0 — Topic lock + source pack

**Skills:** `outlier-mining`, `compliance-preflight` (YMYL full)

- [ ] Pick topic from verified lane (`tax_irs_banking` or `benefits_ss_medicare`)
- [ ] Build `*_sources.json` — every claim maps to primary .gov URL
- [ ] **GATE:** User confirms source pack before script

---

## Step 1 — Title

**Skills:** `title-creation`

- [ ] Draft 3 titles using lane templates + competitor shape (Markus Graves)
- [ ] Include "(Verified)" suffix
- [ ] **GATE:** User picks title (`high_touch`)

---

## Step 2 — Thumbnail brief

**Skills:** `thumbnail-design`, `ai-host-setup`

- [ ] Download 1–2 competitor ref thumbs to `_thumb_ref/`
- [ ] Seedream v4.5 edit: host ref + layout ref → 3 variants
- [ ] **GATE:** User picks variant (`user_picks` workflow)

---

## Step 3 — Script

**Skills:** `script-writing`, `compliance-preflight`

- [ ] Structure: hook → definition → who affected → scenarios → what agency did NOT say → checklist + disclaimer
- [ ] ~145 wpm; scene list with motion_graphic beats
- [ ] **GATE:** User approves script OR sources pre-verified → skip (`low_touch` option)

---

## Step 4 — Storyboard / beats

**Skills:** `storyboard`

- [ ] Map scenes → avatar chunks + motion_graphic inserts
- [ ] Aurora chunk plan (≤55s audio per chunk)
- [ ] Write beats file (`*_rook_beats.py`)

---

## Step 5 — Voice

**Skills:** `voice-tts`

- [ ] ElevenLabs render per scene/chunk
- [ ] Silence-kill + loudnorm pass

---

## Step 6 — Avatar video

**Skills:** `aurora-avatar-video` (Studio; Rookcast: `heygen-avatar-video`)

- [ ] Aurora per chunk from locked host still
- [ ] A/V sync verify before assemble

---

## Step 7 — Motion graphics

**Skills:** `image-generation` (for cards if needed)

- [ ] source_proof cards, stat counters, checklists
- [ ] Overlay on avatar timeline

---

## Step 8 — Assembly

**Skills:** `audio-mix-assembly`, `captions`

- [ ] ffmpeg concat + mix
- [ ] Optional burned captions

---

## Step 9 — Upload pack

**Skills:** `description-writing`

- [ ] `UPLOAD_READY.txt` + `upload_pack.json`
- [ ] Category Entertainment (24)
- [ ] Chapters, tags, pinned comment with sources
- [ ] **GATE:** Final review before upload

---

## Anti-patterns

- Do not start Aurora before script + beats locked (wasted fal spend)
- Do not use stock B-roll
- Do not ship without primary source URLs in description
