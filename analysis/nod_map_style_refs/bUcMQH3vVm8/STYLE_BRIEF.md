# Nod Map — Visual Style Brief (from frame extract)

**Source:** [Where Are the Missing Scientists Actually Going?](https://www.youtube.com/watch?v=bUcMQH3vVm8) · **Nod Map** · 4K (3840×2160) · ~12 min  
**Frames:** `analysis/nod_map_style_refs/bUcMQH3vVm8/frames/`

---

## What they actually do (not photoreal)

| Layer | Nod Map approach | Your takeaway |
|-------|------------------|---------------|
| **Characters** | Smooth **white faceless 3D mannequins** — no eyes/mouth, matte plastic/clay | Lock **one cast model**; reuse every scene |
| **Wardrobe** | Lab coats, suits — simple geometry, same body proportions | 2–3 outfit variants max (lab / suit / casual) |
| **Environment** | **Changes every beat** — lab, void+classified board, corkboard, office diorama | **Only swap background + props** per scene |
| **Lighting** | Cold fluorescent OR high-contrast red/black “classified” void | Pick one palette per video, not per scene |
| **Graphics** | Title cards (`CLASSIFIED INFORMATION`), name plates, polaroid pins | Chapter cards + one on-screen number (your playbook) |
| **Quality** | Clean 3D render — **not** AI photoreal people | Seedream t2i + **edit-with-refs** for consistency |

They do **not** regenerate a unique human model per shot. They reuse the **same white mannequin mesh** in different dioramas.

---

## Frame references

| Frame | Time | Beat type |
|-------|------|-----------|
| `frame_1_0-05.png` | 0:05 | Cold open — empty lab, 2 mannequins, worm’s-eye, ceiling fluorescents |
| `frame_2_0-15.png` | 0:15 | Classified board — mugshots + red laser ring + crowd of mannequins |
| `frame_4_1-00.png` | 1:00 | Character intro — waist-up mannequin, name plate UI, purple rim light |
| `frame_5_2-00.png` | 2:00 | Evidence corkboard — polaroids of same silhouette, red `UNTIMELY DEATH` banner |

---

## PB Lies brand adaptation (not a Nod Map clone)

- **Cast:** white matte 3D investigative figure (faceless) — **forensic red** accent (stamp glow, laser line) not Nod’s pure red void only
- **Watermark:** `PB LIES` bottom-left (small, teal-red like channel thumb spec)
- **Scene 1 (Mockingbird):** manila folder + redacted CIA document on black reflective floor, one mannequin silhouette examining pages — **not** photoreal faces
- **4K path:** generate stills at **1920×1080 minimum** (Seedream auto_2K); upscale final encode to 2160p in ffmpeg later — don’t pay for 4K gen on every still yet

---

## Cost strategy ($44.56 fal budget)

1. **Cast ref sheet** (3 angles): 3 × $0.04 = **$0.12**
2. **Each scene still** (edit + refs, background swap only): **$0.04**
3. **LTX clip** per scene: **$0.04**
4. **15-min PB (~36 scenes):** ~$0.12 cast + 36×($0.04+$0.04+$0.008 sfx) ≈ **~$3 fal** (matches live calculator)

**Rule:** Never t2i a full character from scratch per scene — always **Seedream edit** with approved cast refs.
