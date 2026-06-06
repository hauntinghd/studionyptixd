# Rook / RookCast format brief (from qxvumPV5ims)

**Reference:** [How I made $47,333 in two months posting AI slop](https://www.youtube.com/watch?v=qxvumPV5ims) (Rook, ~25 min tutorial)

## What actually wins (NOT stock B-roll VO)

| Layer | What Rook uses | What we tried (wrong) |
|-------|----------------|----------------------|
| **A-roll** | HeyGen AI avatar at desk + mic (Marcus Graves) | ElevenLabs VO + Pexels clips |
| **B-roll** | Simple **motion graphics** (text cards, stats, headlines) | Generic stock footage |
| **Assembly** | RookCast pipeline (script → avatar → gfx → publish) | ffmpeg drawtext on stock |

## Video structure (content video, not tutorial)

1. **Hook (0–30s):** Avatar on camera — direct question / claim tied to search intent
2. **Body:** Alternate **avatar talking** (~60%) + **motion graphic beats** (~40%)
3. **Sources:** Motion graphic cards with `SOURCE: Google Search Blog · May 19, 2026`
4. **Close:** Avatar CTA — comment + subscribe, no pitch

## Motion graphic types (from ref + our stat_card.py)

- Big stat reveal (`1B` AI Mode users, `2.5B` AI Overviews)
- Headline card mimicking publisher (GOOGLE / SEARCH · headline text)
- Timeline (`Live now` → `Summer 2026` → `Pro/Ultra first`)
- Checklist: `FREE` vs `PAID` features

## Avatar spec (CrypticScience host — not Marcus clone)

- Professional male, 35–50, neutral American accent
- Desk + broadcast mic, bookshelf/blur background
- **HeyGen** (Rook's stack) — needs `HEYGEN_API_KEY`
- Alternate: Hedra / fal lip-sync if HeyGen unavailable

## CrypticScience Google AI Mode — beat map (8–10 min)

| Beat | Type | Content |
|------|------|---------|
| 1 | Avatar | Hook: what changed May 19 (not "Google killed Search") |
| 2 | MG | `1B` / `2.5B` user stats (verified) |
| 3 | Avatar | AI Mode vs AI Overviews |
| 4 | MG | Three things live today (Flash, search box, Overview handoff) |
| 5 | Avatar | Information agents explained |
| 6 | MG | Timeline: summer rollout, Pro/Ultra first |
| 7 | Avatar | Generative UI + Personal Intelligence |
| 8 | MG | FREE vs PAID table |
| 9 | Avatar | What it means for you (searcher + creator) |
| 10 | Avatar | Close + sources in description |

## Tools needed to build v3

1. **HeyGen API** (or user-provided avatar MP4 + lip-sync)
2. **motion_graphics/** (`StatCard`, `NewsCard`, `TimelineCard`, `CounterCard`) — already in repo
3. **ElevenLabs** — script timing / fallback audio for avatar gen
4. **ffmpeg** — interleave avatar clips + MG clips, loudnorm

## Do NOT use

- Pexels stock as primary visual
- Voiceover with random B-roll
- Skeleton AI / 3D mannequins for this niche
