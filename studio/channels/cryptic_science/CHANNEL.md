# CrypticScience

> Verified high-RPM explainers — tax/IRS/banking + Social Security/Medicare (primary sources only)

## Archetype

`avatar_authority_longform` — verified variant (`cryptic_verified_rook`)

Steals Markus Graves title shape + production speed; every claim traces to a primary .gov source.

## Niche

`senior_finance_irs` + `benefits_ss_medicare` (see `long_form/cryptic_verified_lanes.py`)

## Audience

- Level: informed (US adults 45+, retirees, caregivers)
- Notes: Wary of fear-mongering; trust authentic-at-home host over studio polish

## Format

- Visual: Photoreal AI host (charcoal studio / home desk) + motion graphics (source_proof, stat_card, checklist)
- Length: 8–12 min (Rook build) · 10–14 min (Graves lane)
- Aspect: 16:9
- B-roll: **None** — avatar + motion graphics only

## Host

- Reference still: `D:/recaps/cryptic_science/ctr_ss_rook_v1/host_v2.png`
- Voice: ElevenLabs `onwK4e9ZLuTAKqWW03F9` (Daniel), speed 0.94
- Lip-sync: Creatify Aurora (`fal-ai/creatify/aurora`), chunked ≤55s
- Profession matrix row: **Tax pro / financial advisor**

## Provider preferences (Studio)

| Layer | Tool |
|-------|------|
| Host still | Seedream v4.5 edit |
| Avatar video | Creatify Aurora |
| Voice | ElevenLabs |
| Thumbnail | Seedream v4.5 edit + competitor ref thumbs |
| Motion gfx | `long_form/motion_graphics/stat_card.py` |

## Thumbnails

- Style: `bold_text_face` (Markus Graves rage-stamp register)
- Recurring: Host punch-in right; parchment/alert left panel; dollar specificity in headline
- Workflow: **`user_picks`** — generate 3 Seedream variants, user selects (skip text-glitch variants)

## Approval cadence

`high_touch` for title + thumbnail + source list; `low_touch` optional for script if sources pre-verified

## Compliance

- Domain 4 (financial advice) + Domain 5 (institutional naming) — **full preflight**
- Mandatory: educational disclaimer, consult CPA/attorney, primary source URLs in description
- Forbidden: unsourced "they don't want you to know" unless reframed as fact-check

## YouTube upload defaults

- Category: **Entertainment** (24)
- Tags: see `UPLOAD_TAGS_BASE` in `cryptic_verified_lanes.py`
- Chapters + pinned comment with source links

## Credit estimate

~$40–70 USD fal per Rook episode (Aurora + Seedream); ~$15–25 Graves avatar-only

## Pipeline

- `pipeline_kind`: `cryptic_verified_rook`
- CLI: `python long_form/build_cryptic_ctr_ss_rook.py`
- Latest ship: `D:/recaps/cryptic_science/ctr_ss_rook_v1/`

## Reference channels

- Markus Graves `$10K bank rule`: https://www.youtube.com/watch?v=UfLIPjy0F_4
- Rook HeyGen reference: https://www.youtube.com/watch?v=qxvumPV5ims

## Notes

- CTR + SS combo videos perform when title merges two search intents with "(Verified)" suffix
- Motion graphics must quote exact .gov line + URL + retrieval date
