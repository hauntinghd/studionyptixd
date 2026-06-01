# Studio overrides vs Rookcast defaults

Rookcast skills assume the RookCast v1 provider stack. NYPTID Studio keeps the **playbooks and gates** but swaps providers where we have proven in-repo paths.

## Provider mapping

| Capability | Rookcast default | Studio default | Notes |
|------------|------------------|----------------|-------|
| Host still | Nano Banana | **Seedream v4.5 edit** | Locked ref: `host_v2.png` |
| Avatar lip-sync | HeyGen | **Creatify Aurora** (`fal-ai/creatify/aurora`) | Chunk ≤55s audio; Stable Avatar for long takes |
| Voice TTS | Minimax Speech-02 HD | **ElevenLabs** | Voice ID in CHANNEL.md |
| Thumbnail composite | Nano Banana / Flux | **Seedream v4.5 edit** + ref thumbs | See `long_form/generate_cryptic_ctr_ss_thumb.py` |
| Motion graphics | Remotion (Rookcast) | **`long_form/motion_graphics/`** | source_proof, stat_card |
| B-roll | Stock / i2v | **No stock B-roll** (Cryptic Rook) | Motion graphics + avatar only |
| Music | Suno v5.5 | Optional / lane-specific | Cryptic: light bed or none |
| Outlier research | Rookcast internal | **Catalyst corpus** | `analysis/claude_corpus_query.py` |

## Skill-specific swaps

### ai-host-setup
- Replace "HeyGen avatar setup" → Aurora chunk render from locked still
- Replace "Nano Banana" → Seedream v4.5 with host reference image

### heygen-avatar-video
- Studio equivalent: **`aurora-avatar-video`** (pending skill file) — same chunking, silence-kill, loudnorm from `build_cryptic_ctr_ss_rook.py`

### channel-onboarding
- Credit estimates: show **USD fal estimate** from `long_form/fal_pricing.py`, not Rookcast credits
- Archetype `avatar_authority_longform` → `pipeline_kind: cryptic_verified_rook` when verification policy applies

## Decision gates (unchanged)

These Rookcast gates apply verbatim in Studio:

- **Sample-then-confirm** for host (4 variants, user picks)
- **Thumbnail workflow** from CHANNEL.md (`user_picks` / `agent_picks` / `single_shot`)
- **Approval cadence** (`high_touch` vs `low_touch`)
- **Compliance preflight** for YMYL (full) vs entertainment (light)

## YouTube metadata

| Field | CrypticScience verified |
|-------|-------------------------|
| Category | **Entertainment** (ID 24) — competitor parity (Markus Graves lane) |
| Disclaimer | Educational only; not tax/legal/financial advice |
| Sources | Primary .gov URLs in description + pinned comment |
