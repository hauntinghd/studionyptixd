"""
Skeleton AI — short-form video pipeline for Cryptic Science niche.

Spec source: project_skeleton_spec_canonical.md (locked 2026-05-05 from
Casey's hand-curated reference videos in D:/recaps/do this/).

Pipeline:
  1. Topic prompt → Grok 4.1 Fast Reasoning script (10-12 beats, ~150 wpm)
  2. Beats → seedream v4.5 stills (mint green BG, anatomical skull, real clothes)
  3. Stills → Seedance 2.0 i2v (Kling 2.1 Pro upgrade for premium tier)
  4. Script → ElevenLabs TTS narration
  5. Compose → trim + 2-tier captions + watermark + concat + mux

Cost basis: ~$1.40 fal per standard short (5 AC) — see pricing tier doc.
"""
__version__ = "0.1.0"
