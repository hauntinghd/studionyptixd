"""
Long-form pipeline module for Studio.

Mirrors the Skeleton AI module pattern but at episode scale (~10-60 min,
sometimes 9 hr for History Rewind sleep docs).

Six target channels (canonical specs in prompts/channels.py):
  1. We Are Lacuna       — fully-AI horror documentaries (LEMMiNO-style)
  2. Hidden Cortex       — psychology/consciousness/conspiracy docs
  3. PB Live             — criminal/CIA cases (photoreal)
  4. Lo-Fi Radio         — extended ambient music with looping cinematic visuals
  5. Empire Magnates     — yellow-porcelain mannequin + Fern fraud documentaries
  6. History Rewind      — 9-HOUR sleep documentaries

Pipeline (v6, building on locked v5 from project_v5_pipeline_locked.md):
  1. Channel selected → load canonical grammar (style + tone + voice + duration)
  2. Topic chosen — manual or seeded from Catalyst Hub top-performer suggestions
  3. Grok outline → chapter list with per-chapter beats
  4. Stills per beat via per-channel image model
  5. i2v per beat via per-channel motion model (LTX 60fps for EM, Kling 2.1
     Std for cinematic, Pixverse V6 for budget)
  6. Voiceover via per-channel TTS (fal MiniMax for HR, ElevenLabs for rest)
  7. v5 compose: silence-kill, Whisper word-timed callouts, mmaudio SFX,
     2-pass loudnorm, final mux

The legacy /api/longform/* + /api/creative/* routes remain alive (28 routes
serving the existing v5 sessions and shipped episodes — Wirecard, Mongol 9H,
Ottoman 9H). New /api/long-form/* (hyphenated) routes wrap them in the clean
module-router pattern that Skeleton AI uses, and become the canonical surface
once parity is verified.
"""
