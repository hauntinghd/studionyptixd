"""Production defaults Studio Agent should enforce in plans and renders."""

from __future__ import annotations

# VO clearly audible over bed; not shouting (Magnates/Jake Tran documentary mix).
AUDIO_MIX_DEFAULTS = {
    "voice_gain": 1.0,
    "bgm_gain": 0.42,
    "ambience_gain": 0.15,
    "sfx_gain": 1.0,
    "ducking_note": (
        "Narration sits ~3–6 dB above background music. Music supports emotion per scene; "
        "never competes with VO. Avoid extreme sidechain pumping."
    ),
}

PACING_BENCHMARKS = {
    "premium_doc": {
        "label": "Jake Tran / Magnates / Lume documentary",
        "hook_window_sec": 8,
        "pattern_interrupt_sec": 55,
        "avg_shot_sec_target": 4.5,
    },
    "viral_short": {
        "label": "MrBeast / high-retention short",
        "hook_window_sec": 3,
        "pattern_interrupt_sec": 12,
        "avg_shot_sec_target": 2.5,
    },
    "story_manhwa": {
        "label": "Mamoru / manhwa recap",
        "hook_window_sec": 5,
        "pattern_interrupt_sec": 35,
        "avg_shot_sec_target": 5.0,
    },
}
