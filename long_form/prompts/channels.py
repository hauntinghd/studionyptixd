"""
Per-channel canonical grammar registry for long-form generation.

Each entry locks the channel's signature so Grok / seedream / i2v all see
the SAME context and produce on-brand output. Adding a 7th channel = add
a key here + update the frontend channel picker.

Sourced from:
  project_channel_lanes_locked.md (2026-05-01)
  project_em_grammar_locked.md (2026-05-01)
  project_lacuna_dyatlov_pending.md
  project_v5_pipeline_locked.md (2026-04-24)
  feedback_hr_premium_fal_tts.md (HR voice rule)
  feedback_all_channels_photoreal_premium.md (photoreal default for non-skeleton)
"""
from __future__ import annotations
from typing import Any


CHANNELS: dict[str, dict[str, Any]] = {
    "lacuna": {
        "key": "lacuna",
        "label": "We Are Lacuna",
        "tagline": "Fully-AI sci-fi/horror documentaries — LEMMiNO grade",
        "icon": "🪐",
        "format": "long_form",
        "channel_id": "UCYsJtSyepSf6MD7MS2GJ8vA",
        "default_minutes": 60,
        "fps": 24,
        "image_model_default": "seedream_45",
        "i2v_model_default": "kling_v21_standard",   # cinematic 24fps
        "voice_provider_default": "elevenlabs",
        "voice_id_default": "",                       # falls back to Brian
        "cost_estimate_usd": 120.0,                   # $61 base / $120-160 all-in
        "system_prompt": (
            "You write 60-minute LEMMiNO-style sci-fi/horror documentary scripts "
            "for the We Are Lacuna channel. Tone: cold, clinical, methodical "
            "investigation of paranormal/unexplained events. Diegetic narration — "
            "the narrator is a researcher reading their case notes. Each chapter "
            "opens with a cold-open hook, builds with archival-style evidence, "
            "ends with an unsettling unanswered question. NO sponsor reads, no "
            "audience addresses. ~150 wpm, ~9000 narrated words target."
        ),
        "visual_style": (
            "Photoreal cinematic 24fps. Dimly-lit moody compositions, cold blue "
            "+ amber accent palette, fog and atmospheric haze, no human faces "
            "(silhouettes and over-shoulder only). LEMMiNO color grade with "
            "deep shadows and milky highlights."
        ),
    },
    "hidden_cortex": {
        "key": "hidden_cortex",
        "label": "Hidden Cortex",
        "tagline": "Psychology, consciousness, and the science of mind",
        "icon": "🧠",
        "format": "long_form",
        "channel_id": "",                             # not OAuth'd yet
        "default_minutes": 25,
        "fps": 30,
        "image_model_default": "seedream_45",
        "i2v_model_default": "kling_v21_standard",
        "voice_provider_default": "elevenlabs",
        "voice_id_default": "",
        "cost_estimate_usd": 35.0,
        "system_prompt": (
            "You write 20-30 minute educational mind/consciousness documentaries "
            "for the Hidden Cortex channel. Tone: curious, scholarly but "
            "approachable — Vsauce + The Why Files + Inside Job. Each chapter "
            "introduces a psychological phenomenon, walks through the canonical "
            "studies + numbers, ends with a 'so what?' implication for the "
            "viewer. Specific names, dates, percentages. No sponsor reads."
        ),
        "visual_style": (
            "Photoreal cinematic 30fps. Brain/neuron macro shots, lab equipment, "
            "vintage academic settings, soft warm lighting on archival textures. "
            "Avoid stock-shutterstock look — go cinematic editorial."
        ),
    },
    "pb_live": {
        "key": "pb_live",
        "label": "PB Live",
        "tagline": "Criminal cases, CIA operations, true-crime deep dives",
        "icon": "🔍",
        "format": "long_form",
        "channel_id": "UCO7hTodSkBNszjiPdTMrrKA",     # stored as 'PB Lies' in OAuth store
        "default_minutes": 30,
        "fps": 30,
        "image_model_default": "seedream_45",
        "i2v_model_default": "kling_v21_standard",
        "voice_provider_default": "elevenlabs",
        "voice_id_default": "",
        "cost_estimate_usd": 45.0,
        "system_prompt": (
            "You write 25-35 minute true-crime and intelligence-operation "
            "documentaries for the PB Live channel. Tone: investigative "
            "journalism — Cold Case Files crossed with Coldfusion. Each "
            "chapter walks one segment of the case timeline: setup, key "
            "evidence, twist, resolution. Use real names always. Concrete "
            "dates, locations, dollar amounts, agency designations."
        ),
        "visual_style": (
            "Photoreal cinematic 30fps. Surveillance camera grain, manila "
            "case files, redacted documents, dimly lit interrogation rooms. "
            "Forensic-grade detail. No editorial cartoons."
        ),
    },
    "lofi_radio": {
        "key": "lofi_radio",
        "label": "Lo-Fi Radio",
        "tagline": "Extended ambient music with looping cinematic visuals",
        "icon": "🎶",
        "format": "long_form",
        "channel_id": "",                             # not OAuth'd yet
        "default_minutes": 60,
        "fps": 30,
        "image_model_default": "seedream_45",
        "i2v_model_default": "pixverse_v6",          # cheap looping motion
        "voice_provider_default": "none",             # no narration
        "voice_id_default": "",
        "cost_estimate_usd": 20.0,
        "system_prompt": (
            "You design lo-fi music station visual concepts. There is NO "
            "narration — output only the chapter list as a series of long-hold "
            "visual scenes (3-5 minutes each), each describing a quiet moody "
            "tableau (rainy window, vinyl record spinning, cat asleep on "
            "warm laptop, neon sign at night). 8-12 scenes total."
        ),
        "visual_style": (
            "Anime / lo-fi illustration aesthetic. Slow cinematic 30fps. "
            "Low saturation warm palette (amber, dusty pink, deep blue). "
            "Gentle camera drift, no cuts, no fast motion. Rainy windows, "
            "cozy interior lighting, subtle ambient particles (dust, embers)."
        ),
    },
    "empire_magnates": {
        "key": "empire_magnates",
        "label": "Empire Magnates",
        "tagline": "Red porcelain mannequin + photoreal financial-scam epics",
        "icon": "💼",
        "format": "long_form",
        "channel_id": "UCA_cn0-EW2UbBsyEA0TahNA",
        "default_minutes": 20,
        "fps": 24,
        "image_model_default": "seedream_45",
        "i2v_model_default": "ltx_13b",               # locked winner per memory
        "voice_provider_default": "elevenlabs",
        "voice_id_default": "",
        "cost_estimate_usd": 50.76,                   # locked v5 number
        "system_prompt": (
            "You write 20-minute photoreal financial-fraud / corporate-empire "
            "documentary scripts for the Empire Magnates channel. Tone: "
            "Lume-emulating long-form (porcelain-mannequin cast inside "
            "documentary methodology). Hook formula MUST be: "
            "'In [year], a [profession] walked into a [mundane location] "
            "and legally [verb] $[amount] from [target].' "
            "Use real names. Dollar amounts to the cent. Diegetic narration."
        ),
        "visual_style": (
            "Photoreal cinematic 24fps. Red-porcelain stylized mannequin "
            "characters integrated INTO real-world environments (boardrooms, "
            "courtrooms, trading floors). Red porcelain heads, real opaque "
            "tailored clothing. Coldfusion color grade — deep blacks, neutral "
            "midtones, restrained saturation."
        ),
    },
    "history_rewind": {
        "key": "history_rewind",
        "label": "History Rewind",
        "tagline": "9-hour sleep documentaries — premium fal MiniMax narration",
        "icon": "🏛️",
        "format": "long_form",
        "channel_id": "UCHmwsIGud6CeZ3CIs5cuaUA",
        "default_minutes": 540,                       # 9 hours
        "fps": 30,
        "image_model_default": "seedream_45",
        "i2v_model_default": "ken_burns",              # zoom/pan stills, no real i2v
        "voice_provider_default": "fal_minimax",
        "voice_id_default": "",
        "cost_estimate_usd": 35.0,                    # fal MiniMax 9hr block
        "system_prompt": (
            "You write 9-hour sleep documentary scripts for the History "
            "Rewind channel. Tone: gentle, slow, methodical — designed for "
            "falling asleep to. Each chapter (~30 min) walks one era or "
            "dynasty in calm chronological order with specific names, dates, "
            "and outcomes. NO startling moments, NO loud transitions. ~120 "
            "wpm calm narration. Total ~65000 narrated words."
        ),
        "visual_style": (
            "Cinematic 30fps Ken-Burns stills (no full i2v). Ancient world "
            "photoreal — period-correct architecture, costume, props, soft "
            "warm lighting. Slow zoom + pan only, no fast motion."
        ),
    },

    # ────────────────────────────────────────────────────────────────────
    # Shorts channels — surfaced in the LongForm panel for ANALYTICS only.
    # The render pipeline for these defers to the existing skeleton_ai/
    # short-form module (Create tab); these entries exist so the user can
    # see Catalyst Hub data for every connected channel in one panel.
    # ────────────────────────────────────────────────────────────────────
    "zerotier": {
        "key": "zerotier",
        "label": "ZeroTier",
        "tagline": "Comic-book / DC-character shorts — Conflict Arc retention formula",
        "icon": "⚡",
        "format": "shorts",
        "channel_id": "UC9Gth_4MVet6rdPH7MHJf-g",
        "default_minutes": 0.6,                        # ~36s shorts
        "fps": 30,
        "image_model_default": "seedream_45",
        "i2v_model_default": "kling_v21_standard",
        "voice_provider_default": "elevenlabs",
        "voice_id_default": "",
        "cost_estimate_usd": 1.40,                     # standard skeleton-AI shorts cost
        "system_prompt": (
            "ZeroTier is a comic-book / DC-character shorts channel. The "
            "render path for shorts goes through the Skeleton AI pipeline "
            "in the Create tab, not Long Form. This entry exists so Catalyst "
            "data for the channel surfaces in the Long Form analytics view."
        ),
        "visual_style": (
            "See skeleton_ai/prompts/base_style.py — anatomical white skull "
            "+ mint backdrop + real opaque clothing + canonical comic costume."
        ),
    },
    "cryptic_science": {
        "key": "cryptic_science",
        "label": "Cryptic Science",
        "tagline": "Skeleton AI shorts (Human Limits / Marvel vs DC / etc.)",
        "icon": "💀",
        "format": "shorts",
        "channel_id": "UCOHnksm14B-9AqGhlpRxG5A",
        "default_minutes": 1.0,                        # ~60s shorts
        "fps": 30,
        "image_model_default": "seedream_45",
        "i2v_model_default": "seedance_2_0",
        "voice_provider_default": "elevenlabs",
        "voice_id_default": "",
        "cost_estimate_usd": 1.40,
        "system_prompt": (
            "Cryptic Science is the skeleton-AI shorts channel. Render path "
            "is the Skeleton AI pipeline in the Create tab. This entry exists "
            "so Catalyst data surfaces alongside the long-form channels."
        ),
        "visual_style": "See skeleton_ai/prompts/base_style.py for the locked spec.",
    },
    "lexi_manhwa": {
        "key": "lexi_manhwa",
        "label": "Lexi Manhwa",
        "tagline": "Manhwa recap shorts — pending pipeline rebuild",
        "icon": "📖",
        "format": "shorts",
        "channel_id": "UCbtE_YDmqWZX2OfaKi0QHnA",
        "default_minutes": 1.0,
        "fps": 30,
        "image_model_default": "seedream_45",
        "i2v_model_default": "pixverse_v6",
        "voice_provider_default": "elevenlabs",
        "voice_id_default": "",
        "cost_estimate_usd": 1.20,
        "system_prompt": (
            "Lexi Manhwa is a manhwa-recap shorts channel. Pipeline rebuild "
            "pending. This entry exists for Catalyst analytics visibility."
        ),
        "visual_style": "Manhwa recap pipeline (project_manhwa_recap_pipeline.md).",
    },
}


def list_channels(format_filter: str | None = None) -> list[dict[str, Any]]:
    """Return channels in UI render order. format_filter='long_form' or 'shorts' to
    restrict; None returns both."""
    out = []
    for v in CHANNELS.values():
        fmt = v.get("format", "long_form")
        if format_filter and fmt != format_filter:
            continue
        out.append({
            "key": v["key"],
            "label": v["label"],
            "tagline": v["tagline"],
            "icon": v["icon"],
            "format": fmt,
            "channel_id": v.get("channel_id", ""),
            "default_minutes": v["default_minutes"],
            "fps": v["fps"],
            "image_model_default": v["image_model_default"],
            "i2v_model_default": v["i2v_model_default"],
            "voice_provider_default": v["voice_provider_default"],
            "cost_estimate_usd": v["cost_estimate_usd"],
        })
    return out


def get_channel(key: str) -> dict[str, Any]:
    if key not in CHANNELS:
        raise ValueError(f"unknown channel {key!r}. valid: {sorted(CHANNELS.keys())}")
    return CHANNELS[key]
