"""
Per-channel canonical grammar registry for long-form generation.

Each entry locks the channel's signature so Grok / seedream / i2v all see
the SAME context and produce on-brand output. Adding a 7th channel = add
a key here + update the frontend channel picker.

Per-channel renderable fields used downstream:
  system_prompt          — Grok system prompt for outline + chapter expansion
  visual_style           — appended to image-gen prompts
  title_template         — REQUIRED title shape (winner-pattern decoded from
                           competitor analysis); outline pass enforces this
  description_tail       — appended to YouTube description (e.g. HR's
                           proven 'Human Voiced, No Ads' signal worth 2,090 v/d)
  thumbnail_style_prompt — passed to seedream when generating cover art

Sourced from:
  project_channel_lanes_locked.md (2026-05-01)
  project_em_grammar_locked.md (2026-05-01)
  project_lacuna_dyatlov_pending.md
  project_v5_pipeline_locked.md (2026-04-24)
  feedback_hr_premium_fal_tts.md (HR voice rule)
  feedback_all_channels_photoreal_premium.md (photoreal default for non-skeleton)
  D:/recaps/history_rewind/competitor_decode_2026-05-07.md (HR title-format winners)
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
        "pipeline_kind": "v5_episode",                # cinematic LTX + EL + silence-kill
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
        # Title pattern decoded from LEMMiNO's top performers (Bigfoot, MH370,
        # Voynich Manuscript, Bermuda Triangle, Black Knight Satellite). Short,
        # declarative, mystery-noun first. The channel's framing is
        # 'investigation' so we suffix with that to brand-mark.
        "title_template": (
            "The {Subject} — {Hook} | A Lacuna Investigation"
        ),
        "title_examples": [
            "The Dyatlov Pass — Nine Hikers, One Mountain, No Survivors | A Lacuna Investigation",
            "The Voynich Manuscript — A Book Nobody Can Read | A Lacuna Investigation",
            "Area 51 — What's Actually Inside | A Lacuna Investigation",
        ],
        "title_avoid": [
            "Top 10",                # clickbait, kills LEMMiNO grade
            "You won't believe",     # tabloid voice
            "Watch what happens",    # listicle voice
            "Will Make You",         # listicle voice
        ],
        "description_tail": (
            "\n\nA Lacuna investigation. Subscribe for new mysteries weekly. "
            "Original research, archival sources cited in pinned comment."
        ),
        "thumbnail_style_prompt": (
            "Cinematic 16:9 documentary thumbnail, LEMMiNO-grade. ONE dominant "
            "subject (object, location, or silhouetted figure) center frame, "
            "dimly lit, cold blue + amber accent palette, atmospheric fog and "
            "haze, deep shadows. Bold sans-serif title overlay top — 2-4 words "
            "in caps. NO faces, NO smiling humans, NO clickbait arrows. "
            "Editorial-grade documentary cover."
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
        "pipeline_kind": "v5_episode",                # cinematic essay-style episodes
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
        # Title pattern decoded from Vsauce / Why Files / Inside Job top hits.
        # Two dominant variants:
        #   (a) named-effect declarative: "The {Effect} Effect"
        #   (b) curious-question: "What Happens When/If {Phenomenon}"
        # We default to (a) because named-effect titles compound (each video
        # builds the channel's vocabulary) and have stronger CTR baseline at
        # small-channel scale per the corpus_v2_104 analysis.
        "title_template": (
            "The {Effect} Effect: Why {Phenomenon}"
        ),
        "title_examples": [
            "The Halo Effect: Why We Trust Beautiful People Without Knowing Them",
            "The Bystander Effect: Why Nobody Calls the Cops",
            "The Dunning-Kruger Effect: Why Idiots Think They're Geniuses",
        ],
        "title_avoid": [
            "Top 5",                  # listicle, weak CTR for science
            "Mind-blowing",           # generic clickbait
            "Will Change Your Life",  # tabloid voice
            "10 Things You Didn't Know",  # legacy listicle
        ],
        "description_tail": (
            "\n\nHidden Cortex — the science of mind, no fluff. "
            "Studies cited in description below. Subscribe for weekly deep dives."
        ),
        "thumbnail_style_prompt": (
            "Cinematic 16:9 editorial-science thumbnail. Macro shot of brain "
            "tissue, neurons firing, or vintage neuroscience equipment "
            "(EEG cap, MRI scanner cross-section, lab notebook with "
            "diagrams). Soft warm tungsten lighting on archival textures, "
            "shallow depth of field. Bold serif title overlay 2-4 caps "
            "words. NO talking-head shots, NO stock-photography people. "
            "Vsauce/Why Files editorial grade."
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
        "pipeline_kind": "v5_episode",                # forensic cinematic with SFX
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
        # Title pattern decoded from Coldfusion + Cold Case Files + Inside Job
        # top performers (Operation MK-Ultra, Operation Mockingbird, Berlin
        # Tunnel, Northwoods). Operation-name first when applicable, otherwise
        # subject:hook colon-form. Agency designation lifts CTR significantly
        # (CIA/FBI/NSA/KGB/MI6/Stasi all dominate the search/recommendation
        # signal for true-crime + intel niches per the IP/copyright memory).
        "title_template": (
            "{Operation_or_Case}: How the {Agency} {Action}"
        ),
        "title_examples": [
            "Operation Mockingbird: How the CIA Bought the American Press",
            "The Berlin Tunnel: How the CIA Tapped the KGB for 11 Months",
            "MK-Ultra: How the CIA Drugged 7,000 American Citizens",
        ],
        "title_avoid": [
            "Most Disturbing",          # tabloid voice
            "Top 10 Cases",             # listicle
            "What They Don't Want",     # conspiracy clickbait
            "EXPOSED",                  # tabloid caps
            "Crazy True Story",         # weak framing
        ],
        "description_tail": (
            "\n\nPB Live — investigative documentaries on declassified "
            "operations and unsolved cases. Sources cited in description. "
            "Subscribe for new case files weekly."
        ),
        "thumbnail_style_prompt": (
            "Cinematic 16:9 forensic-grade thumbnail. Manila case file with "
            "bold redacted sections + black-and-white surveillance photo "
            "or document close-up. Top-secret stamp + classification banner. "
            "Cold tungsten + amber underglow, deep blacks, 1970s-90s "
            "documentary aesthetic. Bold sans-serif 2-4 caps title overlay. "
            "NO smiling faces, NO crime-scene gore, NO clickbait arrows. "
            "Cold Case Files / Coldfusion editorial grade."
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
        "pipeline_kind": "v5_episode",                # routes to v5 sub-pipeline
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
        # Title pattern decoded from Lume's gold-standard "The Trader Who
        # Legally Stole $440M From Citibank in 18 Seconds" — the
        # 'Loophole / Legally Stole' magic-words pattern in
        # project_fern_lume_formulas.md. Profession + Verb + $ amount + Target
        # compresses the entire hook into 8-12 words and triggers high CTR
        # at small-channel scale (Casey's <100k tier).
        "title_template": (
            "The {profession} Who Legally {verb} ${amount} From {target}"
        ),
        "title_examples": [
            "The Trader Who Legally Stole $440M From Citibank",
            "The Auditor Who Legally Drained $1.6B From Wirecard",
            "The CEO Who Legally Vanished $12B From Sanjay Shah's Treasury Empire",
        ],
        "title_avoid": [
            "Rise and Fall",      # zero-velocity pattern
            "Full Documentary",   # zero-velocity pattern
            "The Story of",       # generic
            "Inside the",         # generic
        ],
        "description_tail": (
            "\n\nA Loophole Files investigation. "
            "Subscribe for new fraud + financial-empire deep dives weekly."
        ),
        "thumbnail_style_prompt": (
            "Photoreal cinematic 16:9 thumbnail. A red-porcelain stylized "
            "mannequin executive figure (smooth red ceramic head, real "
            "tailored navy suit) standing alone in a richly-lit boardroom or "
            "trading floor, looking at the camera. Coldfusion color grade — "
            "deep blacks, neutral midtones, restrained saturation. "
            "BOLD WHITE SANS-SERIF text overlay top: '$[AMOUNT]' in huge type, "
            "secondary line below: '[TARGET COMPANY]' in smaller red caps. "
            "High click-through-rate documentary-thriller cover. No watermarks."
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
        # Real cost: fal MiniMax (~478k chars × $0.10/1k = $47.80)
        # + 540 ernie-image scenes × $0.03 = $16.20
        # + 18 chapter LLM passes ($9) + outline/ambient/thumbnails ($0.40)
        # ≈ $73 all-in. Old $35 estimate excluded fal MiniMax.
        "cost_estimate_usd": 73.0,
        "pipeline_kind": "sleep_doc",                 # routes to sleep-doc sub-pipeline
        "system_prompt": (
            "You write 9-hour sleep documentary scripts for the History "
            "Rewind channel. Tone: gentle, slow, methodical — designed for "
            "falling asleep to. Each chapter (~30 min) walks one era or "
            "dynasty in calm chronological order with specific names, dates, "
            "and outcomes. NO startling moments, NO loud transitions. ~120 "
            "wpm calm narration. Total ~65000 narrated words. Open with a "
            "soft 'Drift off to sleep with...' line, never a cliffhanger or "
            "cold-open hook."
        ),
        "visual_style": (
            "Cinematic 30fps Ken-Burns stills (no full i2v). Ancient world "
            "photoreal — period-correct architecture, costume, props, soft "
            "warm lighting. Slow zoom + pan only, no fast motion."
        ),
        # Title pattern decoded from competitor_decode_2026-05-07.md:
        # 'History for Sleep' suffix = 93 hits, avg 543 v/d (vs Casey's
        # 'Rise and Fall ... Full Documentary | N Hours' = 2 hits avg 16 v/d).
        # Joe's Sleepy History 1.3M-view monster, History at Night 917K,
        # Bedtime History 611K all use this pattern. Pair with a topic
        # noun-phrase like the empire/dynasty + a dark-secret hook.
        "title_template": (
            "The {empire_or_topic} — {hook_noun_phrase} | History for Sleep | 9 Hours"
        ),
        "title_examples": [
            "The Khmer Empire — Angkor's Dark Secret | History for Sleep | 9 Hours",
            "The Phoenicians — The Sea Empire That Wrote the World | History for Sleep | 9 Hours",
            "The Inca Empire — The Last Days of the Sun Kings | History for Sleep | 9 Hours",
        ],
        "title_avoid": [
            "Rise and Fall",      # 2 hits in 264-video corpus, avg 16 v/d
            "Full Documentary",   # 0 winners in corpus
            "The Story of",       # generic, low signal
            "Complete History",   # generic
        ],
        # 'Human Voiced, No Ads' — proven 2,090 v/d signal in
        # competitor_decode_2026-05-07.md (n=30 hits). Goes in description
        # tail since the title_template is already capped at 9 Hours suffix.
        "description_tail": (
            "\n\nHuman Voiced, No Ads. "
            "Drift off to sleep with the full story — told slowly, "
            "from beginning to end."
        ),
        # Thumbnail style decoded from ASMR Historian 9h winners + History at
        # Night 900K-view covers + Joe's Sleepy History 1.3M monster:
        # dark moody single-figure (robed/torch-lit), 3-word caps overlay,
        # corner '9 HOURS' badge. NOT golden-hour establishing shots
        # (Casey's old pattern, 0 winners).
        "thumbnail_style_prompt": (
            "Cinematic 16:9 sleep-documentary thumbnail. ONE dominant "
            "photoreal figure (robed historical figure or silhouetted "
            "ruler) center-left, dark moody torch-lit composition, deep "
            "shadows, occult or imperial symbol behind them. ASMR Historian "
            "/ History at Night style. Bold yellow-white sans-serif 3-word "
            "title caps overlay (e.g. 'KHMER EMPIRE' or 'ANGKOR SECRETS') "
            "filling top third. Small bright '9 HOURS' badge top-right "
            "corner. High contrast, no watermarks. Sleep-doc cover."
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
    restrict; None returns both.

    Surfaces (in addition to base config) the title_template, title_examples,
    description_tail snippet, and pipeline_kind so the frontend Render tab
    can show the user exactly what title shape Grok will produce + which
    sub-pipeline will run — no guessing.
    """
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
            # Per-PR-#119 hardening fields (new for HR + EM, empty/absent
            # for older entries — frontend renders only when present).
            "pipeline_kind": v.get("pipeline_kind", ""),
            "title_template": v.get("title_template", ""),
            "title_examples": list(v.get("title_examples") or []),
            "title_avoid": list(v.get("title_avoid") or []),
            "description_tail": v.get("description_tail", ""),
            "thumbnail_style_prompt": v.get("thumbnail_style_prompt", ""),
        })
    return out


def get_channel(key: str) -> dict[str, Any]:
    if key not in CHANNELS:
        raise ValueError(f"unknown channel {key!r}. valid: {sorted(CHANNELS.keys())}")
    return CHANNELS[key]


def channel_outline_prompt_extras(key: str) -> str:
    """Format the title_template + title_avoid + title_examples block as a
    system-prompt addendum to inject into long_form.scripting.generate_outline.

    Returns "" when the channel has no template configured (back-compat for
    Lacuna / Hidden Cortex / PB Live / Lo-Fi Radio which don't yet have
    decoded winner patterns).
    """
    rec = CHANNELS.get(key) or {}
    tpl = (rec.get("title_template") or "").strip()
    if not tpl:
        return ""
    lines = [
        "TITLE FORMAT — STRICT REQUIREMENT (decoded from competitor analysis "
        "+ this channel's own performance data; deviation reduces velocity 30x):",
        f"  Required pattern: {tpl}",
    ]
    examples = rec.get("title_examples") or []
    if examples:
        lines.append("  Working examples (match this exact shape):")
        for ex in examples[:3]:
            lines.append(f"    - {ex}")
    avoid = rec.get("title_avoid") or []
    if avoid:
        lines.append(
            "  NEVER use these phrases in the title (zero-velocity patterns): "
            + ", ".join(f'"{a}"' for a in avoid[:6])
        )
    desc_tail = (rec.get("description_tail") or "").strip()
    if desc_tail:
        # Compact whitespace for prompt context
        compact = " ".join(desc_tail.split())
        lines.append(
            "DESCRIPTION TAIL — append this to the YouTube description verbatim "
            f"(proven CTR signal): {compact}"
        )
    return "\n".join(lines)
