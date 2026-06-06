"""CrypticScience Google AI Mode — Rook-style beat map (~10 min).

Reference: https://www.youtube.com/watch?v=qxvumPV5ims
Avatar ~45% | Motion graphics ~30% | Verified source proof B-roll ~25%
"""
from __future__ import annotations

from long_form.cryptic_google_ai_mode_script import SCENES

TITLE = "What Google AI Mode Actually Changes (Verified — I/O 2026)"

HOST_IMAGE_PROMPT = (
    "4K photorealistic male tech news host age 40, navy blue button-down shirt, clean short hair, "
    "direct eye contact with camera. Medium close-up shoulders-up crop — face fills upper two-thirds. "
    "Small black broadcast microphone at lower edge of frame only, never blocking face or chest. "
    "Dark charcoal grey seamless studio backdrop with subtle gradient, no bookshelf clutter. "
    "Soft uniform front key light, even exposure, no golden-hour side sun or lens flare. "
    "Ultra-sharp focus on eyes, shallow depth of field, professional YouTube explainer, 16:9"
)

AVATAR_MOTION = (
    "4K studio interview, medium close-up shoulders-up crop. Dark charcoal seamless backdrop, "
    "uniform soft key-light, no lighting change. Presenter faces lens with steady eye-contact. "
    "Hands remain below frame, body perfectly still. Minimal natural head movement while speaking. "
    "Ultra-sharp, broadcast explainer quality."
)

STABLE_AVATAR_PROMPT = (
    "A person is in a relaxed seated position. As the video progresses, the character speaks while "
    "arm and body movements are minimal and consistent with a natural speaking posture. Hand "
    "gestures remain minimal. Don't blink too often. Preserve background integrity matching the "
    "reference image's spatial configuration, lighting conditions, and color temperature."
)

_NARR = {s["id"]: s["narration"] for s in SCENES}

_BLOG = "https://blog.google/products-and-platforms/products/search/search-io-2026/"
_KEYNOTE = "https://blog.google/innovation-and-ai/sundar-pichai-io-2026/"

BEATS = [
    {"id": "01_hook", "type": "avatar", "narration": _NARR["01_hook"]},
    {
        "id": "01b_proof_io",
        "type": "motion",
        "mg": "source_proof",
        "duration_sec": 8.0,
        "mg_args": {
            "url": _BLOG,
            "page_title": "A new era for AI Search",
            "author": "Elizabeth Reid, VP of Search",
            "date_str": "May 19, 2026",
            "quote": (
                "Today at I/O we shared the latest on AI Mode in Search — "
                "including Gemini 3.5 Flash, a rebuilt search box, and Search agents."
            ),
            "highlights": ["AI Mode", "Gemini 3.5 Flash"],
        },
    },
    {
        "id": "02_stat_aimode",
        "type": "motion",
        "mg": "counter",
        "duration_sec": 8.0,
        "mg_args": {
            "final_value": 1,
            "prefix": "",
            "suffix": "B+",
            "label": "AI MODE MONTHLY USERS",
            "source": "Google Search Blog · May 19, 2026",
            "accent_color": (80, 160, 255),
        },
    },
    {
        "id": "02b_proof_1b",
        "type": "motion",
        "mg": "source_proof",
        "duration_sec": 8.0,
        "mg_args": {
            "url": _BLOG,
            "page_title": "AI Mode user growth",
            "author": "Elizabeth Reid, VP of Search",
            "date_str": "May 19, 2026",
            "quote": (
                "AI Mode surpassed 1 billion monthly users in its first year, "
                "with queries more than doubling every quarter since launch."
            ),
            "highlights": ["1 billion", "doubling every quarter"],
        },
    },
    {"id": "03_what_is", "type": "avatar", "narration": _NARR["02_what_is"]},
    {
        "id": "04_stat_overviews",
        "type": "motion",
        "mg": "counter",
        "duration_sec": 8.0,
        "mg_args": {
            "final_value": 2.5,
            "suffix": "B",
            "label": "AI OVERVIEWS MONTHLY USERS",
            "source": "Sundar Pichai · I/O 2026 Keynote",
            "decimals": 1,
            "accent_color": (235, 180, 60),
        },
    },
    {
        "id": "04b_proof_pichai",
        "type": "motion",
        "mg": "source_proof",
        "duration_sec": 8.0,
        "mg_args": {
            "url": _KEYNOTE,
            "page_title": "Google I/O 2026 Keynote",
            "author": "Sundar Pichai, CEO",
            "date_str": "May 19, 2026",
            "quote": "AI Overviews now have 2.5 billion monthly active users.",
            "highlights": ["2.5 billion", "AI Overviews"],
        },
    },
    {
        "id": "05_compare_modes",
        "type": "motion",
        "mg": "compare",
        "duration_sec": 9.0,
        "mg_args": {
            "headline": "AI OVERVIEWS VS AI MODE",
            "left_title": "AI OVERVIEWS",
            "right_title": "AI MODE",
            "left_items": [
                "Short summary on results page",
                "2.5B monthly users (Pichai)",
                "Entry point from classic Search",
            ],
            "right_items": [
                "Full conversational thread",
                "1B+ users in first year (Reid)",
                "Multimodal + agents testbed",
            ],
            "source": "Google I/O 2026 · Reid + Pichai",
        },
    },
    {"id": "06_live_today", "type": "avatar", "narration": _NARR["03_live_today"]},
    {
        "id": "06b_proof_searchbox",
        "type": "motion",
        "mg": "source_proof",
        "duration_sec": 8.0,
        "mg_args": {
            "url": _BLOG,
            "page_title": "Search box redesign",
            "author": "Google Search Blog",
            "date_str": "May 19, 2026",
            "quote": (
                "The biggest upgrade to the search box in 25 years — "
                "accepting text, images, files, video, and Chrome tabs in one query."
            ),
            "highlights": ["25 years", "Chrome tabs"],
        },
    },
    {
        "id": "07_headline_flash",
        "type": "motion",
        "mg": "news",
        "duration_sec": 8.0,
        "mg_args": {
            "publisher": "GOOG",
            "section": "SEARCH · I/O 2026",
            "headline": "Gemini 3.5 Flash Now Default in AI Mode Globally",
            "highlight": "AI Mode",
            "date_str": "MAY 19, 2026",
            "show_attribution": "GOOGLE SEARCH BLOG",
        },
    },
    {
        "id": "08_check_live",
        "type": "motion",
        "mg": "checklist",
        "duration_sec": 10.0,
        "mg_args": {
            "title": "LIVE NOW WORLDWIDE",
            "items": [
                "Gemini 3.5 Flash default in AI Mode",
                "Rebuilt multimodal Search box",
                "AI Overview → AI Mode handoff",
            ],
            "source": "Google Search Blog · May 19, 2026",
        },
    },
    {"id": "09_agents", "type": "avatar", "narration": _NARR["04_agents"]},
    {
        "id": "09b_proof_agents",
        "type": "motion",
        "mg": "source_proof",
        "duration_sec": 8.0,
        "mg_args": {
            "url": _BLOG,
            "page_title": "Search agents",
            "author": "Google Search Blog",
            "date_str": "May 19, 2026",
            "quote": (
                "Information agents monitor the web 24/7. Add keep me updated to a search. "
                "Launching summer 2026 — Google AI Pro and Ultra subscribers first."
            ),
            "highlights": ["24/7", "keep me updated"],
        },
    },
    {
        "id": "10_timeline",
        "type": "motion",
        "mg": "timeline",
        "duration_sec": 8.0,
        "mg_args": {
            "years": [2025, 2026, 2027],
            "event_year": 2026,
            "event_label": "SUMMER: AGENTS + GENERATIVE UI",
        },
    },
    {
        "id": "11_headline_agents",
        "type": "motion",
        "mg": "news",
        "duration_sec": 8.0,
        "mg_args": {
            "publisher": "GOOG",
            "section": "SEARCH AGENTS",
            "headline": "Information Agents Monitor the Web 24/7 — Keep Me Updated",
            "highlight": "24/7",
            "date_str": "SUMMER 2026 · PRO & ULTRA FIRST",
            "show_attribution": "GOOGLE SEARCH BLOG",
        },
    },
    {"id": "12_generative", "type": "avatar", "narration": _NARR["05_generative_ui"]},
    {
        "id": "12b_proof_ui",
        "type": "motion",
        "mg": "source_proof",
        "duration_sec": 8.0,
        "mg_args": {
            "url": _BLOG,
            "page_title": "Generative UI in Search",
            "author": "Google Search Blog",
            "date_str": "May 19, 2026",
            "quote": (
                "Generative UI capabilities arrive this summer for everyone, free of charge — "
                "Search can build charts, tables, and simulations on the fly."
            ),
            "highlights": ["free of charge", "Generative UI"],
        },
    },
    {
        "id": "13_headline_ui",
        "type": "motion",
        "mg": "news",
        "duration_sec": 8.0,
        "mg_args": {
            "publisher": "GOOG",
            "section": "GENERATIVE UI",
            "headline": "Search Builds Custom Layouts On the Fly — Charts, Tables, Simulations",
            "highlight": "On the Fly",
            "date_str": "FREE · SUMMER 2026",
            "show_attribution": "GOOGLE SEARCH BLOG",
        },
    },
    {"id": "14_personal", "type": "avatar", "narration": _NARR["06_personal"]},
    {
        "id": "14b_proof_personal",
        "type": "motion",
        "mg": "source_proof",
        "duration_sec": 8.0,
        "mg_args": {
            "url": _BLOG,
            "page_title": "Personal Intelligence expansion",
            "author": "Google Search Blog",
            "date_str": "May 19, 2026",
            "quote": (
                "Personal Intelligence expands to nearly 200 countries and 98 languages — "
                "no subscription required. Connect Gmail, Photos, and soon Calendar — opt-in only."
            ),
            "highlights": ["200 countries", "opt-in only"],
        },
    },
    {
        "id": "15_stat_countries",
        "type": "motion",
        "mg": "counter",
        "duration_sec": 8.0,
        "mg_args": {
            "final_value": 200,
            "suffix": "",
            "label": "COUNTRIES & TERRITORIES — PERSONAL INTELLIGENCE",
            "source": "Google Search Blog · May 19, 2026",
            "accent_color": (60, 210, 120),
        },
    },
    {
        "id": "16_stat_languages",
        "type": "motion",
        "mg": "counter",
        "duration_sec": 7.0,
        "mg_args": {
            "final_value": 98,
            "suffix": "",
            "label": "LANGUAGES SUPPORTED — NO SUBSCRIPTION REQUIRED",
            "source": "Google Search Blog · May 19, 2026",
            "accent_color": (180, 140, 255),
        },
    },
    {"id": "17_searchers", "type": "avatar", "narration": _NARR["07_searchers"]},
    {
        "id": "18_compare_cost",
        "type": "motion",
        "mg": "compare",
        "duration_sec": 10.0,
        "mg_args": {
            "left_title": "FREE FOR ALL",
            "right_title": "PRO / ULTRA FIRST",
            "left_items": [
                "Gemini 3.5 Flash in AI Mode",
                "Redesigned Search box",
                "Generative UI (summer)",
                "Personal Intelligence expansion",
            ],
            "right_items": [
                "Information agents (summer)",
                "Persistent mini-app dashboards",
                "Early access rollouts",
            ],
            "source": "Google Search Blog · May 19, 2026",
        },
    },
    {"id": "19_creators", "type": "avatar", "narration": _NARR["08_creators"]},
    {
        "id": "20_pct_citations",
        "type": "motion",
        "mg": "percentage",
        "duration_sec": 7.0,
        "mg_args": {
            "percentage": 100,
            "subtitle": "OF CLAIMS IN THIS VIDEO",
            "body": "SOURCED TO GOOGLE I/O POSTS — NOT THIRD-PARTY HYPE",
            "source": "CrypticScience verification policy",
            "accent_color": (80, 160, 255),
        },
    },
    {"id": "21_limits", "type": "avatar", "narration": _NARR["09_limits"]},
    {
        "id": "22_check_not_claimed",
        "type": "motion",
        "mg": "checklist",
        "duration_sec": 10.0,
        "mg_args": {
            "title": "GOOGLE DID NOT ANNOUNCE",
            "items": [
                "End date for traditional blue links",
                "Forced AI Mode on every query",
                "Auto-on Personal Intelligence",
                "Universal day-one agent access",
            ],
            "source": "Verified limits · May 19, 2026",
            "check_color": (235, 90, 90),
        },
    },
    {"id": "23_cta", "type": "avatar", "narration": _NARR["10_cta"]},
]
