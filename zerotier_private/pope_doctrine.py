"""ZeroTier — Pope / Real World 2.0 Shorts doctrine (extracted from screen recording).

Source: jointherealworld.com tutorial on @therealworld2.0 (May 2026).
Benchmark video cited in training: "How Tristan Tate SAVED Top G From BBC"
  — 79.8% stayed to watch, 20.2% swiped away, 0:33, 2.9M engaged views.

ZeroTier channel reality (May 2026): 30–57% stayed → failing seed test.
This module encodes Pope's rules + ZT-specific targets for script/render/upload.
"""
from __future__ import annotations

import re
from typing import Any

# ── Verbatim caption lines from Pope training video ──────────────────────────
POPE_CAPTIONS: tuple[str, ...] = (
    "UP UNTIL THIS POINT, YOUR UNDERSTANDING HAS BEEN THEORY ONLY.",
    "OPEN YOUTUBE. TAP CREATE, THE PLUS BUTTON.",
    "IF THE VIDEO IS VERTICAL AND UNDER 60 SECONDS:",
    "YOUTUBE AUTOMATICALLY CLASSIFIES IT AS A SHORT.",
    "NOW, THE TITLE.",
    "YOUR TITLE MUST REINFORCE THE HOOK, NOT EXPLAIN THE VIDEO.",
    "THIS MISTAKE IS KILLING YOUR WEIGHT LOSS JOURNEY.",
    "AVOID LONG TITLES AND AVOID CLICKBAIT THAT LIES.",
    "THE DESCRIPTION IS OPTIONAL.",
    "THE BRAIN DETECTS DECEPTION INSTANTLY AND DECEPTION COLLAPSES TRUST.",
    "NOW YOUR SHORT MOST OF THE TIME WILL FALL INTO THE 'NOT FOR KIDS' CATEGORY.",
    "A TEXT ONLY THOUGHT, A PHOTO WITH SHORT CONTEXT,",
    "THEY REMIND THE AUDIENCE THAT THE CHANNEL IS ALIVE, ACTIVE AND INTENTIONAL.",
    "NEXT: HOW TO READ YOUR YOUTUBE ANALYTICS.",
    "I WANT YOU TO IGNORE VANITY METRICS.",
    "SWIPE RATE. 'DID ATTENTION STOP?'",
    "AND AUDIENCE RETENTION. 'DID ATTENTION HOLD?'",
    "THESE TWO METRICS DECIDE THE DISTRIBUTION.",
    "NOW, LOOK FOR PATTERNS:",
    "WHERE EXACTLY DO VIEWERS LEAVE?",
    "WHICH FORMAT REPEATS SUCCESS?",
    "NEVER GUESS. LET DATA DECIDE.",
    "THEN DO ONE THING:",
    "REPEAT WHAT WORKS. FIX WHAT FAILS.",
    "THIS STEP IS CRITICAL AND THIS IS WHERE MOST PEOPLE SABOTAGE THEMSELVES.",
    "NEXT: HOW TO WARM UP A YOUTUBE ACCOUNT CORRECTLY.",
)

POPE_SHORTS_DOCTRINE = """
POPE SHORTS DOCTRINE (Real World 2.0 — non-negotiable for ZeroTier):

DISTRIBUTION GATE — only two metrics matter for Shorts feed push:
  1. STAYED TO WATCH vs SWIPED AWAY — "Did attention stop?" (hook / frame 1)
  2. AUDIENCE RETENTION — "Did attention hold?" (pacing after the hook)
Ignore vanity views. A 2.3K flatline with 27% stayed is a hook failure, not a views failure.

BENCHMARK TO BEAT (Pope's reference Short, 33s):
  • 79.8% stayed to watch / 20.2% swiped away
  • Flat retention curve after hook (no mid-video lecture cliffs)
ZeroTier current band: 30–57% stayed. Target next uploads: ≥45%, stretch ≥55%.

TITLE RULES:
  • Title MUST REINFORCE THE HOOK — not explain the plot or summarize the video.
  • Good: "How Wally West Lost To Barry Allen" (names conflict + outcome)
  • Bad: "The Time Wally West Returned to a Wife Who Forgot Him" (story summary)
  • Avoid long titles (>60 chars). No lying clickbait — brain detects deception instantly.
  • Description is optional. Default audience: NOT made for kids.

RUNTIME & FORMAT:
  • Vertical, under 60s (YouTube auto-classifies as Short).
  • Pope benchmark = 33s. ZeroTier sweet spot: 28–34s, 4 scenes max.
  • **Tier P (preferred):** REAL comic panel scans + Ken Burns + VO only (~$0.10 fal or $0 with ElevenLabs).
  • Tier A (fallback): AI Seedream stills + Ken Burns (~$0.26) — use only when no panel exists.
  • Frame 1: biggest visual + ALL-CAPS hook text on screen BEFORE narration lands.
  • New visual or text change every 2–3 seconds — no listicle triptychs mid-video.
  • Credit comic issue/artist in description (comic_ref per scene).

SCRIPT STRUCTURE (Loop → Stakes → Mechanism → Receipt):
  Scene 1 (0–3s): silent/visual punch + hook text (no warm intro, no "hey guys")
  Scene 2–3: mechanism in 2 beats max (never "three signs" list in one scene)
  Scene 4: quotable receipt line + soft sub bridge ("subscribe for part 2")

POST-UPLOAD DATA LOOP:
  • Compare stayed-to-watch vs channel typical (~42%). Below 40% = fix hook, not production.
  • Read retention graph: WHERE EXACTLY DO VIEWERS LEAVE?
  • REPEAT WHAT WORKS (Lost To Barry 56.5%, Tragedy 52.8%). FIX WHAT FAILS (Lost Identity 30.5%).
  • NEVER GUESS. LET DATA DECIDE which title/frame-1 pattern to clone next.
"""

# ZeroTier empirical winners/losers (May 2026 analytics batch)
ZT_STAYED_BENCHMARKS: dict[str, float] = {
    "lost_to_barry": 56.5,
    "tragedy_outrun": 52.8,
    "outran_black_hole": 44.4,
    "channel_typical": 42.0,
    "black_flash_mechanism": 27.5,
    "lost_identity": 30.5,
    "pope_reference": 79.8,
}

POPE_TARGET_STAYED_MIN = 45.0
POPE_TARGET_STAYED_STRETCH = 55.0
POPE_MAX_DURATION_SEC = 36.0
POPE_IDEAL_DURATION_SEC = 33.0
POPE_MAX_SCENES = 4
POPE_TITLE_MAX_CHARS = 60

_TITLE_SUMMARY_RE = re.compile(
    r"^(the time wally west|when wally west|wally west's)\s",
    re.I,
)


def validate_pope_script(script: dict[str, Any]) -> list[str]:
    """Return human-readable violations. Empty list = passes Pope checks."""
    issues: list[str] = []
    title = str(script.get("title") or "").strip()
    scenes = script.get("scenes") or []

    if len(title) > POPE_TITLE_MAX_CHARS:
        issues.append(f"title too long ({len(title)} chars; Pope max {POPE_TITLE_MAX_CHARS})")
    if _TITLE_SUMMARY_RE.match(title):
        issues.append(
            "title reads like story summary — Pope: reinforce the hook, not explain the video"
        )
    if len(scenes) > POPE_MAX_SCENES:
        issues.append(f"too many scenes ({len(scenes)}; Pope Tier A max {POPE_MAX_SCENES})")

    total = sum(float(s.get("duration_sec") or s.get("duration") or 0) for s in scenes)
    if total > POPE_MAX_DURATION_SEC:
        issues.append(f"runtime {total:.0f}s exceeds Pope max {POPE_MAX_DURATION_SEC:.0f}s")

    if scenes:
        hook = str(scenes[0].get("text_overlay") or scenes[0].get("caption") or "")
        if len(hook.split()) > 7:
            issues.append("frame-1 text_overlay too long (max 7 words for mute scroll-stop)")
        if hook != hook.upper() and hook:
            issues.append("frame-1 text_overlay should be ALL CAPS (Pope hook pattern)")

    for i, scene in enumerate(scenes):
        overlay = str(scene.get("text_overlay") or scene.get("caption") or "").lower()
        if "three signs" in overlay or "three ways" in overlay:
            issues.append(f"scene {i + 1}: listicle overlay — Pope retention killer on ZT data")

    return issues


def pope_doctrine_block() -> str:
    return POPE_SHORTS_DOCTRINE.strip()
