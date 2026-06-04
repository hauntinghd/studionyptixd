"""
Skeleton AI category registry — YouTube-aligned presets + per-user custom lanes.

Built-ins: 20 categories mapped from YouTube's standard video categories, tuned for
60s Skeleton AI shorts (canonical host, 10–12 beats).

Custom: stored per user under data/skeleton_custom_categories/{user_id}.json
(keys prefixed with ``custom_`` when needed to avoid builtin collisions).
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
CUSTOM_DIR = Path(
    __import__("os").getenv("SKELETON_CUSTOM_CATEGORIES_DIR", str(ROOT / "data" / "skeleton_custom_categories"))
)
MAX_CUSTOM_PER_USER = 50

# Legacy Create-panel / niche keys → builtin slug
LEGACY_ALIASES: dict[str, str] = {
    "classical_clash": "comparison_vs",
    "medieval_clash": "history",
    "gunpowder_clash": "history",
    "wildcard_clash": "outcast",
    "human_limits": "science_technology",
}

_SKELETON_FORMAT = (
    "Write a 60-second YouTube Short narration for NYPTID Skeleton AI. "
    "The on-screen host is the locked canonical 3D skeleton (never describe "
    "redesigning it). 10–12 beats, one punchy sentence each. Strong hook on "
    "beat 1. Clear payoff before the end. Close with a comment-driving CTA."
)


def _prompt(lane: str, *, seeds: list[str]) -> dict[str, Any]:
    return {
        "system_prompt": f"{lane}\n\n{_SKELETON_FORMAT}",
        "seed_ideas": seeds,
    }


# 20 built-ins (YouTube category alignment + skeleton-native lanes)
BUILTIN_CATEGORIES: dict[str, dict[str, Any]] = {
    "film_animation": {
        "label": "Film & Animation",
        "tagline": "Cinematic stories, scene-driven hooks, trailer energy",
        "youtube_category": "Film & Animation",
        "builtin": True,
        **_prompt(
            "YouTube lane: Film & Animation. Narrate like a mini-movie trailer — "
            "stakes, reveal, emotional turn. Visual beats should feel cinematic.",
            seeds=[
                "The 24 hours before the heist that changed everything",
                "What if the villain was right the whole time",
                "One line of dialogue that rewrote the entire franchise",
            ],
        ),
    },
    "autos_vehicles": {
        "label": "Autos & Vehicles",
        "tagline": "Cars, bikes, engineering, speed comparisons",
        "youtube_category": "Autos & Vehicles",
        "builtin": True,
        **_prompt(
            "YouTube lane: Autos & Vehicles. Compare machines, specs, and driver "
            "constraints with concrete numbers.",
            seeds=[
                "F1 car vs fighter jet on a runway — what actually wins",
                "Why EV torque breaks tires differently than gas",
                "The one modification that voids your warranty instantly",
            ],
        ),
    },
    "music": {
        "label": "Music",
        "tagline": "Artists, genres, industry mechanics, culture",
        "youtube_category": "Music",
        "builtin": True,
        **_prompt(
            "YouTube lane: Music. Explain trends, rivalries, or production facts "
            "without quoting copyrighted lyrics.",
            seeds=[
                "Why this chord progression is in every pop hit",
                "Studio trick that makes vocals sound twice as big",
                "Genre that died and came back stronger",
            ],
        ),
    },
    "pets_animals": {
        "label": "Pets & Animals",
        "tagline": "Wildlife facts, pet science, animal matchups",
        "youtube_category": "Pets & Animals",
        "builtin": True,
        **_prompt(
            "YouTube lane: Pets & Animals. Biology-first, respectful tone, vivid comparisons.",
            seeds=[
                "House cat vs coyote — survival math explained",
                "Why dogs tilt their heads (the real reason)",
                "Animal with the most unfair evolutionary advantage",
            ],
        ),
    },
    "sports": {
        "label": "Sports",
        "tagline": "Athletes, rules, records, training science",
        "youtube_category": "Sports",
        "builtin": True,
        **_prompt(
            "YouTube lane: Sports. Use real stats, biomechanics, and rulebook edges.",
            seeds=[
                "Why sprinters can't hold top speed for more than 2 seconds",
                "Rule change that broke an entire sport overnight",
                "Athlete skill that looks illegal but isn't",
            ],
        ),
    },
    "travel_events": {
        "label": "Travel & Events",
        "tagline": "Places, survival, festivals, geography",
        "youtube_category": "Travel & Events",
        "builtin": True,
        **_prompt(
            "YouTube lane: Travel & Events. Ground stories in real places, climate, and logistics.",
            seeds=[
                "City where tourists always make the same fatal mistake",
                "Festival tradition that almost got banned worldwide",
                "Country with a law tourists break on day one",
            ],
        ),
    },
    "gaming": {
        "label": "Gaming",
        "tagline": "Games, mechanics, speedruns, lore",
        "youtube_category": "Gaming",
        "builtin": True,
        **_prompt(
            "YouTube lane: Gaming. Mechanics, meta shifts, developer intent, player psychology.",
            seeds=[
                "Boss fight everyone skips — and why that's optimal",
                "Patch that accidentally created a new esport",
                "Game mechanic players hate but designers love",
            ],
        ),
    },
    "people_blogs": {
        "label": "People & Blogs",
        "tagline": "Personal challenges, lifestyle experiments, day-in-the-life",
        "youtube_category": "People & Blogs",
        "builtin": True,
        **_prompt(
            "YouTube lane: People & Blogs. First-person challenge framing, stakes, daily beats, "
            "honest outcomes. Good for 'what if I tried X for 30 days' hooks.",
            seeds=[
                "I tried waking up at 4am for 30 days — what broke first",
                "Living on $5 a day in a major city — day-by-day math",
                "Deleting social media for 30 days — the withdrawal curve",
            ],
        ),
    },
    "comedy": {
        "label": "Comedy",
        "tagline": "Absurd hypotheticals, observational punchlines",
        "youtube_category": "Comedy",
        "builtin": True,
        **_prompt(
            "YouTube lane: Comedy. Escalating absurdity, crisp punchlines, still fact-anchored.",
            seeds=[
                "Jobs that sound fake but pay six figures",
                "Laws that read like a sitcom writer pitched them",
                "Everyday object with the most unhinged history",
            ],
        ),
    },
    "entertainment": {
        "label": "Entertainment",
        "tagline": "Pop culture, celebrity, viral moments",
        "youtube_category": "Entertainment",
        "builtin": True,
        **_prompt(
            "YouTube lane: Entertainment. Pop-culture explainers, behind-the-scenes mechanics, "
            "fair-use commentary tone.",
            seeds=[
                "Scene everyone misquotes — what actually happened",
                "Franchise decision that aged terribly in hindsight",
                "Trend that peaked and died in 72 hours",
            ],
        ),
    },
    "news_politics": {
        "label": "News & Politics",
        "tagline": "Current events, policy, civics explainers",
        "youtube_category": "News & Politics",
        "builtin": True,
        **_prompt(
            "YouTube lane: News & Politics. Neutral explainer tone, cite mechanisms not "
            "conspiracy. Frame hypotheticals clearly as thought experiments.",
            seeds=[
                "How a bill actually becomes law — the steps schools skip",
                "Policy lever most voters misunderstand",
                "Why this headline statistic is misleading",
            ],
        ),
    },
    "howto_style": {
        "label": "Howto & Style",
        "tagline": "Tutorials, hacks, fashion, DIY",
        "youtube_category": "Howto & Style",
        "builtin": True,
        **_prompt(
            "YouTube lane: Howto & Style. Step-by-step clarity, one core technique per short.",
            seeds=[
                "Styling trick stylists use on camera but never say out loud",
                "Tool everyone owns that fixes this common mistake",
                "5-minute upgrade that doubles perceived quality",
            ],
        ),
    },
    "education": {
        "label": "Education",
        "tagline": "Lessons, study hacks, explainers",
        "youtube_category": "Education",
        "builtin": True,
        **_prompt(
            "YouTube lane: Education. Teach one concept with a memorable analogy and recap.",
            seeds=[
                "Concept teachers rush past that breaks everything later",
                "Memory trick that actually works for exams",
                "Subject everyone thinks is hard — the one insight that unlocks it",
            ],
        ),
    },
    "science_technology": {
        "label": "Science & Technology",
        "tagline": "STEM facts, gadgets, future tech",
        "youtube_category": "Science & Technology",
        "builtin": True,
        **_prompt(
            "YouTube lane: Science & Technology. One mechanism, numbers where possible, "
            "no sci-fi hand-waving.",
            seeds=[
                "Technology everyone uses that violates physics on paper",
                "Experiment that failed publicly but taught the field everything",
                "Body limit science can't cheat — and why",
            ],
        ),
    },
    "nonprofits": {
        "label": "Nonprofits & Activism",
        "tagline": "Causes, charity mechanics, social impact",
        "youtube_category": "Nonprofits & Activism",
        "builtin": True,
        **_prompt(
            "YouTube lane: Nonprofits & Activism. Inspire action, show systems change, "
            "avoid guilt-only framing.",
            seeds=[
                "Donation path where most money actually lands",
                "Grassroots tactic that scaled without going viral",
                "Problem everyone shares but nobody funds — why",
            ],
        ),
    },
    "comparison_vs": {
        "label": "Comparison / VS",
        "tagline": "Who wins, rankings, head-to-head verdicts",
        "youtube_category": "Skeleton staple",
        "builtin": True,
        **_prompt(
            "Skeleton staple: Comparison / VS. Format '[A] vs [B] — who actually wins?' "
            "Cover size, tools, terrain, opening move, turning point, verdict, caveat.",
            seeds=[
                "Roman legion vs Macedonian phalanx at full strength",
                "Modern special forces vs ancient Spartans — fair rules",
                "Two technologies everyone argues about — settled with math",
            ],
        ),
    },
    "true_crime": {
        "label": "True Crime",
        "tagline": "Cases, investigations, forensic twists",
        "youtube_category": "Skeleton staple",
        "builtin": True,
        **_prompt(
            "Skeleton staple: True Crime. Timeline beats, evidence turns, no glorification "
            "of violence. Respect victims.",
            seeds=[
                "Case solved by one detail everyone overlooked",
                "Alibi that sounded perfect until this timestamp",
                "Investigation tool that didn't exist when the crime happened",
            ],
        ),
    },
    "horror_scary": {
        "label": "Horror & Scary",
        "tagline": "Creepy facts, paranormal-adjacent, tension",
        "youtube_category": "Skeleton staple",
        "builtin": True,
        **_prompt(
            "Skeleton staple: Horror & Scary. Slow-burn dread, sensory detail, twist reveal. "
            "No gore porn.",
            seeds=[
                "Place tourists visit that has a body count pattern",
                "Sound your brain flags as danger before you know why",
                "Urban legend with a documented real origin",
            ],
        ),
    },
    "history": {
        "label": "History",
        "tagline": "Empires, battles, forgotten events",
        "youtube_category": "Skeleton staple",
        "builtin": True,
        **_prompt(
            "Skeleton staple: History. Period-correct details, named figures, cause-and-effect.",
            seeds=[
                "Battle everyone remembers wrong — what orders actually were",
                "Empire that fell from one logistical mistake",
                "Invention that arrived 100 years too early to matter",
            ],
        ),
    },
    "outcast": {
        "label": "Outcast",
        "tagline": "Edgy, contrarian, whistleblower hypotheticals",
        "youtube_category": "Skeleton staple",
        "builtin": True,
        **_prompt(
            "Skeleton staple: Outcast. Contrarian social experiments, anti-establishment "
            "hooks, 'what if you exposed X for 30 days' narratives. Frame as thought "
            "experiments — not actionable illegal advice.",
            seeds=[
                "What happens if you try to expose the government for 30 days and never get caught",
                "Living completely off-grid with $0 for 30 days",
                "Saying the quiet part out loud in every meeting for a week",
            ],
        ),
    },
}


def slugify_category_key(label: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", (label or "").strip().lower()).strip("_")
    return s[:48] or "custom_lane"


def _resolve_key(key: str) -> str:
    k = (key or "").strip().lower()
    return LEGACY_ALIASES.get(k, k)


def _custom_store_path(user_id: str) -> Path:
    safe = re.sub(r"[^a-zA-Z0-9_-]", "", (user_id or "").strip())[:64] or "anon"
    return CUSTOM_DIR / f"{safe}.json"


def _load_custom_rows(user_id: str | None) -> list[dict[str, Any]]:
    if not user_id:
        return []
    path = _custom_store_path(user_id)
    if not path.is_file():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []
    rows = data.get("categories") if isinstance(data, dict) else data
    return list(rows) if isinstance(rows, list) else []


def _save_custom_rows(user_id: str, rows: list[dict[str, Any]]) -> None:
    path = _custom_store_path(user_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"categories": rows, "updated_at": time.time()}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _custom_by_key(user_id: str | None) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in _load_custom_rows(user_id):
        if not isinstance(row, dict):
            continue
        key = str(row.get("key") or "").strip().lower()
        if key:
            out[key] = row
    return out


def list_valid_keys(user_id: str | None = None) -> list[str]:
    keys = list(BUILTIN_CATEGORIES.keys())
    keys.extend(sorted(_custom_by_key(user_id).keys()))
    keys.extend(LEGACY_ALIASES.keys())
    return sorted(set(keys))


def list_categories(*, user_id: str | None = None) -> list[dict[str, Any]]:
    """Built-ins first (20), then user custom categories."""
    rows: list[dict[str, Any]] = []
    for k, v in BUILTIN_CATEGORIES.items():
        rows.append({
            "key": k,
            "label": v["label"],
            "tagline": v.get("tagline", ""),
            "seeds": list(v.get("seed_ideas") or []),
            "youtube_category": v.get("youtube_category", ""),
            "builtin": True,
            "custom": False,
        })
    for k, v in sorted(_custom_by_key(user_id).items()):
        rows.append({
            "key": k,
            "label": v.get("label", k),
            "tagline": v.get("tagline", ""),
            "seeds": list(v.get("seed_ideas") or []),
            "youtube_category": "Custom",
            "builtin": False,
            "custom": True,
        })
    return rows


def get_category(key: str, *, user_id: str | None = None) -> dict[str, Any]:
    resolved = _resolve_key(key)
    if resolved in BUILTIN_CATEGORIES:
        return dict(BUILTIN_CATEGORIES[resolved])
    custom = _custom_by_key(user_id)
    if resolved in custom:
        row = custom[resolved]
        return {
            "label": row.get("label", resolved),
            "tagline": row.get("tagline", ""),
            "system_prompt": row.get("system_prompt") or _default_custom_system_prompt(row),
            "seed_ideas": list(row.get("seed_ideas") or []),
        }
    valid = list_valid_keys(user_id)
    raise ValueError(
        f"Unknown category: {key!r}. "
        f"Use list_skeleton_categories or POST /api/skeleton-ai/categories to create a custom lane. "
        f"Valid keys ({len(valid)}): {valid[:30]}{'…' if len(valid) > 30 else ''}"
    )


def _default_custom_system_prompt(row: dict[str, Any]) -> str:
    label = str(row.get("label") or "Custom").strip()
    tagline = str(row.get("tagline") or "").strip()
    extra = f" Lane focus: {tagline}." if tagline else ""
    return (
        f"Custom Skeleton AI category: {label}.{extra} Match the topic tone the user provides.\n\n"
        f"{_SKELETON_FORMAT}"
    )


def create_custom_category(
    user_id: str,
    *,
    label: str,
    key: str | None = None,
    tagline: str | None = None,
    system_prompt: str | None = None,
    seed_ideas: list[str] | None = None,
) -> dict[str, Any]:
    if not user_id:
        raise ValueError("user_id required to create a custom category")
    label = (label or "").strip()
    if len(label) < 2:
        raise ValueError("label must be at least 2 characters")

    rows = _load_custom_rows(user_id)
    if len(rows) >= MAX_CUSTOM_PER_USER:
        raise ValueError(f"Maximum {MAX_CUSTOM_PER_USER} custom categories per account")

    raw_key = slugify_category_key(key or label)
    if raw_key in BUILTIN_CATEGORIES and not raw_key.startswith("custom_"):
        raw_key = f"custom_{raw_key}"
    if raw_key in LEGACY_ALIASES:
        raw_key = f"custom_{raw_key}"

    existing_keys = {str(r.get("key") or "") for r in rows} | set(BUILTIN_CATEGORIES.keys())
    final_key = raw_key
    n = 2
    while final_key in existing_keys:
        final_key = f"{raw_key}_{n}"
        n += 1

    entry = {
        "key": final_key,
        "label": label,
        "tagline": (tagline or "").strip() or f"Custom lane: {label}",
        "system_prompt": (system_prompt or "").strip() or None,
        "seed_ideas": [s.strip() for s in (seed_ideas or []) if str(s).strip()][:12],
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    rows.append(entry)
    _save_custom_rows(user_id, rows)
    return entry
