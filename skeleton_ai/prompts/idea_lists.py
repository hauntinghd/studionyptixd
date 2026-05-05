"""
Idea-list categories shown to user in the script-gen modal.

4 categories per Casey 2026-05-05:
  1. Human Limits          — body/brain failure over time
  2. Marvel vs DC          — comic narration mix
  3. Ancient History Mix   — random topics in ancient settings (no Socrates)
  4. Futuristic Socrates   — occasional rotation

Each category has a Grok system prompt + a list of seed ideas to surface
in the modal. Grok generates fresh ideas on demand using the system prompt
as context.
"""

CATEGORIES = {
    "human_limits": {
        "label": "Human Limits",
        "tagline": "Body and brain failure over time",
        "system_prompt": (
            "You write 60-second YouTube Shorts narration scripts in the "
            "Cryptic Science / Skeleton AI style. Topic category: HUMAN LIMITS — "
            "scripts about how the human body and brain decay, fail, or peak "
            "across a lifetime. Use specific numbers and ages. Format: "
            "'[X] vs [Y]. [Question about limits].' Build escalation across "
            "10-12 narration beats (~5 sec each, ~150 wpm). End with engagement "
            "bait: 'Which would you rather have?' or similar."
        ),
        "seed_ideas": [
            "What aging really does to your bones year by year",
            "The age your brain peaks vs when it declines",
            "How many heartbeats you actually have",
            "Why your eyes fail at 40 (not 60)",
            "Reaction time at 20 vs 50",
        ],
    },
    "marvel_vs_dc": {
        "label": "Marvel vs DC",
        "tagline": "Comic narration mix — power, money, lore",
        "system_prompt": (
            "You write 60-second YouTube Shorts narration scripts in the "
            "Cryptic Science / Skeleton AI style. Topic category: MARVEL VS DC — "
            "scripts comparing characters, abilities, finances, or storylines "
            "across the two comic universes. Use specific lore, dollar amounts, "
            "fight stats. Format: '[Character A] vs [Character B]. Who [verb]?' "
            "Build escalation across 10-12 beats. End with: 'Who would win? "
            "Comments below.'"
        ),
        "seed_ideas": [
            "Tony Stark vs Bruce Wayne net worth",
            "Iron Man vs Batman who's smarter",
            "Wolverine vs Deadpool fight to the death",
            "Doctor Strange vs Zatanna magic showdown",
            "Hulk vs Doomsday raw strength",
        ],
    },
    "ancient_history": {
        "label": "Ancient History Mix",
        "tagline": "Roman, Egyptian, Greek, Chinese — careers, finances, life",
        "system_prompt": (
            "You write 60-second YouTube Shorts narration scripts in the "
            "Cryptic Science / Skeleton AI style. Topic category: ANCIENT "
            "HISTORY MIX — scripts comparing professions, salaries, life "
            "outcomes from ancient settings (Rome, Egypt, Greece, China, "
            "Mesopotamia). DO NOT default to Socrates. Use real historical "
            "figures or roles. Convert ancient currencies to USD-equivalent. "
            "Format: '[Role A] vs [Role B]. Who made more?' Build 10-12 beats. "
            "End with: 'Who would you rather be?'"
        ),
        "seed_ideas": [
            "Roman centurion vs gladiator pay over a career",
            "Pyramid worker vs scribe salary",
            "Spartan soldier vs Persian immortal",
            "Caravan trader vs farmer in ancient China",
            "Greek philosopher vs trireme captain",
        ],
    },
    "futuristic_socrates": {
        "label": "Futuristic Socrates",
        "tagline": "Sci-fi setting + Socratic dialogue (occasional rotation)",
        "system_prompt": (
            "You write 60-second YouTube Shorts narration scripts in the "
            "Cryptic Science / Skeleton AI style. Topic category: FUTURISTIC "
            "SOCRATES — scripts set in 2050+ sci-fi futures with Socratic "
            "questioning style ('What if you knew that...?'). Mix tech "
            "professions with philosophical framing. Format: '[Role A] vs "
            "[Role B]. Who actually wins?' Build 10-12 beats with sci-fi "
            "specifics. End with: 'Which future would you choose?'"
        ),
        "seed_ideas": [
            "Mars colonist vs space miner pay",
            "AI engineer 2050 vs surgeon 2050",
            "Quantum trader vs gene editor net worth",
            "Mind-uploader vs body-augmenter lifespan",
            "Drone pilot vs starship navigator career",
        ],
    },
}


def get_category(key: str) -> dict:
    """Look up category by key, raise if not found."""
    if key not in CATEGORIES:
        raise ValueError(f"Unknown category: {key}. Valid: {list(CATEGORIES.keys())}")
    return CATEGORIES[key]


def list_categories() -> list[dict]:
    """Return all 4 categories as a list (UI render order)."""
    return [
        {"key": k, "label": v["label"], "tagline": v["tagline"], "seeds": v["seed_ideas"]}
        for k, v in CATEGORIES.items()
    ]
