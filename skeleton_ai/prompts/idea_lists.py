"""
Idea-list categories shown to user in the script-gen modal.

Refactored 2026-05-08: Skeleton AI niche → Alternate History Battles niche.

4 categories per Casey 2026-05-08:
  1. classical_clash    — ancient-world counterfactuals (Rome vs Greece vs Persia)
  2. medieval_clash     — medieval-era counterfactuals (Mongols vs Crusaders, Vikings vs Samurai)
  3. gunpowder_clash    — early-modern counterfactuals (Napoleon vs Aztecs, Conquistadors vs Mongols)
  4. wildcard_clash     — cross-era + mythological wildcards (Spartans vs Marines, Achilles vs Lancelot)

Each category has a Grok system prompt + a list of seed ideas to surface
in the modal. Grok generates fresh ideas on demand using the system prompt
as context.
"""

CATEGORIES = {
    "classical_clash": {
        "label": "Classical Clash",
        "tagline": "Ancient-world counterfactuals (Rome / Greece / Persia / Egypt / Carthage)",
        "system_prompt": (
            "You write 60-second YouTube Shorts narration scripts in the "
            "Alternate History Battles style. Topic category: CLASSICAL CLASH — "
            "counterfactual ancient-world matchups (Roman legion vs Macedonian "
            "phalanx, Spartan hoplites vs Persian Immortals, Carthaginian war "
            "elephants vs Roman triarii, Egyptian charioteers vs Hittite "
            "infantry). Cite real military doctrine, period-correct weapons, "
            "real generals. Format: '[Force A] vs [Force B] — Who Actually "
            "Wins?' Build 10-12 beats covering: army size, tech tier, "
            "formation, terrain, opening volley, tactical reveal, turning "
            "point, verdict, caveat. End with: 'Comments: who wins?'"
        ),
        "seed_ideas": [
            "Roman legion vs Macedonian phalanx at full strength",
            "300 Spartans vs 1000 Persian Immortals on neutral ground",
            "Hannibal's war elephants vs Caesar's legions in Gaul",
            "Egyptian charioteers vs Hittite infantry at Kadesh rematch",
            "Greek triremes vs Carthaginian quinqueremes in open sea",
        ],
    },
    "medieval_clash": {
        "label": "Medieval Clash",
        "tagline": "Medieval-era counterfactuals (Mongols / Crusaders / Vikings / Samurai)",
        "system_prompt": (
            "You write 60-second YouTube Shorts narration scripts in the "
            "Alternate History Battles style. Topic category: MEDIEVAL CLASH — "
            "counterfactual medieval-era matchups (Mongol horse archers vs "
            "Crusader knights, Vikings vs Samurai, English longbows vs "
            "Mamluk cavalry, Teutonic Knights vs Aztec Eagle Warriors). Cite "
            "real military doctrine, period-correct armor and weapons, real "
            "leaders (Genghis, Saladin, Edward III, Yoritomo). Format: "
            "'[Force A] vs [Force B] — Who Actually Wins?' Build 10-12 beats. "
            "End with: 'Comments: who wins?'"
        ),
        "seed_ideas": [
            "Mongol horse archers vs European knights at full strength",
            "Vikings vs Samurai on neutral coastline",
            "English longbowmen at Agincourt vs Mamluk cavalry",
            "Teutonic Knights vs Aztec Eagle Warriors",
            "Saladin's army vs Richard the Lionheart at peak strength",
        ],
    },
    "gunpowder_clash": {
        "label": "Gunpowder Clash",
        "tagline": "Early-modern counterfactuals (Napoleon / Conquistadors / Civil War)",
        "system_prompt": (
            "You write 60-second YouTube Shorts narration scripts in the "
            "Alternate History Battles style. Topic category: GUNPOWDER "
            "CLASH — counterfactual early-modern matchups (Napoleon's Grand "
            "Army vs Aztec armies at full strength, Conquistadors vs Inca, "
            "Confederate cavalry vs Imperial Japan riflemen, Spanish tercios "
            "vs Ottoman janissaries). Cite real doctrine, period weapons "
            "(musket, cannon, bayonet), real commanders. Format: '[Force A] "
            "vs [Force B] — Who Actually Wins?' Build 10-12 beats covering "
            "army size, weapons tier, formations, terrain, verdict. End with: "
            "'Comments: who wins?'"
        ),
        "seed_ideas": [
            "Napoleon's Grand Army vs Aztec army at full strength",
            "Conquistadors vs Inca Empire at Cajamarca rematch",
            "Spanish tercios vs Ottoman janissaries on the Danube",
            "Confederate cavalry vs Imperial Japanese infantry",
            "Wellington's redcoats at Waterloo vs Mongol horde",
        ],
    },
    "wildcard_clash": {
        "label": "Wildcard Clash",
        "tagline": "Cross-era + mythological matchups (Spartans vs Marines / Achilles vs Lancelot)",
        "system_prompt": (
            "You write 60-second YouTube Shorts narration scripts in the "
            "Alternate History Battles style. Topic category: WILDCARD "
            "CLASH — cross-era counterfactuals and mythological matchups "
            "(300 Spartans vs modern Marines, Achilles vs Lancelot, Mongol "
            "horde vs Roman legion, Genghis Khan vs Caesar one-on-one, "
            "samurai with bushido vs Templar with chivalry). Cite real "
            "doctrine on both sides, fairly weight tech vs training. "
            "Format: '[Force A] vs [Force B] — Who Actually Wins?' Build "
            "10-12 beats. End with: 'Comments: who wins?'"
        ),
        "seed_ideas": [
            "300 Spartans vs modern US Marine platoon at full strength",
            "Achilles vs Lancelot in single combat",
            "Mongol horde vs Roman legion at peak Pax Romana",
            "Genghis Khan vs Julius Caesar one-on-one",
            "Templar knight vs samurai under bushido code",
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
