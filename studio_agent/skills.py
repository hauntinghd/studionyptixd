"""Load Rookcast skills + channel docs from studio/."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STUDIO = ROOT / "studio"
SKILLS = STUDIO / "skills"
CHANNELS = STUDIO / "channels"


def list_skill_slugs() -> list[str]:
    if not SKILLS.exists():
        return []
    return sorted(
        p.name
        for p in SKILLS.iterdir()
        if p.is_dir() and (p / "SKILL.md").exists()
    )


def read_skill(slug: str, *, max_chars: int = 80_000) -> str:
    slug = slug.strip().replace("/", "").replace("\\", "")
    path = SKILLS / slug / "SKILL.md"
    if not path.exists():
        raise FileNotFoundError(f"skill not found: {slug}")
    text = path.read_text(encoding="utf-8")
    if len(text) > max_chars:
        return text[:max_chars] + f"\n\n… truncated ({len(text) - max_chars} chars omitted)"
    return text


def read_skill_companion(slug: str, filename: str, *, max_chars: int = 80_000) -> str:
    slug = slug.strip().replace("/", "").replace("\\", "")
    # Companions legitimately live in subdirectories (references/, scripts/).
    # Taking only Path(filename).name flattened "references/bank.md" to
    # "bank.md" and raised FileNotFoundError for every skill that organises its
    # material -- silently, since callers treat a missing companion as "none".
    # Resolve the real path, then confirm it stayed inside the skill directory
    # so a caller cannot walk out of it with "../".
    root = (SKILLS / slug).resolve()
    candidate = (root / str(filename or "").replace("\\", "/")).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise FileNotFoundError(f"{slug}/{filename} is outside the skill directory") from exc
    path = candidate
    if not path.exists():
        raise FileNotFoundError(f"{slug}/{filename} not found")
    text = path.read_text(encoding="utf-8")
    if len(text) > max_chars:
        return text[:max_chars] + f"\n\n… truncated"
    return text


def read_channel_doc(channel_key: str, doc: str = "CHANNEL") -> str:
    key = re.sub(r"[^a-z0-9_]", "", channel_key.lower())
    path = CHANNELS / key / f"{doc}.md"
    if not path.exists():
        raise FileNotFoundError(f"channel doc missing: {key}/{doc}.md")
    return path.read_text(encoding="utf-8")


def skills_index_for_prompt(*, max_skills: int = 40) -> str:
    slugs = list_skill_slugs()
    # Prefer studio-agent-tools first so task-makers see the Tyler tool dictionary.
    if "studio-agent-tools" in slugs:
        slugs = ["studio-agent-tools"] + [s for s in slugs if s != "studio-agent-tools"]
    slugs = slugs[:max_skills]
    lines = ["Available Rookcast skills (load with load_skill):"]
    for slug in slugs:
        skill_path = SKILLS / slug / "SKILL.md"
        desc = ""
        if skill_path.exists():
            head = skill_path.read_text(encoding="utf-8")[:500]
            m = re.search(r"^description:\s*(.+)$", head, re.M)
            if m:
                desc = m.group(1).strip()[:120]
        lines.append(f"- {slug}" + (f" — {desc}" if desc else ""))
    return "\n".join(lines)
