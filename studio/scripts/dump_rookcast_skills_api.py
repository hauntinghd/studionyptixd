"""Dump all Rookcast skills from API CDP capture to studio/skills/."""
from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SKILLS_DIR = ROOT / "studio" / "skills"


def load_api_payload(cdp_path: Path) -> dict:
    raw = json.loads(cdp_path.read_text(encoding="utf-8"))
    b64 = raw["result"]["value"]
    if b64.startswith("ERR:"):
        raise RuntimeError(b64)
    return json.loads(base64.b64decode(b64).decode("utf-8"))


def companion_name(comp: dict) -> str:
    return (comp.get("name") or comp.get("slug") or "unknown.md").strip()


def write_skill(skill: dict) -> dict:
    slug = skill["slug"].strip()
    out_dir = SKILLS_DIR / slug
    out_dir.mkdir(parents=True, exist_ok=True)

    skill_md = out_dir / "SKILL.md"
    body = skill.get("body") or ""
    if body and not body.startswith("---"):
        front = (
            "---\n"
            f"name: {slug}\n"
            f"description: >-\n  {(skill.get('description') or '').replace(chr(10), ' ')[:500]}\n"
            "---\n\n"
        )
        body = front + body
    skill_md.write_text(body, encoding="utf-8")

    siblings: list[str] = []
    for comp in skill.get("companions") or []:
        fname = companion_name(comp)
        if not fname.endswith(".md"):
            fname = f"{fname}.md"
        siblings.append(fname)
        comp_body = comp.get("body") or comp.get("content") or ""
        (out_dir / fname).write_text(comp_body, encoding="utf-8")

    return {
        "slug": slug,
        "skill_chars": len(body),
        "siblings": siblings,
        "sibling_chars": sum((out_dir / s).stat().st_size for s in siblings),
    }


def main() -> None:
    cdp_path = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    if cdp_path is None:
        print("usage: dump_rookcast_skills_api.py <cdp-json-path|api-snapshot.json>")
        raise SystemExit(1)

    if cdp_path.name.endswith("_rookcast_api_snapshot.json") or cdp_path.name == "skills.json":
        payload = json.loads(cdp_path.read_text(encoding="utf-8"))
    else:
        payload = load_api_payload(cdp_path)
    skills = payload.get("data") or []
    if not skills:
        raise SystemExit("no skills in payload")

    manifest: dict = {"skills": {}, "source": "rookcast API", "count": len(skills)}
    total_files = 0
    for skill in sorted(skills, key=lambda s: s["slug"]):
        info = write_skill(skill)
        total_files += 1 + len(info["siblings"])
        manifest["skills"][info["slug"]] = {
            "status": "extracted",
            "siblings": info["siblings"],
            "skill_chars": info["skill_chars"],
        }
        print(
            f"  {info['slug']}: SKILL.md ({info['skill_chars']:,} chars)"
            + (f" + {len(info['siblings'])} files" if info["siblings"] else "")
        )

    manifest_path = SKILLS_DIR / "_manifest.yaml"
    # Write YAML manually (avoid pyyaml dep)
    lines = [
        "# Rookcast skills manifest — full API import",
        f"# Skills: {len(skills)} | Files: {total_files}",
        "",
        "skills:",
    ]
    for slug, meta in manifest["skills"].items():
        lines.append(f"  {slug}:")
        lines.append(f"    status: {meta['status']}")
        if meta["siblings"]:
            sibs = ", ".join(meta["siblings"])
            lines.append(f"    siblings: [{sibs}]")
        else:
            lines.append("    siblings: []")
    manifest_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"\nDone: {len(skills)} skills, {total_files} files -> {SKILLS_DIR}")


if __name__ == "__main__":
    main()
