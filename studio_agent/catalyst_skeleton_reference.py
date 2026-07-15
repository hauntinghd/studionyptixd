"""Catalyst skeleton visual reference — channel style bible from approved YouTube shorts.

Teaches Studio Agent + skeleton still pipeline how MrSkeleWelly scenes must look:
single full-frame skeleton, wardrobe via image edit, any environment style.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent
BUNDLED_MEMORY_PATH = _REPO_ROOT / "ops" / "catalyst_skeleton_reference_memory.json"
REFS_DIR = _REPO_ROOT / "ops" / "catalyst_skeleton_refs"

_LAYOUT_ISSUES = frozenset({
    "split_screen_diptych",
    "center_seam_heuristic",
    "prompt_split_language",
    "layout_artifact",
    "symbolic_clutter",
    "background_artifact",
    "fused_glass",
    "shared_bubble",
    "glass_pod",
})


def resolve_channel_key(workspace: Path | None = None, *, fallback: str = "mrskelewelly") -> str:
    workspace = Path(workspace) if workspace else None
    if workspace and workspace.is_dir():
        for name in ("job_spec.json", "result.json"):
            path = workspace / name
            if not path.is_file():
                continue
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            for key in ("channel_key", "registry_key", "category", "render_style"):
                value = str(data.get(key) or "").strip()
                if value:
                    return value
    return str(fallback or "mrskelewelly")


def catalyst_channel_memory_path() -> Path:
    """Same file backend Catalyst uses — studio still-learning must land here."""
    try:
        from backend_settings import TEMP_DIR

        return Path(TEMP_DIR) / "catalyst_channel_memory.json"
    except Exception:
        import os

        root = Path(os.getenv("APP_DATA_DIR", os.getenv("TEMP_DIR", "/var/data")))
        return root / "temp_assets" / "catalyst_channel_memory.json"


def _load_bundled_memory() -> dict[str, Any]:
    if not BUNDLED_MEMORY_PATH.is_file():
        return {}
    try:
        data = json.loads(BUNDLED_MEMORY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(data) if isinstance(data, dict) else {}


def _load_runtime_channel_bucket(channel_key: str) -> dict[str, Any]:
    path = catalyst_channel_memory_path()
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    bucket = data.get(channel_key)
    return dict(bucket) if isinstance(bucket, dict) else {}


def _dedupe_lines(values: list[str], *, max_items: int = 12) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = " ".join(str(raw or "").split()).strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= max_items:
            break
    return out


def get_skeleton_visual_directives(channel_key: str = "mrskelewelly") -> dict[str, Any]:
    """Merged style bible: bundled YouTube refs + runtime Catalyst channel memory."""
    channel_key = str(channel_key or "").strip() or "mrskelewelly"
    bundled = _load_bundled_memory()
    runtime = _load_runtime_channel_bucket(channel_key)

    style_bible = _dedupe_lines(
        list(bundled.get("style_bible") or []) + list(runtime.get("visual_learnings") or []),
        max_items=14,
    )
    visual_wins = _dedupe_lines(
        list(bundled.get("visual_wins") or []) + list(runtime.get("visual_learnings") or []),
        max_items=10,
    )
    visual_watchouts = _dedupe_lines(
        list(bundled.get("visual_watchouts") or []) + list(runtime.get("visual_watchouts") or []),
        max_items=12,
    )
    wardrobe_rules = _dedupe_lines(list(bundled.get("wardrobe_edit_rules") or []), max_items=8)

    prompt_block = format_skeleton_directives_prompt(
        style_bible=style_bible,
        visual_wins=visual_wins,
        visual_watchouts=visual_watchouts,
        wardrobe_rules=wardrobe_rules,
    )

    refs = list(bundled.get("references") or [])
    return {
        "channel_key": channel_key,
        "style_bible": style_bible,
        "visual_wins": visual_wins,
        "visual_watchouts": visual_watchouts,
        "wardrobe_edit_rules": wardrobe_rules,
        "prompt_block": prompt_block,
        "reference_videos": refs,
        "runtime_memory_loaded": bool(runtime),
        "bundled_memory_loaded": bool(bundled),
    }


def format_skeleton_directives_prompt(
    *,
    style_bible: list[str] | None = None,
    visual_wins: list[str] | None = None,
    visual_watchouts: list[str] | None = None,
    wardrobe_rules: list[str] | None = None,
) -> str:
    parts: list[str] = ["CATALYST SKELETON VISUAL DIRECTIVE (channel-approved references):"]
    for line in list(style_bible or [])[:8]:
        parts.append(f"- {line}")
    if wardrobe_rules:
        parts.append("WARDROBE EDIT RULES:")
        for line in wardrobe_rules[:5]:
            parts.append(f"- {line}")
    if visual_watchouts:
        parts.append("WATCHOUTS:")
        for line in visual_watchouts[:6]:
            parts.append(f"- {line}")
    return " ".join(parts)[:2200]


def append_catalyst_directives_to_prompt(prompt: str, channel_key: str = "mrskelewelly") -> str:
    """Attach Catalyst notes WITHOUT overwriting scene/wardrobe content.

    Historical bug: Catalyst was prepended and the combined string truncated,
    deleting location/wardrobe. We now append only into remaining budget.
    """
    from skeleton_ai.prompt_compose import compose_priority_prompt

    directives = get_skeleton_visual_directives(channel_key)
    block = str(directives.get("prompt_block") or "").strip()
    base = str(prompt or "").strip()
    if not block:
        return base
    if block.lower() in base.lower():
        return base
    return compose_priority_prompt(
        primary=base,
        secondary="",
        tertiary=f"CHANNEL NOTES (low priority): {block}",
        budget=3200,
    )


def catalyst_block_for_compose(channel_key: str = "mrskelewelly") -> str:
    """Raw catalyst text for scene-first composers (never prepend blindly)."""
    directives = get_skeleton_visual_directives(channel_key)
    return str(directives.get("prompt_block") or "").strip()


def catalyst_regenerate_method_for_issues(issues: list[str], *, still_exists: bool) -> str:
    """Layout artifacts must master-regenerate — editing a diptych cannot fix composition."""
    issue_set = {str(i).strip() for i in list(issues or []) if str(i).strip()}
    if issue_set & _LAYOUT_ISSUES:
        return "regenerate"
    if "extra_hand" in issue_set and still_exists:
        return "edit"
    return "edit" if still_exists else "regenerate"


def seed_bundled_reference_memory(channel_key: str = "mrskelewelly") -> dict[str, Any]:
    """Merge bundled YouTube reference learnings into runtime Catalyst channel memory."""
    channel_key = str(channel_key or "").strip() or "mrskelewelly"
    bundled = _load_bundled_memory()
    if not bundled:
        return {"ok": False, "reason": "bundled_memory_missing"}

    path = catalyst_channel_memory_path()
    data: dict[str, Any] = {}
    if path.is_file():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                data = loaded
        except Exception:
            pass

    bucket = dict(data.get(channel_key) or {})
    bucket["key"] = channel_key
    bucket["updated_at"] = time.time()
    bucket["skeleton_reference_seeded_at"] = time.time()
    bucket["skeleton_reference_videos"] = list(bundled.get("references") or [])

    wins = list(bundled.get("visual_wins") or []) + list(bundled.get("style_bible") or [])[:4]
    watchouts = list(bundled.get("visual_watchouts") or [])
    bucket["visual_learnings"] = _dedupe_lines([*wins, *list(bucket.get("visual_learnings") or [])])
    bucket["visual_watchouts"] = _dedupe_lines([*watchouts, *list(bucket.get("visual_watchouts") or [])])

    data[channel_key] = bucket
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return {
        "ok": True,
        "channel_key": channel_key,
        "memory_path": str(path),
        "watchouts_count": len(bucket.get("visual_watchouts") or []),
        "learnings_count": len(bucket.get("visual_learnings") or []),
    }