"""Free / public-domain external media sources for Studio.

High-level dispatch over archival footage, historical stills, public records,
royalty-free music, and CC0 sound effects. See clients.py for per-source detail.
"""
from __future__ import annotations

from typing import Any

from media_sources import clients

# Archival / footage / stills / records (no AI generation).
ARCHIVAL_SOURCES = {
    "internet_archive": clients.internet_archive,
    "nasa": clients.nasa,
    "loc": clients.loc,
    "wikimedia": clients.wikimedia,
    "nps": clients.nps,
    "fbi": clients.fbi,
}

# Best default fan-out per content kind.
PRESETS = {
    "history": ["internet_archive", "loc", "wikimedia"],
    "documentary": ["internet_archive", "loc", "wikimedia", "nps"],
    "science": ["nasa", "wikimedia", "internet_archive"],
    "criminal": ["fbi", "loc", "internet_archive", "wikimedia"],
    "nature": ["nps", "nasa", "internet_archive"],
    "all": list(ARCHIVAL_SOURCES.keys()),
}


def search_archival(
    query: str,
    *,
    sources: list[str] | None = None,
    preset: str = "",
    limit_per_source: int = 8,
) -> dict[str, Any]:
    """Fan out an archival query across selected sources.

    `preset` picks a curated source set (history/documentary/science/criminal/
    nature/all). Explicit `sources` overrides the preset.
    """
    chosen = sources or PRESETS.get(preset.strip().lower(), PRESETS["documentary"])
    results: list[dict[str, Any]] = []
    errors: dict[str, str] = {}
    for name in chosen:
        fn = ARCHIVAL_SOURCES.get(name)
        if not fn:
            errors[name] = "unknown source"
            continue
        try:
            rows = fn(query, limit=limit_per_source)
            for row in rows:
                if isinstance(row, dict) and row.get("error"):
                    errors[name] = str(row["error"])
                else:
                    results.append(row)
        except Exception as exc:  # never break the agent turn
            errors[name] = str(exc)[:200]
    return {
        "query": query,
        "preset": preset or None,
        "sources": chosen,
        "count": len(results),
        "results": results,
        "errors": errors or None,
    }


def search_music(query: str, *, limit: int = 12, instrumental: bool = False) -> dict[str, Any]:
    try:
        rows = clients.jamendo(query, limit=limit, instrumental=instrumental)
    except Exception as exc:
        return {"query": query, "results": [], "errors": {"jamendo": str(exc)[:200]}}
    errs = {"jamendo": rows[0]["error"]} if rows and isinstance(rows[0], dict) and rows[0].get("error") else None
    clean = [r for r in rows if not (isinstance(r, dict) and r.get("error"))]
    return {"query": query, "count": len(clean), "results": clean, "errors": errs}


def fetch_archival_for_video(*args: Any, **kwargs: Any) -> dict[str, Any]:
    """Per-video scene-matched archival manifest (see archive_planner.py)."""
    from media_sources.archive_planner import fetch_archival_for_video as _fetch

    return _fetch(*args, **kwargs)


def resolve_archival_asset(item: dict[str, Any]) -> dict[str, Any]:
    from media_sources.archive_planner import resolve_direct_download

    return resolve_direct_download(item)


def search_sfx(query: str, *, limit: int = 12, cc0_only: bool = True) -> dict[str, Any]:
    try:
        rows = clients.freesound(query, limit=limit, cc0_only=cc0_only)
    except Exception as exc:
        return {"query": query, "results": [], "errors": {"freesound": str(exc)[:200]}}
    errs = {"freesound": rows[0]["error"]} if rows and isinstance(rows[0], dict) and rows[0].get("error") else None
    clean = [r for r in rows if not (isinstance(r, dict) and r.get("error"))]
    return {"query": query, "count": len(clean), "results": clean, "errors": errs}


__all__ = [
    "ARCHIVAL_SOURCES",
    "PRESETS",
    "search_archival",
    "fetch_archival_for_video",
    "resolve_archival_asset",
    "search_music",
    "search_sfx",
    "clients",
]
