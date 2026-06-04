"""Per-video archival footage planning — scene-matched B-roll from free sources."""
from __future__ import annotations

import json
import os
import re
import uuid
from pathlib import Path
from typing import Any

from media_sources import PRESETS, search_archival
from media_sources import clients

ROOT = Path(__file__).resolve().parents[1]
_MANIFEST_ROOT = Path(
    os.environ.get(
        "STUDIO_AGENT_ARCHIVAL_DIR",
        str(
            Path(os.environ.get("APP_DATA_DIR", "")).expanduser() / "archival_manifests"
            if Path(os.environ.get("APP_DATA_DIR", "")).expanduser().is_dir()
            else ROOT / "data" / "archival_manifests"
        ),
    )
)

_CHANNEL_PRESET = {
    "cryptic_science": "documentary",
    "empire_magnates": "criminal",
    "history_rewind": "history",
    "pope": "history",
    "relatable_tails": "documentary",
    "zero_tier": "documentary",
}


def _infer_preset(topic: str, registry_key: str = "", preset: str = "") -> str:
    if preset and preset in PRESETS:
        return preset
    if registry_key:
        pk = _CHANNEL_PRESET.get(registry_key.strip().lower())
        if pk:
            return pk
    t = topic.lower()
    if any(w in t for w in ("fbi", "crime", "murder", "fraud", "scam", "heist")):
        return "criminal"
    if any(w in t for w in ("space", "nasa", "planet", "rocket", "science")):
        return "science"
    if any(w in t for w in ("park", "wildlife", "nature", "forest")):
        return "nature"
    if any(w in t for w in ("war", "history", "ancient", "empire", "century")):
        return "history"
    return "documentary"


def _scene_queries(topic: str, scenes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sc in scenes:
        idx = int(sc.get("scene_index", len(rows)))
        bg = ""
        se = sc.get("seedream_edit") or {}
        if isinstance(se, dict):
            bg = str(se.get("background") or "")
        elif isinstance(sc.get("visual_brief"), str):
            bg = sc["visual_brief"]
        beat = str(sc.get("story_beat") or "")
        parts = [topic, bg, beat]
        q = " ".join(p for p in parts if p).strip()
        q = re.sub(r"\s+", " ", q)[:160]
        rows.append({"scene_index": idx, "query": q or topic, "story_beat": beat})
    return rows


def _load_blueprint_scenes(blueprint_job_id: str) -> list[dict[str, Any]]:
    try:
        from studio_agent import competitor

        work = (competitor.WORK_ROOT / blueprint_job_id).resolve()
        path = work / "scene_blueprint.json"
        if path.is_file():
            data = json.loads(path.read_text(encoding="utf-8"))
            return list(data.get("scenes") or [])
    except Exception:
        pass
    return []


def _search_video_biased(query: str, *, preset: str, limit: int) -> list[dict[str, Any]]:
    """Fan-out with extra video-weighted passes on IA + LOC + NASA."""
    pool: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _add(rows: list[dict[str, Any]]) -> None:
        for r in rows:
            if not isinstance(r, dict) or r.get("error"):
                continue
            key = f"{r.get('source')}:{r.get('id')}:{r.get('page_url')}"
            if key in seen:
                continue
            seen.add(key)
            pool.append(r)

    base = search_archival(query, preset=preset, limit_per_source=max(3, limit // 2))
    _add(base.get("results") or [])

    try:
        _add(clients.internet_archive(query, media_type="movies", collection="prelinger", limit=limit))
        _add(clients.internet_archive(query, media_type="movies", collection="stock", limit=limit))
    except Exception:
        pass
    try:
        _add(clients.loc(query, media_kind="film-and-videos", limit=limit))
    except Exception:
        pass
    try:
        _add(clients.nasa(query, media_type="video", limit=limit))
    except Exception:
        pass
    try:
        _add(clients.wikimedia(f"{query} video", limit=limit))
    except Exception:
        pass

    videos = [r for r in pool if r.get("media_type") == "video"]
    images = [r for r in pool if r.get("media_type") != "video"]
    return (videos + images)[: limit * 3]


def resolve_direct_download(item: dict[str, Any]) -> dict[str, Any]:
    """Attach direct_file_urls[] to a normalized archival result."""
    src = str(item.get("source") or "")
    ident = str(item.get("id") or "")
    out = dict(item)
    files: list[dict[str, Any]] = []

    if src == "internet_archive" and ident:
        raw = clients.internet_archive_files(ident, want_formats=("mp4", "ogv", "mpeg", "webm"))
        for f in raw:
            size = int(f.get("size") or 0)
            if size and size > 800_000_000:
                continue
            files.append(f)
        files.sort(key=lambda x: int(x.get("size") or 0), reverse=True)

    elif src == "nasa" and ident:
        urls = clients.nasa_asset(ident)
        for u in urls:
            if ".mp4" in u.lower() or ".mov" in u.lower():
                files.append({"name": u.rsplit("/", 1)[-1], "download_url": u})

    elif item.get("download_url"):
        files.append({
            "name": item.get("title", "asset"),
            "download_url": item["download_url"],
        })

    out["direct_files"] = files[:5]
    out["best_download_url"] = (files[0].get("download_url") if files else "") or item.get("download_url") or ""
    return out


def _rank_for_scene(assets: list[dict[str, Any]], *, prefer_video: bool = True) -> dict[str, Any] | None:
    if not assets:
        return None
    scored: list[tuple[int, dict[str, Any]]] = []
    for a in assets:
        score = 0
        if prefer_video and a.get("media_type") == "video":
            score += 10
        if a.get("best_download_url"):
            score += 5
        if a.get("thumbnail_url"):
            score += 1
        lic = str(a.get("license") or "").lower()
        if "public domain" in lic or "cc0" in lic:
            score += 2
        scored.append((score, a))
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]


def fetch_archival_for_video(
    topic: str,
    *,
    title: str = "",
    registry_key: str = "",
    preset: str = "",
    blueprint_job_id: str = "",
    scenes: list[dict[str, Any]] | None = None,
    limit_per_scene: int = 5,
    resolve_downloads: bool = True,
    production_job_id: str = "",
) -> dict[str, Any]:
    """
    Build a per-scene archival manifest for one exact video (topic + optional blueprint).
    """
    topic = (topic or "").strip()
    if not topic:
        raise ValueError("topic required")

    chosen_preset = _infer_preset(topic, registry_key, preset)
    scene_list = list(scenes or [])
    if blueprint_job_id and not scene_list:
        scene_list = _load_blueprint_scenes(blueprint_job_id)

    job_id = (production_job_id or blueprint_job_id or uuid.uuid4().hex[:12]).strip()
    manifest_dir = (_MANIFEST_ROOT / job_id).resolve()
    manifest_dir.mkdir(parents=True, exist_ok=True)

    global_query = " ".join(x for x in [topic, title] if x).strip()[:200]
    global_raw = _search_video_biased(global_query, preset=chosen_preset, limit=limit_per_scene + 2)
    global_pool = [resolve_direct_download(r) for r in global_raw] if resolve_downloads else global_raw

    per_scene: list[dict[str, Any]] = []
    if scene_list:
        for row in _scene_queries(topic, scene_list):
            raw = _search_video_biased(row["query"], preset=chosen_preset, limit=limit_per_scene)
            assets = [resolve_direct_download(r) for r in raw] if resolve_downloads else raw
            per_scene.append({
                **row,
                "match_count": len(assets),
                "assets": assets,
                "recommended": _rank_for_scene(assets),
            })
    else:
        per_scene.append({
            "scene_index": 0,
            "query": global_query,
            "story_beat": "full_video",
            "match_count": len(global_pool),
            "assets": global_pool[: limit_per_scene * 2],
            "recommended": _rank_for_scene(global_pool),
        })

    manifest = {
        "production_job_id": job_id,
        "topic": topic,
        "title": title,
        "registry_key": registry_key,
        "preset": chosen_preset,
        "sources_used": PRESETS.get(chosen_preset, PRESETS["documentary"]),
        "global_query": global_query,
        "global_pool": global_pool[:15],
        "per_scene": per_scene,
        "scene_count": len(per_scene),
        "usage_rules": [
            "Prefer per_scene.recommended.best_download_url for B-roll; Ken Burns on stills if only images.",
            "Cite source + license in video description (Internet Archive, LOC, NASA, Wikimedia, NPS, FBI).",
            "Pair with Seedream edit scenes for synthetic shots; archival for real-world proof/evidence.",
        ],
        "manifest_path": str(manifest_dir / "archival_manifest.json"),
    }
    try:
        (manifest_dir / "archival_manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass
    return manifest
