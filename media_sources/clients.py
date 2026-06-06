"""Free / public-domain external media source clients.

All sources here are free to query and (with per-result license filtering)
commercial-safe. They give Studio a non-AI media pool — archival footage,
historical stills, mugshots/records, royalty-free music, CC0 sound effects —
to mix with fal generations. Higher quality, lower fal spend.

Sources:
  - internet_archive : Prelinger / stock_footage / Pond5-PD collections (no key)
  - nasa             : space / earth / science footage + stills (no key)
  - loc              : Library of Congress photos + film (no key)
  - wikimedia        : Wikimedia Commons PD/CC media (no key)
  - nps              : National Park Service multimedia (NPS_API_KEY optional)
  - fbi              : FBI public records / wanted (mugshots, posters) (no key)
  - jamendo          : Creative Commons music (JAMENDO_CLIENT_ID)
  - freesound        : CC0 / CC sound effects (FREESOUND_API_KEY)

Every client returns a list of normalized dicts (see _result()).
Network failures degrade to an empty list + an "errors" channel at the
dispatch layer, never an exception that kills the agent turn.
"""
from __future__ import annotations

import os
from typing import Any

import httpx

_TIMEOUT = httpx.Timeout(20.0, connect=10.0)
_UA = {"User-Agent": "NYPTID-Studio/1.0 (+https://studio.nyptidindustries.com)"}


def _result(
    *,
    source: str,
    title: str,
    media_type: str,
    page_url: str = "",
    download_url: str = "",
    thumbnail_url: str = "",
    license: str = "",
    creator: str = "",
    description: str = "",
    identifier: str = "",
    duration_sec: float | None = None,
    width: int | None = None,
    height: int | None = None,
) -> dict[str, Any]:
    return {
        "source": source,
        "title": (title or "").strip()[:300],
        "media_type": media_type,
        "page_url": page_url,
        "download_url": download_url,
        "thumbnail_url": thumbnail_url,
        "license": license,
        "creator": (creator or "").strip()[:200],
        "description": (description or "").strip()[:600],
        "id": str(identifier or ""),
        "duration_sec": duration_sec,
        "width": width,
        "height": height,
    }


# ---------------------------------------------------------------------------
# Internet Archive — Prelinger, stock_footage, Pond5 PD, general PD/CC media.
# ---------------------------------------------------------------------------
IA_COLLECTIONS = {
    "stock": "stock_footage",
    "prelinger": "prelinger",
    "pond5": "pond5",
    "newsreels": "newsandpublicaffairs",
}


def internet_archive(query: str, *, media_type: str = "movies", collection: str = "", limit: int = 12) -> list[dict[str, Any]]:
    q_parts = [f'({query})']
    if media_type:
        q_parts.append(f"mediatype:({media_type})")
    if collection:
        coll = IA_COLLECTIONS.get(collection, collection)
        q_parts.append(f"collection:({coll})")
    params = {
        "q": " AND ".join(q_parts),
        "fl[]": ["identifier", "title", "creator", "licenseurl", "year", "description"],
        "rows": str(max(1, min(limit, 40))),
        "page": "1",
        "output": "json",
        "sort[]": "downloads desc",
    }
    out: list[dict[str, Any]] = []
    with httpx.Client(timeout=_TIMEOUT, headers=_UA) as client:
        r = client.get("https://archive.org/advancedsearch.php", params=params)
        r.raise_for_status()
        docs = (((r.json() or {}).get("response") or {}).get("docs")) or []
    media_kind = "video" if media_type == "movies" else ("audio" if media_type == "audio" else "image")
    for d in docs:
        ident = str(d.get("identifier") or "")
        if not ident:
            continue
        desc = d.get("description")
        if isinstance(desc, list):
            desc = " ".join(str(x) for x in desc)
        out.append(_result(
            source="internet_archive",
            title=str(d.get("title") or ident),
            media_type=media_kind,
            page_url=f"https://archive.org/details/{ident}",
            download_url=f"https://archive.org/download/{ident}",
            thumbnail_url=f"https://archive.org/services/img/{ident}",
            license=str(d.get("licenseurl") or "Public Domain / see item"),
            creator=str(d.get("creator") or ""),
            description=str(desc or ""),
            identifier=ident,
        ))
    return out


def internet_archive_files(identifier: str, *, want_formats: tuple[str, ...] = ("mp4", "mpeg", "ogv", "mp3", "jpg", "png")) -> list[dict[str, Any]]:
    """Resolve direct downloadable file URLs for a specific IA item."""
    ident = identifier.strip()
    if not ident:
        return []
    with httpx.Client(timeout=_TIMEOUT, headers=_UA) as client:
        r = client.get(f"https://archive.org/metadata/{ident}")
        r.raise_for_status()
        meta = r.json() or {}
    files = meta.get("files") or []
    out: list[dict[str, Any]] = []
    for f in files:
        name = str(f.get("name") or "")
        ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
        if want_formats and ext not in want_formats:
            continue
        out.append({
            "name": name,
            "format": f.get("format"),
            "size": int(f.get("size") or 0) if str(f.get("size") or "").isdigit() else None,
            "download_url": f"https://archive.org/download/{ident}/{name}",
        })
    return out


# ---------------------------------------------------------------------------
# NASA Image and Video Library.
# ---------------------------------------------------------------------------
def nasa(query: str, *, media_type: str = "image,video", limit: int = 12) -> list[dict[str, Any]]:
    params = {"q": query, "media_type": media_type}
    with httpx.Client(timeout=_TIMEOUT, headers=_UA) as client:
        r = client.get("https://images-api.nasa.gov/search", params=params)
        r.raise_for_status()
        items = (((r.json() or {}).get("collection") or {}).get("items")) or []
    out: list[dict[str, Any]] = []
    for it in items[: max(1, min(limit, 40))]:
        data = (it.get("data") or [{}])[0]
        links = it.get("links") or []
        thumb = next((l.get("href") for l in links if l.get("render") == "image"), "")
        nasa_id = str(data.get("nasa_id") or "")
        out.append(_result(
            source="nasa",
            title=str(data.get("title") or nasa_id),
            media_type=str(data.get("media_type") or "image"),
            page_url=f"https://images.nasa.gov/details/{nasa_id}",
            download_url=str(it.get("href") or ""),  # collection.json listing assets
            thumbnail_url=str(thumb or ""),
            license="Public Domain (NASA, with usage guidelines)",
            creator=str(data.get("photographer") or data.get("center") or "NASA"),
            description=str(data.get("description") or ""),
            identifier=nasa_id,
        ))
    return out


def nasa_asset(nasa_id: str) -> list[str]:
    nid = nasa_id.strip()
    if not nid:
        return []
    with httpx.Client(timeout=_TIMEOUT, headers=_UA) as client:
        r = client.get(f"https://images-api.nasa.gov/asset/{nid}")
        r.raise_for_status()
        items = (((r.json() or {}).get("collection") or {}).get("items")) or []
    return [str(i.get("href")) for i in items if i.get("href")]


# ---------------------------------------------------------------------------
# Library of Congress — photos + film/video.
# ---------------------------------------------------------------------------
def loc(query: str, *, media_kind: str = "photos", limit: int = 12) -> list[dict[str, Any]]:
    # media_kind in {photos, film-and-videos, audio}
    base = f"https://www.loc.gov/{media_kind}/"
    params = {"q": query, "fo": "json", "c": str(max(1, min(limit, 40)))}
    with httpx.Client(timeout=_TIMEOUT, headers=_UA, follow_redirects=True) as client:
        r = client.get(base, params=params)
        r.raise_for_status()
        results = (r.json() or {}).get("results") or []
    kind = "video" if "film" in media_kind else ("audio" if media_kind == "audio" else "image")

    def _first_str(value: Any) -> str:
        if isinstance(value, list):
            return str(value[0]) if value else ""
        return str(value or "")

    out: list[dict[str, Any]] = []
    for it in results[: max(1, min(limit, 40))]:
        img = it.get("image_url") or []
        thumb = img[0] if isinstance(img, list) and img else (img if isinstance(img, str) else "")
        out.append(_result(
            source="loc",
            title=str(it.get("title") or ""),
            media_type=kind,
            page_url=str(it.get("id") or it.get("url") or ""),
            download_url="",
            thumbnail_url=str(thumb or ""),
            license="Library of Congress — no known restrictions (verify per item)",
            creator=_first_str(it.get("contributor")),
            description=_first_str(it.get("description")),
            identifier=str(it.get("id") or ""),
        ))
    return out


# ---------------------------------------------------------------------------
# Wikimedia Commons.
# ---------------------------------------------------------------------------
def wikimedia(query: str, *, limit: int = 12) -> list[dict[str, Any]]:
    params = {
        "action": "query",
        "format": "json",
        "generator": "search",
        "gsrsearch": query,
        "gsrnamespace": "6",  # File:
        "gsrlimit": str(max(1, min(limit, 40))),
        "prop": "imageinfo",
        "iiprop": "url|extmetadata|mime|size",
    }
    with httpx.Client(timeout=_TIMEOUT, headers=_UA) as client:
        r = client.get("https://commons.wikimedia.org/w/api.php", params=params)
        r.raise_for_status()
        pages = (((r.json() or {}).get("query") or {}).get("pages")) or {}
    out: list[dict[str, Any]] = []
    for page in pages.values():
        info = (page.get("imageinfo") or [{}])[0]
        meta = info.get("extmetadata") or {}
        mime = str(info.get("mime") or "")
        kind = "video" if mime.startswith("video") else ("audio" if mime.startswith("audio") else "image")
        lic = str((meta.get("LicenseShortName") or {}).get("value") or "see file page")
        artist = str((meta.get("Artist") or {}).get("value") or "")
        # crude HTML strip for artist
        if "<" in artist:
            import re as _re
            artist = _re.sub(r"<[^>]+>", "", artist)
        out.append(_result(
            source="wikimedia",
            title=str(page.get("title") or "").replace("File:", ""),
            media_type=kind,
            page_url=str(info.get("descriptionurl") or ""),
            download_url=str(info.get("url") or ""),
            thumbnail_url=str(info.get("url") or ""),
            license=lic,
            creator=artist,
            description="",
            identifier=str(page.get("pageid") or ""),
            width=int(info.get("width") or 0) or None,
            height=int(info.get("height") or 0) or None,
        ))
    return out


# ---------------------------------------------------------------------------
# National Park Service multimedia (optional key).
# ---------------------------------------------------------------------------
def nps(query: str, *, limit: int = 12) -> list[dict[str, Any]]:
    key = os.getenv("NPS_API_KEY", "").strip()
    if not key:
        # Fall back to NPS B-roll hosted on Internet Archive.
        return internet_archive(query, media_type="movies", collection="nationalparkservice", limit=limit)
    params = {"q": query, "limit": str(max(1, min(limit, 40))), "api_key": key}
    out: list[dict[str, Any]] = []
    with httpx.Client(timeout=_TIMEOUT, headers=_UA) as client:
        r = client.get("https://developer.nps.gov/api/v1/multimedia/videos", params=params)
        r.raise_for_status()
        data = (r.json() or {}).get("data") or []
    for d in data:
        out.append(_result(
            source="nps",
            title=str(d.get("title") or ""),
            media_type="video",
            page_url=str(d.get("permalinkUrl") or d.get("url") or ""),
            download_url=str(d.get("url") or ""),
            thumbnail_url=str((d.get("splashImage") or {}).get("url") or ""),
            license="NPS — public domain (US Government work)",
            creator="National Park Service",
            description=str(d.get("description") or ""),
            identifier=str(d.get("id") or ""),
            duration_sec=float(d.get("durationMs") or 0) / 1000.0 if d.get("durationMs") else None,
        ))
    return out


# ---------------------------------------------------------------------------
# FBI public records / wanted — mugshots, wanted posters, case imagery.
# Critical for criminal-case content (Empire Magnates).
# ---------------------------------------------------------------------------
def fbi(query: str, *, limit: int = 12) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"pageSize": str(max(1, min(limit, 40))), "page": "1"}
    if query.strip():
        params["title"] = query.strip()
    with httpx.Client(timeout=_TIMEOUT, headers=_UA) as client:
        r = client.get("https://api.fbi.gov/wanted/v1/list", params=params)
        r.raise_for_status()
        items = (r.json() or {}).get("items") or []
    out: list[dict[str, Any]] = []
    for it in items[: max(1, min(limit, 40))]:
        images = it.get("images") or []
        thumb = str((images[0] or {}).get("thumb") or "") if images else ""
        large = str((images[0] or {}).get("large") or (images[0] or {}).get("original") or "") if images else ""
        out.append(_result(
            source="fbi",
            title=str(it.get("title") or ""),
            media_type="image",
            page_url=str(it.get("url") or ""),
            download_url=large,
            thumbnail_url=thumb,
            license="US Government work (FBI) — public domain",
            creator="FBI",
            description=str((it.get("description") or it.get("details") or "") or "")[:600],
            identifier=str(it.get("uid") or ""),
        ))
    return out


# ---------------------------------------------------------------------------
# Jamendo — Creative Commons music.
# ---------------------------------------------------------------------------
def jamendo(query: str, *, limit: int = 12, instrumental: bool = False) -> list[dict[str, Any]]:
    cid = os.getenv("JAMENDO_CLIENT_ID", "").strip()
    if not cid:
        return [{"error": "JAMENDO_CLIENT_ID not set — get one free at developer.jamendo.com"}]
    params = {
        "client_id": cid,
        "format": "json",
        "limit": str(max(1, min(limit, 40))),
        "search": query,
        "audioformat": "mp32",
        "include": "musicinfo",
        "groupby": "artist_id",
    }
    if instrumental:
        params["vocalinstrumental"] = "instrumental"
    with httpx.Client(timeout=_TIMEOUT, headers=_UA) as client:
        r = client.get("https://api.jamendo.com/v3.0/tracks/", params=params)
        r.raise_for_status()
        results = (r.json() or {}).get("results") or []
    out: list[dict[str, Any]] = []
    for t in results:
        out.append(_result(
            source="jamendo",
            title=str(t.get("name") or ""),
            media_type="audio",
            page_url=str(t.get("shareurl") or ""),
            download_url=str(t.get("audiodownload") or t.get("audio") or ""),
            thumbnail_url=str(t.get("album_image") or t.get("image") or ""),
            license=str((t.get("license_ccurl") or "Creative Commons")),
            creator=str(t.get("artist_name") or ""),
            description=str(t.get("album_name") or ""),
            identifier=str(t.get("id") or ""),
            duration_sec=float(t.get("duration") or 0) or None,
        ))
    return out


# ---------------------------------------------------------------------------
# Freesound — sound effects (filter CC0 by default for attribution-free use).
# Preview URLs are token-accessible; full-quality download needs OAuth2.
# ---------------------------------------------------------------------------
def freesound(query: str, *, limit: int = 12, cc0_only: bool = True) -> list[dict[str, Any]]:
    key = os.getenv("FREESOUND_API_KEY", "").strip()
    if not key:
        return [{"error": "FREESOUND_API_KEY not set — get one free at freesound.org/apiv2/apply"}]
    params = {
        "query": query,
        "token": key,
        "page_size": str(max(1, min(limit, 40))),
        "fields": "id,name,username,license,previews,duration,description,url",
    }
    if cc0_only:
        params["filter"] = 'license:"Creative Commons 0"'
    with httpx.Client(timeout=_TIMEOUT, headers=_UA) as client:
        r = client.get("https://freesound.org/apiv2/search/text/", params=params)
        r.raise_for_status()
        results = (r.json() or {}).get("results") or []
    out: list[dict[str, Any]] = []
    for s in results:
        prev = s.get("previews") or {}
        out.append(_result(
            source="freesound",
            title=str(s.get("name") or ""),
            media_type="audio",
            page_url=str(s.get("url") or ""),
            download_url=str(prev.get("preview-hq-mp3") or prev.get("preview-lq-mp3") or ""),
            thumbnail_url="",
            license=str(s.get("license") or ""),
            creator=str(s.get("username") or ""),
            description=str(s.get("description") or ""),
            identifier=str(s.get("id") or ""),
            duration_sec=float(s.get("duration") or 0) or None,
        ))
    return out
