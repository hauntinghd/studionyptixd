import argparse
import json
import os
import re
import subprocess
import time
from collections import Counter
from pathlib import Path
from typing import Any

import httpx

try:
    import yt_dlp
except Exception:
    yt_dlp = None


ROOT = Path(__file__).resolve().parents[1]
ANALYSIS_DIR = ROOT / "analysis"
CORPUS_DIR = ANALYSIS_DIR / "catalyst_documentary_corpus"
RAW_CHANNELS_DIR = CORPUS_DIR / "channels"
SEEDS_FILE = ANALYSIS_DIR / "catalyst_documentary_channels.json"
REFERENCE_MEMORY_FILE = CORPUS_DIR / "reference_memory.json"
VIDEO_INDEX_FILE = CORPUS_DIR / "video_index.jsonl"
OPS_REFERENCE_FILE = ROOT / "ops" / "catalyst_documentary_reference_memory.json"
YOUTUBE_DATA_API_BASE = "https://www.googleapis.com/youtube/v3"
STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "from", "into", "your", "their", "will",
    "about", "what", "when", "where", "how", "why", "who", "which", "have", "they", "them",
    "then", "than", "were", "been", "being", "over", "more", "most", "does", "did", "just",
    "because", "after", "before", "under", "only", "also", "like", "through", "video", "videos",
    "channel", "documentary", "explainer", "story", "stories", "full"
}
CURIOUS_WORDS = {
    "hidden", "secret", "secrets", "why", "how", "inside", "truth", "truths", "dark", "real",
    "dangerous", "disturbing", "exposed", "collapse", "killed", "broken", "manipulates", "lies",
    "quietly", "control", "war", "brain", "mind", "money", "power", "risk", "system"
}
STAKES_WORDS = {
    "dangerous", "killed", "collapse", "war", "power", "money", "control", "threat", "risk",
    "dead", "fraud", "crime", "secrets", "disturbing", "lies"
}


def _clip(text: str, max_chars: int) -> str:
    raw = str(text or "").strip()
    if len(raw) <= max_chars:
        return raw
    return raw[: max(0, max_chars - 3)].rstrip() + "..."


def _slugify(text: str) -> str:
    raw = re.sub(r"[^a-zA-Z0-9]+", "_", str(text or "").strip().lower()).strip("_")
    return raw or "item"


def _tokenize(text: str) -> list[str]:
    words = re.findall(r"[A-Za-z0-9']+", str(text or "").lower())
    out: list[str] = []
    for word in words:
        if len(word) <= 2 or word in STOPWORDS:
            continue
        out.append(word)
    return out


def _dedupe_keep_order(values: list[str], *, max_items: int = 12, max_chars: int = 180) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in values:
        text = _clip(str(raw or "").strip(), max_chars)
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


def _weighted_signal_map(signals: list[str], weight: float, *, max_items: int = 12) -> dict[str, float]:
    scores: dict[str, float] = {}
    display: dict[str, str] = {}
    for raw in signals:
        text = _clip(str(raw or "").strip(), 180)
        if not text:
            continue
        key = text.lower()
        scores[key] = round(scores.get(key, 0.0) + float(weight), 4)
        display[key] = text
    ranked = sorted(scores.items(), key=lambda item: (-item[1], display[item[0]].lower()))
    return {display[key]: round(score, 4) for key, score in ranked[:max_items]}


def _weight_from_views(views: int) -> float:
    v = max(0, int(views or 0))
    if v >= 3_000_000:
        return 5.0
    if v >= 1_000_000:
        return 4.5
    if v >= 300_000:
        return 4.0
    if v >= 100_000:
        return 3.5
    if v >= 30_000:
        return 3.0
    if v >= 10_000:
        return 2.5
    if v >= 3_000:
        return 2.0
    if v >= 1_000:
        return 1.5
    return 1.0


def _pick_subtitle_candidate(info: dict, language: str = "en") -> tuple[str, str]:
    preferred = []
    lang = str(language or "en").strip().lower()
    if lang:
        preferred.extend([lang, f"{lang}-us", f"{lang}-en"])
    preferred.extend(["en-us", "en", "en-gb"])
    for pool_name in ("subtitles", "automatic_captions"):
        pool = info.get(pool_name) or {}
        if not isinstance(pool, dict):
            continue
        for wanted in preferred:
            variants = [wanted]
            if "-" in wanted:
                variants.append(wanted.split("-", 1)[0])
            for variant in variants:
                entries = pool.get(variant)
                if not isinstance(entries, list):
                    continue
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    url = str(entry.get("url", "") or "").strip()
                    ext = str(entry.get("ext", "") or "").strip().lower()
                    if url and ext == "vtt":
                        return url, ext
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    url = str(entry.get("url", "") or "").strip()
                    ext = str(entry.get("ext", "") or "").strip().lower()
                    if url:
                        return url, ext or "unknown"
    return "", ""


def _parse_vtt_text(raw_text: str) -> str:
    lines = []
    for raw in str(raw_text or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("WEBVTT") or "-->" in line or re.match(r"^\d+$", line):
            continue
        line = re.sub(r"<[^>]+>", " ", line)
        line = re.sub(r"&[a-zA-Z]+;", " ", line)
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            lines.append(line)
    return _clip(" ".join(lines), 12000)


def _youtube_get(client: httpx.Client, api_key: str, path: str, params: dict[str, Any]) -> dict:
    merged = dict(params or {})
    merged["key"] = api_key
    resp = client.get(f"{YOUTUBE_DATA_API_BASE}{path}", params=merged)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, dict):
        raise RuntimeError("YouTube API returned invalid payload")
    return payload


def _parse_iso_duration(raw_duration: str) -> int:
    value = str(raw_duration or "").strip().upper()
    if not value.startswith("PT"):
        return 0
    hours = int((re.search(r"(\d+)H", value) or [None, 0])[1]) if re.search(r"(\d+)H", value) else 0
    minutes = int((re.search(r"(\d+)M", value) or [None, 0])[1]) if re.search(r"(\d+)M", value) else 0
    seconds = int((re.search(r"(\d+)S", value) or [None, 0])[1]) if re.search(r"(\d+)S", value) else 0
    return (hours * 3600) + (minutes * 60) + seconds


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _fetch_text(client: httpx.Client, url: str) -> str:
    resp = client.get(url, follow_redirects=True)
    resp.raise_for_status()
    return resp.text


def _resolve_channel(client: httpx.Client, api_key: str, seed: dict[str, Any]) -> dict[str, Any]:
    expected_title = str(seed.get("expected_channel_title", "") or "").strip().lower()
    query = str(seed.get("query", "") or "").strip()
    if not query:
        raise RuntimeError(f"Seed {seed.get('slug')} has no query")
    payload = _youtube_get(
        client,
        api_key,
        "/search",
        {
            "part": "snippet",
            "type": "channel",
            "maxResults": 8,
            "q": query,
        },
    )
    items = list(payload.get("items") or [])
    if not items:
        raise RuntimeError(f"No channel results for query: {query}")
    best_score = -1.0
    best_channel_id = ""
    best_title = ""
    for item in items:
        if not isinstance(item, dict):
            continue
        channel_id = str(((item.get("id") or {}).get("channelId")) or "").strip()
        title = str(((item.get("snippet") or {}).get("title")) or "").strip()
        expected_tokens = set(_tokenize(expected_title))
        title_tokens = set(_tokenize(title))
        score = 0.0
        if expected_title and expected_title == title.lower():
            score += 3.0
        if expected_title and expected_title in title.lower():
            score += 1.25
        if expected_tokens and title_tokens:
            score += len(expected_tokens & title_tokens) * 0.5
        if str(seed.get("slug", "") or "").replace("_", "") in title.lower().replace(" ", ""):
            score += 0.75
        if query.lower().split()[0] in title.lower():
            score += 0.5
        if score > best_score:
            best_score = score
            best_channel_id = channel_id
            best_title = title
    if not best_channel_id:
        raise RuntimeError(f"Failed to resolve channel for query: {query}")
    channel_payload = _youtube_get(
        client,
        api_key,
        "/channels",
        {
            "part": "snippet,statistics,contentDetails",
            "id": best_channel_id,
            "maxResults": 1,
        },
    )
    raw = dict((channel_payload.get("items") or [{}])[0] or {})
    snippet = dict(raw.get("snippet") or {})
    stats = dict(raw.get("statistics") or {})
    content = dict(raw.get("contentDetails") or {})
    thumbs = dict(snippet.get("thumbnails") or {})
    related = dict(content.get("relatedPlaylists") or {})
    return {
        "channel_id": best_channel_id,
        "resolved_title": best_title,
        "title": str(snippet.get("title", "") or "").strip(),
        "description": str(snippet.get("description", "") or "").strip(),
        "custom_url": str(snippet.get("customUrl", "") or "").strip(),
        "published_at": str(snippet.get("publishedAt", "") or "").strip(),
        "thumbnail_url": str((((thumbs.get("high") or {}).get("url")) or ((thumbs.get("medium") or {}).get("url")) or ((thumbs.get("default") or {}).get("url")) or "")).strip(),
        "subscriber_count": int(float(stats.get("subscriberCount", 0) or 0)),
        "video_count": int(float(stats.get("videoCount", 0) or 0)),
        "view_count": int(float(stats.get("viewCount", 0) or 0)),
        "uploads_playlist_id": str(related.get("uploads", "") or "").strip(),
        "channel_url": f"https://www.youtube.com/channel/{best_channel_id}",
    }


def _fetch_playlist_video_ids(client: httpx.Client, api_key: str, uploads_playlist_id: str, max_results: int) -> list[str]:
    if not uploads_playlist_id:
        return []
    out: list[str] = []
    page_token = ""
    while len(out) < max_results:
        payload = _youtube_get(
            client,
            api_key,
            "/playlistItems",
            {
                "part": "contentDetails",
                "playlistId": uploads_playlist_id,
                "maxResults": min(50, max_results - len(out)),
                "pageToken": page_token,
            },
        )
        for item in list(payload.get("items") or []):
            if not isinstance(item, dict):
                continue
            vid = str(((item.get("contentDetails") or {}).get("videoId")) or "").strip()
            if vid:
                out.append(vid)
        page_token = str(payload.get("nextPageToken", "") or "").strip()
        if not page_token:
            break
    return out[:max_results]


def _fetch_search_video_ids(client: httpx.Client, api_key: str, channel_id: str, order: str, max_results: int) -> list[str]:
    payload = _youtube_get(
        client,
        api_key,
        "/search",
        {
            "part": "snippet",
            "channelId": channel_id,
            "type": "video",
            "order": order,
            "maxResults": min(25, max_results),
        },
    )
    out: list[str] = []
    for item in list(payload.get("items") or []):
        if not isinstance(item, dict):
            continue
        vid = str(((item.get("id") or {}).get("videoId")) or "").strip()
        if vid:
            out.append(vid)
    return out[:max_results]


def _fetch_videos(client: httpx.Client, api_key: str, video_ids: list[str]) -> list[dict[str, Any]]:
    if not video_ids:
        return []
    ordered_ids = []
    seen: set[str] = set()
    for raw in video_ids:
        vid = str(raw or "").strip()
        if vid and vid not in seen:
            seen.add(vid)
            ordered_ids.append(vid)
    by_id: dict[str, dict[str, Any]] = {}
    for start in range(0, len(ordered_ids), 50):
        batch = ordered_ids[start:start + 50]
        payload = _youtube_get(
            client,
            api_key,
            "/videos",
            {
                "part": "snippet,contentDetails,statistics",
                "id": ",".join(batch),
                "maxResults": len(batch),
            },
        )
        for raw in list(payload.get("items") or []):
            if not isinstance(raw, dict):
                continue
            vid = str(raw.get("id", "") or "").strip()
            snippet = dict(raw.get("snippet") or {})
            stats = dict(raw.get("statistics") or {})
            thumbs = dict(snippet.get("thumbnails") or {})
            duration = str((raw.get("contentDetails") or {}).get("duration", "") or "").strip()
            by_id[vid] = {
                "video_id": vid,
                "url": f"https://www.youtube.com/watch?v={vid}",
                "title": str(snippet.get("title", "") or "").strip(),
                "description": str(snippet.get("description", "") or "").strip(),
                "published_at": str(snippet.get("publishedAt", "") or "").strip(),
                "channel_title": str(snippet.get("channelTitle", "") or "").strip(),
                "thumbnail_url": str((((thumbs.get("high") or {}).get("url")) or ((thumbs.get("medium") or {}).get("url")) or ((thumbs.get("default") or {}).get("url")) or "")).strip(),
                "tags": [str(v).strip() for v in list(snippet.get("tags") or []) if str(v).strip()][:20],
                "duration_sec": _parse_iso_duration(duration),
                "views": int(float(stats.get("viewCount", 0) or 0)),
                "likes": int(float(stats.get("likeCount", 0) or 0)),
                "comments": int(float(stats.get("commentCount", 0) or 0)),
            }
    return [by_id[vid] for vid in ordered_ids if vid in by_id]


def _yt_dlp_extract_info(url: str) -> dict[str, Any]:
    if yt_dlp is None:
        return {}
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "extract_flat": False,
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False) or {}


def _download_thumbnail(client: httpx.Client, url: str, out_path: Path) -> str:
    if not url:
        return ""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    resp = client.get(url, follow_redirects=True)
    resp.raise_for_status()
    out_path.write_bytes(resp.content)
    return str(out_path)


def _download_lowres_video(url: str, out_path: Path) -> str:
    if yt_dlp is None:
        return ""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists() and out_path.stat().st_size > 1024 * 1024:
        return str(out_path)
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "overwrites": True,
        "noplaylist": True,
        "format": "worst[ext=mp4]/worst",
        "outtmpl": str(out_path),
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        ydl.download([url])
    return str(out_path) if out_path.exists() else ""


def _extract_reference_frames(video_path: Path, frames_dir: Path, sample_every_sec: int = 15, max_duration_sec: int = 240) -> list[str]:
    frames_dir.mkdir(parents=True, exist_ok=True)
    frame_pattern = frames_dir / "frame_%03d.jpg"
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(video_path),
        "-t",
        str(max_duration_sec),
        "-vf",
        f"fps=1/{max(1, int(sample_every_sec))}",
        str(frame_pattern),
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return [str(path) for path in sorted(frames_dir.glob("frame_*.jpg"))]


def _summarize_title_patterns(videos: list[dict[str, Any]]) -> tuple[list[str], list[str], list[str]]:
    titles = [str(v.get("title", "") or "").strip() for v in videos if str(v.get("title", "") or "").strip()]
    tokens = Counter()
    numbered = 0
    curiosity = 0
    stakes = 0
    short_titles = 0
    for title in titles:
        tokens.update(_tokenize(title))
        low = title.lower()
        if re.search(r"\b\d+\b", title):
            numbered += 1
        if any(word in low for word in CURIOUS_WORDS):
            curiosity += 1
        if any(word in low for word in STAKES_WORDS):
            stakes += 1
        if len(_tokenize(title)) <= 10:
            short_titles += 1
    proven_keywords = [word for word, _count in tokens.most_common(14)]
    total = max(1, len(titles))
    packaging = []
    if numbered / total >= 0.3:
        packaging.append("Number-led packaging shows up repeatedly in the public winners.")
    if curiosity / total >= 0.45:
        packaging.append("Curiosity-led wording appears frequently in the top public titles.")
    if stakes / total >= 0.35:
        packaging.append("High-stakes language is a recurring pattern in the strongest public videos.")
    if short_titles / total >= 0.5:
        packaging.append("Winning titles tend to stay compact instead of over-explaining the whole topic.")
    if not packaging:
        packaging.append("Winning packaging keeps one dominant promise and one obvious focal idea.")
    title_hints = []
    if proven_keywords:
        title_hints.append("Winning topics and keywords cluster around: " + ", ".join(proven_keywords[:8]))
    title_hints.append("Strong public titles stay in one recognizable arena without repeating exact phrasing.")
    next_moves = [
        "Keep the next video in the same arena, but shift the angle instead of recycling the source title.",
        "Promote one hidden mechanism, conflict, or payoff into the first line and thumbnail package.",
    ]
    return _dedupe_keep_order(proven_keywords, max_items=14, max_chars=80), _dedupe_keep_order(packaging, max_items=8), _dedupe_keep_order(title_hints + next_moves, max_items=10)


def _build_channel_memory_seed(channel: dict[str, Any], videos: list[dict[str, Any]], seed: dict[str, Any]) -> dict[str, Any]:
    top_videos = sorted(videos, key=lambda item: int(item.get("views", 0) or 0), reverse=True)[:12]
    proven_keywords, packaging, next_moves = _summarize_title_patterns(top_videos)
    style_notes = _clip(str(seed.get("style_notes", "") or ""), 200)
    top_titles = [str(v.get("title", "") or "").strip() for v in top_videos if str(v.get("title", "") or "").strip()]
    recent_titles = [str(v.get("title", "") or "").strip() for v in sorted(videos, key=lambda item: str(item.get("published_at", "") or ""), reverse=True)[:12] if str(v.get("title", "") or "").strip()]
    avg_views = int(sum(int(v.get("views", 0) or 0) for v in top_videos) / max(1, len(top_videos))) if top_videos else 0
    avg_duration = int(sum(int(v.get("duration_sec", 0) or 0) for v in top_videos) / max(1, len(top_videos))) if top_videos else 0
    weight = _weight_from_views(avg_views)
    hook_wins = [
        "Open with the central mechanism or hidden driver immediately instead of warming up slowly.",
        "Make the first 20 to 30 seconds pay off the title promise fast.",
    ]
    pacing_wins = [
        "Keep every sequence pushing one new reveal, contrast, or escalation beat.",
        "Avoid dead-air explanation blocks without a visual or narrative step-up.",
    ]
    visual_wins = [style_notes] if style_notes else []
    visual_wins.append("Favor premium designed CG, system cutaways, hero objects, map views, and obvious intentional composition.")
    sound_wins = [
        "Use tension-first sound design that escalates reveals instead of generic ambient beds.",
        "Treat SFX as narrative punctuation for discoveries, reversals, and consequences.",
    ]
    retention_watchouts = [
        "Do not let the intro stay abstract for too long; get to the concrete reveal quickly.",
        "Do not repeat the same visual framing or symbolic object without raising the stakes.",
    ]
    summary = _clip(
        f"Reference documentary corpus from {channel['title']}. "
        f"Public winners average about {avg_views:,} views and {avg_duration // 60 if avg_duration else 0} minutes. "
        f"Style direction: {style_notes}" if style_notes else
        f"Reference documentary corpus from {channel['title']}. Public winners average about {avg_views:,} views.",
        320,
    )
    return {
        "key": f"reference::{channel['channel_id']}::documentary",
        "channel_id": str(channel["channel_id"]),
        "format_preset": "documentary",
        "run_count": len(top_videos),
        "outcome_count": len(top_videos),
        "summary": summary,
        "proven_keywords": proven_keywords,
        "hook_learnings": hook_wins,
        "pacing_learnings": pacing_wins,
        "visual_learnings": _dedupe_keep_order(visual_wins, max_items=10),
        "sound_learnings": _dedupe_keep_order(sound_wins, max_items=8),
        "packaging_learnings": packaging,
        "retention_watchouts": retention_watchouts,
        "next_video_moves": next_moves,
        "recent_source_titles": recent_titles[:10],
        "recent_selected_titles": top_titles[:10],
        "preferred_transition_style": "premium_documentary_escalation",
        "preferred_music_profile": "cinematic_tension_documentary",
        "preferred_visual_engine": "catalyst_documentary_3d",
        "last_outcome_summary": summary,
        "outcome_views_sum": sum(int(v.get("views", 0) or 0) for v in top_videos),
        "outcome_impressions_sum": 0,
        "outcome_ctr_sum": 0.0,
        "outcome_avp_sum": 0.0,
        "outcome_avd_sum": float(sum(int(v.get("duration_sec", 0) or 0) for v in top_videos)),
        "outcome_first30_sum": 0.0,
        "outcome_first60_sum": 0.0,
        "hook_wins_map": _weighted_signal_map(hook_wins, weight),
        "pacing_wins_map": _weighted_signal_map(pacing_wins, weight),
        "visual_wins_map": _weighted_signal_map(visual_wins, weight),
        "sound_wins_map": _weighted_signal_map(sound_wins, weight),
        "packaging_wins_map": _weighted_signal_map(packaging, weight),
        "retention_watchouts_map": _weighted_signal_map(retention_watchouts, weight),
        "next_video_moves_map": _weighted_signal_map(next_moves, weight),
    }


def _merge_reference_memory(channel_entries: list[dict[str, Any]]) -> dict[str, Any]:
    aggregate = {
        "key": "reference::aggregate::documentary",
        "channel_id": "aggregate_documentary_reference",
        "format_preset": "documentary",
        "run_count": 0,
        "outcome_count": 0,
        "summary": "",
        "proven_keywords": [],
        "hook_learnings": [],
        "pacing_learnings": [],
        "visual_learnings": [],
        "sound_learnings": [],
        "packaging_learnings": [],
        "retention_watchouts": [],
        "next_video_moves": [],
        "recent_source_titles": [],
        "recent_selected_titles": [],
        "preferred_transition_style": "premium_documentary_escalation",
        "preferred_music_profile": "cinematic_tension_documentary",
        "preferred_visual_engine": "catalyst_documentary_3d",
        "outcome_views_sum": 0.0,
        "outcome_impressions_sum": 0.0,
        "outcome_ctr_sum": 0.0,
        "outcome_avp_sum": 0.0,
        "outcome_avd_sum": 0.0,
        "outcome_first30_sum": 0.0,
        "outcome_first60_sum": 0.0,
        "hook_wins_map": {},
        "pacing_wins_map": {},
        "visual_wins_map": {},
        "sound_wins_map": {},
        "packaging_wins_map": {},
        "retention_watchouts_map": {},
        "next_video_moves_map": {},
    }
    keywords: list[str] = []
    summaries: list[str] = []
    for entry in channel_entries:
        memory = dict(entry.get("memory_seed") or {})
        channel = dict(entry.get("channel") or {})
        weight = _weight_from_views(int(channel.get("view_count", 0) or 0) or int(entry.get("average_top_video_views", 0) or 0))
        aggregate["run_count"] += int(memory.get("run_count", 0) or 0)
        aggregate["outcome_count"] += int(memory.get("outcome_count", 0) or 0)
        aggregate["outcome_views_sum"] += float(memory.get("outcome_views_sum", 0.0) or 0.0)
        aggregate["outcome_avd_sum"] += float(memory.get("outcome_avd_sum", 0.0) or 0.0)
        keywords.extend(list(memory.get("proven_keywords") or []))
        summaries.append(str(memory.get("summary", "") or ""))
        for field in ("hook_wins_map", "pacing_wins_map", "visual_wins_map", "sound_wins_map", "packaging_wins_map", "retention_watchouts_map", "next_video_moves_map"):
            merged = dict(aggregate.get(field) or {})
            for text, score in dict(memory.get(field) or {}).items():
                merged[text] = round(float(merged.get(text, 0.0) or 0.0) + float(score or 0.0) * weight, 4)
            aggregate[field] = merged
    aggregate["proven_keywords"] = _dedupe_keep_order(keywords, max_items=18, max_chars=80)
    aggregate["hook_learnings"] = list((aggregate.get("hook_wins_map") or {}).keys())[:10]
    aggregate["pacing_learnings"] = list((aggregate.get("pacing_wins_map") or {}).keys())[:10]
    aggregate["visual_learnings"] = list((aggregate.get("visual_wins_map") or {}).keys())[:10]
    aggregate["sound_learnings"] = list((aggregate.get("sound_wins_map") or {}).keys())[:10]
    aggregate["packaging_learnings"] = list((aggregate.get("packaging_wins_map") or {}).keys())[:10]
    aggregate["retention_watchouts"] = list((aggregate.get("retention_watchouts_map") or {}).keys())[:10]
    aggregate["next_video_moves"] = list((aggregate.get("next_video_moves_map") or {}).keys())[:10]
    aggregate["summary"] = _clip(
        f"Reference documentary corpus across {len(channel_entries)} channels. "
        f"Use it as public winner context for packaging, hook design, pacing, visuals, and sound while staying channel-native.",
        320,
    )
    aggregate["last_outcome_summary"] = _clip(" | ".join(summaries[:3]), 320)
    return aggregate


def build_corpus(
    api_key: str,
    *,
    recent_limit: int,
    popular_limit: int,
    transcript_video_limit: int,
    frame_video_limit: int,
    sample_every_sec: int,
    max_frame_duration_sec: int,
) -> dict[str, Any]:
    seeds = json.loads(SEEDS_FILE.read_text(encoding="utf-8"))
    RAW_CHANNELS_DIR.mkdir(parents=True, exist_ok=True)
    CORPUS_DIR.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, Any]] = []
    video_jsonl_lines: list[str] = []
    with httpx.Client(timeout=45, follow_redirects=True) as client:
        for seed in seeds:
            slug = _slugify(seed.get("slug", "channel"))
            print(f"[Catalyst corpus] resolving {slug}...")
            channel = _resolve_channel(client, api_key, seed)
            channel_dir = RAW_CHANNELS_DIR / slug
            channel_dir.mkdir(parents=True, exist_ok=True)
            recent_ids = _fetch_playlist_video_ids(client, api_key, str(channel.get("uploads_playlist_id", "") or ""), recent_limit)
            popular_ids = _fetch_search_video_ids(client, api_key, str(channel.get("channel_id", "") or ""), "viewCount", popular_limit)
            videos = _fetch_videos(client, api_key, popular_ids + recent_ids)
            sorted_popular = sorted(videos, key=lambda item: int(item.get("views", 0) or 0), reverse=True)
            for idx, video in enumerate(videos):
                video_dir = channel_dir / "videos" / str(video["video_id"])
                video_dir.mkdir(parents=True, exist_ok=True)
                video["source_kind"] = "popular" if video["video_id"] in popular_ids else "recent"
                video["seed_slug"] = slug
                video["channel_slug"] = slug
                video["channel_style_notes"] = str(seed.get("style_notes", "") or "").strip()
                video["thumbnail_file"] = ""
                video["transcript_file"] = ""
                video["captions_file"] = ""
                video["reference_frames"] = []
                video["chapters"] = []
                if video.get("thumbnail_url"):
                    try:
                        video["thumbnail_file"] = _download_thumbnail(client, str(video["thumbnail_url"]), video_dir / "thumbnail.jpg")
                    except Exception:
                        video["thumbnail_file"] = ""
                transcript_allowed = idx < transcript_video_limit
                frame_allowed = idx < frame_video_limit
                if yt_dlp is not None and (transcript_allowed or frame_allowed):
                    try:
                        info = _yt_dlp_extract_info(str(video["url"]))
                    except Exception:
                        info = {}
                    if isinstance(info, dict):
                        for raw in list(info.get("chapters") or [])[:16]:
                            if isinstance(raw, dict):
                                video["chapters"].append(
                                    {
                                        "title": str(raw.get("title", "") or "").strip(),
                                        "start_sec": float(raw.get("start_time", 0.0) or 0.0),
                                        "end_sec": float(raw.get("end_time", 0.0) or 0.0),
                                    }
                                )
                        if transcript_allowed:
                            subtitle_url, subtitle_ext = _pick_subtitle_candidate(info)
                            if subtitle_url and subtitle_ext == "vtt":
                                try:
                                    vtt_text = _fetch_text(client, subtitle_url)
                                    captions_path = video_dir / "captions.vtt"
                                    transcript_path = video_dir / "transcript_excerpt.txt"
                                    captions_path.write_text(vtt_text, encoding="utf-8")
                                    transcript_excerpt = _parse_vtt_text(vtt_text)
                                    transcript_path.write_text(transcript_excerpt, encoding="utf-8")
                                    video["captions_file"] = str(captions_path)
                                    video["transcript_file"] = str(transcript_path)
                                    video["transcript_excerpt"] = transcript_excerpt
                                except Exception:
                                    video["transcript_excerpt"] = ""
                        if frame_allowed:
                            try:
                                video_path = Path(_download_lowres_video(str(video["url"]), video_dir / "sample.mp4"))
                                if video_path.exists():
                                    video["reference_frames"] = _extract_reference_frames(
                                        video_path,
                                        video_dir / "frames",
                                        sample_every_sec=sample_every_sec,
                                        max_duration_sec=max_frame_duration_sec,
                                    )
                            except Exception:
                                video["reference_frames"] = []
                _write_json(video_dir / "video.json", video)
                video_jsonl_lines.append(json.dumps(video, ensure_ascii=False))
            memory_seed = _build_channel_memory_seed(channel, videos, seed)
            entry = {
                "seed": seed,
                "channel": channel,
                "average_top_video_views": int(sum(int(v.get("views", 0) or 0) for v in sorted_popular[:8]) / max(1, len(sorted_popular[:8]))) if sorted_popular else 0,
                "recent_upload_titles": [str(v.get("title", "") or "").strip() for v in sorted(videos, key=lambda item: str(item.get("published_at", "") or ""), reverse=True)[:12] if str(v.get("title", "") or "").strip()],
                "top_video_titles": [str(v.get("title", "") or "").strip() for v in sorted_popular[:12] if str(v.get("title", "") or "").strip()],
                "top_videos": sorted_popular[:12],
                "memory_seed": memory_seed,
            }
            entries.append(entry)
            _write_json(channel_dir / "channel.json", entry)
    payload = {
        "generated_at": int(time.time()),
        "format_preset": "documentary",
        "channel_count": len(entries),
        "channels": entries,
        "aggregate_memory_seed": _merge_reference_memory(entries),
    }
    _write_json(REFERENCE_MEMORY_FILE, payload)
    _write_json(OPS_REFERENCE_FILE, payload)
    VIDEO_INDEX_FILE.write_text("\n".join(video_jsonl_lines), encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Catalyst documentary corpus from public YouTube channels.")
    parser.add_argument("--recent-limit", type=int, default=12)
    parser.add_argument("--popular-limit", type=int, default=12)
    parser.add_argument("--transcript-video-limit", type=int, default=6)
    parser.add_argument("--frame-video-limit", type=int, default=2)
    parser.add_argument("--sample-every-sec", type=int, default=15)
    parser.add_argument("--max-frame-duration-sec", type=int, default=240)
    args = parser.parse_args()

    api_key = str(os.getenv("YOUTUBE_API_KEY", "") or os.getenv("GOOGLE_YOUTUBE_API_KEY", "") or "").strip()
    if not api_key:
        raise SystemExit("Set YOUTUBE_API_KEY or GOOGLE_YOUTUBE_API_KEY before running this script.")
    if not SEEDS_FILE.exists():
        raise SystemExit(f"Missing seed file: {SEEDS_FILE}")
    payload = build_corpus(
        api_key,
        recent_limit=max(1, args.recent_limit),
        popular_limit=max(1, args.popular_limit),
        transcript_video_limit=max(0, args.transcript_video_limit),
        frame_video_limit=max(0, args.frame_video_limit),
        sample_every_sec=max(1, args.sample_every_sec),
        max_frame_duration_sec=max(30, args.max_frame_duration_sec),
    )
    print(json.dumps(
        {
            "channel_count": int(payload.get("channel_count", 0) or 0),
            "reference_memory_file": str(REFERENCE_MEMORY_FILE),
            "ops_reference_file": str(OPS_REFERENCE_FILE),
            "video_index_file": str(VIDEO_INDEX_FILE),
        },
        indent=2,
    ))


if __name__ == "__main__":
    main()
