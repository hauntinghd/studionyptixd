import argparse
import json
import sys
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis import catalyst_longform_corpus as base

try:
    import yt_dlp
except Exception:
    yt_dlp = None


def _channel_page_info(channel_url: str, recent_limit: int) -> dict[str, Any]:
    if yt_dlp is None:
        raise RuntimeError("yt_dlp is required for channel supplement scrape")
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": True,
        "playlistend": max(1, int(recent_limit)),
        "ignoreerrors": True,
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(channel_url, download=False) or {}


def _iso_from_upload_date(raw: str) -> str:
    value = str(raw or "").strip()
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}T00:00:00Z"
    return value


def _video_from_detail(detail: dict[str, Any], seed: dict[str, Any], slug: str) -> dict[str, Any]:
    tags = [str(v).strip() for v in list(detail.get("tags") or []) if str(v).strip()][:20]
    video_id = str(detail.get("id", "") or "").strip()
    return {
        "video_id": video_id,
        "url": str(detail.get("webpage_url") or f"https://www.youtube.com/watch?v={video_id}"),
        "title": str(detail.get("title", "") or "").strip(),
        "description": str(detail.get("description", "") or "").strip(),
        "published_at": _iso_from_upload_date(detail.get("upload_date")),
        "channel_title": str(detail.get("channel") or detail.get("uploader") or seed.get("expected_channel_title") or "").strip(),
        "thumbnail_url": str(detail.get("thumbnail", "") or "").strip(),
        "tags": tags,
        "duration_sec": int(float(detail.get("duration", 0) or 0)),
        "views": int(float(detail.get("view_count", 0) or 0)),
        "likes": int(float(detail.get("like_count", 0) or 0)),
        "comments": int(float(detail.get("comment_count", 0) or 0)),
        "source_kind": "recent",
        "seed_slug": slug,
        "channel_slug": slug,
        "channel_style_notes": str(seed.get("style_notes", "") or "").strip(),
        "thumbnail_file": "",
        "transcript_file": "",
        "captions_file": "",
        "reference_frames": [],
        "chapters": [],
        "transcript_excerpt": "",
    }


def _build_channel_from_page(info: dict[str, Any], seed: dict[str, Any], channel_url: str, videos: list[dict[str, Any]]) -> dict[str, Any]:
    channel_title = str(info.get("channel") or info.get("uploader") or seed.get("expected_channel_title") or "").strip()
    channel_id = str(info.get("channel_id") or info.get("id") or "").strip()
    total_views = sum(int(v.get("views", 0) or 0) for v in videos)
    return {
        "channel_id": channel_id,
        "resolved_title": channel_title,
        "title": channel_title,
        "description": str(info.get("description", "") or "").strip(),
        "custom_url": str(info.get("uploader_id", "") or "").strip(),
        "published_at": "",
        "thumbnail_url": "",
        "subscriber_count": int(float(info.get("channel_follower_count", 0) or 0)),
        "video_count": int(float(info.get("playlist_count", 0) or 0)),
        "view_count": int(total_views),
        "uploads_playlist_id": "",
        "channel_url": str(channel_url or "").strip(),
    }


def build_supplement(
    *,
    recent_limit: int,
    transcript_video_limit: int,
    frame_video_limit: int,
    sample_every_sec: int,
    max_frame_duration_sec: int,
) -> dict[str, Any]:
    seeds = json.loads(base.SEEDS_FILE.read_text(encoding="utf-8"))
    existing_payload = json.loads(base.REFERENCE_MEMORY_FILE.read_text(encoding="utf-8")) if base.REFERENCE_MEMORY_FILE.exists() else {
        "generated_at": 0,
        "format_preset": "documentary",
        "channel_count": 0,
        "channels": [],
        "aggregate_memory_seed": {},
    }
    existing_channels = list(existing_payload.get("channels") or [])
    existing_slugs = {
        str((entry.get("seed") or {}).get("slug") or "").strip()
        for entry in existing_channels
        if isinstance(entry, dict)
    }

    new_entries: list[dict[str, Any]] = []
    new_video_lines: list[str] = []

    with httpx.Client(timeout=45, follow_redirects=True) as client:
        for seed in seeds:
            slug = base._slugify(seed.get("slug", "channel"))
            channel_url = str(seed.get("channel_url", "") or "").strip()
            if not channel_url or slug in existing_slugs:
                continue
            print(f"[Catalyst supplement] scraping {slug} via {channel_url}...")
            page_info = _channel_page_info(channel_url, recent_limit)
            raw_entries = list(page_info.get("entries") or [])[: max(1, int(recent_limit))]
            videos: list[dict[str, Any]] = []
            channel_dir = base.RAW_CHANNELS_DIR / slug
            channel_dir.mkdir(parents=True, exist_ok=True)
            for idx, raw_entry in enumerate(raw_entries):
                if not isinstance(raw_entry, dict):
                    continue
                video_url = str(raw_entry.get("url") or "").strip()
                if not video_url:
                    video_id = str(raw_entry.get("id", "") or "").strip()
                    if not video_id:
                        continue
                    video_url = f"https://www.youtube.com/watch?v={video_id}"
                try:
                    detail = base._yt_dlp_extract_info(video_url)
                except Exception:
                    detail = {}
                if not isinstance(detail, dict) or not str(detail.get("id", "") or "").strip():
                    continue
                video = _video_from_detail(detail, seed, slug)
                video_dir = channel_dir / "videos" / str(video["video_id"])
                video_dir.mkdir(parents=True, exist_ok=True)

                if video.get("thumbnail_url"):
                    try:
                        video["thumbnail_file"] = base._download_thumbnail(client, str(video["thumbnail_url"]), video_dir / "thumbnail.jpg")
                    except Exception:
                        video["thumbnail_file"] = ""

                for raw in list(detail.get("chapters") or [])[:16]:
                    if isinstance(raw, dict):
                        video["chapters"].append(
                            {
                                "title": str(raw.get("title", "") or "").strip(),
                                "start_sec": float(raw.get("start_time", 0.0) or 0.0),
                                "end_sec": float(raw.get("end_time", 0.0) or 0.0),
                            }
                        )

                if idx < transcript_video_limit:
                    subtitle_url, subtitle_ext = base._pick_subtitle_candidate(detail)
                    if subtitle_url and subtitle_ext == "vtt":
                        try:
                            vtt_text = base._fetch_text(client, subtitle_url)
                            captions_path = video_dir / "captions.vtt"
                            transcript_path = video_dir / "transcript_excerpt.txt"
                            captions_path.write_text(vtt_text, encoding="utf-8")
                            transcript_excerpt = base._parse_vtt_text(vtt_text)
                            transcript_path.write_text(transcript_excerpt, encoding="utf-8")
                            video["captions_file"] = str(captions_path)
                            video["transcript_file"] = str(transcript_path)
                            video["transcript_excerpt"] = transcript_excerpt
                        except Exception:
                            video["transcript_excerpt"] = ""

                if idx < frame_video_limit:
                    try:
                        video_path = Path(base._download_lowres_video(str(video["url"]), video_dir / "sample.mp4"))
                        if video_path.exists():
                            video["reference_frames"] = base._extract_reference_frames(
                                video_path,
                                video_dir / "frames",
                                sample_every_sec=sample_every_sec,
                                max_duration_sec=max_frame_duration_sec,
                            )
                    except Exception:
                        video["reference_frames"] = []

                base._write_json(video_dir / "video.json", video)
                new_video_lines.append(json.dumps(video, ensure_ascii=False))
                videos.append(video)

            if not videos:
                continue

            channel = _build_channel_from_page(page_info, seed, channel_url, videos)
            memory_seed = base._build_channel_memory_seed(channel, videos, seed)
            sorted_popular = sorted(videos, key=lambda item: int(item.get("views", 0) or 0), reverse=True)
            entry = {
                "seed": seed,
                "channel": channel,
                "average_top_video_views": int(sum(int(v.get("views", 0) or 0) for v in sorted_popular[:8]) / max(1, len(sorted_popular[:8]))) if sorted_popular else 0,
                "recent_upload_titles": [str(v.get("title", "") or "").strip() for v in sorted(videos, key=lambda item: str(item.get("published_at", "") or ""), reverse=True)[:12] if str(v.get("title", "") or "").strip()],
                "top_video_titles": [str(v.get("title", "") or "").strip() for v in sorted_popular[:12] if str(v.get("title", "") or "").strip()],
                "top_videos": sorted_popular[:12],
                "memory_seed": memory_seed,
            }
            new_entries.append(entry)
            base._write_json(channel_dir / "channel.json", entry)

    merged_channels = existing_channels + new_entries
    payload = {
        "generated_at": int(__import__("time").time()),
        "format_preset": "documentary",
        "channel_count": len(merged_channels),
        "channels": merged_channels,
        "aggregate_memory_seed": base._merge_reference_memory(merged_channels),
    }
    base._write_json(base.REFERENCE_MEMORY_FILE, payload)
    base._write_json(base.OPS_REFERENCE_FILE, payload)

    existing_lines = []
    if base.VIDEO_INDEX_FILE.exists():
        existing_lines = [line for line in base.VIDEO_INDEX_FILE.read_text(encoding="utf-8").splitlines() if line.strip()]
    base.VIDEO_INDEX_FILE.write_text("\n".join(existing_lines + new_video_lines), encoding="utf-8")
    return {
        "added_channels": len(new_entries),
        "added_videos": len(new_video_lines),
        "reference_memory_file": str(base.REFERENCE_MEMORY_FILE),
        "ops_reference_file": str(base.OPS_REFERENCE_FILE),
        "video_index_file": str(base.VIDEO_INDEX_FILE),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Supplement Catalyst documentary corpus via yt-dlp channel scraping.")
    parser.add_argument("--recent-limit", type=int, default=6)
    parser.add_argument("--transcript-video-limit", type=int, default=2)
    parser.add_argument("--frame-video-limit", type=int, default=1)
    parser.add_argument("--sample-every-sec", type=int, default=20)
    parser.add_argument("--max-frame-duration-sec", type=int, default=180)
    args = parser.parse_args()
    result = build_supplement(
        recent_limit=max(1, args.recent_limit),
        transcript_video_limit=max(0, args.transcript_video_limit),
        frame_video_limit=max(0, args.frame_video_limit),
        sample_every_sec=max(1, args.sample_every_sec),
        max_frame_duration_sec=max(30, args.max_frame_duration_sec),
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
