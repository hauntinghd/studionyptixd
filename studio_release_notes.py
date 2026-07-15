"""Studio release notes: in-app feed + Discord Studio Alerts on new deploys."""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import studio_alerts

log = logging.getLogger(__name__)

_DATA_DIR = Path(os.getenv("APP_DATA_DIR", "/var/data"))
_RELEASES_PATH = _DATA_DIR / "studio_release_notes.json"
_ANNOUNCED_PATH = _DATA_DIR / "studio_release_announced.json"

# Bump id when shipping; startup announces any id not yet in announced store.
CATALOG: list[dict[str, Any]] = [
    {
        "id": "release-2026-07-13-owner-credits-longform-compose",
        "kind": "success",
        "title": "Owner credits, long-form status, HR compose fix",
        "body": (
            "Owner accounts show infinite credits again in the top bar. Long-form job polls read Fly workspaces "
            "so completed renders stop stuck at 0% Connecting production. History Rewind sleep-doc compose now "
            "uses full narration duration (no more 359 scenes in 11s). Credits pill matches Discord styling."
        ),
        "version": "2026.07.13",
    },
    {
        "id": "release-2026-07-13-studio-boot-routing-fix",
        "kind": "success",
        "title": "Studio boot + agent routing fix",
        "body": (
            "Hard refresh no longer hammers RunPod with /api/config and /api/me (429 storms). "
            "Boot reads hit Fly directly, Studio Agent HTTP routes through api-studio with CORS on proxy errors, "
            "health probes are throttled, and release-note sync is deduped."
        ),
        "version": "2026.07.13",
    },
    {
        "id": "release-2026-07-13-thumbnail-review-downloads",
        "kind": "success",
        "title": "Thumbnail review + in-app downloads",
        "body": (
            "Long-form thumbnail-only jobs now reconcile into a pinned review strip with three candidates "
            "and Download buttons served through Studio (not raw FAL URLs). Stale chapter-json failures "
            "no longer block the dock after refresh."
        ),
        "version": "2026.07.13",
    },
    {
        "id": "release-2026-07-13-longform-chapter-json-fix",
        "kind": "success",
        "title": "Long-form chapter JSON hardening",
        "body": (
            "Sleep-doc and history long-form chapters no longer truncate narration into broken JSON "
            "(LFRenderError chapter 0). Narration and scene prompts are generated in separate passes."
        ),
        "version": "2026.07.13",
    },
    {
        "id": "release-2026-07-13-admin-lane-gate",
        "kind": "info",
        "title": "Long-form + ClipLab admin gate",
        "body": (
            "Long-form and ClipLab are owner/admin-only at launch while short-form stays live for paying users. "
            "Non-admins no longer see those modes in Agent or the niche gallery."
        ),
        "version": "2026.07.13",
    },
    {
        "id": "release-2026-07-13-ty-beta-promo",
        "kind": "billing",
        "title": "TY promo — one month free on Studio Pro",
        "body": (
            "Stripe checkout for studio_pro_1k ($25/mo) now accepts promo code TY for 100% off the first month. "
            "Enter TY at checkout after clicking Subscribe."
        ),
        "version": "2026.07.13",
    },
    {
        "id": "release-2026-07-12-sleep-doc-concept-plans",
        "kind": "success",
        "title": "Channel-aware sleep-doc concept plans",
        "body": (
            "History Rewind and sleep-documentary long-form plans now use channel-aware beats, hooks, and durations "
            "(hours formatting, aligned beat seconds) instead of generic short-form pacing."
        ),
        "version": "2026.07.12",
    },
    {
        "id": "release-2026-07-08-live-demand-all-niches",
        "kind": "success",
        "title": "Live Demand for every niche + ads",
        "body": (
            "Studio Agent now runs a niche-agnostic Live Demand pass before demand-grounded shorts and product ads: "
            "fresh public YouTube search (1–2 day windows when you ask for right-now / last 24h), hydrated evidence, "
            "and a production brief injected into scripts. Works for day trading, psychology, fitness, SaaS ads, and any niche — "
            "not training-memory trends."
        ),
        "version": "2026.07.08",
    },
    {
        "id": "release-2026-07-08-session-grounded-cost-quotes",
        "kind": "success",
        "title": "Render costs use your session models",
        "body": (
            "Cost breakdowns now call estimate_shortform_render_cost with your active image_model_id and video_model "
            "(e.g. Grok Imagine + Grok Imagine Video) instead of hallucinating LTX/Seedream legacy pipeline pricing."
        ),
        "version": "2026.07.08",
    },

    {
        "id": "release-2026-07-08-channel-winner-predictions",
        "kind": "success",
        "title": "Predicted moves follow your channel",
        "body": (
            "When OAuth channel analytics are connected, predicted moves now rank your retention winners first "
            "(APV/AVD/video rows) instead of generic public psychology outliers. Public demand stays in the evidence "
            "section unless a row matches your actual title patterns."
        ),
        "version": "2026.07.08",
    },
    {
        "id": "release-2026-07-08-public-search-niche-gate",
        "kind": "success",
        "title": "Public demand stays on-niche",
        "body": (
            "Agent public YouTube search no longer runs deictic queries like \"videos in this YouTube documentary\" "
            "that returned global MrBeast-scale outliers. Queries coerce to channel-aware niches, irrelevant precedents "
            "are filtered from evidence and predicted moves, and Shorts channels skip the documentary widening pass."
        ),
        "version": "2026.07.08",
    },
    {
        "id": "release-2026-07-08-korpi-skeleton-parity",
        "kind": "success",
        "title": "KORPI-level skeleton still lock",
        "body": (
            "Create → Scenes now requires your skeleton reference upload (same flow as KORPI custom niche). "
            "Every still uses Seedream 4.5 edit from that reference — editable scene prompts, per-beat regenerate, "
            "and Agent shortform defaults to seedream_edit instead of Grok T2I drift."
        ),
        "version": "2026.07.08",
    },
    {
        "id": "release-2026-07-07-public-youtube-research-autorun",
        "kind": "success",
        "title": "Public YouTube research auto-run",
        "body": (
            "Requests to pull/search public YouTube niche performance now auto-run "
            "get_public_search_trends and search_youtube_public instead of the model claiming "
            "the tool is missing. Denial responses are replaced with grounded search evidence."
        ),
        "version": "2026.07.07",
    },
    {
        "id": "release-2026-07-07-reference-failed-card-purge",
        "kind": "success",
        "title": "Reference failed card purge",
        "body": (
            "Stale Expecting value JSON poll failures no longer display as Reference analysis failed. "
            "Ghost errors are detected regardless of kind mislabeling, and failed cards are hidden once "
            "reference analysis text or a complete deliverable exists in the chat."
        ),
        "version": "2026.07.07",
    },
    {
        "id": "release-2026-07-07-ghost-deliverable-strip-fix",
        "kind": "success",
        "title": "Ghost production card purge",
        "body": (
            "Stale shortform JSON failures no longer stick to assistant messages after reject or status checks. "
            "Ghost deliverables are stripped in place (chat text preserved), shortform tracks are cleared on reject, "
            "and local cache no longer resurrects the red Production failed card."
        ),
        "version": "2026.07.07",
    },
    {
        "id": "release-2026-07-07-stats-status-intent-fix",
        "kind": "success",
        "title": "Stats/status no longer spawns video production",
        "body": (
            "Typing stats? or status? now polls reference analysis or channel data instead of "
            "resurrecting a stale start_shortform_generate approval. Option titles from analysis "
            "are no longer auto-promoted to production unless you explicitly pick an option."
        ),
        "version": "2026.07.07",
    },
    {
        "id": "release-2026-07-07-reference-ghost-shortform-fix",
        "kind": "success",
        "title": "Reference ghost shortform fix",
        "body": (
            "Reference analysis no longer shows a parallel Production failed card from stale shortform "
            "polls. Orphan shortform tracks are pruned on resume, ghost JSON failures are stripped from "
            "chat history, and competitor jobs clear stale production tracks automatically."
        ),
        "version": "2026.07.07",
    },
    {
        "id": "release-2026-07-07-reference-poll-stale-job-fix",
        "kind": "success",
        "title": "Reference poll stale-job fix",
        "body": (
            "Studio no longer surfaces a ghost shortform JSON parse failure while reference analysis "
            "is running on Fly. Poll routing always prefers competitor workspaces, prunes stale shortform "
            "tracks, and retries misrouted polls automatically."
        ),
        "version": "2026.07.07",
    },
    {
        "id": "release-2026-07-07-reference-poll-kind-fix",
        "kind": "success",
        "title": "Reference analysis poll routing fix",
        "body": (
            "Uploaded reference jobs no longer misroute to shortform polling, which caused "
            "Expecting value: line 1 column 1 JSON failures and Production failed cards. "
            "The backend now auto-detects competitor workspaces; empty result files return a clear retry message."
        ),
        "version": "2026.07.07",
    },
    {
        "id": "release-2026-07-07-dictation-stt-json-fix",
        "kind": "success",
        "title": "Dictation unlock + reference JSON hardening",
        "body": (
            "Live xAI dictation no longer leaves the prompt box stuck on Transcribing your voice. "
            "Reference analysis now uses safe JSON parsing and xAI STT for uploaded audio, and competitor "
            "job polling no longer misroutes to shortform workspaces. Studio Agent keeps the full Fly toolset enabled by default."
        ),
        "version": "2026.07.07",
    },
    {
        "id": "release-2026-07-07-studio-agent-research-v4",
        "kind": "success",
        "title": "Studio Agent transcript retry + live voice",
        "body": (
            "Reference analysis transcript timestamps no longer crash on FAL list timestamps. "
            "retry_reference_analysis re-runs failed transcript/vision/story stages without re-uploading. "
            "Mic dictation now uses xAI Grok STT (live streaming + batch fallback). "
            "Studio auto-syncs release notes and prefetches UI updates on your next message — no manual hard refresh."
        ),
        "version": "2026.07.07",
    },
    {
        "id": "release-2026-07-07-studio-agent-research-v3",
        "kind": "success",
        "title": "Studio Agent research upgrade",
        "body": (
            "Watch-this-video turns now run deep reference analysis (vision, transcript, hook/story) via FAL fallbacks, "
            "not pacing-only ffmpeg. Public YouTube search waits until the upload returns a real topic — no more searches "
            "on raw chat text. Partial failures show exact stage errors, skip fake combined reads, and omit channel analytics "
            "unless you asked for channel data."
        ),
        "version": "2026.07.07",
    },
    {
        "id": "release-2026-07-07-agent-intent-routing",
        "kind": "info",
        "title": "Studio Agent intent routing fixes",
        "body": (
            "“Watch this video” + “check public YouTube data” now classifies as reference analysis → public demand in order. "
            "Anti-hallucination no longer demands channel analytics when you only asked for upload + public market research."
        ),
        "version": "2026.07.07",
    },
    {
        "id": "release-2026-07-06-bulk-ship-xai-defaults",
        "kind": "success",
        "title": "Bulk ship + xAI skeleton defaults",
        "body": (
            "“Approve all scenes, animate them, and finish the video” now routes to bulk ship instead of expand-short. "
            "Skeleton shorts default to Grok Imagine image editing and Grok Imagine Video for i2v."
        ),
        "version": "2026.07.06",
    },
    {
        "id": "release-2026-07-06-catalyst-skeleton-refs",
        "kind": "success",
        "title": "Catalyst skeleton reference learning",
        "body": (
            "Catalyst now learns from your approved MrSkeleWelly YouTube shorts: single full-frame skeleton, "
            "wardrobe via image edit, no diptychs. Regenerate on split-screen scenes master-rebuilds from the "
            "canonical reference instead of editing the broken still."
        ),
        "version": "2026.07.06",
    },
    {
        "id": "release-2026-07-06-catalyst-regenerate",
        "kind": "success",
        "title": "Catalyst Regenerate on scene cards",
        "body": (
            "Regenerate now runs Catalyst still audit first: keeps exact channel style, fixes extra hands "
            "and split-screen diptychs, and records visual watchouts for self-learning. No GPU required."
        ),
        "version": "2026.07.06",
    },
    {
        "id": "release-2026-07-06-hand-guard",
        "kind": "success",
        "title": "Extra-hand artifact guard",
        "body": (
            "Skeleton stills now ban split-screen/diptych layouts and enforce exactly two hands. "
            "Risky scene prompts auto-sanitize and retry with a stronger hand lock."
        ),
        "version": "2026.07.06",
    },
    {
        "id": "release-2026-07-06-live-scene-refresh",
        "kind": "info",
        "title": "Scene stills update live",
        "body": (
            "Production cards refresh as scenes regenerate — no hard refresh needed. "
            "Still URLs cache-bust when files change."
        ),
        "version": "2026.07.06",
    },
    {
        "id": "release-2026-07-06-bulk-scene-ship",
        "kind": "success",
        "title": "Approve all → animate all → finish",
        "body": (
            "Say “I approve all of the scenes, animate them, and finish the video” and Studio runs "
            "the full chain on every scene automatically."
        ),
        "version": "2026.07.06",
    },
    {
        "id": "release-2026-07-06-upload-package-v2",
        "kind": "info",
        "title": "Upload packages rebuilt for Shorts",
        "body": (
            "Finalize now writes topic-specific titles, hooks, tags, and timestamps — plus honors CC Off in the package notes."
        ),
        "version": "2026.07.06",
    },
    {
        "id": "release-2026-07-06-agent-preview-v2",
        "kind": "success",
        "title": "Studio Agent previews restored",
        "body": (
            "Scene stills always show in chat again. Animated scene clips keep updating after approval. "
            "Final MP4 preview appears when export completes. Long chats stay fast."
        ),
        "version": "2026.07.06",
    },
    {
        "id": "release-2026-07-06-cc-off",
        "kind": "info",
        "title": "CC Off is honored on export",
        "body": "When captions are Off in Studio Agent, finalize no longer burns word captions into your short.",
        "version": "2026.07.06",
    },
    {
        "id": "release-2026-07-06-chat-scene-fix",
        "kind": "info",
        "title": "Fix scenes in chat — no Edit button",
        "body": "Type what is wrong (e.g. add eyeballs) and Studio Agent edits the scene still without the Edit preset.",
        "version": "2026.07.06",
    },
    {
        "id": "release-2026-07-06-fal-voice",
        "kind": "success",
        "title": "Short-form voice now uses fal.ai",
        "body": "Finalize and re-edit narration runs through fal MiniMax TTS so exports no longer fail on ElevenLabs billing.",
        "version": "2026.07.06",
    },
    {
        "id": "release-2026-07-06-fresh-production-reset",
        "kind": "info",
        "title": "New shorts no longer resurrect old scenes",
        "body": "When you ask for a new short, Studio Agent clears stale scene-review jobs instead of re-opening the last artifacted render.",
        "version": "2026.07.06",
    },
    {
        "id": "release-2026-07-06-chat-context-fork",
        "kind": "info",
        "title": "New chat with prior context",
        "body": "Use With context in Studio Agent or say ingest context from my previous chat to pick up channel plans without dragging old renders back.",
        "version": "2026.07.06",
    },
    {
        "id": "release-2026-07-06-expand-proof-short",
        "kind": "info",
        "title": "Finish shorts from an approved scene",
        "body": "Reply to your fixed scene 1 and say make the rest or finish the video — Studio keeps scene 1 and generates the remaining scenes on the same job.",
        "version": "2026.07.06",
    },
    {
        "id": "release-2026-07-06-natural-expand-routing",
        "kind": "info",
        "title": "Natural language scene continuation",
        "body": "Say using this as scene 1, make the rest, or only make it 30 seconds — Studio expands the same job instead of preparing a duplicate approval card.",
        "version": "2026.07.06",
    },
    {
        "id": "release-2026-07-06-deliverable-layout",
        "kind": "info",
        "title": "Tighter video preview in chat",
        "body": "Finished short MP4 previews fit at normal zoom instead of filling the whole screen.",
        "version": "2026.07.06",
    },
]


def _load_announced() -> set[str]:
    try:
        if _ANNOUNCED_PATH.is_file():
            data = json.loads(_ANNOUNCED_PATH.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return {str(x) for x in data}
    except Exception:
        pass
    return set()


def _save_announced(ids: set[str]) -> None:
    try:
        _ANNOUNCED_PATH.parent.mkdir(parents=True, exist_ok=True)
        _ANNOUNCED_PATH.write_text(json.dumps(sorted(ids), indent=2), encoding="utf-8")
    except Exception as exc:
        log.debug("release announced save failed: %s", exc)


def _load_persisted() -> list[dict[str, Any]]:
    try:
        if _RELEASES_PATH.is_file():
            data = json.loads(_RELEASES_PATH.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return [row for row in data if isinstance(row, dict)]
    except Exception:
        pass
    return []


def _save_persisted(rows: list[dict[str, Any]]) -> None:
    try:
        _RELEASES_PATH.parent.mkdir(parents=True, exist_ok=True)
        _RELEASES_PATH.write_text(json.dumps(rows[-80:], indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        log.debug("release notes save failed: %s", exc)


def pending_catalog_release_ids() -> list[str]:
    """Catalog release ids not yet announced on this server."""
    announced = _load_announced()
    pending: list[str] = []
    for row in CATALOG:
        rid = str(row.get("id") or "").strip()
        if rid and rid not in announced:
            pending.append(rid)
    return pending


def list_release_notes(*, limit: int = 30) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    catalog_base = time.time()
    for i, row in enumerate(CATALOG):
        rid = str(row.get("id") or "").strip()
        if not rid:
            continue
        created_at = float(row.get("created_at") or 0)
        if not created_at:
            created_at = catalog_base - (i * 10)
        by_id[rid] = {
            "id": rid,
            "kind": str(row.get("kind") or "info"),
            "title": str(row.get("title") or "Studio update"),
            "body": str(row.get("body") or ""),
            "version": str(row.get("version") or ""),
            "created_at": created_at,
        }
    for row in _load_persisted():
        rid = str(row.get("id") or "").strip()
        if not rid:
            continue
        created_at = float(row.get("created_at") or 0) or None
        existing = by_id.get(rid)
        if existing and (existing.get("created_at") or 0) >= (created_at or 0):
            continue
        by_id[rid] = {
            "id": rid,
            "kind": str(row.get("kind") or "info"),
            "title": str(row.get("title") or "Studio update"),
            "body": str(row.get("body") or ""),
            "version": str(row.get("version") or ""),
            "created_at": created_at,
        }
    rows = list(by_id.values())
    rows.sort(key=lambda r: (r.get("created_at") or 0, r.get("id") or ""), reverse=True)
    return rows[: max(1, min(int(limit or 30), 80))]


def publish_release_note(
    *,
    release_id: str,
    title: str,
    body: str,
    kind: str = "info",
    version: str = "",
    announce_discord: bool = True,
) -> dict[str, Any]:
    rid = str(release_id or "").strip()
    if not rid:
        raise ValueError("release_id required")
    row = {
        "id": rid,
        "kind": str(kind or "info"),
        "title": str(title or "Studio update")[:200],
        "body": str(body or "")[:2000],
        "version": str(version or "")[:40],
        "created_at": time.time(),
    }
    persisted = _load_persisted()
    persisted = [r for r in persisted if str(r.get("id")) != rid]
    persisted.append(row)
    _save_persisted(persisted)
    if announce_discord:
        studio_alerts.send_release(row["title"], row["body"], version=row["version"], release_id=rid)
    announced = _load_announced()
    announced.add(rid)
    _save_announced(announced)
    return row


def announce_pending_catalog_releases() -> int:
    """On startup: Discord + persisted feed for catalog entries not yet announced."""
    announced = _load_announced()
    count = 0
    for row in CATALOG:
        rid = str(row.get("id") or "").strip()
        if not rid or rid in announced:
            continue
        try:
            publish_release_note(
                release_id=rid,
                title=str(row.get("title") or "Studio update"),
                body=str(row.get("body") or ""),
                kind=str(row.get("kind") or "info"),
                version=str(row.get("version") or ""),
                announce_discord=True,
            )
            count += 1
        except Exception as exc:
            log.warning("release announce failed for %s: %s", rid, exc)
    return count