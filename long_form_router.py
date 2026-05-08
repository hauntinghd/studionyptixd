"""
FastAPI router for the Long-Form generation API (clean v6 surface).

Wires into backend.py the same way Skeleton AI does:
    from long_form_router import build_long_form_router
    app.include_router(build_long_form_router(
        require_auth=require_auth,
        catalyst_hub_snapshot_for_user=catalyst_hub_snapshot_for_user,
    ))

Routes:
  GET  /api/long-form/channels                  6 channel registry
  GET  /api/long-form/channel/{key}             single channel canonical
  POST /api/long-form/catalyst-insights          {channel_key} → shaped Catalyst data
  POST /api/long-form/outline                   Grok outline pass (chapter list)
  POST /api/long-form/outline/expand-chapter    Lazy per-chapter beat expansion
  POST /api/long-form/render                    [stub for now] full pipeline kickoff
  GET  /api/long-form/jobs/{job_id}             [stub for now] job poll

The legacy /api/longform/* + /api/creative/* routes (28 endpoints) remain
mounted alongside this — they back the existing v5 sessions that shipped
Wirecard / Mongol 9H / Ottoman 9H. The new /api/long-form/* (hyphenated)
becomes the canonical surface once parity is verified, then legacy retires.
"""
from __future__ import annotations
import os
from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from long_form.prompts.channels import (
    CHANNELS,
    list_channels,
    get_channel,
    channel_outline_prompt_extras,
)
from long_form.scripting import generate_outline, expand_chapter
from long_form.catalyst_bridge import (
    CHANNEL_KEY_TO_ID,
    shape_catalyst_insights,
    insights_to_grok_context,
    fetch_channel_snapshot,
)

from skeleton_ai.scripting_grok import GrokClient, GrokAuthError

# Reference videos thread into the Grok system prompt so generated outlines
# mimic patterns from videos the user picked as inspiration. Best-effort.
try:
    from catalyst_references import list_user_references, references_as_grok_context
    _REFS_AVAILABLE = True
except Exception:
    _REFS_AVAILABLE = False
    def list_user_references(*_a, **_kw):  # type: ignore
        return []
    def references_as_grok_context(*_a, **_kw):  # type: ignore
        return ""


def _references_for_channel(user: dict, channel_key: str) -> str:
    """Pull user's saved refs whose channel_key matches the requested
    channel OR is universal (''). Returns a Grok-ready text block."""
    if not _REFS_AVAILABLE:
        return ""
    uid = str((user or {}).get("id", "") or (user or {}).get("user_id", "") or "")
    if not uid:
        return ""
    matched: list[dict] = []
    seen: set[str] = set()
    for ck in (channel_key, ""):
        try:
            rows = list_user_references(uid, channel_key=ck)
        except Exception:
            continue
        for r in rows or []:
            rid = str((r or {}).get("id", ""))
            if rid and rid not in seen:
                seen.add(rid)
                matched.append(r)
    return references_as_grok_context(matched, max_refs=8)


OUTPUT_ROOT = Path(os.getenv("LONG_FORM_OUTPUT_ROOT", "long_form/output"))


class CatalystInsightsRequest(BaseModel):
    channel_key: str


class OutlineRequest(BaseModel):
    channel_key: str
    topic: str
    target_minutes: int | None = None       # default = channel's preferred length
    use_catalyst_context: bool = True


class ExpandChapterRequest(BaseModel):
    channel_key: str
    outline_title: str
    chapter: dict


class RenderRequest(BaseModel):
    channel_key: str
    outline: dict
    image_model: str | None = None
    voice_id: str | None = None


def build_long_form_router(
    *,
    require_auth: Callable[..., dict] | None = None,
    catalyst_hub_snapshot_for_user: Callable | None = None,
    is_admin_check: Callable[[dict], bool] | None = None,
) -> APIRouter:
    """
    All long-form endpoints are ADMIN-ONLY (Casey 2026-05-05). Public users
    only see /api/skeleton-ai/* via the Create tab. Long-form burns Grok
    tokens + fal money on episode-scale renders, so we gate at the API
    level rather than relying on frontend sidebar hiding alone.
    """
    router = APIRouter(prefix="/api/long-form", tags=["long-form"])
    auth_dep = Depends(require_auth) if require_auth else Depends(lambda: {"user_id": "anon"})

    def _gate_admin(user: dict) -> None:
        if not is_admin_check:
            return  # No admin checker passed (test mode) — open.
        try:
            if is_admin_check(user):
                return
        except Exception:
            pass
        raise HTTPException(403, "long-form is admin-only")

    @router.get("/channels")
    async def list_channels_route(user: dict = auth_dep, format: str | None = None):
        _gate_admin(user)
        # format=long_form filters to the 6 generative channels;
        # format=shorts filters to ZeroTier / CrypticScience / Lexi Manhwa;
        # omit to get all 9.
        return {"channels": list_channels(format_filter=format)}

    @router.get("/connected-channels")
    async def connected_channels(user: dict = auth_dep):
        """
        Returns every OAuth-connected channel from the local store, augmented
        with the long-form registry data (where matched). Lets the frontend
        show 'Catalyst sees N channels' and which of them have harvested data
        vs which are pending the next auto-tick.
        """
        _gate_admin(user)
        try:
            from youtube_connections_store import hydrate
            hyd = hydrate() or {}
        except Exception as e:
            raise HTTPException(503, f"connection store unavailable: {e}")

        # Build a channel_id → registry-key map for fast lookup.
        id_to_key = {v["channel_id"]: k for k, v in CHANNELS.items() if v.get("channel_id")}

        out = []
        for u in hyd.values():
            if not isinstance(u, dict):
                continue
            for ch_id, rec in (u.get("channels") or {}).items():
                if not isinstance(rec, dict):
                    continue
                snap = rec.get("analytics_snapshot") or {}
                key = id_to_key.get(ch_id, "")
                ch_meta = CHANNELS.get(key, {}) if key else {}
                out.append({
                    "channel_id": ch_id,
                    "channel_title": rec.get("title", "") or rec.get("channel_handle", ""),
                    "channel_handle": rec.get("channel_handle", ""),
                    "subscriber_count": int(rec.get("subscriber_count", 0) or 0),
                    "video_count": int(rec.get("video_count", 0) or 0),
                    "view_count": int(rec.get("view_count", 0) or 0),
                    "last_synced_at": rec.get("last_synced_at"),
                    "harvest_present": bool(snap),
                    "registry_key": key,                # may be empty if channel not in long-form registry
                    "registry_label": ch_meta.get("label", ""),
                    "registry_format": ch_meta.get("format", ""),  # 'long_form' or 'shorts'
                })
        # Sort: registered long-form first, then registered shorts, then unregistered.
        def sort_key(c):
            fmt = c["registry_format"]
            return (0 if fmt == "long_form" else (1 if fmt == "shorts" else 2),
                    -c["subscriber_count"])
        out.sort(key=sort_key)
        return {"channels": out, "total": len(out)}

    @router.get("/channel/{key}")
    async def channel_route(key: str, user: dict = auth_dep):
        _gate_admin(user)
        try:
            return {"channel": get_channel(key)}
        except ValueError as e:
            raise HTTPException(404, str(e))

    @router.post("/catalyst-insights")
    async def catalyst_insights(body: CatalystInsightsRequest, user: dict = auth_dep):
        """
        Returns shaped Catalyst insights for the picked channel.

        Reads directly from the OAuth connection-store record (where Catalyst
        Hub auto-tick deposits harvested analytics). If the channel was just
        OAuth'd and no harvest has run yet, returns 200 with empty arrays
        and `harvest_present=False` so the frontend can show "pending refresh".
        """
        _gate_admin(user)
        try:
            channel = get_channel(body.channel_key)
        except ValueError as e:
            raise HTTPException(404, str(e))

        channel_id = CHANNEL_KEY_TO_ID.get(body.channel_key, "")
        record = fetch_channel_snapshot(channel_id) if channel_id else None
        insights = shape_catalyst_insights(record)

        return {
            "channel_key": body.channel_key,
            "channel_label": channel["label"],
            "channel_id": channel_id,
            "insights": insights,
            "catalyst_present": bool(record and record.get("analytics_snapshot")),
        }

    @router.post("/outline")
    async def outline_route(body: OutlineRequest, user: dict = auth_dep):
        _gate_admin(user)
        if not body.topic or not body.topic.strip():
            raise HTTPException(400, "topic is required")
        try:
            channel = get_channel(body.channel_key)
        except ValueError as e:
            raise HTTPException(404, str(e))

        try:
            grok = GrokClient()
        except GrokAuthError as e:
            raise HTTPException(503, f"config: {e}")

        # Pull Catalyst context from the OAuth connection store (best effort).
        catalyst_text = ""
        if body.use_catalyst_context:
            ch_id = CHANNEL_KEY_TO_ID.get(body.channel_key, "")
            if ch_id:
                try:
                    record = fetch_channel_snapshot(ch_id)
                    if record:
                        catalyst_text = insights_to_grok_context(shape_catalyst_insights(record))
                except Exception:
                    catalyst_text = ""

        # Pull reference-video context (user's saved viral inspirations
        # tagged to this channel OR the universal '' tag).
        refs_text = _references_for_channel(user, body.channel_key)

        # Combine both signal blocks under one header so Grok sees them as
        # a single 'channel performance + viral references' bias.
        combined_context_parts: list[str] = []
        if catalyst_text:
            combined_context_parts.append(catalyst_text)
        if refs_text:
            combined_context_parts.append(refs_text)
        combined_context = "\n\n".join(combined_context_parts)

        target_minutes = body.target_minutes or channel["default_minutes"]

        # PR #119: enforce per-channel decoded winner title pattern + 'avoid'
        # phrases + description tail. Empty string for channels without a
        # template (Lacuna / Hidden Cortex / PB Live / Lo-Fi Radio) so the
        # outline pass falls back to the existing free-form title generation.
        title_template_block = channel_outline_prompt_extras(body.channel_key)

        outline = generate_outline(
            grok,
            channel["system_prompt"],
            topic=body.topic,
            target_minutes=int(target_minutes),
            catalyst_context=combined_context,
            title_template_block=title_template_block,
        )

        # Always append the channel's description_tail to outline.description
        # so the YouTube upload metadata carries the proven CTR signal
        # (HR's 'Human Voiced, No Ads', EM's 'Loophole Files investigation'
        # subscribe line) — even when Grok's free-form description omits it.
        desc_tail = (channel.get("description_tail") or "").strip()
        if desc_tail and isinstance(outline, dict):
            existing = (outline.get("description") or "").rstrip()
            if existing and desc_tail not in existing:
                outline["description"] = existing + "\n\n" + desc_tail.lstrip()
            elif not existing:
                outline["description"] = desc_tail.lstrip()

        return {
            "channel_key": body.channel_key,
            "topic": body.topic,
            "target_minutes": int(target_minutes),
            "catalyst_context_used": bool(catalyst_text),
            "references_used": bool(refs_text),
            "title_template_enforced": bool(title_template_block),
            "outline": outline,
        }

    @router.post("/outline/expand-chapter")
    async def expand_chapter_route(body: ExpandChapterRequest, user: dict = auth_dep):
        _gate_admin(user)
        try:
            channel = get_channel(body.channel_key)
        except ValueError as e:
            raise HTTPException(404, str(e))
        try:
            grok = GrokClient()
        except GrokAuthError as e:
            raise HTTPException(503, f"config: {e}")
        beats = expand_chapter(
            grok,
            channel["system_prompt"],
            outline_title=body.outline_title,
            chapter=body.chapter,
            fps=int(channel["fps"]),
        )
        return {
            "channel_key": body.channel_key,
            "chapter_index": int((body.chapter or {}).get("index", 0)),
            "beats": beats,
        }

    @router.post("/render")
    async def render_route(body: RenderRequest, user: dict = auth_dep):
        _gate_admin(user)
        """
        Full long-form render. PHASE 2 — wires into the v5 pipeline.

        For now we 501 with a clear message so the frontend can show
        "render coming soon" without crashing. Once we burn fal money
        on a verification render, this delegates to long_form.pipeline.run().
        """
        try:
            get_channel(body.channel_key)
        except ValueError as e:
            raise HTTPException(404, str(e))
        raise HTTPException(
            501,
            "render pipeline wires up in phase 2 (after fal balance refills). "
            "For now the legacy /api/longform/session endpoints still work and "
            "are what shipped Wirecard / Mongol 9H / Ottoman 9H."
        )

    @router.get("/jobs/{job_id}")
    async def job_route(job_id: str, user: dict = auth_dep):
        _gate_admin(user)
        if not job_id.replace("_", "").isalnum() or len(job_id) > 32:
            raise HTTPException(400, "bad_job_id")
        # Phase 2 — read result.json from long_form/output/{job_id}/.
        raise HTTPException(404, "no jobs yet — render endpoint is phase 2")

    return router
