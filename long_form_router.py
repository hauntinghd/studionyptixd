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

from long_form.prompts.channels import CHANNELS, list_channels, get_channel
from long_form.scripting import generate_outline, expand_chapter
from long_form.catalyst_bridge import (
    CHANNEL_KEY_TO_CATALYST_HANDLE,
    shape_catalyst_insights,
    insights_to_grok_context,
)

from skeleton_ai.scripting_grok import GrokClient, GrokAuthError


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
    async def list_channels_route(user: dict = auth_dep):
        _gate_admin(user)
        return {"channels": list_channels()}

    @router.get("/channel/{key}")
    async def channel_route(key: str, user: dict = auth_dep):
        _gate_admin(user)
        try:
            return {"channel": get_channel(key)}
        except ValueError as e:
            raise HTTPException(404, str(e))

    @router.post("/catalyst-insights")
    async def catalyst_insights(body: CatalystInsightsRequest, user: dict = auth_dep):
        _gate_admin(user)
        """
        Returns the shaped Catalyst snapshot for the picked channel.
        If Catalyst hasn't harvested anything yet (or the helper isn't
        wired), the endpoint still returns a 200 with empty insight arrays
        so the frontend can degrade gracefully.
        """
        try:
            channel = get_channel(body.channel_key)
        except ValueError as e:
            raise HTTPException(404, str(e))

        catalyst_handle = CHANNEL_KEY_TO_CATALYST_HANDLE.get(body.channel_key, "")

        snapshot: dict | None = None
        if catalyst_hub_snapshot_for_user and catalyst_handle:
            try:
                snapshot = await catalyst_hub_snapshot_for_user(user, channel_handle=catalyst_handle)
            except TypeError:
                # Older signature without keyword arg — try positional.
                try:
                    snapshot = await catalyst_hub_snapshot_for_user(user, catalyst_handle)
                except Exception:
                    snapshot = None
            except Exception:
                snapshot = None

        insights = shape_catalyst_insights(snapshot if isinstance(snapshot, dict) else None)
        return {
            "channel_key": body.channel_key,
            "channel_label": channel["label"],
            "catalyst_handle": catalyst_handle,
            "insights": insights,
            "catalyst_present": bool(snapshot),
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

        # Pull Catalyst context (best effort).
        catalyst_text = ""
        if body.use_catalyst_context and catalyst_hub_snapshot_for_user:
            handle = CHANNEL_KEY_TO_CATALYST_HANDLE.get(body.channel_key, "")
            if handle:
                try:
                    snapshot = await catalyst_hub_snapshot_for_user(user, channel_handle=handle)
                    if isinstance(snapshot, dict):
                        catalyst_text = insights_to_grok_context(shape_catalyst_insights(snapshot))
                except Exception:
                    catalyst_text = ""

        target_minutes = body.target_minutes or channel["default_minutes"]
        outline = generate_outline(
            grok,
            channel["system_prompt"],
            topic=body.topic,
            target_minutes=int(target_minutes),
            catalyst_context=catalyst_text,
        )
        return {
            "channel_key": body.channel_key,
            "topic": body.topic,
            "target_minutes": int(target_minutes),
            "catalyst_context_used": bool(catalyst_text),
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
