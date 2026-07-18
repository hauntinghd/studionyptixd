"""
FastAPI router for the Long-Form generation API (clean v6 surface).

Wires into backend.py the same way Skeleton AI does:
    from long_form_router import build_long_form_router
    app.include_router(build_long_form_router(
        require_auth=require_auth,
        catalyst_hub_snapshot_for_user=catalyst_hub_snapshot_for_user,
    ))

Routes:
  GET  /api/long-form/channels                       channel registry list
  GET  /api/long-form/channel/{key}                  single channel canonical
  POST /api/long-form/catalyst-insights              {channel_key} → shaped Catalyst data
  POST /api/long-form/outline                        Selected-model outline pass (chapter list)
  POST /api/long-form/outline/expand-chapter         Lazy per-chapter beat expansion

  POST /api/long-form/render-start                   Kick a background render → job_id
  GET  /api/long-form/jobs                           List recent renders for the panel
  GET  /api/long-form/jobs/{job_id}                  Full state.json + status snapshot
  GET  /api/long-form/jobs/{job_id}/state            Same as above (alias for resume flow)
  GET  /api/long-form/jobs/{job_id}/status           Lightweight phase + percent poll
  GET  /api/long-form/jobs/{job_id}/mp4              Stream final MP4 (owner auth)
  GET  /api/long-form/jobs/{job_id}/thumbnail/{idx}  Serve thumbnail PNG (owner auth)
  GET  /api/long-form/jobs/{job_id}/still/{scene}    Serve scene PNG (owner auth)

The legacy /api/longform/* + /api/creative/* routes (28 endpoints) remain
mounted alongside this — they back the older v5 sessions that shipped
Wirecard / Mongol 9H / Ottoman 9H. The new /api/long-form/* (hyphenated)
is the canonical surface; legacy retires after parity is verified.

PR #120: render pipeline wired live. HR sleep-doc renders end-to-end via
long_form.pipeline.run_sleep_doc_pipeline. PR #122 will register
'v5_episode' (EM) in the same dispatcher.
"""
from __future__ import annotations
import hmac
import json
import os
from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel

from long_form.prompts.channels import (
    CHANNELS,
    list_channels,
    get_channel,
    channel_outline_prompt_extras,
)
from long_form.catalyst_bridge import (
    CHANNEL_KEY_TO_ID,
    shape_catalyst_insights,
    insights_to_grok_context,
    fetch_channel_snapshot,
)

from long_form import pipeline as lf_pipeline

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


class RegenerateSceneRequest(BaseModel):
    scene_idx: int
    new_prompt: str | None = None


class RegenerateThumbnailRequest(BaseModel):
    custom_prompt: str | None = None


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
    model: str | None = None


class ExpandChapterRequest(BaseModel):
    channel_key: str
    outline_title: str
    chapter: dict
    model: str | None = None


class RenderRequest(BaseModel):
    channel_key: str
    outline: dict
    image_model: str | None = None
    voice_id: str | None = None
    model: str | None = None


def build_long_form_router(
    *,
    require_auth: Callable[..., dict] | None = None,
    catalyst_hub_snapshot_for_user: Callable | None = None,
    is_admin_check: Callable[[dict], bool] | None = None,
) -> APIRouter:
    """
    Long-form endpoints require an authenticated, active Studio plan (or an
    owner/admin account). Production spend remains credit-reserved separately;
    the frontend visibility check is never the security boundary.
    """
    router = APIRouter(prefix="/api/long-form", tags=["long-form"])
    auth_dep = Depends(require_auth) if require_auth else Depends(lambda: {"user_id": "anon"})

    def _user_id(user: dict) -> str:
        return str((user or {}).get("id") or (user or {}).get("user_id") or "").strip()

    def _is_admin(user: dict) -> bool:
        if not is_admin_check:
            return True
        try:
            return bool(is_admin_check(user))
        except Exception:
            return False

    def _require_owned_longform_job(job_id: str, user: dict) -> dict[str, Any]:
        """Fail closed before mutating another creator's long-form workspace."""

        from studio_agent import jobs as agent_jobs

        access = agent_jobs.job_access_metadata(job_id, "longform")
        if not access.get("exists") or str(access.get("kind") or "") != "longform":
            raise HTTPException(404, "job_not_found")
        owner_id = str(access.get("owner_id") or "").strip()
        uid = _user_id(user)
        if owner_id:
            if uid and hmac.compare_digest(owner_id, uid):
                return access
            # Do not disclose whether a cross-account job exists, even when an
            # admin panel accidentally carries another creator's job id.
            raise HTTPException(404, "job_not_found")
        # Legacy local jobs predate owner metadata. The surrounding admin gate
        # is their only compatibility exception; new renders stamp owner below.
        if _is_admin(user):
            return access
        raise HTTPException(404, "job_not_found")

    def _gate_admin(user: dict) -> None:
        """Legacy name; allows owners or active Studio subscribers."""
        if not is_admin_check:
            return  # No admin checker passed (test mode) — open.
        try:
            if is_admin_check(user):
                return
        except Exception:
            pass
        uid = _user_id(user)
        if not uid:
            raise HTTPException(401, "authentication_required")
        from studio_agent.access import STUDIO_AGENT_PLANS, unified_plan

        if unified_plan(uid) in STUDIO_AGENT_PLANS:
            return
        raise HTTPException(403, "active_studio_plan_required")

    async def _text_metering_args(model: str | None, *, input_chars: int) -> dict[str, Any]:
        """Resolve model rates before any paid planning inference is sent."""

        from studio_agent import openrouter

        selected = str(model or openrouter.DEFAULT_MODEL).strip() or openrouter.DEFAULT_MODEL
        try:
            prompt_ppm, completion_ppm = await openrouter.model_pricing(selected)
        except Exception as exc:
            raise HTTPException(
                503,
                "Selected text-model pricing is temporarily unavailable; no provider request was sent.",
            ) from exc
        if prompt_ppm is None or completion_ppm is None:
            raise HTTPException(
                503,
                "Selected text-model pricing is unavailable; no provider request was sent.",
            )
        return {
            "model": selected,
            "_billing_prompt_price_per_m": float(prompt_ppm),
            "_billing_completion_price_per_m": float(completion_ppm),
            "_billing_input_chars": max(0, int(input_chars or 0)),
        }

    @router.get("/channels")
    async def list_channels_route(user: dict = auth_dep, format: str | None = None):
        _gate_admin(user)
        rows = list_channels(format_filter=format or "long_form")
        available: list[dict[str, Any]] = []
        for row in rows:
            try:
                lf_pipeline.validate_channel_pipeline(row)
            except lf_pipeline.LFRenderError:
                continue
            available.append(row)
        return {"channels": available}

    @router.get("/fal-pricing")
    async def fal_pricing_route(
        user: dict = auth_dep,
        refresh: bool = False,
        channel_key: str | None = None,
        target_minutes: int | None = None,
    ):
        """Live fal Platform API prices + optional channel cost preview."""
        _gate_admin(user)
        from long_form import fal_pricing as fp

        snap = fp.get_pricing_snapshot(force_refresh=refresh)
        out: dict[str, Any] = {
            "status": fp.pricing_status(),
            "prices": snap.get("prices"),
            "endpoints": snap.get("endpoints"),
        }
        if channel_key:
            try:
                channel = get_channel(channel_key)
            except ValueError as e:
                raise HTTPException(404, str(e)) from e
            minutes = int(target_minutes or channel.get("default_minutes") or 15)
            n_ch = max(3, min(20, round(minutes / 5)))
            outline = {
                "chapters": [{"title": f"Ch{i}"} for i in range(n_ch)],
                "target_duration_sec": minutes * 60,
            }
            out["cost_preview"] = lf_pipeline.compute_render_cost(
                channel,
                outline,
                force_refresh_pricing=refresh,
            )
        return out

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
    async def outline_route(body: OutlineRequest, request: Request, user: dict = auth_dep):
        _gate_admin(user)
        from studio_agent.direct_production import require_idempotency_key

        require_idempotency_key(request)
        if not body.topic or not body.topic.strip():
            raise HTTPException(400, "topic is required")
        try:
            channel = get_channel(body.channel_key)
        except ValueError as e:
            raise HTTPException(404, str(e))

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

        from studio_agent.direct_production import execute_logged_production

        metering = await _text_metering_args(
            body.model,
            input_chars=(
                len(str(channel.get("system_prompt") or ""))
                + len(body.topic)
                + len(combined_context)
                + len(title_template_block)
            ),
        )
        return await execute_logged_production(
            "generate_longform_outline",
            {
                "channel_key": body.channel_key,
                "topic": body.topic.strip(),
                "target_minutes": int(target_minutes),
                "combined_context": combined_context,
                "catalyst_context_used": bool(catalyst_text),
                "references_used": bool(refs_text),
                "title_template_block": title_template_block,
                **metering,
            },
            request=request,
            user_id=_user_id(user),
            content_format="long",
        )

    @router.post("/outline/expand-chapter")
    async def expand_chapter_route(
        body: ExpandChapterRequest,
        request: Request,
        user: dict = auth_dep,
    ):
        _gate_admin(user)
        from studio_agent.direct_production import require_idempotency_key

        require_idempotency_key(request)
        try:
            channel = get_channel(body.channel_key)
        except ValueError as e:
            raise HTTPException(404, str(e))
        from studio_agent.direct_production import execute_logged_production

        chapter_json = json.dumps(body.chapter or {}, ensure_ascii=False, sort_keys=True)
        metering = await _text_metering_args(
            body.model,
            input_chars=(
                len(str(channel.get("system_prompt") or ""))
                + len(body.outline_title or "")
                + len(chapter_json)
            ),
        )
        return await execute_logged_production(
            "expand_longform_chapter",
            {
                "channel_key": body.channel_key,
                "outline_title": body.outline_title,
                "chapter": dict(body.chapter or {}),
                **metering,
            },
            request=request,
            user_id=_user_id(user),
            content_format="long",
        )

    # ─────────────────────────────────────────────────────────────────────
    # Render endpoints (PR #120 — replaces the phase-2 stubs)
    # ─────────────────────────────────────────────────────────────────────

    def _validate_job_id(job_id: str) -> None:
        if not job_id or not job_id.replace("_", "").isalnum() or len(job_id) > 32:
            raise HTTPException(400, "bad_job_id")

    def _runpod_receipt(job_id: str) -> dict[str, Any] | None:
        from studio_agent.runpod_bridge import get_dispatch_receipt_by_studio_job_id

        return get_dispatch_receipt_by_studio_job_id(job_id)

    def _runpod_snapshot(job_id: str) -> dict[str, Any] | None:
        if _runpod_receipt(job_id) is None:
            return None
        from studio_agent.jobs import get_job_snapshot

        return get_job_snapshot(job_id, "longform")

    def _legacy_status_from_snapshot(job_id: str, snapshot: dict[str, Any]) -> dict[str, Any]:
        return {
            "job_id": job_id,
            "phase": str(snapshot.get("stage") or snapshot.get("status") or "unknown"),
            "percent": int(snapshot.get("progress") or 0),
            "error": snapshot.get("error") or "",
            "scene_done": int(snapshot.get("current_scene") or 0),
            "scene_total": int(snapshot.get("total_scenes") or 0),
            "chapter_done": int(snapshot.get("current_chapter") or 0),
            "chapter_total": int(snapshot.get("total_chapters") or 0),
            "running": bool(snapshot.get("running")),
            "execution_backend": snapshot.get("execution_backend"),
            "runpod": snapshot.get("runpod"),
        }

    @router.post("/render-start")
    async def render_start_route(body: RenderRequest, request: Request, user: dict = auth_dep):
        _gate_admin(user)
        outline = dict(body.outline or {})
        outline["user_id"] = _user_id(user)
        if body.model:
            outline["chat_model"] = str(body.model).strip()
        from studio_agent.direct_production import (
            execute_logged_production,
            require_longform_runpod_if_global_enabled,
        )

        require_longform_runpod_if_global_enabled()
        payload = await execute_logged_production(
            "start_longform_render",
            {
                "channel_key": body.channel_key,
                "title": str(outline.get("title") or body.channel_key).strip(),
                "topic": str(outline.get("topic") or outline.get("title") or body.channel_key).strip(),
                "chapters_json": json.dumps(outline, ensure_ascii=False),
                "motion_policy": str(outline.get("motion_policy") or "balanced"),
                "render_style": str(outline.get("render_style") or "cinematic"),
                "visual_proof_only": bool(outline.get("visual_proof_only", True)),
                "image_model_id": str(body.image_model or outline.get("image_model_id") or ""),
                "sfx_enabled": bool(outline.get("sfx_enabled", False)),
                "sound_design_brief": str(outline.get("sound_design_brief") or ""),
                "background_music": str(outline.get("background_music") or "off"),
            },
            request=request,
            user_id=_user_id(user),
            content_format="long",
        )
        job_id = str(payload.get("job_id") or payload.get("studio_job_id") or "").strip()
        payload.update({
            "job_id": job_id,
            "phase": "queued",
            "poll_url": f"/api/long-form/jobs/{job_id}/status",
            "state_url": f"/api/long-form/jobs/{job_id}/state",
            "mp4_url_when_done": f"/api/long-form/jobs/{job_id}/mp4",
            "legacy_route": "/api/long-form/render-start",
        })
        return payload

    @router.get("/jobs")
    async def list_jobs_route(user: dict = auth_dep, limit: int = 20):
        _gate_admin(user)
        rows = lf_pipeline.list_recent_jobs(limit=int(limit))
        # Trim each row to the panel-card shape (frontend doesn't need the
        # full chapters array on this view — it can fetch /jobs/{id}/state
        # when the user clicks Resume).
        out: list[dict] = []
        for st in rows:
            job_id = str(st.get("job_id") or "").strip()
            if not _is_admin(user):
                try:
                    _require_owned_longform_job(job_id, user)
                except HTTPException:
                    continue
            outline = st.get("outline") or {}
            mp4_rel = st.get("mp4_path") or ""
            mp4_present = bool(mp4_rel)
            thumbs_count = int(st.get("thumbnails_generated", 0) or 0)
            out.append({
                "job_id": st.get("job_id"),
                "channel_key": st.get("channel_key"),
                "channel_label": st.get("channel_label"),
                "pipeline_kind": st.get("pipeline_kind", ""),
                "title": outline.get("title", ""),
                "phase": st.get("phase", ""),
                "percent": int(st.get("percent", 0) or 0),
                "error": st.get("error", ""),
                "created_at": st.get("created_at", 0),
                "started_at": st.get("started_at", 0),
                "finished_at": st.get("finished_at", 0),
                "mp4_present": mp4_present,
                "mp4_url": f"/api/long-form/jobs/{st.get('job_id')}/mp4" if mp4_present else "",
                "mp4_duration_sec": float(st.get("mp4_duration_sec", 0) or 0),
                "mp4_size_bytes": int(st.get("mp4_size_bytes", 0) or 0),
                "thumbnail_url": (
                    f"/api/long-form/jobs/{st.get('job_id')}/thumbnail/1"
                    if thumbs_count > 0 else ""
                ),
            })
        return {"jobs": out, "total": len(out)}

    @router.get("/jobs/{job_id}")
    async def job_state_route(job_id: str, user: dict = auth_dep):
        _gate_admin(user)
        _validate_job_id(job_id)
        _require_owned_longform_job(job_id, user)
        st = lf_pipeline.load_state(job_id)
        if not st:
            runpod_snapshot = _runpod_snapshot(job_id)
            if runpod_snapshot is not None:
                return {
                    **_legacy_status_from_snapshot(job_id, runpod_snapshot),
                    "kind": "longform",
                    "status": runpod_snapshot.get("status"),
                    "title": runpod_snapshot.get("title") or "Long-form production",
                }
            raise HTTPException(404, "no such job")
        # Merge in the live status snapshot (phase + percent from registry,
        # in case the on-disk state is stale between phase boundaries).
        live = lf_pipeline.get_status(job_id) or {}
        merged = dict(st)
        runpod_snapshot = _runpod_snapshot(job_id)
        if runpod_snapshot is not None:
            merged.update(_legacy_status_from_snapshot(job_id, runpod_snapshot))
        for k in ("phase", "percent", "error", "scene_done", "scene_total",
                  "narration_done", "narration_total", "chapter_done", "chapter_total"):
            if k in live:
                merged[k] = live[k]
        # Surface URLs for media that exists.
        if merged.get("mp4_path"):
            merged["mp4_url"] = f"/api/long-form/jobs/{job_id}/mp4"
        thumbs = int(merged.get("thumbnails_generated", 0) or 0)
        merged["thumbnail_urls"] = [
            f"/api/long-form/jobs/{job_id}/thumbnail/{i + 1}" for i in range(thumbs)
        ]
        # PR #137: include the dynamic cost estimate so the gate UI shows
        # the real Stage 2 number instead of the stale channel ceiling.
        try:
            channel_for_cost = get_channel(merged.get("channel_key", "")) if merged.get("channel_key") else None
            if channel_for_cost:
                merged["cost_estimate"] = lf_pipeline.compute_render_cost(
                    channel_for_cost, merged.get("outline") or {}
                )
        except Exception:
            pass
        return merged

    @router.get("/jobs/{job_id}/state")
    async def job_state_alias_route(job_id: str, user: dict = auth_dep):
        # Resume flow uses /state — keep an alias so the frontend code
        # mirrors the ZT private convention.
        return await job_state_route(job_id, user=user)

    @router.get("/jobs/{job_id}/status")
    async def job_status_route(job_id: str, user: dict = auth_dep):
        _gate_admin(user)
        _validate_job_id(job_id)
        _require_owned_longform_job(job_id, user)
        runpod_snapshot = _runpod_snapshot(job_id)
        if runpod_snapshot is not None:
            return _legacy_status_from_snapshot(job_id, runpod_snapshot)
        live = lf_pipeline.get_status(job_id)
        if live is None:
            # Fall back to disk state (process restart).
            st = lf_pipeline.load_state(job_id)
            if not st:
                raise HTTPException(404, "no such job")
            live = {
                "phase": st.get("phase", "unknown"),
                "percent": int(st.get("percent", 0) or 0),
                "error": st.get("error", ""),
            }
        return {
            "job_id": job_id,
            "phase": live.get("phase", "unknown"),
            "percent": int(live.get("percent", 0) or 0),
            "error": live.get("error", ""),
            "scene_done": int(live.get("scene_done", 0) or 0),
            "scene_total": int(live.get("scene_total", 0) or 0),
            "narration_done": int(live.get("narration_done", 0) or 0),
            "narration_total": int(live.get("narration_total", 0) or 0),
            "chapter_done": int(live.get("chapter_done", 0) or 0),
            "chapter_total": int(live.get("chapter_total", 0) or 0),
            "updated_at": float(live.get("updated_at", 0) or 0),
        }

    # Private media serving. Native media elements cannot attach a Bearer
    # header, so the frontend fetches these routes with auth and uses temporary
    # object URLs. A job id is an identifier, never an authorization secret.

    @router.get("/jobs/{job_id}/mp4")
    async def job_mp4_route(job_id: str, user: dict = auth_dep):
        _gate_admin(user)
        _validate_job_id(job_id)
        _require_owned_longform_job(job_id, user)
        path = lf_pipeline.job_mp4_path(job_id)
        if not path or not path.exists():
            raise HTTPException(404, "mp4_not_ready")
        return FileResponse(
            path,
            media_type="video/mp4",
            filename=path.name,
            headers={"Cache-Control": "private, max-age=300"},
        )

    @router.get("/jobs/{job_id}/thumbnail/{idx}")
    async def job_thumbnail_route(job_id: str, idx: int, user: dict = auth_dep):
        _gate_admin(user)
        _validate_job_id(job_id)
        _require_owned_longform_job(job_id, user)
        if idx < 1 or idx > 12:
            raise HTTPException(400, "bad_index")
        path = lf_pipeline.job_thumbnail_path(job_id, idx)
        if not path or not path.exists():
            raise HTTPException(404, "thumbnail_not_found")
        return FileResponse(
            path,
            media_type="image/png",
            headers={"Cache-Control": "private, max-age=600"},
        )

    @router.get("/jobs/{job_id}/still/{scene_idx}")
    async def job_still_route(job_id: str, scene_idx: int, user: dict = auth_dep):
        _gate_admin(user)
        _validate_job_id(job_id)
        _require_owned_longform_job(job_id, user)
        if scene_idx < 0 or scene_idx > 9999:
            raise HTTPException(400, "bad_scene_index")
        path = lf_pipeline.job_still_path(job_id, scene_idx)
        if not path or not path.exists():
            raise HTTPException(404, "still_not_found")
        return FileResponse(
            path,
            media_type="image/png",
            headers={"Cache-Control": "private, max-age=600"},
        )

    # ─────────────────────────────────────────────────────────────────────
    # PR #127: per-scene approval gate endpoints
    # ─────────────────────────────────────────────────────────────────────

    @router.post("/jobs/{job_id}/regenerate-scene")
    async def job_regenerate_scene_route(
        job_id: str,
        body: RegenerateSceneRequest,
        request: Request,
        user: dict = auth_dep,
    ):
        _gate_admin(user)
        _validate_job_id(job_id)
        _require_owned_longform_job(job_id, user)
        if body.scene_idx < 0 or body.scene_idx > 9999:
            raise HTTPException(400, "bad_scene_index")
        from studio_agent.direct_production import (
            execute_logged_production,
            require_longform_runpod_if_global_enabled,
        )

        require_longform_runpod_if_global_enabled()
        payload = await execute_logged_production(
            "regenerate_longform_still",
            {
                "job_id": job_id,
                "scene_idx": int(body.scene_idx),
                "reason": str(body.new_prompt or ""),
            },
            request=request,
            user_id=_user_id(user),
            content_format="long",
        )
        payload.update({"job_id": job_id, "scene_idx": int(body.scene_idx)})
        return payload

    @router.post("/jobs/{job_id}/finalize")
    async def job_finalize_route(job_id: str, request: Request, user: dict = auth_dep):
        _gate_admin(user)
        _validate_job_id(job_id)
        _require_owned_longform_job(job_id, user)
        from studio_agent.direct_production import (
            execute_logged_production,
            require_longform_runpod_if_global_enabled,
        )

        require_longform_runpod_if_global_enabled()
        payload = await execute_logged_production(
            "finalize_longform_render",
            {"job_id": job_id},
            request=request,
            user_id=_user_id(user),
            content_format="long",
        )
        payload.update({
            "job_id": job_id,
            "phase": "finalizing",
            "poll_url": f"/api/long-form/jobs/{job_id}/status",
        })
        return payload

    @router.post("/jobs/{job_id}/regenerate-thumbnail/{idx}")
    async def job_regenerate_thumbnail_route(
        job_id: str,
        idx: int,
        request: Request,
        body: RegenerateThumbnailRequest = RegenerateThumbnailRequest(),
        user: dict = auth_dep,
    ):
        """Regenerate one thumbnail tile via seedream. Optional custom_prompt
        body overrides the channel's thumbnail_style + variant hint entirely.
        Returns cache-busted URL so the <img> reloads."""
        _gate_admin(user)
        _validate_job_id(job_id)
        _require_owned_longform_job(job_id, user)
        if idx < 1 or idx > 12:
            raise HTTPException(400, "bad_thumbnail_idx")
        from studio_agent.direct_production import execute_logged_production

        return await execute_logged_production(
            "regenerate_longform_thumbnail",
            {
                "job_id": job_id,
                "idx": int(idx),
                "custom_prompt": str(body.custom_prompt or ""),
            },
            request=request,
            user_id=_user_id(user),
            content_format="long",
        )

    @router.post("/jobs/{job_id}/cancel")
    async def job_cancel_route(job_id: str, user: dict = auth_dep):
        """Cancel an in-flight render. Cooperative cancellation — the
        next per-scene boundary picks it up. Already-rendered stills /
        clips / VOs / SFX stay on disk so the user can re-finalize
        later from where it stopped."""
        _gate_admin(user)
        _validate_job_id(job_id)
        _require_owned_longform_job(job_id, user)
        if _runpod_receipt(job_id) is not None:
            raise HTTPException(
                409,
                "This job is owned by RunPod. Legacy local cancel cannot stop its remote spend and is disabled.",
            )
        try:
            result = lf_pipeline.cancel_render(job_id)
        except lf_pipeline.LFRenderError as e:
            raise HTTPException(400, f"cancel_failed: {e}")
        return {"job_id": job_id, **result}

    @router.get("/jobs/{job_id}/scenes")
    async def job_scenes_route(job_id: str, user: dict = auth_dep):
        """Return the scene grid for the per-scene approval gate.

        Each scene entry includes its prompt + chapter context + still URL
        (capability-token via job_id) + a cache-bust version derived from
        the still file mtime so regenerated stills surface immediately."""
        _gate_admin(user)
        _validate_job_id(job_id)
        _require_owned_longform_job(job_id, user)
        state = lf_pipeline.load_state(job_id)
        if not state:
            raise HTTPException(404, "no such job")
        rows: list[dict] = []
        kind = (state.get("pipeline_kind") or "sleep_doc").strip()

        if kind == "v5_episode":
            for r in state.get("scene_briefs") or []:
                gi = int(r.get("global_idx", -1))
                if gi < 0:
                    continue
                still = lf_pipeline.job_still_path(job_id, gi)
                version = int(still.stat().st_mtime) if (still and still.exists()) else 0
                brief = r.get("brief") or {}
                rows.append({
                    "scene_idx": gi,
                    "chapter_index": int(r.get("chapter_index", 0)),
                    "local_idx": int(r.get("local_idx", 0)),
                    "scene_prompt": brief.get("scene_prompt", ""),
                    "narration": brief.get("narration", ""),
                    "still_url": (
                        f"/api/long-form/jobs/{job_id}/still/{gi}?v={version}"
                        if version else ""
                    ),
                    "still_present": bool(still and still.exists()),
                })
        else:
            # sleep_doc — derive scene grid from chapters.json since this
            # pipeline doesn't persist scene_briefs (storage minimization).
            try:
                chapters_path = (
                    lf_pipeline.LF_OUTPUT_ROOT / job_id / "chapters.json"
                )
                if chapters_path.exists():
                    chapters_data = json.loads(chapters_path.read_text(encoding="utf-8"))
                    chapters = chapters_data.get("chapters") or []
                    spc = int(state.get("scenes_per_chapter", 30))
                    for ch in chapters:
                        ch_idx = int(ch.get("chapter_index", 0))
                        prompts = ch.get("scene_prompts") or []
                        for li, p in enumerate(prompts):
                            gi = ch_idx * spc + li
                            still = lf_pipeline.job_still_path(job_id, gi)
                            version = int(still.stat().st_mtime) if (still and still.exists()) else 0
                            rows.append({
                                "scene_idx": gi,
                                "chapter_index": ch_idx,
                                "local_idx": li,
                                "scene_prompt": p,
                                "narration": "",
                                "still_url": (
                                    f"/api/long-form/jobs/{job_id}/still/{gi}?v={version}"
                                    if version else ""
                                ),
                                "still_present": bool(still and still.exists()),
                            })
            except Exception as e:
                raise HTTPException(500, f"scenes_load_failed: {e}")

        return {
            "job_id": job_id,
            "pipeline_kind": kind,
            "phase": state.get("phase", ""),
            "scenes": rows,
            "total": len(rows),
        }

    return router
