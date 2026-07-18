"""
FastAPI router for NYPTID Studio Agent (OpenRouter + Rookcast skills).

  GET    /api/studio-agent/sessions
  POST   /api/studio-agent/sessions
  GET    /api/studio-agent/sessions/{id}
  PATCH  /api/studio-agent/sessions/{id}
  DELETE /api/studio-agent/sessions/{id}
  POST   /api/studio-agent/sessions/{id}/chat
  POST   /api/studio-agent/sessions/{id}/chat/stream
  POST   /api/studio-agent/sessions/{id}/approve
  POST   /api/studio-agent/sessions/{id}/reject
  GET    /api/studio-agent/models
  GET    /api/studio-agent/skills
  GET    /api/studio-agent/queue
  GET    /api/studio-agent/production-control
  POST   /api/studio-agent/dictation
  POST   /api/studio-agent/sessions/{id}/attachments/video
"""
from __future__ import annotations

import asyncio
import hmac
import json
import os
import time
from pathlib import Path
from typing import Any, Callable, Literal

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile, WebSocket
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from studio_agent import jobs as agent_jobs

from studio_agent.access import account_profile, can_use_studio_agent, is_owner
from studio_agent.dictation import transcribe_audio_bytes

from studio_agent import model_registry, openrouter, skills
from studio_agent import memory, production_budget, runner, store, training_capture
from studio_agent.image_model_catalog import seedream_model_profiles
from studio_agent.video_model_catalog import video_model_profiles
from studio_agent.queue import (
    StudioAgentQueueFullError,
    StudioAgentQueueTimeoutError,
    queue_snapshot,
    reset_queue_counters,
)

_MODELS_CACHE: dict[str, Any] = {"at": 0.0, "payload": None}
_MODELS_CACHE_SEC = max(60, int(os.getenv("STUDIO_MODELS_CACHE_SEC", "900") or 900))


def _user_id(user: dict) -> str:
    return str((user or {}).get("id") or (user or {}).get("user_id") or "")


def _membership_plan_for_user(user: dict) -> str:
    uid = _user_id(user)
    if not uid:
        return ""
    try:
        import unified_credits as uc

        uc.ensure_monthly_grant(uid)
        state = uc.get_state(uid)
        return str(state.get("plan") or "").strip().lower()
    except Exception:
        return ""


def _direct_production_command_id(request: Request, *, runpod_enabled: bool) -> str:
    """Return the caller's stable mutation id for every billable backend."""

    _ = runpod_enabled
    command_id = str(request.headers.get("x-idempotency-key") or "").strip()
    if not command_id:
        raise HTTPException(
            400,
            "X-Idempotency-Key is required for production mutations.",
        )
    if len(command_id) > 512:
        raise HTTPException(400, "X-Idempotency-Key is too long.")
    return command_id


class CreateSessionRequest(BaseModel):
    agent_mode: Literal["plan", "studio", "cliplab"] = "plan"
    model: str | None = None
    approval_mode: Literal["auto", "confirm"] = "confirm"
    content_format: Literal["short", "long", "both"] = "both"
    reasoning_depth: Literal["fast", "balanced", "deep"] = "balanced"
    render_style: str | None = "cinematic"
    image_model: str | None = None
    image_model_id: str | None = None
    video_model: str | None = None
    channel_id: str | None = ""
    registry_key: str | None = ""
    channel_title: str | None = ""
    web_search: bool = True
    animate: bool = True
    captions_enabled: bool = True
    caption_mode: Literal["word", "off"] = "word"
    product_website: str = ""


class PatchSessionRequest(BaseModel):
    agent_mode: Literal["plan", "studio", "cliplab"] | None = None
    model: str | None = None
    approval_mode: Literal["auto", "confirm"] | None = None
    content_format: Literal["short", "long", "both"] | None = None
    reasoning_depth: Literal["fast", "balanced", "deep"] | None = None
    render_style: str | None = None
    image_model: str | None = None
    image_model_id: str | None = None
    video_model: str | None = None
    channel_id: str | None = None
    registry_key: str | None = None
    channel_title: str | None = None
    web_search: bool | None = None
    animate: bool | None = None
    captions_enabled: bool | None = None
    caption_mode: Literal["word", "off"] | None = None
    product_website: str | None = None


class ResetProductionRequest(BaseModel):
    target_title: str = ""


class ChatRequest(BaseModel):
    request_id: str = Field(default="", max_length=128)
    message: str = Field(..., min_length=1, max_length=32000)
    reply_to: dict | None = None  # {job_id, kind, scene_index?} for re-editing a previous video/still in same chat
    attachments: list[dict] = Field(default_factory=list, max_length=4)
    agent_mode: Literal["plan", "studio", "cliplab"] | None = "plan"
    channel_id: str | None = ""
    registry_key: str | None = ""
    channel_title: str | None = ""
    render_style: str | None = None
    image_model: str | None = None
    image_model_id: str | None = None
    video_model: str | None = None
    captions_enabled: bool | None = None
    caption_mode: Literal["word", "off"] | None = None


class RememberRequest(BaseModel):
    note: str = Field(..., min_length=3, max_length=2000)
    scope: Literal["global", "channel"] = "channel"
    channel_id: str = ""
    registry_key: str = ""
    channel_title: str = ""
    kind: str = "preference"
    importance: int = 4


class ApproveRequest(BaseModel):
    action_id: str


class RejectRequest(BaseModel):
    action_id: str
    reason: str = ""


class SceneApprovalRequest(BaseModel):
    animate: bool = False


class ScenePromptRequest(BaseModel):
    prompt: str = Field(..., min_length=12, max_length=759)


class SceneBulkApprovalRequest(BaseModel):
    animate: bool = False
    scene_indices: list[int] | None = None


class TrainingConsentRequest(BaseModel):
    training_opt_in: bool
    human_review_opt_in: bool = False
    include_prompts: bool = True
    include_uploads: bool = True
    include_outputs: bool = True
    include_feedback: bool = True


class LoadTestSimulationRequest(BaseModel):
    stages: list[str] = Field(default_factory=lambda: ["render"], max_length=20)
    iterations: int = Field(1, ge=1, le=50)
    hold_ms: int = Field(250, ge=0, le=30000)
    response_kb: int = Field(1, ge=0, le=256)


def _apply_chat_turn_options(session_id: str, session: dict[str, Any], body: ChatRequest) -> dict[str, Any]:
    has_channel_selection = any(
        str(value or "").strip()
        for value in (body.channel_id, body.registry_key, body.channel_title)
    )
    updates: dict[str, Any] = {}
    if has_channel_selection:
        if body.channel_id is not None and str(body.channel_id or "").strip():
            updates["channel_id"] = str(body.channel_id or "").strip()
        if body.registry_key is not None and str(body.registry_key or "").strip():
            updates["registry_key"] = str(body.registry_key or "").strip()
        if body.channel_title is not None and str(body.channel_title or "").strip():
            updates["channel_title"] = str(body.channel_title or "").strip()
    if body.captions_enabled is not None:
        updates["captions_enabled"] = bool(body.captions_enabled)
    if body.caption_mode is not None:
        updates["caption_mode"] = body.caption_mode
        if body.caption_mode == "off":
            updates["captions_enabled"] = False
    if body.render_style is not None and str(body.render_style or "").strip():
        updates["render_style"] = str(body.render_style or "").strip()
    if body.image_model_id is not None or body.image_model is not None:
        updates["image_model"] = store.normalize_image_model(
            body.image_model_id or body.image_model
        )
    if body.video_model is not None:
        updates["video_model"] = store.normalize_video_model(body.video_model)
    if not updates:
        return session
    return store.update_session(session_id, **updates) or session


def build_studio_agent_router(
    *,
    require_auth: Callable,
    get_current_user: Callable | None = None,
    is_admin_check: Callable[[dict], bool] | None = None,
    lane_access_check: Callable[[dict], bool] | None = None,
    export_access_check: Callable[[dict], bool] | None = None,
) -> APIRouter:
    router = APIRouter(prefix="/api/studio-agent", tags=["studio-agent"])

    def _agent_user(user: dict = Depends(require_auth)) -> dict:
        if not can_use_studio_agent(
            user,
            is_admin_check=is_admin_check,
            lane_access_check=lane_access_check,
        ):
            raise HTTPException(
                403,
                "Studio Agent requires an active Studio ($60/mo) or Studio Pro ($200/mo) plan.",
            )
        return user

    def _billing_profile(user: dict) -> dict:
        return account_profile(user, is_admin_check=is_admin_check)

    def _require_job_access(job_id: str, kind: str, user: dict) -> dict[str, Any]:
        """Enforce per-user production ownership without leaking job existence."""
        access = agent_jobs.job_access_metadata(job_id, kind)
        uid = _user_id(user).strip()
        owner_id = str(access.get("owner_id") or "").strip()
        owner_or_admin = is_owner(user, is_admin_check)
        if not access.get("exists"):
            raise HTTPException(404, "job_not_found")
        if owner_id and uid and hmac.compare_digest(owner_id, uid):
            return access
        if owner_or_admin:
            return access
        # Ownerless workspaces predate ownership metadata and are intentionally
        # admin-only. A 404 avoids disclosing another creator's job id.
        raise HTTPException(404, "job_not_found")

    def _require_final_export_access(user: dict) -> None:
        if is_owner(user, is_admin_check):
            return
        if export_access_check is None:
            return
        try:
            if export_access_check(user):
                return
        except Exception:
            pass
        raise HTTPException(403, "Final video export is disabled for controlled-beta accounts.")

    @router.get("/training-consent")
    async def get_training_consent(user: dict = Depends(_agent_user)):
        return {"consent": training_capture.get_consent(_user_id(user))}

    @router.patch("/training-consent")
    async def update_training_consent(
        body: TrainingConsentRequest,
        user: dict = Depends(_agent_user),
    ):
        consent = training_capture.set_consent(
            _user_id(user),
            training_opt_in=body.training_opt_in,
            human_review_opt_in=body.human_review_opt_in,
            include_prompts=body.include_prompts,
            include_uploads=body.include_uploads,
            include_outputs=body.include_outputs,
            include_feedback=body.include_feedback,
        )
        return {"consent": consent}

    @router.delete("/training-data")
    async def delete_training_data(user: dict = Depends(_agent_user)):
        training_capture.set_consent(_user_id(user), training_opt_in=False)
        return {"deletion": training_capture.delete_user_training_data(_user_id(user))}

    @router.get("/models")
    async def list_models(user: dict = Depends(_agent_user)):
        now = time.time()
        cached = _MODELS_CACHE.get("payload")
        if isinstance(cached, dict) and now - float(_MODELS_CACHE.get("at") or 0) < _MODELS_CACHE_SEC:
            return cached
        fal_enabled = bool(str(os.getenv("FAL_KEY") or os.getenv("FAL_AI_KEY") or "").strip())

        async def _video_profiles() -> list[dict[str, Any]]:
            try:
                return await asyncio.wait_for(
                    run_in_threadpool(video_model_profiles, fal_enabled=fal_enabled),
                    timeout=5.0,
                )
            except Exception:
                # Pricing must never delay Agent boot. Verified effective
                # fallbacks stay visible until the provider cache warms.
                return video_model_profiles(
                    fal_enabled=fal_enabled,
                    pricing_snapshot={"source": "fallback", "prices": {}},
                )
        try:
            live = await openrouter.list_models()
            catalog = openrouter.build_model_catalog(live)
            ids = [m.get("id") for m in catalog if m.get("id")]
            recommended = [m["id"] for m in catalog if m.get("recommended")]
            if not recommended:
                recommended = [m for m in openrouter.RECOMMENDED_MODELS if m in ids] or openrouter.RECOMMENDED_MODELS
            providers = sorted({str(m.get("provider") or "") for m in catalog if m.get("provider")})
            payload = {
                "models": catalog,
                "image_models": seedream_model_profiles(
                    fal_enabled=bool(str(os.getenv("FAL_KEY") or os.getenv("FAL_AI_KEY") or "").strip())
                ),
                "video_models": await _video_profiles(),
                "recommended": recommended,
                "count": len(ids),
                "providers": providers,
                "xai_configured": bool(openrouter.xai_api_key()),
                "anthropic_configured": bool(openrouter.anthropic_api_key()),
                "cached": False,
            }
        except Exception as exc:
            catalog = openrouter.build_model_catalog(None)
            payload = {
                "models": catalog,
                "image_models": seedream_model_profiles(
                    fal_enabled=bool(str(os.getenv("FAL_KEY") or os.getenv("FAL_AI_KEY") or "").strip())
                ),
                "video_models": await _video_profiles(),
                "recommended": openrouter.RECOMMENDED_MODELS,
                "error": str(exc),
                "xai_configured": bool(getattr(openrouter, "xai_api_key", lambda: "")()),
                "anthropic_configured": bool(openrouter.anthropic_api_key()),
                "cached": False,
            }
        _MODELS_CACHE["payload"] = payload
        _MODELS_CACHE["at"] = now
        return payload

    @router.get("/skills")
    async def list_skills(user: dict = Depends(_agent_user)):
        return {"skills": skills.list_skill_slugs(), "count": len(skills.list_skill_slugs())}

    @router.get("/queue")
    async def agent_queue_status(user: dict = Depends(_agent_user)):
        """Live OpenRouter/fal concurrency — for UI wait indicators."""
        return await queue_snapshot()

    @router.get("/catalyst/status")
    async def agent_catalyst_status(user: dict = Depends(_agent_user)):
        """Always-on Catalyst runtime health (warm loop + tracked niches)."""
        try:
            from studio_agent import catalyst_runtime

            return {
                "ok": True,
                "runtime": catalyst_runtime.catalyst_runtime_status(),
                "account": _billing_profile(user),
            }
        except Exception as exc:
            return {"ok": False, "error": str(exc)[:200]}

    @router.get("/production-control")
    async def agent_production_control_status(user: dict = Depends(_agent_user)):
        """Read-only contract for queue lanes, approval gates, and budget caps."""
        return {
            "queue": await queue_snapshot(),
            "lanes": production_budget.QUEUE_PRIORITIES,
            "tool_lanes": production_budget.TOOL_LANES,
            "approval_required_tools": sorted(production_budget.APPROVAL_REQUIRED_TOOLS),
            "stage_gates": production_budget.STAGE_GATES,
            "expensive_tools": sorted(production_budget.EXPENSIVE_TOOLS),
            "default_caps_usd": production_budget.DEFAULT_CAPS_USD,
        }

    @router.post("/load-test/render-simulation", include_in_schema=False)
    async def render_load_test_simulation(request: Request, body: LoadTestSimulationRequest):
        """Token-locked queue exerciser. It never calls OpenRouter, FAL, Stripe, or production tools."""
        expected = str(os.getenv("STUDIO_LOAD_TEST_TOKEN", "") or "").strip()
        if not expected:
            raise HTTPException(404, "not found")
        supplied = str(request.headers.get("x-studio-load-test-token") or "").strip()
        auth = str(request.headers.get("authorization") or "").strip()
        if not supplied and auth.lower().startswith("bearer "):
            supplied = auth.split(None, 1)[1].strip()
        if not supplied or not hmac.compare_digest(supplied, expected):
            raise HTTPException(403, "invalid load-test token")

        allowed = {"render", "stills", "i2v", "i2v_premium", "audio", "compose"}
        stages = [str(stage or "").strip().lower() for stage in (body.stages or ["render"])]
        stages = [stage for stage in stages if stage in allowed]
        if not stages:
            raise HTTPException(400, "no valid stages")

        def _run() -> dict[str, Any]:
            from studio_agent.production_slots import production_slot, slot_snapshot

            admissions: list[dict[str, Any]] = []
            started = time.time()
            for _i in range(int(body.iterations)):
                for lane in stages:
                    with production_slot(lane) as admission:
                        admissions.append(admission.as_dict())
                        time.sleep(float(body.hold_ms) / 1000.0)
            payload = "x" * (int(body.response_kb) * 1024)
            return {
                "ok": True,
                "simulated": True,
                "provider_spend_usd": 0.0,
                "stages": stages,
                "iterations": int(body.iterations),
                "hold_ms": int(body.hold_ms),
                "elapsed_ms": round((time.time() - started) * 1000.0, 2),
                "admissions": admissions,
                "slot_snapshot": slot_snapshot(),
                "payload": payload,
            }

        return await run_in_threadpool(_run)

    @router.post("/queue/reset")
    async def agent_queue_reset(user: dict = Depends(_agent_user)):
        """Clear leaked active/waiting counters (owner/admin only)."""
        if not is_owner(user, is_admin_check):
            raise HTTPException(403, "owner access required")
        cleared = await reset_queue_counters()
        snap = await queue_snapshot()
        return {"ok": True, "cleared": cleared, "queue": snap}

    @router.get("/jobs/{job_id}")
    async def production_job_status(
        job_id: str,
        kind: str = Query("longform"),
        session_id: str = Query(""),
        user: dict = Depends(_agent_user),
    ):
        """Unified poll surface for agent-started renders (longform, shortform, reference)."""
        access = _require_job_access(job_id, kind, user)
        snap = agent_jobs.get_job_snapshot(job_id, str(access.get("kind") or kind))
        uid = _user_id(user)
        if snap.get("status") == "complete":
            agent_jobs.record_production_complete_telemetry(
                uid, snap, session_id=session_id or None
            )
        if session_id and snap.get("status") in ("complete", "failed"):
            if not (
                snap.get("kind") == "longform"
                and snap.get("status") == "failed"
                and not agent_jobs.longform_failed_is_terminal(job_id)
            ):
                agent_jobs.prune_session_job(session_id, job_id, user_id=uid)
        return snap

    @router.post("/jobs/{job_id}/cancel")
    async def cancel_production_job(
        job_id: str,
        kind: str = Query("shortform"),
        session_id: str = Query(""),
        user: dict = Depends(_agent_user),
    ):
        """Cancel a locally owned render; RunPod-owned work fails closed."""
        from studio_agent import runpod_bridge
        from studio_agent.tools import cancel_shortform_job

        access = _require_job_access(job_id, kind, user)
        if str(access.get("kind") or kind) != "shortform":
            raise HTTPException(400, "cancel is currently supported for shortform renders only")
        try:
            runpod_receipt = runpod_bridge.get_dispatch_receipt_by_studio_job_id(job_id)
        except Exception as exc:
            raise HTTPException(
                503,
                {
                    "code": "runpod_cancel_ownership_unknown",
                    "message": (
                        "Cancellation ownership could not be verified. No local cancellation was issued, "
                        "and provider spend may continue."
                    ),
                    "job_id": job_id,
                    "error": str(exc),
                },
            ) from exc
        if runpod_receipt:
            raise HTTPException(
                409,
                {
                    "code": "runpod_remote_cancel_not_supported",
                    "message": (
                        "This production is owned by RunPod, but remote cancellation is not implemented. "
                        "No local cancellation was issued, and provider spend may continue until the "
                        "remote worker reaches a terminal state."
                    ),
                    "job_id": job_id,
                    "runpod_job_id": str(runpod_receipt.get("runpod_job_id") or ""),
                    "dispatch_id": str(runpod_receipt.get("dispatch_id") or ""),
                },
            )
        ok = cancel_shortform_job(job_id)
        if not ok:
            raise HTTPException(404, "job workspace not found (it may have already finished)")
        if session_id:
            agent_jobs.prune_session_job(session_id, job_id, user_id=_user_id(user))
        return {"ok": True, "job_id": job_id, "status": "cancelling"}

    @router.get("/jobs/{job_id}/media")
    async def production_job_media(
        job_id: str,
        kind: str = Query("longform"),
        user: dict = Depends(_agent_user),
    ):
        access = _require_job_access(job_id, kind, user)
        _require_final_export_access(user)
        path = agent_jobs.resolve_media_path(job_id, str(access.get("kind") or kind))
        if not path:
            raise HTTPException(404, "media_not_ready")
        return FileResponse(
            path,
            media_type="video/mp4",
            filename=path.name,
            headers={"Cache-Control": "private, max-age=300"},
        )

    @router.get("/jobs/{job_id}/package")
    async def production_job_package(
        job_id: str,
        kind: str = Query("longform"),
        user: dict = Depends(_agent_user),
    ):
        access = _require_job_access(job_id, kind, user)
        path = agent_jobs.resolve_package_path(job_id, str(access.get("kind") or kind))
        if not path:
            raise HTTPException(404, "package_not_ready")
        return FileResponse(
            path,
            media_type="text/plain; charset=utf-8",
            filename=f"{job_id}_upload_package.txt",
            headers={"Cache-Control": "private, max-age=300"},
        )

    @router.get("/jobs/{job_id}/still/{scene_idx}")
    async def production_job_still(
        job_id: str,
        scene_idx: int,
        user: dict = Depends(_agent_user),
    ):
        _require_job_access(job_id, "", user)
        if scene_idx < 0 or scene_idx > 999:
            raise HTTPException(400, "bad_scene_index")
        path = agent_jobs.resolve_still_path(job_id, scene_idx)
        if not path:
            raise HTTPException(404, "still_not_found")
        return FileResponse(
            path,
            media_type="image/png",
            headers={"Cache-Control": "private, no-cache, must-revalidate"},
        )

    @router.get("/jobs/{job_id}/clip/{scene_idx}")
    async def production_job_clip(
        job_id: str,
        scene_idx: int,
        user: dict = Depends(_agent_user),
    ):
        access = _require_job_access(job_id, "shortform", user)
        if str(access.get("kind") or "") != "shortform":
            raise HTTPException(404, "clip_not_found")
        if scene_idx < 0 or scene_idx > 999:
            raise HTTPException(400, "bad_scene_index")
        path = agent_jobs.resolve_clip_path(job_id, scene_idx)
        if not path:
            raise HTTPException(404, "clip_not_found")
        return FileResponse(
            path,
            media_type="video/mp4",
            headers={"Cache-Control": "private, max-age=600"},
        )

    @router.get("/jobs/{job_id}/thumbnail/{thumbnail_idx}")
    async def production_job_thumbnail(
        job_id: str,
        thumbnail_idx: int,
        user: dict = Depends(_agent_user),
    ):
        """Serve a Plan-mode long-form thumbnail candidate inside Agent chat."""
        access = _require_job_access(job_id, "longform", user)
        if str(access.get("kind") or "") != "longform":
            raise HTTPException(404, "thumbnail_not_found")
        if thumbnail_idx < 1 or thumbnail_idx > 12:
            raise HTTPException(400, "bad_thumbnail_index")
        path = agent_jobs.resolve_longform_thumbnail_path(job_id, thumbnail_idx)
        if not path:
            raise HTTPException(404, "thumbnail_not_found")
        return FileResponse(
            path,
            media_type="image/png",
            headers={"Cache-Control": "private, no-cache, must-revalidate"},
        )

    @router.get("/catalyst/skeleton-health")
    async def catalyst_skeleton_health(
        channel_key: str = Query("mrskelewelly"),
        user: dict = Depends(_agent_user),
    ):
        """Audit Catalyst self-learning + skeleton reference memory for Studio Agent."""
        _ = user
        from studio_agent.catalyst_health import ensure_catalyst_skeleton_learning_ready

        return await run_in_threadpool(ensure_catalyst_skeleton_learning_ready, channel_key)

    @router.post("/jobs/{job_id}/scene/{scene_idx}/regenerate")
    async def production_job_scene_regenerate(
        job_id: str,
        scene_idx: int,
        request: Request,
        user: dict = Depends(_agent_user),
    ):
        """Catalyst-audited scene regenerate — preserves style, fixes artifacting."""
        access = _require_job_access(job_id, "", user)
        if scene_idx < 0 or scene_idx > 999:
            raise HTTPException(400, "bad_scene_index")
        try:
            from long_form import pipeline as lf_pipeline
            from studio_agent import tools as agent_tools

            is_longform = str(access.get("kind") or "") == "longform"
            runpod_enabled = agent_tools._runpod_production_enabled()
            command_id = _direct_production_command_id(
                request,
                runpod_enabled=runpod_enabled,
            )
            tool_name = (
                "regenerate_longform_still"
                if is_longform
                else "regenerate_production_scene"
            )
            scene_key = "scene_idx" if is_longform else "scene_index"
            tool_result = await run_in_threadpool(
                agent_tools.execute_tool_logged,
                tool_name,
                {
                    "job_id": job_id,
                    scene_key: scene_idx,
                    "_runpod_command_id": command_id,
                },
                user_id=_user_id(user),
                content_format="long" if is_longform else "short",
            )

            if is_longform:
                snapshot = agent_jobs.get_job_snapshot(job_id, "longform")
            else:
                snapshot = agent_jobs.get_job_snapshot(job_id, "shortform")
            parsed = json.loads(tool_result or "{}")
        except Exception as exc:
            raise HTTPException(400, f"scene_regenerate_failed: {exc}") from exc
        return {
            "ok": True,
            "job_id": job_id,
            "scene_index": scene_idx,
            "tool_result": parsed,
            "snapshot": snapshot,
        }

    @router.put("/jobs/{job_id}/scene/{scene_idx}/prompt")
    async def production_job_scene_prompt(
        job_id: str,
        scene_idx: int,
        body: ScenePromptRequest,
        user: dict = Depends(_agent_user),
    ):
        """Persist the creator's exact provider prompt for the next regeneration."""
        access = _require_job_access(job_id, "shortform", user)
        if str(access.get("kind") or "") != "shortform":
            raise HTTPException(404, "job_not_found")
        if scene_idx < 0 or scene_idx > 999:
            raise HTTPException(400, "bad_scene_index")
        try:
            from skeleton_ai.styled_pipeline import set_scene_prompt_override
            from studio_agent.tools import _shortform_workspace

            scene = await run_in_threadpool(
                set_scene_prompt_override, _shortform_workspace(job_id), scene_idx, body.prompt,
            )
            snapshot = agent_jobs.get_job_snapshot(job_id, "shortform")
        except Exception as exc:
            raise HTTPException(400, f"scene_prompt_update_failed: {exc}") from exc
        return {"ok": True, "job_id": job_id, "scene_index": scene_idx, "scene": scene, "snapshot": snapshot}

    @router.post("/jobs/{job_id}/scene/{scene_idx}/approval")
    async def production_job_scene_approval(
        job_id: str,
        scene_idx: int,
        body: SceneApprovalRequest,
        request: Request,
        user: dict = Depends(_agent_user),
    ):
        import time as _time

        access = _require_job_access(job_id, "shortform", user)
        if str(access.get("kind") or "") != "shortform":
            raise HTTPException(404, "job_not_found")
        if scene_idx < 0 or scene_idx > 999:
            raise HTTPException(400, "bad_scene_index")
        try:
            from studio_agent import tools as agent_tools

            runpod_enabled = agent_tools._runpod_production_enabled()
            command_id = _direct_production_command_id(
                request,
                runpod_enabled=runpod_enabled and bool(body.animate),
            )
            tool_result = await run_in_threadpool(
                agent_tools.set_production_scenes_animate,
                job_id,
                bool(body.animate),
                [scene_idx],
            )
            spawn_parsed = None
            if body.animate:
                if runpod_enabled:
                    raw_spawn = await run_in_threadpool(
                        agent_tools.execute_tool_logged,
                        "animate_production_scenes",
                        {
                            "job_id": job_id,
                            "scene_indices": [scene_idx],
                            "_runpod_command_id": command_id,
                        },
                        user_id=_user_id(user),
                        content_format="short",
                    )
                else:
                    raw_spawn = await run_in_threadpool(
                        agent_tools.spawn_animate_production_scenes,
                        job_id,
                        [scene_idx],
                        user_id=_user_id(user),
                        command_id=command_id,
                    )
                try:
                    spawn_parsed = __import__("json").loads(raw_spawn or "{}")
                except Exception:
                    spawn_parsed = {"status": "running", "raw": raw_spawn}
            snapshot = agent_jobs.get_job_snapshot(job_id, "shortform")
        except Exception as exc:
            raise HTTPException(400, f"scene_approval_failed: {exc}") from exc
        payload: dict = {
            "ok": True,
            "job_id": job_id,
            "scene_index": scene_idx,
            "animate": bool(body.animate),
            "tool_result": tool_result,
            "snapshot": snapshot,
        }
        if body.animate:
            payload["spawn_result"] = spawn_parsed
            payload["active_jobs"] = [{
                "job_id": job_id,
                "kind": "shortform",
                "title": str(snapshot.get("title") or "Short-form animation"),
                "started_at": _time.time(),
            }]
            payload["poll_url"] = f"/api/studio-agent/jobs/{job_id}?kind=shortform"
        return payload

    @router.post("/jobs/{job_id}/scenes/approval")
    async def production_job_scenes_approval(
        job_id: str,
        body: SceneBulkApprovalRequest,
        request: Request,
        user: dict = Depends(_agent_user),
    ):
        import time as _time

        access = _require_job_access(job_id, "shortform", user)
        if str(access.get("kind") or "") != "shortform":
            raise HTTPException(404, "job_not_found")
        indices = body.scene_indices
        if indices is not None and any(idx < 0 or idx > 999 for idx in indices):
            raise HTTPException(400, "bad_scene_index")
        try:
            from studio_agent import tools as agent_tools

            runpod_enabled = agent_tools._runpod_production_enabled()
            command_id = _direct_production_command_id(
                request,
                runpod_enabled=runpod_enabled and bool(body.animate),
            )
            tool_result = await run_in_threadpool(
                agent_tools.set_production_scenes_animate,
                job_id,
                bool(body.animate),
                indices,
            )
            spawn_parsed = None
            if body.animate:
                if runpod_enabled:
                    raw_spawn = await run_in_threadpool(
                        agent_tools.execute_tool_logged,
                        "animate_production_scenes",
                        {
                            "job_id": job_id,
                            "scene_indices": indices,
                            "_runpod_command_id": command_id,
                        },
                        user_id=_user_id(user),
                        content_format="short",
                    )
                else:
                    raw_spawn = await run_in_threadpool(
                        agent_tools.spawn_animate_production_scenes,
                        job_id,
                        indices,
                        user_id=_user_id(user),
                        command_id=command_id,
                    )
                try:
                    spawn_parsed = __import__("json").loads(raw_spawn or "{}")
                except Exception:
                    spawn_parsed = {"status": "running", "raw": raw_spawn}
            snapshot = agent_jobs.get_job_snapshot(job_id, "shortform")
        except Exception as exc:
            raise HTTPException(400, f"scene_bulk_approval_failed: {exc}") from exc
        payload: dict = {
            "ok": True,
            "job_id": job_id,
            "scene_indices": indices,
            "animate": bool(body.animate),
            "tool_result": tool_result,
            "snapshot": snapshot,
        }
        if body.animate:
            payload["spawn_result"] = spawn_parsed
            payload["active_jobs"] = [{
                "job_id": job_id,
                "kind": "shortform",
                "title": str(snapshot.get("title") or "Short-form animation"),
                "started_at": _time.time(),
            }]
            payload["poll_url"] = f"/api/studio-agent/jobs/{job_id}?kind=shortform"
        return payload

    @router.post("/jobs/{job_id}/animate")
    async def production_job_animate(
        job_id: str,
        request: Request,
        user: dict = Depends(_agent_user),
    ):
        """Run i2v for short-form scenes that were explicitly approved for animation."""
        import time as _time

        access = _require_job_access(job_id, "shortform", user)
        if str(access.get("kind") or "") != "shortform":
            raise HTTPException(404, "job_not_found")
        try:
            from studio_agent import tools as agent_tools

            runpod_enabled = agent_tools._runpod_production_enabled()
            command_id = _direct_production_command_id(
                request,
                runpod_enabled=runpod_enabled,
            )
            if runpod_enabled:
                raw = await run_in_threadpool(
                    agent_tools.execute_tool_logged,
                    "animate_production_scenes",
                    {
                        "job_id": job_id,
                        "_runpod_command_id": command_id,
                    },
                    user_id=_user_id(user),
                    content_format="short",
                )
            else:
                raw = agent_tools.spawn_animate_production_scenes(
                    job_id,
                    user_id=_user_id(user),
                    command_id=command_id,
                )
            try:
                parsed = __import__("json").loads(raw or "{}")
            except Exception:
                parsed = {"status": "running", "raw": raw}
            snapshot = agent_jobs.get_job_snapshot(job_id, "shortform")
        except Exception as exc:
            raise HTTPException(400, f"animation_failed: {exc}") from exc
        return {
            "ok": True,
            "job_id": job_id,
            "tool_result": parsed,
            "snapshot": snapshot,
            "active_jobs": [{
                "job_id": job_id,
                "kind": "shortform",
                "title": str(snapshot.get("title") or "Short-form animation"),
                "started_at": _time.time(),
            }],
            "poll_url": f"/api/studio-agent/jobs/{job_id}?kind=shortform",
        }

    @router.post("/jobs/{job_id}/expand-proof")
    async def production_job_expand_proof(
        job_id: str,
        request: Request,
        user: dict = Depends(_agent_user),
    ):
        """Expand an approved one-scene long-form visual proof into the full still gallery."""
        access = _require_job_access(job_id, "longform", user)
        if str(access.get("kind") or "") != "longform":
            raise HTTPException(404, "job_not_found")
        try:
            from long_form import pipeline as lf_pipeline
            from studio_agent import tools as agent_tools

            # Must run on the FastAPI event-loop thread — create_task fails inside run_in_threadpool.
            runpod_enabled = agent_tools._runpod_production_enabled()
            command_id = _direct_production_command_id(
                request,
                runpod_enabled=runpod_enabled,
            )
            raw = await run_in_threadpool(
                agent_tools.execute_tool_logged,
                "expand_longform_visual_proof",
                {
                    "job_id": job_id,
                    "_runpod_command_id": command_id,
                },
                user_id=_user_id(user),
                content_format="long",
            )
            tool_result = json.loads(raw or "{}")
            snapshot = agent_jobs.get_job_snapshot(job_id, "longform")
        except Exception as exc:
            raise HTTPException(400, str(exc)) from exc
        payload = {"ok": True, "job_id": job_id, "snapshot": snapshot}
        if tool_result is not None:
            payload["tool_result"] = tool_result
        return payload

    @router.post("/jobs/{job_id}/finalize")
    async def production_job_finalize(
        job_id: str,
        request: Request,
        kind: str = Query("longform"),
        captions_enabled: bool | None = Query(None),
        caption_mode: Literal["word", "off"] | None = Query(None),
        user: dict = Depends(_agent_user),
    ):
        """Agent subscribers can finalize approved productions after the relevant review gate."""
        import time as _time

        access = _require_job_access(job_id, kind, user)
        normalized_kind = str(access.get("kind") or kind or "longform").strip().lower()
        from studio_agent import tools as agent_tools

        runpod_enabled = agent_tools._runpod_production_enabled()
        command_id = _direct_production_command_id(
            request,
            runpod_enabled=runpod_enabled,
        )
        if normalized_kind == "shortform":
            try:
                preflight = agent_tools.shortform_finalize_preflight(job_id)
                if preflight.get("status") != "ready":
                    raise HTTPException(409, preflight)
                if runpod_enabled:
                    raw = await run_in_threadpool(
                        agent_tools.execute_tool_logged,
                        "finalize_production",
                        {
                            "job_id": job_id,
                            "captions_enabled": captions_enabled,
                            "caption_mode": caption_mode,
                            "_runpod_command_id": command_id,
                        },
                        user_id=_user_id(user),
                        content_format="short",
                    )
                else:
                    raw = agent_tools.spawn_finalize_production(
                        job_id,
                        captions_enabled=captions_enabled,
                        caption_mode=caption_mode,
                        user_id=_user_id(user),
                        command_id=command_id,
                    )
                try:
                    parsed = __import__("json").loads(raw or "{}")
                except Exception:
                    parsed = {"status": "running", "raw": raw}
                if isinstance(parsed, dict) and parsed.get("status") == "awaiting_animation":
                    raise HTTPException(409, parsed)
                snapshot = agent_jobs.get_job_snapshot(job_id, "shortform")
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(400, f"finalize_failed: {exc}") from exc
            return {
                "ok": True,
                "job_id": job_id,
                "kind": "shortform",
                "tool_result": parsed,
                "snapshot": snapshot,
                "active_jobs": [] if (not runpod_enabled and snapshot.get("status") == "complete") else [{
                    "job_id": job_id,
                    "kind": "shortform",
                    "title": str(snapshot.get("title") or "Short-form finalize"),
                    "started_at": _time.time(),
                }],
                "poll_url": f"/api/studio-agent/jobs/{job_id}?kind=shortform",
            }

        try:
            raw = await run_in_threadpool(
                agent_tools.execute_tool_logged,
                "finalize_longform_render",
                {
                    "job_id": job_id,
                    "_runpod_command_id": command_id,
                },
                user_id=_user_id(user),
                content_format="long",
            )
            parsed = json.loads(raw or "{}")
            out = {
                "ok": True,
                "job_id": job_id,
                "kind": "longform",
                "tool_result": parsed,
                "snapshot": agent_jobs.get_job_snapshot(job_id, "longform"),
            }
        except Exception as exc:
            raise HTTPException(400, f"finalize_failed: {exc}") from exc
        return {
            **out,
            "active_jobs": [{
                "job_id": job_id,
                "kind": "longform",
                "title": "Long-form finalize",
                "started_at": _time.time(),
            }],
            "poll_url": f"/api/studio-agent/jobs/{job_id}?kind=longform",
        }

    @router.post("/dictation")
    async def dictation_transcribe(
        user: dict = Depends(_agent_user),
        audio: UploadFile = File(...),
    ):
        """Server STT for recorded mic audio (xAI primary, FAL fallback)."""
        from upload_limits import MAX_DICTATION_AUDIO_BYTES, UploadTooLargeError, read_upload_limited

        try:
            raw = await read_upload_limited(
                audio,
                max_bytes=MAX_DICTATION_AUDIO_BYTES,
                label="dictation audio",
            )
        except UploadTooLargeError as exc:
            raise HTTPException(413, "Dictation audio exceeds 25MB") from exc
        try:
            text, provider = await transcribe_audio_bytes(
                raw,
                filename=str(audio.filename or "dictation.webm"),
                content_type=str(audio.content_type or ""),
            )
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(503, str(exc)) from exc
        except Exception as exc:
            raise HTTPException(502, f"Transcription failed: {exc}") from exc
        return {"text": text, "chars": len(text), "provider": provider}

    @router.websocket("/dictation/stream")
    async def dictation_live_stream(
        websocket: WebSocket,
        language: str = Query(""),
    ):
        """Authenticated proxy to xAI streaming STT for live voice planning.

        Browser clients authenticate with the first JSON frame; non-browser
        clients may use an Authorization header. Tokens are never accepted in
        the WebSocket URL.
        """
        await websocket.accept()

        async def _resolve_user_from_token(raw: str) -> dict | None:
            cleaned = str(raw or "").strip()
            if not cleaned:
                return None
            if not get_current_user:
                # Misconfigured mount — do not silently accept; surface clearly.
                return None
            # Strip accidental "Bearer " prefix from query/body.
            if cleaned.lower().startswith("bearer "):
                cleaned = cleaned[7:].strip()

            try:
                # Prefer dedicated resolver when auth helpers expose it.
                resolve = getattr(get_current_user, "__resolve_token__", None)
                if callable(resolve):
                    resolved = await resolve(cleaned)
                else:
                    class _FakeCred:
                        scheme = "Bearer"
                        credentials = cleaned

                    resolved = await get_current_user(_FakeCred())
            except Exception:
                return None
            return resolved if isinstance(resolved, dict) else None

        user: dict | None = None
        # Header first when a non-browser client can set one. Browser clients
        # authenticate with the first JSON frame so credentials never enter a URL.
        try:
            auth_header = str(websocket.headers.get("authorization") or websocket.headers.get("Authorization") or "").strip()
        except Exception:
            auth_header = ""
        if auth_header:
            user = await _resolve_user_from_token(auth_header)
        # Short auth frame so JWT is not stuck in browser/proxy URL history.
        if not user:
            try:
                first = await asyncio.wait_for(websocket.receive_json(), timeout=8.0)
            except Exception:
                first = None
            if isinstance(first, dict) and str(first.get("type") or "").strip().lower() == "auth":
                user = await _resolve_user_from_token(str(first.get("token") or first.get("access_token") or ""))
            elif isinstance(first, dict) and str(first.get("token") or "").strip():
                # Tolerate bare token frames from older clients.
                user = await _resolve_user_from_token(str(first.get("token") or ""))

        if not user:
            await websocket.send_json({
                "type": "error",
                "code": "auth_required",
                "message": "Sign in required. Refresh Studio, then try the mic again.",
            })
            await websocket.close(code=4401)
            return
        if not can_use_studio_agent(
            user,
            is_admin_check=is_admin_check,
            lane_access_check=lane_access_check,
        ):
            await websocket.send_json({
                "type": "error",
                "code": "plan_required",
                "message": (
                    "Studio Agent requires an active Studio ($60/mo) or Studio Pro ($200/mo) plan. "
                    "Owners have unlimited access."
                ),
            })
            await websocket.close(code=4403)
            return
        from studio_agent.dictation_stream import proxy_dictation_stream

        await proxy_dictation_stream(websocket, language=str(language or "").strip())

    @router.post("/sessions/{session_id}/attachments/image")
    async def upload_image_attachment(
        session_id: str,
        file: UploadFile = File(...),
        user: dict = Depends(_agent_user),
    ):
        """Persist an uploaded skeleton/product reference image for shortform still locking."""
        from studio_agent.attachments import MAX_IMAGE_ATTACHMENT_BYTES, save_image_attachment
        from upload_limits import UploadTooLargeError, read_upload_limited

        uid = _user_id(user)
        session = store.get_session(session_id, user_id=uid)
        if not session:
            raise HTTPException(404, "session not found")
        if not file or not file.filename:
            raise HTTPException(400, "No image file")
        try:
            raw = await read_upload_limited(
                file,
                max_bytes=MAX_IMAGE_ATTACHMENT_BYTES,
                label="image attachment",
            )
        except UploadTooLargeError as exc:
            raise HTTPException(413, "Image exceeds 12MB") from exc
        if not raw or len(raw) < 1024:
            raise HTTPException(400, "Image too small")
        try:
            saved = save_image_attachment(session_id, str(file.filename), raw)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        store.update_session(
            session_id,
            skeleton_reference_image=str(saved["path"]),
            image_model=store.SKELETON_DEFAULT_IMAGE_MODEL,
        )
        return {
            "ok": True,
            "name": str(saved["name"]),
            "mime_type": str(file.content_type or saved["mime_type"]),
            "size": int(saved["size"]),
            "path": str(saved["path"]),
            "skeleton_reference_image": str(saved["path"]),
        }

    @router.post("/sessions/{session_id}/attachments/video")
    async def upload_video_attachment(
        session_id: str,
        file: UploadFile = File(...),
        user: dict = Depends(_agent_user),
    ):
        """Persist an uploaded Studio Agent reference video for analysis in this chat."""
        from studio_agent.attachments import save_video_upload_attachment
        from upload_limits import UploadTooLargeError

        uid = _user_id(user)
        session = store.get_session(session_id, user_id=uid)
        if not session:
            raise HTTPException(404, "session not found")
        if not file or not file.filename:
            raise HTTPException(400, "No video file")
        try:
            saved = await save_video_upload_attachment(
                session_id,
                str(file.filename),
                file,
            )
        except UploadTooLargeError as exc:
            raise HTTPException(413, "Video exceeds 3GB") from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        existing = [str(p) for p in list(session.get("latest_attachment_paths") or []) if p]
        next_paths = [*existing, str(saved["path"])][-8:]
        store.update_session(
            session_id,
            latest_attachment_paths=next_paths,
            latest_attachment_at=time.time(),
        )
        return {
            "ok": True,
            "name": str(saved["name"]),
            "mime_type": str(file.content_type or saved["mime_type"]),
            "size": int(saved["size"]),
            "path": str(saved["path"]),
        }

    @router.get("/credits")
    async def credits(user: dict = Depends(_agent_user)):
        """Unified wallet + account tier (owner unmetered vs paying subscriber)."""
        import unified_credits as uc

        uid = _user_id(user)
        profile = _billing_profile(user)
        try:
            uc.ensure_monthly_grant(uid)
            state = uc.get_state(uid)
        except Exception as exc:
            state = {"balance": 0, "plan": "", "error": str(exc)}
        state.update(profile)
        state["unlimited"] = bool(profile.get("unlimited"))
        try:
            state["recent"] = uc.recent_ledger(uid, limit=10) if uid and not profile.get("unlimited") else []
        except Exception:
            state["recent"] = []
        return state

    @router.get("/sessions")
    async def list_sessions(user: dict = Depends(_agent_user), limit: int = 40):
        uid = _user_id(user)
        sessions = store.list_sessions(uid, limit=limit)
        return {
            "sessions": [_session_summary(s) for s in sessions],
            "count": len(sessions),
        }

    @router.post("/sessions")
    async def create_session(body: CreateSessionRequest, user: dict = Depends(_agent_user)):
        selected_model = body.model or openrouter.DEFAULT_MODEL
        try:
            model_registry.assert_model_selectable(selected_model)
        except model_registry.ModelSelectionError as exc:
            raise HTTPException(422, str(exc)) from exc
        try:
            openrouter.api_key()
        except RuntimeError as exc:
            raise HTTPException(503, str(exc)) from exc
        image_model = str(body.image_model_id or body.image_model or "").strip()
        session = store.create_session(
            user_id=_user_id(user),
            model=selected_model,
            agent_mode=body.agent_mode,
            approval_mode=body.approval_mode,
            content_format=body.content_format,
            reasoning_depth=body.reasoning_depth,
            render_style=body.render_style or store.DEFAULT_RENDER_STYLE,
            image_model=store.normalize_image_model(image_model or store.DEFAULT_IMAGE_MODEL),
            video_model=store.normalize_video_model(body.video_model or store.DEFAULT_VIDEO_MODEL),
            channel_id=body.channel_id or "",
            registry_key=body.registry_key or "",
            channel_title=body.channel_title or "",
            web_search=body.web_search,
            animate=body.animate,
            captions_enabled=body.captions_enabled,
            caption_mode=body.caption_mode,
            product_website=body.product_website,
        )
        return {"session": _public_session(session)}

    @router.get("/memory")
    async def get_memory(
        channel_id: str = Query(""),
        registry_key: str = Query(""),
        user: dict = Depends(_agent_user),
    ):
        uid = _user_id(user)
        return {
            "ok": True,
            "summary": memory.summarize_for_prompt(
                uid,
                channel_id=channel_id,
                registry_key=registry_key,
            ),
            "profile": memory.public_profile(uid),
        }

    @router.post("/memory")
    async def remember_memory(body: RememberRequest, user: dict = Depends(_agent_user)):
        uid = _user_id(user)
        item = memory.remember(
            uid,
            body.note,
            scope=body.scope,
            channel_id=body.channel_id,
            registry_key=body.registry_key,
            title=body.channel_title,
            kind=body.kind,
            source="api",
            importance=body.importance,
        )
        return {"ok": True, "memory": item, "profile": memory.public_profile(uid)}

    @router.get("/render-styles")
    async def list_render_styles():
        from studio_agent.render_styles import DEFAULT_RENDER_STYLE, list_render_styles

        return {
            "default": DEFAULT_RENDER_STYLE,
            "styles": list_render_styles(),
        }

    @router.get("/continuous-evaluation")
    async def continuous_evaluation_status(user: dict = Depends(_agent_user)):
        """Owner release gate: verified evidence and promoted regressions only."""
        if not is_owner(user, is_admin_check):
            raise HTTPException(403, "owner access required")
        from studio_agent.continuous_evaluation import evaluation_health

        return evaluation_health()

    @router.get("/style-preview/{key}")
    async def style_preview(key: str, user: dict = Depends(_agent_user)):
        """Serve an existing private style still without starting provider work."""
        from studio_agent.render_styles import get_cached_style_preview_path
        from fastapi.responses import FileResponse

        try:
            p = get_cached_style_preview_path(key)
        except KeyError:
            raise HTTPException(404, "style preview not found")
        if p is None:
            raise HTTPException(404, "style preview not found")
        return FileResponse(str(p), media_type="image/png", headers={"Cache-Control": "private, max-age=86400"})

    @router.get("/style-preview/{key}/video")
    async def style_preview_video(key: str, user: dict = Depends(_agent_user)):
        """Serve an existing private motion preview without provider work."""
        from studio_agent.render_styles import get_cached_style_preview_video_path
        from fastapi.responses import FileResponse

        try:
            p = get_cached_style_preview_video_path(key)
        except KeyError:
            raise HTTPException(404, "style preview video not found")
        if p is None:
            raise HTTPException(404, "style preview video not found")
        return FileResponse(str(p), media_type="video/mp4", headers={"Cache-Control": "private, max-age=86400"})

    @router.post("/sessions/{session_id}/rollover")
    async def rollover_session(session_id: str, user: dict = Depends(_agent_user)):
        """Copy transcript, pending actions, and active jobs into a fresh session."""
        uid = _user_id(user)
        session = store.rollover_session(session_id, user_id=uid)
        if not session:
            raise HTTPException(404, "session not found")
        return {"session": _public_session(session), "rolled_over_from": session_id}

    @router.post("/sessions/{session_id}/fork")
    async def fork_session_with_context(session_id: str, user: dict = Depends(_agent_user)):
        """Fresh chat with channel settings + compacted prior context, without prior jobs."""
        uid = _user_id(user)
        session = store.fork_session_with_context(session_id, user_id=uid)
        if not session:
            raise HTTPException(404, "session not found")
        return {"session": _public_session(session), "forked_from": session_id}

    @router.get("/sessions/{session_id}")
    async def get_session(
        session_id: str,
        sync_pending: bool = Query(True),
        message_tail: int = Query(0, ge=0, le=500),
        user: dict = Depends(_agent_user),
    ):
        uid = _user_id(user)
        if sync_pending:
            session = store.force_sync_session(session_id, user_id=uid)
            session = session or store.get_session(session_id, user_id=uid, reconcile_jobs=False)
        else:
            session = store.get_session(session_id, user_id=uid, reconcile_jobs=False)
        if not session:
            raise HTTPException(404, "session not found")
        return {"session": _public_session(session, message_tail=message_tail)}

    @router.post("/sessions/{session_id}/sync")
    async def sync_session(
        session_id: str,
        message_tail: int = Query(0, ge=0, le=500),
        user: dict = Depends(_agent_user),
    ):
        """UI 'Sync chat' — authoritative reload of transcript + pending + jobs.

        Always prunes stale production Approves. Never rebuilds start_shortform
        pending from old transcript tool rows.
        """
        uid = _user_id(user)
        session = store.force_sync_session(session_id, user_id=uid)
        if not session:
            raise HTTPException(404, "session not found")
        return {
            "session": _public_session(session, message_tail=message_tail),
            "synced": True,
            "pending_count": len(session.get("pending_actions") or []),
            "message_count": len(session.get("messages") or []),
            "active_run_count": len(store.active_runs(session) or []),
        }

    @router.post("/sessions/{session_id}/reset-production")
    async def reset_production_state(
        session_id: str,
        body: ResetProductionRequest | None = None,
        message_tail: int = Query(0, ge=0, le=500),
        user: dict = Depends(_agent_user),
    ):
        """Clear poisoned production state in-place — same chat, fresh production boundary."""
        uid = _user_id(user)
        session = store.get_session(session_id, user_id=uid, reconcile_jobs=False)
        if not session:
            raise HTTPException(404, "session not found")
        target = str((body.target_title if body else "") or "").strip()
        if not target:
            target = store.resolve_current_production_target(
                session,
                list(session.get("messages") or []),
            )
        session = store.advance_production_cycle(
            session,
            target_title=target,
            messages=list(session.get("messages") or []),
            reason="user_reset",
            persist=True,
        )
        session = store.force_sync_session(session_id, user_id=uid) or session
        return {
            "session": _public_session(session, message_tail=message_tail),
            "production_state": store.get_production_state(session),
            "reset": True,
        }

    @router.patch("/sessions/{session_id}")
    async def patch_session(
        session_id: str,
        body: PatchSessionRequest,
        user: dict = Depends(_agent_user),
    ):
        session = store.get_session(session_id, user_id=_user_id(user))
        if not session:
            raise HTTPException(404, "session not found")
        updates: dict[str, Any] = {}
        if body.model is not None:
            try:
                model_registry.assert_model_selectable(body.model)
            except model_registry.ModelSelectionError as exc:
                raise HTTPException(422, str(exc)) from exc
            updates["model"] = body.model
        if body.agent_mode is not None:
            updates["agent_mode"] = body.agent_mode
            if body.agent_mode == "plan":
                updates["pending_actions"] = []
                updates["last_production"] = {}
        if body.approval_mode is not None:
            updates["approval_mode"] = body.approval_mode
        if body.content_format is not None:
            updates["content_format"] = body.content_format
        if body.reasoning_depth is not None:
            updates["reasoning_depth"] = body.reasoning_depth
        if body.render_style is not None:
            updates["render_style"] = body.render_style
        if body.image_model_id is not None or body.image_model is not None:
            updates["image_model"] = store.normalize_image_model(
                body.image_model_id or body.image_model
            )
        if body.video_model is not None:
            updates["video_model"] = store.normalize_video_model(body.video_model)
        if body.channel_id is not None:
            updates["channel_id"] = body.channel_id.strip()
        if body.registry_key is not None:
            updates["registry_key"] = body.registry_key.strip()
        if body.channel_title is not None:
            updates["channel_title"] = body.channel_title.strip()
        if body.web_search is not None:
            updates["web_search"] = bool(body.web_search)
        if body.animate is not None:
            updates["animate"] = bool(body.animate)
        if body.captions_enabled is not None:
            updates["captions_enabled"] = bool(body.captions_enabled)
        if body.caption_mode is not None:
            updates["caption_mode"] = body.caption_mode
            if body.caption_mode == "off":
                updates["captions_enabled"] = False
        if body.product_website is not None:
            updates["product_website"] = body.product_website.strip()[:2000]
        session = store.update_session(session_id, **updates)
        return {"session": _public_session(session)}

    @router.delete("/sessions/{session_id}")
    async def delete_session(session_id: str, user: dict = Depends(_agent_user)):
        uid = _user_id(user)
        if not store.delete_session(session_id, user_id=uid):
            raise HTTPException(404, "session not found")
        return {"ok": True, "session_id": session_id}

    @router.post("/sessions/{session_id}/chat")
    async def chat(session_id: str, body: ChatRequest, user: dict = Depends(_agent_user)):
        session = store.get_session(session_id, user_id=_user_id(user))
        if not session:
            raise HTTPException(404, "session not found")
        session = _apply_chat_turn_options(session_id, session, body)
        plan = _membership_plan_for_user(user)
        profile = _billing_profile(user)
        try:
            result = await runner.run_turn(
                session,
                body.message.strip(),
                membership_plan=plan,
                billing_profile=profile,
                reply_to=getattr(body, 'reply_to', None),
                attachments=body.attachments,
                agent_mode=body.agent_mode,
            )
        except (StudioAgentQueueFullError, StudioAgentQueueTimeoutError) as exc:
            snap = await queue_snapshot()
            raise HTTPException(
                503,
                detail=str(exc),
                headers={"X-Studio-Queue": "full" if isinstance(exc, StudioAgentQueueFullError) else "timeout"},
            ) from exc
        except RuntimeError as exc:
            raise HTTPException(502, str(exc)) from exc
        except Exception as exc:
            raise HTTPException(502, f"Agent turn failed: {exc}") from exc
        return result

    @router.post("/sessions/{session_id}/chat/stream")
    async def chat_stream(session_id: str, body: ChatRequest, user: dict = Depends(_agent_user)):
        # Skip heavy job reconcile before first SSE byte — proxies (Cloudflare) 524 if TTFB > ~100s.
        session = store.get_session(session_id, user_id=_user_id(user), reconcile_jobs=False)
        if not session:
            raise HTTPException(404, "session not found")
        session = _apply_chat_turn_options(session_id, session, body)
        plan = _membership_plan_for_user(user)
        profile = _billing_profile(user)
        try:
            openrouter.api_key()
        except RuntimeError as exc:
            raise HTTPException(503, str(exc)) from exc
        run = store.create_run(
            session_id,
            user_text=body.message.strip(),
            request_id=str(body.request_id or "").strip(),
        )
        run_id = run["run_id"]
        idempotent_replay = bool(run.get("idempotent_replay"))

        def sse_frame(event: str, payload: dict[str, Any]) -> str:
            return f"event: {event}\ndata: {json.dumps(payload, default=str)}\n\n"

        async def body_iter():
            # Immediate SSE comment so load balancers see bytes before LLM/tool work begins.
            yield ": connected\n\n"
            if idempotent_replay:
                current = store.get_run(session_id, run_id, user_id=_user_id(user)) or run
                for saved in list(current.get("events") or []):
                    event_name = str(saved.get("event") or "status")
                    if event_name in {"done", "error", "interrupted"}:
                        continue
                    event_data = saved.get("data") if isinstance(saved.get("data"), dict) else {}
                    replay_data = {
                        **event_data,
                        "event": event_name,
                        "run_id": run_id,
                        "idempotent_replay": True,
                    }
                    yield sse_frame(event_name, replay_data)

                run_status = str(current.get("status") or "running")
                if run_status == "complete":
                    saved_result = current.get("result") if isinstance(current.get("result"), dict) else {}
                    yield sse_frame("done", {
                        **saved_result,
                        "event": "done",
                        "run_id": run_id,
                        "idempotent_replay": True,
                    })
                    return
                if run_status in store.ACTIVE_RUN_STATUSES:
                    yield sse_frame("status", {
                        "event": "status",
                        "message": "This run is already in progress. Resume will attach to the saved run.",
                        "run_id": run_id,
                        "run_status": run_status,
                        "idempotent_replay": True,
                        "resume_required": True,
                    })
                    return
                yield sse_frame("error", {
                    "event": "error",
                    "message": str(current.get("error") or "The saved run did not complete."),
                    "run_id": run_id,
                    "run_status": run_status,
                    "idempotent_replay": True,
                })
                return

            accepted = {"event": "status", "message": "Run accepted.", "run_id": run_id}
            yield sse_frame("status", accepted)
            store.append_run_event(
                session_id,
                run_id,
                "status",
                accepted,
            )
            async for chunk in runner.stream_turn(
                session,
                body.message.strip(),
                membership_plan=plan,
                billing_profile=profile,
                reply_to=body.reply_to,
                attachments=body.attachments,
                agent_mode=body.agent_mode,
                run_id=run_id,
            ):
                yield chunk

        return StreamingResponse(
            body_iter(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @router.get("/sessions/{session_id}/runs")
    async def list_session_runs(
        session_id: str,
        active_only: bool = Query(False),
        user: dict = Depends(_agent_user),
    ):
        session = store.get_session(session_id, user_id=_user_id(user))
        if not session:
            raise HTTPException(404, "session not found")
        return {"runs": store.list_runs(session_id, user_id=_user_id(user), active_only=active_only)}

    @router.get("/sessions/{session_id}/runs/{run_id}")
    async def get_session_run(session_id: str, run_id: str, user: dict = Depends(_agent_user)):
        run = store.get_run(session_id, run_id, user_id=_user_id(user))
        if not run:
            raise HTTPException(404, "run not found")
        return {"run": run}

    @router.post("/sessions/{session_id}/approve")
    async def approve(session_id: str, body: ApproveRequest, user: dict = Depends(_agent_user)):
        session = store.get_session(session_id, user_id=_user_id(user))
        if not session:
            raise HTTPException(404, "session not found")
        plan = _membership_plan_for_user(user)
        profile = _billing_profile(user)
        try:
            result = await runner.approve_action(
                session,
                body.action_id,
                membership_plan=plan,
                billing_profile=profile,
            )
            training_capture.capture_event(
                _user_id(user),
                "production_feedback",
                {"decision": "approved", "action_id": body.action_id, "result": result},
                session_id=session_id,
            )
            return result
        except (StudioAgentQueueFullError, StudioAgentQueueTimeoutError) as exc:
            raise HTTPException(503, detail=str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(
                409,
                f"{exc}. Tap Sync chat (reloads pending from transcript), or ask the agent to "
                "propose start_shortform_generate again with the same topic.",
            ) from exc
        except Exception as exc:
            import logging
            import traceback

            logging.getLogger("studio_agent").exception(
                "approve failed session=%s action=%s", session_id, body.action_id
            )
            detail = str(exc) or exc.__class__.__name__
            # Keep detail useful for UI without dumping full stacks to the client.
            raise HTTPException(500, f"Approve failed: {detail}") from exc

    @router.post("/sessions/{session_id}/retry-production")
    async def retry_production(session_id: str, user: dict = Depends(_agent_user)):
        session = store.get_session(session_id, user_id=_user_id(user))
        if not session:
            raise HTTPException(404, "session not found")
        plan = _membership_plan_for_user(user)
        profile = _billing_profile(user)
        try:
            return await runner.retry_last_production(
                session,
                membership_plan=plan,
                billing_profile=profile,
            )
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        except (StudioAgentQueueFullError, StudioAgentQueueTimeoutError) as exc:
            raise HTTPException(503, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(500, str(exc)) from exc

    @router.post("/sessions/{session_id}/reject")
    async def reject(session_id: str, body: RejectRequest, user: dict = Depends(_agent_user)):
        session = store.get_session(session_id, user_id=_user_id(user))
        if not session:
            raise HTTPException(404, "session not found")
        action = store.pop_pending_action(session_id, body.action_id)
        if not action:
            raise HTTPException(404, "pending action not found")
        reason = body.reason.strip() or "User rejected this action."
        messages = session.get("messages") or []
        messages.append({
            "role": "user",
            "content": f"[Rejected {action.get('tool')}] {reason}",
        })
        clear_fields: dict[str, object] = {}
        if str(action.get("tool") or "") in {"start_shortform_generate", "start_longform_render"}:
            clear_fields["last_production"] = {}
            clear_fields["active_jobs"] = [
                j for j in (session.get("active_jobs") or [])
                if str(j.get("kind") or "") not in {"shortform", "longform"}
            ]
        store.update_session(session_id, messages=messages, **clear_fields)
        training_capture.capture_event(
            _user_id(user),
            "production_feedback",
            {
                "decision": "rejected",
                "action_id": body.action_id,
                "tool": action.get("tool"),
                "arguments": action.get("arguments") or {},
                "reason": reason,
            },
            session_id=session_id,
        )
        return {"ok": True, "rejected": action}

    return router


def _session_summary(session: dict[str, Any]) -> dict[str, Any]:
    return {
        "session_id": session.get("session_id"),
        "title": store.derive_title(session),
        "model": session.get("model"),
        "agent_mode": session.get("agent_mode") or "plan",
        "content_format": session.get("content_format"),
        "reasoning_depth": session.get("reasoning_depth") or "balanced",
        "render_style": session.get("render_style") or store.DEFAULT_RENDER_STYLE,
        "image_model": store.normalize_image_model(session.get("image_model")),
        "video_model": store.normalize_video_model(session.get("video_model")),
        "media_route_revision": store.media_route_snapshot(session)["revision"],
        "interaction_state": str(session.get("interaction_state") or "plan"),
        "production_gate_open": bool(session.get("production_gate_open", False)),
        "active_command_id": str(session.get("active_command_id") or ""),
        "channel_id": session.get("channel_id") or "",
        "registry_key": session.get("registry_key") or "",
        "channel_title": session.get("channel_title") or "",
        "web_search": bool(session.get("web_search", True)),
        "animate": bool(session.get("animate", True)),
        "captions_enabled": bool(session.get("captions_enabled", True)),
        "caption_mode": session.get("caption_mode") or ("off" if session.get("captions_enabled") is False else "word"),
        "product_website": session.get("product_website") or "",
        "message_count": len(session.get("messages") or []),
        "pending_count": len(session.get("pending_actions") or []),
        "active_runs": store.active_runs(session),
        "created_at": session.get("created_at"),
        "updated_at": session.get("updated_at"),
        "blocked_job_ids": list(session.get("blocked_job_ids") or []),
        "production_state": store.get_production_state(session),
    }


def _public_session(session: dict[str, Any], *, message_tail: int = 0) -> dict[str, Any]:
    pending_concept = session.get("pending_concept")
    if not isinstance(pending_concept, dict):
        pending_concept = None
    thumbnail_review = session.get("thumbnail_review")
    if not isinstance(thumbnail_review, dict):
        thumbnail_review = None
    messages = list(session.get("messages") or [])
    total_messages = len(messages)
    tail = max(0, int(message_tail or 0))
    if tail > 0 and total_messages > tail:
        messages = messages[-tail:]
    payload = {
        **_session_summary(session),
        "approval_mode": session.get("approval_mode"),
        "pending_actions": session.get("pending_actions") or [],
        "pending_concept": pending_concept,
        "thumbnail_review": thumbnail_review,
        "active_jobs": session.get("active_jobs") or [],
        "active_runs": store.active_runs(session),
        "messages": messages,
        "forked_from": session.get("forked_from") or "",
        "context_ingested": bool(session.get("context_ingested")),
        "skip_job_recovery": bool(session.get("skip_job_recovery")),
    }
    if tail > 0 and total_messages > tail:
        payload["message_count"] = total_messages
        payload["messages_truncated"] = True
    return payload
