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
"""
from __future__ import annotations

import asyncio
import hmac
import os
import time
from pathlib import Path
from typing import Any, Callable, Literal

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from studio_agent import jobs as agent_jobs

from studio_agent.access import account_profile, can_use_studio_agent, is_owner
from studio_agent.dictation import transcribe_audio_bytes

from studio_agent import openrouter, skills
from studio_agent import memory, production_budget, runner, store, training_capture
from studio_agent.queue import (
    StudioAgentQueueFullError,
    StudioAgentQueueTimeoutError,
    queue_snapshot,
    reset_queue_counters,
)


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


class CreateSessionRequest(BaseModel):
    model: str | None = None
    approval_mode: Literal["auto", "confirm"] = "confirm"
    content_format: Literal["short", "long", "both"] = "both"
    reasoning_depth: Literal["fast", "balanced", "deep"] = "balanced"
    render_style: str | None = "cinematic"
    image_model: str = store.DEFAULT_IMAGE_MODEL
    video_model: str = store.DEFAULT_VIDEO_MODEL
    channel_id: str | None = ""
    registry_key: str | None = ""
    channel_title: str | None = ""
    web_search: bool = True
    animate: bool = True
    captions_enabled: bool = True
    caption_mode: Literal["word", "off"] = "word"
    product_website: str = ""


class PatchSessionRequest(BaseModel):
    model: str | None = None
    approval_mode: Literal["auto", "confirm"] | None = None
    content_format: Literal["short", "long", "both"] | None = None
    reasoning_depth: Literal["fast", "balanced", "deep"] | None = None
    render_style: str | None = None
    image_model: str | None = None
    video_model: str | None = None
    channel_id: str | None = None
    registry_key: str | None = None
    channel_title: str | None = None
    web_search: bool | None = None
    animate: bool | None = None
    captions_enabled: bool | None = None
    caption_mode: Literal["word", "off"] | None = None
    product_website: str | None = None


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=32000)
    reply_to: dict | None = None  # {job_id, kind, scene_index?} for re-editing a previous video/still in same chat
    attachments: list[dict] = Field(default_factory=list, max_length=4)
    channel_id: str | None = ""
    registry_key: str | None = ""
    channel_title: str | None = ""
    captions_enabled: bool | None = None
    caption_mode: Literal["word", "off"] | None = None
    image_model: str | None = None
    video_model: str | None = None


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
    if body.image_model is not None and str(body.image_model or "").strip():
        updates["image_model"] = body.image_model
    if body.video_model is not None and str(body.video_model or "").strip():
        updates["video_model"] = body.video_model
    if not updates:
        return session
    return store.update_session(session_id, **updates) or session


def build_studio_agent_router(
    *,
    require_auth: Callable,
    is_admin_check: Callable[[dict], bool] | None = None,
    lane_access_check: Callable[[dict], bool] | None = None,
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
        try:
            live = await openrouter.list_models()
            catalog = openrouter.build_model_catalog(live)
            ids = [m.get("id") for m in live if m.get("id")]
            recommended = [m["id"] for m in catalog if m.get("recommended")]
            if not recommended:
                recommended = [m for m in openrouter.RECOMMENDED_MODELS if m in ids] or openrouter.RECOMMENDED_MODELS
            return {
                "models": catalog,
                "recommended": recommended,
                "count": len(ids),
            }
        except Exception as exc:
            catalog = openrouter.build_model_catalog(None)
            return {
                "models": catalog,
                "recommended": openrouter.RECOMMENDED_MODELS,
                "error": str(exc),
            }

    @router.get("/skills")
    async def list_skills(user: dict = Depends(_agent_user)):
        return {"skills": skills.list_skill_slugs(), "count": len(skills.list_skill_slugs())}

    @router.get("/queue")
    async def agent_queue_status(user: dict = Depends(_agent_user)):
        """Live OpenRouter/fal concurrency — for UI wait indicators."""
        return await queue_snapshot()

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
        snap = agent_jobs.get_job_snapshot(job_id, kind)
        uid = _user_id(user)
        if snap.get("status") == "complete":
            agent_jobs.record_production_complete_telemetry(
                uid, snap, session_id=session_id or None
            )
        if session_id and snap.get("status") in ("complete", "failed"):
            agent_jobs.prune_session_job(session_id, job_id, user_id=uid)
        return snap

    @router.post("/jobs/{job_id}/cancel")
    async def cancel_production_job(
        job_id: str,
        kind: str = Query("shortform"),
        session_id: str = Query(""),
        user: dict = Depends(_agent_user),
    ):
        """Signal a running render to stop at its next checkpoint (no more fal spend)."""
        from studio_agent.tools import cancel_shortform_job

        if kind != "shortform":
            raise HTTPException(400, "cancel is currently supported for shortform renders only")
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
        _ = user
        path = agent_jobs.resolve_media_path(job_id, kind)
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
        _ = user
        path = agent_jobs.resolve_package_path(job_id, kind)
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
        _ = user
        if scene_idx < 0 or scene_idx > 999:
            raise HTTPException(400, "bad_scene_index")
        path = agent_jobs.resolve_still_path(job_id, scene_idx)
        if not path:
            raise HTTPException(404, "still_not_found")
        return FileResponse(
            path,
            media_type="image/png",
            headers={"Cache-Control": "private, max-age=600"},
        )

    @router.post("/jobs/{job_id}/scene/{scene_idx}/approval")
    async def production_job_scene_approval(
        job_id: str,
        scene_idx: int,
        body: SceneApprovalRequest,
        user: dict = Depends(_agent_user),
    ):
        _ = user
        if scene_idx < 0 or scene_idx > 999:
            raise HTTPException(400, "bad_scene_index")
        try:
            from studio_agent import tools as agent_tools

            tool_result = await run_in_threadpool(
                agent_tools.set_production_scenes_animate,
                job_id,
                bool(body.animate),
                [scene_idx],
            )
            snapshot = agent_jobs.get_job_snapshot(job_id, "shortform")
        except Exception as exc:
            raise HTTPException(400, f"scene_approval_failed: {exc}") from exc
        return {
            "ok": True,
            "job_id": job_id,
            "scene_index": scene_idx,
            "animate": bool(body.animate),
            "tool_result": tool_result,
            "snapshot": snapshot,
        }

    @router.post("/jobs/{job_id}/scenes/approval")
    async def production_job_scenes_approval(
        job_id: str,
        body: SceneBulkApprovalRequest,
        user: dict = Depends(_agent_user),
    ):
        _ = user
        indices = body.scene_indices
        if indices is not None and any(idx < 0 or idx > 999 for idx in indices):
            raise HTTPException(400, "bad_scene_index")
        try:
            from studio_agent import tools as agent_tools

            tool_result = await run_in_threadpool(
                agent_tools.set_production_scenes_animate,
                job_id,
                bool(body.animate),
                indices,
            )
            snapshot = agent_jobs.get_job_snapshot(job_id, "shortform")
        except Exception as exc:
            raise HTTPException(400, f"scene_bulk_approval_failed: {exc}") from exc
        return {
            "ok": True,
            "job_id": job_id,
            "scene_indices": indices,
            "animate": bool(body.animate),
            "tool_result": tool_result,
            "snapshot": snapshot,
        }

    @router.post("/jobs/{job_id}/animate")
    async def production_job_animate(
        job_id: str,
        user: dict = Depends(_agent_user),
    ):
        """Run i2v for short-form scenes that were explicitly approved for animation."""
        _ = user
        try:
            from studio_agent import tools as agent_tools

            tool_result = await run_in_threadpool(
                agent_tools.animate_production_scenes,
                job_id,
            )
            snapshot = agent_jobs.get_job_snapshot(job_id, "shortform")
        except Exception as exc:
            raise HTTPException(400, f"animation_failed: {exc}") from exc
        return {
            "ok": True,
            "job_id": job_id,
            "tool_result": tool_result,
            "snapshot": snapshot,
        }

    @router.post("/jobs/{job_id}/finalize")
    async def production_job_finalize(
        job_id: str,
        kind: str = Query("longform"),
        user: dict = Depends(_agent_user),
    ):
        """Agent subscribers can finalize approved productions after the relevant review gate."""
        import time as _time

        normalized_kind = str(kind or "longform").strip().lower()
        if normalized_kind == "shortform":
            try:
                from studio_agent import tools as agent_tools

                raw = await run_in_threadpool(agent_tools.finalize_production, job_id)
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
                "active_jobs": [] if snapshot.get("status") == "complete" else [{
                    "job_id": job_id,
                    "kind": "shortform",
                    "title": str(snapshot.get("title") or "Short-form finalize"),
                    "started_at": _time.time(),
                }],
                "poll_url": f"/api/studio-agent/jobs/{job_id}?kind=shortform",
            }

        try:
            out = agent_jobs.finalize_longform_job(job_id)
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
        """Firefox-safe STT: browser records audio, server runs xAI Grok STT."""
        raw = await audio.read()
        try:
            text = await transcribe_audio_bytes(
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
        return {"text": text, "chars": len(text)}

    @router.post("/sessions/{session_id}/attachments/video")
    async def upload_video_attachment(
        session_id: str,
        file: UploadFile = File(...),
        user: dict = Depends(_agent_user),
    ):
        """Persist an internal Studio Agent video attachment for ClipLab ingestion."""
        if not is_owner(user, is_admin_check):
            raise HTTPException(403, "ClipLab video attachments are internal beta only")
        uid = _user_id(user)
        session = store.get_session(session_id, user_id=uid)
        if not session:
            raise HTTPException(404, "session not found")
        if not file or not file.filename:
            raise HTTPException(400, "No video file")
        ext = Path(file.filename).suffix.lower() or ".mp4"
        if ext not in {".mp4", ".mov", ".mkv", ".webm", ".m4v"}:
            raise HTTPException(400, "Unsupported video format")
        try:
            from studio_agent.tools import SKELETON_OUTPUT
        except Exception:
            SKELETON_OUTPUT = Path(os.getenv("SKELETON_AI_OUTPUT_ROOT", "skeleton_ai/output"))
        safe_session = "".join(c for c in str(session_id or "") if c.isalnum() or c in "-_")[:80]
        target = SKELETON_OUTPUT / "_session_inputs" / safe_session
        target.mkdir(parents=True, exist_ok=True)
        stamp = int(time.time() * 1000)
        dest = target / f"agent_video_{stamp}{ext}"
        size = 0
        with dest.open("wb") as fh:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                fh.write(chunk)
        existing = [str(p) for p in list(session.get("latest_attachment_paths") or []) if p]
        next_paths = [*existing, str(dest.resolve())][-8:]
        store.update_session(
            session_id,
            latest_attachment_paths=next_paths,
            latest_attachment_at=time.time(),
        )
        return {
            "ok": True,
            "name": str(file.filename or dest.name),
            "mime_type": str(file.content_type or "video/mp4"),
            "size": size,
            "path": str(dest.resolve()),
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
        try:
            openrouter.api_key()
        except RuntimeError as exc:
            raise HTTPException(503, str(exc)) from exc
        session = store.create_session(
            user_id=_user_id(user),
            model=body.model or openrouter.DEFAULT_MODEL,
            approval_mode=body.approval_mode,
            content_format=body.content_format,
            reasoning_depth=body.reasoning_depth,
            render_style=body.render_style or store.DEFAULT_RENDER_STYLE,
            image_model=body.image_model,
            video_model=body.video_model,
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

    @router.get("/style-preview/{key}")
    async def style_preview(key: str):
        """Serve (generate on-demand if missing) a single cheap hero preview still for the visual style grid.
        Uses Seedream v4.5 / edit so every style has a distinct visual thumbnail (like the reference grids).
        Skeleton uses the glass anatomical look. Extremely cheap (one image per style).
        """
        from studio_agent.render_styles import get_style_preview_path
        from fastapi.responses import FileResponse

        timeout_sec = float(os.getenv("STYLE_PREVIEW_STILL_TIMEOUT_SEC", "150") or "150")
        try:
            p = await asyncio.wait_for(
                run_in_threadpool(get_style_preview_path, key),
                timeout=timeout_sec,
            )
        except asyncio.TimeoutError:
            raise HTTPException(504, f"style preview still generation timed out for {key}")
        return FileResponse(str(p), media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})

    @router.get("/style-preview/{key}/video")
    async def style_preview_video(key: str):
        """Serve (generate on-demand if missing) a short i2v motion preview for one visual style.

        This route is public like the still preview because browser media tags do
        not send bearer headers. It only exposes generated demo assets, not user
        content.
        """
        from studio_agent.render_styles import get_style_preview_video_path
        from fastapi.responses import FileResponse

        timeout_sec = float(os.getenv("STYLE_PREVIEW_VIDEO_TIMEOUT_SEC", "600") or "600")
        try:
            p = await asyncio.wait_for(
                run_in_threadpool(get_style_preview_video_path, key),
                timeout=timeout_sec,
            )
        except asyncio.TimeoutError:
            raise HTTPException(504, f"style preview video generation timed out for {key}")
        return FileResponse(str(p), media_type="video/mp4", headers={"Cache-Control": "public, max-age=86400"})

    @router.post("/sessions/{session_id}/rollover")
    async def rollover_session(session_id: str, user: dict = Depends(_agent_user)):
        """Copy transcript, pending actions, and active jobs into a fresh session."""
        uid = _user_id(user)
        session = store.rollover_session(session_id, user_id=uid)
        if not session:
            raise HTTPException(404, "session not found")
        return {"session": _public_session(session), "rolled_over_from": session_id}

    @router.get("/sessions/{session_id}")
    async def get_session(
        session_id: str,
        sync_pending: bool = Query(True),
        user: dict = Depends(_agent_user),
    ):
        session = store.get_session(session_id, user_id=_user_id(user))
        if not session:
            raise HTTPException(404, "session not found")
        # Pending production approvals are turn-local safety gates. Always
        # validate them before returning a session, even when the caller is
        # only trying to hydrate cached UI state.
        store.sync_pending_from_messages(session_id)
        session = store.get_session(session_id, user_id=_user_id(user)) or session
        return {"session": _public_session(session)}

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
            updates["model"] = body.model
        if body.approval_mode is not None:
            updates["approval_mode"] = body.approval_mode
        if body.content_format is not None:
            updates["content_format"] = body.content_format
        if body.reasoning_depth is not None:
            updates["reasoning_depth"] = body.reasoning_depth
        if body.render_style is not None:
            updates["render_style"] = body.render_style
        if body.image_model is not None:
            updates["image_model"] = body.image_model
        if body.video_model is not None:
            updates["video_model"] = body.video_model
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
        session = store.get_session(session_id, user_id=_user_id(user))
        if not session:
            raise HTTPException(404, "session not found")
        session = _apply_chat_turn_options(session_id, session, body)
        plan = _membership_plan_for_user(user)
        profile = _billing_profile(user)
        try:
            openrouter.api_key()
        except RuntimeError as exc:
            raise HTTPException(503, str(exc)) from exc
        run = store.create_run(session_id, user_text=body.message.strip())
        store.append_run_event(
            session_id,
            run["run_id"],
            "status",
            {"event": "status", "message": "Run accepted.", "run_id": run["run_id"]},
        )

        async def body_iter():
            yield f"event: status\ndata: {{\"event\":\"status\",\"message\":\"Run accepted.\",\"run_id\":\"{run['run_id']}\"}}\n\n"
            async for chunk in runner.stream_turn(
                session,
                body.message.strip(),
                membership_plan=plan,
                billing_profile=profile,
                reply_to=body.reply_to,
                attachments=body.attachments,
                run_id=run["run_id"],
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
            raise HTTPException(500, str(exc)) from exc

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
        store.update_session(session_id, messages=messages)
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
        "content_format": session.get("content_format"),
        "reasoning_depth": session.get("reasoning_depth") or "balanced",
        "render_style": session.get("render_style") or store.DEFAULT_RENDER_STYLE,
        "image_model": store.normalize_image_model(session.get("image_model")),
        "video_model": store.normalize_video_model(session.get("video_model")),
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
    }


def _public_session(session: dict[str, Any]) -> dict[str, Any]:
    return {
        **_session_summary(session),
        "approval_mode": session.get("approval_mode"),
        "pending_actions": session.get("pending_actions") or [],
        "active_jobs": session.get("active_jobs") or [],
        "active_runs": store.active_runs(session),
        "messages": session.get("messages") or [],
    }
