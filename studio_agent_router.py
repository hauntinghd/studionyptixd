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
  POST   /api/studio-agent/dictation
"""
from __future__ import annotations

from typing import Any, Callable, Literal

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from studio_agent import jobs as agent_jobs

from studio_agent.access import account_profile, can_use_studio_agent, is_owner
from studio_agent.dictation import transcribe_audio_bytes

from studio_agent import openrouter, skills
from studio_agent import runner, store
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
    web_search: bool = True
    animate: bool = True


class PatchSessionRequest(BaseModel):
    model: str | None = None
    approval_mode: Literal["auto", "confirm"] | None = None
    content_format: Literal["short", "long", "both"] | None = None
    reasoning_depth: Literal["fast", "balanced", "deep"] | None = None
    render_style: str | None = None
    web_search: bool | None = None
    animate: bool | None = None


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=32000)
    reply_to: dict | None = None  # {job_id, kind} for re-editing specific previous video in same chat


class ApproveRequest(BaseModel):
    action_id: str


class RejectRequest(BaseModel):
    action_id: str
    reason: str = ""


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
                "Studio Agent requires an active Creator ($60/mo) or Studio ($200/mo) plan.",
            )
        return user

    def _billing_profile(user: dict) -> dict:
        return account_profile(user, is_admin_check=is_admin_check)

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

    @router.post("/jobs/{job_id}/finalize")
    async def production_job_finalize(
        job_id: str,
        user: dict = Depends(_agent_user),
    ):
        """Agent subscribers can finalize long-form after stills gate (no admin long-form tab required)."""
        import time as _time

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
        """Firefox-safe STT: browser records audio, server runs fal whisper."""
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
            web_search=body.web_search,
            animate=body.animate,
        )
        return {"session": _public_session(session)}

    @router.get("/render-styles")
    async def list_render_styles():
        from studio_agent.render_styles import DEFAULT_RENDER_STYLE, list_render_styles

        return {
            "default": DEFAULT_RENDER_STYLE,
            "styles": list_render_styles(),
        }

    @router.get("/style-preview/{key}")
    async def style_preview(key: str, user: dict = Depends(_agent_user)):
        """Serve (generate on-demand if missing) a single cheap hero preview still for the visual style grid.
        Uses Seedream v4.5 / edit so every style has a distinct visual thumbnail (like the reference grids).
        Skeleton uses the glass anatomical look. Extremely cheap (one image per style).
        """
        from studio_agent.render_styles import get_style_preview_path
        from fastapi.responses import FileResponse

        p = get_style_preview_path(key)
        return FileResponse(str(p), media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})

    @router.post("/sessions/{session_id}/rollover")
    async def rollover_session(session_id: str, user: dict = Depends(_agent_user)):
        """Copy transcript, pending actions, and active jobs into a fresh session."""
        uid = _user_id(user)
        session = store.rollover_session(session_id, user_id=uid)
        if not session:
            raise HTTPException(404, "session not found")
        return {"session": _public_session(session), "rolled_over_from": session_id}

    @router.get("/sessions/{session_id}")
    async def get_session(session_id: str, user: dict = Depends(_agent_user)):
        session = store.get_session(session_id, user_id=_user_id(user))
        if not session:
            raise HTTPException(404, "session not found")
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
        if body.web_search is not None:
            updates["web_search"] = bool(body.web_search)
        if body.animate is not None:
            updates["animate"] = bool(body.animate)
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
        plan = _membership_plan_for_user(user)
        profile = _billing_profile(user)
        try:
            result = await runner.run_turn(
                session,
                body.message.strip(),
                membership_plan=plan,
                billing_profile=profile,
                reply_to=getattr(body, 'reply_to', None),
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
        plan = _membership_plan_for_user(user)
        profile = _billing_profile(user)
        try:
            openrouter.api_key()
        except RuntimeError as exc:
            raise HTTPException(503, str(exc)) from exc

        async def body_iter():
            async for chunk in runner.stream_turn(
                session,
                body.message.strip(),
                membership_plan=plan,
                billing_profile=profile,
                reply_to=body.reply_to,
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

    @router.post("/sessions/{session_id}/approve")
    async def approve(session_id: str, body: ApproveRequest, user: dict = Depends(_agent_user)):
        session = store.get_session(session_id, user_id=_user_id(user))
        if not session:
            raise HTTPException(404, "session not found")
        plan = _membership_plan_for_user(user)
        profile = _billing_profile(user)
        try:
            return await runner.approve_action(
                session,
                body.action_id,
                membership_plan=plan,
                billing_profile=profile,
            )
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
        "web_search": bool(session.get("web_search", True)),
        "animate": bool(session.get("animate", True)),
        "message_count": len(session.get("messages") or []),
        "pending_count": len(session.get("pending_actions") or []),
        "created_at": session.get("created_at"),
        "updated_at": session.get("updated_at"),
    }


def _public_session(session: dict[str, Any]) -> dict[str, Any]:
    return {
        **_session_summary(session),
        "approval_mode": session.get("approval_mode"),
        "pending_actions": session.get("pending_actions") or [],
        "active_jobs": session.get("active_jobs") or [],
        "messages": session.get("messages") or [],
    }
