"""
FastAPI router for NYPTID Studio Agent (OpenRouter + Rookcast skills).

  POST   /api/studio-agent/sessions
  GET    /api/studio-agent/sessions/{id}
  PATCH  /api/studio-agent/sessions/{id}
  POST   /api/studio-agent/sessions/{id}/chat
  POST   /api/studio-agent/sessions/{id}/approve
  POST   /api/studio-agent/sessions/{id}/reject
  GET    /api/studio-agent/models
  GET    /api/studio-agent/skills
"""
from __future__ import annotations

from typing import Any, Callable, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from studio_agent import openrouter, skills
from studio_agent import runner, store


def _user_id(user: dict) -> str:
    return str((user or {}).get("id") or (user or {}).get("user_id") or "")


class CreateSessionRequest(BaseModel):
    model: str | None = None
    approval_mode: Literal["auto", "confirm"] = "confirm"
    content_format: Literal["short", "long", "both"] = "both"


class PatchSessionRequest(BaseModel):
    model: str | None = None
    approval_mode: Literal["auto", "confirm"] | None = None
    content_format: Literal["short", "long", "both"] | None = None


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=32000)


class ApproveRequest(BaseModel):
    action_id: str


class RejectRequest(BaseModel):
    action_id: str
    reason: str = ""


def build_studio_agent_router(
    *,
    require_auth: Callable,
    is_admin_check: Callable[[dict], bool] | None = None,
) -> APIRouter:
    router = APIRouter(prefix="/api/studio-agent", tags=["studio-agent"])

    def _admin(user: dict = Depends(require_auth)) -> dict:
        if is_admin_check and not is_admin_check(user):
            raise HTTPException(403, "Studio Agent is admin-only during beta")
        return user

    @router.get("/models")
    async def list_models(user: dict = Depends(_admin)):
        try:
            models = await openrouter.list_models()
            ids = [m.get("id") for m in models if m.get("id")]
            recommended = [m for m in openrouter.RECOMMENDED_MODELS if m in ids]
            extra = [i for i in ids if i in openrouter.RECOMMENDED_MODELS] or openrouter.RECOMMENDED_MODELS
            return {"recommended": recommended or extra, "count": len(ids)}
        except Exception as exc:
            return {
                "recommended": openrouter.RECOMMENDED_MODELS,
                "error": str(exc),
            }

    @router.get("/skills")
    async def list_skills(user: dict = Depends(_admin)):
        return {"skills": skills.list_skill_slugs(), "count": len(skills.list_skill_slugs())}

    @router.post("/sessions")
    async def create_session(body: CreateSessionRequest, user: dict = Depends(_admin)):
        try:
            openrouter.api_key()
        except RuntimeError as exc:
            raise HTTPException(503, str(exc)) from exc
        session = store.create_session(
            user_id=_user_id(user),
            model=body.model or openrouter.DEFAULT_MODEL,
            approval_mode=body.approval_mode,
            content_format=body.content_format,
        )
        return {"session": _public_session(session)}

    @router.get("/sessions/{session_id}")
    async def get_session(session_id: str, user: dict = Depends(_admin)):
        session = store.get_session(session_id, user_id=_user_id(user))
        if not session:
            raise HTTPException(404, "session not found")
        return {"session": _public_session(session)}

    @router.patch("/sessions/{session_id}")
    async def patch_session(
        session_id: str,
        body: PatchSessionRequest,
        user: dict = Depends(_admin),
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
        session = store.update_session(session_id, **updates)
        return {"session": _public_session(session)}

    @router.post("/sessions/{session_id}/chat")
    async def chat(session_id: str, body: ChatRequest, user: dict = Depends(_admin)):
        session = store.get_session(session_id, user_id=_user_id(user))
        if not session:
            raise HTTPException(404, "session not found")
        try:
            result = await runner.run_turn(session, body.message.strip())
        except RuntimeError as exc:
            raise HTTPException(502, str(exc)) from exc
        return result

    @router.post("/sessions/{session_id}/approve")
    async def approve(session_id: str, body: ApproveRequest, user: dict = Depends(_admin)):
        session = store.get_session(session_id, user_id=_user_id(user))
        if not session:
            raise HTTPException(404, "session not found")
        try:
            return await runner.approve_action(session, body.action_id)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        except Exception as exc:
            raise HTTPException(500, str(exc)) from exc

    @router.post("/sessions/{session_id}/reject")
    async def reject(session_id: str, body: RejectRequest, user: dict = Depends(_admin)):
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


def _public_session(session: dict[str, Any]) -> dict[str, Any]:
    return {
        "session_id": session.get("session_id"),
        "model": session.get("model"),
        "approval_mode": session.get("approval_mode"),
        "content_format": session.get("content_format"),
        "pending_actions": session.get("pending_actions") or [],
        "messages": session.get("messages") or [],
        "updated_at": session.get("updated_at"),
    }
