"""
FastAPI router for Catalyst Reference Videos.

Wires into backend.py:
    from catalyst_references_router import build_catalyst_references_router
    app.include_router(build_catalyst_references_router(require_auth=require_auth))

Routes:
  POST   /api/catalyst/references             body: {url, channel_key?, notes?}
  GET    /api/catalyst/references?channel_key=X
  PATCH  /api/catalyst/references/{ref_id}    body: {notes?, channel_key?}
  DELETE /api/catalyst/references/{ref_id}
"""
from __future__ import annotations

from typing import Callable

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from catalyst_references import (
    ingest_reference_video,
    list_user_references,
    delete_reference_video,
    update_reference_notes,
    ReferenceError,
)
from studio_agent.direct_production import claim_direct_production


class AddReferenceRequest(BaseModel):
    url: str
    channel_key: str = Field(default="")
    notes: str = Field(default="")


class UpdateReferenceRequest(BaseModel):
    notes: str | None = None
    channel_key: str | None = None


def build_catalyst_references_router(
    require_auth: Callable[..., dict] | None = None,
) -> APIRouter:
    """Build the Catalyst References router. Auth-gated (any signed-in user
    can manage their own library — RLS enforces ownership at the DB level)."""
    router = APIRouter(prefix="/api/catalyst/references", tags=["catalyst-references"])
    auth_dep = Depends(require_auth) if require_auth else Depends(lambda: {"user_id": "anon"})

    def _user_id(user: dict) -> str:
        uid = str((user or {}).get("id", "") or (user or {}).get("user_id", "") or "")
        if not uid:
            raise HTTPException(401, "user_id missing on auth payload")
        return uid

    @router.post("")
    async def add_reference(body: AddReferenceRequest, request: Request, user: dict = auth_dep):
        if not body.url or not body.url.strip():
            raise HTTPException(400, "url is required")
        user_id = _user_id(user)
        arguments = {
            "url": body.url.strip(),
            "channel_key": (body.channel_key or "").strip(),
            "notes": (body.notes or "").strip(),
        }
        with claim_direct_production(
            "catalyst_add_reference",
            arguments,
            request=request,
            user_id=user_id,
            content_format="catalyst",
        ) as command:
            if command.replay is not None:
                return dict(command.replay)
            try:
                row = ingest_reference_video(user_id=user_id, **arguments)
            except ReferenceError as e:
                raise HTTPException(400, f"reference_failed: {e}")
            return command.complete({"reference": row})

    @router.get("")
    async def list_references(channel_key: str | None = None, user: dict = auth_dep):
        try:
            rows = list_user_references(_user_id(user), channel_key=channel_key)
        except ReferenceError as e:
            raise HTTPException(503, f"reference_list_failed: {e}")
        return {"references": rows, "total": len(rows)}

    @router.patch("/{ref_id}")
    async def update_reference(
        ref_id: str,
        body: UpdateReferenceRequest,
        request: Request,
        user: dict = auth_dep,
    ):
        if not ref_id or not _safe_id(ref_id):
            raise HTTPException(400, "bad ref_id")
        user_id = _user_id(user)
        arguments = {
            "ref_id": ref_id,
            "notes": body.notes or "",
            "channel_key": body.channel_key,
        }
        with claim_direct_production(
            "catalyst_update_reference",
            arguments,
            request=request,
            user_id=user_id,
            content_format="catalyst",
        ) as command:
            if command.replay is not None:
                return dict(command.replay)
            updated = update_reference_notes(user_id=user_id, **arguments)
            if updated is None:
                raise HTTPException(404, "not_found")
            return command.complete({"reference": updated})

    @router.delete("/{ref_id}")
    async def delete_reference(ref_id: str, request: Request, user: dict = auth_dep):
        if not ref_id or not _safe_id(ref_id):
            raise HTTPException(400, "bad ref_id")
        user_id = _user_id(user)
        arguments = {"ref_id": ref_id}
        with claim_direct_production(
            "catalyst_delete_reference",
            arguments,
            request=request,
            user_id=user_id,
            content_format="catalyst",
        ) as command:
            if command.replay is not None:
                return dict(command.replay)
            ok = delete_reference_video(user_id=user_id, ref_id=ref_id)
            if not ok:
                raise HTTPException(404, "not_found")
            return command.complete({"deleted": True, "id": ref_id})

    return router


def _safe_id(s: str) -> bool:
    """uuid pattern: 8-4-4-4-12 hex with dashes."""
    if not s or len(s) != 36:
        return False
    parts = s.split("-")
    if len(parts) != 5 or [len(p) for p in parts] != [8, 4, 4, 4, 12]:
        return False
    return all(c in "0123456789abcdefABCDEF" for c in s.replace("-", ""))
