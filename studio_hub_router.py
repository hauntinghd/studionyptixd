from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field


log = logging.getLogger("studio.hub")


DEFAULT_HUB_STATE: dict[str, Any] = {
    "profile": {
        "display_name": "",
        "bio": "",
        "timezone": "America/New_York",
        "company": "",
        "website": "",
        "avatar_url": "",
    },
    "channels": ["Empire Magnates", "ZeroTier", "NYPTID Clips"],
    "roles": [
        {"name": "Owner", "access": "All studios, billing, roles, refunds"},
        {"name": "Producer", "access": "Studio Agent, packaging, render QA"},
        {"name": "Editor", "access": "ClipLab, Render QA, downloads"},
        {"name": "Analyst", "access": "Wins, analytics, Catalyst read-only"},
    ],
    "checklist": [
        {"id": "connect-channels", "label": "Connect channels", "done": True},
        {"id": "choose-studio", "label": "Choose a studio", "done": False},
        {"id": "open-agent", "label": "Open Studio Agent", "done": False},
        {"id": "approve-packaging", "label": "Approve packaging", "done": False},
        {"id": "ship-production", "label": "Ship production", "done": False},
        {"id": "review-results", "label": "Review results", "done": False},
    ],
    "network_messages": [
        {
            "id": "welcome",
            "name": "Studio",
            "body": "Use Network for updates, wins, questions, feedback, and operator collaboration.",
            "created_at": "2026-06-09T00:00:00+00:00",
        }
    ],
    "wins": [],
}


class HubStatePatch(BaseModel):
    profile: dict[str, Any] | None = None
    channels: list[str] | None = None
    roles: list[dict[str, Any]] | None = None
    checklist: list[dict[str, Any]] | None = None
    network_messages: list[dict[str, Any]] | None = None
    wins: list[dict[str, Any]] | None = None


class HubMessageRequest(BaseModel):
    body: str = Field(..., min_length=1, max_length=2000)
    name: str = Field(default="Operator", max_length=80)


class HubWinRequest(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    body: str = Field(default="", max_length=1000)
    image_url: str = Field(..., min_length=1)
    image_name: str = Field(default="", max_length=200)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _deep_default_state(user: dict | None = None) -> dict[str, Any]:
    state = json.loads(json.dumps(DEFAULT_HUB_STATE))
    email = str((user or {}).get("email", "") or "").strip()
    if email and not state["profile"].get("display_name"):
        state["profile"]["display_name"] = email.split("@", 1)[0]
    return state


def _normalize_state(raw: Any, user: dict | None = None) -> dict[str, Any]:
    state = _deep_default_state(user)
    if not isinstance(raw, dict):
        return state
    for key in ("profile",):
        if isinstance(raw.get(key), dict):
            state[key].update(raw[key])
    for key in ("channels", "roles", "checklist", "network_messages", "wins"):
        if isinstance(raw.get(key), list):
            state[key] = raw[key]
    return state


def _safe_user_key(user: dict) -> str:
    user_id = str(user.get("id", "") or "").strip()
    if not user_id:
        raise HTTPException(401, "Authentication required.")
    return f"studio_hub_state:{user_id}"


def build_studio_hub_router(
    *,
    require_auth: Callable,
    supabase_url: str,
    supabase_service_key: str,
    supabase_anon_key: str,
    local_store_dir: Path,
) -> APIRouter:
    router = APIRouter(prefix="/api/studio-hub", tags=["studio-hub"])
    svc_key = str(supabase_service_key or supabase_anon_key or "").strip()
    table = "app_settings"
    local_store_dir.mkdir(parents=True, exist_ok=True)

    def _local_path(user: dict) -> Path:
        safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in str(user.get("id", "") or "anonymous"))
        return local_store_dir / f"{safe}.json"

    async def _read_state(user: dict) -> dict[str, Any]:
        key = _safe_user_key(user)
        if supabase_url and svc_key:
            try:
                async with httpx.AsyncClient(timeout=12) as client:
                    resp = await client.get(
                        f"{supabase_url}/rest/v1/{table}?key=eq.{quote(key)}&select=value&limit=1",
                        headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
                    )
                if resp.status_code == 200:
                    rows = resp.json()
                    if isinstance(rows, list) and rows:
                        return _normalize_state(rows[0].get("value"), user)
                elif resp.status_code not in {404}:
                    log.warning("Studio Hub state read failed: %s %s", resp.status_code, resp.text[:240])
            except Exception as exc:
                log.warning("Studio Hub state read exception: %s", exc)
        path = _local_path(user)
        if path.exists():
            try:
                return _normalize_state(json.loads(path.read_text(encoding="utf-8")), user)
            except Exception:
                return _deep_default_state(user)
        return _deep_default_state(user)

    async def _write_state(user: dict, state: dict[str, Any]) -> dict[str, Any]:
        key = _safe_user_key(user)
        state = _normalize_state(state, user)
        state["updated_at"] = _now_iso()
        wrote_remote = False
        if supabase_url and svc_key:
            try:
                async with httpx.AsyncClient(timeout=12) as client:
                    existing = await client.get(
                        f"{supabase_url}/rest/v1/{table}?key=eq.{quote(key)}&select=id&limit=1",
                        headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
                    )
                    rows = existing.json() if existing.status_code == 200 else []
                    rows = rows if isinstance(rows, list) else []
                    if rows:
                        resp = await client.patch(
                            f"{supabase_url}/rest/v1/{table}?key=eq.{quote(key)}",
                            headers={
                                "apikey": svc_key,
                                "Authorization": f"Bearer {svc_key}",
                                "Content-Type": "application/json",
                                "Prefer": "return=minimal",
                            },
                            json={"value": state},
                        )
                    else:
                        resp = await client.post(
                            f"{supabase_url}/rest/v1/{table}",
                            headers={
                                "apikey": svc_key,
                                "Authorization": f"Bearer {svc_key}",
                                "Content-Type": "application/json",
                                "Prefer": "return=minimal",
                            },
                            json={"key": key, "value": state},
                        )
                    wrote_remote = resp.status_code in {200, 201, 204}
                    if not wrote_remote:
                        log.warning("Studio Hub state write failed: %s %s", resp.status_code, resp.text[:240])
            except Exception as exc:
                log.warning("Studio Hub state write exception: %s", exc)
        try:
            _local_path(user).write_text(json.dumps(state, indent=2), encoding="utf-8")
        except Exception as exc:
            log.warning("Studio Hub local state write failed: %s", exc)
        return {"state": state, "remote": wrote_remote}

    @router.get("/state")
    async def get_state(user: dict = Depends(require_auth)):
        return {"state": await _read_state(user)}

    @router.patch("/state")
    async def patch_state(body: HubStatePatch, user: dict = Depends(require_auth)):
        state = await _read_state(user)
        patch = body.model_dump(exclude_none=True) if hasattr(body, "model_dump") else body.dict(exclude_none=True)
        for key, value in patch.items():
            if key == "profile" and isinstance(value, dict):
                state["profile"] = {**dict(state.get("profile") or {}), **value}
            else:
                state[key] = value
        return await _write_state(user, state)

    @router.post("/network/messages")
    async def add_message(body: HubMessageRequest, user: dict = Depends(require_auth)):
        state = await _read_state(user)
        message = {
            "id": f"msg-{int(datetime.now(timezone.utc).timestamp() * 1000)}",
            "name": body.name.strip() or "Operator",
            "body": body.body.strip(),
            "created_at": _now_iso(),
        }
        state["network_messages"] = [*list(state.get("network_messages") or []), message][-100:]
        result = await _write_state(user, state)
        result["message"] = message
        return result

    @router.post("/wins")
    async def add_win(body: HubWinRequest, user: dict = Depends(require_auth)):
        state = await _read_state(user)
        win = {
            "id": f"win-{int(datetime.now(timezone.utc).timestamp() * 1000)}",
            "title": body.title.strip(),
            "body": body.body.strip(),
            "image_url": body.image_url.strip(),
            "image_name": body.image_name.strip(),
            "created_at": _now_iso(),
        }
        state["wins"] = [win, *list(state.get("wins") or [])][:100]
        result = await _write_state(user, state)
        result["win"] = win
        return result

    return router
