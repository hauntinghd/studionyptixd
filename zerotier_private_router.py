"""
FastAPI router for the ZeroTier (Private) niche pipeline.

Owner-only endpoints that wrap the validated DC speedster fan-fic pipeline:
  - 8-scene Conflict Arc structure
  - "The Time Wally West [past-tense]" title formula
  - Modern DC comics cel-shaded visual style (seedream v4.5)
  - LTX 13B i2v with Pixverse V6 fallback (when LTX is degraded)
  - MiniMax `English_Trustworthy_Man` narration
  - Lowercase per-scene captions

Routes:
  POST /api/zerotier-private/script        Generate 8-beat script (Grok).
  POST /api/zerotier-private/render        Full pipeline → MP4. Synchronous;
                                            request stays open ~5-10 min.
  GET  /api/zerotier-private/jobs/{id}     Poll a past job's result.json.
  GET  /api/zerotier-private/jobs/{id}/mp4 Stream the rendered MP4.

Phase 2c+ follow-ups (not in this module yet):
  - POST /api/zerotier-private/score    Predict virality 0-100 for a topic
  - GET  /api/zerotier-private/history  Past predictions vs actual outcomes
                                         (the learning-loop visibility surface)

Wiring: backend.py builds + includes this router via:
    from zerotier_private_router import build_zerotier_private_router
    app.include_router(build_zerotier_private_router(
        require_auth=require_auth,
        is_admin_user=_is_admin_user,
    ))
"""
from __future__ import annotations
import json
import os
import re
import uuid
from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse, FileResponse
from pydantic import BaseModel, Field

from backend_script_prompts import TEMPLATE_SYSTEM_PROMPTS
from skeleton_ai.scripting_grok import GrokClient, GrokAuthError
from zerotier_private.pipeline import render_zerotier_short, ZTRenderError


class ZTScriptRequest(BaseModel):
    topic: str = Field(..., min_length=1, description="Candidate short title or topic seed")
    stream: bool = False


class ZTRenderRequest(BaseModel):
    script_json: str = Field(..., min_length=10, description="Grok-generated 8-beat script JSON (raw string)")
    final_filename: str | None = None


# Where rendered MP4s live. Mirrors skeleton_ai's pattern.
ZT_OUTPUT_ROOT = Path(os.getenv("ZEROTIER_PRIVATE_OUTPUT_ROOT", "zerotier_private/output"))


def _safe_filename(raw: str | None, default: str) -> str:
    if not raw:
        return default
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", str(raw)).strip("._-")
    if not s.lower().endswith(".mp4"):
        s = s + ".mp4"
    return s[:96] or default


def _build_zt_user_prompt(topic: str) -> str:
    """The user-side prompt — sets the topic + format expectations.

    The system prompt (TEMPLATE_SYSTEM_PROMPTS["zerotier_private"]) already
    encodes the Conflict Arc + title formula + lowercase caption rules + JSON
    output schema. The user prompt just delivers the topic.
    """
    return (
        f"Topic: {topic}\n\n"
        f"Build the 8-scene short following the structure above. Title MUST "
        f"follow the 'The Time Wally West [past-tense verb]' format. Last "
        f"scene MUST end with a single memorable line that invites a comment. "
        f"Output ONLY the JSON described in the system prompt — no markdown, "
        f"no commentary."
    )


def build_zerotier_private_router(
    require_auth: Callable[..., dict] | None = None,
    *,
    is_admin_user: Callable[[dict | None], bool] | None = None,
) -> APIRouter:
    """
    Build the ZeroTier (Private) router.

    require_auth   — FastAPI dep returning the authed user dict.
    is_admin_user  — bool predicate. If supplied, every route enforces admin.
                     If None (test/dev mode), admin gate is disabled.
    """
    router = APIRouter(prefix="/api/zerotier-private", tags=["zerotier-private"])

    auth_dep = Depends(require_auth) if require_auth else Depends(lambda: {"user_id": "anon"})

    def _gate_admin(user: dict | None) -> None:
        """Raise 403 if the user isn't an owner/admin. No-op when no
        is_admin_user was wired (dev/test)."""
        if is_admin_user is None:
            return
        if not is_admin_user(user):
            raise HTTPException(403, "ZeroTier (Private) is owner-only.")

    # ────────────────────────────────────────────────────────────────────
    # POST /script — generate the 8-beat Conflict Arc script
    # ────────────────────────────────────────────────────────────────────
    @router.post("/script")
    async def generate_zt_script(body: ZTScriptRequest, user: dict = auth_dep):
        _gate_admin(user)

        topic = (body.topic or "").strip()
        if not topic:
            raise HTTPException(400, "topic is required")

        system_prompt = TEMPLATE_SYSTEM_PROMPTS.get("zerotier_private", "").strip()
        if not system_prompt:
            raise HTTPException(503, "zerotier_private template prompt missing — backend deploy out of sync")

        try:
            grok = GrokClient()
        except GrokAuthError as e:
            raise HTTPException(503, f"grok_auth_failed: {e}")

        user_prompt = _build_zt_user_prompt(topic)

        if body.stream:
            def sse_generator():
                try:
                    for piece in grok.stream(system_prompt, user_prompt, max_tokens=2200, temperature=0.85):
                        # Escape newlines so SSE framing isn't broken on the
                        # client side. The frontend reverses this.
                        safe = piece.replace("\n", "\\n")
                        yield f"data: {safe}\n\n"
                    yield "data: [DONE]\n\n"
                except GrokAuthError as e:
                    yield f"event: error\ndata: {e}\n\n"
                except Exception as e:
                    yield f"event: error\ndata: {type(e).__name__}: {e}\n\n"
            return StreamingResponse(sse_generator(), media_type="text/event-stream")

        try:
            text = grok.complete(system_prompt, user_prompt, max_tokens=2200, temperature=0.85)
        except GrokAuthError as e:
            raise HTTPException(503, f"grok_auth_failed: {e}")
        return {
            "script_json": text,
            "topic": topic,
        }

    # ────────────────────────────────────────────────────────────────────
    # POST /render — synchronous render. Request stays open ~5-10 min.
    # Phase 2b ships sync; Phase 2b.5 will add a background-job wrapper.
    # ────────────────────────────────────────────────────────────────────
    @router.post("/render")
    async def render_zt_short(body: ZTRenderRequest, user: dict = auth_dep):
        _gate_admin(user)

        # Validate the script JSON parses + has scenes before kicking off
        # any fal calls (cheap fail-fast).
        try:
            parsed = json.loads(body.script_json)
        except Exception as e:
            raise HTTPException(400, f"script_json must be valid JSON: {e}")
        if not isinstance(parsed, dict) or not parsed.get("scenes"):
            raise HTTPException(400, "script_json must be an object with a non-empty 'scenes' array")

        job_id = uuid.uuid4().hex[:12]
        workspace = ZT_OUTPUT_ROOT / job_id
        workspace.mkdir(parents=True, exist_ok=True)
        # Persist the input script for replay/debugging.
        try:
            (workspace / "script.json").write_text(body.script_json, encoding="utf-8")
        except Exception:
            pass

        final_filename = _safe_filename(
            body.final_filename or f"ZeroTier_{job_id}.mp4",
            default=f"ZeroTier_{job_id}.mp4",
        )

        try:
            result = render_zerotier_short(
                script_json=parsed,
                workspace=workspace,
                final_filename=final_filename,
            )
        except ZTRenderError as e:
            raise HTTPException(500, f"render_failed: {e}")
        except Exception as e:
            raise HTTPException(500, f"render_failed: {type(e).__name__}: {e}")

        # Persist a result.json for /jobs/{id} retrieval.
        result_payload = {
            "job_id": job_id,
            "status": "done",
            **result,
        }
        try:
            (workspace / "result.json").write_text(
                json.dumps(result_payload, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

        # Frontend uses the relative URL to download the MP4.
        result_payload["mp4_url"] = f"/api/zerotier-private/jobs/{job_id}/mp4"
        return result_payload

    @router.get("/jobs/{job_id}")
    async def zt_job_status(job_id: str, user: dict = auth_dep):
        _gate_admin(user)
        if not job_id.replace("_", "").isalnum() or len(job_id) > 32:
            raise HTTPException(400, "bad_job_id")
        result_path = ZT_OUTPUT_ROOT / job_id / "result.json"
        if not result_path.exists():
            raise HTTPException(404, "not_found")
        try:
            return json.loads(result_path.read_text(encoding="utf-8"))
        except Exception as e:
            raise HTTPException(500, f"result_parse_failed: {e}")

    @router.get("/jobs/{job_id}/mp4")
    async def zt_job_mp4(job_id: str, user: dict = auth_dep):
        _gate_admin(user)
        if not job_id.replace("_", "").isalnum() or len(job_id) > 32:
            raise HTTPException(400, "bad_job_id")
        # The MP4 filename comes from result.json's "mp4_path" — but to keep
        # this endpoint simple, scan the workspace for any .mp4 at the root.
        ws = ZT_OUTPUT_ROOT / job_id
        if not ws.exists():
            raise HTTPException(404, "job_not_found")
        for candidate in ws.glob("*.mp4"):
            if candidate.is_file() and candidate.stat().st_size > 1024:
                return FileResponse(
                    str(candidate),
                    media_type="video/mp4",
                    filename=candidate.name,
                )
        raise HTTPException(404, "mp4_not_found")

    return router
