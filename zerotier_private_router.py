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
  POST /api/zerotier-private/script   Generate 8-beat script from a topic
                                       using the zerotier_private template
                                       prompt. Streamable via SSE.

Phase 2 follow-ups (not in this v1 module):
  - POST /api/zerotier-private/scenes   Generate stills for an approved script
  - POST /api/zerotier-private/render   Full pipeline → MP4
  - GET  /api/zerotier-private/jobs/{id}  Poll a render job
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
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from backend_script_prompts import TEMPLATE_SYSTEM_PROMPTS
from skeleton_ai.scripting_grok import GrokClient, GrokAuthError


class ZTScriptRequest(BaseModel):
    topic: str = Field(..., min_length=1, description="Candidate short title or topic seed")
    stream: bool = False


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

    return router
