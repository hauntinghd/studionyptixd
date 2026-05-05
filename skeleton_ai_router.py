"""
FastAPI router for the Skeleton AI short-form generation API.

Wires into backend.py via:
    from skeleton_ai_router import build_skeleton_ai_router
    app.include_router(build_skeleton_ai_router(require_auth=require_auth))

Routes:
  GET  /api/skeleton-ai/categories         List the 4 idea categories
  GET  /api/skeleton-ai/voices              List ElevenLabs voices for picker
  GET  /api/skeleton-ai/pricing             Tier table for pricing UI
  POST /api/skeleton-ai/script              Generate script with Grok (streamable)
  POST /api/skeleton-ai/generate            Run full pipeline → mp4
  GET  /api/skeleton-ai/jobs/{id}           Poll a generation job
"""
from __future__ import annotations
import os
import uuid
import json
from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field

from skeleton_ai.prompts.idea_lists import list_categories, get_category
from skeleton_ai.scripting_grok import GrokClient, GrokAuthError, build_script_prompt
from skeleton_ai.voice_elevenlabs import ElevenLabsClient, ElevenLabsAuthError
from skeleton_ai.pipeline import run as run_pipeline
from skeleton_ai.i2v_engine import AC_COST_STANDARD, AC_COST_PREMIUM


OUTPUT_ROOT = Path(os.getenv("SKELETON_AI_OUTPUT_ROOT", "skeleton_ai/output"))


class ScriptRequest(BaseModel):
    category: str = Field(default="human_limits")
    topic: str | None = None
    stream: bool = False


class GenerateRequest(BaseModel):
    category: str = Field(default="human_limits")
    topic: str | None = None
    tier: str = Field(default="standard")  # "standard" | "premium"
    voice_id: str | None = None
    script_override: str | None = None  # if user edited the script in the textarea


def build_skeleton_ai_router(
    require_auth: Callable[..., dict] | None = None,
) -> APIRouter:
    """
    Build the Skeleton AI router. require_auth is a FastAPI dependency that
    returns the authed user dict (matches the existing backend.py pattern).
    """
    router = APIRouter(prefix="/api/skeleton-ai", tags=["skeleton-ai"])

    auth_dep = Depends(require_auth) if require_auth else Depends(lambda: {"user_id": "anon"})

    # ──────────────────────────────────────────────────────────────────────
    # Read-only metadata endpoints
    # ──────────────────────────────────────────────────────────────────────

    @router.get("/categories")
    async def categories():
        return {"categories": list_categories()}

    @router.get("/pricing")
    async def pricing():
        return {
            "tiers": [
                {"key": "free",    "name": "Free",     "price_usd": 0,   "ac": 0,   "shorts": 1,
                 "features": ["1 demo short with watermark", "Standard quality only"]},
                {"key": "starter", "name": "Starter",  "price_usd": 19,  "ac": 25,  "shorts": 5,
                 "features": ["No watermark", "All voices", "1 voice clone"]},
                {"key": "creator", "name": "Creator",  "price_usd": 39,  "ac": 75,  "shorts": 15,
                 "features": ["3 voice clones", "Premium quality upgrade", "Most popular"]},
                {"key": "pro",     "name": "Pro",      "price_usd": 79,  "ac": 200, "shorts": 40,
                 "features": ["10 voice clones", "Auto-post", "Priority queue"]},
                {"key": "studio",  "name": "Studio",   "price_usd": 179, "ac": 500, "shorts": 100,
                 "features": ["25 voice clones", "API access", "White-label"]},
            ],
            "ac_per_short": {
                "standard": AC_COST_STANDARD,
                "premium":  AC_COST_PREMIUM,
            },
            "overage_packs": [
                {"ac": 50,  "price_usd": 20},
                {"ac": 200, "price_usd": 69},
            ],
        }

    @router.get("/voices")
    async def voices(_user: dict = auth_dep):
        try:
            el = ElevenLabsClient()
            vs = el.list_voices()
        except ElevenLabsAuthError as e:
            raise HTTPException(503, f"elevenlabs_auth_failed: {e}")
        return {
            "voices": [
                {
                    "voice_id": v["voice_id"],
                    "name": v["name"],
                    "category": v.get("category"),
                    "preview_url": v.get("preview_url"),
                    "labels": v.get("labels", {}),
                }
                for v in vs
            ]
        }

    # ──────────────────────────────────────────────────────────────────────
    # Script generation (Grok 4.1 Fast Reasoning)
    # ──────────────────────────────────────────────────────────────────────

    @router.post("/script")
    async def generate_script(body: ScriptRequest, _user: dict = auth_dep):
        try:
            cat = get_category(body.category)
            grok = GrokClient()
        except (ValueError, GrokAuthError) as e:
            raise HTTPException(503, f"config: {e}")

        user_prompt = build_script_prompt(cat["system_prompt"], body.topic)

        if body.stream:
            def sse_generator():
                try:
                    for piece in grok.stream(cat["system_prompt"], user_prompt, max_tokens=1500):
                        # Escape newlines so SSE framing isn't broken.
                        safe = piece.replace("\n", "\\n")
                        yield f"data: {safe}\n\n"
                    yield "data: [DONE]\n\n"
                except GrokAuthError as e:
                    yield f"event: error\ndata: {e}\n\n"
            return StreamingResponse(sse_generator(), media_type="text/event-stream")

        text = grok.complete(cat["system_prompt"], user_prompt, max_tokens=1500)
        return {"script": text}

    # ──────────────────────────────────────────────────────────────────────
    # Full pipeline (synchronous v0; queue later)
    # ──────────────────────────────────────────────────────────────────────

    @router.post("/generate")
    async def generate(body: GenerateRequest, user: dict = auth_dep):
        if body.tier not in ("standard", "premium"):
            raise HTTPException(400, "tier must be standard or premium")

        # TODO(billing): deduct AC here BEFORE running. If user lacks credits, 402.
        # ac_required = AC_COST_PREMIUM if body.tier == "premium" else AC_COST_STANDARD
        # if not billing.deduct(user.get("user_id"), ac_required):
        #     raise HTTPException(402, "insufficient_credits")

        job_id = uuid.uuid4().hex[:12]
        workspace = OUTPUT_ROOT / job_id
        try:
            result = run_pipeline(
                category_key=body.category,
                topic=body.topic,
                workspace=workspace,
                tier=body.tier,
                voice_id=body.voice_id,
            )
        except (GrokAuthError, ElevenLabsAuthError) as e:
            raise HTTPException(503, f"upstream_auth: {e}")
        except Exception as e:
            raise HTTPException(500, f"pipeline_failed: {e}")

        return {"job_id": job_id, **result}

    @router.get("/jobs/{job_id}")
    async def job_status(job_id: str, _user: dict = auth_dep):
        if not job_id.isalnum() or len(job_id) > 32:
            raise HTTPException(400, "bad_job_id")
        result_path = OUTPUT_ROOT / job_id / "result.json"
        if not result_path.exists():
            raise HTTPException(404, "not_found")
        return {"job_id": job_id, "result": json.loads(result_path.read_text())}

    return router
