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
from skeleton_ai.prompts.base_style import assemble_scene_prompt, NEG_STILL
from skeleton_ai.scripting_grok import GrokClient, GrokAuthError, build_script_prompt
from skeleton_ai.voice_elevenlabs import ElevenLabsClient, ElevenLabsAuthError
from skeleton_ai.pipeline import (
    run as run_pipeline,
    analyze_script,
    derive_beat_visuals,
    split_script_into_beats,
)
from skeleton_ai.stills_engine import (
    generate as gen_still_for_model,
    MODEL_ENDPOINTS,
    StillsError,
)
from skeleton_ai.i2v_engine import AC_COST_STANDARD, AC_COST_PREMIUM


OUTPUT_ROOT = Path(os.getenv("SKELETON_AI_OUTPUT_ROOT", "skeleton_ai/output"))


async def _maybe_refund(refund_fn, user: dict, source: str, credits: int) -> None:
    """Reverse an AC reservation when the pipeline failed AFTER charging.
    No-op when source='admin' (no charge happened) or refund_fn is unset."""
    if not refund_fn or source not in ("monthly", "topup"):
        return
    user_id = str(user.get("id", "") or user.get("user_id", "") or "")
    if not user_id:
        return
    try:
        await refund_fn(user_id, source, credits=credits)
    except Exception:
        pass  # refund is best-effort; never let it bubble out of the failure path


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


class PlanRequest(BaseModel):
    script: str
    category: str | None = None
    topic: str | None = None


class ScenesRequest(BaseModel):
    script: str
    category: str | None = None
    topic: str | None = None
    image_model: str = Field(default="seedream_45")
    beats_target: int = 12


def build_skeleton_ai_router(
    require_auth: Callable[..., dict] | None = None,
    *,
    reserve_credit: Callable[..., Any] | None = None,
    refund_credit: Callable[..., Any] | None = None,
) -> APIRouter:
    """
    Build the Skeleton AI router.

    require_auth     — FastAPI dep returning the authed user dict.
    reserve_credit   — async fn(user, ac_cost) → (allowed: bool, source: str, state: dict).
                       Caller wraps backend.py's _reserve_generation_credit + plan resolver.
                       If None, credit gating is disabled (test/dev mode).
    refund_credit    — async fn(user_id, source, *, credits) → None.
                       Used to reverse a reservation if the pipeline fails after charging.
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
    # Visual planner — preview the locked character + style sheet before
    # burning fal money on stills. Frontend calls this after the script lands.
    # ──────────────────────────────────────────────────────────────────────

    @router.post("/plan")
    async def plan_script(body: PlanRequest, _user: dict = auth_dep):
        if not body.script or not body.script.strip():
            raise HTTPException(400, "script is required")
        try:
            grok = GrokClient()
        except GrokAuthError as e:
            raise HTTPException(503, f"config: {e}")
        cat_label = ""
        if body.category:
            try:
                cat_label = get_category(body.category).get("label", "")
            except ValueError:
                cat_label = ""
        plan = analyze_script(grok, body.script, category_label=cat_label, topic=body.topic)
        return {"plan": plan}

    # ──────────────────────────────────────────────────────────────────────
    # Stills-only render: script → plan → 12 stills via the user's chosen
    # image_model. NO i2v, NO audio, NO mux — this is the cheap preview gate
    # before the full pipeline burn. Frontend calls this on Generate Scenes.
    # ──────────────────────────────────────────────────────────────────────

    @router.post("/scenes")
    async def generate_scenes(body: ScenesRequest, _user: dict = auth_dep):
        """
        SSE-streaming stills render. Emits events as scenes complete so the
        frontend can reveal each tile the moment fal returns it (Korpi-style),
        instead of waiting for all 12 to finish before the user sees anything.

        Event sequence:
          event: meta       data: {job_id, image_model, endpoint, total}
          event: plan       data: {plan}             # locked character sheet
          event: scene_start  data: {beat_index, narration}
          event: scene      data: {beat_index, narration, outfit,
                                  scene_action, motion_prompt, image_path}
          ... repeated per beat ...
          event: complete   data: {job_id, scenes_count}
          event: error      data: {beat_index?, message}    # on failure
        """
        if not body.script or not body.script.strip():
            raise HTTPException(400, "script is required")
        if body.image_model not in MODEL_ENDPOINTS:
            raise HTTPException(
                400,
                f"unknown image_model {body.image_model!r}. valid: {sorted(MODEL_ENDPOINTS.keys())}"
            )
        try:
            grok = GrokClient()
        except GrokAuthError as e:
            raise HTTPException(503, f"config: {e}")

        cat_label = ""
        if body.category:
            try:
                cat_label = get_category(body.category).get("label", "")
            except ValueError:
                cat_label = ""

        sentences = split_script_into_beats(body.script, target_count=body.beats_target)
        if not sentences:
            raise HTTPException(400, "script produced zero beats")

        job_id = uuid.uuid4().hex[:12]
        workspace = OUTPUT_ROOT / job_id
        stills_dir = workspace / "stills"
        stills_dir.mkdir(parents=True, exist_ok=True)
        (workspace / "script.txt").write_text(body.script, encoding="utf-8")

        endpoint = MODEL_ENDPOINTS[body.image_model]

        def sse(event: str, data: dict) -> str:
            return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

        def event_stream():
            scenes_out: list[dict] = []
            try:
                # 1. Meta + plan up front so the UI can show "Generating x/N" immediately.
                yield sse("meta", {
                    "job_id": job_id,
                    "image_model": body.image_model,
                    "endpoint": endpoint,
                    "total": len(sentences),
                })
                plan = analyze_script(grok, body.script, category_label=cat_label, topic=body.topic)
                (workspace / "scene_plan.json").write_text(
                    json.dumps(plan, indent=2, ensure_ascii=False), encoding="utf-8"
                )
                yield sse("plan", {"plan": plan})

                # 2. Per beat: derive visuals + render still + stream the result.
                for i, narration in enumerate(sentences):
                    sid = f"b{i:02d}"
                    yield sse("scene_start", {"beat_index": i, "narration": narration})
                    try:
                        outfit, action, motion = derive_beat_visuals(
                            grok, narration, cat_label, plan=plan
                        )
                        still_prompt = assemble_scene_prompt(action, outfit, mint_bg=True)
                        gen_still_for_model(
                            body.image_model,
                            still_prompt,
                            stills_dir / f"{sid}.png",
                            negative_prompt=NEG_STILL,
                        )
                    except StillsError as e:
                        yield sse("error", {"beat_index": i, "message": str(e)})
                        continue
                    except Exception as e:  # pragma: no cover — defensive
                        yield sse("error", {"beat_index": i, "message": f"{type(e).__name__}: {e}"})
                        continue
                    scene = {
                        "beat_index": i,
                        "narration": narration,
                        "outfit": outfit,
                        "scene_action": action,
                        "motion_prompt": motion,
                        "image_path": f"/api/skeleton-ai/jobs/{job_id}/stills/{sid}.png",
                    }
                    scenes_out.append(scene)
                    yield sse("scene", scene)

                # 3. Persist final manifest + signal completion.
                (workspace / "scenes.json").write_text(
                    json.dumps({"job_id": job_id, "scenes": scenes_out}, indent=2),
                    encoding="utf-8",
                )
                yield sse("complete", {"job_id": job_id, "scenes_count": len(scenes_out)})
            except GeneratorExit:
                # Client cancelled (Stop button). Don't crash the worker.
                return
            except Exception as e:  # pragma: no cover
                yield sse("error", {"message": f"fatal: {type(e).__name__}: {e}"})

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    @router.get("/jobs/{job_id}/stills/{filename}")
    async def serve_still(job_id: str, filename: str, _user: dict = auth_dep):
        if not job_id.replace("_", "").isalnum() or len(job_id) > 32:
            raise HTTPException(400, "bad_job_id")
        if not filename.endswith(".png") or "/" in filename or ".." in filename:
            raise HTTPException(400, "bad_filename")
        path = OUTPUT_ROOT / job_id / "stills" / filename
        if not path.exists():
            raise HTTPException(404, "not_found")
        from fastapi.responses import FileResponse
        return FileResponse(str(path), media_type="image/png")

    # ──────────────────────────────────────────────────────────────────────
    # Full pipeline (synchronous v0; queue later)
    # ──────────────────────────────────────────────────────────────────────

    @router.post("/generate")
    async def generate(body: GenerateRequest, user: dict = auth_dep):
        if body.tier not in ("standard", "premium"):
            raise HTTPException(400, "tier must be standard or premium")

        ac_required = AC_COST_PREMIUM if body.tier == "premium" else AC_COST_STANDARD

        # Reserve AC up front. Returns (allowed, source, state). Source 'admin'
        # means free (the admin/owner bypass); 'monthly' or 'topup' means we
        # actually charged the wallet and must refund on pipeline failure.
        credit_source = "admin"
        if reserve_credit:
            try:
                allowed, credit_source, state = await reserve_credit(user, ac_cost=ac_required)
            except Exception as e:
                raise HTTPException(503, f"billing_check_failed: {e}")
            if not allowed:
                have = int(state.get("credits_total_remaining", 0) or 0)
                raise HTTPException(
                    402,
                    detail={
                        "code": "insufficient_credits",
                        "needed": ac_required,
                        "have": have,
                        "tier": body.tier,
                        "topup_packs": [{"ac": 50, "price_usd": 20}, {"ac": 200, "price_usd": 69}],
                    },
                )

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
            await _maybe_refund(refund_credit, user, credit_source, ac_required)
            raise HTTPException(503, f"upstream_auth: {e}")
        except Exception as e:
            await _maybe_refund(refund_credit, user, credit_source, ac_required)
            raise HTTPException(500, f"pipeline_failed: {e}")

        return {"job_id": job_id, "ac_charged": ac_required if credit_source != "admin" else 0,
                "credit_source": credit_source, **result}

    @router.get("/jobs/{job_id}")
    async def job_status(job_id: str, _user: dict = auth_dep):
        if not job_id.isalnum() or len(job_id) > 32:
            raise HTTPException(400, "bad_job_id")
        result_path = OUTPUT_ROOT / job_id / "result.json"
        if not result_path.exists():
            raise HTTPException(404, "not_found")
        return {"job_id": job_id, "result": json.loads(result_path.read_text())}

    return router
