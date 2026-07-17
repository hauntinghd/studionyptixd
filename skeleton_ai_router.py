"""
FastAPI router for the Skeleton AI short-form generation API.

Wires into backend.py via:
    from skeleton_ai_router import build_skeleton_ai_router
    app.include_router(build_skeleton_ai_router(require_auth=require_auth))

Routes:
  GET  /api/skeleton-ai/categories         List built-in + user custom categories
  POST /api/skeleton-ai/categories         Create a custom category (auth)
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

from fastapi import APIRouter, HTTPException, Depends, Request, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field

from skeleton_ai.prompts.category_registry import (
    create_custom_category,
    get_category,
    list_categories,
    list_valid_keys,
)
from skeleton_ai.prompts.base_style import assemble_scene_prompt, NEG_STILL
from skeleton_ai.scripting_grok import GrokClient, GrokAuthError, build_script_prompt
from skeleton_ai.voice_elevenlabs import ElevenLabsClient, ElevenLabsAuthError
from skeleton_ai.pipeline import (
    run as run_pipeline,
    analyze_script,
    derive_beat_visuals,
    split_script_into_beats,
)
from skeleton_ai.canonical_edit import build_scene_edit_prompt, generate_still_edit
from skeleton_ai.stills_engine import (
    generate as gen_still_for_model,
    MODEL_ENDPOINTS,
    StillsError,
)
from skeleton_ai.i2v_engine import AC_COST_STANDARD, AC_COST_PREMIUM
from studio_agent.image_model_catalog import (
    is_seedream_model,
    normalize_seedream_model_id,
    seedream_endpoint,
)

# Reference videos thread into the Grok system prompt so generated scripts
# mimic the patterns Casey saved as 'winning' inspiration. Best-effort —
# if the references module / table isn't available, we silently skip.
try:
    from catalyst_references import list_user_references, references_as_grok_context
    _REFS_AVAILABLE = True
except Exception:
    _REFS_AVAILABLE = False
    def list_user_references(*_a, **_kw):  # type: ignore
        return []
    def references_as_grok_context(*_a, **_kw):  # type: ignore
        return ""

# Skeleton AI shorts default to the user's CrypticScience channel context,
# but a user can re-tag a reference to 'zerotier' or 'lexi_manhwa' if the
# script should mimic that channel's pattern instead.
SKELETON_AI_SHORTS_CHANNEL_KEYS = ("cryptic_science", "zerotier", "lexi_manhwa", "")


def _user_id(user: dict) -> str:
    return str((user or {}).get("id", "") or (user or {}).get("user_id", "") or "")


def _references_for_skeleton_ai(user: dict) -> str:
    """Pull user's saved refs whose channel_key matches a shorts channel
    or is universal (''). Returns a Grok-ready text block, possibly empty."""
    if not _REFS_AVAILABLE:
        return ""
    uid = _user_id(user)
    if not uid:
        return ""
    matched: list[dict] = []
    seen_ids: set[str] = set()
    try:
        for ck in SKELETON_AI_SHORTS_CHANNEL_KEYS:
            try:
                rows = list_user_references(uid, channel_key=ck)
            except Exception:
                continue
            for r in rows or []:
                rid = str((r or {}).get("id", ""))
                if rid and rid not in seen_ids:
                    seen_ids.add(rid)
                    matched.append(r)
    except Exception:
        return ""
    return references_as_grok_context(matched, max_refs=8)


OUTPUT_ROOT = Path(os.getenv("SKELETON_AI_OUTPUT_ROOT", "skeleton_ai/output"))
REFERENCE_ROOT = OUTPUT_ROOT / "_references"


def _write_job_owner(workspace: Path, job_id: str, user: dict) -> None:
    """Persist the authenticated creator before any job artifacts are exposed."""
    uid = _user_id(user).strip()
    if not uid:
        raise HTTPException(401, "auth_required")
    workspace = Path(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    spec_path = workspace / "job_spec.json"
    spec: dict[str, Any] = {}
    if spec_path.is_file():
        try:
            loaded = json.loads(spec_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                spec = loaded
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            spec = {}
    existing_owner = str(spec.get("user_id") or "").strip()
    if existing_owner and existing_owner != uid:
        raise HTTPException(404, "job_not_found")
    spec["job_id"] = str(job_id or workspace.name)
    spec["user_id"] = uid
    temporary = workspace / f".job_spec.{uuid.uuid4().hex}.tmp"
    try:
        temporary.write_text(json.dumps(spec, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(temporary, spec_path)
    finally:
        temporary.unlink(missing_ok=True)


def _require_job_owner(workspace: Path, user: dict) -> str:
    """Fail closed when a local job is missing owner metadata or is cross-user."""
    uid = _user_id(user).strip()
    if not uid:
        raise HTTPException(401, "auth_required")
    spec_path = Path(workspace) / "job_spec.json"
    try:
        payload = json.loads(spec_path.read_text(encoding="utf-8"))
        owner_id = str((payload or {}).get("user_id") or "").strip() if isinstance(payload, dict) else ""
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        owner_id = ""
    if not owner_id or owner_id != uid:
        # Hide both job existence and owner identity from other accounts.
        raise HTTPException(404, "job_not_found")
    return uid


def _persist_skeleton_reference(workspace: Path, reference_image: str) -> str:
    from skeleton_ai.styled_pipeline import _persist_skeleton_reference as persist_ref

    return persist_ref(workspace, reference_image)


def _resolve_skeleton_reference(workspace: Path) -> str:
    from skeleton_ai.styled_pipeline import _resolve_skeleton_master_reference

    return _resolve_skeleton_master_reference(workspace, None)


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
    category: str = Field(default="people_blogs")
    topic: str | None = None
    stream: bool = False


class GenerateRequest(BaseModel):
    category: str = Field(default="people_blogs")
    topic: str | None = None
    tier: str = Field(default="standard")  # legacy; use video_model when possible
    video_model: str | None = Field(
        default=None,
        description="seedance | pixverse | kling_pro — stills always canonical Seedream edit",
    )
    image_model: str | None = None
    voice_id: str | None = None
    script_override: str | None = None
    script: str | None = None  # frontend alias
    render_tier: str = Field(default="draft")  # draft | ship | documentary
    reference_image: str | None = Field(
        default=None,
        description="User-uploaded skeleton reference (HTTPS URL or data:image/... base64)",
    )


class PlanRequest(BaseModel):
    script: str
    category: str | None = None
    topic: str | None = None


class ScenesRequest(BaseModel):
    script: str
    category: str | None = None
    topic: str | None = None
    image_model: str = Field(default="seedream_edit")  # ignored — canonical edit is always used
    beats_target: int = 12
    reference_image: str | None = Field(
        default=None,
        description="User-uploaded skeleton reference (HTTPS URL or data:image/... base64)",
    )


class RegenerateSceneRequest(BaseModel):
    job_id: str
    beat_index: int = Field(ge=0)
    outfit: str | None = None
    scene_action: str | None = None
    motion_prompt: str | None = None
    reference_image: str | None = None
    image_model: str | None = None


class CreateCategoryRequest(BaseModel):
    label: str = Field(min_length=2, max_length=80)
    key: str | None = Field(default=None, max_length=48)
    tagline: str | None = Field(default=None, max_length=200)
    system_prompt: str | None = Field(default=None, max_length=4000)
    seed_ideas: list[str] = Field(default_factory=list, max_length=12)


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
    async def categories(user: dict = auth_dep):
        uid = _user_id(user)
        cats = list_categories(user_id=uid or None)
        return {
            "categories": cats,
            "builtin_count": sum(1 for c in cats if c.get("builtin")),
            "custom_count": sum(1 for c in cats if c.get("custom")),
            "valid_keys": list_valid_keys(uid or None),
        }

    @router.post("/categories")
    async def create_category(body: CreateCategoryRequest, user: dict = auth_dep):
        uid = _user_id(user)
        if not uid or uid == "anon":
            raise HTTPException(401, "sign in required to create custom categories")
        try:
            entry = create_custom_category(
                uid,
                label=body.label,
                key=body.key,
                tagline=body.tagline,
                system_prompt=body.system_prompt,
                seed_ideas=body.seed_ideas,
            )
        except ValueError as e:
            raise HTTPException(400, str(e))
        return {"category": entry, "valid_keys": list_valid_keys(uid)}

    @router.get("/video-models")
    async def video_models():
        from skeleton_ai.i2v_engine import list_video_models

        return {
            "video_models": list_video_models(),
            "stills_model": {
                "key": "seedream_edit",
                "label": "Seedream 4.5 Edit (canonical skeleton master)",
                "locked": True,
            },
        }

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

    @router.post("/reference")
    async def upload_reference(reference_image: UploadFile = File(...), user: dict = auth_dep):
        from upload_limits import MAX_REFERENCE_IMAGE_BYTES, UploadTooLargeError, read_upload_limited

        uid = _user_id(user) or "anon"
        try:
            raw = await read_upload_limited(
                reference_image,
                max_bytes=MAX_REFERENCE_IMAGE_BYTES,
                label="skeleton reference image",
            )
        except UploadTooLargeError as exc:
            raise HTTPException(413, "Reference image exceeds 12MB") from exc
        if not raw or len(raw) < 1024:
            raise HTTPException(400, "reference image too small")
        mime = str(reference_image.content_type or "image/png").strip().lower()
        if not mime.startswith("image/"):
            raise HTTPException(400, "reference must be an image")
        ext = ".png"
        if "jpeg" in mime or "jpg" in mime:
            ext = ".jpg"
        elif "webp" in mime:
            ext = ".webp"
        REFERENCE_ROOT.mkdir(parents=True, exist_ok=True)
        ref_name = f"{uid}_{uuid.uuid4().hex[:12]}{ext}"
        ref_path = REFERENCE_ROOT / ref_name
        ref_path.write_bytes(raw)
        return {
            "ok": True,
            "reference_image": str(ref_path),
            "reference_image_url": f"/api/skeleton-ai/references/{ref_name}",
        }

    @router.get("/references/{filename}")
    async def serve_reference(filename: str, _user: dict = auth_dep):
        safe = os.path.basename(filename)
        if not safe or safe != filename or ".." in filename:
            raise HTTPException(400, "bad_filename")
        uid = _user_id(_user).strip()
        if not uid:
            raise HTTPException(401, "auth_required")
        if not safe.startswith(f"{uid}_"):
            raise HTTPException(404, "not_found")
        path = REFERENCE_ROOT / safe
        if not path.exists():
            raise HTTPException(404, "not_found")
        from fastapi.responses import FileResponse

        media_type = "image/png"
        if path.suffix.lower() in {".jpg", ".jpeg"}:
            media_type = "image/jpeg"
        elif path.suffix.lower() == ".webp":
            media_type = "image/webp"
        return FileResponse(str(path), media_type=media_type)

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
    async def generate_script(body: ScriptRequest, user: dict = auth_dep):
        try:
            cat = get_category(body.category, user_id=_user_id(user) or None)
            grok = GrokClient()
        except ValueError as e:
            raise HTTPException(400, str(e))
        except GrokAuthError as e:
            raise HTTPException(503, f"config: {e}")

        # Append the user's reference-video context (if any) so the script
        # mimics patterns from saved viral inspirations.
        refs_block = _references_for_skeleton_ai(user)
        if refs_block:
            system = (
                cat["system_prompt"]
                + "\n\n"
                + refs_block
                + "\n\nMatch the hook structure and pacing of these references where it fits the topic."
            )
        else:
            system = cat["system_prompt"]

        user_prompt = build_script_prompt(cat["system_prompt"], body.topic)

        if body.stream:
            def sse_generator():
                try:
                    for piece in grok.stream(system, user_prompt, max_tokens=1500):
                        # Escape newlines so SSE framing isn't broken.
                        safe = piece.replace("\n", "\\n")
                        yield f"data: {safe}\n\n"
                    yield "data: [DONE]\n\n"
                except GrokAuthError as e:
                    yield f"event: error\ndata: {e}\n\n"
            return StreamingResponse(sse_generator(), media_type="text/event-stream")

        text = grok.complete(system, user_prompt, max_tokens=1500)
        return {
            "script": text,
            "references_used": bool(refs_block),
        }

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
                cat_label = get_category(body.category, user_id=_user_id(_user) or None).get("label", "")
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
    async def generate_scenes(body: ScenesRequest, request: Request, user: dict = auth_dep):
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
        from studio_agent.direct_production import fail_closed_uncovered, runpod_production_enabled

        if runpod_production_enabled():
            fail_closed_uncovered(request, "/api/skeleton-ai/scenes")
        if not body.script or not body.script.strip():
            raise HTTPException(400, "script is required")
        uid = _user_id(user) or None
        if body.category:
            try:
                get_category(body.category, user_id=uid)
            except ValueError as e:
                raise HTTPException(400, str(e))
        # Stills always use canonical Seedream edit; image_model is kept for API compat.
        selected_image_model = normalize_seedream_model_id(body.image_model or "seedream_edit")
        if not is_seedream_model(selected_image_model):
            raise HTTPException(
                400,
                f"unknown canonical image_model {body.image_model!r}"
            )
        try:
            grok = GrokClient()
        except GrokAuthError as e:
            raise HTTPException(503, f"config: {e}")

        cat_label = ""
        if body.category:
            try:
                cat_label = get_category(body.category, user_id=uid).get("label", "")
            except ValueError:
                cat_label = ""

        sentences = split_script_into_beats(body.script, target_count=body.beats_target)
        if not sentences:
            raise HTTPException(400, "script produced zero beats")

        job_id = uuid.uuid4().hex[:12]
        workspace = OUTPUT_ROOT / job_id
        stills_dir = workspace / "stills"
        stills_dir.mkdir(parents=True, exist_ok=True)
        _write_job_owner(workspace, job_id, user)
        (workspace / "script.txt").write_text(body.script, encoding="utf-8")
        master_ref = ""
        if body.reference_image and str(body.reference_image).strip():
            master_ref = _persist_skeleton_reference(workspace, str(body.reference_image).strip())
        else:
            master_ref = _resolve_skeleton_reference(workspace)

        endpoint = seedream_endpoint(selected_image_model, edit=True)

        def sse(event: str, data: dict) -> str:
            return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

        def event_stream():
            scenes_out: list[dict] = []
            try:
                # 1. Meta + plan up front so the UI can show "Generating x/N" immediately.
                yield sse("meta", {
                    "job_id": job_id,
                    "image_model": selected_image_model,
                    "endpoint": endpoint,
                    "render_mode": "user_reference_edit" if master_ref else "canonical_master_edit",
                    "reference_locked": bool(master_ref),
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
                        out_file = stills_dir / f"{sid}.png"
                        edit_prompt = build_scene_edit_prompt(
                            topic=body.topic or cat_label,
                            visual_description=action,
                            outfit=outfit,
                        )
                        generate_still_edit(
                            edit_prompt,
                            out_file,
                            master_url=master_ref,
                            seed=420042 + i,
                            image_model_id=selected_image_model,
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
                        "image_model_id": selected_image_model,
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

    @router.post("/scenes/regenerate")
    async def regenerate_scene(
        body: RegenerateSceneRequest,
        request: Request,
        _user: dict = auth_dep,
    ):
        from studio_agent.direct_production import fail_closed_uncovered, runpod_production_enabled

        if runpod_production_enabled():
            fail_closed_uncovered(request, "/api/skeleton-ai/scenes/regenerate")
        job_id = str(body.job_id or "").strip()
        if not job_id.replace("_", "").isalnum() or len(job_id) > 32:
            raise HTTPException(400, "bad_job_id")
        workspace = OUTPUT_ROOT / job_id
        if not workspace.is_dir():
            raise HTTPException(404, "job_not_found")
        _require_job_owner(workspace, _user)
        if body.reference_image and str(body.reference_image).strip():
            _persist_skeleton_reference(workspace, str(body.reference_image).strip())
        master_ref = _resolve_skeleton_reference(workspace)
        scenes_path = workspace / "scenes.json"
        scenes_doc: dict[str, Any] = {"job_id": job_id, "scenes": []}
        if scenes_path.is_file():
            try:
                loaded = json.loads(scenes_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    scenes_doc = loaded
            except Exception:
                pass
        scenes = list(scenes_doc.get("scenes") or [])
        scene = next((s for s in scenes if int(s.get("beat_index", -1)) == int(body.beat_index)), None)
        if scene is None:
            raise HTTPException(404, "scene_not_found")
        outfit = str(body.outfit if body.outfit is not None else scene.get("outfit") or "")
        action = str(body.scene_action if body.scene_action is not None else scene.get("scene_action") or "")
        motion = str(body.motion_prompt if body.motion_prompt is not None else scene.get("motion_prompt") or "")
        sid = f"b{int(body.beat_index):02d}"
        stills_dir = workspace / "stills"
        stills_dir.mkdir(parents=True, exist_ok=True)
        out_file = stills_dir / f"{sid}.png"
        edit_prompt = build_scene_edit_prompt(
            topic=str(scene.get("topic") or ""),
            visual_description=action,
            outfit=outfit,
        )
        selected_image_model = normalize_seedream_model_id(
            body.image_model or scene.get("image_model_id") or "seedream_edit"
        )
        if not is_seedream_model(selected_image_model):
            raise HTTPException(400, f"unknown canonical image_model {selected_image_model!r}")
        generate_still_edit(
            edit_prompt,
            out_file,
            master_url=master_ref,
            seed=880000 + int(body.beat_index),
            image_model_id=selected_image_model,
        )
        scene.update({
            "outfit": outfit,
            "scene_action": action,
            "motion_prompt": motion,
            "edit_prompt": edit_prompt,
            "image_path": f"/api/skeleton-ai/jobs/{job_id}/stills/{sid}.png",
            "image_model_id": selected_image_model,
        })
        scenes_doc["scenes"] = scenes
        scenes_path.write_text(json.dumps(scenes_doc, indent=2, ensure_ascii=False), encoding="utf-8")
        return {"ok": True, "scene": scene}

    @router.get("/jobs/{job_id}/stills/{filename}")
    async def serve_still(job_id: str, filename: str, _user: dict = auth_dep):
        if not job_id.replace("_", "").isalnum() or len(job_id) > 32:
            raise HTTPException(400, "bad_job_id")
        if not filename.endswith(".png") or "/" in filename or ".." in filename:
            raise HTTPException(400, "bad_filename")
        workspace = OUTPUT_ROOT / job_id
        _require_job_owner(workspace, _user)
        path = workspace / "stills" / filename
        if not path.exists():
            raise HTTPException(404, "not_found")
        from fastapi.responses import FileResponse
        return FileResponse(str(path), media_type="image/png")

    # ──────────────────────────────────────────────────────────────────────
    # Full pipeline (synchronous v0; queue later)
    # ──────────────────────────────────────────────────────────────────────

    @router.post("/generate")
    async def generate(body: GenerateRequest, request: Request, user: dict = auth_dep):
        from studio_agent.direct_production import execute_logged_production, runpod_production_enabled

        if runpod_production_enabled():
            script_text = (body.script_override or body.script or "").strip()
            payload = await execute_logged_production(
                "start_shortform_generate",
                {
                    "category_key": body.category,
                    "topic": str(body.topic or "Custom scripted short").strip(),
                    "script": script_text,
                    "scene_count": 1,
                    "render_style": "skeleton_host",
                    "video_model": str(body.video_model or "seedance"),
                    "tier": body.tier,
                    "image_model_id": str(body.image_model or "seedream_edit"),
                    "reference_image": str(body.reference_image or "").strip(),
                    "visual_proof_only": True,
                    "animate": False,
                    "captions_enabled": True,
                    "caption_mode": "word",
                },
                request=request,
                user_id=_user_id(user),
                content_format="short",
            )
            payload.setdefault("poll_url", f"/api/skeleton-ai/jobs/{payload.get('job_id', '')}")
            payload["legacy_route"] = "/api/skeleton-ai/generate"
            payload["staged_visual_proof"] = True
            return payload
        if body.tier not in ("standard", "premium"):
            raise HTTPException(400, "tier must be standard or premium")
        if body.render_tier == "ship" and body.tier != "premium":
            body = body.model_copy(update={"tier": "premium"})
        script_text = (body.script_override or body.script or "").strip() or None

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

        uid = _user_id(user) or None
        try:
            get_category(body.category, user_id=uid)
            from skeleton_ai.i2v_engine import resolve_video_model_chain

            resolve_video_model_chain(video_model=body.video_model, tier=body.tier)
        except ValueError as e:
            raise HTTPException(400, str(e))
        except Exception as e:
            from skeleton_ai.i2v_engine import I2VError

            if isinstance(e, I2VError):
                raise HTTPException(400, str(e))

        job_id = uuid.uuid4().hex[:12]
        workspace = OUTPUT_ROOT / job_id
        _write_job_owner(workspace, job_id, user)
        master_ref = ""
        if body.reference_image and str(body.reference_image).strip():
            master_ref = _persist_skeleton_reference(workspace, str(body.reference_image).strip())
        try:
            result = run_pipeline(
                category_key=body.category,
                topic=body.topic,
                workspace=workspace,
                tier=body.tier,
                video_model=body.video_model,
                voice_id=body.voice_id,
                script_override=script_text,
                user_id=uid,
                master_reference_url=master_ref,
                image_model_id=(
                    normalize_seedream_model_id(body.image_model or "seedream_edit")
                    if is_seedream_model(body.image_model or "seedream_edit")
                    else "seedream_edit"
                ),
            )
        except ValueError as e:
            await _maybe_refund(refund_credit, user, credit_source, ac_required)
            raise HTTPException(400, str(e))
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
        if not job_id.replace("_", "").isalnum() or len(job_id) > 32:
            raise HTTPException(400, "bad_job_id")
        from studio_agent.runpod_bridge import get_dispatch_receipt_by_studio_job_id

        if get_dispatch_receipt_by_studio_job_id(job_id) is not None:
            from studio_agent.jobs import get_job_snapshot, job_access_metadata

            access = job_access_metadata(job_id, "shortform")
            uid = _user_id(_user).strip()
            if not uid:
                raise HTTPException(401, "auth_required")
            if not access.get("exists") or str(access.get("owner_id") or "").strip() != uid:
                raise HTTPException(404, "job_not_found")
            snapshot = get_job_snapshot(job_id, "shortform")
            return {"job_id": job_id, "result": snapshot, **snapshot}
        workspace = OUTPUT_ROOT / job_id
        _require_job_owner(workspace, _user)
        result_path = workspace / "result.json"
        if not result_path.exists():
            raise HTTPException(404, "not_found")
        return {"job_id": job_id, "result": json.loads(result_path.read_text())}

    return router
