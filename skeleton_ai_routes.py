"""
Flask blueprint for the Skeleton AI short-form generation API.

Routes:
  GET  /api/skeleton-ai/categories         List the 4 idea categories
  GET  /api/skeleton-ai/voices              List ElevenLabs voices for picker
  POST /api/skeleton-ai/script              Generate script with Grok (streamable)
  POST /api/skeleton-ai/generate            Run full pipeline → mp4
  GET  /api/skeleton-ai/jobs/<id>           Poll a generation job
  GET  /api/skeleton-ai/pricing             Tier table for pricing UI

Wire into the main Flask app via:
    from skeleton_ai_routes import skeleton_ai_bp
    app.register_blueprint(skeleton_ai_bp)
"""
from __future__ import annotations
import os
import uuid
from pathlib import Path
from flask import Blueprint, request, jsonify, Response, stream_with_context

from skeleton_ai.prompts.idea_lists import list_categories, get_category
from skeleton_ai.scripting_grok import GrokClient, GrokAuthError, build_script_prompt
from skeleton_ai.voice_elevenlabs import ElevenLabsClient, ElevenLabsAuthError
from skeleton_ai.pipeline import run as run_pipeline
from skeleton_ai.i2v_engine import AC_COST_STANDARD, AC_COST_PREMIUM


skeleton_ai_bp = Blueprint("skeleton_ai", __name__, url_prefix="/api/skeleton-ai")

# Output workspaces — replace with Supabase storage in production.
OUTPUT_ROOT = Path(os.getenv("SKELETON_AI_OUTPUT_ROOT", "skeleton_ai/output"))


# ─────────────────────────────────────────────────────────────────────────────
# Read-only metadata endpoints (no fal/Grok/EL spend)
# ─────────────────────────────────────────────────────────────────────────────


@skeleton_ai_bp.get("/categories")
def categories():
    return jsonify({"categories": list_categories()})


@skeleton_ai_bp.get("/pricing")
def pricing():
    """Static pricing tiers — Casey-locked 2026-05-05."""
    return jsonify({
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
    })


@skeleton_ai_bp.get("/voices")
def voices():
    """List ElevenLabs voices for the picker UI."""
    try:
        el = ElevenLabsClient()
        vs = el.list_voices()
    except ElevenLabsAuthError as e:
        return jsonify({"error": "elevenlabs_auth_failed", "detail": str(e)}), 503
    return jsonify({
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
    })


# ─────────────────────────────────────────────────────────────────────────────
# Script generation (Grok streaming)
# ─────────────────────────────────────────────────────────────────────────────


@skeleton_ai_bp.post("/script")
def generate_script():
    """Stream script generation via Grok 4.1 Fast Reasoning."""
    body = request.get_json(force=True, silent=True) or {}
    category_key = body.get("category", "human_limits")
    topic = body.get("topic")  # optional
    stream_mode = bool(body.get("stream", False))

    try:
        cat = get_category(category_key)
        grok = GrokClient()
    except (ValueError, GrokAuthError) as e:
        return jsonify({"error": "config", "detail": str(e)}), 503

    user_prompt = build_script_prompt(cat["system_prompt"], topic)

    if stream_mode:
        def gen():
            for piece in grok.stream(cat["system_prompt"], user_prompt, max_tokens=1500):
                yield f"data: {piece}\n\n"
            yield "data: [DONE]\n\n"
        return Response(stream_with_context(gen()), mimetype="text/event-stream")

    text = grok.complete(cat["system_prompt"], user_prompt, max_tokens=1500)
    return jsonify({"script": text})


# ─────────────────────────────────────────────────────────────────────────────
# Full pipeline kickoff (synchronous for v0; queue later)
# ─────────────────────────────────────────────────────────────────────────────


@skeleton_ai_bp.post("/generate")
def generate():
    """
    Run the full Skeleton AI pipeline.
    Body: {category, topic?, tier ("standard"|"premium"), voice_id?}

    For v0 this is synchronous and may block ~3-5 min. v1 should push to a
    queue and return a job_id immediately.
    """
    body = request.get_json(force=True, silent=True) or {}
    category_key = body.get("category", "human_limits")
    topic = body.get("topic")
    tier = body.get("tier", "standard")
    voice_id = body.get("voice_id")

    if tier not in ("standard", "premium"):
        return jsonify({"error": "bad_request", "detail": "tier must be standard or premium"}), 400

    # TODO(billing): deduct AC here BEFORE running. If user lacks credits, 402.
    # ac_required = AC_COST_PREMIUM if tier == "premium" else AC_COST_STANDARD
    # if not billing.deduct(user_id, ac_required): return 402

    job_id = uuid.uuid4().hex[:12]
    workspace = OUTPUT_ROOT / job_id
    try:
        result = run_pipeline(
            category_key=category_key,
            topic=topic,
            workspace=workspace,
            tier=tier,
            voice_id=voice_id,
        )
    except (GrokAuthError, ElevenLabsAuthError) as e:
        return jsonify({"error": "upstream_auth", "detail": str(e)}), 503
    except Exception as e:
        return jsonify({"error": "pipeline_failed", "detail": str(e)}), 500

    return jsonify({"job_id": job_id, **result})


@skeleton_ai_bp.get("/jobs/<job_id>")
def job_status(job_id: str):
    """Look up a finished job's result.json."""
    if not job_id.isalnum() or len(job_id) > 32:
        return jsonify({"error": "bad_job_id"}), 400
    result_path = OUTPUT_ROOT / job_id / "result.json"
    if not result_path.exists():
        return jsonify({"error": "not_found"}), 404
    return jsonify({"job_id": job_id, "result": result_path.read_text()})
