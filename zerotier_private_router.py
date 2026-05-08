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
import asyncio
import json
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse, FileResponse
from pydantic import BaseModel, Field

from backend_script_prompts import TEMPLATE_SYSTEM_PROMPTS
from skeleton_ai.scripting_grok import GrokClient, GrokAuthError
from zerotier_private.pipeline import (
    render_zerotier_short,
    render_stills_only,
    regenerate_one_still,
    render_finalize,
    ZTRenderError,
)


class ZTScriptRequest(BaseModel):
    topic: str = Field(..., min_length=1, description="Candidate short title or topic seed")
    stream: bool = False


class ZTGenerateTopicsRequest(BaseModel):
    count: int = Field(default=8, ge=3, le=15, description="How many topic candidates to generate")


class ZTRenderRequest(BaseModel):
    script_json: str = Field(..., min_length=10, description="Grok-generated 8-beat script JSON (raw string)")
    final_filename: str | None = None
    # Phase 3 — learning loop. Frontend includes its heuristic-v1 score so we
    # can log prediction-vs-actual once Catalyst harvests outcomes.
    predicted_score: float | None = None
    predicted_like_rate: float | None = None
    topic: str | None = None


# Phase 4.5: per-scene approval flow
class ZTStillsOnlyRequest(BaseModel):
    script_json: str = Field(..., min_length=10)
    topic: str | None = None
    predicted_score: float | None = None
    predicted_like_rate: float | None = None


class ZTRegenStillRequest(BaseModel):
    job_id: str
    scene_index: int = Field(ge=0, le=20)
    custom_prompt: str | None = None


class ZTFinalizeRequest(BaseModel):
    job_id: str
    final_filename: str | None = None


# Where rendered MP4s + stills live. CRITICAL: must be on the persistent
# volume (/var/data on Fly) so renders survive backend deploys. The Fly
# machine reboots on every image push, which wipes the ephemeral container
# filesystem (/app and below) but preserves /var/data. The default below
# falls back to a relative path for local dev where /var/data doesn't exist.
def _zt_output_root_default() -> str:
    if os.path.isdir("/var/data"):
        return "/var/data/zerotier_private/output"
    return "zerotier_private/output"
ZT_OUTPUT_ROOT = Path(os.getenv("ZEROTIER_PRIVATE_OUTPUT_ROOT", _zt_output_root_default()))

# Phase 3 — append-only JSONL log of every render's prediction. Each line is
# a JSON object: {job_id, ts, title, predicted_score, predicted_like_rate}.
# Cross-referenced with actual outcomes (views/likes from Catalyst's channel
# sync) by GET /predictions. Same persistent-volume requirement as
# ZT_OUTPUT_ROOT — lives at /var/data on Fly so it survives deploys.
def _zt_predictions_log_default() -> str:
    if os.path.isdir("/var/data"):
        return "/var/data/zerotier_private/predictions.jsonl"
    return "zerotier_private/predictions.jsonl"
ZT_PREDICTIONS_LOG = Path(os.getenv("ZEROTIER_PRIVATE_PREDICTIONS_LOG", _zt_predictions_log_default()))

# Phase 4.5b — background job state. Keys: job_id. Values: {
#   stage: "stills" | "finalize",
#   status: "pending" | "ready" | "failed",
#   started_at: float,
#   result?: dict,           # populated when status="ready"
#   error?: str,             # populated when status="failed"
#   _task?: asyncio.Task,    # strong ref so GC doesn't kill it mid-flight
# }
# Stored in-memory because Fly machines are single-instance for this app and
# the job lifecycle is < 10 minutes — no need for Redis. result.json on disk
# remains the durable record once a job finishes.
_zt_jobs_status: dict[str, dict] = {}


def _safe_filename(raw: str | None, default: str) -> str:
    if not raw:
        return default
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", str(raw)).strip("._-")
    if not s.lower().endswith(".mp4"):
        s = s + ".mp4"
    return s[:96] or default


def _normalize_title(title: str) -> str:
    """Lowercase + collapse whitespace + strip punctuation for fuzzy match."""
    s = str(title or "").lower()
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _title_word_overlap(a: str, b: str) -> float:
    """Word-set overlap ratio in [0,1]. Symmetric Jaccard on words ≥4 chars."""
    aw = set(w for w in _normalize_title(a).split() if len(w) >= 4)
    bw = set(w for w in _normalize_title(b).split() if len(w) >= 4)
    if not aw or not bw:
        return 0.0
    inter = aw & bw
    union = aw | bw
    return len(inter) / len(union) if union else 0.0


def _append_prediction_log(record: dict) -> None:
    """Atomic-ish append. The Fly volume is single-writer per machine; this
    is good enough for the prediction log."""
    try:
        ZT_PREDICTIONS_LOG.parent.mkdir(parents=True, exist_ok=True)
        with ZT_PREDICTIONS_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass  # log is best-effort; never block a render


def _read_prediction_log(limit: int = 100) -> list[dict]:
    """Read the last N predictions, newest first."""
    if not ZT_PREDICTIONS_LOG.exists():
        return []
    try:
        lines = ZT_PREDICTIONS_LOG.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []
    out: list[dict] = []
    for raw in reversed(lines[-(limit * 2):]):
        raw = raw.strip()
        if not raw:
            continue
        try:
            out.append(json.loads(raw))
        except Exception:
            continue
        if len(out) >= limit:
            break
    return out


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
    # POST /generate-topics — autonomous topic generation
    #
    # Pulls the user's ZeroTier channel data (winners + losers + already-
    # uploaded titles), encodes the validated formula + competitor patterns
    # decoded from CreationsComic (329K subs, 7.70% top LR) and TheManDeeDubs
    # (1.89M subs, 8.20% top LR), and asks Grok for N fresh "The Time Wally
    # West [past-tense]" topic ideas optimized for predicted virality.
    #
    # Returns: {topics: [string, ...], channel_baseline: {top_lr, avg_lr}}.
    # ────────────────────────────────────────────────────────────────────
    @router.post("/generate-topics")
    async def generate_zt_topics(body: ZTGenerateTopicsRequest, user: dict = auth_dep):
        _gate_admin(user)

        # Pull channel uploads from the connection store (same path as
        # /predictions). If the channel isn't synced, the prompt falls back
        # to the channel-wide patterns alone.
        uploads: list[dict] = []
        try:
            from youtube import _load_youtube_connections, _youtube_bucket_for_user
            user_id = str(user.get("id", "") or user.get("user_id", "") or "")
            if user_id:
                _load_youtube_connections()
                bucket = _youtube_bucket_for_user(user_id)
                channels = (bucket or {}).get("channels") or {}
                ZT_ID = "UC9Gth_4MVet6rdPH7MHJf-g"
                ch = channels.get(ZT_ID) or {}
                snap = (ch.get("analytics_snapshot") or {})
                uploads = list(snap.get("uploaded_videos") or [])
        except Exception:
            uploads = []

        # Compute like-rates + sort
        scored: list[tuple[str, float]] = []
        for v in uploads:
            views = int(float((v or {}).get("views", 0) or 0))
            likes = int(float((v or {}).get("likes", 0) or 0))
            title = str((v or {}).get("title", "") or "").strip()
            if not title or views < 50:
                continue
            lr = (likes / views) * 100 if views > 0 else 0.0
            scored.append((title, round(lr, 2)))
        scored.sort(key=lambda x: x[1], reverse=True)

        # Drop bot-spike outliers (anomalously high views with < 0.3% LR — the
        # Wally vs Death pattern). Keep them out of "winners" and "losers" lists.
        valid = [(t, lr) for t, lr in scored if lr >= 0.5 or len(scored) <= 5]
        winners = valid[:5]
        losers = valid[-3:] if len(valid) >= 8 else valid[-min(3, len(valid)):]

        already_uploaded = [t for t, _ in scored]

        # Compose the dynamic-context user prompt. The system prompt remains
        # the locked zerotier_private TEMPLATE_SYSTEM_PROMPT (Conflict Arc +
        # title formula + comic visuals + JSON schema) but we override the
        # output: instead of a single 8-beat script, request a topic LIST.
        winners_block = "\n".join([f"  - \"{t}\" — {lr:.2f}% like-rate" for t, lr in winners]) or "  (none yet)"
        losers_block = "\n".join([f"  - \"{t}\" — {lr:.2f}% like-rate" for t, lr in losers]) or "  (none yet)"
        already_block = "\n".join([f"  - {t}" for t in already_uploaded]) or "  (none)"

        topic_user_prompt = (
            f"You are generating fresh short-video topic ideas for a small DC speedster fan-fiction "
            f"channel called ZeroTier. Use the locked formula encoded in the system prompt above.\n\n"
            f"=== This channel's actual data ===\n\n"
            f"TOP PERFORMERS (what works — highest like-rate):\n{winners_block}\n\n"
            f"UNDER-PERFORMERS (what to avoid):\n{losers_block}\n\n"
            f"ALREADY UPLOADED (do NOT propose these or close variants):\n{already_block}\n\n"
            f"=== Public competitor patterns ===\n\n"
            f"From CreationsComic (329K subs, 7.70% top like-rate) and TheManDeeDubs (1.89M, "
            f"8.20% top): the highest-engagement DC fan-fiction shorts hit on (1) outsmart-not-"
            f"overpower payoffs, (2) family-bond reveals, (3) parallel-universe identity flips, "
            f"(4) iconic-character non-physical confrontations.\n\n"
            f"=== Your task ===\n\n"
            f"Generate {body.count} FRESH short-video topic titles that:\n"
            f"  - Strictly follow the format \"The Time Wally West [past-tense verb]\"\n"
            f"  - Hit cosmic / identity / mortality / time-loss stakes (NOT raw power scaling)\n"
            f"  - Use impersonal forces or iconic characters as antagonists (NOT generic villains)\n"
            f"  - Carry an emotional COMEBACK beat (sacrifice / forgiveness / identity loss)\n"
            f"  - Are NOT in the already-uploaded list above\n"
            f"  - Are NOT minor variants of already-uploaded titles\n"
            f"  - Vary across different stakes archetypes (don't propose 8 time-travel ideas)\n\n"
            f"Output ONLY a JSON object with a single 'topics' key — array of strings, no scenes,"
            f" no descriptions:\n"
            f"  {{\"topics\": [\"The Time Wally West [verb] ...\", ...]}}\n\n"
            f"No commentary, no markdown."
        )

        system_prompt = TEMPLATE_SYSTEM_PROMPTS.get("zerotier_private", "").strip()
        if not system_prompt:
            raise HTTPException(503, "zerotier_private template prompt missing")

        try:
            grok = GrokClient()
        except GrokAuthError as e:
            raise HTTPException(503, f"grok_auth_failed: {e}")

        try:
            text = grok.complete(system_prompt, topic_user_prompt, max_tokens=1500, temperature=0.95)
        except GrokAuthError as e:
            raise HTTPException(503, f"grok_auth_failed: {e}")

        # Parse the response. Grok sometimes wraps in ```json fences.
        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
            cleaned = re.sub(r"\s*```\s*$", "", cleaned)
        topics: list[str] = []
        try:
            parsed = json.loads(cleaned)
            raw_topics = parsed.get("topics") if isinstance(parsed, dict) else parsed
            if isinstance(raw_topics, list):
                topics = [str(t).strip() for t in raw_topics if str(t).strip()]
        except Exception:
            # Fallback: extract lines that look like titles
            for line in cleaned.splitlines():
                m = re.search(r'"(The Time Wally West [^"]+)"', line)
                if m:
                    topics.append(m.group(1).strip())

        # Dedupe + drop topics that match already-uploaded too closely
        seen: set[str] = set()
        deduped: list[str] = []
        for t in topics:
            key = re.sub(r"\s+", " ", t.lower()).strip()
            if key in seen:
                continue
            seen.add(key)
            # Skip titles that overlap an existing upload (≥4 word matches)
            t_words = set(w for w in re.split(r"\W+", key) if len(w) >= 5)
            duplicate = False
            for existing in already_uploaded:
                e_words = set(w for w in re.split(r"\W+", existing.lower()) if len(w) >= 5)
                if len(t_words & e_words) >= 4:
                    duplicate = True
                    break
            if not duplicate:
                deduped.append(t)

        baseline = {
            "channel_top_lr": winners[0][1] if winners else 0.0,
            "channel_avg_lr": (
                round(sum(lr for _, lr in valid) / len(valid), 2) if valid else 0.0
            ),
            "uploads_considered": len(valid),
        }

        return {
            "ok": True,
            "topics": deduped[:body.count],
            "raw_count": len(topics),
            "baseline": baseline,
        }

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
            "topic": (body.topic or result.get("title") or "").strip(),
            "predicted_score": body.predicted_score,
            "predicted_like_rate": body.predicted_like_rate,
            **result,
        }
        try:
            (workspace / "result.json").write_text(
                json.dumps(result_payload, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

        # Phase 3: append prediction record so we can cross-reference with
        # actual YouTube outcomes once the user uploads + Catalyst harvests.
        try:
            _append_prediction_log({
                "job_id": job_id,
                "ts": int(__import__("time").time()),
                "topic": (body.topic or "").strip(),
                "title": result.get("title", ""),
                "predicted_score": body.predicted_score,
                "predicted_like_rate": body.predicted_like_rate,
                "scene_count": result.get("scene_count"),
                "duration_total_sec": result.get("duration_total_sec"),
                "fal_cost_estimate_usd": result.get("fal_cost_estimate_usd"),
            })
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

    # ────────────────────────────────────────────────────────────────────
    # Phase 4.5: per-scene approval flow.
    #
    # Stage 1: POST /render-stills
    #   - Generates 8 seedream stills in parallel (~$0.32, 60s).
    #   - Returns {job_id, scenes: [{scene_index, scene_id, caption,
    #     narration, duration, still_url, visual_prompt_preview}]}
    #   - User reviews stills before committing to i2v + compose.
    #
    # Stage 2 (per-scene as needed): POST /render-still-one
    #   - Regenerates a single still given {job_id, scene_index, optional
    #     custom_prompt to override the visual description}. Caches the
    #     other 7. ~$0.04 per regen.
    #
    # Stage 3: POST /render-finalize
    #   - Given a job_id with all 8 stills approved, runs Pixverse i2v +
    #     MiniMax narration + ffmpeg compose. ~$1.86, 5-7 min.
    #   - Returns the final MP4.
    #
    # Stills are served via GET /jobs/{job_id}/stills/{filename}.
    # ────────────────────────────────────────────────────────────────────
    @router.post("/render-stills")
    async def zt_render_stills(body: ZTStillsOnlyRequest, user: dict = auth_dep):
        _gate_admin(user)

        # Validate JSON parses
        try:
            parsed = json.loads(body.script_json)
        except Exception as e:
            raise HTTPException(400, f"script_json must be valid JSON: {e}")
        if not isinstance(parsed, dict) or not parsed.get("scenes"):
            raise HTTPException(400, "script_json must be an object with a non-empty 'scenes' array")

        job_id = uuid.uuid4().hex[:12]
        workspace = ZT_OUTPUT_ROOT / job_id
        workspace.mkdir(parents=True, exist_ok=True)
        try:
            (workspace / "script.json").write_text(body.script_json, encoding="utf-8")
        except Exception:
            pass

        try:
            result = render_stills_only(
                script_json=parsed,
                workspace=workspace,
            )
        except ZTRenderError as e:
            raise HTTPException(500, f"stills_failed: {e}")
        except Exception as e:
            raise HTTPException(500, f"stills_failed: {type(e).__name__}: {e}")

        # Persist a partial result.json so /jobs/{id} can return progress.
        partial = {
            "job_id": job_id,
            "stage": "stills_done",
            "topic": (body.topic or result.get("title") or "").strip(),
            "predicted_score": body.predicted_score,
            "predicted_like_rate": body.predicted_like_rate,
            **result,
        }
        try:
            (workspace / "result.json").write_text(
                json.dumps(partial, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

        # Annotate scene rows with absolute still URLs so frontend can render previews.
        scenes_with_url = []
        for s in result.get("scenes", []):
            scenes_with_url.append({
                **s,
                "still_url": f"/api/zerotier-private/jobs/{job_id}/stills/{s['still_filename']}",
            })

        return {
            "ok": True,
            "job_id": job_id,
            "title": result.get("title"),
            "scene_count": result.get("scene_count"),
            "scenes": scenes_with_url,
            "fal_cost_so_far_usd": result.get("fal_cost_estimate_usd_so_far", 0.32),
        }

    @router.post("/render-still-one")
    async def zt_render_still_one(body: ZTRegenStillRequest, user: dict = auth_dep):
        _gate_admin(user)
        if not body.job_id.replace("_", "").isalnum() or len(body.job_id) > 32:
            raise HTTPException(400, "bad_job_id")
        workspace = ZT_OUTPUT_ROOT / body.job_id
        if not workspace.exists():
            raise HTTPException(404, "job_not_found")
        try:
            result = regenerate_one_still(
                workspace=workspace,
                scene_index=body.scene_index,
                custom_prompt=body.custom_prompt,
            )
        except ZTRenderError as e:
            raise HTTPException(400, f"regen_failed: {e}")
        except Exception as e:
            raise HTTPException(500, f"regen_failed: {type(e).__name__}: {e}")
        return {
            "ok": True,
            "job_id": body.job_id,
            **result,
            "still_url": f"/api/zerotier-private/jobs/{body.job_id}/stills/{result['still_filename']}",
        }

    async def _run_finalize_background(job_id: str, workspace: Path, final_filename: str) -> None:
        """Background task for the i2v + TTS + compose stage."""
        try:
            result = await asyncio.to_thread(
                render_finalize,
                workspace=workspace,
                final_filename=final_filename,
            )
            # Merge with existing partial result.json (preserves prediction metadata)
            prior = {}
            try:
                prior = json.loads((workspace / "result.json").read_text(encoding="utf-8"))
            except Exception:
                pass
            result_payload = {
                **prior,
                "job_id": job_id,
                "stage": "done",
                "status": "done",
                **result,
                "mp4_url": f"/api/zerotier-private/jobs/{job_id}/mp4",
            }
            try:
                (workspace / "result.json").write_text(
                    json.dumps(result_payload, indent=2),
                    encoding="utf-8",
                )
            except Exception:
                pass
            # Phase 3: append prediction record
            try:
                _append_prediction_log({
                    "job_id": job_id,
                    "ts": int(time.time()),
                    "topic": str(prior.get("topic", "") or "").strip(),
                    "title": result.get("title", ""),
                    "predicted_score": prior.get("predicted_score"),
                    "predicted_like_rate": prior.get("predicted_like_rate"),
                    "scene_count": result.get("scene_count"),
                    "duration_total_sec": result.get("duration_total_sec"),
                    "fal_cost_estimate_usd": result.get("fal_cost_estimate_usd"),
                })
            except Exception:
                pass
            _zt_jobs_status[job_id].update({
                "status": "ready",
                "finished_at": time.time(),
                "result": result_payload,
            })
        except Exception as e:
            _zt_jobs_status[job_id].update({
                "status": "failed",
                "finished_at": time.time(),
                "error": f"{type(e).__name__}: {e}",
            })

    @router.post("/render-finalize")
    async def zt_render_finalize(body: ZTFinalizeRequest, user: dict = auth_dep):
        """Phase 4.5b: returns immediately with {job_id, status:'pending'}.
        The frontend polls /jobs/{job_id}/status until ready. Same pattern
        as /render-stills but for the longer i2v + TTS + compose stage."""
        _gate_admin(user)
        if not body.job_id.replace("_", "").isalnum() or len(body.job_id) > 32:
            raise HTTPException(400, "bad_job_id")
        workspace = ZT_OUTPUT_ROOT / body.job_id
        if not workspace.exists():
            raise HTTPException(404, "job_not_found")

        final_filename = _safe_filename(
            body.final_filename or f"ZeroTier_{body.job_id}.mp4",
            default=f"ZeroTier_{body.job_id}.mp4",
        )

        # Re-use the SAME job_id since the workspace is already keyed to it
        # from the stills stage. The status registry overwrites the "stills"
        # entry with "finalize" — once stills are ready we don't need to
        # poll for them anymore.
        _zt_jobs_status[body.job_id] = {
            "stage": "finalize",
            "status": "pending",
            "started_at": time.time(),
        }
        task = asyncio.create_task(
            _run_finalize_background(body.job_id, workspace, final_filename)
        )
        _zt_jobs_status[body.job_id]["_task"] = task

        return {
            "ok": True,
            "job_id": body.job_id,
            "status": "pending",
            "stage": "finalize",
            "poll_url": f"/api/zerotier-private/jobs/{body.job_id}/status",
        }

    @router.get("/jobs/{job_id}/status")
    async def zt_job_polling_status(job_id: str, user: dict = auth_dep):
        """Phase 4.5b: poll endpoint for in-flight render-stills /
        render-finalize jobs. Returns the current stage + status, plus the
        full result payload when status='ready'."""
        _gate_admin(user)
        if not job_id.replace("_", "").isalnum() or len(job_id) > 32:
            raise HTTPException(400, "bad_job_id")
        entry = _zt_jobs_status.get(job_id)
        if not entry:
            # Maybe the server restarted but the result.json is on disk
            rp = ZT_OUTPUT_ROOT / job_id / "result.json"
            if rp.exists():
                try:
                    return {
                        "ok": True,
                        "job_id": job_id,
                        "status": "ready",
                        "stage": "done",
                        "result": json.loads(rp.read_text(encoding="utf-8")),
                    }
                except Exception:
                    pass
            raise HTTPException(404, "job_not_found")
        # Don't include the strong-ref task in the response payload
        public = {k: v for k, v in entry.items() if not k.startswith("_")}
        return {"ok": True, "job_id": job_id, **public}

    @router.get("/jobs/{job_id}/stills/{filename}")
    async def zt_job_still(job_id: str, filename: str):
        # Auth-less by design: <img src> can't send Authorization headers, so
        # we gate on the random 12-hex job_id (~3e14 entropy) which acts as an
        # opaque capability token. The user already has the job_id only via
        # the auth-gated render-stills response.
        if not job_id.replace("_", "").isalnum() or len(job_id) > 32:
            raise HTTPException(400, "bad_job_id")
        if not filename.endswith(".png") or "/" in filename or ".." in filename:
            raise HTTPException(400, "bad_filename")
        path = ZT_OUTPUT_ROOT / job_id / "stills" / filename
        if not path.exists():
            raise HTTPException(404, "not_found")
        return FileResponse(str(path), media_type="image/png")

    @router.get("/jobs/{job_id}/mp4")
    async def zt_job_mp4(job_id: str):
        # Auth-less for the same reason as /stills/{filename} — browsers
        # can't send Authorization on direct media downloads / video tag.
        if not job_id.replace("_", "").isalnum() or len(job_id) > 32:
            raise HTTPException(400, "bad_job_id")
        ws = ZT_OUTPUT_ROOT / job_id
        if not ws.exists():
            raise HTTPException(404, "job_not_found")
        # Prefer the FINAL muxed MP4 ("ZeroTier_<jobid>.mp4") over any
        # intermediate files (silent_combined.mp4 — video-only, no narration).
        # The previous glob-and-pick-first behavior could serve the silent
        # intermediate, which is what Casey hit ('zero sound design or VO').
        preferred = ws / f"ZeroTier_{job_id}.mp4"
        if preferred.is_file() and preferred.stat().st_size > 1024:
            return FileResponse(str(preferred), media_type="video/mp4", filename=preferred.name)
        # Fall back to any non-intermediate .mp4 in the root
        EXCLUDE = {"silent_combined.mp4"}
        candidates = sorted(
            (p for p in ws.glob("*.mp4")
             if p.is_file() and p.name not in EXCLUDE and p.stat().st_size > 1024),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            return FileResponse(str(candidates[0]), media_type="video/mp4", filename=candidates[0].name)
        raise HTTPException(404, "mp4_not_found")

    # ────────────────────────────────────────────────────────────────────
    # Phase 4.5c: list past jobs so user can resume a project they left.
    # Reads result.json from each workspace directory.
    # ────────────────────────────────────────────────────────────────────
    @router.get("/jobs")
    async def zt_list_jobs(user: dict = auth_dep, limit: int = 30):
        _gate_admin(user)
        limit = max(1, min(100, int(limit or 30)))
        if not ZT_OUTPUT_ROOT.exists():
            return {"ok": True, "jobs": []}
        rows: list[dict] = []
        # Iterate per-job workspace dirs, newest first by mtime
        try:
            dirs = sorted(
                [p for p in ZT_OUTPUT_ROOT.iterdir() if p.is_dir()],
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
        except Exception:
            dirs = []
        for d in dirs[:limit * 2]:
            job_id = d.name
            if not job_id.replace("_", "").isalnum() or len(job_id) > 32:
                continue
            rj = d / "result.json"
            if not rj.exists():
                continue
            try:
                data = json.loads(rj.read_text(encoding="utf-8"))
            except Exception:
                continue
            scenes_meta = data.get("scenes") or []
            # Find the first still filename for thumbnail preview
            stills_dir = d / "stills"
            first_still = None
            if scenes_meta and isinstance(scenes_meta, list):
                first = scenes_meta[0]
                if isinstance(first, dict):
                    fn = first.get("still_filename") or (first.get("scene_id") and f"{first['scene_id']}.png")
                    if fn and (stills_dir / fn).exists():
                        first_still = fn
            if not first_still:
                # Fallback: first PNG in the stills dir
                if stills_dir.exists():
                    for p in sorted(stills_dir.glob("*.png")):
                        if p.is_file():
                            first_still = p.name
                            break
            stage = data.get("stage") or ("done" if data.get("mp4_url") else "stills_done")
            mtime = d.stat().st_mtime
            rows.append({
                "job_id": job_id,
                "title": data.get("title", ""),
                "topic": data.get("topic", ""),
                "stage": stage,
                "scene_count": data.get("scene_count", len(scenes_meta)),
                "predicted_score": data.get("predicted_score"),
                "predicted_like_rate": data.get("predicted_like_rate"),
                "duration_total_sec": data.get("duration_total_sec"),
                "fal_cost_estimate_usd": data.get("fal_cost_estimate_usd"),
                "thumbnail_url": (
                    f"/api/zerotier-private/jobs/{job_id}/stills/{first_still}"
                    if first_still else None
                ),
                "mp4_url": (
                    f"/api/zerotier-private/jobs/{job_id}/mp4"
                    if (d / f"ZeroTier_{job_id}.mp4").exists()
                    else None
                ),
                "updated_at": mtime,
            })
            if len(rows) >= limit:
                break
        return {"ok": True, "jobs": rows}

    @router.get("/jobs/{job_id}/state")
    async def zt_job_state(job_id: str, user: dict = auth_dep):
        """Resume-a-job endpoint: returns enough state to re-hydrate the
        modal — the original script_json, the persisted scenes (with full
        still URLs), and any final MP4 URL."""
        _gate_admin(user)
        if not job_id.replace("_", "").isalnum() or len(job_id) > 32:
            raise HTTPException(400, "bad_job_id")
        ws = ZT_OUTPUT_ROOT / job_id
        if not ws.exists():
            raise HTTPException(404, "job_not_found")

        result_path = ws / "result.json"
        scenes_path = ws / "scenes.json"
        script_path = ws / "script.json"
        result = {}
        scenes_data = {}
        script_text = ""
        try:
            if result_path.exists():
                result = json.loads(result_path.read_text(encoding="utf-8"))
        except Exception:
            pass
        try:
            if scenes_path.exists():
                scenes_data = json.loads(scenes_path.read_text(encoding="utf-8"))
        except Exception:
            pass
        try:
            if script_path.exists():
                script_text = script_path.read_text(encoding="utf-8")
        except Exception:
            pass

        # Build scene rows with still URLs
        scenes_with_url: list[dict] = []
        normalized_scenes = scenes_data.get("scenes") or []
        for i, s in enumerate(normalized_scenes):
            if not isinstance(s, dict):
                continue
            sid = str(s.get("id", "") or "")
            still_filename = f"{sid}.png"
            still_path = ws / "stills" / still_filename
            if not still_path.exists():
                continue
            scenes_with_url.append({
                "scene_index": i,
                "scene_id": sid,
                "caption": s.get("caption", ""),
                "narration": s.get("narration", ""),
                "duration": float(s.get("duration", 4.0)),
                "still_url": f"/api/zerotier-private/jobs/{job_id}/stills/{still_filename}",
                "visual_prompt_preview": (s.get("visual_prompt", "")[:200] + ("…" if len(s.get("visual_prompt", "")) > 200 else "")),
            })

        return {
            "ok": True,
            "job_id": job_id,
            "title": result.get("title") or scenes_data.get("title", ""),
            "topic": result.get("topic", ""),
            "stage": result.get("stage", "stills_done"),
            "predicted_score": result.get("predicted_score"),
            "predicted_like_rate": result.get("predicted_like_rate"),
            "scene_count": result.get("scene_count", len(normalized_scenes)),
            "scenes": scenes_with_url,
            "script_json": script_text,
            "mp4_url": (
                f"/api/zerotier-private/jobs/{job_id}/mp4"
                if (ws / f"ZeroTier_{job_id}.mp4").exists() else None
            ),
            "duration_total_sec": result.get("duration_total_sec"),
            "fal_cost_estimate_usd": result.get("fal_cost_estimate_usd"),
        }

    # ────────────────────────────────────────────────────────────────────
    # Phase 3: predictions-vs-actuals learning surface.
    #
    # GET /predictions returns the last N logged predictions (from the
    # JSONL log) joined against actual YouTube outcomes (views, likes,
    # like-rate). The match is fuzzy: predictions log a `title` (the Grok
    # output's title), and we look it up in the user's connection-store
    # uploaded_videos by exact-normalized then word-overlap-≥0.6 fuzzy.
    #
    # The frontend "Calibration" panel displays these so the user can see
    # which predictions hit and which missed. Future Phase 3.5 reads this
    # log and reweights the heuristic v1 → v2 → ... or feeds Grok with
    # actuals as training context.
    # ────────────────────────────────────────────────────────────────────
    @router.get("/predictions")
    async def zt_predictions(user: dict = auth_dep, limit: int = 30):
        _gate_admin(user)
        limit = max(1, min(100, int(limit or 30)))
        records = _read_prediction_log(limit=limit)
        if not records:
            return {"ok": True, "predictions": [], "match_summary": {}}

        # Pull the user's ZeroTier uploaded_videos to attempt match.
        # We'd ideally use the same _list_connected_youtube_channels_for_user
        # the rest of the backend uses, but to keep this router decoupled we
        # just read the connection store directly.
        uploads: list[dict] = []
        try:
            from youtube import _load_youtube_connections, _youtube_bucket_for_user
            user_id = str(user.get("id", "") or user.get("user_id", "") or "")
            if user_id:
                _load_youtube_connections()
                bucket = _youtube_bucket_for_user(user_id)
                channels = (bucket or {}).get("channels") or {}
                # ZeroTier channel id is hard-coded for the private niche
                ZT_ID = "UC9Gth_4MVet6rdPH7MHJf-g"
                ch = channels.get(ZT_ID) or {}
                snap = (ch.get("analytics_snapshot") or {})
                uploads = list(snap.get("uploaded_videos") or [])
        except Exception:
            uploads = []

        def _match(title: str) -> dict | None:
            t_norm = _normalize_title(title)
            if not t_norm:
                return None
            # Try exact-normalized first
            for v in uploads:
                if _normalize_title(str(v.get("title", "") or "")) == t_norm:
                    return v
            # Fuzzy fallback ≥0.6 word overlap
            best = None
            best_score = 0.0
            for v in uploads:
                s = _title_word_overlap(title, str(v.get("title", "") or ""))
                if s > best_score and s >= 0.6:
                    best_score = s
                    best = v
            return best

        joined: list[dict] = []
        hits = 0
        for rec in records:
            title = str(rec.get("title", "") or "").strip()
            matched = _match(title)
            actual_lr = None
            actual_views = None
            actual_likes = None
            video_id = None
            if matched:
                hits += 1
                video_id = str(matched.get("video_id", "") or "") or None
                actual_views = int(float(matched.get("views", 0) or 0))
                actual_likes = int(float(matched.get("likes", 0) or 0))
                if actual_views >= 50:
                    actual_lr = round((actual_likes / actual_views) * 100, 2)
            joined.append({
                **rec,
                "matched": bool(matched),
                "video_id": video_id,
                "actual_views": actual_views,
                "actual_likes": actual_likes,
                "actual_like_rate": actual_lr,
                "delta_lr": (
                    round(actual_lr - float(rec.get("predicted_like_rate", 0) or 0), 2)
                    if actual_lr is not None and rec.get("predicted_like_rate") is not None
                    else None
                ),
            })

        return {
            "ok": True,
            "predictions": joined,
            "match_summary": {
                "total": len(joined),
                "matched": hits,
                "unmatched": len(joined) - hits,
            },
        }

    return router
