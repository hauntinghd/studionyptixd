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
    # Phase 3 — learning loop. Frontend includes its heuristic-v1 score so we
    # can log prediction-vs-actual once Catalyst harvests outcomes.
    predicted_score: float | None = None
    predicted_like_rate: float | None = None
    topic: str | None = None


# Where rendered MP4s live. Mirrors skeleton_ai's pattern.
ZT_OUTPUT_ROOT = Path(os.getenv("ZEROTIER_PRIVATE_OUTPUT_ROOT", "zerotier_private/output"))

# Phase 3 — append-only JSONL log of every render's prediction. Each line is
# a JSON object: {job_id, ts, title, predicted_score, predicted_like_rate}.
# Cross-referenced with actual outcomes (views/likes from Catalyst's channel
# sync) by GET /predictions. Lives outside ZT_OUTPUT_ROOT so log clean-ups
# of past renders don't blow away the calibration history.
ZT_PREDICTIONS_LOG = Path(os.getenv("ZEROTIER_PRIVATE_PREDICTIONS_LOG", "zerotier_private/predictions.jsonl"))


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
