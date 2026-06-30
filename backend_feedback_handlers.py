"""Feedback and admin quota route handlers for the Studio API."""

from pathlib import Path
import json
import time

from fastapi import Depends, HTTPException


def build_submit_feedback_handler(
    *,
    feedback_request_model,
    require_auth,
    training_data_dir,
    clip_text,
    log,
):
    async def submit_feedback(req: feedback_request_model, user: dict = Depends(require_auth)):
        try:
            rating = int(req.rating)
        except (TypeError, ValueError):
            raise HTTPException(400, "rating must be an integer")
        if rating < 1 or rating > 5:
            raise HTTPException(400, "rating must be between 1 and 5")
        row = {
            "type": "studio_feedback",
            "ts": time.time(),
            "user_id": str(user.get("id", "") or ""),
            "email": str(user.get("email", "") or ""),
            "job_id": clip_text(str(req.job_id or "").strip(), 120),
            "rating": rating,
            "comment": clip_text(str(req.comment or "").strip(), 2000),
            "template": clip_text(str(req.template or "").strip(), 120),
            "language": clip_text(str(req.language or "").strip(), 40),
            "feature": clip_text(str(req.feature or "").strip(), 120),
        }
        try:
            feedback_path = Path(training_data_dir) / "studio_feedback.jsonl"
            feedback_path.parent.mkdir(parents=True, exist_ok=True)
            with feedback_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=True, sort_keys=True) + "\n")
        except Exception as exc:
            log.warning("Failed to persist Studio feedback: %s", exc)
            raise HTTPException(500, "Failed to record feedback") from exc
        return {"ok": True}

    return submit_feedback


def build_admin_youtube_quota_handler(*, require_auth, admin_emails: set[str]):
    async def admin_youtube_quota(user: dict = Depends(require_auth)):
        if str(user.get("email", "") or "") not in admin_emails:
            raise HTTPException(403, "Admin access required")
        import youtube_quota

        return {
            "ok": True,
            "quota": await youtube_quota.breakdown(),
            "history": await youtube_quota.history(days=7),
        }

    return admin_youtube_quota
