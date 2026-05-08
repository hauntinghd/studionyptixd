"""
Alt-History (Private) router — admin-only Catalyst-fed surface for the
Cryptic Science channel (where Casey's alt-history-battles content is
uploaded). Mirrors the zerotier_private_router pattern: Catalyst pulls
the channel's actual analytics, Grok generates fresh topic ideas tuned
to what's actually working there, and the panel shows recent renders +
predicted virality scores.

The actual RENDER pipeline is the existing skeleton_ai_router (which
already powers the public Alt-Battles niche in the Create tab) — this
panel doesn't duplicate the render code, it just provides the
admin-only Catalyst-driven topic discovery + analytics view that makes
private testing more focused.

Routes:
  POST /api/alt-history-private/generate-topics
       Pulls Cryptic Science uploads from the OAuth connection store,
       computes like-rates, sends winners + losers + already-uploaded
       to Grok with the alt-battles system prompt and asks for N fresh
       topic ideas. Returns {topics, channel_baseline}.

Wired into backend.py the same way zerotier_private_router is.
"""
from __future__ import annotations

import json
import re
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from skeleton_ai.scripting_grok import GrokClient, GrokAuthError


# Cryptic Science channel ID (per project_channel_lanes_locked.md +
# long_form/prompts/channels.py registry).
CRYPTIC_SCIENCE_CHANNEL_ID = "UCOHnksm14B-9AqGhlpRxG5A"


# Locked alt-battles system prompt — derived from Casey's CreatePanel
# header documentation + the long_form Empire Magnates emulation.
# Grammar: Kings-and-Generals / Total War / Ridley Scott painterly
# cinematic battle illustration. Period-correct gear. NOT photoreal,
# NOT modern.
ALT_BATTLES_SYSTEM_PROMPT = (
    "You generate alt-history battle topic ideas for a YouTube shorts "
    "channel called 'Cryptic Science' (parent channel that hosts the "
    "Alt-History Battles niche). The format is COUNTERFACTUAL military "
    "matchups — what if [historical force A] fought [historical force B] "
    "in a setting that didn't actually happen? Topic ideas must:\n\n"
    "  - Pit two well-known historical forces / leaders / nations against "
    "    each other in a clash that never occurred (or only occurred "
    "    briefly).\n"
    "  - Vary the matchup era pairings — don't propose 8 Roman ideas. "
    "    Mix classical / medieval / gunpowder / modern.\n"
    "  - Have a clear narrative hook (terrain, technology gap, leader "
    "    psychology, supply lines, weather) that promises a specific "
    "    answer rather than a vague 'who would win.'\n"
    "  - Use real names always (commanders, units, places, dates).\n\n"
    "Output STRICT JSON with a single 'topics' key — array of strings:\n"
    "  {\"topics\": [\"<topic title>\", ...]}\n"
    "Each topic title is 6-14 words, no markdown, no commentary."
)


class AltHistoryGenerateTopicsRequest(BaseModel):
    count: int = 8


def build_alt_history_private_router(
    *,
    require_auth: Callable[..., dict] | None = None,
    is_admin_user: Callable[[dict], bool] | None = None,
) -> APIRouter:
    """Construct the Alt-History (Private) router. Mounts at
    /api/alt-history-private. Admin-only — gates per request.

    The render path stays on /api/skeleton-ai/* (existing alt-battles
    pipeline). This router only adds the Catalyst-aware topic generator
    so suggestions are tuned to the channel's actual top performers.
    """
    router = APIRouter(prefix="/api/alt-history-private", tags=["alt-history-private"])
    auth_dep = Depends(require_auth) if require_auth else Depends(lambda: {"user_id": "anon"})

    def _gate_admin(user: dict) -> None:
        if not is_admin_user:
            return
        try:
            if is_admin_user(user):
                return
        except Exception:
            pass
        raise HTTPException(403, "alt-history-private is admin-only")

    @router.post("/generate-topics")
    async def generate_alt_topics(
        body: AltHistoryGenerateTopicsRequest,
        user: dict = auth_dep,
    ):
        """Pull Cryptic Science uploads, score by like-rate, ask Grok for
        N fresh alt-battles topics tuned to actual channel performance.
        Mirrors zerotier_private_router's /generate-topics route shape.
        """
        _gate_admin(user)

        # Pull uploads from the YouTube connection store (same path the
        # ZT private router uses). If the channel isn't synced yet we
        # still generate topics — the prompt just falls back to the
        # generic alt-battles system prompt without channel context.
        uploads: list[dict] = []
        try:
            from youtube import (
                _load_youtube_connections,
                _youtube_bucket_for_user,
            )
            user_id = str(user.get("id", "") or user.get("user_id", "") or "")
            if user_id:
                _load_youtube_connections()
                bucket = _youtube_bucket_for_user(user_id)
                channels = (bucket or {}).get("channels") or {}
                ch = channels.get(CRYPTIC_SCIENCE_CHANNEL_ID) or {}
                snap = (ch.get("analytics_snapshot") or {})
                uploads = list(snap.get("uploaded_videos") or [])
        except Exception:
            uploads = []

        # Compute like-rates + sort
        scored: list[tuple[str, float, int]] = []
        for v in uploads:
            views = int(float((v or {}).get("views", 0) or 0))
            likes = int(float((v or {}).get("likes", 0) or 0))
            title = str((v or {}).get("title", "") or "").strip()
            if not title or views < 50:
                continue
            lr = (likes / views) * 100 if views > 0 else 0.0
            scored.append((title, round(lr, 2), views))
        scored.sort(key=lambda x: x[1], reverse=True)

        # Drop bot-spike outliers (high views with < 0.3% LR)
        valid = [(t, lr, v) for t, lr, v in scored if lr >= 0.3 or len(scored) <= 5]
        winners = valid[:5]
        losers = valid[-3:] if len(valid) >= 8 else valid[-min(3, len(valid)):]
        already_uploaded = [t for t, _, _ in scored]

        winners_block = "\n".join(
            [f'  - "{t}" — {lr:.2f}% like-rate ({v:,} views)' for t, lr, v in winners]
        ) or "  (none yet — channel may be new or unsynced)"
        losers_block = "\n".join(
            [f'  - "{t}" — {lr:.2f}% like-rate ({v:,} views)' for t, lr, v in losers]
        ) or "  (none yet)"
        already_block = "\n".join([f"  - {t}" for t in already_uploaded]) or "  (none)"

        topic_user_prompt = (
            "Generating fresh alt-history battle topic ideas for the "
            "Cryptic Science channel.\n\n"
            "=== This channel's actual data ===\n\n"
            f"TOP PERFORMERS (what's working — highest like-rate):\n"
            f"{winners_block}\n\n"
            f"UNDER-PERFORMERS (avoid this energy):\n{losers_block}\n\n"
            f"ALREADY UPLOADED (do NOT propose these or close variants):\n"
            f"{already_block}\n\n"
            "=== Your task ===\n\n"
            f"Generate {body.count} FRESH alt-history battle topic titles "
            "that vary across eras (classical / medieval / gunpowder / "
            "modern), pit two well-known forces against each other in a "
            "matchup that didn't actually happen, and have a specific "
            "narrative hook (terrain, tech gap, leader psychology, etc.) "
            "rather than a generic 'who would win.'\n\n"
            "Return STRICT JSON only:\n"
            '  {"topics": ["<topic>", ...]}\n'
            "No markdown fences, no preamble, no commentary."
        )

        try:
            grok = GrokClient()
        except GrokAuthError as e:
            raise HTTPException(503, f"grok_auth_failed: {e}")

        try:
            text = grok.complete(
                ALT_BATTLES_SYSTEM_PROMPT,
                topic_user_prompt,
                max_tokens=1500,
                temperature=0.95,
            )
        except GrokAuthError as e:
            raise HTTPException(503, f"grok_auth_failed: {e}")

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
            # Fallback: extract any quoted strings that look like titles
            for line in cleaned.splitlines():
                m = re.search(r'"([^"]{20,})"', line)
                if m:
                    topics.append(m.group(1).strip())

        # Dedupe + drop topics that overlap already-uploaded too closely
        seen: set[str] = set()
        deduped: list[str] = []
        for t in topics:
            key = re.sub(r"\s+", " ", t.lower()).strip()
            if key in seen:
                continue
            seen.add(key)
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
                round(sum(lr for _, lr, _ in valid) / len(valid), 2) if valid else 0.0
            ),
            "uploads_considered": len(scored),
            "channel_id": CRYPTIC_SCIENCE_CHANNEL_ID,
        }

        return {
            "topics": deduped[:body.count],
            "raw_count": len(topics),
            "channel_baseline": baseline,
        }

    return router
