"""
History Rewind (Private) router — admin-only Catalyst-fed surface for
the History Rewind channel (UCHmwsIGud6CeZ3CIs5cuaUA, Casey's 9-hour
sleep documentary channel). Mirrors zerotier_private_router and
alt_history_private_router patterns.

The actual RENDER pipeline is the existing long_form_router sleep_doc
flow (which already handles HR 9hr renders via /api/long-form/*) — this
panel doesn't duplicate the render code, it just adds Catalyst-driven
topic discovery + analytics tied to the actual HR channel performance.

Topic format enforced: "[Topic] | History for Sleep | 9 Hours" — the
decoded winner pattern from D:/recaps/history_rewind/competitor_decode_2026-05-07.md
(n=264 LF analyzed; 'History for Sleep' suffix wins 543 v/d vs Casey's
'Rise and Fall' format at 16 v/d).

Routes:
  POST /api/history-rewind-private/generate-topics
       Pulls HR channel uploads, computes view-velocity, sends winners
       + losers + already-uploaded + competitor whitespace empires
       (Khmer, Phoenicians, Inca, Mughal, Persian, Hittite — zero
       hits in the 264-vid corpus) to Grok with the locked HR sleep-doc
       system prompt. Returns {topics, channel_baseline}.
"""
from __future__ import annotations

import json
import re
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from skeleton_ai.scripting_grok import GrokClient, GrokAuthError


HISTORY_REWIND_CHANNEL_ID = "UCHmwsIGud6CeZ3CIs5cuaUA"


# Locked HR sleep-doc system prompt — mirrors what long_form/prompts/channels.py
# has for history_rewind, but specialized for topic-list generation.
HR_SLEEP_DOC_SYSTEM_PROMPT = (
    "You generate 9-hour sleep documentary topic ideas for the "
    "'History Rewind' YouTube channel. The channel produces calm, "
    "methodical, 9-hour sleep-paced documentaries about ancient empires, "
    "lost civilizations, and historical eras. Each topic must:\n\n"
    "  - Title format STRICTLY: '[Empire/Topic] — [Hook noun phrase] | "
    "    History for Sleep | 9 Hours'  (per "
    "    competitor_decode_2026-05-07.md: n=93 hits at 543 v/d for that "
    "    suffix; n=2 hits at 16 v/d for 'Rise and Fall ... Full "
    "    Documentary' — strictly avoid the latter pattern).\n"
    "  - Choose empires / civilizations with ZERO competitor coverage in "
    "    the analyzed corpus where possible: Khmer, Phoenicians, Inca, "
    "    Aztec, Maya, Mughal, Achaemenid Persian, Hittite, Carthaginian, "
    "    Olmec, Etruscan, Mycenaean, Minoan. Saturated empires (Roman, "
    "    Greek, Egypt, Mongol, Ottoman) are deprioritized.\n"
    "  - Include a SPECIFIC narrative hook in the noun phrase (e.g. "
    "    'Angkor's Dark Secret', 'The Sea Empire That Wrote the World', "
    "    'The Last Days of the Sun Kings').\n"
    "  - Use real names always (empires, dynasties, capital cities, "
    "    famous rulers, wars).\n\n"
    "Output STRICT JSON with a single 'topics' key — array of strings:\n"
    "  {\"topics\": [\"<title>\", ...]}\n"
    "No markdown fences, no commentary."
)


class HRGenerateTopicsRequest(BaseModel):
    count: int = 8


def build_history_rewind_private_router(
    *,
    require_auth: Callable[..., dict] | None = None,
    is_admin_user: Callable[[dict], bool] | None = None,
) -> APIRouter:
    """Construct the History Rewind (Private) router. Mounts at
    /api/history-rewind-private. Admin-only.

    The render path stays on /api/long-form/* (existing sleep_doc
    pipeline). This router only adds Catalyst-aware topic gen.
    """
    router = APIRouter(prefix="/api/history-rewind-private", tags=["history-rewind-private"])
    auth_dep = Depends(require_auth) if require_auth else Depends(lambda: {"user_id": "anon"})

    def _gate_admin(user: dict) -> None:
        if not is_admin_user:
            return
        try:
            if is_admin_user(user):
                return
        except Exception:
            pass
        raise HTTPException(403, "history-rewind-private is admin-only")

    @router.post("/generate-topics")
    async def generate_hr_topics(
        body: HRGenerateTopicsRequest,
        user: dict = auth_dep,
    ):
        """Pull HR uploads, score by view-velocity (HR is sleep-doc, not
        like-rate driven), ask Grok for N fresh sleep-doc topics tuned
        to channel + competitor whitespace.
        """
        _gate_admin(user)

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
                ch = channels.get(HISTORY_REWIND_CHANNEL_ID) or {}
                snap = (ch.get("analytics_snapshot") or {})
                uploads = list(snap.get("uploaded_videos") or [])
        except Exception:
            uploads = []

        # Score by view-velocity (views per day since publish) — HR
        # cares about long-tail sleep traffic, not like-rate spikes.
        import datetime as _dt
        scored: list[tuple[str, float, int]] = []
        for v in uploads:
            views = int(float((v or {}).get("views", 0) or 0))
            title = str((v or {}).get("title", "") or "").strip()
            pub_at = str((v or {}).get("published_at", "") or "")
            if not title or views < 50:
                continue
            days = 1
            try:
                if pub_at:
                    pub = _dt.datetime.fromisoformat(pub_at.replace("Z", "+00:00"))
                    delta = (_dt.datetime.now(_dt.timezone.utc) - pub).days
                    days = max(1, delta)
            except Exception:
                days = 30  # fallback
            vps = views / days
            scored.append((title, round(vps, 1), views))
        scored.sort(key=lambda x: x[1], reverse=True)

        winners = scored[:5]
        losers = scored[-3:] if len(scored) >= 8 else scored[-min(3, len(scored)):]
        already_uploaded = [t for t, _, _ in scored]

        winners_block = "\n".join(
            [f'  - "{t}" — {vps:.1f} views/day ({v:,} total)' for t, vps, v in winners]
        ) or "  (none yet — channel may be new or unsynced)"
        losers_block = "\n".join(
            [f'  - "{t}" — {vps:.1f} views/day ({v:,} total)' for t, vps, v in losers]
        ) or "  (none yet)"
        already_block = "\n".join([f"  - {t}" for t in already_uploaded]) or "  (none)"

        topic_user_prompt = (
            "Generating fresh 9-hour sleep documentary topic ideas for "
            "the History Rewind channel.\n\n"
            "=== This channel's actual data (sorted by views/day) ===\n\n"
            f"TOP PERFORMERS:\n{winners_block}\n\n"
            f"UNDER-PERFORMERS:\n{losers_block}\n\n"
            f"ALREADY UPLOADED (do NOT propose these or close variants):\n"
            f"{already_block}\n\n"
            "=== Competitor whitespace (zero coverage in 264-video corpus) ===\n\n"
            "  - Khmer Empire (Angkor, water engineering, Jayavarman VII)\n"
            "  - Phoenicians (Carthage, alphabet origins, sea trade)\n"
            "  - Inca Empire (Cusco, Sun Kings, Spanish conquest)\n"
            "  - Aztec / Mexica civilization (Tenochtitlan)\n"
            "  - Maya civilization (Classic-period collapse)\n"
            "  - Mughal India (Akbar, Taj, Aurangzeb)\n"
            "  - Achaemenid Persia (Cyrus, Darius)\n"
            "  - Hittite Empire (bronze-age superpower)\n"
            "  - Carthaginian Empire (pre-Punic-War rise)\n"
            "  - Olmec / Etruscan / Mycenaean / Minoan civilizations\n\n"
            "=== Your task ===\n\n"
            f"Generate {body.count} FRESH 9-hour sleep documentary topic "
            "titles in the locked format:\n"
            "  '[Empire/Topic] — [Hook noun phrase] | History for Sleep | 9 Hours'\n\n"
            "Prefer the whitespace empires above. STRICTLY avoid 'Rise "
            "and Fall' / 'Full Documentary' patterns (zero-velocity in "
            "the corpus). Each title 8-15 words.\n\n"
            "Return STRICT JSON only:\n"
            '  {"topics": ["<title>", ...]}\n'
            "No markdown fences, no preamble."
        )

        try:
            grok = GrokClient()
        except GrokAuthError as e:
            raise HTTPException(503, f"grok_auth_failed: {e}")

        try:
            text = grok.complete(
                HR_SLEEP_DOC_SYSTEM_PROMPT,
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
            for line in cleaned.splitlines():
                m = re.search(r'"([^"]{30,})"', line)
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
            "channel_top_vps": winners[0][1] if winners else 0.0,
            "channel_avg_vps": (
                round(sum(vps for _, vps, _ in scored) / len(scored), 1) if scored else 0.0
            ),
            "uploads_considered": len(scored),
            "channel_id": HISTORY_REWIND_CHANNEL_ID,
        }

        return {
            "topics": deduped[:body.count],
            "raw_count": len(topics),
            "channel_baseline": baseline,
        }

    return router
