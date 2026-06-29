"""Public Studio utility handlers."""

from __future__ import annotations

import hashlib
import os
import random
import time
from typing import Callable


def build_studio_utility_handlers(
    *,
    youtube_fetch_public_trend_titles: Callable,
    log,
):
    async def studio_shorts_ideas(q: str = "", max_results: int = 8, seed: str = ""):
        query = (q or "").strip()
        if not query:
            return {"ideas": []}
        candidate_cap = 12
        wanted = max(3, min(int(max_results or 8), candidate_cap))
        try:
            candidates = await youtube_fetch_public_trend_titles(query, max_results=candidate_cap)
        except Exception as exc:
            log.warning(f"/api/studio/shorts/ideas upstream failed for q={query!r}: {exc}")
            return {"ideas": []}
        candidates = [str(title).strip() for title in (candidates or []) if title]
        candidates = [title for title in candidates if title]
        if not candidates:
            return {"ideas": []}
        if len(candidates) <= wanted:
            return {"ideas": candidates}
        seed_text = (seed or str(time.time())).encode("utf-8", errors="ignore")
        digest = hashlib.sha256(seed_text + query.encode("utf-8", errors="ignore")).digest()
        rng = random.Random(int.from_bytes(digest[:8], "big", signed=False))
        shuffled = list(candidates)
        rng.shuffle(shuffled)
        return {"ideas": shuffled[:wanted]}

    async def studio_queue_status():
        try:
            import fal_gate

            waiting = int(fal_gate.queue_depth())
            slots_free = int(fal_gate.available_slots())
            pool_size = int(fal_gate.pool_size())
        except Exception:
            waiting = 0
            slots_free = 16
            pool_size = 1
        per_key_cap = int(os.getenv("FAL_CONCURRENT_SOFT_CAP", "16") or "16")
        cap = per_key_cap * max(1, pool_size)
        in_flight = max(0, cap - slots_free)
        eta_sec = int(waiting * 4.0) if waiting > 0 else 0
        saturation_threshold = int(cap * 0.75)
        saturated = bool(waiting > 0 or in_flight >= saturation_threshold)
        return {
            "in_flight": in_flight,
            "waiting": waiting,
            "cap": cap,
            "slots_free": slots_free,
            "pool_size": pool_size,
            "eta_sec": eta_sec,
            "saturated": saturated,
        }

    return studio_shorts_ideas, studio_queue_status
