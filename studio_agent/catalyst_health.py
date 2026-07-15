"""Catalyst self-learning health audit for Studio Agent skeleton production."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from studio_agent.catalyst_skeleton_reference import (
    BUNDLED_MEMORY_PATH,
    REFS_DIR,
    catalyst_channel_memory_path,
    get_skeleton_visual_directives,
    seed_bundled_reference_memory,
)


def audit_catalyst_self_learning(channel_key: str = "mrskelewelly") -> dict[str, Any]:
    """Report whether Catalyst still-learning is wired and has persisted signals."""
    channel_key = str(channel_key or "").strip() or "mrskelewelly"
    memory_path = catalyst_channel_memory_path()
    learning_records_path = memory_path.parent / "catalyst_learning_records.json"

    bundled_ok = BUNDLED_MEMORY_PATH.is_file()
    refs_videos = sorted(REFS_DIR.glob("*.mp4")) if REFS_DIR.is_dir() else []
    directives = get_skeleton_visual_directives(channel_key)

    runtime_bucket: dict[str, Any] = {}
    memory_file_exists = memory_path.is_file()
    if memory_file_exists:
        try:
            data = json.loads(memory_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                runtime_bucket = dict(data.get(channel_key) or {})
        except Exception:
            pass

    has_still_learning = bool(
        runtime_bucket.get("last_still_artifact_job_id")
        or runtime_bucket.get("visual_watchouts_map")
        or runtime_bucket.get("last_still_artifact_scene") is not None
    )
    has_visual_watchouts = bool(runtime_bucket.get("visual_watchouts"))
    has_visual_learnings = bool(runtime_bucket.get("visual_learnings"))
    reference_seeded = bool(runtime_bucket.get("skeleton_reference_seeded_at"))

    learning_records_count = 0
    if learning_records_path.is_file():
        try:
            records = json.loads(learning_records_path.read_text(encoding="utf-8"))
            if isinstance(records, dict):
                learning_records_count = len(records)
        except Exception:
            pass

    issues: list[str] = []
    if not bundled_ok:
        issues.append("bundled_skeleton_reference_memory_missing")
    if not refs_videos:
        issues.append("youtube_reference_videos_not_downloaded")
    if not memory_file_exists:
        issues.append("catalyst_channel_memory_file_missing")
    if not has_visual_watchouts:
        issues.append("no_runtime_visual_watchouts_for_channel")
    if not reference_seeded:
        issues.append("bundled_references_not_seeded_into_runtime_memory")
    if not has_still_learning:
        issues.append("no_still_artifact_learning_events_recorded_yet")

    status = "healthy" if not issues else ("degraded" if has_visual_watchouts or bundled_ok else "unhealthy")

    return {
        "status": status,
        "channel_key": channel_key,
        "checked_at": time.time(),
        "memory_path": str(memory_path),
        "memory_file_exists": memory_file_exists,
        "bundled_reference_loaded": bundled_ok,
        "reference_video_count": len(refs_videos),
        "reference_videos": [p.name for p in refs_videos],
        "directives_loaded": bool(directives.get("prompt_block")),
        "runtime_memory_loaded": bool(directives.get("runtime_memory_loaded")),
        "visual_watchouts_count": len(directives.get("visual_watchouts") or []),
        "visual_learnings_count": len(directives.get("visual_wins") or []),
        "still_artifact_learning_active": has_still_learning,
        "reference_seeded": reference_seeded,
        "learning_records_count": learning_records_count,
        "issues": issues,
        "fix_actions": _fix_actions_for_issues(issues),
    }


def _fix_actions_for_issues(issues: list[str]) -> list[str]:
    actions: list[str] = []
    if "bundled_references_not_seeded_into_runtime_memory" in issues:
        actions.append("run seed_bundled_reference_memory() on deploy or first still gen")
    if "no_runtime_visual_watchouts_for_channel" in issues:
        actions.append("seed bundled reference memory and regenerate a scene to record watchouts")
    if "no_still_artifact_learning_events_recorded_yet" in issues:
        actions.append("trigger Catalyst regenerate on an artifacted scene — learning records on each regen")
    return actions


def ensure_catalyst_skeleton_learning_ready(channel_key: str = "mrskelewelly") -> dict[str, Any]:
    """Idempotent bootstrap: seed refs into runtime memory if missing."""
    audit = audit_catalyst_self_learning(channel_key)
    if "bundled_references_not_seeded_into_runtime_memory" in list(audit.get("issues") or []):
        seed_result = seed_bundled_reference_memory(channel_key)
        audit = audit_catalyst_self_learning(channel_key)
        audit["seed_result"] = seed_result
    return audit