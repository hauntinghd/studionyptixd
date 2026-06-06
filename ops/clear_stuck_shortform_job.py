#!/usr/bin/env python3
"""One-off: clear zombie shortform job from Studio Agent session."""
from __future__ import annotations

import json
import os
from pathlib import Path

SESSION_ID = os.environ.get("SESSION_ID", "sa_1f796bb8ce77439b")
JOB_ID = os.environ.get("JOB_ID", "adbebe5e3108")
SESSIONS = Path(os.environ.get("STUDIO_AGENT_SESSIONS_DIR", "/var/data/studio_agent_sessions"))
SKELETON_ROOT = Path(os.environ.get("SKELETON_AI_OUTPUT_ROOT", "/var/data/skeleton_output"))


def main() -> None:
    SKELETON_ROOT.mkdir(parents=True, exist_ok=True)
    path = SESSIONS / f"{SESSION_ID}.json"
    if not path.is_file():
        print(f"session not found: {path}")
        return
    session = json.loads(path.read_text(encoding="utf-8"))
    before = len(session.get("active_jobs") or [])
    session["active_jobs"] = [
        j for j in (session.get("active_jobs") or [])
        if str(j.get("job_id") or "") != JOB_ID
    ]
    path.write_text(json.dumps(session, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"cleared job {JOB_ID} from {SESSION_ID} ({before} -> {len(session['active_jobs'])} active)")


if __name__ == "__main__":
    main()
