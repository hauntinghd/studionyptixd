"""RunPod Serverless process entrypoint for production-only Studio work."""
from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from studio_agent.runpod_worker import handler  # noqa: E402


if __name__ == "__main__":
    import runpod

    runpod.serverless.start({"handler": handler})
