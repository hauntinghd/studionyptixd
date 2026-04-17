"""
Diagnose why the Studio RunPod serverless endpoint is not draining its queue.

Usage:
    # from the repo root, with RUNPOD_API_KEY and RUNPOD_ENDPOINT_ID exported
    python runpod-serverless/diag_studio_api.py

    # or inline:
    RUNPOD_API_KEY=xxx RUNPOD_ENDPOINT_ID=yyy python runpod-serverless/diag_studio_api.py

What it does:
    1. GET /v2/{endpoint_id}/health → reports worker pool state (idle/running/throttled + queue depth)
    2. POST /v2/{endpoint_id}/runsync with a trivial { GET /health } payload → tests the full path
    3. Prints a concrete "next step" verdict based on what it saw

Exit 0 on clean, nonzero on any problem detected.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request


API_KEY = os.getenv("RUNPOD_API_KEY", "").strip()
ENDPOINT_ID = os.getenv("RUNPOD_ENDPOINT_ID", "").strip()

if not API_KEY:
    print("FATAL: RUNPOD_API_KEY not set. Export it or add to runpod-serverless/.runpod.env")
    sys.exit(2)
if not ENDPOINT_ID:
    print("FATAL: RUNPOD_ENDPOINT_ID not set. Find it in the RunPod dashboard under the studio-api-ada24 endpoint.")
    sys.exit(2)


def http_get(url: str, timeout: int = 15) -> tuple[int, dict | str]:
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {API_KEY}"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", errors="replace")
            try:
                return r.status, json.loads(raw)
            except json.JSONDecodeError:
                return r.status, raw
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return e.code, body
    except Exception as e:
        return 0, f"request_error: {type(e).__name__}: {e}"


def http_post_json(url: str, payload: dict, timeout: int = 60) -> tuple[int, dict | str]:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", errors="replace")
            try:
                return r.status, json.loads(raw)
            except json.JSONDecodeError:
                return r.status, raw
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return e.code, body
    except Exception as e:
        return 0, f"request_error: {type(e).__name__}: {e}"


def banner(title: str) -> None:
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)


# ─── 1. Endpoint health ─────────────────────────────────────────────────
banner(f"1. Endpoint /health   ({ENDPOINT_ID})")
health_url = f"https://api.runpod.ai/v2/{ENDPOINT_ID}/health"
status, health = http_get(health_url)
print(f"HTTP {status}")
print(json.dumps(health, indent=2) if isinstance(health, dict) else health)

workers = (health or {}).get("workers", {}) if isinstance(health, dict) else {}
jobs = (health or {}).get("jobs", {}) if isinstance(health, dict) else {}
idle = int(workers.get("idle", 0))
running = int(workers.get("running", 0))
initializing = int(workers.get("initializing", 0))
ready = idle + running
in_queue = int(jobs.get("inQueue", 0))
in_progress = int(jobs.get("inProgress", 0))
completed = int(jobs.get("completed", 0))
failed = int(jobs.get("failed", 0))


# ─── 2. Synthetic health-check call ────────────────────────────────────
banner("2. Synthetic /runsync GET /api/health")
runsync_url = f"https://api.runpod.ai/v2/{ENDPOINT_ID}/runsync"
t0 = time.time()
status2, resp = http_post_json(
    runsync_url,
    {"input": {"method": "GET", "path": "/api/health", "headers": {}, "body": None}},
    timeout=90,
)
elapsed = time.time() - t0
print(f"HTTP {status2}   elapsed {elapsed:.1f}s")
if isinstance(resp, dict):
    # Truncate huge payloads so stdout stays useful
    printable = dict(resp)
    output = printable.get("output")
    if isinstance(output, dict):
        body = output.get("body")
        if isinstance(body, str) and len(body) > 300:
            output = {**output, "body": body[:300] + "…"}
        printable["output"] = output
    print(json.dumps(printable, indent=2))
else:
    print(resp)


# ─── 3. Verdict ─────────────────────────────────────────────────────────
banner("VERDICT")
verdicts: list[str] = []
fatal = False

if status != 200:
    verdicts.append(f"✗ /health returned HTTP {status} — endpoint or API key is wrong.")
    fatal = True
else:
    if ready == 0 and in_queue > 0:
        verdicts.append(
            f"✗ {in_queue} jobs queued but 0 workers ready (idle={idle} running={running} initializing={initializing}). "
            "Workers are not booting. Check:\n"
            "   (a) Is the GHCR image ghcr.io/crypticsciencee/studio-backend public? "
            "(GH workflow 'build-studio-serverless.yml' sets it public after push — rerun if failed.)\n"
            "   (b) RunPod dashboard → studio-api-ada24 → Logs. Look for 'ImagePullBackOff' / "
            "'Error pulling image' / 'crash loop' / 'handler exception'.\n"
            "   (c) Endpoint region may be out of CPU capacity. Try changing gpuIds to "
            "a broader set in deploy.sh (currently 'CPU3C,CPU5C')."
        )
        fatal = True
    elif initializing > 0 and ready == 0:
        verdicts.append(
            f"⚠ {initializing} worker(s) initializing, none ready yet. Give it 60-120s and re-run. "
            "If they stay stuck in 'initializing', image pull or backend boot is hanging."
        )
    elif ready > 0 and in_queue > 0 and in_progress == 0:
        verdicts.append(
            f"⚠ {ready} worker(s) ready but {in_queue} jobs queued and 0 in progress — scaler may be throttled. "
            "Check scalerType/scalerValue in the endpoint config."
        )
    else:
        verdicts.append(f"✓ Workers: idle={idle} running={running} initializing={initializing}, queue={in_queue} in_progress={in_progress} completed={completed} failed={failed}")

if status2 == 200 and isinstance(resp, dict):
    out = resp.get("output") or {}
    out_status = out.get("status_code")
    if out_status == 200:
        verdicts.append(f"✓ Synthetic GET /api/health → {out_status} in {elapsed:.1f}s. Backend boots clean.")
    elif out_status == 503 and isinstance(out.get("body"), dict) and out["body"].get("error") == "backend_boot_failed":
        detail = out["body"].get("detail", "")[:500]
        verdicts.append(
            f"✗ Backend boot FAILED inside the worker. Handler returned 503 backend_boot_failed:\n"
            f"   {detail}\n"
            f"   Fix the import error and redeploy the image."
        )
        fatal = True
    elif out_status:
        verdicts.append(f"⚠ Synthetic GET /api/health → {out_status}. Not 200 but worker is alive. Check the backend route.")
    else:
        verdicts.append(f"⚠ /runsync returned a payload without a clear status_code. Raw: {json.dumps(out)[:300]}")
elif status2 == 200:
    verdicts.append(f"⚠ /runsync returned HTTP 200 but body parsing failed. Raw: {str(resp)[:300]}")
else:
    verdicts.append(f"✗ /runsync returned HTTP {status2} in {elapsed:.1f}s. Raw: {str(resp)[:300]}")
    fatal = True

for v in verdicts:
    print(v)

sys.exit(1 if fatal else 0)
