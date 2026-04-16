# 🚀 Studio Backend → RunPod Serverless — Handoff

> **This document is for another Claude Code session to execute the port.**
> It is fully self-contained — no prior conversation context required.

---

## Goal

Port the existing Studio backend (FastAPI app at `E:/Games/asd/backend.py`, ~22K lines, ~30 modules) from its current always-on hosting to **RunPod Serverless**. Result: 24/7 public availability with cost = $0 when idle, per-second billing only when actual requests arrive.

Public API routes via Cloudflare Worker at `api.studio.nyptidindustries.com/*` → RunPod endpoint.

## What's Already Done (prep work in `E:/Games/asd/runpod-serverless/`)

| File | What it does | Status |
|---|---|---|
| `handler.py` | RunPod serverless event handler. Warm-loads FastAPI app once, bridges each RunPod event to an in-process FastAPI request via `TestClient`. Supports JSON, text, and binary (base64) bodies. | ✅ Complete |
| `Dockerfile` | Multi-stage build: Node 20 alpine for Vite frontend, Python 3.11-slim runtime with FFmpeg + fonts. Adds `runpod>=1.7.0` pip package. `CMD ["python", "-u", "handler.py"]`. | ✅ Complete |
| `worker-proxy.js` | Cloudflare Worker that maps public HTTP → RunPod `/runsync` or `/run` (async for render jobs >30s). Returns `job_id` + `poll_url` for async. Handles JSON / text / binary bodies. | ✅ Complete |
| `wrangler.toml` | CF Worker config. Binds `api.studio.nyptidindustries.com/*`. | ✅ Complete |
| `deploy.sh` | One-command bash deploy: builds + pushes Docker image, upserts RunPod template via GraphQL, upserts endpoint with scale 0→3 CPU workers, flashboot enabled. | ✅ Complete |

## What You Need To Do

### Phase 1 — Verify the handler works with the actual backend (LOCAL, free)

```bash
cd E:/Games/asd
pip install runpod fastapi httpx
# Quick smoke test:
python -c "
from runpod_serverless.handler import handler
result = handler({'input': {'method': 'GET', 'path': '/health'}})
print(result)
"
```

Expected: `{'status_code': 200, 'body': {...}}` matching whatever `/health` returns in `backend.py`. If it errors on missing env vars, add them to a `.env.test` and source before the test.

**Common issues to fix:**
1. Backend.py probably has startup tasks that call external services (Supabase, Redis, ComfyUI, RunPod SSH compositor). These need to be:
   - **Lazy-loaded** — don't connect at import time, connect on first request
   - **Guarded by env vars** — if `SUPABASE_URL` missing, skip Supabase-dependent routes
   - **Tolerant of cold start** — SSH tunnel to compositor pod should reconnect each invocation
2. File-based state (SQLite, local JSON, `generated_videos/`, `temp_assets/`) won't persist across serverless invocations. Three fixes:
   - **Read-only defaults** (bundle in image)
   - **Shared network volume** — attach RunPod `networkVolumeId` to endpoint (modify `deploy.sh`)
   - **External storage** — Supabase Storage, S3, R2 (cheapest on Cloudflare)
3. Long-running routes (video render >30s) **must return a job ID**, not block. The worker-proxy.js already switches to async `/run` for paths starting with `/api/render/`, `/api/generate/`, `/api/longform/`. Make sure those endpoints return immediately and write results to a queue/DB that the status endpoint can poll.

### Phase 2 — Build the Docker image

```bash
cd E:/Games/asd
# Test build (no push yet)
docker build -f runpod-serverless/Dockerfile -t studio-backend:test .
# Run locally to verify
docker run --rm -p 8000:8000 -e RUNPOD_WEBHOOK_URL=... studio-backend:test
```

Check the image starts without crashing. The handler should log `✓ Backend loaded, TestClient ready`.

If build fails on missing modules — the backend imports files that aren't in the repo root. Fix by:
- Adding them to `COPY` lines in `Dockerfile`, or
- Moving them under a `backend_modules/` dir and `COPY backend_modules ./`

### Phase 3 — Deploy

```bash
export RUNPOD_API_KEY="$(pass runpod/api-key)"   # or from his .env
export DOCKER_REGISTRY="docker.io/caseynyptid"    # or ghcr.io/crypticsciencee
export IMAGE_TAG="studio-backend:v1.0"

cd E:/Games/asd
bash runpod-serverless/deploy.sh
```

Output will include an `Endpoint ID`. Save it.

### Phase 4 — Wire Cloudflare Worker

```bash
cd E:/Games/asd/runpod-serverless
npm install -g wrangler   # if not already
wrangler login            # OAuth to the Cloudflare account that owns nyptidindustries.com

wrangler secret put RUNPOD_API_KEY
# paste the API key from step 3

wrangler secret put RUNPOD_ENDPOINT_ID
# paste the Endpoint ID from deploy.sh output

wrangler deploy
```

Confirm `api.studio.nyptidindustries.com` responds.

### Phase 5 — Point the frontend

In `E:/Games/asd/ViralShorts-App/` (the Vite frontend), find the API base URL constant. It's probably in `src/api/client.ts` or similar. Change it to:

```typescript
export const API_BASE = "https://api.studio.nyptidindustries.com";
```

For async renders, update the client to poll `/status/:job_id` after initial response returns 202 + `job_id`.

Rebuild + redeploy the frontend (however Studio's frontend is hosted — Vercel/CF Pages/etc).

### Phase 6 — Cutover

1. Keep the old backend host running
2. Test the new endpoint for 24-48 hours
3. Once stable, point `studio.nyptidindustries.com` DNS at the new frontend + shut down old host

---

## Cost Model

| Component | Cost |
|---|---|
| Cloudflare Worker | $0 (free tier: 100K req/day) |
| RunPod CPU serverless (idle) | $0 — billed per second of execution only |
| RunPod CPU serverless (active) | ~$0.0004/s × worker count × burst duration |
| Docker Hub storage | $0 (public) or $5/mo (private) |

Realistic monthly estimate with ~500 render jobs, avg 2 min each:
- Compute: 500 × 120s × $0.0004 = **$24/mo**
- Compared to Render.com persistent instance: $25-85/mo
- **Savings at low usage, scales with revenue**

---

## Architecture Diagram

```
  ┌─────────────────────────────────────────────────────────────┐
  │  Browser → studio.nyptidindustries.com (Vite SPA on Vercel) │
  └──────────────────────────┬──────────────────────────────────┘
                             │ fetch
                             ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  Cloudflare Worker @ api.studio.nyptidindustries.com        │
  │  (worker-proxy.js)                                          │
  │   • Auth edge checks (future)                               │
  │   • Picks /runsync vs /run based on path                    │
  │   • HTTP ↔ RunPod event translation                         │
  └──────────────────────────┬──────────────────────────────────┘
                             │ POST /v2/{endpoint}/runsync
                             ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  RunPod Serverless (endpoint: studio-backend)               │
  │   • 0 → 3 CPU workers, flashboot on                         │
  │   • Each worker runs: python handler.py                     │
  │   • handler.py boots FastAPI backend once per worker        │
  │   • Dispatches event → TestClient → backend routes          │
  └──────────────────────────┬──────────────────────────────────┘
             ┌───────────────┼──────────────────┐
             ▼               ▼                  ▼
       Supabase         Redis queue       RunPod Compositor Pod
       (auth/db)        (async jobs)      (ComfyUI + GPU FFmpeg)
                                          — already deployed
```

---

## Key Files — Quick Reference

```
E:/Games/asd/runpod-serverless/
├── handler.py            # RunPod event handler (entry point)
├── Dockerfile            # Multi-stage build
├── deploy.sh             # One-command deploy (RunPod GraphQL)
├── worker-proxy.js       # Cloudflare Worker HTTP bridge
├── wrangler.toml         # CF Worker config
└── HANDOFF_TO_CLAUDE.md  # This file
```

The existing repo stays untouched. All new files are in `runpod-serverless/` subdirectory.

---

## If Something Breaks

| Symptom | Fix |
|---|---|
| `ImportError` on cold boot | Add missing module to `Dockerfile` COPY list |
| Handler returns 500 `handler_exception` | Check RunPod worker logs via dashboard, usually a runtime env var missing |
| Cold-start > 30s on render routes | Normal. That's why we switch to async `/run` for render paths — confirm `worker-proxy.js` `ASYNC_PATHS` list matches your render endpoints |
| Binary responses corrupt (images/video) | Make sure `handler.py` returns `body_b64` (base64), not `body` — already handled for non-JSON content types |
| Auth/Supabase fails | Set RunPod endpoint env vars via GraphQL `env: [{key: "SUPABASE_URL", value: "..."}, ...]` — modify `deploy.sh` |
| Worker says "RunPod not configured" | `wrangler secret put` was not run, or wrong endpoint ID |

---

## Questions for Casey (resolve before Phase 1)

1. **Docker registry choice?** Docker Hub free public, or GHCR (GitHub Container Registry) private via his `crypticsciencee` account?
2. **Env vars list?** Compile full list from `backend_settings.py` so `deploy.sh` can pass them to the endpoint template.
3. **Shared storage strategy?** RunPod network volume ($0.07/GB/mo) vs Supabase Storage vs Cloudflare R2? Must decide before Phase 2 for `generated_videos/` and `temp_assets/`.
4. **Keep SSH-to-compositor-pod model?** Or move compositor to its own serverless endpoint too? (Recommend: keep existing GPU pod until compositor is proven serverless-compatible.)

---

**Start with Phase 1 (local handler smoke test). Don't push anything to RunPod until that passes.**
