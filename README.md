# NYPTID Studio

NYPTID Studio is the backend and frontend for Studio Agent, Catalyst, long-form and short-form video generation, YouTube intelligence, billing, and production orchestration.

## Current Architecture

- `backend.py` is the FastAPI entry point and currently mounts most production routes.
- `studio_agent/` contains the chat agent, tool registry, grounding guards, memory, telemetry, and training capture.
- `catalyst.py` plus `backend_catalyst_*.py` contain Catalyst learning, channel memory, reference analysis, and recommendation logic.
- `ViralShorts-App/` is the Vite/React Studio frontend.
- `long_form/`, `skeleton_ai/`, `zerotier_private/`, `media_sources/`, and `cliplab/` contain specialized generation and source-ingest pipelines.
- `*_router.py` files expose feature-specific FastAPI routers. These are candidates for future package cleanup, but should only be moved with route-by-route verification.

## Local Setup

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
cd ViralShorts-App
npm install
npm run build
cd ..
uvicorn backend:app --reload --host 0.0.0.0 --port 10000
```

Use `npm.cmd` instead of `npm` in PowerShell if script execution policy blocks npm shims.

## Configuration

Configuration is environment-driven. Keep local secrets in `.env` or deployment secret stores, not in Git.

Important env groups:

- OpenRouter / model routing keys for Studio Agent.
- YouTube Data API keys and Google OAuth client values for Catalyst and connected-channel intelligence.
- Stripe and PayPal credentials for billing.
- Supabase credentials for durable user/channel state.
- RunPod, fal.ai, ComfyUI, and render-worker credentials for generation pipelines.

`client_secrets.json` is intentionally ignored. Production deploys should use environment-configured OAuth values. A local ignored `client_secrets.json` can still be used for development fallback when needed.

## Deployment

Fly backend deploy:

```powershell
flyctl deploy --remote-only --config fly.toml --app nyptid-studio
```

The Docker image builds the frontend from `ViralShorts-App/` and copies the generated `dist` into the runtime image. Do not commit `ViralShorts-App/dist/` or `/build/`; they are generated artifacts.

## Hygiene Rules

- Do not commit `client_secrets.json`, `.env`, local databases, generated videos, build output, RunPod scratch files, or extracted review media.
- Use `requirements.txt` as the single authoritative Python dependency file.
- Keep production-sensitive changes incremental and verify `/api/health`, auth-protected route mounting, Studio Agent tool paths, Catalyst hub routes, and at least one generation path after meaningful backend edits.

## Refactor Direction

The next cleanup should be incremental:

1. Keep `backend.py` working while moving coherent chunks into packages.
2. Consolidate settings only after deployment env coverage is confirmed.
3. Move routers into a `routers/` package or equivalent grouped layout, not one unreviewable mega-file.
4. Move media generation and external service clients behind stable interfaces.
5. Verify after every phase and avoid behavior changes during file movement.
