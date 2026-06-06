# Studio Agent — Bug Audit (resolved)

**Goal:** Chat → ideation → script → scenes → finished MP4 in ~1 hour.

**Status:** All P0/P1 items fixed in worktree. Deploy with `ops/deploy_studio_agent.ps1`.

---

## What was fixed

### Routing (429 / session split)
- All agent, YouTube, and analytics calls use `resolveStudioBackendUrl()` → Fly (`nyptid-studio.fly.dev`).
- Cloudflare worker `FLY_DIRECT_PREFIXES` includes `/api/studio-agent`, `/api/youtube/`, `/api/studio/analytics`.
- `authFetch` in AgentPanel uses the same resolver for every path.

### Channel analytics
- Subscribers see **their own** linked channels (not admin-only).
- 403 shows “Connect YouTube in Settings…”.
- Open Studio Agent button visible for any signed-in user.

### Production pipeline
- `visual_brief` wired through `analyze_script` + `derive_beat_visuals`.
- Mid-job `progress.json` + poll reads stages.
- `job_spec.json` written at spawn; `SKELETON_AI_OUTPUT_ROOT` on Fly volume.
- `last_production` saved on approve/auto-run; **POST `/retry-production`** re-spawns job.
- Render dock: running / failed (Retry) / complete (Download).

### Agent UX
- Pending/approve recovery; queue bypass for owner + approve + retry.
- Failed jobs no longer say “Your video is ready”.
- Long chats trimmed for model (`MAX_MESSAGES_FOR_MODEL=80`); full transcript kept on disk.
- Job poll retries once on 429/502/503.

### Downloads
- `AgentJobDeliverable` and render dock use `mediaUrl()` → Fly, not RunPod `API`.

---

## Deploy

```powershell
cd "d:\Games\asd\.claude\worktrees\laughing-mclean-b5c91d"
.\ops\deploy_studio_agent.ps1
```

Or manually: `fly deploy`, `wrangler deploy` in `runpod-serverless/`, Vercel prod for `ViralShorts-App`.

---

## Smoke test

1. Hard refresh Studio.
2. Your channel loads (no 429).
3. Studio Agent → topic + visual brief → Approve once.
4. Render dock progress → Download MP4.
5. If failed → **Retry** in dock.

---

## Remaining (infra, not blocking single-machine)

- **Multi Fly machine:** `min_machines_running = 1` in `fly.toml` — scale-out needs shared job queue later.
- **Heavy GPU routes** (`/api/render`, `/api/cliplab`) still on RunPod by design.
