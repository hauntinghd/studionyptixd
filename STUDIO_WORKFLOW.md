# Studio End-to-End Workflow

**Purpose:** the canonical, step-by-step recipe for producing a video entirely through Studio (no Claude Code orchestration). Owner-only beta gates apply — admin email must match `ADMIN_EMAILS` env var.

**Last verified:** 2026-04-30 (audit Round 2). Pipeline plumbing is complete; cost-confirm dialog and mid-chapter resume are still polish items.

---

## 0 · One-time setup

You only need to do these once per workstation / per channel.

1. **Be signed in to Studio.** Visit `https://studio.nyptidindustries.com` and sign in with the admin email. The owner-beta gate at [`backend.py:14942`](backend.py) and the new `WAITLIST_ONLY_MODE` gate at [`backend.py:930`](backend.py) both require an admin email.
2. **Connect the YouTube channel** you want to publish from. Studio → "Channels" → connect each of: Empire Magnates, History Rewind, PB Lies, NYPTID Clips. The OAuth flow grants `youtube.upload` + `youtube.readonly` + `yt-analytics.readonly` + `youtube.force-ssl` scopes (see [`youtube.py:53-58`](youtube.py)).
3. **Set the channel's locked visual lane.** This is enforced inside the long-form prompt path:
   - Empire Magnates → white porcelain mannequin (`_coerce_empire_longform_channel_memory` at [`backend.py:12995`](backend.py))
   - History Rewind → illustration (`_coerce_history_rewind_longform_channel_memory` at [`backend.py:13184`](backend.py))
   - Cryptic Reads → cryptic science / skeleton (`_coerce_cryptic_longform_channel_memory` at [`backend.py:13098`](backend.py))
   - PB Lies → wooden / theatrical (manual; not yet a `_coerce_*` helper — set in the long-form panel manually)
4. **Confirm Render is up.** Hit `https://studio.nyptidindustries.com/api/health` — should return 200. If you see 503 with `X-Render-Routing: suspend`, the Render service is paused — unsuspend it before proceeding.

---

## 1 · Long-form video (10–60 min documentary)

This replaces the Claude-Code-orchestrated pipeline used for Wirecard / Sanjay Shah / Mongol 9H / Ottoman 9H.

### 1.1 Plan the budget
- **Cost target:** ~$50 fal credits per 12–15 min episode at standard quality. ~$33 for a 9H Ernie-image-only doc with no animation. ~$60–160 for a 60-min Lacuna-style fully-animated cinematic.
- **Top up fal.ai if balance < $80.** No automatic guardrail in Studio yet (see audit finding 3.2 — cost confirmation is on the polish backlog).
- **Verify YouTube quota headroom** at `/api/admin/youtube-quota`. Reference video analysis costs ~4 units per video. For a typical session you'll burn under 100 units; not a real concern unless Catalyst auto-pilot is on.

### 1.2 Open the Long-Form panel
1. In Studio, click **Long Form** in the side nav.
2. Click **Create new session**.
3. Fill the form:
   - **Channel** — pick from dropdown (lane-locked memory loads automatically).
   - **Format preset** — `documentary`, `recap`, `explainer`, or `story_channel`. Documentary is what `build_episode_v5.py` was; locks image model to `grok_imagine` per [`backend.py:13283`](backend.py).
   - **Topic / title** — what the episode is about.
   - **Source URL** (optional) — paste a YouTube URL of a video you want to study/repackage. Catalyst will auto-analyze the first 24 screenshots (see [`LongFormPanel.tsx:1508`](ViralShorts-App/src/studio/panels/LongFormPanel.tsx)).
   - **Analytics notes** (optional) — paste any retention curves, view counts, comments-of-interest.
   - **Auto-pipeline** — leave OFF for the first run (gives you per-chapter approval). Turn ON later when you trust the recipe.
4. Submit. Backend creates a session via `POST /api/longform/session` ([`backend.py:14938`](backend.py)). If you also passed `source_url`, the bootstrap call ([`backend.py:_create_longform_session_bootstrap`](backend.py)) starts the YouTube + transcript ingest in the background.

### 1.3 Per-chapter loop (when auto-pipeline is OFF)
For each chapter (typically 6–24 chapters per episode):
1. **Wait for "ready for review"** — Studio shows the generated chapter outline + draft scenes.
2. **Eyeball the script.** If the angle is wrong, click **Regenerate** (uses 1 retry; goes back through the LLM with your feedback).
3. **Approve the chapter** — fires `POST /api/longform/session/{id}/chapter-action` ([`backend.py:15814`](backend.py)) with action `approve`. The next chapter starts generating in the background while you're reviewing this one.
4. **Repeat** until all chapters are approved.

### 1.4 Finalize → render to MP4
1. Click **Finalize** in the panel ([`LongFormPanel.tsx:940`](ViralShorts-App/src/studio/panels/LongFormPanel.tsx)).
2. Backend kicks off `_start_longform_finalize_internal` ([`backend.py:16200`](backend.py)) → `_run_longform_pipeline` ([`backend.py:14035`](backend.py)).
3. **Pipeline stages** (per scene):
   - Image gen via Grok Imagine (locked for documentary format).
   - Image-to-video via **LTX Video 13B** at 24fps cinematic ($0.04/video, the validated winner per memory).
   - SFX via mmaudio-v2 + ElevenLabs.
   - 2-pass loudnorm normalization.
   - Concat + scene transitions in ffmpeg.
4. **Render time:** rough 1.5–2× video duration on the current FAL key pool (6 keys, 16 slots each = 96 slots). A 60-min episode finishes in ~90–120 min wall-clock if no scene fails.
5. **Output:** lands in `/var/data/generated_videos/longform_{session_id}.mp4`. Studio surfaces a download link in the panel + writes a `metadata.json` next to it.

### 1.5 If a scene fails 2 hours into a 9H render
- **Stalled task auto-recovery** at [`backend.py:15130`](backend.py) handles 180–300s task stalls — the chapter moves back to `pending_review`.
- **No mid-chapter resume yet** (audit finding 3.3). You must regenerate the failed chapter from its first scene.
- **Workaround:** keep chapter lengths short (5–8 min each instead of one 60-min monolith). You lose less when one fails.

### 1.6 Upload to YouTube
1. From the long-form session detail page, click **Upload to YouTube**.
2. Pick the channel (must be OAuth-connected from §0.2).
3. Set title / description / tags / privacy (`private` while you eyeball, then flip to `public`).
4. Studio calls `youtube_upload_video()` at [`youtube.py:5092`](youtube.py).
5. **Quota cost:** 1600 units for `videos.insert` + 50 for `thumbnails.set`. Both are now properly tracked through `youtube_quota.reserve()` (audit fix 1.2). View at `/api/admin/youtube-quota`.

---

## 2 · Short-form video (15–90s)

For TikTok-style / Shorts-style clips.

### 2.1 Open the Create panel
1. Studio → **Create**.
2. Pick a **template**: `skeleton`, `story`, `motivation`, `daytrading`, `chatstory`, `reddit`.
   - `skeleton` is the most robust — has a local fallback that kicks in if the LLM is rate-limited.
   - All others **will hard-fail** if the LLM call times out (audit fix 2.1 surfaces a clear error message now: "switch to skeleton template if you need a guaranteed-render fallback").
3. Pick a **YouTube channel** (optional — enables Catalyst-driven script seeds based on your channel's recent winners).
4. Set the **topic** in plain English.
5. Submit. Backend route is `POST /api/generate` → `_generate_short` ([`backend.py:19199`](backend.py)).

### 2.2 Watching progress
Progress bar advances `5 → 6 → 7 → 8 → 9 → 10 → ... → 100`:
- `5` = generating script (LLM call in flight)
- `6–9` = heartbeat ticks every 20s while LLM is still working (audit fix 2.1 — no more "frozen at 5" anxiety)
- `10` = script done, image gen starting
- `10–55` = image gen + animation per scene
- `55–70` = voice synthesis (ElevenLabs)
- `70–80` = SFX
- `80–98` = compositing
- `100` = done

### 2.3 If it fails
- Skeleton template → fallback kicks in automatically using local script + cached channel context.
- Other templates → you'll see a specific error like "Script generation timed out after 150s for template 'story'. This usually means the LLM provider (fal any-llm / Claude) is rate-limited or overloaded. Try again in a minute, or switch to the 'skeleton' template which has a local fallback." Retry or switch template.

### 2.4 Upload to YouTube
Same as long-form (§1.6).

---

## 3 · Catalyst (channel research / "what's working")

Catalyst is the YouTube research engine. It's the demo highlight for the YouTube Data API quota review (10k → 990k).

### 3.1 Sync a connected channel's outcomes
1. Studio → **Catalyst**.
2. Pick a channel.
3. Click **Sync outcomes** — pulls the latest video stats + analytics for ranking.
4. Cost: ~10–40 quota units depending on video count. All routed through `youtube_quota.reserve()` + cached for 6h (`youtube_cache.py`).

### 3.2 Reference-video analysis (the demo's wow moment)
1. From a long-form session OR the Catalyst hub, paste a YouTube URL of a video you want to learn from.
2. Studio fetches the metadata, transcript, and up to 24 representative screenshots.
3. Catalyst feeds these into an analysis prompt ("what's the hook? what's the visual grammar? where does retention spike?") and stores the result in `_longform_sessions[session_id]["channel_memory"]`.
4. **Quota cost per reference video:** ~4 units (`get_video_stats`) + transcript fetch (free; uses captions endpoint @ 50 units once per video, cached forever).
5. **Demo angle for Google:** show the cached + quota-tracked behavior. Open `/api/admin/youtube-quota` and walk through the breakdown — they'll see responsible usage.

### 3.3 Auto-pilot (off by default — DON'T turn on for the demo)
Catalyst can auto-monitor your connected channels for "decay" (view velocity dropping below threshold) and trigger a new long-form pipeline run. It's gated behind `POST /api/catalyst/hub/auto-pilot?enabled=true` and DISABLED by default. Don't enable it during the demo recording — the auto-tick burn is visible in the quota dashboard and looks unprofessional unless you explain it carefully.

---

## 4 · Recording the YouTube API quota review demo

Casey, this is the actual script for tomorrow.

### Pre-flight (10 min)
1. ✅ Render service is unsuspended → `/api/health` returns 200.
2. ✅ `STRIPE_WEBHOOK_SECRET` is set on Render env (otherwise webhook 503s — by design after audit fix 1.1).
3. ✅ Supabase migration `2026-04-20_jobs.sql` is applied (check `jobs.user_id` column type is `text`).
4. ✅ One YouTube channel has been Catalyst-synced today so the quota dashboard already shows real usage.
5. ✅ Have a 2nd YouTube URL ready to paste as a "reference video" mid-demo.

### Demo flow (record in OBS at 1080p)
1. **Open Studio dashboard.** Show the connected channels, the Catalyst panel, the recent renders.
2. **Show `/api/admin/youtube-quota`** — let reviewers see today's `videos.insert: 1600`, `search.list: 100s`, etc. By-method breakdown is the credibility moment.
3. **Paste a reference video URL** into Catalyst → narrate "Catalyst pulls metadata + transcript + screenshots for analysis. Every API call goes through `youtube_quota.reserve()` and our 6-hour disk cache."
4. **Walk through a long-form session** — show the per-chapter approval flow + the channel-locked visual lanes. Frame it as "we're studying high-performing videos to inform our own production."
5. **Show the upload step** — narrate "videos.insert costs 1600 units; we track every upload in our quota dashboard so we never accidentally exceed our cap."
6. **Pitch the 990k ask** — "Today we operate 4 channels. With the increased quota we want to (a) sync analytics for all 4 channels every 6h via Catalyst auto-pilot, (b) analyze 5–10 reference videos per session × 10–20 sessions/day, (c) upload 30+ videos/day across all channels."

---

## 5 · Cost reference (per memory + audit)

| Item | Cost | Source |
|------|------|--------|
| 12–15 min documentary (LTX video) | ~$50 fal | `project_v5_pipeline_locked.md` |
| 9H Ernie-image sleep doc | ~$33 fal | `project_mongol_9h_history_rewind.md` |
| 60-min Lacuna cinematic 24fps | ~$60–160 fal | `project_lacuna_dyatlov_pending.md` |
| Single short-form (skeleton) | ~$0.50–2 fal | varies by scene count |
| LTX Video 13B i2v | $0.04/video, 720p/24fps | `project_ltx_i2v_winner.md` |
| Pixverse v6 i2v | $0.05–0.07 | `reference_fal_endpoints.md` |
| Ernie image | $0.03 each | `project_mongol_9h_history_rewind.md` |
| YouTube `videos.insert` | 1600 units | tracked in `youtube_quota.py` |
| YouTube `thumbnails.set` | 50 units | tracked in `youtube_quota.py` |
| YouTube `search.list` | 100 units | tracked in `youtube_quota.py` |
| YouTube `videos.list` (stats) | 1 unit | tracked in `youtube_quota.py` |

---

## 6 · When to fall back to Claude Code

Studio handles the canonical pipeline end-to-end. Stay in Claude Code for:

- **Custom one-off rebuilds** with non-template channel grammar (e.g. the Wirecard rebuild needed `scene_brief_v3.json` schema work).
- **Dataset corpus extraction + analysis** (`extract.sh` + per-video analysis JSONs are not in Studio).
- **Trying a brand-new image / video model** — model wiring is in `backend_image_prompts.py` and needs code changes.
- **Bulk channel migrations** — moving content between channels.

For everything else — script + scenes + render + upload — Studio should be the only tool.

---

## 7 · Where things live

| Concern | File |
|---------|------|
| Long-form panel (frontend) | [`ViralShorts-App/src/studio/panels/LongFormPanel.tsx`](ViralShorts-App/src/studio/panels/LongFormPanel.tsx) |
| Long-form pipeline (backend) | [`backend.py:14035`](backend.py) `_run_longform_pipeline` |
| Long-form session create | [`backend.py:14938`](backend.py) `_create_longform_session` |
| Long-form chapter action | [`backend.py:15814`](backend.py) `_longform_chapter_action` |
| Long-form finalize | [`backend.py:16200`](backend.py) `_start_longform_finalize_internal` |
| Short-form generate | [`backend.py:19199`](backend.py) `_generate_short` |
| Creative finalize | [`backend.py:17575`](backend.py) `_creative_finalize` |
| YouTube upload | [`youtube.py:5092`](youtube.py) `youtube_upload_video` (now quota-tracked) |
| YouTube OAuth start | [`youtube.py:_youtube_build_auth_url`](youtube.py) |
| Catalyst routes | [`backend_youtube_catalyst_routes.py`](backend_youtube_catalyst_routes.py) |
| Catalyst learning loop | [`backend_catalyst_learning.py`](backend_catalyst_learning.py) |
| Quota tracker | [`youtube_quota.py`](youtube_quota.py) |
| Cache | [`youtube_cache.py`](youtube_cache.py) |
| Stripe webhook (now fail-closed) | [`backend.py:21187`](backend.py) `_stripe_webhook` |
| Waitlist signup (now rate-limited) | [`backend.py:21204`](backend.py) `_join_waitlist` |
| Render config | [`render.yaml`](render.yaml) |
| Frontend Vercel config | [`ViralShorts-App/vercel.json`](ViralShorts-App/vercel.json) |
