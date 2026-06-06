# Helios Clips — Total Build Plan
**Project**: Helios Clips (heliosclips.nyptid.com)  
**Positioning**: Prompt-driven, multi-pass video intelligence agent. One long video → hundreds of perfectly edited 9:16 shorts that match *any* user prompt.  
**Status**: Planning document created 2026-05-26. MVP target: 6–9 weeks of focused work.  
**Owner**: Casey (user) + Cursor agent (me)

---

## 1. Vision & Core Promise

**Tagline**:  
"Give Helios any video (30 min → 2+ hours) + any prompt. It watches the entire thing, finds every matching moment, and turns them into properly edited, viral-ready 9:16 shorts — as many prompts as you want, hundreds of clips per video."

**Why this beats Opus Clip**:
- Opus = "our secret model guesses the 10–20 best clips."
- Helios = "you decide what matters. Run 50 different prompts on the same interview. Get 300+ clips. Zero guesswork."

**Primary Use Cases** (initial):
- Podcast / interview clipping (pricing complaints, funny stories, technical deep-dives, controversial takes)
- Long-form YouTube repurposing
- Creator highlight reels
- Research / archival footage mining

---

## 2. Product Name & Domain

- **Name**: Helios Clips
- **Domain**: heliosclips.nyptid.com (subdomain on existing nyptid.com infrastructure)
- **Future domains** (optional): heliosclips.com, getheliosclips.com

---

## 3. API Keys & Cost Model (Day 1)

You already pay for these — no new expensive services required for MVP:

| Key | Purpose | Notes |
|---|---|---|
| XAI / Grok | Script generation, prompt → segment ranking, virality/hook scoring, natural language understanding | Primary intelligence engine |
| Claude (Anthropic) | Heavy multimodal analysis, long-context transcript reading, fallback judge | Excellent at structured output |
| FAL.ai | Seedream stills (if needed for thumbnails), Pixverse i2v (motion), MiniMax voice, mmaudio SFX | Already proven in `zerotier_private` |
| YouTube Data API | Direct video import + metadata | Already connected |
| Stripe | Credit-based billing (1 credit = 1 minute of input video) | New |

**Estimated cost per 60-minute video** (aggressive batching + caching):
- Transcription (Whisper large-v3 or Grok ASR): ~$0.60–1.20
- Grok/Claude analysis passes: ~$0.80–2.00
- Rendering 20 clips (Pixverse + MiniMax + mmaudio): ~$8–15
- **Total**: ~$10–18 per hour of input video (user pays via credits)

---

## 4. Phased Build Plan (6–9 Weeks to Strong MVP)

### Phase 0 — Core Intelligence Loop (5–7 days)
**Goal**: User pastes URL → full transcription → enters prompt → returns ranked list of matching segments.

**Deliverables**:
- Next.js frontend scaffold (heliosclips.nyptid.com)
- Python worker service (FastAPI or similar)
- Job queue (Redis + BullMQ or Celery)
- YouTube download (yt-dlp) + direct file upload (up to 2 GB for MVP)
- Full Whisper transcription (word-level timestamps) via Grok or FAL
- Prompt ingestion endpoint
- Grok/Claude "watch the entire transcript" analysis → structured JSON of matching segments
  - Each segment: `{start, end, confidence, why_it_matches, suggested_hook_reorder, virality_score}`
- Simple results UI: list of segments + "Render these N clips" button

**Success metric**: 30-minute video + prompt "find every time they talk about pricing" returns 8–15 relevant segments in < 3 minutes.

### Phase 1 — Full Editing Pipeline (7–10 days)
**Goal**: Take the ranked segments → produce actual 9:16 MP4s with professional editing.

**Deliverables** (reuse + extend `zerotier_private/pipeline.py`):
- Smart reframing to 9:16 (MediaPipe BlazeFace or simple center-crop with motion smoothing)
- Hook-first restructuring (Grok rewrites opening 3 seconds to be the strongest moment)
- Dynamic captions (karaoke-style, keyword highlight, configurable fonts/colors)
- Brand kit basics (logo overlay, color palette, font)
- FFmpeg render queue (2 concurrent workers)
- Per-clip progress WebSocket updates

**Success metric**: 10 clips rendered from one prompt, all properly framed, captioned, and under 60 seconds.

### Phase 2 — Multi-Prompt + Batch + Billing (10–14 days)
**Goal**: Run many prompts on one video + basic SaaS features.

**Deliverables**:
- Multi-prompt UI (user can queue 5–10 prompts at once)
- Credit system (1 credit = 1 min input, purchasable packs)
- Stripe integration (checkout + webhook)
- Auth (email + Google for MVP)
- Job history + clip library
- One-click publish to YouTube Shorts (YouTube API already exists)

**Success metric**: User can process a 45-minute video with 6 different prompts and download/publish 40+ clips in one session.

### Phase 3 — Polish & Launch (2–3 weeks)
**Deliverables**:
- Timeline preview (Remotion or simple video player with segment trimming)
- Manual caption editor
- B-roll (stock via Pexels or simple AI gen)
- Analytics dashboard (which prompts perform best)
- TikTok + Instagram Reels publishing
- Public landing page + onboarding flow
- Error handling, retry logic, cost tracking per job

**Success metric**: End-to-end flow from landing page → first paid job → published clip in < 15 minutes for new users.

---

## 5. Core Technical Architecture

**Monorepo** (Turborepo or simple folder structure):
```
heliosclips/
├── apps/
│   ├── web/                 # Next.js 14 (App Router) — frontend + API routes
│   ├── worker/              # Python (FastAPI) — job processing
│   └── renderer/            # FFmpeg + Remotion worker
├── packages/
│   ├── ai/                  # Grok/Claude prompt templates + structured output schemas
│   ├── video/               # Shared FFmpeg helpers, reframing logic
│   └── types/               # Zod + TypeScript shared types
├── infra/
│   ├── docker-compose.yml   # Local Redis + Postgres
│   └── wrangler.toml        # If we deploy any edge pieces to Cloudflare
└── heliosclip_plan_totalbuild.md
```

**Key Data Models** (initial):
- `Video` — url, duration, status, transcript_path, thumbnail
- `PromptRun` — video_id, prompt_text, status, segments_json
- `Clip` — prompt_run_id, start, end, render_status, mp4_path, virality_score
- `BrandKit` — user_id, logo_url, primary_color, caption_style

**Queue Strategy**:
- `download` (I/O bound, high concurrency)
- `transcribe` (API rate limited)
- `analyze` (Grok/Claude — expensive, low concurrency)
- `render` (CPU/GPU heavy, 2 concurrent)

---

## 6. The Intelligence Loop (The Real Product)

This is the heart of Helios. Everything else is scaffolding.

**Step-by-step**:
1. Full Whisper transcript with word timestamps + speaker diarization.
2. User enters natural language prompt (no length limit).
3. We chunk the transcript intelligently (Grok 128k or Claude 200k context).
4. Primary pass: Grok/Claude reads every chunk + the user's prompt and returns candidate segments.
5. Secondary pass (optional but powerful): For each candidate, run a "is this actually the best example of the prompt?" judge.
6. Tertiary pass: For the final shortlist, generate:
   - Suggested 9:16 hook (first 3 seconds should be the strongest moment)
   - Virality score (0–99) based on hook strength, emotional arc, perceived value
   - Why it matches the prompt (for user transparency)
7. User selects which segments to render.
8. Rendering pipeline (Phase 1) executes.

**Future self-improvement** (post-MVP):
- Every time a user keeps vs. discards a clip, or edits the hook, log the decision.
- Periodically fine-tune a small reranker model on this feedback.
- This is exactly how Opus improved their ClipAnything model over time.

---

## 7. Prompt Schema (Initial)

```json
{
  "prompt": "find every time the guest talks about pricing complaints or money issues",
  "video_id": "abc123",
  "segments": [
    {
      "start": 184.2,
      "end": 197.8,
      "confidence": 0.94,
      "why_it_matches": "Guest explicitly says 'the pricing model is the biggest blocker for enterprise'",
      "suggested_hook_reorder": true,
      "virality_score": 87,
      "hook_text": "The pricing model is the biggest blocker..."
    }
  ]
}
```

---

## 8. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Grok/Claude hallucinate segments | Always return raw transcript snippet + timestamp; user can verify |
| Pixverse rejects some prompts as off-brand for their model | Pre-flight prompt rewriting via Sonnet to keep wording neutral and on-topic, plus a Ken Burns still-image fallback when video gen is unavailable (pattern already proven in our existing pipeline) |
| Cost per video too high for users | Aggressive caching of transcripts + descriptions; show cost estimate before rendering |
| Long videos (2+ hours) timeout | Chunked analysis + progress streaming; allow partial results |
| User expects "magic" on day 1 | Transparent "why this segment matched" + manual override tools |

---

## 9. Future Phases (Post-MVP)

- **Phase 4**: Agent Opus-style generation (text/URL → full video)
- **Phase 5**: Trained virality reranker + genre detection models
- **Phase 6**: Team workspaces + shared brand kits + API
- **Phase 7**: Mobile app + desktop wrapper
- **Phase 8**: SOC 2 + enterprise features

---

## 10. Immediate Next Action (When Ready)

When you return to this doc, the first concrete step is:

**"Start Phase 0 scaffold"** — create the monorepo, wire up your existing Grok client + FAL keys, and build the "upload → transcribe → prompt → ranked segments" loop.

Everything else can be layered on top.

---

**Document version**: 1.0  
**Last updated**: 2026-05-26  
**Next review**: When user returns and says "let's start Phase 0"