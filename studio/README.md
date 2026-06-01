# NYPTID Studio — Rookcast-Compatible Orchestration Layer

Studio mirrors [Rookcast](https://rookcast.com)’s **skills library + CHANNEL.md + FLOW.md** pattern while keeping NYPTID advantages:

- **Catalyst corpus** for title/thumbnail/outlier research
- **Verified CrypticScience lanes** (primary-source-only YMYL)
- **Direct fal** (Seedream, Aurora, ElevenLabs) — no credit markup
- **Multi-channel recipes** in `long_form/prompts/channels.py`

## Layout

```text
studio/
├── README.md                 ← this file
├── STUDIO_OVERRIDES.md       ← Rookcast → Studio tool swaps
├── skills/                   ← one folder per skill (Rookcast parity)
│   ├── _manifest.yaml        ← skill index + extraction status
│   └── <skill-name>/SKILL.md
└── channels/
    └── <channel_key>/
        ├── CHANNEL.md        ← locked channel memory
        └── FLOW.md           ← per-video production steps + gates
```

## How agents use this

1. **New channel** → load `studio/skills/channel-onboarding/SKILL.md` (+ `niche-bank.md`, `pipeline-bank.md`)
2. **Produce episode** → read `studio/channels/<key>/FLOW.md`, load skills cited per step
3. **Decision gates** → honor `approval_cadence` in CHANNEL.md (`high_touch` vs `low_touch`)
4. **Render** → dispatch to `long_form/` pipeline via `pipeline_kind` in `channels.py`

## Extraction source

Full import via Rookcast API (authenticated browser session):

```text
GET /api/channels/{channel_id}/skills
```

Offline snapshot: `studio/skills/_rookcast_api_snapshot.json` (~1.1MB)

Re-import without browser:

```powershell
python studio/scripts/dump_rookcast_skills_api.py studio/skills/_rookcast_api_snapshot.json
```

**26 skills · 41 markdown files** (see `studio/skills/_manifest.yaml`).

## Studio Agent (OpenRouter beta)

Admin-only panel: **Studio Agent** in sidebar.

- **OpenRouter** — any tool-capable model (not limited to one provider)
- **26 Rookcast skills** — loaded on demand via `load_skill` tool
- **confirm** vs **auto** — user approves credit-spending / file-write commands, or auto-runs allowlisted tools
- **Short + long** — content focus toggles system prompt (skeleton-ai vs long_form + FLOW.md)

Env:

```env
OPENROUTER_API_KEY=sk-or-v1-...   # from openrouter.ai/keys — never commit
STUDIO_AGENT_MODEL=anthropic/claude-sonnet-4
```

YouTube OAuth + API keys: `studio/docs/YOUTUBE_OAUTH_SCOPES.md`

## Wired pipelines (Long Form UI)

| `pipeline_kind` | Status | Build entry |
|-----------------|--------|-------------|
| `sleep_doc` | Registered | Long Form UI |
| `v5_episode` | Registered | EM / Lacuna / PB |
| `cryptic_verified_rook` | **CLI only** | `long_form/build_cryptic_ctr_ss_rook.py` |

Next wiring step: register `cryptic_verified_rook` in `long_form/pipeline.py` `SUB_PIPELINES`.
