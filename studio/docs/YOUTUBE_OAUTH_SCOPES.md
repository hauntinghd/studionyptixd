# YouTube OAuth scopes — Studio vs Composio

Studio connects YouTube through **your own Google Cloud OAuth client** (not Composio). Composio’s consent screen shows the **superset** of scopes a third-party aggregator typically requests. Studio requests the **minimum set** needed for production + Catalyst.

## What Composio asked for (your screenshot)

| Composio permission | Maps to Google scope | Studio needs it? |
|---------------------|----------------------|------------------|
| Channel Members | `youtube.channel-memberships.creator` | Optional — member analytics only |
| Video Management (Full) | `youtube` + `youtube.force-ssl` | Partial — we use force-ssl + upload, not full `youtube` |
| Audit Information | Partner audit (Content ID) | No — unless you join YouTube Partner audit programs |
| Manage Videos | `youtube.force-ssl` | Yes — captions, metadata updates |
| View Account | `youtube.readonly` | Yes |
| Asset Management | Content ID / partner scopes | No — unless managing Content ID assets |
| Analytics | `yt-analytics.readonly` | Yes — Catalyst CTR/retention |
| Manage Account | `youtube.upload` + force-ssl | Yes — upload on explicit user action |

## Scopes Studio already requests

Defined in `youtube.py` → `YOUTUBE_SCOPES`:

```text
https://www.googleapis.com/auth/youtube.readonly
https://www.googleapis.com/auth/yt-analytics.readonly
https://www.googleapis.com/auth/youtube.force-ssl
https://www.googleapis.com/auth/youtube.upload
```

These cover: channel list, video metadata, Analytics reports, upload, thumbnails, captions listing.

## Google Cloud Console — step by step

### A. YouTube Data API key (server quota, no user login)

Used for: public video metadata, Catalyst reference ingest, transcript-adjacent lookups.

1. Open [Google Cloud Console](https://console.cloud.google.com/) → select your **NYPTID Studio** project.
2. **APIs & Services → Library** → enable:
   - **YouTube Data API v3**
   - **YouTube Analytics API** (for server-side analytics helpers)
3. **APIs & Services → Credentials → Create credentials → API key**
4. **Restrict key**:
   - Application restrictions: IP addresses (your RunPod/backend IPs) or None for dev only
   - API restrictions: **YouTube Data API v3** only (separate keys if you split Analytics)
5. Add to `.env`:
   ```env
   YOUTUBE_API_KEY=AIza...
   # Or rotate pool:
   YOUTUBE_API_KEYS=key1,key2,key3
   ```

Quota: default 10,000 units/day per project. Upload uses OAuth, not this key.

### B. OAuth client (user connects their channel)

Used for: upload, Analytics per-channel, refresh tokens stored in Supabase.

1. **APIs & Services → OAuth consent screen**
   - User type: **External**
   - App name: **NYPTID Studio**
   - Privacy: `https://studio.nyptidindustries.com/privacy`
   - Terms: `https://studio.nyptidindustries.com/terms`
2. **Scopes → Add or remove scopes** — add all four in `YOUTUBE_SCOPES` (see above).
   - `youtube.upload` is **Restricted** → requires Google verification for production publish.
3. **Credentials → Create OAuth client ID**
   - Type: **Web application**
   - Authorized redirect URI: your live callback (see `GOOGLE_REDIRECT_URI` in `backend_settings.py`)
4. Download JSON → `client_secrets.json` in repo root (gitignored).
5. In Studio: **Settings → Connect YouTube** — completes OAuth and stores refresh token.

Full publish checklist: `OAUTH_PUBLISH_RUNBOOK.md`.

### C. Optional scopes (Composio parity)

Add only if you build the feature:

| Scope | When to add |
|-------|-------------|
| `youtube.channel-memberships.creator` | Membership tier analytics |
| `youtube` (full) | Rare; prefer force-ssl + upload |
| `youtubepartner` / Content ID | Asset management dashboards |

After adding scopes, users must **re-consent** (disconnect + reconnect in Settings).

## Studio Agent + YouTube

The Studio Agent tool `youtube_oauth_status` returns this doc. For live channel lists, use Studio **Settings** or `GET /api/youtube/channels` (OAuth-connected channels only).

**Do not** paste OAuth tokens or API keys into Agent chat. Keys live in `.env` and Supabase only.

## Composio vs Studio Agent

| | Composio | NYPTID Studio Agent |
|---|----------|---------------------|
| LLM | Their stack | **OpenRouter** (any model) |
| Skills | Generic | **26 Rookcast playbooks** in `studio/skills/` |
| Approvals | Varies | **confirm** / **auto** per session |
| YouTube | OAuth via Composio | **Direct OAuth** (your GCP project) |
| Renders | External | **fal / long_form / skeleton-ai** in-repo |

You get Composio-style orchestration without giving Composio your channel tokens — as long as Studio OAuth is connected.
