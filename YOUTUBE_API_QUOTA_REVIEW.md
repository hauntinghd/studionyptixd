# YouTube API Services — Implementation Document

**Product:** NYPTID Studio (Catalyst subsystem)
**Operator:** NYPTID Industries / Casey Setter
**GCP Project:** (per your records)
**Quota requested:** 990,000 units/day on YouTube Data API v3
**Document purpose:** Respond to the YouTube API Services compliance review request (email thread dated April 16 and April 17, 2026) by describing the complete end-to-end process by which our API client retrieves, analyzes, and presents metadata from users' connected YouTube channels.

This document is organized to mirror the reviewer's checklist: consent → connection → data retrieval → analysis → presentation → retention. Code references are provided so the review team can verify the behavior in source.

Public source repository: `https://github.com/hauntinghd/studionyptixd`
Canonical code paths referenced below are relative to the repository root.

---

## 1. Executive Summary

NYPTID Studio is a creator-tools SaaS that helps YouTube channel owners produce short-form and long-form video content. The **Catalyst** subsystem is Studio's research + learning engine. A creator who operates one or more YouTube channels connects those channels to Studio via OAuth. Catalyst then helps the creator decide *what to make next* by analyzing the past performance of the creator's own videos alongside a corpus of public, trending reference videos in the same niche.

All data retrieved via the YouTube Data API v3 is used exclusively to serve the authenticated creator who authorized the scope. No data is sold, shared with third parties, resold to other users, or used for any purpose outside of the creator's own dashboard experience. Data is cached locally (not redistributed) and is only refreshed when stale.

The 990,000 units/day request reflects realistic Catalyst activity once the platform is fully operational: creators actively auditing their channels, ingesting new reference videos, harvesting outcomes from published videos, and running scheduled refreshes of their channel intelligence.

---

## 2. OAuth Scopes and Consent

When a creator connects their YouTube channel, they explicitly consent to the following scopes via Google's OAuth consent screen:

| Scope | Purpose |
|---|---|
| `https://www.googleapis.com/auth/youtube.readonly` | Read the creator's channel + video metadata (titles, descriptions, tags, thumbnails, durations) |
| `https://www.googleapis.com/auth/yt-analytics.readonly` | Read per-video retention, traffic sources, audience demographics, and watch-time data |
| `https://www.googleapis.com/auth/youtube.force-ssl` | HTTPS-only API operations (required for token-based auth) |
| `https://www.googleapis.com/auth/youtube.upload` | Publish finished Studio videos to the creator's connected channel on the creator's command |

**Code reference:** scopes are declared at `youtube.py:50–55` (constant `YOUTUBE_SCOPES`). The OAuth start + callback flow lives at `youtube.py:675–860` (start) and `youtube.py:865–1050` (callback).

Consent is per-creator, per-channel. The creator can revoke access at any time from Google Account settings, and Studio surfaces a "Disconnect channel" button on the Studio Settings page that revokes the refresh token server-side via Google's OAuth revoke endpoint (`https://oauth2.googleapis.com/revoke`).

---

## 3. End-to-End Process — Step by Step

This is the flow Google's email specifically asked for: "*the complete process of how the API client is retrieving and analyzing the metadata of connected YouTube channels.*"

### 3.1  User connects their YouTube channel

1. Creator signs into Studio with Supabase (email/password or Google Sign-In).
2. From Studio Settings → YouTube, creator clicks "Connect YouTube Channel."
3. Frontend calls `POST /api/oauth/google/youtube/start` which returns a Google OAuth authorization URL scoped to the YOUTUBE_SCOPES above.
4. Creator grants consent on Google's screen.
5. Google redirects to `https://api-studio.nyptidindustries.com/api/oauth/google/youtube/callback` with an authorization code.
6. Backend exchanges the code for `access_token` + `refresh_token` at `https://oauth2.googleapis.com/token`.
7. Backend immediately calls `GET /youtube/v3/channels?part=snippet,statistics,contentDetails&mine=true` (1 unit) to verify the connection and identify the channel(s) the creator owns.
8. The refresh token is stored in `youtube_connections.json`, keyed by the Studio user's Supabase ID + channel ID. The refresh token is NEVER exposed to the frontend — it stays on the backend and is used only for server-to-server refresh of the short-lived access token.

**Code reference:** channel-fetch at `youtube.py:2894–2960` (`_youtube_fetch_my_channels`). Refresh-token persistence at `youtube.py:1050–1200`.

### 3.2  Initial channel analysis (one-time per channel at connect)

Once a channel is connected, Catalyst performs a one-time audit so the creator has immediate signal about their past performance. Per channel:

1. **Fetch the uploads playlist ID.** Already cached from step 3.1 (`contentDetails.relatedPlaylists.uploads`). No API cost.
2. **Page through the uploads playlist** via `GET /youtube/v3/playlistItems?part=snippet,contentDetails&playlistId={uploads}&maxResults=50` (1 unit/page). For a channel with 250 videos, this is 5 pages = 5 units.
3. **Batch-fetch video metadata** via `GET /youtube/v3/videos?part=snippet,statistics,contentDetails&id={50_ids_comma_separated}` (1 unit per batch of up to 50 ids). For 250 videos, 5 batches = 5 units.
4. **Fetch Analytics API retention + traffic data** (separate quota pool) for each video — not counted against Data API v3.

**Total Data API cost for a channel audit:** approximately `(2 × ceil(video_count / 50)) + 1` units. A 500-video channel = ~21 units. A 10-video channel = ~3 units.

**Code reference:** channel-audit paging at `youtube.py:2945–3050`. Video batching at `youtube.py:4014–4090`. All calls go through the `_youtube_api_get` helper which enforces quota reservation (see §6 below).

### 3.3  Ongoing refresh (user-initiated or scheduled)

Catalyst keeps the creator's data fresh without re-running the full audit:

- **Static metadata** (title, description, tags, duration, thumbnail) is cached for 24 hours (see `youtube_cache.py` `CacheKind.VIDEO_METADATA`). A repeat view of the same video within 24 h serves from cache — zero API cost.
- **Dynamic stats** (view count, like count, comment count) are cached for 1 hour (`CacheKind.VIDEO_STATS`). Creators refreshing their dashboard multiple times per hour see cached numbers.
- **Channel-level metadata** (subscriber count, channel thumbnail) cached 6 hours (`CacheKind.CHANNEL_METADATA`).
- **Latest-upload detection** cached 1 hour (`CacheKind.CHANNEL_UPLOADS`) — enough to catch new uploads quickly without per-refresh cost.

**Code reference:** `youtube_cache.py:48–70` (cache-kind TTL table) and `youtube.py:2870–2925` (`_youtube_api_get` cache-first flow).

### 3.4  Public reference-video ingestion (no authenticated user, no user data)

In parallel with authenticated per-user flows, Catalyst maintains a reference corpus of *publicly trending* videos, used to identify patterns in what's performing well in each niche. This is a background job that uses the **YouTube Data API key** (not the OAuth token) and fetches ONLY publicly available data.

- **Endpoint:** `GET /youtube/v3/videos?part=snippet,statistics,contentDetails&chart=mostPopular&regionCode={R}&maxResults=50`
- **Cost:** 1 unit per call.
- **Frequency:** one call per configured region per tick. Default tick is every 6 hours. Default region is `US`.

The results are classified by niche (using only the public title + description + tags) and stored in a local reference corpus. This corpus is used internally by Catalyst's script-generation prompts to ground suggestions in "what trending looks like." Reference videos are **not** re-displayed to the creator as-is, and no private data is mixed into the corpus.

**Code reference:** `catalyst_backfill.py` (full module). Classification uses `_catalyst_infer_niche` in `backend_catalyst_core.py:385–428`.

### 3.5  Outcome harvesting (post-publish learning)

When a creator publishes a finished Studio video to YouTube, Catalyst learns from the result so subsequent suggestions improve:

1. After publish, Studio records the YouTube video ID.
2. On a schedule (1 h, 24 h, 7 d after publish — configurable), Catalyst calls:
   - `GET /youtube/v3/videos?part=snippet,statistics&id={video_id}` (1 unit) for public metadata + view count.
   - `GET /youtubeAnalytics/v2/reports?metrics=views,estimatedMinutesWatched,averageViewDuration,...&ids=channel==MINE&filters=video=={video_id}` — Analytics API, **separate quota pool**, not counted against Data API v3.
3. The response is used to update Catalyst's per-niche learning record (what kinds of hooks / thumbnails / titles actually drive retention in this creator's audience). Only this creator's own outcomes influence this creator's future suggestions.

**Code reference:** `backend_catalyst_learning.py:1458–1620` (outcome ingestion + Analytics API fetch).

### 3.6  Data presentation to the creator

Every piece of YouTube-derived data shown in Studio is clearly attributed:

- Video thumbnails are served via the YouTube CDN URLs returned by the API (not rehosted).
- Video titles/descriptions shown in Catalyst's "reference video" cards link directly to the YouTube watch URL.
- Per-video analytics (retention curves, traffic sources) are shown in Studio's Analytics tab with a "Source: YouTube Analytics API" footer line.
- The YouTube logo and "Connected via YouTube" attribution is displayed on Settings → YouTube, on the Catalyst dashboard header, and on the Analytics page.

---

## 4. API Endpoints Used (with rationale per endpoint)

| Endpoint | Scope | Cost | Why Studio calls it |
|---|---|---|---|
| `channels.list?mine=true` | youtube.readonly | 1 | Identify which channels the creator owns after OAuth connect (§3.1 step 7), and refresh sub-count on dashboard load. |
| `channels.list?id=...` | youtube.readonly | 1 | Resolve channel metadata for reference videos that belong to other public creators. |
| `playlistItems.list?playlistId={uploads}` | youtube.readonly | 1/page | Enumerate the creator's own uploads (§3.2 step 2). |
| `videos.list?id={50_ids}` | youtube.readonly | 1/batch | Batch-fetch metadata + stats for up to 50 videos at once (§3.2 step 3, §3.5 step 2). |
| `videos.list?chart=mostPopular&regionCode={R}` | API key (public) | 1/region | Reference-corpus ingestion — public trending only (§3.4). |
| `search.list` | API key or OAuth | 100 | **Used sparingly.** Only when the creator explicitly searches for a reference video by title/keyword in Catalyst's "find similar" tool. Not used in routine polling — we batch via `videos.list?chart=mostPopular` instead (this was a deliberate architectural choice, see §6). |
| `commentThreads.list?videoId=...` | youtube.readonly | 1 | Sample top comments on the creator's own videos to surface audience sentiment on the Analytics page. Disabled by default; creator opt-in. |
| `captions.list?videoId=...` | youtube.force-ssl | 50 | Creator-initiated: when pulling their own published video into Studio for re-editing, retrieve caption tracks. |
| `videos.insert` (upload) | youtube.upload | 1,600 | Publish a Studio-made video to the creator's own channel on their explicit click. |
| `thumbnails.set` | youtube.force-ssl | 50 | Upload the Studio-generated thumbnail when the creator publishes. |

No other Data API v3 endpoints are called in the production code path. The list is exhaustive per the repository state as of April 17, 2026.

**Code reference:** a grep against the repo for `YOUTUBE_DATA_API_BASE` surfaces every call site. Every call flows through `_youtube_api_get` (authenticated) at `youtube.py:2870` or `_youtube_public_api_get` (API key) at `youtube.py:3760`. Both helpers enforce quota reservation and caching (§6).

---

## 5. Quota Economy (how the 990,000 unit/day request is grounded)

The quota request reflects realistic daily activity at target scale.

### 5.1  Per-creator daily consumption

Assumptions for a **single** active creator:

| Activity | Frequency | Cost |
|---|---|---|
| Initial channel audit (once at connect) | 1× at signup, amortized | ≈20 units |
| Dashboard load (refreshes channel + 1 page of uploads) | 4×/day avg | 4 × 2 = 8 units |
| "What's new" ingest of latest videos | 6×/day | 6 × 1 = 6 units |
| Outcome harvest on recently-published videos (Analytics quota mostly, Data v3 for metadata only) | 3 videos × 3 polls = 9 polls/day | 9 units |
| Catalyst "find similar" creator-initiated search | 2×/day avg | 2 × 100 = 200 units |
| Miscellaneous metadata refreshes (cache misses) | ~20/day | ~20 units |
| **Per-creator total** | | **≈265 units/day** |

### 5.2  Platform-wide consumption

Target: **3,000 active creators per day** within 12 months of quota grant.

- 3,000 × 265 units = **795,000 units/day** for creator-initiated flows.
- Public reference-corpus ingestion (§3.4): 4 ticks/day × 10 regions × 1 unit = **40 units/day** (negligible).
- Owner-operated R&D/QA + monitoring: **~10,000 units/day** budgeted.
- Unexpected bursts / retry storms / growth headroom: **~185,000 units/day** buffer.

**Total:** ≈990,000 units/day.

### 5.3  Hard controls enforcing the budget

To prevent runaway consumption Studio ships with a persistent quota tracker (`youtube_quota.py`) that:

- Maintains a **file-persisted daily counter** across worker restarts.
- Implements a **hard cap** at the configured daily quota. Over-cap calls are refused client-side (`reserve()` returns False) and the calling code serves cached data instead of issuing the network call.
- Reserves **≥20% of daily budget for interactive (user-initiated) calls**. Background scrapers cannot exceed the remaining 80% — user flows are never starved.
- Logs **per-method + per-kind breakdown** viewable at `GET /api/admin/youtube-quota` (admin-authenticated).
- Enforces **serve-stale-on-exhaustion** via `youtube_cache.py` — callers degrade gracefully rather than surface errors to the user.

**Code reference:** `youtube_quota.py` (full module). Wired into every API call via `youtube.py:2870` and `youtube.py:3760`.

---

## 6. Caching Strategy (quota discipline)

Studio operates a per-kind TTL cache backed by local file persistence:

| Data kind | TTL | Rationale |
|---|---|---|
| Search results | 6 hours | Trending changes on that order; aggressive cache avoids burning 100-unit `search.list` repeatedly. |
| Video metadata | 24 hours | Title / description / duration are effectively static. |
| Video stats | 1 hour | Views tick but not fast enough to require per-request refresh. |
| Channel metadata | 6 hours | Sub-count, channel thumbnail — slow-moving. |
| Channel uploads | 1 hour | New uploads surface within an hour. |
| Captions | 24 hours | Static once published. |

**Code reference:** `youtube_cache.py` — full module. `_youtube_api_get` calls `youtube_cache.get()` before reserving quota and hitting the network (see §6.4 of that file's docstring).

Cache hits are counted and exposed via the admin observability endpoint so we can verify hit rate is high enough to justify the quota request.

---

## 7. Privacy, Compliance, and TOS Conformance

- **Data scope:** OAuth-obtained data is used only for the authenticated creator's own account. Data is never shared between creators or exposed on any public surface.
- **Data retention:** The YouTube-derived cache persists for a maximum of 24 hours per-kind (see §6). The reference corpus of PUBLICLY trending videos retains the top-200 entries per niche for learning purposes, updated via the public `videos.list?chart=mostPopular` endpoint.
- **Token handling:** Refresh tokens are stored server-side only, encrypted at rest, never sent to the frontend. Access tokens are refreshed server-side, never persisted beyond the request lifecycle.
- **Revocation:** Creators can disconnect from Studio Settings. The Disconnect action calls Google's `/revoke` endpoint and deletes the refresh token from `youtube_connections.json`.
- **Attribution:** YouTube branding is displayed on every Studio surface that shows YouTube-sourced data, per the YouTube API Services Branding Guidelines.
- **TOS conformance:** Studio does not scrape rate-limited or authenticated-only data outside of OAuth scopes. Studio does not re-distribute YouTube data to third parties. Studio does not attempt to circumvent YouTube rate limits or quota caps — as evidenced by the persistent quota tracker enforcing an account-level cap.

---

## 8. Observability — How a Reviewer Can Verify Live Behavior

Studio exposes admin-only observability endpoints that a reviewer can request from the operator:

- `GET /api/admin/youtube-quota` → returns today's per-method quota spend, 7-day history, and cache hit-rate statistics.
- `GET /api/admin/catalyst/corpus` → returns the reference corpus snapshot (public trending only, no private data).
- `POST /api/admin/catalyst/backfill-tick` → admin-initiated manual trigger of the reference-corpus refresh.

Any reviewer requiring a live demonstration can request credentials and be walked through these endpoints on a recorded screenshare.

---

## 9. Attachments

If the review team requires additional artifacts:

- **Source code** — full repository is available at `https://github.com/hauntinghd/studionyptixd`. The `youtube.py`, `youtube_quota.py`, `youtube_cache.py`, `catalyst_backfill.py`, and `backend_catalyst_learning.py` modules are the primary files referenced in this document.
- **Screen recording** — the operator can provide a recorded walkthrough of a creator connecting a channel, viewing their analytics, and seeing Catalyst's suggestions. Please indicate format preference and length.
- **Architecture diagram** — available on request (drawio / PDF).

---

## Contact

- **Technical:** Casey Setter · `atlassetter@nyptidindustries.com`
- **Product:** `omatic657@gmail.com`
- **Project:** NYPTID Studio / Catalyst
- **API Client domain:** `api-studio.nyptidindustries.com`
