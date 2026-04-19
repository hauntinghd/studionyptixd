# OAuth Publish Runbook — NYPTID Studio

**Goal:** Move the Google OAuth consent screen from **Testing** → **In production / Published** so external users can connect their YouTube channels AND refresh tokens stop expiring after 7 days.

**Why this matters:**
- In **Testing** mode, Google invalidates refresh tokens after **7 days** regardless of stored state. Every connected channel has to re-authorize weekly. This is the primary cause of "my channels keep disconnecting."
- Testing mode also caps at 100 external test users total.
- Publishing is the only permanent fix.

**Audience:** Casey / NYPTID operator. These are manual Google Cloud Console steps — not something the backend does automatically.

---

## Prerequisite state (already done in this repo)

- ✅ OAuth scopes requested in code: `youtube.readonly`, `yt-analytics.readonly`, `youtube.force-ssl`, `youtube.upload` ([youtube.py:53-58](youtube.py:53))
- ✅ Refresh-token persistence to Supabase ([youtube_connections_store.py](youtube_connections_store.py))
- ✅ Token auto-refresh on expiry ([youtube.py:2006-2063](youtube.py:2006))
- ✅ Disconnect flow calls Google revoke endpoint ([youtube.py:2875-2879](youtube.py:2875))
- ✅ Privacy policy live at `https://studio.nyptidindustries.com/privacy`
- ✅ Terms of Service live at `https://studio.nyptidindustries.com/terms`
- ✅ YouTube API Services compliance doc drafted ([YOUTUBE_API_QUOTA_REVIEW.md](YOUTUBE_API_QUOTA_REVIEW.md)) — submit alongside the consent screen verification

---

## Step 1 — Finalize the OAuth consent screen

Open [Google Cloud Console → APIs & Services → OAuth consent screen](https://console.cloud.google.com/apis/credentials/consent) in the GCP project that owns the live Studio web client.

### App information
| Field | Value |
|---|---|
| User Type | **External** |
| App name | **NYPTID Studio** |
| User support email | `atlassetter@nyptidindustries.com` |
| App logo | Upload `ViralShorts-App/public/logo.png` (must be exactly 120×120 px, PNG/JPG/BMP, ≤1 MB) |
| Application home page | `https://studio.nyptidindustries.com` |
| Application privacy policy link | `https://studio.nyptidindustries.com/privacy` |
| Application terms of service link | `https://studio.nyptidindustries.com/terms` |

### Authorized domains
Add these so Google accepts the URLs above:
- `nyptidindustries.com`

(Google only accepts the apex domain; all subdomains are implicitly authorized.)

### Developer contact information
- `atlassetter@nyptidindustries.com`

Click **Save and continue**.

---

## Step 2 — Scopes

Click **Add or remove scopes** and confirm all four are present:

| Scope | Classification | Why Studio needs it |
|---|---|---|
| `https://www.googleapis.com/auth/youtube.readonly` | Sensitive | Fetch channel + video metadata (titles, stats, thumbnails) |
| `https://www.googleapis.com/auth/yt-analytics.readonly` | Sensitive | Pull CTR, impressions, retention for Catalyst |
| `https://www.googleapis.com/auth/youtube.force-ssl` | Sensitive | HTTPS-only operations, required for `youtube.upload` and `captions.list` |
| `https://www.googleapis.com/auth/youtube.upload` | **Restricted** | Publish Studio-made videos to the creator's channel on explicit user action |

The `youtube.upload` scope is **Restricted**, which is the blocker that triggers the mandatory security assessment — see Step 5.

Click **Save and continue**.

---

## Step 3 — Test users (only relevant while still in Testing)

While Google reviews the submission you'll remain in Testing mode. Add as test users:
- Your own Google accounts (up to 100 total)
- Any beta creators you want connected before the app is published

Once published this list becomes irrelevant.

Click **Save and continue** → **Back to dashboard**.

---

## Step 4 — Domain verification (one-time)

Google requires proof you own `nyptidindustries.com` before publishing.

1. Go to [Google Search Console](https://search.google.com/search-console).
2. Add **nyptidindustries.com** as a **Domain property** (not URL prefix — domain proves all subdomains).
3. Verify via DNS TXT record (recommended — survives site moves). Google will show you the TXT value. Add it at your DNS provider for `nyptidindustries.com`.
4. Wait for DNS propagation (usually minutes). Click **Verify**.
5. Back on the OAuth consent screen, the domain should now appear under **Verified domains**.

---

## Step 5 — Submit for verification

On the OAuth consent screen dashboard, click **Publish app**.

Because you have at least one **Restricted** scope (`youtube.upload`), Google will prompt you to submit for verification. The flow will ask for:

### 5a — Demo video
Record a 2–5 minute screencast showing:
1. A creator lands on `https://studio.nyptidindustries.com` and signs up / signs in.
2. Creator navigates to Settings → YouTube.
3. Clicks **Connect YouTube Channel**.
4. Google consent screen appears showing all 4 scopes — zoom in so reviewer can read each scope and justification.
5. User approves. Studio confirms the channel is connected.
6. Demonstrate **each scope in actual use**:
    - `youtube.readonly`: Catalyst shows channel audit / recent uploads
    - `yt-analytics.readonly`: Analytics dashboard shows impressions + CTR
    - `youtube.force-ssl`: (Covered by the upload flow since `upload` requires force-ssl)
    - `youtube.upload`: User clicks "Publish to YouTube" on a rendered Studio video, selects channel, and upload completes
7. Show the **Disconnect** button and have the reviewer watch the token revocation flow (check network tab hitting `oauth2.googleapis.com/revoke`).
8. Narrate throughout. Upload to YouTube as **unlisted** and paste the link in the verification form.

### 5b — Security assessment (only for Restricted scopes)
Google requires an independent security assessment by one of their approved CASA assessors for apps using Restricted scopes in production. This is the big step and can take weeks to months.

**Current options (ranked):**
1. **Submit, accept the delay.** The assessment has a cost (commonly $6k–$15k) and takes 6–12 weeks. Studio continues to operate in Testing mode during review — every test user still re-authorizes every 7 days. **This is the production-ready path.**
2. **Split into two OAuth clients (recommended interim):**
    - Client A — read-only: `youtube.readonly` + `yt-analytics.readonly` + `youtube.force-ssl` (Sensitive only, no Restricted). This can publish without the security assessment — only a standard Sensitive-scope verification.
    - Client B — upload-only: `youtube.upload` + `youtube.force-ssl` for the publish flow. Keep this in Testing mode until the security assessment clears.
    - See **Appendix A** below for the code changes.
3. **Stay in Testing mode for Casey's own channels only.** Re-auth every 7 days is annoying but works. This is the cheapest short-term option but blocks the "publish to external users" goal.

### 5c — Justify each scope in writing
The form asks for per-scope justification. Use this text (matches [YOUTUBE_API_QUOTA_REVIEW.md §2](YOUTUBE_API_QUOTA_REVIEW.md)):

> **youtube.readonly** — Required to read the creator's channel metadata (title, subscriber count, uploads playlist ID) and per-video metadata (titles, descriptions, statistics, thumbnails). This data powers the Catalyst research engine, which analyzes the creator's own past performance to suggest better titles and thumbnails for new videos.

> **yt-analytics.readonly** — Required to read per-video retention curves, impression counts, click-through rates, traffic sources, and average view duration. These are the metrics Catalyst uses to learn what works on this specific creator's channel — CTR signals what thumbnails convert, retention signals what opening hooks hold viewers. Without this scope the entire Catalyst learning loop is impossible.

> **youtube.force-ssl** — Required by Google for any upload operation, for captions.list when pulling a creator's own video back into Studio for re-editing, and for thumbnails.set when publishing a custom thumbnail.

> **youtube.upload** — Required only when the creator explicitly clicks "Publish to YouTube" on a finished Studio video. The scope is never used automatically or in the background. A single click in Studio's UI corresponds to a single videos.insert call. No videos are uploaded without explicit per-video confirmation.

### 5d — Attachments
Attach the YouTube API Services compliance document that's already in the repo:
- [YOUTUBE_API_QUOTA_REVIEW.md](YOUTUBE_API_QUOTA_REVIEW.md) — this was drafted specifically for this review.

Click **Submit**.

---

## Step 6 — What happens next

| Phase | Who | Typical duration |
|---|---|---|
| Sensitive-scope verification (automated + manual review) | Google Trust & Safety | 1–4 weeks |
| Restricted-scope security assessment | Independent CASA assessor + Google | 6–12 weeks |
| Approval → app is **In production** | Google | Takes effect within hours of final approval |

Google emails `atlassetter@nyptidindustries.com` with status updates and any follow-up questions. Answer within 48 hours — delayed responses reset the clock.

---

## Step 7 — After approval

1. Refresh tokens issued from this point onward no longer expire after 7 days (they effectively live until the user revokes or Google detects abuse).
2. Existing refresh tokens issued **during** Testing mode will still be the old 7-day-TTL tokens — each connected channel has to reconnect once to mint a new long-lived token under the published client.
3. Studio users signing up from this point will see the "Verified by Google" badge on the consent screen — which substantially improves conversion.

Email all current test users with a one-click reconnect prompt after publish.

---

## Appendix A — Two-client split (interim)

If you want to ship read-only analytics to users without waiting for the Restricted-scope assessment:

### Backend changes

Add two env var sets:

```bash
# Primary (read-only, publishable via Sensitive review only):
GOOGLE_CLIENT_ID=<read-only client id>
GOOGLE_CLIENT_SECRET=<read-only client secret>
GOOGLE_REDIRECT_URI=https://api-studio.nyptidindustries.com/api/oauth/google/youtube/callback

# Upload (stays in Testing until security assessment clears):
GOOGLE_UPLOAD_CLIENT_ID=<upload client id>
GOOGLE_UPLOAD_CLIENT_SECRET=<upload client secret>
GOOGLE_UPLOAD_REDIRECT_URI=https://api-studio.nyptidindustries.com/api/oauth/google/youtube/upload-callback
```

Modify `YOUTUBE_SCOPES` in [youtube.py:53-58](youtube.py:53) to default to read-only scopes only, and introduce a second `YOUTUBE_UPLOAD_SCOPES` list that's requested in a separate flow triggered only when the user clicks "Publish to YouTube" for the first time.

This is a one-file change plus backend_settings.py env wiring. Non-trivial but well-bounded.

### Why this is worth considering

- **Unblocks Catalyst Phase 2 immediately.** Creators can connect channels, refresh tokens last indefinitely, and the analytics dashboard populates — without waiting for the security assessment.
- **Upload continues to work** for Casey's own channels (which stay in the test-user list on the upload client).
- **Defers the expensive assessment** until the product is generating enough revenue to justify the cost.

---

## Appendix B — Quick sanity checks

Before submitting for verification, verify the following over the live site:

```bash
# 1. Consent screen renders Studio's branding
open https://accounts.google.com/o/oauth2/v2/auth?client_id=$GOOGLE_CLIENT_ID&redirect_uri=https://api-studio.nyptidindustries.com/api/oauth/google/youtube/callback&response_type=code&scope=https://www.googleapis.com/auth/youtube.readonly&access_type=offline&include_granted_scopes=true

# 2. Privacy + Terms reachable
curl -I https://studio.nyptidindustries.com/privacy
curl -I https://studio.nyptidindustries.com/terms

# 3. Backend health
curl -I https://api-studio.nyptidindustries.com/api/oauth/google/youtube/start

# 4. Logo exists
curl -I https://studio.nyptidindustries.com/logo.png
```

All four must return 200 OK (or the appropriate auth challenge for the OAuth URL).

---

## Appendix C — Common verification rejections and fixes

| Rejection reason | Fix |
|---|---|
| "Privacy policy does not adequately describe Google user data handling" | Ensure the privacy page names the specific scopes AND describes what data each scope accesses. Our [/privacy](ViralShorts-App/src/studio/pages/PrivacyPage.tsx) already does this — §3 covers it. |
| "Homepage does not match authorized domain" | Make sure `studio.nyptidindustries.com` is the actual landing URL and that the homepage link in the consent screen is exactly `https://studio.nyptidindustries.com` (no trailing slash / no redirect). |
| "Unable to verify domain ownership" | Redo Step 4 with a DNS TXT record. HTML file verification is fragile behind CDNs. |
| "Demo video does not show scope in use" | Re-record making sure each scope triggers a visible network call or UI change. Zoom in on the scopes list during the consent screen. |
| "Limited Use disclosure missing" | The exact language is already in [privacy](ViralShorts-App/src/studio/pages/PrivacyPage.tsx) §3 — copy that same paragraph into the Google verification submission text box if asked. |

---

## Ownership

- **Primary:** Casey (`atlassetter@nyptidindustries.com`)
- **Escalation path for Google questions:** Casey — Google email threads in the product inbox
- **Code owner for OAuth flow:** `youtube.py` module

Last verified: 2026-04-19
