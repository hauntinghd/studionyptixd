# Google YouTube OAuth Setup

Studio now supports two YouTube OAuth paths:

1. Preferred `web` mode
   - Uses the backend callback at `https://api.nyptidindustries.com/api/oauth/google/youtube/callback`
   - Best UX when you have an active Google Web application OAuth client
2. Fallback `installed` mode
   - Uses the repo's `client_secrets.json` desktop client
   - Opens Google consent, then asks the user to paste the final `http://localhost/...` redirect URL back into Studio
   - Useful when the web OAuth client is suspended, disabled, or not ready yet

## Preferred Google Cloud Setup
1. Create or choose the Google Cloud project that should own the live YouTube integration.
2. Enable these APIs:
   - YouTube Data API v3
   - YouTube Analytics API
   - YouTube Reporting API
3. Configure the OAuth consent screen and add these scopes:
   - `https://www.googleapis.com/auth/youtube.readonly`
   - `https://www.googleapis.com/auth/yt-analytics.readonly`
   - `https://www.googleapis.com/auth/youtube.force-ssl`
4. Create an OAuth client of type `Web application`.
5. Add this authorized redirect URI for production:
   - `https://api.nyptidindustries.com/api/oauth/google/youtube/callback`

## Backend Configuration
Web mode:
- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `GOOGLE_REDIRECT_URI=https://api.nyptidindustries.com/api/oauth/google/youtube/callback`
- `YOUTUBE_API_KEY`
- Optional: `YOUTUBE_OAUTH_MODE=web`

Installed fallback mode:
- Ship `client_secrets.json` with the backend image
- Set `YOUTUBE_OAUTH_MODE=installed`
- Keep `YOUTUBE_API_KEY` configured for public inventory fallback and channel discovery

Automatic mode:
- `YOUTUBE_OAUTH_MODE=auto`
- Studio prefers the web client when it is configured cleanly and falls back to the installed client when only that path is available

## Important
- If Google returns errors about a disabled OAuth client or a suspended consumer project, the deployed backend is still using the wrong Google web client. Switch to `YOUTUBE_OAUTH_MODE=installed` or replace the live `GOOGLE_CLIENT_ID` and `GOOGLE_CLIENT_SECRET`.
- Existing refresh tokens stay tied to the client they were minted under. After switching OAuth clients, reconnect each affected channel so fresh refresh tokens are stored.
