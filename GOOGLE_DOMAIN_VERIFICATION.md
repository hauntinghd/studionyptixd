# Google domain verification — NYPTID Studio

Google needs proof you own the domain before OAuth branding and Search Console data apply to production.

## Which property to add

| Goal | Search Console property type | Best method |
|------|------------------------------|-------------|
| OAuth consent screen (“Verified domains”) | **Domain** → `nyptidindustries.com` | **DNS TXT** (covers `studio.`, `api-studio.`, etc.) |
| Studio homepage only | **URL prefix** → `https://studio.nyptidindustries.com/` | **HTML tag** or **HTML file** on Vercel |

For OAuth, use **Domain** `nyptidindustries.com` + DNS TXT.

---

## Option A — DNS TXT (recommended for OAuth)

1. Open [Google Search Console](https://search.google.com/search-console) → **Add property** → **Domain**.
2. Enter: `nyptidindustries.com`
3. Google shows a TXT record like:
   - **Host / name:** `@` (or leave blank — depends on DNS UI)
   - **Value:** `google-site-verification=XXXXXXXXXXXXXXXX`
4. At your DNS provider (Cloudflare, Namecheap, Vercel DNS, etc.) for **nyptidindustries.com**, add a **TXT** record with that exact value.
5. Wait 5–30 minutes (sometimes up to 48h). Click **Verify** in Search Console.
6. In [Google Cloud Console → OAuth consent screen](https://console.cloud.google.com/apis/credentials/consent), confirm **nyptidindustries.com** appears under verified domains.

No code deploy required for this path.

---

## Option B — HTML meta tag (URL prefix: studio subdomain)

1. In Search Console, add **URL prefix** property: `https://studio.nyptidindustries.com/`
2. Choose **HTML tag**. Google gives a `content="..."` token (long alphanumeric string).
3. In Vercel → project **studio-frontend-asd** → **Settings → Environment Variables** (Production):
   - Name: `VITE_GOOGLE_SITE_VERIFICATION`
   - Value: paste the token only (not the full meta tag)
4. Redeploy production (or push to trigger deploy).
5. Confirm the live homepage HTML contains:
   ```html
   <meta name="google-site-verification" content="YOUR_TOKEN" />
   ```
   View source on https://studio.nyptidindustries.com/
6. Click **Verify** in Search Console.

---

## Option C — HTML file upload (URL prefix)

1. Search Console → **HTML file** method → download or note the filename (e.g. `googleabc123.html`).
2. Copy that file into:
   `ViralShorts-App/public/google-site-verification/`  
   (or directly into `ViralShorts-App/public/` so it sits at the site root)
3. Deploy Vercel production.
4. Open `https://studio.nyptidindustries.com/googleabc123.html` in a browser — you should see the verification string Google gave you.
5. Click **Verify**.

---

## After verification

- OAuth consent screen: **Verified domains** should list `nyptidindustries.com`.
- Continue [OAUTH_PUBLISH_RUNBOOK.md](OAUTH_PUBLISH_RUNBOOK.md) Step 5 (branding + restricted-scope review).

## Troubleshooting

| Issue | Fix |
|-------|-----|
| DNS TXT not found | Remove old Google verification TXT records; only one active token per attempt. |
| HTML tag not seen | Ensure `VITE_GOOGLE_SITE_VERIFICATION` is set on **Production** in Vercel and redeploy. |
| File URL 404 | File must live under `public/` before build; SPA rewrite must not override — static files in `dist/` are served first on Vercel. |
| Verified in Search Console but not OAuth | Use **Domain** property on apex `nyptidindustries.com`, not only a subdomain URL prefix. |
