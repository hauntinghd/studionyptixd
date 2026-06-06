# Google Search Console — HTML file verification

If Search Console gives you an **HTML file** (not the meta tag), drop the file Google names here, e.g.:

`google1234567890abcdef.html`

Vite copies everything under `public/` to the site root on deploy, so it will be live at:

`https://studio.nyptidindustries.com/google1234567890abcdef.html`

Then click **Verify** in Search Console.

**Preferred for OAuth (all subdomains):** verify `nyptidindustries.com` as a **Domain** property via DNS TXT at your DNS host — see `GOOGLE_DOMAIN_VERIFICATION.md` in the repo root.
