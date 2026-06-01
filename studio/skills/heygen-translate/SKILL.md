---
name: heygen-translate
description: >-
  Translates and dubs an EXISTING video into another language via HeyGen Video Translation v3 (voice clone + lip re-sync). Load when localizing/dubbing a finished video or podcast. NOT for creating new videos — that's heygen-avatar-video.
---

# Skill — HeyGen Video Translation

This is the operational knowledge for translating and dubbing an **existing** video into another language. The HeyGen Video Translation engine keeps the presenter's face, clones their voice into the target language, re-syncs their lips to the new audio, and optionally burns in captions. You supply a source video + a target language; the engine handles transcription, translation, voice cloning, lip-sync, and captions.

This is **not** new-video generation. The presenter, performance, framing, and brand assets in the original are preserved — translation rides on top of what's already there. If the user wants to *create* a brand-new video in another language (no source video exists yet), that's the `heygen-avatar-video` skill instead — write the script in the target language there. Use this skill only when there is an existing source video to localize.

**Use when:** "translate this video to Spanish", "dub this into Japanese", "make this in French and German", "I need this in 10 languages for a launch", "translate my podcast but keep my video", "localize this clip", "subtitle and dub this".

**NOT for:** creating new videos from scratch (use `heygen-avatar-video`), avatar creation, TTS-only synthesis, or text-only translation.

---

## 1. How it runs on this platform (read first)

HeyGen Video Translation is just another HeyGen generation endpoint, so it follows **exactly the same rules as `heygen-avatar-video`** — there is no HeyGen CLI, no MCP plugin, and no OpenClaw here:

- **Use the HeyGen v3 API.** This skill targets the current **`/v3/video-translations`** endpoints (not the legacy `/v2/video_translate`). The platform's cost-tracking and auto-poll are wired for the v3 path.
- **Call the proxied HTTP API.** Read the host from `$HEYGEN_API_BASE_URL` and the key from `$HEYGEN_API_KEY` (auth header `x-api-key`). **Never** hardcode `https://api.heygen.com` and never reach for a `heygen` CLI command — that bypasses RookCast's auth injection, credit gating, cost tracking, and BYOK/OAuth routing. Everything routes through the env vars.
- **Submit via a Python script, not shell curl.** There's a JSON body and (for local files) an upload step — that's the case for one Python script with real dict/variable construction (see §4). `requests` is pre-installed.
- **One target language per submit.** `/v3/video-translations` accepts an `output_languages` array, but the platform tracks one async job per request. **Submit ONE language per call** so each translation is independently tracked and auto-downloaded. For several languages, loop and POST once per language (each its own gated, tracked job).
- **Generation is async and auto-tracked.** A successful submit returns `data.video_translation_ids` (a one-element array when you submit one language). That means *accepted*, not done. Print the status code AND the full body, confirm the id, then **end your turn.** The platform polls `GET /v3/video-translations/{id}`, downloads the result when `status` is `completed`, and notifies you with the saved path — the same flow as avatar video. **Do NOT call `poll_endpoint`, do NOT write a polling loop, and do NOT announce completion yourself.**
- **Submission is confirmation-gated.** The proxy holds the POST connection until the user approves in the UI (can take a minute+). Use a generous client timeout (`requests` default is fine; if you set one, ≥300s). A POST that "times out" after a short wait is almost always your own client timeout firing during approval — do NOT retry (each retry stacks another pending approval).
- **Account scoping applies.** `$ROOKCAST_HEYGEN_AUTH` (`platform`/`byok`/`oauth`/`mcp`) tells you whose account renders this turn. Translation runs on a supplied source video (not an account-scoped avatar), so it works on the platform account — but `brand_voice_id` (if used) is account-scoped like any voice id.
- **When a call fails, `WebFetch` the reference before retrying.** The v3 reference: `https://developers.heygen.com/docs/video-translate` (guide), `https://developers.heygen.com/reference/create-video-translation` (submit), `https://developers.heygen.com/reference/get-video-translation` (status). The bundled `${WORKSPACE_ROOT}/docs/providers/heygen.txt` also covers it — grep for `video-translations`. One read of the right page beats blind retries on a paid endpoint.

---

## 2. User-facing behavior

1. **Be concise.** Don't dump translation ids, raw payloads, or status JSON in chat. Report the result (video link/path, language), not the plumbing.
2. **No internal jargon.** Don't say "polling", "video_translation_id", "v3 endpoint". Say "translating", "almost done", "your file".
3. **Polling is silent and handled by the platform** — say the translation has started, then end your turn. Speak again only when the completion notification arrives (or on a hard failure).
4. **One result, one message.** When a language is done, send the link/path plus a one-line summary (language, duration, mode). Deliver each language as it completes — don't wait for all.
5. **Communicate in the user's language.** Detect from their messages. Questions/confirmations in their language; technical directives stay in English.

---

## 3. Workflow — four phases

Phase 1 (Discovery) is the only place you ask questions. Phase 2 (Pre-flight) is silent. Phase 3 (Submit) ends your turn. Phase 4 (Deliver) happens on the completion notification.

### Phase 1 — Discovery

Ask only what you don't already have. One or two questions per turn, **never a form.**

**Required (block until you have these):**

1. **Source video.** A public HTTPS URL, a local sandbox file path, or a HeyGen `asset_id` from a prior step. If missing, ask: *"What's the source video — a URL or a file?"*
2. **Target language(s).** Open-ended — *"Which language should I translate it into?"* Don't present a picker; let the user type freely (one language, several, or a region-specific variant). Validate against the canonical list in Phase 2.

**Important (ask if not provided; smart defaults):**

3. **Speaker count.** Default 1. Ask once when ambiguous: *"How many distinct speakers are in the video?"* Wrong speaker count is the **#1 quality killer** — it makes voices bleed across speakers. Don't skip for multi-person content.
4. **Content type.** Usually infer and confirm — the five profiles in Phase 2 cover ~95% of cases. Only ask if genuinely ambiguous.
5. **Caption preference.** Default ON for talking-head/corporate; OFF for podcast/audio-only. Mention briefly in Phase 4 if you flip the default.
6. **Duration flexibility.** Ask: *"Does the translation need to be exactly the same length as the original, or can it run slightly longer/shorter? Flexibility usually sounds more natural."* Default flexible (`enable_dynamic_duration: true`, which is also the API default). Set `false` only for frame-exact timing (timeline/ad-slot sync).

**Optional (only if relevant):**

7. **Glossary / do-not-translate terms.** For corporate/technical content: *"Any product or company names I should keep in the original language?"* HeyGen has no hard glossary field on submit — this becomes guidance for the proofread step when stakes are high.
8. **Proofread before final render?** Default OFF. Default ON for: long videos (>3 min), corporate/branded, high-stakes legal/medical/educational, or languages the user reads natively. Ask: *"Want to review and edit the subtitles before final render? Adds ~5 min but lets you fix any wrong terms."*

### Phase 2 — Pre-flight (silent)

**2a — Language validation.** Fetch the canonical supported-languages list and match the user's input case-insensitively against the exact strings ("Spanish", "Spanish (Spain)", "Chinese (Mandarin, Simplified)", "Arabic (Saudi Arabia)"):

```
GET  $HEYGEN_API_BASE_URL/v3/video-translations/languages    (x-api-key header)
```

If the user says "Spanish", default to the standard variant and confirm in Phase 4. If they specify a region ("Mexican Spanish"), map it ("Spanish (Mexico)"). No match → present closest options.

**2b — Source video routing.** v3 takes a `video` object discriminated by `type`:

| Source the user gave you | `video` value |
|---|---|
| Public HTTPS URL (no auth, returns video on `HEAD`) | `{"type": "url", "url": "<url>"}` |
| Auth-walled URL, 403/404, or HTML response | Tell the user; ask for a public URL or a local file |
| Local sandbox file | **Upload via the sandbox upload-url flow** (see §4) → pass the returned public URL as `{"type": "url", "url": "<public-url>"}`. Do NOT POST multi-MB files through the proxy (4.5 MB Function cap → `Backend proxy unreachable: fetch failed`). |
| Existing HeyGen asset id | `{"type": "asset_id", "asset_id": "<id>"}` |

**2c — Content profile.** Pick one silently; only ask if genuinely ambiguous.

| Profile | Use when | Flags |
|---|---|---|
| **Talking head / presenter** (default) | One person to camera, clean audio | `mode: "precision"`, `enable_speech_enhancement: true`, `enable_caption: true`, `enable_dynamic_duration: true` |
| **Podcast / audio-only** | Visual is static or absent | `mode: "precision"`, `translate_audio_only: true`, `enable_speech_enhancement: true`, `enable_caption: true` |
| **Music / high-soundtrack** | Background music interferes with speech | `mode: "precision"`, `disable_music_track: true`, `enable_speech_enhancement: true`, `enable_dynamic_duration: true` |
| **Multi-speaker** | Two+ distinct speakers | Talking-head defaults + `speaker_num: <count>` (REQUIRED — don't guess) |
| **Corporate / branded** | Brand voice, glossary discipline, high stakes | Talking-head defaults + `brand_voice_id` (if the user has one). Strongly consider proofread. |

**Always:** `mode: "precision"` unless the user explicitly asks for "fast"/"speed" (then `"speed"`, the API default). Dynamic duration especially matters for high-compression pairs (see §5).

> **Cost note:** the active rate depends on these flags — `mode: "precision"` bills at the precision-lipsync rate, `"speed"` at the speed-lipsync rate, and `translate_audio_only: true` at the (cheaper) audio rate regardless of mode. Pick `precision` deliberately on long batches.

### Phase 3 — Submit (one language per call)

For each target language, run the §4 script once. Confirm a non-empty `data.video_translation_ids` in the response, then **end your turn.** The platform tracks each job and notifies you on completion/failure with the saved path. Do not poll.

**Proofread path (proofread = ON).** For high-stakes content, run a proofread session first so the user can review/edit the translated subtitles before the engine commits to a final render. The v3 flow:
1. `POST /v3/video-translations/proofreads` (same body shape, returns `proofread_id`(s)) → poll the proofread session to `completed`.
2. `GET /v3/video-translations/proofreads/{proofread_id}/srt` → presigned URLs for the editable + original SRTs. Download the editable one.
3. Edit the SRT (glossary, register, names).
4. Host the edited SRT at a public URL (the sandbox upload-url flow — the `asset_id` route does not accept SRTs) and `PUT /v3/video-translations/proofreads/{proofread_id}/srt` with `{"srt": {"type": "url", "url": "<edited-srt-url>"}}`.
5. `POST /v3/video-translations/proofreads/{proofread_id}/generate` — kicks off the final render and returns a `video_translation_id`. Treat it like any other submission: **end your turn, platform notifies.**

`WebFetch` `https://developers.heygen.com/reference/generate-video-from-proofread` (and the surrounding proofread refs) for the exact bodies before calling — don't guess the proofread sub-resource shapes.

### Phase 4 — Deliver

One message per completed language, on the notification:

> ✅ Spanish — <video path/link>
> 1m 47s, precision mode, captions on.

If a language failed, one short line with the cause (see §6). Don't flood with retry options unless asked. Captions, if enabled, can be fetched as an SRT/VTT sidecar: `GET /v3/video-translations/{id}/caption?format=srt`.

---

## 4. Worked example — submit ONE language via a Python script

A local source file is uploaded to storage first, then HeyGen fetches it by URL (this is also how `heygen-avatar-video` handles large audio). Keep the upload and the submit in **one** script so the signed public URL stays an in-process variable — its query string is full of `&` and corrupts if pushed through shell variables/`sed`.

```python
import os, requests

base, key = os.environ["HEYGEN_API_BASE_URL"], os.environ["HEYGEN_API_KEY"]

# --- (only for a LOCAL source file) upload to storage, get a public URL ---
backend, sid = os.environ["ROOKCAST_BACKEND_URL"], os.environ["RUN_SESSION_ID"]
up = requests.post(f"{backend}/api/sandbox/upload-url?_sid={sid}",
                   json={"filename": "source.mp4", "contentType": "video/mp4"}).json()
with open("source.mp4", "rb") as f:
    requests.put(up["uploadUrl"], data=f, headers={"Content-Type": "video/mp4"})
video_url = up["publicUrl"]          # stays intact as a variable — never string-substitute it
# (If the user gave a public URL, skip the block above and set video_url = "<their url>".)

# --- submit ONE language (loop this per language for a batch) ---
body = {
    "video": {"type": "url", "url": video_url},
    "output_languages": ["Spanish"],     # ONE language per request
    "title": "launch-promo-es",
    "mode": "precision",                 # "speed" if the user wants fast turnaround
    "enable_speech_enhancement": True,
    "enable_caption": True,
    "enable_dynamic_duration": True,
    # "speaker_num": 2,                  # include only when multi-speaker is known
    # "translate_audio_only": True,      # podcast / no-face source
}
r = requests.post(f"{base}/v3/video-translations", headers={"x-api-key": key}, json=body)
print(r.status_code)
print(r.text)   # {"data":{"video_translation_ids":["tr_..."]}} = ACCEPTED. An "error"/empty ids = failure.
```

Then **end your turn.** A non-empty `video_translation_ids` means accepted, not finished. Confirm the field shape matches the reference before assuming success — a 2xx without an id means you're on the wrong endpoint/payload; re-read the reference and resubmit. Do not write a polling loop; the platform notifies you.

---

## 5. Embedded expertise

The defaults above cover the common case. These are judgement calls, not a checklist to recite.

### Speaker count is the #1 quality killer
Talking-head = 1. Interviews/podcasts/panels: count exactly, don't guess. The engine separates voices by `speaker_num`; a wrong count makes voices bleed across speakers. If unsure, ask the user to scrub the video and count.

### Source-quality triage (before submitting)
A 30-second triage saves 10–30 min of bad translation. Read the source (you can `Read` a downloaded frame / inspect with `ffprobe`) and check the first ~10s:
- **Audio:** speech clear? music dominant? noise? → unclear speech ⇒ `enable_speech_enhancement: true`; dominant music ⇒ `disable_music_track: true`; both ⇒ warn quality may be lower regardless.
- **Face visibility:** front-facing, well-lit, on-camera most of the time? Heavy occlusion (sunglasses, hands), profile-only, fast cuts, or sub-720p faces all cap lip-sync quality.
- **Burned-in source captions:** they will NOT be re-rendered — they stay in the source language. If the user wants new-language captions too, they'll have two tracks: propose `enable_caption: true` AND warn about the existing burn-in.

### Locale-pair gotchas
- **Tonal compression/expansion.** en→zh/ja/ko run ~30% shorter; de→en, ja→en run longer; en→ar/he expand. Dynamic duration matters most here — without it, en→zh sounds artificially slow. If the user chose fixed-length, warn quality degrades on high-compression pairs.
- **Formality/register.** ja (敬語), ko (honorifics), de (Sie/du), th (royal/polite/casual), id (formal/colloquial) — the engine defaults to neutral-formal. If the source is conversational and register matters, flag for proofread or pre-warn it'll sound more formal.
- **RTL languages.** Arabic, Hebrew, Urdu, Persian render captions right-to-left and can collide with lower-third source graphics. If the source has on-screen text, propose audio-only OR proofread with caption review.
- **Regional variants matter.** Spanish (Spain) vs (Mexico) vs (Argentina); Portuguese (Portugal vs Brazil); French (France vs Canada); Arabic (19 variants). Default to the audience region; ask once if unspecified for Spanish.
- **Mandarin.** "Chinese (Mandarin, Simplified)" for mainland; "Chinese (Cantonese, Traditional)" for HK/diaspora; "Chinese (Taiwanese Mandarin, Traditional)" for Taiwan. Not interchangeable.

### Lip-sync ceiling
Best on: stable front-facing shots, ≥720p faces, clean lighting, long takes. Degrades on: profile/looking-down/occluded faces, fast cuts (<2s), low light, motion blur, heavy gesturing. If the source has these, warn in Phase 1/2: *"the source has [X], so lip-sync won't be as tight — proceed anyway, or switch to audio-only?"*

### Captions: burned-in vs sidecar
`enable_caption: true` produces captions you can pull as an SRT/VTT sidecar (`GET /v3/video-translations/{id}/caption`). For restyle-friendly captions (brand kit, language-specific font), the proofread workflow gives you an editable SRT before final render.

### Audio-only translation
`translate_audio_only: true` skips lip-sync and returns an audio file (usually MP3). Use for podcasts, audio you'll re-composite later, or cases where lip-sync is impossible (no face / very poor source). Tell the user it's a translated audio track to composite over the original — do NOT pitch it as a "quality workaround" for bad lip-sync; it's a different deliverable.

### Cost & time awareness
Translation bills by source duration × language count (a 5-min video × 5 languages = 25 billable minutes), and the per-minute rate depends on `mode`/`translate_audio_only`. Quote source-minutes × languages and an honest render-time range (≈10–20 min per language); don't quote dollars (varies by plan/account).

---

## 6. Failure-mode decoder

| Symptom | Likely cause | Fix |
|---|---|---|
| `400` "video URL not accessible" | URL needs auth, returned HTML, or wrong MIME | Ask for a public URL or local file → upload-url flow |
| `400` "language not supported" | String didn't match the canonical list | Re-fetch `/v3/video-translations/languages`; present closest matches |
| `failed` "audio extraction" | No audible speech / corrupted audio / wrong codec | Verify the source has speech; consider re-encoding |
| `failed` "speaker detection" | `speaker_num` mismatch, or audio too noisy | Resubmit with the correct speaker count or `enable_speech_enhancement: true` |
| Stuck a long time in `running` | Backend queue / occasional stalls | Don't poll — the platform handles it; if a completion notification never arrives, tell the user it's taking unusually long |
| Lip-sync looks bad on output | Source face conditions (see §5 ceiling) | Reframe expectation; offer audio-only as an alternative |
| Captions in wrong direction | RTL language burned-in caption colliding with source layout | Switch to proofread + sidecar SRT |

---

## 7. Anti-patterns

- **Never use a HeyGen CLI or `mcp__heygen__*` tool, and never hardcode `api.heygen.com`.** Always go through `$HEYGEN_API_BASE_URL` + `x-api-key` so auth, gating, and cost tracking apply. (HeyGen's public skills assume a CLI/MCP that does not exist in this sandbox.)
- **Never submit multiple languages in one request.** The platform tracks one job per request — batch ids beyond the first won't be downloaded. One language per POST.
- **Never use the legacy `/v2/video_translate` endpoint.** This platform's tracking is wired for v3 `/v3/video-translations`.
- **Never poll the translation yourself or announce completion.** Submit, confirm the `video_translation_ids`, end your turn — the platform notifies you. (Same rule as `heygen-avatar-video`.)
- **Never guess speaker count for multi-person content** — wrong count bleeds voices across speakers.
- **Never submit without validating the target-language string** against the canonical list.
- **Never POST a multi-MB source video or SRT through the proxy.** Use the sandbox upload-url flow and pass the public URL.
- **Never invent the proofread sub-resource paths** — `WebFetch` the v3 reference for the exact path/body before calling.
- **Never pitch audio-only as a fix for bad lip-sync** — it's a separate deliverable (no video).
- **Never surface the source-quality disclaimer *after* a bad result** — raise borderline source conditions in Phase 2, before submitting.
