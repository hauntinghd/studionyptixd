---
name: heygen-avatar-video
description: >-
  Generates avatar talking-head video via HeyGen (Avatar III / IV / talking-photo). Load when producing HeyGen avatar segments. Mandatory sample gate before any batch submission.
---

# Skill — HeyGen Avatar Video

This is the operational knowledge for generating lip-synced talking-head video via the HeyGen API. HeyGen avatar generation is the single most expensive per-operation cost in the pipeline — a batch of 6 segments can take 90+ minutes and burn significant credits if settings are wrong. Every decision in this skill is designed to catch mistakes before they become expensive.

The single rule that governs every output: **never submit a batch of HeyGen segments without first rendering and presenting a single 15-second test clip for user approval.**

---

## 1. The job

HeyGen avatar video turns voice audio + a visual identity into a lip-synced talking-head video. Two character types exist:

- **`avatar`** — trained from a photo set or HeyGen's stock library. Organized as a **group** (the character) containing one or more **looks** (outfits/poses/styles). The id you render with is the **look** id — that is the value HeyGen calls `avatar_id` at video creation. The group id (from `GET /v3/avatars`) is NOT renderable. Supports full-body motion on Avatar IV/V. Requires a HeyGen subscription or platform credits.
- **`talking_photo`** — instant avatar from a single image. Has a `talking_photo_id`. Head/face animation only (body stays still). Cheaper and faster but lower quality. Suitable for quick tests or channels that accept the head-only style.

**The #1 cause of `avatar_not_found` is passing a group id where a look id is required.** `GET /v3/avatars` returns groups (each with a `looks_count`); their `id` is a group id. You must resolve the group to a look before rendering (Step 2 below). A group id — or a photo avatar submitted as `type: "avatar"` instead of `type: "talking_photo"` — returns `avatar_not_found` / `avatar look not found ... space_id: ...` even though the id is in the account.

The agent picks the right type based on what's available on the user's HeyGen account and what CHANNEL.md specifies.

---

## 2. Discovery workflow (mandatory before first generation)

Before generating any HeyGen video, the agent must know which avatar, engine, and voice to use. Never guess — always read or discover.

### Step 1: Check CHANNEL.md

Read the `## Locked Provider Assets` section. If it contains locked HeyGen settings (avatar_id, talking_photo_id, avatar_engine, voice_id), use those. Skip to Section 5 (sample gate).

### Step 2: Discover available avatars

If no locked assets exist, discover avatars in **two stages** — groups first, then looks. The renderable id (`avatar_id`) is a **look** id, never a group id.

```bash
# 2a. List avatar GROUPS (characters). The `id` here is a GROUP id with a
#     `looks_count` — it is NOT renderable. Use it only to find looks in 2b.
curl -s "$HEYGEN_API_BASE_URL/v3/avatars" \
  -H "x-api-key: $HEYGEN_API_KEY" | jq '.data[] | {id, name, looks_count, preview_image_url}'

# 2b. Resolve a chosen group to its LOOKS. Each look's `id` is the value you
#     pass as `avatar_id` at video creation (type: "avatar").
curl -s "$HEYGEN_API_BASE_URL/v3/avatars/looks?group_id=<GROUP_ID>" \
  -H "x-api-key: $HEYGEN_API_KEY" | jq '.data[] | {id, name, group_id}'

# 2c. List talking photos / instant avatars. These render as type:"talking_photo"
#     with `talking_photo_id` — NOT as type:"avatar". A group whose preview_image_url
#     is under .../talking_photo/... is a photo avatar: render it this way.
curl -s "$HEYGEN_API_BASE_URL/v1/talking_photo.list" \
  -H "x-api-key: $HEYGEN_API_KEY" | jq '.data.talking_photos[:10]'
```

> The docs back this up: in `docs/providers/heygen.txt`, "Avatar Looks" states *"A look is one outfit/pose/style for a character (avatar group). It's the value you pass as `avatar_id` to video creation,"* and "Avatar Groups" labels the `/v3/avatars` `id` field a *"Unique group identifier."* If unsure which id you're holding, grep those sections before submitting.

> **Avatar readiness gate (for freshly created avatars).** If the avatar/look was just created this session (a digital twin, photo avatar, or instant avatar), confirm it's ready before rendering: the look's `preview_image_url` must be non-null. A still-processing avatar renders to a **silent failure** — the job is accepted but produces nothing usable. Re-check the look (`/v3/avatars/looks?group_id=...`) until `preview_image_url` is populated before you submit. Locked assets in CHANNEL.md are already-ready, so this only applies to brand-new avatars.

### Step 3: Discover available voices

```bash
# List user's ElevenLabs voices (if connected)
curl -s "$ELEVENLABS_API_BASE_URL/v1/voices" \
  -H "xi-api-key: $ELEVENLABS_API_KEY" | jq '.voices[] | {voice_id, name}'
```

### Step 4: Present options and get user approval

Surface the discovered avatars and voices to the user. Let them pick. Never auto-select.

### Step 5: Lock to CHANNEL.md

After user approval, update CHANNEL.md `## Locked Provider Assets → HeyGen` with:
- Avatar engine (III / IV / V)
- character_type ("avatar" or "talking_photo")
- avatar_id or talking_photo_id
- talking_photo_style (omit by default)

And `## Locked Provider Assets → ElevenLabs` with:
- voice_id
- voice_name

---

## 2.5 Worked example — submit via a Python script, not shell curl

Generation involves a JSON body, dynamic values (the audio URL), and multiple steps (TTS → host audio → submit). That is exactly the case where you **write one Python script and run it** — never hand-assemble the body with heredocs/`sed` or chain it across separate Bash calls (each Bash call is a fresh shell; a variable from a prior call is gone, and string-substituting a signed URL with `&` in it corrupts the body). Build the body with real dict/variables, read the base URL + key from the environment, and print the response.

A **photo avatar** (the look's `avatar_type` is `photo_avatar`) renders as `type: "talking_photo"` with its **look id** as `talking_photo_id` — NOT `type: "avatar"`, and NOT `type: "photo_avatar"` (that value is not a valid discriminator). A trained **avatar** look uses `{"type": "avatar", "avatar_id": "<LOOK_ID>", "avatar_style": "normal"}`. Either way `<LOOK_ID>` is a **look** id from `/v3/avatars/looks`, never a group id from `/v3/avatars`.

```python
import os, requests

base, key = os.environ["HEYGEN_API_BASE_URL"], os.environ["HEYGEN_API_KEY"]

# audio_url is the public URL from the upload step (Section 7) — a plain variable
# here, so its signed query string (full of `&`) stays intact. No sed, no heredoc.
body = {
    "video_inputs": [{
        "character": {"type": "talking_photo", "talking_photo_id": LOOK_ID},
        "voice": {"type": "audio", "audio_url": audio_url},
        "background": {"type": "color", "value": "#000000"},
    }],
    "dimension": {"width": 1920, "height": 1080},
}
r = requests.post(f"{base}/v2/video/generate", headers={"x-api-key": key}, json=body)
print(r.status_code)
print(r.text)  # {"data":{"video_id":"..."}} = ACCEPTED, not done. An "error" field = failure even on 200.
```

Then **end your turn** — a `video_id` means the job was accepted, not finished. Do not report success, announce completion yourself, or write a polling loop; the platform watches the job and notifies you when it's ready (or failed).

**When a call fails, `WebFetch` the reference before retrying** — fetch the `doc_url` in the error body, or HeyGen's Studio API reference page. One read of the right page beats guessing; do not mutate-and-retry a paid endpoint blindly.

---

## 3. Avatar engine selection

| Engine | API endpoint | Body shape | When to use |
|---|---|---|---|
| Avatar III (legacy) | `POST /v2/video/generate` | Head-only for talking_photo; limited body for trained avatars | User explicitly requests III, or CHANNEL.md locks it |
| Avatar IV (default) | `POST /v3/videos` | Full-body motion, motion_prompt support | Default for new channels without a preference |
| Avatar V | `POST /v3/videos` with `engine.type: "avatar_v"` | Most natural motion | Only for eligible avatar looks (check `supported_api_engines`) |

### Rules

- **Never switch engines without user approval.** If CHANNEL.md says "Avatar III", use Avatar III via the v2 endpoint. Do not "upgrade" to IV because it's newer.
- **Avatar III requires the v2 endpoint.** It is NOT accessible via `/v3/videos`. You MUST use `/v2/video/generate` (Studio API) or `/v2/videos` (simplified). Read `/workspace/docs/providers/heygen-studio-api.md` for the v2 schema.
- **When in doubt, default to Avatar IV** via `/v3/videos`. Only use Avatar III when explicitly requested or locked.
- **When you pass a `motion_prompt` (Avatar IV) or any prompt text alongside a chosen `avatar_id`, do NOT re-describe the presenter's appearance in that text.** Refer to "the selected presenter" and describe only motion/scene. Re-describing the face/outfit fights the locked avatar and is a common cause of the rendered presenter not matching the chosen look. (Avatar V doesn't take `motion_prompt`; this applies to Avatar IV.)

---

## 4. Parameter defaults

| Parameter | Default | Notes |
|---|---|---|
| `talking_photo_style` | **OMIT entirely** | `"circle"` bakes a circular crop into the rendered MP4 — almost never desired. Omitting produces the cleanest full-frame output. Only set to `"circle"` if the user explicitly asks for it. |
| `dimension` | `{ "width": 1920, "height": 1080 }` | 16:9 landscape. Use `1080x1920` only for vertical/Shorts. |
| `voice.type` | `"audio"` with `audio_url` | Always use pre-generated audio (ElevenLabs/Minimax), not HeyGen's built-in TTS. External audio gives consistent voice across segments. |
| `background.type` | `"color"` with `"value": "#000000"` or channel-appropriate color | Solid color is safest for Remotion compositing. Use `"image"` or `"video"` only when the channel specifically calls for it. |

### Critical: talking_photo_style

The `talking_photo_style` parameter controls the visual framing of talking-photo avatars:
- **Omit entirely** (default) → full-frame, no crop, cleanest result
- `"square"` → square crop
- `"circle"` → circular mask baked into the MP4

Setting `"circle"` on a batch of 6 full-length segments destroys all of them — the circle is baked into every frame of the rendered video and cannot be removed in post. This has caused multiple production failures.

---

## 5. Mandatory sample gate

> **Before submitting a batch of N segments, ALWAYS:**
>
> 1. Take ~15 seconds of audio from the first segment
> 2. Render ONE test clip using the exact settings you plan to use for the full batch (same avatar, voice, style, dimension, background)
> 3. End your turn and wait to be notified when it's ready (a short clip takes 2-5 min)
> 4. Present the test clip to the user with `present` (kind "video"), using the file path from that notification
> 5. Ask explicitly: "Does this look and sound right? Correct avatar, voice, framing, and style?"
> 6. **Wait for user approval.** Do not proceed until the user confirms.
> 7. Only after approval, submit the remaining segments

### Why this matters

A test clip costs 1 HeyGen render (~2-5 min). A failed batch costs 6+ renders (~90+ min) plus wasted credits. The sample gate catches:
- Wrong avatar or talking_photo_id
- Wrong voice (female voice on male character)
- Unwanted circle crop from `talking_photo_style`
- Head-only animation when the user expects full-body
- Wrong background or dimension
- Account routing issues (platform vs user's own account)

Every one of these has happened in production when the sample gate was skipped.

---

## 6. Voice pairing

1. **Check CHANNEL.md** for locked `voice_id` under `## Locked Provider Assets → ElevenLabs`
2. If locked, use that voice. Do not substitute.
3. If not locked, **query available voices** via the ElevenLabs voices endpoint
4. Match to the channel's Voice DNA (from Skill 03 / CHANNEL.md voice description)
5. Present the top 2-3 matches to the user, let them pick
6. Lock the chosen voice_id to CHANNEL.md
7. **Never use a default/generic voice** without confirming with the user first

The sample gate (Section 5) also validates voice pairing — the user hears the voice on the test clip before batch submission.

---

## 7. Batch submission workflow

After the sample gate passes:

1. **Split the full voiceover** into segments of 2-3 minutes each (HeyGen handles longer audio but render time scales non-linearly — shorter segments render faster and can be parallelized)
2. **Host each audio chunk and pass it as `audio_url`** (preferred — works for any file size). Do **NOT** upload audio through the HeyGen proxy with `POST /v3/assets`: provider calls are proxied through a Function with a ~4.5 MB request-body limit, so any multi-MB audio chunk fails with `{"ok":false,"error":"Backend proxy unreachable: fetch failed"}`. Instead, upload straight to storage and hand HeyGen the URL (see "Hosting audio for `audio_url`" below).
3. **Submit all segments** with identical settings — same avatar/talking_photo, same style, same dimension, same background
4. **End your turn immediately.** The platform polls each submission, downloads completed videos to the sandbox, and notifies you when each is ready. **Do NOT call `poll_endpoint`** — it is handled for you.
5. On re-invocation: present each completed video to the user, then assemble in Remotion.

### Hosting audio for `audio_url`

Provider proxy calls go through a Function capped at ~4.5 MB of request body, so you cannot POST multi-MB audio to HeyGen directly. Upload the chunk straight to storage and pass the returned URL as `voice.audio_url`. **Do the upload and the submit in the SAME Python script** — that keeps the public URL as an in-process variable (its signed query string is full of `&`; the moment you push it through shell variables / `sed` / a heredoc it gets corrupted, and a variable from one Bash call is gone by the next).

```python
import os, requests

backend, sid = os.environ["ROOKCAST_BACKEND_URL"], os.environ["RUN_SESSION_ID"]

# a) Presigned upload URL (tiny request). b) PUT the file straight to storage
#    (bypasses the 4.5 MB Function cap). Content-Type must match what you asked for.
up = requests.post(f"{backend}/api/sandbox/upload-url?_sid={sid}",
                   json={"filename": "part1.mp3", "contentType": "audio/mpeg"}).json()
with open("part1.mp3", "rb") as f:
    requests.put(up["uploadUrl"], data=f, headers={"Content-Type": "audio/mpeg"})
audio_url = up["publicUrl"]  # stays intact as a variable — never string-substitute it

# c) Submit, in the same script, using audio_url directly in the body (see §2.5).
base, key = os.environ["HEYGEN_API_BASE_URL"], os.environ["HEYGEN_API_KEY"]
body = {"video_inputs": [{"character": {"type": "talking_photo", "talking_photo_id": LOOK_ID},
                          "voice": {"type": "audio", "audio_url": audio_url},
                          "background": {"type": "color", "value": "#000000"}}],
        "dimension": {"width": 1920, "height": 1080}}
r = requests.post(f"{base}/v2/video/generate", headers={"x-api-key": key}, json=body)
print(r.status_code); print(r.text)
```

Notes:
- The `publicUrl` is valid for 24h — ample for HeyGen to fetch it at submit time.
- This same upload-url endpoint works for any large asset a provider fetches by URL (avatar source images for fal, etc.), not just HeyGen audio.
- Only use `POST /v3/assets` for genuinely small files (< 4 MB) when you specifically need a HeyGen `asset_id` rather than a URL.

---

## 8. Cost estimate + confirmation

Before batch submission, tell the user:

> "I'll submit N segments (~X minutes of video total) to HeyGen. Each segment takes 15-25 minutes to render — total wait is approximately Y minutes since they render in parallel. This will use HeyGen credits from [your account / platform credits]. Ready to proceed?"

Wait for confirmation. This is not the sample gate (that already passed) — this is the final "go" before committing to the full batch.

---

## 9. Anti-patterns

These have all caused production failures:

- **Never set `talking_photo_style: "circle"` as a default.** It bakes a circular mask into the video. Omit the field.
- **Never submit a full batch without testing one clip first.** The sample gate is mandatory.
- **Never use a voice without checking available voices first.** Wrong voice = wrong gender, wrong accent, wrong character.
- **Never switch from Avatar III to IV** (or vice versa) without user approval. The engines produce fundamentally different output.
- **Never call `poll_endpoint` for HeyGen generations, and never announce completion yourself.** The platform auto-polls and notifies you when done.
- **Never invent avatar_id, voice_id, or talking_photo_id from memory.** Always discover via API list endpoints or read from CHANNEL.md locked assets.
- **Never pass a `/v3/avatars` group id as `avatar_id`.** That endpoint returns groups (note `looks_count`), not renderable avatars. Resolve to a look via `/v3/avatars/looks?group_id=...` and pass the look's `id`. A group id — or a photo avatar submitted as `type:"avatar"` instead of `type:"talking_photo"` — returns `avatar_not_found` / `avatar look not found ... space_id:` even though the id is in the account. **Do not read that error as an account/connection mismatch until you've confirmed the id is a look id used with the matching character type** — otherwise you'll wrongly tell the user to reconnect HeyGen.
- **Never assemble the request body with heredocs, `sed`, or string substitution, and never thread the audio URL through shell variables across separate Bash calls.** Build it in one Python script with real dicts/variables (see §2.5 / Section 7). A signed URL contains `&`; `sed`'s replacement treats `&` as the matched text and corrupts it, and a shell variable set in one Bash call is empty in the next. Both have shipped broken `audio_url`s that fail the render with `HTTP_DOWNLOAD_FAILED`.
- **Never re-upload assets that are already uploaded.** Check the session directory for existing audio chunks before uploading again.
- **Never POST multi-MB audio to `/v3/assets` through the proxy.** It fails with "Backend proxy unreachable: fetch failed" (4.5 MB Function cap). Use the `audio_url` upload flow in Section 7 instead.
- **Never use `/v1/video.generate`.** Only `/v3/videos`, `/v2/video/generate`, or `/v2/videos` create videos. v1 is a deprecated API with a different request/response schema — it returns HTTP 200 but with no usable `video_id`, silently producing nothing. **Validate every generation response actually contains a `video_id` before ending your turn** — a 200 with an empty/missing id means you're on the wrong endpoint; switch to v2/v3 and resubmit, don't assume success.
- **Never submit to HeyGen without confirming which account is active.** Check the Integration Status in the system prompt — platform vs OAuth determines whose credits are consumed.
