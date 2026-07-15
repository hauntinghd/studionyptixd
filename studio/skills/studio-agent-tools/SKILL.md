---
name: studio-agent-tools
description: Tyler-loop Studio Agent tool dictionary — use when planning or calling any Studio tool.
---

# Studio Agent Tools SOP (Tyler loop + live registry)

**Purpose:** Teach the agent how to **plan and call tools correctly** — not dump research forms.
Built from: (1) Tyler Keane closed-loop design (call transcript ~6:26+), (2) live `tool_schemas()` inventory.

**Machine twin:** `studio/STUDIO_AGENT_INVENTORY.json`  
**Regenerate inventory:** `python ops/extract_studio_agent_inventory.py`  
**Rebuild this SOP:** `python ops/build_tyler_tools_sop.py`

---

## 1. Closed loop (how Studio should think)

All of this happens in **one session** (continuity is mandatory).

```
USER MESSAGE
    │
    ▼
[1] CONVERSATION AGENT
    Talk like a human producer (Grok-class). Extract goal + preferences.
    Skill level adjusts how hard you lead vs rely on the user.
    │
    ▼
[2] COMPLETENESS GATE
    Do I have enough to act? (topic, niche, format short/long, style,
    product URL for ads, captions, voice, budget if relevant)
    Missing → ask clearly. Do NOT invent. Do NOT research blindly first.
    │
    ▼
[3] OPTIONAL PLAN (recommended for production / ads)
    20 seconds to confirm beats > 4–40 minutes redoing a render.
    User edits plan → update criteria.
    │
    ▼
[4] TASK-MAKER
    Read THIS SOP like a dictionary. Build a tool sequence (script of actions).
    │
    ▼
[5] CHECKER
    Will this sequence fulfill the user goal? If no → rewrite task plan.
    │
    ▼
[6] RUN TOOLS
    Execute; honor approval_required when approval_mode=confirm.
    │
    ▼
[7] JUDGE
    Did the result match the goal? If no → feedback to task-maker in SAME session.
    If yes → natural language answer + next step (not a research form).
```

### Listen first (cost + UX)

- **Do not** fire heavy Live Demand / production tools while the user is still explaining.
- After they finish, completeness gate → then research/tools.
- People hate feeling unheard more than a slightly slower perfect answer.

### Skill level (product dial)

| Level | Completeness / conversation | Tool aggressiveness |
|-------|----------------------------|---------------------|
| **Beginner** | Ask more; fill gaps; guide hard | Studio proposes tools + plans proactively |
| **Intermediate** | Balanced questions | Shared control |
| **Professional** | Assume expertise; fewer remedial questions | Execute when brief is clear |
| **Intermediate + Professional** | Balanced but lean on user | Balanced help; user drives key creative calls |

### Output voice (non-negotiable)

- Conversation-first. **No** research forms (`I verified public YouTube demand`, `score 0.49`, `Confirmed vs blocked`).
- **No** channel required for public niche research.
- Cite real titles/views only from tool results, in plain English.

---

## 2. Approval-required tools (confirm mode)

These spend money or mutate state. In `approval_mode=confirm`, surface for user approval before execute:

- `animate_production_scenes`
- `finalize_longform_render`
- `finalize_production`
- `ingest_cliplab_attachment`
- `remix_cliplab_short`
- `render_cliplab_segments`
- `run_build_script`
- `set_production_scenes_animate`
- `start_longform_render`
- `start_shortform_generate`
- `write_project_file`

---

## 3. Tool dictionary (use when / do not)

Task-maker: pick tools like parts in an instruction manual.

### Group: `knowledge_skills`

#### `list_skills`

List all Rookcast skill slugs imported into studio/skills/.

- **Use when:** User asks what craft playbooks exist, or before load_skill when unsure of slug.

#### `load_skill`

Load a Rookcast SKILL.md playbook by slug (e.g. script-writing, thumbnail-design).

- **Required params:** `slug`
- **Use when:** Need craft guidance (script, thumbnail, voice, captions, storyboard, etc.) before writing copy or planning beats.

### Group: `channel_docs_memory`

#### `load_channel_docs`

Load CHANNEL.md and/or FLOW.md for a Studio channel key.

- **Required params:** `channel_key`
- **Use when:** User selected a Studio channel OR packaging must match a known channel's CHANNEL.md/FLOW.md rules.

#### `list_studio_channels`

List long-form channel keys from long_form/prompts/channels.py registry.

- **Use when:** User asks which Studio channels exist, or to pick a channel key for load_channel_docs.

#### `get_perpetual_memory`

Read durable Studio Agent memory for this user and optionally a specific YouTube channel. Use before channel strategy, packaging, visual defaults, and production planning.

- **Use when:** Need prior preferences / past wins for this user or channel before planning.

#### `remember_channel_preference`

Persist a durable user/channel preference, rule, lesson, or strategy note. Use when the user says remember/always/never, when analytics reveals a lesson, or after production feedback changes the channel playbook.

- **Required params:** `note`
- **Use when:** User states a durable preference (style, voice, caption, niche) to keep across sessions.

### Group: `youtube_oauth_analytics`

#### `youtube_oauth_status`

Explain Studio YouTube OAuth scopes and how to connect channels in Settings.

- **Use when:** User asks if YouTube is connected, or before private analytics.

#### `list_youtube_channels`

List OAuth-connected YouTube channels with harvest/analytics status.

- **Use when:** User needs to pick/list linked YouTube channels (SELECT CHANNEL flow).

#### `get_channel_analytics`

Channel intelligence: Catalyst harvest + live YouTube Analytics (90d Reporting API: views, CTR, AVD, per-video retention rows when available, Shorts-specific latest-vs-winner comparison, top titles, series arcs) and latest upload velocity when OAuth is connected. If video_level_retention_available is false, do not infer which specific video had high AVD.

- **Use when:** User wants THEIR channel performance, retention, winners — requires connected channel. NOT for pure public niche research.
- **Do not:** Do not claim private retention without connection. Do not block pure public research on missing OAuth.

#### `refresh_channel_intelligence`

Re-sync a connected YouTube channel into Catalyst harvest (analytics, packaging/retention learnings). Run after new uploads or when recommendations feel stale.

- **Required params:** `channel_id`
- **Use when:** Channel analytics look stale or user asks to refresh connected-channel data.

### Group: `public_research_demand`

#### `search_youtube_public`

Search public YouTube for niche demand and reference candidates. Always call this for 14-30 day / fresh / current demand requests. Uses YouTube Data API publishedAfter via the days parameter (7-90) for recent-momentum uploads, plus a separate 365-day order=viewCount top-performer pass. Returns hydrated title/channel/views/likes/published_at/support_label. Does not return private analytics like AVD or retention.

- **Required params:** `query`
- **Use when:** Need a one-off public video search by query. Prefer get_public_search_trends for Live Demand (quota). Avoid dual public search same turn.
- **Do not:** Do not pair with get_public_search_trends same niche same turn (quota burn).

#### `get_public_search_trends`

Public YouTube search demand (last 30 days) + predicted topic scores. Use registry_key to bias queries to a channel niche. Returned videos include hydrated public stats when available; do not call something trending/high-volume unless support_label and hydrated stats justify it.

- **Use when:** PRIMARY Live Demand tool. User asks what people want / what's working / viral / niche demand. Channel NOT required. One call per turn.
- **Do not:** Do not invent views if youtube_quota_exhausted. Do not ship research forms to user.

#### `recommend_video_topics`

For creators who don't know what to film: merge channel analytics (if connected), growth playbook, and public search trends into ranked topic + niche recommendations. Does not imply Skeleton AI â€” recommend format-appropriate pipelines (short script, long-form, reference blueprint, or skeleton only if user wants that visual).

- **Use when:** User wants next-video topic ideas grounded in public demand and/or connected channel analytics.

### Group: `reference_competitor`

#### `analyze_reference_video`

Analyze a reference video from YouTube (yt-dlp) or from an uploaded Studio Agent attachment (local_path). Extracts metadata, scene keyframes, cut timeline pacing, and audio for transcription. Poll poll_render_job(kind=competitor), then build_scene_blueprint_from_reference.

- **Use when:** User uploaded or linked a reference video to study pacing, look, story, packaging.

#### `analyze_competitor_video`

Alias of analyze_reference_video (competitor/outlier study).

- **Required params:** `url`
- **Use when:** User wants competitor/reference channel video analysis (not their own upload path).

#### `retry_reference_analysis`

Re-run failed reference-analysis stages (transcript, vision, storytelling) on an existing competitor job_id without re-uploading. Use when transcript/vision/story failed but keyframes or pacing already exist. Poll poll_render_job(kind=competitor) is not required — this returns the refreshed analysis payload immediately.

- **Required params:** `job_id`
- **Use when:** Reference analysis failed or was pacing-only; user says try again / retry stages.

#### `build_scene_blueprint_from_reference`

After analyze_reference_video completes: map keyframes + pacing into per-scene rows (1â€“5 characters), Seedream v4.5 edit fields, i2v duration, BGM cues, audio mix.

- **Required params:** `job_id`, `topic`
- **Use when:** After solid reference analysis, user wants a scene/shot blueprint from that reference.

### Group: `shortform_production`

#### `list_skeleton_video_models`

List selectable i2v models for Skeleton AI shorts. Image stills are ALWAYS canonical Seedream 4.5 edit (not selectable). User picks video only.

- **Use when:** User asks i2v/motion model options, or before locking video_model on shortform.

#### `list_skeleton_categories`

List Skeleton AI script categories: 20 YouTube-aligned built-ins (outcast, people_blogs, gaming, â€¦) plus this user's custom categories. Call before start_shortform_generate when category is non-obvious.

- **Use when:** User needs Skeleton content-lane keys (human_limits, etc.) — not YouTube channel keys.

#### `create_skeleton_category`

Create a custom Skeleton AI category for this user (e.g. outcast, true crime lane, channel-specific tone). Returns the new category_key.

- **Required params:** `label`
- **Use when:** User wants a new custom Skeleton content lane for scripts.

#### `list_render_styles`

List Studio shortform render styles (cinematic, comic book, Ghibli, skeleton host, etc.). ALWAYS pass render_style to start_shortform_generate â€” default to the user's session picker unless they explicitly choose another. skeleton_host = Skeleton niche art style. Returns visual preview URLs for a gallery grid (like the reference style cards).

- **Use when:** User asks available art styles, or before start_shortform_generate when style not set in session.

#### `start_shortform_generate` **[APPROVAL]**

Queue a styled shortform render (9:16, ~12 beats). REQUIRED: render_style from list_render_styles or the user's session Art Style picker. category_key is a Skeleton content lane, not the selected YouTube channel key. For MrSkeleWelly psychology shorts, use human_limits. Default cinematic/photoreal for documentaries and real people â€” NOT skeleton unless render_style=skeleton_host. Comic/history/anime/etc. each have their own T2I look. Call list_skeleton_video_models for video_model; list_skeleton_categories for script tone. If the user asks for one still, one image, one scene, first still/image, visual proof, or to approve the look before a full short, pass visual_proof_only=true and scene_count=1. After starting (for non-skeleton styles), the job goes to a review gate where you can use the scene control tools (list_production_scenes, edit_production_scene_still with V4.5 edit, set_production_scenes_animate, animate_production_scenes, etc.) for full creative control.

- **Required params:** `category_key`, `topic`, `video_model`, `render_style`
- **Use when:** User explicitly wants a Short produced (after plan/completeness). Requires style, category_key, topic, video_model. Use visual_proof_only for one still first. APPROVAL.
- **Do not:** Do not start without topic + render_style + category_key + video_model. Do not use skeleton_host unless user wants skeleton. Do not skip plan/cost when spend is high.
- **video_model enum:** `ltx_budget`, `seedance`, `pixverse`, `kling_pro`, `kling21_standard`, `pixverse_v6`, `pixverse_c1`, `kling21_pro`, `veo3_fast`, `kling21_master`, `grok_imagine_video`, `grok_imagine_video_15`, `grok_imagine_video_15_1080p`
- **image_model_id enum:** `grok_imagine`, `grok_imagine_standard`, `imagen4_fast`, `imagen4_preview`, `imagen4_ultra`, `recraft_v4`, `seedream45`, `ernie_image`, `flux_2_pro`, `nano_banana_pro`, `recraft_v4_pro`, `flux_lora_skeleton`
- **caption_mode enum:** `word`, `off`

#### `list_production_scenes`

For a shortform production job (after start_shortform_generate), list all scenes with their current still, animate flag, duration, status, and preview info. Use this to inspect before editing or selectively animating. Essential for giving users full creative control over exactly which scenes get motion and iterating with V4.5 edits.

- **Required params:** `job_id`
- **Use when:** Inspect scene stills, animate flags, durations after shortform job started.

#### `edit_production_scene_still`

Use Seedream V4.5 *edit* (image-to-image edit) to modify ONE specific scene's still with natural language. Example: 'make the background a rainy cyberpunk alley at night, add neon reflections on the wet ground'. This is the primary way to get pixel-perfect creative control and iterate a scene until it is exactly right before deciding to animate it. Use scope='character' to change only the subject/mannequin/skeleton, scope='background' to preserve the subject and change only the world, or scope='props' for held items/screens/objects. The previous clip (if any) is invalidated so you can re-animate after the edit.

- **Required params:** `job_id`, `scene_index`, `instruction`
- **Use when:** User wants one scene still fixed (prompt edit / V4.5) without full regenerate.

#### `edit_production_scenes_still`

Use Seedream V4.5 edit to apply the same visual change to MULTIPLE shortform scene stills. Use this when the user says every scene/all scenes or gives a global wardrobe/character rule. Example: 'put the skeleton in a proper doctor's uniform, black pants, white T-shirt, white tux coat'. This edits stills only and does not animate/finalize; the user must review the updated stills first.

- **Required params:** `job_id`, `instruction`
- **Use when:** User wants the same still edit applied to multiple scenes.

#### `regenerate_production_scene_still`

Catalyst-audited scene regenerate. Preserves exact channel style while fixing artifacting (extra hands, split-screen diptychs). Prefer this when the user clicks Regenerate or reports limb/layout artifacts.

- **Required params:** `job_id`, `scene_index`
- **Use when:** User wants a full still regen for a scene (not a small edit).

#### `set_production_scenes_animate` **[APPROVAL]**

Precisely control animation per scene for a shortform job. Set animate=true/false on specific scene indices (or all). This is how you achieve 'animate exactly 20 minutes out of a 30-minute piece' or 'only animate these three hero scenes'. Non-animated scenes will use a tasteful Ken Burns push in the final compose.

- **Required params:** `job_id`, `animate`
- **Use when:** User chooses which scenes get motion before animate. APPROVAL.

#### `set_production_scene_duration`

Override the duration (in seconds) for one or more specific scenes. Useful for pacing control â€” shorter for punchy beats, longer for emotional moments.

- **Required params:** `job_id`, `scene_index`, `duration_sec`
- **Use when:** User changes seconds for a specific scene.

#### `animate_production_scenes` **[APPROVAL]**

Run i2v animation (using the job's video_model) on specific scenes only, or all scenes currently marked animate=true. Call this after editing stills with edit_production_scene_still until they are perfect. You can iterate: edit still -> animate only that scene -> review -> edit again -> re-animate only that one. For visual_proof_only jobs, pass scene_indices=[0] and animate exactly that approved proof scene.

- **Required params:** `job_id`
- **Use when:** User approves animating selected scenes (i2v). APPROVAL.

#### `finalize_production` **[APPROVAL]**

After the stills are perfect and you have set exactly which scenes should be animated (and their durations), call this to generate any missing motion, do the final VO, captions, mixing, and produce the deliverable MP4. Supports mixed animated + Ken-Burns scenes in one video for perfect pacing control.

- **Required params:** `job_id`
- **Use when:** User wants final stitched Short MP4 (export). APPROVAL.
- **Do not:** Do not finalize if user only asked for stills/review. Respect captions off.

#### `re_edit_production`

THE PREFERRED TOOL for reply-to re-edit requests ('re-edit this video', 'fix the pacing/story/CTA/packaging on the one you just made', 'make the editing proper on the short you showed me', etc.). Takes the *exact same prior production* (job_id + its existing stills/clips/scenes.json/video the user already saw), records the re-edit instruction, and re-finalizes a new version with improved editing, pacing, storytelling, instruction-matched captions (captions off when requested; otherwise word-level captions by default), visual-narration lockstep, and a clear subscribe CTA at the end â€” *without* throwing away the video and regenerating everything from scratch. The LLM should usually call list_production_scenes (or list_longform_scenes) + any needed targeted edit_production_scene_still / set_*_duration first, then call this. Only creates a full new generation if the user explicitly asks to 'start over' or 'change the entire visual style'.

- **Required params:** `job_id`, `instruction`
- **Use when:** User wants to re-enter edit after partial production without full restart.

#### `record_production_feedback`

Log what worked or failed on a published video for NYPTID model improvement. Internal training signal only â€” never sold to advertisers.

- **Required params:** `channel_id`, `outcome`
- **Use when:** User gives quality feedback to store for learning after a job.

#### `poll_render_job`

Poll job status by job_id and kind. Use kind='competitor' for analyze_competitor_video to surface live progress stages.

- **Required params:** `job_id`, `kind`
- **Use when:** Need status/progress of shortform or longform job (running / awaiting review / complete).

### Group: `longform_production`

#### `start_longform_render` **[APPROVAL]**

Queue a long-form render via the Studio pipeline. Requires channel_key + outline JSON. Spends fal credits.

- **Required params:** `channel_key`, `title`, `topic`
- **Use when:** User wants long-form documentary-style render. APPROVAL.

#### `generate_longform_thumbnails`

Generate or reprompt 1-3 thumbnails for a longform job (user chooses for A/B test). feedback for reprompt (e.g. 'more dramatic lighting, teal/orange grade, teaser not spoiler, match the video tone exactly'). Uses Seedream edit for cheap iterations. Pulls from channel style. After user approves, download the package.txt (title/tags/desc + exact timestamps).

- **Required params:** `job_id`
- **Use when:** Longform package needs thumbnail options.

#### `finalize_longform_render` **[APPROVAL]**

After stills gate (phase awaiting_approval): run voice, SFX, thumbnails, and MP4 composite. Returns job_id to poll via Studio production monitor.

- **Required params:** `job_id`
- **Use when:** User approves finishing longform export. APPROVAL.

### Group: `product_ads`

#### `ingest_product_reference`

Create a durable product-reference manifest for a software or physical-product advertisement. Uses images attached in the current chat and/or safely crawls a public product website for dedicated product images. If website_url is omitted, use the product website saved on the user's Studio profile. Call before start_shortform_generate for product ads.

- **Use when:** Product ad / promo: crawl product URL (or profile website) BEFORE shortform so product is visually locked.

### Group: `cliplab`

#### `ingest_cliplab_attachment` **[APPROVAL]**

Internal/admin ClipLab: ingest the latest uploaded video attachment from this Studio Agent chat as a long-form source, transcribe it, and create a ClipLab video_id. Use this first when the user uploads a long recording and asks Studio Agent to find/produce clips. This does not use Studio short-form render styles because it cuts existing footage.

- **Use when:** User wants ClipLab ingest of an attached video. APPROVAL.

#### `analyze_cliplab_video`

ClipLab: analyze an already-ingested long video/short by ClipLab video_id and return ranked 9:16 clip candidates. Use after the user uploads/pulls a source in ClipLab or gives a ClipLab video_id. Logs candidates into Catalyst training data. Do not apply generated short-form style presets; this is clip selection from existing footage.

- **Required params:** `video_id`, `prompt`
- **Use when:** ClipLab: analyze ingested video for segments/cuts.

#### `render_cliplab_segments` **[APPROVAL]**

ClipLab: render selected analyzed segments into 9:16 clips with face-track reframe and captions. Requires an analyze_cliplab_video job_id and selected segment indices. Does not use generated-scene short-form styles or image-to-video styles.

- **Required params:** `video_id`, `analyze_job_id`, `segment_indices`
- **Use when:** ClipLab: render chosen segments. APPROVAL.

#### `remix_cliplab_short` **[APPROVAL]**

ClipLab Remix Lab: polish an already-cut 9:16 short with blurred background, captions, color, and pacing treatment. Use when the user uploads an Opus-style clip and wants Studio to make it feel native/viral.

- **Required params:** `video_id`
- **Use when:** ClipLab: remix into a short. APPROVAL.

#### `poll_cliplab_job`

Poll a ClipLab ingest/analyze/render/remix job and return persisted segments, clips, errors, or remix output.

- **Required params:** `job_id`
- **Use when:** ClipLab job status polling.

### Group: `archival_media_audio`

#### `fetch_archival_for_video`

Get archival B-roll matched to THIS exact video: per-scene queries from topic + scene blueprint, fan-out Internet Archive (Prelinger/stock), LOC film, NASA video, Wikimedia, NPS, FBI. Resolves direct MP4/download URLs. Call after build_scene_blueprint_from_reference or with topic + registry_key. Use BEFORE fal generation â€” Lume/Magnates docs are ~90% archival stills+B-roll.

- **Required params:** `topic`
- **Use when:** Need archival B-roll/assets for a planned video subject.

#### `resolve_archival_asset`

Resolve direct download URLs for one archival search hit (pass the asset object from fetch_archival_for_video or search_archival_media).

- **Required params:** `source`
- **Use when:** Lock a specific archival asset ID for use.

#### `search_archival_media`

Quick single-query archival search. For a full video shot list use fetch_archival_for_video instead (per-scene, direct downloads).

- **Required params:** `query`
- **Use when:** Search archival library by keywords.

#### `search_music`

Search free Creative Commons music (Jamendo) for background tracks. Returns direct audio download URLs.

- **Required params:** `query`
- **Use when:** User wants bed music options for a piece.

#### `search_sfx`

Search free sound effects (Freesound, CC0 by default for attribution-free commercial use).

- **Required params:** `query`
- **Use when:** User wants SFX/ambience options.

### Group: `billing_pricing`

#### `get_studio_credits`

Unified credit wallet balance, plan, recent ledger. Use before expensive renders; tell user to top up in Studio Wallet when low.

- **Use when:** User asks balance/credits remaining.

#### `get_fal_pricing`

Fetch live fal.ai Platform API pricing for image/i2v/TTS endpoints. Supplemental only — prefer estimate_shortform_render_cost for user-facing short quotes.

- **Use when:** Need live FAL unit prices for cost talk.

#### `estimate_shortform_render_cost`

Grounded USD estimate for a shortform render using the user's active session image_model_id and video_model. REQUIRED before quoting per-short production cost — never invent LTX/Seedream pricing from memory.

- **Use when:** BEFORE expensive shortform: cost preflight so user can approve spend.

### Group: `project_files_build`

#### `read_project_file`

Read a text file under the repo root (paths must stay inside workspace).

- **Required params:** `relative_path`
- **Use when:** Need to read a project file for longform/build context.

#### `write_project_file` **[APPROVAL]**

Write or overwrite a text file under studio/ or long_form/ (approval in confirm mode).

- **Required params:** `relative_path`, `content`
- **Use when:** Need to write project file. APPROVAL. Rare for normal chat production.
- **Do not:** Do not use for normal creator chat unless building project files.

#### `run_build_script` **[APPROVAL]**

Run an allowlisted long_form build script (approval in confirm mode). Example: long_form/build_cryptic_ctr_ss_rook.py --preview

- **Required params:** `script`
- **Use when:** Allowed build script execution. APPROVAL. Advanced/dev.
- **Do not:** Never invent scripts outside allowlist.

---

## 4. Recipe scripts (common jobs)

### A) Public niche / Live Demand (no channel needed)

1. Completeness: niche label (day trading, fitness, …) — if missing, ask. Discovery-only is weak.
2. `get_public_search_trends` once (cache-first unless user said right now / live).
3. Judge: if quota exhausted → say so; do not invent stats.
4. Reply in conversation; offer: script a short / tighten niche / connect channel for personal winners.

### B) Demand → plan → short (organic)

1. Conversation + completeness (topic, style, duration, captions).
2. Optional `get_public_search_trends` if demand not already known this session.
3. Optional plan confirm (beats / hook).
4. `estimate_shortform_render_cost` if spend matters.
5. `start_shortform_generate` (approval) — consider `visual_proof_only=true` first.
6. `poll_render_job` / `list_production_scenes` → edit tools as needed.
7. `set_production_scenes_animate` → `animate_production_scenes` → `finalize_production`.

### C) Product ad short

1. Completeness: product URL or profile website, offer, CTA, length, style.
2. `ingest_product_reference` **before** generate.
3. Optional niche demand via `get_public_search_trends` for hooks.
4. Plan (hook → problem → product → proof → CTA).
5. `start_shortform_generate` with `product_reference_id` (+ approval path as B).

### D) Reference-led short

1. `analyze_reference_video` (or competitor path).
2. `retry_reference_analysis` if incomplete.
3. Optional `build_scene_blueprint_from_reference`.
4. Optional public demand for niche.
5. Production path as B.

### E) Connected-channel strategy

1. `youtube_oauth_status` / `list_youtube_channels` if needed.
2. `get_channel_analytics` (+ `refresh_channel_intelligence` if stale).
3. Optional `recommend_video_topics` / `get_public_search_trends`.
4. Conversational strategy answer; production only if asked.

### F) ClipLab

1. `ingest_cliplab_attachment` → `analyze_cliplab_video` → render/remix tools → `poll_cliplab_job`.

### G) Longform

1. Completeness: subject, channel packaging, duration goals.
2. `start_longform_render` → poll → thumbs → `finalize_longform_render`.

---

## 5. Render styles (session Art Style picker)

| Key | Label | Group |
|-----|-------|-------|
| `cinematic` | Cinematic | Realism |
| `ultra_realism` | Ultra realism | Realism |
| `historical_18th_century` | 18th century historical | Realism |
| `comic_realism` | Comic realism | Comic |
| `comic_book` | Comic book (color) | Comic |
| `bw_comic` | B&W comic | Comic |
| `dark_comic` | Dark comic | Comic |
| `dark_cartoon` | Dark cartoon | Animation |
| `adult_cartoon` | Adult cartoon | Animation |
| `cute_anime` | Cute anime | Animation |
| `studio_ghibli` | Studio Ghibli | Animation |
| `pixar` | Pixar | Animation |
| `claymation` | Claymation | Animation |
| `disney_90s` | 90s Disney | Animation |
| `simpsons` | Simpsons | Animation |
| `creepy_cartoon_v1` | Creepy cartoon v1 | Animation |
| `creepy_cartoon_v2` | Creepy cartoon v2 | Animation |
| `illustrated_book` | Illustrated book | Specialty |
| `whiteboard` | Whiteboard | Specialty |
| `lego` | LEGO | Specialty |
| `minecraft` | Minecraft | Specialty |
| `low_poly` | Low poly | Specialty |
| `hand_drawn_2d` | 2D hand-drawn | Specialty |
| `skeleton_host` | Skeleton (Anatomical) | Niche |

**Use when:** user picked style in UI → pass same `render_style` to `start_shortform_generate`.
**skeleton_host:** only if user wants skeleton / anatomical host look — not default for all Shorts.

---

## 6. Skills (load when crafting)

- `ai-host-setup` — use via `load_skill` when that craft is active
- `audio-mix-assembly` — use via `load_skill` when that craft is active
- `authentic-setting` — use via `load_skill` when that craft is active
- `broll-selection` — use via `load_skill` when that craft is active
- `captions` — use via `load_skill` when that craft is active
- `channel-onboarding` — use via `load_skill` when that craft is active
- `character-continuity` — use via `load_skill` when that craft is active
- `compliance-preflight` — use via `load_skill` when that craft is active
- `composite-character` — use via `load_skill` when that craft is active
- `description-writing` — use via `load_skill` when that craft is active
- `gimp-image-editing` — use via `load_skill` when that craft is active
- `heygen-avatar-video` — use via `load_skill` when that craft is active
- `heygen-translate` — use via `load_skill` when that craft is active
- `image-generation` — use via `load_skill` when that craft is active
- `image-to-video` — use via `load_skill` when that craft is active
- `import-existing-channel` — use via `load_skill` when that craft is active
- `outlier-mining` — use via `load_skill` when that craft is active
- `performance-analysis` — use via `load_skill` when that craft is active
- `photoreal-vs-cinematic` — use via `load_skill` when that craft is active
- `reference-channel` — use via `load_skill` when that craft is active
- `script-writing` — use via `load_skill` when that craft is active
- `skill-customization` — use via `load_skill` when that craft is active
- `storyboard` — use via `load_skill` when that craft is active
- `thumbnail-design` — use via `load_skill` when that craft is active
- `title-creation` — use via `load_skill` when that craft is active
- `voice-tts` — use via `load_skill` when that craft is active

---

## 7. Session controls

- **approval_mode:** `['confirm', 'auto']`
- **reasoning_depth:** `['fast', 'balanced', 'deep']`
- **content_format:** `['short', 'long', 'auto']`
- **caption_mode:** `['word', 'off']`
- **captions_enabled:** `[True, False]`
- **art_style:** `session render_style picker; must match list_render_styles keys`
- **image_model:** `session image model picker; see start_shortform_generate.image_model_id enum`
- **video_model_i2v:** `session I2V picker; see start_shortform_generate.video_model enum`
- **channel_select:** `SELECT CHANNEL UI + list_youtube_channels / list_studio_channels`
- **product_website:** `profile product URL for ads / ingest_product_reference`

---

## 8. Judge checklist (after tools run)

- [ ] Did tools address the **user's stated goal** (not a side quest)?
- [ ] Any `error` / quota / approval still pending?
- [ ] Reply is natural conversation (no form dump, no score lines)?
- [ ] Clear next step (ask / plan / produce / wait for approval)?
- [ ] Session memory updated for niche / product / style if established?

---

## 9. Catalog notes

- **Inventory tool count:** 53
- **Public research ≠ channel OAuth.**
- **Quota survival:** one public demand tool per turn; cache-first unless live/right-now.
- **Composite tools OK** (e.g. recommend topics wrapping research) — document behavior in use-when.

_End of SOP. Task-maker: treat this file as the instruction manual for tools._
