---
name: import-existing-channel
description: >-
  Set up a fresh Rookcast channel from an existing YouTube channel the user already runs. Load when the user's first message asks to set up a video flow for an existing channel and includes a YouTube URL (e.g. "I want to set up a video generation flow for my youtube channel at https://youtube.com/@..."). Renames the project to match the real channel, analyzes recent uploads to infer niche / format / tone, then asks ONE batched question for residual gaps (existing avatar/voice uploads, budget mode)
---

# Import existing channel

The user pasted a YouTube channel URL on the new-channel selector. The first message they ever send to this session is a templated string of the form:

```
I want to set up a video generation flow for my youtube channel at <URL>
```

Don't ask them what kind of channel it is. They told you — analyze the channel and infer.

## When to load

- The user's first message contains a YouTube channel URL (`youtube.com/@…`, `youtube.com/channel/UC…`, or the bare `@handle` form). The default new-channel onboarding doesn't fire here — this skill takes its place.
- User explicitly asks to "re-import" or "re-analyze" their channel later (treat the same way).

Don't load on shaped channels (CHANNEL.md already filled in) — the production protocol takes over.

## Do AS MUCH WORK AS POSSIBLE before asking anything

The whole point of this flow is that we already know what the channel is. The user pays for that with their attention — every question we ask after pasting the URL is a tax on the experience. Synthesize first, ask second.

## Step 1 — Resolve the channel

Pull the URL out of the user's message and use the `youtube_channel_stats` tool to resolve the channel. If you need to search for the channel by name or URL, use `youtube_search` with `type: "channel"`.

If the tool returns an error or the channel can't be found:

- Tell the user the URL didn't resolve and ask for a corrected link. Common cause: legacy `/c/CustomName` URLs aren't directly resolvable — ask them for the `@handle` form (visible at the top of their channel page) instead.
- Don't burn a `propose_channel` round on a bad URL. Just one short message and stop until they reply.

On success you have: channel id, title, description, subscriberCount, videoCount, totalViewCount.

## Step 2 — Pull recent uploads + sample transcripts

Get a representative slice of the channel's catalog. Use `youtube_list_videos` with `max_results: 25` to pull recent uploads.

Sort by view count and pick the **top 3** — those are the channel's actual identity, not the latest experiment. For each, use `youtube_video_details` to get full metadata (title, description, tags, duration, view/like/comment counts).

To read tone + structure, fetch transcripts for each top video via Bash using the Supadata transcript API:

```bash
curl -s "${SUPADATA_API_BASE_URL}/youtube/transcript?videoId=<videoId>&text=true" \
  -H "x-api-key: $SUPADATA_API_KEY"
```

Skip any video whose duration is under 60s (shorts) or where the transcript fetch fails — don't retry, just work with what you have.

## Step 3 — Infer the channel profile

Read titles, descriptions, durations, tags, and the 3 transcripts together. Lock in:

- **Niche & topic** — what's this channel actually about? Be specific. "Math" is wrong; "famous unsolved math problems explained for general audiences" is right.
- **Audience level** — `general` / `informed` / `expert`. Inferred from vocabulary density in transcripts and average view-count-vs-subscriber ratio. Wide-appeal = general; deep-vertical = expert.
- **Visual format** — `avatar` (talking-head with on-camera person), `voiceover_broll` (no host on camera, narration over stock/B-roll), `animated_stories` (motion graphics or cartoon), `compilation` (clip-stitch), or `other`. Inferred from thumbnail style hints in `thumbnailUrl` patterns + transcript shape (interview cadence vs. narration vs. character voices).
- **Length target** — derived from the median upload duration. Bucket as `<60s shorts`, `2–5 min`, `5–12 min mid-form`, `10–25 min long-form`, or `>25 min long-form`.
- **Tone & register** — adjective list (e.g. "warm + curious", "deadpan + dry", "high-energy + meme-heavy"). Pull this from transcript sample — actual word choice beats anything inferred from titles.
- **Recurring thumbnail elements** — read 3-5 thumbnail URLs from the channel-uploads response. If there's a recurring face, color palette, font, or framing, note it. If not, say "no recurring elements detected".
- **Topic patterns** — recurring hooks, formats, or series the channel returns to. Two or three bullets.

When the inference for a slot is genuinely ambiguous (e.g. format is borderline between voiceover_broll and animated_stories), say so — don't pretend to know. Phrase the inferred slot as a best-guess in the proposal so the user can correct it.

## Step 4 — One batched `ask_user` for residual gaps

Everything below is information you can't infer from the channel alone. Ask in ONE batched call so the user resolves all three at once:

```
ask_user({
  questions: [
    {
      id: "existing_assets",
      prompt: "Do you have an avatar image or voice sample you'd like Rook to use for this channel? You can attach files in the chat composer (paperclip icon). Skip and we'll generate a host that matches your channel's vibe.",
      type: "text",
      optional: true
    },
    {
      id: "budget",
      prompt: "How aggressive do you want Rook to be on per-video spend?",
      type: "single_choice",
      options: [
        { id: "tight", label: "Tight", description: "Cheaper providers, fewer regenerations. Best for high-cadence channels." },
        { id: "balanced", label: "Balanced", description: "Mix of mid-tier providers and selective high-tier where it matters.", recommended: true },
        { id: "premium", label: "Premium", description: "High-tier providers everywhere, more regen budget per video." }
      ]
    },
    {
      id: "inference_corrections",
      prompt: "Anything wrong about how Rook read your channel? (Niche, audience, format, tone — say in your own words.) Skip if the read looks right.",
      type: "text",
      optional: true
    }
  ]
})
```

If the user attaches a file in their reply, treat it as an avatar (image) or voice sample (audio) and stage it under `/workspace/assets/` — the upload route already persists it; you just reference its `dbId` later in `propose_channel.host.avatarPreviewUrl` / `voicePreviewUrl`. If they skip the upload, fold an inferred host description into `propose_channel.host` (only for `avatar`-format channels) — Rook will generate samples on first run.

## Step 5 — Lock in via `propose_channel`

Draft both files upfront and pass them inline. Do **not** call Write — `propose_channel` writes them itself on confirm.

CHANNEL.md template (fill from the inference + user answers):

```markdown
# <Channel Name>

<one-line tagline>

## Identity
- **Niche:** <niche/topic>
- **Audience:** <general | informed | expert> — <one-line reason>
- **Tone:** <adjective list>
- **Source channel:** <YouTube URL>

## Format
- **Type:** <avatar | voiceover_broll | animated_stories | compilation | other>
- **Length target:** <bucket>
- **Recurring thumbnail elements:** <list or "none detected">

## Topic patterns
- <pattern 1>
- <pattern 2>
- <pattern 3>

## Reference channel notes
<2-4 bullets summarizing what the source channel does well — for the production agent to reference when scripting / shooting.>
```

FLOW.md should describe the per-video production recipe inferred from the channel. Reuse the structure from `channel-onboarding` Phase F's FLOW.md template (Read it via Read tool if you need to copy the schema). Tune values to match the inferred channel:

- `lengthTarget` from Step 3
- `thumbnailStyle` from Step 3 + Step 4 corrections
- Provider defaults from `budget` answer (tight → cheaper providers; balanced → mid; premium → high-tier)
- `host` block populated only for `avatar` format

Pass `host` to `propose_channel` only for `avatar` channels. Pass `notes` as a list of any inference-correction bullets the user added in `inference_corrections`.

## What this skill does NOT do

- **Voice cloning from the source channel.** We can't legally clone the real host's voice without consent + sample they upload. If the user attached a voice sample in Step 4, route it; otherwise the host gets a generated voice matched to the channel's vibe.
- **Avatar cloning from the source channel's actual host.** Same legal/UX rule — generated likeness only, unless the user uploads source imagery in Step 4 to ride on as reference.
- **Auto-publishing** the user's videos back to the source channel. Publishing requires the user to connect YouTube via OAuth from settings — this skill stops at "channel ready to produce". Mention this in your final message after `propose_channel` confirm.
