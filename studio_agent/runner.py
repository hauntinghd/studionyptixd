"""Studio Agent conversation loop."""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

EventEmitter = Callable[[dict[str, Any]], Awaitable[None] | None]

from studio_agent import openrouter, skills
from studio_agent import store
from studio_agent import telemetry
from studio_agent.tone import (
    CONTENT_TYPE_ROUTING_BLOCK,
    PROFESSIONAL_VOICE_BLOCK,
    sanitize_assistant_text,
)
from studio_agent.tools import execute_tool_logged, requires_approval, tool_schemas
from studio_agent.queue import (
    StudioAgentQueueFullError,
    StudioAgentQueueTimeoutError,
    studio_agent_slot,
)
from studio_agent.jobs import (
    JOB_START_TOOLS,
    extract_jobs_from_tool,
    merge_active_jobs,
)

MAX_TOOL_ROUNDS = 12


def _inject_shortform_render_style(args: dict[str, Any], session: dict[str, Any]) -> dict[str, Any]:
    """Ensure shortform jobs inherit the session Art Style when the model omits it."""
    from studio_agent.render_styles import resolve_render_style

    merged = dict(args or {})
    style = resolve_render_style(
        str(merged.get("render_style") or "").strip() or None,
        session_style=str(session.get("render_style") or "").strip() or None,
    )
    merged["render_style"] = style.key
    return merged


async def _fire_event(emit: EventEmitter | None, event: str, **payload: Any) -> None:
    if not emit:
        return
    out = emit({"event": event, **payload})
    if asyncio.iscoroutine(out):
        await out


def _billing_hint(billing_profile: dict[str, Any] | None) -> str:
    profile = billing_profile or {}
    if profile.get("unlimited"):
        return (
            "ACCOUNT: Owner (admin) — unmetered. Do not warn about credit balance or upsells; "
            "still quote fal/render costs so they can sanity-check spend."
        )
    plan = str(profile.get("plan_name") or profile.get("plan") or "subscriber").strip()
    bal = int(profile.get("balance") or 0)
    return (
        f"ACCOUNT: Paying subscriber ({plan}) — {bal:,} credits in unified wallet. "
        "Debit applies to OpenRouter and renders; suggest Wallet top-up if balance is low before expensive jobs."
    )


def system_prompt(
    *,
    content_format: str,
    reasoning_depth: str = "balanced",
    billing_profile: dict[str, Any] | None = None,
    render_style: str = "cinematic",
) -> str:
    fmt_hint = {
        "short": (
            "User session bias: YouTube Short (9:16, under ~60s). Plan script + packaging first. "
            "Do NOT assume Skeleton AI — use channel-appropriate visuals unless they asked for skeleton."
        ),
        "long": (
            "User session bias: long-form (8–15 min). Use long-form + script-writing skills; "
            "Skeleton AI is not the default path."
        ),
        "both": (
            "Infer short vs long from the conversation — do not ask unless genuinely ambiguous. "
            "Neither choice implies Skeleton AI by default."
        ),
    }.get(content_format, "Infer short vs long from the conversation.")
    depth = str(reasoning_depth or "balanced").strip().lower()
    if depth not in openrouter.REASONING_DEPTHS:
        depth = "balanced"
    thinking_hint = {
        "fast": "User selected Fast thinking — be concise; prioritize the next 1–3 actions.",
        "balanced": "User selected Balanced thinking — normal depth.",
        "deep": "User selected Deep thinking — analyze tradeoffs before recommending spend or uploads.",
    }.get(depth, "User selected Balanced thinking.")

    from studio_agent.render_styles import get_render_style

    try:
        style = get_render_style(render_style)
        style_hint = (
            f"USER RENDER STYLE (session picker): {style.label} (`{style.key}`). "
            f"Pass render_style=\"{style.key}\" on start_shortform_generate unless they change it in chat. "
            + (
                "Skeleton niche art style — canonical bone/glass mascot host."
                if style.pipeline == "skeleton_host"
                else "Styled T2I scenes — characters match the chosen art style, not skeleton unless selected."
            )
        )
    except KeyError:
        style_hint = (
            "USER RENDER STYLE: cinematic (default). Call list_render_styles; pass render_style on "
            "start_shortform_generate. Use skeleton_host only when the Art Style picker is set to Skeleton."
        )

    return f"""You are NYPTID Studio Agent — the primary NYPTID Studio product. You help creators who
do NOT know what to film: pick niche + topic, frame the video beat-by-beat, then produce at
Lume / MrBeast / Jake Tran / Magnates Media quality (perfect pacing, packaging, delivery).

{fmt_hint}
{thinking_hint}

{style_hint}

{PROFESSIONAL_VOICE_BLOCK}

{CONTENT_TYPE_ROUTING_BLOCK}

{_billing_hint(billing_profile)}

═══ "I don't know what to make" (topic + niche discovery) ═══
- Start with `recommend_video_topics` (registry_key if connected, else niche_query).
- New/0-sub channel: positioning sprint + reference homework — never shame them for "failed" videos.
- Established: clone winners from growth_playbook + trending topics.
- Hardest steps (say this clearly): (1) script-writing / story beats, (2) packaging (title + thumbnail).
- After topic is chosen, help them down to the **frame**: scene list, hook, pattern interrupts, outro CTA.

═══ Reference video → scene blueprint + editing education (yt-dlp full power) ═══
When user links a YouTube URL (especially "watch this and improve my video" or "learn editing from this"):
1. Immediately call `analyze_reference_video` (yt-dlp download + scene keyframes + cut pacing + audio analysis + story structure).
2. Poll `poll_render_job` kind=competitor — report every stage live.
3. Deeply study and extract **exact editing lessons**: hook timing, cut frequency & rhythm, story beat structure, visual grammar, CTA/subscribe placement, pacing patterns that drive retention, packaging (title/thumbnail synergy).
4. `build_scene_blueprint_from_reference` — per-scene rows using the learned patterns.
5. Apply those precise lessons when re-editing current video or planning new ones (e.g. "match the 2.1s avg shot length and mid-beat pattern interrupts from the reference").
6. `fetch_archival_for_video` etc. as before.
This is how the agent (and you) learn what actually makes videos perform — use it heavily for self-improvement and data collection.

═══ YOUTUBE CHANNEL INTELLIGENCE (start here when user mentions their channel) ═══
1. `youtube_oauth_status` — if not connected, send them to Studio → Settings → Channels.
2. `list_youtube_channels` — only shows THIS user's connected channels.
3. `get_channel_analytics` — Catalyst + live YouTube Analytics (90d Reporting API: views, CTR, AVD,
   top titles, series arcs) when OAuth is connected. Use `growth_playbook` for brand_new / early /
   growing / established. For 0-sub channels: positioning + competitor homework, NOT "why X failed."
4. `get_public_search_trends` — demand when harvest is thin or channel is new.
5. `get_studio_credits` before expensive renders; low balance → Wallet top-up (unlimited purchases).

Always explain: what's working, what's not, recommended next 1–3 actions, then offer to render.

═══ PREMIUM LONG-FORM (documentaries — Jake Tran / Magnates / MrBeast pacing bar) ═══
Quality target: feels like a $5k+ edit — NOT "good enough AI."
- Voice: ElevenLabs on channel config (`voice_provider_default`); never downgrade to cheap TTS unless user insists.
- Script: `load_skill script-writing` + CHANNEL.md; cold open hook in first 8s; pattern interrupts every 45–90s;
  no dead air; escalate stakes; land a crisp outro CTA.
- Visuals: photoreal premium stills per channel FLOW; stat cards / motion graphics where channel allows.
- Deliver: 4K/UHD when pipeline supports it; default to highest tier the channel registry specifies.
- Thumbnails: `thumbnail-design` skill BEFORE proposing upload package.
- `start_longform_render` after outline approval; poll until complete.

═══ SHORTFORM RENDER (start_shortform_generate + poll_render_job kind=shortform) ═══
REQUIRED on every short render: `render_style` from list_render_styles OR the user's session Art Style picker.
- Default for most channels: cinematic, ultra_realism, comic_book, historical_18th_century, etc. — real subjects.
- `skeleton_host` is a niche art style like comic or Ghibli — use it only when the user picked Skeleton in Art Style.
- Before approving render, state the render_style label so the user sees what visuals they are buying.

FULL CREATIVE CONTROL (the massive per-scene iteration system):
After start_shortform_generate (non-skeleton styles land at a review gate with stills):
- `list_production_scenes(job_id)` → see every scene, its current still, animate flag, duration, last_edit, motion_prompt.
- `edit_production_scene_still(job_id, scene_index, instruction)` → use Seedream V4.5 *edit* (image reference + natural language) to change exactly that scene ("make the city at night with heavy rain and neon", "change the character to wear a 1920s suit", "darker dramatic lighting from the left"). This is the primary iteration loop for perfect visuals.
- `regenerate_production_scene_still(job_id, scene_index)` → new seed on the base prompt.
- `set_production_scenes_animate(job_id, scene_indices=[3,7,12], animate=true/false)` or for all.
- `set_production_scene_duration(job_id, scene_index, duration_sec=4.5)` → precise pacing control per beat.
- `animate_production_scenes(job_id, scene_indices=[3,7])` → run i2v *only* on the chosen ones (or all currently flagged animate=true).
- `finalize_production(job_id)` → any missing motion uses Ken Burns, full VO + captions + mux. Produces the final MP4 with exactly the mix you chose.
You (the agent) can stay in a tight loop with the user: "I edited scene 4 with your note, here's the new still. Animate it? Or another change?" until they say the scenes are perfect, then finalize. This is how you achieve pixel-perfect pacing and visuals over many iterations. For long 30-min docs the same philosophy applies via longform + its regenerate tools + chapter approval.

═══ SKELETON NICHE (render_style=skeleton_host) ═══
When the user's Art Style is skeleton_host:
Never use load_skill image-generation for scene stills — Seedream edit from canonical master only.

STILLS (locked — not user-selectable):
- One canonical master PNG → every scene is Seedream 4.5 **edit** (`seedream_v45_edit_canonical`).
- SAME skeleton every beat: ivory bones, glass shell, realistic eyes. Identity never changes.
- Per scene, edit ONLY: background/environment, outfit/clothes, props, pose.
- Need muscles? Add muscle definition ON the same skeleton (wardrobe/body overlay) — not a new character.
- Need clothes? Edit wardrobe on the same skeleton. Different location? Edit background only.
- Rinse and repeat: master → edit → master → edit for every beat.

VIDEO (user-selectable — ask if unclear):
- Call `list_skeleton_video_models` and pass `video_model` to start_shortform_generate:
  `seedance` (default, 5 AC), `pixverse` (permissive), `kling_pro` (7 AC, best motion).

SCRIPT CATEGORY:
- Call `list_skeleton_categories` (20 YouTube lanes + user custom). Use `outcast` for edgy/contrarian.
- Missing lane → `create_skeleton_category` then render with returned key.

WARDROBE / STYLING (same skeleton — not a new character):
- When the user specifies clothes, age vibe, muscles, or props, pass it in `visual_brief` on
  `start_shortform_generate` (e.g. "teenager 18+, black hoodie and black pants, urban night").
- This locks outfit/props on the canonical skeleton; Seedream edit changes background + wardrobe only.

Do NOT tell users only classical_clash / wildcard_clash exist. Do NOT pick image models for skeleton shorts.

═══ Long-form (up to ~15 minutes or 30+ min documentaries) + thumbnails ═══
- Long-form: `start_longform_render` + `load_skill script-writing` + channel CHANNEL.md.
  Target ~8–15 chapters for a 15-minute doc; use compliance-preflight on YMYL channels.
- Thumbnails: `load_skill thumbnail-design` before proposing; cite channel grammar.
- Poll `poll_render_job` with kind=longform until complete.
- Granular control: After chapters are ready or in the render phase use `list_longform_scenes(job_id)` + `regenerate_longform_still(job_id, global_scene_idx)` to re-do specific stills with the longform image model. For full per-scene animate selection on a long doc, guide the user through chapter approval then use the still review + selective re-renders before the final compose (the pipeline already supports mixed animation costs and per-scene motion). The same "edit until perfect, animate only the hero moments" philosophy applies.

═══ Short-form & long-form without Skeleton AI (default for most creators) ═══
Rookcast skills at studio/skills/ — load_skill before steps. image-generation skills apply
to photoreal / channel stills, reference blueprints, thumbnails, and b-roll plans.
Before quoting spend, call get_fal_pricing when helpful.
YMYL: compliance-preflight + .gov sources. Follow CHANNEL.md when channel_key is set.

When proposing renders, explain cost/risk and which pipeline (blueprint short, long-form, Skeleton AI, etc.).
For topic research use get_public_search_trends and get_channel_analytics.
After starting a render, poll poll_render_job until complete or failed. The Studio UI also
auto-polls production jobs: live progress lines in chat, a production rail, bottom-right
monitor, stills gallery at awaiting_approval, one-click Finalize, and in-chat MP4 download.
Use finalize_longform_render when long-form hits awaiting_approval (or tell user to click
Finalize in chat). For shortform non-skeleton jobs the powerful scene control tools (list/edit/ selective animate / finalize) give you the ability to iterate individual scenes with V4.5 edit as many times as needed and choose exactly which ones get real motion. refresh_channel_intelligence after uploads; record_production_feedback
when the user reports performance (internal training, never sold).

Progress reporting (important): long-running tools run in the background and return a job_id.
- analyze_reference_video / analyze_competitor_video: poll kind=competitor through pacing + audio.
- Never go silent between start and finish. Summarize pacing (avg shot length) + hook window.

Data: every turn and tool call is logged for product improvement and future custom model training
(previews only; no secrets). Encourage users to connect YouTube and paste reference URLs — richer signal.

REPLY-TO RE-EDIT SUPPORT: Users can click the small "Reply & re-edit" arrow (or "Reply & re-edit" button under the player) on any completed video card in chat.
This sets up a reply context with the exact prior job_id + kind. When the incoming user message has this context (it will be prefixed with "[User is replying to their previous ... RE-EDIT that exact video]"), you **must** treat it as a request to surgically improve *that specific video the user was just shown*, not generate a fresh one.

Mandatory flow for almost all re-edit replies:
1. Call list_production_scenes (or list_longform_scenes) on the job_id from the context so you can see the current stills, durations, animate flags, narrations.
2. If the instruction calls for visual changes to particular scenes, use edit_production_scene_still (the Seedream V4.5 *edit* tool) on only those indices — never full new T2I for the whole thing.
3. For pacing, story flow, caption rhythm, or CTA issues, use set_production_scene_duration (selective), set_production_scenes_animate (selective indices), etc.
4. Call re_edit_production(job_id=..., instruction=the user's full request text, kind=...) — this is the dedicated tool for the "re-edit the same video" use case. It records the intent, re-uses the prior assets, and drives a re-finalize that produces a new MP4 + package.txt with proper editing, tighter pacing, 3-word captions, visual-VO lockstep, and a subscribe CTA at the end.
Only fall back to a full new start_*_generate if the user explicitly says "make a completely new one" or "change the entire art style and start over".

The user does real video editing and wants the AI to study real references (via analyze_youtube_video + yt-dlp when they paste links) and apply the exact observed decisions (hook length, cut rhythm, caption timing, CTA placement) on these re-edits. Re-use the video they already have; don't waste it by regenerating everything.

{skills.skills_index_for_prompt()}
"""


async def run_turn(
    session: dict[str, Any],
    user_text: str,
    *,
    membership_plan: str = "",
    billing_profile: dict[str, Any] | None = None,
    reply_to: dict | None = None,
) -> dict[str, Any]:
    """Process one user message; may queue pending_actions in confirm mode."""
    user_id = str(session.get("user_id") or "")
    profile = billing_profile or {}
    async with studio_agent_slot(
        user_id=user_id,
        plan=membership_plan,
        operation="chat",
        unlimited=bool(profile.get("unlimited")),
    ) as admission:
        result = await _run_turn_impl(session, user_text, billing_profile=billing_profile, reply_to=reply_to)
        if admission.mode != "disabled":
            result["queue"] = admission.as_dict()
        return result


async def stream_turn(
    session: dict[str, Any],
    user_text: str,
    *,
    membership_plan: str = "",
    billing_profile: dict[str, Any] | None = None,
    reply_to: dict | None = None,
) -> AsyncIterator[str]:
    """SSE stream of tool/status events, ending with event=done."""
    import json as _json

    user_id = str(session.get("user_id") or "")
    profile = billing_profile or {}

    def _sse(event: str, data: dict[str, Any]) -> str:
        return f"event: {event}\ndata: {_json.dumps(data, default=str)}\n\n"

    queue: asyncio.Queue[tuple[str, dict[str, Any] | None]] = asyncio.Queue()

    async def emit(payload: dict[str, Any]) -> None:
        await queue.put(("event", payload))

    async def worker() -> None:
        try:
            async with studio_agent_slot(
                user_id=user_id,
                plan=membership_plan,
                operation="chat",
                unlimited=bool(profile.get("unlimited")),
            ) as admission:
                result = await _run_turn_impl(
                    session,
                    user_text,
                    billing_profile=billing_profile,
                    emit=emit,
                    reply_to=reply_to,
                )
                if admission.mode != "disabled":
                    result["queue"] = admission.as_dict()
                await queue.put(("done", result))
        except (StudioAgentQueueFullError, StudioAgentQueueTimeoutError) as exc:
            await queue.put(("error", {"message": str(exc), "queue": True}))
        except Exception as exc:
            await queue.put(("error", {"message": str(exc)}))

    task = asyncio.create_task(worker())
    try:
        while True:
            kind, payload = await queue.get()
            if kind == "event" and payload:
                ev = str(payload.get("event") or "status")
                yield _sse(ev, payload)
                continue
            if kind == "done" and payload:
                yield _sse("done", payload)
                break
            if kind == "error":
                yield _sse("error", payload or {"message": "Agent turn failed"})
                break
    finally:
        if not task.done():
            task.cancel()


async def _run_turn_impl(
    session: dict[str, Any],
    user_text: str,
    *,
    billing_profile: dict[str, Any] | None = None,
    emit: EventEmitter | None = None,
    reply_to: dict | None = None,
) -> dict[str, Any]:
    sid = session["session_id"]
    user_id = session["user_id"]
    model = session.get("model") or openrouter.DEFAULT_MODEL
    approval_mode = session.get("approval_mode") or "confirm"
    content_format = session.get("content_format") or "both"
    reasoning_depth = session.get("reasoning_depth") or "balanced"
    web_search = bool(session.get("web_search", True))

    messages: list[dict[str, Any]] = list(session.get("messages") or [])
    messages.append({"role": "user", "content": user_text})
    store.touch_title_from_user_message(sid, user_text)

    if reply_to:
        job_id = str(reply_to.get("job_id") or "")
        kind = str(reply_to.get("kind") or "shortform")
        is_long = kind.lower().startswith("long")
        scene_tool_hint = "list_longform_scenes + regenerate_longform_still + longform finalize tools" if is_long else "list_production_scenes + edit_production_scene_still (V4.5 edit) + set_production_scenes_animate + set_production_scene_duration + animate_production_scenes + finalize_production"
        context_note = (
            f"[User is replying to their previous {kind} video production (job_id={job_id}). "
            "Treat the following message as instructions to RE-EDIT **that exact video** the user was just shown (do not start a brand new generation or regenerate all stills unless they explicitly say 'start over' or 'new visual style'). "
            "Goal: proper editing, pacing, storytelling, packaging + a clear subscribe CTA at the end. "
            f"First inspect with list_production_scenes (or list_longform_scenes). Use targeted edits only where the instruction requires (edit_production_scene_still for V4.5 edits on specific scenes, set_production_scene_duration, set_production_scenes_animate for selective animation). "
            "Then call the dedicated re_edit_production(job_id, instruction=the user's exact request, kind=...) tool — this is the correct surgical path that re-uses the prior stills/clips/video the user already has and only re-assembles with better timing, lockstep VO, 3-word captions, and CTA. NEVER call start_shortform_generate or start_longform_render during a reply-to re-edit — those create brand new jobs and full visual regeneration. "
            "After it finishes, the new improved deliverable (same job) will appear in chat for the user.]"
        )
        messages[-1]["content"] = context_note + "\n\n" + user_text

        # Pre-load the current state of the video being re-edited so the model sees the exact scenes/stills/narrations
        # immediately. This makes surgical decisions (which scene to tweak, what timing/CTA change) natural.
        try:
            list_tool = "list_longform_scenes" if is_long else "list_production_scenes"
            pre_list = execute_tool_logged(
                list_tool,
                {"job_id": job_id},
                user_id=user_id,
                content_format=content_format,
                session_id=sid,
            )
            # Inject as a tool observation so the model starts with the structure of the video to re-edit.
            messages.append({
                "role": "tool",
                "tool_call_id": f"pre_reply_{list_tool}",
                "content": pre_list,
            })
        except Exception as pre_exc:
            # Non-fatal; the model can still call list itself.
            pass
    telemetry.record_session_turn(
        user_id, sid, role="user", content_preview=user_text,
        model=session.get("model"), content_format=content_format,
    )

    profile = billing_profile or {}
    owner_unmetered = bool(profile.get("unlimited"))
    sys_content = system_prompt(
        content_format=content_format,
        reasoning_depth=reasoning_depth,
        billing_profile=profile,
        render_style=str(session.get("render_style") or "cinematic"),
    )
    if messages and messages[0].get("role") == "system":
        messages[0] = {"role": "system", "content": sys_content}
    else:
        messages.insert(0, {"role": "system", "content": sys_content})

    tools = tool_schemas()
    pending: list[dict[str, Any]] = []
    active_jobs: list[dict[str, Any]] = []
    assistant_text = ""
    usage_total: dict[str, Any] = {}
    acc_prompt_tokens = 0
    acc_completion_tokens = 0

    await _fire_event(emit, "status", message="Thinking…")

    for round_idx in range(MAX_TOOL_ROUNDS):
        model_messages = store.trim_messages_for_model(messages)
        await _fire_event(emit, "model_round", round=round_idx + 1)
        resp = await openrouter.chat_completion(
            messages=model_messages,
            tools=tools,
            model=model,
            reasoning_depth=reasoning_depth,
            web_search=web_search,
        )
        msg = openrouter.message_from_response(resp)
        usage = openrouter.usage_from_response(resp)
        usage_total = usage
        acc_prompt_tokens += int(usage.get("prompt_tokens", 0) or 0)
        acc_completion_tokens += int(usage.get("completion_tokens", 0) or 0)

        tool_calls = msg.get("tool_calls") or []
        content = msg.get("content") or ""

        if tool_calls:
            messages.append({
                "role": "assistant",
                "content": content or None,
                "tool_calls": tool_calls,
            })
            blocked = False
            for tc in tool_calls:
                fn = tc.get("function") or {}
                name = fn.get("name") or ""
                try:
                    raw_args = fn.get("arguments") or "{}"
                    args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                except json.JSONDecodeError:
                    args = {}
                if name == "start_shortform_generate":
                    args = _inject_shortform_render_style(args, session)

                # HARD GUARD for reply-to re-edit: never allow a full new production start when the user
                # explicitly clicked "Reply & re-edit" on an existing video card. Force the surgical path
                # on the exact prior job_id so we re-use the already-made stills/clips/video and only re-edit
                # pacing, captions, CTA, timing, story packaging.
                if reply_to and name in ("start_shortform_generate", "start_longform_render"):
                    job_id = str(reply_to.get("job_id") or "")
                    kind = str(reply_to.get("kind") or "shortform")
                    # Use the original user instruction (the part after the injected context note)
                    raw_user = user_text
                    reedit_res = re_edit_production(job_id, raw_user, kind)
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.get("id"),
                        "content": reedit_res,
                    })
                    await _fire_event(emit, "tool_end", tool=name, status="redirected_to_reedit")
                    continue

                await _fire_event(
                    emit,
                    "tool_start",
                    tool=name,
                    round=round_idx + 1,
                    awaiting_approval=approval_mode == "confirm" and requires_approval(name),
                )

                if approval_mode == "confirm" and requires_approval(name):
                    action_id = f"act_{uuid.uuid4().hex[:12]}"
                    pending.append({
                        "id": action_id,
                        "tool": name,
                        "arguments": args,
                        "summary": f"{name}({json.dumps(args)[:200]})",
                    })
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.get("id"),
                        "content": json.dumps({
                            "status": "awaiting_user_approval",
                            "action_id": action_id,
                            "message": "User must approve this action in Studio Agent UI (confirm mode).",
                        }),
                    })
                    blocked = True
                    await _fire_event(emit, "tool_end", tool=name, status="awaiting_approval")
                    continue

                try:
                    result = execute_tool_logged(
                        name,
                        args,
                        user_id=user_id,
                        content_format=content_format,
                        session_id=sid,
                    )
                except Exception as exc:
                    result = json.dumps({"error": str(exc)})

                if name in JOB_START_TOOLS:
                    active_jobs = merge_active_jobs(
                        active_jobs,
                        extract_jobs_from_tool(name, result),
                    )
                    store.update_session(
                        sid,
                        last_production={
                            "tool": name,
                            "arguments": args,
                            "updated_at": time.time(),
                        },
                    )
                    await _fire_event(emit, "active_jobs", jobs=active_jobs)

                err_preview = ""
                try:
                    parsed = json.loads(result or "{}")
                    if isinstance(parsed, dict) and parsed.get("error"):
                        err_preview = str(parsed.get("error"))[:120]
                except json.JSONDecodeError:
                    pass
                await _fire_event(
                    emit,
                    "tool_end",
                    tool=name,
                    status="error" if err_preview else "ok",
                    error=err_preview or None,
                )

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.get("id"),
                    "content": result,
                })

            if blocked and pending:
                await _fire_event(emit, "pending_actions", actions=pending)
                assistant_text = sanitize_assistant_text(
                    content
                    or "I prepared the next steps but need your approval before running commands that spend credits or write files."
                )
                break
            continue

        assistant_text = sanitize_assistant_text(content or "")
        messages.append({"role": "assistant", "content": assistant_text})
        break

    if assistant_text:
        assistant_text = sanitize_assistant_text(assistant_text)

    store.update_session(sid, messages=messages)
    if pending:
        store.set_pending_actions(sid, pending)
    if assistant_text:
        telemetry.record_session_turn(
            user_id, sid, role="assistant", content_preview=assistant_text,
            model=model, content_format=content_format,
        )

    if active_jobs:
        store.update_session(sid, active_jobs=active_jobs)

    # Meter OpenRouter token spend against the unified credit wallet.
    credits_charged = 0
    usd_cost = 0.0
    prompt_ppm = completion_ppm = None
    try:
        prompt_ppm, completion_ppm = await openrouter.model_pricing(model)
    except Exception:
        prompt_ppm = completion_ppm = None
    try:
        import unified_credits as uc

        usage_for_cost = {
            "prompt_tokens": acc_prompt_tokens,
            "completion_tokens": acc_completion_tokens,
        }
        usd_cost = uc.openrouter_usd(usage_for_cost, prompt_ppm, completion_ppm)
        if usd_cost > 0 and user_id and not owner_unmetered:
            credits_charged, _bal = uc.debit_usd(
                user_id,
                usd_cost,
                reason="studio_agent_openrouter",
                metadata={
                    "model": model,
                    "session_id": sid,
                    "prompt_tokens": acc_prompt_tokens,
                    "completion_tokens": acc_completion_tokens,
                    "prompt_price_per_m": prompt_ppm,
                    "completion_price_per_m": completion_ppm,
                },
                allow_negative=False,
            )
        elif usd_cost > 0 and user_id and owner_unmetered:
            credits_charged = 0
    except Exception:
        pass

    result = {
        "session_id": sid,
        "assistant_message": assistant_text,
        "pending_actions": pending,
        "active_jobs": active_jobs,
        "approval_mode": approval_mode,
        "reasoning_depth": reasoning_depth,
        "usage": usage_total,
        "billing": {
            "credits_charged": credits_charged,
            "provider_usd": round(float(usd_cost or 0.0), 6),
            "prompt_tokens": acc_prompt_tokens,
            "completion_tokens": acc_completion_tokens,
        },
    }
    await _fire_event(emit, "active_jobs", jobs=active_jobs)
    return result


async def approve_action(
    session: dict[str, Any],
    action_id: str,
    *,
    membership_plan: str = "",
    billing_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    user_id = str(session.get("user_id") or "")
    profile = billing_profile or {}
    async with studio_agent_slot(
        user_id=user_id,
        plan=membership_plan,
        operation="approve",
        unlimited=bool(profile.get("unlimited")),
    ) as admission:
        result = await _approve_action_impl(session, action_id, billing_profile=billing_profile)
        if admission.mode != "disabled":
            result["queue"] = admission.as_dict()
        return result


async def _approve_action_impl(
    session: dict[str, Any],
    action_id: str,
    *,
    billing_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sid = session["session_id"]
    fresh = store.get_session(sid) or session
    action = store.pop_pending_action(sid, action_id)
    if not action:
        action = store.recover_pending_action_from_messages(fresh, action_id)
    if not action:
        store.sync_pending_from_messages(sid)
        still = store.get_session(sid) or fresh
        for row in still.get("pending_actions") or []:
            if row.get("id") == action_id:
                action = store.pop_pending_action(sid, action_id)
                break
    if not action:
        action = store.recover_pending_action_from_messages(
            store.get_session(sid) or fresh,
            action_id,
        )
    if not action:
        raise KeyError(f"pending action not found: {action_id}")

    name = action["tool"]
    args = action.get("arguments") or {}
    if name == "start_shortform_generate":
        args = _inject_shortform_render_style(args, session)
    tool_error = ""
    try:
        result = execute_tool_logged(
            name,
            args,
            user_id=session["user_id"],
            content_format=session.get("content_format") or "both",
            session_id=sid,
        )
    except Exception as exc:
        tool_error = str(exc)
        result = json.dumps({"error": tool_error})

    started = extract_jobs_from_tool(name, result)
    fresh = store.get_session(sid) or session
    messages: list[dict[str, Any]] = list(fresh.get("messages") or [])
    messages.append({
        "role": "user",
        "content": (
            f"[User approved {name}]\nTool result:\n{result[:12000]}\n"
            "Summarize what happened and propose the next production step."
        ),
    })
    store.update_session(sid, messages=messages)

    if tool_error:
        hint = f"Approved {name} failed: {tool_error}"
        return {
            "session_id": sid,
            "assistant_message": hint,
            "pending_actions": [],
            "active_jobs": started,
            "approved_action": {
                "id": action_id,
                "tool": name,
                "error": tool_error,
                "result_preview": result[:2000],
            },
        }

    # Job-start tools: return immediately so approve does not block on a second LLM turn
    # (RunPod /runsync budget is ~90s; shortform spawn should surface in the render dock).
    if name in JOB_START_TOOLS:
        try:
            parsed = json.loads(result or "{}")
            preview = parsed.get("error") or parsed.get("note") or parsed.get("status")
        except json.JSONDecodeError:
            preview = result[:400]
        assistant_note = (
            f"Approved {name} — production is running. "
            f"{preview or 'Track progress in the render dock.'}"
        )
        messages.append({"role": "assistant", "content": assistant_note})
        store.update_session(
            sid,
            messages=messages,
            active_jobs=started,
            last_production={"tool": name, "arguments": args, "updated_at": time.time()},
        )
        return {
            "session_id": sid,
            "assistant_message": assistant_note,
            "pending_actions": [],
            "active_jobs": started,
            "approved_action": {"id": action_id, "tool": name, "result_preview": result[:2000]},
        }

    if started:
        store.update_session(sid, active_jobs=started)

    refreshed = store.get_session(sid) or session
    follow_up = await _run_turn_impl(
        refreshed,
        "Continue production from the approved action result.",
        billing_profile=billing_profile,
    )
    follow_up["active_jobs"] = merge_active_jobs(
        started,
        follow_up.get("active_jobs") or [],
    )
    follow_up["approved_action"] = {"id": action_id, "tool": name, "result_preview": result[:2000]}
    if started and not str(follow_up.get("assistant_message") or "").strip():
        follow_up["assistant_message"] = (
            f"Started {name} — track progress in the render dock and chat."
        )
    return follow_up


async def retry_last_production(
    session: dict[str, Any],
    *,
    membership_plan: str = "",
    billing_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Re-run the last approved/auto production tool (shortform/longform spawn)."""
    sid = session["session_id"]
    fresh = store.get_session(sid) or session
    lp = store.recover_last_production(fresh) or {}
    name = str(lp.get("tool") or "").strip()
    args = lp.get("arguments") or {}
    if not name or name not in JOB_START_TOOLS:
        raise KeyError("no production to retry — approve or run start_shortform_generate first")
    if name == "start_shortform_generate":
        args = _inject_shortform_render_style(args, fresh)
        # Resume the last shortform job's workspace so finished stills/clips/VO
        # are reused instead of re-rendered (and re-billed) from scratch.
        prev = [
            j for j in (fresh.get("active_jobs") or [])
            if j.get("kind") == "shortform" and j.get("job_id")
        ]
        if prev:
            args = {**args, "_resume_job_id": str(prev[-1]["job_id"])}
    if lp.get("recovered"):
        store.update_session(sid, last_production=lp)

    user_id = str(session.get("user_id") or "")
    profile = billing_profile or {}
    async with studio_agent_slot(
        user_id=user_id,
        plan=membership_plan,
        operation="retry_production",
        unlimited=bool(profile.get("unlimited")),
    ):
        try:
            result = execute_tool_logged(
                name,
                args,
                user_id=session["user_id"],
                content_format=session.get("content_format") or "both",
                session_id=sid,
            )
        except Exception as exc:
            raise RuntimeError(str(exc)) from exc

    started = extract_jobs_from_tool(name, result)
    messages = list((store.get_session(sid) or fresh).get("messages") or [])
    messages.append({
        "role": "user",
        "content": f"[User retried {name}]\nTool result:\n{result[:8000]}",
    })
    messages.append({
        "role": "assistant",
        "content": f"Retrying {name} — track the new job in the render dock.",
    })
    store.update_session(
        sid,
        messages=messages,
        active_jobs=merge_active_jobs(list(fresh.get("active_jobs") or []), started),
        last_production={"tool": name, "arguments": args, "updated_at": time.time()},
    )
    return {
        "session_id": sid,
        "assistant_message": f"Retrying {name} — production is running.",
        "active_jobs": started,
        "retried_tool": name,
    }
