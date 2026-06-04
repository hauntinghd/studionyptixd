"""Studio Agent conversation loop."""
from __future__ import annotations

import json
import uuid
from typing import Any

from studio_agent import openrouter, skills
from studio_agent import store
from studio_agent import telemetry
from studio_agent.tools import execute_tool_logged, requires_approval, tool_schemas

MAX_TOOL_ROUNDS = 12


def system_prompt(*, content_format: str) -> str:
    fmt_hint = {
        "short": "User session bias: short-form. Still confirm if they pivot to long-form.",
        "long": "User session bias: long-form. Still confirm if they pivot to shorts.",
        "both": "Infer short vs long from the conversation — do not ask unless genuinely ambiguous.",
    }.get(content_format, "Infer short vs long from the conversation.")

    return f"""You are NYPTID Studio Agent — the primary NYPTID Studio product. You help creators who
do NOT know what to film: pick niche + topic, frame the video beat-by-beat, then produce at
Lume / MrBeast / Jake Tran / Magnates Media quality (perfect pacing, packaging, delivery).

{fmt_hint}

═══ "I don't know what to make" (topic + niche discovery) ═══
- Start with `recommend_video_topics` (registry_key if connected, else niche_query).
- New/0-sub channel: positioning sprint + reference homework — never shame them for "failed" videos.
- Established: clone winners from growth_playbook + trending topics.
- Hardest steps (say this clearly): (1) script-writing / story beats, (2) packaging (title + thumbnail).
- After topic is chosen, help them down to the **frame**: scene list, hook, pattern interrupts, outro CTA.

═══ Reference video → scene blueprint (yt-dlp full power) ═══
When user links a Lume/MrBeast/Jake Tran/Magnates/Mamoru-style video:
1. `analyze_reference_video` (yt-dlp download + scene keyframes + cut pacing + audio).
2. Poll `poll_render_job` kind=competitor — report every stage live.
3. `build_scene_blueprint_from_reference` — per-scene rows: 1–5 characters (channel-dependent),
   Seedream v4.5 **edit** for wardrobe/props/background only (same identity), i2v duration per cut.
4. `load_skill script-writing` — map narration to story_beat labels in the blueprint.
5. Audio: VO slightly louder than BGM (not extreme); per-scene BGM mood in blueprint; use search_music.

═══ YOUTUBE CHANNEL INTELLIGENCE (start here when user mentions their channel) ═══
1. `youtube_oauth_status` — if not connected, send them to Studio → Settings → Channels.
2. `list_youtube_channels` — only shows THIS user's connected channels.
3. `get_channel_analytics` — Catalyst winners/losers + `growth_playbook` (brand_new / early /
   growing / established). For 0-sub channels: positioning + competitor homework, NOT "why X failed."
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

═══ SKELETON AI SHORTS (mandatory when user wants skeleton / NYPTID / comparison shorts) ═══
ONLY render path: `start_shortform_generate` (+ poll_render_job kind=shortform). Never use
load_skill image-generation, never fal T2I per scene, never ERNIE/Flux for scene stills.

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

═══ Long-form (up to ~15 minutes) + thumbnails ═══
- Long-form: `start_longform_render` + `load_skill script-writing` + channel CHANNEL.md.
  Target ~8–15 chapters for a 15-minute doc; use compliance-preflight on YMYL channels.
- Thumbnails: `load_skill thumbnail-design` before proposing; cite channel grammar.
- Poll `poll_render_job` with kind=longform until complete.

═══ Other content (non-skeleton stills) ═══
Rookcast skills at studio/skills/ — load_skill before steps. image-generation skills apply
ONLY to non-skeleton work. Before quoting spend, call get_fal_pricing when helpful.
YMYL: compliance-preflight + .gov sources. Follow CHANNEL.md when channel_key is set.

When proposing renders, explain cost/risk. For non-skeleton topic research use get_public_search_trends.
After starting a render, poll poll_render_job until complete or failed.

Progress reporting (important): long-running tools run in the background and return a job_id.
- analyze_reference_video / analyze_competitor_video: poll kind=competitor through pacing + audio.
- Never go silent between start and finish. Summarize pacing (avg shot length) + hook window.

Data: every turn and tool call is logged for product improvement and future custom model training
(previews only; no secrets). Encourage users to connect YouTube and paste reference URLs — richer signal.

{skills.skills_index_for_prompt()}
"""


async def run_turn(
    session: dict[str, Any],
    user_text: str,
) -> dict[str, Any]:
    """Process one user message; may queue pending_actions in confirm mode."""
    sid = session["session_id"]
    user_id = session["user_id"]
    model = session.get("model") or openrouter.DEFAULT_MODEL
    approval_mode = session.get("approval_mode") or "confirm"
    content_format = session.get("content_format") or "both"

    messages: list[dict[str, Any]] = list(session.get("messages") or [])
    messages.append({"role": "user", "content": user_text})
    store.touch_title_from_user_message(sid, user_text)
    telemetry.record_session_turn(
        user_id, sid, role="user", content_preview=user_text,
        model=session.get("model"), content_format=content_format,
    )

    if not any(m.get("role") == "system" for m in messages):
        messages.insert(0, {"role": "system", "content": system_prompt(content_format=content_format)})

    tools = tool_schemas()
    pending: list[dict[str, Any]] = []
    assistant_text = ""
    usage_total: dict[str, Any] = {}
    acc_prompt_tokens = 0
    acc_completion_tokens = 0

    for _ in range(MAX_TOOL_ROUNDS):
        resp = await openrouter.chat_completion(
            messages=messages,
            tools=tools,
            model=model,
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

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.get("id"),
                    "content": result,
                })

            if blocked and pending:
                assistant_text = (
                    content
                    or "I prepared the next steps but need your approval before running commands that spend credits or write files."
                )
                break
            continue

        assistant_text = content or ""
        messages.append({"role": "assistant", "content": assistant_text})
        break

    store.update_session(sid, messages=messages)
    if pending:
        store.set_pending_actions(sid, pending)
    if assistant_text:
        telemetry.record_session_turn(
            user_id, sid, role="assistant", content_preview=assistant_text,
            model=model, content_format=content_format,
        )

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
        if usd_cost > 0 and user_id:
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
                allow_negative=True,
            )
    except Exception:
        pass

    return {
        "session_id": sid,
        "assistant_message": assistant_text,
        "pending_actions": pending,
        "approval_mode": approval_mode,
        "usage": usage_total,
        "billing": {
            "credits_charged": credits_charged,
            "provider_usd": round(float(usd_cost or 0.0), 6),
            "prompt_tokens": acc_prompt_tokens,
            "completion_tokens": acc_completion_tokens,
        },
    }


async def approve_action(session: dict[str, Any], action_id: str) -> dict[str, Any]:
    action = store.pop_pending_action(session["session_id"], action_id)
    if not action:
        raise KeyError(f"pending action not found: {action_id}")

    name = action["tool"]
    args = action.get("arguments") or {}
    result = execute_tool_logged(
        name,
        args,
        user_id=session["user_id"],
        content_format=session.get("content_format") or "both",
        session_id=session["session_id"],
    )

    messages: list[dict[str, Any]] = list(session.get("messages") or [])
    messages.append({
        "role": "user",
        "content": (
            f"[User approved {name}]\nTool result:\n{result[:12000]}\n"
            "Summarize what happened and propose the next production step."
        ),
    })
    store.update_session(session["session_id"], messages=messages)

    refreshed = store.get_session(session["session_id"]) or session
    follow_up = await run_turn(refreshed, "Continue production from the approved action result.")
    follow_up["approved_action"] = {"id": action_id, "tool": name, "result_preview": result[:2000]}
    return follow_up
