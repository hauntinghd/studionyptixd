"""Studio Agent tool registry + execution."""
from __future__ import annotations

import asyncio
import concurrent.futures
import json
import os
import subprocess
import sys
import threading
import uuid
from pathlib import Path
from typing import Any

from studio_agent import skills as skill_loader
from studio_agent import telemetry

ROOT = Path(__file__).resolve().parents[1]
SKELETON_OUTPUT = Path(os.getenv("SKELETON_AI_OUTPUT_ROOT", "skeleton_ai/output"))

# Tools that mutate state or spend money — require confirm mode approval.
APPROVAL_REQUIRED = frozenset({
    "start_longform_render",
    "start_shortform_generate",
    "run_build_script",
    "write_project_file",
})

_async_pool = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="studio-agent-async")


def _run_async(coro):
    """Run async coroutine from sync execute_tool (may be called inside FastAPI loop)."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    fut = _async_pool.submit(asyncio.run, coro)
    return fut.result(timeout=120)


def tool_schemas() -> list[dict[str, Any]]:
    return [
        {
            "type": "function",
            "function": {
                "name": "list_skills",
                "description": "List all Rookcast skill slugs imported into studio/skills/.",
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "load_skill",
                "description": "Load a Rookcast SKILL.md playbook by slug (e.g. script-writing, thumbnail-design).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "slug": {"type": "string", "description": "Skill folder name"},
                        "companion": {
                            "type": "string",
                            "description": "Optional companion file e.g. beat-anatomy.md",
                        },
                    },
                    "required": ["slug"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "load_channel_docs",
                "description": "Load CHANNEL.md and/or FLOW.md for a Studio channel key.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "channel_key": {"type": "string"},
                        "doc": {
                            "type": "string",
                            "enum": ["CHANNEL", "FLOW", "both"],
                            "default": "both",
                        },
                    },
                    "required": ["channel_key"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_studio_channels",
                "description": "List long-form channel keys from long_form/prompts/channels.py registry.",
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "read_project_file",
                "description": "Read a text file under the repo root (paths must stay inside workspace).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "relative_path": {"type": "string"},
                        "max_chars": {"type": "integer", "default": 12000},
                    },
                    "required": ["relative_path"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "write_project_file",
                "description": "Write or overwrite a text file under studio/ or long_form/ (approval in confirm mode).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "relative_path": {"type": "string"},
                        "content": {"type": "string"},
                    },
                    "required": ["relative_path", "content"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "start_longform_render",
                "description": (
                    "Queue a long-form render via the Studio pipeline. "
                    "Requires channel_key + outline JSON. Spends fal credits."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "channel_key": {"type": "string"},
                        "title": {"type": "string"},
                        "topic": {"type": "string"},
                        "chapters_json": {
                            "type": "string",
                            "description": "JSON string: {title, chapters:[{title, beats}]}",
                        },
                    },
                    "required": ["channel_key", "title", "topic"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_skeleton_video_models",
                "description": (
                    "List selectable i2v models for Skeleton AI shorts. Image stills are "
                    "ALWAYS canonical Seedream 4.5 edit (not selectable). User picks video only."
                ),
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_skeleton_categories",
                "description": (
                    "List Skeleton AI script categories: 20 YouTube-aligned built-ins "
                    "(outcast, people_blogs, gaming, …) plus this user's custom categories. "
                    "Call before start_shortform_generate when category is non-obvious."
                ),
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "create_skeleton_category",
                "description": (
                    "Create a custom Skeleton AI category for this user (e.g. outcast, "
                    "true crime lane, channel-specific tone). Returns the new category_key."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "label": {"type": "string", "description": "Display name e.g. Outcast"},
                        "key": {"type": "string", "description": "Optional slug; auto-generated if omitted"},
                        "tagline": {"type": "string"},
                        "system_prompt": {
                            "type": "string",
                            "description": "Optional Grok system tone; auto-generated from label if omitted",
                        },
                        "seed_ideas": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                    },
                    "required": ["label"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "start_shortform_generate",
                "description": (
                    "Queue a Skeleton AI short (canonical skeleton stills + user-chosen i2v). "
                    "Stills: locked Seedream edit from master — same skeleton; only background/outfit/props change. "
                    "Call list_skeleton_video_models for video_model. list_skeleton_categories for category_key."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "category_key": {
                            "type": "string",
                            "description": "e.g. outcast, people_blogs, custom_my_lane",
                        },
                        "topic": {"type": "string"},
                        "script": {"type": "string", "description": "Optional pre-written script"},
                        "video_model": {
                            "type": "string",
                            "enum": ["seedance", "pixverse", "kling_pro"],
                            "description": "i2v model (required for skeleton shorts). Stills are NOT configurable.",
                        },
                        "visual_brief": {
                            "type": "string",
                            "description": (
                                "Locked wardrobe/styling on the SAME canonical skeleton "
                                "(e.g. teenager 18+, black hoodie and black pants). "
                                "Applied to every beat via Seedream edit."
                            ),
                        },
                    },
                    "required": ["category_key", "topic", "video_model"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "run_build_script",
                "description": (
                    "Run an allowlisted long_form build script (approval in confirm mode). "
                    "Example: long_form/build_cryptic_ctr_ss_rook.py --preview"
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "script": {
                            "type": "string",
                            "description": "Path under long_form/ e.g. build_cryptic_ctr_ss_rook.py",
                        },
                        "args": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "CLI args e.g. ['--preview']",
                        },
                    },
                    "required": ["script"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "youtube_oauth_status",
                "description": "Explain Studio YouTube OAuth scopes and how to connect channels in Settings.",
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_youtube_channels",
                "description": "List OAuth-connected YouTube channels with harvest/analytics status.",
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_studio_credits",
                "description": (
                    "Unified credit wallet balance, plan, recent ledger. "
                    "Use before expensive renders; tell user to top up in Studio Wallet when low."
                ),
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_channel_analytics",
                "description": (
                    "Connected-channel Catalyst insights + growth playbook (new vs established, "
                    "what's working / not, next posts) + latest video velocity when OAuth allows."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "channel_id": {"type": "string"},
                        "registry_key": {
                            "type": "string",
                            "description": "long_form channel key e.g. cryptic_science",
                        },
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_public_search_trends",
                "description": (
                    "Public YouTube search demand (last 30 days) + predicted topic scores. "
                    "Use registry_key to bias queries to a channel niche."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "registry_key": {"type": "string"},
                        "days": {"type": "integer", "default": 30},
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_fal_pricing",
                "description": (
                    "Fetch live fal.ai Platform API pricing for image/i2v/TTS endpoints. "
                    "Use before quoting render costs. Returns USD estimates per model/endpoint."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "endpoints": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Optional fal endpoint ids e.g. fal-ai/flux-pro/v1.1",
                        },
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "search_archival_media",
                "description": (
                    "Search FREE public-domain / archival media (footage, stills, records) "
                    "across Internet Archive, NASA, Library of Congress, Wikimedia Commons, "
                    "NPS, and FBI. Use this BEFORE generating with fal — real archival assets "
                    "are higher quality and cost nothing. Great for history, documentary, "
                    "science, and criminal-case content (e.g. Empire Magnates)."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "preset": {
                            "type": "string",
                            "enum": ["history", "documentary", "science", "criminal", "nature", "all"],
                            "description": "Curated source set. Omit to use 'documentary'.",
                        },
                        "sources": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "enum": ["internet_archive", "nasa", "loc", "wikimedia", "nps", "fbi"],
                            },
                            "description": "Explicit sources (overrides preset).",
                        },
                        "limit_per_source": {"type": "integer", "default": 8},
                    },
                    "required": ["query"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "analyze_reference_video",
                "description": (
                    "Download a reference YouTube video (Lume, MrBeast, Jake Tran, Magnates, Mamoru, etc.) "
                    "via yt-dlp: metadata, scene keyframes, cut timeline pacing, audio for transcription. "
                    "Poll poll_render_job(kind=competitor), then build_scene_blueprint_from_reference."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "url": {"type": "string", "description": "YouTube video URL"},
                        "scene_threshold": {"type": "number", "default": 0.3},
                        "max_frames": {"type": "integer", "default": 40},
                    },
                    "required": ["url"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "analyze_competitor_video",
                "description": "Alias of analyze_reference_video (competitor/outlier study).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "url": {"type": "string", "description": "YouTube video URL"},
                        "scene_threshold": {
                            "type": "number",
                            "default": 0.3,
                            "description": "Scene-cut sensitivity (0.2 = more frames, 0.4 = fewer).",
                        },
                        "max_frames": {"type": "integer", "default": 32},
                    },
                    "required": ["url"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "build_scene_blueprint_from_reference",
                "description": (
                    "After analyze_reference_video completes: map keyframes + pacing into per-scene "
                    "rows (1–5 characters), Seedream v4.5 edit fields, i2v duration, BGM cues, audio mix."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "topic": {"type": "string"},
                        "channel_style": {
                            "type": "string",
                            "enum": ["premium_doc", "viral_short", "story_manhwa"],
                            "default": "premium_doc",
                        },
                        "characters_per_scene": {
                            "type": "integer",
                            "default": 1,
                            "description": "1 for skeleton host; up to 5 for ensemble/cast channels.",
                        },
                        "visual_brief": {"type": "string"},
                        "target_scene_count": {"type": "integer"},
                    },
                    "required": ["job_id", "topic"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "recommend_video_topics",
                "description": (
                    "For creators who don't know what to film: merge channel analytics (if connected), "
                    "growth playbook, and public search trends into ranked topic + niche recommendations."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "registry_key": {"type": "string"},
                        "niche_query": {"type": "string"},
                        "days": {"type": "integer", "default": 30},
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "search_music",
                "description": "Search free Creative Commons music (Jamendo) for background tracks. Returns direct audio download URLs.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "instrumental": {"type": "boolean", "default": False},
                        "limit": {"type": "integer", "default": 12},
                    },
                    "required": ["query"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "search_sfx",
                "description": "Search free sound effects (Freesound, CC0 by default for attribution-free commercial use).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "cc0_only": {"type": "boolean", "default": True},
                        "limit": {"type": "integer", "default": 12},
                    },
                    "required": ["query"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "poll_render_job",
                "description": (
                    "Poll job status by job_id and kind. Use kind='competitor' for "
                    "analyze_competitor_video to surface live progress stages."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "kind": {"type": "string", "enum": ["longform", "shortform", "competitor"]},
                    },
                    "required": ["job_id", "kind"],
                },
            },
        },
    ]


def _safe_path(relative: str) -> Path:
    rel = relative.replace("\\", "/").lstrip("/")
    if ".." in rel.split("/"):
        raise ValueError("path traversal not allowed")
    full = (ROOT / rel).resolve()
    if not str(full).startswith(str(ROOT.resolve())):
        raise ValueError("path outside workspace")
    return full


def _allow_write(path: Path) -> None:
    rel = path.relative_to(ROOT.resolve())
    parts = rel.parts
    allowed_roots = ("studio", "long_form", "recaps")
    if not parts or parts[0] not in allowed_roots:
        raise ValueError(f"writes only allowed under studio/, long_form/, recaps/ — got {rel}")


ALLOWED_BUILD_SCRIPTS = frozenset({
    "build_cryptic_ctr_ss_rook.py",
    "build_cryptic_google_ai_mode_rook.py",
    "build_cryptic_google_ai_mode.py",
})


def _build_outline_from_args(args: dict[str, Any]) -> dict[str, Any]:
    raw = str(args.get("chapters_json") or "").strip()
    if raw:
        outline = json.loads(raw)
        if isinstance(outline, dict) and outline.get("chapters"):
            return outline
    title = str(args.get("title") or "Untitled").strip()
    topic = str(args.get("topic") or title).strip()
    return {
        "title": title,
        "chapters": [
            {
                "title": topic,
                "beats": [
                    {"text": f"Intro: {topic}", "visual": f"Cinematic opening — {topic}"},
                    {"text": f"Core story: {topic}", "visual": f"Documentary still — {topic}"},
                    {"text": f"Conclusion: {topic}", "visual": f"Closing frame — {topic}"},
                ],
            }
        ],
    }


def _debit_fal_for_outline(user_id: str, outline: dict[str, Any], *, job_id: str, kind: str) -> dict[str, Any]:
    """Charge the unified wallet for fal spend using the outline's scene plan.

    Estimate-at-start using live fal pricing (reconciled by the pipeline later).
    One image + ~5s i2v clip per beat, plus narration TTS chars.
    """
    try:
        import unified_credits as uc

        beats = 0
        tts_chars = 0
        for ch in outline.get("chapters") or []:
            for beat in ch.get("beats") or []:
                beats += 1
                tts_chars += len(str(beat.get("text") or ""))
        if beats <= 0:
            return {"credits_charged": 0, "note": "no beats to price"}
        credits, balance = uc.debit_fal_render(
            user_id,
            images=beats,
            video_seconds=beats * 5.0,
            tts_chars=tts_chars,
            reason=f"studio_agent_{kind}_estimate",
            metadata={"job_id": job_id, "beats": beats},
        )
        return {"credits_charged": credits, "balance_after": balance, "estimate": True, "beats": beats}
    except Exception as exc:
        return {"credits_charged": 0, "error": str(exc)[:200]}


def _spawn_shortform_job(
    *,
    category_key: str,
    topic: str | None,
    script: str | None,
    tier: str = "standard",
    video_model: str | None = None,
    visual_brief: str | None = None,
    user_id: str | None = None,
) -> str:
    job_id = uuid.uuid4().hex[:12]
    workspace = (ROOT / SKELETON_OUTPUT / job_id).resolve()
    workspace.mkdir(parents=True, exist_ok=True)

    def _work() -> None:
        from skeleton_ai.pipeline import run as run_pipeline

        try:
            result = run_pipeline(
                category_key=category_key,
                topic=topic,
                workspace=workspace,
                tier=tier,
                video_model=video_model,
                visual_brief=visual_brief,
                script_override=script,
                user_id=user_id,
            )
            payload = {"status": "complete", "job_id": job_id, **result}
        except Exception as exc:
            payload = {"status": "failed", "job_id": job_id, "error": str(exc)}
        (workspace / "result.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    threading.Thread(target=_work, daemon=True, name=f"sf-{job_id}").start()
    return job_id


def execute_tool(
    name: str,
    arguments: dict[str, Any],
    *,
    user_id: str,
    content_format: str,
    session_id: str | None = None,
) -> str:
    args = arguments or {}
    if name in ("analyze_reference_video", "analyze_competitor_video"):
        name = "analyze_reference_video"

    if name == "list_skills":
        return json.dumps({"skills": skill_loader.list_skill_slugs()}, indent=2)

    if name == "load_skill":
        slug = str(args.get("slug", "")).strip()
        companion = str(args.get("companion") or "").strip()
        if companion:
            text = skill_loader.read_skill_companion(slug, companion)
        else:
            text = skill_loader.read_skill(slug)
        return text

    if name == "load_channel_docs":
        key = str(args.get("channel_key", "")).strip()
        doc = str(args.get("doc") or "both").strip().lower()
        out: dict[str, str] = {}
        if doc in ("channel", "both"):
            out["CHANNEL"] = skill_loader.read_channel_doc(key, "CHANNEL")
        if doc in ("flow", "both"):
            out["FLOW"] = skill_loader.read_channel_doc(key, "FLOW")
        return json.dumps(out, indent=2)

    if name == "list_studio_channels":
        from long_form.prompts.channels import list_channels
        return json.dumps(list_channels(), indent=2, ensure_ascii=True)

    if name == "read_project_file":
        path = _safe_path(str(args.get("relative_path", "")))
        if not path.is_file():
            raise FileNotFoundError(str(path))
        max_chars = int(args.get("max_chars") or 12000)
        text = path.read_text(encoding="utf-8", errors="replace")
        if len(text) > max_chars:
            text = text[:max_chars] + "\n… truncated"
        return text

    if name == "write_project_file":
        path = _safe_path(str(args.get("relative_path", "")))
        _allow_write(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(args.get("content") or ""), encoding="utf-8")
        return json.dumps({"written": str(path.relative_to(ROOT)), "bytes": path.stat().st_size})

    if name == "start_longform_render":
        from long_form.prompts.channels import get_channel
        from long_form import pipeline as lf_pipeline

        channel_key = str(args.get("channel_key") or "").strip()
        channel = get_channel(channel_key)
        outline = _build_outline_from_args(args)
        job_id = lf_pipeline.start_render(channel, outline)
        billing = _debit_fal_for_outline(user_id, outline, job_id=job_id, kind="longform")
        return json.dumps({
            "status": "started",
            "job_id": job_id,
            "channel_key": channel_key,
            "pipeline_kind": channel.get("pipeline_kind") or "sleep_doc",
            "poll_url": f"/api/long-form/jobs/{job_id}/status",
            "finalize_url": f"/api/long-form/jobs/{job_id}/finalize",
            "outline_title": outline.get("title"),
            "chapters": len(outline.get("chapters") or []),
            "billing": billing,
        }, indent=2)

    if name == "list_skeleton_video_models":
        from skeleton_ai.i2v_engine import list_video_models

        return json.dumps(
            {
                "video_models": list_video_models(),
                "stills": {
                    "model": "seedream_v45_edit",
                    "locked": True,
                    "rule": (
                        "Every scene edits the canonical skeleton master — same identity; "
                        "change background, clothes, muscles-on-shell, props only."
                    ),
                },
            },
            indent=2,
        )

    if name == "list_skeleton_categories":
        from skeleton_ai.prompts.category_registry import list_categories, list_valid_keys

        uid = str(user_id or "").strip() or None
        cats = list_categories(user_id=uid)
        return json.dumps(
            {
                "categories": cats,
                "valid_keys": list_valid_keys(uid),
                "hint": "Built-in outcast covers edgy/contrarian lanes; use create_skeleton_category for a personalized variant.",
            },
            indent=2,
            ensure_ascii=True,
        )

    if name == "create_skeleton_category":
        from skeleton_ai.prompts.category_registry import create_custom_category

        uid = str(user_id or "").strip()
        if not uid:
            raise ValueError("sign in required to create custom categories")
        entry = create_custom_category(
            uid,
            label=str(args.get("label") or "").strip(),
            key=str(args.get("key") or "").strip() or None,
            tagline=str(args.get("tagline") or "").strip() or None,
            system_prompt=str(args.get("system_prompt") or "").strip() or None,
            seed_ideas=[str(s) for s in (args.get("seed_ideas") or [])],
        )
        return json.dumps({"category": entry, "category_key": entry["key"]}, indent=2)

    if name == "start_shortform_generate":
        from skeleton_ai.prompts.category_registry import get_category
        from skeleton_ai.i2v_engine import resolve_video_model_chain

        category_key = str(args.get("category_key") or "people_blogs").strip()
        topic = str(args.get("topic") or "").strip() or None
        script = str(args.get("script") or "").strip() or None
        video_model = str(args.get("video_model") or "seedance").strip()
        tier = str(args.get("tier") or "standard").strip()
        if video_model == "kling_pro":
            tier = "premium"
        uid = str(user_id or "").strip() or None
        get_category(category_key, user_id=uid)
        _, resolved_vm = resolve_video_model_chain(video_model=video_model, tier=tier)
        job_id = _spawn_shortform_job(
            category_key=category_key,
            topic=topic,
            script=script,
            tier=tier,
            video_model=resolved_vm,
            visual_brief=visual_brief,
            user_id=uid,
        )
        return json.dumps({
            "status": "started",
            "job_id": job_id,
            "category_key": category_key,
            "topic": topic,
            "visual_brief": visual_brief,
            "video_model": resolved_vm,
            "stills_model": "seedream_v45_edit_canonical",
            "poll_url": f"/api/skeleton-ai/jobs/{job_id}",
            "note": (
                "Skeleton AI: canonical master + Seedream edit per scene (identity locked); "
                f"i2v via {resolved_vm}. Poll until result.json is complete or failed."
            ),
        }, indent=2)

    if name == "run_build_script":
        script_name = Path(str(args.get("script", ""))).name
        if script_name not in ALLOWED_BUILD_SCRIPTS:
            raise ValueError(f"script not allowlisted: {script_name}")
        script_path = ROOT / "long_form" / script_name
        if not script_path.is_file():
            raise FileNotFoundError(script_name)
        cli_args = [sys.executable, str(script_path)] + [str(a) for a in (args.get("args") or [])]
        proc = subprocess.run(
            cli_args,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=3600,
        )
        return json.dumps({
            "exit_code": proc.returncode,
            "stdout_tail": (proc.stdout or "")[-8000:],
            "stderr_tail": (proc.stderr or "")[-4000:],
        }, indent=2)

    if name == "youtube_oauth_status":
        doc = ROOT / "studio" / "docs" / "YOUTUBE_OAUTH_SCOPES.md"
        if doc.exists():
            return doc.read_text(encoding="utf-8")
        return (
            "Connect YouTube in Studio → Settings → Channels. "
            "Scopes: youtube.readonly, yt-analytics.readonly, youtube.force-ssl, youtube.upload. "
            "See OAUTH_PUBLISH_RUNBOOK.md for Google Cloud Console steps."
        )

    if name == "get_studio_credits":
        import unified_credits as uc

        uid = str(user_id or "").strip()
        if not uid:
            raise ValueError("sign in required")
        try:
            uc.ensure_monthly_grant(uid)
            state = uc.get_state(uid)
            state["recent"] = uc.recent_ledger(uid, limit=8)
        except Exception as exc:
            state = {"balance": 0, "error": str(exc)}
        if int(state.get("balance") or 0) < 15:
            state["top_up_hint"] = "Low balance — user can add credits anytime in Studio → Wallet (unlimited top-ups)."
        return json.dumps(state, indent=2)

    if name == "list_youtube_channels":
        from youtube_connections_store import hydrate
        from long_form.prompts.channels import CHANNELS

        hyd = hydrate() or {}
        uid = str(user_id or "").strip()
        id_to_key = {v["channel_id"]: k for k, v in CHANNELS.items() if v.get("channel_id")}
        out: list[dict[str, Any]] = []
        for owner_id, u in hyd.items():
            if not isinstance(u, dict):
                continue
            if uid and str(owner_id) != uid:
                continue
            for ch_id, rec in (u.get("channels") or {}).items():
                if not isinstance(rec, dict):
                    continue
                key = id_to_key.get(ch_id, "")
                out.append({
                    "channel_id": ch_id,
                    "title": rec.get("title") or rec.get("channel_handle"),
                    "subscribers": int(rec.get("subscriber_count", 0) or 0),
                    "harvest_present": bool(rec.get("analytics_snapshot")),
                    "registry_key": key,
                })
        return json.dumps({"channels": out, "total": len(out)}, indent=2)

    if name == "get_channel_analytics":
        async def _fetch():
            from long_form.catalyst_bridge import (
                CHANNEL_KEY_TO_ID,
                assess_channel_growth,
                fetch_channel_snapshot,
                shape_catalyst_insights,
            )

            ch_id = str(args.get("channel_id") or "").strip()
            reg_key = str(args.get("registry_key") or "").strip()
            if not ch_id and reg_key:
                ch_id = CHANNEL_KEY_TO_ID.get(reg_key, "")
            if not ch_id:
                raise ValueError("channel_id or registry_key required")
            record = fetch_channel_snapshot(ch_id) or {}
            insights = shape_catalyst_insights(record)
            harvest = bool((record.get("analytics_snapshot") or {}))
            growth = assess_channel_growth(insights, harvest_present=harvest)

            velocity: dict[str, Any] = {}
            uid = str(user_id or "").strip()
            if uid:
                try:
                    from youtube import (
                        _youtube_connected_channel_access_token,
                        youtube_get_latest_video_velocity,
                    )

                    access_token, _rec = await _youtube_connected_channel_access_token(
                        {"id": uid}, ch_id
                    )
                    if access_token:
                        velocity = await youtube_get_latest_video_velocity(
                            access_token=access_token,
                            channel_id=ch_id,
                        )
                except Exception:
                    velocity = {}

            return {
                "channel_id": ch_id,
                "registry_key": reg_key or next(
                    (k for k, v in CHANNEL_KEY_TO_ID.items() if v == ch_id),
                    "",
                ),
                "channel_title": record.get("title") or record.get("channel_handle") or "",
                "insights": insights,
                "growth_playbook": growth,
                "latest_video_velocity": velocity,
            }

        return json.dumps(_run_async(_fetch()), indent=2)

    if name == "get_public_search_trends":
        async def _fetch():
            from studio_analytics_router import _fetch_public_search_videos, _default_queries_for_registry, _predict_topics
            from long_form.catalyst_bridge import CHANNEL_KEY_TO_ID, fetch_channel_snapshot, shape_catalyst_insights

            reg_key = str(args.get("registry_key") or "").strip()
            query = str(args.get("query") or "").strip()
            days = int(args.get("days") or 30)
            queries = [query] if query else (_default_queries_for_registry(reg_key) if reg_key else ["YouTube viral"])
            channel_titles: list[str] = []
            niche_keywords: list[str] = []
            if reg_key:
                try:
                    from long_form.prompts.channels import get_channel

                    ch = get_channel(reg_key)
                    niche_keywords = [str(ch.get("label") or reg_key)]
                    ch_id = CHANNEL_KEY_TO_ID.get(reg_key, "")
                    if ch_id:
                        ins = shape_catalyst_insights(fetch_channel_snapshot(ch_id))
                        channel_titles = [t.get("title", "") for t in ins.get("top_titles") or []]
                except Exception:
                    pass
            all_titles: list[str] = []
            rows: list[dict[str, Any]] = []
            for q in queries[:2]:
                batch = await _fetch_public_search_videos(q, days=days, max_results=10, order="viewCount")
                rows.extend(batch)
                all_titles.extend([str(r.get("title") or "") for r in batch if r.get("title")])
            predictions = _predict_topics(
                trending_titles=all_titles,
                channel_titles=channel_titles,
                niche_keywords=niche_keywords or queries,
            )
            return {"window_days": days, "queries": queries, "videos": rows[:20], "predicted_topics": predictions}

        return json.dumps(_run_async(_fetch()), indent=2)

    if name == "get_fal_pricing":
        try:
            from long_form.fal_pricing import get_pricing_snapshot, ENDPOINTS

            snap = get_pricing_snapshot()
            endpoints = args.get("endpoints")
            if endpoints:
                filt = {k: v for k, v in ENDPOINTS.items() if v in endpoints or k in endpoints}
                snap = {**snap, "filtered_endpoints": filt}
            return json.dumps(snap, indent=2)
        except Exception as exc:
            return json.dumps({"error": str(exc), "note": "Set FAL_AI_KEY for live fal.ai pricing."})

    if name == "search_archival_media":
        from media_sources import search_archival

        query = str(args.get("query") or "").strip()
        if not query:
            raise ValueError("query required")
        sources = args.get("sources") or None
        preset = str(args.get("preset") or "").strip()
        limit = int(args.get("limit_per_source") or 8)
        data = search_archival(query, sources=sources, preset=preset, limit_per_source=limit)
        return json.dumps(data, indent=2, ensure_ascii=True)

    if name == "analyze_reference_video":
        from studio_agent import competitor

        url = str(args.get("url") or "").strip()
        if not url:
            raise ValueError("url required")
        telemetry.record_event(
            user_id,
            "reference_video_started",
            {"url": url[:500]},
            session_id=session_id,
        )
        job_id = competitor.start_analysis(
            url,
            scene_threshold=float(args.get("scene_threshold") or 0.3),
            max_frames=int(args.get("max_frames") or 40),
        )
        out = {
            "status": "started",
            "job_id": job_id,
            "kind": "competitor",
            "stages": [s[0] for s in competitor.STAGES],
            "note": (
                "Poll poll_render_job(job_id, kind='competitor'): metadata → download → keyframes → "
                "pacing → audio → complete. Then build_scene_blueprint_from_reference."
            ),
        }
        return json.dumps(out, indent=2)

    if name == "build_scene_blueprint_from_reference":
        from studio_agent import reference_planner

        job_id = str(args.get("job_id") or "").strip()
        topic = str(args.get("topic") or "").strip()
        if not job_id or not topic:
            raise ValueError("job_id and topic required")
        blueprint = reference_planner.build_scene_blueprint(
            job_id,
            topic=topic,
            channel_style=str(args.get("channel_style") or "premium_doc"),
            characters_per_scene=int(args.get("characters_per_scene") or 1),
            visual_brief=str(args.get("visual_brief") or "").strip(),
            target_scene_count=int(args["target_scene_count"]) if args.get("target_scene_count") else None,
        )
        telemetry.record_event(
            user_id,
            "scene_blueprint_built",
            {"job_id": job_id, "topic": topic[:200], "scene_count": len(blueprint.get("scenes") or [])},
            session_id=session_id,
        )
        return json.dumps(blueprint, indent=2, ensure_ascii=False)

    if name == "recommend_video_topics":
        async def _topics():
            from long_form.catalyst_bridge import (
                CHANNEL_KEY_TO_ID,
                assess_channel_growth,
                fetch_channel_snapshot,
                shape_catalyst_insights,
            )
            from studio_analytics_router import _fetch_public_search_videos, _predict_topics

            reg_key = str(args.get("registry_key") or "").strip()
            niche = str(args.get("niche_query") or "").strip()
            days = int(args.get("days") or 30)

            channel_block: dict[str, Any] = {}
            if reg_key:
                ch_id = CHANNEL_KEY_TO_ID.get(reg_key, "")
                if ch_id:
                    rec = fetch_channel_snapshot(ch_id)
                    ins = shape_catalyst_insights(rec)
                    harvest = bool((rec or {}).get("analytics_snapshot"))
                    channel_block = {
                        "registry_key": reg_key,
                        "insights": ins,
                        "growth_playbook": assess_channel_growth(ins, harvest_present=harvest),
                    }

            queries = [niche] if niche else (
                [reg_key.replace("_", " ")] if reg_key else ["YouTube documentary viral 2026"]
            )
            videos: list[dict[str, Any]] = []
            titles: list[str] = []
            for q in queries[:2]:
                batch = await _fetch_public_search_videos(q, days=days, max_results=12, order="viewCount")
                videos.extend(batch)
                titles.extend([str(r.get("title") or "") for r in batch if r.get("title")])

            top_titles = (channel_block.get("insights") or {}).get("top_titles") or []
            channel_titles = [t.get("title", "") for t in top_titles if isinstance(t, dict)]
            predictions = _predict_topics(
                trending_titles=titles,
                channel_titles=channel_titles,
                niche_keywords=queries,
            )
            playbook = channel_block.get("growth_playbook") or {}
            stage = playbook.get("stage", "unknown")
            framing = (
                "You don't need a topic yet — here's your positioning sprint."
                if stage in ("brand_new", "early") and not channel_titles
                else "Double down on what's already working on your channel."
            )
            return {
                "framing_for_creator": framing,
                "channel": channel_block,
                "trending_sample": videos[:15],
                "recommended_topics": predictions[:12],
                "next_actions": (playbook.get("recommended_next_actions") or [])[:5],
                "hardest_steps_reminder": [
                    "Script-writing + story beats (use reference blueprint if you linked a Lume/MrBeast video)",
                    "Packaging: title + thumbnail before you render",
                ],
            }

        result = _run_async(_topics())
        telemetry.record_event(
            user_id,
            "topic_recommendations",
            {"registry_key": str(args.get("registry_key") or ""), "niche": str(args.get("niche_query") or "")[:120]},
            session_id=session_id,
        )
        return result

    if name == "search_music":
        from media_sources import search_music

        query = str(args.get("query") or "").strip()
        if not query:
            raise ValueError("query required")
        data = search_music(
            query,
            limit=int(args.get("limit") or 12),
            instrumental=bool(args.get("instrumental")),
        )
        return json.dumps(data, indent=2, ensure_ascii=True)

    if name == "search_sfx":
        from media_sources import search_sfx

        query = str(args.get("query") or "").strip()
        if not query:
            raise ValueError("query required")
        data = search_sfx(
            query,
            limit=int(args.get("limit") or 12),
            cc0_only=bool(args.get("cc0_only", True)),
        )
        return json.dumps(data, indent=2, ensure_ascii=True)

    if name == "poll_render_job":
        job_id = str(args.get("job_id") or "").strip()
        kind = str(args.get("kind") or "longform").strip().lower()
        if not job_id:
            raise ValueError("job_id required")
        if kind == "competitor":
            from studio_agent import competitor

            return json.dumps(competitor.read_status(job_id), indent=2, ensure_ascii=True)
        if kind == "shortform":
            result_path = (ROOT / SKELETON_OUTPUT / job_id / "result.json").resolve()
            if not result_path.is_file():
                return json.dumps({"job_id": job_id, "status": "running", "kind": kind})
            data = json.loads(result_path.read_text(encoding="utf-8"))
            return json.dumps({"job_id": job_id, "kind": kind, **data}, indent=2)
        from long_form.pipeline import load_state, get_status

        st = load_state(job_id) or {}
        status = get_status(job_id) or {}
        return json.dumps({
            "job_id": job_id,
            "kind": kind,
            "phase": status.get("phase") or st.get("phase"),
            "percent": status.get("percent", st.get("percent")),
            "error": status.get("error") or st.get("error"),
            "awaiting_approval": (status.get("phase") or st.get("phase")) == "awaiting_approval",
        }, indent=2)

    raise ValueError(f"unknown tool: {name}")


def execute_tool_logged(
    name: str,
    arguments: dict[str, Any],
    *,
    user_id: str,
    content_format: str,
    session_id: str | None = None,
) -> str:
    """Wrapper that records telemetry then runs the tool."""
    try:
        result = execute_tool(
            name, arguments, user_id=user_id, content_format=content_format, session_id=session_id
        )
    except Exception as exc:
        telemetry.record_tool_call(
            user_id, name, arguments, session_id=session_id, result_preview=f"error: {exc}"
        )
        raise
    telemetry.record_tool_call(user_id, name, arguments, session_id=session_id, result_preview=result[:800])
    return result


def requires_approval(name: str) -> bool:
    return name in APPROVAL_REQUIRED
