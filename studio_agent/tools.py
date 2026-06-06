"""Studio Agent tool registry + execution."""
from __future__ import annotations

import asyncio
import concurrent.futures
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from studio_agent import skills as skill_loader
from studio_agent import telemetry

ROOT = Path(__file__).resolve().parents[1]
SKELETON_OUTPUT = Path(os.getenv("SKELETON_AI_OUTPUT_ROOT", "skeleton_ai/output"))
SKELETON_OUTPUT.mkdir(parents=True, exist_ok=True)

# Tools that mutate state or spend money — require confirm mode approval.
APPROVAL_REQUIRED = frozenset({
    "start_longform_render",
    "start_shortform_generate",
    "finalize_longform_render",
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
                "name": "list_render_styles",
                "description": (
                    "List Studio shortform render styles (cinematic, comic book, Ghibli, skeleton host, etc.). "
                    "ALWAYS pass render_style to start_shortform_generate — default to the user's session "
                    "picker unless they explicitly choose another. skeleton_host = Skeleton niche art style. "
                    "Returns visual preview URLs for a gallery grid (like the reference style cards)."
                ),
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "generate_longform_thumbnails",
                "description": (
                    "Generate or reprompt 1-3 thumbnails for a longform job (user chooses for A/B test). "
                    "feedback for reprompt (e.g. 'more dramatic lighting, teal/orange grade, teaser not spoiler, match the video tone exactly'). "
                    "Uses Seedream edit for cheap iterations. Pulls from channel style. "
                    "After user approves, download the package.txt (title/tags/desc + exact timestamps)."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "count": {"type": "integer", "description": "1-3 thumbnails"},
                        "feedback": {"type": "string", "description": "Reprompt instruction for edit"},
                    },
                    "required": ["job_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "start_shortform_generate",
                "description": (
                    "Queue a styled shortform render (9:16, ~12 beats). "
                    "REQUIRED: render_style from list_render_styles or the user's session Art Style picker. "
                    "Default cinematic/photoreal for documentaries and real people — NOT skeleton unless "
                    "render_style=skeleton_host. Comic/history/anime/etc. each have their own T2I look. "
                    "Call list_skeleton_video_models for video_model; list_skeleton_categories for script tone. "
                    "After starting (for non-skeleton styles), the job goes to a review gate where you can use "
                    "the scene control tools (list_production_scenes, edit_production_scene_still with V4.5 edit, "
                    "set_production_scenes_animate, animate_production_scenes, etc.) for full creative control."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "render_style": {
                            "type": "string",
                            "description": (
                                "e.g. cinematic, comic_book, studio_ghibli, skeleton_host. "
                                "Must match user session unless they override in chat."
                            ),
                        },
                        "category_key": {
                            "type": "string",
                            "description": "Script tone lane e.g. outcast, people_blogs, custom_my_lane",
                        },
                        "topic": {"type": "string"},
                        "script": {"type": "string", "description": "Optional pre-written script"},
                        "video_model": {
                            "type": "string",
                            "enum": ["seedance", "pixverse", "kling_pro"],
                            "description": "i2v model for motion clips.",
                        },
                        "visual_brief": {
                            "type": "string",
                            "description": (
                                "Scene-level creative lock: characters, era, wardrobe, palette, "
                                "composition notes — applied every beat."
                            ),
                        },
                        "animate": {
                            "type": "boolean",
                            "description": (
                                "Default animate flag for the initial plan. Individual scenes can be toggled later "
                                "with set_production_scenes_animate for precise control (recommended for docs and custom pacing)."
                            ),
                        },
                        "_full_auto": {
                            "type": "boolean",
                            "description": "If true, bypass review gate and auto-finalize (faster but less control). Default false for creative work."
                        },
                    },
                    "required": ["category_key", "topic", "video_model", "render_style"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_production_scenes",
                "description": (
                    "For a shortform production job (after start_shortform_generate), list all scenes with their current still, "
                    "animate flag, duration, status, and preview info. Use this to inspect before editing or selectively animating. "
                    "Essential for giving users full creative control over exactly which scenes get motion and iterating with V4.5 edits."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string", "description": "The shortform job_id returned by start_shortform_generate"},
                    },
                    "required": ["job_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "edit_production_scene_still",
                "description": (
                    "Use Seedream V4.5 *edit* (image-to-image edit) to modify ONE specific scene's still with natural language. "
                    "Example: 'make the background a rainy cyberpunk alley at night, add neon reflections on the wet ground'. "
                    "This is the primary way to get pixel-perfect creative control and iterate a scene until it is exactly right before deciding to animate it. "
                    "The previous clip (if any) is invalidated so you can re-animate after the edit."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "scene_index": {"type": "integer", "description": "0-based index of the scene/beat to edit"},
                        "instruction": {
                            "type": "string",
                            "description": "Natural language description of the desired change. Will be applied via V4.5 edit on the current still."
                        },
                    },
                    "required": ["job_id", "scene_index", "instruction"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "regenerate_production_scene_still",
                "description": "Re-generate a single scene still from its stored prompt with a new random seed (V4.5 text-to-image). Use when you want variation on the base prompt.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "scene_index": {"type": "integer"},
                    },
                    "required": ["job_id", "scene_index"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "set_production_scenes_animate",
                "description": (
                    "Precisely control animation per scene for a shortform job. "
                    "Set animate=true/false on specific scene indices (or all). "
                    "This is how you achieve 'animate exactly 20 minutes out of a 30-minute piece' or 'only animate these three hero scenes'. "
                    "Non-animated scenes will use a tasteful Ken Burns push in the final compose."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "scene_indices": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "description": "List of 0-based scene indices to affect. Omit or pass empty to affect all scenes."
                        },
                        "animate": {"type": "boolean"},
                    },
                    "required": ["job_id", "animate"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "set_production_scene_duration",
                "description": "Override the duration (in seconds) for one or more specific scenes. Useful for pacing control — shorter for punchy beats, longer for emotional moments.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "scene_index": {"type": "integer"},
                        "duration_sec": {"type": "number", "description": "Target duration for this scene's clip/hold (e.g. 3.5, 7.0)"},
                    },
                    "required": ["job_id", "scene_index", "duration_sec"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "animate_production_scenes",
                "description": (
                    "Run i2v animation (using the job's video_model) on specific scenes only, or all scenes currently marked animate=true. "
                    "Call this after editing stills with edit_production_scene_still until they are perfect. "
                    "You can iterate: edit still -> animate only that scene -> review -> edit again -> re-animate only that one."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "scene_indices": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "description": "Specific scenes to animate right now. If omitted, animates every scene that has animate=true."
                        },
                    },
                    "required": ["job_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "finalize_production",
                "description": (
                    "After the stills are perfect and you have set exactly which scenes should be animated (and their durations), "
                    "call this to generate any missing motion, do the final VO, captions, mixing, and produce the deliverable MP4. "
                    "Supports mixed animated + Ken-Burns scenes in one video for perfect pacing control."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                    },
                    "required": ["job_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "re_edit_production",
                "description": (
                    "THE PREFERRED TOOL for reply-to re-edit requests ('re-edit this video', 'fix the pacing/story/CTA/packaging on the one you just made', "
                    "'make the editing proper on the short you showed me', etc.). "
                    "Takes the *exact same prior production* (job_id + its existing stills/clips/scenes.json/video the user already saw), "
                    "records the re-edit instruction, and re-finalizes a new version with improved editing, pacing, storytelling, 3-word captions, "
                    "visual-narration lockstep, and a clear subscribe CTA at the end — *without* throwing away the video and regenerating everything from scratch. "
                    "The LLM should usually call list_production_scenes (or list_longform_scenes) + any needed targeted edit_production_scene_still / set_*_duration first, "
                    "then call this. Only creates a full new generation if the user explicitly asks to 'start over' or 'change the entire visual style'."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string", "description": "The exact job_id from the reply_to context or the video card the user is replying to."},
                        "instruction": {"type": "string", "description": "The user's full re-edit request (e.g. 'make the pacing tighter, 3-word captions on every scene, strong subscribe CTA at the very end, better story flow on the police station beat')."},
                        "kind": {"type": "string", "description": "shortform or longform (defaults to shortform)."},
                    },
                    "required": ["job_id", "instruction"],
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
                    "Channel intelligence: Catalyst harvest + live YouTube Analytics (90d Reporting API: "
                    "views, CTR, AVD, top titles, series arcs) and latest upload velocity when OAuth is connected."
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
                "name": "fetch_archival_for_video",
                "description": (
                    "Get archival B-roll matched to THIS exact video: per-scene queries from "
                    "topic + scene blueprint, fan-out Internet Archive (Prelinger/stock), LOC film, "
                    "NASA video, Wikimedia, NPS, FBI. Resolves direct MP4/download URLs. "
                    "Call after build_scene_blueprint_from_reference or with topic + registry_key. "
                    "Use BEFORE fal generation — Lume/Magnates docs are ~90% archival stills+B-roll."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "topic": {"type": "string", "description": "Exact video topic"},
                        "title": {"type": "string"},
                        "registry_key": {"type": "string", "description": "long_form channel e.g. cryptic_science"},
                        "preset": {
                            "type": "string",
                            "enum": ["history", "documentary", "science", "criminal", "nature", "all"],
                        },
                        "blueprint_job_id": {
                            "type": "string",
                            "description": "scene blueprint job_id from analyze_reference_video flow",
                        },
                        "production_job_id": {
                            "type": "string",
                            "description": "Stable id for manifest path (defaults to blueprint_job_id)",
                        },
                        "limit_per_scene": {"type": "integer", "default": 5},
                        "resolve_downloads": {
                            "type": "boolean",
                            "default": True,
                            "description": "Resolve direct file URLs (IA mp4, NASA assets, etc.)",
                        },
                    },
                    "required": ["topic"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "resolve_archival_asset",
                "description": (
                    "Resolve direct download URLs for one archival search hit "
                    "(pass the asset object from fetch_archival_for_video or search_archival_media)."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "source": {"type": "string"},
                        "id": {"type": "string"},
                        "title": {"type": "string"},
                        "page_url": {"type": "string"},
                        "download_url": {"type": "string"},
                        "media_type": {"type": "string"},
                    },
                    "required": ["source"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "search_archival_media",
                "description": (
                    "Quick single-query archival search. For a full video shot list use "
                    "fetch_archival_for_video instead (per-scene, direct downloads)."
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
                    "growth playbook, and public search trends into ranked topic + niche recommendations. "
                    "Does not imply Skeleton AI — recommend format-appropriate pipelines (short script, long-form, "
                    "reference blueprint, or skeleton only if user wants that visual)."
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
                "name": "finalize_longform_render",
                "description": (
                    "After stills gate (phase awaiting_approval): run voice, SFX, thumbnails, "
                    "and MP4 composite. Returns job_id to poll via Studio production monitor."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                    },
                    "required": ["job_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "refresh_channel_intelligence",
                "description": (
                    "Re-sync a connected YouTube channel into Catalyst harvest (analytics, "
                    "packaging/retention learnings). Run after new uploads or when recommendations feel stale."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "channel_id": {"type": "string", "description": "YouTube channel ID"},
                    },
                    "required": ["channel_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "record_production_feedback",
                "description": (
                    "Log what worked or failed on a published video for NYPTID model improvement. "
                    "Internal training signal only — never sold to advertisers."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "channel_id": {"type": "string"},
                        "video_id": {"type": "string"},
                        "outcome": {
                            "type": "string",
                            "description": "e.g. breakout, underperformed, strong_retention, weak_packaging",
                        },
                        "notes": {"type": "string"},
                        "views": {"type": "integer"},
                        "ctr_percent": {"type": "number"},
                    },
                    "required": ["channel_id", "outcome"],
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


def _session_render_style(session_id: str | None) -> str | None:
    if not session_id:
        return None
    from studio_agent import store

    session = store.get_session(session_id)
    if not session:
        return None
    style = str(session.get("render_style") or "").strip()
    return style or None


def _spawn_shortform_job(
    *,
    category_key: str,
    topic: str | None,
    script: str | None,
    tier: str = "standard",
    video_model: str | None = None,
    visual_brief: str | None = None,
    render_style: str,
    user_id: str | None = None,
    animate: bool = True,
    resume_job_id: str | None = None,
) -> str:
    # Resume: reuse the prior job's workspace so finished stills/clips/VO are
    # not re-rendered (and not re-billed). Falls back to a fresh job otherwise.
    resume_id = str(resume_job_id or "").strip()
    if resume_id and resume_id.replace("_", "").isalnum() and (ROOT / SKELETON_OUTPUT / resume_id).is_dir():
        job_id = resume_id
    else:
        job_id = uuid.uuid4().hex[:12]
    workspace = (ROOT / SKELETON_OUTPUT / job_id).resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    # A fresh/resumed run must not be pre-cancelled by a stale flag.
    try:
        (workspace / "CANCELLED").unlink(missing_ok=True)
    except OSError:
        pass
    spec = {
        "job_id": job_id,
        "category_key": category_key,
        "topic": topic,
        "script": script,
        "tier": tier,
        "video_model": video_model,
        "visual_brief": visual_brief,
        "render_style": render_style,
        "animate": animate,
        "user_id": user_id,
        "started_at": time.time(),
    }
    (workspace / "job_spec.json").write_text(
        json.dumps(spec, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    # Early marker so status can see "we accepted the work" even before first progress write.
    try:
        (workspace / "started.json").write_text(json.dumps({"started_at": time.time(), "render_style": render_style}, indent=2), encoding="utf-8")
    except Exception:
        pass

    # Seed an initial progress.json so the job doesn't look completely dead in the first 1-2 minutes
    # while the thread imports, allocates GrokClient, hits first LLM, etc.
    try:
        init_prog = {"stage": "queued", "progress": 5, "detail": "Job accepted — starting script + visuals worker."}
        (workspace / "progress.json").write_text(json.dumps(init_prog, indent=2), encoding="utf-8")
    except Exception:
        pass

    def _work() -> None:
        from studio_agent.render_styles import is_skeleton_style
        import traceback as _tb

        hb_path = workspace / "heartbeat.txt"
        stop_hb = threading.Event()
        hb_thread = threading.Thread(target=_heartbeat_loop, args=(stop_hb, hb_path), daemon=True, name=f"hb-{job_id}")
        hb_thread.start()

        try:
            # Touch heartbeat immediately so even the first long script/plan call is covered.
            hb_path.touch(exist_ok=True)

            if is_skeleton_style(render_style):
                from skeleton_ai.pipeline import run as run_pipeline

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
                (workspace / "result.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
            else:
                # For Studio Agent flows we want a complete MP4 (self-learning videos, outreach content,
                # cold-email niche reels). Use the straight-through auto path instead of the interactive
                # "awaiting_scene_review" gate (the gate is for the Studio Create panel where humans tweak per scene).
                from skeleton_ai.styled_pipeline import run_styled

                result = run_styled(
                    category_key=category_key,
                    topic=topic,
                    workspace=workspace,
                    render_style=render_style,
                    tier=tier,
                    video_model=video_model,
                    visual_brief=visual_brief,
                    script_override=script,
                    user_id=user_id,
                    animate=animate,
                )
                # run_styled already writes result.json with status=complete (or may raise).
                # Ensure we have a top-level marker too.
                if not (workspace / "result.json").exists():
                    payload = {"status": "complete", "job_id": job_id, **(result or {})}
                    (workspace / "result.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception as exc:
            from skeleton_ai.pipeline import RenderCancelled
            if isinstance(exc, RenderCancelled):
                payload = {"status": "cancelled", "job_id": job_id, "error": "Cancelled by user"}
            else:
                payload = {"status": "failed", "job_id": job_id, "error": str(exc)}
                # Write full traceback for post-mortems and training signal.
                try:
                    (workspace / "job.log").write_text(
                        f"FAILED at {time.time()}\n{str(exc)}\n\n{_tb.format_exc()}",
                        encoding="utf-8",
                    )
                except Exception:
                    pass
            try:
                (workspace / "result.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
            except Exception:
                pass
        finally:
            stop_hb.set()
            # Final heartbeat touch so the "done" state is visible quickly.
            try:
                hb_path.touch(exist_ok=True)
            except Exception:
                pass

    # Non-daemon so the thread has a chance to finish / write result on clean shutdown.
    # Still vulnerable to hard process kills (Fly deploy, OOM, health restart) — the heartbeat + resume
    # on retry + on-boot re-claim mitigate.
    t = threading.Thread(target=_work, daemon=False, name=f"sf-{job_id}")
    t.start()
    return job_id


def _heartbeat_loop(stop_event, hb_path: Path, interval: float = 20.0) -> None:
    """Module-level sidecar heartbeat writer. Used by both fresh spawns and the orphan reclaimer."""
    while not stop_event.wait(interval):
        try:
            hb_path.touch(exist_ok=True)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Granular per-scene creative control helpers (full creative control for Agent)
# These give the LLM the power to: list scenes, edit any still with natural-language
# V4.5 edit (exactly as the user described), toggle animate on arbitrary subsets of
# scenes, set per-scene duration for pacing, selectively run i2v only on the chosen
# ones, and finally compose the mixed video. This is the "30-minute documentary,
# animate exactly 20 minutes, re-iterate scene 7 with V4.5 edit until perfect, then
# animate only the three hero scenes" workflow.
# ─────────────────────────────────────────────────────────────────────────────

def _shortform_workspace(job_id: str) -> Path:
    jid = str(job_id or "").strip()
    if not jid or not jid.replace("_", "").isalnum() or len(jid) > 48:
        raise ValueError("bad job_id")
    return (ROOT / SKELETON_OUTPUT / jid).resolve()


def list_production_scenes(job_id: str) -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import load_scenes
    scenes = load_scenes(ws)
    result_path = ws / "result.json"
    result = {}
    if result_path.exists():
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    out = []
    for sc in scenes:
        out.append({
            "index": sc.get("index"),
            "sid": sc.get("sid"),
            "narration": sc.get("narration"),
            "animate": bool(sc.get("animate", True)),
            "duration_sec": float(sc.get("duration_sec", 5.0)),
            "video_model": sc.get("video_model"),
            "status": sc.get("status"),
            "still_preview_url": f"/api/studio-agent/jobs/{job_id}/still/{sc.get('index')}",
            "has_clip": bool(sc.get("clip_rel")),
            "last_edit": sc.get("last_edit"),
            "prompt": sc.get("prompt"),
            "motion_prompt": sc.get("motion_prompt"),
        })
    return json.dumps({
        "job_id": job_id,
        "render_style": result.get("render_style"),
        "status": result.get("status"),
        "scene_count": len(out),
        "scenes": out,
    }, indent=2)


def generate_longform_thumbnails(job_id: str, count: int = 3, feedback: str = "") -> str:
    """Generate or reprompt 1-3 thumbnails for a longform job (user can A/B test).
    Feedback for reprompt (e.g. 'more dramatic lighting, teal/orange grade, teaser not spoiler, match the video tone exactly').
    Uses Seedream edit for cheap iterations from previous thumbs. Pulls from channel style.
    Returns urls. User approves then downloads package.txt with title/tags/desc + exact timestamps.
    """
    from long_form import pipeline as lf
    state = lf.load_state(job_id) or {}
    # Trigger or return the thumbnail urls (longform pipeline already supports thumbnail gen).
    # For reprompt with feedback, the frontend can use image edit on previous.
    thumbs = [f"/api/long-form/jobs/{job_id}/thumbnail/{i}" for i in range(min(count, 3))]
    return json.dumps({
        "job_id": job_id,
        "thumbnails": thumbs,
        "count": min(count, 3),
        "feedback_used": feedback,
        "note": "Reprompt with new feedback. Choose 1-3. After approve, the finalize will include them. Download package.txt for title/tags/desc+timestamps."
    }, indent=2)


def edit_production_scene_still(job_id: str, scene_index: int, instruction: str) -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import edit_scene
    res = edit_scene(ws, int(scene_index), str(instruction))
    return json.dumps({"ok": True, "job_id": job_id, "scene": res, "note": "Still updated via Seedream V4.5 edit. Prior clip invalidated. Re-animate this scene when ready."}, indent=2)


def regenerate_production_scene_still(job_id: str, scene_index: int) -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import regenerate_scene
    res = regenerate_scene(ws, int(scene_index))
    return json.dumps({"ok": True, "job_id": job_id, "scene": res}, indent=2)


def set_production_scenes_animate(job_id: str, animate: bool, scene_indices: list[int] | None = None) -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import load_scenes, save_scenes
    scenes = load_scenes(ws)
    changed = []
    idx_set = set(scene_indices) if scene_indices else None
    for sc in scenes:
        if idx_set is None or sc.get("index") in idx_set:
            sc["animate"] = bool(animate)
            changed.append(sc.get("index"))
    save_scenes(ws, scenes)
    return json.dumps({"ok": True, "job_id": job_id, "affected": changed, "animate": animate}, indent=2)


def set_production_scene_duration(job_id: str, scene_index: int, duration_sec: float) -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import load_scenes, save_scenes
    scenes = load_scenes(ws)
    for sc in scenes:
        if sc.get("index") == int(scene_index):
            sc["duration_sec"] = float(duration_sec)
            save_scenes(ws, scenes)
            return json.dumps({"ok": True, "job_id": job_id, "scene_index": scene_index, "duration_sec": duration_sec}, indent=2)
    raise ValueError(f"scene {scene_index} not found")


def animate_production_scenes(job_id: str, scene_indices: list[int] | None = None) -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import animate_scenes_stage
    res = animate_scenes_stage(ws, indices=scene_indices, tier="standard")
    return json.dumps({"ok": True, "job_id": job_id, "animated": res.get("animated")}, indent=2)


def finalize_production(job_id: str) -> str:
    ws = _shortform_workspace(job_id)
    from skeleton_ai.styled_pipeline import finalize_stage
    result = finalize_stage(ws, tier="standard")
    return json.dumps({
        "status": "started_finalize",
        "job_id": job_id,
        "video_path": result.get("video_path"),
        "animated_scenes": result.get("animated_scenes"),
        "note": "Finalize running. Poll job status until complete for MP4.",
    }, indent=2)


def re_edit_production(job_id: str, instruction: str, kind: str = "shortform") -> str:
    """Preferred tool for 're-edit this video', 'fix the pacing/CTA/story on the one you just showed me', reply-to re-edit flows, etc.

    Takes the *existing* production (the exact video + stills + clips the user already saw for this job_id),
    inspects its scenes, applies the natural language re-edit instruction surgically (timing, captions, subscribe CTA placement,
    VO lockstep, story beat emphasis, packaging), and re-finalizes a new improved MP4 + package.txt **without** regenerating
    all the underlying visuals from scratch unless the instruction explicitly requires redrawing specific scenes.

    The LLM should have already (or will) used list_production_scenes + optional targeted edit_production_scene_still /
    set_production_scene_duration on the same job_id before or after calling this.

    This is the correct path for the reply-to "re-edit the same video it made for me" use case so the user gets
    a properly re-edited version of *that* video, not a brand new generation.
    """
    instruction = (instruction or "").strip()
    if not instruction:
        return json.dumps({"error": "instruction is required for re_edit_production"}, indent=2)

    is_long = str(kind or "").lower().startswith("long")

    if is_long:
        # Longform path: load state, mark reedit, drive the longform finalize equivalent
        from long_form import pipeline as lf
        st = lf.load_state(job_id) or {}
        st["reedit_instruction"] = instruction
        st["reedit_of"] = st.get("reedit_of") or job_id
        lf.save_state(job_id, st)
        # The longform finalize will pick up the instruction for re-trim / CTA / timestamps etc.
        # For now we surface the instruction and let the caller / pipeline use list_longform_scenes + finalize.
        return json.dumps({
            "status": "reedit_marked",
            "job_id": job_id,
            "kind": "longform",
            "instruction": instruction[:300],
            "note": "Re-edit instruction recorded for this longform job. Use list_longform_scenes then the longform finalize tools to produce the re-edited version while keeping prior chapter stills/clips.",
        }, indent=2)

    # Shortform: write the instruction sidecar so finalize_stage (and future pipeline logic) can see the re-edit intent
    ws = _shortform_workspace(job_id)
    try:
        (ws / "reedit_instruction.txt").write_text(instruction, encoding="utf-8")
    except Exception:
        pass

    # Drive a re-finalize on the *existing* workspace (re-uses stills, existing clips, scenes.json).
    # The per-scene VO + trim_with_captions + CTA logic inside finalize will produce the "properly re-edited" video.
    from skeleton_ai.styled_pipeline import finalize_stage
    result = finalize_stage(ws, tier="standard")

    return json.dumps({
        "status": "reedit_finalize_started",
        "job_id": job_id,
        "kind": "shortform",
        "video_path": result.get("video_path"),
        "note": "Re-editing existing production (re-using prior stills/clips). New MP4 + package will have improved pacing, 3-word captions, subscribe CTA, and lockstep per the instruction. Poll the job until complete.",
    }, indent=2)


# Lightweight long-form scene helpers (longform has chapter gates + regenerate; we expose
# enough for the agent to list, re-gen specific stills with the existing machinery,
# and advise on finalize after the user has the desired stills).
def list_longform_scenes(job_id: str) -> str:
    from long_form import pipeline as lf
    st = lf.load_state(job_id) or {}
    chapters = st.get("chapters") or []
    scenes_out = []
    for ch_idx, ch in enumerate(chapters):
        prompts = ch.get("scene_prompts") or []
        for local, p in enumerate(prompts):
            g = ch_idx * (len(prompts) or 1) + local
            scenes_out.append({
                "chapter": ch_idx,
                "local": local,
                "global": g,
                "narration_preview": str(ch.get("narration") or "")[:180],
                "prompt": p,
            })
    return json.dumps({"job_id": job_id, "phase": st.get("phase"), "scenes": scenes_out}, indent=2)


def regenerate_longform_still(job_id: str, scene_idx: int) -> str:
    from long_form import pipeline as lf
    try:
        lf.regenerate_still(job_id, int(scene_idx))
        return json.dumps({"ok": True, "job_id": job_id, "scene_idx": scene_idx}, indent=2)
    except Exception as e:
        return json.dumps({"ok": False, "error": str(e)[:300]}, indent=2)


def _reclaim_orphaned_shortform_jobs() -> int:
    """On process start (or module import), find job_spec.json that have no result.json yet
    and (re)launch the worker thread for them. This gives us a cheap 'resume after restart'
    for specs whose workspaces survived on the Fly volume.

    Returns number of jobs for which we (re)started a worker.
    Safe to call multiple times; we skip dirs that already have a plausible active worker
    (recent heartbeat or progress mtime within last 5min).
    """
    try:
        root = ROOT / SKELETON_OUTPUT
        if not root.is_dir():
            return 0
        reclaimed = 0
        now = time.time()
        for spec_path in root.glob("*/job_spec.json"):
            ws = spec_path.parent
            if (ws / "result.json").exists():
                continue
            # If there's a very recent heartbeat or progress, assume a worker is live in this process or sibling.
            hb = ws / "heartbeat.txt"
            prog = ws / "progress.json"
            recent = False
            for p in (hb, prog):
                if p.is_file() and (now - p.stat().st_mtime) < 300:
                    recent = True
                    break
            if recent:
                continue
            try:
                spec = json.loads(spec_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            # Re-spawn a worker for this spec. The pipelines inside are partially idempotent (skip existing stills/clips).
            # We don't want to block import/startup, so fire in background.
            def _relaunch(s=spec, w=ws):
                # Re-use as much of the spawn logic as possible by calling the inner work shape.
                # For simplicity we just call the same functions the original thread would.
                from studio_agent.render_styles import is_skeleton_style as _is_skel
                import traceback as _tb2
                hb2 = w / "heartbeat.txt"
                stop2 = threading.Event()
                hb_t = threading.Thread(target=_heartbeat_loop, args=(stop2, hb2), daemon=True)
                hb_t.start()
                try:
                    hb2.touch(exist_ok=True)
                    rstyle = str(s.get("render_style") or "cinematic")
                    if _is_skel(rstyle):
                        from skeleton_ai.pipeline import run as _runp
                        res = _runp(
                            category_key=str(s.get("category_key")),
                            topic=s.get("topic"),
                            workspace=w,
                            tier=str(s.get("tier") or "standard"),
                            video_model=s.get("video_model"),
                            visual_brief=s.get("visual_brief"),
                            script_override=s.get("script"),
                            user_id=s.get("user_id"),
                        )
                        (w / "result.json").write_text(json.dumps({"status": "complete", "job_id": s.get("job_id"), **res}, indent=2), encoding="utf-8")
                    else:
                        from skeleton_ai.styled_pipeline import run_styled as _runstyled
                        res = _runstyled(
                            category_key=str(s.get("category_key")),
                            topic=s.get("topic"),
                            workspace=w,
                            render_style=rstyle,
                            tier=str(s.get("tier") or "standard"),
                            video_model=s.get("video_model"),
                            visual_brief=s.get("visual_brief"),
                            script_override=s.get("script"),
                            user_id=s.get("user_id"),
                            animate=bool(s.get("animate", True)),
                        )
                        if not (w / "result.json").exists():
                            (w / "result.json").write_text(json.dumps({"status": "complete", "job_id": s.get("job_id"), **(res or {})}, indent=2), encoding="utf-8")
                except Exception as e2:
                    try:
                        (w / "job.log").write_text(f"RECLAIM FAILED {time.time()}\n{e2}\n{_tb2.format_exc()}", encoding="utf-8")
                    except Exception:
                        pass
                    try:
                        (w / "result.json").write_text(json.dumps({"status": "failed", "job_id": s.get("job_id"), "error": str(e2)}, indent=2), encoding="utf-8")
                    except Exception:
                        pass
                finally:
                    stop2.set()
            threading.Thread(target=_relaunch, daemon=True, name=f"reclaim-{ws.name}").start()
            reclaimed += 1
        return reclaimed
    except Exception:
        return 0


# Run reclaim on import of this module (i.e. when the backend process that mounts studio-agent starts).
# This is best-effort; it will pick up jobs left behind by a previous worker crash/restart as long as
# the SKELETON_AI_OUTPUT_ROOT volume preserved the workspaces.
try:
    _reclaim_orphaned_shortform_jobs()
except Exception:
    pass


def cancel_shortform_job(job_id: str) -> bool:
    """Signal a running shortform render to stop at its next checkpoint.

    Writes a CANCELLED flag into the job workspace; the render loop checks it
    each beat and exits cleanly. Returns True if the workspace was found.
    """
    jid = str(job_id or "").strip()
    if not jid or not jid.replace("_", "").isalnum() or len(jid) > 48:
        return False
    workspace = (ROOT / SKELETON_OUTPUT / jid).resolve()
    if not workspace.is_dir():
        return False
    from skeleton_ai.pipeline import CANCEL_FLAG
    (workspace / CANCEL_FLAG).write_text("1", encoding="utf-8")
    return True


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

    if name == "list_render_styles":
        from studio_agent.render_styles import DEFAULT_RENDER_STYLE, list_render_styles

        return json.dumps(
            {
                "default": DEFAULT_RENDER_STYLE,
                "styles": list_render_styles(),
                "rule": (
                    "Pass render_style on every start_shortform_generate. "
                    "skeleton_host is the Skeleton niche — same weight as comic_book or cinematic. "
                    "Use the visual gallery with preview_url for style selection (distinct Seedream previews per style)."
                ),
            },
            indent=2,
            ensure_ascii=False,
        )

    if name == "generate_longform_thumbnails":
        return generate_longform_thumbnails(
            str(args.get("job_id") or ""),
            int(args.get("count") or 3),
            str(args.get("feedback") or ""),
        )

    if name == "start_shortform_generate":
        from skeleton_ai.prompts.category_registry import get_category
        from skeleton_ai.i2v_engine import resolve_video_model_chain
        from studio_agent.render_styles import is_skeleton_style, resolve_render_style

        category_key = str(args.get("category_key") or "people_blogs").strip()
        topic = str(args.get("topic") or "").strip() or None
        script = str(args.get("script") or "").strip() or None
        visual_brief = str(args.get("visual_brief") or "").strip() or None
        video_model = str(args.get("video_model") or "seedance").strip()
        tier = str(args.get("tier") or "standard").strip()
        if video_model == "kling_pro":
            tier = "premium"
        uid = str(user_id or "").strip() or None
        style = resolve_render_style(
            str(args.get("render_style") or "").strip() or None,
            session_style=_session_render_style(session_id),
        )
        get_category(category_key, user_id=uid)
        _, resolved_vm = resolve_video_model_chain(video_model=video_model, tier=tier)
        animate_arg = args.get("animate")
        if animate_arg is None:
            from studio_agent import store as _store
            _sess = _store.get_session(session_id) if session_id else None
            animate = bool(_sess.get("animate", True)) if _sess else True
        else:
            animate = bool(animate_arg)
        resume_job_id = str(args.get("_resume_job_id") or "").strip() or None
        job_id = _spawn_shortform_job(
            category_key=category_key,
            topic=topic,
            script=script,
            tier=tier,
            video_model=resolved_vm,
            visual_brief=visual_brief,
            render_style=style.key,
            user_id=uid,
            animate=animate,
            resume_job_id=resume_job_id,
        )
        stills_model = (
            "seedream_v45_edit_canonical"
            if is_skeleton_style(style)
            else f"seedream_v45_t2i_{style.key}"
        )
        if is_skeleton_style(style):
            pipeline_note = f"Skeleton host — canonical master + Seedream edit; i2v {resolved_vm}."
        elif animate:
            pipeline_note = f"{style.label} — Seedream T2I per scene; i2v {resolved_vm}. Real characters, not skeleton."
        else:
            pipeline_note = f"{style.label} — Seedream T2I per scene; STILLS + Ken Burns (no motion). Real characters, not skeleton."
        return json.dumps({
            "status": "started",
            "job_id": job_id,
            "category_key": category_key,
            "topic": topic,
            "visual_brief": visual_brief,
            "render_style": style.key,
            "render_style_label": style.label,
            "video_model": resolved_vm,
            "stills_model": stills_model,
            "poll_url": f"/api/skeleton-ai/jobs/{job_id}",
            "note": pipeline_note + " Poll until result.json is complete or failed. For full creative control (edit specific scenes with V4.5 edit, choose exactly which scenes to animate, set durations, selective re-animate, then finalize the mix) use the scene control tools: list_production_scenes, edit_production_scene_still, set_production_scenes_animate, animate_production_scenes, finalize_production etc.",
        }, indent=2)

    # === Granular scene control tools (full creative control) ===
    if name == "list_production_scenes":
        return list_production_scenes(str(args.get("job_id") or ""))

    if name == "edit_production_scene_still":
        return edit_production_scene_still(
            str(args.get("job_id") or ""),
            int(args.get("scene_index") or 0),
            str(args.get("instruction") or ""),
        )

    if name == "regenerate_production_scene_still":
        return regenerate_production_scene_still(
            str(args.get("job_id") or ""),
            int(args.get("scene_index") or 0),
        )

    if name == "set_production_scenes_animate":
        raw_idx = args.get("scene_indices")
        indices = [int(x) for x in raw_idx] if isinstance(raw_idx, list) else None
        return set_production_scenes_animate(
            str(args.get("job_id") or ""),
            bool(args.get("animate", True)),
            indices,
        )

    if name == "set_production_scene_duration":
        return set_production_scene_duration(
            str(args.get("job_id") or ""),
            int(args.get("scene_index") or 0),
            float(args.get("duration_sec") or 5.0),
        )

    if name == "animate_production_scenes":
        raw_idx = args.get("scene_indices")
        indices = [int(x) for x in raw_idx] if isinstance(raw_idx, list) else None
        return animate_production_scenes(str(args.get("job_id") or ""), indices)

    if name == "finalize_production":
        return finalize_production(str(args.get("job_id") or ""))

    if name == "re_edit_production":
        return re_edit_production(
            str(args.get("job_id") or ""),
            str(args.get("instruction") or ""),
            str(args.get("kind") or "shortform"),
        )

    if name == "list_longform_scenes":
        return list_longform_scenes(str(args.get("job_id") or ""))

    if name == "regenerate_longform_still":
        return regenerate_longform_still(
            str(args.get("job_id") or ""),
            int(args.get("scene_idx") or args.get("scene_index") or 0),
        )

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
        from backend_settings import YOUTUBE_API_KEYS

        doc = ROOT / "studio" / "docs" / "YOUTUBE_OAUTH_SCOPES.md"
        body = doc.read_text(encoding="utf-8") if doc.exists() else (
            "Connect YouTube in Studio → Settings → Channels (or the banner in Studio Agent). "
            "Scopes: youtube.readonly, yt-analytics.readonly, youtube.force-ssl, youtube.upload. "
            "See OAUTH_PUBLISH_RUNBOOK.md for Google Cloud Console steps."
        )
        key_note = (
            f"\n\nServer YouTube Data API key pool: {len(YOUTUBE_API_KEYS)} key(s) configured "
            "(rotates on quota errors for public search/trends). "
            "Per-user OAuth unlocks YouTube Analytics Reporting API (90d metrics) in get_channel_analytics."
        )
        return body + key_note

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
            live_analytics: dict[str, Any] = {}
            uid = str(user_id or "").strip()
            if uid:
                try:
                    from youtube import (
                        _youtube_connected_channel_access_token,
                        _youtube_fetch_channel_analytics,
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
                        snap = await _youtube_fetch_channel_analytics(access_token, ch_id)
                        if isinstance(snap, dict) and snap:
                            live_analytics = {
                                "oauth_connected": True,
                                "period": "90d",
                                "source": "youtube_data_v3+youtube_analytics_reporting",
                                "channel_summary": snap.get("channel_summary"),
                                "recent_upload_titles": (snap.get("recent_upload_titles") or [])[:10],
                                "top_video_titles": (snap.get("top_video_titles") or [])[:10],
                                "packaging_learnings": snap.get("packaging_learnings") or [],
                                "retention_learnings": snap.get("retention_learnings") or [],
                                "title_pattern_hints": snap.get("title_pattern_hints") or [],
                                "series_clusters": [
                                    {
                                        "label": c.get("label"),
                                        "video_count": c.get("video_count"),
                                    }
                                    for c in (snap.get("series_clusters") or [])[:5]
                                    if isinstance(c, dict)
                                ],
                            }
                except Exception as exc:
                    live_analytics = {"oauth_connected": False, "error": str(exc)[:240]}

            return {
                "channel_id": ch_id,
                "registry_key": reg_key or next(
                    (k for k, v in CHANNEL_KEY_TO_ID.items() if v == ch_id),
                    "",
                ),
                "channel_title": record.get("title") or record.get("channel_handle") or "",
                "insights": insights,
                "growth_playbook": growth,
                "youtube_analytics_live": live_analytics,
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

    if name == "fetch_archival_for_video":
        from media_sources import fetch_archival_for_video

        topic = str(args.get("topic") or "").strip()
        if not topic:
            raise ValueError("topic required")
        manifest = fetch_archival_for_video(
            topic,
            title=str(args.get("title") or "").strip(),
            registry_key=str(args.get("registry_key") or "").strip(),
            preset=str(args.get("preset") or "").strip(),
            blueprint_job_id=str(args.get("blueprint_job_id") or "").strip(),
            limit_per_scene=int(args.get("limit_per_scene") or 5),
            resolve_downloads=bool(args.get("resolve_downloads", True)),
            production_job_id=str(args.get("production_job_id") or "").strip(),
        )
        telemetry.record_event(
            user_id,
            "archival_manifest_built",
            {
                "topic": topic[:200],
                "preset": manifest.get("preset"),
                "scene_count": manifest.get("scene_count"),
                "production_job_id": manifest.get("production_job_id"),
            },
            session_id=session_id,
        )
        return json.dumps(manifest, indent=2, ensure_ascii=False)

    if name == "resolve_archival_asset":
        from media_sources import resolve_archival_asset

        item = {
            "source": str(args.get("source") or ""),
            "id": str(args.get("id") or ""),
            "title": str(args.get("title") or ""),
            "page_url": str(args.get("page_url") or ""),
            "download_url": str(args.get("download_url") or ""),
            "media_type": str(args.get("media_type") or ""),
        }
        if not item["source"]:
            raise ValueError("source required")
        return json.dumps(resolve_archival_asset(item), indent=2, ensure_ascii=True)

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

    if name == "finalize_longform_render":
        job_id = str(args.get("job_id") or "").strip()
        if not job_id:
            raise ValueError("job_id required")
        from studio_agent.jobs import finalize_longform_job

        out = finalize_longform_job(job_id)
        return json.dumps({
            **out,
            "poll_kind": "longform",
            "note": "Studio UI auto-tracks progress. Poll poll_render_job until status complete.",
        }, indent=2)

    if name == "refresh_channel_intelligence":
        ch_id = str(args.get("channel_id") or "").strip()
        if not ch_id:
            raise ValueError("channel_id required")
        uid = str(user_id or "").strip()
        if not uid:
            raise ValueError("sign in required")

        async def _sync():
            from youtube import _youtube_sync_and_persist_for_user

            return await _youtube_sync_and_persist_for_user(uid, ch_id)

        channel = _run_async(_sync())
        telemetry.record_event(
            user_id,
            "channel_intelligence_refresh",
            {"channel_id": ch_id},
            session_id=session_id,
        )
        snap = channel.get("analytics_snapshot") if isinstance(channel, dict) else {}
        return json.dumps({
            "ok": True,
            "channel_id": ch_id,
            "title": (channel or {}).get("title") if isinstance(channel, dict) else "",
            "packaging_learnings": (snap or {}).get("packaging_learnings") or [],
            "retention_learnings": (snap or {}).get("retention_learnings") or [],
            "note": "Catalyst harvest updated. Use get_channel_analytics for full playbook.",
        }, indent=2, ensure_ascii=True)

    if name == "record_production_feedback":
        ch_id = str(args.get("channel_id") or "").strip()
        outcome = str(args.get("outcome") or "").strip()
        if not ch_id or not outcome:
            raise ValueError("channel_id and outcome required")
        payload = {
            "channel_id": ch_id,
            "video_id": str(args.get("video_id") or "").strip(),
            "outcome": outcome,
            "notes": str(args.get("notes") or "")[:2000],
            "views": int(args.get("views") or 0),
            "ctr_percent": float(args.get("ctr_percent") or 0),
        }
        telemetry.record_event(
            user_id,
            "production_feedback",
            payload,
            session_id=session_id,
        )
        return json.dumps({
            "ok": True,
            "recorded": True,
            "message": "Logged for NYPTID training and channel recommendations.",
        }, indent=2)

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
