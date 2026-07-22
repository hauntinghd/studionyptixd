from __future__ import annotations

import asyncio
import inspect
from pathlib import Path

import pytest
from fastapi import HTTPException

import analytics
from backend_clone_handler import build_clone_video_handler


def test_clone_semantic_hooks_use_injected_completion_without_provider_io(monkeypatch) -> None:
    calls: list[dict] = []

    class NoNetwork:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("clone semantic hooks attempted direct provider I/O")

    async def completion(system_prompt, user_prompt, *, temperature, timeout_sec):
        calls.append(
            {
                "system": system_prompt,
                "user": user_prompt,
                "temperature": temperature,
                "timeout_sec": timeout_sec,
            }
        )
        if system_prompt == "analyze clone":
            return {
                "detected_template": "story",
                "viral_analysis": {
                    "hook_type": "question",
                    "pacing": "fast",
                    "avg_scene_duration": 3.0,
                    "scene_count": 1,
                    "tone": "direct",
                    "retention_tricks": ["open loop"],
                    "what_made_it_viral": "clarity",
                    "follow_up_topic": "follow up",
                },
                "optimized_prompt": "optimized",
            }
        return {
            "title": "Clone",
            "description": "Direct rewrite",
            "tags": ["clone"],
            "scenes": [
                {
                    "narration": "One clean beat.",
                    "visual_description": "A single cinematic reveal.",
                    "duration_sec": 3,
                }
            ],
        }

    monkeypatch.setattr(analytics.httpx, "AsyncClient", NoNetwork)
    analytics.configure_clone_analysis_hooks(
        json_completion=completion,
        clone_analysis_prompt="analyze clone",
        template_system_prompts={"story": "write clone"},
        heuristic_clone_analysis_fn=lambda *_args: {
            "detected_template": "story",
            "viral_analysis": {},
            "optimized_prompt": "",
        },
    )

    analysis = asyncio.run(analytics.analyze_viral_video("topic", "source"))
    script = asyncio.run(
        analytics.generate_clone_script("story", "topic", dict(analysis["viral_analysis"]))
    )

    assert analysis["viral_analysis"]["hook_type"] == "question"
    assert script["scenes"][0]["narration"] == "One clean beat."
    assert [call["temperature"] for call in calls] == [0.6, 0.7]
    assert "api.x.ai" not in inspect.getsource(analytics.analyze_viral_video)
    assert "api.x.ai" not in inspect.getsource(analytics.generate_clone_script)


def test_clone_provider_preflight_runs_after_auth_but_before_credit_reservation(tmp_path: Path) -> None:
    events: list[str] = []

    async def current_user(_request):
        events.append("auth")
        return {"id": "user-1", "plan": "creator"}

    async def reserve(*_args, **_kwargs):
        events.append("reserve")
        return True, "monthly", {"month_key": "2026-07"}

    handler = build_clone_video_handler(
        get_current_user_from_request=current_user,
        user_has_paid_access=lambda _user: True,
        normalize_output_resolution=lambda value, priority_allowed=False: value,
        normalize_external_source_url=lambda value: value,
        temp_dir=tmp_path,
        jobs_ref={},
        enqueue_generation_job=lambda *_args, **_kwargs: asyncio.sleep(0),
        queue_full_error=RuntimeError,
        run_clone_pipeline=lambda *_args, **_kwargs: None,
        persist_job_state=lambda *_args, **_kwargs: asyncio.sleep(0),
        resolve_user_plan_for_limits=lambda _user: ("creator", {}),
        billing_active_for_user=lambda _user: True,
        is_admin_user=lambda _user: False,
        reserve_generation_credit=reserve,
        refund_generation_credit=lambda *_args, **_kwargs: asyncio.sleep(0),
        clone_credit_cost=20,
        clone_providers_ready=lambda: False,
        xai_api_key="stale-and-ignored",
        elevenlabs_api_key="stale-and-ignored",
    )

    with pytest.raises(HTTPException) as exc:
        asyncio.run(handler(topic="test", request=object()))

    assert exc.value.status_code == 503
    assert events == ["auth"]


def test_clone_pipeline_dispatch_is_explicit_fal_only() -> None:
    source = Path("backend.py").read_text(encoding="utf-8")
    start = source.index("async def run_clone_pipeline(")
    end = source.index("\n_clone_video = build_clone_video_handler(", start)
    clone_source = source[start:end]

    assert "selected_model_id=DEFAULT_CREATIVE_IMAGE_MODEL_ID" in clone_source
    assert "video_model_id=DEFAULT_CREATIVE_VIDEO_MODEL_ID" in clone_source
    assert "_transcribe_clone_audio_fal(" in clone_source
    assert "_generate_clone_voiceover_fal(" in clone_source
    assert "RUNWAY_API_KEY" not in clone_source
    assert "generate_voiceover(" not in clone_source
    assert "transcribe_audio_with_grok(" not in clone_source
