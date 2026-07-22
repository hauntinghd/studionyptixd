from __future__ import annotations

import json
from pathlib import Path

import pytest

from long_form import pipeline as longform_pipeline
from skeleton_ai import canonical_edit, render_simulation, styled_pipeline
from studio_agent import (
    catalyst_health,
    catalyst_still_audit,
    production_budget,
    production_costs,
    tools,
    visual_qa,
)
from studio_agent.visual_fix_contract import PROMPT_CHAR_BUDGET, artifact_fix_plan


def _pass_report(summary: str = "passed") -> dict:
    return {"status": "pass", "pass": True, "summary": summary, "issues": []}


def test_visual_fix_contract_preserves_two_host_story_and_varies_retry() -> None:
    common = {
        "topic": "Love bombing in a relationship",
        "scene_action": "He overwhelms her with gifts at the apartment doorway.",
        "aspect_ratio": "16:9",
    }
    first = artifact_fix_plan(**common, attempt=0)
    second = artifact_fix_plan(**common, attempt=1)

    assert first["cast_count"] == 2
    assert "He overwhelms her with gifts" in first["narrative_anchor"]
    assert "two hosts" in first["still_prompt"]
    assert "no shared bubble" in first["still_prompt"]
    assert len(first["still_prompt"]) <= PROMPT_CHAR_BUDGET
    assert len(first["motion_prompt"]) <= PROMPT_CHAR_BUDGET
    assert first["still_prompt"] != second["still_prompt"]
    assert first["variant_id"] != second["variant_id"]


def test_scene_aggregate_qa_is_bound_to_current_asset_bytes(tmp_path: Path) -> None:
    workspace = tmp_path / "job"
    (workspace / "stills").mkdir(parents=True)
    still = workspace / "stills" / "b00.png"
    still.write_bytes(b"first-still")
    scene = {
        "index": 0,
        "sid": "b00",
        "still_rel": "stills/b00.png",
        "narration": "A specific story beat.",
        "scene_action": "The host opens a letter at the kitchen table.",
        "cast_count": 1,
    }
    aggregate = visual_qa.build_scene_visual_qa(
        workspace,
        scene,
        still_qa=_pass_report("identity passed"),
        correspondence_qa=_pass_report("correspondence passed"),
        require_clip=False,
    )
    scene["visual_qa"] = aggregate

    assert visual_qa.scene_visual_qa_is_fresh(workspace, scene) is True
    still.write_bytes(b"replacement-still-with-different-bytes")
    assert visual_qa.scene_visual_qa_is_fresh(workspace, scene) is False


def test_finalize_preflight_fails_closed_on_current_aggregate_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "job"
    (workspace / "stills").mkdir(parents=True)
    (workspace / "clips").mkdir(parents=True)
    (workspace / "stills" / "b00.png").write_bytes(b"still")
    (workspace / "clips" / "b00.mp4").write_bytes(b"clip")
    scenes = [{
        "index": 0,
        "sid": "b00",
        "still_rel": "stills/b00.png",
        "clip_rel": "clips/b00.mp4",
        "animate": True,
        "approved_for_animation": True,
    }]
    (workspace / "scenes.json").write_text(json.dumps(scenes), encoding="utf-8")
    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: workspace)
    monkeypatch.setattr(
        tools,
        "_refresh_scene_visual_qa",
        lambda *_args, **_kwargs: {
            "status": "fail",
            "pass": False,
            "summary": "Clip identity drift",
            "failed_components": ["clip"],
            "fingerprint": "current",
        },
    )

    result = tools.shortform_finalize_preflight("job")

    assert result["status"] == "visual_qa_failed"
    assert result["qa_blocked"] is True
    assert result["failed_scenes"][0]["failed_components"] == ["clip"]


def test_failed_i2v_qa_rejects_candidate_without_auto_backing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "job"
    (workspace / "stills").mkdir(parents=True)
    (workspace / "clips").mkdir(parents=True)
    (workspace / "stills" / "b00.png").write_bytes(b"approved-still")
    clip = workspace / "clips" / "b00.mp4"
    old_clip = b"old-approved-clip" * 200
    clip.write_bytes(old_clip)
    (workspace / "job_spec.json").write_text(
        json.dumps({"render_style": "cinematic"}),
        encoding="utf-8",
    )
    (workspace / "scenes.json").write_text(json.dumps([{
        "index": 0,
        "sid": "b00",
        "still_rel": "stills/b00.png",
        "clip_rel": "clips/b00.mp4",
        "status": "clip_ready",
        "narration": "The camera reveals the evidence.",
        "prompt": "Cinematic evidence room.",
        "approved_for_video": True,
        "approved_for_animation": True,
        "animate": True,
        "video_model": "seedance",
        "duration_sec": 5,
    }]), encoding="utf-8")

    def fake_clip(_still: Path, _motion: str, out: Path, **_kwargs) -> Path:
        out.write_bytes(b"rejected-new-clip" * 200)
        out.with_suffix(out.suffix + ".fal.json").write_text(json.dumps({
            "provider": "fal",
            "endpoint": "fal-ai/seedance",
            "cost_usd": 0,
        }), encoding="utf-8")
        return out

    monkeypatch.setenv("STUDIO_I2V_QA_MAX_ATTEMPTS", "1")
    monkeypatch.setattr(styled_pipeline, "gen_clip", fake_clip)
    monkeypatch.setattr(
        visual_qa,
        "audit_generic_clip",
        lambda *_args, **_kwargs: {
            "status": "fail",
            "pass": False,
            "provider": "semantic_qa",
            "confidence": 0.95,
            "summary": "Visible morphing and prompt drift",
        },
    )

    result = styled_pipeline.animate_scenes_stage(workspace, indices=[0])
    scene = styled_pipeline.load_scenes(workspace)[0]

    assert result["failed"] == [0]
    assert result["animated"] == []
    assert clip.read_bytes() == old_clip
    assert scene.get("motion_fallback") is None
    assert scene["i2v_qa"]["pass"] is False
    assert "prior asset retained" in scene["last_repair_error"]


def test_longform_skeleton_regenerate_uses_cast_aware_retry_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    out = tmp_path / "scene_0000.png"
    prompts: list[str] = []
    audited_casts: list[int] = []
    audit_results = iter([
        {"status": "fail", "pass": False, "summary": "first candidate failed"},
        _pass_report(),
    ])

    def fake_generate(prompt: str, target: Path, **_kwargs) -> Path:
        prompts.append(prompt)
        target.write_bytes(f"candidate-{len(prompts)}".encode("utf-8"))
        return target

    def fake_audit(_path: Path, **kwargs) -> dict:
        audited_casts.append(int(kwargs.get("cast_count") or 0))
        return next(audit_results)

    monkeypatch.setattr(canonical_edit, "generate_still_edit", fake_generate)
    monkeypatch.setattr(canonical_edit, "resolve_master_reference_local", lambda: None)
    monkeypatch.setattr(visual_qa, "audit_skeleton_still", fake_audit)

    result = longform_pipeline._gen_skeleton_longform_scene(
        "He overwhelms her with gifts at the apartment doorway.",
        out,
        topic="Love bombing in a relationship",
        image_model_id="seedream_edit",
    )

    assert result == out
    assert audited_casts == [2, 2]
    assert len(prompts) == 2
    assert prompts[0] != prompts[1]
    assert all(len(prompt) <= PROMPT_CHAR_BUDGET for prompt in prompts)
    assert all("two hosts" in prompt for prompt in prompts)


def test_incremental_budget_blocks_next_paid_attempt_before_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "job"
    workspace.mkdir()
    (workspace / "credit_reservation.json").write_text(json.dumps({
        "tool": "regenerate_production_scene",
        "cost_baseline_usd": 0,
        "budget": {"max_budget_usd": 0.10},
    }), encoding="utf-8")
    monkeypatch.setattr(
        production_costs,
        "load_summary",
        lambda _workspace: {"total_usd_decimal": "0.08"},
    )

    with pytest.raises(production_budget.BudgetExceededError) as exc:
        production_budget.enforce_incremental_spend(
            workspace,
            0.03,
            operation="retry",
            provider="fal",
            model="seedream",
        )

    assert "budget_exceeded_mid_job" in str(exc.value)
    allowed = production_budget.enforce_incremental_spend(
        workspace,
        0.02,
        operation="final allowed attempt",
    )
    assert allowed["remaining_usd"] == 0.02


def test_simulation_mode_repairs_one_scene_end_to_end_without_paid_provider(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "simulation-job"
    (workspace / "stills").mkdir(parents=True)
    (workspace / "clips").mkdir(parents=True)
    still = render_simulation.write_still(workspace / "stills" / "b00.png")
    reference = render_simulation.write_still(tmp_path / "canonical.png")
    (workspace / "job_spec.json").write_text(json.dumps({
        "render_style": "skeleton_host",
        "topic": "Love bombing in a relationship",
        "cast_count": 2,
        "image_model_id": "seedream_edit",
        "video_model": "seedance",
        "locked_outfit": "no clothing",
    }), encoding="utf-8")
    (workspace / "scenes.json").write_text(json.dumps([{
        "index": 0,
        "sid": "b00",
        "narration": "He overwhelms her with gifts at the apartment doorway.",
        "scene_action": "He overwhelms her with gifts at the apartment doorway.",
        "prompt": "Old rejected scene.",
        "still_rel": "stills/b00.png",
        "clip_rel": None,
        "status": "still_ready",
        "cast_count": 2,
        "outfit": "no clothing",
        "approved_for_video": False,
        "approved_for_animation": False,
        "animate": False,
        "image_model_id": "seedream_edit",
        "video_model": "seedance",
        "duration_sec": 1,
    }]), encoding="utf-8")
    (workspace / "result.json").write_text(json.dumps({
        "status": "budget_exceeded",
        "budget_exceeded": True,
        "qa_blocked": True,
        "error": "stale failure",
    }), encoding="utf-8")

    monkeypatch.setenv("STUDIO_RENDER_SIMULATION_MODE", "1")
    monkeypatch.setenv("STUDIO_RENDER_SIMULATION_SLEEP_SCALE", "0")
    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: workspace)
    monkeypatch.setattr(catalyst_health, "ensure_catalyst_skeleton_learning_ready", lambda: None)
    monkeypatch.setattr(canonical_edit, "resolve_master_reference_local", lambda *_args, **_kwargs: reference)
    monkeypatch.setattr(visual_qa, "_workspace_skeleton_reference", lambda _workspace: reference)
    monkeypatch.setattr(visual_qa, "audit_skeleton_still", lambda *_args, **_kwargs: _pass_report())
    monkeypatch.setattr(visual_qa, "audit_scene_correspondence", lambda *_args, **_kwargs: _pass_report())
    monkeypatch.setattr(visual_qa, "audit_skeleton_clip", lambda *_args, **_kwargs: _pass_report())
    monkeypatch.setattr(
        catalyst_still_audit,
        "audit_scene_still",
        lambda *_args, **_kwargs: {
            "status": "pass",
            "pass": True,
            "method": "none",
            "channel_key": "simulation",
            "issue_labels": [],
        },
    )
    monkeypatch.setattr(
        catalyst_still_audit,
        "record_catalyst_still_artifact_learning",
        lambda **_kwargs: {},
    )

    result = json.loads(tools.regenerate_production_scene(
        "simulation-job",
        0,
        reason="Fix the still artifact and fused glass.",
        animate=True,
        image_model_id="seedream_edit",
        video_model="seedance",
    ))
    scene = styled_pipeline.load_scenes(workspace)[0]
    durable = json.loads((workspace / "result.json").read_text(encoding="utf-8"))

    assert result["ok"] is True
    assert still.is_file()
    assert (workspace / str(scene["clip_rel"])).is_file()
    assert scene["visual_qa"]["pass"] is True
    assert scene["visual_qa"]["require_clip"] is True
    assert scene["visual_qa"]["fingerprint"]
    assert durable["qa_blocked"] is False
    assert durable["budget_exceeded"] is False
    assert durable.get("error") is None
    assert production_costs.load_summary(workspace)["total_usd"] == 0

