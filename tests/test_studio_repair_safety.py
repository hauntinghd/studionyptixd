from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from skeleton_ai import compose, i2v_engine, styled_pipeline
from skeleton_ai.styled_stills import StyledStillError
from studio_agent import catalyst_still_audit, tools, visual_qa


JOB_ID = "repair-safety-job"


def _workspace(tmp_path: Path, *, existing_clip: bool = False) -> Path:
    workspace = tmp_path / JOB_ID
    (workspace / "stills").mkdir(parents=True)
    (workspace / "clips").mkdir(parents=True)
    (workspace / "stills" / "b00.png").write_bytes(b"still")
    clip_rel = None
    status = "still_ready"
    if existing_clip:
        clip = workspace / "clips" / "b00.mp4"
        clip.write_bytes(b"old-playable-clip" * 200)
        clip.with_suffix(".mp4.fal.json").write_text("old provider metadata", encoding="utf-8")
        clip.with_suffix(".mp4.visualqa.json").write_text("old visual QA", encoding="utf-8")
        clip_rel = "clips/b00.mp4"
        status = "clip_ready"
    (workspace / "job_spec.json").write_text(
        json.dumps({"render_style": "cinematic", "topic": "A distinct story beat"}),
        encoding="utf-8",
    )
    (workspace / "scenes.json").write_text(
        json.dumps([
            {
                "index": 0,
                "sid": "b00",
                "narration": "A distinct story beat.",
                "scene_action": "A subject reacts in a specific physical location.",
                "prompt": "A cinematic interior.",
                "still_rel": "stills/b00.png",
                "clip_rel": clip_rel,
                "status": status,
                "animate": True,
                "approved_for_video": True,
                "approved_for_animation": True,
                "video_model": "grok_imagine_video",
                "duration_sec": 5.0,
            }
        ]),
        encoding="utf-8",
    )
    return workspace


def _pass_still(*_args, **_kwargs):
    return {"status": "pass", "pass": True, "issues": []}


def test_emotional_narrative_finding_is_not_misclassified_as_motion_only():
    report = {
        "pass": False,
        "issues": ["narrative_mismatch", "duplicate_adjacent", "generic_staging"],
        "summary": "The still repeats the prior emotional beat and generic staging.",
        "recommended_restage": "Use a new location, composition, and opening pose.",
    }

    assert tools._scene_correspondence_motion_only(report) is False


def test_narrative_correspondence_failure_restages_still(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path)
    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: workspace)
    monkeypatch.setattr(visual_qa, "audit_generic_still", _pass_still)
    monkeypatch.setattr(
        visual_qa,
        "audit_scene_correspondence",
        lambda *_args, **_kwargs: {
            "status": "fail",
            "pass": False,
            "issues": ["narrative_mismatch", "duplicate_adjacent"],
            "summary": "The still repeats the prior emotional beat.",
            "recommended_restage": "Use a distinct location and opening pose.",
        },
    )
    restaged: list[tuple[int, str]] = []

    def fake_regenerate(_job_id, scene_index, **_kwargs):
        restaged.append((scene_index, str(_kwargs.get("restage_direction") or "")))
        return json.dumps({"ok": True, "scene_index": scene_index})

    monkeypatch.setattr(tools, "regenerate_production_scene", fake_regenerate)
    monkeypatch.setattr(
        tools,
        "repair_production_scene_animation",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("narrative still failure must not route to animation-only repair")
        ),
    )

    result = json.loads(tools.audit_and_repair_production_scenes(JOB_ID, [0], "fix it"))

    assert restaged == [(0, "Use a distinct location and opening pose.")]
    assert result["ok"] is True
    assert result["repaired_stills"] == [0]
    assert result["repaired_animations"] == []
    assert result["passed_without_changes"] == []


def test_motion_only_failure_with_missing_clip_is_repaired_not_false_passed(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path)
    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: workspace)
    monkeypatch.setattr(visual_qa, "audit_generic_still", _pass_still)
    monkeypatch.setattr(
        visual_qa,
        "audit_scene_correspondence",
        lambda *_args, **_kwargs: {
            "status": "fail",
            "pass": False,
            "issues": [],
            "summary": "The still is correct, but the animation lacks multi-second motion over time.",
            "recommended_restage": "Strengthen the animation and camera push.",
        },
    )
    attempts: list[int] = []

    def fake_animation_repair(_job_id, scene_index, _reason, **_kwargs):
        attempts.append(scene_index)
        return json.dumps({"ok": False, "failed": [scene_index]})

    monkeypatch.setattr(tools, "repair_production_scene_animation", fake_animation_repair)

    result = json.loads(tools.audit_and_repair_production_scenes(JOB_ID, [0], "fix motion"))

    assert attempts == [0]
    assert result["ok"] is False
    assert result["repaired_animations"] == []
    assert result["attempted_animation_repairs"] == [0]
    assert result["failed"] == [0]
    assert result["passed_without_changes"] == []


def test_failed_reanimation_restores_previous_clip_and_sidecars(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path, existing_clip=True)
    clip = workspace / "clips" / "b00.mp4"
    old_clip = clip.read_bytes()
    monkeypatch.setattr(
        styled_pipeline,
        "gen_clip",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("provider unavailable")),
    )

    result = styled_pipeline.animate_scenes_stage(workspace, indices=[0])
    scene = styled_pipeline.load_scenes(workspace)[0]

    assert result["failed"] == [0]
    assert result["animated"] == []
    assert clip.read_bytes() == old_clip
    assert clip.with_suffix(".mp4.fal.json").read_text(encoding="utf-8") == "old provider metadata"
    assert clip.with_suffix(".mp4.visualqa.json").read_text(encoding="utf-8") == "old visual QA"
    assert scene["clip_rel"] == "clips/b00.mp4"
    assert scene["status"] == "clip_ready"
    assert "provider unavailable" in scene["last_repair_error"]
    assert not list((workspace / "clips").glob("*.replace"))


def test_animation_tool_reports_failed_replacement_even_when_old_clip_was_restored(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path, existing_clip=True)
    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: workspace)
    monkeypatch.setattr(
        styled_pipeline,
        "animate_scenes_stage",
        lambda *_args, **_kwargs: {"status": "partial", "animated": [], "failed": [0]},
    )

    result = json.loads(tools.animate_production_scenes(JOB_ID, [0]))

    assert result["ok"] is False
    assert result["animated"] == []
    assert result["failed"] == [0]


def test_per_scene_animation_failures_accumulate_and_use_human_scene_numbers(tmp_path, monkeypatch):
    workspace = tmp_path / JOB_ID
    (workspace / "stills").mkdir(parents=True)
    (workspace / "clips").mkdir()
    scenes = []
    for index in range(3):
        sid = f"b{index:02d}"
        (workspace / "stills" / f"{sid}.png").write_bytes(b"still")
        scenes.append({
            "index": index,
            "sid": sid,
            "still_rel": f"stills/{sid}.png",
            "clip_rel": None,
            "status": "still_ready",
            "animate": True,
            "approved_for_video": True,
            "approved_for_animation": True,
        })
    (workspace / "scenes.json").write_text(json.dumps(scenes), encoding="utf-8")
    (workspace / "job_spec.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: workspace)

    def fail_selected(_workspace, *, indices, tier, route_resolver=None):
        assert tier == "standard"
        assert route_resolver is None
        return {"status": "partial", "animated": [], "failed": list(indices)}

    monkeypatch.setattr(styled_pipeline, "animate_scenes_stage", fail_selected)

    first = json.loads(tools.animate_production_scenes(JOB_ID, [1]))
    second = json.loads(tools.animate_production_scenes(JOB_ID, [2]))
    persisted = json.loads((workspace / "result.json").read_text(encoding="utf-8"))

    assert first["failed"] == [1]
    assert second["failed"] == [2]
    assert persisted["animation_failed"] == [1, 2]
    assert persisted["animation_failed_scene_numbers"] == [2, 3]
    assert persisted["error"] == "Animation failed for scene(s): [2, 3]"


def test_xai_credit_limit_falls_back_to_funded_fal_video_lane(tmp_path, monkeypatch):
    still = tmp_path / "still.png"
    output = tmp_path / "clip.mp4"
    still.write_bytes(b"image")
    fallback_calls: list[str] = []

    monkeypatch.setattr(i2v_engine.render_simulation, "enabled", lambda: False)
    monkeypatch.setattr(
        i2v_engine,
        "_xai_i2v_result",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            i2v_engine.I2VError(
                'grok-imagine-video 403: {"code":"permission-denied",'
                '"error":"used all available credits or reached its monthly spending limit"}'
            )
        ),
    )
    monkeypatch.setattr(i2v_engine, "_ensure_fal", lambda: "test-fal-key")
    monkeypatch.setattr(
        i2v_engine,
        "fal_client",
        SimpleNamespace(upload_file=lambda _path: "https://example.test/still.png"),
    )

    def fake_queue(endpoint, _args, *, timeout_sec):
        assert timeout_sec > 0
        fallback_calls.append(endpoint)
        return {
            "video": {"url": "https://example.test/video.mp4"},
            "_fal_endpoint": endpoint,
            "_fal_request_id": "fal-request-1",
        }

    monkeypatch.setattr(i2v_engine, "_queue_result", fake_queue)
    monkeypatch.setattr(i2v_engine, "_download", lambda _url, path: Path(path).write_bytes(b"video"))
    monkeypatch.setattr(compose, "strip_clip_audio", lambda _path: None)

    result = i2v_engine.generate(
        still,
        "SILENT: subtle movement",
        output,
        video_model="grok_imagine_video",
    )
    metadata = json.loads(output.with_suffix(".mp4.fal.json").read_text(encoding="utf-8"))

    assert result == output
    assert fallback_calls == [i2v_engine.SEEDANCE_ENDPOINT]
    assert metadata["endpoint"] == i2v_engine.SEEDANCE_ENDPOINT


def test_video_fallback_guard_blocks_stale_fal_generation_request(tmp_path, monkeypatch):
    still = tmp_path / "still.png"
    still.write_bytes(b"image")
    guard_calls: list[bool] = []

    monkeypatch.setattr(i2v_engine.render_simulation, "enabled", lambda: False)
    monkeypatch.setattr(
        i2v_engine,
        "_xai_i2v_result",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            i2v_engine.I2VError(
                'grok-imagine-video 403: {"error":"used all available credits"}'
            )
        ),
    )
    monkeypatch.setattr(
        i2v_engine,
        "_ensure_fal",
        lambda: (_ for _ in ()).throw(
            AssertionError("stale route must abort before FAL upload or generation")
        ),
    )
    monkeypatch.setattr(
        i2v_engine,
        "_queue_result",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("stale route must not spend on FAL generation")
        ),
    )

    def route_guard() -> bool:
        guard_calls.append(True)
        return False

    with pytest.raises(i2v_engine.I2VRouteChanged, match="before video fallback"):
        i2v_engine.generate(
            still,
            "SILENT: subtle movement",
            tmp_path / "clip.mp4",
            video_model="grok_imagine_video",
            fallback_guard=route_guard,
        )

    assert guard_calls == [True]


def test_unrelated_xai_forbidden_error_does_not_hide_auth_problem(tmp_path, monkeypatch):
    still = tmp_path / "still.png"
    still.write_bytes(b"image")
    monkeypatch.setattr(i2v_engine.render_simulation, "enabled", lambda: False)
    monkeypatch.setattr(
        i2v_engine,
        "_xai_i2v_result",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            i2v_engine.I2VError("grok-imagine-video 403: invalid API entitlement")
        ),
    )
    monkeypatch.setattr(
        i2v_engine,
        "_queue_result",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("non-billing authorization errors must not be masked by fallback")
        ),
    )

    with pytest.raises(i2v_engine.I2VError, match="invalid API entitlement"):
        i2v_engine.generate(
            still,
            "SILENT: subtle movement",
            tmp_path / "clip.mp4",
            video_model="grok_imagine_video",
        )


def test_xai_image_credit_limit_falls_back_without_touching_canonical_still(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path)
    canonical = workspace / "stills" / "b00.png"
    prior = canonical.read_bytes()
    candidate = workspace / "stills" / ".candidate.png"
    calls: list[str] = []

    def fake_generate(_prompt, output, **kwargs):
        model = str(kwargs.get("image_model_id") or "")
        calls.append(model)
        if model == "grok_imagine":
            raise StyledStillError(
                'grok-imagine-image-quality 403: {"code":"permission-denied",'
                '"error":"used all available credits or reached its monthly spending limit"}',
                provider="xai",
            )
        Path(output).write_bytes(b"funded-fal-candidate")
        return {"provider": model}

    monkeypatch.setattr(styled_pipeline, "generate_still_t2i", fake_generate)

    result = styled_pipeline.regenerate_scene(
        workspace,
        0,
        image_model_id="grok_imagine",
        fallback_image_model_id="seedream_v4",
        candidate_path=candidate,
        defer_commit=True,
    )

    assert calls == ["grok_imagine", "seedream_v4"]
    assert canonical.read_bytes() == prior
    assert candidate.read_bytes() == b"funded-fal-candidate"
    assert result["image_model_id"] == "seedream_v4"
    assert result["fallback_from"] == "grok_imagine"


def test_image_fallback_guard_blocks_stale_fal_generation_request(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path)
    canonical = workspace / "stills" / "b00.png"
    candidate = workspace / "stills" / ".candidate.png"
    prior = canonical.read_bytes()
    calls: list[str] = []

    def fake_generate(_prompt, output, **kwargs):
        model = str(kwargs.get("image_model_id") or "")
        calls.append(model)
        if model == "grok_imagine":
            raise StyledStillError(
                'grok-imagine-image-quality 403: {"error":"used all available credits"}',
                provider="xai",
            )
        Path(output).write_bytes(b"stale-fallback")
        return {"provider": model}

    monkeypatch.setattr(styled_pipeline, "generate_still_t2i", fake_generate)

    with pytest.raises(styled_pipeline.MediaRouteChangedError, match="before image fallback"):
        styled_pipeline.regenerate_scene(
            workspace,
            0,
            image_model_id="grok_imagine",
            fallback_image_model_id="seedream_v4",
            candidate_path=candidate,
            defer_commit=True,
            fallback_guard=lambda: False,
        )

    assert calls == ["grok_imagine"]
    assert canonical.read_bytes() == prior
    assert not candidate.exists()


def test_xai_image_auth_error_does_not_fallback_or_delete_prior_still(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path)
    canonical = workspace / "stills" / "b00.png"
    prior = canonical.read_bytes()
    calls: list[str] = []

    def fail_auth(_prompt, _output, **kwargs):
        calls.append(str(kwargs.get("image_model_id") or ""))
        raise StyledStillError(
            "grok-imagine-image-quality 403: invalid API entitlement",
            provider="xai",
        )

    monkeypatch.setattr(styled_pipeline, "generate_still_t2i", fail_auth)

    with pytest.raises(StyledStillError, match="invalid API entitlement"):
        styled_pipeline.regenerate_scene(
            workspace,
            0,
            image_model_id="grok_imagine",
            fallback_image_model_id="seedream_v4",
            candidate_path=workspace / "stills" / ".candidate.png",
            defer_commit=True,
        )

    assert calls == ["grok_imagine"]
    assert canonical.read_bytes() == prior


def test_failed_candidate_qa_retains_previous_still_and_clip(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path, existing_clip=True)
    canonical = workspace / "stills" / "b00.png"
    clip = workspace / "clips" / "b00.mp4"
    old_still = canonical.read_bytes()
    old_clip = clip.read_bytes()
    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: workspace)
    monkeypatch.setattr(
        catalyst_still_audit,
        "audit_scene_still",
        lambda *_args, **_kwargs: {
            "status": "pass",
            "pass": True,
            "method": "none",
            "channel_key": "test",
            "issue_labels": [],
        },
    )
    monkeypatch.setattr(
        catalyst_still_audit,
        "record_catalyst_still_artifact_learning",
        lambda **_kwargs: {"recorded": True},
    )

    def fake_candidate(_workspace, _index, **kwargs):
        candidate = Path(kwargs["candidate_path"])
        candidate.write_bytes(b"candidate-that-fails-qa")
        return {
            "index": 0,
            "candidate_path": str(candidate),
            "image_model_id": "seedream_edit",
            "image_provider": "fal",
        }

    monkeypatch.setattr(styled_pipeline, "regenerate_scene_with_catalyst", fake_candidate)
    monkeypatch.setattr(
        visual_qa,
        "audit_generic_still",
        lambda *_args, **_kwargs: {
            "status": "fail",
            "pass": False,
            "summary": "candidate does not match the scene",
        },
    )

    result = json.loads(tools.regenerate_production_scene_still(
        JOB_ID,
        0,
        image_model_id="seedream_edit",
    ))

    assert result["ok"] is False
    assert canonical.read_bytes() == old_still
    assert clip.read_bytes() == old_clip
    assert result["scene"]["committed"] is False
    assert result["quarantined_candidates"]


def test_midflight_image_route_change_quarantines_old_result_and_restarts(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path)
    canonical = workspace / "stills" / "b00.png"
    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: workspace)
    snapshots = [
        {"revision": 1, "image_model_id": "grok_imagine", "video_model": "grok_imagine_video"},
        {"revision": 1, "image_model_id": "grok_imagine", "video_model": "grok_imagine_video"},
        {"revision": 1, "image_model_id": "grok_imagine", "video_model": "grok_imagine_video"},
        {"revision": 2, "image_model_id": "seedream_v4", "video_model": "seedance"},
    ]
    latest = {"revision": 2, "image_model_id": "seedream_v4", "video_model": "seedance"}

    def route(*_args, **_kwargs):
        return dict(snapshots.pop(0) if snapshots else latest)

    monkeypatch.setattr(tools, "_repair_route_snapshot", route)
    monkeypatch.setattr(
        catalyst_still_audit,
        "audit_scene_still",
        lambda *_args, **_kwargs: {
            "status": "pass", "pass": True, "method": "none", "channel_key": "test", "issue_labels": [],
        },
    )
    monkeypatch.setattr(
        catalyst_still_audit,
        "record_catalyst_still_artifact_learning",
        lambda **_kwargs: {},
    )

    def fake_candidate(_workspace, _index, **kwargs):
        candidate = Path(kwargs["candidate_path"])
        model = str(kwargs.get("image_model_id") or "")
        candidate.write_bytes(model.encode("utf-8"))
        return {"candidate_path": str(candidate), "image_model_id": model, "image_provider": "test"}

    monkeypatch.setattr(styled_pipeline, "regenerate_scene_with_catalyst", fake_candidate)
    monkeypatch.setattr(visual_qa, "audit_generic_still", _pass_still)

    result = json.loads(tools.regenerate_production_scene_still(
        JOB_ID,
        0,
        session_id="session-route-test",
    ))

    assert result["ok"] is True
    assert result["route"]["revision"] == 2
    assert result["route_switches"]
    assert canonical.read_bytes() == b"seedream_v4"
    assert result["quarantined_candidates"]


def test_midflight_video_route_change_quarantines_old_result(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path, existing_clip=True)
    calls: list[str] = []
    routes = [
        {"revision": 1, "video_model": "grok_imagine_video"},
        {"revision": 1, "video_model": "grok_imagine_video"},
        {"revision": 2, "video_model": "seedance"},
    ]
    latest = {"revision": 2, "video_model": "seedance"}

    def resolve_route():
        return dict(routes.pop(0) if routes else latest)

    def fake_clip(_still, _motion, output, **kwargs):
        model = str(kwargs.get("video_model") or "")
        calls.append(model)
        output = Path(output)
        output.write_bytes((model + "-clip").encode("utf-8") * 200)
        output.with_suffix(".mp4.fal.json").write_text(
            json.dumps({"endpoint": f"test:{model}", "video_model": model, "duration_sec": 5}),
            encoding="utf-8",
        )

    monkeypatch.setattr(styled_pipeline, "gen_clip", fake_clip)
    monkeypatch.setattr(visual_qa, "audit_generic_clip", lambda *_args, **_kwargs: _pass_still())

    result = styled_pipeline.animate_scenes_stage(
        workspace,
        indices=[0],
        route_resolver=resolve_route,
    )

    assert calls == ["grok_imagine_video", "seedance"]
    assert result["failed"] == []
    assert result["route"]["revision"] == 2
    assert result["route_switches"]


def test_video_fallback_route_change_restarts_before_secondary_provider_spend(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path, existing_clip=True)
    calls: list[str] = []
    routes = [
        {"revision": 1, "video_model": "grok_imagine_video"},  # scene route
        {"revision": 1, "video_model": "grok_imagine_video"},  # first dispatch
        {"revision": 2, "video_model": "seedance"},  # fallback guard
    ]
    latest = {"revision": 2, "video_model": "seedance"}

    def resolve_route():
        return dict(routes.pop(0) if routes else latest)

    def fake_clip(_still, _motion, output, **kwargs):
        model = str(kwargs.get("video_model") or "")
        calls.append(model)
        if model == "grok_imagine_video":
            if kwargs["fallback_guard"]() is not True:
                raise i2v_engine.I2VRouteChanged("route changed before fallback")
            raise AssertionError("stale fallback guard unexpectedly accepted old route")
        output = Path(output)
        output.write_bytes(b"current-route-clip" * 200)
        output.with_suffix(".mp4.fal.json").write_text(
            json.dumps({"endpoint": "test:seedance", "video_model": model, "duration_sec": 5}),
            encoding="utf-8",
        )

    monkeypatch.setattr(styled_pipeline, "gen_clip", fake_clip)
    monkeypatch.setattr(visual_qa, "audit_generic_clip", lambda *_args, **_kwargs: _pass_still())

    result = styled_pipeline.animate_scenes_stage(
        workspace,
        indices=[0],
        route_resolver=resolve_route,
    )

    assert calls == ["grok_imagine_video", "seedance"]
    assert result["failed"] == []
    assert any(row["stage"] == "video_fallback" for row in result["route_switches"])


def test_video_route_change_at_final_commit_restores_previous_clip(tmp_path, monkeypatch):
    workspace = _workspace(tmp_path, existing_clip=True)
    clip = workspace / "clips" / "b00.mp4"
    old_clip = clip.read_bytes()
    routes = [
        {"revision": 1, "video_model": "grok_imagine_video"},  # scene route
        {"revision": 1, "video_model": "grok_imagine_video"},  # dispatch
        {"revision": 1, "video_model": "grok_imagine_video"},  # provider return
        {"revision": 1, "video_model": "grok_imagine_video"},  # post-QA commit gate
        {"revision": 2, "video_model": "seedance"},  # final atomic commit gate
    ]
    latest = {"revision": 2, "video_model": "seedance"}

    def resolve_route():
        return dict(routes.pop(0) if routes else latest)

    def fake_clip(_still, _motion, output, **kwargs):
        output = Path(output)
        output.write_bytes(b"stale-new-clip" * 200)
        output.with_suffix(".mp4.fal.json").write_text(
            json.dumps({
                "endpoint": "test:grok",
                "video_model": kwargs.get("video_model"),
                "duration_sec": 5,
            }),
            encoding="utf-8",
        )

    monkeypatch.setattr(styled_pipeline, "gen_clip", fake_clip)
    monkeypatch.setattr(visual_qa, "audit_generic_clip", lambda *_args, **_kwargs: _pass_still())

    result = styled_pipeline.animate_scenes_stage(
        workspace,
        indices=[0],
        route_resolver=resolve_route,
    )

    assert result["failed"] == [0]
    assert clip.read_bytes() == old_clip
    assert any(row["stage"] == "video_final_commit" for row in result["route_switches"])


def test_repair_reconciliation_clears_stale_animation_error_for_still_failure(tmp_path):
    workspace = _workspace(tmp_path)
    (workspace / "result.json").write_text(json.dumps({
        "status": "failed",
        "topic": "preserve me",
        "animation_failed": [0],
        "error": "Animation failed for scene(s): [1]",
    }), encoding="utf-8")
    reports = [{
        "scene_index": 0,
        "status": "failed",
        "failure_stage": "still",
        "repair": {"error": "replacement still failed QA"},
    }]

    tools._reconcile_audit_repair_state(
        workspace,
        job_id=JOB_ID,
        selected=[0],
        failed=[0],
        reports=reports,
        repaired_stills=[],
        repaired_animations=[],
    )
    result = json.loads((workspace / "result.json").read_text(encoding="utf-8"))
    progress = json.loads((workspace / "progress.json").read_text(encoding="utf-8"))

    assert result["topic"] == "preserve me"
    assert result["animation_failed"] == []
    assert "animation was not started" in result["error"]
    assert "Animation failed for scene(s): [1]" not in result["error"]
    assert progress["detail"] == result["error"]
