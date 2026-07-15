import json
import multiprocessing
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

from skeleton_ai import styled_pipeline
from studio_agent import production_budget, tools
from studio_agent.visual_fix_contract import harden_planned_scenes_for_expand


def _hold_expand_file_lock(workspace: str, entered, output, hold_seconds: float) -> None:
    started = time.time()
    with tools._expand_job_file_lock(Path(workspace)):
        acquired = time.time()
        entered.set()
        time.sleep(hold_seconds)
    output.put((started, acquired, time.time()))


def _proof_workspace(tmp_path: Path, *, job_id: str = "proof-job") -> Path:
    workspace = tmp_path / job_id
    workspace.mkdir(parents=True)
    (workspace / "job_spec.json").write_text(
        json.dumps(
            {
                "job_id": job_id,
                "topic": "Why Men Love Bomb Then Disappear",
                "visual_proof_only": True,
                "scene_count": 1,
                "user_id": "user-1",
                "image_model_id": "grok_imagine",
                "video_model": "grok_imagine_video",
            }
        ),
        encoding="utf-8",
    )
    (workspace / "scenes.json").write_text(
        json.dumps(
            [
                {
                    "index": 0,
                    "sid": "b00",
                    "still_rel": "stills/b00.png",
                    "clip_rel": "clips/b00.mp4",
                    "prompt": "approved proof prompt",
                    "motion_prompt": "approved proof motion",
                    "approved_for_video": True,
                    "approved_for_animation": True,
                    "animate": True,
                    "status": "clip_ready",
                    "duration_sec": 5.0,
                }
            ]
        ),
        encoding="utf-8",
    )
    return workspace


def _expand_args(**updates):
    args = {
        "job_id": "proof-job",
        "scene_count": 6,
        "existing_scene_count": 1,
        "preserve_scene_indices": [0],
        "animate_scene_indices": [1, 2, 3, 4, 5],
        "duration_seconds": 30,
        "image_model_id": "grok_imagine",
        "video_model": "grok_imagine_video",
        "animate_policy": "all",
    }
    args.update(updates)
    return args


def test_expand_budget_prices_only_incremental_stills_and_selected_new_animation():
    estimate = production_budget.estimate_tool_cost(
        "expand_visual_proof_shortform",
        _expand_args(),
    )

    assert estimate.estimated_usd > 0
    assert estimate.estimated_usd <= estimate.max_budget_usd
    assert estimate.breakdown["existing_scene_count"] == 1
    assert estimate.breakdown["additional_scene_count"] == 5
    assert estimate.breakdown["animate_scene_indices"] == [1, 2, 3, 4, 5]
    assert estimate.breakdown["animated_new_scene_seconds"] == 25.0
    assert estimate.breakdown["stills_usd"] == 0.5
    assert estimate.breakdown["video_usd"] == 1.25


def test_expand_budget_honors_explicit_no_animation_contract():
    estimate = production_budget.estimate_tool_cost(
        "expand_visual_proof_shortform",
        _expand_args(animate_scene_indices=[]),
    )

    assert estimate.breakdown["animation_estimate_mode"] == "explicit_indices"
    assert estimate.breakdown["animated_new_scene_count"] == 0
    assert estimate.breakdown["animated_new_scene_seconds"] == 0.0
    assert estimate.breakdown["video_usd"] == 0.0


def test_expand_has_budget_approval_lane_stage_and_durable_contracts():
    args = _expand_args()
    estimate = production_budget.estimate_tool_cost("expand_visual_proof_shortform", args)
    control = production_budget.production_control_metadata(
        "expand_visual_proof_shortform", args, estimate
    )

    assert production_budget.is_budgeted_tool("expand_visual_proof_shortform")
    assert "expand_visual_proof_shortform" in tools.APPROVAL_REQUIRED
    assert control["requires_approval"] is True
    assert control["lane"] == "render"
    assert control["stage_gates"][0] == "proof_approved"
    assert control["durable_state"] == {
        "kind": "shortform",
        "key": "job_id",
        "job_id": "proof-job",
        "must_persist": True,
    }


def test_expand_command_replay_is_idempotent_and_echoes_contract(tmp_path, monkeypatch):
    workspace = _proof_workspace(tmp_path)
    started_threads = []

    class DeferredThread:
        def __init__(self, *args, **kwargs):
            started_threads.append(kwargs.get("name"))

        def start(self):
            return None

    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: workspace)
    monkeypatch.setattr(tools.threading, "Thread", DeferredThread)

    first = json.loads(
        tools.expand_visual_proof_shortform(
            "proof-job",
            6,
            duration_seconds=30,
            animate_policy="all",
            command_id="turn-123:expand",
            existing_scene_count=1,
            preserve_scene_indices=[0],
            animate_scene_indices=[1, 2, 3, 4, 5],
        )
    )
    replay = json.loads(
        tools.expand_visual_proof_shortform(
            "proof-job",
            6,
            duration_seconds=30,
            animate_policy="all",
            command_id="turn-123:expand",
            existing_scene_count=1,
            preserve_scene_indices=[0],
            animate_scene_indices=[1, 2, 3, 4, 5],
        )
    )

    assert first["scene_count"] == 6
    assert first["additional_scene_count"] == 5
    assert first["preserve_scene_indices"] == [0]
    assert first["animate_scene_indices"] == [1, 2, 3, 4, 5]
    assert first["idempotent_replay"] is False
    assert replay["idempotent_replay"] is True
    assert started_threads == ["expand-proof-job"]


def test_expand_different_command_id_cannot_start_a_concurrent_worker(tmp_path, monkeypatch):
    workspace = _proof_workspace(tmp_path)
    started_threads = []

    class DeferredThread:
        def __init__(self, *args, **kwargs):
            started_threads.append(kwargs.get("name"))

        def start(self):
            return None

    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: workspace)
    monkeypatch.setattr(tools.threading, "Thread", DeferredThread)

    first = json.loads(
        tools.expand_visual_proof_shortform(
            "proof-job",
            6,
            command_id="command-a",
            existing_scene_count=1,
            preserve_scene_indices=[0],
            animate_scene_indices=[1, 2, 3, 4, 5],
        )
    )
    concurrent = json.loads(
        tools.expand_visual_proof_shortform(
            "proof-job",
            6,
            command_id="command-b",
            existing_scene_count=1,
            preserve_scene_indices=[0],
            animate_scene_indices=[1, 2, 3, 4, 5],
        )
    )

    assert first["idempotent_replay"] is False
    assert concurrent["idempotent_replay"] is True
    assert concurrent["command_id"] == "command-a"
    assert concurrent["replayed_for_command_id"] == "command-b"
    assert started_threads == ["expand-proof-job"]


def test_different_inflight_expand_contract_returns_conflict_not_false_replay(tmp_path, monkeypatch):
    workspace = _proof_workspace(tmp_path)
    started_threads = []

    class DeferredThread:
        def __init__(self, *args, **kwargs):
            started_threads.append(kwargs.get("name"))

        def start(self):
            return None

    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: workspace)
    monkeypatch.setattr(tools.threading, "Thread", DeferredThread)

    first = json.loads(
        tools.expand_visual_proof_shortform(
            "proof-job",
            6,
            command_id="command-six",
            existing_scene_count=1,
            preserve_scene_indices=[0],
            animate_scene_indices=[1, 2, 3, 4, 5],
        )
    )
    conflict = json.loads(
        tools.expand_visual_proof_shortform(
            "proof-job",
            8,
            command_id="command-eight",
            existing_scene_count=1,
            preserve_scene_indices=[0],
            animate_scene_indices=[1, 2, 3, 4, 5, 6, 7],
        )
    )

    assert first["ok"] is True
    assert conflict["ok"] is False
    assert conflict["status"] == "conflict"
    assert conflict["idempotent_replay"] is False
    assert conflict["active_scene_count"] == 6
    assert conflict["requested_scene_count"] == 8
    assert started_threads == ["expand-proof-job"]


def test_expand_claim_lock_serializes_separate_processes(tmp_path):
    workspace = _proof_workspace(tmp_path)
    context = multiprocessing.get_context("spawn")
    first_entered = context.Event()
    second_entered = context.Event()
    first_output = context.Queue()
    second_output = context.Queue()
    first = context.Process(
        target=_hold_expand_file_lock,
        args=(str(workspace), first_entered, first_output, 0.4),
    )
    second = context.Process(
        target=_hold_expand_file_lock,
        args=(str(workspace), second_entered, second_output, 0.0),
    )

    first.start()
    assert first_entered.wait(5)
    second.start()
    first.join(10)
    second.join(10)
    assert first.exitcode == 0
    assert second.exitcode == 0
    _first_started, _first_acquired, first_released = first_output.get(timeout=2)
    _second_started, second_acquired, _second_released = second_output.get(timeout=2)
    assert second_acquired >= first_released - 0.02


def test_conflicting_expand_releases_credit_reservation_without_charge(monkeypatch):
    import unified_credits as uc

    estimate = production_budget.BudgetEstimate(
        tool="expand_visual_proof_shortform",
        estimated_usd=1.25,
        max_budget_usd=8.0,
        mode="test",
        breakdown={},
    )
    reservation = {
        "reservation_id": "hold-conflict",
        "credits": 125,
        "unlimited": False,
    }
    released = []
    monkeypatch.setattr(production_budget, "enforce_budget", lambda *_args, **_kwargs: estimate)
    monkeypatch.setattr(tools, "_public_provider_block_message", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(
        tools,
        "execute_tool",
        lambda *_args, **_kwargs: json.dumps(
            {
                "ok": False,
                "status": "conflict",
                "error": "different expansion already in progress",
                "idempotent_replay": False,
            }
        ),
    )
    monkeypatch.setattr(uc, "reserve_usd", lambda *_args, **_kwargs: dict(reservation))
    monkeypatch.setattr(
        uc,
        "release_reservation",
        lambda user_id, reservation_id, *, reason: released.append((user_id, reservation_id, reason)),
    )
    monkeypatch.setattr(
        uc,
        "commit_reservation",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("conflict must not charge")),
    )
    monkeypatch.setattr(tools.telemetry, "record_tool_call", lambda *_args, **_kwargs: None)

    result = json.loads(
        tools.execute_tool_logged(
            "expand_visual_proof_shortform",
            {"job_id": "proof-job", "scene_count": 8, "existing_scene_count": 1},
            user_id="user-1",
            content_format="short",
            session_id="sa-conflict",
        )
    )

    assert result["ok"] is False
    assert result["status"] == "conflict"
    assert result["credits"]["charged"] == 0
    assert result["credits"]["reservation_released"] is True
    assert released == [
        (
            "user-1",
            "hold-conflict",
            "studio_tool_not_started:expand_visual_proof_shortform:conflict",
        )
    ]


def test_expand_legacy_arguments_remain_supported(tmp_path, monkeypatch):
    workspace = _proof_workspace(tmp_path)

    class DeferredThread:
        def __init__(self, *args, **kwargs):
            pass

        def start(self):
            return None

    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: workspace)
    monkeypatch.setattr(tools.threading, "Thread", DeferredThread)

    result = json.loads(
        tools.expand_visual_proof_shortform(
            "proof-job",
            6,
            30,
            "keep the approved visual language",
            "heroes",
        )
    )

    assert result["scene_count"] == 6
    assert result["existing_scene_count"] == 1
    assert result["preserve_scene_indices"] == [0]
    assert result["animate_scene_indices"] is None


def test_expand_rejects_animation_of_preserved_scene(tmp_path, monkeypatch):
    workspace = _proof_workspace(tmp_path)
    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: workspace)

    with pytest.raises(ValueError, match="new, non-preserved scenes"):
        tools.expand_visual_proof_shortform(
            "proof-job",
            6,
            command_id="invalid-expand",
            existing_scene_count=1,
            preserve_scene_indices=[0],
            animate_scene_indices=[0, 1, 2],
        )


def test_expand_rejects_wrong_owner_before_command_claim(tmp_path, monkeypatch):
    workspace = _proof_workspace(tmp_path)
    spec_path = workspace / "job_spec.json"
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    spec["user_id"] = "owner-user"
    spec_path.write_text(json.dumps(spec), encoding="utf-8")
    original = spec_path.read_text(encoding="utf-8")
    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: workspace)

    with pytest.raises(ValueError, match="ownership mismatch"):
        tools.expand_visual_proof_shortform(
            "proof-job",
            6,
            command_id="unauthorized-expand",
            existing_scene_count=1,
            credit_user_id="other-user",
        )

    assert spec_path.read_text(encoding="utf-8") == original


def test_preserved_scene_helper_keeps_clip_and_all_metadata():
    previous = {
        "index": 0,
        "sid": "b00",
        "still_rel": "stills/b00.png",
        "clip_rel": "clips/b00.mp4",
        "prompt": "creator-approved prompt",
        "motion_prompt": "creator-approved motion",
        "approved_for_video": True,
        "approved_for_animation": True,
        "animate": True,
        "status": "clip_ready",
        "duration_sec": 4.75,
        "still_qa": {"status": "pass", "pass": True},
    }

    preserved = styled_pipeline._preserved_scene_for_replan(0, previous, {0})

    assert preserved == previous
    assert preserved is not previous
    assert preserved["clip_rel"] == "clips/b00.mp4"
    assert styled_pipeline._preserved_scene_for_replan(1, previous, {0}) is None


def test_expand_hardening_does_not_rewrite_preserved_scene_metadata():
    previous = {
        "index": 0,
        "sid": "b00",
        "still_rel": "stills/b00.png",
        "clip_rel": "clips/b00.mp4",
        "prompt": "creator-approved prompt",
        "motion_prompt": "creator-approved motion with exact punctuation!!!",
        "approved_for_video": True,
        "approved_for_animation": True,
        "animate": True,
        "status": "clip_ready",
        "duration_sec": 4.75,
    }

    hardened = harden_planned_scenes_for_expand(
        [previous],
        topic="Skeleton psychology short",
        animate_policy="all",
    )

    assert hardened == [previous]
    assert hardened[0] is not previous


def test_worker_animates_only_explicit_new_scene_indices(tmp_path, monkeypatch):
    workspace = _proof_workspace(tmp_path)
    scene_state = json.loads((workspace / "scenes.json").read_text(encoding="utf-8"))
    animated = []

    @contextmanager
    def immediate_slot(*_args, **_kwargs):
        yield None

    def fake_plan_scenes(**_kwargs):
        scene_state[:] = [
            scene_state[0],
            *[
                {
                    "index": index,
                    "sid": f"b{index:02d}",
                    "still_rel": f"stills/b{index:02d}.png",
                    "clip_rel": None,
                    "approved_for_video": True,
                    "approved_for_animation": True,
                    "animate": True,
                    "status": "still_ready",
                }
                for index in range(1, 6)
            ],
        ]

    def fake_save_scenes(_workspace, scenes):
        scene_state[:] = [dict(scene) for scene in scenes]

    def fake_animate(_workspace, *, indices, tier):
        animated.extend(indices)
        for scene in scene_state:
            if scene["index"] in indices:
                scene["clip_rel"] = f"clips/b{scene['index']:02d}.mp4"

    class InlineExpandThread:
        def __init__(self, *args, **kwargs):
            self.target = kwargs.get("target")
            self.name = kwargs.get("name", "")

        def start(self):
            if self.name.startswith("expand-"):
                self.target()

    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: workspace)
    monkeypatch.setattr(tools, "production_slot", immediate_slot)
    monkeypatch.setattr(tools.threading, "Thread", InlineExpandThread)
    monkeypatch.setattr(styled_pipeline, "plan_scenes", fake_plan_scenes)
    monkeypatch.setattr(styled_pipeline, "load_scenes", lambda _workspace: scene_state)
    monkeypatch.setattr(styled_pipeline, "save_scenes", fake_save_scenes)
    monkeypatch.setattr(styled_pipeline, "animate_scenes_stage", fake_animate)
    monkeypatch.setattr(
        "studio_agent.visual_fix_contract.harden_planned_scenes_for_expand",
        lambda scenes, **_kwargs: scenes,
    )
    monkeypatch.setattr(tools, "_reconcile_shortform_costs", lambda *_args, **_kwargs: {})

    tools.expand_visual_proof_shortform(
        "proof-job",
        6,
        command_id="targeted-expand",
        existing_scene_count=1,
        preserve_scene_indices=[0],
        animate_scene_indices=[2, 4],
        animate_policy="all",
    )

    assert animated == [2, 4]
    assert scene_state[0]["clip_rel"] == "clips/b00.mp4"
    assert scene_state[0]["status"] == "clip_ready"
    assert [scene["index"] for scene in scene_state if scene.get("approved_for_animation") and scene["index"] > 0] == [2, 4]
