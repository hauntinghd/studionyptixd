from __future__ import annotations

import json
from pathlib import Path

from skeleton_ai import pipeline, prompt_compose
from skeleton_ai.canonical_edit import build_scene_edit_prompt
from studio_agent import catalyst_still_audit, visual_qa


def test_catalyst_propagates_job_cast_count_to_scene_six_still_qa(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A two-host Scene 6 must never be audited with the one-host default."""
    workspace = tmp_path / "two-host-short"
    still = workspace / "stills" / "b05.png"
    still.parent.mkdir(parents=True)
    still.write_bytes(b"candidate-still")
    (workspace / "job_spec.json").write_text(
        json.dumps({
            "render_style": "skeleton_anatomical",
            "cast_count": 2,
            "topic": "Why he ghosted her",
        }),
        encoding="utf-8",
    )
    (workspace / "scenes.json").write_text(
        json.dumps([{
            "index": 5,
            "sid": "b05",
            "narration": "She finally understands why he pulled away.",
            "scene_action": "Rainy cafe window; both skeleton hosts face each other across the table.",
            "outfit": "no clothing",
            "still_rel": "stills/b05.png",
        }]),
        encoding="utf-8",
    )
    seen: list[int] = []

    def fake_audit(*_args, **kwargs):
        seen.append(int(kwargs.get("cast_count") or 1))
        return {"status": "pass", "pass": True, "issues": []}

    monkeypatch.setattr(visual_qa, "audit_skeleton_still", fake_audit)
    monkeypatch.setattr(visual_qa, "_workspace_skeleton_reference", lambda _workspace: None)
    monkeypatch.setattr(catalyst_still_audit, "_load_channel_memory", lambda _key: {})

    report = catalyst_still_audit.audit_scene_still(workspace, 5)

    assert seen == [2]
    assert report["cast_count"] == 2
    assert "one host" not in report["fix_instruction"].lower()


def test_still_semantic_cache_identity_includes_cast_count(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A cached one-host verdict cannot poison a later two-host Scene 6 audit."""
    workspace = tmp_path / "two-host-short"
    still = workspace / "stills" / "b05.png"
    reference = workspace / "master.png"
    still.parent.mkdir(parents=True)
    still.write_bytes(b"candidate")
    reference.write_bytes(b"reference")
    (workspace / "job_spec.json").write_text(
        json.dumps({"cast_count": 2, "topic": "Why he ghosted her"}),
        encoding="utf-8",
    )
    (workspace / "scenes.json").write_text(
        json.dumps([{
            "index": 5,
            "sid": "b05",
            "still_rel": "stills/b05.png",
            "narration": "She understands why he pulled away.",
            "scene_action": "Two skeleton hosts face each other in a rainy cafe.",
        }]),
        encoding="utf-8",
    )
    prompts: list[str] = []

    def fake_qa_jpeg(_source: Path, target: Path) -> Path:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"jpeg")
        return target

    def fake_vision(_paths, *, prompt: str):
        prompts.append(prompt)
        two_hosts = "exactly TWO" in prompt
        return {
            "provider": "test",
            "model": "deterministic",
            "parsed": {
                "pass": two_hosts,
                "confidence": 0.99,
                "summary": "two-host contract" if two_hosts else "expected exactly one host",
                "layout_artifact": False,
            },
        }

    monkeypatch.setattr(visual_qa, "_qa_jpeg", fake_qa_jpeg)
    monkeypatch.setattr(visual_qa, "_run_semantic_vision", fake_vision)

    one_host = visual_qa.audit_skeleton_still(
        still,
        reference=reference,
        cast_count=1,
    )
    two_hosts = visual_qa.audit_skeleton_still(
        still,
        reference=reference,
    )

    assert one_host["status"] == "fail"
    assert two_hosts["status"] == "pass"
    assert two_hosts["cast_count"] == 2
    assert len(prompts) == 2
    assert one_host["fingerprint"] != two_hosts["fingerprint"]


def test_clip_semantic_cache_identity_includes_cast_count(tmp_path: Path) -> None:
    clip = tmp_path / "b05.mp4"
    still = tmp_path / "b05.png"
    clip.write_bytes(b"clip")
    still.write_bytes(b"still")

    one_host = visual_qa._semantic_fingerprint(clip, still, "no clothing", cast_count=1)
    two_hosts = visual_qa._semantic_fingerprint(clip, still, "no clothing", cast_count=2)

    assert one_host != two_hosts


def test_dual_host_compaction_keeps_specific_location_and_action() -> None:
    raw = (
        f"{prompt_compose.dual_host_scene_prefix()} "
        f"{prompt_compose.dual_host_staging_brief()}. "
        "Rainy cafe window booth at dusk; the left host offers an apology while the right host turns toward the exit."
    )

    compact = prompt_compose.compact_skeleton_scene_direction(raw, max_chars=150)
    provider_prompt = prompt_compose.compose_skeleton_still_prompt(
        visual_description=raw,
        outfit="no clothing",
        cast_count=2,
    )

    assert "Rainy cafe window booth" in compact
    assert "left host offers an apology" in compact
    assert "Rainy cafe window booth" in provider_prompt
    assert "two hosts" in provider_prompt
    assert "one host" not in provider_prompt


class _PlannerStub:
    def __init__(self) -> None:
        self.system_prompts: list[str] = []

    def complete(self, system: str, _user: str, **_kwargs) -> str:
        self.system_prompts.append(system)
        return json.dumps({
            "outfit": "no clothing",
            "scene_action": "Rainy cafe window booth; left leans forward while right turns toward the exit.",
            "motion_prompt": "Left opens both hands while right draws back; slow camera push-in.",
            "bare_torso": False,
        })


def test_scene_planner_and_provider_contract_honor_explicit_cast_count() -> None:
    planner = _PlannerStub()
    _outfit, dual_action, dual_motion = pipeline.derive_beat_visuals(
        planner,
        "He overwhelmed her with attention, then pulled away.",
        "Relationship psychology",
        plan={"characters": {}, "fallback_outfit": "no clothing"},
        cast_count=2,
    )
    dual_system = planner.system_prompts[-1]
    dual_prompt = build_scene_edit_prompt(
        topic="Relationship psychology",
        narration="He overwhelmed her with attention, then pulled away.",
        visual_description=dual_action,
        outfit="no clothing",
        cast_count=2,
    )

    assert "exactly TWO identical canonical ivory skeleton hosts" in dual_system
    assert "Exactly four hands total" in dual_system
    assert "two hosts" in dual_prompt
    assert "one host" not in dual_prompt
    assert "Two hosts only" in dual_motion

    _outfit, single_action, single_motion = pipeline.derive_beat_visuals(
        planner,
        "He overwhelmed her with attention, then pulled away.",
        "Relationship psychology",
        plan={"characters": {}, "fallback_outfit": "no clothing"},
        cast_count=1,
    )
    single_system = planner.system_prompts[-1]
    single_prompt = build_scene_edit_prompt(
        topic="Relationship psychology",
        narration="He overwhelmed her with attention, then pulled away.",
        visual_description=single_action,
        outfit="no clothing",
        cast_count=1,
    )

    assert "exactly ONE canonical ivory skeleton host" in single_system
    assert "Exactly two hands total" in single_system
    assert "one host" in single_prompt
    assert "two hosts" not in single_prompt
    assert "One host only" in single_motion
