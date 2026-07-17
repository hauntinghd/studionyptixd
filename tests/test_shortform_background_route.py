from __future__ import annotations

import json

from studio_agent import store, tools


def test_stale_background_route_quarantines_new_media_and_restores_scene_state(tmp_path) -> None:
    workspace = tmp_path / "job-one"
    (workspace / "stills").mkdir(parents=True)
    original_scenes = [{"index": 0, "still_rel": "stills/b00.png"}]
    (workspace / "scenes.json").write_text(json.dumps(original_scenes), encoding="utf-8")
    (workspace / "stills" / "b00.png").write_bytes(b"approved-proof")
    snapshot = tools._capture_shortform_background_state(workspace)

    (workspace / "scenes.json").write_text(
        json.dumps([*original_scenes, {"index": 1, "still_rel": "stills/b01.png"}]),
        encoding="utf-8",
    )
    (workspace / "stills" / "b01.png").write_bytes(b"stale-provider-result")

    quarantined = tools._rollback_stale_shortform_route(
        workspace,
        snapshot,
        command_id="expand-command",
        revision=3,
        stage="image",
    )

    assert json.loads((workspace / "scenes.json").read_text(encoding="utf-8")) == original_scenes
    assert (workspace / "stills" / "b00.png").read_bytes() == b"approved-proof"
    assert not (workspace / "stills" / "b01.png").exists()
    assert len(quarantined) == 1
    assert (workspace / quarantined[0]).read_bytes() == b"stale-provider-result"


def test_background_route_rejects_picker_revision_or_new_gate_owner(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path / "sessions")
    session = store.create_session(
        user_id="route-user",
        model="test-model",
        image_model="seedream_edit",
        video_model="seedance",
    )
    session_id = session["session_id"]
    expected = store.media_route_snapshot(session)

    assert tools._shortform_background_route_is_current(
        session_id=session_id,
        command_id="command-one",
        job_id="job-one",
        expected=expected,
        stage="image",
    )

    store.update_session(session_id, image_model="seedream_v4")
    assert not tools._shortform_background_route_is_current(
        session_id=session_id,
        command_id="command-one",
        job_id="job-one",
        expected=expected,
        stage="image",
    )

    current = store.get_session(session_id, reconcile_jobs=False)
    assert current is not None
    current_route = store.media_route_snapshot(current)
    assert store.claim_production_gate(
        session_id,
        command_id="command-two",
        job_id="job-one",
    ) is not None
    assert not tools._shortform_background_route_is_current(
        session_id=session_id,
        command_id="command-one",
        job_id="job-one",
        expected=current_route,
        stage="image",
    )
