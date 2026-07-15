from __future__ import annotations

import pytest

from studio_agent.command_execution import FileExecutionLedger
from studio_agent import idempotent_mutations


def test_local_mutation_replays_completed_result_without_second_claim(tmp_path, monkeypatch):
    monkeypatch.setattr(idempotent_mutations, "_LEDGER", FileExecutionLedger(tmp_path))
    arguments = {"job_id": "job-1", "scene_index": 2, "_runpod_command_id": "same-key"}

    claim, replay = idempotent_mutations.begin(
        tool_name="regenerate_production_scene",
        arguments=arguments,
        command_id="same-key",
        user_id="user-1",
    )
    assert claim is not None and replay is None
    idempotent_mutations.complete(claim, {"ok": True, "job_id": "job-1"})

    duplicate_claim, duplicate = idempotent_mutations.begin(
        tool_name="regenerate_production_scene",
        arguments=arguments,
        command_id="same-key",
        user_id="user-1",
    )
    assert duplicate_claim is None
    assert duplicate and duplicate["ok"] is True
    assert duplicate["job_id"] == "job-1"
    assert duplicate["idempotent_replay"] is True
    assert str(duplicate["duplicate_of"]).startswith("exec_")


def test_local_mutation_rejects_reused_key_with_different_contract(tmp_path, monkeypatch):
    monkeypatch.setattr(idempotent_mutations, "_LEDGER", FileExecutionLedger(tmp_path))
    claim, _ = idempotent_mutations.begin(
        tool_name="regenerate_production_scene",
        arguments={"job_id": "job-1", "scene_index": 1},
        command_id="reused-key",
        user_id="user-1",
    )
    assert claim is not None
    idempotent_mutations.complete(claim, {"ok": True})

    with pytest.raises(RuntimeError, match="different production mutation"):
        idempotent_mutations.begin(
            tool_name="regenerate_production_scene",
            arguments={"job_id": "job-1", "scene_index": 9},
            command_id="reused-key",
            user_id="user-1",
        )


def test_local_mutation_pending_claim_fails_closed(tmp_path, monkeypatch):
    ledger = FileExecutionLedger(tmp_path)
    monkeypatch.setattr(idempotent_mutations, "_LEDGER", ledger)
    first, _ = idempotent_mutations.begin(
        tool_name="finalize_production",
        arguments={"job_id": "job-1"},
        command_id="pending-key",
        user_id="user-1",
    )
    assert first is not None

    second, replay = idempotent_mutations.begin(
        tool_name="finalize_production",
        arguments={"job_id": "job-1"},
        command_id="pending-key",
        user_id="user-1",
    )
    assert second is None
    assert replay and replay["status"] == "claim_pending"
    assert replay["idempotent_replay"] is True
