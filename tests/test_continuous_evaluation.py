from __future__ import annotations

import json

from studio_agent import continuous_evaluation


CHANNEL_REGRESSION = "claimed channel analytics/performance evidence without a channel-data tool"


def _isolate(monkeypatch, tmp_path) -> None:
    eval_dir = tmp_path / "continuous_evaluation"
    monkeypatch.setattr(continuous_evaluation, "EVAL_DIR", eval_dir)
    monkeypatch.setattr(continuous_evaluation, "EVENTS_PATH", eval_dir / "events.jsonl")
    monkeypatch.setattr(continuous_evaluation, "CASES_PATH", eval_dir / "regression_cases.json")
    monkeypatch.setattr(continuous_evaluation, "SUMMARY_PATH", eval_dir / "summary.json")


def _promote(monkeypatch, tmp_path) -> str:
    _isolate(monkeypatch, tmp_path)
    for _ in range(2):
        continuous_evaluation.record_evidence(
            session={"session_id": "session-test"},
            event_type="agent_guard",
            outcome="failure",
            evidence={"failure": CHANNEL_REGRESSION},
        )
    cases = json.loads(continuous_evaluation.CASES_PATH.read_text(encoding="utf-8"))
    regression_id = next(iter(cases))
    assert cases[regression_id]["status"] == "promoted"
    return regression_id


def test_known_channel_evidence_regression_verifier_is_deterministic():
    result = continuous_evaluation.verify_known_regression(CHANNEL_REGRESSION)

    assert result["ok"] is True
    assert result["checks"] == {
        "blocked_without_channel_tool": True,
        "accepted_with_channel_tool": True,
    }


def test_promoted_regression_requires_passing_verification(monkeypatch, tmp_path):
    regression_id = _promote(monkeypatch, tmp_path)

    rejected = continuous_evaluation.resolve_regression(
        regression_id,
        verifier="test",
        verification={"ok": False},
    )

    assert rejected["ok"] is False
    assert rejected["reason"] == "verification_failed"
    assert continuous_evaluation.refresh_summary()["release_ready"] is False


def test_known_regression_resolution_is_audited_and_reopens(monkeypatch, tmp_path):
    regression_id = _promote(monkeypatch, tmp_path)

    resolved = continuous_evaluation.resolve_known_regression(regression_id)

    assert resolved["ok"] is True
    assert resolved["summary"]["release_ready"] is True
    cases = json.loads(continuous_evaluation.CASES_PATH.read_text(encoding="utf-8"))
    assert cases[regression_id]["status"] == "resolved"
    assert cases[regression_id]["resolution"]["verifier"] == "channel_evidence_guard_v1"

    events = [
        json.loads(line)
        for line in continuous_evaluation.EVENTS_PATH.read_text(encoding="utf-8").splitlines()
    ]
    assert events[-1]["event_type"] == "regression_resolution"
    assert events[-1]["evidence"]["regression_id"] == regression_id

    reopened = continuous_evaluation.record_evidence(
        session={"session_id": "session-later"},
        event_type="agent_guard",
        outcome="failure",
        evidence={"failure": CHANNEL_REGRESSION},
    )
    assert reopened["release_ready"] is False
    assert reopened["promoted_regressions"] == 1


def test_resolution_stays_promoted_when_audit_event_cannot_persist(monkeypatch, tmp_path):
    regression_id = _promote(monkeypatch, tmp_path)
    monkeypatch.setattr(continuous_evaluation, "_append_event", lambda _row: False)

    result = continuous_evaluation.resolve_known_regression(regression_id)

    assert result == {
        "ok": False,
        "reason": "event_write_failed",
        "regression_id": regression_id,
    }
    cases = json.loads(continuous_evaluation.CASES_PATH.read_text(encoding="utf-8"))
    assert cases[regression_id]["status"] == "promoted"
    assert continuous_evaluation.refresh_summary()["release_ready"] is False
