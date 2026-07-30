"""A creator must never pay for a video that does not exist.

Studio already refunded the *unused* repair reserve on a failed production, but
the spend already consumed stayed charged. A live canary proved the cost of
that: $2.72 of provider spend billed for a job that died at beat one and
delivered nothing.

The consumed spend is real, but it is the house's cost of a failed render, not
the creator's purchase. `absorb_terminal_failure_costs` watermarks it as billed
at zero credits and returns the hold in full.

The delivery check is what keeps this honest in the other direction: a failure
*after* a video exists (a repair, an edit, a re-render) is not terminal, and
that spend stays billable.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from studio_agent import production_costs, tools


@pytest.fixture()
def workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    job_id = "job_terminal_test"
    root = tmp_path / "out"
    ws = root / job_id
    ws.mkdir(parents=True)
    monkeypatch.setattr(tools, "_shortform_workspace", lambda _jid: ws)
    return ws


def _spend(ws: Path, usd: float) -> None:
    production_costs.record_event(
        ws,
        stage="stills",
        provider="fal",
        operation="seedream_edit",
        usd=usd,
        quantity=1,
        unit="image",
    )


def _deliver(ws: Path, name: str = "final.mp4") -> None:
    video = ws / name
    video.write_bytes(b"\x00" * 2048)
    (ws / "result.json").write_text(json.dumps({"video_path": name}), encoding="utf-8")


def test_no_result_file_means_nothing_was_delivered(workspace: Path) -> None:
    assert tools.shortform_delivery_exists(workspace) is False


def test_a_result_pointing_at_a_missing_file_is_not_a_delivery(workspace: Path) -> None:
    """The pipeline writes result.json incrementally; a path is not a video."""
    (workspace / "result.json").write_text(
        json.dumps({"video_path": "final.mp4"}), encoding="utf-8"
    )
    assert tools.shortform_delivery_exists(workspace) is False


def test_a_zero_byte_video_is_not_a_delivery(workspace: Path) -> None:
    (workspace / "final.mp4").write_bytes(b"")
    (workspace / "result.json").write_text(
        json.dumps({"video_path": "final.mp4"}), encoding="utf-8"
    )
    assert tools.shortform_delivery_exists(workspace) is False


def test_a_real_video_is_a_delivery(workspace: Path) -> None:
    _deliver(workspace)
    assert tools.shortform_delivery_exists(workspace) is True


def test_absorbing_leaves_nothing_pending_and_charges_nothing(workspace: Path) -> None:
    _spend(workspace, 2.72)
    assert production_costs.pending_billable_usd(workspace) > 0

    outcome = tools.absorb_terminal_failure_costs(
        "user-1", "job_terminal_test", reservation_payload={}, reason="test_terminal"
    )

    assert outcome["charged"] == 0
    assert outcome["terminal_failure"] is True
    assert float(outcome["absorbed_usd_decimal"]) == pytest.approx(2.72)
    assert production_costs.pending_billable_usd(workspace) == 0


def test_absorbing_twice_does_not_absorb_twice(workspace: Path) -> None:
    """Worker restarts and settle replays must not double-refund."""
    _spend(workspace, 1.50)
    first = tools.absorb_terminal_failure_costs(
        "user-1", "job_terminal_test", reservation_payload={}, reason="test_terminal"
    )
    second = tools.absorb_terminal_failure_costs(
        "user-1", "job_terminal_test", reservation_payload={}, reason="test_terminal"
    )

    assert float(first["absorbed_usd_decimal"]) == pytest.approx(1.50)
    assert float(second["absorbed_usd_decimal"]) == 0.0
    assert second["charged"] == 0


def test_absorbed_spend_is_recorded_as_the_houses_cost(workspace: Path) -> None:
    """Absorbing must not silently erase the cost - it still has to be auditable."""
    _spend(workspace, 0.98)
    tools.absorb_terminal_failure_costs(
        "user-1", "job_terminal_test", reservation_payload={}, reason="test_terminal"
    )

    state = production_costs.load_billing_state(workspace)
    rows = [row for row in (state.get("charges") or []) if row]
    assert rows, f"no billing rows recorded: {state}"
    absorbed = [row for row in rows if (row.get("metadata") or {}).get("absorbed_by_house")]
    assert absorbed, f"absorption was not recorded: {rows}"
    assert int(absorbed[-1].get("credits") or 0) == 0
    # The provider spend itself is untouched - only the billable watermark moved.
    summary = production_costs.load_summary(workspace)
    assert float(summary.get("total_usd") or 0) == pytest.approx(0.98)


def test_a_delivered_job_is_not_treated_as_terminal(workspace: Path) -> None:
    """A repair that fails after delivery must keep its spend billable."""
    _spend(workspace, 0.49)
    _deliver(workspace)
    assert tools.shortform_delivery_exists(workspace) is True
    assert production_costs.pending_billable_usd(workspace) > 0
