from __future__ import annotations

import threading
import time

import pytest

import unified_credits
from studio_agent import jobs, runpod_bridge, runpod_reconciliation, runpod_storage


@pytest.fixture(autouse=True)
def _isolated_ledgers(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("RUNPOD_RECONCILIATION_LEDGER_DIR", str(tmp_path / "reconciliation"))
    monkeypatch.setenv("RUNPOD_DISPATCH_LEDGER_DIR", str(tmp_path / "dispatch"))


def _receipt() -> dict:
    return {
        "ok": True,
        "status": "accepted",
        "dispatch_id": "rpd_" + "a" * 40,
        "studio_job_id": "studio-short-1",
        "runpod_job_id": "runpod-job-1",
        "endpoint_id": "endpoint-1",
    }


def _terminal_status(*, failed: bool = False, provider_usd: str = "0.25") -> dict:
    output = {
        "ok": not failed,
        "dispatch_id": "rpd_" + "a" * 40,
        "job_snapshot": {
            "job_id": "worker-reported-id",
            "status": "failed" if failed else "awaiting_animation_review",
            "stage": "error" if failed else "animation_review",
            "progress": 73,
            "video_url": "/api/local-worker-file-that-is-not-on-fly.mp4",
        },
        "billing": {
            "report_complete": True,
            "provider_cost_facts": [
                {
                    "kind": "provider_usd",
                    "dispatch_id": "rpd_" + "a" * 40,
                    "provider_usd_decimal": provider_usd,
                    "user_id": "user-1",
                    "authoritative": True,
                    "operation_scoped": True,
                },
                {
                    "kind": "reservation_commit",
                    "dispatch_id": "rpd_" + "a" * 40,
                    "user_id": "user-1",
                    "reservation_id": "res-control-plane-1",
                    "ts": 2,
                },
            ]
        },
    }
    return {
        "ok": not failed,
        "status": "failed" if failed else "complete",
        "stage": "error" if failed else "complete",
        "stage_label": "failed" if failed else "complete",
        "running": False,
        "terminal": True,
        "progress": 100,
        "runpod_status": "FAILED" if failed else "COMPLETED",
        "runpod_job_id": "runpod-job-1",
        "output": output,
        "job_snapshot": output["job_snapshot"],
    }


def test_repeated_job_polls_are_pure_and_backend_reconciliation_commits_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", "true")
    monkeypatch.setattr(
        jobs,
        "_shortform_status",
        lambda _job_id: pytest.fail("RunPod-owned polling must not restart local production"),
    )
    monkeypatch.setattr(runpod_bridge, "get_dispatch_receipt_by_studio_job_id", lambda _job_id: _receipt())
    monkeypatch.setattr(
        runpod_storage,
        "sync_job_workspace",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "sync_pending",
            "pending": True,
            "files_downloaded": 0,
        },
    )
    methods: list[str] = []

    def get_status(runpod_job_id: str, **_kwargs):
        assert runpod_job_id == "runpod-job-1"
        methods.append("GET")
        return _terminal_status()

    commits: list[tuple] = []
    releases: list[tuple] = []
    monkeypatch.setattr(runpod_bridge, "get_runpod_job_status", get_status)
    monkeypatch.setattr(unified_credits, "usd_to_credits", lambda amount: 37)
    monkeypatch.setattr(
        unified_credits,
        "settle_reservation",
        lambda *args, **kwargs: commits.append((args, kwargs)) or {"credits_charged": 37},
    )
    monkeypatch.setattr(
        unified_credits,
        "release_reservation",
        lambda *args, **kwargs: releases.append((args, kwargs)) or {},
    )

    first = jobs.get_job_snapshot("studio-short-1", "shortform", lightweight=True)
    second = jobs.get_job_snapshot("studio-short-1", "shortform", lightweight=True)

    assert methods == ["GET", "GET"]
    assert commits == []
    assert releases == []
    assert first["runpod"]["billing_reconciliation"]["state"] == "background_reconciliation_pending"
    assert second["runpod"]["billing_reconciliation"]["state"] == "background_reconciliation_pending"

    maintenance_first = runpod_reconciliation.project_runpod_job_snapshot(
        "studio-short-1",
        "shortform",
        {"job_id": "studio-short-1", "kind": "shortform"},
        reconcile=True,
    )
    maintenance_second = runpod_reconciliation.project_runpod_job_snapshot(
        "studio-short-1",
        "shortform",
        {"job_id": "studio-short-1", "kind": "shortform"},
        reconcile=True,
    )

    assert methods == ["GET", "GET", "GET", "GET"]
    assert len(commits) == 1
    assert releases == []
    assert commits[0][0] == ("user-1", "res-control-plane-1")
    assert commits[0][1]["actual_credits"] == 37
    assert first["job_id"] == "studio-short-1"
    assert first["kind"] == "shortform"
    assert first["status"] == "awaiting_animation_review"
    assert first["runpod"]["artifacts_local"] is False
    assert "video_url" not in first
    assert first["runpod"]["worker_job_snapshot"]["video_url"].startswith("/api/")
    assert maintenance_first["runpod"]["billing_reconciliation"]["state"] == "settled"
    assert maintenance_second["runpod"]["billing_reconciliation"]["idempotent_replay"] is True


def test_terminal_workspace_sync_enables_local_artifact_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", "1")
    monkeypatch.setattr(runpod_bridge, "get_dispatch_receipt_by_studio_job_id", lambda _job_id: _receipt())
    monkeypatch.setattr(runpod_bridge, "get_runpod_job_status", lambda *_args, **_kwargs: _terminal_status())
    monkeypatch.setattr(
        runpod_storage,
        "sync_job_workspace",
        lambda job_id, kind, dispatch_id: {
            "ok": True,
            "status": "synced",
            "pending": False,
            "job_id": job_id,
            "kind": kind,
            "dispatch_id": dispatch_id,
            "files_downloaded": 7,
        },
    )
    monkeypatch.setattr(
        jobs,
        "_shortform_status",
        lambda job_id: {
            "job_id": job_id,
            "kind": "shortform",
            "status": "awaiting_approval",
            "still_preview_urls": [f"/api/studio-agent/jobs/{job_id}/still/0"],
            "video_url": f"/api/studio-agent/jobs/{job_id}/video",
        },
    )
    monkeypatch.setattr(
        runpod_reconciliation,
        "reconcile_terminal_billing",
        lambda *_args, **_kwargs: {"state": "committed"},
    )

    result = runpod_reconciliation.project_runpod_job_snapshot(
        "studio-short-1",
        "shortform",
        {"job_id": "studio-short-1", "kind": "shortform"},
        reconcile=True,
    )

    assert result["job_id"] == "studio-short-1"
    assert result["runpod"]["artifacts_local"] is True
    assert result["runpod"]["workspace_sync"]["files_downloaded"] == 7
    assert result["video_url"].endswith("/video")
    assert result["still_preview_urls"]
    # The worker remains authoritative for progress/review state even after
    # local artifact URLs become available.
    assert result["status"] == "awaiting_animation_review"


def test_failed_terminal_job_commits_actual_spend_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    releases: list[tuple] = []
    commits: list[tuple] = []
    monkeypatch.setattr(
        unified_credits,
        "release_reservation",
        lambda *args, **kwargs: releases.append((args, kwargs)) or {},
    )
    monkeypatch.setattr(
        unified_credits,
        "settle_reservation",
        lambda *args, **kwargs: commits.append((args, kwargs)) or {},
    )

    first = runpod_reconciliation.reconcile_terminal_billing(_terminal_status(failed=True), _receipt())
    second = runpod_reconciliation.reconcile_terminal_billing(_terminal_status(failed=True), _receipt())

    assert releases == []
    assert len(commits) == 1
    assert commits[0][0] == ("user-1", "res-control-plane-1")
    assert first["state"] == "settled"
    assert second["idempotent_replay"] is True


def test_failed_terminal_job_with_zero_spend_releases_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    releases: list[tuple] = []
    monkeypatch.setattr(
        unified_credits,
        "release_reservation",
        lambda *args, **kwargs: releases.append((args, kwargs)) or {},
    )
    monkeypatch.setattr(
        unified_credits,
        "settle_reservation",
        lambda *_args, **_kwargs: pytest.fail("zero-spend failure was charged"),
    )

    result = runpod_reconciliation.reconcile_terminal_billing(
        _terminal_status(failed=True, provider_usd="0"), _receipt()
    )

    assert result["state"] == "released"
    assert len(releases) == 1


def test_no_spend_releases_reservation(monkeypatch: pytest.MonkeyPatch) -> None:
    releases: list[tuple] = []
    monkeypatch.setattr(
        unified_credits,
        "release_reservation",
        lambda *args, **kwargs: releases.append((args, kwargs)) or {},
    )
    monkeypatch.setattr(
        unified_credits,
        "settle_reservation",
        lambda *_args, **_kwargs: pytest.fail("zero spend was committed"),
    )

    result = runpod_reconciliation.reconcile_terminal_billing(
        _terminal_status(provider_usd="0"), _receipt()
    )

    assert result["state"] == "released"
    assert len(releases) == 1


def test_terminal_without_worker_billing_retains_control_plane_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status = _terminal_status(failed=True, provider_usd="0")
    status.pop("output", None)
    receipt = {
        **_receipt(),
        "billing_user_id": "user-1",
        "credit_reservation_id": "res-control-plane-1",
    }
    monkeypatch.setattr(
        unified_credits,
        "release_reservation",
        lambda *_args, **_kwargs: pytest.fail("unknown billing was released"),
    )
    monkeypatch.setattr(
        unified_credits,
        "settle_reservation",
        lambda *_args, **_kwargs: pytest.fail("unknown billing was committed"),
    )

    result = runpod_reconciliation.reconcile_terminal_billing(status, receipt)

    assert result["state"] == "worker_billing_report_incomplete"
    assert result["fail_closed"] is True
    assert result["wallet_mutated"] is False


def test_empty_worker_billing_report_retains_control_plane_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status = _terminal_status()
    status["output"]["billing"] = {
        "report_complete": True,
        "provider_cost_facts": [],
    }
    receipt = {
        **_receipt(),
        "billing_user_id": "user-1",
        "credit_reservation_id": "res-control-plane-1",
    }
    monkeypatch.setattr(
        unified_credits,
        "release_reservation",
        lambda *_args, **_kwargs: pytest.fail("empty billing released the hold"),
    )
    monkeypatch.setattr(
        unified_credits,
        "settle_reservation",
        lambda *_args, **_kwargs: pytest.fail("empty billing settled the hold"),
    )

    result = runpod_reconciliation.reconcile_terminal_billing(status, receipt)

    assert result["state"] == "worker_billing_report_incomplete"
    assert result["fail_closed"] is True


def test_verified_actual_over_hold_uses_atomic_settlement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settlements: list[tuple] = []
    monkeypatch.setattr(unified_credits, "usd_to_credits", lambda _amount: 250)
    monkeypatch.setattr(
        unified_credits,
        "settle_reservation",
        lambda *args, **kwargs: settlements.append((args, kwargs))
        or {"credits_charged": 250, "overage_credits": 100},
    )

    result = runpod_reconciliation.reconcile_terminal_billing(
        _terminal_status(provider_usd="2.50"), _receipt()
    )

    assert settlements[0][0] == ("user-1", "res-control-plane-1")
    assert settlements[0][1]["actual_credits"] == 250
    assert result["credits_charged"] == 250
    assert result["overage_credits"] == 100


def test_cumulative_cost_snapshot_without_operation_fact_retains_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status = _terminal_status()
    facts = status["output"]["billing"]["provider_cost_facts"]
    facts[:] = [
        {
            "kind": "job_cost_snapshot",
            "dispatch_id": "rpd_" + "a" * 40,
            "cost": {"total_usd": "0.42"},
        },
        {
            "kind": "reservation_commit",
            "dispatch_id": "rpd_" + "a" * 40,
            "user_id": "user-1",
            "reservation_id": "res-control-plane-1",
        },
    ]
    monkeypatch.setattr(
        unified_credits,
        "settle_reservation",
        lambda *_args, **_kwargs: pytest.fail("cumulative snapshot was charged"),
    )

    result = runpod_reconciliation.reconcile_terminal_billing(status, _receipt())

    assert result["state"] == "worker_billing_report_incomplete"
    assert result["fail_closed"] is True


def test_enabled_without_receipt_stays_local_and_never_polls_runpod(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", "1")
    monkeypatch.setattr(
        jobs,
        "_shortform_status",
        lambda _job_id: {"job_id": _job_id, "status": "running", "progress": 12},
    )
    monkeypatch.setattr(runpod_bridge, "get_dispatch_receipt_by_studio_job_id", lambda _job_id: None)
    monkeypatch.setattr(
        runpod_bridge,
        "get_runpod_job_status",
        lambda *_args, **_kwargs: pytest.fail("no receipt must make zero RunPod network calls"),
    )

    result = jobs.get_job_snapshot("local-only", "shortform", lightweight=True)

    assert result["status"] == "running"
    assert "runpod_job_id" not in result


def test_disabled_path_does_not_even_lookup_receipt_or_touch_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", "0")
    monkeypatch.setattr(
        jobs,
        "_shortform_status",
        lambda _job_id: {"job_id": _job_id, "status": "running", "progress": 12},
    )
    monkeypatch.setattr(
        runpod_bridge,
        "get_dispatch_receipt_by_studio_job_id",
        lambda _job_id: pytest.fail("disabled path touched receipt ledger"),
    )
    monkeypatch.setattr(
        runpod_bridge,
        "get_runpod_job_status",
        lambda *_args, **_kwargs: pytest.fail("disabled path touched RunPod network"),
    )

    result = jobs.get_job_snapshot("local-only", "shortform", lightweight=True)

    assert result["job_id"] == "local-only"
    assert result["status"] == "running"


def test_concurrent_reconciliation_has_one_wallet_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mutations: list[str] = []
    guard = threading.Lock()

    def commit(*_args, **_kwargs):
        with guard:
            mutations.append("commit")
        time.sleep(0.03)
        return {}

    monkeypatch.setattr(unified_credits, "usd_to_credits", lambda _amount: 25)
    monkeypatch.setattr(unified_credits, "settle_reservation", commit)
    outputs: list[dict] = []

    def call() -> None:
        outputs.append(
            runpod_reconciliation.reconcile_terminal_billing(_terminal_status(), _receipt())
        )

    threads = [threading.Thread(target=call) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert mutations == ["commit"]
    assert any(item["state"] == "settled" for item in outputs)
    assert all(item["state"] in {"settled", "reconciliation_in_progress_or_unknown"} for item in outputs)


def test_status_failure_is_annotated_without_breaking_local_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STUDIO_RUNPOD_PRODUCTION_ENABLED", "yes")
    monkeypatch.setattr(
        jobs,
        "_shortform_status",
        lambda _job_id: {"job_id": _job_id, "status": "running", "progress": 15},
    )
    monkeypatch.setattr(runpod_bridge, "get_dispatch_receipt_by_studio_job_id", lambda _job_id: _receipt())
    monkeypatch.setattr(
        runpod_bridge,
        "get_runpod_job_status",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("temporary 503")),
    )

    result = jobs.get_job_snapshot("studio-short-1", "shortform", lightweight=True)

    # A known RunPod-owned job fails closed instead of invoking the local
    # short-form status helper, which can restart stale production.
    assert result["status"] == "queued"
    assert result["runpod"]["status"] == "status_unavailable"
    assert result["runpod"]["billing_reconciliation"]["state"] == "status_unavailable"
