from __future__ import annotations

import concurrent.futures
import io
import json
import threading
import urllib.error
from typing import Any

import pytest

from studio_agent import runpod_bridge
from studio_agent.runpod_contract import (
    RUNPOD_ENVELOPE_SCHEMA,
    semantic_dispatch_id,
    verify_signed_envelope,
)


SECRET = "test-runpod-dispatch-secret-at-least-32-bytes"


@pytest.fixture(autouse=True)
def _legacy_bridge_harness(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise archived bridge invariants without reopening runtime policy."""

    monkeypatch.setattr(runpod_bridge, "assert_runpod_execution_retired", lambda: None)


class _FakeResponse:
    def __init__(self, payload: dict[str, Any], status: int = 200):
        self.payload = payload
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


def _configure(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("RUNPOD_API_KEY", "runpod-api-test-key")
    monkeypatch.setenv("RUNPOD_ENDPOINT_ID", "endpoint-test-id")
    monkeypatch.setenv("RUNPOD_DISPATCH_SECRET", SECRET)
    monkeypatch.setenv("RUNPOD_DISPATCH_LEDGER_DIR", str(tmp_path / "dispatch-ledger"))
    monkeypatch.setenv("RUNPOD_DISPATCH_CLAIM_WAIT_SEC", "2")


def test_dispatch_emits_one_signed_async_production_envelope(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _configure(monkeypatch, tmp_path)
    calls: list[Any] = []

    def fake_urlopen(request, timeout):
        calls.append(request)
        if request.get_method() == "GET":
            assert request.full_url.endswith("/health")
            return _FakeResponse(
                {
                    "jobs": {"inQueue": 0, "inProgress": 0},
                    "workers": {"ready": 0, "idle": 0, "initializing": 0},
                }
            )
        assert request.get_method() == "POST"
        assert request.full_url.endswith("/run")
        return _FakeResponse({"id": "rp-job-123", "status": "IN_QUEUE"})

    monkeypatch.setattr(runpod_bridge.urllib.request, "urlopen", fake_urlopen)
    result = runpod_bridge.dispatch_production_tool(
        "expand_visual_proof_shortform",
        {
            "job_id": "short-1",
            "scene_count": 6,
            "preserve_scene_indices": [0],
            "animate_scene_indices": [5, 2, 3, 1, 4],
            "_credit_reservation": {"reservation_id": "hold-1"},
        },
        command_id="cmd-expand-1",
        user_id="user-1",
        session_id="session-1",
        content_format="shortform",
    )

    assert result["ok"] is True
    assert result["runpod_job_id"] == "rp-job-123"
    assert result["runpod_status"] == "IN_QUEUE"
    assert result["schema"] == RUNPOD_ENVELOPE_SCHEMA
    assert result["studio_job_id"] == "short-1"
    assert len(calls) == 2
    assert sum(request.get_method() == "POST" for request in calls) == 1

    posted = json.loads(calls[1].data.decode("utf-8"))
    assert set(posted) == {"input"}
    envelope = posted["input"]
    verified = verify_signed_envelope(envelope, secret=SECRET)
    assert verified["tool"] == "expand_visual_proof_shortform"
    assert verified["dispatch_id"] == result["dispatch_id"]
    assert verified["arguments"]["_credit_reservation"]["reservation_id"] == "hold-1"


@pytest.mark.parametrize(
    "tool",
    [
        "",
        "chat",
        "poll_render_job",
        "list_production_scenes",
        "get_channel_analytics",
        "read_project_file",
        "status",
        "generate_longform_thumbnails",
    ],
)
def test_nonproduction_tools_are_rejected_before_network(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    tool: str,
) -> None:
    _configure(monkeypatch, tmp_path)
    calls: list[Any] = []
    monkeypatch.setattr(
        runpod_bridge.urllib.request,
        "urlopen",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    with pytest.raises(runpod_bridge.RunPodDispatchPolicyError):
        runpod_bridge.dispatch_production_tool(
            tool,
            {},
            command_id="cmd-read-1",
            user_id="user-1",
        )

    assert calls == []


def test_payment_required_preflight_prevents_run_submission(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _configure(monkeypatch, tmp_path)
    calls: list[Any] = []

    def payment_required(request, timeout):
        calls.append(request)
        raise urllib.error.HTTPError(
            request.full_url,
            402,
            "Payment Required",
            hdrs=None,
            fp=io.BytesIO(b'{"error":"payment_required"}'),
        )

    monkeypatch.setattr(runpod_bridge.urllib.request, "urlopen", payment_required)

    with pytest.raises(runpod_bridge.RunPodPaymentRequired) as caught:
        runpod_bridge.dispatch_production_tool(
            "start_shortform_generate",
            {"topic": "test"},
            command_id="cmd-start-1",
            user_id="user-1",
        )

    assert caught.value.status_code == 402
    assert len(calls) == 1
    assert calls[0].get_method() == "GET"
    assert calls[0].full_url.endswith("/health")


def test_backlog_safety_cap_prevents_run_submission(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _configure(monkeypatch, tmp_path)
    calls: list[Any] = []

    def healthy_but_backlogged(request, timeout):
        calls.append(request)
        return _FakeResponse(
            {
                "jobs": {"inQueue": 12, "inProgress": 1},
                "workers": {"ready": 1, "running": 1},
            }
        )

    monkeypatch.setattr(runpod_bridge.urllib.request, "urlopen", healthy_but_backlogged)

    with pytest.raises(runpod_bridge.RunPodEndpointBacklogged):
        runpod_bridge.dispatch_production_tool(
            "finalize_production",
            {"job_id": "short-1"},
            command_id="cmd-finalize-1",
            user_id="user-1",
            max_queue_depth=10,
        )

    assert len(calls) == 1
    assert calls[0].get_method() == "GET"


@pytest.mark.parametrize("invalid_cap", [0, -1])
def test_queue_cap_cannot_be_disabled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    invalid_cap: int,
) -> None:
    _configure(monkeypatch, tmp_path)
    calls: list[Any] = []
    monkeypatch.setattr(
        runpod_bridge.urllib.request,
        "urlopen",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    with pytest.raises(runpod_bridge.RunPodConfigurationError, match="at least 1"):
        runpod_bridge.preflight_runpod_endpoint(max_queue_depth=invalid_cap)

    assert calls == []


def test_global_production_lease_rejects_a_distinct_command(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _configure(monkeypatch, tmp_path)
    first = "rpd_" + "1" * 40
    second = "rpd_" + "2" * 40

    lease = runpod_bridge.acquire_production_lease(
        first, studio_job_id="job-1", tool="finalize_production"
    )
    replay = runpod_bridge.acquire_production_lease(
        first, studio_job_id="job-1", tool="finalize_production"
    )
    with pytest.raises(runpod_bridge.RunPodEndpointBacklogged):
        runpod_bridge.acquire_production_lease(
            second, studio_job_id="job-2", tool="finalize_production"
        )

    assert lease["acquired"] is True
    assert replay["acquired"] is False
    assert runpod_bridge.release_production_lease(second) is False
    assert runpod_bridge.release_production_lease(first) is True


def test_dispatch_id_is_semantic_and_ignores_runtime_noise() -> None:
    left = semantic_dispatch_id(
        "animate_production_scenes",
        {
            "job_id": "short-1",
            "scene_indices": [5, 2, 4, 3, 1],
            "_credit_reservation": {"reservation_id": "hold-a"},
            "queued_at": 100,
        },
        command_id="cmd-animate-1",
        user_id="user-1",
    )
    right = semantic_dispatch_id(
        "animate_production_scenes",
        {
            "job_id": "short-1",
            "scene_indices": [1, 2, 3, 4, 5],
            "_credit_reservation": {"reservation_id": "hold-b"},
            "queued_at": 999,
        },
        command_id="cmd-animate-1",
        user_id="user-1",
    )
    other_command = semantic_dispatch_id(
        "animate_production_scenes",
        {"job_id": "short-1", "scene_indices": [1, 2, 3, 4, 5]},
        command_id="cmd-animate-2",
        user_id="user-1",
    )

    assert left == right
    assert left.startswith("rpd_")
    assert left != other_command


def test_missing_signing_secret_is_configuration_error_before_health(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.delenv("RUNPOD_DISPATCH_SECRET")
    calls: list[Any] = []
    monkeypatch.setattr(
        runpod_bridge.urllib.request,
        "urlopen",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    with pytest.raises(runpod_bridge.RunPodConfigurationError):
        runpod_bridge.dispatch_production_tool(
            "start_shortform_generate",
            {"topic": "test"},
            command_id="cmd-start-1",
            user_id="user-1",
        )

    assert calls == []


def test_sequential_duplicate_replays_receipt_without_another_request(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _configure(monkeypatch, tmp_path)
    calls: list[Any] = []

    def fake_urlopen(request, timeout):
        calls.append(request)
        if request.get_method() == "GET":
            return _FakeResponse({"jobs": {"inQueue": 0}, "workers": {}})
        return _FakeResponse({"id": "rp-job-once", "status": "IN_QUEUE"})

    monkeypatch.setattr(runpod_bridge.urllib.request, "urlopen", fake_urlopen)
    kwargs = {
        "command_id": "cmd-once",
        "user_id": "user-1",
        "session_id": "session-1",
        "content_format": "shortform",
    }
    first = runpod_bridge.dispatch_production_tool(
        "animate_production_scenes",
        {"job_id": "short-1", "scene_indices": [1, 2, 3, 4, 5]},
        **kwargs,
    )
    second = runpod_bridge.dispatch_production_tool(
        "animate_production_scenes",
        {"job_id": "short-1", "scene_indices": [1, 2, 3, 4, 5]},
        **kwargs,
    )

    assert first["idempotent_replay"] is False
    assert second["idempotent_replay"] is True
    assert second["dispatch_id"] == first["dispatch_id"]
    assert second["runpod_job_id"] == first["runpod_job_id"] == "rp-job-once"
    assert [request.get_method() for request in calls] == ["GET", "POST"]


def test_concurrent_duplicate_issues_exactly_one_post(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _configure(monkeypatch, tmp_path)
    calls: list[Any] = []
    calls_lock = threading.Lock()
    start = threading.Barrier(2)
    post_started = threading.Event()
    finish_post = threading.Event()

    def fake_urlopen(request, timeout):
        with calls_lock:
            calls.append(request)
        if request.get_method() == "GET":
            return _FakeResponse({"jobs": {"inQueue": 0}, "workers": {}})
        post_started.set()
        assert finish_post.wait(timeout=2)
        return _FakeResponse({"id": "rp-job-concurrent", "status": "IN_QUEUE"})

    monkeypatch.setattr(runpod_bridge.urllib.request, "urlopen", fake_urlopen)

    def dispatch():
        start.wait(timeout=2)
        return runpod_bridge.dispatch_production_tool(
            "animate_production_scenes",
            {"job_id": "short-1", "scene_indices": [1, 2, 3, 4, 5]},
            command_id="cmd-concurrent",
            user_id="user-1",
            claim_wait_sec=2,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(dispatch) for _ in range(2)]
        assert post_started.wait(timeout=2)
        finish_post.set()
        results = [future.result(timeout=3) for future in futures]

    methods = [request.get_method() for request in calls]
    assert methods.count("GET") == 1
    assert methods.count("POST") == 1
    assert {result["runpod_job_id"] for result in results} == {"rp-job-concurrent"}
    assert sorted(result["idempotent_replay"] for result in results) == [False, True]


def test_preflight_failure_releases_claim_for_safe_retry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _configure(monkeypatch, tmp_path)
    calls: list[Any] = []
    first_health = True

    def fake_urlopen(request, timeout):
        nonlocal first_health
        calls.append(request)
        if request.get_method() == "GET" and first_health:
            first_health = False
            raise urllib.error.HTTPError(
                request.full_url,
                402,
                "Payment Required",
                hdrs=None,
                fp=io.BytesIO(b'{"error":"payment_required"}'),
            )
        if request.get_method() == "GET":
            return _FakeResponse({"jobs": {"inQueue": 0}, "workers": {}})
        return _FakeResponse({"id": "rp-job-after-retry", "status": "IN_QUEUE"})

    monkeypatch.setattr(runpod_bridge.urllib.request, "urlopen", fake_urlopen)
    kwargs = {"command_id": "cmd-retry", "user_id": "user-1"}
    with pytest.raises(runpod_bridge.RunPodPaymentRequired):
        runpod_bridge.dispatch_production_tool(
            "start_shortform_generate",
            {"topic": "skeleton"},
            **kwargs,
        )

    result = runpod_bridge.dispatch_production_tool(
        "start_shortform_generate",
        {"topic": "skeleton"},
        **kwargs,
    )
    assert result["runpod_job_id"] == "rp-job-after-retry"
    assert [request.get_method() for request in calls] == ["GET", "GET", "POST"]


def test_ambiguous_post_failure_is_retained_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _configure(monkeypatch, tmp_path)
    calls: list[Any] = []

    def fake_urlopen(request, timeout):
        calls.append(request)
        if request.get_method() == "GET":
            return _FakeResponse({"jobs": {"inQueue": 0}, "workers": {}})
        raise urllib.error.URLError("connection reset after submission")

    monkeypatch.setattr(runpod_bridge.urllib.request, "urlopen", fake_urlopen)
    kwargs = {"command_id": "cmd-ambiguous", "user_id": "user-1"}
    with pytest.raises(runpod_bridge.RunPodEndpointUnavailable):
        runpod_bridge.dispatch_production_tool(
            "finalize_production",
            {
                "job_id": "short-1",
                "_credit_reservation": {
                    "reservation_id": "hold-ambiguous",
                    "credits": 321,
                    "unlimited": False,
                },
            },
            **kwargs,
        )

    replay = runpod_bridge.dispatch_production_tool(
        "finalize_production",
        {
            "job_id": "short-1",
            "_credit_reservation": {
                "reservation_id": "hold-retry-noise",
                "credits": 999,
                "unlimited": False,
            },
        },
        **kwargs,
    )
    assert replay["ok"] is False
    assert replay["status"] == "dispatch_unknown"
    assert replay["fail_closed"] is True
    assert replay["credit_reservation_id"] == "hold-ambiguous"
    assert replay["credit_reservation_credits"] == 321
    assert replay["billing_user_id"] == "user-1"
    assert replay["idempotent_replay"] is True
    assert [request.get_method() for request in calls] == ["GET", "POST"]


@pytest.mark.parametrize(
    ("runpod_status", "studio_status", "running", "terminal"),
    [
        ("IN_QUEUE", "queued", True, False),
        ("IN_PROGRESS", "running", True, False),
        ("RUNNING", "running", True, False),
        ("COMPLETED", "complete", False, True),
        ("FAILED", "failed", False, True),
    ],
)
def test_get_runpod_job_status_normalizes_with_get_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    runpod_status: str,
    studio_status: str,
    running: bool,
    terminal: bool,
) -> None:
    _configure(monkeypatch, tmp_path)
    calls: list[Any] = []

    def fake_urlopen(request, timeout):
        calls.append(request)
        return _FakeResponse({"id": "rp-status-123", "status": runpod_status})

    monkeypatch.setattr(runpod_bridge.urllib.request, "urlopen", fake_urlopen)
    result = runpod_bridge.get_runpod_job_status("rp-status-123")

    assert result["status"] == studio_status
    assert result["running"] is running
    assert result["terminal"] is terminal
    assert result["runpod_status"] == runpod_status
    assert len(calls) == 1
    assert calls[0].get_method() == "GET"
    assert calls[0].full_url.endswith("/status/rp-status-123")
    assert all(request.get_method() != "POST" for request in calls)
    assert not list(runpod_bridge.dispatch_ledger_dir().glob("*.claim.json"))


def test_get_runpod_job_status_preserves_worker_snapshot_and_progress(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _configure(monkeypatch, tmp_path)
    calls: list[Any] = []
    snapshot = {
        "job_id": "short-1",
        "status": "awaiting_animation_review",
        "stage": "animation_review",
        "progress": 73,
    }
    output = {
        "ok": True,
        "status": "completed",
        "job_id": "short-1",
        "job_snapshot": snapshot,
        "progress": {"completed_scenes": 5, "total_scenes": 5},
    }

    def fake_urlopen(request, timeout):
        calls.append(request)
        return _FakeResponse(
            {
                "id": "rp-status-snapshot",
                "status": "COMPLETED",
                "output": output,
            }
        )

    monkeypatch.setattr(runpod_bridge.urllib.request, "urlopen", fake_urlopen)
    result = runpod_bridge.get_runpod_job_status("rp-status-snapshot")

    assert result["job_snapshot"] == snapshot
    assert result["progress"] == output["progress"]
    assert result["output"] == output
    assert result["studio_job_id"] == "short-1"
    assert [request.get_method() for request in calls] == ["GET"]


@pytest.mark.parametrize("progress_location", ["progress", "output"])
def test_live_progress_json_is_decoded_without_enqueuing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    progress_location: str,
) -> None:
    _configure(monkeypatch, tmp_path)
    calls: list[Any] = []
    progress = {"job_id": "short-live", "status": "running", "stage": "animation", "progress": 44}

    def fake_urlopen(request, timeout):
        calls.append(request)
        payload = {"id": "rp-live-progress", "status": "IN_PROGRESS"}
        payload[progress_location] = json.dumps(progress)
        return _FakeResponse(payload)

    monkeypatch.setattr(runpod_bridge.urllib.request, "urlopen", fake_urlopen)
    result = runpod_bridge.get_runpod_job_status("rp-live-progress")

    assert result["progress"] == progress
    assert result["job_snapshot"] == progress
    assert result["studio_job_id"] == "short-live"
    assert [request.get_method() for request in calls] == ["GET"]


def test_invalid_runpod_status_id_is_rejected_without_network(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _configure(monkeypatch, tmp_path)
    calls: list[Any] = []
    monkeypatch.setattr(
        runpod_bridge.urllib.request,
        "urlopen",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    with pytest.raises(runpod_bridge.RunPodDispatchPolicyError):
        runpod_bridge.get_runpod_job_status("../../run")

    assert calls == []


def test_receipt_lookup_by_dispatch_and_studio_job_id_is_local_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _configure(monkeypatch, tmp_path)
    calls: list[Any] = []
    monkeypatch.setattr(
        runpod_bridge.urllib.request,
        "urlopen",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )
    older_id = "rpd_" + "a" * 40
    newest_id = "rpd_" + "b" * 40
    runpod_bridge._save_dispatch_receipt(
        older_id,
        {
            "dispatch_id": older_id,
            "studio_job_id": "short-lookup",
            "runpod_job_id": "rp-older",
            "submitted_at": 10,
        },
    )
    runpod_bridge._save_dispatch_receipt(
        newest_id,
        {
            "dispatch_id": newest_id,
            "studio_job_id": "short-lookup",
            "runpod_job_id": "rp-newest",
            "submitted_at": 20,
        },
    )

    direct = runpod_bridge.get_dispatch_receipt(newest_id)
    by_studio_job = runpod_bridge.get_dispatch_receipt_by_studio_job_id("short-lookup")

    assert direct is not None and direct["runpod_job_id"] == "rp-newest"
    assert by_studio_job is not None and by_studio_job["dispatch_id"] == newest_id
    assert calls == []
