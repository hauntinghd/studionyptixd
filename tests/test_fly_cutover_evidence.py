from __future__ import annotations

import importlib.util
import json
import sys
import time
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "ops" / "contabo" / "fly_cutover_evidence.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("fly_cutover_evidence", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _machine(machine_id: str = "683d1304bd13e8") -> dict:
    return {
        "id": machine_id,
        "state": "stopped",
        "config": {
            "services": [],
            "checks": {},
            "init": {"cmd": ["sleep", "infinity"]},
            "restart": {"policy": "no"},
            "mounts": [{"path": "/var/data", "volume": "vol_expected"}],
        },
    }


def _evidence_tree(tmp_path: Path, *, now: int) -> Path:
    app = "nyptid-studio"
    machine_id = "683d1304bd13e8"
    origin = "https://nyptid-studio.fly.dev"
    machine = _machine(machine_id)
    _write(tmp_path / "machine-list-before.json", [machine])
    _write(tmp_path / "machine-before.json", machine)
    _write(tmp_path / "app-config.json", {"app": app, "config": {}})
    _write(
        tmp_path / "origin-probe.json",
        {
            "format": 1,
            "app": app,
            "machine_id": machine_id,
            "origin": origin,
            "captured_at_epoch": now - 10,
            "attempts": [
                {"curl_exit_code": 7, "http_status": 0},
                {"curl_exit_code": 22, "http_status": 502},
                {"curl_exit_code": 28, "http_status": 0},
            ],
        },
    )
    zero_counts = {f"lane_{index}": 0 for index in range(9)}
    _write(
        tmp_path / "legacy-queue.json",
        {
            "format": 1,
            "app": app,
            "machine_id": machine_id,
            "drained": True,
            "samples": [
                {"captured_at_epoch": now - seconds, "counts": zero_counts}
                for seconds in (16, 14, 12)
            ],
        },
    )
    _write(
        tmp_path / "file-quiescence.json",
        {
            "format": 1,
            "app": app,
            "machine_id": machine_id,
            "captured_at_epoch": now - 12,
            "drained": True,
            "total_blockers": 0,
            "counts": {
                "active_jobs": 0,
                "commands": 0,
                "production_runs": 0,
                "production_workflows": 0,
            },
        },
    )
    _write(tmp_path / "machine-after.json", machine)
    _write(tmp_path / "machine-list-after.json", [machine])
    return tmp_path


def test_fly_cutover_evidence_accepts_one_stopped_service_free_machine(
    tmp_path: Path,
) -> None:
    module = _load_module()
    now = int(time.time())
    evidence = _evidence_tree(tmp_path, now=now)

    result = module.verify(
        evidence,
        app="nyptid-studio",
        machine_id="683d1304bd13e8",
        origin="https://nyptid-studio.fly.dev",
        max_age=1800,
        now=now,
    )

    assert result["EVIDENCE_FORMAT"] == "2"
    assert len(result["BUNDLE_SHA256"]) == 64
    assert "MACHINE_LIST_BEFORE_SHA256" in result
    assert "MACHINE_LIST_AFTER_SHA256" in result


def test_fly_cutover_evidence_rejects_any_second_machine(tmp_path: Path) -> None:
    module = _load_module()
    now = int(time.time())
    evidence = _evidence_tree(tmp_path, now=now)
    _write(
        evidence / "machine-list-after.json",
        [_machine(), _machine("another-machine")],
    )

    with pytest.raises(module.EvidenceError, match="exactly one retained machine"):
        module.verify(
            evidence,
            app="nyptid-studio",
            machine_id="683d1304bd13e8",
            origin="https://nyptid-studio.fly.dev",
            max_age=1800,
            now=now,
        )


def test_fly_cutover_evidence_rejects_service_admission_or_nonzero_work(
    tmp_path: Path,
) -> None:
    module = _load_module()
    now = int(time.time())
    evidence = _evidence_tree(tmp_path, now=now)
    admitted = _machine()
    admitted["config"]["services"] = [{"ports": [{"port": 443}]}]
    _write(evidence / "machine-after.json", admitted)

    with pytest.raises(module.EvidenceError, match="service admission"):
        module.verify(
            evidence,
            app="nyptid-studio",
            machine_id="683d1304bd13e8",
            origin="https://nyptid-studio.fly.dev",
            max_age=1800,
            now=now,
        )

    evidence = _evidence_tree(tmp_path, now=now)
    queue = json.loads((evidence / "legacy-queue.json").read_text(encoding="utf-8"))
    queue["samples"][2]["counts"]["lane_0"] = 1
    _write(evidence / "legacy-queue.json", queue)
    with pytest.raises(module.EvidenceError, match="not zero"):
        module.verify(
            evidence,
            app="nyptid-studio",
            machine_id="683d1304bd13e8",
            origin="https://nyptid-studio.fly.dev",
            max_age=1800,
            now=now,
        )
