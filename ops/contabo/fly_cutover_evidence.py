#!/usr/bin/env python3
"""Validate and hash-bind raw Fly cutover evidence without trusting prose."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import stat
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit


class EvidenceError(RuntimeError):
    pass


EVIDENCE_FILES = {
    "machine_list_before": "machine-list-before.json",
    "machine_before": "machine-before.json",
    "app_config": "app-config.json",
    "origin_probe": "origin-probe.json",
    "queue_probe": "legacy-queue.json",
    "file_quiescence": "file-quiescence.json",
    "machine_after": "machine-after.json",
    "machine_list_after": "machine-list-after.json",
}


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise EvidenceError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _load(path: Path) -> tuple[Any, str]:
    if path.is_symlink() or not path.is_file():
        raise EvidenceError(f"evidence is not a regular file: {path}")
    if path.stat().st_size > 16 * 1024 * 1024:
        raise EvidenceError(f"evidence file is unexpectedly large: {path.name}")
    raw = path.read_bytes()
    try:
        payload = json.loads(raw, object_pairs_hook=_strict_object)
    except Exception as exc:
        raise EvidenceError(
            f"invalid evidence JSON: {path.name} ({type(exc).__name__})"
        ) from exc
    return payload, hashlib.sha256(raw).hexdigest()


def _machine(payload: dict[str, Any]) -> dict[str, Any]:
    for key in ("machine", "Machine"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            return nested
    return payload


def _verify_machine_list(
    payload: Any,
    *,
    machine_id: str,
    require_stopped: bool,
) -> None:
    machines: Any = payload
    if isinstance(payload, dict):
        machines = payload.get("machines", payload.get("Machines"))
    if not isinstance(machines, list):
        raise EvidenceError("Fly machine-list evidence is not an array")
    if len(machines) != 1:
        raise EvidenceError(
            f"Fly app must have exactly one retained machine; found {len(machines)}"
        )
    machine = machines[0]
    if not isinstance(machine, dict):
        raise EvidenceError("Fly machine-list entry is malformed")
    _verify_machine(
        machine,
        machine_id=machine_id,
        require_stopped=require_stopped,
    )


def _value(payload: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in payload:
            return payload[name]
    return None


def _normalized_command(value: Any) -> list[str]:
    if isinstance(value, list):
        parts = [str(item) for item in value]
        if len(parts) == 1:
            try:
                return shlex.split(parts[0])
            except ValueError:
                return []
        return parts
    if isinstance(value, str):
        try:
            return shlex.split(value)
        except ValueError:
            return []
    return []


def _verify_machine(
    payload: dict[str, Any],
    *,
    machine_id: str,
    require_stopped: bool,
) -> None:
    machine = _machine(payload)
    actual_id = str(_value(machine, "id", "ID", "machine_id") or "")
    if actual_id != machine_id:
        raise EvidenceError("Fly machine evidence targets a different machine")
    state = str(_value(machine, "state", "State") or "").strip().lower()
    if require_stopped and state != "stopped":
        raise EvidenceError(f"Fly machine is not stopped: {state or '<missing>'}")
    config = _value(machine, "config", "Config")
    if not isinstance(config, dict):
        raise EvidenceError("Fly machine config is missing")
    services = config.get("services", [])
    if services not in (None, [], {}):
        raise EvidenceError("Fly machine still exposes service admission")
    checks = config.get("checks", {})
    if checks not in (None, [], {}):
        raise EvidenceError("Fly copy-only machine still has health checks")
    init = config.get("init")
    init = init if isinstance(init, dict) else {}
    command = init.get("cmd", config.get("cmd", config.get("command")))
    if _normalized_command(command) != ["sleep", "infinity"]:
        raise EvidenceError("Fly machine command is not exact copy-only sleep")
    if init.get("entrypoint") not in (None, "", []):
        raise EvidenceError("Fly copy-only machine retains an entrypoint")
    if config.get("processes") not in (None, {}, []):
        raise EvidenceError("Fly copy-only machine retains process definitions")
    restart = config.get("restart")
    policy = (
        str(restart.get("policy") or "").lower()
        if isinstance(restart, dict)
        else str(restart or "").lower()
    )
    if policy != "no":
        raise EvidenceError("Fly machine restart policy is not no")
    if config.get("auto_destroy") not in (False, None):
        raise EvidenceError("Fly copy-only machine unexpectedly auto-destroys")
    mounts = config.get("mounts")
    if not isinstance(mounts, list) or not any(
        isinstance(item, dict)
        and str(item.get("path") or item.get("destination") or "") == "/var/data"
        for item in mounts
    ):
        raise EvidenceError("Fly machine evidence does not retain /var/data")


def _app_config(payload: dict[str, Any]) -> dict[str, Any]:
    nested = payload.get("config")
    return nested if isinstance(nested, dict) else payload


def _verify_app_config(payload: dict[str, Any], *, app: str) -> None:
    named = str(payload.get("app") or payload.get("app_name") or "")
    if named and named != app:
        raise EvidenceError("Fly app config targets a different app")
    config = _app_config(payload)
    declarations: list[dict[str, Any]] = []
    http_service = config.get("http_service")
    if isinstance(http_service, dict):
        declarations.append(http_service)
    services = config.get("services")
    if isinstance(services, list):
        declarations.extend(item for item in services if isinstance(item, dict))
    # A reviewed copy-only app config may deliberately contain no public
    # service declaration at all. With every retained machine also verified
    # stopped and service-free, absence of services is the strongest possible
    # admission/autostart proof.
    if not declarations:
        return
    for service in declarations:
        auto_start = service.get(
            "auto_start_machines",
            service.get("autostart", service.get("auto_start")),
        )
        if auto_start is not False:
            raise EvidenceError("Fly app config does not disable autostart")
        minimum = service.get(
            "min_machines_running",
            service.get("min_machines", service.get("min")),
        )
        if minimum != 0:
            raise EvidenceError("Fly app config min_machines is not zero")


def _verify_origin_probe(
    payload: dict[str, Any],
    *,
    app: str,
    machine_id: str,
    origin: str,
    now: int,
    max_age: int,
) -> int:
    if payload.get("format") != 1:
        raise EvidenceError("origin probe format is invalid")
    if payload.get("app") != app or payload.get("machine_id") != machine_id:
        raise EvidenceError("origin probe target is wrong")
    if payload.get("origin") != origin:
        raise EvidenceError("origin probe URL is wrong")
    captured = payload.get("captured_at_epoch")
    if not isinstance(captured, int) or captured > now or now - captured > max_age:
        raise EvidenceError("origin probe is stale or future-dated")
    attempts = payload.get("attempts")
    if not isinstance(attempts, list) or len(attempts) < 3:
        raise EvidenceError("origin probe requires at least three attempts")
    for attempt in attempts:
        if not isinstance(attempt, dict):
            raise EvidenceError("origin probe attempt is malformed")
        exit_code = attempt.get("curl_exit_code")
        status_code = attempt.get("http_status")
        if not isinstance(exit_code, int) or not isinstance(status_code, int):
            raise EvidenceError("origin probe result is not numeric")
        if exit_code == 0 or 200 <= status_code < 400:
            raise EvidenceError("legacy origin still admits successful requests")
    return captured


def _verify_queue(
    payload: dict[str, Any],
    *,
    app: str,
    machine_id: str,
    now: int,
    max_age: int,
) -> int:
    if payload.get("format") != 1:
        raise EvidenceError("legacy queue evidence format is invalid")
    if payload.get("app") != app or payload.get("machine_id") != machine_id:
        raise EvidenceError("legacy queue evidence target is wrong")
    samples = payload.get("samples")
    if payload.get("drained") is not True or not isinstance(samples, list) or len(samples) < 3:
        raise EvidenceError("legacy queue evidence is not a repeated drain proof")
    latest = 0
    for sample in samples:
        if not isinstance(sample, dict) or not isinstance(sample.get("counts"), dict):
            raise EvidenceError("legacy queue sample is malformed")
        captured = sample.get("captured_at_epoch")
        if not isinstance(captured, int):
            raise EvidenceError("legacy queue timestamp is invalid")
        latest = max(latest, captured)
        counts = sample["counts"]
        if len(counts) != 9 or any(
            not isinstance(value, int) or value != 0 for value in counts.values()
        ):
            raise EvidenceError("legacy queue/inflight/lease proof is not zero")
    if latest > now or now - latest > max_age:
        raise EvidenceError("legacy queue evidence is stale or future-dated")
    return latest


def _verify_file_quiescence(
    payload: dict[str, Any],
    *,
    app: str,
    machine_id: str,
    now: int,
    max_age: int,
) -> int:
    if payload.get("format") != 1:
        raise EvidenceError("file quiescence evidence format is invalid")
    if payload.get("app") != app or payload.get("machine_id") != machine_id:
        raise EvidenceError("file quiescence evidence target is wrong")
    captured = payload.get("captured_at_epoch")
    if not isinstance(captured, int) or captured > now or now - captured > max_age:
        raise EvidenceError("file quiescence evidence is stale or future-dated")
    counts = payload.get("counts")
    if (
        payload.get("drained") is not True
        or payload.get("total_blockers") != 0
        or not isinstance(counts, dict)
        or len(counts) != 4
        or any(not isinstance(value, int) or value != 0 for value in counts.values())
    ):
        raise EvidenceError("resumable file-backed production is not quiescent")
    return captured


def _canonical_origin(raw: str) -> str:
    parsed = urlsplit(raw)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise EvidenceError("legacy origin must be a plain HTTPS origin")
    path = parsed.path.rstrip("/")
    if path:
        raise EvidenceError("legacy origin must not include a path")
    return urlunsplit(("https", parsed.netloc, "", "", ""))


def verify(
    evidence_dir: Path,
    *,
    app: str,
    machine_id: str,
    origin: str,
    max_age: int,
    now: int | None = None,
) -> dict[str, str]:
    evidence_dir = evidence_dir.resolve(strict=True)
    if evidence_dir.is_symlink() or not evidence_dir.is_dir():
        raise EvidenceError("evidence directory must be a real directory")
    if not app or not machine_id:
        raise EvidenceError("app and machine ID are required")
    origin = _canonical_origin(origin)
    now = int(now or time.time())
    payloads: dict[str, Any] = {}
    hashes: dict[str, str] = {}
    for key, name in EVIDENCE_FILES.items():
        payloads[key], hashes[key] = _load(evidence_dir / name)

    for key in (
        "machine_before",
        "machine_after",
        "app_config",
        "origin_probe",
        "queue_probe",
        "file_quiescence",
    ):
        if not isinstance(payloads[key], dict):
            raise EvidenceError(f"{EVIDENCE_FILES[key]} root is not an object")
    _verify_machine_list(
        payloads["machine_list_before"],
        machine_id=machine_id,
        require_stopped=True,
    )
    _verify_machine(
        payloads["machine_before"],
        machine_id=machine_id,
        require_stopped=True,
    )
    _verify_machine(
        payloads["machine_after"],
        machine_id=machine_id,
        require_stopped=True,
    )
    _verify_machine_list(
        payloads["machine_list_after"],
        machine_id=machine_id,
        require_stopped=True,
    )
    _verify_app_config(payloads["app_config"], app=app)
    probe_at = _verify_origin_probe(
        payloads["origin_probe"],
        app=app,
        machine_id=machine_id,
        origin=origin,
        now=now,
        max_age=max_age,
    )
    queue_at = _verify_queue(
        payloads["queue_probe"],
        app=app,
        machine_id=machine_id,
        now=now,
        max_age=max_age,
    )
    files_at = _verify_file_quiescence(
        payloads["file_quiescence"],
        app=app,
        machine_id=machine_id,
        now=now,
        max_age=max_age,
    )
    if queue_at > probe_at or files_at > probe_at:
        raise EvidenceError("origin retirement proof predates a quiescence proof")

    bundle_material = "\n".join(
        f"{key}={hashes[key]}" for key in sorted(hashes)
    ).encode("ascii")
    result = {
        "EVIDENCE_FORMAT": "2",
        "APP": app,
        "MACHINE_ID": machine_id,
        "ORIGIN": origin,
        "VERIFIED_AT_EPOCH": str(now),
        "ORIGIN_PROBED_AT_EPOCH": str(probe_at),
        "BUNDLE_SHA256": hashlib.sha256(bundle_material).hexdigest(),
    }
    for key, digest in hashes.items():
        result[f"{key.upper()}_SHA256"] = digest
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--app", required=True)
    parser.add_argument("--machine-id", required=True)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--max-age-seconds", type=int, default=1800)
    args = parser.parse_args()
    if args.max_age_seconds < 1 or args.max_age_seconds > 3600:
        print("max evidence age must be between 1 and 3600 seconds", file=sys.stderr)
        return 2
    try:
        result = verify(
            args.evidence_dir,
            app=args.app,
            machine_id=args.machine_id,
            origin=args.origin,
            max_age=args.max_age_seconds,
        )
    except (OSError, EvidenceError) as exc:
        print(f"Fly evidence verification failed: {exc}", file=sys.stderr)
        return 1
    for key, value in result.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
