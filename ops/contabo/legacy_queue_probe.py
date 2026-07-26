#!/usr/bin/env python3
"""Capture repeated, atomic, secret-free Redis drain samples for Fly cutover."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


FIELDS = (
    "backend_queued",
    "backend_inflight",
    "backend_admitted",
    "agent_active",
    "agent_waiting",
    "agent_leases",
    "slots_active",
    "slots_waiting",
    "slots_leases",
)

LUA = r"""
local prefix = ARGV[1]
local now = ARGV[2]
local queued =
  redis.call("LLEN", prefix .. ":queue:p0") +
  redis.call("LLEN", prefix .. ":queue:p1") +
  redis.call("LLEN", prefix .. ":queue:p2")
local inflight = redis.call("LLEN", prefix .. ":queue:processing")
local admitted = #redis.call("KEYS", prefix .. ":queue:admitted:*")
local agent_active_key = prefix .. ":studio_agent:active"
local agent_leases_key = prefix .. ":studio_agent:leases"
redis.call("ZREMRANGEBYSCORE", agent_leases_key, "-inf", now)
local agent_leases = redis.call("ZCARD", agent_leases_key)
-- Active counters are only a cache of the lease set. Interrupted releases
-- can leave them nonzero forever, so reconcile them to the authoritative
-- nonexpired lease count before deciding whether production is drained.
redis.call("SET", agent_active_key, agent_leases)
local agent_active = agent_leases
local agent_waiting = tonumber(redis.call("GET", prefix .. ":studio_agent:waiting") or "0")
local slots_active = 0
local slots_waiting = 0
local slots_leases = 0
for _, lane in ipairs({"render", "stills", "i2v", "i2v_premium", "audio", "compose"}) do
  local base = prefix .. ":production_slots:" .. lane
  redis.call("ZREMRANGEBYSCORE", base .. ":leases", "-inf", now)
  local lane_leases = redis.call("ZCARD", base .. ":leases")
  redis.call("SET", base .. ":active", lane_leases)
  slots_active = slots_active + lane_leases
  slots_waiting = slots_waiting + tonumber(redis.call("GET", base .. ":waiting") or "0")
  slots_leases = slots_leases + lane_leases
end
return {
  queued, inflight, admitted,
  agent_active, agent_waiting, agent_leases,
  slots_active, slots_waiting, slots_leases
}
"""


def _write_atomic(path: Path, payload: dict[str, Any]) -> None:
    path = path.resolve(strict=False)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def capture(
    redis_url: str,
    *,
    prefix: str,
    samples: int,
    interval: float,
    app: str,
    machine_id: str,
) -> dict[str, Any]:
    try:
        import redis
    except Exception as exc:  # pragma: no cover - deployment dependency
        raise RuntimeError("the redis Python package is required") from exc

    client = redis.Redis.from_url(
        redis_url,
        socket_connect_timeout=10,
        socket_timeout=20,
        decode_responses=False,
    )
    rows: list[dict[str, Any]] = []
    try:
        client.ping()
        for index in range(samples):
            raw = client.eval(LUA, 0, prefix, str(time.time()))
            if not isinstance(raw, (list, tuple)) or len(raw) != len(FIELDS):
                raise RuntimeError("Redis returned an incomplete drain sample")
            counts = {}
            for key, value in zip(FIELDS, raw):
                parsed = int(value)
                if parsed < 0:
                    raise RuntimeError("Redis returned a negative drain count")
                counts[key] = parsed
            rows.append({"captured_at_epoch": int(time.time()), "counts": counts})
            if index + 1 < samples:
                time.sleep(interval)
    finally:
        try:
            client.close()
        except Exception:
            pass
    drained = all(all(value == 0 for value in row["counts"].values()) for row in rows)
    return {
        "format": 1,
        "app": app,
        "machine_id": machine_id,
        "prefix": prefix,
        "sample_count": len(rows),
        "samples": rows,
        "drained": drained,
        "active_counter_source": "nonexpired_lease_sets",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    parser.add_argument("--app", required=True)
    parser.add_argument("--machine-id", required=True)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--require-drained", action="store_true")
    args = parser.parse_args()
    prefix = str(os.getenv("REDIS_QUEUE_PREFIX", "studio") or "studio").strip()
    redis_url = str(os.getenv("REDIS_URL", "") or "").strip()
    if not redis_url:
        print("REDIS_URL is required", file=sys.stderr)
        return 2
    if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,80}", prefix):
        print("REDIS_QUEUE_PREFIX is invalid", file=sys.stderr)
        return 2
    if args.samples < 3 or args.samples > 20:
        print("--samples must be between 3 and 20", file=sys.stderr)
        return 2
    if args.interval < 0 or args.interval > 60:
        print("--interval must be between 0 and 60 seconds", file=sys.stderr)
        return 2
    try:
        payload = capture(
            redis_url,
            prefix=prefix,
            samples=args.samples,
            interval=args.interval,
            app=args.app,
            machine_id=args.machine_id,
        )
        if args.output:
            _write_atomic(args.output, payload)
        else:
            print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        if args.require_drained and not payload["drained"]:
            print("legacy Redis is not drained", file=sys.stderr)
            return 1
        return 0
    except Exception as exc:
        print(f"legacy Redis probe failed: {type(exc).__name__}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
