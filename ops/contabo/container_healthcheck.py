"""Strict in-container health probe for the single Studio API/consumer owner."""

from __future__ import annotations

import json
import os
import sys
import urllib.request


def fail(message: str) -> None:
    print(f"unhealthy: {message}", file=sys.stderr)
    raise SystemExit(1)


expected_sha = str(os.environ.get("EXPECTED_GIT_SHA", "") or "").strip()
expected_build = str(os.environ.get("EXPECTED_BUILD_ID", "") or "").strip()
if len(expected_sha) != 40 or any(ch not in "0123456789abcdef" for ch in expected_sha):
    fail("EXPECTED_GIT_SHA is not a full lowercase Git SHA")
if not expected_build:
    fail("EXPECTED_BUILD_ID is empty")

request = urllib.request.Request(
    "http://127.0.0.1:10000/api/health",
    headers={"Host": "api-studio.nyptidindustries.com"},
)
try:
    with urllib.request.urlopen(request, timeout=8) as response:
        payload = json.load(response)
except Exception as exc:
    fail(f"health request failed ({type(exc).__name__})")

if payload.get("status") != "online":
    fail("API status is not online")
if payload.get("backend_commit") != expected_sha:
    fail("backend commit does not match the immutable candidate")
if payload.get("frontend_bundle") != expected_build:
    fail("frontend bundle does not match the immutable candidate")
if payload.get("deployment_target") != "contabo":
    fail("deployment target is not Contabo")
if payload.get("release_id") != expected_build:
    fail("release identity does not match the immutable candidate")
if payload.get("queue_mode") != "redis":
    fail("production queue is not using Redis")
if payload.get("queue_consumer_ready") is not True:
    fail("production consumer is not ready")
if payload.get("queue_consumer_running") is not True:
    fail("production consumer is not running")

queue = payload.get("queue_consumer")
if not isinstance(queue, dict) or queue.get("workers") != 1:
    fail("exactly one embedded production worker is required")

print("healthy")
