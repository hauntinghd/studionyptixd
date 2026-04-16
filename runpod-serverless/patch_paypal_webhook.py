"""
Patch the existing PayPal webhook registration to point at the new Cloudflare
Worker URL (api.studio.nyptidindustries.com) instead of the old Render domain.

Idempotent — safe to run repeatedly. Reads creds from .env at repo root.

Usage:
    python runpod-serverless/patch_paypal_webhook.py
"""
from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_ROOT / ".env"
NEW_URL = "https://api.studio.nyptidindustries.com/api/paypal/webhook"


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    if not ENV_FILE.exists():
        return env
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def paypal_base(env_name: str) -> str:
    return "https://api-m.sandbox.paypal.com" if str(env_name or "").strip().lower() == "sandbox" else "https://api-m.paypal.com"


def get_token(client_id: str, client_secret: str, base: str) -> str:
    auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    with httpx.Client(timeout=30) as c:
        r = c.post(
            f"{base}/v1/oauth2/token",
            headers={"Authorization": f"Basic {auth}", "Content-Type": "application/x-www-form-urlencoded"},
            content="grant_type=client_credentials",
        )
        r.raise_for_status()
        return r.json()["access_token"]


def main() -> int:
    env = load_env()
    client_id = env.get("PAYPAL_CLIENT_ID", "").strip()
    client_secret = env.get("PAYPAL_CLIENT_SECRET", "").strip()
    webhook_id = env.get("PAYPAL_WEBHOOK_ID", "").strip()
    paypal_env = env.get("PAYPAL_ENV", "live").strip().lower()

    if not (client_id and client_secret):
        print("ERROR: PAYPAL_CLIENT_ID / PAYPAL_CLIENT_SECRET missing from .env", file=sys.stderr)
        return 2
    if not webhook_id:
        print("ERROR: PAYPAL_WEBHOOK_ID missing from .env", file=sys.stderr)
        return 2

    base = paypal_base(paypal_env)
    token = get_token(client_id, client_secret, base)

    # Read current webhook
    with httpx.Client(timeout=30) as c:
        r = c.get(f"{base}/v1/notifications/webhooks/{webhook_id}", headers={"Authorization": f"Bearer {token}"})
        if r.status_code == 404:
            print(f"Webhook {webhook_id} not found on {base}. Nothing to patch.")
            return 1
        r.raise_for_status()
        current = r.json()
        current_url = current.get("url", "")

    if current_url == NEW_URL:
        print(f"Already pointing at {NEW_URL}. No change.")
        return 0

    print(f"Current URL: {current_url}")
    print(f"Patching to: {NEW_URL}")

    patch_body = [{"op": "replace", "path": "/url", "value": NEW_URL}]
    with httpx.Client(timeout=30) as c:
        r = c.patch(
            f"{base}/v1/notifications/webhooks/{webhook_id}",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=patch_body,
        )
        if r.status_code >= 400:
            print(f"Patch failed ({r.status_code}): {r.text[:400]}", file=sys.stderr)
            return 1
        print(f"OK — webhook {webhook_id} now points at {NEW_URL}")
        ev_count = len(r.json().get("event_types", []))
        print(f"  event subscriptions preserved: {ev_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
