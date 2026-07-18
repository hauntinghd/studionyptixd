#!/usr/bin/env python3
"""Grant unified Studio credits to a user by email (local wallet on Fly volume).

Usage:
  python ops/grant_studio_credits.py user@example.com 500 --reason early_access_waitlist

Requires TEMP_DIR / unified wallet path to match production when run against live data,
or call the production API as admin instead:

  curl -X POST https://api-studio.nyptidindustries.com/api/admin/grant-credits \\
    -H "Authorization: Bearer <admin_jwt>" \\
    -H "Content-Type: application/json" \\
    -d '{"email":"user@example.com","credits":500,"reason":"waitlist_pro_39"}'
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _lookup_user_id_by_email(email: str) -> str:
    import os
    import httpx
    from backend_settings import SUPABASE_ANON_KEY, SUPABASE_SERVICE_KEY, SUPABASE_URL

    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not SUPABASE_URL or not svc_key:
        raise SystemExit("Supabase not configured (SUPABASE_URL / service key)")
    normalized = email.strip().lower()
    with httpx.Client(timeout=30) as client:
        resp = client.get(
            f"{SUPABASE_URL}/auth/v1/admin/users?per_page=500",
            headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
        )
        resp.raise_for_status()
        data = resp.json()
        users = data.get("users", data) if isinstance(data, dict) else data
        for u in users:
            if str(u.get("email", "") or "").lower() == normalized:
                return str(u.get("id", "") or "")
    raise SystemExit(f"No Supabase user for {email}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Grant unified Studio credits by email")
    parser.add_argument("email", help="User email (must exist in Supabase auth)")
    parser.add_argument("credits", type=int, nargs="?", default=0, help="Credits to add (e.g. 500)")
    parser.add_argument(
        "--target-balance",
        type=int,
        default=0,
        help="Add only enough credits to reach this available balance",
    )
    parser.add_argument("--reason", default="admin_grant", help="Ledger reason")
    parser.add_argument("--idempotency-key", default="", help="Stable retry key for this grant")
    parser.add_argument(
        "--pending-if-missing",
        action="store_true",
        help="Persist the target grant for automatic claim on first login",
    )
    args = parser.parse_args()
    if args.credits <= 0 and args.target_balance <= 0:
        raise SystemExit("credits or --target-balance must be positive")

    import unified_credits as uc

    try:
        user_id = _lookup_user_id_by_email(args.email)
    except SystemExit:
        if not args.pending_if_missing or args.target_balance <= 0 or not args.idempotency_key:
            raise
        row = uc.register_pending_grant(
            args.email,
            args.target_balance,
            reason=args.reason,
            idempotency_key=args.idempotency_key,
        )
        print(f"Pending grant registered for {row['email']} (target={row['target_balance']})")
        return
    current = uc.get_balance(user_id)
    credits = max(0, args.target_balance - current) if args.target_balance > 0 else args.credits
    if credits <= 0:
        print(f"No grant needed for {args.email} (balance={current}, target={args.target_balance})")
        return
    wallet = uc.add_credits(
        user_id,
        credits,
        reason=args.reason,
        metadata={"email": args.email.strip().lower()},
        idempotency_key=args.idempotency_key,
    )
    print(
        f"Granted {credits} credits to {args.email} "
        f"(balance={wallet.get('balance')})"
    )


if __name__ == "__main__":
    main()
