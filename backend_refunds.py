"""Refund request handlers for Studio billing support."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import httpx
from fastapi import HTTPException, Request


def build_refund_handlers(
    *,
    get_current_user_from_request: Callable,
    admin_emails: set[str],
    supabase_url: str,
    supabase_service_key: str,
    supabase_anon_key: str,
    app_data_dir: Path,
    log,
):
    async def billing_refund_request(req: Request):
        """User-submitted refund request with Supabase primary and JSONL fallback."""
        user = await get_current_user_from_request(req) if req else None
        if not user:
            raise HTTPException(401, "Auth required")
        try:
            body = await req.json()
        except Exception:
            body = {}
        reason = str(body.get("reason", "") or "").strip()
        amount_raw = body.get("amount_usd")
        payment_reference = str(body.get("payment_reference", "") or "").strip()
        image_proof = str(body.get("image_proof", "") or "").strip()
        if not reason:
            raise HTTPException(400, "Reason is required")
        if len(reason) > 4000:
            reason = reason[:4000]
        try:
            amount_usd = float(amount_raw) if amount_raw is not None and amount_raw != "" else None
        except (TypeError, ValueError):
            raise HTTPException(400, "Amount paid must be a number")
        if amount_usd is None or amount_usd <= 0:
            raise HTTPException(400, "Amount paid is required and must be greater than zero")
        if not payment_reference:
            raise HTTPException(400, "PayPal order / invoice id is required")
        if not image_proof:
            raise HTTPException(400, "Image proof is required")
        is_data_url = image_proof.startswith("data:image/")
        is_http_url = image_proof.startswith("https://") or image_proof.startswith("http://")
        if not (is_data_url or is_http_url):
            raise HTTPException(400, "Image proof must be an uploaded image or an https URL")
        if len(image_proof) > 3 * 1024 * 1024:
            raise HTTPException(413, "Image proof is too large (max 2 MB)")
        payload = {
            "user_id": str(user.get("id", "") or ""),
            "email": str(user.get("email", "") or ""),
            "reason": reason,
            "amount_usd": amount_usd,
            "payment_reference": payment_reference,
            "image_proof": image_proof,
            "status": "pending",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        wrote_remote = False
        if supabase_url and (supabase_service_key or supabase_anon_key):
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    key = supabase_service_key or supabase_anon_key
                    resp = await client.post(
                        f"{supabase_url}/rest/v1/refund_requests",
                        headers={
                            "apikey": key,
                            "Authorization": f"Bearer {key}",
                            "Content-Type": "application/json",
                            "Prefer": "return=representation",
                        },
                        json=payload,
                    )
                    if resp.status_code in (200, 201):
                        wrote_remote = True
                    else:
                        log.warning(f"Refund-request Supabase insert returned {resp.status_code}: {resp.text[:300]}")
            except Exception as exc:
                log.warning(f"Refund-request Supabase insert failed, will fallback: {exc}")
        if not wrote_remote:
            try:
                fallback_path = Path(app_data_dir) / "refund_requests.jsonl"
                fallback_path.parent.mkdir(parents=True, exist_ok=True)
                with fallback_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(payload, ensure_ascii=True) + "\n")
            except Exception as exc:
                log.error(f"Refund-request fallback write failed: {exc}")
        return {"ok": True, "stored": "supabase" if wrote_remote else "local", "status": "pending"}

    async def admin_refunds_list(request: Request):
        """Admin view: all refund requests plus verified users roster."""
        user = await get_current_user_from_request(request) if request else None
        if not user:
            raise HTTPException(401, "Auth required")
        email = str(user.get("email", "") or "").lower()
        if email not in {entry.lower() for entry in admin_emails}:
            raise HTTPException(403, "Admin only")
        refunds: list[dict] = []
        verified_users: list[dict] = []
        if supabase_url and (supabase_service_key or supabase_anon_key):
            key = supabase_service_key or supabase_anon_key
            headers = {"apikey": key, "Authorization": f"Bearer {key}"}
            async with httpx.AsyncClient(timeout=20) as client:
                try:
                    response = await client.get(
                        f"{supabase_url}/rest/v1/refund_requests?select=*&order=created_at.desc&limit=500",
                        headers=headers,
                    )
                    if response.status_code == 200:
                        refunds = response.json() or []
                except Exception as exc:
                    log.warning(f"/api/admin/refunds supabase fetch failed: {exc}")
                try:
                    response = await client.get(
                        f"{supabase_url}/rest/v1/profiles?select=id,email,plan,created_at&order=created_at.desc&limit=1000",
                        headers=headers,
                    )
                    if response.status_code == 200:
                        verified_users = response.json() or []
                except Exception as exc:
                    log.warning(f"/api/admin/refunds verified-users fetch failed: {exc}")
        try:
            fallback_path = Path(app_data_dir) / "refund_requests.jsonl"
            if fallback_path.exists():
                for line in fallback_path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        refunds.append(json.loads(line))
                    except Exception:
                        pass
        except Exception:
            pass
        return {"refunds": refunds, "verified_users": verified_users}

    async def admin_refund_update(refund_id: str, request: Request):
        """Admin PATCH: update status plus admin note on a refund request."""
        user = await get_current_user_from_request(request) if request else None
        if not user:
            raise HTTPException(401, "Auth required")
        email = str(user.get("email", "") or "").lower()
        if email not in {entry.lower() for entry in admin_emails}:
            raise HTTPException(403, "Admin only")
        try:
            body = await request.json()
        except Exception:
            body = {}
        status_val = str(body.get("status", "") or "").strip().lower()
        if status_val and status_val not in ("pending", "approved", "denied", "refunded"):
            raise HTTPException(400, "Invalid status")
        note = body.get("admin_note")
        update_payload: dict = {}
        if status_val:
            update_payload["status"] = status_val
        if isinstance(note, str):
            update_payload["admin_note"] = note[:2000]
        if not update_payload:
            raise HTTPException(400, "No fields to update")
        if supabase_url and (supabase_service_key or supabase_anon_key):
            key = supabase_service_key or supabase_anon_key
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    response = await client.patch(
                        f"{supabase_url}/rest/v1/refund_requests?id=eq.{refund_id}",
                        headers={
                            "apikey": key,
                            "Authorization": f"Bearer {key}",
                            "Content-Type": "application/json",
                            "Prefer": "return=minimal",
                        },
                        json=update_payload,
                    )
                    if response.status_code >= 400:
                        raise HTTPException(response.status_code, f"Supabase update failed: {response.text[:300]}")
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(500, f"Supabase update error: {exc}") from exc
        return {"ok": True, "updated": update_payload}

    return billing_refund_request, admin_refunds_list, admin_refund_update
