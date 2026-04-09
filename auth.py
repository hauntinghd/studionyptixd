from __future__ import annotations

from typing import Callable, Optional

import httpx
import jwt
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer


ManualSnapshotFn = Callable[[dict], dict]
FALLBACK_SUPABASE_URL = "https://qdwzilgqvpegekxrrnnn.supabase.co"
FALLBACK_SUPABASE_ANON_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFkd3ppbGdxdnBlZ2VreHJybm5uIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYwMjQ3NzYsImV4cCI6MjA4MTYwMDc3Nn0."
    "89jrswXUwk1Th_e2y7QEq_vLf3M2XhQJjIfByWOD7EE"
)


def _extract_request_token(request: Request | None) -> str:
    if request is None:
        return ""
    for header_name in ("authorization", "x-access-token", "x-auth-token"):
        header_value = str(request.headers.get(header_name) or "").strip()
        if not header_value:
            continue
        parts = header_value.split(None, 1)
        if len(parts) == 2 and str(parts[0] or "").strip().lower() == "bearer":
            token = str(parts[1] or "").strip()
            if token:
                return token
        if header_name != "authorization":
            return header_value
    return str(request.query_params.get("access_token", "") or request.query_params.get("token", "") or "").strip()


def _extract_role(payload: dict | None) -> str:
    if not isinstance(payload, dict):
        return ""
    role = str(payload.get("role", "") or "").strip().lower()
    if role:
        return role
    app_metadata = payload.get("app_metadata")
    if isinstance(app_metadata, dict):
        role = str(app_metadata.get("role", "") or "").strip().lower()
    return role


def build_auth_helpers(
    *,
    supabase_url: str,
    supabase_anon_key: str,
    supabase_jwt_secret: str,
    supabase_service_key: str,
    hardcoded_plans: dict[str, str],
    paypal_snapshot_for_user: ManualSnapshotFn,
):
    security = HTTPBearer(auto_error=False)
    effective_supabase_url = str(supabase_url or "").strip() or FALLBACK_SUPABASE_URL
    effective_supabase_anon_key = str(supabase_anon_key or "").strip() or FALLBACK_SUPABASE_ANON_KEY

    async def _resolve_user_via_supabase(token: str) -> Optional[dict]:
        cleaned = str(token or "").strip()
        if not cleaned or not effective_supabase_url or not effective_supabase_anon_key:
            return None
        headers = {
            "apikey": effective_supabase_anon_key,
            "Authorization": f"Bearer {cleaned}",
        }
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                resp = await client.get(f"{effective_supabase_url}/auth/v1/user", headers=headers)
            if resp.status_code != 200:
                return None
            payload = resp.json()
        except Exception:
            return None
        data = payload.get("user") if isinstance(payload, dict) and isinstance(payload.get("user"), dict) else payload
        if not isinstance(data, dict):
            return None
        role = _extract_role(data)
        if role == "anon":
            return None
        user_id = str(data.get("id", "") or data.get("sub", "") or "").strip()
        if not user_id:
            return None
        return {
            "id": user_id,
            "email": str(data.get("email", "") or "").strip(),
        }

    async def get_current_user(
        cred: HTTPAuthorizationCredentials = Depends(security),
    ) -> Optional[dict]:
        if cred is None:
            return None
        token = str(getattr(cred, "credentials", "") or "").strip()
        if not token:
            return None

        user_id = ""
        email = ""
        try:
            payload = jwt.decode(
                token,
                supabase_jwt_secret,
                algorithms=["HS256"],
                options={"verify_aud": False},
            )
            role = _extract_role(payload)
            if role == "anon":
                return None
            user_id = str(payload.get("sub", "") or payload.get("id", "") or "").strip()
            email = str(payload.get("email", "") or "").strip()
        except jwt.exceptions.PyJWTError:
            payload = None

        if not user_id:
            resolved = await _resolve_user_via_supabase(token)
            if not resolved:
                return None
            user_id = str(resolved.get("id", "") or "").strip()
            email = str(resolved.get("email", "") or "").strip()
        if not user_id:
            return None

        plan = hardcoded_plans.get(email, "")

        if not plan and effective_supabase_url and effective_supabase_anon_key:
            try:
                svc_key = supabase_service_key or effective_supabase_anon_key
                async with httpx.AsyncClient(timeout=8) as client:
                    resp = await client.get(
                        f"{effective_supabase_url}/rest/v1/profiles?id=eq.{user_id}&select=plan,role",
                        headers={
                            "apikey": svc_key,
                            "Authorization": f"Bearer {svc_key}",
                        },
                    )
                    if resp.status_code == 200:
                        rows = resp.json()
                        if rows:
                            plan = rows[0].get("plan", "none")
            except Exception:
                pass

        plan = str(plan or "free").strip().lower() or "free"
        if plan == "none":
            plan = "free"

        manual_snapshot = paypal_snapshot_for_user(
            {"id": user_id, "email": email, "plan": plan}
        )
        if manual_snapshot.get("billing_active"):
            plan = str(manual_snapshot.get("plan", plan) or plan)

        return {"id": user_id, "email": email, "plan": plan}

    async def get_current_user_from_request(request: Request) -> Optional[dict]:
        token = _extract_request_token(request)
        if not token:
            return None

        class _FakeCred:
            credentials = ""

        fake = _FakeCred()
        fake.credentials = token
        return await get_current_user(fake)

    async def require_auth(
        cred: HTTPAuthorizationCredentials = Depends(security),
    ) -> dict:
        user = await get_current_user(cred)
        if not user:
            raise HTTPException(401, "Authentication required. Please sign in.")
        return user

    return security, get_current_user, get_current_user_from_request, require_auth
