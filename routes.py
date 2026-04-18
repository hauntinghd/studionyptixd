from __future__ import annotations

from typing import Callable, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, Request, Response, UploadFile


def build_core_router(
    *,
    require_auth,
    admin_emails: set[str],
    plan_limits: dict,
    demo_pro_price_id: str,
    product_demo_public_enabled: bool,
    youtube_oauth_mode: str,
    google_oauth_source: str,
    google_oauth_client_kind: str,
    google_oauth_config_issue: str,
    google_installed_oauth_source: str,
    google_installed_oauth_config_issue: str,
    youtube_connections_lock,
    load_youtube_connections: Callable[[], None],
    youtube_bucket_for_user: Callable[[str], dict],
    youtube_auth_configured: Callable[[], bool],
    youtube_active_oauth_mode: Callable[..., str],
    paid_access_snapshot_for_user: Callable[[dict], dict],
    stripe_subscription_snapshot: Callable[[str], dict],
    next_renewal_from_anchor: Callable[[int, int], int],
    credit_state_for_user: Callable[..., dict],
    membership_plan_for_user: Callable[[dict, dict], str],
    plan_features_for: Callable[..., list[str]],
    public_lane_access_for_user: Callable[..., dict],
    longform_deep_analysis_enabled: Callable[[dict | None], bool],
    projects_ref: dict,
):
    router = APIRouter()

    @router.get("/api/me")
    async def get_me(user: dict = Depends(require_auth)):
        email = user.get("email", "")
        is_admin = email in admin_emails
        access_snapshot = paid_access_snapshot_for_user(user)
        billing_active = bool(access_snapshot.get("billing_active"))
        plan = str(access_snapshot.get("plan", user.get("plan", "none")) or "none").strip().lower()
        if plan == "none":
            plan = "free"
        next_renewal_unix = int(access_snapshot.get("next_renewal_unix", 0) or 0)
        next_renewal_source = str(access_snapshot.get("next_renewal_source", "") or "")
        billing_anchor_unix = int(access_snapshot.get("billing_anchor_unix", 0) or 0)
        if (
            billing_active
            and str(access_snapshot.get("source", "") or "") == "stripe"
            and next_renewal_unix <= 0
            and billing_anchor_unix > 0
        ):
            stripe_diag = stripe_subscription_snapshot(email)
            interval_months = max(1, int((stripe_diag or {}).get("interval_months", 1) or 1))
            rolled = next_renewal_from_anchor(billing_anchor_unix, interval_months)
            if rolled > 0:
                next_renewal_unix = int(rolled)
                next_renewal_source = next_renewal_source or "paid_at_rollforward_fallback"
        if is_admin:
            limits = plan_limits["pro"]
            limits = {**limits, "videos_per_month": 9999}
        else:
            limits = plan_limits.get(plan, plan_limits.get("free", plan_limits["starter"]))
        credit_state = credit_state_for_user(user, plan if not is_admin else "pro", billing_active, is_admin=is_admin)
        has_demo = is_admin or (product_demo_public_enabled and plan == "demo_pro")
        effective_plan = "pro" if is_admin else plan
        membership_plan_id = membership_plan_for_user(user, access_snapshot)
        features = plan_features_for(effective_plan, is_admin=is_admin)
        lane_access = public_lane_access_for_user(user, access_snapshot)
        async with youtube_connections_lock:
            load_youtube_connections()
            yt_bucket = youtube_bucket_for_user(str(user.get("id", "") or ""))
            yt_channels = dict(yt_bucket.get("channels") or {})
            yt_default_channel_id = str(yt_bucket.get("default_channel_id", "") or "")
        return {
            "id": user["id"],
            "email": email,
            "plan": effective_plan,
            "role": "admin" if is_admin else "user",
            "owner_override": is_admin,
            "billing_active": billing_active,
            "membership_active": billing_active,
            "membership_plan_id": membership_plan_id,
            "membership_source": str(access_snapshot.get("source", "") or ""),
            "membership_label": "Catalyst Membership" if billing_active or is_admin else "",
            "next_renewal_unix": next_renewal_unix,
            "next_renewal_source": next_renewal_source,
            "billing_anchor_unix": billing_anchor_unix,
            "limits": limits,
            "features": features,
            "lane_access": lane_access,
            "animated_credits_remaining": credit_state["animated_monthly_remaining"],
            "animated_credits_used": credit_state["animated_monthly_used"],
            "animated_credits_limit": credit_state["animated_monthly_limit"],
            "animated_topup_credits_remaining": credit_state["animated_topup_credits"],
            "animated_credits_total_remaining": credit_state["animated_total_remaining"],
            "non_animated_ops_remaining": credit_state["non_animated_monthly_remaining"],
            "non_animated_ops_used": credit_state["non_animated_monthly_used"],
            "non_animated_ops_limit": credit_state["non_animated_monthly_limit"],
            "monthly_credits_remaining": credit_state["monthly_remaining"],
            "monthly_credits_used": credit_state["monthly_used"],
            "monthly_credits_limit": credit_state["monthly_limit"],
            "topup_credits_remaining": credit_state["topup_credits"],
            "credits_total_remaining": credit_state["credits_total_remaining"],
            "included_credits_remaining": credit_state["monthly_remaining"],
            "included_credits_used": credit_state["monthly_used"],
            "included_credits_limit": credit_state["monthly_limit"],
            "credit_wallet_balance": credit_state["topup_credits"],
            "billing_source_precedence": ["monthly", "topup"],
            "requires_topup": credit_state["requires_topup"],
            "credit_month": credit_state["month_key"],
            "demo_access": has_demo,
            "demo_price_id": demo_pro_price_id,
            "demo_coming_soon": (not product_demo_public_enabled),
            "longform_owner_beta": bool(longform_deep_analysis_enabled(user)),
            "youtube_oauth_configured": youtube_auth_configured(),
            "youtube_oauth_preferred_mode": youtube_oauth_mode,
            "youtube_oauth_active_mode": youtube_active_oauth_mode(),
            "youtube_oauth_source": google_oauth_source,
            "youtube_oauth_client_kind": google_oauth_client_kind,
            "youtube_oauth_issue": google_oauth_config_issue,
            "youtube_installed_oauth_source": google_installed_oauth_source,
            "youtube_installed_oauth_issue": google_installed_oauth_config_issue,
            "youtube_connected_channel_count": len(yt_channels),
            "youtube_default_channel_id": yt_default_channel_id,
        }

    @router.get("/api/projects")
    async def list_projects(request: Request = None):
        user = await require_auth_from_request(request, require_auth)
        uid = user.get("id", "")
        rows = [p for p in projects_ref.values() if p.get("user_id") == uid]
        rows.sort(key=lambda p: p.get("updated_at", 0), reverse=True)
        drafts = [p for p in rows if p.get("status") in ("draft", "rendering")]
        renders = [p for p in rows if p.get("status") in ("rendered", "error")]
        return {"drafts": drafts, "renders": renders, "total": len(rows)}

    @router.get("/api/projects/{project_id}")
    async def get_project(project_id: str, request: Request = None):
        user = await require_auth_from_request(request, require_auth)
        proj = projects_ref.get(project_id)
        if not proj or proj.get("user_id") != user.get("id"):
            raise HTTPException(404, "Project not found")
        return {"project": proj}

    return router


def build_misc_router(
    *,
    require_auth,
    list_languages_payload: Callable[[], dict],
    health_payload,
    set_comfyui_url_handler,
    training_stats_handler,
    admin_analytics_handler,
    admin_waiting_list_handler,
    admin_billing_audit_handler,
    set_maintenance_banner_handler,
    public_config_payload,
    landing_notifications_payload,
):
    router = APIRouter()

    @router.get("/api/languages")
    async def list_languages():
        return list_languages_payload()

    @router.get("/api/health")
    async def health():
        return await health_payload()

    @router.head("/api/health")
    async def health_head():
        return Response(status_code=200)

    @router.post("/api/admin/comfyui-url")
    async def set_comfyui_url(body: dict, user: dict = Depends(require_auth)):
        return await set_comfyui_url_handler(body, user)

    @router.get("/api/admin/training-stats")
    async def training_stats(user: dict = Depends(require_auth)):
        return await training_stats_handler(user)

    @router.get("/api/admin/analytics")
    async def admin_analytics(user: dict = Depends(require_auth)):
        return await admin_analytics_handler(user)

    @router.get("/api/admin/waiting-list")
    async def admin_waiting_list(user: dict = Depends(require_auth)):
        return await admin_waiting_list_handler(user)

    @router.get("/api/admin/billing-audit")
    async def admin_billing_audit(user: dict = Depends(require_auth)):
        return await admin_billing_audit_handler(user)

    @router.post("/api/admin/maintenance-banner")
    async def admin_set_maintenance_banner(body: dict, user: dict = Depends(require_auth)):
        return await set_maintenance_banner_handler(body, user)

    @router.get("/api/config")
    async def public_config():
        return await public_config_payload()

    @router.get("/api/landing/notifications")
    async def landing_notifications_feed():
        return await landing_notifications_payload()

    return router


def build_media_router(
    *,
    auto_scene_image_handler,
    auto_regenerate_scene_image_handler,
    job_status_handler,
    download_video_handler,
    render_chat_story_handler,
    clone_video_handler,
    list_jobs_handler,
):
    router = APIRouter()

    @router.get("/api/auto/scene-image/{job_id}/{filename}")
    async def auto_scene_image(job_id: str, filename: str):
        return await auto_scene_image_handler(job_id, filename)

    @router.post("/api/auto/regenerate-scene-image")
    async def auto_regenerate_scene_image(body: dict, request: Request = None):
        return await auto_regenerate_scene_image_handler(body, request)

    @router.get("/api/status/{job_id}")
    async def job_status(job_id: str):
        return await job_status_handler(job_id)

    @router.get("/api/download/{filename}")
    async def download_video(filename: str):
        return await download_video_handler(filename)

    @router.post("/api/chatstory/render")
    async def render_chat_story(
        request: Request,
        payload: str = Form(...),
        avatar: Optional[UploadFile] = File(None),
        background_video: Optional[UploadFile] = File(None),
    ):
        return await render_chat_story_handler(request, payload, avatar, background_video)

    @router.post("/api/clone")
    async def clone_video(
        topic: str = Form(""),
        resolution: str = Form("720p"),
        source_url: str = Form(""),
        analytics_notes: str = Form(""),
        file: UploadFile = File(None),
        background_tasks: BackgroundTasks = None,
        request: Request = None,
    ):
        return await clone_video_handler(
            topic,
            resolution,
            source_url,
            analytics_notes,
            file,
            background_tasks,
            request,
        )

    @router.get("/api/jobs")
    async def list_jobs():
        return await list_jobs_handler()

    return router


def build_billing_router(
    *,
    create_checkout_endpoint,
    create_topup_checkout_endpoint,
    paypal_return_endpoint,
    paypal_webhook_endpoint,
    paypal_verify_order_endpoint,
    create_billing_portal_session_endpoint,
    join_waitlist_endpoint,
    stripe_webhook_endpoint,
    admin_set_plan_endpoint,
    admin_cancel_subscription_endpoint,
    submit_feedback_endpoint,
    get_all_feedback_endpoint,
    get_admin_kpi_endpoint,
    get_admin_youtube_quota_endpoint,
    admin_catalyst_backfill_tick_endpoint,
    get_admin_catalyst_corpus_endpoint,
    get_admin_youtube_video_retention_endpoint,
):
    router = APIRouter()
    router.add_api_route("/api/checkout", create_checkout_endpoint, methods=["POST"])
    router.add_api_route("/api/checkout/topup", create_topup_checkout_endpoint, methods=["POST"])
    router.add_api_route("/api/paypal/return", paypal_return_endpoint, methods=["GET"])
    router.add_api_route("/api/paypal/webhook", paypal_webhook_endpoint, methods=["POST"])
    router.add_api_route("/api/paypal/verify/{order_id}", paypal_verify_order_endpoint, methods=["GET"])
    router.add_api_route("/api/billing-portal", create_billing_portal_session_endpoint, methods=["POST"])
    router.add_api_route("/api/waitlist/join", join_waitlist_endpoint, methods=["POST"])
    router.add_api_route("/api/stripe-webhook", stripe_webhook_endpoint, methods=["POST"])
    router.add_api_route("/api/admin/set-plan", admin_set_plan_endpoint, methods=["POST"])
    router.add_api_route("/api/admin/cancel-subscription", admin_cancel_subscription_endpoint, methods=["POST"])
    router.add_api_route("/api/feedback", submit_feedback_endpoint, methods=["POST"])
    router.add_api_route("/api/admin/feedback", get_all_feedback_endpoint, methods=["GET"])
    router.add_api_route("/api/admin/kpi", get_admin_kpi_endpoint, methods=["GET"])
    router.add_api_route("/api/admin/youtube-quota", get_admin_youtube_quota_endpoint, methods=["GET"])
    router.add_api_route("/api/admin/catalyst/backfill-tick", admin_catalyst_backfill_tick_endpoint, methods=["POST"])
    router.add_api_route("/api/admin/catalyst/corpus", get_admin_catalyst_corpus_endpoint, methods=["GET"])
    router.add_api_route("/api/admin/youtube/video-retention", get_admin_youtube_video_retention_endpoint, methods=["GET"])
    return router


def build_assets_router(
    *,
    training_status_endpoint,
    sync_thumbnail_library_endpoint,
    upload_thumbnails_endpoint,
    list_thumbnails_endpoint,
    thumbnail_feedback_endpoint,
    serve_thumbnail_endpoint,
    delete_thumbnail_endpoint,
    serve_public_thumbnail_share_endpoint,
    serve_generated_thumbnail_endpoint,
    generate_thumbnail_endpoint,
    list_voices_endpoint,
    preview_voice_endpoint,
    create_demo_video_endpoint,
):
    router = APIRouter()
    router.add_api_route("/api/thumbnails/training-status", training_status_endpoint, methods=["GET"])
    router.add_api_route("/api/thumbnails/sync-library", sync_thumbnail_library_endpoint, methods=["POST"])
    router.add_api_route("/api/thumbnails/upload", upload_thumbnails_endpoint, methods=["POST"])
    router.add_api_route("/api/thumbnails/library", list_thumbnails_endpoint, methods=["GET"])
    router.add_api_route("/api/thumbnails/feedback", thumbnail_feedback_endpoint, methods=["POST"])
    router.add_api_route("/api/thumbnails/library/{filename}", serve_thumbnail_endpoint, methods=["GET"])
    router.add_api_route("/api/thumbnails/library/{filename}", delete_thumbnail_endpoint, methods=["DELETE"])
    router.add_api_route("/api/public/thumbnail-share/{token}", serve_public_thumbnail_share_endpoint, methods=["GET"])
    router.add_api_route("/api/thumbnails/generated/{filename}", serve_generated_thumbnail_endpoint, methods=["GET"])
    router.add_api_route("/api/thumbnails/generate", generate_thumbnail_endpoint, methods=["POST"])
    router.add_api_route("/api/voices", list_voices_endpoint, methods=["GET"])
    router.add_api_route("/api/voices/preview", preview_voice_endpoint, methods=["POST"])
    router.add_api_route("/api/demo", create_demo_video_endpoint, methods=["POST"])
    return router


def build_youtube_catalyst_router(
    *,
    start_google_youtube_oauth_endpoint,
    start_google_youtube_oauth_browser_endpoint,
    google_youtube_oauth_installed_helper_endpoint,
    google_youtube_oauth_complete_endpoint,
    google_youtube_oauth_callback_endpoint,
    catalyst_hub_snapshot_endpoint,
    catalyst_hub_refresh_endpoint,
    catalyst_hub_reference_video_analysis_endpoint,
    catalyst_hub_reference_video_analysis_manual_endpoint,
    catalyst_hub_reference_video_clear_endpoint,
    catalyst_hub_save_instructions_endpoint,
    catalyst_hub_launch_endpoint,
    catalyst_hub_longform_suggestions_endpoint=None,
    list_youtube_channels_endpoint,
    select_youtube_channel_endpoint,
    sync_youtube_channel_endpoint,
    sync_youtube_channel_outcomes_endpoint,
    delete_youtube_channel_endpoint,
    catalyst_auto_tick_endpoint=None,
    catalyst_auto_pilot_endpoint=None,
    catalyst_upload_endpoint=None,
    catalyst_velocity_endpoint=None,
):
    router = APIRouter()
    router.add_api_route("/api/oauth/google/youtube/start", start_google_youtube_oauth_endpoint, methods=["POST"])
    router.add_api_route("/api/oauth/google/youtube/browser-start", start_google_youtube_oauth_browser_endpoint, methods=["POST"])
    router.add_api_route("/api/oauth/google/youtube/installed", google_youtube_oauth_installed_helper_endpoint, methods=["GET"])
    router.add_api_route("/api/oauth/google/youtube/complete", google_youtube_oauth_complete_endpoint, methods=["POST"])
    router.add_api_route("/api/oauth/google/youtube/callback", google_youtube_oauth_callback_endpoint, methods=["GET"])
    router.add_api_route("/api/catalyst/hub", catalyst_hub_snapshot_endpoint, methods=["GET"])
    router.add_api_route("/api/catalyst/hub/refresh", catalyst_hub_refresh_endpoint, methods=["POST"])
    router.add_api_route("/api/catalyst/hub/reference-video-analysis", catalyst_hub_reference_video_analysis_endpoint, methods=["POST"])
    router.add_api_route("/api/catalyst/hub/reference-video-analysis/manual", catalyst_hub_reference_video_analysis_manual_endpoint, methods=["POST"])
    router.add_api_route("/api/catalyst/hub/reference-video-analysis/clear", catalyst_hub_reference_video_clear_endpoint, methods=["POST"])
    router.add_api_route("/api/catalyst/hub/instructions", catalyst_hub_save_instructions_endpoint, methods=["POST"])
    router.add_api_route("/api/catalyst/hub/launch", catalyst_hub_launch_endpoint, methods=["POST"])
    if catalyst_hub_longform_suggestions_endpoint:
        router.add_api_route("/api/catalyst/hub/longform-suggestions", catalyst_hub_longform_suggestions_endpoint, methods=["POST"])
    router.add_api_route("/api/youtube/channels", list_youtube_channels_endpoint, methods=["GET"])
    router.add_api_route("/api/youtube/channels/select", select_youtube_channel_endpoint, methods=["POST"])
    router.add_api_route("/api/youtube/channels/{channel_id}/sync", sync_youtube_channel_endpoint, methods=["POST"])
    router.add_api_route("/api/youtube/channels/{channel_id}/sync-outcomes", sync_youtube_channel_outcomes_endpoint, methods=["POST"])
    router.add_api_route("/api/youtube/channels/{channel_id}", delete_youtube_channel_endpoint, methods=["DELETE"])
    if catalyst_auto_tick_endpoint:
        router.add_api_route("/api/catalyst/hub/auto-tick", catalyst_auto_tick_endpoint, methods=["POST"])
    if catalyst_auto_pilot_endpoint:
        router.add_api_route("/api/catalyst/hub/auto-pilot", catalyst_auto_pilot_endpoint, methods=["POST"])
    if catalyst_upload_endpoint:
        router.add_api_route("/api/catalyst/hub/upload", catalyst_upload_endpoint, methods=["POST"])
    if catalyst_velocity_endpoint:
        router.add_api_route("/api/catalyst/hub/velocity", catalyst_velocity_endpoint, methods=["GET"])
    return router


def build_longform_creative_router(
    *,
    create_longform_session_endpoint,
    create_longform_session_bootstrap_endpoint,
    longform_reference_image_endpoint,
    longform_character_reference_endpoint,
    longform_scene_assignment_endpoint,
    longform_reference_file_endpoint,
    longform_session_status_endpoint,
    list_longform_sessions_endpoint,
    longform_preview_file_endpoint,
    longform_chapter_action_endpoint,
    longform_resolve_error_endpoint,
    longform_force_clear_error_endpoint=None,
    longform_finalize_endpoint,
    longform_stop_session_endpoint,
    longform_ingest_outcome_endpoint,
    longform_auto_ingest_outcome_endpoint,
    creative_generate_script_endpoint,
    creative_ingest_url_endpoint,
    creative_create_session_endpoint,
    creative_reference_image_endpoint,
    creative_reference_file_endpoint,
    creative_session_status_endpoint,
    creative_session_scene_images_endpoint,
    creative_scene_image_endpoint,
    creative_scene_feedback_endpoint,
    creative_update_scene_endpoint,
    creative_finalize_endpoint,
):
    router = APIRouter()
    router.add_api_route("/api/longform/session", create_longform_session_endpoint, methods=["POST"])
    router.add_api_route("/api/longform/session/bootstrap", create_longform_session_bootstrap_endpoint, methods=["POST"])
    router.add_api_route("/api/longform/session/{session_id}/reference-image", longform_reference_image_endpoint, methods=["POST"])
    router.add_api_route("/api/longform/session/{session_id}/character-reference", longform_character_reference_endpoint, methods=["POST"])
    router.add_api_route("/api/longform/session/{session_id}/scene-assignment", longform_scene_assignment_endpoint, methods=["POST"])
    router.add_api_route("/api/longform/reference-file/{filename}", longform_reference_file_endpoint, methods=["GET"])
    router.add_api_route("/api/longform/session/{session_id}/status", longform_session_status_endpoint, methods=["GET"])
    router.add_api_route("/api/longform/sessions", list_longform_sessions_endpoint, methods=["GET"])
    router.add_api_route("/api/longform/preview/{filename}", longform_preview_file_endpoint, methods=["GET"])
    router.add_api_route("/api/longform/session/{session_id}/chapter-action", longform_chapter_action_endpoint, methods=["POST"])
    router.add_api_route("/api/longform/session/{session_id}/resolve-error", longform_resolve_error_endpoint, methods=["POST"])
    if longform_force_clear_error_endpoint:
        router.add_api_route("/api/longform/session/{session_id}/force-clear-error", longform_force_clear_error_endpoint, methods=["POST"])
    router.add_api_route("/api/longform/session/{session_id}/finalize", longform_finalize_endpoint, methods=["POST"])
    router.add_api_route("/api/longform/session/{session_id}/stop", longform_stop_session_endpoint, methods=["POST"])
    router.add_api_route("/api/longform/session/{session_id}/outcome", longform_ingest_outcome_endpoint, methods=["POST"])
    router.add_api_route("/api/longform/session/{session_id}/outcome/auto", longform_auto_ingest_outcome_endpoint, methods=["POST"])
    router.add_api_route("/api/creative/script", creative_generate_script_endpoint, methods=["POST"])
    router.add_api_route("/api/creative/ingest-url", creative_ingest_url_endpoint, methods=["POST"])
    router.add_api_route("/api/creative/session", creative_create_session_endpoint, methods=["POST"])
    router.add_api_route("/api/creative/reference-image", creative_reference_image_endpoint, methods=["POST"])
    router.add_api_route("/api/creative/reference-file/{filename}", creative_reference_file_endpoint, methods=["GET"])
    router.add_api_route("/api/creative/session/{session_id}/status", creative_session_status_endpoint, methods=["GET"])
    router.add_api_route("/api/creative/session/{session_id}/scene-images", creative_session_scene_images_endpoint, methods=["GET"])
    router.add_api_route("/api/creative/scene-image", creative_scene_image_endpoint, methods=["POST"])
    router.add_api_route("/api/creative/scene-feedback", creative_scene_feedback_endpoint, methods=["POST"])
    router.add_api_route("/api/creative/scene/{session_id}/{scene_index}", creative_update_scene_endpoint, methods=["PUT"])
    router.add_api_route("/api/creative/finalize", creative_finalize_endpoint, methods=["POST"])
    return router


def build_generation_router(*, generate_short_endpoint):
    router = APIRouter()
    router.add_api_route("/api/generate", generate_short_endpoint, methods=["POST"])
    return router


async def require_auth_from_request(request: Request, require_auth) -> dict:
    if request is None:
        raise HTTPException(401, "Auth required")
    user = await _get_current_user_from_request_via_dependency(request, require_auth)
    if not user:
        raise HTTPException(401, "Auth required")
    return user


async def _get_current_user_from_request_via_dependency(request: Request, require_auth):
    token = ""
    if request:
        for header_name in ("authorization", "x-access-token", "x-auth-token"):
            header_value = str(request.headers.get(header_name) or "").strip()
            if not header_value:
                continue
            parts = header_value.split(None, 1)
            if len(parts) == 2 and str(parts[0] or "").strip().lower() == "bearer":
                token = str(parts[1] or "").strip()
                break
            if header_name != "authorization":
                token = header_value
                break
    if not token and request:
        token = str(request.query_params.get("access_token", "") or request.query_params.get("token", "") or "").strip()
    if not token:
        return None

    class _FakeCred:
        credentials = ""

    fake = _FakeCred()
    fake.credentials = token
    try:
        return await require_auth(fake)
    except HTTPException:
        return None
