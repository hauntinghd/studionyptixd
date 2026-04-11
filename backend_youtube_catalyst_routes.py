from __future__ import annotations

import logging
from pathlib import Path

from fastapi import Depends, File, Form, HTTPException, Request, UploadFile

from backend_models import (
    CatalystChannelOutcomeSyncRequest,
    CatalystHubDirectiveRequest,
    CatalystHubLaunchRequest,
    CatalystHubReferenceVideoAnalysisRequest,
    CatalystHubReferenceVideoClearRequest,
    CatalystHubRefreshRequest,
    YouTubeChannelSelectRequest,
    YouTubeOAuthStartRequest,
)
from routes import build_youtube_catalyst_router

log = logging.getLogger("nyptid-studio")

# In-memory state to prevent overlapping auto-tick runs
_catalyst_auto_tick_running: dict[str, bool] = {}


def build_youtube_catalyst_app_router(
    *,
    require_auth,
    get_current_user,
    get_current_user_from_request,
    youtube_start_oauth_for_user,
    youtube_start_oauth_browser_redirect,
    google_youtube_oauth_installed_helper_response,
    google_youtube_oauth_complete_redirect,
    google_youtube_oauth_callback_redirect,
    catalyst_hub_snapshot_for_user,
    catalyst_hub_refresh_for_user,
    catalyst_hub_reference_video_analysis_for_user,
    catalyst_hub_reference_video_analysis_manual_for_user,
    catalyst_hub_clear_reference_video_analysis_for_user,
    catalyst_hub_save_instructions_for_user,
    catalyst_hub_launch_longform_for_user,
    list_connected_youtube_channels_for_user,
    select_connected_youtube_channel_for_user,
    sync_connected_youtube_channel_for_user,
    sync_connected_youtube_channel_outcomes_for_user,
    disconnect_connected_youtube_channel_for_user,
    bool_from_any,
    catalyst_reference_analysis_default_minutes: float,
    upload_dir: Path,
    longform_owner_beta_enabled,
    harvest_catalyst_outcomes_for_channel,
    youtube_upload_video_for_user=None,
    youtube_get_velocity_for_user=None,
):
    async def _start_google_youtube_oauth(
        req: YouTubeOAuthStartRequest,
        user: dict = Depends(require_auth),
    ):
        return await youtube_start_oauth_for_user(user, str((req or {}).next_url or "").strip())

    async def _start_google_youtube_oauth_browser(
        next_url: str = Form(""),
        access_token: str = Form(""),
    ):
        return await youtube_start_oauth_browser_redirect(
            next_url=next_url,
            access_token=access_token,
            get_current_user=get_current_user,
        )

    async def _google_youtube_oauth_installed_helper(state: str = ""):
        return await google_youtube_oauth_installed_helper_response(state=state)

    async def _google_youtube_oauth_complete(state: str = Form(""), redirect_url: str = Form("")):
        return await google_youtube_oauth_complete_redirect(state=state, redirect_url=redirect_url)

    async def _google_youtube_oauth_callback(
        code: str = "",
        state: str = "",
        error: str = "",
    ):
        return await google_youtube_oauth_callback_redirect(code=code, state=state, error=error)

    async def _catalyst_hub_snapshot(
        request: Request,
        channel_id: str = "",
        refresh: bool = False,
    ):
        user = await get_current_user_from_request(request)
        if not user:
            raise HTTPException(401, "Auth required")
        return await catalyst_hub_snapshot_for_user(
            user=user,
            channel_id=str(channel_id or "").strip(),
            refresh=bool(refresh),
        )

    async def _catalyst_hub_refresh(
        req: CatalystHubRefreshRequest,
        user: dict = Depends(require_auth),
    ):
        return await catalyst_hub_refresh_for_user(
            user=user,
            channel_id=str((req or {}).channel_id or "").strip(),
            include_public_benchmarks=bool((req or {}).include_public_benchmarks),
            refresh_outcomes=bool((req or {}).refresh_outcomes),
        )

    async def _catalyst_hub_reference_video_analysis(
        req: CatalystHubReferenceVideoAnalysisRequest,
        user: dict = Depends(require_auth),
    ):
        return await catalyst_hub_reference_video_analysis_for_user(
            user=user,
            channel_id=str((req or {}).channel_id or "").strip(),
            workspace_id=str((req or {}).workspace_id or "documentary").strip().lower() or "documentary",
            video_id=str((req or {}).video_id or "").strip(),
            max_analysis_minutes=float((req or {}).max_analysis_minutes or catalyst_reference_analysis_default_minutes),
        )

    async def _catalyst_hub_reference_video_analysis_manual(
        channel_id: str = Form(""),
        workspace_id: str = Form("documentary"),
        video_id: str = Form(""),
        max_analysis_minutes: float = Form(catalyst_reference_analysis_default_minutes),
        reference_source_url: str = Form(""),
        reference_title: str = Form(""),
        reference_channel: str = Form(""),
        analytics_notes: str = Form(""),
        transcript_text: str = Form(""),
        reference_video: UploadFile | None = File(None),
        comparison_video: UploadFile | None = File(None),
        analytics_images: list[UploadFile] = File([]),
        user: dict = Depends(require_auth),
    ):
        return await catalyst_hub_reference_video_analysis_manual_for_user(
            user=user,
            channel_id=str(channel_id or "").strip(),
            workspace_id=str(workspace_id or "documentary").strip().lower() or "documentary",
            video_id=str(video_id or "").strip(),
            max_analysis_minutes=float(max_analysis_minutes or catalyst_reference_analysis_default_minutes),
            reference_source_url=str(reference_source_url or "").strip(),
            reference_title=str(reference_title or "").strip(),
            reference_channel=str(reference_channel or "").strip(),
            analytics_notes=str(analytics_notes or "").strip(),
            transcript_text=str(transcript_text or "").strip(),
            reference_video=reference_video,
            comparison_video=comparison_video,
            analytics_images=list(analytics_images or []),
            upload_dir=upload_dir,
        )

    async def _catalyst_hub_clear_reference_video_analysis(
        req: CatalystHubReferenceVideoClearRequest,
        user: dict = Depends(require_auth),
    ):
        return await catalyst_hub_clear_reference_video_analysis_for_user(
            user=user,
            channel_id=str((req or {}).channel_id or "").strip(),
            workspace_id=str((req or {}).workspace_id or "documentary").strip().lower() or "documentary",
        )

    async def _catalyst_hub_save_instructions(
        req: CatalystHubDirectiveRequest,
        user: dict = Depends(require_auth),
    ):
        return await catalyst_hub_save_instructions_for_user(
            user=user,
            channel_id=str((req or {}).channel_id or "").strip(),
            directive=str((req or {}).directive or "").strip(),
            mission=str((req or {}).mission or "").strip(),
            guardrails=list((req or {}).guardrails or []),
            target_niches=list((req or {}).target_niches or []),
            apply_scope=str((req or {}).apply_scope or "all").strip().lower() or "all",
        )

    async def _catalyst_hub_launch_longform(
        req: CatalystHubLaunchRequest,
        user: dict = Depends(require_auth),
    ):
        return await catalyst_hub_launch_longform_for_user(
            user=user,
            channel_id=str((req or {}).channel_id or "").strip(),
            workspace_id=str((req or {}).workspace_id or "").strip().lower(),
            mission=str((req or {}).mission or "").strip(),
            directive=str((req or {}).directive or "").strip(),
            guardrails=list((req or {}).guardrails or []),
            target_niches=list((req or {}).target_niches or []),
            include_public_benchmarks=bool_from_any((req or {}).include_public_benchmarks, True),
            refresh_outcomes=bool_from_any((req or {}).refresh_outcomes, True),
            target_minutes=float((req or {}).target_minutes or 0.0),
            language=str((req or {}).language or "en"),
            animation_enabled=bool_from_any((req or {}).animation_enabled, True),
            sfx_enabled=bool_from_any((req or {}).sfx_enabled, True),
            auto_pipeline=bool_from_any((req or {}).auto_pipeline, True),
        )

    async def _list_connected_youtube_channels(
        request: Request,
        sync: bool = True,
    ):
        user = await get_current_user_from_request(request)
        if not user:
            raise HTTPException(401, "Auth required")
        return await list_connected_youtube_channels_for_user(user=user, sync=sync)

    async def _select_connected_youtube_channel(
        req: YouTubeChannelSelectRequest,
        request: Request,
    ):
        user = await get_current_user_from_request(request)
        if not user:
            raise HTTPException(401, "Auth required")
        return await select_connected_youtube_channel_for_user(
            user=user,
            channel_id=str((req or {}).channel_id or "").strip(),
        )

    async def _sync_connected_youtube_channel(channel_id: str, request: Request):
        user = await get_current_user_from_request(request)
        if not user:
            raise HTTPException(401, "Auth required")
        return await sync_connected_youtube_channel_for_user(user=user, channel_id=channel_id)

    async def _sync_connected_youtube_channel_outcomes(
        channel_id: str,
        req: CatalystChannelOutcomeSyncRequest,
        request: Request,
    ):
        user = await get_current_user_from_request(request)
        if not user:
            raise HTTPException(401, "Auth required")
        return await sync_connected_youtube_channel_outcomes_for_user(
            user=user,
            channel_id=channel_id,
            session_id=str((req or {}).session_id or "").strip(),
            candidate_limit=int((req or {}).candidate_limit or 18),
            refresh_existing=bool((req or {}).refresh_existing),
            longform_owner_beta_enabled=longform_owner_beta_enabled,
            harvest_catalyst_outcomes_for_channel=harvest_catalyst_outcomes_for_channel,
        )

    async def _disconnect_connected_youtube_channel(channel_id: str, request: Request):
        user = await get_current_user_from_request(request)
        if not user:
            raise HTTPException(401, "Auth required")
        return await disconnect_connected_youtube_channel_for_user(user=user, channel_id=channel_id)

    # ─── Autonomous Pipeline Endpoints ─────────────────────────────────

    async def _catalyst_upload_video(
        request: Request,
        session_id: str = Form(""),
        channel_id: str = Form(""),
        privacy: str = Form("private"),
    ):
        """Upload a completed longform session's video to YouTube."""
        user = await get_current_user_from_request(request)
        if not user:
            raise HTTPException(401, "Auth required")
        if not youtube_upload_video_for_user:
            raise HTTPException(501, "YouTube upload not configured")
        return await youtube_upload_video_for_user(
            user=user,
            session_id=str(session_id).strip(),
            channel_id=str(channel_id).strip(),
            privacy=str(privacy).strip() or "private",
        )

    async def _catalyst_velocity(
        request: Request,
        channel_id: str = "",
    ):
        """Get latest video's view velocity for decay detection."""
        user = await get_current_user_from_request(request)
        if not user:
            raise HTTPException(401, "Auth required")
        if not youtube_get_velocity_for_user:
            raise HTTPException(501, "Velocity detection not configured")
        return await youtube_get_velocity_for_user(
            user=user,
            channel_id=str(channel_id).strip(),
        )

    async def _catalyst_auto_tick(
        request: Request,
        channel_id: str = Form(""),
        workspace: str = Form("documentary"),
    ):
        """Autonomous pipeline tick: check decay → generate → upload.

        Safe to call repeatedly; prevents overlapping runs.
        """
        user = await get_current_user_from_request(request)
        if not user:
            raise HTTPException(401, "Auth required")
        user_id = str(user.get("id", user.get("sub", "")) or "unknown")
        tick_key = f"{user_id}:{channel_id}"

        if _catalyst_auto_tick_running.get(tick_key):
            return {"status": "already_running", "message": "An autonomous run is already in progress for this channel"}

        _catalyst_auto_tick_running[tick_key] = True
        try:
            # Step 1: Check velocity / decay
            velocity_data = {}
            if youtube_get_velocity_for_user and channel_id:
                try:
                    velocity_data = await youtube_get_velocity_for_user(user=user, channel_id=channel_id)
                except Exception as vel_exc:
                    log.warning("Auto-tick velocity check failed: %s", str(vel_exc)[:200])

            is_decaying = velocity_data.get("is_decaying", True)  # Default to True if can't check
            velocity_vph = velocity_data.get("velocity_vph", 0)

            if not is_decaying:
                return {
                    "status": "not_decaying",
                    "velocity_vph": velocity_vph,
                    "message": f"Latest video still performing ({velocity_vph} views/hr). No new video needed yet.",
                }

            # Step 2: Launch new longform pipeline
            log.info("Auto-tick: decay detected (%.1f vph), launching new longform for channel %s", velocity_vph, channel_id)
            launch_result = await catalyst_hub_launch_longform_for_user(
                user=user,
                channel_id=channel_id,
                workspace=workspace,
                auto_pipeline=True,
            )

            return {
                "status": "launched",
                "velocity_vph": velocity_vph,
                "session_id": launch_result.get("session_id", ""),
                "message": f"Decay detected ({velocity_vph} vph). New longform pipeline launched.",
            }
        finally:
            _catalyst_auto_tick_running[tick_key] = False

    async def _catalyst_auto_pilot_toggle(
        request: Request,
        channel_id: str = Form(""),
        enabled: str = Form("true"),
        interval_hours: str = Form("6"),
    ):
        user = await get_current_user_from_request(request)
        if not user:
            raise HTTPException(401, "Auth required")
        user_id = str(user.get("id", user.get("sub", "")) or "unknown")
        key = f"{user_id}:{channel_id}"
        from backend import _catalyst_auto_pilot_channels
        _catalyst_auto_pilot_channels[key] = {
            "enabled": str(enabled).lower() in ("true", "1", "yes"),
            "interval_hours": max(1, min(24, float(interval_hours or 6))),
            "last_check": 0,
            "channel_id": channel_id,
        }
        return {
            "status": "ok",
            "enabled": _catalyst_auto_pilot_channels[key]["enabled"],
            "interval_hours": _catalyst_auto_pilot_channels[key]["interval_hours"],
        }

    return build_youtube_catalyst_router(
        start_google_youtube_oauth_endpoint=_start_google_youtube_oauth,
        start_google_youtube_oauth_browser_endpoint=_start_google_youtube_oauth_browser,
        google_youtube_oauth_installed_helper_endpoint=_google_youtube_oauth_installed_helper,
        google_youtube_oauth_complete_endpoint=_google_youtube_oauth_complete,
        google_youtube_oauth_callback_endpoint=_google_youtube_oauth_callback,
        catalyst_hub_snapshot_endpoint=_catalyst_hub_snapshot,
        catalyst_hub_refresh_endpoint=_catalyst_hub_refresh,
        catalyst_hub_reference_video_analysis_endpoint=_catalyst_hub_reference_video_analysis,
        catalyst_hub_reference_video_analysis_manual_endpoint=_catalyst_hub_reference_video_analysis_manual,
        catalyst_hub_reference_video_clear_endpoint=_catalyst_hub_clear_reference_video_analysis,
        catalyst_hub_save_instructions_endpoint=_catalyst_hub_save_instructions,
        catalyst_hub_launch_endpoint=_catalyst_hub_launch_longform,
        list_youtube_channels_endpoint=_list_connected_youtube_channels,
        select_youtube_channel_endpoint=_select_connected_youtube_channel,
        sync_youtube_channel_endpoint=_sync_connected_youtube_channel,
        sync_youtube_channel_outcomes_endpoint=_sync_connected_youtube_channel_outcomes,
        delete_youtube_channel_endpoint=_disconnect_connected_youtube_channel,
        catalyst_auto_tick_endpoint=_catalyst_auto_tick,
        catalyst_auto_pilot_endpoint=_catalyst_auto_pilot_toggle,
        catalyst_upload_endpoint=_catalyst_upload_video if youtube_upload_video_for_user else None,
        catalyst_velocity_endpoint=_catalyst_velocity if youtube_get_velocity_for_user else None,
    )
