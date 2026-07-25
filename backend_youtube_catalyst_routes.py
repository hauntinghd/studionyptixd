from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Awaitable, Callable

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
from studio_agent.direct_production import (
    claim_direct_production,
    require_idempotency_key,
    upload_content_contract,
)

log = logging.getLogger("nyptid-studio")

# In-memory state to prevent overlapping auto-tick runs
_catalyst_auto_tick_running: dict[str, bool] = {}


async def _execute_catalyst_command(
    *,
    name: str,
    arguments: dict[str, Any],
    request: Request,
    user: dict,
    operation: Callable[[], Awaitable[dict[str, Any]]],
    content_format: str = "catalyst",
) -> dict[str, Any]:
    """Claim the shared production receipt before any Catalyst side effect."""

    user_id = str((user or {}).get("id") or (user or {}).get("sub") or "").strip()
    with claim_direct_production(
        name,
        dict(arguments or {}),
        request=request,
        user_id=user_id,
        content_format=content_format,
    ) as command:
        if command.replay is not None:
            return dict(command.replay)
        result = await operation()
        return command.complete(dict(result or {}))


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
    catalyst_hub_longform_suggestions_for_user,
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
    youtube_upload_short_for_user=None,
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
        request: Request,
        user: dict = Depends(require_auth),
    ):
        channel_id = str((req or {}).channel_id or "").strip()
        include_public_benchmarks = bool((req or {}).include_public_benchmarks)
        refresh_outcomes = bool((req or {}).refresh_outcomes)
        return await _execute_catalyst_command(
            name="catalyst_refresh_hub",
            arguments={
                "channel_id": channel_id,
                "include_public_benchmarks": include_public_benchmarks,
                "refresh_outcomes": refresh_outcomes,
            },
            request=request,
            user=user,
            operation=lambda: catalyst_hub_refresh_for_user(
                user=user,
                channel_id=channel_id,
                include_public_benchmarks=include_public_benchmarks,
                refresh_outcomes=refresh_outcomes,
            ),
        )

    async def _catalyst_hub_reference_video_analysis(
        req: CatalystHubReferenceVideoAnalysisRequest,
        request: Request,
        user: dict = Depends(require_auth),
    ):
        channel_id = str((req or {}).channel_id or "").strip()
        workspace_id = str((req or {}).workspace_id or "documentary").strip().lower() or "documentary"
        video_id = str((req or {}).video_id or "").strip()
        max_analysis_minutes = float((req or {}).max_analysis_minutes or catalyst_reference_analysis_default_minutes)
        return await _execute_catalyst_command(
            name="catalyst_analyze_reference_video",
            arguments={
                "channel_id": channel_id,
                "workspace_id": workspace_id,
                "video_id": video_id,
                "max_analysis_minutes": max_analysis_minutes,
            },
            request=request,
            user=user,
            operation=lambda: catalyst_hub_reference_video_analysis_for_user(
                user=user,
                channel_id=channel_id,
                workspace_id=workspace_id,
                video_id=video_id,
                max_analysis_minutes=max_analysis_minutes,
            ),
            content_format="longform",
        )

    async def _catalyst_hub_reference_video_analysis_manual(
        request: Request,
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
        normalized_channel_id = str(channel_id or "").strip()
        normalized_workspace_id = str(workspace_id or "documentary").strip().lower() or "documentary"
        normalized_video_id = str(video_id or "").strip()
        normalized_minutes = float(max_analysis_minutes or catalyst_reference_analysis_default_minutes)
        image_uploads = list(analytics_images or [])
        require_idempotency_key(request)
        reference_video_contract = await upload_content_contract(reference_video)
        comparison_video_contract = await upload_content_contract(comparison_video)
        analytics_image_contracts = [
            await upload_content_contract(upload) for upload in image_uploads
        ]
        return await _execute_catalyst_command(
            name="catalyst_analyze_reference_video_manual",
            arguments={
                "channel_id": normalized_channel_id,
                "workspace_id": normalized_workspace_id,
                "video_id": normalized_video_id,
                "max_analysis_minutes": normalized_minutes,
                "reference_source_url": str(reference_source_url or "").strip(),
                "reference_title": str(reference_title or "").strip(),
                "reference_channel": str(reference_channel or "").strip(),
                "analytics_notes": str(analytics_notes or "").strip(),
                "transcript_text": str(transcript_text or "").strip(),
                "reference_video": reference_video_contract,
                "comparison_video": comparison_video_contract,
                "analytics_images": analytics_image_contracts,
            },
            request=request,
            user=user,
            operation=lambda: catalyst_hub_reference_video_analysis_manual_for_user(
                user=user,
                channel_id=normalized_channel_id,
                workspace_id=normalized_workspace_id,
                video_id=normalized_video_id,
                max_analysis_minutes=normalized_minutes,
                reference_source_url=str(reference_source_url or "").strip(),
                reference_title=str(reference_title or "").strip(),
                reference_channel=str(reference_channel or "").strip(),
                analytics_notes=str(analytics_notes or "").strip(),
                transcript_text=str(transcript_text or "").strip(),
                reference_video=reference_video,
                comparison_video=comparison_video,
                analytics_images=image_uploads,
                upload_dir=upload_dir,
            ),
            content_format="longform",
        )

    async def _catalyst_hub_clear_reference_video_analysis(
        req: CatalystHubReferenceVideoClearRequest,
        request: Request,
        user: dict = Depends(require_auth),
    ):
        channel_id = str((req or {}).channel_id or "").strip()
        workspace_id = str((req or {}).workspace_id or "documentary").strip().lower() or "documentary"
        return await _execute_catalyst_command(
            name="catalyst_clear_reference_video",
            arguments={"channel_id": channel_id, "workspace_id": workspace_id},
            request=request,
            user=user,
            operation=lambda: catalyst_hub_clear_reference_video_analysis_for_user(
                user=user,
                channel_id=channel_id,
                workspace_id=workspace_id,
            ),
            content_format="longform",
        )

    async def _catalyst_hub_save_instructions(
        req: CatalystHubDirectiveRequest,
        request: Request,
        user: dict = Depends(require_auth),
    ):
        arguments = {
            "channel_id": str((req or {}).channel_id or "").strip(),
            "directive": str((req or {}).directive or "").strip(),
            "mission": str((req or {}).mission or "").strip(),
            "guardrails": list((req or {}).guardrails or []),
            "target_niches": list((req or {}).target_niches or []),
            "apply_scope": str((req or {}).apply_scope or "all").strip().lower() or "all",
        }
        return await _execute_catalyst_command(
            name="catalyst_save_instructions",
            arguments=arguments,
            request=request,
            user=user,
            operation=lambda: catalyst_hub_save_instructions_for_user(user=user, **arguments),
        )

    async def _catalyst_hub_launch_longform(
        req: CatalystHubLaunchRequest,
        request: Request,
        user: dict = Depends(require_auth),
    ):
        arguments = {
            "channel_id": str((req or {}).channel_id or "").strip(),
            "workspace_id": str((req or {}).workspace_id or "").strip().lower(),
            "mission": str((req or {}).mission or "").strip(),
            "directive": str((req or {}).directive or "").strip(),
            "guardrails": list((req or {}).guardrails or []),
            "target_niches": list((req or {}).target_niches or []),
            "include_public_benchmarks": bool_from_any((req or {}).include_public_benchmarks, True),
            "refresh_outcomes": bool_from_any((req or {}).refresh_outcomes, True),
            "target_minutes": float((req or {}).target_minutes or 0.0),
            "language": str((req or {}).language or "en"),
            "animation_enabled": bool_from_any((req or {}).animation_enabled, True),
            "sfx_enabled": bool_from_any((req or {}).sfx_enabled, True),
            "auto_pipeline": bool_from_any((req or {}).auto_pipeline, True),
            "topic": str(getattr(req, "topic", "") or "").strip(),
            "input_title": str(getattr(req, "input_title", "") or "").strip(),
            "input_description": str(getattr(req, "input_description", "") or "").strip(),
        }
        return await _execute_catalyst_command(
            name="catalyst_launch_longform",
            arguments=arguments,
            request=request,
            user=user,
            operation=lambda: catalyst_hub_launch_longform_for_user(user=user, **arguments),
            content_format="longform",
        )

    async def _catalyst_hub_longform_suggestions(
        body: dict,
        request: Request,
        user: dict = Depends(require_auth),
    ):
        channel_id = str((body or {}).get("channel_id", "") or "").strip()
        workspace_id = str((body or {}).get("workspace_id", "documentary") or "documentary").strip().lower()
        return await _execute_catalyst_command(
            name="catalyst_generate_longform_suggestions",
            arguments={"channel_id": channel_id, "workspace_id": workspace_id},
            request=request,
            user=user,
            operation=lambda: catalyst_hub_longform_suggestions_for_user(
                user=user,
                channel_id=channel_id,
                workspace_id=workspace_id,
            ),
            content_format="longform",
        )

    async def _list_connected_youtube_channels(
        request: Request,
        sync: bool = False,
    ):
        user = await get_current_user_from_request(request)
        if not user:
            raise HTTPException(401, "Auth required")
        if not sync:
            return await list_connected_youtube_channels_for_user(user=user, sync=False)
        return await _execute_catalyst_command(
            name="catalyst_sync_channels",
            arguments={"sync": True},
            request=request,
            user=user,
            operation=lambda: list_connected_youtube_channels_for_user(user=user, sync=True),
        )

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
        normalized_channel_id = str(channel_id or "").strip()
        return await _execute_catalyst_command(
            name="catalyst_sync_channel",
            arguments={"channel_id": normalized_channel_id},
            request=request,
            user=user,
            operation=lambda: sync_connected_youtube_channel_for_user(
                user=user,
                channel_id=normalized_channel_id,
            ),
        )

    async def _sync_connected_youtube_channel_outcomes(
        channel_id: str,
        req: CatalystChannelOutcomeSyncRequest,
        request: Request,
    ):
        user = await get_current_user_from_request(request)
        if not user:
            raise HTTPException(401, "Auth required")
        arguments = {
            "channel_id": str(channel_id or "").strip(),
            "session_id": str((req or {}).session_id or "").strip(),
            "candidate_limit": int((req or {}).candidate_limit or 18),
            "refresh_existing": bool((req or {}).refresh_existing),
        }
        return await _execute_catalyst_command(
            name="catalyst_sync_channel_outcomes",
            arguments=arguments,
            request=request,
            user=user,
            operation=lambda: sync_connected_youtube_channel_outcomes_for_user(
                user=user,
                **arguments,
                longform_owner_beta_enabled=longform_owner_beta_enabled,
                harvest_catalyst_outcomes_for_channel=harvest_catalyst_outcomes_for_channel,
            ),
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
        arguments = {
            "session_id": str(session_id).strip(),
            "channel_id": str(channel_id).strip(),
            "privacy": str(privacy).strip() or "private",
        }
        return await _execute_catalyst_command(
            name="catalyst_upload_longform",
            arguments=arguments,
            request=request,
            user=user,
            operation=lambda: youtube_upload_video_for_user(user=user, **arguments),
            content_format="longform",
        )

    async def _short_upload_video(
        request: Request,
        job_id: str = Form(""),
        channel_id: str = Form(""),
        privacy: str = Form("private"),
    ):
        """Upload a completed short-form job's rendered MP4 to YouTube."""
        user = await get_current_user_from_request(request)
        if not user:
            raise HTTPException(401, "Auth required")
        if not youtube_upload_short_for_user:
            raise HTTPException(501, "YouTube upload not configured")
        arguments = {
            "job_id": str(job_id).strip(),
            "channel_id": str(channel_id).strip(),
            "privacy": str(privacy).strip() or "private",
        }
        return await _execute_catalyst_command(
            name="catalyst_upload_short",
            arguments=arguments,
            request=request,
            user=user,
            operation=lambda: youtube_upload_short_for_user(user=user, **arguments),
            content_format="shortform",
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
        normalized_workspace = str(workspace or "documentary").strip().lower() or "documentary"

        async def run_auto_tick() -> dict[str, Any]:
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
                    workspace_id=normalized_workspace,
                    auto_pipeline=True,
                )
                session = dict(launch_result.get("session") or {})
                return {
                    "status": "launched",
                    "velocity_vph": velocity_vph,
                    "session_id": str(session.get("session_id") or launch_result.get("session_id") or ""),
                    "message": f"Decay detected ({velocity_vph} vph). New longform pipeline launched.",
                }
            finally:
                _catalyst_auto_tick_running[tick_key] = False

        return await _execute_catalyst_command(
            name="catalyst_auto_tick",
            arguments={"channel_id": str(channel_id).strip(), "workspace_id": normalized_workspace},
            request=request,
            user=user,
            operation=run_auto_tick,
            content_format="longform",
        )

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
        normalized_enabled = str(enabled).lower() in ("true", "1", "yes")
        normalized_interval = max(1, min(24, float(interval_hours or 6)))

        async def update_auto_pilot() -> dict[str, Any]:
            from backend import _catalyst_auto_pilot_channels

            _catalyst_auto_pilot_channels[key] = {
                "enabled": normalized_enabled,
                "interval_hours": normalized_interval,
                "last_check": 0,
                "channel_id": channel_id,
            }
            return {
                "status": "ok",
                "enabled": _catalyst_auto_pilot_channels[key]["enabled"],
                "interval_hours": _catalyst_auto_pilot_channels[key]["interval_hours"],
            }

        return await _execute_catalyst_command(
            name="catalyst_set_auto_pilot",
            arguments={
                "channel_id": str(channel_id).strip(),
                "enabled": normalized_enabled,
                "interval_hours": normalized_interval,
            },
            request=request,
            user=user,
            operation=update_auto_pilot,
        )

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
        catalyst_hub_longform_suggestions_endpoint=_catalyst_hub_longform_suggestions,
        list_youtube_channels_endpoint=_list_connected_youtube_channels,
        select_youtube_channel_endpoint=_select_connected_youtube_channel,
        sync_youtube_channel_endpoint=_sync_connected_youtube_channel,
        sync_youtube_channel_outcomes_endpoint=_sync_connected_youtube_channel_outcomes,
        delete_youtube_channel_endpoint=_disconnect_connected_youtube_channel,
        catalyst_auto_tick_endpoint=_catalyst_auto_tick,
        catalyst_auto_pilot_endpoint=_catalyst_auto_pilot_toggle,
        catalyst_upload_endpoint=_catalyst_upload_video if youtube_upload_video_for_user else None,
        short_upload_endpoint=_short_upload_video if youtube_upload_short_for_user else None,
        catalyst_velocity_endpoint=_catalyst_velocity if youtube_get_velocity_for_user else None,
    )
