import os
import re
import base64
import hashlib
import shutil
import random
import secrets
import asyncio
import json
import time
import subprocess
import tempfile
import logging
import io
import calendar
import zipfile
import html as html_lib
import httpx
import jwt
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import quote, urlparse, unquote, parse_qs, urlencode
from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from typing import Optional
import stripe as stripe_lib
import uvicorn
from audio import (
    DEFAULT_ELEVENLABS_VOICES,
    _audio_track_exists,
    _build_atempo_filter_chain,
    _cache_voice_catalog,
    _default_elevenlabs_voice_priority,
    _extract_word_timings,
    _fallback_voice_catalog,
    _fetch_voice_catalog,
    _generate_catalyst_bgm_track,
    _generate_voiceover_with_edge_tts,
    _is_retryable_elevenlabs_voice_error,
    _mix_ambience_tracks,
    _probe_audio_duration_seconds,
    _probe_video_duration_seconds,
    _quintuple_check_scene_sfx,
    _rebalance_scene_durations_for_audio,
    _seconds_to_ass_timestamp,
    generate_ass_scene_subtitles,
    generate_ass_subtitles,
    generate_scene_sfx,
    generate_sfx_for_scene,
    generate_voiceover,
    _sfx_enabled,
    _resolve_edge_tts_voice,
    _resolve_elevenlabs_voice_candidates,
    _voice_provider_snapshot,
)
from auth import FALLBACK_SUPABASE_ANON_KEY, FALLBACK_SUPABASE_URL, build_auth_helpers
from routes import (
    build_assets_router,
    build_billing_router,
    build_core_router,
    build_generation_router,
    build_longform_creative_router,
    build_media_router,
    build_misc_router,
)
from backend_youtube_catalyst_routes import build_youtube_catalyst_app_router
from backend_settings import (
    XAI_API_KEY,
    ELEVENLABS_API_KEY,
    PIKZELS_API_KEY,
    COMFYUI_URL,
    SUPABASE_URL,
    SUPABASE_ANON_KEY,
    SUPABASE_JWT_SECRET,
    YOUTUBE_API_KEY,
    YOUTUBE_API_KEYS,
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    GOOGLE_REDIRECT_URI,
    GOOGLE_OAUTH_SOURCE,
    GOOGLE_OAUTH_CLIENT_KIND,
    GOOGLE_OAUTH_CONFIG_ISSUE,
    GOOGLE_INSTALLED_CLIENT_ID,
    GOOGLE_INSTALLED_CLIENT_SECRET,
    GOOGLE_INSTALLED_REDIRECT_URI,
    GOOGLE_INSTALLED_OAUTH_SOURCE,
    GOOGLE_INSTALLED_OAUTH_CONFIG_ISSUE,
    YOUTUBE_OAUTH_MODE,
    STRIPE_SECRET_KEY,
    STRIPE_WEBHOOK_SECRET,
    STRIPE_TOPUP_PUBLIC_ENABLED,
    PAYPAL_CLIENT_ID,
    PAYPAL_CLIENT_SECRET,
    PAYPAL_ENV,
    SITE_URL,
    FAL_AI_KEY,
    FAL_IMAGE_BACKUP_MODEL,
    XAI_IMAGE_MODEL,
    XAI_VIDEO_MODEL,
    PIKZELS_THUMBNAIL_MODEL,
    PIKZELS_RECREATE_MODEL,
    PIKZELS_TITLE_MODEL,
    RUNWAY_API_KEY,
    RUNWAY_API_KEY_SOURCE,
    RUNWAY_VIDEO_MODEL,
    RUNWAY_API_VERSION,
    PLAN_PRICE_USD,
    KLING21_STANDARD_I2V_5S_USD,
    ANIMATION_MARKUP_MULTIPLIER,
    ANIMATION_CREDIT_UNIT_USD,
    XAI_IMAGE_ASPECT_RATIO,
    XAI_IMAGE_RESOLUTION,
    USE_XAI_VIDEO,
    PRODUCT_DEMO_PUBLIC_ENABLED,
    WAITLIST_ONLY_MODE,
    WAITLIST_REQUIRE_STRIPE_PAYMENT,
    SKELETON_GLOBAL_REFERENCE_IMAGE_URL,
    STORY_GLOBAL_REFERENCE_IMAGE_URL,
    MOTIVATION_GLOBAL_REFERENCE_IMAGE_URL,
    USE_FAL_GROK_IMAGE,
    IMAGE_PROVIDER_ORDER,
    XAI_IMAGE_FALLBACK_ENABLED,
    HIDREAM_ENABLED,
    HIDREAM_MODEL,
    HIDREAM_EDIT_ENABLED,
    HIDREAM_EDIT_MODEL,
    HIDREAM_EDIT_WEIGHT_DTYPE,
    HIDREAM_CLIP_L,
    HIDREAM_CLIP_G,
    HIDREAM_T5,
    HIDREAM_LLAMA,
    HIDREAM_VAE,
    HIDREAM_SHIFT,
    HIDREAM_STEPS,
    HIDREAM_CFG,
    HIDREAM_SAMPLER,
    HIDREAM_SCHEDULER,
    WAN22_T2I_CHECKPOINT,
    WAN22_T2I_CLIP,
    WAN22_T2I_VAE,
    WAN22_T2I_UNET,
    WAN22_T2I_UNET_FP8,
    TEMPLATE_ADAPTER_ROUTING_ENABLED,
    TEMPLATE_ADAPTER_ROUTING,
    IMAGE_LOCAL_PROVIDER_RETRIES,
    IMAGE_PROVIDER_FAILURE_COOLDOWN_SEC,
    IMAGE_PROVIDER_WAN_SKIP_IF_UNAVAILABLE,
    SKELETON_REQUIRE_WAN22,
    SKELETON_SDXL_LORA_ENABLED,
    IMAGE_LOCAL_MIN_FILE_BYTES,
    IMAGE_QUALITY_BESTOF_ENABLED,
    IMAGE_QUALITY_BESTOF_COUNT,
    IMAGE_QUALITY_MIN_SCORE,
    RUNPOD_IMAGE_FEEDBACK_ENABLED,
    RUNPOD_IMAGE_FEEDBACK_SSH,
    RUNPOD_IMAGE_FEEDBACK_BASE_DIR,
    RUNPOD_COMPOSITOR_ENABLED,
    RUNPOD_COMPOSITOR_FALLBACK_LOCAL,
    RUNPOD_COMPOSITOR_HOST,
    RUNPOD_COMPOSITOR_SSH_PORT,
    RUNPOD_COMPOSITOR_BASE_DIR,
    FORCE_720P_ONLY,
    SCRIPT_TO_SHORT_ENABLED,
    STORY_ADVANCED_CONTROLS_ENABLED,
    STORY_RETENTION_TUNING_ENABLED,
    DISABLE_ALL_SFX,
    LONGFORM_BETA_ENABLED,
    LONGFORM_DEFAULT_TARGET_MINUTES,
    LONGFORM_MIN_TARGET_MINUTES,
    LONGFORM_MAX_TARGET_MINUTES,
    LONGFORM_MAX_SCENE_RETRIES,
    MAINTENANCE_BANNER_ENABLED,
    MAINTENANCE_BANNER_MESSAGE,
    REDIS_QUEUE_ENABLED,
    REDIS_URL,
    STRIPE_PRICE_TO_PLAN,
    DEMO_PRO_PRICE_ID,
    TOPUP_PACKS,
    PUBLIC_PLAN_IDS,
    PUBLIC_TOPUP_PACK_IDS,
    SUPABASE_SERVICE_KEY,
    OUTPUT_DIR,
    TEMP_DIR,
    THUMBNAIL_DIR,
    TRAINING_DATA_DIR,
)
from video_pipeline import (
    DEFAULT_CREATIVE_IMAGE_MODEL_ID,
    DEFAULT_CREATIVE_VIDEO_MODEL_ID,
    CREATIVE_IMAGE_MODEL_PROFILES,
    CREATIVE_IMAGE_MODEL_MAP,
    CREATIVE_VIDEO_MODEL_PROFILES,
    CREATIVE_VIDEO_MODEL_MAP,
    _creative_model_catalog_copy,
    _build_skeleton_shorts_local_fallback,
    _normalize_creative_image_model_id,
    _normalize_scene_image_model_id,
    _normalize_creative_video_model_id,
    _creative_image_model_profile,
    _creative_video_model_profile,
    _creative_image_credit_cost,
    _creative_video_credit_multiplier,
    TRANSITION_STYLE_MAP,
    MICRO_ESCALATION_MAX_SOURCE_SCENES,
    MICRO_ESCALATION_MAX_OUTPUT_CLIPS,
    _build_sdxl_workflow_with_loras,
    _normalize_output_resolution,
    _plan_features_for,
    _bool_from_any,
    _normalize_transition_style,
    _transition_duration_for_style,
    _normalize_micro_escalation_mode,
    _creative_template_sound_mix_profile,
    _creative_template_force_sfx,
    _creative_template_supports_voice_controls,
    _normalize_cinematic_boost,
    _normalize_voice_speed,
    _normalize_pacing_mode,
    _apply_story_pacing,
    _build_micro_escalation_clips,
    _normalize_longform_template,
    _normalize_longform_target_minutes,
    _normalize_longform_whisper_mode,
    _normalize_longform_language,
    _normalize_longform_scenes_for_render,
    _clean_longform_scene_text,
    _story_scene_prefers_explainer_visuals,
    _build_documentary_scene_repair,
    _build_longform_scene_execution_prompt,
    _build_longform_scene_motion_prompt,
    _build_longform_scene_sfx_brief,
    _longform_detect_tone,
    _longform_is_horror_tone,
    _longform_prefers_3d_documentary_visuals,
    _longform_default_art_style,
    _longform_documentary_archetype,
    _longform_tone_locked_visual_description,
    _longform_enforce_tone_on_scenes,
    _longform_chapter_count_for_minutes,
    _longform_chapter_scene_targets,
    _longform_placeholder_chapter,
    _longform_brand_slot,
    _longform_title_variant,
    _longform_fallback_chapter,
    _longform_fallback_visual_focus,
    _longform_text_is_strategy_garbage,
    _longform_scene_looks_like_machine_filler,
    _longform_psychology_scene_too_generic,
    _longform_crime_scene_too_generic,
    _repair_longform_generated_scenes,
    _render_longform_blueprint_prompt_context,
    _scale_scene_durations_to_target,
    _longform_chapter_retention_score,
    _remove_nyptid_mentions,
    _longform_apply_brand_slot,
    _longform_prefers_psychology_documentary_visuals,
    _generate_longform_chapter,
    generate_script,
    _split_text_into_fallback_beats,
    configure_video_pipeline_runtime_hooks,
)
from analytics import (
    _audit_manual_comparison_video,
    _contains_analytics_signal,
    _dedupe_clip_list,
    _duration_text_to_seconds,
    _extract_analytics_text_metrics,
    _summarize_longform_analytics_text,
    _build_analytics_contact_sheet,
    _build_catalyst_analysis_proxy_video,
    _extract_reference_video_full_audit,
    _extract_reference_preview_frames_from_urls,
    _extract_reference_video_sample_frames,
    analyze_viral_video,
    generate_clone_script,
    configure_reference_video_audit_hooks,
    configure_clone_analysis_hooks,
)
from catalyst import (
    CATALYST_MARKETING_DOCTRINE,
    _apply_manual_operator_evidence_to_reference_video,
    _build_catalyst_reference_video_analysis,
    _build_catalyst_reference_analysis_evidence,
    _build_longform_operator_notes,
    _build_shorts_public_reference_playbook,
    _catalyst_short_memory_public_snapshot,
    _marketing_doctrine_text,
    _persist_catalyst_short_learning_for_render,
    _persist_public_shorts_playbook_memory,
    _public_shorts_playbook_from_memory_view,
    _build_shorts_trend_query,
    _build_shorts_catalyst_extra_instructions,
    _catalyst_failure_mode_label,
    _catalyst_hub_workspace_label,
    _catalyst_hub_snapshot_for_user,
    _catalyst_hub_refresh_for_user,
    _catalyst_hub_reference_video_analysis_for_user,
    _catalyst_hub_reference_video_analysis_manual_for_user,
    _catalyst_hub_clear_reference_video_analysis_for_user,
    _catalyst_hub_save_instructions_for_user,
    _catalyst_hub_launch_longform_for_user,
    _catalyst_reference_analysis_confidence_label,
    _catalyst_reference_workspace_profile,
    _catalyst_reference_video_analysis_public_view,
    _catalyst_longform_preflight,
    _heuristic_catalyst_reference_video_analysis,
    _merge_operator_evidence_into_reference_analysis,
    _normalize_catalyst_reference_video_analysis,
    _harvest_catalyst_outcomes_for_channel_for_user,
    _maybe_refresh_channel_outcomes_before_longform_run_for_user,
    _persist_catalyst_reference_video_analysis,
    _reconcile_reference_video_analysis_with_inventory,
    configure_catalyst_runtime_hooks,
)
from billing import (
    KPI_TARGETS,
    LANDING_NOTIFICATIONS_LIMIT,
    LANDING_NOTIFICATIONS_PATH,
    LANDING_NOTIFICATIONS_PUBLIC_LIMIT,
    PAYPAL_ORDERS_PATH,
    PAYPAL_SUBSCRIPTIONS_PATH,
    TOPUP_WALLET_PATH,
    USAGE_LEDGER_PATH,
    _append_landing_notification,
    _append_usage_ledger,
    _credit_state_for_user,
    _credit_topup_wallet,
    _estimate_job_cost_usd,
    _add_months_utc,
    _invoice_paid_at_unix,
    _invoice_period_end_unix,
    _kpi_metrics,
    _landing_notifications,
    _landing_notifications_lock,
    _load_kpi_metrics,
    _load_landing_notifications,
    _load_paypal_orders,
    _load_paypal_subscriptions,
    _load_topup_wallets,
    _month_key,
    _paypal_orders,
    _paypal_orders_lock,
    _paypal_subscriptions,
    _paypal_subscriptions_lock,
    _plan_monthly_animated_limit,
    _plan_monthly_non_animated_limit,
    _next_renewal_from_anchor,
    _record_kpi_for_job,
    _refund_generation_credit,
    _reserve_generation_credit,
    _save_kpi_metrics,
    _save_paypal_orders,
    _save_paypal_subscriptions,
    _save_topup_wallets,
    _stripe_find_customer_id_by_email,
    _stripe_subscription_snapshot,
    _stripe_value,
    _topup_wallet_lock,
    _topup_wallets,
    _subscription_interval_months,
    _supabase_find_user_id_by_email,
    _supabase_get_waitlist_rows,
    _supabase_set_user_plan,
    _supabase_upsert_waitlist_entry,
    _to_unix,
    _wallet_for_user,
)
from youtube import (
    YOUTUBE_OAUTH_STATE_TTL_SEC,
    YOUTUBE_TOKEN_REFRESH_MARGIN_SEC,
    YOUTUBE_SCOPES,
    YOUTUBE_AUTH_BASE_URL,
    GOOGLE_TOKEN_URL,
    GOOGLE_REVOKE_URL,
    YOUTUBE_DATA_API_BASE,
    YOUTUBE_ANALYTICS_API_BASE,
    _youtube_connections,
    _youtube_connections_lock,
    _youtube_oauth_states,
    _youtube_oauth_states_lock,
    _load_youtube_connections,
    _save_youtube_connections,
    _prune_youtube_oauth_states,
    _load_youtube_oauth_states,
    _save_youtube_oauth_states,
    _youtube_web_auth_configured,
    _youtube_installed_auth_configured,
    _youtube_active_oauth_mode,
    _youtube_auth_context,
    _youtube_auth_configured,
    _youtube_auth_issue_message,
    _format_google_oauth_failure,
    _youtube_bucket_for_user,
    configure_youtube_analysis_hooks,
    configure_youtube_runtime_hooks,
    _append_youtube_signal_log,
    _google_exchange_code_for_tokens,
    _google_oauth_error_suggests_reconnect_required,
    _google_oauth_error_suggests_stale_client,
    _youtube_connection_public_view,
    _youtube_connected_channel_access_token,
    _youtube_connected_channel_public_view,
    _youtube_ensure_access_token,
    _youtube_extract_code_or_error,
    _youtube_fetch_channel_analytics,
    _youtube_fetch_channel_search,
    _youtube_fetch_video_analytics_bulk,
    _youtube_historical_compare_measured_public_view,
    _youtube_channel_audit_measured_public_view,
    _youtube_apply_public_inventory_to_snapshot,
    _youtube_installed_helper_html,
    _youtube_order_inventory_rows,
    _youtube_refresh_public_channel_record_without_oauth,
    _youtube_repair_channel_record_from_sibling,
    _youtube_redirect_target,
    _youtube_pkce_pair,
    _youtube_pkce_challenge,
    _youtube_build_auth_url,
    _youtube_helper_page_url,
    _youtube_api_get,
    _youtube_channel_url,
    _youtube_fetch_my_channels,
    _youtube_fetch_videos,
    _youtube_fetch_uploads_playlist_videos,
    _youtube_parse_published_date,
    _youtube_watch_url,
    _normalize_external_source_url,
    _parse_vtt_text,
    _pick_subtitle_candidate,
    _source_url_video_id,
    _extract_html_meta_content,
    _extract_html_link_href,
    _parse_youtube_duration_from_html,
    _fetch_public_video_bundle_fallback,
    _yt_dlp_extract_info_blocking,
    _parse_youtube_iso8601_duration,
    _youtube_public_api_key_candidates,
    _youtube_public_api_get,
    _youtube_fetch_public_video_bundle_api_key,
    _youtube_fetch_public_videos_api_key,
    _youtube_fetch_public_channel_search_api_key,
    _youtube_channel_videos_page_url,
    _youtube_extract_video_ids_from_channel_page,
    _youtube_renderer_text,
    _youtube_parse_compact_count,
    _youtube_video_renderer_duration_sec,
    _youtube_extract_public_channel_page_rows,
    _youtube_extract_public_channel_rows_with_ytdlp,
    _youtube_merge_public_video_rows,
    _youtube_fetch_public_channel_page_videos,
    _youtube_fetch_public_trend_titles,
    _youtube_fetch_public_reference_shorts,
    _youtube_title_keywords,
    _youtube_caption_language_candidates,
    _youtube_caption_track_sort_key,
    _youtube_download_caption_vtt,
    _youtube_download_public_timedtext_transcript,
    _download_youtube_video_for_reference_analysis,
    _extract_reference_video_stream_clip,
    _fetch_source_video_bundle,
    _fetch_algrow_reference_enrichment,
    _youtube_fetch_owned_video_bundle_oauth,
    _youtube_finalize_oauth_connection,
    _youtube_start_oauth_browser_redirect,
    _google_youtube_oauth_installed_helper_response,
    _google_youtube_oauth_complete_redirect,
    _google_youtube_oauth_callback_redirect,
    _list_connected_youtube_channels_for_user,
    _select_connected_youtube_channel_for_user,
    _sync_connected_youtube_channel_for_user,
    _sync_connected_youtube_channel_outcomes_for_user,
    _disconnect_connected_youtube_channel_for_user,
    _slugify_file_component,
    _pick_catalyst_reference_video,
    _manual_catalyst_reference_video_id,
    _reference_video_analysis_dir,
    _pick_reference_preview_frame_urls,
    _youtube_selected_channel_context,
    _youtube_start_oauth_for_user,
    _youtube_sync_and_persist_for_user,
)
from backend_catalog import (
    PLAN_LIMITS,
    PLAN_FEATURES,
    RESOLUTION_CONFIGS,
    ADMIN_EMAILS,
    HARDCODED_PLANS,
    PUBLIC_TEMPLATE_ALLOWLIST,
    SUPPORTED_LANGUAGES,
)
from backend_image_prompts import (
    SKELETON_IMAGE_STYLE_PREFIX,
    SKELETON_MASTER_CONSISTENCY_PROMPT,
    SKELETON_IMAGE_SUFFIX,
    TEMPLATE_KLING_MOTION,
    TEMPLATE_PROMPT_PREFIXES,
    TEMPLATE_NEGATIVE_PROMPTS,
    NEGATIVE_PROMPT,
    WAN22_I2V_HIGH,
    WAN22_T2V_HIGH,
    WAN22_T2V_LOW,
)
from backend_catalyst_core import (
    _CATALYST_NICHE_RULES,
    _build_catalyst_cluster_playbook,
    _catalyst_build_channel_series_clusters,
    _catalyst_channel_memory_key,
    _catalyst_channel_memory_public_view,
    _catalyst_extract_series_anchor,
    _catalyst_infer_archetype,
    _catalyst_infer_niche,
    _catalyst_merge_signal_lists,
    _catalyst_merge_weighted_signals,
    _catalyst_metric_average,
    _catalyst_metric_score,
    _catalyst_outcome_weight,
    _catalyst_pressure_label,
    _catalyst_reference_score_tier,
    _catalyst_reference_signal_list,
    _catalyst_rewrite_pressure_profile,
    _catalyst_series_memory_key,
    _catalyst_signal_balance_score,
    _catalyst_text_overlap_score,
    _catalyst_title_novelty_score,
    _catalyst_update_weighted_signals,
    _catalyst_weighted_signal_items,
    _extract_catalyst_keywords,
    _render_catalyst_series_cluster_context,
    _resolve_catalyst_series_context,
    _select_catalyst_channel_series_cluster,
)
from backend_catalyst_profiles import (
    _catalyst_audio_mix_profile,
    _catalyst_chapter_blueprint_for_index,
    _catalyst_default_sound_profile,
    _catalyst_default_visual_engine,
    _catalyst_scene_execution_profile,
    _catalyst_short_execution_pack,
    _catalyst_rank_shorts_angle_candidates,
    _heuristic_catalyst_chapter_blueprints,
)
from backend_catalyst_blueprint import _build_catalyst_edit_blueprint
from backend_catalyst_learning import (
    _apply_catalyst_public_shorts_playbook_to_channel_memory,
    _apply_catalyst_outcome_to_channel_memory,
    _build_auto_outcome_request,
    _build_catalyst_outcome_record,
    _heuristic_catalyst_longform_execution_qa,
    _heuristic_catalyst_learning_record,
    _heuristic_catalyst_short_learning_record,
    _match_published_video_to_longform_session,
    _youtube_fetch_video_analytics,
    _update_catalyst_channel_memory,
)
from backend_catalyst_reference import (
    _build_catalyst_reference_playbook,
    _render_catalyst_channel_memory_context,
    _render_catalyst_reference_corpus_context,
    _score_catalyst_outcome_against_reference,
)
from backend_models import (
    GenerateRequest,
    SceneImageRequest,
    FinalizeRequest,
    CheckoutRequest,
    TopupCheckoutRequest,
    WaitlistJoinRequest,
    SetPlanRequest,
    FeedbackRequest,
    ThumbnailFeedbackRequest,
    ThumbnailGenerateRequest,
    LongFormSessionCreateRequest,
    LongFormChapterActionRequest,
    LongFormResolveErrorRequest,
    LongFormSceneAssignmentRequest,
    CatalystOutcomeIngestRequest,
    CatalystAutoOutcomeHarvestRequest,
)
from backend_demo import (
    DEMO_DIR,
    analyze_screen_recording,
    generate_demo_script,
    generate_talking_head,
    composite_demo_video,
)
from backend_state import (
    _creative_sessions,
    _creative_sessions_lock,
    _projects,
    _projects_lock,
    _save_creative_sessions_to_disk,
    _get_creative_session,
    _save_projects_store,
    _new_project_id,
)
from backend_queue import (
    QueueFullError,
    enqueue_generation_job,
    get_queue_depth,
    get_queue_max_depth,
    get_queue_workers,
    get_persisted_job_state,
    init_queue_runtime,
    persist_job_state,
)
try:
    import paramiko
except Exception:
    paramiko = None

try:
    from PIL import Image, ImageChops, ImageDraw, ImageEnhance, ImageFilter, ImageStat
except Exception:
    Image = None
    ImageChops = None
    ImageDraw = None
    ImageEnhance = None
    ImageFilter = None
    ImageStat = None

try:
    import numpy as np
except Exception:
    np = None

try:
    import cv2
except Exception:
    cv2 = None

try:
    import yt_dlp
except Exception:
    yt_dlp = None

try:
    from faster_whisper import WhisperModel as FasterWhisperModel
except Exception:
    FasterWhisperModel = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("nyptid-studio")

CATALYST_REFERENCE_ANALYSIS_DEFAULT_MINUTES = 20.0
CATALYST_REFERENCE_ANALYSIS_MAX_SECONDS = 20.0 * 60.0
CATALYST_REFERENCE_TRANSCRIPT_MAX_CHARS = 2400
CATALYST_REFERENCE_AUDIO_MAX_SECONDS = 360.0
CATALYST_REFERENCE_UPLOAD_PROXY_FPS = 6.0
CATALYST_REFERENCE_UPLOAD_PROXY_MAX_WIDTH = 960
CATALYST_REFERENCE_FRAME_AUDIT_TARGET_FPS = 4.0
CATALYST_REFERENCE_FRAME_AUDIT_MAX_TIMELINE_FRAMES = 4800
CATALYST_REFERENCE_FRAME_AUDIT_MAX_REPORT_ROWS = 1200
CATALYST_REFERENCE_FRAME_AUDIT_WORKING_MAX_WIDTH = 640
_reference_whisper_model = None
_reference_whisper_lock = asyncio.Lock()

configure_reference_video_audit_hooks(
    analysis_max_seconds=CATALYST_REFERENCE_ANALYSIS_MAX_SECONDS,
    upload_proxy_fps=CATALYST_REFERENCE_UPLOAD_PROXY_FPS,
    upload_proxy_max_width=CATALYST_REFERENCE_UPLOAD_PROXY_MAX_WIDTH,
    frame_audit_target_fps=CATALYST_REFERENCE_FRAME_AUDIT_TARGET_FPS,
    frame_audit_max_timeline_frames=CATALYST_REFERENCE_FRAME_AUDIT_MAX_TIMELINE_FRAMES,
    frame_audit_max_report_rows=CATALYST_REFERENCE_FRAME_AUDIT_MAX_REPORT_ROWS,
    frame_audit_working_max_width=CATALYST_REFERENCE_FRAME_AUDIT_WORKING_MAX_WIDTH,
    extract_video_metadata=lambda *args, **kwargs: extract_video_metadata(*args, **kwargs),
    extract_audio_from_video=lambda *args, **kwargs: extract_audio_from_video(*args, **kwargs),
    transcribe_audio_with_grok=lambda *args, **kwargs: transcribe_audio_with_grok(*args, **kwargs),
    reference_audio_max_seconds=CATALYST_REFERENCE_AUDIO_MAX_SECONDS,
)


def _ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None

app = FastAPI(title="NYPTID Studio Engine", version="3.0")
_deploy_meta_cache = {"ts": 0.0, "backend_commit": "", "frontend_bundle": ""}
_frontend_asset_cache = {"ts": 0.0, "js": "", "css": ""}
_frontend_cache_buster = str(int(time.time()))


def _resolve_latest_frontend_assets() -> tuple[str, str]:
    now = time.time()
    if now - float(_frontend_asset_cache.get("ts", 0.0)) < 10.0:
        return str(_frontend_asset_cache.get("js", "")), str(_frontend_asset_cache.get("css", ""))
    js_name = ""
    css_name = ""
    try:
        default_dist = (Path(__file__).resolve().parent / "ViralShorts-App" / "dist").resolve()
        dist_root = Path(os.getenv("FRONTEND_DIST_DIR", str(default_dist))).resolve()
        assets_dir = dist_root / "assets"
        if assets_dir.exists():
            js_candidates = sorted(assets_dir.glob("index-*.js"), key=lambda p: p.stat().st_mtime, reverse=True)
            css_candidates = sorted(assets_dir.glob("index-*.css"), key=lambda p: p.stat().st_mtime, reverse=True)
            if js_candidates:
                js_name = js_candidates[0].name
            if css_candidates:
                css_name = css_candidates[0].name
    except Exception:
        js_name = ""
        css_name = ""
    _frontend_asset_cache["ts"] = now
    _frontend_asset_cache["js"] = js_name
    _frontend_asset_cache["css"] = css_name
    return js_name, css_name


def _resolve_frontend_asset_path(filename: str) -> Path:
    default_dist = (Path(__file__).resolve().parent / "ViralShorts-App" / "dist").resolve()
    dist_root = Path(os.getenv("FRONTEND_DIST_DIR", str(default_dist))).resolve()
    return dist_root / "assets" / filename


def _apply_runtime_js_text_hotfix(js: str) -> str:
    """Patch legacy pricing strings in stale frontend bundles."""
    if not js:
        return js
    js = js.replace("Unlimited videos", "300 videos/month")
    js = js.replace("Sign Up Free", "Sign Up to Subscribe")
    js = js.replace("Start Creating Free", "Start Creating")
    return js


def _read_deploy_meta() -> tuple[str, str]:
    now = time.time()
    if now - float(_deploy_meta_cache.get("ts", 0.0)) < 15.0:
        return str(_deploy_meta_cache.get("backend_commit", "")), str(_deploy_meta_cache.get("frontend_bundle", ""))

    backend_commit = (os.getenv("STUDIO_COMMIT_SHA", "") or os.getenv("GITHUB_SHA", "")).strip()
    if not backend_commit:
        try:
            backend_commit = subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=str(Path(__file__).resolve().parent),
                text=True,
                timeout=2,
            ).strip()
        except Exception:
            backend_commit = ""

    frontend_bundle = ""
    try:
        default_dist = (Path(__file__).resolve().parent / "ViralShorts-App" / "dist").resolve()
        current_dist = Path(os.getenv("FRONTEND_DIST_DIR", str(default_dist))).resolve()
        index_path = current_dist / "index.html"
        if index_path.exists():
            html = index_path.read_text(encoding="utf-8", errors="ignore")
            m = re.search(r"/assets/(index-[^\"']+\.js)", html)
            if m:
                frontend_bundle = m.group(1)
        if not frontend_bundle:
            latest_js, _ = _resolve_latest_frontend_assets()
            frontend_bundle = latest_js
    except Exception:
        frontend_bundle = ""

    _deploy_meta_cache["ts"] = now
    _deploy_meta_cache["backend_commit"] = backend_commit
    _deploy_meta_cache["frontend_bundle"] = frontend_bundle
    return backend_commit, frontend_bundle

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def _disable_html_cache(request: Request, call_next):
    """Prevent stale frontend shell and asset caching so new bundles load immediately."""
    response = await call_next(request)
    path = request.url.path or ""
    if path == "/" or path.endswith(".html"):
        try:
            content_type = str(response.headers.get("content-type", "")).lower()
            if "text/html" in content_type and hasattr(response, "body_iterator"):
                body = b""
                async for chunk in response.body_iterator:
                    body += chunk
                html = body.decode("utf-8", errors="ignore")
                latest_js, latest_css = _resolve_latest_frontend_assets()
                # Keep the compiled JS asset path from the built index.html.
                # Overriding to runtime-hotfix.js can mask newly deployed frontend bundles.
                if latest_css:
                    html = re.sub(r"/assets/index-[^\"']+\.css(\?[^\"']*)?", f"/assets/{latest_css}?v={_frontend_cache_buster}", html)
                headers = dict(response.headers)
                headers.pop("content-length", None)
                headers.pop("Content-Length", None)
                headers.pop("content-type", None)
                headers.pop("Content-Type", None)
                response = Response(
                    content=html,
                    status_code=response.status_code,
                    headers=headers,
                    media_type="text/html",
                )
        except Exception:
            pass
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    if path.startswith("/assets/") and (path.endswith(".js") or path.endswith(".css")):
        # Do not consume streaming asset responses in middleware.
        # Reading body_iterator here can drain the response and cause truncated JS (blank page).
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        # Extra hints for CDN layers to avoid stale bundle reuse.
        response.headers["CDN-Cache-Control"] = "no-store"
        response.headers["Cloudflare-CDN-Cache-Control"] = "no-store"
    return response


@app.get("/assets/runtime-hotfix.js")
async def serve_runtime_hotfix_js():
    """Serve JS with runtime text hotfixes to bypass stale CDN bundle caching."""
    latest_js, _ = _resolve_latest_frontend_assets()
    target = _resolve_frontend_asset_path(latest_js) if latest_js else _resolve_frontend_asset_path("index-BlMPK7KO.js")
    if not target.exists():
        fallback = _resolve_frontend_asset_path("index-BlMPK7KO.js")
        if fallback.exists():
            target = fallback
        else:
            raise HTTPException(status_code=404, detail="Hotfix JS not found")
    js = _apply_runtime_js_text_hotfix(target.read_text(encoding="utf-8", errors="ignore"))
    resp = Response(content=js, media_type="text/javascript")
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.get("/assets/index-BlMPK7KO.js")
async def serve_legacy_firefox_bundle_alias():
    """Alias stale cached bundle URL to the latest built JS asset."""
    latest_js, _ = _resolve_latest_frontend_assets()
    if latest_js:
        latest_path = _resolve_frontend_asset_path(latest_js)
        if latest_path.exists():
            return FileResponse(str(latest_path), media_type="text/javascript")
    legacy_path = _resolve_frontend_asset_path("index-BlMPK7KO.js")
    if legacy_path.exists():
        return FileResponse(str(legacy_path), media_type="text/javascript")
    raise HTTPException(status_code=404, detail="Asset not found")


jobs: dict = {}
security, get_current_user, get_current_user_from_request, require_auth = build_auth_helpers(
    supabase_url=SUPABASE_URL,
    supabase_anon_key=SUPABASE_ANON_KEY,
    supabase_jwt_secret=SUPABASE_JWT_SECRET,
    supabase_service_key=SUPABASE_SERVICE_KEY,
    hardcoded_plans=HARDCODED_PLANS,
    paypal_snapshot_for_user=lambda user: _paypal_subscription_snapshot_for_user(user),
)
init_queue_runtime(jobs, log)
AUTO_SCENE_IMAGE_ROOT = Path(TRAINING_DATA_DIR) / "auto_scene_images"
AUTO_SCENE_IMAGE_ROOT.mkdir(parents=True, exist_ok=True)
LONGFORM_SESSIONS_FILE = TEMP_DIR / "longform_sessions_store.json"
LONGFORM_PREVIEW_DIR = TEMP_DIR / "longform_previews"
LONGFORM_PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
_longform_sessions: dict[str, dict] = {}
_longform_sessions_lock = asyncio.Lock()
_longform_analysis_semaphore = asyncio.Semaphore(1)
_longform_draft_semaphore = asyncio.Semaphore(1)
_longform_render_semaphore = asyncio.Semaphore(1)
_longform_outcome_sync_semaphore = asyncio.Semaphore(1)
_SHORTFORM_PROTECTED_ACTIVE_JOB_STATUSES = {
    "queued",
    "generating_script",
    "generating_images",
    "animating_scenes",
    "generating_voice",
    "generating_sfx",
    "compositing",
    "analyzing",
    "rendering",
}
_SHORTFORM_PROTECTED_LANES = {"create", "chatstory"}
CATALYST_LEARNING_RECORDS_FILE = TEMP_DIR / "catalyst_learning_records.json"
CATALYST_CHANNEL_MEMORY_FILE = TEMP_DIR / "catalyst_channel_memory.json"
CATALYST_REFERENCE_MEMORY_FILE = Path(__file__).resolve().parent / "ops" / "catalyst_documentary_reference_memory.json"
_catalyst_learning_records: dict[str, dict] = {}
_catalyst_channel_memory: dict[str, dict] = {}
_catalyst_reference_memory: dict[str, dict] = {}
_catalyst_memory_lock = asyncio.Lock()
_JOB_RETENTION_ACTIVE_SEC = 12 * 3600
_JOB_RETENTION_FINAL_SEC = 2 * 3600

# Runtime banner state can be updated by admin without restart.
_maintenance_banner_enabled = bool(MAINTENANCE_BANNER_ENABLED)
_maintenance_banner_message = (
    (MAINTENANCE_BANNER_MESSAGE or "").strip()
    or "Studio is under high load. Queue times may be longer than usual while we scale capacity."
)


def _prune_longform_sessions(max_age_seconds: int = 72 * 3600) -> None:
    now = time.time()
    stale = [
        sid for sid, sess in list(_longform_sessions.items())
        if now - float((sess or {}).get("created_at", now)) > max_age_seconds
    ]
    for sid in stale:
        _longform_sessions.pop(sid, None)


def _load_longform_sessions() -> None:
    try:
        if not LONGFORM_SESSIONS_FILE.exists():
            return
        data = json.loads(LONGFORM_SESSIONS_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            _longform_sessions.clear()
            _longform_sessions.update(data)
            _prune_longform_sessions()
    except Exception:
        _longform_sessions.clear()


def _save_longform_sessions() -> None:
    try:
        _prune_longform_sessions()
        LONGFORM_SESSIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
        LONGFORM_SESSIONS_FILE.write_text(json.dumps(_longform_sessions, ensure_ascii=True), encoding="utf-8")
    except Exception:
        pass


def _load_catalyst_memory() -> None:
    global _catalyst_learning_records, _catalyst_channel_memory, _catalyst_reference_memory
    try:
        if CATALYST_LEARNING_RECORDS_FILE.exists():
            data = json.loads(CATALYST_LEARNING_RECORDS_FILE.read_text(encoding="utf-8"))
            _catalyst_learning_records = data if isinstance(data, dict) else {}
        else:
            _catalyst_learning_records = {}
    except Exception:
        _catalyst_learning_records = {}
    try:
        if CATALYST_CHANNEL_MEMORY_FILE.exists():
            data = json.loads(CATALYST_CHANNEL_MEMORY_FILE.read_text(encoding="utf-8"))
            _catalyst_channel_memory = data if isinstance(data, dict) else {}
        else:
            _catalyst_channel_memory = {}
    except Exception:
        _catalyst_channel_memory = {}
    try:
        if CATALYST_REFERENCE_MEMORY_FILE.exists():
            data = json.loads(CATALYST_REFERENCE_MEMORY_FILE.read_text(encoding="utf-8"))
            _catalyst_reference_memory = data if isinstance(data, dict) else {}
        else:
            _catalyst_reference_memory = {}
    except Exception:
        _catalyst_reference_memory = {}


def _save_catalyst_memory() -> None:
    try:
        CATALYST_LEARNING_RECORDS_FILE.parent.mkdir(parents=True, exist_ok=True)
        CATALYST_LEARNING_RECORDS_FILE.write_text(
            json.dumps(_catalyst_learning_records, ensure_ascii=True),
            encoding="utf-8",
        )
        CATALYST_CHANNEL_MEMORY_FILE.write_text(
            json.dumps(_catalyst_channel_memory, ensure_ascii=True),
            encoding="utf-8",
        )
    except Exception:
        pass


def _dedupe_preserve_order(values: list[str], max_items: int = 8, max_chars: int = 200) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in list(values or []):
        value = _clip_text(str(raw or "").strip(), max_chars)
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
        if len(out) >= max_items:
            break
    return out


_maintenance_banner_message = (
    (MAINTENANCE_BANNER_MESSAGE or "").strip()
    or "Studio is under high load. Queue times may be longer than usual while we scale capacity."
)


def _resolve_user_plan_for_limits(user: dict | None) -> tuple[str, dict]:
    if not user:
        return "free", PLAN_LIMITS.get("free", PLAN_LIMITS["starter"])
    email = str(user.get("email", "") or "")
    if email in ADMIN_EMAILS:
        pro = dict(PLAN_LIMITS.get("pro", PLAN_LIMITS["starter"]))
        pro["videos_per_month"] = max(int(pro.get("videos_per_month", 300) or 300), 9999)
        return "pro", pro
    plan = str(user.get("plan", "free") or "free").strip().lower()
    if plan in {"none", "admin"}:
        plan = "free"
    if plan not in PLAN_LIMITS:
        plan = "free"
    return plan, PLAN_LIMITS.get(plan, PLAN_LIMITS.get("free", PLAN_LIMITS["starter"]))


def _longform_owner_beta_enabled(user: dict | None) -> bool:
    return bool((_public_lane_access_for_user(user) or {}).get("longform"))


def _longform_deep_analysis_enabled(user: dict | None) -> bool:
    return _is_admin_user(user)


def _longform_subject_lock(topic: str, input_title: str, input_description: str = "") -> str:
    subject = _longform_fallback_visual_focus(topic, input_title)
    named_human_lock = _named_human_subject_likeness_lock(topic, input_title, input_description)
    if not subject or subject == "the central mechanism, object, or environment being explained":
        base = (
            "Primary subject lock: keep the exact named subject or system from the topic/title visible. "
            "Do not substitute a different object, organ, country, person, or symbol."
        )
        return f"{base} {named_human_lock}".strip()
    base = (
        f"Primary subject lock: keep {subject} as the focal subject throughout this chapter whenever the beat refers to it. "
        f"Do not replace {subject} with a different organ, object, machine, person, or symbol."
    )
    return f"{base} {named_human_lock}".strip()


PACKAGING_STOP_WORDS = {
    "the", "and", "that", "with", "from", "your", "this", "into", "when", "then", "they", "them",
    "their", "have", "will", "over", "because", "under", "after", "before", "while", "where", "about",
    "called", "actually", "really", "complete", "full", "breakdown", "explained", "video", "channel",
    "follow", "followup", "follow-up", "hidden", "hides", "from", "you", "how", "why", "what", "does",
    "make", "made", "that", "these", "those", "more", "than", "into", "inside", "secret", "secrets",
}


def _packaging_tokens(text: str, max_items: int = 12) -> list[str]:
    tokens: list[str] = []
    for raw in re.findall(r"[A-Za-z0-9']+", str(text or "").lower()):
        token = raw.strip("'")
        if len(token) < 3 or token in PACKAGING_STOP_WORDS:
            continue
        if token not in tokens:
            tokens.append(token)
        if len(tokens) >= max_items:
            break
    return tokens


def _title_stays_in_same_arena(candidate: str, source_title: str, topic: str = "") -> bool:
    cand = str(candidate or "").strip()
    source = str(source_title or "").strip()
    if not cand or not source:
        return True
    if _title_is_too_close_to_source(cand, source):
        return False
    cand_tokens = set(_packaging_tokens(cand, max_items=12))
    ref_tokens = set(_packaging_tokens(source + " " + str(topic or ""), max_items=16))
    if cand.lower().startswith("top ") and source.lower().startswith("top "):
        return len(cand_tokens & ref_tokens) >= 1
    return len(cand_tokens & ref_tokens) >= 2


def _source_title_pattern(source_title: str) -> tuple[str, int]:
    title = str(source_title or "").strip()
    lower = title.lower()
    top_match = re.match(r"^\s*top\s+(\d+)\b", lower)
    if top_match:
        return ("top_list", int(top_match.group(1)))
    if lower.startswith("why "):
        return ("why", 0)
    if lower.startswith("how "):
        return ("how", 0)
    if lower.startswith("what happened") or lower.startswith("what really happened"):
        return ("what_happened", 0)
    if any(token in lower for token in ["killed", "war", "leader", "assassin", "empire", "crime", "iran"]):
        return ("investigation", 0)
    return ("documentary", 0)


def _same_arena_subject(source_bundle: dict, topic: str = "") -> str:
    topic_text = str(topic or "").strip()
    if topic_text:
        subject = topic_text
    else:
        subject = str((source_bundle or {}).get("title", "") or "").strip()
    focus_hint = _same_arena_focus_entity(source_bundle, topic=topic_text)
    if focus_hint:
        return focus_hint
    if not subject:
        tags = [str(tag).strip() for tag in list((source_bundle or {}).get("tags") or []) if str(tag).strip()]
        subject = tags[0] if tags else "the core subject"
    subject = re.sub(r"^\s*top\s+\d+\s+", "", subject, flags=re.IGNORECASE)
    subject = re.sub(r"^\s*(how|why)\s+", "", subject, flags=re.IGNORECASE)
    subject = re.sub(r"^\s*what\s+(?:really\s+)?happened\s+to\s+", "", subject, flags=re.IGNORECASE)
    subject = re.sub(r"^\s*(the\s+truth\s+about|inside|the\s+story\s+of)\s+", "", subject, flags=re.IGNORECASE)
    subject = re.sub(r"\s*\|\s*.*$", "", subject)
    subject = re.sub(r"\s*[-:]\s*(complete|full)\s+breakdown\s*$", "", subject, flags=re.IGNORECASE)
    subject = re.sub(r"\s+", " ", subject).strip(" -,:")
    words = subject.split()
    if len(words) > 10:
        subject = " ".join(words[:10]).strip()
    return subject or "the core subject"


def _title_signature_tokens(text: str) -> list[str]:
    cleaned = re.sub(r"^\s*top\s+\d+\b", "", str(text or "").lower())
    cleaned = re.sub(r"\b(full|complete|ultimate)\s+breakdown\b", "", cleaned)
    cleaned = re.sub(r"[^a-z0-9\s']", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return _packaging_tokens(cleaned, max_items=18)


def _title_is_too_close_to_source(candidate: str, source_title: str) -> bool:
    cand = str(candidate or "").strip()
    source = str(source_title or "").strip()
    if not cand or not source:
        return False
    cand_lower = re.sub(r"\s+", " ", cand.lower())
    source_lower = re.sub(r"\s+", " ", source.lower())
    if cand_lower == source_lower:
        return True
    cand_tokens = _title_signature_tokens(cand)
    source_tokens = _title_signature_tokens(source)
    if not cand_tokens or not source_tokens:
        return False
    if cand_tokens == source_tokens:
        return True
    shared = len(set(cand_tokens) & set(source_tokens))
    overlap = shared / max(len(set(source_tokens)), 1)
    if cand_lower.startswith("top ") and source_lower.startswith("top ") and overlap >= 0.50:
        return True
    if overlap >= 0.78:
        return True
    if shared >= 4 and len(source_tokens) <= 5:
        return True
    return False


def _title_is_too_close_to_any(candidate: str, existing_titles: list[str]) -> bool:
    cand = str(candidate or "").strip()
    if not cand:
        return False
    for existing in list(existing_titles or []):
        current = str(existing or "").strip()
        if current and _title_is_too_close_to_source(cand, current):
            return True
    return False


def _title_opening_signature(text: str, max_tokens: int = 3) -> str:
    cleaned = str(text or "").strip().lower()
    if not cleaned:
        return ""
    cleaned = re.sub(r"^\s*top\s+\d+\b", "toplist", cleaned)
    cleaned = re.sub(r"^\s*what\s+(?:really\s+)?happened(?:\s+to)?\b", "whathappened", cleaned)
    tokens = _title_signature_tokens(cleaned)
    if not tokens:
        return ""
    return " ".join(tokens[: max(1, int(max_tokens or 3))]).strip()


def _title_reuses_opening_pattern(candidate: str, source_title: str = "", recent_titles: list[str] | None = None) -> bool:
    cand = str(candidate or "").strip()
    if not cand:
        return False
    cand_sig = _title_opening_signature(cand)
    cand_pattern, _ = _source_title_pattern(cand)
    cand_focus = _clean_same_arena_phrase(_same_arena_focus_entity({"title": cand}), max_words=4).lower()
    compare_titles = [str(source_title or "").strip(), *[str(v).strip() for v in list(recent_titles or []) if str(v).strip()]]
    for existing in compare_titles:
        if not existing:
            continue
        if cand_sig and cand_sig == _title_opening_signature(existing):
            return True
        existing_pattern, _ = _source_title_pattern(existing)
        existing_focus = _clean_same_arena_phrase(_same_arena_focus_entity({"title": existing}), max_words=4).lower()
        if cand_pattern == "top_list" and existing_pattern == "top_list" and cand_focus and cand_focus == existing_focus:
            return True
    return False


def _clean_same_arena_phrase(text: str, max_words: int = 8) -> str:
    phrase = str(text or "").strip()
    if not phrase:
        return ""
    phrase = re.sub(r"\s*\|\s*.*$", "", phrase)
    phrase = re.sub(r"^\s*top\s+\d+\s+", "", phrase, flags=re.IGNORECASE)
    phrase = re.sub(r"^\s*(how|why)\s+", "", phrase, flags=re.IGNORECASE)
    phrase = re.sub(r"^\s*what\s+(?:really\s+)?happened\s+to\s+", "", phrase, flags=re.IGNORECASE)
    phrase = re.sub(r"^\s*(the\s+truth\s+about|inside|the\s+story\s+of)\s+", "", phrase, flags=re.IGNORECASE)
    phrase = re.sub(
        r"\b(disturbing|shocking|hidden|dangerous|crazy|insane|darkest|biggest|worst|ultimate|complete|full)\b",
        "",
        phrase,
        flags=re.IGNORECASE,
    )
    phrase = re.sub(
        r"\b(secrets?|facts?|truths?|lessons?|reasons?|ways?|mistakes?|patterns?|blind\s+spots?)\b",
        "",
        phrase,
        flags=re.IGNORECASE,
    )
    phrase = re.sub(
        r"\b(hides?\s+from\s+you|hide\s+from\s+you|keeps?\s+from\s+you|keep\s+from\s+you|you\s+never\s+notice|most\s+people\s+never\s+notice)\b",
        "",
        phrase,
        flags=re.IGNORECASE,
    )
    phrase = re.sub(r"\s+", " ", phrase).strip(" -,:")
    words = phrase.split()
    if len(words) > max_words:
        phrase = " ".join(words[:max_words]).strip()
    return phrase


def _same_arena_focus_entity(source_bundle: dict, topic: str = "") -> str:
    topic_text = str(topic or "").strip()
    title = topic_text or str((source_bundle or {}).get("title", "") or "").strip()
    patterns = [
        r"^\s*top\s+\d+\s+(?:[a-z' -]{0,24})?(?:secrets?|facts?|truths?|mistakes?|patterns?|blind\s+spots?)\s+(.+?)\s+(?:hide|hides|keep|keeps)\s+from\s+you\b",
        r"^\s*top\s+\d+\s+(?:[a-z' -]{0,24})?(?:secrets?|facts?|truths?|mistakes?|patterns?|blind\s+spots?)\s+(.+)$",
        r"^\s*(.+?)\s+(?:hide|hides)\s+from\s+you\b",
        r"^\s*(?:why|how)\s+(.+?)\s+(?:quietly|actually|really|works|became|is|are|keeps?)\b",
        r"^\s*what\s+(?:really\s+)?happened\s+to\s+(.+)$",
    ]
    for pattern in patterns:
        match = re.search(pattern, title, flags=re.IGNORECASE)
        if not match:
            continue
        candidate = _clean_same_arena_phrase(match.group(1), max_words=6)
        if candidate and 1 <= len(candidate.split()) <= 6:
            return candidate
    fallback = _clean_same_arena_phrase(title or topic_text, max_words=6)
    return fallback


def _manga_recap_packaging_rescue_titles(
    source_title: str,
    focus: str,
    series_anchor: str = "",
    max_items: int = 6,
) -> list[str]:
    raw_title = str(source_title or "").strip()
    anchor = _clean_same_arena_phrase(series_anchor or focus or raw_title, max_words=5)
    lowered = raw_title.lower()
    variants: list[str] = []

    if any(token in lowered for token in ["sect", "clan", "blade", "heir", "ruins", "returned", "returns"]):
        variants.extend([
            "They Destroyed His Sect. He Came Back Untouchable",
            "The Last Heir They Tried to Erase",
            "Betrayed by His Clan, Feared by Everyone",
            "They Left Him in Ruins. He Returned Deadlier",
            f"{anchor}: The Return They Never Saw Coming" if anchor else "",
        ])

    if any(token in lowered for token in ["married", "count", "husband", "wife", "empress", "duke"]):
        variants.extend([
            "She Married a Monster in Disguise",
            "She Tried to Expose Him. He Was Worse Than Expected",
            "Her Husband Was the Biggest Trap of All",
            f"{anchor}: The Marriage That Turned Into a Nightmare" if anchor else "",
        ])

    if any(token in lowered for token in ["punishment", "sentenced", "prison", "hell", "abyss"]):
        variants.extend([
            "His Punishment Created a Monster",
            "They Gave Him the Worst Fate Possible",
            "The Punishment That Made Him Unstoppable",
        ])

    if not variants:
        focus_subject = anchor or "the hero"
        variants.extend([
            f"They Betrayed {focus_subject}. Now Everyone Pays",
            f"{focus_subject.title()} Became the Threat They Feared Most",
            f"The Return of {focus_subject.title()}",
        ])

    compact = []
    for value in variants:
        title = _clip_text(str(value or "").strip(), 96)
        if not title:
            continue
        compact.append(title)
    return _dedupe_clip_list(compact, max_items=max_items, max_chars=110)


def _same_arena_title_variants(
    source_bundle: dict,
    topic: str = "",
    format_preset: str = "documentary",
    max_items: int = 3,
) -> list[str]:
    source_title = str((source_bundle or {}).get("title", "") or "").strip()
    subject = _same_arena_subject(source_bundle, topic=topic)
    focus = _same_arena_focus_entity(source_bundle, topic=topic) or subject
    niche = _catalyst_infer_niche(source_title, subject, focus, topic, format_preset=format_preset)
    niche_key = str(niche.get("key", "") or "").strip().lower()
    series_anchor = _catalyst_extract_series_anchor(source_title, topic, focus, niche_key=niche_key)
    pattern, top_number = _source_title_pattern(source_title)
    variants: list[str] = []
    if niche_key == "manga_recap":
        variants.extend(_manga_recap_packaging_rescue_titles(source_title, focus, series_anchor))
        if series_anchor:
            variants.extend([
                f"Why {series_anchor} Became Everyone's Worst Nightmare",
                f"How {series_anchor} Broke the Entire Power System",
                f"The Brutal Rise of {series_anchor}",
            ])
        variants.extend([
            f"Why {focus} Became Everyone's Worst Nightmare",
            f"How {focus} Broke the Entire Power System",
            f"The Brutal Rise of {focus}",
        ])
    elif niche_key == "day_trading":
        variants.extend([
            "Why Most Traders Keep Losing This Setup",
            "The Day Trading Trap That Keeps Wiping People Out",
            "The Market Pattern Most Traders Keep Misreading",
        ])
    elif niche_key == "dark_psychology":
        variants.extend([
            "The Hidden Pattern Quietly Rewriting Your Decisions",
            "Why Your Brain Keeps Falling for the Same Trap",
            "The Mental Blind Spot Running More Than You Think",
        ])
    elif niche_key == "geopolitics_history":
        variants.extend([
            f"The Hidden Power System Behind {focus}",
            f"How {focus} Quietly Changed the Entire Region",
            f"What {focus} Set in Motion Next",
        ])
    elif niche_key == "business_documentary":
        variants.extend([
            f"The System Behind {focus} No One Sees",
            f"Why {focus} Quietly Controls the Outcome",
            f"How {focus} Shapes Power Behind the Scenes",
        ])
    if pattern == "top_list":
        if re.search(r"\b(brain|mind|memory|attention)\b", focus, flags=re.IGNORECASE):
            variants.extend([
                "The Brain Blind Spots Quietly Rewriting Your Decisions",
                "Why Your Brain Hides the Signals That Matter Most",
                "The Mental Shortcuts Quietly Running More Than You Think",
            ])
        else:
            variants.extend([
                f"The Hidden System Behind {focus}",
                f"Why {focus} Keeps Misleading People",
                f"What {focus} Reveals About How This Really Works",
            ])
    elif pattern == "why":
        variants.extend([
            f"Why {subject} Quietly Drives Everything",
            f"Why {subject} Is More Dangerous Than It Looks",
            f"Why {subject} Keeps Repeating the Same Pattern",
        ])
    elif pattern == "how":
        variants.extend([
            f"How {subject} Actually Works",
            f"How {subject} Quietly Shapes the Outcome",
            f"How {subject} Became This Powerful",
        ])
    elif pattern == "what_happened":
        variants.extend([
            f"What Really Happened to {subject}",
            f"The Hidden Story Behind {subject}",
            f"How {subject} Ended Up Here",
        ])
    elif pattern == "investigation":
        variants.extend([
            f"How {subject} Was Really Pulled Off",
            f"The Real Story Behind {subject}",
            f"What {subject} Changed Next",
        ])
    else:
        variants.extend([
            f"The Hidden System Behind {subject}",
            f"How {subject} Quietly Runs the Game",
            f"The Truth About {subject}",
        ])
    if str(format_preset or "").strip().lower() == "documentary":
        variants.append(f"How {subject} Actually Shapes Power")
    filtered = [v for v in variants if not _title_is_too_close_to_source(v, source_title)]
    if not filtered:
        filtered = variants
    return _dedupe_clip_list(filtered, max_items=max_items, max_chars=140)


def _same_arena_description_variants(
    source_bundle: dict,
    topic: str = "",
    source_analysis: dict | None = None,
    max_items: int = 3,
) -> list[str]:
    subject = _same_arena_subject(source_bundle, topic=topic)
    improvement_moves = [str(v).strip() for v in list((source_analysis or {}).get("improvement_moves") or []) if str(v).strip()]
    move = improvement_moves[0] if improvement_moves else "Open faster, tighten the payoff, and keep every scene pushing one concrete reveal."
    base_lines = [
        f"A sharper follow-up on {subject} with a clearer opening promise, stronger escalation, and cleaner documentary packaging.",
        f"Catalyst rebuilds this angle around {subject} with a faster first 30 seconds, better visual variety, and a more satisfying payoff.",
        f"Built as a premium faceless documentary package around {subject}. Improvement focus: {move}",
    ]
    return _dedupe_clip_list(base_lines, max_items=max_items, max_chars=220)


def _same_arena_thumbnail_angles(
    source_bundle: dict,
    topic: str = "",
    format_preset: str = "documentary",
    max_items: int = 3,
) -> list[str]:
    subject = _same_arena_subject(source_bundle, topic=topic)
    normalized_format = str(format_preset or "").strip().lower()
    niche = _catalyst_infer_niche(str((source_bundle or {}).get("title", "") or ""), subject, topic, format_preset=format_preset)
    niche_key = str(niche.get("key", "") or "").strip().lower()
    series_anchor = _catalyst_extract_series_anchor(str((source_bundle or {}).get("title", "") or ""), topic, subject, niche_key=niche_key)
    if niche_key == "manga_recap":
        angles = [
            f"{series_anchor or subject} as one dominant angry protagonist close-up, betrayal cue or ruined clan in the background, premium manga/manhwa recap thumbnail, strong hierarchy, 16:9",
            f"{series_anchor or subject} at the peak of a revenge or power reveal, one clear enemy threat, intense contrast, one obvious focal face, recap-ready 16:9 packaging",
            f"One overpowered hero-versus-system composition built around {series_anchor or subject}, clean betrayal stakes, bold emotion, premium recap thumbnail, clear focal hierarchy, 16:9",
        ]
    elif niche_key == "day_trading":
        angles = [
            f"{subject} turned into a premium trading thumbnail: one dominant chart or setup, one trader reaction, one red or green consequence cue, 16:9",
            f"Day trading desk scene built around {subject}, sharp chart hierarchy, one highlighted setup failure or win, premium finance thumbnail, 16:9",
            f"One oversized candlestick or liquidity zone interacting with a trader silhouette, clean black background, strong contrast, 16:9",
        ]
    elif normalized_format == "documentary":
        angles = [
            f"{subject} shown as one dominant 3D hero object in a dark void, hard contrast, one red accent element, minimal background clutter, 16:9 documentary thumbnail",
            f"{subject} translated into a clean tabletop system or map composition with one red highlighted zone, white-gray environment, strong hierarchy, 16:9",
            f"One stylized 3D human subject interacting with an oversized symbol of {subject}, black background, strong rim light, red accent, premium documentary clarity",
        ]
    else:
        angles = [
            f"One dominant visual symbol for {subject}, clear hierarchy, no clutter, 16:9",
            f"Human-versus-system composition built around {subject}, strong contrast, 16:9",
            f"Minimal graphic scene centered on {subject} with one aggressive focal cue, 16:9",
        ]
    return _dedupe_clip_list(angles, max_items=max_items, max_chars=220)


def _longform_build_publish_package_candidates(
    *,
    template: str,
    format_preset: str,
    topic: str,
    input_title: str,
    input_description: str,
    source_bundle: dict | None = None,
    source_analysis: dict | None = None,
    channel_context: dict | None = None,
    channel_memory: dict | None = None,
) -> dict:
    source_bundle = dict(source_bundle or {})
    source_analysis = dict(source_analysis or {})
    channel_context = dict(channel_context or {})
    source_title = str(source_bundle.get("title", "") or "").strip()
    effective_topic = str(topic or input_title or source_title or "Follow-up video breakdown").strip()
    series_context = _resolve_catalyst_series_context(
        channel_context,
        channel_memory=channel_memory,
        topic=effective_topic,
        source_title=source_title,
        input_title=input_title,
        input_description=input_description,
        format_preset=format_preset,
    )
    series_anchor_override = str(series_context.get("series_anchor_override", "") or "").strip()
    selected_cluster = dict(series_context.get("selected_cluster") or {})
    channel_memory = dict(series_context.get("memory_view") or {})
    cluster_playbook = dict(series_context.get("cluster_playbook") or {})
    reference_playbook = _build_catalyst_reference_playbook(
        reference_memory=_catalyst_reference_memory,
        format_preset=format_preset,
        topic=effective_topic,
        channel_memory=channel_memory,
        selected_cluster=selected_cluster,
    )
    rewrite_pressure = dict(channel_memory.get("rewrite_pressure") or {})
    best_cluster = dict(cluster_playbook.get("best_cluster") or {})
    series_anchor = _clip_text(str(channel_memory.get("series_anchor", "") or series_anchor_override or ""), 120)
    archetype_label = _clip_text(str(channel_memory.get("archetype_label", "") or selected_cluster.get("archetype_label", "") or ""), 120)
    archetype_hook_rule = _clip_text(str(channel_memory.get("archetype_hook_rule", "") or selected_cluster.get("archetype_hook_rule", "") or ""), 220)
    archetype_visual_rule = _clip_text(str(channel_memory.get("archetype_visual_rule", "") or selected_cluster.get("archetype_visual_rule", "") or ""), 220)
    archetype_packaging_rule = _clip_text(str(channel_memory.get("archetype_packaging_rule", "") or selected_cluster.get("archetype_packaging_rule", "") or ""), 220)
    channel_title_memory = [
        str(v).strip()
        for v in [
            *list(channel_context.get("recent_upload_titles") or []),
            *list(channel_context.get("top_video_titles") or []),
            *list(channel_memory.get("recent_selected_titles") or []),
            *list(selected_cluster.get("sample_titles") or []),
        ]
        if str(v).strip()
    ]
    weighted_packaging_rewrites = [str(v).strip() for v in list(channel_memory.get("reference_packaging_rewrites") or []) if str(v).strip()]
    weighted_next_moves = [str(v).strip() for v in list(channel_memory.get("reference_next_video_moves") or channel_memory.get("next_video_moves") or []) if str(v).strip()]
    reference_packaging_rewrites = [str(v).strip() for v in list(reference_playbook.get("packaging_rewrites") or []) if str(v).strip()]
    reference_next_moves = [str(v).strip() for v in list(reference_playbook.get("next_video_moves") or []) if str(v).strip()]
    reference_hook_rewrites = [str(v).strip() for v in list(reference_playbook.get("hook_rewrites") or []) if str(v).strip()]
    reference_visual_rewrites = [str(v).strip() for v in list(reference_playbook.get("visual_rewrites") or []) if str(v).strip()]
    reference_proven_keywords = [str(v).strip() for v in list(reference_playbook.get("proven_keywords") or []) if str(v).strip()]
    cluster_next_moves = [str(v).strip() for v in list(cluster_playbook.get("next_run_moves") or []) if str(v).strip()]
    cluster_winning_patterns = [str(v).strip() for v in list(cluster_playbook.get("winning_patterns") or []) if str(v).strip()]
    best_cluster_keywords = [str(v).strip() for v in list(best_cluster.get("keywords") or []) if str(v).strip()]
    pressure_primary_focus = str(rewrite_pressure.get("primary_focus", "") or "").strip()
    pressure_subject = _same_arena_subject(source_bundle or {"title": effective_topic}, topic=effective_topic) or effective_topic
    cluster_label = _clip_text(str(selected_cluster.get("label", "") or "").strip(), 120)
    cluster_follow_rule = _clip_text(str(selected_cluster.get("follow_up_rule", "") or "").strip(), 220)
    package_subject = series_anchor or cluster_label or pressure_subject
    best_archetype_memory = dict(channel_memory.get("best_archetype_memory") or {})
    weakest_archetype_memory = dict(channel_memory.get("weakest_archetype_memory") or {})
    archetype_memory_summary = _clip_text(str(channel_memory.get("archetype_memory_summary", "") or "").strip(), 240)
    promoted_archetype_keywords = [str(v).strip() for v in list(best_archetype_memory.get("proven_keywords") or []) if str(v).strip()]
    demoted_archetype_keywords = [str(v).strip() for v in list(weakest_archetype_memory.get("proven_keywords") or []) if str(v).strip()]
    package_niche = _catalyst_infer_niche(
        source_title,
        package_subject,
        effective_topic,
        format_preset=format_preset,
    )
    package_niche_key = str(package_niche.get("key", "") or "").strip().lower()
    title_candidates: list[tuple[str, int]] = []
    for candidate in [
        *list(source_analysis.get("title_angles") or []),
        *[
            f"{package_subject} | {keyword}".replace("|", "vs.") if " vs." not in package_subject.lower() else f"{package_subject} {keyword}"
            for keyword in list(channel_memory.get("proven_keywords") or [])[:2]
        ],
        *(
            [
                f"The Hidden System Behind {package_subject}",
                f"Why {package_subject} Quietly Controls More Than You Think",
                f"What {package_subject} Is Really Doing Behind the Scenes",
            ]
            if pressure_primary_focus in {"hook", "packaging"} or weighted_packaging_rewrites
            else []
        ),
        *(
            [
                f"{package_subject}: The Hidden Edge Most People Miss",
                f"The Truth About {package_subject} That Changes Everything",
            ]
            if archetype_label in {"Trading Execution", "Dark Psychology", "Power History"}
            else []
        ),
        *[
            f"{package_subject} {keyword}"
            for keyword in best_cluster_keywords[:2]
            if keyword and keyword.lower() not in package_subject.lower()
        ],
        *_same_arena_title_variants(
            source_bundle or {"title": effective_topic},
            topic=effective_topic,
            format_preset=format_preset,
            max_items=6,
        ),
        _longform_title_variant(input_title or package_subject, package_subject),
    ]:
        value = str(candidate or "").strip()
        if not value:
            continue
        if source_title and _title_is_too_close_to_source(value, source_title):
            continue
        if _title_is_too_close_to_any(value, channel_title_memory):
            continue
        if _title_reuses_opening_pattern(value, source_title, channel_title_memory):
            continue
        keyword_bonus = 0
        if any(keyword.lower() in value.lower() for keyword in list(channel_memory.get("proven_keywords") or [])[:6]):
            keyword_bonus += 6
        if any(keyword.lower() in value.lower() for keyword in reference_proven_keywords[:8]):
            keyword_bonus += 5
        if any(keyword.lower() in value.lower() for keyword in promoted_archetype_keywords[:6]):
            keyword_bonus += 5
        if any(keyword.lower() in value.lower() for keyword in demoted_archetype_keywords[:6]):
            keyword_bonus -= 6
        if not re.match(r"^\s*\d+", value) and re.match(r"^\s*\d+", source_title):
            keyword_bonus += 8
        novelty = _catalyst_title_novelty_score(value, source_title=source_title, recent_titles=channel_title_memory)
        candidate_score = novelty + keyword_bonus
        if package_niche_key == "manga_recap":
            lowered_value = value.lower()
            title_len = len(value)
            if title_len <= 72:
                candidate_score += 8
            elif title_len >= 90:
                candidate_score -= 14
            if " then " in lowered_value or "," in value:
                candidate_score -= 10
            if lowered_value.startswith("a ") or lowered_value.startswith("the "):
                candidate_score -= 2
            if any(token in lowered_value for token in ["betrayed", "return", "returned", "insane", "monster", "deadliest", "worst", "punishment"]):
                candidate_score += 6
            if "| manhwa recap" in lowered_value or "| manga recap" in lowered_value:
                candidate_score -= 2 if title_len > 78 else 0
        if (weighted_packaging_rewrites or reference_packaging_rewrites) and any(
            token.lower() in value.lower()
            for token in _extract_catalyst_keywords(
                " ".join([*weighted_packaging_rewrites, *reference_packaging_rewrites]),
                max_items=12,
            )
        ):
            candidate_score += 4
        title_candidates.append((value, candidate_score))
    title_candidates.sort(key=lambda row: row[1], reverse=True)
    title_variants = _dedupe_preserve_order([value for value, _score in title_candidates], max_items=6, max_chars=160)
    if not title_variants:
        rescue_subject = series_anchor or _same_arena_subject(source_bundle, topic=effective_topic) or effective_topic
        for candidate in [
            f"The Hidden System Behind {rescue_subject}",
            f"Why {rescue_subject} Quietly Rewrites More Than You Think",
            f"How {rescue_subject} Actually Shapes the Outcome",
        ]:
            value = str(candidate or "").strip()
            if (
                value
                and not _title_is_too_close_to_any(value, [source_title, *channel_title_memory])
                and not _title_reuses_opening_pattern(value, source_title, channel_title_memory)
                and value not in title_variants
            ):
                title_variants.append(value)
    if package_niche_key == "manga_recap":
        title_variants = _dedupe_preserve_order(
            [
                *_manga_recap_packaging_rescue_titles(source_title, effective_topic or package_subject, series_anchor, max_items=4),
                *title_variants,
            ],
            max_items=6,
            max_chars=120,
        )

    description_variants: list[str] = []
    for candidate in [
        input_description,
        *list(source_analysis.get("description_angles") or []),
        *cluster_next_moves[:2],
        *weighted_next_moves[:2],
        *reference_next_moves[:2],
        cluster_follow_rule,
        archetype_memory_summary,
        archetype_hook_rule,
        archetype_packaging_rule,
        *reference_hook_rewrites[:2],
        *list(channel_memory.get("packaging_learnings") or [])[:2],
        *_same_arena_description_variants(
            source_bundle or {"title": effective_topic},
            topic=effective_topic,
            source_analysis=source_analysis,
            max_items=4,
        ),
    ]:
        value = _clip_text(str(candidate or "").strip(), 420)
        if value and value not in description_variants:
            description_variants.append(value)

    thumbnail_prompts: list[str] = []
    for candidate in [
        *list(source_analysis.get("thumbnail_angles") or []),
        *cluster_winning_patterns[:2],
        *weighted_packaging_rewrites[:2],
        *reference_packaging_rewrites[:2],
        *cluster_next_moves[:1],
        *weighted_next_moves[:2],
        *reference_visual_rewrites[:2],
        archetype_memory_summary,
        archetype_visual_rule,
        archetype_packaging_rule,
        *list(channel_memory.get("packaging_learnings") or [])[:2],
        *_same_arena_thumbnail_angles(
            source_bundle or {"title": effective_topic},
            topic=package_subject,
            format_preset=format_preset,
            max_items=4,
        ),
        f"{package_subject} premium faceless documentary thumbnail, one dominant 3D subject, strong contrast, 16:9",
    ]:
        value = str(candidate or "").strip()
        if value and value not in thumbnail_prompts:
            thumbnail_prompts.append(value)

    tags: list[str] = []
    for candidate in [
        template,
        format_preset,
        "nyptid",
        "longform",
        archetype_label[:32].replace(" ", "_").lower() if archetype_label else "",
        package_subject[:32].replace(" ", "_").lower(),
        *[str(tag).strip().replace(" ", "_").lower() for tag in list(source_bundle.get("tags") or [])[:10] if str(tag).strip()],
        *[str(tag).strip().replace(" ", "_").lower() for tag in list(channel_memory.get("proven_keywords") or [])[:6] if str(tag).strip()],
        *[str(tag).strip().replace(" ", "_").lower() for tag in reference_proven_keywords[:8] if str(tag).strip()],
        *[str(tag).strip().replace(" ", "_").lower() for tag in list(selected_cluster.get("keywords") or [])[:6] if str(tag).strip()],
    ]:
        value = str(candidate or "").strip()
        if value and value not in tags:
            tags.append(value)

    return {
        "title_variants": title_variants[:3],
        "description_variants": description_variants[:3],
        "thumbnail_prompts": thumbnail_prompts[:3],
        "tags": tags[:16],
    }


async def _persist_catalyst_outcome_for_session(
    *,
    session_id: str,
    user_id: str,
    session: dict,
    req: CatalystOutcomeIngestRequest,
    video_meta: dict | None = None,
    analytics_metrics: dict | None = None,
) -> tuple[dict, dict]:
    outcome_record = _build_catalyst_outcome_record(
        session_snapshot=session,
        outcome_req=req,
        video_meta=video_meta,
        analytics_metrics=analytics_metrics,
        source_url_video_id_fn=_source_url_video_id,
        score_against_reference_fn=lambda **kwargs: _score_catalyst_outcome_against_reference(
            reference_memory=_catalyst_reference_memory,
            **kwargs,
        ),
    )

    channel_id = str(session.get("youtube_channel_id", "") or "").strip()
    async with _catalyst_memory_lock:
        _load_catalyst_memory()
        learning_key = str(session.get("session_id", "") or session_id)
        channel_memory_key = str(
            session.get("channel_memory_key", "")
            or _catalyst_channel_memory_key(
                user_id,
                channel_id,
                str(session.get("format_preset", "") or "documentary"),
            )
        )
        existing_memory = dict(_catalyst_channel_memory.get(channel_memory_key) or session.get("channel_memory") or {})
        updated_channel_memory = _apply_catalyst_outcome_to_channel_memory(
            existing=existing_memory,
            session_snapshot=session,
            outcome_record=outcome_record,
        )
        updated_channel_memory["key"] = channel_memory_key
        _catalyst_channel_memory[channel_memory_key] = updated_channel_memory
        existing_learning_entry = _catalyst_learning_records.get(learning_key)
        if isinstance(existing_learning_entry, dict) and ("prepublish" in existing_learning_entry or "outcome_history" in existing_learning_entry):
            learning_entry = dict(existing_learning_entry)
        else:
            learning_entry = {
                "prepublish": dict(session.get("learning_record") or existing_learning_entry or {}),
                "outcome_history": [],
            }
        history = [dict(item or {}) for item in list(learning_entry.get("outcome_history") or []) if isinstance(item, dict)]
        history.append(outcome_record)
        learning_entry["latest_outcome"] = outcome_record
        learning_entry["outcome_history"] = history[-16:]
        _catalyst_learning_records[learning_key] = learning_entry
        _save_catalyst_memory()

    async with _longform_sessions_lock:
        _load_longform_sessions()
        session_live = _longform_sessions.get(session_id)
        if not isinstance(session_live, dict):
            raise HTTPException(404, "Long-form session not found")
        if channel_id:
            session_live["youtube_channel_id"] = channel_id
        metadata_pack = dict(session_live.get("metadata_pack") or {})
        youtube_channel_context = dict((dict(session.get("metadata_pack") or {})).get("youtube_channel") or {})
        if youtube_channel_context:
            metadata_pack["youtube_channel"] = youtube_channel_context
        metadata_pack["catalyst_channel_memory"] = _catalyst_channel_memory_public_view(updated_channel_memory)
        session_live["metadata_pack"] = metadata_pack
        session_live["latest_outcome"] = outcome_record
        session_live["channel_memory"] = _catalyst_channel_memory_public_view(updated_channel_memory)
        session_live["updated_at"] = time.time()
        _save_longform_sessions()
        session_live = dict(session_live)
    return outcome_record, session_live


async def _harvest_catalyst_outcomes_for_channel(
    *,
    user_id: str,
    channel_id: str,
    session_id: str = "",
    candidate_limit: int = 18,
    refresh_existing: bool = False,
) -> dict:
    return await _harvest_catalyst_outcomes_for_channel_for_user(
        user_id=str(user_id or "").strip(),
        channel_id=str(channel_id or "").strip(),
        session_id=str(session_id or "").strip(),
        candidate_limit=int(candidate_limit or 18),
        refresh_existing=bool(refresh_existing),
    )


async def _maybe_refresh_channel_outcomes_before_longform_run(
    *,
    user_id: str,
    channel_id: str,
    min_interval_sec: int = 1800,
) -> dict:
    return await _maybe_refresh_channel_outcomes_before_longform_run_for_user(
        user_id=str(user_id or "").strip(),
        channel_id=str(channel_id or "").strip(),
        min_interval_sec=int(min_interval_sec or 1800),
    )


async def _shortform_priority_snapshot() -> dict:
    queue_depth = 0
    queue_workers = 1
    queue_max_depth = 1
    try:
        queue_depth = max(0, int(await get_queue_depth()))
    except Exception:
        queue_depth = 0
    try:
        queue_workers = max(1, int(get_queue_workers()))
    except Exception:
        queue_workers = 1
    try:
        queue_max_depth = max(1, int(get_queue_max_depth()))
    except Exception:
        queue_max_depth = max(1, queue_workers)

    active_shortform = 0
    active_longform = 0
    for job in list(jobs.values()):
        if not isinstance(job, dict):
            continue
        status = str(job.get("status", "") or "").strip().lower()
        if status not in _SHORTFORM_PROTECTED_ACTIVE_JOB_STATUSES:
            continue
        lane = str(job.get("lane", "") or "").strip().lower()
        if lane == "longform":
            active_longform += 1
            continue
        if lane in _SHORTFORM_PROTECTED_LANES:
            active_shortform += 1

    queue_utilization_pct = round((queue_depth / max(queue_max_depth, 1)) * 100.0, 1)
    priority_active = bool(active_shortform > 0 or queue_depth > 0)
    reason = ""
    if active_shortform > 0:
        reason = "active_shortform_jobs"
    elif queue_depth > 0:
        reason = "shared_queue_busy"
    return {
        "priority_active": priority_active,
        "reason": reason,
        "active_shortform_jobs": int(active_shortform),
        "active_longform_jobs": int(active_longform),
        "queue_depth": int(queue_depth),
        "queue_workers": int(queue_workers),
        "queue_max_depth": int(queue_max_depth),
        "queue_utilization_pct": float(queue_utilization_pct),
    }


async def _mark_longform_waiting_for_shortform_capacity(
    session_id: str,
    *,
    stage: str,
    snapshot: dict | None = None,
) -> None:
    snapshot = dict(snapshot or {})
    async with _longform_sessions_lock:
        live = _longform_sessions.get(session_id)
        if not isinstance(live, dict):
            return
        progress = dict(live.get("draft_progress") or {})
        progress["stage"] = stage
        progress["shortform_priority"] = {
            "reason": str(snapshot.get("reason", "") or ""),
            "active_shortform_jobs": int(snapshot.get("active_shortform_jobs", 0) or 0),
            "queue_depth": int(snapshot.get("queue_depth", 0) or 0),
            "queue_workers": int(snapshot.get("queue_workers", 1) or 1),
            "queue_utilization_pct": float(snapshot.get("queue_utilization_pct", 0.0) or 0.0),
        }
        live["draft_progress"] = progress
        live["status"] = "draft_review"
        live["updated_at"] = time.time()
        _save_longform_sessions()


def _heuristic_source_performance_analysis(
    source_bundle: dict,
    analytics_notes: str = "",
    topic: str = "",
    input_title: str = "",
    input_description: str = "",
    format_preset: str = "documentary",
) -> dict:
    source_title = str((source_bundle or {}).get("title", "") or "").strip()
    channel = str((source_bundle or {}).get("channel", "") or "").strip()
    public_summary = _clip_text(str((source_bundle or {}).get("public_summary", "") or "").strip(), 260)
    transcript_excerpt = _clip_text(str((source_bundle or {}).get("transcript_excerpt", "") or "").strip(), 220)
    what_worked_parts = []
    if source_title:
        what_worked_parts.append(f"Source title already frames a strong curiosity gap: {source_title}.")
    if channel:
        what_worked_parts.append(f"Channel context: {channel}.")
    if public_summary:
        what_worked_parts.append(public_summary)
    operator_lines = [
        re.sub(r"\s+", " ", line).strip()
        for line in str(analytics_notes or "").splitlines()
        if str(line or "").strip()
    ]
    operator_hurt_candidates: list[str] = []
    for line in operator_lines:
        lowered = line.lower()
        if lowered.startswith("weak points:") or lowered.startswith("retention findings:") or lowered.startswith("improvement moves:"):
            operator_hurt_candidates.extend(part.strip() for part in line.split(":", 1)[1].split(";") if part.strip())
        elif lowered.startswith("private analytics notes:"):
            operator_hurt_candidates.append(line.split(":", 1)[1].strip())
    cleaned_operator_notes = _strip_model_reasoning_artifacts(analytics_notes)
    if cleaned_operator_notes and _contains_analytics_signal(cleaned_operator_notes):
        operator_hurt_candidates.append(cleaned_operator_notes)
    what_hurt = _clip_text(
        next((item for item in operator_hurt_candidates if str(item or "").strip()), ""),
        240,
    ) or "No private analytics notes were supplied, so improvement focus should stay on the hook, thumbnail clarity, and first-minute pacing."
    hook_learnings = [
        "Keep the first beat readable in under three seconds.",
        "Use one dominant promise, not three competing ideas in the opening.",
    ]
    if transcript_excerpt:
        hook_learnings.append("Preserve the strongest spoken explanation beat from the source transcript, but compress the ramp-up.")
    click_drivers = [
        "Stay in the same subject arena as the source instead of drifting into a different niche.",
        "Use one strong visual symbol or conflict in the package.",
    ]
    dropoff_risks = [
        "Starting with setup before the payoff is visible.",
        "Repeating the same visual environment too many times in a row.",
    ]
    improvement_moves = [
        "Make the first 20 to 30 seconds payoff-oriented instead of scene-setting.",
        "Push more visual variation: hero object, macro mechanism, human interaction, map/system view, and before-versus-after contrasts.",
        "Keep the title in the same arena as the source, but tighten the promise and raise the stakes.",
    ]
    if analytics_notes:
        improvement_moves.append("Reflect the supplied analytics notes directly in the opening hook, pace, and thumbnail choice.")
    if input_description:
        improvement_moves.append("Preserve the requested documentary format while keeping the emotional charge high.")
    return {
        "what_worked": _clip_text(" ".join(what_worked_parts), 240),
        "what_hurt": what_hurt,
        "hook_learnings": _dedupe_clip_list(hook_learnings, max_items=5, max_chars=180),
        "click_drivers": _dedupe_clip_list(click_drivers, max_items=5, max_chars=180),
        "dropoff_risks": _dedupe_clip_list(dropoff_risks, max_items=5, max_chars=180),
        "improvement_moves": _dedupe_clip_list(improvement_moves, max_items=6, max_chars=200),
        "title_angles": _same_arena_title_variants(source_bundle, topic=topic or input_title, format_preset=format_preset),
        "thumbnail_angles": _same_arena_thumbnail_angles(source_bundle, topic=topic or input_title, format_preset=format_preset),
        "description_angles": _same_arena_description_variants(source_bundle, topic=topic or input_title, source_analysis={"improvement_moves": improvement_moves}),
    }


def _merge_source_analysis(primary: dict | None, fallback: dict | None) -> dict:
    primary = dict(primary or {})
    fallback = dict(fallback or {})
    merged: dict = {}
    for key in ("what_worked", "what_hurt", "analytics_summary"):
        merged[key] = _clip_text(str(primary.get(key) or fallback.get(key) or "").strip(), 240)
    for key, max_chars in {
        "hook_learnings": 180,
        "click_drivers": 180,
        "dropoff_risks": 180,
        "improvement_moves": 200,
        "title_angles": 140,
        "thumbnail_angles": 220,
        "description_angles": 220,
        "strongest_signals": 180,
        "weak_points": 180,
        "retention_findings": 180,
        "packaging_findings": 180,
    }.items():
        merged[key] = _dedupe_clip_list(
            [str(v).strip() for v in list(primary.get(key) or []) if str(v).strip()]
            + [str(v).strip() for v in list(fallback.get(key) or []) if str(v).strip()],
            max_items=8,
            max_chars=max_chars,
        )
    return merged


def _same_arena_follow_up_topic(source_bundle: dict, format_preset: str = "documentary") -> str:
    source_title = str((source_bundle or {}).get("title", "") or "").strip()
    subject = _same_arena_subject(source_bundle)
    focus = _same_arena_focus_entity(source_bundle) or subject
    niche = _catalyst_infer_niche(source_title, subject, focus, format_preset=format_preset)
    niche_key = str(niche.get("key", "") or "").strip().lower()
    series_anchor = _catalyst_extract_series_anchor(source_title, subject, focus, niche_key=niche_key)
    pattern, top_number = _source_title_pattern(source_title)
    if niche_key == "manga_recap":
        if series_anchor:
            return f"How {series_anchor} Raised the Stakes Even Higher"
        return f"How {focus} Became the Most Dangerous Force in the Story"
    if niche_key == "day_trading":
        return "The trading setup most people keep getting wrong"
    if niche_key == "dark_psychology":
        return "The hidden pattern quietly rewriting your decisions"
    if niche_key == "geopolitics_history":
        return f"The hidden power system behind {focus}"
    if pattern == "top_list":
        if re.search(r"\b(brain|mind|memory|attention)\b", focus, flags=re.IGNORECASE):
            return "The hidden blind spots inside your brain"
        return f"The hidden system behind {focus}"
    if pattern in {"why", "how"}:
        return f"How {focus} quietly shapes the outcome"
    if pattern in {"what_happened", "investigation"}:
        return f"The hidden chain of events behind {focus}"
    if str(format_preset or "").strip().lower() == "documentary":
        return f"The hidden system behind {focus}"
    return focus


def _heuristic_clone_analysis(topic: str, video_description: str, transcript_hint: str = "", source_notes: str = "") -> dict:
    source_title_match = re.search(r"Title:\s*([^|]+)", str(video_description or ""), flags=re.IGNORECASE)
    source_title = str(source_title_match.group(1) if source_title_match else "").strip()
    duration_match = re.search(r"Duration:\s*(\d+)s", str(video_description or ""), flags=re.IGNORECASE)
    duration_sec = int(duration_match.group(1)) if duration_match else 0
    lower_source = f"{source_title} {video_description} {source_notes}".lower()
    if any(token in lower_source for token in ["aita", "tifu", "reddit", "my sister", "my brother", "boyfriend", "girlfriend"]):
        detected_template = "reddit"
    elif re.match(r"^\s*top\s+\d+\b", source_title.lower()):
        detected_template = "top5"
    elif any(token in lower_source for token in ["roman", "empire", "iran", "war", "battle", "leader", "killed", "assassin"]):
        detected_template = "history"
    elif re.search(r"\b(vs\.?|versus)\b", source_title, flags=re.IGNORECASE):
        detected_template = "skeleton"
    else:
        detected_template = "story"
    pacing = "fast" if duration_sec <= 90 or detected_template in {"top5", "skeleton"} else "medium"
    avg_scene_duration = 3.5 if pacing == "fast" else 5.0
    scene_count = max(8, min(14, int(round((duration_sec or 45) / avg_scene_duration))))
    if detected_template == "history":
        tone = "authoritative documentary"
        tricks = ["open on consequence first", "use escalating evidence reveals", "anchor each beat to one big visual symbol"]
    elif detected_template == "top5":
        tone = "fast shocking countdown"
        tricks = ["numbered reveals", "higher stakes every beat", "one clean surprise per scene"]
    elif detected_template == "reddit":
        tone = "personal dramatic confession"
        tricks = ["conflict escalation", "reaction beats", "social-proof tension"]
    elif detected_template == "skeleton":
        tone = "aggressive comparison explainer"
        tricks = ["size contrast", "money comparison", "one-word impact captions"]
    else:
        tone = "cinematic documentary explainer"
        tricks = ["strong first reveal", "symbolic visual escalation", "tight payoff wording"]
    effective_topic = str(topic or "").strip() or _same_arena_follow_up_topic({"title": source_title}, format_preset="documentary")
    return {
        "detected_template": detected_template,
        "viral_analysis": {
            "hook_type": "numbered shock list" if detected_template == "top5" else ("comparison claim" if detected_template == "skeleton" else "documentary promise"),
            "pacing": pacing,
            "avg_scene_duration": avg_scene_duration,
            "scene_count": scene_count,
            "tone": tone,
            "retention_tricks": tricks,
            "what_made_it_viral": _clip_text(
                f"The source packaging works because it stays in one clear arena, makes the promise legible fast, and keeps escalating with one dominant idea per beat. {transcript_hint}",
                220,
            ),
            "follow_up_topic": effective_topic,
        },
        "optimized_prompt": _clip_text(
            f"Rebuild the source arena around {effective_topic}. Keep the same hook category, pacing pressure, and payoff style, but make the first three beats cleaner and more specific.",
            280,
        ),
    }


REFERENCE_LOCK_MODES = {"strict", "inspired"}
TEMPLATE_DEFAULT_REFERENCE_URLS = {
    "skeleton": str(SKELETON_GLOBAL_REFERENCE_IMAGE_URL or "").strip(),
    "story": str(STORY_GLOBAL_REFERENCE_IMAGE_URL or "").strip(),
    "motivation": str(MOTIVATION_GLOBAL_REFERENCE_IMAGE_URL or "").strip(),
}
DEFAULT_ART_STYLE_PRESETS = {
    "auto": "",
    "cinematic_realism": "Photoreal cinematic realism with natural skin detail, physically-plausible lighting, clean lens behavior, and premium film color grade.",
    "commercial_polish": "High-end commercial look: crisp product-grade detail, controlled highlights, clean background separation, and premium ad-level finish.",
    "moody_noir": "Moody low-key cinematic style with rich shadow contrast, tasteful grain-free clarity, and dramatic yet realistic lighting.",
    "bright_lifestyle": "Bright modern lifestyle aesthetic with soft natural light, inviting color balance, realistic textures, and clean premium framing.",
}


def _story_art_style_catalog_path() -> Path:
    return Path(__file__).parent / "ViralShorts-App" / "src" / "studio" / "lib" / "storyArtStyles.json"


def _load_story_art_style_presets() -> dict[str, str]:
    catalog_path = _story_art_style_catalog_path()
    try:
        rows = json.loads(catalog_path.read_text(encoding="utf-8"))
    except Exception:
        return dict(DEFAULT_ART_STYLE_PRESETS)
    if not isinstance(rows, list):
        return dict(DEFAULT_ART_STYLE_PRESETS)
    out: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        style_id = str(row.get("id", "") or "").strip().lower()
        if not style_id:
            continue
        out[style_id] = str(row.get("prompt", "") or "").strip()
    if "auto" not in out:
        out["auto"] = ""
    return out or dict(DEFAULT_ART_STYLE_PRESETS)


ART_STYLE_PRESETS = _load_story_art_style_presets()


def _normalize_reference_lock_mode(value, default: str = "strict") -> str:
    text = str(value or default).strip().lower()
    return text if text in REFERENCE_LOCK_MODES else default


def _default_reference_for_template(template: str) -> str:
    return str(TEMPLATE_DEFAULT_REFERENCE_URLS.get(str(template or "").strip().lower(), "") or "").strip()


def _normalize_reference_with_default(template: str, reference_image_url: str) -> str:
    value = str(reference_image_url or "").strip()
    if value:
        return value
    return _default_reference_for_template(template)


def _is_template_default_reference(template: str, reference_image_url: str) -> bool:
    current = str(reference_image_url or "").strip()
    default_ref = _default_reference_for_template(template)
    return bool(current and default_ref and current == default_ref)


def _skeleton_session_has_explicit_reference(session: dict | None) -> bool:
    if not isinstance(session, dict):
        return False
    return bool(
        session.get("reference_image_uploaded")
        or session.get("custom_reference_uploaded")
    )


def _skeleton_default_identity_locked(session: dict | None) -> bool:
    return not _skeleton_session_has_explicit_reference(session)


def _reference_url_to_local_asset_path(reference_image_url: str) -> Path | None:
    source = str(reference_image_url or "").strip()
    if not source:
        return None
    if Path(source).exists():
        return Path(source)
    try:
        parsed = urlparse(source)
    except Exception:
        return None
    if parsed.scheme not in {"http", "https"}:
        return None
    rel_path = unquote(str(parsed.path or "").lstrip("/"))
    if not rel_path:
        return None
    rel_parts = [part for part in Path(rel_path).parts if part not in ("..", "")]
    if not rel_parts:
        return None
    candidates = (
        Path(__file__).parent / "ViralShorts-App" / "public" / Path(*rel_parts),
        Path(__file__).parent / "public" / Path(*rel_parts),
    )
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


async def _read_reference_image_bytes(reference_image_url: str) -> bytes:
    source = str(reference_image_url or "").strip()
    if not source:
        return b""
    if source.startswith("data:image/"):
        raw, _mime = _decode_data_image_url(source)
        return raw or b""
    local_path = _reference_url_to_local_asset_path(source)
    if local_path is not None:
        try:
            return local_path.read_bytes()
        except Exception:
            return b""
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            resp = await client.get(source)
        if resp.status_code >= 400:
            return b""
        return bytes(resp.content or b"")
    except Exception:
        return b""


async def _extract_reference_profile(reference_image_url: str, template: str, lock_mode: str) -> tuple[dict, dict]:
    source = str(reference_image_url or "").strip()
    if not source:
        return {}, {}
    raw_ref = await _read_reference_image_bytes(source)
    if not raw_ref:
        return {}, {}
    quality = _analyze_reference_quality(raw_ref, lock_mode=lock_mode)
    reference_dna = _extract_reference_dna(raw_ref, template=template)
    return reference_dna, quality


def _billing_site_url() -> str:
    configured = str(os.getenv("BILLING_SITE_URL", "") or "").strip().rstrip("/")
    if configured:
        return configured
    site = str(SITE_URL or "").strip().rstrip("/")
    if not site:
        return "https://studio.nyptidindustries.com"
    match = re.match(r"^(https?://)([^/]+)(.*)$", site, flags=re.IGNORECASE)
    if not match:
        return "https://studio.nyptidindustries.com"
    scheme, host, suffix = match.groups()
    host_l = host.lower()
    for apex in ("nyptidindustries.com", "niptidindustries.com"):
        if host_l == f"studio.{apex}":
            return f"{scheme}{host}{suffix}"
        if host_l in {apex, f"billing.{apex}", f"invoicer.{apex}"} or host_l.endswith("." + apex):
            return f"{scheme}studio.{apex}{suffix}"
    return "https://studio.nyptidindustries.com"


def _api_public_url() -> str:
    configured = str(os.getenv("API_PUBLIC_URL", "") or "").strip().rstrip("/")
    if configured:
        return configured
    site = str(SITE_URL or "").strip().rstrip("/")
    if not site:
        return "https://api.nyptidindustries.com"
    match = re.match(r"^(https?://)([^/]+)(.*)$", site, flags=re.IGNORECASE)
    if not match:
        return "https://api.nyptidindustries.com"
    scheme, host, suffix = match.groups()
    host_l = host.lower()
    for apex in ("nyptidindustries.com", "niptidindustries.com"):
        if host_l == f"api.{apex}":
            return f"{scheme}{host}{suffix}"
        if host_l in {apex, f"studio.{apex}", f"billing.{apex}"} or host_l.endswith("." + apex):
            return f"{scheme}api.{apex}{suffix}"
    return "https://api.nyptidindustries.com"


def _paypal_enabled() -> bool:
    return bool(str(PAYPAL_CLIENT_ID or "").strip() and str(PAYPAL_CLIENT_SECRET or "").strip())


def _paypal_api_base() -> str:
    return "https://api-m.sandbox.paypal.com" if str(PAYPAL_ENV or "").strip().lower() == "sandbox" else "https://api-m.paypal.com"


_paypal_token_cache = {"token": "", "expires_at": 0.0}


async def _paypal_access_token() -> str:
    if not _paypal_enabled():
        raise HTTPException(500, "PayPal is not configured")
    now = time.time()
    cached = str(_paypal_token_cache.get("token", "") or "")
    expires_at = float(_paypal_token_cache.get("expires_at", 0.0) or 0.0)
    if cached and (expires_at - now) > 60:
        return cached
    auth = base64.b64encode(f"{PAYPAL_CLIENT_ID}:{PAYPAL_CLIENT_SECRET}".encode("utf-8")).decode("ascii")
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{_paypal_api_base()}/v1/oauth2/token",
            headers={
                "Authorization": f"Basic {auth}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            content="grant_type=client_credentials",
        )
    if resp.status_code >= 400:
        raise HTTPException(500, f"PayPal auth failed: {resp.text[:200]}")
    data = resp.json()
    token = str(data.get("access_token", "") or "")
    expires_in = int(data.get("expires_in", 0) or 0)
    if not token:
        raise HTTPException(500, "PayPal auth token missing")
    _paypal_token_cache["token"] = token
    _paypal_token_cache["expires_at"] = now + max(60, expires_in)
    return token


async def _paypal_request(method: str, path: str, *, json_body: dict | None = None) -> dict:
    token = await _paypal_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=45.0) as client:
        resp = await client.request(
            method.upper(),
            f"{_paypal_api_base()}{path}",
            headers=headers,
            json=json_body,
        )
    if resp.status_code >= 400:
        raise HTTPException(resp.status_code, f"PayPal request failed: {resp.text[:400]}")
    try:
        return resp.json()
    except Exception:
        return {}


def _normalize_art_style(value, template: str = "", default: str = "auto") -> str:
    if str(template or "").strip().lower() == "skeleton":
        return "auto"
    text = str(value or default).strip().lower()
    return text if text in ART_STYLE_PRESETS else default


def _art_style_prompt_fragment(art_style: str, template: str = "") -> str:
    style = _normalize_art_style(art_style, template=template, default="auto")
    return ART_STYLE_PRESETS.get(style, "")


def _looks_like_provider_moderation_error(message: str) -> bool:
    msg = str(message or "").strip().lower()
    if not msg:
        return False
    moderation_markers = (
        "safety",
        "moderation",
        "policy",
        "violence",
        "graphic",
        "disallowed",
        "not allowed",
        "blocked",
        "rejected",
        "refused",
        "content violation",
    )
    provider_markers = ("xai", "x.ai", "grok", "fal.ai", "fal", "provider")
    has_moderation = any(t in msg for t in moderation_markers)
    # Some providers return moderation rejections without explicit vendor labels.
    if has_moderation:
        return True
    return any(p in msg for p in provider_markers) and ("400" in msg or "422" in msg or "invalid_request_error" in msg)


def _prompt_likely_moderated(prompt: str) -> bool:
    text = str(prompt or "").strip().lower()
    if not text:
        return False
    risky_terms = (
        "fallen bodies",
        "dead body",
        "dead bodies",
        "lifeless",
        "corpse",
        "gore",
        "gory",
        "graphic injury",
        "blood-soaked",
    )
    return any(t in text for t in risky_terms)


def _soften_story_prompt_for_moderation(prompt: str, aggressive: bool = False) -> str:
    text = str(prompt or "")
    if not text:
        return text
    replacements = [
        (r"\bfallen bodies?\b", "devastating aftermath"),
        (r"\bdead bodies?\b", "chaotic aftermath"),
        (r"\bcorpse(s)?\b", "aftermath"),
        (r"\blifeless\b", "unresponsive"),
        (r"\bbloody\b", "tense"),
        (r"\bgore\b", "distress"),
        (r"\bgory\b", "intense"),
        (r"\bgraphic injury\b", "emotional impact"),
    ]
    if aggressive:
        replacements.extend([
            (r"\bdeath\b", "loss"),
            (r"\bkilled\b", "hurt"),
            (r"\bmurder(ed|ous)?\b", "harmful"),
            (r"\bviolence\b", "conflict"),
            (r"\bweapon(s)?\b", "threat"),
            (r"\bbleeding\b", "injured"),
        ])
    softened = text
    for pattern, repl in replacements:
        softened = re.sub(pattern, repl, softened, flags=re.IGNORECASE)
    if softened != text:
        suffix = " Keep the emotional gravity high, avoid explicit gore or graphic injury."
        if aggressive:
            suffix = " Keep the emotional gravity high with cinematic tension, but avoid explicit violence, death wording, gore, or injuries."
        softened += suffix
    return softened


def _decode_data_image_url(data_url: str) -> tuple[bytes, str]:
    text = str(data_url or "").strip()
    if not text.startswith("data:image/") or "," not in text:
        return b"", ""
    try:
        header, b64_data = text.split(",", 1)
        mime = "image/png"
        if ";" in header and ":" in header:
            mime = header.split(":", 1)[1].split(";", 1)[0].strip() or mime
        raw = base64.b64decode(b64_data, validate=False)
        return (raw if raw else b"", mime)
    except Exception:
        return b"", ""


def _longform_reference_file_public_url(filename: str) -> str:
    safe = os.path.basename(str(filename or "").strip())
    return f"{SITE_URL.rstrip('/')}/api/longform/reference-file/{safe}"


def _persist_longform_reference_image(
    session: dict,
    *,
    reference_image_url: str,
    reference_lock_mode: str = "strict",
) -> dict:
    session = dict(session or {})
    data_url = str(reference_image_url or "").strip()
    if not data_url:
        return session
    raw, mime = _decode_data_image_url(data_url)
    if not raw:
        raise HTTPException(400, "Reference image is empty or invalid")
    if len(raw) > 8 * 1024 * 1024:
        raise HTTPException(400, "Reference image must be <= 8MB")
    lock_mode = _normalize_reference_lock_mode(
        reference_lock_mode,
        default=_normalize_reference_lock_mode(session.get("reference_lock_mode"), "strict"),
    )
    quality = _analyze_reference_quality(raw, lock_mode=lock_mode)
    if not quality.get("accepted", True) and lock_mode == "strict":
        raise HTTPException(
            400,
            "Reference image quality too low for Strict Reference Lock. Upload a higher-resolution image or switch to Style Inspired mode.",
        )
    ext = ".png"
    if "jpeg" in mime or "jpg" in mime:
        ext = ".jpg"
    elif "webp" in mime:
        ext = ".webp"
    ref_dir = TEMP_DIR / "longform_references"
    ref_dir.mkdir(parents=True, exist_ok=True)
    session_id = str(session.get("session_id", "") or "").strip() or f"lf_ref_{int(time.time())}_{random.randint(1000, 9999)}"
    ref_name = f"{session_id}_reference{ext}"
    ref_path = ref_dir / ref_name
    ref_path.write_bytes(raw)
    public_url = _longform_reference_file_public_url(ref_name)
    template = str(session.get("template", "story") or "story").strip().lower() or "story"
    session["reference_image_url"] = data_url
    session["reference_image_path"] = str(ref_path)
    session["reference_image_public_url"] = public_url
    session["reference_lock_mode"] = lock_mode
    session["reference_quality"] = quality
    session["reference_dna"] = _extract_reference_dna(raw, template=template)
    session["reference_image_uploaded"] = True
    session["rolling_reference_image_url"] = public_url
    if template == "skeleton":
        session["skeleton_reference_image"] = public_url
    return session


def _longform_character_reference_public_view(raw_reference: dict) -> dict:
    ref = dict(raw_reference or {})
    return {
        "character_id": str(ref.get("character_id", "") or "").strip(),
        "name": str(ref.get("name", "") or "").strip(),
        "reference_image_public_url": str(ref.get("reference_image_public_url", "") or "").strip(),
        "reference_lock_mode": str(ref.get("reference_lock_mode", "strict") or "strict").strip() or "strict",
        "reference_quality": dict(ref.get("reference_quality") or {}),
        "created_at": float(ref.get("created_at", 0.0) or 0.0),
    }


def _persist_longform_character_reference(
    session: dict,
    *,
    name: str,
    reference_image_url: str,
    reference_lock_mode: str = "strict",
) -> tuple[dict, dict]:
    session = dict(session or {})
    character_name = _clip_text(str(name or "").strip(), 60)
    if not character_name:
        raise HTTPException(400, "Character name is required")
    data_url = str(reference_image_url or "").strip()
    if not data_url:
        raise HTTPException(400, "Reference image is empty or invalid")
    raw, mime = _decode_data_image_url(data_url)
    if not raw:
        raise HTTPException(400, "Reference image is empty or invalid")
    if len(raw) > 8 * 1024 * 1024:
        raise HTTPException(400, "Reference image must be <= 8MB")
    lock_mode = _normalize_reference_lock_mode(reference_lock_mode, default="strict")
    quality = _analyze_reference_quality(raw, lock_mode=lock_mode)
    if not quality.get("accepted", True) and lock_mode == "strict":
        raise HTTPException(
            400,
            "Character reference quality too low for Strict Reference Lock. Upload a higher-resolution image or switch to Style Inspired mode.",
        )
    ext = ".png"
    if "jpeg" in mime or "jpg" in mime:
        ext = ".jpg"
    elif "webp" in mime:
        ext = ".webp"
    ref_dir = TEMP_DIR / "longform_references"
    ref_dir.mkdir(parents=True, exist_ok=True)
    session_id = str(session.get("session_id", "") or "").strip() or f"lf_ref_{int(time.time())}_{random.randint(1000, 9999)}"
    character_id = f"lfc_{int(time.time())}_{random.randint(1000, 9999)}"
    ref_name = f"{session_id}_{character_id}{ext}"
    ref_path = ref_dir / ref_name
    ref_path.write_bytes(raw)
    public_url = _longform_reference_file_public_url(ref_name)
    template = str(session.get("template", "story") or "story").strip().lower() or "story"
    character_reference = {
        "character_id": character_id,
        "name": character_name,
        "reference_image_path": str(ref_path),
        "reference_image_public_url": public_url,
        "reference_lock_mode": lock_mode,
        "reference_quality": quality,
        "reference_dna": _extract_reference_dna(raw, template=template),
        "created_at": time.time(),
    }
    existing = [
        dict(item or {})
        for item in list(session.get("character_references") or [])
        if isinstance(item, dict) and str((item or {}).get("character_id", "") or "").strip()
    ]
    existing.append(character_reference)
    session["character_references"] = existing
    return session, character_reference


def _longform_scene_assigned_character_reference(session: dict, scene: dict) -> dict:
    refs = {
        str((item or {}).get("character_id", "") or "").strip(): dict(item or {})
        for item in list((dict(session or {})).get("character_references") or [])
        if isinstance(item, dict) and str((item or {}).get("character_id", "") or "").strip()
    }
    scene_payload = dict(scene or {})
    assigned_id = str(scene_payload.get("assigned_character_id", "") or "").strip()
    if not assigned_id:
        assigned_ids = [
            str(item or "").strip()
            for item in list(scene_payload.get("assigned_character_ids") or [])
            if str(item or "").strip()
        ]
        assigned_id = assigned_ids[0] if assigned_ids else ""
    return dict(refs.get(assigned_id) or {})


def _extract_reference_dna(raw: bytes, template: str = "skeleton") -> dict:
    dna = {
        "template": str(template or ""),
        "orientation": "portrait",
        "aspect_ratio": 0.562,
        "palette_family": "neutral_cool",
        "lighting_style": "balanced",
        "contrast_style": "cinematic",
        "sharpness_style": "clean",
        "width": 0,
        "height": 0,
    }
    if not raw or Image is None:
        return dna
    try:
        img = Image.open(io.BytesIO(raw)).convert("RGB")
        w, h = img.size
        if w > 0 and h > 0:
            ratio = float(w / h)
            dna["width"] = int(w)
            dna["height"] = int(h)
            dna["aspect_ratio"] = round(ratio, 4)
            if ratio < 0.85:
                dna["orientation"] = "portrait"
            elif ratio > 1.15:
                dna["orientation"] = "landscape"
            else:
                dna["orientation"] = "square"

        tiny = img.resize((128, max(1, int(128 * (img.height / max(img.width, 1))))), Image.BILINEAR)
        stat_rgb = ImageStat.Stat(tiny)
        mean_r, mean_g, mean_b = [float(v) for v in stat_rgb.mean[:3]]
        gray = tiny.convert("L")
        bright = float(ImageStat.Stat(gray).mean[0])
        contrast = float(ImageStat.Stat(gray).stddev[0])

        hsv_px = list(tiny.convert("HSV").getdata())
        sat_vals = [int(px[1]) for px in hsv_px]
        hue_vals = [int(px[0]) for px in hsv_px if int(px[1]) > 25]
        sat_avg = float(sum(sat_vals) / max(len(sat_vals), 1))
        hue_avg = float(sum(hue_vals) / max(len(hue_vals), 1)) if hue_vals else 0.0

        if ImageFilter is not None:
            edge = tiny.convert("L").filter(ImageFilter.FIND_EDGES)
            edge_strength = float(ImageStat.Stat(edge).mean[0])
        else:
            edge_strength = 0.0

        palette = "neutral_cool"
        if sat_avg < 45:
            palette = "desaturated"
        elif mean_r > mean_b + 8:
            palette = "warm"
        elif mean_b > mean_r + 8:
            palette = "cool"
        elif 18 <= hue_avg <= 45:
            palette = "warm"
        elif 95 <= hue_avg <= 145:
            palette = "cool"

        if bright < 70:
            lighting = "moody_lowkey"
        elif bright > 180:
            lighting = "highkey_bright"
        else:
            lighting = "balanced"

        if contrast < 28:
            contrast_style = "soft"
        elif contrast > 68:
            contrast_style = "punchy"
        else:
            contrast_style = "cinematic"

        sharpness_style = "clean" if edge_strength >= 24 else "soft"
        dna.update({
            "palette_family": palette,
            "lighting_style": lighting,
            "contrast_style": contrast_style,
            "sharpness_style": sharpness_style,
            "avg_brightness": round(bright, 2),
            "avg_contrast": round(contrast, 2),
            "avg_saturation": round(sat_avg, 2),
            "edge_strength": round(edge_strength, 2),
        })
    except Exception:
        return dna
    return dna


def _analyze_reference_quality(raw: bytes, lock_mode: str = "strict") -> dict:
    out = {"accepted": True, "warnings": [], "metrics": {}}
    if not raw:
        return {"accepted": False, "warnings": ["empty_image"], "metrics": {}}
    size_kb = len(raw) / 1024.0
    out["metrics"]["size_kb"] = round(size_kb, 1)
    if size_kb < 40:
        out["warnings"].append("very_small_file")

    dims = {"width": 0, "height": 0, "aspect_ratio": 0.0}
    if Image is not None:
        try:
            img = Image.open(io.BytesIO(raw))
            w, h = img.size
            dims = {"width": int(w), "height": int(h), "aspect_ratio": round(float(w / max(h, 1)), 4)}
            if min(w, h) < 768:
                out["warnings"].append("low_resolution_reference")
            ratio = float(w / max(h, 1))
            if ratio < 0.45 or ratio > 0.8:
                out["warnings"].append("non_portrait_aspect")
        except Exception:
            out["warnings"].append("unreadable_image")
    out["metrics"].update(dims)

    fatal = {"empty_image", "unreadable_image"}
    if "low_resolution_reference" in out["warnings"] and lock_mode == "strict":
        out["accepted"] = False
    if any(w in fatal for w in out["warnings"]):
        out["accepted"] = False
    return out


def _reference_dna_prompt_fragment(reference_dna: dict | None, lock_mode: str, template: str) -> str:
    dna = reference_dna or {}
    if not dna:
        return ""
    strength = "hard lock" if lock_mode == "strict" else "soft style guidance"
    return (
        "REFERENCE DNA (" + strength + "): "
        + "orientation=" + str(dna.get("orientation", "portrait")) + ", "
        + "palette=" + str(dna.get("palette_family", "neutral")) + ", "
        + "lighting=" + str(dna.get("lighting_style", "balanced")) + ", "
        + "contrast=" + str(dna.get("contrast_style", "cinematic")) + ", "
        + "sharpness=" + str(dna.get("sharpness_style", "clean")) + ". "
        + ("Maintain exact identity/anatomy continuity scene-to-scene. " if lock_mode == "strict" else "Preserve style while allowing pose/composition variety. ")
        + (
            "For skeleton keep skull geometry, eye size/spacing, bone finish, and a clearly visible translucent body silhouette unchanged."
            if template == "skeleton"
            else (
                "For story keep recurring subjects, key locations, and grade continuity consistent when the script indicates recurrence."
                if template == "story"
                else "Keep subject styling and grading consistent across scenes."
            )
        )
    )


def _canonical_skeleton_anchor() -> str:
    return (
        "CONSISTENCY ANCHOR -- use one unchanged canonical skeleton identity in every scene: "
        "ivory-white anatomical skeleton, large realistic eyeballs with visible iris, "
        "clearly visible translucent soft-tissue silhouette around torso/limbs, identical skull proportions and bone structure, "
        "preserve anatomy and the translucent body shell even when the scene requests role-specific props or wardrobe. "
    )


def _skeleton_has_explicit_outfit_request(text: str) -> bool:
    raw = str(text or "").strip().lower()
    if not raw:
        return False
    if re.search(r"\b(no clothing|shirtless|nude|naked|bare(?:\s+body)?|no outfit)\b", raw):
        return False
    return bool(
        re.search(
            r"\b("
            r"spacesuit|space suit|astronaut|helmet|visor|chef|chef hat|apron|hoodie|jacket|coat|robe|uniform|armor|costume|"
            r"suit|tuxedo|dress|shirt|pants|gloves|boots|scrubs|jersey|cloak|cape|mask|gown|lab coat"
            r")\b",
            raw,
        )
    )


def _skeleton_outfit_coverage_lock(text: str) -> str:
    if not _skeleton_has_explicit_outfit_request(text):
        return ""
    return (
        "WARDROBE LOCK: keep the requested outfit or uniform physically worn over the same canonical Jerry-style skeleton. "
        "The clothing must enclose the torso, pelvis, shoulders, arms, and legs wherever the outfit covers them. "
        "Do not leave the ribcage, spine, pelvis, or limb bones visibly exposed outside the clothing, and do not render transparent clothes that reveal the full skeleton body underneath. "
        "The glass-like body shell and same eyes still exist, but the body should look properly dressed inside the outfit rather than naked bones wearing accessories."
    )


_SKELETON_NAMED_SUBJECT_CONNECTORS = {
    "the", "de", "da", "del", "la", "le", "van", "von", "bin", "ibn", "st", "st.",
}
_SKELETON_NAMED_SUBJECT_NONPERSON_SUFFIXES = {
    "media", "science", "studio", "studios", "channel", "channels", "clips", "clip", "group",
    "company", "companies", "agency", "department", "government", "state", "states", "university",
    "college", "hospital", "network", "records", "podcast", "podcasts", "team", "teams", "court",
    "courts", "project", "projects", "app", "apps", "api",
}
_SKELETON_NAMED_SUBJECT_NONPERSON_TOKENS = {
    "a", "an", "the", "this", "that", "these", "those", "my", "your", "our",
    "january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december",
    "empire", "magnates", "cryptic", "science", "nyptid", "clips", "clip", "studio", "studios", "channel", "channels",
    "youtube", "google", "cloud", "algrow", "api", "flow", "fal", "grok", "argument", "earth", "system", "systems",
    "engine", "doctrine", "business", "psychology", "recap", "recaps", "manhwa", "manga", "anime", "video", "videos",
    "chapter", "chapters", "scene", "scenes", "upload", "uploads", "project", "case", "latest", "recent", "active",
    "request", "details", "federal", "consumer", "oauth", "consumer", "billing", "dashboard", "membership",
}
_SKELETON_NAMED_SUBJECT_NONPERSON_PHRASES = {
    "empire magnates",
    "cryptic science",
    "nyptid clips",
    "magnates media",
    "flat earth",
    "youtube studio",
    "google cloud",
    "algrow api",
    "my first project",
    "internet anarchist",
}
_SKELETON_HUMAN_FACE_NEGATIVE_TOKENS = {
    "skin",
    "flesh",
    "muscles",
    "human face",
    "realistic person",
}
_SKELETON_EXTRA_PERSON_NEGATIVE_TOKENS = {
    "extra person",
}


def _extract_likely_named_human_subjects(*texts: str, max_items: int = 3) -> list[str]:
    pattern = re.compile(
        r"\b[A-Z][A-Za-z0-9'’.-]*(?:\s+(?:[A-Z][A-Za-z0-9'’.-]*|the|The|de|De|da|Da|del|Del|la|La|le|Le|van|Van|von|Von|bin|Bin|ibn|Ibn|st\.?|St\.?)){1,3}\b"
    )
    seen: set[str] = set()
    out: list[str] = []
    for text in texts:
        raw = re.sub(r"\s+", " ", str(text or "")).strip()
        if not raw:
            continue
        for match in pattern.finditer(raw):
            candidate = re.sub(r"\s+", " ", str(match.group(0) or "")).strip(" ,.;:!?()[]{}\"'")
            if not _is_likely_named_human_subject(candidate):
                continue
            key = candidate.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(candidate)
            if len(out) >= max(1, int(max_items or 1)):
                return out
    return out


def _is_likely_named_human_subject(candidate: str) -> bool:
    cleaned = re.sub(r"\s+", " ", str(candidate or "")).strip(" ,.;:!?()[]{}\"'")
    if not cleaned:
        return False
    lowered = cleaned.lower()
    if lowered in _SKELETON_NAMED_SUBJECT_NONPERSON_PHRASES:
        return False
    words = [
        re.sub(r"(^[^A-Za-z0-9]+|[^A-Za-z0-9.]+$)", "", part)
        for part in cleaned.split()
    ]
    words = [part for part in words if part]
    if len(words) < 2 or len(words) > 4:
        return False
    if words[0].lower().rstrip(".") in _SKELETON_NAMED_SUBJECT_NONPERSON_TOKENS:
        return False
    last = words[-1].lower().rstrip(".")
    if last in _SKELETON_NAMED_SUBJECT_NONPERSON_SUFFIXES:
        return False
    meaningful = 0
    titlelike = 0
    for word in words:
        lower = word.lower().rstrip(".")
        if lower in _SKELETON_NAMED_SUBJECT_CONNECTORS:
            continue
        meaningful += 1
        if lower in _SKELETON_NAMED_SUBJECT_NONPERSON_TOKENS:
            return False
        if re.match(r"^[A-Z][A-Za-z0-9'’.-]*$", word) or re.match(r"^[A-Z]{2,}$", word):
            titlelike += 1
            continue
        return False
    return meaningful >= 2 and titlelike >= 2


def _human_subject_list(subjects: list[str]) -> str:
    values = [str(subject or "").strip() for subject in subjects if str(subject or "").strip()]
    if not values:
        return ""
    if len(values) == 1:
        return values[0]
    if len(values) == 2:
        return f"{values[0]} and {values[1]}"
    return ", ".join(values[:-1]) + f", and {values[-1]}"


def _named_human_subject_likeness_lock(*texts: str, skeleton_mode: bool = False) -> str:
    subjects = _extract_likely_named_human_subjects(*texts, max_items=3)
    if not subjects:
        return ""
    subject_list = _human_subject_list(subjects)
    lock = (
        f"NAMED HUMAN SUBJECT LOCK: if the scene includes {subject_list}, render each named person as a recognizable photoreal human with accurate face structure, "
        "skin tone, age cues, body build, hair or facial hair, tattoos, and signature styling. "
        f"Do not replace {subject_list} with a generic lookalike, anonymous stock person, mannequin, or a different public figure."
    )
    if skeleton_mode:
        lock += " Keep the canonical skeleton separate and unchanged; never humanize the skeleton itself."
    return lock


def _longform_named_human_priority_lock(
    *texts: str,
    template: str = "",
    archetype_key: str = "",
) -> str:
    subjects = _extract_likely_named_human_subjects(*texts, max_items=3)
    if not subjects:
        return ""
    subject_list = _human_subject_list(subjects)
    archetype = str(archetype_key or "").strip().lower()
    if archetype == "crime_documentary":
        scene_rule = (
            "Keep the named human as the dominant focal subject in a premium medium shot, close-up, or over-shoulder evidence interaction. "
            "Evidence boards, maps, monitors, phones, and paperwork must support the person, not replace them."
        )
    elif archetype == "psychology_documentary":
        scene_rule = (
            "Keep the named human as the dominant focal subject in a premium close-up or human-pressure interaction. "
            "Rooms, symbols, and props must stay secondary to readable face, posture, and consequence."
        )
    else:
        scene_rule = (
            "Keep the named human as the dominant focal subject instead of generic rooms, props, or abstract system diagrams."
        )
    skeleton_rule = ""
    if str(template or "").strip().lower() == "skeleton":
        skeleton_rule = (
            " If the canonical skeleton host appears, keep it secondary, brief, and clearly separate; omit it entirely when the beat is about the real person."
        )
    return (
        f"REAL HUMAN PRIORITY: if {subject_list} appears in this beat, render {subject_list} as the main subject with accurate face structure, skin tone, age cues, body build, "
        "hair or facial hair, tattoos, and signature styling. "
        f"{scene_rule}{skeleton_rule} "
        "No mannequin faces, no anonymous stock-person substitutions, no waxy skin, and no deformed eyes or hands."
    ).strip()


def _skeleton_scene_supporting_humans_requested(*texts: str) -> bool:
    return bool(_extract_likely_named_human_subjects(*texts, max_items=1))


def _filter_negative_prompt_entries(negative_text: str, blocked_entries: set[str]) -> str:
    parts = [part.strip() for part in str(negative_text or "").split(",") if part and part.strip()]
    if not parts:
        return ""
    blocked = {re.sub(r"\s+", " ", str(entry or "").strip().lower()) for entry in blocked_entries if str(entry or "").strip()}
    out: list[str] = []
    for part in parts:
        key = re.sub(r"\s+", " ", part.strip().lower())
        if key in blocked:
            continue
        out.append(part)
    return ", ".join(out).strip(", ")


def _skeleton_named_human_negative_adjustment(negative_text: str, prompt: str, allow_extra_people: bool = False) -> str:
    neg = str(negative_text or "").strip()
    if not neg or not _skeleton_scene_supporting_humans_requested(prompt):
        return neg
    blocked = set(_SKELETON_HUMAN_FACE_NEGATIVE_TOKENS)
    if allow_extra_people:
        blocked |= _SKELETON_EXTRA_PERSON_NEGATIVE_TOKENS
    filtered = _filter_negative_prompt_entries(neg, blocked)
    mismatch_tokens = [
        "wrong named person identity",
        "generic anonymous face",
        "unrecognizable human face",
        "mismatched hairstyle",
        "mismatched facial hair",
        "wrong tattoos",
    ]
    return ", ".join(part for part in [filtered, ", ".join(mismatch_tokens)] if part).strip(", ")


def _skeleton_image_suffix_for_scene(scene_text: str) -> str:
    suffix = str(SKELETON_IMAGE_SUFFIX or "").strip()
    if not _skeleton_scene_supporting_humans_requested(scene_text):
        return suffix
    suffix = suffix.replace(
        "A clearly visible translucent soft-tissue silhouette around torso/limbs is REQUIRED in every scene, but no full human skin face. ",
        "A clearly visible translucent soft-tissue silhouette around torso/limbs is REQUIRED on the canonical skeleton in every scene. Explicitly named supporting humans may keep natural human faces. ",
    )
    return (
        f"{suffix} Supporting human rule: any explicitly named real person must keep accurate facial structure, skin tone, age cues, "
        "hair or facial hair, body build, tattoos, and signature styling."
    ).strip()


def _build_scene_prompt_with_reference(
    template: str,
    visual_description: str,
    quality_mode: str = "standard",
    skeleton_anchor: str = "",
    reference_dna: dict | None = None,
    reference_lock_mode: str = "strict",
    art_style: str = "auto",
) -> str:
    delta = str(visual_description or "").strip()
    immutable = _reference_dna_prompt_fragment(reference_dna, _normalize_reference_lock_mode(reference_lock_mode), template)
    style_fragment = _art_style_prompt_fragment(art_style, template=template)
    immutable_blocks = [block for block in [immutable, style_fragment] if block]
    immutable_context = " ".join(immutable_blocks).strip()
    if template == "skeleton":
        return _build_skeleton_image_prompt(
            delta,
            skeleton_anchor=skeleton_anchor,
            quality_mode=quality_mode,
            immutable_context=immutable_context,
        )
    if template == "story":
        return _build_story_image_prompt(
            delta,
            quality_mode=quality_mode,
            immutable_context=immutable_context,
        )
    prefix = TEMPLATE_PROMPT_PREFIXES.get(template, "")
    if immutable_context:
        return f"{prefix} IMMUTABLE STYLE: {immutable_context} SCENE DELTA: {delta}"
    return prefix + delta


def _build_skeleton_identity_passthrough_prompt(
    visual_description: str,
    reference_dna: dict | None = None,
) -> str:
    delta = _sanitize_skeleton_scene_delta(str(visual_description or "").strip())
    if not delta:
        return (
            "Show the canonical Jerry-style NYPTID skeleton first: ivory-white anatomical skeleton, "
            "large realistic human-like eyeballs clearly visible in both eye sockets with readable iris and pupil, "
            "and a continuous translucent glass-like skin shell clearly visible over the full body, hugging the skull, face, neck, torso, arms, hands, and legs like real transparent skin."
        )
    internal_focus = _skeleton_scene_prefers_internal_cutaway(delta)
    ref = reference_dna or {}
    ref_bits: list[str] = []
    lighting = str(ref.get("lighting_style", "") or "").strip()
    contrast = str(ref.get("contrast_style", "") or "").strip()
    if lighting or contrast:
        ref_bits.append(
            "Keep the default Jerry reference grade: "
            + " ".join(part for part in [lighting, contrast] if part).strip()
            + "."
        )
    outfit_lock = _skeleton_outfit_coverage_lock(delta)
    named_human_lock = _named_human_subject_likeness_lock(delta, skeleton_mode=True)
    anchor = (
        "Always show the canonical Jerry-style NYPTID skeleton first and keep visible skeleton anatomy in frame: "
        "ivory-white anatomical skeleton, large realistic human-like eyeballs clearly visible in both eye sockets with readable iris and pupil, "
        "never empty sockets, never missing eyes, continuous translucent glass-like skin shell clearly visible over the full body, "
        "hugging the skull, face, neck, torso, arms, hands, and legs like real transparent skin, never bare bones-only look, never faint shell, never x-ray look."
    )
    if internal_focus:
        bridge = (
            "Render the requested microscopic or internal subject as a zoom-in / cutaway happening inside the skeleton's "
            "arm, torso, blood, or tissue while some canonical skeleton anatomy remains visible in frame."
        )
    else:
        bridge = "The skeleton remains the main on-screen subject while following the user's scene request."
    return " ".join([
        anchor,
        bridge,
        _skeleton_scene_context_lock(delta),
        _skeleton_scene_framing_lock(delta),
        outfit_lock,
        named_human_lock,
        f"USER SCENE REQUEST: {delta}",
        *ref_bits,
    ]).strip()


def _build_creative_passthrough_scene_prompt(
    template: str,
    visual_description: str,
    quality_mode: str = "standard",
    skeleton_anchor: str = "",
    reference_dna: dict | None = None,
    reference_lock_mode: str = "strict",
    art_style: str = "auto",
) -> str:
    # Passthrough must honor the user's prompt. Skeleton is the only exception:
    # it keeps a minimal identity anchor so the subject never stops being Jerry-style skeleton.
    if str(template or "").strip().lower() == "skeleton":
        return _build_skeleton_identity_passthrough_prompt(
            visual_description,
            reference_dna=reference_dna,
        )
    return str(visual_description or "").strip()


def _augment_skeleton_negative_prompt(base_negative: str, prompt: str) -> str:
    text = str(prompt or "").strip().lower()
    if not text:
        return str(base_negative or "").strip()
    extras: list[str] = []
    has_brain = bool(re.search(r"\bbrain\b", text))
    has_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", text))
    has_table = bool(re.search(r"\btable|desk|countertop\b", text))
    has_damage_cues = bool(re.search(r"\b(crack|cracks|fracture|fractured|chip|chipped|bruise|bruises|damaged|damage)\b", text))
    has_tired_cues = bool(re.search(r"\b(tired|fatigued|weary|slouch|slouched|hunch|hunched|droop|drooping|exhausted)\b", text))
    if has_brain:
        extras.extend([
            "smooth sphere as brain replacement",
            "fruit instead of brain",
            "brain without visible folds",
        ])
    if has_money:
        extras.extend([
            "coins-only replacing money pile",
            "blank paper replacing banknotes",
            "missing cash stack",
        ])
    if has_brain and has_money:
        extras.extend([
            "extra random glowing orb replacing required props",
            "missing one of the two required props",
        ])
    if has_table:
        extras.extend([
            "standing pose with no table",
            "missing tabletop surface",
            "floating props with no table context",
            "arched portal behind skeleton",
            "backlit doorway behind skeleton",
            "altar pedestal replacing table",
            "props on floor instead of table",
            "monocle",
            "eye patch",
            "missing one eye",
        ])
    if has_damage_cues:
        extras.extend([
            "pristine undamaged bones",
            "perfectly clean intact skeleton",
            "no cracks on bones",
            "no bruising marks",
        ])
    if has_tired_cues:
        extras.extend([
            "upright heroic posture",
            "confident energetic stance",
        ])
    parts = [str(base_negative or "").strip()] if str(base_negative or "").strip() else []
    if extras:
        parts.append(", ".join(dict.fromkeys(extras)))
    merged = ", ".join([p for p in parts if p]).strip(", ")
    return _skeleton_named_human_negative_adjustment(merged, prompt, allow_extra_people=True)


def _relax_skeleton_negative_prompt_for_passthrough(base_negative: str, prompt: str) -> str:
    text = str(prompt or "").strip().lower()
    neg = str(base_negative or "").strip()
    if not neg:
        return neg
    wants_damage = bool(re.search(r"\b(crack|cracks|fracture|fractured|chip|chipped|bruise|bruises|damaged|damage)\b", text))
    wants_tired = bool(re.search(r"\b(tired|fatigued|weary|slouch|slouched|hunch|hunched|droop|drooping|exhausted)\b", text))
    parts = [p.strip() for p in neg.split(",") if p and p.strip()]
    out: list[str] = []
    for part in parts:
        pl = part.lower()
        if wants_damage and ("broken bones" in pl or "dislocated joints" in pl):
            continue
        if wants_tired and ("unnatural pose" in pl or "mannequin" in pl):
            continue
        if _skeleton_scene_supporting_humans_requested(prompt):
            if pl in _SKELETON_HUMAN_FACE_NEGATIVE_TOKENS or pl in _SKELETON_EXTRA_PERSON_NEGATIVE_TOKENS:
                continue
        out.append(part)
    merged = ", ".join(out).strip(", ")
    return _skeleton_named_human_negative_adjustment(merged, prompt, allow_extra_people=True)


def _truncate_words(text: str, max_words: int = 120) -> str:
    raw = re.sub(r"\s+", " ", str(text or "")).strip()
    if not raw:
        return ""
    words = raw.split(" ")
    if len(words) <= max_words:
        return raw
    return " ".join(words[:max_words]).strip()


def _shortform_delivery_hints(text: str, template: str = "") -> list[str]:
    raw = re.sub(r"\s+", " ", str(text or "")).strip().lower()
    if not raw:
        return []
    hints: list[str] = []
    if re.search(r"\b(shorts?|short-form|short form|thumbnail|thumbnails?|viral short|reel|reels)\b", raw):
        hints.extend([
            "vertical 9:16 short-form composition",
            "thumbnail-readable centered subject",
            "clean uncluttered background with large readable props",
        ])
    if re.search(r"\bmust\s+be\s+able\s+to\s+work\b|\bno\s+matter\s+what\b", raw):
        hints.append("high-clarity production framing with reliable readable composition")
    if re.search(r"\b(create|generate)\s+images?\s+for\s+shorts?\b", raw):
        hints.append("strong subject separation and prop readability for short-form cover frames")
    if str(template or "").strip().lower() == "skeleton" and hints:
        hints.append("keep skull, both eyes, and upper torso instantly readable at first glance")
    seen: set[str] = set()
    out: list[str] = []
    for hint in hints:
        key = hint.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(hint.strip())
    return out


def _sanitize_skeleton_scene_delta(text: str) -> str:
    """Remove instructions that conflict with canonical Skeleton identity."""
    raw = re.sub(r"\s+", " ", str(text or "")).strip()
    if not raw:
        return ""
    cleaned = raw
    nonvisual_patterns = [
        r"\bit\s+must\s+be\s+able\s+to\s+work\s+and\s+create\s+images?\s+for\s+shorts?\b",
        r"\bmust\s+be\s+able\s+to\s+work\s+and\s+create\s+images?\s+for\s+shorts?\b",
        r"\bmust\s+be\s+able\s+to\s+work\b",
        r"\b(?:create|generate)\s+images?\s+for\s+shorts?\b",
        r"\bno\s+matter\s+what\b",
    ]
    for pat in nonvisual_patterns:
        cleaned = re.sub(pat, " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(a|an|the)\s+sits\b", "sits", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(a|an|the)\s+is\b", "is", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.")
    return cleaned or raw


def _skeleton_scene_prefers_internal_cutaway(text: str) -> bool:
    raw = str(text or "").strip().lower()
    if not raw:
        return False
    internal_markers = (
        "cell", "cells", "immune", "fever", "virus", "bacteria", "bloodstream", "blood vessel", "blood vessels", "pyrogen", "pyrogens",
        "hypothalamus", "neuron", "brain chemistry", "organ", "microscopic", "microshot",
        "macro shot", "close-up of tissue", "inside the body", "inside body", "inside the skeleton",
        "cutaway", "cross-section", "cross section", "blood", "tissue", "artery", "vein",
    )
    return any(marker in raw for marker in internal_markers)


def _skeleton_scene_requests_minimal_background(text: str) -> bool:
    raw = str(text or "").strip().lower()
    if not raw:
        return False
    return bool(re.search(
        r"\b(plain background|blank background|white background|black background|solid background|"
        r"minimal background|minimalist background|empty background|void background|studio seamless|"
        r"seamless backdrop|clean backdrop|isolated cutout|isolated on white|featureless backdrop)\b",
        raw,
    ))


def _skeleton_scene_has_environment_cue(text: str) -> bool:
    raw = str(text or "").strip().lower()
    if not raw:
        return False
    return bool(re.search(
        r"\b(environment|background|setting|location|interior|exterior|room|office|lab|laboratory|street|city|"
        r"arena|stadium|courtroom|hospital|factory|warehouse|classroom|garage|workshop|home|kitchen|bedroom|"
        r"rooftop|subway|train|airplane|plane|ship|boat|spaceship|space|planet|forest|desert|jungle|beach|"
        r"ocean|underwater|mountain|cave|temple|castle|battlefield|store|market|restaurant|bar|stage|studio|"
        r"table|desk|countertop|inside|within|cutaway|macro|microscopic|bloodstream|organ|cell|tissue)\b",
        raw,
    ))


def _skeleton_scene_has_camera_cue(text: str) -> bool:
    raw = str(text or "").strip().lower()
    if not raw:
        return False
    return bool(re.search(
        r"\b(close[- ]?up|closeup|macro|wide(?: shot)?|establishing|over[- ]?shoulder|over the shoulder|"
        r"low[- ]?angle|high[- ]?angle|bird'?s[- ]?eye|top[- ]?down|profile|silhouette|medium shot|mid[- ]?shot|"
        r"three[- ]?quarter|3/4|waist[- ]?up|full[- ]?body|full body|portrait|hero framing|framing|camera|lens|"
        r"dolly|push[- ]?in|tracking|pov|point of view)\b",
        raw,
    ))


def _skeleton_scene_context_lock(text: str) -> str:
    raw = str(text or "").strip()
    if _skeleton_scene_requests_minimal_background(raw):
        return (
            "BACKGROUND/STAGING LOCK: honor the explicitly requested minimal or studio backdrop, "
            "but make it look intentional, premium, and fully lit rather than empty or unfinished."
        )
    if _skeleton_scene_prefers_internal_cutaway(raw):
        return (
            "BACKGROUND/STAGING LOCK: use a readable internal, anatomical, or microscopic environment with "
            "clear contextual structures and layered depth; never reduce the beat to a plain void."
        )
    return (
        "BACKGROUND/STAGING LOCK: place the skeleton inside a rich topic-specific environment with clear "
        "location cues, layered foreground/midground/background depth, and contextual objects; never leave "
        "the subject on a blank, plain, or empty backdrop."
    )


def _skeleton_scene_framing_lock(text: str) -> str:
    raw = str(text or "").strip()
    if _skeleton_scene_prefers_internal_cutaway(raw):
        return (
            "SHOT VARIETY LOCK: if the beat is internal or microscopic, use close cutaway or macro framing "
            "while keeping some canonical skeleton anatomy readable in frame."
        )
    return (
        "SHOT VARIETY LOCK: choose framing that best communicates the beat instead of repeating the same "
        "centered medium hero shot. Use wide environmental, medium action, low-angle, over-shoulder, "
        "prop-detail, or full-body movement framing as appropriate."
    )


def _extract_scene_content_lock(prompt: str) -> str:
    raw = str(prompt or "")
    m = re.search(
        r"SCENE CONTENT LOCK\s*\(must be visible and preserved\):\s*(.*?)\s*SCENE PRIORITY RULE:",
        raw,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m:
        return re.sub(r"\s+", " ", str(m.group(1) or "")).strip(" .")
    return ""


def _compact_skeleton_local_prompt(prompt: str) -> str:
    raw = re.sub(r"\s+", " ", str(prompt or "")).strip()
    if not raw:
        return raw
    delivery_hints = _shortform_delivery_hints(raw, template="skeleton")
    scene = _sanitize_skeleton_scene_delta(_extract_scene_content_lock(raw) or raw)
    scene = re.sub(
        r"keep every explicitly requested prop/object/action visible and readable in frame\.?",
        "",
        scene,
        flags=re.IGNORECASE,
    )
    scene = re.sub(r"\s+", " ", scene).strip(" .")
    scene_l = scene.lower()
    needs_table = bool(re.search(r"\b(table|desk|countertop)\b", scene_l))
    needs_brain = "brain" in scene_l
    needs_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", scene_l))
    needs_glow = bool(re.search(r"\b(glow|glowing|emissive|luminous|light[- ]?emitting)\b", scene_l))
    internal_focus = _skeleton_scene_prefers_internal_cutaway(scene)
    explicit_outfit_request = _skeleton_has_explicit_outfit_request(scene)
    named_human_lock = _named_human_subject_likeness_lock(scene, skeleton_mode=True)
    parts: list[str] = [
        "photoreal cinematic 3D render",
        "canonical ivory-white anatomical skeleton",
        "both eyes visible with realistic iris reflections",
        "clearly visible translucent glass-like body shell",
        "natural bone color with neutral grading, not x-ray not radiograph",
    ]
    parts.append("requested outfit is fully worn and covers the body correctly" if explicit_outfit_request else "no clothing no costume no armor")
    if re.search(r"\bdark\b|\bnight\b|\blow[- ]?light\b", scene_l):
        parts.append("dark moody room lighting")
    prompt_parts: list[str] = [", ".join(parts) + "."]
    prompt_parts.append(f"Scene: {_truncate_words(scene, 56)}.")
    if explicit_outfit_request:
        prompt_parts.append(_skeleton_outfit_coverage_lock(scene))
    if needs_table:
        prompt_parts.append(
            "Skeleton is seated behind a real table and the tabletop is clearly visible across the lower foreground. "
            "Both forearms or hands rest on or directly above the tabletop."
        )
    if needs_brain and needs_money:
        glow = "glowing " if needs_glow else ""
        prompt_parts.append(
            f"On the table are exactly two {glow}props: one realistic human brain with visible gyri/sulci folds and one pile/stack of paper cash banknotes, both clearly visible and not replaced by spheres."
        )
    elif needs_brain:
        glow = "glowing " if needs_glow else ""
        prompt_parts.append(
            f"Show one {glow}realistic human brain with visible gyri/sulci folds (never a smooth sphere)."
        )
    elif needs_money:
        glow = "glowing " if needs_glow else ""
        prompt_parts.append(
            f"Show one {glow}pile/stack of paper cash banknotes clearly visible {'on the table' if needs_table else 'near the skeleton in the environment'}."
        )
    prompt_parts.append(_skeleton_scene_context_lock(scene))
    prompt_parts.append(
        _skeleton_scene_framing_lock(scene)
        if not internal_focus
        else "SHOT VARIETY LOCK: close cutaway or macro framing is allowed here, but some canonical skeleton anatomy must remain readable in frame."
    )
    prompt_parts.append("Ultra sharp focus, high contrast, readable props, no text, no watermark.")
    prompt_parts.append(
        "Glass-shell lock: transparent glass-like skin hugs the skull, torso, arms, and legs tightly like a real outer body shell, "
        "not a halo, not a bubble, not a portal, and not a glowing arch behind the subject."
    )
    if named_human_lock:
        prompt_parts.append(named_human_lock)
    if delivery_hints:
        prompt_parts.append("Short-form lock: " + "; ".join(delivery_hints) + ".")
    return _truncate_words(" ".join(prompt_parts), 150)


def _compact_skeleton_prop_first_prompt(prompt: str) -> str:
    raw = re.sub(r"\s+", " ", str(prompt or "")).strip()
    scene = _sanitize_skeleton_scene_delta(_extract_scene_content_lock(raw) or raw)
    scene_l = scene.lower()
    needs_table = bool(re.search(r"\b(table|desk|countertop)\b", scene_l))
    needs_brain = "brain" in scene_l
    needs_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", scene_l))
    needs_glow = bool(re.search(r"\b(glow|glowing|emissive|luminous|light[- ]?emitting)\b", scene_l))
    dark_room = bool(re.search(r"\b(dark|night|shadowy|moody|low[- ]?light)\b", scene_l))
    props = []
    if needs_brain:
        props.append(
            ("glowing " if needs_glow else "")
            + "human brain with visible gyri and sulci folds"
        )
    if needs_money:
        props.append(
            ("glowing " if needs_glow else "")
            + "pile of paper cash banknotes"
        )
    props_text = " and ".join(props) if props else "scene props"
    placement = "on the table" if needs_table else "in front of the skeleton"
    explicit_outfit_request = _skeleton_has_explicit_outfit_request(scene)
    named_human_lock = _named_human_subject_likeness_lock(scene, skeleton_mode=True)
    base_parts = [
        "photoreal 3D cinematic render",
        "transparent-glass anatomical skeleton with both eyes visible",
        "natural ivory bone color, no x-ray or radiograph look",
        "glass shell wraps tightly around skull torso arms and legs like transparent skin, never a halo or portal",
        ("dark moody environment" if dark_room else "premium cinematic lighting"),
    ]
    prompt_parts = [", ".join(base_parts) + "."]
    if needs_table:
        prompt_parts.append(
            "Skeleton is seated behind a real table with the tabletop clearly visible across the lower foreground and both hands near or on the tabletop."
        )
    else:
        prompt_parts.append("Skeleton interacts with the requested props inside a readable topic-matched environment.")
    prompt_parts.append(f"Mandatory props: {props_text} {placement}.")
    if needs_table:
        prompt_parts.append("Keep the required props large and obvious on the tabletop foreground. Do not omit either prop.")
    else:
        prompt_parts.append("Keep the required props large, obvious, and readable near the skeleton in the environment. Do not omit either prop.")
    prompt_parts.append("Do not replace required props with spheres, balls, or generic abstract objects.")
    if explicit_outfit_request:
        prompt_parts.append(_skeleton_outfit_coverage_lock(scene))
    prompt_parts.append(_skeleton_scene_context_lock(scene))
    prompt_parts.append(_skeleton_scene_framing_lock(scene))
    if named_human_lock:
        prompt_parts.append(named_human_lock)
    prompt_parts.append("Sharp focus, readable skull and prop detail, no text, no watermark.")
    return _truncate_words(" ".join(prompt_parts), 120)


def _extract_skeleton_scene_delta_for_fast_path(prompt: str) -> str:
    raw = re.sub(r"\s+", " ", str(prompt or "")).strip()
    if not raw:
        return ""
    scene = _extract_scene_content_lock(raw)
    if scene:
        return _sanitize_skeleton_scene_delta(scene)
    match = re.search(
        r"Scene:\s*(.*?)(?:\s+(?:Skeleton is seated|Show exactly|Glass-shell lock:|Short-form lock:|Match the reference skeleton anatomy exactly:)|$)",
        raw,
        flags=re.IGNORECASE,
    )
    if match:
        return _sanitize_skeleton_scene_delta(match.group(1))
    return _sanitize_skeleton_scene_delta(raw)


def _build_skeleton_lora_fast_prompt(prompt: str) -> str:
    scene = _extract_skeleton_scene_delta_for_fast_path(prompt)
    scene_l = scene.lower()
    needs_table = bool(re.search(r"\b(table|desk|countertop)\b", scene_l))
    needs_brain = "brain" in scene_l
    needs_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", scene_l))
    needs_glow = bool(re.search(r"\b(glow|glowing|emissive|luminous|light[- ]?emitting)\b", scene_l))
    dark_room = bool(re.search(r"\b(dark|night|shadowy|moody|low[- ]?light)\b", scene_l))
    explicit_outfit_request = _skeleton_has_explicit_outfit_request(scene)
    named_human_lock = _named_human_subject_likeness_lock(scene, skeleton_mode=True)
    prompt_parts = [
        "Single skeleton subject only.",
        ("Dark moody environment with readable background detail." if dark_room else "Detailed topic-matched environment with layered background depth."),
        "Photoreal 3D render.",
        "Anatomical skeleton with large realistic eyes and transparent glass skin tightly wrapped around the skull, torso, arms, and legs.",
        (
            "No second skeleton. Additional human subjects are allowed only when they are explicitly named in the scene, and those named people must stay recognizable."
            if named_human_lock
            else "No second skeleton and no extra person."
        ),
    ]
    if needs_table:
        prompt_parts.append(
            "Skeleton sits behind a real table with the tabletop clearly visible across the lower foreground and both hands near or on the tabletop."
        )
    else:
        prompt_parts.append("Keep the skeleton inside a readable scene that clearly matches the topic, not a blank backdrop.")
    if explicit_outfit_request:
        prompt_parts.append(_skeleton_outfit_coverage_lock(scene))
    if needs_brain and needs_money:
        glow = "glowing " if needs_glow else ""
        prompt_parts.append(
            f"Exactly two props on the tabletop: one realistic {glow}human brain with visible folds and one {glow}pile of paper cash banknotes."
        )
    elif needs_brain:
        glow = "glowing " if needs_glow else ""
        prompt_parts.append(f"One realistic {glow}human brain is clearly visible.")
    elif needs_money:
        glow = "glowing " if needs_glow else ""
        prompt_parts.append(f"One {glow}pile of paper cash banknotes is clearly visible.")
    else:
        prompt_parts.append(scene.rstrip(". ") + ".")
    if named_human_lock:
        prompt_parts.append(named_human_lock)
    prompt_parts.append(_skeleton_scene_framing_lock(scene))
    prompt_parts.append("Sharp focus, realistic lighting, readable environment depth.")
    return _truncate_words(" ".join(part for part in prompt_parts if part).strip(), 110)


def _compact_skeleton_negative_prompt(base_negative: str, prompt: str) -> str:
    text = str(prompt or "").lower()
    explicit_outfit_request = _skeleton_has_explicit_outfit_request(text)
    named_human_support = _skeleton_scene_supporting_humans_requested(prompt)
    tokens = [
        "blurry",
        "low quality",
        "text",
        "watermark",
        "cartoon",
        "anime",
        "painting",
        "clothed skeleton",
        "armor",
        "costume",
        "empty eye sockets",
        "missing eye",
        "monocle",
        "eyepatch",
        "glowing eyes",
        "human skin face",
        "human bust statue",
        "severed head",
        "head sculpture",
        "no translucent shell",
        "bare bones only",
        "x-ray style",
        "radiograph style",
        "fluoroscopy look",
        "neon blue xray glow",
        "medical scan visualization",
    ]
    if explicit_outfit_request:
        tokens = [t for t in tokens if t not in {"clothed skeleton", "armor", "costume"}]
        tokens.extend([
            "exposed ribcage outside clothing",
            "exposed pelvis outside clothing",
            "transparent outfit revealing full skeleton body",
            "naked skeleton body with only accessories",
        ])
    if named_human_support:
        tokens = [t for t in tokens if t not in {"human skin face"}]
        tokens.extend([
            "wrong named person identity",
            "generic anonymous person",
            "unrecognizable human face",
            "mismatched hairstyle",
            "mismatched facial hair",
            "wrong tattoos",
        ])
    if "brain" in text:
        tokens.extend([
            "smooth ball instead of brain",
            "fruit instead of brain",
            "missing brain",
        ])
    if re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", text):
        tokens.extend([
            "coins-only money",
            "blank paper instead of cash",
            "missing money stack",
        ])
    if re.search(r"\b(table|desk|countertop)\b", text):
        tokens.extend([
            "standing pose no table",
            "floating props without table",
            "empty table with no props",
        ])
    base = str(base_negative or "").strip()
    merged = ", ".join(dict.fromkeys([t for t in tokens if t and t.strip()]))
    if base:
        merged = f"{base}, {merged}"
    merged = _skeleton_named_human_negative_adjustment(merged, prompt, allow_extra_people=True)
    return _truncate_words(merged, 95)


def _build_skeleton_lora_fast_negative(base_negative: str, prompt: str) -> str:
    text = str(prompt or "").lower()
    named_human_support = _skeleton_scene_supporting_humans_requested(prompt)
    tokens = [
        "blurry",
        "low quality",
        "text",
        "watermark",
        "duplicate subject",
        "two skeletons",
        "extra skeleton",
        "extra person",
        "crowd",
        "reflection duplicate",
        "glass display case",
        "glass dome",
        "portal",
        "archway",
        "window frame",
        "xray",
        "radiograph",
        "medical scan",
        "gems",
        "crystals",
        "candle",
        "bowl",
        "cup",
        "extra props",
        "background made of money",
        "lying down pose",
    ]
    if named_human_support:
        tokens = [t for t in tokens if t not in {"extra person"}]
        tokens.extend([
            "wrong named person identity",
            "generic anonymous person",
            "unrecognizable human face",
            "mismatched hairstyle",
            "mismatched facial hair",
            "wrong tattoos",
        ])
    if "brain" in text:
        tokens.extend(["missing brain", "fruit instead of brain", "orb instead of brain"])
    if re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", text):
        tokens.extend(["missing money", "coins only", "blank paper instead of cash"])
    if re.search(r"\b(table|desk|countertop)\b", text):
        tokens.extend(["no table", "standing pose", "floating props", "empty tabletop"])
    base = str(base_negative or "").strip()
    merged = ", ".join(dict.fromkeys([t for t in tokens if t and t.strip()]))
    if base:
        merged = f"{base}, {merged}"
    merged = _skeleton_named_human_negative_adjustment(merged, prompt, allow_extra_people=True)
    return _truncate_words(merged, 110)


def _score_generated_image_quality(image_path: str, prompt: str = "", template: str = "") -> dict:
    """Heuristic image quality scorer used for best-of candidate selection."""
    if Image is None or ImageStat is None:
        return {"score": 0.0, "ok": False, "reason": "pillow_unavailable"}
    p = Path(image_path)
    if not p.exists() or p.stat().st_size <= 0:
        return {"score": 0.0, "ok": False, "reason": "missing_image"}
    try:
        with Image.open(p) as im:
            rgb = im.convert("RGB")
            w, h = rgb.size
            mega_px = (float(w) * float(h)) / 1_000_000.0
            gray = rgb.convert("L")
            bright = float(ImageStat.Stat(gray).mean[0])
            contrast = float(ImageStat.Stat(gray).stddev[0])
            hsv = rgb.convert("HSV")
            sat = float(ImageStat.Stat(hsv).mean[1])
            edge_strength = 0.0
            if ImageFilter is not None:
                edge_img = gray.filter(ImageFilter.FIND_EDGES)
                edge_strength = float(ImageStat.Stat(edge_img).mean[0])
            shell_ratio = 0.0
            shell_to_bone_ratio = 0.0
            subject_height_ratio = 0.0
            subject_width_ratio = 0.0
            brain_color_ratio = 0.0
            money_color_ratio = 0.0
            brain_component_area_ratio = 0.0
            money_component_area_ratio = 0.0
            brain_edge_density = 0.0
            brain_round_area_ratio = 0.0
            money_rect_area_ratio = 0.0
            prop_component_count = 0
            seated_full_body_required = False
            bone_r_mean = 0.0
            bone_g_mean = 0.0
            bone_b_mean = 0.0

            def _clamp01(v: float) -> float:
                if v <= 0.0:
                    return 0.0
                if v >= 1.0:
                    return 1.0
                return v

            sharp_norm = _clamp01((edge_strength - 10.0) / 35.0)
            contrast_norm = _clamp01((contrast - 26.0) / 48.0)
            sat_norm = _clamp01((sat - 28.0) / 95.0)
            exposure_norm = _clamp01(1.0 - (abs(bright - 145.0) / 145.0))
            size_norm = _clamp01(mega_px / 0.9)  # 720x1280 ~= 0.92MP baseline

            score = 100.0 * (
                0.34 * sharp_norm
                + 0.24 * contrast_norm
                + 0.14 * sat_norm
                + 0.16 * exposure_norm
                + 0.12 * size_norm
            )
            notes = []
            if edge_strength < 12:
                score -= 10.0
                notes.append("soft_edges")
            if contrast < 22:
                score -= 8.0
                notes.append("flat_contrast")
            if sat < 20:
                score -= 6.0
                notes.append("washed_colors")
            if template == "skeleton" and ("eyes" in prompt.lower() or "eyeballs" in prompt.lower()) and edge_strength < 14:
                score -= 5.0
                notes.append("weak_eye_detail")
            if template == "skeleton" and np is not None and ImageFilter is not None:
                try:
                    arr = np.asarray(rgb).astype(np.float32)
                    r = arr[:, :, 0]
                    g = arr[:, :, 1]
                    b = arr[:, :, 2]
                    maxc = np.maximum(np.maximum(r, g), b)
                    minc = np.minimum(np.minimum(r, g), b)
                    sat_arr = np.zeros_like(maxc, dtype=np.float32)
                    np.divide((maxc - minc), np.maximum(maxc, 1e-6), out=sat_arr, where=maxc > 1e-6)
                    sat_arr *= 255.0
                    lum_arr = 0.2126 * r + 0.7152 * g + 0.0722 * b
                    neutral = (
                        (np.abs(r - g) < 40.0)
                        & (np.abs(g - b) < 40.0)
                        & (np.abs(r - b) < 52.0)
                    )
                    bone = ((lum_arr > 150.0) & (sat_arr < 95.0) & neutral)
                    bone_px = int(bone.sum())
                    if bone_px > 1500:
                        try:
                            bone_r_mean = float(r[bone].mean())
                            bone_g_mean = float(g[bone].mean())
                            bone_b_mean = float(b[bone].mean())
                        except Exception:
                            bone_r_mean = 0.0
                            bone_g_mean = 0.0
                            bone_b_mean = 0.0
                        # Penalize blue radiograph/X-ray drift; reward natural ivory balance.
                        if bone_b_mean > (bone_r_mean + 10.0) and bone_b_mean > (bone_g_mean + 6.0):
                            score -= 16.0
                            notes.append("xray_style_drift")
                        elif bone_r_mean >= (bone_b_mean - 4.0) and bone_g_mean >= (bone_b_mean - 6.0):
                            score += 6.0
                        ys, xs = np.where(bone)
                        if ys.size > 0 and xs.size > 0:
                            subject_height_ratio = float((ys.max() - ys.min() + 1)) / float(max(1, h))
                            subject_width_ratio = float((xs.max() - xs.min() + 1)) / float(max(1, w))
                            if subject_height_ratio < 0.42:
                                score -= 9.0
                                notes.append("subject_too_small")
                            if subject_width_ratio < 0.18:
                                score -= 7.0
                                notes.append("subject_too_narrow")
                        bone_img = Image.fromarray((bone.astype(np.uint8) * 255), mode="L")
                        ring = ImageChops.subtract(
                            bone_img.filter(ImageFilter.MaxFilter(size=41)),
                            bone_img.filter(ImageFilter.MaxFilter(size=9)),
                        ).filter(ImageFilter.GaussianBlur(radius=3.5))
                        ring_arr = np.asarray(ring).astype(np.float32) / 255.0
                        shell_like = (
                            (ring_arr > 0.08)
                            & (lum_arr > 26.0)
                            & (lum_arr < 190.0)
                            & (b > (r * 0.90))
                            & (g > (r * 0.82))
                        )
                        shell_px = int(shell_like.sum())
                        ring_px = int((ring_arr > 0.08).sum())
                        shell_ratio = float(shell_px) / float(max(1, ring_px))
                        shell_to_bone_ratio = float(shell_px) / float(max(1, bone_px))
                        if shell_ratio < 0.045:
                            score -= 22.0
                            notes.append("missing_translucent_shell")
                        elif shell_ratio < 0.085:
                            score -= 10.0
                            notes.append("weak_translucent_shell")
                        if shell_to_bone_ratio < 0.16:
                            score -= 12.0
                            notes.append("shell_not_visible_enough")
                    else:
                        score -= 34.0
                        notes.append("not_skeleton_structure")

                    prompt_l = str(prompt or "").lower()
                    needs_brain = "brain" in prompt_l
                    needs_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", prompt_l))
                    seated_full_body_required = bool(
                        re.search(r"\b(sit|sits|seated|sitting)\b", prompt_l)
                        or re.search(r"\b(table|desk|countertop)\b", prompt_l)
                    )
                    if needs_brain or needs_money:
                        y0 = int(h * 0.50)
                        y1 = int(h * 0.97)
                        x0 = int(w * 0.16)
                        x1 = int(w * 0.84)
                        if y1 > y0 and x1 > x0:
                            rr = r[y0:y1, x0:x1]
                            gg = g[y0:y1, x0:x1]
                            bb = b[y0:y1, x0:x1]
                            if rr.size > 0:
                                brain_mask = (
                                    (rr > (gg + 10.0))
                                    & (gg > (bb + 3.0))
                                    & (rr > 75.0)
                                )
                                money_mask = (
                                    (gg > (rr + 8.0))
                                    & (gg > (bb + 8.0))
                                    & (gg > 65.0)
                                )
                                brain_color_ratio = float(brain_mask.mean())
                                money_color_ratio = float(money_mask.mean())
                                if cv2 is not None:
                                    try:
                                        roi = arr[y0:y1, x0:x1].astype(np.uint8)
                                        gray_roi = cv2.cvtColor(roi, cv2.COLOR_RGB2GRAY)
                                        edges_roi = cv2.Canny(gray_roi, 48, 132)
                                        if needs_brain:
                                            brain_u8 = (brain_mask.astype(np.uint8) * 255)
                                            comp_count, labels, stats, _ = cv2.connectedComponentsWithStats(brain_u8, connectivity=8)
                                            if comp_count > 1:
                                                areas = stats[1:, cv2.CC_STAT_AREA]
                                                largest_idx = int(1 + np.argmax(areas))
                                                largest = int(areas.max())
                                                brain_component_area_ratio = float(largest) / float(max(1, brain_u8.size))
                                                comp_sel = labels == largest_idx
                                                if comp_sel.any():
                                                    brain_edge_density = float((edges_roi[comp_sel] > 0).mean())
                                        if needs_money:
                                            money_u8 = (money_mask.astype(np.uint8) * 255)
                                            comp_count2, labels2, stats2, _ = cv2.connectedComponentsWithStats(money_u8, connectivity=8)
                                            if comp_count2 > 1:
                                                areas2 = stats2[1:, cv2.CC_STAT_AREA]
                                                largest2 = int(areas2.max())
                                                money_component_area_ratio = float(largest2) / float(max(1, money_u8.size))
                                        # Shape fallback to reduce false negatives when lighting grade desaturates props.
                                        cnts, _ = cv2.findContours(edges_roi, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                                        roi_h, roi_w = gray_roi.shape[:2]
                                        roi_area = float(max(1, roi_h * roi_w))
                                        for c in cnts:
                                            area = float(cv2.contourArea(c))
                                            if area < roi_area * 0.0018:
                                                continue
                                            x, y, ww, hh = cv2.boundingRect(c)
                                            if ww < 6 or hh < 6:
                                                continue
                                            fill = area / float(max(1, ww * hh))
                                            peri = float(cv2.arcLength(c, True))
                                            circ = (4.0 * 3.14159265 * area / (peri * peri)) if peri > 1e-6 else 0.0
                                            aspect = float(ww) / float(max(1, hh))
                                            local_ratio = area / roi_area
                                            # Brain-like rounded prop in lower half.
                                            if y > int(roi_h * 0.45) and 0.42 <= circ <= 1.25 and 0.0022 <= local_ratio <= 0.18:
                                                brain_round_area_ratio = max(brain_round_area_ratio, local_ratio)
                                            # Money-like rectangular stacks in lower half.
                                            if (
                                                y > int(roi_h * 0.45)
                                                and 1.2 <= aspect <= 6.0
                                                and fill >= 0.28
                                                and local_ratio >= 0.0015
                                            ):
                                                money_rect_area_ratio = max(money_rect_area_ratio, local_ratio)
                                        # Layout fallback: count distinct non-bone foreground props in lower ROI.
                                        bone_roi = bone[y0:y1, x0:x1] if isinstance(bone, np.ndarray) else None
                                        lum_roi = lum_arr[y0:y1, x0:x1]
                                        sat_roi = sat_arr[y0:y1, x0:x1]
                                        if bone_roi is not None and bone_roi.shape == lum_roi.shape:
                                            prop_seed = (
                                                (lum_roi > 18.0)
                                                & (lum_roi < 236.0)
                                                & (sat_roi > 14.0)
                                                & (~bone_roi)
                                            )
                                            prop_u8 = (prop_seed.astype(np.uint8) * 255)
                                            comp_n, comp_labels, comp_stats, _ = cv2.connectedComponentsWithStats(prop_u8, connectivity=8)
                                            for ci in range(1, int(comp_n)):
                                                area = int(comp_stats[ci, cv2.CC_STAT_AREA])
                                                if area < int(roi_area * 0.0017):
                                                    continue
                                                y_comp = int(comp_stats[ci, cv2.CC_STAT_TOP])
                                                if y_comp < int(roi_h * 0.42):
                                                    continue
                                                prop_component_count += 1
                                    except Exception:
                                        pass

                                layout_has_two_props = prop_component_count >= 2
                                brain_present_by_shape = brain_round_area_ratio >= 0.0030
                                money_present_by_shape = money_rect_area_ratio >= 0.0022
                                if needs_brain and needs_money and layout_has_two_props:
                                    brain_present_by_shape = True
                                    money_present_by_shape = True
                                brain_fail_count = int(brain_color_ratio < 0.012) + int(brain_component_area_ratio < 0.0023) + int(brain_edge_density < 0.038)
                                money_fail_count = int(money_color_ratio < 0.007) + int(money_component_area_ratio < 0.0018)
                                brain_bad = needs_brain and (brain_fail_count >= 2) and not brain_present_by_shape
                                money_bad = needs_money and (money_fail_count >= 2) and not money_present_by_shape
                                if brain_bad:
                                    score -= 16.0
                                    notes.append("brain_prop_missing_or_wrong")
                                if money_bad:
                                    score -= 14.0
                                    notes.append("money_prop_missing_or_wrong")
                    if seated_full_body_required:
                        # Reject skull-only closeups for seated/table prompts.
                        if subject_height_ratio < 0.55 or subject_width_ratio < 0.24:
                            score -= 18.0
                            notes.append("full_skeleton_not_visible")
                except Exception:
                    pass

            score = max(0.0, min(100.0, score))
            return {
                "score": round(score, 2),
                "ok": score >= float(IMAGE_QUALITY_MIN_SCORE),
                "metrics": {
                    "width": w,
                    "height": h,
                    "mega_px": round(mega_px, 3),
                    "brightness": round(bright, 2),
                    "contrast": round(contrast, 2),
                    "saturation": round(sat, 2),
                    "edge_strength": round(edge_strength, 2),
                    "shell_ratio": round(shell_ratio, 3),
                    "shell_to_bone_ratio": round(shell_to_bone_ratio, 3),
                    "subject_height_ratio": round(subject_height_ratio, 3),
                    "subject_width_ratio": round(subject_width_ratio, 3),
                    "brain_color_ratio": round(brain_color_ratio, 4),
                    "money_color_ratio": round(money_color_ratio, 4),
                    "brain_component_area_ratio": round(brain_component_area_ratio, 4),
                    "money_component_area_ratio": round(money_component_area_ratio, 4),
                    "brain_edge_density": round(brain_edge_density, 4),
                    "brain_round_area_ratio": round(brain_round_area_ratio, 4),
                    "money_rect_area_ratio": round(money_rect_area_ratio, 4),
                    "prop_component_count": int(prop_component_count),
                    "bone_r_mean": round(bone_r_mean, 2),
                    "bone_g_mean": round(bone_g_mean, 2),
                    "bone_b_mean": round(bone_b_mean, 2),
                },
                "notes": notes,
            }
    except Exception as e:
        return {"score": 0.0, "ok": False, "reason": f"scoring_error:{e}"}


def _image_quality_min_score(template: str = "", lock_mode: str = "strict", has_reference: bool = False) -> float:
    base = float(IMAGE_QUALITY_MIN_SCORE)
    t = str(template or "").strip().lower()
    if t == "skeleton":
        floor = max(64.0, base * 0.96)
        if str(lock_mode or "").strip().lower() == "strict":
            floor = max(floor, 69.0 if has_reference else 66.0)
        return floor
    if t == "story":
        return max(base, 64.0)
    return base


def _image_quality_gate(
    qa: dict,
    template: str = "",
    lock_mode: str = "strict",
    has_reference: bool = False,
    prompt: str = "",
) -> tuple[bool, float]:
    threshold = _image_quality_min_score(template=template, lock_mode=lock_mode, has_reference=has_reference)
    score = float((qa or {}).get("score", 0.0) or 0.0)
    ok = score >= threshold
    t = str(template or "").strip().lower()
    if t == "skeleton":
        prompt_lower = str(prompt or "").strip().lower()
        notes = set(str(n or "").strip().lower() for n in list((qa or {}).get("notes", []) or []))
        hard_fail = {
            "not_skeleton_structure",
            "full_skeleton_not_visible",
            "missing_translucent_shell",
            "shell_not_visible_enough",
        }
        documentary_case_prompt = bool(re.search(r"\b(documentary|crime|case|court|evidence|timeline|surveillance|records|source-capture|true-crime)\b", prompt_lower))
        if documentary_case_prompt and not notes.intersection(hard_fail):
            threshold = min(threshold, 53.0 if has_reference else 51.0)
            ok = score >= threshold
        if notes.intersection(hard_fail):
            ok = False
    return ok, threshold


def _skeleton_notes_are_severe(notes: list[str] | set[str] | tuple[str, ...]) -> bool:
    raw = set(str(n or "").strip().lower() for n in list(notes or []))
    return bool(
        "not_skeleton_structure" in raw
        or "full_skeleton_not_visible" in raw
    )


def _interactive_soft_accept_notes(notes: list[str] | set[str] | tuple[str, ...]) -> list[str]:
    cleaned: list[str] = []
    for n in list(notes or []):
        val = str(n or "").strip().lower()
        if not val:
            continue
        if val in {
            "brain_prop_missing_or_wrong",
            "money_prop_missing_or_wrong",
            "subject_too_small",
            "full_skeleton_not_visible",
        }:
            # Interactive mode can return the best available frame; avoid hard mismatch messaging in UI.
            continue
        if val not in cleaned:
            cleaned.append(val)
    if "interactive_soft_accept" not in cleaned:
        cleaned.append("interactive_soft_accept")
    return cleaned


def _resolve_reference_for_scene(session: dict, template: str, scene_index: int) -> str:
    if str(template or "").strip().lower() == "skeleton" and _skeleton_default_identity_locked(session):
        # For Skeleton AI, keep the built-in Jerry identity reference fixed across every
        # generation unless the user explicitly uploads a different custom reference.
        return _default_reference_for_template("skeleton")
    base_ref_public = str(session.get("reference_image_public_url", "") or "")
    base_ref_inline = str(session.get("reference_image_url", "") or "")
    base_ref = base_ref_public or base_ref_inline
    skeleton_ref = str(session.get("skeleton_reference_image", "") or "")
    rolling_ref = str(session.get("rolling_reference_image_url", "") or "")
    lock_mode = _normalize_reference_lock_mode(session.get("reference_lock_mode"), default="strict")
    selected = skeleton_ref if template == "skeleton" and skeleton_ref else base_ref
    selected = _normalize_reference_with_default(template, selected)
    # External providers are more reliable with public HTTPS URLs than data URLs.
    if lock_mode == "strict":
        if rolling_ref and scene_index > 0:
            return rolling_ref
        if base_ref_public:
            return base_ref_public
        if rolling_ref:
            return rolling_ref
    if lock_mode == "inspired" and rolling_ref and scene_index > 0 and scene_index % 3 == 0 and not base_ref_public:
        return rolling_ref
    return selected


def _ensure_reference_public_url(session_id: str, session: dict) -> str:
    """Backfill a public reference URL for legacy sessions that only stored a data URL."""
    existing = str(session.get("reference_image_public_url", "") or "")
    if existing:
        return existing
    data_url = str(session.get("reference_image_url", "") or "")
    if not data_url.startswith("data:image/"):
        return ""
    try:
        header, b64_data = data_url.split(",", 1)
        mime = "image/png"
        if ";" in header and ":" in header:
            mime = header.split(":", 1)[1].split(";", 1)[0].strip() or mime
        ext = ".png"
        if "jpeg" in mime or "jpg" in mime:
            ext = ".jpg"
        elif "webp" in mime:
            ext = ".webp"
        raw = base64.b64decode(b64_data, validate=False)
        if not raw:
            return ""
        ref_dir = TEMP_DIR / "creative_references"
        ref_dir.mkdir(parents=True, exist_ok=True)
        ref_name = f"{session_id}_reference{ext}"
        ref_path = ref_dir / ref_name
        ref_path.write_bytes(raw)
        public_url = f"{SITE_URL.rstrip('/')}/api/creative/reference-file/{ref_name}"
        session["reference_image_path"] = str(ref_path)
        session["reference_image_public_url"] = public_url
        # Keep skeleton compatibility key aligned with provider-friendly URL.
        if session.get("template") == "skeleton" and str(session.get("skeleton_reference_image", "") or "").startswith("data:image/"):
            session["skeleton_reference_image"] = public_url
        return public_url
    except Exception:
        return ""


def _auto_scene_dir(job_id: str) -> Path:
    d = AUTO_SCENE_IMAGE_ROOT / str(job_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def _auto_scene_url(job_id: str, filename: str) -> str:
    return f"/api/auto/scene-image/{job_id}/{filename}"


def _prune_in_memory_jobs():
    """Bound in-memory job growth to reduce long-run RAM pressure."""
    if not jobs:
        return
    now = time.time()
    final_states = {"complete", "error"}
    stale_ids = []
    for jid, state in list(jobs.items()):
        if not isinstance(state, dict):
            stale_ids.append(jid)
            continue
        created_at = float(state.get("created_at") or now)
        age_sec = max(0.0, now - created_at)
        status = str(state.get("status", "") or "")
        max_age = _JOB_RETENTION_FINAL_SEC if status in final_states else _JOB_RETENTION_ACTIVE_SEC
        if age_sec > max_age:
            stale_ids.append(jid)
    for jid in stale_ids:
        jobs.pop(jid, None)


def _persist_auto_scene_image(job_id: str, scene_index: int, image_path: str, scene: dict, template: str, img_result: dict | None, source: str) -> dict | None:
    """Persist auto-mode 720p scene images for regeneration/training workflows."""
    try:
        src = Path(image_path)
        if not src.exists():
            return None
        dst_name = f"scene_{scene_index + 1:02d}.png"
        dst = _auto_scene_dir(job_id) / dst_name
        shutil.copy2(src, dst)
        result = img_result or {}
        return {
            "scene_index": scene_index,
            "filename": dst_name,
            "image_url": _auto_scene_url(job_id, dst_name),
            "local_path": str(dst),
            "visual_description": str((scene or {}).get("visual_description", "") or ""),
            "template": template,
            "generation_id": str(result.get("generation_id", "") or ""),
            "cdn_url": str(result.get("cdn_url", "") or ""),
            "source": source,
            "updated_at": time.time(),
        }
    except Exception as e:
        log.warning(f"[{job_id}] Failed to persist scene image {scene_index + 1}: {e}")
        return None


def _persist_env_overrides(updates: dict[str, str]) -> None:
    env_file = Path(__file__).parent / ".env"
    lines = env_file.read_text(encoding="utf-8").splitlines() if env_file.exists() else []
    out = []
    seen = set()
    for line in lines:
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in line:
            out.append(line)
            continue
        key, _ = line.split("=", 1)
        key = key.strip()
        if key in updates:
            out.append(f"{key}={updates[key]}")
            seen.add(key)
        else:
            out.append(line)
    for key, val in updates.items():
        if key not in seen:
            out.append(f"{key}={val}")
    env_file.write_text("\n".join(out).strip() + "\n", encoding="utf-8")


_load_kpi_metrics()
_load_topup_wallets()
_load_paypal_orders()
_load_paypal_subscriptions()
_load_landing_notifications()
_load_longform_sessions()
_load_catalyst_memory()
_load_youtube_connections()
_load_youtube_oauth_states()


# â”€â”€â”€ Auth â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _is_admin_user(user: Optional[dict]) -> bool:
    if not user:
        return False
    return user.get("email", "") in ADMIN_EMAILS


def _profile_plan_is_paid(plan: str) -> bool:
    normalized = str(plan or "").strip().lower()
    if not normalized or normalized in {"none", "free"}:
        return False
    return normalized in PLAN_LIMITS


CHAT_STORY_ALLOWED_PLANS = {"starter", "creator", "pro"}
DEFAULT_MEMBERSHIP_PLAN_ID = "starter"


def _default_membership_plan_id() -> str:
    return DEFAULT_MEMBERSHIP_PLAN_ID if DEFAULT_MEMBERSHIP_PLAN_ID in PLAN_LIMITS else "starter"


def _membership_plan_for_user(user: Optional[dict], access_snapshot: Optional[dict] = None) -> str:
    snapshot = access_snapshot or _paid_access_snapshot_for_user(user)
    plan = str((snapshot or {}).get("plan", "none") or "none").strip().lower()
    if plan in PLAN_LIMITS:
        return plan
    stored_plan = str((user or {}).get("plan", "free") or "free").strip().lower()
    if stored_plan in PLAN_LIMITS:
        return stored_plan
    return _default_membership_plan_id() if bool((snapshot or {}).get("billing_active")) else "free"


def _credit_wallet_balance_for_user(user: Optional[dict]) -> int:
    if not user:
        return 0
    wallet = _wallet_for_user(str((user or {}).get("id", "") or ""))
    return int(wallet.get("animated_topup_credits", wallet.get("topup_credits", 0)) or 0)


def _public_lane_access_for_user(user: Optional[dict], access_snapshot: Optional[dict] = None) -> dict[str, bool]:
    is_admin = _is_admin_user(user)
    authenticated = bool(user)
    snapshot = access_snapshot or _paid_access_snapshot_for_user(user)
    public_live = authenticated
    membership_plan = _membership_plan_for_user(user, snapshot)
    chatstory_live = is_admin or (
        bool((snapshot or {}).get("billing_active"))
        and membership_plan in CHAT_STORY_ALLOWED_PLANS
    )
    return {
        "create": public_live,
        "thumbnails": is_admin,
        "clone": is_admin,
        "longform": is_admin,
        "chatstory": chatstory_live,
        "autoclipper": is_admin,
        "demo": is_admin,
        "analytics": is_admin,
        "membership": bool((snapshot or {}).get("billing_active")) or is_admin,
        "wallet": (_credit_wallet_balance_for_user(user) > 0) or is_admin,
    }


def _paypal_subscription_lookup_keys(user_id: str = "", email: str = "") -> list[str]:
    keys: list[str] = []
    normalized_user_id = str(user_id or "").strip()
    normalized_email = str(email or "").strip().lower()
    if normalized_user_id:
        keys.append(normalized_user_id)
    if normalized_email and normalized_email not in keys:
        keys.append(normalized_email)
    return keys


def _paypal_subscription_record_for_user(user: Optional[dict]) -> dict:
    if not user:
        return {}
    normalized_user_id = str((user or {}).get("id", "") or "").strip()
    normalized_email = str((user or {}).get("email", "") or "").strip().lower()
    for key in _paypal_subscription_lookup_keys(normalized_user_id, normalized_email):
        record = _paypal_subscriptions.get(key)
        if isinstance(record, dict):
            return dict(record)
    best_record: dict = {}
    best_sort = -1.0
    for record in list(_paypal_subscriptions.values()):
        if not isinstance(record, dict):
            continue
        record_user_id = str(record.get("user_id", "") or "").strip()
        record_email = str(record.get("email", "") or "").strip().lower()
        if normalized_user_id and record_user_id == normalized_user_id:
            pass
        elif normalized_email and record_email == normalized_email:
            pass
        else:
            continue
        sort_value = max(
            float(record.get("period_end_unix", 0) or 0),
            float(record.get("updated_at", 0) or 0),
            float(record.get("created_at", 0) or 0),
        )
        if sort_value >= best_sort:
            best_record = dict(record)
            best_sort = sort_value
    return best_record


def _paypal_subscription_snapshot_for_user(user: Optional[dict]) -> dict:
    out = {
        "known": False,
        "provider": "",
        "plan": "none",
        "record_plan": "none",
        "billing_active": False,
        "next_renewal_unix": 0,
        "next_renewal_source": "",
        "billing_anchor_unix": 0,
        "status": "",
        "expired": False,
    }
    record = _paypal_subscription_record_for_user(user)
    if not record:
        return out
    plan = str(record.get("plan", "none") or "none").strip().lower()
    period_end_unix = int(record.get("period_end_unix", 0) or 0)
    billing_anchor_unix = int(
        record.get("period_start_unix", 0)
        or record.get("captured_at", 0)
        or record.get("created_at", 0)
        or 0
    )
    status = str(record.get("status", "") or "").strip().lower()
    now_unix = int(time.time())
    active = (
        plan in CHAT_STORY_ALLOWED_PLANS
        and status in {"active", "captured"}
        and period_end_unix > now_unix
    )
    out.update(
        {
            "known": True,
            "provider": "paypal_manual",
            "plan": plan if active else "none",
            "record_plan": plan if plan in CHAT_STORY_ALLOWED_PLANS else "none",
            "billing_active": active,
            "next_renewal_unix": period_end_unix if active else 0,
            "next_renewal_source": "paypal_manual" if active else "",
            "billing_anchor_unix": billing_anchor_unix,
            "status": status or ("expired" if period_end_unix > 0 and period_end_unix <= now_unix else ""),
            "expired": bool(period_end_unix > 0 and period_end_unix <= now_unix and not active),
        }
    )
    return out


def _paid_access_snapshot_for_user(user: Optional[dict]) -> dict:
    out = {
        "billing_active": False,
        "plan": "none",
        "source": "",
        "next_renewal_unix": 0,
        "next_renewal_source": "",
        "billing_anchor_unix": 0,
        "manual_record_present": False,
    }
    if not user:
        return out
    email = str((user or {}).get("email", "") or "").strip().lower()
    if email in ADMIN_EMAILS:
        out.update(
            {
                "billing_active": True,
                "plan": "pro",
                "source": "admin",
            }
        )
        return out
    manual_snapshot = _paypal_subscription_snapshot_for_user(user)
    out["manual_record_present"] = bool(manual_snapshot.get("known"))
    if manual_snapshot.get("billing_active"):
        out.update(
            {
                "billing_active": True,
                "plan": str(manual_snapshot.get("plan", "none") or "none"),
                "source": str(manual_snapshot.get("provider", "paypal_manual") or "paypal_manual"),
                "next_renewal_unix": int(manual_snapshot.get("next_renewal_unix", 0) or 0),
                "next_renewal_source": str(manual_snapshot.get("next_renewal_source", "") or ""),
                "billing_anchor_unix": int(manual_snapshot.get("billing_anchor_unix", 0) or 0),
            }
        )
        return out
    stripe_diag = _stripe_subscription_snapshot(email) if email else {}
    stripe_status = str(stripe_diag.get("status", "") or "").strip().lower()
    stripe_ok = bool(stripe_diag.get("ok")) and stripe_status in {"active", "trialing", "past_due"}
    if stripe_ok:
        stripe_plan = str(stripe_diag.get("plan", "") or "").strip().lower()
        stored_plan = str((user or {}).get("plan", "none") or "none").strip().lower()
        effective_plan = stripe_plan if stripe_plan in PLAN_LIMITS else stored_plan if stored_plan in PLAN_LIMITS else "none"
        out.update(
            {
                "billing_active": True,
                "plan": effective_plan,
                "source": "stripe",
                "next_renewal_unix": int(stripe_diag.get("next_renewal_unix", 0) or 0),
                "next_renewal_source": str(stripe_diag.get("next_renewal_source", "") or ""),
                "billing_anchor_unix": int(stripe_diag.get("paid_at_unix", 0) or 0),
            }
        )
        return out
    stored_plan = str((user or {}).get("plan", "none") or "none").strip().lower()
    if _profile_plan_is_paid(stored_plan) and not bool(manual_snapshot.get("known")):
        log.warning("Stripe check failed/inactive; allowing paid access via profile plan for %s", email)
        out.update(
            {
                "billing_active": True,
                "plan": stored_plan,
                "source": "profile_fallback",
            }
        )
        return out
    return out


def _billing_active_for_user(user: Optional[dict]) -> bool:
    """Resolve paid access across Stripe and manual PayPal monthly access."""
    return bool((_paid_access_snapshot_for_user(user) or {}).get("billing_active"))


def _chat_story_access_for_user(user: Optional[dict]) -> bool:
    return bool((_public_lane_access_for_user(user) or {}).get("chatstory"))


def _ensure_template_allowed(template: str, user: Optional[dict]):
    if _is_admin_user(user):
        return
    if template not in PUBLIC_TEMPLATE_ALLOWLIST:
        raise HTTPException(403, f"Template '{template}' is not available on your plan yet.")


async def get_user_plan(user: dict) -> dict:
    """Look up user's plan from Supabase. Falls back to free."""
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        return PLAN_LIMITS.get("free", PLAN_LIMITS["starter"])
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/profiles?id=eq.{user['id']}&select=plan",
                headers={
                    "apikey": SUPABASE_ANON_KEY,
                    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    plan_name = str(data[0].get("plan", "free") or "free").strip().lower()
                    if plan_name == "none":
                        plan_name = "free"
                    return PLAN_LIMITS.get(plan_name, PLAN_LIMITS.get("free", PLAN_LIMITS["starter"]))
    except Exception as e:
        log.warning(f"Failed to fetch user plan: {e}")
    return PLAN_LIMITS.get("free", PLAN_LIMITS["starter"])


# â”€â”€â”€ xAI Grok Script Generation â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

from backend_script_prompts import TEMPLATE_SYSTEM_PROMPTS




# â”€â”€â”€ ElevenLabs TTS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


async def _xai_json_completion(system_prompt: str, user_prompt: str, temperature: float = 0.7, timeout_sec: int = 90) -> dict:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=timeout_sec) as client:
                resp = await client.post(
                    "https://api.x.ai/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {XAI_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": "grok-3-mini-fast",
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt},
                        ],
                        "temperature": max(0.2, float(temperature) - (attempt * 0.08)),
                    },
                )
                if resp.status_code in {429, 500, 502, 503, 504}:
                    raise httpx.HTTPStatusError(
                        f"Retryable xAI status {resp.status_code}",
                        request=resp.request,
                        response=resp,
                    )
                resp.raise_for_status()
                content = resp.json()["choices"][0]["message"]["content"]
            start = content.find("{")
            end = content.rfind("}") + 1
            if start == -1 or end <= 0:
                raise ValueError("No JSON found in xAI response")
            return json.loads(content[start:end])
        except Exception as exc:
            last_error = exc
            if attempt >= 2:
                break
            wait_seconds = (attempt + 1) * 2
            await asyncio.sleep(wait_seconds)
    raise last_error if last_error is not None else RuntimeError("xAI JSON completion failed")


async def _xai_json_completion_multimodal(
    system_prompt: str,
    user_prompt: str,
    image_paths: list[str] | None = None,
    temperature: float = 0.35,
    timeout_sec: int = 120,
    model: str = "grok-4",
) -> dict:
    content_items: list[dict] = [{"type": "text", "text": user_prompt}]
    for image_path in list(image_paths or [])[:24]:
        data_url = _file_to_data_image_url(str(image_path), max_bytes=18 * 1024 * 1024)
        if not data_url:
            continue
        content_items.append(
            {
                "type": "image_url",
                "image_url": {
                    "url": data_url,
                    "detail": "high",
                },
            }
        )
    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        resp = await client.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {XAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": content_items},
                ],
                "temperature": temperature,
            },
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
    start = content.find("{")
    end = content.rfind("}") + 1
    if start == -1 or end <= 0:
        raise ValueError("No JSON found in xAI multimodal response")
    return json.loads(content[start:end])


FAL_IMAGE_UNDERSTAND_URL = "https://fal.run/fal-ai/bagel/understand"


def _extract_json_object_from_text(content: str, source_name: str = "model response") -> dict:
    text = str(content or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        text = text.strip()
    start = text.find("{")
    end = text.rfind("}") + 1
    if start == -1 or end <= 0:
        raise ValueError(f"No JSON found in {source_name}")
    return json.loads(text[start:end])


def _strip_model_reasoning_artifacts(content: str) -> str:
    text = str(content or "").strip()
    if not text:
        return ""
    text = re.sub(r"(?is)```[a-zA-Z0-9_-]*\s*(.*?)\s*```", r"\1", text)
    text = re.sub(r"(?is)<think>.*?</think>", " ", text)
    text = re.sub(r"(?i)</?think>", " ", text)
    sentences = re.split(r"(?<=[.!?])\s+|\n+", text)
    cleaned: list[str] = []
    for raw_sentence in sentences:
        sentence = re.sub(r"\s+", " ", str(raw_sentence or "").strip())
        if not sentence:
            continue
        if re.search(
            r"^(okay|first|next|now|let'?s|let me|i need to|i should|the user wants|we need to|we should|"
            r"this task|for more information check)\b",
            sentence,
            flags=re.IGNORECASE,
        ):
            continue
        cleaned.append(sentence)
    return re.sub(r"\s+", " ", " ".join(cleaned)).strip()


async def _fal_image_understanding_json(
    prompt: str,
    image_path: str,
    timeout_sec: int = 120,
) -> dict:
    if not FAL_AI_KEY:
        raise RuntimeError("FAL_AI_KEY not configured")
    image_url = await _upload_image_to_fal(image_path)
    headers = {
        "Authorization": "Key " + FAL_AI_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "image_url": image_url,
        "prompt": str(prompt or "").strip(),
    }
    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        resp = await client.post(FAL_IMAGE_UNDERSTAND_URL, headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError("fal.ai image understanding failed (" + str(resp.status_code) + "): " + resp.text[:300])
        data = resp.json()
    content = str((data or {}).get("text", "") or "").strip()
    if not content:
        raise ValueError("fal.ai image understanding returned no text")
    return _extract_json_object_from_text(content, "fal.ai image understanding response")


async def _fal_image_understanding_text(
    prompt: str,
    image_path: str,
    timeout_sec: int = 120,
) -> str:
    if not FAL_AI_KEY:
        raise RuntimeError("FAL_AI_KEY not configured")
    image_url = await _upload_image_to_fal(image_path)
    headers = {
        "Authorization": "Key " + FAL_AI_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "image_url": image_url,
        "prompt": str(prompt or "").strip(),
    }
    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        resp = await client.post(FAL_IMAGE_UNDERSTAND_URL, headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError("fal.ai image understanding failed (" + str(resp.status_code) + "): " + resp.text[:300])
        data = resp.json()
    content = str((data or {}).get("text", "") or "").strip()
    if not content:
        raise ValueError("fal.ai image understanding returned no text")
    stripped = _strip_model_reasoning_artifacts(content)
    return stripped or content


configure_youtube_runtime_hooks(
    source_url_video_id=_source_url_video_id,
    normalize_external_source_url=lambda value: _normalize_external_source_url(value),
    duration_text_to_seconds=_duration_text_to_seconds,
    parse_vtt_text=_parse_vtt_text,
    ytdlp_module=yt_dlp,
    ytdlp_extract_info_blocking=_yt_dlp_extract_info_blocking,
)
configure_youtube_analysis_hooks(
    dedupe_clip_list=_dedupe_clip_list,
    catalyst_infer_niche=_catalyst_infer_niche,
    catalyst_infer_archetype=_catalyst_infer_archetype,
    catalyst_extract_series_anchor=_catalyst_extract_series_anchor,
    catalyst_build_channel_series_clusters=_catalyst_build_channel_series_clusters,
    build_catalyst_cluster_playbook=_build_catalyst_cluster_playbook,
    packaging_tokens=_packaging_tokens,
    same_arena_subject=_same_arena_subject,
    same_arena_title_variants=_same_arena_title_variants,
)
configure_video_pipeline_runtime_hooks(
    named_human_subject_likeness_lock=_named_human_subject_likeness_lock,
    longform_named_human_priority_lock=_longform_named_human_priority_lock,
    build_scene_prompt_with_reference=_build_scene_prompt_with_reference,
    longform_subject_lock=_longform_subject_lock,
    xai_json_completion=_xai_json_completion,
)


CATALYST_HUB_SHORT_WORKSPACES = ["story", "motivation", "skeleton", "daytrading", "chatstory"]
CATALYST_HUB_LONGFORM_WORKSPACES = ["documentary", "recap", "explainer", "story_channel"]






def _clip_text(value: str, max_chars: int = 320) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 3)].rstrip() + "..."


async def _build_source_performance_analysis(
    source_bundle: dict,
    channel_context: dict | None = None,
    channel_memory: dict | None = None,
    analytics_notes: str = "",
    topic: str = "",
    input_title: str = "",
    input_description: str = "",
    strategy_notes: str = "",
) -> dict:
    if not source_bundle and not analytics_notes and not channel_context and not channel_memory:
        return {}
    format_preset = "documentary" if re.search(r"\b(documentary|explainer|breakdown|analysis)\b", f"{topic} {input_title} {input_description}", flags=re.IGNORECASE) else "explainer"
    series_context = _resolve_catalyst_series_context(
        channel_context,
        channel_memory=channel_memory,
        topic=topic,
        source_title=str((source_bundle or {}).get("title", "") or ""),
        input_title=input_title,
        input_description=input_description,
        format_preset=format_preset,
    )
    series_anchor_override = str(series_context.get("series_anchor_override", "") or "").strip()
    selected_cluster = dict(series_context.get("selected_cluster") or {})
    cluster_context = _clip_text(str(series_context.get("cluster_context", "") or "").strip(), 1200)
    memory_view = dict(series_context.get("memory_view") or {})
    cluster_playbook = dict(series_context.get("cluster_playbook") or {})
    heuristic = _heuristic_source_performance_analysis(
        source_bundle=source_bundle,
        analytics_notes=analytics_notes,
        topic=topic,
        input_title=input_title,
        input_description=input_description,
        format_preset=format_preset,
    )
    reference_corpus_context = _render_catalyst_reference_corpus_context(
        reference_memory=_catalyst_reference_memory,
        format_preset=format_preset,
        topic=topic or input_title or input_description,
        channel_memory=memory_view,
        selected_cluster=selected_cluster,
    )
    system_prompt = (
        "You are a YouTube growth strategist for NYPTID Studio. "
        "Analyze a source video using public metadata plus optional operator notes. "
        "If connected-channel context exists, use it to stay inside the channel's proven topic and packaging arena without copying old titles. "
        "Output strict JSON with keys: what_worked, what_hurt, hook_learnings, click_drivers, "
        "dropoff_risks, improvement_moves, title_angles, thumbnail_angles, description_angles. "
        "Keep every field practical and specific for building a better follow-up video. "
        "Stay in the same topic arena as the source title and preserve the same viewer promise category instead of drifting sideways. "
        "Title angles must not recycle the exact source title, the same numbered-list wording, the same opening phrase, or any connected-channel winner title."
    )
    user_prompt = (
        f"New target topic: {topic}\n"
        f"Draft title constraint: {input_title}\n"
        f"Draft description constraint: {input_description}\n"
        f"Public source bundle: {json.dumps(source_bundle or {}, ensure_ascii=True)}\n"
        f"Connected channel context: {json.dumps(channel_context or {}, ensure_ascii=True)}\n"
        f"Matched channel series cluster: {json.dumps(selected_cluster, ensure_ascii=True)}\n"
        f"Matched channel series cluster context: {cluster_context}\n"
        f"Catalyst cluster playbook: {json.dumps(cluster_playbook, ensure_ascii=True)}\n"
        f"Catalyst channel memory: {json.dumps(memory_view, ensure_ascii=True)}\n"
        f"Reference documentary corpus: {_clip_text(reference_corpus_context, 3000)}\n"
        f"Private analytics/operator notes: {_clip_text(analytics_notes, 1800)}\n"
        "Use this marketing doctrine as operating context:\n"
        f"{_marketing_doctrine_text(strategy_notes)}"
    )
    try:
        raw = await _xai_json_completion(system_prompt, user_prompt, temperature=0.35, timeout_sec=60)
        return _merge_source_analysis(raw, heuristic)
    except Exception as e:
        fallback = dict(heuristic)
        improvement_moves = [str(v).strip() for v in list(fallback.get("improvement_moves") or []) if str(v).strip()]
        improvement_moves.append(_clip_text(str(e), 220))
        fallback["improvement_moves"] = _dedupe_clip_list(improvement_moves, max_items=8, max_chars=200)
        return fallback


async def _derive_longform_seed_from_source(
    source_bundle: dict,
    source_analysis: dict,
    channel_context: dict | None = None,
    channel_memory: dict | None = None,
    format_preset: str = "explainer",
    strategy_notes: str = "",
) -> dict:
    if not source_bundle and not source_analysis and not channel_context and not channel_memory:
        return {}
    source_title = _clip_text(str((source_bundle or {}).get("title", "") or "").strip(), 140)
    source_summary = _clip_text(str((source_bundle or {}).get("public_summary", "") or "").strip(), 420)
    series_context = _resolve_catalyst_series_context(
        channel_context,
        channel_memory=channel_memory,
        topic=source_title,
        source_title=source_title,
        input_title=source_title,
        input_description=str((source_bundle or {}).get("description", "") or ""),
        format_preset=format_preset,
    )
    series_anchor_override = str(series_context.get("series_anchor_override", "") or "").strip()
    selected_cluster = dict(series_context.get("selected_cluster") or {})
    cluster_context = _clip_text(str(series_context.get("cluster_context", "") or "").strip(), 1200)
    memory_view = dict(series_context.get("memory_view") or {})
    cluster_playbook = dict(series_context.get("cluster_playbook") or {})
    channel_title_memory = [str(v).strip() for v in list((channel_context or {}).get("recent_upload_titles") or []) if str(v).strip()]
    channel_title_memory.extend(str(v).strip() for v in list((channel_context or {}).get("top_video_titles") or []) if str(v).strip())
    channel_title_memory.extend(str(v).strip() for v in list(memory_view.get("recent_selected_titles") or []) if str(v).strip())
    channel_title_memory.extend(str(v).strip() for v in list(selected_cluster.get("sample_titles") or []) if str(v).strip())
    improvement_moves = source_analysis.get("improvement_moves") or []
    heuristic_titles = _same_arena_title_variants(source_bundle, topic="", format_preset=format_preset)
    heuristic_descriptions = _same_arena_description_variants(source_bundle, topic="", source_analysis=source_analysis)
    title_angles = [str(x).strip() for x in list(source_analysis.get("title_angles") or []) if str(x).strip()] or heuristic_titles
    description_angles = [str(x).strip() for x in list(source_analysis.get("description_angles") or []) if str(x).strip()] or heuristic_descriptions
    primary_move = _clip_text(str(improvement_moves[0] if improvement_moves else ""), 180)
    reference_corpus_context = _render_catalyst_reference_corpus_context(
        reference_memory=_catalyst_reference_memory,
        format_preset=format_preset,
        topic=source_title,
        channel_memory=memory_view,
        selected_cluster=selected_cluster,
    )
    system_prompt = (
        "You are a faceless YouTube strategist for NYPTID Studio. "
        "A user has a source video URL but does not want to hand-write the next topic, title, or description. "
        "Create a sharper follow-up brief on the same general subject, but improve the angle, hook clarity, and packaging. "
        "Do not drift into a different topic arena. Stay in the same documentary or explainer lane as the source. "
        "Do not copy the source title verbatim. Do not reuse the same lead phrase or the same Top-N phrasing if the source used it. "
        "Do not reuse any recent connected-channel title verbatim or near-verbatim either. "
        "The new title must feel adjacent but genuinely new. Output strict JSON with keys: topic, title, description."
    )
    user_prompt = (
        f"Format preset: {format_preset}\n"
        f"Public source bundle: {json.dumps(source_bundle or {}, ensure_ascii=True)}\n"
        f"Source performance analysis: {json.dumps(source_analysis or {}, ensure_ascii=True)}\n"
        f"Connected channel context: {json.dumps(channel_context or {}, ensure_ascii=True)}\n"
        f"Matched channel series cluster: {json.dumps(selected_cluster, ensure_ascii=True)}\n"
        f"Matched channel series cluster context: {cluster_context}\n"
        f"Catalyst cluster playbook: {json.dumps(cluster_playbook, ensure_ascii=True)}\n"
        f"Catalyst channel memory: {json.dumps(memory_view, ensure_ascii=True)}\n"
        f"Reference documentary corpus: {_clip_text(reference_corpus_context, 3000)}\n"
        "Use this marketing doctrine as operating context:\n"
        f"{_marketing_doctrine_text(strategy_notes)}"
    )
    try:
        raw = await _xai_json_completion(system_prompt, user_prompt, temperature=0.4, timeout_sec=60)
        derived_topic = _clip_text(str((raw or {}).get("topic", "") or "").strip(), 140)
        derived_title = _clip_text(str((raw or {}).get("title", "") or "").strip(), 140)
        derived_description = _clip_text(str((raw or {}).get("description", "") or "").strip(), 420)
    except Exception:
        derived_topic = ""
        derived_title = ""
        derived_description = ""
    if "follow-up" in derived_topic.lower() or "same arena" in derived_topic.lower():
        derived_topic = ""
    if derived_title and (
        _title_is_too_close_to_source(derived_title, source_title)
        or _title_is_too_close_to_any(derived_title, channel_title_memory)
        or _title_reuses_opening_pattern(derived_title, source_title, channel_title_memory)
    ):
        derived_title = ""
    cluster_follow_rule = str(selected_cluster.get("follow_up_rule", "") or "").strip()
    fallback_topic = derived_topic or cluster_follow_rule or _same_arena_follow_up_topic(source_bundle, format_preset=format_preset) or source_title or "Follow-up video breakdown"
    if derived_title and not _title_stays_in_same_arena(derived_title, source_title, fallback_topic):
        derived_title = ""
    filtered_title_angles = [
        tt
        for tt in title_angles
        if not _title_is_too_close_to_any(tt, [source_title, *channel_title_memory])
        and not _title_reuses_opening_pattern(tt, source_title, channel_title_memory)
    ]
    fallback_title = derived_title or ((filtered_title_angles or title_angles or [source_title or "New follow-up video"])[0])
    fallback_description = derived_description or (
        description_angles[0]
        if description_angles
        else "Follow-up on the source topic with a clearer hook, tighter pacing, and stronger packaging."
    )
    if primary_move:
        fallback_description = _clip_text(f"{fallback_description} Improvement focus: {primary_move}", 420)
    elif source_summary and not derived_description:
        fallback_description = _clip_text(f"{fallback_description} Source context: {source_summary}", 420)
    return {
        "topic": fallback_topic,
        "title": fallback_title,
        "description": fallback_description,
    }


async def _summarize_longform_operator_evidence(
    transcript_text: str = "",
    image_paths: list[str] | None = None,
    source_bundle: dict | None = None,
) -> dict:
    transcript_text = str(transcript_text or "").strip()
    image_paths = [str(p) for p in list(image_paths or []) if str(p).strip()]
    if not transcript_text and not image_paths:
        return {}
    system_prompt = (
        "You are a YouTube analytics strategist for NYPTID Studio. "
        "The user is supplying transcript text and/or screenshots from YouTube analytics so Catalyst can improve the next video. "
        "Read the screenshots carefully and summarize the practical signals. "
        "Output strict JSON with keys: analytics_summary, strongest_signals, weak_points, retention_findings, packaging_findings, improvement_moves."
    )
    source_title = _clip_text(str((source_bundle or {}).get("title", "") or "").strip(), 180)
    source_summary = _clip_text(str((source_bundle or {}).get("public_summary", "") or "").strip(), 700)
    user_prompt = (
        f"Source title: {source_title}\n"
        f"Source summary: {source_summary}\n"
        f"Manual transcript text (may be partial): {_clip_text(transcript_text, 12000)}\n"
        "Screenshots may include retention graphs, CTR, AVD, impressions, browse/source data, and other YouTube analytics. "
        "Turn them into concise operator guidance for the next version."
    )
    per_image_results: list[dict] = []
    per_image_errors: list[str] = []
    temp_contact_sheets: list[Path] = []
    image_prompt = (
        "Read this YouTube analytics screenshot carefully. Extract only what is visibly present. "
        "Return plain text with short lines. Include visible metrics, percentages, labels, graph captions, traffic sources, "
        "retention timestamps, CTR, average view duration, views, impressions, average percentage viewed, and any warnings or notes. "
        "Do not infer hidden analytics, do not invent numbers, and do not speculate beyond what is visible.\n\n"
        + user_prompt
    )
    analysis_paths: list[str] = []
    raw_paths = image_paths[:24]
    if len(raw_paths) > 4:
        for batch_index, start in enumerate(range(0, len(raw_paths), 4)):
            batch = raw_paths[start:start + 4]
            sheet_path = TEMP_DIR / "longform_bootstrap" / f"analytics_sheet_{int(time.time() * 1000)}_{batch_index}.png"
            try:
                built = _build_analytics_contact_sheet(batch, str(sheet_path))
                if built:
                    temp_contact_sheets.append(Path(built))
                    analysis_paths.append(str(built))
            except Exception:
                analysis_paths.extend(batch)
    else:
        analysis_paths = raw_paths

    try:
        for image_path in analysis_paths:
            try:
                raw_text = await _fal_image_understanding_text(image_prompt, image_path, timeout_sec=120)
                result = _summarize_longform_analytics_text(raw_text, source_bundle=source_bundle)
                per_image_results.append(result)
            except Exception as e:
                per_image_errors.append(str(e))
    finally:
        for temp_path in temp_contact_sheets:
            try:
                temp_path.unlink(missing_ok=True)
            except Exception:
                pass

    summary_parts: list[str] = []
    strongest_signals: list[str] = []
    weak_points: list[str] = []
    retention_findings: list[str] = []
    packaging_findings: list[str] = []
    improvement_moves: list[str] = []

    for result in per_image_results:
        summary = _clip_text(str(result.get("analytics_summary", "") or "").strip(), 220)
        if summary:
            summary_parts.append(summary)
        strongest_signals.extend(str(v).strip() for v in list(result.get("strongest_signals") or []) if str(v).strip())
        weak_points.extend(str(v).strip() for v in list(result.get("weak_points") or []) if str(v).strip())
        retention_findings.extend(str(v).strip() for v in list(result.get("retention_findings") or []) if str(v).strip())
        packaging_findings.extend(str(v).strip() for v in list(result.get("packaging_findings") or []) if str(v).strip())
        improvement_moves.extend(str(v).strip() for v in list(result.get("improvement_moves") or []) if str(v).strip())

    if transcript_text:
        summary_parts.append("Manual transcript supplied to preserve the source explanation beats while rewriting the next version.")
        improvement_moves.append("Use the transcript for factual continuity, but rewrite the first 20 to 30 seconds for a faster promise and payoff.")
        retention_findings.append("Manual transcript is available, so the next draft can keep the strongest information while tightening the opening rhythm.")

    if per_image_errors and not per_image_results:
        summary_parts.append("fal.ai screenshot analysis failed on the supplied analytics images.")
        improvement_moves.append(_clip_text(per_image_errors[0], 220))

    analytics_summary = " ".join(_dedupe_clip_list(summary_parts, max_items=4, max_chars=240))
    return {
        "analytics_summary": analytics_summary,
        "strongest_signals": _dedupe_clip_list(strongest_signals, max_items=8),
        "weak_points": _dedupe_clip_list(weak_points, max_items=8),
        "retention_findings": _dedupe_clip_list(retention_findings, max_items=8),
        "packaging_findings": _dedupe_clip_list(packaging_findings, max_items=8),
        "improvement_moves": _dedupe_clip_list(improvement_moves, max_items=10),
    }


def _render_source_context(
    source_bundle: dict,
    source_analysis: dict,
    analytics_notes: str = "",
    channel_context: dict | None = None,
    channel_memory: dict | None = None,
    selected_cluster: dict | None = None,
    cluster_playbook: dict | None = None,
) -> str:
    parts: list[str] = []
    if source_bundle:
        parts.append("Public source analysis:")
        parts.append(str(source_bundle.get("public_summary", "") or "").strip())
    if channel_context:
        parts.append("Connected YouTube channel context:")
        summary = str((channel_context or {}).get("summary", "") or "").strip()
        if summary:
            parts.append(summary)
        title_hints = [str(v).strip() for v in list((channel_context or {}).get("title_pattern_hints") or []) if str(v).strip()]
        if title_hints:
            parts.append("Channel title pattern hints: " + "; ".join(_clip_text(v, 120) for v in title_hints[:4]))
        packaging = [str(v).strip() for v in list((channel_context or {}).get("packaging_learnings") or []) if str(v).strip()]
        if packaging:
            parts.append("Channel packaging learnings: " + "; ".join(_clip_text(v, 120) for v in packaging[:4]))
        historical_compare = dict((channel_context or {}).get("historical_compare") or {})
        historical_summary = str(historical_compare.get("winner_vs_loser_summary", "") or "").strip()
        if historical_summary:
            parts.append("Channel winner vs loser compare: " + _clip_text(historical_summary, 240))
        historical_moves = [str(v).strip() for v in list(historical_compare.get("next_moves") or []) if str(v).strip()]
        if historical_moves:
            parts.append("Historical channel next moves: " + "; ".join(_clip_text(v, 120) for v in historical_moves[:3]))
    cluster_context = _render_catalyst_series_cluster_context(selected_cluster)
    if cluster_context:
        parts.append(cluster_context)
    cluster_playbook = dict(cluster_playbook or {})
    playbook_summary = str(cluster_playbook.get("summary", "") or "").strip()
    if playbook_summary:
        parts.append("Catalyst arc playbook: " + _clip_text(playbook_summary, 280))
    archetype_summary = str((channel_memory or {}).get("archetype_memory_summary", "") or "").strip()
    if archetype_summary:
        parts.append("Catalyst archetype playbook: " + _clip_text(archetype_summary, 280))
    winning_patterns = [str(v).strip() for v in list(cluster_playbook.get("winning_patterns") or []) if str(v).strip()]
    if winning_patterns:
        parts.append("Best-arc patterns: " + "; ".join(_clip_text(v, 140) for v in winning_patterns[:3]))
    losing_patterns = [str(v).strip() for v in list(cluster_playbook.get("losing_patterns") or []) if str(v).strip()]
    if losing_patterns:
        parts.append("Weak-arc watchouts: " + "; ".join(_clip_text(v, 140) for v in losing_patterns[:3]))
    next_run_moves = [str(v).strip() for v in list(cluster_playbook.get("next_run_moves") or []) if str(v).strip()]
    if next_run_moves:
        parts.append("Arc rewrite moves: " + "; ".join(_clip_text(v, 140) for v in next_run_moves[:3]))
    memory_context = _render_catalyst_channel_memory_context(channel_memory)
    if memory_context:
        parts.append(memory_context)
    if source_analysis:
        worked = source_analysis.get("what_worked")
        hurt = source_analysis.get("what_hurt")
        if worked:
            parts.append("What worked: " + _clip_text(str(worked), 240))
        if hurt:
            parts.append("What hurt: " + _clip_text(str(hurt), 240))
        analytics_summary = source_analysis.get("analytics_summary")
        if analytics_summary:
            parts.append("Analytics summary: " + _clip_text(str(analytics_summary), 260))
        moves = source_analysis.get("improvement_moves") or []
        if isinstance(moves, list) and moves:
            parts.append("Improvement moves: " + "; ".join(_clip_text(str(m), 120) for m in moves[:5] if str(m).strip()))
        retention_findings = source_analysis.get("retention_findings") or []
        if isinstance(retention_findings, list) and retention_findings:
            parts.append("Retention findings: " + "; ".join(_clip_text(str(m), 120) for m in retention_findings[:4] if str(m).strip()))
        packaging_findings = source_analysis.get("packaging_findings") or []
        if isinstance(packaging_findings, list) and packaging_findings:
            parts.append("Packaging findings: " + "; ".join(_clip_text(str(m), 120) for m in packaging_findings[:4] if str(m).strip()))
    if str(analytics_notes or "").strip():
        parts.append("Private analytics notes: " + _clip_text(analytics_notes, 320))
    return "\n".join(part for part in parts if part)


# â”€â”€â”€ ComfyUI Image Generation with Upscaling â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€





_active_comfyui_url = str(COMFYUI_URL or "").strip().rstrip("/")


def _comfyui_candidate_urls() -> list[str]:
    candidates: list[str] = []
    for raw in (
        _active_comfyui_url,
        COMFYUI_URL,
        "http://127.0.0.1:8188",
    ):
        base = str(raw or "").strip().rstrip("/")
        if not base:
            continue
        if base not in candidates:
            candidates.append(base)
    return candidates


_COMFYUI_OBJECT_INFO_TTL_SEC = 120
_comfyui_object_info_cache: dict[str, dict] = {}
_image_provider_fail_until: dict[str, float] = {}
_image_provider_fail_counts: dict[str, int] = {}
_image_provider_success_counts: dict[str, int] = {}
_image_provider_fallback_total = 0
_image_provider_fallback_pairs: dict[str, int] = {}
_HIDREAM_AVAILABILITY_TTL_SEC = max(5, int(os.getenv("HIDREAM_AVAILABILITY_TTL_SEC", "30") or 30))
_HIDREAM_STALE_OK_SEC = max(_HIDREAM_AVAILABILITY_TTL_SEC, int(os.getenv("HIDREAM_STALE_OK_SEC", "180") or 180))
_WAN22_T2I_AVAILABILITY_TTL_SEC = max(5, int(os.getenv("WAN22_T2I_AVAILABILITY_TTL_SEC", "30") or 30))
_WAN22_T2I_STALE_OK_SEC = max(_WAN22_T2I_AVAILABILITY_TTL_SEC, int(os.getenv("WAN22_T2I_STALE_OK_SEC", "180") or 180))
# Interactive defaults are tuned for local-only lanes (WAN/SDXL) when paid xAI fallback is disabled.
HIDREAM_INTERACTIVE_ATTEMPTS = max(1, int(os.getenv("HIDREAM_INTERACTIVE_ATTEMPTS", "1") or 1))
HIDREAM_INTERACTIVE_MAX_WAIT_SEC = max(30, int(os.getenv("HIDREAM_INTERACTIVE_MAX_WAIT_SEC", "75") or 75))
HIDREAM_REFERENCE_STRICT_DENOISE = max(0.18, min(0.75, float(os.getenv("HIDREAM_REFERENCE_STRICT_DENOISE", "0.44") or 0.44)))
HIDREAM_REFERENCE_INSPIRED_DENOISE = max(0.22, min(0.82, float(os.getenv("HIDREAM_REFERENCE_INSPIRED_DENOISE", "0.58") or 0.58)))
HIDREAM_REFERENCE_UPSCALE_METHOD = str(os.getenv("HIDREAM_REFERENCE_UPSCALE_METHOD", "lanczos") or "lanczos").strip() or "lanczos"
WAN22_INTERACTIVE_ATTEMPTS = max(1, int(os.getenv("WAN22_INTERACTIVE_ATTEMPTS", "6") or 6))
WAN22_INTERACTIVE_MAX_WAIT_SEC = max(18, int(os.getenv("WAN22_INTERACTIVE_MAX_WAIT_SEC", "35") or 35))
WAN22_INTERACTIVE_LATENT_ATTEMPTS = max(1, int(os.getenv("WAN22_INTERACTIVE_LATENT_ATTEMPTS", "4") or 4))
WAN22_INTERACTIVE_LATENT_MAX_WAIT_SEC = max(30, int(os.getenv("WAN22_INTERACTIVE_LATENT_MAX_WAIT_SEC", "70") or 70))
WAN22_PREFER_FP16 = str(os.getenv("WAN22_PREFER_FP16", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
_hidream_availability_cache: dict[str, float | bool | str] = {
    "checked_ts": 0.0,
    "ready": False,
    "last_ok_ts": 0.0,
    "last_error": "",
    "model_name": "",
    "clip_l": "",
    "clip_g": "",
    "t5_name": "",
    "llama_name": "",
    "vae_name": "",
}
_hidream_edit_availability_cache: dict[str, float | bool | str] = {
    "checked_ts": 0.0,
    "ready": False,
    "last_ok_ts": 0.0,
    "last_error": "",
    "model_name": "",
    "clip_l": "",
    "clip_g": "",
    "t5_name": "",
    "llama_name": "",
    "vae_name": "",
}
_wan22_t2i_availability_cache: dict[str, float | bool | str] = {
    "checked_ts": 0.0,
    "ready": False,
    "last_ok_ts": 0.0,
    "last_error": "",
    "mode": "",
    "ckpt_name": "",
    "unet_name": "",
}


def _extract_comfy_choice_list(raw_value) -> list[str]:
    if isinstance(raw_value, (tuple, list)):
        if len(raw_value) >= 2 and isinstance(raw_value[1], dict):
            options = raw_value[1].get("options")
            if isinstance(options, (tuple, list)):
                return [str(v) for v in options if str(v or "").strip()]
        if raw_value and isinstance(raw_value[0], (tuple, list)):
            return [str(v) for v in raw_value[0] if str(v or "").strip()]
        return [str(v) for v in raw_value if str(v or "").strip()]
    return []


def _extract_comfy_node_choices(node_info: dict, input_name: str) -> list[str]:
    if not isinstance(node_info, dict):
        return []
    input_spec = node_info.get("input", {})
    if not isinstance(input_spec, dict):
        return []
    for section in ("required", "optional"):
        section_spec = input_spec.get(section, {})
        if not isinstance(section_spec, dict):
            continue
        if input_name in section_spec:
            return _extract_comfy_choice_list(section_spec.get(input_name))
    return []


async def _get_comfyui_object_info(node_name: str | None = None) -> dict:
    global _active_comfyui_url
    cache_suffix = (node_name or "__all__").strip()
    now = time.time()
    last_error = None
    for base_url in _comfyui_candidate_urls():
        cache_key = f"{base_url}|{cache_suffix}"
        cached = _comfyui_object_info_cache.get(cache_key)
        if cached and (now - float(cached.get("ts", 0))) <= _COMFYUI_OBJECT_INFO_TTL_SEC:
            return dict(cached.get("data", {}))
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                url = f"{base_url}/object_info"
                if node_name:
                    url = f"{url}/{node_name}"
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, dict):
                    _comfyui_object_info_cache[cache_key] = {"ts": now, "data": data}
                    _active_comfyui_url = base_url
                    return data
        except Exception as e:
            last_error = e
            continue
    if last_error:
        log.warning(f"ComfyUI object_info lookup failed for '{cache_suffix}': {last_error}")
    return {}


async def _get_comfyui_node_choices(node_name: str, input_name: str) -> list[str]:
    info = await _get_comfyui_object_info(node_name)
    node_info = info.get(node_name) if isinstance(info, dict) else None
    if not isinstance(node_info, dict) and isinstance(info, dict):
        # Some ComfyUI builds return node payload directly for /object_info/<node>.
        node_info = info
    return _extract_comfy_node_choices(node_info or {}, input_name)


def _resolve_comfy_choice(preferred: str, choices: list[str], label: str) -> str:
    want = str(preferred or "").strip()
    if not choices:
        return want
    if want in choices:
        return want
    want_name = Path(want).name.lower()
    for candidate in choices:
        cand = str(candidate or "").strip()
        if cand and Path(cand).name.lower() == want_name:
            if cand != want:
                log.warning(f"{label} '{want}' missing; using matching available '{cand}'")
            return cand
    selected = str(choices[0] or "").strip() or want
    if selected != want:
        log.warning(f"{label} '{want}' missing; using available '{selected}'")
    return selected


def _normalize_image_provider_key(provider: str) -> str:
    key = str(provider or "").strip().lower()
    if key in {"hidream", "hidream-i1", "hidream_i1"}:
        return "hidream"
    if key in {"wan", "wan22", "wan2.2"}:
        return "wan22"
    if key in {"sdxl", "comfy", "comfyui", "local"}:
        return "sdxl"
    if key in {"xai", "grok", "fal"}:
        return "xai"
    return key


def _provider_cooldown_remaining(provider: str) -> float:
    key = _normalize_image_provider_key(provider)
    until = float(_image_provider_fail_until.get(key, 0.0) or 0.0)
    return max(0.0, until - time.time())


def _provider_is_available(provider: str) -> bool:
    return _provider_cooldown_remaining(provider) <= 0.0


def _provider_mark_success(provider: str) -> None:
    key = _normalize_image_provider_key(provider)
    _image_provider_success_counts[key] = int(_image_provider_success_counts.get(key, 0) or 0) + 1
    _image_provider_fail_until.pop(key, None)


def _provider_mark_failure(provider: str, reason: str = "", cooldown_sec: int | None = None) -> None:
    key = _normalize_image_provider_key(provider)
    _image_provider_fail_counts[key] = int(_image_provider_fail_counts.get(key, 0) or 0) + 1
    cool = int(IMAGE_PROVIDER_FAILURE_COOLDOWN_SEC if cooldown_sec is None else cooldown_sec)
    if cool > 0:
        _image_provider_fail_until[key] = time.time() + cool
    if reason:
        log.warning(f"Image provider '{key}' failure: {reason}")


def _record_provider_fallback(from_provider: str, to_provider: str = "next") -> None:
    global _image_provider_fallback_total
    src = _normalize_image_provider_key(from_provider)
    dst = _normalize_image_provider_key(to_provider)
    pair = f"{src}->{dst}"
    _image_provider_fallback_total += 1
    _image_provider_fallback_pairs[pair] = int(_image_provider_fallback_pairs.get(pair, 0) or 0) + 1


def _provider_cooldown_snapshot() -> dict:
    out: dict[str, int] = {}
    for key in sorted(set(list(_image_provider_fail_until.keys()) + list(_image_provider_fail_counts.keys()))):
        rem = int(round(_provider_cooldown_remaining(key)))
        if rem > 0:
            out[key] = rem
    return out


def _ensure_generated_image_valid(path: str) -> None:
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"generated image missing: {path}")
    size = int(p.stat().st_size or 0)
    if size < int(IMAGE_LOCAL_MIN_FILE_BYTES):
        raise RuntimeError(
            f"generated image too small ({size} bytes < {IMAGE_LOCAL_MIN_FILE_BYTES}): {p.name}"
        )


def _apply_skeleton_glass_shell(rgb: "Image.Image", aggressive: bool = False) -> "Image.Image":
    if Image is None or ImageFilter is None or np is None:
        return rgb
    try:
        arr = np.asarray(rgb).astype(np.float32)
        if arr.ndim != 3 or arr.shape[2] < 3:
            return rgb
        r = arr[:, :, 0]
        g = arr[:, :, 1]
        b = arr[:, :, 2]
        maxc = np.maximum(np.maximum(r, g), b)
        minc = np.minimum(np.minimum(r, g), b)
        sat = np.zeros_like(maxc, dtype=np.float32)
        np.divide((maxc - minc), np.maximum(maxc, 1e-6), out=sat, where=maxc > 1e-6)
        sat *= 255.0
        lum = 0.2126 * r + 0.7152 * g + 0.0722 * b
        lum_floor = 138.0 if aggressive else 150.0
        sat_ceiling = 112.0 if aggressive else 96.0
        neutral = (
            (np.abs(r - g) < 42.0)
            & (np.abs(g - b) < 42.0)
            & (np.abs(r - b) < 54.0)
        )
        bone_mask = ((lum > lum_floor) & (sat < sat_ceiling) & neutral).astype(np.uint8) * 255
        bone_count = int((bone_mask > 0).sum())
        if bone_count <= 2500:
            return rgb
        bone_img = Image.fromarray(bone_mask, mode="L")
        outer_soft = bone_img.filter(ImageFilter.MaxFilter(size=53 if aggressive else 43)).filter(
            ImageFilter.GaussianBlur(radius=8.5 if aggressive else 6.2)
        )
        core_soft = bone_img.filter(ImageFilter.MaxFilter(size=19 if aggressive else 13)).filter(
            ImageFilter.GaussianBlur(radius=4.4 if aggressive else 3.2)
        )
        outer_arr = np.asarray(outer_soft).astype(np.float32) / 255.0
        core_arr = np.asarray(core_soft).astype(np.float32) / 255.0
        ring_arr = np.clip(outer_arr - core_arr, 0.0, 1.0)
        core_fill = np.clip(core_arr * 0.60, 0.0, 1.0)
        not_bone = (bone_mask < 64).astype(np.float32)
        ring_gain = 0.24 if aggressive else 0.20
        fill_gain = 0.16 if aggressive else 0.12
        alpha = np.clip((ring_arr * ring_gain + core_fill * fill_gain) * not_bone, 0.0, 0.30 if aggressive else 0.25)[:, :, None]
        shell_color = np.zeros_like(arr)
        shell_color[:, :, 0] = 154.0
        shell_color[:, :, 1] = 188.0
        shell_color[:, :, 2] = 222.0
        arr = arr * (1.0 - alpha) + shell_color * alpha
        edge = outer_soft.filter(ImageFilter.FIND_EDGES).filter(ImageFilter.GaussianBlur(radius=1.2))
        edge_arr = np.asarray(edge).astype(np.float32) / 255.0
        edge_alpha = np.clip(edge_arr * 0.10, 0.0, 0.13 if aggressive else 0.11)[:, :, None]
        highlight = np.full_like(arr, 238.0)
        arr = arr * (1.0 - edge_alpha) + highlight * edge_alpha

        h = arr.shape[0]
        y = np.arange(h, dtype=np.int32)[:, None]
        yy = np.repeat(y, arr.shape[1], axis=1)
        max_eye = np.maximum(np.maximum(arr[:, :, 0], arr[:, :, 1]), arr[:, :, 2])
        min_eye = np.minimum(np.minimum(arr[:, :, 0], arr[:, :, 1]), arr[:, :, 2])
        chroma_eye = max_eye - min_eye
        blue_bias = (arr[:, :, 2] > (arr[:, :, 0] + 10.0)) & (arr[:, :, 2] > (arr[:, :, 1] + 6.0))
        yellow_bias = (arr[:, :, 0] > (arr[:, :, 1] + 8.0)) & (arr[:, :, 1] > (arr[:, :, 2] + 8.0))
        eye_glow = (
            (yy < int(h * 0.55))
            & (max_eye > 188.0)
            & ((chroma_eye > 22.0) | blue_bias | yellow_bias)
        )
        if eye_glow.any():
            eye_alpha = eye_glow.astype(np.float32)[:, :, None] * (0.74 if aggressive else 0.66)
            eye_target = np.zeros_like(arr)
            eye_target[:, :, 0] = 188.0
            eye_target[:, :, 1] = 194.0
            eye_target[:, :, 2] = 206.0
            arr = arr * (1.0 - eye_alpha) + eye_target * eye_alpha
        return Image.fromarray(np.clip(arr, 0.0, 255.0).astype(np.uint8), mode="RGB")
    except Exception:
        return rgb


def _prompt_prefers_dark_reference_scene(prompt: str) -> bool:
    return bool(
        re.search(
            r"\b(dark|night|noir|shadow|shadowy|moody|dim|dramatic|cinematic|low[- ]key|storm|rain|thunder|black background)\b",
            str(prompt or ""),
            flags=re.IGNORECASE,
        )
    )


def _prepare_skeleton_reference_conditioning_image(source_path: str, prompt: str = "") -> str:
    if Image is None:
        return source_path
    ref_dir = TEMP_DIR / "runtime_reference_inputs"
    ref_dir.mkdir(parents=True, exist_ok=True)
    out_path = ref_dir / f"hidream_ref_subject_{int(time.time() * 1000)}_{random.randint(1000, 9999)}.png"
    try:
        with Image.open(source_path) as im:
            base_rgba = im.convert("RGBA")
            base_rgb = base_rgba.convert("RGB")
            if np is None:
                out = _apply_skeleton_glass_shell(base_rgb, aggressive=True)
                out.save(out_path, format="PNG", optimize=True)
                return str(out_path)

            arr = np.asarray(base_rgb).astype(np.float32)
            h, w = arr.shape[:2]
            if h < 16 or w < 16:
                out = _apply_skeleton_glass_shell(base_rgb, aggressive=True)
                out.save(out_path, format="PNG", optimize=True)
                return str(out_path)

            border_px = max(4, min(18, max(1, min(h, w) // 18)))
            border = np.concatenate(
                [
                    arr[:border_px, :, :].reshape(-1, 3),
                    arr[-border_px:, :, :].reshape(-1, 3),
                    arr[:, :border_px, :].reshape(-1, 3),
                    arr[:, -border_px:, :].reshape(-1, 3),
                ],
                axis=0,
            )
            bg = np.median(border, axis=0)
            bg_lum = float(bg.mean())
            maxc = np.max(arr, axis=2)
            minc = np.min(arr, axis=2)
            lum = 0.2126 * arr[:, :, 0] + 0.7152 * arr[:, :, 1] + 0.0722 * arr[:, :, 2]
            sat = maxc - minc
            diff = np.max(np.abs(arr - bg[None, None, :]), axis=2)

            base_mask = (
                (diff > 20.0)
                | (lum > (bg_lum + 9.0))
                | ((sat < 65.0) & (lum > (bg_lum + 6.0)))
            )
            neutral_bone = (
                (lum > max(114.0, bg_lum + 5.0))
                & (sat < 68.0)
                & (np.abs(arr[:, :, 0] - arr[:, :, 1]) < 42.0)
                & (np.abs(arr[:, :, 1] - arr[:, :, 2]) < 42.0)
            )
            mask = (base_mask | neutral_bone).astype(np.uint8) * 255

            if cv2 is not None:
                close_kernel = np.ones((9, 9), dtype=np.uint8)
                dilate_kernel = np.ones((31, 31), dtype=np.uint8)
                mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, close_kernel, iterations=2)
                bone_u8 = (neutral_bone.astype(np.uint8) * 255)
                mask = np.maximum(mask, cv2.dilate(bone_u8, dilate_kernel, iterations=1))
                components, labels, stats, _ = cv2.connectedComponentsWithStats(mask, connectivity=8)
                if components > 1:
                    areas = stats[1:, cv2.CC_STAT_AREA]
                    largest_idx = int(1 + np.argmax(areas))
                    mask = (labels == largest_idx).astype(np.uint8) * 255
            else:
                mask_img = Image.fromarray(mask, mode="L")
                mask_img = mask_img.filter(ImageFilter.MaxFilter(size=17)).filter(ImageFilter.GaussianBlur(radius=2.2))
                bone_img = Image.fromarray((neutral_bone.astype(np.uint8) * 255), mode="L")
                bone_img = bone_img.filter(ImageFilter.MaxFilter(size=31))
                mask = np.maximum(np.asarray(mask_img), np.asarray(bone_img))

            subject_pixels = np.argwhere(mask > 20)
            if subject_pixels.size == 0:
                out = _apply_skeleton_glass_shell(base_rgb, aggressive=True)
                out.save(out_path, format="PNG", optimize=True)
                return str(out_path)

            y0, x0 = subject_pixels.min(axis=0)
            y1, x1 = subject_pixels.max(axis=0)
            span_h = max(1, int(y1 - y0 + 1))
            span_w = max(1, int(x1 - x0 + 1))
            pad_x = max(24, int(span_w * 0.18))
            pad_y = max(24, int(span_h * 0.16))
            x0 = max(0, int(x0 - pad_x))
            y0 = max(0, int(y0 - pad_y))
            x1 = min(w, int(x1 + pad_x))
            y1 = min(h, int(y1 + pad_y))

            crop_rgba = base_rgba.crop((x0, y0, x1, y1))
            crop_mask = Image.fromarray(mask, mode="L").crop((x0, y0, x1, y1))
            if ImageFilter is not None:
                crop_mask = crop_mask.filter(ImageFilter.MaxFilter(size=13)).filter(ImageFilter.GaussianBlur(radius=3.4))
            crop_rgba.putalpha(crop_mask)

            canvas_w, canvas_h = base_rgba.size
            dark_prompt = _prompt_prefers_dark_reference_scene(prompt)
            bg_rgb = (12, 16, 24) if dark_prompt else (28, 36, 46)
            canvas = Image.new("RGBA", (canvas_w, canvas_h), (*bg_rgb, 255))

            subject_w, subject_h = crop_rgba.size
            target_w = int(canvas_w * 0.54)
            target_h = int(canvas_h * 0.72)
            scale = min(
                float(target_w) / float(max(1, subject_w)),
                float(target_h) / float(max(1, subject_h)),
            )
            resized = crop_rgba.resize(
                (
                    max(1, int(round(subject_w * scale))),
                    max(1, int(round(subject_h * scale))),
                ),
                Image.LANCZOS,
            )
            paste_x = max(0, (canvas_w - resized.width) // 2)
            paste_y = max(0, int(canvas_h * 0.10))
            if paste_y + resized.height > canvas_h:
                paste_y = max(0, (canvas_h - resized.height) // 2)
            canvas.alpha_composite(resized, (paste_x, paste_y))

            conditioned = _apply_skeleton_glass_shell(canvas.convert("RGB"), aggressive=True)
            conditioned.save(out_path, format="PNG", optimize=True)
            return str(out_path)
    except Exception as e:
        log.warning(f"Skeleton reference conditioning preprocess skipped ({Path(source_path).name}): {e}")
        try:
            with Image.open(source_path) as im:
                out = _apply_skeleton_glass_shell(im.convert("RGB"), aggressive=True)
                out.save(out_path, format="PNG", optimize=True)
                return str(out_path)
        except Exception:
            return source_path


def _postprocess_generated_image(path: str, provider: str = "", template: str = "", aggressive: bool = False) -> None:
    """Light local post-process to recover detail from WAN latent stills."""
    if Image is None or ImageFilter is None:
        return
    provider_key = _normalize_image_provider_key(provider)
    p = Path(path)
    if not p.exists() or p.stat().st_size <= 0:
        return
    t = str(template or "").strip().lower()
    # Keep this narrow: only touch templates where WAN latent softness hurts UX.
    if t not in {"skeleton", "story", "motivation"}:
        return
    try:
        with Image.open(p) as im:
            rgb = im.convert("RGB")
            out = rgb
            if provider_key == "wan22":
                sharpen_percent = 260 if (t == "skeleton" and aggressive) else (240 if t == "skeleton" else 190)
                out = rgb.filter(ImageFilter.UnsharpMask(radius=2, percent=sharpen_percent, threshold=2))
                if ImageEnhance is not None:
                    if t == "skeleton":
                        out = ImageEnhance.Contrast(out).enhance(1.10 if aggressive else 1.08)
                    else:
                        out = ImageEnhance.Contrast(out).enhance(1.05)
            elif t == "skeleton" and ImageEnhance is not None:
                out = ImageEnhance.Contrast(out).enhance(1.03 if aggressive else 1.02)

            if t == "skeleton":
                out = _apply_skeleton_glass_shell(out, aggressive=aggressive or provider_key in {"hidream", "hidream-i1", "hidream_i1"})

            out.save(str(p), format="PNG", optimize=True)
    except Exception as e:
        log.warning(f"Generated image post-process skipped ({p.name}): {e}")


async def _resolve_wan22_t2i_runtime_assets() -> dict:
    ckpt_choices = await _get_comfyui_node_choices("CheckpointLoaderSimple", "ckpt_name")
    clip_choices = await _get_comfyui_node_choices("CLIPLoader", "clip_name")
    vae_choices = await _get_comfyui_node_choices("VAELoader", "vae_name")
    unet_choices = await _get_comfyui_node_choices("UNETLoader", "unet_name")

    ckpt_name = WAN22_T2I_CHECKPOINT
    if ckpt_choices and ckpt_name not in ckpt_choices:
        wan_exact = [c for c in ckpt_choices if "wan" in c.lower() and "t2i" in c.lower()]
        wan_generic = [
            c for c in ckpt_choices
            if "wan" in c.lower() and "t2v" not in c.lower() and "i2v" not in c.lower()
        ]
        selected = (wan_exact or wan_generic or [None])[0]
        if selected:
            log.warning(f"Configured WAN22 checkpoint missing; auto-using available checkpoint '{selected}'")
            ckpt_name = selected

    clip_name = WAN22_T2I_CLIP
    if clip_choices and clip_name not in clip_choices:
        clip_candidates = [c for c in clip_choices if "umt5" in c.lower() or "wan" in c.lower()]
        selected_clip = (clip_candidates or [None])[0]
        if selected_clip:
            log.warning(f"Configured WAN22 clip missing; auto-using '{selected_clip}'")
            clip_name = selected_clip
    if clip_choices and clip_name not in clip_choices:
        clip_name = str((clip_choices or [""])[0] or "")

    vae_name = WAN22_T2I_VAE
    if vae_choices and vae_name not in vae_choices:
        # Prefer the WAN 2.1 VAE when available because Comfy's Wan 2.2 low/high T2V lanes decode correctly with it.
        preferred = next((c for c in vae_choices if "wan_2.1_vae" in str(c).lower()), None)
        selected_vae = preferred or (next((c for c in vae_choices if "wan" in str(c).lower()), None))
        if selected_vae:
            log.warning(f"Configured WAN22 VAE missing; auto-using '{selected_vae}'")
            vae_name = str(selected_vae)
    if vae_choices and vae_name not in vae_choices:
        vae_name = str((vae_choices or [""])[0] or "")

    # Prefer real WAN2.2 TI2V UNET path when available.
    ti2v_unet_name = ""
    if unet_choices:
        preferred_unets: list[str] = []
        for candidate in [WAN22_T2I_UNET, WAN22_T2I_UNET_FP8]:
            c = str(candidate or "").strip()
            if c:
                preferred_unets.append(c)
        for preferred_unet in preferred_unets:
            if preferred_unet in unet_choices:
                ti2v_unet_name = preferred_unet
                break
        if not ti2v_unet_name:
            ti2v_pool = [str(u) for u in unet_choices if "wan2.2" in str(u).lower() and "ti2v" in str(u).lower()]
            fp16_ti2v = next((u for u in ti2v_pool if "fp16" in u.lower()), "")
            fp8_ti2v = next((u for u in ti2v_pool if "fp8" in u.lower()), "")
            ti2v_unet_name = fp16_ti2v or fp8_ti2v or (ti2v_pool[0] if ti2v_pool else "")

    # Latent-mode fallback should prefer WAN 2.1 VAE if present.
    # WAN 2.2 VAE can fail decode for 16-channel latents in some ComfyUI Wan graph paths.
    latent_vae_name = str(vae_name or "")
    if vae_choices:
        wan21_vae = next(
            (
                str(c)
                for c in vae_choices
                if "wan_2.1_vae" in str(c).lower()
                or "wan2.1_vae" in str(c).lower()
                or ("wan" in str(c).lower() and "2.1" in str(c).lower())
            ),
            "",
        )
        non22_wan_vae = next(
            (str(c) for c in vae_choices if "wan" in str(c).lower() and "2.2" not in str(c).lower()),
            "",
        )
        any_wan_vae = next((str(c) for c in vae_choices if "wan" in str(c).lower()), "")
        latent_vae_name = wan21_vae or non22_wan_vae or any_wan_vae or latent_vae_name

    low_unet_name = ""
    high_unet_name = ""

    def _pick_wan22_t2v_noise(kind: str) -> str:
        pool = [
            str(u) for u in unet_choices
            if "wan2.2" in str(u).lower() and "t2v" in str(u).lower() and f"{kind}_noise" in str(u).lower()
        ]
        if not pool:
            return ""
        # Prefer fp16 for best quality; fallback to fp8-scaled when fp16 is unavailable.
        fp16 = next((u for u in pool if "fp16" in u.lower()), "")
        if fp16:
            return fp16
        fp8 = next((u for u in pool if "fp8" in u.lower()), "")
        return fp8 or pool[0]

    low_unet_name = _pick_wan22_t2v_noise("low")
    high_unet_name = _pick_wan22_t2v_noise("high")
    if not low_unet_name:
        low_unet_name = next((str(u) for u in unet_choices if "wan" in str(u).lower() and "t2v" in str(u).lower() and "low" in str(u).lower()), "")
    if not high_unet_name:
        high_unet_name = next((str(u) for u in unet_choices if "wan" in str(u).lower() and "t2v" in str(u).lower() and "high" in str(u).lower()), "")

    if ti2v_unet_name and clip_name and vae_name:
        return {
            "mode": "ti2v_unet",
            "ckpt_name": "",
            "clip_name": clip_name,
            "vae_name": vae_name,
            "unet_name": ti2v_unet_name,
            "low_unet_name": "",
            "high_unet_name": "",
        }

    if ckpt_choices and ckpt_name in ckpt_choices:
        return {
            "mode": "checkpoint",
            "ckpt_name": ckpt_name,
            "clip_name": clip_name,
            "vae_name": vae_name,
            "unet_name": "",
            "low_unet_name": "",
            "high_unet_name": "",
        }

    # Fallback local mode: synthesize a still via Wan 2.2 low/high T2V lanes.
    if clip_name and vae_name and low_unet_name and high_unet_name:
        if latent_vae_name and latent_vae_name != vae_name:
            log.warning(
                f"WAN latent fallback forcing compatible VAE '{latent_vae_name}' "
                f"(configured/default was '{vae_name}')"
            )
        log.warning(
            "WAN22 checkpoint missing; using local WAN latent fallback "
            f"(low='{low_unet_name}', high='{high_unet_name}', vae='{latent_vae_name or vae_name}')"
        )
        return {
            "mode": "latent",
            "ckpt_name": "",
            "clip_name": clip_name,
            "vae_name": latent_vae_name or vae_name,
            "unet_name": "",
            "low_unet_name": low_unet_name,
            "high_unet_name": high_unet_name,
        }

    if ckpt_choices and ckpt_name not in ckpt_choices:
        wan_unets = [u for u in unet_choices if "wan" in str(u).lower()]
        ckpt_preview = ", ".join(ckpt_choices[:6]) or "none"
        unet_preview = ", ".join(wan_unets[:6]) or "none"
        raise RuntimeError(
            "WAN22 text-to-image checkpoint not found in ComfyUI checkpoints. "
            f"Configured='{WAN22_T2I_CHECKPOINT}'. Available checkpoints: {ckpt_preview}. "
            f"Detected WAN UNETs (video models): {unet_preview}. "
            f"Expected WAN UNET names include '{WAN22_T2I_UNET}' or '{WAN22_T2I_UNET_FP8}' "
            f"(preferred TI2V) or '{WAN22_T2V_HIGH}'/'{WAN22_T2V_LOW}' (fallback split T2V)."
        )

    raise RuntimeError("WAN22 text-to-image assets unavailable on ComfyUI")


async def _resolve_hidream_runtime_assets_for_model(
    model_hint: str,
    *,
    enabled: bool,
    label: str,
    weight_dtype: str = "default",
) -> dict:
    if not enabled:
        raise RuntimeError(f"{label} is disabled by configuration")
    unet_choices = await _get_comfyui_node_choices("UNETLoader", "unet_name")
    clip_choices = await _get_comfyui_node_choices("QuadrupleCLIPLoader", "clip_name1")
    vae_choices = await _get_comfyui_node_choices("VAELoader", "vae_name")

    model_name = _resolve_comfy_choice(model_hint, unet_choices, label)
    clip_l_name = _resolve_comfy_choice(HIDREAM_CLIP_L, clip_choices, "HiDream clip_l")
    clip_g_name = _resolve_comfy_choice(HIDREAM_CLIP_G, clip_choices, "HiDream clip_g")
    t5_name = _resolve_comfy_choice(HIDREAM_T5, clip_choices, "HiDream t5xxl")
    llama_name = _resolve_comfy_choice(HIDREAM_LLAMA, clip_choices, "HiDream llama")
    vae_name = _resolve_comfy_choice(HIDREAM_VAE, vae_choices, "HiDream VAE")

    missing: list[str] = []
    if not model_name:
        missing.append("diffusion model")
    if not clip_l_name:
        missing.append("clip_l")
    if not clip_g_name:
        missing.append("clip_g")
    if not t5_name:
        missing.append("t5xxl")
    if not llama_name:
        missing.append("llama")
    if not vae_name:
        missing.append("vae")
    if missing:
        raise RuntimeError(f"{label} assets unavailable on ComfyUI: missing " + ", ".join(missing))

    return {
        "model_name": model_name,
        "weight_dtype": str(weight_dtype or "default"),
        "clip_l_name": clip_l_name,
        "clip_g_name": clip_g_name,
        "t5_name": t5_name,
        "llama_name": llama_name,
        "vae_name": vae_name,
    }


async def _resolve_hidream_runtime_assets() -> dict:
    return await _resolve_hidream_runtime_assets_for_model(
        HIDREAM_MODEL,
        enabled=HIDREAM_ENABLED,
        label="HiDream I1 diffusion model",
        weight_dtype="default",
    )


async def _resolve_hidream_edit_runtime_assets() -> dict:
    return await _resolve_hidream_runtime_assets_for_model(
        HIDREAM_EDIT_MODEL,
        enabled=HIDREAM_EDIT_ENABLED,
        label="HiDream E1.1 diffusion model",
        weight_dtype=HIDREAM_EDIT_WEIGHT_DTYPE,
    )


def _normalize_template_adapter_route(route: dict | None) -> dict:
    raw = route if isinstance(route, dict) else {}
    loras: list[dict] = []
    for item in list(raw.get("loras", []) or []):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "") or "").strip()
        if not name:
            continue
        try:
            strength_model = float(item.get("strength_model", 0.7) or 0.7)
        except Exception:
            strength_model = 0.7
        try:
            strength_clip = float(item.get("strength_clip", strength_model) or strength_model)
        except Exception:
            strength_clip = strength_model
        loras.append(
            {
                "name": name,
                "strength_model": max(-2.0, min(2.0, strength_model)),
                "strength_clip": max(-2.0, min(2.0, strength_clip)),
            }
        )
    return {
        "enabled": bool(raw.get("enabled", True)),
        "checkpoint": str(raw.get("checkpoint", "sd_xl_base_1.0.safetensors") or "sd_xl_base_1.0.safetensors").strip(),
        "prepend_trigger": str(raw.get("prepend_trigger", "") or "").strip(),
        "prompt_suffix": str(raw.get("prompt_suffix", "") or "").strip(),
        "negative_suffix": str(raw.get("negative_suffix", "") or "").strip(),
        "apply_prompt_globally": bool(raw.get("apply_prompt_globally", True)),
        "loras": loras,
    }


def _resolve_template_adapter_route(template: str, override_route: dict | None = None) -> dict:
    base = _normalize_template_adapter_route((TEMPLATE_ADAPTER_ROUTING or {}).get("default", {}))
    if not TEMPLATE_ADAPTER_ROUTING_ENABLED:
        base["enabled"] = False
        base["loras"] = []
    key = str(template or "").strip().lower()
    raw = None
    if isinstance(override_route, dict):
        raw = override_route
    elif key:
        raw = (TEMPLATE_ADAPTER_ROUTING or {}).get(key)
    if isinstance(raw, dict):
        overlay = _normalize_template_adapter_route(raw)
        merged = dict(base)
        raw_keys = set(str(k) for k in raw.keys())
        for field in ("enabled", "checkpoint", "prepend_trigger", "prompt_suffix", "negative_suffix", "apply_prompt_globally"):
            if field not in raw_keys:
                continue
            value = overlay.get(field)
            if isinstance(value, bool) or isinstance(value, str):
                merged[field] = value
        if "loras" in raw_keys:
            merged["loras"] = list(overlay.get("loras", []))
        return merged
    return base


def _apply_template_prompt_route(
    template: str,
    prompt: str,
    negative_prompt: str = "",
    provider: str = "",
    adapter_route: dict | None = None,
) -> tuple[str, str, dict]:
    route = _resolve_template_adapter_route(template, override_route=adapter_route)
    out_prompt = str(prompt or "").strip()
    out_negative = str(negative_prompt or "").strip()
    if not route.get("enabled", True):
        return out_prompt, out_negative, route
    if route.get("apply_prompt_globally", True):
        trigger = str(route.get("prepend_trigger", "") or "").strip()
        if trigger and trigger.lower() not in out_prompt.lower():
            out_prompt = f"{trigger}, {out_prompt}" if out_prompt else trigger
        suffix = str(route.get("prompt_suffix", "") or "").strip()
        if suffix:
            out_prompt = f"{out_prompt} {suffix}".strip()
        neg_suffix = str(route.get("negative_suffix", "") or "").strip()
        if neg_suffix:
            out_negative = f"{out_negative}, {neg_suffix}".strip(", ").strip()
    return out_prompt, out_negative, route


async def _resolve_checkpoint_name(preferred: str, fallback: str = "sd_xl_base_1.0.safetensors") -> str:
    want = str(preferred or "").strip() or fallback
    choices = await _get_comfyui_node_choices("CheckpointLoaderSimple", "ckpt_name")
    if not choices:
        return want
    if want in choices:
        return want
    if fallback in choices:
        log.warning(f"Checkpoint '{want}' missing; using fallback '{fallback}'")
        return fallback
    selected = str(choices[0] or "").strip() or want
    log.warning(f"Checkpoint '{want}' missing; using available '{selected}'")
    return selected


async def _resolve_available_loras(loras: list[dict]) -> list[dict]:
    if not loras:
        return []
    names = await _get_comfyui_node_choices("LoraLoader", "lora_name")
    if not names:
        if loras:
            log.warning("ComfyUI LoraLoader has no available LoRA entries; skipping configured adapter LoRAs")
        return []
    available = set(str(n or "").strip() for n in names)
    resolved: list[dict] = []
    for lora in loras:
        name = str((lora or {}).get("name", "") or "").strip()
        if not name:
            continue
        if name in available:
            resolved.append(lora)
        else:
            log.warning(f"Adapter LoRA not found on ComfyUI, skipping: {name}")
    return resolved






async def _run_comfyui_workflow(
    workflow: dict,
    output_node: str,
    output_type: str = "images",
    max_wait_sec: int = 900,
    poll_interval_sec: float = 2.0,
) -> dict:
    """Submit a workflow to ComfyUI and wait for the specified output node to complete."""
    global _active_comfyui_url
    last_error = None
    for base_url in _comfyui_candidate_urls():
        try:
            client_timeout = max(20, min(900, int(max_wait_sec) + 30))
            async with httpx.AsyncClient(timeout=client_timeout) as client:
                resp = await client.post(f"{base_url}/prompt", json={"prompt": workflow})
                if resp.status_code != 200:
                    if resp.status_code in (404, 502, 503, 504):
                        raise RuntimeError(f"ComfyUI endpoint unavailable ({resp.status_code}) at {base_url}")
                    log.error(f"ComfyUI rejected workflow ({resp.status_code}) on {base_url}: {resp.text[:1000]}")
                    resp.raise_for_status()
                prompt_id = resp.json()["prompt_id"]
                _active_comfyui_url = base_url

                wait_s = max(6, int(max_wait_sec))
                poll_s = max(0.5, float(poll_interval_sec))
                max_polls = max(1, int(wait_s / poll_s))
                for poll_i in range(max_polls):
                    await asyncio.sleep(poll_s)
                    history = await client.get(f"{base_url}/history/{prompt_id}")
                    history.raise_for_status()
                    hist_data = history.json()
                    if prompt_id in hist_data:
                        outputs = hist_data[prompt_id].get("outputs", {})
                        if output_node in outputs:
                            node_out = outputs[output_node]
                            if node_out.get(output_type):
                                return node_out
                            for key in ("videos", "gifs", "images"):
                                if node_out.get(key):
                                    return node_out
                        status = hist_data[prompt_id].get("status", {})
                        if status.get("status_str") == "error":
                            raise RuntimeError(f"ComfyUI workflow error: {status.get('messages', 'unknown')}")
                        elapsed_s = int((poll_i + 1) * poll_s)
                        if poll_i > 0 and elapsed_s % 30 == 0:
                            log.info(f"ComfyUI workflow still running on {base_url}... {elapsed_s}s elapsed")
                raise TimeoutError(f"ComfyUI workflow timed out after {wait_s}s on {base_url}")
        except Exception as e:
            if isinstance(e, TimeoutError):
                last_error = e
                log.warning(f"ComfyUI endpoint timed out ({base_url}): {e}")
                break
            last_error = e
            log.warning(f"ComfyUI endpoint failed ({base_url}): {e}")
            continue
    raise RuntimeError(f"All ComfyUI endpoints failed: {last_error}")


async def _download_comfyui_file(file_info: dict, output_path: str):
    """Download a generated file (image or video frame) from ComfyUI."""
    global _active_comfyui_url
    filename = file_info["filename"]
    subfolder = file_info.get("subfolder", "")
    ftype = file_info.get("type", "output")
    last_error = None
    for base_url in _comfyui_candidate_urls():
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                url = f"{base_url}/view?filename={filename}&subfolder={subfolder}&type={ftype}"
                resp = await client.get(url)
                resp.raise_for_status()
                with open(output_path, "wb") as f:
                    f.write(resp.content)
                _active_comfyui_url = base_url
                return
        except Exception as e:
            last_error = e
            continue
    raise RuntimeError(f"ComfyUI file download failed for {filename}: {last_error}")


def _fal_aspect_ratio_for_resolution(resolution: str) -> str:
    raw = str(resolution or "").strip().lower()
    return "16:9" if "landscape" in raw else "9:16"


def _fal_image_size_for_creative_model(resolution: str) -> str:
    raw = str(resolution or "").strip().lower()
    if "landscape" in raw:
        return "landscape_16_9"
    return "portrait_16_9"


def _creative_model_prompt(prompt: str, negative_prompt: str = "") -> str:
    prompt_text = str(prompt or "").strip()
    negative_text = str(negative_prompt or "").strip()
    if not negative_text:
        return prompt_text
    return f"{prompt_text}\n\nAvoid: {negative_text}"


async def _write_fal_image_result_to_path(image_value: str, output_path: str) -> str | None:
    raw = str(image_value or "").strip()
    if not raw:
        raise RuntimeError("fal image result did not contain a usable image value")
    if raw.startswith("data:image/"):
        payload = raw.split(",", 1)[1] if "," in raw else ""
        if not payload:
            raise RuntimeError("fal image data URI was empty")
        Path(output_path).write_bytes(base64.b64decode(payload))
        return None
    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        img_resp = await client.get(raw)
        if img_resp.status_code != 200:
            raise RuntimeError(f"Failed to download fal image ({img_resp.status_code})")
        Path(output_path).write_bytes(img_resp.content)
    return raw


def _fal_image_model_url(endpoint_id: str) -> str:
    return "https://fal.run/" + endpoint_id.strip("/")


def _build_fal_image_model_payload(model_id: str, prompt: str, resolution: str) -> dict:
    payload: dict[str, object] = {
        "prompt": prompt,
        "sync_mode": True,
    }
    aspect_ratio = _fal_aspect_ratio_for_resolution(resolution)
    image_size = _fal_image_size_for_creative_model(resolution)
    if model_id in {"imagen4_fast", "imagen4_ultra"}:
        payload["aspect_ratio"] = aspect_ratio
        payload["output_format"] = "png"
    elif model_id == "flux_2_pro":
        payload["image_size"] = image_size
        payload["output_format"] = "png"
    elif model_id == "seedream45":
        payload["image_size"] = image_size
        payload["output_format"] = "png"
    elif model_id in {"recraft_v4", "recraft_v4_pro"}:
        payload["style"] = "realistic_image"
    elif model_id == "nano_banana_pro":
        payload["output_format"] = "png"
    return payload


async def _generate_image_fal_selected_model(
    model_id: str,
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    negative_prompt: str = "",
    reference_image_url: str = "",
) -> dict:
    profile = _creative_image_model_profile(model_id)
    endpoint_id = str(profile.get("fal_endpoint_id", "") or "").strip()
    if not endpoint_id:
        raise RuntimeError(f"Image model '{model_id}' has no fal endpoint configured")
    if not FAL_AI_KEY:
        raise RuntimeError("FAL_AI_KEY not configured")

    payload = _build_fal_image_model_payload(
        str(profile.get("id", "") or model_id),
        _creative_model_prompt(prompt, negative_prompt=negative_prompt),
        resolution,
    )
    if str(profile.get("id", "") or "") == "grok_imagine" and reference_image_url:
        payload["image_url"] = reference_image_url

    headers = {
        "Authorization": "Key " + FAL_AI_KEY,
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=180) as client:
        resp = await client.post(_fal_image_model_url(endpoint_id), headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"{profile.get('label', model_id)} via fal.ai failed ({resp.status_code}): {resp.text[:300]}"
            )
        data = resp.json()
    images = data.get("images", []) if isinstance(data, dict) else []
    if not images:
        raise RuntimeError(f"{profile.get('label', model_id)} returned no images")
    first = images[0] or {}
    image_value = str(first.get("url") or first.get("data") or "").strip()
    cdn_url = await _write_fal_image_result_to_path(image_value, output_path)
    log.info(
        f"Fal selected image saved via {profile.get('label', model_id)}: {output_path} "
        f"({Path(output_path).stat().st_size / 1024:.0f} KB)"
    )
    gen_id = await _save_training_candidate(prompt, output_path, source=f"fal_{model_id}")
    return {
        "local_path": output_path,
        "cdn_url": cdn_url,
        "generation_id": gen_id,
        "provider": model_id,
        "provider_label": str(profile.get("label", model_id) or model_id),
    }


GROK_IMAGINE_URL = "https://fal.run/xai/grok-imagine-image"
FAL_FLUX_SCHNELL_URL = "https://fal.run/fal-ai/flux/schnell"


def _normalize_fal_image_backup_model(value: str | None) -> str:
    key = str(value or "").strip().lower()
    if key in {"grok", "grok_imagine", "grok-imagine", "xai", "xai_grok"}:
        return "grok_imagine"
    if key in {"flux", "flux_schnell", "flux-schnell", "flux1_schnell", "flux.1-schnell"}:
        return "flux_schnell"
    return "flux_schnell"


def _fal_image_size_for_resolution(resolution: str) -> str:
    raw = str(resolution or "").strip().lower()
    if raw.endswith("_landscape"):
        return "landscape_16_9"
    if raw == "1080p":
        return "portrait_16_9"
    return "portrait_16_9"


async def _generate_image_fal_flux_schnell(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
) -> dict:
    if not FAL_AI_KEY:
        raise RuntimeError("FAL_AI_KEY not configured")
    headers = {
        "Authorization": "Key " + FAL_AI_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "prompt": str(prompt or "").strip(),
        "image_size": _fal_image_size_for_resolution(resolution),
        "num_inference_steps": 4,
        "num_images": 1,
        "guidance_scale": 3.5,
        "output_format": "png",
        "enable_safety_checker": True,
    }
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(FAL_FLUX_SCHNELL_URL, headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError("FLUX schnell via fal.ai failed (" + str(resp.status_code) + "): " + resp.text[:300])
        data = resp.json()
    images = data.get("images", []) if isinstance(data, dict) else []
    if not images:
        raise RuntimeError("FLUX schnell returned no images")
    cdn_url = str((images[0] or {}).get("url", "") or "")
    if not cdn_url:
        raise RuntimeError("FLUX schnell returned no image URL")
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        img_resp = await client.get(cdn_url)
        if img_resp.status_code != 200:
            raise RuntimeError("Failed to download FLUX schnell image")
        with open(output_path, "wb") as f:
            f.write(img_resp.content)
    log.info(f"FLUX schnell (fal.ai) saved: {output_path} ({Path(output_path).stat().st_size / 1024:.0f} KB)")
    gen_id = await _save_training_candidate(prompt, output_path, source="fal_flux_schnell")
    return {"local_path": output_path, "cdn_url": cdn_url, "generation_id": gen_id}


_pending_training: dict[str, dict] = {}

async def _save_training_candidate(prompt: str, image_path: str, template: str = "", source: str = "grok", metadata: Optional[dict] = None) -> str:
    """Stage a prompt+image pair as a training candidate. Returns generation_id.
    Image is saved immediately but only promoted to 'accepted' via feedback."""
    gen_id = f"gen_{int(time.time() * 1000)}_{id(image_path) % 9999:04d}"
    try:
        img_dest = TRAINING_DATA_DIR / f"{gen_id}.png"
        txt_dest = TRAINING_DATA_DIR / f"{gen_id}.txt"
        shutil.copy2(image_path, str(img_dest))
        txt_dest.write_text(prompt, encoding="utf-8")
        _pending_training[gen_id] = {
            "prompt": prompt,
            "image_path": str(img_dest),
            "txt_path": str(txt_dest),
            "template": template,
            "source": source,
            "status": "pending",
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "metadata": metadata or {},
        }
        log.info(f"Training candidate staged: {gen_id} ({template or 'generic'}/{source})")
    except Exception as e:
        log.warning(f"Training candidate save failed (non-fatal): {e}")
    return gen_id


async def _sync_training_feedback_to_runpod(gen_id: str, entry: dict, status: str):
    """Sync accepted/rejected training examples to RunPod for continuous dataset growth."""
    if not RUNPOD_IMAGE_FEEDBACK_ENABLED:
        return
    image_path = entry.get("image_path", "")
    txt_path = entry.get("txt_path", "")
    if not image_path or not txt_path or not Path(image_path).exists() or not Path(txt_path).exists():
        return
    try:
        remote_root = RUNPOD_IMAGE_FEEDBACK_BASE_DIR.rstrip("/")
        remote_img_dir = f"{remote_root}/{status}/images"
        remote_txt_dir = f"{remote_root}/{status}/prompts"
        remote_meta_dir = f"{remote_root}/{status}/metadata"
        mkdir_cmd = (
            f"{RUNPOD_IMAGE_FEEDBACK_SSH} "
            f"'mkdir -p {remote_img_dir} {remote_txt_dir} {remote_meta_dir}'"
        )
        proc_mkdir = await asyncio.create_subprocess_shell(
            mkdir_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await proc_mkdir.communicate()
        if proc_mkdir.returncode != 0:
            log.warning(f"RunPod training mkdir failed for {gen_id}")
            return

        img_ext = Path(image_path).suffix.lower() or ".png"
        remote_img = f"{remote_img_dir}/{gen_id}{img_ext}"
        remote_txt = f"{remote_txt_dir}/{gen_id}.txt"
        scp_img_cmd = f"scp -o StrictHostKeyChecking=no -P 22092 \"{image_path}\" root@69.30.85.41:{remote_img}"
        scp_txt_cmd = f"scp -o StrictHostKeyChecking=no -P 22092 \"{txt_path}\" root@69.30.85.41:{remote_txt}"
        proc_img = await asyncio.create_subprocess_shell(
            scp_img_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, img_err = await proc_img.communicate()
        if proc_img.returncode != 0:
            log.warning(f"RunPod image sync failed for {gen_id}: {img_err.decode()[:200]}")
            return
        proc_txt = await asyncio.create_subprocess_shell(
            scp_txt_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, txt_err = await proc_txt.communicate()
        if proc_txt.returncode != 0:
            log.warning(f"RunPod prompt sync failed for {gen_id}: {txt_err.decode()[:200]}")
            return

        meta = {
            "generation_id": gen_id,
            "status": status,
            "template": entry.get("template", ""),
            "source": entry.get("source", ""),
            "created_at": entry.get("created_at", ""),
            "feedback_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "metadata": entry.get("metadata", {}),
        }
        meta_path = TEMP_DIR / f"{gen_id}_feedback.json"
        meta_path.write_text(json.dumps(meta, ensure_ascii=True), encoding="utf-8")
        try:
            remote_meta = f"{remote_meta_dir}/{gen_id}.json"
            scp_meta_cmd = f"scp -o StrictHostKeyChecking=no -P 22092 \"{str(meta_path)}\" root@69.30.85.41:{remote_meta}"
            proc_meta = await asyncio.create_subprocess_shell(
                scp_meta_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            _, meta_err = await proc_meta.communicate()
            if proc_meta.returncode != 0:
                log.warning(f"RunPod metadata sync failed for {gen_id}: {meta_err.decode()[:200]}")
        finally:
            meta_path.unlink(missing_ok=True)

        log.info(f"RunPod training sync complete: {gen_id} [{status}]")
    except Exception as e:
        log.warning(f"RunPod training feedback sync error for {gen_id}: {e}")


async def _mark_training_feedback(gen_id: str, accepted: bool, user_id: str = "", event: str = ""):
    """Mark a training candidate as accepted or rejected.
    Accepted pairs are logged to Supabase for LoRA training.
    Rejected pairs are cleaned up from disk."""
    entry = _pending_training.get(gen_id)
    if not entry:
        return
    status = "accepted" if accepted else "rejected"
    entry["status"] = status
    if user_id:
        entry.setdefault("metadata", {})["user_id"] = user_id
    if event:
        entry.setdefault("metadata", {})["event"] = event

    if not accepted:
        if entry.get("source") == "thumbnail_ai":
            try:
                img_path = entry.get("image_path", "")
                if img_path and Path(img_path).exists():
                    reject_dir = "/workspace/thumbnail_training/rejected"
                    ext = Path(img_path).suffix.lower() or ".png"
                    remote_path = f"{reject_dir}/{gen_id}{ext}"
                    ok, err = await asyncio.to_thread(_sync_file_to_runpod_blocking, img_path, remote_path)
                    if not ok:
                        log.warning(f"Thumbnail reject sync file failed for {gen_id}: {err}")
            except Exception as e:
                log.warning(f"Thumbnail reject sync failed for {gen_id}: {e}")
        await _sync_training_feedback_to_runpod(gen_id, entry, status="rejected")
        for p in [entry.get("image_path"), entry.get("txt_path")]:
            try:
                Path(p).unlink(missing_ok=True)
            except Exception:
                pass
        _pending_training.pop(gen_id, None)
        log.info(f"Training candidate {gen_id} REJECTED and cleaned up")
        return

    if SUPABASE_URL and SUPABASE_ANON_KEY:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(
                    f"{SUPABASE_URL}/rest/v1/training_data",
                    headers={
                        "apikey": SUPABASE_ANON_KEY,
                        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                        "Content-Type": "application/json",
                        "Prefer": "return=minimal",
                    },
                    json={
                        "prompt": entry["prompt"][:2000],
                        "image_filename": gen_id + ".png",
                        "template": entry.get("template", ""),
                        "source": entry.get("source", "grok"),
                        "status": "accepted",
                        "created_at": entry["created_at"],
                    },
                )
        except Exception as e:
            log.warning(f"Supabase training log failed (non-fatal): {e}")
    await _sync_training_feedback_to_runpod(gen_id, entry, status="accepted")
    if entry.get("source") == "thumbnail_ai":
        try:
            img_path = entry.get("image_path", "")
            if img_path and Path(img_path).exists():
                ext = Path(img_path).suffix.lower() or ".png"
                remote_path = f"{RUNPOD_TRAINING_DIR}/{gen_id}{ext}"
                ok, err = await asyncio.to_thread(_sync_file_to_runpod_blocking, img_path, remote_path)
                if not ok:
                    log.warning(f"Thumbnail accept sync file failed for {gen_id}: {err}")
        except Exception as e:
            log.warning(f"Thumbnail accept sync failed for {gen_id}: {e}")
    log.info(f"Training candidate {gen_id} ACCEPTED -> training dataset")


def _file_to_data_image_url(image_path: str, max_bytes: int = 8 * 1024 * 1024) -> str:
    """Encode a local image file as a data URL for xAI image reference conditioning."""
    p = Path(image_path)
    if not p.exists() or p.stat().st_size == 0:
        return ""
    file_size = p.stat().st_size
    if file_size > max_bytes:
        log.warning(f"Skipping data URI encode for {p.name}: {file_size} bytes exceeds limit {max_bytes}")
        return ""
    ext = p.suffix.lower()
    mime = "image/png" if ext == ".png" else ("image/webp" if ext == ".webp" else "image/jpeg")
    b64 = base64.b64encode(p.read_bytes()).decode("ascii")
    return f"data:{mime};base64,{b64}"


async def _generate_image_xai_direct(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    reference_image_url: str = "",
    reference_lock_mode: str = "strict",
) -> dict:
    """Generate image directly via xAI API. No fal.ai needed.
    Returns {"local_path": str, "cdn_url": str}.
    """
    if not XAI_API_KEY:
        raise RuntimeError("XAI_API_KEY not configured")

    headers = {"Authorization": f"Bearer {XAI_API_KEY}", "Content-Type": "application/json"}
    aspect = "16:9" if str(resolution or "").strip().lower().endswith("_landscape") else XAI_IMAGE_ASPECT_RATIO
    payload = {
        "model": XAI_IMAGE_MODEL,
        "prompt": prompt,
        "n": 1,
        "response_format": "url",
        "aspect_ratio": aspect,
        "resolution": XAI_IMAGE_RESOLUTION,
    }
    if reference_image_url:
        payload["image_url"] = reference_image_url
        log.info(f"xAI direct image conditioning enabled: {'https_url' if reference_image_url.startswith('http') else 'inline_data_url'}")

    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                resp = await client.post("https://api.x.ai/v1/images/generations", headers=headers, json=payload)
                if resp.status_code in (200, 201):
                    data = resp.json().get("data", [])
                    if data and data[0].get("url"):
                        cdn_url = data[0]["url"]
                        dl = await client.get(cdn_url, follow_redirects=True)
                        if dl.status_code == 200:
                            with open(output_path, "wb") as f:
                                f.write(dl.content)
                            log.info(f"xAI direct image saved: {output_path} ({Path(output_path).stat().st_size / 1024:.0f} KB)")
                            gen_id = await _save_training_candidate(prompt, output_path, source="xai_direct")
                            return {"local_path": output_path, "cdn_url": cdn_url, "generation_id": gen_id}
                if resp.status_code in (429, 500, 502, 503, 504):
                    wait = (attempt + 1) * 5
                    log.warning(f"xAI image gen attempt {attempt+1} got {resp.status_code}, retrying in {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                # In strict mode, never silently drop identity lock.
                if reference_image_url and resp.status_code in (400, 404, 422):
                    if _normalize_reference_lock_mode(reference_lock_mode) == "strict":
                        raise RuntimeError(
                            f"xAI rejected strict reference image payload ({resp.status_code}); "
                            "strict lock cannot continue without reference conditioning"
                        )
                    payload.pop("image_url", None)
                    reference_image_url = ""
                    log.warning("xAI image reference payload rejected; retrying without reference image")
                    continue
                raise RuntimeError(f"xAI image gen failed ({resp.status_code}): {resp.text[:200]}")
        except RuntimeError:
            raise
        except Exception as e:
            log.warning(f"xAI image gen attempt {attempt+1} error: {e}")
            await asyncio.sleep((attempt + 1) * 3)

    raise RuntimeError("xAI direct image generation failed after retries")


async def generate_image_grok(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    reference_image_url: str = "",
    reference_lock_mode: str = "strict",
) -> dict:
    """Generate an image using the configured remote fallback lane.
    Returns {"local_path": str, "cdn_url": str} so animation backends can reuse the CDN URL directly.
    """
    fal_model = _normalize_fal_image_backup_model(FAL_IMAGE_BACKUP_MODEL)
    if FAL_AI_KEY:
        try:
            if reference_image_url:
                fal_model = "grok_imagine"
            if fal_model == "flux_schnell":
                return await _generate_image_fal_flux_schnell(
                    prompt,
                    output_path,
                    resolution=resolution,
                )
            aspect = "16:9" if str(resolution or "").strip().lower().endswith("_landscape") else "9:16"
            headers = {
                "Authorization": "Key " + FAL_AI_KEY,
                "Content-Type": "application/json",
            }
            payload = {
                "prompt": prompt,
                "num_images": 1,
                "aspect_ratio": aspect,
                "output_format": "png",
            }
            if reference_image_url:
                payload["image_url"] = reference_image_url
                log.info(
                    f"Fal Grok image conditioning enabled: "
                    f"{'https_url' if reference_image_url.startswith('http') else 'inline_data_url'}"
                )

            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(GROK_IMAGINE_URL, headers=headers, json=payload)
                if resp.status_code not in (200, 201):
                    raise RuntimeError("Grok Imagine via fal.ai failed (" + str(resp.status_code) + "): " + resp.text[:300])
                data = resp.json()

            images = data.get("images", [])
            if not images:
                raise RuntimeError("Grok Imagine returned no images")

            cdn_url = images[0].get("url", "")
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
                img_resp = await client.get(cdn_url)
                if img_resp.status_code != 200:
                    raise RuntimeError("Failed to download Grok image")
                with open(output_path, "wb") as f:
                    f.write(img_resp.content)

            log.info(
                f"Fal remote image saved via {fal_model}: {output_path} "
                f"({Path(output_path).stat().st_size / 1024:.0f} KB)"
            )
            gen_id = await _save_training_candidate(prompt, output_path, source=f"fal_{fal_model}")
            return {"local_path": output_path, "cdn_url": cdn_url, "generation_id": gen_id}
        except Exception as e:
            log.warning(f"Fal remote image fallback failed ({fal_model}), falling back to direct xAI: {e}")

    if not XAI_API_KEY:
        raise RuntimeError("No remote image fallback configured (Fal and xAI unavailable)")
    log.info(f"Using direct xAI API image generation model={XAI_IMAGE_MODEL}")
    return await _generate_image_xai_direct(
        prompt,
        output_path,
        resolution=resolution,
        reference_image_url=reference_image_url,
        reference_lock_mode=reference_lock_mode,
    )


SKELETON_LORA_CANDIDATES = [
    "nyptid_skeleton_base_identity_v2.safetensors",
    "nyptid_skeleton_glass_v1.safetensors",
]
SKELETON_LORA_STRENGTH = 0.72
SKELETON_TRIGGER_TOKEN = "nyptid_skeleton_glass"
SKELETON_LORA_NEGATIVE = "blurry, low quality, text, watermark, deformed, ugly, bad anatomy, non-skeleton, human skin, flesh, muscles, realistic human, cartoon, anime, painting, 2D, illustration, clothed skeleton, skeleton in uniform, skeleton in armor, skeleton in costume, helmet covering skull, mask covering skull, bare-bone skeleton without translucent body silhouette, bones-only look with no translucent body shell, broken bones, dislocated joints, extra limbs, missing limbs, empty eye sockets, no eyes, hollow eyes, robotic motion, stiff pose, jerky movement, x-ray scan, radiograph, medical imaging, glowing portal, glass display case, glass dome, terrarium, pod, capsule, archway, window frame"
SKELETON_LORA_REFINEMENT_NEGATIVE = "glass display case, glass dome, pod, capsule, archway, doorway, window frame, x-ray scan, radiograph, medical imaging, detached shell, shell hovering away from body, bones-only silhouette"


async def _get_active_skeleton_lora_name() -> str | None:
    """Resolve the preferred available skeleton LoRA name from ComfyUI."""
    for base_url in _comfyui_candidate_urls():
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(f"{base_url}/object_info/LoraLoader")
                if resp.status_code == 200:
                    data = resp.json()
                    lora_list = data.get("LoraLoader", {}).get("input", {}).get("required", {}).get("lora_name", [[]])[0]
                    for candidate in SKELETON_LORA_CANDIDATES:
                        if candidate in lora_list:
                            return candidate
        except Exception:
            continue
    return None


async def check_skeleton_lora_available() -> bool:
    """Check if a preferred skeleton LoRA exists on the ComfyUI server."""
    if not SKELETON_SDXL_LORA_ENABLED:
        return False
    if not _configured_local_image_provider_order():
        return False
    return (await _get_active_skeleton_lora_name()) is not None


async def generate_image_hidream_t2i(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    negative_prompt: str = "",
    allow_default_negative: bool = True,
    max_wait_sec: int = 900,
    runtime_assets: Optional[dict] = None,
) -> str:
    """Generate an image via HiDream-I1 on ComfyUI."""
    config = RESOLUTION_CONFIGS[resolution]
    pos = str(prompt or "").strip()
    neg = str(negative_prompt or "").strip()
    if not neg and allow_default_negative:
        neg = NEGATIVE_PROMPT
    hidream_steps = int(HIDREAM_STEPS)
    if int(max_wait_sec or 0) <= max(90, HIDREAM_INTERACTIVE_MAX_WAIT_SEC + 5):
        hidream_steps = min(hidream_steps, 16)
    runtime = dict(runtime_assets or (await _resolve_hidream_runtime_assets()) or {})
    workflow = {
        "54": {
            "class_type": "QuadrupleCLIPLoader",
            "inputs": {
                "clip_name1": str(runtime.get("clip_l_name", "")),
                "clip_name2": str(runtime.get("clip_g_name", "")),
                "clip_name3": str(runtime.get("t5_name", "")),
                "clip_name4": str(runtime.get("llama_name", "")),
            },
        },
        "55": {
            "class_type": "VAELoader",
            "inputs": {"vae_name": str(runtime.get("vae_name", ""))},
        },
        "69": {
            "class_type": "UNETLoader",
            "inputs": {
                "unet_name": str(runtime.get("model_name", "")),
                "weight_dtype": str(runtime.get("weight_dtype", "default") or "default"),
            },
        },
        "70": {
            "class_type": "ModelSamplingSD3",
            "inputs": {"shift": float(HIDREAM_SHIFT), "model": ["69", 0]},
        },
        "53": {
            "class_type": "EmptySD3LatentImage",
            "inputs": {"width": int(config["gen_width"]), "height": int(config["gen_height"]), "batch_size": 1},
        },
        "16": {
            "class_type": "CLIPTextEncodeHiDream",
            "inputs": {
                "clip": ["54", 0],
                "clip_l": pos,
                "clip_g": pos,
                "t5xxl": pos,
                "llama": pos,
            },
        },
        "40": {
            "class_type": "CLIPTextEncodeHiDream",
            "inputs": {
                "clip": ["54", 0],
                "clip_l": neg,
                "clip_g": neg,
                "t5xxl": neg,
                "llama": neg,
            },
        },
        "3": {
            "class_type": "KSampler",
            "inputs": {
                "seed": random.randint(0, 2**32),
                "steps": hidream_steps,
                "cfg": float(HIDREAM_CFG),
                "sampler_name": str(HIDREAM_SAMPLER or "euler"),
                "scheduler": str(HIDREAM_SCHEDULER or "simple"),
                "denoise": 1.0,
                "model": ["70", 0],
                "positive": ["16", 0],
                "negative": ["40", 0],
                "latent_image": ["53", 0],
            },
        },
        "8": {
            "class_type": "VAEDecode",
            "inputs": {"samples": ["3", 0], "vae": ["55", 0]},
        },
        "9": {
            "class_type": "SaveImage",
            "inputs": {"filename_prefix": "nyptid_hidream_t2i", "images": ["8", 0]},
        },
    }
    result = await _run_comfyui_workflow(workflow, "9", "images", max_wait_sec=max_wait_sec, poll_interval_sec=2.0)
    await _download_comfyui_file(result["images"][0], output_path)
    return output_path


async def _materialize_reference_image_input(reference_image_url: str, template: str = "", prompt: str = "") -> tuple[str, bool]:
    source = str(reference_image_url or "").strip()
    if not source:
        raise RuntimeError("Reference image source missing")
    existing = Path(source)
    if existing.exists():
        if str(template or "").strip().lower() == "skeleton":
            processed = _prepare_skeleton_reference_conditioning_image(str(existing), prompt=prompt)
            return str(processed), str(processed) != str(existing)
        return str(existing), False

    raw = b""
    mime = "image/png"
    if source.startswith("data:image/"):
        raw, mime = _decode_data_image_url(source)
    else:
        async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
            resp = await client.get(source)
            resp.raise_for_status()
            raw = resp.content
            mime = str(resp.headers.get("content-type", "") or mime).split(";", 1)[0].strip() or mime
    if not raw:
        raise RuntimeError("Reference image source was empty")

    ext = ".png"
    mime_l = mime.lower()
    if "jpeg" in mime_l or "jpg" in mime_l:
        ext = ".jpg"
    elif "webp" in mime_l:
        ext = ".webp"
    elif "png" not in mime_l:
        try:
            if source.lower().endswith(".jpg") or source.lower().endswith(".jpeg"):
                ext = ".jpg"
            elif source.lower().endswith(".webp"):
                ext = ".webp"
        except Exception:
            pass

    ref_dir = TEMP_DIR / "runtime_reference_inputs"
    ref_dir.mkdir(parents=True, exist_ok=True)
    out_path = ref_dir / f"hidream_ref_{int(time.time() * 1000)}_{random.randint(1000, 9999)}{ext}"
    out_path.write_bytes(raw)
    if str(template or "").strip().lower() == "skeleton":
        processed = _prepare_skeleton_reference_conditioning_image(str(out_path), prompt=prompt)
        try:
            Path(out_path).unlink(missing_ok=True)
        except Exception:
            pass
        return str(processed), True
    return str(out_path), True


async def generate_image_hidream_reference_locked(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    negative_prompt: str = "",
    template: str = "",
    reference_image_url: str = "",
    reference_lock_mode: str = "strict",
    allow_default_negative: bool = True,
    max_wait_sec: int = 900,
    runtime_assets: Optional[dict] = None,
) -> str:
    """Generate a HiDream image edit using the uploaded reference image as the latent starting point."""
    if not str(reference_image_url or "").strip():
        return await generate_image_hidream_t2i(
            prompt,
            output_path,
            resolution=resolution,
            negative_prompt=negative_prompt,
            allow_default_negative=allow_default_negative,
            max_wait_sec=max_wait_sec,
            runtime_assets=runtime_assets,
        )

    config = RESOLUTION_CONFIGS[resolution]
    pos = str(prompt or "").strip()
    neg = str(negative_prompt or "").strip()
    if not neg and allow_default_negative:
        neg = NEGATIVE_PROMPT
    runtime = dict(runtime_assets or (await _resolve_hidream_edit_runtime_assets()) or {})
    hidream_steps = int(HIDREAM_STEPS)
    if int(max_wait_sec or 0) <= max(90, HIDREAM_INTERACTIVE_MAX_WAIT_SEC + 5):
        hidream_steps = min(hidream_steps, 18)
    ref_path = ""
    cleanup_ref = False
    try:
        ref_path, cleanup_ref = await _materialize_reference_image_input(reference_image_url, template=template, prompt=prompt)
        uploaded_name = await _upload_image_to_comfyui(ref_path)
        target_mp = max(0.6, round((float(config["gen_width"]) * float(config["gen_height"])) / 1_000_000.0, 2))
        lock_mode = _normalize_reference_lock_mode(reference_lock_mode, default="strict")
        denoise = HIDREAM_REFERENCE_STRICT_DENOISE if lock_mode == "strict" else HIDREAM_REFERENCE_INSPIRED_DENOISE
        prompt_l = str(prompt or "").strip().lower()
        cfg = float(HIDREAM_CFG)
        if re.search(r"\b(table|desk|countertop|brain|money|cash|banknotes?|holding|sits?|standing|walk|room|office|background)\b", prompt_l):
            denoise = min(0.82, denoise + 0.04)
        scene_override_prompt = bool(
            re.search(
                r"\b(table|desk|countertop|chair|seated|sitting|brain|money|cash|banknotes?|dark room|office|studio|battlefield|throne|street|city|forest|desert|indoors|interior|outdoor|background)\b",
                prompt_l,
            )
        )
        if scene_override_prompt:
            denoise = max(denoise, 0.76 if lock_mode == "strict" else 0.84)
            cfg = min(8.9, cfg + 0.8)
        table_edit_prompt = bool(re.search(r"\b(table|desk|countertop)\b", prompt_l)) and bool(
            re.search(r"\b(sits?|seated|sitting)\b", prompt_l)
        )
        if table_edit_prompt:
            denoise = max(denoise, 0.84 if lock_mode == "strict" else 0.90)
            cfg = min(9.2, cfg + 0.4)

        workflow = {
            "54": {
                "class_type": "QuadrupleCLIPLoader",
                "inputs": {
                    "clip_name1": str(runtime.get("clip_l_name", "")),
                    "clip_name2": str(runtime.get("clip_g_name", "")),
                    "clip_name3": str(runtime.get("t5_name", "")),
                    "clip_name4": str(runtime.get("llama_name", "")),
                },
            },
            "55": {
                "class_type": "VAELoader",
                "inputs": {"vae_name": str(runtime.get("vae_name", ""))},
            },
            "69": {
                "class_type": "UNETLoader",
                "inputs": {
                    "unet_name": str(runtime.get("model_name", "")),
                    "weight_dtype": str(runtime.get("weight_dtype", "default") or "default"),
                },
            },
            "70": {
                "class_type": "ModelSamplingSD3",
                "inputs": {"shift": float(HIDREAM_SHIFT), "model": ["69", 0]},
            },
            "90": {
                "class_type": "LoadImage",
                "inputs": {"image": uploaded_name},
            },
            "91": {
                "class_type": "ImageScaleToTotalPixels",
                "inputs": {
                    "image": ["90", 0],
                    "upscale_method": HIDREAM_REFERENCE_UPSCALE_METHOD,
                    "megapixels": float(target_mp),
                    "resolution_steps": 64,
                },
            },
            "92": {
                "class_type": "VAEEncode",
                "inputs": {"pixels": ["91", 0], "vae": ["55", 0]},
            },
            "16": {
                "class_type": "CLIPTextEncodeHiDream",
                "inputs": {
                    "clip": ["54", 0],
                    "clip_l": pos,
                    "clip_g": pos,
                    "t5xxl": pos,
                    "llama": pos,
                },
            },
            "40": {
                "class_type": "CLIPTextEncodeHiDream",
                "inputs": {
                    "clip": ["54", 0],
                    "clip_l": neg,
                    "clip_g": neg,
                    "t5xxl": neg,
                    "llama": neg,
                },
            },
            "3": {
                "class_type": "KSampler",
                "inputs": {
                    "seed": random.randint(0, 2**32),
                    "steps": hidream_steps,
                    "cfg": float(cfg),
                    "sampler_name": str(HIDREAM_SAMPLER or "euler"),
                    "scheduler": str(HIDREAM_SCHEDULER or "simple"),
                    "denoise": float(denoise),
                    "model": ["70", 0],
                    "positive": ["16", 0],
                    "negative": ["40", 0],
                    "latent_image": ["92", 0],
                },
            },
            "8": {
                "class_type": "VAEDecode",
                "inputs": {"samples": ["3", 0], "vae": ["55", 0]},
            },
            "9": {
                "class_type": "SaveImage",
                "inputs": {"filename_prefix": "nyptid_hidream_ref", "images": ["8", 0]},
            },
        }
        result = await _run_comfyui_workflow(workflow, "9", "images", max_wait_sec=max_wait_sec, poll_interval_sec=2.0)
        await _download_comfyui_file(result["images"][0], output_path)
        return output_path
    except Exception as e:
        log.warning(f"HiDream E1.1 reference-conditioned generation failed, falling back to txt2img: {e}")
        return await generate_image_hidream_t2i(
            prompt,
            output_path,
            resolution=resolution,
            negative_prompt=negative_prompt,
            allow_default_negative=allow_default_negative,
            max_wait_sec=max_wait_sec,
            runtime_assets=runtime_assets,
        )
    finally:
        if cleanup_ref and ref_path:
            try:
                Path(ref_path).unlink(missing_ok=True)
            except Exception:
                pass


async def generate_image_skeleton_lora(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    source_image_path: str = "",
    denoise: float = 1.0,
    negative_prompt: str = "",
) -> str:
    """Generate or refine a skeleton image using the Jerry glass-shell SDXL LoRA on ComfyUI."""
    config = RESOLUTION_CONFIGS[resolution]
    active_lora_name = await _get_active_skeleton_lora_name()
    if not active_lora_name:
        raise RuntimeError("No preferred skeleton LoRA is available on ComfyUI.")
    prompt_text = str(prompt or "").strip()
    if source_image_path:
        lora_prompt = (
            f"{SKELETON_TRIGGER_TOKEN}, {prompt_text}, preserve the exact composition, pose, props, and framing from the source image. "
            "Add a clear translucent glass-like body shell wrapped tightly around the skeleton anatomy. "
            "The shell must hug the skeleton body, not become a separate dome, display case, portal, archway, or window."
        ).strip()
    else:
        lora_prompt = f"{SKELETON_TRIGGER_TOKEN}, {prompt_text}".strip()
    neg_text = ", ".join(
        part for part in [str(negative_prompt or "").strip(), SKELETON_LORA_NEGATIVE, SKELETON_LORA_REFINEMENT_NEGATIVE if source_image_path else ""] if part
    )

    if source_image_path and Path(source_image_path).exists():
        uploaded_name = await _upload_image_to_comfyui(source_image_path)
        target_mp = max(0.6, round((float(config["gen_width"]) * float(config["gen_height"])) / 1_000_000.0, 2))
        workflow = {
            "1": {
                "class_type": "CheckpointLoaderSimple",
                "inputs": {"ckpt_name": "sd_xl_base_1.0.safetensors"},
            },
            "99": {
                "class_type": "LoraLoader",
                "inputs": {
                    "lora_name": active_lora_name,
                    "strength_model": SKELETON_LORA_STRENGTH,
                    "strength_clip": SKELETON_LORA_STRENGTH,
                    "model": ["1", 0],
                    "clip": ["1", 1],
                },
            },
            "2": {
                "class_type": "LoadImage",
                "inputs": {"image": uploaded_name},
            },
            "3": {
                "class_type": "ImageScaleToTotalPixels",
                "inputs": {
                    "image": ["2", 0],
                    "upscale_method": "lanczos",
                    "megapixels": float(target_mp),
                    "resolution_steps": 64,
                },
            },
            "4": {
                "class_type": "VAEEncode",
                "inputs": {"pixels": ["3", 0], "vae": ["1", 2]},
            },
            "5": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": lora_prompt, "clip": ["99", 1]},
            },
            "6": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": neg_text, "clip": ["99", 1]},
            },
            "7": {
                "class_type": "KSampler",
                "inputs": {
                    "seed": random.randint(0, 2**32),
                    "steps": 12,
                    "cfg": 6.4,
                    "sampler_name": "dpmpp_2m",
                    "scheduler": "karras",
                    "denoise": float(max(0.18, min(0.55, denoise))),
                    "model": ["99", 0],
                    "positive": ["5", 0],
                    "negative": ["6", 0],
                    "latent_image": ["4", 0],
                },
            },
            "8": {
                "class_type": "VAEDecode",
                "inputs": {"samples": ["7", 0], "vae": ["1", 2]},
            },
            "9": {
                "class_type": "SaveImage",
                "inputs": {"filename_prefix": "nyptid_skeleton_lora_refine", "images": ["8", 0]},
            },
        }
    else:
        workflow = {
            "4": {
                "class_type": "CheckpointLoaderSimple",
                "inputs": {"ckpt_name": "sd_xl_base_1.0.safetensors"},
            },
            "10": {
                "class_type": "LoraLoader",
                "inputs": {
                    "lora_name": active_lora_name,
                    "strength_model": SKELETON_LORA_STRENGTH,
                    "strength_clip": SKELETON_LORA_STRENGTH,
                    "model": ["4", 0],
                    "clip": ["4", 1],
                },
            },
            "5": {
                "class_type": "EmptyLatentImage",
                "inputs": {"width": config["gen_width"], "height": config["gen_height"], "batch_size": 1},
            },
            "6": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": lora_prompt, "clip": ["10", 1]},
            },
            "7": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": neg_text, "clip": ["10", 1]},
            },
            "3": {
                "class_type": "KSampler",
                "inputs": {
                    "seed": random.randint(0, 2**32),
                    "steps": 35,
                    "cfg": 7.0,
                    "sampler_name": "dpmpp_2m",
                    "scheduler": "karras",
                    "denoise": 1.0,
                    "model": ["10", 0],
                    "positive": ["6", 0],
                    "negative": ["7", 0],
                    "latent_image": ["5", 0],
                },
            },
            "8": {
                "class_type": "VAEDecode",
                "inputs": {"samples": ["3", 0], "vae": ["4", 2]},
            },
            "9": {
                "class_type": "SaveImage",
                "inputs": {"filename_prefix": "nyptid_skeleton_lora", "images": ["8", 0]},
            },
        }

    result = await _run_comfyui_workflow(workflow, "9", "images")
    await _download_comfyui_file(result["images"][0], output_path)
    log.info(
        f"Skeleton LoRA image generated via {active_lora_name}: "
        f"{output_path} ({Path(output_path).stat().st_size / 1024:.0f} KB)"
    )
    return output_path


async def generate_image_wan22_t2i(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    negative_prompt: str = "",
    allow_default_negative: bool = True,
    max_wait_sec: int = 900,
    runtime_assets: Optional[dict] = None,
) -> str:
    """Generate an image via WAN 2.2 text-to-image on ComfyUI."""
    config = RESOLUTION_CONFIGS[resolution]
    neg = str(negative_prompt or "").strip()
    if not neg and allow_default_negative:
        neg = NEGATIVE_PROMPT
    runtime = dict(runtime_assets or (await _resolve_wan22_t2i_runtime_assets()) or {})
    mode = str(runtime.get("mode", "checkpoint"))
    ckpt_name = str(runtime.get("ckpt_name", ""))
    clip_name = str(runtime.get("clip_name", ""))
    vae_name = str(runtime.get("vae_name", ""))
    unet_name = str(runtime.get("unet_name", ""))
    low_unet_name = str(runtime.get("low_unet_name", ""))
    high_unet_name = str(runtime.get("high_unet_name", ""))

    if mode == "ti2v_unet":
        prompt_l = str(prompt or "").lower()
        semantic_heavy = any(
            token in prompt_l
            for token in ["brain", "money", "cash", "banknote", "table", "desk", "countertop", "glow", "glowing"]
        )
        steps = 20 if semantic_heavy else 16
        cfg_scale = 4.4 if semantic_heavy else 3.6
        sampler_name = "euler"
        scheduler = "simple"
        workflow = {
            "71": {
                "class_type": "CLIPLoader",
                "inputs": {"clip_name": clip_name, "type": "wan", "device": "default"},
            },
            "72": {
                "class_type": "CLIPTextEncode",
                "inputs": {"clip": ["71", 0], "text": neg},
            },
            "89": {
                "class_type": "CLIPTextEncode",
                "inputs": {"clip": ["71", 0], "text": prompt},
            },
            "73": {
                "class_type": "VAELoader",
                "inputs": {"vae_name": vae_name},
            },
            "76": {
                "class_type": "UNETLoader",
                "inputs": {"unet_name": unet_name, "weight_dtype": "default"},
            },
            "82": {
                "class_type": "ModelSamplingSD3",
                "inputs": {"model": ["76", 0], "shift": 5.0},
            },
            "74": {
                "class_type": "EmptyHunyuanLatentVideo",
                "inputs": {
                    "width": int(config["gen_width"]),
                    "height": int(config["gen_height"]),
                    "length": 1,
                    "batch_size": 1,
                },
            },
            "81": {
                "class_type": "KSampler",
                "inputs": {
                    "seed": random.randint(0, 2**32),
                    "steps": steps,
                    "cfg": cfg_scale,
                    "sampler_name": sampler_name,
                    "scheduler": scheduler,
                    "denoise": 1.0,
                    "model": ["82", 0],
                    "positive": ["89", 0],
                    "negative": ["72", 0],
                    "latent_image": ["74", 0],
                },
            },
            "87": {
                "class_type": "VAEDecode",
                "inputs": {"samples": ["81", 0], "vae": ["73", 0]},
            },
            "9": {
                "class_type": "SaveImage",
                "inputs": {"filename_prefix": "nyptid_wan22_t2i", "images": ["87", 0]},
            },
        }
    elif mode == "latent":
        # Local WAN fallback: use low/high T2V lanes to synthesize a still frame.
        # Keep latent length at 1 for still-image generation to avoid upstream request timeouts.
        latent_length = 1
        prompt_l = str(prompt or "").lower()
        semantic_heavy = any(
            token in prompt_l
            for token in ["brain", "money", "cash", "banknote", "table", "desk", "countertop", "glow", "glowing"]
        )
        total_steps = 12 if semantic_heavy else 9
        low_noise_end = 6 if semantic_heavy else 4
        cfg_scale = 4.8 if semantic_heavy else 3.0
        workflow = {
            "71": {
                "class_type": "CLIPLoader",
                "inputs": {"clip_name": clip_name, "type": "wan", "device": "default"},
            },
            "72": {
                "class_type": "CLIPTextEncode",
                "inputs": {"clip": ["71", 0], "text": neg},
            },
            "89": {
                "class_type": "CLIPTextEncode",
                "inputs": {"clip": ["71", 0], "text": prompt},
            },
            "73": {
                "class_type": "VAELoader",
                "inputs": {"vae_name": vae_name},
            },
            "76": {
                "class_type": "UNETLoader",
                "inputs": {"unet_name": low_unet_name, "weight_dtype": "default"},
            },
            "75": {
                "class_type": "UNETLoader",
                "inputs": {"unet_name": high_unet_name, "weight_dtype": "default"},
            },
            "82": {
                "class_type": "ModelSamplingSD3",
                "inputs": {"model": ["76", 0], "shift": 5.0},
            },
            "86": {
                "class_type": "ModelSamplingSD3",
                "inputs": {"model": ["75", 0], "shift": 5.0},
            },
            "74": {
                "class_type": "EmptyHunyuanLatentVideo",
                "inputs": {
                    "width": int(config["gen_width"]),
                    "height": int(config["gen_height"]),
                    "length": latent_length,
                    "batch_size": 1,
                },
            },
            "81": {
                "class_type": "KSamplerAdvanced",
                "inputs": {
                    "model": ["82", 0],
                    "add_noise": "enable",
                    "noise_seed": random.randint(0, 2**32),
                    "steps": total_steps,
                    "cfg": cfg_scale,
                    "sampler_name": "euler",
                    "scheduler": "simple",
                    "positive": ["89", 0],
                    "negative": ["72", 0],
                    "latent_image": ["74", 0],
                    "start_at_step": 0,
                    "end_at_step": low_noise_end,
                    "return_with_leftover_noise": "enable",
                },
            },
            "78": {
                "class_type": "KSamplerAdvanced",
                "inputs": {
                    "model": ["86", 0],
                    "add_noise": "disable",
                    "noise_seed": 0,
                    "steps": total_steps,
                    "cfg": cfg_scale,
                    "sampler_name": "euler",
                    "scheduler": "simple",
                    "positive": ["89", 0],
                    "negative": ["72", 0],
                    "latent_image": ["81", 0],
                    "start_at_step": low_noise_end,
                    "end_at_step": total_steps,
                    "return_with_leftover_noise": "disable",
                },
            },
            "87": {
                "class_type": "VAEDecode",
                "inputs": {"samples": ["78", 0], "vae": ["73", 0]},
            },
            "9": {
                "class_type": "SaveImage",
                "inputs": {"filename_prefix": "nyptid_wan22_t2i", "images": ["87", 0]},
            },
        }
    else:
        workflow = {
            "3": {
                "class_type": "KSampler",
                "inputs": {
                    "seed": random.randint(0, 2**32),
                    "steps": 32,
                    "cfg": 4.5,
                    "sampler_name": "uni_pc_bh2",
                    "scheduler": "simple",
                    "denoise": 1.0,
                    "model": ["4", 0],
                    "positive": ["6", 0],
                    "negative": ["7", 0],
                    "latent_image": ["5", 0],
                },
            },
            "4": {
                "class_type": "CheckpointLoaderSimple",
                "inputs": {"ckpt_name": ckpt_name},
            },
            "5": {
                "class_type": "EmptyLatentImage",
                "inputs": {"width": config["gen_width"], "height": config["gen_height"], "batch_size": 1},
            },
            "6": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": prompt, "clip": ["4", 1]},
            },
            "7": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": neg, "clip": ["4", 1]},
            },
            "8": {
                "class_type": "VAEDecode",
                "inputs": {"samples": ["3", 0], "vae": ["4", 2]},
            },
            "9": {
                "class_type": "SaveImage",
                "inputs": {"filename_prefix": "nyptid_wan22_t2i", "images": ["8", 0]},
            },
        }
        if clip_name and vae_name:
            workflow["30"] = {
                "class_type": "CLIPLoader",
                "inputs": {"clip_name": clip_name, "type": "wan"},
            }
            workflow["31"] = {
                "class_type": "VAELoader",
                "inputs": {"vae_name": vae_name},
            }
            workflow["6"]["inputs"]["clip"] = ["30", 0]
            workflow["7"]["inputs"]["clip"] = ["30", 0]
            workflow["8"]["inputs"]["vae"] = ["31", 0]

    result = await _run_comfyui_workflow(workflow, "9", "images", max_wait_sec=max_wait_sec, poll_interval_sec=2.0)
    await _download_comfyui_file(result["images"][0], output_path)
    return output_path


async def generate_scene_image(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    negative_prompt: str = "",
    template: str = "",
    channel_context: dict | None = None,
    reference_image_url: str = "",
    reference_lock_mode: str = "strict",
    best_of_enabled: bool = True,
    salvage_enabled: bool = True,
    interactive_fast: bool = False,
    prompt_passthrough: bool = False,
    selected_model_id: str = "",
) -> dict:
    """Generate a scene image. Priority for skeleton template: LoRA > Grok Imagine > SDXL.
    For other templates: Grok Imagine > SDXL.
    Returns {"local_path": str, "cdn_url": str | None}.
    """
    async def _enforce_1080_image(path: str) -> None:
        if resolution != "1080p":
            return
        if not _ffmpeg_available():
            log.warning("ffmpeg not found on host; skipping 1080p image upscale")
            return
        target = RESOLUTION_CONFIGS.get("1080p", {})
        out_w = int(target.get("output_width", 1080) or 1080)
        out_h = int(target.get("output_height", 1920) or 1920)
        src = Path(path)
        if not src.exists():
            return
        upscaled = src.with_name(src.stem + "_up1080.png")
        vf = (
            f"scale={out_w}:{out_h}:force_original_aspect_ratio=decrease:flags=lanczos,"
            f"pad={out_w}:{out_h}:(ow-iw)/2:(oh-ih)/2:black,"
            "unsharp=5:5:0.55:5:5:0.0"
        )
        cmd = [
            "ffmpeg", "-y", "-i", str(src),
            "-vf", vf,
            "-frames:v", "1",
            str(upscaled),
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, err = await proc.communicate()
        if proc.returncode == 0 and upscaled.exists() and upscaled.stat().st_size > 0:
            shutil.move(str(upscaled), str(src))
            log.info(f"Image upscaled to 1080p: {src.name}")
        else:
            upscaled.unlink(missing_ok=True)
            log.warning(f"Image 1080p upscale skipped for {src.name}: {err.decode()[-200:]}")

    lock_mode = _normalize_reference_lock_mode(reference_lock_mode, default="strict")
    channel_context = dict(channel_context or {})
    channel_blocks_fal_scene = _channel_blocks_fal_scene_generation(channel_context)
    channel_prefers_fal_scene = _channel_prefers_fal_scene_generation(channel_context)
    if template == "skeleton" and _is_template_default_reference(template, reference_image_url):
        log.info("Skeleton default style lock active: using reference DNA only and skipping direct image conditioning")
        reference_image_url = ""
    has_reference = bool(str(reference_image_url or "").strip())
    template_adapter_route = _resolve_template_adapter_route(template)
    if not prompt_passthrough:
        prompt, negative_prompt, template_adapter_route = _apply_template_prompt_route(
            template=template,
            prompt=prompt,
            negative_prompt=negative_prompt,
            provider="scene_image",
            adapter_route=template_adapter_route,
        )
    named_human_support = template == "skeleton" and _skeleton_scene_supporting_humans_requested(prompt)
    if template == "skeleton":
        if prompt_passthrough:
            negative_prompt = _relax_skeleton_negative_prompt_for_passthrough(negative_prompt, prompt)
        else:
            negative_prompt = _augment_skeleton_negative_prompt(negative_prompt, prompt)

    def _skeleton_repair_prompt(base_prompt: str) -> str:
        if template != "skeleton":
            return base_prompt
        prompt_l = str(base_prompt or "").lower()
        wants_damage = bool(
            re.search(r"\b(crack|cracks|fracture|fractured|chip|chipped|bruise|bruises|damaged|damage)\b", prompt_l)
        )
        wants_tired = bool(
            re.search(r"\b(tired|fatigued|weary|slouch|slouched|hunch|hunched|droop|drooping|exhausted)\b", prompt_l)
        )
        damage_lock = (
            "DAMAGE DETAIL LOCK: preserve any requested damage cues (cracks, fractures, chips, bruising) clearly on bones."
            if wants_damage
            else ""
        )
        tired_lock = (
            "POSTURE LOCK: preserve requested fatigue cues with slouched shoulders, slight forward head, and low-energy gait."
            if wants_tired
            else ""
        )
        outfit_lock = _skeleton_outfit_coverage_lock(base_prompt)
        return (
            f"{base_prompt} "
            + "CRITICAL QUALITY LOCK: identical canonical skeleton identity, ultra-crisp skull and eye detail, "
            + "readable anatomical structure, high-contrast lighting, sharp cinematic focus, "
            + ("clearly visible translucent body silhouette, requested wardrobe stays correctly worn and body-covering. " if outfit_lock else "clearly visible translucent body silhouette, no clothing or costume. ")
            + "Canonical ivory bone color only; never x-ray/radiograph/CT scan look, never neon-blue medical scan aesthetics. "
            + "Do not omit scene props: every explicitly requested object/action must be visible. "
            + "Never replace requested props with generic spheres/balls. "
            + "Keep a rich topic-matched environment or readable cutaway context; do not collapse to a blank empty backdrop unless explicitly requested. "
            + (damage_lock + " " if damage_lock else "")
            + (tired_lock + " " if tired_lock else "")
            + (outfit_lock + " " if outfit_lock else "")
            + "Glass-shell visibility must be obvious (not faint): medium-opacity translucent shell around the skeleton form."
        ).strip()

    explicit_image_model_requested = bool(str(selected_model_id or "").strip())
    explicit_image_model_id = _normalize_creative_image_model_id(selected_model_id, template=template)
    if explicit_image_model_requested and channel_blocks_fal_scene:
        explicit_profile = _creative_image_model_profile(explicit_image_model_id, template=template)
        if str(explicit_profile.get("provider", "") or "").strip().lower() == "fal":
            log.info(
                "Channel policy disabled explicit fal scene model '%s'; using configured local/provider fallback lane instead",
                explicit_image_model_id,
            )
            explicit_image_model_requested = False
            explicit_image_model_id = DEFAULT_CREATIVE_IMAGE_MODEL_ID
    if explicit_image_model_requested and explicit_image_model_id != DEFAULT_CREATIVE_IMAGE_MODEL_ID:
        profile = _creative_image_model_profile(explicit_image_model_id, template=template)
        effective_prompt = _creative_model_prompt(prompt, negative_prompt=negative_prompt)
        if explicit_image_model_id == "grok_imagine":
            result = await generate_image_grok(
                effective_prompt,
                output_path,
                resolution=resolution,
                reference_image_url=reference_image_url,
                reference_lock_mode=lock_mode,
            )
        else:
            result = await _generate_image_fal_selected_model(
                explicit_image_model_id,
                prompt,
                output_path,
                resolution=resolution,
                negative_prompt=negative_prompt,
                reference_image_url=reference_image_url,
            )
        await _enforce_1080_image(output_path)
        _ensure_generated_image_valid(output_path)
        qa = _score_generated_image_quality(output_path, prompt=effective_prompt, template=template)
        qa_gate_ok, qa_gate_min = _image_quality_gate(
            qa,
            template=template,
            lock_mode=lock_mode,
            has_reference=has_reference,
            prompt=effective_prompt,
        )
        result["provider"] = explicit_image_model_id
        result["provider_label"] = str(profile.get("label", explicit_image_model_id) or explicit_image_model_id)
        result["qa_score"] = qa.get("score", 0.0)
        result["qa_ok"] = bool(qa_gate_ok)
        result["qa_min_score"] = qa_gate_min
        result["qa_notes"] = list(qa.get("notes", []) or [])
        if template == "skeleton" and not qa_gate_ok:
            try:
                repair_prompt = _skeleton_repair_prompt(prompt)
                repair_path = str(Path(output_path).with_name(Path(output_path).stem + "_grok_repair" + Path(output_path).suffix))
                repair_result = await generate_image_grok(
                    repair_prompt,
                    repair_path,
                    resolution=resolution,
                    reference_image_url=reference_image_url,
                    reference_lock_mode=lock_mode,
                )
                await _enforce_1080_image(repair_path)
                _ensure_generated_image_valid(repair_path)
                repair_qa = _score_generated_image_quality(repair_path, prompt=repair_prompt, template=template)
                repair_gate_ok, repair_gate_min = _image_quality_gate(
                    repair_qa,
                    template=template,
                    lock_mode=lock_mode,
                    has_reference=has_reference,
                    prompt=repair_prompt,
                )
                if repair_gate_ok or float(repair_qa.get("score", 0.0)) > float(result.get("qa_score", 0.0)):
                    shutil.copyfile(repair_path, output_path)
                    result = dict(repair_result)
                    result["local_path"] = output_path
                    result["provider"] = explicit_image_model_id
                    result["provider_label"] = str(profile.get("label", explicit_image_model_id) or explicit_image_model_id)
                    result["qa_score"] = repair_qa.get("score", 0.0)
                    result["qa_ok"] = bool(repair_gate_ok)
                    result["qa_min_score"] = repair_gate_min
                    result["qa_notes"] = list(repair_qa.get("notes", []) or [])
                Path(repair_path).unlink(missing_ok=True)
            except Exception as repair_err:
                log.warning(f"Skeleton Grok-only repair pass failed: {repair_err}")
        if template == "skeleton" and not bool(result.get("qa_ok", False)):
            if interactive_fast:
                notes = _interactive_soft_accept_notes(result["qa_notes"])
                if not _skeleton_notes_are_severe(notes):
                    result["qa_ok"] = False
                    result["qa_notes"] = notes
                    return result
            raise RuntimeError(
                f"{profile.get('label', explicit_image_model_id)} failed skeleton QA gate "
                f"(score={result.get('qa_score', 0.0)}, notes={result.get('qa_notes', [])})"
            )
        return result
    if template == "skeleton" and interactive_fast and not has_reference and not named_human_support:
        try:
            lora_available = await check_skeleton_lora_available()
            if lora_available:
                fast_prompt = _build_skeleton_lora_fast_prompt(prompt)
                fast_negative = _build_skeleton_lora_fast_negative(negative_prompt, prompt)
                await generate_image_skeleton_lora(
                    fast_prompt,
                    output_path,
                    resolution=resolution,
                    negative_prompt=fast_negative,
                )
                await _enforce_1080_image(output_path)
                _ensure_generated_image_valid(output_path)
                qa = _score_generated_image_quality(output_path, prompt=fast_prompt, template=template)
                qa_gate_ok, qa_gate_min = _image_quality_gate(
                    qa,
                    template=template,
                    lock_mode=lock_mode,
                    has_reference=False,
                    prompt=fast_prompt,
                )
                base_result = {
                    "local_path": output_path,
                    "cdn_url": None,
                    "provider": "skeleton_lora",
                    "qa_score": qa.get("score", 0.0),
                    "qa_ok": bool(qa_gate_ok),
                    "qa_min_score": qa_gate_min,
                    "qa_notes": list(qa.get("notes", []) or []),
                    "attempt": 1,
                }
                if qa_gate_ok:
                    log.info("Skeleton interactive fast path generated via Jerry LoRA")
                    return base_result
                notes = _interactive_soft_accept_notes(base_result["qa_notes"])
                if not _skeleton_notes_are_severe(notes):
                    base_result["qa_ok"] = False
                    base_result["qa_notes"] = notes
                    log.info(
                        "Skeleton interactive fast path soft-accepted via Jerry LoRA "
                        f"(score={float(base_result['qa_score'] or 0.0):.2f}, min={qa_gate_min:.2f})"
                    )
                    return base_result
                log.warning(
                    "Skeleton interactive fast path failed QA; falling back to configured providers "
                    f"(score={float(base_result['qa_score'] or 0.0):.2f}, notes={base_result['qa_notes']})"
                )
        except Exception as e:
            log.warning(f"Skeleton interactive fast path failed, falling back to configured providers: {e}")
    if template == "skeleton" and SKELETON_SDXL_LORA_ENABLED and not named_human_support:
        if reference_image_url and lock_mode == "strict":
            log.info("Skipping Skeleton LoRA for strict reference lock; using conditioned generator")
        else:
            try:
                lora_available = await check_skeleton_lora_available()
                if lora_available:
                    await generate_image_skeleton_lora(prompt, output_path, resolution=resolution)
                    await _enforce_1080_image(output_path)
                    log.info("Skeleton image generated via LoRA (zero API cost)")
                    return {"local_path": output_path, "cdn_url": None}
            except Exception as e:
                log.warning(f"Skeleton LoRA generation failed, falling back to Grok Imagine: {e}")

    provider_order = _configured_image_provider_order()
    if channel_blocks_fal_scene:
        provider_order = [
            provider
            for provider in provider_order
            if _normalize_image_provider_key(provider) not in {"fal", "xai", "grok"}
        ]
        if not provider_order:
            provider_order = [
                provider
                for provider in _configured_image_provider_order()
                if _normalize_image_provider_key(provider) not in {"fal", "xai", "grok"}
            ]
        if not provider_order:
            provider_order = ["wan22", "sdxl"] if template == "skeleton" else ["sdxl"]
        log.info("Channel policy: fal scene lane disabled for this request")
    elif channel_prefers_fal_scene and bool(FAL_AI_KEY or XAI_API_KEY):
        reordered = ["fal"]
        for provider in provider_order:
            if _normalize_image_provider_key(provider) in {"fal", "xai", "grok"}:
                continue
            reordered.append(provider)
        provider_order = reordered
    skeleton_wan_lock = template == "skeleton" and bool(SKELETON_REQUIRE_WAN22) and not named_human_support
    if interactive_fast and template == "skeleton":
        if named_human_support:
            preferred_order: list[str] = []
            if bool(FAL_AI_KEY or XAI_API_KEY) and not channel_blocks_fal_scene:
                preferred_order.append("fal")
            configured = _configured_image_provider_order()
            for provider_key in configured:
                normalized = _normalize_image_provider_key(provider_key)
                if normalized in {"fal", "xai", "grok"} and "fal" in preferred_order:
                    continue
                preferred_order.append(provider_key)
            provider_order = preferred_order or _configured_image_provider_order()
        elif skeleton_wan_lock:
            # Strict skeleton mode must stay WAN-only.
            provider_order = ["wan22"]
        else:
            compact = _configured_local_image_provider_order()
            if compact:
                provider_order = compact
    if not provider_order:
        provider_order = ["wan22"] if skeleton_wan_lock else _configured_image_provider_order()
    precooled = [p for p in provider_order if not _provider_is_available(p)]
    if precooled:
        provider_order = [p for p in provider_order if _provider_is_available(p)]
        if not provider_order:
            if skeleton_wan_lock:
                provider_order = ["wan22"]
            elif (not channel_blocks_fal_scene) and XAI_IMAGE_FALLBACK_ENABLED and bool(FAL_AI_KEY or XAI_API_KEY):
                provider_order = ["fal"]
            else:
                provider_order = ["sdxl"]
        for p in precooled:
            rem = int(round(_provider_cooldown_remaining(p)))
            log.warning(f"Image provider '{_normalize_image_provider_key(p)}' is cooling down ({rem}s left); skipping early")
    xai_aliases = {"xai", "grok", "fal"}
    hidream_requested = any(_normalize_image_provider_key(p) == "hidream" for p in provider_order)
    hidream_ready: Optional[bool] = None
    hidream_edit_ready: Optional[bool] = None
    if hidream_requested:
        hidream_ready = await check_hidream_available()
        if has_reference:
            hidream_edit_ready = await check_hidream_edit_available()
    if hidream_requested and not hidream_ready:
        provider_order = [p for p in provider_order if _normalize_image_provider_key(p) != "hidream"]
        if not provider_order:
            raise RuntimeError("HiDream is the only configured image provider, but HiDream assets are unavailable.")
        _provider_mark_failure("hidream", "hidream_unavailable", cooldown_sec=max(120, IMAGE_PROVIDER_FAILURE_COOLDOWN_SEC))
        log.warning("HiDream unavailable; skipping HiDream provider in this request")
    if hidream_requested and has_reference and hidream_ready and hidream_edit_ready is False:
        log.warning("HiDream E1.1 edit assets unavailable; reference edits will fall back to I1 img2img behavior")
    wan_requested = any(_normalize_image_provider_key(p) == "wan22" for p in provider_order)
    wan_t2i_ready: Optional[bool] = None
    if wan_requested:
        wan_t2i_ready = await check_wan22_t2i_available()
        if template == "skeleton" and bool(SKELETON_REQUIRE_WAN22) and not wan_t2i_ready:
            raise RuntimeError(
                "Skeleton generation blocked: WAN2.2 text-to-image is unavailable and fallback is disabled."
            )
    if IMAGE_PROVIDER_WAN_SKIP_IF_UNAVAILABLE and wan_requested:
        if wan_t2i_ready is None:
            wan_t2i_ready = await check_wan22_t2i_available()
        if not wan_t2i_ready:
            provider_order = [p for p in provider_order if _normalize_image_provider_key(p) != "wan22"]
            if not provider_order:
                provider_order = ["sdxl"]
            _provider_mark_failure("wan22", "wan22_t2i_unavailable", cooldown_sec=max(120, IMAGE_PROVIDER_FAILURE_COOLDOWN_SEC))
            log.warning("WAN22 T2I unavailable; skipping WAN provider in this request")
    cooled = [p for p in provider_order if not _provider_is_available(p)]
    if cooled:
        provider_order = [p for p in provider_order if _provider_is_available(p)]
        if not provider_order:
            if skeleton_wan_lock:
                provider_order = ["wan22"]
            elif (not channel_blocks_fal_scene) and XAI_IMAGE_FALLBACK_ENABLED and bool(FAL_AI_KEY or XAI_API_KEY):
                provider_order = ["fal"]
            else:
                provider_order = ["sdxl"]
        for p in cooled:
            rem = int(round(_provider_cooldown_remaining(p)))
            log.warning(f"Image provider '{_normalize_image_provider_key(p)}' is cooling down ({rem}s left); skipping")

    async def _local_provider_result(provider: str) -> dict | None:
        provider_key = str(provider or "").strip().lower()
        attempts = max(1, int(IMAGE_LOCAL_PROVIDER_RETRIES))
        if provider_key not in {"hidream", "hidream-i1", "hidream_i1", "wan", "wan22", "wan2.2", "sdxl", "comfy", "comfyui", "local"}:
            return None
        hidream_runtime_assets: dict | None = None
        wan_runtime_assets: dict | None = None
        wan_runtime_mode = ""
        compact_skeleton_prompt = _compact_skeleton_local_prompt(prompt) if template == "skeleton" else prompt
        skeleton_source_prompt = prompt if (template == "skeleton" and prompt_passthrough) else compact_skeleton_prompt
        compact_skeleton_negative = (
            str(negative_prompt or "").strip()
            if prompt_passthrough
            else _compact_skeleton_negative_prompt(negative_prompt, skeleton_source_prompt)
            if template == "skeleton"
            else negative_prompt
        )
        compact_prop_first_prompt = (
            _compact_skeleton_prop_first_prompt(prompt)
            if template == "skeleton"
            else prompt
        )
        if interactive_fast:
            if provider_key in {"hidream", "hidream-i1", "hidream_i1"}:
                provider_wait_sec = HIDREAM_INTERACTIVE_MAX_WAIT_SEC
            else:
                provider_wait_sec = WAN22_INTERACTIVE_MAX_WAIT_SEC if provider_key in {"wan", "wan22", "wan2.2"} else 12
        else:
            provider_wait_sec = 900
        if template == "skeleton":
            # Keep retries bounded in interactive mode while still giving local providers a fair chance.
            if interactive_fast:
                if provider_key in {"hidream", "hidream-i1", "hidream_i1"}:
                    attempts = HIDREAM_INTERACTIVE_ATTEMPTS
                    provider_wait_sec = HIDREAM_INTERACTIVE_MAX_WAIT_SEC
                    if has_reference:
                        # Public interactive requests route through a tunnel with a hard timeout budget.
                        # Reference-conditioned HiDream passes take materially longer than plain txt2img,
                        # so keep them to a single pass and soft-accept the first usable frame.
                        attempts = 1
                    try:
                        hidream_runtime_assets = await (
                            _resolve_hidream_edit_runtime_assets()
                            if has_reference
                            else _resolve_hidream_runtime_assets()
                        )
                    except Exception:
                        hidream_runtime_assets = None
                elif provider_key in {"wan", "wan22", "wan2.2"}:
                    try:
                        wan_runtime_assets = await _resolve_wan22_t2i_runtime_assets()
                        wan_runtime_mode = str((wan_runtime_assets or {}).get("mode", "") or "")
                    except Exception:
                        wan_runtime_assets = None
                        wan_runtime_mode = ""
                    if wan_runtime_mode == "latent":
                        # Missing WAN T2I checkpoint: latent fallback can work but needs a longer single pass.
                        attempts = WAN22_INTERACTIVE_LATENT_ATTEMPTS
                        provider_wait_sec = WAN22_INTERACTIVE_LATENT_MAX_WAIT_SEC
                    else:
                        attempts = WAN22_INTERACTIVE_ATTEMPTS
                        provider_wait_sec = WAN22_INTERACTIVE_MAX_WAIT_SEC
                    prompt_lc = skeleton_source_prompt.lower()
                    has_table_scene = bool(re.search(r"\b(table|desk|countertop)\b", prompt_lc))
                    has_brain = "brain" in prompt_lc
                    has_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", prompt_lc))
                    if has_table_scene and has_brain and has_money:
                        # Complex two-prop scenes need a wider candidate pool to avoid weak compositions.
                        attempts = max(attempts, 8)
                else:
                    attempts = 2
            else:
                attempts = max(attempts, 2)
        best_soft_result: dict | None = None
        best_soft_path = Path(output_path).with_name(
            Path(output_path).stem + f"_{provider_key}_best_soft" + (Path(output_path).suffix or ".png")
        )
        last_err: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                attempt_prompt = prompt
                attempt_negative = negative_prompt
                if template == "skeleton":
                    if prompt_passthrough:
                        attempt_prompt = skeleton_source_prompt
                        attempt_negative = compact_skeleton_negative
                    elif interactive_fast:
                        if attempt == 1:
                            attempt_prompt = skeleton_source_prompt
                        elif attempt == 2:
                            attempt_prompt = compact_prop_first_prompt
                        elif attempt == 3:
                            attempt_prompt = (
                                compact_prop_first_prompt
                                + " strict prop framing: full tabletop visible. left side brain with clear folds. right side pile of cash banknotes."
                            ).strip()
                        elif attempt == 4:
                            attempt_prompt = (
                                compact_prop_first_prompt
                                + " frontal medium shot. full ribcage and skull visible. both props in foreground on tabletop."
                            ).strip()
                        elif attempt == 5:
                            attempt_prompt = (
                                compact_prop_first_prompt
                                + " camera closer to tabletop so both brain and money are large and obvious."
                            ).strip()
                        else:
                            attempt_prompt = (
                                compact_prop_first_prompt
                                + " no human head statue, no bust sculpture, only full anatomical skeleton seated at table."
                            ).strip()
                        attempt_negative = _compact_skeleton_negative_prompt(negative_prompt, attempt_prompt)
                    else:
                        attempt_prompt = skeleton_source_prompt
                        attempt_negative = compact_skeleton_negative
                if provider_key in {"hidream", "hidream-i1", "hidream_i1", "wan", "wan22", "wan2.2"} and template == "skeleton":
                    if prompt_passthrough:
                        # Creative prompt passthrough means use the scene text exactly.
                        attempt_prompt = skeleton_source_prompt
                    else:
                        attempt_prompt = _skeleton_repair_prompt(skeleton_source_prompt)
                        if attempt > 1:
                            attempt_prompt = (
                                attempt_prompt
                                + " HARD READABILITY LOCK: skeleton stays dominant and ultra-sharp in frame with crisp eyes and props, while preserving readable environment depth. No haze, no motion blur."
                            ).strip()
                        if attempt >= 3:
                            attempt_prompt = (
                                attempt_prompt
                                + " STRICT PROP LOCK: all requested objects must be clearly visible and recognizable in-frame."
                            ).strip()
                if provider_key in {"hidream", "hidream-i1", "hidream_i1"}:
                    if has_reference:
                        await generate_image_hidream_reference_locked(
                            attempt_prompt,
                            output_path,
                            resolution=resolution,
                            negative_prompt=attempt_negative,
                            template=template,
                            reference_image_url=reference_image_url,
                            reference_lock_mode=lock_mode,
                            allow_default_negative=not prompt_passthrough,
                            max_wait_sec=provider_wait_sec,
                            runtime_assets=hidream_runtime_assets,
                        )
                    else:
                        await generate_image_hidream_t2i(
                            attempt_prompt,
                            output_path,
                            resolution=resolution,
                            negative_prompt=attempt_negative,
                            allow_default_negative=not prompt_passthrough,
                            max_wait_sec=provider_wait_sec,
                            runtime_assets=hidream_runtime_assets,
                        )
                if provider_key in {"wan", "wan22", "wan2.2"}:
                    await generate_image_wan22_t2i(
                        attempt_prompt,
                        output_path,
                        resolution=resolution,
                        negative_prompt=attempt_negative,
                        allow_default_negative=not prompt_passthrough,
                        max_wait_sec=provider_wait_sec,
                        runtime_assets=wan_runtime_assets,
                    )
                elif provider_key in {"sdxl", "comfy", "comfyui", "local"}:
                    await generate_image_comfyui(
                        attempt_prompt,
                        output_path,
                        resolution=resolution,
                        negative_prompt=attempt_negative,
                        allow_default_negative=not prompt_passthrough,
                        template=template,
                        adapter_route=template_adapter_route,
                        max_wait_sec=provider_wait_sec,
                    )
                if template == "skeleton" and provider_key in {"hidream", "hidream-i1", "hidream_i1", "wan", "wan22", "wan2.2"}:
                    try:
                        lora_available = await check_skeleton_lora_available()
                    except Exception:
                        lora_available = False
                    if lora_available:
                        refine_prompt_l = str(attempt_prompt or "").lower()
                        wants_glass_refine = (
                            has_reference
                            or "glass" in refine_prompt_l
                            or "translucent" in refine_prompt_l
                            or "shell" in refine_prompt_l
                            or "table" in refine_prompt_l
                            or "brain" in refine_prompt_l
                            or bool(re.search(r"\b(money|cash|banknotes?)\b", refine_prompt_l))
                        )
                        if wants_glass_refine:
                            refine_path = str(Path(output_path).with_name(Path(output_path).stem + "_glass_refine.png"))
                            refine_denoise = 0.24 if has_reference else 0.30
                            try:
                                await generate_image_skeleton_lora(
                                    attempt_prompt,
                                    refine_path,
                                    resolution=resolution,
                                    source_image_path=output_path,
                                    denoise=refine_denoise,
                                    negative_prompt=attempt_negative,
                                )
                                shutil.move(refine_path, output_path)
                                log.info(f"Applied Jerry glass-shell LoRA refinement via SDXL ({provider_key} -> skeleton refine)")
                            except Exception as refine_err:
                                Path(refine_path).unlink(missing_ok=True)
                                log.warning(f"Jerry glass-shell LoRA refinement skipped: {refine_err}")
                await _enforce_1080_image(output_path)
                _postprocess_generated_image(output_path, provider=provider_key, template=template)
                _ensure_generated_image_valid(output_path)
                qa_prompt = skeleton_source_prompt if template == "skeleton" else prompt
                qa = _score_generated_image_quality(output_path, prompt=qa_prompt, template=template)
                qa_gate_ok, qa_gate_min = _image_quality_gate(
                    qa,
                    template=template,
                    lock_mode=lock_mode,
                    has_reference=has_reference,
                    prompt=qa_prompt,
                )
                if template == "skeleton" and interactive_fast:
                    current_score = float(qa.get("score", 0.0) or 0.0)
                    best_score = float((best_soft_result or {}).get("qa_score", -1.0))
                    if best_soft_result is None or current_score > best_score:
                        try:
                            shutil.copyfile(output_path, str(best_soft_path))
                        except Exception:
                            pass
                        best_soft_result = {
                            "local_path": output_path,
                            "cdn_url": None,
                            "provider": provider_key,
                            "qa_score": current_score,
                            "qa_ok": bool(qa_gate_ok),
                            "qa_min_score": qa_gate_min,
                            "qa_notes": list(qa.get("notes", [])),
                            "attempt": attempt,
                        }
                if template == "skeleton" and not qa_gate_ok and attempt < attempts:
                    log.warning(
                        f"Skeleton image below QA gate via '{provider_key}' (attempt {attempt}/{attempts}, score={qa.get('score', 0.0):.2f}, "
                        f"min={qa_gate_min:.2f}); retrying."
                    )
                    continue
                if template == "skeleton" and not qa_gate_ok and attempt >= attempts:
                    if interactive_fast and has_reference and provider_key in {"hidream", "hidream-i1", "hidream_i1"}:
                        notes = _interactive_soft_accept_notes(qa.get("notes", []) or [])
                        if not _skeleton_notes_are_severe(notes):
                            _provider_mark_success(provider_key)
                            return {
                                "local_path": output_path,
                                "cdn_url": None,
                                "provider": provider_key,
                                "qa_score": qa.get("score", 0.0),
                                "qa_ok": False,
                                "qa_min_score": qa_gate_min,
                                "qa_notes": notes,
                                "attempt": attempt,
                            }
                    if interactive_fast and best_soft_result is not None:
                        best_notes = list(best_soft_result.get("qa_notes", []) or [])
                        severe = _skeleton_notes_are_severe(best_notes)
                        if not severe:
                            if best_soft_path.exists():
                                try:
                                    shutil.copyfile(str(best_soft_path), output_path)
                                except Exception:
                                    pass
                            notes = _interactive_soft_accept_notes(best_soft_result.get("qa_notes", []) or [])
                            best_soft_result["qa_notes"] = notes
                            best_soft_result["qa_ok"] = False
                            _provider_mark_success(provider_key)
                            best_soft_path.unlink(missing_ok=True)
                            log.warning(
                                f"Interactive fast-mode soft-accepted skeleton image via '{provider_key}' "
                                f"(score={best_soft_result.get('qa_score', 0.0):.2f}, notes={best_soft_result.get('qa_notes', [])})"
                            )
                            return best_soft_result
                    raise RuntimeError(
                        f"Skeleton QA gate failed for provider '{provider_key}' after {attempts} attempts "
                        f"(score={qa.get('score', 0.0):.2f}, min={qa_gate_min:.2f}, notes={qa.get('notes', [])})"
                    )
                _provider_mark_success(provider_key)
                best_soft_path.unlink(missing_ok=True)
                return {
                    "local_path": output_path,
                    "cdn_url": None,
                    "provider": provider_key,
                    "qa_score": qa.get("score", 0.0),
                    "qa_ok": bool(qa_gate_ok),
                    "qa_min_score": qa_gate_min,
                    "qa_notes": qa.get("notes", []),
                    "attempt": attempt,
                }
            except Exception as err:
                last_err = err
                err_l = str(err or "").lower()
                # Avoid stacking timed-out WAN jobs (they can still be running in ComfyUI and consume VRAM).
                if (
                    interactive_fast
                    and provider_key in {"hidream", "hidream-i1", "hidream_i1", "wan", "wan22", "wan2.2"}
                    and ("timed out" in err_l or "timeout" in err_l)
                ):
                    break
                if attempt < attempts:
                    await asyncio.sleep(min(5, attempt))
        if interactive_fast and template == "skeleton" and best_soft_result is not None:
            best_notes = list(best_soft_result.get("qa_notes", []) or [])
            severe = _skeleton_notes_are_severe(best_notes)
            if not severe:
                if best_soft_path.exists():
                    try:
                        shutil.copyfile(str(best_soft_path), output_path)
                    except Exception:
                        pass
                notes = _interactive_soft_accept_notes(best_soft_result.get("qa_notes", []) or [])
                best_soft_result["qa_notes"] = notes
                best_soft_result["qa_ok"] = False
                _provider_mark_success(provider_key)
                best_soft_path.unlink(missing_ok=True)
                log.warning(
                    f"Interactive fast-mode returning best available skeleton image via '{provider_key}' "
                    f"(score={best_soft_result.get('qa_score', 0.0):.2f}, notes={best_soft_result.get('qa_notes', [])})"
                )
                return best_soft_result
        best_soft_path.unlink(missing_ok=True)
        _provider_mark_failure(provider_key, reason=str(last_err or "unknown_error"))
        mode_suffix = f" (mode={wan_runtime_mode})" if wan_runtime_mode else ""
        raise RuntimeError(f"provider '{provider_key}' failed after {attempts} attempts{mode_suffix}: {last_err}")

    last_local_provider_err: Exception | None = None
    best_soft_local_result: dict | None = None
    xai_index = next((i for i, p in enumerate(provider_order) if p in xai_aliases), len(provider_order))
    local_providers = list(provider_order[:xai_index])
    for idx, provider in enumerate(local_providers):
        provider_norm = _normalize_image_provider_key(provider)
        next_provider = local_providers[idx + 1] if (idx + 1) < len(local_providers) else ""
        try:
            local_result = await _local_provider_result(provider)
            if local_result:
                if (
                    template == "skeleton"
                    and interactive_fast
                    and provider_norm in {"hidream", "wan22"}
                    and not bool(local_result.get("qa_ok", False))
                    and bool(next_provider)
                    and not skeleton_wan_lock
                ):
                    score = float(local_result.get("qa_score", 0.0) or 0.0)
                    best_score = float((best_soft_local_result or {}).get("qa_score", -1.0) or -1.0)
                    if best_soft_local_result is None or score > best_score:
                        best_soft_local_result = dict(local_result)
                    _record_provider_fallback(provider, next_provider)
                    log.warning(
                        f"Skeleton interactive: '{provider_norm}' soft result score={score:.2f}; trying next local provider '{_normalize_image_provider_key(next_provider)}'"
                    )
                    continue
                if (
                    template == "skeleton"
                    and interactive_fast
                    and best_soft_local_result is not None
                    and not bool(local_result.get("qa_ok", False))
                ):
                    current = float(local_result.get("qa_score", 0.0) or 0.0)
                    best = float(best_soft_local_result.get("qa_score", 0.0) or 0.0)
                    if current >= best:
                        return local_result
                    return best_soft_local_result
                return local_result
        except Exception as local_err:
            last_local_provider_err = local_err
            _record_provider_fallback(provider, "next")
            log.warning(f"Local image provider '{provider}' failed, trying next: {local_err}")
    if best_soft_local_result is not None:
        return best_soft_local_result

    xai_enabled = (
        not channel_blocks_fal_scene
        and
        XAI_IMAGE_FALLBACK_ENABLED
        and any(p in xai_aliases for p in provider_order)
        and bool(FAL_AI_KEY or XAI_API_KEY)
    )
    if xai_enabled:
        try:
            if best_of_enabled and IMAGE_QUALITY_BESTOF_ENABLED and IMAGE_QUALITY_BESTOF_COUNT > 1:
                best_count = max(2, int(IMAGE_QUALITY_BESTOF_COUNT))
                if template in {"skeleton", "story", "motivation"}:
                    best_count = max(best_count, 4)
                if lock_mode == "strict" and reference_image_url and template in {"skeleton", "story"}:
                    best_count = max(best_count, 5)
                if template == "skeleton":
                    best_count = max(best_count, 5)
                    if lock_mode == "strict" and has_reference:
                        best_count = max(best_count, 6)
                cand_root = Path(output_path)
                candidates = []
                for idx in range(best_count):
                    cand_path = str(cand_root.with_name(f"{cand_root.stem}_cand_{idx}{cand_root.suffix or '.png'}"))
                    try:
                        cand_result = await generate_image_grok(
                            prompt,
                            cand_path,
                            resolution=resolution,
                            reference_image_url=reference_image_url,
                            reference_lock_mode=lock_mode,
                        )
                        await _enforce_1080_image(cand_path)
                        qa = _score_generated_image_quality(cand_path, prompt=prompt, template=template)
                        gate_ok, gate_min = _image_quality_gate(
                            qa,
                            template=template,
                            lock_mode=lock_mode,
                            has_reference=has_reference,
                            prompt=prompt,
                        )
                        qa["gate_ok"] = gate_ok
                        qa["gate_min_score"] = gate_min
                        candidates.append({"path": cand_path, "result": cand_result, "qa": qa, "idx": idx})
                        log.info(
                            f"Best-of candidate {idx+1}/{best_count} score={qa.get('score', 0.0)} "
                            f"gate_ok={qa.get('gate_ok', False)} min={qa.get('gate_min_score', 0.0)} "
                            f"notes={','.join(qa.get('notes', []))}"
                        )
                    except Exception as cand_err:
                        log.warning(f"Best-of candidate {idx+1}/{best_count} failed: {cand_err}")
                if not candidates:
                    raise RuntimeError("all best-of image candidates failed")

                acceptable = [c for c in candidates if bool(c.get("qa", {}).get("gate_ok", False))]
                winner = max((acceptable or candidates), key=lambda c: float(c.get("qa", {}).get("score", 0.0)))
                winner_path = str(winner["path"])
                winner_result = dict(winner["result"])
                winner_qa = dict(winner.get("qa", {}))
                if Path(winner_path).resolve() != Path(output_path).resolve():
                    shutil.copyfile(winner_path, output_path)
                winner_result["local_path"] = output_path
                winner_result["qa_score"] = winner_qa.get("score", 0.0)
                winner_result["qa_ok"] = bool(winner_qa.get("gate_ok", False))
                winner_result["qa_min_score"] = winner_qa.get("gate_min_score", _image_quality_min_score(template=template, lock_mode=lock_mode, has_reference=has_reference))
                winner_result["qa_notes"] = winner_qa.get("notes", [])
                # Story salvage pass: if winner is still below threshold, do one extra high-realism attempt.
                if salvage_enabled and template == "story" and not winner_result["qa_ok"]:
                    try:
                        salvage_prompt = f"{prompt} {_story_salvage_refinement_for_scene(prompt)}"
                        salvage_path = str(cand_root.with_name(f"{cand_root.stem}_salvage{cand_root.suffix or '.png'}"))
                        salvage_result = await generate_image_grok(
                            salvage_prompt,
                            salvage_path,
                            resolution=resolution,
                            reference_image_url=reference_image_url,
                            reference_lock_mode=lock_mode,
                        )
                        await _enforce_1080_image(salvage_path)
                        salvage_qa = _score_generated_image_quality(salvage_path, prompt=salvage_prompt, template=template)
                        salvage_gate_ok, salvage_gate_min = _image_quality_gate(
                            salvage_qa,
                            template=template,
                            lock_mode=lock_mode,
                            has_reference=has_reference,
                            prompt=salvage_prompt,
                        )
                        salvage_qa["gate_ok"] = salvage_gate_ok
                        salvage_qa["gate_min_score"] = salvage_gate_min
                        if float(salvage_qa.get("score", 0.0)) > float(winner_result.get("qa_score", 0.0)):
                            shutil.copyfile(salvage_path, output_path)
                            winner_result = dict(salvage_result)
                            winner_result["local_path"] = output_path
                            winner_result["qa_score"] = salvage_qa.get("score", 0.0)
                            winner_result["qa_ok"] = bool(salvage_qa.get("gate_ok", False))
                            winner_result["qa_min_score"] = salvage_qa.get("gate_min_score", winner_result.get("qa_min_score", 0.0))
                            winner_result["qa_notes"] = salvage_qa.get("notes", [])
                            log.info(f"Story salvage image replaced winner, score={winner_result['qa_score']}")
                        Path(salvage_path).unlink(missing_ok=True)
                    except Exception as salvage_err:
                        log.warning(f"Story salvage pass failed: {salvage_err}")
                if salvage_enabled and template == "skeleton" and not winner_result["qa_ok"]:
                    for salvage_idx in range(1):
                        try:
                            salvage_prompt = _skeleton_repair_prompt(prompt)
                            salvage_path = str(cand_root.with_name(f"{cand_root.stem}_skeleton_repair_{salvage_idx}{cand_root.suffix or '.png'}"))
                            salvage_result = await generate_image_grok(
                                salvage_prompt,
                                salvage_path,
                                resolution=resolution,
                                reference_image_url=reference_image_url,
                                reference_lock_mode=lock_mode,
                            )
                            await _enforce_1080_image(salvage_path)
                            salvage_qa = _score_generated_image_quality(salvage_path, prompt=salvage_prompt, template=template)
                            salvage_gate_ok, salvage_gate_min = _image_quality_gate(
                                salvage_qa,
                                template=template,
                                lock_mode=lock_mode,
                                has_reference=has_reference,
                                prompt=salvage_prompt,
                            )
                            salvage_qa["gate_ok"] = salvage_gate_ok
                            salvage_qa["gate_min_score"] = salvage_gate_min
                            if (
                                salvage_gate_ok
                                or float(salvage_qa.get("score", 0.0)) > float(winner_result.get("qa_score", 0.0))
                            ):
                                shutil.copyfile(salvage_path, output_path)
                                winner_result = dict(salvage_result)
                                winner_result["local_path"] = output_path
                                winner_result["qa_score"] = salvage_qa.get("score", 0.0)
                                winner_result["qa_ok"] = bool(salvage_qa.get("gate_ok", False))
                                winner_result["qa_min_score"] = salvage_qa.get("gate_min_score", winner_result.get("qa_min_score", 0.0))
                                winner_result["qa_notes"] = salvage_qa.get("notes", [])
                                log.info(
                                    f"Skeleton repair pass {salvage_idx + 1}/1 score={winner_result['qa_score']} "
                                    f"ok={winner_result['qa_ok']}"
                                )
                            Path(salvage_path).unlink(missing_ok=True)
                            if winner_result["qa_ok"]:
                                break
                        except Exception as salvage_err:
                            log.warning(f"Skeleton repair pass {salvage_idx + 1}/1 failed: {salvage_err}")
                if not winner_result["qa_ok"]:
                    if template == "skeleton":
                        notes = _interactive_soft_accept_notes(winner_result.get("qa_notes", []) or [])
                        if not _skeleton_notes_are_severe(notes):
                            winner_result["qa_notes"] = notes
                            winner_result["qa_ok"] = False
                            log.warning(
                                "Skeleton hosted best-of soft-accepted "
                                f"(score={winner_result.get('qa_score', 0.0):.2f}, notes={winner_result.get('qa_notes', [])})"
                            )
                            for cand in candidates:
                                p = Path(str(cand.get("path", "") or ""))
                                if p.exists() and p.resolve() != Path(output_path).resolve():
                                    p.unlink(missing_ok=True)
                            return winner_result
                        raise RuntimeError(
                            "Skeleton best-of candidates failed QA gate "
                            f"(score={winner_result.get('qa_score', 0.0)}, notes={winner_result.get('qa_notes', [])})"
                        )
                    log.warning(
                        f"Best-of winner below threshold {winner_result.get('qa_min_score', IMAGE_QUALITY_MIN_SCORE)}: "
                        f"score={winner_result['qa_score']} path={Path(output_path).name}"
                    )
                else:
                    log.info(
                        f"Best-of winner selected score={winner_result['qa_score']} "
                        f"candidate={winner.get('idx', -1)+1}/{best_count}"
                    )

                for cand in candidates:
                    p = Path(str(cand.get("path", "") or ""))
                    if p.exists() and p.resolve() != Path(output_path).resolve():
                        p.unlink(missing_ok=True)
                return winner_result

            result = await generate_image_grok(
                prompt,
                output_path,
                resolution=resolution,
                reference_image_url=reference_image_url,
                reference_lock_mode=lock_mode,
            )
            await _enforce_1080_image(output_path)
            qa = _score_generated_image_quality(output_path, prompt=prompt, template=template)
            qa_gate_ok, qa_gate_min = _image_quality_gate(
                qa,
                template=template,
                lock_mode=lock_mode,
                has_reference=has_reference,
                prompt=prompt,
            )
            result["qa_score"] = qa.get("score", 0.0)
            result["qa_ok"] = bool(qa_gate_ok)
            result["qa_min_score"] = qa_gate_min
            result["qa_notes"] = qa.get("notes", [])
            if salvage_enabled and template == "story" and not result["qa_ok"]:
                try:
                    salvage_prompt = f"{prompt} {_story_salvage_refinement_for_scene(prompt)}"
                    salvage_path = str(Path(output_path).with_name(Path(output_path).stem + "_salvage" + Path(output_path).suffix))
                    salvage_result = await generate_image_grok(
                        salvage_prompt,
                        salvage_path,
                        resolution=resolution,
                        reference_image_url=reference_image_url,
                    )
                    await _enforce_1080_image(salvage_path)
                    salvage_qa = _score_generated_image_quality(salvage_path, prompt=salvage_prompt, template=template)
                    salvage_gate_ok, salvage_gate_min = _image_quality_gate(
                        salvage_qa,
                        template=template,
                        lock_mode=lock_mode,
                        has_reference=has_reference,
                        prompt=salvage_prompt,
                    )
                    if float(salvage_qa.get("score", 0.0)) > float(result.get("qa_score", 0.0)):
                        shutil.copyfile(salvage_path, output_path)
                        result = dict(salvage_result)
                        result["local_path"] = output_path
                        result["qa_score"] = salvage_qa.get("score", 0.0)
                        result["qa_ok"] = bool(salvage_gate_ok)
                        result["qa_min_score"] = salvage_gate_min
                        result["qa_notes"] = salvage_qa.get("notes", [])
                        log.info(f"Story single-pass salvage improved image score={result['qa_score']}")
                    Path(salvage_path).unlink(missing_ok=True)
                except Exception as salvage_err:
                    log.warning(f"Story single-pass salvage failed: {salvage_err}")
            if salvage_enabled and template == "skeleton" and not result["qa_ok"]:
                for salvage_idx in range(1):
                    try:
                        salvage_prompt = _skeleton_repair_prompt(prompt)
                        salvage_path = str(Path(output_path).with_name(Path(output_path).stem + f"_skeleton_repair_{salvage_idx}" + Path(output_path).suffix))
                        salvage_result = await generate_image_grok(
                            salvage_prompt,
                            salvage_path,
                            resolution=resolution,
                            reference_image_url=reference_image_url,
                            reference_lock_mode=lock_mode,
                        )
                        await _enforce_1080_image(salvage_path)
                        salvage_qa = _score_generated_image_quality(salvage_path, prompt=salvage_prompt, template=template)
                        salvage_gate_ok, salvage_gate_min = _image_quality_gate(
                            salvage_qa,
                            template=template,
                            lock_mode=lock_mode,
                            has_reference=has_reference,
                            prompt=salvage_prompt,
                        )
                        if salvage_gate_ok or float(salvage_qa.get("score", 0.0)) > float(result.get("qa_score", 0.0)):
                            shutil.copyfile(salvage_path, output_path)
                            result = dict(salvage_result)
                            result["local_path"] = output_path
                            result["qa_score"] = salvage_qa.get("score", 0.0)
                            result["qa_ok"] = bool(salvage_gate_ok)
                            result["qa_min_score"] = salvage_gate_min
                            result["qa_notes"] = salvage_qa.get("notes", [])
                            log.info(f"Skeleton single-pass repair {salvage_idx + 1}/1 score={result['qa_score']} ok={result['qa_ok']}")
                        Path(salvage_path).unlink(missing_ok=True)
                        if result["qa_ok"]:
                            break
                    except Exception as salvage_err:
                        log.warning(f"Skeleton single-pass repair {salvage_idx + 1}/1 failed: {salvage_err}")
            if template == "skeleton" and not bool(result.get("qa_ok", False)):
                notes = _interactive_soft_accept_notes(result.get("qa_notes", []) or [])
                if not _skeleton_notes_are_severe(notes):
                    result["qa_notes"] = notes
                    result["qa_ok"] = False
                    return result
                raise RuntimeError(
                    "Skeleton single-pass generation failed QA gate "
                    f"(score={result.get('qa_score', 0.0)}, notes={result.get('qa_notes', [])})"
                )
            return result
        except Exception as e:
            log.warning(f"Grok image generation failed (fal.ai + xAI direct), falling back to SDXL: {e}")

    async def _hosted_fal_backup_result() -> dict | None:
        if not FAL_AI_KEY:
            return None
        fallback_candidates: list[str] = []
        primary_backup = _normalize_fal_image_backup_model(FAL_IMAGE_BACKUP_MODEL)
        if primary_backup and primary_backup != "grok_imagine":
            fallback_candidates.append(primary_backup)
        if template == "skeleton":
            fallback_candidates.extend(["imagen4_fast", "seedream45", "imagen4_ultra", "recraft_v4", "flux_2_pro"])
        else:
            fallback_candidates.extend(["imagen4_fast", "recraft_v4", "seedream45", "imagen4_ultra", "flux_2_pro"])
        seen_candidates: set[str] = set()
        for candidate in fallback_candidates:
            candidate_id = _normalize_creative_image_model_id(candidate, template=template)
            if not candidate_id or candidate_id == "grok_imagine" or candidate_id in seen_candidates:
                continue
            seen_candidates.add(candidate_id)
            try:
                backup_result = await _generate_image_fal_selected_model(
                    candidate_id,
                    prompt,
                    output_path,
                    resolution=resolution,
                    negative_prompt=negative_prompt,
                    reference_image_url=reference_image_url,
                )
                await _enforce_1080_image(output_path)
                _ensure_generated_image_valid(output_path)
                qa = _score_generated_image_quality(output_path, prompt=prompt, template=template)
                qa_gate_ok, qa_gate_min = _image_quality_gate(
                    qa,
                    template=template,
                    lock_mode=lock_mode,
                    has_reference=has_reference,
                    prompt=prompt,
                )
                if template == "skeleton" and not qa_gate_ok:
                    notes = _interactive_soft_accept_notes(qa.get("notes", []) or [])
                    if _skeleton_notes_are_severe(notes):
                        raise RuntimeError(
                            f"Hosted fallback '{candidate_id}' failed skeleton QA gate "
                            f"(score={qa.get('score', 0.0)}, notes={qa.get('notes', [])})"
                        )
                    backup_result["qa_notes"] = notes
                backup_result["provider"] = candidate_id
                backup_result["qa_score"] = qa.get("score", 0.0)
                backup_result["qa_ok"] = bool(qa_gate_ok)
                backup_result["qa_min_score"] = qa_gate_min
                backup_result["qa_notes"] = backup_result.get("qa_notes", qa.get("notes", []))
                log.info(f"Hosted fal backup image succeeded via {candidate_id}")
                return backup_result
            except Exception as backup_err:
                log.warning(f"Hosted fal backup '{candidate_id}' failed: {backup_err}")
        return None

    hosted_backup_result = await _hosted_fal_backup_result()
    if hosted_backup_result:
        return hosted_backup_result

    for provider in provider_order[xai_index + 1:]:
        try:
            local_result = await _local_provider_result(provider)
            if local_result:
                return local_result
        except Exception as local_err:
            last_local_provider_err = local_err
            _record_provider_fallback(provider, "next")
            log.warning(f"Post-xAI local provider '{provider}' failed: {local_err}")

    if template == "skeleton" and interactive_fast:
        if last_local_provider_err is not None:
            raise RuntimeError(str(last_local_provider_err))
        raise RuntimeError("Skeleton interactive generation exhausted WAN2.2 attempt budget")

    if not _configured_local_image_provider_order():
        if last_local_provider_err is not None:
            raise RuntimeError(f"Remote image generation failed and no local providers are configured: {last_local_provider_err}")
        raise RuntimeError("Remote image generation failed and no local providers are configured")

    safe_route = dict(template_adapter_route or {})
    safe_route["enabled"] = False
    safe_route["loras"] = []
    await generate_image_comfyui(
        prompt,
        output_path,
        resolution=resolution,
        negative_prompt=negative_prompt,
        allow_default_negative=not prompt_passthrough,
        template=template,
        adapter_route=safe_route,
        max_wait_sec=(16 if interactive_fast else 900),
    )
    await _enforce_1080_image(output_path)
    _ensure_generated_image_valid(output_path)
    qa = _score_generated_image_quality(output_path, prompt=prompt, template=template)
    qa_gate_ok, qa_gate_min = _image_quality_gate(
        qa,
        template=template,
        lock_mode=lock_mode,
        has_reference=has_reference,
        prompt=prompt,
    )
    if template == "skeleton" and not qa_gate_ok:
        if interactive_fast:
            notes = _interactive_soft_accept_notes(qa.get("notes", []) or [])
            if not _skeleton_notes_are_severe(notes):
                return {
                    "local_path": output_path,
                    "cdn_url": None,
                    "qa_score": qa.get("score", 0.0),
                    "qa_ok": False,
                    "qa_min_score": qa_gate_min,
                    "qa_notes": notes,
                }
        raise RuntimeError(
            "Skeleton fallback generation failed QA gate "
            f"(score={qa.get('score', 0.0)}, notes={qa.get('notes', [])})"
        )
    return {
        "local_path": output_path,
        "cdn_url": None,
        "qa_score": qa.get("score", 0.0),
        "qa_ok": bool(qa_gate_ok),
        "qa_min_score": qa_gate_min,
        "qa_notes": qa.get("notes", []),
    }


async def generate_image_comfyui(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    negative_prompt: str = "",
    allow_default_negative: bool = True,
    template: str = "",
    adapter_route: Optional[dict] = None,
    max_wait_sec: int = 900,
) -> str:
    """Generate image via ComfyUI SDXL with optional template adapter stack."""
    config = RESOLUTION_CONFIGS[resolution]
    route = _resolve_template_adapter_route(template, override_route=adapter_route)
    checkpoint_name = await _resolve_checkpoint_name(route.get("checkpoint", "sd_xl_base_1.0.safetensors"))
    loras = []
    if route.get("enabled", True):
        loras = await _resolve_available_loras(list(route.get("loras", []) or []))
    if loras:
        lora_names = ", ".join(str(l.get("name", "")) for l in loras)
        log.info(f"Applying adapter route for template '{template or 'default'}': {lora_names}")
    safe_negative_prompt = str(negative_prompt or "").strip()
    if not safe_negative_prompt and allow_default_negative:
        safe_negative_prompt = NEGATIVE_PROMPT
    workflow, output_node = _build_sdxl_workflow_with_loras(
        prompt=str(prompt or "").strip(),
        negative_prompt=safe_negative_prompt,
        width=int(config["gen_width"]),
        height=int(config["gen_height"]),
        checkpoint_name=checkpoint_name,
        loras=loras,
        upscale=bool(config.get("upscale")),
        upscale_factor=float(config.get("upscale_factor", 1.0) or 1.0),
        filename_prefix="nyptid_gen",
    )
    result = await _run_comfyui_workflow(workflow, output_node, "images", max_wait_sec=max_wait_sec, poll_interval_sec=2.0)
    await _download_comfyui_file(result["images"][0], output_path)
    return output_path


async def check_hidream_available(force_refresh: bool = False) -> bool:
    """Best-effort HiDream availability with short TTL and stale-OK guard."""
    now = time.time()
    checked_ts = float(_hidream_availability_cache.get("checked_ts", 0.0) or 0.0)
    if not force_refresh and checked_ts > 0 and (now - checked_ts) <= _HIDREAM_AVAILABILITY_TTL_SEC:
        return bool(_hidream_availability_cache.get("ready", False))
    try:
        assets = await _resolve_hidream_runtime_assets()
        _hidream_availability_cache["checked_ts"] = now
        _hidream_availability_cache["ready"] = True
        _hidream_availability_cache["last_ok_ts"] = now
        _hidream_availability_cache["last_error"] = ""
        _hidream_availability_cache["model_name"] = str((assets or {}).get("model_name", "") or "")
        _hidream_availability_cache["clip_l"] = str((assets or {}).get("clip_l_name", "") or "")
        _hidream_availability_cache["clip_g"] = str((assets or {}).get("clip_g_name", "") or "")
        _hidream_availability_cache["t5_name"] = str((assets or {}).get("t5_name", "") or "")
        _hidream_availability_cache["llama_name"] = str((assets or {}).get("llama_name", "") or "")
        _hidream_availability_cache["vae_name"] = str((assets or {}).get("vae_name", "") or "")
        return True
    except Exception as e:
        _hidream_availability_cache["checked_ts"] = now
        _hidream_availability_cache["ready"] = False
        _hidream_availability_cache["last_error"] = str(e)
        last_ok_ts = float(_hidream_availability_cache.get("last_ok_ts", 0.0) or 0.0)
        if last_ok_ts > 0 and (now - last_ok_ts) <= _HIDREAM_STALE_OK_SEC:
            age = int(now - last_ok_ts)
            log.warning(f"HiDream probe failed but using stale healthy state ({age}s old): {e}")
            return True
        return False


async def check_hidream_edit_available(force_refresh: bool = False) -> bool:
    """Best-effort HiDream E1.1 availability with short TTL and stale-OK guard."""
    now = time.time()
    checked_ts = float(_hidream_edit_availability_cache.get("checked_ts", 0.0) or 0.0)
    if not force_refresh and checked_ts > 0 and (now - checked_ts) <= _HIDREAM_AVAILABILITY_TTL_SEC:
        return bool(_hidream_edit_availability_cache.get("ready", False))
    try:
        assets = await _resolve_hidream_edit_runtime_assets()
        _hidream_edit_availability_cache["checked_ts"] = now
        _hidream_edit_availability_cache["ready"] = True
        _hidream_edit_availability_cache["last_ok_ts"] = now
        _hidream_edit_availability_cache["last_error"] = ""
        _hidream_edit_availability_cache["model_name"] = str((assets or {}).get("model_name", "") or "")
        _hidream_edit_availability_cache["clip_l"] = str((assets or {}).get("clip_l_name", "") or "")
        _hidream_edit_availability_cache["clip_g"] = str((assets or {}).get("clip_g_name", "") or "")
        _hidream_edit_availability_cache["t5_name"] = str((assets or {}).get("t5_name", "") or "")
        _hidream_edit_availability_cache["llama_name"] = str((assets or {}).get("llama_name", "") or "")
        _hidream_edit_availability_cache["vae_name"] = str((assets or {}).get("vae_name", "") or "")
        return True
    except Exception as e:
        _hidream_edit_availability_cache["checked_ts"] = now
        _hidream_edit_availability_cache["ready"] = False
        _hidream_edit_availability_cache["last_error"] = str(e)
        last_ok_ts = float(_hidream_edit_availability_cache.get("last_ok_ts", 0.0) or 0.0)
        if last_ok_ts > 0 and (now - last_ok_ts) <= _HIDREAM_STALE_OK_SEC:
            age = int(now - last_ok_ts)
            log.warning(f"HiDream E1.1 probe failed but using stale healthy state ({age}s old): {e}")
            return True
        return False


async def check_wan22_available() -> bool:
    """Check if the Wan 2.2 I2V models exist on the ComfyUI server."""
    global _active_comfyui_url
    for base_url in _comfyui_candidate_urls():
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(f"{base_url}/object_info")
                if resp.status_code == 200:
                    content = resp.text
                    if WAN22_I2V_HIGH.split(".")[0] in content or "wan2.2" in content.lower():
                        _active_comfyui_url = base_url
                        return True
        except Exception as e:
            log.warning(f"Wan 2.2 availability check failed on {base_url}: {e}")
    return False


async def check_wan22_t2i_available(force_refresh: bool = False) -> bool:
    """Best-effort WAN T2I availability with short TTL and stale-OK guard."""
    now = time.time()
    checked_ts = float(_wan22_t2i_availability_cache.get("checked_ts", 0.0) or 0.0)
    if not force_refresh and checked_ts > 0 and (now - checked_ts) <= _WAN22_T2I_AVAILABILITY_TTL_SEC:
        return bool(_wan22_t2i_availability_cache.get("ready", False))
    try:
        assets = await _resolve_wan22_t2i_runtime_assets()
        _wan22_t2i_availability_cache["checked_ts"] = now
        _wan22_t2i_availability_cache["ready"] = True
        _wan22_t2i_availability_cache["last_ok_ts"] = now
        _wan22_t2i_availability_cache["last_error"] = ""
        _wan22_t2i_availability_cache["mode"] = str((assets or {}).get("mode", "") or "")
        _wan22_t2i_availability_cache["ckpt_name"] = str((assets or {}).get("ckpt_name", "") or "")
        _wan22_t2i_availability_cache["unet_name"] = str((assets or {}).get("unet_name", "") or "")
        return True
    except Exception as e:
        _wan22_t2i_availability_cache["checked_ts"] = now
        _wan22_t2i_availability_cache["ready"] = False
        _wan22_t2i_availability_cache["last_error"] = str(e)
        last_ok_ts = float(_wan22_t2i_availability_cache.get("last_ok_ts", 0.0) or 0.0)
        if last_ok_ts > 0 and (now - last_ok_ts) <= _WAN22_T2I_STALE_OK_SEC:
            age = int(now - last_ok_ts)
            log.warning(
                f"WAN22 T2I probe failed but using stale healthy state ({age}s old): {e}"
            )
            return True
        return False


RUNPOD_SSH_HOST = "root@69.30.85.41"
RUNPOD_SSH_PORT = "22118"
COMFYUI_INPUT_DIR = "/workspace/ComfyUI/input"

FAL_SUBMIT_URL = "https://queue.fal.run/fal-ai/kling-video/v2.1/standard/image-to-video"
FAL_STATUS_URL = "https://queue.fal.run/fal-ai/kling-video/v2.1/standard/image-to-video/requests"
FAL_UPLOAD_URL = "https://fal.run/fal-ai/fal-file-storage/upload"


def _fal_queue_urls(endpoint_id: str) -> tuple[str, str]:
    base = "https://queue.fal.run/" + endpoint_id.strip("/")
    return base, base + "/requests"


def _build_fal_video_payload(
    profile: dict,
    prompt: str,
    image_url: str,
    duration_sec: float,
    aspect_ratio: str,
) -> dict:
    model_id = str(profile.get("id", "") or "").strip().lower()
    payload: dict[str, object] = {
        "prompt": prompt,
        "image_url": image_url,
        "aspect_ratio": aspect_ratio,
    }
    if model_id.startswith("kling21_"):
        payload["duration"] = str(10 if float(duration_sec) >= 7.5 else 5)
        payload["negative_prompt"] = "blur, distort, low quality, watermark, text overlay, UI elements"
        payload["cfg_scale"] = 0.5
    elif model_id == "veo3_fast":
        payload["generate_audio"] = False
    return payload


async def animate_image_fal_queue_model(
    endpoint_id: str,
    image_path: str,
    prompt: str,
    output_clip_path: str,
    *,
    duration_sec: float = 5,
    aspect_ratio: str = "9:16",
    image_cdn_url: str = None,
    profile: dict | None = None,
) -> str:
    if not FAL_AI_KEY:
        raise RuntimeError("FAL_AI_KEY not configured")
    if image_cdn_url:
        image_url = image_cdn_url
    else:
        image_url = await _upload_image_to_fal(image_path)

    active_profile = dict(profile or {})
    submit_url, requests_base = _fal_queue_urls(endpoint_id)
    headers = {
        "Authorization": "Key " + FAL_AI_KEY,
        "Content-Type": "application/json",
    }
    payload = _build_fal_video_payload(active_profile, prompt, image_url, duration_sec, aspect_ratio)

    async with httpx.AsyncClient(timeout=45) as client:
        resp = await client.post(submit_url, headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"{active_profile.get('label', endpoint_id)} submit failed ({resp.status_code}): {resp.text[:300]}"
            )
        submit_data = resp.json()

    request_id = submit_data.get("request_id")
    if not request_id:
        direct_video_url = str(((submit_data.get("video", {}) or {}).get("url", "")) or "")
        if direct_video_url:
            await _download_url_to_file(direct_video_url, output_clip_path)
            return output_clip_path
        raise RuntimeError(f"No request_id from {endpoint_id}: " + json.dumps(submit_data)[:300])

    status_url = submit_data.get("status_url", requests_base + "/" + request_id + "/status")
    result_url = submit_data.get("response_url", requests_base + "/" + request_id)
    max_wait = 900
    poll_interval = 5
    elapsed = 0
    while elapsed < max_wait:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
        async with httpx.AsyncClient(timeout=45) as client:
            st_resp = await client.get(status_url, headers={"Authorization": "Key " + FAL_AI_KEY})
            if st_resp.status_code == 202:
                continue
            if st_resp.status_code != 200:
                log.warning(f"{endpoint_id} status poll HTTP {st_resp.status_code}: {st_resp.text[:200]}")
                continue
            st_data = st_resp.json()
            status = str(st_data.get("status", "") or "")
            if status == "COMPLETED":
                break
            if status in {"FAILED", "CANCELLED"}:
                raise RuntimeError(f"{active_profile.get('label', endpoint_id)} generation failed: " + json.dumps(st_data)[:300])
        if poll_interval < 15:
            poll_interval = min(poll_interval + 2, 15)
    else:
        raise TimeoutError(f"{active_profile.get('label', endpoint_id)} timed out after {max_wait}s")

    async with httpx.AsyncClient(timeout=60) as client:
        res_resp = await client.get(result_url, headers={"Authorization": "Key " + FAL_AI_KEY})
        if res_resp.status_code != 200:
            fallback_result_url = status_url[:-7] if str(status_url).endswith("/status") else ""
            if fallback_result_url:
                res_resp = await client.get(fallback_result_url, headers={"Authorization": "Key " + FAL_AI_KEY})
        if res_resp.status_code != 200:
            raise RuntimeError(f"{active_profile.get('label', endpoint_id)} result fetch failed: {res_resp.status_code}")
        result_data = res_resp.json()

    video_url = str(((result_data.get("video", {}) or {}).get("url", "")) or "")
    if not video_url:
        raise RuntimeError(f"No video URL in {endpoint_id} result: " + json.dumps(result_data)[:300])
    await _download_url_to_file(video_url, output_clip_path)
    return output_clip_path


async def _upload_image_to_fal(image_path: str) -> str:
    """Upload a local image to fal.ai CDN and return a public URL for it."""
    if not FAL_AI_KEY:
        raise RuntimeError("FAL_AI_KEY not configured")
    headers = {"Authorization": "Key " + FAL_AI_KEY}
    img_bytes = Path(image_path).read_bytes()
    filename = "nyptid_" + str(int(time.time() * 1000)) + ".png"
    upload_url = "https://fal.ai/api/storage/upload/initiate"

    async with httpx.AsyncClient(timeout=90) as client:
        resp = await client.post(
            upload_url,
            headers={**headers, "Accept": "application/json", "Content-Type": "application/json"},
            json={"file_name": filename, "content_type": "image/png"},
        )
        if resp.status_code == 200:
            upload_info = resp.json()
            presigned = upload_info.get("upload_url") or upload_info.get("presigned_url")
            file_url = upload_info.get("file_url")
            if presigned and file_url:
                put_resp = await client.put(presigned, content=img_bytes, headers={"Content-Type": "image/png"})
                if put_resp.status_code in (200, 201):
                    log.info(f"Image uploaded to fal.ai CDN: {file_url[:80]}")
                    return file_url

        target_path = "uploads/" + filename
        rest_url = "https://api.fal.ai/v1/serverless/files/file/local/" + target_path
        import io
        files = {"file_upload": (filename, io.BytesIO(img_bytes), "image/png")}
        resp2 = await client.post(rest_url, headers=headers, files=files)
        if resp2.status_code in (200, 201):
            cdn_url = "https://api.fal.ai/v1/serverless/files/file/" + target_path
            log.info(f"Image uploaded to fal.ai REST: {cdn_url}")
            return cdn_url

    import base64
    log.warning("fal.ai CDN upload failed, using data URL fallback")
    b64 = base64.b64encode(img_bytes).decode()
    return "data:image/png;base64," + b64


async def animate_image_kling(image_path: str, prompt: str, output_clip_path: str, duration: str = "5", aspect_ratio: str = "9:16", image_cdn_url: str = None) -> str:
    """Use fal.ai Kling 2.1 Standard I2V to animate an image into a video clip.
    If image_cdn_url is provided (from Grok Imagine), skip the upload step.
    Returns the local path to the downloaded MP4 clip.
    """
    if not FAL_AI_KEY:
        raise RuntimeError("FAL_AI_KEY not configured")

    if image_cdn_url:
        image_url = image_cdn_url
        log.info("Kling I2V: using existing CDN URL (from Grok Imagine)")
    else:
        image_url = await _upload_image_to_fal(image_path)

    log.info(f"Kling I2V: submitting job (duration={duration}s, ar={aspect_ratio})")

    headers = {
        "Authorization": "Key " + FAL_AI_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "prompt": prompt,
        "image_url": image_url,
        "duration": duration,
        "aspect_ratio": aspect_ratio,
        "negative_prompt": "blur, distort, low quality, watermark, text overlay, UI elements",
        "cfg_scale": 0.5,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(FAL_SUBMIT_URL, headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError("Kling submit failed (" + str(resp.status_code) + "): " + resp.text[:300])
        submit_data = resp.json()

    request_id = submit_data.get("request_id")
    if not request_id:
        if submit_data.get("video", {}).get("url"):
            video_url = submit_data["video"]["url"]
            await _download_url_to_file(video_url, output_clip_path)
            return output_clip_path
        raise RuntimeError("No request_id from Kling submit: " + json.dumps(submit_data)[:300])

    log.info(f"Kling I2V queued: request_id={request_id}")
    status_url = submit_data.get("status_url", FAL_STATUS_URL + "/" + request_id + "/status")
    result_url = submit_data.get("response_url", FAL_STATUS_URL + "/" + request_id)

    max_wait = 600
    poll_interval = 5
    elapsed = 0
    while elapsed < max_wait:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
        async with httpx.AsyncClient(timeout=30) as client:
            st_resp = await client.get(status_url, headers={"Authorization": "Key " + FAL_AI_KEY})
            if st_resp.status_code == 202:
                st_data = st_resp.json()
                status = st_data.get("status", "IN_PROGRESS")
                if elapsed % 30 == 0:
                    log.info(f"Kling I2V waiting... {elapsed}s elapsed, status={status}")
                continue
            if st_resp.status_code != 200:
                log.warning(f"Kling status poll HTTP {st_resp.status_code}: {st_resp.text[:200]}")
                continue
            st_data = st_resp.json()
            status = st_data.get("status", "")
            if status == "COMPLETED":
                break
            if status in ("FAILED", "CANCELLED"):
                raise RuntimeError("Kling generation failed: " + json.dumps(st_data)[:300])
            if elapsed % 30 == 0:
                log.info(f"Kling I2V waiting... {elapsed}s elapsed, status={status}")
        if poll_interval < 15:
            poll_interval = min(poll_interval + 2, 15)
    else:
        raise TimeoutError("Kling I2V timed out after " + str(max_wait) + "s")

    async with httpx.AsyncClient(timeout=60) as client:
        res_resp = await client.get(result_url, headers={"Authorization": "Key " + FAL_AI_KEY})
        if res_resp.status_code != 200:
            # Fallback: some queue variants expose result at status_url without the trailing /status.
            fallback_result_url = status_url[:-7] if status_url.endswith("/status") else ""
            if fallback_result_url:
                res_resp = await client.get(fallback_result_url, headers={"Authorization": "Key " + FAL_AI_KEY})
            if res_resp.status_code != 200:
                raise RuntimeError("Kling result fetch failed: " + str(res_resp.status_code))
        result_data = res_resp.json()

    video_url = result_data.get("video", {}).get("url")
    if not video_url:
        raise RuntimeError("No video URL in Kling result: " + json.dumps(result_data)[:300])

    log.info(f"Kling I2V complete, downloading video from {video_url[:80]}...")
    await _download_url_to_file(video_url, output_clip_path)
    log.info(f"Kling clip saved: {output_clip_path} ({Path(output_clip_path).stat().st_size / 1024:.0f} KB)")
    return output_clip_path


async def _download_url_to_file(url: str, output_path: str):
    """Download a file from a URL to a local path using streaming writes."""
    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        async with client.stream("GET", url) as resp:
            if resp.status_code != 200:
                raise RuntimeError("Download failed (" + str(resp.status_code) + ") from " + url[:100])
            with open(output_path, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)


async def animate_image_grok_video(image_path: str, prompt: str, output_clip_path: str, duration_sec: float = 5, aspect_ratio: str = "9:16", image_cdn_url: str = None) -> str:
    """Animate an image via xAI Grok Imagine Video and download resulting MP4."""
    if not XAI_API_KEY:
        raise RuntimeError("XAI_API_KEY not configured")
    headers = {"Authorization": f"Bearer {XAI_API_KEY}", "Content-Type": "application/json"}
    duration = max(1, min(int(round(float(duration_sec))), 15))
    image_url = image_cdn_url or _file_to_data_image_url(image_path)
    if not image_url:
        raise RuntimeError("No source image URL for Grok video")

    payload = {
        "model": XAI_VIDEO_MODEL,
        "prompt": prompt,
        "duration": duration,
        "aspect_ratio": aspect_ratio,
        "resolution": "720p",
        "image": {"url": image_url},
    }
    async with httpx.AsyncClient(timeout=60) as client:
        submit = await client.post("https://api.x.ai/v1/videos/generations", headers=headers, json=payload)
        if submit.status_code not in (200, 201):
            raise RuntimeError(f"Grok video submit failed ({submit.status_code}): {submit.text[:300]}")
        submit_data = submit.json()
    request_id = submit_data.get("request_id")
    if not request_id:
        raise RuntimeError("Grok video submit returned no request_id")

    poll_url = f"https://api.x.ai/v1/videos/{request_id}"
    max_wait = 900
    elapsed = 0
    while elapsed < max_wait:
        await asyncio.sleep(4)
        elapsed += 4
        async with httpx.AsyncClient(timeout=30) as client:
            status_resp = await client.get(poll_url, headers={"Authorization": f"Bearer {XAI_API_KEY}"})
            if status_resp.status_code != 200:
                continue
            status_data = status_resp.json()
        status = str(status_data.get("status", "")).lower()
        if status == "done":
            video_url = status_data.get("video", {}).get("url")
            if not video_url:
                raise RuntimeError("Grok video done status missing video URL")
            await _download_url_to_file(video_url, output_clip_path)
            return output_clip_path
        if status == "expired":
            raise RuntimeError("Grok video request expired")
    raise TimeoutError("Grok video timed out")


def _aspect_ratio_to_runway_ratio(aspect_ratio: str) -> str:
    if aspect_ratio == "9:16":
        return "720:1280"
    if aspect_ratio == "16:9":
        return "1280:720"
    allowed = {"1280:720", "720:1280", "1104:832", "960:960", "832:1104", "1584:672"}
    return aspect_ratio if aspect_ratio in allowed else "720:1280"


async def animate_image_runway_video(image_path: str, prompt: str, output_clip_path: str, duration_sec: float = 5, aspect_ratio: str = "9:16", image_cdn_url: str = None) -> str:
    """Animate an image via Runway image-to-video and download resulting MP4."""
    if not RUNWAY_API_KEY:
        raise RuntimeError("RUNWAY_API_KEY not configured")
    headers = {
        "Authorization": f"Bearer {RUNWAY_API_KEY}",
        "Content-Type": "application/json",
        "X-Runway-Version": RUNWAY_API_VERSION,
    }
    duration = max(2, min(int(round(float(duration_sec))), 10))
    image_url = image_cdn_url or _file_to_data_image_url(image_path, max_bytes=3_700_000)
    if not image_url:
        raise RuntimeError("No source image URL for Runway video (image too large for inline data URI; provide CDN URL)")

    payload = {
        "model": RUNWAY_VIDEO_MODEL,
        "promptText": prompt,
        "promptImage": image_url,
        "ratio": _aspect_ratio_to_runway_ratio(aspect_ratio),
        "duration": duration,
    }
    async with httpx.AsyncClient(timeout=60) as client:
        submit = await client.post("https://api.dev.runwayml.com/v1/image_to_video", headers=headers, json=payload)
        if submit.status_code not in (200, 201):
            raise RuntimeError(f"Runway submit failed ({submit.status_code}): {submit.text[:300]}")
        submit_data = submit.json()
    task_id = submit_data.get("id")
    if not task_id:
        raise RuntimeError("Runway submit returned no task id")

    poll_url = f"https://api.dev.runwayml.com/v1/tasks/{task_id}"
    max_wait = 900
    elapsed = 0
    while elapsed < max_wait:
        await asyncio.sleep(5)
        elapsed += 5
        async with httpx.AsyncClient(timeout=30) as client:
            status_resp = await client.get(poll_url, headers=headers)
            if status_resp.status_code != 200:
                continue
            status_data = status_resp.json()
        status = str(status_data.get("status", "")).upper()
        if status == "SUCCEEDED":
            output = status_data.get("output")
            video_url = None
            if isinstance(output, list) and output:
                first = output[0]
                if isinstance(first, str):
                    video_url = first
                elif isinstance(first, dict):
                    video_url = first.get("url") or first.get("uri")
            elif isinstance(output, dict):
                video_url = output.get("url") or output.get("uri") or output.get("video")
            if not video_url:
                raise RuntimeError("Runway done status missing output URL")
            await _download_url_to_file(video_url, output_clip_path)
            return output_clip_path
        if status in ("FAILED", "CANCELLED"):
            raise RuntimeError("Runway task failed: " + json.dumps(status_data)[:300])
    raise TimeoutError("Runway video timed out")


async def animate_scene(
    image_path: str,
    prompt: str,
    output_dir_path: str,
    scene_idx: int,
    job_ts: str,
    duration_sec: float = 5,
    num_frames: int = 81,
    image_cdn_url: str = None,
    prefer_wan: bool = False,
    video_model_id: str = "",
) -> dict:
    """Animate a scene image, preferring local Wan 2.2 for skeleton flows when requested."""
    provider_errors = []
    try:
        requested_duration = float(duration_sec)
    except Exception:
        requested_duration = 5.0
    # FalAI Kling I2V accepts only 5s or 10s durations.
    kling_duration = 10 if requested_duration >= 7.5 else 5

    explicit_video_model_requested = bool(str(video_model_id or "").strip())
    normalized_video_model_id = _normalize_creative_video_model_id(video_model_id)
    if explicit_video_model_requested:
        profile = _creative_video_model_profile(normalized_video_model_id)
        endpoint_id = str(profile.get("fal_endpoint_id", "") or "").strip()
        if endpoint_id:
            explicit_clip_path = str(Path(output_dir_path) / (normalized_video_model_id + "_scene_" + str(scene_idx) + "_" + job_ts + ".mp4"))
            try:
                if normalized_video_model_id == "kling21_standard":
                    await animate_image_kling(
                        image_path,
                        prompt,
                        explicit_clip_path,
                        duration=str(kling_duration),
                        aspect_ratio="9:16",
                        image_cdn_url=image_cdn_url,
                    )
                else:
                    await animate_image_fal_queue_model(
                        endpoint_id,
                        image_path,
                        prompt,
                        explicit_clip_path,
                        duration_sec=requested_duration,
                        aspect_ratio="9:16",
                        image_cdn_url=image_cdn_url,
                        profile=profile,
                    )
                return {"type": "fal_clip", "path": explicit_clip_path, "provider_label": str(profile.get("label", normalized_video_model_id) or normalized_video_model_id)}
            except Exception as e:
                provider_errors.append(normalized_video_model_id + ": " + str(e))
                raise RuntimeError("Selected video model failed: " + str(e)) from e

    if prefer_wan:
        try:
            wan_ready = await check_wan22_available()
        except Exception:
            wan_ready = False
        if wan_ready:
            wan_clip_path = str(Path(output_dir_path) / ("wan_scene_" + str(scene_idx) + "_" + job_ts + ".mp4"))
            try:
                # 5s @ ~16fps is ~80 frames, keep slightly above for smoothness.
                target_frames = max(49, int(round(max(3.0, requested_duration) * 16)))
                await animate_image_wan22(
                    image_path=image_path,
                    prompt=prompt,
                    output_clip_path=wan_clip_path,
                    num_frames=target_frames,
                )
                return {"type": "wan_clip", "path": wan_clip_path}
            except Exception as e:
                provider_errors.append("wan22: " + str(e))
                log.warning(f"Wan 2.2 scene animation failed, trying fallback providers: {e}")

    if RUNWAY_API_KEY:
        runway_clip_path = str(Path(output_dir_path) / ("runway_scene_" + str(scene_idx) + "_" + job_ts + ".mp4"))
        try:
            await animate_image_runway_video(
                image_path=image_path,
                prompt=prompt,
                output_clip_path=runway_clip_path,
                duration_sec=requested_duration,
                image_cdn_url=image_cdn_url,
            )
            return {"type": "runway_clip", "path": runway_clip_path}
        except Exception as e:
            provider_errors.append("runway: " + str(e))
            log.warning(f"Runway scene animation failed, trying FalAI Kling fallback: {e}")

    if FAL_AI_KEY:
        kling_clip_path = str(Path(output_dir_path) / ("kling_scene_" + str(scene_idx) + "_" + job_ts + ".mp4"))
        try:
            await animate_image_kling(
                image_path,
                prompt,
                kling_clip_path,
                duration=str(kling_duration),
                aspect_ratio="9:16",
                image_cdn_url=image_cdn_url,
            )
            return {"type": "kling_clip", "path": kling_clip_path}
        except Exception as e:
            provider_errors.append("fal_kling: " + str(e))
            log.warning(f"FalAI Kling scene animation failed: {e}")

    if not provider_errors:
        raise RuntimeError("No video engine configured (set RUNWAY_API_KEY or FAL_AI_KEY)")
    raise RuntimeError("All video providers failed: " + " | ".join(provider_errors))


async def _upload_image_to_comfyui(image_path: str) -> str:
    """Upload an image to ComfyUI's input directory via HTTP API."""
    src = Path(image_path)
    suffix = src.suffix.lower() or ".png"
    mime = "image/png"
    if suffix in {".jpg", ".jpeg"}:
        mime = "image/jpeg"
        suffix = ".jpg"
    elif suffix == ".webp":
        mime = "image/webp"
    filename = "nyptid_scene_" + str(int(time.time() * 1000)) + suffix
    img_bytes = src.read_bytes()
    global _active_comfyui_url
    last_error = None
    for base_url in _comfyui_candidate_urls():
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                resp = await client.post(
                    f"{base_url}/upload/image",
                    files={"image": (filename, img_bytes, mime)},
                    data={"overwrite": "true"},
                )
                if resp.status_code != 200:
                    raise RuntimeError(f"ComfyUI image upload failed ({resp.status_code}): {resp.text[:200]}")
                result = resp.json()
                uploaded_name = result.get("name", filename)
                _active_comfyui_url = base_url
                log.info(f"Image uploaded to ComfyUI via HTTP: {uploaded_name} ({base_url})")
                return uploaded_name
        except Exception as e:
            last_error = e
            continue
    raise RuntimeError(f"ComfyUI image upload failed on all endpoints: {last_error}")


async def animate_image_wan22(image_path: str, prompt: str, output_clip_path: str, num_frames: int = 81) -> str:
    """Animate an image via Wan 2.2 I2V on ComfyUI. Returns path to downloaded MP4."""
    uploaded_name = await _upload_image_to_comfyui(image_path)

    workflow = {
        "1": {
            "class_type": "UNETLoader",
            "inputs": {
                "unet_name": WAN22_I2V_HIGH,
                "weight_dtype": "fp8_e4m3fn",
            },
        },
        "3": {
            "class_type": "CLIPLoader",
            "inputs": {
                "clip_name": "umt5_xxl_fp8_e4m3fn_scaled.safetensors",
                "type": "wan",
            },
        },
        "4": {
            "class_type": "CLIPTextEncode",
            "inputs": {
                "text": prompt,
                "clip": ["3", 0],
            },
        },
        "5": {
            "class_type": "VAELoader",
            "inputs": {"vae_name": "wan2.2_vae.safetensors"},
        },
        "6": {
            "class_type": "CLIPVisionLoader",
            "inputs": {"clip_name": "clip_vision_h.safetensors"},
        },
        "6b": {
            "class_type": "CLIPVisionEncode",
            "inputs": {
                "clip_vision": ["6", 0],
                "image": ["7", 0],
                "crop": "center",
            },
        },
        "7": {
            "class_type": "LoadImage",
            "inputs": {"image": uploaded_name},
        },
        "8": {
            "class_type": "WanImageToVideo",
            "inputs": {
                "positive": ["4", 0],
                "negative": ["12", 0],
                "vae": ["5", 0],
                "width": 480,
                "height": 832,
                "length": num_frames,
                "batch_size": 1,
                "clip_vision_output": ["6b", 0],
                "start_image": ["7", 0],
            },
        },
        "11": {
            "class_type": "KSampler",
            "inputs": {
                "seed": random.randint(0, 2**32),
                "steps": 30,
                "cfg": 3.0,
                "sampler_name": "uni_pc_bh2",
                "scheduler": "simple",
                "denoise": 1.0,
                "model": ["1", 0],
                "positive": ["8", 0],
                "negative": ["8", 1],
                "latent_image": ["8", 2],
            },
        },
        "12": {
            "class_type": "CLIPTextEncode",
            "inputs": {
                "text": "",
                "clip": ["3", 0],
            },
        },
        "13": {
            "class_type": "VAEDecode",
            "inputs": {
                "samples": ["11", 0],
                "vae": ["5", 0],
            },
        },
        "15": {
            "class_type": "CreateVideo",
            "inputs": {
                "images": ["13", 0],
                "fps": 16.0,
            },
        },
        "16": {
            "class_type": "SaveVideo",
            "inputs": {
                "video": ["15", 0],
                "filename_prefix": "nyptid_wan",
                "format": "mp4",
                "codec": "h264",
            },
        },
    }

    result = await _run_comfyui_workflow(workflow, "16", "videos")

    if not result.get("videos"):
        result = await _run_comfyui_workflow(workflow, "16", "gifs")
        if not result.get("gifs"):
            raise RuntimeError("Wan 2.2 produced no video output")
        vid_info = result["gifs"][0]
    else:
        vid_info = result["videos"][0]

    await _download_comfyui_file(vid_info, output_clip_path)
    file_size = Path(output_clip_path).stat().st_size
    log.info(f"Wan 2.2 video saved: {output_clip_path} ({file_size / 1024:.0f} KB)")
    return output_clip_path


# â”€â”€â”€ FFmpeg Video Compositor â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def frames_to_clip(frame_paths: list, duration: float, output_clip: str, out_w: int, out_h: int, text_overlay: str = "", resolution: str = "720p") -> str:
    """Convert SVD frames into a video clip, stretched/looped to fill the scene duration."""
    frame_dir = Path(frame_paths[0]).parent
    num_frames = len(frame_paths)
    native_fps = 8
    native_duration = num_frames / native_fps

    drawtext = ""
    if text_overlay:
        safe_text = _ffmpeg_safe_text(text_overlay).upper()
        font_size = 96 if resolution == "1080p" else 72
        border_w = 6 if resolution == "1080p" else 4
        drawtext = (
            ",drawtext=text='" + safe_text + "'"
            + ":fontsize=" + str(font_size) + ":fontcolor=white:borderw=" + str(border_w) + ":bordercolor=black"
            + ":x=(w-text_w)/2:y=h*3/4"
            + ":fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        )

    speed_factor = native_duration / duration if duration > 0 else 1.0
    speed_factor = max(0.25, min(speed_factor, 4.0))

    first_frame = Path(frame_paths[0]).name
    prefix = first_frame.rsplit("_", 1)[0]
    input_pattern = str(frame_dir / f"{prefix}_%04d.png")
    cmd = [
        "ffmpeg", "-y",
        "-framerate", str(native_fps),
        "-i", input_pattern,
        "-t", str(duration),
        "-vf", (
            f"setpts={1.0/speed_factor}*PTS,"
            f"scale={out_w}:{out_h}:force_original_aspect_ratio=decrease,"
            f"pad={out_w}:{out_h}:(ow-iw)/2:(oh-ih)/2:black,"
            f"format=yuv420p"
            f"{drawtext}"
        ),
        "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
        "-r", "30",
        str(output_clip),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        log.warning(f"SVD clip ffmpeg error: {stderr.decode()[:500]}")
        raise RuntimeError("Failed to create clip from SVD frames")
    return output_clip


def _ffmpeg_safe_text(text: str) -> str:
    """Escape text for FFmpeg drawtext filter."""
    import re
    t = re.sub(r"[^\w\s.,!?\-+=#&]", "", text)
    t = t.replace(":", "\\:").replace("'", "").replace("%", "")
    return t


async def static_image_to_clip(img_path: str, duration: float, output_clip: str, out_w: int, out_h: int, text_overlay: str = "", resolution: str = "720p") -> str:
    """Fallback: create a video clip from a static image with slow zoom."""
    base_vf = "scale=" + str(out_w) + ":" + str(out_h) + ":force_original_aspect_ratio=decrease,pad=" + str(out_w) + ":" + str(out_h) + ":(ow-iw)/2:(oh-ih)/2:black,format=yuv420p"

    drawtext_vf = base_vf
    if text_overlay:
        safe_text = _ffmpeg_safe_text(text_overlay).upper()
        font_size = 96 if resolution == "1080p" else 72
        border_w = 6 if resolution == "1080p" else 4
        drawtext_vf = (
            base_vf
            + ",drawtext=text='" + safe_text + "'"
            + ":fontsize=" + str(font_size) + ":fontcolor=white:borderw=" + str(border_w) + ":bordercolor=black"
            + ":x=(w-text_w)/2:y=h*3/4"
            + ":fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        )

    cmd = [
        "ffmpeg", "-y",
        "-loop", "1", "-i", str(img_path),
        "-t", str(duration),
        "-vf", drawtext_vf,
        "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
        "-r", "30",
        str(output_clip),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()

    if proc.returncode != 0 and text_overlay:
        log.warning(f"Drawtext failed, retrying without text overlay: {stderr.decode()[-200:]}")
        cmd_plain = [
            "ffmpeg", "-y",
            "-loop", "1", "-i", str(img_path),
            "-t", str(duration),
            "-vf", base_vf,
            "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
            "-r", "30",
            str(output_clip),
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd_plain, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()

    if proc.returncode != 0:
        err = stderr.decode()[-300:]
        log.error(f"FFmpeg static clip error: {err}")
        raise RuntimeError("FFmpeg failed on static image clip: " + err[-100:])
    return output_clip


async def kling_clip_to_scene(kling_clip: str, duration: float, output_clip: str, out_w: int, out_h: int, text_overlay: str = "", resolution: str = "720p") -> str:
    """Re-encode a Kling MP4 clip to exact output dimensions, trim/loop to duration, add text overlay."""
    drawtext = ""
    if text_overlay:
        safe_text = _ffmpeg_safe_text(text_overlay).upper()
        font_size = 96 if resolution == "1080p" else 72
        border_w = 6 if resolution == "1080p" else 4
        drawtext = (
            ",drawtext=text='" + safe_text + "'"
            + ":fontsize=" + str(font_size) + ":fontcolor=white:borderw=" + str(border_w) + ":bordercolor=black"
            + ":x=(w-text_w)/2:y=h*3/4"
            + ":fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        )
    vf = (
        "scale=" + str(out_w) + ":" + str(out_h) + ":force_original_aspect_ratio=decrease,"
        + "pad=" + str(out_w) + ":" + str(out_h) + ":(ow-iw)/2:(oh-ih)/2:black,"
        + "format=yuv420p"
        + drawtext
    )
    cmd = [
        "ffmpeg", "-y",
        "-i", str(kling_clip),
        "-t", str(duration),
        "-vf", vf,
        "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
        "-r", "30",
        str(output_clip),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0 and text_overlay:
        log.warning(f"Kling clip drawtext failed, retrying without: {stderr.decode()[-200:]}")
        vf_plain = (
            "scale=" + str(out_w) + ":" + str(out_h) + ":force_original_aspect_ratio=decrease,"
            + "pad=" + str(out_w) + ":" + str(out_h) + ":(ow-iw)/2:(oh-ih)/2:black,"
            + "format=yuv420p"
        )
        cmd[cmd.index("-vf") + 1] = vf_plain
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()
    if proc.returncode != 0:
        err = stderr.decode()[-300:]
        log.error(f"FFmpeg Kling clip error: {err}")
        raise RuntimeError("FFmpeg failed on Kling clip: " + err[-100:])
    return output_clip


def _estimate_short_audio_duration_seconds(scenes: list, ending_buffer_sec: float = 0.8) -> float:
    total = 0.0
    for raw_scene in scenes or []:
        try:
            duration = float((raw_scene or {}).get("duration_sec", 5.0) or 5.0)
        except Exception:
            duration = 5.0
        total += max(0.5, duration)
    total += max(0.0, float(ending_buffer_sec or 0.0))
    return max(0.8, round(total, 2))


async def _generate_silent_audio_track(duration_sec: float, output_path: str) -> str:
    duration = max(0.5, float(duration_sec or 0.5))
    cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "lavfi",
        "-i",
        "anullsrc=r=44100:cl=mono",
        "-t",
        str(duration),
        "-c:a",
        "libmp3lame",
        "-ar",
        "44100",
        "-ac",
        "1",
        output_path,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0 or not _audio_track_exists(output_path):
        err = (stderr.decode(errors="ignore") or "")[-200:]
        raise RuntimeError(f"Silent audio track generation failed: {err}")
    return output_path


async def _prepare_short_audio_assets(
    scenes: list,
    *,
    audio_path: str,
    subtitle_path: str = "",
    template: str,
    language: str = "en",
    override_voice_id: str = "",
    override_speed: float | None = None,
    subtitles_enabled: bool = True,
    resolution: str = "720p",
    job_id: str = "",
    full_narration_override: str = "",
) -> dict:
    full_narration = str(full_narration_override or "").strip()
    if not full_narration:
        full_narration = " ".join(
            str((scene or {}).get("narration", "") or "").strip()
            for scene in scenes or []
        ).strip()
    word_timings: list[dict] = []
    generated_subtitle_path = ""
    audio_warning = ""
    audio_mode = "voiceover"

    if full_narration:
        try:
            vo_result = await generate_voiceover(
                full_narration,
                audio_path,
                template=template,
                language=language,
                override_voice_id=override_voice_id,
                override_speed=override_speed,
            )
            audio_path = str(vo_result.get("audio_path", audio_path) or audio_path)
            word_timings = list(vo_result.get("word_timings", []) or [])
            audio_mode = str(vo_result.get("provider", "") or "voiceover")
        except Exception as exc:
            audio_mode = "silent_fallback"
            audio_warning = _clip_text(
                f"Voice generation unavailable; rendered with caption/SFX fallback instead ({type(exc).__name__}: {exc})",
                220,
            )
            log.warning("[%s] Voice generation failed; using silent track fallback: %s", job_id or "short", exc)
            await _generate_silent_audio_track(
                _estimate_short_audio_duration_seconds(scenes),
                audio_path,
            )
    else:
        audio_mode = "silent_no_narration"
        await _generate_silent_audio_track(
            _estimate_short_audio_duration_seconds(scenes),
            audio_path,
        )

    if subtitles_enabled and subtitle_path:
        try:
            if word_timings:
                generate_ass_subtitles(
                    word_timings,
                    subtitle_path,
                    resolution=resolution,
                    template=template,
                )
                generated_subtitle_path = subtitle_path if Path(subtitle_path).exists() else ""
            else:
                generated_subtitle_path = generate_ass_scene_subtitles(
                    scenes,
                    subtitle_path,
                    resolution=resolution,
                    template=template,
                ) or ""
        except Exception as subtitle_exc:
            log.warning("[%s] Subtitle generation failed (non-fatal): %s", job_id or "short", subtitle_exc)
            generated_subtitle_path = ""

    return {
        "audio_path": audio_path,
        "word_timings": word_timings,
        "subtitle_path": generated_subtitle_path,
        "audio_mode": audio_mode,
        "audio_warning": audio_warning,
        "full_narration": full_narration,
    }


async def _merge_sfx_track(sfx_paths: list[str], scenes: list, output_path: str) -> str:
    """Concatenate per-scene SFX clips into one continuous audio track.
    Pads missing scenes with silence to keep timing aligned."""
    job_ts = str(int(time.time() * 1000))
    padded_clips = []

    for i, scene in enumerate(scenes):
        dur = scene.get("duration_sec", 5)
        if i == len(scenes) - 1:
            dur += 1.0
        sfx = sfx_paths[i] if i < len(sfx_paths) else ""

        padded_path = str(TEMP_DIR / f"sfx_pad_{i}_{job_ts}.mp3")
        if sfx and Path(sfx).exists() and Path(sfx).stat().st_size > 0:
            cmd = [
                "ffmpeg", "-y", "-i", sfx,
                "-af", f"apad=whole_dur={dur},afade=t=in:st=0:d=0.15,afade=t=out:st={max(dur - 0.3, 0)}:d=0.3",
                "-t", str(dur), "-ar", "44100", "-ac", "1",
                padded_path,
            ]
        else:
            cmd = [
                "ffmpeg", "-y", "-f", "lavfi", "-i", f"anullsrc=r=44100:cl=mono",
                "-t", str(dur), padded_path,
            ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await proc.communicate()
        padded_clips.append(padded_path)

    concat_file = TEMP_DIR / f"sfx_concat_{job_ts}.txt"
    with open(concat_file, "w") as f:
        for p in padded_clips:
            f.write(f"file '{Path(p).resolve()}'\n")

    cmd = [
        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
        "-i", str(concat_file), "-c:a", "libmp3lame", "-ar", "44100",
        output_path,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    await proc.communicate()

    for p in padded_clips:
        Path(p).unlink(missing_ok=True)
    concat_file.unlink(missing_ok=True)

    if Path(output_path).exists() and Path(output_path).stat().st_size > 0:
        log.info(f"SFX track merged: {output_path}")
        return output_path
    return ""




async def _generate_spooky_bgm_track(total_duration_sec: float, output_path: str, whisper_mode: str = "subtle") -> str:
    """Create a spooky background music bed for horror long-form videos."""
    duration = max(6.0, float(total_duration_sec or 0.0))
    fade_out_start = max(0.0, duration - 2.0)
    seed_clip = str(TEMP_DIR / f"bgm_seed_{int(time.time() * 1000)}.mp3")
    mood_hint = "subtle" if whisper_mode == "off" else ("cinematic tension" if whisper_mode == "cinematic" else "dark atmospheric")
    bgm_prompt = (
        "Instrumental cinematic horror ambience music bed, haunting low drones, distant eerie textures, "
        "slow tension build, no vocals, no speech, no abrupt stingers, seamless loop quality, "
        f"{mood_hint} tone under narration"
    )

    try:
        if ELEVENLABS_API_KEY:
            async with httpx.AsyncClient(timeout=90) as client:
                resp = await client.post(
                    "https://api.elevenlabs.io/v1/sound-generation",
                    headers={
                        "xi-api-key": ELEVENLABS_API_KEY,
                        "Content-Type": "application/json",
                    },
                    json={
                        "text": bgm_prompt,
                        "duration_seconds": 22.0,
                        "prompt_influence": 0.62,
                    },
                )
            if resp.status_code in (200, 201):
                with open(seed_clip, "wb") as f:
                    f.write(resp.content)
                if _audio_track_exists(seed_clip):
                    cmd = [
                        "ffmpeg",
                        "-y",
                        "-stream_loop",
                        "-1",
                        "-i",
                        seed_clip,
                        "-t",
                        str(duration),
                        "-af",
                        f"afade=t=in:st=0:d=0.8,afade=t=out:st={fade_out_start}:d=2.0,apad=pad_dur=0.8",
                        "-ar",
                        "44100",
                        "-ac",
                        "1",
                        output_path,
                    ]
                    proc = await asyncio.create_subprocess_exec(
                        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                    )
                    await proc.communicate()
                    if proc.returncode == 0 and _audio_track_exists(output_path):
                        return output_path
        # Fallback: synthesize a low-key eerie bed locally.
        cmd = [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            f"sine=frequency=54:sample_rate=44100:duration={duration}",
            "-f",
            "lavfi",
            "-i",
            f"sine=frequency=81:sample_rate=44100:duration={duration}",
            "-f",
            "lavfi",
            "-i",
            f"anoisesrc=color=pink:amplitude=0.025:sample_rate=44100:duration={duration}",
            "-filter_complex",
            (
                "[0:a]volume=0.16[a0];"
                "[1:a]volume=0.09,lowpass=f=900[a1];"
                "[2:a]highpass=f=140,lowpass=f=1500,volume=0.10[a2];"
                f"[a0][a1][a2]amix=inputs=3:duration=longest:normalize=0,"
                f"afade=t=in:st=0:d=1.0,afade=t=out:st={fade_out_start}:d=2.0,apad=pad_dur=0.8[aout]"
            ),
            "-map",
            "[aout]",
            "-ar",
            "44100",
            "-ac",
            "1",
            output_path,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await proc.communicate()
        if proc.returncode == 0 and _audio_track_exists(output_path):
            return output_path
    except Exception as e:
        log.warning(f"Horror BGM generation failed (non-fatal): {e}")
    finally:
        Path(seed_clip).unlink(missing_ok=True)

    return ""


async def _composite_video_on_runpod(
    scene_clips: list[Path],
    audio_path: str,
    output_path: str,
    subtitle_path: str = None,
    sfx_track: str = "",
    transition_style: str = "smooth",
    clip_durations: list[float] | None = None,
) -> str:
    """Run final concat + merge on RunPod to reduce Render RAM pressure."""
    run_id = f"cmp_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
    remote_dir = f"{RUNPOD_COMPOSITOR_BASE_DIR.rstrip('/')}/{run_id}"
    remote_merged = f"{remote_dir}/merged.mp4"
    remote_output = f"{remote_dir}/final.mp4"

    ok, err = await asyncio.to_thread(
        _run_remote_cmd_blocking,
        RUNPOD_COMPOSITOR_HOST,
        RUNPOD_COMPOSITOR_SSH_PORT,
        f"mkdir -p '{remote_dir}'",
    )
    if not ok:
        raise RuntimeError(f"RunPod mkdir failed: {err}")

    local_concat = TEMP_DIR / f"remote_concat_{run_id}.txt"
    local_concat.write_text("", encoding="utf-8")
    uploaded_remote_files = []
    try:
        for i, clip in enumerate(scene_clips):
            remote_clip = f"{remote_dir}/scene_{i}.mp4"
            up_ok, up_err = await asyncio.to_thread(
                _upload_file_to_runpod_blocking,
                RUNPOD_COMPOSITOR_HOST,
                RUNPOD_COMPOSITOR_SSH_PORT,
                str(clip),
                remote_clip,
            )
            if not up_ok:
                raise RuntimeError(f"RunPod clip upload failed ({clip.name}): {up_err}")
            uploaded_remote_files.append(remote_clip)

        remote_audio = f"{remote_dir}/voice.mp3"
        up_ok, up_err = await asyncio.to_thread(
            _upload_file_to_runpod_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            audio_path,
            remote_audio,
        )
        if not up_ok:
            raise RuntimeError(f"RunPod audio upload failed: {up_err}")

        remote_sub = ""
        if subtitle_path and Path(subtitle_path).exists():
            remote_sub = f"{remote_dir}/captions.ass"
            up_ok, up_err = await asyncio.to_thread(
                _upload_file_to_runpod_blocking,
                RUNPOD_COMPOSITOR_HOST,
                RUNPOD_COMPOSITOR_SSH_PORT,
                subtitle_path,
                remote_sub,
            )
            if not up_ok:
                raise RuntimeError(f"RunPod subtitle upload failed: {up_err}")

        remote_sfx = ""
        if sfx_track and Path(sfx_track).exists():
            remote_sfx = f"{remote_dir}/sfx.mp3"
            up_ok, up_err = await asyncio.to_thread(
                _upload_file_to_runpod_blocking,
                RUNPOD_COMPOSITOR_HOST,
                RUNPOD_COMPOSITOR_SSH_PORT,
                sfx_track,
                remote_sfx,
            )
            if not up_ok:
                raise RuntimeError(f"RunPod sfx upload failed: {up_err}")

        concat_lines = "".join([f"file '{p}'\n" for p in uploaded_remote_files])
        local_concat.write_text(concat_lines, encoding="utf-8")
        remote_concat = f"{remote_dir}/concat.txt"
        up_ok, up_err = await asyncio.to_thread(
            _upload_file_to_runpod_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            str(local_concat),
            remote_concat,
        )
        if not up_ok:
            raise RuntimeError(f"RunPod concat upload failed: {up_err}")

        style = _normalize_transition_style(transition_style)
        xfade_type = TRANSITION_STYLE_MAP.get(style, "fade")
        transition_dur = _transition_duration_for_style(style)
        use_xfade = (style != "no_motion" and style != "none" and len(uploaded_remote_files) > 1)
        if use_xfade:
            durations = list(clip_durations or [])
            while len(durations) < len(uploaded_remote_files):
                durations.append(5.0)
            durations = [max(0.4, float(d or 5.0)) for d in durations[:len(uploaded_remote_files)]]
            ff_inputs = " ".join([f"-i '{p}'" for p in uploaded_remote_files])
            parts = []
            cumulative = durations[0]
            for idx in range(1, len(uploaded_remote_files)):
                left = "0:v" if idx == 1 else f"v{idx-1}"
                out = f"v{idx}"
                offset = max(0.0, cumulative - transition_dur * idx)
                parts.append(f"[{left}][{idx}:v]xfade=transition={xfade_type}:duration={transition_dur:.3f}:offset={offset:.3f}[{out}]")
                cumulative += durations[idx]
            final_label = f"v{len(uploaded_remote_files) - 1}"
            concat_cmd = (
                f"ffmpeg -y {ff_inputs} -filter_complex \"{';'.join(parts)}\" "
                f"-map '[{final_label}]' -c:v libx264 -preset fast -pix_fmt yuv420p '{remote_merged}'"
            )
        else:
            concat_cmd = (
                f"ffmpeg -y -f concat -safe 0 -i '{remote_concat}' "
                f"-c:v libx264 -preset fast -pix_fmt yuv420p '{remote_merged}'"
            )
        ok, err = await asyncio.to_thread(
            _run_remote_cmd_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            concat_cmd,
        )
        if not ok:
            raise RuntimeError(f"RunPod concat failed: {err}")

        if remote_sub and remote_sfx:
            merge_cmd = (
                f"ffmpeg -y -i '{remote_merged}' -i '{remote_audio}' -i '{remote_sfx}' "
                f"-vf \"ass={remote_sub}\" "
                f"-filter_complex \"[1:a]volume=1.0[voice];[2:a]volume=0.16[sfx];"
                f"[voice][sfx]amix=inputs=2:duration=first:dropout_transition=2,apad=pad_dur=0.8[aout]\" "
                f"-map 0:v -map \"[aout]\" -c:v libx264 -preset fast -pix_fmt yuv420p "
                f"-c:a aac -b:a 192k -shortest '{remote_output}'"
            )
        elif remote_sub:
            merge_cmd = (
                f"ffmpeg -y -i '{remote_merged}' -i '{remote_audio}' "
                f"-vf \"ass={remote_sub}\" -af apad=pad_dur=0.8 "
                f"-c:v libx264 -preset fast -pix_fmt yuv420p -c:a aac -b:a 192k -shortest '{remote_output}'"
            )
        elif remote_sfx:
            merge_cmd = (
                f"ffmpeg -y -i '{remote_merged}' -i '{remote_audio}' -i '{remote_sfx}' "
                f"-filter_complex \"[1:a]volume=1.0[voice];[2:a]volume=0.16[sfx];"
                f"[voice][sfx]amix=inputs=2:duration=first:dropout_transition=2,apad=pad_dur=0.8[aout]\" "
                f"-map 0:v -map \"[aout]\" -c:v libx264 -preset fast -pix_fmt yuv420p "
                f"-c:a aac -b:a 192k -shortest '{remote_output}'"
            )
        else:
            merge_cmd = (
                f"ffmpeg -y -i '{remote_merged}' -i '{remote_audio}' "
                f"-af apad=pad_dur=0.8 -c:v libx264 -preset fast -pix_fmt yuv420p "
                f"-c:a aac -b:a 192k -shortest '{remote_output}'"
            )

        ok, err = await asyncio.to_thread(
            _run_remote_cmd_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            merge_cmd,
        )
        if not ok:
            raise RuntimeError(f"RunPod final merge failed: {err}")

        dl_ok, dl_err = await asyncio.to_thread(
            _download_file_from_runpod_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            remote_output,
            output_path,
        )
        if not dl_ok:
            raise RuntimeError(f"RunPod output download failed: {dl_err}")

        if not Path(output_path).exists() or Path(output_path).stat().st_size == 0:
            raise RuntimeError("RunPod output file missing after download")
        return output_path
    finally:
        local_concat.unlink(missing_ok=True)
        await asyncio.to_thread(
            _run_remote_cmd_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            f"rm -rf '{remote_dir}'",
        )


async def composite_video(
    scenes: list,
    scene_assets: list,
    audio_path: str,
    output_path: str,
    resolution: str = "720p",
    use_svd: bool = False,
    subtitle_path: str = None,
    sfx_paths: list[str] = None,
    bgm_track: str = "",
    transition_style: str = "smooth",
    micro_escalation_mode: bool = False,
    pattern_interrupt_interval_sec: int = 12,
    voice_gain: float = 1.0,
    ambience_gain: float = 0.18,
    sfx_gain: float = 1.0,
    bgm_gain: float = 0.55,
    job_id: str | None = None,
    minimum_scene_duration: float = 3.5,
) -> str:
    """Composite scene clips into final MP4 with optional burned-in captions and SFX.
    scene_assets: list of dicts with keys: image, frames, kling_clip
    subtitle_path: optional ASS subtitle file for word-synced captions
    sfx_paths: optional per-scene SFX audio files to mix under voiceover
    """
    config = RESOLUTION_CONFIGS[resolution]
    out_w = config["output_width"]
    out_h = config["output_height"]

    job_ts = str(int(time.time() * 1000))
    concat_file = TEMP_DIR / ("concat_" + job_ts + ".txt")
    scene_clips = []
    clip_durations = []

    num_scenes = len(scenes)
    planned_scene_durations = _rebalance_scene_durations_for_audio(
        scenes,
        audio_path,
        minimum_scene_duration=minimum_scene_duration,
    )
    if planned_scene_durations:
        log.info(
            "Composite durations aligned to audio: scenes=%s total=%.2fs audio=%.2fs min_scene=%.2fs",
            len(planned_scene_durations),
            sum(planned_scene_durations),
            _probe_audio_duration_seconds(audio_path),
            float(minimum_scene_duration or 3.5),
        )
    for i, (scene, asset) in enumerate(zip(scenes, scene_assets)):
        chapter_index = int((scene or {}).get("_chapter_index", 0) or 0)
        scene_num = int((scene or {}).get("scene_num", i + 1) or (i + 1))
        if job_id:
            _job_update_scene_pointer(
                job_id,
                i + 1,
                num_scenes,
                chapter_index=chapter_index + 1,
                scene_num=scene_num,
            )
            preview_url = str((scene or {}).get("image_url", "") or "")
            if preview_url:
                _job_update_preview(
                    job_id,
                    preview_url=preview_url,
                    preview_type="image",
                    preview_label=f"Chapter {chapter_index + 1} Scene {scene_num} render preview",
                )
            _job_set_stage(job_id, "compositing", 84 + int(((i + 1) / max(num_scenes, 1)) * 4))
        duration = planned_scene_durations[i] if i < len(planned_scene_durations) else max(
            float((scene or {}).get("duration_sec", minimum_scene_duration or 3.5) or (minimum_scene_duration or 3.5)),
            float(minimum_scene_duration or 3.5),
        )
        text_overlay = "" if subtitle_path else scene.get("text_overlay", "")
        clip_name = "scene_" + str(i) + "_" + job_ts + ".mp4"
        clip_path = str(TEMP_DIR / clip_name)

        if asset.get("kling_clip"):
            try:
                await kling_clip_to_scene(
                    asset["kling_clip"], duration, clip_path,
                    out_w, out_h, text_overlay, resolution,
                )
                scene_clips.append(Path(clip_path))
                clip_durations.append(float(duration))
                continue
            except Exception as e:
                log.warning(f"Kling clip processing failed for scene {i}: {e}")

        if use_svd and asset.get("frames"):
            try:
                await frames_to_clip(
                    asset["frames"], duration, clip_path,
                    out_w, out_h, text_overlay, resolution,
                )
                scene_clips.append(Path(clip_path))
                clip_durations.append(float(duration))
                continue
            except Exception as e:
                log.warning(f"SVD clip failed for scene {i}, falling back to static: {e}")

        await static_image_to_clip(
            asset["image"], duration, clip_path,
            out_w, out_h, text_overlay, resolution,
        )
        scene_clips.append(Path(clip_path))
        clip_durations.append(float(duration))

    if job_id:
        _job_set_stage(job_id, "compositing", 86)

    existing_clips = [c for c in scene_clips if c.exists() and c.stat().st_size > 0]
    if not existing_clips:
        raise RuntimeError("No scene clips were created -- nothing to composite")
    working_clips = list(existing_clips)
    working_durations = [clip_durations[i] if i < len(clip_durations) else _probe_video_duration_seconds(str(c)) for i, c in enumerate(existing_clips)]
    if micro_escalation_mode:
        try:
            working_clips, working_durations = await _build_micro_escalation_clips(
                existing_clips,
                working_durations,
                job_ts,
                scene_payloads=scenes,
                pattern_interrupt_interval_sec=pattern_interrupt_interval_sec,
            )
            log.info(f"Micro-escalation enabled: {len(existing_clips)} base clips -> {len(working_clips)} beat clips")
        except Exception as e:
            log.warning(f"Micro-escalation build failed, falling back to base clips: {e}")
            working_clips = list(existing_clips)
            working_durations = list(working_durations)
    log.info(f"Compositing {len(working_clips)} scene clips into video")

    sfx_track = ""
    if sfx_paths and any(sfx_paths):
        sfx_track_path = str(TEMP_DIR / ("sfx_full_" + job_ts + ".mp3"))
        sfx_track = await _merge_sfx_track(sfx_paths, scenes, sfx_track_path)
    ambience_track = ""
    if sfx_track or _audio_track_exists(bgm_track):
        ambience_path = str(TEMP_DIR / ("ambience_full_" + job_ts + ".mp3"))
        ambience_track = await _mix_ambience_tracks(
            sfx_track,
            bgm_track,
            ambience_path,
            sfx_gain=sfx_gain,
            bgm_gain=bgm_gain,
        )

    if RUNPOD_COMPOSITOR_ENABLED:
        try:
            await _composite_video_on_runpod(
                working_clips,
                audio_path,
                output_path,
                subtitle_path=subtitle_path,
                sfx_track=ambience_track,
                transition_style=transition_style,
                clip_durations=working_durations,
            )
            for clip in set(scene_clips + working_clips):
                clip.unlink(missing_ok=True)
            if ambience_track:
                Path(ambience_track).unlink(missing_ok=True)
            if sfx_track:
                Path(sfx_track).unlink(missing_ok=True)
            log.info("Final compositing offloaded to RunPod")
            if job_id:
                _job_set_stage(job_id, "compositing", 98)
            return str(output_path)
        except Exception as e:
            if not RUNPOD_COMPOSITOR_FALLBACK_LOCAL:
                raise
            log.warning(f"RunPod compositing failed, falling back local: {e}")

    merged_video = TEMP_DIR / ("merged_" + job_ts + ".mp4")
    style = _normalize_transition_style(transition_style)
    xfade_type = TRANSITION_STYLE_MAP.get(style, "fade")
    transition_dur = _transition_duration_for_style(style)
    use_xfade = (style != "no_motion" and style != "none" and len(working_clips) > 1)

    def _write_concat_manifest(clips: list[Path]) -> None:
        with open(concat_file, "w") as f:
            for clip in clips:
                f.write("file '" + str(clip.resolve()) + "'\n")

    async def _run_ffmpeg_merge(cmd: list[str]) -> tuple[int, str]:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr_data = await proc.communicate()
        return proc.returncode, stderr_data.decode()

    async def _concat_merge(clips: list[Path]) -> tuple[int, str]:
        _write_concat_manifest(clips)
        cmd = [
            "ffmpeg", "-y", "-f", "concat", "-safe", "0",
            "-i", str(concat_file),
            "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
            str(merged_video),
        ]
        return await _run_ffmpeg_merge(cmd)

    merge_code = 0
    merge_stderr = ""
    if use_xfade:
        durations = []
        for i, clip in enumerate(working_clips):
            probed = _probe_video_duration_seconds(str(clip))
            fallback = working_durations[i] if i < len(working_durations) else 5.0
            durations.append(max(0.4, probed if probed > 0.1 else float(fallback)))
        cmd = ["ffmpeg", "-y"]
        for clip in working_clips:
            cmd.extend(["-i", str(clip)])
        parts = []
        cumulative = durations[0]
        for idx in range(1, len(working_clips)):
            left = "0:v" if idx == 1 else f"v{idx-1}"
            out = f"v{idx}"
            offset = max(0.0, cumulative - transition_dur * idx)
            parts.append(f"[{left}][{idx}:v]xfade=transition={xfade_type}:duration={transition_dur:.3f}:offset={offset:.3f}[{out}]")
            cumulative += durations[idx]
        final_label = f"v{len(working_clips)-1}"
        cmd.extend([
            "-filter_complex", ";".join(parts),
            "-map", f"[{final_label}]",
            "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
            str(merged_video),
        ])
        merge_code, merge_stderr = await _run_ffmpeg_merge(cmd)
        if merge_code != 0:
            log.warning(
                "FFmpeg xfade merge failed, retrying slideshow merge with plain base-scene concat: %s",
                merge_stderr[-300:],
            )
            fallback_clips = list(existing_clips)
            merge_code, merge_stderr = await _concat_merge(fallback_clips)
    else:
        merge_code, merge_stderr = await _concat_merge(working_clips)
        if merge_code != 0 and working_clips != existing_clips:
            log.warning(
                "FFmpeg concat on working clips failed, retrying slideshow merge with base-scene concat: %s",
                merge_stderr[-300:],
            )
            merge_code, merge_stderr = await _concat_merge(list(existing_clips))

    if merge_code != 0:
        err_msg = merge_stderr[-500:]
        log.error(f"FFmpeg merge error: {err_msg}")
        raise RuntimeError("FFmpeg failed to merge scene clips: " + err_msg[-200:])

    if not merged_video.exists() or merged_video.stat().st_size == 0:
        raise RuntimeError("FFmpeg concat produced no output file")

    if job_id:
        if Path(output_path).exists() and Path(output_path).stat().st_size > 0:
            _job_update_preview(
                job_id,
                preview_url=f"/api/download/{Path(output_path).name}",
                preview_type="video",
                preview_label="Current long-form render output",
            )
        _job_set_stage(job_id, "compositing", 90)

    has_sfx = ambience_track and Path(ambience_track).exists()

    if subtitle_path and Path(subtitle_path).exists():
        sub_abs = str(Path(subtitle_path).resolve()).replace("\\", "/").replace(":", "\\:")
        if has_sfx:
            cmd = [
                "ffmpeg", "-y",
                "-i", str(merged_video),
                "-i", audio_path,
                "-i", ambience_track,
                "-vf", f"ass={sub_abs}",
                "-filter_complex", f"[1:a]volume={max(0.6, float(voice_gain or 1.0)):.3f}[voice];[2:a]volume={max(0.08, float(ambience_gain or 0.18)):.3f}[sfx];[voice][sfx]amix=inputs=2:duration=first:dropout_transition=2,apad=pad_dur=0.8[aout]",
                "-map", "0:v", "-map", "[aout]",
                "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest",
                str(output_path),
            ]
        else:
            cmd = [
                "ffmpeg", "-y",
                "-i", str(merged_video),
                "-i", audio_path,
                "-vf", f"ass={sub_abs}",
                "-af", "apad=pad_dur=0.8",
                "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest",
                str(output_path),
            ]
        log.info(f"Burning captions from {subtitle_path}" + (" + SFX" if has_sfx else ""))
    else:
        if has_sfx:
            cmd = [
                "ffmpeg", "-y",
                "-i", str(merged_video),
                "-i", audio_path,
                "-i", ambience_track,
                "-filter_complex", f"[1:a]volume={max(0.6, float(voice_gain or 1.0)):.3f}[voice];[2:a]volume={max(0.08, float(ambience_gain or 0.18)):.3f}[sfx];[voice][sfx]amix=inputs=2:duration=first:dropout_transition=2,apad=pad_dur=0.8[aout]",
                "-map", "0:v", "-map", "[aout]",
                "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest",
                str(output_path),
            ]
        else:
            cmd = [
                "ffmpeg", "-y",
                "-i", str(merged_video),
                "-i", audio_path,
                "-af", "apad=pad_dur=0.8",
                "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest",
                str(output_path),
            ]

    if job_id:
        _job_set_stage(job_id, "compositing", 94)

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr_merge = await proc.communicate()
    if proc.returncode != 0:
        if subtitle_path:
            log.warning(f"Subtitle burn-in failed, retrying without: {stderr_merge.decode()[-300:]}")
            if has_sfx:
                cmd_fallback = [
                    "ffmpeg", "-y",
                    "-i", str(merged_video),
                    "-i", audio_path,
                    "-i", ambience_track,
                    "-filter_complex", f"[1:a]volume={max(0.6, float(voice_gain or 1.0)):.3f}[voice];[2:a]volume={max(0.08, float(ambience_gain or 0.18)):.3f}[sfx];[voice][sfx]amix=inputs=2:duration=first:dropout_transition=2,apad=pad_dur=0.8[aout]",
                    "-map", "0:v", "-map", "[aout]",
                    "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
                    "-c:a", "aac", "-b:a", "192k",
                    "-shortest",
                    str(output_path),
                ]
            else:
                cmd_fallback = [
                    "ffmpeg", "-y",
                    "-i", str(merged_video),
                    "-i", audio_path,
                    "-af", "apad=pad_dur=0.8",
                    "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
                    "-c:a", "aac", "-b:a", "192k",
                    "-shortest",
                    str(output_path),
                ]
            proc = await asyncio.create_subprocess_exec(
                *cmd_fallback, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            _, stderr_merge = await proc.communicate()
        if proc.returncode != 0:
            err_msg = stderr_merge.decode()[-500:]
            log.error(f"FFmpeg final merge error: {err_msg}")
            raise RuntimeError("FFmpeg failed to merge video + audio: " + err_msg[-200:])

    if not Path(output_path).exists() or Path(output_path).stat().st_size == 0:
        raise RuntimeError("FFmpeg produced no final output file")

    if job_id:
        _job_update_preview(
            job_id,
            preview_url=f"/api/download/{Path(output_path).name}",
            preview_type="video",
            preview_label="Current long-form render output",
        )
        _job_set_stage(job_id, "compositing", 98)

    for clip in set(scene_clips + working_clips):
        clip.unlink(missing_ok=True)
    concat_file.unlink(missing_ok=True)
    merged_video.unlink(missing_ok=True)
    if ambience_track:
        Path(ambience_track).unlink(missing_ok=True)
    if sfx_track:
        Path(sfx_track).unlink(missing_ok=True)

    log.info(f"Video composited successfully: {Path(output_path).stat().st_size / 1024 / 1024:.1f} MB")
    return str(output_path)


# â”€â”€â”€ Full Generation Pipeline â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


def _normalize_scenes_for_render(scenes: list) -> list:
    normalized = []
    for idx, raw_scene in enumerate(scenes or []):
        scene = dict(raw_scene or {})
        narration = str(scene.get("narration", "") or "").strip()
        visual_description = str(scene.get("visual_description", "") or "").strip()
        negative_prompt = str(scene.get("negative_prompt", "") or "").strip()
        if not visual_description:
            visual_description = narration or f"Scene {idx + 1} visual"
        try:
            duration = float(scene.get("duration_sec", 5))
        except Exception:
            duration = 5.0
        duration = max(3.5, min(duration, 10.0))
        scene["narration"] = narration
        scene["visual_description"] = visual_description
        scene["negative_prompt"] = negative_prompt
        scene["duration_sec"] = round(duration, 2)
        normalized.append(scene)
    return normalized


SKELETON_QUALITY_MODES = {"standard", "cinematic"}
SKELETON_CINEMATIC_PROMPT_ADDON = (
    "CINEMATIC UPGRADE RULES: Open each scene with immediate tension or high-stakes action. "
    "Use dynamic 24-35mm framing, low-angle or over-shoulder composition, strong foreground/background depth, "
    "volumetric rim lighting, and high local contrast. Keep a premium viral short aesthetic with clean silhouette separation. "
    "Avoid static centered idle poses, flat lighting, washed contrast, cartoon style, and text overlays."
)
STORY_MASTER_CONSISTENCY_PROMPT = (
    "MASTER STORY CONSISTENCY RULES (apply to every scene): "
    "Keep one continuous visual universe across all scenes. Keep recurring people, places, era details, "
    "color grade, and camera language continuity scene-to-scene when the script indicates recurrence. "
    "Do not switch art style or turn into cartoon/anime unless explicitly requested. "
    "Preserve realistic skin texture, grounded proportions, and emotionally readable expressions. "
    "When multiple subjects appear, prioritize whichever subject the current script beat is about. "
    "Maintain bright readable mids, clean separation, and premium cinematic detail in every frame."
)
STORY_CINEMATIC_PROMPT_ADDON = (
    "CINEMATIC STORY UPGRADE RULES: Open each scene with visible emotional stakes or conflict. "
    "Use dynamic 24-35mm framing, motivated camera motion, foreground/background depth, and volumetric edge lighting. "
    "Keep every composition readable in vertical format and avoid static idle center-framing. "
    "Avoid washed contrast, muddy shadows, low-detail backgrounds, and stock-photo framing."
)
STORY_EXPLAINER_CONSISTENCY_PROMPT = (
    "MASTER STORY CONSISTENCY RULES (apply to every scene): "
    "Keep one continuous visual universe across all scenes, but do not force a recurring protagonist or human presenter. "
    "For concept-heavy beats, preserve continuity through recurring anatomy, objects, mechanisms, environments, color grade, and camera language. "
    "Prioritize the core concept, process, body system, object, or environment the beat is explaining. "
    "Use readable cinematic layouts that make the mechanism obvious at phone-screen size."
)
STORY_EXPLAINER_CINEMATIC_PROMPT_ADDON = (
    "CINEMATIC STORY UPGRADE RULES: visualize the mechanism directly with premium explainer cinematography. "
    "Use readable cutaway views, macro detail, layered depth, motivated lighting, and vertical-friendly composition. "
    "Prefer anatomy, systems, heat, cells, objects, environments, diagrams, or process visuals when they explain the beat more clearly than a person on screen. "
    "Avoid defaulting to a single human character unless the beat explicitly requires one."
)
STORY_REALISM_REFINEMENT = (
    "Realism lock: photoreal human skin texture, natural pores, physically plausible hands and fingers, "
    "natural eye reflections, grounded facial proportions, realistic fabric micro-detail, and non-plastic materials. "
    "No CGI waxiness, no uncanny face, no extra fingers, no malformed hands."
)
STORY_EXPLAINER_REALISM_REFINEMENT = (
    "Realism lock: premium photoreal explainer visual with medically and physically plausible anatomy, "
    "clear body-system visualization, readable objects and environments, grounded materials, and crisp cinematic lighting. "
    "No stock-photo spokesperson, no random presenter, no irrelevant repeated character, no plastic CGI anatomy, and no abstract blob shapes replacing the core concept."
)
STORY_DOCUMENTARY_PROMPT_PREFIX = (
    "Premium stylized 3D documentary scene, obviously computer-generated and intentionally designed, with cinematic lighting, "
    "clean focal hierarchy, premium production design, readable depth, and no live-action photography."
)
STORY_DOCUMENTARY_CONSISTENCY_PROMPT = (
    "MASTER DOCUMENTARY CONSISTENCY RULES (apply to every scene): keep one premium documentary visual universe across scenes through recurring spaces, "
    "evidence objects, color grade, and camera language. Prioritize the consequence, proof, or control mechanism the beat is about. "
    "Use designed environments and human-scale stakes instead of generic floating props or abstract machine showcases."
)
STORY_DOCUMENTARY_CINEMATIC_PROMPT_ADDON = (
    "CINEMATIC DOCUMENTARY UPGRADE RULES: open on the payoff image or contradiction first. Use dossier-table reveals, archive proof, surveillance framing, "
    "boardroom or influence staging, controlled negative space, and one dominant proof idea per frame. "
    "Avoid sterile labs, product-shot hero objects, repeated machine stages, or generic explainer filler."
)
STORY_DOCUMENTARY_REALISM_REFINEMENT = (
    "Documentary realism lock: premium CGI materials, believable lighting, human-scale environments, clear evidence props, and readable emotional consequence. "
    "No anatomy filler, floating gears, random machine cores, or glossy sci-fi widgets replacing the real point of the beat."
)
STORY_PSYCHOLOGY_DOCUMENTARY_CONSISTENCY_PROMPT = (
    "MASTER PSYCHOLOGY DOCUMENTARY CONSISTENCY RULES (apply to every scene): keep one premium hidden-behavior visual universe across scenes through recurring surveillance spaces, "
    "dossiers, influence webs, mirrors, archive props, and emotional consequence environments. Prioritize hidden triggers, manipulation, attention capture, fear, shame, desire, "
    "or social control in human-scale frames instead of literal anatomy showcases."
)
STORY_PSYCHOLOGY_DOCUMENTARY_CINEMATIC_PROMPT_ADDON = (
    "CINEMATIC PSYCHOLOGY DOCUMENTARY UPGRADE RULES: open on the invasive consequence or contradiction before the explanation. Use surveillance rooms, dossier-table reveals, "
    "mirror or split-self reversals, upscale social power tableaux, archive evidence, restrained dark-stage contrast, and one dominant emotional proof idea per frame. "
    "Avoid literal exposed brains, sterile labs, floating machine props, or generic sci-fi mechanism displays unless the narration explicitly requires them."
)
STORY_PSYCHOLOGY_DOCUMENTARY_REALISM_REFINEMENT = (
    "Psychology documentary realism lock: premium designed CG, believable lighting, elegant dark contrast, controlled social environments, and readable human consequence. "
    "No textbook anatomy, no brain-in-a-box filler, no floating gears, no sterile medical staging, and no random abstract machine sculptures."
)




def _story_scene_prefers_documentary_visuals(text: str) -> bool:
    source = str(text or "").strip().lower()
    if not source:
        return False
    return any(
        marker in source for marker in [
            "3d documentary style lock:",
            "3d psychology documentary style lock:",
            "premium stylized 3d documentary",
            "premium stylized 3d faceless documentary",
            "systems documentary",
            "psychology documentary",
            "dossier",
            "surveillance",
            "boardroom",
            "archive proof",
        ]
    )


def _story_scene_prefers_psychology_documentary_visuals_text(text: str) -> bool:
    source = str(text or "").strip().lower()
    if not source:
        return False
    if "3d psychology documentary style lock:" in source or "psychology documentary" in source:
        return True
    return bool(re.search(
        r"\b(psychology|psychological|mind|mental|behavior|behaviour|manipulat|bias|blind spot|subconscious|attention|dopamine|emotion|thought|memory|control|choice|choices|decision|decisions|influence|gaslight|fear|shame|envy|desire|hidden behavior|trigger)\b",
        source,
    ))


def _configured_image_provider_order() -> list[str]:
    provider_order = [
        str(p or "").strip().lower()
        for p in str(IMAGE_PROVIDER_ORDER or "").split(",")
        if str(p or "").strip()
    ]
    if provider_order:
        return provider_order
    if XAI_IMAGE_FALLBACK_ENABLED and bool(FAL_AI_KEY or XAI_API_KEY):
        return ["fal"]
    return ["hidream", "wan22", "sdxl"] if HIDREAM_ENABLED else ["wan22", "sdxl"]


def _configured_local_image_provider_order() -> list[str]:
    local_providers: list[str] = []
    for provider in _configured_image_provider_order():
        key = _normalize_image_provider_key(provider)
        if key in {"hidream", "wan22", "sdxl"} and key not in local_providers:
            local_providers.append(key)
    return local_providers


def _story_prompt_prefix_for_scene(visual_description: str) -> str:
    if _story_scene_prefers_documentary_visuals(visual_description):
        return STORY_DOCUMENTARY_PROMPT_PREFIX
    if _story_scene_prefers_explainer_visuals(visual_description):
        return (
            "Cinematic photoreal explainer scene, Unreal Engine 5 grade realism with filmic cinematography, "
            "emotionally resonant composition with depth of field and grounded concept detail, "
            "dramatic volumetric lighting with motivated light sources, ray traced global illumination, "
            "atmospheric particles, lens behavior, film grain, and richly detailed cinematic environment, 8k ultra HD, "
        )
    return TEMPLATE_PROMPT_PREFIXES.get("story", "")


def _story_consistency_prompt_for_scene(visual_description: str) -> str:
    if _story_scene_prefers_documentary_visuals(visual_description):
        if _story_scene_prefers_psychology_documentary_visuals_text(visual_description):
            return STORY_PSYCHOLOGY_DOCUMENTARY_CONSISTENCY_PROMPT
        return STORY_DOCUMENTARY_CONSISTENCY_PROMPT
    return STORY_EXPLAINER_CONSISTENCY_PROMPT if _story_scene_prefers_explainer_visuals(visual_description) else STORY_MASTER_CONSISTENCY_PROMPT


def _story_cinematic_addon_for_scene(visual_description: str) -> str:
    if _story_scene_prefers_documentary_visuals(visual_description):
        if _story_scene_prefers_psychology_documentary_visuals_text(visual_description):
            return STORY_PSYCHOLOGY_DOCUMENTARY_CINEMATIC_PROMPT_ADDON
        return STORY_DOCUMENTARY_CINEMATIC_PROMPT_ADDON
    return STORY_EXPLAINER_CINEMATIC_PROMPT_ADDON if _story_scene_prefers_explainer_visuals(visual_description) else STORY_CINEMATIC_PROMPT_ADDON


def _story_salvage_refinement_for_scene(prompt: str) -> str:
    if _story_scene_prefers_documentary_visuals(prompt):
        if _story_scene_prefers_psychology_documentary_visuals_text(prompt):
            return STORY_PSYCHOLOGY_DOCUMENTARY_REALISM_REFINEMENT
        return STORY_DOCUMENTARY_REALISM_REFINEMENT
    return STORY_EXPLAINER_REALISM_REFINEMENT if _story_scene_prefers_explainer_visuals(prompt) else STORY_REALISM_REFINEMENT


def _normalize_skeleton_quality_mode(value: str | None, template: str = "skeleton") -> str:
    if template not in {"skeleton", "story"}:
        return "standard"
    raw = str(value or "").strip().lower()
    if raw in {"pro", "high", "cinematic_plus"}:
        raw = "cinematic"
    if raw in SKELETON_QUALITY_MODES:
        return raw
    # Default to cinematic for skeleton/story upgrades while keeping generation memory profile unchanged.
    return "cinematic"


def _normalize_mint_mode(value, template: str) -> bool:
    t = str(template or "").strip().lower()
    # Skeleton prompts already pass through strong template constraints; mint rewrite
    # can over-truncate scene-specific objects/actions in creative control.
    if t == "skeleton":
        return False
    default_on = t in {"story"}
    return _bool_from_any(value, default=default_on)


def _creative_prompt_passthrough_enabled(session: dict | None) -> bool:
    """When enabled, Creative Control uses the user's scene prompt verbatim."""
    if not isinstance(session, dict):
        return True
    return _bool_from_any(session.get("prompt_passthrough"), True)


def _apply_mint_scene_compiler(scenes: list, template: str, mint_mode: bool = True) -> list:
    """Deterministic scene rewrite pass for stronger first-render reliability."""
    if not _normalize_mint_mode(mint_mode, template):
        return scenes
    if template not in {"skeleton", "story", "daytrading"}:
        return scenes

    def _sentences(text: str) -> list[str]:
        parts = re.split(r"(?<=[.!?])\s+", text.strip())
        return [p.strip() for p in parts if p and p.strip()]

    compiled = []
    for raw_scene in scenes or []:
        scene = dict(raw_scene or {})
        visual = re.sub(r"\s+", " ", str(scene.get("visual_description", "") or "")).strip()
        chunks = _sentences(visual)
        if not chunks:
            if template == "skeleton":
                chunks = [
                    "A canonical ivory-white anatomical skeleton with large realistic eyeballs and a clearly visible translucent body silhouette appears in a detailed cinematic environment.",
                    "The skeleton takes a clear action pose with role-specific props inside a richly detailed environment that clearly matches the specific topic, with readable background depth.",
                    "Motion begins instantly with smooth human-like momentum and directional continuity into the next shot.",
                ]
            else:
                chunks = [
                    "The primary subject(s) for this script beat appear in a specific cinematic environment.",
                    "The subject(s) perform a clear action with visible stakes and readable composition.",
                    "Camera movement and subject motion continue smoothly to preserve scene-to-scene continuity.",
                ]
        if len(chunks) > 3:
            chunks = chunks[:3]
        while len(chunks) < 3:
            chunks.append("")

        if template == "skeleton":
            skeleton_lock = "A canonical ivory-white anatomical skeleton with large realistic eyeballs and a clearly visible translucent body silhouette appears in a topic-matched cinematic environment."
            first_chunk = chunks[0].strip()
            explicit_outfit_request = bool(re.search(r"\b(outfit|uniform|suit|coat|armor|jersey|scrubs|clothes|clothing|costume|dress|robe|hoodie|jacket)\b", " ".join(chunks), re.IGNORECASE))
            if not re.search(r"\b(canonical|anatomical)\b", first_chunk, re.IGNORECASE) or not re.search(r"\b(translucent|glass[- ]?like|body silhouette)\b", first_chunk, re.IGNORECASE):
                first_chunk = f"{skeleton_lock} {first_chunk}".strip()
            if explicit_outfit_request:
                first_chunk = first_chunk.rstrip(".!?") + ". Keep any explicitly requested outfit visible while preserving the same canonical skeleton anatomy."
            elif not re.search(r"\b(no clothing|no costume|no uniforms?|no armor)\b", first_chunk, re.IGNORECASE):
                first_chunk = first_chunk.rstrip(".!?") + ". No clothing, uniforms, armor, or costumes."
            chunks[0] = first_chunk
            if not explicit_outfit_request:
                chunks[1] = re.sub(r"\b(outfit|uniform|suit|coat|armor|jersey|scrubs|clothes|clothing|costume)\b", "props", chunks[1], flags=re.IGNORECASE)
            combined = " ".join(chunks[:3])
            internal_focus = _skeleton_scene_prefers_internal_cutaway(combined)
            minimal_background = _skeleton_scene_requests_minimal_background(combined)
            if not _skeleton_scene_has_environment_cue(chunks[0]):
                if minimal_background:
                    chunks[0] = chunks[0].rstrip(".!?") + " With an intentional premium minimal backdrop."
                elif internal_focus:
                    chunks[0] = chunks[0].rstrip(".!?") + " In a readable internal or microscopic cutaway environment."
                else:
                    chunks[0] = chunks[0].rstrip(".!?") + " In a richly detailed environment matching the scene request."
            if not re.search(r"\b(holding|aiming|running|facing|fighting|action|pose|gestur|turning|pointing|sit|sits|seated|sitting)\b", chunks[1], re.IGNORECASE):
                chunks[1] = (chunks[1].rstrip(".!?") + " " if chunks[1] else "") + \
                    "The skeleton takes a clear action pose with a role-specific prop and readable composition."
            has_table = bool(re.search(r"\b(table|desk|countertop)\b", combined, re.IGNORECASE))
            has_sit = bool(re.search(r"\b(sit|sits|seated|sitting)\b", combined, re.IGNORECASE))
            has_brain = bool(re.search(r"\bbrain\b", combined, re.IGNORECASE))
            has_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", combined, re.IGNORECASE))
            if internal_focus and not re.search(r"\b(cutaway|micro|microscopic|internal|bloodstream|blood vessels?|organ|cell|tissue|anatomical|artery|vein)\b", chunks[1], re.IGNORECASE):
                chunks[1] = chunks[1].rstrip(".!?") + ". Use a readable internal or microscopic cutaway environment with clear contextual structures and layered depth."
            if has_table and not has_sit:
                chunks[1] = chunks[1].rstrip(".!?") + ". Skeleton is clearly seated at the table."
            if has_brain and has_money and not re.search(r"\b(two|both)\b", combined, re.IGNORECASE):
                chunks[2] = chunks[2].rstrip(".!?") + ". Keep both required props visible: one brain and one pile of money."
            if not _skeleton_scene_has_environment_cue(combined):
                if minimal_background:
                    chunks[1] = chunks[1].rstrip(".!?") + " Keep the requested minimal backdrop intentional and clearly lit."
                elif internal_focus:
                    chunks[1] = chunks[1].rstrip(".!?") + " Use a readable internal or microscopic cutaway environment with layered depth."
                else:
                    chunks[1] = chunks[1].rstrip(".!?") + " Place the skeleton in a richly detailed environment with readable background context."
            if not re.search(r"\b(prop|object|item|scene request|requested)\b", " ".join(chunks[:3]), re.IGNORECASE):
                chunks[1] = chunks[1].rstrip(".!?") + ". Keep all explicitly requested props/objects visible and readable."
            if not re.search(r"\b(motion|camera|moves|moving|drift|continuity|momentum)\b", chunks[2], re.IGNORECASE):
                chunks[2] = (chunks[2].rstrip(".!?") + " " if chunks[2] else "") + \
                    "Motion begins instantly with smooth momentum and directional continuity."
            if not _skeleton_scene_has_camera_cue(combined):
                chunks[2] = chunks[2].rstrip(".!?") + " Choose framing that fits the beat instead of repeating the same centered medium hero shot."
        else:
            if template == "daytrading":
                if not re.search(r"\b(trader|chart|candlestick|market|ticker|screen|dashboard|price|order flow|trading desk|broker|asset|stock|option|futures|exchange)\b", chunks[0], re.IGNORECASE):
                    chunks[0] = (chunks[0].rstrip(".!?") + " " if chunks[0] else "") + \
                        "A premium trading or investing visual is clearly visible with readable charts, dashboards, or market context."
                if not re.search(r"\b(environment|background|setting|location|desk|studio|screen|floor|exchange|dashboard)\b", chunks[0], re.IGNORECASE):
                    chunks[0] = chunks[0].rstrip(".!?") + " In a specific trading environment with professional market screens."
                if not re.search(r"\b(photoreal|realistic|ue5|terminal|tradingview|bloomberg|dom ladder|time-and-sales|volume profile|level 2)\b", " ".join(chunks[:2]), re.IGNORECASE):
                    chunks[0] = chunks[0].rstrip(".!?") + " The setup should feel photoreal and professional, with believable chart windows, order-flow tools, and trading-terminal realism."
                if re.search(r"\b(heart|brain|organ|anatomy|medical|cells?|neuron|blood|virus|capsule|machine core|reactor)\b", " ".join(chunks[:3]), re.IGNORECASE):
                    chunks[0] = chunks[0].rstrip(".!?") + " Replace any anatomy, medical, or random machine imagery with realistic trading screens, execution dashboards, chart structures, or a professional trading desk."
                if not re.search(r"\b(action|moving|running|walking|turning|interacting|holding|breaking|spiking|reversing|rotating|flashing|updating)\b", chunks[1], re.IGNORECASE):
                    chunks[1] = (chunks[1].rstrip(".!?") + " " if chunks[1] else "") + \
                        "Show the trade idea, market move, or risk/reward mechanism actively happening with visible change."
                if not re.search(r"\b(camera|motion|continuity|transition|push|orbit|parallax|zoom)\b", chunks[2], re.IGNORECASE):
                    chunks[2] = (chunks[2].rstrip(".!?") + " " if chunks[2] else "") + \
                        "Camera movement and motion graphics should keep the market explanation sharp and continuous into the next scene."
            else:
                explainer_visuals = template == "story" and _story_scene_prefers_explainer_visuals(" ".join(chunks[:3]))
                if explainer_visuals:
                    if not re.search(r"\b(anatomy|mechanism|process|system|cells?|organs?|brain|blood|temperature|infection|bacteria|virus|heat|sweat|diagram|cutaway|thermometer|molecule)\b", chunks[0], re.IGNORECASE):
                        chunks[0] = (chunks[0].rstrip(".!?") + " " if chunks[0] else "") + \
                            "The core mechanism or body-system concept for this script beat is clearly visible."
                    if not re.search(r"\b(environment|background|setting|location|interior|exterior|cutaway|macro|inside|within)\b", chunks[0], re.IGNORECASE):
                        chunks[0] = chunks[0].rstrip(".!?") + " In a specific cinematic environment or readable cutaway view."
                    if not re.search(r"\b(action|moving|running|walking|turning|interacting|holding|flow|circulat|heat|signal|response|reaction|change)\b", chunks[1], re.IGNORECASE):
                        chunks[1] = (chunks[1].rstrip(".!?") + " " if chunks[1] else "") + \
                            "Show the mechanism actively happening with visible cause-and-effect and readable stakes."
                    if not re.search(r"\b(camera|motion|continuity|transition|cutaway|macro|push)\b", chunks[2], re.IGNORECASE):
                        chunks[2] = (chunks[2].rstrip(".!?") + " " if chunks[2] else "") + \
                            "Camera movement and visual continuity should track the process clearly into the next scene."
                else:
                    if not re.search(r"\b(character|protagonist|person|figure|subject|crowd|group|family|worker)\b", chunks[0], re.IGNORECASE):
                        chunks[0] = (chunks[0].rstrip(".!?") + " " if chunks[0] else "") + \
                            "The primary subject(s) for this script beat are clearly visible."
                    if not re.search(r"\b(environment|background|setting|location|interior|exterior)\b", chunks[0], re.IGNORECASE):
                        chunks[0] = chunks[0].rstrip(".!?") + " In a specific cinematic environment."
                    if not re.search(r"\b(action|moving|running|walking|turning|interacting|holding)\b", chunks[1], re.IGNORECASE):
                        chunks[1] = (chunks[1].rstrip(".!?") + " " if chunks[1] else "") + \
                            "The subject(s) perform a clear action with visible stakes."
                    if not re.search(r"\b(camera|motion|continuity|transition)\b", chunks[2], re.IGNORECASE):
                        chunks[2] = (chunks[2].rstrip(".!?") + " " if chunks[2] else "") + \
                            "Camera and subject motion preserve smooth continuity into the next scene."

        scene["visual_description"] = " ".join([
            c.strip().rstrip(".!?") + "."
            for c in chunks[:3]
            if c and c.strip()
        ]).strip()
        compiled.append(scene)
    return compiled


def _build_skeleton_image_prompt(
    visual_description: str,
    skeleton_anchor: str = "",
    quality_mode: str = "cinematic",
    immutable_context: str = "",
) -> str:
    def _scene_object_lock(scene_text: str) -> str:
        text = str(scene_text or "").strip().lower()
        if not text:
            return ""
        rules: list[str] = []
        has_brain = bool(re.search(r"\bbrain\b", text))
        has_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", text))
        has_table = bool(re.search(r"\btable|desk|countertop\b", text))
        has_glow = bool(re.search(r"\b(glow|glowing|emissive|light[- ]?emitting|luminous)\b", text))
        if has_table:
            rules.append(
                "TABLE LOCK: skeleton is seated at a table/desk and the tabletop is clearly visible in frame."
            )
            rules.append(
                "PROP PLACEMENT LOCK: required props are on/above the tabletop, not random floating orbs unless explicitly requested."
            )
        if has_brain:
            rules.append(
                "BRAIN PROP LOCK: show one clearly identifiable human brain with realistic gyri/sulci folds (not a smooth sphere, fruit, or generic ball)."
            )
        if has_money:
            rules.append(
                "MONEY PROP LOCK: show one clearly identifiable pile/stack of paper cash banknotes (not coins-only, not blank paper scraps)."
            )
        if has_brain and has_money:
            rules.append("OBJECT COUNT LOCK: include both required props (brain and money) in the same shot and keep both readable.")
        if has_glow and (has_brain or has_money):
            rules.append(
                "GLOW LOCK: each requested glowing prop must visibly emit soft bloom/light so the glow is obvious at phone-screen size."
            )
        return " ".join(rules).strip()

    mode = _normalize_skeleton_quality_mode(quality_mode, template="skeleton")
    addon = (SKELETON_CINEMATIC_PROMPT_ADDON + " ") if mode == "cinematic" else ""
    immutable = (str(immutable_context or "").strip() + " ") if immutable_context else ""
    delta = _sanitize_skeleton_scene_delta(str(visual_description or "").strip())
    explicit_outfit_request = bool(re.search(r"\b(suit|tuxedo|armor|uniform|costume|hoodie|jacket|dress|shirt|pants|coat|robe|scrubs|jersey)\b", delta, re.IGNORECASE))
    named_human_lock = _named_human_subject_likeness_lock(delta, skeleton_mode=True)
    supports_named_humans = bool(named_human_lock)
    object_lock = _scene_object_lock(delta)
    glow_lock = ""
    context_lock = _skeleton_scene_context_lock(delta)
    framing_lock = _skeleton_scene_framing_lock(delta)
    if re.search(r"\b(glow|glowing|emissive|neon|luminescen|light[- ]?emitting)\b", delta, re.IGNORECASE):
        glow_lock = (
            "PROP LIGHTING LOCK: any requested glowing object must visibly emit light/bloom and be clearly identifiable on-screen. "
        )
    scene_mandate = (
        "SCENE CONTENT LOCK (must be visible and preserved): "
        + delta
        + ". Keep every explicitly requested prop/object/action visible and readable in frame."
    ).strip()
    return (
        SKELETON_IMAGE_STYLE_PREFIX + " "
        + scene_mandate + " "
        + (
            "SCENE PRIORITY RULE: preserve requested setting, pose, action, props, object count, and any explicitly requested outfit from the scene content lock while keeping the same canonical skeleton anatomy. "
            if explicit_outfit_request
            else "SCENE PRIORITY RULE: preserve requested setting, pose, action, props, and object count from the scene content lock; ignore any clothing/costume directives to preserve canonical skeleton identity. "
        )
        + (object_lock + " " if object_lock else "")
        + glow_lock
        + context_lock + " "
        + framing_lock + " "
        + (named_human_lock + " " if named_human_lock else "")
        + SKELETON_MASTER_CONSISTENCY_PROMPT + " "
        + "NON-NEGOTIABLE CHARACTER RULE: use the exact same canonical anatomical skeleton in every scene: ivory-white skull and bones, large realistic eyeballs with visible iris and wet reflective highlights, clearly visible translucent soft-tissue silhouette around torso/limbs, identical skull proportions and eye spacing. "
        + "COLOR/RENDER RULE: realistic photographic rendering with natural ivory bone tones; never x-ray, radiograph, CT, fluoroscopy, or neon-blue scan aesthetics. "
        + "EYE RULE: eyes must be realistic and non-glowing; no emissive, neon, laser, or light-emitting eyes. Any glow in scene is only from props/environment, never from eye sockets. "
        + "CANONICAL LOOK LOCK: this is NEVER a bare-bones model. A clear glass-like translucent human shell must visibly wrap all shown body regions (head, torso, arms, hands, pelvis, legs when visible) with bones clearly seen through it. "
        + "VISIBILITY LOCK: the translucent shell must be clearly noticeable at phone-screen size, with medium-opacity glass edges and subtle interior translucency around the skeleton form. "
        + (
            "ANATOMY LOCK: skull-only face geometry and no human skin or flesh face features apply only to the canonical skeleton; any explicitly named supporting human subject must keep a natural human face. "
            if supports_named_humans
            else "ANATOMY LOCK: skull-only face geometry, never human skin/flesh face features. "
        )
        + (
            "HAIR LOCK: no human hair, scalp, beard, eyebrows, eyelashes, wig, or hairstyle elements apply only to the canonical skeleton; explicitly named supporting humans keep their real hair or facial hair. "
            if supports_named_humans
            else "HAIR LOCK: no human hair, scalp, beard, eyebrows, eyelashes, wig, or hairstyle elements. "
        )
        + (
            "NON-NEGOTIABLE STYLE RULE: if the scene explicitly requests clothing, keep that wardrobe visible while preserving the same skeleton identity, translucent shell, and anatomy. "
            if explicit_outfit_request
            else "NON-NEGOTIABLE STYLE RULE: no clothing, uniforms, armor, or costumes on the skeleton body. "
        )
        + "COMPOSITION RULE: keep the skeleton prominent and instantly readable in vertical 9:16, but allow off-center placement and visible environment depth whenever that makes the beat clearer. The frame should feel designed, not generic. "
        + addon + immutable + skeleton_anchor + delta + " "
        + _skeleton_image_suffix_for_scene(delta)
    )


def _build_story_image_prompt(
    visual_description: str,
    quality_mode: str = "cinematic",
    immutable_context: str = "",
) -> str:
    mode = _normalize_skeleton_quality_mode(quality_mode, template="story")
    addon = (_story_cinematic_addon_for_scene(visual_description) + " ") if mode == "cinematic" else ""
    immutable = (str(immutable_context or "").strip() + " ") if immutable_context else ""
    return (
        _story_prompt_prefix_for_scene(visual_description) + " "
        + _story_consistency_prompt_for_scene(visual_description) + " "
        + addon + immutable + str(visual_description or "").strip()
    ).strip()


def _apply_template_scene_constraints(scenes: list, template: str, quality_mode: str = "standard") -> list:
    """Harden model output so template-critical constraints cannot drift."""
    if template not in {"skeleton", "story", "motivation"}:
        return scenes
    quality_mode = _normalize_skeleton_quality_mode(quality_mode, template=template)

    def _sentences(text: str) -> list[str]:
        parts = re.split(r"(?<=[.!?])\s+", text.strip())
        return [p.strip() for p in parts if p and p.strip()]

    constrained = []
    for raw_scene in scenes or []:
        scene = dict(raw_scene or {})
        visual = re.sub(r"\s+", " ", str(scene.get("visual_description", "") or "")).strip()
        chunks = _sentences(visual)
        if not chunks:
            if template == "skeleton":
                chunks = [
                    "A canonical ivory-white anatomical skeleton with large realistic eyeballs and a clearly visible translucent body silhouette appears in a topic-matched cinematic environment.",
                    "The skeleton holds role-specific props inside a richly detailed environment that clearly matches the specific topic, with readable background elements and layered depth.",
                    "Motion cue: smooth natural arm and head movement with realistic momentum.",
                ]
            else:
                chunks = [
                    "The key subject(s) from this story beat appear in a specific cinematic environment.",
                    "The subject(s) perform a clear action with visible emotional stakes and readable composition.",
                    "Motion cue: camera and subject movement preserve smooth continuity into the next scene.",
                ]
        if len(chunks) > 3:
            chunks = chunks[:3]

        if template == "skeleton":
            skeleton_lock = "A canonical ivory-white anatomical skeleton with large realistic eyeballs and a clearly visible translucent body silhouette appears in a topic-matched cinematic environment."
            first_chunk = chunks[0].strip()
            explicit_outfit_request = bool(re.search(r"\b(outfit|uniform|suit|coat|armor|jersey|scrubs|clothes|clothing|costume|dress|robe|hoodie|jacket)\b", " ".join(chunks), re.IGNORECASE))
            if not re.search(r"\b(canonical|anatomical)\b", first_chunk, re.IGNORECASE) or not re.search(r"\b(translucent|glass[- ]?like|body silhouette)\b", first_chunk, re.IGNORECASE):
                first_chunk = f"{skeleton_lock} {first_chunk}".strip()
            if explicit_outfit_request:
                first_chunk = first_chunk.rstrip(".!?") + ". Keep any explicitly requested outfit visible while preserving the same canonical skeleton anatomy."
            elif not re.search(r"\b(no clothing|no costume|no uniforms?|no armor)\b", first_chunk, re.IGNORECASE):
                first_chunk = first_chunk.rstrip(".!?") + ". No clothing, uniforms, armor, or costumes."
            chunks[0] = first_chunk

            if len(chunks) < 2:
                chunks.append("The skeleton holds role-specific props inside a richly detailed environment that clearly matches the specific topic, with readable background elements and layered depth.")
            if len(chunks) < 3:
                chunks.append("Motion cue: smooth natural arm and head movement with realistic momentum.")
            # Some scene prompts come through as a single sentence. Ensure secondary chunk exists before mutating it.
            if not explicit_outfit_request:
                chunks[1] = re.sub(
                    r"\b(outfit|uniform|suit|coat|armor|jersey|scrubs|clothes|clothing|costume)\b",
                    "props",
                    chunks[1],
                    flags=re.IGNORECASE,
                )
            combined = " ".join(chunks[:3])
            internal_focus = _skeleton_scene_prefers_internal_cutaway(combined)
            minimal_background = _skeleton_scene_requests_minimal_background(combined)
            has_table = bool(re.search(r"\b(table|desk|countertop)\b", combined, re.IGNORECASE))
            has_sit = bool(re.search(r"\b(sit|sits|seated|sitting)\b", combined, re.IGNORECASE))
            has_brain = bool(re.search(r"\bbrain\b", combined, re.IGNORECASE))
            has_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", combined, re.IGNORECASE))
            if internal_focus and not re.search(r"\b(cutaway|micro|microscopic|internal|bloodstream|blood vessels?|organ|cell|tissue|anatomical|artery|vein)\b", chunks[1], re.IGNORECASE):
                chunks[1] = chunks[1].rstrip(".!?") + ". Use a readable internal or microscopic cutaway environment with clear contextual structures and layered depth."
            if has_table and not has_sit:
                chunks[1] = chunks[1].rstrip(".!?") + ". Skeleton is clearly seated at the table."
            if has_brain and has_money and not re.search(r"\b(two|both)\b", combined, re.IGNORECASE):
                chunks[2] = chunks[2].rstrip(".!?") + ". Keep both required props visible: one brain and one pile of money."
            if not _skeleton_scene_has_environment_cue(combined):
                if minimal_background:
                    chunks[1] = chunks[1].rstrip(".!?") + ". Keep the requested minimal backdrop intentional, premium, and clearly lit."
                elif internal_focus:
                    chunks[1] = chunks[1].rstrip(".!?") + ". Use a readable internal or microscopic cutaway environment with clear contextual structures and layered depth."
                else:
                    chunks[1] = chunks[1].rstrip(".!?") + ". Place the skeleton in a richly detailed environment that clearly matches the topic, with readable background elements and layered depth."
            if not re.search(r"\b(prop|object|item|scene request|requested)\b", " ".join(chunks[:3]), re.IGNORECASE):
                chunks[1] = chunks[1].rstrip(".!?") + ". Keep all explicitly requested props/objects visible and readable."
            if not _skeleton_scene_has_camera_cue(combined):
                chunks[2] = chunks[2].rstrip(".!?") + ". Choose framing that fits the beat: wide environmental for scale, medium action for gestures, low-angle or over-shoulder for drama, and close cutaway or prop detail when needed. Avoid repeating the same centered medium hero shot."

            # Hard default for Skeleton AI: eyeballs must always be present.
            eyes_ok = any(
                re.search(r"\b(eye|eyes|eyeball|eyeballs|iris|pupil|sockets?)\b", c, re.IGNORECASE)
                for c in chunks[:3]
            )
            if not eyes_ok:
                chunks[0] = chunks[0].rstrip(".!?") + " Keep realistic visible eyeballs in both eye sockets."
        else:
            explainer_visuals = template == "story" and _story_scene_prefers_explainer_visuals(" ".join(chunks[:3]))
            first = chunks[0]
            if explainer_visuals:
                if not re.search(r"\b(anatomy|mechanism|process|system|cells?|organs?|brain|blood|temperature|infection|bacteria|virus|heat|sweat|diagram|cutaway|thermometer|molecule)\b", first, re.IGNORECASE):
                    if first:
                        first = f"The key concept or mechanism for this beat {first[0].lower() + first[1:]}"
                    else:
                        first = "The key concept or mechanism for this beat is clearly visible."
                if not re.search(r"\b(environment|setting|location|interior|exterior|city|street|room|forest|desert|studio|cutaway|macro|inside|within)\b", first, re.IGNORECASE):
                    first = first.rstrip(".!?") + " in a specific cinematic environment or readable cutaway view."
                chunks[0] = first

                if len(chunks) < 2:
                    chunks.append("Show the process actively happening with readable cause-and-effect and visible stakes.")
                if len(chunks) < 3:
                    chunks.append("Motion cue: camera movement and transitions preserve visual continuity while explaining the mechanism.")
            else:
                if not re.search(r"\b(main character|protagonist|hero|lead|character|person|subject|crowd|group|family|worker)\b", first, re.IGNORECASE):
                    if first:
                        first = f"The key subject(s) for this beat {first[0].lower() + first[1:]}"
                    else:
                        first = "The key subject(s) for this beat are clearly visible."
                if not re.search(r"\b(environment|setting|location|interior|exterior|city|street|room|forest|desert|studio)\b", first, re.IGNORECASE):
                    first = first.rstrip(".!?") + " in a specific cinematic environment."
                chunks[0] = first

                if len(chunks) < 2:
                    chunks.append("The subject(s) perform a clear action with visible emotional stakes and readable composition.")
                if len(chunks) < 3:
                    chunks.append("Motion cue: camera and subject movement preserve scene-to-scene continuity.")

        if quality_mode == "cinematic":
            if not re.search(r"\b(high[- ]stakes|danger|conflict|urgent|impact|tension|emotional stakes)\b", chunks[0], re.IGNORECASE):
                chunks[0] = chunks[0].rstrip(".!?") + " in an immediate high-stakes moment."
            if len(chunks) < 2:
                chunks.append("Dynamic 24-35mm low-angle cinematic framing with volumetric rim lighting and deep subject separation.")
            elif not re.search(r"\b(lens|camera|low-angle|over-shoulder|dolly|framing|rim light|volumetric)\b", chunks[1], re.IGNORECASE):
                chunks[1] = chunks[1].rstrip(".!?") + ". Dynamic 24-35mm cinematic framing with low-angle depth and volumetric rim lighting."
            if len(chunks) < 3:
                chunks.append("Motion starts instantly with smooth human-like momentum and carries continuity into the next shot.")
            elif not re.search(r"\b(motion|moves|moving|turns|gestures|drift|camera|continuity)\b", chunks[2], re.IGNORECASE):
                chunks[2] = chunks[2].rstrip(".!?") + ". Motion starts instantly with smooth momentum and clear directional continuity."

        scene["visual_description"] = " ".join([
            c.strip().rstrip(".!?") + "."
            for c in chunks[:3]
            if c and c.strip()
        ]).strip()
        constrained.append(scene)
    return constrained


def _force_template_scene_duration(scenes: list, template: str) -> list:
    """Lock selected templates to fixed 5s scenes for stable Kling generation."""
    if template not in {"skeleton", "story", "motivation"}:
        return scenes
    forced = []
    for s in scenes or []:
        scene = dict(s or {})
        scene["duration_sec"] = 5.0
        forced.append(scene)
    return forced


def _estimate_spoken_duration_seconds(text: str, voice_speed: float = 1.0) -> float:
    words = [w for w in str(text or "").split() if w.strip()]
    if not words:
        return 0.0
    base_wps = 2.45 * max(0.8, min(1.35, float(voice_speed or 1.0)))
    pauses = 0.12 * max(0, len(re.findall(r"[,.!?;:]", str(text or ""))))
    return max(0.2, (len(words) / base_wps) + pauses)


def _short_template_narration_fit_enabled(template: str) -> bool:
    return str(template or "").strip().lower() in {"story", "motivation", "skeleton", "daytrading", "chatstory"}


def _build_short_narration_fit_clauses(scene: dict, template: str, scene_index: int, total_scenes: int) -> list[str]:
    template_key = str(template or "").strip().lower()
    visual = str((scene or {}).get("visual_description", "") or "").strip()
    purpose = str((scene or {}).get("engagement_purpose", "") or "").strip()
    base_clauses: list[str] = []

    if template_key == "skeleton":
        base_clauses = [
            "And that damage keeps compounding the longer those bones stay exposed.",
            "That is exactly where the next part starts getting darker.",
            "And that slow breakdown is what most people never think about.",
        ]
    elif template_key == "daytrading":
        base_clauses = [
            "And that single move is usually where traders either lock in or lose control.",
            "That is the exact moment the setup either confirms or completely falls apart.",
            "And if you miss that signal, the rest of the trade gets harder fast.",
        ]
    elif template_key == "motivation":
        base_clauses = [
            "And that is usually the exact moment most people give up too early.",
            "That is where discipline matters more than motivation ever will.",
            "And the people who keep going are the ones who actually separate themselves.",
        ]
    elif template_key == "chatstory":
        base_clauses = [
            "And that is where the whole situation suddenly shifts.",
            "That single message is what changes everything next.",
            "And that is the moment the tension really starts climbing.",
        ]
    else:
        base_clauses = [
            "And that is what pushes the next part of the story forward.",
            "That is the point where the stakes start feeling more real.",
            "And that is why the next reveal hits harder than the first one.",
        ]

    visual_lower = visual.lower()
    purpose_lower = purpose.lower()
    contextual: list[str] = []
    if any(token in visual_lower for token in ["before", "after", "compare", "contrast"]):
        contextual.append("And once you see the contrast, the difference becomes impossible to ignore.")
    if any(token in visual_lower for token in ["system", "mechanism", "process", "inside", "cutaway"]):
        contextual.append("And that hidden process is what quietly drives everything that happens next.")
    if any(token in visual_lower for token in ["danger", "dark", "mystery", "betray", "secret", "fear"]):
        contextual.append("And that is exactly where the tension starts getting heavier.")
    if any(token in purpose_lower for token in ["hook", "reveal", "interrupt", "contrast", "payoff"]):
        contextual.append("And that is the beat that keeps the viewer locked in for the next reveal.")
    if scene_index == 0:
        contextual.insert(0, "And that opening hit is what makes people keep watching.")
    elif scene_index >= max(0, total_scenes - 2):
        contextual.insert(0, "And that payoff is what makes the whole point finally land.")
    return contextual + base_clauses


def _apply_short_scene_narration_fit(scenes: list, template: str, voice_speed: float = 1.0) -> list:
    if not _short_template_narration_fit_enabled(template):
        return scenes
    fitted: list[dict] = []
    total = len(scenes or [])
    for index, raw in enumerate(scenes or []):
        scene = dict(raw or {})
        narration = str(scene.get("narration", "") or "").strip()
        duration = max(3.5, float(scene.get("duration_sec", 5.0) or 5.0))
        estimate = _estimate_spoken_duration_seconds(narration, voice_speed=voice_speed)
        target_floor = max(3.9, duration - 0.45)
        if narration and estimate < target_floor:
            clauses = _build_short_narration_fit_clauses(scene, template, index, total)
            for clause in clauses:
                candidate = narration.rstrip(".!? ")
                if clause.lower() in candidate.lower():
                    continue
                candidate = f"{candidate}. {clause}".strip()
                candidate_estimate = _estimate_spoken_duration_seconds(candidate, voice_speed=voice_speed)
                narration = candidate
                estimate = candidate_estimate
                if estimate >= target_floor:
                    break
            scene["narration"] = narration
            scene["_narration_fit_adjusted"] = True
            scene["_narration_fit_estimate_sec"] = round(estimate, 2)
        fitted.append(scene)
    return fitted


def _short_template_pattern_interrupt_interval_sec(template: str, pacing_mode: str = "standard") -> int:
    normalized = str(template or "").strip().lower()
    base = 12
    if normalized in {"skeleton", "daytrading", "chatstory"}:
        base = 10
    elif normalized in {"story"}:
        base = 11
    elif normalized in {"motivation"}:
        base = 12
    mode = _normalize_pacing_mode(pacing_mode)
    if mode == "fast":
        base = max(8, base - 1)
    elif mode == "very_fast":
        base = max(7, base - 2)
    return int(base)


def _apply_short_execution_pacing_profile(
    scenes: list,
    template: str,
    voice_speed: float = 1.0,
    pacing_mode: str = "standard",
    topic: str = "",
    memory_public: dict | None = None,
) -> list:
    if not _short_template_narration_fit_enabled(template):
        return scenes
    normalized = str(template or "").strip().lower()
    total = len(scenes or [])
    mode = _normalize_pacing_mode(pacing_mode)
    scene_texts: list[str] = []
    for raw in scenes or []:
        payload = dict(raw or {})
        scene_texts.extend(
            [
                str(payload.get("narration", "") or "").strip(),
                str(payload.get("visual_description", "") or "").strip(),
            ]
        )
    execution_pack = _catalyst_short_execution_pack(
        template=template,
        topic=topic,
        scene_texts=scene_texts,
        pacing_mode=mode,
        memory_public=memory_public,
    )
    interrupt_every = max(2, int(round(float(execution_pack.get("pattern_interrupt_interval_sec", 9) or 9) / 5.0)))
    profiled: list[dict] = []
    for index, raw in enumerate(scenes or []):
        scene = dict(raw or {})
        duration = max(3.5, float(scene.get("duration_sec", 5.0) or 5.0))
        estimate = float(scene.get("_narration_fit_estimate_sec", _estimate_spoken_duration_seconds(scene.get("narration", ""), voice_speed=voice_speed)) or 0.0)
        fill_ratio = max(0.0, min(1.6, estimate / max(duration, 0.1)))
        is_opening = index == 0
        is_early = index <= 1
        is_payoff = index >= max(0, total - 2)
        is_interrupt = index > 0 and (index + 1) % interrupt_every == 0

        opening_intensity = str(execution_pack.get("opening_intensity", "aggressive") or "aggressive").strip().lower()
        interrupt_strength = str(execution_pack.get("interrupt_strength", "medium") or "medium").strip().lower()
        cut_profile = str(execution_pack.get("cut_profile", "dynamic-cut") or "dynamic-cut").strip().lower()
        caption_rhythm = str(execution_pack.get("caption_rhythm", "pulse-synced") or "pulse-synced").strip().lower()
        sound_density = str(execution_pack.get("sound_density", "controlled") or "controlled").strip().lower()
        voice_pacing_bias = str(execution_pack.get("voice_pacing_bias", "steady") or "steady").strip().lower()
        visual_variation_rule = _clip_text(str(execution_pack.get("visual_variation_rule", "") or ""), 180)
        payoff_hold_sec = max(0.85, float(execution_pack.get("payoff_hold_sec", 1.05) or 1.05))
        execution_intensity = str(execution_pack.get("default_execution_intensity", "medium") or "medium").strip().lower()
        if is_opening and opening_intensity == "attack":
            execution_intensity = "attack"
        elif is_opening and opening_intensity == "aggressive" and execution_intensity == "medium":
            execution_intensity = "high"

        if fill_ratio < 0.74:
            interrupt_strength = "high"
            cut_profile = "punch-cut" if normalized in {"daytrading", "chatstory"} else "contrast-cut"
            if is_opening or normalized in {"skeleton", "story"}:
                execution_intensity = "attack"
        elif fill_ratio < 0.9:
            if interrupt_strength != "high":
                interrupt_strength = "medium"
            if cut_profile == "dynamic-cut":
                cut_profile = "contrast-cut" if is_early else "dynamic-cut"

        if mode == "very_fast":
            interrupt_strength = "high"
            if execution_intensity == "medium":
                execution_intensity = "high"
            if cut_profile == "dynamic-cut":
                cut_profile = "punch-cut"
        elif mode == "fast" and interrupt_strength != "high":
            interrupt_strength = "medium"

        if is_payoff:
            payoff_hold_sec = max(payoff_hold_sec, 1.2 if fill_ratio >= 0.9 else 1.05)

        motion_direction = str(scene.get("motion_direction", "") or "").strip()
        if not motion_direction:
            motion_direction = "controlled push-in with a clean contrast cut"
        if visual_variation_rule and visual_variation_rule.lower() not in motion_direction.lower():
            motion_direction = motion_direction.rstrip(". ") + f"; {visual_variation_rule}"
        if interrupt_strength == "high" and "interrupt" not in motion_direction.lower():
            motion_direction = motion_direction.rstrip(". ") + "; add a fast pattern interrupt before the beat lands."
        elif is_payoff and "payoff" not in motion_direction.lower():
            motion_direction = motion_direction.rstrip(". ") + "; hold the payoff slightly longer so the final point lands."

        sfx_direction = str(scene.get("sfx_direction", "") or "").strip()
        if not sfx_direction:
            sfx_direction = "tight cinematic accent"
        if interrupt_strength == "high" and "interrupt" not in sfx_direction.lower():
            sfx_direction = sfx_direction.rstrip(". ") + "; sharper interrupt accent and stronger whoosh timing."
        elif is_payoff and "resolve" not in sfx_direction.lower():
            sfx_direction = sfx_direction.rstrip(". ") + "; add a cleaner payoff resolve under the final phrase."
        if sound_density == "trailer-heavy" and "trailer" not in sfx_direction.lower():
            sfx_direction = sfx_direction.rstrip(". ") + "; trailer-grade low-end accent and bigger reveal hit timing."

        engagement_purpose = str(scene.get("engagement_purpose", "") or "").strip()
        if not engagement_purpose:
            engagement_purpose = "advance the short with a stronger contrast, reveal, or payoff"
        if fill_ratio < 0.8 and "keep the viewer locked in" not in engagement_purpose.lower():
            engagement_purpose = engagement_purpose.rstrip(". ") + ". Keep the viewer locked in with a stronger mid-beat escalation."
        if is_interrupt and "pattern interrupt" not in engagement_purpose.lower():
            engagement_purpose = engagement_purpose.rstrip(". ") + ". Land a visible pattern interrupt here."

        scene["_narration_fill_ratio"] = round(fill_ratio, 2)
        scene["_execution_intensity"] = execution_intensity
        scene["_archetype_key"] = str(execution_pack.get("archetype_key", "") or "")
        scene["_archetype_label"] = str(execution_pack.get("archetype_label", "") or "")
        scene["_opening_intensity"] = opening_intensity
        scene["_interrupt_strength"] = interrupt_strength
        scene["_cut_profile"] = cut_profile
        scene["_caption_rhythm"] = caption_rhythm
        scene["_sound_density"] = sound_density
        scene["_voice_pacing_bias"] = voice_pacing_bias
        scene["_visual_variation_rule"] = visual_variation_rule
        scene["_pattern_interrupt_interval_sec"] = int(execution_pack.get("pattern_interrupt_interval_sec", 9) or 9)
        scene["_payoff_hold_sec"] = round(float(payoff_hold_sec), 2)
        scene["motion_direction"] = motion_direction
        scene["sfx_direction"] = sfx_direction
        scene["engagement_purpose"] = engagement_purpose
        profiled.append(scene)
    return profiled


def _job_diag_init(job_id: str, mode: str):
    _prune_in_memory_jobs()
    job = jobs.get(job_id)
    if not job:
        return
    now = time.time()
    job["diagnostics"] = {
        "mode": mode,
        "started_at": now,
        "last_updated_at": now,
        "current_stage": "queued",
        "stage_started_at": now,
        "stage_durations_sec": {},
        "scene_events": [],
    }
    try:
        asyncio.create_task(persist_job_state(job_id, job))
    except Exception:
        pass


def _job_set_stage(job_id: str, status: str, progress: int | None = None):
    job = jobs.get(job_id)
    if not job:
        return
    now = time.time()
    diag = job.setdefault("diagnostics", {
        "mode": "unknown",
        "started_at": now,
        "last_updated_at": now,
        "current_stage": "queued",
        "stage_started_at": now,
        "stage_durations_sec": {},
        "scene_events": [],
    })
    prev_stage = diag.get("current_stage")
    prev_started = float(diag.get("stage_started_at") or now)
    if prev_stage:
        elapsed = max(0.0, now - prev_started)
        durations = diag.setdefault("stage_durations_sec", {})
        durations[prev_stage] = round(float(durations.get(prev_stage, 0.0)) + elapsed, 2)
    diag["current_stage"] = status
    diag["stage_started_at"] = now
    diag["last_updated_at"] = now
    job["status"] = status
    if progress is not None:
        job["progress"] = progress
    try:
        asyncio.create_task(persist_job_state(job_id, job))
    except Exception:
        pass


def _job_record_scene_event(job_id: str, scene_idx: int, total_scenes: int, event: str, detail: str = ""):
    job = jobs.get(job_id)
    if not job:
        return
    now = time.time()
    diag = job.setdefault("diagnostics", {"scene_events": [], "last_updated_at": now})
    events = diag.setdefault("scene_events", [])
    events.append({
        "ts": round(now, 3),
        "scene": scene_idx + 1,
        "total_scenes": total_scenes,
        "event": event,
        "detail": detail[:240],
    })
    if len(events) > 30:
        del events[:-30]
    diag["last_updated_at"] = now
    try:
        asyncio.create_task(persist_job_state(job_id, job))
    except Exception:
        pass


def _job_update_preview(job_id: str, preview_url: str = "", preview_type: str = "image", preview_label: str = ""):
    job = jobs.get(job_id)
    if not job:
        return
    if str(preview_url or "").strip():
        job["preview_url"] = str(preview_url or "").strip()
        job["preview_type"] = str(preview_type or "image").strip() or "image"
        job["preview_label"] = str(preview_label or "").strip()
    try:
        asyncio.create_task(persist_job_state(job_id, job))
    except Exception:
        pass


def _job_update_scene_pointer(
    job_id: str,
    current_scene: int,
    total_scenes: int,
    *,
    chapter_index: int | None = None,
    scene_num: int | None = None,
):
    job = jobs.get(job_id)
    if not job:
        return
    job["current_scene"] = max(0, int(current_scene or 0))
    job["total_scenes"] = max(0, int(total_scenes or 0))
    if chapter_index is not None:
        job["current_chapter"] = max(0, int(chapter_index or 0))
    if scene_num is not None:
        job["current_chapter_scene"] = max(0, int(scene_num or 0))
    try:
        asyncio.create_task(persist_job_state(job_id, job))
    except Exception:
        pass


def _job_diag_finalize(job_id: str):
    job = jobs.get(job_id)
    if not job:
        return
    now = time.time()
    diag = job.get("diagnostics")
    if not isinstance(diag, dict):
        return
    current_stage = diag.get("current_stage")
    stage_started = float(diag.get("stage_started_at") or now)
    if current_stage:
        elapsed = max(0.0, now - stage_started)
        durations = diag.setdefault("stage_durations_sec", {})
        durations[current_stage] = round(float(durations.get(current_stage, 0.0)) + elapsed, 2)
    diag["stage_started_at"] = now
    diag["last_updated_at"] = now
    status = str(job.get("status", "") or "")
    if status in {"complete", "error"} and not job.get("completed_at"):
        job["completed_at"] = now
    if status == "error" and job.get("credit_charged") and (not job.get("credit_refunded")):
        user_id = str(job.get("user_id", "") or "")
        source = str(job.get("credit_source", "") or "")
        month_key = str(job.get("credit_month_key", "") or "")
        if user_id and source in {"monthly", "topup"}:
            try:
                asyncio.create_task(_refund_generation_credit(
                    user_id,
                    source,
                    month_key=month_key,
                    credits=int(job.get("credit_amount", 1) or 1),
                ))
                job["credit_refunded"] = True
            except Exception:
                pass
    if status in {"complete", "error"} and job.get("credit_charged"):
        _append_usage_ledger({
            "type": "generation_terminal",
            "job_id": job_id,
            "user_id": str(job.get("user_id", "") or ""),
            "status": status,
            "template": str(job.get("template", "") or ""),
            "plan": str(job.get("plan", "") or ""),
            "quality_mode": str(job.get("quality_mode", "") or ""),
            "mint_mode": bool(job.get("mint_mode", False)),
            "credit_source": str(job.get("credit_source", "") or ""),
            "credit_refunded": bool(job.get("credit_refunded", False)),
            "estimated_cost_usd": float(_estimate_job_cost_usd(job)),
            "ts": now,
        })
    _record_kpi_for_job(job_id, job)
    try:
        asyncio.create_task(persist_job_state(job_id, job))
    except Exception:
        pass

async def run_generation_pipeline(
    job_id: str,
    template: str,
    topic: str,
    resolution: str = "720p",
    language: str = "en",
    quality_mode: str = "standard",
    mint_mode: bool = True,
    transition_style: str = "smooth",
    micro_escalation_mode: bool = True,
):
    try:
        job_state = jobs.get(job_id, {})
        voice_id = str(job_state.get("voice_id", "") or "").strip()
        voice_speed = _normalize_voice_speed(job_state.get("voice_speed", 1.0), default=1.0)
        pacing_mode = _normalize_pacing_mode(job_state.get("pacing_mode", "standard"))
        art_style = _normalize_art_style(job_state.get("art_style", "auto"), template=template)
        youtube_channel_id = str(job_state.get("youtube_channel_id", "") or "").strip()
        trend_hunt_enabled = _bool_from_any(job_state.get("trend_hunt_enabled"), False)
        reference_image_url = _normalize_reference_with_default(template, str(job_state.get("reference_image_url", "") or "").strip())
        reference_lock_mode = _normalize_reference_lock_mode(job_state.get("reference_lock_mode"), default="strict")
        reference_dna = job_state.get("reference_dna", {}) if isinstance(job_state.get("reference_dna"), dict) else {}
        _job_set_stage(job_id, "generating_script", 5)
        lang_name = SUPPORTED_LANGUAGES.get(language, {}).get("name", "English")
        log.info(f"[{job_id}] Generating script for '{topic}' ({template}, {resolution}, {lang_name})")

        lang_instruction = ""
        if language != "en":
            lang_instruction = f"\n\nIMPORTANT: Write ALL narration text in {lang_name}. The visual_description fields should remain in English (for image generation), but ALL narration/voiceover text MUST be in {lang_name}."
        catalyst_shorts_instructions = str(job_state.get("catalyst_shorts_instructions", "") or "").strip()
        extra_instructions = lang_instruction + (("\n\n" + catalyst_shorts_instructions) if catalyst_shorts_instructions else "")
        try:
            script_data = await generate_script(template, topic, extra_instructions=extra_instructions)
        except Exception as e:
            if template == "skeleton":
                channel_context = {}
                trend_titles: list[str] = []
                public_shorts_playbook: dict = {}
                memory_public: dict = {}
                user_id = str(job_state.get("user_id", "") or "").strip()
                if user_id and (youtube_channel_id or trend_hunt_enabled):
                    channel_context = await _youtube_selected_channel_context({"id": user_id}, youtube_channel_id)
                    memory_channel_id = str(channel_context.get("channel_id", "") or youtube_channel_id or "").strip()
                    if memory_channel_id:
                        memory_key = _catalyst_channel_memory_key(user_id, memory_channel_id, template)
                        async with _catalyst_memory_lock:
                            _load_catalyst_memory()
                            memory_public = _catalyst_channel_memory_public_view(dict(_catalyst_channel_memory.get(memory_key) or {}))
                try:
                    public_shorts_playbook = await _build_shorts_public_reference_playbook(
                        template,
                        topic,
                        channel_context,
                        {},
                        memory_public=memory_public,
                        trend_hunt_enabled=trend_hunt_enabled,
                    )
                except Exception as inner_exc:
                    log.warning(f"Skeleton public shorts playbook fallback failed: {inner_exc}")
                    public_shorts_playbook = {}
                if trend_hunt_enabled:
                    trend_query = _build_shorts_trend_query(template, topic, channel_context, {})
                    trend_titles = await _youtube_fetch_public_trend_titles(trend_query, max_results=6)
                log.warning(
                    "Skeleton auto generation script phase failed, using local fallback "
                    f"(trend_hunt={trend_hunt_enabled}): {e}"
                )
                script_data = _build_skeleton_shorts_local_fallback(
                    topic,
                    channel_context=channel_context,
                    trend_titles=trend_titles,
                    public_shorts_playbook=public_shorts_playbook,
                    script_to_short_mode=False,
                    trend_hunt_enabled=trend_hunt_enabled,
                )
            else:
                raise
        scenes = _normalize_scenes_for_render(script_data.get("scenes", []))
        quality_mode = _normalize_skeleton_quality_mode(quality_mode, template=template)
        mint_mode = _normalize_mint_mode(mint_mode, template=template)
        micro_escalation_mode = _normalize_micro_escalation_mode(micro_escalation_mode, template=template)
        scenes = _apply_template_scene_constraints(scenes, template, quality_mode=quality_mode)
        scenes = _apply_mint_scene_compiler(scenes, template, mint_mode=mint_mode)
        if not (template == "story" and pacing_mode != "standard"):
            scenes = _force_template_scene_duration(scenes, template)
        scenes = _apply_story_pacing(scenes, template, pacing_mode=pacing_mode)
        scenes = _apply_short_scene_narration_fit(scenes, template, voice_speed=voice_speed)
        catalyst_memory_public = _catalyst_short_memory_public_snapshot(
            user_id=str(job_state.get("user_id", "") or ""),
            template=template,
            youtube_channel_id=youtube_channel_id,
            seed_memory_public=dict(job_state.get("channel_memory") or {}),
        )
        scenes = _apply_short_execution_pacing_profile(
            scenes,
            template,
            voice_speed=voice_speed,
            pacing_mode=pacing_mode,
            topic=topic,
            memory_public=catalyst_memory_public,
        )
        if not scenes:
            raise ValueError("Script generation returned no scenes")

        fal_video_enabled = bool(FAL_AI_KEY)
        runway_video_enabled = bool(RUNWAY_API_KEY)
        use_video_engine = fal_video_enabled or runway_video_enabled
        animation_enabled = _bool_from_any(job_state.get("animation_enabled"), _bool_from_any(job_state.get("story_animation_enabled"), True))
        use_video = bool(use_video_engine and animation_enabled)
        if template == "reddit":
            use_video = False
        if template != "reddit" and animation_enabled and not use_video_engine:
            raise RuntimeError("Video is required but no engine is configured (set RUNWAY_API_KEY or FAL_AI_KEY)")
        if not animation_enabled or template == "reddit":
            mode_label = "static image"
        elif runway_video_enabled:
            mode_label = "Runway (primary)"
        elif fal_video_enabled:
            mode_label = "FalAI Kling 2.1"
        else:
            mode_label = "static image"
        jobs[job_id]["generation_mode"] = "video" if use_video else "image"

        _job_set_stage(job_id, "generating_images", 10)
        jobs[job_id]["total_scenes"] = len(scenes)
        log.info(f"[{job_id}] Script ready: {len(scenes)} scenes. Mode: {mode_label}, {resolution}")

        neg_prompt = TEMPLATE_NEGATIVE_PROMPTS.get(template, NEGATIVE_PROMPT)
        scene_assets = []
        scene_prompts = [str(s.get("visual_description", "") or "") for s in scenes]
        scene_images = []
        total_steps = len(scenes) * (2 if use_video else 1)
        gen_ts = str(int(time.time() * 1000))

        skeleton_anchor = ""
        skeleton_reference_image_url = ""
        if template == "skeleton":
            skeleton_anchor = _canonical_skeleton_anchor()
            skeleton_reference_image_url = reference_image_url or SKELETON_GLOBAL_REFERENCE_IMAGE_URL

        for i, scene in enumerate(scenes):
            jobs[job_id]["current_scene"] = i + 1
            step_base = i * (2 if use_video else 1)
            _job_set_stage(job_id, "generating_images", 10 + int((step_base / total_steps) * 55))
            _job_record_scene_event(job_id, i, len(scenes), "image_start")

            full_prompt = _build_scene_prompt_with_reference(
                template=template,
                visual_description=scene.get("visual_description", ""),
                quality_mode=quality_mode,
                skeleton_anchor=skeleton_anchor,
                reference_dna=reference_dna,
                reference_lock_mode=reference_lock_mode,
                art_style=art_style,
            )
            img_path = str(TEMP_DIR / (job_id + "_scene_" + str(i) + ".png"))
            scene_reference_url = _resolve_reference_for_scene(job_state, template, i) or (
                skeleton_reference_image_url if template == "skeleton" else reference_image_url
            )
            img_result = await generate_scene_image(
                full_prompt,
                img_path,
                resolution=resolution,
                negative_prompt=neg_prompt,
                template=template,
                channel_context=channel_context,
                reference_image_url=scene_reference_url,
                reference_lock_mode=reference_lock_mode,
            )
            if template == "skeleton" and not skeleton_reference_image_url and i == 0:
                skeleton_reference_image_url = _file_to_data_image_url(img_path)
            cdn_url = img_result.get("cdn_url")
            engine_name = "Skeleton LoRA" if (template == "skeleton" and not cdn_url) else ("Grok Imagine" if cdn_url else "SDXL")
            log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} image generated ({engine_name})")
            _job_record_scene_event(job_id, i, len(scenes), "image_ready", engine_name)
            if resolution == "720p":
                persisted_scene = _persist_auto_scene_image(
                    job_id,
                    i,
                    img_path,
                    scene,
                    template,
                    img_result,
                    source="initial_auto",
                )
                if persisted_scene:
                    scene_images.append(persisted_scene)

            asset = {"image": img_path, "frames": None, "kling_clip": None}

            if use_video:
                _job_set_stage(job_id, "animating_scenes", 10 + int(((step_base + 1) / total_steps) * 55))
                _job_record_scene_event(job_id, i, len(scenes), "animation_start")
                kling_motion = TEMPLATE_KLING_MOTION.get(template, "Cinematic motion, smooth camera movement, subtle animation.")
                anim_prompt = scene.get("visual_description", "") + " " + kling_motion
                try:
                    anim_result = await animate_scene(
                        img_path, anim_prompt,
                        str(TEMP_DIR), i, gen_ts,
                        duration_sec=scene.get("duration_sec", 5),
                        image_cdn_url=cdn_url,
                        prefer_wan=(template == "skeleton"),
                    )
                except Exception as anim_err:
                    jobs[job_id]["animation_warnings"] = int(jobs[job_id].get("animation_warnings", 0)) + 1
                    log.warning(f"[{job_id}] Scene {i+1}/{len(scenes)} animation failed, using static image: {anim_err}")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_failed", str(anim_err))
                    anim_result = {"type": "static"}
                if anim_result["type"] in ("kling_clip", "wan_clip", "grok_clip", "runway_clip"):
                    asset["kling_clip"] = anim_result["path"]
                    if anim_result["type"] == "runway_clip":
                        engine = "Runway"
                    elif anim_result["type"] == "grok_clip":
                        engine = "Grok Imagine Video"
                    elif anim_result["type"] == "kling_clip":
                        engine = "Kling 2.1"
                    else:
                        engine = "Wan 2.2"
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} animated by {engine}")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_ready", engine)
                else:
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} using static image")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_static")

            scene_assets.append(asset)

        jobs[job_id]["scene_prompts"] = scene_prompts
        jobs[job_id]["scene_images"] = scene_images

        _job_set_stage(job_id, "generating_voice", 70)
        log.info(f"[{job_id}] Generating voiceover...")
        audio_assets = await _prepare_short_audio_assets(
            scenes,
            audio_path=str(TEMP_DIR / (job_id + "_voice.mp3")),
            subtitle_path=str(TEMP_DIR / (job_id + "_captions.ass")),
            template=template,
            language=language,
            override_voice_id=voice_id,
            override_speed=voice_speed,
            subtitles_enabled=True,
            resolution=resolution,
            job_id=job_id,
            full_narration_override="",
        )
        audio_path = str(audio_assets.get("audio_path", "") or "")
        word_timings = list(audio_assets.get("word_timings", []) or [])
        subtitle_path = str(audio_assets.get("subtitle_path", "") or "") or None
        jobs[job_id]["audio_mode"] = str(audio_assets.get("audio_mode", "") or "")
        jobs[job_id]["audio_warning"] = str(audio_assets.get("audio_warning", "") or "")
        if word_timings:
            log.info(f"[{job_id}] Word-synced captions generated: {len(word_timings)} words ({lang_name})")
        elif subtitle_path:
            log.info(f"[{job_id}] Scene-timed captions generated without voice timestamps")

        sound_mix_profile = _creative_template_sound_mix_profile(template)
        sfx_paths = []
        if (_sfx_enabled() or _creative_template_force_sfx(template)) and ELEVENLABS_API_KEY:
            _job_set_stage(job_id, "generating_sfx", 78)
            for i, scene in enumerate(scenes):
                sfx_out = str(TEMP_DIR / (job_id + "_sfx_" + str(i) + ".mp3"))
                desc = scene.get("visual_description", "")
                dur = scene.get("duration_sec", 5)
                sfx_file = await generate_scene_sfx(
                    desc,
                    dur,
                    sfx_out,
                    template=template,
                    scene_index=i,
                    total_scenes=len(scenes),
                    force=_creative_template_force_sfx(template),
                )
                sfx_paths.append(sfx_file)
            sfx_paths = await _quintuple_check_scene_sfx(scenes, sfx_paths, template, job_id=job_id)
            log.info(f"[{job_id}] SFX generated: {sum(1 for s in sfx_paths if s)}/{len(scenes)} scenes")
        else:
            log.info(f"[{job_id}] SFX disabled globally; skipping generation/mix")

        bgm_track = ""
        if bool(sound_mix_profile.get("bgm_required")):
            _job_set_stage(job_id, "generating_sfx", 80)
            bgm_path = str(TEMP_DIR / f"{job_id}_short_bgm.mp3")
            total_duration = sum(float((s or {}).get("duration_sec", 5.0) or 5.0) for s in scenes) + 0.8
            bgm_track = await _generate_catalyst_bgm_track(
                total_duration,
                bgm_path,
                music_profile=str(sound_mix_profile.get("music_profile", "") or ""),
                whisper_mode=str(sound_mix_profile.get("whisper_mode", "off") or "off"),
                format_preset="shorts",
            )

        _job_set_stage(job_id, "compositing", 82)
        log.info(f"[{job_id}] Compositing final video at {resolution}...")

        output_filename = template + "_" + job_id + ".mp4"
        output_path = str(OUTPUT_DIR / output_filename)
        await composite_video(
            scenes,
            scene_assets,
            audio_path,
            output_path,
            resolution=resolution,
            use_svd=use_video,
            subtitle_path=subtitle_path,
            sfx_paths=sfx_paths,
            bgm_track=bgm_track,
            transition_style=_normalize_transition_style(transition_style),
            micro_escalation_mode=micro_escalation_mode,
            pattern_interrupt_interval_sec=_short_template_pattern_interrupt_interval_sec(template, pacing_mode=pacing_mode),
            voice_gain=float(sound_mix_profile.get("voice_gain", 1.0) or 1.0),
            ambience_gain=float(sound_mix_profile.get("ambience_gain", 0.18) or 0.18),
            sfx_gain=float(sound_mix_profile.get("sfx_gain", 1.0) or 1.0),
            bgm_gain=float(sound_mix_profile.get("bgm_gain", 0.55) or 0.55),
            job_id=job_id,
            minimum_scene_duration=5.0,
        )

        try:
            await _persist_catalyst_short_learning_for_render(
                user_id=str(job_state.get("user_id", "") or ""),
                job_id=job_id,
                template=template,
                topic=topic,
                youtube_channel_id=str(job_state.get("youtube_channel_id", "") or ""),
                script_data=script_data,
                scenes=scenes,
                word_timings=word_timings,
                sound_mix_profile=sound_mix_profile,
                transition_style=transition_style,
                pacing_mode=pacing_mode,
                voice_speed=voice_speed,
                sfx_paths=sfx_paths,
                bgm_track=bgm_track,
                subtitle_path=subtitle_path or "",
                animation_enabled=animation_enabled,
            )
        except Exception as learning_err:
            log.warning(f"[{job_id}] Catalyst short learning persistence failed: {learning_err}")

        for sfx in sfx_paths:
            if sfx:
                Path(sfx).unlink(missing_ok=True)
        if bgm_track:
            Path(bgm_track).unlink(missing_ok=True)
        for asset in scene_assets:
            Path(asset["image"]).unlink(missing_ok=True)
            if asset.get("kling_clip"):
                Path(asset["kling_clip"]).unlink(missing_ok=True)
            if asset.get("frames"):
                for fp in asset["frames"]:
                    Path(fp).unlink(missing_ok=True)
                frame_dir = Path(asset["frames"][0]).parent
                if frame_dir.exists():
                    shutil.rmtree(frame_dir, ignore_errors=True)
        Path(audio_path).unlink(missing_ok=True)
        if subtitle_path:
            Path(subtitle_path).unlink(missing_ok=True)

        _job_set_stage(job_id, "complete", 100)
        jobs[job_id]["output_file"] = output_filename
        jobs[job_id]["resolution"] = resolution
        jobs[job_id]["metadata"] = {
            "title": script_data.get("title", topic),
            "description": script_data.get("description", ""),
            "tags": script_data.get("tags", []),
        }
        await _update_project_by_job(job_id, {
            "status": "rendered",
            "output_file": output_filename,
            "title": script_data.get("title", topic),
        })
        _job_diag_finalize(job_id)
        await persist_job_state(job_id, jobs[job_id])
        log.info(f"[{job_id}] COMPLETE: {output_filename} ({resolution}, {mode_label})")

    except Exception as e:
        log.error(f"[{job_id}] Pipeline failed: {e}", exc_info=True)
        _job_set_stage(job_id, "error")
        jobs[job_id]["error"] = str(e)
        _job_diag_finalize(job_id)
        await persist_job_state(job_id, jobs[job_id])
        await _update_project_by_job(job_id, {"status": "error", "error": str(e)})


# â”€â”€â”€ API Endpoints â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def _create_or_update_project(project_id: str, data: dict):
    async with _projects_lock:
        existing = _projects.get(project_id, {})
        merged = {**existing, **data}
        merged["project_id"] = project_id
        merged["updated_at"] = time.time()
        if "created_at" not in merged:
            merged["created_at"] = time.time()
        _projects[project_id] = merged
        _save_projects_store()


async def _update_project_by_job(job_id: str, fields: dict):
    async with _projects_lock:
        target_id = None
        for pid, p in _projects.items():
            if p.get("job_id") == job_id:
                target_id = pid
                break
        if not target_id:
            return
        _projects[target_id] = {**_projects[target_id], **fields, "updated_at": time.time()}
        _save_projects_store()


async def _update_project_by_session(user_id: str, session_id: str, fields: dict):
    async with _projects_lock:
        target_id = None
        for pid, p in _projects.items():
            if p.get("user_id") == user_id and p.get("session_id") == session_id:
                target_id = pid
                break
        if not target_id:
            return
        _projects[target_id] = {**_projects[target_id], **fields, "updated_at": time.time()}
        _save_projects_store()


class LongFormPauseError(RuntimeError):
    def __init__(self, message: str, details: dict):
        super().__init__(message)
        self.details = dict(details or {})


def _longform_review_state(session: dict) -> dict:
    chapters = list((session or {}).get("chapters") or [])
    total = len(chapters)
    approved = sum(1 for c in chapters if str((c or {}).get("status", "")).strip().lower() == "approved")
    pending = total - approved
    avg_score = 0.0
    if total > 0:
        avg_score = sum(float((c or {}).get("viral_score", 0) or 0) for c in chapters) / total
    return {
        "total_chapters": total,
        "approved_chapters": approved,
        "pending_chapters": max(0, pending),
        "all_approved": (total > 0 and approved == total),
        "viral_score_total": round(avg_score, 2),
    }


def _catalyst_default_fix_note_for_session(session: dict, chapter_index: int, manual_note: str = "") -> str:
    manual = _clip_text(str(manual_note or "").strip(), 1600)
    if manual:
        return manual
    session_snapshot = dict(session or {})
    format_preset = str(session_snapshot.get("format_preset", "") or "").strip().lower()
    edit_blueprint = dict(session_snapshot.get("edit_blueprint") or {})
    chapter_blueprint = _catalyst_chapter_blueprint_for_index(edit_blueprint, chapter_index)
    metadata_pack = dict(session_snapshot.get("metadata_pack") or {})
    channel_context = dict(metadata_pack.get("youtube_channel") or {})
    channel_title = _clip_text(
        str(channel_context.get("channel_title", "") or channel_context.get("title", "") or "").strip(),
        120,
    )
    preflight = _catalyst_longform_preflight(session_snapshot)
    blockers = [str(v).strip() for v in list(preflight.get("blockers") or []) if str(v).strip()]
    next_fixes = [str(v).strip() for v in list(preflight.get("next_fixes") or []) if str(v).strip()]
    memory_view = _coerce_documentary_longform_channel_memory(
        channel_context,
        dict(metadata_pack.get("catalyst_channel_memory") or session_snapshot.get("channel_memory") or {}),
        format_preset=format_preset,
        topic=str(session_snapshot.get("topic", "") or ""),
        input_title=str(session_snapshot.get("input_title", "") or ""),
    )
    rewrite_pressure = dict(memory_view.get("rewrite_pressure") or edit_blueprint.get("rewrite_pressure") or {})
    rewrite_priorities = [str(v).strip() for v in list(rewrite_pressure.get("next_run_priorities") or []) if str(v).strip()]
    chapter_focus = _clip_text(str(chapter_blueprint.get("focus", "") or "").strip(), 220)
    chapter_hook_job = _clip_text(str(chapter_blueprint.get("hook_job", "") or "").strip(), 180)
    chapter_visual_motif = _clip_text(str(chapter_blueprint.get("visual_motif", "") or "").strip(), 220)
    chapter_motion_note = _clip_text(str(chapter_blueprint.get("motion_note", "") or "").strip(), 180)
    chapter_improvement = _clip_text(str(chapter_blueprint.get("improvement_focus", "") or "").strip(), 180)
    is_empire_magnates = bool(
        format_preset == "documentary"
        and any(
            token in f"{channel_title} {str(channel_context.get('channel_handle', '') or '').strip()}".lower()
            for token in ("empire magnates", "@empiremagnates", "empiremagnates")
        )
    )
    note_parts = [
        f"Regenerate chapter {int(chapter_index) + 1} from the saved Catalyst blueprint, not the previous visual pattern.",
        blockers[0] if blockers else "",
        next_fixes[0] if next_fixes else "",
        next_fixes[1] if len(next_fixes) > 1 else "",
        rewrite_priorities[0] if rewrite_priorities else "",
        f"Focus this chapter on {chapter_focus}." if chapter_focus else "",
        f"Open on: {chapter_hook_job}" if chapter_hook_job else "",
        f"Visual motif: {chapter_visual_motif}" if chapter_visual_motif else "",
        f"Motion note: {chapter_motion_note}" if chapter_motion_note else "",
        f"Improvement target: {chapter_improvement}" if chapter_improvement else "",
    ]
    if is_empire_magnates:
        note_parts.extend(
            [
                "For Empire Magnates, use premium 3D business-documentary proof frames only: dossier, boardroom, map, archive, network, mechanism, infrastructure, or consequence imagery.",
                "Do not use literal brains, textbook anatomy, sterile lab props, or generic floating-object filler unless the narration explicitly requires a symbolic mind-world proof frame.",
                "Make the opener feel more expensive, more invasive, and more consequence-first than the last run.",
            ]
        )
    return _clip_text(" ".join(part for part in note_parts if part), 1600)


async def _refresh_longform_edit_blueprint_for_session(session: dict) -> dict:
    session_snapshot = dict(session or {})
    metadata_pack = dict(session_snapshot.get("metadata_pack") or {})
    channel_context = dict(metadata_pack.get("youtube_channel") or {})
    format_preset = str(session_snapshot.get("format_preset", "") or "documentary").strip().lower()
    channel_memory = _coerce_documentary_longform_channel_memory(
        channel_context,
        dict(metadata_pack.get("catalyst_channel_memory") or session_snapshot.get("channel_memory") or {}),
        format_preset=format_preset,
        topic=str(session_snapshot.get("topic", "") or ""),
        input_title=str(session_snapshot.get("input_title", "") or ""),
    )
    return await _build_catalyst_edit_blueprint(
        template=str(session_snapshot.get("template", "story") or "story"),
        format_preset=format_preset,
        topic=str(session_snapshot.get("topic", "") or ""),
        input_title=str(session_snapshot.get("input_title", "") or ""),
        input_description=str(session_snapshot.get("input_description", "") or ""),
        chapter_count=max(1, len(list(session_snapshot.get("chapters") or []))),
        chapter_target_sec=float(((list(session_snapshot.get("chapters") or [{}]) or [{}])[0] or {}).get("target_sec", 70) or 70),
        source_bundle=dict(metadata_pack.get("source_video") or {}),
        source_analysis=dict(metadata_pack.get("source_analysis") or {}),
        channel_context=channel_context,
        channel_memory=channel_memory,
        strategy_notes=str(session_snapshot.get("strategy_notes", "") or ""),
        xai_json_completion_fn=_xai_json_completion,
        marketing_doctrine_text_fn=_marketing_doctrine_text,
        render_reference_corpus_context_fn=lambda **kwargs: _render_catalyst_reference_corpus_context(
            reference_memory=_catalyst_reference_memory,
            **kwargs,
        ),
        same_arena_subject_fn=_same_arena_subject,
    )


def _longform_public_session(session: dict) -> dict:
    s = dict(session or {})
    catalyst_preflight = _catalyst_longform_preflight(s)
    metadata_pack = dict(s.get("metadata_pack") or {})
    public_channel_memory = _coerce_documentary_longform_channel_memory(
        dict(metadata_pack.get("youtube_channel") or {}),
        _catalyst_channel_memory_public_view(s.get("channel_memory") or {}),
        format_preset=str(s.get("format_preset", "") or "documentary"),
        topic=str(s.get("topic", "") or ""),
        input_title=str(s.get("input_title", "") or ""),
    )
    chapters = []
    public_character_references = [
        _longform_character_reference_public_view(item)
        for item in list(s.get("character_references") or [])
        if isinstance(item, dict) and str((item or {}).get("character_id", "") or "").strip()
    ]
    character_name_by_id = {
        str(item.get("character_id", "") or "").strip(): str(item.get("name", "") or "").strip()
        for item in public_character_references
        if str(item.get("character_id", "") or "").strip()
    }
    for ch in list(s.get("chapters") or []):
        chapter = dict(ch or {})
        chapter_scenes = []
        for scene_idx, raw_scene in enumerate(list(chapter.get("scenes") or [])):
            scene = dict(raw_scene or {})
            chapter_scenes.append({
                "scene_num": int(scene.get("scene_num", scene_idx + 1) or (scene_idx + 1)),
                "duration_sec": float(scene.get("duration_sec", 5.0) or 5.0),
                "narration": str(scene.get("narration", "") or ""),
                "visual_description": str(scene.get("visual_description", "") or ""),
                "text_overlay": str(scene.get("text_overlay", "") or ""),
                "motion_direction": str(scene.get("motion_direction", "") or ""),
                "sfx_direction": str(scene.get("sfx_direction", "") or ""),
                "engagement_purpose": str(scene.get("engagement_purpose", "") or ""),
                "assigned_character_id": str(scene.get("assigned_character_id", "") or ""),
                "assigned_character_name": str(
                    scene.get("assigned_character_name", "")
                    or character_name_by_id.get(str(scene.get("assigned_character_id", "") or "").strip(), "")
                    or ""
                ),
                "image_url": str(scene.get("image_url", "") or ""),
                "image_status": str(scene.get("image_status", "missing") or "missing"),
                "image_error": str(scene.get("image_error", "") or ""),
                "image_provider": str(scene.get("image_provider", "") or ""),
                "image_provider_label": str(scene.get("image_provider_label", "") or ""),
            })
        chapters.append({
            "index": int(chapter.get("index", 0) or 0),
            "title": str(chapter.get("title", "") or ""),
            "summary": str(chapter.get("summary", "") or ""),
            "target_sec": float(chapter.get("target_sec", 0) or 0),
            "status": str(chapter.get("status", "pending_review") or "pending_review"),
            "retry_count": int(chapter.get("retry_count", 0) or 0),
            "viral_score": int(chapter.get("viral_score", 0) or 0),
            "brand_slot": str(chapter.get("brand_slot", "") or ""),
            "scene_count": len(chapter_scenes),
            "scenes": chapter_scenes,
            "last_error": str(chapter.get("last_error", "") or ""),
        })
    return {
        "session_id": str(s.get("session_id", "") or ""),
        "template": str(s.get("template", "") or ""),
        "format_preset": str(s.get("format_preset", "explainer") or "explainer"),
        "auto_pipeline": bool(s.get("auto_pipeline", False)),
        "topic": str(s.get("topic", "") or ""),
        "input_title": str(s.get("input_title", "") or ""),
        "input_description": str(s.get("input_description", "") or ""),
        "source_url": str(s.get("source_url", "") or ""),
        "youtube_channel_id": str(s.get("youtube_channel_id", "") or ""),
        "analytics_notes": str(s.get("analytics_notes", "") or ""),
        "strategy_notes": str(s.get("strategy_notes", "") or ""),
        "target_minutes": float(s.get("target_minutes", 0) or 0),
        "language": str(s.get("language", "en") or "en"),
        "resolution": str(s.get("resolution", "720p_landscape") or "720p_landscape"),
        "animation_enabled": bool(s.get("animation_enabled", True)),
        "sfx_enabled": bool(s.get("sfx_enabled", True)),
        "whisper_mode": str(s.get("whisper_mode", "subtle") or "subtle"),
        "status": str(s.get("status", "draft_review") or "draft_review"),
        "job_id": str(s.get("job_id", "") or ""),
        "paused_error": s.get("paused_error", None),
        "has_reference_image": bool(
            str(s.get("reference_image_public_url", "") or "").strip()
            or str(s.get("reference_image_url", "") or "").strip()
        ),
        "reference_image_uploaded": bool(s.get("reference_image_uploaded", False)),
        "reference_image_public_url": str(s.get("reference_image_public_url", "") or ""),
        "reference_lock_mode": str(s.get("reference_lock_mode", "strict") or "strict"),
        "character_references": public_character_references,
        "metadata_pack": dict(s.get("metadata_pack") or {}),
        "edit_blueprint": dict(s.get("edit_blueprint") or {}),
        "learning_record": dict(s.get("learning_record") or {}),
        "latest_outcome": dict(s.get("latest_outcome") or {}),
        "channel_memory": public_channel_memory,
        "catalyst_preflight": catalyst_preflight,
        "chapters": chapters,
        "review_state": _longform_review_state(s),
        "draft_progress": dict(s.get("draft_progress") or {}),
        "created_at": float(s.get("created_at", 0) or 0),
        "updated_at": float(s.get("updated_at", 0) or 0),
        "package": dict(s.get("package") or {}),
    }


def _longform_session_summary(session: dict) -> dict:
    s = dict(session or {})
    chapters = list(s.get("chapters") or [])
    preview_image_url = ""
    for chapter in reversed(chapters):
        chapter_scenes = list((chapter or {}).get("scenes") or [])
        for scene in chapter_scenes:
            candidate = str((scene or {}).get("image_url", "") or "").strip()
            if candidate:
                preview_image_url = candidate
                break
        if preview_image_url:
            break
    package = dict(s.get("package") or {})
    output_file = str(package.get("output_file", "") or "")
    return {
        "session_id": str(s.get("session_id", "") or ""),
        "template": str(s.get("template", "") or ""),
        "format_preset": str(s.get("format_preset", "explainer") or "explainer"),
        "auto_pipeline": bool(s.get("auto_pipeline", False)),
        "topic": str(s.get("topic", "") or ""),
        "input_title": str(s.get("input_title", "") or ""),
        "source_url": str(s.get("source_url", "") or ""),
        "youtube_channel_id": str(s.get("youtube_channel_id", "") or ""),
        "target_minutes": float(s.get("target_minutes", 0) or 0),
        "language": str(s.get("language", "en") or "en"),
        "resolution": str(s.get("resolution", "720p_landscape") or "720p_landscape"),
        "status": str(s.get("status", "draft_review") or "draft_review"),
        "job_id": str(s.get("job_id", "") or ""),
        "review_state": _longform_review_state(s),
        "draft_progress": dict(s.get("draft_progress") or {}),
        "paused_error": s.get("paused_error", None),
        "preview_image_url": preview_image_url,
        "output_file": output_file,
        "created_at": float(s.get("created_at", 0) or 0),
        "updated_at": float(s.get("updated_at", 0) or 0),
    }


def _longform_session_requires_source_analysis(session: dict) -> bool:
    s = dict(session or {})
    metadata_pack = dict(s.get("metadata_pack") or {})
    return bool(
        str(s.get("source_url", "") or "").strip()
        or str(s.get("youtube_channel_id", "") or "").strip()
        or str(s.get("analytics_notes", "") or "").strip()
        or str(s.get("transcript_text", "") or "").strip()
        or int(metadata_pack.get("analytics_asset_count", 0) or 0) > 0
    )


def _longform_session_analysis_complete(session: dict) -> bool:
    s = dict(session or {})
    if not _longform_session_requires_source_analysis(s):
        return True
    metadata_pack = dict(s.get("metadata_pack") or {})
    source_analysis = dict(metadata_pack.get("source_analysis") or {})
    analytics_summary = str(metadata_pack.get("analytics_evidence_summary", "") or "").strip()
    has_primary_analysis = bool(
        str(source_analysis.get("what_worked", "") or "").strip()
        or str(source_analysis.get("what_hurt", "") or "").strip()
        or [str(v).strip() for v in list(source_analysis.get("strongest_signals") or []) if str(v).strip()]
        or [str(v).strip() for v in list(source_analysis.get("weak_points") or []) if str(v).strip()]
        or [str(v).strip() for v in list(source_analysis.get("retention_findings") or []) if str(v).strip()]
        or [str(v).strip() for v in list(source_analysis.get("packaging_findings") or []) if str(v).strip()]
        or [str(v).strip() for v in list(source_analysis.get("improvement_moves") or []) if str(v).strip()]
    )
    return bool(has_primary_analysis or analytics_summary)


def _longform_preview_filename(session_id: str, chapter_index: int, scene_index: int) -> str:
    safe_sid = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(session_id or "").strip())[:64] or "lf"
    return f"{safe_sid}_c{int(chapter_index) + 1:02d}_s{int(scene_index) + 1:03d}.png"


def _longform_preview_url(filename: str) -> str:
    return f"/api/longform/preview/{filename}"


CATALYST_REFERENCE_FRAMES_DIR = Path(__file__).resolve().parent / "analysis" / "reference_frames"
FERN_REFERENCE_SHEET = CATALYST_REFERENCE_FRAMES_DIR / "fern_sheet.jpg"
FERN_EXACT_PRESIDENT_REFERENCE_SHEET = CATALYST_REFERENCE_FRAMES_DIR / "fern_exact_president_sheet.jpg"
EMPIRE_FERN_CURATED_REFERENCE_SHEET = CATALYST_REFERENCE_FRAMES_DIR / "empire_fern_magnates_curated_test.jpg"
EMPIRE_PSYCHOLOGY_REFERENCE_SHEET = CATALYST_REFERENCE_FRAMES_DIR / "empire_psychology_reference_sheet_v2.jpg"
EMPIRE_SYSTEMS_REFERENCE_SHEET = CATALYST_REFERENCE_FRAMES_DIR / "empire_systems_reference_sheet_v2.jpg"
CRYPTIC_SCIENCE_CASE_REFERENCE_SHEET = CATALYST_REFERENCE_FRAMES_DIR / "cryptic_science_case_sheet.jpg"
_LONGFORM_REFERENCE_IMAGE_CACHE: dict[str, str] = {}


def _longform_documentary_reference_sheet_path(
    *,
    template: str,
    format_preset: str,
    channel_context: dict | None = None,
    edit_blueprint: dict | None = None,
    chapter_blueprint: dict | None = None,
    topic: str = "",
    input_title: str = "",
    narration: str = "",
    visual_description: str = "",
) -> Path | None:
    if not _longform_prefers_3d_documentary_visuals(template, format_preset):
        return None
    empire_channel = _is_empire_magnates_channel(channel_context)
    cryptic_science_channel = _is_cryptic_science_channel(channel_context)
    if not empire_channel and not cryptic_science_channel:
        return None
    documentary_archetype = _longform_documentary_archetype(
        edit_blueprint=edit_blueprint,
        chapter_blueprint=chapter_blueprint,
        format_preset=format_preset,
        topic=topic,
        input_title=input_title,
        narration=narration,
        visual_description=visual_description,
    )
    if documentary_archetype == "psychology_documentary":
        if empire_channel and FERN_EXACT_PRESIDENT_REFERENCE_SHEET.exists():
            return FERN_EXACT_PRESIDENT_REFERENCE_SHEET
        if FERN_REFERENCE_SHEET.exists():
            return FERN_REFERENCE_SHEET
    if cryptic_science_channel:
        if documentary_archetype == "crime_documentary" and CRYPTIC_SCIENCE_CASE_REFERENCE_SHEET.exists():
            return CRYPTIC_SCIENCE_CASE_REFERENCE_SHEET
        if FERN_REFERENCE_SHEET.exists():
            return FERN_REFERENCE_SHEET
        return None
    if EMPIRE_FERN_CURATED_REFERENCE_SHEET.exists():
        return EMPIRE_FERN_CURATED_REFERENCE_SHEET
    candidate = (
        EMPIRE_PSYCHOLOGY_REFERENCE_SHEET
        if documentary_archetype == "psychology_documentary"
        else EMPIRE_SYSTEMS_REFERENCE_SHEET
    )
    return candidate if candidate.exists() else None


def _longform_documentary_reference_image_url(
    *,
    template: str,
    format_preset: str,
    channel_context: dict | None = None,
    edit_blueprint: dict | None = None,
    chapter_blueprint: dict | None = None,
    topic: str = "",
    input_title: str = "",
    narration: str = "",
    visual_description: str = "",
) -> str:
    reference_path = _longform_documentary_reference_sheet_path(
        template=template,
        format_preset=format_preset,
        channel_context=channel_context,
        edit_blueprint=edit_blueprint,
        chapter_blueprint=chapter_blueprint,
        topic=topic,
        input_title=input_title,
        narration=narration,
        visual_description=visual_description,
    )
    if not reference_path:
        return ""
    cache_key = str(reference_path.resolve())
    cached = _LONGFORM_REFERENCE_IMAGE_CACHE.get(cache_key, "")
    if cached:
        return cached
    encoded = _file_to_data_image_url(str(reference_path))
    if encoded:
        _LONGFORM_REFERENCE_IMAGE_CACHE[cache_key] = encoded
    return encoded


def _longform_session_subject_reference_image_url(session: dict | None, template: str = "") -> str:
    s = dict(session or {})
    current = str(s.get("reference_image_public_url", "") or s.get("reference_image_url", "") or "").strip()
    if current and _bool_from_any(s.get("reference_image_uploaded"), False):
        return current
    return ""


def _longform_scene_reference_bundle(session: dict | None, scene: dict | None, template: str = "") -> dict:
    session_payload = dict(session or {})
    scene_payload = dict(scene or {})
    assigned_reference = _longform_scene_assigned_character_reference(session_payload, scene_payload)
    if assigned_reference:
        return {
            "reference_image_url": str(assigned_reference.get("reference_image_public_url", "") or "").strip(),
            "reference_lock_mode": _normalize_reference_lock_mode(
                assigned_reference.get("reference_lock_mode"),
                default=_normalize_reference_lock_mode(session_payload.get("reference_lock_mode"), default="strict"),
            ),
            "reference_name": str(assigned_reference.get("name", "") or "").strip(),
            "has_subject_reference": True,
        }
    session_reference_image_url = _longform_session_subject_reference_image_url(session_payload, template)
    return {
        "reference_image_url": session_reference_image_url,
        "reference_lock_mode": _normalize_reference_lock_mode(session_payload.get("reference_lock_mode"), default="strict"),
        "reference_name": "",
        "has_subject_reference": bool(session_reference_image_url),
    }


def _channel_context_haystack(channel_context: dict | None) -> str:
    channel_context = dict(channel_context or {})
    parts: list[str] = []
    for key in (
        "title",
        "channel_title",
        "name",
        "custom_url",
        "channel_handle",
        "handle",
        "channel_url",
        "url",
        "id",
        "channel_id",
        "summary",
        "channel_summary",
        "workspace_focus",
        "workspace",
    ):
        value = channel_context.get(key)
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            parts.extend(str(item or "").strip() for item in value if str(item or "").strip())
            continue
        parts.append(str(value or "").strip())
    return " ".join(parts).lower()


def _is_empire_magnates_channel(channel_context: dict | None) -> bool:
    haystack = _channel_context_haystack(channel_context)
    return any(token in haystack for token in ("empire magnates", "@empiremagnates", "empiremagnates"))


def _is_cryptic_science_channel(channel_context: dict | None) -> bool:
    haystack = _channel_context_haystack(channel_context)
    return any(token in haystack for token in ("cryptic science", "crypticscience", "@crypticscience"))


def _is_history_rewind_channel(channel_context: dict | None) -> bool:
    haystack = _channel_context_haystack(channel_context)
    return any(
        token in haystack
        for token in (
            "history rewind",
            "historyrewind",
            "@historyrewind",
            "@historyyyrewindddd",
            "historyyyrewindddd",
        )
    )


def _is_nyptid_clips_channel(channel_context: dict | None) -> bool:
    haystack = _channel_context_haystack(channel_context)
    return any(token in haystack for token in ("nyptid clips", "nyptidclips", "@nyptidclips"))


def _channel_prefers_fal_scene_generation(channel_context: dict | None) -> bool:
    return _is_empire_magnates_channel(channel_context) or _is_cryptic_science_channel(channel_context)


def _channel_blocks_fal_scene_generation(channel_context: dict | None) -> bool:
    return _is_history_rewind_channel(channel_context) or _is_nyptid_clips_channel(channel_context)


def _coerce_empire_longform_channel_memory(
    channel_context: dict | None,
    channel_memory: dict | None,
    *,
    format_preset: str = "",
) -> dict:
    memory = dict(channel_memory or {})
    if str(format_preset or "").strip().lower() != "documentary" or not _is_empire_magnates_channel(channel_context):
        return memory
    updated = dict(memory)
    psychology_haystack = " ".join([
        str((channel_context or {}).get("summary", "") or "").strip().lower(),
        str((channel_context or {}).get("channel_summary", "") or "").strip().lower(),
        str(updated.get("niche_key", "") or "").strip().lower(),
        str(updated.get("niche_label", "") or "").strip().lower(),
        str(updated.get("archetype_key", "") or "").strip().lower(),
        str(updated.get("archetype_label", "") or "").strip().lower(),
        str(updated.get("selected_cluster_label", "") or "").strip().lower(),
        " ".join(str(v).strip().lower() for v in list(updated.get("operator_target_niches") or []) if str(v).strip()),
    ])
    empire_psychology_mode = bool(re.search(
        r"\b(psychology|psychological|brain|mind|behavior|behaviour|manipulat|bias|blind spot|subconscious|attention|dopamine|emotion|thought|memory|control|choice|choices|decision|decisions|influence|hidden behavior)\b",
        psychology_haystack,
    ))
    updated["niche_key"] = "psychology_documentary" if empire_psychology_mode else "business_documentary"
    updated["niche_label"] = "Psychology / Hidden Behavior" if empire_psychology_mode else "Business Documentary"
    updated["archetype_key"] = "psychology_documentary" if empire_psychology_mode else "systems_documentary"
    updated["archetype_label"] = "Psychology Documentary" if empire_psychology_mode else "Systems Documentary"
    updated["archetype_hook_rule"] = _clip_text(
        str(updated.get("archetype_hook_rule", "") or "").strip()
        or (
            "Open on an invasive contradiction, hidden behavior trigger, or personal consequence before any explanation."
            if empire_psychology_mode
            else "Open on a consequence, contradiction, or hidden control point before any explanation."
        ),
        220,
    )
    updated["archetype_pace_rule"] = _clip_text(
        "Use premium proof-first pacing: claim, proof, trigger, consequence, payoff. No generic setup drift."
        if empire_psychology_mode
        else "Use premium proof-first pacing: claim, proof, system, consequence, payoff. No generic setup drift.",
        220,
    )
    updated["archetype_visual_rule"] = _clip_text(
        "Use premium 3D symbolic mind-worlds, surveillance, social manipulation tableaux, mirror or split-self reversals, dossiers, emotional consequence scenes, and hidden-control environments. Avoid literal exposed brains, textbook anatomy, sterile labs, floating gears, and generic machine filler."
        if empire_psychology_mode
        else "Use premium 3D boardroom, dossier, archive, map, network, infrastructure, mechanism, and consequence frames. Avoid literal brains, anatomy, sterile labs, and floating-object filler.",
        220,
    )
    updated["archetype_sound_rule"] = _clip_text(
        "Use expensive documentary tension, invasive low-end pulses, restrained silence pockets, and precise reveal accents that feel personal instead of horror camp."
        if empire_psychology_mode
        else "Use expensive documentary tension, controlled low-end pulses, and silence pockets before reveals instead of horror-heavy texture.",
        220,
    )
    updated["archetype_packaging_rule"] = _clip_text(
        "Package around one invasive contradiction, one hidden behavior mechanism, and one premium consequence image instead of generic clickbait or textbook psychology."
        if empire_psychology_mode
        else "Package around one contradiction, one hidden system, and one premium proof image instead of generic psychology clickbait.",
        220,
    )
    updated["summary"] = _clip_text(
        str(updated.get("summary", "") or "").strip()
        or (
            "Empire Magnates should run as premium 3D psychology documentary: hidden behavior, emotional consequence, and premium proof frames instead of generic brains or machines."
            if empire_psychology_mode
            else "Empire Magnates should run as premium 3D systems documentary, not literal dark-psychology filler."
        ),
        320,
    )
    existing_guardrails = [str(v).strip() for v in list(updated.get("operator_guardrails") or []) if str(v).strip()]
    updated["operator_guardrails"] = _dedupe_preserve_order(
        [
            *existing_guardrails,
            "No literal brains or textbook anatomy unless the beat explicitly demands a symbolic mind-world proof frame",
            "No sterile lab filler",
            "No generic floating-object hero shots",
            "No generic gears, machine filler, or stock systems widgets unless the beat explicitly demands them",
            "Keep visuals obviously premium CG and documentary-grade",
        ],
        max_items=10,
        max_chars=180,
    )
    existing_niches = [str(v).strip() for v in list(updated.get("operator_target_niches") or []) if str(v).strip()]
    updated["operator_target_niches"] = _dedupe_preserve_order(
        [
            *existing_niches,
            "psychology",
            "hidden behavior",
            "attention and manipulation",
            "emotional control",
            "business documentaries",
            "wealth systems",
            "hidden power structures",
            "economic manipulation",
        ],
        max_items=8,
        max_chars=80,
    )
    updated["rewrite_pressure"] = _catalyst_rewrite_pressure_profile(updated)
    return updated


def _coerce_cryptic_longform_channel_memory(
    channel_context: dict | None,
    channel_memory: dict | None,
    *,
    format_preset: str = "",
    topic: str = "",
    input_title: str = "",
) -> dict:
    memory = dict(channel_memory or {})
    if str(format_preset or "").strip().lower() != "documentary" or not _is_cryptic_science_channel(channel_context):
        return memory
    haystack = " ".join([
        str(topic or "").strip().lower(),
        str(input_title or "").strip().lower(),
        str((channel_context or {}).get("summary", "") or "").strip().lower(),
        str((channel_context or {}).get("channel_summary", "") or "").strip().lower(),
        str(memory.get("niche_key", "") or "").strip().lower(),
        str(memory.get("niche_label", "") or "").strip().lower(),
        str(memory.get("archetype_key", "") or "").strip().lower(),
        str(memory.get("archetype_label", "") or "").strip().lower(),
    ])
    case_mode = bool(re.search(
        r"\b(true crime|crime|criminal|case|court|sentenc|trial|charges?|charged|complaint|indict|kidnapp|robbery|victim|defendant|prosecutor|federal|evidence|surveillance|forensic|dna|detective|arrest|warrant|plea|home confinement|monitoring|records|timeline|case file|studio ambush)\b",
        haystack,
    ))
    if not case_mode:
        return memory
    updated = dict(memory)
    updated["niche_key"] = "crime_documentary"
    updated["niche_label"] = "Crime / Case Breakdown"
    updated["archetype_key"] = "crime_documentary"
    updated["archetype_label"] = "Crime Documentary"
    updated["archetype_hook_rule"] = _clip_text(
        "Open on the contradiction, allegation, or evidence cue that instantly makes the case feel bigger than a headline.",
        220,
    )
    updated["archetype_pace_rule"] = _clip_text(
        "Use proof-first case pacing: contradiction, allegation, proof trail, consequence, unresolved pressure. No setup drift.",
        220,
    )
    updated["archetype_visual_rule"] = _clip_text(
        "Use premium 3D case-board, court, studio, route-map, phone, records, surveillance, and consequence frames. Prioritize photoreal named-human close-ups, over-shoulder evidence handling, and pressure scenes whenever real people are central to the beat. Keep the recurring skeleton as editorial connective tissue only; keep named humans human when the beat is about evidence or pressure. Avoid generic boardrooms, untouched dossier tables, skeleton crowds, mannequin faces, and anatomy filler.",
        220,
    )
    updated["archetype_sound_rule"] = _clip_text(
        "Use expensive investigative tension, controlled low-end pulses, document-hit accents, and silence pockets before reveals instead of horror camp or generic trailer sludge.",
        220,
    )
    updated["archetype_packaging_rule"] = _clip_text(
        "Package around one contradiction, one evidence trail, and one consequence image instead of vague gossip or abstract systems language.",
        220,
    )
    updated["summary"] = _clip_text(
        "Cryptic Science should run this topic as a premium 3D case-breakdown documentary: evidence-led, contradiction-first, and human-scale, not a generic science explainer or corporate boardroom scene.",
        320,
    )
    existing_guardrails = [str(v).strip() for v in list(updated.get("operator_guardrails") or []) if str(v).strip()]
    updated["operator_guardrails"] = _dedupe_preserve_order(
        [
            *existing_guardrails,
            "No generic boardroom filler",
            "No multi-skeleton meeting rooms or x-ray humans",
            "No untouched dossier-table still life without case context",
            "No science-mechanism drift, anatomy filler, or floating machine props",
            "Keep visuals obviously premium CG and evidence-led",
        ],
        max_items=10,
        max_chars=180,
    )
    existing_niches = [str(v).strip() for v in list(updated.get("operator_target_niches") or []) if str(v).strip()]
    updated["operator_target_niches"] = _dedupe_preserve_order(
        [
            *existing_niches,
            "true crime",
            "court breakdowns",
            "case evidence",
            "public contradictions",
            "documentary explainers",
        ],
        max_items=8,
        max_chars=80,
    )
    updated["rewrite_pressure"] = _catalyst_rewrite_pressure_profile(updated)
    return updated


def _coerce_documentary_longform_channel_memory(
    channel_context: dict | None,
    channel_memory: dict | None,
    *,
    format_preset: str = "",
    topic: str = "",
    input_title: str = "",
) -> dict:
    updated = _coerce_empire_longform_channel_memory(
        channel_context,
        channel_memory,
        format_preset=format_preset,
    )
    return _coerce_cryptic_longform_channel_memory(
        channel_context,
        updated,
        format_preset=format_preset,
        topic=topic,
        input_title=input_title,
    )


def _longform_hosted_image_model_candidates(
    template: str,
    format_preset: str = "",
    *,
    reference_image_url: str = "",
) -> list[str]:
    if _longform_prefers_3d_documentary_visuals(template, format_preset):
        # Empire / long-form documentary scenes are locked to Grok.
        # Seedream stays thumbnail-only and Imagen/Recraft do not replace the
        # Fern-conditioned documentary scene lane.
        candidates = ["grok_imagine"]
    else:
        candidates = ["grok_imagine"]
    deduped: list[str] = []
    for candidate in candidates:
        normalized = _normalize_creative_image_model_id(candidate, template=template)
        if normalized and normalized not in deduped:
            deduped.append(normalized)
    return deduped or ["grok_imagine"]


async def _longform_generate_scene_image(
    prompt: str,
    output_path: str,
    resolution: str,
    negative_prompt: str,
    template: str,
    format_preset: str = "",
    channel_context: dict | None = None,
    reference_image_url: str = "",
    reference_lock_mode: str = "strict",
    best_of_enabled: bool = False,
    salvage_enabled: bool = False,
) -> dict:
    errors: list[str] = []
    documentary_passthrough = _longform_prefers_3d_documentary_visuals(template, format_preset)
    if documentary_passthrough:
        grok_profile = dict(CREATIVE_IMAGE_MODEL_MAP.get("grok_imagine") or {})
        if not bool(grok_profile.get("enabled", False)):
            raise RuntimeError("Long-form documentary scenes require Grok Imagine, but the Grok lane is not enabled.")
        try:
            return await generate_scene_image(
                prompt,
                output_path,
                resolution=resolution,
                negative_prompt=negative_prompt,
                template=template,
                channel_context=channel_context,
                reference_image_url=reference_image_url,
                reference_lock_mode=reference_lock_mode,
                best_of_enabled=best_of_enabled,
                salvage_enabled=salvage_enabled,
                prompt_passthrough=True,
                selected_model_id="grok_imagine",
            )
        except Exception as e:
            raise RuntimeError(f"Long-form documentary scene generation requires Grok Imagine and it failed: {e}")
    for model_id in _longform_hosted_image_model_candidates(
        template,
        format_preset,
        reference_image_url=reference_image_url,
    ):
        for attempt in range(2):
            try:
                return await generate_scene_image(
                    prompt,
                    output_path,
                    resolution=resolution,
                    negative_prompt=negative_prompt,
                    template=template,
                    channel_context=channel_context,
                    reference_image_url=reference_image_url,
                    reference_lock_mode=reference_lock_mode,
                    best_of_enabled=best_of_enabled,
                    salvage_enabled=salvage_enabled,
                    prompt_passthrough=documentary_passthrough,
                    selected_model_id=model_id,
                )
            except Exception as e:
                errors.append(f"{model_id} attempt {attempt + 1}: {e}")
                if attempt == 0:
                    await asyncio.sleep(1.5)
    detail = " | ".join(errors[-2:]) if errors else "no hosted image models were available"
    raise RuntimeError(f"Long-form hosted image generation failed: {detail}")


def _longform_thumbnail_model_candidates(format_preset: str = "", channel_context: dict | None = None) -> list[str]:
    if _is_empire_magnates_channel(channel_context):
        ordered = ["seedream45", "imagen4_fast", "recraft_v4_pro", "recraft_v4", "imagen4_ultra", "grok_imagine"]
    elif str(format_preset or "").strip().lower() == "documentary":
        ordered = ["seedream45", "imagen4_fast", "recraft_v4", "imagen4_ultra", "grok_imagine"]
    else:
        ordered = ["seedream45", "imagen4_fast", "recraft_v4", "imagen4_ultra", "grok_imagine"]
    deduped: list[str] = []
    for candidate in ordered:
        normalized = _normalize_creative_image_model_id(candidate)
        if normalized and normalized not in deduped:
            deduped.append(normalized)
    return deduped


async def _generate_longform_package_thumbnail(
    *,
    prompt: str,
    selected_title: str,
    output_path: str,
    format_preset: str = "",
    channel_context: dict | None = None,
) -> dict:
    base_prompt = _clip_text(
        f"{str(prompt or '').strip()} Packaging hook: {str(selected_title or '').strip()}. "
        "Create a premium faceless YouTube thumbnail in 16:9. One dominant subject, one dominant contradiction, "
        "premium 3D documentary lighting, strong contrast, instantly readable at phone size, no text baked into the image, "
        "no collage clutter, no anatomy filler, no generic floating object.",
        1200,
    )
    negative_prompt = (
        f"{NEGATIVE_PROMPT}. Avoid tiny unreadable details, multi-panel collage layouts, repeated subjects, "
        "sterile medical/lab imagery, and weak low-contrast staging."
    )
    errors: list[str] = []
    for model_id in _longform_thumbnail_model_candidates(format_preset, channel_context):
        try:
            result = await _generate_image_fal_selected_model(
                model_id,
                base_prompt,
                output_path,
                resolution="1080p_landscape",
                negative_prompt=negative_prompt,
            )
            await _enforce_thumbnail_1080(output_path)
            return {
                **dict(result or {}),
                "provider": str(result.get("provider", model_id) or model_id),
                "provider_label": str(result.get("provider_label", model_id) or model_id),
            }
        except Exception as e:
            errors.append(f"{model_id}: {e}")
    if PIKZELS_API_KEY:
        pikzels_result = await _generate_thumbnail_image(
            prompt=base_prompt,
            negative_prompt=negative_prompt,
            output_path=output_path,
            user=None,
            mode="describe",
        )
        await _enforce_thumbnail_1080(output_path)
        return {
            "local_path": output_path,
            "provider": "pikzels",
            "provider_label": "Pikzels",
            "request_id": str(pikzels_result.get("request_id", "") or ""),
        }
    detail = " | ".join(errors[-3:]) if errors else "no long-form thumbnail model was available"
    raise RuntimeError(f"Long-form package thumbnail generation failed: {detail}")


async def _longform_attach_scene_previews(
    session_id: str,
    template: str,
    chapter: dict,
    resolution: str = "720p_landscape",
    format_preset: str = "",
    edit_blueprint: dict | None = None,
    chapter_blueprint: dict | None = None,
) -> dict:
    out = dict(chapter or {})
    chapter_index = int(out.get("index", 0) or 0)
    scenes = _normalize_longform_scenes_for_render(list(out.get("scenes") or []))
    scenes = _scale_scene_durations_to_target(scenes, out.get("target_sec", 0))
    if not scenes:
        out["scenes"] = []
        out["target_sec"] = 0.0
        return out

    session_topic = ""
    session_input_title = ""
    channel_context: dict = {}
    session_reference_image_url = ""
    live_session: dict = {}
    try:
        async with _longform_sessions_lock:
            live_session = dict(_longform_sessions.get(session_id) or {})
        session_topic = str(live_session.get("topic", "") or "").strip()
        session_input_title = str(live_session.get("input_title", "") or "").strip()
        channel_context = dict((dict(live_session.get("metadata_pack") or {})).get("youtube_channel") or {})
        session_reference_image_url = _longform_session_subject_reference_image_url(live_session, template)
    except Exception:
        pass

    neg_prompt = TEMPLATE_NEGATIVE_PROMPTS.get(template, NEGATIVE_PROMPT)
    if _longform_prefers_3d_documentary_visuals(template, format_preset):
        neg_prompt = (
            neg_prompt
            + ", live-action photography, candid human photo, gritty warehouse realism, street-photo realism, film still, "
            + "photographic actor portrait, handheld live-action frame, generic medical textbook render, repetitive lab stage, wrong organ substitution, readable wall text, giant title words, fake UI typography, pseudo text, lower-third captions, anonymous stock-person face, mannequin face, waxy skin, deformed hands, extra fingers, deformed eyes"
        )
        if _is_empire_magnates_channel(channel_context):
            neg_prompt = (
                neg_prompt
                + ", isolated floating object, pedestal product shot, centered spotlight object, hero object stage, macro machine cutaway, literal exposed brain, anatomy cross-section, glowing gear sphere, pseudo text, title words in frame"
            )
        elif _is_cryptic_science_channel(channel_context):
            neg_prompt = (
                neg_prompt
                + ", generic corporate boardroom, empty archive room, untouched dossier table, skeleton committee, multiple skeleton people, x-ray humans, static meeting room filler, anatomy filler, floating machine props, pseudo text, title words in frame"
            )
    skeleton_anchor = _canonical_skeleton_anchor() if template == "skeleton" else ""
    # Previews should be cheap and fast. Avoid strict conditioning that can force costly retries.
    reference_image_url = ""
    preview_resolution = "720p_landscape" if str(resolution or "").endswith("_landscape") else "720p"
    LONGFORM_PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
    now_cache_bust = int(time.time())
    chapter_tone = str(out.get("tone", "") or "").strip().lower()
    if chapter_tone not in {"horror", "neutral"}:
        derived_narration = " ".join(str((s or {}).get("narration", "") or "") for s in scenes)
        chapter_tone = _longform_detect_tone(
            template,
            str(out.get("title", "") or ""),
            str(out.get("summary", "") or ""),
            derived_narration,
        )
    out["tone"] = chapter_tone
    if _longform_is_horror_tone(chapter_tone):
        neg_prompt = (
            neg_prompt
            + ", bright cheerful lighting, warm happy tone, colorful festival mood, comedy framing, daylight beach party aesthetics"
        )
    scenes = _repair_longform_generated_scenes(
        scenes,
        template=template,
        format_preset=format_preset,
        topic=session_topic,
        input_title=session_input_title,
        edit_blueprint=edit_blueprint,
        chapter_blueprint=chapter_blueprint,
        allow_narration_rewrite=False,
    )
    existing_scene_assignments: dict[int, dict] = {}
    try:
        async with _longform_sessions_lock:
            current_session = dict(_longform_sessions.get(session_id) or {})
        live_chapters = list(current_session.get("chapters") or [])
        if 0 <= chapter_index < len(live_chapters):
            for raw_scene in list((dict(live_chapters[chapter_index] or {})).get("scenes") or []):
                live_scene = dict(raw_scene or {})
                scene_num = int(live_scene.get("scene_num", 0) or 0)
                if scene_num <= 0:
                    continue
                existing_scene_assignments[scene_num] = {
                    "assigned_character_id": str(live_scene.get("assigned_character_id", "") or "").strip(),
                    "assigned_character_ids": [
                        str(item or "").strip()
                        for item in list(live_scene.get("assigned_character_ids") or [])
                        if str(item or "").strip()
                    ],
                    "assigned_character_name": str(live_scene.get("assigned_character_name", "") or "").strip(),
                }
    except Exception:
        existing_scene_assignments = {}

    scripted_scenes: list[dict] = []
    for scene_idx, raw_scene in enumerate(scenes):
        scene = dict(raw_scene or {})
        scene["scene_num"] = int(scene.get("scene_num", scene_idx + 1) or (scene_idx + 1))
        scene["duration_sec"] = float(raw_scene.get("duration_sec", 5.0) or 5.0)
        scene["visual_description"] = _longform_tone_locked_visual_description(
            str(scene.get("visual_description", "") or ""),
            tone=chapter_tone,
            template=template,
            format_preset=format_preset,
        )
        scene["image_url"] = ""
        scene["image_status"] = "pending_script_lock"
        scene["image_error"] = ""
        persisted_assignment = dict(existing_scene_assignments.get(int(scene.get("scene_num", 0) or 0)) or {})
        if persisted_assignment:
            scene["assigned_character_id"] = str(persisted_assignment.get("assigned_character_id", "") or "").strip()
            scene["assigned_character_ids"] = [
                str(item or "").strip()
                for item in list(persisted_assignment.get("assigned_character_ids") or [])
                if str(item or "").strip()
            ]
            scene["assigned_character_name"] = str(persisted_assignment.get("assigned_character_name", "") or "").strip()
        scripted_scenes.append(scene)

    out["scenes"] = scripted_scenes
    out["target_sec"] = round(sum(float((scene or {}).get("duration_sec", 5.0) or 5.0) for scene in scripted_scenes), 2)

    # Persist all scripted scenes first so owner can review story flow before images finish.
    try:
        async with _longform_sessions_lock:
            live = _longform_sessions.get(session_id)
            if isinstance(live, dict):
                chapters_live = list(live.get("chapters") or [])
                if 0 <= chapter_index < len(chapters_live):
                    chapter_live = dict(chapters_live[chapter_index] or {})
                    chapter_live["scenes"] = [dict(s) for s in scripted_scenes]
                    chapter_live["target_sec"] = out["target_sec"]
                    chapter_live["status"] = "draft_generating_images"
                    chapters_live[chapter_index] = chapter_live
                    live["chapters"] = chapters_live
                    progress = dict(live.get("draft_progress") or {})
                    progress["preview_scene_total"] = int(len(scripted_scenes))
                    progress["preview_scene_generated"] = 0
                    live["draft_progress"] = progress
                    live["updated_at"] = time.time()
                    _save_longform_sessions()
    except Exception:
        pass

    for scene_idx, raw_scene in enumerate(scripted_scenes):
        scene = dict(raw_scene or {})
        scene["image_status"] = "generating"
        visual_desc = _longform_tone_locked_visual_description(
            str(scene.get("visual_description", "") or ""),
            tone=chapter_tone,
            template=template,
            format_preset=format_preset,
        )
        scene["visual_description"] = visual_desc
        scene_reference_bundle = _longform_scene_reference_bundle(live_session, scene, template)
        reference_image_url = str(scene_reference_bundle.get("reference_image_url", "") or "").strip() or _longform_documentary_reference_image_url(
            template=template,
            format_preset=format_preset,
            channel_context=channel_context,
            edit_blueprint=edit_blueprint,
            chapter_blueprint=chapter_blueprint,
            topic=session_topic,
            input_title=session_input_title,
            narration=str(scene.get("narration", "") or ""),
            visual_description=visual_desc,
        )
        reference_lock_mode = _normalize_reference_lock_mode(
            scene_reference_bundle.get("reference_lock_mode"),
            default=_normalize_reference_lock_mode(
                (dict(live_session or {})).get("reference_lock_mode"),
                default="strict",
            ),
        )

        prompt = _build_longform_scene_execution_prompt(
            scene=scene,
            template=template,
            format_preset=format_preset,
            topic=session_topic,
            input_title=session_input_title,
            edit_blueprint=edit_blueprint,
            chapter_blueprint=chapter_blueprint,
            scene_index=scene_idx,
            total_scenes=len(scripted_scenes),
            skeleton_anchor=skeleton_anchor,
            art_style=_longform_default_art_style(template, format_preset),
            has_subject_reference=bool(scene_reference_bundle.get("has_subject_reference", False)),
            subject_reference_name=str(scene_reference_bundle.get("reference_name", "") or ""),
        )
        filename = _longform_preview_filename(session_id, chapter_index, scene_idx)
        output_path = str(LONGFORM_PREVIEW_DIR / filename)
        try:
            img_result = await _longform_generate_scene_image(
                prompt,
                output_path,
                resolution=preview_resolution,
                negative_prompt=neg_prompt,
                template=template,
                format_preset=format_preset,
                channel_context=channel_context,
                reference_image_url=reference_image_url,
                reference_lock_mode=reference_lock_mode,
                best_of_enabled=False,
                salvage_enabled=False,
            )
            scene["image_provider"] = str((img_result or {}).get("provider", "") or "")
            scene["image_provider_label"] = str((img_result or {}).get("provider_label", "") or "")
            if Path(output_path).exists():
                scene["image_url"] = f"{_longform_preview_url(filename)}?v={now_cache_bust}"
                scene["image_status"] = "ready"
                scene["image_error"] = ""
            else:
                cdn_url = str((img_result or {}).get("cdn_url", "") or "")
                if cdn_url:
                    scene["image_url"] = cdn_url
                    scene["image_status"] = "ready"
                    scene["image_error"] = ""
                else:
                    scene["image_status"] = "error"
                    scene["image_error"] = "Preview image was not generated"
        except Exception as e:
            scene["image_status"] = "error"
            scene["image_error"] = str(e)[:220]

        scripted_scenes[scene_idx] = scene
        # Stream image progress while preserving full scripted scene list.
        try:
            async with _longform_sessions_lock:
                live = _longform_sessions.get(session_id)
                if isinstance(live, dict):
                    chapters_live = list(live.get("chapters") or [])
                    if 0 <= chapter_index < len(chapters_live):
                        chapter_live = dict(chapters_live[chapter_index] or {})
                        chapter_live["scenes"] = [dict(s) for s in scripted_scenes]
                        chapter_live["target_sec"] = out["target_sec"]
                        chapter_live["status"] = "draft_generating_images"
                        chapters_live[chapter_index] = chapter_live
                        live["chapters"] = chapters_live
                        progress = dict(live.get("draft_progress") or {})
                        progress["preview_scene_total"] = int(len(scripted_scenes))
                        progress["preview_scene_generated"] = int(scene_idx + 1)
                        live["draft_progress"] = progress
                        live["updated_at"] = time.time()
                        _save_longform_sessions()
        except Exception:
            pass

    out["scenes"] = scripted_scenes
    out["target_sec"] = round(sum(float((scene or {}).get("duration_sec", 5.0) or 5.0) for scene in scripted_scenes), 2)
    return out


def _longform_generated_chapter_count(chapters: list[dict]) -> int:
    total = 0
    for chapter in list(chapters or []):
        if list((chapter or {}).get("scenes") or []):
            total += 1
    return total


def _longform_approved_chapter_count(chapters: list[dict]) -> int:
    return sum(1 for chapter in list(chapters or []) if str((chapter or {}).get("status", "") or "") == "approved")


async def _generate_longform_chapter_for_session(session_id: str, chapter_index: int) -> None:
    chapter_count = 0
    should_continue = False
    async with _longform_draft_semaphore:
        try:
            async with _longform_sessions_lock:
                session = dict(_longform_sessions.get(session_id) or {})
            if not session:
                return
            if _bool_from_any(session.get("stop_requested"), False) or str(session.get("status", "") or "").strip().lower() in {"stopped", "cancelled", "canceled"}:
                return

            template = _normalize_longform_template(session.get("template", "story"))
            format_preset = str(session.get("format_preset", "explainer") or "explainer").strip().lower()
            topic = str(session.get("topic", "") or "").strip()
            input_title = str(session.get("input_title", "") or "").strip()
            input_description = str(session.get("input_description", "") or "").strip()
            metadata_pack = dict(session.get("metadata_pack") or {})
            source_context = str(metadata_pack.get("source_context", "") or "").strip()
            strategy_notes = _marketing_doctrine_text(str(session.get("strategy_notes", "") or "").strip())
            language = _normalize_longform_language(session.get("language", "en"))
            resolution = str(session.get("resolution", "720p_landscape") or "720p_landscape")
            chapter_tone = _longform_detect_tone(template, topic, input_title, input_description)
            target_minutes = _normalize_longform_target_minutes(session.get("target_minutes", LONGFORM_DEFAULT_TARGET_MINUTES))
            chapter_count, chapter_target_sec = _longform_chapter_scene_targets(target_minutes)
            edit_blueprint = dict(session.get("edit_blueprint") or {})

            chapter_list = list(session.get("chapters") or [])
            if chapter_index < 0 or chapter_index >= len(chapter_list):
                return
            chapter_seed = dict(chapter_list[chapter_index] or {})
            chapter_blueprint = _catalyst_chapter_blueprint_for_index(edit_blueprint, chapter_index)
            slot = str(chapter_seed.get("brand_slot", "") or _longform_brand_slot(chapter_index, chapter_count))
            prior_retry = int(chapter_seed.get("retry_count", 0) or 0)

            try:
                chapter = await _generate_longform_chapter(
                    template=template,
                    topic=topic,
                    input_title=input_title,
                    input_description=input_description,
                    format_preset=format_preset,
                    chapter_index=chapter_index,
                    chapter_count=chapter_count,
                    chapter_target_sec=chapter_target_sec,
                    language=language,
                    brand_slot=slot,
                    source_context=source_context,
                    strategy_notes=strategy_notes,
                    edit_blueprint=edit_blueprint,
                    chapter_blueprint=chapter_blueprint,
                )
                chapter["last_error"] = ""
            except Exception as e:
                chapter = _longform_fallback_chapter(
                    topic=topic,
                    input_title=input_title or topic or "Untitled",
                    chapter_index=chapter_index,
                    chapter_target_sec=chapter_target_sec,
                    chapter_count=chapter_count,
                    brand_slot=slot,
                    tone=chapter_tone,
                    template=template,
                    format_preset=format_preset,
                    edit_blueprint=edit_blueprint,
                    chapter_blueprint=chapter_blueprint,
                )
                chapter["last_error"] = ""
                log.warning(f"[longform:{session_id}] chapter {chapter_index + 1}/{chapter_count} fallback used: {e}")

            chapter = await _longform_attach_scene_previews(
                session_id=session_id,
                template=template,
                chapter=chapter,
                resolution=resolution,
                format_preset=str(session.get("format_preset", "") or ""),
                edit_blueprint=edit_blueprint,
                chapter_blueprint=chapter_blueprint,
            )
            auto_pipeline = _bool_from_any(session.get("auto_pipeline"), False)
            chapter["status"] = "approved" if auto_pipeline else "pending_review"
            chapter["retry_count"] = max(prior_retry, int(chapter.get("retry_count", 0) or 0))

            async with _longform_sessions_lock:
                live = _longform_sessions.get(session_id)
                if not isinstance(live, dict):
                    return
                if _bool_from_any(live.get("stop_requested"), False) or str(live.get("status", "") or "").strip().lower() in {"stopped", "cancelled", "canceled"}:
                    return
                chapters_live = list(live.get("chapters") or [])
                if chapter_index >= len(chapters_live):
                    return
                chapters_live[chapter_index] = chapter
                live["chapters"] = chapters_live
                progress = dict(live.get("draft_progress") or {})
                live["status"] = "draft_review"
                live["draft_progress"] = {
                    "total_chapters": int(len(chapters_live)),
                    "generated_chapters": int(_longform_generated_chapter_count(chapters_live)),
                    "approved_chapters": int(_longform_approved_chapter_count(chapters_live)),
                    "failed_chapters": int(progress.get("failed_chapters", 0) or 0),
                    "stage": "auto_pipeline_progress" if auto_pipeline else "awaiting_owner_approval",
                }
                live["updated_at"] = time.time()
                _save_longform_sessions()
                should_continue = bool(auto_pipeline) and not live.get("paused_error")
            if should_continue:
                await _queue_next_longform_chapter_if_ready(session_id)
        except Exception as e:
            log.error(f"[longform:{session_id}] chapter generation failed: {e}", exc_info=True)
            async with _longform_sessions_lock:
                live = _longform_sessions.get(session_id)
                if isinstance(live, dict):
                    if _bool_from_any(live.get("stop_requested"), False) or str(live.get("status", "") or "").strip().lower() in {"stopped", "cancelled", "canceled"}:
                        return
                    live["status"] = "error"
                    live["paused_error"] = {
                        "stage": "draft_generation",
                        "chapter_index": int(chapter_index),
                        "error": str(e),
                    }
                    chapters_live = list(live.get("chapters") or [])
                    progress = dict(live.get("draft_progress") or {})
                    live["draft_progress"] = {
                        "total_chapters": int(chapter_count or len(chapters_live)),
                        "generated_chapters": int(_longform_generated_chapter_count(chapters_live)),
                        "approved_chapters": int(_longform_approved_chapter_count(chapters_live)),
                        "failed_chapters": int(progress.get("failed_chapters", 0) or 0),
                        "stage": "error",
                    }
                    live["updated_at"] = time.time()
                    _save_longform_sessions()


async def _queue_next_longform_chapter_if_ready(session_id: str) -> None:
    next_idx: int | None = None
    should_auto_finalize = False
    async with _longform_sessions_lock:
        live = _longform_sessions.get(session_id)
        if not isinstance(live, dict):
            return
        if _bool_from_any(live.get("stop_requested"), False) or str(live.get("status", "") or "").strip().lower() in {"stopped", "cancelled", "canceled"}:
            return
        if str(live.get("status", "") or "") == "bootstrapping":
            return
        if not _longform_session_analysis_complete(live):
            progress = dict(live.get("draft_progress") or {})
            progress["stage"] = "analyzing_source"
            live["draft_progress"] = progress
            live["status"] = "bootstrapping"
            live["updated_at"] = time.time()
            _save_longform_sessions()
            return
        chapters = list(live.get("chapters") or [])
        if not chapters:
            return
        if any(str((c or {}).get("status", "") or "") == "draft_generating" for c in chapters):
            return

        auto_pipeline = _bool_from_any(live.get("auto_pipeline"), False)
        for i, chapter in enumerate(chapters):
            status = str((chapter or {}).get("status", "") or "")
            if status == "awaiting_previous_approval":
                next_idx = i
                break

        generated = _longform_generated_chapter_count(chapters)
        approved = _longform_approved_chapter_count(chapters)
        progress = dict(live.get("draft_progress") or {})

        if next_idx is None:
            catalyst_preflight = _catalyst_longform_preflight(live)
            live["status"] = "draft_review"
            live["draft_progress"] = {
                "total_chapters": int(len(chapters)),
                "generated_chapters": int(generated),
                "approved_chapters": int(approved),
                "failed_chapters": int(progress.get("failed_chapters", 0) or 0),
                "stage": (
                    "catalyst_preflight_blocked"
                    if catalyst_preflight.get("status") == "blocked" and approved == len(chapters)
                    else ("ready_for_finalize" if approved == len(chapters) else ("auto_pipeline_progress" if auto_pipeline else "awaiting_owner_approval"))
                ),
            }
            if catalyst_preflight.get("status") == "blocked" and approved == len(chapters):
                suggested_fix_note = _catalyst_default_fix_note_for_session(live, 0, "")
                live["paused_error"] = {
                    "stage": "catalyst_preflight",
                    "error": str(catalyst_preflight.get("summary", "") or "Catalyst preflight blocked finalize."),
                    "blockers": list(catalyst_preflight.get("blockers") or []),
                    "chapter_index": 0,
                    "suggested_fix_note": suggested_fix_note,
                }
            elif isinstance(live.get("paused_error"), dict) and str((live.get("paused_error") or {}).get("stage", "") or "") == "catalyst_preflight":
                live["paused_error"] = None
            live["updated_at"] = time.time()
            _save_longform_sessions()
            should_auto_finalize = (
                bool(auto_pipeline)
                and approved == len(chapters)
                and len(chapters) > 0
                and not live.get("paused_error")
                and str(live.get("status", "") or "") != "complete"
                and not (str(live.get("job_id", "") or "").strip() and str(live.get("status", "") or "") == "rendering")
                and catalyst_preflight.get("status") != "blocked"
            )
        else:
            if any(str((chapters[j] or {}).get("status", "") or "") != "approved" for j in range(next_idx)):
                live["status"] = "draft_review"
                live["draft_progress"] = {
                    "total_chapters": int(len(chapters)),
                    "generated_chapters": int(generated),
                    "approved_chapters": int(approved),
                    "failed_chapters": int(progress.get("failed_chapters", 0) or 0),
                    "stage": "awaiting_owner_approval",
                }
                live["updated_at"] = time.time()
                _save_longform_sessions()
                return

            chapter = dict(chapters[next_idx] or {})
            chapter["status"] = "draft_generating"
            chapter["summary"] = chapter.get("summary") or "Generating chapter draft..."
            chapter["last_error"] = ""
            chapter["scenes"] = []
            chapters[next_idx] = chapter
            live["chapters"] = chapters
            live["status"] = "draft_generating"
            live["draft_progress"] = {
                "total_chapters": int(len(chapters)),
                "generated_chapters": int(generated),
                "approved_chapters": int(approved),
                "failed_chapters": int(progress.get("failed_chapters", 0) or 0),
                "stage": f"generating_chapter_{next_idx + 1}",
            }
            live["updated_at"] = time.time()
            _save_longform_sessions()

    shortform_priority = await _shortform_priority_snapshot()
    if shortform_priority.get("priority_active"):
        if should_auto_finalize:
            await _mark_longform_waiting_for_shortform_capacity(
                session_id,
                stage="awaiting_render_capacity",
                snapshot=shortform_priority,
            )
            return
        if next_idx is not None:
            await _mark_longform_waiting_for_shortform_capacity(
                session_id,
                stage="waiting_for_shortform_capacity",
                snapshot=shortform_priority,
            )
            return

    if should_auto_finalize:
        asyncio.create_task(_auto_finalize_longform_session(session_id))
        return
    if next_idx is not None:
        asyncio.create_task(_generate_longform_chapter_for_session(session_id, next_idx))


async def _run_longform_pipeline(job_id: str, session_id: str):
    pause_details: dict = {}
    session_snapshot: dict = {}
    try:
        async with _longform_sessions_lock:
            # Worker runs in a separate process from the API; reload disk state
            # so newly-created sessions are visible before render starts.
            if session_id not in _longform_sessions:
                _load_longform_sessions()
            if session_id not in _longform_sessions:
                raise RuntimeError("Long-form session not found")
            session_snapshot = dict(_longform_sessions[session_id])
        if _bool_from_any(session_snapshot.get("stop_requested"), False) or str(session_snapshot.get("status", "") or "").strip().lower() in {"stopped", "cancelled", "canceled"}:
            await _mark_longform_job_cancelled(job_id, "Stopped by owner.")
            return
        template = str(session_snapshot.get("template", "story") or "story")
        resolution = str(session_snapshot.get("resolution", "720p_landscape") or "720p_landscape")
        language = _normalize_longform_language(session_snapshot.get("language", "en"))
        animation_enabled = _bool_from_any(session_snapshot.get("animation_enabled"), True)
        sfx_enabled = _bool_from_any(session_snapshot.get("sfx_enabled"), True)
        whisper_mode = _normalize_longform_whisper_mode(session_snapshot.get("whisper_mode", "subtle"))
        chapters = list(session_snapshot.get("chapters") or [])
        topic = str(session_snapshot.get("topic", "") or "")
        input_title = str(session_snapshot.get("input_title", "") or topic or "Untitled")
        input_description = str(session_snapshot.get("input_description", "") or "")
        format_preset = str(session_snapshot.get("format_preset", "explainer") or "explainer").strip().lower()
        edit_blueprint = dict(session_snapshot.get("edit_blueprint") or {})
        channel_memory_snapshot = dict(session_snapshot.get("channel_memory") or {})
        channel_context = dict((dict(session_snapshot.get("metadata_pack") or {})).get("youtube_channel") or {})
        sound_mix_profile = _catalyst_audio_mix_profile(
            edit_blueprint,
            format_preset=format_preset,
            render_horror_audio=False,
        )
        session_tone = _longform_detect_tone(template, topic, input_title, input_description)
        chapter_tones = {
            int((chapter or {}).get("index", idx) or idx): str((chapter or {}).get("tone", session_tone) or session_tone)
            for idx, chapter in enumerate(chapters)
        }
        render_horror_audio = _longform_is_horror_tone(session_tone) or any(
            _longform_is_horror_tone(tone) for tone in chapter_tones.values()
        )
        sound_mix_profile = _catalyst_audio_mix_profile(
            edit_blueprint,
            format_preset=format_preset,
            render_horror_audio=render_horror_audio,
        )
        transition_style = _normalize_transition_style(
            str(
                edit_blueprint.get("motion_strategy", {}).get("transition_style", "")
                or ("cinematic" if format_preset in {"recap", "explainer", "documentary"} else "smooth")
            )
        )
        micro_escalation_mode = _normalize_micro_escalation_mode(
            edit_blueprint.get("motion_strategy", {}).get(
                "micro_escalation_mode",
                bool(format_preset in {"recap", "explainer", "documentary"} or animation_enabled),
            ),
            template=template,
        )

        scenes: list[dict] = []
        chapter_markers: list[dict] = []
        cursor_sec = 0.0
        for chapter in chapters:
            chapter_idx = int((chapter or {}).get("index", len(chapter_markers)) or len(chapter_markers))
            chapter_title = str((chapter or {}).get("title", f"Chapter {chapter_idx + 1}") or f"Chapter {chapter_idx + 1}")
            chapter_scenes = _normalize_longform_scenes_for_render((chapter or {}).get("scenes", []))
            chapter_scenes = _scale_scene_durations_to_target(chapter_scenes, float((chapter or {}).get("target_sec", 70) or 70))
            chapter_scenes = list(
                (
                    _longform_apply_brand_slot(
                        {"scenes": chapter_scenes},
                        str((chapter or {}).get("brand_slot", "") or ""),
                        input_title=input_title,
                    ).get("scenes")
                    or chapter_scenes
                )
            )
            chapter_scenes = _normalize_longform_scenes_for_render(chapter_scenes)
            chapter_scenes = _repair_longform_generated_scenes(
                chapter_scenes,
                template=template,
                format_preset=format_preset,
                topic=topic,
                input_title=input_title,
                edit_blueprint=edit_blueprint,
                chapter_blueprint=_catalyst_chapter_blueprint_for_index(edit_blueprint, chapter_idx),
                allow_narration_rewrite=False,
            )
            chapter_tone = str((chapter or {}).get("tone", chapter_tones.get(chapter_idx, session_tone)) or chapter_tones.get(chapter_idx, session_tone))
            chapter_scenes = _longform_enforce_tone_on_scenes(chapter_scenes, tone=chapter_tone, template=template)
            if not chapter_scenes:
                raise LongFormPauseError(
                    f"Chapter {chapter_idx + 1} has no scenes",
                    {"chapter_index": chapter_idx, "stage": "planning", "error": "empty_chapter_scenes"},
                )
            start_sec = cursor_sec
            for scene in chapter_scenes:
                scene_copy = dict(scene or {})
                scene_copy["_chapter_index"] = chapter_idx
                scene_copy["_chapter_blueprint"] = _catalyst_chapter_blueprint_for_index(edit_blueprint, chapter_idx)
                scenes.append(scene_copy)
                cursor_sec += float(scene_copy.get("duration_sec", 6) or 6)
            chapter_markers.append({
                "index": chapter_idx,
                "title": chapter_title,
                "start_sec": round(start_sec, 2),
                "end_sec": round(cursor_sec, 2),
            })

        if not scenes:
            raise RuntimeError("Long-form render has no scenes")

        _job_set_stage(job_id, "generating_images", 8)
        jobs[job_id]["total_scenes"] = len(scenes)
        jobs[job_id]["generation_mode"] = "video" if animation_enabled else "image"
        neg_prompt = TEMPLATE_NEGATIVE_PROMPTS.get(template, NEGATIVE_PROMPT)
        if _longform_prefers_3d_documentary_visuals(template, format_preset):
            neg_prompt = (
                neg_prompt
                + ", live-action photography, candid human photo, gritty warehouse realism, street-photo realism, film still, "
                + "photographic actor portrait, handheld live-action frame, readable wall text, giant title words, fake UI typography, pseudo text, lower-third captions, anonymous stock-person face, mannequin face, waxy skin, deformed hands, extra fingers, deformed eyes"
            )
            if _is_empire_magnates_channel(channel_context):
                neg_prompt = (
                    neg_prompt
                    + ", isolated floating object, pedestal product shot, centered spotlight object, hero object stage, macro machine cutaway, literal exposed brain, anatomy cross-section, glowing gear sphere, pseudo text, title words in frame"
                )
            elif _is_cryptic_science_channel(channel_context):
                neg_prompt = (
                    neg_prompt
                    + ", generic corporate boardroom, empty archive room, untouched dossier table, skeleton committee, multiple skeleton people, x-ray humans, static meeting room filler, anatomy filler, floating machine props, pseudo text, timestamp label, website footer, title words in frame"
                )
        if render_horror_audio:
            neg_prompt = (
                neg_prompt
                + ", bright cheerful lighting, warm happy tone, colorful festival mood, comedy framing, daylight beach party aesthetics"
            )
        scene_assets: list[dict] = []
        scene_prompts: list[str] = []
        skeleton_anchor = _canonical_skeleton_anchor() if template == "skeleton" else ""
        longform_art_style = _longform_default_art_style(template, format_preset)
        total_steps = len(scenes) * (2 if animation_enabled else 1)

        for i, scene in enumerate(scenes):
            async with _longform_sessions_lock:
                live_session = dict(_longform_sessions.get(session_id) or {})
            if _bool_from_any(live_session.get("stop_requested"), False) or str(live_session.get("status", "") or "").strip().lower() in {"stopped", "cancelled", "canceled"}:
                await _mark_longform_job_cancelled(job_id, "Stopped by owner.")
                return
            chapter_index = int(scene.get("_chapter_index", 0) or 0)
            scene_num = int(scene.get("scene_num", i + 1) or (i + 1))
            _job_update_scene_pointer(
                job_id,
                i + 1,
                len(scenes),
                chapter_index=chapter_index + 1,
                scene_num=scene_num,
            )
            step_base = i * (2 if animation_enabled else 1)
            _job_set_stage(job_id, "generating_images", 8 + int((step_base / max(total_steps, 1)) * 58))
            chapter_tone = str(chapter_tones.get(chapter_index, session_tone) or session_tone)
            locked_visual = _longform_tone_locked_visual_description(
                str(scene.get("visual_description", "") or ""),
                tone=chapter_tone,
                template=template,
                format_preset=format_preset,
            )
            scene["visual_description"] = locked_visual
            chapter_blueprint = dict(scene.get("_chapter_blueprint") or {})
            execution_profile = _catalyst_scene_execution_profile(
                edit_blueprint=edit_blueprint,
                chapter_blueprint=chapter_blueprint,
                scene_index=i,
                total_scenes=len(scenes),
            )
            scene["_execution_intensity"] = str(execution_profile.get("execution_intensity", "") or "")
            scene["_interrupt_strength"] = str(execution_profile.get("interrupt_strength", "") or "")
            scene["_cut_profile"] = str(execution_profile.get("cut_profile", "") or "")
            scene["_payoff_hold_sec"] = float(execution_profile.get("payoff_hold_sec", 1.1) or 1.1)
            scene["_caption_rhythm"] = str(execution_profile.get("caption_rhythm", "") or "")
            scene["_sound_density"] = str(execution_profile.get("sound_density", "") or "")
            scene_reference_bundle = _longform_scene_reference_bundle(live_session, scene, template)
            reference_image_url = str(scene_reference_bundle.get("reference_image_url", "") or "").strip() or _longform_documentary_reference_image_url(
                template=template,
                format_preset=format_preset,
                channel_context=channel_context,
                edit_blueprint=edit_blueprint,
                chapter_blueprint=chapter_blueprint,
                topic=topic,
                input_title=input_title,
                narration=str(scene.get("narration", "") or ""),
                visual_description=locked_visual,
            )
            reference_lock_mode = _normalize_reference_lock_mode(
                scene_reference_bundle.get("reference_lock_mode"),
                default=_normalize_reference_lock_mode(live_session.get("reference_lock_mode"), default="strict"),
            )
            if not reference_image_url:
                reference_image_url = _normalize_reference_with_default(template, "")
            full_prompt = _build_longform_scene_execution_prompt(
                scene=scene,
                template=template,
                format_preset=format_preset,
                topic=topic,
                input_title=input_title,
                edit_blueprint=edit_blueprint,
                chapter_blueprint=chapter_blueprint,
                scene_index=i,
                total_scenes=len(scenes),
                skeleton_anchor=skeleton_anchor,
                art_style=longform_art_style,
                has_subject_reference=bool(scene_reference_bundle.get("has_subject_reference", False)),
                subject_reference_name=str(scene_reference_bundle.get("reference_name", "") or ""),
            )
            scene_prompts.append(locked_visual)

            img_path = str(TEMP_DIR / f"{job_id}_lf_scene_{i}.png")
            img_result = None
            image_ok = False
            image_error = ""
            for attempt in range(1, LONGFORM_MAX_SCENE_RETRIES + 1):
                try:
                    img_result = await _longform_generate_scene_image(
                        full_prompt,
                        img_path,
                        resolution=resolution,
                        negative_prompt=neg_prompt,
                        template=template,
                        format_preset=format_preset,
                        channel_context=channel_context,
                        reference_image_url=reference_image_url,
                        reference_lock_mode=reference_lock_mode,
                        best_of_enabled=False,
                        salvage_enabled=False,
                    )
                    quality = _score_generated_image_quality(img_path, full_prompt, template=template)
                    score = float(quality.get("score", 0.0) or 0.0)
                    min_score = _image_quality_min_score(
                        template=template,
                        lock_mode="strict",
                        has_reference=bool(reference_image_url),
                    )
                    if score < min_score:
                        log.warning(
                            f"[{job_id}] Long-form image quality below gate for scene {i + 1}: "
                            f"{score:.1f} < {min_score:.1f}. Accepting for throughput."
                        )
                    cdn_url = str((img_result or {}).get("cdn_url", "") or "")
                    preview_url = ""
                    if Path(img_path).exists():
                        preview_url = str(scene.get("image_url", "") or "")
                    if not preview_url:
                        preview_url = str(cdn_url or "")
                    if preview_url:
                        _job_update_preview(
                            job_id,
                            preview_url=preview_url,
                            preview_type="image",
                            preview_label=f"Chapter {chapter_index + 1} Scene {scene_num} preview",
                        )
                    image_ok = True
                    break
                except Exception as e:
                    image_error = str(e)
            if not image_ok:
                pause_details = {
                    "chapter_index": chapter_index,
                    "scene_index": i,
                    "stage": "image_generation",
                    "error": image_error or "image_generation_failed_after_retries",
                }
                raise LongFormPauseError("Image generation failed repeatedly", pause_details)

            asset = {"image": img_path, "frames": None, "kling_clip": None}
            cdn_url = str((img_result or {}).get("cdn_url", "") or "")
            if animation_enabled:
                _job_set_stage(job_id, "animating_scenes", 8 + int(((step_base + 1) / max(total_steps, 1)) * 58))
                anim_error = ""
                clip_ok = False
                for _attempt in range(1, LONGFORM_MAX_SCENE_RETRIES + 1):
                    try:
                        clip_path = str(TEMP_DIR / f"{job_id}_lf_clip_{i}.mp4")
                        motion_prompt = _build_longform_scene_motion_prompt(
                            scene=scene,
                            edit_blueprint=edit_blueprint,
                            chapter_blueprint=chapter_blueprint,
                            scene_index=i,
                            total_scenes=len(scenes),
                        ) + " " + TEMPLATE_KLING_MOTION.get(template, "Cinematic motion.")
                        out_clip = await animate_image_kling(
                            image_path=img_path,
                            prompt=motion_prompt,
                            output_clip_path=clip_path,
                            duration=str(max(4, min(12, int(round(float(scene.get("duration_sec", 6) or 6)))))),
                            aspect_ratio="16:9",
                            image_cdn_url=cdn_url,
                        )
                        if out_clip and Path(out_clip).exists():
                            asset["kling_clip"] = out_clip
                            clip_ok = True
                            break
                    except Exception as e:
                        anim_error = str(e)
                if not clip_ok:
                    pause_details = {
                        "chapter_index": chapter_index,
                        "scene_index": i,
                        "stage": "animation",
                        "error": anim_error or "animation_failed_after_retries",
                    }
                    raise LongFormPauseError("Animation failed repeatedly", pause_details)
            scene_assets.append(asset)

        _job_set_stage(job_id, "generating_voice", 70)
        full_narration = " ".join(str((s or {}).get("narration", "") or "") for s in scenes)
        audio_path = str(TEMP_DIR / f"{job_id}_lf_voice.mp3")
        vo_result = await generate_voiceover(
            full_narration,
            audio_path,
            template=template,
            language=language,
            override_speed=float(sound_mix_profile.get("voice_speed", 1.0) or 1.0),
        )
        audio_path = vo_result["audio_path"]
        word_timings = vo_result.get("word_timings", [])

        subtitle_path = None
        if word_timings:
            subtitle_path = str(TEMP_DIR / f"{job_id}_lf_captions.ass")
            generate_ass_subtitles(word_timings, subtitle_path, resolution=resolution, template=template)

        sfx_paths: list[str] = []
        if sfx_enabled:
            _job_set_stage(job_id, "generating_sfx", 78)
            whisper_hint = ""
            if whisper_mode == "subtle":
                whisper_hint = " with subtle whispered ambience under bed"
            elif whisper_mode == "cinematic":
                whisper_hint = " with cinematic whisper textures and breathy tension"
            for i, scene in enumerate(scenes):
                sfx_out = str(TEMP_DIR / f"{job_id}_lf_sfx_{i}.mp3")
                chapter_tone = str(chapter_tones.get(int(scene.get("_chapter_index", 0) or 0), session_tone) or session_tone)
                desc = _build_longform_scene_sfx_brief(
                    scene={
                        **dict(scene or {}),
                        "visual_description": _longform_tone_locked_visual_description(
                            str(scene.get("visual_description", "") or ""),
                            tone=chapter_tone,
                            template=template,
                            format_preset=format_preset,
                        )
                    },
                    edit_blueprint=edit_blueprint,
                    chapter_blueprint=dict(scene.get("_chapter_blueprint") or {}),
                    scene_index=i,
                    total_scenes=len(scenes),
                ) + whisper_hint
                dur = float(scene.get("duration_sec", 6) or 6)
                sfx_file = await generate_scene_sfx(desc, dur, sfx_out, template=template, scene_index=i, total_scenes=len(scenes))
                sfx_paths.append(sfx_file)
            sfx_paths = await _quintuple_check_scene_sfx(scenes, sfx_paths, template, job_id=job_id)

        bgm_track = ""
        if bool(sound_mix_profile.get("bgm_required")):
            bgm_path = str(TEMP_DIR / f"{job_id}_lf_horror_bgm.mp3")
            total_duration = sum(float((s or {}).get("duration_sec", 5.0) or 5.0) for s in scenes) + 1.0
            if render_horror_audio:
                bgm_track = await _generate_spooky_bgm_track(total_duration, bgm_path, whisper_mode=whisper_mode)
            else:
                bgm_track = await _generate_catalyst_bgm_track(
                    total_duration,
                    bgm_path,
                    music_profile=str(sound_mix_profile.get("music_profile", "") or ""),
                    whisper_mode=whisper_mode,
                    format_preset=format_preset,
                )

        _job_set_stage(job_id, "compositing", 84)
        output_filename = f"longform_{template}_{job_id}.mp4"
        output_path = str(OUTPUT_DIR / output_filename)
        await composite_video(
            scenes,
            scene_assets,
            audio_path,
            output_path,
            resolution=resolution,
            use_svd=animation_enabled,
            subtitle_path=subtitle_path,
            sfx_paths=sfx_paths,
            bgm_track=bgm_track,
            transition_style=transition_style,
            micro_escalation_mode=micro_escalation_mode,
            pattern_interrupt_interval_sec=int(
                edit_blueprint.get("pacing_strategy", {}).get("pattern_interrupt_interval_sec", 12) or 12
            ),
            voice_gain=float(sound_mix_profile.get("voice_gain", 1.0) or 1.0),
            ambience_gain=float(sound_mix_profile.get("ambience_gain", 0.18) or 0.18),
            sfx_gain=float(sound_mix_profile.get("sfx_gain", 1.0) or 1.0),
            bgm_gain=float(sound_mix_profile.get("bgm_gain", 0.55) or 0.55),
            job_id=job_id,
        )

        for sfx in sfx_paths:
            if sfx:
                Path(sfx).unlink(missing_ok=True)
        if bgm_track:
            Path(bgm_track).unlink(missing_ok=True)
        for asset in scene_assets:
            Path(str(asset.get("image", ""))).unlink(missing_ok=True)
            if asset.get("kling_clip"):
                Path(str(asset["kling_clip"])).unlink(missing_ok=True)
            if asset.get("frames"):
                for fp in asset["frames"]:
                    Path(fp).unlink(missing_ok=True)
        Path(audio_path).unlink(missing_ok=True)
        if subtitle_path:
            Path(subtitle_path).unlink(missing_ok=True)

        _job_set_stage(job_id, "complete", 100)
        jobs[job_id]["output_file"] = output_filename
        jobs[job_id]["resolution"] = resolution
        jobs[job_id]["metadata"] = {
            "title": input_title,
            "description": str(session_snapshot.get("input_description", "") or ""),
            "tags": list(((session_snapshot.get("metadata_pack") or {}).get("tags") or [])),
            "chapters": chapter_markers,
        }
        metadata_pack_snapshot = dict(session_snapshot.get("metadata_pack") or {})
        publish_candidates = _longform_build_publish_package_candidates(
            template=template,
            format_preset=format_preset,
            topic=topic,
            input_title=input_title,
            input_description=str(session_snapshot.get("input_description", "") or ""),
            source_bundle=dict(metadata_pack_snapshot.get("source_video") or {}),
            source_analysis=dict(metadata_pack_snapshot.get("source_analysis") or {}),
            channel_context=dict(metadata_pack_snapshot.get("youtube_channel") or {}),
            channel_memory=channel_memory_snapshot,
        )
        package_title_variants = list(publish_candidates.get("title_variants") or [])
        package_description_variants = list(publish_candidates.get("description_variants") or [])
        package_thumbnail_prompts = list(publish_candidates.get("thumbnail_prompts") or [])
        package_tags = list(publish_candidates.get("tags") or [])
        selected_title = str((package_title_variants or [input_title or topic or "Untitled"])[0] or "").strip()
        selected_description = str((package_description_variants or [str(session_snapshot.get("input_description", "") or "")])[0] or "").strip()
        selected_tags = list(package_tags[:16])
        thumbnail_prompt_selected = str((package_thumbnail_prompts or [f"{selected_title} premium faceless documentary thumbnail, 16:9"])[0] or "").strip()
        package_thumbnail_file = ""
        package_thumbnail_url = ""
        package_thumbnail_error = ""
        package_thumbnail_provider = ""
        if thumbnail_prompt_selected:
            try:
                thumbnail_user = {"id": str(session_snapshot.get("user_id", "") or "")}
                thumbnail_output_name = f"{job_id}_longform_package.png"
                thumbnail_output_path = str(_thumbnail_output_dir_for_user(thumbnail_user) / thumbnail_output_name)
                thumbnail_render = await _generate_longform_package_thumbnail(
                    prompt=thumbnail_prompt_selected,
                    selected_title=selected_title,
                    output_path=thumbnail_output_path,
                    format_preset=format_preset,
                    channel_context=dict(metadata_pack_snapshot.get("youtube_channel") or {}),
                )
                package_thumbnail_file = thumbnail_output_name
                package_thumbnail_url = f"/api/thumbnails/generated/{thumbnail_output_name}"
                package_thumbnail_provider = str(thumbnail_render.get("provider", "") or thumbnail_render.get("provider_label", "") or "").strip()
                jobs[job_id]["thumbnail_provider"] = package_thumbnail_provider or "fal"
                jobs[job_id]["thumbnail_request_id"] = str(thumbnail_render.get("request_id", "") or "")
            except Exception as e:
                package_thumbnail_error = str(e)[:240]
        package_payload = {
            "output_file": output_filename,
            "chapters": chapter_markers,
            "title_variants": list(package_title_variants),
            "description_variants": list(package_description_variants),
            "thumbnail_prompts": list(package_thumbnail_prompts),
            "tags": list(package_tags),
            "selected_title": selected_title,
            "selected_description": selected_description,
            "selected_tags": list(selected_tags),
            "thumbnail_prompt": thumbnail_prompt_selected,
            "thumbnail_file": package_thumbnail_file,
            "thumbnail_url": package_thumbnail_url,
            "thumbnail_error": package_thumbnail_error,
            "thumbnail_provider": package_thumbnail_provider,
        }
        learning_record = _heuristic_catalyst_learning_record(
            session_snapshot=session_snapshot,
            edit_blueprint=edit_blueprint,
            chapter_markers=chapter_markers,
            package=package_payload,
        )
        updated_channel_memory = _update_catalyst_channel_memory(
            existing=channel_memory_snapshot,
            session_snapshot=session_snapshot,
            learning_record=learning_record,
            edit_blueprint=edit_blueprint,
            package=package_payload,
        )
        async with _catalyst_memory_lock:
            _load_catalyst_memory()
            learning_key = str(session_snapshot.get("session_id", "") or job_id or f"longform:{session_id}")
            _catalyst_learning_records[learning_key] = learning_record
            channel_memory_key = str(
                session_snapshot.get("channel_memory_key", "")
                or _catalyst_channel_memory_key(
                    str(session_snapshot.get("user_id", "") or ""),
                    str(session_snapshot.get("youtube_channel_id", "") or ""),
                    format_preset,
                )
            )
            if channel_memory_key:
                updated_channel_memory["key"] = channel_memory_key
                _catalyst_channel_memory[channel_memory_key] = updated_channel_memory
            _save_catalyst_memory()
        _job_diag_finalize(job_id)
        await persist_job_state(job_id, jobs[job_id])
        async with _longform_sessions_lock:
            session_live = _longform_sessions.get(session_id)
            if isinstance(session_live, dict):
                session_live["status"] = "complete"
                session_live["paused_error"] = None
                live_metadata_pack = dict(session_live.get("metadata_pack") or {})
                live_metadata_pack["title_variants"] = list(package_title_variants)
                live_metadata_pack["description_variants"] = list(package_description_variants)
                live_metadata_pack["thumbnail_prompts"] = list(package_thumbnail_prompts)
                live_metadata_pack["tags"] = list(package_tags)
                live_metadata_pack["catalyst_channel_memory"] = _catalyst_channel_memory_public_view(updated_channel_memory)
                session_live["metadata_pack"] = live_metadata_pack
                session_live["package"] = package_payload
                session_live["learning_record"] = learning_record
                session_live["channel_memory"] = _catalyst_channel_memory_public_view(updated_channel_memory)
                session_live["updated_at"] = time.time()
                _save_longform_sessions()
    except LongFormPauseError as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
        _job_diag_finalize(job_id)
        await persist_job_state(job_id, jobs[job_id])
        async with _longform_sessions_lock:
            session_live = _longform_sessions.get(session_id)
            if isinstance(session_live, dict):
                session_live["status"] = "paused_needs_fix"
                session_live["paused_error"] = dict(e.details or pause_details or {})
                session_live["updated_at"] = time.time()
                _save_longform_sessions()
    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
        _job_diag_finalize(job_id)
        await persist_job_state(job_id, jobs[job_id])
        async with _longform_sessions_lock:
            session_live = _longform_sessions.get(session_id)
            if isinstance(session_live, dict):
                session_live["status"] = "error"
                if pause_details:
                    session_live["paused_error"] = dict(pause_details)
                session_live["updated_at"] = time.time()
                _save_longform_sessions()


async def _run_longform_pipeline_isolated(job_id: str, session_id: str) -> None:
    async with _longform_render_semaphore:
        await _run_longform_pipeline(job_id, session_id)


async def _create_longform_session_internal(
    *,
    user: dict,
    template: str,
    topic: str = "",
    input_title: str = "",
    input_description: str = "",
    format_preset: str = "explainer",
    source_url: str = "",
    youtube_channel_id: str = "",
    analytics_notes: str = "",
    strategy_notes: str = "",
    transcript_text: str = "",
    reference_image_data_url: str = "",
    reference_lock_mode: str = "strict",
    analytics_image_paths: list[str] | None = None,
    target_minutes: float = LONGFORM_DEFAULT_TARGET_MINUTES,
    language: str = "en",
    animation_enabled: bool = True,
    sfx_enabled: bool = True,
    whisper_mode: str = "subtle",
    auto_pipeline_requested: bool = False,
    session_id_override: str = "",
) -> dict:
    if not XAI_API_KEY:
        raise HTTPException(500, "XAI_API_KEY not configured")

    template = _normalize_longform_template(template)
    format_preset = str(format_preset or "explainer").strip().lower()
    if format_preset not in {"recap", "explainer", "documentary", "story_channel"}:
        format_preset = "explainer"
    topic = str(topic or "").strip()
    input_title = str(input_title or "").strip()
    input_description = str(input_description or "").strip()
    raw_source_url = str(source_url or "").strip()
    source_url = _normalize_external_source_url(raw_source_url)
    youtube_channel_id = str(youtube_channel_id or "").strip()
    if raw_source_url and not source_url:
        raise HTTPException(400, "Source URL is invalid")
    analytics_notes = str(analytics_notes or "").strip()
    strategy_notes = str(strategy_notes or "").strip()
    transcript_text = str(transcript_text or "").strip()
    reference_image_data_url = str(reference_image_data_url or "").strip()
    reference_lock_mode = _normalize_reference_lock_mode(reference_lock_mode, default="strict")
    target_minutes = _normalize_longform_target_minutes(target_minutes)
    language = _normalize_longform_language(language)
    whisper_mode = _normalize_longform_whisper_mode(whisper_mode)
    analytics_image_paths = [str(p).strip() for p in list(analytics_image_paths or []) if str(p).strip()]
    auto_pipeline = bool(auto_pipeline_requested and _is_admin_user(user))
    channel_context = await _youtube_selected_channel_context(user, preferred_channel_id=youtube_channel_id)
    memory_channel_id = str((channel_context or {}).get("channel_id", "") or youtube_channel_id or "").strip()
    if memory_channel_id:
        await _maybe_refresh_channel_outcomes_before_longform_run(
            user_id=str(user.get("id", "") or ""),
            channel_id=memory_channel_id,
        )
        channel_context = await _youtube_selected_channel_context(user, preferred_channel_id=memory_channel_id)
        memory_channel_id = str((channel_context or {}).get("channel_id", "") or memory_channel_id or "").strip()
    channel_memory_key = _catalyst_channel_memory_key(str(user.get("id", "") or ""), memory_channel_id, format_preset)
    async with _catalyst_memory_lock:
        _load_catalyst_memory()
        channel_memory = dict(_catalyst_channel_memory.get(channel_memory_key) or {})
    channel_memory = _coerce_documentary_longform_channel_memory(
        channel_context,
        channel_memory,
        format_preset=format_preset,
        topic=topic,
        input_title=input_title,
    )

    source_bundle = await _fetch_source_video_bundle(source_url, language=language) if source_url else {}
    if source_bundle:
        source_bundle = dict(source_bundle)
    if transcript_text:
        source_bundle["manual_transcript_excerpt"] = _clip_text(transcript_text, 12000)
        if not str(source_bundle.get("transcript_excerpt", "") or "").strip():
            source_bundle["transcript_excerpt"] = _clip_text(transcript_text, 12000)

    operator_evidence = await _summarize_longform_operator_evidence(
        transcript_text=transcript_text,
        image_paths=analytics_image_paths,
        source_bundle=source_bundle,
    )
    merged_analytics_notes = _build_longform_operator_notes(
        analytics_notes=analytics_notes,
        transcript_text=transcript_text,
        operator_evidence=operator_evidence,
    )
    source_analysis = dict(await _build_source_performance_analysis(
        source_bundle=source_bundle,
        channel_context=channel_context,
        channel_memory=channel_memory,
        analytics_notes=merged_analytics_notes,
        topic=topic,
        input_title=input_title,
        input_description=input_description,
        strategy_notes=strategy_notes,
    ) or {})
    analytics_summary = str(operator_evidence.get("analytics_summary", "") or "").strip()
    if analytics_summary:
        source_analysis["analytics_summary"] = analytics_summary
    for key in ("strongest_signals", "weak_points", "retention_findings", "packaging_findings"):
        values = [str(v).strip() for v in list(operator_evidence.get(key) or []) if str(v).strip()]
        if values:
            source_analysis[key] = values[:8]
    existing_moves = [str(v).strip() for v in list(source_analysis.get("improvement_moves") or []) if str(v).strip()]
    for move in [str(v).strip() for v in list(operator_evidence.get("improvement_moves") or []) if str(v).strip()]:
        if move and move not in existing_moves:
            existing_moves.append(move)
    if existing_moves:
        source_analysis["improvement_moves"] = existing_moves[:10]

    if source_url and (not topic or not input_title or not input_description):
        auto_seed = await _derive_longform_seed_from_source(
            source_bundle=source_bundle,
            source_analysis=source_analysis,
            channel_context=channel_context,
            channel_memory=channel_memory,
            format_preset=format_preset,
            strategy_notes=strategy_notes,
        )
        if not topic:
            topic = str(auto_seed.get("topic", "") or "").strip()
        if not input_title:
            input_title = str(auto_seed.get("title", "") or "").strip()
        if not input_description:
            input_description = str(auto_seed.get("description", "") or "").strip()
        source_analysis = dict(await _build_source_performance_analysis(
            source_bundle=source_bundle,
            channel_context=channel_context,
            channel_memory=channel_memory,
            analytics_notes=merged_analytics_notes,
            topic=topic,
            input_title=input_title,
            input_description=input_description,
            strategy_notes=strategy_notes,
        ) or {})
        if analytics_summary:
            source_analysis["analytics_summary"] = analytics_summary
        for key in ("strongest_signals", "weak_points", "retention_findings", "packaging_findings"):
            values = [str(v).strip() for v in list(operator_evidence.get(key) or []) if str(v).strip()]
            if values:
                source_analysis[key] = values[:8]
        existing_moves = [str(v).strip() for v in list(source_analysis.get("improvement_moves") or []) if str(v).strip()]
        for move in [str(v).strip() for v in list(operator_evidence.get("improvement_moves") or []) if str(v).strip()]:
            if move and move not in existing_moves:
                existing_moves.append(move)
        if existing_moves:
            source_analysis["improvement_moves"] = existing_moves[:10]
    if not topic:
        raise HTTPException(400, "Topic is required unless a source URL can be analyzed into a follow-up brief")
    if not input_title:
        raise HTTPException(400, "Video title is required unless a source URL can be analyzed into a follow-up brief")
    if not input_description:
        raise HTTPException(400, "Video description is required unless a source URL can be analyzed into a follow-up brief")
    selected_series_context = _resolve_catalyst_series_context(
        channel_context,
        channel_memory=channel_memory,
        topic=topic,
        source_title=str((source_bundle or {}).get("title", "") or ""),
        input_title=input_title,
        input_description=input_description,
        format_preset=format_preset,
    )
    selected_series_cluster = dict(selected_series_context.get("selected_cluster") or {})
    selected_series_anchor = str(selected_series_context.get("series_anchor_override", "") or "").strip()
    channel_memory = dict(selected_series_context.get("memory_view") or channel_memory or {})
    selected_cluster_playbook = dict(selected_series_context.get("cluster_playbook") or {})
    source_context = _render_source_context(
        source_bundle,
        source_analysis,
        merged_analytics_notes,
        channel_context=channel_context,
        channel_memory=channel_memory,
        selected_cluster=selected_series_cluster,
        cluster_playbook=selected_cluster_playbook,
    )

    chapter_count, chapter_target_sec = _longform_chapter_scene_targets(target_minutes)
    edit_blueprint = await _build_catalyst_edit_blueprint(
        template=template,
        format_preset=format_preset,
        topic=topic,
        input_title=input_title,
        input_description=input_description,
        chapter_count=chapter_count,
        chapter_target_sec=chapter_target_sec,
        source_bundle=source_bundle,
        source_analysis=source_analysis,
        channel_context=channel_context,
        channel_memory=channel_memory,
        strategy_notes=strategy_notes,
        xai_json_completion_fn=_xai_json_completion,
        marketing_doctrine_text_fn=_marketing_doctrine_text,
        render_reference_corpus_context_fn=lambda **kwargs: _render_catalyst_reference_corpus_context(
            reference_memory=_catalyst_reference_memory,
            **kwargs,
        ),
        same_arena_subject_fn=_same_arena_subject,
    )
    chapters = [
        _longform_placeholder_chapter(
            i,
            chapter_target_sec,
            _longform_brand_slot(i, chapter_count),
            status="awaiting_previous_approval",
        )
        for i in range(chapter_count)
    ]

    publish_candidates = _longform_build_publish_package_candidates(
        template=template,
        format_preset=format_preset,
        topic=topic,
        input_title=input_title,
        input_description=input_description,
        source_bundle=source_bundle,
        source_analysis=source_analysis,
        channel_context=channel_context,
        channel_memory=channel_memory,
    )

    metadata_pack = {
        "title_variants": list(publish_candidates.get("title_variants") or []),
        "description_variants": list(publish_candidates.get("description_variants") or []),
        "thumbnail_prompts": list(publish_candidates.get("thumbnail_prompts") or []),
        "tags": list(publish_candidates.get("tags") or []),
        "source_video": source_bundle,
        "source_analysis": source_analysis,
        "youtube_channel": channel_context,
        "source_context": source_context,
        "strategy_notes": strategy_notes,
        "marketing_doctrine": list(CATALYST_MARKETING_DOCTRINE),
        "analytics_evidence_summary": analytics_summary,
        "analytics_asset_count": int(len(analytics_image_paths)),
        "manual_transcript_supplied": bool(transcript_text),
        "manual_transcript_excerpt": _clip_text(transcript_text, 2000),
        "analytics_notes_effective": _clip_text(merged_analytics_notes, 2400),
        "catalyst_channel_memory": _catalyst_channel_memory_public_view(channel_memory),
        "selected_series_cluster": selected_series_cluster,
        "selected_series_anchor": selected_series_anchor,
    }
    session_id = str(session_id_override or "").strip() or f"lf_{int(time.time())}_{random.randint(1000, 9999)}"
    now = time.time()
    created_at = now
    async with _longform_sessions_lock:
        existing_session = dict(_longform_sessions.get(session_id) or {}) if session_id_override else {}
    if existing_session:
        created_at = float(existing_session.get("created_at", now) or now)
    session_data = {
        "session_id": session_id,
        "user_id": str(user.get("id", "") or ""),
        "template": template,
        "format_preset": format_preset,
        "auto_pipeline": auto_pipeline,
        "owner_override": bool(_is_admin_user(user)),
        "topic": topic,
        "input_title": input_title,
        "input_description": input_description,
        "source_url": source_url,
        "youtube_channel_id": str((channel_context or {}).get("channel_id", "") or youtube_channel_id),
        "analytics_notes": analytics_notes,
        "strategy_notes": strategy_notes,
        "transcript_text": _clip_text(transcript_text, 12000),
        "reference_image_url": "",
        "reference_image_public_url": "",
        "reference_image_path": "",
        "reference_image_uploaded": False,
        "reference_lock_mode": reference_lock_mode,
        "reference_dna": {},
        "reference_quality": {},
        "rolling_reference_image_url": "",
        "character_references": [dict(item or {}) for item in list(existing_session.get("character_references") or []) if isinstance(item, dict)] if existing_session else [],
        "target_minutes": float(target_minutes),
        "language": language,
        "resolution": "720p_landscape",
        "animation_enabled": _bool_from_any(animation_enabled, True),
        "sfx_enabled": _bool_from_any(sfx_enabled, True),
        "whisper_mode": whisper_mode,
        "chapters": chapters,
        "status": "draft_review",
        "paused_error": None,
        "job_id": "",
        "metadata_pack": metadata_pack,
        "edit_blueprint": edit_blueprint,
        "learning_record": dict(existing_session.get("learning_record") or {}) if existing_session else {},
        "latest_outcome": dict(existing_session.get("latest_outcome") or {}) if existing_session else {},
        "channel_memory_key": channel_memory_key,
        "channel_memory": _catalyst_channel_memory_public_view(channel_memory),
        "draft_progress": {
            "total_chapters": int(chapter_count),
            "generated_chapters": 0,
            "approved_chapters": 0,
            "failed_chapters": 0,
            "stage": "queued_first_chapter",
        },
        "package": {},
        "created_at": created_at,
        "updated_at": now,
    }
    if reference_image_data_url:
        session_data = _persist_longform_reference_image(
            session_data,
            reference_image_url=reference_image_data_url,
            reference_lock_mode=reference_lock_mode,
        )
    async with _longform_sessions_lock:
        _longform_sessions[session_id] = session_data
        _save_longform_sessions()
    await _queue_next_longform_chapter_if_ready(session_id)
    async with _longform_sessions_lock:
        session_live = _longform_sessions.get(session_id, session_data)
    return _longform_public_session(session_live)


async def _create_longform_session(req: LongFormSessionCreateRequest, request: Request = None):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _longform_owner_beta_enabled(user):
        raise HTTPException(403, "Long-form owner beta is restricted")
    busy_session_id = await _active_longform_capacity_session_id(str(user.get("id", "") or ""))
    if busy_session_id:
        raise HTTPException(
            409,
            f"Long-form isolated capacity is already busy on session {busy_session_id}. Wait for that run to finish before starting another active Long Form generation.",
        )
    if not _longform_deep_analysis_enabled(user):
        wants_deep_analysis = bool(
            str(getattr(req, "source_url", "") or "").strip()
            or str(getattr(req, "youtube_channel_id", "") or "").strip()
            or str(getattr(req, "analytics_notes", "") or "").strip()
            or str(getattr(req, "transcript_text", "") or "").strip()
            or bool(getattr(req, "auto_pipeline", False))
        )
        if wants_deep_analysis:
            raise HTTPException(
                403,
                "Source-video deep analysis is owner beta for now. Public Long Form stays on the lighter manual workflow while Catalyst is being tuned.",
            )
    async with _longform_analysis_semaphore:
        session_public = await _create_longform_session_internal(
            user=user,
            template=req.template,
            topic=req.topic,
            input_title=req.input_title,
            input_description=req.input_description,
            format_preset=req.format_preset,
            source_url=req.source_url,
            youtube_channel_id=getattr(req, "youtube_channel_id", ""),
            analytics_notes=req.analytics_notes,
            strategy_notes=req.strategy_notes,
            transcript_text=getattr(req, "transcript_text", ""),
            reference_image_data_url=str(getattr(req, "reference_image_url", "") or ""),
            reference_lock_mode=str(getattr(req, "reference_lock_mode", "strict") or "strict"),
            target_minutes=req.target_minutes,
            language=req.language,
            animation_enabled=req.animation_enabled,
            sfx_enabled=req.sfx_enabled,
            whisper_mode=req.whisper_mode,
            auto_pipeline_requested=getattr(req, "auto_pipeline", False),
        )
    return {"session": session_public}


def _create_longform_bootstrap_placeholder_session(
    *,
    user: dict,
    template: str,
    topic: str,
    input_title: str,
    input_description: str,
    format_preset: str,
    source_url: str,
    youtube_channel_id: str,
    analytics_notes: str,
    strategy_notes: str,
    transcript_text: str,
    target_minutes: float,
    language: str,
    animation_enabled: bool,
    sfx_enabled: bool,
    whisper_mode: str,
    auto_pipeline: bool,
    analytics_asset_count: int,
    reference_lock_mode: str = "strict",
) -> dict:
    chapter_count, chapter_target_sec = _longform_chapter_scene_targets(target_minutes)
    session_id = f"lf_{int(time.time())}_{random.randint(1000, 9999)}"
    now = time.time()
    metadata_pack = {
        "title_variants": [],
        "description_variants": [],
        "thumbnail_prompts": [],
        "tags": [],
        "source_video": {},
        "source_analysis": {},
        "youtube_channel": {},
        "source_context": "",
        "strategy_notes": strategy_notes,
        "marketing_doctrine": list(CATALYST_MARKETING_DOCTRINE),
        "analytics_evidence_summary": "",
        "analytics_asset_count": int(analytics_asset_count),
        "manual_transcript_supplied": bool(str(transcript_text or "").strip()),
        "manual_transcript_excerpt": _clip_text(str(transcript_text or "").strip(), 2000),
        "analytics_notes_effective": _clip_text(str(analytics_notes or "").strip(), 2400),
        "catalyst_channel_memory": {},
    }
    return {
        "session_id": session_id,
        "user_id": str(user.get("id", "") or ""),
        "template": _normalize_longform_template(template),
        "format_preset": str(format_preset or "explainer").strip().lower() or "explainer",
        "auto_pipeline": bool(auto_pipeline),
        "owner_override": bool(_is_admin_user(user)),
        "topic": str(topic or "").strip(),
        "input_title": str(input_title or "").strip(),
        "input_description": str(input_description or "").strip(),
        "source_url": str(source_url or "").strip(),
        "youtube_channel_id": str(youtube_channel_id or "").strip(),
        "analytics_notes": str(analytics_notes or "").strip(),
        "strategy_notes": str(strategy_notes or "").strip(),
        "transcript_text": _clip_text(str(transcript_text or "").strip(), 12000),
        "reference_image_url": "",
        "reference_image_public_url": "",
        "reference_image_path": "",
        "reference_image_uploaded": False,
        "reference_lock_mode": _normalize_reference_lock_mode(reference_lock_mode, default="strict"),
        "reference_dna": {},
        "reference_quality": {},
        "rolling_reference_image_url": "",
        "character_references": [],
        "target_minutes": float(target_minutes),
        "language": _normalize_longform_language(language),
        "resolution": "720p_landscape",
        "animation_enabled": _bool_from_any(animation_enabled, True),
        "sfx_enabled": _bool_from_any(sfx_enabled, True),
        "whisper_mode": _normalize_longform_whisper_mode(whisper_mode),
        "chapters": [
            _longform_placeholder_chapter(
                i,
                chapter_target_sec,
                _longform_brand_slot(i, chapter_count),
                status="awaiting_previous_approval",
            )
            for i in range(chapter_count)
        ],
        "status": "bootstrapping",
        "paused_error": None,
        "job_id": "",
        "metadata_pack": metadata_pack,
        "edit_blueprint": {},
        "learning_record": {},
        "latest_outcome": {},
        "channel_memory_key": _catalyst_channel_memory_key(str(user.get("id", "") or ""), str(youtube_channel_id or "").strip(), str(format_preset or "explainer").strip().lower() or "explainer"),
        "channel_memory": {},
        "draft_progress": {
            "total_chapters": int(chapter_count),
            "generated_chapters": 0,
            "approved_chapters": 0,
            "failed_chapters": 0,
            "preview_scene_total": 0,
            "preview_scene_generated": 0,
            "stage": "analyzing_source",
        },
        "package": {},
        "created_at": now,
        "updated_at": now,
    }


def _longform_session_uses_isolated_capacity(session: dict | None) -> bool:
    s = dict(session or {})
    if _bool_from_any(s.get("stop_requested"), False):
        return False
    status = str(s.get("status", "") or "").strip().lower()
    if status in {"stopped", "cancelled", "canceled"}:
        return False
    if status in {"bootstrapping", "draft_generating", "rendering"}:
        return True
    chapters = list(s.get("chapters") or [])
    for chapter in chapters:
        chapter_status = str((chapter or {}).get("status", "") or "").strip().lower()
        if chapter_status in {"regenerating", "draft_generating_images"}:
            return True
    return False


async def _mark_longform_job_cancelled(job_id: str, reason: str = "Stopped by owner.") -> None:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return
    job = dict(jobs.get(normalized_job_id) or {})
    if not job:
        persisted = await get_persisted_job_state(normalized_job_id)
        if isinstance(persisted, dict) and persisted:
            job = dict(persisted)
    if not job:
        return
    job["status"] = "cancelled"
    job["error"] = str(reason or "Stopped by owner.").strip()
    job["cancelled_at"] = time.time()
    jobs[normalized_job_id] = job
    await persist_job_state(normalized_job_id, job)


async def _recover_stalled_longform_session_for_capacity(session: dict | None, *, now_ts: float | None = None) -> tuple[dict, bool]:
    s = dict(session or {})
    if not s:
        return s, False

    now = float(now_ts or time.time())
    changed = False
    updated_at = float(s.get("updated_at", 0) or 0)
    stalled_sec = max(0.0, now - updated_at)
    status = str(s.get("status", "") or "").strip().lower()
    chapters = list(s.get("chapters") or [])
    progress = dict(s.get("draft_progress") or {})
    auto_pipeline = _bool_from_any(s.get("auto_pipeline"), False)

    in_progress_idx = -1
    in_progress_status = ""
    for idx, raw_chapter in enumerate(chapters):
        chapter_status = str((raw_chapter or {}).get("status", "") or "").strip().lower()
        if chapter_status in {"draft_generating", "draft_generating_images", "regenerating"}:
            in_progress_idx = idx
            in_progress_status = chapter_status
            break

    if in_progress_idx >= 0:
        chapter_live = dict(chapters[in_progress_idx] or {})
        chapter_scenes = list(chapter_live.get("scenes") or [])
        scene_count = len(chapter_scenes)
        if in_progress_status == "draft_generating" and scene_count <= 0 and stalled_sec > 180.0:
            chapter_live["status"] = "awaiting_previous_approval"
            chapter_live["last_error"] = "Recovered from stalled generation task; chapter resumed."
            chapters[in_progress_idx] = chapter_live
            s["chapters"] = chapters
            s["status"] = "draft_review"
            progress["stage"] = "auto_resume_after_stall_capacity_gate"
            progress["preview_scene_generated"] = 0
            s["draft_progress"] = progress
            s["updated_at"] = now
            changed = True
        elif in_progress_status == "draft_generating_images" and scene_count > 0 and stalled_sec > 180.0:
            ready_count = 0
            for scene_idx, raw_scene in enumerate(chapter_scenes):
                scene = dict(raw_scene or {})
                has_img = bool(str(scene.get("image_url", "") or "").strip())
                if has_img:
                    ready_count += 1
                else:
                    image_status = str(scene.get("image_status", "") or "").strip().lower()
                    if image_status != "error":
                        scene["image_status"] = "error"
                        scene["image_error"] = "Preview generation stalled; regenerate chapter to refresh this scene."
                    chapter_scenes[scene_idx] = scene
            missing_count = max(0, scene_count - ready_count)
            chapter_live["scenes"] = chapter_scenes
            chapter_live["status"] = "approved" if (auto_pipeline and missing_count == 0) else "pending_review"
            chapter_live["last_error"] = (
                f"Recovered from stalled preview generation ({ready_count}/{scene_count} ready). "
                f"{missing_count} previews missing; regenerate chapter if needed."
                if missing_count > 0
                else "Recovered from stalled preview generation; chapter moved to review."
            )
            chapters[in_progress_idx] = chapter_live
            s["chapters"] = chapters
            s["status"] = "draft_review"
            progress["stage"] = "auto_resume_after_preview_stall_capacity_gate"
            progress["preview_scene_total"] = int(scene_count)
            progress["preview_scene_generated"] = int(ready_count)
            progress["generated_chapters"] = int(_longform_generated_chapter_count(chapters))
            progress["approved_chapters"] = int(_longform_approved_chapter_count(chapters))
            s["draft_progress"] = progress
            s["updated_at"] = now
            changed = True
        elif in_progress_status == "regenerating" and stalled_sec > 300.0:
            chapter_live["status"] = "pending_review"
            chapter_live["last_error"] = "Recovered from stalled chapter regeneration; review or regenerate again."
            chapters[in_progress_idx] = chapter_live
            s["chapters"] = chapters
            s["status"] = "draft_review"
            progress["stage"] = "auto_resume_after_regeneration_stall_capacity_gate"
            progress["generated_chapters"] = int(_longform_generated_chapter_count(chapters))
            progress["approved_chapters"] = int(_longform_approved_chapter_count(chapters))
            s["draft_progress"] = progress
            s["updated_at"] = now
            changed = True

    status = str(s.get("status", "") or "").strip().lower()
    if status == "bootstrapping" and stalled_sec > 900.0:
        s["status"] = "error"
        s["paused_error"] = {
            "stage": "bootstrapping",
            "message": "Recovered from stalled long-form bootstrap. Start a new session or retry.",
        }
        s["updated_at"] = now
        changed = True
    elif status == "rendering":
        job_id = str(s.get("job_id", "") or "").strip()
        job = jobs.get(job_id, {}) if job_id else {}
        if job_id:
            persisted = await get_persisted_job_state(job_id)
            if isinstance(persisted, dict) and persisted:
                jobs[job_id] = persisted
                job = persisted
        job_status = str((job or {}).get("status", "") or "").strip().lower()
        if job_status in {"complete", "completed", "success", "succeeded"}:
            s["status"] = "complete"
            s["updated_at"] = now
            changed = True
        elif job_status in {"error", "failed", "cancelled", "canceled"}:
            s["status"] = "error"
            s["updated_at"] = now
            changed = True
        elif stalled_sec > 900.0 and (not job_id or not isinstance(job, dict) or not job):
            s["status"] = "draft_review"
            s["job_id"] = ""
            s["paused_error"] = {
                "stage": "rendering",
                "message": "Recovered from stale render job. Retry finalize when ready.",
            }
            progress = dict(s.get("draft_progress") or {})
            progress["stage"] = "stale_render_recovered"
            s["draft_progress"] = progress
            s["updated_at"] = now
            changed = True

    return s, changed


async def _active_longform_capacity_session_id(user_id: str) -> str:
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        return ""
    now = time.time()
    async with _longform_sessions_lock:
        _load_longform_sessions()
        sessions = [dict(s or {}) for s in _longform_sessions.values()]
    sessions.sort(key=lambda s: float((s or {}).get("updated_at", 0) or 0), reverse=True)

    normalized_sessions: list[dict] = []
    changed_sessions: dict[str, dict] = {}
    for session in sessions:
        if str((session or {}).get("user_id", "") or "").strip() != normalized_user_id:
            continue
        recovered, changed = await _recover_stalled_longform_session_for_capacity(session, now_ts=now)
        if changed:
            session_id = str((recovered or {}).get("session_id", "") or "").strip()
            if session_id:
                changed_sessions[session_id] = recovered
        normalized_sessions.append(recovered)

    if changed_sessions:
        async with _longform_sessions_lock:
            _load_longform_sessions()
            for session_id, recovered in changed_sessions.items():
                if session_id in _longform_sessions:
                    _longform_sessions[session_id] = recovered
            _save_longform_sessions()

    for session in normalized_sessions:
        if _longform_session_uses_isolated_capacity(session):
            return str((session or {}).get("session_id", "") or "").strip()
    return ""


async def _bootstrap_longform_session_background(
    *,
    session_id: str,
    user: dict,
    template: str,
    topic: str,
    input_title: str,
    input_description: str,
    format_preset: str,
    source_url: str,
    youtube_channel_id: str,
    analytics_notes: str,
    strategy_notes: str,
    transcript_text: str,
    reference_image_data_url: str,
    reference_lock_mode: str,
    analytics_image_paths: list[str],
    target_minutes: float,
    language: str,
    animation_enabled: bool,
    sfx_enabled: bool,
    whisper_mode: str,
    auto_pipeline_requested: bool,
) -> None:
    try:
        async with _longform_analysis_semaphore:
            await _create_longform_session_internal(
                user=user,
                template=template,
                topic=topic,
                input_title=input_title,
                input_description=input_description,
                format_preset=format_preset,
                source_url=source_url,
                youtube_channel_id=youtube_channel_id,
                analytics_notes=analytics_notes,
                strategy_notes=strategy_notes,
                transcript_text=transcript_text,
                reference_image_data_url=reference_image_data_url,
                reference_lock_mode=reference_lock_mode,
                analytics_image_paths=analytics_image_paths,
                target_minutes=target_minutes,
                language=language,
                animation_enabled=animation_enabled,
                sfx_enabled=sfx_enabled,
                whisper_mode=whisper_mode,
                auto_pipeline_requested=auto_pipeline_requested,
                session_id_override=session_id,
            )
    except Exception as e:
        log.error(f"[longform:{session_id}] bootstrap failed: {e}", exc_info=True)
        async with _longform_sessions_lock:
            session_live = _longform_sessions.get(session_id)
            if isinstance(session_live, dict):
                progress = dict(session_live.get("draft_progress") or {})
                session_live["status"] = "error"
                session_live["paused_error"] = {
                    "stage": "bootstrap",
                    "error": str(e),
                }
                progress["stage"] = "bootstrap_error"
                session_live["draft_progress"] = progress
                session_live["updated_at"] = time.time()
                _save_longform_sessions()
    finally:
        for image_path in list(analytics_image_paths or []):
            try:
                Path(image_path).unlink(missing_ok=True)
            except Exception:
                pass


async def _create_longform_session_bootstrap(
    template: str = Form(...),
    topic: str = Form(""),
    input_title: str = Form(""),
    input_description: str = Form(""),
    format_preset: str = Form("explainer"),
    source_url: str = Form(""),
    youtube_channel_id: str = Form(""),
    analytics_notes: str = Form(""),
    strategy_notes: str = Form(""),
    transcript_text: str = Form(""),
    reference_lock_mode: str = Form("strict"),
    auto_pipeline: bool = Form(False),
    target_minutes: float = Form(8.0),
    language: str = Form("en"),
    animation_enabled: bool = Form(True),
    sfx_enabled: bool = Form(True),
    whisper_mode: str = Form("subtle"),
    subject_reference_image: UploadFile | None = File(None),
    analytics_images: list[UploadFile] = File([]),
    request: Request = None,
):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    busy_session_id = await _active_longform_capacity_session_id(str(user.get("id", "") or ""))
    if busy_session_id:
        raise HTTPException(
            409,
            f"Long-form isolated capacity is already busy on session {busy_session_id}. Wait for that run to finish before starting another active Long Form generation.",
        )
    if not _longform_deep_analysis_enabled(user):
        raise HTTPException(
            403,
            "Source-video deep analysis is owner beta for now. Public Long Form stays on the lighter manual workflow while Catalyst is being tuned.",
        )
    normalized_template = _normalize_longform_template(template)
    normalized_format_preset = str(format_preset or "explainer").strip().lower()
    if normalized_format_preset not in {"recap", "explainer", "documentary", "story_channel"}:
        normalized_format_preset = "explainer"
    normalized_language = _normalize_longform_language(language)
    normalized_target_minutes = _normalize_longform_target_minutes(target_minutes)
    normalized_whisper_mode = _normalize_longform_whisper_mode(whisper_mode)
    normalized_reference_lock_mode = _normalize_reference_lock_mode(reference_lock_mode, default="strict")
    normalized_source_url = _normalize_external_source_url(str(source_url or "").strip())
    if str(source_url or "").strip() and not normalized_source_url:
        raise HTTPException(400, "Source URL is invalid")
    auto_pipeline_requested = bool(_bool_from_any(auto_pipeline, False))
    upload_dir = TEMP_DIR / "longform_bootstrap"
    upload_dir.mkdir(parents=True, exist_ok=True)
    saved_image_paths: list[str] = []
    reference_image_data_url = ""
    if subject_reference_image is not None and getattr(subject_reference_image, "filename", ""):
        if not subject_reference_image.content_type or not subject_reference_image.content_type.startswith("image/"):
            raise HTTPException(400, "Reference file must be an image")
        raw_reference = await subject_reference_image.read()
        if not raw_reference:
            raise HTTPException(400, "Reference image is empty")
        if len(raw_reference) > 8 * 1024 * 1024:
            raise HTTPException(400, "Reference image must be <= 8MB")
        quality = _analyze_reference_quality(raw_reference, lock_mode=normalized_reference_lock_mode)
        if not quality.get("accepted", True) and normalized_reference_lock_mode == "strict":
            raise HTTPException(400, "Reference image quality too low for Strict Reference Lock. Upload a higher-resolution image or switch to Style Inspired mode.")
        mime = subject_reference_image.content_type or "image/png"
        reference_image_data_url = f"data:{mime};base64,{base64.b64encode(raw_reference).decode()}"
    for idx, analytics_image in enumerate(list(analytics_images or [])[:24]):
        filename = str(getattr(analytics_image, "filename", "") or "").strip()
        if not filename:
            continue
        ext = Path(filename).suffix.lower()
        if ext not in {".png", ".jpg", ".jpeg", ".webp"}:
            ext = ".png"
        saved_path = upload_dir / f"lf_bootstrap_{int(time.time())}_{random.randint(1000, 9999)}_{idx}{ext}"
        with open(saved_path, "wb") as fh:
            while chunk := await analytics_image.read(1024 * 1024):
                fh.write(chunk)
        if saved_path.exists() and saved_path.stat().st_size > 0:
            saved_image_paths.append(str(saved_path))

    placeholder_session = _create_longform_bootstrap_placeholder_session(
        user=user,
        template=normalized_template,
        topic=topic,
        input_title=input_title,
        input_description=input_description,
        format_preset=normalized_format_preset,
        source_url=normalized_source_url,
        youtube_channel_id=str(youtube_channel_id or "").strip(),
        analytics_notes=analytics_notes,
        strategy_notes=strategy_notes,
        transcript_text=transcript_text,
        target_minutes=normalized_target_minutes,
        language=normalized_language,
        animation_enabled=animation_enabled,
        sfx_enabled=sfx_enabled,
        whisper_mode=normalized_whisper_mode,
        auto_pipeline=bool(auto_pipeline_requested and _is_admin_user(user)),
        analytics_asset_count=len(saved_image_paths),
        reference_lock_mode=normalized_reference_lock_mode,
    )
    if reference_image_data_url:
        placeholder_session = _persist_longform_reference_image(
            placeholder_session,
            reference_image_url=reference_image_data_url,
            reference_lock_mode=normalized_reference_lock_mode,
        )
    async with _longform_sessions_lock:
        _longform_sessions[placeholder_session["session_id"]] = placeholder_session
        _save_longform_sessions()

    asyncio.create_task(
        _bootstrap_longform_session_background(
            session_id=str(placeholder_session["session_id"]),
            user=dict(user),
            template=normalized_template,
            topic=topic,
            input_title=input_title,
            input_description=input_description,
            format_preset=normalized_format_preset,
            source_url=normalized_source_url,
            youtube_channel_id=str(youtube_channel_id or "").strip(),
            analytics_notes=analytics_notes,
            strategy_notes=strategy_notes,
            transcript_text=transcript_text,
            reference_image_data_url=reference_image_data_url,
            reference_lock_mode=normalized_reference_lock_mode,
            analytics_image_paths=list(saved_image_paths),
            target_minutes=normalized_target_minutes,
            language=normalized_language,
            animation_enabled=animation_enabled,
            sfx_enabled=sfx_enabled,
            whisper_mode=normalized_whisper_mode,
            auto_pipeline_requested=auto_pipeline_requested,
        )
    )
    return {"session": _longform_public_session(placeholder_session)}


async def _longform_reference_image(
    session_id: str,
    reference_image: UploadFile = File(...),
    reference_lock_mode: str = Form("strict"),
    request: Request = None,
):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    async with _longform_sessions_lock:
        _load_longform_sessions()
        session = dict(_longform_sessions.get(session_id) or {})
        if not session:
            raise HTTPException(404, "Long-form session not found")
        if str(session.get("user_id", "") or "").strip() != str(user.get("id", "") or "").strip():
            raise HTTPException(403, "Not your session")
        if not reference_image.content_type or not reference_image.content_type.startswith("image/"):
            raise HTTPException(400, "Reference file must be an image")
        raw = await reference_image.read()
        if not raw:
            raise HTTPException(400, "Reference image is empty")
        mime = reference_image.content_type or "image/png"
        data_url = f"data:{mime};base64,{base64.b64encode(raw).decode()}"
        session = _persist_longform_reference_image(
            session,
            reference_image_url=data_url,
            reference_lock_mode=reference_lock_mode,
        )
        session["updated_at"] = time.time()
        _longform_sessions[session_id] = session
        _save_longform_sessions()
    return {
        "ok": True,
        "session": _longform_public_session(session),
    }


async def _longform_character_reference(
    session_id: str,
    character_name: str = Form(""),
    reference_image: UploadFile = File(...),
    reference_lock_mode: str = Form("strict"),
    request: Request = None,
):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    async with _longform_sessions_lock:
        _load_longform_sessions()
        session = dict(_longform_sessions.get(session_id) or {})
        if not session:
            raise HTTPException(404, "Long-form session not found")
        if str(session.get("user_id", "") or "").strip() != str(user.get("id", "") or "").strip():
            raise HTTPException(403, "Not your session")
        if not reference_image.content_type or not reference_image.content_type.startswith("image/"):
            raise HTTPException(400, "Reference file must be an image")
        raw = await reference_image.read()
        if not raw:
            raise HTTPException(400, "Reference image is empty")
        mime = reference_image.content_type or "image/png"
        data_url = f"data:{mime};base64,{base64.b64encode(raw).decode()}"
        session, created_reference = _persist_longform_character_reference(
            session,
            name=character_name,
            reference_image_url=data_url,
            reference_lock_mode=reference_lock_mode,
        )
        session["updated_at"] = time.time()
        _longform_sessions[session_id] = session
        _save_longform_sessions()
    return {
        "ok": True,
        "session": _longform_public_session(session),
        "character_reference": _longform_character_reference_public_view(created_reference),
    }


async def _longform_scene_assignment(
    session_id: str,
    req: LongFormSceneAssignmentRequest,
    request: Request = None,
):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    async with _longform_sessions_lock:
        _load_longform_sessions()
        session = dict(_longform_sessions.get(session_id) or {})
        if not session:
            raise HTTPException(404, "Long-form session not found")
        if str(session.get("user_id", "") or "").strip() != str(user.get("id", "") or "").strip():
            raise HTTPException(403, "Not your session")
        chapters = list(session.get("chapters") or [])
        chapter_index = int(req.chapter_index)
        if chapter_index < 0 or chapter_index >= len(chapters):
            raise HTTPException(400, "Invalid chapter index")
        character_id = str(req.character_id or "").strip()
        refs_by_id = {
            str((item or {}).get("character_id", "") or "").strip(): dict(item or {})
            for item in list(session.get("character_references") or [])
            if isinstance(item, dict) and str((item or {}).get("character_id", "") or "").strip()
        }
        if character_id and character_id not in refs_by_id:
            raise HTTPException(400, "Character reference not found")
        chapter = dict(chapters[chapter_index] or {})
        scenes = list(chapter.get("scenes") or [])
        target_scene_index = -1
        target_scene_num = int(req.scene_num or 0)
        for idx, raw_scene in enumerate(scenes):
            scene_num = int((dict(raw_scene or {})).get("scene_num", idx + 1) or (idx + 1))
            if scene_num == target_scene_num:
                target_scene_index = idx
                break
        if target_scene_index < 0:
            raise HTTPException(400, "Scene not found")
        scene = dict(scenes[target_scene_index] or {})
        scene["assigned_character_id"] = character_id
        scene["assigned_character_ids"] = [character_id] if character_id else []
        scene["assigned_character_name"] = str((refs_by_id.get(character_id) or {}).get("name", "") or "").strip()
        if character_id:
            scene["image_status"] = "error"
            scene["image_error"] = "Character assignment changed. Regenerate this chapter to refresh the preview."
        scenes[target_scene_index] = scene
        chapter["scenes"] = scenes
        chapters[chapter_index] = chapter
        session["chapters"] = chapters
        session["updated_at"] = time.time()
        _longform_sessions[session_id] = session
        _save_longform_sessions()
    return {
        "ok": True,
        "session": _longform_public_session(session),
        "chapter": dict(chapter or {}),
        "scene": dict(scene or {}),
    }


async def _longform_reference_file(filename: str):
    safe = os.path.basename(filename)
    if not safe or safe != filename:
        raise HTTPException(400, "Invalid filename")
    path = TEMP_DIR / "longform_references" / safe
    if not path.exists():
        raise HTTPException(404, "Reference image not found")
    media_type = "image/png"
    if path.suffix.lower() in {".jpg", ".jpeg"}:
        media_type = "image/jpeg"
    elif path.suffix.lower() == ".webp":
        media_type = "image/webp"
    return FileResponse(str(path), media_type=media_type, filename=safe)


async def _longform_session_status(session_id: str, request: Request = None):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _longform_owner_beta_enabled(user):
        raise HTTPException(403, "Long-form owner beta is restricted")
    should_resume = False
    should_try_finalize = False
    now = time.time()
    async with _longform_sessions_lock:
        _load_longform_sessions()
        session = _longform_sessions.get(session_id)
        if isinstance(session, dict):
            session_status = str(session.get("status", "") or "")
            chapters = list(session.get("chapters") or [])
            in_progress_idx = -1
            in_progress_status = ""
            for idx, chapter in enumerate(chapters):
                status = str((chapter or {}).get("status", "") or "")
                if status in {"draft_generating", "draft_generating_images"}:
                    in_progress_idx = idx
                    in_progress_status = status
                    break

            stalled_sec = max(0.0, now - float(session.get("updated_at", 0) or 0))
            if in_progress_idx >= 0:
                chapter_live = dict(chapters[in_progress_idx] or {})
                chapter_scenes = list(chapter_live.get("scenes") or [])
                scene_count = len(chapter_scenes)
                # Recover from crash/restart if a generation task died before scripts/scenes were persisted.
                if in_progress_status == "draft_generating" and scene_count <= 0 and stalled_sec > 180.0:
                    chapter_live["status"] = "awaiting_previous_approval"
                    chapter_live["last_error"] = "Recovered from stalled generation task; chapter resumed."
                    chapters[in_progress_idx] = chapter_live
                    session["chapters"] = chapters
                    session["status"] = "draft_review"
                    progress = dict(session.get("draft_progress") or {})
                    progress["stage"] = "auto_resume_after_stall"
                    progress["preview_scene_generated"] = 0
                    session["draft_progress"] = progress
                    session["updated_at"] = now
                    _save_longform_sessions()
                    should_resume = True
                # Recover from stalled preview-image generation so owner can approve/regenerate and continue.
                elif in_progress_status == "draft_generating_images" and scene_count > 0 and stalled_sec > 180.0:
                    ready_count = 0
                    auto_pipeline = _bool_from_any(session.get("auto_pipeline"), False)
                    for idx, raw_scene in enumerate(chapter_scenes):
                        scene = dict(raw_scene or {})
                        has_img = bool(str(scene.get("image_url", "") or "").strip())
                        if has_img:
                            ready_count += 1
                        else:
                            status = str(scene.get("image_status", "") or "")
                            if status != "error":
                                scene["image_status"] = "error"
                                scene["image_error"] = "Preview generation stalled; regenerate chapter to refresh this scene."
                            chapter_scenes[idx] = scene

                    missing_count = max(0, scene_count - ready_count)
                    chapter_live["scenes"] = chapter_scenes
                    chapter_live["status"] = "approved" if (auto_pipeline and missing_count == 0) else "pending_review"
                    chapter_live["last_error"] = (
                        f"Recovered from stalled preview generation ({ready_count}/{scene_count} ready). "
                        f"{missing_count} previews missing; regenerate chapter if needed."
                        if missing_count > 0
                        else "Recovered from stalled preview generation; chapter moved to review."
                    )
                    chapters[in_progress_idx] = chapter_live
                    session["chapters"] = chapters
                    session["status"] = "draft_review"
                    progress = dict(session.get("draft_progress") or {})
                    progress["stage"] = "auto_resume_after_preview_stall"
                    progress["preview_scene_total"] = int(scene_count)
                    progress["preview_scene_generated"] = int(ready_count)
                    progress["generated_chapters"] = int(_longform_generated_chapter_count(chapters))
                    progress["approved_chapters"] = int(_longform_approved_chapter_count(chapters))
                    session["draft_progress"] = progress
                    session["updated_at"] = now
                    _save_longform_sessions()
                    should_resume = bool(auto_pipeline and missing_count == 0)
            elif session_status != "bootstrapping" and any(str((c or {}).get("status", "") or "") == "awaiting_previous_approval" for c in chapters):
                should_resume = True
            progress_stage = str((dict(session.get("draft_progress") or {})).get("stage", "") or "").strip().lower()
            if progress_stage == "waiting_for_shortform_capacity":
                should_resume = True
            if progress_stage == "awaiting_render_capacity" and _longform_review_state(session).get("all_approved", False):
                should_try_finalize = True
    if not session:
        raise HTTPException(404, "Long-form session not found")
    if str(session.get("user_id", "") or "") != str(user.get("id", "") or ""):
        raise HTTPException(403, "Forbidden")
    if should_resume:
        await _queue_next_longform_chapter_if_ready(session_id)
        async with _longform_sessions_lock:
            _load_longform_sessions()
            session = _longform_sessions.get(session_id) or session
    if should_try_finalize:
        await _auto_finalize_longform_session(session_id)
        async with _longform_sessions_lock:
            _load_longform_sessions()
            session = _longform_sessions.get(session_id) or session
    job_id = str(session.get("job_id", "") or "")
    job = jobs.get(job_id, {}) if job_id else {}
    if job_id:
        persisted = await get_persisted_job_state(job_id)
        if isinstance(persisted, dict) and persisted:
            jobs[job_id] = persisted
            job = persisted
        # If worker failed before it could write session state, prevent a
        # permanently stuck "rendering" session in the UI.
        if str((job or {}).get("status", "") or "") == "error":
            async with _longform_sessions_lock:
                _load_longform_sessions()
                session_live = _longform_sessions.get(session_id)
                if isinstance(session_live, dict) and str(session_live.get("status", "") or "") == "rendering":
                    session_live["status"] = "error"
                    session_live["updated_at"] = time.time()
                    _save_longform_sessions()
                    session = session_live
    return {"session": _longform_public_session(session), "job": job}


async def _list_longform_sessions(request: Request = None, limit: int = 25):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _longform_owner_beta_enabled(user):
        raise HTTPException(403, "Long-form owner beta is restricted")

    limit = max(1, min(100, int(limit or 25)))
    user_id = str(user.get("id", "") or "")
    async with _longform_sessions_lock:
        _load_longform_sessions()
        sessions = [
            _longform_session_summary(s)
            for s in list(_longform_sessions.values())
            if str((s or {}).get("user_id", "") or "") == user_id
        ]
    sessions.sort(key=lambda s: float(s.get("updated_at", 0) or 0), reverse=True)
    return {"sessions": sessions[:limit]}


async def _longform_preview_file(filename: str):
    safe = os.path.basename(filename)
    if not safe or safe != filename:
        raise HTTPException(400, "Invalid filename")
    path = LONGFORM_PREVIEW_DIR / safe
    if not path.exists():
        raise HTTPException(404, "Preview not found")
    suffix = path.suffix.lower()
    media_map = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
    }
    return FileResponse(str(path), media_type=media_map.get(suffix, "application/octet-stream"), filename=safe)


async def _longform_chapter_action(session_id: str, req: LongFormChapterActionRequest, request: Request = None):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _longform_owner_beta_enabled(user):
        raise HTTPException(403, "Long-form owner beta is restricted")
    action = str(req.action or "").strip().lower()
    if action not in {"approve", "regenerate"}:
        raise HTTPException(400, "action must be approve or regenerate")

    should_queue_next = False
    async with _longform_sessions_lock:
        _load_longform_sessions()
        session = _longform_sessions.get(session_id)
        if not session:
            raise HTTPException(404, "Long-form session not found")
        if str(session.get("user_id", "") or "") != str(user.get("id", "") or ""):
            raise HTTPException(403, "Forbidden")
        chapters = list(session.get("chapters") or [])
        chapter_index = int(req.chapter_index)
        if chapter_index < 0 or chapter_index >= len(chapters):
            raise HTTPException(400, "Invalid chapter index")
        chapter = dict(chapters[chapter_index] or {})
        chapter_status = str(chapter.get("status", "") or "")
        if action == "approve":
            if chapter_status in {"awaiting_previous_approval", "draft_generating"}:
                raise HTTPException(400, "This chapter is not ready for approval yet")
            chapter["status"] = "approved"
            chapter["last_error"] = ""
            chapters[chapter_index] = chapter
            session["chapters"] = chapters
            session["status"] = "draft_review"
            progress = dict(session.get("draft_progress") or {})
            session["draft_progress"] = {
                "total_chapters": int(len(chapters)),
                "generated_chapters": int(_longform_generated_chapter_count(chapters)),
                "approved_chapters": int(_longform_approved_chapter_count(chapters)),
                "failed_chapters": int(progress.get("failed_chapters", 0) or 0),
                "stage": "awaiting_owner_approval",
            }
            session["updated_at"] = time.time()
            _save_longform_sessions()
            should_queue_next = True
        else:
            if chapter_status in {"awaiting_previous_approval", "draft_generating"}:
                raise HTTPException(400, "This chapter is not ready for regeneration yet")

            # Mark in-progress before releasing lock for generation.
            chapter["status"] = "regenerating"
            chapter["retry_count"] = int(chapter.get("retry_count", 0) or 0) + 1
            chapters[chapter_index] = chapter
            session["chapters"] = chapters
            session["updated_at"] = time.time()
            _save_longform_sessions()
            session_copy = dict(session)

    if should_queue_next:
        await _queue_next_longform_chapter_if_ready(session_id)
        async with _longform_sessions_lock:
            session_live = _longform_sessions.get(session_id)
        if not session_live:
            raise HTTPException(404, "Long-form session not found")
        chapter_live = dict((list(session_live.get("chapters") or [])[chapter_index]) or {})
        return {"session": _longform_public_session(session_live), "chapter": chapter_live}

    edit_blueprint = dict(session_copy.get("edit_blueprint") or {})
    try:
        refreshed_edit_blueprint = await _refresh_longform_edit_blueprint_for_session(session_copy)
        if refreshed_edit_blueprint:
            edit_blueprint = dict(refreshed_edit_blueprint or {})
    except Exception as e:
        log.warning(f"[longform:{session_id}] regenerate blueprint refresh failed: {e}")
    regenerated = await _generate_longform_chapter(
        template=str(session_copy.get("template", "story") or "story"),
        topic=str(session_copy.get("topic", "") or ""),
        input_title=str(session_copy.get("input_title", "") or ""),
        input_description=str(session_copy.get("input_description", "") or ""),
        format_preset=str(session_copy.get("format_preset", "explainer") or "explainer"),
        chapter_index=chapter_index,
        chapter_count=len(list(session_copy.get("chapters") or [])),
        chapter_target_sec=float(chapter.get("target_sec", 70) or 70),
        language=_normalize_longform_language(session_copy.get("language", "en")),
        brand_slot=str(chapter.get("brand_slot", "") or ""),
        fix_note=_catalyst_default_fix_note_for_session(session_copy, chapter_index, str(req.reason or "").strip()),
        source_context=str((dict(session_copy.get("metadata_pack") or {})).get("source_context", "") or ""),
        strategy_notes=_marketing_doctrine_text(str(session_copy.get("strategy_notes", "") or "").strip()),
        edit_blueprint=edit_blueprint,
        chapter_blueprint=_catalyst_chapter_blueprint_for_index(edit_blueprint, chapter_index),
    )
    regenerated = await _longform_attach_scene_previews(
        session_id=session_id,
        template=str(session_copy.get("template", "story") or "story"),
        chapter=regenerated,
        resolution=str(session_copy.get("resolution", "720p_landscape") or "720p_landscape"),
        format_preset=str(session_copy.get("format_preset", "") or ""),
    )
    auto_pipeline = _bool_from_any(session_copy.get("auto_pipeline"), False)
    regenerated["status"] = "approved" if auto_pipeline else "pending_review"
    regenerated["retry_count"] = int(chapter.get("retry_count", 1) or 1)
    async with _longform_sessions_lock:
        session_live = _longform_sessions.get(session_id)
        if not session_live:
            raise HTTPException(404, "Long-form session not found")
        chapters_live = list(session_live.get("chapters") or [])
        chapters_live[chapter_index] = regenerated
        session_live["chapters"] = chapters_live
        session_live["edit_blueprint"] = edit_blueprint
        session_live["status"] = "draft_review"
        progress = dict(session_live.get("draft_progress") or {})
        session_live["draft_progress"] = {
            "total_chapters": int(len(chapters_live)),
            "generated_chapters": int(_longform_generated_chapter_count(chapters_live)),
            "approved_chapters": int(_longform_approved_chapter_count(chapters_live)),
            "failed_chapters": int(progress.get("failed_chapters", 0) or 0),
            "stage": "auto_pipeline_progress" if auto_pipeline else "awaiting_owner_approval",
        }
        session_live["updated_at"] = time.time()
        live_metadata_pack = dict(session_live.get("metadata_pack") or {})
        live_metadata_pack["catalyst_channel_memory"] = _coerce_documentary_longform_channel_memory(
            dict(live_metadata_pack.get("youtube_channel") or {}),
            dict(live_metadata_pack.get("catalyst_channel_memory") or session_live.get("channel_memory") or {}),
            format_preset=str(session_live.get("format_preset", "") or "documentary"),
            topic=str(session_live.get("topic", "") or ""),
            input_title=str(session_live.get("input_title", "") or ""),
        )
        session_live["metadata_pack"] = live_metadata_pack
        session_live["channel_memory"] = _coerce_documentary_longform_channel_memory(
            dict(live_metadata_pack.get("youtube_channel") or {}),
            dict(session_live.get("channel_memory") or {}),
            format_preset=str(session_live.get("format_preset", "") or "documentary"),
            topic=str(session_live.get("topic", "") or ""),
            input_title=str(session_live.get("input_title", "") or ""),
        )
        _save_longform_sessions()
    if auto_pipeline:
        await _queue_next_longform_chapter_if_ready(session_id)
    async with _longform_sessions_lock:
        session_latest = _longform_sessions.get(session_id)
    if not session_latest:
        raise HTTPException(404, "Long-form session not found")
    return {"session": _longform_public_session(session_latest), "chapter": regenerated}


async def _longform_resolve_error(session_id: str, req: LongFormResolveErrorRequest, request: Request = None):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _longform_owner_beta_enabled(user):
        raise HTTPException(403, "Long-form owner beta is restricted")
    async with _longform_sessions_lock:
        _load_longform_sessions()
        session = _longform_sessions.get(session_id)
        if not session:
            raise HTTPException(404, "Long-form session not found")
        if str(session.get("user_id", "") or "") != str(user.get("id", "") or ""):
            raise HTTPException(403, "Forbidden")
        paused = dict(session.get("paused_error") or {})
        if not paused:
            raise HTTPException(400, "Session is not paused for manual fix")
        chapter_index = int(req.chapter_index)
        chapters = list(session.get("chapters") or [])
        if chapter_index < 0 or chapter_index >= len(chapters):
            raise HTTPException(400, "Invalid chapter index")
        chapter = dict(chapters[chapter_index] or {})
        session_copy = dict(session)

    edit_blueprint = dict(session_copy.get("edit_blueprint") or {})
    try:
        refreshed_edit_blueprint = await _refresh_longform_edit_blueprint_for_session(session_copy)
        if refreshed_edit_blueprint:
            edit_blueprint = dict(refreshed_edit_blueprint or {})
    except Exception as e:
        log.warning(f"[longform:{session_id}] resolve-error blueprint refresh failed: {e}")
    regenerated = await _generate_longform_chapter(
        template=str(session_copy.get("template", "story") or "story"),
        topic=str(session_copy.get("topic", "") or ""),
        input_title=str(session_copy.get("input_title", "") or ""),
        input_description=str(session_copy.get("input_description", "") or ""),
        format_preset=str(session_copy.get("format_preset", "explainer") or "explainer"),
        chapter_index=chapter_index,
        chapter_count=len(list(session_copy.get("chapters") or [])),
        chapter_target_sec=float(chapter.get("target_sec", 70) or 70),
        language=_normalize_longform_language(session_copy.get("language", "en")),
        brand_slot=str(chapter.get("brand_slot", "") or ""),
        fix_note=_catalyst_default_fix_note_for_session(session_copy, chapter_index, str(req.fix_note or "").strip()),
        source_context=str((dict(session_copy.get("metadata_pack") or {})).get("source_context", "") or ""),
        strategy_notes=_marketing_doctrine_text(str(session_copy.get("strategy_notes", "") or "").strip()),
        edit_blueprint=edit_blueprint,
        chapter_blueprint=_catalyst_chapter_blueprint_for_index(edit_blueprint, chapter_index),
    )
    regenerated = await _longform_attach_scene_previews(
        session_id=session_id,
        template=str(session_copy.get("template", "story") or "story"),
        chapter=regenerated,
        resolution=str(session_copy.get("resolution", "720p_landscape") or "720p_landscape"),
        format_preset=str(session_copy.get("format_preset", "") or ""),
    )
    auto_pipeline = _bool_from_any(session_copy.get("auto_pipeline"), False)
    regenerated["status"] = "approved" if (_bool_from_any(req.force_accept, False) or auto_pipeline) else "pending_review"
    regenerated["retry_count"] = int(chapter.get("retry_count", 0) or 0) + 1
    force_accept = _bool_from_any(req.force_accept, False)
    async with _longform_sessions_lock:
        session_live = _longform_sessions.get(session_id)
        if not session_live:
            raise HTTPException(404, "Long-form session not found")
        chapters_live = list(session_live.get("chapters") or [])
        chapters_live[chapter_index] = regenerated
        session_live["chapters"] = chapters_live
        session_live["edit_blueprint"] = edit_blueprint
        session_live["status"] = "draft_review"
        session_live["paused_error"] = None
        progress = dict(session_live.get("draft_progress") or {})
        session_live["draft_progress"] = {
            "total_chapters": int(len(chapters_live)),
            "generated_chapters": int(_longform_generated_chapter_count(chapters_live)),
            "approved_chapters": int(_longform_approved_chapter_count(chapters_live)),
            "failed_chapters": int(progress.get("failed_chapters", 0) or 0),
            "stage": "auto_pipeline_progress" if auto_pipeline else "awaiting_owner_approval",
        }
        session_live["updated_at"] = time.time()
        live_metadata_pack = dict(session_live.get("metadata_pack") or {})
        live_metadata_pack["catalyst_channel_memory"] = _coerce_documentary_longform_channel_memory(
            dict(live_metadata_pack.get("youtube_channel") or {}),
            dict(live_metadata_pack.get("catalyst_channel_memory") or session_live.get("channel_memory") or {}),
            format_preset=str(session_live.get("format_preset", "") or "documentary"),
            topic=str(session_live.get("topic", "") or ""),
            input_title=str(session_live.get("input_title", "") or ""),
        )
        session_live["metadata_pack"] = live_metadata_pack
        session_live["channel_memory"] = _coerce_documentary_longform_channel_memory(
            dict(live_metadata_pack.get("youtube_channel") or {}),
            dict(session_live.get("channel_memory") or {}),
            format_preset=str(session_live.get("format_preset", "") or "documentary"),
            topic=str(session_live.get("topic", "") or ""),
            input_title=str(session_live.get("input_title", "") or ""),
        )
        _save_longform_sessions()
    if force_accept or auto_pipeline:
        await _queue_next_longform_chapter_if_ready(session_id)
    async with _longform_sessions_lock:
        session_latest = _longform_sessions.get(session_id)
    if not session_latest:
        raise HTTPException(404, "Long-form session not found")
    return {"session": _longform_public_session(session_latest), "chapter": regenerated}


async def _start_longform_finalize_internal(session_id: str, acting_user: Optional[dict] = None) -> str:
    acting_user_id = str((acting_user or {}).get("id", "") or "")
    acting_is_admin = bool(_is_admin_user(acting_user)) if acting_user else False
    shortform_priority = await _shortform_priority_snapshot()
    if shortform_priority.get("priority_active"):
        await _mark_longform_waiting_for_shortform_capacity(
            session_id,
            stage="awaiting_render_capacity",
            snapshot=shortform_priority,
        )
        raise HTTPException(
            409,
            "Short-form generation is using shared render capacity right now. Long Form finalize is waiting so the public short-form lanes stay responsive.",
        )
    async with _longform_sessions_lock:
        _load_longform_sessions()
        session = _longform_sessions.get(session_id)
        if not session:
            raise HTTPException(404, "Long-form session not found")
        if acting_user_id and str(session.get("user_id", "") or "") != acting_user_id:
            raise HTTPException(403, "Forbidden")
        if session.get("paused_error"):
            raise HTTPException(400, "Session is paused due to an unresolved chapter error")
        review = _longform_review_state(session)
        if not review.get("all_approved", False):
            raise HTTPException(
                400,
                f"All chapters must be approved before finalize ({review.get('approved_chapters', 0)}/{review.get('total_chapters', 0)} approved).",
            )
        catalyst_preflight = _catalyst_longform_preflight(session)
        if catalyst_preflight.get("status") == "blocked":
            suggested_fix_note = _catalyst_default_fix_note_for_session(session, 0, "")
            session["paused_error"] = {
                "stage": "catalyst_preflight",
                "error": str(catalyst_preflight.get("summary", "") or "Catalyst preflight blocked finalize."),
                "blockers": list(catalyst_preflight.get("blockers") or []),
                "chapter_index": 0,
                "suggested_fix_note": suggested_fix_note,
            }
            progress = dict(session.get("draft_progress") or {})
            progress["stage"] = "catalyst_preflight_blocked"
            session["draft_progress"] = progress
            session["status"] = "draft_review"
            session["updated_at"] = time.time()
            _save_longform_sessions()
            blocker_summary = "; ".join([str(v).strip() for v in list(catalyst_preflight.get("blockers") or []) if str(v).strip()][:3])
            raise HTTPException(
                409,
                f"Catalyst preflight blocked finalize. {blocker_summary or str(catalyst_preflight.get('summary', '') or '')}".strip(),
            )
        if isinstance(session.get("paused_error"), dict) and str((session.get("paused_error") or {}).get("stage", "") or "") == "catalyst_preflight":
            session["paused_error"] = None
        current_job_id = str(session.get("job_id", "") or "").strip()
        if current_job_id and str(session.get("status", "") or "") == "rendering":
            return current_job_id
        job_id = f"{int(time.time())}_{random.randint(1000, 9999)}"
        jobs[job_id] = {
            "status": "queued",
            "progress": 0,
            "template": str(session.get("template", "story") or "story"),
            "topic": str(session.get("topic", "") or ""),
            "lane": "longform",
            "mode": "longform_finalize",
            "resolution": str(session.get("resolution", "720p_landscape") or "720p_landscape"),
            "plan": "pro",
            "user_id": str(session.get("user_id", "") or acting_user_id),
            "created_at": time.time(),
            "type": "longform",
            "target_minutes": float(session.get("target_minutes", 0) or 0),
            "animation_enabled": bool(session.get("animation_enabled", True)),
            "story_animation_enabled": bool(session.get("animation_enabled", True)),
            "voice_id": "",
            "voice_speed": 1.0,
            "pacing_mode": "standard",
            "subtitles_enabled": True,
            "reference_lock_mode": "strict",
            "transition_style": "smooth",
            "micro_escalation_mode": False,
            "credit_charged": False,
            "credit_source": "admin",
            "credit_cost": 0,
            "billing_source": "owner_override" if acting_is_admin or _bool_from_any(session.get("owner_override"), False) else "workspace_access",
            "credit_month_key": _month_key(),
            "credit_refunded": False,
        }
        session["job_id"] = job_id
        session["status"] = "rendering"
        session["updated_at"] = time.time()
        _save_longform_sessions()

    _job_diag_init(job_id, "longform")
    asyncio.create_task(_run_longform_pipeline_isolated(job_id, session_id))
    return job_id


async def _auto_finalize_longform_session(session_id: str) -> None:
    try:
        await _start_longform_finalize_internal(session_id, None)
    except Exception as e:
        log.error(f"[longform:{session_id}] auto-finalize failed: {e}", exc_info=True)
        async with _longform_sessions_lock:
            session_live = _longform_sessions.get(session_id)
            if isinstance(session_live, dict) and str(session_live.get("status", "") or "") != "complete":
                status_code = int(getattr(e, "status_code", 0) or 0)
                error_text = str(getattr(e, "detail", "") or str(e))
                if status_code in {400, 409} and "Catalyst preflight blocked finalize" in error_text:
                    suggested_fix_note = _catalyst_default_fix_note_for_session(session_live, 0, "")
                    session_live["status"] = "draft_review"
                    session_live["paused_error"] = {
                        "stage": "catalyst_preflight",
                        "error": error_text,
                        "chapter_index": 0,
                        "suggested_fix_note": suggested_fix_note,
                    }
                    progress = dict(session_live.get("draft_progress") or {})
                    progress["stage"] = "catalyst_preflight_blocked"
                    session_live["draft_progress"] = progress
                else:
                    session_live["status"] = "error"
                    session_live["paused_error"] = {
                        "stage": "auto_finalize",
                        "error": str(e),
                    }
                session_live["updated_at"] = time.time()
                _save_longform_sessions()


async def _longform_finalize(session_id: str, background_tasks: BackgroundTasks, request: Request = None):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _longform_owner_beta_enabled(user):
        raise HTTPException(403, "Long-form owner beta is restricted")
    job_id = await _start_longform_finalize_internal(session_id, user)
    return {"job_id": job_id}


async def _longform_stop_session(session_id: str, request: Request = None):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _longform_owner_beta_enabled(user):
        raise HTTPException(403, "Long-form owner beta is restricted")

    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id:
        raise HTTPException(400, "Session id required")

    now_ts = time.time()
    stopped_session: dict | None = None
    active_job_id = ""
    async with _longform_sessions_lock:
        _load_longform_sessions()
        session_live = _longform_sessions.get(normalized_session_id)
        if not isinstance(session_live, dict):
            raise HTTPException(404, "Long-form session not found")
        if str((session_live or {}).get("user_id", "") or "").strip() != str(user.get("id", "") or "").strip():
            raise HTTPException(403, "You do not have access to this long-form session")

        active_job_id = str(session_live.get("job_id", "") or "").strip()
        session_live["stop_requested"] = True
        session_live["stop_requested_at"] = now_ts
        session_live["status"] = "stopped"
        session_live["job_id"] = ""
        progress = dict(session_live.get("draft_progress") or {})
        progress["stage"] = "stopped_by_owner"
        session_live["draft_progress"] = progress
        session_live["paused_error"] = {
            "stage": "stopped",
            "error": "Stopped by owner.",
        }

        chapters_live = list(session_live.get("chapters") or [])
        for idx, raw_chapter in enumerate(chapters_live):
            chapter = dict(raw_chapter or {})
            chapter_status = str(chapter.get("status", "") or "").strip().lower()
            if chapter_status in {"draft_generating", "draft_generating_images", "regenerating", "awaiting_previous_approval"}:
                chapter["status"] = "pending_review"
                if not str(chapter.get("last_error", "") or "").strip():
                    chapter["last_error"] = "Stopped by owner."
                chapters_live[idx] = chapter
        session_live["chapters"] = chapters_live
        session_live["updated_at"] = now_ts
        _longform_sessions[normalized_session_id] = session_live
        _save_longform_sessions()
        stopped_session = dict(session_live)

    await _mark_longform_job_cancelled(active_job_id, "Stopped by owner.")
    return {
        "ok": True,
        "stopped_session_id": normalized_session_id,
        "session": _longform_public_session(stopped_session or {}),
    }


async def _longform_ingest_outcome(session_id: str, req: CatalystOutcomeIngestRequest, request: Request = None):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _longform_owner_beta_enabled(user):
        raise HTTPException(403, "Long-form owner beta is restricted")
    user_id = str(user.get("id", "") or "").strip()
    async with _longform_sessions_lock:
        _load_longform_sessions()
        session = dict(_longform_sessions.get(session_id) or {})
    if not session:
        raise HTTPException(404, "Long-form session not found")
    if str(session.get("user_id", "") or "") != user_id:
        raise HTTPException(403, "Forbidden")

    channel_id = str(session.get("youtube_channel_id", "") or "").strip()
    normalized_video_url = _normalize_external_source_url(str((req or {}).video_url or "").strip())
    video_id = str((req or {}).video_id or "").strip() or _source_url_video_id(normalized_video_url)
    video_meta: dict = {}
    analytics_metrics: dict = {}
    if _bool_from_any(getattr(req, "auto_fetch_channel_metrics", True), True) and channel_id and video_id:
        async with _youtube_connections_lock:
            _load_youtube_connections()
            bucket = _youtube_bucket_for_user(user_id)
            record = dict((dict(bucket.get("channels") or {})).get(channel_id) or {})
        if record:
            try:
                access_token, refreshed_record = await _youtube_ensure_access_token(record)
                video_rows = await _youtube_fetch_videos(access_token, [video_id])
                video_meta = dict(video_rows[0] or {}) if video_rows else {}
                analytics_metrics = await _youtube_fetch_video_analytics(access_token, channel_id, video_id)
                async with _youtube_connections_lock:
                    _load_youtube_connections()
                    bucket = _youtube_bucket_for_user(user_id)
                    bucket_channels = dict(bucket.get("channels") or {})
                    if channel_id in bucket_channels:
                        bucket_channels[channel_id] = refreshed_record
                        bucket["channels"] = bucket_channels
                        _save_youtube_connections()
            except Exception as e:
                log.warning(f"[longform:{session_id}] outcome auto-fetch failed: {e}")

    outcome_record, session_live = await _persist_catalyst_outcome_for_session(
        session_id=session_id,
        user_id=user_id,
        session=session,
        req=req,
        video_meta=video_meta,
        analytics_metrics=analytics_metrics,
    )

    return {
        "ok": True,
        "outcome": outcome_record,
        "session": _longform_public_session(session_live),
    }


async def _longform_auto_ingest_outcome(session_id: str, req: CatalystAutoOutcomeHarvestRequest, request: Request = None):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _longform_owner_beta_enabled(user):
        raise HTTPException(403, "Long-form owner beta is restricted")
    user_id = str(user.get("id", "") or "").strip()
    async with _longform_sessions_lock:
        _load_longform_sessions()
        session = dict(_longform_sessions.get(session_id) or {})
    if not session:
        raise HTTPException(404, "Long-form session not found")
    if str(session.get("user_id", "") or "") != user_id:
        raise HTTPException(403, "Forbidden")

    channel_id = str(session.get("youtube_channel_id", "") or "").strip()
    if not channel_id:
        raise HTTPException(400, "Connect a YouTube channel before auto-pulling outcomes")

    normalized_video_url = _normalize_external_source_url(str((req or {}).video_url or "").strip())
    explicit_video_id = str((req or {}).video_id or "").strip() or _source_url_video_id(normalized_video_url)
    candidate_limit = max(3, min(25, int(getattr(req, "candidate_limit", 12) or 12)))

    async with _youtube_connections_lock:
        _load_youtube_connections()
        bucket = _youtube_bucket_for_user(user_id)
        record = dict((dict(bucket.get("channels") or {})).get(channel_id) or {})
    if not record:
        raise HTTPException(400, "Connected YouTube channel record was not found")

    matched_video: dict = {}
    analytics_metrics: dict = {}
    try:
        access_token, refreshed_record = await _youtube_ensure_access_token(record)
        if explicit_video_id:
            explicit_rows = await _youtube_fetch_videos(access_token, [explicit_video_id])
            matched_video = dict(explicit_rows[0] or {}) if explicit_rows else {}
        if not matched_video:
            recent_candidates = await _youtube_fetch_channel_search(access_token, channel_id, order="date", max_results=candidate_limit)
            top_candidates = await _youtube_fetch_channel_search(access_token, channel_id, order="viewCount", max_results=max(6, min(25, candidate_limit)))
            deduped_candidates: dict[str, dict] = {}
            for row in [*recent_candidates, *top_candidates]:
                candidate = dict(row or {})
                video_key = str(candidate.get("video_id", "") or "").strip()
                if video_key and video_key not in deduped_candidates:
                    deduped_candidates[video_key] = candidate
            matched_video = _match_published_video_to_longform_session(session, list(deduped_candidates.values()))
        if not matched_video:
            raise HTTPException(404, "Catalyst could not match a published video on the connected channel yet")
        matched_video["url"] = normalized_video_url or f"https://www.youtube.com/watch?v={str(matched_video.get('video_id', '') or '').strip()}"
        if _bool_from_any(getattr(req, "auto_fetch_channel_metrics", True), True):
            analytics_metrics = await _youtube_fetch_video_analytics(
                access_token,
                channel_id,
                str(matched_video.get("video_id", "") or "").strip(),
            )
        async with _youtube_connections_lock:
            _load_youtube_connections()
            bucket = _youtube_bucket_for_user(user_id)
            bucket_channels = dict(bucket.get("channels") or {})
            if channel_id in bucket_channels:
                bucket_channels[channel_id] = refreshed_record
                bucket["channels"] = bucket_channels
                _save_youtube_connections()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Automatic outcome pull failed: {_clip_text(str(e), 220)}")

    auto_req = _build_auto_outcome_request(
        session_snapshot=session,
        video_meta=matched_video,
        analytics_metrics=analytics_metrics,
    )
    outcome_record, session_live = await _persist_catalyst_outcome_for_session(
        session_id=session_id,
        user_id=user_id,
        session=session,
        req=auto_req,
        video_meta=matched_video,
        analytics_metrics=analytics_metrics,
    )
    return {
        "ok": True,
        "auto": True,
        "matched_video": matched_video,
        "outcome": outcome_record,
        "session": _longform_public_session(session_live),
    }


async def _creative_generate_script(req: GenerateRequest, request: Request = None):
    """Phase 1: Generate script + scenes for user review. Returns editable scene list."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _user_has_paid_access(user):
        raise HTTPException(402, "Active subscription required. Please choose a plan.")
    _ensure_template_allowed(req.template, user)
    quality_mode = _normalize_skeleton_quality_mode(req.quality_mode, template=req.template)
    mint_mode = _normalize_mint_mode(req.mint_mode, template=req.template)
    art_style = _normalize_art_style(req.art_style, template=req.template)
    image_model_id = _normalize_scene_image_model_id(getattr(req, "image_model_id", ""), template=req.template)
    video_model_id = _normalize_creative_video_model_id(getattr(req, "video_model_id", ""))
    user_plan, _plan_limits = _resolve_user_plan_for_limits(user)
    is_admin = user.get("email", "") in ADMIN_EMAILS
    billing_active = _billing_active_for_user(user)
    can_run, _source, _state = await _reserve_generation_credit(
        user,
        user_plan if not is_admin else "pro",
        billing_active,
        is_admin=is_admin,
        usage_kind="non_animated",
    )
    if not can_run:
        raise HTTPException(402, "Non-animated meter exhausted for this month. Please wait for renewal or upgrade plan.")
    cinematic_boost = _normalize_cinematic_boost(getattr(req, "cinematic_boost", True))
    reference_lock_mode = _normalize_reference_lock_mode(req.reference_lock_mode, default="strict")
    default_reference_url = _default_reference_for_template(req.template)
    try:
        reference_dna, reference_quality = await _extract_reference_profile(default_reference_url, req.template, reference_lock_mode)
    except Exception as e:
        log.warning(f"Creative script reference-profile setup failed for template={req.template}: {e}")
        reference_dna, reference_quality = {}, {}
    transition_style = _normalize_transition_style(req.transition_style)
    micro_escalation_mode = _normalize_micro_escalation_mode(req.micro_escalation_mode, template=req.template)
    if cinematic_boost:
        quality_mode = "cinematic"
        transition_style = "cinematic"
        micro_escalation_mode = True
        mint_mode = True
    if cinematic_boost:
        quality_mode = "cinematic"
        transition_style = "cinematic"
        micro_escalation_mode = True
        mint_mode = True
    voice_id = str(req.voice_id or "").strip()
    voice_speed = _normalize_voice_speed(req.voice_speed, default=1.0)
    pacing_mode = _normalize_pacing_mode(req.pacing_mode)
    animation_enabled = _bool_from_any(req.animation_enabled, _bool_from_any(req.story_animation_enabled, True))
    if not _creative_template_supports_voice_controls(req.template):
        voice_id = ""
        voice_speed = 1.0
        pacing_mode = "standard"
    youtube_channel_id = str(getattr(req, "youtube_channel_id", "") or "").strip()
    trend_hunt_enabled = _bool_from_any(getattr(req, "trend_hunt_enabled", False), False)
    lang_name = SUPPORTED_LANGUAGES.get(req.language, {}).get("name", "English")
    lang_instruction = ""
    if req.language != "en":
        lang_instruction = f"\n\nIMPORTANT: Write ALL narration text in {lang_name}. The visual_description fields should remain in English (for image generation), but ALL narration/voiceover text MUST be in {lang_name}."
    script_to_short_mode = str(req.mode or "").strip().lower() == "script_to_short"
    if script_to_short_mode and not SCRIPT_TO_SHORT_ENABLED:
        raise HTTPException(503, "Script to Short is temporarily disabled")
    script_to_short_instruction = ""
    if script_to_short_mode:
        scene_range = "12-15" if req.template == "story" else "10-14"
        script_to_short_instruction = (
            "\n\nSCRIPT-TO-SHORT MODE: The user input is already a full script. "
            "Do not invent a different story. Keep the original intent and wording as much as possible while adapting for timing. "
            f"Split into {scene_range} scenes with clear visual progression and strong retention pacing. "
            "Process the script in strict chronological order from the first sentence to the final payoff. "
            "Do not skip late beats, collapse major transitions into generic filler, or rewrite the source into a different premise. "
            "Each scene must map to one consecutive beat from the exact source script and preserve the same subject, setting, action, and emotional turn implied by that beat. "
            "If the script names a person, object, outfit, location, mechanism, number, or timeframe, keep it in the matching scene instead of generalizing it away. "
            "Do not force a single unchanged main character in every scene; match subjects to each script beat. "
            "Each scene must have concise narration and a cinematic visual_description that can be directly rendered. "
            "Make the visual_description fields self-contained, specific, and editable so users can regenerate each scene from the prompt alone. "
            "Every visual_description should read like a production-ready image prompt with the exact subject, setting, action, framing, lighting, and key props required by that script beat."
        )
    try:
        catalyst_shorts_instructions = await _build_shorts_catalyst_extra_instructions(
            user,
            req.template,
            preferred_channel_id=youtube_channel_id,
            topic=req.prompt,
            trend_hunt_enabled=trend_hunt_enabled,
        )
    except Exception as e:
        log.warning(f"Creative script Catalyst setup failed for template={req.template}: {e}")
        catalyst_shorts_instructions = ""
    persisted_public_shorts: dict = {}
    channel_context: dict = {}
    trend_titles: list[str] = []
    try:
        if youtube_channel_id or trend_hunt_enabled:
            channel_context = await _youtube_selected_channel_context(user, youtube_channel_id)
        if trend_hunt_enabled:
            trend_query = _build_shorts_trend_query(req.template, req.prompt, channel_context, {})
            trend_titles = await _youtube_fetch_public_trend_titles(trend_query, max_results=6)
    except Exception as e:
        log.warning(f"Creative script YouTube trend setup failed for template={req.template}: {e}")
        channel_context = {}
        trend_titles = []
    try:
        persisted_public_shorts = await _persist_public_shorts_playbook_memory(
            user=user,
            template=req.template,
            topic=req.prompt,
            preferred_channel_id=youtube_channel_id,
            trend_hunt_enabled=trend_hunt_enabled,
            channel_context=channel_context,
        )
        if not channel_context:
            channel_context = dict(persisted_public_shorts.get("channel_context") or {})
    except Exception as e:
        log.warning(f"Creative script shorts benchmark persistence failed for template={req.template}: {e}")
    resolution = _normalize_output_resolution(req.resolution, priority_allowed=False)
    try:
        script_data = await generate_script(
            req.template,
            req.prompt,
            extra_instructions=(lang_instruction + script_to_short_instruction + (("\n\n" + catalyst_shorts_instructions) if catalyst_shorts_instructions else "")),
        )
    except Exception as e:
        status_code = getattr(getattr(e, "response", None), "status_code", None)
        if req.template == "skeleton":
            public_shorts_playbook: dict = dict(persisted_public_shorts.get("playbook") or {})
            memory_public = dict(persisted_public_shorts.get("memory_public") or {})
            try:
                if not public_shorts_playbook:
                    public_shorts_playbook = await _build_shorts_public_reference_playbook(
                        req.template,
                        req.prompt,
                        channel_context,
                        {},
                        memory_public=memory_public,
                        trend_hunt_enabled=trend_hunt_enabled,
                    )
            except Exception as inner_exc:
                log.warning(f"Skeleton public shorts playbook build failed during creative fallback: {inner_exc}")
                public_shorts_playbook = dict(persisted_public_shorts.get("playbook") or {})
            log.warning(
                "Skeleton script generation failed, using local Catalyst fallback "
                f"(status={status_code}, trend_hunt={trend_hunt_enabled}): {e}"
            )
            script_data = _build_skeleton_shorts_local_fallback(
                req.prompt,
                channel_context=channel_context,
                trend_titles=trend_titles,
                public_shorts_playbook=public_shorts_playbook,
                script_to_short_mode=script_to_short_mode,
                trend_hunt_enabled=trend_hunt_enabled,
            )
        elif isinstance(e, httpx.HTTPStatusError) and status_code == 429:
            raise HTTPException(
                503,
                "Script generation is temporarily rate-limited upstream. Retry in a minute.",
            ) from e
        elif isinstance(e, httpx.HTTPStatusError):
            raise HTTPException(
                502,
                "Script generation failed upstream. Retry shortly.",
            ) from e
        else:
            raise HTTPException(
                502,
                "Script generation failed. Retry shortly.",
            ) from e
    scenes = _normalize_scenes_for_render(script_data.get("scenes", []))
    scenes = _apply_template_scene_constraints(scenes, req.template, quality_mode=quality_mode)
    scenes = _apply_mint_scene_compiler(scenes, req.template, mint_mode=mint_mode)
    if not scenes:
        raise HTTPException(500, "Script generation returned no scenes")
    session_id = f"cs_{int(time.time())}_{random.randint(1000, 9999)}"
    async with _creative_sessions_lock:
        _creative_sessions[session_id] = {
            "session_id": session_id,
            "user_id": user["id"],
            "template": req.template,
            "topic": req.prompt,
            "resolution": resolution,
            "language": req.language,
            "script_data": script_data,
            "scenes": scenes,
            "scene_images": {},
            "quality_mode": quality_mode,
            "mint_mode": mint_mode,
            "art_style": art_style,
            "image_model_id": image_model_id,
            "video_model_id": video_model_id,
            "cinematic_boost": cinematic_boost,
            "transition_style": transition_style,
            "micro_escalation_mode": micro_escalation_mode,
            "voice_id": voice_id,
            "voice_speed": voice_speed,
            "pacing_mode": pacing_mode,
            "animation_enabled": animation_enabled,
            "story_animation_enabled": animation_enabled if req.template == "story" else True,
            "reference_image_url": default_reference_url,
            "reference_lock_mode": reference_lock_mode,
            "reference_dna": reference_dna,
            "reference_quality": reference_quality,
            "reference_image_uploaded": False,
            "rolling_reference_image_url": default_reference_url,
            "youtube_channel_id": youtube_channel_id,
            "trend_hunt_enabled": trend_hunt_enabled,
            "catalyst_shorts_instructions": catalyst_shorts_instructions,
            "channel_memory": dict(persisted_public_shorts.get("memory_public") or {}),
            "catalyst_public_shorts_playbook": dict(persisted_public_shorts.get("playbook") or {}),
            "prompt_passthrough": True,
            "created_at": time.time(),
        }
        try:
            _save_creative_sessions_to_disk()
        except Exception as e:
            log.warning(f"Creative script session persistence failed for {session_id}: {e}")
    return {
        "session_id": session_id,
        "title": script_data.get("title", req.prompt),
        "scenes": [
            {
                "index": i,
                "narration": s.get("narration", ""),
                "visual_description": s.get("visual_description", ""),
                "duration_sec": s.get("duration_sec", 5),
            }
            for i, s in enumerate(scenes)
        ],
        "quality_mode": quality_mode,
        "mint_mode": mint_mode,
        "art_style": art_style,
        "image_model_id": image_model_id,
        "video_model_id": video_model_id,
        "cinematic_boost": cinematic_boost,
        "transition_style": transition_style,
        "micro_escalation_mode": micro_escalation_mode,
        "voice_id": voice_id,
        "voice_speed": voice_speed,
        "pacing_mode": pacing_mode,
        "animation_enabled": animation_enabled,
        "reference_lock_mode": reference_lock_mode,
        "youtube_channel_id": youtube_channel_id,
        "trend_hunt_enabled": trend_hunt_enabled,
        "prompt_passthrough": True,
        "mode": "script_to_short" if script_to_short_mode else "creative",
    }


async def _creative_create_session(body: dict, request: Request = None):
    """Create an empty creative session (no script generation). User builds scenes manually."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _user_has_paid_access(user):
        raise HTTPException(402, "Active subscription required. Please choose a plan.")
    template = body.get("template", "skeleton")
    quality_mode = _normalize_skeleton_quality_mode(body.get("quality_mode"), template=template)
    mint_mode = _normalize_mint_mode(body.get("mint_mode"), template=template)
    art_style = _normalize_art_style(body.get("art_style"), template=template)
    image_model_id = _normalize_scene_image_model_id(body.get("image_model_id"), template=template)
    video_model_id = _normalize_creative_video_model_id(body.get("video_model_id"))
    transition_style = _normalize_transition_style(body.get("transition_style", "smooth"))
    micro_escalation_mode = _normalize_micro_escalation_mode(body.get("micro_escalation_mode"), template=template)
    cinematic_boost = _normalize_cinematic_boost(body.get("cinematic_boost", True))
    if cinematic_boost:
        quality_mode = "cinematic"
        transition_style = "cinematic"
        micro_escalation_mode = True
        mint_mode = True
    voice_id = str(body.get("voice_id", "") or "").strip()
    voice_speed = _normalize_voice_speed(body.get("voice_speed", 1.0), default=1.0)
    pacing_mode = _normalize_pacing_mode(body.get("pacing_mode", "standard"))
    if not _creative_template_supports_voice_controls(template):
        voice_id = ""
        voice_speed = 1.0
        pacing_mode = "standard"
    youtube_channel_id = str(body.get("youtube_channel_id", "") or "").strip()
    trend_hunt_enabled = _bool_from_any(body.get("trend_hunt_enabled"), False)
    try:
        catalyst_shorts_instructions = await _build_shorts_catalyst_extra_instructions(
            user,
            template,
            preferred_channel_id=youtube_channel_id,
            topic=str(body.get("topic", "") or ""),
            trend_hunt_enabled=trend_hunt_enabled,
        )
    except Exception as e:
        log.warning(f"Creative session Catalyst setup failed for template={template}: {e}")
        catalyst_shorts_instructions = ""
    persisted_public_shorts: dict = {}
    try:
        persisted_public_shorts = await _persist_public_shorts_playbook_memory(
            user=user,
            template=template,
            topic=str(body.get("topic", "") or ""),
            preferred_channel_id=youtube_channel_id,
            trend_hunt_enabled=trend_hunt_enabled,
        )
    except Exception as e:
        log.warning(f"Creative session shorts benchmark persistence failed for template={template}: {e}")
    reference_lock_mode = _normalize_reference_lock_mode(body.get("reference_lock_mode"), default="strict")
    default_reference_url = _default_reference_for_template(template)
    reference_dna, reference_quality = await _extract_reference_profile(default_reference_url, template, reference_lock_mode)
    _ensure_template_allowed(template, user)
    _user_plan, plan_limits = _resolve_user_plan_for_limits(user)
    requested_resolution = body.get("resolution", "720p")
    resolution = _normalize_output_resolution(requested_resolution, priority_allowed=bool(plan_limits.get("priority", False)))
    animation_enabled = _bool_from_any(body.get("animation_enabled"), _bool_from_any(body.get("story_animation_enabled"), True))
    story_animation_enabled = animation_enabled if template == "story" else True
    session_id = f"cs_{int(time.time())}_{random.randint(1000, 9999)}"
    async with _creative_sessions_lock:
        _creative_sessions[session_id] = {
            "session_id": session_id,
            "user_id": user["id"],
            "template": template,
            "topic": body.get("topic", "Untitled"),
            "resolution": resolution,
            "language": body.get("language", "en"),
            "script_data": {"title": body.get("topic", "Untitled"), "tags": []},
            "scenes": [],
            "scene_images": {},
            "quality_mode": quality_mode,
            "mint_mode": mint_mode,
            "art_style": art_style,
            "image_model_id": image_model_id,
            "video_model_id": video_model_id,
            "cinematic_boost": cinematic_boost,
            "transition_style": transition_style,
            "micro_escalation_mode": micro_escalation_mode,
            "voice_id": voice_id,
            "voice_speed": voice_speed,
            "pacing_mode": pacing_mode,
            "animation_enabled": animation_enabled,
            "story_animation_enabled": story_animation_enabled,
            "reference_image_url": default_reference_url,
            "reference_lock_mode": reference_lock_mode,
            "reference_dna": reference_dna,
            "reference_quality": reference_quality,
            "reference_image_uploaded": False,
            "rolling_reference_image_url": default_reference_url,
            "youtube_channel_id": youtube_channel_id,
            "trend_hunt_enabled": trend_hunt_enabled,
            "catalyst_shorts_instructions": catalyst_shorts_instructions,
            "channel_memory": dict(persisted_public_shorts.get("memory_public") or {}),
            "catalyst_public_shorts_playbook": dict(persisted_public_shorts.get("playbook") or {}),
            "prompt_passthrough": True,
            "created_at": time.time(),
        }
        _save_creative_sessions_to_disk()
    project_id = _new_project_id()
    await _create_or_update_project(project_id, {
        "user_id": user["id"],
        "template": template,
        "topic": body.get("topic", "Untitled"),
        "mode": "creative",
        "status": "draft",
        "resolution": resolution,
        "language": body.get("language", "en"),
        "art_style": art_style,
        "image_model_id": image_model_id,
        "video_model_id": video_model_id,
        "cinematic_boost": cinematic_boost,
        "voice_id": voice_id,
        "voice_speed": voice_speed,
        "pacing_mode": pacing_mode,
        "animation_enabled": animation_enabled,
        "youtube_channel_id": youtube_channel_id,
        "trend_hunt_enabled": trend_hunt_enabled,
        "session_id": session_id,
        "story_animation_enabled": story_animation_enabled,
        "scene_count": 0,
    })
    log.info(f"Creative session created: {session_id} for user {user['id']}")
    return {
        "session_id": session_id,
        "project_id": project_id,
        "story_animation_enabled": story_animation_enabled,
        "quality_mode": quality_mode,
        "mint_mode": mint_mode,
        "art_style": art_style,
        "image_model_id": image_model_id,
        "video_model_id": video_model_id,
        "cinematic_boost": cinematic_boost,
        "transition_style": transition_style,
        "micro_escalation_mode": micro_escalation_mode,
        "voice_id": voice_id,
        "voice_speed": voice_speed,
        "pacing_mode": pacing_mode,
        "animation_enabled": animation_enabled,
        "reference_lock_mode": reference_lock_mode,
        "youtube_channel_id": youtube_channel_id,
        "trend_hunt_enabled": trend_hunt_enabled,
        "prompt_passthrough": True,
    }


async def _creative_reference_image(
    session_id: str = Form(...),
    reference_image: UploadFile = File(...),
    reference_lock_mode: str = Form("strict"),
    request: Request = None,
):
    """Upload optional creative reference style image and persist it for all scene generations."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    session = _get_creative_session(session_id)
    if not session:
        raise HTTPException(404, "Creative session not found")
    if session["user_id"] != user["id"]:
        raise HTTPException(403, "Not your session")
    if not reference_image.content_type or not reference_image.content_type.startswith("image/"):
        raise HTTPException(400, "Reference file must be an image")

    raw = await reference_image.read()
    if not raw:
        raise HTTPException(400, "Reference image is empty")
    if len(raw) > 8 * 1024 * 1024:
        raise HTTPException(400, "Reference image must be <= 8MB")

    lock_mode = _normalize_reference_lock_mode(reference_lock_mode, default=_normalize_reference_lock_mode(session.get("reference_lock_mode"), "strict"))
    quality = _analyze_reference_quality(raw, lock_mode=lock_mode)
    if not quality.get("accepted", True) and lock_mode == "strict":
        raise HTTPException(400, "Reference image quality too low for Strict Reference Lock. Upload a higher-resolution image or switch to Style Inspired mode.")

    mime = reference_image.content_type or "image/png"
    data_url = f"data:{mime};base64,{base64.b64encode(raw).decode()}"
    ext = ".png"
    if "jpeg" in mime or "jpg" in mime:
        ext = ".jpg"
    elif "webp" in mime:
        ext = ".webp"
    ref_dir = TEMP_DIR / "creative_references"
    ref_dir.mkdir(parents=True, exist_ok=True)
    ref_name = f"{session_id}_reference{ext}"
    ref_path = ref_dir / ref_name
    ref_path.write_bytes(raw)
    public_url = f"{SITE_URL.rstrip('/')}/api/creative/reference-file/{ref_name}"
    session["reference_image_url"] = data_url
    session["reference_image_path"] = str(ref_path)
    session["reference_image_public_url"] = public_url
    session["reference_lock_mode"] = lock_mode
    session["reference_quality"] = quality
    session["reference_dna"] = _extract_reference_dna(raw, template=str(session.get("template", "skeleton") or "skeleton"))
    session["reference_image_uploaded"] = True
    session["rolling_reference_image_url"] = public_url
    if session.get("template") == "skeleton":
        # Keep legacy skeleton key in sync for downstream compatibility.
        session["skeleton_reference_image"] = public_url
    async with _creative_sessions_lock:
        _save_creative_sessions_to_disk()
    return {
        "ok": True,
        "reference_lock_mode": lock_mode,
        "quality": quality,
        "reference_image_public_url": public_url,
        "reference_dna": session.get("reference_dna", {}),
    }


async def _creative_reference_file(filename: str):
    """Serve uploaded creative reference images over HTTPS for provider conditioning."""
    safe = os.path.basename(filename)
    if not safe or safe != filename:
        raise HTTPException(400, "Invalid filename")
    path = TEMP_DIR / "creative_references" / safe
    if not path.exists():
        raise HTTPException(404, "Reference image not found")
    media_type = "image/png"
    if path.suffix.lower() in {".jpg", ".jpeg"}:
        media_type = "image/jpeg"
    elif path.suffix.lower() == ".webp":
        media_type = "image/webp"
    return FileResponse(str(path), media_type=media_type, filename=safe)


async def _creative_session_status(session_id: str, request: Request = None):
    """Return lightweight status for restoring Creative Control UI state after refresh."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    session = _get_creative_session(session_id)
    if not session:
        raise HTTPException(404, "Creative session not found")
    if session["user_id"] != user["id"]:
        raise HTTPException(403, "Not your session")
    return {
        "session_id": session_id,
        "has_reference_image": bool(
            _skeleton_session_has_explicit_reference(session)
            if str(session.get("template", "") or "").strip().lower() == "skeleton"
            else (session.get("reference_image_url") or session.get("skeleton_reference_image"))
        ),
        "reference_lock_mode": _normalize_reference_lock_mode(session.get("reference_lock_mode"), "strict"),
        "reference_quality": session.get("reference_quality", {}),
        "template": session.get("template", ""),
        "quality_mode": _normalize_skeleton_quality_mode(session.get("quality_mode"), template=session.get("template", "")),
        "mint_mode": _normalize_mint_mode(session.get("mint_mode"), template=session.get("template", "")),
        "art_style": _normalize_art_style(session.get("art_style", "auto"), template=session.get("template", "")),
        "image_model_id": _normalize_scene_image_model_id(session.get("image_model_id"), template=session.get("template", "")),
        "video_model_id": _normalize_creative_video_model_id(session.get("video_model_id")),
        "cinematic_boost": _normalize_cinematic_boost(session.get("cinematic_boost", False)),
        "transition_style": _normalize_transition_style(session.get("transition_style", "smooth")),
        "micro_escalation_mode": _normalize_micro_escalation_mode(session.get("micro_escalation_mode"), template=session.get("template", "")),
        "topic": session.get("topic", ""),
        "scene_count": len(session.get("scenes", [])),
        "animation_enabled": _bool_from_any(session.get("animation_enabled"), _bool_from_any(session.get("story_animation_enabled"), True)),
        "story_animation_enabled": _bool_from_any(session.get("story_animation_enabled"), True),
        "prompt_passthrough": _creative_prompt_passthrough_enabled(session),
    }


async def _creative_session_scene_images(session_id: str, request: Request = None):
    """Return persisted creative scene images from backend storage (RunPod), not browser cache."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    session = _get_creative_session(session_id)
    if not session:
        raise HTTPException(404, "Creative session not found")
    if session["user_id"] != user["id"]:
        raise HTTPException(403, "Not your session")

    out = []
    scene_images = session.get("scene_images", {}) or {}
    for key, info in scene_images.items():
        if not isinstance(info, dict):
            continue
        path = str(info.get("path", "") or "")
        if not path:
            continue
        p = Path(path)
        if not p.exists():
            continue
        try:
            idx = int(key)
        except Exception:
            continue
        mime = "image/png" if p.suffix.lower() == ".png" else "image/jpeg"
        payload = base64.b64encode(p.read_bytes()).decode("ascii")
        out.append({
            "scene_index": idx,
            "image_data": f"data:{mime};base64,{payload}",
            "generation_id": str(info.get("generation_id", "") or ""),
            "cdn_url": str(info.get("cdn_url", "") or ""),
        })
    out.sort(key=lambda x: int(x.get("scene_index", 0)))
    return {"session_id": session_id, "scene_images": out}


async def _creative_scene_image(req: SceneImageRequest, request: Request = None):
    """Generate (or regenerate) an image for a specific scene. Unlimited regenerations."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _user_has_paid_access(user):
        raise HTTPException(402, "Active subscription required. Please choose a plan.")
    session = _get_creative_session(req.session_id)
    if not session:
        raise HTTPException(404, "Creative session not found")
    if session["user_id"] != user["id"]:
        raise HTTPException(403, "Not your session")

    if "scene_images" not in session:
        session["scene_images"] = {}

    while len(session["scenes"]) <= req.scene_index:
        session["scenes"].append({"visual_description": "", "narration": "", "duration_sec": 5})

    prev_img = session["scene_images"].get(req.scene_index, {})
    prev_gen_id = prev_img.get("generation_id")
    if prev_gen_id:
        asyncio.create_task(_mark_training_feedback(prev_gen_id, accepted=False, user_id=user.get("id", ""), event="regenerate"))

    template = session.get("template", req.template)
    quality_mode = _normalize_skeleton_quality_mode(req.quality_mode or session.get("quality_mode"), template=template)
    mint_mode = _normalize_mint_mode(req.mint_mode if req.mint_mode is not None else session.get("mint_mode"), template=template)
    art_style = _normalize_art_style(req.art_style or session.get("art_style", "auto"), template=template)
    cinematic_boost = _normalize_cinematic_boost(getattr(req, "cinematic_boost", session.get("cinematic_boost", False)))
    if cinematic_boost:
        quality_mode = "cinematic"
        mint_mode = True
    youtube_channel_id = str(getattr(req, "youtube_channel_id", "") or session.get("youtube_channel_id", "") or "").strip()
    if youtube_channel_id:
        session["youtube_channel_id"] = youtube_channel_id
    reference_lock_mode = _normalize_reference_lock_mode(req.reference_lock_mode or session.get("reference_lock_mode"), "strict")
    session["reference_lock_mode"] = reference_lock_mode
    session["art_style"] = art_style
    _ensure_reference_public_url(req.session_id, session)
    user_plan, plan_limits = _resolve_user_plan_for_limits(user)
    is_admin = user.get("email", "") in ADMIN_EMAILS
    billing_active = _billing_active_for_user(user)
    resolution = _normalize_output_resolution(session.get("resolution", req.resolution), priority_allowed=bool(plan_limits.get("priority", False)))
    neg_prompt = str(getattr(req, "negative_prompt", "") or "").strip()
    raw_prompt = str(req.prompt or "").strip()
    if not raw_prompt:
        raise HTTPException(400, "Scene prompt is required")
    image_model_id = _normalize_scene_image_model_id(
        getattr(req, "image_model_id", "") or session.get("image_model_id"),
        template=template,
    )
    image_model_profile = _creative_image_model_profile(image_model_id, template=template)
    image_credit_cost = _creative_image_credit_cost(image_model_id, template=template)
    usage_kind = "animated" if image_credit_cost > 0 else "non_animated"
    credits_needed = max(1, image_credit_cost) if image_credit_cost > 0 else 1
    can_run, credit_source, credit_state = await _reserve_generation_credit(
        user,
        user_plan if not is_admin else "pro",
        billing_active,
        is_admin=is_admin,
        usage_kind=usage_kind,
        credits_needed=credits_needed,
    )
    if not can_run:
        if usage_kind == "non_animated":
            raise HTTPException(402, "Non-animated meter exhausted for this month. Please wait for renewal or upgrade plan.")
        available_credits = int(credit_state.get("credits_total_remaining", 0) or 0)
        required_credits = int(credit_state.get("credits_needed", credits_needed) or credits_needed)
        raise HTTPException(
            402,
            f"{image_model_profile.get('label', 'Selected image model')} needs {required_credits} Catalyst credits per image, but only {available_credits} are available.",
        )
    prompt_passthrough = _creative_prompt_passthrough_enabled(session)
    scene_reference = _resolve_reference_for_scene(session, template, req.scene_index)
    prompt_requests_damage_or_fatigue = bool(
        re.search(
            r"\b(crack|cracks|fracture|fractured|chip|chipped|bruise|bruises|damaged|damage|tired|fatigued|weary|slouch|slouched|hunch|hunched|droop|drooping|exhausted)\b",
            raw_prompt,
            flags=re.IGNORECASE,
        )
    )
    effective_reference_lock_mode = reference_lock_mode
    if (
        template == "skeleton"
        and prompt_passthrough
        and reference_lock_mode == "strict"
        and prompt_requests_damage_or_fatigue
        and not bool(scene_reference)
    ):
        # Strict reference lock can suppress requested damage/posture deltas.
        # In passthrough mode, soften to inspired so prompt details can render.
        effective_reference_lock_mode = "inspired"
    if template == "skeleton":
        log.info(
            f"Creative scene prompt mode: passthrough={prompt_passthrough} "
            f"lock={reference_lock_mode}->{effective_reference_lock_mode} "
            f"prompt='{raw_prompt[:220]}'"
        )
    if prompt_passthrough:
        constrained_prompt = raw_prompt
        full_prompt = _build_creative_passthrough_scene_prompt(
            template=template,
            visual_description=raw_prompt,
            quality_mode=quality_mode,
            reference_dna=session.get("reference_dna", {}),
            reference_lock_mode=reference_lock_mode,
            art_style=art_style,
        )
    else:
        constrained_prompt = raw_prompt
        if template == "skeleton":
            constrained_prompt = _apply_template_scene_constraints(
                [{"visual_description": raw_prompt}],
                template,
                quality_mode=quality_mode,
            )[0]["visual_description"]
        constrained_prompt = _apply_mint_scene_compiler(
            [{"visual_description": constrained_prompt}],
            template,
            mint_mode=mint_mode,
        )[0]["visual_description"]
        full_prompt = _build_scene_prompt_with_reference(
            template=template,
            visual_description=constrained_prompt,
            quality_mode=quality_mode,
            reference_dna=session.get("reference_dna", {}),
            reference_lock_mode=reference_lock_mode,
            art_style=art_style,
        )
    channel_context = dict((dict(session.get("metadata_pack") or {})).get("youtube_channel") or {})
    if not channel_context and str(session.get("youtube_channel_id", "") or "").strip():
        channel_context = {"channel_id": str(session.get("youtube_channel_id", "") or "").strip()}

    img_path = str(TEMP_DIR / f"{req.session_id}_scene_{req.scene_index}.png")
    try:
        img_result = await generate_scene_image(
            full_prompt,
            img_path,
            resolution=resolution,
            negative_prompt=neg_prompt,
            template=template,
            channel_context=channel_context,
            reference_image_url=scene_reference,
            reference_lock_mode=effective_reference_lock_mode,
            best_of_enabled=False,
            salvage_enabled=(template == "story"),
            interactive_fast=True,
            prompt_passthrough=prompt_passthrough,
            selected_model_id=image_model_id,
        )
    except Exception as e:
        await _refund_generation_credit(
            str(user.get("id", "") or ""),
            credit_source,
            month_key=str(credit_state.get("month_key", "") or ""),
            credits=credits_needed,
        )
        err_text = str(e or "").strip()
        err_l = err_text.lower()
        if "hidream is the only configured image provider" in err_l:
            raise HTTPException(503, err_text) from e
        if template == "skeleton" and "hidream" in err_l and ("timed out" in err_l or "timeout" in err_l):
            raise HTTPException(
                504,
                "Skeleton image generation timed out on the NYPTID Studio image engine. Click Regenerate to retry.",
            ) from e
        if template == "skeleton" and (
            "Skeleton generation blocked" in err_text
            or "WAN2.2 text-to-image is unavailable" in err_text
        ):
            raise HTTPException(503, err_text) from e
        if template == "skeleton" and (
            "timed out" in err_l
            or "timeout" in err_l
        ):
            raise HTTPException(
                504,
                "Skeleton image generation timed out on the NYPTID Studio image engine. Click Regenerate to retry.",
            ) from e
        if template == "skeleton" and "exhausted wan2.2 attempt budget" in err_l:
            raise HTTPException(
                504,
                "WAN2.2 exhausted interactive budget before finishing. Install the real WAN2.2 TI2V/T2I model (wan2.2_ti2v_5B_fp16.safetensors) to avoid slow fallback lanes.",
            ) from e
        if template == "skeleton" and (
            "qa gate failed" in err_l
            or "failed qa gate" in err_l
            or ("qa gate" in err_l and "skeleton" in err_l)
            or "prompt mismatch" in err_l
            or "prop_missing_or_wrong" in err_l
        ):
            raise HTTPException(
                422,
                "Scene prompt not satisfied strongly enough (skeleton/props mismatch). Click Regenerate to retry with stricter composition.",
            ) from e
        should_try_moderation_rewrite = (
            template == "story"
            and (
                _looks_like_provider_moderation_error(str(e or ""))
                or _prompt_likely_moderated(constrained_prompt)
            )
        )
        if should_try_moderation_rewrite:
            retry_candidates = [
                _soften_story_prompt_for_moderation(constrained_prompt, aggressive=False),
                _soften_story_prompt_for_moderation(constrained_prompt, aggressive=True),
            ]
            seen = {constrained_prompt}
            retried = False
            last_retry_error: Exception | None = None
            for candidate in retry_candidates:
                if not candidate or candidate in seen:
                    continue
                seen.add(candidate)
                retried = True
                constrained_prompt = candidate
                full_prompt = _build_scene_prompt_with_reference(
                    template=template,
                    visual_description=constrained_prompt,
                    quality_mode=quality_mode,
                    reference_dna=session.get("reference_dna", {}),
                    reference_lock_mode=reference_lock_mode,
                    art_style=art_style,
                )
                try:
                    img_result = await generate_scene_image(
                        full_prompt,
                        img_path,
                        resolution=resolution,
                        negative_prompt=neg_prompt,
                        template=template,
                        channel_context=channel_context,
                        reference_image_url=scene_reference,
                        reference_lock_mode=effective_reference_lock_mode,
                        best_of_enabled=False,
                        salvage_enabled=False,
                        interactive_fast=True,
                        prompt_passthrough=prompt_passthrough,
                    )
                    last_retry_error = None
                    break
                except Exception as retry_err:
                    last_retry_error = retry_err
            if last_retry_error is not None or not retried:
                raise HTTPException(
                    400,
                    "Image provider rejected this scene prompt due to safety policy. Try regenerate, or simplify violent wording while keeping the same emotion.",
                ) from (last_retry_error or e)
        else:
            if template == "skeleton":
                detail = err_text if err_text else "unknown skeleton image generation failure"
                if len(detail) > 260:
                    detail = detail[:260].rstrip() + "..."
                raise HTTPException(500, f"Skeleton image generation failed: {detail}") from e
            raise HTTPException(
                500,
                "Scene image generation failed. Please regenerate this scene.",
            ) from e
    if template == "skeleton":
        qa_notes = list(img_result.get("qa_notes", []) or [])
        qa_ok = bool(img_result.get("qa_ok", False))
        severe_qa = (not qa_ok) and _skeleton_notes_are_severe(qa_notes)
        if severe_qa:
            raise HTTPException(
                422,
                "Scene prompt not satisfied strongly enough (skeleton/props mismatch). Click Regenerate to retry with stricter composition.",
            )
        # Creative prompt-passthrough mode should not nudge extra regenerations for
        # non-severe soft-accept outcomes; treat those as acceptable matches.
        if prompt_passthrough and not qa_ok:
            img_result["qa_ok"] = True
            img_result["qa_notes"] = [n for n in qa_notes if str(n or "").strip().lower() != "interactive_soft_accept"]
    if template == "skeleton" and not _skeleton_default_identity_locked(session) and req.scene_index == 0 and not (session.get("skeleton_reference_image") or session.get("reference_image_url")):
        session["skeleton_reference_image"] = _file_to_data_image_url(img_path)
    if template == "skeleton" and _skeleton_default_identity_locked(session):
        session["rolling_reference_image_url"] = _default_reference_for_template("skeleton")
    elif reference_lock_mode == "strict" or not session.get("rolling_reference_image_url"):
        rolled = str(img_result.get("cdn_url", "") or "").strip() or _file_to_data_image_url(img_path)
        if rolled:
            session["rolling_reference_image_url"] = rolled

    gen_id = img_result.get("generation_id", "")

    import base64 as b64mod
    img_bytes = Path(img_path).read_bytes()
    img_b64 = b64mod.b64encode(img_bytes).decode()

    session["scene_images"][req.scene_index] = {
        "path": img_path,
        "cdn_url": img_result.get("cdn_url"),
        "prompt": constrained_prompt,
        "generation_id": gen_id,
        "image_model_id": image_model_id,
        "image_model_label": str(image_model_profile.get("label", image_model_id) or image_model_id),
        "qa_score": float(img_result.get("qa_score", 0.0) or 0.0),
        "qa_ok": bool(img_result.get("qa_ok", False)),
        "qa_notes": img_result.get("qa_notes", []),
    }

    while len(session["scenes"]) <= req.scene_index:
        session["scenes"].append({"narration": "", "visual_description": "", "negative_prompt": "", "duration_sec": 5})
    session["scenes"][req.scene_index]["visual_description"] = constrained_prompt
    session["scenes"][req.scene_index]["negative_prompt"] = neg_prompt
    session["quality_mode"] = quality_mode
    session["mint_mode"] = mint_mode
    session["art_style"] = art_style
    session["cinematic_boost"] = cinematic_boost
    session["image_model_id"] = image_model_id
    async with _creative_sessions_lock:
        _save_creative_sessions_to_disk()
    await _update_project_by_session(user.get("id", ""), req.session_id, {
        "status": "draft",
        "scene_count": len(session["scenes"]),
        "scenes": session.get("scenes", []),
        "narration": session.get("narration", ""),
        "image_model_id": image_model_id,
    })

    return {
        "scene_index": req.scene_index,
        "image_data": f"data:image/png;base64,{img_b64}",
        "prompt_used": constrained_prompt,
        "generation_id": gen_id,
        "image_model_id": image_model_id,
        "image_model_label": str(image_model_profile.get("label", image_model_id) or image_model_id),
        "credit_cost": image_credit_cost,
        "billing_source": "owner_override" if is_admin else (credit_source if image_credit_cost > 0 else "non_animated_free"),
        "quality_mode": quality_mode,
        "mint_mode": mint_mode,
        "qa_score": float(img_result.get("qa_score", 0.0) or 0.0),
        "qa_ok": bool(img_result.get("qa_ok", False)),
        "qa_notes": img_result.get("qa_notes", []),
    }


async def _creative_scene_feedback(body: dict, request: Request = None):
    """Mark a generated image as accepted (user moved on) or rejected (user regenerated)."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    gen_id = body.get("generation_id", "")
    accepted = body.get("accepted", True)
    if gen_id:
        await _mark_training_feedback(gen_id, accepted=accepted, user_id=user.get("id", ""), event="scene_feedback")
    return {"ok": True, "generation_id": gen_id, "status": "accepted" if accepted else "rejected"}


async def _creative_update_scene(session_id: str, scene_index: int, body: dict, request: Request = None):
    """Update a scene's narration or visual description."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    session = _get_creative_session(session_id)
    if not session or session["user_id"] != user["id"]:
        raise HTTPException(404, "Session not found")
    if scene_index >= len(session["scenes"]):
        raise HTTPException(400, "Invalid scene index")
    scene = session["scenes"][scene_index]
    if "narration" in body:
        scene["narration"] = body["narration"]
    if "visual_description" in body:
        scene["visual_description"] = body["visual_description"]
    if "negative_prompt" in body:
        scene["negative_prompt"] = str(body.get("negative_prompt", "") or "")
    if "duration_sec" in body:
        scene["duration_sec"] = body["duration_sec"]
    async with _creative_sessions_lock:
        _save_creative_sessions_to_disk()
    await _update_project_by_session(user.get("id", ""), session_id, {
        "status": "draft",
        "scene_count": len(session["scenes"]),
        "scenes": session.get("scenes", []),
        "narration": session.get("narration", ""),
    })
    return {"ok": True, "scene": scene}


async def _creative_finalize(req: FinalizeRequest, background_tasks: BackgroundTasks, request: Request = None):
    """Phase 3: Run the full pipeline using the user's scenes + images."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _user_has_paid_access(user):
        raise HTTPException(402, "Active subscription required. Please choose a plan.")
    session = _get_creative_session(req.session_id)
    if not session or session["user_id"] != user["id"]:
        raise HTTPException(404, "Session not found")
    _ensure_template_allowed(session.get("template", req.template), user)
    _ensure_reference_public_url(req.session_id, session)

    if req.narration:
        session["narration"] = req.narration
    if req.scenes:
        session["scenes"] = req.scenes
    quality_mode = _normalize_skeleton_quality_mode(req.quality_mode or session.get("quality_mode"), template=session.get("template", req.template))
    mint_mode = _normalize_mint_mode(req.mint_mode if req.mint_mode is not None else session.get("mint_mode"), template=session.get("template", req.template))
    art_style = _normalize_art_style(req.art_style or session.get("art_style", "auto"), template=session.get("template", req.template))
    cinematic_boost = _normalize_cinematic_boost(getattr(req, "cinematic_boost", session.get("cinematic_boost", False)))
    transition_style = _normalize_transition_style(req.transition_style or session.get("transition_style"))
    micro_escalation_mode = _normalize_micro_escalation_mode(
        req.micro_escalation_mode if req.micro_escalation_mode is not None else session.get("micro_escalation_mode"),
        template=session.get("template", req.template),
    )
    if cinematic_boost:
        quality_mode = "cinematic"
        mint_mode = True
        transition_style = "cinematic"
        micro_escalation_mode = True
    voice_id = str(req.voice_id or session.get("voice_id", "") or "").strip()
    voice_speed = _normalize_voice_speed(req.voice_speed if req.voice_speed is not None else session.get("voice_speed", 1.0), default=1.0)
    pacing_mode = _normalize_pacing_mode(req.pacing_mode or session.get("pacing_mode", "standard"))
    subtitles_enabled = _bool_from_any(req.subtitles_enabled, _bool_from_any(session.get("subtitles_enabled"), True))
    if not _creative_template_supports_voice_controls(session.get("template", req.template)):
        voice_id = ""
        voice_speed = 1.0
        pacing_mode = "standard"
    youtube_channel_id = str(getattr(req, "youtube_channel_id", "") or session.get("youtube_channel_id", "") or "").strip()
    reference_lock_mode = _normalize_reference_lock_mode(req.reference_lock_mode or session.get("reference_lock_mode"), "strict")
    image_model_id = _normalize_scene_image_model_id(req.image_model_id or session.get("image_model_id"), template=session.get("template", req.template))
    video_model_id = _normalize_creative_video_model_id(req.video_model_id or session.get("video_model_id"))
    video_model_profile = _creative_video_model_profile(video_model_id)
    animation_enabled = _bool_from_any(
        req.animation_enabled,
        _bool_from_any(session.get("animation_enabled"), _bool_from_any(req.story_animation_enabled, _bool_from_any(session.get("story_animation_enabled"), True))),
    )
    story_animation_enabled = animation_enabled if session.get("template") == "story" else True
    async with _creative_sessions_lock:
        session["quality_mode"] = quality_mode
        session["mint_mode"] = mint_mode
        session["art_style"] = art_style
        session["cinematic_boost"] = cinematic_boost
        session["transition_style"] = transition_style
        session["micro_escalation_mode"] = micro_escalation_mode
        session["voice_id"] = voice_id
        session["voice_speed"] = voice_speed
        session["pacing_mode"] = pacing_mode
        session["subtitles_enabled"] = subtitles_enabled
        session["youtube_channel_id"] = youtube_channel_id
        session["reference_lock_mode"] = reference_lock_mode
        session["animation_enabled"] = animation_enabled
        session["story_animation_enabled"] = story_animation_enabled
        session["image_model_id"] = image_model_id
        session["video_model_id"] = video_model_id
        _save_creative_sessions_to_disk()
    if not session["scenes"]:
        raise HTTPException(400, "No scenes provided")

    user_plan, plan_limits = _resolve_user_plan_for_limits(user)
    is_admin = user.get("email", "") in ADMIN_EMAILS
    billing_active = _billing_active_for_user(user)
    resolution = _normalize_output_resolution(session.get("resolution", req.resolution), priority_allowed=bool(plan_limits.get("priority", False)))
    total_duration = sum(float(s.get("duration_sec", 5) or 5) for s in session.get("scenes", []))
    if total_duration > float(plan_limits.get("max_duration_sec", 60)):
        raise HTTPException(400, f"Creative project exceeds plan duration limit ({int(plan_limits.get('max_duration_sec', 60))}s).")
    # The 12-scene cap was for legacy WAN-heavy skeleton renders.
    # With Runway as primary, allow longer skeleton projects.
    if (not RUNWAY_API_KEY) and session.get("template") == "skeleton" and len(session.get("scenes", [])) > 12:
        raise HTTPException(400, "Skeleton Creative projects are limited to 12 scenes when Runway is unavailable.")

    usage_kind = "animated" if animation_enabled else "non_animated"
    credits_required = max(1, len(session.get("scenes", [])) * _creative_video_credit_multiplier(video_model_id)) if animation_enabled else 1
    can_render, credit_source, credit_state = await _reserve_generation_credit(
        user,
        user_plan if not is_admin else "pro",
        billing_active,
        is_admin=is_admin,
        usage_kind=usage_kind,
        credits_needed=credits_required,
    )
    if not can_render:
        if usage_kind == "non_animated":
            raise HTTPException(402, "Non-animated meter exhausted for this month. Please wait for renewal or upgrade plan.")
        available_credits = int(credit_state.get("credits_total_remaining", 0) or 0)
        required_credits = int(credit_state.get("credits_needed", credits_required) or credits_required)
        raise HTTPException(
            402,
            f"{video_model_profile.get('label', 'Selected video model')} needs {required_credits} Catalyst credits for this render, but only {available_credits} are available. Buy more credits or switch to slideshow.",
        )

    job_id = f"{int(time.time())}_{random.randint(1000, 9999)}"
    jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "template": session["template"],
        "topic": session["topic"],
        "lane": "create",
        "mode": "creative_finalize" if animation_enabled else "creative_slideshow",
        "resolution": resolution,
        "plan": user_plan,
        "user_id": user.get("id"),
        "created_at": time.time(),
        "animation_enabled": animation_enabled,
        "story_animation_enabled": story_animation_enabled,
        "quality_mode": quality_mode,
        "mint_mode": mint_mode,
        "art_style": art_style,
        "image_model_id": image_model_id,
        "video_model_id": video_model_id,
        "video_model_label": str(video_model_profile.get("label", video_model_id) or video_model_id),
        "transition_style": transition_style,
        "micro_escalation_mode": micro_escalation_mode,
        "voice_id": voice_id,
        "voice_speed": voice_speed,
        "pacing_mode": pacing_mode,
        "subtitles_enabled": subtitles_enabled,
        "reference_lock_mode": reference_lock_mode,
        "credit_charged": True,
        "credit_source": credit_source,
        "credit_amount": credits_required,
        "credit_cost": credits_required if animation_enabled else 0,
        "billing_source": "owner_override" if is_admin else credit_source,
        "credit_month_key": credit_state.get("month_key", _month_key()),
        "credit_refunded": False,
    }
    _job_diag_init(job_id, "creative")
    await _update_project_by_session(user.get("id", ""), req.session_id, {
        "status": "rendering",
        "job_id": job_id,
        "scene_count": len(session.get("scenes", [])),
        "scenes": session.get("scenes", []),
        "narration": session.get("narration", ""),
        "voice_id": voice_id,
        "voice_speed": voice_speed,
        "pacing_mode": pacing_mode,
        "animation_enabled": animation_enabled,
        "youtube_channel_id": youtube_channel_id,
        "story_animation_enabled": story_animation_enabled,
        "image_model_id": image_model_id,
        "video_model_id": video_model_id,
    })

    for si_data in session.get("scene_images", {}).values():
        gen_id = si_data.get("generation_id")
        if gen_id:
            asyncio.create_task(_mark_training_feedback(gen_id, accepted=True, user_id=user.get("id", ""), event="finalize"))

    try:
        # Queue runtime compatibility:
        # pass only the original positional args and read render modes from session.
        await enqueue_generation_job(job_id, user_plan, _run_creative_pipeline, (job_id, session, resolution))
    except QueueFullError as e:
        if jobs[job_id].get("credit_charged") and str(jobs[job_id].get("credit_source", "")) in {"monthly", "topup"}:
            await _refund_generation_credit(
                str(jobs[job_id].get("user_id", "") or ""),
                str(jobs[job_id].get("credit_source", "") or ""),
                month_key=str(jobs[job_id].get("credit_month_key", "") or ""),
                credits=int(jobs[job_id].get("credit_amount", 1) or 1),
            )
            jobs[job_id]["credit_refunded"] = True
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
        await persist_job_state(job_id, jobs[job_id])
        raise HTTPException(429, str(e))
    return {"job_id": job_id}


async def _run_creative_pipeline(
    job_id: str,
    session: dict,
    resolution: str,
    quality_mode: str = "standard",
    mint_mode: bool = True,
    transition_style: str = "smooth",
    micro_escalation_mode: bool = True,
):
    """Run generation using pre-approved creative session scenes."""
    try:
        template = session["template"]
        quality_mode = _normalize_skeleton_quality_mode(session.get("quality_mode") or quality_mode, template=template)
        mint_mode = _normalize_mint_mode(session.get("mint_mode") if session.get("mint_mode") is not None else mint_mode, template=template)
        transition_style = _normalize_transition_style(session.get("transition_style") or transition_style)
        micro_escalation_mode = _normalize_micro_escalation_mode(session.get("micro_escalation_mode") if session.get("micro_escalation_mode") is not None else micro_escalation_mode, template=template)
        cinematic_boost = _normalize_cinematic_boost(session.get("cinematic_boost", False))
        if cinematic_boost:
            quality_mode = "cinematic"
            mint_mode = True
            transition_style = "cinematic"
            micro_escalation_mode = True
        voice_id = str(session.get("voice_id", "") or "").strip()
        voice_speed = _normalize_voice_speed(session.get("voice_speed", 1.0), default=1.0)
        pacing_mode = _normalize_pacing_mode(session.get("pacing_mode", "standard"))
        subtitles_enabled = _bool_from_any(session.get("subtitles_enabled"), True)
        image_model_id = _normalize_scene_image_model_id(session.get("image_model_id"), template=template)
        video_model_id = _normalize_creative_video_model_id(session.get("video_model_id"))
        script_data = dict(session.get("script_data", {}) or {})
        scenes = _normalize_scenes_for_render(session["scenes"])
        prompt_passthrough = _creative_prompt_passthrough_enabled(session)
        if not prompt_passthrough:
            scenes = _apply_template_scene_constraints(scenes, template, quality_mode=quality_mode)
            scenes = _apply_mint_scene_compiler(scenes, template, mint_mode=mint_mode)
        if not (template == "story" and pacing_mode != "standard"):
            scenes = _force_template_scene_duration(scenes, template)
        scenes = _apply_story_pacing(scenes, template, pacing_mode=pacing_mode)
        scenes = _apply_short_scene_narration_fit(scenes, template, voice_speed=voice_speed)
        catalyst_memory_public = _catalyst_short_memory_public_snapshot(
            user_id=str(session.get("user_id", "") or ""),
            template=template,
            youtube_channel_id=str(session.get("youtube_channel_id", "") or ""),
            seed_memory_public=dict(
                session.get("channel_memory")
                or dict(dict(session.get("metadata_pack") or {}).get("catalyst_channel_memory") or {})
            ),
        )
        scenes = _apply_short_execution_pacing_profile(
            scenes,
            template,
            voice_speed=voice_speed,
            pacing_mode=pacing_mode,
            topic=str(session.get("topic", "") or script_data.get("title", "") or ""),
            memory_public=catalyst_memory_public,
        )
        language = session.get("language", "en")
        scene_images = session.get("scene_images", {})
        reference_lock_mode = _normalize_reference_lock_mode(session.get("reference_lock_mode"), "strict")
        reference_dna = session.get("reference_dna", {}) if isinstance(session.get("reference_dna"), dict) else {}
        art_style = _normalize_art_style(session.get("art_style", "auto"), template=template)
        channel_context = dict((dict(session.get("metadata_pack") or {})).get("youtube_channel") or {})
        if not channel_context and str(session.get("youtube_channel_id", "") or "").strip():
            channel_context = {"channel_id": str(session.get("youtube_channel_id", "") or "").strip()}

        fal_video_enabled = bool(FAL_AI_KEY)
        runway_video_enabled = bool(RUNWAY_API_KEY)
        use_video_engine = fal_video_enabled or runway_video_enabled
        animation_enabled = _bool_from_any(session.get("animation_enabled"), _bool_from_any(session.get("story_animation_enabled"), True))
        story_animation_enabled = _bool_from_any(session.get("story_animation_enabled"), animation_enabled)
        use_video = bool(use_video_engine and animation_enabled)
        if use_video and not use_video_engine:
            raise RuntimeError("Video is required but no engine is configured (set RUNWAY_API_KEY or FAL_AI_KEY)")
        jobs[job_id]["animation_enabled"] = bool(animation_enabled)
        jobs[job_id]["story_animation_enabled"] = bool(story_animation_enabled)
        gen_ts = str(int(time.time() * 1000))

        _job_set_stage(job_id, "generating_images", 10)
        jobs[job_id]["total_scenes"] = len(scenes)

        scene_assets = []
        total_steps = len(scenes) * (2 if use_video else 1)
        skeleton_anchor = ""
        if template == "skeleton" and scenes:
            skeleton_anchor = _canonical_skeleton_anchor()

        for i, scene in enumerate(scenes):
            jobs[job_id]["current_scene"] = i + 1
            step_base = i * (2 if use_video else 1)
            _job_set_stage(job_id, "generating_images", 10 + int((step_base / total_steps) * 55))
            _job_record_scene_event(job_id, i, len(scenes), "image_start")

            pre_gen = scene_images.get(i) or scene_images.get(str(i))
            if pre_gen and Path(pre_gen["path"]).exists():
                img_path = pre_gen["path"]
                cdn_url = pre_gen.get("cdn_url")
                log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} using pre-approved image")
                _job_record_scene_event(job_id, i, len(scenes), "image_ready", "pre_approved")
            else:
                if prompt_passthrough:
                    full_prompt = _build_creative_passthrough_scene_prompt(
                        template=template,
                        visual_description=scene.get("visual_description", ""),
                        quality_mode=quality_mode,
                        skeleton_anchor=skeleton_anchor,
                        reference_dna=reference_dna,
                        reference_lock_mode=reference_lock_mode,
                        art_style=art_style,
                    )
                else:
                    full_prompt = _build_scene_prompt_with_reference(
                        template=template,
                        visual_description=scene.get("visual_description", ""),
                        quality_mode=quality_mode,
                        skeleton_anchor=skeleton_anchor,
                        reference_dna=reference_dna,
                        reference_lock_mode=reference_lock_mode,
                        art_style=art_style,
                    )
                img_path = str(TEMP_DIR / (job_id + "_scene_" + str(i) + ".png"))
                scene_negative_prompt = str(scene.get("negative_prompt", "") or "").strip()
                img_result = await generate_scene_image(
                    full_prompt,
                    img_path,
                    resolution=resolution,
                    negative_prompt=scene_negative_prompt,
                    template=template,
                    channel_context=channel_context,
                    reference_image_url=_resolve_reference_for_scene(session, template, i),
                    reference_lock_mode=reference_lock_mode,
                    prompt_passthrough=prompt_passthrough,
                    selected_model_id=image_model_id,
                )
                if template == "skeleton" and not _skeleton_default_identity_locked(session) and not (session.get("reference_image_url") or session.get("skeleton_reference_image")) and i == 0:
                    skeleton_seed = _file_to_data_image_url(img_path)
                    if skeleton_seed:
                        session["skeleton_reference_image"] = skeleton_seed
                if template == "skeleton" and _skeleton_default_identity_locked(session):
                    session["rolling_reference_image_url"] = _default_reference_for_template("skeleton")
                elif reference_lock_mode == "strict" or not session.get("rolling_reference_image_url"):
                    rolled = str(img_result.get("cdn_url", "") or "").strip() or _file_to_data_image_url(img_path)
                    if rolled:
                        session["rolling_reference_image_url"] = rolled
                cdn_url = img_result.get("cdn_url")
                image_engine = str(img_result.get("provider_label", "") or "generated")
                log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} image generated fresh via {image_engine}")
                _job_record_scene_event(job_id, i, len(scenes), "image_ready", image_engine)

            asset = {"image": img_path, "frames": None, "kling_clip": None}

            if use_video:
                _job_set_stage(job_id, "animating_scenes", 10 + int(((step_base + 1) / total_steps) * 55))
                _job_record_scene_event(job_id, i, len(scenes), "animation_start")
                kling_motion = TEMPLATE_KLING_MOTION.get(template, "Cinematic motion, smooth camera movement, subtle animation.")
                anim_prompt = scene.get("visual_description", "") + " " + kling_motion
                try:
                    anim_result = await animate_scene(
                        img_path, anim_prompt, str(TEMP_DIR), i, gen_ts,
                        duration_sec=scene.get("duration_sec", 5), image_cdn_url=cdn_url,
                        prefer_wan=(template == "skeleton"),
                        video_model_id=video_model_id,
                    )
                except Exception as anim_err:
                    jobs[job_id]["animation_warnings"] = int(jobs[job_id].get("animation_warnings", 0)) + 1
                    log.warning(f"[{job_id}] Scene {i+1}/{len(scenes)} animation failed, using static image: {anim_err}")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_failed", str(anim_err))
                    anim_result = {"type": "static"}
                if anim_result["type"] in ("kling_clip", "wan_clip", "grok_clip", "runway_clip", "fal_clip"):
                    asset["kling_clip"] = anim_result["path"]
                    if anim_result["type"] == "fal_clip":
                        engine = str(anim_result.get("provider_label", "Fal Video") or "Fal Video")
                    elif anim_result["type"] == "runway_clip":
                        engine = "Runway"
                    elif anim_result["type"] == "grok_clip":
                        engine = "Grok Imagine Video"
                    elif anim_result["type"] == "kling_clip":
                        engine = "Kling 2.1"
                    else:
                        engine = "Wan 2.2"
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} animated by {engine}")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_ready", engine)
                else:
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} using static image")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_static")

            scene_assets.append(asset)

        _job_set_stage(job_id, "generating_voice", 70)
        audio_assets = await _prepare_short_audio_assets(
            scenes,
            audio_path=str(TEMP_DIR / (job_id + "_voice.mp3")),
            subtitle_path=str(TEMP_DIR / (job_id + "_captions.ass")),
            template=template,
            language=language,
            override_voice_id=voice_id,
            override_speed=voice_speed,
            subtitles_enabled=subtitles_enabled,
            resolution=resolution,
            job_id=job_id,
            full_narration_override=str(session.get("narration", "") or ""),
        )
        audio_path = str(audio_assets.get("audio_path", "") or "")
        word_timings = list(audio_assets.get("word_timings", []) or [])
        subtitle_path = str(audio_assets.get("subtitle_path", "") or "") or None
        jobs[job_id]["audio_mode"] = str(audio_assets.get("audio_mode", "") or "")
        jobs[job_id]["audio_warning"] = str(audio_assets.get("audio_warning", "") or "")

        sound_mix_profile = _creative_template_sound_mix_profile(template)
        sfx_paths = []
        if (_sfx_enabled() or _creative_template_force_sfx(template)) and ELEVENLABS_API_KEY:
            _job_set_stage(job_id, "generating_sfx", 78)
            for i, scene in enumerate(scenes):
                sfx_out = str(TEMP_DIR / (job_id + "_sfx_" + str(i) + ".mp3"))
                desc = scene.get("visual_description", "")
                dur = scene.get("duration_sec", 5)
                sfx_file = await generate_scene_sfx(
                    desc,
                    dur,
                    sfx_out,
                    template=template,
                    scene_index=i,
                    total_scenes=len(scenes),
                    force=_creative_template_force_sfx(template),
                )
                sfx_paths.append(sfx_file)
            sfx_paths = await _quintuple_check_scene_sfx(scenes, sfx_paths, template, job_id=job_id)
            log.info(f"[{job_id}] SFX generated: {sum(1 for s in sfx_paths if s)}/{len(scenes)} scenes")
        else:
            log.info(f"[{job_id}] SFX disabled globally; skipping generation/mix")

        bgm_track = ""
        if bool(sound_mix_profile.get("bgm_required")):
            _job_set_stage(job_id, "generating_sfx", 80)
            bgm_path = str(TEMP_DIR / f"{job_id}_creative_bgm.mp3")
            total_duration = sum(float((s or {}).get("duration_sec", 5.0) or 5.0) for s in scenes) + 0.8
            bgm_track = await _generate_catalyst_bgm_track(
                total_duration,
                bgm_path,
                music_profile=str(sound_mix_profile.get("music_profile", "") or ""),
                whisper_mode=str(sound_mix_profile.get("whisper_mode", "off") or "off"),
                format_preset="shorts",
            )

        _job_set_stage(job_id, "compositing", 82)
        output_filename = template + "_" + job_id + ".mp4"
        output_path = str(OUTPUT_DIR / output_filename)
        await composite_video(
            scenes,
            scene_assets,
            audio_path,
            output_path,
            resolution=resolution,
            use_svd=use_video,
            subtitle_path=subtitle_path,
            sfx_paths=sfx_paths,
            bgm_track=bgm_track,
            transition_style=_normalize_transition_style(transition_style),
            micro_escalation_mode=micro_escalation_mode,
            pattern_interrupt_interval_sec=_short_template_pattern_interrupt_interval_sec(template, pacing_mode=pacing_mode),
            voice_gain=float(sound_mix_profile.get("voice_gain", 1.0) or 1.0),
            ambience_gain=float(sound_mix_profile.get("ambience_gain", 0.18) or 0.18),
            sfx_gain=float(sound_mix_profile.get("sfx_gain", 1.0) or 1.0),
            bgm_gain=float(sound_mix_profile.get("bgm_gain", 0.55) or 0.55),
            job_id=job_id,
            minimum_scene_duration=5.0,
        )

        try:
            await _persist_catalyst_short_learning_for_render(
                user_id=str(session.get("user_id", "") or ""),
                job_id=job_id,
                template=template,
                topic=str(session.get("topic", "") or ""),
                youtube_channel_id=str(session.get("youtube_channel_id", "") or ""),
                script_data=script_data,
                scenes=scenes,
                word_timings=word_timings,
                sound_mix_profile=sound_mix_profile,
                transition_style=transition_style,
                pacing_mode=pacing_mode,
                voice_speed=voice_speed,
                sfx_paths=sfx_paths,
                bgm_track=bgm_track,
                subtitle_path=subtitle_path or "",
                animation_enabled=animation_enabled,
            )
        except Exception as learning_err:
            log.warning(f"[{job_id}] Creative Catalyst short learning persistence failed: {learning_err}")

        for sfx in sfx_paths:
            if sfx:
                Path(sfx).unlink(missing_ok=True)
        if bgm_track:
            Path(bgm_track).unlink(missing_ok=True)
        for asset in scene_assets:
            Path(asset["image"]).unlink(missing_ok=True)
            if asset.get("kling_clip"):
                Path(asset["kling_clip"]).unlink(missing_ok=True)
        Path(audio_path).unlink(missing_ok=True)
        if subtitle_path:
            Path(subtitle_path).unlink(missing_ok=True)

        _job_set_stage(job_id, "complete", 100)
        jobs[job_id]["output_file"] = output_filename
        jobs[job_id]["resolution"] = resolution
        jobs[job_id]["metadata"] = {
            "title": script_data.get("title", session.get("topic", "")),
            "description": script_data.get("description", ""),
            "tags": script_data.get("tags", []),
        }
        await _update_project_by_job(job_id, {
            "status": "rendered",
            "output_file": output_filename,
            "title": script_data.get("title", session.get("topic", "")),
        })
        _job_diag_finalize(job_id)
        await persist_job_state(job_id, jobs[job_id])
        log.info(f"[{job_id}] CREATIVE PIPELINE COMPLETE: {output_filename}")

        sid = session.get("session_id")
        if sid:
            async with _creative_sessions_lock:
                _creative_sessions.pop(sid, None)
                _save_creative_sessions_to_disk()

    except Exception as e:
        log.error(f"[{job_id}] Creative pipeline failed: {e}", exc_info=True)
        _job_set_stage(job_id, "error")
        jobs[job_id]["error"] = str(e)
        _job_diag_finalize(job_id)
        await persist_job_state(job_id, jobs[job_id])
        await _update_project_by_job(job_id, {"status": "error", "error": str(e)})


app.include_router(
    build_longform_creative_router(
        create_longform_session_endpoint=_create_longform_session,
        create_longform_session_bootstrap_endpoint=_create_longform_session_bootstrap,
        longform_reference_image_endpoint=_longform_reference_image,
        longform_character_reference_endpoint=_longform_character_reference,
        longform_scene_assignment_endpoint=_longform_scene_assignment,
        longform_reference_file_endpoint=_longform_reference_file,
        longform_session_status_endpoint=_longform_session_status,
        list_longform_sessions_endpoint=_list_longform_sessions,
        longform_preview_file_endpoint=_longform_preview_file,
        longform_chapter_action_endpoint=_longform_chapter_action,
        longform_resolve_error_endpoint=_longform_resolve_error,
        longform_finalize_endpoint=_longform_finalize,
        longform_stop_session_endpoint=_longform_stop_session,
        longform_ingest_outcome_endpoint=_longform_ingest_outcome,
        longform_auto_ingest_outcome_endpoint=_longform_auto_ingest_outcome,
        creative_generate_script_endpoint=_creative_generate_script,
        creative_create_session_endpoint=_creative_create_session,
        creative_reference_image_endpoint=_creative_reference_image,
        creative_reference_file_endpoint=_creative_reference_file,
        creative_session_status_endpoint=_creative_session_status,
        creative_session_scene_images_endpoint=_creative_session_scene_images,
        creative_scene_image_endpoint=_creative_scene_image,
        creative_scene_feedback_endpoint=_creative_scene_feedback,
        creative_update_scene_endpoint=_creative_update_scene,
        creative_finalize_endpoint=_creative_finalize,
    )
)


def _languages_payload():
    return {"languages": [{"code": k, "name": v["name"]} for k, v in SUPPORTED_LANGUAGES.items()]}


async def _health_payload():
    skeleton_lora = await check_skeleton_lora_available()
    provider_order = _configured_image_provider_order()
    hidream_configured = any(_normalize_image_provider_key(p) == "hidream" for p in provider_order)
    wan_configured = any(_normalize_image_provider_key(p) == "wan22" for p in provider_order)
    hidream_ready = await check_hidream_available() if hidream_configured else False
    hidream_edit_ready = await check_hidream_edit_available() if hidream_configured else False
    wan_ready = await check_wan22_available() if wan_configured else False
    wan_t2i_ready = await check_wan22_t2i_available() if wan_configured else False
    now_ts = time.time()
    hidream_checked_ts = float(_hidream_availability_cache.get("checked_ts", 0.0) or 0.0)
    hidream_last_ok_ts = float(_hidream_availability_cache.get("last_ok_ts", 0.0) or 0.0)
    hidream_last_error = str(_hidream_availability_cache.get("last_error", "") or "")
    hidream_model = str(_hidream_availability_cache.get("model_name", "") or "")
    hidream_edit_checked_ts = float(_hidream_edit_availability_cache.get("checked_ts", 0.0) or 0.0)
    hidream_edit_last_ok_ts = float(_hidream_edit_availability_cache.get("last_ok_ts", 0.0) or 0.0)
    hidream_edit_last_error = str(_hidream_edit_availability_cache.get("last_error", "") or "")
    hidream_edit_model = str(_hidream_edit_availability_cache.get("model_name", "") or "")
    wan_t2i_checked_ts = float(_wan22_t2i_availability_cache.get("checked_ts", 0.0) or 0.0)
    wan_t2i_last_ok_ts = float(_wan22_t2i_availability_cache.get("last_ok_ts", 0.0) or 0.0)
    wan_t2i_last_error = str(_wan22_t2i_availability_cache.get("last_error", "") or "")
    wan_t2i_mode = str(_wan22_t2i_availability_cache.get("mode", "") or "")
    wan_t2i_checkpoint = str(_wan22_t2i_availability_cache.get("ckpt_name", "") or "")
    wan_t2i_unet = str(_wan22_t2i_availability_cache.get("unet_name", "") or "")
    provider_label = " > ".join(provider_order)
    backend_commit, frontend_bundle = _read_deploy_meta()
    fal_video_enabled = bool(FAL_AI_KEY)
    runway_video_enabled = bool(RUNWAY_API_KEY)
    grok_video_enabled = USE_XAI_VIDEO and bool(XAI_API_KEY)
    if runway_video_enabled and fal_video_enabled:
        video_engine = "Runway (primary) + FalAI Kling fallback"
    elif runway_video_enabled and grok_video_enabled:
        video_engine = "Runway (primary) + Grok fallback"
    elif fal_video_enabled:
        video_engine = "FalAI Kling 2.1"
    elif runway_video_enabled:
        video_engine = "Runway Image-to-Video"
    elif grok_video_enabled:
        video_engine = "Grok Imagine Video"
    elif wan_ready:
        video_engine = "Wan 2.2 (RunPod)"
    elif FAL_AI_KEY:
        video_engine = "Kling 2.1 Standard"
    else:
        video_engine = "Static"
    return {
        "status": "online",
        "engine": "NYPTID Studio Engine v3.0",
        "ffmpeg_available": _ffmpeg_available(),
        "kling_enabled": bool(FAL_AI_KEY),
        "hidream_ready": hidream_ready,
        "hidream_model": hidream_model,
        "hidream_checked_ago_sec": int(max(0.0, now_ts - hidream_checked_ts)) if hidream_checked_ts else -1,
        "hidream_last_ok_ago_sec": int(max(0.0, now_ts - hidream_last_ok_ts)) if hidream_last_ok_ts else -1,
        "hidream_last_error": hidream_last_error,
        "hidream_edit_ready": hidream_edit_ready,
        "hidream_edit_model": hidream_edit_model,
        "hidream_edit_checked_ago_sec": int(max(0.0, now_ts - hidream_edit_checked_ts)) if hidream_edit_checked_ts else -1,
        "hidream_edit_last_ok_ago_sec": int(max(0.0, now_ts - hidream_edit_last_ok_ts)) if hidream_edit_last_ok_ts else -1,
        "hidream_edit_last_error": hidream_edit_last_error,
        "wan22_ready": wan_ready,
        "wan22_t2i_ready": wan_t2i_ready,
        "wan22_t2i_mode": wan_t2i_mode,
        "wan22_t2i_checkpoint": wan_t2i_checkpoint,
        "wan22_t2i_unet": wan_t2i_unet,
        "wan22_t2i_checked_ago_sec": int(max(0.0, now_ts - wan_t2i_checked_ts)) if wan_t2i_checked_ts else -1,
        "wan22_t2i_last_ok_ago_sec": int(max(0.0, now_ts - wan_t2i_last_ok_ts)) if wan_t2i_last_ok_ts else -1,
        "wan22_t2i_last_error": wan_t2i_last_error,
        "video_engine": video_engine,
        "runway_key_configured": runway_video_enabled,
        "runway_key_source": RUNWAY_API_KEY_SOURCE if runway_video_enabled else "",
        "runway_video_model": RUNWAY_VIDEO_MODEL if runway_video_enabled else "",
        "comfyui_url": COMFYUI_URL[:50],
        "skeleton_lora": skeleton_lora,
        "image_engine_skeleton": (
            ("Skeleton LoRA (local) > " + provider_label)
            if skeleton_lora
            else provider_label
        ),
        "image_provider_order": provider_order,
        "xai_image_fallback_enabled": bool(XAI_IMAGE_FALLBACK_ENABLED),
        "fal_image_backup_model": _normalize_fal_image_backup_model(FAL_IMAGE_BACKUP_MODEL) if FAL_AI_KEY else "",
        "image_local_provider_retries": int(IMAGE_LOCAL_PROVIDER_RETRIES),
        "image_provider_failure_cooldown_sec": int(IMAGE_PROVIDER_FAILURE_COOLDOWN_SEC),
        "image_provider_wan_skip_if_unavailable": bool(IMAGE_PROVIDER_WAN_SKIP_IF_UNAVAILABLE),
        "skeleton_require_wan22": bool(SKELETON_REQUIRE_WAN22),
        "image_provider_fail_counts": dict(_image_provider_fail_counts),
        "image_provider_success_counts": dict(_image_provider_success_counts),
        "image_provider_cooldowns_sec": _provider_cooldown_snapshot(),
        "image_fallback_events_total": int(_image_provider_fallback_total),
        "image_fallback_events_pairs": dict(_image_provider_fallback_pairs),
        "template_adapter_routing_enabled": bool(TEMPLATE_ADAPTER_ROUTING_ENABLED),
        "template_adapter_routes": sorted(k for k in (TEMPLATE_ADAPTER_ROUTING or {}).keys()),
        "backend_commit": backend_commit,
        "frontend_bundle": frontend_bundle,
        "queue_mode": "redis" if (REDIS_QUEUE_ENABLED and bool(REDIS_URL)) else "inprocess",
        "force_720p_only": FORCE_720P_ONLY,
    }


async def _set_comfyui_url(body: dict, user: dict):
    """Admin-only: update the ComfyUI URL at runtime (e.g. after cloudflared restart)."""
    email = user.get("email", "")
    if email not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin only")
    global COMFYUI_URL
    new_url = body.get("url", "").strip().rstrip("/")
    if not new_url:
        raise HTTPException(400, "url required")
    COMFYUI_URL = new_url
    wan_ready = await check_wan22_available()
    return {"ok": True, "comfyui_url": COMFYUI_URL, "wan22_ready": wan_ready}


async def _training_stats_payload(user: dict):
    """Admin: get training data collection stats."""
    email = user.get("email", "")
    if email not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin only")
    pairs = list(TRAINING_DATA_DIR.glob("*.png"))
    accepted = sum(1 for e in _pending_training.values() if e["status"] == "accepted")
    rejected = sum(1 for e in _pending_training.values() if e["status"] == "rejected")
    pending = sum(1 for e in _pending_training.values() if e["status"] == "pending")
    return {
        "total_on_disk": len(pairs),
        "accepted": accepted,
        "rejected": rejected,
        "pending_review": pending,
        "disk_mb": round(sum(p.stat().st_size for p in pairs) / (1024 * 1024), 1),
    }


async def _admin_analytics_payload(user: dict):
    """Admin dashboard analytics: active usage + paid tier totals + monthly revenue estimate."""
    email = user.get("email", "")
    if email not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin only")

    active_job_statuses = {"queued", "generating_script", "generating_images", "animating_scenes", "generating_voice", "generating_sfx", "compositing", "analyzing"}
    active_jobs = []
    for j in jobs.values():
        if isinstance(j, dict) and j.get("status") in active_job_statuses:
            active_jobs.append(j)
    active_generations = len(active_jobs)
    active_generating_users = len({j.get("user_id") for j in active_jobs if j.get("user_id")})

    tier_counts = {"starter": 0, "creator": 0, "pro": 0, "elite": 0, "demo_pro": 0}
    monthly_revenue_usd = 0.0
    revenue_source = "none"

    if STRIPE_SECRET_KEY:
        try:
            subs = stripe_lib.Subscription.list(status="all", limit=100, expand=["data.items.data.price"])
            for sub in subs.auto_paging_iter():
                sub_status = sub.get("status")
                if sub_status not in ("active", "trialing", "past_due", "unpaid"):
                    continue
                for item in sub.get("items", {}).get("data", []):
                    price = item.get("price", {}) or {}
                    price_id = price.get("id", "")
                    plan = STRIPE_PRICE_TO_PLAN.get(price_id)
                    if plan in tier_counts:
                        qty = int(item.get("quantity", 1) or 1)
                        tier_counts[plan] += qty
                        monthly_revenue_usd += ((price.get("unit_amount") or 0) / 100.0) * qty
            revenue_source = "stripe"
        except Exception as e:
            log.warning(f"Admin analytics stripe read failed: {e}")

    if revenue_source != "stripe" and SUPABASE_URL and (SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY):
        # Fallback: infer subscriber counts from profile plans if Stripe is unavailable.
        try:
            svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.get(
                    f"{SUPABASE_URL}/rest/v1/profiles?select=plan&limit=5000",
                    headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
                )
                if resp.status_code == 200:
                    for row in resp.json():
                        p = row.get("plan")
                        if p in tier_counts:
                            tier_counts[p] += 1
                            monthly_revenue_usd += float(PLAN_PRICE_USD.get(p, 0.0) or 0.0)
                    revenue_source = "profiles"
        except Exception as e:
            log.warning(f"Admin analytics profiles fallback failed: {e}")

    active_users_signins_15m = 0
    if SUPABASE_URL and (SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY):
        try:
            svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
            now_utc = datetime.now(timezone.utc)
            async with httpx.AsyncClient(timeout=20) as client:
                users_resp = await client.get(
                    f"{SUPABASE_URL}/auth/v1/admin/users?per_page=500",
                    headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
                )
                if users_resp.status_code == 200:
                    users_data = users_resp.json()
                    user_list = users_data.get("users", users_data) if isinstance(users_data, dict) else users_data
                    for u in user_list:
                        ts = u.get("last_sign_in_at")
                        if not ts:
                            continue
                        try:
                            signed_in_at = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                            if (now_utc - signed_in_at).total_seconds() <= 15 * 60:
                                active_users_signins_15m += 1
                        except Exception:
                            continue
        except Exception as e:
            log.warning(f"Admin analytics active-users fetch failed: {e}")

    queue_depth = 0
    queue_workers = 1
    queue_max_depth = 1
    try:
        queue_depth = max(0, int(await get_queue_depth()))
    except Exception as e:
        log.warning(f"Admin analytics queue depth read failed: {e}")
    try:
        queue_workers = max(1, int(get_queue_workers()))
    except Exception as e:
        log.warning(f"Admin analytics queue workers read failed: {e}")
    try:
        queue_max_depth = max(1, int(get_queue_max_depth()))
    except Exception as e:
        log.warning(f"Admin analytics queue max-depth read failed: {e}")
    active_users_estimate = max(active_generating_users, active_users_signins_15m)
    queue_utilization_pct = round((queue_depth / queue_max_depth) * 100, 1)
    active_generations_per_worker = round(active_generations / queue_workers, 2)
    high_load_detected = queue_utilization_pct >= 70.0 or active_generations_per_worker >= 1.0
    voice_diag = await _voice_provider_snapshot(force_refresh=False)
    return {
        "active_generations": active_generations,
        "queue_depth": queue_depth,
        "queue_workers": queue_workers,
        "queue_max_depth": queue_max_depth,
        "queue_utilization_pct": queue_utilization_pct,
        "active_generations_per_worker": active_generations_per_worker,
        "high_load_detected": high_load_detected,
        "active_users_generating": active_generating_users,
        "active_users_signins_15m": active_users_signins_15m,
        "active_users_estimate": active_users_estimate,
        "maintenance_banner_enabled": _maintenance_banner_enabled,
        "maintenance_banner_message": _maintenance_banner_message,
        "subscribers_by_tier": tier_counts,
        "total_paid_subscribers": sum(tier_counts.values()),
        "monthly_revenue_usd": round(monthly_revenue_usd, 2),
        # We currently expose gross subscription revenue as a profit proxy.
        "monthly_profit_usd": round(monthly_revenue_usd, 2),
        "revenue_source": revenue_source,
        "voice_provider_ok": voice_diag["provider_ok"],
        "voice_catalog_source": voice_diag["source"],
        "voice_catalog_count": voice_diag["count"],
        "voice_catalog_warning": voice_diag["warning"],
    }


async def _admin_waiting_list_payload(user: dict):
    if user.get("email", "") not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin only")
    rows = await _supabase_get_waitlist_rows(limit=3000)
    total = len(rows)
    by_plan = {"starter": 0, "creator": 0, "pro": 0, "elite": 0}
    paid_revenue_monthly = 0.0
    for row in rows:
        plan = str((row or {}).get("plan", "") or "").strip().lower()
        if plan in by_plan:
            by_plan[plan] += 1
        if bool((row or {}).get("paid")):
            paid_revenue_monthly += float((row or {}).get("price_usd", 0.0) or 0.0)
    return {
        "rows": rows,
        "summary": {
            "total": total,
            "by_plan": by_plan,
            "paid_revenue_monthly_usd": round(paid_revenue_monthly, 2),
        },
    }


async def _admin_billing_audit_payload(user: dict):
    email = str(user.get("email", "") or "")
    if email not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin only")
    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not SUPABASE_URL or not svc_key:
        raise HTTPException(500, "Supabase not configured")

    rows: list[dict] = []
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                f"{SUPABASE_URL}/auth/v1/admin/users?per_page=500",
                headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
            )
            if resp.status_code != 200:
                raise HTTPException(500, "Failed to read auth users for billing audit")
            users_data = resp.json()
            user_list = users_data.get("users", users_data) if isinstance(users_data, dict) else users_data
            by_email = {
                str(u.get("email", "") or "").strip().lower(): str(u.get("id", "") or "")
                for u in (user_list or [])
                if u and str(u.get("email", "") or "").strip()
            }

            prof = await client.get(
                f"{SUPABASE_URL}/rest/v1/profiles?select=id,plan",
                headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
            )
            if prof.status_code != 200:
                raise HTTPException(500, "Failed to read profile plans for billing audit")
            profiles = prof.json()
            profiles = profiles if isinstance(profiles, list) else []
            for p in profiles:
                plan = str((p or {}).get("plan", "none") or "none").strip().lower()
                if not _profile_plan_is_paid(plan):
                    continue
                uid = str((p or {}).get("id", "") or "")
                acct_email = ""
                for e, eid in by_email.items():
                    if eid == uid:
                        acct_email = e
                        break
                if not acct_email:
                    continue
                stripe_diag = _stripe_subscription_snapshot(acct_email)
                stripe_status = str(stripe_diag.get("status", "") or "")
                stripe_ok = bool(stripe_diag.get("ok")) and stripe_status in {"active", "trialing", "past_due"}
                cancel_at_period_end = bool(stripe_diag.get("cancel_at_period_end", False))
                status_source = "stripe" if stripe_ok else "profile_fallback"
                next_renewal_unix = int(stripe_diag.get("next_renewal_unix", 0) or 0)
                next_renewal_source = str(stripe_diag.get("next_renewal_source", "") or "")
                paid_at_unix = int(stripe_diag.get("paid_at_unix", 0) or 0)
                interval_months = max(1, int(stripe_diag.get("interval_months", 1) or 1))
                if next_renewal_unix <= 0 and paid_at_unix > 0 and not cancel_at_period_end:
                    rolled = _next_renewal_from_anchor(paid_at_unix, interval_months)
                    if rolled > 0:
                        next_renewal_unix = int(rolled)
                        next_renewal_source = next_renewal_source or "paid_at_rollforward_fallback"
                rows.append(
                    {
                        "email": acct_email,
                        "user_id": uid,
                        "plan": plan,
                        "status_source": status_source,
                        "stripe_status": stripe_status or "unknown",
                        "cancel_at_period_end": cancel_at_period_end,
                        "billing_active": bool(stripe_ok or _profile_plan_is_paid(plan)),
                        "next_renewal_unix": next_renewal_unix,
                        "next_renewal_source": next_renewal_source,
                        "paid_at_unix": paid_at_unix,
                    }
                )
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Admin billing audit failed: {e}")
        raise HTTPException(500, "Billing audit failed")

    rows.sort(key=lambda r: (r.get("plan", ""), r.get("email", "")))
    return {"rows": rows, "total_paid_profiles": len(rows)}


async def _set_maintenance_banner_payload(body: dict, user: dict):
    global _maintenance_banner_enabled, _maintenance_banner_message
    email = user.get("email", "")
    if email not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin only")

    enabled = _bool_from_any(body.get("enabled"), _maintenance_banner_enabled)
    message = str(body.get("message", _maintenance_banner_message)).strip()
    if not message:
        message = "Studio is under high load. Queue times may be longer than usual while we scale capacity."

    _maintenance_banner_enabled = enabled
    _maintenance_banner_message = message

    try:
        escaped_message = '"' + message.replace('"', '\\"') + '"'
        _persist_env_overrides(
            {
                "MAINTENANCE_BANNER_ENABLED": "1" if enabled else "0",
                "MAINTENANCE_BANNER_MESSAGE": escaped_message,
            }
        )
    except Exception as e:
        log.warning(f"Failed to persist maintenance banner settings to .env: {e}")

    return {
        "ok": True,
        "maintenance_banner_enabled": _maintenance_banner_enabled,
        "maintenance_banner_message": _maintenance_banner_message,
    }


async def _public_config_payload():
    public_plans = {
        name: {k: v for k, v in limits.items()}
        for name, limits in PLAN_LIMITS.items()
        if name in PUBLIC_PLAN_IDS
    }
    public_plan_features = {
        name: list(features)
        for name, features in PLAN_FEATURES.items()
        if name in PUBLIC_PLAN_IDS
    }
    public_plan_prices = {
        k: float(v)
        for k, v in PLAN_PRICE_USD.items()
        if k in PUBLIC_PLAN_IDS
    }
    public_topup_packs = [
        {"price_id": price_id, **meta}
        for price_id, meta in TOPUP_PACKS.items()
        if price_id in PUBLIC_TOPUP_PACK_IDS
    ]
    return {
        "supabase_url": str(SUPABASE_URL or "").strip() or FALLBACK_SUPABASE_URL,
        "supabase_anon_key": str(SUPABASE_ANON_KEY or "").strip() or FALLBACK_SUPABASE_ANON_KEY,
        "stripe_enabled": False,
        "waitlist_only_mode": False,
        "waitlist_requires_stripe_payment": False,
        "maintenance_banner_enabled": _maintenance_banner_enabled,
        "maintenance_banner_message": _maintenance_banner_message,
        "plans": public_plans,
        "plan_features": public_plan_features,
        "plan_prices_usd": public_plan_prices,
        "prices": {},
        "topup_packs": public_topup_packs,
        "transition_styles": list(TRANSITION_STYLE_MAP.keys()),
        "story_art_style_count": len(ART_STYLE_PRESETS),
        "render_capabilities": {
            "animated_max_resolution": ("720p" if FORCE_720P_ONLY else "1080p"),
            "micro_escalation_supported": True,
            "micro_escalation_max_source_scenes": MICRO_ESCALATION_MAX_SOURCE_SCENES,
            "micro_escalation_max_output_clips": MICRO_ESCALATION_MAX_OUTPUT_CLIPS,
        },
        "creative_model_catalog": {
            "default_image_model_id": DEFAULT_CREATIVE_IMAGE_MODEL_ID,
            "default_video_model_id": DEFAULT_CREATIVE_VIDEO_MODEL_ID,
            "premium_image_credit_multiplier": 4,
            "elite_image_credit_multiplier": 5,
            "premium_video_credit_multiplier": 4,
            "elite_video_credit_multiplier": 5,
            "image_models": _creative_model_catalog_copy(CREATIVE_IMAGE_MODEL_PROFILES),
            "video_models": _creative_model_catalog_copy(CREATIVE_VIDEO_MODEL_PROFILES),
        },
        "billing_model": {
            "hybrid_enabled": True,
            "model": "membership_plus_credits",
            "default_membership_plan_id": _default_membership_plan_id(),
            "membership_label": "Catalyst Membership",
            "paypal_primary": True,
            "slideshows_free": True,
            "animated_credit_label": "Catalyst credits",
            "non_animated_credit_label": "script/image/slideshow operations",
            "overage_label": "credit wallet top-ups",
            "hard_stop_on_animated_exhaustion": True,
            "waitlist_only_mode": False,
            "waitlist_requires_stripe_payment": False,
            "kling21_standard_i2v_5s_usd": KLING21_STANDARD_I2V_5S_USD,
            "animation_markup_multiplier": ANIMATION_MARKUP_MULTIPLIER,
            "animation_credit_unit_usd": ANIMATION_CREDIT_UNIT_USD,
        },
        "youtube_integration": {
            "oauth_configured": _youtube_auth_configured(),
            "api_key_configured": bool(YOUTUBE_API_KEY),
            "api_key_pool_size": len(_youtube_public_api_key_candidates()),
            "redirect_uri": GOOGLE_REDIRECT_URI,
            "oauth_preferred_mode": YOUTUBE_OAUTH_MODE,
            "oauth_active_mode": _youtube_active_oauth_mode(),
            "oauth_source": GOOGLE_OAUTH_SOURCE,
            "oauth_client_kind": GOOGLE_OAUTH_CLIENT_KIND,
            "oauth_config_issue": GOOGLE_OAUTH_CONFIG_ISSUE,
            "installed_oauth_source": GOOGLE_INSTALLED_OAUTH_SOURCE,
            "installed_oauth_issue": GOOGLE_INSTALLED_OAUTH_CONFIG_ISSUE,
            "multiple_channels_supported": True,
        },
        "auth": {
            "primary_provider": "google",
            "email_fallback_enabled": True,
        },
        "feature_flags": {
            "script_to_short_enabled": SCRIPT_TO_SHORT_ENABLED,
            "story_advanced_controls_enabled": STORY_ADVANCED_CONTROLS_ENABLED,
            "story_retention_tuning_enabled": STORY_RETENTION_TUNING_ENABLED,
            "disable_all_sfx": DISABLE_ALL_SFX,
            "longform_beta_enabled": bool(LONGFORM_BETA_ENABLED),
        },
    }


async def _landing_notifications_payload():
    cutoff = time.time() - (7 * 24 * 3600)
    async with _landing_notifications_lock:
        events = [
            e for e in _landing_notifications
            if isinstance(e, dict) and float(e.get("ts") or 0.0) >= cutoff
        ]
        events = events[-LANDING_NOTIFICATIONS_PUBLIC_LIMIT:]
    return {"events": events}


app.include_router(
    build_core_router(
        require_auth=require_auth,
        admin_emails=ADMIN_EMAILS,
        plan_limits=PLAN_LIMITS,
        demo_pro_price_id=DEMO_PRO_PRICE_ID,
        product_demo_public_enabled=PRODUCT_DEMO_PUBLIC_ENABLED,
        youtube_oauth_mode=YOUTUBE_OAUTH_MODE,
        google_oauth_source=GOOGLE_OAUTH_SOURCE,
        google_oauth_client_kind=GOOGLE_OAUTH_CLIENT_KIND,
        google_oauth_config_issue=GOOGLE_OAUTH_CONFIG_ISSUE,
        google_installed_oauth_source=GOOGLE_INSTALLED_OAUTH_SOURCE,
        google_installed_oauth_config_issue=GOOGLE_INSTALLED_OAUTH_CONFIG_ISSUE,
        youtube_connections_lock=_youtube_connections_lock,
        load_youtube_connections=_load_youtube_connections,
        youtube_bucket_for_user=_youtube_bucket_for_user,
        youtube_auth_configured=_youtube_auth_configured,
        youtube_active_oauth_mode=_youtube_active_oauth_mode,
        paid_access_snapshot_for_user=_paid_access_snapshot_for_user,
        stripe_subscription_snapshot=lambda email: _stripe_subscription_snapshot(email),
        next_renewal_from_anchor=lambda anchor_unix, months: _next_renewal_from_anchor(anchor_unix, months),
        credit_state_for_user=_credit_state_for_user,
        membership_plan_for_user=_membership_plan_for_user,
        plan_features_for=_plan_features_for,
        public_lane_access_for_user=_public_lane_access_for_user,
        longform_deep_analysis_enabled=_longform_deep_analysis_enabled,
        projects_ref=_projects,
    )
)

app.include_router(
    build_misc_router(
        require_auth=require_auth,
        list_languages_payload=_languages_payload,
        health_payload=_health_payload,
        set_comfyui_url_handler=_set_comfyui_url,
        training_stats_handler=_training_stats_payload,
        admin_analytics_handler=_admin_analytics_payload,
        admin_waiting_list_handler=_admin_waiting_list_payload,
        admin_billing_audit_handler=_admin_billing_audit_payload,
        set_maintenance_banner_handler=_set_maintenance_banner_payload,
        public_config_payload=_public_config_payload,
        landing_notifications_payload=_landing_notifications_payload,
    )
)


app.include_router(
    build_youtube_catalyst_app_router(
        require_auth=require_auth,
        get_current_user=get_current_user,
        get_current_user_from_request=get_current_user_from_request,
        youtube_start_oauth_for_user=_youtube_start_oauth_for_user,
        youtube_start_oauth_browser_redirect=_youtube_start_oauth_browser_redirect,
        google_youtube_oauth_installed_helper_response=_google_youtube_oauth_installed_helper_response,
        google_youtube_oauth_complete_redirect=_google_youtube_oauth_complete_redirect,
        google_youtube_oauth_callback_redirect=_google_youtube_oauth_callback_redirect,
        catalyst_hub_snapshot_for_user=_catalyst_hub_snapshot_for_user,
        catalyst_hub_refresh_for_user=_catalyst_hub_refresh_for_user,
        catalyst_hub_reference_video_analysis_for_user=_catalyst_hub_reference_video_analysis_for_user,
        catalyst_hub_reference_video_analysis_manual_for_user=_catalyst_hub_reference_video_analysis_manual_for_user,
        catalyst_hub_clear_reference_video_analysis_for_user=_catalyst_hub_clear_reference_video_analysis_for_user,
        catalyst_hub_save_instructions_for_user=_catalyst_hub_save_instructions_for_user,
        catalyst_hub_launch_longform_for_user=_catalyst_hub_launch_longform_for_user,
        list_connected_youtube_channels_for_user=_list_connected_youtube_channels_for_user,
        select_connected_youtube_channel_for_user=_select_connected_youtube_channel_for_user,
        sync_connected_youtube_channel_for_user=_sync_connected_youtube_channel_for_user,
        sync_connected_youtube_channel_outcomes_for_user=_sync_connected_youtube_channel_outcomes_for_user,
        disconnect_connected_youtube_channel_for_user=_disconnect_connected_youtube_channel_for_user,
        bool_from_any=_bool_from_any,
        catalyst_reference_analysis_default_minutes=CATALYST_REFERENCE_ANALYSIS_DEFAULT_MINUTES,
        upload_dir=TEMP_DIR / "catalyst_reference_evidence",
        longform_owner_beta_enabled=_longform_owner_beta_enabled,
        harvest_catalyst_outcomes_for_channel=_harvest_catalyst_outcomes_for_channel,
    )
)


def _user_has_paid_access(user: dict | None) -> bool:
    return bool(user)


async def _generate_short(req: GenerateRequest, background_tasks: BackgroundTasks, request: Request = None):
    if not XAI_API_KEY:
        raise HTTPException(500, "XAI_API_KEY not configured")

    user = await get_current_user_from_request(request) if request else None
    if user and not _user_has_paid_access(user):
        raise HTTPException(402, "Active subscription required. Please choose a plan.")
    _ensure_template_allowed(req.template, user)
    quality_mode = _normalize_skeleton_quality_mode(req.quality_mode, template=req.template)
    mint_mode = _normalize_mint_mode(req.mint_mode, template=req.template)
    art_style = _normalize_art_style(req.art_style, template=req.template)
    voice_id = str(req.voice_id or "").strip()
    voice_speed = _normalize_voice_speed(req.voice_speed, default=1.0)
    pacing_mode = _normalize_pacing_mode(req.pacing_mode)
    animation_enabled = _bool_from_any(req.animation_enabled, _bool_from_any(req.story_animation_enabled, True))
    if not _creative_template_supports_voice_controls(req.template):
        voice_id = ""
        voice_speed = 1.0
        pacing_mode = "standard"
    youtube_channel_id = str(getattr(req, "youtube_channel_id", "") or "").strip()
    trend_hunt_enabled = _bool_from_any(getattr(req, "trend_hunt_enabled", False), False)
    try:
        catalyst_shorts_instructions = await _build_shorts_catalyst_extra_instructions(
            user,
            req.template,
            preferred_channel_id=youtube_channel_id,
            topic=req.prompt,
            trend_hunt_enabled=trend_hunt_enabled,
        )
    except Exception as e:
        log.warning(f"Generate short Catalyst setup failed for template={req.template}: {e}")
        catalyst_shorts_instructions = ""
    persisted_public_shorts: dict = {}
    try:
        persisted_public_shorts = await _persist_public_shorts_playbook_memory(
            user=user,
            template=req.template,
            topic=req.prompt,
            preferred_channel_id=youtube_channel_id,
            trend_hunt_enabled=trend_hunt_enabled,
        )
    except Exception as e:
        log.warning(f"Generate short shorts benchmark persistence failed for template={req.template}: {e}")
    reference_lock_mode = _normalize_reference_lock_mode(req.reference_lock_mode, default="strict")
    reference_image_url = _normalize_reference_with_default(req.template, str(req.reference_image_url or "").strip())
    reference_dna, reference_quality = await _extract_reference_profile(reference_image_url, req.template, reference_lock_mode)
    if (
        reference_quality
        and not reference_quality.get("accepted", True)
        and reference_lock_mode == "strict"
        and not _is_template_default_reference(req.template, reference_image_url)
    ):
        raise HTTPException(400, "Reference image quality too low for Strict lock. Upload a higher-resolution image or use Inspired lock.")
    user_plan = "starter"
    plan_limits = PLAN_LIMITS["starter"]
    is_admin = False
    if user:
        user_plan, plan_limits = _resolve_user_plan_for_limits(user)
        is_admin = user.get("email", "") in ADMIN_EMAILS
        billing_active = _billing_active_for_user(user)
        usage_kind = "animated" if animation_enabled else "non_animated"
        can_render, credit_source, credit_state = await _reserve_generation_credit(
            user,
            user_plan if not is_admin else "pro",
            billing_active,
            is_admin=is_admin,
            usage_kind=usage_kind,
        )
        if not can_render:
            if usage_kind == "non_animated":
                raise HTTPException(402, "Non-animated meter exhausted for this month. Please wait for renewal or upgrade plan.")
            raise HTTPException(
                402,
                "No animated render credits left this month. Buy an animated top-up pack to continue.",
            )
    else:
        credit_source = ""
        credit_state = {"month_key": _month_key()}
    transition_style = _normalize_transition_style(req.transition_style)
    micro_escalation_mode = _normalize_micro_escalation_mode(req.micro_escalation_mode, template=req.template)
    cinematic_boost = _normalize_cinematic_boost(getattr(req, "cinematic_boost", False))

    resolution = _normalize_output_resolution(req.resolution, priority_allowed=bool(plan_limits.get("priority", False)))

    job_id = f"{int(time.time())}_{random.randint(1000, 9999)}"
    jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "template": req.template,
        "topic": req.prompt,
        "lane": "create",
        "mode": "auto_generate",
        "resolution": resolution,
        "plan": user_plan,
        "user_id": user.get("id") if user else None,
        "created_at": time.time(),
        "quality_mode": quality_mode,
        "mint_mode": mint_mode,
        "art_style": art_style,
        "cinematic_boost": cinematic_boost,
        "transition_style": transition_style,
        "micro_escalation_mode": micro_escalation_mode,
        "voice_id": voice_id,
        "voice_speed": voice_speed,
        "pacing_mode": pacing_mode,
        "animation_enabled": animation_enabled,
        "story_animation_enabled": animation_enabled if req.template == "story" else True,
        "youtube_channel_id": youtube_channel_id,
        "trend_hunt_enabled": trend_hunt_enabled,
        "catalyst_shorts_instructions": catalyst_shorts_instructions,
        "channel_memory": dict(persisted_public_shorts.get("memory_public") or {}),
        "catalyst_public_shorts_playbook": dict(persisted_public_shorts.get("playbook") or {}),
        "reference_image_url": reference_image_url,
        "reference_lock_mode": reference_lock_mode,
        "reference_dna": reference_dna,
        "reference_quality": reference_quality,
        "credit_charged": bool(user),
        "credit_source": credit_source,
        "credit_amount": 1,
        "credit_cost": 1 if animation_enabled else 0,
        "billing_source": "owner_override" if is_admin else (credit_source or "workspace_access"),
        "credit_month_key": credit_state.get("month_key", _month_key()),
        "credit_refunded": False,
    }
    _job_diag_init(job_id, "auto")
    language = req.language if req.language in SUPPORTED_LANGUAGES else "en"
    if user:
        project_id = _new_project_id()
        await _create_or_update_project(project_id, {
            "user_id": user.get("id"),
            "template": req.template,
            "topic": req.prompt,
            "mode": "auto",
            "status": "rendering",
            "resolution": resolution,
            "language": language,
            "art_style": art_style,
            "cinematic_boost": cinematic_boost,
            "voice_id": voice_id,
            "voice_speed": voice_speed,
            "pacing_mode": pacing_mode,
            "animation_enabled": animation_enabled,
            "youtube_channel_id": youtube_channel_id,
            "job_id": job_id,
        })
        jobs[job_id]["project_id"] = project_id

    try:
        await enqueue_generation_job(
            job_id,
            user_plan,
            run_generation_pipeline,
            (job_id, req.template, req.prompt, resolution, language, quality_mode, mint_mode, transition_style, micro_escalation_mode),
        )
    except QueueFullError as e:
        if jobs[job_id].get("credit_charged") and str(jobs[job_id].get("credit_source", "")) in {"monthly", "topup"}:
            await _refund_generation_credit(
                str(jobs[job_id].get("user_id", "") or ""),
                str(jobs[job_id].get("credit_source", "") or ""),
                month_key=str(jobs[job_id].get("credit_month_key", "") or ""),
                credits=int(jobs[job_id].get("credit_amount", 1) or 1),
            )
            jobs[job_id]["credit_refunded"] = True
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
        await persist_job_state(job_id, jobs[job_id])
        raise HTTPException(429, str(e))

    return {"status": "accepted", "job_id": job_id}


app.include_router(
    build_generation_router(
        generate_short_endpoint=_generate_short,
    )
)


async def _auto_scene_image(job_id: str, filename: str):
    safe = Path(filename).name
    if safe != filename:
        raise HTTPException(400, "Invalid filename")
    path = _auto_scene_dir(job_id) / safe
    if not path.exists():
        raise HTTPException(404, "Image not found")
    media_type = "image/png" if path.suffix.lower() == ".png" else "image/jpeg"
    return FileResponse(str(path), media_type=media_type, filename=safe)


async def _auto_regenerate_scene_image(body: dict, request: Request = None):
    job_id = str((body or {}).get("job_id", "") or "").strip()
    if not job_id:
        raise HTTPException(400, "job_id is required")
    try:
        scene_index = int((body or {}).get("scene_index", 0))
    except Exception:
        raise HTTPException(400, "scene_index must be an integer")

    state = jobs.get(job_id)
    in_memory_job = True
    if not isinstance(state, dict):
        persisted = await get_persisted_job_state(job_id)
        if not isinstance(persisted, dict):
            raise HTTPException(404, "Job not found")
        state = dict(persisted)
        in_memory_job = False

    # If the job is tied to a user, require matching auth for regeneration.
    owner_user_id = str(state.get("user_id", "") or "")
    if owner_user_id:
        user = await get_current_user_from_request(request) if request else None
        if not user or str(user.get("id", "")) != owner_user_id:
            raise HTTPException(403, "Not authorized for this job")

    resolution = str(state.get("resolution", "720p") or "720p")
    if resolution != "720p":
        raise HTTPException(400, "Image regeneration is available only for 720p auto mode")

    scene_images = state.get("scene_images", [])
    if not isinstance(scene_images, list) or not scene_images:
        raise HTTPException(400, "No persisted scene images for this job")
    if scene_index < 0 or scene_index >= len(scene_images):
        raise HTTPException(404, "Scene index out of range")

    template = str(state.get("template", "skeleton") or "skeleton")
    quality_mode = _normalize_skeleton_quality_mode((body or {}).get("quality_mode") or state.get("quality_mode"), template=template)
    mint_mode = _normalize_mint_mode((body or {}).get("mint_mode") if isinstance(body, dict) else None, template=template)
    art_style = _normalize_art_style(state.get("art_style", "auto"), template=template)
    reference_lock_mode = _normalize_reference_lock_mode(state.get("reference_lock_mode"), default="strict")
    reference_dna = state.get("reference_dna", {}) if isinstance(state.get("reference_dna"), dict) else {}
    reference_image_url = _normalize_reference_with_default(template, str(state.get("reference_image_url", "") or "").strip())
    scene_entry = scene_images[scene_index] if isinstance(scene_images[scene_index], dict) else {}
    scene_prompt = str(scene_entry.get("visual_description", "") or "")
    if not scene_prompt:
        prompts = state.get("scene_prompts", [])
        if isinstance(prompts, list) and 0 <= scene_index < len(prompts):
            scene_prompt = str(prompts[scene_index] or "")
    if not scene_prompt:
        raise HTTPException(400, "Scene prompt unavailable for regeneration")
    scene_prompt = _apply_mint_scene_compiler(
        [{"visual_description": scene_prompt}],
        template,
        mint_mode=mint_mode,
    )[0]["visual_description"]

    neg_prompt = TEMPLATE_NEGATIVE_PROMPTS.get(template, NEGATIVE_PROMPT)
    skeleton_anchor = ""
    if template == "skeleton" and isinstance(scene_images, list) and scene_images:
        skeleton_anchor = _canonical_skeleton_anchor()
    if template == "skeleton":
        scene_prompt = _apply_template_scene_constraints(
            [{"visual_description": scene_prompt}],
            template,
            quality_mode=quality_mode,
        )[0]["visual_description"]
    full_prompt = _build_scene_prompt_with_reference(
        template=template,
        visual_description=scene_prompt,
        quality_mode=quality_mode,
        skeleton_anchor=skeleton_anchor,
        reference_dna=reference_dna,
        reference_lock_mode=reference_lock_mode,
        art_style=art_style,
    )

    ts = int(time.time() * 1000)
    out_name = f"scene_{scene_index + 1:02d}_regen_{ts}.png"
    out_path = str(_auto_scene_dir(job_id) / out_name)
    scene_reference_url = _resolve_reference_for_scene(state, template, scene_index) or reference_image_url
    channel_context = dict((dict(state.get("metadata_pack") or {})).get("youtube_channel") or {})
    if not channel_context and str(state.get("youtube_channel_id", "") or "").strip():
        channel_context = {"channel_id": str(state.get("youtube_channel_id", "") or "").strip()}
    img_result = await generate_scene_image(
        full_prompt,
        out_path,
        resolution="720p",
        negative_prompt=neg_prompt,
        template=template,
        channel_context=channel_context,
        reference_image_url=scene_reference_url,
        reference_lock_mode=reference_lock_mode,
    )

    old_gen_id = str(scene_entry.get("generation_id", "") or "")
    new_gen_id = str(img_result.get("generation_id", "") or "")
    if old_gen_id and new_gen_id and old_gen_id != new_gen_id:
        try:
            asyncio.create_task(_mark_training_feedback(old_gen_id, accepted=False, user_id=owner_user_id, event="auto_regenerate"))
        except Exception:
            pass

    updated = {
        "scene_index": scene_index,
        "filename": out_name,
        "image_url": _auto_scene_url(job_id, out_name),
        "local_path": str(Path(out_path)),
        "visual_description": scene_prompt,
        "template": template,
        "quality_mode": quality_mode,
        "generation_id": new_gen_id,
        "cdn_url": str(img_result.get("cdn_url", "") or ""),
        "source": "regenerated_auto",
        "updated_at": time.time(),
    }
    scene_images[scene_index] = updated
    state["scene_images"] = scene_images
    state["regenerate_count"] = int(state.get("regenerate_count", 0) or 0) + 1
    if in_memory_job:
        jobs[job_id]["scene_images"] = scene_images
        jobs[job_id]["regenerate_count"] = int(jobs[job_id].get("regenerate_count", 0) or 0) + 1
    await persist_job_state(job_id, state)
    return {"ok": True, "job_id": job_id, "scene_index": scene_index, "image": updated}


async def _job_status_payload(job_id: str):
    _prune_in_memory_jobs()
    persisted = await get_persisted_job_state(job_id)
    if isinstance(persisted, dict):
        _record_kpi_for_job(job_id, persisted)
        if persisted.get("kpi_recorded"):
            await persist_job_state(job_id, persisted)
        return persisted
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    _record_kpi_for_job(job_id, jobs[job_id])
    return jobs[job_id]


async def _download_video_response(filename: str):
    path = OUTPUT_DIR / filename
    if not path.exists():
        raise HTTPException(404, "Video not found")
    return FileResponse(str(path), media_type="video/mp4", filename=filename)


async def _render_chat_story(
    request: Request,
    payload: str = Form(...),
    avatar: Optional[UploadFile] = File(None),
    background_video: Optional[UploadFile] = File(None),
):
    user = await get_current_user_from_request(request)
    if not user:
        raise HTTPException(401, "Authentication required")
    if not _chat_story_access_for_user(user):
        raise HTTPException(403, "Chat Story requires an active Starter, Creator, or Pro monthly plan.")

    try:
        parsed_payload = json.loads(payload or "{}")
    except Exception:
        raise HTTPException(400, "Invalid chat story payload")

    messages = parsed_payload.get("messages") or []
    if not isinstance(messages, list) or not any(str(item.get("text", "") or "").strip() for item in messages if isinstance(item, dict)):
        raise HTTPException(400, "Add at least one chat message before rendering.")

    render_id = f"chatstory_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
    work_dir = TEMP_DIR / render_id
    work_dir.mkdir(parents=True, exist_ok=True)
    avatar_path = ""
    bg_video_path = ""

    try:
        if avatar and avatar.filename:
            avatar_ext = Path(avatar.filename).suffix or ".png"
            avatar_path = str(work_dir / f"avatar{avatar_ext}")
            with open(avatar_path, "wb") as handle:
                while chunk := await avatar.read(1024 * 1024):
                    handle.write(chunk)

        if background_video and background_video.filename:
            bg_ext = Path(background_video.filename).suffix or ".mp4"
            bg_video_path = str(work_dir / f"background{bg_ext}")
            with open(bg_video_path, "wb") as handle:
                while chunk := await background_video.read(1024 * 1024):
                    handle.write(chunk)

        payload_path = work_dir / "payload.json"
        payload_path.write_text(json.dumps(parsed_payload, ensure_ascii=False, indent=2), encoding="utf-8")

        output_name = f"{render_id}.mp4"
        output_path = OUTPUT_DIR / output_name
        script_path = Path(__file__).resolve().parent / "ops" / "render_chat_story.py"

        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            str(script_path),
            "--payload",
            str(payload_path),
            "--output",
            str(output_path),
            "--avatar",
            avatar_path,
            "--background-video",
            bg_video_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            log.error("Chat Story render failed: %s", (stderr or b"").decode("utf-8", errors="replace"))
            raise HTTPException(500, "Chat Story render failed.")

        meta = {}
        try:
            meta = json.loads((stdout or b"{}").decode("utf-8", errors="replace").strip() or "{}")
        except Exception:
            meta = {}

        if not output_path.exists():
            raise HTTPException(500, "Chat Story render did not produce an output video.")

        return {
            "ok": True,
            "output_file": output_name,
            "download_url": f"/api/download/{quote(output_name)}",
            "lane": "chatstory",
            "mode": "chatstory_render",
            "credit_cost": 0,
            "billing_source": "workspace_access" if not _is_admin_user(user) else "owner_override",
            "duration_sec": meta.get("duration_sec"),
            "message_count": meta.get("message_count"),
            "voice": meta.get("voice"),
            "theme": meta.get("theme"),
            "background": meta.get("background"),
            "used_background_video": bool(meta.get("used_background_video")),
        }
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


CLONE_ANALYSIS_PROMPT = """You are a viral video reverse-engineering expert. Analyze the source video and extract its EXACT winning formula so it can be replicated on a new topic.

You will receive:
1. Context about a viral short (uploaded video metadata, audio timing, or description)
2. A new topic to apply the viral formula to

Your job: figure out the EXACT structure, pacing, and style of the source video and replicate it beat-for-beat on the new topic.

Analyze these elements:
- HOOK: What made someone stop scrolling in the first 1-3 seconds? (question? claim? shock?)
- PACING: How fast are cuts? What is the average scene duration?
- VISUAL STYLE: 3D skeletons? Cinematic? Text-heavy? What background color?
- NARRATION STYLE: Fast? Punchy? How many sentences per scene?
- TEXT OVERLAYS: One word at a time? Full sentences? Bold impact font?
- STRUCTURE: VS comparison? Countdown? Story arc? How is info revealed?
- RETENTION TRICKS: Money flying, size comparisons, face-offs, shocking numbers?

TEMPLATE DEFINITIONS (pick the one that matches BEST):
- "skeleton" = Canonical 3D anatomical skeleton identity (same skull/eyes/bone proportions every scene, no clothing/costumes), topic-relevant environments allowed, VS comparisons/career breakdowns, one-word bold captions. Example: "NASCAR vs F1 Driver Who Makes More Money"
- "history" = Epic cinematic historical scenes. Battles, empires, ancient events. Dramatic narrator, god rays, film grain. 2-4 word caption phrases. Example: "What Happened to the Roman Legion That Vanished"
- "story" = Cinematic AI visual stories with emotional arc. Pixar/UE5 quality. Continuity of recurring subjects/locations across scenes (do not force one unchanged protagonist). Poetic narration. Minimal captions. Example: "The Last Lighthouse Keeper"
- "reddit" = Reddit story narration. First-person dramatic stories (AITA, TIFU). Photorealistic modern-day scenes illustrating the story. Dialogue/reaction captions. Example: "AITA for Kicking Out My Sister"
- "top5" = Ranked countdown lists (#5 to #1). Each item dramatically different. Documentary quality visuals. Numbered captions. Example: "Top 5 Most Expensive Things Ever Sold"
- "random" = Chaotic, fast-paced, unpredictable content. Every scene wildly different. Surreal visuals. 1-3 word reaction captions. Example: "Things That Should Not Exist"

Output MUST be valid JSON:
{
  "detected_template": "skeleton|history|story|reddit|top5|random",
  "viral_analysis": {
    "hook_type": "What kind of hook (shock/question/claim/visual)",
    "pacing": "fast|medium|slow",
    "avg_scene_duration": 3.5,
    "scene_count": 10,
    "tone": "Description of voice/narration tone",
    "retention_tricks": ["trick1", "trick2"],
    "what_made_it_viral": "1-2 sentence summary"
  },
  "optimized_prompt": "An enhanced prompt that combines the viral formula with the new topic. This should be detailed enough to pass directly to the script generator."
}"""


async def extract_audio_from_video(video_path: str, *, max_seconds: float | None = None) -> str | None:
    """Extract a lighter audio sample from a video file for transcription."""
    audio_path = video_path.rsplit(".", 1)[0] + "_audio.mp3"
    cmd = [
        "ffmpeg", "-y", "-i", video_path,
    ]
    if max_seconds and float(max_seconds or 0.0) > 0:
        cmd += ["-t", f"{float(max_seconds):.2f}"]
    cmd += [
        "-vn",
        "-ac",
        "1",
        "-ar",
        "16000",
        "-acodec",
        "libmp3lame",
        "-q:a",
        "6",
        audio_path,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    await proc.communicate()
    if proc.returncode == 0 and Path(audio_path).exists():
        return audio_path
    return None


async def transcribe_audio_with_grok(audio_path: str) -> str:
    """Extract a real transcript excerpt from sampled audio when YouTube captions are unavailable."""
    source_path = str(audio_path or "").strip()
    if not source_path or not Path(source_path).exists():
        return ""

    def _transcribe_blocking() -> str:
        global _reference_whisper_model

        if FasterWhisperModel is None:
            return ""

        try:
            if _reference_whisper_model is None:
                model_dir = TEMP_DIR / "whisper_models"
                model_dir.mkdir(parents=True, exist_ok=True)
                _reference_whisper_model = FasterWhisperModel(
                    "tiny.en",
                    device="cpu",
                    compute_type="int8",
                    download_root=str(model_dir),
                )
            segments, _info = _reference_whisper_model.transcribe(
                source_path,
                language="en",
                beam_size=1,
                best_of=1,
                vad_filter=True,
                condition_on_previous_text=False,
                without_timestamps=True,
            )
            parts: list[str] = []
            total_chars = 0
            for seg in segments:
                text = re.sub(r"\s+", " ", str(getattr(seg, "text", "") or "").strip())
                if not text:
                    continue
                parts.append(text)
                total_chars += len(text) + 1
                if total_chars >= CATALYST_REFERENCE_TRANSCRIPT_MAX_CHARS:
                    break
            transcript = re.sub(r"\s+", " ", " ".join(parts)).strip()
            return _clip_text(transcript, CATALYST_REFERENCE_TRANSCRIPT_MAX_CHARS)
        except Exception as e:
            log.warning(f"Reference audio transcription fallback failed: {e}")
            return ""

    async with _reference_whisper_lock:
        transcript = await asyncio.to_thread(_transcribe_blocking)
    if not transcript:
        return ""
    return f"Sampled audio transcript excerpt: {transcript}"


configure_catalyst_runtime_hooks(
    catalyst_hub_short_workspaces=CATALYST_HUB_SHORT_WORKSPACES,
    catalyst_hub_longform_workspaces=CATALYST_HUB_LONGFORM_WORKSPACES,
    catalyst_reference_analysis_default_minutes=CATALYST_REFERENCE_ANALYSIS_DEFAULT_MINUTES,
    catalyst_learning_records_getter=lambda: _catalyst_learning_records,
    catalyst_channel_memory_getter=lambda: _catalyst_channel_memory,
    catalyst_failure_mode_label=_catalyst_failure_mode_label,
    clip_text=_clip_text,
    load_youtube_connections=_load_youtube_connections,
    save_youtube_connections_runtime=_save_youtube_connections,
    youtube_bucket_for_user=_youtube_bucket_for_user,
    youtube_sync_and_persist_for_user=_youtube_sync_and_persist_for_user,
    youtube_connection_public_view=_youtube_connection_public_view,
    longform_owner_beta_enabled=_longform_owner_beta_enabled,
    is_admin_user=_is_admin_user,
    longform_deep_analysis_enabled=_longform_deep_analysis_enabled,
    active_longform_capacity_session_id=_active_longform_capacity_session_id,
    create_longform_session_internal=_create_longform_session_internal,
    bool_from_any=_bool_from_any,
    normalize_longform_target_minutes=_normalize_longform_target_minutes,
    normalize_longform_language=_normalize_longform_language,
    harvest_catalyst_outcomes_for_channel=_harvest_catalyst_outcomes_for_channel,
    longform_outcome_sync_semaphore=_longform_outcome_sync_semaphore,
    youtube_ensure_access_token=_youtube_ensure_access_token,
    youtube_fetch_channel_search=_youtube_fetch_channel_search,
    longform_sessions_lock=_longform_sessions_lock,
    load_longform_sessions_runtime=_load_longform_sessions,
    longform_sessions_getter=lambda: _longform_sessions,
    save_longform_sessions_runtime=_save_longform_sessions,
    match_published_video_to_longform_session=_match_published_video_to_longform_session,
    youtube_fetch_video_analytics=_youtube_fetch_video_analytics,
    build_auto_outcome_request=_build_auto_outcome_request,
    persist_catalyst_outcome_for_session=_persist_catalyst_outcome_for_session,
    longform_public_session=_longform_public_session,
    title_is_too_close_to_source=_title_is_too_close_to_source,
    shortform_priority_snapshot=lambda: _shortform_priority_snapshot(),
    youtube_selected_channel_context=_youtube_selected_channel_context,
    youtube_connected_channel_access_token=_youtube_connected_channel_access_token,
    youtube_fetch_public_channel_page_videos=_youtube_fetch_public_channel_page_videos,
    youtube_apply_public_inventory_to_snapshot=_youtube_apply_public_inventory_to_snapshot,
    youtube_historical_compare_measured_public_view=_youtube_historical_compare_measured_public_view,
    youtube_channel_audit_measured_public_view=_youtube_channel_audit_measured_public_view,
    load_catalyst_memory=_load_catalyst_memory,
    catalyst_channel_memory_key=_catalyst_channel_memory_key,
    catalyst_channel_memory_public_view=_catalyst_channel_memory_public_view,
    persist_public_shorts_playbook_memory=_persist_public_shorts_playbook_memory,
    public_shorts_playbook_from_memory_view=_public_shorts_playbook_from_memory_view,
    reconcile_reference_video_analysis_with_inventory=_reconcile_reference_video_analysis_with_inventory,
    catalyst_reference_video_analysis_public_view=_catalyst_reference_video_analysis_public_view,
    build_catalyst_reference_video_analysis=_build_catalyst_reference_video_analysis,
    resolve_catalyst_series_context=_resolve_catalyst_series_context,
    youtube_connections_lock=_youtube_connections_lock,
    catalyst_memory_lock=_catalyst_memory_lock,
    catalyst_hub_workspace_label=_catalyst_hub_workspace_label,
    xai_json_completion=_xai_json_completion,
    title_is_too_close_to_any=_title_is_too_close_to_any,
    catalyst_reference_workspace_profile=_catalyst_reference_workspace_profile,
    normalize_catalyst_reference_video_analysis=_normalize_catalyst_reference_video_analysis,
    catalyst_infer_niche=_catalyst_infer_niche,
    catalyst_infer_archetype=_catalyst_infer_archetype,
    same_arena_title_variants=_same_arena_title_variants,
    longform_review_state=_longform_review_state,
    heuristic_catalyst_longform_execution_qa=_heuristic_catalyst_longform_execution_qa,
    heuristic_catalyst_short_learning_record=_heuristic_catalyst_short_learning_record,
    update_catalyst_channel_memory=_update_catalyst_channel_memory,
    normalize_transition_style=_normalize_transition_style,
    jobs_getter=lambda: jobs,
    dedupe_preserve_order=_dedupe_preserve_order,
    render_catalyst_channel_memory_context=_render_catalyst_channel_memory_context,
    catalyst_rewrite_pressure_profile=_catalyst_rewrite_pressure_profile,
    build_catalyst_reference_playbook=_build_catalyst_reference_playbook,
    catalyst_rank_shorts_angle_candidates=_catalyst_rank_shorts_angle_candidates,
    build_shorts_public_reference_playbook=_build_shorts_public_reference_playbook,
    build_shorts_trend_query=_build_shorts_trend_query,
    youtube_fetch_public_trend_titles=_youtube_fetch_public_trend_titles,
    youtube_fetch_public_reference_shorts=_youtube_fetch_public_reference_shorts,
    youtube_title_keywords=_youtube_title_keywords,
    catalyst_reference_memory_getter=lambda: _catalyst_reference_memory,
    catalyst_reference_analysis_confidence_label=_catalyst_reference_analysis_confidence_label,
    dedupe_clip_list=_dedupe_clip_list,
    youtube_watch_url=_youtube_watch_url,
    save_catalyst_memory=_save_catalyst_memory,
    catalyst_reference_analysis_max_seconds=CATALYST_REFERENCE_ANALYSIS_MAX_SECONDS,
    catalyst_reference_audio_max_seconds=CATALYST_REFERENCE_AUDIO_MAX_SECONDS,
    normalize_external_source_url=lambda value: _normalize_external_source_url(value),
    reference_video_analysis_dir=_reference_video_analysis_dir,
    build_longform_operator_notes=_build_longform_operator_notes,
    manual_catalyst_reference_video_id=_manual_catalyst_reference_video_id,
    pick_catalyst_reference_video=_pick_catalyst_reference_video,
    pick_reference_preview_frame_urls=_pick_reference_preview_frame_urls,
    summarize_longform_operator_evidence=_summarize_longform_operator_evidence,
    extract_reference_video_stream_clip=_extract_reference_video_stream_clip,
    extract_reference_video_full_audit=_extract_reference_video_full_audit,
    extract_reference_preview_frames_from_urls=_extract_reference_preview_frames_from_urls,
    xai_json_completion_multimodal=_xai_json_completion_multimodal,
    fetch_source_video_bundle=_fetch_source_video_bundle,
    fetch_algrow_reference_enrichment=_fetch_algrow_reference_enrichment,
    youtube_fetch_video_analytics_bulk=_youtube_fetch_video_analytics_bulk,
    download_youtube_video_for_reference_analysis=_download_youtube_video_for_reference_analysis,
    audit_manual_comparison_video=_audit_manual_comparison_video,
    build_catalyst_analysis_proxy_video=_build_catalyst_analysis_proxy_video,
    extract_video_metadata=lambda *args, **kwargs: extract_video_metadata(*args, **kwargs),
    youtube_fetch_owned_video_bundle_oauth=_youtube_fetch_owned_video_bundle_oauth,
    extract_audio_from_video=lambda *args, **kwargs: extract_audio_from_video(*args, **kwargs),
    transcribe_audio_with_grok=lambda *args, **kwargs: transcribe_audio_with_grok(*args, **kwargs),
)


configure_clone_analysis_hooks(
    xai_api_key=XAI_API_KEY,
    clone_analysis_prompt=CLONE_ANALYSIS_PROMPT,
    template_system_prompts=TEMPLATE_SYSTEM_PROMPTS,
    heuristic_clone_analysis_fn=_heuristic_clone_analysis,
    clip_text=_clip_text,
)


async def extract_video_metadata(file_path: str) -> dict:
    """Extract basic metadata from uploaded video using ffprobe."""
    try:
        cmd = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_format", "-show_streams", str(file_path),
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        if proc.returncode == 0:
            data = json.loads(stdout.decode())
            duration = float(data.get("format", {}).get("duration", 0))
            streams = data.get("streams", [])
            video_stream = next((s for s in streams if s.get("codec_type") == "video"), {})
            return {
                "duration_sec": round(duration, 1),
                "width": int(video_stream.get("width", 0)),
                "height": int(video_stream.get("height", 0)),
                "fps": video_stream.get("r_frame_rate", "30/1"),
            }
    except Exception as e:
        log.warning(f"ffprobe metadata extraction failed: {e}")
    return {}


async def run_clone_pipeline(
    job_id: str,
    topic: str,
    video_path: str | None,
    source_url: str = "",
    analytics_notes: str = "",
    resolution: str = "720p",
):
    try:
        _job_set_stage(job_id, "analyzing", 5)
        effective_topic = str(topic or "").strip()
        log.info(f"[{job_id}] Clone: analyzing viral video for topic '{effective_topic or '[source-derived]'}'")

        video_context = effective_topic
        transcript_hint = ""
        meta = {}
        source_bundle = {}
        normalized_source_url = _normalize_external_source_url(source_url)
        if normalized_source_url:
            source_bundle = await _fetch_source_video_bundle(normalized_source_url)
            if source_bundle:
                jobs[job_id]["source_video"] = {
                    "source_url": str(source_bundle.get("source_url", "") or normalized_source_url),
                    "title": str(source_bundle.get("title", "") or ""),
                    "channel": str(source_bundle.get("channel", "") or ""),
                    "duration_sec": int(source_bundle.get("duration_sec", 0) or 0),
                    "thumbnail_url": str(source_bundle.get("thumbnail_url", "") or ""),
                    "public_summary": str(source_bundle.get("public_summary", "") or ""),
                }
                video_context = str(source_bundle.get("public_summary", "") or effective_topic)
                transcript_hint = str(source_bundle.get("transcript_excerpt", "") or "")
                if not effective_topic:
                    effective_topic = _same_arena_follow_up_topic(source_bundle, format_preset="documentary")
        if video_path:
            meta = await extract_video_metadata(video_path) or {}
            if meta:
                upload_context = (
                    "Source video file uploaded: "
                    + str(meta.get("duration_sec", "?")) + "s long, "
                    + str(meta.get("width", "?")) + "x" + str(meta.get("height", "?")) + " resolution"
                )
                video_context = f"{video_context}\n{upload_context}".strip()
            audio_path = await extract_audio_from_video(video_path)
            if audio_path:
                transcript_hint = await transcribe_audio_with_grok(audio_path) or transcript_hint
                Path(audio_path).unlink(missing_ok=True)
        if not effective_topic:
            effective_topic = _same_arena_follow_up_topic(source_bundle or {"title": ""}, format_preset="documentary") or "A sharper documentary topic"
        jobs[job_id]["topic"] = effective_topic

        analysis = await analyze_viral_video(effective_topic, video_context, transcript_hint, analytics_notes)
        detected_template = analysis.get("detected_template", "random")
        viral_info = analysis.get("viral_analysis", {})
        optimized_prompt = str(analysis.get("optimized_prompt", "") or "").strip()
        derived_follow_up_topic = str(viral_info.get("follow_up_topic", "") or "").strip()
        if derived_follow_up_topic and not str(topic or "").strip():
            effective_topic = derived_follow_up_topic
            jobs[job_id]["topic"] = effective_topic

        jobs[job_id]["template"] = detected_template
        jobs[job_id]["viral_analysis"] = viral_info
        if optimized_prompt:
            jobs[job_id]["optimized_prompt"] = optimized_prompt
        jobs[job_id]["progress"] = 12
        log.info(f"[{job_id}] Clone analysis: template={detected_template}, hook={viral_info.get('hook_type', '?')}, scenes={viral_info.get('scene_count', '?')}")

        if video_path:
            Path(video_path).unlink(missing_ok=True)

        _job_set_stage(job_id, "generating_script", 15)
        script_payload = dict(viral_info or {})
        if optimized_prompt:
            script_payload["optimized_prompt"] = optimized_prompt
        script_data = await generate_clone_script(detected_template, effective_topic, script_payload)
        scenes = _normalize_scenes_for_render(script_data.get("scenes", []))
        clone_quality_mode = _normalize_skeleton_quality_mode("cinematic", template=detected_template)
        clone_mint_mode = _normalize_mint_mode(True, template=detected_template)
        scenes = _apply_template_scene_constraints(scenes, detected_template, quality_mode=clone_quality_mode)
        scenes = _apply_mint_scene_compiler(scenes, detected_template, mint_mode=clone_mint_mode)
        scenes = _force_template_scene_duration(scenes, detected_template)
        if not scenes:
            raise ValueError("Clone script generation returned no scenes")

        fal_video_enabled = bool(FAL_AI_KEY)
        runway_video_enabled = bool(RUNWAY_API_KEY)
        use_video_engine = fal_video_enabled or runway_video_enabled
        use_video = use_video_engine
        if detected_template == "reddit":
            use_video = False
        if detected_template != "reddit" and not use_video_engine:
            raise RuntimeError("Video is required but no engine is configured (set RUNWAY_API_KEY or FAL_AI_KEY)")
        if runway_video_enabled:
            mode_label = "Runway (primary)"
        elif fal_video_enabled:
            mode_label = "FalAI Kling 2.1"
        else:
            mode_label = "static image"
        jobs[job_id]["generation_mode"] = "video" if use_video_engine else "image"

        _job_set_stage(job_id, "generating_images", 20)
        jobs[job_id]["total_scenes"] = len(scenes)
        log.info(f"[{job_id}] Clone script ready: {len(scenes)} scenes. Mode: {mode_label}, {resolution}")

        prompt_prefix = TEMPLATE_PROMPT_PREFIXES.get(detected_template, "")
        neg_prompt = TEMPLATE_NEGATIVE_PROMPTS.get(detected_template, NEGATIVE_PROMPT)
        scene_assets = []
        total_steps = len(scenes) * (2 if use_video else 1)
        gen_ts = str(int(time.time() * 1000))

        clone_skeleton_anchor = ""
        clone_skeleton_reference_image_url = SKELETON_GLOBAL_REFERENCE_IMAGE_URL if detected_template == "skeleton" else ""
        if detected_template == "skeleton" and scenes:
            clone_skeleton_anchor = _canonical_skeleton_anchor()

        for i, scene in enumerate(scenes):
            jobs[job_id]["current_scene"] = i + 1
            step_base = i * (2 if use_video else 1)
            _job_set_stage(job_id, "generating_images", 20 + int((step_base / total_steps) * 50))
            _job_record_scene_event(job_id, i, len(scenes), "image_start")

            if detected_template == "skeleton":
                vis_desc = scene.get("visual_description", "")
                full_prompt = _build_skeleton_image_prompt(
                    vis_desc,
                    skeleton_anchor=clone_skeleton_anchor,
                    quality_mode=clone_quality_mode,
                )
            else:
                full_prompt = prompt_prefix + scene.get("visual_description", "")
            img_path = str(TEMP_DIR / (job_id + "_scene_" + str(i) + ".png"))
            img_result = await generate_scene_image(
                full_prompt,
                img_path,
                resolution=resolution,
                negative_prompt=neg_prompt,
                template=detected_template,
                reference_image_url=clone_skeleton_reference_image_url if detected_template == "skeleton" else "",
                reference_lock_mode="strict",
            )
            if detected_template == "skeleton" and not clone_skeleton_reference_image_url and i == 0:
                clone_skeleton_reference_image_url = _file_to_data_image_url(img_path)
            cdn_url = img_result.get("cdn_url")
            engine_name = "Skeleton LoRA" if (detected_template == "skeleton" and not cdn_url) else ("Grok Imagine" if cdn_url else "SDXL")
            log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} image generated ({engine_name})")
            _job_record_scene_event(job_id, i, len(scenes), "image_ready", engine_name)

            asset = {"image": img_path, "frames": None, "kling_clip": None}

            if use_video:
                _job_set_stage(job_id, "animating_scenes", 20 + int(((step_base + 1) / total_steps) * 50))
                _job_record_scene_event(job_id, i, len(scenes), "animation_start")
                kling_motion = TEMPLATE_KLING_MOTION.get(detected_template, "Cinematic motion, smooth camera movement, subtle animation.")
                anim_prompt = scene.get("visual_description", "") + " " + kling_motion
                try:
                    anim_result = await animate_scene(
                        img_path, anim_prompt,
                        str(TEMP_DIR), i, gen_ts,
                        duration_sec=scene.get("duration_sec", 5),
                        image_cdn_url=cdn_url,
                        prefer_wan=(detected_template == "skeleton"),
                    )
                except Exception as anim_err:
                    jobs[job_id]["animation_warnings"] = int(jobs[job_id].get("animation_warnings", 0)) + 1
                    log.warning(f"[{job_id}] Scene {i+1}/{len(scenes)} animation failed, using static image: {anim_err}")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_failed", str(anim_err))
                    anim_result = {"type": "static"}
                if anim_result["type"] in ("kling_clip", "wan_clip", "grok_clip", "runway_clip"):
                    asset["kling_clip"] = anim_result["path"]
                    if anim_result["type"] == "runway_clip":
                        engine = "Runway"
                    elif anim_result["type"] == "grok_clip":
                        engine = "Grok Imagine Video"
                    elif anim_result["type"] == "kling_clip":
                        engine = "Kling 2.1"
                    else:
                        engine = "Wan 2.2"
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} animated by {engine}")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_ready", engine)
                else:
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} using static image")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_static")

            scene_assets.append(asset)

        _job_set_stage(job_id, "generating_voice", 75)
        log.info(f"[{job_id}] Generating voiceover...")

        full_narration = " ".join(s.get("narration", "") for s in scenes)
        audio_path = str(TEMP_DIR / (job_id + "_voice.mp3"))
        vo_result = await generate_voiceover(full_narration, audio_path, template=detected_template)
        audio_path = vo_result["audio_path"]
        word_timings = vo_result.get("word_timings", [])

        subtitle_path = None
        if word_timings:
            subtitle_path = str(TEMP_DIR / (job_id + "_captions.ass"))
            generate_ass_subtitles(word_timings, subtitle_path, resolution=resolution, template=detected_template)
            log.info(f"[{job_id}] Word-synced captions generated: {len(word_timings)} words")

        _job_set_stage(job_id, "compositing", 85)
        log.info(f"[{job_id}] Compositing final video at {resolution}...")

        output_filename = detected_template + "_" + job_id + ".mp4"
        output_path = str(OUTPUT_DIR / output_filename)
        await composite_video(
            scenes,
            scene_assets,
            audio_path,
            output_path,
            resolution=resolution,
            use_svd=use_video,
            subtitle_path=subtitle_path,
            job_id=job_id,
            minimum_scene_duration=5.0,
        )

        for asset in scene_assets:
            Path(asset["image"]).unlink(missing_ok=True)
            if asset.get("kling_clip"):
                Path(asset["kling_clip"]).unlink(missing_ok=True)
            if asset.get("frames"):
                for fp in asset["frames"]:
                    Path(fp).unlink(missing_ok=True)
                frame_dir = Path(asset["frames"][0]).parent
                if frame_dir.exists():
                    shutil.rmtree(frame_dir, ignore_errors=True)
        Path(audio_path).unlink(missing_ok=True)
        if subtitle_path:
            Path(subtitle_path).unlink(missing_ok=True)

        _job_set_stage(job_id, "complete", 100)
        jobs[job_id]["output_file"] = output_filename
        jobs[job_id]["resolution"] = resolution
        jobs[job_id]["metadata"] = {
            "title": script_data.get("title", effective_topic),
            "description": script_data.get("description", ""),
            "tags": script_data.get("tags", []),
        }
        _job_diag_finalize(job_id)
        await persist_job_state(job_id, jobs[job_id])
        log.info(f"[{job_id}] COMPLETE: {output_filename} ({resolution}, {mode_label})")

    except Exception as e:
        log.error(f"[{job_id}] Clone pipeline failed: {e}", exc_info=True)
        _job_set_stage(job_id, "error")
        jobs[job_id]["error"] = str(e)
        _job_diag_finalize(job_id)
        await persist_job_state(job_id, jobs[job_id])
        if video_path:
            Path(video_path).unlink(missing_ok=True)


async def _clone_video(
    topic: str = Form(""),
    resolution: str = Form("720p"),
    source_url: str = Form(""),
    analytics_notes: str = Form(""),
    file: UploadFile = File(None),
    background_tasks: BackgroundTasks = None,
    request: Request = None,
):
    if not XAI_API_KEY or not ELEVENLABS_API_KEY:
        raise HTTPException(500, "API keys not configured")

    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _user_has_paid_access(user):
        raise HTTPException(402, "Active subscription required. Please choose a plan.")

    res = _normalize_output_resolution(resolution, priority_allowed=False)
    normalized_source_url = _normalize_external_source_url(source_url)
    if not str(topic or "").strip() and not normalized_source_url and not (file and file.filename):
        raise HTTPException(400, "Provide a new topic, a source URL, or an uploaded source video.")

    video_path = None
    if file and file.filename:
        video_path = str(TEMP_DIR / f"clone_upload_{int(time.time())}.mp4")
        with open(video_path, "wb") as f:
            while chunk := await file.read(1024 * 1024):
                f.write(chunk)

    job_id = f"clone_{int(time.time())}_{random.randint(1000, 9999)}"
    jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "template": "analyzing...",
        "topic": topic,
        "source_url": normalized_source_url,
        "lane": "clone",
        "mode": "clone_rebuild",
        "resolution": res,
        "credit_cost": 0,
        "billing_source": "workspace_access",
        "user_id": str(user.get("id", "") or ""),
        "created_at": time.time(),
    }

    try:
        await enqueue_generation_job(
            job_id,
            "starter",
            run_clone_pipeline,
            (job_id, topic, video_path, normalized_source_url, analytics_notes, res),
        )
    except QueueFullError as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
        await persist_job_state(job_id, jobs[job_id])
        raise HTTPException(429, str(e))
    return {"status": "accepted", "job_id": job_id}


async def _list_jobs_payload():
    return {jid: {k: v for k, v in j.items() if k != "output_file"} for jid, j in jobs.items()}


app.include_router(
    build_media_router(
        auto_scene_image_handler=_auto_scene_image,
        auto_regenerate_scene_image_handler=_auto_regenerate_scene_image,
        job_status_handler=_job_status_payload,
        download_video_handler=_download_video_response,
        render_chat_story_handler=_render_chat_story,
        clone_video_handler=_clone_video,
        list_jobs_handler=_list_jobs_payload,
    )
)


# â”€â”€â”€ Stripe Payments â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


def _price_id_for_plan_id(plan_id: str) -> str:
    normalized = str(plan_id or "").strip().lower()
    for price_id, mapped_plan in STRIPE_PRICE_TO_PLAN.items():
        if str(mapped_plan or "").strip().lower() == normalized:
            return str(price_id or "").strip()
    return ""


async def _create_checkout(req: CheckoutRequest, user: dict = Depends(require_auth)):
    requested_product = str(getattr(req, "product", "") or "").strip().lower()
    requested_plan = str(getattr(req, "plan", "") or "").strip().lower()
    price_id = str(req.price_id or "").strip()
    if requested_product == "membership" and not requested_plan and not price_id:
        requested_plan = _default_membership_plan_id()
    if requested_plan and not price_id:
        price_id = _price_id_for_plan_id(requested_plan)
    if not price_id:
        raise HTTPException(400, "Missing price id")
    plan = str(STRIPE_PRICE_TO_PLAN.get(price_id, requested_plan) or "").strip().lower()
    if plan not in CHAT_STORY_ALLOWED_PLANS:
        raise HTTPException(400, "This membership plan is not available for PayPal checkout.")
    price_usd = float(PLAN_PRICE_USD.get(plan, 0.0) or 0.0)
    if price_usd <= 0:
        raise HTTPException(400, f"Membership pricing is not configured for {plan}.")
    checkout_url = await _create_paypal_subscription_order(user, price_id, plan, price_usd)
    return {"checkout_url": checkout_url}


async def _capture_paypal_order_api(order_id: str) -> tuple[dict, str]:
    capture = await _paypal_request("POST", f"/v2/checkout/orders/{order_id}/capture", json_body={})
    status = str(capture.get("status", "") or "").upper()
    capture_id = ""
    for purchase_unit in list(capture.get("purchase_units", []) or []):
        payments = purchase_unit.get("payments", {}) or {}
        captures = list(payments.get("captures", []) or [])
        if captures:
            capture_id = str(captures[0].get("id", "") or "")
            break
    if status not in {"COMPLETED", "APPROVED"} and not capture_id:
        raise HTTPException(400, "PayPal payment was not completed")
    return capture, capture_id


async def _create_paypal_subscription_order(user: dict, price_id: str, plan: str, price_usd: float) -> str:
    if not _paypal_enabled():
        raise HTTPException(400, "PayPal is not configured on this billing account yet.")
    normalized_plan = str(plan or "").strip().lower()
    if normalized_plan not in CHAT_STORY_ALLOWED_PLANS:
        raise HTTPException(400, "Only Starter, Creator, and Pro legacy plan IDs can be sold through membership checkout.")
    order_payload = {
        "intent": "CAPTURE",
        "purchase_units": [
            {
                "reference_id": str(price_id or ""),
                "custom_id": str(user.get("id", "") or ""),
                "description": f"NYPTID Studio Catalyst Membership ({normalized_plan.title()})",
                "amount": {
                    "currency_code": "USD",
                    "value": f"{float(price_usd or 0.0):.2f}",
                },
            }
        ],
        "application_context": {
            "brand_name": "NYPTID Studio",
            "landing_page": "LOGIN",
            "user_action": "PAY_NOW",
            "shipping_preference": "NO_SHIPPING",
            "return_url": f"{_api_public_url()}/api/paypal/return",
            "cancel_url": f"{_billing_site_url()}?page=subscription&subscription=cancelled&provider=paypal",
        },
    }
    data = await _paypal_request("POST", "/v2/checkout/orders", json_body=order_payload)
    order_id = str(data.get("id", "") or "")
    approve_url = ""
    for link in list(data.get("links", []) or []):
        if str(link.get("rel", "") or "").lower() == "approve":
            approve_url = str(link.get("href", "") or "")
            break
    if not order_id or not approve_url:
        raise HTTPException(500, "PayPal approval URL missing")
    async with _paypal_orders_lock:
        _paypal_orders[order_id] = {
            "kind": "subscription",
            "user_id": str(user.get("id", "") or ""),
            "email": str(user.get("email", "") or "").strip().lower(),
            "price_id": str(price_id or ""),
            "plan": normalized_plan,
            "price_usd": float(price_usd or 0.0),
            "activated": False,
            "capture_id": "",
            "created_at": time.time(),
        }
        _save_paypal_orders()
    return approve_url


async def _create_paypal_topup_order(user: dict, price_id: str, pack: dict) -> str:
    if not _paypal_enabled():
        raise HTTPException(400, "PayPal is not configured on this billing account yet.")
    order_payload = {
        "intent": "CAPTURE",
        "purchase_units": [
            {
                "reference_id": str(price_id or ""),
                "custom_id": str(user.get("id", "") or ""),
                "description": f"NYPTID Studio {str(pack.get('pack', '') or '').title()} AC Credits",
                "amount": {
                    "currency_code": "USD",
                    "value": f"{float(pack.get('price_usd', 0.0) or 0.0):.2f}",
                },
            }
        ],
        "application_context": {
            "brand_name": "NYPTID Studio",
            "landing_page": "LOGIN",
            "user_action": "PAY_NOW",
            "shipping_preference": "NO_SHIPPING",
            "return_url": f"{_api_public_url()}/api/paypal/return",
            "cancel_url": f"{_billing_site_url()}?topup=cancelled&provider=paypal",
        },
    }
    data = await _paypal_request("POST", "/v2/checkout/orders", json_body=order_payload)
    order_id = str(data.get("id", "") or "")
    approve_url = ""
    for link in list(data.get("links", []) or []):
        if str(link.get("rel", "") or "").lower() == "approve":
            approve_url = str(link.get("href", "") or "")
            break
    if not order_id or not approve_url:
        raise HTTPException(500, "PayPal approval URL missing")
    async with _paypal_orders_lock:
        _paypal_orders[order_id] = {
            "kind": "topup",
            "user_id": str(user.get("id", "") or ""),
            "email": str(user.get("email", "") or ""),
            "price_id": str(price_id or ""),
            "pack": str(pack.get("pack", "") or ""),
            "credits": int(pack.get("credits", 0) or 0),
            "price_usd": float(pack.get("price_usd", 0.0) or 0.0),
            "credited": False,
            "capture_id": "",
            "created_at": time.time(),
        }
        _save_paypal_orders()
    return approve_url


async def _capture_paypal_topup_order(order_id: str) -> dict:
    order_id = str(order_id or "").strip()
    if not order_id:
        raise HTTPException(400, "Missing PayPal order id")
    async with _paypal_orders_lock:
        order_meta = dict(_paypal_orders.get(order_id, {}) or {})
    if not order_meta:
        raise HTTPException(404, "PayPal order was not found")
    if order_meta.get("credited"):
        return order_meta
    _, capture_id = await _capture_paypal_order_api(order_id)
    async with _paypal_orders_lock:
        latest = dict(_paypal_orders.get(order_id, {}) or {})
        if latest.get("credited"):
            return latest
        await _credit_topup_wallet(
            user_id=str(latest.get("user_id", "") or ""),
            credits=int(latest.get("credits", 0) or 0),
            source=str(latest.get("pack", "paypal") or "paypal"),
            stripe_session_id=order_id,
        )
        latest["credited"] = True
        latest["capture_id"] = capture_id
        latest["captured_at"] = time.time()
        _paypal_orders[order_id] = latest
        _save_paypal_orders()
    await _append_landing_notification(
        event_type="topup",
            credits=int(latest.get("credits", 0) or 0),
            customer_email=str(latest.get("email", "") or ""),
    )
    return latest


async def _capture_paypal_subscription_order(order_id: str) -> dict:
    order_id = str(order_id or "").strip()
    if not order_id:
        raise HTTPException(400, "Missing PayPal order id")
    async with _paypal_orders_lock:
        order_meta = dict(_paypal_orders.get(order_id, {}) or {})
    if not order_meta:
        raise HTTPException(404, "PayPal order was not found")
    if str(order_meta.get("kind", "") or "").strip().lower() not in {"subscription", "monthly"}:
        raise HTTPException(400, "PayPal order is not a monthly subscription checkout")
    if order_meta.get("activated"):
        return order_meta
    _, capture_id = await _capture_paypal_order_api(order_id)
    user_id = str(order_meta.get("user_id", "") or "").strip()
    email = str(order_meta.get("email", "") or "").strip().lower()
    plan = str(order_meta.get("plan", "none") or "none").strip().lower()
    if plan not in CHAT_STORY_ALLOWED_PLANS:
        raise HTTPException(400, "PayPal subscription order is missing a valid plan")
    now_unix = int(time.time())
    current_user = {"id": user_id, "email": email, "plan": plan}
    current_record = _paypal_subscription_record_for_user(current_user)
    current_snapshot = _paypal_subscription_snapshot_for_user(current_user)
    period_start_unix = now_unix
    if current_snapshot.get("billing_active") and str(current_snapshot.get("record_plan", "none") or "none") == plan:
        active_end = int((current_record or {}).get("period_end_unix", 0) or 0)
        if active_end > now_unix:
            period_start_unix = active_end
    period_end_unix = _add_months_utc(period_start_unix, 1)
    if period_end_unix <= period_start_unix:
        period_end_unix = period_start_unix + (31 * 24 * 3600)
    subscription_key_candidates = _paypal_subscription_lookup_keys(user_id, email)
    subscription_key = subscription_key_candidates[0] if subscription_key_candidates else order_id
    subscription_record = {
        "provider": "paypal_manual",
        "user_id": user_id,
        "email": email,
        "plan": plan,
        "price_id": str(order_meta.get("price_id", "") or ""),
        "price_usd": float(order_meta.get("price_usd", 0.0) or 0.0),
        "order_id": order_id,
        "capture_id": capture_id,
        "status": "active",
        "period_start_unix": int(period_start_unix),
        "period_end_unix": int(period_end_unix),
        "created_at": float((current_record or {}).get("created_at", 0) or order_meta.get("created_at", time.time()) or time.time()),
        "updated_at": time.time(),
        "renewal_mode": "manual_paypal",
    }
    async with _paypal_subscriptions_lock:
        _paypal_subscriptions[subscription_key] = subscription_record
        _save_paypal_subscriptions()
    async with _paypal_orders_lock:
        latest = dict(_paypal_orders.get(order_id, {}) or {})
        if latest.get("activated"):
            return latest
        latest["activated"] = True
        latest["capture_id"] = capture_id
        latest["captured_at"] = time.time()
        latest["period_start_unix"] = int(period_start_unix)
        latest["period_end_unix"] = int(period_end_unix)
        latest["paypal_subscription_key"] = subscription_key
        _paypal_orders[order_id] = latest
        _save_paypal_orders()
    if user_id:
        await _supabase_set_user_plan(user_id, plan)
    await _append_landing_notification(
        event_type="subscription",
        plan=plan,
        customer_email=email,
    )
    return latest


async def _create_topup_checkout(req: TopupCheckoutRequest, user: dict = Depends(require_auth)):
    pack = TOPUP_PACKS.get(req.price_id)
    if not pack:
        raise HTTPException(400, "Invalid top-up pack")
    if user.get("email", "") in ADMIN_EMAILS:
        raise HTTPException(400, "Admin account does not require top-up packs")
    preferred_method = str(getattr(req, "preferred_method", "paypal") or "paypal").strip().lower()
    if preferred_method not in {"card", "paypal"}:
        preferred_method = "paypal"
    if preferred_method == "card" and not STRIPE_TOPUP_PUBLIC_ENABLED:
        raise HTTPException(400, "Stripe checkout is coming soon. Use PayPal for now.")
    if preferred_method == "paypal":
        checkout_url = await _create_paypal_topup_order(user, req.price_id, pack)
        return {"checkout_url": checkout_url}
    if not STRIPE_SECRET_KEY:
        raise HTTPException(500, "Stripe not configured")
    try:
        checkout_price_id = str(pack.get("stripe_price_id", "") or "").strip()
        line_item = (
            {"price": checkout_price_id, "quantity": 1}
            if checkout_price_id
            else {
                "price_data": {
                    "currency": "usd",
                    "product_data": {
                        "name": f"NYPTID Studio {str(pack.get('pack', '') or '').title()} AC Credits",
                        "description": f"{int(pack.get('credits', 0) or 0)} animation credits",
                    },
                    "unit_amount": int(round(float(pack.get("price_usd", 0.0) or 0.0) * 100)),
                },
                "quantity": 1,
            }
        )
        checkout_payload = {
            "mode": "payment",
            "line_items": [line_item],
            "success_url": f"{_billing_site_url()}?topup=success",
            "cancel_url": f"{_billing_site_url()}?topup=cancelled",
            "client_reference_id": user["id"],
            "metadata": {
                "user_id": user["id"],
                "topup_price_id": req.price_id,
                "topup_pack": str(pack.get("pack", "")),
                "topup_credits": str(int(pack.get("credits", 0) or 0)),
            },
        }
        customer_id = _stripe_find_customer_id_by_email(user["email"])
        if customer_id:
            checkout_payload["customer"] = customer_id
        else:
            checkout_payload["customer_email"] = user["email"]
        session = stripe_lib.checkout.Session.create(
            **checkout_payload,
            payment_method_types=["card"],
        )
        return {"checkout_url": session.url}
    except Exception as e:
        log.error(f"Stripe top-up checkout error: {e}")
        raise HTTPException(500, f"Payment error: {str(e)}")


async def _paypal_return(token: str = "", PayerID: str = ""):
    order_id = str(token or "").strip()
    async with _paypal_orders_lock:
        order_meta = dict(_paypal_orders.get(order_id, {}) or {})
    order_kind = str(order_meta.get("kind", "topup") or "topup").strip().lower()
    success_url = f"{_billing_site_url()}?topup=success&provider=paypal"
    error_url = f"{_billing_site_url()}?topup=cancelled&provider=paypal"
    if order_kind in {"subscription", "monthly"}:
        plan = str(order_meta.get("plan", "") or "").strip().lower()
        plan_suffix = f"&plan={quote(plan)}" if plan else ""
        success_url = f"{_billing_site_url()}?page=subscription&subscription=success&provider=paypal{plan_suffix}"
        error_url = f"{_billing_site_url()}?page=subscription&subscription=cancelled&provider=paypal{plan_suffix}"
    try:
        if order_kind in {"subscription", "monthly"}:
            await _capture_paypal_subscription_order(order_id)
        else:
            await _capture_paypal_topup_order(order_id)
        return RedirectResponse(url=success_url, status_code=302)
    except Exception as e:
        log.error(f"PayPal return/capture failed for {order_id} payer={PayerID}: {e}")
        message = quote(str(getattr(e, "detail", str(e)) or "PayPal capture failed")[:180])
        separator = "&" if "?" in error_url else "?"
        return RedirectResponse(url=f"{error_url}{separator}error={message}", status_code=302)


async def _create_billing_portal_session(user: dict = Depends(require_auth)):
    """Create a Stripe customer portal session so users can cancel/manage plans."""
    access_snapshot = _paid_access_snapshot_for_user(user)
    billing_active = bool(access_snapshot.get("billing_active"))
    access_source = str(access_snapshot.get("source", "") or "").strip().lower()
    if billing_active and access_source != "stripe":
        plan = str(access_snapshot.get("plan", "") or "").strip().lower()
        plan_suffix = f"&plan={quote(plan)}" if plan else ""
        provider_suffix = f"&provider={quote(access_source)}" if access_source else ""
        subscription_state = "manual" if access_source == "paypal_manual" else "details"
        return {
            "portal_url": (
                f"{_billing_site_url()}?page=subscription&subscription={subscription_state}"
                f"{provider_suffix}{plan_suffix}"
            )
        }
    if not STRIPE_SECRET_KEY:
        raise HTTPException(500, "Stripe not configured")
    customer_id = _stripe_find_customer_id_by_email(user.get("email", ""))
    if not customer_id:
        raise HTTPException(404, "No billing profile found yet. Complete a credit-pack purchase first.")
    try:
        portal = stripe_lib.billing_portal.Session.create(
            customer=customer_id,
            return_url=f"{_billing_site_url()}?billing=updated",
        )
        return {"portal_url": portal.url}
    except Exception as e:
        log.error(f"Stripe billing portal error: {e}")
        raise HTTPException(500, "Could not open billing portal")


async def _join_waitlist(req: WaitlistJoinRequest, user: dict = Depends(require_auth)):
    raise HTTPException(410, "Waiting list has been removed from Studio.")


async def _stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    try:
        if STRIPE_WEBHOOK_SECRET:
            event = stripe_lib.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
        else:
            event = json.loads(payload)
    except Exception as e:
        log.error(f"Webhook signature verification failed: {e}")
        raise HTTPException(400, "Invalid signature")

    if event.get("type") == "checkout.session.completed":
        session_data = event["data"]["object"]
        metadata = session_data.get("metadata", {}) or {}
        mode = str(session_data.get("mode", "") or "")
        user_id = str(session_data.get("client_reference_id") or metadata.get("user_id") or "")
        customer_email = str(
            (session_data.get("customer_details", {}) or {}).get("email")
            or session_data.get("customer_email")
            or ""
        )
        if mode == "subscription":
            plan = metadata.get("plan", "starter")
            if user_id and SUPABASE_URL:
                svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
                try:
                    async with httpx.AsyncClient(timeout=15) as client:
                        await client.post(
                            f"{SUPABASE_URL}/rest/v1/profiles",
                            headers={
                                "apikey": svc_key,
                                "Authorization": f"Bearer {svc_key}",
                                "Content-Type": "application/json",
                                "Prefer": "resolution=merge-duplicates",
                            },
                            json={"id": user_id, "plan": plan},
                        )
                    log.info(f"Stripe webhook: user {user_id} upgraded to {plan}")
                except Exception as e:
                    log.error(f"Failed to update plan for {user_id}: {e}")
            await _append_landing_notification(
                event_type="subscription",
                plan=str(plan or "starter"),
                customer_email=customer_email,
            )
        elif mode == "payment":
            checkout_kind = str(metadata.get("checkout_kind", "") or "")
            if checkout_kind == "waitlist_reservation":
                plan = str(metadata.get("plan", "") or "").strip().lower()
                plan_price_usd = float(metadata.get("plan_price_usd", PLAN_PRICE_USD.get(plan, 0.0)) or 0.0)
                if customer_email and plan in {"starter", "creator", "pro", "elite"}:
                    await _supabase_upsert_waitlist_entry(
                        email=customer_email,
                        plan=plan,
                        price_usd=plan_price_usd,
                        paid=True,
                    )
                    await _append_landing_notification(
                        event_type="subscription",
                        plan=plan,
                        customer_email=customer_email,
                    )
                    log.info(f"Stripe webhook: confirmed waitlist reservation {customer_email} -> {plan}")
                return {"status": "ok"}
            topup_credits = int(str(metadata.get("topup_credits", "0") or "0"))
            if user_id and topup_credits > 0:
                await _credit_topup_wallet(
                    user_id=user_id,
                    credits=topup_credits,
                    source=str(metadata.get("topup_pack", "topup") or "topup"),
                    stripe_session_id=str(session_data.get("id", "") or ""),
                )
                log.info(f"Stripe webhook: credited {topup_credits} top-up credits to {user_id}")
                await _append_landing_notification(
                    event_type="topup",
                    credits=topup_credits,
                    customer_email=customer_email,
                )

    elif event.get("type") in {"customer.subscription.created", "customer.subscription.updated", "customer.subscription.deleted"}:
        sub = event["data"]["object"]
        customer_email = str(sub.get("customer_email", "") or "")
        customer_id = str(sub.get("customer", "") or "")
        if not customer_email and customer_id and STRIPE_SECRET_KEY:
            try:
                customer = stripe_lib.Customer.retrieve(customer_id)
                customer_email = str(getattr(customer, "email", "") or "")
            except Exception as e:
                log.warning(f"Failed to resolve Stripe customer email from {customer_id}: {e}")
        if not customer_email:
            log.warning("Subscription lifecycle event received without resolvable customer email")
            return {"status": "ok"}

        event_type = str(event.get("type", "") or "")
        status = str(sub.get("status", "") or "")
        items = (((sub.get("items", {}) or {}).get("data", []) or []))
        active_price_id = ""
        if items:
            active_price_id = str((((items[0] or {}).get("price", {}) or {}).get("id", "") or ""))
        mapped_plan = STRIPE_PRICE_TO_PLAN.get(active_price_id, "")

        try:
            target_id = await _supabase_find_user_id_by_email(customer_email)
            if not target_id:
                log.warning(f"Subscription lifecycle event but no Supabase user found for {customer_email}")
                return {"status": "ok"}
            if event_type == "customer.subscription.deleted":
                await _supabase_set_user_plan(target_id, "none")
                log.info(f"Subscription deleted; downgraded {customer_email} -> none")
            elif status in {"active", "trialing", "past_due"} and mapped_plan:
                await _supabase_set_user_plan(target_id, mapped_plan)
                log.info(f"Subscription lifecycle sync: {customer_email} -> {mapped_plan} ({status})")
            elif status in {"canceled", "unpaid", "incomplete_expired"}:
                await _supabase_set_user_plan(target_id, "none")
                log.info(f"Subscription became inactive; downgraded {customer_email} -> none")
        except Exception as e:
            log.error(f"Failed subscription lifecycle sync for {customer_email}: {e}")

    elif event.get("type") in {"invoice.payment_failed", "invoice.payment_succeeded"}:
        inv = event["data"]["object"]
        customer_email = str(inv.get("customer_email", "") or "")
        customer_id = str(inv.get("customer", "") or "")
        if not customer_email and customer_id and STRIPE_SECRET_KEY:
            try:
                customer = stripe_lib.Customer.retrieve(customer_id)
                customer_email = str(getattr(customer, "email", "") or "")
            except Exception as e:
                log.warning(f"Failed to resolve invoice customer email from {customer_id}: {e}")
        if not customer_email:
            return {"status": "ok"}
        target_id = await _supabase_find_user_id_by_email(customer_email)
        if not target_id:
            return {"status": "ok"}
        if event.get("type") == "invoice.payment_failed":
            await _supabase_set_user_plan(target_id, "none")
            log.info(f"Invoice payment failed; downgraded {customer_email} -> none")
        else:
            # Keep profile + Stripe lifecycle in sync on successful recurring renewals.
            lines = (((inv.get("lines", {}) or {}).get("data", []) or []))
            price_id = ""
            if lines:
                price_id = str((((lines[0] or {}).get("price", {}) or {}).get("id", "") or ""))
            plan = STRIPE_PRICE_TO_PLAN.get(price_id, "")
            if plan:
                await _supabase_set_user_plan(target_id, plan)
                log.info(f"Invoice payment succeeded; ensured {customer_email} -> {plan}")

    return {"status": "ok"}


# â”€â”€â”€ Admin: set plan for a user (admin-only) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def _admin_set_plan(req: SetPlanRequest, user: dict = Depends(require_auth)):
    if user.get("email") not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin access required")
    if req.plan not in list(PLAN_LIMITS.keys()) + ["admin", "none"]:
        raise HTTPException(400, f"Invalid plan. Options: {list(PLAN_LIMITS.keys())}")
    if req.plan == "admin" and req.email not in ADMIN_EMAILS:
        raise HTTPException(400, "Only whitelisted admin emails can be admin.")

    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not svc_key or not SUPABASE_URL:
        raise HTTPException(500, "Supabase not configured")

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            users_resp = await client.get(
                f"{SUPABASE_URL}/auth/v1/admin/users?per_page=500",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                },
            )
            target_id = None
            if users_resp.status_code == 200:
                users_data = users_resp.json()
                user_list = users_data.get("users", users_data) if isinstance(users_data, dict) else users_data
                for u in user_list:
                    if u.get("email") == req.email:
                        target_id = u["id"]
                        break

            if not target_id:
                return {"error": f"User {req.email} not found in Supabase auth"}

            await client.post(
                f"{SUPABASE_URL}/rest/v1/profiles",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                    "Content-Type": "application/json",
                    "Prefer": "resolution=merge-duplicates",
                },
                json={"id": target_id, "plan": req.plan, "role": "admin" if req.email in ADMIN_EMAILS else "user"},
            )
        return {"status": "ok", "email": req.email, "plan": req.plan}
    except Exception as e:
        log.error(f"Admin set-plan error: {e}")
        raise HTTPException(500, str(e))


async def _admin_cancel_subscription(body: dict, user: dict = Depends(require_auth)):
    if user.get("email") not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin access required")
    if not STRIPE_SECRET_KEY:
        raise HTTPException(500, "Stripe not configured")

    target_email = str((body or {}).get("email", "") or "").strip().lower()
    if not target_email:
        raise HTTPException(400, "Missing email")
    cancel_now = _bool_from_any((body or {}).get("cancel_now"), False)

    try:
        customer_id = _stripe_find_customer_id_by_email(target_email)
        if not customer_id:
            raise HTTPException(404, f"No Stripe customer found for {target_email}")

        subs = stripe_lib.Subscription.list(customer=customer_id, status="all", limit=50)
        data = list(getattr(subs, "data", []) or [])
        canceled: list[dict] = []
        for sub in data:
            sub_id = str(getattr(sub, "id", "") or "")
            sub_status = str(getattr(sub, "status", "") or "")
            if not sub_id:
                continue
            if sub_status not in {"active", "trialing", "past_due", "incomplete"}:
                continue
            if cancel_now:
                stripe_lib.Subscription.delete(sub_id)
                action = "canceled_now"
            else:
                stripe_lib.Subscription.modify(sub_id, cancel_at_period_end=True)
                action = "cancel_at_period_end"
            canceled.append({"id": sub_id, "status": sub_status, "action": action})

        return {
            "ok": True,
            "email": target_email,
            "cancel_now": cancel_now,
            "canceled_count": len(canceled),
            "subscriptions": canceled,
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Admin cancel-subscription error for {target_email}: {e}")
        raise HTTPException(500, f"Failed to cancel subscription(s): {e}")


# â”€â”€â”€ User Feedback Collection â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def _submit_feedback(req: FeedbackRequest, user: dict = Depends(require_auth)):
    if req.rating < 1 or req.rating > 5:
        raise HTTPException(400, "Rating must be 1-5")

    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not SUPABASE_URL or not svc_key:
        raise HTTPException(500, "Feedback storage not configured")

    feedback_data = {
        "user_id": user["id"],
        "email": user.get("email", ""),
        "job_id": req.job_id,
        "rating": req.rating,
        "comment": req.comment[:2000] if req.comment else "",
        "template": req.template,
        "language": req.language,
        "feature": req.feature or "general",
        "plan": user.get("plan", "starter"),
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{SUPABASE_URL}/rest/v1/feedback",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
                json=feedback_data,
            )
            if resp.status_code not in (200, 201, 204):
                log.warning(f"Feedback insert failed ({resp.status_code}): {resp.text[:200]}")
                raise HTTPException(500, "Failed to save feedback")
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Feedback submit error: {e}")
        raise HTTPException(500, "Failed to save feedback")

    log.info(f"Feedback received: {req.rating}/5 from {user.get('email', '?')} for {req.feature} (job: {req.job_id[:20]})")
    return {"status": "ok"}


async def _get_all_feedback(user: dict = Depends(require_auth)):
    if user.get("email") not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin access required")

    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not SUPABASE_URL or not svc_key:
        return {"feedback": []}

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/feedback?order=created_at.desc&limit=500",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                avg_rating = sum(f.get("rating", 0) for f in data) / len(data) if data else 0
                return {
                    "feedback": data,
                    "total": len(data),
                    "avg_rating": round(avg_rating, 2),
                }
    except Exception as e:
        log.error(f"Feedback fetch error: {e}")

    return {"feedback": [], "total": 0, "avg_rating": 0}


async def _get_admin_kpi(user: dict = Depends(require_auth)):
    if user.get("email") not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin access required")
    total_jobs = int(_kpi_metrics.get("total_jobs", 0))
    completed_jobs = int(_kpi_metrics.get("completed_jobs", 0))
    first_pass = int(_kpi_metrics.get("first_render_pass_jobs", 0))
    total_time = float(_kpi_metrics.get("total_publishable_time_sec", 0.0))
    total_cost = float(_kpi_metrics.get("total_estimated_cost_usd", 0.0))
    success_rate = (completed_jobs / total_jobs) if total_jobs else 0.0
    first_render_success_rate = (first_pass / total_jobs) if total_jobs else 0.0
    avg_time_to_publishable_sec = (total_time / completed_jobs) if completed_jobs else 0.0
    avg_estimated_cost_per_short = (total_cost / max(total_jobs, 1)) if total_jobs else 0.0
    return {
        "targets": KPI_TARGETS,
        "metrics": {
            **_kpi_metrics,
            "success_rate": round(success_rate, 4),
            "first_render_success_rate": round(first_render_success_rate, 4),
            "avg_time_to_publishable_sec": round(avg_time_to_publishable_sec, 2),
            "avg_estimated_cost_per_short_usd": round(avg_estimated_cost_per_short, 3),
        },
        "on_track": {
            "first_render_success_rate": first_render_success_rate >= float(KPI_TARGETS["first_render_success_rate"]),
            "time_to_publishable_sec": avg_time_to_publishable_sec <= float(KPI_TARGETS["time_to_publishable_sec"]),
            "estimated_cost_per_short_usd": avg_estimated_cost_per_short <= float(KPI_TARGETS["estimated_cost_per_short_usd"]),
        },
    }


app.include_router(
    build_billing_router(
        create_checkout_endpoint=_create_checkout,
        create_topup_checkout_endpoint=_create_topup_checkout,
        paypal_return_endpoint=_paypal_return,
        create_billing_portal_session_endpoint=_create_billing_portal_session,
        join_waitlist_endpoint=_join_waitlist,
        stripe_webhook_endpoint=_stripe_webhook,
        admin_set_plan_endpoint=_admin_set_plan,
        admin_cancel_subscription_endpoint=_admin_cancel_subscription,
        submit_feedback_endpoint=_submit_feedback,
        get_all_feedback_endpoint=_get_all_feedback,
        get_admin_kpi_endpoint=_get_admin_kpi,
    )
)


# â”€â”€â”€ Startup: seed accounts â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

SEED_ACCOUNTS = {
    "omatic657@gmail.com": {"plan": "admin", "role": "admin"},
}


@app.on_event("startup")
async def seed_profiles():
    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not SUPABASE_URL or not svc_key:
        log.warning("Supabase not configured, skipping profile seeding")
        return

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            users_resp = await client.get(
                f"{SUPABASE_URL}/auth/v1/admin/users?per_page=500",
                headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
            )
            if users_resp.status_code != 200:
                log.warning(f"Could not list users for seeding (status {users_resp.status_code})")
                return

            users_data = users_resp.json()
            user_list = users_data.get("users", users_data) if isinstance(users_data, dict) else users_data

            for email, profile in SEED_ACCOUNTS.items():
                user_id = None
                for u in user_list:
                    if u.get("email") == email:
                        user_id = u["id"]
                        break
                if not user_id:
                    log.info(f"Seed user {email} not found in auth yet (will be seeded on first login)")
                    continue

                resp = await client.post(
                    f"{SUPABASE_URL}/rest/v1/profiles",
                    headers={
                        "apikey": svc_key,
                        "Authorization": f"Bearer {svc_key}",
                        "Content-Type": "application/json",
                        "Prefer": "resolution=merge-duplicates",
                    },
                    json={"id": user_id, "plan": profile["plan"], "role": profile["role"]},
                )
                log.info(f"Seeded {email} -> {profile['plan']} (status {resp.status_code})")
    except Exception as e:
        log.warning(f"Profile seeding failed: {e}")


# â”€â”€â”€ Thumbnail System â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

THUMBNAIL_DIR.mkdir(parents=True, exist_ok=True)
THUMBNAIL_UPLOAD_DIR = THUMBNAIL_DIR / "library"
THUMBNAIL_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
THUMBNAIL_OUTPUT_DIR = THUMBNAIL_DIR / "generated"
THUMBNAIL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
THUMBNAIL_SHARE_DIR = THUMBNAIL_DIR / "shared"
THUMBNAIL_SHARE_DIR.mkdir(parents=True, exist_ok=True)
THUMBNAIL_SHARE_STATE_FILE = THUMBNAIL_DIR / "share_links.json"
THUMBNAIL_STYLE_STATE_FILE = THUMBNAIL_DIR / "pikzels_style_profiles.json"
_thumbnail_share_links: dict[str, dict] = {}
_thumbnail_share_lock = asyncio.Lock()
_thumbnail_style_profiles: dict[str, dict] = {}
_thumbnail_style_lock = asyncio.Lock()

THUMBNAIL_RUNPOD_HOST = os.getenv("THUMBNAIL_RUNPOD_HOST", "root@69.30.85.41")
THUMBNAIL_RUNPOD_SSH_PORT = os.getenv("THUMBNAIL_RUNPOD_SSH_PORT", "22118")
RUNPOD_SSH = f"ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p {THUMBNAIL_RUNPOD_SSH_PORT} {THUMBNAIL_RUNPOD_HOST}"
RUNPOD_TRAINING_DIR = "/workspace/thumbnail_training/images"
LORA_NAME = "nyptid_thumbnails.safetensors"


def _load_thumbnail_share_links() -> None:
    global _thumbnail_share_links
    try:
        if THUMBNAIL_SHARE_STATE_FILE.exists():
            data = json.loads(THUMBNAIL_SHARE_STATE_FILE.read_text(encoding="utf-8"))
            _thumbnail_share_links = data if isinstance(data, dict) else {}
        else:
            _thumbnail_share_links = {}
    except Exception:
        _thumbnail_share_links = {}


def _save_thumbnail_share_links() -> None:
    THUMBNAIL_SHARE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    THUMBNAIL_SHARE_STATE_FILE.write_text(json.dumps(_thumbnail_share_links, indent=2), encoding="utf-8")


def _load_thumbnail_style_profiles() -> None:
    global _thumbnail_style_profiles
    try:
        if THUMBNAIL_STYLE_STATE_FILE.exists():
            data = json.loads(THUMBNAIL_STYLE_STATE_FILE.read_text(encoding="utf-8"))
            _thumbnail_style_profiles = data if isinstance(data, dict) else {}
        else:
            _thumbnail_style_profiles = {}
    except Exception:
        _thumbnail_style_profiles = {}


def _save_thumbnail_style_profiles() -> None:
    THUMBNAIL_STYLE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    THUMBNAIL_STYLE_STATE_FILE.write_text(json.dumps(_thumbnail_style_profiles, indent=2), encoding="utf-8")


def _thumbnail_style_profile_key(user: Optional[dict]) -> str:
    return _storage_safe_user_segment(user)


def _thumbnail_share_url(token: str) -> str:
    return f"{_api_public_url().rstrip('/')}/api/public/thumbnail-share/{quote(str(token or '').strip())}"


async def _register_thumbnail_share(path: Path, user: Optional[dict], ttl_sec: int = 7200) -> str:
    if not path.exists():
        raise FileNotFoundError(str(path))
    token = secrets.token_urlsafe(24)
    async with _thumbnail_share_lock:
        _load_thumbnail_share_links()
        _thumbnail_share_links[token] = {
            "path": str(path),
            "user_id": str((user or {}).get("id", "") or ""),
            "expires_at": int(time.time() + max(300, int(ttl_sec or 7200))),
        }
        _save_thumbnail_share_links()
    return token


def _thumbnail_library_images(user: Optional[dict]) -> list[Path]:
    library_dir = _thumbnail_library_dir_for_user(user) if user else THUMBNAIL_UPLOAD_DIR
    files = [
        p for p in library_dir.iterdir()
        if p.is_file() and p.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}
    ] if library_dir.exists() else []
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files


async def _build_thumbnail_style_zip(user: Optional[dict]) -> tuple[Path, list[str], str]:
    files = _thumbnail_library_images(user)[:3]
    if len(files) < 1:
        raise RuntimeError("Upload at least 1 thumbnail before building a style pack.")
    safe_user = _storage_safe_user_segment(user)
    signature = "|".join(
        f"{p.name}:{int(p.stat().st_size)}:{int(p.stat().st_mtime)}"
        for p in files
    )
    zip_path = THUMBNAIL_SHARE_DIR / f"{safe_user}_style_pack.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for idx, file_path in enumerate(files, start=1):
            ext = ".jpg" if file_path.suffix.lower() not in {".png", ".jpg", ".jpeg"} else file_path.suffix.lower()
            zf.write(file_path, arcname=f"reference_{idx}{ext}")
    return zip_path, [p.name for p in files], signature


def _pikzels_error_message(payload: dict, fallback: str) -> str:
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            message = str(error.get("message", "") or "").strip()
            details = str(error.get("details", "") or "").strip()
            if message and details:
                return f"{message}: {details}"
            if message:
                return message
    return fallback


async def _pikzels_request(method: str, path: str, json_body: Optional[dict] = None, params: Optional[dict] = None) -> dict:
    if not PIKZELS_API_KEY:
        raise RuntimeError("PIKZELS_API_KEY not configured")
    async with httpx.AsyncClient(timeout=90, follow_redirects=True) as client:
        resp = await client.request(
            method.upper(),
            f"https://api.pikzels.com{path}",
            headers={
                "X-Api-Key": PIKZELS_API_KEY,
                "Content-Type": "application/json",
            },
            json=json_body,
            params=params,
        )
    try:
        payload = resp.json()
    except Exception:
        payload = {}
    if resp.status_code not in (200, 201):
        raise RuntimeError(_pikzels_error_message(payload, f"Pikzels request failed ({resp.status_code})"))
    return payload if isinstance(payload, dict) else {}


async def _refresh_thumbnail_style_profile(user: Optional[dict]) -> dict:
    key = _thumbnail_style_profile_key(user)
    async with _thumbnail_style_lock:
        _load_thumbnail_style_profiles()
        profile = dict(_thumbnail_style_profiles.get(key, {}) or {})
    style_id = str(profile.get("style_id", "") or "").strip()
    if not style_id or not PIKZELS_API_KEY:
        return profile
    try:
        data = await _pikzels_request("GET", "/v1/style", params={"id": style_id})
        normalized_status = str(data.get("status", "") or "").strip().lower()
        profile.update({
            "style_id": style_id,
            "status": normalized_status,
            "progress": int(float(data.get("progress", 0) or 0)),
            "name": str(data.get("name", profile.get("name", "")) or ""),
            "preview_url": str(data.get("temp_portrait_url", profile.get("preview_url", "")) or ""),
            "last_checked_at": int(time.time()),
        })
        async with _thumbnail_style_lock:
            _load_thumbnail_style_profiles()
            _thumbnail_style_profiles[key] = profile
            _save_thumbnail_style_profiles()
    except Exception as e:
        profile["last_error"] = str(e)
    return profile


async def _train_thumbnail_style_profile(user: Optional[dict]) -> dict:
    if not PIKZELS_API_KEY:
        raise RuntimeError("PIKZELS_API_KEY not configured")
    zip_path, selected_files, signature = await _build_thumbnail_style_zip(user)
    key = _thumbnail_style_profile_key(user)
    safe_user = re.sub(r"[^a-zA-Z0-9_-]+", "", key)[:20] or "studio"
    async with _thumbnail_style_lock:
        _load_thumbnail_style_profiles()
        existing = dict(_thumbnail_style_profiles.get(key, {}) or {})
    if str(existing.get("library_signature", "") or "") == signature and str(existing.get("style_id", "") or "").strip():
        return await _refresh_thumbnail_style_profile(user)
    share_token = await _register_thumbnail_share(zip_path, user, ttl_sec=10800)
    style_name = f"{safe_user[:20]}-{int(time.time())}"[:25]
    result = await _pikzels_request(
        "POST",
        "/v1/style",
        json_body={
            "name": style_name,
            "training_data": _thumbnail_share_url(share_token),
        },
    )
    profile = {
        "style_id": str(result.get("id", "") or ""),
        "status": str(result.get("status", "queued") or "queued").strip().lower(),
        "progress": 0,
        "name": style_name,
        "preview_url": "",
        "library_signature": signature,
        "reference_files": selected_files,
        "last_synced_at": int(time.time()),
    }
    async with _thumbnail_style_lock:
        _load_thumbnail_style_profiles()
        _thumbnail_style_profiles[key] = profile
        _save_thumbnail_style_profiles()
    return profile


def _storage_safe_user_segment(user: Optional[dict]) -> str:
    raw = str((user or {}).get("id", "") or "").strip()
    if not raw:
        raw = str((user or {}).get("email", "") or "").strip().lower()
    safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", raw).strip("_")
    return safe or "anonymous"


def _thumbnail_library_dir_for_user(user: Optional[dict]) -> Path:
    path = THUMBNAIL_UPLOAD_DIR / _storage_safe_user_segment(user)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _thumbnail_output_dir_for_user(user: Optional[dict]) -> Path:
    path = THUMBNAIL_OUTPUT_DIR / _storage_safe_user_segment(user)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _thumbnail_scp_cmd(local_path: str, remote_path: str) -> str:
    return (
        f"scp -o StrictHostKeyChecking=no -P {THUMBNAIL_RUNPOD_SSH_PORT} "
        f"\"{local_path}\" {THUMBNAIL_RUNPOD_HOST}:{remote_path}"
    )


def _thumbnail_target_user_host() -> tuple[str, str]:
    if "@" in THUMBNAIL_RUNPOD_HOST:
        user, host = THUMBNAIL_RUNPOD_HOST.split("@", 1)
        return user, host
    return "root", THUMBNAIL_RUNPOD_HOST


def _sftp_mkdir_p(sftp, remote_dir: str):
    parts = [p for p in remote_dir.strip("/").split("/") if p]
    cur = "/"
    for part in parts:
        cur = f"{cur}{part}/"
        try:
            sftp.stat(cur)
        except Exception:
            sftp.mkdir(cur)


def _sync_file_to_runpod_blocking(local_path: str, remote_path: str) -> tuple[bool, str]:
    if not Path(local_path).exists():
        return False, "local file missing"

    if paramiko is not None:
        user, host = _thumbnail_target_user_host()
        client = None
        sftp = None
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=host,
                port=int(THUMBNAIL_RUNPOD_SSH_PORT),
                username=user,
                timeout=20,
                allow_agent=True,
                look_for_keys=True,
            )
            sftp = client.open_sftp()
            remote_parent = str(Path(remote_path).parent).replace("\\", "/")
            _sftp_mkdir_p(sftp, remote_parent)
            sftp.put(local_path, remote_path)
            return True, ""
        except Exception as e:
            log.warning(f"Paramiko thumbnail sync failed, falling back to scp: {e}")
        finally:
            try:
                if sftp:
                    sftp.close()
            except Exception:
                pass
            try:
                if client:
                    client.close()
            except Exception:
                pass

    cmd = _thumbnail_scp_cmd(local_path, remote_path)
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if proc.returncode == 0:
        return True, ""
    return False, (proc.stderr or proc.stdout or "scp failed")[:300]


def _runpod_target_user_host(host_value: str) -> tuple[str, str]:
    if "@" in host_value:
        user, host = host_value.split("@", 1)
        return user, host
    return "root", host_value


def _run_remote_cmd_blocking(host_value: str, port_value: str, remote_cmd: str) -> tuple[bool, str]:
    user, host = _runpod_target_user_host(host_value)
    if paramiko is not None:
        client = None
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=host,
                port=int(port_value),
                username=user,
                timeout=30,
                allow_agent=True,
                look_for_keys=True,
            )
            _, stdout, stderr = client.exec_command(remote_cmd, timeout=1800)
            out = stdout.read().decode(errors="ignore")
            err = stderr.read().decode(errors="ignore")
            code = stdout.channel.recv_exit_status()
            if code == 0:
                return True, out
            return False, (err or out or f"remote exit {code}")[:500]
        except Exception as e:
            return False, str(e)[:500]
        finally:
            try:
                if client:
                    client.close()
            except Exception:
                pass

    ssh_cmd = (
        f"ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p {port_value} "
        f"{host_value} \"{remote_cmd}\""
    )
    proc = subprocess.run(ssh_cmd, shell=True, capture_output=True, text=True)
    if proc.returncode == 0:
        return True, proc.stdout[:500]
    return False, (proc.stderr or proc.stdout or "ssh failed")[:500]


def _download_file_from_runpod_blocking(host_value: str, port_value: str, remote_path: str, local_path: str) -> tuple[bool, str]:
    local_parent = Path(local_path).parent
    local_parent.mkdir(parents=True, exist_ok=True)
    user, host = _runpod_target_user_host(host_value)

    if paramiko is not None:
        client = None
        sftp = None
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=host,
                port=int(port_value),
                username=user,
                timeout=30,
                allow_agent=True,
                look_for_keys=True,
            )
            sftp = client.open_sftp()
            sftp.get(remote_path, local_path)
            return True, ""
        except Exception as e:
            return False, str(e)[:500]
        finally:
            try:
                if sftp:
                    sftp.close()
            except Exception:
                pass
            try:
                if client:
                    client.close()
            except Exception:
                pass

    scp_cmd = (
        f"scp -o StrictHostKeyChecking=no -P {port_value} "
        f"{host_value}:{remote_path} \"{local_path}\""
    )
    proc = subprocess.run(scp_cmd, shell=True, capture_output=True, text=True)
    if proc.returncode == 0:
        return True, ""
    return False, (proc.stderr or proc.stdout or "scp download failed")[:500]


def _upload_file_to_runpod_blocking(host_value: str, port_value: str, local_path: str, remote_path: str) -> tuple[bool, str]:
    if not Path(local_path).exists():
        return False, "local file missing"
    user, host = _runpod_target_user_host(host_value)

    if paramiko is not None:
        client = None
        sftp = None
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=host,
                port=int(port_value),
                username=user,
                timeout=30,
                allow_agent=True,
                look_for_keys=True,
            )
            sftp = client.open_sftp()
            remote_parent = str(Path(remote_path).parent).replace("\\", "/")
            _sftp_mkdir_p(sftp, remote_parent)
            sftp.put(local_path, remote_path)
            return True, ""
        except Exception as e:
            return False, str(e)[:500]
        finally:
            try:
                if sftp:
                    sftp.close()
            except Exception:
                pass
            try:
                if client:
                    client.close()
            except Exception:
                pass

    scp_cmd = (
        f"scp -o StrictHostKeyChecking=no -P {port_value} "
        f"\"{local_path}\" {host_value}:{remote_path}"
    )
    proc = subprocess.run(scp_cmd, shell=True, capture_output=True, text=True)
    if proc.returncode == 0:
        return True, ""
    return False, (proc.stderr or proc.stdout or "scp upload failed")[:500]


async def sync_thumbnail_to_runpod(local_path: str) -> tuple[bool, str]:
    """SCP a thumbnail to RunPod's training images directory."""
    try:
        remote_path = f"{RUNPOD_TRAINING_DIR.rstrip('/')}/{Path(local_path).name}"
        ok, err = await asyncio.to_thread(_sync_file_to_runpod_blocking, local_path, remote_path)
        if ok:
            log.info(f"Synced {Path(local_path).name} to RunPod training dir")
            return True, ""
        log.warning(f"RunPod sync failed: {err}")
        return False, err
    except Exception as e:
        err = str(e)
        log.warning(f"RunPod thumbnail sync error: {err}")
        return False, err


async def check_lora_status(user: Optional[dict] = None) -> dict:
    """Expose thumbnail style-training status using Pikzels-backed profiles."""
    library_dir = _thumbnail_library_dir_for_user(user) if user else THUMBNAIL_UPLOAD_DIR
    local_count = len([p for p in library_dir.iterdir() if p.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}]) if library_dir.exists() else 0
    profile = await _refresh_thumbnail_style_profile(user)
    status = str(profile.get("status", "") or "").strip().lower()
    ready = status == "ready"
    is_training = status in {"queued", "analyzing", "training"}
    return {
        "lora_available": ready,
        "is_training": is_training,
        "total_images": len(list(profile.get("reference_files") or [])),
        "local_library_images": local_count,
        "trained_images": len(list(profile.get("reference_files") or [])),
        "version": 1 if ready else 0,
        "last_train": int(profile.get("last_synced_at", 0) or 0),
        "training_available": bool(PIKZELS_API_KEY) and local_count >= 1,
        "provider": "pikzels",
        "style_id": str(profile.get("style_id", "") or ""),
        "style_status": status,
        "portrait_url": str(profile.get("preview_url", "") or ""),
    }


async def _training_status(user: dict = Depends(require_auth)):
    return await check_lora_status(user)


async def _sync_thumbnail_library(user: dict = Depends(require_auth)):
    files = _thumbnail_library_images(user)
    if not files:
        return {"status": "no_files", "queued": 0, "synced": 0, "failed": 0}
    try:
        profile = await _train_thumbnail_style_profile(user)
    except Exception as e:
        raise HTTPException(400, str(e))
    return {
        "status": str(profile.get("status", "queued") or "queued"),
        "queued": min(3, len(files)),
        "synced": min(3, len(list(profile.get("reference_files") or []))),
        "failed": 0,
        "style_id": str(profile.get("style_id", "") or ""),
        "style_status": str(profile.get("status", "") or ""),
    }


async def _upload_thumbnails(
    files: list[UploadFile] = File(...),
    background_tasks: BackgroundTasks = None,
    user: dict = Depends(require_auth),
):
    library_dir = _thumbnail_library_dir_for_user(user)
    saved = []
    for file in files:
        if not file.filename:
            continue
        ext = Path(file.filename).suffix.lower()
        if ext not in {".png", ".jpg", ".jpeg", ".webp"}:
            continue
        ts = int(time.time() * 1000)
        safe_name = str(ts) + "_" + str(random.randint(1000, 9999)) + ext
        dest = library_dir / safe_name
        with open(dest, "wb") as f:
            while chunk := await file.read(1024 * 1024):
                f.write(chunk)
        saved.append({
            "id": safe_name,
            "name": file.filename,
            "size": dest.stat().st_size,
            "url": "/api/thumbnails/library/" + safe_name,
        })
        if background_tasks and _is_admin_user(user):
            background_tasks.add_task(sync_thumbnail_to_runpod, str(dest))
    return {"uploaded": len(saved), "files": saved}


async def _list_thumbnails(user: dict = Depends(require_auth)):
    library_dir = _thumbnail_library_dir_for_user(user)
    files = []
    for f in sorted(library_dir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if f.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}:
            files.append({
                "id": f.name,
                "name": f.name,
                "size": f.stat().st_size,
                "url": f"/api/thumbnails/library/{f.name}",
                "created_at": f.stat().st_mtime,
            })
    return {"files": files, "total": len(files)}


async def _thumbnail_feedback(req: ThumbnailFeedbackRequest, user: dict = Depends(require_auth)):
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    await _mark_training_feedback(
        req.generation_id,
        accepted=req.accepted,
        user_id=user.get("id", ""),
        event="thumbnail_feedback",
    )
    return {"ok": True, "generation_id": req.generation_id, "status": "accepted" if req.accepted else "rejected"}


async def _serve_thumbnail(filename: str, request: Request):
    user = await get_current_user_from_request(request)
    if not user:
        raise HTTPException(401, "Authentication required")
    path = _thumbnail_library_dir_for_user(user) / filename
    if not path.exists():
        raise HTTPException(404, "Thumbnail not found")
    mime = "image/png" if path.suffix == ".png" else "image/jpeg" if path.suffix in (".jpg", ".jpeg") else "image/webp"
    return FileResponse(str(path), media_type=mime)


async def _delete_thumbnail(filename: str, user: dict = Depends(require_auth)):
    path = _thumbnail_library_dir_for_user(user) / filename
    if path.exists():
        path.unlink()
    return {"status": "deleted"}


async def _serve_public_thumbnail_share(token: str):
    async with _thumbnail_share_lock:
        _load_thumbnail_share_links()
        entry = dict(_thumbnail_share_links.get(token, {}) or {})
        if not entry:
            raise HTTPException(404, "Shared asset not found")
        expires_at = int(entry.get("expires_at", 0) or 0)
        if expires_at > 0 and expires_at < int(time.time()):
            _thumbnail_share_links.pop(token, None)
            _save_thumbnail_share_links()
            raise HTTPException(410, "Shared asset expired")
    path = Path(str(entry.get("path", "") or ""))
    if not path.exists():
        raise HTTPException(404, "Shared asset missing")
    suffix = path.suffix.lower()
    media_type = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".zip": "application/zip",
    }.get(suffix, "application/octet-stream")
    return FileResponse(str(path), media_type=media_type, filename=path.name)


async def _serve_generated_thumbnail(filename: str, request: Request):
    user = await get_current_user_from_request(request)
    if not user:
        raise HTTPException(401, "Authentication required")
    path = _thumbnail_output_dir_for_user(user) / filename
    if not path.exists():
        raise HTTPException(404, "Generated thumbnail not found")
    mime = "image/png"
    return FileResponse(str(path), media_type=mime)


THUMBNAIL_ANALYSIS_PROMPT = """You are an elite YouTube thumbnail design strategist and art director. You analyze what makes thumbnails get clicks and generate precise production prompts that create thumbnails that outperform human designers.

You understand:
- Color psychology (red/yellow = urgency, blue = trust, contrast = attention)
- Face/emotion science (shocked expressions get 2-3x CTR)
- Text placement rules (big bold text, 3-5 words max, high contrast)
- Composition (rule of thirds, leading lines, depth)
- Platform-specific optimization (1920x1080 output, mobile-first readability)

When given a video description, style reference, or sketch, you produce a detailed SDXL image generation prompt that will create a click-worthy thumbnail.

Output MUST be valid JSON:
{
  "prompt": "Detailed thumbnail prompt. Include: subject, composition, lighting, colors, text elements, style, camera angle. Be extremely specific about every visual element. High-end professional YouTube thumbnail.",
  "negative_prompt": "Elements to avoid in the generation",
  "title_text": "The 3-5 word overlay text for the thumbnail (if applicable, empty string if none)",
  "style_notes": "Brief description of the design strategy being used"
}"""


THUMBNAIL_STYLE_TRANSFER_PROMPT = """You are an elite YouTube thumbnail design strategist. You will receive:
1. A description of the STYLE to emulate (from a reference thumbnail the user likes)
2. A description of what the NEW thumbnail should show

Your job: merge the visual style of the reference with the new content to create a prompt that produces a thumbnail in that exact style but with new content.

Analyze the style reference for: color palette, composition style, lighting mood, text treatment, overall aesthetic.
Then apply that exact style to the new content.

Output MUST be valid JSON:
{
  "prompt": "Detailed thumbnail prompt combining the reference style with new content. Professional YouTube thumbnail, 1920x1080 composition.",
  "negative_prompt": "Elements to avoid",
  "title_text": "The overlay text for this thumbnail (3-5 words, empty string if none)",
  "style_notes": "How the reference style was adapted"
}"""


THUMBNAIL_SCREENSHOT_PROMPT = """You are an elite YouTube thumbnail analyst and designer. The user has provided a description of their YouTube channel's existing thumbnails (or a description of what has worked for them before).

Your job: identify the patterns that made those thumbnails successful, then generate a NEW thumbnail prompt that follows the same winning formula but feels fresh and evolved.

Analyze for: recurring color schemes, text styles, composition patterns, emotional triggers, branding elements.

Output MUST be valid JSON:
{
  "prompt": "Detailed thumbnail prompt for a new thumbnail following the user's proven style. Professional YouTube thumbnail, 1920x1080.",
  "negative_prompt": "Elements to avoid",
  "title_text": "Overlay text (3-5 words, empty if none)",
  "style_notes": "Pattern analysis of what works for this channel",
  "patterns_detected": ["pattern1", "pattern2", "pattern3"]
}"""


THUMBNAIL_STYLE_PRESET_HINTS = {
    "red_machine": (
        "Style preset: black void or ultra-clean dark stage, one dominant red 3D subject/object, hard contrast, minimal composition, "
        "white block-label typography, glossy stylized 3D materials, and immediate thumbnail readability."
    ),
    "runner_void": (
        "Style preset: isolated hero on black background, strong red subject treatment, floating symbolic object, clean negative space, "
        "bright red/white typography blocks, crisp 3D lighting, and dramatic minimalism."
    ),
    "map_strike": (
        "Style preset: grayscale map or city-plan base with aggressive red highlighted territory/object, tactical infographic framing, "
        "clean high-contrast overlays, circular portrait inset if relevant, and bold documentary-news thumbnail clarity."
    ),
}


async def _generate_thumbnail_prompt(req: ThumbnailGenerateRequest) -> dict:
    style_preset_hint = THUMBNAIL_STYLE_PRESET_HINTS.get(str(req.style_preset or "").strip().lower(), "")
    if req.mode == "style_transfer":
        system_prompt = THUMBNAIL_STYLE_TRANSFER_PROMPT
        user_msg = f"STYLE REFERENCE: {req.description}\n\nNEW THUMBNAIL CONTENT: {req.screenshot_description or req.description}"
    elif req.mode == "screenshot_analysis":
        system_prompt = THUMBNAIL_SCREENSHOT_PROMPT
        user_msg = f"CHANNEL THUMBNAIL ANALYSIS: {req.screenshot_description or req.description}\n\nNEW VIDEO TO MAKE THUMBNAIL FOR: {req.description}"
    else:
        system_prompt = THUMBNAIL_ANALYSIS_PROMPT
        user_msg = f"Create a viral YouTube thumbnail for: {req.description}"
    if style_preset_hint:
        user_msg += f"\n\nPREFERRED STYLE DIRECTION: {style_preset_hint}"

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {XAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "grok-3-mini-fast",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_msg},
                ],
                "temperature": 0.7,
            },
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        start = content.find("{")
        end = content.rfind("}") + 1
        if start == -1 or end == 0:
            raise ValueError("No JSON in thumbnail AI response")
        return json.loads(content[start:end])


async def _check_lora_exists() -> bool:
    """Quick check if the trained LoRA file exists in ComfyUI."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(COMFYUI_URL + "/object_info/LoraLoader")
            if resp.status_code == 200:
                data = resp.json()
                lora_info = data.get("LoraLoader", {}).get("input", {}).get("required", {})
                lora_names = lora_info.get("lora_name", [[]])[0]
                return LORA_NAME in lora_names
    except Exception:
        pass
    return False


async def _enforce_thumbnail_1080(output_path: str) -> str:
    """Force final thumbnail image to exactly 1920x1080."""
    fixed_out = str(Path(output_path).with_name(Path(output_path).stem + "_1920x1080.png"))
    cmd = [
        "ffmpeg", "-y",
        "-i", output_path,
        "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,setsar=1",
        fixed_out,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError("Failed to enforce 1920x1080 thumbnail: " + stderr.decode()[-200:])
    fixed = Path(fixed_out)
    if not fixed.exists() or fixed.stat().st_size == 0:
        raise RuntimeError("1920x1080 thumbnail output missing")
    Path(output_path).unlink(missing_ok=True)
    fixed.rename(output_path)
    return output_path


def _thumbnail_fal_model_candidates() -> list[str]:
    ordered = ["seedream45", "imagen4_fast", "recraft_v4", "grok_imagine", "imagen4_ultra"]
    deduped: list[str] = []
    for candidate in ordered:
        profile = dict(CREATIVE_IMAGE_MODEL_MAP.get(candidate) or {})
        if not profile or not bool(profile.get("enabled", False)):
            continue
        model_id = str(profile.get("id", "") or "").strip().lower()
        if model_id and model_id not in deduped:
            deduped.append(model_id)
    return deduped


async def _generate_thumbnail_image(
    prompt: str,
    negative_prompt: str,
    output_path: str,
    user: Optional[dict],
    mode: str = "describe",
    style_ref_path: str = "",
) -> dict:
    """Generate a thumbnail via fal.ai first, then fallback to Pikzels."""
    mode_normalized = str(mode or "describe").strip().lower()
    composed_prompt = str(prompt or "").strip()
    if negative_prompt:
        composed_prompt = f"{composed_prompt}\nAvoid: {negative_prompt.strip()}"
    if style_ref_path and Path(style_ref_path).exists():
        # Fal lanes currently do not support this local style-reference transfer endpoint.
        composed_prompt = (
            composed_prompt
            + "\nStyle lock: preserve the same thumbnail clarity, contrast, and visual hierarchy as the selected studio reference style."
        )

    fal_errors: list[str] = []
    for model_id in _thumbnail_fal_model_candidates():
        try:
            fal_result = await _generate_image_fal_selected_model(
                model_id,
                composed_prompt,
                output_path,
                resolution="1080p_landscape",
                negative_prompt=negative_prompt,
            )
            return {
                "path": output_path,
                "output_url": str(fal_result.get("cdn_url", "") or ""),
                "request_id": "",
                "style_id": "",
                "provider_mode": f"fal_{model_id}",
                "provider": str(fal_result.get("provider", model_id) or model_id),
                "provider_label": str(fal_result.get("provider_label", model_id) or model_id),
            }
        except Exception as fal_err:
            fal_errors.append(f"{model_id}: {fal_err}")

    if not PIKZELS_API_KEY:
        detail = " | ".join(fal_errors[-3:]) if fal_errors else "no fal thumbnail models available"
        raise RuntimeError(f"Thumbnail generation failed: {detail}")

    profile = await _refresh_thumbnail_style_profile(user)
    style_id = ""
    if str(profile.get("status", "") or "").strip().lower() == "ready":
        style_id = str(profile.get("style_id", "") or "").strip()

    if style_ref_path and Path(style_ref_path).exists():
        ref_token = await _register_thumbnail_share(Path(style_ref_path), user, ttl_sec=7200)
        body = {
            "prompt": composed_prompt,
            "image_url": _thumbnail_share_url(ref_token),
            "model": PIKZELS_RECREATE_MODEL,
            "format": "16:9",
        }
        if style_id:
            body["style"] = style_id
        result = await _pikzels_request("POST", "/v1/recreate", json_body=body)
        provider_mode = "recreate"
    else:
        body = {
            "prompt": composed_prompt,
            "model": PIKZELS_THUMBNAIL_MODEL,
            "format": "16:9",
        }
        if style_id and mode_normalized in {"describe", "screenshot_analysis", "style_transfer"}:
            body["style"] = style_id
        result = await _pikzels_request("POST", "/v1/thumbnail", json_body=body)
        provider_mode = "thumbnail"

    output_url = str(result.get("output", "") or "").strip()
    if not output_url:
        raise RuntimeError("Pikzels did not return an output URL")
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        download = await client.get(output_url)
        if download.status_code != 200:
            raise RuntimeError("Failed to download Pikzels thumbnail output")
    with open(output_path, "wb") as f:
        f.write(download.content)
    return {
        "path": output_path,
        "output_url": output_url,
        "request_id": str(result.get("request_id", "") or ""),
        "style_id": style_id,
        "provider_mode": provider_mode,
        "provider": "pikzels",
        "provider_label": "Pikzels",
    }


async def _generate_thumbnail(req: ThumbnailGenerateRequest, background_tasks: BackgroundTasks, user: dict = Depends(require_auth)):
    if not XAI_API_KEY:
        raise HTTPException(500, "XAI_API_KEY not configured")
    if not FAL_AI_KEY and not PIKZELS_API_KEY:
        raise HTTPException(500, "Thumbnail image backend not configured (set FAL_AI_KEY or PIKZELS_API_KEY)")
    library_dir = _thumbnail_library_dir_for_user(user)
    output_dir = _thumbnail_output_dir_for_user(user)

    job_id = f"thumb_{int(time.time())}_{random.randint(1000, 9999)}"
    jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "type": "thumbnail",
        "lane": "thumbnails",
        "mode": req.mode,
        "credit_cost": 0,
        "billing_source": "workspace_access",
        "user_id": str(user.get("id", "") or ""),
        "created_at": time.time(),
    }

    async def _run_thumbnail_pipeline():
        try:
            jobs[job_id]["status"] = "analyzing"
            jobs[job_id]["progress"] = 10
            log.info(f"[{job_id}] Thumbnail gen: mode={req.mode}")

            ai_result = await _generate_thumbnail_prompt(req)
            thumb_prompt = ai_result.get("prompt", req.description)
            thumb_negative = ai_result.get("negative_prompt", NEGATIVE_PROMPT)
            title_text = ai_result.get("title_text", "")
            style_notes = ai_result.get("style_notes", "")
            if title_text:
                thumb_prompt = f"{thumb_prompt} Prominent thumbnail text overlay: {title_text}."

            jobs[job_id]["status"] = "generating"
            jobs[job_id]["progress"] = 30
            jobs[job_id]["ai_analysis"] = {
                "title_text": title_text,
                "style_notes": style_notes,
                "patterns": ai_result.get("patterns_detected", []),
            }

            style_ref_path = ""
            if req.style_reference_id:
                ref_path = library_dir / req.style_reference_id
                if ref_path.exists():
                    style_ref_path = str(ref_path)

            output_name = f"{job_id}.png"
            output_path = str(output_dir / output_name)
            render_result = await _generate_thumbnail_image(
                thumb_prompt,
                thumb_negative,
                output_path,
                user=user,
                mode=req.mode,
                style_ref_path=style_ref_path,
            )

            await _enforce_thumbnail_1080(output_path)
            provider_key = str(render_result.get("provider", "pikzels") or "pikzels")
            provider_mode = str(render_result.get("provider_mode", "") or "")
            thumb_gen_id = await _save_training_candidate(
                thumb_prompt,
                output_path,
                template="thumbnail",
                source=provider_key,
                metadata={
                    "mode": req.mode,
                    "title_text": title_text,
                    "user_id": user.get("id", ""),
                    "provider_mode": provider_mode,
                    "provider_request_id": str(render_result.get("request_id", "") or ""),
                    "output_url": str(render_result.get("output_url", "") or ""),
                    "style_id": str(render_result.get("style_id", "") or ""),
                },
            )

            jobs[job_id]["status"] = "complete"
            jobs[job_id]["progress"] = 100
            jobs[job_id]["output_file"] = output_name
            jobs[job_id]["output_url"] = f"/api/thumbnails/generated/{output_name}"
            jobs[job_id]["generation_id"] = thumb_gen_id
            jobs[job_id]["provider"] = provider_key
            jobs[job_id]["provider_label"] = str(render_result.get("provider_label", provider_key) or provider_key)
            jobs[job_id]["provider_mode"] = provider_mode
            jobs[job_id]["provider_request_id"] = str(render_result.get("request_id", "") or "")
            log.info(f"[{job_id}] Thumbnail COMPLETE: {output_name}")

        except Exception as e:
            log.error(f"[{job_id}] Thumbnail pipeline failed: {e}", exc_info=True)
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = str(e)

    background_tasks.add_task(_run_thumbnail_pipeline)
    return {"status": "accepted", "job_id": job_id}


COMPRESS_THRESHOLD_MB = 50


async def compress_video_if_needed(video_path: str, job_id: str, label: str = "demo") -> str:
    """Compress video to 720p if file exceeds COMPRESS_THRESHOLD_MB. Returns path (possibly new)."""
    file_size_mb = Path(video_path).stat().st_size / (1024 * 1024)
    if file_size_mb <= COMPRESS_THRESHOLD_MB:
        return video_path

    log.info(f"[{job_id}] {label} video is {file_size_mb:.0f}MB (>{COMPRESS_THRESHOLD_MB}MB), compressing to 720p...")
    jobs[job_id]["status"] = f"compressing_{label}"
    jobs[job_id]["progress"] = jobs[job_id].get("progress", 0)
    jobs[job_id]["compress_info"] = {
        "label": label,
        "original_size_mb": round(file_size_mb, 1),
        "target": "720p",
    }

    compressed_path = video_path.rsplit(".", 1)[0] + "_compressed.mp4"
    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-vf", "scale=-2:720",
        "-c:v", "libx264", "-preset", "fast", "-crf", "28",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        compressed_path
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    _, stderr_data = await proc.communicate()

    if proc.returncode == 0 and Path(compressed_path).exists():
        new_size_mb = Path(compressed_path).stat().st_size / (1024 * 1024)
        log.info(f"[{job_id}] {label} compressed: {file_size_mb:.0f}MB -> {new_size_mb:.0f}MB")
        jobs[job_id]["compress_info"]["compressed_size_mb"] = round(new_size_mb, 1)
        Path(video_path).unlink(missing_ok=True)
        return compressed_path
    else:
        log.warning(f"[{job_id}] {label} compression failed, using original. stderr: {stderr_data.decode()[-300:]}")
        Path(compressed_path).unlink(missing_ok=True)
        return video_path


async def run_demo_pipeline(job_id: str, demo_path: str, ref_path: str, face_path: str,
                            product_name: str, reference_notes: str,
                            pip_position: str = "bottom-right", voice_id: str = ""):
    """Full product demo video generation pipeline."""
    try:
        ref_style = ""
        if ref_path and Path(ref_path).exists():
            jobs[job_id]["status"] = "analyzing_reference"
            jobs[job_id]["progress"] = 3
            log.info(f"[{job_id}] Analyzing reference video for style...")
            ref_analysis = await analyze_screen_recording(ref_path)
            ref_style = (
                f"Match the style, pacing, and energy of this reference video: "
                f"{ref_analysis.get('description', '')}. "
                f"Duration: {ref_analysis.get('duration', 0):.0f}s, "
                f"{ref_analysis.get('frame_count', 0)} key frames analyzed."
            )
            Path(ref_path).unlink(missing_ok=True)

        jobs[job_id]["status"] = "analyzing"
        jobs[job_id]["progress"] = 5
        log.info(f"[{job_id}] Demo pipeline: analyzing demo video...")

        analysis = await analyze_screen_recording(demo_path)
        jobs[job_id]["analysis"] = {
            "duration": analysis["duration"],
            "frame_count": analysis["frame_count"]
        }
        jobs[job_id]["progress"] = 20
        log.info(f"[{job_id}] Screen analyzed: {analysis['frame_count']} frames, {analysis['duration']:.1f}s")

        jobs[job_id]["status"] = "scripting"
        jobs[job_id]["progress"] = 25
        log.info(f"[{job_id}] Generating demo script...")

        full_ref_notes = reference_notes
        if ref_style:
            full_ref_notes = ref_style + ("\n" + reference_notes if reference_notes else "")
        script_data = await generate_demo_script(analysis, product_name, full_ref_notes)
        jobs[job_id]["script"] = script_data
        jobs[job_id]["progress"] = 40
        log.info(f"[{job_id}] Script ready: {len(script_data.get('segments', []))} segments")

        jobs[job_id]["status"] = "generating_voice"
        jobs[job_id]["progress"] = 45
        log.info(f"[{job_id}] Generating voiceover...")

        full_narration = " ".join(seg.get("text", seg.get("narration", "")) for seg in script_data.get("segments", []))
        audio_path = str(TEMP_DIR / (job_id + "_demo_voice.mp3"))
        vo_result = await generate_voiceover(full_narration, audio_path, template="motivation", override_voice_id=voice_id)
        audio_path = vo_result["audio_path"]
        word_timings = vo_result.get("word_timings", [])
        jobs[job_id]["progress"] = 60

        # Keep demo narration aligned to the uploaded recording duration without rushed speech.
        target_demo_dur = float(analysis.get("duration", 0.0) or 0.0)
        source_audio_dur = _probe_audio_duration_seconds(audio_path)
        if target_demo_dur > 0.2 and source_audio_dur > 0.2:
            speed_ratio = source_audio_dur / target_demo_dur
            if source_audio_dur < (target_demo_dur - 0.08):
                fit_audio_path = str(TEMP_DIR / (job_id + "_demo_voice_fit.mp3"))
                # Only slow down short narration; never speed up long narration (sounds rushed).
                gentle_ratio = max(speed_ratio, 0.88)
                atempo_chain = _build_atempo_filter_chain(gentle_ratio)
                fit_cmd = [
                    "ffmpeg", "-y", "-i", audio_path,
                    "-af", f"{atempo_chain},apad=pad_dur={target_demo_dur + 0.25:.3f}",
                    "-t", f"{target_demo_dur:.3f}",
                    "-c:a", "libmp3lame", "-b:a", "192k",
                    fit_audio_path,
                ]
                fit_proc = await asyncio.create_subprocess_exec(
                    *fit_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                _, fit_err = await fit_proc.communicate()
                if fit_proc.returncode == 0 and Path(fit_audio_path).exists():
                    Path(audio_path).unlink(missing_ok=True)
                    audio_path = fit_audio_path
                    if word_timings:
                        remapped = []
                        for wt in word_timings:
                            start = max(0.0, float(wt.get("start", 0.0)) / gentle_ratio)
                            end = max(start + 0.01, float(wt.get("end", start)) / gentle_ratio)
                            if start <= target_demo_dur + 0.15:
                                remapped.append({
                                    "word": wt.get("word", ""),
                                    "start": start,
                                    "end": min(end, target_demo_dur + 0.15),
                                })
                        word_timings = remapped
                    jobs[job_id]["audio_sync"] = {
                        "source_duration": round(source_audio_dur, 3),
                        "target_duration": round(target_demo_dur, 3),
                        "speed_ratio": round(gentle_ratio, 5),
                        "mode": "slowdown_and_pad",
                    }
                    log.info(f"[{job_id}] Demo voiceover fit to video duration ({source_audio_dur:.2f}s -> {target_demo_dur:.2f}s)")
                else:
                    log.warning(f"[{job_id}] Demo voiceover duration-fit failed, using original audio: {(fit_err.decode(errors='ignore')[-200:])}")
            else:
                jobs[job_id]["audio_sync"] = {
                    "source_duration": round(source_audio_dur, 3),
                    "target_duration": round(target_demo_dur, 3),
                    "speed_ratio": 1.0,
                    "mode": "no_speedup",
                }

        subtitle_path = None
        if word_timings:
            demo_w, demo_h = 1920, 1080
            try:
                probe_cmd = [
                    "ffprobe", "-v", "error", "-show_entries", "stream=width,height",
                    "-of", "csv=p=0:s=x", demo_path
                ]
                p = await asyncio.create_subprocess_exec(
                    *probe_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                out, _ = await p.communicate()
                dims = out.decode().strip().split("\n")[0]
                if "x" in dims:
                    demo_w, demo_h = int(dims.split("x")[0]), int(dims.split("x")[1])
            except Exception:
                pass
            subtitle_path = str(TEMP_DIR / (job_id + "_demo_captions.ass"))
            generate_ass_subtitles(word_timings, subtitle_path, video_width=demo_w, video_height=demo_h)

        jobs[job_id]["status"] = "generating_face"
        jobs[job_id]["progress"] = 65

        talking_head_path = None
        has_face = False

        try:
            if face_path and Path(face_path).exists():
                log.info(f"[{job_id}] Generating talking head animation...")
                talking_head_path = str(TEMP_DIR / (job_id + "_talking_head.mp4"))
                await generate_talking_head(face_path, audio_path, talking_head_path)
                has_face = True
                log.info(f"[{job_id}] Talking head generated")
            else:
                log.info(f"[{job_id}] Face PiP disabled (no face image uploaded)")
                talking_head_path = None
                has_face = False
            jobs[job_id]["progress"] = 80
        except Exception as face_err:
            log.warning(f"[{job_id}] Talking head failed ({face_err}), continuing without face PiP")
            talking_head_path = None
            has_face = False
            jobs[job_id]["progress"] = 80

        jobs[job_id]["status"] = "compositing"
        jobs[job_id]["progress"] = 85
        log.info(f"[{job_id}] Compositing demo video...")

        output_filename = f"demo_{job_id}.mp4"
        output_path = str(OUTPUT_DIR / output_filename)

        if has_face and talking_head_path and Path(talking_head_path).exists():
            await composite_demo_video(
                demo_path, talking_head_path, audio_path,
                output_path, subtitle_path=subtitle_path,
                pip_position=pip_position
            )
        else:
            sub_filter = ""
            if subtitle_path and Path(subtitle_path).exists():
                sub_abs = str(Path(subtitle_path).resolve()).replace("\\", "/").replace(":", "\\:")
                sub_filter = f"-vf ass={sub_abs}"
            cmd = ["ffmpeg", "-y", "-i", demo_path, "-i", audio_path]
            if sub_filter:
                cmd += ["-vf", f"ass={sub_abs}"]
            cmd += [
                "-map", "0:v:0",
                "-map", "1:a:0",
                "-c:v", "libx264", "-preset", "fast", "-crf", "20",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest", "-movflags", "+faststart",
                output_path
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            await proc.communicate()
            if not Path(output_path).exists():
                cmd_simple = ["ffmpeg", "-y", "-i", demo_path, "-i", audio_path,
                              "-c:v", "libx264", "-preset", "fast", "-crf", "20",
                              "-c:a", "aac", "-b:a", "192k", "-shortest", output_path]
                proc = await asyncio.create_subprocess_exec(
                    *cmd_simple, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                await proc.communicate()

        jobs[job_id]["progress"] = 95

        if talking_head_path:
            Path(talking_head_path).unlink(missing_ok=True)
        Path(audio_path).unlink(missing_ok=True)
        if subtitle_path:
            Path(subtitle_path).unlink(missing_ok=True)

        jobs[job_id]["status"] = "complete"
        jobs[job_id]["progress"] = 100
        jobs[job_id]["output_file"] = output_filename
        jobs[job_id]["output_url"] = f"/api/download/{output_filename}"
        log.info(f"[{job_id}] Demo COMPLETE: {output_filename}")

    except Exception as e:
        log.error(f"[{job_id}] Demo pipeline failed: {e}", exc_info=True)
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)

    finally:
        Path(demo_path).unlink(missing_ok=True)
        if ref_path and Path(ref_path).exists():
            Path(ref_path).unlink(missing_ok=True)
        if face_path and Path(face_path).exists():
            Path(face_path).unlink(missing_ok=True)


async def _list_voices():
    """List available ElevenLabs voices for the user to choose from."""
    voices, source, provider_ok, warning = await _fetch_voice_catalog()
    _cache_voice_catalog(source, provider_ok, len(voices), warning)
    out = {"voices": voices, "source": source, "provider_ok": provider_ok}
    if warning:
        out["warning"] = warning
    return out


async def _preview_voice(request: Request):
    """Generate a short voice preview with a given voice_id."""
    body = await request.json()
    voice_id = body.get("voice_id", "")
    if not voice_id:
        raise HTTPException(400, "voice_id required")
    preview_text = body.get("text", "Hey there! This is a quick preview of what I sound like. Pretty cool, right?")
    preview_audio: bytes | None = None
    used_voice_id = voice_id
    if ELEVENLABS_API_KEY:
        _, voice_candidates, available_voice_ids = await _resolve_elevenlabs_voice_candidates(voice_id)
        async with httpx.AsyncClient(timeout=30) as client:
            for candidate_voice_id in voice_candidates:
                if available_voice_ids and candidate_voice_id not in available_voice_ids:
                    continue
                resp = await client.post(
                    f"https://api.elevenlabs.io/v1/text-to-speech/{candidate_voice_id}",
                    headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
                    json={
                        "text": preview_text,
                        "model_id": "eleven_turbo_v2_5",
                        "voice_settings": {"stability": 0.5, "similarity_boost": 0.75, "style": 0.3},
                    },
                )
                if resp.status_code == 200:
                    preview_audio = resp.content
                    used_voice_id = candidate_voice_id
                    break
                if _is_retryable_elevenlabs_voice_error(resp.status_code):
                    log.warning(
                        "ElevenLabs preview voice %s rejected with %s. Retrying next voice.",
                        candidate_voice_id,
                        resp.status_code,
                    )
                    continue
                raise HTTPException(resp.status_code, f"ElevenLabs error: {resp.text[:200]}")
    else:
        log.warning("ElevenLabs API key not configured; using Edge TTS preview fallback.")
    if preview_audio is None:
        tmp_preview = tempfile.NamedTemporaryFile(delete=False, suffix=".mp3")
        tmp_preview.close()
        try:
            preview_result = await _generate_voiceover_with_edge_tts(
                text=preview_text,
                output_path=tmp_preview.name,
                language="en",
                requested_voice_id=voice_id,
                speed=1.0,
            )
            preview_audio = Path(tmp_preview.name).read_bytes()
            used_voice_id = str(preview_result.get("voice_id", voice_id))
        finally:
            try:
                os.unlink(tmp_preview.name)
            except OSError:
                pass
    from fastapi.responses import Response
    return Response(content=preview_audio, media_type="audio/mpeg",
                    headers={"Content-Disposition": f"inline; filename=preview_{used_voice_id}.mp3"})


async def _create_demo_video(
    background_tasks: BackgroundTasks,
    demo_video: UploadFile = File(...),
    reference_video: Optional[UploadFile] = File(None),
    face_image: Optional[UploadFile] = File(None),
    product_name: str = Form(""),
    reference_notes: str = Form(""),
    pip_position: str = Form("bottom-right"),
    voice_id: str = Form(""),
    request: Request = None,
):
    user = await get_current_user_from_request(request)
    if not user:
        raise HTTPException(401, "Authentication required")

    user_email = user.get("email", "")
    user_plan = user.get("plan", "starter")
    is_admin = user_email in ADMIN_EMAILS
    if (not PRODUCT_DEMO_PUBLIC_ENABLED) and (not is_admin):
        raise HTTPException(403, "Product Demo is coming soon.")
    has_demo = is_admin or (user_plan == "demo_pro")
    if not has_demo:
        raise HTTPException(403, "Product Demo requires the Demo Pro plan ($150/mo). Upgrade to access this feature.")

    job_id = f"demo_{int(time.time()*1000)}_{random.randint(1000,9999)}"

    demo_ext = Path(demo_video.filename or "video.mp4").suffix or ".mp4"
    demo_path = str(DEMO_DIR / (job_id + "_demo" + demo_ext))
    with open(demo_path, "wb") as f:
        while chunk := await demo_video.read(1024 * 1024):
            f.write(chunk)

    ref_path = ""
    if reference_video and reference_video.filename:
        ref_ext = Path(reference_video.filename).suffix or ".mp4"
        ref_path = str(DEMO_DIR / (job_id + "_reference" + ref_ext))
        with open(ref_path, "wb") as f:
            while chunk := await reference_video.read(1024 * 1024):
                f.write(chunk)

    face_path = ""
    if face_image and face_image.filename:
        face_ext = Path(face_image.filename).suffix or ".png"
        face_path = str(DEMO_DIR / (job_id + "_face" + face_ext))
        with open(face_path, "wb") as f:
            while chunk := await face_image.read(1024 * 1024):
                f.write(chunk)

    jobs[job_id] = {
        "status": "queued", "progress": 0, "type": "demo",
        "product_name": product_name,
        "created_at": time.time()
    }

    background_tasks.add_task(
        run_demo_pipeline, job_id, demo_path, ref_path, face_path,
        product_name, reference_notes, pip_position, voice_id
    )

    return {"status": "accepted", "job_id": job_id}


app.include_router(
    build_assets_router(
        training_status_endpoint=_training_status,
        sync_thumbnail_library_endpoint=_sync_thumbnail_library,
        upload_thumbnails_endpoint=_upload_thumbnails,
        list_thumbnails_endpoint=_list_thumbnails,
        thumbnail_feedback_endpoint=_thumbnail_feedback,
        serve_thumbnail_endpoint=_serve_thumbnail,
        delete_thumbnail_endpoint=_delete_thumbnail,
        serve_public_thumbnail_share_endpoint=_serve_public_thumbnail_share,
        serve_generated_thumbnail_endpoint=_serve_generated_thumbnail,
        generate_thumbnail_endpoint=_generate_thumbnail,
        list_voices_endpoint=_list_voices,
        preview_voice_endpoint=_preview_voice,
        create_demo_video_endpoint=_create_demo_video,
    )
)


# â”€â”€â”€ Static Files â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_default_dist_dir = (Path(__file__).resolve().parent / "ViralShorts-App" / "dist").resolve()
dist_dir = Path(os.getenv("FRONTEND_DIST_DIR", str(_default_dist_dir))).resolve()
if dist_dir.exists():
    log.info(f"Serving frontend dist from: {dist_dir}")
    assets_dir = dist_dir / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir), html=False), name="assets")

    @app.get("/", include_in_schema=False)
    async def _serve_landing_page():
        index_path = dist_dir / "index.html"
        if not index_path.exists():
            raise HTTPException(404, "Landing page not found")
        return FileResponse(str(index_path), media_type="text/html")

    @app.get("/{page_name}.html", include_in_schema=False)
    async def _serve_dist_html(page_name: str):
        if not re.fullmatch(r"[A-Za-z0-9._-]+", page_name or ""):
            raise HTTPException(404, "Not Found")
        html_path = dist_dir / f"{page_name}.html"
        if not html_path.exists():
            raise HTTPException(404, "Not Found")
        return FileResponse(str(html_path), media_type="text/html")

    @app.get("/{asset_name}", include_in_schema=False)
    async def _serve_root_asset(asset_name: str):
        if "/" in asset_name or "\\" in asset_name:
            raise HTTPException(404, "Not Found")
        asset_path = dist_dir / asset_name
        if not asset_path.exists() or not asset_path.is_file():
            raise HTTPException(404, "Not Found")
        suffix = asset_path.suffix.lower()
        media_map = {
            ".txt": "text/plain; charset=utf-8",
            ".xml": "application/xml",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".webp": "image/webp",
            ".svg": "image/svg+xml",
            ".ico": "image/x-icon",
            ".json": "application/json",
        }
        return FileResponse(str(asset_path), media_type=media_map.get(suffix))
else:
    log.warning(f"Frontend dist directory not found: {dist_dir}")


if __name__ == "__main__":
    for f in OUTPUT_DIR.iterdir():
        if f.suffix == ".mp4" and f.stat().st_mtime < time.time() - 86400:
            f.unlink(missing_ok=True)
    uvicorn.run("backend:app", host="0.0.0.0", port=int(os.getenv("PORT", "8091")), reload=True)
