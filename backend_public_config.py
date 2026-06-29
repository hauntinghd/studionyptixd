"""Public configuration payload for the Studio frontend."""

from __future__ import annotations

from typing import Callable

from auth import FALLBACK_SUPABASE_ANON_KEY, FALLBACK_SUPABASE_URL
from backend_settings import (
    ANIMATION_CREDIT_UNIT_USD,
    ANIMATION_MARKUP_MULTIPLIER,
    BILLING_STRIPE_PRIMARY,
    DISABLE_ALL_SFX,
    FORCE_720P_ONLY,
    GOOGLE_OAUTH_CLIENT_KIND,
    GOOGLE_OAUTH_CONFIG_ISSUE,
    GOOGLE_OAUTH_SOURCE,
    GOOGLE_REDIRECT_URI,
    GOOGLE_INSTALLED_OAUTH_CONFIG_ISSUE,
    GOOGLE_INSTALLED_OAUTH_SOURCE,
    KLING21_STANDARD_I2V_5S_USD,
    LONGFORM_BETA_ENABLED,
    PLAN_PRICE_USD,
    PUBLIC_PLAN_IDS,
    PUBLIC_TOPUP_PACK_IDS,
    SCRIPT_TO_SHORT_ENABLED,
    STORY_ADVANCED_CONTROLS_ENABLED,
    STORY_RETENTION_TUNING_ENABLED,
    STRIPE_SECRET_KEY,
    SUPABASE_ANON_KEY,
    SUPABASE_URL,
    TOPUP_PACKS,
    UNIFIED_PLANS,
    WAITLIST_ONLY_MODE,
    WAITLIST_REQUIRE_STRIPE_PAYMENT,
    YOUTUBE_API_KEY,
    YOUTUBE_OAUTH_MODE,
)
from studio_agent.queue import queue_config
from video_pipeline import (
    CREATIVE_IMAGE_MODEL_PROFILES,
    CREATIVE_VIDEO_MODEL_PROFILES,
    DEFAULT_CREATIVE_IMAGE_MODEL_ID,
    DEFAULT_CREATIVE_VIDEO_MODEL_ID,
    MICRO_ESCALATION_MAX_OUTPUT_CLIPS,
    MICRO_ESCALATION_MAX_SOURCE_SCENES,
    TRANSITION_STYLE_MAP,
    _creative_model_catalog_copy,
)


def build_public_config_payload(
    *,
    maintenance_snapshot: Callable[[], tuple[bool, str]],
    story_art_style_count: Callable[[], int],
    default_membership_plan_id: Callable[[], str],
    youtube_auth_configured: Callable[[], bool],
    youtube_public_api_key_candidates: Callable[[], list[str]],
    youtube_active_oauth_mode: Callable[[], str],
):
    async def public_config_payload():
        public_plans = {
            plan_id: {
                "monthly_credits": int(spec.get("monthly_credits", 0) or 0),
                "price_usd": float(spec.get("price_usd", 0) or 0),
                "name": str(spec.get("name") or plan_id),
                "best_value": bool(spec.get("best_value")),
            }
            for plan_id, spec in UNIFIED_PLANS.items()
            if plan_id in PUBLIC_PLAN_IDS
        }
        public_plan_features = {
            plan_id: ["studio_agent", "openrouter", "fal_render", "elevenlabs", "competitor_analysis"]
            for plan_id in PUBLIC_PLAN_IDS
        }
        public_plan_prices = {
            plan_id: float((UNIFIED_PLANS.get(plan_id) or {}).get("price_usd", PLAN_PRICE_USD.get(plan_id, 0)))
            for plan_id in PUBLIC_PLAN_IDS
        }
        public_topup_packs = [
            {"price_id": price_id, **meta}
            for price_id, meta in TOPUP_PACKS.items()
            if price_id in PUBLIC_TOPUP_PACK_IDS
        ]
        maintenance_banner_enabled, maintenance_banner_message = maintenance_snapshot()
        return {
            "supabase_url": str(SUPABASE_URL or "").strip() or FALLBACK_SUPABASE_URL,
            "supabase_anon_key": str(SUPABASE_ANON_KEY or "").strip() or FALLBACK_SUPABASE_ANON_KEY,
            "stripe_enabled": bool(STRIPE_SECRET_KEY),
            "waitlist_only_mode": bool(WAITLIST_ONLY_MODE),
            "waitlist_requires_stripe_payment": bool(WAITLIST_REQUIRE_STRIPE_PAYMENT),
            "maintenance_banner_enabled": maintenance_banner_enabled,
            "maintenance_banner_message": maintenance_banner_message,
            "plans": public_plans,
            "plan_features": public_plan_features,
            "plan_prices_usd": public_plan_prices,
            "prices": {},
            "topup_packs": public_topup_packs,
            "transition_styles": list(TRANSITION_STYLE_MAP.keys()),
            "story_art_style_count": story_art_style_count(),
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
                "model": "unified_credits",
                "default_membership_plan_id": default_membership_plan_id(),
                "membership_label": "Studio credits",
                "paypal_primary": not bool(BILLING_STRIPE_PRIMARY and STRIPE_SECRET_KEY),
                "stripe_primary": bool(BILLING_STRIPE_PRIMARY and STRIPE_SECRET_KEY),
                "slideshows_free": False,
                "animated_credit_label": "Credits",
                "non_animated_credit_label": "Credits",
                "overage_label": "credit top-ups",
                "hard_stop_on_animated_exhaustion": True,
                "waitlist_only_mode": bool(WAITLIST_ONLY_MODE),
                "waitlist_requires_stripe_payment": bool(WAITLIST_REQUIRE_STRIPE_PAYMENT),
                "kling21_standard_i2v_5s_usd": KLING21_STANDARD_I2V_5S_USD,
                "animation_markup_multiplier": ANIMATION_MARKUP_MULTIPLIER,
                "animation_credit_unit_usd": ANIMATION_CREDIT_UNIT_USD,
            },
            "studio_agent_queue": queue_config(),
            "unified_plans": [
                {
                    "id": plan_id,
                    "name": str(spec.get("name") or plan_id),
                    "price_usd": float(spec.get("price_usd", 0) or 0),
                    "monthly_credits": int(spec.get("monthly_credits", 0) or 0),
                    "best_value": bool(spec.get("best_value")),
                }
                for plan_id, spec in UNIFIED_PLANS.items()
                if plan_id in PUBLIC_PLAN_IDS
            ],
            "youtube_integration": {
                "oauth_configured": youtube_auth_configured(),
                "api_key_configured": bool(YOUTUBE_API_KEY),
                "api_key_pool_size": len(youtube_public_api_key_candidates()),
                "redirect_uri": GOOGLE_REDIRECT_URI,
                "oauth_preferred_mode": YOUTUBE_OAUTH_MODE,
                "oauth_active_mode": youtube_active_oauth_mode(),
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

    return public_config_payload
