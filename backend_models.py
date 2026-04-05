from pydantic import BaseModel


class GenerateRequest(BaseModel):
    template: str
    prompt: str
    youtube_channel_id: str = ""
    trend_hunt_enabled: bool = False
    resolution: str = "720p"
    language: str = "en"
    mode: str = "auto"
    quality_mode: str = "cinematic"
    mint_mode: bool = True
    transition_style: str = "smooth"
    micro_escalation_mode: bool = True
    voice_id: str = ""
    voice_speed: float = 1.0
    pacing_mode: str = "standard"
    art_style: str = "auto"
    cinematic_boost: bool = False
    animation_enabled: bool = True
    story_animation_enabled: bool = True
    reference_image_url: str = ""
    reference_lock_mode: str = "strict"
    image_model_id: str = ""
    video_model_id: str = ""
    scenes: list = []


class SceneImageRequest(BaseModel):
    prompt: str
    negative_prompt: str = ""
    scene_index: int = 0
    session_id: str = ""
    youtube_channel_id: str = ""
    template: str = "skeleton"
    resolution: str = "720p"
    quality_mode: str = "cinematic"
    mint_mode: bool = True
    transition_style: str = "smooth"
    micro_escalation_mode: bool = True
    reference_lock_mode: str = "strict"
    art_style: str = "auto"
    cinematic_boost: bool = False
    image_model_id: str = ""


class FinalizeRequest(BaseModel):
    session_id: str
    template: str = "skeleton"
    youtube_channel_id: str = ""
    resolution: str = "720p"
    language: str = "en"
    quality_mode: str = "cinematic"
    mint_mode: bool = True
    transition_style: str = "smooth"
    micro_escalation_mode: bool = True
    narration: str = ""
    scenes: list = []
    animation_enabled: bool = True
    story_animation_enabled: bool = True
    reference_lock_mode: str = "strict"
    voice_id: str = ""
    voice_speed: float = 1.0
    pacing_mode: str = "standard"
    art_style: str = "auto"
    cinematic_boost: bool = False
    subtitles_enabled: bool = True
    image_model_id: str = ""
    video_model_id: str = ""


class CheckoutRequest(BaseModel):
    price_id: str = ""
    product: str = ""
    plan: str = ""


class TopupCheckoutRequest(BaseModel):
    price_id: str
    preferred_method: str = "card"


class WaitlistJoinRequest(BaseModel):
    plan: str


class SetPlanRequest(BaseModel):
    email: str
    plan: str


class FeedbackRequest(BaseModel):
    job_id: str = ""
    rating: int
    comment: str = ""
    template: str = ""
    language: str = ""
    feature: str = ""


class ThumbnailFeedbackRequest(BaseModel):
    generation_id: str
    accepted: bool


class ThumbnailGenerateRequest(BaseModel):
    mode: str  # "describe", "style_transfer", "screenshot_analysis"
    description: str
    style_reference_id: str = ""
    sketch_image_id: str = ""
    screenshot_description: str = ""
    style_preset: str = ""


class LongFormSessionCreateRequest(BaseModel):
    template: str
    topic: str = ""
    input_title: str = ""
    input_description: str = ""
    format_preset: str = "explainer"
    source_url: str = ""
    youtube_channel_id: str = ""
    analytics_notes: str = ""
    strategy_notes: str = ""
    transcript_text: str = ""
    auto_pipeline: bool = False
    target_minutes: float = 8.0
    language: str = "en"
    animation_enabled: bool = True
    sfx_enabled: bool = True
    whisper_mode: str = "subtle"


class LongFormChapterActionRequest(BaseModel):
    chapter_index: int
    action: str = "approve"
    reason: str = ""


class LongFormResolveErrorRequest(BaseModel):
    chapter_index: int
    fix_note: str = ""
    force_accept: bool = False


class YouTubeOAuthStartRequest(BaseModel):
    next_url: str = ""


class YouTubeChannelSelectRequest(BaseModel):
    channel_id: str = ""


class CatalystOutcomeIngestRequest(BaseModel):
    video_url: str = ""
    video_id: str = ""
    title_used: str = ""
    description_used: str = ""
    thumbnail_prompt: str = ""
    thumbnail_url: str = ""
    tags: list[str] = []
    failure_mode_key: str = ""
    failure_mode_label: str = ""
    failure_mode_summary: str = ""
    views: int = 0
    impressions: int = 0
    likes: int = 0
    comments: int = 0
    estimated_minutes_watched: float = 0.0
    average_view_duration_sec: float = 0.0
    average_percentage_viewed: float = 0.0
    impression_click_through_rate: float = 0.0
    first_30_sec_retention_pct: float = 0.0
    first_60_sec_retention_pct: float = 0.0
    operator_summary: str = ""
    strongest_signals: list[str] = []
    weak_points: list[str] = []
    hook_wins: list[str] = []
    hook_watchouts: list[str] = []
    pacing_wins: list[str] = []
    pacing_watchouts: list[str] = []
    visual_wins: list[str] = []
    visual_watchouts: list[str] = []
    sound_wins: list[str] = []
    sound_watchouts: list[str] = []
    packaging_wins: list[str] = []
    packaging_watchouts: list[str] = []
    retention_wins: list[str] = []
    retention_watchouts: list[str] = []
    next_video_moves: list[str] = []
    auto_fetch_channel_metrics: bool = True


class CatalystAutoOutcomeHarvestRequest(BaseModel):
    video_url: str = ""
    video_id: str = ""
    candidate_limit: int = 12
    auto_fetch_channel_metrics: bool = True


class CatalystChannelOutcomeSyncRequest(BaseModel):
    session_id: str = ""
    candidate_limit: int = 18
    refresh_existing: bool = False


class CatalystHubDirectiveRequest(BaseModel):
    channel_id: str = ""
    directive: str = ""
    mission: str = ""
    guardrails: list[str] = []
    target_niches: list[str] = []
    apply_scope: str = "all"


class CatalystHubRefreshRequest(BaseModel):
    channel_id: str = ""
    include_public_benchmarks: bool = True
    refresh_outcomes: bool = False


class CatalystHubLaunchRequest(BaseModel):
    channel_id: str = ""
    workspace_id: str = ""
    mission: str = ""
    directive: str = ""
    guardrails: list[str] = []
    target_niches: list[str] = []
    target_minutes: float = 0.0
    language: str = "en"
    animation_enabled: bool = True
    sfx_enabled: bool = True
    auto_pipeline: bool = True
    include_public_benchmarks: bool = True
    refresh_outcomes: bool = True


class CatalystHubReferenceVideoAnalysisRequest(BaseModel):
    channel_id: str = ""
    workspace_id: str = ""
    video_id: str = ""
    max_analysis_minutes: float = 3.0


class CatalystHubReferenceVideoClearRequest(BaseModel):
    channel_id: str = ""
    workspace_id: str = ""
