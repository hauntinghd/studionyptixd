from pydantic import BaseModel


class GenerateRequest(BaseModel):
    template: str
    prompt: str
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
