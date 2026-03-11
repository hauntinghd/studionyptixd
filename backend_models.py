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
    scenes: list = []


class SceneImageRequest(BaseModel):
    prompt: str
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


class CheckoutRequest(BaseModel):
    price_id: str


class TopupCheckoutRequest(BaseModel):
    price_id: str


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
