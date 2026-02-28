from pydantic import BaseModel


class GenerateRequest(BaseModel):
    template: str
    prompt: str
    resolution: str = "720p"
    language: str = "en"
    mode: str = "auto"
    scenes: list = []


class SceneImageRequest(BaseModel):
    prompt: str
    scene_index: int = 0
    session_id: str = ""
    template: str = "skeleton"
    resolution: str = "720p"


class FinalizeRequest(BaseModel):
    session_id: str
    template: str = "skeleton"
    resolution: str = "720p"
    language: str = "en"
    narration: str = ""
    scenes: list = []


class CheckoutRequest(BaseModel):
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
