"""ClipLab Pydantic schemas."""
from __future__ import annotations

from pydantic import BaseModel, Field


class TranscriptWord(BaseModel):
    text: str
    start: float
    end: float
    confidence: float = 1.0


class TranscriptCue(BaseModel):
    start: float
    end: float
    text: str
    words: list[TranscriptWord] = []


class ClipSegment(BaseModel):
    start: float
    end: float
    confidence: float = 0.0
    virality_score: float = 0.0
    why_it_matches: str = ""
    hook_text: str = ""
    suggested_hook_reorder: bool = False
    transcript_snippet: str = ""
    model_source: str = "local_llm"


class ClipLabIngestRequest(BaseModel):
    youtube_url: str = ""
    upload_id: str = ""


class ClipLabAnalyzeRequest(BaseModel):
    video_id: str
    prompt: str = Field(..., min_length=3, max_length=4000)
    max_segments: int = Field(12, ge=1, le=40)


class ClipLabRenderRequest(BaseModel):
    video_id: str
    prompt_run_id: str = ""
    segment_indices: list[int] = Field(default_factory=list)
    burn_captions: bool = True
    caption_style: str = "karaoke"  # karaoke | minimal


class ClipLabFeedbackRequest(BaseModel):
    clip_id: str
    kept: bool = True
    edited_hook: str = ""
    published: bool = False
    notes: str = ""


class FaceTrajectoryPoint(BaseModel):
    t: float
    cx: float
    cy: float
    face_w: float = 0.0
    face_h: float = 0.0
    confidence: float = 0.0
