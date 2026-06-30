"""Small generation helpers shared by Studio backend routes."""


def estimate_auto_short_credits(
    *,
    scene_count: int,
    video_per_scene: int,
    image_per_scene: int,
    animation_enabled: bool,
) -> int:
    scenes = max(1, int(scene_count or 0))
    image_cost = max(0, int(image_per_scene or 0))
    video_cost = max(0, int(video_per_scene or 0))
    if animation_enabled:
        return max(1, scenes * video_cost + scenes * image_cost)
    return max(1, scenes * image_cost)


def auto_scene_channel_context(state: dict) -> dict:
    metadata = dict((state or {}).get("metadata_pack") or {})
    context = dict(metadata.get("youtube_channel") or {})
    if context:
        return context
    channel_id = str((state or {}).get("youtube_channel_id", "") or "").strip()
    if channel_id:
        return {"channel_id": channel_id}
    return {}


def build_auto_scene_update(
    *,
    scene_index: int,
    filename: str,
    image_url: str,
    local_path: str,
    visual_description: str,
    template: str,
    quality_mode: str,
    generation_id: str,
    cdn_url: str,
    updated_at: float,
) -> dict:
    return {
        "scene_index": int(scene_index),
        "filename": str(filename or ""),
        "image_url": str(image_url or ""),
        "local_path": str(local_path or ""),
        "visual_description": str(visual_description or ""),
        "template": str(template or ""),
        "quality_mode": str(quality_mode or ""),
        "generation_id": str(generation_id or ""),
        "cdn_url": str(cdn_url or ""),
        "source": "regenerated_auto",
        "updated_at": float(updated_at or 0.0),
    }
