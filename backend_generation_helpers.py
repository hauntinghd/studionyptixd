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
