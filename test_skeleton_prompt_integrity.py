from skeleton_ai.canonical_edit import (
    NEG_EDIT,
    build_scene_edit_prompt,
    sanitize_skeleton_outfit,
)
from skeleton_ai.pipeline import _visual_brief_requests_wardrobe


def test_bare_feet_are_rewritten_as_skeletal_glass_feet():
    cleaned = sanitize_skeleton_outfit(
        "Dark hoodie, black jeans, bare feet, minimalist watch"
    )
    assert "bare feet" not in cleaned.lower()
    assert "glass-and-bone skeletal feet" in cleaned.lower()


def test_scene_prompt_forbids_human_tissue_at_clothing_edges():
    prompt = build_scene_edit_prompt(
        topic="Emotional walls",
        visual_description="Standing beside a bed with one hand on chest",
        outfit="hoodie, jeans, bare feet",
    ).lower()
    assert "zero human skin" in prompt
    assert "every exposed limb segment" in prompt
    assert "bare feet" not in prompt
    assert "glass-and-bone skeletal feet" in prompt
    assert "muscle definition" not in prompt


def test_negative_prompt_blocks_hybrid_human_anatomy():
    low = NEG_EDIT.lower()
    for phrase in (
        "human arm",
        "human hand",
        "human foot",
        "human skin",
        "hybrid human skeleton",
        "half human",
    ):
        assert phrase in low


def test_scene_only_visual_brief_does_not_request_wardrobe():
    assert not _visual_brief_requests_wardrobe(
        "Skeleton in a dark bedroom, hand toward chest, cyan nervous system overlay"
    )
    assert _visual_brief_requests_wardrobe(
        "Skeleton wearing a charcoal hoodie and black jeans"
    )


if __name__ == "__main__":
    test_bare_feet_are_rewritten_as_skeletal_glass_feet()
    test_scene_prompt_forbids_human_tissue_at_clothing_edges()
    test_negative_prompt_blocks_hybrid_human_anatomy()
    test_scene_only_visual_brief_does_not_request_wardrobe()
    print("skeleton prompt integrity tests passed")
