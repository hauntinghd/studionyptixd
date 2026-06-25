from skeleton_ai.canonical_edit import (
    NEG_EDIT,
    build_scene_edit_prompt,
    sanitize_skeleton_outfit,
)
from skeleton_ai.pipeline import (
    _expand_locked_scene_direction,
    _merge_locked_scene_with_generated,
    _visual_brief_beat_direction,
    _visual_brief_requests_wardrobe,
)
from skeleton_ai.styled_pipeline import _scoped_edit_prompt


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
    assert "every exposed body part" in prompt
    assert "bare feet" not in prompt
    assert "glass-and-bone skeletal feet" in prompt
    assert "muscle definition" not in prompt
    assert prompt.startswith("primary edit")
    assert len(prompt) < 1800


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


def test_numbered_beat_direction_is_extracted_exactly():
    brief = (
        "Beat 1: skeleton in dark bedroom, hand reaching toward chest. "
        "Beat 2: brain highlighted with cyan neural pathways. "
        "Beat 3: childhood memory space."
    )
    assert _visual_brief_beat_direction(brief, 0) == (
        "skeleton in dark bedroom, hand reaching toward chest"
    )
    assert _visual_brief_beat_direction(brief, 1) == (
        "brain highlighted with cyan neural pathways"
    )


def test_sparse_locked_scene_gets_visible_environment_and_wide_framing():
    direction = _expand_locked_scene_direction(
        "skeleton in dark room, hand reaching toward chest"
    ).lower()
    assert direction.startswith("skeleton in dark room")
    assert "replace the entire reference background" in direction
    assert "medium-wide vertical shot" in direction
    assert "head to knees" in direction
    assert "no isolated close-up" in direction
    assert "never use a black void" in direction


def test_locked_scene_keeps_planner_environment_as_subordinate_detail():
    merged = _merge_locked_scene_with_generated(
        "skeleton in dark room, hand reaching toward chest",
        "Dim bedroom with an unmade bed, sheer curtains, and blue moonlight",
    ).lower()
    assert merged.startswith("skeleton in dark room")
    assert "dim bedroom with an unmade bed" in merged
    assert "supporting environment detail" in merged
    assert "must not change the mandatory location" in merged


def test_character_scoped_repair_preserves_background():
    prompt, scope = _scoped_edit_prompt(
        "Skeleton standing in a flawless dark bedroom.",
        "Repair skeleton anatomy and glass-shell artifacts.",
        "character",
    )
    low = prompt.lower()
    assert scope == "character"
    assert "preserve the current background" in low
    assert "repair skeleton anatomy" in low


if __name__ == "__main__":
    test_bare_feet_are_rewritten_as_skeletal_glass_feet()
    test_scene_prompt_forbids_human_tissue_at_clothing_edges()
    test_negative_prompt_blocks_hybrid_human_anatomy()
    test_scene_only_visual_brief_does_not_request_wardrobe()
    test_numbered_beat_direction_is_extracted_exactly()
    test_sparse_locked_scene_gets_visible_environment_and_wide_framing()
    test_locked_scene_keeps_planner_environment_as_subordinate_detail()
    test_character_scoped_repair_preserves_background()
    print("skeleton prompt integrity tests passed")
