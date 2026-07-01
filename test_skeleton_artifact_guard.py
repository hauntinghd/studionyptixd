from skeleton_ai.canonical_edit import ARTIFACT_GUARD, NEG_EDIT, build_scene_edit_prompt


BAD_ARTIFACT_TERMS = (
    "bell jar",
    "capsule",
    "dome",
    "specimen tube",
    "cylinder",
    "display case",
    "helmet bubble",
    "glass container",
    "circular base",
    "readable text",
)


def test_artifact_guard_blocks_known_skeleton_failure_modes():
    guard = ARTIFACT_GUARD.lower()
    negative = NEG_EDIT.lower()

    for term in BAD_ARTIFACT_TERMS:
        assert term in guard
        assert term in negative


def test_scene_edit_prompt_puts_glass_container_ban_before_truncation():
    prompt = build_scene_edit_prompt(
        topic="male psychology",
        visual_description=(
            "Skeleton in a concrete room with cyan and yellow nervous-system lines, "
            "hand reaching toward chest."
        ),
        outfit="no clothing",
    )
    lower = prompt.lower()

    assert len(prompt) <= 1800
    assert "glass-shell rule" in lower
    assert "like clear skin" in lower
    assert "bell jar" in lower
    assert "capsule" in lower
    assert "display case" in lower
    assert "readable text" in lower
