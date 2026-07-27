"""Regression: "remake/redo/recreate the scenes" must be understood as a
scene-regeneration command on the active production, and full remakes that also
ask for animation must scope to full_quality. Negations/questions must never
authorize a mutation.
"""
from __future__ import annotations

import pytest

from studio_agent.command_contract import (
    infer_scene_repair_scope,
    scene_repair_authorization_evidence,
    scene_repair_block_reason,
    scene_repair_candidate,
)


def test_remake_all_scenes_then_animate_is_full_quality() -> None:
    msg = "all 6 scenes need to be fully remade so please do that then animate them"
    assert scene_repair_candidate(msg)
    assert scene_repair_block_reason(msg) == ""
    assert scene_repair_authorization_evidence(msg) != ""
    # "remade" (still regen) + "animate them" => regenerate stills AND re-animate.
    assert infer_scene_repair_scope(msg) == "full_quality"


@pytest.mark.parametrize(
    "msg",
    [
        "remake them",
        "please remake the scenes",
        "recreate all the scenes",
        "redo scene 3",
        "rebuild those shots",
        "re-make the stills",
        "reproduce them",
    ],
)
def test_remake_synonyms_are_recognized_and_authorized(msg: str) -> None:
    assert scene_repair_candidate(msg), msg
    assert scene_repair_authorization_evidence(msg) != "", msg


def test_bare_remake_scopes_to_visual_still_regeneration() -> None:
    assert infer_scene_repair_scope("remake all the scenes") == "visual_quality"


def test_animate_verb_is_detected_for_scope() -> None:
    # Bare "animate" (not just "animation"/"animated") must register as animation.
    assert infer_scene_repair_scope("redo the stills and animate them") == "full_quality"


@pytest.mark.parametrize(
    "msg",
    [
        "do not remake them",
        "don't remake the scenes",
        "never redo them",
        "stop remaking them",
        "leave the scenes alone",
        "are the scenes remade?",
        "what if I remade them?",
    ],
)
def test_negations_and_questions_never_authorize_a_remake(msg: str) -> None:
    assert scene_repair_authorization_evidence(msg) == "", msg
