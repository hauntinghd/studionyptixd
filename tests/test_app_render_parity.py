"""The app must be able to render what the canary renders.

A canary short came out branded "MrSkelewelly" with art direction applied. The
same code path driven through the app came out branded "Studio" with none -
because `/api/skeleton-ai/generate` never passed `watermark_text`, `visual_brief`
or `beats_target` to `run_pipeline`, even though the pipeline had always
accepted them.

That is the entire gap between "the canary makes a great short" and "I can make
that short". These tests pin the parameters through, so the route cannot quietly
drop them again.
"""
from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
ROUTER = (ROOT / "skeleton_ai_router.py").read_text(encoding="utf-8")
PANEL = (
    ROOT / "ViralShorts-App" / "src" / "studio" / "panels" / "CreatePanel.tsx"
).read_text(encoding="utf-8")


@pytest.mark.parametrize("field", ["watermark_text", "visual_brief", "beats_target"])
def test_the_request_model_accepts_the_field(field: str) -> None:
    from skeleton_ai_router import GenerateRequest

    assert field in GenerateRequest.model_fields, f"the app cannot send {field}"


@pytest.mark.parametrize("field", ["watermark_text", "visual_brief", "beats_target"])
def test_the_field_is_optional(field: str) -> None:
    """Existing callers must keep working untouched."""
    from skeleton_ai_router import GenerateRequest

    assert GenerateRequest().model_dump()[field] is None


@pytest.mark.parametrize("field", ["watermark_text", "visual_brief", "beats_target"])
def test_the_route_forwards_the_field_to_the_pipeline(field: str) -> None:
    """Accepting a field and dropping it is worse than rejecting it."""
    call = ROUTER[ROUTER.index("result = run_pipeline(") :]
    call = call[: call.index("\n        except ")]
    assert field in call, f"{field} is accepted by the API but never reaches the render"


@pytest.mark.parametrize("field", ["watermark_text", "visual_brief", "beats_target"])
def test_the_pipeline_actually_takes_the_field(field: str) -> None:
    from skeleton_ai.pipeline import run

    assert field in inspect.signature(run).parameters


def test_an_unset_brand_still_renders_as_studio() -> None:
    """The default must not become empty and burn a blank watermark."""
    from skeleton_ai.pipeline import run

    assert inspect.signature(run).parameters["watermark_text"].default == "Studio"


def test_the_beat_count_is_clamped() -> None:
    """A hostile beats_target must not be able to order 10,000 clips."""
    call = ROUTER[ROUTER.index("result = run_pipeline(") :]
    call = call[: call.index("\n        except ")]
    assert re.search(r"min\(\s*30\s*,", call), "beats_target is not bounded"
    assert re.search(r"max\(\s*1\s*,", call), "beats_target has no floor"


def test_the_brand_is_length_limited() -> None:
    call = ROUTER[ROUTER.index("brand = ") : ROUTER.index("result = run_pipeline(")]
    assert "[:48]" in call, "an unbounded brand string reaches the video overlay"


# --- The UI has to be able to reach them --------------------------------------

@pytest.mark.parametrize("field", ["watermark_text", "visual_brief"])
def test_the_create_panel_sends_the_field(field: str) -> None:
    assert f"{field}:" in PANEL, f"the Create panel cannot set {field}"


@pytest.mark.parametrize("label", ["Watermark / Brand", "Visual Brief"])
def test_the_create_panel_exposes_a_control(label: str) -> None:
    """A field the creator cannot see is a field they do not have."""
    assert label in PANEL


def test_the_brand_persists_between_renders() -> None:
    """A creator sets their channel brand once, not on every short."""
    assert "studio.watermarkText" in PANEL
