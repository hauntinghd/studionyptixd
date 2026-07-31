"""All 24 registered styles are reachable, and "skeleton" means skeleton.

Two problems, one visible and one silent.

The visible one: the launch gate was pinned to four keys, so twenty finished
styles sat in the registry unreachable by the picker, by natural language, or by
the agent - which is why the agent invented generic options ("talking head",
"text overlays") instead of offering Studio's own catalog.

The silent one was worse. Aliases were applied only to spoken text, so an
explicit or stored value of "skeleton" matched no registry key (it is
`skeleton_host`) and fell through to the cinematic default. The wrong art style
rendered and was charged for, and nothing reported a mismatch.
"""
from __future__ import annotations

import pytest

from studio_agent.render_styles import (
    DEFAULT_RENDER_STYLE,
    LAUNCH_RENDER_STYLE_KEYS,
    RENDER_STYLES,
    VALIDATED_RENDER_STYLE_KEYS,
    list_render_styles,
    resolve_render_style,
)


def test_every_registered_style_is_reachable() -> None:
    assert set(LAUNCH_RENDER_STYLE_KEYS) == set(RENDER_STYLES)
    assert len(LAUNCH_RENDER_STYLE_KEYS) >= 24


def test_the_catalog_lists_them_all() -> None:
    """The agent offers what this returns; anything missing does not exist to it."""
    assert len(list_render_styles()) == len(RENDER_STYLES)


def test_validated_styles_are_still_recorded_separately() -> None:
    """Opening the gate must not lose which styles are actually proven."""
    assert VALIDATED_RENDER_STYLE_KEYS <= set(RENDER_STYLES)
    assert "skeleton_host" in VALIDATED_RENDER_STYLE_KEYS
    assert VALIDATED_RENDER_STYLE_KEYS != set(RENDER_STYLES)


# --- The silent wrong-style bug ----------------------------------------------

@pytest.mark.parametrize(
    "value,expected",
    [
        ("skeleton", "skeleton_host"),
        ("Skeleton Host", "skeleton_host"),
        ("skeleton_host", "skeleton_host"),
        ("ghibli", "studio_ghibli"),
        ("anime", "cute_anime"),
        ("lego", "lego"),
        ("minecraft", "minecraft"),
    ],
)
def test_an_explicit_human_name_resolves(value: str, expected: str) -> None:
    assert resolve_render_style(value).key == expected


@pytest.mark.parametrize(
    "value,expected", [("skeleton", "skeleton_host"), ("ghibli", "studio_ghibli")]
)
def test_a_stored_session_style_resolves_the_same_way(value: str, expected: str) -> None:
    """A picker value stored as "skeleton" must not silently become cinematic."""
    assert resolve_render_style(None, session_style=value).key == expected


def test_skeleton_never_silently_becomes_the_default() -> None:
    """The regression that would render the wrong show and bill for it."""
    for value in ("skeleton", "Skeleton", "SKELETON", "skeleton style"):
        assert resolve_render_style(value).key != DEFAULT_RENDER_STYLE
        assert resolve_render_style(value).key == "skeleton_host"


def test_an_unknown_style_still_falls_back_to_the_default() -> None:
    assert resolve_render_style("not-a-real-style").key == DEFAULT_RENDER_STYLE


def test_spoken_style_still_outranks_the_picker() -> None:
    """Natural language is a hard lock; the picker is only the fallback."""
    style = resolve_render_style("lego", user_text="actually make it skeleton")
    assert style.key == "skeleton_host"


def test_every_style_resolves_to_itself_by_key() -> None:
    """No registry key may be shadowed by another style's alias."""
    for key in RENDER_STYLES:
        assert resolve_render_style(key).key == key


# --- The subset override ------------------------------------------------------

def test_the_launch_set_can_be_narrowed_again(monkeypatch: pytest.MonkeyPatch) -> None:
    from studio_agent import render_styles

    monkeypatch.setenv("STUDIO_LAUNCH_RENDER_STYLES", "skeleton_host, cinematic")
    assert render_styles._resolve_launch_render_style_keys() == frozenset(
        {"skeleton_host", "cinematic"}
    )


def test_an_override_naming_nothing_real_does_not_disable_everything(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A typo in configuration must not take every style offline."""
    from studio_agent import render_styles

    monkeypatch.setenv("STUDIO_LAUNCH_RENDER_STYLES", "typo,alsotypo")
    assert render_styles._resolve_launch_render_style_keys() == frozenset(RENDER_STYLES)
