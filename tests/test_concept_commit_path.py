"""Committing a concept plan must not crash on a function that does not exist.

A creator planned a short, got a concept card, and said "yes make it" four
times. Nothing happened - no production, no error, no assistant reply, and an
empty server log. The turn returned HTTP 200 each time.

The conversion was firing correctly. It then called
`store._prepare_shortform_pending_args`, which two runner call sites referenced
and which had never been written:

    AttributeError: module 'studio_agent.store' has no attribute
    '_prepare_shortform_pending_args'

Two failures, and the second is why it took so long to find: the crash was
forwarded to the client as an SSE error and logged nowhere, so `docker logs`
had nothing to show.
"""
from __future__ import annotations

import inspect
import re

import pytest

from studio_agent import runner, store


MESSAGES = [{"role": "user", "content": "yes make it"}]


def test_the_function_the_runner_calls_exists() -> None:
    """The whole bug in one assertion."""
    assert hasattr(store, "_prepare_shortform_pending_args")


def test_every_store_helper_the_runner_calls_exists() -> None:
    """Catches the next missing reference, not just this one."""
    source = inspect.getsource(runner)
    referenced = set(re.findall(r"\bstore\.(_[a-z0-9_]+)\s*\(", source))
    missing = sorted(name for name in referenced if not hasattr(store, name))
    assert not missing, f"runner calls store helpers that do not exist: {missing}"


def test_the_pending_card_keeps_the_planned_scene_count() -> None:
    """Execution collapses to one still; the Approve card must not.

    Clients treat a one-scene pending card as stale and hide it, so collapsing
    here means the creator can never authorise the render the card is asking
    them to authorise.
    """
    args = {"topic": "Why Men Self-Sabotage", "scene_count": 6}
    prepared = store._prepare_shortform_pending_args(args, MESSAGES)
    assert prepared["scene_count"] == 6


def test_execution_still_enforces_the_one_still_gate() -> None:
    """The staged contract must survive the pending-card fix."""
    args = {"topic": "Why Men Self-Sabotage", "scene_count": 6}
    executed = store._prepare_shortform_execution_args(args, MESSAGES)
    assert executed["scene_count"] == 1
    assert executed.get("visual_proof_only") is True


def test_the_two_paths_agree_on_everything_except_scene_count() -> None:
    args = {"topic": "Why Men Self-Sabotage", "scene_count": 6}
    pending = store._prepare_shortform_pending_args(args, MESSAGES)
    executed = store._prepare_shortform_execution_args(args, MESSAGES)
    assert pending["topic"] == executed["topic"]
    differing = {k for k in set(pending) | set(executed) if pending.get(k) != executed.get(k)}
    assert differing == {"scene_count"}, differing


@pytest.mark.parametrize("count", [None, 0, 1, "", "not-a-number"])
def test_a_missing_or_single_scene_count_is_left_alone(count) -> None:
    """Only a real multi-scene plan overrides the execution default."""
    args = {"topic": "T"}
    if count is not None:
        args["scene_count"] = count
    prepared = store._prepare_shortform_pending_args(args, MESSAGES)
    assert prepared["scene_count"] == 1


def test_turn_failures_are_logged_with_a_traceback() -> None:
    """The reason this bug survived four attempts and an empty log."""
    source = inspect.getsource(runner)
    assert "log.exception(" in source, "turn failures are not logged server-side"
    assert '"traceback": traceback.format_exc()' in source
