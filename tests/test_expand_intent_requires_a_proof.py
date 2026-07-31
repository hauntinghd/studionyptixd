"""Expanding a proof requires a proof to exist in this conversation.

A creator finished planning a script, said "All right, let's make the entire
short.", and Studio replied:

    Do you approve the current Scene 1 and want me to preserve it?

There was no Scene 1. The session's `active_jobs` was empty and nothing had been
rendered. Two things combined to invent one:

  1. Intent came from a regex on the phrase alone - `is_expand_short_request`
     matches "make the entire short", which means "expand my proof" only when a
     proof exists and "start one" otherwise.
  2. The target hunt then escalated from the session, to scraping any job id out
     of raw message text, to globbing every job on disk. With WITH CONTEXT on,
     carried-over conversation supplies an unrelated job id.

So a fresh planning chat latched onto someone else's old job and asked for
approval of a scene the creator had never seen - a dead end, every time.
"""
from __future__ import annotations

import pytest

from studio_agent import store
from studio_agent.runner import _session_has_expandable_proof_context


def test_the_phrase_alone_is_still_read_as_expansion() -> None:
    """The regex is not wrong, it is just insufficient on its own."""
    assert store.is_expand_short_request("All right, let's make the entire short.")


def test_a_fresh_conversation_has_no_proof_to_expand() -> None:
    """The exact failing state: planning done, nothing rendered."""
    assert _session_has_expandable_proof_context({"active_jobs": [], "messages": []}) is False


def test_a_job_id_in_raw_message_text_is_not_evidence() -> None:
    """Carried-over context can mention a job from another conversation.

    This is the specific leak that manufactured the phantom Scene 1.
    """
    session = {
        "active_jobs": [],
        "messages": [{"role": "user", "content": '{"job_id": "116de45c8205"}'}],
    }
    assert _session_has_expandable_proof_context(session) is False


# --- Real evidence still counts ------------------------------------------------

def test_an_active_job_counts() -> None:
    assert _session_has_expandable_proof_context({"active_jobs": [{"job_id": "abc123"}]}) is True


def test_replying_to_a_job_card_counts() -> None:
    assert _session_has_expandable_proof_context({}, reply_to={"job_id": "abc123"}) is True


def test_a_rendered_deliverable_counts() -> None:
    """Something was actually produced in this thread."""
    session = {
        "messages": [{"role": "assistant", "jobDeliverable": {"job_id": "abc123"}}]
    }
    assert _session_has_expandable_proof_context(session) is True


def test_a_pending_expansion_intake_counts() -> None:
    """A clarification already in flight must keep routing to the compiler."""
    assert _session_has_expandable_proof_context({"short_expansion_intake": {"scenes": 6}}) is True


# --- Degenerate shapes must not crash or fake evidence -------------------------

@pytest.mark.parametrize(
    "session",
    [
        {},
        {"active_jobs": None, "messages": None},
        {"active_jobs": [{}]},
        {"active_jobs": [{"job_id": "   "}]},
        {"messages": [{"role": "assistant", "jobDeliverable": {}}]},
        {"messages": [None, "not-a-dict"]},
    ],
)
def test_empty_or_malformed_state_is_not_a_proof(session: dict) -> None:
    assert _session_has_expandable_proof_context(session) is False


def test_an_empty_reply_to_job_id_is_not_evidence() -> None:
    assert _session_has_expandable_proof_context({}, reply_to={"job_id": ""}) is False
