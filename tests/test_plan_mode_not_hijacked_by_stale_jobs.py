"""A brand-new Plan-mode chat must answer the question it was asked.

The creator opened a fresh chat in Plan mode and asked for an analytics
breakdown of their best-performing short. Studio replied "Your stills are
ready for review (0/1 approved so far)" - a card about job 992c0e5f4fe8,
which belonged to an entirely different conversation. The question was never
answered, and it looked like Catalyst was broken when Catalyst never ran.

Three faults stacked:
  1. the status-follow-up predicate matched a 200-word analytics brief
  2. that branch was the only continuation path with no plan_only gate
  3. job recovery adopted an orphan job from an unrelated session
"""
from __future__ import annotations

import pytest

from studio_agent.runner import _is_job_status_followup, _recover_shortform_job_from_session

REAL_QUESTION = (
    "Can you please look at the most popular video on my channel, which is currently the "
    "video titled. Why Men Self-Sabotage When They Fall In Love. That's the video's title. "
    "I want you to analyze what made it perform at 800 views and how we could have done "
    "better. Because it got 800 views with a 58.6% state to watch, 29% average view "
    "duration. And Catalyst, which is your backend engine, should have the ability to "
    "properly see all of the like in-depth analytics on my in my channel and be able to "
    "understand why a video did good or did bad and how we can replicate success and make "
    "it do better and better and better so do that for the white man stuff sabotage when "
    "they fall in love find a way to make it better and base it off what's in demand "
    "currently on youtube"
)


def test_an_analytics_brief_is_not_a_status_ping() -> None:
    """The pattern used an unbounded .* across the whole message.

    It matched "what" near the start ("look at what made it perform") against
    "find" at the very end ("find a way to make it better"), which almost any
    long dictated request contains.
    """
    assert _is_job_status_followup(REAL_QUESTION) is False


@pytest.mark.parametrize(
    "probe",
    ["status", "what did you find", "is it done", "any update", "show me the results",
     "so what did you find?", "tell me what you found"],
)
def test_real_status_pings_still_match(probe: str) -> None:
    assert _is_job_status_followup(probe) is True


@pytest.mark.parametrize(
    "probe",
    [
        "can you show me how to write a better hook for this short",
        "tell me why the analysis of my last video said retention was the problem",
    ],
)
def test_long_questions_containing_the_keywords_are_not_status_pings(probe: str) -> None:
    assert _is_job_status_followup(probe) is False


def test_a_brand_new_chat_cannot_adopt_an_orphan_job() -> None:
    """33 of 36 job specs on the box recorded no owning session.

    The cross-session guard only ran when a spec named an owner, so it was
    inert for almost every job and any new chat could inherit one.
    """
    fresh = {"session_id": "sa_brandnew", "user_id": "some-user", "messages": []}
    assert _recover_shortform_job_from_session(fresh) is None


def test_a_session_that_produced_something_still_recovers_its_own_job(tmp_path, monkeypatch) -> None:
    """The scan exists to recover a job after active_jobs was cleared."""
    import json as _json

    import studio_agent.jobs as jobs

    root = tmp_path / "skeleton_output"
    workspace = root / "sf_owned"
    workspace.mkdir(parents=True)
    (workspace / "job_spec.json").write_text(
        _json.dumps({"user_id": "u1", "topic": "t", "scene_count": 1}), encoding="utf-8"
    )
    (workspace / "progress.json").write_text(
        _json.dumps({"stage": "awaiting_scene_review"}), encoding="utf-8"
    )
    monkeypatch.setattr(jobs, "ROOT", tmp_path, raising=False)
    monkeypatch.setattr(jobs, "SKELETON_OUTPUT", "skeleton_output", raising=False)

    produced = {
        "session_id": "sa_owner",
        "user_id": "u1",
        "messages": [],
        "last_production": {"tool": "start_shortform_generate", "arguments": {}},
    }
    assert _recover_shortform_job_from_session(produced) == "sf_owned"
