"""A paid still repair must never ship a regression.

The skeleton pipeline allows exactly one recovery attempt per still. Until now
it quarantined attempt 1 *before* rendering attempt 2, so whatever attempt 2
produced was used unconditionally - the retry re-stages the scene and re-seeds,
so it can easily come back worse. The creator paid for a repair that degraded
their video, and the original was already in quarantine.

`_still_qa_rank` is the comparison the keep-best branch uses: it decides whether
the second attempt actually earned its place.
"""
from __future__ import annotations

from skeleton_ai.styled_pipeline import _still_qa_rank


PASS = {"status": "pass", "pass": True, "issues": []}
FAIL_ONE = {"status": "fail", "pass": False, "issues": ["glass_shell_artifact"]}
FAIL_THREE = {
    "status": "fail",
    "pass": False,
    "issues": ["glass_shell_artifact", "extra_limb", "wrong_cast_count"],
}
UNAVAILABLE = {"status": "fail", "pass": False, "issues": ["qa_unavailable"]}


def test_a_pass_beats_any_failure() -> None:
    assert _still_qa_rank(PASS) > _still_qa_rank(FAIL_ONE)
    assert _still_qa_rank(PASS) > _still_qa_rank(FAIL_THREE)
    assert _still_qa_rank(PASS) > _still_qa_rank(UNAVAILABLE)


def test_between_two_failures_fewer_issues_wins() -> None:
    assert _still_qa_rank(FAIL_ONE) > _still_qa_rank(FAIL_THREE)


def test_an_unaudited_still_never_displaces_a_judged_one() -> None:
    """qa_unavailable is absence of evidence, not evidence of quality."""
    assert _still_qa_rank(UNAVAILABLE) < _still_qa_rank(FAIL_THREE)
    assert _still_qa_rank(UNAVAILABLE) < _still_qa_rank(FAIL_ONE)


def test_missing_report_ranks_lowest() -> None:
    for empty in (None, {}):
        assert _still_qa_rank(empty) < _still_qa_rank(FAIL_THREE)


def test_an_equal_retry_does_not_displace_the_original() -> None:
    """The pipeline reverts on `<=`, so ties must keep attempt 1.

    A retry that is merely as good as the original is not an improvement, and
    keeping the first still avoids churning the scene's staging for nothing.
    """
    assert _still_qa_rank(FAIL_ONE) == _still_qa_rank(dict(FAIL_ONE))
    assert not (_still_qa_rank(FAIL_ONE) > _still_qa_rank(dict(FAIL_ONE)))


def test_duplicate_issues_do_not_inflate_the_penalty() -> None:
    noisy = {"status": "fail", "pass": False, "issues": ["extra_limb", "extra_limb"]}
    single = {"status": "fail", "pass": False, "issues": ["extra_limb"]}
    assert _still_qa_rank(noisy) == _still_qa_rank(single)
