import os
import sys
import types

os.environ["REDIS_QUEUE_ENABLED"] = "0"
os.environ["REDIS_URL"] = ""
sys.modules.setdefault("stripe", types.SimpleNamespace())

from studio_agent.runner import _needs_fresh_public_search, _needs_public_search_preflight


def test_updated_data_followup_requires_public_search_preflight():
    text = "same thing again, but lets get more updated data since its been about 3 days now"

    assert _needs_public_search_preflight(text)
    assert _needs_fresh_public_search(text)


def test_plain_followup_without_data_does_not_force_public_search():
    assert not _needs_public_search_preflight("same thing again")


if __name__ == "__main__":
    test_updated_data_followup_requires_public_search_preflight()
    test_plain_followup_without_data_does_not_force_public_search()
    print("studio agent public search routing tests passed")
