from studio_agent import store
from studio_agent.runner import _needs_channel_data_preflight, _needs_public_search_preflight
from studio_agent.turn_plan import build_turn_plan


PUBLIC_AND_PRIVATE_CHANNEL_PROMPT = (
    "can you see the private data from my channel? if so, can you pull the most "
    "recent public data from YouTube in this channels niche so we can better "
    "understand what does and does not work properly for this kind of psychology "
    "short form content?"
)


def test_compound_capability_question_executes_required_data_tools() -> None:
    assert store.is_public_youtube_research_request(PUBLIC_AND_PRIVATE_CHANNEL_PROMPT)
    assert store.should_auto_run_tools(PUBLIC_AND_PRIVATE_CHANNEL_PROMPT)
    assert _needs_channel_data_preflight(PUBLIC_AND_PRIVATE_CHANNEL_PROMPT)
    assert _needs_public_search_preflight(PUBLIC_AND_PRIVATE_CHANNEL_PROMPT)

    plan = build_turn_plan(PUBLIC_AND_PRIVATE_CHANNEL_PROMPT, {})
    assert not plan.conversational_only
    assert plan.public_youtube_demand
    assert plan.channel_analytics


def test_pure_capability_question_stays_conversational() -> None:
    prompt = "Can you access public YouTube data?"
    assert not store.is_public_youtube_research_request(prompt)
    assert not store.should_auto_run_tools(prompt)
