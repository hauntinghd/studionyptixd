from studio_agent import store
from studio_agent.runner import (
    _needs_channel_data_preflight,
    _needs_public_search_preflight,
    _without_stale_turn_scoped_system_messages,
)
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


def test_existing_short_expansion_is_not_reclassified_as_research() -> None:
    for prompt in (
        "I like scene one. Let's go ahead and make the other five scenes and animate them.",
        "good, now make the rest of the scenes and animate them",
    ):
        plan = build_turn_plan(prompt, {})
        assert not plan.has_execution
        assert not plan.public_youtube_demand
        assert not plan.channel_analytics
        assert not plan.reference_analysis


def test_old_turn_control_directives_do_not_poison_the_next_turn() -> None:
    history = [
        {"role": "system", "content": "You are NYPTID Studio Agent."},
        {
            "role": "system",
            "content": "[Studio Agent approved research execution mode]\nDo not start production.",
        },
        {"role": "user", "content": "Research this niche."},
        {"role": "assistant", "content": "Here is the grounded result."},
        {
            "role": "system",
            "content": "[Studio Agent mode: Plan & Conversation — HARD BOUNDARY]\nDo not render.",
        },
        {
            "role": "system",
            "content": "[Studio Agent preflight tool result: get_public_search_trends]\n{}",
        },
    ]

    cleaned = _without_stale_turn_scoped_system_messages(history)

    assert [message["content"] for message in cleaned] == [
        "You are NYPTID Studio Agent.",
        "Research this niche.",
        "Here is the grounded result.",
        "[Studio Agent preflight tool result: get_public_search_trends]\n{}",
    ]
