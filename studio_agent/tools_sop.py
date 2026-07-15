"""Inject Studio tools SOP only on tool-planning turns (Tyler loop).

Chat-only turns get a thin card. Task-maker turns get the full use-when dictionary.
"""
from __future__ import annotations

from typing import Any

from studio_agent import store

# Hard cap so planner context stays usable on smaller models.
_TOOLS_SOP_MAX_CHARS = 42_000


THIN_CHAT_CARD = """
STUDIO CHAT MODE (this turn may be conversation-only):
- Talk like Grok: clear, direct, human. No research forms, no score lines, no "I verified public demand".
- Listen first. Do not fire heavy research or production tools while the user is still explaining.
- Completeness: if goal is unclear, ask for missing pieces (niche, topic, length, style, product URL for ads).
- Public niche research does NOT require a connected YouTube channel.
- Skill level (when set): beginner = Studio leads more; intermediate = share; professional = rely on user;
  intermediate+professional = share but lean on user expertise.
- When you are about to plan or call tools, the task-maker dictionary (tools SOP) is injected separately.
  Follow that dictionary for use-when / do-not and tool order.
""".strip()


def should_inject_tools_sop(
    *,
    user_text: str,
    intent_text: str = "",
    turn_plan: Any = None,
    conversational_turn: bool = False,
    ideation_turn: bool = False,
    reply_to: str = "",
    live_demand_preflight: bool = False,
    public_search_preflight: bool = False,
    channel_data_preflight: bool = False,
    competitor_or_reference: bool = False,
    production_now: bool = False,
) -> bool:
    """True when this turn will plan or execute tools (task-maker needs full SOP)."""
    text = str(intent_text or user_text or "")
    if str(reply_to or "").strip():
        return True
    if production_now or store.is_explicit_production_request(text):
        return True
    if live_demand_preflight or public_search_preflight or channel_data_preflight:
        return True
    if competitor_or_reference:
        return True
    if store.is_public_youtube_research_request(text):
        return True
    if store.should_auto_run_tools(text):
        return True
    if turn_plan is not None and bool(getattr(turn_plan, "has_execution", False)):
        return True
    if ideation_turn and store.should_auto_run_tools(text):
        return True
    # Pure chat: no auto tools, no execution plan
    if conversational_turn and not store.should_auto_run_tools(text):
        return False
    # Default: if tools will run via auto, inject
    return bool(store.should_auto_run_tools(text))


def load_tools_sop_text(*, max_chars: int = _TOOLS_SOP_MAX_CHARS) -> str:
    """Load Tyler tools dictionary for the task-maker."""
    try:
        from studio_agent.skills import read_skill

        body = read_skill("studio-agent-tools", max_chars=max_chars)
        return (
            "[Studio tools SOP — TASK-MAKER DICTIONARY for this turn]\n"
            "You are planning or calling tools. Use this as the instruction manual: "
            "use-when, do-not, recipes, approval tools. "
            "Still reply to the user in natural conversation after tools (no form dumps).\n\n"
            + body
        )
    except Exception:
        # Fallback if skill missing
        return (
            "[Studio tools SOP — compact fallback]\n"
            "- Live Demand: get_public_search_trends only (one call); channel not required.\n"
            "- Do not dual-call search_youtube_public same niche same turn.\n"
            "- Shorts: estimate cost → start_shortform_generate (approval) → scenes → animate → finalize.\n"
            "- Product ads: ingest_product_reference before start_shortform_generate.\n"
            "- No research forms or score lines in user replies.\n"
        )


def thin_chat_card() -> str:
    return THIN_CHAT_CARD
