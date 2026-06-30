import json
import tempfile
from pathlib import Path

from studio_agent import memory


def _use_temp_memory(root: Path) -> None:
    memory.MEMORY_DIR = root


def test_public_youtube_search_saves_hydrated_evidence_contract():
    with tempfile.TemporaryDirectory() as tmp:
        _use_temp_memory(Path(tmp))
        payload = {
            "source": "youtube_data_api_public_search",
            "query": "dark psychology shorts",
            "order": "viewCount",
            "cache_status": "fresh",
            "private_analytics": False,
            "videos": [
                {
                    "video_id": "abc123",
                    "title": "The Narcissist's Fake Apology",
                    "channel_title": "Caroline Strawson",
                    "views": 7927557,
                    "likes": 243910,
                    "evidence_level": "hydrated_video_stats",
                    "support_label": "supported_high_public_precedent",
                }
            ],
        }

        memory.observe_tool_result(
            "user-public",
            "search_youtube_public",
            {"query": "dark psychology shorts", "order": "viewCount"},
            json.dumps(payload),
        )

        summary = memory.summarize_for_prompt("user-public")
        assert "Public YouTube evidence for 'dark psychology shorts'" in summary
        assert "The Narcissist's Fake Apology" in summary
        assert "Do not claim CTR, AVD, retention" in summary


def test_channel_analytics_saves_private_channel_scoped_learning_contract():
    with tempfile.TemporaryDirectory() as tmp:
        _use_temp_memory(Path(tmp))
        payload = {
            "channel_id": "UC123",
            "registry_key": "hidden_cortex",
            "channel_title": "Hidden Cortex",
            "growth_playbook": {"stage": "early"},
            "analytics_data_quality": {
                "effective_source": "youtube_analytics_live",
                "oauth_connected": True,
                "video_rows_available": 9,
                "retention_rows_available": 4,
            },
            "insights": {
                "top_titles": [
                    {"title": "Why People Self Sabotage Good Things", "views": 451155}
                ],
                "breakout_titles": [
                    {
                        "title": "Why People Self Sabotage Good Things",
                        "views": 451155,
                        "lift_vs_baseline": 3.2,
                    }
                ],
                "hook_patterns": ["Open with one concrete hidden behavior."],
                "thumbnail_signals": ["Avoid generic brain imagery."],
            },
        }

        memory.observe_tool_result(
            "user-private",
            "get_channel_analytics",
            {"channel_id": "UC123", "registry_key": "hidden_cortex"},
            json.dumps(payload),
        )

        summary = memory.summarize_for_prompt("user-private", channel_id="UC123")
        assert "Channel memory: Hidden Cortex" in summary
        assert "Growth stage/playbook: early" in summary
        assert "Top channel performer visible to Catalyst" in summary
        assert "Private/channel analytics evidence source: youtube_analytics_live" in summary
        assert "do not mix it into other channels" in summary


if __name__ == "__main__":
    test_public_youtube_search_saves_hydrated_evidence_contract()
    test_channel_analytics_saves_private_channel_scoped_learning_contract()
    print("studio agent memory learning tests passed")
