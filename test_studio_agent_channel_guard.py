import sys
import types

sys.modules.setdefault("stripe", types.SimpleNamespace())

from studio_agent import runner


def test_current_text_zerotier_overrides_stale_empire_context():
    session = {"registry_key": ""}
    active = runner._active_registry_key(session, "WRONG channel, ZeroTier does comic books")
    assert active == "zerotier"


def test_channel_guard_rewrites_wrong_registry_for_analytics():
    args = runner._channel_guard_tool_args(
        "get_channel_analytics",
        {"registry_key": "empire_magnates"},
        "zerotier",
    )
    assert args["registry_key"] == "zerotier"
    assert args["_corrected_registry_key"]["requested"] == "empire_magnates"


def test_channel_guard_does_not_touch_unscoped_tools():
    args = runner._channel_guard_tool_args(
        "list_render_styles",
        {"registry_key": "empire_magnates"},
        "zerotier",
    )
    assert args["registry_key"] == "empire_magnates"


if __name__ == "__main__":
    test_current_text_zerotier_overrides_stale_empire_context()
    test_channel_guard_rewrites_wrong_registry_for_analytics()
    test_channel_guard_does_not_touch_unscoped_tools()
    print("channel guard tests passed")
