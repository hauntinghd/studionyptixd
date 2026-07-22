from __future__ import annotations

import pytest

from studio_agent import provider_policy, tools


def _schema_names(rows: list[dict]) -> set[str]:
    return {
        str((row.get("function") or {}).get("name") or "")
        for row in rows
        if isinstance(row, dict)
    }


def test_generic_project_mutation_and_subprocess_tools_are_not_offered() -> None:
    denied = {"run_build_script", "write_project_file"}

    assert denied.isdisjoint(_schema_names(tools.tool_schemas()))
    assert denied.isdisjoint(_schema_names(tools.tools_for_user("policy-user")))
    assert denied == set(tools.DISABLED_PROVIDER_ESCAPE_TOOLS)


@pytest.mark.parametrize("tool_name", ["run_build_script", "write_project_file"])
def test_persisted_escape_tool_action_fails_closed_before_dispatch(tool_name: str) -> None:
    with pytest.raises(provider_policy.ProviderPolicyDenied, match=f"disables tool {tool_name}"):
        tools.execute_tool(
            tool_name,
            {
                "script": "build_cryptic_google_ai_mode.py",
                "relative_path": "long_form/build_cryptic_google_ai_mode.py",
                "content": "raise RuntimeError('must never be written')",
            },
            user_id="policy-user",
            content_format="json",
            session_id="policy-session",
        )
