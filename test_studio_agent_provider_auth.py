import os
import unittest
from unittest.mock import AsyncMock, patch

from skeleton_ai.fal_auth import require_fal_key
from studio_agent import openrouter


class StudioAgentProviderAuthTests(unittest.IsolatedAsyncioTestCase):
    def test_fal_key_alias_populates_both_supported_names(self):
        with patch.dict(os.environ, {"FAL_AI_KEY": "", "FAL_KEY": "fal-test-key"}, clear=False):
            key = require_fal_key("test")
            self.assertEqual(key, "fal-test-key")
            self.assertEqual(os.environ["FAL_AI_KEY"], "fal-test-key")
            self.assertEqual(os.environ["FAL_KEY"], "fal-test-key")

    def test_fal_key_missing_has_actionable_error(self):
        with patch.dict(os.environ, {"FAL_AI_KEY": "", "FAL_KEY": ""}, clear=False):
            with self.assertRaisesRegex(RuntimeError, "FAL_AI_KEY or FAL_KEY"):
                require_fal_key("test generation")

    def test_anthropic_model_normalization_preserves_selected_versions(self):
        self.assertEqual(
            openrouter._normalize_anthropic_model("anthropic/claude-3-5-haiku-latest"),
            "anthropic/claude-3-5-haiku-latest",
        )
        self.assertEqual(
            openrouter._normalize_anthropic_model("anthropic/claude-sonnet-4"),
            "anthropic/claude-sonnet-4",
        )

    def test_oversized_anthropic_tool_set_keeps_relevant_callable_tools(self):
        tools = [
            {
                "type": "function",
                "function": {
                    "name": f"tool_{index}",
                    "description": "x" * 500,
                    "parameters": {"type": "object", "properties": {}},
                },
            }
            for index in range(30)
        ]
        tools.append({
            "type": "function",
            "function": {
                "name": "poll_render_job",
                "description": "Poll a production job.",
                "parameters": {
                    "type": "object",
                    "properties": {"job_id": {"type": "string"}, "kind": {"type": "string"}},
                    "required": ["job_id", "kind"],
                },
            },
        })
        selected = openrouter._select_anthropic_tools(
            tools,
            [{"role": "user", "content": "continue and poll the job status"}],
            budget=2500,
        )
        names = {row["name"] for row in selected}
        self.assertIn("poll_render_job", names)
        self.assertLessEqual(openrouter._anthropic_tool_size(selected), 2500)

    async def test_direct_anthropic_primary_bypasses_openrouter(self):
        response = {
            "provider": "anthropic_direct",
            "model": "claude-sonnet-4-6",
            "choices": [{"message": {"role": "assistant", "content": "ok"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1},
        }
        direct = AsyncMock(return_value=response)
        with (
            patch.dict(
                os.environ,
                {
                    "ANTHROPIC_API_KEY": "anthropic-test-key",
                    "OPENROUTER_API_KEY": "",
                },
                clear=False,
            ),
            patch.object(openrouter, "PRIMARY_PROVIDER", "anthropic"),
            patch.object(openrouter, "_anthropic_chat_completion", direct),
        ):
            result = await openrouter.chat_completion(
                messages=[{"role": "user", "content": "hello"}],
                model="anthropic/claude-sonnet-4",
            )

        self.assertEqual(result["provider"], "anthropic_direct")
        direct.assert_awaited_once()
        kwargs = direct.await_args.kwargs
        self.assertEqual(kwargs["provider_label"], "anthropic_direct")
        self.assertEqual(kwargs["model_override"], "claude-sonnet-4")


if __name__ == "__main__":
    unittest.main()
