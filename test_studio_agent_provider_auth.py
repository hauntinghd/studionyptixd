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

    def test_anthropic_aliases_normalize_to_supported_ids(self):
        self.assertEqual(
            openrouter._normalize_anthropic_model("anthropic/claude-3-5-haiku-latest"),
            "claude-haiku-4-5-20251001",
        )
        self.assertEqual(
            openrouter._normalize_anthropic_model("anthropic/claude-sonnet-4"),
            "claude-sonnet-4-6",
        )

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
        self.assertEqual(kwargs["model_override"], "anthropic/claude-sonnet-4")


if __name__ == "__main__":
    unittest.main()
