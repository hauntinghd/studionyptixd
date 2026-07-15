from __future__ import annotations

import os
import unittest
from unittest.mock import AsyncMock, patch

from studio_agent import model_registry, openrouter


class ModelPolicyTests(unittest.TestCase):
    def test_grok_build_is_visible_but_not_selectable(self) -> None:
        catalog = openrouter.build_model_catalog([])
        row = next(item for item in catalog if item["id"] == "grok-build-0.1")

        self.assertFalse(row["selectable"])
        self.assertTrue(row["disabled"])
        self.assertFalse(row["recommended"])
        self.assertIn("not available", str(row["disabled_reason"]).lower())

    def test_grok_build_policy_applies_to_openrouter_slug(self) -> None:
        with self.assertRaises(model_registry.ModelDisabledError):
            model_registry.assert_model_selectable("x-ai/grok-build-0.1")


class ModelRouteTests(unittest.TestCase):
    def test_anthropic_normalization_does_not_upgrade_model_version(self) -> None:
        selected = "claude-3-5-haiku-20241022"
        self.assertEqual(openrouter._normalize_anthropic_model(selected), selected)

    def test_bare_grok_prefers_xai_direct(self) -> None:
        route = model_registry.resolve_model_route(
            "grok-4.5",
            xai_configured=True,
            anthropic_configured=True,
            openrouter_configured=True,
        )
        self.assertEqual(route.route_provider, "xai_direct")
        self.assertEqual(route.provider_model_id, "grok-4.5")
        self.assertEqual(route.canonical_model, "grok-4.5")

    def test_bare_grok_uses_exact_openrouter_route_without_xai_key(self) -> None:
        route = model_registry.resolve_model_route(
            "grok-4.5",
            xai_configured=False,
            anthropic_configured=True,
            openrouter_configured=True,
        )
        self.assertEqual(route.route_provider, "openrouter")
        self.assertEqual(route.provider_model_id, "x-ai/grok-4.5")
        self.assertEqual(route.canonical_model, "grok-4.5")

    def test_provider_qualified_grok_stays_on_openrouter(self) -> None:
        route = model_registry.resolve_model_route(
            "x-ai/grok-4.5",
            xai_configured=True,
            anthropic_configured=False,
            openrouter_configured=True,
        )
        self.assertEqual(route.route_provider, "openrouter")
        self.assertEqual(route.provider_model_id, "x-ai/grok-4.5")

    def test_non_claude_selection_never_routes_to_anthropic(self) -> None:
        route = model_registry.resolve_model_route(
            "deepseek/deepseek-chat",
            xai_configured=False,
            anthropic_configured=True,
            openrouter_configured=True,
        )
        self.assertEqual(route.route_provider, "openrouter")
        self.assertEqual(route.provider_model_id, "deepseek/deepseek-chat")

    def test_bare_claude_uses_same_model_through_openrouter(self) -> None:
        route = model_registry.resolve_model_route(
            "claude-sonnet-4-6",
            xai_configured=False,
            anthropic_configured=False,
            openrouter_configured=True,
        )
        self.assertEqual(route.route_provider, "openrouter")
        self.assertEqual(route.provider_model_id, "anthropic/claude-sonnet-4.6")
        self.assertEqual(route.canonical_model, "claude-sonnet-4-6")

    def test_missing_matching_provider_does_not_substitute_model(self) -> None:
        with self.assertRaises(model_registry.ModelUnavailableError):
            model_registry.resolve_model_route(
                "grok-4.5",
                xai_configured=False,
                anthropic_configured=True,
                openrouter_configured=False,
            )


class OpenRouterIntegrationPolicyTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_anthropic_models_are_not_limited_to_curated_ids(self) -> None:
        class Response:
            status_code = 200

            def raise_for_status(self) -> None:
                return None

            def json(self):
                return {
                    "data": [
                        {
                            "id": "claude-future-9",
                            "display_name": "Claude Future 9",
                            "context_window": 500_000,
                        }
                    ]
                }

        class Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def get(self, url, *, headers):
                return Response()

        with (
            patch.object(openrouter, "anthropic_api_key", return_value="anthropic-key"),
            patch.object(openrouter, "xai_api_key", return_value=""),
            patch.object(openrouter, "_openrouter_api_key_optional", return_value=""),
            patch.object(openrouter.httpx, "AsyncClient", return_value=Client()),
        ):
            rows = await openrouter.list_models()

        future = next(row for row in rows if row.get("id") == "claude-future-9")
        self.assertEqual(future["name"], "Claude Future 9")
        self.assertEqual(future["provider"], "Anthropic")

    async def test_model_catalog_aggregates_direct_and_marketplace_models(self) -> None:
        class Response:
            status_code = 200

            def raise_for_status(self) -> None:
                return None

            def json(self):
                return {
                    "data": [
                        {
                            "id": "openai/gpt-5.9",
                            "name": "GPT 5.9",
                            "architecture": {"modality": "text->text"},
                        }
                    ]
                }

        class Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def get(self, url, *, headers):
                return Response()

        with (
            patch.object(openrouter, "anthropic_api_key", return_value=""),
            patch.object(openrouter, "xai_api_key", return_value="xai-key"),
            patch.object(openrouter, "_openrouter_api_key_optional", return_value="openrouter-key"),
            patch.object(
                openrouter,
                "list_xai_models",
                AsyncMock(return_value=[{"id": "grok-4.5", "name": "Grok 4.5", "provider": "xAI"}]),
            ),
            patch.object(openrouter.httpx, "AsyncClient", return_value=Client()),
        ):
            rows = await openrouter.list_models()

        ids = {str(row.get("id") or "") for row in rows}
        self.assertIn("grok-4.5", ids)
        self.assertIn("openai/gpt-5.9", ids)

    async def test_disabled_model_is_rejected_before_provider_request(self) -> None:
        env = {
            "XAI_API_KEY": "test-xai-key",
            "X_AI_API_KEY": "",
            "GROK_API_KEY": "",
            "OPENROUTER_API_KEY": "",
            "OPEN_ROUTER_API_KEY": "",
            "ANTHROPIC_API_KEY": "",
            "CLAUDE_API_KEY": "",
        }
        with patch.dict(os.environ, env, clear=False):
            with self.assertRaises(model_registry.ModelDisabledError):
                await openrouter.chat_completion(
                    messages=[{"role": "user", "content": "hello"}],
                    model="grok-build-0.1",
                )

    async def test_explicit_anthropic_model_does_not_fall_back_to_haiku(self) -> None:
        calls: list[str] = []

        class Response:
            status_code = 404
            text = "model not found"

        class Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def post(self, url, *, headers, json):
                calls.append(str(json.get("model") or ""))
                return Response()

        env = {
            "XAI_API_KEY": "",
            "X_AI_API_KEY": "",
            "GROK_API_KEY": "",
            "OPENROUTER_API_KEY": "",
            "OPEN_ROUTER_API_KEY": "",
            "ANTHROPIC_API_KEY": "test-anthropic-key",
            "CLAUDE_API_KEY": "",
        }
        with (
            patch.dict(os.environ, env, clear=False),
            patch.object(openrouter.httpx, "AsyncClient", return_value=Client()),
        ):
            with self.assertRaises(RuntimeError):
                await openrouter.chat_completion(
                    messages=[{"role": "user", "content": "hello"}],
                    model="claude-opus-4-8",
                )

        self.assertEqual(calls, ["claude-opus-4-8"])

    def test_openrouter_route_wins_for_non_claude_even_with_anthropic_key(self) -> None:
        env = {
            "XAI_API_KEY": "",
            "X_AI_API_KEY": "",
            "GROK_API_KEY": "",
            "OPENROUTER_API_KEY": "test-openrouter-key",
            "OPEN_ROUTER_API_KEY": "",
            "ANTHROPIC_API_KEY": "test-anthropic-key",
            "CLAUDE_API_KEY": "",
        }
        with patch.dict(os.environ, env, clear=False):
            route = openrouter.resolve_chat_route("openai/gpt-4o")
        self.assertEqual(route.route_provider, "openrouter")
        self.assertEqual(route.provider_model_id, "openai/gpt-4o")

    def test_grok_with_openrouter_only_uses_openrouter_slug(self) -> None:
        env = {
            "XAI_API_KEY": "",
            "X_AI_API_KEY": "",
            "GROK_API_KEY": "",
            "OPENROUTER_API_KEY": "test-openrouter-key",
            "OPEN_ROUTER_API_KEY": "",
            "ANTHROPIC_API_KEY": "",
            "CLAUDE_API_KEY": "",
        }
        with patch.dict(os.environ, env, clear=False):
            route = openrouter.resolve_chat_route("grok-4.5")
        self.assertEqual(route.route_provider, "openrouter")
        self.assertEqual(route.provider_model_id, "x-ai/grok-4.5")


if __name__ == "__main__":
    unittest.main()
