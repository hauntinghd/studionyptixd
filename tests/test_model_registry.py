from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from studio_agent import model_registry, openrouter, provider_policy


class ModelPolicyTests(unittest.TestCase):
    def test_denied_models_are_hidden_and_rejected(self) -> None:
        self.assertEqual(openrouter.build_model_catalog([]), [])
        self.assertTrue(openrouter.CURATED_META)
        self.assertTrue(all("/" not in model_id for model_id in openrouter.CURATED_META))
        self.assertTrue(all(
            provider_policy.model_provider(model_id) == "anthropic"
            for model_id in openrouter.CURATED_META
        ))
        for model_id in (
            "grok-4.5",
            "x-ai/grok-4.5",
            "openai/gpt-5.9",
            "google/gemini-2.5-flash",
            "deepseek/deepseek-chat",
        ):
            with self.subTest(model_id=model_id):
                with self.assertRaises(model_registry.ModelDisabledError):
                    model_registry.assert_model_selectable(model_id)

    def test_fable_is_denied_and_excluded_from_catalog(self) -> None:
        # Fable 5 (and the Mythos family) are usage-based only, not offered as
        # selectable Studio runners — denied even if the live account lists them.
        for model_id in ("claude-fable-5", "anthropic/claude-fable-5", "claude-mythos-5"):
            with self.subTest(model_id=model_id):
                with self.assertRaises(provider_policy.ProviderPolicyDenied):
                    provider_policy.assert_runner_model_allowed(model_id)
        self.assertNotIn("claude-fable-5", openrouter.CURATED_META)
        self.assertNotIn("claude-fable-5", openrouter.RECOMMENDED_MODELS)
        catalog = openrouter.build_model_catalog([{"id": "claude-fable-5"}, {"id": "claude-sonnet-5"}])
        self.assertEqual([r["id"] for r in catalog], ["claude-sonnet-5"])

    def test_premium_runners_surface_first_party_list_price(self) -> None:
        # Every model the user listed shows a per-token list price in the picker.
        expected = {
            "claude-opus-5": (5.0, 25.0),
            "claude-opus-4-8": (5.0, 25.0),
            "claude-opus-4-7": (5.0, 25.0),
            "claude-opus-4-6": (5.0, 25.0),
            "claude-opus-4-5": (5.0, 25.0),
            "claude-opus-4-1": (15.0, 75.0),
            "claude-sonnet-5": (3.0, 15.0),
            "claude-sonnet-4-6": (3.0, 15.0),
            "claude-sonnet-4-5": (3.0, 15.0),
            "claude-haiku-4-5-20251001": (1.0, 5.0),
        }
        catalog = openrouter.build_model_catalog([{"id": mid} for mid in expected])
        priced = {r["id"]: (r["prompt_price_per_m"], r["completion_price_per_m"]) for r in catalog}
        for model_id, prices in expected.items():
            with self.subTest(model_id=model_id):
                self.assertEqual(priced.get(model_id), prices)

    def test_sonnet_five_is_default_and_alias_normalizes(self) -> None:
        self.assertEqual(openrouter.DEFAULT_MODEL, "claude-sonnet-5")
        self.assertEqual(openrouter._normalize_anthropic_model("sonnet"), "claude-sonnet-5")
        self.assertEqual(
            openrouter._normalize_anthropic_model("anthropic/claude-sonnet-5"),
            "claude-sonnet-5",
        )

    def test_anthropic_normalization_does_not_upgrade_explicit_version(self) -> None:
        selected = "claude-3-5-haiku-20241022"
        self.assertEqual(openrouter._normalize_anthropic_model(selected), selected)


class ModelRouteTests(unittest.TestCase):
    def test_direct_anthropic_wins_regardless_of_stale_denied_keys(self) -> None:
        route = model_registry.resolve_model_route(
            "anthropic/claude-sonnet-5",
            xai_configured=True,
            anthropic_configured=True,
            openrouter_configured=True,
        )
        self.assertEqual(route.route_provider, "anthropic_direct")
        self.assertEqual(route.provider_model_id, "claude-sonnet-5")

    def test_denied_routes_fail_before_provider_selection(self) -> None:
        for model_id in ("grok-4.5", "openai/gpt-4o", "google/gemini-2.5-pro"):
            with self.subTest(model_id=model_id):
                with self.assertRaises(model_registry.ModelDisabledError):
                    model_registry.resolve_model_route(
                        model_id,
                        xai_configured=True,
                        anthropic_configured=True,
                        openrouter_configured=True,
                    )

    def test_missing_anthropic_key_never_falls_back_to_openrouter(self) -> None:
        with self.assertRaises(model_registry.ModelUnavailableError):
            model_registry.resolve_model_route(
                "claude-sonnet-5",
                xai_configured=True,
                anthropic_configured=False,
                openrouter_configured=True,
            )


class _Response:
    def __init__(
        self,
        payload: dict | None = None,
        *,
        error: Exception | None = None,
        status_code: int = 200,
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self.text = text
        self._payload = payload or {}
        self._error = error

    def raise_for_status(self) -> None:
        if self._error:
            raise self._error

    def json(self):
        return self._payload


class _Client:
    def __init__(self, *, get_response: _Response | None = None, post_response: _Response | None = None) -> None:
        self.get_response = get_response or _Response()
        self.post_response = post_response or _Response()
        self.posts: list[tuple[str, dict]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, *, headers):
        return self.get_response

    async def post(self, url, *, headers, json):
        self.posts.append((url, dict(json)))
        return self.post_response


class OpenRouterIntegrationPolicyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        openrouter._LAST_KNOWN_ANTHROPIC_MODELS.clear()
        openrouter._MODELS_CACHE.update({"at": 0.0, "by_id": {}})

    async def test_live_catalog_is_anthropic_only_ordered_and_has_no_ghost_rows(self) -> None:
        client = _Client(get_response=_Response({
            "data": [
                {"id": "claude-future-9", "display_name": "Claude Future 9", "context_window": 500_000},
                {"id": "openai/gpt-5.9", "display_name": "Denied"},
                {"id": "claude-sonnet-4-6", "display_name": "Claude Sonnet 4.6"},
                {"id": "claude-sonnet-5", "display_name": "Claude Sonnet 5"},
            ]
        }))
        with (
            patch.dict(os.environ, {"ANTHROPIC_API_KEY": "account-a", "CLAUDE_API_KEY": ""}, clear=False),
            patch.object(openrouter.httpx, "AsyncClient", return_value=client),
        ):
            rows = await openrouter.list_models()
            catalog = openrouter.build_model_catalog(rows)

        self.assertEqual(
            [row["id"] for row in catalog],
            ["claude-sonnet-5", "claude-sonnet-4-6", "claude-future-9"],
        )
        self.assertNotIn("claude-opus-4-8", {row["id"] for row in catalog})
        self.assertTrue(all(row["provider"] == "Anthropic" for row in catalog))
        # Curated models surface their first-party list price in the picker even
        # though Anthropic's /models response omits pricing; a live model with no
        # curated entry (claude-future-9) still shows no price.
        priced = {row["id"]: row["prompt_price_per_m"] for row in catalog}
        self.assertEqual(priced["claude-sonnet-5"], 3.0)
        self.assertEqual(priced["claude-sonnet-4-6"], 3.0)
        self.assertIsNone(priced["claude-future-9"])

    async def test_no_anthropic_key_returns_empty_without_constructing_client(self) -> None:
        env = {
            "ANTHROPIC_API_KEY": "",
            "CLAUDE_API_KEY": "",
            "XAI_API_KEY": "stale",
            "OPENROUTER_API_KEY": "stale",
        }
        with (
            patch.dict(os.environ, env, clear=False),
            patch.object(openrouter.httpx, "AsyncClient", side_effect=AssertionError("network attempted")),
        ):
            self.assertEqual(await openrouter.list_models(), [])

    async def test_last_known_valid_catalog_is_scoped_to_same_key(self) -> None:
        live = _Client(get_response=_Response({"data": [{"id": "claude-sonnet-5"}]}))
        failed = _Client(get_response=_Response(error=RuntimeError("offline")))
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "account-a", "CLAUDE_API_KEY": ""}, clear=False):
            with patch.object(openrouter.httpx, "AsyncClient", return_value=live):
                self.assertEqual([row["id"] for row in await openrouter.list_models()], ["claude-sonnet-5"])
            with patch.object(openrouter.httpx, "AsyncClient", return_value=failed):
                cached = await openrouter.list_models()
        self.assertEqual(cached[0]["catalog_source"], "last_known_valid")

        with (
            patch.dict(os.environ, {"ANTHROPIC_API_KEY": "account-b", "CLAUDE_API_KEY": ""}, clear=False),
            patch.object(openrouter.httpx, "AsyncClient", return_value=failed),
        ):
            self.assertEqual(await openrouter.list_models(), [])

    async def test_denied_model_never_constructs_network_client(self) -> None:
        env = {
            "XAI_API_KEY": "stale-xai",
            "OPENROUTER_API_KEY": "stale-openrouter",
            "ANTHROPIC_API_KEY": "anthropic-key",
            "CLAUDE_API_KEY": "",
        }
        with (
            patch.dict(os.environ, env, clear=False),
            patch.object(openrouter.httpx, "AsyncClient", side_effect=AssertionError("network attempted")),
        ):
            with self.assertRaises(model_registry.ModelDisabledError):
                await openrouter.chat_completion(
                    messages=[{"role": "user", "content": "hello"}],
                    model="grok-4.5",
                )

    async def test_sonnet_five_uses_release_max_tokens_and_omits_manual_controls(self) -> None:
        client = _Client(post_response=_Response({
            "id": "msg-test",
            "model": "claude-sonnet-5",
            "content": [{"type": "text", "text": "ok"}],
            "usage": {"input_tokens": 1, "output_tokens": 1},
        }))
        with (
            patch.dict(os.environ, {"ANTHROPIC_API_KEY": "account-a", "CLAUDE_API_KEY": ""}, clear=False),
            patch.object(openrouter.httpx, "AsyncClient", return_value=client),
        ):
            result = await openrouter.chat_completion(
                messages=[{"role": "user", "content": "hello"}],
                model="claude-sonnet-5",
                temperature=0.91,
                reasoning_depth="deep",
                max_tokens=777,
            )

        self.assertEqual(result["model"], "claude-sonnet-5")
        payload = client.posts[0][1]
        self.assertEqual(payload["max_tokens"], 777)
        for denied_key in ("temperature", "top_p", "top_k", "thinking", "reasoning"):
            self.assertNotIn(denied_key, payload)

    async def test_explicit_anthropic_model_does_not_fall_back_to_another_claude(self) -> None:
        client = _Client(post_response=_Response(status_code=404, text="model not found"))
        with (
            patch.dict(os.environ, {"ANTHROPIC_API_KEY": "account-a", "CLAUDE_API_KEY": ""}, clear=False),
            patch.object(openrouter.httpx, "AsyncClient", return_value=client),
        ):
            with self.assertRaises(RuntimeError):
                await openrouter.chat_completion(
                    messages=[{"role": "user", "content": "hello"}],
                    model="claude-opus-4-8",
                )
        self.assertEqual([payload["model"] for _, payload in client.posts], ["claude-opus-4-8"])

    def test_effective_denied_keys_are_inert(self) -> None:
        with patch.dict(os.environ, {
            "XAI_API_KEY": "stale-xai",
            "OPENROUTER_API_KEY": "stale-openrouter",
            "ANTHROPIC_API_KEY": "anthropic-key",
            "CLAUDE_API_KEY": "",
        }, clear=False):
            self.assertEqual(openrouter.xai_api_key(), "")
            self.assertEqual(openrouter._openrouter_api_key_optional(), "")
            self.assertTrue(openrouter.any_llm_provider_configured())


if __name__ == "__main__":
    unittest.main()
