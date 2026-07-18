from __future__ import annotations

import tempfile
import unittest
import sys
import types
from pathlib import Path
from unittest.mock import patch

# backend_settings configures Stripe at import time, but wallet unit tests do
# not make network calls. Stub only in genuinely minimal environments so this
# module cannot replace the real SDK for later tests in the same process.
try:
    import stripe  # noqa: F401
except ModuleNotFoundError:
    sys.modules.setdefault("stripe", types.SimpleNamespace(api_key=""))

import unified_credits as credits


class UnifiedCreditsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        credits.WALLETS_PATH = root / "wallets.json"
        credits.LEDGER_PATH = root / "ledger.jsonl"
        credits._wallets.clear()
        credits._loaded = False

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_owner_is_never_debited_or_blocked(self) -> None:
        credits.set_unlimited("owner", True)
        reservation = credits.reserve_credits("owner", 1_000_000, reason="longform")
        self.assertTrue(reservation["unlimited"])
        self.assertEqual(credits.get_balance("owner"), 999_999_999)
        ok, balance = credits.debit_credits("owner", 1_000_000, reason="render")
        self.assertTrue(ok)
        self.assertEqual(balance, 999_999_999)

    def test_monthly_credit_rollover_expires_after_one_month(self) -> None:
        with patch.object(credits, "_month_key", return_value="2026-06"):
            state = credits.set_plan("user", "creator", grant_now=True)
            self.assertEqual(state["balance"], 2_000)
            credits.debit_credits("user", 500, reason="usage")
        with patch.object(credits, "_month_key", return_value="2026-07"):
            credits.ensure_monthly_grant("user")
            state = credits.get_state("user")
            self.assertEqual(state["rollover_balance"], 1_500)
            self.assertEqual(state["monthly_balance"], 2_000)
        with patch.object(credits, "_month_key", return_value="2026-08"):
            credits.ensure_monthly_grant("user")
            state = credits.get_state("user")
            self.assertEqual(state["rollover_balance"], 2_000)
            self.assertEqual(state["monthly_balance"], 2_000)
            self.assertEqual(state["balance"], 4_000)

    def test_topup_is_idempotent_and_does_not_expire(self) -> None:
        credits.add_credits("user", 1_000, idempotency_key="stripe_checkout:1")
        credits.add_credits("user", 1_000, idempotency_key="stripe_checkout:1")
        with patch.object(credits, "_month_key", return_value="2026-06"):
            credits.set_plan("user", "creator", grant_now=True)
        with patch.object(credits, "_month_key", return_value="2026-08"):
            credits.ensure_monthly_grant("user")
        self.assertEqual(credits.get_state("user")["topup_balance"], 1_000)

    def test_reservation_refunds_repair_buffer(self) -> None:
        credits.add_credits("user", 1_000)
        reservation = credits.reserve_usd(
            "user",
            5.0,
            reason="production",
            repair_reserve_pct=0.25,
        )
        self.assertEqual(reservation["credits"], 625)
        self.assertEqual(credits.get_balance("user"), 375)
        state = credits.commit_reservation(
            "user",
            reservation["reservation_id"],
            actual_credits=500,
        )
        self.assertEqual(state["balance"], 500)
        self.assertEqual(state["lifetime_spent"], 500)

    def test_decimal_usd_conversion_ceilings_are_stable(self) -> None:
        self.assertEqual(credits.usd_to_credits("0.000001"), 1)
        self.assertEqual(credits.usd_to_credits("0.010000"), 1)
        self.assertEqual(credits.usd_to_credits("0.010001"), 2)
        self.assertEqual(credits.openrouter_usd(
            {"prompt_tokens": 333, "completion_tokens": 667},
            "0.15",
            "0.60",
        ), credits._usd_decimal("0.000450"))

    def test_debit_usd_records_exact_decimal_metadata(self) -> None:
        credits.add_credits("user", 10)
        charged, balance = credits.debit_usd("user", "0.010001", reason="exact_test")
        self.assertEqual(charged, 2)
        self.assertEqual(balance, 8)
        ledger = credits.LEDGER_PATH.read_text(encoding="utf-8")
        self.assertIn('"provider_usd_decimal": "0.010001"', ledger)

    def test_failed_operation_releases_full_reservation(self) -> None:
        credits.add_credits("user", 1_000)
        reservation = credits.reserve_credits("user", 700, reason="production")
        self.assertEqual(credits.get_balance("user"), 300)
        credits.release_reservation("user", reservation["reservation_id"])
        self.assertEqual(credits.get_balance("user"), 1_000)


if __name__ == "__main__":
    unittest.main()
