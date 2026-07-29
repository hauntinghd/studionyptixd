"""Public tier credit volumes must hold their target gross margin.

One credit buys exactly one cent of raw provider spend (CREDIT_USD_VALUE=0.01,
CREDIT_MARGIN=0.0), so a tier's provider allowance is monthly_credits / 100 and
gross margin follows directly from the credit grant. Before this existed nothing
guarded those numbers: the tiers had drifted to 36-60% gross, worst at the top,
so the biggest customers were the worst margin.

These are deliberately strict. Changing a credit grant changes what every
subscriber on that tier receives and what each sale earns, so it should require
editing an assertion, not just a dict.
"""
from __future__ import annotations

import pytest

import backend_settings
import unified_credits


TARGET_GROSS_MARGIN = 0.70

# Measured provider cost of one clean 6-scene animated short on the Kling 2.1 Pro
# default. Used to assert a tier is actually usable, not merely profitable.
SHORT_COGS_USD = 4.0


def _provider_allowance_usd(monthly_credits: int) -> float:
    return monthly_credits * unified_credits.CREDIT_USD_VALUE


def test_credit_unit_economics_are_unchanged() -> None:
    """The margin maths below only holds while a credit is a cent, unmarked up."""
    assert unified_credits.CREDIT_USD_VALUE == 0.01
    assert unified_credits.CREDIT_MARGIN == 0.0


@pytest.mark.parametrize("plan_id", list(backend_settings.PUBLIC_PLAN_IDS))
def test_public_tier_holds_target_margin(plan_id: str) -> None:
    plan = backend_settings.UNIFIED_PLANS[plan_id]
    price = float(plan["price_usd"])
    allowance = _provider_allowance_usd(int(plan["monthly_credits"]))
    gross = 1.0 - (allowance / price)
    assert gross == pytest.approx(TARGET_GROSS_MARGIN, abs=0.005), (
        f"{plan_id} is at {gross:.1%} gross; expected {TARGET_GROSS_MARGIN:.0%}. "
        f"${price:.0f} grants {plan['monthly_credits']} credits "
        f"= ${allowance:.2f} of provider spend."
    )


@pytest.mark.parametrize("plan_id", list(backend_settings.PUBLIC_PLAN_IDS))
def test_public_tier_buys_at_least_two_shorts(plan_id: str) -> None:
    """A tier that cannot finish two videos is not a sellable tier.

    The entry tier is the binding case: a single unbounded repair loop used to
    consume more credits than the whole tier contained, stranding a paying
    customer mid-production.
    """
    plan = backend_settings.UNIFIED_PLANS[plan_id]
    allowance = _provider_allowance_usd(int(plan["monthly_credits"]))
    assert allowance / SHORT_COGS_USD >= 1.85, (
        f"{plan_id} only buys {allowance / SHORT_COGS_USD:.1f} shorts "
        f"(${allowance:.2f} allowance at ~${SHORT_COGS_USD:.2f} per short)"
    )


def test_margin_does_not_degrade_as_price_rises() -> None:
    """Volume discounts must not make big customers the worst margin.

    This is the inversion the recalibration fixed: the top tier had been 36%
    gross while the entry tier was 60%.
    """
    ordered = sorted(
        (
            (float(backend_settings.UNIFIED_PLANS[pid]["price_usd"]), pid)
            for pid in backend_settings.PUBLIC_PLAN_IDS
        ),
    )
    margins = []
    for price, pid in ordered:
        allowance = _provider_allowance_usd(
            int(backend_settings.UNIFIED_PLANS[pid]["monthly_credits"])
        )
        margins.append((pid, 1.0 - allowance / price))
    worst = min(margins, key=lambda row: row[1])
    best = max(margins, key=lambda row: row[1])
    assert best[1] - worst[1] <= 0.02, (
        f"margin spread across public tiers is too wide: "
        f"{worst[0]} at {worst[1]:.1%} vs {best[0]} at {best[1]:.1%}"
    )
