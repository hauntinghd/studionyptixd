"""CrypticScience — verified high-RPM lane registry (tax / banking / benefits).

Strategy: steal Markus Graves production speed + title shape; never ship unsourced claims.
Every video starts from primary_sources JSON (see cryptic_google_ai_mode_sources.json pattern).

Lanes:
  tax_irs_banking   — CTR, structuring, reporting, deposits, gift/estate basics
  benefits_ss_medicare — COLA, FRA, Part B/D, IRMAA, enrollment windows

Production modes (fal budget):
  graves  — avatar-only, 10–14 min, ~$15–25 Aurora
  rook    — avatar + source_proof + stat cards, 8–12 min, ~$40–70 Aurora
"""
from __future__ import annotations

from typing import Any

LANES: dict[str, dict[str, Any]] = {
    "tax_irs_banking": {
        "label": "Tax · IRS · Banking",
        "rpm_tier": "very_high",
        "cadence": "1–2/week when news drops; evergreen refresh monthly",
        "audience": "US adults 45+, retirees, small-business owners",
        "title_templates": [
            "What the {Agency} {Action} About {Specific_Rule} (Verified)",
            "The {Dollar_Amount} {Rule_Name} Explained — What Banks Actually Report",
            "{Rule_Name}: What Changed in {Year} (Primary Sources Only)",
        ],
        "title_examples": [
            "The $10,000 Bank Rule Explained — What Banks Actually Report (Verified)",
            "What the IRS Says About Cash Deposits Over $10,000 (Verified)",
            "Structuring vs Legal Deposits — What FinCEN Actually Defines",
        ],
        "forbidden": [
            "Claims IRS 'doesn't want you to know' without citing a primary document",
            "Specific penalty amounts unless quoted from IRS/FinCEN text",
            "Legal advice — always educational framing + consult CPA/attorney disclaimer",
        ],
        "primary_source_allowlist": [
            "irs.gov",
            "fincen.gov",
            "treasury.gov",
            "federalregister.gov",
            "congress.gov",
            "consumerfinance.gov",
            "fdic.gov",
            "occ.gov",
        ],
        "source_hubs": [
            {
                "id": "irs_newsroom",
                "title": "IRS Newsroom",
                "url": "https://www.irs.gov/newsroom",
            },
            {
                "id": "fincen_bsa",
                "title": "FinCEN — Bank Secrecy Act",
                "url": "https://www.fincen.gov/resources/statutes-and-regulations/bank-secrecy-act",
            },
            {
                "id": "irs_ctr",
                "title": "IRS — Currency Transaction Reporting",
                "url": "https://www.irs.gov/businesses/small-businesses-self-employed/currency-transaction-reporting",
            },
            {
                "id": "cfpb_deposits",
                "title": "CFPB — Deposits and accounts",
                "url": "https://www.consumerfinance.gov/consumer-tools/bank-accounts/",
            },
        ],
        "motion_graphics": [
            "source_proof",  # quoted .gov line + URL + date
            "counter",       # dollar thresholds, report counts
            "checklist",     # do / don't at bank
            "compare",       # CTR vs SAR, legal vs structuring
        ],
        "chapter_shape": [
            "Cold open — hook + what triggers the rule",
            "Definition — agency + statute name",
            "What is live today vs proposed",
            "Real scenarios (car sale, gift, inheritance) — sourced only",
            "What is NOT illegal / common myths",
            "Checklist + disclaimer + subscribe",
        ],
    },
    "benefits_ss_medicare": {
        "label": "Benefits · Social Security · Medicare",
        "rpm_tier": "very_high",
        "cadence": "1–2/week; spike on COLA/enrollment/premium announcements",
        "audience": "US retirees, near-retirees, caregivers",
        "title_templates": [
            "Social Security {Change} in {Year} — What SSA Actually Posted",
            "Medicare Part B {Topic} in {Year} (Verified — CMS)",
            "What {Benefit} Changes Mean If You Turn {Age} This Year",
        ],
        "title_examples": [
            "Social Security COLA 2026 — What SSA Actually Posted (Verified)",
            "Medicare Part B Premium 2026 — CMS Numbers Explained",
            "Full Retirement Age in 2026 — SSA Rules Without the Hype",
        ],
        "forbidden": [
            "Guaranteed benefit amounts without SSA/CMS citation",
            "Political predictions about solvency without Trustees report quote",
            "Enrollment deadlines invented for clickbait",
        ],
        "primary_source_allowlist": [
            "ssa.gov",
            "medicare.gov",
            "cms.gov",
            "hhs.gov",
            "benefits.gov",
            "usa.gov",
        ],
        "source_hubs": [
            {
                "id": "ssa_cola",
                "title": "SSA — Cost-of-Living Adjustment",
                "url": "https://www.ssa.gov/cola/",
            },
            {
                "id": "ssa_retirement_age",
                "title": "SSA — Full Retirement Age",
                "url": "https://www.ssa.gov/benefits/retirement/planner/ageincrease.html",
            },
            {
                "id": "medicare_costs",
                "title": "Medicare.gov — Costs",
                "url": "https://www.medicare.gov/basics/costs/medicare-costs",
            },
            {
                "id": "cms_newsroom",
                "title": "CMS Newsroom",
                "url": "https://www.cms.gov/newsroom",
            },
        ],
        "motion_graphics": [
            "source_proof",
            "counter",       # COLA %, premium $, FRA age
            "timeline",      # enrollment windows
            "compare",       # Part A/B/D, Original vs Advantage
        ],
        "chapter_shape": [
            "Cold open — who is affected + effective date",
            "Official number / rule from SSA or CMS",
            "Who qualifies / who does not",
            "Timeline — when to act",
            "What the agency did NOT announce",
            "Checklist + disclaimer + subscribe",
        ],
    },
}

PRODUCTION_MODES: dict[str, dict[str, Any]] = {
    "graves": {
        "label": "Graves mode (avatar-only)",
        "avatar_pct": 100,
        "motion_pct": 0,
        "target_minutes": (10, 14),
        "est_fal_usd": (15, 25),
        "avatar_backend": "aurora",
        "notes": "One Aurora pass. Source links in description. Optional 1 source_proof card at open.",
    },
    "rook": {
        "label": "Verified Rook mode",
        "avatar_pct": 45,
        "motion_pct": 55,
        "target_minutes": (8, 12),
        "est_fal_usd": (40, 70),
        "avatar_backend": "aurora",
        "notes": "Avatar + source_proof + stat cards. Use for flagship topics.",
    },
}

HOST_IMAGE_PROMPT = (
    "4K photorealistic male financial explainer host age 45, navy blue button-down shirt, "
    "clean short hair, direct eye contact. Medium close-up shoulders-up. "
    "Small black lapel microphone at lower edge only. "
    "Dark charcoal grey seamless studio backdrop, soft front key light, 16:9"
)

AVATAR_MOTION = (
    "4K studio interview, medium close-up. Dark charcoal backdrop, uniform soft key-light. "
    "Presenter faces lens with steady eye-contact. Hands below frame, minimal head movement. "
    "Broadcast explainer quality."
)

DISCLAIMER = (
    "This video is for educational purposes only and does not constitute tax, legal, or "
    "financial advice. Consult a qualified CPA, tax attorney, or licensed advisor for your "
    "specific situation."
)

UPLOAD_TAGS_BASE = (
    "CrypticScience, verified explainer, primary sources, IRS, taxes, Social Security, "
    "Medicare, retirement planning, seniors, banking rules"
)


def lane_keys() -> list[str]:
    return list(LANES.keys())


def get_lane(key: str) -> dict[str, Any]:
    if key not in LANES:
        raise KeyError(f"Unknown lane {key!r}; choose from {lane_keys()}")
    return LANES[key]
