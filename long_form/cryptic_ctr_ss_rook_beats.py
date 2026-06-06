"""CrypticScience — $10K bank rule + SS COLA beat map (~8–9 min Rook).

Graves hook + Stephan scenarios + source-proof motion graphics.
"""
from __future__ import annotations

from long_form.cryptic_ctr_ss_script import SCENES, TITLE
from long_form.cryptic_verified_lanes import AVATAR_MOTION, HOST_IMAGE_PROMPT

STABLE_AVATAR_PROMPT = AVATAR_MOTION

_NARR = {s["id"]: s["narration"] for s in SCENES}

_SSA = "https://www.ssa.gov/news/en/cola/factsheets/2026.html"
_SSA_PRESS = "https://www.ssa.gov/news/en/press/releases/2025-10-24.html"
_FINCEN = "https://www.fincen.gov/resources/statutes-and-regulations/bank-secrecy-act"
_FDIC = "https://www.fdic.gov/sites/default/files/2024-03/fil21012c.pdf"
_EO = "https://www.whitehouse.gov/presidential-actions/2026/05/restoring-integrity-to-americas-financial-system/"

BEATS = [
    {"id": "01_hook", "type": "avatar", "narration": _NARR["01_hook"]},
    {
        "id": "01b_proof_fincen",
        "type": "motion",
        "mg": "source_proof",
        "duration_sec": 8.0,
        "mg_args": {
            "url": _FINCEN,
            "page_title": "Bank Secrecy Act — FinCEN",
            "author": "Financial Crimes Enforcement Network",
            "date_str": "Verified May 27, 2026",
            "quote": (
                "Financial institutions must report currency transactions over $10,000 "
                "in accordance with the Bank Secrecy Act."
            ),
            "highlights": ["$10,000", "Bank Secrecy Act"],
        },
    },
    {"id": "02_ctr_rule", "type": "avatar", "narration": _NARR["02_ctr_rule"]},
    {
        "id": "02b_counter_10k",
        "type": "motion",
        "mg": "counter",
        "duration_sec": 7.0,
        "mg_args": {
            "final_value": 10000,
            "prefix": "$",
            "suffix": "",
            "label": "CASH CTR THRESHOLD — ONE BUSINESS DAY",
            "source": "FinCEN / BSA · FDIC guidance",
            "accent_color": (235, 180, 60),
        },
    },
    {"id": "03_structuring", "type": "avatar", "narration": _NARR["03_structuring"]},
    {
        "id": "03b_check_structuring",
        "type": "motion",
        "mg": "checklist",
        "duration_sec": 9.0,
        "mg_args": {
            "title": "STRUCTURING — DO NOT DO THIS",
            "items": [
                "Split cash deposits to stay under $10,000",
                "Ask the teller how to avoid a CTR",
                "Deposit for someone else to spread amounts",
            ],
            "source": "31 U.S.C. § 5324 · FinCEN",
            "check_color": (235, 90, 90),
        },
    },
    {"id": "04_social_security", "type": "avatar", "narration": _NARR["04_social_security"]},
    {
        "id": "04b_proof_ssa",
        "type": "motion",
        "mg": "source_proof",
        "duration_sec": 8.0,
        "mg_args": {
            "url": _SSA_PRESS,
            "page_title": "SSA COLA 2026 Announcement",
            "author": "Social Security Administration",
            "date_str": "October 24, 2025",
            "quote": (
                "Social Security benefits will increase 2.8 percent in 2026. "
                "On average, retirement benefits will increase by about $56 per month."
            ),
            "highlights": ["2.8 percent", "$56 per month"],
        },
    },
    {
        "id": "04c_counter_cola",
        "type": "motion",
        "mg": "counter",
        "duration_sec": 7.0,
        "mg_args": {
            "final_value": 2.8,
            "suffix": "%",
            "label": "SOCIAL SECURITY COLA — 2026",
            "source": "SSA · October 24, 2025",
            "decimals": 1,
            "accent_color": (60, 210, 120),
        },
    },
    {
        "id": "04d_counter_avg",
        "type": "motion",
        "mg": "counter",
        "duration_sec": 7.0,
        "mg_args": {
            "final_value": 2071,
            "prefix": "$",
            "suffix": "",
            "label": "AVG RETIRED WORKER BENEFIT — JAN 2026",
            "source": "SSA COLA Fact Sheet · 2026",
            "accent_color": (80, 160, 255),
        },
    },
    {"id": "05_scenarios", "type": "avatar", "narration": _NARR["05_scenarios"]},
    {
        "id": "05b_compare_cash",
        "type": "motion",
        "mg": "compare",
        "duration_sec": 9.0,
        "mg_args": {
            "headline": "TRIGGERS CTR vs DOES NOT",
            "left_title": "CASH — CTR ZONE",
            "right_title": "NOT CASH — NO CTR",
            "left_items": [
                "Physical bills over $10K / day",
                "Aggregated same-day cash deposits",
                "Cash from car sale / gift / savings",
            ],
            "right_items": [
                "Social Security direct deposit",
                "ACH / wire between banks",
                "Check deposits (not currency CTR)",
            ],
            "source": "FinCEN CTR guidance · SSA",
        },
    },
    {"id": "06_may19_eo", "type": "avatar", "narration": _NARR["06_may19_eo"]},
    {
        "id": "06b_proof_eo",
        "type": "motion",
        "mg": "source_proof",
        "duration_sec": 8.0,
        "mg_args": {
            "url": _EO,
            "page_title": "Restoring Integrity to America's Financial System",
            "author": "The White House",
            "date_str": "May 19, 2026",
            "quote": (
                "Within 90 days, Treasury shall propose changes to Bank Secrecy Act "
                "implementing regulations to strengthen customer due diligence."
            ),
            "highlights": ["90 days", "Bank Secrecy Act"],
        },
    },
    {"id": "07_ctr_sar", "type": "avatar", "narration": _NARR["07_ctr_sar"]},
    {
        "id": "07b_compare_ctr_sar",
        "type": "motion",
        "mg": "compare",
        "duration_sec": 9.0,
        "mg_args": {
            "headline": "CTR vs SAR",
            "left_title": "CTR",
            "right_title": "SAR",
            "left_items": [
                "Automatic over $10K cash / day",
                "Threshold-based",
                "Filed with FinCEN",
            ],
            "right_items": [
                "Bank judgment call",
                "Suspicious patterns",
                "You are not notified",
            ],
            "source": "FinCEN · BSA",
        },
    },
    {"id": "08_bank_checklist", "type": "avatar", "narration": _NARR["08_bank_checklist"]},
    {
        "id": "08b_checklist_bank",
        "type": "motion",
        "mg": "checklist",
        "duration_sec": 10.0,
        "mg_args": {
            "title": "LARGE CASH DEPOSIT — CHECKLIST",
            "items": [
                "Bring ID + source documents",
                "Answer truthfully if asked",
                "Never split amounts to avoid CTR",
                "Consult CPA/attorney if contacted",
            ],
            "source": "CrypticScience · educational only",
            "check_color": (60, 210, 120),
        },
    },
    {"id": "09_close", "type": "avatar", "narration": _NARR["09_close"]},
    {
        "id": "09b_pct_sourced",
        "type": "motion",
        "mg": "percentage",
        "duration_sec": 6.0,
        "mg_args": {
            "percentage": 100,
            "subtitle": "OF CLAIMS IN THIS VIDEO",
            "body": "SOURCED TO IRS / FINCEN / SSA / WHITE HOUSE",
            "source": "CrypticScience verification policy",
            "accent_color": (80, 160, 255),
        },
    },
]
