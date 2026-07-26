"""Retired Studio RunPod volume mutator.

Studio production data is owned by the Contabo release contract. This module
intentionally exits before importing an HTTP client or reading credentials.
"""

raise SystemExit(
    "RETIRED: Studio data is Contabo-owned; no RunPod volume mutation was attempted."
)
