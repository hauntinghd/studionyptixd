"""Retired Studio RunPod endpoint mutator.

The production API and its single durable consumer run on Contabo. This module
intentionally exits before importing an HTTP client or reading credentials.
"""

raise SystemExit(
    "RETIRED: Studio endpoints are Contabo-owned; no RunPod endpoint mutation was attempted."
)
