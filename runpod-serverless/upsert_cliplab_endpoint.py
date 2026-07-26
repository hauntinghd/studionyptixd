"""Retired ClipLab RunPod endpoint mutator.

ClipLab production execution is part of the Contabo-owned Studio backend. This
module intentionally exits before importing an HTTP client or reading
credentials.
"""

raise SystemExit(
    "RETIRED: ClipLab execution is Contabo-owned; no RunPod endpoint mutation was attempted."
)
