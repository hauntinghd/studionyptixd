"""Retired runtime-registry activation entrypoint.

Training artifacts may remain available for offline research, but this script
cannot select a RunPod-backed production runtime.
"""

raise SystemExit(
    "RETIRED: ClipLab RunPod runtime activation is disabled; no registry was changed."
)
