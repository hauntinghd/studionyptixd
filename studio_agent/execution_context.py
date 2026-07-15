"""Per-turn execution identity used by production dispatch boundaries.

The value lives in a ``ContextVar`` so concurrent chat streams cannot borrow
one another's command identity.  It is deliberately small and contains no
RunPod implementation details.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator


_PRODUCTION_COMMAND_ID: ContextVar[str] = ContextVar(
    "studio_production_command_id",
    default="",
)


def current_production_command_id() -> str:
    """Return the stable identity bound to the current agent turn, if any."""

    return str(_PRODUCTION_COMMAND_ID.get() or "").strip()


@contextmanager
def production_command_scope(command_id: str | None) -> Iterator[None]:
    """Bind one stable command identity for the duration of a turn."""

    token = _PRODUCTION_COMMAND_ID.set(str(command_id or "").strip())
    try:
        yield
    finally:
        _PRODUCTION_COMMAND_ID.reset(token)


__all__ = ["current_production_command_id", "production_command_scope"]
