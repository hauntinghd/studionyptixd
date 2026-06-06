"""Backward-compatible re-exports — use category_registry for new code."""

from .category_registry import (
    BUILTIN_CATEGORIES,
    LEGACY_ALIASES,
    create_custom_category,
    get_category,
    list_categories,
    list_valid_keys,
    slugify_category_key,
)

# Legacy name used by a few imports
CATEGORIES = BUILTIN_CATEGORIES
