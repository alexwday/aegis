"""Interactive HTML CM readthrough editor ETL."""

from typing import Any

__all__ = [
    "generate_cm_readthrough_editor",
    "CMReadthroughEditorResult",
    "CMReadthroughEditorError",
    "CMReadthroughEditorUserError",
    "CMReadthroughEditorSystemError",
]


def __getattr__(name: str) -> Any:
    """Lazily expose public symbols from ``main`` without eager module import."""
    if name in __all__:
        from . import main as _main  # pylint: disable=import-outside-toplevel

        return getattr(_main, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
