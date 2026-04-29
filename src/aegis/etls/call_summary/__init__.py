"""Call summary ETL replacement with legacy DOCX and editor HTML outputs."""

from typing import Any

__all__ = [
    "generate_call_summary",
    "CallSummaryResult",
    "CallSummaryError",
    "CallSummaryUserError",
    "CallSummarySystemError",
]


def __getattr__(name: str) -> Any:
    """Lazily expose public symbols from ``main`` without eager module import."""
    if name in __all__:
        from . import main as _main  # pylint: disable=import-outside-toplevel

        return getattr(_main, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
