"""
CM Readthrough ETL - Generate capital markets readthrough reports.

This ETL processes multiple banks' earnings call transcripts to extract
Investment Banking & Trading outlook and categorized analyst questions.
"""

from .main import (
    main,
    generate_cm_readthrough,
    CMReadthroughResult,
    CMReadthroughError,
    CMReadthroughUserError,
    CMReadthroughSystemError,
)

__all__ = [
    "main",
    "generate_cm_readthrough",
    "CMReadthroughResult",
    "CMReadthroughError",
    "CMReadthroughUserError",
    "CMReadthroughSystemError",
]
