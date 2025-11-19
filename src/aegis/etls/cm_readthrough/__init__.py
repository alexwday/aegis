"""
CM Readthrough ETL - Generate capital markets readthrough reports.

This ETL processes multiple banks' earnings call transcripts to extract
Investment Banking & Trading outlook and categorized analyst questions.
"""

from .main import main

__all__ = ["main"]
