"""
CM Readthrough ETL - Generate capital markets readthrough reports.

This ETL processes multiple banks' earnings call transcripts to extract
Investment Banking & Trading outlook and categorized analyst questions.
"""

from .main import main, get_bank_info, process_all_banks

__all__ = ["main", "get_bank_info", "process_all_banks"]