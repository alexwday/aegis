"""
ETL Orchestrator - Automated Report Generation Scheduler

This script orchestrates two types of ETL workflows:

1. GAP-BASED ETLs (call_summary, key_themes):
   - Monitors aegis_data_availability for new transcript data
   - Generates missing per-bank reports automatically
   - Each ETL uses its own monitored_institutions.yaml config
   - Runs in parallel across banks (sequential within each bank)

2. SCHEDULE-BASED ETLs (cm_readthrough):
   - Runs on specific dates defined in quarterly_schedule.yaml
   - Generates cross-bank quarterly reports
   - Supports retry_until_next logic for late-arriving data
   - Runs sequentially (expensive cross-bank operations)

Features:
- Parallel ETL execution for gap-based ETLs
- Exponential backoff retry on failures
- Execution lock to prevent concurrent runs
- Gap detection between availability and existing reports
- Schedule-based execution with configurable run dates
- Force regeneration option for existing reports

Usage:
    python -m aegis.etls.etl_orchestrator --from-year 2025           # Production run
    python -m aegis.etls.etl_orchestrator --dry-run --from-year 2025 # Preview only
    python -m aegis.etls.etl_orchestrator --etl-type cm_readthrough  # Schedule-based only
    python -m aegis.etls.etl_orchestrator --force --etl-type cm_readthrough  # Force regen
"""

import argparse
import asyncio
import fcntl
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from aegis.connections.postgres_connector import get_connection  # noqa: E402
from aegis.utils.logging import setup_logging, get_logger  # noqa: E402

# Initialize logging
setup_logging()
logger = get_logger()

# Configuration
LOCK_FILE_PATH = "/tmp/aegis_etl_orchestrator.lock"
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 5  # seconds
MAX_RETRY_DELAY = 300  # 5 minutes
MAX_PARALLEL_ETLS = 4  # Number of banks to process in parallel
QUARTERLY_SCHEDULE_PATH = "src/aegis/etls/cm_readthrough/config/quarterly_schedule.yaml"

# ETL configurations
# - Gap-based ETLs: Run when transcript data is available but report doesn't exist
# - Schedule-based ETLs: Run on specific dates per quarterly_schedule.yaml
ETL_CONFIGS = {
    "call_summary": {
        "module": "aegis.etls.call_summary.main",
        "report_type": "call_summary",
        "description": "Earnings call summary by category",
        "monitored_institutions_path": (
            "src/aegis/etls/call_summary/config/monitored_institutions.yaml"
        ),
        "is_cross_bank": False,
        "schedule_driven": False,
    },
    "key_themes": {
        "module": "aegis.etls.key_themes.main",
        "report_type": "key_themes",
        "description": "Q&A themes extraction and grouping",
        "monitored_institutions_path": (
            "src/aegis/etls/key_themes/config/monitored_institutions.yaml"
        ),
        "is_cross_bank": False,
        "schedule_driven": False,
    },
    "cm_readthrough": {
        "module": "aegis.etls.cm_readthrough.main",
        "report_type": "cm_readthrough",
        "description": "Capital markets readthrough across US/European banks",
        "monitored_institutions_path": (
            "src/aegis/etls/cm_readthrough/config/monitored_institutions.yaml"
        ),
        "is_cross_bank": True,
        "schedule_driven": True,
    },
    "bank_earnings_report": {
        "module": "aegis.etls.bank_earnings_report.main",
        "report_type": "bank_earnings_report",
        "description": "Quarterly bank earnings report with metrics and analysis",
        "monitored_institutions_path": (
            "src/aegis/etls/bank_earnings_report/config/monitored_institutions.yaml"
        ),
        "is_cross_bank": False,
        "schedule_driven": False,
        # Two-phase generation: Phase 1 uses RTS+Supp, Phase 2 adds Transcripts
        "two_phase": True,
        "phase1_required": ["rts", "supplementary"],
        "phase2_required": ["transcripts"],
    },
}


class ExecutionLock:
    """
    File-based execution lock to prevent concurrent orchestrator runs.

    This ensures that if the previous run is still in progress, the next
    scheduled run will wait until completion rather than running in parallel.
    """

    def __init__(self, lock_file: str):
        """Initialize lock with file path."""
        self.lock_file = lock_file
        self.file_handle = None

    def __enter__(self):
        """Acquire lock (blocking)."""
        logger.info(f"Attempting to acquire execution lock: {self.lock_file}")
        self.file_handle = open(self.lock_file, "w", encoding="utf-8")
        try:
            # This will block until lock is available
            fcntl.flock(self.file_handle.fileno(), fcntl.LOCK_EX)
            # Write PID and timestamp to lock file
            self.file_handle.write(f"PID: {os.getpid()}\n")
            self.file_handle.write(f"Started: {datetime.now().isoformat()}\n")
            self.file_handle.flush()
            logger.info("Execution lock acquired successfully")
        except Exception as e:
            logger.error(f"Failed to acquire lock: {e}")
            if self.file_handle:
                self.file_handle.close()
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Release lock."""
        if self.file_handle:
            try:
                fcntl.flock(self.file_handle.fileno(), fcntl.LOCK_UN)
                self.file_handle.close()
                logger.info("Execution lock released")
            except Exception as e:
                logger.warning(f"Error releasing lock: {e}")


def load_monitored_institutions(
    etl_type: Optional[str] = None, config_path: Optional[str] = None
) -> Dict[str, Dict[str, Any]]:
    """
    Load monitored institutions from YAML file.

    Args:
        etl_type: ETL type to load institutions for (uses ETL_CONFIGS path)
        config_path: Direct path to config file (overrides etl_type)

    Returns:
        Dictionary mapping database bank_symbol (without country suffix) to bank info
        Example: {"RY": {"id": 1, "name": "Royal Bank of Canada", "yaml_key": "RY-CA", ...}}

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If neither etl_type nor config_path provided
    """
    # Determine the path to use
    if config_path:
        yaml_path = Path(config_path)
    elif etl_type:
        if etl_type not in ETL_CONFIGS:
            raise ValueError(f"Unknown ETL type: {etl_type}")
        yaml_path = Path(ETL_CONFIGS[etl_type]["monitored_institutions_path"])
    else:
        raise ValueError("Either etl_type or config_path must be provided")

    logger.info(f"Loading monitored institutions from {yaml_path}")

    if not yaml_path.exists():
        raise FileNotFoundError(f"Monitored institutions file not found: {yaml_path}")

    with open(yaml_path, "r", encoding="utf-8") as f:
        yaml_data = yaml.safe_load(f)

    # Use YAML keys directly (RY-CA, BMO-CA, etc.) as they match database bank_symbol
    institutions = {}
    for yaml_key, data in yaml_data.items():
        institutions[yaml_key] = {
            **data,
            "yaml_key": yaml_key,
            "db_symbol": yaml_key.split("-")[0],  # Keep short symbol for reference
        }

    logger.info(
        f"Loaded {len(institutions)} monitored institutions for {etl_type or 'custom path'}"
    )
    return institutions


async def get_data_availability(
    bank_symbols: Optional[List[str]] = None, from_year: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Query aegis_data_availability table for banks with transcript data.

    Args:
        bank_symbols: Optional list to filter by specific bank symbols
        from_year: Optional minimum fiscal year (inclusive) to process

    Returns:
        List of dicts with bank_id, bank_symbol, fiscal_year, quarter
    """
    logger.info("Querying data availability table...")

    query = """
        SELECT
            bank_id,
            bank_name,
            bank_symbol,
            fiscal_year,
            quarter,
            database_names
        FROM aegis_data_availability
        WHERE 'transcripts' = ANY(database_names)
    """

    params = {}
    if bank_symbols:
        query += " AND bank_symbol = ANY(:symbols)"
        params["symbols"] = bank_symbols

    if from_year:
        query += " AND fiscal_year >= :from_year"
        params["from_year"] = from_year
        logger.info(f"Filtering to fiscal year >= {from_year}")

    query += " ORDER BY fiscal_year DESC, quarter DESC, bank_id"

    async with get_connection() as conn:
        result = await conn.execute(text(query), params)
        rows = result.fetchall()

    availability = [
        {
            "bank_id": row.bank_id,
            "bank_name": row.bank_name,
            "bank_symbol": row.bank_symbol,
            "fiscal_year": row.fiscal_year,
            "quarter": row.quarter,
            "database_names": row.database_names,
        }
        for row in rows
    ]

    logger.info(f"Found {len(availability)} bank-period combinations with transcript data")
    return availability


async def get_existing_reports(
    bank_symbols: Optional[List[str]] = None,
    report_types: Optional[List[str]] = None,
) -> Set[Tuple[int, int, str, str]]:
    """
    Query aegis_reports table for existing per-bank reports.

    Args:
        bank_symbols: Optional list to filter by specific bank symbols
        report_types: Optional list of report types to check (defaults to gap-based ETLs)

    Returns:
        Set of tuples: (bank_id, fiscal_year, quarter, report_type)
    """
    logger.info("Querying existing per-bank reports...")

    # Default to gap-based ETL report types
    if report_types is None:
        report_types = [
            cfg["report_type"]
            for cfg in ETL_CONFIGS.values()
            if not cfg.get("is_cross_bank", False)
        ]

    query = """
        SELECT
            bank_id,
            fiscal_year,
            quarter,
            report_type
        FROM aegis_reports
        WHERE report_type = ANY(:report_types)
          AND bank_id IS NOT NULL
    """

    params = {"report_types": report_types}
    if bank_symbols:
        query += " AND bank_symbol = ANY(:symbols)"
        params["symbols"] = bank_symbols

    async with get_connection() as conn:
        result = await conn.execute(text(query), params)
        rows = result.fetchall()

    existing = {(row.bank_id, row.fiscal_year, row.quarter, row.report_type) for row in rows}

    logger.info(f"Found {len(existing)} existing per-bank reports")
    return existing


async def get_existing_cross_bank_reports(
    report_types: Optional[List[str]] = None,
) -> Set[Tuple[int, str, str]]:
    """
    Query aegis_reports table for existing cross-bank reports.

    Cross-bank reports have bank_id = NULL and are identified by
    (fiscal_year, quarter, report_type).

    Args:
        report_types: Optional list of report types to check (defaults to schedule-based ETLs)

    Returns:
        Set of tuples: (fiscal_year, quarter, report_type)
    """
    logger.info("Querying existing cross-bank reports...")

    # Default to schedule-based ETL report types
    if report_types is None:
        report_types = [
            cfg["report_type"] for cfg in ETL_CONFIGS.values() if cfg.get("is_cross_bank", False)
        ]

    if not report_types:
        logger.info("No cross-bank report types configured")
        return set()

    query = """
        SELECT
            fiscal_year,
            quarter,
            report_type
        FROM aegis_reports
        WHERE report_type = ANY(:report_types)
          AND bank_id IS NULL
    """

    params = {"report_types": report_types}

    async with get_connection() as conn:
        result = await conn.execute(text(query), params)
        rows = result.fetchall()

    existing = {(row.fiscal_year, row.quarter, row.report_type) for row in rows}

    logger.info(f"Found {len(existing)} existing cross-bank reports")
    return existing


def identify_gaps(
    availability: List[Dict[str, Any]],
    existing_reports: Set[Tuple[int, int, str, str]],
    etl_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Identify missing reports by comparing availability to existing reports.

    This function only processes gap-based (per-bank) ETLs, not schedule-driven ETLs.
    Each ETL type uses its own monitored institutions list.

    Args:
        availability: List of available data from aegis_data_availability
        existing_reports: Set of existing reports from aegis_reports
        etl_filter: Optional filter for specific ETL type

    Returns:
        List of ETL jobs to execute with metadata
    """
    logger.info("Identifying gaps between availability and reports...")

    gaps = []

    # Only process gap-based (non-schedule-driven) ETLs
    if etl_filter:
        if etl_filter not in ETL_CONFIGS:
            logger.warning(f"Unknown ETL type: {etl_filter}")
            return gaps
        if ETL_CONFIGS[etl_filter].get("schedule_driven", False):
            logger.info(f"ETL {etl_filter} is schedule-driven, skipping gap detection")
            return gaps
        etl_types = [etl_filter]
    else:
        etl_types = [
            name for name, cfg in ETL_CONFIGS.items() if not cfg.get("schedule_driven", False)
        ]

    # Load institutions per ETL type and identify gaps
    for etl_type in etl_types:
        try:
            institutions = load_monitored_institutions(etl_type=etl_type)
        except (FileNotFoundError, ValueError) as e:
            logger.error(f"Failed to load institutions for {etl_type}: {e}")
            continue

        report_type = ETL_CONFIGS[etl_type]["report_type"]

        for data in availability:
            bank_id = data["bank_id"]
            bank_symbol = data["bank_symbol"]
            fiscal_year = data["fiscal_year"]
            quarter = data["quarter"]

            # Find institution metadata (using this ETL's institution list)
            institution = institutions.get(bank_symbol)
            if not institution:
                # Bank not monitored by this ETL - this is normal, not a warning
                continue

            # Check if report exists
            report_key = (bank_id, fiscal_year, quarter, report_type)
            if report_key not in existing_reports:
                gaps.append(
                    {
                        "etl_type": etl_type,
                        "bank_id": bank_id,
                        "bank_name": data["bank_name"],
                        "bank_symbol": bank_symbol,
                        "fiscal_year": fiscal_year,
                        "quarter": quarter,
                        "report_type": report_type,
                    }
                )

    logger.info(f"Identified {len(gaps)} missing per-bank reports")
    return gaps


async def get_bank_earnings_data_availability(
    bank_symbols: Optional[List[str]] = None, from_year: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Query aegis_data_availability for bank earnings report eligibility.

    Returns entries where RTS and Supplementary data are available (Phase 1 requirement).

    Args:
        bank_symbols: Optional list to filter by specific bank symbols
        from_year: Optional minimum fiscal year (inclusive) to process

    Returns:
        List of dicts with bank info, period, and database availability
    """
    logger.info("Querying data availability for bank earnings reports...")

    query = """
        SELECT
            bank_id,
            bank_name,
            bank_symbol,
            fiscal_year,
            quarter,
            database_names
        FROM aegis_data_availability
        WHERE 'rts' = ANY(database_names)
          AND 'supplementary' = ANY(database_names)
    """

    params = {}
    if bank_symbols:
        query += " AND bank_symbol = ANY(:symbols)"
        params["symbols"] = bank_symbols

    if from_year:
        query += " AND fiscal_year >= :from_year"
        params["from_year"] = from_year

    query += " ORDER BY fiscal_year DESC, quarter DESC, bank_id"

    async with get_connection() as conn:
        result = await conn.execute(text(query), params)
        rows = result.fetchall()

    availability = [
        {
            "bank_id": row.bank_id,
            "bank_name": row.bank_name,
            "bank_symbol": row.bank_symbol,
            "fiscal_year": row.fiscal_year,
            "quarter": row.quarter,
            "database_names": row.database_names,
            "has_transcript": "transcripts" in row.database_names,
        }
        for row in rows
    ]

    logger.info(f"Found {len(availability)} bank-period combinations with RTS+Supplementary data")
    return availability


async def get_bank_earnings_existing_reports(
    bank_symbols: Optional[List[str]] = None,
) -> Dict[Tuple[int, int, str], Dict[str, Any]]:
    """
    Query aegis_reports for existing bank_earnings_report entries with metadata.

    Returns a dict keyed by (bank_id, fiscal_year, quarter) with report metadata,
    specifically the has_transcript flag from metadata JSONB.

    Args:
        bank_symbols: Optional list to filter by specific bank symbols

    Returns:
        Dict mapping (bank_id, fiscal_year, quarter) to report info including has_transcript
    """
    logger.info("Querying existing bank earnings reports...")

    query = """
        SELECT
            bank_id,
            fiscal_year,
            quarter,
            metadata
        FROM aegis_reports
        WHERE report_type = 'bank_earnings_report'
          AND bank_id IS NOT NULL
    """

    params = {}
    if bank_symbols:
        query += " AND bank_symbol = ANY(:symbols)"
        params["symbols"] = bank_symbols

    async with get_connection() as conn:
        result = await conn.execute(text(query), params)
        rows = result.fetchall()

    existing = {}
    for row in rows:
        key = (row.bank_id, row.fiscal_year, row.quarter)
        # Parse metadata to get has_transcript flag
        metadata = row.metadata or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (json.JSONDecodeError, TypeError):
                metadata = {}
        existing[key] = {
            "has_transcript": metadata.get("has_transcript", False),
            "sources_used": metadata.get("sources_used", []),
        }

    logger.info(f"Found {len(existing)} existing bank earnings reports")
    return existing


async def identify_bank_earnings_gaps(
    bank_symbols: Optional[List[str]] = None,
    from_year: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Identify bank earnings report gaps for two-phase generation.

    Phase 1: RTS + Supplementary available, no existing report
    Phase 2: Transcript available, existing report generated WITHOUT transcript

    Args:
        bank_symbols: Optional list to filter by specific bank symbols
        from_year: Optional minimum fiscal year (inclusive) to process

    Returns:
        Tuple of (phase1_gaps, phase2_gaps):
            - phase1_gaps: New reports to generate (no existing report)
            - phase2_gaps: Reports to regenerate (transcript now available)
    """
    etl_type = "bank_earnings_report"
    etl_config = ETL_CONFIGS[etl_type]

    # Load monitored institutions for this ETL
    try:
        institutions = load_monitored_institutions(etl_type=etl_type)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Failed to load institutions for {etl_type}: {e}")
        return [], []

    # Get data availability (RTS + Supplementary required)
    availability = await get_bank_earnings_data_availability(bank_symbols, from_year)

    if not availability:
        logger.info("No bank-period combinations with RTS+Supplementary data")
        return [], []

    # Get existing reports with metadata
    existing_reports = await get_bank_earnings_existing_reports(bank_symbols)

    phase1_gaps = []  # New reports
    phase2_gaps = []  # Regenerations with transcript

    for data in availability:
        bank_id = data["bank_id"]
        bank_symbol = data["bank_symbol"]
        fiscal_year = data["fiscal_year"]
        quarter = data["quarter"]
        has_transcript_available = data["has_transcript"]

        # Check if bank is monitored by this ETL
        institution = institutions.get(bank_symbol)
        if not institution:
            continue

        report_key = (bank_id, fiscal_year, quarter)
        existing_report = existing_reports.get(report_key)

        job_base = {
            "etl_type": etl_type,
            "bank_id": bank_id,
            "bank_name": data["bank_name"],
            "bank_symbol": bank_symbol,
            "fiscal_year": fiscal_year,
            "quarter": quarter,
            "report_type": etl_config["report_type"],
        }

        if existing_report is None:
            # Phase 1: No report exists - generate new
            phase1_gaps.append({**job_base, "phase": 1, "is_regeneration": False})
        elif has_transcript_available and not existing_report.get("has_transcript", False):
            # Phase 2: Transcript now available, report was generated without it
            phase2_gaps.append({**job_base, "phase": 2, "is_regeneration": True})
        # else: Report exists with transcript already, nothing to do

    logger.info(
        f"Bank earnings report gaps: {len(phase1_gaps)} new, {len(phase2_gaps)} regenerations"
    )
    return phase1_gaps, phase2_gaps


def load_quarterly_schedule() -> Dict[str, Dict[str, Any]]:
    """
    Load the quarterly schedule configuration from YAML file.

    Returns:
        Dictionary mapping ETL type to schedule entries.
        Example: {
            "cm_readthrough": {
                "FY2025_Q1": {"fiscal_year": 2025, "quarter": "Q1", "run_date": "2025-02-15", ...},
                ...
            }
        }

    Raises:
        FileNotFoundError: If schedule file doesn't exist
    """
    schedule_path = Path(QUARTERLY_SCHEDULE_PATH)

    if not schedule_path.exists():
        raise FileNotFoundError(f"Quarterly schedule file not found: {schedule_path}")

    logger.info(f"Loading quarterly schedule from {schedule_path}")

    with open(schedule_path, "r", encoding="utf-8") as f:
        schedule = yaml.safe_load(f)

    if not schedule:
        logger.warning("Quarterly schedule file is empty")
        return {}

    # Count enabled entries per ETL
    for etl_type, entries in schedule.items():
        if entries:
            enabled_count = sum(1 for e in entries.values() if e.get("enabled", True))
            logger.info(f"Loaded {enabled_count} enabled schedule entries for {etl_type}")

    return schedule


def get_next_quarter_run_date(
    schedule_entries: Dict[str, Any], current_entry_key: str
) -> Optional[date]:
    """
    Get the run_date of the next quarter's schedule entry.

    Args:
        schedule_entries: All schedule entries for an ETL type
        current_entry_key: The key of the current entry (e.g., "FY2025_Q1")

    Returns:
        The run_date of the next quarter, or None if not found
    """
    # Parse current entry
    current = schedule_entries.get(current_entry_key, {})
    current_year = current.get("fiscal_year")
    current_quarter = current.get("quarter")

    if not current_year or not current_quarter:
        return None

    # Calculate next quarter
    quarter_num = int(current_quarter[1])  # Q1 -> 1, Q2 -> 2, etc.
    if quarter_num == 4:
        next_year = current_year + 1
        next_quarter = "Q1"
    else:
        next_year = current_year
        next_quarter = f"Q{quarter_num + 1}"

    # Find next quarter's entry
    next_entry_key = f"FY{next_year}_{next_quarter}"
    next_entry = schedule_entries.get(next_entry_key)

    if next_entry and next_entry.get("run_date"):
        try:
            return datetime.strptime(next_entry["run_date"], "%Y-%m-%d").date()
        except ValueError:
            return None

    return None


def check_scheduled_etls(
    schedule: Dict[str, Dict[str, Any]],
    existing_cross_bank_reports: Set[Tuple[int, str, str]],
    force: bool = False,
    etl_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Check which scheduled ETLs should run based on current date and existing reports.

    Args:
        schedule: Quarterly schedule configuration
        existing_cross_bank_reports: Set of (fiscal_year, quarter, report_type) tuples
        force: If True, run even if report already exists
        etl_filter: Optional filter for specific ETL type

    Returns:
        List of scheduled ETL jobs to execute
    """
    today = date.today()
    scheduled_jobs = []

    # Filter to schedule-driven ETLs only
    schedule_driven_etls = [
        name for name, cfg in ETL_CONFIGS.items() if cfg.get("schedule_driven", False)
    ]

    if etl_filter:
        if etl_filter not in schedule_driven_etls:
            if etl_filter in ETL_CONFIGS:
                logger.info(f"ETL {etl_filter} is not schedule-driven")
            return scheduled_jobs
        schedule_driven_etls = [etl_filter]

    for etl_type in schedule_driven_etls:
        if etl_type not in schedule:
            logger.warning(f"No schedule entries found for {etl_type}")
            continue

        entries = schedule[etl_type]
        report_type = ETL_CONFIGS[etl_type]["report_type"]

        for entry_key, entry in entries.items():
            # Skip disabled entries
            if not entry.get("enabled", True):
                logger.debug(f"Schedule entry {entry_key} is disabled - skipping")
                continue

            fiscal_year = entry.get("fiscal_year")
            quarter = entry.get("quarter")
            run_date_str = entry.get("run_date")
            use_latest = entry.get("use_latest", True)
            retry_until_next = entry.get("retry_until_next", True)
            force_regenerate = entry.get("force_regenerate", False)

            if not all([fiscal_year, quarter, run_date_str]):
                logger.warning(f"Incomplete schedule entry {entry_key} - skipping")
                continue

            try:
                run_date = datetime.strptime(run_date_str, "%Y-%m-%d").date()
            except ValueError:
                logger.warning(f"Invalid run_date format in {entry_key}: {run_date_str}")
                continue

            # Check if report already exists
            report_key = (fiscal_year, quarter, report_type)
            report_exists = report_key in existing_cross_bank_reports

            # Determine if we should run this entry
            should_run = False
            skip_reason = None

            if today < run_date:
                # Not yet time to run
                skip_reason = f"run_date {run_date_str} is in the future"
            elif today == run_date:
                # Exact run date
                if report_exists and not force and not force_regenerate:
                    skip_reason = "report already exists (use --force to regenerate)"
                else:
                    should_run = True
            else:
                # Past run date - check retry_until_next logic
                if retry_until_next:
                    next_run_date = get_next_quarter_run_date(entries, entry_key)
                    if next_run_date and today >= next_run_date:
                        skip_reason = f"past retry window (next quarter run_date: {next_run_date})"
                    elif report_exists and not force and not force_regenerate:
                        skip_reason = "report already exists (use --force to regenerate)"
                    else:
                        should_run = True
                else:
                    # No retry - only run on exact date
                    skip_reason = f"past run_date {run_date_str} and retry_until_next=false"

            if should_run:
                logger.info(
                    f"📅 SCHEDULED: {etl_type} {fiscal_year} {quarter} - "
                    f"{'regenerating' if report_exists else 'generating new report'}"
                )
                scheduled_jobs.append(
                    {
                        "etl_type": etl_type,
                        "fiscal_year": fiscal_year,
                        "quarter": quarter,
                        "report_type": report_type,
                        "use_latest": use_latest,
                        "is_regeneration": report_exists,
                    }
                )
            else:
                logger.debug(f"Schedule entry {entry_key} skipped: {skip_reason}")

    logger.info(f"Identified {len(scheduled_jobs)} scheduled ETL jobs to run")
    return scheduled_jobs


async def run_scheduled_etl_with_retry(
    etl_type: str,
    fiscal_year: int,
    quarter: str,
    use_latest: bool = True,
    dry_run: bool = False,
    verbose: bool = False,
) -> Dict[str, Any]:
    """
    Execute a scheduled (cross-bank) ETL with exponential backoff retry logic.

    Args:
        etl_type: Type of ETL (e.g., cm_readthrough)
        fiscal_year: Fiscal year
        quarter: Quarter (Q1-Q4)
        use_latest: If True, use latest available data
        dry_run: If True, skip actual execution
        verbose: If True, stream subprocess output to console

    Returns:
        Result dictionary with success status and metadata
    """
    etl_config = ETL_CONFIGS[etl_type]
    module = etl_config["module"]

    log_prefix = f"{etl_type} {fiscal_year} {quarter}"

    if dry_run:
        logger.info(f"[DRY RUN] Would execute scheduled ETL: {log_prefix}")
        return {
            "success": True,
            "dry_run": True,
            "etl_type": etl_type,
            "fiscal_year": fiscal_year,
            "quarter": quarter,
            "is_scheduled": True,
        }

    # Build command for cross-bank ETL
    cmd = [
        sys.executable,
        "-m",
        module,
        "--year",
        str(fiscal_year),
        "--quarter",
        quarter,
    ]

    if use_latest:
        cmd.append("--use-latest")

    # Retry loop with exponential backoff
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Executing scheduled ETL {log_prefix} (attempt {attempt}/{MAX_RETRIES})")
            start_time = time.time()

            # Run ETL subprocess
            if verbose:
                print(f"\n{'='*60}")
                print(f"OUTPUT: {log_prefix}")
                print(f"{'='*60}")
                result = subprocess.run(cmd, timeout=7200)  # 2 hour timeout for cross-bank
                stdout_output = ""
                stderr_output = ""
            else:
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=7200  # 2 hour timeout
                )
                stdout_output = result.stdout or ""
                stderr_output = result.stderr or ""

            duration = time.time() - start_time

            if result.returncode == 0:
                logger.info(f"✅ Success {log_prefix} in {duration:.1f}s")
                return {
                    "success": True,
                    "etl_type": etl_type,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                    "duration": duration,
                    "attempts": attempt,
                    "is_scheduled": True,
                }
            else:
                error_msg = stderr_output[-500:] if stderr_output else "Unknown error"
                if not verbose and stdout_output:
                    error_msg += f"\n[stdout]: {stdout_output[-500:]}"
                logger.error(f"❌ Failed {log_prefix} (attempt {attempt}): {error_msg}")

                if attempt < MAX_RETRIES:
                    delay = min(INITIAL_RETRY_DELAY * (2 ** (attempt - 1)), MAX_RETRY_DELAY)
                    logger.info(f"Retrying in {delay}s...")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Max retries reached for {log_prefix} - skipping")
                    return {
                        "success": False,
                        "etl_type": etl_type,
                        "fiscal_year": fiscal_year,
                        "quarter": quarter,
                        "error": error_msg,
                        "attempts": attempt,
                        "is_scheduled": True,
                    }

        except subprocess.TimeoutExpired:
            logger.error(f"❌ Timeout {log_prefix} (attempt {attempt})")
            if attempt >= MAX_RETRIES:
                return {
                    "success": False,
                    "etl_type": etl_type,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                    "error": "Timeout after 2 hours",
                    "attempts": attempt,
                    "is_scheduled": True,
                }
            delay = min(INITIAL_RETRY_DELAY * (2 ** (attempt - 1)), MAX_RETRY_DELAY)
            await asyncio.sleep(delay)

        except Exception as e:
            logger.error(f"❌ Exception {log_prefix}: {e}", exc_info=True)
            if attempt >= MAX_RETRIES:
                return {
                    "success": False,
                    "etl_type": etl_type,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                    "error": str(e),
                    "attempts": attempt,
                    "is_scheduled": True,
                }
            delay = min(INITIAL_RETRY_DELAY * (2 ** (attempt - 1)), MAX_RETRY_DELAY)
            await asyncio.sleep(delay)


async def run_etl_with_retry(
    etl_type: str,
    bank_symbol: str,
    fiscal_year: int,
    quarter: str,
    dry_run: bool = False,
    verbose: bool = False,
) -> Dict[str, Any]:
    """
    Execute an ETL with exponential backoff retry logic.

    Args:
        etl_type: Type of ETL (call_summary or key_themes)
        bank_symbol: Bank symbol to process
        fiscal_year: Fiscal year
        quarter: Quarter (Q1-Q4)
        dry_run: If True, skip actual execution
        verbose: If True, stream subprocess output to console

    Returns:
        Result dictionary with success status and metadata
    """
    etl_config = ETL_CONFIGS[etl_type]
    module = etl_config["module"]

    # The downstream ETLs expect the short ticker (e.g., RY) or ID, not the country suffix.
    bank_arg = bank_symbol.replace("_", "-").strip().split("-")[0]

    log_prefix = f"{bank_symbol} {fiscal_year} {quarter} [{etl_type}]"

    if dry_run:
        logger.info(f"[DRY RUN] Would execute: {log_prefix}")
        return {
            "success": True,
            "dry_run": True,
            "etl_type": etl_type,
            "bank_symbol": bank_symbol,
            "fiscal_year": fiscal_year,
            "quarter": quarter,
        }

    # Build command
    cmd = [
        sys.executable,
        "-m",
        module,
        "--bank",
        bank_arg,
        "--year",
        str(fiscal_year),
        "--quarter",
        quarter,
    ]

    # Retry loop with exponential backoff
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Executing {log_prefix} (attempt {attempt}/{MAX_RETRIES})")
            start_time = time.time()

            # Run ETL subprocess
            if verbose:
                # Stream output in real-time
                print(f"\n{'='*60}")
                print(f"OUTPUT: {log_prefix}")
                print(f"{'='*60}")
                result = subprocess.run(cmd, timeout=3600)  # 1 hour timeout
                stdout_output = ""
                stderr_output = ""
            else:
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=3600  # 1 hour timeout
                )
                stdout_output = result.stdout or ""
                stderr_output = result.stderr or ""

            duration = time.time() - start_time

            if result.returncode == 0:
                logger.info(f"✅ Success {log_prefix} in {duration:.1f}s")
                return {
                    "success": True,
                    "etl_type": etl_type,
                    "bank_symbol": bank_symbol,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                    "duration": duration,
                    "attempts": attempt,
                }
            else:
                error_msg = stderr_output[-500:] if stderr_output else "Unknown error"
                if not verbose and stdout_output:
                    # Also check stdout for errors when not in verbose mode
                    error_msg += f"\n[stdout]: {stdout_output[-500:]}"
                logger.error(f"❌ Failed {log_prefix} (attempt {attempt}): {error_msg}")

                if attempt < MAX_RETRIES:
                    # Calculate exponential backoff delay
                    delay = min(INITIAL_RETRY_DELAY * (2 ** (attempt - 1)), MAX_RETRY_DELAY)
                    logger.info(f"Retrying in {delay}s...")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Max retries reached for {log_prefix} - skipping")
                    return {
                        "success": False,
                        "etl_type": etl_type,
                        "bank_symbol": bank_symbol,
                        "fiscal_year": fiscal_year,
                        "quarter": quarter,
                        "error": error_msg,
                        "attempts": attempt,
                    }

        except subprocess.TimeoutExpired:
            logger.error(f"❌ Timeout {log_prefix} (attempt {attempt})")
            if attempt >= MAX_RETRIES:
                return {
                    "success": False,
                    "etl_type": etl_type,
                    "bank_symbol": bank_symbol,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                    "error": "Timeout after 1 hour",
                    "attempts": attempt,
                }
            delay = min(INITIAL_RETRY_DELAY * (2 ** (attempt - 1)), MAX_RETRY_DELAY)
            await asyncio.sleep(delay)

        except Exception as e:
            logger.error(f"❌ Exception {log_prefix}: {e}", exc_info=True)
            if attempt >= MAX_RETRIES:
                return {
                    "success": False,
                    "etl_type": etl_type,
                    "bank_symbol": bank_symbol,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                    "error": str(e),
                    "attempts": attempt,
                }
            delay = min(INITIAL_RETRY_DELAY * (2 ** (attempt - 1)), MAX_RETRY_DELAY)
            await asyncio.sleep(delay)


async def execute_etls_parallel(
    gaps: List[Dict[str, Any]],
    dry_run: bool = False,
    max_parallel: int = MAX_PARALLEL_ETLS,
    verbose: bool = False,
) -> Dict[str, Any]:
    """
    Execute ETLs in parallel across banks.

    Within each bank, ETLs run sequentially (call_summary then key_themes).
    Across banks, process up to max_parallel banks simultaneously.

    Args:
        gaps: List of ETL jobs to execute
        dry_run: If True, skip actual execution
        max_parallel: Maximum number of banks to process in parallel
        verbose: If True, stream subprocess output in real-time

    Returns:
        Execution summary with success/failure counts
    """
    logger.info(
        f"Starting parallel execution of {len(gaps)} ETL jobs (max {max_parallel} parallel)"
    )
    start_time = time.time()

    # Group gaps by bank-period (so call_summary and key_themes for same bank run sequentially)
    grouped = {}
    for gap in gaps:
        key = (gap["bank_symbol"], gap["fiscal_year"], gap["quarter"])
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(gap)

    logger.info(f"Grouped into {len(grouped)} bank-period combinations")

    # Execute with semaphore to limit parallelism
    semaphore = asyncio.Semaphore(max_parallel)

    async def process_bank_period(
        bank_symbol: str, fiscal_year: int, quarter: str, jobs: List[Dict[str, Any]]
    ):
        """Process all ETLs for a single bank-period sequentially."""
        async with semaphore:
            results = []
            for job in jobs:
                result = await run_etl_with_retry(
                    etl_type=job["etl_type"],
                    bank_symbol=bank_symbol,
                    fiscal_year=fiscal_year,
                    quarter=quarter,
                    dry_run=dry_run,
                    verbose=verbose,
                )
                results.append(result)
            return results

    # Create tasks for each bank-period
    tasks = [
        process_bank_period(bank_symbol, fiscal_year, quarter, jobs)
        for (bank_symbol, fiscal_year, quarter), jobs in grouped.items()
    ]

    # Execute all tasks
    all_results = await asyncio.gather(*tasks)

    # Flatten results
    results = [result for sublist in all_results for result in sublist]

    # Calculate summary
    duration = time.time() - start_time
    successful = sum(1 for r in results if r.get("success"))
    failed = len(results) - successful

    summary = {
        "total_jobs": len(results),
        "successful": successful,
        "failed": failed,
        "duration": duration,
        "results": results,
    }

    logger.info(f"Execution complete: {successful}/{len(results)} successful in {duration:.1f}s")

    return summary


def print_summary(
    availability: List[Dict[str, Any]],
    existing_reports: Set[Tuple[int, int, str, str]],
    gaps: List[Dict[str, Any]],
    scheduled_jobs: Optional[List[Dict[str, Any]]] = None,
    existing_cross_bank_reports: Optional[Set[Tuple[int, str, str]]] = None,
    bank_earnings_phase1: Optional[List[Dict[str, Any]]] = None,
    bank_earnings_phase2: Optional[List[Dict[str, Any]]] = None,
    execution_summary: Optional[Dict[str, Any]] = None,
):
    """Print execution summary to console."""
    print("\n" + "=" * 80)
    print("ETL ORCHESTRATOR EXECUTION SUMMARY")
    print("=" * 80)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Data Availability: {len(availability)} bank-period combinations with transcripts")
    print(f"Existing Per-Bank Reports: {len(existing_reports)} reports")
    if existing_cross_bank_reports is not None:
        print(f"Existing Cross-Bank Reports: {len(existing_cross_bank_reports)} reports")
    print()

    # Bank Earnings Report section (two-phase)
    phase1 = bank_earnings_phase1 or []
    phase2 = bank_earnings_phase2 or []
    if phase1 or phase2:
        total_bank_earnings = len(phase1) + len(phase2)
        print(f"📊 BANK EARNINGS REPORTS: {total_bank_earnings} jobs")
        print("-" * 80)

        if phase1:
            print(f"\n  Phase 1 - New Reports (RTS+Supp, no transcript): {len(phase1)}")
            for job in phase1:
                print(f"    • {job['bank_symbol']} {job['fiscal_year']} {job['quarter']}")

        if phase2:
            print(f"\n  Phase 2 - Regenerations (transcript now available): {len(phase2)}")
            for job in phase2:
                print(f"    • {job['bank_symbol']} {job['fiscal_year']} {job['quarter']} [regen]")

        print()

    # Gap-based ETLs section (excluding bank_earnings_report which is shown separately)
    standard_gaps = [g for g in gaps if g.get("etl_type") != "bank_earnings_report"]
    if standard_gaps:
        print(f"📋 GAP-BASED ETLs: {len(standard_gaps)} missing per-bank reports")
        print("-" * 80)

        # Group by bank for display
        by_bank = {}
        for gap in standard_gaps:
            key = f"{gap['bank_name']} ({gap['bank_symbol']})"
            if key not in by_bank:
                by_bank[key] = []
            by_bank[key].append(f"{gap['fiscal_year']} {gap['quarter']}: {gap['etl_type']}")

        for bank, jobs in sorted(by_bank.items()):
            print(f"\n{bank}:")
            for job in jobs:
                print(f"  • {job}")
        print()
    elif not phase1 and not phase2:
        print("✅ GAP-BASED ETLs: All per-bank reports up to date!")
        print()

    # Scheduled ETLs section
    if scheduled_jobs is not None:
        if scheduled_jobs:
            print(f"📅 SCHEDULED ETLs: {len(scheduled_jobs)} jobs to run")
            print("-" * 80)
            for job in scheduled_jobs:
                status = "regenerating" if job.get("is_regeneration") else "new"
                print(f"  • {job['etl_type']} {job['fiscal_year']} {job['quarter']} ({status})")
            print()
        else:
            print("✅ SCHEDULED ETLs: No scheduled jobs due at this time")
            print()

    if execution_summary:
        print("-" * 80)
        print("EXECUTION RESULTS")
        print("-" * 80)
        print(f"Total Jobs: {execution_summary['total_jobs']}")
        print(f"Successful: {execution_summary['successful']} ✅")
        print(f"Failed: {execution_summary['failed']} ❌")
        print(f"Duration: {execution_summary['duration']:.1f}s")

        # Show failures if any
        failures = [r for r in execution_summary["results"] if not r.get("success")]
        if failures:
            print("\nFailed Jobs:")
            for failure in failures:
                error_preview = failure.get("error", "Unknown")[:100]
                # Handle both per-bank and cross-bank failures
                if failure.get("is_scheduled"):
                    print(
                        f"  ❌ {failure['etl_type']} {failure['fiscal_year']} {failure['quarter']}"
                        f": {error_preview}"
                    )
                else:
                    print(
                        f"  ❌ {failure.get('bank_symbol', 'N/A')} {failure['fiscal_year']} "
                        f"{failure['quarter']} [{failure['etl_type']}]: {error_preview}"
                    )
        print()

    print("=" * 80 + "\n")


async def main():
    """Main orchestrator entry point."""
    parser = argparse.ArgumentParser(
        description="ETL Orchestrator - Automated report generation for monitored banks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
IMPORTANT: Use --from-year to avoid processing all historical data!

Examples:
  # RECOMMENDED: Production run from 2025 forward
  python -m aegis.etls.etl_orchestrator --from-year 2025

  # Dry run - preview what would be processed (always use --from-year!)
  python -m aegis.etls.etl_orchestrator --dry-run --from-year 2025

  # Process specific bank from 2025 forward
  python -m aegis.etls.etl_orchestrator --bank-symbol RY-CA --from-year 2025

  # Process only call summaries from 2025 forward
  python -m aegis.etls.etl_orchestrator --etl-type call_summary --from-year 2025

  # Run CM Readthrough (schedule-driven, uses quarterly_schedule.yaml dates)
  python -m aegis.etls.etl_orchestrator --etl-type cm_readthrough

  # Force regeneration of CM Readthrough even if report exists
  python -m aegis.etls.etl_orchestrator --force --etl-type cm_readthrough

  # WARNING: This processes ALL historical data (use with caution!)
  python -m aegis.etls.etl_orchestrator

  # No lock (for testing - allows concurrent runs)
  python -m aegis.etls.etl_orchestrator --no-lock --from-year 2025
        """,
    )

    parser.add_argument(
        "--dry-run", action="store_true", help="Preview gaps without executing ETLs"
    )

    parser.add_argument("--bank-symbol", help="Process specific bank only (e.g., RY-CA, JPM-US)")

    parser.add_argument(
        "--etl-type",
        choices=list(ETL_CONFIGS.keys()),
        help="Process specific ETL type only",
    )

    parser.add_argument(
        "--no-lock", action="store_true", help="Disable execution lock (for testing only)"
    )

    parser.add_argument(
        "--max-parallel",
        type=int,
        default=MAX_PARALLEL_ETLS,
        help=f"Maximum parallel bank processes (default: {MAX_PARALLEL_ETLS})",
    )

    parser.add_argument(
        "--from-year",
        type=int,
        help=(
            "IMPORTANT: Only process data from this fiscal year forward (e.g., 2025). "
            "Without this flag, ALL historical data will be processed. "
            "Recommended for production runs to avoid reprocessing old data."
        ),
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Stream subprocess output in real-time (useful for debugging)",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Force regeneration of reports even if they already exist",
    )

    args = parser.parse_args()

    execution_id = str(uuid.uuid4())
    logger.info(f"Starting ETL Orchestrator [execution_id={execution_id}]")

    try:
        # Acquire execution lock (unless disabled)
        lock_context = ExecutionLock(LOCK_FILE_PATH) if not args.no_lock else None

        if lock_context:
            with lock_context:
                await _run_orchestrator(args, execution_id)
        else:
            logger.warning("Running WITHOUT execution lock - concurrent runs possible")
            await _run_orchestrator(args, execution_id)

    except Exception as e:
        logger.error(f"Orchestrator failed with error: {e}", exc_info=True)
        sys.exit(1)


async def _run_orchestrator(args, execution_id: str):
    """Internal orchestrator logic (separated for lock management)."""
    logger.info(f"Orchestrator run started [execution_id={execution_id}]")

    # Filter by bank symbol if specified
    bank_symbols = [args.bank_symbol] if args.bank_symbol else None

    # Initialize collections
    gaps = []
    scheduled_jobs = []
    existing_reports = set()
    existing_cross_bank_reports = set()
    availability = []

    # =========================================================================
    # PART 1: Gap-based ETLs (call_summary, key_themes)
    # =========================================================================
    # Only run gap-based processing if:
    # - No ETL filter specified (run all), OR
    # - ETL filter is a gap-based ETL
    should_run_gap_based = args.etl_type is None or not ETL_CONFIGS.get(args.etl_type, {}).get(
        "schedule_driven", False
    )

    if should_run_gap_based:
        logger.info("Processing gap-based ETLs...")

        # Get data availability
        availability = await get_data_availability(bank_symbols, from_year=args.from_year)

        if availability:
            # Get existing per-bank reports
            existing_reports = await get_existing_reports(bank_symbols)

            # Identify gaps (loads institutions per ETL type internally)
            gaps = identify_gaps(availability, existing_reports, args.etl_type)
        else:
            logger.info("No transcript data available for gap-based ETLs")

    # =========================================================================
    # PART 1B: Two-phase ETLs (bank_earnings_report)
    # =========================================================================
    # Bank earnings report has special two-phase logic:
    # - Phase 1: RTS + Supplementary available, no existing report
    # - Phase 2: Transcript available, report exists without transcript (regenerate)
    bank_earnings_phase1 = []
    bank_earnings_phase2 = []

    should_run_bank_earnings = args.etl_type is None or args.etl_type == "bank_earnings_report"

    if should_run_bank_earnings:
        logger.info("Processing bank earnings report ETL (two-phase)...")

        bank_earnings_phase1, bank_earnings_phase2 = await identify_bank_earnings_gaps(
            bank_symbols, from_year=args.from_year
        )

        # Add to gaps list for unified execution
        gaps.extend(bank_earnings_phase1)
        gaps.extend(bank_earnings_phase2)

    # =========================================================================
    # PART 2: Schedule-based ETLs (cm_readthrough)
    # =========================================================================
    # Only run schedule-based processing if:
    # - No ETL filter specified (run all), OR
    # - ETL filter is a schedule-driven ETL
    should_run_scheduled = args.etl_type is None or ETL_CONFIGS.get(args.etl_type, {}).get(
        "schedule_driven", False
    )

    if should_run_scheduled:
        logger.info("Processing schedule-based ETLs...")

        try:
            # Load quarterly schedule
            schedule = load_quarterly_schedule()

            # Get existing cross-bank reports
            existing_cross_bank_reports = await get_existing_cross_bank_reports()

            # Check which scheduled ETLs should run
            scheduled_jobs = check_scheduled_etls(
                schedule=schedule,
                existing_cross_bank_reports=existing_cross_bank_reports,
                force=args.force,
                etl_filter=args.etl_type,
            )
        except FileNotFoundError as e:
            logger.warning(f"Quarterly schedule not found: {e}")
        except Exception as e:
            logger.error(f"Error processing schedule: {e}")

    # =========================================================================
    # Print summary of what needs to be done
    # =========================================================================
    print_summary(
        availability=availability,
        existing_reports=existing_reports,
        gaps=gaps,
        scheduled_jobs=scheduled_jobs,
        existing_cross_bank_reports=existing_cross_bank_reports,
        bank_earnings_phase1=bank_earnings_phase1,
        bank_earnings_phase2=bank_earnings_phase2,
    )

    # Check if there's anything to do
    total_jobs = len(gaps) + len(scheduled_jobs)
    if total_jobs == 0:
        logger.info("No jobs to execute - all reports up to date")
        return

    if args.dry_run:
        logger.info("DRY RUN MODE - Skipping execution")
        return

    # =========================================================================
    # Execute all ETLs
    # =========================================================================
    all_results = []
    start_time = time.time()

    # Execute gap-based ETLs in parallel
    if gaps:
        logger.info(f"Executing {len(gaps)} gap-based ETL jobs...")
        gap_summary = await execute_etls_parallel(
            gaps, dry_run=args.dry_run, max_parallel=args.max_parallel, verbose=args.verbose
        )
        all_results.extend(gap_summary.get("results", []))

    # Execute scheduled ETLs sequentially (they're typically expensive cross-bank operations)
    if scheduled_jobs:
        logger.info(f"Executing {len(scheduled_jobs)} scheduled ETL jobs...")
        for job in scheduled_jobs:
            result = await run_scheduled_etl_with_retry(
                etl_type=job["etl_type"],
                fiscal_year=job["fiscal_year"],
                quarter=job["quarter"],
                use_latest=job.get("use_latest", True),
                dry_run=args.dry_run,
                verbose=args.verbose,
            )
            all_results.append(result)

    # Calculate combined summary
    total_duration = time.time() - start_time
    successful = sum(1 for r in all_results if r.get("success"))
    failed = len(all_results) - successful

    execution_summary = {
        "total_jobs": len(all_results),
        "successful": successful,
        "failed": failed,
        "duration": total_duration,
        "results": all_results,
    }

    # Print final summary
    print_summary(
        availability=availability,
        existing_reports=existing_reports,
        gaps=gaps,
        scheduled_jobs=scheduled_jobs,
        existing_cross_bank_reports=existing_cross_bank_reports,
        bank_earnings_phase1=bank_earnings_phase1,
        bank_earnings_phase2=bank_earnings_phase2,
        execution_summary=execution_summary,
    )

    # Exit with error code if any failures
    if failed > 0:
        logger.warning(f"{failed} jobs failed")
        sys.exit(1)
    else:
        logger.info("All jobs completed successfully")


if __name__ == "__main__":
    asyncio.run(main())
