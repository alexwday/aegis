"""
ETL Orchestrator - Automated Report Generation Scheduler

This script monitors the aegis_data_availability table and automatically generates
missing reports for monitored institutions. Designed to run every 15 minutes to
catch new transcript data and generate Call Summary and Key Themes reports.

Features:
- Parallel ETL execution across banks (sequential within each ETL)
- Exponential backoff retry on failures
- Execution lock to prevent concurrent runs
- Gap detection between availability and existing reports
- Only processes Canadian and US banks from monitored_institutions.yaml

Usage:
    python scripts/etl_orchestrator.py                    # Full run
    python scripts/etl_orchestrator.py --dry-run          # Preview only
    python scripts/etl_orchestrator.py --bank-symbol RY   # Specific bank
    python scripts/etl_orchestrator.py --etl-type call_summary  # Specific ETL
"""

import argparse
import asyncio
import sys
import os
import time
import yaml
import subprocess
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple, Set
from sqlalchemy import text
from pathlib import Path
import uuid
import json
import fcntl

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger
from aegis.utils.settings import config

# Initialize logging
setup_logging()
logger = get_logger()

# Configuration
MONITORED_INSTITUTIONS_PATH = "src/aegis/etls/call_summary/config/monitored_institutions.yaml"
LOCK_FILE_PATH = "/tmp/aegis_etl_orchestrator.lock"
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 5  # seconds
MAX_RETRY_DELAY = 300  # 5 minutes
MAX_PARALLEL_ETLS = 4  # Number of banks to process in parallel

# ETL configurations
ETL_CONFIGS = {
    "call_summary": {
        "module": "aegis.etls.call_summary.main",
        "report_type": "call_summary",
        "description": "Earnings call summary by category",
    },
    "key_themes": {
        "module": "aegis.etls.key_themes.main",
        "report_type": "key_themes",
        "description": "Q&A themes extraction and grouping",
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
        self.file_handle = open(self.lock_file, "w")
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


def load_monitored_institutions() -> Dict[str, Dict[str, Any]]:
    """
    Load monitored institutions from YAML file.

    Returns:
        Dictionary mapping database bank_symbol (without country suffix) to bank info
        Example: {"RY": {"id": 1, "name": "Royal Bank of Canada", "yaml_key": "RY-CA", ...}}
    """
    logger.info(f"Loading monitored institutions from {MONITORED_INSTITUTIONS_PATH}")

    yaml_path = Path(MONITORED_INSTITUTIONS_PATH)
    if not yaml_path.exists():
        raise FileNotFoundError(f"Monitored institutions file not found: {yaml_path}")

    with open(yaml_path, "r") as f:
        yaml_data = yaml.safe_load(f)

    # Use YAML keys directly (RY-CA, BMO-CA, etc.) as they match database bank_symbol
    institutions = {}
    for yaml_key, data in yaml_data.items():
        institutions[yaml_key] = {
            **data,
            "yaml_key": yaml_key,
            "db_symbol": yaml_key.split("-")[0],  # Keep short symbol for reference
        }

    logger.info(f"Loaded {len(institutions)} monitored institutions")
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
) -> Set[Tuple[int, int, str, str]]:
    """
    Query aegis_reports table for existing reports.

    Args:
        bank_symbols: Optional list to filter by specific bank symbols

    Returns:
        Set of tuples: (bank_id, fiscal_year, quarter, report_type)
    """
    logger.info("Querying existing reports...")

    query = """
        SELECT
            bank_id,
            fiscal_year,
            quarter,
            report_type
        FROM aegis_reports
        WHERE report_type IN ('call_summary', 'key_themes')
    """

    params = {}
    if bank_symbols:
        query += " AND bank_symbol = ANY(:symbols)"
        params["symbols"] = bank_symbols

    async with get_connection() as conn:
        result = await conn.execute(text(query), params)
        rows = result.fetchall()

    existing = {(row.bank_id, row.fiscal_year, row.quarter, row.report_type) for row in rows}

    logger.info(f"Found {len(existing)} existing reports")
    return existing


def identify_gaps(
    availability: List[Dict[str, Any]],
    existing_reports: Set[Tuple[int, int, str, str]],
    institutions: Dict[str, Dict[str, Any]],
    etl_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Identify missing reports by comparing availability to existing reports.

    Args:
        availability: List of available data from aegis_data_availability
        existing_reports: Set of existing reports from aegis_reports
        institutions: Monitored institutions metadata
        etl_filter: Optional filter for specific ETL type

    Returns:
        List of ETL jobs to execute with metadata
    """
    logger.info("Identifying gaps between availability and reports...")

    gaps = []
    etl_types = [etl_filter] if etl_filter else list(ETL_CONFIGS.keys())

    for data in availability:
        bank_id = data["bank_id"]
        bank_symbol = data["bank_symbol"]
        fiscal_year = data["fiscal_year"]
        quarter = data["quarter"]

        # Find institution metadata
        institution = institutions.get(bank_symbol)
        if not institution:
            logger.warning(f"Bank {bank_symbol} not in monitored institutions - skipping")
            continue

        # Check each ETL type
        for etl_type in etl_types:
            report_type = ETL_CONFIGS[etl_type]["report_type"]

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

    logger.info(f"Identified {len(gaps)} missing reports")
    return gaps


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
    institutions: Dict[str, Dict[str, Any]],
    availability: List[Dict[str, Any]],
    existing_reports: Set[Tuple[int, int, str, str]],
    gaps: List[Dict[str, Any]],
    execution_summary: Optional[Dict[str, Any]] = None,
):
    """Print execution summary to console."""
    print("\n" + "=" * 80)
    print("ETL ORCHESTRATOR EXECUTION SUMMARY")
    print("=" * 80)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Monitored Institutions: {len(institutions)} banks")
    print(f"Data Availability: {len(availability)} bank-period combinations with transcripts")
    print(f"Existing Reports: {len(existing_reports)} reports")
    print()

    if gaps:
        print(f"📋 GAPS IDENTIFIED: {len(gaps)} missing reports")
        print("-" * 80)

        # Group by bank for display
        by_bank = {}
        for gap in gaps:
            key = f"{gap['bank_name']} ({gap['bank_symbol']})"
            if key not in by_bank:
                by_bank[key] = []
            by_bank[key].append(f"{gap['fiscal_year']} {gap['quarter']}: {gap['etl_type']}")

        for bank, jobs in sorted(by_bank.items()):
            print(f"\n{bank}:")
            for job in jobs:
                print(f"  • {job}")
        print()
    else:
        print("✅ NO GAPS FOUND - All reports up to date!")
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
                print(
                    f"  ❌ {failure['bank_symbol']} {failure['fiscal_year']} {failure['quarter']} "
                    f"[{failure['etl_type']}]: {error_preview}"
                )
        print()

    print("=" * 80 + "\n")


async def main():
    """Main orchestrator entry point."""
    parser = argparse.ArgumentParser(
        description="ETL Orchestrator - Automated report generation for monitored banks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full run - process all gaps
  python scripts/etl_orchestrator.py

  # Dry run - preview gaps without executing
  python scripts/etl_orchestrator.py --dry-run

  # Process only 2024 and later (skip historical data)
  python scripts/etl_orchestrator.py --from-year 2024

  # Process specific bank
  python scripts/etl_orchestrator.py --bank-symbol RY-CA

  # Process only call summaries from 2025 forward
  python scripts/etl_orchestrator.py --etl-type call_summary --from-year 2025

  # No lock (for testing - allows concurrent runs)
  python scripts/etl_orchestrator.py --no-lock
        """,
    )

    parser.add_argument(
        "--dry-run", action="store_true", help="Preview gaps without executing ETLs"
    )

    parser.add_argument("--bank-symbol", help="Process specific bank only (e.g., RY-CA, JPM-US)")

    parser.add_argument(
        "--etl-type", choices=["call_summary", "key_themes"], help="Process specific ETL type only"
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
        "--from-year", type=int, help="Only process data from this fiscal year forward (e.g., 2024)"
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Stream subprocess output in real-time (useful for debugging)",
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
    # Load monitored institutions
    institutions = load_monitored_institutions()

    # Filter by bank symbol if specified
    bank_symbols = [args.bank_symbol] if args.bank_symbol else None

    # Get data availability
    availability = await get_data_availability(bank_symbols, from_year=args.from_year)

    if not availability:
        logger.info("No transcript data available - nothing to process")
        print_summary(institutions, availability, set(), [])
        return

    # Get existing reports
    existing_reports = await get_existing_reports(bank_symbols)

    # Identify gaps
    gaps = identify_gaps(availability, existing_reports, institutions, args.etl_type)

    # Print preview
    print_summary(institutions, availability, existing_reports, gaps)

    if not gaps:
        logger.info("No gaps identified - all reports up to date")
        return

    if args.dry_run:
        logger.info("DRY RUN MODE - Skipping execution")
        return

    # Execute ETLs
    logger.info(f"Executing {len(gaps)} ETL jobs...")
    execution_summary = await execute_etls_parallel(
        gaps, dry_run=args.dry_run, max_parallel=args.max_parallel, verbose=args.verbose
    )

    # Print final summary
    print_summary(institutions, availability, existing_reports, gaps, execution_summary)

    # Exit with error code if any failures
    if execution_summary["failed"] > 0:
        logger.warning(f"{execution_summary['failed']} jobs failed")
        sys.exit(1)
    else:
        logger.info("All jobs completed successfully")


if __name__ == "__main__":
    asyncio.run(main())
