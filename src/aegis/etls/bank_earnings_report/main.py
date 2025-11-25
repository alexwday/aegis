"""
Bank Earnings Report ETL Script.

Generates quarterly earnings reports by retrieving data from multiple sources
and rendering to HTML via Jinja2 template.

Usage:
    python -m aegis.etls.bank_earnings_report.main --bank "Royal Bank of Canada" --year 2024 --quarter Q3
    python -m aegis.etls.bank_earnings_report.main --bank RY --year 2024 --quarter Q3
    python -m aegis.etls.bank_earnings_report.main --bank 1 --year 2024 --quarter Q3
"""

import argparse
import asyncio
import json
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import yaml
from jinja2 import Environment, FileSystemLoader
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from aegis.connections.oauth_connector import setup_authentication
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import get_logger, setup_logging
from aegis.utils.settings import config
from aegis.utils.ssl import setup_ssl
from aegis.utils.sql_prompt import postgresql_prompts

setup_logging()
logger = get_logger()


# =============================================================================
# Configuration
# =============================================================================


class ETLConfig:
    """
    ETL configuration loader that reads YAML configs and resolves model references.
    """

    def __init__(self, config_path: str):
        """Initialize the ETL configuration loader."""
        self.config_path = config_path
        self._config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def get_model(self, model_key: str) -> str:
        """
        Get the actual model name for a given model key.

        Resolves model tier references (small/medium/large) to actual model names
        from the global settings configuration.
        """
        if "models" not in self._config or model_key not in self._config["models"]:
            raise KeyError(f"Model key '{model_key}' not found in configuration")

        tier = self._config["models"][model_key].get("tier")
        if not tier:
            raise ValueError(f"No tier specified for model '{model_key}'")

        tier_map = {
            "small": config.llm.small.model,
            "medium": config.llm.medium.model,
            "large": config.llm.large.model,
        }

        if tier not in tier_map:
            raise ValueError(
                f"Invalid tier '{tier}' for model '{model_key}'. "
                f"Valid tiers: {list(tier_map.keys())}"
            )

        return tier_map[tier]

    @property
    def temperature(self) -> float:
        """Get the LLM temperature parameter."""
        return self._config.get("llm", {}).get("temperature", 0.1)

    @property
    def max_tokens(self) -> int:
        """Get the LLM max_tokens parameter."""
        return self._config.get("llm", {}).get("max_tokens", 16384)


etl_config = ETLConfig(os.path.join(os.path.dirname(__file__), "config", "config.yaml"))


# =============================================================================
# Monitored Institutions (shared with call_summary)
# =============================================================================

_MONITORED_INSTITUTIONS = None


def _load_monitored_institutions() -> Dict[int, Dict[str, Any]]:
    """
    Load and cache monitored institutions configuration (Canadian banks only).

    Returns:
        Dictionary mapping bank_id to institution details
    """
    global _MONITORED_INSTITUTIONS
    if _MONITORED_INSTITUTIONS is None:
        config_path = os.path.join(
            os.path.dirname(__file__),
            "config",
            "monitored_institutions.yaml",
        )
        with open(config_path, "r", encoding="utf-8") as f:
            yaml_data = yaml.safe_load(f)

        _MONITORED_INSTITUTIONS = {}
        for key, value in yaml_data.items():
            symbol = key.split("-")[0]
            _MONITORED_INSTITUTIONS[value["id"]] = {**value, "symbol": symbol}
    return _MONITORED_INSTITUTIONS


def get_bank_info_from_config(bank_identifier: str) -> Dict[str, Any]:
    """
    Look up bank from monitored institutions configuration file.

    Args:
        bank_identifier: Bank ID (as string/int), symbol (e.g., "RY"), or name

    Returns:
        Dictionary with bank_id, bank_name, bank_symbol, bank_type

    Raises:
        ValueError: If bank not found in monitored institutions
    """
    institutions = _load_monitored_institutions()

    # Try lookup by ID
    if bank_identifier.isdigit():
        bank_id = int(bank_identifier)
        if bank_id in institutions:
            inst = institutions[bank_id]
            return {
                "bank_id": inst["id"],
                "bank_name": inst["name"],
                "bank_symbol": inst["symbol"],
                "bank_type": inst["type"],
            }

    # Try lookup by symbol or name
    bank_identifier_upper = bank_identifier.upper()
    bank_identifier_lower = bank_identifier.lower()

    for inst in institutions.values():
        if inst["symbol"].upper() == bank_identifier_upper:
            return {
                "bank_id": inst["id"],
                "bank_name": inst["name"],
                "bank_symbol": inst["symbol"],
                "bank_type": inst["type"],
            }

        if bank_identifier_lower in inst["name"].lower():
            return {
                "bank_id": inst["id"],
                "bank_name": inst["name"],
                "bank_symbol": inst["symbol"],
                "bank_type": inst["type"],
            }

    available = [f"{inst['symbol']} ({inst['name']})" for inst in institutions.values()]
    raise ValueError(
        f"Bank '{bank_identifier}' not found in monitored institutions.\n"
        f"Available banks: {', '.join(sorted(available))}"
    )


# =============================================================================
# Data Availability Verification
# =============================================================================


async def verify_data_availability(
    bank_id: int, bank_name: str, fiscal_year: int, quarter: str
) -> Dict[str, bool]:
    """
    Verify which data sources are available for the specified bank and period.

    Args:
        bank_id: Bank ID
        bank_name: Bank name (for error messages)
        fiscal_year: Year (e.g., 2024)
        quarter: Quarter (e.g., "Q3")

    Returns:
        Dict mapping database names to availability (True/False)

    Raises:
        ValueError: If no data sources are available
    """
    async with get_connection() as conn:
        result = await conn.execute(
            text(
                """
                SELECT database_names
                FROM aegis_data_availability
                WHERE bank_id = :bank_id
                  AND fiscal_year = :fiscal_year
                  AND quarter = :quarter
                """
            ),
            {"bank_id": bank_id, "fiscal_year": fiscal_year, "quarter": quarter},
        )
        row = result.fetchone()

        if not row or not row[0]:
            raise ValueError(
                f"No data available for {bank_name} {quarter} {fiscal_year}"
            )

        available_dbs = row[0]
        availability = {
            "supplementary": "supplementary" in available_dbs,
            "pillar3": "pillar3" in available_dbs,
            "rts": "rts" in available_dbs,
            "transcripts": "transcripts" in available_dbs,
        }

        return availability


# =============================================================================
# Stage 2: Data Retrieval (Placeholder functions)
# =============================================================================


async def retrieve_supplementary_data(
    bank_id: int, fiscal_year: int, quarter: str, context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Retrieve supplementary pack data for financial metrics.

    TODO: Implement actual database retrieval.

    Args:
        bank_id: Bank ID
        fiscal_year: Fiscal year
        quarter: Quarter (Q1-Q4)
        context: Execution context

    Returns:
        Raw supplementary data dict
    """
    logger.info(
        "etl.bank_earnings_report.retrieve_supplementary",
        execution_id=context.get("execution_id"),
        bank_id=bank_id,
        period=f"{quarter} {fiscal_year}",
    )
    # TODO: Implement retrieval from aegis_supplementary table
    return {}


async def retrieve_pillar3_data(
    bank_id: int, fiscal_year: int, quarter: str, context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Retrieve Pillar 3 capital and risk metrics.

    TODO: Implement actual database retrieval.

    Args:
        bank_id: Bank ID
        fiscal_year: Fiscal year
        quarter: Quarter (Q1-Q4)
        context: Execution context

    Returns:
        Raw Pillar 3 data dict
    """
    logger.info(
        "etl.bank_earnings_report.retrieve_pillar3",
        execution_id=context.get("execution_id"),
        bank_id=bank_id,
        period=f"{quarter} {fiscal_year}",
    )
    # TODO: Implement retrieval from aegis_pillar3 table
    return {}


async def retrieve_rts_data(
    bank_id: int, fiscal_year: int, quarter: str, context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Retrieve RTS regulatory filing narratives.

    TODO: Implement actual database retrieval.

    Args:
        bank_id: Bank ID
        fiscal_year: Fiscal year
        quarter: Quarter (Q1-Q4)
        context: Execution context

    Returns:
        Raw RTS data dict
    """
    logger.info(
        "etl.bank_earnings_report.retrieve_rts",
        execution_id=context.get("execution_id"),
        bank_id=bank_id,
        period=f"{quarter} {fiscal_year}",
    )
    # TODO: Implement retrieval from aegis_rts table
    return {}


async def retrieve_transcript_data(
    bank_id: int, fiscal_year: int, quarter: str, context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Retrieve earnings call transcript data for quotes and Q&A.

    TODO: Implement actual database retrieval.

    Args:
        bank_id: Bank ID
        fiscal_year: Fiscal year
        quarter: Quarter (Q1-Q4)
        context: Execution context

    Returns:
        Raw transcript data dict
    """
    logger.info(
        "etl.bank_earnings_report.retrieve_transcripts",
        execution_id=context.get("execution_id"),
        bank_id=bank_id,
        period=f"{quarter} {fiscal_year}",
    )
    # TODO: Implement retrieval from aegis_transcripts table
    return {}


# =============================================================================
# Stage 3: Section Extraction (Placeholder functions)
# =============================================================================


def get_period_ending_date(fiscal_year: int, quarter: str) -> str:
    """
    Calculate the period ending date for Canadian banks (Oct 31 fiscal year end).

    For Canadian banks, the calendar year matches the fiscal year for all quarters:
    - Q1 ends January 31 of fiscal year
    - Q2 ends April 30 of fiscal year
    - Q3 ends July 31 of fiscal year
    - Q4 ends October 31 of fiscal year

    Args:
        fiscal_year: Fiscal year (e.g., 2024)
        quarter: Quarter (Q1-Q4)

    Returns:
        Formatted date string (e.g., "July 31, 2024")
    """
    period_endings = {
        "Q1": ("January", 31),
        "Q2": ("April", 30),
        "Q3": ("July", 31),
        "Q4": ("October", 31),
    }

    month, day = period_endings.get(quarter, ("", 0))
    if not month:
        return ""

    return f"{month} {day}, {fiscal_year}"


def extract_header_params(
    bank_info: Dict[str, Any], fiscal_year: int, quarter: str
) -> Dict[str, Any]:
    """
    Generate header parameters (no LLM needed - direct input).

    Args:
        bank_info: Bank information dict
        fiscal_year: Fiscal year
        quarter: Quarter

    Returns:
        Header params JSON structure
    """
    return {
        "bank_name": bank_info["bank_name"],
        "fiscal_quarter": quarter,
        "fiscal_year": str(fiscal_year),
        "period_ending": get_period_ending_date(fiscal_year, quarter),
        "currency": "CAD",
    }


async def extract_all_sections(
    bank_info: Dict[str, Any],
    fiscal_year: int,
    quarter: str,
    raw_data: Dict[str, Any],
    context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extract all JSON sections from raw data using LLM.

    TODO: Implement LLM-based extraction for each section.

    Args:
        bank_info: Bank information
        fiscal_year: Fiscal year
        quarter: Quarter
        raw_data: Combined raw data from all sources
        context: Execution context

    Returns:
        Dict mapping section names to extracted JSON data
    """
    execution_id = context.get("execution_id")

    logger.info(
        "etl.bank_earnings_report.extract_sections_start",
        execution_id=execution_id,
        bank=bank_info["bank_symbol"],
    )

    sections = {}

    # Section 0: Header (no LLM needed)
    sections["0_header_params"] = extract_header_params(
        bank_info, fiscal_year, quarter
    )

    # TODO: Implement LLM extraction for remaining sections:
    # sections["0_header_dividend"] = await extract_dividend(raw_data, context)
    # sections["1_keymetrics_overview"] = await extract_overview(raw_data, context)
    # sections["1_keymetrics_tiles"] = await extract_metrics_tiles(raw_data, context)
    # sections["1_keymetrics_chart"] = await extract_metrics_chart(raw_data, context)
    # sections["1_keymetrics_items"] = await extract_items_of_note(raw_data, context)
    # sections["2_narrative"] = await extract_narrative(raw_data, context)
    # sections["3_analyst_focus"] = await extract_analyst_qa(raw_data, context)
    # sections["4_segments"] = await extract_segments(raw_data, context)
    # sections["5_capital_risk"] = await extract_capital_risk(raw_data, context)

    logger.info(
        "etl.bank_earnings_report.extract_sections_complete",
        execution_id=execution_id,
        sections_extracted=len(sections),
    )

    return sections


# =============================================================================
# Stage 4: Template Rendering
# =============================================================================


def render_report(sections: Dict[str, Any], output_path: Path) -> Path:
    """
    Render the HTML report from extracted sections using Jinja2.

    Args:
        sections: Dict mapping section names to JSON data
        output_path: Path for the output HTML file

    Returns:
        Path to the rendered HTML file
    """
    base_dir = Path(__file__).parent
    template_path = base_dir / "report_template.html"

    # Prepare template data with underscore prefix for numeric keys
    template_data = {}
    for key, value in sections.items():
        if key[0].isdigit():
            template_data[f"_{key}"] = value
        else:
            template_data[key] = value

    # Render template
    env = Environment(loader=FileSystemLoader(base_dir))
    template = env.get_template(template_path.name)
    rendered = template.render(**template_data)

    # Write output
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(rendered)

    return output_path


# =============================================================================
# Stage 5: Database Storage
# =============================================================================


async def save_to_database(
    bank_info: Dict[str, Any],
    fiscal_year: int,
    quarter: str,
    filepath: Path,
    execution_id: str,
) -> None:
    """
    Save report metadata to aegis_reports table.

    Args:
        bank_info: Bank information
        fiscal_year: Fiscal year
        quarter: Quarter
        filepath: Path to generated report
        execution_id: Execution ID for tracking
    """
    generation_timestamp = datetime.now()

    try:
        async with get_connection() as conn:
            # Delete existing report for same bank/period/type
            await conn.execute(
                text(
                    """
                    DELETE FROM aegis_reports
                    WHERE bank_id = :bank_id
                      AND fiscal_year = :fiscal_year
                      AND quarter = :quarter
                      AND report_type = :report_type
                    """
                ),
                {
                    "bank_id": bank_info["bank_id"],
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                    "report_type": "bank_earnings_report",
                },
            )

            # Insert new report
            await conn.execute(
                text(
                    """
                    INSERT INTO aegis_reports (
                        report_name,
                        report_description,
                        report_type,
                        bank_id,
                        bank_name,
                        bank_symbol,
                        fiscal_year,
                        quarter,
                        local_filepath,
                        generation_date,
                        generated_by,
                        execution_id,
                        metadata
                    ) VALUES (
                        :report_name,
                        :report_description,
                        :report_type,
                        :bank_id,
                        :bank_name,
                        :bank_symbol,
                        :fiscal_year,
                        :quarter,
                        :local_filepath,
                        :generation_date,
                        :generated_by,
                        :execution_id,
                        :metadata
                    )
                    """
                ),
                {
                    "report_name": f"{bank_info['bank_symbol']} {quarter} {fiscal_year} Earnings Report",
                    "report_description": "Quarterly earnings report with key metrics, narratives, and analysis",
                    "report_type": "bank_earnings_report",
                    "bank_id": bank_info["bank_id"],
                    "bank_name": bank_info["bank_name"],
                    "bank_symbol": bank_info["bank_symbol"],
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                    "local_filepath": str(filepath),
                    "generation_date": generation_timestamp,
                    "generated_by": "bank_earnings_report_etl",
                    "execution_id": execution_id,
                    "metadata": json.dumps({"format": "html"}),
                },
            )

            await conn.commit()

            logger.info(
                "etl.bank_earnings_report.saved_to_db",
                execution_id=execution_id,
                bank=bank_info["bank_symbol"],
            )

    except SQLAlchemyError as e:
        logger.error(
            "etl.bank_earnings_report.db_error",
            execution_id=execution_id,
            error=str(e),
        )
        raise


# =============================================================================
# Main ETL Orchestrator
# =============================================================================


async def generate_bank_earnings_report(
    bank_name: str, fiscal_year: int, quarter: str
) -> str:
    """
    Generate a bank earnings report.

    Args:
        bank_name: ID, name, or symbol of the bank
        fiscal_year: Year (e.g., 2024)
        quarter: Quarter (e.g., "Q3")

    Returns:
        Success/error message string
    """
    execution_id = str(uuid.uuid4())
    logger.info(
        "etl.bank_earnings_report.started",
        execution_id=execution_id,
        bank_name=bank_name,
        fiscal_year=fiscal_year,
        quarter=quarter,
    )

    try:
        # =================================================================
        # Stage 1: Setup & Validation
        # =================================================================
        bank_info = get_bank_info_from_config(bank_name)

        availability = await verify_data_availability(
            bank_info["bank_id"], bank_info["bank_name"], fiscal_year, quarter
        )

        logger.info(
            "etl.bank_earnings_report.data_availability",
            execution_id=execution_id,
            availability=availability,
        )

        ssl_config = setup_ssl()
        auth_config = await setup_authentication(execution_id, ssl_config)

        if not auth_config["success"]:
            error_msg = f"Authentication failed: {auth_config.get('error', 'Unknown error')}"
            logger.error(
                "etl.bank_earnings_report.auth_failed",
                execution_id=execution_id,
                error=error_msg,
            )
            raise RuntimeError(error_msg)

        context = {
            "execution_id": execution_id,
            "auth_config": auth_config,
            "ssl_config": ssl_config,
        }

        # =================================================================
        # Stage 2: Data Retrieval (Parallel)
        # =================================================================
        logger.info(
            "etl.bank_earnings_report.retrieval_start",
            execution_id=execution_id,
        )

        # Retrieve all data sources in parallel
        supplementary_task = retrieve_supplementary_data(
            bank_info["bank_id"], fiscal_year, quarter, context
        )
        pillar3_task = retrieve_pillar3_data(
            bank_info["bank_id"], fiscal_year, quarter, context
        )
        rts_task = retrieve_rts_data(
            bank_info["bank_id"], fiscal_year, quarter, context
        )
        transcript_task = retrieve_transcript_data(
            bank_info["bank_id"], fiscal_year, quarter, context
        )

        supplementary_data, pillar3_data, rts_data, transcript_data = await asyncio.gather(
            supplementary_task, pillar3_task, rts_task, transcript_task
        )

        raw_data = {
            "supplementary": supplementary_data,
            "pillar3": pillar3_data,
            "rts": rts_data,
            "transcripts": transcript_data,
        }

        logger.info(
            "etl.bank_earnings_report.retrieval_complete",
            execution_id=execution_id,
        )

        # =================================================================
        # Stage 3: Section Extraction
        # =================================================================
        sections = await extract_all_sections(
            bank_info, fiscal_year, quarter, raw_data, context
        )

        # =================================================================
        # Stage 4: Template Rendering
        # =================================================================
        output_dir = Path(__file__).parent / "output"
        output_dir.mkdir(exist_ok=True)

        filename = f"{bank_info['bank_symbol']}_{fiscal_year}_{quarter}.html"
        output_path = output_dir / filename

        render_report(sections, output_path)

        logger.info(
            "etl.bank_earnings_report.rendered",
            execution_id=execution_id,
            output_path=str(output_path),
        )

        # =================================================================
        # Stage 5: Database Storage
        # =================================================================
        await save_to_database(
            bank_info, fiscal_year, quarter, output_path, execution_id
        )

        logger.info(
            "etl.bank_earnings_report.completed",
            execution_id=execution_id,
        )

        return f"✅ Complete: {output_path}"

    except ValueError as e:
        logger.error(
            "etl.bank_earnings_report.error",
            execution_id=execution_id,
            error=str(e),
        )
        return f"⚠️ {str(e)}"
    except Exception as e:
        error_msg = f"Error generating report: {str(e)}"
        logger.error(
            "etl.bank_earnings_report.error",
            execution_id=execution_id,
            error=error_msg,
            exc_info=True,
        )
        return f"❌ {error_msg}"


# =============================================================================
# CLI Entry Point
# =============================================================================


def main():
    """Main entry point for command-line execution."""
    parser = argparse.ArgumentParser(
        description="Generate bank earnings reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--bank", required=True, help="Bank ID, name, or symbol")
    parser.add_argument("--year", type=int, required=True, help="Fiscal year")
    parser.add_argument(
        "--quarter", required=True, choices=["Q1", "Q2", "Q3", "Q4"], help="Quarter"
    )

    args = parser.parse_args()

    # Load prompts from database
    postgresql_prompts()

    print(f"\n🔄 Generating report for {args.bank} {args.quarter} {args.year}...\n")

    result = asyncio.run(
        generate_bank_earnings_report(
            bank_name=args.bank, fiscal_year=args.year, quarter=args.quarter
        )
    )

    is_success = isinstance(result, str) and result.strip().startswith("✅")
    output_stream = sys.stdout if is_success else sys.stderr
    print(result, file=output_stream)

    if not is_success:
        sys.exit(1)


if __name__ == "__main__":
    main()
