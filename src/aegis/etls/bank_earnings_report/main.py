"""
Bank Earnings Report ETL Script.

Generates quarterly earnings reports by retrieving data from multiple sources
and rendering to HTML via Jinja2 template.

Usage:
    python -m aegis.etls.bank_earnings_report.main --bank "Royal Bank" --year 2024 --quarter Q3
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
from aegis.utils.ssl import setup_ssl
from aegis.utils.sql_prompt import postgresql_prompts

setup_logging()
logger = get_logger()

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
            raise ValueError(f"No data available for {bank_name} {quarter} {fiscal_year}")

        available_dbs = row[0]
        availability = {
            "supplementary": "supplementary" in available_dbs,
            "pillar3": "pillar3" in available_dbs,
            "rts": "rts" in available_dbs,
            "transcripts": "transcripts" in available_dbs,
        }

        return availability


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
    return {}


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
    Extract all JSON sections for the earnings report.

    Args:
        bank_info: Bank information
        fiscal_year: Fiscal year
        quarter: Quarter
        raw_data: Combined raw data from all sources (currently unused)
        context: Execution context

    Returns:
        Dict mapping section names to extracted JSON data
    """
    from .retrieval.supplementary import (
        retrieve_dividend,
        format_dividend_json,
        retrieve_all_metrics,
        retrieve_metrics_by_names,
        format_key_metrics_json,
        retrieve_metric_history,
        format_multi_chart_json,
        retrieve_available_platforms,
        retrieve_segment_metrics,
        format_segment_json,
        retrieve_all_metrics_with_history,
        format_raw_metrics_table,
        retrieve_segment_metrics_with_history,
        format_segment_raw_table,
    )
    from .extraction.key_metrics import select_chart_and_tile_metrics, KEY_METRICS
    from .extraction.segment_metrics import (
        select_top_segment_metrics,
        MONITORED_PLATFORMS,
        CORE_SEGMENT_METRICS,
        DEFAULT_CORE_METRICS,
    )
    from .extraction.analyst_focus import extract_analyst_focus
    from .extraction.management_narrative import extract_transcript_quotes
    from .extraction.transcript_insights import (
        extract_transcript_overview,
        extract_transcript_items_of_note,
    )
    from .retrieval.rts import get_all_segment_drivers_from_rts

    execution_id = context.get("execution_id")
    db_symbol = f"{bank_info['bank_symbol']}-CA"

    logger.info(
        "etl.bank_earnings_report.extract_sections_start",
        execution_id=execution_id,
        bank=bank_info["bank_symbol"],
    )

    sections = {}

    sections["0_header_params"] = extract_header_params(bank_info, fiscal_year, quarter)
    logger.info("etl.bank_earnings_report.section_complete", section="0_header_params")

    dividend_data = await retrieve_dividend(db_symbol, fiscal_year, quarter, context)
    sections["0_header_dividend"] = format_dividend_json(dividend_data)
    logger.info("etl.bank_earnings_report.section_complete", section="0_header_dividend")

    all_metrics = await retrieve_all_metrics(db_symbol, fiscal_year, quarter, context)

    llm_debug_log = {
        "execution_id": str(execution_id),
        "bank": bank_info["bank_name"],
        "period": f"{quarter} {fiscal_year}",
        "sections": {},
    }

    available_metric_names = {m["parameter"] for m in all_metrics} if all_metrics else set()
    found_key_metrics = [m for m in KEY_METRICS if m in available_metric_names]
    missing_key_metrics = [m for m in KEY_METRICS if m not in available_metric_names]

    logger.info(
        "etl.bank_earnings_report.key_metrics_availability",
        execution_id=execution_id,
        found=len(found_key_metrics),
        total=len(KEY_METRICS),
        found_metrics=found_key_metrics,
        missing_metrics=missing_key_metrics,
    )

    if all_metrics:
        selection_result = await select_chart_and_tile_metrics(
            metrics=all_metrics,
            bank_name=bank_info["bank_name"],
            quarter=quarter,
            fiscal_year=fiscal_year,
            context=context,
            num_tile_metrics=6,
            num_dynamic_metrics=5,
        )

        llm_debug_log["sections"]["1_keymetrics_selection"] = {
            "available_metrics": selection_result.get("available_metrics", 0),
            "available_chartable": selection_result.get("available_chartable", []),
            "chart_metric": selection_result.get("chart_metric", ""),
            "chart_reasoning": selection_result.get("chart_reasoning", ""),
            "tile_metrics": selection_result.get("tile_metrics", []),
            "tile_reasoning": selection_result.get("tile_reasoning", ""),
            "dynamic_metrics": selection_result.get("dynamic_metrics", []),
            "dynamic_reasoning": selection_result.get("dynamic_reasoning", ""),
            "all_metrics_summary": selection_result.get("all_metrics_summary", []),
        }

        tile_names = selection_result.get("tile_metrics", [])
        tile_metrics = await retrieve_metrics_by_names(
            db_symbol, fiscal_year, quarter, tile_names, context
        )
        tiles_json = format_key_metrics_json(tile_metrics)
        sections["1_keymetrics_tiles"] = tiles_json

        llm_debug_log["sections"]["1_keymetrics_tiles"] = {
            "requested_metrics": tile_names,
            "retrieved_count": len(tile_metrics),
            "formatted_metrics": tiles_json.get("metrics", []),
        }

        dynamic_names = selection_result.get("dynamic_metrics", [])
        if dynamic_names:
            dynamic_metrics = await retrieve_metrics_by_names(
                db_symbol, fiscal_year, quarter, dynamic_names, context
            )
            dynamic_json = format_key_metrics_json(dynamic_metrics)
            sections["1_keymetrics_dynamic"] = dynamic_json

            llm_debug_log["sections"]["1_keymetrics_dynamic"] = {
                "requested_metrics": dynamic_names,
                "retrieved_count": len(dynamic_metrics),
                "formatted_metrics": dynamic_json.get("metrics", []),
            }
        else:
            sections["1_keymetrics_dynamic"] = {"source": "Supp Pack", "metrics": []}
            llm_debug_log["sections"]["1_keymetrics_dynamic"] = {
                "requested_metrics": [],
                "retrieved_count": 0,
                "formatted_metrics": [],
            }

        chart_metric_name = selection_result.get("chart_metric", "")
        all_chart_metrics = []
        seen_metrics = set()

        if chart_metric_name and chart_metric_name not in seen_metrics:
            all_chart_metrics.append(chart_metric_name)
            seen_metrics.add(chart_metric_name)

        for name in tile_names:
            if name not in seen_metrics:
                all_chart_metrics.append(name)
                seen_metrics.add(name)

        for name in dynamic_names:
            if name not in seen_metrics:
                all_chart_metrics.append(name)
                seen_metrics.add(name)

        metrics_with_history = []
        metrics_by_param = {m["parameter"]: m for m in all_metrics}

        for metric_name in all_chart_metrics:
            metric_data = metrics_by_param.get(metric_name)
            if not metric_data:
                continue

            history = await retrieve_metric_history(
                bank_symbol=db_symbol,
                metric_name=metric_name,
                fiscal_year=fiscal_year,
                quarter=quarter,
                context=context,
                num_quarters=8,
            )

            if history:
                metrics_with_history.append(
                    {
                        "name": metric_name,
                        "history": history,
                        "is_bps": metric_data.get("is_bps", False),
                    }
                )

        if metrics_with_history:
            chart_json = format_multi_chart_json(metrics_with_history, chart_metric_name)
            sections["1_keymetrics_chart"] = chart_json

            llm_debug_log["sections"]["1_keymetrics_chart"] = {
                "initial_metric": chart_metric_name,
                "total_metrics": len(metrics_with_history),
                "metric_names": [m["name"] for m in metrics_with_history],
                "initial_index": chart_json.get("initial_index", 0),
            }
        else:
            sections["1_keymetrics_chart"] = {
                "initial_index": 0,
                "metrics": [],
            }
            llm_debug_log["sections"]["1_keymetrics_chart"] = {
                "initial_metric": "N/A",
                "reason": "No metrics with history available",
                "total_metrics": 0,
            }
    else:
        sections["1_keymetrics_tiles"] = {"source": "Supp Pack", "metrics": []}
        sections["1_keymetrics_dynamic"] = {"source": "Supp Pack", "metrics": []}
        sections["1_keymetrics_chart"] = {
            "initial_index": 0,
            "metrics": [],
        }
        llm_debug_log["sections"]["1_keymetrics_selection"] = {
            "available_metrics": 0,
            "chart_metric": "",
            "chart_reasoning": "No metrics available",
            "tile_metrics": [],
            "tile_reasoning": "No metrics available",
            "dynamic_metrics": [],
            "dynamic_reasoning": "No metrics available",
        }

    context["llm_debug_log"] = llm_debug_log

    logger.info("etl.bank_earnings_report.section_complete", section="1_keymetrics_tiles")
    logger.info("etl.bank_earnings_report.section_complete", section="1_keymetrics_chart")

    raw_metrics_with_history = await retrieve_all_metrics_with_history(
        bank_symbol=db_symbol,
        fiscal_year=fiscal_year,
        quarter=quarter,
        context=context,
        num_quarters=8,
    )

    if raw_metrics_with_history:
        raw_table = format_raw_metrics_table(raw_metrics_with_history)
        sections["1_keymetrics_raw"] = raw_table
        llm_debug_log["sections"]["1_keymetrics_raw"] = {
            "metric_count": len(raw_metrics_with_history),
            "quarters": 8,
        }
    else:
        sections["1_keymetrics_raw"] = {"headers": [], "rows": [], "tsv": ""}
        llm_debug_log["sections"]["1_keymetrics_raw"] = {
            "metric_count": 0,
            "reason": "No metrics with history available",
        }

    logger.info("etl.bank_earnings_report.section_complete", section="1_keymetrics_raw")

    logger.info(
        "etl.bank_earnings_report.transcript_overview_start",
        execution_id=execution_id,
    )

    transcript_overview = await extract_transcript_overview(
        bank_info=bank_info,
        fiscal_year=fiscal_year,
        quarter=quarter,
        context=context,
    )

    sections["1_keymetrics_overview"] = {
        "narrative": transcript_overview.get("narrative", ""),
    }

    llm_debug_log["sections"]["1_keymetrics_overview"] = {
        "source": "Transcript",
        "narrative_length": len(transcript_overview.get("narrative", "")),
    }

    logger.info(
        "etl.bank_earnings_report.section_complete",
        section="1_keymetrics_overview",
        narrative_length=len(transcript_overview.get("narrative", "")),
    )

    logger.info(
        "etl.bank_earnings_report.transcript_items_start",
        execution_id=execution_id,
    )

    transcript_items = await extract_transcript_items_of_note(
        bank_info=bank_info,
        fiscal_year=fiscal_year,
        quarter=quarter,
        context=context,
        max_items=10,
    )

    sections["1_keymetrics_items"] = {
        "source": "Transcript",
        "entries": transcript_items.get("items", []),
    }

    llm_debug_log["sections"]["1_keymetrics_items"] = {
        "source": "Transcript",
        "items_count": len(transcript_items.get("items", [])),
        "items": transcript_items.get("items", []),
        "extraction_notes": transcript_items.get("notes", ""),
    }

    logger.info(
        "etl.bank_earnings_report.section_complete",
        section="1_keymetrics_items",
        items_count=len(transcript_items.get("items", [])),
    )

    logger.info(
        "etl.bank_earnings_report.management_narrative_start",
        execution_id=execution_id,
    )

    transcript_quotes = await extract_transcript_quotes(
        bank_info=bank_info,
        fiscal_year=fiscal_year,
        quarter=quarter,
        context=context,
        num_quotes=5,
    )

    sections["2_narrative"] = {"entries": transcript_quotes}

    llm_debug_log["sections"]["2_narrative"] = {
        "transcript_quotes": len(transcript_quotes),
        "speakers": [q.get("speaker", "") for q in transcript_quotes],
    }

    logger.info(
        "etl.bank_earnings_report.section_complete",
        section="2_narrative",
        transcript_quotes=len(transcript_quotes),
    )

    logger.info(
        "etl.bank_earnings_report.analyst_focus_start",
        execution_id=execution_id,
    )

    analyst_focus_result = await extract_analyst_focus(
        bank_info=bank_info,
        fiscal_year=fiscal_year,
        quarter=quarter,
        context=context,
        max_entries=12,  # Extract more, then rank top 4
    )

    sections["3_analyst_focus"] = analyst_focus_result

    llm_debug_log["sections"]["3_analyst_focus"] = {
        "total_entries": len(analyst_focus_result.get("entries", [])),
        "featured_entries": len(analyst_focus_result.get("featured", [])),
        "themes": [e.get("theme", "") for e in analyst_focus_result.get("entries", [])],
    }

    logger.info(
        "etl.bank_earnings_report.section_complete",
        section="3_analyst_focus",
        entries=len(analyst_focus_result.get("entries", [])),
    )

    logger.info(
        "etl.bank_earnings_report.segments_start",
        execution_id=execution_id,
    )

    available_platforms = await retrieve_available_platforms(
        db_symbol, fiscal_year, quarter, context
    )

    found_platforms = [p for p in MONITORED_PLATFORMS if p in available_platforms]
    missing_platforms = [p for p in MONITORED_PLATFORMS if p not in available_platforms]

    logger.info(
        "etl.bank_earnings_report.platform_availability",
        execution_id=execution_id,
        found=len(found_platforms),
        total=len(MONITORED_PLATFORMS),
        found_platforms=found_platforms,
        missing_platforms=missing_platforms,
    )

    segment_entries = []
    segment_debug = {
        "available_platforms": available_platforms,
        "monitored_platforms": MONITORED_PLATFORMS,
        "matched_platforms": found_platforms,
        "missing_platforms": missing_platforms,
        "segment_selections": {},
    }

    all_segment_drivers = await get_all_segment_drivers_from_rts(
        bank=db_symbol,  # e.g., "RY-CA"
        year=fiscal_year,
        quarter=quarter,
        segment_names=found_platforms,
        context=context,
    )

    logger.info(
        "etl.bank_earnings_report.rts_drivers_retrieved",
        execution_id=execution_id,
        segments_with_drivers=sum(1 for v in all_segment_drivers.values() if v),
        total_segments=len(found_platforms),
    )

    for platform in found_platforms:
        logger.info(
            "etl.bank_earnings_report.segment_matched",
            execution_id=execution_id,
            platform=platform,
        )

        segment_metrics = await retrieve_segment_metrics(
            db_symbol, fiscal_year, quarter, platform, context
        )

        if segment_metrics:
            metrics_by_name = {m["parameter"]: m for m in segment_metrics}

            segment_core_metrics = CORE_SEGMENT_METRICS.get(platform, DEFAULT_CORE_METRICS)
            core_metrics_data = []
            for core_name in segment_core_metrics:
                if core_name in metrics_by_name:
                    core_metrics_data.append(metrics_by_name[core_name])

            selection = await select_top_segment_metrics(
                metrics=segment_metrics,
                segment_name=platform,
                bank_name=bank_info["bank_name"],
                quarter=quarter,
                fiscal_year=fiscal_year,
                context=context,
                num_metrics=3,
            )

            segment_debug["segment_selections"][platform] = {
                "available_metrics": len(segment_metrics),
                "core_metrics": [m["parameter"] for m in core_metrics_data],
                "selected_metrics": selection.get("selected_metrics", []),
                "reasoning": selection.get("reasoning", ""),
            }

            description = all_segment_drivers.get(platform, "")
            segment_debug["segment_selections"][platform]["rts_drivers"] = bool(description)

            segment_entry = format_segment_json(
                segment_name=platform,
                description=description,
                core_metrics=core_metrics_data,
                highlighted_metrics=selection.get("metrics_data", []),
            )

            segment_metrics_history = await retrieve_segment_metrics_with_history(
                bank_symbol=db_symbol,
                fiscal_year=fiscal_year,
                quarter=quarter,
                platform=platform,
                context=context,
                num_quarters=8,
            )
            if segment_metrics_history:
                segment_entry["raw_table"] = format_segment_raw_table(
                    segment_metrics_history, display_quarters=4
                )
            else:
                segment_entry["raw_table"] = {"headers": [], "rows": [], "tsv": ""}

            segment_entries.append(segment_entry)

            logger.info(
                "etl.bank_earnings_report.segment_complete",
                execution_id=execution_id,
                segment=platform,
                core_metrics=len(core_metrics_data),
                highlighted_metrics=len(selection.get("metrics_data", [])),
                raw_table_rows=len(segment_entry["raw_table"].get("rows", [])),
            )
        else:
            logger.warning(
                "etl.bank_earnings_report.segment_no_metrics",
                execution_id=execution_id,
                platform=platform,
            )

    sections["4_segments"] = {"entries": segment_entries}
    llm_debug_log["sections"]["4_segments"] = segment_debug

    logger.info(
        "etl.bank_earnings_report.segments_complete",
        execution_id=execution_id,
        segments_found=len(segment_entries),
    )

    sections["5_capital_risk"] = {
        "source": "Pillar 3",
        "regulatory_capital": [],
        "rwa": {"components": [], "total": ""},
        "liquidity_credit": [],
    }

    logger.info(
        "etl.bank_earnings_report.extract_sections_complete",
        execution_id=execution_id,
        sections_extracted=len(sections),
    )

    return sections


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
    template_path = base_dir / "templates" / "report_template.html"

    template_data = {}
    for key, value in sections.items():
        if key[0].isdigit():
            template_data[f"_{key}"] = value
        else:
            template_data[key] = value

    env = Environment(loader=FileSystemLoader(base_dir))
    template = env.get_template(template_path.name)
    rendered = template.render(**template_data)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(rendered)

    return output_path


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
                    "report_name": (
                        f"{bank_info['bank_symbol']} {quarter} {fiscal_year} Earnings Report"
                    ),
                    "report_description": (
                        "Quarterly earnings report with key metrics, narratives, and analysis"
                    ),
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


async def generate_bank_earnings_report(bank_name: str, fiscal_year: int, quarter: str) -> str:
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

        logger.info(
            "etl.bank_earnings_report.retrieval_start",
            execution_id=execution_id,
        )

        supplementary_task = retrieve_supplementary_data(
            bank_info["bank_id"], fiscal_year, quarter, context
        )
        pillar3_task = retrieve_pillar3_data(bank_info["bank_id"], fiscal_year, quarter, context)
        rts_task = retrieve_rts_data(bank_info["bank_id"], fiscal_year, quarter, context)
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

        sections = await extract_all_sections(bank_info, fiscal_year, quarter, raw_data, context)

        output_dir = Path(__file__).parent / "output"
        output_dir.mkdir(exist_ok=True)

        filename = f"{bank_info['bank_symbol']}_{fiscal_year}_{quarter}.html"
        output_path = output_dir / filename

        render_report(sections, output_path)

        if "llm_debug_log" in context:
            debug_filename = f"{bank_info['bank_symbol']}_{fiscal_year}_{quarter}_llm_debug.json"
            debug_path = output_dir / debug_filename
            with open(debug_path, "w", encoding="utf-8") as f:
                json.dump(context["llm_debug_log"], f, indent=2, default=str)
            logger.info(
                "etl.bank_earnings_report.debug_log_saved",
                execution_id=execution_id,
                debug_path=str(debug_path),
            )

        logger.info(
            "etl.bank_earnings_report.rendered",
            execution_id=execution_id,
            output_path=str(output_path),
        )

        await save_to_database(bank_info, fiscal_year, quarter, output_path, execution_id)

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
