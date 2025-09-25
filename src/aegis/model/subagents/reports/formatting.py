"""
Reports Subagent - Formatting Functions

This module handles formatting of retrieved reports data for presentation.
It converts database records into formatted output with download links.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from ....utils.logging import get_logger


async def format_reports_list(
    reports: List[Dict[str, Any]],
    context: Dict[str, Any]
) -> str:
    """
    Format a list of available reports for display.

    Args:
        reports: List of report dictionaries
        context: Runtime context with execution_id

    Returns:
        Formatted string listing available reports
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    if not reports:
        return "No reports available for the specified criteria."

    output = "**Available Reports:**\n\n"

    for report in reports:
        output += f"• **{report['report_name']}** - {report['bank_symbol']} {report['quarter']} {report['fiscal_year']}\n"
        output += f"  {report['report_description']}\n"
        output += f"  Generated: {report['generation_date'].strftime('%Y-%m-%d %H:%M')}\n"

        # Add download links if S3 names are available
        if report.get('s3_document_name'):
            output += f"  [Download Word Document]({report['s3_document_name']})\n"
        if report.get('s3_pdf_name'):
            output += f"  [Download PDF]({report['s3_pdf_name']})\n"

        output += "\n"

    return output


async def format_report_content(
    report: Dict[str, Any],
    include_links: bool = True,
    context: Optional[Dict[str, Any]] = None
) -> str:
    """
    Format a single report's content for display.

    Args:
        report: Report dictionary with markdown_content
        include_links: Whether to include download links
        context: Runtime context with execution_id

    Returns:
        Formatted report content with metadata
    """
    logger = get_logger()
    execution_id = context.get("execution_id") if context else None

    output = []

    # Add report header
    header = f"# {report['report_name']} - {report['bank_symbol']} {report['quarter']} {report['fiscal_year']}\n"
    output.append(header)

    # Add generation metadata
    metadata = f"*Generated on {report['generation_date'].strftime('%B %d, %Y at %I:%M %p')}*\n"
    output.append(metadata)

    # Add download links if available and requested using special markers
    # These markers will be processed by the main agent to create actual S3 URLs
    if include_links:
        # Always include bank/period and report name in display text for clarity
        period_label = f"{report['bank_symbol']} {report['quarter']} {report['fiscal_year']}"
        report_name = report.get('report_name', 'Report')
        links = []
        if report.get('s3_document_name'):
            # Use marker format for main agent to process - download action
            links.append(f'{{{{S3_LINK:download:docx:{report["s3_document_name"]}:Download {report_name} Document ({period_label})}}}}')
        if report.get('s3_pdf_name'):
            # Use marker format for main agent to process - open action
            links.append(f'{{{{S3_LINK:open:pdf:{report["s3_pdf_name"]}:Open {report_name} PDF ({period_label})}}}}')

        if links:
            output.append(" | ".join(links) + "\n")

    # Add horizontal separator
    output.append("---\n\n")

    # Add the main markdown content
    if report.get('markdown_content') and report['markdown_content']:
        output.append(report['markdown_content'])
    else:
        # Only show "no content" message if we also don't have any links
        if not report.get('s3_document_name') and not report.get('s3_pdf_name'):
            output.append("*No content available for this report.*")
        else:
            # If we have links but no markdown, provide a helpful message
            output.append("*Report content is available in the document above. Please download to view the full report.*")

    # Add footer with source info
    footer = f"\n\n---\n*Source: {report.get('generated_by', 'Unknown')} | "
    footer += f"Last Modified: {report['date_last_modified'].strftime('%Y-%m-%d %H:%M')}*"
    output.append(footer)

    return "\n".join(output)


async def format_multiple_reports(
    reports: List[Dict[str, Any]],
    context: Dict[str, Any],
    requested_combinations: Optional[List[Dict[str, Any]]] = None
) -> str:
    """
    Format multiple reports for consolidated display.

    Args:
        reports: List of report dictionaries
        context: Runtime context with execution_id
        requested_combinations: Original bank-period combinations requested (optional)

    Returns:
        Formatted string with all reports
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    if not reports:
        return "No reports found for the specified banks and periods."

    output = []

    # Track which combinations had data
    found_combinations = set()
    for report in reports:
        found_combinations.add((report['bank_symbol'], report['fiscal_year'], report['quarter']))

    # Group reports by bank for better organization
    reports_by_bank = {}
    for report in reports:
        bank_key = (report['bank_symbol'], report['bank_name'])
        if bank_key not in reports_by_bank:
            reports_by_bank[bank_key] = []
        reports_by_bank[bank_key].append(report)

    # First, show unavailable reports if we have the requested combinations
    if requested_combinations:
        missing_combinations = []
        for combo in requested_combinations:
            combo_key = (combo['bank_symbol'], combo['fiscal_year'], combo['quarter'])
            if combo_key not in found_combinations:
                missing_combinations.append(combo)

        if missing_combinations:
            for combo in missing_combinations:
                output.append(f"## {combo['bank_name']} ({combo['bank_symbol']})\n")
                output.append(f"### {combo['quarter']} {combo['fiscal_year']}\n")
                output.append(f"*No call summary report available for {combo['bank_name']} {combo['quarter']} {combo['fiscal_year']}.*\n")
                output.append("\n---\n\n")

    # Format each bank's reports that DO have data
    for (symbol, name), bank_reports in reports_by_bank.items():
        output.append(f"## {name} ({symbol})\n")

        # Sort by period (newest first)
        bank_reports.sort(
            key=lambda r: (r['fiscal_year'], r['quarter']),
            reverse=True
        )

        for report in bank_reports:
            # Add period header
            output.append(f"### {report['quarter']} {report['fiscal_year']} - {report['report_name']}\n")

            # Add download links using markers for main agent processing
            # Include bank/period and report name in display text for clarity
            period_label = f"{symbol} {report['quarter']} {report['fiscal_year']}"
            report_name = report.get('report_name', 'Report')
            links = []
            if report.get('s3_document_name'):
                links.append(f'{{{{S3_LINK:download:docx:{report["s3_document_name"]}:Download {report_name} Document ({period_label})}}}}')
            if report.get('s3_pdf_name'):
                links.append(f'{{{{S3_LINK:open:pdf:{report["s3_pdf_name"]}:Open {report_name} PDF ({period_label})}}}}')

            if links:
                output.append(" | ".join(links) + "\n\n")

            # Add the markdown content
            if report.get('markdown_content'):
                # Limit content length for multiple reports
                content = report['markdown_content']
                if content and len(content) > 5000:  # Check content is not None before getting length
                    content = content[:4997] + "..."
                if content:  # Only append if content is not None/empty
                    output.append(content)
            else:
                # Check if we have links to show different message
                if report.get('s3_document_name') or report.get('s3_pdf_name'):
                    output.append("*Report content is available in the documents above. Please download to view the full report.*")
                else:
                    output.append("*No content available for this report.*")

            output.append("\n---\n\n")

    # Add summary footer
    if reports:
        output.append(f"\n*Total reports available: {len(reports)}*")
    if requested_combinations and len(requested_combinations) > len(reports):
        unavailable_count = len(requested_combinations) - len(reports)
        output.append(f"\n*Reports unavailable: {unavailable_count}*")

    return "\n".join(output)


# S3 Link Marker Format:
# {{S3_LINK:file_type:s3_key:display_text}}
#
# These markers are processed by the main agent to create actual S3 URLs.
# The main agent will:
# 1. Parse the marker to extract file_type and s3_key
# 2. Construct the full S3 URL using S3_REPORTS_BASE_URL from config
# 3. Replace the marker with an HTML link
#
# Example marker: {{S3_LINK:docx:RY_2025_Q2_abc123.docx:📄 Download Word}}
# Becomes: <a href="https://s3.amazonaws.com/bucket/reports/RY_2025_Q2_abc123.docx">📄 Download Word</a>


async def format_no_data_message(
    bank_period_combinations: List[Dict[str, Any]],
    context: Dict[str, Any]
) -> str:
    """
    Format a message when no reports are available.

    Args:
        bank_period_combinations: Requested combinations
        context: Runtime context

    Returns:
        Formatted message explaining no data
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    if not bank_period_combinations:
        return "No bank and period combinations were specified."

    message = "No pre-generated reports are available for:\n\n"

    for combo in bank_period_combinations:
        message += f"• {combo['bank_name']} ({combo['bank_symbol']}) - {combo['quarter']} {combo['fiscal_year']}\n"

    message += "\nReports are generated periodically through ETL processes. "
    message += "Please check back later or contact support if you believe reports should be available."

    logger.info(
        "subagent.reports.no_data",
        execution_id=execution_id,
        combinations_requested=len(bank_period_combinations)
    )

    return message


async def format_error_message(
    error: str,
    context: Dict[str, Any]
) -> str:
    """
    Format an error message for display.

    Args:
        error: Error description
        context: Runtime context

    Returns:
        Formatted error message
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.error(
        "subagent.reports.formatting_error",
        execution_id=execution_id,
        error=error
    )

    return f"⚠️ An error occurred while retrieving reports: {error}\n\nPlease try again or contact support if the issue persists."