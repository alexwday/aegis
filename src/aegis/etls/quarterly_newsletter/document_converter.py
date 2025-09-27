"""
Document converter utilities for quarterly newsletter ETL.

This module provides functions to convert Word documents to PDF and
generate markdown versions of quarterly newsletter reports.
"""

import os
import subprocess
import platform
from typing import Optional, Dict, List, Any
from pathlib import Path
from datetime import datetime
from docx import Document
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor
from aegis.utils.logging import get_logger

logger = get_logger()


def convert_docx_to_pdf_native(docx_path: str, pdf_path: str) -> bool:
    """
    Convert DOCX to PDF using native OS tools if available.

    Args:
        docx_path: Path to input DOCX file
        pdf_path: Path to output PDF file

    Returns:
        True if conversion succeeded, False otherwise
    """
    system = platform.system()

    try:
        if system == "Darwin":  # macOS
            # Try LibreOffice first (best quality)
            try:
                result = subprocess.run(
                    [
                        "soffice",
                        "--headless",
                        "--convert-to",
                        "pdf",
                        "--outdir",
                        os.path.dirname(pdf_path),
                        docx_path
                    ],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                if result.returncode == 0:
                    # LibreOffice creates PDF with same name in outdir
                    generated_pdf = os.path.join(
                        os.path.dirname(pdf_path),
                        Path(docx_path).stem + ".pdf"
                    )
                    if generated_pdf != pdf_path:
                        os.rename(generated_pdf, pdf_path)
                    logger.info(f"PDF created using LibreOffice: {pdf_path}")
                    return True
            except (FileNotFoundError, subprocess.TimeoutExpired):
                pass

        elif system == "Linux":
            # Try LibreOffice
            try:
                result = subprocess.run(
                    [
                        "soffice",
                        "--headless",
                        "--convert-to",
                        "pdf",
                        "--outdir",
                        os.path.dirname(pdf_path),
                        docx_path
                    ],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                if result.returncode == 0:
                    generated_pdf = os.path.join(
                        os.path.dirname(pdf_path),
                        Path(docx_path).stem + ".pdf"
                    )
                    if generated_pdf != pdf_path:
                        os.rename(generated_pdf, pdf_path)
                    logger.info(f"PDF created using LibreOffice: {pdf_path}")
                    return True
            except (FileNotFoundError, subprocess.TimeoutExpired):
                pass

        elif system == "Windows":
            # Try using Word COM automation
            try:
                import win32com.client
                word = win32com.client.Dispatch("Word.Application")
                word.Visible = False
                doc = word.Documents.Open(os.path.abspath(docx_path))
                doc.SaveAs(os.path.abspath(pdf_path), FileFormat=17)  # 17 = PDF format
                doc.Close()
                word.Quit()
                logger.info(f"PDF created using Word COM: {pdf_path}")
                return True
            except Exception:
                pass

    except Exception as e:
        logger.warning(f"Native PDF conversion failed: {e}")

    return False


def convert_docx_to_pdf_fallback(docx_path: str, pdf_path: str) -> bool:
    """
    Fallback PDF conversion using python-docx and reportlab.
    This is a basic conversion that may not preserve all formatting.

    Args:
        docx_path: Path to input DOCX file
        pdf_path: Path to output PDF file

    Returns:
        True if conversion succeeded, False otherwise
    """
    try:
        # Read the DOCX file
        doc = Document(docx_path)

        # Create PDF
        pdf = SimpleDocTemplate(
            pdf_path,
            pagesize=letter,
            rightMargin=0.5*inch,
            leftMargin=0.6*inch,
            topMargin=0.4*inch,
            bottomMargin=0.4*inch
        )

        # Get styles
        styles = getSampleStyleSheet()
        story = []

        # Define custom styles
        heading1_style = ParagraphStyle(
            'CustomHeading1',
            parent=styles['Heading1'],
            fontSize=16,
            textColor=HexColor('#000000'),
            spaceAfter=12,
            spaceBefore=12
        )

        heading2_style = ParagraphStyle(
            'CustomHeading2',
            parent=styles['Heading2'],
            fontSize=14,
            textColor=HexColor('#000000'),
            spaceAfter=8,
            spaceBefore=8
        )

        # Process each paragraph
        for para in doc.paragraphs:
            text = para.text.strip()
            if not text:
                continue

            # Escape special characters for reportlab
            text = text.replace('&', '&amp;')
            text = text.replace('<', '&lt;')
            text = text.replace('>', '&gt;')

            # Determine style based on paragraph style
            if para.style.name.startswith('Heading 1'):
                story.append(Paragraph(text, heading1_style))
            elif para.style.name.startswith('Heading 2'):
                story.append(Paragraph(text, heading2_style))
            else:
                story.append(Paragraph(text, styles['BodyText']))

            story.append(Spacer(1, 3))

        # Build PDF
        pdf.build(story)
        logger.info(f"PDF created using fallback method: {pdf_path}")
        return True

    except Exception as e:
        logger.error(f"Fallback PDF conversion failed: {e}")
        return False


def convert_docx_to_pdf(docx_path: str, pdf_path: Optional[str] = None) -> Optional[str]:
    """
    Convert DOCX to PDF, trying native tools first, then fallback.

    Args:
        docx_path: Path to input DOCX file
        pdf_path: Optional path to output PDF file (defaults to same name as DOCX)

    Returns:
        Path to created PDF file, or None if conversion failed
    """
    if not pdf_path:
        pdf_path = docx_path.replace('.docx', '.pdf')

    # Try native conversion first (better quality)
    if convert_docx_to_pdf_native(docx_path, pdf_path):
        return pdf_path

    # Fallback to Python-based conversion
    logger.info("Native PDF conversion unavailable, using fallback method")
    if convert_docx_to_pdf_fallback(docx_path, pdf_path):
        return pdf_path

    logger.error(f"Failed to convert {docx_path} to PDF")
    return None


def bank_summaries_to_markdown(
    summaries: List[Any],
    quarter: str,
    fiscal_year: int
) -> str:
    """
    Convert bank summaries to markdown format for database storage.

    Args:
        summaries: List of BankSummary objects
        quarter: Quarter string (Q1-Q4)
        fiscal_year: Year

    Returns:
        Markdown-formatted string
    """
    # Start with document header
    markdown = f"# Quarterly Newsletter - {quarter} {fiscal_year}\n\n"
    markdown += f"*Generated: {datetime.now().strftime('%B %d, %Y')}*\n\n"

    # Group summaries by bank type
    canadian_banks = [s for s in summaries if s.bank_type == "Canadian_Banks" and s.processing_success]
    us_banks = [s for s in summaries if s.bank_type == "US_Banks" and s.processing_success]
    failed_banks = [s for s in summaries if not s.processing_success]

    # Add Canadian Banks section
    if canadian_banks:
        markdown += "## Canadian Banks\n\n"
        for summary in canadian_banks:
            markdown += f"### {summary.bank_name} ({summary.bank_symbol})\n\n"
            markdown += f"{summary.summary_paragraph}\n\n"

    # Add US Banks section
    if us_banks:
        markdown += "## US Banks\n\n"
        for summary in us_banks:
            markdown += f"### {summary.bank_name} ({summary.bank_symbol})\n\n"
            markdown += f"{summary.summary_paragraph}\n\n"

    # Add failed banks note at the end if any
    if failed_banks:
        markdown += "---\n\n"
        markdown += "## Processing Notes\n\n"
        markdown += "The following banks could not be processed:\n\n"
        for summary in failed_banks:
            markdown += f"- **{summary.bank_name}**: {summary.error_message or 'Unknown error'}\n"
        markdown += "\n"

    return markdown


def get_standard_report_metadata() -> Dict[str, str]:
    """
    Get standard metadata for quarterly newsletter reports.

    Returns:
        Dictionary with report_name, report_description, and report_type
    """
    return {
        "report_name": "Quarterly Newsletter",
        "report_description": (
            "Consolidated multi-bank earnings summary report containing AI-generated "
            "paragraph summaries for all monitored financial institutions in a given quarter. "
            "Provides a comprehensive overview of quarterly performance across the banking sector."
        ),
        "report_type": "quarterly_newsletter"
    }