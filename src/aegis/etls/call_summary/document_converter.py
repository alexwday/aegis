"""
Document converter utilities for call summary ETL.

This module provides functions to convert DOCX to PDF and extract markdown content.
"""

import os
import subprocess
import platform
from typing import Optional, Dict, List, Any
from pathlib import Path
from docx import Document
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT
from reportlab.lib.colors import HexColor
from aegis.utils.logging import get_logger

logger = get_logger()


def convert_docx_to_pdf_native(docx_path: str, pdf_path: str) -> bool:
    """
    Convert DOCX to PDF using native OS tools if available.

    On macOS: Uses soffice (LibreOffice) or textutil
    On Linux: Uses soffice (LibreOffice)
    On Windows: Uses COM automation with Word if available

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

            # Fallback to textutil (lower quality but always available)
            try:
                # Convert DOCX to HTML first
                html_path = pdf_path.replace('.pdf', '.html')
                result = subprocess.run(
                    ["textutil", "-convert", "html", docx_path, "-output", html_path],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                if result.returncode == 0:
                    # Convert HTML to PDF using cupsfilter
                    result = subprocess.run(
                        ["cupsfilter", html_path],
                        capture_output=True,
                        timeout=30
                    )
                    if result.returncode == 0:
                        with open(pdf_path, 'wb') as f:
                            f.write(result.stdout)
                        # Clean up temp HTML
                        os.remove(html_path)
                        logger.info(f"PDF created using textutil/cupsfilter: {pdf_path}")
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
            fontSize=14,
            textColor=HexColor('#000000'),
            spaceAfter=10,
            spaceBefore=10
        )

        heading2_style = ParagraphStyle(
            'CustomHeading2',
            parent=styles['Heading2'],
            fontSize=11,
            textColor=HexColor('#000000'),
            spaceAfter=6,
            spaceBefore=6
        )

        bullet_style = ParagraphStyle(
            'BulletStyle',
            parent=styles['BodyText'],
            fontSize=9,
            leftIndent=20,
            bulletIndent=10
        )

        quote_style = ParagraphStyle(
            'QuoteStyle',
            parent=styles['BodyText'],
            fontSize=8,
            leftIndent=54,
            rightIndent=36,
            textColor=HexColor('#404040'),
            fontName='Helvetica-Oblique'
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
            elif para.style.name == 'List Bullet':
                story.append(Paragraph(f"• {text}", bullet_style))
            elif para.paragraph_format.left_indent and para.paragraph_format.left_indent > inch/2:
                story.append(Paragraph(text, quote_style))
            else:
                story.append(Paragraph(text, styles['BodyText']))

            story.append(Spacer(1, 2))

        # Build PDF
        pdf.build(story)
        logger.info(f"PDF created using fallback method: {pdf_path}")
        return True

    except Exception as e:
        logger.error(f"Fallback PDF conversion failed: {e}")
        return False


def convert_docx_to_pdf(docx_path: str, pdf_path: Optional[str] = None) -> str:
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


def structured_data_to_markdown(category_results: List[Dict[str, Any]],
                                bank_info: Dict[str, str],
                                quarter: str,
                                fiscal_year: int) -> str:
    """
    Convert structured category data to markdown format.

    Args:
        category_results: List of category dictionaries with structured data
        bank_info: Dictionary with bank_name, bank_symbol
        quarter: Quarter string (Q1-Q4)
        fiscal_year: Year

    Returns:
        Markdown-formatted string
    """
    from itertools import groupby

    # Filter out rejected categories
    valid_categories = [c for c in category_results if not c.get('rejected', False)]

    # Start with document header
    markdown = f"# {quarter}/{str(fiscal_year)[-2:]} Results and Call Summary - {bank_info['bank_symbol']}\n\n"

    # Sort categories by report section and index
    sorted_categories = sorted(valid_categories, key=lambda x: (
        0 if x.get('report_section', 'Results Summary') == 'Results Summary' else 1,
        x.get('index', 0)
    ))

    # Group by report section
    for section_name, section_categories in groupby(sorted_categories,
                                                    key=lambda x: x.get('report_section', 'Results Summary')):
        section_categories = list(section_categories)

        # Add section header
        markdown += f"## {section_name}\n\n"

        # Add each category
        for category_data in section_categories:
            # Category title
            markdown += f"### {category_data['title']}\n\n"

            # Process summary statements
            for statement_data in category_data.get('summary_statements', []):
                # Add statement as bullet point
                statement = statement_data['statement']
                # Convert **bold** to markdown bold
                statement = statement.replace('**', '*')
                markdown += f"- {statement}\n"

                # Add evidence as nested quotes
                for evidence in statement_data.get('evidence', []):
                    content = evidence['content']
                    # Truncate long evidence
                    if len(content) > 500:
                        content = content[:497] + '...'

                    speaker = evidence['speaker']

                    if evidence['type'] == 'quote':
                        markdown += f"  > \"{content}\" — {speaker}\n"
                    else:
                        markdown += f"  > {content} — {speaker}\n"

                markdown += "\n"

            markdown += "\n"

    return markdown


def get_standard_report_metadata() -> Dict[str, str]:
    """
    Get standard metadata for call summary reports.

    Returns:
        Dictionary with report_name and report_description
    """
    return {
        "report_name": "Earnings Call Summary",
        "report_description": (
            "AI-generated comprehensive summary of quarterly earnings call transcripts, "
            "extracting key financial metrics, strategic insights, and management guidance. "
            "Includes categorized analysis of financial performance, credit quality, "
            "capital position, business segments, and forward-looking statements."
        ),
        "report_type": "call_summary"
    }