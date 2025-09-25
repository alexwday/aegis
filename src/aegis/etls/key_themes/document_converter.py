"""
Document converter utilities for key themes ETL.

This module provides functions to create Word/PDF documents from themed Q&A data
and utilities for markdown processing.
"""

import os
import subprocess
import platform
from typing import Optional, Dict, List, Any
from pathlib import Path
from datetime import datetime
import re
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.style import WD_STYLE_TYPE
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from aegis.utils.logging import get_logger

logger = get_logger()


def convert_docx_to_pdf(docx_path: str, pdf_path: str) -> bool:
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
                logger.warning("LibreOffice not available, skipping PDF generation")

        logger.warning(f"PDF conversion not implemented for {system}")
        return False

    except Exception as e:
        logger.error(f"Error converting to PDF: {str(e)}")
        return False


def create_key_themes_document(
    themes_data: List[Dict[str, Any]],
    bank_name: str,
    fiscal_year: int,
    quarter: str,
    output_path: str
) -> None:
    """
    Create a Word document from key themes data.

    Args:
        themes_data: List of processed Q&A themes
        bank_name: Name of the bank
        fiscal_year: Year of the report
        quarter: Quarter identifier
        output_path: Path to save the DOCX file
    """
    doc = Document()

    # Set up document styles
    setup_document_styles(doc)

    # Add title page
    add_title_page(doc, bank_name, fiscal_year, quarter)

    # Add executive summary
    add_executive_summary(doc, themes_data)

    # Add table of contents
    add_table_of_contents(doc, themes_data)

    # Add themed Q&A sections
    for idx, theme in enumerate(themes_data):
        add_theme_section(doc, theme, idx + 1)

    # Save document
    doc.save(output_path)
    logger.info(f"Document saved: {output_path}")


def setup_document_styles(doc: Document) -> None:
    """Set up custom styles for the document."""
    styles = doc.styles

    # Create theme title style
    theme_style = styles.add_style('ThemeTitle', WD_STYLE_TYPE.PARAGRAPH)
    theme_style.font.name = 'Calibri'
    theme_style.font.size = Pt(14)
    theme_style.font.bold = True
    theme_style.font.color.rgb = RGBColor(0x1F, 0x49, 0x7D)  # Dark blue
    theme_style.paragraph_format.space_after = Pt(6)

    # Create Q&A content style
    qa_style = styles.add_style('QAContent', WD_STYLE_TYPE.PARAGRAPH)
    qa_style.font.name = 'Calibri'
    qa_style.font.size = Pt(11)
    qa_style.paragraph_format.space_after = Pt(6)
    qa_style.paragraph_format.line_spacing = 1.15


def add_title_page(
    doc: Document,
    bank_name: str,
    fiscal_year: int,
    quarter: str
) -> None:
    """Add a title page to the document."""
    # Title
    title = doc.add_heading(level=0)
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = title.add_run(f"{bank_name}")
    run.font.size = Pt(24)
    run.font.bold = True

    # Subtitle
    subtitle = doc.add_paragraph()
    subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = subtitle.add_run(f"Earnings Call Key Themes\n{quarter} {fiscal_year}")
    run.font.size = Pt(18)

    # Add space
    doc.add_paragraph()
    doc.add_paragraph()

    # Generated date
    date_para = doc.add_paragraph()
    date_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = date_para.add_run(f"Generated: {datetime.now().strftime('%B %d, %Y')}")
    run.font.size = Pt(12)
    run.font.italic = True

    # Page break
    doc.add_page_break()


def add_executive_summary(
    doc: Document,
    themes_data: List[Dict[str, Any]]
) -> None:
    """Add an executive summary section."""
    doc.add_heading('Executive Summary', level=1)

    summary_para = doc.add_paragraph()
    summary_para.add_run(
        f"This document contains {len(themes_data)} key themes extracted from "
        f"the earnings call Q&A session. Each theme represents a distinct topic "
        f"of discussion between analysts and company executives."
    )

    # Add theme overview list
    doc.add_paragraph()
    doc.add_paragraph("Key themes discussed:")

    for idx, theme in enumerate(themes_data, 1):
        theme_title = theme.get('theme_title', f'Theme {idx}')
        doc.add_paragraph(f"  {idx}. {theme_title}", style='List Bullet')

    doc.add_page_break()


def add_table_of_contents(
    doc: Document,
    themes_data: List[Dict[str, Any]]
) -> None:
    """Add a table of contents."""
    doc.add_heading('Table of Contents', level=1)

    for idx, theme in enumerate(themes_data, 1):
        theme_title = theme.get('theme_title', f'Theme {idx}')
        toc_para = doc.add_paragraph()
        toc_para.add_run(f"{idx}. {theme_title}")

    doc.add_page_break()


def add_theme_section(
    doc: Document,
    theme: Dict[str, Any],
    section_num: int
) -> None:
    """
    Add a themed Q&A section to the document.

    Args:
        doc: Document object
        theme: Theme data dictionary
        section_num: Section number
    """
    # Add section heading
    theme_title = theme.get('theme_title', f'Theme {section_num}')
    heading = doc.add_heading(level=2)
    heading.add_run(f"{section_num}. {theme_title}")

    # Add formatted content
    formatted_content = theme.get('formatted_content', '')
    if formatted_content:
        add_markdown_content(doc, formatted_content)
    else:
        # Fallback to original content if no formatted version
        original_content = theme.get('original_content', '')
        if original_content:
            doc.add_paragraph(original_content)

    # Add separator
    doc.add_paragraph()
    add_horizontal_line(doc)
    doc.add_paragraph()


def add_markdown_content(doc: Document, markdown_text: str) -> None:
    """
    Add markdown-formatted content to the document.

    This function processes markdown and applies appropriate Word formatting.

    Args:
        doc: Document object
        markdown_text: Markdown formatted text
    """
    # Split into paragraphs
    paragraphs = markdown_text.split('\n\n')

    for para_text in paragraphs:
        if not para_text.strip():
            continue

        # Check for special formatting
        if para_text.strip().startswith('---'):
            add_horizontal_line(doc)
        elif para_text.strip().startswith('>'):
            # Block quote
            quote_text = para_text.strip().lstrip('>')
            para = doc.add_paragraph()
            para.paragraph_format.left_indent = Inches(0.5)
            add_formatted_runs(para, quote_text)
        elif para_text.strip().startswith(('- ', '* ', '+ ')):
            # Bullet list
            lines = para_text.strip().split('\n')
            for line in lines:
                clean_line = re.sub(r'^[-*+]\s+', '', line)
                para = doc.add_paragraph(style='List Bullet')
                add_formatted_runs(para, clean_line)
        elif re.match(r'^\d+\.\s+', para_text.strip()):
            # Numbered list
            lines = para_text.strip().split('\n')
            for line in lines:
                clean_line = re.sub(r'^\d+\.\s+', '', line)
                para = doc.add_paragraph(style='List Number')
                add_formatted_runs(para, clean_line)
        else:
            # Regular paragraph
            para = doc.add_paragraph()
            add_formatted_runs(para, para_text)


def add_formatted_runs(paragraph, text: str) -> None:
    """
    Add formatted runs to a paragraph based on markdown syntax.

    Args:
        paragraph: Document paragraph object
        text: Text with markdown formatting
    """
    # Pattern to match markdown formatting
    pattern = r'(\*\*\*(.+?)\*\*\*|\*\*(.+?)\*\*|\*(.+?)\*|__(.+?)__|<u>(.+?)</u>)'

    last_end = 0
    for match in re.finditer(pattern, text):
        # Add text before the match
        if match.start() > last_end:
            paragraph.add_run(text[last_end:match.start()])

        # Add formatted text
        if match.group(2):  # Bold + Italic
            run = paragraph.add_run(match.group(2))
            run.font.bold = True
            run.font.italic = True
        elif match.group(3):  # Bold
            run = paragraph.add_run(match.group(3))
            run.font.bold = True
        elif match.group(4):  # Italic
            run = paragraph.add_run(match.group(4))
            run.font.italic = True
        elif match.group(5) or match.group(6):  # Underline
            content = match.group(5) if match.group(5) else match.group(6)
            run = paragraph.add_run(content)
            run.font.underline = True

        last_end = match.end()

    # Add remaining text
    if last_end < len(text):
        paragraph.add_run(text[last_end:])


def add_horizontal_line(doc: Document) -> None:
    """Add a horizontal line to the document."""
    para = doc.add_paragraph()
    run = para.add_run()
    fldChar = OxmlElement('w:fldChar')
    fldChar.set(qn('w:fldCharType'), 'begin')
    instrText = OxmlElement('w:instrText')
    instrText.text = "PAGE"
    fldChar2 = OxmlElement('w:fldChar')
    fldChar2.set(qn('w:fldCharType'), 'end')
    run._r.append(fldChar)
    run._r.append(instrText)
    run._r.append(fldChar2)


def structured_data_to_markdown(themes_data: List[Dict[str, Any]]) -> str:
    """
    Convert structured themes data to markdown format.

    Args:
        themes_data: List of theme dictionaries

    Returns:
        Markdown formatted string
    """
    markdown_parts = []

    for idx, theme in enumerate(themes_data, 1):
        theme_title = theme.get('theme_title', f'Theme {idx}')
        formatted_content = theme.get('formatted_content', '')

        markdown_parts.append(f"## {idx}. {theme_title}\n")
        markdown_parts.append(formatted_content)
        markdown_parts.append("\n---\n")

    return "\n".join(markdown_parts)


def get_standard_report_metadata(
    bank_name: str,
    fiscal_year: int,
    quarter: str,
    report_type: str,
    theme_count: int
) -> Dict[str, Any]:
    """
    Generate standard metadata for a key themes report.

    Args:
        bank_name: Name of the bank
        fiscal_year: Year of the report
        quarter: Quarter identifier
        report_type: Type of report (e.g., 'key_themes')
        theme_count: Number of themes extracted

    Returns:
        Dictionary containing report metadata
    """
    return {
        'report_type': report_type,
        'bank_name': bank_name,
        'fiscal_year': fiscal_year,
        'quarter': quarter,
        'generated_at': datetime.now().isoformat(),
        'theme_count': theme_count,
        'version': '1.0'
    }