"""
Document converter utilities for CM Readthrough ETL.

This module provides functions to create combined Word documents for capital markets analysis.
"""

import os
import subprocess
import platform
from typing import Optional, Dict, List, Any
from pathlib import Path
from datetime import datetime
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.style import WD_STYLE_TYPE
from docx.enum.section import WD_ORIENTATION
from docx.oxml import OxmlElement, parse_xml as docx_parse_xml
from docx.oxml.ns import qn, nsdecls as docx_nsdecls
from aegis.utils.logging import get_logger
import re

logger = get_logger()


def clean_xml_text(text: str) -> str:
    """
    Clean text to be XML-compatible by removing NULL bytes and control characters.

    Args:
        text: Input text that may contain invalid XML characters

    Returns:
        Cleaned text safe for XML/Word document insertion
    """
    if not text:
        return text

    # Remove NULL bytes
    text = text.replace('\x00', '')

    # Remove other control characters except tab, newline, and carriage return
    # XML 1.0 valid characters: #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD]
    cleaned = []
    for char in text:
        code = ord(char)
        if (code == 0x9 or  # tab
            code == 0xA or  # newline
            code == 0xD or  # carriage return
            (0x20 <= code <= 0xD7FF) or
            (0xE000 <= code <= 0xFFFD)):
            cleaned.append(char)

    return ''.join(cleaned)


def parse_xml(xml_string):
    """Parse XML string into OxmlElement using python-docx's parse_xml."""
    return docx_parse_xml(xml_string)


def nsdecls(*prefixes):
    """Generate namespace declarations for XML using python-docx's nsdecls."""
    return docx_nsdecls(*prefixes)


def add_html_formatted_runs(paragraph, text: str, font_size: int = 9) -> None:
    """
    Add formatted runs to a paragraph processing HTML tags.

    Supports: <strong><u>text</u></strong> for bold+underline

    Args:
        paragraph: Document paragraph object
        text: Text with HTML formatting
        font_size: Font size in points
    """
    # Clean text of NULL bytes and control characters
    text = clean_xml_text(text)

    # Pattern to match <strong><u>...</u></strong>
    pattern = r'<strong><u>(.*?)</u></strong>'

    last_end = 0
    for match in re.finditer(pattern, text):
        # Add text before the match
        if match.start() > last_end:
            run = paragraph.add_run(text[last_end:match.start()])
            run.font.size = Pt(font_size)

        # Add bold+underlined text
        run = paragraph.add_run(match.group(1))
        run.font.bold = True
        run.font.underline = True
        run.font.size = Pt(font_size)

        last_end = match.end()

    # Add remaining text
    if last_end < len(text):
        run = paragraph.add_run(text[last_end:])
        run.font.size = Pt(font_size)


def convert_docx_to_pdf(docx_path: str, pdf_path: str) -> bool:
    """
    Convert DOCX to PDF using multiple methods.

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
                    # Then HTML to PDF using cupsfilter
                    result = subprocess.run(
                        ["cupsfilter", html_path],
                        capture_output=True,
                        timeout=30
                    )
                    if result.returncode == 0:
                        with open(pdf_path, 'wb') as f:
                            f.write(result.stdout)
                        os.remove(html_path)  # Clean up
                        logger.info(f"PDF created using textutil/cups: {pdf_path}")
                        return True
            except Exception:
                pass

        elif system == "Linux":
            # Try LibreOffice
            try:
                result = subprocess.run(
                    [
                        "libreoffice",
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
        logger.error(f"Error converting to PDF: {e}")

    logger.warning(f"PDF conversion failed for {docx_path}")
    return False


def generate_main_title(quarter: str, year: int) -> str:
    """Generate main title for the report."""
    return f"Read Through For Capital Markets: {quarter}/{str(year)[2:]} Select U.S. & European Banks"


def create_combined_document(results: Dict[str, Any], output_path: str) -> None:
    """
    Create a combined Word document for CM Readthrough analysis with 3 sections.

    Args:
        results: Structured results from processing with 3 sections
        output_path: Path to save the document
    """
    # Create document
    doc = Document()

    # Section 1: Outlook
    add_section1_outlook(doc, results)

    # Page break before Section 2
    doc.add_page_break()

    # Section 2: Q&A (4 categories)
    add_section2_qa(doc, results)

    # Page break before Section 3
    doc.add_page_break()

    # Section 3: Q&A (2 categories)
    add_section3_qa(doc, results)

    # Save document
    doc.save(output_path)
    logger.info(f"Document saved to {output_path}")


def add_title_page(doc: Document, metadata: Dict[str, Any]) -> None:
    """Add title page to document."""
    # Title
    title = doc.add_heading("Capital Markets Readthrough", 0)
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER

    # Subtitle
    subtitle = doc.add_paragraph()
    subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = subtitle.add_run(f"{metadata['fiscal_year']} {metadata['quarter']}")
    run.font.size = Pt(18)
    run.font.color.rgb = RGBColor(0x40, 0x40, 0x40)

    doc.add_paragraph()
    doc.add_paragraph()

    # Banks covered
    info = doc.add_paragraph()
    info.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = info.add_run(f"Analysis of {metadata['banks_processed']} Financial Institutions")
    run.font.size = Pt(14)

    doc.add_paragraph()

    # Generation date
    date_para = doc.add_paragraph()
    date_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = date_para.add_run(f"Generated: {datetime.now().strftime('%B %d, %Y')}")
    run.font.size = Pt(12)
    run.font.italic = True

    doc.add_paragraph()
    doc.add_paragraph()

    # Disclaimer
    disclaimer = doc.add_paragraph()
    disclaimer.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = disclaimer.add_run(
        "This document contains paraphrased commentary and verbatim analyst questions "
        "from earnings call transcripts. For internal use only."
    )
    run.font.size = Pt(10)
    run.font.color.rgb = RGBColor(0x80, 0x80, 0x80)


def add_executive_summary(doc: Document, results: Dict[str, Any]) -> None:
    """Add executive summary section."""
    doc.add_page_break()
    doc.add_heading("Executive Summary", 1)

    # Summary statistics
    doc.add_heading("Coverage Summary", 2)

    stats = [
        f"• Banks Analyzed: {results['metadata']['banks_processed']}",
        f"• Total Analyst Questions Categorized: {results['metadata']['total_qas']}",
        f"• Reporting Period: {results['metadata']['fiscal_year']} {results['metadata']['quarter']}",
    ]

    for stat in stats:
        doc.add_paragraph(stat)

    # Key themes summary
    doc.add_heading("Key Themes Identified", 2)

    # Count questions by category
    qa_counts = {}
    for category, questions in results.get("categorized_qas", {}).items():
        qa_counts[category] = len(questions)

    # Sort by count
    sorted_categories = sorted(qa_counts.items(), key=lambda x: x[1], reverse=True)

    # Show all categories in executive summary
    for category, count in sorted_categories:
        doc.add_paragraph(f"• {category}: {count} questions")


def add_section1_outlook(doc: Document, results: Dict[str, Any]) -> None:
    """
    Add Section 1: Investment Banking & Trading Outlook.

    Args:
        doc: Document object
        results: Full results dictionary
    """
    metadata = results["metadata"]
    outlook_data = results.get("outlook", {})

    # Set landscape orientation for this section
    section = doc.sections[-1] if doc.sections else doc.sections[0]
    section.orientation = WD_ORIENTATION.LANDSCAPE
    section.page_width, section.page_height = section.page_height, section.page_width
    section.top_margin = Inches(0.75)
    section.bottom_margin = Inches(0.75)
    section.left_margin = Inches(1)
    section.right_margin = Inches(1)

    # Add main title
    main_title = generate_main_title(metadata["quarter"], metadata["fiscal_year"])
    title_para = doc.add_paragraph()
    title_run = title_para.add_run(main_title)
    title_run.font.size = Pt(16)
    title_run.font.bold = True
    title_run.font.color.rgb = RGBColor(0, 32, 96)  # Dark blue
    title_para.alignment = WD_ALIGN_PARAGRAPH.CENTER

    # Add Section 1 subtitle
    subtitle_text = metadata.get("subtitle_section1", "Outlook: Capital markets activity")
    subtitle_para = doc.add_paragraph()
    subtitle_run = subtitle_para.add_run(subtitle_text)
    subtitle_run.font.size = Pt(12)
    subtitle_run.font.italic = True
    subtitle_para.alignment = WD_ALIGN_PARAGRAPH.CENTER

    doc.add_paragraph()  # Spacing

    # Add horizontal line
    doc.add_paragraph("_" * 100)
    doc.add_paragraph()  # Spacing

    if not outlook_data:
        doc.add_paragraph("No outlook statements available.")
        return

    # outlook_data structure: {bank_name: {"bank_symbol": "...", "statements": [...]}}
    banks_with_content = {bank: content for bank, content in outlook_data.items()
                          if content.get("statements")}

    if not banks_with_content:
        return

    # Get bank symbols for ticker mapping (already in the data structure)
    name_to_symbol = {bank_name: data.get("bank_symbol", bank_name[:4].upper())
                      for bank_name, data in outlook_data.items()}

    # Create table with 2 columns
    table = doc.add_table(rows=len(banks_with_content) + 1, cols=2)

    # Set column widths: narrow for tickers, wide for content
    table.columns[0].width = Inches(1.0)  # Narrow ticker column
    table.columns[1].width = Inches(6.0)  # Wide content column

    # Header row styling - dark blue background with white text
    header_cells = table.rows[0].cells
    header_cells[0].text = "Banks/\nSegments"
    header_cells[1].text = "Investment Banking and Trading Outlook"

    # Format header with dark blue background and white text
    for cell in header_cells:
        # Set dark blue background (RGB: 0, 32, 96 - professional dark blue)
        shading_elm = parse_xml(r'<w:shd {} w:fill="002060"/>'.format(nsdecls('w')))
        cell._tc.get_or_add_tcPr().append(shading_elm)

        for paragraph in cell.paragraphs:
            paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
            for run in paragraph.runs:
                run.font.bold = True
                run.font.size = Pt(10)
                run.font.color.rgb = RGBColor(255, 255, 255)  # White text

    # Data rows
    row_idx = 1
    for bank_name, bank_data in banks_with_content.items():
        row = table.rows[row_idx]

        # Column 1: Bank ticker
        ticker = name_to_symbol.get(bank_name, bank_name[:4].upper())
        ticker_cell = row.cells[0]
        ticker_para = ticker_cell.paragraphs[0]
        ticker_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
        ticker_run = ticker_para.add_run(ticker)
        ticker_run.font.size = Pt(9)
        ticker_run.font.bold = True

        # Column 2: Outlook statements content
        content_cell = row.cells[1]
        content_cell.text = ""  # Clear default text

        # Add each statement as a bullet point
        statements = bank_data.get("statements", [])
        for statement in statements:
            category = statement.get("category", "")
            # Use formatted_quote if available, otherwise fall back to statement
            content_text = statement.get("formatted_quote", statement.get("statement", ""))

            # Add bullet paragraph
            bullet_para = content_cell.add_paragraph(style='List Bullet')

            # Add category in bold (replacing old "theme")
            category_run = bullet_para.add_run(f"{category}: ")
            category_run.font.bold = True
            category_run.font.size = Pt(9)

            # Add formatted content, processing HTML tags
            add_html_formatted_runs(bullet_para, content_text, font_size=9)

        row_idx += 1

    # Set table borders: inner borders + top/bottom, no left/right
    tbl = table._element
    tbl_pr = tbl.tblPr
    if tbl_pr is None:
        tbl_pr = parse_xml(r'<w:tblPr {}/> '.format(nsdecls('w')))
        tbl.insert(0, tbl_pr)

    # Define border style
    tbl_borders = parse_xml(
        r'<w:tblBorders {}>'
        r'<w:top w:val="single" w:sz="4" w:color="000000"/>'
        r'<w:bottom w:val="single" w:sz="4" w:color="000000"/>'
        r'<w:insideH w:val="single" w:sz="4" w:color="000000"/>'
        r'<w:insideV w:val="single" w:sz="4" w:color="000000"/>'
        r'</w:tblBorders>'.format(nsdecls('w'))
    )
    tbl_pr.append(tbl_borders)

    # Add footer with horizontal line
    doc.add_paragraph()
    doc.add_paragraph("_" * 100)

    footer_table = doc.add_table(rows=1, cols=2)
    footer_table.rows[0].cells[0].text = "Source: Company Reports, Transcripts"
    footer_table.rows[0].cells[1].text = "RBC"

    for cell in footer_table.rows[0].cells:
        for paragraph in cell.paragraphs:
            for run in paragraph.runs:
                run.font.size = Pt(8)
                run.font.italic = True


def add_section2_qa(doc: Document, results: Dict[str, Any]) -> None:
    """
    Add Section 2: Q&A for Global Markets, Risk Management, Corporate Banking, Regulatory Changes.

    Args:
        doc: Document object
        results: Full results dictionary
    """
    metadata = results["metadata"]
    questions_data = results.get("section2_questions", {})

    # Set landscape orientation
    section = doc.sections[-1]
    section.orientation = WD_ORIENTATION.LANDSCAPE
    section.page_width, section.page_height = section.page_height, section.page_width
    section.top_margin = Inches(0.75)
    section.bottom_margin = Inches(0.75)
    section.left_margin = Inches(1)
    section.right_margin = Inches(1)

    # Add main title
    main_title = generate_main_title(metadata["quarter"], metadata["fiscal_year"])
    title_para = doc.add_paragraph()
    title_run = title_para.add_run(main_title)
    title_run.font.size = Pt(16)
    title_run.font.bold = True
    title_run.font.color.rgb = RGBColor(0, 32, 96)  # Dark blue
    title_para.alignment = WD_ALIGN_PARAGRAPH.CENTER

    # Add Section 2 subtitle
    subtitle_text = metadata.get("subtitle_section2", "Conference calls: Market dynamics")
    subtitle_para = doc.add_paragraph()
    subtitle_run = subtitle_para.add_run(subtitle_text)
    subtitle_run.font.size = Pt(12)
    subtitle_run.font.italic = True
    subtitle_para.alignment = WD_ALIGN_PARAGRAPH.CENTER

    doc.add_paragraph()  # Spacing

    # Add horizontal line
    doc.add_paragraph("_" * 100)
    doc.add_paragraph()  # Spacing

    if not questions_data:
        doc.add_paragraph("No Section 2 questions available.")
        return

    # Create 3-column table: Themes | Banks | Relevant Questions
    # Count total questions first
    total_questions = sum(len(data.get("questions", [])) for data in questions_data.values())

    if total_questions == 0:
        doc.add_paragraph("No Section 2 questions available.")
        return

    table = doc.add_table(rows=total_questions + 1, cols=3)
    table.columns[0].width = Inches(2.0)  # Themes
    table.columns[1].width = Inches(1.0)  # Banks
    table.columns[2].width = Inches(5.5)  # Questions

    # Header row
    header_cells = table.rows[0].cells
    header_cells[0].text = "Themes"
    header_cells[1].text = "Banks"
    header_cells[2].text = "Relevant Questions"

    # Format header
    for cell in header_cells:
        shading_elm = parse_xml(r'<w:shd {} w:fill="002060"/>'.format(nsdecls('w')))
        cell._tc.get_or_add_tcPr().append(shading_elm)
        for paragraph in cell.paragraphs:
            paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
            for run in paragraph.runs:
                run.font.bold = True
                run.font.size = Pt(10)
                run.font.color.rgb = RGBColor(255, 255, 255)

    # Populate table rows
    row_idx = 1
    for bank_name, bank_data in questions_data.items():
        ticker = bank_data.get("bank_symbol", "")
        for question in bank_data.get("questions", []):
            row = table.rows[row_idx]

            # Column 1: Theme/Category
            row.cells[0].text = question.get("category", "")
            for paragraph in row.cells[0].paragraphs:
                for run in paragraph.runs:
                    run.font.size = Pt(9)
                    run.font.bold = True

            # Column 2: Bank ticker
            row.cells[1].text = ticker
            row.cells[1].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
            for paragraph in row.cells[1].paragraphs:
                for run in paragraph.runs:
                    run.font.size = Pt(9)

            # Column 3: Question text
            row.cells[2].text = question.get("verbatim_question", "")
            for paragraph in row.cells[2].paragraphs:
                for run in paragraph.runs:
                    run.font.size = Pt(9)

            row_idx += 1

    # Add table borders
    tbl = table._element
    tbl_pr = tbl.tblPr
    if tbl_pr is None:
        tbl_pr = parse_xml(r'<w:tblPr {}/> '.format(nsdecls('w')))
        tbl.insert(0, tbl_pr)

    tbl_borders = parse_xml(
        r'<w:tblBorders {}>'
        r'<w:top w:val="single" w:sz="4" w:color="000000"/>'
        r'<w:bottom w:val="single" w:sz="4" w:color="000000"/>'
        r'<w:insideH w:val="single" w:sz="4" w:color="000000"/>'
        r'<w:insideV w:val="single" w:sz="4" w:color="000000"/>'
        r'</w:tblBorders>'.format(nsdecls('w'))
    )
    tbl_pr.append(tbl_borders)


def add_section3_qa(doc: Document, results: Dict[str, Any]) -> None:
    """
    Add Section 3: Q&A for Investment Banking/M&A and Transaction Banking.

    Args:
        doc: Document object
        results: Full results dictionary
    """
    metadata = results["metadata"]
    questions_data = results.get("section3_questions", {})

    # Set landscape orientation
    section = doc.sections[-1]
    section.orientation = WD_ORIENTATION.LANDSCAPE
    section.page_width, section.page_height = section.page_height, section.page_width
    section.top_margin = Inches(0.75)
    section.bottom_margin = Inches(0.75)
    section.left_margin = Inches(1)
    section.right_margin = Inches(1)

    # Add main title
    main_title = generate_main_title(metadata["quarter"], metadata["fiscal_year"])
    title_para = doc.add_paragraph()
    title_run = title_para.add_run(main_title)
    title_run.font.size = Pt(16)
    title_run.font.bold = True
    title_run.font.color.rgb = RGBColor(0, 32, 96)  # Dark blue
    title_para.alignment = WD_ALIGN_PARAGRAPH.CENTER

    # Add Section 3 subtitle
    subtitle_text = metadata.get("subtitle_section3", "Conference calls: Pipeline dynamics")
    subtitle_para = doc.add_paragraph()
    subtitle_run = subtitle_para.add_run(subtitle_text)
    subtitle_run.font.size = Pt(12)
    subtitle_run.font.italic = True
    subtitle_para.alignment = WD_ALIGN_PARAGRAPH.CENTER

    doc.add_paragraph()  # Spacing

    # Add horizontal line
    doc.add_paragraph("_" * 100)
    doc.add_paragraph()  # Spacing

    if not questions_data:
        doc.add_paragraph("No Section 3 questions available.")
        return

    # Create 3-column table: Themes | Banks | Relevant Questions
    total_questions = sum(len(data.get("questions", [])) for data in questions_data.values())

    if total_questions == 0:
        doc.add_paragraph("No Section 3 questions available.")
        return

    table = doc.add_table(rows=total_questions + 1, cols=3)
    table.columns[0].width = Inches(2.0)  # Themes
    table.columns[1].width = Inches(1.0)  # Banks
    table.columns[2].width = Inches(5.5)  # Questions

    # Header row
    header_cells = table.rows[0].cells
    header_cells[0].text = "Themes"
    header_cells[1].text = "Banks"
    header_cells[2].text = "Relevant Questions"

    # Format header
    for cell in header_cells:
        shading_elm = parse_xml(r'<w:shd {} w:fill="002060"/>'.format(nsdecls('w')))
        cell._tc.get_or_add_tcPr().append(shading_elm)
        for paragraph in cell.paragraphs:
            paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
            for run in paragraph.runs:
                run.font.bold = True
                run.font.size = Pt(10)
                run.font.color.rgb = RGBColor(255, 255, 255)

    # Populate table rows
    row_idx = 1
    for bank_name, bank_data in questions_data.items():
        ticker = bank_data.get("bank_symbol", "")
        for question in bank_data.get("questions", []):
            row = table.rows[row_idx]

            # Column 1: Theme/Category
            row.cells[0].text = question.get("category", "")
            for paragraph in row.cells[0].paragraphs:
                for run in paragraph.runs:
                    run.font.size = Pt(9)
                    run.font.bold = True

            # Column 2: Bank ticker
            row.cells[1].text = ticker
            row.cells[1].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
            for paragraph in row.cells[1].paragraphs:
                for run in paragraph.runs:
                    run.font.size = Pt(9)

            # Column 3: Question text
            row.cells[2].text = question.get("verbatim_question", "")
            for paragraph in row.cells[2].paragraphs:
                for run in paragraph.runs:
                    run.font.size = Pt(9)

            row_idx += 1

    # Add table borders
    tbl = table._element
    tbl_pr = tbl.tblPr
    if tbl_pr is None:
        tbl_pr = parse_xml(r'<w:tblPr {}/> '.format(nsdecls('w')))
        tbl.insert(0, tbl_pr)

    tbl_borders = parse_xml(
        r'<w:tblBorders {}>'
        r'<w:top w:val="single" w:sz="4" w:color="000000"/>'
        r'<w:bottom w:val="single" w:sz="4" w:color="000000"/>'
        r'<w:insideH w:val="single" w:sz="4" w:color="000000"/>'
        r'<w:insideV w:val="single" w:sz="4" w:color="000000"/>'
        r'</w:tblBorders>'.format(nsdecls('w'))
    )
    tbl_pr.append(tbl_borders)


def add_qa_section(doc: Document, questions_data: Dict[str, Any]) -> None:
    """Add Analyst Questions section organized by bank and category."""
    doc.add_heading("Analyst Questions by Category", 1)

    if not questions_data:
        doc.add_paragraph("No relevant analyst questions identified.")
        return

    # questions_data structure: {bank_name: {"bank_symbol": "...", "questions": [...]}}
    # First, organize questions by category across all banks
    categorized_questions = {}
    for bank_name, bank_data in questions_data.items():
        questions = bank_data.get("questions", [])
        for question in questions:
            category = question.get("category", "Other")
            if category not in categorized_questions:
                categorized_questions[category] = []
            # Add bank name to question for display
            question_with_bank = question.copy()
            question_with_bank["bank_name"] = bank_name
            categorized_questions[category].append(question_with_bank)

    # Display by category
    for category, questions in categorized_questions.items():
        # Category heading
        doc.add_heading(category, 2)
        doc.add_paragraph(f"({len(questions)} questions)")

        for q_num, question in enumerate(questions, 1):
            bank = question.get("bank_name", "Unknown Bank")
            analyst = question.get("analyst_name", "Unknown Analyst")
            firm = question.get("analyst_firm", "Unknown Firm")
            verbatim = question.get("verbatim_question", "")

            # Question header
            para = doc.add_paragraph()
            para.add_run(f"Q{q_num}. [{bank}] ").bold = True
            para.add_run(f"{analyst} ({firm})")

            # Verbatim question
            question_para = doc.add_paragraph()
            question_para.style = "Quote"
            question_para.add_run(f'"{verbatim}"')

            # Add spacing
            doc.add_paragraph()


def add_appendix(doc: Document, metadata: Dict[str, Any]) -> None:
    """Add appendix with metadata and technical details."""
    doc.add_page_break()
    doc.add_heading("Appendix", 1)

    doc.add_heading("Document Metadata", 2)

    metadata_items = [
        f"Generation Date: {metadata.get('generation_date', 'N/A')}",
        f"Fiscal Year: {metadata.get('fiscal_year', 'N/A')}",
        f"Quarter: {metadata.get('quarter', 'N/A')}",
        f"Banks Processed: {metadata.get('banks_processed', 0)}",
        f"Total Questions: {metadata.get('total_qas', 0)}",
    ]

    for item in metadata_items:
        doc.add_paragraph(f"• {item}")

    doc.add_heading("Methodology", 2)
    doc.add_paragraph(
        "This document was generated using automated analysis of earnings call transcripts. "
        "Investment Banking and Trading commentary was extracted and paraphrased from "
        "management discussion sections. Analyst questions were categorized based on "
        "predefined capital markets categories and extracted verbatim from Q&A sections."
    )


def structured_data_to_markdown(results: Dict[str, Any]) -> str:
    """
    Convert structured results to markdown format for database storage.

    Args:
        results: Structured results dictionary

    Returns:
        Markdown string
    """
    lines = []

    # Title
    lines.append("# Capital Markets Readthrough")
    lines.append(f"## {results['metadata']['fiscal_year']} {results['metadata']['quarter']}")
    lines.append("")

    # Summary
    lines.append("## Executive Summary")
    lines.append(f"- Banks Analyzed: {results['metadata']['banks_processed']}")
    lines.append(f"- Banks with Outlook: {results['metadata']['banks_with_outlook']}")
    lines.append(f"- Banks with Section 2 Q&A: {results['metadata']['banks_with_section2']}")
    lines.append(f"- Banks with Section 3 Q&A: {results['metadata']['banks_with_section3']}")
    lines.append("")

    # Outlook Section
    lines.append("## Capital Markets Outlook by Bank")
    lines.append("")

    for bank_name, bank_data in results.get("outlook", {}).items():
        lines.append(f"### {bank_name}")
        lines.append("")

        for statement in bank_data.get("statements", []):
            category = statement.get("category", "General")
            statement_text = statement.get("statement", "")
            lines.append(f"- **{category}**: {statement_text}")

        lines.append("")

    # Analyst Questions Section
    lines.append("## Analyst Questions by Category")
    lines.append("")

    # Organize questions by category
    categorized_questions = {}
    for bank_name, bank_data in results.get("questions", {}).items():
        for question in bank_data.get("questions", []):
            category = question.get("category", "Other")
            if category not in categorized_questions:
                categorized_questions[category] = []
            question_with_bank = question.copy()
            question_with_bank["bank_name"] = bank_name
            categorized_questions[category].append(question_with_bank)

    for category, questions in categorized_questions.items():
        lines.append(f"### {category}")
        lines.append(f"*({len(questions)} questions)*")
        lines.append("")

        for question in questions:
            bank = question.get("bank_name", "Unknown")
            analyst = question.get("analyst_name", "Unknown")
            firm = question.get("analyst_firm", "Unknown")
            verbatim = question.get("verbatim_question", "")

            lines.append(f"**[{bank}] {analyst} ({firm}):**")
            lines.append(f"> {verbatim}")
            lines.append("")

    return "\n".join(lines)


def get_standard_report_metadata(
    fiscal_year: int,
    quarter: str,
    report_type: str = "cm_readthrough"
) -> Dict[str, Any]:
    """
    Get standard metadata for report generation.

    Args:
        fiscal_year: Year
        quarter: Quarter
        report_type: Type of report

    Returns:
        Metadata dictionary
    """
    return {
        "report_type": report_type,
        "fiscal_year": fiscal_year,
        "quarter": quarter,
        "generation_date": datetime.now().isoformat(),
        "version": "1.0"
    }