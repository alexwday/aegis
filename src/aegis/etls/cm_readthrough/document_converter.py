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
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from aegis.utils.logging import get_logger

logger = get_logger()


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


def create_combined_document(results: Dict[str, Any], output_path: str) -> None:
    """
    Create a combined Word document for CM Readthrough analysis.

    Args:
        results: Structured results from processing
        output_path: Path to save the document
    """
    # Create document
    doc = Document()

    # Set landscape orientation and margins
    sections = doc.sections
    for section in sections:
        # Set to landscape
        section.orientation = WD_ORIENTATION.LANDSCAPE
        # Swap width and height for landscape
        section.page_width, section.page_height = section.page_height, section.page_width

        # Set margins (adjusted for landscape)
        section.top_margin = Inches(0.75)
        section.bottom_margin = Inches(0.75)
        section.left_margin = Inches(1)
        section.right_margin = Inches(1)

    # Add title page
    add_title_page(doc, results["metadata"])

    # Add executive summary
    add_executive_summary(doc, results)

    # Add table of contents placeholder
    doc.add_page_break()
    doc.add_heading("Table of Contents", 1)
    doc.add_paragraph("(Table of Contents will be generated here)")

    # Section 1: Investment Banking & Trading Outlook
    doc.add_page_break()
    add_ib_trading_section(doc, results.get("ib_trading_outlook", {}))

    # Section 2: Analyst Questions by Category
    doc.add_page_break()
    add_qa_section(doc, results.get("categorized_qas", {}))

    # Add appendix if needed
    add_appendix(doc, results["metadata"])

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


def add_ib_trading_section(doc: Document, ib_trading_data: Dict[str, Any]) -> None:
    """Add Investment Banking & Trading Outlook section."""
    doc.add_heading("Investment Banking & Trading Outlook", 1)

    if not ib_trading_data:
        doc.add_paragraph("No Investment Banking or Trading commentary identified.")
        return

    for bank_name, insights in ib_trading_data.items():
        # Bank heading
        doc.add_heading(bank_name, 2)

        # Investment Banking insights
        if insights.get("investment_banking"):
            doc.add_heading("Investment Banking", 3)
            for item in insights["investment_banking"]:
                topic = item.get("topic", "General")
                insight = item.get("insight", "")
                context = item.get("context", "")

                para = doc.add_paragraph()
                para.add_run(f"{topic}: ").bold = True
                para.add_run(insight)

                if context:
                    para.add_run(f" ({context})").font.italic = True

        # Trading outlook
        if insights.get("trading_outlook"):
            doc.add_heading("Trading Outlook", 3)
            for item in insights["trading_outlook"]:
                topic = item.get("topic", "General")
                insight = item.get("insight", "")
                context = item.get("context", "")

                para = doc.add_paragraph()
                para.add_run(f"{topic}: ").bold = True
                para.add_run(insight)

                if context:
                    para.add_run(f" ({context})").font.italic = True

        # Market conditions
        if insights.get("market_conditions"):
            doc.add_heading("Market Conditions", 3)
            for item in insights["market_conditions"]:
                condition = item.get("condition", "")
                impact = item.get("impact", "")

                para = doc.add_paragraph()
                para.add_run(f"Condition: ").bold = True
                para.add_run(condition)
                doc.add_paragraph(f"Impact: {impact}")


def add_qa_section(doc: Document, categorized_qas: Dict[str, List[Dict[str, Any]]]) -> None:
    """Add Analyst Questions section organized by category."""
    doc.add_heading("Analyst Questions by Category", 1)

    if not categorized_qas:
        doc.add_paragraph("No relevant analyst questions identified.")
        return

    for category, questions in categorized_qas.items():
        # Category heading
        doc.add_heading(category, 2)
        doc.add_paragraph(f"({len(questions)} questions)")

        for q_num, question in enumerate(questions, 1):
            bank = question.get("bank_name", "Unknown Bank")
            analyst = question.get("analyst_name", "Unknown Analyst")
            firm = question.get("analyst_firm", "Unknown Firm")
            verbatim = question.get("verbatim_question", "")
            topics = question.get("key_topics", [])

            # Question header
            para = doc.add_paragraph()
            para.add_run(f"Q{q_num}. [{bank}] ").bold = True
            para.add_run(f"{analyst} ({firm})")

            # Verbatim question
            question_para = doc.add_paragraph()
            question_para.style = "Quote"
            question_para.add_run(f'"{verbatim}"')

            # Key topics if available
            if topics:
                topics_para = doc.add_paragraph()
                topics_para.add_run("Topics: ").font.italic = True
                topics_para.add_run(", ".join(topics)).font.italic = True

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
    lines.append(f"- Total Questions: {results['metadata']['total_qas']}")
    lines.append("")

    # IB & Trading Outlook
    lines.append("## Investment Banking & Trading Outlook")
    lines.append("")

    for bank_name, insights in results.get("ib_trading_outlook", {}).items():
        lines.append(f"### {bank_name}")
        lines.append("")

        if insights.get("investment_banking"):
            lines.append("#### Investment Banking")
            for item in insights["investment_banking"]:
                lines.append(f"- **{item.get('topic', 'General')}**: {item.get('insight', '')}")
            lines.append("")

        if insights.get("trading_outlook"):
            lines.append("#### Trading Outlook")
            for item in insights["trading_outlook"]:
                lines.append(f"- **{item.get('topic', 'General')}**: {item.get('insight', '')}")
            lines.append("")

    # Analyst Questions
    lines.append("## Analyst Questions by Category")
    lines.append("")

    for category, questions in results.get("categorized_qas", {}).items():
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