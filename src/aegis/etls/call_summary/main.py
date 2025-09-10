"""
Call Summary ETL Script - Generates call summary reports using direct transcript functions.

This script directly calls the transcripts subagent's internal functions to bypass
the full orchestration layer for efficient ETL processing.

Usage:
    python -m aegis.etls.call_summary.main --bank "Royal Bank of Canada" --year 2024 --quarter Q3
    python -m aegis.etls.call_summary.main --bank RY --year 2024 --quarter Q3
    python -m aegis.etls.call_summary.main --bank 1 --year 2024 --quarter Q3 --output report.txt
"""

import argparse
import json
import sys
import uuid
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
from sqlalchemy import text
import pandas as pd
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.style import WD_STYLE_TYPE
import re
import mistune
from html.parser import HTMLParser
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_LEFT

# Import direct transcript functions
from aegis.model.subagents.transcripts.retrieval import retrieve_full_section
from aegis.model.subagents.transcripts.formatting import (
    format_full_section_chunks,
    generate_research_statement
)
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger

# Initialize logging
setup_logging()
logger = get_logger()


class MarkdownToDocxParser(HTMLParser):
    """
    Custom HTML parser to convert markdown-generated HTML to Word document format.
    """
    def __init__(self, doc):
        super().__init__()
        self.doc = doc
        self.current_paragraph = None
        self.current_run = None
        self.text_stack = []
        self.format_stack = []  # Track bold, italic, etc.
        self.in_list = False
        self.list_type = None
        self.list_counter = 1  # Track numbered list counter
        
    def handle_starttag(self, tag, attrs):
        if tag == 'h1':
            self.current_paragraph = self.doc.add_heading(level=1)
        elif tag == 'h2':
            self.current_paragraph = self.doc.add_heading(level=2)
        elif tag == 'h3':
            self.current_paragraph = self.doc.add_heading(level=3)
        elif tag == 'p':
            self.current_paragraph = self.doc.add_paragraph()
        elif tag == 'strong' or tag == 'b':
            self.format_stack.append('bold')
        elif tag == 'em' or tag == 'i':
            self.format_stack.append('italic')
        elif tag == 'ul':
            self.in_list = True
            self.list_type = 'bullet'
        elif tag == 'ol':
            self.in_list = True
            self.list_type = 'number'
            self.list_counter = 1  # Reset counter for new list
        elif tag == 'li':
            # Create a regular paragraph with manual bullet/number
            self.current_paragraph = self.doc.add_paragraph()
            from docx.shared import Inches
            self.current_paragraph.paragraph_format.left_indent = Inches(0.25)
            
            if self.list_type == 'bullet':
                # Add bullet character manually at the start
                self.current_paragraph.add_run('• ')
            elif self.list_type == 'number':
                # Add number manually at the start
                self.current_paragraph.add_run(f'{self.list_counter}. ')
                self.list_counter += 1
        elif tag == 'br':
            if self.current_paragraph:
                self.current_paragraph.add_run('\n')
                
    def handle_endtag(self, tag):
        if tag in ['h1', 'h2', 'h3', 'p', 'li']:
            self.current_paragraph = None
        elif tag in ['strong', 'b']:
            if 'bold' in self.format_stack:
                self.format_stack.remove('bold')
        elif tag in ['em', 'i']:
            if 'italic' in self.format_stack:
                self.format_stack.remove('italic')
        elif tag in ['ul', 'ol']:
            self.in_list = False
            self.list_type = None
            self.list_counter = 1  # Reset counter
            # Don't add extra spacing - let normal paragraph flow handle it
            
    def handle_data(self, data):
        if not data.strip():
            return
            
        if self.current_paragraph is None:
            self.current_paragraph = self.doc.add_paragraph()
            
        run = self.current_paragraph.add_run(data)
        
        # Apply formatting from stack
        if 'bold' in self.format_stack:
            run.bold = True
        if 'italic' in self.format_stack:
            run.italic = True


def parse_and_add_formatted_text(doc, text: str):
    """
    Parse markdown text and add to Word document with proper formatting.
    Uses mistune to convert markdown to HTML, then parses HTML to Word.
    """
    # Clean up unwanted elements first
    lines = text.split('\n')
    cleaned_lines = []
    
    for line in lines:
        # Skip unwanted headers and dividers
        if line.strip().startswith('###') or line.strip() == '---':
            continue
        cleaned_lines.append(line)
    
    cleaned_text = '\n'.join(cleaned_lines)
    
    # Check if this is a category header (contains colon and matches expected pattern)
    first_line = cleaned_lines[0] if cleaned_lines else ''
    
    # Debug logging for header detection
    logger.debug(f"Checking first line for header: {first_line[:100] if first_line else 'Empty'}")
    
    # More flexible header detection - look for category name pattern with colon
    # Headers typically follow pattern: "Category Name: Dynamic Title"
    is_header = (
        ':' in first_line and 
        len(first_line) < 200 and  # Headers are typically short
        not first_line.strip().startswith('-') and  # Not a list item
        not first_line.strip().startswith('"')  # Not a quote
    )
    
    if is_header:
        # Extract category header
        header_line = first_line.strip().replace('**', '')
        doc.add_heading(header_line, level=2)
        # Don't add extra spacing - Word handles heading spacing
        # Process the rest of the content
        remaining_text = '\n'.join(cleaned_lines[1:])
        if remaining_text.strip():
            # Convert markdown to HTML
            markdown = mistune.create_markdown()
            html_content = markdown(remaining_text)
            
            # Parse HTML and add to Word document
            parser = MarkdownToDocxParser(doc)
            parser.feed(html_content)
    else:
        # No special header, process all content
        if cleaned_text.strip():
            # Convert markdown to HTML
            markdown = mistune.create_markdown()
            html_content = markdown(cleaned_text)
            
            # Parse HTML and add to Word document
            parser = MarkdownToDocxParser(doc)
            parser.feed(html_content)


def save_transcript_content_to_pdf(
    content: str,
    filename: str,
    title: str,
    subtitle: str = "",
    output_dir: Optional[str] = None
) -> str:
    """
    Save formatted transcript content to PDF for verification.
    
    Args:
        content: The formatted transcript content
        filename: Output filename (without .pdf extension)
        title: PDF title
        subtitle: Optional subtitle
        output_dir: Output directory path (defaults to output folder)
    
    Returns:
        Path to the saved PDF file
    """
    # Set up output directory
    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    # Create PDF filepath
    pdf_path = os.path.join(output_dir, f"{filename}.pdf")
    
    # Create PDF document
    doc = SimpleDocTemplate(
        pdf_path,
        pagesize=letter,
        rightMargin=72,
        leftMargin=72,
        topMargin=72,
        bottomMargin=72
    )
    
    # Get styles
    styles = getSampleStyleSheet()
    
    # Create custom styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=16,
        spaceAfter=12,
        alignment=TA_LEFT
    )
    
    subtitle_style = ParagraphStyle(
        'CustomSubtitle',
        parent=styles['Heading2'],
        fontSize=12,
        spaceAfter=24,
        alignment=TA_LEFT
    )
    
    body_style = ParagraphStyle(
        'CustomBody',
        parent=styles['BodyText'],
        fontSize=10,
        leading=14,
        spaceAfter=6
    )
    
    # Build story
    story = []
    
    # Add title
    story.append(Paragraph(title, title_style))
    
    # Add subtitle if provided
    if subtitle:
        story.append(Paragraph(subtitle, subtitle_style))
    
    story.append(Spacer(1, 0.2 * inch))
    
    # Process content - escape XML characters and split into paragraphs
    # Replace problematic characters for XML
    safe_content = content.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    
    # Split content into lines and process
    lines = safe_content.split('\n')
    for line in lines:
        if line.strip():
            # Add each non-empty line as a paragraph
            story.append(Paragraph(line, body_style))
        else:
            # Add small space for empty lines
            story.append(Spacer(1, 0.1 * inch))
    
    # Build PDF
    doc.build(story)
    
    logger.info(
        "etl.call_summary.pdf_saved",
        pdf_path=pdf_path,
        title=title,
        content_length=len(content)
    )
    
    return pdf_path


def load_categories_from_xlsx(bank_type: str) -> List[Dict[str, str]]:
    """
    Load categories from the appropriate XLSX file based on bank type.
    
    Args:
        bank_type: Either "Canadian_Banks" or "US_Banks"
        
    Returns:
        List of dictionaries with transcripts_section, category_name, and category_description
    """
    # Determine which file to use - now matches monitored_institutions.yaml categories
    if bank_type == "Canadian_Banks":
        file_name = "canadian_banks_categories.xlsx"
    elif bank_type == "US_Banks":
        file_name = "us_banks_categories.xlsx"
    else:
        # Default to US if unknown type
        file_name = "us_banks_categories.xlsx"
    
    # Build path to XLSX file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    xlsx_path = os.path.join(current_dir, file_name)
    
    if not os.path.exists(xlsx_path):
        logger.warning(f"Categories file not found: {xlsx_path}, using default ALL section")
        return [{
            "transcripts_section": "ALL",
            "category_name": "Full Transcript Analysis",
            "category_description": "Complete transcript analysis"
        }]
    
    try:
        # Read the Template sheet
        df = pd.read_excel(xlsx_path, sheet_name="Template")
        
        # Convert to list of dictionaries
        categories = df.to_dict('records')
        
        logger.info(f"Loaded {len(categories)} categories from {file_name}")
        return categories
        
    except Exception as e:
        logger.error(f"Error loading categories from {xlsx_path}: {e}")
        return [{
            "transcripts_section": "ALL",
            "category_name": "Full Transcript Analysis", 
            "category_description": "Complete transcript analysis"
        }]


def get_bank_type(bank_id: int) -> str:
    """
    Determine if a bank is Canadian or US based on its ID.
    
    Args:
        bank_id: Bank ID from database
        
    Returns:
        "Canadian_Banks" or "US_Banks"
    """
    # IDs 1-7 are Canadian banks, 8-14 are US banks (based on monitored_institutions.yaml)
    if bank_id <= 7:
        return "Canadian_Banks"
    else:
        return "US_Banks"


def get_bank_info(bank_name: str) -> Dict[str, Any]:
    """
    Look up bank information from the aegis_data_availability table.
    
    Args:
        bank_name: Name, symbol, or ID of the bank
        
    Returns:
        Dictionary with bank_id, bank_name, and bank_symbol
        
    Raises:
        ValueError: If bank not found
    """
    with get_connection() as conn:
        # Check if input is a numeric ID
        if bank_name.isdigit():
            result = conn.execute(text(
                """
                SELECT DISTINCT bank_id, bank_name, bank_symbol
                FROM aegis_data_availability
                WHERE bank_id = :bank_id
                LIMIT 1
                """
            ), {"bank_id": int(bank_name)}).fetchone()
        else:
            # Try exact match first on name or symbol
            result = conn.execute(text(
                """
                SELECT DISTINCT bank_id, bank_name, bank_symbol
                FROM aegis_data_availability
                WHERE LOWER(bank_name) = LOWER(:bank_name)
                   OR LOWER(bank_symbol) = LOWER(:bank_name)
                LIMIT 1
                """
            ), {"bank_name": bank_name}).fetchone()
            
            if not result:
                # Try partial match
                result = conn.execute(text(
                    """
                    SELECT DISTINCT bank_id, bank_name, bank_symbol
                    FROM aegis_data_availability
                    WHERE LOWER(bank_name) LIKE LOWER(:pattern)
                       OR LOWER(bank_symbol) LIKE LOWER(:pattern)
                    LIMIT 1
                    """
                ), {"pattern": f"%{bank_name}%"}).fetchone()
        
        if not result:
            # List available banks for user
            available = conn.execute(text(
                """
                SELECT DISTINCT bank_symbol, bank_name
                FROM aegis_data_availability
                ORDER BY bank_symbol
                """
            )).fetchall()
            
            bank_list = "\n".join([f"  - {r.bank_symbol}: {r.bank_name}" for r in available])
            raise ValueError(
                f"Bank '{bank_name}' not found. Available banks:\n{bank_list}"
            )
        
        return {
            "bank_id": result.bank_id,
            "bank_name": result.bank_name,
            "bank_symbol": result.bank_symbol
        }


def verify_data_availability(bank_id: int, fiscal_year: int, quarter: str) -> bool:
    """
    Check if transcript data is available for the specified bank and period.
    
    Args:
        bank_id: Bank ID
        fiscal_year: Year (e.g., 2024)
        quarter: Quarter (e.g., "Q3")
        
    Returns:
        True if transcript data is available, False otherwise
    """
    with get_connection() as conn:
        result = conn.execute(text(
            """
            SELECT database_names
            FROM aegis_data_availability
            WHERE bank_id = :bank_id
              AND fiscal_year = :fiscal_year
              AND quarter = :quarter
            """
        ), {
            "bank_id": bank_id,
            "fiscal_year": fiscal_year,
            "quarter": quarter
        }).fetchone()
        
        if result and result.database_names:
            return 'transcripts' in result.database_names
        
        return False


def generate_call_summary(
    bank_name: str,
    fiscal_year: int,
    quarter: str
) -> str:
    """
    Generate a call summary by directly calling transcript functions.
    
    Args:
        bank_name: ID, name, or symbol of the bank
        fiscal_year: Year (e.g., 2024)
        quarter: Quarter (e.g., "Q3")
        
    Returns:
        The generated call summary content
    """
    execution_id = str(uuid.uuid4())
    logger.info(
        "etl.call_summary.started",
        execution_id=execution_id,
        bank_name=bank_name,
        fiscal_year=fiscal_year,
        quarter=quarter
    )
    
    try:
        # Step 1: Look up bank information
        bank_info = get_bank_info(bank_name)
        logger.info(
            "etl.call_summary.bank_found",
            execution_id=execution_id,
            bank_id=bank_info["bank_id"],
            bank_name=bank_info["bank_name"],
            bank_symbol=bank_info["bank_symbol"]
        )
        
        # Step 2: Verify data availability
        if not verify_data_availability(bank_info["bank_id"], fiscal_year, quarter):
            error_msg = f"No transcript data available for {bank_info['bank_name']} {quarter} {fiscal_year}"
            logger.warning(
                "etl.call_summary.no_data",
                execution_id=execution_id,
                message=error_msg
            )
            
            # Check what periods are available
            with get_connection() as conn:
                available_periods = conn.execute(text(
                    """
                    SELECT DISTINCT fiscal_year, quarter
                    FROM aegis_data_availability
                    WHERE bank_id = :bank_id
                      AND 'transcripts' = ANY(database_names)
                    ORDER BY fiscal_year DESC, quarter DESC
                    LIMIT 10
                    """
                ), {"bank_id": bank_info["bank_id"]}).fetchall()
                
                if available_periods:
                    period_list = ", ".join([f"{p.quarter} {p.fiscal_year}" for p in available_periods])
                    error_msg += f"\n\nAvailable periods for {bank_info['bank_name']}: {period_list}"
            
            return f"⚠️ {error_msg}"
        
        # Step 3: Setup context for function calls
        ssl_config = setup_ssl()
        auth_config = setup_authentication(execution_id, ssl_config)
        
        if not auth_config["success"]:
            error_msg = f"Authentication failed: {auth_config.get('error', 'Unknown error')}"
            logger.error(
                "etl.call_summary.auth_failed",
                execution_id=execution_id,
                error=error_msg
            )
            return f"⚠️ {error_msg}"
        
        context = {
            "execution_id": execution_id,
            "auth_config": auth_config,
            "ssl_config": ssl_config
        }
        
        # Step 4: Create bank-period combination
        combo = {
            "bank_id": bank_info["bank_id"],
            "bank_name": bank_info["bank_name"],
            "bank_symbol": bank_info["bank_symbol"],
            "fiscal_year": fiscal_year,
            "quarter": quarter,
            "query_intent": "ETL structured extraction"
        }
        
        logger.info(
            "etl.call_summary.retrieving_transcript",
            execution_id=execution_id,
            combo=combo
        )
        
        # Step 5: Load categories based on bank type
        bank_type = get_bank_type(bank_info["bank_id"])
        categories = load_categories_from_xlsx(bank_type)
        
        logger.info(
            "etl.call_summary.categories_loaded",
            execution_id=execution_id,
            bank_type=bank_type,
            num_categories=len(categories)
        )
        
        # Step 6: FIRST STAGE - Generate Research Plan
        # Pull ALL sections from transcript
        combo = {
            "bank_id": bank_info["bank_id"],
            "bank_name": bank_info["bank_name"],
            "bank_symbol": bank_info["bank_symbol"],
            "fiscal_year": fiscal_year,
            "quarter": quarter,
            "query_intent": "Generate comprehensive research plan for earnings call summary"
        }
        
        logger.info(
            "etl.call_summary.retrieving_full_transcript",
            execution_id=execution_id,
            combo=combo
        )
        
        # Retrieve ALL transcript sections
        chunks = retrieve_full_section(
            combo=combo,
            sections="ALL",  # Get complete transcript
            context=context
        )
        
        if not chunks:
            return f"⚠️ No transcript chunks found for {bank_info['bank_name']} {quarter} {fiscal_year}"
        
        logger.info(
            "etl.call_summary.full_transcript_retrieved",
            execution_id=execution_id,
            num_chunks=len(chunks)
        )
        
        # Format the complete transcript
        formatted_transcript = format_full_section_chunks(
            chunks=chunks,
            combo=combo,
            context=context
        )
        
        logger.info(
            "etl.call_summary.transcript_formatted",
            execution_id=execution_id,
            content_length=len(formatted_transcript)
        )
        
        # Save Stage 1 transcript content to PDF for verification
        save_transcript_content_to_pdf(
            content=formatted_transcript,
            filename=f"{bank_info['bank_symbol']}_{fiscal_year}_{quarter}_Stage1_ALL_{execution_id[:8]}",
            title=f"Stage 1: Full Transcript - ALL Sections",
            subtitle=f"{bank_info['bank_name']} ({bank_info['bank_symbol']}) - {quarter} {fiscal_year}"
        )
        
        # Build CO-STAR+XML prompt for research plan
        costar_prompt = f"""<context>
You are creating a comprehensive earnings call summary report for {bank_info['bank_name']} ({bank_info['bank_symbol']}) for {quarter} {fiscal_year}.
You have been provided with the complete earnings call transcript.
</context>

<objective>
Create a RESEARCH PLAN that organizes and structures how the transcript content should be distributed across report categories.
This is an organizational exercise only - DO NOT extract actual values, numbers, or verbatim quotes.
</objective>

<style>
- Focus on content organization and structure, not extraction
- Identify topics and themes without providing specific values
- Note speaker roles and discussion sections without quoting
- Map relationships between categories to avoid duplication
- Provide directional guidance for the extraction phase
</style>

<task>
Review the transcript and create a research plan for each category below. Each category has specific instructions and a designated section source (MD only, QA only, or ALL). Your plan should explain how to extract and organize content for that category while respecting its section constraints.

Categories to plan for:
"""
        
        # Add indexed categories to the prompt with section context
        for i, category in enumerate(categories, 1):
            # Determine section context description
            if category['transcripts_section'] == 'MD':
                section_context = "using ONLY the Management Discussion section"
            elif category['transcripts_section'] == 'QA':
                section_context = "using ONLY the Q&A section"
            else:  # ALL
                section_context = "using BOTH Management Discussion and Q&A sections"
            
            costar_prompt += f"""
<category index="{i}">
<name>{category['category_name']}</name>
<section_source>{section_context}</section_source>
<instructions>{category['category_description']}</instructions>
</category>
"""
        
        costar_prompt += f"""
</task>

<audience>
Financial analysts and investors who need structured, comprehensive analysis of earnings calls.
</audience>

<response_format>
CRITICAL: You MUST use this EXACT XML structure for EVERY category. This format is required for parsing.

For EACH of the {len(categories)} categories above, provide EXACTLY this structure:

<category_plan index="[NUMBER]">
<name>[CATEGORY NAME - must match exactly from list above]</name>
<research_plan>
[A paragraph describing the research plan for this category. Based on the category instructions and section source, explain what topics to look for, what types of metrics to extract (without values), which speakers or discussions to reference, and how this content should be organized. Note any overlaps with other categories to avoid duplication. Focus on planning the extraction and organization, not writing the actual content. Remember to respect the section_source constraint - if it says "ONLY Management Discussion" then do not plan to use Q&A content, and vice versa.]
</research_plan>
</category_plan>

REQUIREMENTS:
1. You MUST provide EXACTLY {len(categories)} category_plan blocks
2. Index numbers MUST be sequential from 1 to {len(categories)}
3. Category names MUST match exactly as provided in the list above
4. Research plan MUST be in paragraph format (not bullet points)
5. Research plan MUST respect the section_source constraint (MD only, QA only, or ALL)
6. Do NOT include actual numbers, percentages, dollar amounts, or verbatim quotes
7. Do NOT add any text outside the XML structures

This structured format is required for automated parsing. Non-compliance will cause processing errors.
</response_format>

Based on the transcript above, create the research plan:"""
        
        # Generate the research plan
        research_plan = generate_research_statement(
            formatted_content=formatted_transcript,
            combo=combo,
            context=context,
            method=0,  # Full retrieval
            method_reasoning="ETL Stage 1: Research Plan Generation",
            custom_prompt=costar_prompt
        )
        
        logger.info(
            "etl.call_summary.research_plan_generated",
            execution_id=execution_id,
            plan_length=len(research_plan)
        )
        
        # Step 7: SECOND STAGE - Generate each category section using the research plan
        category_results = []
        
        for i, category in enumerate(categories, 1):
            logger.info(
                "etl.call_summary.processing_category",
                execution_id=execution_id,
                category_index=i,
                category_name=category["category_name"],
                section=category["transcripts_section"]
            )
            
            # Retrieve chunks for this specific category's section
            chunks = retrieve_full_section(
                combo=combo,
                sections=category["transcripts_section"],  # MD, QA, or ALL
                context=context
            )
            
            if not chunks:
                logger.warning(
                    "etl.call_summary.no_chunks_for_category",
                    execution_id=execution_id,
                    category_name=category["category_name"]
                )
                category_results.append({
                    "category_name": category["category_name"],
                    "content": f"No {category['transcripts_section']} section data available for this category."
                })
                continue
            
            # Format the chunks for this category
            formatted_section = format_full_section_chunks(
                chunks=chunks,
                combo=combo,
                context=context
            )
            
            # Save Stage 2 transcript content to PDF for each category
            save_transcript_content_to_pdf(
                content=formatted_section,
                filename=f"{bank_info['bank_symbol']}_{fiscal_year}_{quarter}_Stage2_Cat{i:02d}_{category['transcripts_section']}_{execution_id[:8]}",
                title=f"Stage 2 Category {i}: {category['category_name']}",
                subtitle=f"Section: {category['transcripts_section']} | {bank_info['bank_name']} - {quarter} {fiscal_year}"
            )
            
            # Build context from previous research outputs
            previous_research = "\n\n".join([
                f"[Category {j}: {res['category_name']}]\n{res['content']}" 
                for j, res in enumerate(category_results, 1)
            ]) if category_results else "No previous sections completed yet."
            
            # Create CO-STAR+XML prompt for this specific category
            category_prompt = f"""<context>
You are continuing to write section {i} of {len(categories)} in an ongoing report.
The bank and period context ({bank_info['bank_name']}, {quarter} {fiscal_year}) is already established.
Do NOT repeat the bank name or period in your opening or closing.
</context>

<objective>
Continue the report by writing the next section for this category.
Provide rich content with quotes and insights that flow naturally from previous sections.
Do NOT reintroduce the bank or restate the context - dive directly into the content.
Do NOT end with a summary paragraph that repeats the bank name or period.
</objective>

<style>
- Start with category name and a dynamic title based on the content
- Provide short summary statements to introduce topics
- Include substantial direct quotes with speaker attribution
- Mix paraphrased content with verbatim quotes
- Focus on extracting key statements and insights
- Keep summaries concise - let quotes carry the detail
</style>

<task>
Current Category: {category['category_name']}
Category Instructions: {category['category_description']}
Section Source: {category['transcripts_section']} section only

Research Plan for this Category:
{[plan for plan in research_plan.split('</category_plan>') if f"<name>{category['category_name']}</name>" in plan][0] if research_plan else "No plan available"}

Previous Sections Completed:
{previous_research}

Based on the transcript section above, generate the research output for this category.
</task>

<audience>
Financial analysts requiring detailed earnings call analysis with supporting quotes and evidence.
</audience>

<response_format>
IMPORTANT: Do NOT include:
- Headers like "### Royal Bank of Canada - Q2 2025" 
- Section breaks like "---"
- Any bank/period headers - just start with the category

MANDATORY FORMAT - Your response MUST begin EXACTLY like this:
**{category['category_name']}: [Your Dynamic Title]**

The double asterisks ** are ABSOLUTELY REQUIRED at start and end.
DO NOT FORGET THE ** MARKERS!

Example of correct format:
**{category['category_name']}: Strong Performance Driven by Capital Markets**

Then continue with:
[Opening paragraph with key summary points. Use **bold** for important metrics or key terms.]

[Topic area with context], followed by relevant quotes:

- "[Direct quote with specific detail]" - *Speaker Name, Title*
- Management noted that [paraphrased content with **key points** highlighted]

[Continue pattern of summary + quotes for each major topic]

Use proper markdown formatting:
- **bold** for important numbers, metrics, and key terms
- *italic* for speaker names and emphasis
- Bullet lists: ensure blank line before list, then use "- " for each item
- For multiple quotes, create proper markdown lists with blank lines before/after

CRITICAL RULES:
- Start DIRECTLY with **Category Name: Title** format
- NO opening like "Royal Bank of Canada reported..."
- NO closing like "In summary, Royal Bank's Q2 2025..."
- Let content flow naturally into the next section
- Focus on quotes and insights, not repetitive summaries
- Write as a continuation of the report, not a standalone piece
</response_format>

Generate the research content for this category:"""
            
            # Generate research for this category
            category_research = generate_research_statement(
                formatted_content=formatted_section,
                combo=combo,
                context=context,
                method=0,
                method_reasoning=f"ETL Stage 2: Category {i} - {category['category_name']}",
                custom_prompt=category_prompt
            )
            
            # Log a sample of the markdown for debugging list formatting
            logger.debug(
                "etl.call_summary.category_markdown_sample",
                execution_id=execution_id,
                category_name=category["category_name"],
                markdown_sample=category_research[:500] if category_research else "Empty"
            )
            
            category_results.append({
                "category_name": category["category_name"],
                "content": category_research
            })
            
            logger.info(
                "etl.call_summary.category_completed",
                execution_id=execution_id,
                category_index=i,
                category_name=category["category_name"],
                content_length=len(category_research)
            )
        
        # Step 8: Generate Word Document
        logger.info(
            "etl.call_summary.generating_document",
            execution_id=execution_id,
            num_sections=len(category_results)
        )
        
        # Create Word document
        doc = Document()
        
        # Add title
        title = doc.add_heading(f'{bank_info["bank_name"]} ({bank_info["bank_symbol"]})', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        # Add subtitle
        subtitle = doc.add_heading(f'Earnings Call Summary - {quarter} {fiscal_year}', 1)
        subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        # Add spacing after title
        doc.add_paragraph()
        
        # Add each category section
        for i, result in enumerate(category_results, 1):
            # Use the parsing function to add formatted content
            parse_and_add_formatted_text(doc, result['content'])
            
            # Add minimal spacing between sections (if not last section)
            if i < len(category_results):
                doc.add_paragraph()
        
        # Save the document
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        filename = f"{bank_info['bank_symbol']}_{fiscal_year}_{quarter}_{execution_id[:8]}.docx"
        filepath = os.path.join(output_dir, filename)
        doc.save(filepath)
        
        logger.info(
            "etl.call_summary.document_saved",
            execution_id=execution_id,
            filepath=filepath
        )
        
        logger.info(
            "etl.call_summary.completed",
            execution_id=execution_id,
            stage="full_report",
            num_categories=len(category_results)
        )
        
        # Return summary output
        output = f"""
================================================================================
CALL SUMMARY ETL REPORT - COMPLETE
================================================================================
Bank: {bank_info['bank_name']} ({bank_info['bank_symbol']})
Period: {quarter} {fiscal_year}
Generated: {datetime.now().isoformat()}
Execution ID: {execution_id}
Bank Type: {bank_type}
Categories Processed: {len(category_results)}
================================================================================

RESEARCH PLAN GENERATED:
{len(research_plan)} characters

SECTIONS COMPLETED:
"""
        for i, result in enumerate(category_results, 1):
            output += f"\n{i}. {result['category_name']} - {len(result['content'])} characters"
        
        output += f"""

WORD DOCUMENT SAVED:
{filepath}

================================================================================
END OF REPORT
================================================================================
"""
        
        return output
        
    except Exception as e:
        error_msg = f"Error generating call summary: {str(e)}"
        logger.error(
            "etl.call_summary.error",
            execution_id=execution_id,
            error=error_msg,
            exc_info=True
        )
        return f"❌ {error_msg}"


def main():
    """Main entry point for command-line execution."""
    parser = argparse.ArgumentParser(
        description="Generate call summary reports using direct transcript function calls",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using bank symbol
  python -m aegis.etls.call_summary.main --bank RY --year 2024 --quarter Q3
  
  # Using bank name
  python -m aegis.etls.call_summary.main --bank "Toronto-Dominion Bank" --year 2024 --quarter Q2
  
  # Using bank ID
  python -m aegis.etls.call_summary.main --bank 2 --year 2024 --quarter Q3
  
  # Save to file
  python -m aegis.etls.call_summary.main --bank 1 --year 2024 --quarter Q3 --output report.txt
        """
    )
    
    parser.add_argument(
        "--bank",
        required=True,
        help="Bank ID, name, or symbol (e.g., '1', 'Royal Bank of Canada', 'RY')"
    )
    
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Fiscal year (e.g., 2024)"
    )
    
    parser.add_argument(
        "--quarter",
        required=True,
        choices=["Q1", "Q2", "Q3", "Q4"],
        help="Quarter (Q1, Q2, Q3, Q4)"
    )
    
    
    parser.add_argument(
        "--output",
        help="Optional output file path (defaults to stdout)"
    )
    
    args = parser.parse_args()
    
    # Generate the call summary
    print(f"\n🔄 Generating report for {args.bank} {args.quarter} {args.year}...\n")
    
    result = generate_call_summary(
        bank_name=args.bank,
        fiscal_year=args.year,
        quarter=args.quarter
    )
    
    # Output the result
    if args.output:
        with open(args.output, 'w') as f:
            f.write(result)
        print(f"✅ Report saved to: {args.output}")
    else:
        print(result)


if __name__ == "__main__":
    main()