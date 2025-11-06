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
import asyncio
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
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
import re
import hashlib
from difflib import SequenceMatcher

# Import document converter functions
from aegis.etls.call_summary.document_converter import (
    convert_docx_to_pdf,
    structured_data_to_markdown,
    get_standard_report_metadata
)

# Import direct transcript functions
from aegis.model.subagents.transcripts.retrieval import retrieve_full_section
from aegis.model.subagents.transcripts.formatting import format_full_section_chunks
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication
from aegis.connections.llm_connector import complete_with_tools
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger
from aegis.utils.settings import config
from aegis.utils.prompt_loader import load_prompt_from_db
from aegis.utils.sql_prompt import postgresql_prompts
from aegis.etls.call_summary.config.config import MODELS, TEMPERATURE, MAX_TOKENS

# Initialize logging
setup_logging()
logger = get_logger()

# =============================================================================
# MODEL CONFIGURATION - QUALITY OPTIMIZED
# =============================================================================
# Model selection optimized for maximum output quality.
# Set to None to use the default model from config.
# 
# QUALITY-FIRST STRATEGY:
# - Research Planning: Comprehensive analysis → Use high-capability models
# - Category Extraction: Deep synthesis → Use best available models
# - Focus on insight quality over token cost
# 
# Recommended models by capability:
# - o1/o3: Superior reasoning and synthesis (best for complex analysis)
# - o1-mini: Excellent reasoning with faster response
# - gpt-4-turbo/gpt-4o: High quality with good speed
# - gpt-4o-mini: Fast but may miss nuances
# =============================================================================

MODEL_OVERRIDES = {
    # Research Planning: Comprehensive content mapping and strategy
    "RESEARCH_PLAN_MODEL": None,  # Recommended: "gpt-4o" or "o1-mini" for thorough analysis
    
    # Category Extraction: Maximum quality synthesis and insight generation
    "CATEGORY_EXTRACTION_MODEL": None,  # Recommended: "o1-mini" or "o1" for best quality
}

# Category-specific model overrides for specialized content
# Use highest capability models for complex financial analysis
CATEGORY_MODEL_OVERRIDES = {
    # Complex financial analysis benefits from O-series reasoning
    # "Financial Performance & Metrics": "o1",
    # "Credit Quality & Risk Metrics": "o1",
    # "Capital & Liquidity Position": "o1-mini",
    # "Business Segment Performance": "o1-mini",
    
    # Standard categories can use GPT-4 variants
    # "Forward Guidance & Outlook": "gpt-4o",
    # "Key Risks & Challenges": "gpt-4o",
}

# Helper function to get model for each stage
def get_model_for_stage(stage: str, category_name: Optional[str] = None) -> str:
    """
    Get the model to use for a specific stage, with override support.

    Args:
        stage: The processing stage (RESEARCH_PLAN_MODEL, CATEGORY_EXTRACTION_MODEL)
        category_name: Optional category name for fine-grained model selection

    Returns:
        Model name to use
    """
    # Check for category-specific override first (highest priority)
    if category_name and stage == "CATEGORY_EXTRACTION_MODEL":
        category_override = CATEGORY_MODEL_OVERRIDES.get(category_name)
        if category_override:
            logger.info(f"Using category-specific model for '{category_name}': {category_override}")
            return category_override

    # Check for stage-level override
    stage_override = MODEL_OVERRIDES.get(stage)
    if stage_override:
        logger.info(f"Using stage override model for {stage}: {stage_override}")
        return stage_override

    # Map stage names to config model keys
    stage_to_config_map = {
        "RESEARCH_PLAN_MODEL": "research_plan",
        "CATEGORY_EXTRACTION_MODEL": "category_extraction",
    }

    # Use stage-specific config model if available
    config_key = stage_to_config_map.get(stage, "summarization")
    model = MODELS.get(config_key, MODELS["summarization"])
    logger.debug(f"Using configured model for {stage}: {model} (config key: {config_key})")
    return model


# Legacy YAML loader functions - kept for reference but no longer used
def load_research_plan_config_yaml():
    """DEPRECATED: Load the research plan prompt and tool definition from YAML file."""
    prompt_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'prompts', 'category_extraction_prompt.yaml'
    )
    with open(prompt_path, 'r') as f:
        config = yaml.safe_load(f)
    return {
        'system_template': config['system_template'],
        'tool': config['tool']
    }


def add_page_numbers(doc):
    """Add page numbers to the footer of the document."""
    for section in doc.sections:
        footer = section.footer
        
        # Clear existing footer content
        footer.paragraphs[0].clear()
        
        # Create a paragraph for the page number
        footer_para = footer.paragraphs[0]
        footer_para.alignment = WD_ALIGN_PARAGRAPH.RIGHT
        
        # Add page number field
        run = footer_para.add_run()
        fldChar1 = OxmlElement('w:fldChar')
        fldChar1.set(qn('w:fldCharType'), 'begin')
        run._element.append(fldChar1)
        
        instrText = OxmlElement('w:instrText')
        instrText.text = 'PAGE'
        run._element.append(instrText)
        
        fldChar2 = OxmlElement('w:fldChar')
        fldChar2.set(qn('w:fldCharType'), 'end')
        run._element.append(fldChar2)


def setup_toc_styles(doc):
    """Setup custom TOC styles for formatting."""
    styles = doc.styles
    
    # Try to modify TOC 1 style (Level 1 - main sections)
    try:
        toc1_style = styles['TOC 1']
    except KeyError:
        # Create TOC 1 style if it doesn't exist
        toc1_style = styles.add_style('TOC 1', WD_STYLE_TYPE.PARAGRAPH)
    
    toc1_style.font.size = Pt(8)  # Reduced from 9pt
    toc1_style.font.bold = True   # Bold for main sections
    toc1_style.paragraph_format.space_before = Pt(1)  # Minimal space before
    toc1_style.paragraph_format.space_after = Pt(1)   # Minimal space after
    toc1_style.paragraph_format.line_spacing = 0.9    # Tighter line spacing
    
    # Try to modify TOC 2 style (Level 2 - subsections)
    try:
        toc2_style = styles['TOC 2']
    except KeyError:
        # Create TOC 2 style if it doesn't exist
        toc2_style = styles.add_style('TOC 2', WD_STYLE_TYPE.PARAGRAPH)
    
    toc2_style.font.size = Pt(7)  # Reduced from 8pt
    toc2_style.font.bold = False  # Not bold for subsections
    toc2_style.paragraph_format.left_indent = Inches(0.2)  # Slightly less indent
    toc2_style.paragraph_format.space_before = Pt(0)  # No space before
    toc2_style.paragraph_format.space_after = Pt(0.5) # Minimal space after
    toc2_style.paragraph_format.line_spacing = 0.9    # Tighter line spacing


def add_table_of_contents(doc):
    """Add a real table of contents field to the document."""
    # Setup TOC styles first
    setup_toc_styles(doc)
    
    # Add TOC heading as a regular paragraph with bold formatting (not a heading level)
    toc_title = doc.add_paragraph()
    toc_title_run = toc_title.add_run('Contents')
    toc_title_run.font.size = Pt(10)  # Reduced from 11pt
    toc_title_run.font.bold = True
    toc_title.alignment = WD_ALIGN_PARAGRAPH.LEFT
    toc_title.paragraph_format.space_after = Pt(3)  # Reduced space before TOC entries
    
    # Add the actual TOC field
    paragraph = doc.add_paragraph()
    paragraph.paragraph_format.line_spacing = 1.0  # Tighter line spacing
    run = paragraph.add_run()
    
    # Create the TOC field code
    fldChar = OxmlElement('w:fldChar')
    fldChar.set(qn('w:fldCharType'), 'begin')
    # Set the dirty attribute to force update on open
    fldChar.set(qn('w:dirty'), 'true')
    
    instrText = OxmlElement('w:instrText')
    instrText.set(qn('xml:space'), 'preserve')
    instrText.text = 'TOC \\o "1-2" \\h \\z \\u'  # TOC for heading levels 1-2 with hyperlinks
    
    fldChar2 = OxmlElement('w:fldChar')
    fldChar2.set(qn('w:fldCharType'), 'separate')
    
    # Add placeholder text to keep the field visible
    fldChar3 = OxmlElement('w:t')
    fldChar3.text = "[Table of Contents will be generated here]"
    
    fldChar4 = OxmlElement('w:fldChar')
    fldChar4.set(qn('w:fldCharType'), 'end')
    
    r_element = run._element
    r_element.append(fldChar)
    r_element.append(instrText)
    r_element.append(fldChar2)
    r_element.append(fldChar3)
    r_element.append(fldChar4)
    
    # Add smaller font size to the TOC placeholder
    run.font.size = Pt(8)
    run.font.color.rgb = RGBColor(128, 128, 128)  # Gray color for placeholder
    
    # Add page break after TOC
    doc.add_page_break()


def mark_document_for_update(doc):
    """Mark the document settings to update fields on open."""
    # Access the document settings
    settings = doc.settings.element
    
    # Create updateFields element if it doesn't exist
    updateFields = OxmlElement('w:updateFields')
    updateFields.set(qn('w:val'), 'true')
    
    # Add to settings
    settings.append(updateFields)


def parse_and_format_text(paragraph, text: str, base_font_size=None, base_color=None, base_italic=False) -> None:
    """
    Parse markdown-style formatting and add formatted runs to paragraph.
    Supports **bold** for emphasis and __underline__ for important phrases.
    
    Args:
        paragraph: Word paragraph object to add formatted text to
        text: Text containing markdown formatting
        base_font_size: Base font size for all runs (optional)
        base_color: Base RGB color for all runs (optional) 
        base_italic: Whether base text should be italic (default False)
    """
    import re
    
    # Pattern to match bold (**text**) and underline (__text__) markdown
    # This handles nested cases and ensures proper matching
    pattern = r'(\*\*[^*]+\*\*|__[^_]+__|[^*_]+)'
    
    # If text doesn't contain any formatting, add it as a single run
    if '**' not in text and '__' not in text:
        run = paragraph.add_run(text)
        if base_font_size:
            run.font.size = base_font_size
        if base_color:
            run.font.color.rgb = base_color
        run.italic = base_italic
        return
    
    # Process the text with formatting
    remaining_text = text
    while remaining_text:
        # Try to find the next formatted section
        bold_match = re.search(r'\*\*([^*]+)\*\*', remaining_text)
        underline_match = re.search(r'__([^_]+)__', remaining_text)
        
        # Determine which comes first
        next_match = None
        match_type = None
        
        if bold_match and underline_match:
            if bold_match.start() < underline_match.start():
                next_match = bold_match
                match_type = 'bold'
            else:
                next_match = underline_match
                match_type = 'underline'
        elif bold_match:
            next_match = bold_match
            match_type = 'bold'
        elif underline_match:
            next_match = underline_match
            match_type = 'underline'
        
        if next_match:
            # Add any text before the match
            if next_match.start() > 0:
                run = paragraph.add_run(remaining_text[:next_match.start()])
                if base_font_size:
                    run.font.size = base_font_size
                if base_color:
                    run.font.color.rgb = base_color
                run.italic = base_italic
            
            # Add the formatted text
            run = paragraph.add_run(next_match.group(1))
            if base_font_size:
                run.font.size = base_font_size
            if base_color:
                run.font.color.rgb = base_color
            run.italic = base_italic
            
            # Apply the formatting
            if match_type == 'bold':
                run.bold = True
            elif match_type == 'underline':
                run.underline = True
            
            # Move to the remaining text
            remaining_text = remaining_text[next_match.end():]
        else:
            # No more formatting, add the rest as plain text
            run = paragraph.add_run(remaining_text)
            if base_font_size:
                run.font.size = base_font_size
            if base_color:
                run.font.color.rgb = base_color
            run.italic = base_italic
            break


def add_structured_content_to_doc(doc, category_data: dict, heading_level: int = 2) -> None:
    """
    Add structured category data directly to Word document with proper formatting.
    
    Args:
        doc: Word document object
        category_data: Dictionary with title, summary_statements, evidence structure
        heading_level: Heading level for category title (default 2)
    """
    if category_data.get('rejected', False):
        return  # Skip rejected categories
    
    try:
        # Add the category title as a heading
        heading = doc.add_heading(category_data['title'], level=heading_level)
        # Compact heading formatting for content-heavy document
        for run in heading.runs:
            run.font.size = Pt(11) if heading_level == 1 else Pt(10)
            run.font.bold = True
        heading.paragraph_format.space_before = Pt(6) if heading_level == 1 else Pt(4)
        heading.paragraph_format.space_after = Pt(3)
        # Keep heading with next paragraph (prevent orphan headings)
        heading.paragraph_format.keep_with_next = True
        heading.paragraph_format.page_break_before = False
        
        # Process each summary statement
        statements = category_data.get('summary_statements', [])
        for idx, statement_data in enumerate(statements):
            # Add the statement as a bullet point with markdown formatting
            statement_para = doc.add_paragraph(style='List Bullet')
            # Use the new parsing function to handle **bold** markdown
            parse_and_format_text(statement_para, statement_data['statement'], base_font_size=Pt(9))
            statement_para.paragraph_format.space_after = Pt(2)
            statement_para.paragraph_format.line_spacing = 1.0
            # Keep statement with its evidence (prevent splits)
            statement_para.paragraph_format.keep_with_next = True
            
            # Add supporting evidence as indented quotes (no bullets)
            evidence_list = statement_data.get('evidence', [])
            if evidence_list:
                for i, evidence in enumerate(evidence_list):
                    # Create indented paragraph for quotes/evidence (no bullet)
                    evidence_para = doc.add_paragraph()
                    # Significant indentation for block quotes to make them stand out
                    evidence_para.paragraph_format.left_indent = Inches(0.75)   # Significant left indent
                    evidence_para.paragraph_format.right_indent = Inches(0.5)   # Significant right indent
                    evidence_para.paragraph_format.first_line_indent = Inches(0)  # No additional first line indent
                    evidence_para.paragraph_format.space_after = Pt(1)
                    evidence_para.paragraph_format.line_spacing = 1.0
                    
                    # Keep evidence together with statement (for last evidence item, don't keep with next)
                    if i < len(evidence_list) - 1:
                        evidence_para.paragraph_format.keep_with_next = True
                    elif idx < len(statements) - 1:
                        # Add spacing after last evidence before next statement
                        evidence_para.paragraph_format.space_after = Pt(4)
                    
                    # Prevent widows and orphans
                    evidence_para.paragraph_format.widow_control = True
                    
                    # Add the evidence content in italics with professional font size
                    # Use full evidence content - no truncation
                    evidence_content = evidence['content']
                    
                    # Add quotes if needed and parse markdown (__underline__)
                    if evidence['type'] == 'quote':
                        # Add opening quote
                        evidence_para.add_run('"').italic = True
                        # Parse the content with underline support
                        parse_and_format_text(
                            evidence_para, 
                            evidence_content,
                            base_font_size=Pt(8),
                            base_color=RGBColor(64, 64, 64),
                            base_italic=True
                        )
                        # Add closing quote
                        closing_run = evidence_para.add_run('"')
                        closing_run.italic = True
                        closing_run.font.size = Pt(8)
                        closing_run.font.color.rgb = RGBColor(64, 64, 64)
                    else:  # paraphrase
                        parse_and_format_text(
                            evidence_para,
                            evidence_content,
                            base_font_size=Pt(8),
                            base_color=RGBColor(64, 64, 64),
                            base_italic=True
                        )
                    
                    # Add speaker attribution with em dash
                    speaker = evidence.get("speaker", "Unknown")
                    speaker_run = evidence_para.add_run(f' — {speaker}')
                    speaker_run.italic = False  # Attribution not italic, just smaller
                    speaker_run.font.size = Pt(7)
                    speaker_run.font.color.rgb = RGBColor(96, 96, 96)  # Lighter gray
    
    except Exception as e:
        # Log error but don't fail the entire document generation
        logger.warning(f"Error formatting category content: {e}", exc_info=True)
        # Add basic content as fallback
        try:
            fallback_para = doc.add_paragraph(f"[Error formatting content for: {category_data.get('title', 'Unknown')}]")
            fallback_para.font.color.rgb = RGBColor(255, 0, 0)
        except:
            pass  # Silently fail if even fallback fails






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
    
    # Build path to XLSX file in config folder
    current_dir = os.path.dirname(os.path.abspath(__file__))
    xlsx_path = os.path.join(current_dir, 'config', file_name)
    
    if not os.path.exists(xlsx_path):
        error_msg = f"Categories file not found: {xlsx_path}. Cannot proceed without category definitions."
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    try:
        # Read the first sheet (previously named Template)
        df = pd.read_excel(xlsx_path, sheet_name=0)  # Use first sheet

        # Validate that we have required columns
        required_columns = ['transcripts_section', 'category_name', 'category_description']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns in {file_name}: {missing_columns}")

        # Convert to list of dictionaries
        categories = df.to_dict('records')

        if not categories:
            raise ValueError(f"No categories found in {file_name} - file appears to be empty")

        logger.info(f"Loaded {len(categories)} categories from {file_name}")
        return categories

    except Exception as e:
        error_msg = f"Failed to load categories from {xlsx_path}: {str(e)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)


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


async def get_bank_info(bank_name: str) -> Dict[str, Any]:
    """
    Look up bank information from the aegis_data_availability table.

    Args:
        bank_name: Name, symbol, or ID of the bank

    Returns:
        Dictionary with bank_id, bank_name, and bank_symbol

    Raises:
        ValueError: If bank not found
    """
    async with get_connection() as conn:
        # Check if input is a numeric ID
        if bank_name.isdigit():
            result = await conn.execute(text(
                """
                SELECT DISTINCT bank_id, bank_name, bank_symbol
                FROM aegis_data_availability
                WHERE bank_id = :bank_id
                LIMIT 1
                """
            ), {"bank_id": int(bank_name)})
            row = result.fetchone()
            result = row._asdict() if row else None
        else:
            # Try exact match first on name or symbol
            result = await conn.execute(text(
                """
                SELECT DISTINCT bank_id, bank_name, bank_symbol
                FROM aegis_data_availability
                WHERE LOWER(bank_name) = LOWER(:bank_name)
                   OR LOWER(bank_symbol) = LOWER(:bank_name)
                LIMIT 1
                """
            ), {"bank_name": bank_name})
            row = result.fetchone()
            result = row._asdict() if row else None
            
            if not result:
                # Try partial match
                partial_result = await conn.execute(text(
                    """
                    SELECT DISTINCT bank_id, bank_name, bank_symbol
                    FROM aegis_data_availability
                    WHERE LOWER(bank_name) LIKE LOWER(:pattern)
                       OR LOWER(bank_symbol) LIKE LOWER(:pattern)
                    LIMIT 1
                    """
                ), {"pattern": f"%{bank_name}%"})
                row = partial_result.fetchone()
                result = row._asdict() if row else None
        
        if not result:
            # List available banks for user
            available = await conn.execute(text(
                """
                SELECT DISTINCT bank_symbol, bank_name
                FROM aegis_data_availability
                ORDER BY bank_symbol
                """
            ))
            available = await available.fetchall()
            
            bank_list = "\n".join([f"  - {r['bank_symbol']}: {r['bank_name']}" for r in available])
            raise ValueError(
                f"Bank '{bank_name}' not found. Available banks:\n{bank_list}"
            )
        
        return {
            "bank_id": result["bank_id"],
            "bank_name": result["bank_name"],
            "bank_symbol": result["bank_symbol"]
        }


async def verify_data_availability(bank_id: int, fiscal_year: int, quarter: str) -> bool:
    """
    Check if transcript data is available for the specified bank and period.

    Args:
        bank_id: Bank ID
        fiscal_year: Year (e.g., 2024)
        quarter: Quarter (e.g., "Q3")

    Returns:
        True if transcript data is available, False otherwise
    """
    async with get_connection() as conn:
        result = await conn.execute(text(
            """
            SELECT database_names
            FROM aegis_data_availability
            WHERE bank_id = :bank_id
              AND fiscal_year = :fiscal_year
              AND quarter = :quarter
            """
        ), {"bank_id": bank_id, "fiscal_year": fiscal_year, "quarter": quarter})
        row = result.fetchone()

        if row and row[0]:  # row[0] is database_names column
            return 'transcripts' in row[0]

        return False


async def generate_call_summary(
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
        bank_info = await get_bank_info(bank_name)
        logger.info(
            "etl.call_summary.bank_found",
            execution_id=execution_id,
            bank_id=bank_info["bank_id"],
            bank_name=bank_info["bank_name"],
            bank_symbol=bank_info["bank_symbol"]
        )
        
        # Step 2: Verify data availability
        if not await verify_data_availability(bank_info["bank_id"], fiscal_year, quarter):
            error_msg = f"No transcript data available for {bank_info['bank_name']} {quarter} {fiscal_year}"
            logger.warning(
                "etl.call_summary.no_data",
                execution_id=execution_id,
                message=error_msg
            )
            
            # Check what periods are available
            async with get_connection() as conn:
                result = await conn.execute(text(
                    """
                    SELECT DISTINCT fiscal_year, quarter
                    FROM aegis_data_availability
                    WHERE bank_id = :bank_id
                      AND 'transcripts' = ANY(database_names)
                    ORDER BY fiscal_year DESC, quarter DESC
                    LIMIT 10
                    """
                ))
                available_periods = await result.fetchall()
                
                if available_periods:
                    period_list = ", ".join([f"{p['quarter']} {p['fiscal_year']}" for p in available_periods])
                    error_msg += f"\n\nAvailable periods for {bank_info['bank_name']}: {period_list}"
            
            return f"⚠️ {error_msg}"
        
        # Step 3: Setup context for function calls
        ssl_config = setup_ssl()
        auth_config = await setup_authentication(execution_id, ssl_config)
        
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
        
        # Step 6: FIRST STAGE - Generate Research Plan using Tool Calling
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
        chunks = await retrieve_full_section(
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
        formatted_transcript = await format_full_section_chunks(
            chunks=chunks,
            combo=combo,
            context=context
        )
        
        logger.info(
            "etl.call_summary.transcript_formatted",
            execution_id=execution_id,
            content_length=len(formatted_transcript)
        )

        # DIAGNOSTIC: Count words in MD vs QA sections
        md_word_count = 0
        qa_word_count = 0

        # Split by section headers to count words per section
        if "## Section 1: MANAGEMENT DISCUSSION SECTION" in formatted_transcript:
            # Extract MD section (from Section 1 to Section 2 or end)
            md_start = formatted_transcript.index("## Section 1: MANAGEMENT DISCUSSION SECTION")
            if "## Section 2:" in formatted_transcript:
                md_end = formatted_transcript.index("## Section 2:")
                md_content = formatted_transcript[md_start:md_end]
            else:
                md_content = formatted_transcript[md_start:]
            md_word_count = len(md_content.split())

        if "## Section 2:" in formatted_transcript:
            # Extract QA section (from Section 2 to end)
            qa_start = formatted_transcript.index("## Section 2:")
            qa_content = formatted_transcript[qa_start:]
            qa_word_count = len(qa_content.split())

            # Check section name
            qa_header_line = qa_content.split('\n')[0]
            logger.info(
                "etl.call_summary.qa_section_found",
                execution_id=execution_id,
                qa_section_header=qa_header_line
            )

        total_words = len(formatted_transcript.split())
        md_percentage = (md_word_count / total_words * 100) if total_words > 0 else 0
        qa_percentage = (qa_word_count / total_words * 100) if total_words > 0 else 0

        logger.info(
            "etl.call_summary.section_word_counts",
            execution_id=execution_id,
            total_words=total_words,
            md_words=md_word_count,
            md_percentage=f"{md_percentage:.1f}%",
            qa_words=qa_word_count,
            qa_percentage=f"{qa_percentage:.1f}%",
            has_qa_content=qa_word_count > 0
        )

        # Load research plan prompts from database (matches transcripts pattern)
        research_prompts = load_prompt_from_db(
            layer="call_summary_etl",
            name="research_plan",
            compose_with_globals=False,  # ETL doesn't use global contexts
            available_databases=None,
            execution_id=execution_id
        )

        # Format categories for system prompt
        categories_text = ""
        for i, category in enumerate(categories, 1):
            section_desc = {
                'MD': 'Management Discussion section only',
                'QA': 'Q&A section only', 
                'ALL': 'Both Management Discussion and Q&A sections'
            }.get(category['transcripts_section'], 'ALL sections')
            
            categories_text += f"""
Category {i}:
- Name: {category['category_name']}
- Section: {section_desc}
- Instructions: {category['category_description']}
"""
        
        # Format system prompt with all context
        system_prompt = research_prompts['system_prompt'].format(
            bank_name=bank_info['bank_name'],
            bank_symbol=bank_info['bank_symbol'],
            quarter=quarter,
            fiscal_year=fiscal_year,
            categories_list=categories_text
        )
        
        # Keep user prompt minimal - just the transcript
        user_prompt = f"Analyze this transcript and create the research plan:\n\n{formatted_transcript}"
        
        # Build messages for LLM
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        # Make tool call to generate research plan with retry logic
        logger.info(
            "etl.call_summary.calling_llm_research_plan",
            execution_id=execution_id
        )

        max_retries = 3
        research_plan_data = None

        for attempt in range(max_retries):
            try:
                response = await complete_with_tools(
                    messages=messages,
                    tools=[research_prompts['tool_definition']],
                    context=context,
                    llm_params={
                        "model": get_model_for_stage("RESEARCH_PLAN_MODEL"),
                        "temperature": TEMPERATURE,
                        "max_tokens": MAX_TOKENS
                    }
                )

                # Parse the tool response
                tool_call = response['choices'][0]['message']['tool_calls'][0]
                research_plan_data = json.loads(tool_call['function']['arguments'])

                logger.info(
                    "etl.call_summary.research_plan_generated",
                    execution_id=execution_id,
                    num_plans=len(research_plan_data['category_plans']),
                    attempt=attempt + 1
                )

                # Log if we got fewer plans than categories (this is OK - some may not apply)
                if len(research_plan_data['category_plans']) != len(categories):
                    logger.warning(
                        "etl.call_summary.plan_count_differs",
                        execution_id=execution_id,
                        expected=len(categories),
                        received=len(research_plan_data['category_plans']),
                        note="Some categories may not apply to this bank/transcript"
                    )

                # Check which categories have plans (for informational purposes)
                plan_indices = {plan['index'] for plan in research_plan_data['category_plans']}
                plan_names = {plan['name'] for plan in research_plan_data['category_plans']}
                category_names = {cat['category_name'] for cat in categories}
                missing = category_names - plan_names
                if missing:
                    logger.info(
                        "etl.call_summary.categories_without_plans",
                        execution_id=execution_id,
                        categories_without_plans=list(missing),
                        note="These categories may not have relevant content in the transcript"
                    )

                # Success - break out of retry loop
                break

            except Exception as e:
                logger.error(
                    "etl.call_summary.research_plan_error",
                    execution_id=execution_id,
                    error=str(e),
                    attempt=attempt + 1
                )
                if attempt < max_retries - 1:
                    logger.info(f"Retrying due to error: {str(e)} (attempt {attempt + 2}/{max_retries})")
                    continue
                else:
                    return f"❌ Error generating research plan after {max_retries} attempts: {str(e)}"

        if not research_plan_data:
            return f"❌ Failed to generate research plan after {max_retries} attempts"
        
        # Step 7: SECOND STAGE - Extract content for each category using tool calling
        category_results = []
        
        # Load category extraction prompts from database (matches transcripts pattern)
        extraction_prompts = load_prompt_from_db(
            layer="call_summary_etl",
            name="category_extraction",
            compose_with_globals=False,  # ETL doesn't use global contexts
            available_databases=None,
            execution_id=execution_id
        )
        
        for i, category in enumerate(categories, 1):
            logger.info(
                "etl.call_summary.processing_category",
                execution_id=execution_id,
                category_index=i,
                category_name=category["category_name"],
                section=category["transcripts_section"]
            )
            
            # Get research plan for this category by index (more reliable than name)
            category_plan = next(
                (p for p in research_plan_data['category_plans']
                 if p.get('index') == i),  # Match by index
                None
            )

            # Validate name matches as secondary check (strip whitespace to avoid false mismatches)
            if category_plan and category_plan.get('name', '').strip() != category['category_name'].strip():
                logger.warning(
                    "etl.call_summary.name_mismatch",
                    execution_id=execution_id,
                    expected_name=category['category_name'].strip(),
                    received_name=category_plan.get('name', '').strip(),
                    index=i
                )


            if not category_plan:
                # Category may not apply to this bank/transcript - this is acceptable
                logger.info(
                    "etl.call_summary.category_skipped_no_plan",
                    execution_id=execution_id,
                    category_name=category["category_name"],
                    category_index=i,
                    note="Category not applicable to this transcript based on research plan analysis"
                )
                # Skip this category entirely
                category_results.append({
                    "index": i,
                    "name": category["category_name"],
                    "report_section": category.get("report_section", "Results Summary"),
                    "rejected": True,
                    "rejection_reason": "Category not applicable to this transcript based on research plan analysis"
                })
                continue
            
            # Retrieve chunks for this specific category's section
            chunks = await retrieve_full_section(
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
                # Create rejected result
                category_results.append({
                    "index": i,
                    "name": category["category_name"],
                    "report_section": category.get("report_section", "Results Summary"),
                    "rejected": True,
                    "rejection_reason": f"No {category['transcripts_section']} section data available"
                })
                continue
            
            # Format the chunks for this category
            formatted_section = await format_full_section_chunks(
                chunks=chunks,
                combo=combo,
                context=context
            )
            
            # Format previous sections with FULL content for deduplication
            previous_summary = ""
            extracted_themes = ""
            if category_results:
                # Get non-rejected completed categories
                completed_results = [r for r in category_results if not r.get('rejected', False)]
                if completed_results:
                    # List completed category names
                    completed_names = [r['name'] for r in completed_results]
                    previous_summary = f"Already completed: {', '.join(completed_names)}"

                    # Include ALL statements from completed categories for proper deduplication
                    all_statements = []
                    for result in completed_results:
                        if 'summary_statements' in result:
                            for stmt in result['summary_statements']:
                                # Include statement and evidence summary for context
                                statement_text = f"[{result['name']}] {stmt['statement']}"

                                # Add evidence snippets to help identify overlapping content
                                if 'evidence' in stmt and stmt['evidence']:
                                    # Show up to 3 quotes from evidence to help detect duplicates
                                    quote_snippets = []
                                    for idx, ev in enumerate(stmt['evidence'][:3]):  # First 3 quotes
                                        if ev.get('type') == 'quote' and ev.get('content'):
                                            # Take first 80 chars of each quote
                                            snippet = ev['content'][:80]
                                            if len(ev['content']) > 80:
                                                snippet += "..."
                                            quote_snippets.append(f"Q{idx+1}: \"{snippet}\"")

                                    if quote_snippets:
                                        statement_text += f"\n  → Quotes: {' | '.join(quote_snippets)}"

                                all_statements.append(statement_text)

                    if all_statements:
                        # Provide all extracted content to prevent duplication
                        extracted_themes = "\n".join(all_statements)
                    else:
                        extracted_themes = "No specific themes extracted yet"
                else:
                    previous_summary = "No previous sections completed yet"
                    extracted_themes = "No themes extracted yet"
            else:
                extracted_themes = "Starting extraction - no prior themes"
            
            # Format system prompt with ALL context
            # Validate cross_category_notes (should be mandatory and substantive)
            cross_cat_notes = category_plan.get('cross_category_notes', '')
            if not cross_cat_notes or len(cross_cat_notes.strip()) < 20:
                logger.warning(
                    "etl.call_summary.weak_cross_category_notes",
                    execution_id=execution_id,
                    category_name=category['category_name'],
                    notes_length=len(cross_cat_notes.strip()) if cross_cat_notes else 0,
                    message="Cross-category notes missing or too brief. Deduplication guidance may be insufficient."
                )

            system_prompt = extraction_prompts['system_prompt'].format(
                category_index=i,
                total_categories=len(categories),
                bank_name=bank_info['bank_name'],
                bank_symbol=bank_info['bank_symbol'],
                quarter=quarter,
                fiscal_year=fiscal_year,
                category_name=category['category_name'],
                category_description=category['category_description'],
                transcripts_section=category['transcripts_section'],
                research_plan=category_plan['extraction_strategy'],
                cross_category_notes=cross_cat_notes,
                previous_sections=previous_summary,
                extracted_themes=extracted_themes
            )
            
            # Keep user prompt minimal - just the transcript section
            user_prompt = f"Extract content from this transcript section:\n\n{formatted_section}"
            
            # Build messages
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            
            # Make tool call with retry logic
            max_extraction_retries = 2
            extracted_data = None

            for attempt in range(max_extraction_retries):
                try:
                    response = await complete_with_tools(
                        messages=messages,
                        tools=[extraction_prompts['tool_definition']],
                        context=context,
                        llm_params={
                            "model": get_model_for_stage("CATEGORY_EXTRACTION_MODEL", category["category_name"]),
                            "temperature": TEMPERATURE,
                            "max_tokens": MAX_TOKENS
                        }
                    )

                    # Parse the tool response
                    tool_call = response['choices'][0]['message']['tool_calls'][0]
                    extracted_data = json.loads(tool_call['function']['arguments'])

                    # Add index, name, and report section
                    extracted_data['index'] = i
                    extracted_data['name'] = category['category_name']
                    extracted_data['report_section'] = category.get('report_section', 'Results Summary')

                    category_results.append(extracted_data)

                    logger.info(
                        "etl.call_summary.category_completed",
                        execution_id=execution_id,
                        category_index=i,
                        category_name=category["category_name"],
                        rejected=extracted_data.get('rejected', False),
                        attempt=attempt + 1
                    )

                    # Passive duplicate detection (logging only, no rejection)
                    if not extracted_data.get('rejected', False) and 'summary_statements' in extracted_data:
                        # Get all prior statements for comparison
                        all_prior_statements = []
                        for prior_result in [r for r in category_results[:-1] if not r.get('rejected', False)]:
                            if 'summary_statements' in prior_result:
                                for prior_stmt in prior_result['summary_statements']:
                                    all_prior_statements.append({
                                        'category': prior_result['name'],
                                        'statement': prior_stmt['statement']
                                    })

                        # Check each new statement against prior statements
                        for new_stmt in extracted_data['summary_statements']:
                            for prior in all_prior_statements:
                                similarity = SequenceMatcher(
                                    None,
                                    new_stmt['statement'].lower(),
                                    prior['statement'].lower()
                                ).ratio()

                                if similarity > 0.7:  # 70% similarity threshold
                                    logger.warning(
                                        "etl.call_summary.potential_duplicate_detected",
                                        execution_id=execution_id,
                                        current_category=category["category_name"],
                                        prior_category=prior['category'],
                                        similarity_pct=f"{similarity*100:.0f}%",
                                        current_statement=new_stmt['statement'][:100],
                                        prior_statement=prior['statement'][:100],
                                        message="Potential semantic overlap detected - review for duplication"
                                    )

                    break  # Success

                except Exception as e:
                    logger.error(
                        "etl.call_summary.category_extraction_error",
                        execution_id=execution_id,
                        category_name=category["category_name"],
                        error=str(e),
                        attempt=attempt + 1
                    )

                    if attempt < max_extraction_retries - 1:
                        logger.info(
                            f"Retrying extraction for {category['category_name']} "
                            f"(attempt {attempt + 2}/{max_extraction_retries})"
                        )
                        continue
                    else:
                        # Final failure - add error result
                        category_results.append({
                            "index": i,
                            "name": category["category_name"],
                            "report_section": category.get("report_section", "Results Summary"),
                            "rejected": True,
                            "rejection_reason": f"Error after {max_extraction_retries} attempts: {str(e)}"
                        })
        
        # Step 8: Generate Word Document from structured data
        logger.info(
            "etl.call_summary.generating_document",
            execution_id=execution_id,
            num_categories=len(category_results),
            num_rejected=sum(1 for c in category_results if c.get('rejected', False))
        )
        
        # Filter out rejected categories
        valid_categories = [c for c in category_results if not c.get('rejected', False)]
        
        if not valid_categories:
            logger.warning(
                "etl.call_summary.no_valid_categories",
                execution_id=execution_id
            )
            return "⚠️ All categories were rejected - no content to generate document"
        
        # Create Word document
        doc = Document()
        
        # Set narrow margins for content-heavy document
        sections = doc.sections
        for section in sections:
            section.top_margin = Inches(0.4)  # Even narrower top margin for more space
            section.bottom_margin = Inches(0.4)  # Even narrower bottom margin
            section.left_margin = Inches(0.6)  # Slightly wider left for binding
            section.right_margin = Inches(0.5)  # Narrow right margin
            # Set gutter for binding if needed
            section.gutter = Inches(0)
        
        # Add page numbers to footer
        add_page_numbers(doc)
        
        # Title Page with Banner
        # Check for banner image in config folder
        etl_dir = os.path.dirname(os.path.abspath(__file__))
        config_dir = os.path.join(etl_dir, 'config')
        banner_path = None
        for ext in ['jpg', 'jpeg', 'png']:
            potential_banner = os.path.join(config_dir, f'banner.{ext}')
            if os.path.exists(potential_banner):
                banner_path = potential_banner
                break
        
        # Add banner image if found
        if banner_path:
            try:
                # Add the banner image at the top, adjusted for narrow margins
                doc.add_picture(banner_path, width=Inches(7.4))  # Full width with narrow margins
                last_paragraph = doc.paragraphs[-1] 
                last_paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
                last_paragraph.paragraph_format.space_after = Pt(3)  # Even smaller space after banner
                
                logger.info(
                    "etl.call_summary.banner_added",
                    execution_id=execution_id,
                    banner_path=banner_path
                )
            except Exception as e:
                logger.warning(
                    "etl.call_summary.banner_failed",
                    execution_id=execution_id,
                    error=str(e)
                )
        
        # Add title - Left-aligned with bank symbol
        title_text = f"{quarter}/{str(fiscal_year)[-2:]} Results and Call Summary - {bank_info['bank_symbol']}"
        title = doc.add_heading(title_text, 0)
        title.alignment = WD_ALIGN_PARAGRAPH.LEFT  # Left-aligned
        for run in title.runs:
            run.font.size = Pt(14)  # Reduced from 16pt to save space
            run.font.bold = True
            try:
                run.font.name = 'Arial'  # Professional font
            except:
                pass  # Use default font if Arial not available
        title.paragraph_format.space_after = Pt(4)  # Reduced space before TOC
        
        # Add Table of Contents
        add_table_of_contents(doc)
        
        # Mark document to update fields when opened
        try:
            mark_document_for_update(doc)
        except Exception as e:
            logger.debug(f"Could not set auto-update fields: {e}")
        
        # Group categories by report section
        from itertools import groupby
        
        # Sort by report_section first to group them, then by original index to maintain order
        sorted_categories = sorted(valid_categories, key=lambda x: (
            0 if x.get('report_section', 'Results Summary') == 'Results Summary' else 1,
            x.get('index', 0)
        ))
        
        # Group by report_section
        for section_name, section_categories in groupby(sorted_categories, key=lambda x: x.get('report_section', 'Results Summary')):
            section_categories = list(section_categories)
            
            # Add main section heading (Level 1) with page break control
            section_heading = doc.add_heading(section_name, level=1)
            section_heading.paragraph_format.space_before = Pt(10)
            section_heading.paragraph_format.space_after = Pt(6)
            section_heading.paragraph_format.keep_with_next = True
            # Add page break before major sections (except first)
            if section_name != 'Results Summary':
                section_heading.paragraph_format.page_break_before = True
            
            # Add categories within this section
            for i, category_data in enumerate(section_categories, 1):
                # Use the new function to add structured content (as Level 2)
                add_structured_content_to_doc(doc, category_data, heading_level=2)
                
                # Add professional spacing between categories (if not last in section)
                if i < len(section_categories):
                    spacer = doc.add_paragraph()
                    spacer.paragraph_format.space_after = Pt(6)  # Reduced separation
                    # Add a subtle separator line for clarity
                    spacer.add_run()  # Empty paragraph for spacing
                
                # Log progress
                logger.debug(
                    "etl.call_summary.category_added_to_doc",
                    execution_id=execution_id,
                    section=section_name,
                    category_name=category_data['name'],
                    num_statements=len(category_data.get('summary_statements', []))
                )
        
        # Save the document
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
        os.makedirs(output_dir, exist_ok=True)

        # Generate unique hash for this report based on content
        content_hash = hashlib.md5(
            f"{bank_info['bank_id']}_{fiscal_year}_{quarter}_{datetime.now().isoformat()}".encode()
        ).hexdigest()[:8]

        filename_base = f"{bank_info['bank_symbol']}_{fiscal_year}_{quarter}_{content_hash}"
        docx_filename = f"{filename_base}.docx"
        filepath = os.path.join(output_dir, docx_filename)
        doc.save(filepath)
        
        logger.info(
            "etl.call_summary.document_saved",
            execution_id=execution_id,
            filepath=filepath,
            valid_categories=len(valid_categories),
            rejected_categories=len(category_results) - len(valid_categories)
        )

        # Step 9: Convert to PDF
        logger.info(
            "etl.call_summary.converting_to_pdf",
            execution_id=execution_id
        )

        pdf_filename = f"{filename_base}.pdf"
        pdf_filepath = os.path.join(output_dir, pdf_filename)
        pdf_result = convert_docx_to_pdf(filepath, pdf_filepath)

        if pdf_result:
            logger.info(
                "etl.call_summary.pdf_created",
                execution_id=execution_id,
                pdf_filepath=pdf_result
            )
        else:
            logger.warning(
                "etl.call_summary.pdf_creation_failed",
                execution_id=execution_id
            )
            pdf_filepath = None
            pdf_filename = None

        # Step 10: Generate Markdown content
        logger.info(
            "etl.call_summary.generating_markdown",
            execution_id=execution_id
        )

        markdown_content = structured_data_to_markdown(
            category_results=valid_categories,
            bank_info=bank_info,
            quarter=quarter,
            fiscal_year=fiscal_year
        )

        # Step 11: Save to database
        logger.info(
            "etl.call_summary.saving_to_database",
            execution_id=execution_id
        )

        report_metadata = get_standard_report_metadata()
        generation_timestamp = datetime.now()

        try:
            async with get_connection() as conn:
                # Delete any existing report for this bank/period/type combination
                delete_result = await conn.execute(text(
                    """
                    DELETE FROM aegis_reports
                    WHERE bank_id = :bank_id
                      AND fiscal_year = :fiscal_year
                      AND quarter = :quarter
                      AND report_type = :report_type
                    RETURNING id
                    """
                ), {
                    "bank_id": bank_info["bank_id"],
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                    "report_type": report_metadata["report_type"]
                })
                deleted = delete_result.fetchall()

                if deleted:
                    logger.info(
                        "etl.call_summary.existing_reports_deleted",
                        execution_id=execution_id,
                        deleted_count=len(deleted),
                        deleted_ids=[row.id for row in deleted]
                    )

                    # Check if there are any other reports for this bank/period
                    remaining_result = await conn.execute(text(
                        """
                        SELECT COUNT(*) as count
                        FROM aegis_reports
                        WHERE bank_id = :bank_id
                          AND fiscal_year = :fiscal_year
                          AND quarter = :quarter
                        """
                    ), {
                        "bank_id": bank_info["bank_id"],
                        "fiscal_year": fiscal_year,
                        "quarter": quarter
                    })
                    remaining_reports = remaining_result.scalar()

                    # If no other reports exist, remove 'reports' from availability
                    if remaining_reports == 0:
                        await conn.execute(text(
                            """
                            UPDATE aegis_data_availability
                            SET database_names = array_remove(database_names, 'reports')
                            WHERE bank_id = :bank_id
                              AND fiscal_year = :fiscal_year
                              AND quarter = :quarter
                              AND 'reports' = ANY(database_names)
                            """
                        ), {
                            "bank_id": bank_info["bank_id"],
                            "fiscal_year": fiscal_year,
                            "quarter": quarter
                        })
                        logger.info(
                            "etl.call_summary.availability_reports_removed",
                            execution_id=execution_id,
                            reason="No remaining reports after deletion"
                        )

                # Insert new report
                result = await conn.execute(text(
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
                        s3_document_name,
                        s3_pdf_name,
                        markdown_content,
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
                        :s3_document_name,
                        :s3_pdf_name,
                        :markdown_content,
                        :generation_date,
                        :generated_by,
                        :execution_id,
                        :metadata
                    )
                    RETURNING id
                    """
                ), {
                    "report_name": report_metadata["report_name"],
                    "report_description": report_metadata["report_description"],
                    "report_type": report_metadata["report_type"],
                    "bank_id": bank_info["bank_id"],
                    "bank_name": bank_info["bank_name"],
                    "bank_symbol": bank_info["bank_symbol"],
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                    "local_filepath": filepath,
                    "s3_document_name": docx_filename,  # Will be updated when uploaded to S3
                    "s3_pdf_name": pdf_filename,  # Will be updated when uploaded to S3
                    "markdown_content": markdown_content,
                    "generation_date": generation_timestamp,
                    "generated_by": "call_summary_etl",
                    "execution_id": execution_id,
                    "metadata": json.dumps({
                        "bank_type": bank_type,
                        "categories_processed": len(category_results),
                        "categories_included": len(valid_categories),
                        "categories_rejected": len(category_results) - len(valid_categories)
                    })
                })
                report_row = result.fetchone()
                report_id = report_row.id
                logger.info(
                    "etl.call_summary.database_inserted",
                    execution_id=execution_id,
                    report_id=report_id
                )

                # Update aegis_data_availability to include 'reports' database
                update_result = await conn.execute(text("""
                    UPDATE aegis_data_availability
                    SET database_names =
                        CASE
                            WHEN 'reports' = ANY(database_names) THEN database_names
                            ELSE array_append(database_names, 'reports')
                        END
                    WHERE bank_id = :bank_id
                      AND fiscal_year = :fiscal_year
                      AND quarter = :quarter
                      AND NOT ('reports' = ANY(database_names))
                    RETURNING bank_id
                """), {
                    "bank_id": bank_info["bank_id"],
                    "fiscal_year": fiscal_year,
                    "quarter": quarter
                })
                update_count = update_result.rowcount

                if update_count > 0:
                    logger.info(
                        "etl.call_summary.availability_updated",
                        execution_id=execution_id,
                        bank_id=bank_info["bank_id"],
                        fiscal_year=fiscal_year,
                        quarter=quarter
                    )

                # Commit all changes at once
                await conn.commit()

        except Exception as e:
            logger.error(
                "etl.call_summary.database_error",
                execution_id=execution_id,
                error=str(e)
            )
        
        logger.info(
            "etl.call_summary.completed",
            execution_id=execution_id,
            stage="full_report",
            num_categories=len(valid_categories)
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
================================================================================

RESEARCH PLAN:
- Categories Analyzed: {len(research_plan_data['category_plans'])}

EXTRACTION RESULTS:
- Categories Processed: {len(category_results)}
- Categories Included: {len(valid_categories)}
- Categories Rejected: {len(category_results) - len(valid_categories)}
"""
        
        # Add rejection reasons if any
        rejected = [c for c in category_results if c.get('rejected', False)]
        if rejected:
            output += "\nREJECTED CATEGORIES:\n"
            for cat in rejected:
                output += f"  - {cat['name']}: {cat.get('rejection_reason', 'No reason provided')}\n"
        
        # Add included categories summary
        output += "\nINCLUDED CATEGORIES:\n"
        for cat in valid_categories:
            num_statements = len(cat.get('summary_statements', []))
            output += f"  - {cat['name']}: {num_statements} key findings\n"
        
        output += f"""

DOCUMENT OUTPUTS:
- Word Document: {filepath}
- PDF Document: {pdf_filepath if pdf_filepath else 'PDF generation failed'}
- Database Entry: {'Saved to aegis_reports table' if markdown_content else 'Not saved'}
- Markdown Length: {len(markdown_content)} characters

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

    # Initialize PostgreSQL prompts cache
    postgresql_prompts()

    # Generate the call summary
    print(f"\n🔄 Generating report for {args.bank} {args.quarter} {args.year}...\n")

    result = asyncio.run(generate_call_summary(
        bank_name=args.bank,
        fiscal_year=args.year,
        quarter=args.quarter
    ))
    
    # Output the result
    if args.output:
        with open(args.output, 'w') as f:
            f.write(result)
        print(f"✅ Report saved to: {args.output}")
    else:
        print(result)


if __name__ == "__main__":
    main()