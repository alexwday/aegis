"""
Key Themes ETL with Optimized Parallel Processing and Comprehensive Grouping

This version uses a more efficient architecture:
1. Load all Q&A blocks into an index
2. Process each independently to extract themes (parallelizable)
3. Make ONE comprehensive grouping decision with full visibility
4. Apply grouping programmatically
5. Generate organized document

Usage:
    python -m aegis.etls.key_themes.main_optimized --bank "Royal Bank of Canada" --year 2024 --quarter Q3
"""

import argparse
import asyncio
import json
import sys
import uuid
import os
# import re  # No longer needed - using HTML parser instead
from datetime import datetime
from typing import Dict, Any, List, Optional
from sqlalchemy import text
import yaml
from docx import Document
from docx.shared import Pt, Inches, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement, parse_xml
from docx.oxml.ns import qn, nsdecls
import hashlib
from collections import defaultdict
from html.parser import HTMLParser

# Import document converter functions
from aegis.etls.key_themes.document_converter import (
    convert_docx_to_pdf,
    get_standard_report_metadata
)

# Import direct transcript functions
from aegis.model.subagents.transcripts.retrieval import retrieve_full_section
from aegis.model.subagents.transcripts.formatting import format_full_section_chunks
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication
from aegis.connections.llm_connector import complete, complete_with_tools
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger
from aegis.etls.key_themes.config.config import MODELS, TEMPERATURE, MAX_TOKENS

# Initialize logging
setup_logging()
logger = get_logger()


class HTMLToDocx(HTMLParser):
    """
    Parser to convert HTML-formatted text to Word document formatting.
    Supports: <b>, <strong>, <i>, <em>, <u>, <mark> tags and their nesting.
    """

    def __init__(self, paragraph, font_size=Pt(9)):
        super().__init__()
        self.paragraph = paragraph
        self.font_size = font_size
        self.text_buffer = ""
        self.format_stack = []  # Stack to track nested formatting

    def handle_starttag(self, tag, attrs):
        # Flush any pending text before changing format
        self._flush_text()

        # Add format to stack
        if tag in ['b', 'strong']:
            self.format_stack.append('bold')
        elif tag in ['i', 'em']:
            self.format_stack.append('italic')
        elif tag == 'u':
            self.format_stack.append('underline')
        elif tag == 'mark':
            self.format_stack.append('highlight')

    def handle_endtag(self, tag):
        # Flush any pending text before changing format
        self._flush_text()

        # Remove format from stack
        if tag in ['b', 'strong'] and 'bold' in self.format_stack:
            self.format_stack.remove('bold')
        elif tag in ['i', 'em'] and 'italic' in self.format_stack:
            self.format_stack.remove('italic')
        elif tag == 'u' and 'underline' in self.format_stack:
            self.format_stack.remove('underline')
        elif tag == 'mark' and 'highlight' in self.format_stack:
            self.format_stack.remove('highlight')

    def handle_data(self, data):
        # Buffer text to handle whitespace properly
        self.text_buffer += data

    def _flush_text(self):
        """Apply formatting and add text to document."""
        if not self.text_buffer:
            return

        run = self.paragraph.add_run(self.text_buffer)
        run.font.size = self.font_size

        # Apply all active formats
        if 'bold' in self.format_stack:
            run.font.bold = True
        if 'italic' in self.format_stack:
            run.font.italic = True
        if 'underline' in self.format_stack:
            run.font.underline = True
        if 'highlight' in self.format_stack:
            run.font.highlight_color = 'yellow'

        self.text_buffer = ""

    def close(self):
        """Ensure any remaining text is flushed."""
        self._flush_text()
        super().close()


class QABlock:
    """Represents a single Q&A block with its extracted information."""

    def __init__(self, qa_id: str, position: int, original_content: str):
        self.qa_id = qa_id
        self.position = position
        self.original_content = original_content
        self.theme_title = None
        self.summary = None
        self.formatted_content = None
        self.assigned_group = None
        self.is_valid = True  # Default to valid until proven otherwise


class ThemeGroup:
    """Represents a group of related Q&A blocks under a unified theme."""

    def __init__(self, group_title: str, qa_ids: List[str], rationale: str = ""):
        self.group_title = group_title
        self.qa_ids = qa_ids
        self.rationale = rationale
        self.qa_blocks = []


async def resolve_bank_info(bank_input: str, context: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve bank input to full bank information from database."""
    async with get_connection() as conn:
        # Try as bank_id (integer)
        try:
            bank_id = int(bank_input)
            result = await conn.execute(
                text("""
                    SELECT DISTINCT bank_id, bank_name, bank_symbol
                    FROM aegis_data_availability
                    WHERE bank_id = :bank_id
                    LIMIT 1
                """),
                {"bank_id": bank_id}
            )
            row = result.fetchone()
            if row:
                return {
                    'id': row.bank_id,
                    'name': row.bank_name,
                    'ticker': row.bank_symbol,
                    'symbol': row.bank_symbol
                }
        except ValueError:
            pass

        # Try as exact ticker/symbol or name match first
        result = await conn.execute(
            text("""
                SELECT DISTINCT bank_id, bank_name, bank_symbol
                FROM aegis_data_availability
                WHERE UPPER(bank_symbol) = UPPER(:symbol)
                   OR LOWER(bank_name) = LOWER(:name)
                LIMIT 1
            """),
            {"symbol": bank_input, "name": bank_input}
        )
        row = result.fetchone()
        if row:
            return {
                'id': row.bank_id,
                'name': row.bank_name,
                'ticker': row.bank_symbol,
                'symbol': row.bank_symbol
            }

        # Try partial match on ticker/symbol (e.g. "RY" matches "RY-CA")
        result = await conn.execute(
            text("""
                SELECT DISTINCT bank_id, bank_name, bank_symbol
                FROM aegis_data_availability
                WHERE UPPER(bank_symbol) LIKE UPPER(:pattern) || '%'
                   OR UPPER(bank_symbol) LIKE '%' || UPPER(:pattern) || '%'
                LIMIT 1
            """),
            {"pattern": bank_input}
        )
        row = result.fetchone()
        if row:
            return {
                'id': row.bank_id,
                'name': row.bank_name,
                'ticker': row.bank_symbol,
                'symbol': row.bank_symbol
            }

        # Try as bank name (partial match)
        result = await conn.execute(
            text("""
                SELECT DISTINCT bank_id, bank_name, bank_symbol
                FROM aegis_data_availability
                WHERE LOWER(bank_name) LIKE LOWER(:name_pattern)
                LIMIT 1
            """),
            {"name_pattern": f"%{bank_input}%"}
        )
        row = result.fetchone()
        if row:
            return {
                'id': row.bank_id,
                'name': row.bank_name,
                'ticker': row.bank_symbol,
                'symbol': row.bank_symbol
            }

        # List available banks for user
        available = await conn.execute(text("""
            SELECT DISTINCT bank_symbol, bank_name
            FROM aegis_data_availability
            ORDER BY bank_symbol
        """))
        available_banks = available.fetchall()

        bank_list = "\n".join([f"  - {r['bank_symbol']}: {r['bank_name']}" for r in available_banks])
        raise ValueError(f"Could not resolve bank '{bank_input}'. Available banks:\n{bank_list}")


async def load_qa_blocks(
    bank_name: str,
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any]
) -> Dict[str, QABlock]:
    """
    Step 1: Load all Q&A blocks and create an index.

    Returns:
        Dictionary indexed by qa_id containing QABlock objects
    """
    logger.info(f"Loading Q&A blocks for {bank_name} {fiscal_year} {quarter}")

    # Retrieve Q&A chunks from database
    combo = {
        "bank_name": bank_name,
        "bank_id": 1,  # Will be resolved from database
        "bank_symbol": "",  # Will be resolved
        "fiscal_year": fiscal_year,
        "quarter": quarter,
        "query_intent": "Retrieve Q&A section for key themes extraction"
    }

    # Get full bank info from database
    try:
        bank_info = await resolve_bank_info(bank_name, context)
        combo['bank_id'] = bank_info['id']
        combo['bank_symbol'] = bank_info['symbol']
        logger.info(f"Resolved bank: {bank_name} -> bank_id={combo['bank_id']}, symbol={combo['bank_symbol']}")
    except Exception as e:
        logger.warning(f"Could not resolve bank info: {e}")

    # Query transcripts - the table uses institution_id (string) not bank_id
    from sqlalchemy import text

    async with get_connection() as conn:
        # Query using institution_id (the aegis_transcripts table doesn't have bank_id column)
        logger.info(f"Querying Q&A data with institution_id={str(combo['bank_id'])}, year={combo['fiscal_year']}, quarter={combo['quarter']}")
        try:
            result = await conn.execute(
                text("""
                    SELECT
                        qa_group_id,
                        chunk_content as content
                    FROM aegis_transcripts
                    WHERE institution_id = :institution_id
                    AND fiscal_year = :fiscal_year
                    AND fiscal_quarter = :fiscal_quarter
                    AND section_name = 'Q&A'
                    ORDER BY qa_group_id
                """),
                {
                    "institution_id": str(combo['bank_id']),  # Convert to string for institution_id
                    "fiscal_year": combo['fiscal_year'],
                    "fiscal_quarter": combo['quarter']
                }
            )
            rows = result.fetchall()
            chunks = [{"qa_group_id": row[0], "content": row[1]} for row in rows]
            logger.info(f"Found {len(chunks)} Q&A chunks")
        except Exception as e:
            logger.warning(f"Failed to retrieve Q&A data: {e}")
            chunks = []

    if not chunks:
        logger.warning(f"No Q&A data found for {bank_name} {fiscal_year} {quarter}")
        return {}

    # Group chunks by Q&A group ID
    qa_groups = {}
    for chunk in chunks:
        qa_group_id = chunk.get('qa_group_id')
        if qa_group_id is not None:
            if qa_group_id not in qa_groups:
                qa_groups[qa_group_id] = []
            qa_groups[qa_group_id].append(chunk)

    # Create QABlock index
    qa_index = {}
    for qa_group_id, group_chunks in qa_groups.items():
        # Combine chunk content for this Q&A group
        qa_content = "\n".join([
            chunk.get('content', '')
            for chunk in group_chunks
            if chunk.get('content')
        ])

        if qa_content:
            qa_id = f'qa_{qa_group_id}'
            qa_index[qa_id] = QABlock(qa_id, qa_group_id, qa_content)

    logger.info(f"Loaded {len(qa_index)} Q&A blocks into index")
    return qa_index


async def extract_theme_and_summary(qa_block: QABlock, context: Dict[str, Any]):
    """
    Step 2A: Extract theme title and summary for a single Q&A block.
    Validates content and skips invalid Q&A sessions.
    """
    # Load theme extraction tool
    tool_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'prompts', 'theme_extraction_prompt.yaml'
    )
    with open(tool_path, 'r') as f:
        tool_config = yaml.safe_load(f)

    # Format the system template with actual values
    system_prompt = tool_config['system_template'].format(
        bank_name=context.get('bank_name', 'Bank'),
        quarter=context.get('quarter', 'Q'),
        fiscal_year=context.get('fiscal_year', 'Year')
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Extract theme from this Q&A session:\n\n{qa_block.original_content}"}
    ]

    try:
        response = await complete_with_tools(
            messages=messages,
            tools=[tool_config['tool']],
            context=context,
            llm_params={"model": MODELS["theme_extraction"], "temperature": TEMPERATURE, "max_tokens": MAX_TOKENS}
        )

        if response:
            tool_calls = response.get('choices', [{}])[0].get('message', {}).get('tool_calls', [])
            if tool_calls:
                result = json.loads(tool_calls[0]['function']['arguments'])

                # Check if the Q&A is valid
                is_valid = result.get('is_valid', True)

                if is_valid:
                    qa_block.theme_title = result['theme_title']
                    qa_block.summary = result.get('summary', '')
                    qa_block.is_valid = True
                    logger.debug(f"Extracted theme for {qa_block.qa_id}: {qa_block.theme_title}")
                else:
                    # Mark as invalid and log the reason
                    qa_block.is_valid = False
                    rejection_reason = result.get('rejection_reason', 'Invalid Q&A content')
                    logger.info(f"Skipping invalid Q&A {qa_block.qa_id}: {rejection_reason}")
                    qa_block.theme_title = None
                    qa_block.summary = None

    except Exception as e:
        logger.error(f"Error extracting theme for {qa_block.qa_id}: {str(e)}")
        # Set defaults on error
        qa_block.theme_title = f"Q&A Discussion {qa_block.position}"
        qa_block.summary = "Theme extraction failed"
        qa_block.is_valid = True  # Assume valid on error to avoid losing data


async def format_qa_html(qa_block: QABlock, context: Dict[str, Any]):
    """
    Step 2B: Format Q&A block with HTML tags for emphasis.
    Only formats valid Q&A blocks that passed validation.
    """
    # Skip formatting if the block is invalid
    if not qa_block.is_valid:
        logger.debug(f"Skipping formatting for invalid Q&A block {qa_block.qa_id}")
        qa_block.formatted_content = None
        return

    # Load HTML formatting config
    format_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'prompts', 'html_formatting_prompt.yaml'
    )
    with open(format_path, 'r') as f:
        format_config = yaml.safe_load(f)

    # Format the system template with actual values
    system_prompt = format_config['system_template'].format(
        bank_name=context.get('bank_name', 'Bank'),
        quarter=context.get('quarter', 'Q'),
        fiscal_year=context.get('fiscal_year', 'Year')
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Format this Q&A exchange with HTML tags for emphasis:\n\n{qa_block.original_content}"}
    ]

    try:
        response = await complete(messages, context, {"model": MODELS["formatting"], "temperature": TEMPERATURE, "max_tokens": MAX_TOKENS})

        if isinstance(response, dict):
            qa_block.formatted_content = response.get('choices', [{}])[0].get('message', {}).get('content', '')
        else:
            qa_block.formatted_content = str(response)

    except Exception as e:
        logger.error(f"Error formatting {qa_block.qa_id}: {str(e)}")
        qa_block.formatted_content = qa_block.original_content  # Fallback to original


async def process_all_qa_blocks(qa_index: Dict[str, QABlock], context: Dict[str, Any]):
    """
    Step 2: Process all Q&A blocks independently (in parallel).
    Extracts themes and formats markdown for each block.
    """
    logger.info(f"Processing {len(qa_index)} Q&A blocks in parallel")

    # Create tasks for parallel processing
    tasks = []
    for qa_block in qa_index.values():
        # Create coroutines for theme extraction and HTML formatting
        theme_task = extract_theme_and_summary(qa_block, context)
        format_task = format_qa_html(qa_block, context)
        tasks.extend([theme_task, format_task])

    # Execute all tasks in parallel
    await asyncio.gather(*tasks)

    logger.info("Completed processing all Q&A blocks")


async def determine_comprehensive_grouping(
    qa_index: Dict[str, QABlock],
    context: Dict[str, Any]
) -> List[ThemeGroup]:
    """
    Step 3: Make ONE comprehensive grouping decision for all themes.
    Only processes valid Q&A blocks.
    """
    # Filter out invalid Q&A blocks
    valid_qa_blocks = {qa_id: qa_block for qa_id, qa_block in qa_index.items() if qa_block.is_valid}
    invalid_count = len(qa_index) - len(valid_qa_blocks)

    if invalid_count > 0:
        logger.info(f"Filtered out {invalid_count} invalid Q&A blocks from grouping")

    if not valid_qa_blocks:
        logger.warning("No valid Q&A blocks to group")
        return []

    logger.info(f"Determining comprehensive theme grouping for {len(valid_qa_blocks)} valid Q&A blocks")

    # Load grouping tool
    tool_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'prompts', 'theme_grouping_prompt.yaml'
    )
    with open(tool_path, 'r') as f:
        tool_config = yaml.safe_load(f)

    # Prepare Q&A blocks info for the LLM - only valid blocks
    qa_blocks_info = []
    for qa_id, qa_block in sorted(valid_qa_blocks.items(), key=lambda x: x[1].position):
        qa_blocks_info.append(
            f"ID: {qa_id}\n"
            f"Title: {qa_block.theme_title}\n"
            f"Summary: {qa_block.summary}\n"
        )

    qa_blocks_str = "\n\n".join(qa_blocks_info)

    # Build prompt with all Q&A information and context
    system_prompt = tool_config['system_template'].format(
        bank_name=context.get('bank_name', 'Bank'),
        bank_symbol=context.get('bank_symbol', 'BANK'),
        quarter=context.get('quarter', 'Q'),
        fiscal_year=context.get('fiscal_year', 'Year'),
        total_qa_blocks=len(valid_qa_blocks),  # Use valid count
        qa_blocks_info=qa_blocks_str
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": "Analyze all Q&A blocks and return optimal grouping instructions."}
    ]

    try:
        response = await complete_with_tools(
            messages=messages,
            tools=[tool_config['tool']],
            context=context,
            llm_params={"model": MODELS["grouping"], "temperature": TEMPERATURE, "max_tokens": MAX_TOKENS}
        )

        if response:
            tool_calls = response.get('choices', [{}])[0].get('message', {}).get('tool_calls', [])
            if tool_calls:
                result = json.loads(tool_calls[0]['function']['arguments'])

                # Create ThemeGroup objects from LLM response
                theme_groups = []
                for group_data in result['theme_groups']:
                    group = ThemeGroup(
                        group_title=group_data['group_title'],
                        qa_ids=group_data['qa_ids'],
                        rationale=group_data.get('rationale', 'Grouped by topic similarity')
                    )
                    theme_groups.append(group)

                logger.info(f"Created {len(theme_groups)} theme groups")
                return theme_groups

    except Exception as e:
        logger.error(f"Error in comprehensive grouping: {str(e)}")

    # Fallback: each valid Q&A gets its own theme
    logger.warning("Falling back to individual themes")
    theme_groups = []
    for qa_id, qa_block in valid_qa_blocks.items():  # Use valid_qa_blocks
        group = ThemeGroup(
            group_title=qa_block.theme_title,
            qa_ids=[qa_id],
            rationale="Fallback - individual theme"
        )
        theme_groups.append(group)

    return theme_groups


def apply_grouping_to_index(
    qa_index: Dict[str, QABlock],
    theme_groups: List[ThemeGroup]
):
    """
    Step 4: Apply grouping decisions to the Q&A index.
    """
    logger.info("Applying grouping decisions to Q&A index")

    # Clear any previous assignments
    for qa_block in qa_index.values():
        qa_block.assigned_group = None

    # Apply group assignments
    for group in theme_groups:
        for qa_id in group.qa_ids:
            if qa_id in qa_index:
                qa_block = qa_index[qa_id]
                qa_block.assigned_group = group
                group.qa_blocks.append(qa_block)
            else:
                logger.warning(f"Q&A ID {qa_id} not found in index")

    # Log statistics
    assigned_count = sum(1 for qa in qa_index.values() if qa.assigned_group)
    logger.info(f"Assigned {assigned_count}/{len(qa_index)} Q&A blocks to groups")


def add_page_numbers_with_footer(doc, bank_symbol, quarter, fiscal_year):
    """Add custom footer with bank info, document title, and page numbers."""
    for section in doc.sections:
        footer = section.footer

        # Clear existing footer content
        footer.paragraphs[0].clear()

        # Create a table in the footer for proper left/right alignment

        # Add a table with 1 row and 2 columns
        tbl = footer.add_table(1, 2, width=doc.sections[0].page_width - doc.sections[0].left_margin - doc.sections[0].right_margin)
        tbl.autofit = False
        tbl.allow_autofit = False

        # Hide table borders
        tbl.style = 'Table Grid'
        for row in tbl.rows:
            for cell in row.cells:
                # Remove all borders
                tc = cell._element
                tcPr = tc.get_or_add_tcPr()
                tcBorders = parse_xml(f'<w:tcBorders {nsdecls("w")}><w:top w:val="nil"/><w:left w:val="nil"/><w:bottom w:val="nil"/><w:right w:val="nil"/></w:tcBorders>')
                tcPr.append(tcBorders)

        # Left cell - bank info and doc title
        left_cell = tbl.rows[0].cells[0]
        left_para = left_cell.paragraphs[0]
        left_para.alignment = WD_ALIGN_PARAGRAPH.LEFT
        left_text = f"{bank_symbol} | {quarter}/{str(fiscal_year)[-2:]} | Investor Call - Key Themes"
        run_left = left_para.add_run(left_text)
        run_left.font.size = Pt(9)
        run_left.font.color.rgb = RGBColor(68, 68, 68)

        # Right cell - page number
        right_cell = tbl.rows[0].cells[1]
        right_para = right_cell.paragraphs[0]
        right_para.alignment = WD_ALIGN_PARAGRAPH.RIGHT

        # Add page number field
        run = right_para.add_run()
        fldChar1 = OxmlElement('w:fldChar')
        fldChar1.set(qn('w:fldCharType'), 'begin')
        run._element.append(fldChar1)

        instrText = OxmlElement('w:instrText')
        instrText.text = 'PAGE'
        run._element.append(instrText)

        fldChar2 = OxmlElement('w:fldChar')
        fldChar2.set(qn('w:fldCharType'), 'end')
        run._element.append(fldChar2)

        # Add "| Page" text after page number
        run_page = right_para.add_run(' | Page')
        run_page.font.size = Pt(9)
        run_page.font.color.rgb = RGBColor(68, 68, 68)


def add_theme_header_with_background(doc, theme_number, theme_title):
    """Add a theme header with dark blue text on light blue background."""
    heading = doc.add_paragraph()
    heading.paragraph_format.space_before = Pt(12)
    heading.paragraph_format.space_after = Pt(8)
    heading.paragraph_format.keep_with_next = True

    # Create the full header text
    full_text = f"Theme {theme_number}: {theme_title}"
    run = heading.add_run(full_text)
    run.font.size = Pt(14)
    run.font.bold = True
    run.font.color.rgb = RGBColor(31, 73, 125)  # Dark blue

    # Add shading to the paragraph (light blue background)
    shading_elm = OxmlElement('w:shd')
    shading_elm.set(qn('w:val'), 'clear')
    shading_elm.set(qn('w:color'), 'auto')
    shading_elm.set(qn('w:fill'), 'D4E1F5')  # Light blue background
    heading._element.get_or_add_pPr().append(shading_elm)

    return heading


def create_optimized_document(
    theme_groups: List[ThemeGroup],
    bank_name: str,
    fiscal_year: int,
    quarter: str,
    output_path: str
):
    """
    Step 5: Create Word document with grouped themes matching call summary style.
    """
    doc = Document()

    # Get bank symbol (assuming it's the ticker from the first part of bank_name)
    bank_symbol = bank_name.split()[0] if bank_name else "RBC"
    if bank_name == "Royal Bank of Canada":
        bank_symbol = "RY"

    # Set narrow margins for content-heavy document
    sections = doc.sections
    for section in sections:
        section.top_margin = Inches(0.5)
        section.bottom_margin = Inches(0.6)  # More space for footer
        section.left_margin = Inches(0.6)
        section.right_margin = Inches(0.5)
        section.gutter = Inches(0)

    # Add page numbers and footer
    add_page_numbers_with_footer(doc, bank_symbol, quarter, fiscal_year)

    # Check for banner image in config directories
    etl_dir = os.path.dirname(os.path.abspath(__file__))
    banner_path = None

    # First check in key_themes config directory
    config_dir = os.path.join(etl_dir, 'config')
    for ext in ['jpg', 'jpeg', 'png']:
        potential_banner = os.path.join(config_dir, f'banner.{ext}')
        if os.path.exists(potential_banner):
            banner_path = potential_banner
            break

    # If not found, check in call_summary config directory (fallback)
    if not banner_path:
        call_summary_config_dir = os.path.join(os.path.dirname(etl_dir), 'call_summary', 'config')
        for ext in ['jpg', 'jpeg', 'png']:
            potential_banner = os.path.join(call_summary_config_dir, f'banner.{ext}')
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
            last_paragraph.paragraph_format.space_after = Pt(6)

            logger.info(f"Banner added from: {banner_path}")
        except Exception as e:
            logger.warning(f"Could not add banner: {str(e)}")

    # No title page - go straight to content after banner

    # Add detailed content for each theme group
    for i, group in enumerate(theme_groups, 1):
        # Use the new styled theme header with background
        add_theme_header_with_background(doc, i, group.group_title)

        # No metadata about conversation count - removed per request

        # Sort Q&A blocks by position for logical flow
        sorted_blocks = sorted(group.qa_blocks, key=lambda x: x.position)

        # Add formatted content for each Q&A in this group
        for j, qa_block in enumerate(sorted_blocks, 1):
            # Add conversation sub-header with underlining
            conv_para = doc.add_paragraph()
            conv_para.paragraph_format.space_before = Pt(6)
            conv_para.paragraph_format.space_after = Pt(4)

            # Add the entire "Conversation X:" with underline and black font
            conv_text = f"Conversation {j}:"
            conv_run = conv_para.add_run(conv_text)
            conv_run.font.underline = True
            conv_run.font.size = Pt(11)
            conv_run.font.color.rgb = RGBColor(0, 0, 0)  # Black color

            # Add the formatted content with HTML processing and indentation
            content = qa_block.formatted_content or qa_block.original_content
            for line in content.split('\n'):
                if line.strip():
                    # Skip horizontal rules
                    if line.strip() in ['---', '***', '___', '<hr>', '<hr/>', '<hr />']:
                        continue

                    # Create paragraph with indentation and parse HTML
                    p = doc.add_paragraph()
                    p.paragraph_format.left_indent = Inches(0.3)  # Indent conversation content
                    p.paragraph_format.space_after = Pt(3)
                    p.paragraph_format.line_spacing = 1.15

                    # Use HTML parser to add formatted text
                    parser = HTMLToDocx(p, font_size=Pt(9))
                    parser.feed(line.strip())
                    parser.close()

                    # If no content was added (empty line), add it as plain text
                    if not p.runs:
                        run = p.add_run(line.strip())
                        run.font.size = Pt(9)

            # Add subtle separator between conversations within the same theme
            if j < len(sorted_blocks):
                separator = doc.add_paragraph()
                separator.paragraph_format.left_indent = Inches(0.3)  # Match conversation indentation
                separator.paragraph_format.space_before = Pt(6)
                separator.paragraph_format.space_after = Pt(6)
                # Add a subtle horizontal line
                separator_run = separator.add_run("_" * 50)
                separator_run.font.size = Pt(8)
                separator_run.font.color.rgb = RGBColor(200, 200, 200)

        # No page breaks between themes - let them flow continuously
        # Add a bit more spacing between themes instead
        if i < len(theme_groups):
            spacing = doc.add_paragraph()
            spacing.paragraph_format.space_before = Pt(12)
            spacing.paragraph_format.space_after = Pt(12)

    # Save document
    doc.save(output_path)
    logger.info(f"Document saved to {output_path}")


async def main():
    """Main ETL function with optimized parallel processing."""
    parser = argparse.ArgumentParser(
        description='Generate key themes report with optimized grouping'
    )
    parser.add_argument('--bank', required=True,
                       help='Bank name, ticker, or ID')
    parser.add_argument('--year', type=int, required=True,
                       help='Fiscal year')
    parser.add_argument('--quarter', required=True,
                       help='Quarter (Q1, Q2, Q3, Q4)')
    parser.add_argument('--output-dir',
                       default='src/aegis/etls/key_themes/output',
                       help='Output directory for reports')
    parser.add_argument('--no-pdf', action='store_true',
                       help='Skip PDF generation')

    args = parser.parse_args()

    # Create execution context
    execution_id = str(uuid.uuid4())
    ssl_config = setup_ssl()
    auth_config = await setup_authentication(execution_id, ssl_config)

    context = {
        'execution_id': execution_id,
        'ssl_config': ssl_config,
        'auth_config': auth_config
    }

    try:
        # Resolve bank information
        bank_info = await resolve_bank_info(args.bank, context)
        logger.info(f"Processing key themes for {bank_info['name']} ({bank_info['ticker']})")

        # Add bank info to context for all prompts
        context['bank_name'] = bank_info['name']
        context['bank_symbol'] = bank_info['ticker']
        context['quarter'] = args.quarter
        context['fiscal_year'] = args.year

        # Step 1: Load all Q&A blocks into index
        qa_index = await load_qa_blocks(
            bank_info['name'],
            args.year,
            args.quarter,
            context
        )

        if not qa_index:
            logger.error("No Q&A blocks found")
            return 1

        # Step 2: Process all Q&A blocks independently (parallel)
        await process_all_qa_blocks(qa_index, context)

        # Step 3: Determine comprehensive grouping
        theme_groups = await determine_comprehensive_grouping(qa_index, context)

        # Step 4: Apply grouping to index
        apply_grouping_to_index(qa_index, theme_groups)

        # Step 5: Create output document
        os.makedirs(args.output_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_filename = f"{bank_info['ticker']}_{args.year}_{args.quarter}_optimized_themes_{timestamp}"
        docx_path = os.path.join(args.output_dir, f"{base_filename}.docx")

        logger.info(f"Generating Word document: {docx_path}")
        create_optimized_document(
            theme_groups,
            bank_info['name'],
            args.year,
            args.quarter,
            docx_path
        )

        # Generate PDF if requested
        if not args.no_pdf:
            pdf_path = os.path.join(args.output_dir, f"{base_filename}.pdf")
            logger.info(f"Generating PDF: {pdf_path}")
            convert_docx_to_pdf(docx_path, pdf_path)

        # Report statistics
        total_qa = sum(len(group.qa_blocks) for group in theme_groups)
        invalid_qa = sum(1 for qa in qa_index.values() if not qa.is_valid)
        logger.info(f"✓ Key themes report generated successfully")
        logger.info(f"  Theme Groups: {len(theme_groups)}")
        logger.info(f"  Valid Q&As: {total_qa}")
        if invalid_qa > 0:
            logger.info(f"  Invalid Q&As filtered: {invalid_qa}")
        logger.info(f"  DOCX: {docx_path}")

        return 0

    except Exception as e:
        logger.error(f"Error generating optimized themes report: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))