"""
Key Themes ETL - Extract and Group Earnings Call Themes

This ETL processes earnings call Q&A sessions to:
1. Load all Q&A blocks into an index
2. Process each independently to extract themes (parallelizable)
3. Make ONE comprehensive grouping decision with full visibility
4. Apply grouping programmatically
5. Generate formatted document

Usage:
    python -m aegis.etls.key_themes.main --bank "Royal Bank of Canada" --year 2024 --quarter Q3
"""

import argparse
import asyncio
import json
import sys
import uuid
import os
import time
# import re  # No longer needed - using HTML parser instead
from datetime import datetime
from typing import Dict, Any, List, Optional
from sqlalchemy import text
import yaml
from docx import Document
from docx.shared import Pt, Inches, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_COLOR_INDEX
from docx.oxml import OxmlElement, parse_xml
from docx.oxml.ns import qn, nsdecls
import hashlib
from collections import defaultdict
from html.parser import HTMLParser

# Import document converter functions
from aegis.etls.key_themes.document_converter import (
    convert_docx_to_pdf,
    get_standard_report_metadata,
    theme_groups_to_markdown
)

# Import direct transcript functions
from aegis.model.subagents.transcripts.retrieval import retrieve_full_section
from aegis.model.subagents.transcripts.formatting import format_full_section_chunks
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication
from aegis.connections.llm_connector import complete, complete_with_tools
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger
from aegis.etls.key_themes.config.config import get_model, TEMPERATURE, MAX_TOKENS

# Initialize logging
setup_logging()
logger = get_logger()


class HTMLToDocx(HTMLParser):
    """
    Parser to convert HTML-formatted text to Word document formatting.
    Supports: <b>, <strong>, <i>, <em>, <u>, <mark>, <span style="..."> tags and their nesting.
    """

    def __init__(self, paragraph, font_size=Pt(9)):
        super().__init__()
        self.paragraph = paragraph
        self.font_size = font_size
        self.text_buffer = ""
        self.format_stack = []  # Stack to track nested formatting
        self.style_stack = []  # Stack for span styles

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
            # Check for background-color style in mark tag
            style_dict = self._parse_style(attrs)
            if style_dict and 'background-color' in style_dict:
                self.format_stack.append('highlight_yellow')
            else:
                self.format_stack.append('highlight')
        elif tag == 'span':
            # Parse style attribute for span tags
            style_dict = self._parse_style(attrs)
            self.style_stack.append(style_dict if style_dict else {})

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
        elif tag == 'mark':
            if 'highlight_yellow' in self.format_stack:
                self.format_stack.remove('highlight_yellow')
            elif 'highlight' in self.format_stack:
                self.format_stack.remove('highlight')
        elif tag == 'span' and self.style_stack:
            self.style_stack.pop()

    def handle_data(self, data):
        # Buffer text to handle whitespace properly
        self.text_buffer += data

    def _parse_style(self, attrs):
        """Parse style attribute from HTML tags."""
        style_dict = {}
        for attr_name, attr_value in attrs:
            if attr_name == 'style':
                # Parse CSS style string
                styles = attr_value.split(';')
                for style in styles:
                    if ':' in style:
                        prop, value = style.split(':', 1)
                        style_dict[prop.strip().lower()] = value.strip()
        return style_dict

    def _flush_text(self):
        """Apply formatting and add text to document."""
        if not self.text_buffer:
            return

        run = self.paragraph.add_run(self.text_buffer)

        # Apply span styles if present (from top of stack)
        if self.style_stack:
            current_style = self.style_stack[-1]

            # Apply color
            if 'color' in current_style:
                color_hex = current_style['color'].lstrip('#')
                if color_hex.startswith('1e4d8b'):  # Dark blue for key questions
                    run.font.color.rgb = RGBColor(0x1e, 0x4d, 0x8b)
                elif color_hex.startswith('4d94ff'):  # Medium blue for key answers
                    run.font.color.rgb = RGBColor(0x4d, 0x94, 0xff)

            # Apply font size
            if 'font-size' in current_style:
                size_str = current_style['font-size']
                if 'pt' in size_str:
                    size = float(size_str.replace('pt', '').strip())
                    run.font.size = Pt(size)
                else:
                    run.font.size = self.font_size
            else:
                run.font.size = self.font_size

            # Apply font weight from span
            if 'font-weight' in current_style and current_style['font-weight'] == 'bold':
                run.font.bold = True
        else:
            run.font.size = self.font_size

        # Apply all active formats from format stack
        if 'bold' in self.format_stack:
            run.font.bold = True
        if 'italic' in self.format_stack:
            run.font.italic = True
        if 'underline' in self.format_stack:
            run.font.underline = True
        if 'highlight' in self.format_stack or 'highlight_yellow' in self.format_stack:
            run.font.highlight_color = WD_COLOR_INDEX.YELLOW

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

    # Retry logic for better reliability
    max_retries = 3
    result = None

    for attempt in range(max_retries):
        try:
            response = await complete_with_tools(
                messages=messages,
                tools=[tool_config['tool']],
                context=context,
                llm_params={"model": get_model("theme_extraction"), "temperature": TEMPERATURE, "max_tokens": MAX_TOKENS}
            )

            if response:
                tool_calls = response.get('choices', [{}])[0].get('message', {}).get('tool_calls', [])
                if tool_calls:
                    result = json.loads(tool_calls[0]['function']['arguments'])
                    break  # Success, exit retry loop

        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Retry {attempt + 1}/{max_retries} for theme extraction {qa_block.qa_id}: {str(e)}")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                continue
            else:
                logger.error(f"Error extracting theme for {qa_block.qa_id} after {max_retries} attempts: {str(e)}")
                # Set defaults on error
                qa_block.theme_title = f"Q&A Discussion {qa_block.position}"
                qa_block.summary = "Theme extraction failed"
                qa_block.is_valid = True  # Assume valid on error to avoid losing data
                return

    if result:
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

    # Retry logic for formatting
    max_retries = 2
    for attempt in range(max_retries):
        try:
            response = await complete(messages, context, {"model": get_model("formatting"), "temperature": TEMPERATURE, "max_tokens": MAX_TOKENS})

            if isinstance(response, dict):
                qa_block.formatted_content = response.get('choices', [{}])[0].get('message', {}).get('content', '')
            else:
                qa_block.formatted_content = str(response)
            break  # Success

        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Retry {attempt + 1}/{max_retries} for formatting {qa_block.qa_id}: {str(e)}")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"Error formatting {qa_block.qa_id} after {max_retries} attempts: {str(e)}")
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

    # Retry logic for grouping (critical step)
    max_retries = 3
    result = None

    for attempt in range(max_retries):
        try:
            response = await complete_with_tools(
                messages=messages,
                tools=[tool_config['tool']],
                context=context,
                llm_params={"model": get_model("grouping"), "temperature": TEMPERATURE, "max_tokens": MAX_TOKENS}
            )

            if response:
                tool_calls = response.get('choices', [{}])[0].get('message', {}).get('tool_calls', [])
                if tool_calls:
                    try:
                        result = json.loads(tool_calls[0]['function']['arguments'])
                        logger.debug(f"Grouping result keys: {result.keys()}")
                        break  # Success
                    except json.JSONDecodeError as je:
                        if attempt < max_retries - 1:
                            logger.warning(f"JSON decode error on attempt {attempt + 1}: {je}")
                            await asyncio.sleep(2 ** attempt)
                            continue
                        else:
                            raise

        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Retry {attempt + 1}/{max_retries} for grouping: {str(e)}")
                await asyncio.sleep(2 ** attempt)
                continue
            else:
                logger.error(f"Failed to group themes after {max_retries} attempts: {e}")
                return None

    if result:
        # Check if the expected key exists
        if 'theme_groups' not in result:
            logger.error(f"Missing 'theme_groups' key in result. Available keys: {list(result.keys())}")
            logger.error(f"Full result: {json.dumps(result, indent=2)}")
            raise KeyError("'theme_groups' key not found in LLM response")

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


def create_document(
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
    """Main ETL function for key themes extraction."""
    parser = argparse.ArgumentParser(
        description='Generate key themes report from earnings call Q&A'
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
        base_filename = f"{bank_info['ticker']}_{args.year}_{args.quarter}_key_themes_{timestamp}"
        docx_path = os.path.join(args.output_dir, f"{base_filename}.docx")

        logger.info(f"Generating Word document: {docx_path}")
        create_document(
            theme_groups,
            bank_info['name'],
            args.year,
            args.quarter,
            docx_path
        )

        # Generate PDF if requested
        pdf_path = None
        pdf_filename = None
        if not args.no_pdf:
            pdf_path = os.path.join(args.output_dir, f"{base_filename}.pdf")
            pdf_filename = f"{base_filename}.pdf"
            logger.info(f"Generating PDF: {pdf_path}")
            pdf_result = convert_docx_to_pdf(docx_path, pdf_path)
            if not pdf_result:
                logger.warning("PDF generation failed")
                pdf_path = None
                pdf_filename = None

        # Step 6: Generate Markdown content for database storage
        logger.info("Generating markdown for database storage")
        markdown_content = theme_groups_to_markdown(
            theme_groups,
            bank_info,
            args.quarter,
            args.year
        )

        # Step 7: Save to database
        logger.info("Saving report to aegis_reports table")
        report_metadata = get_standard_report_metadata()
        generation_timestamp = datetime.now()
        execution_id = str(uuid.uuid4())

        try:
            async with get_connection() as conn:
                # Delete any existing report for this bank/period/type combination
                deleted = await conn.execute(text(
                    """
                    DELETE FROM aegis_reports
                    WHERE bank_id = :bank_id
                      AND fiscal_year = :fiscal_year
                      AND quarter = :quarter
                      AND report_type = :report_type
                    RETURNING id
                    """
                ), {
                    "bank_id": bank_info["id"],
                    "fiscal_year": args.year,
                    "quarter": args.quarter,
                    "report_type": report_metadata["report_type"]
                })
                deleted_rows = deleted.fetchall()

                if deleted_rows:
                    logger.info(f"Deleted {len(deleted_rows)} existing key_themes report(s)")

                    # Check if there are any other reports for this bank/period
                    remaining_reports = await conn.execute(text(
                        """
                        SELECT COUNT(*) as count
                        FROM aegis_reports
                        WHERE bank_id = :bank_id
                          AND fiscal_year = :fiscal_year
                          AND quarter = :quarter
                        """
                    ), {
                        "bank_id": bank_info["id"],
                        "fiscal_year": args.year,
                        "quarter": args.quarter
                    })
                    count_result = remaining_reports.scalar()

                    # If no other reports exist, remove 'reports' from availability
                    if count_result == 0:
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
                            "bank_id": bank_info["id"],
                            "fiscal_year": args.year,
                            "quarter": args.quarter
                        })
                        logger.info("Removed 'reports' from aegis_data_availability")

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
                    "bank_id": bank_info["id"],
                    "bank_name": bank_info["name"],
                    "bank_symbol": bank_info.get("ticker", bank_info.get("symbol", "")),
                    "fiscal_year": args.year,
                    "quarter": args.quarter,
                    "local_filepath": docx_path,
                    "s3_document_name": f"{base_filename}.docx",
                    "s3_pdf_name": pdf_filename,
                    "markdown_content": markdown_content,
                    "generation_date": generation_timestamp,
                    "generated_by": "key_themes_etl",
                    "execution_id": execution_id,
                    "metadata": json.dumps({
                        "theme_groups": len(theme_groups),
                        "total_qa_blocks": sum(len(group.qa_blocks) for group in theme_groups),
                        "invalid_qa_filtered": sum(1 for qa in qa_index.values() if not qa.is_valid)
                    })
                })
                await conn.commit()
                report_id = result.fetchone().id
                logger.info(f"Report saved to database with ID: {report_id}")

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
                    "bank_id": bank_info["id"],
                    "fiscal_year": args.year,
                    "quarter": args.quarter
                })

                if update_result.rowcount > 0:
                    await conn.commit()
                    logger.info("Updated aegis_data_availability to include 'reports'")

        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            # Continue even if database save fails

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
        import traceback
        logger.error(f"Error generating key themes report: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.debug(f"Full traceback:\n{traceback.format_exc()}")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))