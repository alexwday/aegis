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
import yaml
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.style import WD_STYLE_TYPE
import re

# Import direct transcript functions
from aegis.model.subagents.transcripts.retrieval import retrieve_full_section
from aegis.model.subagents.transcripts.formatting import format_full_section_chunks
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication
from aegis.connections.llm_connector import complete_with_tools
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger
from aegis.utils.settings import config

# Initialize logging
setup_logging()
logger = get_logger()

# =============================================================================
# MODEL CONFIGURATION OVERRIDES
# =============================================================================
# Configure which models to use for different stages of the ETL process.
# Set to None to use the default model from config (config.llm.large.model)
# 
# O-series models (o1, o1-mini, o3, etc.) offer superior reasoning capabilities
# and now support tool/function calling (as of December 2024).
# They don't support the temperature parameter but excel at complex analysis.
# 
# Available models include:
# - O-series: o1, o1-mini, o3, o3-mini (better reasoning, no temperature)
# - GPT-4 series: gpt-4, gpt-4-turbo, gpt-4o (standard capabilities)
# =============================================================================

MODEL_OVERRIDES = {
    # Model for generating research plans (uses tool calling)
    "RESEARCH_PLAN_MODEL": None,  # e.g., "o1-mini", "gpt-4-turbo", "gpt-4o", or None for default
    
    # Model for extracting category data (uses tool calling)  
    "CATEGORY_EXTRACTION_MODEL": None,  # e.g., "o1", "o3-mini", "gpt-4o", or None for default
}

# Helper function to get model for each stage
def get_model_for_stage(stage: str) -> str:
    """Get the model to use for a specific stage, with override support."""
    override = MODEL_OVERRIDES.get(stage)
    if override:
        logger.info(f"Using override model for {stage}: {override}")
        return override
    default = config.llm.large.model
    logger.debug(f"Using default model for {stage}: {default}")
    return default


def load_research_plan_config():
    """Load the research plan prompt and tool definition from YAML file."""
    prompt_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 
        'research_plan_prompt.yaml'
    )
    with open(prompt_path, 'r') as f:
        config = yaml.safe_load(f)
    return {
        'system_template': config['system_template'],
        'tool': config['tool']
    }


def load_category_extraction_config():
    """Load the category extraction prompt and tool definition from YAML file."""
    prompt_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 
        'category_extraction_prompt.yaml'
    )
    with open(prompt_path, 'r') as f:
        config = yaml.safe_load(f)
    return {
        'system_template': config['system_template'],
        'tool': config['tool']
    }


def add_structured_content_to_doc(doc, category_data: dict) -> None:
    """
    Add structured category data directly to Word document with proper formatting.
    
    Args:
        doc: Word document object
        category_data: Dictionary with title, summary_statements, evidence structure
    """
    if category_data.get('rejected', False):
        return  # Skip rejected categories
    
    # Add the category title as a heading
    heading = doc.add_heading(category_data['title'], level=2)
    # Reduce heading font size and spacing
    for run in heading.runs:
        run.font.size = Pt(12)
    heading.paragraph_format.space_before = Pt(4)  # Minimal space before new section
    heading.paragraph_format.space_after = Pt(1)   # Almost no space after title
    
    # Process each summary statement
    for statement_data in category_data.get('summary_statements', []):
        # Add the statement as a bullet point
        statement_para = doc.add_paragraph(style='List Bullet')
        statement_run = statement_para.add_run(statement_data['statement'])
        statement_run.font.size = Pt(10)
        statement_para.paragraph_format.space_after = Pt(2)
        statement_para.paragraph_format.line_spacing = 1.0
        
        # Add supporting evidence as indented quotes (no bullets)
        if statement_data.get('evidence'):
            for evidence in statement_data['evidence']:
                # Create indented paragraph for quotes/evidence (no bullet)
                evidence_para = doc.add_paragraph()
                # Set significant indentation from both sides
                evidence_para.paragraph_format.left_indent = Inches(0.75)   # Much more left indent
                evidence_para.paragraph_format.right_indent = Inches(0.75)  # Much more right indent
                evidence_para.paragraph_format.space_after = Pt(1)
                evidence_para.paragraph_format.line_spacing = 1.0
                
                # Add the evidence content in italics with smaller font
                if evidence['type'] == 'quote':
                    content_run = evidence_para.add_run(f'"{evidence["content"]}"')
                else:  # paraphrase
                    content_run = evidence_para.add_run(evidence['content'])
                
                content_run.italic = True
                content_run.font.size = Pt(8)
                content_run.font.color.rgb = RGBColor(64, 64, 64)  # Dark gray
                
                # Add speaker attribution
                speaker_run = evidence_para.add_run(f' — {evidence["speaker"]}')
                speaker_run.italic = True
                speaker_run.font.size = Pt(8)
                speaker_run.font.color.rgb = RGBColor(96, 96, 96)  # Lighter gray






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
        
        # Load research plan configuration (prompt + tool)
        research_config = load_research_plan_config()
        
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
        system_prompt = research_config['system_template'].format(
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
        
        # Make tool call to generate research plan
        logger.info(
            "etl.call_summary.calling_llm_research_plan",
            execution_id=execution_id
        )
        
        try:
            response = complete_with_tools(
                messages=messages,
                tools=[research_config['tool']],
                context=context,
                llm_params={
                    "model": get_model_for_stage("RESEARCH_PLAN_MODEL")
                }
            )
            
            # Parse the tool response
            tool_call = response['choices'][0]['message']['tool_calls'][0]
            research_plan_data = json.loads(tool_call['function']['arguments'])
            
            logger.info(
                "etl.call_summary.research_plan_generated",
                execution_id=execution_id,
                num_plans=len(research_plan_data['category_plans'])
            )
            
            # Validate we got plans for all categories
            if len(research_plan_data['category_plans']) != len(categories):
                logger.warning(
                    "etl.call_summary.plan_count_mismatch",
                    execution_id=execution_id,
                    expected=len(categories),
                    received=len(research_plan_data['category_plans'])
                )
            
        except Exception as e:
            logger.error(
                "etl.call_summary.research_plan_error",
                execution_id=execution_id,
                error=str(e)
            )
            return f"❌ Error generating research plan: {str(e)}"
        
        # Step 7: SECOND STAGE - Extract content for each category using tool calling
        category_results = []
        
        # Load category extraction configuration
        extraction_config = load_category_extraction_config()
        
        for i, category in enumerate(categories, 1):
            logger.info(
                "etl.call_summary.processing_category",
                execution_id=execution_id,
                category_index=i,
                category_name=category["category_name"],
                section=category["transcripts_section"]
            )
            
            # Get research plan for this category
            category_plan = next(
                (p for p in research_plan_data['category_plans'] 
                 if p['name'] == category['category_name']),
                None
            )
            
            if not category_plan:
                logger.warning(
                    "etl.call_summary.no_plan_for_category",
                    execution_id=execution_id,
                    category_name=category["category_name"]
                )
                category_plan = {"plan": "No specific plan available"}
            
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
                # Create rejected result
                category_results.append({
                    "index": i,
                    "name": category["category_name"],
                    "rejected": True,
                    "rejection_reason": f"No {category['transcripts_section']} section data available"
                })
                continue
            
            # Format the chunks for this category
            formatted_section = format_full_section_chunks(
                chunks=chunks,
                combo=combo,
                context=context
            )
            
            # Format previous sections summary (keep minimal)
            previous_summary = ""
            if category_results:
                # Just list category names that were completed
                completed_names = [r['name'] for r in category_results if not r.get('rejected', False)]
                if completed_names:
                    previous_summary = f"Already completed: {', '.join(completed_names)}"
                else:
                    previous_summary = "No previous sections completed yet"
            
            # Format system prompt with ALL context
            system_prompt = extraction_config['system_template'].format(
                category_index=i,
                total_categories=len(categories),
                bank_name=bank_info['bank_name'],
                quarter=quarter,
                fiscal_year=fiscal_year,
                category_name=category['category_name'],
                category_description=category['category_description'],
                transcripts_section=category['transcripts_section'],
                research_plan=category_plan['plan'],
                previous_sections=previous_summary
            )
            
            # Keep user prompt minimal - just the transcript section
            user_prompt = f"Extract content from this transcript section:\n\n{formatted_section}"
            
            # Build messages
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            
            # Make tool call
            try:
                response = complete_with_tools(
                    messages=messages,
                    tools=[extraction_config['tool']],
                    context=context,
                    llm_params={
                        "model": get_model_for_stage("CATEGORY_EXTRACTION_MODEL")
                    }
                )
                
                # Parse the tool response
                tool_call = response['choices'][0]['message']['tool_calls'][0]
                extracted_data = json.loads(tool_call['function']['arguments'])
                
                # Add index and name
                extracted_data['index'] = i
                extracted_data['name'] = category['category_name']
                
                category_results.append(extracted_data)
                
                logger.info(
                    "etl.call_summary.category_completed",
                    execution_id=execution_id,
                    category_index=i,
                    category_name=category["category_name"],
                    rejected=extracted_data.get('rejected', False)
                )
                
            except Exception as e:
                logger.error(
                    "etl.call_summary.category_extraction_error",
                    execution_id=execution_id,
                    category_name=category["category_name"],
                    error=str(e)
                )
                # Add error result
                category_results.append({
                    "index": i,
                    "name": category["category_name"],
                    "rejected": True,
                    "rejection_reason": f"Error extracting content: {str(e)}"
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
        
        # Set narrow margins for the entire document
        sections = doc.sections
        for section in sections:
            section.top_margin = Inches(0.5)
            section.bottom_margin = Inches(0.5)
            section.left_margin = Inches(0.5)
            section.right_margin = Inches(0.5)
        
        # Title Page
        # Add bank name as main title
        title = doc.add_heading(bank_info["bank_name"], 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        for run in title.runs:
            run.font.size = Pt(20)
        title.paragraph_format.space_after = Pt(3)  # Reduced from 6
        
        # Add ticker symbol
        ticker = doc.add_paragraph()
        ticker.alignment = WD_ALIGN_PARAGRAPH.CENTER
        ticker_run = ticker.add_run(f'({bank_info["bank_symbol"]})')
        ticker_run.font.size = Pt(14)
        ticker_run.font.color.rgb = RGBColor(64, 64, 64)
        ticker.paragraph_format.space_after = Pt(6)  # Reduced from 12
        
        # Add report type
        report_type = doc.add_heading('Earnings Call Summary', 1)
        report_type.alignment = WD_ALIGN_PARAGRAPH.CENTER
        for run in report_type.runs:
            run.font.size = Pt(16)
        report_type.paragraph_format.space_after = Pt(3)  # Reduced from 6
        
        # Add period
        period = doc.add_paragraph()
        period.alignment = WD_ALIGN_PARAGRAPH.CENTER
        period_run = period.add_run(f'{quarter} {fiscal_year}')
        period_run.font.size = Pt(12)
        period_run.bold = True
        period.paragraph_format.space_after = Pt(6)  # Reduced from 18 - minimal space before first content
        
        # Add generation date
        generated = doc.add_paragraph()
        generated.alignment = WD_ALIGN_PARAGRAPH.CENTER
        gen_run = generated.add_run(f'Generated: {datetime.now().strftime("%B %d, %Y")}')
        gen_run.font.size = Pt(9)
        gen_run.font.color.rgb = RGBColor(128, 128, 128)
        
        # Add page break after title page
        doc.add_page_break()
        
        # Add each valid category section
        for i, category_data in enumerate(valid_categories, 1):
            # Use the new function to add structured content
            add_structured_content_to_doc(doc, category_data)
            
            # Add minimal spacing between sections (if not last section)
            if i < len(valid_categories):
                para = doc.add_paragraph()
                para.paragraph_format.space_after = Pt(3)
            
            # Log progress
            logger.debug(
                "etl.call_summary.category_added_to_doc",
                execution_id=execution_id,
                category_name=category_data['name'],
                num_statements=len(category_data.get('summary_statements', []))
            )
        
        # Save the document
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        filename = f"{bank_info['bank_symbol']}_{fiscal_year}_{quarter}_{execution_id[:8]}.docx"
        filepath = os.path.join(output_dir, filename)
        doc.save(filepath)
        
        logger.info(
            "etl.call_summary.document_saved",
            execution_id=execution_id,
            filepath=filepath,
            valid_categories=len(valid_categories),
            rejected_categories=len(category_results) - len(valid_categories)
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