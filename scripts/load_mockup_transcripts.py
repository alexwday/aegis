#!/usr/bin/env python3
"""
Load mockup transcript data into aegis_transcripts table.
Creates realistic sample data for Canadian and US banks Q1-Q2 2025.
"""

import sys
import random
import json
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.aegis.connections.postgres_connector import get_connection
from src.aegis.utils.logging import get_logger
from sqlalchemy import text
import numpy as np

logger = get_logger()

# Sample transcript content templates
CEO_REMARKS = [
    "Thank you for joining us today for our {quarter} {year} earnings call. We delivered strong results this quarter with revenue growth of {revenue_growth}% and improved efficiency ratios.",
    "Our {quarter} performance reflects the strength of our diversified business model and disciplined expense management. Net income reached ${net_income} billion.",
    "We continue to see momentum across all our business segments, with particularly strong performance in {segment}.",
]

CFO_REMARKS = [
    "Let me provide more detail on our financial performance for {quarter} {year}. Total revenue was ${revenue} billion, up {revenue_growth}% year-over-year.",
    "Our efficiency ratio improved to {efficiency}%, demonstrating our continued focus on operational excellence.",
    "Net interest margin expanded to {nim}%, benefiting from the current rate environment.",
]

ANALYST_QUESTIONS = [
    "Can you provide more color on the loan growth outlook for the remainder of {year}?",
    "How are you thinking about credit provisions given the current economic environment?",
    "What are your expectations for net interest margin as we move through {year}?",
]

MANAGEMENT_ANSWERS = [
    "That's a great question. We're seeing strong loan demand across both commercial and retail segments, and we expect mid-single digit growth to continue.",
    "We remain cautiously optimistic about credit quality. Our provisions reflect a prudent approach given economic uncertainties.",
    "We expect NIM to remain relatively stable, with some potential upside if rates remain elevated.",
]

# Classification categories
CLASSIFICATIONS = {
    1: "Revenue",
    2: "Expenses", 
    3: "Net Income",
    4: "Loan Growth",
    5: "Credit Quality",
    6: "Capital Ratios",
    7: "Efficiency Ratio",
    8: "Net Interest Margin",
    9: "Digital Banking",
    10: "ESG",
    11: "Guidance",
    12: "Market Conditions",
}

def generate_embedding_vector():
    """Generate a random 3072-dimension embedding vector (simulated)."""
    # In production, this would call OpenAI's embedding API
    # For mockup, generate random normalized vector
    vector = np.random.randn(3072)
    # Normalize to unit length (typical for embeddings)
    vector = vector / np.linalg.norm(vector)
    return vector.tolist()

def generate_transcript_chunks(bank_id, bank_name, ticker, fiscal_year, fiscal_quarter):
    """Generate transcript chunks for a bank's earnings call."""
    chunks = []
    speaker_block_id = 0
    qa_group_id = 0
    
    # CEO opening remarks
    speaker_block_id += 1
    ceo_content = random.choice(CEO_REMARKS).format(
        quarter=fiscal_quarter,
        year=fiscal_year,
        revenue_growth=random.randint(3, 12),
        net_income=round(random.uniform(1.5, 4.5), 1),
        segment=random.choice(["wealth management", "capital markets", "retail banking"])
    )
    
    chunks.append({
        "section_name": "MANAGEMENT DISCUSSION SECTION",
        "speaker_block_id": speaker_block_id,
        "qa_group_id": None,
        "speaker": f"CEO - {bank_name}",
        "content": ceo_content,
        "chunk_id": 1,
        "chunk_tokens": len(ceo_content.split()) * 2,  # Rough token estimate
        "classifications": random.sample(list(CLASSIFICATIONS.keys()), k=random.randint(2, 4)),
        "block_summary": f"CEO discusses {fiscal_quarter} {fiscal_year} performance highlights"
    })
    
    # CFO remarks (might be multiple chunks if long)
    speaker_block_id += 1
    cfo_content_parts = []
    for template in random.sample(CFO_REMARKS, k=2):
        content = template.format(
            quarter=fiscal_quarter,
            year=fiscal_year,
            revenue=round(random.uniform(5.0, 15.0), 1),
            revenue_growth=random.randint(3, 12),
            efficiency=random.randint(45, 65),
            nim=round(random.uniform(2.5, 3.5), 2)
        )
        cfo_content_parts.append(content)
    
    # Split CFO remarks into 2 chunks
    for i, content in enumerate(cfo_content_parts, 1):
        chunks.append({
            "section_name": "MANAGEMENT DISCUSSION SECTION",
            "speaker_block_id": speaker_block_id,
            "qa_group_id": None,
            "speaker": f"CFO - {bank_name}",
            "content": content,
            "chunk_id": i,
            "chunk_tokens": len(content.split()) * 2,
            "classifications": random.sample(list(CLASSIFICATIONS.keys()), k=random.randint(2, 4)),
            "block_summary": f"CFO provides detailed financial metrics for {fiscal_quarter}"
        })
    
    # Q&A Section - 3 Q&A exchanges
    for qa_num in range(1, 4):
        qa_group_id += 1
        
        # Analyst question
        speaker_block_id += 1
        question = random.choice(ANALYST_QUESTIONS).format(year=fiscal_year)
        chunks.append({
            "section_name": "Q&A",
            "speaker_block_id": speaker_block_id,
            "qa_group_id": qa_group_id,
            "speaker": f"Analyst {qa_num}",
            "content": question,
            "chunk_id": 1,
            "chunk_tokens": len(question.split()) * 2,
            "classifications": random.sample(list(CLASSIFICATIONS.keys()), k=random.randint(1, 2)),
            "block_summary": f"Analyst asks about {random.choice(['outlook', 'guidance', 'strategy'])}"
        })
        
        # Management answer
        speaker_block_id += 1
        answer = random.choice(MANAGEMENT_ANSWERS)
        chunks.append({
            "section_name": "Q&A",
            "speaker_block_id": speaker_block_id,
            "qa_group_id": qa_group_id,
            "speaker": random.choice([f"CEO - {bank_name}", f"CFO - {bank_name}"]),
            "content": answer,
            "chunk_id": 1,
            "chunk_tokens": len(answer.split()) * 2,
            "classifications": random.sample(list(CLASSIFICATIONS.keys()), k=random.randint(2, 3)),
            "block_summary": f"Management responds regarding {random.choice(['growth prospects', 'risk management', 'financial targets'])}"
        })
    
    return chunks

def load_mockup_data():
    """Load mockup transcript data for Canadian and US banks Q1-Q2 2025."""
    
    # Bank data (matching monitored_institutions.yaml)
    banks = [
        # Canadian Banks
        {"bank_id": 1, "name": "Royal Bank of Canada", "ticker": "RY", "type": "Canadian_Banks"},
        {"bank_id": 2, "name": "Toronto-Dominion Bank", "ticker": "TD", "type": "Canadian_Banks"},
        {"bank_id": 3, "name": "Bank of Nova Scotia", "ticker": "BNS", "type": "Canadian_Banks"},
        {"bank_id": 4, "name": "Bank of Montreal", "ticker": "BMO", "type": "Canadian_Banks"},
        {"bank_id": 5, "name": "Canadian Imperial Bank of Commerce", "ticker": "CM", "type": "Canadian_Banks"},
        {"bank_id": 6, "name": "National Bank of Canada", "ticker": "NA", "type": "Canadian_Banks"},
        {"bank_id": 7, "name": "Laurentian Bank of Canada", "ticker": "LB", "type": "Canadian_Banks"},
        # US Banks
        {"bank_id": 8, "name": "JPMorgan Chase & Co.", "ticker": "JPM", "type": "US_Banks"},
        {"bank_id": 9, "name": "Bank of America Corporation", "ticker": "BAC", "type": "US_Banks"},
        {"bank_id": 10, "name": "Wells Fargo & Company", "ticker": "WFC", "type": "US_Banks"},
        {"bank_id": 11, "name": "Citigroup Inc.", "ticker": "C", "type": "US_Banks"},
        {"bank_id": 12, "name": "Goldman Sachs Group", "ticker": "GS", "type": "US_Banks"},
        {"bank_id": 13, "name": "Morgan Stanley", "ticker": "MS", "type": "US_Banks"},
        {"bank_id": 14, "name": "U.S. Bancorp", "ticker": "USB", "type": "US_Banks"},
    ]
    
    periods = [
        {"year": 2025, "quarter": "Q1"},
        {"year": 2025, "quarter": "Q2"},
    ]
    
    total_records = 0
    
    with get_connection() as conn:
        # Clear existing mockup data
        logger.info("Clearing existing mockup data...")
        conn.execute(text("TRUNCATE TABLE aegis_transcripts"))
        conn.commit()
        
        logger.info("Loading mockup transcript data...")
        
        for bank in banks:
            for period in periods:
                # Generate event IDs
                event_id = f"{random.randint(10000000, 99999999)}"
                version_id = f"{random.randint(10000000, 99999999)}"
                
                # Generate transcript chunks
                chunks = generate_transcript_chunks(
                    bank["bank_id"],
                    bank["name"],
                    bank["ticker"],
                    period["year"],
                    period["quarter"]
                )
                
                # Insert each chunk
                for chunk in chunks:
                    # Generate embedding
                    embedding = generate_embedding_vector()
                    
                    # Convert classifications to arrays
                    classification_names = [CLASSIFICATIONS[id] for id in chunk["classifications"]]
                    
                    # Create paragraph IDs (simulated)
                    num_paragraphs = random.randint(1, 3)
                    paragraph_ids = [f"p{i}" for i in range(1, num_paragraphs + 1)]
                    
                    insert_sql = text("""
                        INSERT INTO aegis_transcripts (
                            file_path,
                            filename,
                            date_last_modified,
                            title,
                            transcript_type,
                            event_id,
                            version_id,
                            fiscal_year,
                            fiscal_quarter,
                            institution_type,
                            institution_id,
                            ticker,
                            company_name,
                            section_name,
                            speaker_block_id,
                            qa_group_id,
                            classification_ids,
                            classification_names,
                            block_summary,
                            chunk_id,
                            chunk_tokens,
                            chunk_content,
                            chunk_paragraph_ids,
                            chunk_embedding
                        ) VALUES (
                            :file_path,
                            :filename,
                            :date_last_modified,
                            :title,
                            :transcript_type,
                            :event_id,
                            :version_id,
                            :fiscal_year,
                            :fiscal_quarter,
                            :institution_type,
                            :institution_id,
                            :ticker,
                            :company_name,
                            :section_name,
                            :speaker_block_id,
                            :qa_group_id,
                            :classification_ids,
                            :classification_names,
                            :block_summary,
                            :chunk_id,
                            :chunk_tokens,
                            :chunk_content,
                            :chunk_paragraph_ids,
                            CAST(:chunk_embedding AS vector)
                        )
                    """)
                    
                    conn.execute(insert_sql, {
                        "file_path": f"/transcripts/{bank['ticker']}/{period['year']}/{period['quarter']}/earnings_call.pdf",
                        "filename": f"{bank['ticker']}_{period['quarter']}_{period['year']}_earnings.pdf",
                        "date_last_modified": datetime.now(timezone.utc),
                        "title": f"{bank['name']} {period['quarter']} {period['year']} Earnings Call",
                        "transcript_type": "Corrected",
                        "event_id": event_id,
                        "version_id": version_id,
                        "fiscal_year": period["year"],
                        "fiscal_quarter": period["quarter"],
                        "institution_type": bank["type"],
                        "institution_id": str(bank["bank_id"]),
                        "ticker": bank["ticker"],
                        "company_name": bank["name"],
                        "section_name": chunk["section_name"],
                        "speaker_block_id": chunk["speaker_block_id"],
                        "qa_group_id": chunk["qa_group_id"],
                        "classification_ids": chunk["classifications"],
                        "classification_names": classification_names,
                        "block_summary": chunk["block_summary"],
                        "chunk_id": chunk["chunk_id"],
                        "chunk_tokens": chunk["chunk_tokens"],
                        "chunk_content": chunk["content"],
                        "chunk_paragraph_ids": paragraph_ids,
                        "chunk_embedding": f"[{','.join(map(str, embedding))}]"
                    })
                    
                    total_records += 1
        
        conn.commit()
        logger.info(f"✓ Successfully loaded {total_records} transcript chunks")
        
        # Show sample of loaded data
        result = conn.execute(text("""
            SELECT 
                ticker, 
                fiscal_year, 
                fiscal_quarter, 
                section_name, 
                COUNT(*) as chunk_count
            FROM aegis_transcripts
            GROUP BY ticker, fiscal_year, fiscal_quarter, section_name
            ORDER BY ticker, fiscal_year, fiscal_quarter, section_name
            LIMIT 10
        """))
        
        logger.info("\nSample of loaded data:")
        logger.info("-" * 60)
        for row in result:
            logger.info(f"{row[0]} {row[1]} {row[2]}: {row[3]} ({row[4]} chunks)")

if __name__ == "__main__":
    load_mockup_data()