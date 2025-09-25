"""
Script to insert realistic Q&A test data for Royal Bank of Canada.
"""

import asyncio
from datetime import datetime
from sqlalchemy import text
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger

setup_logging()
logger = get_logger()

# Realistic Q&A data for RBC Q3 2024 earnings call
TEST_QA_DATA = [
    {
        "qa_group_id": 1,
        "speaker_block_id": 1,
        "content": """
Analyst (John Smith - Goldman Sachs): Thank you for taking my question. I wanted to start with net interest margin. You mentioned the 15 basis point increase this quarter to 2.45%. Can you talk about what's driving that improvement and how we should think about the trajectory going forward, especially given the rate environment we're seeing?
""".strip()
    },
    {
        "qa_group_id": 1,
        "speaker_block_id": 2,
        "content": """
CEO (David McKay): Thanks John, great question. So the NIM expansion, um, we're really seeing three key drivers here. First, our asset repricing is continuing to benefit from the rate hikes we saw earlier in the cycle. Second, we've been very disciplined on deposit costs - our total deposit costs increased only 5 basis points this quarter despite competitive pressures. And third, our asset mix is improving with stronger growth in higher-yielding commercial loans.

Looking forward, we expect NIM to remain relatively stable in the 2.40% to 2.50% range through fiscal 2025. We're being conservative here because we do anticipate some pressure from potential rate cuts in the second half of next year, but our strong deposit franchise and disciplined pricing should help offset most of that impact.
""".strip()
    },
    {
        "qa_group_id": 2,
        "speaker_block_id": 3,
        "content": """
Analyst (Sarah Johnson - JP Morgan): Thanks. I'd like to ask about your capital position. Your CET1 ratio improved to 13.2% this quarter. How are you thinking about capital deployment, especially given the regulatory changes coming next year? And should we expect any changes to your dividend or buyback strategy?
""".strip()
    },
    {
        "qa_group_id": 2,
        "speaker_block_id": 4,
        "content": """
CFO (Nadine Ahn): Thank you Sarah. Yes, we're very pleased with our capital position. The 13.2% CET1 ratio gives us significant flexibility, especially as we're already well above the regulatory minimum of 11.5% that comes into effect next year.

In terms of deployment, our priorities remain unchanged. First, we want to support organic growth - we're seeing great opportunities in our commercial banking and wealth management businesses. Second, we'll continue our progressive dividend policy. We increased our dividend by 3% this quarter, and we remain committed to that 40-50% payout ratio target.

On buybacks, we did 2 billion in buybacks this quarter, and we have authorization for another 3 billion. We'll be opportunistic here, but given where our shares are trading and our strong capital generation, I'd expect us to remain active in the market.
""".strip()
    },
    {
        "qa_group_id": 3,
        "speaker_block_id": 5,
        "content": """
Analyst (Mike Chen - Bank of America): Good morning. I wanted to dig into credit quality a bit. Your PCL ratio came in at 27 basis points, which was better than expected. Can you provide some color on what you're seeing across your portfolios, particularly in commercial real estate and the consumer side?
""".strip()
    },
    {
        "qa_group_id": 3,
        "speaker_block_id": 6,
        "content": """
Chief Risk Officer (Graeme Hepworth): Thanks Mike. Credit quality remains very strong across our portfolios. The 27 basis point PCL ratio reflects both the quality of our underwriting and the resilience we're seeing in the Canadian economy.

On commercial real estate specifically, our exposure is about 52 billion, but it's important to note that 65% of that is in multi-family residential, which continues to perform very well given the housing shortage in Canada. Office represents only about 8 billion, and we've been very selective there - focusing on Class A properties in prime locations with strong sponsors.

On the consumer side, our Canadian mortgage portfolio continues to perform exceptionally well. Delinquencies remain below pre-pandemic levels at just 14 basis points. We are seeing some normalization in credit cards and auto loans, but nothing concerning - these are returning to more historical levels after being artificially low during the pandemic.

We're maintaining our full-year PCL guidance of 25 to 30 basis points. We think that's prudent given some of the economic uncertainty, but based on current trends, we'd expect to be at the lower end of that range.
""".strip()
    },
    {
        "qa_group_id": 4,
        "speaker_block_id": 7,
        "content": """
Analyst (Lisa Wong - Barclays): Thank you. Can you talk about your technology investments and digital transformation? You mentioned 1.3 billion in tech spend this quarter. How much of that is maintenance versus new capabilities, and what kind of efficiency gains are you expecting?
""".strip()
    },
    {
        "qa_group_id": 4,
        "speaker_block_id": 8,
        "content": """
CEO (David McKay): Great question Lisa. Technology is absolutely critical to our strategy. Of that 1.3 billion quarterly spend, about 60% is going toward new capabilities and transformation, with 40% on maintenance and regulatory requirements.

The investments are really in three areas. First, we're modernizing our core banking platforms - moving to cloud, upgrading our data infrastructure. This is foundational work that will drive efficiency for years to come. Second, we're investing heavily in AI and machine learning capabilities, particularly in risk management and customer service. And third, we're enhancing our digital channels - our mobile app now has 7.2 million active users, up 12% year-over-year.

On efficiency, we're already seeing benefits. Our cost-to-income ratio improved 120 basis points this quarter to 57.3%. We've been able to handle 15% volume growth with flat headcount in our retail operations. And our straight-through processing rate for simple transactions is now over 95%.

Looking forward, we expect these investments to drive our efficiency ratio below 55% by the end of fiscal 2025, while also improving customer satisfaction scores, which are already at record highs.
""".strip()
    }
]

async def insert_test_data():
    """Insert test Q&A data into the transcripts table."""
    async with get_connection() as conn:
        # First, clear any existing test data for RBC Q3 2024
        await conn.execute(text("""
            DELETE FROM aegis_transcripts
            WHERE company_name = 'Royal Bank of Canada'
            AND fiscal_year = 2024
            AND fiscal_quarter = 'Q3'
            AND section_name = 'Q&A'
        """))
        logger.info("Cleared existing test data")

        # Insert test Q&A data
        for idx, qa_data in enumerate(TEST_QA_DATA):
            await conn.execute(text("""
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
                    chunk_id,
                    chunk_content,
                    created_at,
                    updated_at
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
                    :chunk_id,
                    :chunk_content,
                    :created_at,
                    :updated_at
                )
            """), {
                "file_path": "/data/transcripts/RBC_Q3_2024_earnings_call.txt",
                "filename": "RBC_Q3_2024_earnings_call.txt",
                "date_last_modified": datetime.now(),
                "title": "Royal Bank of Canada Q3 2024 Earnings Call",
                "transcript_type": "earnings_call",
                "event_id": "EC_RBC_2024Q3",
                "version_id": "v1",
                "fiscal_year": 2024,
                "fiscal_quarter": "Q3",
                "institution_type": "Canadian_Banks",
                "institution_id": "RY-CA",
                "ticker": "RY",
                "company_name": "Royal Bank of Canada",
                "section_name": "Q&A",
                "speaker_block_id": qa_data["speaker_block_id"],
                "qa_group_id": qa_data["qa_group_id"],
                "chunk_id": idx + 1,
                "chunk_content": qa_data["content"],
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            })

        await conn.commit()
        logger.info(f"✓ Inserted {len(TEST_QA_DATA)} Q&A blocks for RBC Q3 2024")

        # Verify the data
        result = await conn.execute(text("""
            SELECT qa_group_id, speaker_block_id,
                   substring(chunk_content, 1, 100) as content_preview
            FROM aegis_transcripts
            WHERE company_name = 'Royal Bank of Canada'
            AND fiscal_year = 2024
            AND fiscal_quarter = 'Q3'
            AND section_name = 'Q&A'
            ORDER BY qa_group_id, speaker_block_id
        """))

        logger.info("\nInserted data preview:")
        for row in result:
            logger.info(f"  QA {row[0]}, Block {row[1]}: {row[2]}...")

if __name__ == "__main__":
    asyncio.run(insert_test_data())