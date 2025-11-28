#!/usr/bin/env python
"""
Standalone script to explore the rts_embedding table.

Discovers table structure, tests similarity search, and inspects chunk content
for use in the bank earnings report ETL.

Usage:
    python -m aegis.etls.bank_earnings_report.scripts.explore_rts_table

Features:
    1. Table exploration - distinct banks, quarters, years, row counts
    2. Sample data retrieval - chunks for a specific bank/period
    3. Embedding similarity search - test semantic search with sample queries
    4. JSON field inspection - propositions and tables arrays
"""

import asyncio
import json
import traceback
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from aegis.connections.llm_connector import embed
from aegis.connections.oauth_connector import setup_authentication
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import get_logger, setup_logging
from aegis.utils.ssl import setup_ssl

setup_logging()
logger = get_logger()


# =============================================================================
# Table Exploration
# =============================================================================


async def explore_table_structure() -> Dict[str, Any]:
    """
    Explore the rts_embedding table structure and contents.

    Returns:
        Dict with exploration results
    """
    print("\n" + "=" * 80)
    print("RTS_EMBEDDING TABLE EXPLORATION")
    print("=" * 80)

    results = {}

    try:
        async with get_connection() as conn:
            # Total row count
            result = await conn.execute(text("SELECT COUNT(*) FROM rts_embedding"))
            total_rows = result.scalar()
            results["total_rows"] = total_rows
            print(f"\n📊 Total rows: {total_rows:,}")

            # Distinct banks
            result = await conn.execute(
                text("SELECT DISTINCT bank FROM rts_embedding ORDER BY bank")
            )
            banks = [row[0] for row in result.fetchall()]
            results["banks"] = banks
            print(f"\n🏦 Banks ({len(banks)}):")
            for bank in banks:
                print(f"   - {bank}")

            # Distinct years
            result = await conn.execute(
                text("SELECT DISTINCT year FROM rts_embedding ORDER BY year DESC")
            )
            years = [row[0] for row in result.fetchall()]
            results["years"] = years
            print(f"\n📅 Years: {years}")

            # Distinct quarters
            result = await conn.execute(
                text("SELECT DISTINCT quarter FROM rts_embedding ORDER BY quarter")
            )
            quarters = [row[0] for row in result.fetchall()]
            results["quarters"] = quarters
            print(f"\n📆 Quarters: {quarters}")

            # Data availability by bank/year/quarter
            print("\n📋 Data Availability (chunks per bank/period):")
            print("-" * 60)
            result = await conn.execute(
                text(
                    """
                    SELECT bank, year, quarter, COUNT(*) as chunk_count
                    FROM rts_embedding
                    GROUP BY bank, year, quarter
                    ORDER BY bank, year DESC, quarter DESC
                    """
                )
            )
            availability = []
            for row in result.fetchall():
                availability.append(
                    {
                        "bank": row[0],
                        "year": row[1],
                        "quarter": row[2],
                        "chunk_count": row[3],
                    }
                )
                print(f"   {row[0]:<30} | {row[1]} {row[2]} | {row[3]:>4} chunks")
            results["availability"] = availability
            print("-" * 60)

            # Distinct source sections (sample)
            print("\n📂 Sample Source Sections (first 10):")
            result = await conn.execute(
                text(
                    """
                    SELECT DISTINCT source_section
                    FROM rts_embedding
                    WHERE source_section IS NOT NULL
                    LIMIT 10
                    """
                )
            )
            sections = [row[0] for row in result.fetchall()]
            for section in sections:
                print(f"   - {section}")
            results["sample_sections"] = sections

            # Check embedding dimensions
            print("\n🔢 Embedding Check:")
            result = await conn.execute(
                text(
                    """
                    SELECT
                        id,
                        embedding IS NOT NULL as has_embedding,
                        CASE WHEN embedding IS NOT NULL
                             THEN array_length(embedding::real[], 1)
                             ELSE NULL
                        END as dimensions
                    FROM rts_embedding
                    LIMIT 1
                    """
                )
            )
            row = result.fetchone()
            if row:
                print(f"   Has embeddings: {row[1]}")
                print(f"   Dimensions: {row[2]}")
                results["has_embeddings"] = row[1]
                results["embedding_dimensions"] = row[2]

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"\n❌ Error exploring table: {e}")
        results["error"] = str(e)

    return results


# =============================================================================
# Sample Data Retrieval
# =============================================================================


async def get_sample_chunks(
    bank: str,
    year: int,
    quarter: str,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """
    Retrieve sample chunks for a specific bank/period.

    Args:
        bank: Bank name (e.g., "Royal Bank of Canada")
        year: Fiscal year
        quarter: Quarter (e.g., "Q3")
        limit: Max chunks to retrieve

    Returns:
        List of chunk dicts
    """
    print("\n" + "=" * 80)
    print(f"SAMPLE CHUNKS: {bank} {quarter} {year}")
    print("=" * 80)

    chunks = []

    try:
        async with get_connection() as conn:
            # First check if data exists
            result = await conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM rts_embedding
                    WHERE bank = :bank AND year = :year AND quarter = :quarter
                    """
                ),
                {"bank": bank, "year": year, "quarter": quarter},
            )
            count = result.scalar()
            print(f"\n📊 Total chunks for {bank} {quarter} {year}: {count}")

            if count == 0:
                print("⚠️  No data found for this bank/period")
                return []

            # Get sample chunks
            result = await conn.execute(
                text(
                    """
                    SELECT
                        id,
                        chunk_id,
                        page_no,
                        filename,
                        summary_title,
                        source_section,
                        LENGTH(raw_text) as raw_text_length,
                        raw_text,
                        propositions,
                        tables
                    FROM rts_embedding
                    WHERE bank = :bank AND year = :year AND quarter = :quarter
                    ORDER BY chunk_id
                    LIMIT :limit
                    """
                ),
                {"bank": bank, "year": year, "quarter": quarter, "limit": limit},
            )

            print(f"\n📄 Sample Chunks (first {limit}):\n")
            print("-" * 80)

            for row in result.fetchall():
                chunk = {
                    "id": row[0],
                    "chunk_id": row[1],
                    "page_no": row[2],
                    "filename": row[3],
                    "summary_title": row[4],
                    "source_section": row[5],
                    "raw_text_length": row[6],
                    "raw_text": row[7],
                    "propositions": row[8],
                    "tables": row[9],
                }
                chunks.append(chunk)

                print(f"\n🔹 Chunk {chunk['chunk_id']} (ID: {chunk['id']})")
                print(f"   Page: {chunk['page_no']}")
                print(f"   File: {chunk['filename']}")
                print(f"   Section: {chunk['source_section']}")
                print(
                    f"   Summary: {chunk['summary_title'][:100]}..."
                    if chunk["summary_title"] and len(chunk["summary_title"]) > 100
                    else f"   Summary: {chunk['summary_title']}"
                )
                print(f"   Raw text: {chunk['raw_text_length']} chars")

                # Parse and count propositions
                if chunk["propositions"]:
                    try:
                        props = json.loads(chunk["propositions"])
                        print(f"   Propositions: {len(props)} items")
                    except json.JSONDecodeError:
                        print("   Propositions: [parse error]")
                else:
                    print("   Propositions: None")

                # Parse and count tables
                if chunk["tables"]:
                    try:
                        tables = json.loads(chunk["tables"])
                        print(f"   Tables: {len(tables)} items")
                    except json.JSONDecodeError:
                        print("   Tables: [parse error]")
                else:
                    print("   Tables: None")

            print("-" * 80)

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"\n❌ Error retrieving chunks: {e}")

    return chunks


async def inspect_chunk_detail(chunk_id: int) -> Optional[Dict[str, Any]]:
    """
    Get detailed view of a single chunk including full text and parsed JSON.

    Args:
        chunk_id: Database ID of the chunk

    Returns:
        Chunk dict with full content
    """
    print("\n" + "=" * 80)
    print(f"CHUNK DETAIL: ID {chunk_id}")
    print("=" * 80)

    try:
        async with get_connection() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT
                        id,
                        chunk_id,
                        page_no,
                        bank,
                        quarter,
                        year,
                        filename,
                        summary_title,
                        source_section,
                        raw_text,
                        propositions,
                        tables
                    FROM rts_embedding
                    WHERE id = :chunk_id
                    """
                ),
                {"chunk_id": chunk_id},
            )

            row = result.fetchone()
            if not row:
                print(f"⚠️  Chunk ID {chunk_id} not found")
                return None

            chunk = {
                "id": row[0],
                "chunk_id": row[1],
                "page_no": row[2],
                "bank": row[3],
                "quarter": row[4],
                "year": row[5],
                "filename": row[6],
                "summary_title": row[7],
                "source_section": row[8],
                "raw_text": row[9],
                "propositions": row[10],
                "tables": row[11],
            }

            print("\n📋 Metadata:")
            print(f"   Bank: {chunk['bank']}")
            print(f"   Period: {chunk['quarter']} {chunk['year']}")
            print(f"   File: {chunk['filename']}")
            print(f"   Page: {chunk['page_no']}")
            print(f"   Section: {chunk['source_section']}")

            print("\n📝 Summary Title:")
            print(f"   {chunk['summary_title']}")

            print(f"\n📄 Raw Text ({len(chunk['raw_text'] or '')} chars):")
            print("-" * 80)
            print(chunk["raw_text"][:1500] if chunk["raw_text"] else "[empty]")
            if chunk["raw_text"] and len(chunk["raw_text"]) > 1500:
                print(f"\n... [truncated, {len(chunk['raw_text']) - 1500} more chars]")
            print("-" * 80)

            # Parse and display propositions
            print("\n💡 Propositions:")
            if chunk["propositions"]:
                try:
                    props = json.loads(chunk["propositions"])
                    print(f"   Count: {len(props)}")
                    for i, prop in enumerate(props[:5], 1):
                        print(f"   {i}. {prop}")
                    if len(props) > 5:
                        print(f"   ... and {len(props) - 5} more")
                except json.JSONDecodeError as e:
                    print(f"   [JSON parse error: {e}]")
            else:
                print("   [none]")

            # Parse and display tables
            print("\n📊 Tables:")
            if chunk["tables"]:
                try:
                    tables = json.loads(chunk["tables"])
                    print(f"   Count: {len(tables)}")
                    for i, table in enumerate(tables[:2], 1):
                        preview = table[:200] if len(table) > 200 else table
                        print(f"   Table {i}: {preview}...")
                except json.JSONDecodeError as e:
                    print(f"   [JSON parse error: {e}]")
            else:
                print("   [none]")

            return chunk

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"\n❌ Error: {e}")
        return None


# =============================================================================
# Embedding Similarity Search
# =============================================================================


# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
async def test_similarity_search(
    query: str,
    bank: str,
    year: int,
    quarter: str,
    context: Dict[str, Any],
    top_k: int = 5,
) -> List[Dict[str, Any]]:
    """
    Test embedding similarity search against RTS chunks.

    Args:
        query: Search query text
        bank: Bank name to filter
        year: Fiscal year to filter
        quarter: Quarter to filter
        context: Execution context with auth
        top_k: Number of results to return

    Returns:
        List of matching chunks with similarity scores
    """
    print("\n" + "=" * 80)
    print("SIMILARITY SEARCH")
    print("=" * 80)
    print(f'\n🔍 Query: "{query}"')
    print(f"📌 Filter: {bank} {quarter} {year}")

    results = []

    try:
        # Generate embedding for query
        print("\n⏳ Generating query embedding...")
        embedding_response = await embed(
            input_text=query,
            context=context,
            embedding_params={"model": "text-embedding-3-large", "dimensions": 3072},
        )

        if not embedding_response.get("data"):
            print("❌ Failed to generate embedding")
            return []

        query_embedding = embedding_response["data"][0]["embedding"]
        print(f"✅ Generated embedding ({len(query_embedding)} dimensions)")

        # Format embedding for PostgreSQL
        embedding_str = "[" + ",".join(str(x) for x in query_embedding) + "]"

        # Perform similarity search
        print("\n⏳ Searching for similar chunks...")

        async with get_connection() as conn:
            # Using cosine distance (1 - cosine_similarity)
            # Lower distance = more similar
            result = await conn.execute(
                text(
                    """
                    SELECT
                        id,
                        chunk_id,
                        page_no,
                        summary_title,
                        source_section,
                        raw_text,
                        propositions,
                        embedding <=> :query_embedding::halfvec AS distance
                    FROM rts_embedding
                    WHERE bank = :bank AND year = :year AND quarter = :quarter
                    ORDER BY embedding <=> :query_embedding::halfvec
                    LIMIT :top_k
                    """
                ),
                {
                    "query_embedding": embedding_str,
                    "bank": bank,
                    "year": year,
                    "quarter": quarter,
                    "top_k": top_k,
                },
            )

            print(f"\n📊 Top {top_k} Results:")
            print("-" * 80)

            for row in result.fetchall():
                similarity = 1 - row[7]  # Convert distance to similarity
                result_dict = {
                    "id": row[0],
                    "chunk_id": row[1],
                    "page_no": row[2],
                    "summary_title": row[3],
                    "source_section": row[4],
                    "raw_text": row[5],
                    "propositions": row[6],
                    "similarity": similarity,
                    "distance": row[7],
                }
                results.append(result_dict)

                print(f"\n🔹 Chunk {result_dict['chunk_id']} (Similarity: {similarity:.4f})")
                print(f"   Section: {result_dict['source_section']}")
                print(
                    f"   Summary: {result_dict['summary_title'][:100]}..."
                    if result_dict["summary_title"] and len(result_dict["summary_title"]) > 100
                    else f"   Summary: {result_dict['summary_title']}"
                )

                # Show snippet of raw text
                if result_dict["raw_text"]:
                    snippet = result_dict["raw_text"][:200].replace("\n", " ")
                    print(f"   Text: {snippet}...")

            print("-" * 80)

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"\n❌ Error in similarity search: {e}")
        traceback.print_exc()

    return results


# =============================================================================
# Source Section Analysis
# =============================================================================


async def analyze_source_sections(
    bank: str,
    year: int,
    quarter: str,
) -> Dict[str, int]:
    """
    Analyze the distribution of source sections for a bank/period.

    Args:
        bank: Bank name
        year: Fiscal year
        quarter: Quarter

    Returns:
        Dict mapping section paths to chunk counts
    """
    print("\n" + "=" * 80)
    print(f"SOURCE SECTION ANALYSIS: {bank} {quarter} {year}")
    print("=" * 80)

    sections = {}

    try:
        async with get_connection() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT source_section, COUNT(*) as chunk_count
                    FROM rts_embedding
                    WHERE bank = :bank AND year = :year AND quarter = :quarter
                    GROUP BY source_section
                    ORDER BY chunk_count DESC
                    """
                ),
                {"bank": bank, "year": year, "quarter": quarter},
            )

            print("\n📂 Sections by chunk count:\n")
            for row in result.fetchall():
                section = row[0] or "[No Section]"
                count = row[1]
                sections[section] = count
                print(f"   {count:>3} chunks | {section}")

            print(f"\n📊 Total unique sections: {len(sections)}")

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"\n❌ Error: {e}")

    return sections


# =============================================================================
# Main Entry Point
# =============================================================================


async def main():
    """Main entry point for RTS table exploration."""

    print("\n" + "=" * 80)
    print("RTS_EMBEDDING TABLE EXPLORER")
    print("Bank Earnings Report ETL - Data Discovery Script")
    print("=" * 80)

    # Setup context
    execution_id = str(uuid.uuid4())
    ssl_config = setup_ssl()
    auth_config = await setup_authentication(execution_id, ssl_config)

    if not auth_config.get("success"):
        print(f"\n❌ Authentication failed: {auth_config.get('error')}")
        return

    context = {
        "execution_id": execution_id,
        "ssl_config": ssl_config,
        "auth_config": auth_config,
    }

    # Step 1: Explore table structure
    exploration = await explore_table_structure()

    if exploration.get("error") or not exploration.get("availability"):
        print("\n⚠️  Cannot proceed - table exploration failed or no data")
        return

    # Step 2: Pick a bank/period to explore (use first available)
    if exploration["availability"]:
        sample = exploration["availability"][0]
        bank = sample["bank"]
        year = sample["year"]
        quarter = sample["quarter"]

        print(f"\n\n🎯 Using sample period: {bank} {quarter} {year}")

        # Step 3: Get sample chunks
        chunks = await get_sample_chunks(bank, year, quarter, limit=5)

        # Step 4: Inspect first chunk in detail
        if chunks:
            await inspect_chunk_detail(chunks[0]["id"])

        # Step 5: Analyze source sections
        await analyze_source_sections(bank, year, quarter)

        # Step 6: Test similarity search with sample queries
        test_queries = [
            "net interest income growth drivers",
            "credit loss provisions and allowances",
            "capital ratios CET1 regulatory requirements",
            "operating expenses and efficiency",
            "loan portfolio quality and performance",
        ]

        print("\n\n" + "=" * 80)
        print("SIMILARITY SEARCH TESTS")
        print("=" * 80)

        for query in test_queries[:2]:  # Test first 2 queries to save time
            await test_similarity_search(
                query=query,
                bank=bank,
                year=year,
                quarter=quarter,
                context=context,
                top_k=3,
            )

    print("\n" + "=" * 80)
    print("EXPLORATION COMPLETE")
    print("=" * 80)
    print("\n💡 Next steps:")
    print("   1. Review the source sections to understand document structure")
    print("   2. Test similarity search with domain-specific queries")
    print("   3. Build retrieval functions for bank earnings report ETL")
    print("   4. Consider filtering by source_section for targeted retrieval")


if __name__ == "__main__":
    asyncio.run(main())
