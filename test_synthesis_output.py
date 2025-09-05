#!/usr/bin/env python3
"""
Test script to verify that transcripts subagent returns synthesized research statements,
not raw Q&A content, but that the synthesis is detailed and comprehensive.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import uuid
from aegis.model.subagents.transcripts.main import transcripts_agent
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication
from aegis.utils.logging import setup_logging, get_logger

# Setup logging
setup_logging()
logger = get_logger()


def test_synthesized_output():
    """Test that we get synthesized research, not raw Q&A groups."""
    print("\n" + "="*80)
    print("TEST: Synthesized Research Output (Not Raw Content)")
    print("="*80)
    
    # Create context
    execution_id = str(uuid.uuid4())
    ssl_config = setup_ssl()
    auth_config = setup_authentication()
    context = {
        "execution_id": execution_id,
        "ssl_config": ssl_config,
        "auth_config": auth_config
    }
    
    # Test different query types
    test_cases = [
        {
            "query": "What questions did analysts ask about credit quality and loan losses?",
            "intent": "Retrieve analyst questions about credit quality",
            "expected_method": 0  # Should trigger full Q&A section
        },
        {
            "query": "What did management say about the outlook?",
            "intent": "Management commentary on outlook",
            "expected_method": 0  # Should trigger full MD section
        },
        {
            "query": "What were the revenue and margin trends?",
            "intent": "Financial performance metrics",
            "expected_method": 1  # Should trigger category-based
        }
    ]
    
    for test_case in test_cases:
        print(f"\n{'='*60}")
        print(f"Query: {test_case['query']}")
        print(f"Expected retrieval method: {test_case['expected_method']}")
        print("-" * 60)
        
        bank_period_combinations = [{
            "bank_id": 1,
            "bank_name": "Royal Bank of Canada",
            "bank_symbol": "RY",
            "fiscal_year": 2024,
            "quarter": "Q3",
            "query_intent": test_case['intent']
        }]
        
        conversation = [{"role": "user", "content": test_case['query']}]
        
        # Collect output
        full_output = ""
        for chunk in transcripts_agent(
            conversation=conversation,
            latest_message=test_case['query'],
            bank_period_combinations=bank_period_combinations,
            basic_intent=test_case['intent'],
            full_intent=test_case['query'],
            database_id="transcripts",
            context=context
        ):
            full_output += chunk.get("content", "")
        
        # Analyze output characteristics
        print("\n📊 Output Analysis:")
        print(f"- Total length: {len(full_output):,} characters")
        
        # Check for raw Q&A indicators vs synthesis indicators
        raw_indicators = [
            "Q&A Group ",  # Raw Q&A group IDs
            "Question 1 (Q&A Group",  # Raw question numbering
            "## Section",  # Raw section headers
            "speaker_block_id",  # Raw database fields
            "chunk_id"  # Raw chunk IDs
        ]
        
        synthesis_indicators = [
            "asked about",  # Synthesis language
            "management noted",
            "highlighted",
            "emphasized",
            "discussed",
            "responded",
            "summary",
            "key points",
            "main themes"
        ]
        
        raw_count = sum(1 for ind in raw_indicators if ind in full_output)
        synthesis_count = sum(1 for ind in synthesis_indicators if ind.lower() in full_output.lower())
        
        print(f"- Raw content indicators found: {raw_count}")
        print(f"- Synthesis indicators found: {synthesis_count}")
        
        if raw_count > 0:
            print("  ⚠️  WARNING: Output contains raw content indicators")
            for ind in raw_indicators:
                if ind in full_output:
                    print(f"     Found: '{ind}'")
        
        if synthesis_count < 2:
            print("  ⚠️  WARNING: Output lacks synthesis language")
        else:
            print(f"  ✅ Output appears to be synthesized research")
        
        # Check for comprehensive detail
        paragraph_count = full_output.count('\n\n')
        quote_count = full_output.count('"')
        
        print(f"- Paragraphs: ~{paragraph_count}")
        print(f"- Quotes: ~{quote_count // 2}")
        
        if test_case['expected_method'] == 0:  # Full section should be detailed
            if paragraph_count < 3:
                print("  ⚠️  WARNING: Full section synthesis seems too brief")
            else:
                print("  ✅ Synthesis appears appropriately detailed")
        
        # Show preview
        print("\n📝 Output Preview (first 600 chars):")
        print("-" * 40)
        print(full_output[:600])
        print("-" * 40)
        
        # Check structure
        if full_output.startswith("## Earnings Transcript Analysis"):
            print("✅ Has proper header structure")
        
        if "### Royal Bank of Canada" in full_output:
            print("✅ Has bank-specific section")
        
        # Look for synthesis quality
        if "Based on the transcript" in full_output or "According to" in full_output or "The transcript shows" in full_output:
            print("✅ References transcript as source")


if __name__ == "__main__":
    print("\n" + "="*80)
    print("SYNTHESIS OUTPUT TEST")
    print("Verifying transcripts returns synthesized research, not raw content")
    print("="*80)
    
    try:
        test_synthesized_output()
        
        print("\n" + "="*80)
        print("TEST COMPLETE")
        print("="*80)
        print("\n✅ Review the analysis above to verify:")
        print("   1. Output is synthesized research, not raw Q&A groups")
        print("   2. Synthesis is detailed and comprehensive")
        print("   3. Full sections get 3-5 paragraph synthesis")
        print("   4. Includes quotes and specific details from transcript")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        logger.error("Test failed", error=str(e), exc_info=True)