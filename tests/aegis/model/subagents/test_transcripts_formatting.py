"""
Tests for transcripts formatting module.

Provides coverage for formatting, reranking, expansion, and research generation functions.
"""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
import json

from aegis.model.subagents.transcripts.formatting import (
    format_full_section_chunks,
    rerank_similarity_chunks,
    expand_speaker_blocks,
    fill_gaps_in_speaker_blocks,
    format_category_or_similarity_chunks,
    generate_research_statement
)


class TestFormatFullSectionChunks:
    """Tests for formatting full section transcript chunks."""

    @pytest.mark.asyncio
    async def test_format_full_section_chunks_empty(self):
        """Test formatting with empty chunks."""
        combo = {"bank_id": 1, "bank_symbol": "RBC", "bank_name": "Royal Bank", "quarter": "Q3", "fiscal_year": 2024}
        context = {"execution_id": "test-123"}

        result = await format_full_section_chunks([], combo, context)

        assert result == "No transcript data available."

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.formatting.get_logger")
    async def test_format_full_section_chunks_valid_data(self, mock_get_logger):
        """Test formatting with valid transcript chunks."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        chunks = [
            {
                "id": 1,
                "section_name": "MANAGEMENT DISCUSSION SECTION",
                "speaker_block_id": 1,
                "chunk_id": 1,
                "content": "Good morning, I'm the CEO.",
                "speaker": "John Smith, CEO",
                "title": "Q3 2024 Earnings Call"
            },
            {
                "id": 2,
                "section_name": "Q&A",
                "qa_group_id": 1,
                "chunk_id": 2,
                "content": "What about loan growth?",
                "speaker": "Analyst",
                "title": "Q3 2024 Earnings Call"
            }
        ]

        combo = {
            "bank_id": 1,
            "bank_symbol": "RBC",
            "bank_name": "Royal Bank",
            "quarter": "Q3",
            "fiscal_year": 2024
        }
        context = {"execution_id": "test-123"}

        result = await format_full_section_chunks(chunks, combo, context)

        # Verify header information
        assert "Institution ID: 1" in result
        assert "Ticker: RBC" in result
        assert "Company: Royal Bank" in result
        assert "Period: Q3 2024" in result
        assert "Title: Q3 2024 Earnings Call" in result

        # Verify section formatting
        assert "## Section 1: MANAGEMENT DISCUSSION SECTION" in result
        assert "## Section 2: Q&A" in result
        assert "**John Smith, CEO**" in result
        assert "Good morning, I'm the CEO." in result

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.formatting.get_logger")
    async def test_format_full_section_chunks_invalid_ids(self, mock_get_logger):
        """Test filtering chunks with missing required IDs."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        chunks = [
            {
                "id": 1,
                "section_name": "Q&A",
                "chunk_id": 1,
                "content": "Question without qa_group_id"
                # Missing qa_group_id
            },
            {
                "id": 2,
                "section_name": "MANAGEMENT DISCUSSION SECTION",
                "chunk_id": 2,
                "content": "Statement without speaker_block_id"
                # Missing speaker_block_id
            }
        ]

        combo = {"bank_id": 1, "bank_symbol": "RBC", "bank_name": "Royal Bank", "quarter": "Q3", "fiscal_year": 2024}
        context = {"execution_id": "test-123"}

        result = await format_full_section_chunks(chunks, combo, context)

        assert result == "No valid transcript data available (all chunks missing required IDs)."

        # Verify warnings were logged
        assert mock_logger.warning.call_count == 2


class TestRerankSimilarityChunks:
    """Tests for reranking similarity search results."""

    @pytest.mark.asyncio
    async def test_rerank_similarity_chunks_empty(self):
        """Test reranking with empty chunks."""
        context = {"execution_id": "test-123"}

        result = await rerank_similarity_chunks([], "test query", context)

        assert result == []

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.formatting.get_logger")
    @patch("aegis.model.subagents.transcripts.formatting.complete")
    @patch("aegis.model.subagents.transcripts.formatting.config")
    async def test_rerank_similarity_chunks_success(self, mock_config, mock_complete, mock_get_logger):
        """Test successful reranking with LLM response."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock config
        mock_model_config = MagicMock()
        mock_model_config.model = "gpt-4"
        mock_config.llm.large = mock_model_config

        # Mock LLM response indicating indices 1 and 3 are irrelevant
        mock_complete.return_value = {
            "choices": [{
                "message": {
                    "content": "[1, 3]"
                }
            }]
        }

        chunks = [
            {"content": "Revenue grew 5%", "block_summary": "Revenue discussion"},
            {"content": "Weather was nice", "block_summary": "Off-topic comment"},
            {"content": "Expenses were controlled", "block_summary": "Expense management"},
            {"content": "The office coffee is good", "block_summary": "Irrelevant comment"}
        ]

        context = {"execution_id": "test-123"}

        result = await rerank_similarity_chunks(chunks, "financial performance", context)

        # Should return chunks 0 and 2 (indices 1 and 3 filtered out)
        assert len(result) == 2
        assert result[0]["content"] == "Revenue grew 5%"
        assert result[1]["content"] == "Expenses were controlled"

        # Verify LLM was called
        mock_complete.assert_called_once()

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.formatting.get_logger")
    @patch("aegis.model.subagents.transcripts.formatting.complete")
    async def test_rerank_similarity_chunks_llm_error(self, mock_complete, mock_get_logger):
        """Test handling LLM errors during reranking."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock LLM error
        mock_complete.side_effect = Exception("LLM API error")

        chunks = [{"content": "test", "block_summary": "test"}]
        context = {"execution_id": "test-123"}

        result = await rerank_similarity_chunks(chunks, "query", context)

        # Should return original chunks on error
        assert result == chunks
        mock_logger.error.assert_called_once()


class TestExpandSpeakerBlocks:
    """Tests for expanding speaker blocks to complete content."""

    @pytest.mark.asyncio
    async def test_expand_speaker_blocks_empty(self):
        """Test expansion with empty chunks."""
        combo = {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3"}
        context = {"execution_id": "test-123"}

        result = await expand_speaker_blocks([], combo, context)

        assert result == []

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.formatting.get_logger")
    @patch("aegis.model.subagents.transcripts.formatting.get_connection")
    async def test_expand_speaker_blocks_success(self, mock_get_conn, mock_get_logger):
        """Test successful speaker block expansion."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock database connection and results
        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # Mock database query results (additional chunks for same speaker block)
        mock_result = [
            (2, "MANAGEMENT DISCUSSION SECTION", 1, None, 2, "Continuing my remarks...", "Block summary", None, None, "Earnings Call"),
            (3, "MANAGEMENT DISCUSSION SECTION", 1, None, 3, "In conclusion...", "Block summary", None, None, "Earnings Call")
        ]
        mock_conn.execute.return_value = mock_result

        # Input chunks
        input_chunks = [
            {"section_name": "MANAGEMENT DISCUSSION SECTION", "speaker_block_id": 1, "content": "Opening remarks"},
            {"section_name": "Q&A", "qa_group_id": 1, "content": "What about growth?"}  # Should be preserved
        ]

        combo = {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3"}
        context = {"execution_id": "test-123"}

        result = await expand_speaker_blocks(input_chunks, combo, context)

        # Should have original Q&A chunk plus expanded MD chunks
        assert len(result) >= 3  # Original Q&A + 2 expanded MD chunks

        # Verify Q&A chunk preserved
        qa_chunks = [c for c in result if c.get("section_name") == "Q&A"]
        assert len(qa_chunks) == 1
        assert qa_chunks[0]["content"] == "What about growth?"

        # Verify database was queried
        mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.formatting.get_logger")
    @patch("aegis.model.subagents.transcripts.formatting.get_connection")
    async def test_expand_speaker_blocks_database_error(self, mock_get_conn, mock_get_logger):
        """Test handling database errors during expansion."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock database error
        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None
        mock_conn.execute.side_effect = Exception("Database connection failed")

        chunks = [{"section_name": "MANAGEMENT DISCUSSION SECTION", "speaker_block_id": 1}]
        combo = {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3"}
        context = {"execution_id": "test-123"}

        result = await expand_speaker_blocks(chunks, combo, context)

        # Should return original chunks on error
        assert result == chunks
        mock_logger.error.assert_called_once()


class TestFillGapsInSpeakerBlocks:
    """Tests for filling gaps in speaker block sequences."""

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.formatting.get_logger")
    async def test_fill_gaps_no_md_chunks(self, mock_get_logger):
        """Test gap filling with no MD chunks."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        chunks = [{"section_name": "Q&A", "qa_group_id": 1}]
        combo = {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3"}
        context = {"execution_id": "test-123"}

        result = await fill_gaps_in_speaker_blocks(chunks, combo, context)

        assert result == chunks

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.formatting.get_logger")
    @patch("aegis.model.subagents.transcripts.formatting.get_connection")
    async def test_fill_gaps_found_and_filled(self, mock_get_conn, mock_get_logger):
        """Test finding and filling gaps in speaker blocks."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock database connection
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        mock_get_conn.return_value.__exit__.return_value = None

        # Mock gap chunk data
        mock_result = [
            (5, "MANAGEMENT DISCUSSION SECTION", 2, None, 1, "Gap content", "Summary", None, None, "Title")
        ]
        mock_conn.execute.return_value = mock_result

        # Input chunks with gap: blocks 1 and 3 (missing block 2)
        chunks = [
            {"section_name": "MANAGEMENT DISCUSSION SECTION", "speaker_block_id": 1},
            {"section_name": "MANAGEMENT DISCUSSION SECTION", "speaker_block_id": 3}
        ]

        combo = {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3"}
        context = {"execution_id": "test-123"}

        result = await fill_gaps_in_speaker_blocks(chunks, combo, context)

        # Should have original 2 chunks plus 1 gap chunk
        assert len(result) == 3

        # Verify gap was logged
        mock_logger.info.assert_called_once()
        assert "gap_filling" in mock_logger.info.call_args[0][0]


class TestFormatCategoryOrSimilarityChunks:
    """Tests for formatting category/similarity chunks with gap notation."""

    @pytest.mark.asyncio
    async def test_format_category_chunks_with_gaps(self):
        """Test formatting with gap notation enabled."""
        chunks = [
            {
                "section_name": "MANAGEMENT DISCUSSION SECTION",
                "speaker_block_id": 1,
                "speaker": "CEO",
                "content": "Opening remarks",
                "title": "Q3 Call"
            },
            {
                "section_name": "MANAGEMENT DISCUSSION SECTION",
                "speaker_block_id": 3,  # Gap: missing block 2
                "speaker": "CFO",
                "content": "Financial results",
                "title": "Q3 Call"
            }
        ]

        combo = {"bank_id": 1, "bank_symbol": "RBC", "bank_name": "Royal Bank", "quarter": "Q3", "fiscal_year": 2024}
        context = {"execution_id": "test-123"}

        result = await format_category_or_similarity_chunks(chunks, combo, context, note_gaps=True)

        # Should include gap notation
        assert "*[Gap: 1 speaker block omitted]*" in result
        assert "**CEO**" in result
        assert "**CFO**" in result

    @pytest.mark.asyncio
    async def test_format_category_chunks_no_gap_notation(self):
        """Test formatting with gap notation disabled."""
        chunks = [
            {
                "section_name": "MANAGEMENT DISCUSSION SECTION",
                "speaker_block_id": 1,
                "content": "Content 1",
                "title": "Call"
            },
            {
                "section_name": "MANAGEMENT DISCUSSION SECTION",
                "speaker_block_id": 3,
                "content": "Content 3",
                "title": "Call"
            }
        ]

        combo = {"bank_id": 1, "bank_symbol": "RBC", "bank_name": "Royal Bank", "quarter": "Q3", "fiscal_year": 2024}
        context = {"execution_id": "test-123"}

        result = await format_category_or_similarity_chunks(chunks, combo, context, note_gaps=False)

        # Should not include gap notation
        assert "*[Gap:" not in result
        assert "Content 1" in result
        assert "Content 3" in result


class TestGenerateResearchStatement:
    """Tests for generating research synthesis statements."""

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.formatting.get_logger")
    @patch("aegis.model.subagents.transcripts.formatting.complete")
    @patch("aegis.model.subagents.transcripts.formatting.config")
    async def test_generate_research_statement_success(self, mock_config, mock_complete, mock_get_logger):
        """Test successful research statement generation."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock config
        mock_model_config = MagicMock()
        mock_model_config.model = "gpt-4"
        mock_config.llm.large = mock_model_config

        # Mock LLM response
        mock_complete.return_value = {
            "choices": [{
                "message": {
                    "content": "Royal Bank reported strong Q3 results with revenue growth of 5%."
                }
            }]
        }

        formatted_content = "Formatted transcript content..."
        combo = {
            "bank_name": "Royal Bank",
            "bank_symbol": "RBC",
            "quarter": "Q3",
            "fiscal_year": 2024,
            "query_intent": "Revenue analysis"
        }
        context = {"execution_id": "test-123"}

        result = await generate_research_statement(
            formatted_content, combo, context, method=0, method_reasoning="Full section needed"
        )

        # Verify header format and content
        assert "### Royal Bank - Q3 2024" in result
        assert "Royal Bank reported strong Q3 results" in result
        assert "---" in result

        # Verify LLM was called
        mock_complete.assert_called_once()

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.formatting.get_logger")
    async def test_generate_research_statement_custom_prompt(self, mock_get_logger):
        """Test research statement with custom ETL prompt."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock the complete function to avoid actual LLM call
        with patch("aegis.model.subagents.transcripts.formatting.complete") as mock_complete:
            mock_complete.return_value = {
                "choices": [{
                    "message": {
                        "content": "Custom analysis result based on ETL prompt."
                    }
                }]
            }

            formatted_content = "Transcript content..."
            combo = {"bank_name": "RBC", "bank_symbol": "RBC", "quarter": "Q3", "fiscal_year": 2024}
            context = {"execution_id": "test-123"}
            custom_prompt = "Extract all revenue metrics and growth rates"

            result = await generate_research_statement(
                formatted_content, combo, context,
                method=0, method_reasoning="ETL", custom_prompt=custom_prompt
            )

            # Verify custom prompt mode was logged
            mock_logger.info.assert_called()
            log_call = mock_logger.info.call_args[0][0]
            assert "using_custom_prompt" in log_call

            # Verify result format
            assert "### RBC - Q3 2024" in result
            assert "Custom analysis result" in result

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.formatting.get_logger")
    @patch("aegis.model.subagents.transcripts.formatting.complete")
    async def test_generate_research_statement_llm_error(self, mock_complete, mock_get_logger):
        """Test handling LLM errors during research generation."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock LLM error
        mock_complete.side_effect = Exception("LLM API failed")

        formatted_content = "Content..."
        combo = {"bank_name": "Royal Bank", "bank_symbol": "RBC", "quarter": "Q3", "fiscal_year": 2024}
        context = {"execution_id": "test-123"}

        result = await generate_research_statement(formatted_content, combo, context)

        # Should return fallback response
        assert "### Royal Bank - Q3 2024" in result
        assert "Transcript data retrieved" in result
        assert "---" in result

        # Verify error was logged
        mock_logger.error.assert_called_once()

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.formatting.get_logger")
    @patch("aegis.model.subagents.transcripts.formatting.complete")
    @patch("aegis.model.subagents.transcripts.formatting.config")
    async def test_generate_research_statement_different_methods(self, mock_config, mock_complete, mock_get_logger):
        """Test different response styles based on retrieval methods."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock config
        mock_model_config = MagicMock()
        mock_model_config.model = "gpt-4"
        mock_config.llm.large = mock_model_config

        mock_complete.return_value = {
            "choices": [{"message": {"content": "Test response"}}]
        }

        combo = {"bank_name": "RBC", "bank_symbol": "RBC", "quarter": "Q3", "fiscal_year": 2024}
        context = {"execution_id": "test-123"}

        # Test Method 0 (Full section)
        await generate_research_statement("content", combo, context, method=0)
        prompt_call = mock_complete.call_args[1]["messages"][0]["content"]
        assert "DETAILED and COMPREHENSIVE synthesis" in prompt_call

        # Test Method 1 (Category)
        mock_complete.reset_mock()
        await generate_research_statement("content", combo, context, method=1)
        prompt_call = mock_complete.call_args[1]["messages"][0]["content"]
        assert "focused synthesis" in prompt_call

        # Test Method 2 (Similarity)
        mock_complete.reset_mock()
        await generate_research_statement("content", combo, context, method=2)
        prompt_call = mock_complete.call_args[1]["messages"][0]["content"]
        assert "targeted synthesis" in prompt_call