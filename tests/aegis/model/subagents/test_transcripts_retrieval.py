"""
Tests for transcripts retrieval module.

Provides coverage for database retrieval functions including full section,
category-based, and similarity search retrievals.
"""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock

from aegis.model.subagents.transcripts.retrieval import (
    retrieve_full_section,
    retrieve_by_categories,
    retrieve_by_similarity
)


class TestRetrieveFullSection:
    """Tests for full section transcript retrieval."""

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.retrieval.get_logger")
    @patch("aegis.model.subagents.transcripts.retrieval.get_filter_diagnostics")
    @patch("aegis.model.subagents.transcripts.retrieval.get_connection")
    async def test_retrieve_full_section_success(self, mock_get_conn, mock_diagnostics, mock_get_logger):
        """Test successful full section retrieval."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock diagnostics
        mock_diagnostics.return_value = {"total_records": 1000, "matching_all_filters": 5}

        # Mock database connection and results
        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # Mock query results
        mock_result = [
            (1, "MANAGEMENT DISCUSSION SECTION", 1, None, 1, "CEO opening remarks", "Block summary", [1, 2], ["Revenue", "Growth"]),
            (2, "Q&A", None, 1, 2, "Analyst question about revenue", "Q&A summary", [1], ["Revenue"])
        ]
        mock_conn.execute.return_value = mock_result

        combo = {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3", "bank_symbol": "RBC"}
        context = {"execution_id": "test-123"}

        result = await retrieve_full_section(combo, "ALL", context)

        # Verify results
        assert len(result) == 2
        assert result[0]["section_name"] == "MANAGEMENT DISCUSSION SECTION"
        assert result[0]["content"] == "CEO opening remarks"
        assert result[1]["section_name"] == "Q&A"
        assert result[1]["content"] == "Analyst question about revenue"

        # Verify database query was called
        mock_conn.execute.assert_called_once()

        # Verify diagnostics were logged
        mock_diagnostics.assert_called_once_with(combo, context)

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.retrieval.get_logger")
    @patch("aegis.model.subagents.transcripts.retrieval.get_filter_diagnostics")
    @patch("aegis.model.subagents.transcripts.retrieval.get_connection")
    async def test_retrieve_full_section_md_only(self, mock_get_conn, mock_diagnostics, mock_get_logger):
        """Test retrieval with MD section only."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_diagnostics.return_value = {"total_records": 1000}

        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # Mock result with only MD section
        mock_result = [
            (1, "MANAGEMENT DISCUSSION SECTION", 1, None, 1, "CEO remarks", "Summary", [], [])
        ]
        mock_conn.execute.return_value = mock_result

        combo = {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3", "bank_symbol": "RBC"}
        context = {"execution_id": "test-123"}

        result = await retrieve_full_section(combo, "MD", context)

        assert len(result) == 1
        assert result[0]["section_name"] == "MANAGEMENT DISCUSSION SECTION"

        # Verify query parameters included only MD section
        query_args = mock_conn.execute.call_args[0][1]  # Second positional argument
        assert query_args["sections"] == ["MANAGEMENT DISCUSSION SECTION"]

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.retrieval.get_logger")
    @patch("aegis.model.subagents.transcripts.retrieval.get_filter_diagnostics")
    @patch("aegis.model.subagents.transcripts.retrieval.get_connection")
    async def test_retrieve_full_section_no_results(self, mock_get_conn, mock_diagnostics, mock_get_logger):
        """Test handling when no results are found."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock diagnostics showing no matches
        mock_diagnostics.return_value = {
            "total_records": 1000,
            "matching_all_filters": 0,
            "matching_bank_id": 0,
            "matching_year": 100,
            "matching_quarter": 200,
            "sample_available_banks": ["TD", "BMO"]
        }

        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # Empty result
        mock_conn.execute.return_value = []

        combo = {"bank_id": 999, "fiscal_year": 2024, "quarter": "Q3", "bank_symbol": "UNKNOWN"}
        context = {"execution_id": "test-123"}

        result = await retrieve_full_section(combo, "ALL", context)

        assert result == []

        # Verify warning was logged with diagnostic details
        mock_logger.warning.assert_called_once()
        warning_call = mock_logger.warning.call_args[0][0]
        assert "no_results_found" in warning_call

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.retrieval.get_logger")
    @patch("aegis.model.subagents.transcripts.retrieval.get_filter_diagnostics")
    @patch("aegis.model.subagents.transcripts.retrieval.get_connection")
    async def test_retrieve_full_section_database_error(self, mock_get_conn, mock_diagnostics, mock_get_logger):
        """Test handling database errors."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_diagnostics.return_value = {"total_records": 1000}

        # Mock database error
        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None
        mock_conn.execute.side_effect = Exception("Database connection failed")

        combo = {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3", "bank_symbol": "RBC"}
        context = {"execution_id": "test-123"}

        result = await retrieve_full_section(combo, "ALL", context)

        # Should return empty list on error
        assert result == []

        # Verify error was logged
        mock_logger.error.assert_called_once()
        error_call = mock_logger.error.call_args[0][0]
        assert "full_section_error" in error_call


class TestRetrieveByCategoryes:
    """Tests for category-based transcript retrieval."""

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.retrieval.get_logger")
    @patch("aegis.model.subagents.transcripts.retrieval.get_filter_diagnostics")
    @patch("aegis.model.subagents.transcripts.retrieval.get_connection")
    async def test_retrieve_by_categories_success(self, mock_get_conn, mock_diagnostics, mock_get_logger):
        """Test successful category-based retrieval."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock diagnostics as awaitable
        mock_diagnostics.return_value = {"total_records": 1000, "matching_all_filters": 3}

        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # Mock query results with category matches
        mock_result = [
            (1, "MANAGEMENT DISCUSSION SECTION", 1, None, 1, "Revenue discussion", "Summary", [1, 3], ["Revenue", "Growth"]),
            (2, "Q&A", None, 1, 2, "Question about expenses", "Q&A summary", [2], ["Expenses"])
        ]
        mock_conn.execute.return_value = mock_result

        combo = {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3", "bank_symbol": "RBC"}
        context = {"execution_id": "test-123"}
        category_ids = [1, 2, 3]  # Revenue, Expenses, Growth

        result = await retrieve_by_categories(combo, category_ids, context)

        assert len(result) == 2
        assert result[0]["content"] == "Revenue discussion"
        assert result[1]["content"] == "Question about expenses"

        # Verify query parameters
        query_args = mock_conn.execute.call_args[0][1]  # Second positional argument
        assert query_args["category_ids"] == ["1", "2", "3"]  # Converted to strings

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.retrieval.get_logger")
    @patch("aegis.model.subagents.transcripts.retrieval.get_filter_diagnostics")
    @patch("aegis.model.subagents.transcripts.retrieval.get_connection")
    async def test_retrieve_by_categories_no_matches(self, mock_get_conn, mock_diagnostics, mock_get_logger):
        """Test category retrieval with no matching categories."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock diagnostics showing no category matches
        mock_diagnostics.return_value = {
            "total_records": 1000,
            "matching_all_filters": 0,
            "matching_bank_id": 50,
            "matching_year": 200,
            "sample_available_banks": ["RBC", "TD"]
        }

        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        mock_conn.execute.return_value = []

        combo = {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3", "bank_symbol": "RBC"}
        context = {"execution_id": "test-123"}
        category_ids = [999]  # Non-existent category

        result = await retrieve_by_categories(combo, category_ids, context)

        assert result == []

        # Verify warning was logged
        mock_logger.warning.assert_called_once()
        warning_call = mock_logger.warning.call_args[0][0]
        assert "no_results_category" in warning_call

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.retrieval.get_logger")
    @patch("aegis.model.subagents.transcripts.retrieval.get_filter_diagnostics")
    @patch("aegis.model.subagents.transcripts.retrieval.get_connection")
    async def test_retrieve_by_categories_database_error(self, mock_get_conn, mock_diagnostics, mock_get_logger):
        """Test handling database errors in category retrieval."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_diagnostics.return_value = {"total_records": 1000}

        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None
        mock_conn.execute.side_effect = Exception("SQL syntax error")

        combo = {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3", "bank_symbol": "RBC"}
        context = {"execution_id": "test-123"}

        result = await retrieve_by_categories(combo, [1, 2], context)

        assert result == []

        # Verify error was logged
        mock_logger.error.assert_called_once()
        error_call = mock_logger.error.call_args[0][0]
        assert "category_error" in error_call


class TestRetrieveBySimilarity:
    """Tests for similarity-based transcript retrieval."""

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.retrieval.get_logger")
    @patch("aegis.model.subagents.transcripts.retrieval.get_filter_diagnostics")
    @patch("aegis.model.subagents.transcripts.retrieval.embed")
    @patch("aegis.model.subagents.transcripts.retrieval.get_connection")
    async def test_retrieve_by_similarity_success(self, mock_get_conn, mock_embed, mock_diagnostics, mock_get_logger):
        """Test successful similarity search retrieval."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_diagnostics.return_value = {"total_records": 1000, "matching_all_filters": 5}

        # Mock embedding response
        mock_embed.return_value = {
            "data": [{
                "embedding": [0.1, 0.2, 0.3, 0.4, 0.5]  # Mock embedding vector
            }]
        }

        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # Mock similarity search results (including distance scores)
        mock_result = [
            (1, "MANAGEMENT DISCUSSION SECTION", 1, None, 1, "Revenue grew significantly", "Revenue summary", [1], ["Revenue"], 0.1),
            (2, "Q&A", None, 1, 2, "Growth expectations question", "Growth Q&A", [3], ["Growth"], 0.3)
        ]
        mock_conn.execute.return_value = mock_result

        combo = {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3", "bank_symbol": "RBC"}
        context = {"execution_id": "test-123"}
        search_phrase = "revenue growth trends"

        result = await retrieve_by_similarity(combo, search_phrase, context, top_k=10)

        assert len(result) == 2
        assert result[0]["content"] == "Revenue grew significantly"
        assert result[0]["similarity_score"] == 0.9  # 1.0 - 0.1 distance
        assert result[1]["similarity_score"] == 0.7  # 1.0 - 0.3 distance

        # Verify embedding was created
        mock_embed.assert_called_once_with(
            input_text=search_phrase,
            context=context
        )

        # Verify query parameters
        query_args = mock_conn.execute.call_args[0][1]  # Second positional argument
        assert query_args["top_k"] == 10
        assert "[0.1,0.2,0.3,0.4,0.5]" in query_args["embedding"]

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.retrieval.get_logger")
    @patch("aegis.model.subagents.transcripts.retrieval.get_filter_diagnostics")
    @patch("aegis.model.subagents.transcripts.retrieval.embed")
    async def test_retrieve_by_similarity_embedding_error(self, mock_embed, mock_diagnostics, mock_get_logger):
        """Test handling embedding creation errors."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_diagnostics.return_value = {"total_records": 1000}

        # Mock embedding failure
        mock_embed.return_value = None

        combo = {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3", "bank_symbol": "RBC"}
        context = {"execution_id": "test-123"}

        result = await retrieve_by_similarity(combo, "test query", context)

        assert result == []

        # Verify error was logged
        mock_logger.error.assert_called()
        error_call = mock_logger.error.call_args[0][0]
        assert "Failed to create embedding" in error_call

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.retrieval.get_logger")
    @patch("aegis.model.subagents.transcripts.retrieval.get_filter_diagnostics")
    @patch("aegis.model.subagents.transcripts.retrieval.embed")
    @patch("aegis.model.subagents.transcripts.retrieval.get_connection")
    async def test_retrieve_by_similarity_no_results(self, mock_get_conn, mock_embed, mock_diagnostics, mock_get_logger):
        """Test similarity search with no matching results."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock diagnostics showing no matches
        mock_diagnostics.return_value = {
            "total_records": 1000,
            "matching_all_filters": 0,
            "matching_bank_id": 0,
            "sample_available_banks": ["TD", "BMO"]
        }

        mock_embed.return_value = {
            "data": [{"embedding": [0.1, 0.2, 0.3]}]
        }

        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # No results from similarity search
        mock_conn.execute.return_value = []

        combo = {"bank_id": 999, "fiscal_year": 2024, "quarter": "Q3", "bank_symbol": "UNKNOWN"}
        context = {"execution_id": "test-123"}

        result = await retrieve_by_similarity(combo, "nonexistent topic", context)

        assert result == []

        # Verify warning was logged
        mock_logger.warning.assert_called_once()
        warning_call = mock_logger.warning.call_args[0][0]
        assert "no_results_similarity" in warning_call

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.retrieval.get_logger")
    @patch("aegis.model.subagents.transcripts.retrieval.get_filter_diagnostics")
    @patch("aegis.model.subagents.transcripts.retrieval.embed")
    @patch("aegis.model.subagents.transcripts.retrieval.get_connection")
    async def test_retrieve_by_similarity_database_error(self, mock_get_conn, mock_embed, mock_diagnostics, mock_get_logger):
        """Test handling database errors in similarity search."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_diagnostics.return_value = {"total_records": 1000}

        mock_embed.return_value = {
            "data": [{"embedding": [0.1, 0.2, 0.3]}]
        }

        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None
        mock_conn.execute.side_effect = Exception("Vector search failed")

        combo = {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3", "bank_symbol": "RBC"}
        context = {"execution_id": "test-123"}

        result = await retrieve_by_similarity(combo, "test query", context)

        assert result == []

        # Verify error was logged
        mock_logger.error.assert_called_once()
        error_call = mock_logger.error.call_args[0][0]
        assert "similarity_error" in error_call

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.retrieval.get_logger")
    @patch("aegis.model.subagents.transcripts.retrieval.get_filter_diagnostics")
    @patch("aegis.model.subagents.transcripts.retrieval.embed")
    @patch("aegis.model.subagents.transcripts.retrieval.get_connection")
    async def test_retrieve_by_similarity_custom_top_k(self, mock_get_conn, mock_embed, mock_diagnostics, mock_get_logger):
        """Test similarity search with custom top_k parameter."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_diagnostics.return_value = {"total_records": 1000, "matching_all_filters": 2}

        mock_embed.return_value = {
            "data": [{"embedding": [0.1, 0.2, 0.3]}]
        }

        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # Mock fewer results than default
        mock_result = [
            (1, "MD", 1, None, 1, "Content 1", "Summary", [], [], 0.2),
            (2, "MD", 2, None, 2, "Content 2", "Summary", [], [], 0.4)
        ]
        mock_conn.execute.return_value = mock_result

        combo = {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3", "bank_symbol": "RBC"}
        context = {"execution_id": "test-123"}

        # Test custom top_k
        result = await retrieve_by_similarity(combo, "test query", context, top_k=5)

        assert len(result) == 2

        # Verify custom top_k was used in query
        query_args = mock_conn.execute.call_args[0][1]  # Second positional argument
        assert query_args["top_k"] == 5