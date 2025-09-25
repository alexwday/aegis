"""
Basic tests for transcripts utils module.

Provides coverage for utility functions used by transcripts subagent.
"""

import pytest
from unittest.mock import patch, mock_open, AsyncMock, MagicMock
import yaml

from aegis.model.subagents.transcripts.utils import load_financial_categories, get_filter_diagnostics


class TestLoadFinancialCategories:
    """Tests for loading financial categories from YAML."""

    @pytest.mark.asyncio
    @patch("builtins.open", new_callable=mock_open)
    @patch("aegis.model.subagents.transcripts.utils.yaml.safe_load")
    async def test_load_financial_categories_success(self, mock_yaml_load, mock_file):
        """Test successful loading of financial categories."""
        mock_yaml_data = [
            {"id": 1, "name": "Capital Markets", "description": "Investment banking revenue"},
            {"id": 2, "name": "Trading", "description": "Trading and markets revenue"},
            {"id": 3, "name": "Credit Risk", "description": "Credit risk management"}
        ]
        mock_yaml_load.return_value = mock_yaml_data

        result = await load_financial_categories()

        assert len(result) == 3
        assert result[1]["name"] == "Capital Markets"
        assert result[1]["description"] == "Investment banking revenue"
        assert result[2]["name"] == "Trading"
        assert result[3]["name"] == "Credit Risk"

        # Verify file was opened
        mock_file.assert_called_once()

    @pytest.mark.asyncio
    @patch("builtins.open", side_effect=FileNotFoundError("YAML file not found"))
    @patch("aegis.model.subagents.transcripts.utils.get_logger")
    async def test_load_financial_categories_file_not_found(self, mock_get_logger, mock_open):
        """Test fallback when YAML file is missing."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        result = await load_financial_categories()

        # Should return fallback categories
        assert len(result) >= 3  # At least a few fallback categories
        assert 0 in result
        assert result[0]["name"] == "Non-Relevant"
        assert 1 in result
        assert "Capital Markets" in result[1]["name"]

        # Verify get_logger was called and logger.warning was called
        mock_get_logger.assert_called_once()
        mock_logger.warning.assert_called_once()
        assert "Failed to load financial categories" in mock_logger.warning.call_args[0][0]

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.utils.yaml.safe_load")
    @patch("builtins.open", new_callable=mock_open)
    @patch("aegis.model.subagents.transcripts.utils.get_logger")
    async def test_load_financial_categories_yaml_parse_error(self, mock_get_logger, mock_file, mock_yaml_load):
        """Test fallback when YAML parsing fails."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_yaml_load.side_effect = yaml.YAMLError("Invalid YAML syntax")

        result = await load_financial_categories()

        # Should return fallback categories
        assert len(result) >= 3
        assert 0 in result
        assert result[0]["name"] == "Non-Relevant"

        # Verify get_logger was called and warning was logged
        mock_get_logger.assert_called_once()
        mock_logger.warning.assert_called_once()


class TestGetFilterDiagnostics:
    """Tests for database filter diagnostics."""

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.utils.get_connection")
    async def test_get_filter_diagnostics_success(self, mock_get_conn):
        """Test successful filter diagnostics retrieval."""
        # Mock database connection and results
        mock_conn = AsyncMock()
        mock_result = AsyncMock()

        # Mock the context manager
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # Mock query results - need to mock the actual 8 queries made by the function
        mock_results = []
        for value in [1000, 50, 30, 25, 20, 15, 12, 8]:
            mock_result = MagicMock()
            mock_result.scalar.return_value = value
            mock_results.append(mock_result)
        mock_conn.execute.side_effect = mock_results

        combo = {
            "bank_id": 1,
            "bank_name": "RBC",
            "fiscal_year": 2024,
            "quarter": "Q3"
        }
        context = {"execution_id": "test-123"}

        result = await get_filter_diagnostics(combo, context)

        # Verify the result structure (using actual field names from implementation)
        assert "total_records" in result
        assert "matching_bank_id" in result
        assert "matching_year" in result
        assert "matching_quarter" in result
        assert "matching_bank_and_year" in result
        assert "matching_bank_and_quarter" in result
        assert "matching_year_and_quarter" in result
        assert "matching_all_filters" in result

        # Verify connection was called
        mock_get_conn.assert_called_once()

        # Verify all diagnostic queries were executed (8 queries total)
        assert mock_conn.execute.call_count == 8

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.utils.get_connection")
    async def test_get_filter_diagnostics_database_error(self, mock_get_conn):
        """Test error handling in filter diagnostics."""
        # Mock database connection error
        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        mock_conn.execute.side_effect = Exception("Database connection failed")

        combo = {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3"}
        context = {"execution_id": "test-error"}

        result = await get_filter_diagnostics(combo, context)

        # Function catches exceptions and returns error in dict
        assert "error" in result
        assert "Database connection failed" in result["error"]

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.transcripts.utils.get_connection")
    async def test_get_filter_diagnostics_partial_results(self, mock_get_conn):
        """Test diagnostics with some zero results."""
        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # Mock some zero results to test edge cases
        mock_results = []
        for value in [1000, 0, 100, 25, 0, 0, 50, 0]:  # 8 queries with some zeros
            mock_result = MagicMock()
            mock_result.scalar.return_value = value
            mock_results.append(mock_result)
        mock_conn.execute.side_effect = mock_results

        combo = {"bank_id": 999, "fiscal_year": 2024, "quarter": "Q3"}  # Non-existent bank
        context = {"execution_id": "test-zero"}

        result = await get_filter_diagnostics(combo, context)

        # Should handle zero results gracefully (using actual field names)
        assert result["matching_bank_id"] == 0
        assert result["matching_all_filters"] == 0
        assert result["total_records"] == 1000  # Table should still have records