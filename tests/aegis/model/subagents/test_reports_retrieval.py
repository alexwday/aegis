"""
Tests for reports retrieval module.

Provides coverage for database retrieval functions including getting available reports,
unique report types, and retrieving reports by type.
"""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from datetime import datetime, timezone

from aegis.model.subagents.reports.retrieval import (
    get_available_reports,
    get_unique_report_types,
    retrieve_reports_by_type
)


class TestGetAvailableReports:
    """Tests for retrieving available reports for a bank-period combination."""

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.retrieval.get_logger")
    @patch("aegis.model.subagents.reports.retrieval.get_connection")
    async def test_get_available_reports_success(self, mock_get_conn, mock_get_logger):
        """Test successful retrieval of available reports."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock database connection and results
        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # Create mock row objects with named attributes
        mock_row_1 = MagicMock()
        mock_row_1.id = 1
        mock_row_1.report_name = "Q3 2024 Call Summary"
        mock_row_1.report_description = "Comprehensive earnings call analysis"
        mock_row_1.report_type = "call_summary"
        mock_row_1.bank_id = 1
        mock_row_1.bank_name = "Royal Bank of Canada"
        mock_row_1.bank_symbol = "RBC"
        mock_row_1.fiscal_year = 2024
        mock_row_1.quarter = "Q3"
        mock_row_1.local_filepath = "/reports/RBC_Q3_2024.docx"
        mock_row_1.s3_document_name = "RBC_Q3_2024.docx"
        mock_row_1.s3_pdf_name = "RBC_Q3_2024.pdf"
        mock_row_1.markdown_content = "# Q3 Results\n\nRevenue increased 5%"
        mock_row_1.generation_date = datetime(2024, 11, 1, 14, 30, tzinfo=timezone.utc)
        mock_row_1.date_last_modified = datetime(2024, 11, 2, 10, 15, tzinfo=timezone.utc)
        mock_row_1.generated_by = "ETL System v2.1"
        mock_row_1.metadata = {"version": "2.1", "source": "earnings_call"}

        mock_row_2 = MagicMock()
        mock_row_2.id = 2
        mock_row_2.report_name = "Q3 2024 Key Themes"
        mock_row_2.report_description = "Key themes from earnings discussion"
        mock_row_2.report_type = "key_themes"
        mock_row_2.bank_id = 1
        mock_row_2.bank_name = "Royal Bank of Canada"
        mock_row_2.bank_symbol = "RBC"
        mock_row_2.fiscal_year = 2024
        mock_row_2.quarter = "Q3"
        mock_row_2.local_filepath = "/reports/RBC_Q3_2024_themes.docx"
        mock_row_2.s3_document_name = "RBC_Q3_2024_themes.docx"
        mock_row_2.s3_pdf_name = None
        mock_row_2.markdown_content = "# Key Themes\n\n1. Digital transformation"
        mock_row_2.generation_date = datetime(2024, 11, 1, 16, 0, tzinfo=timezone.utc)
        mock_row_2.date_last_modified = datetime(2024, 11, 1, 16, 0, tzinfo=timezone.utc)
        mock_row_2.generated_by = "ETL System v2.1"
        mock_row_2.metadata = {"themes_count": 5}

        mock_conn.execute.return_value = [mock_row_1, mock_row_2]

        combo = {
            "bank_id": 1,
            "bank_name": "Royal Bank of Canada",
            "bank_symbol": "RBC",
            "fiscal_year": 2024,
            "quarter": "Q3"
        }
        context = {"execution_id": "test-123"}

        result = await get_available_reports(combo, context)

        # Verify results
        assert len(result) == 2

        # Verify first report
        assert result[0]["id"] == 1
        assert result[0]["report_name"] == "Q3 2024 Call Summary"
        assert result[0]["report_type"] == "call_summary"
        assert result[0]["bank_symbol"] == "RBC"
        assert result[0]["markdown_content"] == "# Q3 Results\n\nRevenue increased 5%"
        assert result[0]["s3_document_name"] == "RBC_Q3_2024.docx"
        assert result[0]["s3_pdf_name"] == "RBC_Q3_2024.pdf"

        # Verify second report
        assert result[1]["id"] == 2
        assert result[1]["report_name"] == "Q3 2024 Key Themes"
        assert result[1]["s3_pdf_name"] is None

        # Verify database query was called with correct parameters
        query_args = mock_conn.execute.call_args[0][1]  # Second positional argument
        assert query_args["bank_id"] == 1
        assert query_args["fiscal_year"] == 2024
        assert query_args["quarter"] == "Q3"

        # Verify logging
        mock_logger.info.assert_called_once()

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.retrieval.get_logger")
    @patch("aegis.model.subagents.reports.retrieval.get_connection")
    async def test_get_available_reports_no_results(self, mock_get_conn, mock_get_logger):
        """Test retrieval when no reports are found."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # Empty result
        mock_conn.execute.return_value = []

        combo = {
            "bank_id": 999,
            "bank_name": "Unknown Bank",
            "bank_symbol": "UNK",
            "fiscal_year": 2024,
            "quarter": "Q3"
        }
        context = {"execution_id": "test-123"}

        result = await get_available_reports(combo, context)

        assert result == []

        # Verify logging shows 0 reports found
        mock_logger.info.assert_called_once()
        log_call = mock_logger.info.call_args
        assert log_call[1]["reports_found"] == 0

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.retrieval.get_logger")
    @patch("aegis.model.subagents.reports.retrieval.get_connection")
    async def test_get_available_reports_database_error(self, mock_get_conn, mock_get_logger):
        """Test handling database errors."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock database error
        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None
        mock_conn.execute.side_effect = Exception("Database connection failed")

        combo = {
            "bank_id": 1,
            "bank_name": "Royal Bank",
            "bank_symbol": "RBC",
            "fiscal_year": 2024,
            "quarter": "Q3"
        }
        context = {"execution_id": "test-123"}

        result = await get_available_reports(combo, context)

        # Should return empty list on error
        assert result == []

        # Verify error was logged
        mock_logger.error.assert_called_once()
        error_call = mock_logger.error.call_args[0][0]
        assert "subagent.reports.retrieval_error" in error_call


class TestGetUniqueReportTypes:
    """Tests for retrieving unique report types across bank-period combinations."""

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.retrieval.get_logger")
    @patch("aegis.model.subagents.reports.retrieval.get_connection")
    async def test_get_unique_report_types_success(self, mock_get_conn, mock_get_logger):
        """Test successful retrieval of unique report types."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # Mock unique report types
        mock_row_1 = MagicMock()
        mock_row_1.report_name = "Call Summary"
        mock_row_1.report_description = "Comprehensive earnings call analysis"
        mock_row_1.report_type = "call_summary"

        mock_row_2 = MagicMock()
        mock_row_2.report_name = "Key Themes"
        mock_row_2.report_description = "Key themes from earnings discussion"
        mock_row_2.report_type = "key_themes"

        mock_conn.execute.return_value = [mock_row_1, mock_row_2]

        combinations = [
            {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3"},
            {"bank_id": 2, "fiscal_year": 2024, "quarter": "Q3"},
            {"bank_id": 1, "fiscal_year": 2024, "quarter": "Q2"}
        ]
        context = {"execution_id": "test-123"}

        result = await get_unique_report_types(combinations, context)

        # Verify results
        assert len(result) == 2
        assert result[0]["report_name"] == "Call Summary"
        assert result[0]["report_type"] == "call_summary"
        assert result[1]["report_name"] == "Key Themes"
        assert result[1]["report_type"] == "key_themes"

        # Verify query was built with all combinations
        query_args = mock_conn.execute.call_args[0][1]  # Second positional argument
        assert "bank_id_0" in query_args
        assert "bank_id_1" in query_args
        assert "bank_id_2" in query_args
        assert query_args["bank_id_0"] == 1
        assert query_args["bank_id_1"] == 2
        assert query_args["bank_id_2"] == 1

        # Verify logging
        mock_logger.info.assert_called_once()
        log_call = mock_logger.info.call_args[1]
        assert log_call["num_combinations"] == 3
        assert log_call["unique_types"] == 2

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.retrieval.get_logger")
    @patch("aegis.model.subagents.reports.retrieval.get_connection")
    async def test_get_unique_report_types_empty_combinations(self, mock_get_conn, mock_get_logger):
        """Test with empty combinations list."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        mock_conn.execute.return_value = []

        context = {"execution_id": "test-123"}

        result = await get_unique_report_types([], context)

        assert result == []

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.retrieval.get_logger")
    @patch("aegis.model.subagents.reports.retrieval.get_connection")
    async def test_get_unique_report_types_database_error(self, mock_get_conn, mock_get_logger):
        """Test handling database errors."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None
        mock_conn.execute.side_effect = Exception("SQL syntax error")

        combinations = [{"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3"}]
        context = {"execution_id": "test-123"}

        result = await get_unique_report_types(combinations, context)

        assert result == []

        # Verify error was logged
        mock_logger.error.assert_called_once()


class TestRetrieveReportsByType:
    """Tests for retrieving reports by specific type."""

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.retrieval.get_logger")
    @patch("aegis.model.subagents.reports.retrieval.get_connection")
    async def test_retrieve_reports_by_type_success(self, mock_get_conn, mock_get_logger):
        """Test successful retrieval of reports by type."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # Mock report found for first combination
        mock_row_1 = MagicMock()
        mock_row_1.id = 1
        mock_row_1.report_name = "Call Summary"
        mock_row_1.report_description = "Q3 earnings analysis"
        mock_row_1.report_type = "call_summary"
        mock_row_1.bank_id = 1
        mock_row_1.bank_name = "Royal Bank"
        mock_row_1.bank_symbol = "RBC"
        mock_row_1.fiscal_year = 2024
        mock_row_1.quarter = "Q3"
        mock_row_1.local_filepath = "/reports/RBC_Q3.docx"
        mock_row_1.s3_document_name = "RBC_Q3_2024.docx"
        mock_row_1.s3_pdf_name = None
        mock_row_1.markdown_content = "RBC Q3 content"
        mock_row_1.generation_date = datetime(2024, 11, 1, 14, 30)
        mock_row_1.date_last_modified = datetime(2024, 11, 1, 14, 30)
        mock_row_1.metadata = {"version": "1.0"}

        # Configure mock to return different results for different calls
        mock_results = [mock_row_1, None]  # First call returns report, second returns None
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            result = AsyncMock()
            if call_count < len(mock_results) and mock_results[call_count] is not None:
                result.fetchone.return_value = mock_results[call_count]
            else:
                result.fetchone.return_value = None
            call_count += 1
            return result

        mock_conn.execute.side_effect = side_effect

        combinations = [
            {
                "bank_id": 1,
                "bank_name": "Royal Bank",
                "bank_symbol": "RBC",
                "fiscal_year": 2024,
                "quarter": "Q3"
            },
            {
                "bank_id": 2,
                "bank_name": "TD Bank",
                "bank_symbol": "TD",
                "fiscal_year": 2024,
                "quarter": "Q3"
            }
        ]
        context = {"execution_id": "test-123"}
        report_type = "call_summary"

        result = await retrieve_reports_by_type(combinations, report_type, context)

        # Should return only the first report (second had no results)
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["report_name"] == "Call Summary"
        assert result[0]["bank_symbol"] == "RBC"
        assert result[0]["markdown_content"] == "RBC Q3 content"

        # Verify database was queried twice (once per combination)
        assert mock_conn.execute.call_count == 2

        # Verify completion logging
        mock_logger.info.assert_called()
        completion_log = None
        for call in mock_logger.info.call_args_list:
            if "retrieval_complete" in call[0][0]:
                completion_log = call[1]
                break

        assert completion_log is not None
        assert completion_log["requested_combos"] == 2
        assert completion_log["reports_found"] == 1

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.retrieval.get_logger")
    @patch("aegis.model.subagents.reports.retrieval.get_connection")
    async def test_retrieve_reports_by_type_no_results(self, mock_get_conn, mock_get_logger):
        """Test retrieval when no reports match the type."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # Mock no results
        mock_result = AsyncMock()
        mock_result.fetchone.return_value = None
        mock_conn.execute.return_value = mock_result

        combinations = [
            {"bank_id": 1, "bank_symbol": "RBC", "fiscal_year": 2024, "quarter": "Q3"}
        ]
        context = {"execution_id": "test-123"}

        result = await retrieve_reports_by_type(combinations, "nonexistent_type", context)

        assert result == []

        # Verify debug logging for no report
        debug_calls = [call for call in mock_logger.debug.call_args_list
                      if "no_report" in call[0][0]]
        assert len(debug_calls) == 1

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.retrieval.get_logger")
    @patch("aegis.model.subagents.reports.retrieval.get_connection")
    async def test_retrieve_reports_by_type_database_error(self, mock_get_conn, mock_get_logger):
        """Test handling database errors during retrieval."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # Mock database error for first combination, success for second
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            if call_count == 0:
                call_count += 1
                raise Exception("Database connection failed")
            else:
                call_count += 1
                result = AsyncMock()
                result.fetchone.return_value = None
                return result

        mock_conn.execute.side_effect = side_effect

        combinations = [
            {"bank_id": 1, "bank_symbol": "RBC", "fiscal_year": 2024, "quarter": "Q3"},
            {"bank_id": 2, "bank_symbol": "TD", "fiscal_year": 2024, "quarter": "Q3"}
        ]
        context = {"execution_id": "test-123"}

        result = await retrieve_reports_by_type(combinations, "call_summary", context)

        # Should continue processing despite error in first combination
        assert result == []

        # Verify error was logged for first combination
        error_calls = [call for call in mock_logger.error.call_args_list
                      if "retrieve_error" in call[0][0]]
        assert len(error_calls) == 1

        # Verify completion logging still occurs
        completion_calls = [call for call in mock_logger.info.call_args_list
                           if "retrieval_complete" in call[0][0]]
        assert len(completion_calls) == 1

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.retrieval.get_logger")
    @patch("aegis.model.subagents.reports.retrieval.get_connection")
    async def test_retrieve_reports_by_type_single_combination(self, mock_get_conn, mock_get_logger):
        """Test retrieval with single bank-period combination."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_conn = AsyncMock()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # Mock successful report retrieval
        mock_row = MagicMock()
        mock_row.id = 1
        mock_row.report_name = "Key Themes Report"
        mock_row.report_type = "key_themes"
        mock_row.bank_symbol = "BMO"
        mock_row.markdown_content = "Key themes content"
        # Set other required attributes
        for attr in ['report_description', 'bank_id', 'bank_name', 'fiscal_year', 'quarter',
                     'local_filepath', 's3_document_name', 's3_pdf_name', 'generation_date',
                     'date_last_modified', 'metadata']:
            setattr(mock_row, attr, None)

        mock_result = AsyncMock()
        mock_result.fetchone.return_value = mock_row
        mock_conn.execute.return_value = mock_result

        combinations = [
            {"bank_id": 3, "bank_symbol": "BMO", "fiscal_year": 2024, "quarter": "Q2"}
        ]
        context = {"execution_id": "test-123"}

        result = await retrieve_reports_by_type(combinations, "key_themes", context)

        assert len(result) == 1
        assert result[0]["report_name"] == "Key Themes Report"
        assert result[0]["bank_symbol"] == "BMO"

        # Verify single database query
        assert mock_conn.execute.call_count == 1

        # Verify query parameters
        query_args = mock_conn.execute.call_args[0][1]  # Second positional argument
        assert query_args["bank_id"] == 3
        assert query_args["fiscal_year"] == 2024
        assert query_args["quarter"] == "Q2"
        assert query_args["report_type"] == "key_themes"