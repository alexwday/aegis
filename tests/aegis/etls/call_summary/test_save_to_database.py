"""Tests for _save_to_database in call_summary ETL (D3.2).

Tests database save logic with mocked connection, covering successful
save, DELETE+INSERT flow, and error handling.
"""

import json

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from aegis.etls.call_summary.main import _save_to_database


@pytest.fixture
def save_etl_context():
    """ETL context for database save."""
    return {
        "bank_info": {
            "bank_id": 1,
            "bank_name": "Royal Bank of Canada",
            "bank_symbol": "RY",
        },
        "quarter": "Q3",
        "fiscal_year": 2024,
        "bank_type": "Canadian_Banks",
        "execution_id": "test-exec-123",
    }


@pytest.fixture
def sample_category_results():
    """Category results for save testing."""
    return [
        {
            "index": 1,
            "name": "Revenue",
            "title": "Revenue: Strong growth",
            "report_section": "Results Summary",
            "rejected": False,
            "summary_statements": [{"statement": "Revenue grew **5%**."}],
        },
        {
            "index": 2,
            "name": "ESG",
            "report_section": "Results Summary",
            "rejected": True,
            "rejection_reason": "No ESG content.",
        },
    ]


@pytest.fixture
def valid_categories(sample_category_results):
    """Non-rejected categories."""
    return [r for r in sample_category_results if not r.get("rejected")]


class TestSaveToDatabase:
    """Tests for _save_to_database()."""

    @pytest.mark.asyncio
    async def test_successful_save(
        self, sample_category_results, valid_categories, save_etl_context
    ):
        """Successful save executes DELETE then INSERT."""
        mock_conn = AsyncMock()
        mock_delete_result = MagicMock()
        mock_delete_result.fetchall.return_value = []
        mock_insert_result = MagicMock()
        mock_insert_result.fetchone.return_value = (42,)
        mock_conn.execute = AsyncMock(
            side_effect=[mock_delete_result, mock_insert_result]
        )

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "aegis.etls.call_summary.main.get_connection", return_value=mock_cm
        ):
            await _save_to_database(
                category_results=sample_category_results,
                valid_categories=valid_categories,
                filepath="/tmp/test.docx",
                docx_filename="RY_2024_Q3.docx",
                etl_context=save_etl_context,
            )

        # Should have executed exactly 2 queries: DELETE + INSERT
        assert mock_conn.execute.call_count == 2

        # Verify DELETE was called first
        delete_call = mock_conn.execute.call_args_list[0]
        delete_sql = str(delete_call.args[0])
        assert "DELETE" in delete_sql

        # Verify INSERT was called second
        insert_call = mock_conn.execute.call_args_list[1]
        insert_sql = str(insert_call.args[0])
        assert "INSERT" in insert_sql

    @pytest.mark.asyncio
    async def test_insert_params_correct(
        self, sample_category_results, valid_categories, save_etl_context
    ):
        """INSERT parameters contain correct metadata."""
        mock_conn = AsyncMock()
        mock_delete_result = MagicMock()
        mock_delete_result.fetchall.return_value = []
        mock_insert_result = MagicMock()
        mock_insert_result.fetchone.return_value = (1,)
        mock_conn.execute = AsyncMock(
            side_effect=[mock_delete_result, mock_insert_result]
        )

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "aegis.etls.call_summary.main.get_connection", return_value=mock_cm
        ):
            await _save_to_database(
                category_results=sample_category_results,
                valid_categories=valid_categories,
                filepath="/tmp/test.docx",
                docx_filename="RY_2024_Q3.docx",
                etl_context=save_etl_context,
            )

        insert_call = mock_conn.execute.call_args_list[1]
        params = insert_call.args[1]
        assert params["bank_id"] == 1
        assert params["bank_symbol"] == "RY"
        assert params["fiscal_year"] == 2024
        assert params["quarter"] == "Q3"
        assert params["execution_id"] == "test-exec-123"
        assert params["generated_by"] == "call_summary_etl"

        metadata = json.loads(params["metadata"])
        assert metadata["categories_processed"] == 2
        assert metadata["categories_included"] == 1
        assert metadata["categories_rejected"] == 1
        assert metadata["bank_type"] == "Canadian_Banks"

    @pytest.mark.asyncio
    async def test_delete_replaces_existing_report(
        self, sample_category_results, valid_categories, save_etl_context
    ):
        """DELETE removes existing report for same bank/period/type before INSERT."""
        mock_conn = AsyncMock()
        mock_delete_result = MagicMock()
        mock_delete_result.fetchall.return_value = [(99,)]  # Existing report deleted
        mock_insert_result = MagicMock()
        mock_insert_result.fetchone.return_value = (100,)
        mock_conn.execute = AsyncMock(
            side_effect=[mock_delete_result, mock_insert_result]
        )

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "aegis.etls.call_summary.main.get_connection", return_value=mock_cm
        ):
            await _save_to_database(
                category_results=sample_category_results,
                valid_categories=valid_categories,
                filepath="/tmp/test.docx",
                docx_filename="RY_2024_Q3.docx",
                etl_context=save_etl_context,
            )

        # Both queries should succeed
        assert mock_conn.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_raises_on_database_error(
        self, sample_category_results, valid_categories, save_etl_context
    ):
        """SQLAlchemy errors propagate to caller."""
        from sqlalchemy.exc import OperationalError

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(
            side_effect=OperationalError("connection refused", None, None)
        )

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "aegis.etls.call_summary.main.get_connection", return_value=mock_cm
        ):
            with pytest.raises(OperationalError):
                await _save_to_database(
                    category_results=sample_category_results,
                    valid_categories=valid_categories,
                    filepath="/tmp/test.docx",
                    docx_filename="RY_2024_Q3.docx",
                    etl_context=save_etl_context,
                )
