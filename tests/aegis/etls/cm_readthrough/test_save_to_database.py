"""Tests for save_to_database in CM readthrough ETL."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from aegis.etls.cm_readthrough.main import save_to_database


@pytest.fixture
def sample_results():
    return {
        "metadata": {"banks_processed": 2},
        "outlook": {},
        "section2_questions": {},
        "section3_questions": {},
    }


class TestSaveToDatabase:
    """Tests save_to_database DELETE+INSERT contract and stage-aware errors."""

    @pytest.mark.asyncio
    async def test_successful_save_executes_delete_insert_commit(self, sample_results):
        mock_conn = AsyncMock()
        mock_delete_result = MagicMock()
        mock_delete_result.fetchall.return_value = []
        mock_insert_result = MagicMock()
        mock_insert_result.fetchone.return_value = (42,)
        mock_conn.execute = AsyncMock(side_effect=[mock_delete_result, mock_insert_result])

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        with patch("aegis.etls.cm_readthrough.main.get_connection", return_value=mock_cm):
            await save_to_database(
                results=sample_results,
                fiscal_year=2024,
                quarter="Q3",
                execution_id="exec-123",
                local_filepath="/tmp/cm.docx",
                s3_document_name="cm.docx",
            )

        assert mock_conn.execute.call_count == 2
        assert mock_conn.commit.await_count == 1
        assert "DELETE" in str(mock_conn.execute.call_args_list[0].args[0])
        assert "INSERT" in str(mock_conn.execute.call_args_list[1].args[0])

    @pytest.mark.asyncio
    async def test_logs_stage_when_database_error_occurs(self, sample_results):
        mock_conn = AsyncMock()
        mock_delete_result = MagicMock()
        mock_delete_result.fetchall.return_value = []
        mock_conn.execute = AsyncMock(
            side_effect=[mock_delete_result, OperationalError("insert failed", None, None)]
        )

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("aegis.etls.cm_readthrough.main.get_connection", return_value=mock_cm),
            patch("aegis.etls.cm_readthrough.main.logger.error") as log_error,
        ):
            with pytest.raises(OperationalError):
                await save_to_database(
                    results=sample_results,
                    fiscal_year=2024,
                    quarter="Q3",
                    execution_id="exec-123",
                    local_filepath="/tmp/cm.docx",
                    s3_document_name="cm.docx",
                )

        assert log_error.called
        _, kwargs = log_error.call_args
        assert kwargs["stage"] == "inserting new report"
