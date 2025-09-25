"""
Tests for PostgreSQL connector functionality.

Tests database operations including connection management, CRUD operations,
and table utilities using SQLAlchemy.
"""

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, AsyncMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.exc import SQLAlchemyError

from aegis.connections import (
    close_all_connections,
    delete_record,
    execute_query,
    fetch_all,
    fetch_one,
    get_connection,
    get_table_schema,
    insert_many,
    insert_record,
    table_exists,
    update_record,
)


@pytest.fixture
def sample_data():
    """
    Fixture providing sample data for process_monitor_logs table.

    Returns:
        Dictionary with sample data
    """
    return {
        "run_uuid": str(uuid.uuid4()),
        "model_name": "test_model",
        "stage_name": "test_stage",
        "stage_start_time": datetime.now(timezone.utc),
        "status": "Success",
        "environment": "test",
    }


@pytest.fixture
def mock_async_engine():
    """
    Fixture providing a mock SQLAlchemy async engine.

    Returns:
        Mock async engine object
    """
    with patch("aegis.connections.postgres_connector._async_engine") as mock:
        yield mock


class TestConnectionManagement:
    """Tests for database connection management."""

    @pytest.mark.asyncio
    async def test_get_connection_success(self, mock_async_engine):
        """Test successful connection retrieval from pool."""
        mock_conn = AsyncMock()
        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_conn
        mock_context.__aexit__.return_value = None
        mock_async_engine.begin.return_value = mock_context

        with patch("aegis.connections.postgres_connector._get_async_engine") as mock_get:
            mock_get.return_value = mock_async_engine

            async with get_connection("test-exec-id") as conn:
                assert conn == mock_conn
                mock_async_engine.begin.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_connection_error(self, mock_async_engine):
        """Test connection error handling."""
        mock_async_engine.begin.side_effect = SQLAlchemyError("Connection failed")

        with patch("aegis.connections.postgres_connector._get_async_engine") as mock_get:
            mock_get.return_value = mock_async_engine

            with pytest.raises(SQLAlchemyError, match="Connection failed"):
                async with get_connection("test-exec-id"):
                    pass

    @pytest.mark.asyncio
    async def test_close_all_connections(self):
        """Test closing all connections in the pool."""
        with patch("aegis.connections.postgres_connector._async_engine") as mock_engine:
            mock_engine.dispose = AsyncMock()

            await close_all_connections()

            mock_engine.dispose.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_all_connections_no_engine(self):
        """Test closing connections when no engine exists."""
        with patch("aegis.connections.postgres_connector._async_engine", None):
            # Should not raise error
            await close_all_connections()

    @pytest.mark.asyncio
    async def test_get_connection_with_exception_in_context(self, mock_async_engine):
        """Test connection cleanup when exception occurs in context."""
        mock_conn = AsyncMock()
        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_conn
        mock_context.__aexit__.return_value = None
        mock_async_engine.begin.return_value = mock_context

        with patch("aegis.connections.postgres_connector._get_async_engine") as mock_get:
            mock_get.return_value = mock_async_engine

            # Simulate exception within context
            with pytest.raises(ValueError):
                async with get_connection("test-exec-id"):
                    raise ValueError("Test error")

            # Connection should still be cleaned up via __aexit__
            mock_context.__aexit__.assert_called()


class TestQueryExecution:
    """Tests for query execution functions."""

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_execute_query_success(self, mock_get_conn):
        """Test successful query execution."""
        mock_conn = AsyncMock()
        mock_result = AsyncMock()
        mock_result.rowcount = 1
        mock_conn.execute.return_value = mock_result
        mock_conn.commit = AsyncMock()

        # Setup async context manager
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        result = await execute_query(
            "UPDATE test_table SET name = :name WHERE id = :id",
            {"name": "test", "id": 1},
            "test-exec-id",
        )

        assert result == 1
        mock_conn.commit.assert_called_once()

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_fetch_all_success(self, mock_get_conn):
        """Test fetching all results from a query."""
        mock_conn = AsyncMock()
        mock_result = AsyncMock()
        mock_row1 = MagicMock()
        mock_row1._mapping = {"id": 1, "name": "test1"}
        mock_row2 = MagicMock()
        mock_row2._mapping = {"id": 2, "name": "test2"}
        mock_result.fetchall.return_value = [mock_row1, mock_row2]
        mock_conn.execute.return_value = mock_result

        # Setup async context manager
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        results = await fetch_all(
            "SELECT * FROM test_table WHERE status = :status",
            {"status": "active"},
            "test-exec-id",
        )

        assert len(results) == 2
        assert results[0] == {"id": 1, "name": "test1"}
        assert results[1] == {"id": 2, "name": "test2"}

    @patch("aegis.connections.postgres_connector.get_connection")
    def test_fetch_one_with_result(self, mock_get_conn):
        """Test fetching a single result."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row._mapping = {"id": 1, "name": "test"}
        mock_result.fetchone.return_value = mock_row

        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_conn.execute.return_value = mock_result
        mock_get_conn.return_value = mock_conn

        result = fetch_one(
            "SELECT * FROM test_table WHERE id = :id",
            {"id": 1},
            "test-exec-id",
        )

        assert result == {"id": 1, "name": "test"}

    @patch("aegis.connections.postgres_connector.get_connection")
    def test_fetch_one_no_result(self, mock_get_conn):
        """Test fetching when no result exists."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None

        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_conn.execute.return_value = mock_result
        mock_get_conn.return_value = mock_conn

        result = fetch_one(
            "SELECT * FROM test_table WHERE id = :id",
            {"id": 999},
            "test-exec-id",
        )

        assert result is None


class TestCRUDOperations:
    """Tests for CRUD operations."""

    @patch("aegis.connections.postgres_connector.Table")
    @patch("aegis.connections.postgres_connector.insert")
    @patch("aegis.connections.postgres_connector.get_connection")
    def test_insert_record_without_returning(self, mock_get_conn, mock_insert, mock_table):
        """Test inserting a record without returning value."""
        mock_conn = MagicMock()
        mock_stmt = MagicMock()
        mock_insert.return_value.values.return_value = mock_stmt

        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_get_conn.return_value = mock_conn

        result = insert_record(
            "process_monitor_logs",
            {"model_name": "test", "stage_name": "init"},
            execution_id="test-exec-id",
        )

        assert result is None
        mock_conn.execute.assert_called_with(mock_stmt)
        mock_conn.commit.assert_called_once()

    @patch("aegis.connections.postgres_connector.Table")
    @patch("aegis.connections.postgres_connector.insert")
    @patch("aegis.connections.postgres_connector.get_connection")
    def test_insert_record_with_returning(self, mock_get_conn, mock_insert, mock_table):
        """Test inserting a record with returning value."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = [123]
        mock_conn.execute.return_value = mock_result

        mock_stmt = MagicMock()
        mock_stmt.returning.return_value = mock_stmt
        mock_insert.return_value.values.return_value = mock_stmt

        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_get_conn.return_value = mock_conn

        result = insert_record(
            "process_monitor_logs",
            {"model_name": "test", "stage_name": "init"},
            returning="log_id",
            execution_id="test-exec-id",
        )

        assert result == 123
        mock_conn.commit.assert_called_once()

    @patch("aegis.connections.postgres_connector.Table")
    @patch("aegis.connections.postgres_connector.insert")
    @patch("aegis.connections.postgres_connector.get_connection")
    def test_insert_many_success(self, mock_get_conn, mock_insert, mock_table):
        """Test inserting multiple records."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 3
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_get_conn.return_value = mock_conn

        data_list = [
            {"model_name": "test1", "stage_name": "init"},
            {"model_name": "test2", "stage_name": "process"},
            {"model_name": "test3", "stage_name": "complete"},
        ]

        result = insert_many(
            "process_monitor_logs",
            data_list,
            execution_id="test-exec-id",
        )

        assert result == 3
        mock_conn.commit.assert_called_once()

    def test_insert_many_empty_list(self):
        """Test inserting empty list returns 0."""
        result = insert_many(
            "process_monitor_logs",
            [],
            execution_id="test-exec-id",
        )

        assert result == 0

    @patch("aegis.connections.postgres_connector.Table")
    @patch("aegis.connections.postgres_connector.update")
    @patch("aegis.connections.postgres_connector.get_connection")
    def test_update_record_success(self, mock_get_conn, mock_update, mock_table):
        """Test updating records."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 2
        mock_conn.execute.return_value = mock_result

        mock_stmt = MagicMock()
        mock_stmt.where.return_value = mock_stmt
        mock_update.return_value.values.return_value = mock_stmt

        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_get_conn.return_value = mock_conn

        result = update_record(
            "process_monitor_logs",
            {"status": "Failed"},
            {"model_name": "test_model"},
            execution_id="test-exec-id",
        )

        assert result == 2
        mock_conn.commit.assert_called_once()

    @patch("aegis.connections.postgres_connector.Table")
    @patch("aegis.connections.postgres_connector.delete")
    @patch("aegis.connections.postgres_connector.get_connection")
    def test_delete_record_success(self, mock_get_conn, mock_delete, mock_table):
        """Test deleting records."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 5
        mock_conn.execute.return_value = mock_result

        mock_stmt = MagicMock()
        mock_stmt.where.return_value = mock_stmt
        mock_delete.return_value = mock_stmt

        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_get_conn.return_value = mock_conn

        result = delete_record(
            "process_monitor_logs",
            {"environment": "test"},
            execution_id="test-exec-id",
        )

        assert result == 5
        mock_conn.commit.assert_called_once()


class TestTableUtilities:
    """Tests for table utility functions."""

    @patch("aegis.connections.postgres_connector.fetch_one")
    def test_table_exists_true(self, mock_fetch_one):
        """Test checking if table exists - returns True."""
        mock_fetch_one.return_value = {"exists": True}

        result = table_exists("process_monitor_logs", "test-exec-id")

        assert result is True
        mock_fetch_one.assert_called_once()

    @patch("aegis.connections.postgres_connector.fetch_one")
    def test_table_exists_false(self, mock_fetch_one):
        """Test checking if table exists - returns False."""
        mock_fetch_one.return_value = {"exists": False}

        result = table_exists("non_existent_table", "test-exec-id")

        assert result is False

    @patch("aegis.connections.postgres_connector.fetch_all")
    def test_get_table_schema(self, mock_fetch_all):
        """Test retrieving table schema."""
        mock_schema = [
            {
                "column_name": "log_id",
                "data_type": "bigint",
                "is_nullable": "NO",
            },
            {
                "column_name": "model_name",
                "data_type": "character varying",
                "is_nullable": "NO",
            },
        ]
        mock_fetch_all.return_value = mock_schema

        result = get_table_schema("process_monitor_logs", "test-exec-id")

        assert len(result) == 2
        assert result[0]["column_name"] == "log_id"
        assert result[1]["data_type"] == "character varying"


class TestErrorHandling:
    """Tests for error handling in database operations."""

    @patch("aegis.connections.postgres_connector.get_connection")
    def test_execute_query_rollback_on_error(self, mock_get_conn):
        """Test that query execution rolls back on error."""
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_conn.execute.side_effect = SQLAlchemyError("Query failed")
        mock_get_conn.return_value = mock_conn

        with pytest.raises(SQLAlchemyError, match="Query failed"):
            execute_query(
                "INVALID SQL",
                {},
                "test-exec-id",
            )

        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()

    @patch("aegis.connections.postgres_connector.Table")
    @patch("aegis.connections.postgres_connector.insert")
    @patch("aegis.connections.postgres_connector.get_connection")
    def test_insert_record_rollback_on_error(self, mock_get_conn, mock_insert, mock_table):
        """Test that insert rolls back on error."""
        mock_conn = MagicMock()
        mock_stmt = MagicMock()
        mock_insert.return_value.values.return_value = mock_stmt

        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_conn.execute.side_effect = SQLAlchemyError("Insert failed")
        mock_get_conn.return_value = mock_conn

        with pytest.raises(SQLAlchemyError, match="Insert failed"):
            insert_record(
                "process_monitor_logs",
                {"invalid": "data"},
                execution_id="test-exec-id",
            )

        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()
