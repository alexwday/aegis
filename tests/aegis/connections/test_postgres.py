"""
Tests for PostgreSQL async connector functionality.

Tests database operations including connection management, CRUD operations,
and table utilities using SQLAlchemy async.
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
        mock_result = MagicMock()  # Result object is not async
        mock_result.rowcount = 1
        mock_conn.execute.return_value = mock_result
        # No need for commit with engine.begin() context manager

        # Setup async context manager
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        result = await execute_query(
            "UPDATE test_table SET name = :name WHERE id = :id",
            {"name": "test", "id": 1},
            "test-exec-id",
        )

        assert result == 1
        # Commit is automatic with engine.begin(), no need to assert

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_fetch_all_success(self, mock_get_conn):
        """Test fetching all results from a query."""
        mock_conn = AsyncMock()
        mock_result = MagicMock()  # Result is not async
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

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_fetch_one_with_result(self, mock_get_conn):
        """Test fetching a single result."""
        mock_conn = AsyncMock()
        mock_result = MagicMock()  # Result object is not async
        mock_row = MagicMock()
        mock_row._mapping = {"id": 1, "name": "test"}
        mock_result.fetchone.return_value = mock_row
        mock_conn.execute.return_value = mock_result

        # Setup async context manager
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        result = await fetch_one(
            "SELECT * FROM test_table WHERE id = :id",
            {"id": 1},
            "test-exec-id",
        )

        assert result == {"id": 1, "name": "test"}

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_fetch_one_no_result(self, mock_get_conn):
        """Test fetching when no result exists."""
        mock_conn = AsyncMock()
        mock_result = MagicMock()  # Result object is not async
        mock_result.fetchone.return_value = None
        mock_conn.execute.return_value = mock_result

        # Setup async context manager
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        result = await fetch_one(
            "SELECT * FROM test_table WHERE id = :id",
            {"id": 999},
            "test-exec-id",
        )

        assert result is None


class TestCRUDOperations:
    """Tests for CRUD operations."""

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.insert")
    @patch("aegis.connections.postgres_connector.MetaData")
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_insert_record_without_returning(self, mock_get_conn, mock_metadata_class, mock_insert):
        """Test inserting a record without returning value."""
        mock_conn = AsyncMock()
        mock_result = MagicMock()  # Result object is not async
        mock_conn.execute.return_value = mock_result

        # Mock metadata reflection
        mock_metadata = MagicMock()
        mock_table = MagicMock()
        mock_metadata.tables = {"process_monitor_logs": mock_table}
        mock_metadata_class.return_value = mock_metadata

        # Mock insert statement
        mock_stmt = MagicMock()
        mock_insert.return_value.values.return_value = mock_stmt

        # Setup async context manager
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        result = await insert_record(
            "process_monitor_logs",
            {"model_name": "test", "stage_name": "init"},
            execution_id="test-exec-id",
        )

        assert result is None
        mock_conn.execute.assert_called()
    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.insert")
    @patch("aegis.connections.postgres_connector.MetaData")
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_insert_many_success(self, mock_get_conn, mock_metadata_class, mock_insert):
        """Test inserting multiple records."""
        mock_conn = AsyncMock()
        mock_result = MagicMock()  # Result object is not async
        mock_result.rowcount = 2
        mock_conn.execute.return_value = mock_result

        # Mock metadata reflection
        mock_metadata = MagicMock()
        mock_table = MagicMock()
        mock_metadata.tables = {"process_monitor_logs": mock_table}
        mock_metadata_class.return_value = mock_metadata

        # Mock insert statement
        mock_stmt = MagicMock()
        mock_insert.return_value = mock_stmt

        # Setup async context manager
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        data = [
            {"model_name": "test1", "stage_name": "init"},
            {"model_name": "test2", "stage_name": "process"},
        ]

        result = await insert_many(
            "process_monitor_logs",
            data,
            "test-exec-id",
        )

        assert result == 2
    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.update")
    @patch("aegis.connections.postgres_connector.MetaData")
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_update_record_success(self, mock_get_conn, mock_metadata_class, mock_update):
        """Test updating records."""
        mock_conn = AsyncMock()
        mock_result = MagicMock()  # Result object is not async
        mock_result.rowcount = 1
        mock_conn.execute.return_value = mock_result
        mock_conn.run_sync = AsyncMock()

        # Mock metadata reflection
        mock_metadata = MagicMock()
        mock_table = MagicMock()
        mock_column = MagicMock()
        mock_table.c = {"run_uuid": mock_column}
        mock_metadata.tables = {"process_monitor_logs": mock_table}
        mock_metadata_class.return_value = mock_metadata

        # Mock update statement
        mock_stmt = MagicMock()
        mock_stmt.values.return_value = mock_stmt
        mock_stmt.where.return_value = mock_stmt
        mock_update.return_value = mock_stmt

        # Setup async context manager
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        result = await update_record(
            "process_monitor_logs",
            {"status": "Failed"},
            {"run_uuid": "test-uuid"},
            "test-exec-id",
        )

        assert result == 1
    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.delete")
    @patch("aegis.connections.postgres_connector.MetaData")
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_delete_record_success(self, mock_get_conn, mock_metadata_class, mock_delete):
        """Test deleting records."""
        mock_conn = AsyncMock()
        mock_result = MagicMock()  # Result object is not async
        mock_result.rowcount = 1
        mock_conn.execute.return_value = mock_result
        mock_conn.run_sync = AsyncMock()

        # Mock metadata reflection
        mock_metadata = MagicMock()
        mock_table = MagicMock()
        mock_column = MagicMock()
        mock_table.c = {"run_uuid": mock_column}
        mock_metadata.tables = {"process_monitor_logs": mock_table}
        mock_metadata_class.return_value = mock_metadata

        # Mock delete statement
        mock_stmt = MagicMock()
        mock_stmt.where.return_value = mock_stmt
        mock_delete.return_value = mock_stmt

        # Setup async context manager
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        result = await delete_record(
            "process_monitor_logs",
            {"run_uuid": "test-uuid"},
            "test-exec-id",
        )

        assert result == 1

class TestTableOperations:
    """Tests for table operations."""

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.fetch_one")
    async def test_table_exists_true(self, mock_fetch_one):
        """Test checking if a table exists."""
        mock_fetch_one.return_value = {"exists": True}

        result = await table_exists("process_monitor_logs", "test-exec-id")

        assert result is True
        mock_fetch_one.assert_called_once()

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.fetch_one")
    async def test_table_exists_false(self, mock_fetch_one):
        """Test checking if a table doesn't exist."""
        mock_fetch_one.return_value = {"exists": False}

        result = await table_exists("non_existent_table", "test-exec-id")

        assert result is False
        mock_fetch_one.assert_called_once()

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.fetch_all")
    async def test_get_table_schema(self, mock_fetch_all):
        """Test getting table schema information."""
        mock_fetch_all.return_value = [{
            "column_name": "id",
            "data_type": "integer",
            "is_nullable": "NO",
            "column_default": "nextval('id_seq')",
            "character_maximum_length": None,
            "numeric_precision": None,
            "numeric_scale": None
        }]

        result = await get_table_schema("process_monitor_logs", "test-exec-id")

        assert len(result) == 1
        assert result[0]["column_name"] == "id"
        assert result[0]["data_type"] == "integer"
        mock_fetch_all.assert_called_once()


class TestErrorHandling:
    """Tests for error handling scenarios."""

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_execute_query_with_error(self, mock_get_conn):
        """Test query execution with database error."""
        mock_conn = AsyncMock()
        mock_conn.execute.side_effect = SQLAlchemyError("Query failed")

        # Setup async context manager
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        with pytest.raises(SQLAlchemyError, match="Query failed"):
            await execute_query(
                "INVALID SQL",
                {},
                "test-exec-id",
            )

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_fetch_with_connection_error(self, mock_get_conn):
        """Test fetch operations with connection errors."""
        mock_get_conn.side_effect = SQLAlchemyError("Connection pool exhausted")

        with pytest.raises(SQLAlchemyError):
            await fetch_all("SELECT * FROM test", {}, "test-exec-id")

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_insert_many_empty_list(self, mock_get_conn):
        """Test inserting empty list of records."""
        result = await insert_many("test_table", [], "test-exec-id")

        assert result == 0
        mock_get_conn.assert_not_called()


class TestIntegration:
    """Integration tests for combined operations."""

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.delete")
    @patch("aegis.connections.postgres_connector.update")
    @patch("aegis.connections.postgres_connector.insert")
    @patch("aegis.connections.postgres_connector.MetaData")
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_full_crud_cycle(self, mock_get_conn, mock_metadata_class,
                                   mock_insert, mock_update, mock_delete):
        """Test complete CRUD cycle."""
        mock_conn = AsyncMock()

        # Mock metadata reflection
        mock_metadata = MagicMock()
        mock_table = MagicMock()
        mock_table.c = MagicMock()
        mock_table.c.id = "id_column"
        mock_metadata.tables = {"test_table": mock_table}
        mock_metadata_class.return_value = mock_metadata

        # Mock insert with returning
        insert_result = MagicMock()
        insert_result.rowcount = 1
        insert_result.fetchone.return_value = (1,)  # Returning ID

        # Mock select result
        select_result = MagicMock()
        select_row = MagicMock()
        select_row._mapping = {"id": 1, "name": "test", "status": "active"}
        select_result.fetchone.return_value = select_row

        # Mock update result
        update_result = MagicMock()
        update_result.rowcount = 1

        # Mock delete result
        delete_result = MagicMock()
        delete_result.rowcount = 1

        # Setup mock statements
        mock_insert_stmt = MagicMock()
        mock_insert_stmt.values.return_value = mock_insert_stmt
        mock_insert_stmt.returning.return_value = mock_insert_stmt
        mock_insert.return_value = mock_insert_stmt

        mock_update_stmt = MagicMock()
        mock_update_stmt.values.return_value = mock_update_stmt
        mock_update_stmt.where.return_value = mock_update_stmt
        mock_update.return_value = mock_update_stmt

        mock_delete_stmt = MagicMock()
        mock_delete_stmt.where.return_value = mock_delete_stmt
        mock_delete.return_value = mock_delete_stmt

        # Configure execute to return different results
        call_count = 0
        def execute_side_effect(stmt, *args):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return insert_result
            elif call_count == 2:
                return select_result
            elif call_count == 3:
                return update_result
            else:
                return delete_result

        mock_conn.execute.side_effect = execute_side_effect
        mock_conn.run_sync = AsyncMock()

        # Setup async context manager
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        # Insert
        inserted_id = await insert_record(
            "test_table",
            {"name": "test", "status": "active"},
            returning="id",
            execution_id="test-exec-id",
        )
        assert inserted_id == 1

        # Select
        record = await fetch_one(
            "SELECT * FROM test_table WHERE id = :id",
            {"id": 1},
            "test-exec-id",
        )
        assert record["name"] == "test"

        # Update
        updated = await update_record(
            "test_table",
            {"status": "inactive"},
            {"id": 1},
            "test-exec-id",
        )
        assert updated == 1

        # Delete
        deleted = await delete_record(
            "test_table",
            {"id": 1},
            "test-exec-id",
        )
        assert deleted == 1

        # Verify all operations were called
        assert mock_conn.execute.call_count == 4


class TestAsyncEngineManagement:
    """Tests for async engine creation and management."""

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.create_async_engine")
    @patch("aegis.connections.postgres_connector.async_sessionmaker")
    async def test_get_async_engine_creation(self, mock_sessionmaker, mock_create_engine):
        """Test async engine creation on first call."""
        from aegis.connections.postgres_connector import _get_async_engine

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = MagicMock()

        # Clear the global engine to force creation
        import aegis.connections.postgres_connector as pg
        pg._async_engine = None

        result = await _get_async_engine()

        assert result == mock_engine
        mock_create_engine.assert_called_once()
        mock_sessionmaker.assert_called_once()

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.create_async_engine")
    async def test_get_async_engine_creation_error(self, mock_create_engine):
        """Test error handling during async engine creation."""
        from aegis.connections.postgres_connector import _get_async_engine
        from sqlalchemy.exc import SQLAlchemyError

        mock_create_engine.side_effect = SQLAlchemyError("Connection failed")

        # Clear the global engine to force creation
        import aegis.connections.postgres_connector as pg
        pg._async_engine = None

        with pytest.raises(SQLAlchemyError):
            await _get_async_engine()

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector._get_async_engine")
    async def test_get_connection_engine_error(self, mock_get_engine):
        """Test get_connection when engine creation fails."""
        from aegis.connections.postgres_connector import get_connection

        mock_get_engine.side_effect = Exception("Engine creation failed")

        with pytest.raises(Exception, match="Engine creation failed"):
            async with get_connection("test-exec-id"):
                pass


class TestInsertManyAsync:
    """Tests for the insert_many_async function."""

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector._get_async_engine")
    async def test_insert_many_async_success(self, mock_get_engine):
        """Test successful async batch insert."""
        from aegis.connections.postgres_connector import insert_many_async

        # Setup mock engine and connection
        mock_engine = MagicMock()
        mock_conn = AsyncMock()
        mock_result = AsyncMock()
        mock_result.rowcount = 3
        mock_conn.execute.return_value = mock_result

        # Setup async context manager for engine.begin()
        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_conn
        mock_context.__aexit__.return_value = None
        # begin() should return the context manager, not a coroutine
        mock_engine.begin = MagicMock(return_value=mock_context)

        # _get_async_engine is async, return the engine directly
        mock_get_engine.return_value = mock_engine

        data_list = [
            {"name": "Item 1", "value": 100},
            {"name": "Item 2", "value": 200},
            {"name": "Item 3", "value": 300}
        ]

        result = await insert_many_async(
            "test_table",
            data_list,
            "test-exec-id"
        )

        assert result == 3
        mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_insert_many_async_empty_list(self):
        """Test insert_many_async with empty list."""
        from aegis.connections.postgres_connector import insert_many_async

        result = await insert_many_async(
            "test_table",
            [],
            "test-exec-id"
        )

        assert result == 0

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector._get_async_engine")
    async def test_insert_many_async_error(self, mock_get_engine):
        """Test insert_many_async error handling."""
        from aegis.connections.postgres_connector import insert_many_async

        # Setup mock engine and connection
        mock_engine = MagicMock()
        mock_conn = AsyncMock()
        mock_conn.execute.side_effect = Exception("Database error")

        # Setup async context manager for engine.begin()
        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_conn
        mock_context.__aexit__.return_value = None
        # begin() should return the context manager, not a coroutine
        mock_engine.begin = MagicMock(return_value=mock_context)

        # _get_async_engine is async, return the engine directly
        mock_get_engine.return_value = mock_engine

        data_list = [{"name": "Item 1"}]

        with pytest.raises(RuntimeError, match="Failed to insert records into test_table"):
            await insert_many_async(
                "test_table",
                data_list,
                "test-exec-id"
            )


class TestAdditionalErrorPaths:
    """Tests for additional error handling paths."""

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_fetch_all_connection_error_in_context(self, mock_get_conn):
        """Test fetch_all when connection fails within context manager."""
        from aegis.connections.postgres_connector import fetch_all

        # Mock context manager that raises on __aenter__
        mock_context = AsyncMock()
        mock_context.__aenter__.side_effect = Exception("Connection failed in context")
        mock_get_conn.return_value = mock_context

        with pytest.raises(Exception, match="Connection failed in context"):
            await fetch_all(
                "SELECT * FROM test",
                {},
                "test-exec-id"
            )

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_execute_query_rollback_on_error(self, mock_get_conn):
        """Test execute_query rollback behavior on error."""
        from aegis.connections.postgres_connector import execute_query

        mock_conn = AsyncMock()
        mock_conn.execute.side_effect = Exception("SQL error")
        mock_conn.rollback = AsyncMock()

        # Setup async context manager
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        with pytest.raises(Exception, match="SQL error"):
            await execute_query(
                "UPDATE test SET name = :name",
                {"name": "test"},
                "test-exec-id"
            )

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    @patch("aegis.connections.postgres_connector.MetaData")
    @patch("aegis.connections.postgres_connector.insert")
    async def test_insert_record_error_handling(self, mock_insert_func, mock_metadata_class, mock_get_conn):
        """Test insert_record error handling paths."""
        from aegis.connections.postgres_connector import insert_record

        # Setup mock connection
        mock_conn = AsyncMock()

        # Setup mock metadata and table
        mock_metadata = MagicMock()
        mock_table = MagicMock()
        mock_metadata.tables = {"test_table": mock_table}
        mock_metadata_class.return_value = mock_metadata

        # Setup mock insert statement that raises error
        mock_stmt = MagicMock()
        mock_insert_func.return_value.values.return_value = mock_stmt
        mock_conn.execute.side_effect = Exception("Insert failed")

        # Setup async context manager for connection
        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_conn
        mock_context.__aexit__.return_value = None
        mock_get_conn.return_value = mock_context

        with pytest.raises(Exception, match="Insert failed"):
            await insert_record(
                "test_table",
                {"name": "test"},
                execution_id="test-exec-id"
            )

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_table_exists_error(self, mock_get_conn):
        """Test table_exists error handling."""
        from aegis.connections.postgres_connector import table_exists

        mock_conn = AsyncMock()
        mock_conn.execute.side_effect = Exception("Schema query failed")

        # Setup async context manager
        mock_get_conn.return_value.__aenter__.return_value = mock_conn
        mock_get_conn.return_value.__aexit__.return_value = None

        with pytest.raises(Exception, match="Schema query failed"):
            await table_exists("test_table", "test-exec-id")