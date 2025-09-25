"""
Additional tests to achieve 100% coverage for postgres_connector.py.
"""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from sqlalchemy.exc import SQLAlchemyError


class TestPostgresFullCoverage:
    """Tests for missing coverage lines in postgres_connector."""

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector._get_async_engine")
    async def test_get_connection_sqlalchemy_error(self, mock_get_engine):
        """Test get_connection handling SQLAlchemyError (lines 109-114)."""
        from aegis.connections.postgres_connector import get_connection

        # Setup mock engine to raise error
        mock_engine = MagicMock()
        mock_context = AsyncMock()
        mock_context.__aenter__.side_effect = SQLAlchemyError("Connection failed")
        mock_engine.begin.return_value = mock_context
        mock_get_engine.return_value = mock_engine

        with pytest.raises(SQLAlchemyError, match="Connection failed"):
            async with get_connection("test-exec-id") as conn:
                pass

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_execute_query_with_sqlalchemy_error(self, mock_get_conn):
        """Test execute_query with SQLAlchemyError (lines 195-202)."""
        from aegis.connections.postgres_connector import execute_query

        mock_conn = AsyncMock()
        mock_conn.execute.side_effect = SQLAlchemyError("Query execution failed")

        # Setup async context manager
        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_conn
        mock_context.__aexit__.return_value = None
        mock_get_conn.return_value = mock_context

        # execute_query logs error and re-raises SQLAlchemyError
        with pytest.raises(SQLAlchemyError, match="Query execution failed"):
            await execute_query(
                "UPDATE test SET value = 1",
                {},
                "test-exec-id"
            )
        mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_fetch_all_with_sqlalchemy_error(self, mock_get_conn):
        """Test fetch_all with SQLAlchemyError (lines 238-244)."""
        from aegis.connections.postgres_connector import fetch_all

        mock_conn = AsyncMock()
        mock_conn.execute.side_effect = SQLAlchemyError("Fetch failed")

        # Setup async context manager
        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_conn
        mock_context.__aexit__.return_value = None
        mock_get_conn.return_value = mock_context

        # fetch_all logs error and re-raises SQLAlchemyError
        with pytest.raises(SQLAlchemyError, match="Fetch failed"):
            await fetch_all(
                "SELECT * FROM test",
                {},
                "test-exec-id"
            )
        mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    @patch("aegis.connections.postgres_connector.MetaData")
    @patch("aegis.connections.postgres_connector.insert")
    async def test_insert_record_with_sqlalchemy_error(self, mock_insert_func, mock_metadata_class, mock_get_conn):
        """Test insert_record with SQLAlchemyError (lines 297-303)."""
        from aegis.connections.postgres_connector import insert_record

        # Setup mock connection
        mock_conn = AsyncMock()

        # Setup mock metadata and table
        mock_metadata = MagicMock()
        mock_table = MagicMock()
        mock_metadata.tables = {"test_table": mock_table}
        mock_metadata_class.return_value = mock_metadata

        # Setup mock insert statement that raises SQLAlchemy error
        mock_stmt = MagicMock()
        mock_returning = MagicMock()
        mock_stmt.returning.return_value = mock_returning
        mock_insert_func.return_value.values.return_value = mock_stmt
        mock_conn.execute.side_effect = SQLAlchemyError("Insert failed")

        # Setup async context manager
        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_conn
        mock_context.__aexit__.return_value = None
        mock_get_conn.return_value = mock_context

        # insert_record logs error and re-raises SQLAlchemyError
        with pytest.raises(SQLAlchemyError, match="Insert failed"):
            await insert_record(
                "test_table",
                {"name": "test"},
                returning="id",
                execution_id="test-exec-id"
            )
        mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    @patch("aegis.connections.postgres_connector.MetaData")
    @patch("aegis.connections.postgres_connector.update")
    async def test_update_record_with_sqlalchemy_error(self, mock_update_func, mock_metadata_class, mock_get_conn):
        """Test update_record with SQLAlchemyError (lines 346-354)."""
        from aegis.connections.postgres_connector import update_record

        # Setup mock connection
        mock_conn = AsyncMock()

        # Setup mock metadata and table
        mock_metadata = MagicMock()
        mock_table = MagicMock()
        mock_metadata.tables = {"test_table": mock_table}
        mock_metadata_class.return_value = mock_metadata

        # Setup mock update that raises error
        mock_stmt = MagicMock()
        mock_where = MagicMock()
        mock_stmt.where.return_value = mock_where
        mock_update_func.return_value.values.return_value = mock_stmt
        mock_conn.execute.side_effect = SQLAlchemyError("Update failed")

        # Setup async context manager
        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_conn
        mock_context.__aexit__.return_value = None
        mock_get_conn.return_value = mock_context

        # update_record logs error and re-raises SQLAlchemyError
        with pytest.raises(SQLAlchemyError, match="Update failed"):
            await update_record(
                "test_table",
                {"value": 100},
                {"id": 1},
                "test-exec-id"
            )
        mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    @patch("aegis.connections.postgres_connector.MetaData")
    @patch("aegis.connections.postgres_connector.delete")
    async def test_delete_record_with_sqlalchemy_error(self, mock_delete_func, mock_metadata_class, mock_get_conn):
        """Test delete_record with SQLAlchemyError (lines 399-406)."""
        from aegis.connections.postgres_connector import delete_record

        # Setup mock connection
        mock_conn = AsyncMock()

        # Setup mock metadata and table
        mock_metadata = MagicMock()
        mock_table = MagicMock()
        mock_metadata.tables = {"test_table": mock_table}
        mock_metadata_class.return_value = mock_metadata

        # Setup mock delete that raises error
        mock_stmt = MagicMock()
        mock_where = MagicMock()
        mock_stmt.where.return_value = mock_where
        mock_delete_func.return_value = mock_stmt
        mock_conn.execute.side_effect = SQLAlchemyError("Delete failed")

        # Setup async context manager
        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_conn
        mock_context.__aexit__.return_value = None
        mock_get_conn.return_value = mock_context

        # delete_record logs error and re-raises SQLAlchemyError
        with pytest.raises(SQLAlchemyError, match="Delete failed"):
            await delete_record(
                "test_table",
                {"id": 1},
                "test-exec-id"
            )
        mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    async def test_table_exists_with_sqlalchemy_error(self, mock_get_conn):
        """Test table_exists with SQLAlchemyError (lines 449-456)."""
        from aegis.connections.postgres_connector import table_exists

        mock_conn = AsyncMock()
        mock_conn.execute.side_effect = SQLAlchemyError("Schema query failed")

        # Setup async context manager
        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_conn
        mock_context.__aexit__.return_value = None
        mock_get_conn.return_value = mock_context

        # table_exists logs error and re-raises SQLAlchemyError
        with pytest.raises(SQLAlchemyError, match="Schema query failed"):
            await table_exists("test_table", "test-exec-id")
        mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector._get_async_engine")
    async def test_get_connection_error_during_yield(self, mock_get_engine):
        """Test get_connection error handling during yield (lines 109-114)."""
        from aegis.connections.postgres_connector import get_connection

        # Setup mock engine and connection
        mock_engine = MagicMock()
        mock_conn = AsyncMock()

        # Setup async context manager that yields successfully
        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_conn
        mock_context.__aexit__.return_value = None
        mock_engine.begin.return_value = mock_context
        mock_get_engine.return_value = mock_engine

        # Test error during context manager body
        with pytest.raises(SQLAlchemyError, match="Test error during yield"):
            async with get_connection("test-exec-id") as conn:
                # Raise error after connection is obtained
                raise SQLAlchemyError("Test error during yield")

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector._get_async_engine")
    async def test_insert_many_async_with_json_fields(self, mock_get_engine):
        """Test insert_many_async with dict/list fields needing JSON conversion (line 585)."""
        from aegis.connections.postgres_connector import insert_many_async

        # Setup mock engine and connection
        mock_engine = MagicMock()
        mock_conn = AsyncMock()
        mock_result = AsyncMock()
        mock_result.rowcount = 1
        mock_conn.execute.return_value = mock_result

        # Setup async context manager for engine.begin()
        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_conn
        mock_context.__aexit__.return_value = None
        mock_engine.begin = MagicMock(return_value=mock_context)

        mock_get_engine.return_value = mock_engine

        # Test data with dict and list fields that need JSON conversion
        data_list = [
            {
                "name": "Item 1",
                "metadata": {"key": "value"},  # Dict field
                "tags": ["tag1", "tag2"]       # List field
            }
        ]

        result = await insert_many_async(
            "test_table",
            data_list,
            "test-exec-id"
        )

        assert result == 1
        # Verify execute was called with JSON-converted data
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        # The processed records should have JSON strings for dict/list fields
        assert len(call_args[0]) == 2  # query and records

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector._get_async_engine")
    async def test_insert_many_async_with_sqlalchemy_error(self, mock_get_engine):
        """Test insert_many_async with SQLAlchemyError (lines 346-354)."""
        from aegis.connections.postgres_connector import insert_many_async

        # Setup mock engine and connection
        mock_engine = MagicMock()
        mock_conn = AsyncMock()
        # Make execute raise SQLAlchemyError
        mock_conn.execute.side_effect = SQLAlchemyError("Batch insert failed")

        # Setup async context manager for engine.begin()
        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_conn
        mock_context.__aexit__.return_value = None
        mock_engine.begin = MagicMock(return_value=mock_context)

        mock_get_engine.return_value = mock_engine

        # Test data
        data_list = [{"name": "Item 1"}]

        # insert_many_async wraps SQLAlchemyError in RuntimeError
        with pytest.raises(RuntimeError, match="Failed to insert records into test_table"):
            await insert_many_async(
                "test_table",
                data_list,
                "test-exec-id"
            )

    @pytest.mark.asyncio
    @patch("aegis.connections.postgres_connector.get_connection")
    @patch("aegis.connections.postgres_connector.MetaData")
    @patch("aegis.connections.postgres_connector.insert")
    async def test_insert_many_with_sqlalchemy_error(self, mock_insert_func, mock_metadata_class, mock_get_conn):
        """Test insert_many with SQLAlchemyError (lines 346-354)."""
        from aegis.connections.postgres_connector import insert_many

        # Setup mock connection
        mock_conn = AsyncMock()

        # Setup mock metadata and table
        mock_metadata = MagicMock()
        mock_table = MagicMock()
        mock_metadata.tables = {"test_table": mock_table}
        mock_metadata_class.return_value = mock_metadata

        # Setup mock insert that raises error
        mock_stmt = MagicMock()
        mock_insert_func.return_value = mock_stmt
        mock_conn.execute.side_effect = SQLAlchemyError("Batch insert failed")

        # Setup async context manager
        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_conn
        mock_context.__aexit__.return_value = None
        mock_get_conn.return_value = mock_context

        data_list = [{"name": "Item 1"}, {"name": "Item 2"}]

        # insert_many raises the exception after logging
        with pytest.raises(SQLAlchemyError, match="Batch insert failed"):
            await insert_many(
                "test_table",
                data_list,
                "test-exec-id"
            )