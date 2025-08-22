"""
PostgreSQL database connector using SQLAlchemy.

This module provides a functional interface for PostgreSQL operations
using SQLAlchemy for connection management and query execution.
"""

from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

from sqlalchemy import (
    MetaData,
    Table,
    create_engine,
    delete,
    insert,
    text,
    update,
)
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool

from ..utils.logging import get_logger
from ..utils.settings import config

logger = get_logger()

_engine: Optional[Engine] = None


def _get_engine() -> Engine:
    """
    Get or create the SQLAlchemy engine with connection pooling.

    Returns:
        SQLAlchemy Engine instance

    Raises:
        SQLAlchemyError: If unable to create engine
    """
    global _engine  # pylint: disable=global-statement
    # Engine must be global singleton for connection pooling across the application.

    if _engine is None:
        try:
            database_url = (
                f"postgresql://{config.postgres_user}:{config.postgres_password}"
                f"@{config.postgres_host}:{config.postgres_port}/{config.postgres_database}"
            )

            _engine = create_engine(
                database_url,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=3600,
                echo=False,
            )

            logger.info(
                "PostgreSQL engine created",
                host=config.postgres_host,
                port=config.postgres_port,
                database=config.postgres_database,
            )
        except SQLAlchemyError as e:
            logger.error(
                "Failed to create PostgreSQL engine",
                error=str(e),
                host=config.postgres_host,
                port=config.postgres_port,
                database=config.postgres_database,
            )
            raise

    return _engine


@contextmanager
def get_connection(execution_id: Optional[str] = None) -> Generator[Connection, None, None]:
    """
    Get a database connection from the pool.

    Args:
        execution_id: Optional execution ID for logging

    Yields:
        SQLAlchemy connection object

    Raises:
        SQLAlchemyError: If unable to get connection
    """
    engine = _get_engine()
    conn: Optional[Connection] = None

    try:
        conn = engine.connect()
        logger.debug("Got connection from pool", execution_id=execution_id)
        yield conn
    except SQLAlchemyError as e:
        logger.error(
            "Error with database connection",
            execution_id=execution_id,
            error=str(e),
        )
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("Returned connection to pool", execution_id=execution_id)


def execute_query(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    execution_id: Optional[str] = None,
) -> Optional[int]:
    """
    Execute a query that doesn't return results (INSERT, UPDATE, DELETE).

    Args:
        query: SQL query to execute
        params: Query parameters as dictionary
        execution_id: Optional execution ID for logging

    Returns:
        Number of affected rows

    Raises:
        SQLAlchemyError: If query execution fails
    """
    with get_connection(execution_id) as conn:
        try:
            result = conn.execute(text(query), params or {})
            conn.commit()  # pylint: disable=no-member
            # SQLAlchemy connection proxy has commit() but pylint can't detect it.

            logger.debug(
                "Query executed",
                execution_id=execution_id,
                affected_rows=result.rowcount,
            )

            return result.rowcount
        except SQLAlchemyError as e:
            conn.rollback()  # pylint: disable=no-member
            # SQLAlchemy connection proxy has rollback() but pylint can't detect it.
            logger.error(
                "Query execution failed",
                execution_id=execution_id,
                error=str(e),
                query=query[:500],
            )
            raise


def fetch_all(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    execution_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Execute a SELECT query and return all results.

    Args:
        query: SQL SELECT query
        params: Query parameters as dictionary
        execution_id: Optional execution ID for logging

    Returns:
        List of dictionaries representing rows

    Raises:
        SQLAlchemyError: If query execution fails
    """
    with get_connection(execution_id) as conn:
        try:
            result = conn.execute(text(query), params or {})
            rows = result.fetchall()

            # SQLAlchemy's Row._mapping is the official way to convert to dict
            # It's a public API despite the underscore prefix
            results = [dict(row._mapping) for row in rows]  # pylint: disable=protected-access

            logger.debug(
                "Fetched all results",
                execution_id=execution_id,
                row_count=len(results),
            )

            return results
        except SQLAlchemyError as e:
            logger.error(
                "Failed to fetch results",
                execution_id=execution_id,
                error=str(e),
                query=query[:500],
            )
            raise


def fetch_one(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    execution_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Execute a SELECT query and return the first result.

    Args:
        query: SQL SELECT query
        params: Query parameters as dictionary
        execution_id: Optional execution ID for logging

    Returns:
        Dictionary representing the first row, or None if no results

    Raises:
        SQLAlchemyError: If query execution fails
    """
    with get_connection(execution_id) as conn:
        try:
            result = conn.execute(text(query), params or {})
            row = result.fetchone()

            if row:
                logger.debug("Fetched one result", execution_id=execution_id)
                # SQLAlchemy's Row._mapping is the official way to convert to dict
                # It's a public API despite the underscore prefix
                return dict(row._mapping)  # pylint: disable=protected-access

            logger.debug("No results found", execution_id=execution_id)
            return None
        except SQLAlchemyError as e:
            logger.error(
                "Failed to fetch result",
                execution_id=execution_id,
                error=str(e),
                query=query[:500],
            )
            raise


def insert_record(
    table: str,
    data: Dict[str, Any],
    returning: Optional[str] = None,
    execution_id: Optional[str] = None,
) -> Optional[Any]:
    """
    Insert a record into a table.

    Args:
        table: Table name
        data: Dictionary of column names and values
        returning: Optional column to return (e.g., 'log_id')
        execution_id: Optional execution ID for logging

    Returns:
        Value of the returning column if specified, otherwise None

    Raises:
        SQLAlchemyError: If insertion fails
    """
    with get_connection(execution_id) as conn:
        try:
            metadata = MetaData()
            table_obj = Table(table, metadata, autoload_with=conn)

            stmt = insert(table_obj).values(**data)

            if returning:
                stmt = stmt.returning(table_obj.c[returning])
                result = conn.execute(stmt)
                conn.commit()  # pylint: disable=no-member
                # SQLAlchemy connection proxy has commit() but pylint can't detect it.
                row = result.fetchone()

                returning_value = row[0] if row else None
                logger.info(
                    "Record inserted with returning value",
                    execution_id=execution_id,
                    table=table,
                    returning_column=returning,
                    returning_value=returning_value,
                )
                return returning_value

            conn.execute(stmt)
            conn.commit()  # pylint: disable=no-member
            # SQLAlchemy connection proxy has commit() but pylint can't detect it.
            logger.info("Record inserted", execution_id=execution_id, table=table)
            return None
        except SQLAlchemyError as e:
            conn.rollback()  # pylint: disable=no-member
            # SQLAlchemy connection proxy has rollback() but pylint can't detect it.
            logger.error(
                "Failed to insert record",
                execution_id=execution_id,
                table=table,
                error=str(e),
            )
            raise


def insert_many(
    table: str,
    data_list: List[Dict[str, Any]],
    execution_id: Optional[str] = None,
) -> int:
    """
    Insert multiple records into a table.

    Args:
        table: Table name
        data_list: List of dictionaries with column names and values
        execution_id: Optional execution ID for logging

    Returns:
        Number of inserted records

    Raises:
        SQLAlchemyError: If insertion fails
    """
    if not data_list:
        logger.warning("No data to insert", execution_id=execution_id, table=table)
        return 0

    with get_connection(execution_id) as conn:
        try:
            metadata = MetaData()
            table_obj = Table(table, metadata, autoload_with=conn)

            stmt = insert(table_obj)
            result = conn.execute(stmt, data_list)
            conn.commit()  # pylint: disable=no-member
            # SQLAlchemy connection proxy has commit() but pylint can't detect it.

            logger.info(
                "Multiple records inserted",
                execution_id=execution_id,
                table=table,
                record_count=result.rowcount,
            )

            return result.rowcount
        except SQLAlchemyError as e:
            conn.rollback()  # pylint: disable=no-member
            # SQLAlchemy connection proxy has rollback() but pylint can't detect it.
            logger.error(
                "Failed to insert multiple records",
                execution_id=execution_id,
                table=table,
                error=str(e),
                record_count=len(data_list),
            )
            raise


def update_record(
    table: str,
    data: Dict[str, Any],
    where: Dict[str, Any],
    execution_id: Optional[str] = None,
) -> int:
    """
    Update records in a table.

    Args:
        table: Table name
        data: Dictionary of columns to update
        where: Dictionary of WHERE conditions
        execution_id: Optional execution ID for logging

    Returns:
        Number of updated records

    Raises:
        SQLAlchemyError: If update fails
    """
    with get_connection(execution_id) as conn:
        try:
            metadata = MetaData()
            table_obj = Table(table, metadata, autoload_with=conn)

            stmt = update(table_obj).values(**data)

            for col, val in where.items():
                stmt = stmt.where(table_obj.c[col] == val)

            result = conn.execute(stmt)
            conn.commit()  # pylint: disable=no-member
            # SQLAlchemy connection proxy has commit() but pylint can't detect it.

            logger.info(
                "Records updated",
                execution_id=execution_id,
                table=table,
                affected_rows=result.rowcount,
            )

            return result.rowcount
        except SQLAlchemyError as e:
            conn.rollback()  # pylint: disable=no-member
            # SQLAlchemy connection proxy has rollback() but pylint can't detect it.
            logger.error(
                "Failed to update records",
                execution_id=execution_id,
                table=table,
                error=str(e),
            )
            raise


def delete_record(
    table: str,
    where: Dict[str, Any],
    execution_id: Optional[str] = None,
) -> int:
    """
    Delete records from a table.

    Args:
        table: Table name
        where: Dictionary of WHERE conditions
        execution_id: Optional execution ID for logging

    Returns:
        Number of deleted records

    Raises:
        SQLAlchemyError: If deletion fails
    """
    with get_connection(execution_id) as conn:
        try:
            metadata = MetaData()
            table_obj = Table(table, metadata, autoload_with=conn)

            stmt = delete(table_obj)

            for col, val in where.items():
                stmt = stmt.where(table_obj.c[col] == val)

            result = conn.execute(stmt)
            conn.commit()  # pylint: disable=no-member
            # SQLAlchemy connection proxy has commit() but pylint can't detect it.

            logger.info(
                "Records deleted",
                execution_id=execution_id,
                table=table,
                affected_rows=result.rowcount,
            )

            return result.rowcount
        except SQLAlchemyError as e:
            conn.rollback()  # pylint: disable=no-member
            # SQLAlchemy connection proxy has rollback() but pylint can't detect it.
            logger.error(
                "Failed to delete records",
                execution_id=execution_id,
                table=table,
                error=str(e),
            )
            raise


def table_exists(table: str, execution_id: Optional[str] = None) -> bool:
    """
    Check if a table exists in the database.

    Args:
        table: Table name to check
        execution_id: Optional execution ID for logging

    Returns:
        True if table exists, False otherwise

    Raises:
        SQLAlchemyError: If query fails
    """
    query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = :table_name
        )
    """

    result = fetch_one(query, {"table_name": table}, execution_id)
    exists = result["exists"] if result else False

    logger.debug(
        "Table existence check",
        execution_id=execution_id,
        table=table,
        exists=exists,
    )

    return exists


def get_table_schema(table: str, execution_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Get the schema information for a table.

    Args:
        table: Table name
        execution_id: Optional execution ID for logging

    Returns:
        List of column information dictionaries

    Raises:
        SQLAlchemyError: If query fails
    """
    query = """
        SELECT
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name = :table_name
        ORDER BY ordinal_position
    """

    schema = fetch_all(query, {"table_name": table}, execution_id)

    logger.debug(
        "Retrieved table schema",
        execution_id=execution_id,
        table=table,
        column_count=len(schema),
    )

    return schema


def close_all_connections():
    """
    Dispose of the connection pool and close all connections.

    This should be called when shutting down the application.
    """
    global _engine  # pylint: disable=global-statement
    # Need to modify global engine instance to properly dispose of connection pool.

    if _engine:
        _engine.dispose()
        _engine = None
        logger.info("All PostgreSQL connections closed")
