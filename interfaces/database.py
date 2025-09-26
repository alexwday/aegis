"""
Database viewer interface for exploring PostgreSQL tables and data.

This module provides Flask routes and functionality for viewing database schemas,
table data, and executing ad-hoc queries.
"""

import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pandas as pd
from flask import jsonify, request, render_template
from sqlalchemy import create_engine, inspect, text

from src.aegis.utils.settings import config
from src.aegis.utils.logging import get_logger

logger = get_logger()


def decimal_to_float(obj):
    """Convert Decimal objects to float for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


class DatabaseViewer:
    """
    Database viewer for exploring PostgreSQL tables and data.
    
    Attributes:
        engine: SQLAlchemy engine for database connection
        inspector: SQLAlchemy inspector for schema introspection
    """
    
    def __init__(self):
        """Initialize database connection."""
        self.engine = None
        self.inspector = None
        self._connect()
    
    def _connect(self):
        """Establish database connection."""
        try:
            connection_string = (
                f"postgresql://{config.postgres_user}:{config.postgres_password}"
                f"@{config.postgres_host}:{config.postgres_port}/{config.postgres_database}"
            )
            self.engine = create_engine(connection_string)
            self.inspector = inspect(self.engine)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def get_tables(self) -> List[str]:
        """
        Get list of all tables in the database.
        
        Returns:
            List of table names
        """
        try:
            return self.inspector.get_table_names()
        except Exception as e:
            logger.error(f"Failed to get tables: {e}")
            return []
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing table schema information
        """
        try:
            # Get fresh column info each time (don't cache)
            columns_raw = self.inspector.get_columns(table_name)
            # Create new list with converted types (don't modify original)
            columns = []
            for col in columns_raw:
                col_copy = dict(col)  # Create a copy of the column dict
                col_copy["type"] = str(col_copy["type"])  # Convert type to string
                columns.append(col_copy)
            
            pk_constraint = self.inspector.get_pk_constraint(table_name)
            foreign_keys = self.inspector.get_foreign_keys(table_name)
            indexes = self.inspector.get_indexes(table_name)
            
            # Get row count
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = result.scalar()
            
            return {
                "columns": columns,
                "primary_key": pk_constraint,
                "foreign_keys": foreign_keys,
                "indexes": indexes,
                "row_count": int(row_count) if row_count is not None else 0
            }
        except Exception as e:
            logger.error(f"Failed to get table info for {table_name}: {e}")
            return {}
    
    def get_table_data(
        self, 
        table_name: str, 
        limit: int = 100, 
        offset: int = 0,
        order_by: Optional[str] = None,
        filter_condition: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get data from a table with pagination and filtering.
        
        Args:
            table_name: Name of the table
            limit: Number of rows to return
            offset: Number of rows to skip
            order_by: Column to order by
            filter_condition: SQL WHERE clause condition
            
        Returns:
            DataFrame containing table data
        """
        try:
            query = f"SELECT * FROM {table_name}"
            
            if filter_condition:
                query += f" WHERE {filter_condition}"
            
            if order_by:
                query += f" ORDER BY {order_by}"
            
            query += f" LIMIT {limit} OFFSET {offset}"
            
            with self.engine.connect() as conn:
                return pd.read_sql_query(query, conn)
        except Exception as e:
            logger.error(f"Failed to get data from {table_name}: {e}")
            return pd.DataFrame()
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a custom SQL query.
        
        Args:
            query: SQL query to execute
            
        Returns:
            DataFrame containing query results
        """
        try:
            with self.engine.connect() as conn:
                return pd.read_sql_query(query, conn)
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise
    
    def get_sample_queries(self) -> List[Dict[str, str]]:
        """
        Get sample queries for common operations.
        
        Returns:
            List of sample queries with descriptions
        """
        return [
            {
                "name": "Process Monitor Logs (Recent)",
                "query": """
                    SELECT log_id, execution_id, stage_name, status, 
                           duration_ms, model_name, total_tokens, total_cost
                    FROM process_monitor_logs
                    ORDER BY stage_start_time DESC
                    LIMIT 20
                """
            },
            {
                "name": "Data Availability",
                "query": """
                    SELECT bank_id, bank_name, bank_symbol, 
                           fiscal_year, quarter, database_names
                    FROM aegis_data_availability
                    ORDER BY fiscal_year DESC, quarter DESC
                    LIMIT 50
                """
            },
            {
                "name": "Process Monitor Summary by Stage",
                "query": """
                    SELECT stage_name, status, 
                           COUNT(*) as count,
                           AVG(duration_ms) as avg_duration_ms,
                           SUM(total_tokens) as total_tokens,
                           SUM(total_cost) as total_cost
                    FROM process_monitor_logs
                    GROUP BY stage_name, status
                    ORDER BY stage_name, status
                """
            },
            {
                "name": "Failed Processes",
                "query": """
                    SELECT execution_id, stage_name, 
                           stage_start_time, error_message
                    FROM process_monitor_logs
                    WHERE status = 'failed'
                    ORDER BY stage_start_time DESC
                    LIMIT 20
                """
            },
            {
                "name": "Banks with Most Data",
                "query": """
                    SELECT bank_name, bank_symbol,
                           COUNT(*) as quarters_available,
                           MIN(fiscal_year || '-Q' || quarter) as earliest_period,
                           MAX(fiscal_year || '-Q' || quarter) as latest_period
                    FROM aegis_data_availability
                    GROUP BY bank_name, bank_symbol
                    ORDER BY quarters_available DESC
                    LIMIT 20
                """
            }
        ]


def register_database_routes(app):
    """
    Register database viewer routes with the Flask app.
    
    Args:
        app: Flask application instance
    """
    viewer = DatabaseViewer()
    
    @app.route("/database")
    def database():
        """
        Serve the database viewer interface.
        
        Returns:
            HTML template for the database viewer
        """
        return render_template("database.html")
    
    @app.route("/api/database/tables", methods=["GET"])
    def get_tables():
        """
        Get list of all database tables.
        
        Returns:
            JSON response with list of tables
        """
        try:
            tables = viewer.get_tables()
            return jsonify({"tables": tables})
        except Exception as e:
            logger.error(f"Failed to get tables: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route("/api/database/table/<table_name>/schema", methods=["GET"])
    def get_table_schema(table_name):
        """
        Get schema information for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            JSON response with table schema
        """
        try:
            info = viewer.get_table_info(table_name)
            return jsonify(info)
        except Exception as e:
            logger.error(f"Failed to get table schema: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route("/api/database/table/<table_name>/data", methods=["GET"])
    def get_table_data(table_name):
        """
        Get data from a specific table with pagination.
        
        Args:
            table_name: Name of the table
            
        Returns:
            JSON response with table data
        """
        try:
            limit = request.args.get("limit", 100, type=int)
            offset = request.args.get("offset", 0, type=int)
            order_by = request.args.get("order_by", None)
            filter_condition = request.args.get("filter", None)
            
            data = viewer.get_table_data(
                table_name, 
                limit=limit, 
                offset=offset,
                order_by=order_by,
                filter_condition=filter_condition
            )
            
            # Convert DataFrame to dict with proper handling of special types
            result = {
                "data": json.loads(
                    data.to_json(orient="records", default_handler=decimal_to_float)
                ),
                "total_rows": len(data)
            }
            return jsonify(result)
        except Exception as e:
            logger.error(f"Failed to get table data: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route("/api/database/query", methods=["POST"])
    def execute_query():
        """
        Execute a custom SQL query.
        
        Returns:
            JSON response with query results
        """
        try:
            data = request.json
            query = data.get("query", "").strip()
            
            if not query:
                return jsonify({"error": "No query provided"}), 400
            
            # Security check - only allow SELECT queries
            if not query.upper().strip().startswith("SELECT"):
                return jsonify({"error": "Only SELECT queries are allowed"}), 403
            
            result_df = viewer.execute_query(query)
            
            # Convert DataFrame to dict with proper handling of special types
            result = {
                "data": json.loads(
                    result_df.to_json(orient="records", default_handler=decimal_to_float)
                ),
                "columns": list(result_df.columns),
                "total_rows": len(result_df)
            }
            return jsonify(result)
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route("/api/database/samples", methods=["GET"])
    def get_sample_queries():
        """
        Get sample queries for the interface.
        
        Returns:
            JSON response with sample queries
        """
        try:
            samples = viewer.get_sample_queries()
            return jsonify({"samples": samples})
        except Exception as e:
            logger.error(f"Failed to get sample queries: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route("/api/database/overview", methods=["GET"])
    def get_database_overview():
        """
        Get overview statistics for all tables.
        
        Returns:
            JSON response with database overview
        """
        try:
            tables = viewer.get_tables()
            overview = []
            
            for table in tables:
                info = viewer.get_table_info(table)
                overview.append({
                    "table": table,
                    "rows": info.get("row_count", 0),
                    "columns": len(info.get("columns", [])),
                    "indexes": len(info.get("indexes", [])),
                    "has_pk": bool(info.get("primary_key", {}).get("constrained_columns")),
                    "foreign_keys": len(info.get("foreign_keys", []))
                })
            
            # Calculate summary statistics
            total_rows = sum(t["rows"] for t in overview)
            total_tables = len(overview)
            avg_columns = sum(t["columns"] for t in overview) / total_tables if total_tables > 0 else 0
            total_indexes = sum(t["indexes"] for t in overview)
            
            return jsonify({
                "tables": overview,
                "summary": {
                    "total_tables": total_tables,
                    "total_rows": total_rows,
                    "avg_columns": round(avg_columns, 1),
                    "total_indexes": total_indexes
                }
            })
        except Exception as e:
            logger.error(f"Failed to get database overview: {e}")
            return jsonify({"error": str(e)}), 500
