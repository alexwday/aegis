"""
Monitoring interface for viewing process monitor logs.

This module provides Flask routes and functionality for viewing
and analyzing process monitoring data from PostgreSQL.
"""

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from flask import jsonify, request

from src.aegis.connections.postgres_connector import fetch_all
from src.aegis.utils.logging import get_logger

logger = get_logger()


def decimal_to_float(obj):
    """Convert Decimal objects to float for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def format_duration(duration_ms: Optional[int]) -> str:
    """
    Format duration in milliseconds to human-readable format.

    Args:
        duration_ms: Duration in milliseconds

    Returns:
        Formatted duration string
    """
    if duration_ms is None:
        return "N/A"

    if duration_ms < 1000:
        return f"{duration_ms}ms"
    elif duration_ms < 60000:
        seconds = duration_ms / 1000
        return f"{seconds:.2f}s"
    else:
        minutes = duration_ms / 60000
        return f"{minutes:.2f}m"


def get_monitoring_summary(hours: int = 24, limit: int = 100) -> Dict[str, Any]:
    """
    Get summary of monitoring data for the dashboard.

    Args:
        hours: Number of hours to look back (default 24)
        limit: Maximum number of recent runs to return

    Returns:
        Dictionary with monitoring summary data
    """
    try:
        # Calculate the time threshold
        threshold = datetime.now(timezone.utc) - timedelta(hours=hours)

        # Query for recent runs summary
        recent_runs_query = """
        WITH run_summary AS (
            SELECT 
                run_uuid,
                MIN(stage_start_time) as start_time,
                MAX(stage_end_time) as end_time,
                COUNT(*) as stage_count,
                SUM(duration_ms) as total_duration_ms,
                SUM(total_tokens) as total_tokens,
                SUM(total_cost) as total_cost,
                STRING_AGG(DISTINCT status, ', ') as statuses,
                BOOL_OR(status != 'Success') as has_errors
            FROM process_monitor_logs
            WHERE stage_start_time >= :threshold
            GROUP BY run_uuid
            ORDER BY start_time DESC
            LIMIT :limit
        )
        SELECT * FROM run_summary
        """

        recent_runs = fetch_all(
            recent_runs_query, 
            params={"threshold": threshold, "limit": limit}, 
            execution_id="monitoring"
        )

        # Query for stage statistics
        stage_stats_query = """
        SELECT 
            stage_name,
            COUNT(*) as execution_count,
            AVG(duration_ms) as avg_duration_ms,
            MIN(duration_ms) as min_duration_ms,
            MAX(duration_ms) as max_duration_ms,
            SUM(CASE WHEN status = 'Success' THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN status != 'Success' THEN 1 ELSE 0 END) as failure_count
        FROM process_monitor_logs
        WHERE stage_start_time >= :threshold
        GROUP BY stage_name
        ORDER BY execution_count DESC
        """

        stage_stats = fetch_all(
            stage_stats_query, 
            params={"threshold": threshold}, 
            execution_id="monitoring"
        )

        # Query for overall statistics
        overall_stats_query = """
        SELECT 
            COUNT(DISTINCT run_uuid) as total_runs,
            COUNT(*) as total_stages,
            AVG(duration_ms) as avg_duration_ms,
            SUM(total_tokens) as total_tokens_used,
            SUM(total_cost) as total_cost_usd
        FROM process_monitor_logs
        WHERE stage_start_time >= :threshold
        """

        overall_stats = fetch_all(
            overall_stats_query, 
            params={"threshold": threshold}, 
            execution_id="monitoring"
        )

        return {
            "recent_runs": recent_runs,
            "stage_statistics": stage_stats,
            "overall_stats": overall_stats[0] if overall_stats else {},
            "time_range_hours": hours,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        logger.error("Failed to get monitoring summary", error=str(e))
        return {"error": str(e)}


def get_run_details(run_uuid: str) -> Dict[str, Any]:
    """
    Get detailed information for a specific run.

    Args:
        run_uuid: UUID of the run to get details for

    Returns:
        Dictionary with run details including all stages
    """
    try:
        # Query for all stages in the run
        stages_query = """
        SELECT
            log_id,
            stage_name,
            stage_start_time,
            stage_end_time,
            duration_ms,
            status,
            total_tokens,
            total_cost,
            decision_details,
            error_message,
            custom_metadata,
            llm_calls
        FROM process_monitor_logs
        WHERE run_uuid = :run_uuid
        ORDER BY stage_start_time
        """

        stages = fetch_all(
            stages_query,
            params={"run_uuid": run_uuid},
            execution_id="monitoring"
        )

        # Calculate run summary
        if stages:
            run_summary = {
                "run_uuid": run_uuid,
                "start_time": stages[0]["stage_start_time"],
                "end_time": stages[-1]["stage_end_time"],
                "total_duration_ms": sum(s["duration_ms"] or 0 for s in stages),
                "total_stages": len(stages),
                "total_tokens": sum(s["total_tokens"] or 0 for s in stages),
                "total_cost": sum(s["total_cost"] or 0 for s in stages),
                "has_errors": any(s["status"] != "Success" for s in stages),
            }
        else:
            run_summary = {
                "run_uuid": run_uuid,
                "error": "Run not found",
                "total_duration_ms": 0,
                "total_stages": 0,
                "total_tokens": 0,
                "total_cost": 0,
                "has_errors": False
            }

        return {"run_summary": run_summary, "stages": stages if stages else []}

    except Exception as e:
        logger.error(f"Failed to get run details for {run_uuid}", error=str(e))
        return {"error": str(e)}


def get_stage_trends(stage_name: str, hours: int = 24) -> Dict[str, Any]:
    """
    Get performance trends for a specific stage.

    Args:
        stage_name: Name of the stage to analyze
        hours: Number of hours to look back

    Returns:
        Dictionary with stage trend data
    """
    try:
        threshold = datetime.now(timezone.utc) - timedelta(hours=hours)

        # Query for stage performance over time
        trends_query = """
        SELECT 
            DATE_TRUNC('hour', stage_start_time) as hour,
            COUNT(*) as execution_count,
            AVG(duration_ms) as avg_duration_ms,
            MIN(duration_ms) as min_duration_ms,
            MAX(duration_ms) as max_duration_ms,
            SUM(CASE WHEN status = 'Success' THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN status != 'Success' THEN 1 ELSE 0 END) as failure_count
        FROM process_monitor_logs
        WHERE stage_name = :stage_name AND stage_start_time >= :threshold
        GROUP BY DATE_TRUNC('hour', stage_start_time)
        ORDER BY hour
        """

        trends = fetch_all(
            trends_query, 
            params={"stage_name": stage_name, "threshold": threshold}, 
            execution_id="monitoring"
        )

        return {
            "stage_name": stage_name,
            "time_range_hours": hours,
            "trends": trends,
        }

    except Exception as e:
        logger.error(f"Failed to get trends for stage {stage_name}", error=str(e))
        return {"error": str(e)}


def search_runs(
    status: Optional[str] = None,
    stage_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """
    Search for runs based on filters.

    Args:
        status: Filter by status
        stage_name: Filter by stage name
        start_date: Start date (ISO format)
        end_date: End date (ISO format)
        limit: Maximum number of results

    Returns:
        List of matching runs
    """
    try:
        # Build the query with filters
        conditions = []
        params = {}

        if status:
            conditions.append("status = :status")
            params["status"] = status

        if stage_name:
            conditions.append("stage_name = :stage_name")
            params["stage_name"] = stage_name

        if start_date:
            conditions.append("stage_start_time >= :start_date")
            params["start_date"] = start_date

        if end_date:
            conditions.append("stage_start_time <= :end_date")
            params["end_date"] = end_date

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        query = f"""
        WITH filtered_runs AS (
            SELECT DISTINCT run_uuid
            FROM process_monitor_logs
            {where_clause}
        ),
        run_summary AS (
            SELECT 
                p.run_uuid,
                MIN(p.stage_start_time) as start_time,
                MAX(p.stage_end_time) as end_time,
                COUNT(*) as stage_count,
                SUM(p.duration_ms) as total_duration_ms,
                STRING_AGG(DISTINCT p.status, ', ') as statuses
            FROM process_monitor_logs p
            INNER JOIN filtered_runs f ON p.run_uuid = f.run_uuid
            GROUP BY p.run_uuid
            ORDER BY start_time DESC
            LIMIT :limit
        )
        SELECT * FROM run_summary
        """

        params["limit"] = limit
        results = fetch_all(query, params=params, execution_id="monitoring")

        return results

    except Exception as e:
        logger.error("Failed to search runs", error=str(e))
        return []


# Flask route handlers
def register_monitoring_routes(app):
    """
    Register monitoring routes with the Flask app.

    Args:
        app: Flask application instance
    """

    @app.route("/api/monitoring/summary")
    def monitoring_summary():
        """Get monitoring summary data."""
        hours = request.args.get("hours", 24, type=int)
        limit = request.args.get("limit", 100, type=int)
        data = get_monitoring_summary(hours, limit)
        return jsonify(data), 200

    @app.route("/api/monitoring/run/<run_uuid>")
    def run_details(run_uuid):
        """Get details for a specific run."""
        data = get_run_details(run_uuid)
        return jsonify(data), 200

    @app.route("/api/monitoring/stage/<stage_name>/trends")
    def stage_trends(stage_name):
        """Get trends for a specific stage."""
        hours = request.args.get("hours", 24, type=int)
        data = get_stage_trends(stage_name, hours)
        return jsonify(data), 200

    @app.route("/api/monitoring/search")
    def search_monitoring():
        """Search for runs with filters."""
        status = request.args.get("status")
        stage_name = request.args.get("stage_name")
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        limit = request.args.get("limit", 100, type=int)

        results = search_runs(status, stage_name, start_date, end_date, limit)
        return jsonify(results), 200