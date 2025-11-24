#!/usr/bin/env python3
"""
Reports Manager - Standalone web interface for viewing and deleting aegis_reports records.

Usage:
    python scripts/reports_manager.py

Then open browser to: http://localhost:5002
"""

import sys
from pathlib import Path
from contextlib import contextmanager

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from flask import Flask, render_template, jsonify, request  # noqa: E402
from sqlalchemy import create_engine, text  # noqa: E402
from sqlalchemy.pool import QueuePool  # noqa: E402
from aegis.utils.settings import config  # noqa: E402

app = Flask(__name__, template_folder="../templates")

# Create synchronous database engine for Flask
sync_engine = create_engine(
    f"postgresql://{config.postgres_user}:{config.postgres_password}"
    f"@{config.postgres_host}:{config.postgres_port}/{config.postgres_database}",
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
)


@contextmanager
def get_db_connection():
    """Context manager for database connections."""
    conn = sync_engine.connect()
    try:
        yield conn
    finally:
        conn.close()


@app.route("/")
def index():
    """Render main reports manager page."""
    return render_template("reports_manager.html")


@app.route("/api/reports", methods=["GET"])
def get_reports():
    """Get all reports from database."""
    try:
        with get_db_connection() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT
                        id,
                        report_name,
                        report_description,
                        report_type,
                        bank_id,
                        bank_name,
                        bank_symbol,
                        fiscal_year,
                        quarter,
                        local_filepath,
                        s3_document_name,
                        s3_pdf_name,
                        generation_date,
                        date_last_modified,
                        generated_by,
                        metadata,
                        LENGTH(markdown_content) as content_length
                    FROM aegis_reports
                    ORDER BY bank_name, fiscal_year DESC, quarter DESC, report_type
                """
                )
            )

            rows = result.fetchall()
            reports = []

            for row in rows:
                reports.append(
                    {
                        "id": row[0],
                        "report_name": row[1],
                        "report_description": row[2],
                        "report_type": row[3],
                        "bank_id": row[4],
                        "bank_name": row[5],
                        "bank_symbol": row[6],
                        "fiscal_year": row[7],
                        "quarter": row[8],
                        "local_filepath": row[9],
                        "s3_document_name": row[10],
                        "s3_pdf_name": row[11],
                        "generation_date": row[12].isoformat() if row[12] else None,
                        "date_last_modified": row[13].isoformat() if row[13] else None,
                        "generated_by": row[14],
                        "metadata": row[15],
                        "content_length": row[16],
                    }
                )

            return jsonify({"success": True, "reports": reports})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/report/<int:report_id>", methods=["GET"])
def get_report(report_id: int):
    """Get single report by ID including markdown content."""
    try:
        with get_db_connection() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT
                        id,
                        report_name,
                        report_description,
                        report_type,
                        bank_id,
                        bank_name,
                        bank_symbol,
                        fiscal_year,
                        quarter,
                        local_filepath,
                        s3_document_name,
                        s3_pdf_name,
                        markdown_content,
                        generation_date,
                        date_last_modified,
                        generated_by,
                        metadata
                    FROM aegis_reports
                    WHERE id = :id
                """
                ),
                {"id": report_id},
            )

            row = result.fetchone()

            if not row:
                return jsonify({"success": False, "error": "Report not found"}), 404

            report = {
                "id": row[0],
                "report_name": row[1],
                "report_description": row[2],
                "report_type": row[3],
                "bank_id": row[4],
                "bank_name": row[5],
                "bank_symbol": row[6],
                "fiscal_year": row[7],
                "quarter": row[8],
                "local_filepath": row[9],
                "s3_document_name": row[10],
                "s3_pdf_name": row[11],
                "markdown_content": row[12],
                "generation_date": row[13].isoformat() if row[13] else None,
                "date_last_modified": row[14].isoformat() if row[14] else None,
                "generated_by": row[15],
                "metadata": row[16],
            }

            return jsonify({"success": True, "report": report})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/report/<int:report_id>", methods=["DELETE"])
def delete_report(report_id: int):
    """Delete a single report by ID."""
    try:
        with get_db_connection() as conn:
            # First check if the report exists
            result = conn.execute(
                text(
                    "SELECT id, report_name, bank_symbol, quarter, fiscal_year "
                    "FROM aegis_reports WHERE id = :id"
                ),
                {"id": report_id},
            )
            row = result.fetchone()

            if not row:
                return jsonify({"success": False, "error": "Report not found"}), 404

            report_info = f"{row[1]} ({row[2]} {row[3]} {row[4]})"

            # Delete the report
            conn.execute(text("DELETE FROM aegis_reports WHERE id = :id"), {"id": report_id})
            conn.commit()

            return jsonify({"success": True, "message": f"Deleted report: {report_info}"})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/reports/delete-bulk", methods=["POST"])
def delete_reports_bulk():
    """Delete multiple reports by IDs."""
    try:
        data = request.json
        report_ids = data.get("ids", [])

        if not report_ids:
            return jsonify({"success": False, "error": "No report IDs provided"}), 400

        with get_db_connection() as conn:
            # Delete the reports
            result = conn.execute(
                text("DELETE FROM aegis_reports WHERE id = ANY(:ids)"), {"ids": report_ids}
            )
            conn.commit()

            deleted_count = result.rowcount

            return jsonify(
                {
                    "success": True,
                    "message": f"Deleted {deleted_count} report(s)",
                    "deleted_count": deleted_count,
                }
            )

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Reports Manager Starting...")
    print("=" * 60)
    print(f"\nDatabase: {config.postgres_host}:{config.postgres_port}/{config.postgres_database}")
    print("Web Interface: http://localhost:5002")
    print("\nPress Ctrl+C to stop\n")
    print("=" * 60 + "\n")

    app.run(host="0.0.0.0", port=5002, debug=True)
