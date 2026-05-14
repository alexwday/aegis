"""Create PostgreSQL tables for the supplementary financials retriever.

This Aegis-side copy keeps the DDL used by the supplementary financials
ingestion project close to the subagent that queries it.

Usage:
    source venv/bin/activate
    python scripts/create_master_data_table.py
    python scripts/create_master_data_table.py --apply
    python scripts/create_master_data_table.py --env-file /path/.env --apply
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PsycopgConnection

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from aegis.utils.settings import config  # noqa: E402


PUBLIC_SCHEMA = "public"
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"
DEFAULT_DATA_TABLE_NAME = "aegis-financial-supp-data"
DEFAULT_EMBEDDINGS_TABLE_NAME = "aegis-financial-supp-embeddings"
MASTER_DATA_FIELDS = (
    "source_type",
    "fiscal_year",
    "quarter",
    "bank",
    "filename",
    "file_id",
    "file_type",
    "file_path",
    "file_hash",
    "page_number",
    "name",
    "summary",
    "chunk_id",
    "chunk_content",
    "keywords",
    "metrics",
    "keyword_embedding",
    "metric_embedding",
    "summary_embedding",
    "chunk_embedding",
    "created_at",
)
MASTER_EMBEDDINGS_FIELDS = (
    "embedding_id",
    "embedding_type",
    "embedding_scope",
    "source_type",
    "fiscal_year",
    "quarter",
    "bank",
    "filename",
    "file_id",
    "file_type",
    "file_path",
    "file_hash",
    "content_unit_id",
    "content_unit_ids",
    "chunk_id",
    "section_id",
    "embedding_text",
    "text_hash",
    "embedding",
    "embedding_model",
    "embedding_dimensions",
    "created_at",
)
DATA_EMBEDDING_COLUMNS = (
    "keyword_embedding",
    "metric_embedding",
    "summary_embedding",
    "chunk_embedding",
)
DATA_SCALAR_COLUMNS = {
    "source_type": "text NOT NULL",
    "fiscal_year": "text NOT NULL",
    "quarter": "text NOT NULL",
    "bank": "text NOT NULL",
    "filename": "text NOT NULL",
    "file_id": "text NOT NULL",
    "file_type": "text NOT NULL",
    "file_path": "text NOT NULL",
    "file_hash": "text NOT NULL",
    "page_number": "integer",
    "name": "text",
    "summary": "text",
    "chunk_id": "text NOT NULL",
    "chunk_content": "text",
    "keywords": "jsonb NOT NULL DEFAULT '[]'::jsonb",
    "metrics": "jsonb NOT NULL DEFAULT '[]'::jsonb",
    "created_at": "timestamptz",
}
EMBEDDINGS_SCALAR_COLUMNS = {
    "embedding_id": "text NOT NULL",
    "embedding_type": "text NOT NULL",
    "embedding_scope": "text NOT NULL",
    "source_type": "text NOT NULL",
    "fiscal_year": "text NOT NULL",
    "quarter": "text NOT NULL",
    "bank": "text NOT NULL",
    "filename": "text NOT NULL",
    "file_id": "text NOT NULL",
    "file_type": "text NOT NULL",
    "file_path": "text NOT NULL",
    "file_hash": "text NOT NULL",
    "content_unit_id": "text",
    "content_unit_ids": "jsonb NOT NULL DEFAULT '[]'::jsonb",
    "chunk_id": "text",
    "section_id": "text",
    "embedding_text": "text NOT NULL",
    "text_hash": "text",
    "embedding_model": "text NOT NULL",
    "embedding_dimensions": "integer NOT NULL",
    "created_at": "timestamptz NOT NULL",
}


@dataclass(frozen=True)
class ScriptConfig:
    """Resolved inputs required to create the retrieval tables."""

    env_file: Optional[Path]
    master_data_csv: Optional[Path]
    master_embeddings_csv: Optional[Path]
    data_table: str
    embeddings_table: str
    embedding_storage: str
    embedding_dimensions: int
    apply: bool
    create_vector_extension: bool


def main(argv: Optional[list[str]] = None) -> int:
    """Parse CLI arguments and create or display table DDL."""
    args = _parse_args(argv)
    script_config = _resolve_config(args)
    _validate_optional_csv_headers(script_config)

    statements = _build_setup_statements(script_config)
    if not script_config.apply:
        print(_render_statements(statements))
        print(
            "\nDry run complete. Re-run with --apply to execute against PostgreSQL.",
        )
        return 0

    with _get_db_connection() as conn:
        _execute_statements(conn, statements)

    print(
        "Master tables created: "
        f'{PUBLIC_SCHEMA}."{script_config.data_table}", '
        f'{PUBLIC_SCHEMA}."{script_config.embeddings_table}"',
    )
    return 0


def _parse_args(argv: Optional[list[str]]) -> argparse.Namespace:
    """Return validated command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Create public supplementary financials retrieval tables.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help="Dotenv file with Aegis PostgreSQL settings. Defaults to .env.",
    )
    parser.add_argument(
        "--master-data-csv",
        type=Path,
        help="Optional master data CSV to validate against the chunk table schema.",
    )
    parser.add_argument(
        "--master-embeddings-csv",
        type=Path,
        help="Optional master embeddings CSV to validate against the embeddings table schema.",
    )
    parser.add_argument(
        "--data-table-name",
        default=DEFAULT_DATA_TABLE_NAME,
        help=f"Public chunk table name. Defaults to {DEFAULT_DATA_TABLE_NAME!r}.",
    )
    parser.add_argument(
        "--embeddings-table-name",
        default=DEFAULT_EMBEDDINGS_TABLE_NAME,
        help=("Public embeddings table name. Defaults to " f"{DEFAULT_EMBEDDINGS_TABLE_NAME!r}."),
    )
    parser.add_argument(
        "--embedding-storage",
        choices=("vector", "jsonb", "text"),
        default="vector",
        help="Column type for embedding fields.",
    )
    parser.add_argument(
        "--embedding-dimensions",
        type=int,
        help="Vector dimensions when --embedding-storage vector is used.",
    )
    parser.add_argument(
        "--skip-vector-extension",
        action="store_true",
        help="Do not run CREATE EXTENSION IF NOT EXISTS vector.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute SQL. Without this flag, the script is a dry run.",
    )
    return parser.parse_args(argv)


def _resolve_config(args: argparse.Namespace) -> ScriptConfig:
    """Load environment settings and resolve filesystem and DB targets."""
    env_file = args.env_file.expanduser().resolve() if args.env_file else None
    if env_file and env_file.is_file():
        load_dotenv(env_file, override=True)
        config.load_config()
    elif args.apply:
        raise FileNotFoundError(f"Env file not found: {env_file}")

    master_data_csv = _resolve_optional_file(args.master_data_csv, "Master data CSV")
    master_embeddings_csv = _resolve_optional_file(
        args.master_embeddings_csv,
        "Master embeddings CSV",
    )
    dimensions = args.embedding_dimensions or config.llm.embedding.dimensions
    if dimensions < 1:
        raise ValueError("--embedding-dimensions must be a positive integer")

    return ScriptConfig(
        env_file=env_file if env_file and env_file.is_file() else None,
        master_data_csv=master_data_csv,
        master_embeddings_csv=master_embeddings_csv,
        data_table=str(args.data_table_name).strip(),
        embeddings_table=str(args.embeddings_table_name).strip(),
        embedding_storage=args.embedding_storage,
        embedding_dimensions=dimensions,
        apply=args.apply,
        create_vector_extension=not args.skip_vector_extension,
    )


def _resolve_optional_file(path: Optional[Path], label: str) -> Optional[Path]:
    """Return a resolved optional path, raising when a provided file is missing."""
    if path is None:
        return None
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"{label} not found: {resolved}")
    return resolved


def _validate_optional_csv_headers(script_config: ScriptConfig) -> None:
    """Validate optional CSV headers when paths are provided."""
    if script_config.master_data_csv:
        _validate_csv_header(script_config.master_data_csv, MASTER_DATA_FIELDS, "master data")
    if script_config.master_embeddings_csv:
        _validate_csv_header(
            script_config.master_embeddings_csv,
            MASTER_EMBEDDINGS_FIELDS,
            "master embeddings",
        )


def _validate_csv_header(path: Path, expected_fields: tuple[str, ...], label: str) -> None:
    """Reject CSV files whose header does not match the expected schema."""
    with path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.reader(file_obj)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise ValueError(f"{label} CSV is empty: {path}") from exc

    expected = list(expected_fields)
    if header != expected:
        raise ValueError(
            f"{label} CSV header does not match expected fields.\n"
            f"Expected: {expected}\n"
            f"Actual:   {header}",
        )


def _build_setup_statements(script_config: ScriptConfig) -> list[sql.Composable]:
    """Return SQL statements for both public retrieval tables."""
    statements: list[sql.Composable] = []
    if script_config.embedding_storage == "vector" and script_config.create_vector_extension:
        statements.append(sql.SQL("CREATE EXTENSION IF NOT EXISTS vector"))

    statements.append(_create_data_table_statement(script_config))
    statements.append(_create_embeddings_table_statement(script_config))
    return statements


def _create_data_table_statement(script_config: ScriptConfig) -> sql.Composable:
    """Return CREATE TABLE DDL for the chunk-level master data table."""
    definitions = []
    for column in MASTER_DATA_FIELDS:
        definitions.append(
            sql.SQL("{} {}").format(
                sql.Identifier(column),
                sql.SQL(_data_column_type(column, script_config)),
            ),
        )
    definitions.append(sql.SQL("PRIMARY KEY (file_id, chunk_id)"))
    return sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
        _table_ref(script_config.data_table),
        sql.SQL(", ").join(definitions),
    )


def _create_embeddings_table_statement(script_config: ScriptConfig) -> sql.Composable:
    """Return CREATE TABLE DDL for the long-form embeddings table."""
    definitions = []
    for column in MASTER_EMBEDDINGS_FIELDS:
        definitions.append(
            sql.SQL("{} {}").format(
                sql.Identifier(column),
                sql.SQL(_embeddings_column_type(column, script_config)),
            ),
        )
    definitions.append(sql.SQL("PRIMARY KEY (embedding_id)"))
    return sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
        _table_ref(script_config.embeddings_table),
        sql.SQL(", ").join(definitions),
    )


def _data_column_type(column: str, script_config: ScriptConfig) -> str:
    """Return a PostgreSQL column type for one master data field."""
    if column in DATA_EMBEDDING_COLUMNS:
        return _embedding_column_type(script_config)
    return DATA_SCALAR_COLUMNS[column]


def _embeddings_column_type(column: str, script_config: ScriptConfig) -> str:
    """Return a PostgreSQL column type for one master embeddings field."""
    if column == "embedding":
        return _embedding_column_type(script_config)
    return EMBEDDINGS_SCALAR_COLUMNS[column]


def _embedding_column_type(script_config: ScriptConfig) -> str:
    """Return the configured SQL type for vector-like embedding columns."""
    if script_config.embedding_storage == "vector":
        return f"vector({script_config.embedding_dimensions})"
    if script_config.embedding_storage == "jsonb":
        return "jsonb"
    return "text"


def _get_db_connection() -> Any:
    """Create a psycopg2 connection using Aegis settings."""
    db_url = (
        f"host={config.postgres_host} port={config.postgres_port} "
        f"dbname={config.postgres_database} user={config.postgres_user} "
        f"password={config.postgres_password}"
    )
    return psycopg2.connect(
        db_url,
        application_name="aegis-create-supplementary-financials-tables",
    )


def _execute_statements(
    conn: PsycopgConnection,
    statements: list[sql.Composable],
) -> None:
    """Execute DDL statements in one transaction."""
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)


def _render_statements(statements: list[sql.Composable]) -> str:
    """Render SQL for dry-run review without opening a live DB connection."""
    rendered = []
    for statement in statements:
        rendered.append(_render_composable(statement).rstrip(";") + ";")
    return "\n\n".join(rendered)


def _render_composable(value: sql.Composable) -> str:
    """Render psycopg2.sql objects without requiring a PostgreSQL connection."""
    if isinstance(value, sql.SQL):
        return value.string
    if isinstance(value, sql.Identifier):
        return ".".join(_quote_identifier(part) for part in value.strings)
    if isinstance(value, sql.Composed):
        return "".join(_render_composable(part) for part in value.seq)
    raise TypeError(f"Unsupported SQL composable: {type(value)!r}")


def _quote_identifier(value: str) -> str:
    """Return a double-quoted PostgreSQL identifier."""
    return '"' + value.replace('"', '""') + '"'


def _table_ref(table_name: str) -> sql.Identifier:
    """Return a public-schema table identifier."""
    return sql.Identifier(PUBLIC_SCHEMA, table_name)


if __name__ == "__main__":
    raise SystemExit(main())
