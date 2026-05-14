"""Tests for the supplementary financials table creation script."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from scripts import create_master_data_table


def test_main_dry_run_creates_public_data_and_embedding_tables(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Dry runs should print both public retrieval table definitions."""
    missing_env = tmp_path / ".env"

    exit_code = create_master_data_table.main(
        [
            "--env-file",
            str(missing_env),
            "--embedding-dimensions",
            "2",
        ],
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert 'CREATE TABLE IF NOT EXISTS "public"."aegis-financial-supp-data"' in output
    assert 'CREATE TABLE IF NOT EXISTS "public"."aegis-financial-supp-embeddings"' in output
    assert '"chunk_embedding" vector(2)' in output
    assert '"embedding" vector(2)' in output
    assert "Dry run complete" in output


def test_build_setup_statements_can_store_embeddings_as_jsonb(tmp_path: Path) -> None:
    """JSONB embedding storage should avoid pgvector extension DDL."""
    script_config = create_master_data_table.ScriptConfig(
        env_file=tmp_path / ".env",
        master_data_csv=None,
        master_embeddings_csv=None,
        data_table="aegis-financial-supp-data",
        embeddings_table="aegis-financial-supp-embeddings",
        embedding_storage="jsonb",
        embedding_dimensions=3072,
        apply=False,
        create_vector_extension=False,
    )

    ddl = create_master_data_table._render_statements(  # pylint: disable=protected-access
        create_master_data_table._build_setup_statements(script_config),
    )

    assert "CREATE EXTENSION" not in ddl
    assert '"keyword_embedding" jsonb' in ddl
    assert '"embedding" jsonb' in ddl
    assert "PRIMARY KEY (file_id, chunk_id)" in ddl
    assert "PRIMARY KEY (embedding_id)" in ddl


def test_validate_csv_header_rejects_header_drift(tmp_path: Path) -> None:
    """Optional CSV validation should catch schema drift before DB work."""
    master_csv = tmp_path / "master-data.csv"
    _write_csv(master_csv, ["source_type", "file_id"], ["financial-supp", "row-1"])

    with pytest.raises(ValueError, match="master data CSV header does not match"):
        create_master_data_table._validate_csv_header(  # pylint: disable=protected-access
            master_csv,
            create_master_data_table.MASTER_DATA_FIELDS,
            "master data",
        )


def _write_csv(path: Path, header: list[str], row: list[str]) -> None:
    """Write a small CSV fixture."""
    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.writer(file_obj)
        writer.writerow(header)
        writer.writerow(row)
