"""
Script to migrate all category Excel files to the new 6-column standardized format.

New format columns:
1. transcript_sections
2. category_name
3. category_description
4. example_1
5. example_2
6. example_3
"""

import os
import pandas as pd
from pathlib import Path


def migrate_call_summary_file(file_path: str) -> None:
    """
    Migrate Call Summary category file from 3-column to 6-column format.
    Old: transcripts_section, category_name, category_description
    New: transcript_sections, category_name, category_description, example_1, example_2, example_3
    """
    print(f"\nMigrating: {file_path}")

    df = pd.read_excel(file_path)
    print(f"  Current columns: {list(df.columns)}")
    print(f"  Current rows: {len(df)}")

    # Check if already has new format
    if "transcript_sections" in df.columns:
        print("  ✓ Already in new format (has transcript_sections)")
        return

    # Rename transcripts_section to transcript_sections
    if "transcripts_section" in df.columns:
        df.rename(columns={"transcripts_section": "transcript_sections"}, inplace=True)
        print("  ✓ Renamed 'transcripts_section' to 'transcript_sections'")

    # Add example columns if missing
    for col in ["example_1", "example_2", "example_3"]:
        if col not in df.columns:
            df[col] = ""
            print(f"  ✓ Added empty '{col}' column")

    # Ensure correct column order
    df = df[["transcript_sections", "category_name", "category_description", "example_1", "example_2", "example_3"]]

    # Save back to Excel
    df.to_excel(file_path, index=False)
    print(f"  ✓ Saved updated file")


def migrate_key_themes_file(file_path: str) -> None:
    """
    Migrate Key Themes category file from 2-column to 6-column format.
    Old: category_name, category_description
    New: transcript_sections, category_name, category_description, example_1, example_2, example_3
    """
    print(f"\nMigrating: {file_path}")

    df = pd.read_excel(file_path)
    print(f"  Current columns: {list(df.columns)}")
    print(f"  Current rows: {len(df)}")

    # Check if already has new format
    if "transcript_sections" in df.columns:
        print("  ✓ Already in new format (has transcript_sections)")
        return

    # Add transcript_sections column (default to "QA" since Key Themes only uses Q&A)
    if "transcript_sections" not in df.columns:
        df.insert(0, "transcript_sections", "QA")
        print("  ✓ Added 'transcript_sections' column with default 'QA'")

    # Add example columns if missing
    for col in ["example_1", "example_2", "example_3"]:
        if col not in df.columns:
            df[col] = ""
            print(f"  ✓ Added empty '{col}' column")

    # Ensure correct column order
    df = df[["transcript_sections", "category_name", "category_description", "example_1", "example_2", "example_3"]]

    # Save back to Excel
    df.to_excel(file_path, index=False)
    print(f"  ✓ Saved updated file")


def migrate_cm_readthrough_file(file_path: str) -> None:
    """
    Migrate CM Readthrough category file from 5 positional columns to 6 named columns.
    Old: [no column names] Col0=category, Col1=description, Col2-4=examples
    New: transcript_sections, category_name, category_description, example_1, example_2, example_3
    """
    print(f"\nMigrating: {file_path}")

    df = pd.read_excel(file_path, header=None)
    print(f"  Current shape: {df.shape}")

    # Check if it already has named columns (not positional)
    if "category_name" in df.columns or "category_name" in str(df.iloc[0].values):
        print("  ⚠️  File appears to have column names already, loading with header...")
        df = pd.read_excel(file_path)

        # Check if already in new format
        if "transcript_sections" in df.columns:
            print("  ✓ Already in new format")
            return

    # Create new DataFrame with named columns
    new_data = []

    for idx, row in df.iterrows():
        # Skip completely empty rows
        if row.isna().all():
            continue

        new_row = {
            "transcript_sections": "QA",  # Default for CM Readthrough
            "category_name": str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else "",
            "category_description": str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else "",
            "example_1": str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else "",
            "example_2": str(row.iloc[3]).strip() if len(row) > 3 and pd.notna(row.iloc[3]) else "",
            "example_3": str(row.iloc[4]).strip() if len(row) > 4 and pd.notna(row.iloc[4]) else "",
        }

        # Skip rows with empty category_name
        if new_row["category_name"]:
            new_data.append(new_row)

    new_df = pd.DataFrame(new_data)
    print(f"  ✓ Converted {len(new_df)} rows from positional to named columns")
    print(f"  ✓ New columns: {list(new_df.columns)}")

    # Save back to Excel
    new_df.to_excel(file_path, index=False)
    print(f"  ✓ Saved updated file")


def main():
    """Migrate all 6 category files to the new standardized format."""

    base_path = Path(__file__).parent.parent / "src" / "aegis" / "etls"

    # Call Summary files (2)
    call_summary_files = [
        base_path / "call_summary/config/categories/canadian_banks_categories.xlsx",
        base_path / "call_summary/config/categories/us_banks_categories.xlsx",
    ]

    # Key Themes files (1)
    key_themes_files = [
        base_path / "key_themes/config/categories/key_themes_categories.xlsx",
    ]

    # CM Readthrough files (3)
    cm_readthrough_files = [
        base_path / "cm_readthrough/config/categories/outlook_categories.xlsx",
        base_path / "cm_readthrough/config/categories/qa_market_volatility_regulatory_categories.xlsx",
        base_path / "cm_readthrough/config/categories/qa_pipelines_activity_categories.xlsx",
    ]

    print("="*80)
    print("CATEGORY FILE MIGRATION - Converting to 6-column standardized format")
    print("="*80)

    print("\n--- Call Summary Files (2) ---")
    for file_path in call_summary_files:
        if not file_path.exists():
            print(f"\n⚠️  File not found: {file_path}")
            continue
        migrate_call_summary_file(str(file_path))

    print("\n--- Key Themes Files (1) ---")
    for file_path in key_themes_files:
        if not file_path.exists():
            print(f"\n⚠️  File not found: {file_path}")
            continue
        migrate_key_themes_file(str(file_path))

    print("\n--- CM Readthrough Files (3) ---")
    for file_path in cm_readthrough_files:
        if not file_path.exists():
            print(f"\n⚠️  File not found: {file_path}")
            continue
        migrate_cm_readthrough_file(str(file_path))

    print("\n" + "="*80)
    print("MIGRATION COMPLETE")
    print("="*80)
    print("\nAll 6 category files have been migrated to the new 6-column format:")
    print("  1. transcript_sections")
    print("  2. category_name")
    print("  3. category_description")
    print("  4. example_1")
    print("  5. example_2")
    print("  6. example_3")


if __name__ == "__main__":
    main()
