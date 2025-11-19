#!/bin/bash
# Simple script to show only key stages and errors from key_themes ETL
# Usage: ./scripts/show_key_themes_errors.sh --bank "Royal Bank of Canada" --year 2025 --quarter Q2

source venv/bin/activate

echo "Running key_themes ETL with error highlighting..."
echo "=================================================="
echo ""

# Run with color codes preserved and filter for key stages + errors
LOG_LEVEL=INFO python -m aegis.etls.key_themes.main "$@" 2>&1 | \
  grep -E "started|completed|ERROR|FAILED|theme_grouping|grouping|regrouping|✗|❌|WARNING" | \
  head -50

echo ""
echo "=================================================="
echo "For full debug output, run:"
echo "  LOG_LEVEL=DEBUG python -m aegis.etls.key_themes.main $@"
