#!/bin/bash
# Debug script for theme_grouping prompt troubleshooting
# This script runs the key_themes ETL with DEBUG logging and filters for regrouping events

# Save original LOG_LEVEL
ORIGINAL_LOG_LEVEL=$(grep "^LOG_LEVEL=" .env | cut -d= -f2)

# Temporarily set DEBUG logging
sed -i.bak 's/^LOG_LEVEL=.*/LOG_LEVEL=DEBUG/' .env

echo "Running key_themes ETL with DEBUG logging..."
echo "Looking for regrouping.* log entries..."
echo "=========================================="

source venv/bin/activate

python -m aegis.etls.key_themes.main \
  --bank "Royal Bank of Canada" \
  --year 2025 \
  --quarter Q2 \
  2>&1 | grep -E "(regrouping\.|etl\.key_themes\.(started|completed|error)|✅|❌)"

# Restore original LOG_LEVEL
sed -i.bak "s/^LOG_LEVEL=.*/LOG_LEVEL=${ORIGINAL_LOG_LEVEL}/" .env
rm .env.bak

echo ""
echo "=========================================="
echo "Log level restored to: ${ORIGINAL_LOG_LEVEL}"
