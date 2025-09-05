#!/bin/bash
# Fix pgvector installation for PostgreSQL
# This script creates the necessary symbolic links for PostgreSQL to find pgvector

set -e

echo "pgvector Fix Script"
echo "=================="
echo ""

# Detect Mac architecture
if [[ -d "/opt/homebrew" ]]; then
    BREW_PREFIX="/opt/homebrew"
    echo "Detected: Apple Silicon (M1/M2) Mac"
else
    BREW_PREFIX="/usr/local"
    echo "Detected: Intel Mac"
fi

# Check if pgvector is installed
if ! brew list pgvector &>/dev/null; then
    echo "❌ Error: pgvector is not installed"
    echo "Please run: brew install pgvector"
    exit 1
fi

echo "✓ pgvector is installed via Homebrew"

# Find PostgreSQL version
PG_VERSION=$(psql --version | grep -oE '[0-9]+' | head -1)
echo "PostgreSQL major version: $PG_VERSION"

# Define paths
PGVECTOR_LIB="${BREW_PREFIX}/lib/postgresql@${PG_VERSION}/pgvector.so"
PGVECTOR_CONTROL="${BREW_PREFIX}/share/postgresql@${PG_VERSION}/extension/vector.control"
PG_LIB_DIR="${BREW_PREFIX}/opt/postgresql@${PG_VERSION}/lib/postgresql"
PG_EXT_DIR="${BREW_PREFIX}/opt/postgresql@${PG_VERSION}/share/postgresql@${PG_VERSION}/extension"

echo ""
echo "Creating symbolic links..."
echo "=========================="

# Create directories if they don't exist
if [[ ! -d "$PG_LIB_DIR" ]]; then
    echo "Creating lib directory: $PG_LIB_DIR"
    sudo mkdir -p "$PG_LIB_DIR"
fi

if [[ ! -d "$PG_EXT_DIR" ]]; then
    echo "Creating extension directory: $PG_EXT_DIR"
    sudo mkdir -p "$PG_EXT_DIR"
fi

# Create symbolic links
if [[ -f "$PGVECTOR_LIB" ]]; then
    echo "Linking: $PGVECTOR_LIB -> $PG_LIB_DIR/pgvector.so"
    sudo ln -sf "$PGVECTOR_LIB" "$PG_LIB_DIR/pgvector.so"
else
    echo "⚠️  Warning: pgvector.so not found at $PGVECTOR_LIB"
fi

if [[ -f "$PGVECTOR_CONTROL" ]]; then
    echo "Linking: $PGVECTOR_CONTROL -> $PG_EXT_DIR/vector.control"
    sudo ln -sf "$PGVECTOR_CONTROL" "$PG_EXT_DIR/vector.control"
    
    # Also link SQL files
    for sql_file in "${BREW_PREFIX}/share/postgresql@${PG_VERSION}/extension"/vector--*.sql; do
        if [[ -f "$sql_file" ]]; then
            filename=$(basename "$sql_file")
            echo "Linking: $sql_file -> $PG_EXT_DIR/$filename"
            sudo ln -sf "$sql_file" "$PG_EXT_DIR/$filename"
        fi
    done
else
    echo "⚠️  Warning: vector.control not found at $PGVECTOR_CONTROL"
fi

echo ""
echo "Restarting PostgreSQL..."
echo "======================="
brew services restart postgresql@${PG_VERSION}

echo ""
echo "Testing pgvector installation..."
echo "================================"

# Wait for PostgreSQL to restart
sleep 2

# Check if extension is available
if psql -p 34532 -d postgres -U $(whoami) -qtc "SELECT 1 FROM pg_available_extensions WHERE name = 'vector';" | grep -q 1; then
    echo "✓ pgvector extension is now available!"
    echo ""
    echo "Enabling extension in finance-dev database..."
    psql -p 34532 -d finance-dev -U $(whoami) -c "CREATE EXTENSION IF NOT EXISTS vector;" 2>/dev/null && \
        echo "✓ Extension enabled successfully!" || \
        echo "⚠️  Extension may already be enabled or requires superuser privileges"
else
    echo "❌ pgvector extension is still not available"
    echo ""
    echo "Please try the manual installation method:"
    echo "  1. cd /tmp"
    echo "  2. git clone https://github.com/pgvector/pgvector.git"
    echo "  3. cd pgvector"
    echo "  4. make"
    echo "  5. sudo make install"
    echo "  6. brew services restart postgresql@${PG_VERSION}"
fi

echo ""
echo "Done!"