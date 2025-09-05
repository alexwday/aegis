#!/usr/bin/env python3
"""
Diagnostic script for pgvector installation issues.

This script helps identify why PostgreSQL cannot find the pgvector extension
even though it's installed via Homebrew.
"""

import os
import subprocess
import sys
from pathlib import Path

def run_command(cmd, shell=True, capture=True):
    """Run a shell command and return output."""
    try:
        if capture:
            result = subprocess.run(cmd, shell=shell, capture_output=True, text=True)
            return result.stdout.strip(), result.stderr.strip(), result.returncode
        else:
            subprocess.run(cmd, shell=shell)
            return "", "", 0
    except Exception as e:
        return "", str(e), 1

def find_postgres_paths():
    """Find PostgreSQL installation paths."""
    print("=" * 60)
    print("Finding PostgreSQL installation paths...")
    print("=" * 60)
    
    # Check PostgreSQL version and installation
    stdout, _, _ = run_command("psql --version")
    print(f"PostgreSQL client: {stdout}")
    
    # Find PostgreSQL data directory
    stdout, _, _ = run_command("psql -p 34532 -d postgres -U $(whoami) -t -c 'SHOW data_directory;' 2>/dev/null")
    if stdout:
        print(f"Data directory: {stdout}")
    
    # Find extension directory
    stdout, _, _ = run_command("psql -p 34532 -d postgres -U $(whoami) -t -c 'SHOW extension_destdir;' 2>/dev/null")
    print(f"Extension directory setting: {stdout if stdout else 'Not set (using default)'}")
    
    # Find actual extension path
    stdout, _, _ = run_command("pg_config --sharedir 2>/dev/null")
    if stdout:
        print(f"PostgreSQL share directory: {stdout}")
        extension_dir = Path(stdout) / "extension"
        if extension_dir.exists():
            print(f"Extension directory exists: {extension_dir}")
            # List vector-related files
            vector_files = list(extension_dir.glob("vector*"))
            if vector_files:
                print(f"Vector extension files found:")
                for f in vector_files:
                    print(f"  - {f}")
            else:
                print("  ⚠️  No vector extension files found in extension directory")
    
    # Find library directory
    stdout, _, _ = run_command("pg_config --pkglibdir 2>/dev/null")
    if stdout:
        print(f"PostgreSQL library directory: {stdout}")
        lib_dir = Path(stdout)
        if lib_dir.exists():
            # Look for pgvector.so
            pgvector_so = lib_dir / "pgvector.so"
            if pgvector_so.exists():
                print(f"  ✓ pgvector.so found: {pgvector_so}")
            else:
                print(f"  ⚠️  pgvector.so NOT found in {lib_dir}")

def find_pgvector_installation():
    """Find where Homebrew installed pgvector."""
    print("\n" + "=" * 60)
    print("Finding pgvector installation...")
    print("=" * 60)
    
    # Check if pgvector is installed
    stdout, _, returncode = run_command("brew list pgvector 2>/dev/null")
    if returncode != 0:
        print("❌ pgvector is NOT installed via Homebrew")
        print("Run: brew install pgvector")
        return None
    
    print("✓ pgvector is installed via Homebrew")
    
    # Get pgvector version
    stdout, _, _ = run_command("brew info pgvector --json | python3 -c 'import json, sys; data=json.load(sys.stdin)[0]; print(data[\"installed\"][0][\"version\"] if data.get(\"installed\") else \"Not found\")'")
    print(f"Version: {stdout}")
    
    # Find pgvector files
    print("\nSearching for pgvector files...")
    
    # Common locations based on Mac type
    if os.path.exists("/opt/homebrew"):  # M1/M2 Mac
        base_path = "/opt/homebrew"
    else:  # Intel Mac
        base_path = "/usr/local"
    
    # Search for pgvector files
    search_paths = [
        f"{base_path}/lib/postgresql@15/pgvector.so",
        f"{base_path}/lib/postgresql/pgvector.so",
        f"{base_path}/lib/pgvector.so",
        f"{base_path}/share/postgresql@15/extension/vector.control",
        f"{base_path}/share/postgresql/extension/vector.control",
    ]
    
    found_files = {}
    for path in search_paths:
        if os.path.exists(path):
            found_files[path] = True
            print(f"  ✓ Found: {path}")
    
    if not found_files:
        print("  ⚠️  No pgvector files found in expected locations")
        
        # Try to find them with brew
        stdout, _, _ = run_command("brew list pgvector")
        if stdout:
            print("\n  Files installed by Homebrew:")
            for line in stdout.split('\n')[:10]:  # Show first 10 files
                print(f"    {line}")
    
    return base_path

def generate_fix_commands(base_path):
    """Generate commands to fix the installation."""
    print("\n" + "=" * 60)
    print("Suggested Fix Commands")
    print("=" * 60)
    
    if base_path == "/opt/homebrew":  # M1/M2 Mac
        print("For M1/M2 Mac, run these commands:")
        print("""
# Create symbolic links for pgvector
sudo ln -sf /opt/homebrew/lib/postgresql@15/pgvector.so /opt/homebrew/opt/postgresql@15/lib/postgresql/pgvector.so
sudo ln -sf /opt/homebrew/share/postgresql@15/extension/vector.control /opt/homebrew/opt/postgresql@15/share/postgresql@15/extension/vector.control
sudo ln -sf /opt/homebrew/share/postgresql@15/extension/vector--*.sql /opt/homebrew/opt/postgresql@15/share/postgresql@15/extension/

# Restart PostgreSQL
brew services restart postgresql@15

# Enable the extension
psql -p 34532 -d finance-dev -U $(whoami) -c "CREATE EXTENSION IF NOT EXISTS vector;"
        """)
    else:  # Intel Mac
        print("For Intel Mac, run these commands:")
        print("""
# Create symbolic links for pgvector
sudo ln -sf /usr/local/lib/postgresql@15/pgvector.so /usr/local/opt/postgresql@15/lib/postgresql/pgvector.so
sudo ln -sf /usr/local/share/postgresql@15/extension/vector.control /usr/local/opt/postgresql@15/share/postgresql@15/extension/vector.control
sudo ln -sf /usr/local/share/postgresql@15/extension/vector--*.sql /usr/local/opt/postgresql@15/share/postgresql@15/extension/

# Restart PostgreSQL
brew services restart postgresql@15

# Enable the extension
psql -p 34532 -d finance-dev -U $(whoami) -c "CREATE EXTENSION IF NOT EXISTS vector;"
        """)
    
    print("\nAlternative: Install from source")
    print("""
# If the above doesn't work, install from source:
cd /tmp
git clone https://github.com/pgvector/pgvector.git
cd pgvector
make
sudo make install
cd ..
rm -rf pgvector

# Restart PostgreSQL
brew services restart postgresql@15

# Enable the extension
psql -p 34532 -d finance-dev -U $(whoami) -c "CREATE EXTENSION IF NOT EXISTS vector;"
    """)

def check_extension_availability():
    """Check if PostgreSQL can see the vector extension."""
    print("\n" + "=" * 60)
    print("Checking extension availability in PostgreSQL...")
    print("=" * 60)
    
    stdout, stderr, returncode = run_command(
        "psql -p 34532 -d postgres -U $(whoami) -t -c \"SELECT name, default_version FROM pg_available_extensions WHERE name = 'vector';\" 2>&1"
    )
    
    if returncode == 0:
        if "vector" in stdout:
            print("✓ PostgreSQL can see the vector extension!")
            print(f"  Details: {stdout}")
        else:
            print("❌ PostgreSQL cannot see the vector extension")
    else:
        print(f"❌ Error checking extensions: {stderr}")

def main():
    """Main diagnostic routine."""
    print("pgvector Installation Diagnostic Tool")
    print("=" * 60)
    
    # 1. Find PostgreSQL paths
    find_postgres_paths()
    
    # 2. Find pgvector installation
    base_path = find_pgvector_installation()
    
    # 3. Check if PostgreSQL can see the extension
    check_extension_availability()
    
    # 4. Generate fix commands if needed
    if base_path:
        generate_fix_commands(base_path)
    
    print("\n" + "=" * 60)
    print("Diagnostic complete!")
    print("=" * 60)
    print("\nIf the extension is still not available after running the fix commands,")
    print("please share the output of this diagnostic with the support team.")

if __name__ == "__main__":
    main()