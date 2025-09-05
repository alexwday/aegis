# Aegis - AI-Powered Financial Data Assistant

Aegis is an intelligent financial data assistant that helps users query and analyze banking data through natural language conversations. It uses advanced AI agents to understand, clarify, and respond to complex financial data requests.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [PostgreSQL Setup](#postgresql-setup)
- [Database Configuration](#database-configuration)
- [Environment Setup](#environment-setup)
- [Testing the Installation](#testing-the-installation)
- [Running the Application](#running-the-application)
- [Architecture Overview](#architecture-overview)

## Prerequisites

Before you begin, ensure you have the following installed:
- Python 3.11 or higher
- Git
- Homebrew (for macOS users)
- At least 4GB of available RAM
- An OpenAI API key (or compatible LLM API endpoint)

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/aegis.git
cd aegis
```

### 2. Create and Activate Python Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate
```

### 3. Install Python Dependencies

```bash
# Upgrade pip
pip install --upgrade pip

# Install the package in development mode
pip install -e .

# This installs all dependencies and makes 'aegis' importable
```

## PostgreSQL Setup

### Installing PostgreSQL on macOS

#### Option 1: Using Homebrew (Recommended)

```bash
# Install PostgreSQL
brew install postgresql@15

# Install pgvector extension for embeddings support
brew install pgvector

# Start PostgreSQL service
brew services start postgresql@15

# Add PostgreSQL to PATH (add to ~/.zshrc or ~/.bash_profile)
echo 'export PATH="/opt/homebrew/opt/postgresql@15/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

# Verify installation
psql --version
```

#### Option 2: Using PostgreSQL.app

1. Download PostgreSQL.app from https://postgresapp.com/
2. Move to Applications folder
3. Open PostgreSQL.app and click "Initialize"
4. Add to PATH: `sudo mkdir -p /etc/paths.d && echo /Applications/Postgres.app/Contents/Versions/latest/bin | sudo tee /etc/paths.d/postgresapp`

### Configure PostgreSQL for Custom Port (IMPORTANT: Do this first!)

We need PostgreSQL to run on port 34532 instead of the default 5432.

```bash
# Find your postgresql.conf location
# For Homebrew, try:
psql -d postgres -c "SHOW config_file;" 2>/dev/null || psql -p 5432 -d postgres -c "SHOW config_file;"

# If the above doesn't work, the config file is usually at:
# Homebrew (Intel Mac): /usr/local/var/postgresql@15/postgresql.conf
# Homebrew (M1/M2 Mac): /opt/homebrew/var/postgresql@15/postgresql.conf
# PostgreSQL.app: ~/Library/Application Support/Postgres/var-15/postgresql.conf
```

Edit the configuration file using one of these methods:

**Option 1: Using sed (easiest - automatic replacement):**
```bash
# For Homebrew on M1/M2 Macs:
sed -i '' 's/^#*port = 5432/port = 34532/' /opt/homebrew/var/postgresql@15/postgresql.conf

# For Homebrew on Intel Macs:
sed -i '' 's/^#*port = 5432/port = 34532/' /usr/local/var/postgresql@15/postgresql.conf
```

**Option 2: Using VS Code (if installed):**
```bash
# For M1/M2 Macs:
code /opt/homebrew/var/postgresql@15/postgresql.conf

# For Intel Macs:
code /usr/local/var/postgresql@15/postgresql.conf
```

**Option 3: Using TextEdit (GUI):**
```bash
# For M1/M2 Macs:
open -a TextEdit /opt/homebrew/var/postgresql@15/postgresql.conf

# For Intel Macs:
open -a TextEdit /usr/local/var/postgresql@15/postgresql.conf
```

**Option 4: Using nano (if you prefer):**
```bash
# For M1/M2 Macs:
nano /opt/homebrew/var/postgresql@15/postgresql.conf

# For Intel Macs:
nano /usr/local/var/postgresql@15/postgresql.conf
```

Find and change the port setting (usually around line 63):
```
port = 34532                # (change from 5432 to 34532)
```

Note: If the line starts with '#', remove the '#' to uncomment it.

Restart PostgreSQL:
```bash
# For Homebrew:
brew services restart postgresql@15

# For PostgreSQL.app:
# Click on the elephant icon in menu bar and select "Restart"
```

Verify the port change:
```bash
# This should now work:
psql -p 34532 -d postgres -c "SELECT 1;"
```

### Creating the Database and User

```bash
# Connect to PostgreSQL on the new port
# Note: Homebrew typically creates a superuser with your Mac username, not "postgres"
psql -p 34532 -d postgres

# If you get an error about role "postgres" not existing, try:
psql -p 34532 -d postgres -U $(whoami)
```

Run the following SQL commands in the PostgreSQL prompt:

```sql
-- First, check if the database already exists
\l

-- Create the database (if it doesn't exist)
CREATE DATABASE "finance-dev";

-- Create the user with password (if it doesn't exist)
CREATE USER financeuser WITH PASSWORD 'financepass123';

-- Grant all privileges on the database
GRANT ALL PRIVILEGES ON DATABASE "finance-dev" TO financeuser;

-- Connect to the new database
\c "finance-dev"

-- Grant schema permissions
GRANT ALL ON SCHEMA public TO financeuser;

-- Exit PostgreSQL
\q
```

Verify the setup:
```bash
# This should connect successfully:
PGPASSWORD=financepass123 psql -U financeuser -p 34532 -d finance-dev -c "SELECT 1;"
```

## Database Configuration

### Enable pgvector Extension

Before importing tables, enable the pgvector extension in your database:

```bash
# Connect as superuser (use your Mac username for Homebrew installations)
psql -p 34532 -d finance-dev -U $(whoami) -c "CREATE EXTENSION IF NOT EXISTS vector;"

# If you get a permission error, try:
psql -p 34532 -d postgres -U $(whoami) -c "CREATE EXTENSION IF NOT EXISTS vector;"
psql -p 34532 -d postgres -U $(whoami) -c "GRANT CREATE ON DATABASE \"finance-dev\" TO financeuser;"
```

### Import Required Tables

The project includes SQL dump files with the necessary table schemas and sample data. You can import them manually or use the setup script:

#### Option 1: Using Setup Script (Recommended)

```bash
# Activate virtual environment
source venv/bin/activate

# Create all tables (including aegis_transcripts)
python scripts/setup_all_databases.py --create-all

# Or create tables individually
python scripts/setup_all_databases.py --create-table aegis_data_availability
python scripts/setup_all_databases.py --create-table process_monitor_logs
python scripts/setup_all_databases.py --create-table aegis_transcripts

# Check status
python scripts/setup_all_databases.py --status
```

#### Option 2: Manual Import

```bash
# Import table schemas
PGPASSWORD=financepass123 psql -U financeuser -p 34532 -d finance-dev < data/process_monitor_logs_schema.sql
PGPASSWORD=financepass123 psql -U financeuser -p 34532 -d finance-dev < data/aegis_data_availability_schema.sql
PGPASSWORD=financepass123 psql -U financeuser -p 34532 -d finance-dev < data/aegis_transcripts_schema.sql

# Import sample data
PGPASSWORD=financepass123 psql -U financeuser -p 34532 -d finance-dev < data/process_monitor_logs_data.sql
PGPASSWORD=financepass123 psql -U financeuser -p 34532 -d finance-dev < data/aegis_data_availability_data.sql

# Verify the import
PGPASSWORD=financepass123 psql -U financeuser -p 34532 -d finance-dev -c "\dt"
PGPASSWORD=financepass123 psql -U financeuser -p 34532 -d finance-dev -c "SELECT COUNT(*) FROM process_monitor_logs;"
PGPASSWORD=financepass123 psql -U financeuser -p 34532 -d finance-dev -c "SELECT COUNT(*) FROM aegis_data_availability;"
PGPASSWORD=financepass123 psql -U financeuser -p 34532 -d finance-dev -c "SELECT COUNT(*) FROM aegis_transcripts;"
```

### Loading Transcript Data from CSV

If you have transcript embeddings in a CSV file:

```bash
# Load CSV data into aegis_transcripts table
python scripts/setup_aegis_transcripts.py --load-csv /path/to/your/transcripts.csv

# Verify the data
python scripts/setup_aegis_transcripts.py --verify
```

## Environment Setup

### 1. Create Environment File

Copy the example environment file and configure it:

```bash
cp .env.example .env
```

### 2. Configure Environment Variables

Edit the `.env` file with your settings:

```bash
nano .env
```

Update the following key variables:

```bash
# Core Configuration
LOG_LEVEL=INFO  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
ENVIRONMENT=local  # Options: local, dev, sai, prod

# LLM Configuration
API_KEY=your_openai_api_key_here
LLM_BASE_URL=https://api.openai.com/v1
AUTH_METHOD=api_key  # Options: api_key or oauth

# Model Configuration (3-tier system)
# Small Model - Fast, efficient for simple tasks
LLM_MODEL_SMALL=gpt-4.1-nano-2025-04-14
LLM_TEMPERATURE_SMALL=0.3
LLM_MAX_TOKENS_SMALL=1000

# Medium Model - Balanced performance for most tasks  
LLM_MODEL_MEDIUM=gpt-4.1-mini-2025-04-14
LLM_TEMPERATURE_MEDIUM=0.5
LLM_MAX_TOKENS_MEDIUM=2000

# Large Model - Most capable for complex reasoning
LLM_MODEL_LARGE=gpt-4.1-2025-04-14
LLM_TEMPERATURE_LARGE=0.7
LLM_MAX_TOKENS_LARGE=4000

# Embedding Model Configuration
LLM_EMBEDDING_MODEL=text-embedding-3-large
LLM_EMBEDDING_DIMENSIONS=3072  # Options: 256, 1024, or 3072

# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=34532
POSTGRES_DATABASE=finance-dev
POSTGRES_USER=financeuser
POSTGRES_PASSWORD=financepass123

# Conversation Processing
MAX_HISTORY_LENGTH=10
INCLUDE_SYSTEM_MESSAGES=false
ALLOWED_ROLES=user,assistant

# SSL Configuration
SSL_VERIFY=false
SSL_CERT_PATH=src/aegis/utils/ssl/rbc-ca-bundle.cer  # Required if SSL_VERIFY=true

# OAuth Configuration (if using AUTH_METHOD=oauth)
OAUTH_ENDPOINT=https://api.example.com/oauth/token
OAUTH_CLIENT_ID=your_client_id_here
OAUTH_CLIENT_SECRET=your_client_secret_here
OAUTH_GRANT_TYPE=client_credentials
```

**Note**: The complete `.env.example` file includes additional settings for timeouts, retries, and cost tracking. Copy it to `.env` and modify as needed.

## Testing the Installation

### 1. Test Database Connection

```bash
# Activate virtual environment if not already active
source venv/bin/activate

# Test database connection
python -c "
from aegis.connections.postgres_connector import get_connection
from sqlalchemy import text
try:
    with get_connection() as conn:
        result = conn.execute(text('SELECT 1'))
        print('✅ Database connection successful!')
except Exception as e:
    print(f'❌ Database connection failed: {e}')
"
```

### 2. Test LLM Connection

```bash
# Test LLM connectivity (replace with your actual API key)
API_KEY="your_api_key_here" python -c "
from aegis.connections.llm_connector import check_connection
from aegis.connections.oauth_connector import setup_authentication
from aegis.utils.ssl import setup_ssl

auth_config = setup_authentication()
ssl_config = setup_ssl()
context = {
    'execution_id': 'test-connection',
    'auth_config': auth_config,
    'ssl_config': ssl_config
}

result = check_connection(context)
print(f'✅ LLM connection: {result}')
"
```

### 3. Test the Model via Command Line

```bash
# Simple test query
python -c "
from aegis.model.main import model

messages = [{'role': 'user', 'content': 'Hello Aegis, can you help me understand RBC financial data?'}]

print('Sending query to Aegis...')
for chunk in model({'messages': messages}):
    if chunk['type'] == 'content':
        print(chunk['content'], end='')
print()
"
```

### 4. Run the Test Suite

```bash
# Run all tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ --cov=aegis --cov-report=term-missing
```

## Running the Application

### Option 1: Web Interface (Recommended)

```bash
# Activate virtual environment
source venv/bin/activate

# Start the web server
python run_web.py
```

Open your browser and navigate to:
- Local: http://localhost:8000
- Network: http://your-ip-address:8000

The web interface provides:
- Interactive chat with Aegis
- Message history
- Real-time streaming responses
- Debug information toggle
- Clean, modern UI
- Process monitoring dashboard (available at http://localhost:8000/monitoring)
- Database viewer (available at http://localhost:8000/database)

### Option 2: Command Line Interface

```bash
# Activate virtual environment
source venv/bin/activate

# Run interactive Python session
python

# In the Python interpreter:
from aegis.model.main import model

# Create a conversation
messages = [
    {'role': 'user', 'content': 'Show me RBC efficiency ratio for Q3 2024'}
]

# Get response
for chunk in model({'messages': messages}):
    if chunk['type'] == 'content':
        print(chunk['content'], end='')
```

### Option 3: Direct Script Execution

Create a file `test_aegis.py`:

```python
#!/usr/bin/env python
from aegis.model.main import model
import json

def test_query(query):
    messages = [{'role': 'user', 'content': query}]
    
    print(f"Query: {query}")
    print("Response: ", end="")
    
    for chunk in model({'messages': messages}):
        if chunk['type'] == 'content':
            print(chunk['content'], end='')
    print("\n" + "="*50 + "\n")

if __name__ == "__main__":
    # Test various queries
    test_query("Hello Aegis")
    test_query("What Canadian banks do you have data for?")
    test_query("Show me RBC's latest efficiency ratio")
```

Run it:
```bash
python test_aegis.py
```

## Architecture Overview

Aegis uses a multi-agent architecture to process financial data requests:

### Core Components

1. **Router Agent**: Analyzes incoming messages and determines the appropriate processing path
2. **Clarifier Agent**: Ensures queries have all necessary parameters (bank, metric, time period)
3. **Planner Agent**: Creates structured execution plans for data retrieval
4. **Research Subagents**: Specialized agents for different data sources
   - Benchmarking Agent (financial metrics and comparisons)
   - Pillar3 Agent (regulatory capital data)
   - Reports Agent (pre-generated analysis reports)
   - RTS (Real-Time Statistics) Agent (official regulatory filings)
   - Transcripts Agent (earnings call content)
5. **Response Agent**: Formats and presents data in a user-friendly manner
6. **Summarizer Agent**: Creates concise summaries of complex data

### Data Flow

1. User sends a natural language query
2. Router determines if the query needs data retrieval or can be answered directly
3. Clarifier ensures all required parameters are specified
4. Planner creates an execution plan with specific data sources
5. Subagents retrieve data from various sources
6. Response agent formats the final answer
7. Summarizer provides a concise summary if needed

### Key Features

- **Streaming Responses**: Real-time token streaming for better UX
- **Context Management**: Maintains conversation history for follow-up questions
- **Error Handling**: Graceful error recovery with helpful user messages
- **Monitoring**: Comprehensive logging and performance tracking with web dashboard
- **Database Viewer**: Built-in interface for exploring data and schemas
- **3-Tier LLM System**: Small/Medium/Large models optimized for different task complexities
- **OAuth & API Key Support**: Flexible authentication options
- **Cost Tracking**: Integrated token usage and cost monitoring
- **Scalability**: Modular architecture allows easy addition of new data sources

## Troubleshooting

### Common Issues

1. **PostgreSQL Connection Refused**
   - Ensure PostgreSQL is running: `brew services list`
   - Check port configuration: `psql -U financeuser -p 34532 -d finance-dev -c "SELECT 1;"`
   - Verify credentials in `.env` file

2. **Module Import Errors**
   - Ensure virtual environment is activated: `source venv/bin/activate`
   - Reinstall dependencies: `pip install -r requirements.txt`
   - Clear Python cache: `find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null`

3. **LLM API Errors**
   - Verify API key is set correctly in `.env`
   - Check API endpoint URL
   - Ensure you have sufficient API credits

4. **Web Interface Not Loading**
   - Check if port 8000 is available: `lsof -i :8000`
   - Try a different port by setting `SERVER_PORT` in `.env`
   - Check firewall settings

### Getting Help

- Check the logs in `logs/` directory for detailed error information
- Review the CLAUDE.md file for coding standards and architecture details
- Open an issue on GitHub with:
  - Error message and stack trace
  - Steps to reproduce
  - Environment details (OS, Python version, etc.)

## Development

### Code Quality

Before committing code, ensure it meets our standards:

```bash
# Always work in virtual environment
source venv/bin/activate

# Format code
black src/ --line-length 100

# Check style
flake8 src/ --max-line-length 100

# Run linter (must achieve 10.00/10)
pylint src/

# Run tests with coverage
python -m pytest tests/ --cov=aegis --cov-report=term-missing

# Clear Python cache if needed
find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null
rm -rf .pytest_cache/
```

### Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make your changes following the standards in CLAUDE.md
4. Run tests and ensure code quality checks pass
5. Commit with clear messages: `git commit -m "Add feature: description"`
6. Push to your fork: `git push origin feature-name`
7. Create a Pull Request

## License

[Your License Here]

## Project Structure

```
aegis/
├── src/aegis/              # Main package
│   ├── model/             # Core orchestration and agents
│   │   ├── main.py       # Main workflow orchestrator
│   │   ├── agents/       # Core decision agents
│   │   ├── subagents/    # Database-specific agents
│   │   └── prompts/      # YAML prompt templates
│   ├── connections/       # External service connectors
│   │   ├── llm_connector.py
│   │   ├── oauth_connector.py
│   │   └── postgres_connector.py
│   └── utils/            # Utility modules
│       ├── settings.py   # Configuration management
│       ├── logging.py    # Structured logging
│       ├── monitor.py    # Performance tracking
│       └── ssl.py        # SSL configuration
├── interfaces/            # Web interface modules
│   ├── web.py           # Flask web server
│   ├── monitoring.py    # Process monitoring dashboard
│   └── database.py      # Database viewer interface
├── templates/            # HTML templates
├── tests/               # Test suite
├── data/                # SQL schemas and sample data
├── docs/                # Documentation
└── performance_testing/ # Performance test suite
```

## Support

For questions or issues:
- Open an issue on GitHub
- Contact the development team
- Review the documentation in `docs/` and module-specific CLAUDE.md files