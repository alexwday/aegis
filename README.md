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

# Install required packages
pip install -r requirements.txt
```

## PostgreSQL Setup

### Installing PostgreSQL on macOS

#### Option 1: Using Homebrew (Recommended)

```bash
# Install PostgreSQL
brew install postgresql@15

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

### Creating the Database and User

```bash
# Connect to PostgreSQL as the default superuser
psql -U postgres

# Or if using PostgreSQL.app or brew without postgres user:
psql -d postgres
```

Run the following SQL commands in the PostgreSQL prompt:

```sql
-- Create the database
CREATE DATABASE "finance-dev";

-- Create the user with password
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

### Configure PostgreSQL for Custom Port

Edit the PostgreSQL configuration to use port 34532:

```bash
# Find your postgresql.conf location
psql -U postgres -c "SHOW config_file;"

# Edit the configuration file (path may vary)
# For Homebrew installation:
nano /opt/homebrew/var/postgresql@15/postgresql.conf

# For PostgreSQL.app:
nano ~/Library/Application\ Support/Postgres/var-15/postgresql.conf
```

Find and change the port setting:
```
port = 34532
```

Restart PostgreSQL:
```bash
# For Homebrew:
brew services restart postgresql@15

# For PostgreSQL.app:
# Click on the elephant icon in menu bar and select "Restart"
```

## Database Configuration

### Import Required Tables

The project includes SQL dump files with the necessary table schemas and sample data.

```bash
# Import table schemas
PGPASSWORD=financepass123 psql -U financeuser -p 34532 -d finance-dev < data/process_monitor_logs_schema.sql
PGPASSWORD=financepass123 psql -U financeuser -p 34532 -d finance-dev < data/aegis_data_availability_schema.sql

# Import sample data
PGPASSWORD=financepass123 psql -U financeuser -p 34532 -d finance-dev < data/process_monitor_logs_data.sql
PGPASSWORD=financepass123 psql -U financeuser -p 34532 -d finance-dev < data/aegis_data_availability_data.sql

# Verify the import
PGPASSWORD=financepass123 psql -U financeuser -p 34532 -d finance-dev -c "\dt"
PGPASSWORD=financepass123 psql -U financeuser -p 34532 -d finance-dev -c "SELECT COUNT(*) FROM process_monitor_logs;"
PGPASSWORD=financepass123 psql -U financeuser -p 34532 -d finance-dev -c "SELECT COUNT(*) FROM aegis_data_availability;"
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
# LLM Configuration
API_KEY=your_openai_api_key_here
LLM_ENDPOINT=https://api.openai.com/v1
AUTH_METHOD=api_key

# Model Configuration
MODEL_SMALL=gpt-4-1106-preview
MODEL_MEDIUM=gpt-4-1106-preview
MODEL_LARGE=gpt-4-1106-preview

# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=34532
POSTGRES_DATABASE=finance-dev
POSTGRES_USER=financeuser
POSTGRES_PASSWORD=financepass123

# Application Settings
LOG_LEVEL=INFO
SSL_VERIFY=false
MAX_HISTORY_LENGTH=10
INCLUDE_SYSTEM_MESSAGES=false
ALLOWED_ROLES=user,assistant

# Server Configuration (for web interface)
SERVER_HOST=0.0.0.0
SERVER_PORT=8000
```

## Testing the Installation

### 1. Test Database Connection

```bash
# Activate virtual environment if not already active
source venv/bin/activate

# Test database connection
python -c "
from src.aegis.connections.postgres.connector import get_db_connection
try:
    with get_db_connection() as conn:
        result = conn.execute('SELECT 1')
        print('✅ Database connection successful!')
except Exception as e:
    print(f'❌ Database connection failed: {e}')
"
```

### 2. Test LLM Connection

```bash
# Test LLM connectivity
API_KEY="your_api_key_here" python -c "
from src.aegis.connections.llm.connector import check_connection
from src.aegis.connections.auth.connector import setup_authentication
from src.aegis.utils.ssl.setup import setup_ssl

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
from src.aegis.model.main import model

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
python -m pytest tests/ --cov=src/aegis --cov-report=term-missing
```

## Running the Application

### Option 1: Web Interface (Recommended)

```bash
# Activate virtual environment
source venv/bin/activate

# Start the web server
python web_interface.py
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

### Option 2: Command Line Interface

```bash
# Activate virtual environment
source venv/bin/activate

# Run interactive Python session
python

# In the Python interpreter:
from src.aegis.model.main import model

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
from src.aegis.model.main import model
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
   - Benchmarking Agent
   - Reports Agent
   - RTS (Real-Time Statistics) Agent
   - Transcripts Agent
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
- **Monitoring**: Comprehensive logging and performance tracking
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
# Format code
black src/ --line-length 100

# Check style
flake8 src/ --max-line-length 100

# Run linter
pylint src/

# Run tests
python -m pytest tests/
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

## Support

For questions or issues:
- Open an issue on GitHub
- Contact the development team
- Review the documentation in `docs/`