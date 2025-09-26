# Aegis - AI-Powered Financial Data Assistant

Aegis is an intelligent financial data assistant that helps users query and analyze banking data through natural language conversations. It uses advanced AI agents to understand, clarify, and respond to complex financial data requests.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Environment Setup](#environment-setup)
- [Testing the Installation](#testing-the-installation)
- [Running the Application](#running-the-application)
- [Architecture Overview](#architecture-overview)

## Prerequisites

Before you begin, ensure you have the following:
- Python 3.11 or higher
- Git
- At least 4GB of available RAM
- An OpenAI API key (or compatible LLM API endpoint)
- Access credentials to the hosted PostgreSQL development server

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

## Database Configuration

The Aegis application connects to a hosted PostgreSQL development server. All required tables and data have been set up by the IT team.

### Available Tables

The following tables are available on the hosted server:
- `aegis_data_availability` - Bank and period coverage data
- `process_monitor_logs` - Workflow execution tracking
- `aegis_transcripts` - Earnings call transcripts with embeddings

Table schema files are preserved in the `data/` directory for reference:
- `data/aegis_data_availability_schema.sql`
- `data/process_monitor_logs_schema.sql`
- `data/aegis_transcripts_schema.sql`
- `data/aegis_tables_schema.sql`

### Database Status Check

To verify your connection and check table status:

```bash
# Activate virtual environment
source venv/bin/activate

# Check database status
python scripts/database_validator.py --status
```

This will display:
- Connection status to the hosted server
- List of available tables and row counts
- pgvector extension availability

### Loading Additional Data (Optional)

If you need to load transcript data from CSV files:

```bash
# Load CSV data into aegis_transcripts table
python scripts/database_validator.py --load-csv /path/to/your/transcripts.csv

# Options:
# --batch-size 100      # Process rows in batches
# --no-truncate         # Append instead of replacing existing data
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

# Database Configuration (Hosted PostgreSQL Server)
POSTGRES_HOST=your_host_here    # Provided by IT team
POSTGRES_PORT=5432               # Standard PostgreSQL port
POSTGRES_DATABASE=your_db_here  # Provided by IT team
POSTGRES_USER=your_user_here    # Provided by IT team
POSTGRES_PASSWORD=your_pass_here # Provided by IT team

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

### Option 1: FastAPI Web Interface (Recommended)

```bash
# Activate virtual environment
source venv/bin/activate

# Start the FastAPI server with WebSocket support
python run_fastapi.py
```

Open your browser and navigate to:
- Local: http://localhost:8000
- Network: http://your-ip-address:8000

The FastAPI interface provides:
- Interactive chat with Aegis
- Real-time WebSocket streaming responses
- Automatic reconnection
- Per-connection conversation state
- Message history
- Clean, modern UI
- Health check endpoint at /health

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

### Option 4: ETL Command Line Tools

Aegis includes ETL scripts for direct data extraction and report generation, bypassing the conversational AI flow for improved efficiency.

#### Call Summary ETL

The Call Summary ETL generates comprehensive Word documents with structured earnings call analysis, organized into 15 predefined categories.

##### Basic Usage

```bash
# Activate virtual environment
source venv/bin/activate

# Generate a call summary report (outputs to Word document)
python -m aegis.etls.call_summary.main \
  --bank "Royal Bank of Canada" \
  --year 2025 \
  --quarter Q2

# Using bank symbol instead of full name
python -m aegis.etls.call_summary.main \
  --bank RY \
  --year 2025 \
  --quarter Q2

# Using bank ID (1-7 for Canadian banks, 8-14 for US banks)
python -m aegis.etls.call_summary.main \
  --bank 1 \
  --year 2025 \
  --quarter Q2
```

##### Output Format

The ETL generates a Word document (.docx) saved to:
```
src/aegis/etls/call_summary/output/[BANK_SYMBOL]_[YEAR]_[QUARTER]_[ID].docx
```

Example: `RY_2025_Q2_1ce841f8.docx`

##### Document Structure

Each generated report includes:
- **Title Page**: Bank name, symbol, period
- **15 Analysis Categories** (for Canadian banks):
  1. Financial Performance & Metrics
  2. Revenue & Income Breakdown
  3. Expense Management & Efficiency
  4. Credit Quality & Risk Metrics
  5. Capital & Liquidity Position
  6. Business Segment Performance
  7. Strategic Initiatives & Growth Plans
  8. Economic & Market Outlook
  9. Regulatory & Compliance Updates
  10. Technology & Digital Innovation
  11. ESG & Sustainability
  12. Management Guidance & Outlook
  13. Analyst Q&A Key Themes
  14. Competitive Positioning
  15. Notable Quotes & Key Takeaways

Each category includes:
- Dynamic section titles based on content
- Key metric summaries with **bold** highlighting
- Direct quotes with speaker attribution in *italics*
- Bullet points with proper formatting (• for unordered lists)
- Numbered lists where appropriate

##### Testing the ETL

```bash
# Test with different banks
python -m aegis.etls.call_summary.main --bank TD --year 2025 --quarter Q2
python -m aegis.etls.call_summary.main --bank BMO --year 2025 --quarter Q1
python -m aegis.etls.call_summary.main --bank "Bank of America" --year 2025 --quarter Q2

# View available banks
python -c "
from aegis.connections.postgres_connector import get_connection
from sqlalchemy import text
with get_connection() as conn:
    result = conn.execute(text('SELECT DISTINCT bank_id, bank_name, bank_symbol FROM aegis_data_availability ORDER BY bank_id'))
    for row in result:
        print(f'{row.bank_id}: {row.bank_name} ({row.bank_symbol})')
"

# Check what periods are available for a specific bank
python -c "
from aegis.connections.postgres_connector import get_connection
from sqlalchemy import text
bank = 'RY'  # Change this to your bank symbol
with get_connection() as conn:
    result = conn.execute(text('''
        SELECT DISTINCT fiscal_year, quarter 
        FROM aegis_data_availability 
        WHERE bank_symbol = :bank 
        ORDER BY fiscal_year DESC, quarter DESC
    '''), {'bank': bank})
    print(f'Available periods for {bank}:')
    for row in result:
        print(f'  {row.fiscal_year} {row.quarter}')
"
```

##### Monitoring ETL Progress

The ETL logs detailed progress information:
- Stage 1: Research plan generation (analyzes full transcript)
- Stage 2: Category-by-category content extraction (15 iterations)
- Each category shows section source (MD, QA, or ALL)
- Token usage and costs are tracked per LLM call

##### ETL Benefits

- **80% LLM reduction**: Uses 16 targeted calls vs 80+ in conversational flow
- **Structured output**: Consistent Word document format
- **Category-based analysis**: 15 predefined sections ensure comprehensive coverage
- **Quote preservation**: Maintains speaker attribution and verbatim quotes
- **Markdown formatting**: Proper conversion to Word styles (headings, lists, bold, italic)
- **Batch processing**: Can be scripted for multiple banks/periods
- **Database storage**: Reports saved for future reference

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

1. **Database Connection Refused**
   - Verify credentials in `.env` file match those provided by IT team
   - Check network connectivity to the hosted PostgreSQL server
   - Ensure your IP is whitelisted on the database server (contact IT if needed)

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
│   ├── etls/             # ETL scripts for direct data access
│   │   └── call_summary/ # Call summary report generation
│   ├── connections/       # External service connectors
│   │   ├── llm_connector.py
│   │   ├── oauth_connector.py
│   │   └── postgres_connector.py
│   └── utils/            # Utility modules
│       ├── settings.py   # Configuration management
│       ├── logging.py    # Structured logging
│       ├── monitor.py    # Performance tracking
│       └── ssl.py        # SSL configuration
├── run_fastapi.py        # FastAPI server with WebSocket support
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