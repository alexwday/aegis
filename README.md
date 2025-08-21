# Aegis - Orchestrated Agent Workflow System

An intelligent workflow orchestration system for managing agent-based LLM interactions with enterprise-grade authentication and SSL support.

## Features

- 🔐 **Dual Authentication**: Supports both OAuth 2.0 and API key authentication
- 🔒 **SSL/TLS Support**: Enterprise SSL certificate verification
- 🎯 **Smart Conversation Processing**: Filters and manages conversation history
- 📊 **Structured Logging**: JSON-formatted logs with execution tracking
- 🧪 **Comprehensive Testing**: 99% test coverage with isolated test fixtures

## Setup

### 1. Clone the repository
```bash
git clone https://github.com/yourusername/aegis.git
cd aegis
```

### 2. Create virtual environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment
```bash
cp .env.example .env
# Edit .env with your configuration
```

### 5. Configuration Options

#### For Local Development (API Key)
```bash
AUTH_METHOD=api_key
API_KEY=your_actual_api_key_here
SSL_VERIFY=false
```

#### For Production (OAuth + SSL)
```bash
AUTH_METHOD=oauth
OAUTH_ENDPOINT=https://your-auth-server.com/oauth/token
OAUTH_CLIENT_ID=your_client_id
OAUTH_CLIENT_SECRET=your_client_secret
SSL_VERIFY=true
SSL_CERT_PATH=/path/to/your/certificate.cer
```

### 6. SSL Certificate Setup (if using SSL)
Place your `.cer` file in the configured path or update `SSL_CERT_PATH` in `.env`

## Running Tests

### Unit Tests
```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=src/aegis --cov-report=term-missing

# Run specific test file
pytest tests/aegis/connections/test_oauth.py -xvs
```

### Testing LLM Connector
To test the LLM connector with actual API calls:

```bash
# Activate virtual environment
source venv/bin/activate

# Ensure API_KEY is set in .env file
# Run the test script
python src/aegis/connections/llm/test_connector.py
```

This test script:
- Replicates the full workflow process (SSL → conversation processing → authentication)
- Tests actual LLM API connection with all model tiers (small/medium/large)
- Tests all LLM capabilities:
  - `check_connection` - Connection verification
  - `complete` - Standard completions (all 3 model tiers)
  - `stream` - Streaming responses
  - `complete_with_tools` - Function calling with tools
- Provides detailed success/failure feedback

## Project Structure

```
aegis/
├── src/
│   └── aegis/
│       ├── connections/     # OAuth and API connectors
│       ├── model/           # Workflow orchestration
│       └── utils/           # Logging, SSL, settings, conversation
├── tests/                   # Comprehensive test suite
├── .env.example            # Environment template
└── requirements.txt        # Python dependencies
```

## Development Workflow

1. **Local Development**: Use API key authentication for testing
2. **Production Deployment**: Configure OAuth and SSL certificates
3. **Test Isolation**: Tests are isolated from environment variables

## Environment Variables

See `.env.example` for all available configuration options.

## License

[Your License Here]