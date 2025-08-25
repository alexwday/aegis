# RTS (Real-Time Systems) Prompts - Claude Context

## Module Purpose
The RTS prompts module will contain YAML templates for querying real-time financial systems and market data. These prompts enable live data retrieval and monitoring capabilities.

## Planned Prompts

### 1. Market Data (`market_data.yaml`)
**Purpose**: Query real-time market information
- Price feeds
- Volume tracking
- Volatility indicators
- Market depth analysis

### 2. Risk Monitoring (`risk_monitoring.yaml`)
**Purpose**: Real-time risk metric queries
- Exposure calculations
- Limit monitoring
- Alert thresholds
- Breach notifications

### 3. Trading Activity (`trading_activity.yaml`)
**Purpose**: Monitor trading operations
- Position tracking
- Order flow analysis
- Execution metrics
- P&L calculations

### 4. System Health (`system_health.yaml`)
**Purpose**: Monitor system performance
- Latency metrics
- Throughput monitoring
- Error rates
- Capacity utilization

## Real-Time Features
- **Streaming Queries**: Continuous data updates
- **Alert Generation**: Threshold-based notifications
- **Snapshot Capture**: Point-in-time data retrieval
- **Delta Calculations**: Change detection

## Data Sources
- Trading platforms
- Market data providers
- Risk management systems
- Internal monitoring tools

## Update Frequencies
- **Tick-by-tick**: Market prices
- **Second**: Trading volumes
- **Minute**: Risk metrics
- **Hourly**: Aggregated statistics

## Example Use Cases
- "Show current trading volume for RBC stock"
- "Monitor capital adequacy ratio in real-time"
- "Alert when risk exposure exceeds threshold"
- "Track intraday P&L performance"