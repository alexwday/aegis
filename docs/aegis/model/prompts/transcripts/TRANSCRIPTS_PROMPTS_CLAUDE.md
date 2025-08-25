# Transcripts Prompts - Claude Context

## Module Purpose
The transcripts prompts module will contain YAML templates for analyzing earnings calls, investor meetings, and other verbal communications from financial institutions.

## Planned Prompts

### 1. Sentiment Analysis (`sentiment_analysis.yaml`)
**Purpose**: Evaluate tone and sentiment in communications
- Management confidence indicators
- Language pattern analysis
- Positive/negative sentiment scoring
- Uncertainty detection

### 2. Topic Extraction (`topic_extraction.yaml`)
**Purpose**: Identify key themes and topics
- Strategic initiative mentions
- Financial metric discussions
- Risk factor identification
- Forward guidance extraction

### 3. Q&A Analysis (`qa_analysis.yaml`)
**Purpose**: Analyze analyst Q&A sessions
- Question categorization
- Answer completeness assessment
- Deflection detection
- Follow-up identification

### 4. Guidance Tracking (`guidance_tracking.yaml`)
**Purpose**: Extract and track forward-looking statements
- Quantitative guidance
- Qualitative outlooks
- Timeline commitments
- Assumption dependencies

## Analysis Capabilities
- **Entity Recognition**: Identify mentioned companies, people, products
- **Metric Extraction**: Pull out specific numbers and KPIs
- **Comparison Detection**: Identify peer comparisons
- **Change Tracking**: Note changes from previous guidance

## Transcript Sources
- Quarterly earnings calls
- Investor day presentations
- Analyst meetings
- Conference presentations
- Media interviews

## Output Formats
- **Summary**: Key takeaways and highlights
- **Detailed**: Full thematic analysis
- **Comparative**: Period-over-period changes
- **Alerts**: Notable statements or changes

## Example Use Cases
- "What did RBC management say about digital transformation?"
- "Extract TD's guidance for 2025 from latest earnings call"
- "Compare BMO's Q3 tone with Q2 earnings call"
- "Identify risks mentioned in Scotia's investor day"