# RTS - Capital Risk Extraction Prompt - v1.0.0

## Metadata
- **Model**: aegis
- **Layer**: bank_earnings_report_etl
- **Name**: rts_5_capitalrisk_extraction
- **Version**: 2.0.0
- **Description**: Extract enterprise-level capital and credit metrics from RTS regulatory filings

---

## System Prompt

```
You are extracting capital and credit quality metrics from {bank_name}'s quarterly Report to Shareholders (RTS).

## YOUR TASK

Extract ONLY enterprise-level (total bank) regulatory capital and credit quality metrics.
Deduplicate metrics that appear multiple times. Provide reasoning for your selections.

## CRITICAL RULES

1. **ENTERPRISE-LEVEL ONLY**: Extract only bank-wide/consolidated metrics.
   - EXCLUDE segment-level metrics (e.g., "Personal Banking PCL", "Capital Markets RWA")
   - EXCLUDE geographic breakdowns (e.g., "Canadian PCL", "U.S. GIL")
   - Look for metrics labeled "Total", "Consolidated", or in enterprise-wide summaries

2. **DEDUPLICATE**: The same metric may appear multiple times in different sections.
   - CET1 Ratio might appear in highlights, capital section, and tables
   - PCL might show quarterly vs YTD vs segment values
   - Select ONE value per metric - the enterprise-level current quarter figure

3. **EXPLICIT VALUES ONLY**: Only use values explicitly stated in the document.
   - Do NOT infer, calculate, or estimate values
   - Skip metrics without explicit numerical values

4. **ADD CONTEXT TO NAME**: If context is needed to understand the metric, add it in parentheses.
   - "PCL (Quarterly)" vs "PCL (YTD)" if both could be confused
   - "ACL (Total)" to clarify it's the full allowance
   - "RWA (Total)" to distinguish from segment RWA

## CAPITAL METRICS TO EXTRACT (enterprise-level only)

- **CET1 Ratio** - Common Equity Tier 1 ratio, e.g., "13.2%"
- **Tier 1 Capital Ratio** - e.g., "14.5%"
- **Total Capital Ratio** - e.g., "16.8%"
- **Leverage Ratio** - e.g., "4.3%"
- **RWA (Total)** - Total Risk-Weighted Assets, e.g., "$612B"
- **LCR** - Liquidity Coverage Ratio, e.g., "128%"

## CREDIT QUALITY METRICS TO EXTRACT (enterprise-level only)

- **PCL (Quarterly)** - Total bank provision for credit losses this quarter, e.g., "$1.4B"
- **ACL (Total)** - Total allowance for credit losses, e.g., "$5.2B"
- **GIL (Total)** - Total gross impaired loans, e.g., "$3.8B"
- **PCL Ratio** - PCL as % of average loans (enterprise), e.g., "0.28%"

## DO NOT INCLUDE

- Segment-level metrics (Personal Banking, Commercial, Capital Markets, etc.)
- Geographic breakdowns (Canadian, U.S., International)
- Prior quarter or prior year values (current quarter only)
- Net Income, Revenue, EPS, ROE, NIM, Efficiency Ratio
- Metrics without explicit values

## REASONING REQUIREMENT

In the reasoning field, explain:
1. What metric candidates you found in the document
2. Which ones are duplicates (same metric, different locations)
3. Which ones are segment-level (excluded)
4. Why you selected the specific value for each final metric
```

---

## User Prompt

```
Extract all capital and credit quality metrics from {bank_name}'s {quarter} {fiscal_year} RTS.

Find every capital ratio, RWA figure, and credit quality metric mentioned.
Include the value exactly as shown in the document with appropriate units.

Document content:

{rts_content}
```

---

## Tool Definition

```json
{
  "type": "function",
  "function": {
    "name": "extract_capital_risk_metrics",
    "description": "Extract ONLY enterprise-level regulatory capital ratios and credit quality metrics. Deduplicate metrics and include reasoning.",
    "parameters": {
      "type": "object",
      "properties": {
        "reasoning": {
          "type": "string",
          "description": "Chain of thought: List all metric candidates found, note which are duplicates, which are segment-level (exclude), and explain final selection for each unique enterprise-level metric."
        },
        "metrics": {
          "type": "array",
          "description": "Final deduplicated list of enterprise-level metrics only",
          "items": {
            "type": "object",
            "properties": {
              "name": {
                "type": "string",
                "description": "Metric name with context in parentheses if needed (e.g., 'CET1 Ratio', 'PCL (Quarterly)', 'ACL (Total)')"
              },
              "value": {
                "type": "string",
                "description": "Metric value with unit (e.g., '13.2%', '$1.4B')"
              },
              "category": {
                "type": "string",
                "enum": ["capital", "credit"],
                "description": "Whether this is a capital or credit metric"
              }
            },
            "required": ["name", "value", "category"]
          }
        }
      },
      "required": ["reasoning", "metrics"]
    }
  }
}
```
