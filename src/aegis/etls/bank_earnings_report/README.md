# Bank Earnings Report Template

A Jinja2 HTML template for generating bank quarterly earnings reports. The template is data-driven - populate JSON files and render to produce the final HTML report.

## Quick Start

```bash
# Render template with sample data
python3 render_template.py

# Open the output
open rendered_report.html
```

## Project Structure

```
├── render_template.py      # Renders template + JSON → HTML
├── report_template.html    # Jinja2 template
├── rendered_report.html    # Generated output
└── sample_data/            # JSON data files (see schema below)
```

## JSON File Naming Convention

Files are named `{section}_{subsection}.json` and prefixed with section number for sort order:

```
0_header_params.json      → _0_header_params
0_header_dividend.json    → _0_header_dividend
1_keymetrics_*.json       → _1_keymetrics_*
...
```

The render script automatically prefixes filenames starting with numbers with `_` to create valid Jinja2 variable names.

---

## JSON Schemas by Section

### Section 0: Header

**`0_header_params.json`** - Report parameters (direct input, not from LLM)
```json
{
  "bank_name": "Bank of Canada (Example)",
  "fiscal_quarter": "Q3",
  "fiscal_year": "2025",
  "period_ending": "July 31",
  "currency": "CAD"
}
```

**`0_header_dividend.json`** - Dividend data (Source: Supp Pack)
```json
{
  "dividend": {
    "amount": "$1.10/share",
    "qoq": { "value": 4.8, "direction": "positive", "display": "▲ 4.8%" },
    "yoy": { "value": 10.0, "direction": "positive", "display": "▲ 10.0%" }
  }
}
```

---

### Section 1: Key Financial Metrics

**`1_keymetrics_overview.json`** - Overview narrative (Source: RTS)
```json
{
  "narrative": "A resilient quarter underpinned by..."
}
```

**`1_keymetrics_tiles.json`** - 6 metric tiles (Source: Supp Pack)
```json
{
  "source": "Supp Pack",
  "metrics": [
    {
      "label": "Total Revenue",
      "value": "$9,200 M",
      "qoq": { "value": 2.3, "direction": "positive", "display": "▲ 2.3%" },
      "yoy": { "value": 6.8, "direction": "positive", "display": "▲ 6.8%" }
    }
    // ... 5 more metrics (always 6 total, displayed in 2 rows of 3)
  ]
}
```

**`1_keymetrics_chart.json`** - 8-quarter trend chart (Source: Supp Pack)
```json
{
  "label": "Net Interest Margin - 8Q Trend",
  "unit": "%",
  "decimal_places": 2,
  "quarters": ["Q4'23", "Q1'24", "Q2'24", "Q3'24", "Q4'24", "Q1'25", "Q2'25", "Q3'25"],
  "values": [1.52, 1.54, 1.58, 1.64, 1.71, 1.76, 1.74, 1.68]
}
```
*Note: Chart is rendered dynamically via JavaScript. Can display any metric - just change label/unit/values.*

**`1_keymetrics_items.json`** - Items of note table (Source: RTS)
```json
{
  "source": "RTS",
  "entries": [
    {
      "description": "Restructuring charges for workforce optimization...",
      "impact": { "value": -165, "direction": "negative", "display": "▼ $165 M" },
      "segment": "Corporate",
      "timing": "One-time"
    }
    // ... variable length
  ]
}
```

---

### Section 2: Management Narrative

**`2_narrative.json`** - Mixed RTS paragraphs and transcript quotes (Source: RTS, Transcripts)
```json
{
  "entries": [
    {
      "type": "rts",
      "content": "The bank's diversified business model..."
    },
    {
      "type": "transcript",
      "content": "We are managing through this credit normalization...",
      "speaker": "John Smith",
      "title": "CEO"
    }
    // ... variable length, any order
  ]
}
```
*Note: `type` determines styling - "rts" = plain paragraph, "transcript" = indented quote with attribution*

---

### Section 3: Analyst Focus

**`3_analyst_focus.json`** - Q&A from earnings call (Source: Transcripts)
```json
{
  "source": "Transcripts",
  "entries": [
    {
      "theme": "NIM Outlook",
      "question": "Given deposit cost pressures, what's your NIM trajectory for H2?",
      "answer": "CFO expects NIM to stabilize at current levels..."
    }
    // ... variable length
  ]
}
```

---

### Section 4: Segment Performance

**`4_segments.json`** - Business segment metrics (Source: RTS, Supp Pack)
```json
{
  "entries": [
    {
      "name": "Canadian P&C",
      "description": "Strong volume growth in mortgages (+7%)...",
      "revenue": {
        "value": "$4,200 M",
        "qoq": { "direction": "positive", "display": "▲ 2.1%" },
        "yoy": { "direction": "positive", "display": "▲ 5.2%" }
      },
      "net_income": {
        "value": "$1,100 M",
        "qoq": { "direction": "positive", "display": "▲ 1.8%" },
        "yoy": { "direction": "positive", "display": "▲ 4.5%" }
      },
      "roe": {
        "value": "22.5%",
        "qoq": { "direction": "negative", "display": "▼ 20bps" },
        "yoy": { "direction": "positive", "display": "▲ 40bps" }
      }
    }
    // ... variable length (typically 4 segments)
  ]
}
```

---

### Section 5: Capital & Risk Metrics

**`5_capital_risk.json`** - Regulatory capital, RWA, liquidity (Source: Pillar 3)
```json
{
  "source": "Pillar 3",
  "regulatory_capital": [
    {
      "label": "CET1 Ratio",
      "min_requirement": "11.5%",  // null if no minimum
      "value": "13.2%",
      "qoq": { "direction": "positive", "display": "▲ 10bps" },
      "yoy": { "direction": "positive", "display": "▲ 30bps" }
    }
    // ... 4 ratios displayed in 2x2 grid
  ],
  "rwa": {
    "components": [
      { "label": "Credit", "value": "$420B", "percentage": 80.8, "color": "#005587" },
      { "label": "Market", "value": "$65B", "percentage": 12.5, "color": "#0ea5e9" },
      { "label": "Operational", "value": "$35B", "percentage": 6.7, "color": "#64748b" }
    ],
    "total": "$520B"
  },
  "liquidity_credit": [
    {
      "label": "LCR",
      "value": "132%",
      "qoq": { "direction": "positive", "display": "▲ 3%" },
      "yoy": { "direction": "positive", "display": "▲ 8%" }
    }
    // ... 4 metrics
  ]
}
```

---

## Delta Pattern

All QoQ/YoY changes follow this structure:
```json
{
  "value": 4.8,           // Raw numeric value (for calculations if needed)
  "direction": "positive", // "positive", "negative", or "neutral" - controls CSS color
  "display": "▲ 4.8%"     // Pre-formatted string with arrow for direct display
}
```

The `direction` field maps to CSS classes:
- `positive` → green (#4ade80)
- `negative` → red (#f87171)
- `neutral` → gray

---

## Data Sources

| Source | Sections |
|--------|----------|
| **Supp Pack** | Header dividend, Key metrics tiles, Trend chart, Segment metrics |
| **RTS** | Overview narrative, Items of note, Management narrative (partial), Segment descriptions |
| **Transcripts** | Management quotes, Analyst Q&A |
| **Pillar 3** | Capital & risk metrics |

---

## Hardcoded Elements

These labels are hardcoded in the template (not from JSON):
- Section headers: "OVERVIEW", "KEY METRICS", "ITEMS OF NOTE", "Management Narrative", "Analyst Focus", "Segment Performance", "Capital & Risk Metrics"
- Subsection headers: "Regulatory Capital", "RWA Composition", "Liquidity & Credit Quality"
- Column labels: "QoQ", "YoY", "Revenue", "Net Income", "ROE", "Description", "Impact", "Segment", "Timing"
- Pills: "Adjusted figures" (Sections 1 and 4)
- Some source pills in Sections 2 and 4

---

## Requirements

- Python 3.x
- Jinja2 (`pip install jinja2`)
