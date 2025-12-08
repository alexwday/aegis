# Prompt Naming Convention

## Format

```
{source}_{section}_{subsection}_prompt.md
```

Where:
- **source**: Data source (`rts`, `transcript`, `supplementary`, `combined`)
- **section**: Report section number and name (e.g., `1_keymetrics`, `2_narrative`)
- **subsection**: Specific extraction task (e.g., `overview`, `items`, `quotes`)

## Mapping: Old Names → New Names

| # | Old Name | New Name | Source | Section | Purpose |
|---|----------|----------|--------|---------|---------|
| 1 | `analyst_focus_extraction` | `transcript_3_analystfocus_extraction` | transcript | 3_analystfocus | Extract Q&A entries |
| 2 | `analyst_focus_ranking` | `transcript_3_analystfocus_ranking` | transcript | 3_analystfocus | Rank Q&A for featured |
| 3 | `key_metrics_selection` | `supplementary_1_keymetrics_selection` | supplementary | 1_keymetrics | Select tile/dynamic/chart metrics |
| 4 | `management_narrative_extraction` | `transcript_2_narrative_quotes` | transcript | 2_narrative | Extract management quotes |
| 5 | `transcript_overview_extraction` | `transcript_1_keymetrics_overview` | transcript | 1_keymetrics | Overview from transcript |
| 6 | `transcript_items_extraction` | `transcript_1_keymetrics_items` | transcript | 1_keymetrics | Items of note from transcript |
| 7 | `items_deduplication` | `combined_1_keymetrics_items_dedup` | combined | 1_keymetrics | Deduplicate items from both sources |
| 8 | `overview_combination` | `combined_1_keymetrics_overview` | combined | 1_keymetrics | Combine overviews from both sources |
| 9 | `narrative_combination` | `combined_2_narrative_interleave` | combined | 2_narrative | Place quotes between RTS paragraphs |
| 10 | `capital_risk_extraction` | `rts_5_capitalrisk_extraction` | rts | 5_capitalrisk | Extract capital/credit metrics |
| 11 | `segment_drivers_extraction` | `rts_4_segments_drivers` | rts | 4_segments | Extract segment drivers |
| 12 | `rts_items_extraction` | `rts_1_keymetrics_items` | rts | 1_keymetrics | Items of note from RTS |
| 13 | `rts_overview_extraction` | `rts_1_keymetrics_overview` | rts | 1_keymetrics | Overview from RTS |
| 14 | `rts_narrative_extraction` | `rts_2_narrative_paragraphs` | rts | 2_narrative | Extract 4 narrative paragraphs |

## Database Layer

All prompts use:
- `model`: "aegis"
- `layer`: "bank_earnings_report_etl"

## File Naming

Documentation files follow the pattern:
```
documentation/prompts/{new_name}_prompt.md
```
