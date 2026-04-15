# ETL Category Config — Schema Migration Mapping

## Call Summary → New Schema

Source files: `canadian_banks_categories.xlsx`, `us_banks_categories.xlsx`

| Old XLSX Column | New Schema Field | Notes |
|---|---|---|
| *(file name)* | `config_set` | `canadian_banks` or `us_banks` |
| *(implicit)* | `report_type` | `call_summary` (constant) |
| *(implicit)* | `version` | `1` (initial seed) |
| *(implicit)* | `is_active` | `TRUE` (initial seed) |
| *(row position)* | `display_order` | 1-based row index |
| `transcript_sections` | `transcript_sections` | unchanged |
| `report_section` | `report_section` | unchanged |
| `category_name` | `category_name` | unchanged |
| `category_description` | `category_description` | unchanged |
| `example_1` | `example_1` | unchanged |
| `example_2` | `example_2` | unchanged |
| `example_3` | `example_3` | unchanged |

## CM Readthrough → New Schema

Source files: `outlook_categories.xlsx`, `qa_pipelines_activity_categories.xlsx`, `qa_market_volatility_regulatory_categories.xlsx`

| Old XLSX Column | New Schema Field | Notes |
|---|---|---|
| *(file name)* | `config_set` | `outlook`, `qa_pipelines_activity`, or `qa_market_volatility_regulatory` |
| *(implicit)* | `report_type` | `cm_readthrough` (constant) |
| *(implicit)* | `version` | `1` (initial seed) |
| *(implicit)* | `is_active` | `TRUE` (initial seed) |
| *(row position)* | `display_order` | 1-based, dummy "Category/Description" header row dropped |
| `transcript_sections` | `transcript_sections` | unchanged |
| `category_group` | **`report_section`** | only present in `outlook_categories.xlsx`; `NULL` for the two QA files |
| `category_name` | `category_name` | unchanged |
| `category_description` | `category_description` | unchanged |
| `example_1` | `example_1` | unchanged |
| `example_2` | `example_2` | unchanged |
| `example_3` | `example_3` | unchanged |
