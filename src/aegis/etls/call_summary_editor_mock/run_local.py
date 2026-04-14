"""
Local test runner for call_summary — bypasses NAS and OAuth.

Reads a local XML file directly and uses OPENAI_API_KEY from the environment.

Usage:
    python call_summary/run_local.py \\
        --xml call_summary/test_data/BMO-CA_Q1_2026_E1_7654321_1.xml \\
        --ticker BMO-CA \\
        --year 2026 --quarter Q1

Optional:
    --categories  path to .xlsx  (defaults to call_summary_categories.xlsx)
    --dev         limit to first 2 MD blocks + first 3 QA conversations
    --output-dir  where to write the HTML (default: call_summary/output)
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# ── Paths ────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
DEFAULT_CATEGORIES = SCRIPT_DIR / "call_summary_categories.xlsx"
DEFAULT_CONFIG = SCRIPT_DIR / "config.yaml"
DEFAULT_OUTPUT = SCRIPT_DIR / "output"

# ── Import processing functions from main script ─────────────
sys.path.insert(0, str(SCRIPT_DIR))
import main_call_summary as cs


def setup_local(config_path: str, api_key: str) -> None:
    """Populate main_call_summary globals without NAS or OAuth."""
    import yaml

    with open(config_path) as f:
        cs.config = yaml.safe_load(f)

    cs.llm_client = OpenAI(
        api_key=api_key,
        base_url=cs.config.get("llm", {}).get("base_url", "https://api.openai.com/v1"),
        timeout=cs.config.get("llm", {}).get("timeout", 120),
    )

    # No-op so process_bank doesn't try OAuth refresh
    cs.refresh_llm_auth = lambda: None


def main() -> None:
    parser = argparse.ArgumentParser(description="Run call_summary locally against a single XML file.")
    parser.add_argument("--xml",        required=True, help="Path to local FactSet transcript XML")
    parser.add_argument("--ticker",     required=True, help="Ticker e.g. BMO-CA")
    parser.add_argument("--year",       required=True, help="Fiscal year e.g. 2026")
    parser.add_argument("--quarter",    required=True, help="Fiscal quarter e.g. Q1")
    parser.add_argument("--company",    default="",    help="Company name (auto-detected from XML if omitted)")
    parser.add_argument("--categories", default=str(DEFAULT_CATEGORIES))
    parser.add_argument("--config",     default=str(DEFAULT_CONFIG))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--dev",        action="store_true", help="Limit to 2 MD blocks + 3 QA convs")
    args = parser.parse_args()

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY environment variable not set.")
        sys.exit(1)

    xml_path = Path(args.xml)
    if not xml_path.exists():
        print(f"ERROR: XML file not found: {xml_path}")
        sys.exit(1)

    categories_path = Path(args.categories)
    if not categories_path.exists():
        print(f"ERROR: Categories file not found: {categories_path}")
        print("  Pass --categories /path/to/file.xlsx")
        sys.exit(1)

    setup_local(args.config, api_key)

    fiscal_year    = args.year
    fiscal_quarter = args.quarter.upper()
    out_dir        = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cs.log_info(f"Call Summary LOCAL — {fiscal_quarter} {fiscal_year} | dev={args.dev}")
    cs.log_info(f"XML: {xml_path}")
    cs.log_info(f"Categories: {categories_path}")
    cs.log_info(f"Model: {cs.config.get('llm', {}).get('model', '?')}")

    categories = cs.load_categories(str(categories_path))

    xml_bytes = xml_path.read_bytes()
    parsed    = cs.parse_transcript_xml(xml_bytes)
    if parsed is None:
        print("ERROR: XML parse failed.")
        sys.exit(1)

    ticker       = args.ticker.upper()
    company_name = args.company or parsed.get("title", ticker)

    # Try to get a clean company name from the monitored_institutions.yaml
    if not args.company:
        try:
            institutions = cs.load_monitored_institutions()
            if ticker in institutions:
                company_name = institutions[ticker]["name"]
        except Exception:
            pass

    md_raw_blocks, qa_raw_blocks = cs.extract_raw_blocks(parsed, ticker)
    cs.log_info(f"{ticker}: {len(md_raw_blocks)} MD blocks, {len(qa_raw_blocks)} raw QA blocks")

    if args.dev:
        md_raw_blocks  = md_raw_blocks[:2]
        qa_raw_blocks  = qa_raw_blocks[:9]  # enough for ~3 conversations

    categories_text_md = cs.format_categories_for_prompt(categories, "MD")
    categories_text_qa = cs.format_categories_for_prompt(categories, "QA")

    # ── MD classification ────────────────────────────────────
    processed_md = []
    for i, blk in enumerate(md_raw_blocks):
        cs.log_info(f"  MD block {i+1}/{len(md_raw_blocks)}: {blk['id']}")
        try:
            processed_md.append(cs.classify_md_block(
                blk, categories, categories_text_md,
                company_name, fiscal_year, fiscal_quarter,
            ))
        except Exception as e:
            cs.log_error(f"  MD block {blk['id']} failed: {e}")
            traceback.print_exc()

    # ── QA boundary detection ────────────────────────────────
    qa_conversations_raw = cs.detect_qa_boundaries(qa_raw_blocks, categories_text_qa)
    cs.log_info(f"  QA: {len(qa_raw_blocks)} blocks → {len(qa_conversations_raw)} conversations")

    if args.dev:
        qa_conversations_raw = qa_conversations_raw[:3]

    # ── QA classification ────────────────────────────────────
    processed_qa = []
    for i, conv_blocks in enumerate(qa_conversations_raw):
        cs.log_info(f"  QA conv {i+1}/{len(qa_conversations_raw)}")
        try:
            processed_qa.append(cs.classify_qa_conversation(
                i + 1, conv_blocks, ticker, categories,
                categories_text_qa, company_name, fiscal_year, fiscal_quarter,
            ))
        except Exception as e:
            cs.log_error(f"  QA conv {i+1} failed: {e}")
            traceback.print_exc()

    banks_data = {
        ticker: {
            "ticker": ticker,
            "company_name": company_name,
            "transcript_title": parsed.get("title", f"{fiscal_quarter} {fiscal_year} Earnings Call"),
            "fiscal_year": fiscal_year,
            "fiscal_quarter": fiscal_quarter,
            "md_blocks": processed_md,
            "qa_conversations": processed_qa,
        }
    }

    min_imp      = float(cs.config.get("processing", {}).get("min_importance_score", 4.0))
    report_state = cs.build_report_state(banks_data, categories, fiscal_year, fiscal_quarter, min_imp)
    html_content = cs.generate_html(report_state, fiscal_year, fiscal_quarter)

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"call_summary_{ticker}_{fiscal_year}_{fiscal_quarter}_{ts}.html"
    out_path.write_text(html_content, encoding="utf-8")

    print(f"\n✓ Call Summary generated: {out_path}")
    print(f"  Ticker        : {ticker}")
    print(f"  MD blocks     : {len(processed_md)}")
    print(f"  QA convs      : {len(processed_qa)}")
    print(f"  Total LLM cost: ${cs.total_llm_cost:.4f}")
    print(f"  File size     : {out_path.stat().st_size / 1024:.0f} KB")


if __name__ == "__main__":
    main()
