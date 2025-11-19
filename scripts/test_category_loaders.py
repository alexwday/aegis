"""
Test script to verify all 3 ETL category loaders work with the new 6-column format.
"""

import sys
import uuid
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Import the category loading functions from each ETL
from aegis.etls.call_summary.main import load_categories_from_xlsx as load_call_summary_categories
from aegis.etls.key_themes.main import load_categories_from_xlsx as load_key_themes_categories
from aegis.etls.cm_readthrough.main import (
    load_outlook_categories,
    load_qa_market_volatility_regulatory_categories,
    load_qa_pipelines_activity_categories,
)


def test_call_summary():
    """Test Call Summary category loaders for both Canadian and US banks."""
    print("\n" + "="*80)
    print("TESTING CALL SUMMARY CATEGORY LOADERS")
    print("="*80)

    execution_id = str(uuid.uuid4())

    # Test Canadian banks
    print("\n--- Canadian Banks Categories ---")
    try:
        categories = load_call_summary_categories("canadian", execution_id)
        print(f"✓ Loaded {len(categories)} categories")
        print(f"✓ Sample category keys: {list(categories[0].keys())}")
        print(f"✓ Sample category: {categories[0]['category_name']}")
        print(f"  - transcript_sections: '{categories[0]['transcript_sections']}'")
        print(f"  - category_description: '{categories[0]['category_description'][:50]}...'")
        print(f"  - example_1: '{categories[0]['example_1']}'")
        print(f"  - example_2: '{categories[0]['example_2']}'")
        print(f"  - example_3: '{categories[0]['example_3']}'")
    except Exception as e:
        print(f"✗ ERROR: {e}")
        return False

    # Test US banks
    print("\n--- US Banks Categories ---")
    try:
        categories = load_call_summary_categories("us", execution_id)
        print(f"✓ Loaded {len(categories)} categories")
        print(f"✓ Sample category: {categories[0]['category_name']}")
    except Exception as e:
        print(f"✗ ERROR: {e}")
        return False

    return True


def test_key_themes():
    """Test Key Themes category loader."""
    print("\n" + "="*80)
    print("TESTING KEY THEMES CATEGORY LOADER")
    print("="*80)

    execution_id = str(uuid.uuid4())

    print("\n--- Key Themes Categories ---")
    try:
        categories = load_key_themes_categories(execution_id)
        print(f"✓ Loaded {len(categories)} categories")
        print(f"✓ Sample category keys: {list(categories[0].keys())}")
        print(f"✓ Sample category: {categories[0]['category_name']}")
        print(f"  - transcript_sections: '{categories[0]['transcript_sections']}'")
        print(f"  - category_description: '{categories[0]['category_description'][:50]}...'")
        print(f"  - example_1: '{categories[0]['example_1']}'")
        print(f"  - example_2: '{categories[0]['example_2']}'")
        print(f"  - example_3: '{categories[0]['example_3']}'")
    except Exception as e:
        print(f"✗ ERROR: {e}")
        return False

    return True


def test_cm_readthrough():
    """Test CM Readthrough category loaders (3 files)."""
    print("\n" + "="*80)
    print("TESTING CM READTHROUGH CATEGORY LOADERS")
    print("="*80)

    execution_id = str(uuid.uuid4())

    # Test outlook categories
    print("\n--- Outlook Categories ---")
    try:
        categories = load_outlook_categories(execution_id)
        print(f"✓ Loaded {len(categories)} categories")
        print(f"✓ Sample category keys: {list(categories[0].keys())}")
        print(f"✓ Sample category: {categories[0]['category_name']}")
        print(f"  - transcript_sections: '{categories[0]['transcript_sections']}'")
        print(f"  - category_description: '{categories[0]['category_description'][:50]}...'")
        print(f"  - example_1: '{categories[0]['example_1'][:50]}...'")
        print(f"  - example_2: '{categories[0]['example_2'][:50] if categories[0]['example_2'] else ''}...'")
        print(f"  - example_3: '{categories[0]['example_3'][:50] if categories[0]['example_3'] else ''}...'")
    except Exception as e:
        print(f"✗ ERROR: {e}")
        return False

    # Test market volatility/regulatory categories
    print("\n--- Market Volatility/Regulatory Categories ---")
    try:
        categories = load_qa_market_volatility_regulatory_categories(execution_id)
        print(f"✓ Loaded {len(categories)} categories")
        print(f"✓ Sample category: {categories[0]['category_name']}")
    except Exception as e:
        print(f"✗ ERROR: {e}")
        return False

    # Test pipelines/activity categories
    print("\n--- Pipelines/Activity Categories ---")
    try:
        categories = load_qa_pipelines_activity_categories(execution_id)
        print(f"✓ Loaded {len(categories)} categories")
        print(f"✓ Sample category: {categories[0]['category_name']}")
    except Exception as e:
        print(f"✗ ERROR: {e}")
        return False

    return True


def main():
    """Run all category loader tests."""
    print("\n" + "="*80)
    print("CATEGORY LOADER TESTING - New 6-column standardized format")
    print("="*80)

    results = {
        "Call Summary": test_call_summary(),
        "Key Themes": test_key_themes(),
        "CM Readthrough": test_cm_readthrough(),
    }

    print("\n" + "="*80)
    print("TEST RESULTS SUMMARY")
    print("="*80)

    all_passed = True
    for etl_name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{etl_name}: {status}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\n✓ ALL TESTS PASSED - Category loaders working correctly with new format!")
    else:
        print("\n✗ SOME TESTS FAILED - Please review errors above")
        sys.exit(1)


if __name__ == "__main__":
    main()
