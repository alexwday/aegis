"""
Test script to verify XML-formatted category output from all 3 ETLs.
"""

import sys
import uuid
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Import the formatting functions and category loaders
from aegis.etls.call_summary.main import (
    load_categories_from_xlsx as load_call_summary_categories,
    format_categories_for_prompt as format_call_summary,
)
from aegis.etls.key_themes.main import (
    load_categories_from_xlsx as load_key_themes_categories,
    format_categories_for_prompt as format_key_themes,
)
from aegis.etls.cm_readthrough.main import (
    load_outlook_categories,
    format_categories_for_prompt as format_cm_readthrough,
)


def test_call_summary_xml():
    """Test Call Summary XML formatting."""
    print("\n" + "="*80)
    print("CALL SUMMARY - XML FORMATTED CATEGORIES")
    print("="*80)

    execution_id = str(uuid.uuid4())

    try:
        categories = load_call_summary_categories("canadian", execution_id)
        xml_output = format_call_summary(categories[:2])  # Show first 2 categories

        print(f"\nLoaded {len(categories)} categories")
        print(f"\nXML Output (first 2 categories):\n")
        print(xml_output)

        # Verify XML structure
        assert "<category>" in xml_output
        assert "<name>" in xml_output
        assert "<section>" in xml_output
        assert "<description>" in xml_output
        assert "</category>" in xml_output

        print("\n✓ Call Summary XML formatting verified")
        return True

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        return False


def test_key_themes_xml():
    """Test Key Themes XML formatting."""
    print("\n" + "="*80)
    print("KEY THEMES - XML FORMATTED CATEGORIES")
    print("="*80)

    execution_id = str(uuid.uuid4())

    try:
        categories = load_key_themes_categories(execution_id)
        xml_output = format_key_themes(categories[:2])  # Show first 2 categories

        print(f"\nLoaded {len(categories)} categories")
        print(f"\nXML Output (first 2 categories):\n")
        print(xml_output)

        # Verify XML structure
        assert "<category>" in xml_output
        assert "<name>" in xml_output
        assert "<section>" in xml_output
        assert "<description>" in xml_output
        assert "</category>" in xml_output

        print("\n✓ Key Themes XML formatting verified")
        return True

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        return False


def test_cm_readthrough_xml():
    """Test CM Readthrough XML formatting."""
    print("\n" + "="*80)
    print("CM READTHROUGH - XML FORMATTED CATEGORIES")
    print("="*80)

    execution_id = str(uuid.uuid4())

    try:
        categories = load_outlook_categories(execution_id)
        xml_output = format_cm_readthrough(categories[:2])  # Show first 2 categories

        print(f"\nLoaded {len(categories)} categories")
        print(f"\nXML Output (first 2 categories):\n")
        print(xml_output)

        # Verify XML structure
        assert "<category>" in xml_output
        assert "<name>" in xml_output
        assert "<section>" in xml_output
        assert "<description>" in xml_output
        assert "</category>" in xml_output

        # CM Readthrough should have examples
        if "<examples>" in xml_output:
            print("\n✓ Examples included in output")
            assert "<example>" in xml_output
            assert "</example>" in xml_output
        else:
            print("\n⚠️  No examples in output (may be empty in Excel file)")

        print("\n✓ CM Readthrough XML formatting verified")
        return True

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        return False


def main():
    """Run all XML formatting tests."""
    print("\n" + "="*80)
    print("XML FORMATTING TESTS - Standardized category output across all ETLs")
    print("="*80)

    results = {
        "Call Summary": test_call_summary_xml(),
        "Key Themes": test_key_themes_xml(),
        "CM Readthrough": test_cm_readthrough_xml(),
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
        print("\n✓ ALL TESTS PASSED - XML formatting standardized across all ETLs!")
        print("\nStandardized XML Format:")
        print("  <category>")
        print("    <name>Category Name</name>")
        print("    <section>Section Description</section>")
        print("    <description>Category description text</description>")
        print("    <examples>")
        print("      <example>Example 1 text</example>")
        print("      <example>Example 2 text</example>")
        print("    </examples>")
        print("  </category>")
    else:
        print("\n✗ SOME TESTS FAILED - Please review errors above")
        sys.exit(1)


if __name__ == "__main__":
    main()
