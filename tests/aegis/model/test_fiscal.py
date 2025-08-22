"""
Tests for the fiscal period generator.
"""

from datetime import datetime
import pytest

import sys
from pathlib import Path

# Add the global folder to path to import fiscal
fiscal_path = Path(__file__).parent.parent.parent.parent / "src" / "aegis" / "model" / "prompts" / "global"
sys.path.insert(0, str(fiscal_path))
from fiscal import get_fiscal_statement, _get_quarter_dates
sys.path.remove(str(fiscal_path))


class TestGetQuarterDates:
    """Test the quarter date calculation helper."""

    def test_q1_dates(self):
        """Test Q1 (Nov-Jan) date calculation."""
        start, end = _get_quarter_dates(2025, 1)
        assert start == datetime(2024, 11, 1)
        assert end == datetime(2025, 1, 31)

    def test_q2_dates(self):
        """Test Q2 (Feb-Apr) date calculation."""
        start, end = _get_quarter_dates(2025, 2)
        assert start == datetime(2025, 2, 1)
        assert end == datetime(2025, 4, 30)

    def test_q3_dates(self):
        """Test Q3 (May-Jul) date calculation."""
        start, end = _get_quarter_dates(2025, 3)
        assert start == datetime(2025, 5, 1)
        assert end == datetime(2025, 7, 31)

    def test_q4_dates(self):
        """Test Q4 (Aug-Oct) date calculation."""
        start, end = _get_quarter_dates(2025, 4)
        assert start == datetime(2025, 8, 1)
        assert end == datetime(2025, 10, 31)


class TestGetFiscalStatement:
    """Test the fiscal statement generation."""

    def test_fiscal_year_calculation_q1(self):
        """Test fiscal year calculation for Q1 (Nov-Jan)."""
        test_date = datetime(2024, 11, 15)  # November 2024
        statement = get_fiscal_statement(test_date)

        assert "Current Fiscal Year: FY2025" in statement
        assert "Current Fiscal Quarter: FY2025 Q1" in statement

    def test_fiscal_year_calculation_q2(self):
        """Test fiscal year calculation for Q2 (Feb-Apr)."""
        test_date = datetime(2025, 2, 15)  # February 2025
        statement = get_fiscal_statement(test_date)

        assert "Current Fiscal Year: FY2025" in statement
        assert "Current Fiscal Quarter: FY2025 Q2" in statement

    def test_fiscal_year_calculation_q3(self):
        """Test fiscal year calculation for Q3 (May-Jul)."""
        test_date = datetime(2025, 5, 15)  # May 2025
        statement = get_fiscal_statement(test_date)

        assert "Current Fiscal Year: FY2025" in statement
        assert "Current Fiscal Quarter: FY2025 Q3" in statement

    def test_fiscal_year_calculation_q4(self):
        """Test fiscal year calculation for Q4 (Aug-Oct)."""
        test_date = datetime(2025, 8, 15)  # August 2025
        statement = get_fiscal_statement(test_date)

        assert "Current Fiscal Year: FY2025" in statement
        assert "Current Fiscal Quarter: FY2025 Q4" in statement

    def test_fiscal_year_boundary(self):
        """Test fiscal year boundary on November 1st."""
        # Last day of FY2024
        test_date = datetime(2024, 10, 31)
        statement = get_fiscal_statement(test_date)
        assert "Current Fiscal Year: FY2024" in statement
        assert "Current Fiscal Quarter: FY2024 Q4" in statement

        # First day of FY2025
        test_date = datetime(2024, 11, 1)
        statement = get_fiscal_statement(test_date)
        assert "Current Fiscal Year: FY2025" in statement
        assert "Current Fiscal Quarter: FY2025 Q1" in statement

    def test_statement_contains_all_sections(self):
        """Test that statement contains all required sections."""
        test_date = datetime(2025, 3, 15)
        statement = get_fiscal_statement(test_date)

        # Check main sections
        assert "Fiscal Period Context:" in statement
        assert "Today's Date:" in statement
        assert "Current Fiscal Year:" in statement
        assert "Current Fiscal Quarter:" in statement
        assert "Current Quarter:" in statement
        assert "Days Remaining:" in statement
        assert "Days Elapsed:" in statement
        assert "Fiscal Year Quarters:" in statement
        assert "Date Reference Guidelines:" in statement

        # Check all quarters are listed
        assert "Q1 (Nov-Jan):" in statement
        assert "Q2 (Feb-Apr):" in statement
        assert "Q3 (May-Jul):" in statement
        assert "Q4 (Aug-Oct):" in statement

        # Check guidelines
        assert "Year-to-date (YTD):" in statement
        assert "Quarter-to-date (QTD):" in statement
        assert "Prior year comparison:" in statement

    def test_days_calculation(self):
        """Test days remaining and elapsed calculations."""
        # Test at start of quarter
        test_date = datetime(2025, 2, 1)  # First day of Q2
        statement = get_fiscal_statement(test_date)
        assert "Days Elapsed: 1" in statement
        assert "Days Remaining: 89" in statement  # Feb has 28, Mar 31, Apr 30

        # Test at end of quarter
        test_date = datetime(2025, 4, 30)  # Last day of Q2
        statement = get_fiscal_statement(test_date)
        assert "Days Elapsed: 89" in statement
        assert "Days Remaining: 1" in statement

    def test_default_to_current_date(self):
        """Test that function defaults to current date when no date provided."""
        statement = get_fiscal_statement()

        # Should contain today's date
        today = datetime.now()
        today_str = today.strftime("%B %d, %Y")
        assert f"Today's Date: {today_str}" in statement

    def test_leap_year_handling(self):
        """Test correct handling of leap year in February."""
        # 2024 is a leap year
        test_date = datetime(2024, 2, 29)  # Leap day
        statement = get_fiscal_statement(test_date)

        assert "Current Fiscal Year: FY2024" in statement
        assert "Current Fiscal Quarter: FY2024 Q2" in statement
        # Should handle the date without errors

    def test_date_formatting(self):
        """Test that dates are formatted consistently."""
        test_date = datetime(2025, 7, 4)  # July 4, 2025
        statement = get_fiscal_statement(test_date)

        # Check date formats
        assert "Today's Date: July 04, 2025" in statement
        assert "Nov 01," in statement  # Start dates use abbreviated months
        assert "Oct 31," in statement  # End dates use abbreviated months

    def test_fiscal_year_references(self):
        """Test that fiscal year references are correct."""
        test_date = datetime(2025, 6, 15)  # FY2025 Q3
        statement = get_fiscal_statement(test_date)

        # Check current and prior year references
        assert "FY2025 (Nov 1, 2024 - Oct 31, 2025)" in statement
        assert "Prior year comparison: FY2024 (Nov 1, 2023 - Oct 31, 2024)" in statement