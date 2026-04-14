"""Shared fixtures for call_summary ETL tests."""

import pytest


@pytest.fixture
def sample_category():
    """Single valid category dict (ALL sections)."""
    return {
        "transcript_sections": "ALL",
        "report_section": "Results Summary",
        "category_name": "Revenue & Income Breakdown",
        "category_description": "Total revenue, net income, and key P&L line items.",
        "example_1": "Net interest income rose 5% QoQ to $5.2 BN.",
        "example_2": "Non-interest revenue declined 3%.",
        "example_3": "",
    }


@pytest.fixture
def sample_categories():
    """Three categories spanning ALL, QA, and MD sections."""
    return [
        {
            "transcript_sections": "ALL",
            "report_section": "Results Summary",
            "category_name": "Revenue & Income Breakdown",
            "category_description": "Total revenue and net income.",
            "example_1": "Revenue grew 5%.",
            "example_2": "",
            "example_3": "",
        },
        {
            "transcript_sections": "QA",
            "report_section": "Results Summary",
            "category_name": "Credit Quality",
            "category_description": "PCL, NPL ratios, and credit trends.",
            "example_1": "",
            "example_2": "",
            "example_3": "",
        },
        {
            "transcript_sections": "MD",
            "report_section": "Strategic Outlook",
            "category_name": "Forward Guidance",
            "category_description": "Management outlook and guidance.",
            "example_1": "We expect 2025 to deliver strong growth.",
            "example_2": "Our target CET1 ratio remains 12%.",
            "example_3": "",
        },
    ]


@pytest.fixture
def sample_extraction_result():
    """Non-rejected extraction result with statements and evidence."""
    return {
        "index": 1,
        "name": "Revenue & Income Breakdown",
        "title": "Revenue & Income Breakdown",
        "report_section": "Results Summary",
        "rejected": False,
        "summary_statements": [
            {
                "statement": "Net interest income rose **5%** QoQ to **$5.2 BN**.",
                "evidence": [
                    {
                        "content": "We saw strong NII growth of 5% this quarter.",
                        "type": "paraphrase",
                        "speaker": "CFO",
                    },
                    {
                        "content": "NII came in at $5.2 billion, up from $4.95 billion.",
                        "type": "quote",
                        "speaker": "CEO",
                    },
                ],
            },
            {
                "statement": "Non-interest revenue declined **3%** due to lower trading.",
                "evidence": [
                    {
                        "content": "Trading revenue was softer this quarter.",
                        "type": "paraphrase",
                        "speaker": "CFO",
                    },
                ],
            },
        ],
    }


@pytest.fixture
def sample_rejected_result():
    """Rejected extraction result."""
    return {
        "index": 2,
        "name": "Credit Quality",
        "report_section": "Results Summary",
        "rejected": True,
        "rejection_reason": "No substantive credit quality discussion found.",
    }


@pytest.fixture
def sample_etl_context():
    """Minimal ETL context dict for document generation."""
    return {
        "bank_info": {
            "bank_id": 1,
            "bank_name": "Royal Bank of Canada",
            "bank_symbol": "RY",
            "bank_type": "Canadian_Banks",
        },
        "quarter": "Q3",
        "fiscal_year": 2024,
        "execution_id": "test-exec-id-123",
    }
