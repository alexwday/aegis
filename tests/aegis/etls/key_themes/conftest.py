"""Shared fixtures for key_themes ETL tests."""

import pytest


@pytest.fixture
def sample_category():
    """Single valid category dict (QA sections)."""
    return {
        "transcript_sections": "QA",
        "category_name": "Revenue Trends & Net Interest Income",
        "category_description": "NII, fee income, margin trends, and revenue mix.",
        "example_1": "Net interest income rose 5% QoQ to $5.2 BN.",
        "example_2": "NIM expanded by 15 bps to 1.65%.",
        "example_3": "",
    }


@pytest.fixture
def sample_categories():
    """Three categories spanning ALL, QA, and MD sections."""
    return [
        {
            "transcript_sections": "QA",
            "category_name": "Revenue Trends & Net Interest Income",
            "category_description": "NII, fee income, margin trends, and revenue mix.",
            "example_1": "Revenue grew 5%.",
            "example_2": "",
            "example_3": "",
        },
        {
            "transcript_sections": "QA",
            "category_name": "Credit Quality & Risk Outlook",
            "category_description": "PCL, NPL ratios, and credit trends.",
            "example_1": "",
            "example_2": "",
            "example_3": "",
        },
        {
            "transcript_sections": "QA",
            "category_name": "Forward Guidance & Outlook",
            "category_description": "Management outlook and guidance.",
            "example_1": "We expect 2025 to deliver strong growth.",
            "example_2": "Our target CET1 ratio remains 12%.",
            "example_3": "",
        },
    ]


@pytest.fixture
def sample_qa_block():
    """A single valid QABlock."""
    from aegis.etls.key_themes.main import QABlock

    block = QABlock(
        qa_id="qa_1",
        position=1,
        original_content=(
            "John Smith, Goldman Sachs: Can you give us some color on where you "
            "see margins heading given the rate environment?\n\n"
            "Jane Doe, CFO: So, on NIM, we're seeing it at around 1.65% for Q4, "
            "and we expect it to expand to approximately 1.70% to 1.75% by mid "
            "next year as deposit costs normalize."
        ),
    )
    block.is_valid = True
    block.category_name = "Revenue Trends & Net Interest Income"
    block.summary = (
        "Analyst asked about NIM outlook. CFO expects 1.65% Q4, expanding to 1.70-1.75%."
    )
    block.completion_status = "complete"
    return block


@pytest.fixture
def sample_qa_blocks():
    """Dictionary of QABlocks simulating a loaded qa_index."""
    from aegis.etls.key_themes.main import QABlock

    blocks = {}

    block1 = QABlock(qa_id="qa_1", position=1, original_content="Q&A about NIM")
    block1.is_valid = True
    block1.category_name = "Revenue Trends & Net Interest Income"
    block1.summary = "NIM discussion."
    block1.completion_status = "complete"
    blocks["qa_1"] = block1

    block2 = QABlock(qa_id="qa_2", position=2, original_content="Q&A about credit quality")
    block2.is_valid = True
    block2.category_name = "Credit Quality & Risk Outlook"
    block2.summary = "Credit quality discussion."
    block2.completion_status = "complete"
    blocks["qa_2"] = block2

    block3 = QABlock(qa_id="qa_3", position=3, original_content="Operator transition only")
    block3.is_valid = False
    block3.category_name = None
    block3.summary = None
    block3.completion_status = ""
    blocks["qa_3"] = block3

    return blocks


@pytest.fixture
def sample_theme_groups():
    """Sample ThemeGroup list for testing."""
    from aegis.etls.key_themes.main import ThemeGroup

    group1 = ThemeGroup(
        group_title="Net Interest Margin & Revenue Trends",
        qa_ids=["qa_1"],
        rationale="NIM and revenue discussion",
    )
    group2 = ThemeGroup(
        group_title="Credit Quality & Provisions",
        qa_ids=["qa_2"],
        rationale="Credit quality discussion",
    )
    return [group1, group2]


@pytest.fixture
def sample_etl_context():
    """Minimal ETL context dict for testing."""
    return {
        "execution_id": "test-exec-id-123",
        "auth_config": {"method": "api_key", "api_key": "test", "success": True},
        "ssl_config": {"verify": False},
        "bank_name": "Royal Bank of Canada",
        "bank_symbol": "RY",
        "quarter": "Q3",
        "fiscal_year": 2024,
    }
