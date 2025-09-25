"""
Test coverage for aegis.etls.key_themes.main utility functions and classes.

These tests target the simple classes and functions in the key themes ETL script
to get initial coverage of the ETL modules.
"""

import pytest
from unittest.mock import patch, MagicMock

from aegis.etls.key_themes.main import QABlock, ThemeGroup, apply_grouping_to_index


class TestQABlock:
    """Test QABlock class."""

    def test_qa_block_initialization(self):
        """Test QABlock class initialization."""
        block = QABlock(
            qa_id="qa_1",
            position=1,
            original_content="Q: What is your revenue? A: Our revenue was $1B."
        )

        assert block.qa_id == "qa_1"
        assert block.position == 1
        assert block.original_content == "Q: What is your revenue? A: Our revenue was $1B."
        assert block.theme_title is None
        assert block.summary is None
        assert block.key_metrics == []
        assert block.formatted_content is None
        assert block.assigned_group is None


class TestThemeGroup:
    """Test ThemeGroup class."""

    def test_theme_group_initialization(self):
        """Test ThemeGroup class initialization."""
        group = ThemeGroup(
            group_title="Revenue Discussion",
            qa_ids=["qa_1", "qa_2", "qa_3"],
            rationale="All blocks discuss revenue topics"
        )

        assert group.group_title == "Revenue Discussion"
        assert group.qa_ids == ["qa_1", "qa_2", "qa_3"]
        assert group.rationale == "All blocks discuss revenue topics"
        assert group.qa_blocks == []

    def test_theme_group_initialization_minimal(self):
        """Test ThemeGroup class initialization with minimal parameters."""
        group = ThemeGroup(
            group_title="Simple Group",
            qa_ids=["qa_1"]
        )

        assert group.group_title == "Simple Group"
        assert group.qa_ids == ["qa_1"]
        assert group.rationale == ""  # Default empty rationale
        assert group.qa_blocks == []


class TestApplyGroupingToIndex:
    """Test apply_grouping_to_index function."""

    @patch('aegis.etls.key_themes.main.logger')
    def test_apply_grouping_to_index_basic(self, mock_logger):
        """Test basic grouping application."""
        # Create test QA blocks
        qa_index = {
            "qa_1": QABlock("qa_1", 1, "Revenue question and answer"),
            "qa_2": QABlock("qa_2", 2, "Profit question and answer"),
            "qa_3": QABlock("qa_3", 3, "Operations question and answer")
        }

        # Create test theme groups
        theme_groups = [
            ThemeGroup("Financial Metrics", ["qa_1", "qa_2"], "Finance related"),
            ThemeGroup("Operations", ["qa_3"], "Operations related")
        ]

        # Apply grouping (function modifies in-place)
        apply_grouping_to_index(qa_index, theme_groups)

        # Check that blocks were assigned to groups
        assert qa_index["qa_1"].assigned_group == theme_groups[0]
        assert qa_index["qa_2"].assigned_group == theme_groups[0]
        assert qa_index["qa_3"].assigned_group == theme_groups[1]

        # Check that groups have blocks assigned
        assert len(theme_groups[0].qa_blocks) == 2
        assert qa_index["qa_1"] in theme_groups[0].qa_blocks
        assert qa_index["qa_2"] in theme_groups[0].qa_blocks

        assert len(theme_groups[1].qa_blocks) == 1
        assert qa_index["qa_3"] in theme_groups[1].qa_blocks

        # Check logging was called
        mock_logger.info.assert_called()

    @patch('aegis.etls.key_themes.main.logger')
    def test_apply_grouping_to_index_empty_groups(self, mock_logger):
        """Test grouping with empty theme groups."""
        qa_index = {"qa_1": QABlock("qa_1", 1, "Content")}
        theme_groups = []

        apply_grouping_to_index(qa_index, theme_groups)

        # No groups, so no assignments should be made
        assert qa_index["qa_1"].assigned_group is None

    @patch('aegis.etls.key_themes.main.logger')
    def test_apply_grouping_to_index_missing_qa_blocks(self, mock_logger):
        """Test grouping when some QA block IDs don't exist in index."""
        qa_index = {"qa_1": QABlock("qa_1", 1, "Content")}  # Only qa_1 exists
        theme_groups = [
            ThemeGroup("Test Group", ["qa_1", "qa_999"], "Test group with missing block")  # qa_999 doesn't exist
        ]

        apply_grouping_to_index(qa_index, theme_groups)

        # Should only assign the existing block
        assert qa_index["qa_1"].assigned_group == theme_groups[0]
        assert len(theme_groups[0].qa_blocks) == 1
        assert qa_index["qa_1"] in theme_groups[0].qa_blocks

        # Should log warning about missing block
        mock_logger.warning.assert_called_with("Q&A ID qa_999 not found in index")