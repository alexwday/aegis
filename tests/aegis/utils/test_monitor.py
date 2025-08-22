"""
Tests for the simplified process monitoring setup.

Tests all functions in monitor_setup.py.
"""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest

from aegis.utils.monitor import (
    add_monitor_entry,
    clear_monitor_entries,
    create_stage_entry,
    format_llm_call,
    get_monitor_entries,
    initialize_monitor,
    post_monitor_entries,
)


@pytest.fixture
def mock_run_uuid():
    """Generate a test run UUID."""
    return str(uuid.uuid4())


@pytest.fixture(autouse=True)
def clear_monitor_after_test():
    """Clear monitor entries after each test."""
    yield
    clear_monitor_entries()


class TestInitialization:
    """Tests for monitor initialization."""

    def test_initialize_monitor(self, mock_run_uuid):
        """Test monitor initialization."""
        initialize_monitor(mock_run_uuid, "test_model")

        # Should start with empty entries
        entries = get_monitor_entries()
        assert entries == []

    def test_reinitialize_clears_entries(self, mock_run_uuid):
        """Test that reinitializing clears previous entries."""
        # First initialization with an entry
        initialize_monitor(mock_run_uuid, "model1")
        add_monitor_entry(
            stage_name="stage1",
            stage_start_time=datetime.now(timezone.utc),
        )

        # Reinitialize
        new_uuid = str(uuid.uuid4())
        initialize_monitor(new_uuid, "model2")

        # Should have cleared entries
        entries = get_monitor_entries()
        assert len(entries) == 0


class TestAddMonitorEntry:
    """Tests for adding monitor entries."""

    def test_add_entry_without_init(self):
        """Test adding entry without initialization logs warning."""
        # Clear any previous initialization
        clear_monitor_entries()
        initialize_monitor("", "")  # Reset globals

        add_monitor_entry(
            stage_name="test_stage",
            stage_start_time=datetime.now(timezone.utc),
        )

        # Should not add entry
        entries = get_monitor_entries()
        assert len(entries) == 0

    def test_add_basic_entry(self, mock_run_uuid):
        """Test adding a basic monitor entry."""
        initialize_monitor(mock_run_uuid, "test_model")

        start_time = datetime.now(timezone.utc)
        end_time = start_time + timedelta(seconds=1)

        add_monitor_entry(
            stage_name="test_stage",
            stage_start_time=start_time,
            stage_end_time=end_time,
            status="Success",
        )

        entries = get_monitor_entries()
        assert len(entries) == 1

        entry = entries[0]
        assert entry["run_uuid"] == mock_run_uuid
        assert entry["model_name"] == "test_model"
        assert entry["stage_name"] == "test_stage"
        assert entry["status"] == "Success"
        assert entry["duration_ms"] == 1000

    def test_add_entry_with_llm_calls(self, mock_run_uuid):
        """Test adding entry with LLM calls."""
        initialize_monitor(mock_run_uuid, "test_model")

        llm_calls = [
            {"model": "gpt-4", "total_tokens": 150, "cost": 0.003},
            {"model": "gpt-3.5", "total_tokens": 100, "cost": 0.001},
        ]

        add_monitor_entry(
            stage_name="llm_stage",
            stage_start_time=datetime.now(timezone.utc),
            llm_calls=llm_calls,
        )

        entries = get_monitor_entries()
        entry = entries[0]

        assert "llm_calls" in entry
        assert entry["total_tokens"] == 250
        assert entry["total_cost"] == Decimal("0.004")

        # Check llm_calls is a list (not JSON string)
        assert isinstance(entry["llm_calls"], list)
        assert len(entry["llm_calls"]) == 2

    def test_add_entry_with_error(self, mock_run_uuid):
        """Test adding entry with error information."""
        initialize_monitor(mock_run_uuid, "test_model")

        add_monitor_entry(
            stage_name="error_stage",
            stage_start_time=datetime.now(timezone.utc),
            status="Failure",
            error_message="Something went wrong",
            decision_details="Failed to process",
        )

        entries = get_monitor_entries()
        entry = entries[0]

        assert entry["status"] == "Failure"
        assert entry["error_message"] == "Something went wrong"
        assert entry["decision_details"] == "Failed to process"

    def test_add_entry_with_metadata(self, mock_run_uuid):
        """Test adding entry with custom metadata."""
        initialize_monitor(mock_run_uuid, "test_model")

        metadata = {"key1": "value1", "key2": 123}

        add_monitor_entry(
            stage_name="metadata_stage",
            stage_start_time=datetime.now(timezone.utc),
            user_id="user123",
            custom_metadata=metadata,
            notes="Test notes",
        )

        entries = get_monitor_entries()
        entry = entries[0]

        assert entry["user_id"] == "user123"
        assert entry["notes"] == "Test notes"
        assert "custom_metadata" in entry

        # Check metadata is a dict (not JSON string)
        assert isinstance(entry["custom_metadata"], dict)
        assert entry["custom_metadata"] == metadata

    def test_add_entry_auto_end_time(self, mock_run_uuid):
        """Test that end time defaults to now if not provided."""
        initialize_monitor(mock_run_uuid, "test_model")

        start_time = datetime.now(timezone.utc) - timedelta(seconds=2)

        add_monitor_entry(
            stage_name="auto_end_stage",
            stage_start_time=start_time,
            # No end_time provided
        )

        entries = get_monitor_entries()
        entry = entries[0]

        # Duration should be roughly 2000ms
        assert entry["duration_ms"] >= 1900
        assert entry["duration_ms"] <= 2100


class TestCreateStageEntry:
    """Tests for the create_stage_entry helper."""

    def test_create_stage_entry(self, mock_run_uuid):
        """Test creating a stage entry dict."""
        initialize_monitor(mock_run_uuid, "test_model")

        start_time = datetime.now(timezone.utc)
        end_time = start_time + timedelta(milliseconds=500)

        entry = create_stage_entry(
            stage_name="test_stage",
            start_time=start_time,
            end_time=end_time,
            status="Success",
            decision_details="Test decision",
            custom_field="custom_value",
        )

        assert entry["run_uuid"] == mock_run_uuid
        assert entry["model_name"] == "test_model"
        assert entry["stage_name"] == "test_stage"
        assert entry["duration_ms"] == 500
        assert entry["status"] == "Success"
        assert entry["decision_details"] == "Test decision"
        assert entry["custom_field"] == "custom_value"


class TestFormatLLMCall:
    """Tests for formatting LLM calls."""

    def test_format_llm_call_basic(self):
        """Test formatting a basic LLM call."""
        call = format_llm_call(
            model="gpt-4",
            prompt_tokens=100,
            completion_tokens=50,
            cost=0.003,
        )

        assert call["model"] == "gpt-4"
        assert call["prompt_tokens"] == 100
        assert call["completion_tokens"] == 50
        assert call["total_tokens"] == 150
        assert call["cost"] == 0.003
        assert "timestamp" in call

    def test_format_llm_call_with_duration(self):
        """Test formatting LLM call with duration."""
        call = format_llm_call(
            model="gpt-3.5",
            prompt_tokens=80,
            completion_tokens=40,
            cost=0.001,
            duration_ms=250,
        )

        assert call["total_tokens"] == 120
        assert call["duration_ms"] == 250


class TestPostMonitorEntries:
    """Tests for posting entries to database."""

    @patch("aegis.utils.monitor.insert_many")
    def test_post_entries_success(self, mock_insert, mock_run_uuid):
        """Test successful posting to database."""
        mock_insert.return_value = 3

        initialize_monitor(mock_run_uuid, "test_model")

        # Add multiple entries
        for i in range(3):
            add_monitor_entry(
                stage_name=f"stage_{i}",
                stage_start_time=datetime.now(timezone.utc),
                status="Success",
            )

        # Post to database
        count = post_monitor_entries("exec-123")

        assert count == 3
        mock_insert.assert_called_once_with(
            "process_monitor_logs",
            mock_insert.call_args[0][1],  # The entries list
            execution_id="exec-123",
        )

        # Entries should be cleared after posting
        entries = get_monitor_entries()
        assert len(entries) == 0

    @patch("aegis.utils.monitor.insert_many")
    def test_post_empty_entries(self, mock_insert, mock_run_uuid):
        """Test posting with no entries."""
        initialize_monitor(mock_run_uuid, "test_model")

        count = post_monitor_entries()

        assert count == 0
        mock_insert.assert_not_called()

    @patch("aegis.utils.monitor.insert_many")
    def test_post_entries_database_error(self, mock_insert, mock_run_uuid):
        """Test handling database error during posting."""
        mock_insert.side_effect = Exception("Database error")

        initialize_monitor(mock_run_uuid, "test_model")
        add_monitor_entry(
            stage_name="test_stage",
            stage_start_time=datetime.now(timezone.utc),
        )

        with pytest.raises(Exception, match="Database error"):
            post_monitor_entries()

        # Entries should NOT be cleared on error
        entries = get_monitor_entries()
        assert len(entries) == 1


class TestUtilityFunctions:
    """Tests for utility functions."""

    def test_get_monitor_entries(self, mock_run_uuid):
        """Test getting current entries."""
        initialize_monitor(mock_run_uuid, "test_model")

        # Add some entries
        add_monitor_entry(
            stage_name="stage1",
            stage_start_time=datetime.now(timezone.utc),
        )
        add_monitor_entry(
            stage_name="stage2",
            stage_start_time=datetime.now(timezone.utc),
        )

        entries = get_monitor_entries()
        assert len(entries) == 2

        # Should return a copy, not the original
        entries.clear()
        assert len(get_monitor_entries()) == 2

    def test_clear_monitor_entries(self, mock_run_uuid):
        """Test clearing monitor entries."""
        initialize_monitor(mock_run_uuid, "test_model")

        add_monitor_entry(
            stage_name="test_stage",
            stage_start_time=datetime.now(timezone.utc),
        )

        clear_monitor_entries()

        entries = get_monitor_entries()
        assert len(entries) == 0


class TestIntegrationScenarios:
    """Tests for integration scenarios."""

    def test_complete_workflow_monitoring(self, mock_run_uuid):
        """Test monitoring a complete workflow."""
        initialize_monitor(mock_run_uuid, "test_model")

        # Stage 1: Simple success
        add_monitor_entry(
            stage_name="Setup",
            stage_start_time=datetime.now(timezone.utc),
            status="Success",
        )

        # Stage 2: With LLM calls
        llm_calls = [
            format_llm_call("gpt-4", 100, 50, 0.003),
            format_llm_call("gpt-4", 150, 75, 0.00375),
        ]
        add_monitor_entry(
            stage_name="Processing",
            stage_start_time=datetime.now(timezone.utc),
            status="Success",
            llm_calls=llm_calls,
        )

        # Stage 3: Failure
        add_monitor_entry(
            stage_name="Validation",
            stage_start_time=datetime.now(timezone.utc),
            status="Failure",
            error_message="Validation failed",
        )

        entries = get_monitor_entries()
        assert len(entries) == 3

        # Check specific entries
        setup_entry = entries[0]
        assert setup_entry["stage_name"] == "Setup"
        assert setup_entry["status"] == "Success"

        processing_entry = entries[1]
        assert processing_entry["total_tokens"] == 375
        assert processing_entry["total_cost"] == Decimal("0.00675")

        validation_entry = entries[2]
        assert validation_entry["status"] == "Failure"
        assert validation_entry["error_message"] == "Validation failed"

    @patch("aegis.utils.monitor.insert_many")
    def test_workflow_with_posting(self, mock_insert, mock_run_uuid):
        """Test complete workflow with database posting."""
        mock_insert.return_value = 2

        # Initialize
        initialize_monitor(mock_run_uuid, "workflow_model")

        # Add entries during workflow
        start1 = datetime.now(timezone.utc)
        add_monitor_entry(
            stage_name="Stage1",
            stage_start_time=start1,
            stage_end_time=start1 + timedelta(milliseconds=100),
            status="Success",
        )

        start2 = datetime.now(timezone.utc)
        add_monitor_entry(
            stage_name="Stage2",
            stage_start_time=start2,
            stage_end_time=start2 + timedelta(milliseconds=200),
            status="Success",
            llm_calls=[format_llm_call("gpt-4", 100, 50, 0.003)],
        )

        # Post at workflow end
        count = post_monitor_entries(execution_id=mock_run_uuid)

        assert count == 2

        # Verify posted data
        call_args = mock_insert.call_args[0]
        posted_entries = call_args[1]

        assert len(posted_entries) == 2
        assert posted_entries[0]["stage_name"] == "Stage1"
        assert posted_entries[0]["duration_ms"] == 100
        assert posted_entries[1]["stage_name"] == "Stage2"
        assert posted_entries[1]["duration_ms"] == 200
        assert posted_entries[1]["total_tokens"] == 150
