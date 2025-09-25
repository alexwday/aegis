"""
Focused tests for clarifier.py to improve coverage.

Tests uncovered lines in the clarifier module.
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from sqlalchemy import text

from aegis.model.agents.clarifier import (
    load_banks_from_db,
    get_period_availability_from_db,
    create_bank_prompt,
    extract_banks,
    extract_periods,
    clarify_query,
)


class TestLoadBanksFromDb:
    """Test load_banks_from_db function edge cases."""

    @patch("aegis.model.agents.clarifier.fetch_all")
    @pytest.mark.asyncio
    async def test_load_banks_with_database_filter(self, mock_fetch_all):
        """Test filtering banks by available databases."""
        # Mock query results with banks in different databases
        mock_result = [
            # Bank 1 - in benchmarking and reports
            {"bank_id": 1, "bank_name": "Bank One", "bank_symbol": "B1", "bank_aliases": ["alias1"], "bank_tags": ["canadian_big_six"], "all_databases": ["benchmarking", "reports"]},
            # Bank 2 - only in transcripts
            {"bank_id": 2, "bank_name": "Bank Two", "bank_symbol": "B2", "bank_aliases": None, "bank_tags": ["us_bank"], "all_databases": ["transcripts"]},
            # Bank 3 - in benchmarking
            {"bank_id": 3, "bank_name": "Bank Three", "bank_symbol": "B3", "bank_aliases": [], "bank_tags": [], "all_databases": ["benchmarking"]},
        ]
        mock_fetch_all.return_value = mock_result

        # Test with database filter - only benchmarking
        result = await load_banks_from_db(available_databases=["benchmarking"])

        # Should only include banks 1 and 3
        assert len(result["banks"]) == 2

    @patch("aegis.model.agents.clarifier.fetch_all")
    @pytest.mark.asyncio
    async def test_load_banks_no_database_filter(self, mock_fetch_all):
        """Test loading banks without database filter (covers line 82)."""
        mock_result = [
            {"bank_id": 1, "bank_name": "Bank One", "bank_symbol": "B1", "bank_aliases": ["alias1"], "bank_tags": ["canadian_big_six"], "all_databases": ["benchmarking", "reports"]},
            {"bank_id": 2, "bank_name": "Bank Two", "bank_symbol": "B2", "bank_aliases": None, "bank_tags": ["us_bank"], "all_databases": ["transcripts"]},
        ]
        mock_fetch_all.return_value = mock_result

        # Test with no database filter (None)
        result = await load_banks_from_db(available_databases=None)

        # Should include all banks with full database list
        assert len(result["banks"]) == 2
        assert 1 in result["banks"]
        assert result["banks"][1]["databases"] == ["benchmarking", "reports"]  # All databases included
        assert 2 in result["banks"]
        assert result["banks"][2]["databases"] == ["transcripts"]

        # Check categories built correctly for unfiltered case
        assert "big_six" in result["categories"]
        assert result["categories"]["big_six"]["bank_ids"] == [1]
        assert "us_banks" in result["categories"]
        assert result["categories"]["us_banks"]["bank_ids"] == [2]

    @patch("aegis.model.agents.clarifier.fetch_all")
    @pytest.mark.asyncio

    async def test_load_banks_no_matching_databases(self, mock_fetch_all):
        """Test when no banks match the database filter."""

        # Banks only in reports database
        mock_result = [
            {"bank_id": 1, "bank_name": "Bank One", "bank_symbol": "B1", "bank_aliases": [], "bank_tags": [], "all_databases": ["reports"]},
            {"bank_id": 2, "bank_name": "Bank Two", "bank_symbol": "B2", "bank_aliases": [], "bank_tags": [], "all_databases": ["reports"]},
        ]
        mock_fetch_all.return_value = mock_result

        # Filter for transcripts only
        result = await load_banks_from_db(available_databases=["transcripts"])

        # No banks should match
        assert len(result["banks"]) == 0
        assert len(result["categories"]) == 0

    @patch("aegis.model.agents.clarifier.get_logger")
    @patch("aegis.model.agents.clarifier.fetch_all")
    @pytest.mark.asyncio

    async def test_load_banks_database_error(self, mock_fetch_all, mock_logger):
        """Test handling of database errors."""
        # Mock fetch_all to raise exception
        mock_fetch_all.side_effect = Exception("Database connection failed")

        result = await load_banks_from_db()

        # Should return empty structure on error
        assert result == {"banks": {}, "categories": {}}
        mock_logger.return_value.error.assert_called_once()


class TestGetPeriodAvailability:
    """Test get_period_availability_from_db function."""

    @patch("aegis.model.agents.clarifier.fetch_all")
    @pytest.mark.asyncio

    async def test_period_availability_with_bank_filter(self, mock_fetch_all):
        """Test filtering periods by bank IDs."""

        # Mock will be called with bank_ids filter, so only return bank 1 data
        mock_result = [
            {"bank_id": 1, "bank_name": "Bank One", "bank_symbol": "B1", "fiscal_year": 2024, "quarter": "Q3", "database_names": ["benchmarking", "reports"]},
            {"bank_id": 1, "bank_name": "Bank One", "bank_symbol": "B1", "fiscal_year": 2024, "quarter": "Q2", "database_names": ["benchmarking"]},
        ]
        mock_fetch_all.return_value = mock_result

        # Test with bank filter for bank 1 only
        result = await get_period_availability_from_db(bank_ids=[1])

        # Should only have bank 1 data
        assert "1" in result["availability"]
        assert "2" not in result["availability"]

        # Check structure
        assert result["availability"]["1"]["name"] == "Bank One"
        assert "benchmarking" in result["availability"]["1"]["databases"]
        assert 2024 in result["availability"]["1"]["databases"]["benchmarking"]
        assert "Q3" in result["availability"]["1"]["databases"]["benchmarking"][2024]

    @patch("aegis.model.agents.clarifier.fetch_all")
    @pytest.mark.asyncio

    async def test_period_availability_with_database_filter(self, mock_fetch_all):
        """Test filtering periods by available databases."""

        # Mock results with different databases
        mock_result = [
            {"bank_id": 1, "bank_name": "Bank One", "bank_symbol": "B1", "fiscal_year": 2024, "quarter": "Q3", "database_names": ["benchmarking", "reports"]},
            {"bank_id": 1, "bank_name": "Bank One", "bank_symbol": "B1", "fiscal_year": 2024, "quarter": "Q2", "database_names": ["transcripts"]},
        ]
        mock_fetch_all.return_value = mock_result

        # Filter for benchmarking only
        result = await get_period_availability_from_db(available_databases=["benchmarking"])

        # Should only have Q3 (which has benchmarking)
        assert "1" in result["availability"]
        db_data = result["availability"]["1"]["databases"]["benchmarking"]
        assert "Q3" in db_data[2024]
        assert "Q2" not in db_data.get(2024, [])  # Q2 only in transcripts

    @patch("aegis.model.agents.clarifier.fetch_all")
    @pytest.mark.asyncio

    async def test_period_availability_tracks_latest(self, mock_fetch_all):
        """Test tracking of latest reported period."""

        # Mock results with various periods
        mock_result = [
            {"bank_id": 1, "bank_name": "Bank One", "bank_symbol": "B1", "fiscal_year": 2023, "quarter": "Q4", "database_names": ["benchmarking"]},
            {"bank_id": 2, "bank_name": "Bank Two", "bank_symbol": "B2", "fiscal_year": 2024, "quarter": "Q2", "database_names": ["reports"]},
            {"bank_id": 3, "bank_name": "Bank Three", "bank_symbol": "B3", "fiscal_year": 2024, "quarter": "Q3", "database_names": ["transcripts"]},  # Latest
        ]
        mock_fetch_all.return_value = mock_result

        result = await get_period_availability_from_db()

        # Should track Q3 2024 as latest
        assert result["latest_reported"]["fiscal_year"] == 2024
        assert result["latest_reported"]["quarter"] == "Q3"

    @patch("aegis.model.agents.clarifier.get_logger")
    @patch("aegis.model.agents.clarifier.fetch_all")
    @pytest.mark.asyncio

    async def test_period_availability_database_error(self, mock_fetch_all, mock_logger):
        """Test handling of database errors."""
        # Mock fetch_all to raise exception
        mock_fetch_all.side_effect = Exception("Query failed")

        result = await get_period_availability_from_db()

        # Should return empty structure
        assert result == {"latest_reported": {}, "availability": {}}
        mock_logger.return_value.error.assert_called_once()


class TestCreateBankPrompt:
    """Test create_bank_prompt function."""

    def test_create_prompt_with_all_databases(self):
        """Test prompt creation with all databases."""
        banks_data = {
            "banks": {
                1: {
                    "name": "Bank One",
                    "symbol": "B1",
                    "aliases": ["First Bank"],
                    "tags": ["canadian_big_six"],
                    "databases": ["benchmarking", "reports"],
                }
            },
            "categories": {
                "big_six": {
                    "aliases": ["Big Six", "Canadian Big Six"],
                    "bank_ids": [1],
                }
            },
        }

        result = create_bank_prompt(banks_data, ["all"])

        # Check key elements
        assert "<available_banks>" in result
        assert "Based on all available databases" in result
        assert "1. Bank One (B1)" in result
        assert "Aliases: First Bank" in result
        assert "Tags: canadian_big_six" in result
        assert "Available in: benchmarking, reports" in result
        assert "Categories:" in result
        assert "big_six: banks [1]" in result

    def test_create_prompt_with_specific_databases(self):
        """Test prompt creation with specific database filter."""
        banks_data = {
            "banks": {
                1: {"name": "Bank One", "symbol": "B1", "databases": ["benchmarking"]}
            },
            "categories": {},
        }

        result = create_bank_prompt(banks_data, ["benchmarking", "reports"])

        assert "Based on the selected databases (benchmarking, reports)" in result


class TestExtractBanks:
    """Test extract_banks function."""

    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.load_banks_from_db")
    @patch("aegis.model.agents.clarifier.load_yaml")
    @pytest.mark.asyncio
    async def test_extract_banks_no_banks_available(
        self, mock_load_yaml, mock_load_banks, mock_complete
    ):
        """Test when no banks are available."""
        mock_load_banks.return_value = {"banks": {}}
        
        context = {"execution_id": "test-123"}
        result = await extract_banks("Show me bank data", context)

        assert result["status"] == "error"
        assert result["error"] == "No banks available in the system"
        # Should not call LLM if no banks
        mock_complete.assert_not_called()

    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.load_banks_from_db")
    @patch("aegis.model.agents.clarifier.load_yaml")
    @pytest.mark.asyncio
    async def test_extract_banks_empty_bank_ids(
        self, mock_load_yaml, mock_load_banks, mock_complete
    ):
        """Test when LLM returns empty bank_ids."""
        mock_load_banks.return_value = {
            "banks": {1: {"name": "Bank One", "symbol": "B1"}},
            "categories": {},
        }
        mock_load_yaml.return_value = {"content": "Extract banks"}

        # Mock LLM returning empty bank_ids
        mock_complete.return_value = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "name": "banks_found",
                                    "arguments": '{"bank_ids": [], "query_intent": "revenue"}',
                                }
                            }
                        ]
                    }
                }
            ],
            "metrics": {"total_cost": 0.01},
            "usage": {"total_tokens": 100},
        }

        context = {"execution_id": "test-123"}
        result = await extract_banks("Show me data", context)

        assert result["status"] == "needs_clarification"
        assert "Which banks would you like to query?" in result["clarification"]

    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.load_banks_from_db")
    @patch("aegis.model.agents.clarifier.load_yaml")
    @pytest.mark.asyncio
    async def test_extract_banks_invalid_ids(
        self, mock_load_yaml, mock_load_banks, mock_complete
    ):
        """Test when LLM returns invalid bank IDs."""
        mock_load_banks.return_value = {
            "banks": {1: {"name": "Bank One", "symbol": "B1"}},
            "categories": {},
        }
        mock_load_yaml.return_value = {"content": "Extract banks"}

        # Mock LLM returning invalid bank IDs
        mock_complete.return_value = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "name": "banks_found",
                                    "arguments": '{"bank_ids": [99, 100], "query_intent": ""}',
                                }
                            }
                        ]
                    }
                }
            ],
            "metrics": {"total_cost": 0.01},
            "usage": {"total_tokens": 100},
        }

        context = {"execution_id": "test-123"}
        result = await extract_banks("Show me bank 99", context)

        assert result["status"] == "needs_clarification"
        assert "No valid banks found" in result["clarification"]

    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.load_banks_from_db")
    @patch("aegis.model.agents.clarifier.load_yaml")
    @pytest.mark.asyncio
    async def test_extract_banks_with_intent_clarification(
        self, mock_load_yaml, mock_load_banks, mock_complete
    ):
        """Test when banks are found but intent is unclear."""
        mock_load_banks.return_value = {
            "banks": {
                1: {"name": "Bank One", "symbol": "B1"},
                2: {"name": "Bank Two", "symbol": "B2"},
            },
            "categories": {},
        }
        mock_load_yaml.return_value = {"content": "Extract banks"}

        # Mock LLM returning banks but no intent
        mock_complete.return_value = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "name": "banks_found",
                                    "arguments": '{"bank_ids": [1, 2], "query_intent": ""}',
                                }
                            }
                        ]
                    }
                }
            ],
            "metrics": {"total_cost": 0.01},
            "usage": {"total_tokens": 100},
        }

        context = {"execution_id": "test-123"}
        result = await extract_banks("Show me Bank One and Bank Two", context)

        assert result["status"] == "success"
        assert result["bank_ids"] == [1, 2]
        assert result["intent_clarification"] is not None
        assert "What would you like to see" in result["intent_clarification"]
        assert "Bank One" in result["intent_clarification"]

    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.load_banks_from_db")
    @patch("aegis.model.agents.clarifier.load_yaml")
    @pytest.mark.asyncio
    async def test_extract_banks_no_tool_response(
        self, mock_load_yaml, mock_load_banks, mock_complete
    ):
        """Test when LLM doesn't call any tools."""
        mock_load_banks.return_value = {
            "banks": {1: {"name": "Bank One", "symbol": "B1"}},
            "categories": {},
        }
        mock_load_yaml.return_value = {"content": "Extract banks"}

        # Mock LLM with no tool calls
        mock_complete.return_value = {
            "choices": [{"message": {"content": "I'm not sure"}}],
            "metrics": {"total_cost": 0.01},
            "usage": {"total_tokens": 100},
        }

        context = {"execution_id": "test-123"}
        result = await extract_banks("ambiguous query", context)

        assert result["status"] == "needs_clarification"
        assert "Please specify which banks" in result["clarification"]

    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.load_banks_from_db")
    @patch("aegis.model.agents.clarifier.load_yaml")
    @patch("aegis.model.agents.clarifier.get_logger")
    @pytest.mark.asyncio
    async def test_extract_banks_exception_handling(
        self, mock_logger, mock_load_yaml, mock_load_banks, mock_complete
    ):
        """Test exception handling in extract_banks."""
        mock_load_banks.return_value = {
            "banks": {1: {"name": "Bank One", "symbol": "B1"}},
            "categories": {},
        }
        mock_load_yaml.return_value = {"content": "Extract banks"}
        mock_complete.side_effect = Exception("LLM API error")

        context = {"execution_id": "test-123"}
        result = await extract_banks("Show me banks", context)

        assert result["status"] == "error"
        assert "LLM API error" in result["error"]
        mock_logger.return_value.error.assert_called_once()


class TestExtractPeriods:
    """Test extract_periods function."""

    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.get_period_availability_from_db")
    @patch("aegis.model.agents.clarifier.load_yaml")
    @patch("aegis.model.agents.clarifier._load_fiscal_prompt")
    @pytest.mark.asyncio
    async def test_extract_periods_without_banks(
        self, mock_fiscal, mock_load_yaml, mock_get_periods, mock_complete
    ):
        """Test period extraction when banks haven't been identified yet."""
        mock_fiscal.return_value = "Fiscal context"
        mock_load_yaml.return_value = {"content": "Extract periods"}
        mock_get_periods.return_value = {"latest_reported": {}, "availability": {}}

        # Mock LLM saying periods are clear
        mock_complete.return_value = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "name": "periods_valid",
                                    "arguments": '{"periods_clear": true}',
                                }
                            }
                        ]
                    }
                }
            ],
            "metrics": {"total_cost": 0.01},
            "usage": {"total_tokens": 100},
        }

        context = {"execution_id": "test-123"}
        result = await extract_periods("Q3 2024", bank_ids=None, context=context)

        assert result["status"] == "success"
        assert result["decision"] == "periods_clear"
        assert result["periods"] is None  # Will be extracted after banks

    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.get_period_availability_from_db")
    @patch("aegis.model.agents.clarifier.load_yaml")
    @patch("aegis.model.agents.clarifier._load_fiscal_prompt")
    @pytest.mark.asyncio
    async def test_extract_periods_specific_banks(
        self, mock_fiscal, mock_load_yaml, mock_get_periods, mock_complete
    ):
        """Test period extraction with bank-specific periods."""
        mock_fiscal.return_value = "Fiscal context"
        mock_load_yaml.return_value = {"content": "Extract periods"}
        mock_get_periods.return_value = {
            "latest_reported": {"fiscal_year": 2024, "quarter": "Q3"},
            "availability": {
                "1": {
                    "name": "Bank One",
                    "symbol": "B1",
                    "databases": {"benchmarking": {2024: ["Q1", "Q2", "Q3"]}},
                },
                "2": {
                    "name": "Bank Two",
                    "symbol": "B2",
                    "databases": {"reports": {2024: ["Q1", "Q2"]}},
                },
            },
        }

        # Mock LLM returning specific periods for different banks
        mock_complete.return_value = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "name": "periods_specific",
                                    "arguments": '{"bank_periods": [{"bank_id": 1, "fiscal_year": 2024, "quarters": ["Q3"]}, {"bank_id": 2, "fiscal_year": 2024, "quarters": ["Q2"]}]}',
                                }
                            }
                        ]
                    }
                }
            ],
            "metrics": {"total_cost": 0.01},
            "usage": {"total_tokens": 100},
        }

        context = {"execution_id": "test-123"}
        result = await extract_periods("Latest available", bank_ids=[1, 2], context=context)

        assert result["status"] == "success"
        assert result["decision"] == "periods_selected"
        assert "1" in result["periods"]
        assert result["periods"]["1"]["fiscal_year"] == 2024
        assert result["periods"]["1"]["quarters"] == ["Q3"]
        assert "2" in result["periods"]
        assert result["periods"]["2"]["quarters"] == ["Q2"]

    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.get_period_availability_from_db")
    @patch("aegis.model.agents.clarifier.load_yaml")
    @patch("aegis.model.agents.clarifier._load_fiscal_prompt")
    @patch("aegis.model.agents.clarifier.get_logger")
    @pytest.mark.asyncio
    async def test_extract_periods_exception_handling(
        self, mock_logger, mock_fiscal, mock_load_yaml, mock_get_periods, mock_complete
    ):
        """Test exception handling in extract_periods."""
        mock_fiscal.return_value = "Fiscal context"
        mock_load_yaml.return_value = {"content": "Extract periods"}
        mock_get_periods.side_effect = Exception("Database error")

        context = {"execution_id": "test-123"}
        result = await extract_periods("Q3 2024", bank_ids=[1], context=context)

        assert result["status"] == "error"
        assert "Database error" in result["error"]
        mock_logger.return_value.error.assert_called_once()


class TestClarifyQuery:
    """Test the main clarify_query orchestrator."""

    @patch("aegis.model.agents.clarifier.extract_periods")
    @patch("aegis.model.agents.clarifier.extract_banks")
    @pytest.mark.asyncio

    async def test_clarify_both_successful(self, mock_extract_banks, mock_extract_periods):
        """Test when both banks and periods are successfully extracted."""
        mock_extract_banks.return_value = {
            "status": "success",
            "bank_ids": [1, 2],
            "banks_detail": {
                1: {"name": "Bank One", "symbol": "B1"},
                2: {"name": "Bank Two", "symbol": "B2"},
            },
            "query_intent": "revenue",
            "tokens_used": 100,
            "cost": 0.01,
        }

        mock_extract_periods.return_value = {
            "status": "success",
            "decision": "periods_selected",
            "periods": {"apply_all": {"fiscal_year": 2024, "quarters": ["Q3"]}},
            "tokens_used": 50,
            "cost": 0.005,
        }

        context = {"execution_id": "test-123"}
        result = await clarify_query("Show me Q3 2024 revenue for banks", context)

        # Now returns list of combinations on success
        assert isinstance(result, list)
        assert len(result) == 2  # 2 banks x 1 quarter
        assert result[0]["bank_id"] == 1
        assert result[0]["bank_name"] == "Bank One"
        assert result[0]["bank_symbol"] == "B1"
        assert result[0]["fiscal_year"] == 2024
        assert result[0]["quarter"] == "Q3"
        assert result[0]["query_intent"] == "revenue"

    @patch("aegis.model.agents.clarifier.extract_periods")
    @patch("aegis.model.agents.clarifier.extract_banks")
    @pytest.mark.asyncio
    async def test_clarify_banks_need_clarification(
        self, mock_extract_banks, mock_extract_periods
    ):
        """Test when banks need clarification."""
        mock_extract_banks.return_value = {
            "status": "needs_clarification",
            "clarification": "Which banks do you mean?",
            "tokens_used": 100,
            "cost": 0.01,
        }

        mock_extract_periods.return_value = {
            "status": "needs_clarification",
            "clarification": "What time period?",
            "tokens_used": 50,
            "cost": 0.005,
        }

        context = {"execution_id": "test-123"}
        result = await clarify_query("Show me data", context)

        assert isinstance(result, dict)
        assert result["status"] == "needs_clarification"
        assert len(result["clarifications"]) == 2
        assert "Which banks do you mean?" in result["clarifications"]
        assert "What time period?" in result["clarifications"]

    @patch("aegis.model.agents.clarifier.extract_periods")
    @patch("aegis.model.agents.clarifier.extract_banks")
    @pytest.mark.asyncio
    async def test_clarify_intent_clarification_needed(
        self, mock_extract_banks, mock_extract_periods
    ):
        """Test when banks are found but intent needs clarification."""
        mock_extract_banks.return_value = {
            "status": "success",
            "bank_ids": [1],
            "banks_detail": {1: {"name": "Bank One", "symbol": "B1"}},
            "query_intent": "",
            "intent_clarification": "What would you like to see for Bank One?",
            "tokens_used": 100,
            "cost": 0.01,
        }

        mock_extract_periods.return_value = {
            "status": "success",
            "decision": "periods_selected",
            "periods": {"apply_all": {"fiscal_year": 2024, "quarters": ["Q3"]}},
            "tokens_used": 50,
            "cost": 0.005,
        }

        context = {"execution_id": "test-123"}
        result = await clarify_query("Bank One Q3 2024", context)

        assert isinstance(result, dict)
        assert result["status"] == "needs_clarification"
        assert len(result["clarifications"]) == 1
        assert "What would you like to see for Bank One?" in result["clarifications"]

    @patch("aegis.model.agents.clarifier.extract_periods")
    @patch("aegis.model.agents.clarifier.extract_banks")
    @pytest.mark.asyncio
    async def test_clarify_no_clarifications_returns_empty_list(
        self, mock_extract_banks, mock_extract_periods
    ):
        """Test fallback when no specific clarifications provided."""
        mock_extract_banks.return_value = {
            "status": "needs_clarification",
            "tokens_used": 100,
            "cost": 0.01,
        }

        mock_extract_periods.return_value = {
            "status": "success",
            "decision": "periods_clear",
            "tokens_used": 50,
            "cost": 0.005,
        }

        context = {"execution_id": "test-123"}
        result = await clarify_query("vague query", context)

        assert result["status"] == "needs_clarification"
        assert result["clarifications"] == ["Please provide more details about your query."]

    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.get_period_availability_from_db")
    @patch("aegis.model.agents.clarifier.load_yaml")
    @patch("aegis.model.agents.clarifier._load_fiscal_prompt")
    @patch("aegis.model.agents.clarifier.get_logger")
    @pytest.mark.asyncio
    async def test_extract_periods_clarification_needed(
        self, mock_logger, mock_fiscal, mock_load_yaml, mock_get_periods, mock_complete
    ):
        """Test extract_periods when clarification is needed (covers lines 919-933)."""
        mock_fiscal.return_value = "Fiscal context"
        mock_load_yaml.return_value = {"content": "Extract periods"}
        mock_get_periods.return_value = {
            "1": {
                "name": "Bank One",
                "symbol": "B1",
                "available_periods": [
                    {"fiscal_year": 2024, "quarters": ["Q1", "Q2", "Q3"]},
                    {"fiscal_year": 2023, "quarters": ["Q1", "Q2", "Q3", "Q4"]}
                ]
            }
        }

        # Mock LLM response with period_clarification function call
        mock_complete.return_value = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "name": "period_clarification",
                                    "arguments": '{"question": "Which fiscal year and quarter do you need?"}',
                                }
                            }
                        ]
                    }
                }
            ],
            "metrics": {"total_cost": 0.01},
            "usage": {"total_tokens": 100},
        }

        context = {"execution_id": "test-123"}
        result = await extract_periods("unclear period", bank_ids=[1], context=context)

        assert result["status"] == "needs_clarification"
        assert result["decision"] == "clarification_needed"
        assert result["clarification"] == "Which fiscal year and quarter do you need?"
        assert result["tokens_used"] == 100
        assert result["cost"] == 0.01

