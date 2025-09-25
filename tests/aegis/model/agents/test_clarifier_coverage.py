"""
Additional tests to achieve 100% coverage for clarifier.py.
"""

import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from aegis.model.agents.clarifier import (
    extract_banks,
    extract_periods,
    clarify_query,
    _create_bank_period_combinations
)


class TestClarifierFullCoverage:
    """Tests for missing coverage lines in clarifier.py."""

    @pytest.fixture
    def mock_context(self):
        """Mock context for testing."""
        return {
            "execution_id": "test-exec-123",
            "auth_config": {"method": "api_key", "credentials": {"api_key": "test"}},
            "ssl_config": {"verify": False}
        }

    @pytest.fixture
    def mock_banks_from_db(self):
        """Mock banks from database."""
        return {
            1: {
                "bank_id": 1,
                "bank_name": "Royal Bank of Canada",
                "bank_symbol": "RY",
                "categories": ["big_six"]
            },
            2: {
                "bank_id": 2,
                "bank_name": "Bank of Nova Scotia",
                "bank_symbol": "BNS",
                "categories": ["big_six"]
            }
        }

    @patch("aegis.model.agents.clarifier.load_yaml")
    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.load_banks_from_db")
    @pytest.mark.asyncio
    async def test_extract_banks_with_message_history(
        self, mock_load_banks, mock_complete, mock_load_yaml, mock_context, mock_banks_from_db
    ):
        """Test extract_banks with conversation history (lines 335-336, 340)."""
        # Setup mocks
        mock_load_banks.return_value = mock_banks_from_db
        mock_load_yaml.return_value = {"content": "Test prompt"}

        # Mock complete_with_tools response
        mock_complete.return_value = {
            "tool_calls": [{
                "function": {
                    "name": "banks_selected",
                    "arguments": {
                        "bank_ids": [1],
                        "explanation": "Selected RBC",
                        "query_intent": "Revenue analysis for RBC"
                    }
                }
            }],
            "usage": {"total_tokens": 100},
            "metrics": {"total_cost": 0.002}
        }

        # Messages with history
        messages = [
            {"role": "user", "content": "Tell me about Canadian banks"},
            {"role": "assistant", "content": "Sure, what would you like to know?"},
            {"role": "user", "content": "Show me RBC revenue"}
        ]

        result = await extract_banks(
            query="Show me RBC revenue",
            context=mock_context,
            available_databases=["benchmarking"],
            messages=messages
        )

        # Verify messages were added to LLM call
        call_args = mock_complete.call_args
        llm_messages = call_args.kwargs["messages"] if call_args else []

        # Should have system + history messages + extraction request
        assert len(llm_messages) >= 4
        assert any("Tell me about Canadian banks" in msg["content"] for msg in llm_messages)
        assert result["status"] == "success"

    @patch("aegis.model.agents.clarifier.load_yaml")
    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.load_banks_from_db")
    @pytest.mark.asyncio
    async def test_extract_banks_with_small_model_tier(
        self, mock_load_banks, mock_complete, mock_load_yaml, mock_context, mock_banks_from_db
    ):
        """Test extract_banks with small model tier override (line 417)."""
        # Setup mocks
        mock_load_banks.return_value = mock_banks_from_db
        mock_load_yaml.return_value = {"content": "Test prompt"}

        mock_complete.return_value = {
            "tool_calls": [{
                "function": {
                    "name": "banks_selected",
                    "arguments": {
                        "bank_ids": [1],
                        "explanation": "Selected RBC",
                        "query_intent": "Test query"
                    }
                }
            }],
            "usage": {"total_tokens": 50},
            "metrics": {"total_cost": 0.001}
        }

        # Context with small model override
        context_with_small = {
            **mock_context,
            "model_tier_override": "small"
        }

        result = await extract_banks(
            query="RBC revenue",
            context=context_with_small,
            available_databases=["benchmarking"]
        )

        # Check that small model was requested
        call_args = mock_complete.call_args
        llm_params = call_args.kwargs["llm_params"] if call_args else {}

        from aegis.utils.settings import config
        assert llm_params["model"] == config.llm.small.model

    @patch("aegis.model.agents.clarifier.load_yaml")
    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.load_banks_from_db")
    @pytest.mark.asyncio
    async def test_extract_banks_with_medium_model_tier(
        self, mock_load_banks, mock_complete, mock_load_yaml, mock_context, mock_banks_from_db
    ):
        """Test extract_banks with medium model tier override (line 419)."""
        # Setup mocks
        mock_load_banks.return_value = mock_banks_from_db
        mock_load_yaml.return_value = {"content": "Test prompt"}

        mock_complete.return_value = {
            "tool_calls": [{
                "function": {
                    "name": "banks_selected",
                    "arguments": {
                        "bank_ids": [2],
                        "explanation": "Selected BNS",
                        "query_intent": "Test query"
                    }
                }
            }],
            "usage": {"total_tokens": 60},
            "metrics": {"total_cost": 0.0012}
        }

        # Context with medium model override
        context_with_medium = {
            **mock_context,
            "model_tier_override": "medium"
        }

        result = await extract_banks(
            query="BNS metrics",
            context=context_with_medium,
            available_databases=["rts"]
        )

        # Check that medium model was requested
        call_args = mock_complete.call_args
        llm_params = call_args.kwargs["llm_params"] if call_args else {}

        from aegis.utils.settings import config
        assert llm_params["model"] == config.llm.medium.model

    @patch("aegis.model.agents.clarifier.load_yaml")
    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.load_banks_from_db")
    @pytest.mark.asyncio
    async def test_extract_banks_intent_clarification_single(
        self, mock_load_banks, mock_complete, mock_load_yaml, mock_context, mock_banks_from_db
    ):
        """Test extract_banks intent clarification for single bank (line 493)."""
        # Setup mocks
        mock_load_banks.return_value = mock_banks_from_db
        mock_load_yaml.return_value = {"content": "Test prompt"}

        # Mock response with single bank but no intent
        mock_complete.return_value = {
            "tool_calls": [{
                "function": {
                    "name": "banks_selected",
                    "arguments": {
                        "bank_ids": [1],
                        "explanation": "Selected RBC",
                        "query_intent": ""  # Empty intent triggers clarification
                    }
                }
            }],
            "usage": {"total_tokens": 80},
            "metrics": {"total_cost": 0.0016}
        }

        result = await extract_banks(
            query="RBC",  # Vague query
            context=mock_context,
            available_databases=["benchmarking"]
        )

        # Should add intent clarification for single bank
        assert result["status"] == "success"
        assert "intent_clarification" in result
        assert "Royal Bank of Canada?" in result["intent_clarification"]

    @patch("aegis.model.agents.clarifier.load_yaml")
    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.load_banks_from_db")
    @pytest.mark.asyncio
    async def test_extract_banks_intent_clarification_multiple(
        self, mock_load_banks, mock_complete, mock_load_yaml, mock_context
    ):
        """Test extract_banks intent clarification for multiple banks (line 497)."""
        # Setup mocks with many banks
        many_banks = {
            i: {
                "bank_id": i,
                "bank_name": f"Bank {i}",
                "bank_symbol": f"B{i}",
                "categories": []
            }
            for i in range(1, 6)
        }
        mock_load_banks.return_value = many_banks
        mock_load_yaml.return_value = {"content": "Test prompt"}

        # Mock response with multiple banks but no intent
        mock_complete.return_value = {
            "tool_calls": [{
                "function": {
                    "name": "banks_selected",
                    "arguments": {
                        "bank_ids": [1, 2, 3, 4, 5],
                        "explanation": "Selected multiple banks",
                        "query_intent": ""  # Empty intent
                    }
                }
            }],
            "usage": {"total_tokens": 90},
            "metrics": {"total_cost": 0.0018}
        }

        result = await extract_banks(
            query="Banks",  # Vague query
            context=mock_context,
            available_databases=["benchmarking"]
        )

        # Should add intent clarification for multiple banks with truncation
        assert result["status"] == "success"
        assert "intent_clarification" in result
        assert "other banks?" in result["intent_clarification"]

    @patch("aegis.model.agents.clarifier.load_yaml")
    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.load_banks_from_db")
    @pytest.mark.asyncio
    async def test_extract_banks_with_clarification_needed(
        self, mock_load_banks, mock_complete, mock_load_yaml, mock_context, mock_banks_from_db
    ):
        """Test extract_banks with clarification_needed function call (lines 533-543)."""
        # Setup mocks
        mock_load_banks.return_value = mock_banks_from_db
        mock_load_yaml.return_value = {"content": "Test prompt"}

        # Mock response requesting clarification
        mock_complete.return_value = {
            "tool_calls": [{
                "function": {
                    "name": "clarification_needed",
                    "arguments": {
                        "question": "Which Canadian bank are you interested in?",
                        "possible_banks": ["Royal Bank of Canada", "Bank of Nova Scotia"]
                    }
                }
            }],
            "usage": {"total_tokens": 70},
            "metrics": {"total_cost": 0.0014}
        }

        result = await extract_banks(
            query="Show me the bank metrics",  # Ambiguous
            context=mock_context,
            available_databases=["benchmarking"]
        )

        # Should return clarification needed
        assert result["status"] == "needs_clarification"
        assert result["decision"] == "clarification_needed"
        assert "Which Canadian bank" in result["clarification"]
        assert result["possible_banks"] == ["Royal Bank of Canada", "Bank of Nova Scotia"]

    @patch("aegis.model.agents.clarifier.load_yaml")
    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.get_period_availability_from_db")
    @pytest.mark.asyncio
    async def test_extract_periods_with_message_history(
        self, mock_get_periods, mock_complete, mock_load_yaml, mock_context
    ):
        """Test extract_periods with conversation history (lines 684-685, 689)."""
        # Setup mocks
        mock_get_periods.return_value = {
            1: {
                "benchmarking": [{"fiscal_year": 2024, "quarter": "Q3"}]
            }
        }
        mock_load_yaml.return_value = {"content": "Test prompt"}

        mock_complete.return_value = {
            "tool_calls": [{
                "function": {
                    "name": "periods_for_bank",
                    "arguments": {
                        "bank_id": 1,
                        "fiscal_year": 2024,
                        "quarters": ["Q3"]
                    }
                }
            }],
            "usage": {"total_tokens": 60},
            "metrics": {"total_cost": 0.0012}
        }

        # Messages with history
        messages = [
            {"role": "user", "content": "What's the latest quarter?"},
            {"role": "assistant", "content": "Q3 2024 is the latest"},
            {"role": "user", "content": "Show me that data"}
        ]

        result = await extract_periods(
            query="Show me that data",
            bank_ids=[1],
            context=mock_context,
            available_databases=["benchmarking"],
            messages=messages
        )

        # Verify messages were added to LLM call
        call_args = mock_complete.call_args
        llm_messages = call_args.kwargs["messages"] if call_args else []

        # Should include conversation history
        assert any("What's the latest quarter?" in msg["content"] for msg in llm_messages)
        assert result["status"] == "success"

    @patch("aegis.model.agents.clarifier.load_yaml")
    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.get_period_availability_from_db")
    @pytest.mark.asyncio
    async def test_extract_periods_with_small_model(
        self, mock_get_periods, mock_complete, mock_load_yaml, mock_context
    ):
        """Test extract_periods with small model tier (line 817)."""
        # Setup mocks
        mock_get_periods.return_value = {
            1: {
                "rts": [{"fiscal_year": 2024, "quarter": "Q2"}]
            }
        }
        mock_load_yaml.return_value = {"content": "Test prompt"}

        mock_complete.return_value = {
            "tool_calls": [{
                "function": {
                    "name": "periods_for_bank",
                    "arguments": {
                        "bank_id": 1,
                        "fiscal_year": 2024,
                        "quarters": ["Q2"]
                    }
                }
            }],
            "usage": {"total_tokens": 40},
            "metrics": {"total_cost": 0.0008}
        }

        # Context with small model
        context_with_small = {
            **mock_context,
            "model_tier_override": "small"
        }

        result = await extract_periods(
            query="Q2 2024",
            bank_ids=[1],
            context=context_with_small,
            available_databases=["rts"]
        )

        # Check that small model was requested
        call_args = mock_complete.call_args
        llm_params = call_args.kwargs["llm_params"] if call_args else {}

        from aegis.utils.settings import config
        assert llm_params["model"] == config.llm.small.model

    @patch("aegis.model.agents.clarifier.load_yaml")
    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.get_period_availability_from_db")
    @pytest.mark.asyncio
    async def test_extract_periods_with_medium_model(
        self, mock_get_periods, mock_complete, mock_load_yaml, mock_context
    ):
        """Test extract_periods with medium model tier (line 819)."""
        # Setup mocks
        mock_get_periods.return_value = {
            2: {
                "transcripts": [{"fiscal_year": 2024, "quarter": "Q1"}]
            }
        }
        mock_load_yaml.return_value = {"content": "Test prompt"}

        mock_complete.return_value = {
            "tool_calls": [{
                "function": {
                    "name": "periods_for_bank",
                    "arguments": {
                        "bank_id": 2,
                        "fiscal_year": 2024,
                        "quarters": ["Q1"]
                    }
                }
            }],
            "usage": {"total_tokens": 45},
            "metrics": {"total_cost": 0.0009}
        }

        # Context with medium model
        context_with_medium = {
            **mock_context,
            "model_tier_override": "medium"
        }

        result = await extract_periods(
            query="Q1 2024",
            bank_ids=[2],
            context=context_with_medium,
            available_databases=["transcripts"]
        )

        # Check that medium model was requested
        call_args = mock_complete.call_args
        llm_params = call_args.kwargs["llm_params"] if call_args else {}

        from aegis.utils.settings import config
        assert llm_params["model"] == config.llm.medium.model

    @patch("aegis.model.agents.clarifier.load_yaml")
    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.get_period_availability_from_db")
    @pytest.mark.asyncio
    async def test_extract_periods_apply_all(
        self, mock_get_periods, mock_complete, mock_load_yaml, mock_context
    ):
        """Test extract_periods with apply_all function (lines 850-860)."""
        # Setup mocks
        mock_get_periods.return_value = {
            1: {
                "benchmarking": [
                    {"fiscal_year": 2024, "quarter": "Q1"},
                    {"fiscal_year": 2024, "quarter": "Q2"}
                ]
            },
            2: {
                "benchmarking": [
                    {"fiscal_year": 2024, "quarter": "Q1"},
                    {"fiscal_year": 2024, "quarter": "Q2"}
                ]
            }
        }
        mock_load_yaml.return_value = {"content": "Test prompt"}

        # Mock response with apply_all
        mock_complete.return_value = {
            "tool_calls": [{
                "function": {
                    "name": "apply_all",
                    "arguments": {
                        "fiscal_year": 2024,
                        "quarters": ["Q1", "Q2"]
                    }
                }
            }],
            "usage": {"total_tokens": 55},
            "metrics": {"total_cost": 0.0011}
        }

        result = await extract_periods(
            query="Q1 and Q2 2024 for all banks",
            bank_ids=[1, 2],
            context=mock_context,
            available_databases=["benchmarking"]
        )

        # Should return apply_all structure
        assert result["status"] == "success"
        assert result["decision"] == "periods_selected"
        assert "apply_all" in result["periods"]
        assert result["periods"]["apply_all"]["fiscal_year"] == 2024
        assert result["periods"]["apply_all"]["quarters"] == ["Q1", "Q2"]

    def test_create_bank_period_combinations_with_bank_specific(self):
        """Test _create_bank_period_combinations with bank-specific periods (lines 1011-1032)."""
        # Test data with bank-specific periods
        clarifier_results = {
            "banks": {
                "bank_ids": [1, 2],
                "banks_detail": {
                    1: {
                        "bank_id": 1,
                        "bank_name": "Royal Bank of Canada",
                        "bank_symbol": "RY"
                    },
                    2: {
                        "bank_id": 2,
                        "bank_name": "Bank of Nova Scotia",
                        "bank_symbol": "BNS"
                    }
                },
                "query_intent": "Compare revenue"
            },
            "periods": {
                "1": {  # Bank-specific period for bank_id 1
                    "fiscal_year": 2024,
                    "quarters": ["Q2", "Q3"]
                },
                "2": {  # Bank-specific period for bank_id 2
                    "fiscal_year": 2024,
                    "quarters": ["Q1", "Q2"]
                }
            }
        }

        # Transform to combinations
        combinations = _create_bank_period_combinations(clarifier_results)

        # Should have combinations for each bank's specific periods
        assert len(combinations) == 4  # RY: Q2,Q3 + BNS: Q1,Q2

        # Check RY combinations
        ry_combos = [c for c in combinations if c["bank_id"] == 1]
        assert len(ry_combos) == 2
        assert all(c["fiscal_year"] == 2024 for c in ry_combos)
        assert {c["quarter"] for c in ry_combos} == {"Q2", "Q3"}

        # Check BNS combinations
        bns_combos = [c for c in combinations if c["bank_id"] == 2]
        assert len(bns_combos) == 2
        assert all(c["fiscal_year"] == 2024 for c in bns_combos)
        assert {c["quarter"] for c in bns_combos} == {"Q1", "Q2"}

    @patch("aegis.model.agents.clarifier.load_yaml")
    @patch("aegis.model.agents.clarifier.complete_with_tools", new_callable=AsyncMock)
    @patch("aegis.model.agents.clarifier.load_banks_from_db")
    @pytest.mark.asyncio
    async def test_extract_banks_no_intent_clarification_with_good_intent(
        self, mock_load_banks, mock_complete, mock_load_yaml, mock_context, mock_banks_from_db
    ):
        """Test extract_banks with good intent doesn't add clarification (line 509)."""
        # Setup mocks
        mock_load_banks.return_value = mock_banks_from_db
        mock_load_yaml.return_value = {"content": "Test prompt"}

        # Mock response with banks and good intent
        mock_complete.return_value = {
            "tool_calls": [{
                "function": {
                    "name": "banks_selected",
                    "arguments": {
                        "bank_ids": [1, 2],
                        "explanation": "Selected banks",
                        "query_intent": "Compare revenue and efficiency ratios for RBC and BNS"
                    }
                }
            }],
            "usage": {"total_tokens": 85},
            "metrics": {"total_cost": 0.0017}
        }

        result = await extract_banks(
            query="Compare RBC and BNS revenue",
            context=mock_context,
            available_databases=["benchmarking"]
        )

        # Should NOT add intent clarification when intent is clear
        assert result["status"] == "success"
        assert "intent_clarification" not in result or result.get("intent_clarification") is None

    def test_create_bank_period_combinations_edge_cases(self):
        """Test _create_bank_period_combinations with edge cases (lines 911, 936, 1062, 1137, 1174)."""
        # Test with no periods at all
        clarifier_results = {
            "banks": {
                "bank_ids": [1],
                "banks_detail": {
                    1: {"bank_id": 1, "bank_name": "RBC", "bank_symbol": "RY"}
                },
                "query_intent": "Test"
            },
            "periods": {}  # Empty periods
        }

        combinations = _create_bank_period_combinations(clarifier_results)

        # Should return empty when no periods
        assert combinations == []

        # Test with apply_all but no banks
        clarifier_results_no_banks = {
            "banks": {
                "bank_ids": [],
                "banks_detail": {},
                "query_intent": "Test"
            },
            "periods": {
                "apply_all": {
                    "fiscal_year": 2024,
                    "quarters": ["Q1"]
                }
            }
        }

        combinations = _create_bank_period_combinations(clarifier_results_no_banks)
        assert combinations == []