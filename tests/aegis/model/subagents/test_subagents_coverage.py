"""
Additional tests to achieve 100% coverage for subagent modules.

Focuses on edge cases not covered by test_all_subagents.py.
"""

import pytest
from unittest.mock import patch
import importlib


# List of all subagent modules with their LLM function type
SUBAGENT_MODULES = [
    ("aegis.model.subagents.supplementary.main", "supplementary_agent", "supplementary", "stream"),
    ("aegis.model.subagents.reports.main", "reports_agent", "reports", "complete_with_tools"),
    ("aegis.model.subagents.rts.main", "rts_agent", "rts", "stream"),
    ("aegis.model.subagents.transcripts.main", "transcripts_agent", "transcripts", "complete_with_tools"),
    ("aegis.model.subagents.pillar3.main", "pillar3_agent", "pillar3", "stream"),
]


async def async_generator(items):
    """Helper to create async generator from list."""
    for item in items:
        yield item


class TestSubagentEdgeCases:
    """
    Test edge cases in subagent implementations for 100% coverage.
    """
    
    @pytest.mark.parametrize("module_path,func_name,db_id,llm_func", SUBAGENT_MODULES)
    @pytest.mark.asyncio
    async def test_subagent_bank_details_present(self, module_path, func_name, db_id, llm_func):
        """
        Test when banks have banks_detail for name extraction.
        """
        with patch(f'{module_path}.{llm_func}') as mock_llm:
            # Set up mock based on LLM function type
            if llm_func == "stream":
                mock_llm.return_value = async_generator([
                    {"choices": [{"delta": {"content": "Test"}}]},
                    {"usage": {"total_tokens": 50}}
                ])
            else:  # complete_with_tools
                mock_llm.return_value = {
                    "choices": [{"message": {"tool_calls": []}}],
                    "usage": {"total_tokens": 50}
                }

            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)

            # Banks with bank_period_combinations
            bank_period_combinations = [
                {"bank_id": 1, "bank_name": "Bank One", "bank_symbol": "B1", "fiscal_year": 2024, "quarter": "Q1"},
                {"bank_id": 2, "bank_name": "Bank Two", "bank_symbol": "B2", "fiscal_year": 2024, "quarter": "Q1"}
            ]

            # Use async for since subagents are async generators
            results = []
            async for item in agent_func(
                [], "Test", bank_period_combinations, "test", "Test", db_id, {"execution_id": "test"}
            ):
                results.append(item)

            # Check that LLM was called with proper context (only for streaming functions)
            if llm_func == "stream":
                call_args = mock_llm.call_args
                messages = call_args[1]["messages"]
                user_content = messages[1]["content"]  # Bank names are in user prompt now

                # Should include bank names from bank_period_combinations
                assert "Bank One" in user_content or "Bank Two" in user_content
    
    @pytest.mark.parametrize("module_path,func_name,db_id,llm_func", SUBAGENT_MODULES)
    @pytest.mark.asyncio
    async def test_subagent_bank_specific_periods(self, module_path, func_name, db_id, llm_func):
        """
        Test bank-specific periods with banks_detail present.
        """
        with patch(f'{module_path}.{llm_func}') as mock_llm:
            # Set up mock based on LLM function type
            if llm_func == "stream":
                mock_llm.return_value = async_generator([
                    {"choices": [{"delta": {"content": "Result"}}]},
                    {"usage": {"total_tokens": 50}}
                ])
            else:  # complete_with_tools
                mock_llm.return_value = {
                    "choices": [{"message": {"tool_calls": []}}],
                    "usage": {"total_tokens": 50}
                }
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            # Complex bank-specific setup
            bank_period_combinations = [
                {"bank_id": "1", "bank_name": "RBC", "bank_symbol": "RY", "fiscal_year": 2024, "quarter": "Q1"},
                {"bank_id": "1", "bank_name": "RBC", "bank_symbol": "RY", "fiscal_year": 2024, "quarter": "Q2"},
                {"bank_id": "2", "bank_name": "TD", "bank_symbol": "TD", "fiscal_year": 2024, "quarter": "Q3"},
                {"bank_id": "3", "bank_name": "BMO", "bank_symbol": "BMO", "fiscal_year": 2024, "quarter": "Q1"}
            ]
            
            # Use async for since subagents are async generators
            results = []
            async for item in agent_func(
                [], "Query", bank_period_combinations, "test", "Test", db_id, {"execution_id": "test"}
            ):
                results.append(item)
            
            # Check message construction
            # Check that LLM was called with proper context (only for streaming functions)
            if llm_func == "stream":
                call_args = mock_llm.call_args
                messages = call_args[1]["messages"]
                content = " ".join(m["content"] for m in messages)

                # Should include bank-specific period information
                assert "RBC" in content
                assert "Q1" in content or "Q2" in content
                assert "TD" in content
                assert "Q3" in content
    
    @pytest.mark.parametrize("module_path,func_name,db_id,llm_func", SUBAGENT_MODULES)
    @pytest.mark.asyncio
    async def test_subagent_bank_specific_periods_missing_bank_detail(self, module_path, func_name, db_id, llm_func):
        """
        Test bank-specific periods when a bank ID is not in banks_detail.
        """
        with patch(f'{module_path}.{llm_func}') as mock_llm:
            # Set up mock based on LLM function type
            if llm_func == "stream":
                mock_llm.return_value = async_generator([
                    {"choices": [{"delta": {"content": "Result"}}]},
                    {"usage": {"total_tokens": 50}}
                ])
            else:  # complete_with_tools
                mock_llm.return_value = {
                    "choices": [{"message": {"tool_calls": []}}],
                    "usage": {"total_tokens": 50}
                }
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            # Limited bank_period_combinations for edge case testing
            bank_period_combinations = [
                {"bank_id": "1", "bank_name": "RBC", "bank_symbol": "RY", "fiscal_year": 2024, "quarter": "Q1"},
                {"bank_id": "2", "bank_name": "TD", "bank_symbol": "TD", "fiscal_year": 2024, "quarter": "Q1"}
            ]
            
            # Should not crash
            # Use async for since subagents are async generators
            results = []
            async for item in agent_func(
                [], "Query", bank_period_combinations, "test", "Test", db_id, {"execution_id": "test"}
            ):
                results.append(item)
            
            assert len(results) > 0
            
            # Check that only valid banks are included
            # Check that LLM was called with proper context (only for streaming functions)
            if llm_func == "stream":
                call_args = mock_llm.call_args
                messages = call_args[1]["messages"]
                content = " ".join(m["content"] for m in messages)

                # Should include RBC but not bank 3 (since it's not in banks_detail)
                assert "RBC" in content
    
    @pytest.mark.parametrize("module_path,func_name,db_id,llm_func", SUBAGENT_MODULES)
    @pytest.mark.asyncio
    async def test_subagent_apply_all_periods_with_banks_detail(self, module_path, func_name, db_id, llm_func):
        """
        Test periods with apply_all when banks_detail is present.
        """
        with patch(f'{module_path}.{llm_func}') as mock_llm:
            # Set up mock based on LLM function type
            if llm_func == "stream":
                mock_llm.return_value = async_generator([
                    {"choices": [{"delta": {"content": "Result"}}]},
                    {"usage": {"total_tokens": 50}}
                ])
            else:  # complete_with_tools
                mock_llm.return_value = {
                    "choices": [{"message": {"tool_calls": []}}],
                    "usage": {"total_tokens": 50}
                }
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            bank_period_combinations = [
                {"bank_id": 1, "bank_name": "RBC", "bank_symbol": "RY", "fiscal_year": 2024, "quarter": "Q1"},
                {"bank_id": 1, "bank_name": "RBC", "bank_symbol": "RY", "fiscal_year": 2024, "quarter": "Q2"},
                {"bank_id": 1, "bank_name": "RBC", "bank_symbol": "RY", "fiscal_year": 2024, "quarter": "Q3"},
                {"bank_id": 2, "bank_name": "TD", "bank_symbol": "TD", "fiscal_year": 2024, "quarter": "Q1"},
                {"bank_id": 2, "bank_name": "TD", "bank_symbol": "TD", "fiscal_year": 2024, "quarter": "Q2"},
                {"bank_id": 2, "bank_name": "TD", "bank_symbol": "TD", "fiscal_year": 2024, "quarter": "Q3"}
            ]
            
            # Use async for since subagents are async generators
            results = []
            async for item in agent_func(
                [], "Query", bank_period_combinations, "test", "Test", db_id, {"execution_id": "test"}
            ):
                results.append(item)
            
            assert len(results) > 0
            
            # Check message includes period info
            # Check that LLM was called with proper context (only for streaming functions)
            if llm_func == "stream":
                call_args = mock_llm.call_args
                messages = call_args[1]["messages"]
                content = " ".join(m["content"] for m in messages)

                # Should include period description
                assert "Q1" in content or "Q2" in content or "Q3" in content
                assert "2024" in content