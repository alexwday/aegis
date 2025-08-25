"""
Additional tests to achieve 100% coverage for subagent modules.

Focuses on edge cases not covered by test_all_subagents.py.
"""

import pytest
from unittest.mock import patch
import importlib


# List of all subagent modules
SUBAGENT_MODULES = [
    ("aegis.model.subagents.benchmarking", "benchmarking_agent", "benchmarking"),
    ("aegis.model.subagents.reports", "reports_agent", "reports"),
    ("aegis.model.subagents.rts", "rts_agent", "rts"),
    ("aegis.model.subagents.transcripts", "transcripts_agent", "transcripts"),
    ("aegis.model.subagents.pillar3", "pillar3_agent", "pillar3"),
]


class TestSubagentEdgeCases:
    """
    Test edge cases in subagent implementations for 100% coverage.
    """
    
    @pytest.mark.parametrize("module_path,func_name,db_id", SUBAGENT_MODULES)
    def test_subagent_bank_details_present(self, module_path, func_name, db_id):
        """
        Test when banks have banks_detail for name extraction.
        """
        with patch(f'{module_path}.stream') as mock_stream:
            mock_stream.return_value = [
                {"choices": [{"delta": {"content": "Test"}}]},
                {"usage": {"total_tokens": 50}}
            ]
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            # Banks with banks_detail
            banks = {
                "bank_ids": [1, 2],
                "banks_detail": {
                    "1": {"name": "Bank One", "symbol": "B1"},
                    "2": {"name": "Bank Two", "symbol": "B2"}
                }
            }
            periods = {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}
            
            results = list(agent_func(
                [], "Test", banks, periods, "test", "Test", db_id, {"execution_id": "test"}
            ))
            
            # Check that stream was called with proper context
            call_args = mock_stream.call_args
            messages = call_args[1]["messages"]
            system_content = messages[0]["content"]
            
            # Should include bank names from banks_detail
            assert "Bank One" in system_content or "Bank Two" in system_content
    
    @pytest.mark.parametrize("module_path,func_name,db_id", SUBAGENT_MODULES)
    def test_subagent_bank_specific_periods(self, module_path, func_name, db_id):
        """
        Test bank-specific periods with banks_detail present.
        """
        with patch(f'{module_path}.stream') as mock_stream:
            mock_stream.return_value = [
                {"choices": [{"delta": {"content": "Result"}}]},
                {"usage": {"total_tokens": 50}}
            ]
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            # Complex bank-specific setup
            banks = {
                "bank_ids": ["1", "2", "3"],
                "banks_detail": {
                    "1": {"name": "RBC", "symbol": "RY"},
                    "2": {"name": "TD", "symbol": "TD"},
                    "3": {"name": "BMO", "symbol": "BMO"}
                }
            }
            periods = {
                "periods": {
                    "bank_specific": {
                        "1": {"fiscal_year": 2024, "quarters": ["Q1", "Q2"]},
                        "2": {"fiscal_year": 2024, "quarters": ["Q3"]},
                        # Note: bank 3 has no period data
                    }
                }
            }
            
            results = list(agent_func(
                [], "Query", banks, periods, "test", "Test", db_id, {"execution_id": "test"}
            ))
            
            # Check message construction
            call_args = mock_stream.call_args
            messages = call_args[1]["messages"]
            content = " ".join(m["content"] for m in messages)
            
            # Should include bank-specific period information
            assert "RBC" in content
            assert "Q1" in content or "Q2" in content
            assert "TD" in content
            assert "Q3" in content
    
    @pytest.mark.parametrize("module_path,func_name,db_id", SUBAGENT_MODULES)
    def test_subagent_bank_specific_periods_missing_bank_detail(self, module_path, func_name, db_id):
        """
        Test bank-specific periods when a bank ID is not in banks_detail.
        """
        with patch(f'{module_path}.stream') as mock_stream:
            mock_stream.return_value = [
                {"choices": [{"delta": {"content": "Result"}}]},
                {"usage": {"total_tokens": 50}}
            ]
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            # Bank 3 in periods but not in banks_detail
            banks = {
                "bank_ids": ["1", "2"],
                "banks_detail": {
                    "1": {"name": "RBC", "symbol": "RY"},
                    "2": {"name": "TD", "symbol": "TD"}
                }
            }
            periods = {
                "periods": {
                    "bank_specific": {
                        "1": {"fiscal_year": 2024, "quarters": ["Q1"]},
                        "3": {"fiscal_year": 2024, "quarters": ["Q2"]}  # Bank 3 not in banks_detail
                    }
                }
            }
            
            # Should not crash
            results = list(agent_func(
                [], "Query", banks, periods, "test", "Test", db_id, {"execution_id": "test"}
            ))
            
            assert len(results) > 0
            
            # Check that only valid banks are included
            call_args = mock_stream.call_args
            messages = call_args[1]["messages"]
            content = " ".join(m["content"] for m in messages)
            
            # Should include RBC but not bank 3 (since it's not in banks_detail)
            assert "RBC" in content
    
    @pytest.mark.parametrize("module_path,func_name,db_id", SUBAGENT_MODULES)
    def test_subagent_apply_all_periods_with_banks_detail(self, module_path, func_name, db_id):
        """
        Test periods with apply_all when banks_detail is present.
        """
        with patch(f'{module_path}.stream') as mock_stream:
            mock_stream.return_value = [
                {"choices": [{"delta": {"content": "Result"}}]},
                {"usage": {"total_tokens": 50}}
            ]
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            banks = {
                "bank_ids": [1, 2],
                "banks_detail": {
                    "1": {"name": "RBC", "symbol": "RY"},
                    "2": {"name": "TD", "symbol": "TD"}
                }
            }
            # Use periods with apply_all structure wrapped in "periods"
            periods = {
                "periods": {
                    "apply_all": {"fiscal_year": 2024, "quarters": ["Q1", "Q2", "Q3"]}
                }
            }
            
            results = list(agent_func(
                [], "Query", banks, periods, "test", "Test", db_id, {"execution_id": "test"}
            ))
            
            assert len(results) > 0
            
            # Check message includes period info
            call_args = mock_stream.call_args
            messages = call_args[1]["messages"]
            content = " ".join(m["content"] for m in messages)
            
            # Should include period description
            assert "Q1" in content or "Q2" in content or "Q3" in content
            assert "2024" in content