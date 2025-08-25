"""
Comprehensive tests for all subagent modules - simplified version.

Tests all subagents with their identical placeholder implementations.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from typing import Generator
import importlib


# List of all subagent modules
SUBAGENT_MODULES = [
    ("aegis.model.subagents.benchmarking", "benchmarking_agent", "benchmarking"),
    ("aegis.model.subagents.reports", "reports_agent", "reports"),
    ("aegis.model.subagents.rts", "rts_agent", "rts"),
    ("aegis.model.subagents.transcripts", "transcripts_agent", "transcripts"),
    ("aegis.model.subagents.pillar3", "pillar3_agent", "pillar3"),
]


class TestAllSubagents:
    """
    Comprehensive tests for all subagent implementations.
    """
    
    @pytest.mark.parametrize("module_path,func_name,db_id", SUBAGENT_MODULES)
    def test_subagent_basic_execution(self, module_path, func_name, db_id):
        """
        Test basic execution of each subagent without external dependencies.
        """
        with patch(f'{module_path}.stream') as mock_stream:
            # Mock streaming response
            mock_stream.return_value = [
                {"choices": [{"delta": {"content": "Test "}}]},
                {"choices": [{"delta": {"content": "response"}}]},
                {"usage": {"total_tokens": 100}}
            ]
            
            # Import the module and get the function
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            # Test inputs
            conversation = []
            latest_message = "Test query"
            banks = {"bank_ids": [1], "bank_details": [{"id": 1, "name": "Test Bank"}]}
            periods = {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}
            basic_intent = "test"
            full_intent = "Test query"
            context = {"execution_id": "test-123"}
            
            # Execute
            results = list(agent_func(
                conversation, latest_message, banks, periods,
                basic_intent, full_intent, db_id, context
            ))
            
            # Verify results
            assert len(results) > 0
            assert all(r["type"] == "subagent" for r in results)
            assert all(r["name"] == db_id for r in results)
            
            # Verify stream was called
            mock_stream.assert_called_once()
    
    @pytest.mark.parametrize("module_path,func_name,db_id", SUBAGENT_MODULES)
    def test_subagent_error_handling(self, module_path, func_name, db_id):
        """
        Test error handling in subagents with banks_detail present.
        """
        with patch(f'{module_path}.stream') as mock_stream:
            # Make stream raise an exception
            mock_stream.side_effect = Exception("Connection error")
            
            # Import the module and get the function
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            # Test inputs with banks_detail to trigger the error in the try block
            conversation = []
            latest_message = "Test query"
            banks = {
                "bank_ids": [1],
                "banks_detail": {
                    "1": {"name": "Test Bank", "symbol": "TB"}
                }
            }
            periods = {"periods": {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}}
            basic_intent = "test"
            full_intent = "Test query"
            context = {"execution_id": "error-test"}
            
            # Execute - should not raise, but yield error
            results = list(agent_func(
                conversation, latest_message, banks, periods,
                basic_intent, full_intent, db_id, context
            ))
            
            # Should yield error message
            assert len(results) > 0
            last_result = results[-1]
            assert last_result["type"] == "subagent"
            assert last_result["name"] == db_id
            assert "Error" in last_result["content"] or "error" in last_result["content"]
    
    @pytest.mark.parametrize("module_path,func_name,db_id", SUBAGENT_MODULES)
    def test_subagent_logging(self, module_path, func_name, db_id):
        """
        Test that subagents log appropriately.
        """
        with patch(f'{module_path}.stream') as mock_stream, \
             patch(f'{module_path}.get_logger') as mock_logger:
            
            # Setup mocks
            mock_logger_instance = Mock()
            mock_logger.return_value = mock_logger_instance
            
            mock_stream.return_value = [
                {"choices": [{"delta": {"content": "Test"}}]},
                {"usage": {"total_tokens": 50}}
            ]
            
            # Import and execute
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            conversation = []
            latest_message = "Query"
            banks = {"bank_ids": [1]}
            periods = {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}
            basic_intent = "test"
            full_intent = "Test"
            context = {"execution_id": "log-test"}
            
            list(agent_func(
                conversation, latest_message, banks, periods,
                basic_intent, full_intent, db_id, context
            ))
            
            # Check logging
            info_calls = mock_logger_instance.info.call_args_list
            assert len(info_calls) >= 2  # Start and complete logs
    
    @pytest.mark.parametrize("module_path,func_name,db_id", SUBAGENT_MODULES)
    def test_subagent_complex_inputs(self, module_path, func_name, db_id):
        """
        Test subagents with complex multi-bank, multi-period inputs.
        """
        with patch(f'{module_path}.stream') as mock_stream:
            mock_stream.return_value = [
                {"choices": [{"delta": {"content": "Complex response"}}]},
                {"usage": {"total_tokens": 200}}
            ]
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            # Complex inputs
            conversation = [
                {"role": "user", "content": "First message"},
                {"role": "assistant", "content": "Response"},
                {"role": "user", "content": "Follow-up"}
            ]
            latest_message = "Compare all banks"
            banks = {
                "bank_ids": [1, 2, 3, 4, 5],
                "bank_details": [
                    {"id": i, "name": f"Bank{i}", "symbol": f"B{i}"}
                    for i in range(1, 6)
                ]
            }
            periods = {
                "periods": {
                    "bank_specific": {
                        "1": {"fiscal_year": 2024, "quarters": ["Q1", "Q2"]},
                        "2": {"fiscal_year": 2024, "quarters": ["Q3"]},
                        "3": {"fiscal_year": 2023, "quarters": ["Q4"]}
                    }
                }
            }
            basic_intent = "comparison"
            full_intent = "Complex multi-bank comparison"
            context = {"execution_id": "complex-test"}
            
            results = list(agent_func(
                conversation, latest_message, banks, periods,
                basic_intent, full_intent, db_id, context
            ))
            
            assert len(results) > 0
            assert "Complex response" in "".join(r.get("content", "") for r in results)
    
    @pytest.mark.parametrize("module_path,func_name,db_id", SUBAGENT_MODULES)
    def test_subagent_empty_inputs(self, module_path, func_name, db_id):
        """
        Test subagents handle empty inputs gracefully.
        """
        with patch(f'{module_path}.stream') as mock_stream:
            mock_stream.return_value = [
                {"choices": [{"delta": {"content": "Empty"}}]},
                {"usage": {"total_tokens": 10}}
            ]
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            # Empty inputs
            conversation = []
            latest_message = ""
            banks = {"bank_ids": [], "bank_details": []}
            periods = {}
            basic_intent = ""
            full_intent = ""
            context = {"execution_id": "empty-test"}
            
            # Should not crash
            results = list(agent_func(
                conversation, latest_message, banks, periods,
                basic_intent, full_intent, db_id, context
            ))
            
            assert len(results) > 0
    
    @pytest.mark.parametrize("module_path,func_name,db_id", SUBAGENT_MODULES)
    def test_subagent_generator_behavior(self, module_path, func_name, db_id):
        """
        Test that subagents properly yield chunks as generators.
        """
        with patch(f'{module_path}.stream') as mock_stream:
            # Multiple chunks
            mock_stream.return_value = [
                {"choices": [{"delta": {"content": "A"}}]},
                {"choices": [{"delta": {"content": "B"}}]},
                {"choices": [{"delta": {"content": "C"}}]},
                {"usage": {"total_tokens": 3}}
            ]
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            conversation = []
            latest_message = "Query"
            banks = {"bank_ids": [1]}
            periods = {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}
            basic_intent = "test"
            full_intent = "Test"
            context = {"execution_id": "gen-test"}
            
            # Get generator
            gen = agent_func(
                conversation, latest_message, banks, periods,
                basic_intent, full_intent, db_id, context
            )
            
            # Verify it's a generator
            assert hasattr(gen, '__iter__')
            assert hasattr(gen, '__next__')
            
            # Consume and verify chunks
            chunks = list(gen)
            assert len(chunks) >= 3
            content = "".join(c.get("content", "") for c in chunks)
            assert "ABC" in content
    
    @pytest.mark.parametrize("module_path,func_name,db_id", SUBAGENT_MODULES)
    def test_subagent_message_construction(self, module_path, func_name, db_id):
        """
        Test that subagents construct proper messages for the LLM.
        """
        with patch(f'{module_path}.stream') as mock_stream:
            mock_stream.return_value = [
                {"choices": [{"delta": {"content": "Response"}}]},
                {"usage": {"total_tokens": 50}}
            ]
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            conversation = [{"role": "user", "content": "previous message"}]
            latest_message = "Get efficiency ratio"
            banks = {"bank_ids": [1], "bank_details": [{"id": 1, "name": "RBC", "symbol": "RY"}]}
            periods = {"periods": {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1", "Q2"]}}}
            basic_intent = "efficiency ratio"
            full_intent = "Get RBC efficiency ratio for Q1-Q2 2024"
            context = {"execution_id": "msg-test"}
            
            list(agent_func(
                conversation, latest_message, banks, periods,
                basic_intent, full_intent, db_id, context
            ))
            
            # Check message construction
            mock_stream.assert_called_once()
            call_args = mock_stream.call_args
            messages = call_args[1]["messages"]
            
            # Should have system and user messages
            assert len(messages) >= 2
            assert messages[0]["role"] == "system"
            assert messages[1]["role"] == "user"
            
            # System message should mention the database
            system_content = messages[0]["content"]
            # Special case for pillar3 which is displayed as "Pillar 3"
            if db_id == "pillar3":
                assert "pillar 3" in system_content.lower()
            else:
                assert db_id in system_content.lower() or db_id.title() in system_content
            
            # Should include context about banks and periods
            all_content = " ".join(m["content"] for m in messages)
            assert "RBC" in all_content
            assert "2024" in all_content
            assert "efficiency" in all_content.lower()
    
    @pytest.mark.parametrize("module_path,func_name,db_id", SUBAGENT_MODULES)
    def test_subagent_no_usage_data(self, module_path, func_name, db_id):
        """
        Test subagents work when stream doesn't return usage data.
        """
        with patch(f'{module_path}.stream') as mock_stream:
            # No usage data
            mock_stream.return_value = [
                {"choices": [{"delta": {"content": "Data"}}]},
                {"choices": [{"delta": {"content": " only"}}]}
            ]
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            conversation = []
            latest_message = "Query"
            banks = {"bank_ids": [1]}
            periods = {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}
            basic_intent = "test"
            full_intent = "Test"
            context = {"execution_id": "no-usage"}
            
            results = list(agent_func(
                conversation, latest_message, banks, periods,
                basic_intent, full_intent, db_id, context
            ))
            
            # Should still work
            assert len(results) > 0
            content = "".join(r.get("content", "") for r in results)
            assert "Data only" in content
    
    @pytest.mark.parametrize("module_path,func_name,db_id", SUBAGENT_MODULES)
    def test_subagent_llm_params(self, module_path, func_name, db_id):
        """
        Test that subagents use correct LLM parameters.
        """
        with patch(f'{module_path}.stream') as mock_stream, \
             patch('aegis.utils.settings.config') as mock_config:
            
            # Setup config
            mock_config.llm.medium.model = "test-model"
            mock_config.llm.medium.temperature = 0.5
            mock_config.llm.medium.max_tokens = 1000
            
            mock_stream.return_value = [
                {"choices": [{"delta": {"content": "Test"}}]},
                {"usage": {"total_tokens": 50}}
            ]
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            conversation = []
            latest_message = "Query"
            banks = {"bank_ids": [1]}
            periods = {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}
            basic_intent = "test"
            full_intent = "Test"
            context = {"execution_id": "params-test"}
            
            list(agent_func(
                conversation, latest_message, banks, periods,
                basic_intent, full_intent, db_id, context
            ))
            
            # Check LLM params (subagents use default params, not passed explicitly)
            call_args = mock_stream.call_args
            llm_params = call_args[1].get("llm_params", {})
            
            # Subagents use temperature and max_tokens but model is from config
            assert "temperature" in llm_params
            assert "max_tokens" in llm_params
            assert llm_params["temperature"] == 0.7  # Default in subagents
            assert llm_params["max_tokens"] == 500  # Default in subagents