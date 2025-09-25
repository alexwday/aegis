"""
Comprehensive tests for all subagent modules - simplified version.

Tests all subagents with their identical placeholder implementations.
"""

import pytest
import pytest_asyncio
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from typing import Generator
import importlib


# List of all subagent modules with their LLM function type
# Format: (module_path, function_name, database_id, llm_function)
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


class TestAllSubagents:
    """
    Comprehensive tests for all subagent implementations.
    """
    
    @pytest.mark.parametrize("module_path,func_name,db_id,llm_func", SUBAGENT_MODULES)
    @pytest.mark.asyncio
    async def test_subagent_basic_execution(self, module_path, func_name, db_id, llm_func):
        """
        Test basic execution of each subagent without external dependencies.
        """
        async def run_test_with_patches():
            # Mock LLM response based on function type
            with patch(f'{module_path}.{llm_func}') as mock_llm:
                if llm_func == "stream":
                    # Fix AsyncMock streaming issue - use regular mock that returns async generator
                    mock_llm.return_value = async_generator([
                        {"choices": [{"delta": {"content": "Test "}}]},
                        {"choices": [{"delta": {"content": "response"}}]},
                        {"usage": {"total_tokens": 100}}
                    ])
                else:  # complete_with_tools
                    mock_llm.return_value = {
                        "choices": [{"message": {"tool_calls": []}}],
                        "usage": {"total_tokens": 100}
                    }

                return await self._execute_subagent_test(module_path, func_name, db_id, mock_llm)

        # Set up additional patches based on subagent type
        if db_id == "reports":
            # Mock database calls for reports subagent
            with patch(f'{module_path}.get_available_reports') as mock_reports, \
                 patch(f'{module_path}.get_unique_report_types') as mock_types, \
                 patch(f'{module_path}.retrieve_reports_by_type') as mock_retrieve:

                # Mock database functions to return data
                mock_reports.return_value = [{
                    "report_id": 1,
                    "report_type": "call_summary",
                    "content": "Sample report content",
                    "bank_id": 1,
                    "fiscal_year": 2024,
                    "quarter": "Q1"
                }]
                mock_types.return_value = [{"report_type": "call_summary"}]
                from datetime import datetime, timezone
                mock_retrieve.return_value = [{
                    "report_id": 1,
                    "report_type": "call_summary",
                    "report_name": "Test Bank Q1 2024 Call Summary",
                    "content": "Sample report content for Test Bank Q1 2024",
                    "bank_id": 1,
                    "bank_name": "Test Bank",
                    "bank_symbol": "TB",
                    "fiscal_year": 2024,
                    "quarter": "Q1",
                    "created_at": datetime.now(timezone.utc),
                    "generation_date": datetime.now(timezone.utc),
                    "date_last_modified": datetime.now(timezone.utc),
                    "s3_document_name": "test_report.docx"
                }]

                await run_test_with_patches()
        else:
            await run_test_with_patches()

    async def _execute_subagent_test(self, module_path, func_name, db_id, mock_llm):
            
            # Import the module and get the function
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            # Test inputs
            conversation = []
            latest_message = "Test query"
            bank_period_combinations = [
                {
                    "bank_id": 1,
                    "bank_name": "Test Bank",
                    "bank_symbol": "TB",
                    "fiscal_year": 2024,
                    "quarter": "Q1"
                }
            ]
            basic_intent = "test"
            full_intent = "Test query"
            context = {"execution_id": "test-123"}
            
            # Execute
            results = []

            async for item in agent_func(
                conversation, latest_message, bank_period_combinations,
                basic_intent, full_intent, db_id, context
            ):

                results.append(item)
            
            # Verify results
            assert len(results) > 0
            assert all(r["type"] == "subagent" for r in results)
            assert all(r["name"] == db_id for r in results)
            
            # Verify LLM function was called
            mock_llm.assert_called_once()
    
    @pytest.mark.parametrize("module_path,func_name,db_id,llm_func", SUBAGENT_MODULES)
    @pytest.mark.asyncio
    async def test_subagent_error_handling(self, module_path, func_name, db_id, llm_func):
        """
        Test error handling in subagents with banks_detail present.
        """
        with patch(f'{module_path}.{llm_func}') as mock_llm:
            # Make LLM function raise an exception
            mock_llm.side_effect = Exception("Connection error")
            
            # Import the module and get the function
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            # Test inputs with bank_period_combinations to trigger the error in the try block
            conversation = []
            latest_message = "Test query"
            bank_period_combinations = [
                {
                    "bank_id": 1,
                    "bank_name": "Test Bank",
                    "bank_symbol": "TB",
                    "fiscal_year": 2024,
                    "quarter": "Q1"
                }
            ]
            basic_intent = "test"
            full_intent = "Test query"
            context = {"execution_id": "error-test"}
            
            # Execute - should not raise, but yield error
            results = []

            async for item in agent_func(
                conversation, latest_message, bank_period_combinations,
                basic_intent, full_intent, db_id, context
            ):

                results.append(item)
            
            # Should yield error message
            assert len(results) > 0
            last_result = results[-1]
            assert last_result["type"] == "subagent"
            assert last_result["name"] == db_id
            assert "Error" in last_result["content"] or "error" in last_result["content"]
    
    @pytest.mark.parametrize("module_path,func_name,db_id,llm_func", SUBAGENT_MODULES)
    @pytest.mark.asyncio
    async def test_subagent_logging(self, module_path, func_name, db_id, llm_func):
        """
        Test that subagents log appropriately.
        """
        with patch(f'{module_path}.{llm_func}') as mock_llm, \
             patch(f'{module_path}.get_logger') as mock_logger:
            
            # Setup mocks
            mock_logger_instance = Mock()
            mock_logger.return_value = mock_logger_instance
            
            mock_llm.return_value = async_generator([
                {"choices": [{"delta": {"content": "Test"}}]},
                {"usage": {"total_tokens": 50}}
            ])
            
            # Import and execute
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            conversation = []
            latest_message = "Query"
            bank_period_combinations = [
                {
                    "bank_id": 1,
                    "bank_name": "Test Bank",
                    "bank_symbol": "TB",
                    "fiscal_year": 2024,
                    "quarter": "Q1"
                }
            ]
            basic_intent = "test"
            full_intent = "Test"
            context = {"execution_id": "log-test"}
            
            list(agent_func(
                conversation, latest_message, bank_period_combinations,
                basic_intent, full_intent, db_id, context
            ))
            
            # Check logging
            info_calls = mock_logger_instance.info.call_args_list
            assert len(info_calls) >= 2  # Start and complete logs
    
    @pytest.mark.parametrize("module_path,func_name,db_id,llm_func", SUBAGENT_MODULES)
    @pytest.mark.asyncio
    async def test_subagent_complex_inputs(self, module_path, func_name, db_id, llm_func):
        """
        Test subagents with complex multi-bank, multi-period inputs.
        """
        with patch(f'{module_path}.{llm_func}') as mock_llm:
            mock_llm.return_value = async_generator([
                {"choices": [{"delta": {"content": "Complex response"}}]},
                {"usage": {"total_tokens": 200}}
            ])
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            # Complex inputs
            conversation = [
                {"role": "user", "content": "First message"},
                {"role": "assistant", "content": "Response"},
                {"role": "user", "content": "Follow-up"}
            ]
            latest_message = "Compare all banks"
            bank_period_combinations = [
                {"bank_id": 1, "bank_name": "Bank1", "bank_symbol": "B1", "fiscal_year": 2024, "quarter": "Q1"},
                {"bank_id": 1, "bank_name": "Bank1", "bank_symbol": "B1", "fiscal_year": 2024, "quarter": "Q2"},
                {"bank_id": 2, "bank_name": "Bank2", "bank_symbol": "B2", "fiscal_year": 2024, "quarter": "Q3"},
                {"bank_id": 3, "bank_name": "Bank3", "bank_symbol": "B3", "fiscal_year": 2023, "quarter": "Q4"},
            ]
            basic_intent = "comparison"
            full_intent = "Complex multi-bank comparison"
            context = {"execution_id": "complex-test"}
            
            results = []

            
            async for item in agent_func(
                conversation, latest_message, bank_period_combinations,
                basic_intent, full_intent, db_id, context
            ):

            
                results.append(item)
            
            assert len(results) > 0
            assert "Complex response" in "".join(r.get("content", "") for r in results)
    
    @pytest.mark.parametrize("module_path,func_name,db_id,llm_func", SUBAGENT_MODULES)
    @pytest.mark.asyncio
    async def test_subagent_empty_inputs(self, module_path, func_name, db_id, llm_func):
        """
        Test subagents handle empty inputs gracefully.
        """
        with patch(f'{module_path}.{llm_func}') as mock_llm:
            mock_llm.return_value = async_generator([
                {"choices": [{"delta": {"content": "Empty"}}]},
                {"usage": {"total_tokens": 10}}
            ])
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            # Empty inputs
            conversation = []
            latest_message = ""
            bank_period_combinations = []
            basic_intent = ""
            full_intent = ""
            context = {"execution_id": "empty-test"}
            
            # Should not crash
            results = []

            async for item in agent_func(
                conversation, latest_message, bank_period_combinations,
                basic_intent, full_intent, db_id, context
            ):

                results.append(item)
            
            assert len(results) > 0
    
    @pytest.mark.parametrize("module_path,func_name,db_id,llm_func", SUBAGENT_MODULES)
    @pytest.mark.asyncio
    async def test_subagent_generator_behavior(self, module_path, func_name, db_id, llm_func):
        """
        Test that subagents properly yield chunks as generators.
        """
        with patch(f'{module_path}.{llm_func}') as mock_llm:
            # Multiple chunks
            mock_llm.return_value = async_generator([
                {"choices": [{"delta": {"content": "A"}}]},
                {"choices": [{"delta": {"content": "B"}}]},
                {"choices": [{"delta": {"content": "C"}}]},
                {"usage": {"total_tokens": 3}}
            ])
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            conversation = []
            latest_message = "Query"
            bank_period_combinations = [
                {
                    "bank_id": 1,
                    "bank_name": "Test Bank",
                    "bank_symbol": "TB",
                    "fiscal_year": 2024,
                    "quarter": "Q1"
                }
            ]
            basic_intent = "test"
            full_intent = "Test"
            context = {"execution_id": "gen-test"}
            
            # Get generator
            gen = agent_func(
                conversation, latest_message, bank_period_combinations,
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
    
    @pytest.mark.parametrize("module_path,func_name,db_id,llm_func", SUBAGENT_MODULES)
    @pytest.mark.asyncio
    async def test_subagent_message_construction(self, module_path, func_name, db_id, llm_func):
        """
        Test that subagents construct proper messages for the LLM.
        """
        with patch(f'{module_path}.{llm_func}') as mock_llm:
            mock_llm.return_value = async_generator([
                {"choices": [{"delta": {"content": "Response"}}]},
                {"usage": {"total_tokens": 50}}
            ])
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            conversation = [{"role": "user", "content": "previous message"}]
            latest_message = "Get efficiency ratio"
            bank_period_combinations = [
                {"bank_id": 1, "bank_name": "RBC", "bank_symbol": "RY", "fiscal_year": 2024, "quarter": "Q1"},
                {"bank_id": 1, "bank_name": "RBC", "bank_symbol": "RY", "fiscal_year": 2024, "quarter": "Q2"}
            ]
            basic_intent = "efficiency ratio"
            full_intent = "Get RBC efficiency ratio for Q1-Q2 2024"
            context = {"execution_id": "msg-test"}
            
            list(agent_func(
                conversation, latest_message, bank_period_combinations,
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
    
    @pytest.mark.parametrize("module_path,func_name,db_id,llm_func", SUBAGENT_MODULES)
    @pytest.mark.asyncio
    async def test_subagent_no_usage_data(self, module_path, func_name, db_id, llm_func):
        """
        Test subagents work when stream doesn't return usage data.
        """
        with patch(f'{module_path}.{llm_func}') as mock_llm:
            # No usage data
            mock_llm.return_value = async_generator([
                {"choices": [{"delta": {"content": "Data"}}]},
                {"choices": [{"delta": {"content": " only"}}]}
            ])
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            conversation = []
            latest_message = "Query"
            bank_period_combinations = [
                {
                    "bank_id": 1,
                    "bank_name": "Test Bank",
                    "bank_symbol": "TB",
                    "fiscal_year": 2024,
                    "quarter": "Q1"
                }
            ]
            basic_intent = "test"
            full_intent = "Test"
            context = {"execution_id": "no-usage"}
            
            results = []

            
            async for item in agent_func(
                conversation, latest_message, bank_period_combinations,
                basic_intent, full_intent, db_id, context
            ):

            
                results.append(item)
            
            # Should still work
            assert len(results) > 0
            content = "".join(r.get("content", "") for r in results)
            assert "Data only" in content
    
    @pytest.mark.parametrize("module_path,func_name,db_id,llm_func", SUBAGENT_MODULES)
    @pytest.mark.asyncio
    async def test_subagent_llm_params(self, module_path, func_name, db_id, llm_func):
        """
        Test that subagents use correct LLM parameters.
        """
        with patch(f'{module_path}.{llm_func}') as mock_llm, \
             patch('aegis.utils.settings.config') as mock_config:
            
            # Setup config
            mock_config.llm.medium.model = "test-model"
            mock_config.llm.medium.temperature = 0.5
            mock_config.llm.medium.max_tokens = 1000
            
            mock_llm.return_value = async_generator([
                {"choices": [{"delta": {"content": "Test"}}]},
                {"usage": {"total_tokens": 50}}
            ])
            
            module = importlib.import_module(module_path)
            agent_func = getattr(module, func_name)
            
            conversation = []
            latest_message = "Query"
            bank_period_combinations = [
                {
                    "bank_id": 1,
                    "bank_name": "Test Bank",
                    "bank_symbol": "TB",
                    "fiscal_year": 2024,
                    "quarter": "Q1"
                }
            ]
            basic_intent = "test"
            full_intent = "Test"
            context = {"execution_id": "params-test"}
            
            list(agent_func(
                conversation, latest_message, bank_period_combinations,
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