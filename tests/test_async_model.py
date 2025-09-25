"""
Integration tests for the async model implementation.
Tests concurrent execution, streaming, and async/await patterns.
"""

import asyncio
import time
from typing import List, Dict, Any
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
import pytest_asyncio

from aegis.model.main import model
from aegis.utils.settings import config


class TestAsyncModel:
    """Test the async model implementation."""

    @pytest.mark.asyncio
    async def test_model_direct_response(self):
        """Test async model with direct response path (greetings, definitions)."""
        conversation = {
            "messages": [
                {"role": "user", "content": "Hello, what is a derivative?"}
            ]
        }

        responses = []
        with patch('aegis.model.agents.router.route_query', new_callable=AsyncMock) as mock_router:
            # Mock router to return direct_response
            mock_router.return_value = {
                "status": "success",
                "route": "direct_response",
                "confidence": 0.95,
                "reasoning": "Definition question"
            }

            with patch('aegis.model.agents.response.generate_response') as mock_response:
                # Mock response generator as async generator
                async def mock_gen(*args, **kwargs):
                    yield {"type": "agent", "name": "aegis", "content": "A derivative is"}
                    yield {"type": "agent", "name": "aegis", "content": " a financial instrument"}

                mock_response.return_value = mock_gen()

                async for chunk in model(conversation):
                    responses.append(chunk)
                    if len(responses) > 10:  # Safety limit
                        break

        assert len(responses) > 0
        assert responses[0]["type"] == "agent"
        assert responses[0]["name"] == "aegis"
        assert "derivative" in " ".join(r.get("content", "") for r in responses).lower()

    @pytest.mark.asyncio
    async def test_model_research_workflow(self):
        """Test async model with research workflow path (data queries)."""
        conversation = {
            "messages": [
                {"role": "user", "content": "What is RBC's revenue for Q3 2024?"}
            ]
        }

        with patch('aegis.model.agents.router.route_query', new_callable=AsyncMock) as mock_router:
            mock_router.return_value = {
                "status": "success",
                "route": "research_workflow",
                "confidence": 0.90,
                "reasoning": "Revenue data query"
            }

            with patch('aegis.model.agents.clarifier.clarify_query', new_callable=AsyncMock) as mock_clarifier:
                mock_clarifier.return_value = {
                    "status": "success",
                    "bank_period_combinations": [{
                        "bank_id": 1,
                        "bank_name": "Royal Bank of Canada",
                        "bank_symbol": "RY",
                        "fiscal_year": 2024,
                        "quarter": "Q3",
                        "query_intent": "revenue metrics"
                    }],
                    "clarifier_intent": "Extract RBC Q3 2024 revenue"
                }

                with patch('aegis.model.agents.planner.plan_database_queries', new_callable=AsyncMock) as mock_planner:
                    mock_planner.return_value = {
                        "status": "success",
                        "databases": ["transcripts"],
                        "reasoning": "Revenue data in transcripts"
                    }

                    # Mock subagent
                    async def mock_transcripts(*args, **kwargs):
                        yield {"type": "subagent", "name": "transcripts", "content": "RBC revenue: $12.5B"}

                    with patch('aegis.model.subagents.transcripts.main.transcripts_agent', mock_transcripts):
                        responses = []
                        async for chunk in model(conversation, db_names=["transcripts"]):
                            responses.append(chunk)
                            if len(responses) > 20:
                                break

        # Should have subagent responses
        assert any(r.get("type") == "subagent" for r in responses)
        assert any("revenue" in r.get("content", "").lower() for r in responses)

    @pytest.mark.asyncio
    async def test_concurrent_subagents_execution(self):
        """Test that multiple subagents run concurrently, not sequentially."""
        from aegis.model.subagents import SUBAGENT_MAPPING

        # Track execution timing
        execution_times = {}

        async def mock_subagent(*args, **kwargs):
            """Mock subagent that simulates 1 second of work."""
            db_id = kwargs.get("database_id", "unknown")
            start = time.time()
            await asyncio.sleep(1)  # Simulate database query
            execution_times[db_id] = {
                "start": start,
                "end": time.time()
            }
            yield {
                "type": "subagent",
                "name": db_id,
                "content": f"Data from {db_id}"
            }

        # Mock all subagents
        with patch.dict(SUBAGENT_MAPPING, {
            "transcripts": mock_subagent,
            "reports": mock_subagent,
            "rts": mock_subagent
        }):
            # Mock the workflow to go straight to subagents
            with patch('aegis.model.agents.router.route_query', new_callable=AsyncMock) as mock_router:
                mock_router.return_value = {
                    "status": "success",
                    "route": "research_workflow"
                }

                with patch('aegis.model.agents.clarifier.clarify_query', new_callable=AsyncMock) as mock_clarifier:
                    mock_clarifier.return_value = {
                        "status": "success",
                        "bank_period_combinations": [{
                            "bank_id": 1,
                            "bank_name": "Test Bank",
                            "bank_symbol": "TB",
                            "fiscal_year": 2024,
                            "quarter": "Q3"
                        }],
                        "clarifier_intent": "Test query"
                    }

                    with patch('aegis.model.agents.planner.plan_database_queries', new_callable=AsyncMock) as mock_planner:
                        mock_planner.return_value = {
                            "status": "success",
                            "databases": ["transcripts", "reports", "rts"]
                        }

                        # Mock summarizer to avoid errors
                        async def mock_summarizer(*args, **kwargs):
                            yield {"type": "agent", "name": "aegis", "content": "Summary"}

                        with patch('aegis.model.agents.summarizer.synthesize_responses', mock_summarizer):
                            start_time = time.time()
                            responses = []

                            async for chunk in model(
                                {"messages": [{"role": "user", "content": "Get all data"}]},
                                db_names=["transcripts", "reports", "rts"]
                            ):
                                responses.append(chunk)

                            elapsed = time.time() - start_time

        # Verify concurrent execution
        assert len(execution_times) == 3, "All 3 subagents should have executed"

        # Calculate overlap - if running concurrently, they should overlap
        times = list(execution_times.values())
        latest_start = max(t["start"] for t in times)
        earliest_end = min(t["end"] for t in times)

        # There should be significant overlap (at least 0.5s of the 1s execution)
        overlap = earliest_end - latest_start
        assert overlap > 0.5, f"Subagents not running concurrently enough. Overlap: {overlap}s"

        # Total time should be close to 1s (concurrent) not 3s (sequential)
        assert elapsed < 2.0, f"Took {elapsed}s - subagents appear to run sequentially"

    @pytest.mark.asyncio
    async def test_async_database_operations(self):
        """Test async database operations in the model."""
        from aegis.connections.postgres_connector import get_connection

        # Mock the async database connection
        mock_conn = AsyncMock()
        mock_result = AsyncMock()
        mock_result.fetchone.return_value = {
            "bank_id": 1,
            "bank_name": "Royal Bank of Canada",
            "bank_symbol": "RY",
            "fiscal_year": 2024,
            "quarter": "Q3",
            "database_names": ["transcripts", "reports"]
        }
        mock_conn.execute.return_value = mock_result

        with patch('aegis.connections.postgres_connector.get_connection') as mock_get_conn:
            # Make it an async context manager
            mock_get_conn.return_value.__aenter__.return_value = mock_conn
            mock_get_conn.return_value.__aexit__.return_value = None

            # Test clarifier which uses database
            from aegis.model.agents.clarifier import clarify_query

            with patch('aegis.connections.llm_connector.complete_with_tools', new_callable=AsyncMock) as mock_llm:
                mock_llm.return_value = {
                    "choices": [{
                        "message": {
                            "tool_calls": [{
                                "function": {
                                    "arguments": '{"entities": [{"bank": "Royal Bank of Canada", "periods": [{"year": 2024, "quarter": "Q3"}]}]}'
                                }
                            }]
                        }
                    }]
                }

                result = await clarify_query(
                    conversation=[{"role": "user", "content": "RBC Q3 2024 revenue"}],
                    latest_message="RBC Q3 2024 revenue",
                    available_databases=["transcripts"],
                    context={"execution_id": "test-123", "auth_config": {}, "ssl_config": {}}
                )

                assert result["status"] == "success"
                assert len(result["bank_period_combinations"]) > 0

    @pytest.mark.asyncio
    async def test_streaming_consistency(self):
        """Test that streaming yields consistent message format."""
        conversation = {
            "messages": [{"role": "user", "content": "Hello"}]
        }

        # Collect all message types
        message_types = set()
        message_names = set()

        with patch('aegis.model.agents.router.route_query', new_callable=AsyncMock) as mock_router:
            mock_router.return_value = {
                "status": "success",
                "route": "direct_response"
            }

            async def mock_response(*args, **kwargs):
                yield {"type": "agent", "name": "aegis", "content": "Hello"}
                yield {"type": "agent", "name": "aegis", "content": " there!"}

            with patch('aegis.model.agents.response.generate_response', mock_response):
                async for chunk in model(conversation):
                    # Verify message structure
                    assert "type" in chunk, "Message must have 'type' field"
                    assert "name" in chunk, "Message must have 'name' field"

                    message_types.add(chunk["type"])
                    message_names.add(chunk["name"])

                    # Content is optional for control messages
                    if chunk["type"] in ["agent", "subagent"]:
                        assert "content" in chunk, f"{chunk['type']} messages must have content"

        # Verify we got expected message types
        assert "agent" in message_types

    @pytest.mark.asyncio
    async def test_error_handling_in_async_flow(self):
        """Test error handling in async workflow."""
        conversation = {
            "messages": [{"role": "user", "content": "Get data"}]
        }

        with patch('aegis.model.agents.router.route_query', new_callable=AsyncMock) as mock_router:
            # Router throws an error
            mock_router.side_effect = Exception("Router failed")

            responses = []
            async for chunk in model(conversation):
                responses.append(chunk)

            # Should handle error gracefully and return error message
            assert len(responses) > 0
            error_messages = [r.get("content", "") for r in responses]
            assert any("error" in msg.lower() or "⚠" in msg for msg in error_messages)

    @pytest.mark.asyncio
    async def test_semaphore_concurrency_limit(self):
        """Test that semaphore limits concurrent subagent execution."""
        from aegis.model.subagents import SUBAGENT_MAPPING

        concurrent_count = 0
        max_concurrent = 0

        async def mock_subagent(*args, **kwargs):
            """Track concurrent executions."""
            nonlocal concurrent_count, max_concurrent
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0.1)  # Simulate work
            concurrent_count -= 1
            yield {"type": "subagent", "name": kwargs.get("database_id", "test"), "content": "Done"}

        # Create many fake databases to test semaphore
        mock_databases = {f"db_{i}": mock_subagent for i in range(10)}

        with patch.dict(SUBAGENT_MAPPING, mock_databases):
            with patch('aegis.model.agents.router.route_query', new_callable=AsyncMock) as mock_router:
                mock_router.return_value = {"status": "success", "route": "research_workflow"}

                with patch('aegis.model.agents.clarifier.clarify_query', new_callable=AsyncMock) as mock_clarifier:
                    mock_clarifier.return_value = {
                        "status": "success",
                        "bank_period_combinations": [{"bank_id": 1, "fiscal_year": 2024, "quarter": "Q3"}],
                        "clarifier_intent": "Test"
                    }

                    with patch('aegis.model.agents.planner.plan_database_queries', new_callable=AsyncMock) as mock_planner:
                        # Plan to query all 10 databases
                        mock_planner.return_value = {
                            "status": "success",
                            "databases": list(mock_databases.keys())
                        }

                        async def mock_summarizer(*args, **kwargs):
                            yield {"type": "agent", "name": "aegis", "content": "Summary"}

                        with patch('aegis.model.agents.summarizer.synthesize_responses', mock_summarizer):
                            async for _ in model(
                                {"messages": [{"role": "user", "content": "Get all data"}]},
                                db_names=list(mock_databases.keys())
                            ):
                                pass

        # Semaphore is set to 5 in main.py
        assert max_concurrent <= 5, f"Max concurrent was {max_concurrent}, should be ≤ 5 (semaphore limit)"


class TestETLAsync:
    """Test ETL module async functions."""

    @pytest.mark.asyncio
    async def test_get_bank_info_async(self):
        """Test async get_bank_info function."""
        from aegis.etls.call_summary.main import get_bank_info

        # Mock async database connection
        mock_result = MagicMock()
        mock_result._asdict.return_value = {
            "bank_id": 1,
            "bank_name": "Royal Bank of Canada",
            "bank_symbol": "RY"
        }

        mock_conn = AsyncMock()
        mock_exec_result = AsyncMock()
        mock_exec_result.fetchone.return_value = mock_result
        mock_conn.execute.return_value = mock_exec_result

        with patch('aegis.etls.call_summary.main.get_connection') as mock_get_conn:
            mock_get_conn.return_value.__aenter__.return_value = mock_conn
            mock_get_conn.return_value.__aexit__.return_value = None

            result = await get_bank_info("RY")

            assert result["bank_id"] == 1
            assert result["bank_symbol"] == "RY"
            assert result["bank_name"] == "Royal Bank of Canada"

    @pytest.mark.asyncio
    async def test_verify_data_availability_async(self):
        """Test async verify_data_availability function."""
        from aegis.etls.call_summary.main import verify_data_availability

        # Mock result with transcripts available
        mock_result = {"database_names": ["transcripts", "reports"]}

        mock_conn = AsyncMock()
        mock_exec_result = AsyncMock()
        mock_exec_result.fetchone.return_value = mock_result
        mock_conn.execute.return_value = mock_exec_result

        with patch('aegis.etls.call_summary.main.get_connection') as mock_get_conn:
            mock_get_conn.return_value.__aenter__.return_value = mock_conn
            mock_get_conn.return_value.__aexit__.return_value = None

            result = await verify_data_availability(1, 2024, "Q3")

            assert result is True

    @pytest.mark.asyncio
    async def test_generate_call_summary_async(self):
        """Test async generate_call_summary with mocked dependencies."""
        from aegis.etls.call_summary.main import generate_call_summary

        # Mock get_bank_info
        with patch('aegis.etls.call_summary.main.get_bank_info', new_callable=AsyncMock) as mock_get_bank:
            mock_get_bank.return_value = {
                "bank_id": 1,
                "bank_name": "Royal Bank of Canada",
                "bank_symbol": "RY"
            }

            # Mock verify_data_availability
            with patch('aegis.etls.call_summary.main.verify_data_availability', new_callable=AsyncMock) as mock_verify:
                mock_verify.return_value = True

                # Mock setup_authentication
                with patch('aegis.etls.call_summary.main.setup_authentication', new_callable=AsyncMock) as mock_auth:
                    mock_auth.return_value = {"success": True, "token": "test-token"}

                    # Mock complete_with_tools for LLM calls
                    with patch('aegis.etls.call_summary.main.complete_with_tools', new_callable=AsyncMock) as mock_llm:
                        mock_llm.return_value = {
                            "choices": [{
                                "message": {
                                    "tool_calls": [{
                                        "function": {
                                            "arguments": '{"categories": ["Revenue"]}'
                                        }
                                    }]
                                }
                            }]
                        }

                        # Mock other dependencies to avoid actual processing
                        with patch('aegis.etls.call_summary.main.load_categories_from_xlsx'):
                            with patch('aegis.etls.call_summary.main.retrieve_full_section'):
                                result = await generate_call_summary("RY", 2024, "Q3")

                                # Should return some result (even if mocked)
                                assert result is not None
                                mock_get_bank.assert_called_once_with("RY")
                                mock_verify.assert_called_once_with(1, 2024, "Q3")


if __name__ == "__main__":
    # Run specific tests for debugging
    pytest.main([__file__, "-xvs"])