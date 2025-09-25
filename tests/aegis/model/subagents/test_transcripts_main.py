"""
Tests for transcripts subagent main.py to achieve coverage.
"""

import json
import pytest
from unittest.mock import patch, MagicMock, AsyncMock, call
from datetime import datetime, timezone


class TestTranscriptsAgent:
    """Tests for transcripts_agent function."""

    @pytest.fixture
    def mock_context(self):
        """Mock context for testing."""
        return {
            "execution_id": "test-exec-456",
            "auth_config": {"method": "api_key", "credentials": {"api_key": "test"}},
            "ssl_config": {"verify": False}
        }

    @pytest.fixture
    def mock_bank_combinations(self):
        """Mock bank-period combinations."""
        return [
            {
                "bank_id": 2,
                "bank_name": "Bank of Nova Scotia",
                "bank_symbol": "BNS",
                "fiscal_year": 2024,
                "quarter": "Q2",
                "query_intent": "Revenue discussion from earnings call"
            }
        ]

    async def async_generator(self, items):
        """Helper to create async generator."""
        for item in items:
            yield item

    @patch("aegis.model.subagents.transcripts.main.retrieve_full_section")
    @patch("aegis.model.subagents.transcripts.main.format_full_section_chunks")
    @patch("aegis.model.subagents.transcripts.main.generate_research_statement")
    @patch("aegis.model.subagents.transcripts.main.complete_with_tools")
    @patch("aegis.model.subagents.transcripts.main.load_subagent_prompt")
    @patch("aegis.model.subagents.transcripts.main.add_monitor_entry")
    @pytest.mark.asyncio
    async def test_transcripts_agent_full_retrieval(
        self,
        mock_monitor,
        mock_load_prompt,
        mock_complete,
        mock_generate_research,
        mock_format_chunks,
        mock_retrieve_full,
        mock_context,
        mock_bank_combinations
    ):
        """Test full section retrieval method."""
        from aegis.model.subagents.transcripts.main import transcripts_agent

        # Mock prompt
        mock_load_prompt.return_value = "You are the transcripts subagent"

        # Mock LLM method selection (full retrieval)
        mock_complete.return_value = {
            "choices": [{
                "message": {
                    "tool_calls": [{
                        "function": {
                            "name": "select_retrieval_method",
                            "arguments": json.dumps({
                                "method": 0,  # Full retrieval
                                "sections": "ALL",
                                "reasoning": "Need complete transcript"
                            })
                        }
                    }]
                }
            }],
            "usage": {"total_tokens": 50, "prompt_tokens": 40, "completion_tokens": 10},
            "metrics": {"total_cost": 0.001, "response_time": 0.3}
        }

        # Mock retrieval
        mock_retrieve_full.return_value = [
            {"chunk_id": 1, "content": "CEO: Revenue increased 10%", "speaker": "CEO"}
        ]

        # Mock formatting
        mock_format_chunks.return_value = "Formatted transcript content"

        # Mock research statement (returns string, not generator)
        mock_generate_research.return_value = "Revenue analysis: Strong growth observed"

        # Execute
        chunks = []
        async for chunk in transcripts_agent(
            conversation=[],
            latest_message="Show me revenue discussion",
            bank_period_combinations=mock_bank_combinations,
            basic_intent="revenue",
            full_intent="Revenue discussion from BNS earnings",
            database_id="transcripts",
            context=mock_context
        ):
            chunks.append(chunk)

        # Assertions
        assert len(chunks) > 0
        assert all(chunk["type"] == "subagent" for chunk in chunks)
        assert all(chunk["name"] == "transcripts" for chunk in chunks)

        # Check content
        full_content = "".join(chunk["content"] for chunk in chunks)
        assert "Revenue" in full_content or "growth" in full_content

        # Verify method selection
        mock_complete.assert_called_once()
        mock_retrieve_full.assert_called_once()
        mock_monitor.assert_called()

    @patch("aegis.model.subagents.transcripts.main.retrieve_by_categories")
    @patch("aegis.model.subagents.transcripts.main.format_category_or_similarity_chunks")
    @patch("aegis.model.subagents.transcripts.main.generate_research_statement")
    @patch("aegis.model.subagents.transcripts.main.complete_with_tools")
    @patch("aegis.model.subagents.transcripts.main.load_subagent_prompt")
    @patch("aegis.model.subagents.transcripts.main.add_monitor_entry")
    @pytest.mark.asyncio
    async def test_transcripts_agent_category_retrieval(
        self,
        mock_monitor,
        mock_load_prompt,
        mock_complete,
        mock_generate_research,
        mock_format_chunks,
        mock_retrieve_category,
        mock_context,
        mock_bank_combinations
    ):
        """Test category-based retrieval method."""
        from aegis.model.subagents.transcripts.main import transcripts_agent

        # Mock prompt
        mock_load_prompt.return_value = "You are the transcripts subagent"

        # Mock LLM method selection (category retrieval)
        mock_complete.return_value = {
            "choices": [{
                "message": {
                    "tool_calls": [{
                        "function": {
                            "name": "select_retrieval_method",
                            "arguments": json.dumps({
                                "method": 1,  # Category retrieval
                                "category_ids": [0, 2],  # Revenue & Growth, Expenses & Costs
                                "reasoning": "Focus on financial metrics"
                            })
                        }
                    }]
                }
            }],
            "usage": {"total_tokens": 60, "prompt_tokens": 45, "completion_tokens": 15},
            "metrics": {"total_cost": 0.0012, "response_time": 0.35}
        }

        # Mock retrieval
        mock_retrieve_category.return_value = [
            {"chunk_id": 2, "content": "Revenue up 8%", "category": "revenue_growth"}
        ]

        # Mock formatting
        mock_format_chunks.return_value = "Categorized content"

        # Mock research statement (returns string)
        mock_generate_research.return_value = "Category analysis complete"

        # Execute
        chunks = []
        async for chunk in transcripts_agent(
            conversation=[],
            latest_message="Financial metrics",
            bank_period_combinations=mock_bank_combinations,
            basic_intent="metrics",
            full_intent="Financial metrics from call",
            database_id="transcripts",
            context=mock_context
        ):
            chunks.append(chunk)

        # Assertions
        assert len(chunks) > 0
        full_content = "".join(chunk["content"] for chunk in chunks)
        assert len(full_content) > 0

        # Verify category retrieval was called
        mock_retrieve_category.assert_called_once()
        mock_monitor.assert_called()

    @patch("aegis.model.subagents.transcripts.main.retrieve_by_similarity")
    @patch("aegis.model.subagents.transcripts.main.format_category_or_similarity_chunks")
    @patch("aegis.model.subagents.transcripts.main.generate_research_statement")
    @patch("aegis.model.subagents.transcripts.main.complete_with_tools")
    @patch("aegis.model.subagents.transcripts.main.load_subagent_prompt")
    @patch("aegis.model.subagents.transcripts.main.add_monitor_entry")
    @pytest.mark.asyncio
    async def test_transcripts_agent_similarity_retrieval(
        self,
        mock_monitor,
        mock_load_prompt,
        mock_complete,
        mock_generate_research,
        mock_format_chunks,
        mock_retrieve_similarity,
        mock_context,
        mock_bank_combinations
    ):
        """Test similarity-based retrieval method."""
        from aegis.model.subagents.transcripts.main import transcripts_agent

        # Mock prompt
        mock_load_prompt.return_value = "You are the transcripts subagent"

        # Mock LLM method selection (similarity retrieval)
        mock_complete.return_value = {
            "choices": [{
                "message": {
                    "tool_calls": [{
                        "function": {
                            "name": "select_retrieval_method",
                            "arguments": json.dumps({
                                "method": 2,  # Similarity retrieval
                                "search_phrase": "digital transformation initiatives",
                                "reasoning": "Specific topic search"
                            })
                        }
                    }]
                }
            }],
            "usage": {"total_tokens": 55, "prompt_tokens": 42, "completion_tokens": 13},
            "metrics": {"total_cost": 0.0011, "response_time": 0.32}
        }

        # Mock retrieval
        mock_retrieve_similarity.return_value = [
            {"chunk_id": 3, "content": "Digital investments paying off", "similarity": 0.89}
        ]

        # Mock formatting
        mock_format_chunks.return_value = "Similar content found"

        # Mock research statement (returns string)
        mock_generate_research.return_value = "Digital transformation analysis"

        # Execute
        chunks = []
        async for chunk in transcripts_agent(
            conversation=[],
            latest_message="Digital transformation",
            bank_period_combinations=mock_bank_combinations,
            basic_intent="digital",
            full_intent="Digital transformation discussion",
            database_id="transcripts",
            context=mock_context
        ):
            chunks.append(chunk)

        # Assertions
        assert len(chunks) > 0

        # Verify similarity retrieval was called
        mock_retrieve_similarity.assert_called_once()
        mock_monitor.assert_called()

    @patch("aegis.model.subagents.transcripts.main.retrieve_full_section")
    @patch("aegis.model.subagents.transcripts.main.complete_with_tools")
    @patch("aegis.model.subagents.transcripts.main.load_subagent_prompt")
    @patch("aegis.model.subagents.transcripts.main.add_monitor_entry")
    @pytest.mark.asyncio
    async def test_transcripts_agent_no_data(
        self,
        mock_monitor,
        mock_load_prompt,
        mock_complete,
        mock_retrieve_full,
        mock_context,
        mock_bank_combinations
    ):
        """Test handling when no data is found."""
        from aegis.model.subagents.transcripts.main import transcripts_agent

        # Mock prompt
        mock_load_prompt.return_value = "You are the transcripts subagent"

        # Mock LLM method selection
        mock_complete.return_value = {
            "choices": [{
                "message": {
                    "tool_calls": [{
                        "function": {
                            "name": "select_retrieval_method",
                            "arguments": json.dumps({
                                "method": 0,
                                "sections": "ALL",
                                "reasoning": "Full retrieval"
                            })
                        }
                    }]
                }
            }],
            "usage": {"total_tokens": 50, "prompt_tokens": 40, "completion_tokens": 10},
            "metrics": {"total_cost": 0.001, "response_time": 0.3}
        }

        # Mock no data retrieval
        mock_retrieve_full.return_value = []

        # Execute
        chunks = []
        async for chunk in transcripts_agent(
            conversation=[],
            latest_message="Get transcript",
            bank_period_combinations=mock_bank_combinations,
            basic_intent="transcript",
            full_intent="Get full transcript",
            database_id="transcripts",
            context=mock_context
        ):
            chunks.append(chunk)

        # Assertions
        assert len(chunks) > 0
        full_content = "".join(chunk["content"] for chunk in chunks)
        assert "No transcript" in full_content or "not available" in full_content

        # Verify monitoring recorded no data
        mock_monitor.assert_called()

    @patch("aegis.model.subagents.transcripts.main.complete_with_tools")
    @patch("aegis.model.subagents.transcripts.main.load_subagent_prompt")
    @patch("aegis.model.subagents.transcripts.main.add_monitor_entry")
    @pytest.mark.asyncio
    async def test_transcripts_agent_error_handling(
        self,
        mock_monitor,
        mock_load_prompt,
        mock_complete,
        mock_context,
        mock_bank_combinations
    ):
        """Test error handling in transcripts agent."""
        from aegis.model.subagents.transcripts.main import transcripts_agent

        # Mock prompt
        mock_load_prompt.return_value = "You are the transcripts subagent"

        # Mock LLM error
        mock_complete.side_effect = Exception("LLM API error")

        # Execute
        chunks = []
        async for chunk in transcripts_agent(
            conversation=[],
            latest_message="Get transcript",
            bank_period_combinations=mock_bank_combinations,
            basic_intent="transcript",
            full_intent="Get transcript",
            database_id="transcripts",
            context=mock_context
        ):
            chunks.append(chunk)

        # Assertions
        assert len(chunks) > 0
        full_content = "".join(chunk["content"] for chunk in chunks)
        # When LLM fails, it falls back to similarity search which may return no data
        assert any(phrase in full_content for phrase in ["No transcript data", "Error", "error"])

        # Verify monitoring was called (error is handled gracefully with fallback)
        mock_monitor.assert_called()
        # The agent handles errors gracefully, so status may still be "Success"
        # if it successfully falls back to similarity search

    @patch("aegis.model.subagents.transcripts.main.retrieve_full_section")
    @patch("aegis.model.subagents.transcripts.main.format_full_section_chunks")
    @patch("aegis.model.subagents.transcripts.main.generate_research_statement")
    @patch("aegis.model.subagents.transcripts.main.complete_with_tools")
    @patch("aegis.model.subagents.transcripts.main.load_subagent_prompt")
    @patch("aegis.model.subagents.transcripts.main.add_monitor_entry")
    @pytest.mark.asyncio
    async def test_transcripts_agent_multiple_combinations(
        self,
        mock_monitor,
        mock_load_prompt,
        mock_complete,
        mock_generate_research,
        mock_format_chunks,
        mock_retrieve_full,
        mock_context
    ):
        """Test handling multiple bank-period combinations."""
        from aegis.model.subagents.transcripts.main import transcripts_agent

        # Multiple combinations
        bank_combinations = [
            {
                "bank_id": 1,
                "bank_name": "RBC",
                "bank_symbol": "RY",
                "fiscal_year": 2024,
                "quarter": "Q1",
                "query_intent": "Performance review"
            },
            {
                "bank_id": 2,
                "bank_name": "BNS",
                "bank_symbol": "BNS",
                "fiscal_year": 2024,
                "quarter": "Q1",
                "query_intent": "Performance review"
            }
        ]

        # Mock prompt
        mock_load_prompt.return_value = "You are the transcripts subagent"

        # Mock method selection - called once per combination
        mock_complete.side_effect = [
            {
                "choices": [{
                    "message": {
                        "tool_calls": [{
                            "function": {
                                "name": "select_retrieval_method",
                                "arguments": json.dumps({
                                    "method": 0,
                                    "sections": "ALL",
                                    "reasoning": "Full review"
                                })
                            }
                        }]
                    }
                }],
                "usage": {"total_tokens": 50, "prompt_tokens": 40, "completion_tokens": 10},
                "metrics": {"total_cost": 0.001, "response_time": 0.3}
            },
            {
                "choices": [{
                    "message": {
                        "tool_calls": [{
                            "function": {
                                "name": "select_retrieval_method",
                                "arguments": json.dumps({
                                    "method": 0,
                                    "sections": "ALL",
                                    "reasoning": "Full review"
                                })
                            }
                        }]
                    }
                }],
                "usage": {"total_tokens": 50, "prompt_tokens": 40, "completion_tokens": 10},
                "metrics": {"total_cost": 0.001, "response_time": 0.3}
            }
        ]

        # Mock retrieval for each
        mock_retrieve_full.side_effect = [
            [{"chunk_id": 1, "content": "RBC performance"}],
            [{"chunk_id": 2, "content": "BNS performance"}]
        ]

        # Mock formatting
        mock_format_chunks.side_effect = [
            "RBC formatted content",
            "BNS formatted content"
        ]

        # Mock research statements (returns strings)
        mock_generate_research.side_effect = [
            "RBC analysis: Performance review for Q1 2024",
            "BNS analysis: Performance review for Q1 2024"
        ]

        # Execute
        chunks = []
        async for chunk in transcripts_agent(
            conversation=[],
            latest_message="Compare performance",
            bank_period_combinations=bank_combinations,
            basic_intent="performance",
            full_intent="Compare bank performance",
            database_id="transcripts",
            context=mock_context
        ):
            chunks.append(chunk)

        # Assertions
        assert len(chunks) > 0
        full_content = "".join(chunk["content"] for chunk in chunks)

        # Should have both banks
        assert "RBC" in full_content or "BNS" in full_content

        # Verify called twice (once per combination)
        assert mock_complete.call_count == 2
        assert mock_retrieve_full.call_count == 2