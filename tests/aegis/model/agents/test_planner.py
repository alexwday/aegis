"""
Tests for the planner agent module.
"""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy import create_engine

from aegis.model.agents.planner import (
    get_filtered_availability_table,
    get_filtered_database_descriptions,
    plan_database_queries
)


class TestGetFilteredAvailabilityTable:
    """
    Tests for the get_filtered_availability_table function.
    """
    
    @patch('aegis.model.agents.planner._get_engine')
    def test_get_filtered_availability_table_success(self, mock_get_engine):
        """
        Test successful retrieval of filtered availability table.
        """
        # Mock database results
        mock_conn = Mock()
        mock_result = [
            (1, "Royal Bank", "RY", 2024, "Q1", ["benchmarking", "transcripts"]),
            (1, "Royal Bank", "RY", 2024, "Q2", ["benchmarking"]),
            (2, "TD Bank", "TD", 2024, "Q1", ["reports", "rts"])
        ]
        mock_conn.execute.return_value = mock_result
        mock_engine = Mock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        mock_get_engine.return_value = mock_engine
        
        # Test with apply_all periods
        bank_ids = [1, 2]
        periods = {
            "apply_all": {
                "fiscal_year": 2024,
                "quarters": ["Q1", "Q2"]
            }
        }
        
        result = get_filtered_availability_table(bank_ids, periods)
        
        assert result["availability"]["1"]["name"] == "Royal Bank"
        assert result["availability"]["1"]["symbol"] == "RY"
        assert len(result["availability"]["1"]["periods"]) == 2
        assert "benchmarking" in result["available_databases"]
        assert "transcripts" in result["available_databases"]
        assert "<availability_table>" in result["table"]
    
    @patch('aegis.model.agents.planner._get_engine')
    def test_get_filtered_availability_table_with_database_filter(self, mock_get_engine):
        """
        Test filtering with available_databases parameter.
        """
        mock_conn = Mock()
        mock_result = [
            (1, "Royal Bank", "RY", 2024, "Q1", ["benchmarking", "transcripts", "reports"])
        ]
        mock_conn.execute.return_value = mock_result
        mock_engine = Mock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        mock_get_engine.return_value = mock_engine
        
        bank_ids = [1]
        periods = {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}
        available_databases = ["benchmarking", "reports"]  # Filter out transcripts
        
        result = get_filtered_availability_table(bank_ids, periods, available_databases)
        
        assert "benchmarking" in result["available_databases"]
        assert "reports" in result["available_databases"]
        assert "transcripts" not in result["available_databases"]
    
    @patch('aegis.model.agents.planner._get_engine')
    def test_get_filtered_availability_table_bank_specific_periods(self, mock_get_engine):
        """
        Test with bank-specific periods.
        """
        mock_conn = Mock()
        mock_result = [
            (1, "Royal Bank", "RY", 2024, "Q1", ["benchmarking"]),
            (1, "Royal Bank", "RY", 2023, "Q4", ["benchmarking"]),
            (2, "TD Bank", "TD", 2024, "Q2", ["reports"])
        ]
        mock_conn.execute.return_value = mock_result
        mock_engine = Mock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        mock_get_engine.return_value = mock_engine
        
        bank_ids = [1, 2]
        periods = {
            "1": {"fiscal_year": 2024, "quarters": ["Q1"]},
            "2": {"fiscal_year": 2024, "quarters": ["Q2"]}
        }
        
        result = get_filtered_availability_table(bank_ids, periods)
        
        assert "1" in result["availability"]
        assert "2" in result["availability"]
        assert len(result["availability"]["1"]["periods"]) == 1
        assert result["availability"]["1"]["periods"][0]["quarter"] == "Q1"
    
    @patch('aegis.model.agents.planner._get_engine')
    def test_get_filtered_availability_table_no_matches(self, mock_get_engine):
        """
        Test when no data matches the requested periods.
        """
        mock_conn = Mock()
        mock_result = []
        mock_conn.execute.return_value = mock_result
        mock_engine = Mock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        mock_get_engine.return_value = mock_engine
        
        bank_ids = [1]
        periods = {"apply_all": {"fiscal_year": 2025, "quarters": ["Q1"]}}
        
        result = get_filtered_availability_table(bank_ids, periods)
        
        assert result["availability"] == {}
        assert result["available_databases"] == []
        assert "No data available" not in result["table"]
    
    @patch('aegis.model.agents.planner.get_logger')
    @patch('aegis.model.agents.planner._get_engine')
    def test_get_filtered_availability_table_exception(self, mock_get_engine, mock_logger):
        """
        Test exception handling in database connection.
        """
        # Mock the engine to raise exception on connect
        mock_engine = Mock()
        mock_engine.connect.side_effect = Exception("Database error")
        mock_get_engine.return_value = mock_engine
        
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        bank_ids = [1]
        periods = {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}
        
        result = get_filtered_availability_table(bank_ids, periods)
        
        assert result["availability"] == {}
        assert result["available_databases"] == []
        assert "No data available" in result["table"]
        mock_logger_instance.error.assert_called_once()


class TestGetFilteredDatabaseDescriptions:
    """
    Tests for the get_filtered_database_descriptions function.
    """
    
    @patch('aegis.model.agents.planner.load_yaml')
    def test_get_filtered_database_descriptions_success(self, mock_load_yaml):
        """
        Test successful filtering of database descriptions.
        """
        mock_load_yaml.return_value = {
            "databases": [
                {"id": "benchmarking", "name": "Benchmarking DB", "content": "Benchmarking data"},
                {"id": "reports", "name": "Reports DB", "content": "Reports data"},
                {"id": "transcripts", "name": "Transcripts DB", "content": "Transcripts data"}
            ]
        }
        
        available_databases = ["benchmarking", "reports"]
        
        result = get_filtered_database_descriptions(available_databases)
        
        assert "<database_descriptions>" in result
        assert "benchmarking" in result
        assert "reports" in result
        assert "transcripts" not in result
        assert "</database_descriptions>" in result
    
    @patch('aegis.model.agents.planner.load_yaml')
    def test_get_filtered_database_descriptions_no_databases_key(self, mock_load_yaml):
        """
        Test when databases key is missing from YAML.
        """
        mock_load_yaml.return_value = {}
        
        result = get_filtered_database_descriptions(["benchmarking"])
        
        assert result == ""
    
    @patch('aegis.model.agents.planner.load_yaml')
    @patch('aegis.model.agents.planner.get_logger')
    def test_get_filtered_database_descriptions_exception(self, mock_logger, mock_load_yaml):
        """
        Test exception handling.
        """
        mock_load_yaml.side_effect = Exception("YAML error")
        
        result = get_filtered_database_descriptions(["benchmarking"])
        
        assert result == ""
        mock_logger().error.assert_called_once()


class TestPlanDatabaseQueries:
    """
    Tests for the plan_database_queries function.
    """
    
    @patch('aegis.model.agents.planner.complete_with_tools')
    @patch('aegis.model.agents.planner.get_filtered_database_descriptions')
    @patch('aegis.model.agents.planner.get_filtered_availability_table')
    @patch('aegis.model.agents.planner.load_yaml')
    def test_plan_database_queries_success(
        self, mock_load_yaml, mock_get_availability, mock_get_descriptions, mock_complete
    ):
        """
        Test successful database query planning.
        """
        # Setup mocks
        mock_get_availability.return_value = {
            "availability": {"1": {"name": "RBC", "periods": []}},
            "available_databases": ["benchmarking", "reports"],
            "table": "<availability_table>Test</availability_table>"
        }
        mock_get_descriptions.return_value = "<database_descriptions>Test</database_descriptions>"
        mock_load_yaml.return_value = {"content": "Planner instructions"}
        
        # Mock LLM response
        mock_complete.return_value = {
            "choices": [{
                "message": {
                    "tool_calls": [{
                        "function": {
                            "name": "databases_selected",
                            "arguments": json.dumps({
                                "databases": [
                                    {"database_id": "benchmarking", "query_intent": "Get efficiency ratio"},
                                    {"database_id": "reports", "query_intent": "Get revenue data"}
                                ]
                            })
                        }
                    }]
                }
            }],
            "metrics": {"total_cost": 0.05},
            "usage": {"total_tokens": 1000}
        }
        
        # Test inputs
        query = "Show me efficiency ratio"
        conversation = [{"role": "user", "content": "Hello"}]
        banks = {"bank_ids": [1]}
        periods = {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}
        context = {"execution_id": "test-123"}
        
        result = plan_database_queries(query, conversation, banks, periods, context)
        
        assert result["status"] == "success"
        assert len(result["databases"]) == 2
        assert result["databases"][0]["database_id"] == "benchmarking"
        assert result["tokens_used"] == 1000
        assert result["cost"] == 0.05
    
    @patch('aegis.model.agents.planner.complete_with_tools')
    @patch('aegis.model.agents.planner.get_filtered_database_descriptions')
    @patch('aegis.model.agents.planner.get_filtered_availability_table')
    @patch('aegis.model.agents.planner.load_yaml')
    def test_plan_database_queries_with_filtering(
        self, mock_load_yaml, mock_get_availability, mock_get_descriptions, mock_complete
    ):
        """
        Test that planner filters databases to only available ones.
        """
        mock_get_availability.return_value = {
            "availability": {"1": {"name": "RBC", "periods": []}},
            "available_databases": ["benchmarking"],  # Only benchmarking available
            "table": "<availability_table>Test</availability_table>"
        }
        mock_get_descriptions.return_value = "<database_descriptions>Test</database_descriptions>"
        mock_load_yaml.return_value = {"content": "Planner instructions"}
        
        # LLM tries to select both benchmarking and reports
        mock_complete.return_value = {
            "choices": [{
                "message": {
                    "tool_calls": [{
                        "function": {
                            "name": "databases_selected",
                            "arguments": json.dumps({
                                "databases": [
                                    {"database_id": "benchmarking", "query_intent": "Get data"},
                                    {"database_id": "reports", "query_intent": "Get reports"}  # Will be filtered
                                ]
                            })
                        }
                    }]
                }
            }],
            "metrics": {"total_cost": 0.05},
            "usage": {"total_tokens": 1000}
        }
        
        query = "Show me data"
        conversation = []
        banks = {"bank_ids": [1]}
        periods = {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}
        context = {"execution_id": "test-123"}
        
        result = plan_database_queries(query, conversation, banks, periods, context)
        
        assert result["status"] == "success"
        assert len(result["databases"]) == 1  # Only benchmarking included
        assert result["databases"][0]["database_id"] == "benchmarking"
    
    @patch('aegis.model.agents.planner.complete_with_tools')
    @patch('aegis.model.agents.planner.get_filtered_database_descriptions')
    @patch('aegis.model.agents.planner.get_filtered_availability_table')
    @patch('aegis.model.agents.planner.load_yaml')
    def test_plan_database_queries_no_databases_needed(
        self, mock_load_yaml, mock_get_availability, mock_get_descriptions, mock_complete
    ):
        """
        Test when no databases are needed.
        """
        mock_get_availability.return_value = {
            "availability": {},
            "available_databases": ["benchmarking"],
            "table": "<availability_table>Test</availability_table>"
        }
        mock_get_descriptions.return_value = "<database_descriptions>Test</database_descriptions>"
        mock_load_yaml.return_value = {"content": "Planner instructions"}
        
        mock_complete.return_value = {
            "choices": [{
                "message": {
                    "tool_calls": [{
                        "function": {
                            "name": "no_databases_needed",
                            "arguments": json.dumps({
                                "reason": "Query is a greeting"
                            })
                        }
                    }]
                }
            }],
            "metrics": {"total_cost": 0.05},
            "usage": {"total_tokens": 500}
        }
        
        query = "Hello"
        conversation = []
        banks = {"bank_ids": [1]}
        periods = {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}
        context = {"execution_id": "test-123"}
        
        result = plan_database_queries(query, conversation, banks, periods, context)
        
        assert result["status"] == "no_databases"
        assert result["reason"] == "Query is a greeting"
        assert result["databases"] == []
    
    @patch('aegis.model.agents.planner.get_filtered_availability_table')
    def test_plan_database_queries_no_banks(self, mock_get_availability):
        """
        Test when no banks are provided.
        """
        query = "Show me data"
        conversation = []
        banks = {"bank_ids": []}  # Empty bank list
        periods = {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}
        context = {"execution_id": "test-123"}
        
        result = plan_database_queries(query, conversation, banks, periods, context)
        
        assert result["status"] == "error"
        assert "No banks provided" in result["error"]
    
    @patch('aegis.model.agents.planner.get_filtered_availability_table')
    def test_plan_database_queries_no_periods(self, mock_get_availability):
        """
        Test when no periods are provided.
        """
        query = "Show me data"
        conversation = []
        banks = {"bank_ids": [1]}
        periods = {}  # Empty periods
        context = {"execution_id": "test-123"}
        
        result = plan_database_queries(query, conversation, banks, periods, context)
        
        assert result["status"] == "error"
        assert "No periods provided" in result["error"]
    
    @patch('aegis.model.agents.planner.get_filtered_availability_table')
    def test_plan_database_queries_no_available_data(self, mock_get_availability):
        """
        Test when no databases have data for requested banks/periods.
        """
        mock_get_availability.return_value = {
            "availability": {},
            "available_databases": [],  # No available databases
            "table": "<availability_table>No data</availability_table>"
        }
        
        query = "Show me data"
        conversation = []
        banks = {"bank_ids": [1]}
        periods = {"periods": {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}}
        context = {"execution_id": "test-123"}
        
        result = plan_database_queries(query, conversation, banks, periods, context)
        
        assert result["status"] == "no_data"
        assert "No databases have data" in result["message"]
        assert result["databases"] == []
    
    @patch('aegis.model.agents.planner.complete_with_tools')
    @patch('aegis.model.agents.planner.get_filtered_database_descriptions')
    @patch('aegis.model.agents.planner.get_filtered_availability_table')
    @patch('aegis.model.agents.planner.load_yaml')
    def test_plan_database_queries_with_query_intent(
        self, mock_load_yaml, mock_get_availability, mock_get_descriptions, mock_complete
    ):
        """
        Test planning with query intent from clarifier.
        """
        mock_get_availability.return_value = {
            "availability": {"1": {"name": "RBC", "periods": []}},
            "available_databases": ["benchmarking"],
            "table": "<availability_table>Test</availability_table>"
        }
        mock_get_descriptions.return_value = "<database_descriptions>Test</database_descriptions>"
        mock_load_yaml.return_value = {"content": "Planner instructions"}
        
        mock_complete.return_value = {
            "choices": [{
                "message": {
                    "tool_calls": [{
                        "function": {
                            "name": "databases_selected",
                            "arguments": json.dumps({
                                "databases": [
                                    {"database_id": "benchmarking", "query_intent": "Get efficiency ratio"}
                                ]
                            })
                        }
                    }]
                }
            }],
            "metrics": {"total_cost": 0.05},
            "usage": {"total_tokens": 1000}
        }
        
        query = "Show me efficiency"
        conversation = []
        banks = {"bank_ids": [1]}
        periods = {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}
        context = {"execution_id": "test-123"}
        query_intent = "efficiency ratio"  # Provided by clarifier
        
        result = plan_database_queries(
            query, conversation, banks, periods, context, 
            query_intent=query_intent
        )
        
        assert result["status"] == "success"
        # Verify that query intent was included in the user message
        mock_complete.assert_called_once()
        call_args = mock_complete.call_args
        messages = call_args[1]["messages"]
        assert "efficiency ratio" in messages[1]["content"]
    
    @patch('aegis.model.agents.planner.complete_with_tools')
    @patch('aegis.model.agents.planner.get_filtered_database_descriptions')
    @patch('aegis.model.agents.planner.get_filtered_availability_table')
    @patch('aegis.model.agents.planner.load_yaml')
    @patch('aegis.model.agents.planner.config')
    def test_plan_database_queries_model_tier_override(
        self, mock_config, mock_load_yaml, mock_get_availability, mock_get_descriptions, mock_complete
    ):
        """
        Test model tier override functionality.
        """
        # Setup config mocks
        mock_config.llm.small.model = "small-model"
        mock_config.llm.medium.model = "medium-model"
        mock_config.llm.large.model = "large-model"
        
        mock_get_availability.return_value = {
            "availability": {"1": {"name": "RBC", "periods": []}},
            "available_databases": ["benchmarking"],
            "table": "<availability_table>Test</availability_table>"
        }
        mock_get_descriptions.return_value = "<database_descriptions>Test</database_descriptions>"
        mock_load_yaml.return_value = {"content": "Planner instructions"}
        
        mock_complete.return_value = {
            "choices": [{
                "message": {
                    "tool_calls": [{
                        "function": {
                            "name": "databases_selected",
                            "arguments": json.dumps({
                                "databases": []
                            })
                        }
                    }]
                }
            }],
            "metrics": {"total_cost": 0.05},
            "usage": {"total_tokens": 1000}
        }
        
        # Test with small model override
        context = {"execution_id": "test-123", "model_tier_override": "small"}
        result = plan_database_queries(
            "Query", [], {"bank_ids": [1]}, 
            {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}, 
            context
        )
        
        # Verify small model was used
        call_args = mock_complete.call_args[1]
        assert call_args["llm_params"]["model"] == "small-model"
        
        # Test with large model override
        context["model_tier_override"] = "large"
        result = plan_database_queries(
            "Query", [], {"bank_ids": [1]}, 
            {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}, 
            context
        )
        
        # Verify large model was used
        call_args = mock_complete.call_args[1]
        assert call_args["llm_params"]["model"] == "large-model"
    
    @patch('aegis.model.agents.planner.complete_with_tools')
    @patch('aegis.model.agents.planner.get_filtered_database_descriptions')
    @patch('aegis.model.agents.planner.get_filtered_availability_table')
    @patch('aegis.model.agents.planner.load_yaml')
    @patch('aegis.model.agents.planner.get_logger')
    def test_plan_database_queries_exception(
        self, mock_logger, mock_load_yaml, mock_get_availability, mock_get_descriptions, mock_complete
    ):
        """
        Test exception handling.
        """
        mock_get_availability.side_effect = Exception("Database error")
        
        query = "Show me data"
        conversation = []
        banks = {"bank_ids": [1]}
        periods = {"apply_all": {"fiscal_year": 2024, "quarters": ["Q1"]}}
        context = {"execution_id": "test-123"}
        
        result = plan_database_queries(query, conversation, banks, periods, context)
        
        assert result["status"] == "error"
        assert "Database error" in result["error"]
        mock_logger().error.assert_called_once()