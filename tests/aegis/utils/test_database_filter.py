"""
Tests for the database_filter module.
"""

import pytest
from unittest.mock import Mock, patch, mock_open
import yaml

from aegis.utils.database_filter import (
    get_available_databases,
    filter_databases,
    get_database_prompt
)


class TestGetAvailableDatabases:
    """
    Tests for the get_available_databases function.
    """
    
    @patch('aegis.utils.database_filter.yaml.safe_load')
    @patch('builtins.open', new_callable=mock_open)
    def test_get_available_databases_success(self, mock_file, mock_yaml_load):
        """
        Test successful loading of databases from YAML.
        """
        mock_yaml_load.return_value = {
            "databases": [
                {"id": "benchmarking", "name": "Benchmarking Database", "content": "Benchmarking data"},
                {"id": "reports", "name": "Reports Database", "content": "Reports data"},
                {"id": "transcripts", "name": "Transcripts Database", "content": "Transcripts data"}
            ]
        }
        
        result = get_available_databases()
        
        assert len(result) == 3
        assert "benchmarking" in result
        assert result["benchmarking"]["name"] == "Benchmarking Database"
        assert "reports" in result
        assert "transcripts" in result
    
    @patch('aegis.utils.database_filter.yaml.safe_load')
    @patch('builtins.open', new_callable=mock_open)
    def test_get_available_databases_empty(self, mock_file, mock_yaml_load):
        """
        Test loading when no databases in YAML.
        """
        mock_yaml_load.return_value = {"databases": []}
        
        result = get_available_databases()
        
        assert result == {}
    
    @patch('aegis.utils.database_filter.get_logger')
    @patch('builtins.open', side_effect=FileNotFoundError("File not found"))
    def test_get_available_databases_file_not_found(self, mock_file, mock_logger):
        """
        Test handling of missing YAML file.
        """
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        result = get_available_databases()
        
        assert result == {}
        mock_logger_instance.error.assert_called_once()
        error_call = mock_logger_instance.error.call_args
        assert "Failed to load databases" in error_call[0][0]
    
    @patch('aegis.utils.database_filter.get_logger')
    @patch('aegis.utils.database_filter.yaml.safe_load', side_effect=yaml.YAMLError("Invalid YAML"))
    @patch('builtins.open', new_callable=mock_open)
    def test_get_available_databases_yaml_error(self, mock_file, mock_yaml_load, mock_logger):
        """
        Test handling of invalid YAML.
        """
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        result = get_available_databases()
        
        assert result == {}
        mock_logger_instance.error.assert_called_once()


class TestFilterDatabases:
    """
    Tests for the filter_databases function.
    """
    
    @patch('aegis.utils.database_filter.get_available_databases')
    def test_filter_databases_no_filter(self, mock_get_databases):
        """
        Test returning all databases when no filter provided.
        """
        mock_get_databases.return_value = {
            "benchmarking": {"id": "benchmarking", "name": "Benchmarking"},
            "reports": {"id": "reports", "name": "Reports"}
        }
        
        result = filter_databases(None)
        
        assert len(result) == 2
        assert "benchmarking" in result
        assert "reports" in result
    
    @patch('aegis.utils.database_filter.get_available_databases')
    def test_filter_databases_empty_list(self, mock_get_databases):
        """
        Test with empty list returns all databases.
        """
        mock_get_databases.return_value = {
            "benchmarking": {"id": "benchmarking", "name": "Benchmarking"},
            "reports": {"id": "reports", "name": "Reports"}
        }
        
        result = filter_databases([])
        
        assert len(result) == 2
    
    @patch('aegis.utils.database_filter.get_available_databases')
    def test_filter_databases_with_filter(self, mock_get_databases):
        """
        Test filtering to specific databases.
        """
        mock_get_databases.return_value = {
            "benchmarking": {"id": "benchmarking", "name": "Benchmarking"},
            "reports": {"id": "reports", "name": "Reports"},
            "transcripts": {"id": "transcripts", "name": "Transcripts"}
        }
        
        result = filter_databases(["benchmarking", "reports"])
        
        assert len(result) == 2
        assert "benchmarking" in result
        assert "reports" in result
        assert "transcripts" not in result
    
    @patch('aegis.utils.database_filter.get_logger')
    @patch('aegis.utils.database_filter.get_available_databases')
    def test_filter_databases_invalid_id(self, mock_get_databases, mock_logger):
        """
        Test filtering with invalid database ID.
        """
        mock_get_databases.return_value = {
            "benchmarking": {"id": "benchmarking", "name": "Benchmarking"}
        }
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        result = filter_databases(["benchmarking", "invalid_db"])
        
        assert len(result) == 1
        assert "benchmarking" in result
        assert "invalid_db" not in result
        
        # Should log warning for invalid database
        warning_calls = [call for call in mock_logger_instance.warning.call_args_list]
        assert len(warning_calls) == 1
        assert "Requested database not found" in str(warning_calls[0])


class TestGetDatabasePrompt:
    """
    Tests for the get_database_prompt function.
    """
    
    @patch('aegis.utils.database_filter.filter_databases')
    def test_get_database_prompt_with_databases(self, mock_filter):
        """
        Test generating prompt with available databases.
        """
        mock_filter.return_value = {
            "benchmarking": {
                "id": "benchmarking",
                "name": "Benchmarking Database",
                "content": "Contains financial metrics"
            },
            "reports": {
                "id": "reports",
                "name": "Reports Database",
                "content": "Contains annual reports"
            }
        }
        
        result = get_database_prompt(["benchmarking", "reports"])
        
        assert "Available Financial Databases:" in result
        assert "Benchmarking Database:" in result
        assert "Contains financial metrics" in result
        assert "Reports Database:" in result
        assert "Contains annual reports" in result
    
    @patch('aegis.utils.database_filter.filter_databases')
    def test_get_database_prompt_no_databases(self, mock_filter):
        """
        Test generating prompt when no databases available.
        """
        mock_filter.return_value = {}
        
        result = get_database_prompt(["invalid"])
        
        assert result == "No databases available for this query."
    
    @patch('aegis.utils.database_filter.filter_databases')
    def test_get_database_prompt_missing_content(self, mock_filter):
        """
        Test handling database without content field.
        """
        mock_filter.return_value = {
            "benchmarking": {
                "id": "benchmarking",
                "name": "Benchmarking Database"
                # No 'content' field
            }
        }
        
        result = get_database_prompt(["benchmarking"])
        
        assert "Benchmarking Database:" in result
        # Should handle missing content gracefully
        assert "Available Financial Databases:" in result