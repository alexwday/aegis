#!/usr/bin/env python
"""
Performance testing script for the Router agent.

This script loads and tests the router agent in the same way that main.py does,
using the same SSL, OAuth, and configuration setup.
"""

import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Add parent directory to path to import aegis modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.aegis.connections.oauth_connector import setup_authentication
from src.aegis.model.agents import route_query
from src.aegis.utils.database_filter import filter_databases, get_database_prompt
from src.aegis.utils.logging import setup_logging, get_logger
from src.aegis.utils.ssl import setup_ssl

from performance_tester import (
    MetricsCollector,
    ReportGenerator,
    ScenarioLoader,
    TestResult,
    run_agent_test,
)


def setup_router_context():
    """
    Set up the router context exactly as main.py does.
    
    Returns:
        Dictionary containing execution_id, auth_config, ssl_config, 
        database_prompt, and available_databases
    """
    # Initialize logging
    setup_logging()
    logger = get_logger()
    
    # Generate execution ID
    execution_id = str(uuid.uuid4())
    logger.info("test_router.context_setup.started", execution_id=execution_id)
    
    # Setup SSL configuration
    ssl_config = setup_ssl()
    logger.info(
        "test_router.ssl_setup.completed",
        execution_id=execution_id,
        status=ssl_config.get("status", "Unknown"),
    )
    
    # Setup authentication (requires execution_id and ssl_config)
    auth_config = setup_authentication(execution_id, ssl_config)
    logger.info(
        "test_router.auth_setup.completed",
        execution_id=execution_id,
        auth_method=auth_config.get("auth_method", "Unknown"),
    )
    
    # Get filtered databases (no filter applied for testing)
    filtered_databases = filter_databases(None)
    database_prompt = get_database_prompt(filtered_databases)
    
    logger.info(
        "test_router.database_setup.completed",
        execution_id=execution_id,
        database_count=len(filtered_databases),
    )
    
    # Build context matching main.py
    context = {
        "execution_id": execution_id,
        "auth_config": auth_config,
        "ssl_config": ssl_config,
        "database_prompt": database_prompt,
        "available_databases": list(filtered_databases.keys()),
    }
    
    logger.info("test_router.context_setup.completed", execution_id=execution_id)
    
    return context


def test_router_with_scenario(scenario, context):
    """
    Test the router with a single scenario.
    
    Args:
        scenario: Scenario object to test
        context: Router context from setup_router_context()
        
    Returns:
        TestResult object with the test outcome
    """
    logger = get_logger()
    logger.info(
        "test_router.scenario.started",
        execution_id=context["execution_id"],
        scenario_id=scenario.id,
        scenario_name=scenario.name,
    )
    
    # Use the common test runner with our router function
    result = run_agent_test(route_query, scenario, context)
    
    logger.info(
        "test_router.scenario.completed",
        execution_id=context["execution_id"],
        scenario_id=scenario.id,
        success=result.success,
        latency_ms=result.latency_ms,
        tokens_used=result.tokens_used,
        cost=result.cost,
    )
    
    return result


def main():
    """
    Main entry point for router performance testing.
    
    Loads scenarios from router_scenarios.yaml, runs all tests sequentially,
    and generates PDF, CSV, and JSON reports.
    """
    # Initialize logging
    setup_logging()
    logger = get_logger()
    
    print("\n" + "="*60)
    print("ROUTER AGENT PERFORMANCE TESTING")
    print("="*60 + "\n")
    
    # Load scenarios
    scenario_file = Path(__file__).parent / "scenarios" / "router_scenarios.yaml"
    
    try:
        print(f"Loading scenarios from: {scenario_file}")
        metadata, scenarios = ScenarioLoader.load_scenarios(str(scenario_file))
        print(f"✓ Loaded {len(scenarios)} test scenarios")
        print(f"  Test Suite: {metadata['name']}")
        print(f"  Version: {metadata['version']}")
        print()
    except Exception as e:
        print(f"✗ Failed to load scenarios: {e}")
        sys.exit(1)
    
    # Initialize metrics collector
    collector = MetricsCollector()
    collector.start_test_suite()
    
    # Setup router context once (same for all tests)
    print("Setting up router context (SSL, Auth, Databases)...")
    try:
        context = setup_router_context()
        print("✓ Router context initialized")
        print(f"  Execution ID: {context['execution_id']}")
        print(f"  Auth Method: {context['auth_config'].get('auth_method', 'Unknown')}")
        print(f"  Available Databases: {len(context['available_databases'])}")
        print()
    except Exception as e:
        print(f"✗ Failed to setup router context: {e}")
        sys.exit(1)
    
    # Run all scenarios
    print("Running test scenarios...")
    print("-" * 40)
    
    for i, scenario in enumerate(scenarios, 1):
        print(f"[{i}/{len(scenarios)}] {scenario.name}")
        print(f"    Testing: {scenario.description}")
        
        try:
            # Test the scenario
            result = test_router_with_scenario(scenario, context)
            collector.add_result(result)
            
            # Print result
            if result.success:
                print(f"    ✓ PASS (latency: {result.latency_ms/1000:.2f}s, tokens: {result.tokens_used:,}, cost: ${result.cost:.4f})")
            else:
                print(f"    ✗ FAIL: {result.error}")
                print(f"      Expected: {result.expected_output}")
                print(f"      Actual: {result.actual_output}")
        
        except Exception as e:
            # Create failed result for unexpected errors
            result = TestResult(
                scenario_id=scenario.id,
                scenario_name=scenario.name,
                success=False,
                actual_output=None,
                expected_output=scenario.expected,
                latency_ms=0,
                error=f"Unexpected error: {str(e)}"
            )
            collector.add_result(result)
            print(f"    ✗ ERROR: {e}")
        
        print()
    
    # End test suite
    collector.end_test_suite()
    print("-" * 40)
    
    # Generate summary
    summary = collector.get_summary()
    
    print("\nTEST SUMMARY")
    print("=" * 40)
    print(f"Total Tests: {summary['total_tests']}")
    print(f"Passed: {summary['passed']} ({summary['success_rate']:.1f}%)")
    print(f"Failed: {summary['failed']}")
    print(f"\nPerformance Metrics:")
    print(f"  Mean Latency: {summary['latency']['mean']/1000:.2f}s")
    print(f"  Median Latency: {summary['latency']['median']/1000:.2f}s")
    print(f"  Min/Max Latency: {summary['latency']['min']/1000:.2f}s / {summary['latency']['max']/1000:.2f}s")
    
    
    if summary.get('tokens', {}).get('total', 0) > 0:
        print(f"\nToken Usage:")
        print(f"  Total: {summary['tokens']['total']:,}")
        print(f"  Mean per test: {summary['tokens']['mean']:,.0f}")
    
    if summary.get('cost', {}).get('total', 0) > 0:
        print(f"\nEstimated Cost:")
        print(f"  Total: ${summary['cost']['total']:.4f}")
        print(f"  Mean per test: ${summary['cost']['mean']:.4f}")
    
    # Generate reports
    print("\nGenerating reports...")
    generator = ReportGenerator("router")
    
    try:
        report_paths = generator.generate_all_reports(
            metadata=metadata,
            results=collector.results,
            summary=summary
        )
        
        print("✓ Reports generated successfully:")
        for format_type, path in report_paths.items():
            print(f"  {format_type.upper()}: {path}")
    
    except Exception as e:
        print(f"✗ Failed to generate reports: {e}")
        logger.error("test_router.report_generation.failed", error=str(e))
    
    # Overall result
    print("\n" + "="*60)
    if summary['success_rate'] >= 80:
        print("✓ ROUTER TESTING COMPLETED SUCCESSFULLY")
    else:
        print("✗ ROUTER TESTING COMPLETED WITH FAILURES")
    print("="*60 + "\n")
    
    # Exit with appropriate code
    sys.exit(0 if summary['success_rate'] >= 80 else 1)


if __name__ == "__main__":
    main()