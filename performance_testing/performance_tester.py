"""
Performance testing utilities for Aegis agent components.

This module provides shared functionality for loading test scenarios,
collecting performance metrics, and generating reports.
"""

import json
import time
import yaml
import statistics
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict, field
import csv

# PDF generation imports
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.platypus import Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.piecharts import Pie


@dataclass
class Scenario:
    """
    Represents a single test scenario.
    
    Attributes:
        id: Unique identifier for the scenario
        name: Human-readable name
        description: Detailed description of the test
        message: Single message to test (mutually exclusive with conversation)
        conversation: Full conversation history (mutually exclusive with message)
        expected: Expected outcomes (route, confidence, etc.)
        reasoning: Explanation of why this expectation is correct
        tags: Optional tags for categorization
        thresholds: Optional performance thresholds override
    """
    id: str
    name: str
    description: str
    expected: Dict[str, Any]
    reasoning: str
    message: Optional[str] = None
    conversation: Optional[List[Dict[str, str]]] = None
    tags: List[str] = field(default_factory=list)
    thresholds: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Validate that either message or conversation is provided, not both."""
        if not self.message and not self.conversation:
            raise ValueError(f"Scenario {self.id} must have either 'message' or 'conversation'")
        if self.message and self.conversation:
            raise ValueError(f"Scenario {self.id} cannot have both 'message' and 'conversation'")
    
    def get_messages(self) -> List[Dict[str, str]]:
        """
        Get the messages for this scenario in the format expected by agents.
        
        Returns:
            List of message dictionaries with 'role' and 'content' keys
        """
        if self.message:
            return [{"role": "user", "content": self.message}]
        return self.conversation


@dataclass
class TestResult:
    """
    Represents the result of running a single test scenario.
    
    Attributes:
        scenario_id: ID of the scenario that was tested
        scenario_name: Name of the scenario
        success: Whether the test passed
        actual_output: The actual output from the agent
        expected_output: The expected output
        latency_ms: Response time in milliseconds
        tokens_used: Number of tokens consumed
        cost: Estimated cost in dollars
        confidence: Confidence score from the agent (if applicable)
        error: Error message if the test failed
        timestamp: When the test was run
    """
    scenario_id: str
    scenario_name: str
    success: bool
    actual_output: Any
    expected_output: Any
    latency_ms: float
    tokens_used: int = 0
    cost: float = 0.0
    confidence: Optional[float] = None
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


class ScenarioLoader:
    """Loads and validates test scenarios from YAML files."""
    
    @staticmethod
    def load_scenarios(yaml_path: str) -> Tuple[Dict[str, Any], List[Scenario]]:
        """
        Load scenarios from a YAML file.
        
        Args:
            yaml_path: Path to the YAML file containing scenarios
            
        Returns:
            Tuple of (test_suite_metadata, list of Scenario objects)
            
        Raises:
            FileNotFoundError: If the YAML file doesn't exist
            yaml.YAMLError: If the YAML is invalid
            ValueError: If scenario validation fails
        """
        path = Path(yaml_path)
        if not path.exists():
            raise FileNotFoundError(f"Scenario file not found: {yaml_path}")
        
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        
        if not data or 'test_suite' not in data:
            raise ValueError("YAML must contain 'test_suite' root key")
        
        test_suite = data['test_suite']
        scenarios_data = test_suite.get('scenarios', [])
        
        if not scenarios_data:
            raise ValueError("No scenarios found in YAML file")
        
        # Extract metadata
        metadata = {
            'name': test_suite.get('name', 'Unnamed Test Suite'),
            'description': test_suite.get('description', ''),
            'version': test_suite.get('version', '1.0'),
            'default_thresholds': test_suite.get('default_thresholds', {})
        }
        
        # Parse scenarios
        scenarios = []
        for scenario_data in scenarios_data:
            # Apply default thresholds if not overridden
            if 'thresholds' not in scenario_data and metadata['default_thresholds']:
                scenario_data['thresholds'] = metadata['default_thresholds']
            
            scenario = Scenario(**scenario_data)
            scenarios.append(scenario)
        
        return metadata, scenarios


class MetricsCollector:
    """Collects and aggregates performance metrics during testing."""
    
    def __init__(self):
        """Initialize the metrics collector."""
        self.results: List[TestResult] = []
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
    
    def start_test_suite(self):
        """Mark the start of a test suite."""
        self.start_time = datetime.now()
        self.results = []
    
    def end_test_suite(self):
        """Mark the end of a test suite."""
        self.end_time = datetime.now()
    
    def add_result(self, result: TestResult):
        """
        Add a test result to the collection.
        
        Args:
            result: TestResult object to add
        """
        self.results.append(result)
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics for all collected results.
        
        Returns:
            Dictionary containing summary statistics
        """
        if not self.results:
            return {
                'total_tests': 0,
                'passed': 0,
                'failed': 0,
                'success_rate': 0.0
            }
        
        passed = sum(1 for r in self.results if r.success)
        failed = len(self.results) - passed
        latencies = [r.latency_ms for r in self.results]
        tokens = [r.tokens_used for r in self.results]
        costs = [r.cost for r in self.results]
        confidences = [r.confidence for r in self.results if r.confidence is not None]
        
        summary = {
            'total_tests': len(self.results),
            'passed': passed,
            'failed': failed,
            'success_rate': (passed / len(self.results)) * 100,
            'latency': {
                'mean': statistics.mean(latencies) if latencies else 0,
                'median': statistics.median(latencies) if latencies else 0,
                'min': min(latencies) if latencies else 0,
                'max': max(latencies) if latencies else 0,
                'stdev': statistics.stdev(latencies) if len(latencies) > 1 else 0
            },
            'tokens': {
                'total': sum(tokens),
                'mean': statistics.mean(tokens) if tokens else 0
            },
            'cost': {
                'total': sum(costs),
                'mean': statistics.mean(costs) if costs else 0
            }
        }
        
        if confidences:
            summary['confidence'] = {
                'mean': statistics.mean(confidences),
                'median': statistics.median(confidences),
                'min': min(confidences),
                'max': max(confidences)
            }
        
        if self.start_time and self.end_time:
            summary['duration_seconds'] = (self.end_time - self.start_time).total_seconds()
        
        return summary


class ReportGenerator:
    """Generates test reports in various formats (PDF, CSV, JSON)."""
    
    def __init__(self, agent_name: str, results_dir: str = "performance_testing/results"):
        """
        Initialize the report generator.
        
        Args:
            agent_name: Name of the agent being tested
            results_dir: Directory to save reports
        """
        self.agent_name = agent_name
        self.results_dir = Path(results_dir)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def generate_all_reports(self, 
                           metadata: Dict[str, Any],
                           results: List[TestResult],
                           summary: Dict[str, Any]) -> Dict[str, str]:
        """
        Generate reports in all formats.
        
        Args:
            metadata: Test suite metadata
            results: List of test results
            summary: Summary statistics
            
        Returns:
            Dictionary mapping format to file path
        """
        paths = {}
        
        # Generate JSON report
        json_path = self._generate_json(metadata, results, summary)
        paths['json'] = str(json_path)
        
        # Generate CSV report
        csv_path = self._generate_csv(results, summary)
        paths['csv'] = str(csv_path)
        
        # Generate PDF report
        pdf_path = self._generate_pdf(metadata, results, summary)
        paths['pdf'] = str(pdf_path)
        
        return paths
    
    def _generate_json(self, 
                      metadata: Dict[str, Any],
                      results: List[TestResult],
                      summary: Dict[str, Any]) -> Path:
        """Generate JSON report."""
        filename = f"{self.agent_name}_{self.timestamp}.json"
        filepath = self.results_dir / filename
        
        report = {
            'metadata': metadata,
            'summary': summary,
            'results': [
                {
                    'scenario_id': r.scenario_id,
                    'scenario_name': r.scenario_name,
                    'success': r.success,
                    'actual_output': r.actual_output,
                    'expected_output': r.expected_output,
                    'latency_ms': r.latency_ms,
                    'tokens_used': r.tokens_used,
                    'cost': r.cost,
                    'confidence': r.confidence,
                    'error': r.error,
                    'timestamp': r.timestamp.isoformat()
                }
                for r in results
            ],
            'generated_at': datetime.now().isoformat()
        }
        
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2)
        
        return filepath
    
    def _generate_csv(self, results: List[TestResult], summary: Dict[str, Any]) -> Path:
        """Generate CSV report."""
        filename = f"{self.agent_name}_{self.timestamp}.csv"
        filepath = self.results_dir / filename
        
        with open(filepath, 'w', newline='') as f:
            # Write summary section
            writer = csv.writer(f)
            writer.writerow(['SUMMARY'])
            writer.writerow(['Metric', 'Value'])
            writer.writerow(['Total Tests', summary['total_tests']])
            writer.writerow(['Passed', summary['passed']])
            writer.writerow(['Failed', summary['failed']])
            writer.writerow(['Success Rate (%)', f"{summary['success_rate']:.2f}"])
            writer.writerow(['Mean Latency (seconds)', f"{summary['latency']['mean']/1000:.2f}"])
            writer.writerow(['Total Tokens', f"{summary['tokens']['total']:,}"])
            writer.writerow(['Total Cost ($)', f"{summary['cost']['total']:.4f}"])
            writer.writerow([])
            
            # Write detailed results
            writer.writerow(['DETAILED RESULTS'])
            if results:
                fieldnames = ['scenario_id', 'scenario_name', 'success', 'latency_seconds', 
                            'tokens_used', 'cost', 'error']
                writer.writerow(fieldnames)
                
                for r in results:
                    writer.writerow([
                        r.scenario_id,
                        r.scenario_name,
                        r.success,
                        f"{r.latency_ms/1000:.2f}",  # Convert to seconds
                        f"{r.tokens_used:,}",  # Format with comma
                        f"{r.cost:.4f}",
                        r.error or ''
                    ])
        
        return filepath
    
    def _generate_pdf(self,
                     metadata: Dict[str, Any],
                     results: List[TestResult],
                     summary: Dict[str, Any]) -> Path:
        """Generate PDF report with charts and detailed results."""
        filename = f"{self.agent_name}_{self.timestamp}.pdf"
        filepath = self.results_dir / filename
        
        # Create PDF document
        doc = SimpleDocTemplate(str(filepath), pagesize=letter)
        story = []
        styles = getSampleStyleSheet()
        
        # Title style
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#1a73e8'),
            spaceAfter=30,
            alignment=TA_CENTER
        )
        
        # Add title
        title = Paragraph(f"{metadata['name']} - Performance Report", title_style)
        story.append(title)
        
        # Add metadata
        story.append(Paragraph(f"<b>Agent:</b> {self.agent_name}", styles['Normal']))
        story.append(Paragraph(f"<b>Version:</b> {metadata.get('version', 'N/A')}", styles['Normal']))
        story.append(Paragraph(f"<b>Description:</b> {metadata.get('description', 'N/A')}", styles['Normal']))
        story.append(Paragraph(f"<b>Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
        story.append(Spacer(1, 20))
        
        # Executive Summary
        story.append(Paragraph("Executive Summary", styles['Heading2']))
        
        # Summary table
        summary_data = [
            ['Metric', 'Value'],
            ['Total Tests', str(summary['total_tests'])],
            ['Passed', str(summary['passed'])],
            ['Failed', str(summary['failed'])],
            ['Success Rate', f"{summary['success_rate']:.2f}%"],
            ['Mean Latency', f"{summary['latency']['mean']/1000:.2f}s"],
            ['Median Latency', f"{summary['latency']['median']/1000:.2f}s"],
            ['Total Tokens', f"{summary['tokens']['total']:,}"],
            ['Total Cost', f"${summary['cost']['total']:.4f}"]
        ]
        
        summary_table = Table(summary_data)
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(summary_table)
        story.append(Spacer(1, 20))
        
        # Add pass/fail pie chart
        if summary['total_tests'] > 0:
            story.append(Paragraph("Test Results Distribution", styles['Heading3']))
            
            drawing = Drawing(400, 200)
            pie = Pie()
            pie.x = 150
            pie.y = 50
            pie.width = 100
            pie.height = 100
            pie.data = [summary['passed'], summary['failed']]
            pie.labels = [f"Passed ({summary['passed']})", f"Failed ({summary['failed']})"]
            pie.slices[0].fillColor = colors.green
            pie.slices[1].fillColor = colors.red
            
            drawing.add(pie)
            story.append(drawing)
            story.append(Spacer(1, 20))
        
        # Detailed Results
        story.append(PageBreak())
        story.append(Paragraph("Detailed Test Results", styles['Heading2']))
        
        # Results table
        if results:
            results_data = [['Scenario', 'Result', 'Latency', 'Tokens', 'Cost']]
            
            for r in results:
                results_data.append([
                    r.scenario_name[:40] + ('...' if len(r.scenario_name) > 40 else ''),
                    '✓' if r.success else '✗',
                    f"{r.latency_ms/1000:.2f}s",  # Convert ms to seconds
                    f"{r.tokens_used:,}",  # Format with comma separator
                    f"${r.cost:.4f}"
                ])
            
            results_table = Table(results_data)
            results_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 8)
            ]))
            
            # Color code pass/fail
            for i, r in enumerate(results, start=1):
                if r.success:
                    results_table.setStyle(TableStyle([
                        ('TEXTCOLOR', (1, i), (1, i), colors.green)
                    ]))
                else:
                    results_table.setStyle(TableStyle([
                        ('TEXTCOLOR', (1, i), (1, i), colors.red)
                    ]))
            
            story.append(results_table)
        
        # Failed Tests Details
        failed_tests = [r for r in results if not r.success]
        if failed_tests:
            story.append(PageBreak())
            story.append(Paragraph("Failed Test Details", styles['Heading2']))
            
            for r in failed_tests:
                story.append(Paragraph(f"<b>{r.scenario_name}</b>", styles['Heading3']))
                story.append(Paragraph(f"<b>Scenario ID:</b> {r.scenario_id}", styles['Normal']))
                story.append(Paragraph(f"<b>Expected:</b> {r.expected_output}", styles['Normal']))
                story.append(Paragraph(f"<b>Actual:</b> {r.actual_output}", styles['Normal']))
                if r.error:
                    story.append(Paragraph(f"<b>Error:</b> {r.error}", styles['Normal']))
                story.append(Spacer(1, 10))
        
        # Build PDF
        doc.build(story)
        
        return filepath


def run_agent_test(agent_function, scenario: Scenario, context: Dict[str, Any]) -> TestResult:
    """
    Run a single test scenario against an agent.
    
    Args:
        agent_function: The agent function to test (should match main.py usage)
        scenario: The scenario to test
        context: Context dictionary with auth_config, ssl_config, etc.
        
    Returns:
        TestResult object with the test outcome
    """
    start_time = time.time()
    
    try:
        # Get messages for the scenario
        messages = scenario.get_messages()
        
        # Extract the latest message (last in the list)
        latest_message = messages[-1]['content'] if messages else ""
        
        # Call the agent function (matching how main.py calls it)
        result = agent_function(
            conversation_history=messages,
            latest_message=latest_message,
            context=context
        )
        
        # Calculate metrics
        latency_ms = (time.time() - start_time) * 1000
        
        # Extract actual output based on agent response structure
        actual_output = result.get('decision', result.get('route', result))
        confidence = result.get('confidence', result.get('confidence_score'))
        
        # Check if the result matches expectations
        success = True
        errors = []
        
        # Check route/decision if specified
        if 'route' in scenario.expected:
            if actual_output != scenario.expected['route']:
                success = False
                errors.append(f"Expected route '{scenario.expected['route']}', got '{actual_output}'")
        
        # Check confidence threshold if specified
        if 'min_confidence' in scenario.expected and confidence:
            if confidence < scenario.expected['min_confidence']:
                success = False
                errors.append(f"Confidence {confidence:.2f} below threshold {scenario.expected['min_confidence']}")
        
        # Check latency threshold if specified
        if scenario.thresholds and 'max_latency_ms' in scenario.thresholds:
            if latency_ms > scenario.thresholds['max_latency_ms']:
                success = False
                errors.append(f"Latency {latency_ms:.2f}ms exceeds threshold {scenario.thresholds['max_latency_ms']}ms")
        
        # Combine all errors
        error = " | ".join(errors) if errors else None
        
        # Extract token usage and cost if available
        tokens_used = result.get('tokens_used', 0)
        cost = result.get('cost', 0.0)
        
        return TestResult(
            scenario_id=scenario.id,
            scenario_name=scenario.name,
            success=success,
            actual_output=actual_output,
            expected_output=scenario.expected.get('route', scenario.expected),
            latency_ms=latency_ms,
            tokens_used=tokens_used,
            cost=cost,
            confidence=confidence,
            error=error
        )
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        return TestResult(
            scenario_id=scenario.id,
            scenario_name=scenario.name,
            success=False,
            actual_output=None,
            expected_output=scenario.expected,
            latency_ms=latency_ms,
            error=str(e)
        )