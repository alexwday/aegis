"""
Generic PDF report generation for performance testing of all agents.
Used for clarifier, response, and other non-router agents.
"""

from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
)
from reportlab.graphics.shapes import Drawing, String
from reportlab.graphics.charts.barcharts import VerticalBarChart


class GenericPDFGenerator:
    """Generate generic PDF reports for non-router agent testing."""
    
    def __init__(self, agent_name: str, timestamp: str, results_dir: Path):
        self.agent_name = agent_name
        self.timestamp = timestamp
        self.results_dir = results_dir
        
    def generate(self, metadata: Dict[str, Any], results: List, summary: Dict[str, Any]) -> Path:
        """Generate the generic PDF report."""
        filename = f"{self.agent_name}_{self.timestamp}.pdf"
        filepath = self.results_dir / filename
        
        # Create PDF document with margins
        doc = SimpleDocTemplate(
            str(filepath), 
            pagesize=letter,
            leftMargin=0.75*inch,
            rightMargin=0.75*inch,
            topMargin=0.75*inch,
            bottomMargin=0.75*inch
        )
        
        story = []
        styles = getSampleStyleSheet()
        
        # Custom styles
        title_style = ParagraphStyle(
            "CustomTitle",
            parent=styles["Heading1"],
            fontSize=22,
            textColor=colors.HexColor("#1a73e8"),
            spaceAfter=20,
            alignment=1  # Center
        )
        
        heading2_style = ParagraphStyle(
            "CustomHeading2",
            parent=styles["Heading2"],
            fontSize=16,
            textColor=colors.HexColor("#333333"),
            spaceBefore=12,
            spaceAfter=8
        )
        
        heading3_style = ParagraphStyle(
            "CustomHeading3",
            parent=styles["Heading3"],
            fontSize=13,
            textColor=colors.HexColor("#555555"),
            spaceBefore=8,
            spaceAfter=6
        )
        
        normal_style = ParagraphStyle(
            "CustomNormal",
            parent=styles["Normal"],
            fontSize=10,
            leading=12
        )
        
        small_style = ParagraphStyle(
            "Small",
            parent=styles["Normal"],
            fontSize=8,
            leading=10
        )
        
        # Title and metadata
        story.append(Paragraph(metadata.get("name", "Performance Test Report"), title_style))
        story.append(Paragraph("Multi-Model Performance Analysis", heading3_style))
        story.append(Spacer(1, 12))
        
        # Report metadata
        meta_data = [
            f"<b>Agent:</b> {self.agent_name}",
            f"<b>Version:</b> {metadata.get('version', 'N/A')}",
            f"<b>Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ]
        for item in meta_data:
            story.append(Paragraph(item, normal_style))
        
        story.append(Spacer(1, 20))
        
        # 1. NEW EXECUTIVE SUMMARY - Combined model comparison and overall stats
        story.append(Paragraph("Executive Summary", heading2_style))
        
        if "model_comparison" in metadata:
            # Model Performance Comparison Table
            model_data = [["Model", "Tests", "Passed", "Success Rate", "Avg Latency", "Total Cost"]]
            
            for model_tier in ["small", "medium", "large"]:
                if model_tier in metadata["model_comparison"]:
                    ms = metadata["model_comparison"][model_tier]
                    model_data.append([
                        model_tier.upper(),
                        str(ms["total_tests"]),
                        f"{ms['passed']}/{ms['total_tests']}",
                        f"{ms['success_rate']:.1f}%",
                        f"{ms['latency']['mean']/1000:.2f}s",
                        f"${ms['cost']['total']:.4f}"
                    ])
            
            # Add overall summary row
            model_data.append([
                "OVERALL",
                str(summary["total_tests"]),
                f"{summary['passed']}/{summary['total_tests']}",
                f"{summary['success_rate']:.1f}%",
                f"{summary['latency']['mean']/1000:.2f}s",
                f"${summary['cost']['total']:.4f}"
            ])
            
            model_table = Table(model_data, colWidths=[80, 50, 60, 80, 80, 80])
            model_table.setStyle(TableStyle([
                # Header row
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a73e8")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 10),
                # Data rows
                ("BACKGROUND", (0, 1), (-1, -2), colors.white),
                ("FONTSIZE", (0, 1), (-1, -2), 9),
                # Overall row
                ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#f0f0f0")),
                ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                ("LINEABOVE", (0, -1), (-1, -1), 1, colors.black),
                # Grid
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]))
            story.append(model_table)
            story.append(Spacer(1, 20))
        
        # 2. IMPROVED SUCCESS RATE CHART with x/x labels and percentages
        if "model_comparison" in metadata:
            story.append(Paragraph("Success Rate by Model", heading3_style))
            
            drawing = Drawing(450, 250)
            
            # Create bar chart
            bc = VerticalBarChart()
            bc.x = 60
            bc.y = 60
            bc.height = 140
            bc.width = 350
            
            model_names = []
            success_rates = []
            passed_counts = []
            total_counts = []
            
            for model_tier in ["small", "medium", "large"]:
                if model_tier in metadata["model_comparison"]:
                    ms = metadata["model_comparison"][model_tier]
                    model_names.append(model_tier.upper())
                    success_rates.append(ms["success_rate"])
                    passed_counts.append(ms["passed"])
                    total_counts.append(ms["total_tests"])
            
            bc.data = [success_rates]
            bc.categoryAxis.categoryNames = model_names
            bc.valueAxis.valueMin = 0
            bc.valueAxis.valueMax = 100
            bc.valueAxis.valueStep = 20
            bc.bars[0].fillColor = colors.HexColor("#1a73e8")
            bc.categoryAxis.labels.fontSize = 11
            bc.valueAxis.labels.fontSize = 10
            bc.valueAxis.labelTextFormat = "%d%%"
            bc.barWidth = 60
            
            drawing.add(bc)
            
            # Add value labels on top of bars
            bar_x_positions = [60 + 60 + i * 120 for i in range(len(model_names))]
            for i, (rate, passed, total) in enumerate(zip(success_rates, passed_counts, total_counts)):
                # Percentage label
                y_pos = 60 + (rate * 140 / 100) + 5
                drawing.add(String(
                    bar_x_positions[i], y_pos,
                    f"{rate:.1f}%",
                    fontSize=10,
                    fontName="Helvetica-Bold",
                    textAnchor="middle"
                ))
                # x/x label
                drawing.add(String(
                    bar_x_positions[i], y_pos + 12,
                    f"({passed}/{total})",
                    fontSize=9,
                    fontName="Helvetica",
                    textAnchor="middle"
                ))
            
            story.append(drawing)
            story.append(Spacer(1, 20))
        
        # 3. REMOVE PIE CHART - Skip the redundant pie chart
        
        # 4. DETAILED TEST RESULTS - Redesigned for clarity
        story.append(PageBreak())
        story.append(Paragraph("Detailed Test Results", heading2_style))
        
        if results:
            # Group results by scenario
            scenarios_by_id = {}
            for r in results:
                if r.scenario_id not in scenarios_by_id:
                    scenarios_by_id[r.scenario_id] = []
                scenarios_by_id[r.scenario_id].append(r)
            
            for scenario_num, (scenario_id, scenario_results) in enumerate(scenarios_by_id.items(), 1):
                first_result = scenario_results[0]
                
                # Scenario header
                story.append(Paragraph(
                    f"<b>Scenario {scenario_num}: {first_result.scenario_name}</b>",
                    heading3_style
                ))
                
                # Query
                if hasattr(first_result, 'test_query') and first_result.test_query:
                    query_text = first_result.test_query[:300]
                    if len(first_result.test_query) > 300:
                        query_text += "..."
                    story.append(Paragraph(f"<b>Query:</b> {query_text}", small_style))
                
                # Expected output - format banks and periods clearly
                if first_result.expected_output:
                    expected_parts = []
                    exp = first_result.expected_output
                    
                    if isinstance(exp, dict):
                        if "status" in exp:
                            expected_parts.append(f"Status: {exp['status']}")
                        if "bank_ids" in exp:
                            expected_parts.append(f"Banks: {exp['bank_ids']}")
                        if "period_year" in exp:
                            expected_parts.append(f"Year: {exp['period_year']}")
                        if "period_quarters" in exp:
                            expected_parts.append(f"Quarters: {exp['period_quarters']}")
                        if "needs_clarification" in exp and exp["needs_clarification"]:
                            expected_parts.append("Needs Clarification")
                    
                    expected_str = ", ".join(expected_parts) if expected_parts else str(exp)
                    story.append(Paragraph(f"<b>Expected:</b> {expected_str}", small_style))
                
                story.append(Spacer(1, 6))
                
                # Results table for each model
                result_data = [["Model", "Status", "Banks", "Period", "Latency", "Cost"]]
                
                for r in scenario_results:
                    model_name = r.model_used or "Default"
                    status = "✓ Pass" if r.success else "✗ Fail"
                    
                    # Extract banks and periods from actual output
                    banks_str = "-"
                    period_str = "-"
                    
                    if r.actual_output and isinstance(r.actual_output, dict):
                        # Extract banks
                        if "banks" in r.actual_output and r.actual_output["banks"]:
                            bank_ids = r.actual_output["banks"].get("bank_ids", [])
                            if bank_ids:
                                banks_str = str(bank_ids)
                        
                        # Extract periods
                        if "periods" in r.actual_output and r.actual_output["periods"]:
                            periods_data = r.actual_output["periods"].get("periods", {})
                            if "apply_all" in periods_data:
                                year = periods_data["apply_all"].get("fiscal_year")
                                quarters = periods_data["apply_all"].get("quarters")
                                period_parts = []
                                if year:
                                    period_parts.append(str(year))
                                if quarters:
                                    period_parts.append(str(quarters))
                                if period_parts:
                                    period_str = " ".join(period_parts)
                            elif periods_data:
                                # Bank-specific periods
                                first_bank = list(periods_data.keys())[0] if periods_data else None
                                if first_bank and isinstance(periods_data[first_bank], dict):
                                    year = periods_data[first_bank].get("fiscal_year")
                                    quarters = periods_data[first_bank].get("quarters")
                                    period_parts = []
                                    if year:
                                        period_parts.append(str(year))
                                    if quarters:
                                        period_parts.append(str(quarters))
                                    if period_parts:
                                        period_str = " ".join(period_parts)
                        
                        # Handle needs_clarification status
                        if r.actual_output.get("status") == "needs_clarification":
                            if not banks_str or banks_str == "-":
                                banks_str = "Needs clarification"
                            if not period_str or period_str == "-":
                                period_str = "Needs clarification"
                    
                    # Format latency and cost
                    latency_str = f"{r.latency_ms/1000:.2f}s" if r.latency_ms else "-"
                    
                    # Ensure cost is included even for needs_clarification
                    cost_value = r.cost if r.cost else 0
                    if cost_value == 0 and r.actual_output and isinstance(r.actual_output, dict):
                        # Try to extract cost from actual_output
                        cost_value = r.actual_output.get("cost", 0)
                        if cost_value == 0 and "banks" in r.actual_output:
                            banks_cost = r.actual_output["banks"].get("cost", 0)
                            periods_cost = r.actual_output.get("periods", {}).get("cost", 0) if "periods" in r.actual_output else 0
                            cost_value = banks_cost + periods_cost
                    
                    cost_str = f"${cost_value:.5f}" if cost_value > 0 else "$0.00000"
                    
                    result_data.append([
                        model_name.upper(),
                        status,
                        banks_str[:30],  # Truncate if too long
                        period_str[:40],  # Truncate if too long
                        latency_str,
                        cost_str
                    ])
                
                # Create results table with proper column widths
                result_table = Table(
                    result_data,
                    colWidths=[60, 60, 100, 120, 60, 70]
                )
                
                # Style the table
                table_style = [
                    # Header
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#666666")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 9),
                    ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                    # Data
                    ("FONTSIZE", (0, 1), (-1, -1), 8),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8f8f8")]),
                ]
                
                # Color code pass/fail in status column
                for i, r in enumerate(scenario_results, 1):
                    if r.success:
                        table_style.append(("TEXTCOLOR", (1, i), (1, i), colors.green))
                    else:
                        table_style.append(("TEXTCOLOR", (1, i), (1, i), colors.red))
                
                result_table.setStyle(TableStyle(table_style))
                story.append(result_table)
                story.append(Spacer(1, 15))
        
        # Failed test details at the end
        failed_tests = [r for r in results if not r.success]
        if failed_tests:
            story.append(PageBreak())
            story.append(Paragraph("Failed Test Analysis", heading2_style))
            
            for r in failed_tests:
                story.append(Paragraph(
                    f"<b>{r.scenario_name} ({r.model_used.upper() if r.model_used else 'Default'})</b>",
                    heading3_style
                ))
                
                if r.error:
                    story.append(Paragraph(f"<b>Error:</b> {r.error}", small_style))
                
                # Show what was expected vs actual for debugging
                if r.expected_output and r.actual_output:
                    story.append(Paragraph("<b>Mismatch Details:</b>", small_style))
                    
                    # Format for comparison
                    exp_formatted = self._format_for_comparison(r.expected_output)
                    act_formatted = self._format_for_comparison(r.actual_output)
                    
                    story.append(Paragraph(f"Expected: {exp_formatted}", small_style))
                    story.append(Paragraph(f"Actual: {act_formatted}", small_style))
                
                story.append(Spacer(1, 10))
        
        # Build the PDF
        doc.build(story)
        return filepath
    
    def _format_for_comparison(self, data: Any) -> str:
        """Format data for clear comparison in failed tests."""
        if not data:
            return "None"
        
        if isinstance(data, dict):
            parts = []
            
            # For expected format
            if "bank_ids" in data:
                parts.append(f"Banks={data['bank_ids']}")
            if "period_year" in data:
                parts.append(f"Year={data['period_year']}")
            if "period_quarters" in data:
                parts.append(f"Quarters={data['period_quarters']}")
            if "status" in data:
                parts.append(f"Status={data['status']}")
            
            # For actual format
            if "banks" in data and isinstance(data["banks"], dict):
                bank_ids = data["banks"].get("bank_ids", [])
                if bank_ids:
                    parts.append(f"Banks={bank_ids}")
            
            if "periods" in data and isinstance(data["periods"], dict):
                periods_info = data["periods"].get("periods", {})
                if "apply_all" in periods_info:
                    year = periods_info["apply_all"].get("fiscal_year")
                    quarters = periods_info["apply_all"].get("quarters")
                    if year:
                        parts.append(f"Year={year}")
                    if quarters:
                        parts.append(f"Quarters={quarters}")
            
            return ", ".join(parts) if parts else str(data)[:200]
        
        return str(data)[:200]