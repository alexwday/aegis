"""
Constants for segment performance metrics.

Defines the monitored platforms and their core metrics to display.
"""

MONITORED_PLATFORMS = [
    "Canadian Banking",
    "U.S. & International Banking",
    "Capital Markets",
    "Canadian Wealth & Insurance",
    "Corporate Support",
]

CORE_SEGMENT_METRICS = {
    "Canadian Banking": [
        "Net Income",
        "NIM",
        "Total Revenue",
    ],
    "Canadian Wealth & Insurance": [
        "Total Revenue",
        "Net Income",
        "Non Interest Income",
    ],
    "Capital Markets": [
        "Net Revenue",
        "Non Interest Income",
        "Net Income",
    ],
    "U.S. & International Banking": [
        "Net Income",
        "Net Interest Income (TEB)",
        "Average Loans",
    ],
    "Corporate Support": [
        "Net Revenue",
        "Non Interest Expenses",
        "Net Income",
    ],
}

DEFAULT_CORE_METRICS = [
    "Total Revenue",
    "Net Income",
    "Efficiency Ratio",
]


def is_monitored_platform(platform_name: str) -> bool:
    """
    Check if a platform name exactly matches one of our monitored platforms.

    Args:
        platform_name: The Platform value from benchmarking_report

    Returns:
        True if exact match found, False otherwise
    """
    if not platform_name:
        return False

    return platform_name in MONITORED_PLATFORMS
