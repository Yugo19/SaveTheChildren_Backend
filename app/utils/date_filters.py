"""
Utility functions for consistent date filtering across the application.
Supports multiple date formats from frontend:
- Year only: "2024" -> includes all of 2024
- ISO date: "2024-07-15" -> exact date
- ISO datetime: "2024-07-15T10:30:00Z" -> exact datetime
"""

from typing import Optional, Dict, Any
from datetime import datetime


def build_date_filter(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    field_name: str = "case_date"
) -> Dict[str, Any]:
    """
    Build MongoDB date filter that works correctly with string date comparisons.
    
    Args:
        date_from: Start date (year, date, or datetime string)
        date_to: End date (year, date, or datetime string)
        field_name: The field name to filter on (default: "case_date")
    
    Returns:
        Dictionary with MongoDB filter, or empty dict if no dates provided
    
    Examples:
        >>> build_date_filter("2024", "2024")
        {'case_date': {'$gte': '2024-01-01', '$lt': '2025-01-01'}}
        
        >>> build_date_filter("2024-01-01", "2024-12-31")
        {'case_date': {'$gte': '2024-01-01', '$lte': '2024-12-31'}}
        
        >>> build_date_filter("2020", None)
        {'case_date': {'$gte': '2020-01-01'}}
    """
    if not date_from and not date_to:
        return {}
    
    date_filter = {}
    
    if date_from:
        # Normalize date_from
        if len(date_from) == 4 and date_from.isdigit():
            # Year only: start from beginning of year
            date_filter["$gte"] = f"{date_from}-01-01"
        else:
            # Already has month/day/time
            date_filter["$gte"] = date_from
    
    if date_to:
        # Normalize date_to
        if len(date_to) == 4 and date_to.isdigit():
            # Year only: include entire year using < next year
            # This is more accurate than using -12-31 23:59:59
            next_year = str(int(date_to) + 1)
            date_filter["$lt"] = f"{next_year}-01-01"
        else:
            # Already has month/day/time - use <= for inclusive end
            date_filter["$lte"] = date_to
    
    return {field_name: date_filter} if date_filter else {}


def normalize_date_string(date_str: Optional[str]) -> Optional[str]:
    """
    Normalize a date string to ISO format.
    
    Args:
        date_str: Date string in various formats
    
    Returns:
        Normalized date string or None if input is None
    
    Examples:
        >>> normalize_date_string("2024")
        '2024-01-01'
        
        >>> normalize_date_string("2024-07-15")
        '2024-07-15'
    """
    if not date_str:
        return None
    
    # Year only
    if len(date_str) == 4 and date_str.isdigit():
        return f"{date_str}-01-01"
    
    # Already formatted
    return date_str


def parse_date_range(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None
) -> tuple[Optional[str], Optional[str]]:
    """
    Parse and normalize date range for consistent handling.
    
    Args:
        date_from: Start date
        date_to: End date
    
    Returns:
        Tuple of (normalized_from, normalized_to)
    """
    normalized_from = None
    normalized_to = None
    
    if date_from:
        if len(date_from) == 4 and date_from.isdigit():
            normalized_from = f"{date_from}-01-01"
        else:
            normalized_from = date_from
    
    if date_to:
        if len(date_to) == 4 and date_to.isdigit():
            # For end date with year only, use end of year
            normalized_to = f"{date_to}-12-31 23:59:59"
        else:
            normalized_to = date_to
    
    return normalized_from, normalized_to
