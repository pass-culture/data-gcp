from datetime import datetime

import typer
from dateutil import parser


def parse_date(date_str: str) -> datetime:
    """Parse date string to datetime object using dateutil.parser.

    This function can handle a wide variety of date formats including:
    - YYYYMMDD (e.g., "20250323")
    - YYYY-MM-DD HH:mm:ss (e.g., "2025-03-23 09:15:46")
    - ISO format (e.g., "2025-03-23T09:15:46")
    - Various other common formats

    Args:
        date_str: Date string in any common format

    Returns:
        datetime: Parsed datetime object

    Raises:
        typer.BadParameter: If date string cannot be parsed
    """
    try:
        return parser.parse(date_str)
    except (ValueError, TypeError) as e:
        raise typer.BadParameter(f"Could not parse date: {date_str}. Error: {str(e)}")


def parse_hour(hour_str: str) -> int:
    """Parse hour string to integer."""
    try:
        hour = int(hour_str)
        if hour < 0 or hour > 23:
            raise ValueError("Hour must be between 0 and 23")
        return hour
    except ValueError as e:
        raise typer.BadParameter(str(e))


def validate_table(table_name: str, table_config: list) -> None:
    """Validate that the table exists in configuration."""
    if table_name not in table_config:
        raise typer.BadParameter(f"Table {table_name} not found in configuration")
