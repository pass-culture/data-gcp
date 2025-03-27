from datetime import datetime

import typer


def parse_date(date_str: str) -> datetime:
    """Parse date string to datetime object."""
    try:
        return datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        raise typer.BadParameter("Date must be in YYYYMMDD format")


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
