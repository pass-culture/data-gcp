from dataclasses import dataclass
from datetime import datetime


@dataclass
class KeyValueInput:
    key: str
    value: str


def parse_float(value, default_value=None):
    if value is None:
        return default_value
    try:
        return float(value)
    except ValueError:
        return default_value


def parse_int(value, default_value=None):
    if value is None:
        return default_value
    try:
        return int(float(value))
    except ValueError:
        return default_value


def parse_bool(value, default_value=None):
    if value is None:
        return default_value
    try:
        return bool(value)
    except ValueError:
        return default_value


def parse_date(value, default_value=None):
    if value is None:
        return default_value
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return default_value


def parse_to_list(value):
    if value is None:
        return None
    if isinstance(value, list):
        return value if len(value) > 0 else None
    else:
        return [value]


def parse_to_list_of_dict(value):
    if value is None:
        return None
    try:
        if isinstance(value, list):
            return [KeyValueInput(**v) for v in value]
        else:
            return [KeyValueInput(**value)]
    except (ValueError, TypeError):
        return None
