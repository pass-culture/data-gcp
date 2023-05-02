from dataclasses import dataclass
from datetime import datetime


@dataclass
class KeyValueInput:
    key: str
    value: str


def parse_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_int(value):
    if value is None:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def parse_bool(value):
    if value is None:
        return None
    try:
        return bool(value)
    except ValueError:
        return None


def parse_date(value):
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None


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
