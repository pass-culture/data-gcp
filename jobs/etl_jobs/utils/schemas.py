import datetime
from typing import List, Type, get_args, get_origin

from google.cloud import bigquery
from pydantic import BaseModel
from pydantic.fields import FieldInfo


def pydantic_to_bigquery_schema(model: Type[BaseModel]) -> List[bigquery.SchemaField]:
    """
    Converts a Pydantic model to a BigQuery schema list.
    """
    schema = []
    for name, field in model.model_fields.items():
        mode = "REQUIRED" if field.is_required() else "NULLABLE"
        bq_type = _get_bq_type(field)

        schema.append(bigquery.SchemaField(name, bq_type, mode=mode))
    return schema


def _get_bq_type(field: FieldInfo) -> str:
    """Derive BigQuery type from Pydantic field."""
    # Check for explicit override
    if field.json_schema_extra and "bigquery_type" in field.json_schema_extra:
        return field.json_schema_extra["bigquery_type"]

    type_ = field.annotation
    origin = get_origin(type_)

    # Handle Optional[T] / Union[T, None]
    if origin is not None:
        args = get_args(type_)
        non_none_args = [arg for arg in args if arg is not type(None)]
        if non_none_args:
            type_ = non_none_args[0]
            # Unwrap again if needed (e.g. Optional[List[int]])
            # Simple handling for now

    if type_ is int:
        return "INTEGER"
    elif type_ is str:
        return "STRING"
    elif type_ is float:
        return "FLOAT"
    elif type_ is bool:
        return "BOOLEAN"
    elif type_ is datetime.datetime:
        return "DATETIME"  # Default to DATETIME, override to TIMESTAMP if needed
    elif type_ is datetime.date:
        return "DATE"

    # Fallback to STRING for unknown types (e.g. nested dicts often stored as JSON strings)
    return "STRING"
