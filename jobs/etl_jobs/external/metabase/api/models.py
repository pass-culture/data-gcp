"""Pydantic v2 models for Metabase API responses.

Trimmed to only the models needed for card migration.
All models use extra="allow" for forward compatibility with new Metabase versions.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict
from pydantic import Field as PydanticField


class TemplateTags(BaseModel):
    """A single template tag in a native SQL query."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: str | None = None
    name: str | None = None
    display_name: str | None = PydanticField(default=None, alias="display-name")
    type: str | None = None
    dimension: list[Any] | None = None


class NativeQuery(BaseModel):
    """Native SQL query inside a dataset_query."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    query: str | None = None
    template_tags: dict[str, TemplateTags] | None = PydanticField(
        default=None, alias="template-tags"
    )


class QueryBuilderQuery(BaseModel):
    """Query builder (structured) query inside a dataset_query."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    source_table: int | None = PydanticField(default=None, alias="source-table")
    fields: list[Any] | None = None
    filter: list[Any] | None = None
    breakout: list[Any] | None = None
    aggregation: list[Any] | None = None
    joins: list[Any] | None = None
    order_by: list[Any] | None = PydanticField(default=None, alias="order-by")
    expressions: dict[str, Any] | None = None
    limit: int | None = None


class DatasetQuery(BaseModel):
    """Top-level dataset_query on a Card."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    type: str | None = None
    database: int | None = None
    native: NativeQuery | None = None
    query: QueryBuilderQuery | None = None


class ResultMetadataColumn(BaseModel):
    """A single column in result_metadata."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str | None = None
    display_name: str | None = None
    base_type: str | None = None
    field_ref: list[Any] | None = PydanticField(default=None, alias="field_ref")
    id: int | None = None


class Card(BaseModel):
    """A Metabase card (question/saved question)."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: int | None = None
    name: str | None = None
    description: str | None = None
    display: str | None = None
    dataset_query: DatasetQuery | None = None
    table_id: int | None = None
    visualization_settings: dict[str, Any] | None = None
    result_metadata: list[ResultMetadataColumn] | None = None
    collection_id: int | None = None
    archived: bool | None = None


class MetabaseField(BaseModel):
    """A field (column) in Metabase's metadata."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: int
    name: str
    display_name: str | None = None
    base_type: str | None = None
    semantic_type: str | None = None
    table_id: int | None = None


class Table(BaseModel):
    """A table in Metabase's metadata."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: int
    name: str
    schema_: str | None = PydanticField(default=None, alias="schema")
    db_id: int | None = None
    fields: list[MetabaseField] | None = None
