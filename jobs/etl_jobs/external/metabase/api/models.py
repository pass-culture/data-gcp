"""Pydantic v2 models for Metabase API responses.

Trimmed to only the models needed for card migration.
All models use extra="allow" for forward compatibility with new Metabase versions.

Since Metabase v0.57+, the API returns pMBQL (MBQL v5 / stages) format.
See docs/10-pmbql-reference.md for format details.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict
from pydantic import Field as PydanticField


class DatasetQuery(BaseModel):
    """Top-level dataset_query in pMBQL format (Metabase v0.57+).

    pMBQL uses ``"lib/type": "mbql/query"`` at the top level and a
    ``stages`` list instead of the legacy ``type`` + ``native`` / ``query``
    structure.

    Stage types:
    - ``"mbql.stage/native"`` — contains ``"native"`` (SQL string) and
      optionally ``"template-tags"``.
    - ``"mbql.stage/mbql"`` — contains ``"source-table"``, ``"fields"``,
      ``"filter"``, etc.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    lib_type: str | None = PydanticField(default=None, alias="lib/type")
    database: int | None = None
    stages: list[dict[str, Any]] | None = None


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


class CardDependencyInfo(BaseModel):
    """A single card's dependency entry in the cache."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    card_name: str
    card_type: str
    card_owner: str
    collection_path_names: str
    collection_path_ids: str


class TableDependency(BaseModel):
    """All cards depending on a given table."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: int
    schema_: str = PydanticField(alias="schema")
    cards_using_table: dict[str, CardDependencyInfo]
