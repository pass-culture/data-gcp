"""Card migration functions for native SQL and query builder cards.

Handles both native SQL and query builder cards, updating:
- dataset_query (SQL text, template tags, source-table, field refs)
- table_id (top-level table reference)
- result_metadata (field refs in metadata)
- visualization_settings (column-specific display settings)

All functions return new objects — they never mutate their inputs.
"""

from __future__ import annotations

import copy
import logging
import re
from typing import Any

from api.models import Card
from migration.field_walker import replace_field_ids

logger = logging.getLogger(__name__)


def migrate_card(
    card: Card,
    field_mapping: dict[int, int],
    table_mapping: dict[int, int],
    column_mapping: dict[str, str],
    old_schema: str,
    new_schema: str,
    old_table: str,
    new_table: str,
) -> Card:
    """Migrate a card to use new table/field references.

    Dispatches to migrate_native_query or migrate_query_builder
    based on the card's dataset_query.type.

    Args:
        card: The original Card.
        field_mapping: {old_field_id: new_field_id}.
        table_mapping: {old_table_id: new_table_id}.
        column_mapping: {old_column_name: new_column_name}.
        old_schema: Legacy schema name.
        new_schema: New schema name.
        old_table: Legacy table name.
        new_table: New table name.

    Returns:
        A new Card with updated references.
    """
    card_data = card.model_dump(by_alias=True)
    new_card_data = copy.deepcopy(card_data)

    dataset_query = card.dataset_query
    if dataset_query is None:
        logger.warning("Card %s has no dataset_query, skipping", card.id)
        return card

    query_type = dataset_query.type

    if query_type == "native":
        new_dq = migrate_native_query(
            dataset_query=card_data.get("dataset_query", {}),
            column_mapping=column_mapping,
            field_mapping=field_mapping,
            old_schema=old_schema,
            new_schema=new_schema,
            old_table=old_table,
            new_table=new_table,
        )
        new_card_data["dataset_query"] = new_dq

    elif query_type == "query":
        new_dq = migrate_query_builder(
            dataset_query=card_data.get("dataset_query", {}),
            field_mapping=field_mapping,
            table_mapping=table_mapping,
        )
        new_card_data["dataset_query"] = new_dq

    else:
        logger.warning(
            "Card %s has unknown query type '%s', skipping",
            card.id,
            query_type,
        )
        return card

    # Update top-level table_id
    if new_card_data.get("table_id") is not None:
        old_table_id = new_card_data["table_id"]
        new_card_data["table_id"] = table_mapping.get(old_table_id, old_table_id)

    # Update result_metadata field refs
    new_card_data["result_metadata"] = _migrate_result_metadata(
        new_card_data.get("result_metadata"),
        field_mapping,
        table_mapping,
    )

    # Update visualization_settings field refs
    new_card_data["visualization_settings"] = _migrate_visualization_settings(
        new_card_data.get("visualization_settings"),
        field_mapping,
        table_mapping,
    )

    return Card.model_validate(new_card_data)


def migrate_native_query(
    dataset_query: dict[str, Any],
    column_mapping: dict[str, str],
    field_mapping: dict[int, int],
    old_schema: str,
    new_schema: str,
    old_table: str,
    new_table: str,
) -> dict[str, Any]:
    """Migrate a native SQL dataset_query.

    Updates:
    1. SQL string: schema, table, and column names
    2. Template tags: dimension field IDs

    Args:
        dataset_query: The raw dataset_query dict.
        column_mapping: {old_column_name: new_column_name}.
        field_mapping: {old_field_id: new_field_id}.
        old_schema: Legacy schema name.
        new_schema: New schema name.
        old_table: Legacy table name.
        new_table: New table name.

    Returns:
        A new dataset_query dict with updated SQL and template tags.
    """
    result = copy.deepcopy(dataset_query)
    native = result.get("native", {})

    if not native:
        return result

    # Update SQL query
    query = native.get("query", "")
    if query:
        query = _replace_sql_references(
            query, column_mapping, old_schema, new_schema, old_table, new_table
        )
        native["query"] = query

    # Update template tags (dimension field IDs)
    template_tags = native.get("template-tags", {})
    if template_tags:
        native["template-tags"] = _migrate_template_tags(template_tags, field_mapping)

    result["native"] = native
    return result


def migrate_query_builder(
    dataset_query: dict[str, Any],
    field_mapping: dict[int, int],
    table_mapping: dict[int, int],
) -> dict[str, Any]:
    """Migrate a query builder dataset_query.

    Uses the tree walker to safely replace field IDs and source-table
    references throughout the entire MBQL structure.

    Args:
        dataset_query: The raw dataset_query dict.
        field_mapping: {old_field_id: new_field_id}.
        table_mapping: {old_table_id: new_table_id}.

    Returns:
        A new dataset_query dict with updated field and table refs.
    """
    # Use the tree walker on the entire dataset_query
    # It will handle source-table, field refs, fk refs, etc.
    return replace_field_ids(dataset_query, field_mapping, table_mapping)


def _replace_sql_references(
    query: str,
    column_mapping: dict[str, str],
    old_schema: str,
    new_schema: str,
    old_table: str,
    new_table: str,
) -> str:
    """Replace schema, table, and column names in a SQL string.

    Column replacements skip lines containing Metabase optional filter
    syntax ([[ ... ]]) to avoid breaking parameterized queries.

    Args:
        query: The SQL query string.
        column_mapping: {old_column_name: new_column_name}.
        old_schema: Legacy schema name.
        new_schema: New schema name.
        old_table: Legacy table name.
        new_table: New table name.

    Returns:
        The updated SQL string.
    """
    # Replace schema.table pattern first (more specific)
    query = query.replace(
        f"{old_schema}.{old_table}",
        f"{new_schema}.{new_table}",
    )

    # Replace standalone table name references
    # Use word boundary to avoid partial matches
    query = re.sub(
        rf"\b{re.escape(old_table)}\b",
        new_table,
        query,
    )

    # Replace column names, skipping Metabase filter lines
    if column_mapping:
        lines = query.split("\n")
        new_lines = []
        for line in lines:
            if "[[" in line and "]]" in line:
                # Skip Metabase optional filter syntax
                new_lines.append(line)
            else:
                for old_col, new_col in column_mapping.items():
                    line = re.sub(
                        rf"\b{re.escape(old_col)}\b",
                        new_col,
                        line,
                    )
                new_lines.append(line)
        query = "\n".join(new_lines)

    return query


def _migrate_template_tags(
    template_tags: dict[str, Any],
    field_mapping: dict[int, int],
) -> dict[str, Any]:
    """Update dimension field IDs in template tags.

    Template tags of type "dimension" have a "dimension" key containing
    a field reference like ["field", 42, null]. We replace the field ID.

    Args:
        template_tags: Dict of template tag name → tag definition.
        field_mapping: {old_field_id: new_field_id}.

    Returns:
        Updated template tags dict.
    """
    result = copy.deepcopy(template_tags)

    for _tag_name, tag_def in result.items():
        if not isinstance(tag_def, dict):
            continue

        if tag_def.get("type") == "dimension" and "dimension" in tag_def:
            dimension = tag_def["dimension"]
            if (
                isinstance(dimension, list)
                and len(dimension) >= 2
                and isinstance(dimension[1], int)
            ):
                old_id = dimension[1]
                dimension[1] = field_mapping.get(old_id, old_id)

    return result


def _migrate_result_metadata(
    result_metadata: list[dict[str, Any]] | None,
    field_mapping: dict[int, int],
    table_mapping: dict[int, int],
) -> list[dict[str, Any]] | None:
    """Update field refs in result_metadata.

    Args:
        result_metadata: List of column metadata dicts.
        field_mapping: {old_field_id: new_field_id}.
        table_mapping: {old_table_id: new_table_id}.

    Returns:
        Updated result_metadata.
    """
    if not result_metadata:
        return result_metadata

    return replace_field_ids(result_metadata, field_mapping, table_mapping)


def _migrate_visualization_settings(
    viz_settings: dict[str, Any] | None,
    field_mapping: dict[int, int],
    table_mapping: dict[int, int],
) -> dict[str, Any] | None:
    """Update field refs in visualization_settings.

    Visualization settings can contain field references in column
    formatting settings, click behaviors, etc.

    Args:
        viz_settings: The visualization_settings dict.
        field_mapping: {old_field_id: new_field_id}.
        table_mapping: {old_table_id: new_table_id}.

    Returns:
        Updated visualization_settings.
    """
    if not viz_settings:
        return viz_settings

    return replace_field_ids(viz_settings, field_mapping, table_mapping)
