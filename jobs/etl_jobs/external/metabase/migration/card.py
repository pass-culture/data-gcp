"""Card migration functions for native SQL and query builder cards.

Handles both native SQL and query builder cards in pMBQL format
(Metabase v0.57+), updating:
- dataset_query stages (SQL text, template tags, source-table, field refs)
- table_id (top-level table reference)
- result_metadata (field refs in metadata)
- visualization_settings (column-specific display settings)

All functions return new objects — they never mutate their inputs.

pMBQL dispatch uses ``stages[0]["lib/type"]``:
- ``"mbql.stage/native"`` → native SQL card
- ``"mbql.stage/mbql"`` → query builder card
"""

from __future__ import annotations

import copy
import logging
import re
from typing import Any

from api.models import Card
from migration.field_walker import replace_field_ids

logger = logging.getLogger(__name__)


def _get_stage_type(card: Card) -> str | None:
    """Extract the pMBQL stage type from the first stage.

    Returns:
        The ``"lib/type"`` value of the first stage, or None if not available.
    """
    dq = card.dataset_query
    if dq is None or not dq.stages:
        return None
    return dq.stages[0].get("lib/type")


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
    based on the card's ``stages[0]["lib/type"]`` (pMBQL format).

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

    stage_type = _get_stage_type(card)

    if stage_type == "mbql.stage/native":
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

    elif stage_type == "mbql.stage/mbql":
        new_dq = migrate_query_builder(
            dataset_query=card_data.get("dataset_query", {}),
            field_mapping=field_mapping,
            table_mapping=table_mapping,
        )
        new_card_data["dataset_query"] = new_dq

    else:
        logger.warning("Card %s has unknown stage type '%s', skipping", card.id, stage_type)
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
    """Migrate a native SQL dataset_query in pMBQL format.

    Updates:
    1. SQL string: schema, table, and column names (from ``stages[0]["native"]``)
    2. Template tags: dimension field IDs (from ``stages[0]["template-tags"]``)

    Args:
        dataset_query: The raw dataset_query dict (pMBQL format).
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
    stages = result.get("stages", [])

    if not stages:
        return result

    stage = stages[0]

    # Update SQL query (pMBQL: stages[0]["native"] is the SQL string)
    query = stage.get("native", "")
    if query:
        query = _replace_sql_references(query, column_mapping, old_schema, new_schema, old_table, new_table)
        stage["native"] = query

    # Update template tags (dimension field IDs)
    template_tags = stage.get("template-tags", {})
    if template_tags:
        stage["template-tags"] = _migrate_template_tags(template_tags, field_mapping)

    stages[0] = stage
    result["stages"] = stages
    return result


def migrate_query_builder(
    dataset_query: dict[str, Any], field_mapping: dict[int, int], table_mapping: dict[int, int]
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
    result: dict[str, Any] = replace_field_ids(dataset_query, field_mapping, table_mapping)
    return result


def _replace_sql_references(
    query: str, column_mapping: dict[str, str], old_schema: str, new_schema: str, old_table: str, new_table: str
) -> str:
    """Replace schema, table, and column names in SQL (schema-aware).

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
    # Strategy 1: Replace schema-qualified references (SAFE)
    pattern = _create_qualified_table_pattern(old_schema, old_table)
    query = _replace_qualified_table(query, pattern, new_schema, new_table)

    # Strategy 2: Replace unqualified references with WARNING
    query = _replace_unqualified_table_with_warning(query, old_table, new_table, old_schema)

    # Strategy 3: Column replacement (unchanged - already works)
    if column_mapping:
        lines = query.split("\n")
        new_lines = []
        for line in lines:
            if "[[" in line and "]]" in line:
                # Skip Metabase optional filter syntax
                new_lines.append(line)
            else:
                for old_col, new_col in column_mapping.items():
                    line = re.sub(rf"\b{re.escape(old_col)}\b", new_col, line)
                new_lines.append(line)
        query = "\n".join(new_lines)

    return query


def _create_qualified_table_pattern(schema_name: str, table_name: str) -> re.Pattern:
    """Create regex to match schema.table with full or component backticks.

    Matches two patterns:
    1. Full qualified backticks: `schema.table`
    2. Component backticks or none: `schema`.`table`, schema.table, `schema`.table, etc.
    """
    escaped_schema = re.escape(schema_name)
    escaped_table = re.escape(table_name)

    pattern = re.compile(
        r"([\s(`\"\[,]|^)"  # Group 1: Left boundary
        r"(?:"  # Non-capturing group for alternation
            # Option A: Full qualified backticks `schema.table`
            r"`(" + escaped_schema + r"\." + escaped_table + r")`"
            r"|"  # OR
            # Option B: Component-level backticks or no backticks
            r"(`?)" + escaped_schema + r"(`?)\."
            r"(`?)" + escaped_table + r"(`?)"
        r")"
        r"(?=[.\s,)`\"\]]|$)",  # Right lookahead WITH DOT
        re.IGNORECASE,
    )
    return pattern


def _replace_qualified_table(sql: str, pattern: re.Pattern, new_schema: str, new_table: str) -> str:
    """Replace schema.table preserving backticks (both full and component styles)."""

    def replacer(match: re.Match) -> str:
        """Replacement function that preserves backtick style."""
        left_boundary = match.group(1)

        # Check which pattern matched
        if match.group(2) is not None:
            # Option A: Full qualified backticks `schema.table`
            return left_boundary + "`" + new_schema + "." + new_table + "`"
        else:
            # Option B: Component backticks (groups 3-6)
            schema_open = match.group(3) or ""
            schema_close = match.group(4) or ""
            table_open = match.group(5) or ""
            table_close = match.group(6) or ""
            return (left_boundary + schema_open + new_schema + schema_close + "." +
                    table_open + new_table + table_close)

    return pattern.sub(replacer, sql)


def _replace_unqualified_table_with_warning(sql: str, old_table: str, new_table: str, schema_context: str) -> str:
    """Replace unqualified table references with warning log.

    Replaces standalone table names (no schema prefix) and logs a warning
    since we cannot verify the schema context.
    """
    # Pattern for unqualified table references
    escaped_table = re.escape(old_table)
    pattern = re.compile(
        r"([\s(`\"\[,]|^)"  # Group 1: Left boundary
        r"(?<!\.)"  # Not preceded by dot (would be schema.table)
        r"(?<!`\.)"  # Not preceded by backtick+dot
        r"`?" + escaped_table + r"`?" r"(?=[\s,)`\"\]]|$)",
        re.IGNORECASE,
    )

    matches = pattern.findall(sql)
    if matches:
        logger.warning(
            "Found %d unqualified reference(s) to table '%s' (no schema prefix). "
            "Replacing with '%s' but cannot verify schema context. "
            "Expected schema: %s. Consider qualifying table references.",
            len(matches),
            old_table,
            new_table,
            schema_context,
        )

    replacement = r"\g<1>" + new_table
    return pattern.sub(replacement, sql)


def _migrate_template_tags(template_tags: dict[str, Any], field_mapping: dict[int, int]) -> dict[str, Any]:
    """Update dimension field IDs in template tags.

    Template tags of type "dimension" have a "dimension" key containing
    a pMBQL field reference like ["field", {opts}, 42]. We replace the
    field ID at index 2.

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
            if isinstance(dimension, list) and len(dimension) >= 3 and isinstance(dimension[2], int):
                old_id = dimension[2]
                dimension[2] = field_mapping.get(old_id, old_id)

    return result


def _migrate_result_metadata(
    result_metadata: list[dict[str, Any]] | None, field_mapping: dict[int, int], table_mapping: dict[int, int]
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

    updated: list[dict[str, Any]] = replace_field_ids(result_metadata, field_mapping, table_mapping)
    return updated


def _migrate_visualization_settings(
    viz_settings: dict[str, Any] | None, field_mapping: dict[int, int], table_mapping: dict[int, int]
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

    updated: dict[str, Any] = replace_field_ids(viz_settings, field_mapping, table_mapping)
    return updated
