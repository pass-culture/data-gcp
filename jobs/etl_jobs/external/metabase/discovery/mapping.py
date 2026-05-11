"""Field and table mapping via Metabase API.

Builds field mappings by matching field names between old and new tables.
"""

from __future__ import annotations

import logging

from api.client import MetabaseClient

logger = logging.getLogger(__name__)


def build_field_mapping(
    metabase_client: MetabaseClient,
    legacy_table_id: int,
    new_table_id: int,
    column_mapping: dict[str, str] | None = None,
) -> dict[int, int]:
    """Build {old_field_id: new_field_id} mapping by matching field names.

    For each field in the legacy table:
    1. Apply column_mapping to get the new column name (if renamed)
    2. Find the matching field in the new table by name
    3. Map old_field_id → new_field_id

    Args:
        metabase_client: Authenticated Metabase client.
        legacy_table_id: Metabase table ID for the legacy table.
        new_table_id: Metabase table ID for the new table.
        column_mapping: Optional {old_column_name: new_column_name} mapping.

    Returns:
        A dict mapping old field IDs to new field IDs.
    """
    if column_mapping is None:
        column_mapping = {}

    legacy_fields = metabase_client.get_table_fields(legacy_table_id)
    new_fields = metabase_client.get_table_fields(new_table_id)

    # Index new fields by name for O(1) lookup
    new_fields_by_name: dict[str, int] = {f.name: f.id for f in new_fields}

    mapping: dict[int, int] = {}
    unmapped: list[str] = []

    for field in legacy_fields:
        # Apply column rename if it exists
        mapped_name = column_mapping.get(field.name, field.name)
        new_field_id = new_fields_by_name.get(mapped_name)

        if new_field_id is not None:
            mapping[field.id] = new_field_id
        else:
            unmapped.append(field.name)

    if unmapped:
        logger.warning("Could not map %d fields: %s", len(unmapped), ", ".join(unmapped))

    logger.info("Built field mapping: %d fields mapped (%d unmapped)", len(mapping), len(unmapped))
    return mapping


def build_table_mapping(legacy_table_id: int, new_table_id: int) -> dict[int, int]:
    """Build {old_table_id: new_table_id} mapping.

    Simple helper for consistency with field_mapping interface.

    Args:
        legacy_table_id: Old Metabase table ID.
        new_table_id: New Metabase table ID.

    Returns:
        A single-entry dict mapping old table ID to new.
    """
    return {legacy_table_id: new_table_id}
