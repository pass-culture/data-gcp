"""Recursive MBQL tree walker for safe field ID and table ID replacement.

This module replaces field IDs and table IDs ONLY in semantic MBQL contexts,
avoiding the dangerous regex-on-JSON approach of the legacy code.

The old code serialized the entire card to JSON, then did re.sub(r'\\b42\\b', '99'),
which could silently corrupt card_id, dashboard_id, parameter_id, or any other
integer that happened to match a field ID. This tree walker only touches values
in known MBQL constructs.
"""

from __future__ import annotations

from typing import Any


def replace_field_ids(
    node: Any,
    field_mapping: dict[int, int],
    table_mapping: dict[int, int] | None = None,
) -> Any:
    """Recursively walk an MBQL structure, replacing field and table IDs.

    Args:
        node: Any JSON-compatible value (dict, list, str, int, None).
        field_mapping: {old_field_id: new_field_id} for field references.
        table_mapping: {old_table_id: new_table_id} for source-table references.
            If None, table IDs are not replaced.

    Returns:
        A new structure with IDs replaced. The input is never mutated.
    """
    if table_mapping is None:
        table_mapping = {}

    if isinstance(node, dict):
        return _walk_dict(node, field_mapping, table_mapping)
    if isinstance(node, list):
        return _walk_list(node, field_mapping, table_mapping)
    return node


def _walk_dict(
    d: dict[str, Any],
    field_mapping: dict[int, int],
    table_mapping: dict[int, int],
) -> dict[str, Any]:
    """Walk a dict, replacing source-table values and recursing into children."""
    result: dict[str, Any] = {}
    for key, value in d.items():
        if key == "source-table" and isinstance(value, int):
            result[key] = table_mapping.get(value, value)
        else:
            result[key] = replace_field_ids(value, field_mapping, table_mapping)
    return result


def _walk_list(
    lst: list[Any],
    field_mapping: dict[int, int],
    table_mapping: dict[int, int],
) -> list[Any]:
    """Walk a list, detecting MBQL field references and recursing into children."""
    if _is_field_ref(lst):
        # ["field", <id>, <options>] — replace the ID (index 1)
        new_list = list(lst)
        old_id = lst[1]
        if isinstance(old_id, int) and old_id in field_mapping:
            new_list[1] = field_mapping[old_id]
        # Recurse into options (index 2+) in case they contain nested refs
        for i in range(2, len(new_list)):
            new_list[i] = replace_field_ids(new_list[i], field_mapping, table_mapping)
        return new_list

    if _is_fk_ref(lst):
        # ["fk->", <field-ref>, <field-ref>] — recurse into both refs
        new_list = [lst[0]]
        for item in lst[1:]:
            new_list.append(replace_field_ids(item, field_mapping, table_mapping))
        return new_list

    # Generic list — recurse into each element
    return [replace_field_ids(item, field_mapping, table_mapping) for item in lst]


def _is_field_ref(lst: list[Any]) -> bool:
    """Check if a list is a MBQL field reference: ["field", <int>, ...].

    MBQL v4: ["field", 42, null]
    MBQL v5: ["field", 42, {"base-type": "type/Integer"}]
    Also: ["field", 42, {"source-field": 99}]
    """
    return (
        len(lst) >= 2
        and isinstance(lst[0], str)
        and lst[0] == "field"
        and isinstance(lst[1], int)
    )


def _is_fk_ref(lst: list[Any]) -> bool:
    """Check if a list is a MBQL FK reference: ["fk->", ...].

    Legacy format for foreign key references.
    """
    return len(lst) >= 2 and isinstance(lst[0], str) and lst[0] == "fk->"
