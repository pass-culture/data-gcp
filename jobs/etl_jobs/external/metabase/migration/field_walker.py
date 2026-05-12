"""Recursive MBQL tree walker for safe field ID and table ID replacement.

This module replaces field IDs and table IDs ONLY in semantic MBQL contexts,
avoiding the dangerous regex-on-JSON approach of the legacy code.

The old code serialized the entire card to JSON, then did re.sub(r'\\b42\\b', '99'),
which could silently corrupt card_id, dashboard_id, parameter_id, or any other
integer that happened to match a field ID. This tree walker only touches values
in known MBQL constructs.

Since Metabase v0.57+ (pMBQL / MBQL v5), field references use the format:
    ["field", {opts_dict}, <int_id>]
where the options dict is at index 1 and the integer field ID is at index 2.
"""

from __future__ import annotations

from typing import Any


def replace_field_ids(node: Any, field_mapping: dict[int, int], table_mapping: dict[int, int] | None = None) -> Any:
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
    dict_to_walk: dict[str, Any], field_mapping: dict[int, int], table_mapping: dict[int, int]
) -> dict[str, Any]:
    """Walk a dict, replacing source-table values and recursing into children."""
    result: dict[str, Any] = {}
    for key, value in dict_to_walk.items():
        if key == "source-table" and isinstance(value, int):
            result[key] = table_mapping.get(value, value)
        else:
            result[key] = replace_field_ids(value, field_mapping, table_mapping)
    return result


def _walk_list(list_to_walk: list[Any], field_mapping: dict[int, int], table_mapping: dict[int, int]) -> list[Any]:
    """Walk a list, detecting MBQL field references and recursing into children."""
    if _is_field_ref(list_to_walk):
        # pMBQL: ["field", {opts}, <int_id>] — replace ID at index 2
        new_list = list(list_to_walk)
        old_id = list_to_walk[2]
        if isinstance(old_id, int) and old_id in field_mapping:
            new_list[2] = field_mapping[old_id]
        # Recurse into opts dict (index 1) in case it contains nested refs
        new_list[1] = replace_field_ids(new_list[1], field_mapping, table_mapping)
        # Recurse into any extra elements (index 3+)
        for i in range(3, len(new_list)):
            new_list[i] = replace_field_ids(new_list[i], field_mapping, table_mapping)
        return new_list

    if _is_foreign_key_ref(list_to_walk):
        # ["fk->", <field-ref>, <field-ref>] — recurse into both refs
        new_list = [list_to_walk[0]]
        for item in list_to_walk[1:]:
            new_list.append(replace_field_ids(item, field_mapping, table_mapping))
        return new_list

    # Generic list — recurse into each element
    return [replace_field_ids(item, field_mapping, table_mapping) for item in list_to_walk]


def _is_field_ref(field_ref_to_check: list[Any]) -> bool:
    """Check if a list is a pMBQL field reference: ["field", {opts}, <int>].

    pMBQL (v0.57+): ["field", {"lib/uuid": "...", ...}, 42]
    The opts dict is at index 1, the integer field ID is at index 2.
    """
    return (
        len(field_ref_to_check) >= 3
        and isinstance(field_ref_to_check[0], str)
        and field_ref_to_check[0] == "field"
        and isinstance(field_ref_to_check[1], dict)
        and isinstance(field_ref_to_check[2], int)
    )


def _is_foreign_key_ref(foreign_key_ref_to_check: list[Any]) -> bool:
    """Check if a list is a MBQL FK reference: ["fk->", ...].

    Legacy format for foreign key references.
    """
    return (
        len(foreign_key_ref_to_check) >= 2
        and isinstance(foreign_key_ref_to_check[0], str)
        and foreign_key_ref_to_check[0] == "fk->"
    )
