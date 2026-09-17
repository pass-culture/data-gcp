from pathlib import Path

import pandas as pd
from python_calamine import CalamineWorkbook


def read_table(path: Path, sheet: str, header_key: str) -> pd.DataFrame:
    """Read an INSEE-style sheet: skip the title block, start at the row whose first cell
    is `header_key`. Every value is kept as a string so codes keep their leading zeros."""
    rows = CalamineWorkbook.from_path(path).get_sheet_by_name(sheet).to_python()
    header_index = next(
        (i for i, row in enumerate(rows) if row and str(row[0]).strip() == header_key),
        None,
    )
    if header_index is None:
        raise ValueError(
            f"header '{header_key}' not found in sheet '{sheet}' of {path}"
        )

    columns = [str(c).strip() for c in rows[header_index]]
    data = [
        [_to_text(v) for v in row[: len(columns)]]
        for row in rows[header_index + 1 :]
        if row and _to_text(row[0]) is not None
    ]
    return pd.DataFrame(data, columns=columns, dtype=object)


def _to_text(value) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()
