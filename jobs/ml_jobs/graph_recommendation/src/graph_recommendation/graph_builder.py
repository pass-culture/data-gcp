"""Utilities to construct a PyTorch Geometric graph for book recommendations."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

import pandas as pd
import torch
from torch_geometric.data import Data

ID_COLUMN = "item_id"
DEFAULT_METADATA_COLUMNS: Sequence[str] = (
    "rayon",
    "gtl_label_level_1",
    "gtl_label_level_2",
    "gtl_label_level_3",
    "gtl_label_level_4",
    "artist_id",
)

MetadataKey = tuple[str, str]


@dataclass(slots=True)
class BookMetadataGraph:
    """Container holding the PyG graph and the supporting lookups."""

    data: Data
    book_ids: list[str]
    metadata_keys: list[MetadataKey]
    metadata_type_to_id: Mapping[str, int]
    metadata_column_names: Sequence[str]

    def save(self, output_path: Path) -> None:
        """Persist the graph ``Data`` object to disk using ``torch.save``.

        Args:
            output_path: Location of the serialized PyG ``Data`` object.
        """

        output_path.parent.mkdir(parents=True, exist_ok=True)
        torch.save(self.data, output_path)


def _normalise_value(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    result = str(value).strip()
    return result or None


def build_book_metadata_graph_from_dataframe(
    dataframe: pd.DataFrame,
    *,
    metadata_columns: Sequence[str],
    id_column: str,
) -> BookMetadataGraph:
    """Construct a bipartite book-to-metadata graph from a dataframe."""

    missing_columns = [
        column
        for column in (id_column, *metadata_columns)
        if column not in dataframe.columns
    ]
    if missing_columns:
        raise KeyError(f"Missing required columns: {', '.join(missing_columns)}")

    unique_books = dataframe[id_column].dropna().astype(str).drop_duplicates()
    book_ids = unique_books.tolist()
    book_index = {book_id: idx for idx, book_id in enumerate(book_ids)}
    metadata_keys: list[MetadataKey] = []
    metadata_index: dict[MetadataKey, int] = {}

    # Prepare metadata type ids (0 reserved for books)
    metadata_type_to_id = {"book": 0}
    for offset, column_name in enumerate(metadata_columns, start=1):
        metadata_type_to_id[column_name] = offset

    node_types: list[int] = [metadata_type_to_id["book"]] * len(book_ids)

    edges: set[tuple[int, int]] = set()

    relevant_columns = [id_column, *metadata_columns]
    for record in dataframe[relevant_columns].itertuples(index=False):
        record_dict = record._asdict()
        raw_book_id = record_dict[id_column]
        book_id = _normalise_value(raw_book_id)
        if book_id is None or book_id not in book_index:
            continue

        source_idx = book_index[book_id]

        for column in metadata_columns:
            value = _normalise_value(record_dict[column])
            if value is None:
                continue

            key = (column, value)
            target_idx = metadata_index.get(key)
            if target_idx is None:
                target_idx = len(book_ids) + len(metadata_keys)
                metadata_index[key] = target_idx
                metadata_keys.append(key)
                node_types.append(metadata_type_to_id[column])

            edge = (source_idx, target_idx)
            if edge not in edges:
                edges.add(edge)
                edges.add((target_idx, source_idx))

    if not edges:
        raise ValueError(
            "No edges were created; check that metadata columns contain values."
        )

    sorted_edges = sorted(edges)
    edge_index = torch.tensor(sorted_edges, dtype=torch.long).t().contiguous()

    data = Data(edge_index=edge_index, num_nodes=len(node_types))
    data.node_type = torch.tensor(node_types, dtype=torch.long)
    book_mask = torch.zeros(len(node_types), dtype=torch.bool)
    book_mask[: len(book_ids)] = True
    data.book_mask = book_mask
    data.metadata_mask = ~book_mask
    data.metadata_type_to_id = metadata_type_to_id
    data.metadata_columns = list(metadata_columns)

    return BookMetadataGraph(
        data=data,
        book_ids=book_ids,
        metadata_keys=metadata_keys,
        metadata_type_to_id=metadata_type_to_id,
        metadata_column_names=list(metadata_columns),
    )


def build_book_metadata_graph(
    parquet_path: Path | str,
    *,
    nrows: int | None = None,
    filters: Sequence[tuple[str, str, Iterable[object]]] | None = None,
) -> BookMetadataGraph:
    """Load a parquet file and build the corresponding book-metadata graph."""

    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found: {path}")

    read_kwargs: dict[str, object] = {}
    if filters is not None:
        read_kwargs["filters"] = list(filters)

    dataframe = pd.read_parquet(path, **read_kwargs)
    if nrows is not None:
        dataframe = dataframe.sample(nrows, random_state=42)
    return build_book_metadata_graph_from_dataframe(
        dataframe, id_column=ID_COLUMN, metadata_columns=DEFAULT_METADATA_COLUMNS
    )
