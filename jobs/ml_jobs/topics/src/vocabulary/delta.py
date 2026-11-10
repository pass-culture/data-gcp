"""Incremental change detection (spec section 7.1, ``select-delta``): which
items need (re)processing by the expensive lemmatization step.

Change key is the sha256 of the *cleaned* description (section 2's output),
compared against the stored ``description_hash``. A processing-version bump
(any change to cleaning, word lists, or the spaCy model) selects every item,
regardless of hash.
"""

import hashlib

import polars as pl

DESCRIPTION_HASH_COLUMN = "description_hash"


def hash_description(description: str) -> str:
    return hashlib.sha256(description.encode("utf-8")).hexdigest()


def hash_descriptions(descriptions: pl.Series) -> pl.Series:
    return descriptions.map_elements(hash_description, return_dtype=pl.String)


def select_delta(
    current_documents: pl.DataFrame,
    previous_documents: pl.DataFrame | None,
    *,
    processing_version: str,
    previous_processing_version: str | None,
) -> pl.DataFrame:
    """One row per item_id that needs (re)processing: new, with a changed
    ``description_hash``, or -- on a processing-version bump -- every current
    item. Items present in ``previous_documents`` but absent from
    ``current_documents`` are included with ``is_deleted = True``.

    Both dataframes need only an ``item_id`` column; ``current_documents``
    also needs ``description_hash`` (already computed on the cleaned
    description). ``previous_documents`` may be ``None``/empty when no prior
    run exists, in which case every current item is selected.
    """
    version_bumped = (
        previous_processing_version is not None
        and previous_processing_version != processing_version
    )
    no_previous_run = previous_documents is None or previous_documents.is_empty()

    if no_previous_run or version_bumped:
        new_or_changed = current_documents.select("item_id").with_columns(
            pl.lit(False).alias("is_deleted")
        )
    else:
        new_or_changed = (
            current_documents.join(
                previous_documents.select(
                    "item_id", pl.col(DESCRIPTION_HASH_COLUMN).alias("_previous_hash")
                ),
                on="item_id",
                how="left",
            )
            .filter(
                pl.col("_previous_hash").is_null()
                | (pl.col(DESCRIPTION_HASH_COLUMN) != pl.col("_previous_hash"))
            )
            .select("item_id")
            .with_columns(pl.lit(False).alias("is_deleted"))
        )

    if no_previous_run:
        deleted = pl.DataFrame(schema={"item_id": pl.String, "is_deleted": pl.Boolean})
    else:
        deleted = (
            previous_documents.join(
                current_documents.select("item_id"), on="item_id", how="anti"
            )
            .select("item_id")
            .with_columns(pl.lit(True).alias("is_deleted"))
        )

    return pl.concat([new_or_changed, deleted], how="vertical")
