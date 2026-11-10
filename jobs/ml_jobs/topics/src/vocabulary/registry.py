"""Word registry: term ids, POS, and document frequencies (spec section 7.1,
``update-registry``).

Document frequencies are recomputed from the *full* current
``topic_item_document`` state (this run's delta already merged in), not just
the delta -- a word's df_total/df_by_gtl can change because of items this run
never touched. Stop words are still registered (flagged), with a zero
document frequency, since they never survive pruning into any document.
"""

import datetime

import polars as pl

POS_NOUN = "NOUN"
POS_VERB = "VERB"
POS_BOTH = "BOTH"

TOPIC_VOCABULARY_COLUMNS = [
    "term_id",
    "term",
    "pos",
    "df_total",
    "df_by_gtl",
    "is_stop_word",
    "first_seen",
    "last_seen",
]


def compute_term_frequencies(documents: pl.DataFrame) -> pl.DataFrame:
    """One row per term present in at least one document: its ``pos``
    (``BOTH`` if seen as both a noun and a verb across documents), ``df_total``
    (number of distinct items containing it), and ``df_by_gtl`` (per item GTL
    id, so any corpus's frequency is a sum over its GTL ids).

    ``documents`` needs ``item_id``, ``gtl_id``, ``noun_lemmas``, ``verb_lemmas``.
    """
    nouns = (
        documents.select("item_id", "gtl_id", pl.col("noun_lemmas").alias("term"))
        .explode("term")
        .drop_nulls("term")
        .with_columns(pl.lit(POS_NOUN).alias("pos"))
    )
    verbs = (
        documents.select("item_id", "gtl_id", pl.col("verb_lemmas").alias("term"))
        .explode("term")
        .drop_nulls("term")
        .with_columns(pl.lit(POS_VERB).alias("pos"))
    )
    occurrences = pl.concat([nouns, verbs]).unique(subset=["item_id", "term", "pos"])

    if occurrences.is_empty():
        return pl.DataFrame(
            schema={
                "term": pl.String,
                "pos": pl.String,
                "df_total": pl.Int64,
                "df_by_gtl": pl.List(pl.Struct({"gtl_id": pl.String, "df": pl.Int64})),
                "is_stop_word": pl.Boolean,
            }
        )

    pos_by_term = (
        occurrences.group_by("term")
        .agg(
            pl.col("pos").n_unique().alias("n_pos"),
            pl.col("pos").first().alias("pos"),
        )
        .with_columns(
            pl.when(pl.col("n_pos") > 1)
            .then(pl.lit(POS_BOTH))
            .otherwise(pl.col("pos"))
            .alias("pos")
        )
        .select("term", "pos")
    )

    df_total = (
        occurrences.select("item_id", "term")
        .unique()
        .group_by("term")
        .agg(pl.len().alias("df_total"))
    )

    df_by_gtl = (
        occurrences.select("item_id", "term", "gtl_id")
        .unique()
        .group_by("term", "gtl_id")
        .agg(pl.len().alias("df"))
        .group_by("term")
        .agg(pl.struct(["gtl_id", "df"]).alias("df_by_gtl"))
    )

    return (
        pos_by_term.join(df_total, on="term", how="left")
        .join(df_by_gtl, on="term", how="left")
        .with_columns(pl.lit(False).alias("is_stop_word"))
    )


def build_stop_word_entries(
    stop_words: set[str], known_terms: set[str]
) -> pl.DataFrame:
    """Stop words never seen in a document (every occurrence was pruned
    before the ``document`` column was built) are still registered, with a
    zero document frequency and ``pos`` left null (they were never
    POS-classified through the kept pipeline).
    """
    unseen = sorted(stop_words - known_terms)
    return pl.DataFrame(
        {
            "term": unseen,
            "pos": pl.Series([None] * len(unseen), dtype=pl.String),
            "df_total": [0] * len(unseen),
            "df_by_gtl": pl.Series(
                [[] for _ in unseen],
                dtype=pl.List(pl.Struct({"gtl_id": pl.String, "df": pl.Int64})),
            ),
            "is_stop_word": [True] * len(unseen),
        }
    )


def merge_vocabulary(
    term_rows: pl.DataFrame,
    existing_vocabulary: pl.DataFrame | None,
    *,
    now: datetime.datetime,
) -> pl.DataFrame:
    """Assign stable ``term_id``s (existing terms keep theirs; new ones get
    the next unused id, never reused even across runs) and
    ``first_seen``/``last_seen``.
    """
    if existing_vocabulary is None or existing_vocabulary.is_empty():
        next_id = 1
        existing_ids = pl.DataFrame(
            schema={
                "term": pl.String,
                "term_id": pl.Int64,
                "first_seen": pl.Datetime(time_zone="UTC"),
            }
        )
    else:
        next_id = existing_vocabulary["term_id"].max() + 1
        existing_ids = existing_vocabulary.select("term", "term_id", "first_seen")

    merged = term_rows.join(existing_ids, on="term", how="left").sort("term")

    assigned_ids: list[int] = []
    counter = next_id
    for term_id in merged["term_id"].to_list():
        if term_id is None:
            assigned_ids.append(counter)
            counter += 1
        else:
            assigned_ids.append(term_id)

    merged = merged.with_columns(
        pl.Series("term_id", assigned_ids, dtype=pl.Int64),
        pl.col("first_seen").fill_null(now),
        pl.lit(now).alias("last_seen"),
    )
    return merged.select(TOPIC_VOCABULARY_COLUMNS)


def update_vocabulary(
    documents: pl.DataFrame,
    stop_words: set[str],
    existing_vocabulary: pl.DataFrame | None,
    *,
    now: datetime.datetime,
) -> pl.DataFrame:
    """The full ``topic_vocabulary`` table, recomputed from ``documents``
    (the current, non-deleted, non-empty ``topic_item_document`` rows) plus
    the stop-word list.
    """
    frequencies = compute_term_frequencies(documents)
    stop_word_entries = build_stop_word_entries(
        stop_words, set(frequencies["term"].to_list())
    )
    term_rows = pl.concat([frequencies, stop_word_entries], how="diagonal_relaxed")
    return merge_vocabulary(term_rows, existing_vocabulary, now=now)
