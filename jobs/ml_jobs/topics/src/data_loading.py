"""
Load book offer descriptions from BigQuery, and sample them so every GTL
label contributes (roughly) equally -- raw sampling would be dominated by
the handful of huge level-1 labels (Littérature, Jeunesse, ...) and, within
those, by their biggest level-2/3 sub-genres, starving small ones (BD
asiatiques, niche sub-genres, ...) out of downstream processing.
"""

from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

from src.config import (
    FULL_TABLE_NLP_COLS,
    GCP_PROJECT_ID,
    GTL_LEVELS,
    ML_INPUT_DATASET,
)
from src.text_cleaning import remove_boilerplate

QUERIES_DIR = Path(__file__).parent / "queries"
BOOKS_METADATA_QUERY = (QUERIES_DIR / "books_metadata.sql").read_text()

NO_SUBLABEL = "(sans sous-niveau)"


def cache_path(cache_dir: str, project: str, ml_input_dataset: str) -> Path:
    """One cache file per source table, so switching project/dataset never
    silently reuses another environment's data."""
    return Path(cache_dir) / f"items__{project}__{ml_input_dataset}.parquet"


def add_languages(df: pd.DataFrame) -> pd.DataFrame:
    """Add a `lang` column (ISO 639-1, NA when the description is empty).
    Detected once per distinct description: many editions share one."""
    from src.language import detect_languages

    text = df["offer_description"].astype("string").str.strip()
    unique = text[text.notna() & (text != "")].unique()
    logger.info(f"Detecting language for {len(unique)} distinct descriptions...")
    codes = dict(zip(unique, detect_languages(list(unique)), strict=True))
    df = df.copy()
    df["lang"] = text.map(codes).astype("string")
    logger.info("\n" + df["lang"].value_counts().head(10).to_string())
    return df


def load_items(
    cache_dir: str | None,
    project: str = GCP_PROJECT_ID,
    ml_input_dataset: str = ML_INPUT_DATASET,
    *,
    lang: str | None = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """Item-level query (item_id, offer_name, offer_description, gtl_id, GTL
    levels) plus a detected `lang` column, cached as Parquet so repeat runs
    don't re-hit BigQuery nor re-detect languages (pass refresh=True to
    re-query). The query itself lives in queries/books_metadata.sql, not
    inline, so it can be reviewed/edited independently of this module.

    With `lang`, only items in that language are returned -- read straight
    from the cache with a Parquet filter, so other languages are never
    loaded. The cache itself always holds every language."""
    cache = cache_path(cache_dir, project, ml_input_dataset) if cache_dir else None
    if cache and cache.exists() and not refresh:
        age_days = (time.time() - cache.stat().st_mtime) / 86_400
        logger.info(
            f"Loading cached item table from {cache} ({age_days:.1f} days old, "
            f"use --refresh to re-query)"
        )
        import pyarrow.parquet as pq

        names = pq.read_schema(cache).names
        if "lang" in names:
            filters = None if lang is None else [("lang", "==", lang)]
            # Full-table NLP columns (added by the `lemmatize` command) are
            # heavy as pandas objects, unused by sampling, and `entities`
            # would clash with preprocess's: join them on item_id when needed.
            columns = [c for c in names if c not in FULL_TABLE_NLP_COLS]
            return pd.read_parquet(cache, columns=columns, filters=filters)
        df = pd.read_parquet(cache)
        logger.info("Cached item table has no `lang` column yet, adding it")
    else:
        logger.info(
            f"Querying book metadata from BigQuery project={project}, "
            f"dataset={ml_input_dataset}"
        )
        from google.cloud import bigquery

        query = BOOKS_METADATA_QUERY.format(
            project=project, ml_input_dataset=ml_input_dataset
        )
        df = bigquery.Client(project=project).query(query).to_dataframe()
    df = add_languages(df)
    if cache:
        cache.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache, index=False)
        logger.info(f"Cached item table to {cache}")
    return df if lang is None else df[df["lang"] == lang]


def filter_language(df: pd.DataFrame, lang: str | None) -> pd.DataFrame:
    """Keep only items whose description is in `lang` (None keeps all)."""
    if lang is None:
        return df
    kept = df[df["lang"] == lang]
    logger.info(f"{len(kept)}/{len(df)} items kept with lang={lang}")
    return kept


def clean_items(df: pd.DataFrame) -> pd.DataFrame:
    """Keep one row per item_id, with a non-empty description and a level-1 label."""
    df = df.copy()
    df["offer_description"] = df["offer_description"].astype("string").str.strip()
    before = len(df)
    df = df[df["offer_description"].notna() & (df["offer_description"] != "")]
    df = df[
        df["gtl_label_level_1"].notna()
        & (df["gtl_label_level_1"].astype(str).str.strip() != "")
    ]
    df = df.drop_duplicates(subset=["item_id"])
    logger.info(
        f"{len(df)}/{before} items kept after dropping empty descriptions "
        f"/ missing level-1 label / item_id duplicates."
    )
    return df


def _label_at(value) -> str:
    """Items whose GTL path stops before this level (no sub-genre) get their
    own bucket instead of being dropped or silently merged into a sibling."""
    if pd.isna(value) or str(value).strip() == "":
        return NO_SUBLABEL
    return str(value)


def waterfill_quotas(sizes: dict[str, int], n_total: int) -> dict[str, int]:
    """Split n_total as evenly as possible across groups, capping each group
    at its own size and redistributing the slack to the groups that still
    have room (so small groups aren't starved and large groups don't hog the
    sample)."""
    quota = dict.fromkeys(sizes, 0)
    remaining = set(sizes)
    budget = min(n_total, sum(sizes.values()))
    while remaining and budget > 0:
        share = budget / len(remaining)
        capped = [g for g in remaining if sizes[g] - quota[g] <= share]
        if not capped:
            floor_share = int(share)
            for g in remaining:
                quota[g] += floor_share
            budget -= floor_share * len(remaining)
            # distribute the integer leftover; sorted() because set order of
            # str depends on PYTHONHASHSEED, which would make seeds useless
            for g in sorted(remaining)[:budget]:
                quota[g] += 1
            budget = 0
            break
        for g in capped:
            capacity = sizes[g] - quota[g]
            quota[g] += capacity
            budget -= capacity
            remaining.discard(g)
    return quota


def recursive_stratified_sample(
    df: pd.DataFrame, levels: list[str], n_total: int, seed: int
) -> pd.DataFrame:
    """Normalize the sample recursively across `levels`: split n_total evenly
    (waterfilled, see waterfill_quotas) across level[0]'s labels, then split
    each of those quotas evenly across level[1]'s labels *within that
    level[0] group*, and so on. Items that don't go as deep as a given level
    fall into a NO_SUBLABEL bucket at that level rather than being dropped.
    Beyond the last level in `levels`, sampling within the deepest bucket is
    a uniform random draw (no further debiasing)."""
    rng = np.random.default_rng(seed)
    label_cols = {lvl: df[lvl].map(_label_at) for lvl in levels}

    def recurse(idx: np.ndarray, depth: int, quota: int) -> list[np.ndarray]:
        if quota <= 0 or len(idx) == 0:
            return []
        if depth == len(levels):
            take = min(quota, len(idx))
            return [rng.choice(idx, size=take, replace=False)]
        labels = label_cols[levels[depth]].reindex(idx)
        sizes = labels.value_counts().to_dict()
        quotas = waterfill_quotas(sizes, quota)
        parts = []
        for label, q in quotas.items():
            if q <= 0:
                continue
            sub_idx = idx[labels.values == label]
            parts.extend(recurse(sub_idx, depth + 1, q))
        return parts

    parts = recurse(df.index.to_numpy(), 0, n_total)
    if not parts:
        logger.warning(f"Empty sample (n_total={n_total}, pool={len(df)} items)")
        return df.iloc[:0].reset_index(drop=True)
    chosen = np.concatenate(parts)
    sample = df.loc[chosen].sample(frac=1, random_state=seed).reset_index(drop=True)
    report_balance(df, sample, levels, label_cols)
    return sample


def report_balance(
    pool: pd.DataFrame,
    sample: pd.DataFrame,
    levels: list[str],
    pool_labels: dict[str, pd.Series],
) -> None:
    """Report balance per *path* (e.g. level-1 > level-2), not per bare label
    name: the same name -- especially NO_SUBLABEL -- recurs under many
    different parents, and waterfilling only balances siblings within one
    parent, so pooling same-named buckets across parents would understate
    how balanced the sample actually is."""
    logger.info(
        f"Recursive stratified sample: {len(sample)}/{len(pool)} items, "
        f"balanced across {' > '.join(levels)}"
    )
    sample_labels = {lvl: sample[lvl].map(_label_at) for lvl in levels}
    for depth, lvl in enumerate(levels):
        path_levels = levels[: depth + 1]
        pool_paths = pd.Series(
            list(zip(*(pool_labels[level] for level in path_levels), strict=False))
        )
        sample_paths = pd.Series(
            list(zip(*(sample_labels[level] for level in path_levels), strict=False))
        )
        pool_sizes = pool_paths.value_counts()
        sample_sizes = sample_paths.value_counts().reindex(pool_sizes.index).fillna(0)
        cv = (
            sample_sizes.std() / sample_sizes.mean()
            if sample_sizes.mean()
            else float("nan")
        )
        logger.info(
            f"  up to {lvl}: {len(pool_sizes)} distinct paths, sampled per path "
            f"min={int(sample_sizes.min())} median={int(sample_sizes.median())} "
            f"max={int(sample_sizes.max())} (CV={cv:.2f}; lower = more balanced)"
        )
    lvl1 = levels[0]
    detail = (
        pd.DataFrame(
            {
                "n_eligible": pool_labels[lvl1].value_counts(),
                "n_sampled": sample_labels[lvl1].value_counts(),
            }
        )
        .fillna(0)
        .astype(int)
        .sort_values("n_sampled", ascending=False)
    )
    with pd.option_context("display.width", 120, "display.max_rows", None):
        logger.info("\n" + detail.to_string())


def load_and_sample(
    cache_dir: str | None,
    project: str,
    sample_size: int,
    strata_levels: int,
    seed: int,
    ml_input_dataset: str = ML_INPUT_DATASET,
    *,
    lang: str | None = "fr",
    refresh: bool = False,
) -> pd.DataFrame:
    """End-to-end: load the item table (cached), clean it, keep one
    language, strip publisher boilerplate, and draw a recursively stratified
    sample."""
    df = clean_items(
        load_items(cache_dir, project, ml_input_dataset, lang=lang, refresh=refresh)
    )
    df = remove_boilerplate(filter_language(df, lang))
    strata = GTL_LEVELS[:strata_levels]
    return recursive_stratified_sample(df, strata, sample_size, seed)
