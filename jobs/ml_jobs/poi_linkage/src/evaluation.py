from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from loguru import logger

from src.constants import (
    DISTANCE_METERS_COL,
    OFFERER_ADDRESS_ID_COL,
    OFFERER_ID_COL,
    OFFERER_SIREN_COL,
    POI_ID_COL,
    POI_SIRET_COL,
    SEARCH_RADIUS_METERS,
)
from src.matching import POI_COMMON_NAME_SIMILARITY_COL, POI_NAME_SIMILARITY_COL

SIRET_LENGTH = 14
POI_SIREN_COL = "poi_siren"
IS_TRUE_MATCH_COL = "is_true_match"
HAS_GROUND_TRUTH_COL = "has_ground_truth"
MANUAL_REVIEW_LABEL_COL = "manual_is_match"

HEURISTIC_COL = "heuristic"
_HEURISTIC_SCORE_COL = "_heuristic_score"


def add_ground_truth_labels(
    link_df: pd.DataFrame,
    offerer_address_offerer_df: pd.DataFrame,
    offerers_df: pd.DataFrame,
    poi_df: pd.DataFrame,
) -> pd.DataFrame:
    """Silver-standard match labels via SIRET/SIREN cross-reference.

    A candidate pair is a confirmed true match if the POI's SIRET's first 9
    digits equal the offerer's SIREN (both resolvable, SIRET exactly 14
    digits -- at least one POI row is documented to have a malformed 11-digit
    SIRET, see scripts/export_poi_parquet.py, so anything off-length is
    excluded rather than mis-sliced). An offerer_address only counts as
    having ground truth (HAS_GROUND_TRUTH_COL) if exactly one of its
    candidates is a confirmed true match -- addresses where the SIREN
    coincidentally matches more than one candidate are dropped from
    evaluation as ambiguous, not silently kept as positives.
    """
    address_siren_df = offerer_address_offerer_df.merge(
        offerers_df, on=OFFERER_ID_COL, how="left"
    )[[OFFERER_ADDRESS_ID_COL, OFFERER_SIREN_COL]]

    poi_siren_df = poi_df[[POI_ID_COL, POI_SIRET_COL]].copy()
    poi_siren_df[POI_SIREN_COL] = poi_siren_df[POI_SIRET_COL].where(
        poi_siren_df[POI_SIRET_COL].str.len() == SIRET_LENGTH
    ).str[:9]

    labeled_df = link_df.merge(
        address_siren_df, on=OFFERER_ADDRESS_ID_COL, how="left"
    ).merge(poi_siren_df[[POI_ID_COL, POI_SIREN_COL]], on=POI_ID_COL, how="left")

    labeled_df[IS_TRUE_MATCH_COL] = (
        labeled_df[OFFERER_SIREN_COL].notna()
        & labeled_df[POI_SIREN_COL].notna()
        & (labeled_df[OFFERER_SIREN_COL] == labeled_df[POI_SIREN_COL])
    )

    true_match_counts = labeled_df.loc[labeled_df[IS_TRUE_MATCH_COL]].groupby(
        OFFERER_ADDRESS_ID_COL
    ).size()
    unambiguous_addresses = true_match_counts[true_match_counts == 1].index
    ambiguous_address_count = int((true_match_counts > 1).sum())

    labeled_df[HAS_GROUND_TRUTH_COL] = labeled_df[OFFERER_ADDRESS_ID_COL].isin(
        unambiguous_addresses
    )

    total_address_count = labeled_df[OFFERER_ADDRESS_ID_COL].nunique()
    logger.info(
        f"SIREN/SIRET ground truth: {len(unambiguous_addresses)}/{total_address_count} "
        f"offerer_addresses have exactly one confirmed true match candidate "
        f"({ambiguous_address_count} excluded as ambiguous -- SIREN matched more than one candidate)."
    )

    return labeled_df


def _build_heuristic_scores(
    ground_truth_df: pd.DataFrame, radius_m: float
) -> dict[str, pd.Series]:
    """Candidate heuristics to compare, all normalized so higher == more likely a match."""
    distance_score = (1 - ground_truth_df[DISTANCE_METERS_COL] / radius_m).clip(lower=0) * 100
    name_similarity = ground_truth_df[POI_NAME_SIMILARITY_COL]
    common_name_similarity = ground_truth_df[POI_COMMON_NAME_SIMILARITY_COL]
    best_name_similarity = ground_truth_df[
        [POI_NAME_SIMILARITY_COL, POI_COMMON_NAME_SIMILARITY_COL]
    ].max(axis=1)

    return {
        "distance": distance_score,
        "poi_name_similarity": name_similarity,
        "poi_common_name_similarity": common_name_similarity,
        "poi_name_similarity_plus_distance": 0.5 * name_similarity.fillna(0) + 0.5 * distance_score,
        "poi_common_name_similarity_plus_distance": 0.5 * common_name_similarity.fillna(0)
        + 0.5 * distance_score,
        "best_name_similarity_plus_distance": 0.5 * best_name_similarity.fillna(0)
        + 0.5 * distance_score,
    }


def rank_metrics(ground_truth_df: pd.DataFrame, score_col: str) -> dict:
    """Hit@1/Hit@3/MRR of the true match when candidates are ranked by score_col (higher = better, NaN last)."""
    rank = ground_truth_df.groupby(OFFERER_ADDRESS_ID_COL)[score_col].rank(
        method="first", ascending=False, na_option="bottom"
    )
    true_match_rank = rank[ground_truth_df[IS_TRUE_MATCH_COL]]
    return {
        "hit_at_1": (true_match_rank <= 1).mean(),
        "hit_at_3": (true_match_rank <= 3).mean(),
        "mrr": (1 / true_match_rank).mean(),
        "labeled_address_count": ground_truth_df[OFFERER_ADDRESS_ID_COL].nunique(),
    }


def precision_recall_at_threshold(
    ground_truth_df: pd.DataFrame, score_col: str, thresholds: np.ndarray
) -> pd.DataFrame:
    """Precision/recall/F1 across all labeled candidate rows for each threshold (predict match iff score >= threshold)."""
    labels = ground_truth_df[IS_TRUE_MATCH_COL]
    scores = ground_truth_df[score_col].fillna(-np.inf)

    rows = []
    for threshold in thresholds:
        predicted_positive = scores >= threshold
        tp = int((predicted_positive & labels).sum())
        fp = int((predicted_positive & ~labels).sum())
        fn = int((~predicted_positive & labels).sum())
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
        rows.append(
            {"threshold": threshold, "tp": tp, "fp": fp, "fn": fn, "precision": precision, "recall": recall, "f1": f1}
        )
    return pd.DataFrame(rows)


def _best_f1_summary(ground_truth_df: pd.DataFrame, score_col: str, n_thresholds: int = 50) -> dict:
    scores = ground_truth_df[score_col].dropna()
    if scores.empty:
        return {"best_f1": 0.0, "best_threshold": float("nan"), "precision_at_best_f1": 0.0, "recall_at_best_f1": 0.0}

    thresholds = np.linspace(scores.min(), scores.max(), n_thresholds)
    sweep_df = precision_recall_at_threshold(ground_truth_df, score_col, thresholds)
    best_row = sweep_df.loc[sweep_df["f1"].idxmax()]
    return {
        "best_f1": best_row.f1,
        "best_threshold": best_row.threshold,
        "precision_at_best_f1": best_row.precision,
        "recall_at_best_f1": best_row.recall,
    }


def evaluate_heuristics(
    ground_truth_df: pd.DataFrame, radius_m: float = SEARCH_RADIUS_METERS
) -> pd.DataFrame:
    """Compare distance, each POI name column, and simple composites on the SIREN/SIRET-labeled subset.

    One row per heuristic with rank-based metrics (Hit@1/Hit@3/MRR -- does the
    true match come out on top when candidates for an address are ranked by
    this heuristic?) and the best achievable F1 over a threshold sweep.
    """
    heuristic_scores = _build_heuristic_scores(ground_truth_df, radius_m)

    rows = []
    for name, score in heuristic_scores.items():
        scored_df = ground_truth_df.assign(**{_HEURISTIC_SCORE_COL: score})
        row = {HEURISTIC_COL: name}
        row.update(rank_metrics(scored_df, _HEURISTIC_SCORE_COL))
        row.update(_best_f1_summary(scored_df, _HEURISTIC_SCORE_COL))
        rows.append(row)

    return pd.DataFrame(rows)


def plot_score_distributions(ground_truth_df: pd.DataFrame, output_dir: str) -> None:
    """Histogram each raw feature split by true/false match, saved as PNGs -- a quick visual read on separability."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    for feature in (DISTANCE_METERS_COL, POI_NAME_SIMILARITY_COL, POI_COMMON_NAME_SIMILARITY_COL):
        fig, ax = plt.subplots()
        for is_true_match, group in ground_truth_df.groupby(IS_TRUE_MATCH_COL):
            ax.hist(
                group[feature].dropna(),
                bins=30,
                alpha=0.5,
                density=True,
                label=f"is_true_match={is_true_match}",
            )
        ax.set_xlabel(feature)
        ax.set_ylabel("density")
        ax.set_title(f"{feature} distribution by match label")
        ax.legend()
        fig.savefig(Path(output_dir) / f"{feature}_distribution.png")
        plt.close(fig)


def sample_for_manual_review(labeled_df: pd.DataFrame, n: int, output_path: str) -> None:
    """Export all candidates for a random sample of addresses lacking SIREN/SIRET ground truth, for manual labeling."""
    unlabeled_addresses = labeled_df.loc[
        ~labeled_df[HAS_GROUND_TRUTH_COL], OFFERER_ADDRESS_ID_COL
    ].drop_duplicates()
    sampled_addresses = unlabeled_addresses.sample(
        n=min(n, len(unlabeled_addresses)), random_state=0
    )

    sample_df = labeled_df[labeled_df[OFFERER_ADDRESS_ID_COL].isin(sampled_addresses)].assign(
        **{MANUAL_REVIEW_LABEL_COL: ""}
    )

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    sample_df.to_csv(output_path, index=False)
    logger.info(
        f"Wrote {len(sample_df)} candidate pairs across {len(sampled_addresses)} unlabeled "
        f"offerer_addresses to {output_path} for manual review."
    )
