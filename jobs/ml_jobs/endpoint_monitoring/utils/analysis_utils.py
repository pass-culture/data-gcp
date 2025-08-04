"""Utility functions for analyzing predictions and computing metrics."""

from collections import defaultdict
from typing import Any

import numpy as np


def convert_to_dict(obj: Any) -> Any:
    """Convert MapComposite and RepeatedComposite objects to dictionaries."""
    if hasattr(obj, "items"):  # For MapComposite
        return {k: convert_to_dict(v) for k, v in obj.items()}
    elif hasattr(obj, "__iter__") and not isinstance(
        obj, str | bytes
    ):  # For RepeatedComposite and other iterables
        return [convert_to_dict(item) for item in obj]
    else:
        return obj


def compute_cosine_similarity(vec1: np.ndarray, vec2: np.ndarray) -> float:
    """Compute cosine similarity between two vectors."""
    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)
    if norm1 == 0 or norm2 == 0:
        return 0
    return np.dot(vec1, vec2) / (norm1 * norm2)


def analyze_predictions(
    predictions_by_user: dict[str, list[list]],
    user_embedding_dict: dict[str, np.ndarray] | None = None,
    item_embedding_dict: dict[str, np.ndarray] | None = None,
    latencies: list[float] | None = None,
    success_count: int | None = None,
    failure_count: int | None = None,
) -> dict[str, Any]:
    """Analyze predictions and return metrics dictionary for reporting."""
    metrics = {}
    print("\n=== Analysis of Recommendations ===")

    metrics.update(_analyze_recommendation_counts(predictions_by_user))
    metrics.update(
        _analyze_response_codes(
            predictions_by_user,
            success_count=success_count,
            failure_count=failure_count,
        )
    )
    metrics.update(_analyze_search_types(predictions_by_user))
    metrics.update(_analyze_overlap(predictions_by_user))

    metrics.update(_analyze_latencies(latencies))

    # Analyze _distance and _user_item_dot_similarity attributes
    metrics.update(_analyze_prediction_attributes(predictions_by_user))

    # Check if recommended offers are different for different users (identical sets)
    # metrics.update(_analyze_user_recommendation_uniqueness(predictions_by_user))
    # New: Analyze overlap (Jaccard) between user recommendations
    metrics.update(_analyze_user_recommendation_overlap(predictions_by_user))

    if item_embedding_dict:
        metrics.update(
            _analyze_intralist_similarity(predictions_by_user, item_embedding_dict)
        )

    if user_embedding_dict and item_embedding_dict:
        metrics.update(
            _analyze_user_item_similarity(
                predictions_by_user, user_embedding_dict, item_embedding_dict
            )
        )

    return metrics


def _analyze_recommendation_counts(predictions_by_user):
    """Analyze the number of recommendations per call."""
    reco_counts = [
        len(preds)
        for user_preds in predictions_by_user.values()
        for preds in user_preds
    ]
    avg_recos = np.mean(reco_counts)
    print(f"Average number of recommendations per call: {avg_recos:.2f}")
    return {"avg_recommendations": avg_recos}


def _analyze_response_codes(
    predictions_by_user,
    success_count: int | None = None,
    failure_count: int | None = None,
):
    """Analyze response codes from predictions."""
    response_code_counts = {"success": 0, "error": 0}
    total_responses = 0
    for user_preds in predictions_by_user.values():
        for preds in user_preds:
            total_responses += 1
            if preds:
                pred_dict = convert_to_dict(preds[0])
                is_error = any(
                    "error" in str(value).lower() for value in pred_dict.values()
                )
                response_code_counts["error" if is_error else "success"] += 1
            else:
                response_code_counts["error"] += 1

    # If explicit counts are provided, use them instead
    if success_count is not None and failure_count is not None:
        response_code_counts["success"] = success_count
        response_code_counts["error"] = failure_count
        total_responses = success_count + failure_count

    success_rate = (
        response_code_counts["success"] / total_responses * 100
        if total_responses
        else 0
    )
    print("\nResponse code distribution:")
    for code, count in response_code_counts.items():
        percentage = (count / total_responses) * 100 if total_responses else 0
        print(f"- {code}: {count} ({percentage:.2f}%)")

    return {"success_rate": success_rate}


def _analyze_search_types(predictions_by_user):
    """Analyze search type distribution."""
    search_type_counts = defaultdict(int)
    total_predictions = 0

    for user_preds in predictions_by_user.values():
        for preds in user_preds:
            for pred in preds:
                pred_dict = convert_to_dict(pred)
                search_type = pred_dict.get("_search_type", "unknown")
                search_type_counts[search_type] += 1
                total_predictions += 1

    metrics = {"search_types": dict(search_type_counts)}

    print("\nSearch type distribution:")
    for search_type, count in sorted(search_type_counts.items()):
        percentage = (count / total_predictions) * 100
        metrics[f"search_type_{search_type}"] = percentage
        print(f"- {search_type}: {count} ({percentage:.2f}%)")

    return metrics


def _analyze_overlap(predictions_by_user):
    """Analyze overlap between predictions."""
    metrics = {}
    avg_user_overlap = 0
    overlap_count = 0

    for user_preds in predictions_by_user.values():
        if len(user_preds) >= 2:
            set1 = {str(convert_to_dict(item)) for item in user_preds[0]}
            set2 = {str(convert_to_dict(item)) for item in user_preds[1]}
            overlap = len(set1.intersection(set2))
            overlap_percentage = (overlap / len(set1)) * 100 if set1 else 0
            avg_user_overlap += overlap_percentage
            overlap_count += 1

    metrics["avg_user_overlap"] = (
        avg_user_overlap / overlap_count if overlap_count > 0 else 0
    )

    return metrics


def _analyze_latencies(latencies):
    """Analyze latency metrics."""
    metrics = {
        "avg_latency": np.mean(latencies),
        "min_latency": np.min(latencies),
        "max_latency": np.max(latencies),
        "p95_latency": np.percentile(latencies, 95),
    }

    print("\nLatency analysis:")
    print(f"Average latency: {metrics['avg_latency']:.3f} seconds")
    print(f"Min latency: {metrics['min_latency']:.3f} seconds")
    print(f"Max latency: {metrics['max_latency']:.3f} seconds")
    print(f"95th percentile latency: {metrics['p95_latency']:.3f} seconds")

    return metrics


def _analyze_intralist_similarity(predictions_by_user, item_embedding_dict):
    """Analyze similarity between items in the same recommendation list."""
    print("\nItem Intralist Similarity Analysis:")
    avg_intralist_similarities = []

    for user_preds in predictions_by_user.values():
        for preds in user_preds:
            pred_dicts = [convert_to_dict(pred) for pred in preds]
            item_ids = [
                pred.get("item_id")
                for pred in pred_dicts
                if pred.get("item_id") in item_embedding_dict
            ]

            if len(item_ids) >= 2:
                similarities = []
                for i in range(len(item_ids)):
                    for j in range(i + 1, len(item_ids)):
                        sim = compute_cosine_similarity(
                            item_embedding_dict[item_ids[i]],
                            item_embedding_dict[item_ids[j]],
                        )
                        similarities.append(sim)
                if similarities:
                    avg_intralist_similarities.append(np.mean(similarities))

    if avg_intralist_similarities:
        avg_intralist_sim = np.mean(avg_intralist_similarities)
        print(f"Average intralist item similarity: {avg_intralist_sim:.3f}")
        return {"avg_intralist_similarity": avg_intralist_sim}

    return {}


def _analyze_user_item_similarity(
    predictions_by_user,
    user_embedding_dict,
    item_embedding_dict,
):
    """Analyze similarity between user embeddings and recommended item embeddings."""
    print("\nUser-Item Similarity Analysis:")
    avg_user_item_similarities = []

    for user_id, user_preds in predictions_by_user.items():
        user_emb = user_embedding_dict.get(user_id)
        if user_emb is None:
            continue
        for preds in user_preds:
            pred_dicts = [convert_to_dict(pred) for pred in preds]
            item_embs = [
                item_embedding_dict.get(pred.get("item_id"))
                for pred in pred_dicts
                if pred.get("item_id") in item_embedding_dict
            ]
            if not item_embs:
                continue
            similarities = [
                compute_cosine_similarity(user_emb, item_emb) for item_emb in item_embs
            ]
            if similarities:
                avg_user_item_similarities.append(np.mean(similarities))

    if avg_user_item_similarities:
        avg_user_item_sim = np.mean(avg_user_item_similarities)
        print(f"Average user-item similarity: {avg_user_item_sim:.3f}")
        return {"avg_user_item_similarity": avg_user_item_sim}
    return {}


def _analyze_user_recommendation_overlap(predictions_by_user):
    """
    Analyze the overlap (Jaccard similarity) between recommended offers for all
    user pairs. Returns statistics on overlap distribution.
    """
    user_offer_sets = {}
    for user_id, user_preds in predictions_by_user.items():
        all_preds = []
        for preds in user_preds:
            if isinstance(preds, list):
                all_preds.extend(preds)
        item_ids = set()
        for pred in all_preds:
            pred_dict = convert_to_dict(pred)
            item_id = pred_dict.get("item_id")
            if item_id is not None:
                item_ids.add(item_id)
        user_offer_sets[user_id] = item_ids

    user_ids = list(user_offer_sets.keys())
    n_users = len(user_ids)
    overlaps = []
    for i in range(n_users):
        for j in range(i + 1, n_users):
            set1 = user_offer_sets[user_ids[i]]
            set2 = user_offer_sets[user_ids[j]]
            if set1 or set2:
                intersection = len(set1 & set2)
                union = len(set1 | set2)
                jaccard = intersection / union if union else 0
                overlaps.append(jaccard)
    if overlaps:
        avg_overlap = np.mean(overlaps)
        min_overlap = np.min(overlaps)
        max_overlap = np.max(overlaps)
        p95_overlap = np.percentile(overlaps, 95)
        print(
            f"\nUser-user recommendation overlap (Jaccard): avg={avg_overlap:.2f}, "
            f"min={min_overlap:.2f}, max={max_overlap:.2f}, p95={p95_overlap:.2f}"
        )
        high_overlap_count = sum(o > 0.8 for o in overlaps)
        if high_overlap_count > 0:
            print(
                f"Warning: {high_overlap_count} user pairs have >80% overlap in "
                f"recommendations."
            )
        return {
            "user_reco_avg_jaccard": avg_overlap,
            "user_reco_min_jaccard": min_overlap,
            "user_reco_max_jaccard": max_overlap,
            "user_reco_p95_jaccard": p95_overlap,
            "user_reco_high_overlap_pairs": high_overlap_count,
        }
    else:
        return {"user_reco_avg_jaccard": 0}


def _analyze_prediction_attributes(
    predictions_by_user,
):
    """
    Analyze the _distance and _user_item_dot_similarity attributes in predictions.
    Computes average, min, and max for each if present.
    """
    distance_values = []
    dot_sim_values = []
    for user_preds in predictions_by_user.values():
        for preds in user_preds:
            for pred in preds:
                pred_dict = convert_to_dict(pred)
                if "_distance" in pred_dict and pred_dict["_distance"] is not None:
                    distance_values.append(float(pred_dict["_distance"]))
                if (
                    "_user_item_dot_similarity" in pred_dict
                    and pred_dict["_user_item_dot_similarity"] is not None
                ):
                    dot_sim_values.append(float(pred_dict["_user_item_dot_similarity"]))
    metrics = {}
    if distance_values:
        metrics["distance_avg"] = float(np.mean(distance_values))
        metrics["distance_min"] = float(np.min(distance_values))
        metrics["distance_max"] = float(np.max(distance_values))
    if dot_sim_values:
        metrics["user_item_dot_similarity_avg"] = float(np.mean(dot_sim_values))
        metrics["user_item_dot_similarity_min"] = float(np.min(dot_sim_values))
        metrics["user_item_dot_similarity_max"] = float(np.max(dot_sim_values))
    return metrics
