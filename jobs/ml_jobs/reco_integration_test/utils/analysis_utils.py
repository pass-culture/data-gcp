"""Utility functions for analyzing predictions and computing metrics."""

import json
import numpy as np
from collections import defaultdict
from typing import Dict, List, Any

from .similarity_utils import compute_cosine_similarity


def convert_to_dict(obj: Any) -> Any:
    """Convert MapComposite and RepeatedComposite objects to dictionaries."""
    if hasattr(obj, "items"):  # For MapComposite
        return {k: convert_to_dict(v) for k, v in obj.items()}
    elif hasattr(obj, "__iter__") and not isinstance(
        obj, (str, bytes)
    ):  # For RepeatedComposite and other iterables
        return [convert_to_dict(item) for item in obj]
    else:
        return obj


def analyze_predictions(
    predictions_by_user: Dict[str, List[List]],
    latencies: List[float],
    user_embedding_dict: Dict[str, np.ndarray] = None,
    item_embedding_dict: Dict[str, np.ndarray] = None,
) -> Dict[str, Any]:
    """Analyze predictions and return metrics dictionary for reporting."""
    metrics = {}
    print("\n=== Analysis of Recommendations ===")

    metrics.update(_analyze_recommendation_counts(predictions_by_user))
    metrics.update(_analyze_response_codes(predictions_by_user))
    metrics.update(_analyze_search_types(predictions_by_user))
    metrics.update(_analyze_overlap(predictions_by_user))
    metrics.update(_analyze_latencies(latencies))

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


def _analyze_response_codes(predictions_by_user):
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

    success_rate = (
        response_code_counts["success"] / total_responses * 100
        if total_responses
        else 0
    )
    print("\nResponse code distribution:")
    for code, count in response_code_counts.items():
        percentage = (count / total_responses) * 100
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
