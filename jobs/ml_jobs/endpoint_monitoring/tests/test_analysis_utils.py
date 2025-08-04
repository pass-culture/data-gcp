import numpy as np

from utils.analysis_utils import _analyze_overlap, _analyze_user_recommendation_overlap


def test_analyze_overlap_basic():
    # User with two calls, identical recommendations
    predictions_by_user = {
        "user1": [
            [{"item_id": "A"}, {"item_id": "B"}],
            [{"item_id": "A"}, {"item_id": "B"}],
        ]
    }
    metrics = _analyze_overlap(predictions_by_user)
    assert metrics["avg_user_overlap"] == 100.0


def test_analyze_overlap_partial():
    # User with two calls, partial overlap
    predictions_by_user = {
        "user1": [
            [{"item_id": "A"}, {"item_id": "B"}],
            [{"item_id": "B"}, {"item_id": "C"}],
        ]
    }
    metrics = _analyze_overlap(predictions_by_user)
    assert np.isclose(metrics["avg_user_overlap"], 50.0)


def test_analyze_overlap_none():
    # User with two calls, no overlap
    predictions_by_user = {
        "user1": [
            [{"item_id": "A"}],
            [{"item_id": "B"}],
        ]
    }
    metrics = _analyze_overlap(predictions_by_user)
    assert metrics["avg_user_overlap"] == 0.0


def test_analyze_overlap_multiple_users():
    # Two users, only one has two calls
    predictions_by_user = {
        "user1": [
            [{"item_id": "A"}, {"item_id": "B"}],
            [{"item_id": "B"}, {"item_id": "C"}],
        ],
        "user2": [
            [{"item_id": "A"}],
        ],
    }
    metrics = _analyze_overlap(predictions_by_user)
    # Only user1 is counted, overlap is 50%
    assert np.isclose(metrics["avg_user_overlap"], 50.0)


def test_analyze_user_recommendation_overlap_basic():
    # Two users with identical recommendations
    predictions_by_user = {
        "user1": [[{"item_id": "A"}, {"item_id": "B"}, {"item_id": "C"}]],
        "user2": [[{"item_id": "A"}, {"item_id": "B"}, {"item_id": "C"}]],
    }
    metrics = _analyze_user_recommendation_overlap(predictions_by_user)
    assert metrics["user_reco_avg_jaccard"] == 1.0
    assert metrics["user_reco_high_overlap_pairs"] == 1


def test_analyze_user_recommendation_overlap_partial():
    # Two users with partial overlap
    predictions_by_user = {
        "user1": [[{"item_id": "A"}, {"item_id": "B"}]],
        "user2": [[{"item_id": "B"}, {"item_id": "C"}]],
    }
    metrics = _analyze_user_recommendation_overlap(predictions_by_user)
    assert np.isclose(metrics["user_reco_avg_jaccard"], 1 / 3)
    assert metrics["user_reco_high_overlap_pairs"] == 0


def test_analyze_user_recommendation_overlap_none():
    # Two users with no overlap
    predictions_by_user = {
        "user1": [[{"item_id": "A"}]],
        "user2": [[{"item_id": "B"}]],
    }
    metrics = _analyze_user_recommendation_overlap(predictions_by_user)
    assert metrics["user_reco_avg_jaccard"] == 0.0
    assert metrics["user_reco_high_overlap_pairs"] == 0


def test_analyze_user_recommendation_overlap_multiple_users():
    # Three users, mixed overlap
    predictions_by_user = {
        "user1": [[{"item_id": "A"}, {"item_id": "B"}]],
        "user2": [[{"item_id": "B"}, {"item_id": "C"}]],
        "user3": [[{"item_id": "A"}, {"item_id": "C"}]],
    }
    metrics = _analyze_user_recommendation_overlap(predictions_by_user)
    # There are 3 pairs: (user1,user2), (user1,user3), (user2,user3)
    # Each pair has 1/3 overlap
    assert np.isclose(metrics["user_reco_avg_jaccard"], 1 / 3)
    assert metrics["user_reco_high_overlap_pairs"] == 0
