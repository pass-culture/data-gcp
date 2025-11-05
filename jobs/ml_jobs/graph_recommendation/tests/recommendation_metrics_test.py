"""Tests for recommendation metrics utilities."""

from __future__ import annotations

import pandas as pd
import pytest

from src.utils.recommendation_metrics import custom_recall_at_k


def test_custom_recall_at_k_perfect_match():
    """Test recall@k when predictions perfectly match ground truth."""
    rating_true = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1"],
            "itemID": ["item1", "item2", "item3"],
            "rating": [5.0, 4.0, 3.0],
        }
    )
    rating_pred = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1"],
            "itemID": ["item1", "item2", "item3"],
            "prediction": [0.9, 0.8, 0.7],
        }
    )
    recall = custom_recall_at_k(rating_true, rating_pred, k=2)
    # Top-2 predicted: item1, item2
    # Top-2 true: item1, item2
    # Overlap: 2 items
    # Recall@2 = 2/2 = 1.0
    assert recall == 1.0


def test_custom_recall_at_k_no_overlap():
    """Test recall@k when predictions have no overlap with ground truth."""
    rating_true = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1"],
            "itemID": ["item1", "item2", "item3"],
            "rating": [5.0, 4.0, 3.0],
        }
    )
    rating_pred = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1"],
            "itemID": ["item3", "item4", "item5"],
            "prediction": [0.9, 0.8, 0.7],
        }
    )
    recall = custom_recall_at_k(rating_true, rating_pred, k=2)
    # Top-2 predicted: item3, item4
    # Top-2 true: item1, item2
    # Overlap: 0 items
    # Recall@2 = 0/2 = 0.0
    assert recall == 0.0


def test_custom_recall_at_k_partial_overlap():
    """Test recall@k with partial overlap between predictions and ground truth."""
    rating_true = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1", "user1"],
            "itemID": ["item1", "item2", "item3", "item4"],
            "rating": [5.0, 4.0, 3.0, 2.0],
        }
    )
    rating_pred = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1", "user1"],
            "itemID": ["item1", "item3", "item5", "item6"],
            "prediction": [0.9, 0.8, 0.7, 0.6],
        }
    )
    recall = custom_recall_at_k(rating_true, rating_pred, k=3)
    # Top-3 predicted: item1, item3, item5
    # Top-3 true: item1, item2, item3
    # Overlap: 2 items (item1, item3)
    # Recall@3 = 2/3 ≈ 0.667
    assert pytest.approx(recall, 1e-3) == 2 / 3


def test_custom_recall_at_k_with_tied_ratings():
    """Test recall@k properly handles tied ratings in ground truth.

    When ground truth has ties, all items with rating >= k-th rating
    should be included in the relevant set.
    """
    rating_true = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1", "user1", "user1"],
            "itemID": ["item1", "item2", "item3", "item4", "item5"],
            "rating": [5.0, 4.0, 4.0, 4.0, 2.0],  # 3 items tied at 4.0
        }
    )
    rating_pred = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1"],
            "itemID": ["item1", "item2", "item3"],
            "prediction": [0.9, 0.8, 0.7],
        }
    )
    recall = custom_recall_at_k(rating_true, rating_pred, k=2)
    # k=2, so we get 2nd highest rating = 4.0
    # Top-k true (with ties): all items with rating >= 4.0
    #   = item1, item2, item3, item4 (4 items)
    # Top-2 predicted: item1, item2
    # Overlap: 2 items (item1, item2)
    # Recall@2 = 2/2 = 1.0
    assert recall == 1.0


def test_custom_recall_at_k_with_tied_ratings_partial_overlap():
    """Test recall@k with tied ratings and partial overlap."""
    rating_true = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1", "user1", "user1"],
            "itemID": ["item1", "item2", "item3", "item4", "item5"],
            "rating": [5.0, 4.0, 4.0, 4.0, 2.0],  # 3 items tied at 4.0
        }
    )
    rating_pred = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1"],
            "itemID": ["item1", "item5", "item6"],
            "prediction": [0.9, 0.8, 0.7],
        }
    )
    recall = custom_recall_at_k(rating_true, rating_pred, k=2)
    # k=2, so we get 2nd highest rating = 4.0
    # Top-k true (with ties): all items with rating >= 4.0 = item1, item2, item3, item4
    # Top-2 predicted: item1, item5
    # Overlap: 1 item (item1)
    # Recall@2 = 1/2 = 0.5
    assert recall == 0.5


def test_custom_recall_at_k_multiple_users():
    """Test recall@k is averaged across multiple users."""
    rating_true = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user2", "user2", "user3", "user3"],
            "itemID": ["item1", "item2", "item3", "item4", "item5", "item6"],
            "rating": [5.0, 4.0, 5.0, 4.0, 5.0, 4.0],
        }
    )
    rating_pred = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user2", "user2", "user3", "user3"],
            "itemID": ["item1", "item2", "item3", "item5", "item5", "item7"],
            "prediction": [0.9, 0.8, 0.9, 0.7, 0.9, 0.8],
        }
    )
    recall = custom_recall_at_k(rating_true, rating_pred, k=1)
    # User1: Top-1 pred=item1, Top-1 true=item1 → overlap=1, recall=1/1=1.0
    # User2: Top-1 pred=item3, Top-1 true=item3 → overlap=1, recall=1/1=1.0
    # User3: Top-1 pred=item5, Top-1 true=item5 → overlap=1, recall=1/1=1.0
    # Average recall = (1.0 + 1.0 + 1.0) / 3 = 1.0
    assert recall == 1.0


def test_custom_recall_at_k_multiple_users_varying_overlap():
    """Test recall@k with different overlap rates per user."""
    rating_true = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1", "user2", "user2", "user2"],
            "itemID": ["item1", "item2", "item3", "item4", "item5", "item6"],
            "rating": [5.0, 4.0, 3.0, 5.0, 4.0, 3.0],
        }
    )
    rating_pred = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1", "user2", "user2", "user2"],
            "itemID": ["item1", "item2", "item7", "item4", "item8", "item9"],
            "prediction": [0.9, 0.8, 0.7, 0.9, 0.8, 0.7],
        }
    )
    recall = custom_recall_at_k(rating_true, rating_pred, k=2)
    # User1: Top-2 pred=[item1, item2], Top-2 true=[item1, item2]
    #        → overlap=2, recall=2/2=1.0
    # User2: Top-2 pred=[item4, item8], Top-2 true=[item4, item5]
    #        → overlap=1, recall=1/2=0.5
    # Average recall = (1.0 + 0.5) / 2 = 0.75
    assert recall == 0.75


def test_custom_recall_at_k_missing_users_filled_with_zero():
    """Test that users without predictions are handled correctly."""
    rating_true = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user2", "user2"],
            "itemID": ["item1", "item2", "item3", "item4"],
            "rating": [5.0, 4.0, 5.0, 4.0],
        }
    )
    rating_pred = pd.DataFrame(
        {
            "userID": ["user1", "user1"],
            "itemID": ["item1", "item2"],
            "prediction": [0.9, 0.8],
        }
    )
    recall = custom_recall_at_k(rating_true, rating_pred, k=2)
    # User1: Top-2 pred=[item1, item2], Top-2 true=[item1, item2]
    #        → overlap=2, recall=2/2=1.0
    # User2: No predictions → overlap=0, recall=0/2=0.0
    # Average recall = (1.0 + 0.0) / 2 = 0.5
    assert recall == 0.5


def test_custom_recall_at_k_k_equals_1():
    """Test recall@1 (single item recommendation)."""
    rating_true = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1"],
            "itemID": ["item1", "item2", "item3"],
            "rating": [5.0, 4.0, 3.0],
        }
    )
    rating_pred = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1"],
            "itemID": ["item2", "item1", "item3"],
            "prediction": [0.9, 0.8, 0.7],
        }
    )
    recall = custom_recall_at_k(rating_true, rating_pred, k=1)
    # Top-1 predicted: item2
    # Top-1 true: item1
    # Overlap: 0 items
    # Recall@1 = 0/1 = 0.0
    assert recall == 0.0


def test_custom_recall_at_k_all_tied_ratings():
    """Test recall@k when all ground truth ratings are the same."""
    rating_true = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1", "user1"],
            "itemID": ["item1", "item2", "item3", "item4"],
            "rating": [4.0, 4.0, 4.0, 4.0],  # All tied
        }
    )
    rating_pred = pd.DataFrame(
        {
            "userID": ["user1", "user1"],
            "itemID": ["item1", "item2"],
            "prediction": [0.9, 0.8],
        }
    )
    recall = custom_recall_at_k(rating_true, rating_pred, k=2)
    # k=2, so we get 2nd highest rating = 4.0
    # Top-k true (with ties): all items with rating >= 4.0 = all 4 items
    # Top-2 predicted: item1, item2
    # Overlap: 2 items
    # Recall@2 = 2/2 = 1.0
    assert recall == 1.0


def test_custom_recall_at_k_custom_column_names():
    """Test that custom column names work correctly."""
    rating_true = pd.DataFrame(
        {
            "query_id": ["q1", "q1", "q1"],
            "doc_id": ["doc1", "doc2", "doc3"],
            "relevance": [5.0, 4.0, 3.0],
        }
    )
    rating_pred = pd.DataFrame(
        {
            "query_id": ["q1", "q1", "q1"],
            "doc_id": ["doc1", "doc2", "doc4"],
            "score": [0.9, 0.8, 0.7],
        }
    )
    recall = custom_recall_at_k(
        rating_true,
        rating_pred,
        k=2,
        col_user="query_id",
        col_item="doc_id",
        col_prediction="score",
        col_rating="relevance",
    )
    # Top-2 predicted: doc1, doc2
    # Top-2 true: doc1, doc2
    # Overlap: 2 items
    # Recall@2 = 2/2 = 1.0
    assert recall == 1.0


def test_custom_recall_at_k_large_k():
    """Test recall@k when k is larger than number of items.

    When k > number of items, all items should be considered in the relevant set.
    The recall is still calculated as overlap/k.
    """
    rating_true = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1"],
            "itemID": ["item1", "item2", "item3"],
            "rating": [5.0, 4.0, 3.0],
        }
    )
    rating_pred = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1"],
            "itemID": ["item1", "item2", "item3"],
            "prediction": [0.9, 0.8, 0.7],
        }
    )
    recall = custom_recall_at_k(rating_true, rating_pred, k=5)
    # Top-5 predicted: item1, item2, item3 (only 3 available)
    # Top-5 true: all items (since k > number of items)
    # Overlap: 3 items
    # Recall@5 = 3/5 = 0.6
    assert recall == 0.6


def test_custom_recall_at_k_descending_predictions():
    """Test that predictions are correctly sorted in descending order."""
    rating_true = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1", "user1"],
            "itemID": ["item1", "item2", "item3", "item4"],
            "rating": [5.0, 4.0, 3.0, 2.0],
        }
    )
    # Predictions in ascending order (should be re-sorted)
    rating_pred = pd.DataFrame(
        {
            "userID": ["user1", "user1", "user1", "user1"],
            "itemID": ["item4", "item3", "item2", "item1"],
            "prediction": [0.6, 0.7, 0.8, 0.9],
        }
    )
    recall = custom_recall_at_k(rating_true, rating_pred, k=2)
    # After sorting predictions descending:
    #   item1 (0.9), item2 (0.8), item3 (0.7), item4 (0.6)
    # Top-2 predicted: item1, item2
    # Top-2 true: item1, item2
    # Overlap: 2 items
    # Recall@2 = 2/2 = 1.0
    assert recall == 1.0
