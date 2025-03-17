import pandas as pd
import pytest

from two_towers_model.utils.evaluate import compute_metrics


@pytest.fixture
def training_data():
    return pd.DataFrame(
        {"user_id": ["user1", "user1", "user2"], "item_id": ["item1", "item2", "item1"]}
    )


@pytest.fixture
def test_data():
    return pd.DataFrame(
        {
            "user_id": ["user1", "user1", "user2", "user2"],
            "item_id": ["item3", "item4", "item2", "item3"],
        }
    )


@pytest.fixture
def df_predictions():
    return pd.DataFrame(
        {
            "user_id": ["user1", "user1", "user2", "user2"],
            "item_id": ["item1", "item3", "item2", "item3"],
            "score": [0.9, 0.8, 0.6, 0.9],  # Higher scores for relevant items
        }
    )


@pytest.mark.parametrize(
    "k,expected_precision,expected_recall",
    [
        (2, 0.75, 0.75),
        # For k=2:
        # user1: relevant items are item3, item4
        #   - predicted top 2: item1, item3 (one correct) -> precision=0.5, recall=0.5
        # user2: relevant items are item2, item3
        #   - predicted top 2: item1, item2 (two correct) -> precision=1, recall=1
        # Overall: precision=0.75, recall=0.75
        (1, 0.5, 0.25),  # For k=1:
        # user1: relevant items are item3, item4
        #   - predicted top 1: item1 (none correct) -> precision=0.0, recall=0.0
        # user2: relevant items are item2, item3
        #   - predicted top 1: item2 (correct) -> precision=1.0, recall=0.5
        # Overall: precision=0.5, recall=0.25
    ],
)
def test_compute_metrics_basic(
    test_data, training_data, df_predictions, k, expected_precision, expected_recall
):
    metrics = compute_metrics(
        test_data=test_data,
        df_predictions=df_predictions,
        training_data=training_data,
        k=k,
    )

    assert metrics[f"precision_at_{k}"] == pytest.approx(expected_precision)
    assert metrics[f"recall_at_{k}"] == pytest.approx(expected_recall)
