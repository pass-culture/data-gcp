import os

################################  To use Keras 2 instead of 3  ################################
# See [TensorFlow + Keras 2 backwards compatibility section](https://keras.io/getting_started/)
os.environ["TF_USE_LEGACY_KERAS"] = "1"
###############################################################################################

import pandas as pd
import pytest

from two_towers_model.utils.evaluate import compute_metrics


@pytest.fixture
def training_data():
    return pd.DataFrame(
        {
            "user_id": ["user1", "user1", "user2", "user3"],
            "item_id": ["item7", "item6", "item1", "item5"],
        }
    )


@pytest.fixture
def test_data():
    return pd.DataFrame(
        {
            "user_id": ["user1", "user1", "user1", "user2", "user2", "user3"],
            "item_id": ["item3", "item4", "item5", "item3", "item4", "item4"],
        }
    )


@pytest.fixture
def df_predictions():
    return pd.DataFrame(
        {
            "user_id": [
                "user1",
                "user1",
                "user1",
                "user1",
                "user2",
                "user2",
                "user2",
                "user2",
                "user3",
                "user3",
                "user3",
                "user3",
            ],
            "item_id": [
                "item1",
                "item3",
                "item2",
                "item5",
                "item2",
                "item3",
                "item4",
                "item5",
                "item1",
                "item2",
                "item3",
                "item4",
            ],
            "score": [
                0.9,
                0.8,
                0.6,
                0.5,
                0.9,
                0.8,
                0.6,
                0.5,
                0.9,
                0.8,
                0.6,
                0.5,
            ],  # Higher scores for relevant items
        }
    )


@pytest.mark.parametrize(
    "k,expected_precision,expected_recall",
    [
        (3, 1 / 3, 4 / 9),
        # For k=3:
        # user1: relevant items are "item3", "item4", "item5"
        #   - predicted top 3: "item1", "item3", "item2", (one correct) -> precision=1/3, recall=1/3
        # user2: relevant items are "item3", "item4"
        #   - predicted top 3: "item2", "item3", "item4",(two correct) -> precision=2/3, recall=1
        # user3: relevant items are "item4"
        #   - predicted top 3:  "item1", "item2", "item3" (none correct) -> precision=0, recall=0
        # Overall: precision=1/3, recall=4/9
        (2, 1 / 3, 5 / 18),
        # For k=2:
        # user1: relevant items are "item3", "item4", "item5",
        #   - predicted top 2: item1, item3 (one correct) -> precision=0.5, recall=1/3
        # user2: relevant items are "item3", "item4"
        #   - predicted top 2: "item2", "item3" (one correct) -> precision=0.5, recall=0.5
        # user3: relevant items are "item4"
        #   - predicted top 2: "item1", "item2" (none correct) -> precision=0, recall=0
        # Overall: precision=1/3, recall=5/18
        (1, 0, 0),
        # For k=1:
        # user1: relevant items are "item3", "item4", "item5",
        #   - predicted top 1: item1 (none correct) -> precision=0, recall=0
        # user2: relevant items are "item3", "item4"
        #   - predicted top 1: "item2" (none correct) -> precision=0, recall=0
        # user3: relevant items are "item4"
        #   - predicted top 1: "item1"(none correct) -> precision=0, recall=0
        # Overall: precision=0, recall=0
    ],
)
def test_compute_metrics_basic(
    test_data, training_data, df_predictions, k, expected_precision, expected_recall
):
    metrics = compute_metrics(
        test_data=test_data,
        df_predictions=df_predictions,
        train_data=training_data,
        k=k,
    )

    assert metrics[f"precision_at_{k}"] == pytest.approx(expected_precision)
    assert metrics[f"recall_at_{k}"] == pytest.approx(expected_recall)
