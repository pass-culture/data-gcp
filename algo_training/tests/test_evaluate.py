import numpy as np
import pandas as pd
import pytest

from utils.metrics import get_actual_and_predicted


class MockModel:
    @staticmethod
    def predict(input_list: list, verbose: int):
        return np.arange((np.array(input_list).shape[1]))


@pytest.fixture
def data_model_dict():
    return {
        "name": "model_name",
        "data": {
            "raw": None,
            "train": pd.DataFrame(
                {
                    "user_id": [1, 1, 2, 2, 3],
                    "item_id": ["k", "l", "i", "k", "i"],
                    "offer_subcategoryid": ["b", "b", "a", "b", "a"],
                    "count": [1, 1, 1, 1, 1],
                }
            ),
            "test": pd.DataFrame(
                {
                    "user_id": [1, 1, 1, 2, 3],
                    "item_id": ["i", "j", "k", "j", "j"],
                    "offer_subcategoryid": ["a", "a", "b", "a", "a"],
                    "count": [1, 1, 1, 1, 1],
                }
            ),
        },
        "model": MockModel,
        "prediction_input_feature": "user_id",
    }


@pytest.fixture
def expected_output_data_model_dict():
    return {
        "name": "model_name",
        "data": {
            "raw": None,
            "train": pd.DataFrame(
                {
                    "user_id": [1, 1, 2, 2, 3],
                    "item_id": ["k", "l", "i", "k", "i"],
                    "offer_subcategoryid": ["b", "b", "a", "b", "a"],
                    "count": [1, 1, 1, 1, 1],
                }
            ),
            "test": pd.DataFrame(
                {
                    "user_id": [1, 1, 1, 2, 3],
                    "item_id": ["i", "j", "k", "j", "j"],
                    "offer_subcategoryid": ["a", "a", "b", "a", "a"],
                    "count": [1, 1, 1, 1, 1],
                }
            ),
        },
        "model": MockModel,
        "top_offers": pd.DataFrame(
            {
                "user_id": [1, 2, 3],
                "actual": [["i", "j", "k"], ["j"], ["j"]],
                "model_predicted": [["k", "j", "i"], ["k", "j", "i"], ["k", "j", "i"]],
                "predictions_diversified": [
                    ["j", "k", "i"],
                    ["j", "k", "i"],
                    ["j", "k", "i"],
                ],
            }
        ),
    }


class TestEvaluate:
    @staticmethod
    def test_get_actual_and_predicted(data_model_dict, expected_output_data_model_dict):
        output_data_model_dict = get_actual_and_predicted(
            data_model_dict, shuffle_recommendation=False
        )
        pd.testing.assert_frame_equal(
            output_data_model_dict["top_offers"],
            expected_output_data_model_dict["top_offers"],
        )
