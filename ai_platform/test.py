import numpy as np
from sklearn.externals import joblib


def test_model():
    # Given
    offer_ids = np.array([1, 2, 3, 4])

    # When
    model = joblib.load('ai_platform/model.joblib')
    predicted_score = model.predict(offer_ids)

    # Then
    assert len(predicted_score) == 4
    assert sorted(predicted_score) == sorted([1, 1, 1, 1])

