import json
import pytest
from unittest.mock import Mock, patch, MagicMock, call

from typing import Any, List



@patch("pcpapillon.core.extract_embedding._encode_img_from_url")
@patch("pcpapillon.core.extract_embedding._encode_from_feature")
def test_compliance_score(
    encode_from_feature_mock: Mock,
    encode_from_url_mock: Mock,
    offer_to_score: json):
    encode_from_feature_mock.return_value=np.array([0] * 512)
    encode_from_url_mock.return_value=np.array([0] * 512)
    