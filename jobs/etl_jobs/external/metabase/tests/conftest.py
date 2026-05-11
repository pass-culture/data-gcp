from __future__ import annotations

import json
from pathlib import Path
from typing import Any

TESTS_ROOT_PATH = Path(__file__).parent
TESTS_DATA_PATH = TESTS_ROOT_PATH / "data"

with (TESTS_DATA_PATH / "card_native_mbql5_api_response_data.json").open() as f:
    NATIVE_CARD_API_RESPONSE_DATA: dict[str, Any] = json.load(f)

with (TESTS_DATA_PATH / "card_query_mbql5_api_response_data.json").open() as f:
    QUERY_CARD_API_RESPONSE_DATA: dict[str, Any] = json.load(f)
