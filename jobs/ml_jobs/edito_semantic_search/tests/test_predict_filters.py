import pytest
import requests
from loguru import logger

BASE_URL = "http://0.0.0.0:8085"
PREDICT_ENDPOINT = f"{BASE_URL}/predict"


# Helper to send POST to /predict
def post_predict(instances):
    payload = {"instances": instances}
    logger.info(f"Sending POST to {PREDICT_ENDPOINT} with payload: {payload}")
    response = requests.post(PREDICT_ENDPOINT, json=payload)
    logger.info(
        f"Received response: status={response.status_code}, body={response.text}"
    )
    return response


# Example filter scenarios
test_cases = [
    # 2. Filter by offer_category_id (multiple values)
    {
        "name": "filter_by_offer_category_id_in",
        "filters": [
            {
                "column": "offer_category_id",
                "operator": "in",
                "value": ["LIVRE", "MUSEE"],
            }
        ],
    },
    # 3. Filter by last_stock_price (numeric range)
    {
        "name": "filter_by_last_stock_price_range",
        "filters": [{"column": "last_stock_price", "operator": "<=", "value": 10.0}],
    },
    # 5. Filter by stock_beginning_date (datetime between)
    {
        "name": "filter_by_offer_creation_date_between",
        "filters": [
            {
                "column": "offer_creation_date",
                "operator": "between",
                "value": ["2023-01-01", "2026-01-01"],
            }
        ],
    },
    # 5. Filter by stock_beginning_date (datetime between)
    {
        "name": "filter_by_stock_beginning_date_between",
        "filters": [
            {
                "column": "stock_beginning_date",
                "operator": "between",
                "value": ["2023-01-01", "2026-01-01"],
            }
        ],
    },
    # 6. Multiple filters combined
    {
        "name": "filter_by_multiple",
        "filters": [
            {"column": "venue_department_code", "operator": "=", "value": "75"},
            {"column": "last_stock_price", "operator": "<", "value": 50.0},
        ],
    },
    # 7. Wrong operator should return 500
    {
        "name": "filter_with_wrong_operator",
        "filters": [
            {"column": "offer_category_id", "operator": "WRONG_OP", "value": "LIVRE"}
        ],
        "expect_status": 500,
    },
    # 8. Wrong operator should return 500
    {
        "name": "filter_with_wrong_operator",
        "filters": [
            {"column": "offer_category_id", "operator": "between", "value": "LIVRE"}
        ],
        "expect_status": 500,
    },
    # 9. Wrong operator should return 500
    {
        "name": "filter_with_wrong_in_value_type",
        "filters": [
            {"column": "offer_category_id", "operator": "in", "value": "LIVRE"}
        ],
        "expect_status": 500,
    },
]


@pytest.mark.parametrize("case", test_cases, ids=[c["name"] for c in test_cases])
def test_predict_filters(case):
    logger.info(f"Running test case: {case['name']}")
    # Minimal valid PredictionRequest
    instance = {
        "search_query": "test query",
        "filters_list": case["filters"],
    }
    logger.info(f"Test instance: {instance}")
    response = post_predict([instance])
    expect_status = case.get("expect_status", 200)
    assert response.status_code == expect_status, f"Failed: {response.text}"
    if expect_status == 200:
        data = response.json()
        logger.info(f"Response JSON: {data}")
        assert "predictions" in data, "Missing predictions key"
        offers = data["predictions"]["offers"]
        logger.info(f"Offers returned: {offers}")
        assert isinstance(offers, list)
        # Check that offers are not empty
        assert len(offers) > 0, f"No offers returned for case: {case['name']}"
        assert all("offer_id" in o and "pertinence" in o for o in offers)
