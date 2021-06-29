import os
import collections
from typing import List, Dict, Any, Tuple

import pandas as pd
from sqlalchemy import create_engine, engine

RECOMMENDATION_CLOUDSQL_PASSWORD_DEV = os.environ.get(
    "RECOMMENDATION_CLOUDSQL_PASSWORD_DEV"
)

SQL_CONNECTION_NAME = "passculture-data-ehp:europe-west1:cloudsql-recommendation-dev"
query_string = dict(
    {"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(SQL_CONNECTION_NAME)}
)
engine = create_engine(
    f"postgresql://cloudsql-recommendation-dev:{RECOMMENDATION_CLOUDSQL_PASSWORD_DEV}@localhost:5432/cloudsql-recommendation-dev"
)


def create_db_connection():
    return engine.connect().execution_options(autocommit=True)


#    AND (EXISTS (SELECT * FROM offer_has_at_least_one_bookable_stock(offer.offer_id)))

offer = pd.DataFrame(
    {
        "offer_id_at_providers": [None] * 20,
        "offer_modified_at_last_provider_date": ["2021-02-03 09:26:06.560193"] * 20,
        "offer_id": list(range(20)),
        "offer_creation_date": ["2021-02-03 09:25:48.443348"] * 20,
        "offer_product_id": list(range(20)),
        "venue_id": list(range(20)),
        "offer_last_provider_id": [None] * 20,
        "booking_email": ["test@email.com"] * 20,
        "offer_is_active": [True] * 20,
        "offer_type": ["EventType.JEUX"] * 20,
        "offer_name": ["test"] * 20,
        "offer_description": ["test"] * 20,
        "offer_conditions": [None] * 20,
        "offer_age_min": [None] * 20,
        "offer_age_max": [None] * 20,
        "offer_url": ["test_url"] * 20,
        "offer_duration_minutes": [60] * 20,
        "offer_is_national": [False] * 20,
        "offer_extra_data": [None] * 20,
        "offer_is_duo": [False] * 20,
        "offer_withdrawal_details": [None] * 20,
        "offer_validation": ["APPROVED"] * 20,
    }
)

booking = pd.DataFrame(
    {
        "booking_id": list(range(20)),
        "booking_creation_date": ["2021-02-03 09:25:48.770646"] * 20,
        "stock_id": list(range(20)),
        "booking_quantity": [1] * 20,
        "user_id": [1] * 20,
        "booking_amount": [38.00] * 20,
        "booking_is_cancelled": [False] * 20,
        "booking_is_used": [None] * 20,
        "booking_used_date": [None] * 20,
        "booking_cancellation_date": [None] * 20,
    }
)

venue = pd.DataFrame(
    {
        "venue_thumb_count": [10] * 20,
        "venue_id_at_providers": [None] * 20,
        "venue_modified_at_last_provider": ["2021-02-03 17:26:05.933436"] * 20,
        "venue_address": [None] * 20,
        "venue_postal_code": [None] * 20,
        "venue_city": [None] * 20,
        "venue_id": list(range(20)),
        "venue_name": ["Offrenumérique"] * 20,
        "venue_siret": [None] * 20,
        "venue_department_code": [None] * 20,
        "venue_latitude": [None] * 20,
        "venue_longitude": [None] * 20,
        "venue_managing_offerer_id": list(range(20)),
        "venue_booking_email": ["test@email"] * 20,
        "venue_last_provider_id": [None] * 20,
        "venue_is_virtual": [True] * 20,
        "venue_comment": [None] * 20,
        "venue_public_name": [None] * 20,
        "venue_type_id": [727] * 20,
        "venue_label_id": [None] * 20,
        "venue_creation_date": ["2021-02-03 17:26:05.933453"] * 20,
        "venue_validation_token": [None] * 20,
    }
)

offerer = pd.DataFrame(
    {
        "offerer_is_active": [True] * 20,
        "offerer_thumb_count": [10] * 20,
        "offerer_id_at_providers": list(range(20)),
        "offerer_modified_at_last_provider_date": ["2021-02-04 15:08:00.131491"] * 20,
        "offerer_address": ["BOULEVARDDESJARDINIERS"] * 20,
        "offerer_postal_code": ["06200"] * 20,
        "offerer_city": ["NICE"] * 20,
        "offerer_id": list(range(20)),
        "offerer_creation_date": ["2021-02-04 15:08:00.131514"] * 20,
        "offerer_name": ["MUSEENATIONALDUSPORT"] * 20,
        "offerer_siren": [130002645] * 20,
        "offerer_last_provider_id": [None] * 20,
        "offerer_validation_token": [None] * 20,
    }
)

mediation = pd.DataFrame(
    {
        "thumbCount": [10] * 20,
        "idAtProviders": [None] * 20,
        "dateModifiedAtLastProvider": ["2021-02-05 15:17:06.463861"] * 20,
        "id": [4876] * 20,
        "dateCreated": ["2021-02-05 15:17:06.463883"] * 20,
        "authorId": [3814] * 20,
        "lastProviderId": [3814] * 20,
        "offerId": list(range(20)),
        "credit": ["undefined"] * 20,
        "isActive": [True] * 20,
    }
)

stock = pd.DataFrame(
    {
        "stock_id_at_providers": [None] * 20,
        "stock_modified_at_last_provider_date": ["2021-02-04 09:10:35.053872"] * 20,
        "stock_id": [141098] * 20,
        "stock_modified_date": ["2021-02-04 09:10:35.053904"] * 20,
        "stock_price": [0] * 20,
        "stock_quantity": [None] * 20,
        "stock_booking_limit_date": [None] * 20,
        "stock_last_provider_id": list(range(20)),
        "offer_id": list(range(20)),
        "stock_is_soft_deleted": [False] * 20,
        "stock_beginning_date": [None] * 20,
        "stock_creation_date": ["2021-02-04 09:10:35.053892"] * 20,
    }
)
