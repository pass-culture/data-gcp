from refacto_api.tools.ab_testing import query_ab_testing_table, ab_testing_assign_user
from refacto_api.tools.geolocalisation import get_iris_from_coordinates
from refacto_api.tools.db_connection import create_db_connection
from sqlalchemy import text
from utils import log_duration
import time

RECOMMENDABLE_OFFER_TABLE_PREFIX = "recommendable_offers"
RECOMMENDABLE_OFFER_TABLE_SUFFIX_DICT = {
    "15": "eac_15",
    "16": "eac_16_17",
    "17": "eac_16_17",
}


class User:
    def __init__(self, user_id, longitude=None, latitude=None):
        self.id = user_id
        self.longitude = False if longitude is None else longitude
        self.latitude = False if latitude is None else latitude
        self.iris_id = get_iris_from_coordinates(longitude, latitude)
        self.age = self.get_user_age()

        self.iseac = self.is_eac()
        self.group_id = self.get_ab_testing_group()
        self.recommendable_offer_table = (
            RECOMMENDABLE_OFFER_TABLE_PREFIX
            if not self.iseac
            else f"{RECOMMENDABLE_OFFER_TABLE_PREFIX}_{RECOMMENDABLE_OFFER_TABLE_SUFFIX_DICT[self.age]}"
        )

    def is_eac(self):
        start = time.time()
        with create_db_connection() as connection:
            request_response = connection.execute(
                text(
                    "SELECT count(1) > 0 "
                    "FROM public.enriched_user "
                    f"WHERE user_id = '{str(self.id)}' "
                    "AND user_deposit_initial_amount < 300 "
                    "AND FLOOR(DATE_PART('DAY',user_deposit_creation_date - user_birth_date)/365) < 18"
                )
            ).scalar()
        print(f"is_eac_user = {request_response}")
        log_duration(f"is_eac_user for {self.id}", start)
        return request_response

    def get_user_age(self):
        start = time.time()
        with create_db_connection() as connection:
            request_response = connection.execute(
                text(
                    "SELECT FLOOR(DATE_PART('DAY',user_deposit_creation_date - user_birth_date)/365)"
                    "FROM public.enriched_user"
                )
            ).scalar()
        print(f"user_age= {request_response}")
        log_duration(f"user_age for {self.id}", start)
        return int(request_response)

    def get_ab_testing_group(self):
        ab_testing = query_ab_testing_table(self.id)
        if ab_testing:
            group_id = ab_testing[0]
        else:
            group_id = ab_testing_assign_user(self.id)
        return group_id
