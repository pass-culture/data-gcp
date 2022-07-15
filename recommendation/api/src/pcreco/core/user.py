from pcreco.core.utils.ab_testing import (
    query_ab_testing_table,
    ab_testing_assign_user,
)
from pcreco.utils.geolocalisation import get_iris_from_coordinates
from pcreco.utils.db.db_connection import get_db
from sqlalchemy import text
from pcreco.utils.env_vars import (
    RECOMMENDABLE_OFFER_TABLE_PREFIX,
    RECOMMENDABLE_OFFER_TABLE_SUFFIX_DICT,
    AB_TESTING,
)


class User:
    def __init__(self, user_id, longitude=None, latitude=None):
        self.id = user_id
        self.longitude = False if longitude is None else longitude
        self.latitude = False if latitude is None else latitude
        self.iris_id = get_iris_from_coordinates(longitude, latitude)
        self.get_user_profile()
        self.get_ab_testing_group()
        self.recommendable_offer_table = (
            RECOMMENDABLE_OFFER_TABLE_PREFIX
            if not self.is_eac
            else f"{RECOMMENDABLE_OFFER_TABLE_PREFIX}_{RECOMMENDABLE_OFFER_TABLE_SUFFIX_DICT[self.age]}"
        )

    @property
    def is_eac(self) -> bool:
        if self.age:
            return self.age < 18 and self.user_deposit_initial_amount < 300
        else:
            return False

    def get_user_profile(self) -> None:
        self.age = None
        self.user_deposit_initial_amount = 0
        connection = get_db()

        request_response = connection.execute(
            text(
                f"""
                SELECT 
                    FLOOR(DATE_PART('DAY',user_deposit_creation_date - user_birth_date)/365) as age,
                    user_deposit_initial_amount
                    FROM public.enriched_user
                    WHERE user_id = '{str(self.id)}' 
                """
            )
        ).fetchone()
        if request_response is not None:
            self.age = int(request_response[0])
            self.user_deposit_initial_amount = request_response[1]

    def get_ab_testing_group(self) -> None:
        if AB_TESTING:
            ab_testing = query_ab_testing_table(self.id)
            if ab_testing:
                self.group_id = ab_testing[0]
            else:
                self.group_id = ab_testing_assign_user(self.id)
        else:
            self.group_id = None
