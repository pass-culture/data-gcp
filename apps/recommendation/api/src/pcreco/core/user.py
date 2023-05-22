from pcreco.utils.geolocalisation import get_iris_from_coordinates
from pcreco.utils.db.db_connection import get_session
from sqlalchemy import text


class User:
    def __init__(self, user_id, call_id=None, longitude=None, latitude=None):
        self.id = user_id
        self.call_id = call_id
        self.longitude = False if longitude is None else longitude
        self.latitude = False if latitude is None else latitude
        self.iris_id = get_iris_from_coordinates(longitude, latitude)
        self.get_user_profile()

    def get_user_profile(self) -> None:
        """Compute age & remaining credit amount."""

        self.age = None
        # default value
        self.user_deposit_remaining_credit = 300
        connection = get_session()

        request_response = connection.execute(
            text(
                f"""
                SELECT 
                    FLOOR(DATE_PART('DAY',user_deposit_creation_date - user_birth_date)/365) as age,
                    user_theoretical_remaining_credit,
                    user_deposit_initial_amount
                    FROM public.enriched_user_mv
                    WHERE user_id = :user_id 
                """
            ),
            user_id=str(self.id),
        ).fetchone()
        if request_response is not None:
            try:
                self.age = int(request_response[0])
                self.user_deposit_remaining_credit = (
                    request_response[1]
                    if request_response[1] is not None
                    else request_response[2]
                )
            except TypeError:
                pass
