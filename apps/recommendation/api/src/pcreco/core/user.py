from pcreco.utils.geolocalisation import get_iris_from_coordinates
from pcreco.utils.db.db_connection import get_session
from sqlalchemy import text


class User:
    def __init__(
        self,
        user_id: str,
        call_id: str = None,
        longitude: float = None,
        latitude: float = None,
    ):
        self.id = user_id
        self.call_id = call_id
        self.longitude = longitude
        self.latitude = latitude
        self.iris_id = get_iris_from_coordinates(longitude, latitude)
        self.age = None
        self.bookings_count = 0
        self.clicks_count = 0
        self.favorites_count = 0
        self.user_deposit_remaining_credit = 300
        self.__get_user_profile()

    def __get_user_profile(self) -> None:
        """Compute age & remaining credit amount."""
        connection = get_session()

        request_response = connection.execute(
            text(
                f"""
                SELECT 
                    FLOOR(DATE_PART('DAY',user_deposit_creation_date - user_birth_date)/365) as age,
                    user_theoretical_remaining_credit,
                    user_deposit_initial_amount,
                    COALESCE(booking_cnt, 0) as booking_cnt,
                    COALESCE(consult_offer, 0) as consult_offer,
                    COALESCE(has_added_offer_to_favorites, 0) as has_added_offer_to_favorites
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
                self.bookings_count = (
                    request_response[3] if request_response[3] is not None else 0
                )
                self.clicks_count = (
                    request_response[4] if request_response[4] is not None else 0
                )
                self.favorites_count = (
                    request_response[5] if request_response[5] is not None else 0
                )
            except TypeError:
                pass
