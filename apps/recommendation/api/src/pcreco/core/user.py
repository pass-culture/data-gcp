from pcreco.utils.geolocalisation import get_iris_from_coordinates
from pcreco.utils.db.db_connection import get_session
from sqlalchemy import text
from loguru import logger
from pcreco.models.reco.input_params import parse_float, parse_int


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
        self.is_geolocated = self.iris_id is not None
        self.age = None
        self.bookings_count = 0
        self.clicks_count = 0
        self.favorites_count = 0
        self.user_deposit_remaining_credit = 300
        self.found = False
        self.__get_user_profile()

    def __get_user_profile(self) -> None:
        """Compute age & remaining credit amount."""
        connection = get_session()

        request_response = connection.execute(
            text(
                f"""
                SELECT 
                    DATE_PART('year',AGE(user_birth_date)) as age,
                    COALESCE(user_theoretical_remaining_credit, user_deposit_initial_amount) as user_theoretical_remaining_credit,
                    COALESCE(booking_cnt, 0) as booking_cnt,
                    COALESCE(consult_offer, 0) as consult_offer,
                    COALESCE(has_added_offer_to_favorites, 0) as has_added_offer_to_favorites
                FROM public.enriched_user_mv
                WHERE user_id = :user_id 
                """
            ),
            user_id=str(self.id),
        ).fetchone()
        logger.info(f"{self.id}: __get_user_profile {request_response}")
        if request_response is not None:
            self.found = True
            self.age = parse_int(request_response[0])
            self.user_deposit_remaining_credit = parse_float(request_response[1], 300)
            self.bookings_count = parse_int(request_response[2], 0)
            self.clicks_count = parse_int(request_response[3], 0)
            self.favorites_count = parse_int(request_response[4], 0)
            logger.info(
                f"{self.id}: __get_user_profile {self.age}, {self.user_deposit_remaining_credit}, {self.bookings_count}, {self.clicks_count}, {self.favorites_count}"
            )
