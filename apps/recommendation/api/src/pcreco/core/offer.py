from pcreco.utils.geolocalisation import get_iris_from_coordinates
from pcreco.utils.env_vars import log_duration
from pcreco.utils.db.db_connection import get_session
import time
from sqlalchemy import text


class Offer:
    def __init__(self, offer_id, call_id=None, latitude=None, longitude=None) -> None:
        self.id = offer_id
        self.call_id = call_id
        self.iris_id = get_iris_from_coordinates(longitude, latitude)
        self.is_geolocated = self.iris_id is not None
        self.item_id, self.bookings_count, self.found = self.get_offer_characteristics(
            offer_id
        )

    def get_offer_characteristics(self, offer_id) -> str:
        """Get item_id attached to an offer_id & get the number of bookings attached to an offer_id."""
        start = time.time()
        connection = get_session()
        query_result = connection.execute(
            text(
                """
                SELECT item_id, booking_number
                FROM item_ids_mv
                WHERE offer_id = :offer_id
            """
            ),
            offer_id=str(offer_id),
        ).fetchone()
        log_duration(f"get_offer_characteristics for offer_id: {offer_id}", start)
        if query_result is not None:
            return query_result[0], query_result[1], True
        else:

            return None, 0, False
