from pcreco.utils.geolocalisation import get_iris_from_coordinates
from pcreco.utils.env_vars import log_duration
from pcreco.utils.db.db_connection import get_session
from loguru import logger
import time

class Offer:
    def __init__(self, offer_id, call_id=None, latitude=None, longitude=None) -> None:
        self.id = offer_id
        self.call_id = call_id
        self.longitude = False if longitude is None else longitude
        self.latitude = False if latitude is None else latitude
        self.iris_id = get_iris_from_coordinates(longitude, latitude)
        self.item_id = self.get_item_id(offer_id)
        self.cnt_bookings = self.get_cnt_bookings()
    
    
    def get_item_id(self, offer_id) -> str:
        start = time.time()
        connection = get_session()
        query_result = connection.execute(
            f"""
                SELECT item_id 
                FROM item_ids_mv
                WHERE offer_id = '{offer_id}'
            """
        ).fetchone()
        log_duration(f"get_item_id for offer_id: {offer_id}", start)
        if query_result is not None:
            logger.info("get_item_id:found id")
            return query_result[0]
        else:
            logger.info("get_item_id:not_found_id")
            return None

    def get_cnt_bookings(self, offer_id) -> int:
        start = time.time()
        connection = get_session()
        query_result = connection.execute(
            f"""
                SELECT sum(booking_number) as cnt_bookings
                FROM public.recommendable_offers_per_iris_shape_mv
                WHERE offer_id = '{offer_id}'
            """
        ).fetchone()
        log_duration(f"get_cnt_bookings for offer_id: {offer_id}", start)
        if query_result is not None:
            logger.info("get_cnt_bookings:found id")
            return query_result[0]
        else:
            logger.info("get_cnt_bookings:not_found_id")
            return None