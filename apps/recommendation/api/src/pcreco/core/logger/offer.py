import datetime
import pytz
from pcreco.core.model.recommendable_offer import RecommendableOffer
from pcreco.core.user import User
import typing as t
from pcreco.utils.db.db_connection import get_session
from sqlalchemy import text


def save_context(
    offers: t.List[RecommendableOffer], call_id: str, context: str, user: User
) -> None:
    if len(offers) > 0:
        date = datetime.datetime.now(pytz.utc)
        rows = []

        for o in offers:
            db_row = {
                "call_id": call_id,
                "context": context,
                "date": date,
                "user_id": user.id,
                "user_bookings_count": user.bookings_count,
                "user_clicks_count": user.clicks_count,
                "user_favorites_count": user.favorites_count,
                "user_deposit_remaining_credit": user.user_deposit_remaining_credit,
                "user_iris_id": user.iris_id,
                "user_latitude": user.latitude,
                "user_longitude": user.longitude,
                "offer_user_distance": o.user_distance,
                "offer_id": o.offer_id,
                "offer_item_id": o.item_id,
                "offer_booking_number": o.booking_number,
                "offer_stock_price": o.stock_price,
                "offer_creation_date": o.offer_creation_date,
                "offer_stock_beginning_date": o.stock_beginning_date,
                "offer_category": o.category,
                "offer_subcategory_id": o.subcategory_id,
                "offer_item_score": o.item_score,
                "offer_order": o.offer_score,
                "offer_venue_id": o.venue_id,
            }

            rows.append(db_row)

        DB_FIELDS = sorted(list(db_row.keys()))

        connection = get_session()
        fields = ",".join(DB_FIELDS)
        values = ":" + ", :".join(DB_FIELDS)
        connection.execute(
            text(
                f"""
                INSERT INTO public.offer_context ({fields})
                VALUES ({values})
                """
            ),
            rows,
        )
