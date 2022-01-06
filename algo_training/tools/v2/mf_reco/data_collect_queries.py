import pandas as pd
from datetime import datetime, timedelta

from utils import GCP_PROJECT_ID, ENV_SHORT_NAME


# events can be "ConsultOffer" for a clic and 'HasAddedOfferToFavorites' for favorites
def get_firebase_event(start_date, end_date, event_type, event_name):
    query = f"""
    with clicks_clean AS (
        SELECT
        CAST(event.user_id as STRING) as user_id,
        CASE
            WHEN offer.offer_subcategoryId in ('LIVRE_PAPIER','LIVRE_AUDIO_PHYSIQUE','SEANCE_CINE')
            THEN CONCAT('product-', offer.offer_product_id)
            ELSE CONCAT('offer-', offer.offer_id) END
        AS offer_id,
        offer.offer_name,ANY_VALUE(offer_subcategoryid) AS offer_subcategoryid,"{event_name}" as event_type, count(*) as event_count
        FROM `{GCP_PROJECT_ID}.analytics_{ENV_SHORT_NAME}.firebase_events` event 
        JOIN `{GCP_PROJECT_ID}.analytics_{ENV_SHORT_NAME}.applicative_database_offer` offer 
        ON offer.offer_id = event.offer_id
        WHERE event_name = "{event_type}"
        AND event_date >= '{start_date}'
        AND event_date < '{end_date}'
        AND user_id is not null
        AND event.offer_id is not null
        GROUP BY user_id, event.offer_id, offer.offer_id,offer.offer_name,offer.offer_subcategoryid,offer.offer_product_id
    )
    SELECT clicks.user_id, CAST(offer_id AS STRING) as offer_id, offer_name,offer_subcategoryid,event_type,event_count,user.user_age
    from clicks_clean clicks
    JOIN `{GCP_PROJECT_ID}.analytics_{ENV_SHORT_NAME}.applicative_database_user` user 
    ON user.user_id = clicks.user_id
    """
    fbevents = pd.read_gbq(query)
    return fbevents


def get_bookings_v2_mf(start_date, end_date):
    query = f"""
    with bookings as(
            select user_id, 
            CASE
                    WHEN offer.offer_subcategoryId in ('LIVRE_PAPIER','LIVRE_AUDIO_PHYSIQUE','SEANCE_CINE')
                    THEN CONCAT('product-', offer.offer_product_id)
                    ELSE CONCAT('offer-', offer.offer_id) END
                AS offer_id,
            offer_name, ANY_VALUE(offer_subcategoryid) AS offer_subcategoryid,"BOOKING" as event_type,count(*) as event_count, 
            from `{GCP_PROJECT_ID}.clean_{ENV_SHORT_NAME}.applicative_database_booking` booking
            inner join `{GCP_PROJECT_ID}.clean_{ENV_SHORT_NAME}.applicative_database_stock` stock
            on booking.stock_id = stock.stock_id
            inner join `{GCP_PROJECT_ID}.clean_{ENV_SHORT_NAME}.applicative_database_offer` offer
            on stock.offer_id = offer.offer_id 
            where offer.offer_creation_date >= DATETIME '{start_date} 00:00:00'
            and offer.offer_creation_date <= DATETIME '{end_date} 00:00:00'
            group by user_id, offer_id, offer_name, offer.offer_subcategoryid
            )
    SELECT
    CAST(bookings.user_id as STRING) as user_id,CAST(offer_id as STRING) as offer_id, offer_name,offer_subcategoryid, event_type,event_count,user.user_age
    from bookings
    JOIN `{GCP_PROJECT_ID}.analytics_{ENV_SHORT_NAME}.applicative_database_user` user 
    ON user.user_id = bookings.user_id
        """
    bookings = pd.read_gbq(query)
    return bookings
