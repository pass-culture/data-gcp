import pandas as pd
from datetime import datetime, timedelta

from utils import STORAGE_PATH, BOOKING_DAY_NUMBER, MODEL_NAME


def get_bookings(start_date, end_date):
    query = f"""
        select user_id,
        (CASE WHEN offer.offer_subcategoryId in ('LIVRE_PAPIER','LIVRE_AUDIO_PHYSIQUE','SEANCE_CINE') THEN CONCAT('product-', offer.offer_product_id) ELSE CONCAT('offer-', offer.offer_id) END) AS offer_id,
        offer.offer_subcategoryId as subcategoryId, count(*) as nb_bookings
        from `passculture-data-prod.clean_prod.applicative_database_booking` booking
        inner join `passculture-data-prod.clean_prod.applicative_database_stock` stock
        on booking.stock_id = stock.stock_id
        inner join `passculture-data-prod.clean_prod.applicative_database_offer` offer
        on stock.offer_id = offer.offer_id
        where offer.offer_creation_date >= DATETIME '{start_date} 00:00:00'
        and offer.offer_creation_date <= DATETIME '{end_date} 00:00:00'
        and user_id is not null
        group by user_id, offer_id, subcategoryId 
    """
    bookings = pd.read_gbq(query)
    return bookings


def get_clics(start_date, end_date, test_date):
    query_clics = f"""
        WITH events AS (
            SELECT
                user_id,
                offer_id,
                count(*) as click_count,
                CASE
                    WHEN event_date > {test_date} THEN False
                    ELSE True
                END
                AS train_set
            FROM `passculture-data-prod.analytics_prod.firebase_events`
            WHERE event_name = "ConsultOffer"
            AND event_date >= {start_date}
            AND event_date < {end_date}
            AND user_id is not null
            AND offer_id is not null
            AND offer_id != 'NaN'
            GROUP BY user_id, offer_id, train_set
        )
        SELECT
            user_id,
            ANY_VALUE(offer.offer_id) AS offer_id,
            ANY_VALUE(offer_subcategoryid) AS offer_subcategoryid,
            CASE
                WHEN offer.offer_subcategoryId in ('LIVRE_PAPIER','LIVRE_AUDIO_PHYSIQUE','SEANCE_CINE')
                THEN CONCAT('product-', offer.offer_product_id)
                ELSE CONCAT('offer-', offer.offer_id) END
            AS item_id,
            SUM(click_count) as click_count,
            train_set
        FROM events
        JOIN `passculture-data-prod.clean_prod.applicative_database_offer` offer
        ON offer.offer_id = events.offer_id
        GROUP BY user_id, item_id, train_set
        """

    return pd.read_gbq(query_clics)


def main():
    start_date = (datetime.now() - timedelta(days=BOOKING_DAY_NUMBER)).strftime(
        "%Y-%m-%d"
    )
    end_date = datetime.now().strftime("%Y-%m-%d")
    test_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    if MODEL_NAME == "v1":
        bookings = get_bookings(start_date=start_date, end_date=end_date)
        bookings.to_csv(f"{STORAGE_PATH}/raw_data.csv")
    if MODEL_NAME == "v2_deep_reco":
        clics = get_clics(start_date, end_date, test_date)
        clics.to_csv(f"{STORAGE_PATH}/raw_data.csv")


if __name__ == "__main__":
    main()
