import pandas as pd


def get_clics(start_date, end_date, test_date):
    query_clics = f"""
        WITH events AS (
            SELECT
                user_id,
                offer_id,
                count(*) as click_count,
                CASE
                    WHEN event_date > '{test_date}' THEN False
                    ELSE True
                END
                AS train_set
            FROM `passculture-data-prod.analytics_prod.firebase_events`
            WHERE event_name = "ConsultOffer"
            AND event_date >= '{start_date}'
            AND event_date < '{end_date}'
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
