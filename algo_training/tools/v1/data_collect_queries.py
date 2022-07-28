import pandas as pd


def get_bookings(start_date, end_date):
    query = f"""
        select 
        user_id,
        (   CASE 
                WHEN offer.offer_subcategoryId in ('LIVRE_PAPIER','LIVRE_AUDIO_PHYSIQUE','SEANCE_CINE') 
                THEN CONCAT('product-', offer.offer_product_id) ELSE CONCAT('offer-', offer.offer_id) 
            END
        ) AS offer_id,
        offer.offer_subcategoryId as offer_subcategoryid,
        subcategories.category_id as offer_categoryId,
        enroffer.genres,
        enroffer.rayon,
        enroffer.type,
        enroffer.venue_id,
        enroffer.venue_name,
        count(*) as nb_bookings
         
        from `passculture-data-prod.clean_prod.applicative_database_booking` booking

        inner join `passculture-data-prod.clean_prod.applicative_database_stock` stock
        on booking.stock_id = stock.stock_id

        inner join `passculture-data-prod.clean_prod.applicative_database_offer` offer
        on stock.offer_id = offer.offer_id

        inner join `passculture-data-prod.clean_prod.subcategories` subcategories
        on offer.offer_subcategoryId = subcategories.id

        inner join `passculture-data-prod.analytics_prod.enriched_offer_data` enroffer
        on enroffer.offer_id = offer.offer_id

        where booking.booking_creation_date >= DATETIME '{start_date} 00:00:00'
        and booking.booking_creation_date <= DATETIME '{end_date} 00:00:00'
        and user_id is not null
        
        group by user_id, offer_id, offer_categoryId, offer_subcategoryid,enroffer.genres, enroffer.rayon, enroffer.type, enroffer.venue_id, enroffer.venue_name
    """
    bookings = pd.read_gbq(query)
    return bookings
