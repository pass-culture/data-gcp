import pandas as pd
from datetime import datetime, timedelta

from utils import STORAGE_PATH


def get_bookings(start_date, end_date):
    query = f"""
        select user_id,
        (CASE WHEN offer.offer_type in ('ThingType.LIVRE_EDITION', 'EventType.CINEMA') THEN CONCAT('product-', offer.offer_product_id) ELSE CONCAT('offer-', offer.offer_id) END) AS offer_id, offer.offer_type as type,
        count(*) as nb_bookings
        from `passculture-data-prod.clean_prod.applicative_database_booking` booking
        inner join `passculture-data-prod.clean_prod.applicative_database_stock` stock
        on booking.stock_id = stock.stock_id
        inner join `passculture-data-prod.clean_prod.applicative_database_offer` offer
        on stock.offer_id = offer.offer_id
        where offer.offer_creation_date >= DATETIME '{start_date} 00:00:00'
        and offer.offer_creation_date <= DATETIME '{end_date} 00:00:00'
        group by user_id, offer_id, type
    """
    bookings = pd.read_gbq(query)
    return bookings


def main():
    start_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    bookings = get_bookings(start_date=start_date, end_date=end_date)
    bookings.to_csv(f"{STORAGE_PATH}/raw_data.csv")


if __name__ == "__main__":
    main()
