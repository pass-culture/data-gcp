import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime, timedelta


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
    booking_day_numbers = [7 * week for week in range(1, 9)]
    user_count = []
    item_count = []
    row_count = []
    for booking_day_number in booking_day_numbers:
        print(f"{booking_day_number} booking day number...")
        start_date = (datetime.now() - timedelta(days=booking_day_number)).strftime(
            "%Y-%m-%d"
        )
        end_date = datetime.now().strftime("%Y-%m-%d")
        bookings = get_bookings(start_date=start_date, end_date=end_date)
        user_count.append(len(set(list(bookings.user_id.values))))
        item_count.append(len(set(list(bookings.offer_id))))
        row_count.append(bookings.shape[0])

    plt.plot(booking_day_numbers, user_count, label="Users")
    plt.plot(booking_day_numbers, item_count, label="Items")
    plt.plot(booking_day_numbers, row_count, label="Rows")
    plt.xlabel("Booking day number")
    plt.ylabel("Count")
    plt.legend()
    plt.show()


if __name__ == "__main__":
    main()
