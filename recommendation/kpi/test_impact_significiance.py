import pandas as pd
import scipy.stats


def get_user_booking_number(start_date, end_date):
    query = f"""
        WITH booking AS (
            SELECT user_id, 
            groupid,
            COUNT(*) as booking_number,
            FROM `passculture-data-prod.clean_prod.applicative_database_booking` booking
            INNER JOIN `passculture-data-prod.raw_prod.ab_testing_20201207` ab_testing
            ON booking.user_id = ab_testing.userid
            WHERE booking.booking_creation_date >= DATETIME '{start_date} 00:00:00'
            AND booking.booking_creation_date <= DATETIME '{end_date} 00:00:00'
            GROUP BY user_id, groupid
            ORDER BY booking_number DESC
        ),
        connected_users AS (
            SELECT distinct(user_id_dehumanized) AS user_id FROM `passculture-data-prod.clean_prod.matomo_visits`
            WHERE visit_first_action_time >= TIMESTAMP '{start_date} 00:00:00' 
            AND visit_first_action_time <= TIMESTAMP '{end_date} 00:00:00'
        )
        
        SELECT connected_users.user_id, groupid, 
        (CASE WHEN booking_number IS NULL THEN 0 ELSE booking_number END) as booking_number
        FROM connected_users 
        LEFT JOIN  booking 
        ON connected_users.user_id = booking.user_id
        WHERE groupid IS NOT NULL
    """
    user_booking_number = pd.read_gbq(query)
    return user_booking_number


def get_user_offer_type_number(start_date, end_date):
    query = f"""
        WITH booking AS (
            select user_id, 
            groupid,
            count(distinct offer.offer_type) as offer_type_number,
            from `passculture-data-prod.clean_prod.applicative_database_booking` booking
            inner join `passculture-data-prod.clean_prod.applicative_database_stock` stock
            on booking.stock_id = stock.stock_id
            inner join `passculture-data-prod.clean_prod.applicative_database_offer` offer
            on stock.offer_id = offer.offer_id 
            inner join `passculture-data-prod.raw_prod.ab_testing_20201207` ab_testing
            on booking.user_id = ab_testing.userid
            where booking.booking_creation_date >= DATETIME '{start_date} 00:00:00'
            and booking.booking_creation_date <= DATETIME '{end_date} 00:00:00'
            group by user_id, groupid
        ),
        connected_users AS (
            SELECT distinct(user_id_dehumanized) AS user_id FROM `passculture-data-prod.clean_prod.matomo_visits`
            WHERE visit_first_action_time >= TIMESTAMP '{start_date} 00:00:00' 
            AND visit_first_action_time <= TIMESTAMP '{end_date} 00:00:00'
        )
        
        SELECT connected_users.user_id, groupid, 
        (CASE WHEN offer_type_number IS NULL THEN 0 ELSE offer_type_number END) as offer_type_number
        FROM connected_users 
        LEFT JOIN  booking 
        ON connected_users.user_id = booking.user_id
        WHERE groupid IS NOT NULL
    """
    user_offer_type_number = pd.read_gbq(query)
    return user_offer_type_number


booking_number = get_user_booking_number(start_date="2021-03-15", end_date="2021-04-15")
offer_type_number = get_user_offer_type_number(
    start_date="2021-03-15", end_date="2021-04-15"
)

serie_a_booking = booking_number.loc[lambda df: df["groupid"] == "A"]["booking_number"]
serie_b_booking = booking_number.loc[lambda df: df["groupid"] == "B"]["booking_number"]

serie_a_diversification = offer_type_number.loc[lambda df: df["groupid"] == "A"][
    "offer_type_number"
]
serie_b_diversification = offer_type_number.loc[lambda df: df["groupid"] == "B"][
    "offer_type_number"
]


def report(serie_a, serie_b):
    print(f"{len(serie_a)} observations for A with average of {serie_a.mean()}")
    print(f"{len(serie_b)} observations for B with average of {serie_b.mean()}")
    print(
        f"Difference between A and B on average: {((serie_a.mean() - serie_b.mean())/serie_b.mean())*100}"
    )
    result = scipy.stats.ttest_ind(
        list(serie_a.values),
        list(serie_b.values),
        equal_var=False,
        nan_policy="omit",
    )
    print(result)
    print()


report(serie_a_booking, serie_b_booking)
report(serie_a_diversification, serie_b_diversification)
