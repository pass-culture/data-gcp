# from pcreco.utils.health_check_queries import get_available_materialized_view
from psycopg2 import sql

# from pcreco.utils.db.db_connection import as_string
import typing as t
from schemas.user import User

DEFAULT_MAX_DISTANCE = 50000

import psycopg2.extensions


def as_string(composable, encoding="utf-8"):
    if isinstance(composable, sql.Composed):
        return "".join([as_string(x, encoding) for x in composable])
    elif isinstance(composable, sql.SQL):
        return composable.string
    else:
        rv = sql.ext.adapt(composable._wrapped)
        if isinstance(rv, psycopg2.extensions.QuotedString):
            rv.encoding = encoding
        rv = rv.getquoted()
        return rv.decode(encoding) if isinstance(rv, bytes) else rv


class RecommendableIrisOffersQueryBuilder:
    def __init__(self, params_in_filters: sql.SQL, recommendable_offer_limit):
        self.params_in_filters = params_in_filters
        self.recommendable_offer_limit = recommendable_offer_limit
        self.recommendable_iris_table = "recommendable_offers_per_iris_shape_mv"
        self.recommendable_offer_table = "recommendable_offers_raw_mv"

    def generate_query(
        self,
        order_query: str,
        user: str,
    ):
        main_query = sql.SQL(
            """
        WITH distance_filter AS (
            SELECT
                offer_id,
                item_id,
                case
	                when is_geolocated AND {user_geolocated}
                    then st_distance(st_point({user_longitude}::float, {user_latitude}::float)::geography, venue_geo)
	            else 0.0
                end as user_distance
            FROM
                {iris_table_name}
            WHERE
                (
                    {user_geolocated} 
                    AND is_geolocated
                    AND iris_id = {user_iris_id} 
                    AND ST_DWITHIN(st_point({user_longitude}::float, {user_latitude}::float)::geography, venue_geo,  {default_max_distance})
                )
                OR 
                ( NOT {user_geolocated} AND NOT is_geolocated )
        ),

        rank_offers AS (
            SELECT 
                offer_id,
                item_id,
                user_distance,
                ROW_NUMBER() OVER(partition BY item_id ORDER BY user_distance ASC) AS rank
            FROM distance_filter
        )

        SELECT 
            ro.offer_id
            ,   ro.item_id
            ,   ot.venue_id
            ,   ro.user_distance
            ,   ot.booking_number
            ,   ot.category
            ,   ot.subcategory_id
            ,   ot.search_group_name
            ,   ot.is_geolocated
            ,   ot.venue_latitude
            ,   ot.venue_longitude
            FROM
                rank_offers ro
            INNER JOIN {offer_table_name} ot ON ot.offer_id = ro.offer_id AND ot.item_id = ro.item_id
            WHERE 
            ro.rank = 1 
            AND ro.offer_id    NOT IN  (
                SELECT
                    offer_id
                FROM
                    non_recommendable_offers
                WHERE
                    user_id = {user_id}
            )
            AND ot.stock_price < {remaining_credit}
            {params_in_filter}
            {user_profile_filter}
            {order_by}
            """
        ).format(
            iris_table_name=sql.SQL(self.recommendable_iris_table),
            offer_table_name=sql.SQL(self.recommendable_offer_table),
            user_id=sql.Literal(str(user.id)),
            user_iris_id=sql.Literal(user.iris_id),
            user_geolocated=sql.Literal(
                user.longitude is not None and user.latitude is not None
            ),
            user_longitude=sql.Literal(user.longitude),
            user_latitude=sql.Literal(user.latitude),
            remaining_credit=sql.Literal(user.user_deposit_remaining_credit),
            default_max_distance=sql.Literal(DEFAULT_MAX_DISTANCE),
            params_in_filter=self.params_in_filters,
            user_profile_filter=sql.SQL(
                """
            AND is_underage_recommendable 
            """
                if (user.age and user.age < 18)
                else ""
            ),
            order_by=sql.SQL(
                f"""
            ORDER BY {order_query} LIMIT {self.recommendable_offer_limit}
            """
            ),
        )

        return as_string(main_query)
