from pcreco.utils.health_check_queries import get_materialized_view_status
from psycopg2 import sql
from pcreco.utils.db.db_connection import as_string


class RecommendableOffersQueryBuilder:
    def __init__(self, reco_model, recommendable_offer_limit):
        self.reco_model = reco_model
        self.recommendable_offer_limit = recommendable_offer_limit
        self.get_recommendable_offers_table()

    def get_recommendable_offers_table(self):
        view_name = "recommendable_offers_raw_mv"
        if get_materialized_view_status(view_name)[f"is_{view_name}_datasource_exists"]:
            self.recommendable_offer_table = view_name
        else:
            self.recommendable_offer_table = f"{view_name}_old"

    def generate_query(
        self,
        order_query: str,
        user_id: str,
        user_longitude: float,
        user_latitude: float,
    ):
        main_query = sql.SQL(
            """
        
        WITH distance_filter AS (
            SELECT
                *,
                case
	                when is_geolocated AND {user_longitude} is not null  AND {user_latitude} is not null 
                    then st_distance(st_point({user_longitude}, {user_latitude})::geometry, venue_geo)
	            else 0.0
                end as user_distance
            FROM
                {table_name}
            WHERE

                (is_geolocated AND {user_longitude} is not null AND {user_latitude} is not null AND ST_DWITHIN(venue_geo, st_point({user_longitude}, {user_latitude})::geography, default_max_distance))
                OR NOT is_geolocated
        ),

        rank_offers AS (
            SELECT 
                *, ROW_NUMBER() OVER(partition BY item_id ORDER BY user_distance ASC) AS rank
            FROM distance_filter
        )

        SELECT 
            ro.offer_id
            ,   ro.item_id
            ,   ro.venue_id
            ,   ro.user_distance
            ,   ro.booking_number
            ,   ro.category
            ,   ro.subcategory_id
            ,   ro.search_group_name
            ,   ro.is_geolocated
            ,   ro.venue_latitude
            ,   ro.venue_longitude
            FROM
                rank_offers ro
            WHERE 
            ro.rank = 1 
            AND offer_id    NOT IN  (
                SELECT
                    offer_id
                FROM
                    non_recommendable_offers
                WHERE
                    user_id = {user_id}
            )
            AND ro.stock_price < {remaining_credit}
            """
        ).format(
            table_name=sql.SQL(self.recommendable_offer_table),
            user_id=sql.Literal(user_id),
            user_longitude=sql.Literal(user_longitude),
            user_latitude=sql.Literal(user_latitude),
            remaining_credit=sql.Literal(
                self.reco_model.user.user_deposit_remaining_credit
            ),
        )
        params_in_filter = self.reco_model.params_in_filters
        user_profile_filter = sql.SQL(
            """
            AND is_underage_recommendable 
            """
            if (self.reco_model.user.age and self.reco_model.user.age < 18)
            else ""
        )
        order_by = sql.SQL(
            f"""
            ORDER BY {order_query} LIMIT {self.recommendable_offer_limit}
            """
        )

        return as_string(main_query + params_in_filter + user_profile_filter + order_by)
