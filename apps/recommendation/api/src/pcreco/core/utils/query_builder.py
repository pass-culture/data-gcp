from pcreco.utils.health_check_queries import get_available_materialized_view
from psycopg2 import sql
from pcreco.utils.db.db_connection import as_string
import typing as t
from loguru import logger

DEFAULT_MAX_DISTANCE = 50000


class RecommendableIrisOffersQueryBuilder:
    def __init__(self, reco_model, recommendable_offer_limit):
        self.reco_model = reco_model
        self.recommendable_offer_limit = recommendable_offer_limit
        self.recommendable_iris_table = get_available_materialized_view(
            "recommendable_offers_per_iris_shape"
        )
        self.recommendable_offer_table = get_available_materialized_view(
            "recommendable_offers_raw"
        )

    def generate_query(
        self,
        retrieval_order_query: str,
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
            params_in_filter=self.reco_model.params_in_filters,
            user_profile_filter=sql.SQL(
                """
            AND is_underage_recommendable 
            """
                if (user.age and user.age < 18)
                else ""
            ),
            order_by=sql.SQL(
                f"""
            ORDER BY {retrieval_order_query} LIMIT {self.recommendable_offer_limit}
            """
            ),
        )
        return as_string(main_query)


class RecommendableItemQueryBuilder:
    def __init__(self, reco_model):
        self.reco_model = reco_model
        self.recommendable_item_table = get_available_materialized_view(
            "recommendable_items_raw"
        )

    def generate_query(
        self,
        user: str,
        order_query: str = "booking_number DESC",
        offer_limit: int = 50_000,
    ):
        main_query = sql.SQL(
            """

            SELECT 
                item_id
            FROM {table_name}
            WHERE 
            1 = 1
            {params_in_filter}
            {user_profile_filter}
            {order_query}
            {offer_limit}        
        """
        ).format(
            table_name=sql.SQL(self.recommendable_item_table),
            params_in_filter=self.reco_model.params_in_filters,
            user_profile_filter=sql.SQL(
                """
                AND is_underage_recommendable 
                """
                if (user.age and user.age < 18)
                else ""
            ),
            order_query=sql.SQL(f"ORDER BY {order_query}"),
            offer_limit=sql.SQL(f"LIMIT {offer_limit}"),
        )
        return as_string(main_query)


class RecommendableOfferQueryBuilder:
    def __init__(self, reco_model):
        self.reco_model = reco_model
        self.recommendable_offer_table = get_available_materialized_view(
            "recommendable_offers_raw"
        )

    def generate_query(
        self,
        selected_items: t.List[str],
        user: str,
        order_query: str = "user_km_distance ASC, item_rank ASC",
        offer_limit: int = 20,
    ):
        arr_item = ",".join(list(selected_items))
        main_query = sql.SQL(
            """
            WITH ranked_items AS (
                SELECT a.item_id, a.item_rank
                FROM  unnest(string_to_array({arr_item}, ',')) WITH ORDINALITY a(item_id, item_rank)
            ),

            select_offers as (
                SELECT 
                    ro.*,
                    case
                        when is_geolocated AND {user_geolocated}
                        then st_distance(st_point({user_longitude}::float, {user_latitude}::float)::geography, venue_geo)
                    else 0.0
                    end as user_distance,
                    ri.item_rank
                FROM {table_name} ro
                INNER JOIN ranked_items ri on ri.item_id = ro.item_id 
            ),

            rank_offers AS (
                SELECT 
                    *,
                    -- percent over max distance
                    floor(10 * user_distance / default_max_distance) as user_km_distance,
                    ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY user_distance ASC) AS rank
                FROM select_offers
                WHERE user_distance < default_max_distance
            )
            SELECT 
                ro.offer_id,
                ro.item_id,
                ro.venue_id,
                ro.user_distance,
                ro.booking_number,
                ro.category,
                ro.subcategory_id,
                ro.search_group_name,
                ro.is_geolocated,
                ro.venue_latitude,
                ro.venue_longitude
            FROM
                rank_offers ro
            WHERE 
                ro.rank = 1 
            AND offer_id NOT IN  (
                    SELECT
                        offer_id
                    FROM
                        non_recommendable_offers
                    WHERE
                        user_id = {user_id}
            )
            AND ro.stock_price < {remaining_credit}
            {user_profile_filter}
            {order_query}
            {offer_limit}        
        """
        ).format(
            arr_item=sql.Literal(arr_item),
            table_name=sql.SQL(self.recommendable_offer_table),
            user_id=sql.Literal(str(user.id)),
            user_geolocated=sql.Literal(
                user.longitude is not None and user.latitude is not None
            ),
            user_longitude=sql.Literal(user.longitude),
            user_latitude=sql.Literal(user.latitude),
            remaining_credit=sql.Literal(user.user_deposit_remaining_credit),
            order_query=sql.SQL(f"ORDER BY {order_query}"),
            offer_limit=sql.SQL(f"LIMIT {offer_limit}"),
            user_profile_filter=sql.SQL(
                """
                AND is_underage_recommendable 
                """
                if (user.age and user.age < 18)
                else ""
            ),
        )
        return as_string(main_query)
