from pcreco.utils.health_check_queries import get_available_materialized_view
from psycopg2 import sql
from pcreco.utils.db.db_connection import as_string
import typing as t
from pcreco.core.user import User
from pcreco.core.model.recommendable_item import RecommendableItem


class RecommendableOfferQueryBuilder:
    def __init__(self):
        self.recommendable_offer_table = get_available_materialized_view(
            "recommendable_offers_raw"
        )

    def generate_query(
        self,
        recommendable_items: t.List[RecommendableItem],
        user: User,
        order_query: str = "user_km_distance ASC, item_score ASC",
        offer_limit: int = 20,
    ):

        arr_sql = ",".join(
            [
                f"('{item.item_id}'::VARCHAR, {item.item_score}::FLOAT)"
                for item in recommendable_items
            ]
        )
        ranked_items = f"""
            ranked_items AS (
                SELECT s.item_id, s.item_score    
                FROM unnest(ARRAY[{arr_sql}]) 
                AS s(item_id VARCHAR, item_score FLOAT)
            )
        """

        main_query = sql.SQL(
            """
            WITH {ranked_items},

            select_offers as (
                SELECT 
                    ro.*,
                    coalesce(
                        case
                            when {user_geolocated} and is_geolocated
                            then st_distance(st_point({user_longitude}::float, {user_latitude}::float)::geography, venue_geo)
                        else 0.0
                        end,
                        0.0
                    ) as user_distance,
                    ri.item_score
                FROM {table_name} ro
                INNER JOIN ranked_items ri on ri.item_id = ro.item_id 
                
                WHERE 
                    case when {user_geolocated} then true else NOT is_geolocated end
                    
                    AND ro.item_id NOT IN  (
                        SELECT
                            item_id
                        FROM
                            non_recommendable_items
                        WHERE
                            user_id = {user_id}
                    )
                AND ro.stock_price <= {remaining_credit}
                {user_profile_filter}

            ),

            rank_offers AS (
                SELECT 
                    *,
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
                ro.stock_price,
                ro.offer_creation_date,
                ro.stock_beginning_date,
                ro.category,
                ro.subcategory_id,
                ro.search_group_name,
                ro.venue_latitude,
                ro.venue_longitude,
                ro.item_score
            FROM
                rank_offers ro
            WHERE 
                ro.rank = 1 
            {order_query}
            {offer_limit}    
                
        """
        ).format(
            ranked_items=sql.SQL(ranked_items),
            table_name=sql.SQL(self.recommendable_offer_table),
            user_geolocated=sql.Literal(
                user.longitude is not None and user.latitude is not None
            ),
            user_longitude=sql.Literal(user.longitude),
            user_latitude=sql.Literal(user.latitude),
            remaining_credit=sql.Literal(user.user_deposit_remaining_credit),
            user_id=sql.Literal(str(user.id)),
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
