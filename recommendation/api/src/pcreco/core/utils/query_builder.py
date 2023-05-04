
class RecommendableOffersQueryBuilder:
    def __init__(self, reco_model, recommendable_offer_limit):
        self.reco_model = reco_model
        self.recommendable_offer_limit = recommendable_offer_limit

    def generate_query(self, order_query):
        user_profile_filter = (
            "AND is_underage_recommendable"
            if (self.reco_model.user.age and self.reco_model.user.age < 18)
            else ""
        )
        return f"""
        
        WITH distance_filter AS (
            SELECT
                *,
                st_distance(st_point(:user_longitude, :user_latitude)::geometry, st_point(venue_longitude, venue_latitude)::geometry, FALSE) as user_distance
            FROM
                recommendable_offers_raw
            WHERE
                earth_box(ll_to_earth(:user_longitude, :user_latitude), COALESCE(:max_distance, default_max_distance)) @> ll_to_earth(venue_longitude, venue_latitude) 
                AND earth_distance(ll_to_earth(:user_longitude, :user_latitude), 
                        ll_to_earth(venue_longitude, venue_latitude)) < COALESCE(:max_distance, default_max_distance)

        ),

        rank_offers AS (
            SELECT 
                * , ROW_NUMBER() OVER(partition BY item_id ORDER BY user_distance   ASC) AS  rank
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
            offer_id    NOT IN  (
                    SELECT
                        offer_id
                    FROM
                        non_recommendable_offers
                    WHERE
                        user_id =   :user_id
            )
            {self.reco_model.params_in_filters}
            {user_profile_filter}
            AND ro.stock_price < {self.reco_model.user.user_deposit_remaining_credit}
            ORDER BY {order_query}
            LIMIT {self.recommendable_offer_limit}

                
        """