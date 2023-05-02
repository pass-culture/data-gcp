class RecommendableOffersQueryBuilder:
    def __init__(self, reco_model, recommendable_offer_limit):
        self.reco_model = reco_model
        self.recommendable_offer_limit = recommendable_offer_limit

    def _rank_by_distance(self, source_table_name, export_table_name):
        return f"""
            reco_offers_with_distance_to_user   AS  (
                    SELECT
                        offer_id
                    ,   category
                    ,   subcategory_id
                    ,   search_group_name
                    ,   item_id
                    ,   booking_number
                    ,   is_geolocated
                    ,   CASE
                            WHEN
                                (
                                    venue_latitude    IS  NOT NULL
                                AND venue_longitude     IS  NOT NULL
                                AND :user_latitude    IS  NOT NULL
                                AND :user_longitude     IS  NOT NULL
                                and ro."position" ='in'
                                )
                            THEN
                                st_distance(st_point(:user_longitude, :user_latitude)::geometry, st_point(venue_longitude, venue_latitude)::geometry, FALSE)
                            WHEN(
                                    venue_latitude    IS  NOT NULL
                                AND venue_longitude     IS  NOT NULL
                                AND :user_latitude    IS  NOT NULL
                                AND :user_longitude     IS  NOT NULL
                                and ro."position" ='out'
                                )
                            THEN 
                                ro.user_distance
                            ELSE
                                -1
                        END     AS  user_distance
                    FROM
                        {source_table_name} ro
                )
            ,reco_offers_ranked_by_distance      AS  (
                    SELECT
                        *
                    , row_number() OVER(
                                partition BY
                                    item_id
                                ORDER BY
                                    user_distance   ASC
                                )
                        AS  rank
                    FROM
                        reco_offers_with_distance_to_user   ro
                )
            ,{export_table_name}                AS (
                SELECT
                    
                    offer_id
                    ,   category
                    ,   subcategory_id
                    ,   search_group_name
                    ,   item_id
                    ,   user_distance
                    ,   booking_number
                    ,   is_geolocated
                FROM
                    reco_offers_ranked_by_distance
                WHERE
                    rank    =   1
            )  
            """

    def _select_items(self, source_table_name, export_table_name):
        return f"""
            {export_table_name}                AS (
                SELECT
                    
                    offer_id
                    ,   category
                    ,   subcategory_id
                    ,   search_group_name
                    ,   item_id
                    ,   user_distance
                    ,   booking_number
                    ,   is_geolocated

                FROM
                    {source_table_name} 
            )  
        """

    def generate_query(self, order_query) -> str:
        user_profile_filter = (
            "AND is_underage_recommendable"
            if (self.reco_model.user.age and self.reco_model.user.age < 18)
            else ""
        )
        iris_distance = (
            " AND ("
            + " OR ".join(
                [
                    f"venue_distance_to_iris_bucket='{iris_dist}'"
                    for iris_dist in self.reco_model.reco_radius
                ]
            )
            + ")\n"
        )

        reco_geolocated_offers_sql = self._get_reco_offers(
            "(ro.is_geolocated and ro.iris_id = :user_iris_id)" + iris_distance,
            user_profile_filter,
            export_table_name="reco_geolocated_offers",
        )
        filter_by_distance_sql = self._rank_by_distance(
            source_table_name="reco_geolocated_offers",
            export_table_name="selected_geolocated_offers",
        )

        reco_non_geolocated_offers_sql = self._get_reco_offers(
            "(NOT ro.is_geolocated)",
            user_profile_filter,
            export_table_name="reco_non_geolocated_offers",
        )
        selected_offers_sql = self._select_items(
            source_table_name="reco_non_geolocated_offers",
            export_table_name="selected_non_geolocated_offers",
        )
        # Default
        if self.reco_model.user.iris_id and self.reco_model.include_digital:
            return f"""
                WITH {reco_geolocated_offers_sql}, {filter_by_distance_sql}, {reco_non_geolocated_offers_sql}, {selected_offers_sql}
                , tmp AS(
                    SELECT * FROM selected_non_geolocated_offers 
                    UNION ALL
                    SELECT * FROM selected_geolocated_offers
                )
                SELECT * FROM tmp
                ORDER BY {order_query}
                LIMIT {self.recommendable_offer_limit}
            """
        # No digital offer (case filtered by events)
        elif self.reco_model.user.iris_id:
            return f"""
                WITH {reco_geolocated_offers_sql}, {filter_by_distance_sql}
                SELECT * FROM selected_geolocated_offers 
                ORDER BY {order_query}
                LIMIT {self.recommendable_offer_limit}
            """
        # No geoloc
        elif self.reco_model.include_digital:
            return f"""
                WITH {reco_non_geolocated_offers_sql}, {selected_offers_sql}
                SELECT * FROM selected_non_geolocated_offers 
                ORDER BY {order_query}
                LIMIT {self.recommendable_offer_limit}
            """
        # No reco
        else:
            return None

    def _get_reco_offers(
        self,
        geoloc_filter,
        user_profile_filter,
        export_table_name,
    ) -> str:

        return f"""
                {export_table_name} AS  (
                        SELECT ro.offer_id
                        ,   ro.item_id
                        ,   ro.venue_id
                        ,   ro.venue_distance_to_iris as user_distance
                        ,   ro."position"
                        ,   ro.iris_id
                        ,   ro.booking_number
                        ,   ro.category
                        ,   ro.subcategory_id
                        ,   ro.search_group_name
                        ,   ro.is_geolocated
                        ,   ro.venue_latitude
                        ,   ro.venue_longitude
                        FROM
                            {self.reco_model.user.recommendable_offer_table} ro
                        WHERE {geoloc_filter}
                        AND offer_id    NOT IN  (
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
                    )
                    """
