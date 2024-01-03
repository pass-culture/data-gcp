WITH export_table AS (
    SELECT
        pso.id,
        call_id as reco_call_id,
        context,
        context_extra_data,
        date,
        date(date) as event_date,
        CAST(user_id AS STRING) as user_id,
        user_bookings_count,
        user_clicks_count,
        user_favorites_count,
        user_deposit_remaining_credit,
        user_iris_id,
        ii.centroid as user_iris_centroid,
        user_is_geolocated,
        offer_user_distance,
        offer_is_geolocated,
        CAST(offer_id as STRING) as offer_id,
        offer_item_id,
        offer_booking_number,
        offer_stock_price,
        offer_creation_date,
        offer_stock_beginning_date,
        offer_category,
        offer_subcategory_id,
        offer_item_rank,
        offer_item_score,
        offer_order,
        offer_venue_id,
        offer_extra_data,
        import_date
    FROM
        `{{ bigquery_raw_dataset }}.past_offer_context` pso
    LEFT JOIN `{{ bigquery_analytics_dataset }}.iris_france` ii
        on ii.id = pso.user_iris_id 
    WHERE 
        import_date >= DATE('{{ add_days(ds, -60) }}')
    
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            user_id,
            call_id,
            offer_id
        ORDER BY
            date DESC
        ) = 1
)
SELECT
    *,
    ROW_NUMBER() OVER (
            PARTITION BY 
            reco_call_id,
            event_date,
            user_id
            ORDER BY
            id
    ) as item_rank
FROM
    export_table
WHERE
    event_date >= DATE('{{ add_days(ds, -60) }}')