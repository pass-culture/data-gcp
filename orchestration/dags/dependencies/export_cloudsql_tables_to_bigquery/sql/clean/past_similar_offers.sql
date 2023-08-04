WITH export_table AS (
    SELECT
        call_id as reco_call_id
        , CAST(user_id AS STRING) as user_id
        , CAST(origin_offer_id as STRING) as origin_offer_id 
        , CAST(offer_id as STRING) as offer_id
        , date(date) as event_date
        , date as ts
        , model_name
        , model_version
        , venue_iris_id
        , i.centroid as venue_iris_centroid
        , ROW_NUMBER() OVER (PARTITION BY call_id, date(date), user_id, origin_offer_id ORDER BY pso.id ) as item_rank
    FROM `{{ bigquery_raw_dataset }}.past_similar_offers` pso
    LEFT JOIN `{{ bigquery_anlytics_dataset }}.iris_france` i on i.id = pso.venue_iris_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY origin_offer_id, user_id, call_id, offer_id ORDER BY date DESC) = 1

)

SELECT 
    * EXCEPT(item_rank)
    , ROW_NUMBER() OVER (PARTITION BY reco_call_id, event_date, user_id, origin_offer_id ORDER BY item_rank ) as item_rank
FROM export_table
