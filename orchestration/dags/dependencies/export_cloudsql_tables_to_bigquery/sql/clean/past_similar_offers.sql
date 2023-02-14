WITH export_table AS (
    SELECT
        id
        , user_id
        , origin_offer_id
        , date
        , group_id
        , model_name
        , model_version
        , call_id
        , reco_filters
        , venue_iris_id
        , ROW_NUMBER() OVER (PARTITION BY id, user_id, call_id ORDER BY date DESC) as row_number

    FROM `{{ bigquery_raw_dataset }}.past_similar_offers`
)


SELECT * except(row_number)
FROM export_table
WHERE row_number=1