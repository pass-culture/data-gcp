WITH export_table AS (
    SELECT
        id
        , userid
        , offerid
        , date
        , group_id
        , reco_origin
        , model_name
        , model_version
        , call_id
        , reco_filters
        , user_iris_id
        , ROW_NUMBER() OVER (PARTITION BY id, userid, call_id ORDER BY date DESC) as row_number

    FROM `{{ bigquery_raw_dataset }}.past_recommended_offers`
)


SELECT * except(row_number)
FROM export_table
WHERE row_number=1