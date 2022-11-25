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

FROM `{{ bigquery_clean_dataset }}.past_recommended_offers`