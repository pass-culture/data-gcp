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

FROM `{{ bigquery_clean_dataset }}.past_similar_offers`
