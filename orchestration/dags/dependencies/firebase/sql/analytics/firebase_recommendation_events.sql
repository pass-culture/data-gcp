WITH firebase_recommendation_details AS (
    SELECT
        event_date,
        reco_call_id,
        user_id,
        user_pseudo_id,
        COALESCE(max(reco_geo_located), 0) as reco_geo_located,
        max(reco_model_version) as reco_model_version,
        max(reco_model_name) as reco_model_name,
        max(reco_model_endpoint) as reco_model_endpoint,
        max(reco_origin) as reco_origin,
    FROM
        `{{ bigquery_analytics_dataset }}.firebase_events` fe
    WHERE
        reco_call_id is not null
        AND event_date >= DATE_SUB(date('{{ ds }}'), INTERVAL 6 MONTH)
    GROUP BY
        1,
        2,
        3,
        4
),
past_recommended_offers AS (
    SELECT
        date as event_ts,
        CAST(userid as string) as user_id,
        call_id as reco_call_id,
        MAX(reco_filters) as reco_filters,
        MAX(user_iris_id) as user_iris_id,
        ARRAY_AGG(offerid) as offers_id,
    FROM
        `{{ bigquery_clean_dataset }}.past_recommended_offers` fe
    WHERE
        call_id is not null
        AND date(date) >= DATE_SUB(date('{{ ds }}'), INTERVAL 6 MONTH)
    GROUP BY
        1,
        2,
        3
)
SELECT
    frd.event_date,
    frd.reco_call_id,
    frd.user_id,
    frd.user_pseudo_id,
    frd.reco_geo_located,
    frd.reco_model_version,
    frd.reco_model_name,
    frd.reco_model_endpoint,
    frd.reco_origin,
    pro.event_ts,
    pro.reco_filters,
    pro.user_iris_id,
    pro.offers_id
FROM
    firebase_recommendation_details frd -- TODO fix this table has 1 day delay
    LEFT JOIN past_recommended_offers pro on pro.reco_call_id = frd.reco_call_id
    AND pro.user_id = frd.user_id