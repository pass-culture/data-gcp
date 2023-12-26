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
        
        event_date >= DATE('{{ add_days(ds, -356) }}')
        AND reco_call_id is not null
        AND module_id is not null
        AND entry_id is not null 
    GROUP BY
        1,
        2,
        3,
        4
),
past_recommended_offers AS (
    SELECT
        date as event_ts,
        event_date,
        user_id,
        call_id as reco_call_id,
        MAX(reco_filters) as reco_filters,
        MAX(user_iris_id) as user_iris_id,
    FROM
        `{{ bigquery_clean_dataset }}.past_recommended_offers` fe
    WHERE
        event_date >= DATE('{{ add_days(ds, -356) }}')
        AND call_id is not null
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
FROM
    firebase_recommendation_details frd
    LEFT JOIN past_recommended_offers pro on pro.reco_call_id = frd.reco_call_id
    AND pro.user_id = frd.user_id
    AND pro.event_date = frd.event_date